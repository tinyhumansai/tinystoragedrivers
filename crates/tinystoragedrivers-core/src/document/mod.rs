//! The document port: versioned JSON objects in named collections.
//!
//! This is the port most repositories are built on. A handle is already bound
//! to one [`Scope`](crate::Scope); two handles for different scopes on the same
//! backend never see each other's documents, even in the same collection.
//!
//! Concurrency is optimistic: every document carries a [`Version`], and a write
//! can demand a [`Precondition`] on it. [`DocumentStore::claim`] covers the one
//! pattern optimistic writes cannot express cheaply, "take the next eligible
//! item and mark it mine", as a single atomic step on every driver.

mod types;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::capabilities::{Capabilities, Capability};
use crate::error::{Result, StorageError};
use crate::filter::{Filter, Sort};

pub use types::{
    CollectionSpec, Cursor, IndexSpec, MAX_COLLECTION_LEN, MAX_ID_LEN, Page, Precondition, Query,
    RESERVED_PREFIX, SearchHit, SearchSpec, Version, Versioned, WriteOp, WriteResult,
    validate_collection, validate_doc, validate_id,
};

/// Versioned JSON documents in named collections, bound to one scope.
#[async_trait]
pub trait DocumentStore: Send + Sync + std::fmt::Debug {
    /// Optional abilities this driver provides.
    fn capabilities(&self) -> Capabilities;

    /// Declare a collection's indexes, expiry and search fields. Idempotent:
    /// declaring the same spec again is a no-op, and declaring a changed spec
    /// adds what is new.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid spec, [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported)
    /// when the spec asks for expiry the driver lacks, or a backend error.
    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()>;

    /// Read one document. Expired documents read as `None`.
    ///
    /// # Errors
    ///
    /// Invalid names, or a backend error.
    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>>;

    /// Write one document, which must be a JSON object, and return its new
    /// version.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Conflict`](crate::ErrorKind::Conflict) when the
    /// precondition fails,
    /// [`ErrorKind::AlreadyExists`](crate::ErrorKind::AlreadyExists) when a
    /// unique index rejects it, invalid input, or a backend error.
    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version>;

    /// Remove one document and report whether it existed.
    ///
    /// [`Precondition::Absent`] succeeds (removing nothing) only when the
    /// document does not exist.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Conflict`](crate::ErrorKind::Conflict) when the
    /// precondition fails, invalid input, or a backend error.
    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool>;

    /// Read the documents matching a query, one page at a time.
    ///
    /// # Errors
    ///
    /// Invalid names or filter, a cursor this driver did not issue, or a
    /// backend error.
    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>>;

    /// Count the documents matching `filter`.
    ///
    /// # Errors
    ///
    /// Invalid names or filter, or a backend error.
    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64>;

    /// Remove every document matching `filter` and report how many went.
    ///
    /// # Errors
    ///
    /// Invalid names or filter, or a backend error.
    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64>;

    /// Atomically pick the first document matching `filter` in `sort` order,
    /// apply the merge `patch` (RFC 7396, see
    /// [`value::merge_patch`](crate::value::merge_patch)) and return the
    /// updated document. Two concurrent claims never return the same document
    /// when the patch makes it stop matching `filter`.
    ///
    /// # Errors
    ///
    /// Invalid names or filter, a patch that is not an object, or a backend
    /// error.
    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>>;

    /// Apply several writes, across documents and collections, all or nothing.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::Transactions`]; otherwise the first failing write's error,
    /// with nothing applied.
    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        let _ = ops;
        Err(StorageError::unsupported(
            Capability::Transactions,
            "this driver cannot apply writes atomically across documents",
        ))
    }

    /// Full-text search over the collection's
    /// [`SearchSpec`](crate::SearchSpec) fields, best matches first.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::FullText`], invalid input when the collection declares no
    /// search fields, or a backend error.
    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let _ = (collection, text, limit);
        Err(StorageError::unsupported(
            Capability::FullText,
            "this driver has no full-text index",
        ))
    }

    /// Remove every document of the collection in this scope.
    ///
    /// # Errors
    ///
    /// Invalid name, or a backend error.
    async fn drop_collection(&self, collection: &str) -> Result<()>;
}

/// Typed convenience over any [`DocumentStore`]: serde in and out.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use serde::{Deserialize, Serialize};
/// use tinystoragedrivers_core::{DocumentStoreExt, MemoryStorage, Precondition, Scope, StorageBackend};
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct Device { name: String }
///
/// let storage = MemoryStorage::new().for_scope(&Scope::local())?;
/// let docs = storage.documents();
/// docs.put_as("devices", "d1", &Device { name: "laptop".into() }, Precondition::None).await?;
/// let device = docs.get_as::<Device>("devices", "d1").await?.unwrap();
/// assert_eq!(device.doc.name, "laptop");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
#[async_trait]
pub trait DocumentStoreExt: DocumentStore {
    /// [`DocumentStore::get`], decoded into `T`.
    ///
    /// # Errors
    ///
    /// The read's error, or [`ErrorKind::Serialization`](crate::ErrorKind::Serialization)
    /// when the stored body does not decode as `T`.
    async fn get_as<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<Versioned<T>>> {
        match self.get(collection, id).await? {
            Some(found) => Ok(Some(decode(found)?)),
            None => Ok(None),
        }
    }

    /// [`DocumentStore::put`], encoding `doc` first.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Serialization`](crate::ErrorKind::Serialization) when `doc`
    /// does not encode, or the write's error.
    async fn put_as<T: Serialize + Sync>(
        &self,
        collection: &str,
        id: &str,
        doc: &T,
        precondition: Precondition,
    ) -> Result<Version> {
        let value = serde_json::to_value(doc)?;
        self.put(collection, id, value, precondition).await
    }

    /// [`DocumentStore::query`], decoding every document into `T`.
    ///
    /// # Errors
    ///
    /// The query's error, or a decode error.
    async fn query_as<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        query: &Query,
    ) -> Result<Page<Versioned<T>>> {
        let page = self.query(collection, query).await?;
        let items = page
            .items
            .into_iter()
            .map(decode)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            items,
            next: page.next,
        })
    }

    /// Every document matching `query`, following cursors to the end.
    ///
    /// # Errors
    ///
    /// The first page's error.
    async fn query_all(&self, collection: &str, query: &Query) -> Result<Vec<Versioned<Value>>> {
        let mut query = query.clone();
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = self.query(collection, &query).await?;
            out.extend(page.items);
            match page.next {
                // A driver that hands back any cursor it already issued would
                // loop forever; treat that as the driver bug it is.
                Some(cursor) if !seen.insert(cursor.0.clone()) => {
                    return Err(StorageError::backend("query paging did not advance"));
                }
                Some(cursor) => query.cursor = Some(cursor),
                None => return Ok(out),
            }
        }
    }
}

impl<S: DocumentStore + ?Sized> DocumentStoreExt for S {}

fn decode<T: DeserializeOwned>(found: Versioned<Value>) -> Result<Versioned<T>> {
    let doc = serde_json::from_value(found.doc)?;
    Ok(Versioned {
        id: found.id,
        version: found.version,
        doc,
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
