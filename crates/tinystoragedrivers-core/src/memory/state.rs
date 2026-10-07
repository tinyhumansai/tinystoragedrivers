//! The in-memory driver's data and the document rules applied to it.
//!
//! Everything lives in one [`DbState`] behind a mutex per database, so every
//! operation is trivially atomic and [`DocumentStore::atomic_batch`] is "apply
//! to a copy, then swap".
//!
//! [`DocumentStore::atomic_batch`]: crate::DocumentStore::atomic_batch

use std::collections::{BTreeMap, VecDeque};

use serde_json::Value;

use crate::blob::BlobMeta;
use crate::document::{
    CollectionSpec, Precondition, Version, Versioned, validate_collection, validate_doc,
    validate_id,
};
use crate::error::{Result, StorageError};
use crate::filter::Filter;
use crate::value;

/// `(scope, collection or stream or key)`.
pub(super) type ScopedKey = (String, String);

/// One stored document.
#[derive(Debug, Clone)]
pub(super) struct StoredDoc {
    pub(super) version: Version,
    pub(super) doc: Value,
}

/// One stream: offsets `base..base + entries.len()` are retained.
#[derive(Debug, Clone, Default)]
pub(super) struct StoredStream {
    pub(super) base: u64,
    pub(super) entries: VecDeque<Value>,
}

impl StoredStream {
    pub(super) fn len(&self) -> u64 {
        self.base + self.entries.len() as u64
    }
}

/// Everything one in-memory database holds.
#[derive(Debug, Clone, Default)]
pub(super) struct DbState {
    pub(super) specs: BTreeMap<String, CollectionSpec>,
    pub(super) docs: BTreeMap<ScopedKey, BTreeMap<String, StoredDoc>>,
    pub(super) streams: BTreeMap<ScopedKey, StoredStream>,
    pub(super) blobs: BTreeMap<ScopedKey, (BlobMeta, Vec<u8>)>,
}

impl DbState {
    pub(super) fn spec(&self, collection: &str) -> CollectionSpec {
        self.specs
            .get(collection)
            .cloned()
            .unwrap_or_else(|| CollectionSpec::new(collection))
    }

    /// Whether `doc` has passed its collection's expiry time.
    pub(super) fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
        spec.ttl_field
            .as_deref()
            .and_then(|field| value::lookup(doc, field))
            .and_then(Value::as_f64)
            .is_some_and(|expires| {
                // Epoch milliseconds fit an f64 exactly until the year 287396.
                #[allow(clippy::cast_precision_loss)]
                let now = now_ms as f64;
                expires <= now
            })
    }

    /// The live (non-expired) documents of a collection in a scope.
    pub(super) fn live<'a>(
        &'a self,
        scope: &str,
        collection: &str,
        now_ms: u64,
    ) -> Vec<(&'a String, &'a StoredDoc)> {
        let spec = self.spec(collection);
        self.docs
            .get(&(scope.to_owned(), collection.to_owned()))
            .map(|docs| {
                docs.iter()
                    .filter(|(_, stored)| !Self::expired(&spec, &stored.doc, now_ms))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn get(
        &self,
        scope: &str,
        collection: &str,
        id: &str,
        now_ms: u64,
    ) -> Option<Versioned<Value>> {
        let spec = self.spec(collection);
        self.docs
            .get(&(scope.to_owned(), collection.to_owned()))
            .and_then(|docs| docs.get(id))
            .filter(|stored| !Self::expired(&spec, &stored.doc, now_ms))
            .map(|stored| Versioned {
                id: id.to_owned(),
                version: stored.version,
                doc: stored.doc.clone(),
            })
    }

    pub(super) fn put(
        &mut self,
        scope: &str,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        now_ms: u64,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        let spec = self.spec(collection);
        let current = self.get(scope, collection, id, now_ms);
        check(precondition, current.as_ref(), collection, id)?;
        self.check_unique(&spec, scope, id, &doc, now_ms)?;
        let docs = self
            .docs
            .entry((scope.to_owned(), collection.to_owned()))
            .or_default();
        // Versions keep rising across expiry, so a stale CAS on a re-created
        // document still fails.
        let version = match docs.get(id) {
            None => Version::FIRST,
            Some(stored) => stored
                .version
                .next()
                .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
        };
        docs.insert(id.to_owned(), StoredDoc { version, doc });
        Ok(version)
    }

    pub(super) fn delete(
        &mut self,
        scope: &str,
        collection: &str,
        id: &str,
        precondition: Precondition,
        now_ms: u64,
    ) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let current = self.get(scope, collection, id, now_ms);
        check(precondition, current.as_ref(), collection, id)?;
        let removed = self
            .docs
            .get_mut(&(scope.to_owned(), collection.to_owned()))
            .and_then(|docs| docs.remove(id));
        Ok(removed.is_some() && current.is_some())
    }

    fn check_unique(
        &self,
        spec: &CollectionSpec,
        scope: &str,
        id: &str,
        doc: &Value,
        now_ms: u64,
    ) -> Result<()> {
        for index in spec.indexes.iter().filter(|index| index.unique) {
            let Some(key) = index_key(&index.fields, doc) else {
                continue;
            };
            let clash = self
                .live(scope, &spec.name, now_ms)
                .into_iter()
                .any(|(other, stored)| {
                    other != id
                        && index_key(&index.fields, &stored.doc).is_some_and(|theirs| {
                            theirs.iter().zip(&key).all(|(a, b)| value::equal(a, b))
                        })
                });
            if clash {
                return Err(StorageError::already_exists(format!(
                    "unique index `{}` on `{}` already holds this value",
                    index.name, spec.name
                )));
            }
        }
        Ok(())
    }

    /// Remove documents matching `filter`, returning how many.
    pub(super) fn delete_where(
        &mut self,
        scope: &str,
        collection: &str,
        filter: &Filter,
        now_ms: u64,
    ) -> u64 {
        let doomed: Vec<String> = self
            .live(scope, collection, now_ms)
            .into_iter()
            .filter(|(id, stored)| filter.matches(id, &stored.doc))
            .map(|(id, _)| id.clone())
            .collect();
        if let Some(docs) = self
            .docs
            .get_mut(&(scope.to_owned(), collection.to_owned()))
        {
            for id in &doomed {
                docs.remove(id);
            }
        }
        doomed.len() as u64
    }
}

/// The values of `fields` in `doc`, or `None` when any is missing.
fn index_key(fields: &[String], doc: &Value) -> Option<Vec<Value>> {
    fields
        .iter()
        .map(|field| value::lookup(doc, field).cloned())
        .collect()
}

/// Enforce a precondition against the live document, if any.
pub(super) fn check(
    precondition: Precondition,
    current: Option<&Versioned<Value>>,
    collection: &str,
    id: &str,
) -> Result<()> {
    match (precondition, current) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(found)) => Err(StorageError::conflict(format!(
            "`{collection}/{id}` already exists at version {}",
            found.version.0
        ))),
        (Precondition::Version(expected), Some(found)) if found.version == expected => Ok(()),
        (Precondition::Version(expected), found) => Err(StorageError::conflict(format!(
            "`{collection}/{id}` expected at version {}, found {}",
            expected.0,
            found.map_or_else(|| "nothing".to_owned(), |f| f.version.0.to_string())
        ))),
    }
}
