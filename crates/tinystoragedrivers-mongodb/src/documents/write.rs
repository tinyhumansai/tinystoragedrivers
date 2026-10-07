//! Writes: put, delete, and removal into tombstones.
//!
//! A write reads the stored row by `_id`, applies the precondition and expiry
//! rules in Rust ([`stored`](super::stored)), then commits with a
//! compare-and-swap on `_v` (or an insert, which the `_id` index makes
//! exclusive). A lost race re-reads and tries again; inside a transaction the
//! snapshot makes a lost race a conflict instead.
//!
//! Removal never forgets a version: the row stays as a tombstone (`_del:
//! true`, empty body) at its last version. Recreating the id is then an
//! ordinary compare-and-swap on that same row, so allocating the next version
//! and writing the document are one atomic single-document step, and a
//! compare-and-swap prepared before the deletion fails. Reads, counts, claims
//! and search exclude tombstones, and their empty body leaves every unique
//! and text index.

use mongodb::ClientSession;
use mongodb::bson::{Document, doc};
use serde_json::Value;
use tinystoragedrivers_core::{
    Precondition, Result, StorageError, Version, validate_collection, validate_doc, validate_id,
};

use super::MongoDocuments;
use super::stored::{
    Stored, at_version, bury, check, encode, expired_filter, is_live, next_version, not_deleted,
};
use crate::errors;
use crate::scoped::ScopedCollection;

/// How many times a write retries after losing a race.
const WRITE_ATTEMPTS: usize = 32;

/// How many documents one removal round handles.
const REMOVE_CHUNK: usize = 256;

/// The error for a write that kept losing races.
fn contended(in_transaction: bool) -> StorageError {
    if in_transaction {
        StorageError::conflict("document changed inside the transaction")
    } else {
        StorageError::unavailable("document kept changing concurrently; retry")
    }
}

impl MongoDocuments {
    /// Read the stored row `id`: live, expired, or a tombstone.
    pub(super) async fn read_raw(
        &self,
        collection: &ScopedCollection,
        id: &str,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<Stored>> {
        collection
            .find_one(
                doc! {"_id": crate::naming::document_id(&self.scope, id)},
                session,
            )
            .await
            .map_err(errors::failed("read a document"))?
            .as_ref()
            .map(Stored::decode)
            .transpose()
    }

    pub(super) async fn put_one(
        &self,
        collection: &str,
        id: &str,
        doc: &Value,
        precondition: Precondition,
        mut session: Option<&mut ClientSession>,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(doc)?;
        let body = doc.as_object().cloned().unwrap_or_default();
        let spec = self.shared.spec(collection).await?;
        self.prepare(collection).await?;
        let now = self.shared.now();
        let handle = self.collection(collection);
        if spec.ttl_field.is_some() && spec.indexes.iter().any(|index| index.unique) {
            // An expired document still occupies the unique index; the memory
            // driver ignores it, so bury it first.
            self.sweep(collection, session.as_deref_mut()).await?;
        }
        let in_transaction = session.is_some();
        let attempts = if in_transaction { 1 } else { WRITE_ATTEMPTS };
        for _ in 0..attempts {
            let current = self.read_raw(&handle, id, session.as_deref_mut()).await?;
            let live = current
                .as_ref()
                .filter(|stored| is_live(&spec, stored, now))
                .map(|stored| stored.version);
            check(precondition, live)?;
            let next = next_version(current.as_ref().map(|stored| stored.version))?;
            let encoded = encode(&self.scope, id, next, &body)?;
            let outcome = match &current {
                None => handle
                    .insert_one(encoded, session.as_deref_mut())
                    .await
                    .map(|()| true),
                Some(stored) => {
                    handle
                        .replace_one(
                            at_version(&self.scope, id, stored.version),
                            encoded,
                            session.as_deref_mut(),
                        )
                        .await
                }
            };
            match outcome {
                Ok(true) => return Ok(next),
                Ok(false) => {}
                Err(error) => match errors::duplicate_index(&error) {
                    // Another writer created the document first.
                    Some(index) if index == "_id_" => {}
                    _ => return Err(errors::map(error, "write a document")),
                },
            }
        }
        Err(contended(in_transaction))
    }

    pub(super) async fn delete_one(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
        mut session: Option<&mut ClientSession>,
    ) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.shared.spec(collection).await?;
        let now = self.shared.now();
        let handle = self.collection(collection);
        let in_transaction = session.is_some();
        let attempts = if in_transaction { 1 } else { WRITE_ATTEMPTS };
        for _ in 0..attempts {
            let current = self.read_raw(&handle, id, session.as_deref_mut()).await?;
            let live = current
                .as_ref()
                .filter(|stored| is_live(&spec, stored, now))
                .map(|stored| stored.version);
            check(precondition, live)?;
            // An expired document is removed too; a tombstone already is.
            let Some(current) = current.filter(|stored| !stored.deleted) else {
                return Ok(false);
            };
            let pairs = [(current.key, current.version)];
            if self
                .remove(collection, &pairs, session.as_deref_mut())
                .await?
                == 1
            {
                return Ok(live.is_some());
            }
        }
        Err(contended(in_transaction))
    }

    /// Turn the documents at exactly these `(key, version)`s into tombstones
    /// and report how many went. A document that changed since it was read is
    /// left alone.
    pub(super) async fn remove(
        &self,
        collection: &str,
        pairs: &[(String, Version)],
        mut session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        let handle = self.collection(collection);
        let mut removed = 0;
        for chunk in pairs.chunks(REMOVE_CHUNK) {
            let selector = doc! {"$and": [
                not_deleted(),
                {"$or": chunk
                    .iter()
                    .map(|(key, version)| at_version(&self.scope, key, *version))
                    .collect::<Vec<Document>>()},
            ]};
            removed += handle
                .update_many(selector, bury(), session.as_deref_mut())
                .await
                .map_err(errors::failed("remove documents"))?;
        }
        Ok(removed)
    }

    /// Bury this scope's expired documents of `collection`.
    pub(crate) async fn sweep(
        &self,
        collection: &str,
        session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        let spec = self.shared.spec(collection).await?;
        let Some(expired) = expired_filter(&spec, self.shared.now()) else {
            return Ok(0);
        };
        let pairs = self.matching_pairs(collection, &expired).await?;
        self.remove(collection, &pairs, session).await
    }
}
