//! Writes: put, delete, and removal with tombstones.
//!
//! A write reads the stored document by `_id`, applies the precondition and
//! expiry rules in Rust ([`stored`](super::stored)), then commits with a
//! compare-and-swap on `_v` (or an insert, which the `_id` index makes
//! exclusive). A lost race re-reads and tries again; inside a transaction the
//! snapshot makes a lost race a conflict instead.
//!
//! Removal never forgets a version. Before a document goes, its `(key, _v)`
//! is folded into `_tsd_tombstones` with `$max`, so a recreated id continues
//! from there and a compare-and-swap prepared before the deletion fails.
//! The tombstone is written first: a crash in between leaves a live document
//! and a tombstone at its current version, which is harmless.

use mongodb::ClientSession;
use mongodb::bson::{Document, doc};
use serde_json::Value;
use tinystoragedrivers_core::{
    Precondition, Result, StorageError, Version, validate_collection, validate_doc, validate_id,
};

use super::MongoDocuments;
use super::stored::{Stored, at_version, check, encode, expired_filter, is_live, next_version};
use crate::errors;
use crate::naming::{SCOPE, TOMBSTONES, VERSION, tombstone_id};
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
    /// Read the stored document `id`, live or expired.
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

    /// The last version of a removed document, if any.
    async fn buried(
        &self,
        collection: &str,
        id: &str,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<Version>> {
        let tombstone = self
            .shared
            .scoped(TOMBSTONES, &self.scope)
            .find_one(
                doc! {"_id": tombstone_id(collection, &self.scope, id)},
                session,
            )
            .await
            .map_err(errors::failed("read a tombstone"))?;
        Ok(tombstone
            .and_then(|tombstone| tombstone.get_i64(VERSION).ok())
            .and_then(|version| u64::try_from(version).ok())
            .map(Version))
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
            // driver ignores it, so remove it first.
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
            let last = match &current {
                Some(stored) => Some(stored.version),
                None => self.buried(collection, id, session.as_deref_mut()).await?,
            };
            let next = next_version(last)?;
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
            let Some(current) = current else {
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

    /// Remove the documents at exactly these `(key, version)`s, recording
    /// tombstones first, and report how many went. A document that changed
    /// since it was read is left alone.
    pub(super) async fn remove(
        &self,
        collection: &str,
        pairs: &[(String, Version)],
        mut session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        let handle = self.collection(collection);
        let tombstones = self.shared.scoped(TOMBSTONES, &self.scope);
        let mut removed = 0;
        for chunk in pairs.chunks(REMOVE_CHUNK) {
            let selector = doc! {"$or": chunk
            .iter()
            .map(|(key, version)| at_version(&self.scope, key, *version))
            .collect::<Vec<Document>>()};
            if session.is_some() || chunk.len() == 1 {
                for (key, version) in chunk {
                    let version = i64::try_from(version.0).unwrap_or(i64::MAX);
                    tombstones
                        .update_one(
                            doc! {"_id": tombstone_id(collection, &self.scope, key)},
                            doc! {"$max": {VERSION: version}},
                            true,
                            session.as_deref_mut(),
                        )
                        .await
                        .map_err(errors::failed("record a tombstone"))?;
                }
            } else {
                // One server-side pass: `$merge` cannot run in a transaction,
                // which is why the branch above exists.
                let stages = vec![
                    doc! {"$project": {
                        "_id": {"c": {"$literal": collection}, "s": format!("${SCOPE}"), "k": "$_key"},
                        SCOPE: 1,
                        VERSION: 1,
                    }},
                    doc! {"$merge": {
                        "into": tombstones.name(),
                        "on": "_id",
                        "whenMatched": [{"$set": {VERSION: {"$max": [format!("${VERSION}"), format!("$$new.{VERSION}")]}}}],
                        "whenNotMatched": "insert",
                    }},
                ];
                handle
                    .aggregate(selector.clone(), stages)
                    .await
                    .map_err(errors::failed("record tombstones"))?;
            }
            removed += handle
                .delete_many(selector, session.as_deref_mut())
                .await
                .map_err(errors::failed("remove documents"))?;
        }
        Ok(removed)
    }

    /// Remove this scope's expired documents of `collection`.
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
