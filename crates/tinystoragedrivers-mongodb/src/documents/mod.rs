//! [`DocumentStore`] on MongoDB.
//!
//! A port collection `jobs` is the Mongo collection `<prefix>jobs`, holding
//! `{_id: {s, k}, _scope, _key, _v, d}` per document. The `_id` makes each
//! `(scope, key)` unique without any declared index, and every access goes
//! through [`ScopedCollection`], which ANDs `_scope` into the query.
//!
//! - [`stored`]: the stored shape and the precondition, version and expiry
//!   rules, all pure.
//! - [`write`]: put, delete and removal with tombstones.
//! - [`read`]: queries, counts and search, server-side where the translation
//!   is exact and in Rust where it is not.
//!
//! Claims pick the first match and commit it with a compare-and-swap on its
//! version, retrying when another claimer won. The merge patch is applied in
//! Rust with [`value::merge_patch`], so RFC 7396 edge cases (a patch object
//! replacing a scalar) behave exactly as on the memory driver.

mod read;
pub(crate) mod stored;
mod write;

use std::sync::Arc;

use async_trait::async_trait;
use mongodb::ClientSession;
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, DocumentStore, ErrorKind, Filter, Page, Precondition,
    Query, Result, Scope, SearchHit, Sort, StorageError, Version, Versioned, WriteOp, WriteResult,
    validate_collection, validate_doc, validate_id, value,
};

use crate::backend::{Shared, specs::scope_key_model};
use crate::errors;
use crate::naming::{decode_cursor, encode_cursor};
use crate::scoped::ScopedCollection;

/// How many times a claim retries after another claimer took its pick.
const CLAIM_ATTEMPTS: usize = 64;

/// How many times a batch retries a transaction the server aborted as
/// transient.
const TRANSACTION_ATTEMPTS: usize = 8;

/// What a failed commit means for the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitFailure {
    /// The server aborted the transaction: running the batch again is safe.
    Aborted,
    /// The commit may have applied. Replaying could apply the batch twice,
    /// so the caller gets a non-retryable error and must read first.
    Unknown,
    /// Any other failure, which did not commit.
    Failed,
}

/// Classify a commit failure by its labels. An outcome the driver could not
/// learn (even after retrying the commit itself) wins over a transient label,
/// because only an abort guarantees nothing was applied.
pub(crate) fn commit_failure(error: &mongodb::error::Error) -> CommitFailure {
    if errors::is_unknown_commit(error) {
        CommitFailure::Unknown
    } else if errors::is_transient(error) {
        CommitFailure::Aborted
    } else {
        CommitFailure::Failed
    }
}

/// MongoDB documents bound to one scope.
pub(crate) struct MongoDocuments {
    shared: Arc<Shared>,
    scope: Scope,
}

impl std::fmt::Debug for MongoDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDocuments")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl MongoDocuments {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope) -> Self {
        Self { shared, scope }
    }

    fn collection(&self, collection: &str) -> ScopedCollection {
        self.shared.scoped(collection, &self.scope)
    }

    async fn prepare(&self, collection: &str) -> Result<()> {
        self.shared
            .prepare(collection, vec![scope_key_model()])
            .await
    }

    async fn run_batch(
        &self,
        ops: &[WriteOp],
        session: &mut ClientSession,
    ) -> Result<Vec<WriteResult>> {
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            results.push(match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    precondition,
                } => WriteResult::Put {
                    version: self
                        .put_one(collection, id, doc, *precondition, Some(&mut *session))
                        .await?,
                },
                WriteOp::Delete {
                    collection,
                    id,
                    precondition,
                } => WriteResult::Delete {
                    removed: self
                        .delete_one(collection, id, *precondition, Some(&mut *session))
                        .await?,
                },
            });
        }
        Ok(results)
    }

    /// Commit, retrying a commit whose outcome the server could not report.
    async fn commit(session: &mut ClientSession) -> std::result::Result<(), mongodb::error::Error> {
        let mut outcome = session.commit_transaction().await;
        for _ in 0..TRANSACTION_ATTEMPTS {
            match &outcome {
                Err(error) if errors::is_unknown_commit(error) => {
                    outcome = session.commit_transaction().await;
                }
                _ => break,
            }
        }
        outcome
    }
}

#[async_trait]
impl DocumentStore for MongoDocuments {
    fn capabilities(&self) -> Capabilities {
        let base = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Ttl);
        if self.shared.transactions {
            base.with(Capability::Transactions)
        } else {
            base
        }
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        self.shared.ensure(spec).await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.shared.spec(collection).await?;
        let now = self.shared.now();
        Ok(self
            .read_raw(&self.collection(collection), id, None)
            .await?
            .filter(|found| stored::is_live(&spec, found, now))
            .map(stored::Stored::into_versioned))
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_one(collection, id, &doc, precondition, None).await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        self.delete_one(collection, id, precondition, None).await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        query.validate()?;
        validate_collection(collection)?;
        let offset = decode_cursor(collection, query)?;
        let (items, more) = self
            .page(collection, &query.filter, &query.sort, offset, query.limit)
            .await?;
        let next = more.then(|| encode_cursor(collection, query, offset + items.len() as u64));
        Ok(Page { items, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        self.count_matching(collection, filter).await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let pairs = self.live_pairs(collection, filter).await?;
        self.remove(collection, &pairs, None).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_doc(patch)?;
        validate_collection(collection)?;
        filter.validate()?;
        for _ in 0..CLAIM_ATTEMPTS {
            let (first, _) = self.page(collection, filter, sort, 0, Some(1)).await?;
            let Some(first) = first.into_iter().next() else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, patch);
            match self
                .put_one(
                    collection,
                    &first.id,
                    &doc,
                    Precondition::Version(first.version),
                    None,
                )
                .await
            {
                Ok(version) => {
                    return Ok(Some(Versioned {
                        id: first.id,
                        version,
                        doc,
                    }));
                }
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::unavailable(
            "every claim attempt lost to a concurrent claimer; retry",
        ))
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::Transactions,
                "this MongoDB deployment is not a replica set or sharded cluster",
            ));
        }
        // Index builds cannot run inside a transaction: do them first.
        for op in &ops {
            let (WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. }) = op;
            validate_collection(collection)?;
            self.prepare(collection).await?;
            self.shared.spec(collection).await?;
        }
        let mut session = self
            .shared
            .client
            .start_session()
            .await
            .map_err(errors::failed("start a session"))?;
        let mut last = StorageError::unavailable("transaction kept aborting; retry");
        for _ in 0..TRANSACTION_ATTEMPTS {
            session
                .start_transaction()
                .await
                .map_err(errors::failed("start a transaction"))?;
            let outcome = self.run_batch(&ops, &mut session).await;
            let results = match outcome {
                Ok(results) => results,
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    if error.is_retryable() {
                        last = error;
                        continue;
                    }
                    return Err(error);
                }
            };
            match Self::commit(&mut session).await {
                Ok(()) => return Ok(results),
                Err(error) => match commit_failure(&error) {
                    CommitFailure::Aborted => last = errors::map(error, "commit a transaction"),
                    CommitFailure::Unknown => {
                        return Err(StorageError::backend(
                            "the transaction may or may not have committed; read before retrying",
                        )
                        .with_source(error));
                    }
                    CommitFailure::Failed => {
                        return Err(errors::map(error, "commit a transaction"));
                    }
                },
            }
        }
        Err(last)
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        self.search_hits(collection, text, limit).await
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        let pairs = self.matching_pairs(collection, &Filter::All).await?;
        self.remove(collection, &pairs, None).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
