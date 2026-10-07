//! [`DocumentStore`] for the in-memory driver.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::state::DbState;
use super::{Clock, MemoryDb};
use crate::capabilities::Capabilities;
use crate::document::{
    CollectionSpec, Cursor, DocumentStore, Page, Precondition, Query, SearchHit, Version,
    Versioned, WriteOp, WriteResult, validate_collection, validate_doc,
};
use crate::error::{Result, StorageError};
use crate::filter::{Filter, Sort, sort_documents};
use crate::scope::Scope;
use crate::value;

/// In-memory documents bound to one scope.
#[derive(Clone)]
pub struct MemoryDocuments {
    db: Arc<MemoryDb>,
    scope: Scope,
    clock: Clock,
}

impl std::fmt::Debug for MemoryDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryDocuments")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl MemoryDocuments {
    pub(super) fn new(db: Arc<MemoryDb>, scope: Scope, clock: Clock) -> Self {
        Self { db, scope, clock }
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    fn matching(
        &self,
        state: &DbState,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
    ) -> Result<Vec<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        let mut found: Vec<Versioned<Value>> = state
            .live(self.scope.as_str(), collection, self.now())
            .into_iter()
            .filter(|(id, stored)| filter.matches(id, &stored.doc))
            .map(|(id, stored)| Versioned {
                id: id.clone(),
                version: stored.version,
                doc: stored.doc.clone(),
            })
            .collect();
        sort_documents(&mut found, sort, |item| (item.id.as_str(), &item.doc));
        Ok(found)
    }
}

/// A fingerprint of everything that decides a query's result order, so a
/// cursor only resumes the query that issued it.
pub(super) fn fingerprint(collection: &str, query: &Query) -> u64 {
    let mut hasher = DefaultHasher::new();
    collection.hash(&mut hasher);
    serde_json::to_string(&query.filter)
        .unwrap_or_default()
        .hash(&mut hasher);
    serde_json::to_string(&query.sort)
        .unwrap_or_default()
        .hash(&mut hasher);
    hasher.finish()
}

/// Decode an offset cursor issued by [`MemoryDocuments::query`] for this query.
fn parse_cursor(collection: &str, query: &Query) -> Result<usize> {
    let Some(cursor) = &query.cursor else {
        return Ok(0);
    };
    let expected = format!("mem:{:016x}:", fingerprint(collection, query));
    cursor
        .0
        .strip_prefix(&expected)
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| StorageError::invalid_input("cursor was not issued for this query"))
}

#[async_trait]
impl DocumentStore for MemoryDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let mut state = self.db.lock()?;
        let merged = match state.specs.get(&spec.name) {
            Some(existing) => existing.merge(spec)?,
            None => spec.clone(),
        };
        state.check_existing_unique(&merged, self.now())?;
        state.specs.insert(spec.name.clone(), merged);
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        crate::document::validate_id(id)?;
        let state = self.db.lock()?;
        Ok(state.get(self.scope.as_str(), collection, id, self.now()))
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let now = self.now();
        let mut state = self.db.lock()?;
        state.put(self.scope.as_str(), collection, id, doc, precondition, now)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let now = self.now();
        let mut state = self.db.lock()?;
        state.delete(self.scope.as_str(), collection, id, precondition, now)
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        query.validate()?;
        let start = parse_cursor(collection, query)?;
        let state = self.db.lock()?;
        let found = self.matching(&state, collection, &query.filter, &query.sort)?;
        let end = query.limit.map_or(found.len(), |limit| {
            start.saturating_add(limit).min(found.len())
        });
        let items = found.get(start..end).map(<[_]>::to_vec).unwrap_or_default();
        let next = (end < found.len())
            .then(|| Cursor(format!("mem:{:016x}:{end}", fingerprint(collection, query))));
        Ok(Page { items, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let state = self.db.lock()?;
        Ok(self.matching(&state, collection, filter, &[])?.len() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let now = self.now();
        let mut state = self.db.lock()?;
        Ok(state.delete_where(self.scope.as_str(), collection, filter, now))
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_doc(patch)?;
        let now = self.now();
        let mut state = self.db.lock()?;
        let Some(first) = self
            .matching(&state, collection, filter, sort)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let mut doc = first.doc;
        value::merge_patch(&mut doc, patch);
        let version = state.put(
            self.scope.as_str(),
            collection,
            &first.id,
            doc.clone(),
            Precondition::Version(first.version),
            now,
        )?;
        Ok(Some(Versioned {
            id: first.id,
            version,
            doc,
        }))
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        let now = self.now();
        let mut state = self.db.lock()?;
        // A batch only writes documents; copying the rest would cost time
        // proportional to every stored stream and blob.
        let mut draft = DbState {
            specs: state.specs.clone(),
            docs: state.docs.clone(),
            tombstones: state.tombstones.clone(),
            ..DbState::default()
        };
        let scope = self.scope.as_str();
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            results.push(match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    precondition,
                } => WriteResult::Put {
                    version: draft.put(scope, &collection, &id, doc, precondition, now)?,
                },
                WriteOp::Delete {
                    collection,
                    id,
                    precondition,
                } => WriteResult::Delete {
                    removed: draft.delete(scope, &collection, &id, precondition, now)?,
                },
            });
        }
        state.docs = draft.docs;
        state.tombstones = draft.tombstones;
        Ok(results)
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        let state = self.db.lock()?;
        let spec = state.spec(collection);
        let Some(search) = spec.search else {
            return Err(StorageError::invalid_input(
                "this collection declares no search fields",
            ));
        };
        let wanted = value::tokens(&Value::String(text.to_owned()));
        if wanted.is_empty() {
            return Ok(Vec::new());
        }
        let mut hits: Vec<SearchHit> = state
            .live(self.scope.as_str(), collection, self.now())
            .into_iter()
            .filter_map(|(id, stored)| {
                let have: Vec<String> = search
                    .fields
                    .iter()
                    .filter_map(|field| value::lookup(&stored.doc, field))
                    .flat_map(value::tokens)
                    .collect();
                let score = wanted.iter().filter(|token| have.contains(token)).count();
                // Token counts are tiny; the cast is exact.
                #[allow(clippy::cast_precision_loss)]
                let score = score as f64;
                (score > 0.0).then(|| SearchHit {
                    id: id.clone(),
                    score,
                })
            })
            .collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        hits.truncate(limit);
        Ok(hits)
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        let mut state = self.db.lock()?;
        let key = (self.scope.as_str().to_owned(), collection.to_owned());
        let ids: Vec<String> = state
            .docs
            .get(&key)
            .map(|docs| docs.keys().cloned().collect())
            .unwrap_or_default();
        for id in ids {
            state.remove(self.scope.as_str(), collection, &id);
        }
        Ok(())
    }
}
