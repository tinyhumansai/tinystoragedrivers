//! [`DocumentStore`] for the file driver: one pretty-printed JSON file per
//! document.
//!
//! Queries, counts, deletes by filter and claims read every document of the
//! collection and evaluate with the core's [`Filter::matches`] and
//! [`sort_documents`](tinystoragedrivers_core::sort_documents), exactly as the
//! memory driver does. That is linear in the collection's size, which suits
//! the small per-user stores this driver is meant for.

mod store;

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, CollectionSpec, Cursor, DocumentStore, Filter, Page, Precondition, Query, Result,
    Scope, SearchHit, Sort, StorageError, Version, Versioned, validate_collection, validate_doc,
    value,
};

use crate::encode::fnv1a_64;
use crate::storage::{CAPABILITIES, Db};
use store::Docs;

/// File-backed documents bound to one scope.
#[derive(Debug, Clone)]
pub struct FileDocuments {
    db: Arc<Db>,
    scope: Scope,
}

impl FileDocuments {
    pub(crate) fn new(db: Arc<Db>, scope: Scope) -> Self {
        Self { db, scope }
    }

    /// Run `work` under the database lock with this scope's document view.
    async fn with<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Docs<'_>) -> Result<T> + Send + 'static,
    {
        let scope = self.scope.clone();
        self.db
            .run(move |db| {
                let docs = Docs {
                    db,
                    scope: &scope,
                    now: db.now(),
                };
                work(&docs)
            })
            .await
    }
}

/// A fingerprint of everything that decides a query's result order, so a
/// cursor only resumes the query that issued it. FNV-1a is stable across
/// processes, so a cursor survives a restart.
pub(crate) fn fingerprint(collection: &str, query: &Query) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(collection.as_bytes());
    // A separator no collection name contains keeps the parts apart.
    bytes.push(0);
    bytes.extend(serde_json::to_vec(&query.filter).unwrap_or_default());
    bytes.push(0);
    bytes.extend(serde_json::to_vec(&query.sort).unwrap_or_default());
    fnv1a_64(&bytes)
}

/// Decode an offset cursor issued by [`FileDocuments::query`] for this query.
fn parse_cursor(collection: &str, query: &Query) -> Result<usize> {
    let Some(cursor) = &query.cursor else {
        return Ok(0);
    };
    let expected = format!("file:{:016x}:", fingerprint(collection, query));
    cursor
        .0
        .strip_prefix(&expected)
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| StorageError::invalid_input("cursor was not issued for this query"))
}

#[async_trait]
impl DocumentStore for FileDocuments {
    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        let spec = spec.clone();
        self.with(move |docs| docs.ensure(&spec)).await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(move |docs| docs.get(&collection, &id)).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(move |docs| docs.put(&collection, &id, doc, precondition))
            .await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(move |docs| docs.delete(&collection, &id, precondition))
            .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        query.validate()?;
        let start = parse_cursor(collection, query)?;
        let fingerprint = fingerprint(collection, query);
        let (collection, query) = (collection.to_owned(), query.clone());
        self.with(move |docs| {
            let found = docs.matching(&collection, &query.filter, &query.sort)?;
            let end = query.limit.map_or(found.len(), |limit| {
                start.saturating_add(limit).min(found.len())
            });
            let items = found.get(start..end).map(<[_]>::to_vec).unwrap_or_default();
            let next =
                (end < found.len()).then(|| Cursor(format!("file:{fingerprint:016x}:{end}")));
            Ok(Page { items, next })
        })
        .await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(move |docs| Ok(docs.matching(&collection, &filter, &[])?.len() as u64))
            .await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(move |docs| docs.delete_where(&collection, &filter))
            .await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_doc(patch)?;
        let (collection, filter, sort, patch) = (
            collection.to_owned(),
            filter.clone(),
            sort.to_vec(),
            patch.clone(),
        );
        self.with(move |docs| {
            let Some(first) = docs
                .matching(&collection, &filter, &sort)?
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, &patch);
            let version = docs.put(
                &collection,
                &first.id,
                doc.clone(),
                Precondition::Version(first.version),
            )?;
            Ok(Some(Versioned {
                id: first.id,
                version,
                doc,
            }))
        })
        .await
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        let (collection, text) = (collection.to_owned(), text.to_owned());
        self.with(move |docs| {
            let spec = docs.spec(&collection)?;
            let Some(search) = spec.search.clone() else {
                return Err(StorageError::invalid_input(
                    "this collection declares no search fields",
                ));
            };
            let wanted = value::tokens(&Value::String(text));
            if wanted.is_empty() {
                return Ok(Vec::new());
            }
            let mut hits: Vec<SearchHit> = docs
                .live(&spec)?
                .into_iter()
                .filter_map(|stored| {
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
                    (score > 0.0).then_some(SearchHit {
                        id: stored.id,
                        score,
                    })
                })
                .collect();
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
            hits.truncate(limit);
            Ok(hits)
        })
        .await
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        let collection = collection.to_owned();
        self.with(move |docs| docs.drop_collection(&collection))
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
