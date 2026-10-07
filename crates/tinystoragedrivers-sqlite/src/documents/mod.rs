//! [`DocumentStore`] on SQLite.
//!
//! All collections of a database share one `WITHOUT ROWID` table keyed by
//! `(scope, collection, id)`, with the body stored as JSON text. Declared
//! indexes become partial expression indexes over `json_extract`, and declared
//! search fields are mirrored into an FTS5 table. Filters push their exact
//! string and boolean equalities down to SQL and are re-checked in Rust with
//! [`Filter::matches`], so results agree with the memory driver.
//!
//! Every write runs in an immediate transaction under the shared connection,
//! which makes preconditions, unique indexes, claims and batches atomic.

mod ops;
mod pushdown;

use std::fmt;
use std::sync::Arc;

use rusqlite::{Connection, TransactionBehavior};
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, CollectionSpec, Cursor, DocumentStore, Filter, Page, Precondition, Query, Result,
    Scope, SearchHit, Sort, StorageError, Version, Versioned, WriteOp, WriteResult, async_trait,
    validate_doc, value,
};

use crate::connection::Db;
use crate::error::during;
use crate::sql::Tables;
use crate::storage::Clock;

use ops::Ctx;

/// What this driver supports.
pub(crate) fn capabilities() -> Capabilities {
    Capabilities::all()
}

/// SQLite documents bound to one scope.
#[derive(Clone)]
pub struct SqliteDocuments {
    db: Arc<Db>,
    tables: Arc<Tables>,
    scope: Scope,
    clock: Clock,
}

impl fmt::Debug for SqliteDocuments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteDocuments")
            .field("scope", &self.scope)
            .field("path", &self.db.path())
            .finish_non_exhaustive()
    }
}

impl SqliteDocuments {
    pub(crate) fn new(db: Arc<Db>, tables: Arc<Tables>, scope: Scope, clock: Clock) -> Self {
        Self {
            db,
            tables,
            scope,
            clock,
        }
    }

    /// Run `f` with a context for this handle, inside an immediate
    /// transaction when `write` is set.
    async fn with<T, F>(&self, write: bool, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, Ctx<'_>) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let now_ms = (self.clock)();
        self.db
            .run(move |conn| {
                let ctx = Ctx {
                    tables: &tables,
                    scope: scope.as_str(),
                    now_ms,
                };
                if !write {
                    return f(conn, ctx);
                }
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(during("begin"))?;
                let out = f(&tx, ctx)?;
                tx.commit().map_err(during("commit"))?;
                Ok(out)
            })
            .await
    }
}

/// FNV-1a: a stable hash, so a cursor stays valid across processes.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A fingerprint of what decides a query's result order.
fn fingerprint(collection: &str, query: &Query) -> Result<u64> {
    let shape = serde_json::to_string(&(collection, &query.filter, &query.sort))?;
    Ok(fnv1a(shape.as_bytes()))
}

fn parse_cursor(collection: &str, query: &Query) -> Result<usize> {
    let Some(cursor) = &query.cursor else {
        return Ok(0);
    };
    let expected = format!("sqlite:{:016x}:", fingerprint(collection, query)?);
    cursor
        .0
        .strip_prefix(&expected)
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| StorageError::invalid_input("cursor was not issued for this query"))
}

#[async_trait]
impl DocumentStore for SqliteDocuments {
    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let spec = spec.clone();
        self.with(true, move |conn, ctx| {
            ops::declare(conn, ctx.tables, &spec, ctx.now_ms)
        })
        .await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(false, move |conn, ctx| {
            ops::get(conn, ctx, &collection, &id)
        })
        .await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(true, move |conn, ctx| {
            ops::put(conn, ctx, &collection, &id, &doc, precondition)
        })
        .await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(true, move |conn, ctx| {
            ops::delete(conn, ctx, &collection, &id, precondition)
        })
        .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        query.validate()?;
        let start = parse_cursor(collection, query)?;
        let fingerprint = fingerprint(collection, query)?;
        let (collection, query) = (collection.to_owned(), query.clone());
        self.with(false, move |conn, ctx| {
            let found = ops::matching(conn, ctx, &collection, &query.filter, &query.sort)?;
            let end = query.limit.map_or(found.len(), |limit| {
                start.saturating_add(limit).min(found.len())
            });
            let items = found.get(start..end).map(<[_]>::to_vec).unwrap_or_default();
            let next =
                (end < found.len()).then(|| Cursor(format!("sqlite:{fingerprint:016x}:{end}")));
            Ok(Page { items, next })
        })
        .await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(false, move |conn, ctx| {
            Ok(ops::matching(conn, ctx, &collection, &filter, &[])?.len() as u64)
        })
        .await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(true, move |conn, ctx| {
            ops::delete_where(conn, ctx, &collection, &filter)
        })
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
        self.with(true, move |conn, ctx| {
            let Some(first) = ops::matching(conn, ctx, &collection, &filter, &sort)?
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, &patch);
            let version = ops::put(
                conn,
                ctx,
                &collection,
                &first.id,
                &doc,
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

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.with(true, move |conn, ctx| {
            ops.into_iter()
                .map(|op| match op {
                    WriteOp::Put {
                        collection,
                        id,
                        doc,
                        precondition,
                    } => Ok(WriteResult::Put {
                        version: ops::put(conn, ctx, &collection, &id, &doc, precondition)?,
                    }),
                    WriteOp::Delete {
                        collection,
                        id,
                        precondition,
                    } => Ok(WriteResult::Delete {
                        removed: ops::delete(conn, ctx, &collection, &id, precondition)?,
                    }),
                })
                .collect()
        })
        .await
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let (collection, text) = (collection.to_owned(), text.to_owned());
        let hits = self
            .with(false, move |conn, ctx| {
                ops::search(conn, ctx, &collection, &text, limit)
            })
            .await?;
        Ok(hits
            .into_iter()
            .map(|(id, score)| SearchHit { id, score })
            .collect())
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        let collection = collection.to_owned();
        self.with(true, move |conn, ctx| {
            ops::drop_collection(conn, ctx, &collection)
        })
        .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
