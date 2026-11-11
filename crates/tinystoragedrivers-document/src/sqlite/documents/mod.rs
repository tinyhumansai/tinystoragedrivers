//! [`DocumentStore`] on SQLite.
//!
//! All collections of a database share one `WITHOUT ROWID` table keyed by
//! `(scope, collection, id)`, with the body stored as JSON text (or, over 4 KiB,
//! a zstd blob, see `body.rs`) and the record
//! metadata (`schema`, `created_ms`, `updated_ms`, `expires_ms`) in columns of
//! their own. A deleted document keeps its row (`deleted = 1`, body `{}`) so
//! its version continues if the id is written again; every read skips it. Declared
//! index and search fields are projected into the `doc_fields` side table
//! (`fields.rs`), and search fields are mirrored into an FTS5 table. Filters push their exact
//! string and boolean equalities down to SQL and are re-checked in Rust with
//! [`Filter::matches_with`], so results agree with the memory driver.
//!
//! Every write runs in an immediate transaction under the shared connection,
//! which makes preconditions, unique indexes, claims and batches atomic.

pub(crate) mod ops;
mod order;
mod ordered;
mod pushdown;

use rusqlite::Connection;
use serde_json::Value;
use std::fmt;
use tinystoragedrivers_api::{DocumentStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, Filter, Page, Precondition,
    Query, ReadCheck, Result, Scope, SearchHit, Sort, StorageError, Version, Versioned,
    WriteCondition, WriteOp, WriteOptions, WriteResult, validate_collection, validate_doc,
    validate_id, value,
};

use tinystoragedrivers_connect::sqlite::SqliteConnection;

use ops::Ctx;

/// What this driver supports.
pub(crate) fn capabilities() -> Capabilities {
    Capabilities::none()
        .with(Capability::Documents)
        .with(Capability::FullText)
        .with(Capability::Transactions)
        .with(Capability::ValidatedBatch)
        .with(Capability::Ttl)
        .with(Capability::CollectionEnumeration)
        .with(Capability::CollectionValidation)
        .with(Capability::RecordMetadata)
        .with(Capability::SharedAcrossProcesses)
        .with(Capability::CrossProcessCas)
}

/// SQLite documents bound to one scope.
#[derive(Clone)]
pub struct SqliteDocuments {
    connection: SqliteConnection,
    scope: Scope,
    condition: Option<WriteCondition>,
}

impl fmt::Debug for SqliteDocuments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteDocuments")
            .field("scope", &self.scope)
            .field("path", &self.connection.path())
            .finish_non_exhaustive()
    }
}

/// How a call touches the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    /// A plain read.
    Read,
    /// A schema declaration: transactional.
    Declare,
    /// A data write: transactional.
    Write,
}

impl SqliteDocuments {
    /// Bind a document adapter to a shared SQLite connection and opaque scope.
    #[must_use]
    pub fn new(connection: SqliteConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope,
            condition: None,
        }
    }

    /// Bind a document adapter whose mutations check `condition` in their
    /// SQLite write transaction.
    #[must_use]
    pub fn conditioned(
        connection: SqliteConnection,
        scope: Scope,
        condition: WriteCondition,
    ) -> Self {
        Self {
            connection,
            scope,
            condition: Some(condition),
        }
    }

    /// Run `f` with a context for this handle, inside an immediate
    /// transaction unless it only reads.
    async fn with<T, F>(&self, access: Access, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, Ctx<'_>) -> Result<T> + Send + 'static,
    {
        let tables = self.connection.tables().clone();
        let scope = self.scope.clone();
        let condition = self.condition.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |conn| {
                let now_ms = clock();
                let ctx = Ctx {
                    tables: &tables,
                    scope: scope.as_str(),
                    now_ms,
                };
                if access == Access::Read {
                    let tx = conn.transaction().map_err(
                        tinystoragedrivers_connect::sqlite::error::during("start read transaction"),
                    )?;
                    let value = f(&tx, ctx)?;
                    tx.commit()
                        .map_err(tinystoragedrivers_connect::sqlite::error::during(
                            "finish read transaction",
                        ))?;
                    return Ok(value);
                }
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "start write transaction",
                    ))?;
                if access == Access::Write
                    && let Some(condition) = &condition
                {
                    let condition_now_ms = clock();
                    tables.check_write_condition(
                        &tx,
                        condition,
                        i64::try_from(condition_now_ms).unwrap_or(i64::MAX),
                    )?;
                }
                let value = f(&tx, ctx)?;
                tx.commit()
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "finish write transaction",
                    ))?;
                Ok(value)
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
fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> Result<u64> {
    let shape = serde_json::to_string(&(collection, scope.as_str(), &query.filter, &query.sort))?;
    Ok(fnv1a(shape.as_bytes()))
}

fn parse_cursor(collection: &str, scope: &Scope, query: &Query) -> Result<Option<CursorPosition>> {
    let Some(cursor) = &query.cursor else {
        return Ok(None);
    };
    let position = cursor.decode_keyset("sqlite", fingerprint(collection, scope, query)?)?;
    position.validate(&query.sort)?;
    Ok(Some(position))
}

#[async_trait]
impl DocumentStore for SqliteDocuments {
    fn capabilities(&self) -> Capabilities {
        let capabilities = capabilities();
        self.condition.as_ref().map_or(capabilities, |_| {
            capabilities.with(Capability::ConditionalWrites)
        })
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let spec = spec.clone();
        self.with(Access::Declare, move |conn, ctx| {
            ops::declare(conn, ctx.tables, &spec, ctx.now_ms)
        })
        .await
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let required = required.clone();
        self.with(Access::Read, move |conn, ctx| {
            ops::validate_spec(conn, ctx.tables, &required)
        })
        .await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Read, move |conn, ctx| {
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
        self.put_with(collection, id, doc, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Write, move |conn, ctx| {
            ops::put_with(conn, ctx, &collection, &id, &doc, precondition, options)
        })
        .await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Write, move |conn, ctx| {
            ops::delete(conn, ctx, &collection, &id, precondition)
        })
        .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let position = parse_cursor(collection, &self.scope, query)?;
        let fingerprint = fingerprint(collection, &self.scope, query)?;
        let (collection, query) = (collection.to_owned(), query.clone());
        self.with(Access::Read, move |conn, ctx| {
            let found = ops::matching_after(
                conn,
                ctx,
                &collection,
                &query.filter,
                &query.sort,
                position.as_ref(),
                query.limit.map(|limit| limit.saturating_add(1)),
            )?;
            let end = query
                .limit
                .map_or(found.len(), |limit| limit.min(found.len()));
            let items = found.get(..end).map(<[_]>::to_vec).unwrap_or_default();
            let next = if end < found.len() {
                let last = items
                    .last()
                    .ok_or_else(|| StorageError::backend("a continued page was empty"))?;
                Some(Cursor::keyset(
                    "sqlite",
                    fingerprint,
                    &CursorPosition::for_record(&last.id, &last.doc, &last.meta, &query.sort),
                )?)
            } else {
                None
            };
            Ok(Page { items, next })
        })
        .await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(Access::Read, move |conn, ctx| {
            Ok(ops::matching(conn, ctx, &collection, &filter, &[])?.len() as u64)
        })
        .await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(Access::Write, move |conn, ctx| {
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
        validate_collection(collection)?;
        filter.validate()?;
        for order in sort {
            Filter::exists(order.field.clone(), true).validate()?;
        }
        validate_doc(patch)?;
        let (collection, filter, sort, patch) = (
            collection.to_owned(),
            filter.clone(),
            sort.to_vec(),
            patch.clone(),
        );
        self.with(Access::Write, move |conn, ctx| {
            let Some(first) =
                ops::matching_after(conn, ctx, &collection, &filter, &sort, None, Some(1))?
                    .into_iter()
                    .next()
            else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, &patch);
            // A claim patches the body only: the schema and expiry carry over.
            let version = ops::put_with(
                conn,
                ctx,
                &collection,
                &first.id,
                &doc,
                Precondition::Version(first.version),
                WriteOptions {
                    schema: first.meta.schema,
                    expires_ms: first.meta.expires_ms,
                    created_ms: None,
                },
            )?;
            let meta =
                ops::get(conn, ctx, &collection, &first.id)?.map_or(first.meta, |found| found.meta);
            Ok(Some(Versioned {
                id: first.id,
                version,
                doc,
                meta,
            }))
        })
        .await
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.atomic_batch_checked(Vec::new(), ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        for check in &checks {
            check.validate()?;
        }
        for op in &ops {
            match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    ..
                }
                | WriteOp::PutWith {
                    collection,
                    id,
                    doc,
                    ..
                } => {
                    validate_collection(collection)?;
                    validate_id(id)?;
                    validate_doc(doc)?;
                }
                WriteOp::Delete { collection, id, .. } => {
                    validate_collection(collection)?;
                    validate_id(id)?;
                }
            }
        }
        self.with(Access::Write, move |conn, ctx| {
            for check in &checks {
                check.validate()?;
                check.verify(
                    ops::get(conn, ctx, &check.collection, &check.id)?.map(|found| found.version),
                )?;
            }

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
                    WriteOp::PutWith {
                        collection,
                        id,
                        doc,
                        precondition,
                        options,
                    } => Ok(WriteResult::Put {
                        version: ops::put_with(
                            conn,
                            ctx,
                            &collection,
                            &id,
                            &doc,
                            precondition,
                            options,
                        )?,
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
        validate_collection(collection)?;
        let (collection, text) = (collection.to_owned(), text.to_owned());
        let hits = self
            .with(Access::Read, move |conn, ctx| {
                ops::search(conn, ctx, &collection, &text, limit)
            })
            .await?;
        Ok(hits
            .into_iter()
            .map(|(id, score)| SearchHit { id, score })
            .collect())
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        let collection = collection.to_owned();
        self.with(Access::Write, move |conn, ctx| {
            ops::drop_collection(conn, ctx, &collection)
        })
        .await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.with(Access::Read, move |conn, ctx| {
            ops::collections(conn, ctx, &prefix)
        })
        .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
