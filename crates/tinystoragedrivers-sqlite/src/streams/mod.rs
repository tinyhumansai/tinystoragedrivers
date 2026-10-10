//! [`StreamStore`] on SQLite.
//!
//! Records live in one table keyed by `(scope, stream, offset)`; a metadata
//! row per stream holds the first retained offset (`base`) and the next offset
//! to allocate (`len`). Allocating and inserting happen in one immediate
//! transaction, so offsets are dense even with concurrent appenders.

use std::fmt;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;
use tinystoragedrivers_core::{
    Result, Scope, StorageError, StreamEntry, StreamStore, async_trait, validate_stream,
};

use crate::connection::Db;
use crate::error::during;
use crate::fence::{self, Fencing};
use crate::sql::{Tables, ident};

/// SQLite streams bound to one scope.
#[derive(Clone)]
pub struct SqliteStreams {
    db: Arc<Db>,
    tables: Arc<Tables>,
    scope: Scope,
    /// Checked inside every write's transaction, when set.
    fencing: Option<Arc<Fencing>>,
}

impl fmt::Debug for SqliteStreams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStreams")
            .field("scope", &self.scope)
            .field("fenced", &self.fencing.is_some())
            .finish_non_exhaustive()
    }
}

/// `(base, len)` of a stream, if it exists.
fn meta(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
) -> Result<Option<(u64, u64)>> {
    let sql = format!(
        "SELECT base, len FROM {} WHERE scope = ?1 AND stream = ?2",
        ident(&tables.stream_meta)
    );
    let row: Option<(i64, i64)> = conn
        .query_row(&sql, params![scope, stream], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(during("read stream"))?;
    Ok(row.map(|(base, len)| (to_offset(base), to_offset(len))))
}

fn to_offset(stored: i64) -> u64 {
    u64::try_from(stored).unwrap_or(0)
}

fn to_stored(offset: u64) -> Result<i64> {
    i64::try_from(offset).map_err(|_| StorageError::backend("stream offset space is exhausted"))
}

impl SqliteStreams {
    pub(crate) fn new(
        db: Arc<Db>,
        tables: Arc<Tables>,
        scope: Scope,
        fencing: Option<Arc<Fencing>>,
    ) -> Self {
        Self {
            db,
            tables,
            scope,
            fencing,
        }
    }

    /// Run `f`, inside an immediate transaction that first checks the fence
    /// when `write` is set.
    async fn with<T, F>(&self, write: bool, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, &Tables, &str) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                if !write {
                    return f(conn, &tables, scope.as_str());
                }
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(during("begin"))?;
                fence::guard(fencing.as_deref(), &tx, &tables)?;
                let out = f(&tx, &tables, scope.as_str())?;
                tx.commit().map_err(during("commit"))?;
                Ok(out)
            })
            .await
    }
}

#[async_trait]
impl StreamStore for SqliteStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            let (base, first) = meta(conn, tables, scope, &stream)?.unwrap_or((0, 0));
            if values.is_empty() {
                return Ok(first);
            }
            let insert = format!(
                "INSERT INTO {} (scope, stream, seq, value) VALUES (?1, ?2, ?3, ?4)",
                ident(&tables.streams)
            );
            let mut next = first;
            for value in &values {
                conn.execute(
                    &insert,
                    params![
                        scope,
                        stream,
                        to_stored(next)?,
                        serde_json::to_string(value)?
                    ],
                )
                .map_err(during("append"))?;
                next += 1;
            }
            let upsert = format!(
                "INSERT INTO {} (scope, stream, base, len) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (scope, stream) DO UPDATE SET len = excluded.len",
                ident(&tables.stream_meta)
            );
            conn.execute(
                &upsert,
                params![scope, stream, to_stored(base)?, to_stored(next)?],
            )
            .map_err(during("append"))?;
            Ok(first)
        })
        .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(false, move |conn, tables, scope| {
            let sql = format!(
                "SELECT seq, value FROM {} WHERE scope = ?1 AND stream = ?2 AND seq >= ?3
                 ORDER BY seq LIMIT ?4",
                ident(&tables.streams)
            );
            let from = i64::try_from(from).unwrap_or(i64::MAX);
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let mut statement = conn.prepare(&sql).map_err(during("read stream"))?;
            let rows: Vec<(i64, String)> = statement
                .query_map(params![scope, stream, from, limit], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(during("read stream"))?
                .collect::<rusqlite::Result<_>>()
                .map_err(during("read stream"))?;
            rows.into_iter()
                .map(|(offset, value)| {
                    Ok(StreamEntry {
                        offset: to_offset(offset),
                        value: serde_json::from_str(&value)?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(false, move |conn, tables, scope| {
            Ok(meta(conn, tables, scope, &stream)?.map_or(0, |(_, len)| len))
        })
        .await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            let Some((base, len)) = meta(conn, tables, scope, &stream)? else {
                return Ok(0);
            };
            let cut = offset.clamp(base, len);
            let delete = format!(
                "DELETE FROM {} WHERE scope = ?1 AND stream = ?2 AND seq < ?3",
                ident(&tables.streams)
            );
            conn.execute(&delete, params![scope, stream, to_stored(cut)?])
                .map_err(during("truncate stream"))?;
            let update = format!(
                "UPDATE {} SET base = ?3 WHERE scope = ?1 AND stream = ?2",
                ident(&tables.stream_meta)
            );
            conn.execute(&update, params![scope, stream, to_stored(cut)?])
                .map_err(during("truncate stream"))?;
            Ok(cut - base)
        })
        .await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            conn.execute(
                &format!(
                    "DELETE FROM {} WHERE scope = ?1 AND stream = ?2",
                    ident(&tables.streams)
                ),
                params![scope, stream],
            )
            .map_err(during("delete stream"))?;
            let removed = conn
                .execute(
                    &format!(
                        "DELETE FROM {} WHERE scope = ?1 AND stream = ?2",
                        ident(&tables.stream_meta)
                    ),
                    params![scope, stream],
                )
                .map_err(during("delete stream"))?;
            Ok(removed > 0)
        })
        .await
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.with(false, move |conn, tables, scope| {
            let sql = format!(
                "SELECT stream FROM {} WHERE scope = ?1 ORDER BY stream",
                ident(&tables.stream_meta)
            );
            let mut statement = conn.prepare(&sql).map_err(during("list streams"))?;
            let names: Vec<String> = statement
                .query_map(params![scope], |row| row.get(0))
                .map_err(during("list streams"))?
                .collect::<rusqlite::Result<_>>()
                .map_err(during("list streams"))?;
            Ok(names
                .into_iter()
                .filter(|name| name.starts_with(&prefix))
                .collect())
        })
        .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
