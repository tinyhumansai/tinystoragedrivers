//! [`BlobStore`] on SQLite: one row per blob, ranged reads with `substr`.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, Result, Scope, async_trait, clamp_range, validate_blob_key,
};

use crate::connection::Db;
use crate::error::during;
use crate::fence::{self, Fencing};
use crate::sql::{Tables, ident};

/// SQLite blobs bound to one scope.
#[derive(Clone)]
pub struct SqliteBlobs {
    db: Arc<Db>,
    tables: Arc<Tables>,
    scope: Scope,
    /// Checked inside every write's transaction, when set.
    fencing: Option<Arc<Fencing>>,
}

impl fmt::Debug for SqliteBlobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteBlobs")
            .field("scope", &self.scope)
            .field("fenced", &self.fencing.is_some())
            .finish_non_exhaustive()
    }
}

impl SqliteBlobs {
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

    /// Run a write. An unfenced write is one statement and runs as it is; a
    /// fenced one runs in an immediate transaction that checks the fence
    /// first.
    async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                let Some(fencing) = fencing else {
                    return f(conn);
                };
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(during("begin"))?;
                fence::guard(Some(&fencing), &tx, &tables)?;
                let out = f(&tx)?;
                tx.commit().map_err(during("commit"))?;
                Ok(out)
            })
            .await
    }

    fn table(&self) -> String {
        ident(&self.tables.blobs)
    }
}

fn meta(key: String, len: i64, content_type: Option<String>) -> BlobMeta {
    BlobMeta {
        key,
        len: u64::try_from(len).unwrap_or(0),
        content_type,
    }
}

#[async_trait]
impl BlobStore for SqliteBlobs {
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        validate_blob_key(key)?;
        let sql = format!(
            "INSERT INTO {} (scope, key, content_type, bytes) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (scope, key) DO UPDATE SET content_type = excluded.content_type, bytes = excluded.bytes",
            self.table()
        );
        let described = BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        };
        let (scope, row) = (self.scope.clone(), described.clone());
        self.write(move |conn| {
            conn.execute(
                &sql,
                params![scope.as_str(), row.key, row.content_type, bytes],
            )
            .map_err(during("put blob"))
        })
        .await?;
        Ok(described)
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_blob_key(key)?;
        let sql = format!(
            "SELECT content_type, bytes FROM {} WHERE scope = ?1 AND key = ?2",
            self.table()
        );
        let (scope, key) = (self.scope.clone(), key.to_owned());
        self.db
            .run(move |conn| {
                let row: Option<(Option<String>, Vec<u8>)> = conn
                    .query_row(&sql, params![scope.as_str(), key], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .optional()
                    .map_err(during("get blob"))?;
                Ok(row.map(|(content_type, bytes)| Blob {
                    meta: BlobMeta {
                        key,
                        len: bytes.len() as u64,
                        content_type,
                    },
                    bytes,
                }))
            })
            .await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_blob_key(key)?;
        // One statement reads and clamps (`substr` stops at the end of the
        // blob), so a concurrent replace cannot mix two versions of it.
        clamp_range(&range, 0)?;
        let sql = format!(
            "SELECT substr(bytes, ?3, ?4) FROM {} WHERE scope = ?1 AND key = ?2",
            self.table()
        );
        let start = i64::try_from(range.start.saturating_add(1)).unwrap_or(i64::MAX);
        let count = i64::try_from(range.end - range.start).unwrap_or(i64::MAX);
        let (scope, key) = (self.scope.clone(), key.to_owned());
        self.db
            .run(move |conn| {
                conn.query_row(&sql, params![scope.as_str(), key, start, count], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .optional()
                .map_err(during("read blob range"))
            })
            .await
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_blob_key(key)?;
        let sql = format!(
            "SELECT content_type, length(bytes) FROM {} WHERE scope = ?1 AND key = ?2",
            self.table()
        );
        let (scope, key) = (self.scope.clone(), key.to_owned());
        self.db
            .run(move |conn| {
                let row: Option<(Option<String>, i64)> = conn
                    .query_row(&sql, params![scope.as_str(), key], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .optional()
                    .map_err(during("head blob"))?;
                Ok(row.map(|(content_type, len)| meta(key, len, content_type)))
            })
            .await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_blob_key(key)?;
        let sql = format!("DELETE FROM {} WHERE scope = ?1 AND key = ?2", self.table());
        let (scope, key) = (self.scope.clone(), key.to_owned());
        self.write(move |conn| {
            conn.execute(&sql, params![scope.as_str(), key])
                .map(|removed| removed > 0)
                .map_err(during("delete blob"))
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        let sql = format!(
            "SELECT key, length(bytes), content_type FROM {} WHERE scope = ?1 ORDER BY key",
            self.table()
        );
        let (scope, prefix) = (self.scope.clone(), prefix.to_owned());
        self.db
            .run(move |conn| {
                let mut statement = conn.prepare(&sql).map_err(during("list blobs"))?;
                let rows: Vec<(String, i64, Option<String>)> = statement
                    .query_map(params![scope.as_str()], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .map_err(during("list blobs"))?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(during("list blobs"))?;
                Ok(rows
                    .into_iter()
                    .filter(|(key, _, _)| key.starts_with(&prefix))
                    .map(|(key, len, content_type)| meta(key, len, content_type))
                    .collect())
            })
            .await
    }
}
