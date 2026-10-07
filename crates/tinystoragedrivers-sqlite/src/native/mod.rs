//! Raw SQL access for owners that keep their own tables.
//!
//! Large relational stores (a session ledger with FTS, a workflow catalog)
//! keep their schema and SQL when they move onto this driver; they get the
//! shared connection, its locking and blocking-pool scheduling, and a
//! per-owner migration runner instead of opening the file themselves.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use tinystoragedrivers_core::{Result, StorageError};

use crate::connection::Db;
use crate::error::during;

/// The table recording each owner's applied migrations.
pub const MIGRATIONS_TABLE: &str = "_tsd_migrations";

/// Raw access to one SQLite database file.
#[derive(Clone)]
pub struct SqliteNative {
    db: Arc<Db>,
}

impl fmt::Debug for SqliteNative {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteNative")
            .field("path", &self.db.path())
            .finish()
    }
}

impl SqliteNative {
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Open the database file at `path` directly (shared with any storage
    /// already open on it).
    ///
    /// # Errors
    ///
    /// When the file cannot be created or opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::new(Db::open(path.as_ref())?))
    }

    /// The database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.db.path()
    }

    /// Run `f` with the connection.
    ///
    /// # Errors
    ///
    /// The SQLite error `f` returns, mapped (busy and locked are retryable).
    pub async fn with_connection<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        self.db
            .run(move |conn| f(conn).map_err(during("native call")))
            .await
    }

    /// Run `f` in an immediate transaction, committing when it succeeds.
    ///
    /// # Errors
    ///
    /// The SQLite error `f` returns (nothing is committed), or a failure to
    /// begin or commit.
    pub async fn with_transaction<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Transaction<'_>) -> rusqlite::Result<T> + Send + 'static,
    {
        self.db
            .run(move |conn| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(during("begin"))?;
                let out = f(&tx).map_err(during("native transaction"))?;
                tx.commit().map_err(during("commit"))?;
                Ok(out)
            })
            .await
    }

    /// Bring `owner`'s schema up to date: apply `steps[applied..]` in order,
    /// each in its own transaction, recording progress. Steps are append-only;
    /// never edit or reorder one that has shipped. Returns the new version.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// when the database records more steps than `steps` holds (a newer
    /// build ran here), or the failing step's SQLite error.
    pub async fn migrate(&self, owner: &str, steps: &[&str]) -> Result<usize> {
        let owner = owner.to_owned();
        let steps: Vec<String> = steps.iter().map(|step| (*step).to_owned()).collect();
        self.db
            .run(move |conn| {
                conn.execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                         owner TEXT PRIMARY KEY, version INTEGER NOT NULL
                     ) WITHOUT ROWID"
                ))
                .map_err(during("migrate"))?;
                // Read the recorded version inside each step's write
                // transaction, so two processes migrating at once never both
                // apply the same step.
                loop {
                    let tx = conn
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(during("migrate"))?;
                    let applied: i64 = tx
                        .query_row(
                            &format!("SELECT version FROM {MIGRATIONS_TABLE} WHERE owner = ?1"),
                            [&owner],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(during("migrate"))?
                        .unwrap_or(0);
                    let applied = usize::try_from(applied).unwrap_or(0);
                    if applied > steps.len() {
                        return Err(StorageError::invalid_input(
                            "the database schema is newer than this build",
                        ));
                    }
                    let Some(step) = steps.get(applied) else {
                        tx.commit().map_err(during("migrate"))?;
                        break;
                    };
                    tx.execute_batch(step).map_err(during("migrate"))?;
                    let version = i64::try_from(applied + 1).unwrap_or(i64::MAX);
                    tx.execute(
                        &format!(
                            "INSERT INTO {MIGRATIONS_TABLE} (owner, version) VALUES (?1, ?2)
                             ON CONFLICT (owner) DO UPDATE SET version = excluded.version"
                        ),
                        params![owner, version],
                    )
                    .map_err(during("migrate"))?;
                    tx.commit().map_err(during("migrate"))?;
                }
                Ok(steps.len())
            })
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
