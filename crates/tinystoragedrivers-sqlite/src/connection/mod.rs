//! One shared connection per database file.
//!
//! Every handle opened on the same file in this process goes through the same
//! [`Db`], so writers queue on an in-process mutex instead of contending for
//! SQLite's file lock (which would surface as `SQLITE_BUSY`). Calls run on the
//! tokio blocking pool when a runtime is present, so a slow statement never
//! stalls a runtime worker, and inline otherwise.
//!
//! Another process opening the same file is still safe: WAL mode plus a busy
//! timeout make SQLite serialize writers, and lock contention past the timeout
//! is reported as retryable.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use rusqlite::Connection;
use tinystoragedrivers_core::{Result, StorageError};

use crate::error::during;

/// How long a statement waits for another process's write lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// An open database file.
pub(crate) struct Db {
    path: PathBuf,
    conn: Mutex<Connection>,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn registry() -> &'static Mutex<HashMap<PathBuf, Weak<Db>>> {
    static OPEN: OnceLock<Mutex<HashMap<PathBuf, Weak<Db>>>> = OnceLock::new();
    OPEN.get_or_init(Mutex::default)
}

/// The absolute, symlink-resolved form of `path`, creating its directory.
fn normalize(path: &Path) -> Result<PathBuf> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(parent)
        .map_err(io_error("cannot create the sqlite database directory"))?;
    let parent = parent
        .canonicalize()
        .map_err(io_error("cannot resolve the sqlite database directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| StorageError::invalid_input("sqlite database path has no file name"))?;
    Ok(parent.join(name))
}

/// Map a filesystem failure while preparing the database location.
fn io_error(what: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::backend(what).with_source(error)
}

/// Map a blocking task that panicked or was cancelled.
fn join_error(error: tokio::task::JoinError) -> StorageError {
    StorageError::backend("sqlite call did not complete").with_source(error)
}

impl Db {
    /// Open (or share the already-open) database at `path`, creating it.
    pub(crate) fn open(path: &Path) -> Result<Arc<Self>> {
        let path = normalize(path)?;
        let mut open = registry()
            .lock()
            .map_err(|_| StorageError::backend("sqlite connection registry lock poisoned"))?;
        if let Some(db) = open.get(&path).and_then(Weak::upgrade) {
            return Ok(db);
        }
        let conn = Connection::open(&path).map_err(during("open"))?;
        conn.busy_timeout(BUSY_TIMEOUT).map_err(during("open"))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(during("open"))?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(during("open"))?;
        let db = Arc::new(Self {
            path: path.clone(),
            conn: Mutex::new(conn),
        });
        open.retain(|_, weak| weak.strong_count() > 0);
        open.insert(path, Arc::downgrade(&db));
        Ok(db)
    }

    /// The file this database lives in.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Run `f` with exclusive use of the connection.
    pub(crate) async fn run<T, F>(self: &Arc<Self>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let db = Arc::clone(self);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle
                .spawn_blocking(move || db.run_now(f))
                .await
                .map_err(join_error)?,
            Err(_) => db.run_now(f),
        }
    }

    pub(crate) fn run_now<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::backend("sqlite connection lock poisoned"))?;
        f(&mut conn)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
