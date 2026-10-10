//! The SQLite [`StorageBackend`]: file layout and handle construction.
//!
//! - **Directory mode** (any path not ending in `.db` or `.sqlite`): the root
//!   database is `<dir>/storage.db`, and `database(name)` is `<dir>/<name>.db`.
//!   This is the desktop layout: each owner keeps its own file, the way
//!   OpenHuman already splits `approval.db`, `flows.db` and `sessions.db`.
//! - **File mode** (a `.db` or `.sqlite` path): everything lives in that one
//!   file, and `database(name)` prefixes its tables with `<name>__`.
//!
//! Any scope may be used; rows carry their scope.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tinystoragedrivers_core::{
    Capabilities, Fence, Result, Scope, ScopedStorage, StorageBackend, validate_database,
};

use crate::blobs::SqliteBlobs;
use crate::connection::Db;
use crate::documents::{SqliteDocuments, capabilities};
use crate::fence::Fencing;
use crate::native::SqliteNative;
use crate::sql::Tables;
use crate::streams::SqliteStreams;

/// Milliseconds since the Unix epoch, as the driver sees them.
pub use tinystoragedrivers_core::Clock;

/// File name of the root database in directory mode.
pub const ROOT_FILE: &str = "storage.db";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Layout {
    Directory(PathBuf),
    File(PathBuf),
}

/// A SQLite-backed [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_sqlite::SqliteStorage;
///
/// let dir = tempfile::tempdir().unwrap();
/// let storage = SqliteStorage::open(dir.path())?;
/// let approvals = storage.database("approvals")?;
/// assert!(dir.path().join("approvals.db").exists());
/// assert_eq!(approvals.for_scope(&Scope::local())?.driver(), "sqlite");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct SqliteStorage {
    layout: Layout,
    db: Arc<Db>,
    tables: Arc<Tables>,
    clock: Clock,
}

impl fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("path", &self.db.path())
            .field("prefix", &self.tables.prefix)
            .finish_non_exhaustive()
    }
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether `path` names a single database file rather than a directory.
fn is_file_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("db") || ext.eq_ignore_ascii_case("sqlite"))
}

impl SqliteStorage {
    /// Open storage at `path`, choosing directory or file mode from its
    /// extension, and create the driver's tables.
    ///
    /// # Errors
    ///
    /// When the directory or file cannot be created or opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let layout = if is_file_path(path) {
            Layout::File(path.to_path_buf())
        } else {
            Layout::Directory(path.to_path_buf())
        };
        let file = match &layout {
            Layout::Directory(dir) => dir.join(ROOT_FILE),
            Layout::File(file) => file.clone(),
        };
        Self::assemble(layout, &file, "", Arc::new(system_clock))
    }

    fn assemble(layout: Layout, file: &Path, prefix: &str, clock: Clock) -> Result<Self> {
        let db = Db::open(file)?;
        let tables = Arc::new(Tables::new(prefix));
        let create = Arc::clone(&tables);
        db.run_now(move |conn| create.ensure(conn))?;
        Ok(Self {
            layout,
            db,
            tables,
            clock,
        })
    }

    /// Read document expiry time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Raw access to this database's file, for owners that keep their own
    /// tables beside (or instead of) the generic ones.
    #[must_use]
    pub fn native(&self) -> SqliteNative {
        SqliteNative::new(Arc::clone(&self.db))
    }

    fn handles(&self, scope: &Scope, fencing: Option<&Arc<Fencing>>) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(SqliteDocuments::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                Arc::clone(&self.clock),
                fencing.cloned(),
            )),
            Arc::new(SqliteStreams::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                fencing.cloned(),
            )),
            Arc::new(SqliteBlobs::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                fencing.cloned(),
            )),
        )
    }

    /// The database file this storage reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.db.path()
    }
}

impl StorageBackend for SqliteStorage {
    fn driver(&self) -> &'static str {
        "sqlite"
    }

    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        let fencing = Arc::new(Fencing::new(fence.clone(), Arc::clone(&self.clock)));
        Ok(self.handles(scope, Some(&fencing)))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        let clock = Arc::clone(&self.clock);
        let storage = match &self.layout {
            Layout::Directory(dir) => Self::assemble(
                self.layout.clone(),
                &dir.join(format!("{name}.db")),
                "",
                clock,
            )?,
            Layout::File(file) => {
                Self::assemble(self.layout.clone(), file, &format!("{name}__"), clock)?
            }
        };
        Ok(Arc::new(storage))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
