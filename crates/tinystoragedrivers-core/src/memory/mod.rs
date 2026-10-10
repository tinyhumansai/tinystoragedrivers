//! The in-memory driver: the reference implementation of every port.
//!
//! It provides every [`Capability`](crate::Capability), including
//! [`Fencing`](crate::Capability::Fencing) on every write (the fence is
//! checked under the same lock as the write), keeps nothing across process
//! restarts, and is what tests and stateless embedders run on. Because
//! its semantics are the simplest correct reading of the port docs, the
//! conformance suite is written against it first and every other driver must
//! agree with it.

mod blobs;
mod documents;
mod state;
mod streams;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{ScopedStorage, StorageBackend, validate_database};
use crate::capabilities::Capabilities;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::scope::Scope;

pub use blobs::MemoryBlobs;
pub use documents::MemoryDocuments;
pub use streams::MemoryStreams;

use state::DbState;

/// Milliseconds since the Unix epoch. The memory driver reads time through
/// this so tests can drive document expiry deterministically.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// One in-memory database.
#[derive(Debug, Default)]
pub(crate) struct MemoryDb {
    state: Mutex<DbState>,
}

impl MemoryDb {
    fn lock(&self) -> Result<MutexGuard<'_, DbState>> {
        self.state
            .lock()
            .map_err(|_| StorageError::backend("memory storage lock poisoned by a panicked writer"))
    }
}

/// An in-memory [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
///
/// let storage = MemoryStorage::new();
/// let alice = storage.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "memory");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct MemoryStorage {
    db: Arc<MemoryDb>,
    databases: Arc<Mutex<BTreeMap<String, Arc<MemoryDb>>>>,
    clock: Clock,
}

impl MemoryStorage {
    /// An empty backend reading the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(system_clock))
    }

    /// An empty backend reading time from `clock`.
    #[must_use]
    pub fn with_clock(clock: Clock) -> Self {
        Self {
            db: Arc::default(),
            databases: Arc::default(),
            clock,
        }
    }

    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fence>>) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(MemoryDocuments::new(
                Arc::clone(&self.db),
                scope.clone(),
                Arc::clone(&self.clock),
                fence.cloned(),
            )),
            Arc::new(MemoryStreams::new(
                Arc::clone(&self.db),
                scope.clone(),
                Arc::clone(&self.clock),
                fence.cloned(),
            )),
            Arc::new(MemoryBlobs::new(
                Arc::clone(&self.db),
                scope.clone(),
                Arc::clone(&self.clock),
                fence.cloned(),
            )),
        )
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage").finish_non_exhaustive()
    }
}

impl StorageBackend for MemoryStorage {
    fn driver(&self) -> &'static str {
        "memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(scope, Some(&Arc::new(fence.clone()))))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| StorageError::backend("memory database registry lock poisoned"))?;
        let db = Arc::clone(databases.entry(name.to_owned()).or_default());
        Ok(Arc::new(Self {
            db,
            databases: Arc::clone(&self.databases),
            clock: Arc::clone(&self.clock),
        }))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
