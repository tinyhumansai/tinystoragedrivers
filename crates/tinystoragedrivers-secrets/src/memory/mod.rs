//! [`MemorySecrets`]: secrets in process memory, for tests.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use tinystoragedrivers_core::Result;
use zeroize::Zeroizing;

use crate::store::{SecretStore, validate_name, validate_prefix};

/// Secrets in a process-local map. Nothing survives the process, and values
/// are wiped from memory when replaced, deleted, or dropped with the store.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};
///
/// let secrets = MemorySecrets::new();
/// secrets.set("token", b"abc").await?;
/// assert_eq!(secrets.get("token").await?.unwrap().as_slice(), b"abc");
/// assert!(secrets.delete("token").await?);
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
#[derive(Default)]
pub struct MemorySecrets {
    entries: Mutex<BTreeMap<String, Zeroizing<Vec<u8>>>>,
}

impl MemorySecrets {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The map. A panic in another holder cannot leave it half-written (every
    /// mutation is one map call), so a poisoned lock is still safe to use.
    fn entries(&self) -> MutexGuard<'_, BTreeMap<String, Zeroizing<Vec<u8>>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Debug for MemorySecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySecrets")
            .field("len", &self.entries().len())
            .finish()
    }
}

#[async_trait]
impl SecretStore for MemorySecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        Ok(self.entries().get(name).cloned())
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        self.entries()
            .insert(name.to_string(), Zeroizing::new(value.to_vec()));
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        Ok(self.entries().remove(name).is_some())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        Ok(self
            .entries()
            .keys()
            .filter(|name| name.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn backend_name(&self) -> &'static str {
        "memory"
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
