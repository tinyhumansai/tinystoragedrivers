//! [`KeyringSecrets`]: secrets in the OS credential store.
//!
//! Each secret is one credential with the store's service name and the secret
//! name as its user, the layout OpenHuman's `OsBackend` uses (service
//! `openhuman`, user `"{user_id}:{key}"`), so its entries read back here.
//! Values go through the platform's password API, as OpenHuman's do, so they
//! are UTF-8 text and decode the same on every platform (Windows stores a
//! password as UTF-16).
//!
//! The OS stores cannot enumerate their entries, so [`SecretStore::list`]
//! fails with [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend)
//! and [`SecretStore::enumerable`] is `false`.
//!
//! Every call can block (a keychain unlock prompt), so it runs on the
//! runtime's blocking pool. The `keyring` calls themselves are kept to thin
//! wrappers; what an error *means* is decided in [`map_keyring_error`].

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use keyring::{CredentialBuilder, Entry};
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::Zeroizing;

use crate::crypto::{self, KEY_LEN, key_from_hex, key_to_hex};
use crate::file::lock_file;
use crate::store::{SecretStore, require_utf8, validate_name, validate_prefix};
use crate::task::run_blocking;

/// Serializes [`KeyringSecrets::load_or_create_key`] within the process, so
/// two callers cannot both see an absent key and store different ones.
static KEY_INIT: Mutex<()> = Mutex::new(());

/// Secrets in the native OS credential store.
///
/// ```no_run
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{EncryptedFileSecrets, KeyringSecrets, SecretStore};
///
/// // OpenHuman's layout: the `secrets.enc` master key lives in the keychain.
/// let keychain = KeyringSecrets::new("openhuman");
/// let master = keychain.load_or_create_key("app:master_key").await?;
/// let secrets = EncryptedFileSecrets::new("/home/me/.openhuman", master);
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct KeyringSecrets {
    service: Arc<str>,
    builder: Option<Arc<CredentialBuilder>>,
    creation_lock: Option<Arc<Path>>,
}

impl KeyringSecrets {
    /// The store whose credentials carry `service`, on the platform's default
    /// credential store.
    #[must_use]
    pub fn new(service: impl AsRef<str>) -> Self {
        Self {
            service: Arc::from(service.as_ref()),
            builder: None,
            creation_lock: None,
        }
    }

    /// The store whose credentials carry `service`, built by `builder`
    /// instead of the platform default: a specific keychain, or a test double.
    #[must_use]
    pub fn with_credential_builder(
        service: impl AsRef<str>,
        builder: Box<CredentialBuilder>,
    ) -> Self {
        Self {
            service: Arc::from(service.as_ref()),
            builder: Some(Arc::from(builder)),
            creation_lock: None,
        }
    }

    /// Serialize [`KeyringSecrets::load_or_create_key`] across processes with
    /// an exclusive advisory lock on `path` (created if absent), for example
    /// `<workspace>/keyring.lock`. Every process that may create the key must
    /// name the same file.
    #[must_use]
    pub fn with_key_creation_lock(mut self, path: impl Into<PathBuf>) -> Self {
        self.creation_lock = Some(Arc::from(path.into()));
        self
    }

    /// The service name every credential is stored under.
    #[must_use]
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Load the hex-encoded 32-byte key stored as secret `name`, or create and
    /// store a random one when it does not exist.
    ///
    /// Only a genuine absence creates a key. Any other failure (access denied,
    /// a locked keychain) is returned without writing anything, so an existing
    /// key is never replaced by a new one that would orphan everything
    /// encrypted under it. A created key is read back and compared before it
    /// is returned.
    ///
    /// Concurrent calls in one process are serialized, so they all return the
    /// same key. The OS stores offer no create-if-absent, so serializing
    /// *processes* needs a shared lock file: configure one with
    /// [`KeyringSecrets::with_key_creation_lock`]. Without it, two processes
    /// creating the same key at the same moment can each return a different
    /// key, so then create it from one process at boot.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name, [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto)
    /// when the stored key is malformed or a new key does not read back, or
    /// the keyring error mapped by kind.
    pub async fn load_or_create_key(&self, name: &str) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        validate_name(name)?;
        let this = self.clone();
        let name = name.to_string();
        run_blocking(move || {
            let _serialized = KEY_INIT.lock().unwrap_or_else(PoisonError::into_inner);
            let _cross_process = this.creation_lock.as_deref().map(lock_file).transpose()?;
            let entry = this.entry(&name)?;
            match entry.get_password() {
                Ok(hex) => key_from_hex(&Zeroizing::new(hex)),
                Err(keyring::Error::NoEntry) => {
                    let key = crypto::generate_key()?;
                    let hex = key_to_hex(&key);
                    entry.set_password(&hex).map_err(map_keyring_error)?;
                    let stored = Zeroizing::new(entry.get_password().map_err(map_keyring_error)?);
                    if stored.trim() != hex.as_str() {
                        return Err(StorageError::crypto(
                            "a new key did not read back from the keyring",
                        ));
                    }
                    Ok(key)
                }
                Err(e) => Err(map_keyring_error(e)),
            }
        })
        .await
    }

    fn entry(&self, name: &str) -> Result<Entry> {
        match &self.builder {
            Some(builder) => builder
                .build(None, &self.service, name)
                .map(Entry::new_with_credential),
            None => Entry::new(&self.service, name),
        }
        .map_err(map_keyring_error)
    }
}

impl fmt::Debug for KeyringSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyringSecrets")
            .field("service", &self.service)
            .field("custom_builder", &self.builder.is_some())
            .field("creation_lock", &self.creation_lock)
            .finish()
    }
}

#[async_trait]
impl SecretStore for KeyringSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        let this = self.clone();
        let name = name.to_string();
        run_blocking(move || match this.entry(&name)?.get_password() {
            Ok(value) => Ok(Some(Zeroizing::new(value.into_bytes()))),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(map_keyring_error(e)),
        })
        .await
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let value = Zeroizing::new(require_utf8(value, "keyring")?.to_string());
        let this = self.clone();
        let name = name.to_string();
        run_blocking(move || {
            this.entry(&name)?
                .set_password(&value)
                .map_err(map_keyring_error)
        })
        .await
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        let this = self.clone();
        let name = name.to_string();
        run_blocking(move || match this.entry(&name)?.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(e) => Err(map_keyring_error(e)),
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        Err(StorageError::backend(
            "the os credential store cannot enumerate secrets",
        ))
    }

    fn enumerable(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "keyring"
    }
}

/// What a `keyring` failure means to a caller.
///
/// - `NoStorageAccess` (a locked or denied store) is
///   [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable):
///   unlocking it makes a retry succeed.
/// - `TooLong` and `Invalid` reject the name:
///   [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput).
/// - `BadEncoding` is a stored value that is not UTF-8:
///   [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization).
/// - `NoEntry` is [`ErrorKind::NotFound`](tinystoragedrivers_core::ErrorKind::NotFound).
/// - `PlatformFailure`, `Ambiguous` and anything newer are
///   [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend).
///
/// Only platform failures keep the keyring error as the source; the others
/// carry the stored bytes, the credential list, or the secret name, none of
/// which may reach a log.
pub(crate) fn map_keyring_error(error: keyring::Error) -> StorageError {
    match error {
        keyring::Error::NoStorageAccess(_) => {
            StorageError::unavailable("the os credential store is not accessible")
                .with_source(error)
        }
        keyring::Error::PlatformFailure(_) => {
            StorageError::backend("the os credential store failed").with_source(error)
        }
        keyring::Error::NoEntry => StorageError::not_found("no such credential"),
        keyring::Error::BadEncoding(_) => {
            StorageError::serialization("the stored credential is not utf-8 text")
        }
        keyring::Error::TooLong(..) | keyring::Error::Invalid(..) => {
            StorageError::invalid_input("the os credential store rejected the secret name")
        }
        _ => {
            StorageError::backend("the os credential store returned an ambiguous or unknown error")
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
