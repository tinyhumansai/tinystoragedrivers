//! [`EncryptedFileSecrets`]: every secret in one encrypted file, in
//! OpenHuman's `secrets.enc` format.
//!
//! # Format
//!
//! The file is exactly what OpenHuman's `EncryptedFileBackend` writes: the
//! JSON object `{"<name>": "<value>", ...}` (string values), encrypted as one
//! [`crypto::encrypt`] blob (`nonce ‖ ciphertext ‖ tag`, raw bytes, no hex).
//! A missing or empty file is an empty store. Values are therefore UTF-8 text;
//! [`SecretStore::set`] rejects anything else.
//!
//! # Concurrency
//!
//! A `set` or `delete` rewrites the whole file, so the read → modify → write
//! cycle runs under an exclusive advisory lock on the sidecar
//! `secrets.enc.lock`, the same lock OpenHuman takes, so the two can share a
//! workspace. The new file is staged under a name unique to this process and
//! call, synced, made `0600` before any byte lands, and renamed into place, so
//! a reader sees the whole old file or the whole new one.
//!
//! # Corruption
//!
//! A file that fails to decrypt or parse is an error
//! ([`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) or
//! [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization))
//! and is left untouched: no read degrades to "empty", and no write replaces
//! secrets it could not read.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::{Zeroize, Zeroizing};

use crate::crypto::{self, KEY_LEN, key_from_hex, key_to_hex};
use crate::store::{SecretStore, require_utf8, validate_name, validate_prefix};
use crate::task::{io_error, run_blocking};

/// The secrets file name inside a workspace directory.
pub const SECRETS_FILE_NAME: &str = "secrets.enc";

/// The key file name OpenHuman's `SecretStore` keeps beside its config: the
/// 32-byte key as 64 hex characters.
pub const KEY_FILE_NAME: &str = ".secret_key";

/// Distinguishes concurrent temp files written by one process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Secrets in one ChaCha20-Poly1305 file.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use tinystoragedrivers_secrets::{EncryptedFileSecrets, SecretStore, crypto};
///
/// let dir = std::env::temp_dir().join(format!("tsd-doc-{}", std::process::id()));
/// let secrets = EncryptedFileSecrets::new(&dir, crypto::generate_key());
/// secrets.set("alice:api_key", b"sk-live-abc").await?;
/// assert!(dir.join("secrets.enc").exists());
/// # std::fs::remove_dir_all(&dir).unwrap();
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
pub struct EncryptedFileSecrets {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    key: Zeroizing<[u8; KEY_LEN]>,
}

impl EncryptedFileSecrets {
    /// The store at `dir/secrets.enc`, under `key`.
    ///
    /// OpenHuman keeps that key in the OS keychain (service `openhuman`, user
    /// `app:master_key`, as hex); see `KeyringSecrets::load_or_create_key`
    /// under the `keyring` feature.
    #[must_use]
    pub fn new(dir: impl AsRef<Path>, key: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self::at_path(dir.as_ref().join(SECRETS_FILE_NAME), key)
    }

    /// The store at an explicit file path, under `key`.
    #[must_use]
    pub fn at_path(path: impl Into<PathBuf>, key: Zeroizing<[u8; KEY_LEN]>) -> Self {
        Self {
            inner: Arc::new(Inner {
                path: path.into(),
                key,
            }),
        }
    }

    /// The store at `dir/secrets.enc`, keyed by `dir/.secret_key`, which is
    /// created (`0600`, 32 random bytes as hex) when absent. Blocks on disk
    /// I/O; call it at boot.
    ///
    /// # Errors
    ///
    /// As [`load_or_create_key_file`].
    pub fn with_key_file(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        let key = load_or_create_key_file(&dir.join(KEY_FILE_NAME))?;
        Ok(Self::new(dir, key))
    }

    /// The secrets file this store reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }
}

impl fmt::Debug for EncryptedFileSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedFileSecrets")
            .field("path", &self.inner.path)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for EncryptedFileSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        // No lock: writes publish by rename, so a read sees one whole file.
        run_blocking(move || {
            let map = inner.read_map()?;
            Ok(map
                .0
                .get(&name)
                .map(|value| Zeroizing::new(value.as_bytes().to_vec())))
        })
        .await
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let value = Zeroizing::new(require_utf8(value, "encrypted file")?.to_string());
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let _lock = inner.lock()?;
            let mut map = inner.read_map()?;
            map.0.insert(name, value.to_string());
            inner.write_map(&map)
        })
        .await
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        let inner = Arc::clone(&self.inner);
        let name = name.to_string();
        run_blocking(move || {
            let _lock = inner.lock()?;
            let mut map = inner.read_map()?;
            let Some(mut removed) = map.0.remove(&name) else {
                return Ok(false);
            };
            removed.zeroize();
            inner.write_map(&map)?;
            Ok(true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.to_string();
        run_blocking(move || {
            let map = inner.read_map()?;
            Ok(map
                .0
                .keys()
                .filter(|name| name.starts_with(&prefix))
                .cloned()
                .collect())
        })
        .await
    }

    fn backend_name(&self) -> &'static str {
        "encrypted_file"
    }
}

/// The decrypted name → value map, wiped on drop.
#[derive(Default)]
struct SecretMap(BTreeMap<String, String>);

impl Drop for SecretMap {
    fn drop(&mut self) {
        for value in self.0.values_mut() {
            value.zeroize();
        }
    }
}

impl Inner {
    fn read_map(&self) -> Result<SecretMap> {
        let blob = match fs::read(&self.path) {
            Ok(blob) => blob,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SecretMap::default()),
            Err(e) => return Err(io_error("read the secrets file", e)),
        };
        if blob.is_empty() {
            return Ok(SecretMap::default());
        }
        let json = crypto::decrypt(&self.key, &blob)?;
        // The serde message can quote the offending input, which here is
        // plaintext, so it is neither kept nor chained as the source.
        serde_json::from_slice(&json).map(SecretMap).map_err(|_| {
            StorageError::serialization("decrypted secrets file is not a json object of strings")
        })
    }

    fn write_map(&self, map: &SecretMap) -> Result<()> {
        let json = Zeroizing::new(serde_json::to_vec(&map.0)?);
        let blob = crypto::encrypt(&self.key, &json)?;
        write_atomic(&self.path, &blob)
    }

    /// Take the exclusive cross-process lock on `<path>.lock`, held until the
    /// returned file is dropped.
    fn lock(&self) -> Result<File> {
        let mut name = self.path.file_name().unwrap_or_default().to_os_string();
        name.push(".lock");
        let lock_path = self.path.with_file_name(name);
        create_parent(&lock_path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| io_error("open the secrets lock file", e))?;
        fs4::FileExt::lock(&file).map_err(|e| io_error("lock the secrets file", e))?;
        Ok(file)
    }
}

fn create_parent(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            fs::create_dir_all(parent).map_err(|e| io_error("create the secrets directory", e))
        }
        _ => Ok(()),
    }
}

/// Replace `path` with `bytes` atomically, `0600` on Unix.
///
/// `std::fs::rename` replaces an existing destination on every platform
/// (`MoveFileExW` with `MOVEFILE_REPLACE_EXISTING` on Windows), so a reader
/// sees the whole old file or the whole new one.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = stage(path, bytes, &mut next_temp_seq)?;
    fs::rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        io_error("write the secrets file", e)
    })
}

/// The next temp-file sequence number for this process.
fn next_temp_seq() -> u64 {
    TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Write `bytes` to a fresh, synced, `0600` temp sibling of `path` and return
/// its path. Nothing is left behind on failure.
fn stage(path: &Path, bytes: &[u8], next_seq: &mut dyn FnMut() -> u64) -> Result<PathBuf> {
    create_parent(path)?;
    let (tmp_path, mut file) = reserve_temp_file(path, next_seq)?;
    let staged = (|| -> std::io::Result<()> {
        restrict_permissions(&file)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    staged.map(|()| tmp_path.clone()).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        io_error("stage the secrets file", e)
    })
}

/// Create a fresh temp sibling of `path`, skipping leftovers from crashed
/// writers whose pid has been reused.
fn reserve_temp_file(path: &Path, next_seq: &mut dyn FnMut() -> u64) -> Result<(PathBuf, File)> {
    loop {
        let mut name = path.file_name().unwrap_or_default().to_os_string();
        name.push(format!(".{}.{}.tmp", std::process::id(), next_seq()));
        let tmp_path = path.with_file_name(name);
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
        {
            Ok(file) => return Ok((tmp_path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(io_error("stage the secrets file", e)),
        }
    }
}

#[cfg(unix)]
fn restrict_permissions(file: &File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_permissions(_file: &File) -> std::io::Result<()> {
    Ok(())
}

/// Load the hex key at `path`, or create it with 32 random bytes when absent.
///
/// This is the `.secret_key` format of OpenHuman's `SecretStore`: 64 hex
/// characters, surrounding whitespace ignored. A new key is written and synced
/// to a temp sibling (`0600` on Unix) and only then published under `path`
/// with a hard link, which fails if the file already exists. A reader never
/// sees a partial key file, and two processes racing to create it agree on the
/// winner's key. Blocks on disk I/O.
///
/// # Errors
///
/// [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) for a
/// key file that is not 32 bytes of hex, or an I/O error mapped as the file
/// driver maps them (including a filesystem without hard links).
pub fn load_or_create_key_file(path: &Path) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    match read_key_file(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        other => return parse_key_file(other),
    }
    let key = crypto::generate_key();
    let tmp_path = stage(path, key_to_hex(&key).as_bytes(), &mut next_temp_seq)?;
    publish_key(&tmp_path, path, key)
}

/// Publish the staged key at `tmp_path` as `path` unless `path` exists, in
/// which case the existing key wins. The temp file is always removed.
fn publish_key(
    tmp_path: &Path,
    path: &Path,
    key: Zeroizing<[u8; KEY_LEN]>,
) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let linked = fs::hard_link(tmp_path, path);
    let _ = fs::remove_file(tmp_path);
    match linked {
        Ok(()) => Ok(key),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            parse_key_file(read_key_file(path))
        }
        Err(e) => Err(io_error("publish the key file", e)),
    }
}

fn read_key_file(path: &Path) -> std::io::Result<Zeroizing<String>> {
    fs::read_to_string(path).map(Zeroizing::new)
}

fn parse_key_file(read: std::io::Result<Zeroizing<String>>) -> Result<Zeroizing<[u8; KEY_LEN]>> {
    let hex = read.map_err(|e| io_error("read the key file", e))?;
    key_from_hex(&hex)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
