//! The encrypted file driver: conformance, OpenHuman file compatibility,
//! key files, and every failure path.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use tinystoragedrivers_core::ErrorKind;

use super::*;
use crate::crypto::hex_decode;
use crate::crypto::tests::{OPENHUMAN_SECRETS_ENC_HEX, fixture_key};
use crate::testkit::secrets_conformance;

/// A fresh directory under the target dir's temp area, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("tsd-secrets-{label}-{}-{seq}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Make `dir` read-only and report whether that is enforced. It is not for
/// root, where the write failures under test cannot be produced.
#[cfg(unix)]
fn make_readonly(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o500)).unwrap();
    let probe = dir.join("probe");
    if fs::write(&probe, b"").is_ok() {
        fs::remove_file(probe).unwrap();
        make_writable(dir);
        return false;
    }
    true
}

#[cfg(unix)]
fn make_writable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
}

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let dir = TempDir::new("conformance");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "encrypted_file");
    assert_eq!(store.path(), dir.0.join("secrets.enc"));
}

#[tokio::test]
async fn opens_a_secrets_enc_file_openhuman_wrote() {
    let dir = TempDir::new("openhuman");
    fs::write(
        dir.0.join("secrets.enc"),
        hex_decode(OPENHUMAN_SECRETS_ENC_HEX).unwrap(),
    )
    .unwrap();
    let store = EncryptedFileSecrets::new(&dir.0, fixture_key());

    assert_eq!(
        store
            .get("alice:api_key")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"sk-live-abc"
    );
    assert_eq!(
        store.get("alice:oauth").await.unwrap().unwrap().as_slice(),
        "tok-\u{e9}".as_bytes()
    );
    assert_eq!(
        store.list("alice:").await.unwrap(),
        ["alice:api_key", "alice:oauth"]
    );

    // A write keeps the existing entries and stays in OpenHuman's format:
    // the raw blob decrypts to a JSON object of strings, as its `read_map`
    // expects.
    store.set("bob:token", b"t0k").await.unwrap();
    let blob = fs::read(dir.0.join("secrets.enc")).unwrap();
    let json = crypto::decrypt(&fixture_key(), &blob).unwrap();
    let map: HashMap<String, String> = serde_json::from_slice(&json).unwrap();
    assert_eq!(map.len(), 3);
    assert_eq!(map["alice:api_key"], "sk-live-abc");
    assert_eq!(map["bob:token"], "t0k");
}

#[cfg(unix)]
#[tokio::test]
async fn the_written_file_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("perms");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    store.set("a", b"1").await.unwrap();
    let mode = fs::metadata(store.path()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    assert!(dir.0.join("secrets.enc.lock").exists());
}

#[tokio::test]
async fn a_missing_or_empty_file_is_an_empty_store() {
    let dir = TempDir::new("empty");
    let store = EncryptedFileSecrets::new(dir.0.join("nested"), crypto::generate_key());
    assert_eq!(store.list("").await.unwrap(), Vec::<String>::new());
    fs::create_dir_all(dir.0.join("nested")).unwrap();
    fs::write(store.path(), b"").unwrap();
    assert!(store.get("a").await.unwrap().is_none());
}

#[tokio::test]
async fn binary_values_are_rejected() {
    let dir = TempDir::new("binary");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    let error = store.set("bin", &[0xff, 0x00]).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!store.path().exists());
}

#[tokio::test]
async fn a_wrong_key_fails_closed_and_leaves_the_file_alone() {
    let dir = TempDir::new("wrong-key");
    let writer = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    writer.set("keep", b"me").await.unwrap();
    let before = fs::read(writer.path()).unwrap();

    let reader = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    assert_eq!(
        reader.get("keep").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        reader.set("other", b"x").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(
        reader.delete("keep").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
    assert_eq!(fs::read(writer.path()).unwrap(), before);
}

#[tokio::test]
async fn decrypted_garbage_is_a_serialization_error_without_plaintext() {
    let dir = TempDir::new("garbage");
    let key = crypto::generate_key();
    let path = dir.0.join("secrets.enc");
    fs::write(&path, crypto::encrypt(&key, b"[\"sk-leak\"]").unwrap()).unwrap();
    let store = EncryptedFileSecrets::at_path(&path, key);
    let error = store.list("").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(!error.to_string().contains("sk-leak"));
    assert!(std::error::Error::source(&error).is_none());
}

#[tokio::test]
async fn an_unreadable_path_is_a_backend_error() {
    let dir = TempDir::new("unreadable");
    // The "file" is a directory: reading it fails with something other than
    // NotFound.
    let store = EncryptedFileSecrets::at_path(&dir.0, crypto::generate_key());
    assert_eq!(store.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
}

#[tokio::test]
async fn a_parent_that_is_a_file_fails_the_lock() {
    let dir = TempDir::new("parent-file");
    let blocker = dir.0.join("blocker");
    fs::write(&blocker, b"").unwrap();
    let store = EncryptedFileSecrets::new(&blocker, crypto::generate_key());
    assert_eq!(
        store.set("a", b"1").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[tokio::test]
async fn an_unwritable_directory_fails_the_lock_open() {
    let dir = TempDir::new("readonly");
    let store = EncryptedFileSecrets::new(&dir.0, crypto::generate_key());
    if !make_readonly(&dir.0) {
        return;
    }
    let result = store.set("a", b"1").await;
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[cfg(unix)]
#[test]
fn staging_fails_cleanly_in_an_unwritable_directory() {
    let dir = TempDir::new("stage");
    if !make_readonly(&dir.0) {
        return;
    }
    let result = write_atomic(&dir.0.join("secrets.enc"), b"x");
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn a_failed_rename_removes_the_staged_file() {
    let dir = TempDir::new("rename");
    let target = dir.0.join("occupied");
    fs::create_dir_all(target.join("child")).unwrap();
    let error = write_atomic(&target, b"x").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let leftovers: Vec<_> = fs::read_dir(&dir.0)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[test]
fn staging_skips_leftover_temp_files() {
    let dir = TempDir::new("leftover");
    let path = dir.0.join("secrets.enc");
    let start = TEMP_COUNTER.load(Ordering::Relaxed);
    for seq in start..start + 1000 {
        fs::write(
            dir.0
                .join(format!("secrets.enc.{}.{seq}.tmp", std::process::id())),
            b"",
        )
        .unwrap();
    }
    write_atomic(&path, b"fresh").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"fresh");
}

#[test]
fn a_bare_file_name_has_no_parent_to_create() {
    create_parent(Path::new("secrets.enc")).unwrap();
}

#[tokio::test]
async fn with_key_file_creates_then_reuses_the_key() {
    let dir = TempDir::new("keyfile");
    let first = EncryptedFileSecrets::with_key_file(&dir.0).unwrap();
    first.set("a", b"1").await.unwrap();

    let key_hex = fs::read_to_string(dir.0.join(".secret_key")).unwrap();
    assert_eq!(key_hex.len(), 64);
    assert!(key_hex.bytes().all(|b| b.is_ascii_hexdigit()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(dir.0.join(".secret_key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    let second = EncryptedFileSecrets::with_key_file(&dir.0).unwrap();
    assert_eq!(second.get("a").await.unwrap().unwrap().as_slice(), b"1");
}

#[test]
fn an_existing_openhuman_key_file_with_a_newline_loads() {
    let dir = TempDir::new("oh-key");
    let path = dir.0.join(".secret_key");
    fs::write(
        &path,
        format!("{}\n", crate::crypto::tests::FIXTURE_KEY_HEX),
    )
    .unwrap();
    assert_eq!(*load_or_create_key_file(&path).unwrap(), *fixture_key());
}

#[test]
fn a_corrupt_key_file_is_a_crypto_error() {
    let dir = TempDir::new("bad-key");
    let path = dir.0.join(".secret_key");
    fs::write(&path, "not hex").unwrap();
    assert_eq!(
        load_or_create_key_file(&path).unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[test]
fn an_unreadable_key_file_is_a_backend_error() {
    let dir = TempDir::new("key-dir");
    // A directory where the key file should be.
    assert_eq!(
        load_or_create_key_file(&dir.0).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[test]
fn an_uncreatable_key_file_is_a_backend_error() {
    let dir = TempDir::new("key-readonly");
    if !make_readonly(&dir.0) {
        return;
    }
    let result = load_or_create_key_file(&dir.0.join(".secret_key"));
    make_writable(&dir.0);
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn debug_shows_the_path_but_not_the_key() {
    let store = EncryptedFileSecrets::at_path("/x/secrets.enc", fixture_key());
    let debug = format!("{store:?}");
    assert!(debug.contains("/x/secrets.enc"));
    assert!(!debug.contains("0001020304"));
}
