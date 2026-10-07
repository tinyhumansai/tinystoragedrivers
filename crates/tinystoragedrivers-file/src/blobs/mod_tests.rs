//! Blob edge cases: sidecars without bytes, collisions, unreadable data.

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, StorageBackend};

use super::*;
use crate::FileStorage;

fn open() -> (tempfile::TempDir, FileStorage, Arc<dyn BlobStore>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let blobs = Arc::clone(storage.for_scope(&Scope::local()).unwrap().blobs());
    (dir, storage, blobs)
}

fn files(storage: &FileStorage, key: &str) -> (PathBuf, PathBuf) {
    let dir = storage.dir().join("scopes/local/blobs");
    let stem = file_stem(key);
    (dir.join(&stem), dir.join(format!("{stem}.meta.json")))
}

#[tokio::test]
async fn stores_the_bytes_verbatim_with_a_sidecar() {
    let (_dir, storage, blobs) = open();
    blobs
        .put("docs/Report.PDF", b"%PDF".to_vec(), Some("application/pdf"))
        .await
        .unwrap();
    let (data, meta) = files(&storage, "docs/Report.PDF");
    assert_eq!(std::fs::read(&data).unwrap(), b"%PDF");
    let sidecar: serde_json::Value = serde_json::from_slice(&std::fs::read(meta).unwrap()).unwrap();
    assert_eq!(
        sidecar,
        json!({"key": "docs/Report.PDF", "content_type": "application/pdf"})
    );
}

#[tokio::test]
async fn a_sidecar_without_bytes_reads_as_absent() {
    let (_dir, storage, blobs) = open();
    blobs.put("a", vec![1], None).await.unwrap();
    blobs.put("b", vec![2], None).await.unwrap();
    let (data, _) = files(&storage, "a");
    std::fs::remove_file(&data).unwrap();
    assert!(blobs.head("a").await.unwrap().is_none());
    assert!(blobs.get("a").await.unwrap().is_none());
    assert!(blobs.get_range("a", 0..1).await.unwrap().is_none());
    assert!(!blobs.delete("a").await.unwrap());
    let keys: Vec<_> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(keys, ["b"]);
}

#[tokio::test]
async fn a_sidecar_for_another_key_is_a_collision() {
    let (_dir, storage, blobs) = open();
    let key = "z".repeat(400);
    let (_, meta) = files(&storage, &key);
    std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
    std::fs::write(&meta, br#"{"key":"other"}"#).unwrap();
    assert_eq!(
        blobs.head(&key).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        blobs.put(&key, vec![], None).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn unreadable_bytes_are_a_backend_error() {
    let (_dir, storage, blobs) = open();
    blobs.put("a", vec![1, 2, 3], None).await.unwrap();
    let (data, _) = files(&storage, "a");
    std::fs::remove_file(&data).unwrap();
    // A directory where the bytes should be: its size reads, its content
    // does not.
    std::fs::create_dir(&data).unwrap();
    assert_eq!(blobs.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(
        blobs.get_range("a", 0..1).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(format!("{blobs:?}").contains("local"));
}

#[test]
fn a_missing_data_file_has_no_meta() {
    let dir = tempfile::tempdir().unwrap();
    let sidecar = Sidecar {
        key: "k".into(),
        content_type: None,
    };
    assert!(
        meta(&dir.path().join("missing"), sidecar.clone())
            .unwrap()
            .is_none()
    );
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(
        meta(&file.join("below"), sidecar).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_payload_is_refused_for_whole_and_ranged_reads() {
    let (dir, storage, blobs) = open();
    blobs.put("a", b"hello".to_vec(), None).await.unwrap();
    let (data, _) = files(&storage, "a");
    let victim = dir.path().join("victim");
    std::fs::rename(&data, &victim).unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    assert_eq!(blobs.get("a").await.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(
        blobs.get_range("a", 0..2).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}
