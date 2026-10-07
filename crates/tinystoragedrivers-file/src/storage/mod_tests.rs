//! Opening, databases, and the per-database lock.

use super::*;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn opening_a_file_as_the_root_fails() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(
        FileStorage::open(&file).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn rejects_invalid_database_names() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(
        storage.database("Bad Name").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let named = storage.database("one").unwrap();
    assert_eq!(named.driver(), "file");
    assert_eq!(named.capabilities(), CAPABILITIES);
    let nested = named.database("two").unwrap();
    assert!(format!("{nested:?}").contains("databases"));
}

#[test]
fn handles_on_one_directory_share_a_lock() {
    let dir = tempfile::tempdir().unwrap();
    let a = FileStorage::open(dir.path()).unwrap();
    let b = FileStorage::open(dir.path().join(".")).unwrap();
    assert!(Arc::ptr_eq(&a.db.lock, &b.db.lock));
    assert!(format!("{:?}", a.db).starts_with("Db"));
    assert!(system_clock() > 0);
}

#[tokio::test]
async fn a_poisoned_lock_is_taken_over() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let db = Arc::clone(&storage.db);
    let _ = std::thread::spawn(move || {
        let _guard = db.lock.lock().unwrap();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(storage.db.run(|_| Ok(5)).await.unwrap(), 5);
}

#[tokio::test]
async fn a_panicking_task_is_a_backend_error() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let error = storage
        .db
        .run(|_| -> Result<()> { panic!("task panics") })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}
