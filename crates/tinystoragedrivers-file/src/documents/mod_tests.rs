//! Document edge cases beyond the conformance suite: expiry, file names,
//! collisions, corruption and cursors.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, IndexSpec, StorageBackend};

use super::*;
use crate::FileStorage;
use crate::encode::file_stem;

fn open() -> (tempfile::TempDir, FileStorage, Arc<dyn DocumentStore>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let docs = Arc::clone(storage.for_scope(&Scope::local()).unwrap().documents());
    (dir, storage, docs)
}

fn collection_dir(storage: &FileStorage, collection: &str) -> std::path::PathBuf {
    storage
        .dir()
        .join("scopes/local/docs")
        .join(crate::encode::dir_components(collection))
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_now = Arc::clone(&now);
    let storage = FileStorage::open_with_clock(
        dir.path(),
        Arc::new(move || clock_now.load(Ordering::SeqCst)),
    )
    .unwrap();
    let docs = Arc::clone(storage.for_scope(&Scope::local()).unwrap().documents());
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until"))
        .await
        .unwrap();
    let v1 = docs
        .put("leases", "l", json!({"until": 2_000}), Precondition::None)
        .await
        .unwrap();
    assert!(docs.get("leases", "l").await.unwrap().is_some());
    now.store(2_000, Ordering::SeqCst);
    assert!(docs.get("leases", "l").await.unwrap().is_none());
    assert_eq!(docs.count("leases", &Filter::All).await.unwrap(), 0);
    let v2 = docs
        .put("leases", "l", json!({"until": 9_000}), Precondition::Absent)
        .await
        .unwrap();
    assert!(v2 > v1, "versions keep rising across expiry");
    now.store(9_000, Ordering::SeqCst);
    assert!(
        !docs
            .delete("leases", "l", Precondition::None)
            .await
            .unwrap(),
        "deleting an expired document reports nothing removed"
    );
    assert!(
        !collection_dir(&storage, "leases").join("l.json").exists(),
        "but its file is gone"
    );
}

#[tokio::test]
async fn long_and_unusual_ids_round_trip() {
    let (_dir, storage, docs) = open();
    let long = "Ü/".repeat(150);
    for id in [long.as_str(), "..", "a/b", "CON", "Mixed Case"] {
        docs.put("c", id, json!({"id": id}), Precondition::Absent)
            .await
            .unwrap();
        assert_eq!(docs.get("c", id).await.unwrap().unwrap().doc["id"], id);
    }
    let all = docs.query("c", &Query::all()).await.unwrap();
    assert_eq!(all.items.len(), 5);
    let hashed = collection_dir(&storage, "c").join(format!("{}.json", file_stem(&long)));
    let stored: serde_json::Value =
        serde_json::from_slice(&std::fs::read(hashed).unwrap()).unwrap();
    assert_eq!(stored["id"], long, "the real id is kept inside the file");
    assert_eq!(stored["version"], 1);
}

#[tokio::test]
async fn long_collection_names_do_not_disturb_their_prefix() {
    let (_dir, _storage, docs) = open();
    // Encodes to exactly the chunk size: the longer name's first directory.
    let short = format!("{}aa", "A".repeat(66));
    assert_eq!(crate::encode::encode(&short).len(), MAX);
    let long = format!("{short}b");
    docs.put(&short, "x", json!({}), Precondition::None)
        .await
        .unwrap();
    docs.put(&long, "y", json!({}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.count(&short, &Filter::All).await.unwrap(), 1);
    docs.drop_collection(&short).await.unwrap();
    assert_eq!(docs.count(&short, &Filter::All).await.unwrap(), 0);
    assert_eq!(docs.count(&long, &Filter::All).await.unwrap(), 1);
    docs.drop_collection(&long).await.unwrap();
    docs.drop_collection("never-written").await.unwrap();
}

const MAX: usize = crate::encode::MAX_PLAIN;

#[tokio::test]
async fn a_file_holding_another_id_is_a_collision() {
    let (_dir, storage, docs) = open();
    let id = "x".repeat(300);
    let path = collection_dir(&storage, "c").join(format!("{}.json", file_stem(&id)));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, br#"{"id":"other","version":1,"doc":{}}"#).unwrap();
    assert_eq!(
        docs.get("c", &id).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        docs.put("c", &id, json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        docs.delete("c", &id, Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn a_spec_file_for_another_collection_is_a_collision() {
    let (_dir, storage, docs) = open();
    let name = "A".repeat(100);
    let path = storage
        .dir()
        .join("_meta/collections")
        .join(format!("{}.json", file_stem(&name)));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, br#"{"name":"other"}"#).unwrap();
    assert_eq!(
        docs.get(&name, "a").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        docs.ensure_collection(&CollectionSpec::new(&name))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn declarations_merge_and_persist() {
    let (_dir, _storage, docs) = open();
    docs.ensure_collection(
        &CollectionSpec::new("users").index(IndexSpec::new("by_email", ["email"]).unique()),
    )
    .await
    .unwrap();
    docs.ensure_collection(&CollectionSpec::new("users").ttl("expires_at"))
        .await
        .unwrap();
    docs.put("users", "a", json!({"email": "x"}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        docs.put("users", "b", json!({"email": "x"}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
    let clash = CollectionSpec::new("users").index(IndexSpec::new("by_email", ["mail"]));
    assert_eq!(
        docs.ensure_collection(&clash).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn an_exhausted_version_fails_the_write() {
    let (_dir, storage, docs) = open();
    let path = collection_dir(&storage, "c").join("max.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(r#"{{"id":"max","version":{},"doc":{{}}}}"#, u64::MAX),
    )
    .unwrap();
    assert_eq!(
        docs.put("c", "max", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn a_corrupt_document_is_a_serialization_error() {
    let (_dir, storage, docs) = open();
    let path = collection_dir(&storage, "c").join("bad.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"not json").unwrap();
    assert_eq!(
        docs.count("c", &Filter::All).await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn query_pages_past_the_end_are_empty() {
    let (_dir, _storage, docs) = open();
    docs.put("c", "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let past_end = format!("file:{:016x}:9", fingerprint("c", &Query::all()));
    let page = docs
        .query("c", &Query::all().after(Cursor(past_end)))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 0);
    assert!(page.next.is_none());
    for forged in ["file:x", "mem:0000000000000000:1"] {
        assert_eq!(
            docs.query("c", &Query::all().after(Cursor(forged.into())))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    let cursor = docs.query("c", &Query::all().limit(1)).await.unwrap().next;
    assert!(cursor.is_none(), "a single page has no cursor");
}

#[tokio::test]
async fn claims_and_searches_handle_empty_results() {
    let (_dir, _storage, docs) = open();
    assert!(
        docs.claim("c", &Filter::All, &[], &json!({"x": 1}))
            .await
            .unwrap()
            .is_none()
    );
    docs.ensure_collection(&CollectionSpec::new("notes").searchable(["t"]))
        .await
        .unwrap();
    docs.put("notes", "a", json!({"t": "hello"}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.search("notes", "  ,, ", 5).await.unwrap().len(), 0);
    assert_eq!(
        docs.search("bad name", "x", 5).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(format!("{docs:?}").contains("local"));
}
