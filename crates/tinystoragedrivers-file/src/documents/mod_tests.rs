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
    let tombstone: serde_json::Value = serde_json::from_slice(
        &std::fs::read(collection_dir(&storage, "leases").join("l.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        tombstone,
        json!({"id": "l", "version": v2.0}),
        "its file becomes a tombstone"
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

#[tokio::test]
async fn deleted_ids_keep_their_version_across_a_reopen() {
    let (dir, _storage, docs) = open();
    docs.put("c", "a", json!({"n": 1}), Precondition::None)
        .await
        .unwrap();
    let v2 = docs
        .put("c", "a", json!({"n": 2}), Precondition::None)
        .await
        .unwrap();
    docs.put("c", "b", json!({"n": 3}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        docs.delete_where("c", &Filter::eq("n", 2)).await.unwrap(),
        1
    );
    assert!(
        !docs.delete("c", "a", Precondition::None).await.unwrap(),
        "a tombstone is nothing to delete"
    );
    docs.drop_collection("c").await.unwrap();
    assert_eq!(docs.count("c", &Filter::All).await.unwrap(), 0);

    let reopened = FileStorage::open(dir.path()).unwrap();
    let docs = Arc::clone(reopened.for_scope(&Scope::local()).unwrap().documents());
    assert_eq!(
        docs.put("c", "a", json!({}), Precondition::Version(v2))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict,
        "a CAS prepared before the deletion fails"
    );
    let v3 = docs
        .put("c", "a", json!({}), Precondition::Absent)
        .await
        .unwrap();
    assert_eq!(v3.0, 3);
    assert_eq!(
        docs.put("c", "b", json!({}), Precondition::Absent)
            .await
            .unwrap()
            .0,
        2,
        "dropping keeps history too"
    );
}

#[tokio::test]
async fn a_unique_index_is_refused_when_any_scope_violates_it() {
    let (_dir, storage, docs) = open();
    // A scope long enough to need continuation directories.
    let far = Scope::new("T".repeat(120)).unwrap();
    let far_docs = Arc::clone(storage.for_scope(&far).unwrap().documents());
    docs.put("users", "a", json!({"email": "x"}), Precondition::None)
        .await
        .unwrap();
    for id in ["a", "b"] {
        far_docs
            .put("users", id, json!({"email": "y"}), Precondition::None)
            .await
            .unwrap();
    }
    let unique = CollectionSpec::new("users").index(IndexSpec::new("by_email", ["email"]).unique());
    assert_eq!(
        docs.ensure_collection(&unique).await.unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    docs.put("users", "b", json!({"email": "x"}), Precondition::None)
        .await
        .expect("nothing was declared");
    assert!(
        !storage.dir().join("_meta/collections/users.json").exists(),
        "the refused declaration was not persisted"
    );

    far_docs
        .delete("users", "b", Precondition::None)
        .await
        .unwrap();
    docs.delete("users", "b", Precondition::None).await.unwrap();
    docs.ensure_collection(&unique).await.unwrap();
    assert_eq!(
        far_docs
            .put("users", "c", json!({"email": "y"}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
}

#[tokio::test]
async fn an_unreadable_scopes_directory_fails_the_declaration() {
    let (_dir, storage, docs) = open();
    std::fs::write(storage.dir().join("scopes"), b"not a directory").unwrap();
    let unique = CollectionSpec::new("u").index(IndexSpec::new("i", ["k"]).unique());
    assert_eq!(
        docs.ensure_collection(&unique).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}
