//! Layouts, persistence, the clock, and handle construction.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, Precondition};

use super::*;

#[test]
fn picks_the_layout_from_the_extension() {
    assert!(is_file_path(Path::new("/x/state.db")));
    assert!(is_file_path(Path::new("/x/state.SQLITE")));
    assert!(!is_file_path(Path::new("/x/state")));
    assert!(!is_file_path(Path::new("/x/state.json")));
}

#[tokio::test]
async fn directory_mode_gives_each_database_its_own_file() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    assert!(storage.path().ends_with(ROOT_FILE));
    storage.database("flows").unwrap();
    assert!(dir.path().join("flows.db").exists());
    assert_eq!(storage.driver(), "sqlite");
    assert_eq!(storage.capabilities(), Capabilities::all());
    assert!(format!("{storage:?}").contains("storage.db"));
}

#[tokio::test]
async fn file_mode_prefixes_database_tables() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("one.sqlite");
    let storage = SqliteStorage::open(&file).unwrap();
    let flows = storage.database("flows").unwrap();
    flows
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .put("runs", "r1", json!({}), Precondition::None)
        .await
        .unwrap();
    let tables: i64 = storage
        .native()
        .with_connection(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'flows___tsd_docs'",
                [],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(tables, 1);
    assert_eq!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "db"))
            .count(),
        0
    );
}

#[tokio::test]
async fn data_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let storage = SqliteStorage::open(dir.path()).unwrap();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .put("c", "a", json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
        scoped
            .streams()
            .append("log", json!("first"))
            .await
            .unwrap();
        scoped.blobs().put("k", vec![1, 2, 3], None).await.unwrap();
    }
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let doc = scoped.documents().get("c", "a").await.unwrap().unwrap();
    assert_eq!(doc.doc, json!({"n": 1}));
    assert_eq!(scoped.streams().len("log").await.unwrap(), 1);
    assert_eq!(
        scoped.blobs().get("k").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let reading = Arc::clone(&now);
    let storage = SqliteStorage::open(dir.path())
        .unwrap()
        .with_clock(Arc::new(move || reading.load(Ordering::SeqCst)));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until").searchable(["t"]))
        .await
        .unwrap();
    docs.put(
        "leases",
        "l",
        json!({"until": 2_000, "t": "lease"}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 1);
    now.store(2_000, Ordering::SeqCst);
    assert!(docs.get("leases", "l").await.unwrap().is_none());
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 0);
    assert!(system_clock() > 0);
}

#[test]
fn rejects_invalid_database_names() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    assert!(storage.database("Bad Name").is_err());
}
