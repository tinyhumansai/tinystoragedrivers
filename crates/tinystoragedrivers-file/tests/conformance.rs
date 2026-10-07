//! The file driver against the shared conformance suite, and across reopens.

use serde_json::json;
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, Filter, Precondition, Query, Scope, StorageBackend,
    WriteOp, conformance,
};
use tinystoragedrivers_file::FileStorage;

#[tokio::test]
async fn passes_the_conformance_suite() {
    let dir = tempfile::tempdir().unwrap();
    conformance::run(&FileStorage::open(dir.path()).unwrap(), false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn passes_the_conformance_suite_on_a_current_thread_runtime() {
    let dir = tempfile::tempdir().unwrap();
    conformance::run(&FileStorage::open(dir.path()).unwrap(), false).await;
}

#[test]
fn works_without_a_tokio_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    // A minimal executor: the futures never suspend outside a runtime.
    let version = block_on(docs.put("c", "a", json!({}), Precondition::None)).unwrap();
    assert_eq!(version.0, 1);
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

/// Write one of everything through a store that is dropped afterwards.
async fn seed(dir: &std::path::Path, alice: &Scope) -> tinystoragedrivers_core::Result<()> {
    let storage = FileStorage::open(dir)?;
    storage
        .for_scope(alice)?
        .documents()
        .ensure_collection(&CollectionSpec::new("Notes").searchable(["text"]))
        .await?;
    let scoped = storage.for_scope(alice)?;
    scoped
        .documents()
        .put(
            "Notes",
            "n/1",
            json!({"text": "hello world"}),
            Precondition::Absent,
        )
        .await?;
    scoped
        .streams()
        .append_batch("thread:1", vec![json!(1), json!(2), json!(3)])
        .await?;
    scoped.streams().truncate_before("thread:1", 1).await?;
    scoped
        .blobs()
        .put("files/a.txt", b"bytes".to_vec(), Some("text/plain"))
        .await?;
    let named = storage.database("costs")?.for_scope(alice)?;
    named
        .documents()
        .put("usage", "u", json!({"n": 7}), Precondition::None)
        .await?;
    Ok(())
}

#[tokio::test]
async fn data_survives_reopening_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let alice = Scope::new("tenant:alice/1").unwrap();
    seed(dir.path(), &alice).await.unwrap();

    let storage = FileStorage::open(dir.path()).unwrap();
    let scoped = storage.for_scope(&alice).unwrap();
    let doc = scoped
        .documents()
        .get("Notes", "n/1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(doc.version.0, 1);
    assert_eq!(doc.doc["text"], "hello world");
    let hits = scoped
        .documents()
        .search("Notes", "world", 5)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "the collection declaration persisted");
    let v2 = scoped
        .documents()
        .put(
            "Notes",
            "n/1",
            json!({"text": "again"}),
            Precondition::Version(doc.version),
        )
        .await
        .unwrap();
    assert_eq!(v2.0, 2, "versions continue after a reopen");

    assert_eq!(scoped.streams().len("thread:1").await.unwrap(), 3);
    let window = scoped
        .streams()
        .read_window("thread:1", 0, 10)
        .await
        .unwrap();
    assert_eq!(window.iter().map(|e| e.offset).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(
        scoped.streams().append("thread:1", json!(4)).await.unwrap(),
        3
    );
    assert_eq!(scoped.streams().streams("").await.unwrap(), ["thread:1"]);

    let blob = scoped.blobs().get("files/a.txt").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"bytes");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));

    let named = storage
        .database("costs")
        .unwrap()
        .for_scope(&alice)
        .unwrap();
    assert_eq!(
        named
            .documents()
            .count("usage", &Filter::All)
            .await
            .unwrap(),
        1
    );
    assert!(
        scoped
            .documents()
            .get("usage", "u")
            .await
            .unwrap()
            .is_none(),
        "named databases stay apart from the root"
    );
}

#[tokio::test]
async fn cursors_survive_reopening_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let first = FileStorage::open(dir.path()).unwrap();
    let docs = first
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    for id in ["a", "b", "c"] {
        docs.put("c", id, json!({}), Precondition::None)
            .await
            .unwrap();
    }
    let cursor = docs
        .query("c", &Query::all().limit(1))
        .await
        .unwrap()
        .next
        .unwrap();

    let again = FileStorage::open(dir.path()).unwrap();
    let docs = again
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let page = docs
        .query("c", &Query::all().limit(1).after(cursor))
        .await
        .unwrap();
    assert_eq!(page.items[0].id, "b");
}

#[tokio::test]
async fn reports_its_driver_and_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(storage.driver(), "file");
    let caps = storage.capabilities();
    assert!(caps.contains(Capability::Ttl));
    assert!(caps.contains(Capability::FullText));
    assert!(!caps.contains(Capability::Transactions));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(docs.capabilities(), caps);
    let error = docs
        .atomic_batch(vec![WriteOp::Delete {
            collection: "c".into(),
            id: "a".into(),
            precondition: Precondition::None,
        }])
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    assert_eq!(storage.dir(), dir.path().canonicalize().unwrap());
    assert!(format!("{storage:?}").starts_with("FileStorage"));
}
