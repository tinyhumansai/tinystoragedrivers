//! The memory driver passes the conformance suite, and its own edge cases.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

use super::*;
use crate::{CollectionSpec, ErrorKind, Filter, Precondition, Query};

#[tokio::test]
async fn passes_the_conformance_suite() {
    crate::conformance::run(&MemoryStorage::new(), false).await;
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_now = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock_now.load(Ordering::SeqCst)));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until"))
        .await
        .unwrap();
    let v1 = docs
        .put("leases", "l", json!({"until": 2_000}), Precondition::None)
        .await
        .unwrap();
    assert!(docs.get("leases", "l").await.unwrap().is_some());

    now.store(2_000, Ordering::SeqCst);
    assert!(
        docs.get("leases", "l").await.unwrap().is_none(),
        "expires at its time"
    );
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
}

#[tokio::test]
async fn query_pages_past_the_end_are_empty() {
    let storage = MemoryStorage::default();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.put("c", "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let page = docs
        .query("c", &Query::all().after(crate::Cursor("mem:9".into())))
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert!(page.next.is_none());
    let error = docs
        .query("c", &Query::all().after(crate::Cursor("mem:x".into())))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn search_ignores_text_without_tokens() {
    let docs = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("notes").searchable(["t"]))
        .await
        .unwrap();
    docs.put("notes", "a", json!({"t": "hello"}), Precondition::None)
        .await
        .unwrap();
    assert!(docs.search("notes", "  ,, ", 5).await.unwrap().is_empty());
}

#[test]
fn reports_its_driver_and_capabilities() {
    let storage = MemoryStorage::new();
    assert_eq!(storage.driver(), "memory");
    assert_eq!(storage.capabilities(), Capabilities::all());
    assert_eq!(format!("{storage:?}"), "MemoryStorage { .. }");
    let docs = storage.for_scope(&Scope::local()).unwrap();
    assert!(format!("{:?}", docs.documents()).contains("local"));
    assert!(system_clock() > 0);
}

#[test]
fn a_poisoned_lock_is_a_backend_error() {
    let db = Arc::new(MemoryDb::default());
    let poisoner = Arc::clone(&db);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.state.lock().unwrap();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(db.lock().unwrap_err().kind(), ErrorKind::Backend);
}
