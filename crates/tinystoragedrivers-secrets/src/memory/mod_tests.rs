//! The memory driver against the conformance suite, plus binary values.

use super::*;
use crate::testkit::secrets_conformance;

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let store = MemorySecrets::new();
    secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "memory");
}

#[tokio::test]
async fn holds_binary_values_and_redacts_debug() {
    let store = MemorySecrets::new();
    store.set("bin", &[0, 0xff, 0x80]).await.unwrap();
    assert_eq!(
        store.get("bin").await.unwrap().unwrap().as_slice(),
        [0, 0xff, 0x80]
    );
    assert_eq!(format!("{store:?}"), "MemorySecrets { len: 1 }");
}

#[test]
fn a_poisoned_lock_is_recovered() {
    let store = std::sync::Arc::new(MemorySecrets::new());
    let clone = std::sync::Arc::clone(&store);
    let _ = std::thread::spawn(move || {
        let _guard = clone.entries();
        panic!("poison the lock");
    })
    .join();
    assert!(store.entries().is_empty());
}
