//! Database names and the scoped handle bundle.

use super::*;
use crate::MemoryStorage;

#[test]
fn database_names_validate() {
    assert!(validate_database("approvals").is_ok());
    assert!(validate_database("flows-v2_1").is_ok());
    for bad in ["", "Upper", "dot.ted", "sp ace", &"a".repeat(65)] {
        assert!(validate_database(bad).is_err(), "{bad}");
    }
}

#[test]
fn scoped_storage_exposes_its_parts() {
    let scope = Scope::new("t1").unwrap();
    let storage = MemoryStorage::new().for_scope(&scope).unwrap();
    assert_eq!(storage.scope(), &scope);
    assert_eq!(storage.driver(), "memory");
    assert!(
        storage
            .documents()
            .capabilities()
            .contains(crate::Capability::Ttl)
    );
    let _ = (storage.streams(), storage.blobs());
    let rendered = format!("{storage:?}");
    assert!(
        rendered.contains("t1") && rendered.contains("memory"),
        "{rendered}"
    );
}
