//! Scope validation, the local scope, and the serde representation.

use super::*;
use crate::ErrorKind;

#[test]
fn accepts_printable_values() {
    let scope = Scope::new("user:42/agent-a").unwrap();
    assert_eq!(scope.as_str(), "user:42/agent-a");
    assert_eq!(scope.as_ref(), "user:42/agent-a");
    assert_eq!(scope.to_string(), "user:42/agent-a");
    assert_eq!(format!("{scope:?}"), "Scope(\"user:42/agent-a\")");
    assert!(!scope.is_local());
}

#[test]
fn rejects_empty_long_and_spaced_values() {
    for bad in [
        String::new(),
        "a".repeat(MAX_SCOPE_LEN + 1),
        "has space".to_owned(),
        "tab\there".to_owned(),
        "nul\0".to_owned(),
    ] {
        let error = Scope::new(&bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
    assert!(Scope::new("a".repeat(MAX_SCOPE_LEN)).is_ok());
}

#[test]
fn local_is_a_fixed_value() {
    let local = Scope::local();
    assert!(local.is_local());
    assert_eq!(local, Scope::new("local").unwrap());
    assert_eq!(local.as_str(), Scope::LOCAL);
}

#[test]
fn serializes_as_a_plain_string() {
    let scope = Scope::new("tenant-1").unwrap();
    assert_eq!(serde_json::to_string(&scope).unwrap(), "\"tenant-1\"");
    let back: Scope = serde_json::from_str("\"tenant-1\"").unwrap();
    assert_eq!(back, scope);
    assert!(serde_json::from_str::<Scope>("\"\"").is_err());
}
