//! Unit tests for [`Fence`]: validation and the pure check drivers apply.

use serde_json::json;

use super::*;
use crate::document::Version;
use crate::error::ErrorKind;

fn guard(doc: Value) -> Versioned<Value> {
    Versioned {
        id: "lease-1".to_owned(),
        version: Version(3),
        doc,
    }
}

fn lease_fence() -> Fence {
    Fence::epoch(
        Scope::new("cluster").unwrap(),
        "leases",
        "lease-1",
        "epoch",
        7,
    )
}

#[test]
fn exposes_its_address_and_filter() {
    let fence = lease_fence();
    assert_eq!(fence.scope().as_str(), "cluster");
    assert_eq!(fence.collection(), "leases");
    assert_eq!(fence.id(), "lease-1");
    assert_eq!(fence.filter(), &Filter::eq("epoch", 7));
}

#[test]
fn holds_while_the_guard_matches() {
    let fence = lease_fence();
    fence
        .check(Some(&guard(json!({"epoch": 7, "owner": "a"}))))
        .unwrap();
}

#[test]
fn an_absent_guard_is_fenced() {
    let error = lease_fence().check(None).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
    assert!(!error.is_retryable());
    assert!(error.message().contains("leases/lease-1"), "{error}");
}

#[test]
fn a_moved_guard_is_fenced() {
    let error = lease_fence()
        .check(Some(&guard(json!({"epoch": 8}))))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
    assert!(error.message().contains("version 3"), "{error}");
}

#[test]
fn a_filter_may_name_the_guard_id() {
    let fence = Fence::new(
        Scope::local(),
        "leases",
        "lease-1",
        Filter::eq(crate::filter::ID_FIELD, "lease-1"),
    );
    fence.check(Some(&guard(json!({})))).unwrap();
}

#[test]
fn validation_rejects_bad_addresses_and_filters() {
    lease_fence().validate().unwrap();
    let bad_collection = Fence::new(Scope::local(), "_tsd_x", "id", Filter::All);
    assert_eq!(
        bad_collection.validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let bad_id = Fence::new(Scope::local(), "leases", "", Filter::All);
    assert_eq!(
        bad_id.validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let bad_filter = Fence::new(Scope::local(), "leases", "id", Filter::eq("", 1));
    assert_eq!(
        bad_filter.validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
