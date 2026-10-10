//! Capability set arithmetic, requirement checks and rendering.

use super::*;
use crate::ErrorKind;

#[test]
fn starts_empty_and_accumulates() {
    let caps = Capabilities::none();
    assert_eq!(caps, Capabilities::default());
    assert!(!caps.contains(Capability::FullText));

    let caps = caps.with(Capability::FullText).with(Capability::Ttl);
    assert!(caps.contains(Capability::FullText));
    assert!(caps.contains(Capability::Ttl));
    assert!(!caps.contains(Capability::Transactions));

    let caps = caps.without(Capability::FullText);
    assert!(!caps.contains(Capability::FullText));
}

#[test]
fn all_holds_every_capability() {
    let all = Capabilities::all();
    assert_eq!(
        all.iter().collect::<Vec<_>>(),
        vec![
            Capability::FullText,
            Capability::Transactions,
            Capability::Ttl,
            Capability::Fencing
        ]
    );
}

#[test]
fn require_names_the_missing_capability() {
    let error = Capabilities::none()
        .require(Capability::Transactions)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    assert!(error.message().contains("transactions"));
    assert!(
        Capabilities::all()
            .require(Capability::Transactions)
            .is_ok()
    );
}

#[test]
fn renders_names() {
    assert_eq!(Capability::FullText.to_string(), "full_text");
    assert_eq!(Capability::Transactions.to_string(), "transactions");
    assert_eq!(Capability::Ttl.to_string(), "ttl");
    assert_eq!(Capability::Fencing.to_string(), "fencing");
    assert_eq!(
        format!("{:?}", Capabilities::none().with(Capability::Ttl)),
        "{Ttl}"
    );
}
