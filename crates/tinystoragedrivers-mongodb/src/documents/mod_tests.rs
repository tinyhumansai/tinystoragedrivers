//! The pure rules behind every write: stored shape, preconditions, versions
//! and expiry.

use super::stored::*;
use mongodb::bson::{Bson, doc};
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Precondition, Scope, Version};

fn scope() -> Scope {
    Scope::new("alice").unwrap()
}

#[test]
fn encodes_and_decodes_the_stored_shape() {
    let body = json!({"n": 1});
    let encoded = encode(&scope(), "k", Version(3), body.as_object().unwrap()).unwrap();
    assert_eq!(
        encoded,
        doc! {"_id": {"s": "alice", "k": "k"}, "_key": "k", "_v": 3_i64, "d": {"n": 1_i64}}
    );
    let stored = Stored::decode(&encoded).unwrap();
    assert_eq!(
        stored,
        Stored {
            key: "k".into(),
            version: Version(3),
            body: body.clone()
        }
    );
    let versioned = stored.into_versioned();
    assert_eq!(
        (versioned.id.as_str(), versioned.version, versioned.doc),
        ("k", Version(3), body)
    );

    assert_eq!(
        encode(&scope(), "k", Version(u64::MAX), &serde_json::Map::new())
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        encode(
            &scope(),
            "k",
            Version(1),
            json!({"n": u64::MAX}).as_object().unwrap()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn rejects_stored_documents_missing_driver_fields() {
    for broken in [
        doc! {"_v": 1_i64, "d": {}},
        doc! {"_key": "k", "d": {}},
        doc! {"_key": "k", "_v": 0_i64, "d": {}},
        doc! {"_key": "k", "_v": -4_i64, "d": {}},
        doc! {"_key": "k", "_v": 1_i64},
        doc! {"_key": "k", "_v": 1_i64, "d": {"x": Bson::Double(f64::NAN)}},
    ] {
        assert_eq!(
            Stored::decode(&broken).unwrap_err().kind(),
            ErrorKind::Serialization,
            "{broken:?}"
        );
    }
}

#[test]
fn preconditions_match_the_memory_driver() {
    let v = Version(2);
    assert!(check(Precondition::None, Some(v)).is_ok());
    assert!(check(Precondition::Absent, None).is_ok());
    assert!(check(Precondition::Version(v), Some(v)).is_ok());
    for (precondition, live) in [
        (Precondition::Absent, Some(v)),
        (Precondition::Version(Version(1)), Some(v)),
        (Precondition::Version(v), None),
    ] {
        let error = check(precondition, live).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(!error.message().contains("alice"));
    }
}

#[test]
fn versions_rise_until_int64_runs_out() {
    assert_eq!(next_version(None).unwrap(), Version::FIRST);
    assert_eq!(next_version(Some(Version(4))).unwrap(), Version(5));
    let ceiling = Version(u64::try_from(i64::MAX).unwrap());
    assert_eq!(next_version(Some(Version(ceiling.0 - 1))).unwrap(), ceiling);
    assert_eq!(
        next_version(Some(ceiling)).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        next_version(Some(Version(u64::MAX))).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn selectors_pin_a_version() {
    assert_eq!(
        at_version(&scope(), "k", Version(2)),
        doc! {"_id": {"s": "alice", "k": "k"}, "_v": 2_i64}
    );
    assert_eq!(
        at_version(&scope(), "k", Version(u64::MAX)).get("_v"),
        Some(&Bson::Null)
    );
}

#[test]
fn expiry_uses_numbers_at_or_below_now() {
    let spec = CollectionSpec::new("c").ttl("exp");
    let stored = |body| Stored {
        key: "k".into(),
        version: Version(1),
        body,
    };
    assert!(!is_live(&spec, &stored(json!({"exp": 10})), 10));
    assert!(is_live(&spec, &stored(json!({"exp": 11})), 10));
    assert!(is_live(&spec, &stored(json!({"exp": "5"})), 10));
    assert!(is_live(&spec, &stored(json!({})), 10));
    let forever = CollectionSpec::new("c");
    assert!(is_live(&forever, &stored(json!({"exp": 1})), 10));
    assert!(expired_filter(&forever, 10).is_none());
    assert!(expired_filter(&spec, 10).is_some());
}
