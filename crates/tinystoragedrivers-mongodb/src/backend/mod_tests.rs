//! Topology detection, declarations, and connection failures without a
//! server.

use super::specs::{REFRESH, decode_meta, index_models, scope_key_model, text_model};
use super::*;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, IndexSpec, SearchSpec};

#[test]
fn transactions_need_sessions_and_a_replica_set_or_router() {
    let sessions = doc! {"logicalSessionTimeoutMinutes": 30};
    let mut replica = sessions.clone();
    replica.insert("setName", "rs0");
    let mut router = sessions.clone();
    router.insert("msg", "isdbgrid");
    assert!(supports_transactions(&replica));
    assert!(supports_transactions(&router));
    assert!(!supports_transactions(&sessions), "standalone");
    assert!(
        !supports_transactions(&doc! {"setName": "rs0"}),
        "no sessions"
    );
    let mut other = sessions;
    other.insert("msg", "hello");
    assert!(!supports_transactions(&other));
}

#[test]
fn unique_indexes_lead_with_scope_and_skip_missing_fields() {
    let spec = CollectionSpec::new("users")
        .index(IndexSpec::new("by_email", ["email", "org.id"]).unique())
        .index(IndexSpec::new("by_age", ["age"]))
        .index(IndexSpec::new("odd", ["$weird"]));
    let models = index_models(&spec).unwrap();
    assert_eq!(
        models.len(),
        2,
        "a non-unique index over `$` paths is skipped"
    );
    assert_eq!(
        models[0].keys,
        doc! {"_scope": 1, "d.email": 1, "d.org.id": 1}
    );
    let options = models[0].options.as_ref().unwrap();
    assert_eq!(options.unique, Some(true));
    assert_eq!(
        options.partial_filter_expression,
        Some(doc! {"d.email": {"$exists": true}, "d.org.id": {"$exists": true}})
    );
    assert!(options.name.as_deref().unwrap().starts_with("_tsd_ix_"));
    assert_eq!(models[1].options.as_ref().unwrap().unique, None);

    let unique_operator = CollectionSpec::new("c").index(IndexSpec::new("bad", ["$x"]).unique());
    assert_eq!(
        index_models(&unique_operator).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(scope_key_model().keys, doc! {"_scope": 1, "_key": 1});
}

#[test]
fn text_indexes_cover_declared_fields_without_stemming() {
    let model = text_model(&SearchSpec {
        fields: vec!["title".into(), "$skip".into()],
    })
    .unwrap();
    assert_eq!(model.keys, doc! {"_scope": 1, "d.title": "text"});
    let options = model.options.unwrap();
    assert_eq!(options.default_language.as_deref(), Some("none"));
    assert_eq!(options.name.as_deref(), Some("_tsd_text"));
    assert!(
        text_model(&SearchSpec {
            fields: vec!["$x".into()]
        })
        .is_none()
    );
}

#[test]
fn declarations_round_trip_through_meta() {
    let spec = CollectionSpec::new("c").ttl("exp");
    let entry = doc! {"_id": "c", "spec": serde_json::to_string(&spec).unwrap(), "rev": 4_i64};
    assert_eq!(decode_meta(&entry).unwrap(), (spec, 4));
    for broken in [
        doc! {"_id": "c", "rev": 1_i64},
        doc! {"_id": "c", "spec": "{}", "rev": "x"},
        doc! {"_id": "c", "spec": "not json", "rev": 1_i64},
    ] {
        assert_eq!(
            decode_meta(&broken).unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }
    assert!(REFRESH.as_secs() > 0);
}

#[test]
fn the_system_clock_is_after_the_epoch() {
    assert!(system_clock() > 1_600_000_000_000);
}

#[tokio::test]
async fn connect_rejects_bad_input_without_echoing_it() {
    let bad_name = MongoStorage::connect("mongodb://h/db", "a.b")
        .await
        .unwrap_err();
    assert_eq!(bad_name.kind(), ErrorKind::InvalidInput);

    let malformed = MongoStorage::connect("mongodb://app:s3cret@h:notaport/db", "db")
        .await
        .unwrap_err();
    assert_eq!(malformed.kind(), ErrorKind::InvalidInput);
    assert!(!malformed.to_string().contains("s3cret"), "{malformed}");

    let unreachable = MongoStorage::connect(
        "mongodb://app:s3cret@127.0.0.1:1/db?serverSelectionTimeoutMS=100&connectTimeoutMS=100",
        "db",
    )
    .await
    .unwrap_err();
    assert_eq!(unreachable.kind(), ErrorKind::Unavailable);
    assert!(!unreachable.to_string().contains("s3cret"), "{unreachable}");
}
