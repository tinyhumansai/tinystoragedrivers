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
        fields: vec!["title".into()],
    })
    .unwrap()
    .unwrap();
    assert_eq!(model.keys, doc! {"_scope": 1, "d.title": "text"});
    let options = model.options.unwrap();
    assert_eq!(options.default_language.as_deref(), Some("none"));
    assert_eq!(options.name.as_deref(), Some("_tsd_text"));
    assert!(
        text_model(&SearchSpec { fields: vec![] })
            .unwrap()
            .is_none()
    );
    for fields in [
        vec!["$x".to_owned()],
        vec!["ok".to_owned(), "a.$y".to_owned()],
    ] {
        assert_eq!(
            text_model(&SearchSpec { fields }).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
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
}

// Live checks of what the driver stores. They read and plant raw documents,
// so they live here, beside the private layout, rather than in `tests/`.

use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::TryStreamExt as _;
use serde_json::json;
use tinystoragedrivers_core::{Filter, Precondition, Scope};

/// A backend on `TSD_MONGO_URL`, or `None` (with a note) without one.
async fn live(test: &str) -> Option<MongoStorage> {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("{test}: TSD_MONGO_URL is not set; skipping");
        return None;
    };
    let rest = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
    let path = rest.split_once('/').map_or("", |(_, path)| path);
    let name = path
        .split(['?', '/'])
        .next()
        .filter(|name| !name.is_empty());
    Some(
        MongoStorage::connect(&url, name.unwrap_or("tsd_test"))
            .await
            .unwrap(),
    )
}

fn unique(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "unit_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn live_records_are_stamped_scoped_and_buried() {
    let Some(backend) = live("live_records_are_stamped_scoped_and_buried").await else {
        return;
    };
    let coll = unique("stamp");
    let raw = backend.shared.raw(&coll);
    let alice = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let docs = alice.documents();
    docs.put(
        &coll,
        "k",
        json!({"_scope": "mallory", "d": 1}),
        Precondition::None,
    )
    .await
    .unwrap();
    let stored = raw.find_one(doc! {}).await.unwrap().unwrap();
    assert_eq!(stored.get_str("_scope").unwrap(), "alice");
    assert_eq!(stored.get_str("_key").unwrap(), "k");
    assert_eq!(stored.get_i64("_v").unwrap(), 1);
    assert_eq!(
        stored.get_document("d").unwrap(),
        &doc! {"_scope": "mallory", "d": 1_i64},
        "body fields never collide with the driver's"
    );

    // Another tenant's row planted in the same collection stays invisible.
    raw.insert_one(
        doc! {"_id": {"s": "bob", "k": "x"}, "_scope": "bob", "_key": "x", "_v": 1_i64, "d": {}},
    )
    .await
    .unwrap();
    assert_eq!(docs.count(&coll, &Filter::All).await.unwrap(), 1);
    assert!(docs.get(&coll, "x").await.unwrap().is_none());

    // Removal leaves a tombstone at the last version, still in alice's scope.
    assert_eq!(docs.delete_where(&coll, &Filter::All).await.unwrap(), 1);
    let tombstone = raw
        .find_one(doc! {"_scope": "alice"})
        .await
        .unwrap()
        .unwrap();
    assert!(tombstone.get_bool("_del").unwrap());
    assert_eq!(tombstone.get_i64("_v").unwrap(), 1);
    assert_eq!(tombstone.get_document("d").unwrap(), &doc! {});
    assert_eq!(
        raw.count_documents(doc! {"_scope": "bob"}).await.unwrap(),
        1
    );
    assert_eq!(docs.count(&coll, &Filter::All).await.unwrap(), 0);
    assert_eq!(
        docs.put(&coll, "k", json!({}), Precondition::Absent)
            .await
            .unwrap()
            .0,
        2
    );

    let named = backend.database("stamped").unwrap();
    named
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap();
    let prefixed = backend.shared.raw(&format!("stamped:{coll}"));
    assert_eq!(prefixed.count_documents(doc! {}).await.unwrap(), 1);
}

#[tokio::test]
async fn live_versions_stop_at_the_int64_ceiling() {
    let Some(backend) = live("live_versions_stop_at_the_int64_ceiling").await else {
        return;
    };
    let coll = unique("ceiling");
    let raw = backend.shared.raw(&coll);
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    let docs = scoped.documents();
    docs.put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap();
    raw.update_one(doc! {}, doc! {"$set": {"_v": i64::MAX}})
        .await
        .unwrap();
    let error = docs
        .put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let read = docs.get(&coll, "k").await.unwrap().unwrap();
    assert_eq!(read.version.0, i64::MAX.unsigned_abs(), "nothing changed");

    raw.update_one(doc! {}, doc! {"$set": {"_v": "broken"}})
        .await
        .unwrap();
    assert_eq!(
        docs.get(&coll, "k").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        docs.put(&coll, "big", json!({"n": u64::MAX}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn live_a_corrupt_declaration_is_a_serialization_error() {
    let Some(backend) = live("live_a_corrupt_declaration_is_a_serialization_error").await else {
        return;
    };
    let coll = unique("meta");
    backend
        .shared
        .raw(crate::naming::META)
        .insert_one(doc! {"_id": &coll, "spec": 3, "rev": 1_i64})
        .await
        .unwrap();
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        scoped.documents().get(&coll, "a").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn live_an_append_racing_a_delete_never_leaves_a_gap() {
    let Some(backend) = live("live_an_append_racing_a_delete_never_leaves_a_gap").await else {
        return;
    };
    let stream = unique("race");
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    let streams = scoped.streams();
    streams
        .append_batch(&stream, vec![json!(1), json!(2)])
        .await
        .unwrap();
    assert!(streams.delete_stream(&stream).await.unwrap());
    // An appender that read generation 0 and the end 2 before the delete
    // lands its segment afterwards.
    let late = crate::streams::new_segment(&stream, 0, 2, &[json!("late")]).unwrap();
    let raw = backend.shared.raw(crate::naming::STREAMS);
    let mut stamped = late;
    stamped.insert("_scope", "local");
    raw.insert_one(stamped).await.unwrap();

    assert_eq!(
        streams.len(&stream).await.unwrap(),
        0,
        "the late append is ordered before the delete"
    );
    assert_eq!(streams.read_window(&stream, 0, 10).await.unwrap(), []);
    assert_eq!(
        streams.streams(&stream).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        streams.append(&stream, json!("new")).await.unwrap(),
        0,
        "no gap"
    );
    assert!(streams.delete_stream(&stream).await.unwrap());
    let leftovers: Vec<Document> = raw
        .find(doc! {"s": &stream})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        leftovers,
        Vec::<Document>::new(),
        "the next delete clears leftovers"
    );
}
