//! Every filter, insert and pipeline built here carries the scope.

use super::*;
use mongodb::Client;
use mongodb::options::ClientOptions;

fn handle(scope: &str) -> ScopedCollection {
    // Building a client does not connect, so no server is needed; it does
    // need a runtime for its background monitor.
    let options = ClientOptions::builder()
        .hosts(vec![mongodb::options::ServerAddress::Tcp {
            host: "127.0.0.1".into(),
            port: Some(1),
        }])
        .build();
    let client = Client::with_options(options).unwrap();
    ScopedCollection::new(
        client.database("db").collection("docs"),
        Scope::new(scope).unwrap(),
    )
}

#[tokio::test]
async fn filters_always_carry_the_scope() {
    let docs = handle("alice");
    assert_eq!(docs.filter(Document::new()), doc! {"_scope": "alice"});
    assert_eq!(
        docs.filter(doc! {"_scope": "bob"}),
        doc! {"$and": [{"_scope": "alice"}, {"_scope": "bob"}]},
        "an inner filter naming another scope can only narrow, never widen"
    );
    assert_eq!(
        docs.filter(doc! {"$or": [{"a": 1}, {"b": 2}]}),
        doc! {"$and": [{"_scope": "alice"}, {"$or": [{"a": 1}, {"b": 2}]}]}
    );
}

#[tokio::test]
async fn inserts_and_pipelines_carry_the_scope() {
    let docs = handle("alice");
    assert_eq!(
        docs.stamp(doc! {"_scope": "mallory", "x": 1}),
        doc! {"_scope": "alice", "x": 1}
    );
    let pipeline = docs.pipeline(doc! {"x": 1}, vec![doc! {"$limit": 1}]);
    assert_eq!(
        pipeline,
        vec![
            doc! {"$match": {"$and": [{"_scope": "alice"}, {"x": 1}]}},
            doc! {"$limit": 1},
        ]
    );
}

#[test]
fn scoped_filter_takes_any_field() {
    let scope = Scope::new("s").unwrap();
    assert_eq!(
        scoped_filter("metadata._scope", &scope, doc! {"filename": "k"}),
        doc! {"$and": [{"metadata._scope": "s"}, {"filename": "k"}]}
    );
}

#[test]
fn pipelines_may_only_use_single_collection_stages() {
    for allowed in [
        doc! {"$match": {}},
        doc! {"$group": {"_id": "$s"}},
        doc! {"$limit": 1},
    ] {
        assert!(check_stage(&allowed).is_ok(), "{allowed:?}");
    }
    for refused in [
        doc! {"$unionWith": "other"},
        doc! {"$lookup": {"from": "other"}},
        doc! {"$merge": {"into": "other"}},
        doc! {"$out": "other"},
        doc! {"$match": {}, "$limit": 1},
        doc! {},
    ] {
        assert!(check_stage(&refused).is_err(), "{refused:?}");
    }
}

#[test]
fn updates_cannot_touch_the_scope() {
    assert!(check_update(&doc! {"$set": {"d": {}, "_del": true}}).is_ok());
    assert!(check_update(&doc! {"$inc": {"g": 1}}).is_ok());
    assert!(check_update(&doc! {"$set": {"_scopes": 1}}).is_ok());
    for refused in [
        doc! {"$set": {"_scope": "bob"}},
        doc! {"$unset": {"_scope": ""}},
        doc! {"$set": {"_scope.x": 1}},
        doc! {"_key": "replacement"},
        doc! {"$set": 1},
    ] {
        assert!(check_update(&refused).is_err(), "{refused:?}");
    }
}

#[tokio::test]
async fn refused_updates_and_stages_never_reach_the_server() {
    let docs = handle("alice");
    // The handle points at a closed port: an attempt to send would hang on
    // server selection, so these return before any I/O.
    assert!(
        docs.update_one(doc! {}, doc! {"$set": {"_scope": "bob"}}, false, None)
            .await
            .is_err()
    );
    assert!(
        docs.update_many(doc! {}, doc! {"$set": {"_scope": "bob"}}, None)
            .await
            .is_err()
    );
    assert!(
        docs.aggregate(doc! {}, vec![doc! {"$unionWith": "x"}])
            .await
            .is_err()
    );
}
