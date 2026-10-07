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
    assert_eq!(docs.name(), "docs");
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
