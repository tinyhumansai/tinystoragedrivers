//! The MongoDB driver must select, count and order exactly what the memory
//! driver does, over documents chosen to trip MongoDB's own semantics: array
//! traversal, "contains" equality, null versus missing, mixed types, signed
//! zero and integers beyond 2^53.

#![allow(
    clippy::unwrap_used,
    reason = "test helpers report a failed call by panicking"
)]

mod support;

use serde_json::{Value, json};
use support::{connect, skip};
use tinystoragedrivers_core::{
    DocumentStore, DocumentStoreExt, Filter, MemoryStorage, Precondition, Query, Scope, Sort,
    StorageBackend,
};

fn seed() -> Vec<(&'static str, Value)> {
    vec![
        ("a", json!({"n": 1, "s": "b"})),
        ("b", json!({"n": 1.0})),
        ("c", json!({"n": -0.0})),
        ("d", json!({"n": 0})),
        ("e", json!({"n": "1"})),
        ("f", json!({"n": [1, 2]})),
        ("g", json!({"n": null})),
        ("h", json!({})),
        ("i", json!({"n": true})),
        ("j", json!({"n": {"x": 1}})),
        ("k", json!({"o": {"p": 1}})),
        ("l", json!({"o": [{"p": 1}]})),
        ("m", json!({"o": {"p": [1]}})),
        ("n", json!({"t": ["x", "y"], "o": {"1": "z"}})),
        ("p", json!({"t": {"0": "x"}, "s": "a"})),
        ("q", json!({"n": 9_007_199_254_740_993_i64})),
        ("r", json!({"n": 9_007_199_254_740_992.0})),
        ("s", json!({"n": false})),
        ("u", json!({"n": [], "s": "b"})),
        ("v", json!({"n": {"y": 2, "x": 1}})),
        ("w", json!({"n": [[1, 2]]})),
        ("x", json!({"n": [null]})),
        ("y", json!({"t": [{"0": "x"}, "q"]})),
    ]
}

fn filters() -> Vec<Filter> {
    vec![
        Filter::All,
        Filter::eq("n", 1),
        Filter::eq("n", 0),
        Filter::eq("n", -0.0),
        Filter::eq("n", Value::Null),
        Filter::eq("n", json!([1, 2])),
        Filter::eq("n", json!({"x": 1})),
        Filter::eq("n", json!({"x": 1, "y": 2})),
        Filter::eq("n", "1"),
        Filter::eq("n", true),
        Filter::eq("n", 9_007_199_254_740_993_i64),
        Filter::eq("n", u64::MAX),
        Filter::ne("n", 1),
        Filter::ne("n", json!([1, 2])),
        Filter::one_of("n", [json!(1), json!("1"), Value::Null]),
        Filter::one_of("n", Vec::<Value>::new()),
        Filter::gt("n", 0),
        Filter::gte("n", 1),
        Filter::lt("n", 2),
        Filter::gt("n", 9_007_199_254_740_992_i64),
        Filter::gte("n", 9_007_199_254_740_992.0),
        Filter::lte("n", Value::Null),
        Filter::gte("n", Value::Null),
        Filter::gte("n", ""),
        Filter::gt("n", false),
        Filter::gte("n", json!([1])),
        Filter::lt("n", json!({"z": 0})),
        Filter::lt("n", u64::MAX),
        Filter::Range {
            field: "n".into(),
            gt: Some(json!(0)),
            gte: None,
            lt: Some(json!("z")),
            lte: None,
        },
        Filter::exists("n", true),
        Filter::exists("n", false),
        Filter::exists("o.p", true),
        Filter::exists("n.x", true),
        Filter::eq("o.p", 1),
        Filter::eq("o.p", json!([1])),
        Filter::eq("t.0", "x"),
        Filter::eq("t.1", "y"),
        Filter::ne("t.0", "x"),
        Filter::eq("o.1", "z"),
        Filter::eq("n.0", 1),
        Filter::eq("n.x", 1),
        Filter::eq("n", 1).negate(),
        Filter::eq("n", 1).or(Filter::eq("o.p", 1)),
        Filter::eq("n", 1).and(Filter::exists("s", true)),
        Filter::eq("_id", "a"),
        Filter::eq("_id", 3),
        Filter::gt("_id", "m"),
        Filter::lt("_id", 1),
        Filter::exists("_id", false),
        Filter::one_of("_id", ["a", "zz"]),
    ]
}

fn sorts() -> Vec<Vec<Sort>> {
    vec![
        vec![],
        vec![Sort::asc("n")],
        vec![Sort::desc("n")],
        vec![Sort::asc("s"), Sort::desc("_id")],
        vec![Sort::desc("s")],
        vec![Sort::asc("o.p")],
        vec![Sort::asc("t.0")],
        vec![Sort::desc("_id")],
    ]
}

async fn ids(docs: &dyn DocumentStore, coll: &str, query: &Query) -> Vec<String> {
    docs.query_all(coll, query)
        .await
        .unwrap()
        .into_iter()
        .map(|found| found.id)
        .collect()
}

#[tokio::test]
async fn live_filters_sorts_and_pages_agree_with_memory() {
    let Some(backend) = connect().await else {
        return skip("live_filters_sorts_and_pages_agree_with_memory");
    };
    let memory = MemoryStorage::new();
    let scope = Scope::new("differential").unwrap();
    let mongo = backend.for_scope(&scope).unwrap();
    let reference = memory.for_scope(&scope).unwrap();
    let coll = format!("diff_{}", std::process::id());
    mongo.documents().drop_collection(&coll).await.unwrap();
    for (id, doc) in seed() {
        for store in [mongo.documents(), reference.documents()] {
            store
                .put(&coll, id, doc.clone(), Precondition::None)
                .await
                .unwrap();
        }
    }

    for filter in filters() {
        let want = ids(
            reference.documents().as_ref(),
            &coll,
            &Query::filter(filter.clone()),
        )
        .await;
        let got = ids(
            mongo.documents().as_ref(),
            &coll,
            &Query::filter(filter.clone()),
        )
        .await;
        assert_eq!(got, want, "query {filter:?}");
        let count = mongo.documents().count(&coll, &filter).await.unwrap();
        assert_eq!(count, want.len() as u64, "count {filter:?}");
        for sort in sorts() {
            let mut query = Query::filter(filter.clone()).limit(4);
            query.sort.clone_from(&sort);
            let want = ids(reference.documents().as_ref(), &coll, &query).await;
            let got = ids(mongo.documents().as_ref(), &coll, &query).await;
            assert_eq!(got, want, "paged {filter:?} by {sort:?}");
        }
    }

    for filter in [Filter::eq("t.0", "x"), Filter::gte("n", 1), Filter::All] {
        let want = reference
            .documents()
            .delete_where(&coll, &filter)
            .await
            .unwrap();
        let got = mongo
            .documents()
            .delete_where(&coll, &filter)
            .await
            .unwrap();
        assert_eq!(got, want, "delete_where {filter:?}");
    }
}
