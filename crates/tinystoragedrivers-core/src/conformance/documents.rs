//! Document port checks.

use serde_json::{Value, json};

use super::{empty, fails, has, ok, unique};
use crate::backend::ScopedStorage;
use crate::capabilities::Capability;
use crate::document::{
    CollectionSpec, Cursor, IndexSpec, Precondition, Query, Version, WriteOp, WriteResult,
};
use crate::error::ErrorKind;
use crate::filter::{Filter, Sort};

pub(super) async fn run(storage: &ScopedStorage) {
    crud(storage).await;
    preconditions(storage).await;
    validation(storage).await;
    unique_indexes(storage).await;
    recreation(storage).await;
    queries(storage).await;
    claims(storage).await;
    expiry(storage).await;
    batches(storage).await;
    search(storage).await;
}

async fn crud(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("crud");
    assert!(ok(docs.get(&coll, "a").await, "get missing").is_none());

    let v1 = ok(
        docs.put(&coll, "a", json!({"n": 1}), Precondition::None)
            .await,
        "create",
    );
    assert_eq!(v1, Version::FIRST, "a new document starts at version 1");
    let read = ok(docs.get(&coll, "a").await, "get").expect("document exists");
    assert_eq!(
        (read.id.as_str(), read.version, &read.doc),
        ("a", v1, &json!({"n": 1}))
    );

    let v2 = ok(
        docs.put(&coll, "a", json!({"n": 2}), Precondition::None)
            .await,
        "upsert",
    );
    assert!(v2 > v1, "versions rise on every write");

    assert!(ok(
        docs.delete(&coll, "a", Precondition::None).await,
        "delete"
    ));
    assert!(!ok(
        docs.delete(&coll, "a", Precondition::None).await,
        "delete missing"
    ));
    assert!(ok(docs.get(&coll, "a").await, "get deleted").is_none());

    ok(
        docs.put(&coll, "b", json!({}), Precondition::None).await,
        "put b",
    );
    ok(docs.drop_collection(&coll).await, "drop");
    assert_eq!(
        ok(docs.count(&coll, &Filter::All).await, "count dropped"),
        0
    );
}

async fn preconditions(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("pre");
    let v1 = ok(
        docs.put(&coll, "a", json!({"n": 1}), Precondition::Absent)
            .await,
        "insert",
    );
    fails(
        docs.put(&coll, "a", json!({"n": 9}), Precondition::Absent)
            .await,
        ErrorKind::Conflict,
        "insert over an existing document",
    );
    let v2 = ok(
        docs.put(&coll, "a", json!({"n": 2}), Precondition::Version(v1))
            .await,
        "cas at the current version",
    );
    fails(
        docs.put(&coll, "a", json!({"n": 3}), Precondition::Version(v1))
            .await,
        ErrorKind::Conflict,
        "cas at a stale version",
    );
    fails(
        docs.put(&coll, "z", json!({}), Precondition::Version(v1))
            .await,
        ErrorKind::Conflict,
        "cas on a missing document",
    );
    fails(
        docs.delete(&coll, "a", Precondition::Version(v1)).await,
        ErrorKind::Conflict,
        "delete at a stale version",
    );
    fails(
        docs.delete(&coll, "a", Precondition::Absent).await,
        ErrorKind::Conflict,
        "delete-if-absent on an existing document",
    );
    assert!(!ok(
        docs.delete(&coll, "z", Precondition::Absent).await,
        "delete-if-absent on a missing document"
    ));
    assert!(ok(
        docs.delete(&coll, "a", Precondition::Version(v2)).await,
        "delete at the current version"
    ));
}

async fn validation(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("valid");
    fails(
        docs.get("", "a").await,
        ErrorKind::InvalidInput,
        "empty collection",
    );
    fails(
        docs.get("bad name", "a").await,
        ErrorKind::InvalidInput,
        "spaced collection",
    );
    fails(
        docs.get("_tsd_meta", "a").await,
        ErrorKind::InvalidInput,
        "reserved collection",
    );
    fails(
        docs.get(&coll, "").await,
        ErrorKind::InvalidInput,
        "empty id",
    );
    fails(
        docs.put(&coll, "a", json!([1]), Precondition::None).await,
        ErrorKind::InvalidInput,
        "non-object body",
    );
    fails(
        docs.query(&coll, &Query::filter(Filter::eq("", 1))).await,
        ErrorKind::InvalidInput,
        "empty filter path",
    );
    fails(
        docs.query(&coll, &Query::all().after(Cursor("forged".into())))
            .await,
        ErrorKind::InvalidInput,
        "foreign cursor",
    );
    fails(
        docs.claim(&coll, &Filter::All, &[], &json!(1)).await,
        ErrorKind::InvalidInput,
        "non-object claim patch",
    );
    fails(
        docs.ensure_collection(
            &CollectionSpec::new(&coll).index(IndexSpec::new("i", Vec::<String>::new())),
        )
        .await,
        ErrorKind::InvalidInput,
        "index without fields",
    );
}

async fn unique_indexes(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("uniq");
    let spec = CollectionSpec::new(&coll).index(IndexSpec::new("by_email", ["email"]).unique());
    ok(docs.ensure_collection(&spec).await, "ensure");
    ok(
        docs.ensure_collection(&spec).await,
        "ensure again is a no-op",
    );
    ok(
        docs.put(&coll, "a", json!({"email": "x@y"}), Precondition::None)
            .await,
        "first",
    );
    fails(
        docs.put(&coll, "b", json!({"email": "x@y"}), Precondition::None)
            .await,
        ErrorKind::AlreadyExists,
        "duplicate unique value",
    );
    ok(
        docs.put(
            &coll,
            "a",
            json!({"email": "x@y", "n": 1}),
            Precondition::None,
        )
        .await,
        "rewriting the same document keeps its value",
    );
    ok(
        docs.put(&coll, "c", json!({}), Precondition::None).await,
        "missing field is unconstrained",
    );
    ok(
        docs.put(&coll, "d", json!({}), Precondition::None).await,
        "and so is a second one",
    );

    let dup = unique("uniq_late");
    for id in ["a", "b"] {
        ok(
            docs.put(&dup, id, json!({"email": "same"}), Precondition::None)
                .await,
            "seed duplicates",
        );
    }
    fails(
        docs.ensure_collection(
            &CollectionSpec::new(&dup).index(IndexSpec::new("by_email", ["email"]).unique()),
        )
        .await,
        ErrorKind::AlreadyExists,
        "a unique index the stored documents already violate",
    );
}

async fn recreation(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("recreate");
    let v1 = ok(
        docs.put(&coll, "a", json!({}), Precondition::None).await,
        "create",
    );
    assert!(ok(
        docs.delete(&coll, "a", Precondition::None).await,
        "delete"
    ));
    let v2 = ok(
        docs.put(&coll, "a", json!({}), Precondition::Absent).await,
        "recreate",
    );
    assert!(
        v2 > v1,
        "a recreated document continues its version sequence"
    );
    fails(
        docs.put(&coll, "a", json!({}), Precondition::Version(v1))
            .await,
        ErrorKind::Conflict,
        "a compare-and-swap from before the deletion",
    );
    ok(docs.drop_collection(&coll).await, "drop");
    let v3 = ok(
        docs.put(&coll, "a", json!({}), Precondition::Absent).await,
        "recreate after drop",
    );
    assert!(v3 > v2, "dropping a collection keeps version history");
}

async fn queries(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("query");
    for (id, state, rank) in [
        ("a", "queued", 3),
        ("b", "done", 1),
        ("c", "queued", 1),
        ("d", "queued", 2),
        ("e", "failed", 5),
    ] {
        ok(
            docs.put(
                &coll,
                id,
                json!({"state": state, "rank": rank, "meta": {"id": id}}),
                Precondition::None,
            )
            .await,
            "seed",
        );
    }

    let all = ok(docs.query(&coll, &Query::all()).await, "query all");
    let ids: Vec<_> = all.items.iter().map(|d| d.id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c", "d", "e"], "default order is by id");
    assert!(all.next.is_none());

    let queued = Query::filter(Filter::eq("state", "queued")).sort(Sort::asc("rank"));
    let ids: Vec<_> = ok(docs.query(&coll, &queued).await, "filtered")
        .items
        .into_iter()
        .map(|d| d.id)
        .collect();
    assert_eq!(ids, ["c", "d", "a"], "filter then sort");

    let desc = Query::all()
        .sort(Sort::desc("rank"))
        .sort(Sort::asc("state"));
    let ids: Vec<_> = ok(docs.query(&coll, &desc).await, "multi-key sort")
        .items
        .into_iter()
        .map(|d| d.id)
        .collect();
    assert_eq!(ids, ["e", "a", "d", "b", "c"]);

    let mut paged = Vec::new();
    let mut query = Query::all().sort(Sort::asc("rank")).limit(2);
    loop {
        let page = ok(docs.query(&coll, &query).await, "page");
        assert!(page.items.len() <= 2, "limit respected");
        paged.extend(page.items.into_iter().map(|d| d.id));
        match page.next {
            Some(cursor) => query = query.after(cursor),
            None => break,
        }
    }
    assert_eq!(
        paged,
        ["b", "c", "d", "a", "e"],
        "pages cover the result once"
    );

    filters(storage, &coll).await;
}

/// Filter shapes over the documents `queries` seeded.
async fn filters(storage: &ScopedStorage, coll: &str) {
    let docs = storage.documents();
    let nested = Query::filter(Filter::eq("meta.id", "d"));
    assert_eq!(
        ok(docs.query(coll, &nested).await, "nested path")
            .items
            .len(),
        1
    );
    let by_id = Query::filter(Filter::one_of("_id", ["a", "e", "zz"]));
    assert_eq!(
        ok(docs.query(coll, &by_id).await, "id filter").items.len(),
        2
    );
    let range = Filter::gte("rank", 2).and(Filter::lt("rank", 5));
    assert_eq!(ok(docs.count(coll, &range).await, "count range"), 2);
    let not_queued = Filter::eq("state", "queued").negate();
    assert_eq!(ok(docs.count(coll, &not_queued).await, "count negation"), 2);
    let either = Filter::eq("state", "done").or(Filter::eq("rank", 5));
    assert_eq!(ok(docs.count(coll, &either).await, "count disjunction"), 2);
    assert_eq!(
        ok(
            docs.count(coll, &Filter::exists("rank", false)).await,
            "count absent"
        ),
        0
    );

    assert_eq!(
        ok(
            docs.delete_where(coll, &Filter::eq("state", "queued"))
                .await,
            "delete_where"
        ),
        3
    );
    assert_eq!(
        ok(
            docs.count(coll, &Filter::All).await,
            "count after delete_where"
        ),
        2
    );
}

async fn claims(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("claim");
    for (id, at) in [("j1", 20), ("j2", 10), ("j3", 30)] {
        ok(
            docs.put(
                &coll,
                id,
                json!({"state": "queued", "run_at": at, "owner": {"pid": 0, "host": "h"}}),
                Precondition::None,
            )
            .await,
            "seed",
        );
    }
    let eligible = Filter::eq("state", "queued");
    let order = [Sort::asc("run_at")];
    let patch = json!({"state": "running", "owner": {"pid": 7}});

    let first = ok(docs.claim(&coll, &eligible, &order, &patch).await, "claim")
        .expect("a queued job is claimable");
    assert_eq!(first.id, "j2", "claims follow sort order");
    assert_eq!(first.doc["state"], "running");
    assert_eq!(
        first.doc["owner"],
        json!({"pid": 7, "host": "h"}),
        "patch merges nested objects"
    );
    assert_eq!(first.doc["run_at"], 10, "untouched fields survive");
    let stored = ok(docs.get(&coll, "j2").await, "re-read").expect("claimed job exists");
    assert_eq!(
        stored.version, first.version,
        "claim returns the stored version"
    );

    let second = ok(
        docs.claim(&coll, &eligible, &order, &patch).await,
        "claim again",
    )
    .expect("another job is claimable");
    assert_eq!(second.id, "j1", "a claimed job is not handed out twice");
    ok(
        docs.claim(&coll, &eligible, &order, &patch).await,
        "claim last",
    );
    assert!(
        ok(
            docs.claim(&coll, &eligible, &order, &patch).await,
            "claim exhausted"
        )
        .is_none(),
        "nothing left to claim"
    );
}

async fn expiry(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("ttl");
    let spec = CollectionSpec::new(&coll).ttl("expires_at");
    if !has(storage, Capability::Ttl) {
        fails(
            docs.ensure_collection(&spec).await,
            ErrorKind::Unsupported(Capability::Ttl),
            "ttl without the capability",
        );
        return;
    }
    ok(docs.ensure_collection(&spec).await, "ensure ttl");
    ok(
        docs.put(&coll, "old", json!({"expires_at": 1}), Precondition::None)
            .await,
        "expired",
    );
    ok(
        docs.put(
            &coll,
            "new",
            json!({"expires_at": 32_503_680_000_000_u64}),
            Precondition::None,
        )
        .await,
        "far future",
    );
    ok(
        docs.put(&coll, "forever", json!({}), Precondition::None)
            .await,
        "no expiry",
    );
    assert!(
        ok(docs.get(&coll, "old").await, "get expired").is_none(),
        "expired reads absent"
    );
    assert!(ok(docs.get(&coll, "new").await, "get live").is_some());
    assert_eq!(ok(docs.count(&coll, &Filter::All).await, "count live"), 2);
    ok(
        docs.put(&coll, "old", json!({"expires_at": 1}), Precondition::Absent)
            .await,
        "an expired document counts as absent",
    );
}

async fn batches(storage: &ScopedStorage) {
    let docs = storage.documents();
    let left = unique("batch_l");
    let right = unique("batch_r");
    let op = |collection: &str, id: &str, precondition| WriteOp::Put {
        collection: collection.to_owned(),
        id: id.to_owned(),
        doc: json!({"id": id}),
        precondition,
    };
    if !has(storage, Capability::Transactions) {
        fails(
            docs.atomic_batch(vec![op(&left, "a", Precondition::None)])
                .await,
            ErrorKind::Unsupported(Capability::Transactions),
            "batch without the capability",
        );
        return;
    }
    ok(
        docs.put(&left, "x", json!({}), Precondition::None).await,
        "seed",
    );
    let results = ok(
        docs.atomic_batch(vec![
            op(&left, "a", Precondition::Absent),
            op(&right, "b", Precondition::None),
            WriteOp::Delete {
                collection: left.clone(),
                id: "x".into(),
                precondition: Precondition::None,
            },
        ])
        .await,
        "batch",
    );
    assert_eq!(results.len(), 3);
    assert_eq!(results[2], WriteResult::Delete { removed: true });
    assert!(matches!(results[0], WriteResult::Put { .. }));
    assert!(ok(docs.get(&right, "b").await, "batched write landed").is_some());

    fails(
        docs.atomic_batch(vec![
            op(&right, "c", Precondition::None),
            op(&left, "a", Precondition::Absent),
        ])
        .await,
        ErrorKind::Conflict,
        "failing batch",
    );
    assert!(
        ok(docs.get(&right, "c").await, "rolled back").is_none(),
        "a failed batch applies nothing"
    );
}

async fn search(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("search");
    if !has(storage, Capability::FullText) {
        fails(
            docs.search(&coll, "anything", 10).await,
            ErrorKind::Unsupported(Capability::FullText),
            "search without the capability",
        );
        return;
    }
    fails(
        docs.search(&coll, "anything", 10).await,
        ErrorKind::InvalidInput,
        "search on a collection without search fields",
    );
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["title", "body"]))
            .await,
        "ensure searchable",
    );
    let seed: [(&str, Value); 3] = [
        (
            "a",
            json!({"title": "Quarterly planning", "body": "budget review"}),
        ),
        (
            "b",
            json!({"title": "Lunch", "body": "tacos", "secret": "budget"}),
        ),
        ("c", json!({"title": "Budget", "body": "numbers"})),
    ];
    for (id, doc) in seed {
        ok(docs.put(&coll, id, doc, Precondition::None).await, "seed");
    }
    let mut ids: Vec<_> = ok(docs.search(&coll, "budget", 10).await, "search")
        .into_iter()
        .map(|hit| hit.id)
        .collect();
    ids.sort();
    assert_eq!(ids, ["a", "c"], "only declared fields are searched");
    empty(
        &ok(docs.search(&coll, "nothing-matches", 10).await, "no hits"),
        "expected nothing",
    );
    assert_eq!(ok(docs.search(&coll, "budget", 1).await, "limit").len(), 1);
}
