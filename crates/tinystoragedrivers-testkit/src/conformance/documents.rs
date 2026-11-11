//! Document port checks.

use serde_json::{Value, json};

use super::{empty, fails, has, ok, unique};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::Capability;
use tinystoragedrivers_types::ErrorKind;
use tinystoragedrivers_types::{
    CollectionSpec, Cursor, IndexSpec, Precondition, Query, Version, WriteOp, WriteOptions,
    WriteResult,
};
use tinystoragedrivers_types::{Filter, Sort};

/// Runs document behavior checks against a single document handle.
///
/// # Panics
/// Panics when the document driver violates a required behavior.
pub async fn run(storage: &dyn DocumentStore) {
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
    collection_listing(storage).await;
}

async fn crud(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn collection_listing(storage: &dyn DocumentStore) {
    let docs = storage;
    if !has(storage.capabilities(), Capability::CollectionEnumeration) {
        fails(
            docs.collections("").await,
            ErrorKind::Unsupported(Capability::CollectionEnumeration),
            "collections without the capability",
        );
        return;
    }
    let prefix = unique("collections");
    let first = format!("{prefix}_first");
    let second = format!("{prefix}_second");
    let escaped = format!("{prefix}.Upper");
    let unrelated = unique("unrelated_collection");

    empty(
        &ok(docs.collections(&prefix).await, "list empty collections"),
        "an empty scope has no collections",
    );
    ok(
        docs.put(&second, "b", json!({}), Precondition::None).await,
        "put second collection",
    );
    ok(
        docs.put(&unrelated, "c", json!({}), Precondition::None)
            .await,
        "put unrelated collection",
    );
    ok(
        docs.put(&first, "a", json!({}), Precondition::None).await,
        "put first collection",
    );
    ok(
        docs.put(&escaped, "d", json!({}), Precondition::None).await,
        "put escaped collection",
    );

    let mut expected = vec![first.clone(), second, escaped];
    expected.sort();

    assert_eq!(
        ok(docs.collections(&prefix).await, "list matching collections"),
        expected.clone(),
        "collection names are prefix-filtered and sorted"
    );
    ok(
        docs.delete(&first, "a", Precondition::None).await,
        "delete only record",
    );
    assert_eq!(
        ok(docs.collections(&prefix).await, "list tombstone collection"),
        expected,
        "tombstones remain enumerable for complete cleanup"
    );
}

async fn preconditions(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn validation(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn unique_indexes(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn recreation(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn queries(storage: &dyn DocumentStore) {
    let docs = storage;
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

    keyset_continuations_survive_prefix_mutations(storage).await;
    keyset_ties_follow_total_sort_order(storage).await;

    filters(storage, &coll).await;
}

async fn keyset_continuations_survive_prefix_mutations(storage: &dyn DocumentStore) {
    let docs = storage;
    for mutation in ["insert", "delete"] {
        let coll = unique("keyset_mutation");
        for (id, rank) in [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)] {
            ok(
                docs.put(&coll, id, json!({"rank": rank}), Precondition::None)
                    .await,
                "seed keyset mutation document",
            );
        }

        let query = Query::all().sort(Sort::asc("rank")).limit(2);
        let first = ok(docs.query(&coll, &query).await, "first keyset page");
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let cursor = first.next.expect("more documents follow the first page");

        // Moving the prefix must not move the continuation point. Offset
        // cursors skip `b` after insertion and skip `c` after deletion.
        if mutation == "insert" {
            ok(
                docs.put(&coll, "before", json!({"rank": 0}), Precondition::None)
                    .await,
                "insert before cursor",
            );
        } else {
            assert!(ok(
                docs.delete(&coll, "a", Precondition::None).await,
                "delete before cursor",
            ));
        }

        let second = ok(
            docs.query(&coll, &query.clone().after(cursor)).await,
            "continue after prefix mutation",
        );
        assert_eq!(
            second
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["c", "d"],
            "the continuation remains strictly after the last returned sort key"
        );
    }
}

async fn keyset_ties_follow_total_sort_order(storage: &dyn DocumentStore) {
    let docs = storage;
    let coll = unique("keyset_ties");
    for (id, body) in [
        ("a", json!({"primary": 1, "secondary": null})),
        ("b", json!({"primary": 1})),
        ("c", json!({"primary": 1, "secondary": 2})),
        ("d", json!({"primary": 2, "secondary": 1})),
        ("e", json!({"primary": 2, "secondary": 3})),
    ] {
        ok(
            docs.put(&coll, id, body, Precondition::None).await,
            "seed keyset tie document",
        );
    }

    // Exercise an implicit id tie-break, descending values, mixed directions,
    // and missing/null values. A page boundary may split any tied group.
    for (sorts, expected) in [
        (vec![Sort::asc("primary")], ["a", "b", "c", "d", "e"]),
        (vec![Sort::desc("primary")], ["d", "e", "a", "b", "c"]),
        (
            vec![Sort::asc("primary"), Sort::desc("secondary")],
            ["c", "a", "b", "e", "d"],
        ),
        (
            vec![Sort::desc("secondary"), Sort::asc("primary")],
            ["e", "c", "d", "a", "b"],
        ),
        (vec![], ["a", "b", "c", "d", "e"]),
    ] {
        let mut query = Query::all().limit(2);
        query.sort = sorts;
        let mut ids = Vec::new();
        loop {
            let page = ok(docs.query(&coll, &query).await, "keyset tie page");
            ids.extend(page.items.into_iter().map(|item| item.id));
            match page.next {
                Some(cursor) => query = query.after(cursor),
                None => break,
            }
        }
        assert_eq!(
            ids, expected,
            "keyset pages preserve the complete sort order"
        );
    }

    assert_mixed_keyset_values(storage).await;
    assert_literal_keyset_values(storage).await;
}

async fn assert_mixed_keyset_values(storage: &dyn DocumentStore) {
    let docs = storage;
    let mixed = unique("keyset_mixed_values");
    for (id, body) in [
        ("a", json!({})),
        ("b", json!({"value": null})),
        ("c", json!({"value": false})),
        ("d", json!({"value": true})),
        ("e", json!({"value": 1})),
        ("f", json!({"value": "x"})),
        ("g", json!({"value": []})),
        ("h", json!({"value": {}})),
    ] {
        ok(
            docs.put(&mixed, id, body, Precondition::None).await,
            "seed mixed keyset value",
        );
    }
    for (sort, expected) in [
        (Sort::asc("value"), ["a", "b", "c", "d", "e", "f", "g", "h"]),
        (
            Sort::desc("value"),
            ["h", "g", "f", "e", "d", "c", "a", "b"],
        ),
    ] {
        let mut query = Query::all().sort(sort).limit(3);
        let mut ids = Vec::new();
        loop {
            let page = ok(docs.query(&mixed, &query).await, "mixed-value keyset page");
            ids.extend(page.items.into_iter().map(|item| item.id));
            match page.next {
                Some(cursor) => query = query.after(cursor),
                None => break,
            }
        }
        assert_eq!(ids, expected, "nullable mixed JSON ordering is stable");
    }
}

async fn assert_literal_keyset_values(storage: &dyn DocumentStore) {
    let docs = storage;
    let literals = unique("keyset_literal_values");
    for (id, value) in [
        ("$$ID", "$$ROOT"),
        ("$$key", "$$ROOT"),
        ("$key", "$field"),
        ("a", "$field"),
        ("z", "x"),
    ] {
        ok(
            docs.put(&literals, id, json!({"value": value}), Precondition::None)
                .await,
            "seed dollar-prefixed keyset value",
        );
    }
    let mut query = Query::all().sort(Sort::asc("value")).limit(1);
    let mut ids = Vec::new();
    loop {
        let page = ok(docs.query(&literals, &query).await, "literal keyset page");
        ids.extend(page.items.into_iter().map(|item| item.id));
        match page.next {
            Some(cursor) => query = query.after(cursor),
            None => break,
        }
    }
    assert_eq!(ids, ["$$ID", "$$key", "$key", "a", "z"]);
}

/// Filter shapes over the documents `queries` seeded.
async fn filters(storage: &dyn DocumentStore, coll: &str) {
    let docs = storage;
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

async fn claims(storage: &dyn DocumentStore) {
    let docs = storage;
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

async fn expiry(storage: &dyn DocumentStore) {
    let docs = storage;
    let coll = unique("ttl");
    let spec = CollectionSpec::new(&coll).ttl("expires_at");
    if !has(storage.capabilities(), Capability::Ttl) {
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

async fn batches(storage: &dyn DocumentStore) {
    let docs = storage;
    let left = unique("batch_l");
    let right = unique("batch_r");
    let op = |collection: &str, id: &str, precondition| WriteOp::Put {
        collection: collection.to_owned(),
        id: id.to_owned(),
        doc: json!({"id": id}),
        precondition,
    };
    if !has(storage.capabilities(), Capability::Transactions) {
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

    if has(storage.capabilities(), Capability::RecordMetadata) {
        batched_options(docs, &right).await;
    }
}

/// A batched `PutWith` records its options exactly as a single `put_with`,
/// and a batched plain `Put` writes the defaults over them.
async fn batched_options(docs: &dyn DocumentStore, coll: &str) {
    const EXPIRES_MS: i64 = 4_102_444_800_000;
    ok(
        docs.atomic_batch(vec![WriteOp::PutWith {
            collection: coll.to_owned(),
            id: "with".into(),
            doc: json!({"v": 1}),
            precondition: Precondition::Absent,
            options: WriteOptions::new().schema(3).expires_ms(EXPIRES_MS),
        }])
        .await,
        "batched put_with",
    );
    let found =
        ok(docs.get(coll, "with").await, "get batched put_with").expect("batched put_with landed");
    assert_eq!(
        found.meta.schema, 3,
        "a batched put_with records its schema"
    );
    assert_eq!(
        found.meta.expires_ms,
        Some(EXPIRES_MS),
        "a batched put_with records its expiry"
    );

    ok(
        docs.atomic_batch(vec![WriteOp::Put {
            collection: coll.to_owned(),
            id: "with".into(),
            doc: json!({"v": 2}),
            precondition: Precondition::None,
        }])
        .await,
        "batched plain put",
    );
    let found = ok(docs.get(coll, "with").await, "get batched put").expect("batched put landed");
    assert_eq!(
        found.meta.schema, 0,
        "a batched plain put declares no schema"
    );
    assert_eq!(
        found.meta.expires_ms, None,
        "a batched plain put clears the expiry"
    );
}

async fn search(storage: &dyn DocumentStore) {
    let docs = storage;
    let coll = unique("search");
    if !has(storage.capabilities(), Capability::FullText) {
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
