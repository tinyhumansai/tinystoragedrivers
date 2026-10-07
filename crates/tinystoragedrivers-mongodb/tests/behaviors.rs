//! Driver behaviors the conformance suite does not reach: injected clocks,
//! sweeping, version exhaustion, concurrency, and what is actually stored.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "test helpers report a failed call by panicking"
)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mongodb::bson::{Document, doc};
use serde_json::json;
use support::{connect, database, skip, url};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, Filter, IndexSpec, Precondition, Query, Scope, Sort,
    StorageBackend,
};
use tinystoragedrivers_mongodb::MongoStorage;

fn name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "live_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn raw(collection: &str) -> mongodb::Collection<Document> {
    let url = url().expect("only called by live tests");
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    client.database(&database(&url)).collection(collection)
}

#[tokio::test]
async fn live_expiry_follows_the_injected_clock_and_sweeps() {
    let Some(backend) = connect().await else {
        return skip("live_expiry_follows_the_injected_clock_and_sweeps");
    };
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let backend = backend.with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let scope = Scope::new("expiry").unwrap();
    let docs = backend.for_scope(&scope).unwrap();
    let docs = docs.documents();
    let coll = name("ttl");
    docs.ensure_collection(
        &CollectionSpec::new(&coll)
            .ttl("exp")
            .index(IndexSpec::new("by_tag", ["tag"]).unique()),
    )
    .await
    .unwrap();
    let v1 = docs
        .put(
            &coll,
            "a",
            json!({"exp": 2_000, "tag": "t"}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs.put(&coll, "b", json!({"exp": [1]}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.count(&coll, &Filter::All).await.unwrap(), 2);

    now.store(2_000, Ordering::SeqCst);
    assert!(
        docs.get(&coll, "a").await.unwrap().is_none(),
        "expires at its time"
    );
    assert!(
        docs.get(&coll, "b").await.unwrap().is_some(),
        "an array is not a time"
    );
    let page = docs
        .query(&coll, &Query::all().sort(Sort::asc("exp")))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(
        docs.search(&coll, "x", 5).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    // The expired document no longer holds its unique value.
    docs.put(&coll, "c", json!({"tag": "t"}), Precondition::Absent)
        .await
        .unwrap();
    let again = docs
        .put(&coll, "a", json!({}), Precondition::Absent)
        .await
        .unwrap();
    assert!(again > v1, "a swept id continues its versions");

    docs.put(&coll, "d", json!({"exp": 10}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(backend.sweep_expired(&scope, &coll).await.unwrap(), 1);
    assert_eq!(backend.sweep_expired(&scope, &coll).await.unwrap(), 0);
    let plain = name("plain");
    assert_eq!(backend.sweep_expired(&scope, &plain).await.unwrap(), 0);
    assert_eq!(
        backend
            .sweep_expired(&scope, "bad name")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn live_records_are_stamped_and_scoped() {
    let Some(backend) = connect().await else {
        return skip("live_records_are_stamped_and_scoped");
    };
    let coll = name("stamp");
    let alice = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    alice
        .documents()
        .put(
            &coll,
            "k",
            json!({"_scope": "mallory", "d": 1}),
            Precondition::None,
        )
        .await
        .unwrap();
    let stored = raw(&coll).await.find_one(doc! {}).await.unwrap().unwrap();
    assert_eq!(stored.get_str("_scope").unwrap(), "alice");
    assert_eq!(stored.get_str("_key").unwrap(), "k");
    assert_eq!(stored.get_i64("_v").unwrap(), 1);
    assert_eq!(
        stored.get_document("d").unwrap(),
        &doc! {"_scope": "mallory", "d": 1_i64},
        "body fields never collide with the driver's"
    );

    // A foreign document planted in the same collection stays invisible.
    raw(&coll)
        .await
        .insert_one(doc! {"_id": {"s": "bob", "k": "x"}, "_scope": "bob", "_key": "x", "_v": 1_i64, "d": {}})
        .await
        .unwrap();
    let docs = alice.documents();
    assert_eq!(docs.count(&coll, &Filter::All).await.unwrap(), 1);
    assert!(docs.get(&coll, "x").await.unwrap().is_none());
    assert_eq!(docs.delete_where(&coll, &Filter::All).await.unwrap(), 1);
    assert_eq!(raw(&coll).await.count_documents(doc! {}).await.unwrap(), 1);

    let named = backend.database("stamped").unwrap();
    let scoped = named.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap();
    let prefixed = raw(&format!("stamped:{coll}")).await;
    assert_eq!(prefixed.count_documents(doc! {}).await.unwrap(), 1);
}

#[tokio::test]
async fn live_versions_stop_at_the_int64_ceiling() {
    let Some(backend) = connect().await else {
        return skip("live_versions_stop_at_the_int64_ceiling");
    };
    let coll = name("ceiling");
    let docs = backend.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    docs.put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap();
    raw(&coll)
        .await
        .update_one(doc! {}, doc! {"$set": {"_v": i64::MAX}})
        .await
        .unwrap();
    let error = docs
        .put(&coll, "k", json!({}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let read = docs.get(&coll, "k").await.unwrap().unwrap();
    assert_eq!(read.version.0, i64::MAX as u64, "nothing changed");

    raw(&coll)
        .await
        .update_one(doc! {}, doc! {"$set": {"_v": "broken"}})
        .await
        .unwrap();
    let error = docs.get(&coll, "k").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);

    let error = docs
        .put(&coll, "big", json!({"n": u64::MAX}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
}

#[tokio::test]
async fn live_concurrent_claims_and_appends_never_collide() {
    let Some(backend) = connect().await else {
        return skip("live_concurrent_claims_and_appends_never_collide");
    };
    let storage = backend.for_scope(&Scope::new("racers").unwrap()).unwrap();
    let coll = name("race");
    for i in 0..20 {
        storage
            .documents()
            .put(
                &coll,
                &format!("j{i:02}"),
                json!({"state": "queued", "at": i}),
                Precondition::None,
            )
            .await
            .unwrap();
    }
    let stream = name("race_stream");
    let mut tasks = Vec::new();
    for worker in 0..8 {
        let storage = storage.clone();
        let coll = coll.clone();
        let stream = stream.clone();
        tasks.push(tokio::spawn(async move {
            let mut claimed = Vec::new();
            while let Some(job) = storage
                .documents()
                .claim(
                    &coll,
                    &Filter::eq("state", "queued"),
                    &[Sort::asc("at")],
                    &json!({"state": "running", "by": worker}),
                )
                .await
                .unwrap()
            {
                claimed.push(job.id);
            }
            let first = storage
                .streams()
                .append_batch(&stream, vec![json!(worker), json!(worker)])
                .await
                .unwrap();
            (claimed, first)
        }));
    }
    let mut all = Vec::new();
    let mut starts = Vec::new();
    for task in tasks {
        let (claimed, first) = task.await.unwrap();
        all.extend(claimed);
        starts.push(first);
    }
    all.sort();
    let expected: Vec<String> = (0..20).map(|i| format!("j{i:02}")).collect();
    assert_eq!(all, expected, "every job claimed exactly once");

    starts.sort_unstable();
    assert_eq!(starts, (0..8).map(|i| i * 2).collect::<Vec<u64>>());
    let window = storage
        .streams()
        .read_window(&stream, 0, 100)
        .await
        .unwrap();
    assert_eq!(window.len(), 16);
    for pair in window.chunks(2) {
        assert_eq!(pair[0].value, pair[1].value, "a batch is contiguous");
    }
    assert_eq!(
        storage.streams().read_window(&stream, 3, 0).await.unwrap(),
        []
    );
    assert_eq!(
        storage.streams().read_window(&stream, 3, 2).await.unwrap()[0].offset,
        3
    );
}

#[tokio::test]
async fn live_search_and_declarations() {
    let Some(backend) = connect().await else {
        return skip("live_search_and_declarations");
    };
    let docs = backend.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let coll = name("search");
    docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["title"]))
        .await
        .unwrap();
    docs.put(
        &coll,
        "a",
        json!({"title": "alpha", "body": "beta"}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(docs.search(&coll, "beta", 5).await.unwrap(), []);
    docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["body"]))
        .await
        .unwrap();
    let hits = docs.search(&coll, "beta -alpha \"x\"", 5).await.unwrap();
    assert_eq!(hits.len(), 1, "operators are not passed through");
    assert!(hits[0].score > 0.0);
    assert_eq!(docs.search(&coll, "beta", 0).await.unwrap(), []);
    assert_eq!(docs.search(&coll, "  ", 5).await.unwrap(), []);

    let bad = CollectionSpec::new(&coll).index(IndexSpec::new("op", ["$x"]).unique());
    assert_eq!(
        docs.ensure_collection(&bad).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let hint = CollectionSpec::new(&coll)
        .index(IndexSpec::new("hint", ["$x"]))
        .searchable(["$y"]);
    docs.ensure_collection(&hint).await.unwrap();
    let changed = CollectionSpec::new(&coll).index(IndexSpec::new("hint", ["other"]));
    assert_eq!(
        docs.ensure_collection(&changed).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    raw("_tsd_meta")
        .await
        .update_one(doc! {"_id": &coll}, doc! {"$set": {"spec": 3}})
        .await
        .unwrap();
    let fresh = MongoStorage::connect(&url().unwrap(), &database(&url().unwrap()))
        .await
        .unwrap();
    let fresh = fresh.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        fresh.documents().get(&coll, "a").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn live_batches_roll_back_and_report_conflicts() {
    let Some(backend) = connect().await else {
        return skip("live_batches_roll_back_and_report_conflicts");
    };
    let docs = backend.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let coll = name("batch");
    let v1 = docs
        .put(&coll, "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let ops = vec![
        tinystoragedrivers_core::WriteOp::Delete {
            collection: coll.clone(),
            id: "a".into(),
            precondition: Precondition::Version(v1),
        },
        tinystoragedrivers_core::WriteOp::Put {
            collection: coll.clone(),
            id: "a".into(),
            doc: json!({"again": true}),
            precondition: Precondition::Absent,
        },
    ];
    let results = docs.atomic_batch(ops).await.unwrap();
    assert_eq!(
        results[1],
        tinystoragedrivers_core::WriteResult::Put {
            version: tinystoragedrivers_core::Version(2)
        },
        "a delete then recreate inside one batch continues the versions"
    );
    let error = docs
        .atomic_batch(vec![tinystoragedrivers_core::WriteOp::Delete {
            collection: "bad name".into(),
            id: "a".into(),
            precondition: Precondition::None,
        }])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn live_blob_ranges_span_chunks() {
    let Some(backend) = connect().await else {
        return skip("live_blob_ranges_span_chunks");
    };
    let blobs = backend.for_scope(&Scope::local()).unwrap();
    let blobs = blobs.blobs();
    let key = format!("{}/big.bin", name("blob"));
    let bytes: Vec<u8> = (0..700_000_u32).map(|i| (i % 251) as u8).collect();
    blobs.put(&key, bytes.clone(), None).await.unwrap();
    let range = blobs
        .get_range(&key, 261_000..523_000)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(range, bytes[261_000..523_000]);
    assert_eq!(
        blobs.get(&key).await.unwrap().unwrap().bytes.len(),
        bytes.len()
    );
    blobs
        .put(&key, vec![], Some("application/x-empty"))
        .await
        .unwrap();
    assert_eq!(
        blobs.get_range(&key, 0..10).await.unwrap().unwrap(),
        Vec::<u8>::new()
    );
}

#[tokio::test]
async fn live_backend_reports_its_driver_and_capabilities() {
    let Some(backend) = connect().await else {
        return skip("live_backend_reports_its_driver_and_capabilities");
    };
    assert_eq!(backend.driver(), "mongodb");
    assert!(backend.capabilities().contains(Capability::Transactions));
    assert!(backend.capabilities().contains(Capability::Ttl));
    let plain = backend.clone().without_transactions();
    assert!(!plain.capabilities().contains(Capability::Transactions));
    assert!(plain.capabilities().contains(Capability::FullText));
    let shown = format!("{backend:?}");
    assert!(
        shown.contains("MongoStorage") && shown.contains("transactions"),
        "{shown}"
    );
    let scoped = backend.for_scope(&Scope::new("shown").unwrap()).unwrap();
    assert!(format!("{:?}", scoped.documents()).contains("shown"));
    assert!(format!("{:?}", scoped.streams()).contains("MongoStreams"));
    assert!(format!("{:?}", scoped.blobs()).contains("MongoBlobs"));
    assert_eq!(
        backend.database("Bad").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
