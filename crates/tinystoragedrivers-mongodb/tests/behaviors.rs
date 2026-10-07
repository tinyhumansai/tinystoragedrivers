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

use serde_json::json;
use support::{connect, skip};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, Filter, IndexSpec, Precondition, Query, Scope, Sort,
    StorageBackend,
};

fn name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "live_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
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
    let hint = CollectionSpec::new(&coll).index(IndexSpec::new("hint", ["$x"]));
    docs.ensure_collection(&hint).await.unwrap();
    let unsearchable = CollectionSpec::new(&coll).searchable(["$y"]);
    assert_eq!(
        docs.ensure_collection(&unsearchable)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let empty = name("search_empty");
    docs.ensure_collection(&CollectionSpec::new(&empty).searchable(Vec::<String>::new()))
        .await
        .unwrap();
    docs.put(&empty, "a", json!({"t": "x"}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.search(&empty, "x", 5).await.unwrap(), []);
    let changed = CollectionSpec::new(&coll).index(IndexSpec::new("hint", ["other"]));
    assert_eq!(
        docs.ensure_collection(&changed).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
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

#[tokio::test]
async fn live_expired_duplicates_do_not_block_a_unique_index() {
    let Some(backend) = connect().await else {
        return skip("live_expired_duplicates_do_not_block_a_unique_index");
    };
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let backend = backend.with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let docs = backend
        .for_scope(&Scope::new("late_unique").unwrap())
        .unwrap();
    let docs = docs.documents();
    let coll = name("late_unique");
    docs.ensure_collection(&CollectionSpec::new(&coll).ttl("exp"))
        .await
        .unwrap();
    for id in ["a", "b"] {
        docs.put(
            &coll,
            id,
            json!({"exp": 1_500, "tag": "same"}),
            Precondition::None,
        )
        .await
        .unwrap();
    }
    let unique = CollectionSpec::new(&coll).index(IndexSpec::new("by_tag", ["tag"]).unique());
    assert_eq!(
        docs.ensure_collection(&unique).await.unwrap_err().kind(),
        ErrorKind::AlreadyExists,
        "live duplicates still refuse the index"
    );
    now.store(2_000, Ordering::SeqCst);
    docs.ensure_collection(&unique).await.unwrap();
    let v = docs
        .put(&coll, "a", json!({"tag": "same"}), Precondition::Absent)
        .await
        .unwrap();
    assert!(v.0 > 1, "the expired document's version continues");
    assert_eq!(
        docs.put(&coll, "c", json!({"tag": "same"}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::AlreadyExists
    );
}

#[tokio::test]
async fn live_deleting_a_stream_restarts_it_cleanly() {
    let Some(backend) = connect().await else {
        return skip("live_deleting_a_stream_restarts_it_cleanly");
    };
    let streams = backend.for_scope(&Scope::local()).unwrap();
    let streams = streams.streams();
    let stream = name("restart");
    streams
        .append_batch(&stream, vec![json!(1), json!(2)])
        .await
        .unwrap();
    assert!(streams.delete_stream(&stream).await.unwrap());
    assert!(!streams.delete_stream(&stream).await.unwrap());
    assert_eq!(streams.len(&stream).await.unwrap(), 0);
    assert_eq!(
        streams.streams(&stream).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(streams.append(&stream, json!(3)).await.unwrap(), 0);
    assert_eq!(
        streams.streams(&stream).await.unwrap(),
        std::slice::from_ref(&stream)
    );
    let window = streams.read_window(&stream, 0, 10).await.unwrap();
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].value, json!(3));
}
