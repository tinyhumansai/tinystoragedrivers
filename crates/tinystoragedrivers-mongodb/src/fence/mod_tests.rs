//! Fenced transactions: the refusal for unfenceable writes, and (against a
//! live replica set) the guard hold and concurrent fenced writers.

use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{Capability, ErrorKind, Fence, Precondition, Scope, StorageBackend};

use super::*;
use crate::MongoStorage;

#[test]
fn unfenceable_writes_name_the_capability() {
    let error = unfenceable("a blob put");
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::Fencing));
    assert!(error.message().contains("a blob put"), "{error}");
}

fn labelled(labels: &[&str]) -> mongodb::error::Error {
    use mongodb::error::{ErrorKind as MongoKind, WriteConcernError, WriteFailure};
    let concern: WriteConcernError = mongodb::bson::from_document(doc! {
        "code": 64, "codeName": "WriteConcernFailed", "errmsg": "x", "errorLabels": labels,
    })
    .unwrap();
    MongoKind::Write(WriteFailure::WriteConcernError(concern)).into()
}

#[test]
fn a_commit_settles_as_done_retry_or_failure() {
    let mut last = kept_aborting();
    assert_eq!(settle(&mut last, 7, Ok(())).unwrap(), Some(7));
    assert_eq!(
        settle(&mut last, 7, Err(labelled(&["TransientTransactionError"]))).unwrap(),
        None
    );
    assert!(last.message().contains("commit a fenced write"), "{last}");
    let unknown = settle(
        &mut last,
        7,
        Err(labelled(&["UnknownTransactionCommitResult"])),
    )
    .unwrap_err();
    assert_eq!(unknown.kind(), ErrorKind::Backend);
    assert!(!unknown.is_retryable());
    let failed = settle(&mut last, 7, Err(labelled(&[]))).unwrap_err();
    assert_eq!(failed.kind(), ErrorKind::Backend);
}

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
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "fence_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn live_a_fenced_write_holds_the_guard_document() {
    let Some(backend) = live("live_a_fenced_write_holds_the_guard_document").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    let guards = backend.for_scope(&cluster).unwrap();
    guards
        .documents()
        .put(&leases, "l", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster.clone(), &leases, "l", "epoch", 1);
    let fenced = backend.for_scope_fenced(&Scope::local(), &fence).unwrap();
    let coll = unique("data");
    fenced
        .documents()
        .put(&coll, "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let raw = backend
        .shared
        .raw(&leases)
        .find_one(doc! {"_id": document_id(&cluster, "l")})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.get_i64(FENCE).unwrap(), 1, "{raw}");
    // The hold is invisible to readers and leaves the version alone.
    let guard = guards.documents().get(&leases, "l").await.unwrap().unwrap();
    assert_eq!(guard.doc, json!({"epoch": 1}));
    assert_eq!(guard.version.0, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_concurrent_fenced_writers_all_land() {
    let Some(backend) = live("live_concurrent_fenced_writers_all_land").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    backend
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 3}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster, &leases, "l", "epoch", 3);
    let fenced = Arc::new(backend.for_scope_fenced(&Scope::local(), &fence).unwrap());
    let coll = unique("data");
    let writers: Vec<_> = (0..6)
        .map(|n| {
            let (fenced, coll) = (Arc::clone(&fenced), coll.clone());
            tokio::spawn(async move {
                fenced
                    .documents()
                    .put(&coll, &format!("d{n}"), json!({"n": n}), Precondition::None)
                    .await
            })
        })
        .collect();
    let mut landed = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(_) => landed += 1,
            // Contention past every attempt is reported as retryable, never
            // as a silent unfenced write.
            Err(error) => assert!(error.is_retryable(), "{error}"),
        }
    }
    assert!(landed >= 1);
    let count = backend
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .count(&coll, &tinystoragedrivers_core::Filter::All)
        .await
        .unwrap();
    assert_eq!(count, landed);
}

#[tokio::test]
async fn live_the_fenced_transaction_renders_its_fence() {
    let Some(backend) = live("live_the_fenced_transaction_renders_its_fence").await else {
        return;
    };
    let fence = Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    let txn = FencedTxn::start(&backend.shared, &fence).await.unwrap();
    let rendered = format!("{txn:?}");
    assert!(
        rendered.contains("leases") && rendered.contains("attempts"),
        "{rendered}"
    );
}

#[tokio::test]
async fn live_transient_failures_retry_until_the_attempts_run_out() {
    let Some(backend) = live("live_transient_failures_retry_until_the_attempts_run_out").await
    else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    backend
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster, &leases, "l", "epoch", 1);
    let mut txn = FencedTxn::start(&backend.shared, &fence).await.unwrap();
    for _ in 0..FENCE_ATTEMPTS {
        txn.begin().await.unwrap();
        let outcome: tinystoragedrivers_core::Result<()> =
            Err(StorageError::unavailable("simulated transient failure"));
        assert!(txn.finish(outcome).await.unwrap().is_none());
    }
    let exhausted = txn.begin().await.unwrap_err();
    assert!(exhausted.is_retryable());
    assert!(exhausted.message().contains("simulated"), "{exhausted}");
    // A non-transient write error ends the write at once.
    let mut txn = FencedTxn::start(&backend.shared, &fence).await.unwrap();
    txn.begin().await.unwrap();
    let outcome: tinystoragedrivers_core::Result<()> = Err(StorageError::conflict("cas lost"));
    assert_eq!(
        txn.finish(outcome).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
}
