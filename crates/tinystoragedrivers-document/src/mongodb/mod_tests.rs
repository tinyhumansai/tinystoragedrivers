use super::*;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_api::StreamStore;
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ExpectedDocument, Filter, Precondition, Query, ReadCheck, Scope,
    WatchNotice, WatchOptions, WriteCondition, WriteOp, WriteOptions, WriteResult,
};

#[derive(Debug, Default)]
pub(crate) struct TestBlobs {
    values: Mutex<BTreeMap<String, Vec<u8>>>,
    put_started: Option<Arc<tokio::sync::Notify>>,
    allow_put: Option<Arc<tokio::sync::Notify>>,
    put_order: Option<Arc<AtomicUsize>>,
    first_put_started: Option<Arc<tokio::sync::Notify>>,
    allow_first_put: Option<Arc<tokio::sync::Notify>>,
    fail_next_put: Option<Arc<AtomicBool>>,
    delete_started: Option<Arc<tokio::sync::Notify>>,
    allow_delete: Option<Arc<tokio::sync::Notify>>,
    delete_order: Option<Arc<AtomicUsize>>,
}

#[tinystoragedrivers_api::async_trait]
impl BlobStore for TestBlobs {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        tinystoragedrivers_types::Capabilities::none()
    }
    fn max_key_len(&self) -> usize {
        tinystoragedrivers_types::MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::BlobMeta> {
        if let Some(order) = &self.put_order {
            if order.fetch_add(1, Ordering::SeqCst) == 0 {
                if let Some(started) = &self.first_put_started {
                    started.notify_one();
                }
                if let Some(allow) = &self.allow_first_put {
                    allow.notified().await;
                }
            }
        } else {
            if let Some(started) = &self.put_started {
                started.notify_one();
            }
            if let Some(allow) = &self.allow_put {
                allow.notified().await;
            }
        }
        if self
            .fail_next_put
            .as_ref()
            .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Err(tinystoragedrivers_types::StorageError::backend(
                "injected blob upload failure",
            ));
        }
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.clone());
        Ok(tinystoragedrivers_types::BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        })
    }
    async fn get(
        &self,
        key: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::Blob>> {
        Ok(self.values.lock().unwrap().get(key).cloned().map(|bytes| {
            tinystoragedrivers_types::Blob {
                meta: tinystoragedrivers_types::BlobMeta {
                    key: key.to_owned(),
                    len: bytes.len() as u64,
                    content_type: Some("application/zstd".to_owned()),
                },
                bytes,
            }
        }))
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> tinystoragedrivers_types::Result<Option<Vec<u8>>> {
        Ok(self.values.lock().unwrap().get(key).map(|bytes| {
            let start = usize::try_from(range.start.min(bytes.len() as u64)).unwrap();
            let end = usize::try_from(range.end.min(bytes.len() as u64)).unwrap();
            bytes[start..end].to_vec()
        }))
    }
    async fn head(
        &self,
        key: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .get(key)
            .map(|bytes| tinystoragedrivers_types::BlobMeta {
                key: key.to_owned(),
                len: bytes.len() as u64,
                content_type: Some("application/zstd".to_owned()),
            }))
    }
    async fn delete(&self, key: &str) -> tinystoragedrivers_types::Result<bool> {
        if self
            .delete_order
            .as_ref()
            .is_none_or(|order| order.fetch_add(1, Ordering::SeqCst) == 0)
        {
            if let Some(started) = &self.delete_started {
                started.notify_one();
            }
            if let Some(allow) = &self.allow_delete {
                allow.notified().await;
            }
        }
        Ok(self.values.lock().unwrap().remove(key).is_some())
    }
    async fn list(
        &self,
        prefix: &str,
    ) -> tinystoragedrivers_types::Result<Vec<tinystoragedrivers_types::BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, bytes)| tinystoragedrivers_types::BlobMeta {
                key: key.clone(),
                len: bytes.len() as u64,
                content_type: Some("application/zstd".to_owned()),
            })
            .collect())
    }
}

#[tokio::test]
async fn deleting_an_old_stream_generation_preserves_a_recreated_sealed_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_delete_recreate_race").await;
    let delete_started = Arc::new(tokio::sync::Notify::new());
    let allow_delete = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_started: None,
        allow_put: None,
        delete_started: Some(Arc::clone(&delete_started)),
        allow_delete: Some(Arc::clone(&allow_delete)),
        delete_order: Some(Arc::new(AtomicUsize::new(0))),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("old"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();

    let deleting = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.delete_stream("events").await })
    };
    delete_started.notified().await;
    streams
        .append("events", serde_json::json!("new generation"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();
    allow_delete.notify_one();
    assert!(deleting.await.unwrap().unwrap());

    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("new generation"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn deleting_generation_one_does_not_delete_generation_ten_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_delete_prefix_collision").await;
    let blobs = Arc::new(TestBlobs::default());
    let blob_store: Arc<dyn BlobStore> = blobs.clone();
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blob_store);
    streams
        .append("events", serde_json::json!("first incarnation"))
        .await
        .unwrap();
    assert!(streams.delete_stream("events").await.unwrap());
    streams
        .append("events", serde_json::json!("generation one"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();

    let generation_ten = format!("{}/g10", crate::sealed_blob_key("events").unwrap());
    blobs
        .values
        .lock()
        .unwrap()
        .insert(generation_ten.clone(), vec![1, 2, 3]);
    assert!(streams.delete_stream("events").await.unwrap());
    assert!(blobs.head(&generation_ten).await.unwrap().is_some());
    connection.database().drop().await.unwrap();
}

async fn test_connection(uri: &str, stem: &str) -> MongoConnection {
    let database = format!(
        "{stem}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |time| time.as_millis())
    );
    MongoConnection::connect(uri, &database)
        .await
        .expect("test MongoDB is available")
}

#[tokio::test]
async fn transactional_mongo_documents_advertise_cross_process_cas() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd65_capabilities").await;
    assert!(connection.transactions_available());
    let capabilities =
        MongoDocuments::new(connection.clone(), Scope::new("caps").unwrap()).capabilities();
    assert!(capabilities.contains(Capability::SharedAcrossProcesses));
    assert!(capabilities.contains(Capability::CrossProcessCas));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn independent_mongo_connections_arbitrate_absent_and_version_writes() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let first_connection = test_connection(&uri, "tsd65_cas").await;
    assert!(first_connection.transactions_available());
    let database = first_connection.database().name().to_owned();
    let second_connection = MongoConnection::connect(&uri, &database).await.unwrap();
    assert!(second_connection.transactions_available());
    let scope = Scope::new("cas-race").unwrap();
    let first = MongoDocuments::new(first_connection.clone(), scope.clone());
    let second = MongoDocuments::new(second_connection, scope);
    first
        .ensure_collection(&CollectionSpec::new("items"))
        .await
        .unwrap();

    let absent_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let absent_write = |docs: MongoDocuments,
                        barrier: Arc<tokio::sync::Barrier>,
                        value: &'static str| async move {
        barrier.wait().await;
        docs.put(
            "items",
            "absent",
            serde_json::json!({"value": value}),
            Precondition::Absent,
        )
        .await
    };
    let (left, right) = tokio::join!(
        absent_write(first.clone(), absent_barrier.clone(), "left"),
        absent_write(second.clone(), absent_barrier, "right")
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    for error in [left.err(), right.err()].into_iter().flatten() {
        assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    }

    let current = first.get("items", "absent").await.unwrap().unwrap();
    let version_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let version_write = |docs: MongoDocuments,
                         barrier: Arc<tokio::sync::Barrier>,
                         value: &'static str| async move {
        barrier.wait().await;
        docs.put(
            "items",
            "absent",
            serde_json::json!({"value": value}),
            Precondition::Version(current.version),
        )
        .await
    };
    let (left, right) = tokio::join!(
        version_write(first.clone(), version_barrier.clone(), "next-left"),
        version_write(second, version_barrier, "next-right")
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    for error in [left.err(), right.err()].into_iter().flatten() {
        assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    }
    first_connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_mongo_handle_checks_body_deadline_and_allows_same_epoch_renewal() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let now = Arc::new(std::sync::atomic::AtomicU64::new(100));
    let clock = Arc::clone(&now);
    let connection = test_connection(&uri, "tsd59_body_deadline")
        .await
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let guard_scope = Scope::new("deadline-guard").unwrap();
    let target_scope = Scope::new("deadline-target").unwrap();
    let guard = MongoDocuments::new(connection.clone(), guard_scope.clone());
    guard
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    let version = guard
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1, "expires_at": 150}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition = WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1))
        .unwrap()
        .with_valid_until_field("expires_at")
        .unwrap();
    let target = MongoDocuments::conditioned(connection.clone(), target_scope, condition).unwrap();
    target
        .put(
            "items",
            "before-expiry",
            serde_json::json!({}),
            Precondition::None,
        )
        .await
        .unwrap();

    now.store(150, Ordering::SeqCst);
    assert_eq!(
        target
            .put(
                "items",
                "expired",
                serde_json::json!({}),
                Precondition::None
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(guard.get("leases", "writer").await.unwrap().is_some());

    guard
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1, "expires_at": 200}),
            Precondition::Version(version),
        )
        .await
        .unwrap();
    target
        .put(
            "items",
            "renewed",
            serde_json::json!({}),
            Precondition::None,
        )
        .await
        .unwrap();
    now.store(200, Ordering::SeqCst);
    assert_eq!(
        target
            .put(
                "items",
                "expired-after-renewal",
                serde_json::json!({}),
                Precondition::None
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_writes_reject_guards_expired_by_collection_ttl() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let now = Arc::new(std::sync::atomic::AtomicU64::new(100));
    let clock = Arc::clone(&now);
    let connection = test_connection(&uri, "tsd59_guard_collection_ttl")
        .await
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let guard_scope = Scope::new("ttl-guard").unwrap();
    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope.clone());
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases").ttl("expires_at"))
        .await
        .unwrap();
    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1, "expires_at": 100}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let target = MongoDocuments::conditioned(
        connection.clone(),
        Scope::new("ttl-target").unwrap(),
        condition,
    )
    .unwrap();

    assert_eq!(
        target
            .put("items", "denied", serde_json::json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(target.get("items", "denied").await.unwrap().is_none());
    assert!(guard_docs.get("leases", "writer").await.unwrap().is_none());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_stream_sealing_and_deletion_keep_the_guard_at_metadata_mutations() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd59_conditioned_seal").await;
    let guard_scope = Scope::new("condition-guard").unwrap();
    let target_scope = Scope::new("condition-target").unwrap();
    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope.clone());
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    let version = guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::conditioned(connection.clone(), target_scope, condition)
        .unwrap()
        .with_blob_store(blobs);

    streams
        .append("removed-while-current", serde_json::json!("event"))
        .await
        .unwrap();
    streams.seal("removed-while-current").await.unwrap();
    let repeated_seal = streams.seal("removed-while-current").await.unwrap();
    assert_eq!(repeated_seal.len, 1);
    let live_sealed_ms = streams
        .info("removed-while-current")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .unwrap();
    assert!(
        streams
            .delete_sealed_stream("removed-while-current", live_sealed_ms)
            .await
            .unwrap()
    );

    streams
        .append("stale-sealed", serde_json::json!("event"))
        .await
        .unwrap();
    streams.seal("stale-sealed").await.unwrap();
    let stale_sealed_ms = streams
        .info("stale-sealed")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .unwrap();
    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(version),
        )
        .await
        .unwrap();

    assert_eq!(
        streams
            .delete_sealed_stream("stale-sealed", stale_sealed_ms)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(
        streams.seal("stale-sealed").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(streams.info("stale-sealed").await.unwrap().is_some());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "covers conditioned document writes, queries, collection deletion, and takeover"
)]
async fn conditioned_documents_guard_direct_writes_and_deletes() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd59_conditioned_documents").await;
    let guard_scope = Scope::new("condition-guard").unwrap();
    let target_scope = Scope::new("condition-target").unwrap();
    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope.clone());
    let target_docs = MongoDocuments::new(connection.clone(), target_scope.clone());
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    target_docs
        .ensure_collection(&CollectionSpec::new("items"))
        .await
        .unwrap();
    let guard_version = guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let held = MongoDocuments::conditioned(connection.clone(), target_scope, condition).unwrap();
    assert_eq!(
        held.ensure_collection(&CollectionSpec::new("forbidden"))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(Capability::ConditionalWrites)
    );
    let version = held
        .put(
            "items",
            "one",
            serde_json::json!({"value": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert_eq!(
        held.get("items", "one").await.unwrap().unwrap().doc,
        serde_json::json!({"value": 1})
    );
    held.put_with(
        "items",
        "two",
        serde_json::json!({"value": 2, "group": "conditioned"}),
        Precondition::Absent,
        WriteOptions {
            schema: 2,
            expires_ms: None,
            created_ms: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        held.query("items", &Query::filter(Filter::eq("group", "conditioned")),)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    assert_eq!(
        held.count("items", &Filter::eq("group", "conditioned"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        held.delete_where("items", &Filter::eq("group", "conditioned"))
            .await
            .unwrap(),
        1
    );
    assert!(held.get("items", "two").await.unwrap().is_none());
    assert!(
        held.delete("items", "one", Precondition::Version(version))
            .await
            .unwrap()
    );
    assert!(held.get("items", "one").await.unwrap().is_none());

    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(guard_version),
        )
        .await
        .unwrap();
    assert_eq!(
        held.put(
            "items",
            "stale",
            serde_json::json!({"value": 2}),
            Precondition::Absent,
        )
        .await
        .unwrap_err()
        .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(held.get("items", "stale").await.unwrap().is_none());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one Mongo transaction test pins conjunction rollback and each independent guard"
)]
async fn composed_mongo_conditions_all_check_and_rollback_in_one_mutation_transaction() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let now = Arc::new(std::sync::atomic::AtomicU64::new(100));
    let clock = Arc::clone(&now);
    let connection = test_connection(&uri, "tsd59_composed_conditions")
        .await
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let profile_scope = Scope::new("profile-guard").unwrap();
    let import_scope = Scope::new("import-guard").unwrap();
    let target_scope = Scope::new("composed-target").unwrap();
    let profile_docs = MongoDocuments::new(connection.clone(), profile_scope.clone());
    let import_docs = MongoDocuments::new(connection.clone(), import_scope.clone());
    for (docs, collection) in [(&profile_docs, "leases"), (&import_docs, "claims")] {
        docs.ensure_collection(&CollectionSpec::new(collection))
            .await
            .unwrap();
    }
    let profile_version = profile_docs
        .put(
            "leases",
            "active",
            serde_json::json!({"epoch": 1, "expires_ms": 1_000}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let claim_version = import_docs
        .put(
            "claims",
            "source",
            serde_json::json!({"state": "importing", "until_ms": 1_000}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let profile = WriteCondition::new(
        profile_scope.clone(),
        "leases",
        "active",
        Filter::eq("epoch", 1),
    )
    .unwrap()
    .with_valid_until_field("expires_ms")
    .unwrap();
    let import = WriteCondition::new(
        import_scope.clone(),
        "claims",
        "source",
        Filter::eq("state", "importing"),
    )
    .unwrap()
    .with_valid_until_field("until_ms")
    .unwrap();
    let combined = profile.and(import).unwrap();
    let target = MongoDocuments::conditioned(connection.clone(), target_scope, combined).unwrap();
    let raw_database = connection.database();
    let profile_raw = raw_database.collection::<mongodb::bson::Document>("leases");
    let import_raw = raw_database.collection::<mongodb::bson::Document>("claims");
    let profile_id = mongodb::bson::doc! {"s": profile_scope.as_str(), "k": "active"};
    let import_id = mongodb::bson::doc! {"s": import_scope.as_str(), "k": "source"};

    let prepared_claim_version = import_docs
        .put(
            "claims",
            "source",
            serde_json::json!({"state": "prepared", "until_ms": 1_000}),
            Precondition::Version(claim_version),
        )
        .await
        .unwrap();
    assert_eq!(
        target
            .put(
                "items",
                "denied-second",
                serde_json::json!({"value": 1}),
                Precondition::Absent
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(
        target
            .get("items", "denied-second")
            .await
            .unwrap()
            .is_none()
    );
    let profile_after_abort = profile_raw
        .find_one(mongodb::bson::doc! {"_id": profile_id.clone()})
        .await
        .unwrap()
        .unwrap();
    let claim_after_abort = import_raw
        .find_one(mongodb::bson::doc! {"_id": import_id.clone()})
        .await
        .unwrap()
        .unwrap();
    assert!(profile_after_abort.get_i64("_tsd_condition_touch").is_err());
    assert!(claim_after_abort.get_i64("_tsd_condition_touch").is_err());

    let claim_version = import_docs
        .put(
            "claims",
            "source",
            serde_json::json!({"state": "importing", "until_ms": 1_000}),
            Precondition::Version(prepared_claim_version),
        )
        .await
        .unwrap();
    target
        .put(
            "items",
            "accepted",
            serde_json::json!({"value": 2}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    for (collection, id) in [
        (&profile_raw, profile_id.clone()),
        (&import_raw, import_id.clone()),
    ] {
        assert_eq!(
            collection
                .find_one(mongodb::bson::doc! {"_id": id})
                .await
                .unwrap()
                .unwrap()
                .get_i64("_tsd_condition_touch"),
            Ok(1)
        );
    }

    import_docs
        .put(
            "claims",
            "source",
            serde_json::json!({"state": "complete", "until_ms": 1_000}),
            Precondition::Version(claim_version),
        )
        .await
        .unwrap();
    assert_eq!(
        target
            .put(
                "items",
                "denied-claim",
                serde_json::json!({"value": 3}),
                Precondition::Absent
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(target.get("items", "denied-claim").await.unwrap().is_none());
    let profile_after_claim_failure = profile_raw
        .find_one(mongodb::bson::doc! {"_id": profile_id.clone()})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        profile_after_claim_failure.get_i64("_tsd_condition_touch"),
        Ok(1)
    );

    profile_docs
        .put(
            "leases",
            "active",
            serde_json::json!({"epoch": 2, "expires_ms": 1_000}),
            Precondition::Version(profile_version),
        )
        .await
        .unwrap();
    assert_eq!(
        target
            .put(
                "items",
                "denied-profile",
                serde_json::json!({"value": 4}),
                Precondition::Absent
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(
        target
            .get("items", "denied-profile")
            .await
            .unwrap()
            .is_none()
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_document_writes_reject_missing_and_nonmatching_guards() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd59_missing_condition_guard").await;
    let guard_scope = Scope::new("condition-guard").unwrap();
    let target_scope = Scope::new("condition-target").unwrap();
    let condition = WriteCondition::new(
        guard_scope.clone(),
        "leases",
        "writer",
        Filter::eq("epoch", 1),
    )
    .unwrap();
    let held = MongoDocuments::conditioned(connection.clone(), target_scope, condition).unwrap();

    assert_eq!(
        held.put(
            "items",
            "missing",
            serde_json::json!({"value": 1}),
            Precondition::Absent
        )
        .await
        .unwrap_err()
        .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(held.get("items", "missing").await.unwrap().is_none());

    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope);
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 0}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert_eq!(
        held.put(
            "items",
            "wrong-epoch",
            serde_json::json!({"value": 1}),
            Precondition::Absent
        )
        .await
        .unwrap_err()
        .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(held.get("items", "wrong-epoch").await.unwrap().is_none());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_stream_delete_is_transactional_for_empty_and_live_generations() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd59_conditioned_stream_delete").await;
    let guard_scope = Scope::new("condition-guard").unwrap();
    let target_scope = Scope::new("condition-target").unwrap();
    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope.clone());
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    let guard_version = guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let blobs = Arc::new(tinystoragedrivers_blob::MongoBlobStore::new(
        connection.clone(),
        target_scope.clone(),
    ));
    let streams = MongoStreams::conditioned(connection.clone(), target_scope, condition)
        .unwrap()
        .with_blob_store(blobs);
    assert!(!streams.delete_stream("empty").await.unwrap());
    assert!(!streams.delete_stream("empty").await.unwrap());
    streams
        .append("events", serde_json::json!({"value": "retained"}))
        .await
        .unwrap();
    assert!(streams.delete_stream("events").await.unwrap());
    assert_eq!(streams.len("events").await.unwrap(), 0);
    streams
        .append("events", serde_json::json!({"value": "retained"}))
        .await
        .unwrap();
    streams
        .append_batch(
            "truncate",
            vec![serde_json::json!("remove"), serde_json::json!("keep")],
        )
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("truncate", 1).await.unwrap(), 1);
    assert_eq!(
        streams.read_window("truncate", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("keep"),
        }]
    );

    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 1));
    let sealed_ms = streams
        .info("events")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .unwrap();
    assert!(
        streams
            .delete_sealed_stream("events", sealed_ms)
            .await
            .unwrap()
    );
    assert!(
        !streams
            .delete_sealed_stream("events", sealed_ms)
            .await
            .unwrap()
    );
    streams
        .append("events", serde_json::json!({"value": "after-seal"}))
        .await
        .unwrap();

    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(guard_version),
        )
        .await
        .unwrap();
    assert_eq!(
        streams.delete_stream("events").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(streams.len("events").await.unwrap(), 1);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn conditioned_seal_rechecks_after_blob_upload_and_discards_a_stale_attempt() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd59_seal_takeover").await;
    let guard_scope = Scope::new("condition-guard").unwrap();
    let guard_docs = MongoDocuments::new(connection.clone(), guard_scope.clone());
    guard_docs
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    let version = guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        put_started: Some(Arc::clone(&put_started)),
        allow_put: Some(Arc::clone(&allow_put)),
        ..Default::default()
    });
    let streams = MongoStreams::conditioned(
        connection.clone(),
        Scope::new("condition-target").unwrap(),
        condition,
    )
    .unwrap()
    .with_blob_store(blobs.clone());
    streams
        .append("events", serde_json::json!("durable source"))
        .await
        .unwrap();

    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), put_started.notified())
        .await
        .expect("seal reached the external blob write");
    guard_docs
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(version),
        )
        .await
        .unwrap();
    allow_put.notify_one();

    assert_eq!(
        sealing.await.unwrap().unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(streams.len("events").await.unwrap(), 1);
    assert!(
        streams
            .info("events")
            .await
            .unwrap()
            .unwrap()
            .sealed_ms
            .is_none()
    );
    assert!(blobs.values.lock().unwrap().is_empty());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongo_stream_debug_reports_shared_handle_state_without_connection_uri() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_debug").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap());
    let debug = format!("{streams:?}");
    assert!(debug.contains(connection.database().name()));
    assert!(debug.contains("scope-a"));
    assert!(debug.contains("has_blob_store: false"));
    assert!(!debug.contains(&uri));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongo_search_refuses_undeclared_fields_and_empty_queries_early() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_search_validation").await;
    let documents = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    assert_eq!(
        documents
            .search("undeclared", "term", 10)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    documents
        .ensure_collection(&CollectionSpec::new("searchable").searchable(["text"]))
        .await
        .unwrap();
    assert_eq!(
        documents.search("searchable", "", 10).await.unwrap(),
        Vec::new()
    );
    assert_eq!(
        documents.search("searchable", "term", 0).await.unwrap(),
        Vec::new()
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongo_stream_seal_uses_the_real_scope_bound_blob_engine() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_real_blob_seal").await;
    let scope = Scope::new("scope-a").unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(tinystoragedrivers_blob::MongoBlobStore::new(
        connection.clone(),
        scope.clone(),
    ));
    let streams = MongoStreams::new(connection.clone(), scope).with_blob_store(Arc::clone(&blobs));
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("first"), serde_json::json!("second")],
        )
        .await
        .unwrap();
    assert!(streams.capabilities().contains(Capability::StreamSeal));
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 2));
    assert_eq!(streams.seal("events").await.unwrap(), sealed);
    assert_eq!(streams.read_window("events", 0, 10).await.unwrap().len(), 2);

    let key = format!("{}/g0", crate::sealed_blob_key("events").unwrap());
    assert_eq!(
        blobs.get(&key).await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(blobs.list("").await.unwrap(), Vec::new());
    assert_eq!(
        blobs.list_page("", None, 10).await.unwrap().items,
        Vec::new()
    );
    let other_scope = tinystoragedrivers_blob::MongoBlobStore::new(
        connection.clone(),
        Scope::new("scope-b").unwrap(),
    );
    assert_eq!(other_scope.get_sealed(&key).await.unwrap(), None);
    assert!(streams.delete_stream("events").await.unwrap());
    assert_eq!(blobs.head_sealed(&key).await.unwrap(), None);
    connection.database().drop().await.unwrap();
}

/// Multiple append batches keep the stream's first retained offset at zero.
#[tokio::test]
async fn stream_info_base_survives_multiple_append_batches() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_base").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap());

    streams
        .append_batch("events", vec![serde_json::json!("first")])
        .await
        .unwrap();
    streams
        .append_batch("events", vec![serde_json::json!("second")])
        .await
        .unwrap();

    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!(info.base, 0);
    assert_eq!(info.len, 2);

    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn truncating_entire_mongo_stream_preserves_its_next_append_offset() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_truncate").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap());
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();

    assert_eq!(streams.truncate_before("events", 20).await.unwrap(), 2);
    assert_eq!(streams.len("events").await.unwrap(), 2);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        Vec::new()
    );
    assert_eq!(
        streams
            .append("events", serde_json::json!("c"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap()[0].offset,
        2
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongo_seal_recovers_from_marker_and_preserves_blob_backed_offsets() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_seal").await;
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!(sealed.base, 1);
    assert_eq!(sealed.len, 2);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("b"),
        }]
    );
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!(info.base, 1);
    assert!(info.sealed_ms.is_some());
    connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams")
        .insert_one(mongodb::bson::doc! {
            "_scope": "scope-a", "s": "events", "g": 0_i64,
            "o": 0_i64, "n": 1_i64, "e": 1_i64, "t": 0_i64,
            "vs": ["left by interrupted seal"],
        })
        .await
        .unwrap();
    assert_eq!(
        streams
            .append("events", serde_json::json!("c"))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(streams.seal("events").await.unwrap(), sealed);
    assert!(
        streams
            .read_window("events", 0, 10)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.value != serde_json::json!("left by interrupted seal"))
    );
    assert!(
        streams
            .delete_sealed_stream("events", info.sealed_ms.unwrap())
            .await
            .unwrap()
    );
    assert_eq!(streams.len("events").await.unwrap(), 0);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn failed_blob_upload_leaves_stream_unsealed_and_retryable() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_upload_failure").await;
    let blobs = Arc::new(TestBlobs {
        fail_next_put: Some(Arc::new(AtomicBool::new(true))),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("still readable"))
        .await
        .unwrap();

    let failure = streams.seal("events").await.unwrap_err();
    assert!(failure.to_string().contains("injected blob upload failure"));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("still readable"),
        }]
    );
    assert_eq!(
        streams.info("events").await.unwrap().unwrap().sealed_ms,
        None
    );

    assert_eq!(streams.seal("events").await.unwrap().len, 1);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn standalone_truncate_and_seal_linearize_without_losing_the_truncated_view() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_standalone_truncate_seal").await;
    let truncate_started = Arc::new(tokio::sync::Notify::new());
    let resume_truncate = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs)
        .without_transactions_for_test()
        .pause_truncate_for_test(Arc::clone(&truncate_started), Arc::clone(&resume_truncate));
    assert_eq!(streams.append_batch("events", Vec::new()).await.unwrap(), 0);
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("discard"), serde_json::json!("keep")],
        )
        .await
        .unwrap();

    let truncating = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.truncate_before("events", 1).await })
    };
    truncate_started.notified().await;
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (1, 2));
    resume_truncate.notify_one();
    // The truncation published its floor before the seal began, so it took
    // effect: it reports what it removed instead of a conflict after its edits.
    assert_eq!(truncating.await.unwrap().unwrap(), 1);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("keep"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn resealing_a_marker_with_a_missing_blob_reports_corruption() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_missing_sealed_blob").await;
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs.clone());
    streams
        .append("events", serde_json::json!("payload"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();

    let marker = connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams")
        .find_one(mongodb::bson::doc! {"_scope": "scope-a", "s": "events", "seal": true})
        .await
        .unwrap()
        .unwrap();
    let blob_key = marker.get_str("bk").unwrap();
    assert!(blobs.delete_sealed(blob_key).await.unwrap());
    assert_eq!(
        streams.seal("events").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn standalone_truncation_reports_malformed_segment_values() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_malformed_truncate_segment").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .without_transactions_for_test();
    connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams")
        .insert_one(mongodb::bson::doc! {
            "_scope": "scope-a",
            "s": "events",
            "g": 0_i64,
            "o": 0_i64,
            "e": 2_i64,
            "t": 0_i64,
            "vs": "corrupt",
        })
        .await
        .unwrap();

    assert_eq!(
        streams
            .truncate_before("events", 1)
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Serialization
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn sealing_a_legacy_head_without_epoch_fields_preserves_its_contents() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_legacy_seal_epochs").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(Arc::new(TestBlobs::default()));
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("one"), serde_json::json!("two")],
        )
        .await
        .unwrap();
    connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_stream_heads")
        .update_one(
            mongodb::bson::doc! {"_scope": "scope-a", "s": "events"},
            mongodb::bson::doc! {"$unset": {"truncate_epoch": "", "append_epoch": ""}},
        )
        .await
        .unwrap();

    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 2));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 0,
                value: serde_json::json!("one"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 1,
                value: serde_json::json!("two"),
            },
        ]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn transactional_truncation_invalidates_a_seal_snapshot_taken_before_blob_publish() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_truncate_race").await;
    // The transactional seal path; a standalone server takes the floor path,
    // where a truncation racing a seal is clipped rather than refused.
    assert!(connection.transactions_available());
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_started: Some(Arc::clone(&put_started)),
        allow_put: Some(Arc::clone(&allow_put)),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();

    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    allow_put.notify_one();

    let error = sealing.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!((info.base, info.len, info.sealed_ms), (1, 2, None));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("b"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn concurrent_sealers_keep_the_published_generation_blob_readable() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_race").await;
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(Arc::new(TestBlobs {
            values: Mutex::default(),
            put_started: Some(Arc::clone(&put_started)),
            allow_put: Some(Arc::clone(&allow_put)),
            ..Default::default()
        }));
    streams
        .append_batch("events", vec![serde_json::json!("first")])
        .await
        .unwrap();

    let first = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    let second = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    put_started.notified().await;
    allow_put.notify_waiters();
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert!(
        first.is_ok() || second.is_ok(),
        "first={first:?}, second={second:?}"
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("first"),
        }]
    );
    let info = streams.info("events").await.unwrap().unwrap();
    assert!(info.sealed_ms.is_some());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn append_winning_the_end_offset_prevents_a_stale_seal_marker() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_append_race").await;
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(Arc::new(TestBlobs {
            values: Mutex::default(),
            put_started: Some(Arc::clone(&put_started)),
            allow_put: Some(Arc::clone(&allow_put)),
            ..Default::default()
        }));
    streams
        .append("events", serde_json::json!("first"))
        .await
        .unwrap();

    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    assert_eq!(
        streams
            .append("events", serde_json::json!("second"))
            .await
            .unwrap(),
        1
    );
    allow_put.notify_one();

    let error = sealing.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!((info.base, info.len, info.sealed_ms), (0, 2, None));
    assert_eq!(streams.read_window("events", 0, 10).await.unwrap().len(), 2);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn stale_seal_attempt_cannot_replace_the_newer_published_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stale_seal_blob").await;
    let first_put_started = Arc::new(tokio::sync::Notify::new());
    let allow_first_put = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_order: Some(Arc::new(AtomicUsize::new(0))),
        first_put_started: Some(Arc::clone(&first_put_started)),
        allow_first_put: Some(Arc::clone(&allow_first_put)),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("first snapshot"))
        .await
        .unwrap();
    let stale = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    first_put_started.notified().await;

    streams
        .append("events", serde_json::json!("newer snapshot"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();
    allow_first_put.notify_one();
    let error = stale.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 0,
                value: serde_json::json!("first snapshot"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 1,
                value: serde_json::json!("newer snapshot"),
            },
        ]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn collection_declarations_accumulate_and_enumeration_is_scope_bound() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_document_specs").await;
    let scope_a = Scope::new("scope-a").unwrap();
    let scope_b = Scope::new("scope-b").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a);
    let docs_b = MongoDocuments::new(connection.clone(), scope_b);
    let broad = CollectionSpec::new("records").searchable(["name"]);
    let narrow = CollectionSpec::new("records");

    docs_a.ensure_collection(&broad).await.unwrap();
    docs_a.ensure_collection(&narrow).await.unwrap();
    docs_a.validate_collection_spec(&broad).await.unwrap();
    docs_a
        .put(
            "records",
            "one",
            serde_json::json!({"name":"alpha"}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "records",
            "two",
            serde_json::json!({"name":"beta"}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "other_only",
            "three",
            serde_json::json!({"name":"gamma"}),
            Precondition::None,
        )
        .await
        .unwrap();

    assert_eq!(
        docs_a.collections("").await.unwrap(),
        vec!["records".to_owned()]
    );
    assert_eq!(
        docs_b.collections("").await.unwrap(),
        vec!["other_only".to_owned(), "records".to_owned()]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn reads_the_legacy_mongodb_document_shape() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_legacy_document")
        .await
        .with_clock(Arc::new(|| 1_000));
    let scope = Scope::new("legacy-scope").unwrap();
    let collection = connection
        .database()
        .collection::<mongodb::bson::Document>("legacy");
    collection
        .insert_one(mongodb::bson::doc! {
            "_id": {"s": scope.as_str(), "k": "known"},
            "_scope": scope.as_str(),
            "_key": "known",
            "_v": 7_i64,
            "d": {"value": "from-1.0"},
            "_schema": 3_i64,
            "_c": 100_i64,
            "_u": 125_i64,
        "_x": 9_999_i64,
        })
        .await
        .unwrap();
    let docs = MongoDocuments::new(connection.clone(), scope);

    let found = docs.get("legacy", "known").await.unwrap().unwrap();
    assert_eq!(found.version.0, 7);
    assert_eq!(found.doc, serde_json::json!({"value":"from-1.0"}));
    assert_eq!(found.meta.schema, 3);
    assert_eq!(found.meta.created_ms, 100);
    assert_eq!(found.meta.updated_ms, 125);
    assert_eq!(found.meta.expires_ms, Some(9_999));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn reads_legacy_mongodb_stream_segments_in_place() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_legacy_stream").await;
    let scope = Scope::new("legacy-scope").unwrap();
    let segments = connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams");
    segments
        .insert_one(mongodb::bson::doc! {
            "_id": mongodb::bson::oid::ObjectId::new(),
            "_scope": scope.as_str(),
            "s": "legacy/events",
            "g": 0_i64,
            "o": 4_i64,
            "n": 2_i64,
            "e": 6_i64,
            "t": 0_i64,
            "vs": ["from-1.0-a", "from-1.0-b"],
        })
        .await
        .unwrap();

    let streams = MongoStreams::new(connection.clone(), scope);
    assert_eq!(streams.len("legacy/events").await.unwrap(), 6);
    assert_eq!(
        streams.read_window("legacy/events", 0, 10).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 4,
                value: serde_json::json!("from-1.0-a"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 5,
                value: serde_json::json!("from-1.0-b"),
            },
        ]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn ttl_fields_expire_and_declared_text_search_is_scoped() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_ttl_search")
        .await
        .with_clock(Arc::new(|| 100));
    let scope_a = Scope::new("scope-a").unwrap();
    let scope_b = Scope::new("scope-b").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a);
    let docs_b = MongoDocuments::new(connection.clone(), scope_b);
    let spec = CollectionSpec::new("items")
        .ttl("expires_at")
        .searchable(["name"]);
    docs_a.ensure_collection(&spec).await.unwrap();
    docs_a
        .put(
            "items",
            "live",
            serde_json::json!({"name":"blue comet","expires_at":101}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_a
        .put(
            "items",
            "expired",
            serde_json::json!({"name":"blue comet","expires_at":100}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "items",
            "other-scope",
            serde_json::json!({"name":"blue comet","expires_at":101}),
            Precondition::None,
        )
        .await
        .unwrap();

    assert!(docs_a.get("items", "expired").await.unwrap().is_none());
    assert_eq!(docs_a.count("items", &Filter::All).await.unwrap(), 1);
    assert_eq!(
        docs_a
            .search("items", "blue comet", 10)
            .await
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        vec!["live"]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn validated_batches_commit_together_and_roll_back_on_conflict() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_atomic_batch").await;
    assert!(connection.transactions_available());
    let docs = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    let spec = CollectionSpec::new("records");
    docs.ensure_collection(&spec).await.unwrap();
    let version = docs
        .put(
            "records",
            "anchor",
            serde_json::json!({"state":"before"}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let results = docs
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "records".to_owned(),
                id: "anchor".to_owned(),
                expected: ExpectedDocument::Version(version),
            }],
            vec![
                WriteOp::Put {
                    collection: "records".to_owned(),
                    id: "anchor".to_owned(),
                    doc: serde_json::json!({"state":"after"}),
                    precondition: Precondition::Version(version),
                },
                WriteOp::Put {
                    collection: "records".to_owned(),
                    id: "created".to_owned(),
                    doc: serde_json::json!({"state":"created"}),
                    precondition: Precondition::Absent,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(matches!(results[0], WriteResult::Put { .. }));
    assert!(matches!(results[1], WriteResult::Put { .. }));
    let anchor = docs.get("records", "anchor").await.unwrap().unwrap();
    assert_eq!(anchor.doc, serde_json::json!({"state":"after"}));

    let failed = docs
        .atomic_batch(vec![
            WriteOp::Put {
                collection: "records".to_owned(),
                id: "rolled-back".to_owned(),
                doc: serde_json::json!({"state":"temporary"}),
                precondition: Precondition::Absent,
            },
            WriteOp::Put {
                collection: "records".to_owned(),
                id: "anchor".to_owned(),
                doc: serde_json::json!({"state":"conflict"}),
                precondition: Precondition::Absent,
            },
        ])
        .await;
    assert!(failed.is_err());
    assert!(docs.get("records", "rolled-back").await.unwrap().is_none());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn concurrent_insert_absent_does_not_replace_the_winning_document() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd8131_absent_cas").await;
    let docs = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    docs.ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();

    for attempt in 0..16 {
        let key = format!("raced-{attempt}");
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first = docs.clone();
        let first_barrier = barrier.clone();
        let first_key = key.clone();
        let first_write = async move {
            first_barrier.wait().await;
            first
                .put(
                    "records",
                    &first_key,
                    serde_json::json!({"winner": "first"}),
                    Precondition::Absent,
                )
                .await
        };
        let second = docs.clone();
        let second_key = key.clone();
        let second_write = async move {
            barrier.wait().await;
            second
                .put(
                    "records",
                    &second_key,
                    serde_json::json!({"winner": "second"}),
                    Precondition::Absent,
                )
                .await
        };
        let (first, second) = tokio::join!(first_write, second_write);
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        for error in [first.err(), second.err()].into_iter().flatten() {
            assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
        }
        let row = docs.get("records", &key).await.unwrap().unwrap();
        assert!(matches!(
            row.doc["winner"].as_str(),
            Some("first" | "second")
        ));
    }
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn concurrent_unconditional_initial_writes_advance_the_document_version() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd8131_unconditional_cas").await;
    let docs = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    docs.ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();
    for attempt in 0..16 {
        let key = format!("raced-{attempt}");
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first = docs.clone();
        let first_barrier = barrier.clone();
        let first_key = key.clone();
        let first_write = async move {
            first_barrier.wait().await;
            first
                .put(
                    "records",
                    &first_key,
                    serde_json::json!({"writer": "first"}),
                    Precondition::None,
                )
                .await
        };
        let second = docs.clone();
        let second_key = key.clone();
        let second_write = async move {
            barrier.wait().await;
            second
                .put(
                    "records",
                    &second_key,
                    serde_json::json!({"writer": "second"}),
                    Precondition::None,
                )
                .await
        };
        let (first, second) = tokio::join!(first_write, second_write);
        let mut versions = [first.unwrap(), second.unwrap()];
        versions.sort_unstable();
        assert_eq!(
            versions,
            [
                tinystoragedrivers_types::Version::FIRST,
                tinystoragedrivers_types::Version::FIRST.next().unwrap(),
            ]
        );
        let row = docs.get("records", &key).await.unwrap().unwrap();
        assert_eq!(row.version, tinystoragedrivers_types::Version(2));
    }
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn concurrent_validated_batches_report_a_conflict_without_losing_the_winner() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd8131_batch_conflict").await;
    assert!(connection.transactions_available());
    let docs = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    docs.ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();
    let version = docs
        .put(
            "records",
            "shared",
            serde_json::json!({"winner": "before"}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let run = |docs: MongoDocuments, barrier: Arc<tokio::sync::Barrier>, value: &'static str| async move {
        barrier.wait().await;
        docs.atomic_batch_checked(
            vec![ReadCheck {
                collection: "records".to_owned(),
                id: "shared".to_owned(),
                expected: ExpectedDocument::Version(version),
            }],
            vec![WriteOp::Put {
                collection: "records".to_owned(),
                id: "shared".to_owned(),
                doc: serde_json::json!({"winner": value}),
                precondition: Precondition::Version(version),
            }],
        )
        .await
    };
    let first = run(docs.clone(), barrier.clone(), "first");
    let second = run(docs.clone(), barrier, "second");
    let (first, second) = tokio::join!(first, second);
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    for error in [first.err(), second.err()].into_iter().flatten() {
        assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    }
    let row = docs.get("records", "shared").await.unwrap().unwrap();
    assert!(matches!(
        row.doc["winner"].as_str(),
        Some("first" | "second")
    ));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the watch regression pins scope filtering, resume binding, and reconnect behavior together"
)]
async fn native_watch_invalidates_for_scope_mutations_and_rejects_cross_binding_resume() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_watch").await;
    assert!(connection.transactions_available());
    let scope_a = Scope::new("watch-a").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a.clone());
    let docs_b = MongoDocuments::new(connection.clone(), Scope::new("watch-b").unwrap());
    let mut watch = docs_a
        .watch("records", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();

    docs_b
        .put(
            "records",
            "other-scope",
            serde_json::json!({"value":1}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), watch.next())
            .await
            .is_err()
    );

    docs_a
        .put(
            "records",
            "same-scope",
            serde_json::json!({"value":2}),
            Precondition::None,
        )
        .await
        .unwrap();
    let notice = tokio::time::timeout(std::time::Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let WatchNotice::Invalidated {
        cursor: Some(cursor),
    } = notice
    else {
        panic!("expected a resumable scope invalidation");
    };

    let other_connection = MongoConnection::connect(&uri, connection.database().name())
        .await
        .unwrap();
    let other_docs = MongoDocuments::new(other_connection, scope_a.clone());
    let rejected_other_connection = other_docs
        .watch(
            "records",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor.clone()),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        rejected_other_connection.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    let mut resumed = docs_a
        .watch(
            "records",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor.clone()),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    let rejected = docs_a
        .watch(
            "other",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        rejected.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    docs_a
        .put(
            "records",
            "after-resume",
            serde_json::json!({"value":3}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), resumed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        WatchNotice::Invalidated { .. }
    ));
    connection.database().drop().await.unwrap();
}

/// A standalone stream whose seal pauses after its snapshot and blob upload.
async fn paused_standalone_seal(
    stem: &str,
) -> (
    MongoConnection,
    MongoStreams,
    Arc<TestBlobs>,
    Arc<tokio::sync::Notify>,
    Arc<tokio::sync::Notify>,
) {
    let uri = std::env::var("TSD_MONGO_URL").unwrap();
    let connection = test_connection(&uri, stem).await;
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs.clone())
        .without_transactions_for_test()
        .pause_seal_for_test(Arc::clone(&started), Arc::clone(&resume));
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("discard"), serde_json::json!("keep")],
        )
        .await
        .unwrap();
    (connection, streams, blobs, started, resume)
}

#[tokio::test]
async fn a_truncation_finished_during_a_standalone_seal_never_withdraws_it() {
    if std::env::var("TSD_MONGO_URL").is_err() {
        return;
    }
    let (connection, streams, _blobs, started, resume) =
        paused_standalone_seal("tsd49_seal_truncated").await;
    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    started.notified().await;
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    resume.notify_one();
    let sealed = sealing.await.unwrap().unwrap();
    assert_eq!((sealed.base, sealed.len), (1, 2));
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!((info.base, info.len), (1, 2));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("keep"),
        }]
    );
    assert_eq!(
        streams
            .append_batch("events", vec![serde_json::json!("late")])
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn deleting_a_stream_during_a_standalone_seal_withdraws_the_marker_and_its_blob() {
    if std::env::var("TSD_MONGO_URL").is_err() {
        return;
    }
    let (connection, streams, blobs, started, resume) =
        paused_standalone_seal("tsd49_seal_deleted").await;
    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    started.notified().await;
    assert!(streams.delete_stream("events").await.unwrap());
    resume.notify_one();
    assert_eq!(
        sealing.await.unwrap().unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(
        blobs.values.lock().unwrap().is_empty(),
        "the withdrawn seal's blob is deleted"
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        Vec::<tinystoragedrivers_types::StreamEntry>::new()
    );
    streams
        .append_batch("events", vec![serde_json::json!("fresh")])
        .await
        .unwrap();
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("fresh"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn an_append_racing_a_standalone_seal_takes_the_offset_and_the_seal_conflicts() {
    if std::env::var("TSD_MONGO_URL").is_err() {
        return;
    }
    let (connection, streams, blobs, started, resume) =
        paused_standalone_seal("tsd49_seal_appended").await;
    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    started.notified().await;
    assert_eq!(
        streams
            .append_batch("events", vec![serde_json::json!("late")])
            .await
            .unwrap(),
        2
    );
    resume.notify_one();
    assert_eq!(
        sealing.await.unwrap().unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert!(
        blobs.values.lock().unwrap().is_empty(),
        "the refused seal's blob is deleted"
    );
    let window = streams.read_window("events", 0, 10).await.unwrap();
    assert_eq!(window.len(), 3);
    assert_eq!(window[2].value, serde_json::json!("late"));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn a_standalone_truncation_floor_belongs_to_its_generation() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd49_floor_generation").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .without_transactions_for_test();
    streams
        .append_batch("events", vec![serde_json::json!(1), serde_json::json!(2)])
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("events", 2).await.unwrap(), 2);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        Vec::<tinystoragedrivers_types::StreamEntry>::new()
    );
    assert!(streams.delete_stream("events").await.unwrap());
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();
    let offsets: Vec<u64> = streams
        .read_window("events", 0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.offset)
        .collect();
    assert_eq!(
        offsets,
        [0, 1],
        "the deleted generation's floor does not clip the new one"
    );
    connection.database().drop().await.unwrap();
}
