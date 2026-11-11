use super::*;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ErrorKind, ExpectedDocument, Filter, Precondition, ReadCheck,
    Scope, Version, WatchCursor, WatchOptions, WriteOp, WriteOptions, WriteResult,
};

async fn memory_client(scope: &str) -> tinystoragedrivers_api::Storage {
    let server = std::sync::Arc::new(crate::StorageServer::new(memory_storage(
        &Scope::new(scope).unwrap(),
    )));
    crate::BusStorage::connect(std::sync::Arc::new(crate::Loopback::new(
        server,
        crate::Admission::Private,
    )))
    .await
    .unwrap()
}

#[test]
fn batch_converts_valid_operations_and_enforces_both_operation_caps() {
    let put = WriteOp::Put {
        collection: "notes".into(),
        id: "n1".into(),
        doc: serde_json::json!({"v": 1}),
        precondition: Precondition::Absent,
    };
    let converted = batch(&[], vec![put.clone()]).unwrap();
    assert_eq!(converted.ops.len(), 1);
    assert_eq!(converted.checks.len(), 0);

    let invalid_ops = vec![put.clone(); MAX_BATCH_OPS + 1];
    assert_eq!(
        batch(&[], invalid_ops).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    let check = ReadCheck {
        collection: "notes".into(),
        id: "n1".into(),
        expected: ExpectedDocument::Absent,
    };
    assert_eq!(
        batch(&vec![check; MAX_BATCH_OPS + 1], Vec::new())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn migration_creation_time_is_rejected_before_sending_a_put() {
    let storage = memory_storage(&Scope::new("migration-write").unwrap());
    let server = std::sync::Arc::new(crate::StorageServer::new(storage));
    let client = crate::BusStorage::connect(std::sync::Arc::new(crate::Loopback::new(
        server,
        crate::Admission::Private,
    )))
    .await
    .unwrap();
    let error = client
        .documents()
        .put_with(
            "notes",
            "id",
            serde_json::json!({}),
            Precondition::None,
            WriteOptions {
                created_ms: Some(1),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        client
            .documents()
            .get("notes", "id")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn checked_batch_round_trips_a_read_dependency_and_write_result() {
    let storage = memory_client("checked-batch").await;
    let documents = storage.documents();
    let dependency_version = documents
        .put(
            "dependencies",
            "release",
            serde_json::json!({"state": "ready"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let check = ReadCheck {
        collection: "dependencies".into(),
        id: "release".into(),
        expected: ExpectedDocument::Version(dependency_version),
    };
    let put = WriteOp::Put {
        collection: "deployments".into(),
        id: "current".into(),
        doc: serde_json::json!({"release": "ready"}),
        precondition: Precondition::Absent,
    };

    let results = documents
        .atomic_batch_checked(vec![check.clone()], vec![put.clone()])
        .await
        .unwrap();
    assert_eq!(
        results,
        vec![WriteResult::Put {
            version: Version::FIRST
        }]
    );
    assert_eq!(
        documents
            .get("deployments", "current")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"release": "ready"})
    );

    let stale = documents
        .atomic_batch_checked(vec![check], vec![put])
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), ErrorKind::Conflict);
    assert!(
        documents
            .get("deployments", "current")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn non_default_metadata_crosses_the_wire_and_watch_errors_keep_capability() {
    let storage = memory_client("document-options").await;
    let documents = storage.documents();
    documents
        .ensure_collection(&CollectionSpec::new("events"))
        .await
        .unwrap();
    documents
        .validate_collection_spec(&CollectionSpec::new("events"))
        .await
        .unwrap();
    let version = documents
        .put_with(
            "events",
            "one",
            serde_json::json!({"kind": "created"}),
            Precondition::Absent,
            WriteOptions {
                schema: 7,
                expires_ms: Some(i64::MAX),
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(version, Version::FIRST);

    let cursor = WatchCursor::new(b"resume-boundary".to_vec()).unwrap();
    let error = documents
        .watch(
            "events",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor),
                ..WatchOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::Watch));
}

#[tokio::test]
async fn unsupported_document_features_keep_the_requested_capability() {
    let server = std::sync::Arc::new(crate::StorageServer::new(
        tinystoragedrivers_api::Storage::unsupported(),
    ));
    let storage = crate::BusStorage::connect(std::sync::Arc::new(crate::Loopback::new(
        server,
        crate::Admission::Private,
    )))
    .await
    .unwrap();
    let documents = storage.documents();

    assert_eq!(
        documents
            .validate_collection_spec(&CollectionSpec::new("events"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::CollectionValidation)
    );
    assert_eq!(
        documents
            .atomic_batch_checked(Vec::new(), Vec::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ValidatedBatch)
    );
    assert_eq!(
        documents
            .search("events", "ready", 5)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::FullText)
    );
}

#[tokio::test]
async fn a_batched_put_with_carries_its_options_across_the_wire() {
    let storage = memory_client("batched-options").await;
    let documents = storage.documents();
    documents
        .atomic_batch(vec![WriteOp::PutWith {
            collection: "events".into(),
            id: "one".into(),
            doc: serde_json::json!({"kind": "created"}),
            precondition: Precondition::Absent,
            options: WriteOptions::new().schema(7).expires_ms(i64::MAX),
        }])
        .await
        .unwrap();
    let found = documents.get("events", "one").await.unwrap().unwrap();
    assert_eq!(found.meta.schema, 7);
    assert_eq!(found.meta.expires_ms, Some(i64::MAX));
}

#[tokio::test]
async fn a_batched_creation_time_is_rejected_before_sending() {
    let storage = memory_client("batched-created").await;
    let documents = storage.documents();
    let error = documents
        .atomic_batch(vec![WriteOp::PutWith {
            collection: "notes".into(),
            id: "id".into(),
            doc: serde_json::json!({}),
            precondition: Precondition::None,
            options: WriteOptions::new().created_ms(1),
        }])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(documents.get("notes", "id").await.unwrap().is_none());
}
