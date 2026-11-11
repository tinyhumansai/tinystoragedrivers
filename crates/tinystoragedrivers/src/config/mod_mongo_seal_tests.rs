use super::*;

use tinystoragedrivers_types::{
    Capability, ErrorKind, Filter, Precondition, Scope, WriteCondition,
};

type Live = (Arc<dyn StorageBackend>, String, String);

async fn live_backend(stem: &str) -> Result<Option<Live>> {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        eprintln!("{stem}: TSD_MONGO_URL is not set; skipping");
        return Ok(None);
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let database = format!("{stem}_{}_{nanos}", std::process::id());
    let config = StorageConfig::MongoDb {
        uri: uri.clone(),
        database: database.clone(),
    };
    Ok(Some((open(&config).await?, uri, database)))
}

async fn drop_database(uri: &str, database: &str) -> Result<()> {
    tinystoragedrivers_connect::MongoConnection::connect(uri, database)
        .await?
        .database()
        .drop()
        .await
        .map_err(|error| StorageError::backend(error.to_string()))
}

/// The journal maintenance lifecycle a host runs on the facade: append, seal,
/// read the sealed log, then delete the sealed stream by its seal time.
#[tokio::test]
async fn facade_mongo_streams_seal_read_and_delete_a_sealed_stream() {
    let Some((backend, uri, database)) = live_backend("tsd_facade_seal").await.unwrap() else {
        return;
    };
    let scope = Scope::new("facade-seal").unwrap();
    let streams = backend.for_scope(&scope).unwrap().streams().clone();
    assert!(streams.capabilities().contains(Capability::StreamSeal));
    streams
        .append_batch(
            "journal",
            vec![
                serde_json::json!("a"),
                serde_json::json!("b"),
                serde_json::json!("c"),
            ],
        )
        .await
        .unwrap();

    let sealed = streams.seal("journal").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 3));
    let window = streams.read_window("journal", 1, 10).await.unwrap();
    assert_eq!(
        window
            .iter()
            .map(|entry| entry.value.clone())
            .collect::<Vec<_>>(),
        [serde_json::json!("b"), serde_json::json!("c")]
    );
    assert_eq!(
        streams
            .append("journal", serde_json::json!("late"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    let sealed_ms = streams
        .info("journal")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .expect("a sealed stream records its seal time");
    assert!(
        streams
            .delete_sealed_stream("journal", sealed_ms)
            .await
            .unwrap()
    );
    assert_eq!(
        streams.read_window("journal", 0, 10).await.unwrap(),
        Vec::<tinystoragedrivers_types::StreamEntry>::new()
    );

    drop_database(&uri, &database).await.unwrap();
}

/// A conditioned bundle seals too, while its blob port stays unsupported:
/// the sealed log is written outside the condition and removed if the
/// guarded marker transaction fails.
#[tokio::test]
async fn facade_mongo_conditioned_streams_seal_while_blobs_stay_unsupported() {
    let Some((backend, uri, database)) = live_backend("tsd_facade_cseal").await.unwrap() else {
        return;
    };
    let guard_scope = Scope::new("facade-guard").unwrap();
    let target_scope = Scope::new("facade-target").unwrap();
    let ordinary = backend.for_scope(&guard_scope).unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();
    ordinary
        .documents()
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
    let held = match backend.for_scope_conditioned(&target_scope, &condition) {
        Ok(held) => held,
        Err(error) if error.kind() == ErrorKind::Unsupported(Capability::ConditionalWrites) => {
            // A standalone mongod has no transactions, so no conditioned bundle.
            eprintln!("conditioned storage needs transactions; standalone server, skipping");
            drop_database(&uri, &database).await.unwrap();
            return;
        }
        Err(error) => panic!("conditioned scope: {error}"),
    };
    assert!(!held.blobs().capabilities().contains(Capability::Blobs));
    held.streams()
        .append_batch("journal", vec![serde_json::json!(1), serde_json::json!(2)])
        .await
        .unwrap();
    let sealed = held.streams().seal("journal").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 2));
    assert_eq!(
        held.streams()
            .read_window("journal", 0, 10)
            .await
            .unwrap()
            .len(),
        2
    );

    drop_database(&uri, &database).await.unwrap();
}
