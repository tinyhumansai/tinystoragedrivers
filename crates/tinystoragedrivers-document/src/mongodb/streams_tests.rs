use super::*;

#[test]
fn stream_seal_marker_uses_legacy_generation_blob_key_when_key_is_absent() {
    let legacy_key = MongoStreams::generation_blob_key("events", 7).unwrap();
    assert_eq!(
        MongoStreams::marker_blob_key(&Document::new(), "events", 7).unwrap(),
        legacy_key
    );
    assert_eq!(
        MongoStreams::marker_blob_key(&doc! {"bk": "attempt/object-id"}, "events", 7).unwrap(),
        "attempt/object-id"
    );
}

async fn streams_without_server() -> MongoStreams {
    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:1")
        .await
        .unwrap();
    MongoStreams {
        client: client.clone(),
        database: client.database("streams_without_server"),
        scope: tinystoragedrivers_types::Scope::new("scope").unwrap(),
        clock: std::sync::Arc::new(|| 0),
        transactions: false,
        blobs: None,
        condition_guard: None,
        truncate_pause: None,
        seal_pause: None,
    }
}

#[tokio::test]
async fn sealing_without_shared_blob_support_is_explicitly_unsupported() {
    let streams = streams_without_server().await;
    assert!(!streams.capabilities().contains(Capability::StreamSeal));
    assert_eq!(
        streams.seal("events").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(Capability::StreamSeal)
    );
}

#[tokio::test]
async fn invalid_names_and_unrepresentable_appends_fail_before_database_io() {
    let streams = streams_without_server().await;
    assert_eq!(
        MongoStreams::generation_blob_key("", 0).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        streams
            .append_at("events", None, vec![serde_json::json!(u64::MAX)])
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Serialization
    );
}
