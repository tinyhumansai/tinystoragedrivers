use super::*;
#[cfg(feature = "rabbitmq")]
use tinystoragedrivers_types::{NewMessage, QueueSpec};

#[test]
fn parses_storage_and_hides_broker_credentials() {
    assert_eq!(QueueConfig::parse("").unwrap(), QueueConfig::Storage);
    assert_eq!(QueueConfig::parse("storage").unwrap(), QueueConfig::Storage);
    assert_eq!(QueueConfig::Storage.to_string(), "storage");
    assert_eq!(redacted("no scheme"), "no scheme");
    assert_eq!(redacted("amqp://broker/vhost"), "amqp://broker/vhost");
    assert!(QueueConfig::parse("https://broker/queue").is_err());
    let parsed = QueueConfig::parse("amqps://user:secret@broker/vhost");
    if cfg!(feature = "rabbitmq") {
        let config = parsed.unwrap();
        assert_eq!(config.to_string(), "amqps://***@broker/vhost");
        assert!(!format!("{config:?}").contains("secret"));
    } else {
        assert!(parsed.unwrap_err().message().contains("rabbitmq feature"));
    }
}

#[tokio::test]
async fn storage_selection_preserves_the_supplied_primitive_bundle() {
    let backend = crate::open(&crate::StorageConfig::Memory).await.unwrap();
    let scope = Scope::new("local").unwrap();
    let storage = backend.for_scope(&scope).unwrap();
    let storage = open_queues(&QueueConfig::Storage, storage.clone(), &scope)
        .await
        .unwrap();
    storage.kv().put("key", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        storage
            .kv()
            .get("key")
            .await
            .unwrap()
            .map(|value| value.doc),
        Some(serde_json::json!(1))
    );
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
async fn configured_rabbitmq_port_preserves_other_ports_and_delivers_messages() {
    let Ok(url) = std::env::var("TSD_RABBITMQ_URL") else {
        return;
    };
    let backend = crate::open(&crate::StorageConfig::Memory).await.unwrap();
    let scope = Scope::new(&format!("facade-rabbit-{}", std::process::id())).unwrap();
    let original = backend.for_scope(&scope).unwrap();
    let storage = open_queues(&QueueConfig::parse(&url).unwrap(), original.clone(), &scope)
        .await
        .unwrap();
    storage
        .kv()
        .put("facade-preservation", serde_json::json!("kept"))
        .await
        .unwrap();
    assert_eq!(
        storage
            .kv()
            .get("facade-preservation")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!("kept")
    );

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = format!("facade_{}_{nonce}", std::process::id());
    storage
        .queues()
        .ensure_queue(&queue, &QueueSpec::default())
        .await
        .unwrap();
    storage
        .queues()
        .enqueue(&queue, &[NewMessage::new(serde_json::json!({"ok": true}))])
        .await
        .unwrap();
    let deliveries = storage
        .queues()
        .receive(&queue, 1, 5_000, "facade-test")
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].payload, serde_json::json!({"ok": true}));
    assert!(
        storage
            .queues()
            .ack(&queue, &deliveries[0].receipt)
            .await
            .unwrap()
    );
    storage.queues().purge(&queue).await.unwrap();
}

#[cfg(feature = "rabbitmq")]
#[test]
fn plaintext_broker_credentials_need_loopback_or_the_opt_in() {
    let refused = QueueConfig::parse("amqp://user:secret@broker.example/vhost").unwrap_err();
    assert!(refused.message().contains("amqps://"));
    assert!(!refused.message().contains("secret"));
    for allowed in [
        "amqps://user:secret@broker.example/vhost",
        "amqp://user:secret@localhost/vhost",
        "amqp://broker.example/vhost",
    ] {
        assert_eq!(
            QueueConfig::parse(allowed).unwrap(),
            QueueConfig::RabbitMq {
                url: allowed.to_owned()
            }
        );
    }
    // The opt-in stays on the parsed URL; the queue port strips it on connect.
    let opted_in = "amqp://user:secret@broker.example/vhost?insecure=true";
    assert_eq!(
        QueueConfig::parse(opted_in).unwrap(),
        QueueConfig::RabbitMq {
            url: opted_in.to_owned()
        }
    );
}
