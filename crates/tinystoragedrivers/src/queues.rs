//! Select and install a scope-bound queue port independently of storage URLs.

use std::fmt;

#[cfg(feature = "rabbitmq")]
use tinystoragedrivers_api::QueueStore;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_types::{Result, Scope, StorageError};

/// A queue location independent of the document, stream, KV, blob and secret ports.
#[derive(Clone, PartialEq, Eq)]
pub enum QueueConfig {
    /// Keep the queue port already present in the supplied [`Storage`].
    Storage,
    /// Connect to a `RabbitMQ` broker.
    RabbitMq {
        /// Broker URL, credentials included and redacted when rendered.
        url: String,
    },
}

impl QueueConfig {
    /// Parse `storage`, an empty string, or an AMQP broker URL.
    ///
    /// A broker URL must pass `tinystoragedrivers_queue::check_broker_url`:
    /// credentials over plaintext `amqp://` are refused unless the host is
    /// loopback or the URL opts in with `?insecure=true`. The URL is kept as
    /// given; the opt-in is stripped when the queue port connects.
    ///
    /// # Errors
    /// Returns `InvalidInput` for malformed or feature-disabled broker URLs and
    /// for plaintext credentials without the opt-in.
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() || url == "storage" {
            return Ok(Self::Storage);
        }
        if url.starts_with("amqp://") || url.starts_with("amqps://") {
            #[cfg(not(feature = "rabbitmq"))]
            return Err(StorageError::invalid_input(
                "an amqp queue URL needs the rabbitmq feature",
            ));
            #[cfg(feature = "rabbitmq")]
            {
                tinystoragedrivers_queue::check_broker_url(url)?;
                return Ok(Self::RabbitMq {
                    url: url.to_owned(),
                });
            }
        }
        Err(StorageError::invalid_input(
            "a queue URL is `storage`, `amqp://…` or `amqps://…`",
        ))
    }
}

fn redacted(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{scheme}://***{}", &rest[at..]),
        None => url.to_owned(),
    }
}

impl fmt::Display for QueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage => f.write_str("storage"),
            Self::RabbitMq { url } => f.write_str(&redacted(url)),
        }
    }
}

impl fmt::Debug for QueueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QueueConfig({self})")
    }
}

/// Open and install the selected queue port on a scope-bound `storage` bundle.
///
/// The caller owns queue manifests and schema policy; this helper only selects
/// the port. The scope is supplied separately because the storage URL does not
/// own host scope policy.
///
/// # Errors
/// Returns `Unavailable` if the `RabbitMQ` broker cannot be reached.
pub async fn open_queues(config: &QueueConfig, storage: Storage, scope: &Scope) -> Result<Storage> {
    #[cfg(not(feature = "rabbitmq"))]
    let _ = scope;
    match config {
        QueueConfig::Storage => Ok(storage),
        #[cfg(feature = "rabbitmq")]
        QueueConfig::RabbitMq { url } => {
            let queues: std::sync::Arc<dyn QueueStore> = std::sync::Arc::new(
                tinystoragedrivers_queue::RabbitQueues::connect(url, scope).await?,
            );
            Ok(Storage::new(
                storage.documents().clone(),
                storage.streams().clone(),
                storage.kv().clone(),
                storage.blobs().clone(),
                queues,
                storage.secrets().clone(),
            ))
        }
        #[cfg(not(feature = "rabbitmq"))]
        QueueConfig::RabbitMq { .. } => Err(StorageError::invalid_input(
            "an amqp queue URL needs the rabbitmq feature",
        )),
    }
}

#[cfg(test)]
#[path = "queues_tests.rs"]
mod tests;
