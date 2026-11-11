//! Queue engines implementing [`tinystoragedrivers_api::QueueStore`].
//!
//! The default `memory` feature provides a native in-memory queue. The
//! `document` feature provides a queue over any document port, and `rabbitmq`
//! provides a broker-backed queue.

#[cfg(any(feature = "memory", feature = "document"))]
mod clock;
#[cfg(feature = "document")]
mod document;
mod key;
#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "rabbitmq")]
mod rabbitmq;

#[cfg(any(feature = "memory", feature = "document"))]
pub use clock::Clock;
#[cfg(feature = "document")]
pub use document::{DocumentQueue, GROUPS, MESSAGES, SPECS, collection_specs, ensure};
#[cfg(feature = "memory")]
pub use memory::MemoryQueue;
#[cfg(feature = "rabbitmq")]
pub use rabbitmq::{MAX_DELAY_MS, RabbitQueues, check_broker_url};

pub use tinystoragedrivers_api::QueueStore;
pub use tinystoragedrivers_types::{
    DeadLetter, Delivery, Enqueued, MAX_PRIORITY, MAX_QUEUE_NAME_LEN, NewMessage, QueueFeatures,
    QueueSpec, QueueStats, Receipt, Retry, validate_queue_name,
};

fn validate_message(message: &NewMessage) -> tinystoragedrivers_types::Result<()> {
    if message.priority > MAX_PRIORITY {
        return Err(tinystoragedrivers_types::StorageError::invalid_input(
            format!("a message priority runs 0 to {MAX_PRIORITY}"),
        ));
    }
    for (what, value) in [
        ("group", &message.group),
        ("dedupe key", &message.dedupe_key),
    ] {
        if value.as_deref().is_some_and(str::is_empty) {
            return Err(tinystoragedrivers_types::StorageError::invalid_input(
                format!("a message {what} must not be empty"),
            ));
        }
    }
    Ok(())
}

fn validate_spec(spec: &QueueSpec) -> tinystoragedrivers_types::Result<()> {
    if spec.max_attempts == 0 {
        return Err(tinystoragedrivers_types::StorageError::invalid_input(
            "a queue needs at least one attempt per message",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod testkit_tests;
