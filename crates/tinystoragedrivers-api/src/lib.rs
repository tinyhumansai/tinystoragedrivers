//! Runtime-neutral asynchronous storage ports.
//!
//! This crate contains contracts only. Engine selection and composition belong
//! to a host or a primitive engine crate.

mod backend;
mod blob;
mod document;
mod kv;
mod queue;
mod secrets;
mod stream;
mod watch;

/// Attribute used to declare object-safe async port implementations.
pub use async_trait::async_trait;
pub use backend::{Storage, StorageBackend};
pub use blob::{BlobReader, BlobStore, BlobStream};
pub use document::{DocumentStore, DocumentStoreExt};
pub use kv::KvStore;
pub use queue::QueueStore;
pub use secrets::SecretStore;
pub use stream::StreamStore;
/// Shared serialized storage value types.
pub use tinystoragedrivers_types as types;
pub use watch::DocumentWatch;

/// API contract version, independent of the `TinyBus` wire contract.
pub const CONTRACT_VERSION: u32 = 6;

/// Whether a participant implements this exact API contract.
#[must_use]
pub const fn is_compatible(version: u32) -> bool {
    version == CONTRACT_VERSION
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
