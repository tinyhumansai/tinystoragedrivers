//! Storage ports, the in-memory driver, and the driver conformance suite.
//!
//! Every crate that persists data talks to these ports instead of a database
//! library, and a host picks the backend (SQLite on a desktop, `MongoDB` in the
//! cloud, memory in tests) once at boot. The ports are deliberately primitive:
//!
//! - [`DocumentStore`]: versioned JSON objects in named collections, with
//!   filters, sorting, paging, compare-and-swap, atomic claims, and optional
//!   expiry, transactions and full-text search.
//! - [`StreamStore`]: append-only logs with dense offsets.
//! - [`BlobStore`]: opaque bytes by key.
//!
//! Typed repositories (sessions, approvals, workflow runs) belong to the crates
//! that own those records and are built on these ports; this crate knows
//! nothing about them.
//!
//! A [`StorageBackend`] hands out a [`ScopedStorage`] per [`Scope`], the
//! tenant key every record carries, and splits into named databases with
//! [`StorageBackend::database`].
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::{
//!     Filter, MemoryStorage, Precondition, Query, Scope, StorageBackend,
//! };
//!
//! let storage = MemoryStorage::new();
//! let alice = storage.for_scope(&Scope::new("alice")?)?;
//! let docs = alice.documents();
//!
//! let v1 = docs.put("tasks", "t1", json!({"state": "open"}), Precondition::Absent).await?;
//! docs.put("tasks", "t1", json!({"state": "done"}), Precondition::Version(v1)).await?;
//!
//! let open = docs.query("tasks", &Query::filter(Filter::eq("state", "open"))).await?;
//! assert!(open.items.is_empty());
//!
//! let bob = storage.for_scope(&Scope::new("bob")?)?;
//! assert!(bob.documents().get("tasks", "t1").await?.is_none());
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Features
//!
//! - `blocking`: [`Blocking`], a bridge for synchronous callers.
//! - `testkit`: the [`conformance`] suite drivers run in their tests.

mod backend;
mod blob;
mod capabilities;
mod document;
mod error;
mod filter;
mod memory;
mod scope;
mod stream;
pub mod value;

#[cfg(any(test, feature = "blocking"))]
mod blocking;
#[cfg(any(test, feature = "testkit"))]
pub mod conformance;

pub use backend::{ScopedStorage, StorageBackend, validate_database};
pub use blob::{Blob, BlobMeta, BlobStore, MAX_BLOB_KEY_LEN, clamp_range, validate_blob_key};
#[cfg(any(test, feature = "blocking"))]
pub use blocking::Blocking;
pub use capabilities::{Capabilities, Capability};
pub use document::{
    CollectionSpec, Cursor, DocumentStore, DocumentStoreExt, IndexSpec, MAX_COLLECTION_LEN,
    MAX_ID_LEN, Page, Precondition, Query, RESERVED_PREFIX, SearchHit, SearchSpec, Version,
    Versioned, WriteOp, WriteResult, validate_collection, validate_doc, validate_id,
};
pub use error::{ErrorKind, Result, StorageError};
pub use filter::{Direction, Filter, ID_FIELD, Sort, sort_documents};
pub use memory::{Clock, MemoryBlobs, MemoryDocuments, MemoryStorage, MemoryStreams};
pub use scope::{MAX_SCOPE_LEN, Scope};
pub use stream::{StreamEntry, StreamStore, validate_stream};

/// The `async_trait` attribute the port traits are declared with, re-exported
/// so a driver implements them without naming the dependency itself.
pub use async_trait::async_trait;
