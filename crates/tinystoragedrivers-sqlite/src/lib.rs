//! SQLite driver for `tinystoragedrivers`.
//!
//! [`SqliteStorage`] implements the document, stream and blob ports on
//! generic tables, with every [`Capability`](tinystoragedrivers_core::Capability):
//! expiry, transactions (`atomic_batch` in one immediate transaction) and
//! full-text search (FTS5). It passes the shared conformance suite.
//!
//! [`SqliteNative`] gives owners that keep their own relational schema raw
//! access to the same file and connection, with a migration runner, so a
//! store can move onto the driver without rewriting its SQL.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};
//! use tinystoragedrivers_sqlite::SqliteStorage;
//!
//! let dir = tempfile::tempdir().unwrap();
//! let storage = SqliteStorage::open(dir.path())?;
//! let docs = storage.for_scope(&Scope::local())?.documents().clone();
//! docs.put("devices", "d1", json!({"name": "laptop"}), Precondition::Absent).await?;
//! assert_eq!(docs.get("devices", "d1").await?.unwrap().doc["name"], "laptop");
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! The crate pins `rusqlite` exactly (re-exported as [`rusqlite`]) because
//! `libsqlite3-sys` may appear only once in a host's dependency graph.

mod blobs;
mod connection;
mod documents;
mod error;
mod native;
mod sql;
mod storage;
mod streams;

pub use blobs::SqliteBlobs;
pub use documents::SqliteDocuments;
pub use native::{MIGRATIONS_TABLE, SqliteNative};
pub use rusqlite;
pub use storage::{Clock, ROOT_FILE, SqliteStorage};
pub use streams::SqliteStreams;
