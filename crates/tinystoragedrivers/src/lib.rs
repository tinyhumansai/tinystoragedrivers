//! Pluggable storage for OpenHuman and the libraries it hosts.
//!
//! One set of ports ([`DocumentStore`], [`StreamStore`], [`BlobStore`]) with a
//! driver per backend, chosen by URL at boot and compiled in by Cargo feature.
//! Every crate that persists data builds its typed repository on the ports and
//! stays unaware of which backend the host picked.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers::{Precondition, Scope, StorageConfig};
//!
//! let backend = tinystoragedrivers::open(&StorageConfig::parse("memory")?).await?;
//! let approvals = backend.database("approvals")?.for_scope(&Scope::local())?;
//! approvals
//!     .documents()
//!     .put("pending", "req-1", json!({"tool": "shell"}), Precondition::Absent)
//!     .await?;
//! # Ok::<(), tinystoragedrivers::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Drivers
//!
//! | URL | Driver | Feature |
//! | --- | --- | --- |
//! | `memory` | in-process maps | always |
//!
//! SQLite, MongoDB and file drivers land as separate crates and are forwarded
//! here behind `sqlite`, `mongodb` and `file` features.
//!
//! # Secrets
//!
//! With the `secrets` feature, [`secrets`] is the `tinystoragedrivers-secrets`
//! crate: the `SecretStore` port for API keys and tokens, with drivers for
//! memory, an encrypted `secrets.enc` file, and `enc2:` ciphertext in any
//! [`DocumentStore`] under a per-scope key. The `keyring` feature adds the OS
//! credential store. It has no URL form; a host builds the store it wants.
//!
//! This crate holds no repository for any particular record type; sessions,
//! approvals and workflow runs belong to the crates that own them.

mod config;

pub use config::{StorageConfig, open};
pub use tinystoragedrivers_core::*;

/// Encrypted secret storage (feature `secrets`).
#[cfg(feature = "secrets")]
pub use tinystoragedrivers_secrets as secrets;
