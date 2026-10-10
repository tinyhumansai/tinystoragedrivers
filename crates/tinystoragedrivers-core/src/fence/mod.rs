//! Fencing: writes that land only while a guard document still allows them.
//!
//! A lease says which node owns a piece of work, but it cannot stop a node
//! that lost the lease from writing. A paused or partitioned holder keeps
//! running until it notices, and a check made before the write ("do I still
//! hold epoch 7?") can pass and then go stale before the write lands. The only
//! cure is to make the check and the write one atomic step inside the backend.
//!
//! A [`Fence`] names a *guard document* (scope, collection, id) and a
//! [`Filter`] it must match, typically the lease record and "epoch is still
//! 7". [`StorageBackend::for_scope_fenced`] hands out port handles whose every
//! write reads the guard in the same atomic step as the write and fails with
//! [`ErrorKind::Fenced`](crate::ErrorKind::Fenced), changing nothing, when the
//! guard is missing, expired, or no longer matches. A takeover that bumps the
//! epoch therefore cuts off the old holder's writes at the storage layer, no
//! matter how long the old holder was paused.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::{
//!     ErrorKind, Fence, Filter, MemoryStorage, Precondition, Scope, StorageBackend,
//! };
//!
//! let storage = MemoryStorage::new();
//! let cluster = storage.for_scope(&Scope::new("cluster")?)?;
//! cluster
//!     .documents()
//!     .put("leases", "profile-1", json!({"owner": "node-a", "epoch": 7}), Precondition::Absent)
//!     .await?;
//!
//! let fence = Fence::new(
//!     Scope::new("cluster")?,
//!     "leases",
//!     "profile-1",
//!     Filter::eq("owner", "node-a").and(Filter::eq("epoch", 7)),
//! );
//! let alice = storage.for_scope_fenced(&Scope::new("alice")?, &fence)?;
//! alice.documents().put("notes", "n1", json!({"text": "hi"}), Precondition::None).await?;
//!
//! // Another node takes the lease over: epoch 8.
//! cluster
//!     .documents()
//!     .put("leases", "profile-1", json!({"owner": "node-b", "epoch": 8}), Precondition::None)
//!     .await?;
//! let late = alice.documents().put("notes", "n1", json!({"text": "stale"}), Precondition::None).await;
//! assert_eq!(late.unwrap_err().kind(), ErrorKind::Fenced);
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! [`StorageBackend::for_scope_fenced`]: crate::StorageBackend::for_scope_fenced

use serde_json::Value;

use crate::document::{Versioned, validate_collection, validate_id};
use crate::error::{Result, StorageError};
use crate::filter::Filter;
use crate::scope::Scope;

/// A guard document every fenced write re-checks atomically. See the module
/// docs.
///
/// The guard lives in the same backend and named database as the handles it
/// fences, in its own [`Scope`] (often a shared cluster scope that holds the
/// lease records).
#[derive(Debug, Clone, PartialEq)]
pub struct Fence {
    scope: Scope,
    collection: String,
    id: String,
    filter: Filter,
}

impl Fence {
    /// A fence that holds while the document `collection/id` in `scope`
    /// exists, is live (not expired), and matches `filter`.
    #[must_use]
    pub fn new(
        scope: Scope,
        collection: impl Into<String>,
        id: impl Into<String>,
        filter: Filter,
    ) -> Self {
        Self {
            scope,
            collection: collection.into(),
            id: id.into(),
            filter,
        }
    }

    /// A fence that holds while the guard document's `field` equals `epoch`:
    /// the usual lease-epoch token.
    #[must_use]
    pub fn epoch(
        scope: Scope,
        collection: impl Into<String>,
        id: impl Into<String>,
        field: impl Into<String>,
        epoch: u64,
    ) -> Self {
        Self::new(scope, collection, id, Filter::eq(field, epoch))
    }

    /// The scope the guard document lives in.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// The guard document's collection.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The guard document's id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// What the guard document must match.
    #[must_use]
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Check the guard's address and filter.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid collection name, document id or filter.
    pub fn validate(&self) -> Result<()> {
        validate_collection(&self.collection)?;
        validate_id(&self.id)?;
        self.filter.validate()
    }

    /// Decide a write against the guard document as the driver read it, live
    /// documents only (`None` when it is absent or expired). Drivers call
    /// this inside the same atomic step as the write.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Fenced`](crate::ErrorKind::Fenced) when the guard is
    /// absent or does not match the filter.
    pub fn check(&self, guard: Option<&Versioned<Value>>) -> Result<()> {
        match guard {
            None => Err(StorageError::fenced(format!(
                "fence document `{}/{}` is absent",
                self.collection, self.id
            ))),
            Some(found) if !self.filter.matches(&found.id, &found.doc) => {
                Err(StorageError::fenced(format!(
                    "fence document `{}/{}` no longer matches at version {}",
                    self.collection, self.id, found.version.0
                )))
            }
            Some(_) => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
