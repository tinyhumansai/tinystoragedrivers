//! A connected backend and the scoped port handles it hands out.
//!
//! A host opens one [`StorageBackend`] at boot (from a URL, through the
//! `tinystoragedrivers` facade) and asks it for a [`ScopedStorage`] per tenant.
//! [`StorageBackend::database`] splits one backend into independent named
//! databases: separate files under a SQLite directory, a collection prefix on
//! `MongoDB`, a separate map in memory. Owners use it to keep their data apart
//! (`approvals`, `sessions`, `flows`) without inventing collection prefixes.

use std::fmt;
use std::sync::Arc;

use crate::blob::BlobStore;
use crate::capabilities::{Capabilities, Capability};
use crate::document::DocumentStore;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::scope::Scope;
use crate::stream::StreamStore;

/// A connected storage backend.
pub trait StorageBackend: Send + Sync + fmt::Debug {
    /// Driver name, for logs: `"memory"`, `"sqlite"`, `"mongodb"`, `"file"`.
    fn driver(&self) -> &'static str;

    /// Optional abilities every handle from this backend has.
    fn capabilities(&self) -> Capabilities;

    /// Port handles bound to `scope`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// backend only serves particular scopes (a single-operator SQLite file
    /// serves only [`Scope::local`]), or a backend error.
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage>;

    /// Port handles bound to `scope` whose writes land only while `fence`
    /// holds (see [`Fence`]). Requires [`Capability::Fencing`].
    ///
    /// Every write through the returned handles reads the fence's guard
    /// document (in this backend and named database, in the fence's own
    /// scope) in the same atomic step as the write, before the write's own
    /// precondition. When the guard is absent, expired, or does not match the
    /// fence's filter, the write fails with
    /// [`ErrorKind::Fenced`](crate::ErrorKind::Fenced) and changes nothing,
    /// even if it would have changed nothing anyway.
    ///
    /// The fenced writes are the document port's `put`, `delete`,
    /// `delete_where`, `claim`, `atomic_batch` and `drop_collection`, the
    /// stream port's `append`, `append_batch`, `truncate_before` and
    /// `delete_stream`, and the blob port's `put` and `delete`. Reads and
    /// `ensure_collection` are not fenced. A driver that cannot make one of
    /// these writes atomic with the check refuses it with
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported)`(Fencing)`
    /// rather than run it unfenced; each driver's README says which, if any.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::Fencing`] (the default),
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid fence or a scope the backend does not serve, or a backend
    /// error.
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        let _ = (scope, fence);
        Err(StorageError::unsupported(
            Capability::Fencing,
            "this storage driver cannot fence writes",
        ))
    }

    /// The named database `name` on this backend. The same name always
    /// addresses the same data.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid name (see [`validate_database`]), or a backend error.
    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>>;
}

/// Validate a database name: 1 to 64 ASCII lowercase letters, digits, `_` or
/// `-`, so it is safe as a file stem and a collection prefix.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_database(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(StorageError::invalid_input(
            "database name must be 1 to 64 lowercase ASCII letters, digits, `_` or `-`",
        ))
    }
}

/// The ports of one backend, bound to one scope.
#[derive(Clone)]
pub struct ScopedStorage {
    scope: Scope,
    driver: &'static str,
    documents: Arc<dyn DocumentStore>,
    streams: Arc<dyn StreamStore>,
    blobs: Arc<dyn BlobStore>,
}

impl ScopedStorage {
    /// Assemble handles a driver opened for `scope`.
    #[must_use]
    pub fn new(
        scope: Scope,
        driver: &'static str,
        documents: Arc<dyn DocumentStore>,
        streams: Arc<dyn StreamStore>,
        blobs: Arc<dyn BlobStore>,
    ) -> Self {
        Self {
            scope,
            driver,
            documents,
            streams,
            blobs,
        }
    }

    /// The scope every handle is bound to.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// The driver that opened these handles.
    #[must_use]
    pub fn driver(&self) -> &'static str {
        self.driver
    }

    /// The document port.
    #[must_use]
    pub fn documents(&self) -> &Arc<dyn DocumentStore> {
        &self.documents
    }

    /// The stream port.
    #[must_use]
    pub fn streams(&self) -> &Arc<dyn StreamStore> {
        &self.streams
    }

    /// The blob port.
    #[must_use]
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }
}

impl fmt::Debug for ScopedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedStorage")
            .field("scope", &self.scope)
            .field("driver", &self.driver)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
