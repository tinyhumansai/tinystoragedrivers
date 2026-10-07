//! The blob port: opaque byte objects addressed by key.
//!
//! Attachments, exported artifacts and large transcript bodies are blobs. Keys
//! are `/`-separated paths so [`BlobStore::list`] can enumerate a prefix, but no
//! driver promises directories exist.

use std::ops::Range;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};

/// The longest blob key a driver must accept, in bytes.
pub const MAX_BLOB_KEY_LEN: usize = 1024;

/// What is known about a blob without reading it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    /// The blob's key.
    pub key: String,
    /// Size in bytes.
    pub len: u64,
    /// MIME type, when the writer gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// A blob's metadata and bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// Metadata.
    pub meta: BlobMeta,
    /// Contents.
    pub bytes: Vec<u8>,
}

/// Opaque byte objects, bound to one scope.
#[async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug {
    /// Store `bytes` under `key`, replacing any previous blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta>;

    /// Read a whole blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn get(&self, key: &str) -> Result<Option<Blob>>;

    /// Read the bytes in `range`, clamped to the blob's length.
    ///
    /// # Errors
    ///
    /// An invalid key, an inverted range, or a backend error.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>>;

    /// Read a blob's metadata.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;

    /// Remove a blob and report whether it existed.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Metadata of every blob whose key starts with `prefix`, sorted by key.
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>>;
}

/// Validate a blob key: 1 to [`MAX_BLOB_KEY_LEN`] bytes, no NUL, no empty,
/// `.` or `..` path segment, and no leading `/`.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_blob_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_BLOB_KEY_LEN || key.contains('\0') {
        return Err(StorageError::invalid_input(format!(
            "blob key must be 1 to {MAX_BLOB_KEY_LEN} bytes without NUL"
        )));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(StorageError::invalid_input(
            "blob key has an empty, `.` or `..` segment",
        ));
    }
    Ok(())
}

/// Clamp `range` to a blob of `len` bytes.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the range
/// is inverted.
pub fn clamp_range(range: &Range<u64>, len: usize) -> Result<Range<usize>> {
    if range.start > range.end {
        return Err(StorageError::invalid_input(
            "blob range start is after its end",
        ));
    }
    let clamp = |at: u64| usize::try_from(at).map_or(len, |at| at.min(len));
    Ok(clamp(range.start)..clamp(range.end))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
