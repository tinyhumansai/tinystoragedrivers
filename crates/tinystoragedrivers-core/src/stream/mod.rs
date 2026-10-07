//! The stream port: append-only logs of JSON records.
//!
//! Transcripts, event journals and audit logs are streams. Every record gets
//! the next offset of its stream, starting at 0, with no gaps, so a reader that
//! remembers "I have seen up to offset 41" resumes exactly where it stopped.
//! Offsets are never reused, even after [`StreamStore::truncate_before`].

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, StorageError};

/// One record and its offset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    /// Position in the stream, starting at 0.
    pub offset: u64,
    /// The record.
    pub value: Value,
}

/// Append-only JSON logs, bound to one scope.
#[async_trait]
pub trait StreamStore: Send + Sync + std::fmt::Debug {
    /// Append one record and return its offset.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn append(&self, stream: &str, value: Value) -> Result<u64>;

    /// Append several records contiguously and return the first one's offset.
    /// An empty batch appends nothing and returns the current length.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error, with nothing appended.
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64>;

    /// Up to `limit` records starting at offset `from`, in offset order.
    /// Offsets below the truncation point are skipped.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>>;

    /// The offset the next append will receive: the number of records ever
    /// appended. A stream that does not exist has length 0.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn len(&self, stream: &str) -> Result<u64>;

    /// Discard every record below `offset` and report how many went. The
    /// stream's length is unchanged.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64>;

    /// Remove the stream entirely, so its length returns to 0. Reports
    /// whether it existed.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn delete_stream(&self, stream: &str) -> Result<bool>;

    /// Names of the streams in this scope that start with `prefix`, sorted.
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn streams(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Validate a stream name: 1 to 512 bytes without NUL.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_stream(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > crate::document::MAX_ID_LEN || name.contains('\0') {
        return Err(StorageError::invalid_input(
            "stream name must be 1 to 512 bytes without NUL",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
