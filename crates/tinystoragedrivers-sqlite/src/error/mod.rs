//! Mapping SQLite failures onto [`ErrorKind`](tinystoragedrivers_core::ErrorKind).
//!
//! Lock contention (`SQLITE_BUSY`, `SQLITE_LOCKED`) is the only retryable
//! failure; everything else is a backend error carrying the original as its
//! source. Messages name the operation, never the data.

use rusqlite::ErrorCode;
use tinystoragedrivers_core::StorageError;

/// Map a rusqlite error raised while performing `operation`.
pub(crate) fn map(operation: &'static str, error: rusqlite::Error) -> StorageError {
    let busy = matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    );
    let mapped = if busy {
        StorageError::unavailable(format!("sqlite is busy during {operation}"))
    } else {
        StorageError::backend(format!("sqlite failed during {operation}"))
    };
    mapped.with_source(error)
}

/// Bind `operation` for use with `map_err`.
pub(crate) fn during(operation: &'static str) -> impl Fn(rusqlite::Error) -> StorageError {
    move |error| map(operation, error)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
