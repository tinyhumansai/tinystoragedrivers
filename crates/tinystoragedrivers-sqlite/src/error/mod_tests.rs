//! Busy and locked errors are retryable; the rest are backend errors.

use rusqlite::ffi;
use tinystoragedrivers_core::ErrorKind;

use super::*;

fn failure(code: std::ffi::c_int) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)
}

#[test]
fn lock_contention_is_unavailable() {
    for code in [ffi::SQLITE_BUSY, ffi::SQLITE_LOCKED] {
        let error = map("put", failure(code));
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.is_retryable());
        assert_eq!(error.message(), "sqlite is busy during put");
    }
}

#[test]
fn other_failures_are_backend_errors_with_a_source() {
    let error = during("query")(failure(ffi::SQLITE_CORRUPT));
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(std::error::Error::source(&error).is_some());
    let error = map("get", rusqlite::Error::QueryReturnedNoRows);
    assert_eq!(error.kind(), ErrorKind::Backend);
}
