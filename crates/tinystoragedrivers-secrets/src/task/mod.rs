//! Running blocking work (disk I/O, an advisory lock, an OS keychain call)
//! from the async port without stalling the caller's runtime.

use tinystoragedrivers_core::{Result, StorageError};

/// Run `work` on the current tokio runtime's blocking pool, or inline when the
/// caller is not inside a tokio runtime.
pub(crate) async fn run_blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.spawn_blocking(work).await.map_err(|e| {
            StorageError::backend("blocking secrets task did not complete").with_source(e)
        })?,
        Err(_) => work(),
    }
}

/// Map an I/O failure to the kind a caller can act on: transient conditions
/// are [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable),
/// everything else [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend).
/// `what` names the operation, never a path or a secret name.
pub(crate) fn io_error(what: &str, error: std::io::Error) -> StorageError {
    use std::io::ErrorKind as Io;
    let transient = matches!(
        error.kind(),
        Io::WouldBlock | Io::Interrupted | Io::TimedOut
    );
    let message = format!("could not {what}");
    let mapped = if transient {
        StorageError::unavailable(message)
    } else {
        StorageError::backend(message)
    };
    mapped.with_source(error)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
