//! MongoDB failures mapped onto [`ErrorKind`].
//!
//! Callers branch on the kind, so the mapping decides what is retryable:
//! network failures, elections, write conflicts and anything the server labels
//! transient become [`ErrorKind::Unavailable`]. Messages name only the action
//! that failed. The server's own text can quote document values (a duplicate
//! key) and a connection string can carry a password, so neither is copied
//! into the message; the original error stays reachable as the source.

use mongodb::error::{Error, ErrorKind as MongoKind, WriteFailure};
use tinystoragedrivers_core::{ErrorKind, StorageError};

use crate::naming::duplicate_key_index;

/// Server error codes after which a retry may succeed.
const RETRYABLE_CODES: [i32; 15] = [
    6,     // HostUnreachable
    7,     // HostNotFound
    50,    // MaxTimeMSExpired
    89,    // NetworkTimeout
    91,    // ShutdownInProgress
    112,   // WriteConflict
    189,   // PrimarySteppedDown
    251,   // NoSuchTransaction
    262,   // ExceededTimeLimit
    9001,  // SocketException
    10107, // NotWritablePrimary
    11600, // InterruptedAtShutdown
    11602, // InterruptedDueToReplStateChange
    13435, // NotPrimaryNoSecondaryOk
    13436, // NotPrimaryOrSecondary
];

/// Codes the server uses for a duplicate key.
const DUPLICATE_KEY_CODES: [i32; 3] = [11000, 11001, 12582];

/// Labels the server and driver attach to errors worth retrying.
const RETRYABLE_LABELS: [&str; 3] = [
    "TransientTransactionError",
    "RetryableWriteError",
    "UnknownTransactionCommitResult",
];

/// What kind of failure the driver reported, before codes and labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Origin {
    /// The connection, pool or server selection failed.
    Network,
    /// The server rejected the command or write.
    Server,
    /// A value could not be encoded or decoded.
    Encoding,
    /// The caller passed something the client refused.
    Argument,
    /// Anything else.
    Other,
}

/// The [`ErrorKind`] for a failure of `origin` with server `code` and
/// `labels`.
pub(crate) fn classify(origin: Origin, code: Option<i32>, labels: &[&str]) -> ErrorKind {
    if origin == Origin::Network
        || code.is_some_and(|code| RETRYABLE_CODES.contains(&code))
        || labels.iter().any(|label| RETRYABLE_LABELS.contains(label))
    {
        return ErrorKind::Unavailable;
    }
    if code.is_some_and(|code| DUPLICATE_KEY_CODES.contains(&code)) {
        return ErrorKind::AlreadyExists;
    }
    match origin {
        Origin::Encoding => ErrorKind::Serialization,
        Origin::Argument => ErrorKind::InvalidInput,
        _ => ErrorKind::Backend,
    }
}

fn origin(error: &Error) -> Origin {
    match error.kind.as_ref() {
        MongoKind::Io(_)
        | MongoKind::ConnectionPoolCleared { .. }
        | MongoKind::ServerSelection { .. }
        | MongoKind::DnsResolve { .. } => Origin::Network,
        MongoKind::Command(_) | MongoKind::Write(_) | MongoKind::InsertMany(_) => Origin::Server,
        MongoKind::BsonSerialization(_) | MongoKind::BsonDeserialization(_) => Origin::Encoding,
        MongoKind::InvalidArgument { .. } => Origin::Argument,
        _ => Origin::Other,
    }
}

/// The server error code and message, if the server reported one.
fn code_and_message(error: &Error) -> Option<(i32, &str)> {
    match error.kind.as_ref() {
        MongoKind::Command(command) => Some((command.code, command.message.as_str())),
        MongoKind::Write(WriteFailure::WriteError(write)) => {
            Some((write.code, write.message.as_str()))
        }
        MongoKind::Write(WriteFailure::WriteConcernError(concern)) => {
            Some((concern.code, concern.message.as_str()))
        }
        MongoKind::InsertMany(many) => many
            .write_errors
            .as_ref()
            .and_then(|errors| errors.first())
            .map(|first| (first.code, first.message.as_str()))
            .or_else(|| {
                many.write_concern_error
                    .as_ref()
                    .map(|concern| (concern.code, concern.message.as_str()))
            }),
        _ => None,
    }
}

/// The index a duplicate-key failure names, or `None` for any other failure.
pub(crate) fn duplicate_index(error: &Error) -> Option<String> {
    let (code, message) = code_and_message(error)?;
    if !DUPLICATE_KEY_CODES.contains(&code) {
        return None;
    }
    Some(duplicate_key_index(message).unwrap_or_default().to_owned())
}

/// Whether the server labelled `error` as safe to retry the whole transaction.
pub(crate) fn is_transient(error: &Error) -> bool {
    error.contains_label("TransientTransactionError")
}

/// Whether a commit may or may not have applied.
pub(crate) fn is_unknown_commit(error: &Error) -> bool {
    error.contains_label("UnknownTransactionCommitResult")
}

/// Map a MongoDB failure while doing `action` (`"insert a document"`).
pub(crate) fn map(error: Error, action: &str) -> StorageError {
    let labels: Vec<&str> = error.labels().iter().map(String::as_str).collect();
    let kind = classify(
        origin(&error),
        code_and_message(&error).map(|(code, _)| code),
        &labels,
    );
    let message = match kind {
        ErrorKind::Unavailable => format!("mongodb is unavailable; could not {action}"),
        ErrorKind::AlreadyExists => {
            format!("could not {action}: a unique index already holds this value")
        }
        _ => format!("mongodb could not {action}"),
    };
    StorageError::new(kind, message).with_source(error)
}

/// A `map_err` adapter mapping a MongoDB failure while doing `action`.
pub(crate) fn failed(action: &'static str) -> impl Fn(Error) -> StorageError {
    move |error| map(error, action)
}

/// A `map_err` adapter mapping a GridFS stream failure while doing `action`.
pub(crate) fn failed_io(action: &'static str) -> impl Fn(std::io::Error) -> StorageError {
    move |error| map_io(error, action)
}

/// Map an I/O failure from a GridFS stream, which wraps a MongoDB error when
/// the server was involved.
pub(crate) fn map_io(error: std::io::Error, action: &str) -> StorageError {
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Error>())
    {
        Some(inner) => map(inner.clone(), action),
        None => StorageError::unavailable(format!("mongodb is unavailable; could not {action}"))
            .with_source(error),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
