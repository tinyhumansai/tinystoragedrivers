//! Fenced writes on MongoDB: the guard check and the write share one
//! multi-document transaction.
//!
//! Reading the guard inside the transaction is not enough on its own:
//! MongoDB transactions run under snapshot isolation, which detects
//! write-write conflicts but not a write racing a read. So the transaction
//! also *writes* the guard document, incrementing a driver field
//! ([`FENCE`]) at the version it read. A takeover that moves the guard on
//! then conflicts with every fenced transaction still in flight: either the
//! takeover commits first and the fenced transaction aborts (and its retry
//! sees the new epoch and is refused), or the fenced write commits first and
//! the takeover waits for it. Either way no write lands after the takeover.
//!
//! The cost is that fenced writes through one fence serialize on its guard
//! document: concurrent ones abort each other with transient write
//! conflicts and are retried, up to [`FENCE_ATTEMPTS`] times.
//!
//! Requires a deployment that runs transactions (a replica set or a sharded
//! cluster); without one the backend does not offer
//! [`Capability::Fencing`](tinystoragedrivers_core::Capability::Fencing).
//! GridFS cannot join a transaction, so fenced blob writes are refused.

use mongodb::ClientSession;
use mongodb::bson::doc;
use tinystoragedrivers_core::{CollectionSpec, Fence, Result, StorageError};

use crate::backend::Shared;
use crate::documents::stored::{Stored, at_version, is_live};
use crate::documents::{CommitFailure, MongoDocuments, commit_failure};
use crate::errors;
use crate::naming::{FENCE, document_id};

/// How many transactions one fenced write starts before giving up with a
/// retryable error.
pub(crate) const FENCE_ATTEMPTS: usize = 64;

/// Run `$body` (an expression using `$session: &mut ClientSession`) inside a
/// fenced transaction, retrying transient aborts. Evaluates to the body's
/// value as a `Result`.
macro_rules! in_fence {
    ($shared:expr, $fence:expr, $session:ident => $body:expr) => {{
        let mut txn = $crate::fence::FencedTxn::start($shared, $fence).await?;
        loop {
            let $session = txn.begin().await?;
            let outcome = $body;
            if let Some(done) = txn.finish(outcome).await? {
                break Ok::<_, tinystoragedrivers_core::StorageError>(done);
            }
        }
    }};
}
pub(crate) use in_fence;

/// One fenced write's session and attempt count.
pub(crate) struct FencedTxn<'a> {
    shared: &'a Shared,
    fence: &'a Fence,
    session: ClientSession,
    attempts: usize,
    last: StorageError,
}

impl std::fmt::Debug for FencedTxn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FencedTxn")
            .field("fence", &self.fence)
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

fn kept_aborting() -> StorageError {
    StorageError::unavailable("fenced transaction kept aborting; retry")
}

impl<'a> FencedTxn<'a> {
    /// Open the session the attempts share.
    ///
    /// # Errors
    ///
    /// When no session can be started.
    pub(crate) async fn start(shared: &'a Shared, fence: &'a Fence) -> Result<Self> {
        let session = shared
            .client
            .start_session()
            .await
            .map_err(errors::failed("start a session"))?;
        Ok(Self {
            shared,
            fence,
            session,
            attempts: 0,
            last: kept_aborting(),
        })
    }

    /// Start the next attempt's transaction and pass the fence inside it,
    /// then hand out the session the write must use.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Fenced`](tinystoragedrivers_core::ErrorKind::Fenced) when
    /// the fence does not hold, the last transient error once every attempt
    /// is spent, or a server error.
    pub(crate) async fn begin(&mut self) -> Result<&mut ClientSession> {
        // Outside the transaction: the spec cache may read the server.
        let spec = self.shared.spec(self.fence.collection()).await?;
        loop {
            if self.attempts == FENCE_ATTEMPTS {
                return Err(std::mem::replace(&mut self.last, kept_aborting()));
            }
            self.attempts += 1;
            self.session
                .start_transaction()
                .await
                .map_err(errors::failed("start a transaction"))?;
            match self.guard(&spec).await {
                Ok(()) => return Ok(&mut self.session),
                Err(error) => {
                    let _ = self.session.abort_transaction().await;
                    if !error.is_retryable() {
                        return Err(error);
                    }
                    self.last = error;
                }
            }
        }
    }

    /// Check the guard document and hold it for this transaction.
    async fn guard(&mut self, spec: &CollectionSpec) -> Result<()> {
        let fence = self.fence;
        let handle = self.shared.scoped(fence.collection(), fence.scope());
        let found = handle
            .find_one(
                doc! {"_id": document_id(fence.scope(), fence.id())},
                Some(&mut self.session),
            )
            .await
            .map_err(errors::failed("read a fence document"))?;
        let now = self.shared.now();
        let Some(live) = found
            .as_ref()
            .map(Stored::decode)
            .transpose()?
            .filter(|stored| is_live(spec, stored, now))
        else {
            return fence.check(None);
        };
        let version = live.version;
        fence.check(Some(&live.into_versioned()))?;
        // The write that makes a concurrent takeover conflict with us.
        let held = handle
            .update_one(
                at_version(fence.scope(), fence.id(), version),
                doc! {"$inc": {FENCE: 1_i64}},
                false,
                Some(&mut self.session),
            )
            .await
            .map_err(errors::failed("hold a fence document"))?;
        if held.matched_count == 1 {
            Ok(())
        } else {
            Err(StorageError::unavailable(
                "fence document changed while it was checked; retry",
            ))
        }
    }

    /// End the attempt with the write's `outcome`: commit it, or abort.
    /// `Ok(None)` means the attempt was aborted transiently and the caller
    /// should [`begin`](Self::begin) again.
    ///
    /// # Errors
    ///
    /// A non-transient write error (nothing was applied), or a commit that
    /// failed or whose outcome is unknown.
    pub(crate) async fn finish<T>(&mut self, outcome: Result<T>) -> Result<Option<T>> {
        let value = match outcome {
            Ok(value) => value,
            Err(error) => {
                let _ = self.session.abort_transaction().await;
                if !error.is_retryable() {
                    return Err(error);
                }
                self.last = error;
                return Ok(None);
            }
        };
        let committed = MongoDocuments::commit(&mut self.session).await;
        settle(&mut self.last, value, committed)
    }
}

/// What a commit's result means for the fenced write: done, retry
/// (`Ok(None)`, recording the abort in `last`), or a final error.
///
/// # Errors
///
/// A commit that failed outright or whose outcome is unknown.
pub(crate) fn settle<T>(
    last: &mut StorageError,
    value: T,
    committed: std::result::Result<(), mongodb::error::Error>,
) -> Result<Option<T>> {
    match committed {
        Ok(()) => Ok(Some(value)),
        Err(error) => match commit_failure(&error) {
            CommitFailure::Aborted => {
                *last = errors::map(error, "commit a fenced write");
                Ok(None)
            }
            CommitFailure::Unknown => Err(StorageError::backend(
                "the fenced write may or may not have committed; read before retrying",
            )
            .with_source(error)),
            CommitFailure::Failed => Err(errors::map(error, "commit a fenced write")),
        },
    }
}

/// The refusal for a write a fenced MongoDB handle cannot make atomic.
pub(crate) fn unfenceable(what: &str) -> StorageError {
    StorageError::unsupported(
        tinystoragedrivers_core::Capability::Fencing,
        format!("{what} cannot be fenced on mongodb: gridfs cannot join a transaction"),
    )
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
