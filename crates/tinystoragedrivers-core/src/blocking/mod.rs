//! Calling the async ports from synchronous code.
//!
//! Some consumer traits that storage sits behind are still synchronous (a
//! workflow store, a keyring backend). [`Blocking`] runs a port's future to
//! completion on a runtime thread it owns, so a synchronous caller can use any
//! driver without knowing whether it is already inside a tokio runtime.
//! `block_in_place` would panic on a current-thread runtime, and
//! `Handle::block_on` from inside a runtime panics too; a dedicated thread has
//! neither problem.
//!
//! This is a bridge, not a destination: each synchronous consumer should
//! become async, at which point it calls the port directly.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc as std_mpsc;
use std::thread;

use tokio::sync::mpsc;

use crate::error::{Result, StorageError};

type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

thread_local! {
    static ON_BRIDGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A dedicated runtime thread that runs futures for synchronous callers.
///
/// Many callers may share one `Blocking`: each submitted future runs as its own
/// task, so a slow call does not hold up the others. Dropping the last handle
/// stops the thread once in-flight work finishes.
///
/// ```
/// use tinystoragedrivers_core::{Blocking, MemoryStorage, Precondition, Scope, StorageBackend};
/// use serde_json::json;
///
/// let bridge = Blocking::new()?;
/// let docs = MemoryStorage::new().for_scope(&Scope::local())?.documents().clone();
/// let version = bridge.run(async move {
///     docs.put("settings", "theme", json!({"dark": true}), Precondition::None).await
/// })??;
/// assert_eq!(version.0, 1);
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct Blocking {
    jobs: mpsc::UnboundedSender<Job>,
}

impl fmt::Debug for Blocking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Blocking").finish_non_exhaustive()
    }
}

impl Blocking {
    /// Start the runtime thread.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Backend`](crate::ErrorKind::Backend) when the runtime or
    /// thread cannot be created.
    pub fn new() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(startup_error("cannot build the blocking bridge runtime"))?;
        let (jobs, mut receiver) = mpsc::unbounded_channel::<Job>();
        thread::Builder::new()
            .name("tinystoragedrivers-blocking".into())
            .spawn(move || {
                ON_BRIDGE.with(|flag| flag.set(true));
                runtime.block_on(async move {
                    // Each call is its own task, so a slow one does not hold up
                    // the rest; the runtime drives them while this loop waits.
                    let mut running = tokio::task::JoinSet::new();
                    while let Some(job) = receiver.recv().await {
                        running.spawn(job);
                        while running.try_join_next().is_some() {}
                    }
                    // Every handle is gone; let in-flight calls finish rather
                    // than cancelling them with the runtime.
                    while running.join_next().await.is_some() {}
                });
            })
            .map_err(startup_error("cannot start the blocking bridge thread"))?;
        Ok(Self { jobs })
    }

    /// Run `future` on the bridge thread and wait for its output.
    ///
    /// The calling thread blocks until the future completes. The future runs
    /// on the bridge's own runtime, so it must not wait on work that only the
    /// caller's thread can drive: from inside a current-thread runtime, a
    /// future that awaits a task spawned on *that* runtime never completes.
    /// Port futures from any driver in this repository are self-contained and
    /// safe to run here.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Backend`](crate::ErrorKind::Backend) when called from the
    /// bridge thread itself (which would deadlock), when the bridge has
    /// stopped, or when the future panicked.
    pub fn run<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        if ON_BRIDGE.with(std::cell::Cell::get) {
            return Err(StorageError::backend(
                "a future running on the blocking bridge called back into it",
            ));
        }
        let (reply, wait) = std_mpsc::sync_channel(1);
        let job: Job = Box::pin(async move {
            // The caller may have given up waiting; nothing to do then.
            let _ = reply.send(future.await);
        });
        self.jobs
            .send(job)
            .map_err(|_| StorageError::backend("the blocking bridge has stopped"))?;
        wait.recv().map_err(|_| {
            StorageError::backend("the blocking bridge dropped the call (did it panic?)")
        })
    }
}

/// Map an I/O failure while starting the bridge to a backend error.
fn startup_error(what: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::backend(what).with_source(error)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
