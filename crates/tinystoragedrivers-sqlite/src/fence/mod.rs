//! Fenced writes on SQLite.
//!
//! A fenced handle reads its [`Fence`]'s guard document inside the same
//! immediate transaction as the write. An immediate transaction holds the
//! database's write lock from its first statement, so no other writer (in
//! this process or another) can move the guard between the check and the
//! write: a takeover either commits before the transaction begins, and the
//! write is refused, or waits until it has committed.
//!
//! The guard must live in the same database file as the fenced data: in
//! directory mode a named database is its own file, and one transaction
//! cannot span two.

use std::fmt;

use rusqlite::Connection;
use tinystoragedrivers_core::{Fence, Result};

use crate::documents::ops::{self, Ctx};
use crate::sql::Tables;
use crate::storage::Clock;

/// A fence and the clock that decides whether its guard has expired.
pub(crate) struct Fencing {
    fence: Fence,
    clock: Clock,
}

impl fmt::Debug for Fencing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fencing")
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

impl Fencing {
    pub(crate) fn new(fence: Fence, clock: Clock) -> Self {
        Self { fence, clock }
    }

    /// Refuse the write unless the fence holds. Call inside the write's
    /// immediate transaction.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Fenced`](tinystoragedrivers_core::ErrorKind::Fenced), or
    /// a failure reading the guard.
    pub(crate) fn check(&self, conn: &Connection, tables: &Tables) -> Result<()> {
        let ctx = Ctx {
            tables,
            scope: self.fence.scope().as_str(),
            now_ms: (self.clock)(),
        };
        let guard = ops::get(conn, ctx, self.fence.collection(), self.fence.id())?;
        self.fence.check(guard.as_ref())
    }
}

/// [`Fencing::check`] when a handle is fenced; nothing otherwise.
///
/// # Errors
///
/// As [`Fencing::check`].
pub(crate) fn guard(fencing: Option<&Fencing>, conn: &Connection, tables: &Tables) -> Result<()> {
    fencing.map_or(Ok(()), |fencing| fencing.check(conn, tables))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
