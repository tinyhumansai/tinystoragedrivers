//! [`StreamStore`] for the in-memory driver.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::state::DbState;
use super::{Clock, MemoryDb};
use crate::error::Result;
use crate::fence::Fence;
use crate::scope::Scope;
use crate::stream::{StreamEntry, StreamStore, validate_stream};

/// In-memory streams bound to one scope.
#[derive(Clone)]
pub struct MemoryStreams {
    db: Arc<MemoryDb>,
    scope: Scope,
    clock: Clock,
    /// Checked under the lock before every write, when set.
    fence: Option<Arc<Fence>>,
}

impl std::fmt::Debug for MemoryStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStreams")
            .field("scope", &self.scope)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl MemoryStreams {
    pub(super) fn new(
        db: Arc<MemoryDb>,
        scope: Scope,
        clock: Clock,
        fence: Option<Arc<Fence>>,
    ) -> Self {
        Self {
            db,
            scope,
            clock,
            fence,
        }
    }

    /// Refuse a write unless this handle's fence (if any) holds.
    fn guard(&self, state: &DbState) -> Result<()> {
        state.guard(self.fence.as_deref(), (self.clock)())
    }

    fn key(&self, stream: &str) -> (String, String) {
        (self.scope.as_str().to_owned(), stream.to_owned())
    }
}

#[async_trait]
impl StreamStore for MemoryStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        if values.is_empty() {
            return Ok(state
                .streams
                .get(&self.key(stream))
                .map_or(0, super::state::StoredStream::len));
        }
        let entry = state.streams.entry(self.key(stream)).or_default();
        let first = entry.len();
        entry.entries.extend(values);
        Ok(first)
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let state = self.db.lock()?;
        let Some(entry) = state.streams.get(&self.key(stream)) else {
            return Ok(Vec::new());
        };
        let start = from.max(entry.base);
        Ok((start..entry.len())
            .take(limit)
            .filter_map(|offset| {
                let index = usize::try_from(offset - entry.base).ok()?;
                entry.entries.get(index).map(|value| StreamEntry {
                    offset,
                    value: value.clone(),
                })
            })
            .collect())
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let state = self.db.lock()?;
        Ok(state
            .streams
            .get(&self.key(stream))
            .map_or(0, super::state::StoredStream::len))
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        let Some(entry) = state.streams.get_mut(&self.key(stream)) else {
            return Ok(0);
        };
        let cut = offset.clamp(entry.base, entry.len());
        let removed = cut - entry.base;
        for _ in 0..removed {
            entry.entries.pop_front();
        }
        entry.base = cut;
        Ok(removed)
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        Ok(state.streams.remove(&self.key(stream)).is_some())
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let state = self.db.lock()?;
        Ok(state
            .streams
            .keys()
            .filter(|(scope, name)| scope == self.scope.as_str() && name.starts_with(prefix))
            .map(|(_, name)| name.clone())
            .collect())
    }
}
