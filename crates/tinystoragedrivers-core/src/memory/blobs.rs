//! [`BlobStore`] for the in-memory driver.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;

use super::state::DbState;
use super::{Clock, MemoryDb};
use crate::blob::{Blob, BlobMeta, BlobStore, clamp_range, validate_blob_key};
use crate::error::Result;
use crate::fence::Fence;
use crate::scope::Scope;

/// In-memory blobs bound to one scope.
#[derive(Clone)]
pub struct MemoryBlobs {
    db: Arc<MemoryDb>,
    scope: Scope,
    clock: Clock,
    /// Checked under the lock before every write, when set.
    fence: Option<Arc<Fence>>,
}

impl std::fmt::Debug for MemoryBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBlobs")
            .field("scope", &self.scope)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl MemoryBlobs {
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

    /// Refuse a write unless this handle's fence (if any) holds. An
    /// unfenced handle never reads the clock.
    fn guard(&self, state: &DbState) -> Result<()> {
        match self.fence.as_deref() {
            None => Ok(()),
            fence => state.guard(fence, (self.clock)()),
        }
    }

    fn key(&self, key: &str) -> (String, String) {
        (self.scope.as_str().to_owned(), key.to_owned())
    }
}

#[async_trait]
impl BlobStore for MemoryBlobs {
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        validate_blob_key(key)?;
        let meta = BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        };
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        state.blobs.insert(self.key(key), (meta.clone(), bytes));
        Ok(meta)
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_blob_key(key)?;
        let state = self.db.lock()?;
        Ok(state.blobs.get(&self.key(key)).map(|(meta, bytes)| Blob {
            meta: meta.clone(),
            bytes: bytes.clone(),
        }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_blob_key(key)?;
        let state = self.db.lock()?;
        let Some((_, bytes)) = state.blobs.get(&self.key(key)) else {
            return Ok(None);
        };
        let range = clamp_range(&range, bytes.len())?;
        Ok(Some(
            bytes.get(range).map(<[u8]>::to_vec).unwrap_or_default(),
        ))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_blob_key(key)?;
        let state = self.db.lock()?;
        Ok(state
            .blobs
            .get(&self.key(key))
            .map(|(meta, _)| meta.clone()))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_blob_key(key)?;
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        Ok(state.blobs.remove(&self.key(key)).is_some())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        let state = self.db.lock()?;
        Ok(state
            .blobs
            .iter()
            .filter(|((scope, key), _)| scope == self.scope.as_str() && key.starts_with(prefix))
            .map(|(_, (meta, _))| meta.clone())
            .collect())
    }
}
