//! [`BlobStore`] for the file driver: raw bytes plus a JSON sidecar.
//!
//! `<key>` holds the bytes exactly as given, so a blob opens in any tool, and
//! `<key>.meta.json` holds `{"key", "content_type"}`: the real key (file names
//! may be hashed) and the content type. The length is the data file's size.
//!
//! A put writes the bytes, then the sidecar, each atomically; a delete removes
//! the sidecar first. The sidecar decides existence, so a crash mid-put leaves
//! either the old blob, or (when replacing) the new bytes under the old
//! content type, never a blob without bytes.

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, Fence, Result, Scope, StorageError, clamp_range, validate_blob_key,
};

use crate::documents::guard;
use crate::encode::file_stem;
use crate::fsio::{
    files_with_suffix, io_error, open_read, read_json, read_optional, remove_optional,
    write_atomic, write_json,
};
use crate::storage::Db;

const META_SUFFIX: &str = ".meta.json";

/// A blob's sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Sidecar {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

/// File-backed blobs bound to one scope.
#[derive(Debug, Clone)]
pub struct FileBlobs {
    db: Arc<Db>,
    scope: Scope,
    /// Checked under the database lock before every write, when set.
    fence: Option<Arc<Fence>>,
}

/// The paths of one blob.
struct Paths {
    data: PathBuf,
    meta: PathBuf,
}

impl FileBlobs {
    pub(crate) fn new(db: Arc<Db>, scope: Scope, fence: Option<Arc<Fence>>) -> Self {
        Self { db, scope, fence }
    }

    fn dir(db: &Db, scope: &Scope) -> PathBuf {
        db.scope_dir(scope).join("blobs")
    }

    /// Run `work` under the database lock with this key's paths.
    async fn with<T, F>(&self, key: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        self.run(key, None, work).await
    }

    /// [`Self::with`] for a write: the fence (if any) is checked first,
    /// under the same lock.
    async fn write<T, F>(&self, key: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        self.run(key, self.fence.clone(), work).await
    }

    async fn run<T, F>(&self, key: &str, fence: Option<Arc<Fence>>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        validate_blob_key(key)?;
        let (scope, key) = (self.scope.clone(), key.to_owned());
        self.db
            .run(move |db| {
                guard(db, fence.as_deref())?;
                let dir = Self::dir(db, &scope);
                let stem = file_stem(&key);
                let paths = Paths {
                    data: dir.join(&stem),
                    meta: dir.join(format!("{stem}{META_SUFFIX}")),
                };
                work(&paths, &key)
            })
            .await
    }
}

/// The blob's metadata, `None` when it does not exist.
fn head(paths: &Paths, key: &str) -> Result<Option<BlobMeta>> {
    match read_json::<Sidecar>(&paths.meta)? {
        Some(sidecar) if sidecar.key == key => meta(&paths.data, sidecar),
        Some(_) => Err(collision()),
        None => Ok(None),
    }
}

/// Combine a sidecar with its data file's size; `None` when the data file is
/// missing.
fn meta(data: &Path, sidecar: Sidecar) -> Result<Option<BlobMeta>> {
    // `symlink_metadata` does not follow a link, so a planted symlink or
    // directory is refused here exactly as `get` refuses it.
    match std::fs::symlink_metadata(data).and_then(|stat| {
        if stat.file_type().is_file() {
            Ok(stat)
        } else {
            Err(std::io::Error::other("not a regular file"))
        }
    }) {
        Ok(stat) => Ok(Some(BlobMeta {
            key: sidecar.key,
            len: stat.len(),
            content_type: sidecar.content_type,
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read a blob's size")(error)),
    }
}

fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to touch another blob")
}

#[async_trait]
impl BlobStore for FileBlobs {
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        let content_type = content_type.map(str::to_owned);
        self.write(key, move |paths, key| {
            if let Some(existing) = read_json::<Sidecar>(&paths.meta)?
                && existing.key != key
            {
                return Err(collision());
            }
            write_atomic(&paths.data, &bytes)?;
            let sidecar = Sidecar {
                key: key.to_owned(),
                content_type,
            };
            write_json(&paths.meta, &sidecar)?;
            Ok(BlobMeta {
                key: sidecar.key,
                len: bytes.len() as u64,
                content_type: sidecar.content_type,
            })
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.with(key, |paths, key| {
            let Some(meta) = head(paths, key)? else {
                return Ok(None);
            };
            Ok(read_optional(&paths.data)?.map(|bytes| Blob {
                meta: BlobMeta {
                    len: bytes.len() as u64,
                    ..meta
                },
                bytes,
            }))
        })
        .await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.with(key, move |paths, key| {
            let Some(meta) = head(paths, key)? else {
                return Ok(None);
            };
            let len = usize::try_from(meta.len).unwrap_or(usize::MAX);
            let range = clamp_range(&range, len)?;
            let read_error = io_error("read a blob");
            let mut file = open_read(&paths.data).map_err(&read_error)?;
            file.seek(SeekFrom::Start(range.start as u64))
                .map_err(&read_error)?;
            let mut bytes = Vec::with_capacity(range.len());
            file.take(range.len() as u64)
                .read_to_end(&mut bytes)
                .map_err(&read_error)?;
            Ok(Some(bytes))
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.with(key, head).await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.write(key, |paths, key| {
            if head(paths, key)?.is_none() {
                return Ok(false);
            }
            remove_optional(&paths.meta)?;
            remove_optional(&paths.data)?;
            Ok(true)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        let (scope, prefix) = (self.scope.clone(), prefix.to_owned());
        self.db
            .run(move |db| {
                let dir = Self::dir(db, &scope);
                let mut out = Vec::new();
                for path in files_with_suffix(&dir, META_SUFFIX)? {
                    let Some(sidecar) = read_json::<Sidecar>(&path)? else {
                        continue;
                    };
                    if !sidecar.key.starts_with(&prefix) {
                        continue;
                    }
                    let data = dir.join(file_stem(&sidecar.key));
                    if let Some(meta) = meta(&data, sidecar)? {
                        out.push(meta);
                    }
                }
                out.sort_by(|a, b| a.key.cmp(&b.key));
                Ok(out)
            })
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
