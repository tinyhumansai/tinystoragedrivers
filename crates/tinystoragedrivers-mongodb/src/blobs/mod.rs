//! [`BlobStore`] on GridFS.
//!
//! Blobs live in the GridFS bucket `<prefix>_tsd_blobs`. A file's `filename`
//! is the blob key and its `metadata` is `{_scope, content_type}`; every
//! lookup filters on `metadata._scope` through [`scoped_filter`].
//!
//! A replacement uploads the new file first and then deletes every older file
//! with the same key (by `_id`, which orders uploads). Reads pick the newest,
//! so a reader never sees a missing blob mid-replacement, and two concurrent
//! writers converge on the one with the greater id.
//!
//! Ranged reads fetch only the chunks the range touches, straight from the
//! bucket's `chunks` collection, by the file id resolved under the scope.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use mongodb::IndexModel;
use mongodb::bson::{Bson, Document, doc};
use mongodb::gridfs::{FilesCollectionDocument, GridFsBucket};
use mongodb::options::{FindOptions, GridFsBucketOptions, IndexOptions};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, Result, Scope, StorageError, clamp_range, validate_blob_key,
};

use crate::backend::Shared;
use crate::errors;
use crate::naming::{BLOB_KEY_INDEX, BLOBS, SCOPE, prefix_regex};
use crate::scoped::scoped_filter;

/// Where a file's scope lives.
const FILE_SCOPE: &str = "metadata._scope";

/// The metadata stored with a file.
pub(crate) fn file_metadata(scope: &Scope, content_type: Option<&str>) -> Document {
    let mut metadata = doc! {SCOPE: scope.as_str()};
    if let Some(content_type) = content_type {
        metadata.insert("content_type", content_type);
    }
    metadata
}

/// A file's [`BlobMeta`].
pub(crate) fn blob_meta(file: &FilesCollectionDocument) -> BlobMeta {
    BlobMeta {
        key: file.filename.clone().unwrap_or_default(),
        len: file.length,
        content_type: file
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get_str("content_type").ok())
            .map(str::to_owned),
    }
}

/// The chunk numbers `range` touches in chunks of `chunk_size` bytes, and
/// where the range starts inside the first of them.
pub(crate) fn chunk_span(range: &Range<usize>, chunk_size: usize) -> (usize, usize, usize) {
    let size = chunk_size.max(1);
    let first = range.start / size;
    let last = range.end.saturating_sub(1) / size;
    (first, last, range.start - first * size)
}

/// MongoDB blobs bound to one scope.
#[derive(Debug)]
pub(crate) struct MongoBlobs {
    shared: Arc<Shared>,
    scope: Scope,
}

impl MongoBlobs {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope) -> Self {
        Self { shared, scope }
    }

    async fn bucket(&self) -> Result<GridFsBucket> {
        let name = format!("{}{BLOBS}", self.shared.prefix);
        let model = IndexModel::builder()
            .keys(doc! {FILE_SCOPE: 1, "filename": 1, "_id": -1})
            .options(
                IndexOptions::builder()
                    .name(BLOB_KEY_INDEX.to_owned())
                    .build(),
            )
            .build();
        self.shared
            .prepare(&format!("{BLOBS}.files"), vec![model])
            .await?;
        Ok(self
            .shared
            .db
            .gridfs_bucket(GridFsBucketOptions::builder().bucket_name(name).build()))
    }

    /// Files of this scope matching `inner`, newest first within a key.
    async fn files(
        &self,
        bucket: &GridFsBucket,
        inner: Document,
    ) -> Result<Vec<FilesCollectionDocument>> {
        bucket
            .find(scoped_filter(FILE_SCOPE, &self.scope, inner))
            .sort(doc! {"filename": 1, "_id": -1})
            .await
            .map_err(errors::failed("list blobs"))?
            .try_collect()
            .await
            .map_err(errors::failed("list blobs"))
    }

    async fn newest(
        &self,
        bucket: &GridFsBucket,
        key: &str,
    ) -> Result<Option<FilesCollectionDocument>> {
        Ok(self
            .files(bucket, doc! {"filename": key})
            .await?
            .into_iter()
            .next())
    }

    async fn remove(&self, bucket: &GridFsBucket, ids: Vec<Bson>) -> Result<()> {
        for id in ids {
            match bucket.delete(id).await {
                Ok(()) => {}
                // A concurrent delete got there first.
                Err(error) if matches!(*error.kind, mongodb::error::ErrorKind::GridFs(_)) => {}
                Err(error) => return Err(errors::map(error, "delete a blob")),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for MongoBlobs {
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        validate_blob_key(key)?;
        let bucket = self.bucket().await?;
        let mut upload = bucket
            .open_upload_stream(key)
            .metadata(file_metadata(&self.scope, content_type))
            .await
            .map_err(errors::failed("store a blob"))?;
        upload
            .write_all(&bytes)
            .await
            .map_err(errors::failed_io("store a blob"))?;
        upload
            .close()
            .await
            .map_err(errors::failed_io("store a blob"))?;
        let id = upload.id().clone();
        let older: Vec<Bson> = self
            .files(&bucket, doc! {"filename": key, "_id": {"$lt": id.clone()}})
            .await?
            .into_iter()
            .map(|file| file.id)
            .collect();
        self.remove(&bucket, older).await?;
        Ok(BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_blob_key(key)?;
        let bucket = self.bucket().await?;
        let Some(file) = self.newest(&bucket, key).await? else {
            return Ok(None);
        };
        let mut download = bucket
            .open_download_stream(file.id.clone())
            .await
            .map_err(errors::failed("read a blob"))?;
        let mut bytes = Vec::new();
        download
            .read_to_end(&mut bytes)
            .await
            .map_err(errors::failed_io("read a blob"))?;
        Ok(Some(Blob {
            meta: blob_meta(&file),
            bytes,
        }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_blob_key(key)?;
        let bucket = self.bucket().await?;
        let Some(file) = self.newest(&bucket, key).await? else {
            return Ok(None);
        };
        let len = usize::try_from(file.length)
            .map_err(|_| StorageError::backend("blob is larger than this platform can address"))?;
        let range = clamp_range(&range, len)?;
        if range.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let chunk_size = usize::try_from(file.chunk_size_bytes).unwrap_or(usize::MAX);
        let (first, last, skip) = chunk_span(&range, chunk_size);
        let chunks = self
            .shared
            .raw(&format!("{BLOBS}.chunks"))
            .find(doc! {
                "files_id": file.id.clone(),
                "n": {"$gte": i64::try_from(first).unwrap_or(i64::MAX), "$lte": i64::try_from(last).unwrap_or(i64::MAX)},
            })
            .with_options(FindOptions::builder().sort(doc! {"n": 1}).build())
            .await
            .map_err(errors::failed("read a blob range"))?
            .try_collect::<Vec<Document>>()
            .await
            .map_err(errors::failed("read a blob range"))?;
        let mut bytes = Vec::new();
        for chunk in &chunks {
            let data = chunk
                .get_binary_generic("data")
                .map_err(|_| StorageError::serialization("malformed blob chunk"))?;
            bytes.extend_from_slice(data);
        }
        let wanted = range.len();
        Ok(Some(bytes.into_iter().skip(skip).take(wanted).collect()))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_blob_key(key)?;
        let bucket = self.bucket().await?;
        Ok(self.newest(&bucket, key).await?.as_ref().map(blob_meta))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_blob_key(key)?;
        let bucket = self.bucket().await?;
        let ids: Vec<Bson> = self
            .files(&bucket, doc! {"filename": key})
            .await?
            .into_iter()
            .map(|file| file.id)
            .collect();
        let existed = !ids.is_empty();
        self.remove(&bucket, ids).await?;
        Ok(existed)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        let bucket = self.bucket().await?;
        let files = self
            .files(&bucket, doc! {"filename": prefix_regex(prefix)})
            .await?;
        let mut out: Vec<BlobMeta> = Vec::new();
        for file in &files {
            let meta = blob_meta(file);
            let fresh = out.last().is_none_or(|last| last.key != meta.key);
            if fresh && meta.key.starts_with(prefix) {
                out.push(meta);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
