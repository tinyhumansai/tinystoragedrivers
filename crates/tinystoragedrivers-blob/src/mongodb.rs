//! MongoDB GridFS adapter for scoped opaque blobs.

use std::{collections::BTreeMap, io, ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::{
    TryStreamExt,
    io::{AsyncReadExt, AsyncWriteExt},
    lock::Mutex as AsyncMutex,
};
use mongodb::{
    IndexModel,
    bson::{Bson, Document, doc},
    gridfs::{FilesCollectionDocument, GridFsBucket},
    options::{FindOptions, GridFsBucketOptions, IndexOptions},
};
use tinystoragedrivers_api::{BlobReader, BlobStore};
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_types::{
    Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability,
    MAX_BLOB_LIST_PAGE_ITEMS, Result, Scope, StorageError, is_reserved_blob_key,
};

use crate::key::{MAX_KEY_LEN, clamp_range, validate_key, validate_prefix, validate_sealed_key};

const BUCKET_NAME: &str = "_tsd_blobs";
const BLOB_KEY_INDEX: &str = "_tsd_blob_key";
const SCOPE_FIELD: &str = "metadata._scope";
const READ_ATTEMPTS: usize = 8;

/// A scope-bound GridFS blob adapter over the shared Mongo connection.
#[derive(Debug, Clone)]
pub struct MongoBlobStore {
    connection: MongoConnection,
    scope: Scope,
    indexes_ready: Arc<AsyncMutex<bool>>,
    #[cfg(test)]
    sweep_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

impl MongoBlobStore {
    /// Bind a shared MongoDB connection to a scope.
    #[must_use]
    pub fn new(connection: MongoConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope,
            indexes_ready: Arc::new(AsyncMutex::new(false)),
            #[cfg(test)]
            sweep_pause: None,
        }
    }

    /// Pause `sweep_expired` between its two passes.
    #[cfg(test)]
    pub(crate) fn pause_sweep_for_test(
        mut self,
        started: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.sweep_pause = Some((started, resume));
        self
    }

    async fn bucket(&self) -> Result<GridFsBucket> {
        let mut indexes_ready = self.indexes_ready.lock().await;
        if !*indexes_ready {
            let database = self.connection.database();
            let files = database.collection::<Document>(&format!("{BUCKET_NAME}.files"));
            let chunks = database.collection::<Document>(&format!("{BUCKET_NAME}.chunks"));
            files
                .create_index(
                    IndexModel::builder()
                        .keys(doc! {SCOPE_FIELD: 1, "filename": 1, "_id": -1})
                        .options(
                            IndexOptions::builder()
                                .name(BLOB_KEY_INDEX.to_owned())
                                .build(),
                        )
                        .build(),
                )
                .await
                .map_err(mongo_error("create blob file index"))?;
            files
                .create_index(
                    IndexModel::builder()
                        .keys(doc! {"filename": 1, "uploadDate": 1})
                        .build(),
                )
                .await
                .map_err(mongo_error("create GridFS file index"))?;
            chunks
                .create_index(
                    IndexModel::builder()
                        .keys(doc! {"files_id": 1, "n": 1})
                        .options(IndexOptions::builder().unique(true).build())
                        .build(),
                )
                .await
                .map_err(mongo_error("create GridFS chunk index"))?;
            *indexes_ready = true;
        }
        Ok(self.connection.database().gridfs_bucket(
            GridFsBucketOptions::builder()
                .bucket_name(BUCKET_NAME.to_owned())
                .build(),
        ))
    }

    async fn files(&self, filter: Document) -> Result<Vec<FilesCollectionDocument>> {
        self.bucket()
            .await?
            .find(scoped_filter(&self.scope, filter))
            .sort(doc! {"filename": 1, "_id": -1})
            .await
            .map_err(mongo_error("find blob files"))?
            .try_collect()
            .await
            .map_err(mongo_error("read blob files"))
    }

    async fn newest(&self, key: &str) -> Result<Option<FilesCollectionDocument>> {
        let files = self.files(doc! {"filename": key}).await?;
        Ok(files.into_iter().next())
    }

    fn metadata(
        &self,
        content_type: Option<&str>,
        expiry: Option<u64>,
        written_at: u64,
    ) -> Document {
        let mut metadata =
            doc! {"_scope": self.scope.as_str(), "written_at_ms": to_i64(written_at)};
        if let Some(content_type) = content_type {
            metadata.insert("content_type", content_type);
        }
        if let Some(expiry) = expiry {
            metadata.insert("expires_at_ms", to_i64(expiry));
        }
        metadata
    }

    fn is_expired(file: &FilesCollectionDocument, now: u64) -> bool {
        file.metadata
            .as_ref()
            .and_then(|metadata| metadata.get_i64("expires_at_ms").ok())
            .is_some_and(|expiry| expiry <= to_i64(now))
    }

    fn meta(file: &FilesCollectionDocument) -> BlobMeta {
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

    fn age(file: &FilesCollectionDocument) -> Option<u64> {
        u64::try_from(file.upload_date.timestamp_millis()).ok()
    }

    async fn get_key(&self, key: &str, allow_sealed: bool) -> Result<Option<Blob>> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let bucket = self.bucket().await?;
        for _ in 0..READ_ATTEMPTS {
            let Some(file) = self.newest(key).await? else {
                return Ok(None);
            };
            if Self::is_expired(&file, (self.connection.clock())()) {
                return Ok(None);
            }
            if let Some(bytes) = Self::download(&bucket, &file).await? {
                return Ok(Some(Blob {
                    meta: Self::meta(&file),
                    bytes,
                }));
            }
        }
        Err(replaced_too_often())
    }

    async fn head_key(&self, key: &str, allow_sealed: bool) -> Result<Option<BlobMeta>> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let Some(file) = self.newest(key).await? else {
            return Ok(None);
        };
        if Self::is_expired(&file, (self.connection.clock())()) {
            return Ok(None);
        }
        Ok(Some(Self::meta(&file)))
    }

    async fn delete_key(&self, key: &str, allow_sealed: bool) -> Result<bool> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let files = self.files(doc! {"filename": key}).await?;
        let existed = files
            .first()
            .is_some_and(|file| !Self::is_expired(file, (self.connection.clock())()));
        for file in files {
            self.delete_id(file.id).await?;
        }
        Ok(existed)
    }

    async fn upload(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Option<Duration>,
        allow_sealed: bool,
    ) -> Result<BlobMeta> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let now = (self.connection.clock())();
        let expiry =
            ttl.map(|ttl| now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)));
        let bucket = self.bucket().await?;
        let mut upload = bucket
            .open_upload_stream(key)
            .metadata(self.metadata(content_type, expiry, now))
            .await
            .map_err(mongo_error("begin blob upload"))?;
        let transfer: Result<()> = async {
            let mut total = 0_u64;
            while let Some(chunk) = reader.try_next().await? {
                total = total.saturating_add(chunk.len() as u64);
                if total > len {
                    return Err(StorageError::invalid_input(
                        "blob stream exceeds declared length",
                    ));
                }
                upload
                    .write_all(&chunk)
                    .await
                    .map_err(|error| io_error("write GridFS chunk", error))?;
            }
            if total != len {
                return Err(StorageError::invalid_input(
                    "blob stream is shorter than declared length",
                ));
            }
            Ok(())
        }
        .await;
        if let Err(error) = transfer {
            let _ = upload.abort().await;
            return Err(error);
        }
        upload
            .close()
            .await
            .map_err(|error| io_error("finish GridFS upload", error))?;
        let keep_id = upload.id().clone();
        let old = self
            .files(doc! {"filename": key, "_id": {"$lt": keep_id}})
            .await?
            .into_iter()
            .map(|file| file.id)
            .collect::<Vec<_>>();
        for id in old {
            self.delete_id(id).await?;
        }
        Ok(BlobMeta {
            key: key.to_owned(),
            len,
            content_type: content_type.map(str::to_owned),
        })
    }

    async fn delete_id(&self, id: Bson) -> Result<()> {
        match self.bucket().await?.delete(id).await {
            Ok(()) => Ok(()),
            Err(error) if is_gridfs_error(&error) => Ok(()),
            Err(error) => Err(mongo_error("delete GridFS file")(error)),
        }
    }

    async fn download(
        bucket: &GridFsBucket,
        file: &FilesCollectionDocument,
    ) -> Result<Option<Vec<u8>>> {
        let mut stream = match bucket.open_download_stream(file.id.clone()).await {
            Ok(stream) => stream,
            Err(error) if is_gridfs_error(&error) => return Ok(None),
            Err(error) => return Err(mongo_error("open GridFS download")(error)),
        };
        let mut bytes = Vec::new();
        match stream.read_to_end(&mut bytes).await {
            Ok(_) => Ok(Some(bytes)),
            Err(error) if is_gridfs_io_error(&error) => Ok(None),
            Err(error) => Err(io_error("read GridFS download", error)),
        }
    }

    async fn read_chunks(
        &self,
        file: &FilesCollectionDocument,
        range: &Range<usize>,
    ) -> Result<Option<Vec<u8>>> {
        let chunk_size = usize::try_from(file.chunk_size_bytes)
            .unwrap_or(usize::MAX)
            .max(1);
        let first = range.start / chunk_size;
        let last = range.end.saturating_sub(1) / chunk_size;
        let skip = range.start - first * chunk_size;
        let chunks = self
            .connection
            .database()
            .collection::<Document>(&format!("{BUCKET_NAME}.chunks"))
            .find(doc! {
                "files_id": file.id.clone(),
                "n": {
                    "$gte": i64::try_from(first).unwrap_or(i64::MAX),
                    "$lte": i64::try_from(last).unwrap_or(i64::MAX),
                },
            })
            .with_options(FindOptions::builder().sort(doc! {"n": 1}).build())
            .await
            .map_err(mongo_error("read blob range chunks"))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(mongo_error("read blob range chunks"))?;
        let mut bytes = Vec::new();
        for chunk in &chunks {
            let data = chunk
                .get_binary_generic("data")
                .map_err(|_| StorageError::serialization("malformed GridFS blob chunk"))?;
            bytes.extend_from_slice(data);
        }
        if bytes.len() < skip.saturating_add(range.len()) {
            return Ok(None);
        }
        Ok(Some(bytes[skip..skip + range.len()].to_vec()))
    }
}

#[async_trait]
impl BlobStore for MongoBlobStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Blobs)
            .with(Capability::BlobTtl)
            .with(Capability::BlobListing)
    }
    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        let reader: BlobReader = Box::pin(futures_util::stream::once(async move {
            Ok(bytes::Bytes::from(bytes))
        }));
        self.upload(key, reader, len, content_type, None, false)
            .await
    }
    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.upload(key, reader, len, content_type, None, false)
            .await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        let reader: BlobReader = Box::pin(futures_util::stream::once(async move {
            Ok(bytes::Bytes::from(bytes))
        }));
        self.upload(key, reader, len, content_type, Some(ttl), false)
            .await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.upload(key, reader, len, content_type, Some(ttl), false)
            .await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.get_key(key, false).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;
        clamp_range(0, range.start, range.end)?;
        let _bucket = self.bucket().await?;
        for _ in 0..READ_ATTEMPTS {
            let Some(file) = self.newest(key).await? else {
                return Ok(None);
            };
            if Self::is_expired(&file, (self.connection.clock())()) {
                return Ok(None);
            }
            let len = usize::try_from(file.length).map_err(|_| {
                StorageError::backend("blob is larger than this platform can address")
            })?;
            let range = clamp_range(len, range.start, range.end)?;
            if range.is_empty() {
                return Ok(Some(Vec::new()));
            }
            if let Some(bytes) = self.read_chunks(&file, &range).await? {
                return Ok(Some(bytes));
            }
        }
        Err(replaced_too_often())
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.head_key(key, false).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        self.delete_key(key, false).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        let mut output = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .list_page(prefix, cursor.as_deref(), MAX_BLOB_LIST_PAGE_ITEMS)
                .await?;
            for entry in page.items {
                if let Some(meta) = self.head(&entry.meta.key).await? {
                    output.push(meta);
                }
            }
            cursor = page.next_after;
            if cursor.is_none() {
                break;
            }
        }
        Ok(output)
    }

    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        validate_sealed_key(key)?;
        let len = bytes.len() as u64;
        let reader: BlobReader = Box::pin(futures_util::stream::once(async move {
            Ok(bytes::Bytes::from(bytes))
        }));
        self.upload(key, reader, len, content_type, None, true)
            .await
    }

    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        self.get_key(key, true).await
    }

    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.head_key(key, true).await
    }

    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        self.delete_key(key, true).await
    }

    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_sealed_key(prefix)?;
        let regex = format!("^{}", regex_escape(prefix));
        let files = self.files(doc! {"filename": {"$regex": regex}}).await?;
        let mut newest = BTreeMap::new();
        for file in files {
            let key = file.filename.clone().unwrap_or_default();
            if is_reserved_blob_key(&key)
                && key.starts_with(prefix)
                && !Self::is_expired(&file, (self.connection.clock())())
            {
                newest.entry(key).or_insert(file);
            }
        }
        Ok(newest.into_values().map(|file| Self::meta(&file)).collect())
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        validate_prefix(prefix, after)?;
        let regex = format!("^{}", regex_escape(prefix));
        let filter = if let Some(after) = after {
            doc! {"filename": {"$regex": regex, "$gt": after}}
        } else {
            doc! {"filename": {"$regex": regex}}
        };
        let files = self.files(filter).await?;
        let mut newest = BTreeMap::new();
        for file in files {
            let key = file.filename.clone().unwrap_or_default();
            if key.starts_with(prefix)
                && !is_reserved_blob_key(&key)
                && after.is_none_or(|cursor| key.as_bytes() > cursor.as_bytes())
            {
                newest.entry(key).or_insert(file);
            }
        }
        let more = newest.len() > limit;
        let items = newest
            .into_values()
            .take(limit)
            .map(|file| BlobListEntry {
                meta: Self::meta(&file),
                written_at_ms: file
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get_i64("written_at_ms").ok())
                    .and_then(|age| u64::try_from(age).ok())
                    .or_else(|| Self::age(&file)),
            })
            .collect::<Vec<_>>();
        let next_after = if more {
            items.last().map(|entry| entry.meta.key.clone())
        } else {
            None
        };
        Ok(BlobListPage { items, next_after })
    }
    async fn sweep_expired(&self) -> Result<u64> {
        let expired = self
            .files(doc! {"metadata.expires_at_ms": {"$lte": to_i64((self.connection.clock())())}})
            .await?;
        // The newest expired version of each key. `files` sorts `_id`
        // descending within a filename, so the first one seen is it.
        let mut newest_expired = BTreeMap::new();
        for file in expired {
            if let Some(key) = file.filename {
                newest_expired.entry(key).or_insert(file.id);
            }
        }
        #[cfg(test)]
        if let Some((started, resume)) = &self.sweep_pause {
            started.notify_one();
            resume.notified().await;
        }
        let mut count = 0_u64;
        for (key, bound) in newest_expired {
            // Only versions up to that one: a version written since this sweep
            // began is newer and stays. Older versions go first, so a failure
            // partway never leaves a superseded version as the newest.
            let versions = self
                .files(doc! {"filename": &key, "_id": {"$lte": bound}})
                .await?;
            for file in versions.into_iter().rev() {
                self.delete_id(file.id).await?;
                count += 1;
            }
        }
        Ok(count)
    }
}

fn scoped_filter(scope: &Scope, inner: Document) -> Document {
    let mut scoped = doc! {SCOPE_FIELD: scope.as_str()};
    for (key, value) in inner {
        scoped.insert(key, value);
    }
    scoped
}

fn regex_escape(prefix: &str) -> String {
    let mut escaped = String::with_capacity(prefix.len());
    for character in prefix.chars() {
        if matches!(
            character,
            '\\' | '.'
                | '+'
                | '*'
                | '?'
                | '['
                | '^'
                | ']'
                | '$'
                | '('
                | ')'
                | '{'
                | '}'
                | '='
                | '!'
                | '<'
                | '>'
                | '|'
                | ':'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn replaced_too_often() -> StorageError {
    StorageError::unavailable("blob kept being replaced while it was read; retry")
}

fn is_gridfs_error(error: &mongodb::error::Error) -> bool {
    matches!(error.kind.as_ref(), mongodb::error::ErrorKind::GridFs(_))
}

fn is_gridfs_io_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<mongodb::error::Error>())
        .is_some_and(is_gridfs_error)
}

fn mongo_error(operation: &'static str) -> impl FnOnce(mongodb::error::Error) -> StorageError {
    move |_| StorageError::unavailable(format!("MongoDB failed to {operation}"))
}

fn io_error(operation: &'static str, error: io::Error) -> StorageError {
    StorageError::backend(format!("{operation} failed")).with_source(error)
}

#[cfg(test)]
#[path = "mongodb_tests.rs"]
mod tests;
