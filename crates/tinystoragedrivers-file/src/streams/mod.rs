//! [`StreamStore`] for the file driver: one JSONL file per stream.
//!
//! `<name>.jsonl` holds one `{"offset": N, "value": ...}` line per retained
//! entry, and `<name>.meta.json` holds `{"name", "base"}`: the real stream name
//! (file names may be hashed) and the first retained offset. The stream's
//! length is never stored; it is the larger of `base` and one past the last
//! line's offset, read from the end of the file. So:
//!
//! - an append is one `write` of whole lines plus an fsync; a crash can leave
//!   at most a torn final line, which readers ignore and the next append cuts
//!   off;
//! - [`truncate_before`](StreamStore::truncate_before) first raises `base`,
//!   then rewrites the file without the dropped lines. A crash between the two
//!   leaves lines below `base`, which readers skip.
//!
//! A stream exists once its meta file does: from its first non-empty append
//! until [`delete_stream`](StreamStore::delete_stream), even when truncated
//! empty, like the memory driver's.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    Result, Scope, StorageError, StreamEntry, StreamStore, validate_stream,
};

use crate::encode::file_stem;
use crate::fsio::{
    files_with_suffix, io_error, open_append, read_json, remove_optional, write_atomic, write_json,
};
use crate::storage::Db;

const META_SUFFIX: &str = ".meta.json";

/// A stream's sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamMeta {
    name: String,
    base: u64,
}

/// One line of a stream file.
#[derive(Debug, Serialize, Deserialize)]
struct Line {
    offset: u64,
    value: Value,
}

/// File-backed streams bound to one scope.
#[derive(Debug, Clone)]
pub struct FileStreams {
    db: Arc<Db>,
    scope: Scope,
}

/// The paths of one stream.
struct Paths {
    data: PathBuf,
    meta: PathBuf,
}

impl FileStreams {
    pub(crate) fn new(db: Arc<Db>, scope: Scope) -> Self {
        Self { db, scope }
    }

    fn dir(db: &Db, scope: &Scope) -> PathBuf {
        db.scope_dir(scope).join("streams")
    }

    fn paths(db: &Db, scope: &Scope, name: &str) -> Paths {
        let dir = Self::dir(db, scope);
        let stem = file_stem(name);
        Paths {
            data: dir.join(format!("{stem}.jsonl")),
            meta: dir.join(format!("{stem}{META_SUFFIX}")),
        }
    }

    /// Run `work` under the database lock with this stream's paths.
    async fn with<T, F>(&self, name: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths) -> Result<T> + Send + 'static,
    {
        validate_stream(name)?;
        let (scope, name) = (self.scope.clone(), name.to_owned());
        self.db
            .run(move |db| work(&Self::paths(db, &scope, &name)))
            .await
    }
}

/// The stream's sidecar, `None` when the stream does not exist.
fn read_meta(paths: &Paths, name: &str) -> Result<Option<StreamMeta>> {
    match read_json::<StreamMeta>(&paths.meta)? {
        Some(meta) if meta.name == name => Ok(Some(meta)),
        Some(_) => Err(collision()),
        None => Ok(None),
    }
}

fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to touch another stream")
}

/// Where the file's complete lines end, and the last complete line.
///
/// Reads backwards from the end in chunks, so the cost is the size of the
/// last line, not of the file.
fn tail(file: &mut File) -> Result<(u64, Option<Vec<u8>>)> {
    const CHUNK: usize = 8192;
    let read_error = io_error("read a stream file");
    let size = file.seek(SeekFrom::End(0)).map_err(&read_error)?;
    let mut start = size;
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if let Some(last) = buf.iter().rposition(|byte| *byte == b'\n') {
            let complete_end = start + last as u64 + 1;
            if let Some(previous) = buf[..last].iter().rposition(|byte| *byte == b'\n') {
                return Ok((complete_end, Some(buf[previous + 1..last].to_vec())));
            }
            if start == 0 {
                return Ok((complete_end, Some(buf[..last].to_vec())));
            }
        } else if start == 0 {
            return Ok((0, None));
        }
        let step = usize::try_from(start).map_or(CHUNK, |start| start.min(CHUNK));
        start -= step as u64;
        file.seek(SeekFrom::Start(start)).map_err(&read_error)?;
        let mut chunk = vec![0; step];
        file.read_exact(&mut chunk).map_err(&read_error)?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
    }
}

/// The stream's length given its meta and file: one past the last complete
/// line's offset, or `base` when that is larger (or there are no lines).
fn length(meta: &StreamMeta, last_line: Option<&[u8]>) -> Result<u64> {
    let after_last = match last_line {
        Some(line) => serde_json::from_slice::<Line>(line)?.offset + 1,
        None => 0,
    };
    Ok(meta.base.max(after_last))
}

/// The length of an existing stream.
fn stream_len(paths: &Paths, meta: &StreamMeta) -> Result<u64> {
    match File::open(&paths.data) {
        Ok(mut file) => {
            let (_, last) = tail(&mut file)?;
            length(meta, last.as_deref())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(meta.base),
        Err(error) => Err(io_error("open a stream file")(error)),
    }
}

/// Every complete line of the stream file at or after `from`, at most `limit`.
fn read_lines(paths: &Paths, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
    let file = match File::open(&paths.data) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("open a stream file")(error)),
    };
    let mut reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut line = Vec::new();
    while out.len() < limit {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(io_error("read a stream file"))?;
        // End of file, or a torn final line a crashed append left behind.
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        let parsed: Line = serde_json::from_slice(&line)?;
        if parsed.offset >= from {
            out.push(StreamEntry {
                offset: parsed.offset,
                value: parsed.value,
            });
        }
    }
    Ok(out)
}

#[async_trait]
impl StreamStore for FileStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            let meta = read_meta(paths, &name)?;
            if values.is_empty() {
                return meta.map_or(Ok(0), |meta| stream_len(paths, &meta));
            }
            let meta = if let Some(meta) = meta {
                meta
            } else {
                let meta = StreamMeta { name, base: 0 };
                write_json(&paths.meta, &meta)?;
                meta
            };
            let mut file = open_append(&paths.data)?;
            let (complete_end, last) = tail(&mut file)?;
            let first = length(&meta, last.as_deref())?;
            let write_error = io_error("append to a stream file");
            // Cut off a torn line a crashed append left, so the new lines
            // start on a line boundary.
            file.set_len(complete_end).map_err(&write_error)?;
            let mut bytes = Vec::new();
            for (offset, value) in (first..).zip(values) {
                serde_json::to_writer(&mut bytes, &Line { offset, value })?;
                bytes.push(b'\n');
            }
            file.write_all(&bytes).map_err(&write_error)?;
            file.sync_data().map_err(&write_error)?;
            Ok(first)
        })
        .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            let Some(meta) = read_meta(paths, &name)? else {
                return Ok(Vec::new());
            };
            read_lines(paths, from.max(meta.base), limit)
        })
        .await
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            read_meta(paths, &name)?.map_or(Ok(0), |meta| stream_len(paths, &meta))
        })
        .await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            let Some(mut meta) = read_meta(paths, &name)? else {
                return Ok(0);
            };
            let len = stream_len(paths, &meta)?;
            let cut = offset.clamp(meta.base, len);
            let removed = cut - meta.base;
            if removed == 0 {
                return Ok(0);
            }
            meta.base = cut;
            write_json(&paths.meta, &meta)?;
            let mut kept = Vec::new();
            for entry in read_lines(paths, cut, usize::MAX)? {
                serde_json::to_writer(
                    &mut kept,
                    &Line {
                        offset: entry.offset,
                        value: entry.value,
                    },
                )?;
                kept.push(b'\n');
            }
            write_atomic(&paths.data, &kept)?;
            Ok(removed)
        })
        .await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            if read_meta(paths, &name)?.is_none() {
                return Ok(false);
            }
            // The meta file decides existence; remove it first so a crash
            // leaves at worst an orphaned, invisible data file.
            remove_optional(&paths.meta)?;
            remove_optional(&paths.data)?;
            Ok(true)
        })
        .await
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let (scope, prefix) = (self.scope.clone(), prefix.to_owned());
        self.db
            .run(move |db| {
                let mut names = Vec::new();
                for path in files_with_suffix(&Self::dir(db, &scope), META_SUFFIX)? {
                    if let Some(meta) = read_json::<StreamMeta>(&path)?
                        && meta.name.starts_with(&prefix)
                    {
                        names.push(meta.name);
                    }
                }
                names.sort();
                Ok(names)
            })
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
