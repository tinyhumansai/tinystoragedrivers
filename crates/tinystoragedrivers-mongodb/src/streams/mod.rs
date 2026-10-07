//! [`StreamStore`] on MongoDB, with dense offsets and no transactions.
//!
//! A stream is a run of *segments* in `<prefix>_tsd_streams`, one per append:
//! `{_scope, s, o, n, e, t, vs}` covers offsets `o..e` (`e = o + n`), of which
//! the first `t` were truncated away and `vs` holds the rest.
//!
//! **Density.** An append reads the stream's end `L` (the last segment's `e`)
//! and inserts one segment at `o = L`. The unique `(_scope, s, o)` index lets
//! exactly one appender claim each `L`; the others re-read and retry. Because
//! a segment is one document, it lands whole or not at all, so the segments
//! always tile `0..len` with no gap, even across a crash, and a batch's
//! records are always contiguous. The cost is that one batch must fit in a
//! single 16 MiB BSON document.
//!
//! **Truncation** deletes whole segments below the cut, trims the one the cut
//! falls inside, and never deletes the last segment (it is trimmed to empty
//! instead), so `len` survives. Each step is idempotent and monotonic, so an
//! interrupted truncation is finished by the next one.
//!
//! **Deletion** is coordinated with appends through a *generation*. Each
//! stream has a header in `<prefix>_tsd_stream_heads` holding its generation
//! `g` (0 when absent), every segment records the generation it was appended
//! in, and every read considers only the current generation's segments.
//! `delete_stream` advances the generation first and then removes the older
//! segments. An append that read the old generation before the delete lands
//! in the old generation, which no reader looks at: it is ordered before the
//! delete, never interleaved with the new stream, so the new stream still
//! starts at offset 0 with no gap. The next delete removes such leftovers.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::IndexModel;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{FindOptions, IndexOptions};
use serde_json::Value;
use tinystoragedrivers_core::{
    Result, Scope, StorageError, StreamEntry, StreamStore, validate_stream,
};

use crate::backend::Shared;
use crate::convert::{from_bson, to_bson};
use crate::errors;
use crate::naming::{
    SCOPE, STREAM_END_INDEX, STREAM_HEADS, STREAM_SEGMENT_INDEX, STREAMS, prefix_regex,
};
use crate::scoped::ScopedCollection;

/// How many times an append retries after another appender took its offset.
const APPEND_ATTEMPTS: usize = 128;

/// One stored segment.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Segment {
    pub(crate) id: Bson,
    /// First offset covered.
    pub(crate) start: u64,
    /// One past the last offset covered.
    pub(crate) end: u64,
    /// How many leading offsets were truncated away.
    pub(crate) trimmed: u64,
    /// The retained records, for offsets `start + trimmed..end`.
    pub(crate) values: Vec<Bson>,
}

fn offset(doc: &Document, field: &str) -> Result<u64> {
    doc.get_i64(field)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| StorageError::serialization("malformed stream segment"))
}

fn stored_offset(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| StorageError::backend("stream offset space is exhausted"))
}

impl Segment {
    /// Decode a stored segment.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
    /// for a malformed one.
    pub(crate) fn decode(doc: &Document) -> Result<Self> {
        let segment = Self {
            id: doc.get("_id").cloned().unwrap_or(Bson::Null),
            start: offset(doc, "o")?,
            end: offset(doc, "e")?,
            trimmed: offset(doc, "t")?,
            values: doc
                .get_array("vs")
                .map_err(|_| StorageError::serialization("malformed stream segment"))?
                .clone(),
        };
        let covered = segment
            .start
            .checked_add(segment.trimmed)
            .and_then(|first| first.checked_add(segment.values.len() as u64));
        if covered != Some(segment.end) {
            return Err(StorageError::serialization("malformed stream segment"));
        }
        Ok(segment)
    }

    /// The retained entries at or after `from`.
    pub(crate) fn entries_from(&self, from: u64) -> impl Iterator<Item = (u64, &Bson)> {
        let first = self.start + self.trimmed;
        self.values
            .iter()
            .enumerate()
            .map(move |(index, value)| (first + index as u64, value))
            .filter(move |(offset, _)| *offset >= from)
    }
}

/// The segment document for `values` appended at `start` in `generation`.
///
/// # Errors
///
/// A value that does not convert to BSON, or an exhausted offset space.
pub(crate) fn new_segment(
    stream: &str,
    generation: i64,
    start: u64,
    values: &[Value],
) -> Result<Document> {
    let values: Vec<Bson> = values.iter().map(to_bson).collect::<Result<_>>()?;
    let count = values.len() as u64;
    let end = start
        .checked_add(count)
        .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
    Ok(doc! {
        "s": stream,
        "g": generation,
        "o": stored_offset(start)?,
        "n": stored_offset(count)?,
        "e": stored_offset(end)?,
        "t": 0_i64,
        "vs": values,
    })
}

fn segment_models() -> Vec<IndexModel> {
    let named = |name: &str, unique: bool| {
        let mut options = IndexOptions::builder().name(name.to_owned()).build();
        options.unique = Some(unique);
        options
    };
    vec![
        IndexModel::builder()
            .keys(doc! {SCOPE: 1, "s": 1, "g": 1, "o": 1})
            .options(named(STREAM_SEGMENT_INDEX, true))
            .build(),
        IndexModel::builder()
            .keys(doc! {SCOPE: 1, "s": 1, "g": 1, "e": 1})
            .options(named(STREAM_END_INDEX, false))
            .build(),
    ]
}

/// The `_id` of a stream header: unique per scope and stream.
pub(crate) fn head_id(scope: &Scope, stream: &str) -> Document {
    doc! {"s": scope.as_str(), "n": stream}
}

/// MongoDB streams bound to one scope.
#[derive(Debug)]
pub(crate) struct MongoStreams {
    shared: Arc<Shared>,
    scope: Scope,
}

impl MongoStreams {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope) -> Self {
        Self { shared, scope }
    }

    async fn segments(&self) -> Result<ScopedCollection> {
        self.shared.prepare(STREAMS, segment_models()).await?;
        Ok(self.shared.scoped(STREAMS, &self.scope))
    }

    fn heads(&self) -> ScopedCollection {
        self.shared.scoped(STREAM_HEADS, &self.scope)
    }

    /// The current generation of `stream`.
    async fn generation(&self, stream: &str) -> Result<i64> {
        let head = self
            .heads()
            .find_one(doc! {"_id": head_id(&self.scope, stream)}, None)
            .await
            .map_err(errors::failed("read a stream header"))?;
        Ok(head.map_or(0, |head| head.get_i64("g").unwrap_or(0)))
    }

    /// The segment of `stream` in `generation` first in `order` (`1` first,
    /// `-1` last).
    async fn edge(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
        order: i32,
    ) -> Result<Option<Segment>> {
        let found = handle
            .find(
                doc! {"s": stream, "g": generation},
                FindOptions::builder()
                    .sort(doc! {"o": order})
                    .limit(1)
                    .build(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        found.first().map(Segment::decode).transpose()
    }
}

#[async_trait]
impl StreamStore for MongoStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        validate_stream(stream)?;
        let handle = self.segments().await?;
        for _ in 0..APPEND_ATTEMPTS {
            let generation = self.generation(stream).await?;
            let end = self
                .edge(&handle, stream, generation, -1)
                .await?
                .map_or(0, |last| last.end);
            if values.is_empty() {
                return Ok(end);
            }
            match handle
                .insert_one(new_segment(stream, generation, end, &values)?, None)
                .await
            {
                Ok(()) => return Ok(end),
                Err(error)
                    if errors::duplicate_index(&error).as_deref() == Some(STREAM_SEGMENT_INDEX) => {
                }
                Err(error) => return Err(errors::map(error, "append to a stream")),
            }
        }
        Err(StorageError::unavailable(
            "stream kept growing concurrently; retry",
        ))
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let handle = self.segments().await?;
        let generation = self.generation(stream).await?;
        let mut cursor = handle
            .cursor(
                doc! {"s": stream, "g": generation, "e": {"$gt": stored_offset(from).unwrap_or(i64::MAX)}},
                FindOptions::builder().sort(doc! {"o": 1}).build(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        let mut out = Vec::new();
        while let Some(doc) = cursor
            .try_next()
            .await
            .map_err(errors::failed("read a stream"))?
        {
            for (offset, value) in Segment::decode(&doc)?.entries_from(from) {
                out.push(StreamEntry {
                    offset,
                    value: from_bson(value)?,
                });
                if out.len() == limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let handle = self.segments().await?;
        let generation = self.generation(stream).await?;
        Ok(self
            .edge(&handle, stream, generation, -1)
            .await?
            .map_or(0, |last| last.end))
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let handle = self.segments().await?;
        let generation = self.generation(stream).await?;
        let Some(last) = self.edge(&handle, stream, generation, -1).await? else {
            return Ok(0);
        };
        let affected = handle
            .find(
                doc! {"s": stream, "g": generation, "o": {"$lt": stored_offset(offset).unwrap_or(i64::MAX)}},
                FindOptions::builder().sort(doc! {"o": 1}).build(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        let segments: Vec<Segment> = affected
            .iter()
            .map(Segment::decode)
            .collect::<Result<_>>()?;
        // The first retained offset: past every fully trimmed segment.
        let base = segments
            .iter()
            .find(|segment| segment.start + segment.trimmed < segment.end)
            .map_or_else(
                || {
                    segments
                        .last()
                        .map_or(last.end, |segment| segment.end.min(last.end))
                },
                |segment| segment.start + segment.trimmed,
            );
        let cut = offset.clamp(base, last.end);
        for segment in &segments {
            let keep_from = cut.clamp(segment.start, segment.end) - segment.start;
            if segment.end <= cut && segment.start != last.start {
                handle
                    .delete_many(doc! {"_id": segment.id.clone()}, None)
                    .await
                    .map_err(errors::failed("truncate a stream"))?;
            } else if keep_from > segment.trimmed {
                let drop = usize::try_from(keep_from - segment.trimmed).unwrap_or(usize::MAX);
                let rest: Vec<Bson> = segment.values.iter().skip(drop).cloned().collect();
                handle
                    .update_one(
                        doc! {"_id": segment.id.clone(), "t": stored_offset(segment.trimmed)?},
                        doc! {"$set": {"t": stored_offset(keep_from)?, "vs": rest}},
                        false,
                        None,
                    )
                    .await
                    .map_err(errors::failed("truncate a stream"))?;
            }
        }
        Ok(cut - base)
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let handle = self.segments().await?;
        let current = self.generation(stream).await?;
        // Advance first: from here on every reader ignores the old segments,
        // and an append still holding the old generation lands among them.
        self.heads()
            .update_one(
                doc! {"_id": head_id(&self.scope, stream), "n": stream},
                doc! {"$inc": {"g": 1_i64}},
                true,
                None,
            )
            .await
            .map_err(errors::failed("delete a stream"))?;
        let removed = handle
            .delete_many(doc! {"s": stream, "g": {"$lte": current}}, None)
            .await
            .map_err(errors::failed("delete a stream"))?;
        Ok(removed > 0)
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let handle = self.segments().await?;
        let present = handle
            .aggregate(
                doc! {"s": prefix_regex(prefix)},
                vec![doc! {"$group": {"_id": {"s": "$s", "g": "$g"}}}],
            )
            .await
            .map_err(errors::failed("list streams"))?;
        let heads = self
            .heads()
            .find(doc! {"n": prefix_regex(prefix)}, FindOptions::default())
            .await
            .map_err(errors::failed("list streams"))?;
        let generations: std::collections::HashMap<&str, i64> = heads
            .iter()
            .filter_map(|head| Some((head.get_str("n").ok()?, head.get_i64("g").ok()?)))
            .collect();
        let mut names: Vec<String> = present
            .iter()
            .filter_map(|group| {
                let key = group.get_document("_id").ok()?;
                let name = key.get_str("s").ok()?;
                let generation = key.get_i64("g").unwrap_or(0);
                (name.starts_with(prefix)
                    && generations.get(name).copied().unwrap_or(0) == generation)
                    .then(|| name.to_owned())
            })
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
