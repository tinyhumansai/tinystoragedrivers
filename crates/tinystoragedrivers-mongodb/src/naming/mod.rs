//! Names the driver gives things in MongoDB, and the opaque strings it hands
//! back to callers.
//!
//! - **Collections.** A port collection `jobs` lives in the Mongo collection
//!   `jobs` for the root database and `<name>:jobs` for
//!   [`database(name)`](tinystoragedrivers_core::StorageBackend::database).
//!   `:` cannot appear in a port collection name, so no root collection can
//!   collide with a named database's. Driver bookkeeping uses the reserved
//!   [`RESERVED_PREFIX`](tinystoragedrivers_core::RESERVED_PREFIX), which port
//!   names cannot start with.
//! - **Stored documents.** `{_id: {s, k}, _scope, _key, _v, d}`: the body lives
//!   under [`BODY`] so its field names never collide with the driver's.
//! - **Cursors and index names** carry an FNV-1a hash, which, unlike
//!   `DefaultHasher`, is stable across processes and Rust releases. A cursor
//!   issued by one server process is valid on another.

use mongodb::bson::{Bson, Document, doc};
use tinystoragedrivers_core::{
    Cursor, Filter, Query, RESERVED_PREFIX, Result, Scope, Sort, StorageError,
};

/// The field holding the tenant key on every stored record.
pub(crate) const SCOPE: &str = "_scope";
/// The field holding the port-level document id.
pub(crate) const KEY: &str = "_key";
/// The field holding the document version as an `Int64`.
pub(crate) const VERSION: &str = "_v";
/// The field holding the document body.
pub(crate) const BODY: &str = "d";

/// Driver metadata: one document per declared collection.
pub(crate) const META: &str = "_tsd_meta";
/// The last version of every removed document, so a recreated id continues
/// its version sequence.
pub(crate) const TOMBSTONES: &str = "_tsd_tombstones";
/// Stream segments.
pub(crate) const STREAMS: &str = "_tsd_streams";
/// The GridFS bucket holding blobs.
pub(crate) const BLOBS: &str = "_tsd_blobs";

/// The `(_scope, _key)` index every document collection carries.
pub(crate) const SCOPE_KEY_INDEX: &str = "_tsd_scope_key";
/// The one text index a searchable collection carries.
pub(crate) const TEXT_INDEX: &str = "_tsd_text";
/// The unique `(_scope, s, o)` index over stream segments.
pub(crate) const STREAM_SEGMENT_INDEX: &str = "_tsd_stream_segment";
/// The `(_scope, s, e)` index window reads use.
pub(crate) const STREAM_END_INDEX: &str = "_tsd_stream_end";
/// The `(metadata._scope, filename)` index over GridFS files.
pub(crate) const BLOB_KEY_INDEX: &str = "_tsd_blob_key";

/// The `_id` of a tombstone: the port collection, scope and key.
pub(crate) fn tombstone_id(collection: &str, scope: &Scope, key: &str) -> Document {
    doc! {"c": collection, "s": scope.as_str(), "k": key}
}

/// The Mongo collection prefix of a named database.
pub(crate) fn database_prefix(name: &str) -> String {
    format!("{name}:")
}

/// The `_id` of a stored document: unique per scope and key, so the default
/// `_id` index enforces both.
pub(crate) fn document_id(scope: &Scope, key: &str) -> Document {
    doc! {"s": scope.as_str(), "k": key}
}

/// The Mongo index name for a declared [`IndexSpec`](tinystoragedrivers_core::IndexSpec).
///
/// Declared names are free text; hashing them keeps the Mongo name short,
/// free of characters Mongo's duplicate-key messages cannot delimit, and
/// inside the reserved prefix.
pub(crate) fn index_name(declared: &str) -> String {
    format!("{RESERVED_PREFIX}_ix_{:016x}", fnv1a(declared.as_bytes()))
}

/// 64-bit FNV-1a.
pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// A fingerprint of everything that decides a query's result order.
pub(crate) fn fingerprint(collection: &str, filter: &Filter, sort: &[Sort]) -> u64 {
    let mut bytes = collection.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend(serde_json::to_vec(filter).unwrap_or_default());
    bytes.push(0);
    bytes.extend(serde_json::to_vec(sort).unwrap_or_default());
    fnv1a(&bytes)
}

/// The cursor that resumes `query` on `collection` at `offset`.
pub(crate) fn encode_cursor(collection: &str, query: &Query, offset: u64) -> Cursor {
    Cursor(format!(
        "mongo:{:016x}:{offset}",
        fingerprint(collection, &query.filter, &query.sort)
    ))
}

/// The offset `query.cursor` resumes at, or 0 without a cursor.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
/// for a cursor this driver did not issue for this query.
pub(crate) fn decode_cursor(collection: &str, query: &Query) -> Result<u64> {
    let Some(cursor) = &query.cursor else {
        return Ok(0);
    };
    let expected = format!(
        "mongo:{:016x}:",
        fingerprint(collection, &query.filter, &query.sort)
    );
    cursor
        .0
        .strip_prefix(&expected)
        .and_then(|offset| offset.parse().ok())
        .ok_or_else(|| StorageError::invalid_input("cursor was not issued for this query"))
}

/// An anchored regular expression matching strings that start with `prefix`.
pub(crate) fn prefix_regex(prefix: &str) -> Bson {
    let mut pattern = String::with_capacity(prefix.len() + 1);
    pattern.push('^');
    for c in prefix.chars() {
        if "\\^$.|?*+()[]{}".contains(c) {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    Bson::RegularExpression(mongodb::bson::Regex {
        pattern,
        options: String::new(),
    })
}

/// Check a MongoDB database name: 1 to 63 bytes, none of `/\. "$*<>:|?` or NUL.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput).
pub(crate) fn validate_mongo_database(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() < 64
        && !name
            .chars()
            .any(|c| c == '\0' || "/\\. \"$*<>:|?".contains(c));
    if ok {
        Ok(())
    } else {
        Err(StorageError::invalid_input(
            "MongoDB database name must be 1 to 63 bytes without / \\ . space \" $ * < > : | ? or NUL",
        ))
    }
}

/// The index a duplicate-key message names: `… index: <name> dup key: …`.
pub(crate) fn duplicate_key_index(message: &str) -> Option<&str> {
    let (_, rest) = message.split_once("index: ")?;
    rest.split_whitespace().next()
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
