//! The stored form of a document and the rules applied to it before a write.
//!
//! Everything here is pure, so the precondition, version and expiry rules are
//! tested without a server and match the memory driver line for line.

use mongodb::bson::{Bson, Document, doc};
use serde_json::{Map, Value};
use tinystoragedrivers_core::{
    CollectionSpec, Filter, Precondition, Result, Scope, StorageError, Version, Versioned,
};

use crate::convert::{from_document, to_document};
use crate::naming::{BODY, DELETED, KEY, VERSION, document_id};

/// One stored document as read back from MongoDB.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Stored {
    pub(crate) key: String,
    pub(crate) version: Version,
    pub(crate) body: Value,
    /// A tombstone: the document was removed and only its version remains.
    pub(crate) deleted: bool,
}

impl Stored {
    /// Decode a stored document.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
    /// when the document lacks the driver's fields or holds values JSON cannot.
    pub(crate) fn decode(doc: &Document) -> Result<Self> {
        let malformed = || StorageError::serialization("stored document is missing driver fields");
        let key = doc.get_str(KEY).map_err(|_| malformed())?.to_owned();
        let version = doc
            .get_i64(VERSION)
            .ok()
            .and_then(|version| u64::try_from(version).ok())
            .filter(|version| *version >= 1)
            .ok_or_else(malformed)?;
        let body = from_document(doc.get_document(BODY).map_err(|_| malformed())?)?;
        Ok(Self {
            key,
            version: Version(version),
            body,
            deleted: doc.get_bool(DELETED).unwrap_or(false),
        })
    }

    pub(crate) fn into_versioned(self) -> Versioned<Value> {
        Versioned {
            id: self.key,
            version: self.version,
            doc: self.body,
        }
    }
}

/// The filter matching documents still live under `spec`'s expiry at `now`.
pub(crate) fn live_filter(spec: &CollectionSpec, now_ms: u64) -> Filter {
    match &spec.ttl_field {
        // A JSON number at or below now: exactly the memory driver's rule.
        Some(field) => Filter::lte(field.clone(), now_ms).negate(),
        None => Filter::All,
    }
}

/// The filter matching expired documents, or `None` without expiry.
pub(crate) fn expired_filter(spec: &CollectionSpec, now_ms: u64) -> Option<Filter> {
    spec.ttl_field
        .as_ref()
        .map(|field| Filter::lte(field.clone(), now_ms))
}

/// Whether `stored` is a document a read returns: not a tombstone and not
/// expired.
pub(crate) fn is_live(spec: &CollectionSpec, stored: &Stored, now_ms: u64) -> bool {
    !stored.deleted && live_filter(spec, now_ms).matches(&stored.key, &stored.body)
}

/// The query clause excluding tombstones.
pub(crate) fn not_deleted() -> Document {
    doc! {DELETED: {"$exists": false}}
}

/// `query` with tombstones excluded.
pub(crate) fn visible(query: Document) -> Document {
    if query.is_empty() {
        not_deleted()
    } else {
        doc! {"$and": [not_deleted(), query]}
    }
}

/// The update turning a document into its tombstone: the version stays, the
/// body empties (so no unique or text index holds it any more).
pub(crate) fn bury() -> Document {
    doc! {"$set": {BODY: {}, DELETED: true}}
}

/// Enforce a precondition against the live document, if any.
///
/// # Errors
///
/// [`ErrorKind::Conflict`](tinystoragedrivers_core::ErrorKind::Conflict).
pub(crate) fn check(precondition: Precondition, live: Option<Version>) -> Result<()> {
    match (precondition, live) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(found)) => Err(StorageError::conflict(format!(
            "document already exists at version {}",
            found.0
        ))),
        (Precondition::Version(expected), Some(found)) if found == expected => Ok(()),
        (Precondition::Version(expected), found) => Err(StorageError::conflict(format!(
            "document expected at version {}, found {}",
            expected.0,
            found.map_or_else(|| "nothing".to_owned(), |f| f.0.to_string())
        ))),
    }
}

/// The version a write after `last` gets.
///
/// # Errors
///
/// [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend) once
/// the version space is exhausted. Versions are stored as `Int64`, so the
/// space ends at `i64::MAX`.
pub(crate) fn next_version(last: Option<Version>) -> Result<Version> {
    let exhausted = || StorageError::backend("document version space is exhausted");
    let next = match last {
        None => Version::FIRST,
        Some(last) => last.next().ok_or_else(exhausted)?,
    };
    if i64::try_from(next.0).is_err() {
        return Err(exhausted());
    }
    Ok(next)
}

/// The stored form of a body. The scope is stamped by the scoped collection.
///
/// # Errors
///
/// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
/// for a body holding an integer above `i64::MAX`.
pub(crate) fn encode(
    scope: &Scope,
    key: &str,
    version: Version,
    body: &Map<String, Value>,
) -> Result<Document> {
    let version = i64::try_from(version.0)
        .map_err(|_| StorageError::backend("document version space is exhausted"))?;
    Ok(doc! {
        "_id": document_id(scope, key),
        KEY: key,
        VERSION: version,
        BODY: to_document(body)?,
    })
}

/// The filter selecting one stored document at one version.
pub(crate) fn at_version(scope: &Scope, key: &str, version: Version) -> Document {
    let version = i64::try_from(version.0).map_or(Bson::Null, Bson::Int64);
    doc! {"_id": document_id(scope, key), VERSION: version}
}
