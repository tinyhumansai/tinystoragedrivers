//! Values the document port takes and returns.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, StorageError};
use crate::filter::{Filter, Sort};

/// The longest document id a driver must accept, in bytes.
pub const MAX_ID_LEN: usize = 512;

/// The longest collection name a driver must accept, in bytes.
pub const MAX_COLLECTION_LEN: usize = 120;

/// Collection names drivers reserve for their own bookkeeping start with this.
pub const RESERVED_PREFIX: &str = "_tsd";

/// A document's revision. Every successful write of a document produces a
/// strictly greater version; a newly created document starts at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(pub u64);

impl Version {
    /// The version of a freshly created document.
    pub const FIRST: Self = Self(1);

    /// The version after this one, or `None` once the version space is
    /// exhausted. A driver must fail the write rather than reuse a version.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// What must be true of the stored document for a write to proceed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", content = "version", rename_all = "snake_case")]
pub enum Precondition {
    /// Write unconditionally (upsert).
    #[default]
    None,
    /// The document must not exist (insert).
    Absent,
    /// The document must exist at exactly this version (compare-and-swap).
    Version(Version),
}

/// A stored document with its id and revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Versioned<T> {
    /// The document id.
    pub id: String,
    /// The revision this copy was read at.
    pub version: Version,
    /// The document body.
    pub doc: T,
}

impl<T> Versioned<T> {
    /// The precondition that succeeds only if nobody wrote the document since
    /// this copy was read.
    #[must_use]
    pub fn unchanged(&self) -> Precondition {
        Precondition::Version(self.version)
    }

    /// Transform the body, keeping id and version.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Versioned<U> {
        Versioned {
            id: self.id,
            version: self.version,
            doc: f(self.doc),
        }
    }
}

/// A secondary index declaration.
///
/// Every driver must answer filters on any field, indexed or not; an index is a
/// performance hint plus, when `unique`, a constraint every driver enforces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    /// Index name, unique within the collection.
    pub name: String,
    /// Dotted field paths, in key order.
    pub fields: Vec<String>,
    /// Reject a write that would give two documents equal values for every
    /// field. Documents missing any of the fields are not constrained.
    #[serde(default)]
    pub unique: bool,
}

impl IndexSpec {
    /// A non-unique index over `fields`.
    #[must_use]
    pub fn new<S: Into<String>>(
        name: impl Into<String>,
        fields: impl IntoIterator<Item = S>,
    ) -> Self {
        Self {
            name: name.into(),
            fields: fields.into_iter().map(Into::into).collect(),
            unique: false,
        }
    }

    /// Make the index unique.
    #[must_use]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Which string fields [`DocumentStore::search`](crate::DocumentStore::search)
/// covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchSpec {
    /// Dotted paths of the searchable fields.
    pub fields: Vec<String>,
}

/// A collection's declared shape: indexes, expiry and search.
///
/// [`DocumentStore::ensure_collection`](crate::DocumentStore::ensure_collection)
/// is idempotent, and a collection that was never declared behaves as one
/// declared with [`CollectionSpec::new`] alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionSpec {
    /// Collection name.
    pub name: String,
    /// Secondary indexes.
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    /// A field holding an expiry time in Unix epoch milliseconds. Once the time
    /// has passed the document reads as absent and may be removed. Requires
    /// [`Capability::Ttl`](crate::Capability::Ttl).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_field: Option<String>,
    /// Full-text search fields. Requires
    /// [`Capability::FullText`](crate::Capability::FullText) to be searched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchSpec>,
}

impl CollectionSpec {
    /// A collection with no indexes, expiry or search.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            indexes: Vec::new(),
            ttl_field: None,
            search: None,
        }
    }

    /// Add an index.
    #[must_use]
    pub fn index(mut self, index: IndexSpec) -> Self {
        self.indexes.push(index);
        self
    }

    /// Expire documents on `field`.
    #[must_use]
    pub fn ttl(mut self, field: impl Into<String>) -> Self {
        self.ttl_field = Some(field.into());
        self
    }

    /// Make `fields` searchable.
    #[must_use]
    pub fn searchable<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        self.search = Some(SearchSpec {
            fields: fields.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Combine an existing declaration with a newer one for the same
    /// collection: indexes and search fields accumulate, and the expiry field
    /// is kept or added.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// names differ, when an index name is redeclared with different fields or
    /// uniqueness, or when the expiry field changes.
    pub fn merge(&self, newer: &CollectionSpec) -> Result<CollectionSpec> {
        if self.name != newer.name {
            return Err(StorageError::invalid_input(
                "cannot merge declarations of different collections",
            ));
        }
        let mut merged = self.clone();
        for index in &newer.indexes {
            match merged.indexes.iter().find(|have| have.name == index.name) {
                Some(have) if have == index => {}
                Some(_) => {
                    return Err(StorageError::invalid_input(format!(
                        "index `{}` is already declared differently",
                        index.name
                    )));
                }
                None => merged.indexes.push(index.clone()),
            }
        }
        match (&merged.ttl_field, &newer.ttl_field) {
            (Some(have), Some(want)) if have != want => {
                return Err(StorageError::invalid_input(
                    "the expiry field of a collection cannot change",
                ));
            }
            (None, Some(want)) => merged.ttl_field = Some(want.clone()),
            _ => {}
        }
        if let Some(newer_search) = &newer.search {
            let search = merged
                .search
                .get_or_insert_with(|| SearchSpec { fields: Vec::new() });
            for field in &newer_search.fields {
                if !search.fields.contains(field) {
                    search.fields.push(field.clone());
                }
            }
        }
        Ok(merged)
    }

    /// Check the name, index names and field paths.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid collection name, an index with no fields, or a duplicated index
    /// name.
    pub fn validate(&self) -> Result<()> {
        validate_collection(&self.name)?;
        let mut names = std::collections::BTreeSet::new();
        for index in &self.indexes {
            if index.name.is_empty() || index.fields.is_empty() {
                return Err(StorageError::invalid_input(format!(
                    "index `{}` on `{}` needs a name and at least one field",
                    index.name, self.name
                )));
            }
            if !names.insert(index.name.as_str()) {
                return Err(StorageError::invalid_input(format!(
                    "index `{}` is declared twice on `{}`",
                    index.name, self.name
                )));
            }
            for field in &index.fields {
                Filter::exists(field.clone(), true).validate()?;
            }
        }
        if let Some(field) = &self.ttl_field {
            Filter::exists(field.clone(), true).validate()?;
        }
        if let Some(search) = &self.search {
            for field in &search.fields {
                Filter::exists(field.clone(), true).validate()?;
            }
        }
        Ok(())
    }
}

/// Validate a collection name: 1 to [`MAX_COLLECTION_LEN`] ASCII letters,
/// digits, `_`, `-` or `.`, not starting with [`RESERVED_PREFIX`].
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) describing the
/// problem.
pub fn validate_collection(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_COLLECTION_LEN {
        return Err(StorageError::invalid_input(format!(
            "collection name must be 1 to {MAX_COLLECTION_LEN} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(StorageError::invalid_input(
            "collection names may only contain ASCII letters, digits, `_`, `-` and `.`",
        ));
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err(StorageError::invalid_input(format!(
            "collection names starting with `{RESERVED_PREFIX}` are reserved"
        )));
    }
    Ok(())
}

/// Validate a document id: 1 to [`MAX_ID_LEN`] bytes without NUL.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) describing the
/// problem.
pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_ID_LEN || id.contains('\0') {
        return Err(StorageError::invalid_input(format!(
            "document id must be 1 to {MAX_ID_LEN} bytes without NUL"
        )));
    }
    Ok(())
}

/// Require a document body to be a JSON object, the shape every backend can
/// store and index.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for any other
/// JSON value.
pub fn validate_doc(doc: &Value) -> Result<()> {
    if doc.is_object() {
        Ok(())
    } else {
        Err(StorageError::invalid_input(
            "document body must be a JSON object",
        ))
    }
}

/// An opaque position in a query's result order, returned by one page and
/// passed back to fetch the next.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(pub String);

/// A filtered, ordered, paginated read.
///
/// ```
/// use tinystoragedrivers_core::{Filter, Query, Sort};
///
/// let query = Query::filter(Filter::eq("state", "queued"))
///     .sort(Sort::asc("run_at"))
///     .limit(10);
/// assert_eq!(query.limit, Some(10));
/// ```
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Query {
    /// Which documents.
    #[serde(default)]
    pub filter: Filter,
    /// Ordering. Ties (and an empty list) order by id ascending.
    #[serde(default)]
    pub sort: Vec<Sort>,
    /// At most this many documents per page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Continue after this position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

impl Query {
    /// Every document, ordered by id.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Documents matching `filter`.
    #[must_use]
    pub fn filter(filter: Filter) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }

    /// Add a sort key.
    #[must_use]
    pub fn sort(mut self, sort: Sort) -> Self {
        self.sort.push(sort);
        self
    }

    /// Cap the page size.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Reject a query no driver can page through: an untranslatable filter or
    /// a zero page size (which would never advance).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
    pub fn validate(&self) -> Result<()> {
        if self.limit == Some(0) {
            return Err(StorageError::invalid_input(
                "query limit must be at least 1",
            ));
        }
        self.filter.validate()
    }

    /// Continue from a previous page.
    #[must_use]
    pub fn after(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }
}

/// One page of query results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    /// The documents, in query order.
    pub items: Vec<T>,
    /// Where the next page starts, or `None` when this is the last page.
    pub next: Option<Cursor>,
}

/// One write inside an atomic batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteOp {
    /// [`DocumentStore::put`](crate::DocumentStore::put).
    Put {
        /// Collection.
        collection: String,
        /// Document id.
        id: String,
        /// Document body.
        doc: Value,
        /// Precondition.
        #[serde(default)]
        precondition: Precondition,
    },
    /// [`DocumentStore::delete`](crate::DocumentStore::delete).
    Delete {
        /// Collection.
        collection: String,
        /// Document id.
        id: String,
        /// Precondition.
        #[serde(default)]
        precondition: Precondition,
    },
}

/// The outcome of one [`WriteOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WriteResult {
    /// The document's new version.
    Put {
        /// New version.
        version: Version,
    },
    /// Whether a document was removed.
    Delete {
        /// Whether something was removed.
        removed: bool,
    },
}

/// One full-text match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    /// The matching document's id.
    pub id: String,
    /// Driver-specific relevance; larger is better. Only comparable within one
    /// result list.
    pub score: f64,
}
