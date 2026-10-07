//! The document rules applied to files, run under the database lock.
//!
//! These mirror the memory driver's `DbState` one for one: expired documents
//! stay on disk (so versions keep rising across expiry) but read as absent,
//! and a delete removes the file (so a re-created document starts again at
//! [`Version::FIRST`]).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, Filter, Precondition, Result, Scope, Sort, StorageError, Version, Versioned,
    sort_documents, validate_collection, validate_doc, validate_id, value,
};

use crate::encode::{dir_components, file_stem};
use crate::fsio::{files_with_suffix, read_json, remove_optional, write_json};
use crate::storage::Db;

/// One document file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct StoredDoc {
    pub(super) id: String,
    pub(super) version: Version,
    pub(super) doc: Value,
}

impl StoredDoc {
    fn versioned(self) -> Versioned<Value> {
        Versioned {
            id: self.id,
            version: self.version,
            doc: self.doc,
        }
    }
}

/// The error for two names that hash to one file name. Unreachable outside a
/// deliberate 128-bit FNV collision, and refused rather than overwriting the
/// other document.
pub(super) fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to overwrite another entry")
}

/// One scope's documents in one database.
#[derive(Debug)]
pub(super) struct Docs<'a> {
    pub(super) db: &'a Db,
    pub(super) scope: &'a Scope,
    pub(super) now: u64,
}

impl Docs<'_> {
    fn collection_dir(&self, collection: &str) -> PathBuf {
        self.db
            .scope_dir(self.scope)
            .join("docs")
            .join(dir_components(collection))
    }

    fn doc_path(&self, collection: &str, id: &str) -> PathBuf {
        self.collection_dir(collection)
            .join(format!("{}.json", file_stem(id)))
    }

    fn spec_path(&self, collection: &str) -> PathBuf {
        self.db
            .specs_dir()
            .join(format!("{}.json", file_stem(collection)))
    }

    /// The collection's declaration, or the default for an undeclared one.
    pub(super) fn spec(&self, collection: &str) -> Result<CollectionSpec> {
        match read_json::<CollectionSpec>(&self.spec_path(collection))? {
            Some(spec) if spec.name == collection => Ok(spec),
            Some(_) => Err(collision()),
            None => Ok(CollectionSpec::new(collection)),
        }
    }

    /// Merge `spec` into the stored declaration.
    pub(super) fn ensure(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let path = self.spec_path(&spec.name);
        let merged = match read_json::<CollectionSpec>(&path)? {
            Some(existing) if existing.name == spec.name => existing.merge(spec)?,
            Some(_) => return Err(collision()),
            None => spec.clone(),
        };
        write_json(&path, &merged)
    }

    /// The stored file for `id`, expired or not. `Err` when the file belongs
    /// to a different id (a hash collision).
    fn stored(&self, collection: &str, id: &str) -> Result<Option<StoredDoc>> {
        match read_json::<StoredDoc>(&self.doc_path(collection, id))? {
            Some(stored) if stored.id == id => Ok(Some(stored)),
            Some(_) => Err(collision()),
            None => Ok(None),
        }
    }

    fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
        spec.ttl_field
            .as_deref()
            .and_then(|field| value::lookup(doc, field))
            .and_then(Value::as_f64)
            .is_some_and(|expires| {
                // Epoch milliseconds fit an f64 exactly until the year 287396.
                #[allow(clippy::cast_precision_loss)]
                let now = now_ms as f64;
                expires <= now
            })
    }

    /// Every live (non-expired) document of a collection, in file order.
    pub(super) fn live(&self, spec: &CollectionSpec) -> Result<Vec<StoredDoc>> {
        let mut out = Vec::new();
        for path in files_with_suffix(&self.collection_dir(&spec.name), ".json")? {
            if let Some(stored) = read_json::<StoredDoc>(&path)?
                && !Self::expired(spec, &stored.doc, self.now)
            {
                out.push(stored);
            }
        }
        Ok(out)
    }

    /// Read one live document.
    pub(super) fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.spec(collection)?;
        Ok(self
            .stored(collection, id)?
            .filter(|stored| !Self::expired(&spec, &stored.doc, self.now))
            .map(StoredDoc::versioned))
    }

    /// Write one document under `precondition`.
    pub(super) fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        let spec = self.spec(collection)?;
        let stored = self.stored(collection, id)?;
        let current = stored
            .as_ref()
            .filter(|stored| !Self::expired(&spec, &stored.doc, self.now));
        check(precondition, current.map(|stored| stored.version))?;
        self.check_unique(&spec, id, &doc)?;
        // Versions keep rising across expiry, so a stale CAS on a re-created
        // document still fails.
        let version = match &stored {
            None => Version::FIRST,
            Some(stored) => stored
                .version
                .next()
                .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
        };
        let record = StoredDoc {
            id: id.to_owned(),
            version,
            doc,
        };
        write_json(&self.doc_path(collection, id), &record)?;
        Ok(version)
    }

    /// Remove one document under `precondition`; report whether a live one
    /// went.
    pub(super) fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.spec(collection)?;
        let stored = self.stored(collection, id)?;
        let live = stored
            .as_ref()
            .filter(|stored| !Self::expired(&spec, &stored.doc, self.now))
            .map(|stored| stored.version);
        check(precondition, live)?;
        let removed = match stored {
            Some(_) => remove_optional(&self.doc_path(collection, id))?,
            None => false,
        };
        Ok(removed && live.is_some())
    }

    fn check_unique(&self, spec: &CollectionSpec, id: &str, doc: &Value) -> Result<()> {
        let unique: Vec<_> = spec.indexes.iter().filter(|index| index.unique).collect();
        if unique.is_empty() {
            return Ok(());
        }
        let others = self.live(spec)?;
        for index in unique {
            let Some(key) = index_key(&index.fields, doc) else {
                continue;
            };
            let clash = others.iter().any(|other| {
                other.id != id
                    && index_key(&index.fields, &other.doc).is_some_and(|theirs| {
                        theirs.iter().zip(&key).all(|(a, b)| value::equal(a, b))
                    })
            });
            if clash {
                return Err(StorageError::already_exists(format!(
                    "unique index `{}` already holds this value",
                    index.name
                )));
            }
        }
        Ok(())
    }

    /// The live documents matching `filter`, in `sort` order.
    pub(super) fn matching(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
    ) -> Result<Vec<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        let spec = self.spec(collection)?;
        let mut found: Vec<Versioned<Value>> = self
            .live(&spec)?
            .into_iter()
            .filter(|stored| filter.matches(&stored.id, &stored.doc))
            .map(StoredDoc::versioned)
            .collect();
        sort_documents(&mut found, sort, |item| (item.id.as_str(), &item.doc));
        Ok(found)
    }

    /// Remove the live documents matching `filter`, returning how many.
    pub(super) fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let doomed = self.matching(collection, filter, &[])?;
        for doc in &doomed {
            remove_optional(&self.doc_path(collection, &doc.id))?;
        }
        Ok(doomed.len() as u64)
    }

    /// Remove every document file of the collection in this scope. Only the
    /// files go: a collection directory can also hold the continuation
    /// directories of longer collection names (see the `encode` module).
    pub(super) fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        let dir = self.collection_dir(collection);
        for path in files_with_suffix(&dir, ".json")? {
            remove_optional(&path)?;
        }
        // Best effort: fails, harmlessly, when continuation directories remain.
        let _ = std::fs::remove_dir(&dir);
        Ok(())
    }
}

/// The values of `fields` in `doc`, or `None` when any is missing.
fn index_key(fields: &[String], doc: &Value) -> Option<Vec<Value>> {
    fields
        .iter()
        .map(|field| value::lookup(doc, field).cloned())
        .collect()
}

/// Enforce a precondition against the live document's version, if any.
pub(super) fn check(precondition: Precondition, current: Option<Version>) -> Result<()> {
    match (precondition, current) {
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
