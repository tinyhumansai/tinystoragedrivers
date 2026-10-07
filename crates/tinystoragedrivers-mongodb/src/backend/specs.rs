//! Collection declarations and the indexes they become.
//!
//! A declaration is collection-wide, not per scope, so it lives in the
//! driver's `_tsd_meta` collection as `{_id: <collection>, spec: <json>, rev}`
//! and is merged with [`CollectionSpec::merge`] under compare-and-swap on
//! `rev`. Indexes are built *before* the merged spec is written: a unique
//! index the stored documents already violate fails to build, and then
//! nothing is declared.
//!
//! Every index leads with `_scope`, so each tenant's queries stay inside its
//! own key range, and a unique index constrains values per scope (as the
//! memory driver does). A unique index carries a partial filter requiring
//! every field to exist, which leaves documents missing a field
//! unconstrained.
//!
//! Each process caches declarations for [`REFRESH`], so a declaration made by
//! another process is honored within that window; declarations made through
//! this backend apply immediately.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use mongodb::IndexModel;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::IndexOptions;
use tinystoragedrivers_core::{CollectionSpec, Result, SearchSpec, StorageError};

use super::Shared;
use crate::documents::stored::{Stored, bury, expired_filter, not_deleted, visible};
use crate::errors;
use crate::naming::{KEY, META, SCOPE, SCOPE_KEY_INDEX, TEXT_INDEX, VERSION, index_name};
use crate::translate::{self, body_field};

/// How long a declaration read from `_tsd_meta` is trusted.
pub(crate) const REFRESH: Duration = Duration::from_secs(5);

/// Declarations and prepared collections, per named database.
#[derive(Debug, Default)]
pub(crate) struct SpecCache {
    specs: Mutex<HashMap<String, (Instant, CollectionSpec)>>,
    prepared: Mutex<HashSet<String>>,
}

impl SpecCache {
    fn cached(&self, name: &str) -> Option<CollectionSpec> {
        let specs = self.specs.lock().ok()?;
        specs
            .get(name)
            .filter(|(at, _)| at.elapsed() < REFRESH)
            .map(|(_, spec)| spec.clone())
    }

    fn store(&self, spec: CollectionSpec) {
        if let Ok(mut specs) = self.specs.lock() {
            specs.insert(spec.name.clone(), (Instant::now(), spec));
        }
    }

    fn is_prepared(&self, raw: &str) -> bool {
        self.prepared
            .lock()
            .is_ok_and(|prepared| prepared.contains(raw))
    }

    fn mark_prepared(&self, raw: &str) {
        if let Ok(mut prepared) = self.prepared.lock() {
            prepared.insert(raw.to_owned());
        }
    }
}

/// Whether a path can name an indexed field: Mongo reads a segment starting
/// with `$` as an operator.
fn indexable(path: &str) -> bool {
    !path.split('.').any(|segment| segment.starts_with('$'))
}

/// The Mongo indexes for a declaration's [`IndexSpec`](tinystoragedrivers_core::IndexSpec)s.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
/// for a unique index over a path MongoDB cannot index. A non-unique index is
/// only a hint, so such an index is skipped instead.
pub(crate) fn index_models(spec: &CollectionSpec) -> Result<Vec<IndexModel>> {
    let mut models = Vec::new();
    for index in &spec.indexes {
        if !index.fields.iter().all(|field| indexable(field)) {
            if index.unique {
                return Err(StorageError::invalid_input(format!(
                    "unique index `{}` names a field segment starting with `$`, which MongoDB cannot index",
                    index.name
                )));
            }
            continue;
        }
        let mut keys = doc! {SCOPE: 1};
        let mut present = Document::new();
        for field in &index.fields {
            keys.insert(body_field(field), 1);
            present.insert(body_field(field), doc! {"$exists": true});
        }
        let mut options = IndexOptions::builder()
            .name(index_name(&index.name))
            .build();
        if index.unique {
            options.unique = Some(true);
            options.partial_filter_expression = Some(present);
        }
        models.push(IndexModel::builder().keys(keys).options(options).build());
    }
    Ok(models)
}

/// The text index over a declaration's search fields, or `None` when it
/// declares no fields (nothing is searchable, and `search` answers empty).
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
/// for a field MongoDB cannot index, rather than silently leaving it
/// unsearchable.
pub(crate) fn text_model(search: &SearchSpec) -> Result<Option<IndexModel>> {
    if let Some(field) = search.fields.iter().find(|field| !indexable(field)) {
        return Err(StorageError::invalid_input(format!(
            "search field `{field}` has a segment starting with `$`, which MongoDB cannot index"
        )));
    }
    if search.fields.is_empty() {
        return Ok(None);
    }
    let mut keys = doc! {SCOPE: 1};
    for field in &search.fields {
        keys.insert(body_field(field), "text");
    }
    let mut options = IndexOptions::builder().name(TEXT_INDEX.to_owned()).build();
    // No stemming and no stop words, closest to the reference tokenizer; the
    // override names a field no stored document has.
    options.default_language = Some("none".to_owned());
    options.language_override = Some("_tsd_language".to_owned());
    Ok(Some(
        IndexModel::builder().keys(keys).options(options).build(),
    ))
}

/// The `(_scope, _key)` index every document collection carries.
pub(crate) fn scope_key_model() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {SCOPE: 1, KEY: 1})
        .options(
            IndexOptions::builder()
                .name(SCOPE_KEY_INDEX.to_owned())
                .build(),
        )
        .build()
}

/// A declaration stored in `_tsd_meta` and its revision.
///
/// # Errors
///
/// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
/// for a malformed entry.
pub(crate) fn decode_meta(entry: &Document) -> Result<(CollectionSpec, i64)> {
    let malformed = || StorageError::serialization("malformed collection declaration in _tsd_meta");
    let spec = entry.get_str("spec").map_err(|_| malformed())?;
    let rev = entry.get_i64("rev").map_err(|_| malformed())?;
    Ok((serde_json::from_str(spec)?, rev))
}

/// How many times a declaration retries a lost compare-and-swap.
const ENSURE_ATTEMPTS: usize = 16;

impl Shared {
    /// Bury every expired document of `spec`'s collection, in every scope,
    /// before a unique index is built over it.
    ///
    /// An expired document reads as absent, so the memory driver ignores it
    /// when checking a new unique index; MongoDB's index build would count
    /// it. This is the one deliberately collection-wide write: it belongs to
    /// a declaration, which the spec makes collection-wide, and it only
    /// buries documents their own scope can no longer see (keeping their
    /// versions).
    async fn retire_expired(&self, spec: &CollectionSpec) -> Result<()> {
        let Some(expired) = expired_filter(spec, self.now()) else {
            return Ok(());
        };
        let collection = self.raw(&spec.name);
        let rows: Vec<Document> = collection
            .find(visible(translate::filter(&expired).query))
            .await
            .map_err(errors::failed("find expired documents"))?
            .try_collect()
            .await
            .map_err(errors::failed("find expired documents"))?;
        let mut selectors = Vec::new();
        for row in &rows {
            let stored = Stored::decode(row)?;
            if !stored.deleted && expired.matches(&stored.key, &stored.body) {
                selectors.push(doc! {
                    "_id": row.get("_id").cloned().unwrap_or(Bson::Null),
                    VERSION: row.get(VERSION).cloned().unwrap_or(Bson::Null),
                });
            }
        }
        for chunk in selectors.chunks(256) {
            collection
                .update_many(doc! {"$and": [not_deleted(), {"$or": chunk}]}, bury())
                .await
                .map_err(errors::failed("bury expired documents"))?;
        }
        Ok(())
    }

    /// The declaration of `collection`, or an empty one.
    ///
    /// # Errors
    ///
    /// A backend error, or a malformed stored declaration.
    pub(crate) async fn spec(&self, collection: &str) -> Result<CollectionSpec> {
        if let Some(spec) = self.specs.cached(collection) {
            return Ok(spec);
        }
        let stored = self
            .raw(META)
            .find_one(doc! {"_id": collection})
            .await
            .map_err(errors::failed("read a collection declaration"))?;
        let spec = match stored {
            Some(entry) => decode_meta(&entry)?.0,
            None => CollectionSpec::new(collection),
        };
        self.specs.store(spec.clone());
        Ok(spec)
    }

    /// Build `models` on the Mongo collection `name` once per process.
    ///
    /// # Errors
    ///
    /// A backend error.
    pub(crate) async fn prepare(&self, name: &str, models: Vec<IndexModel>) -> Result<()> {
        let key = format!("{}{name}", self.prefix);
        if self.specs.is_prepared(&key) {
            return Ok(());
        }
        self.raw(name)
            .create_indexes(models)
            .await
            .map_err(errors::failed("create the driver's indexes"))?;
        self.specs.mark_prepared(&key);
        Ok(())
    }

    /// Merge `spec` into the stored declaration and build its indexes.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for a spec that does not merge,
    /// [`ErrorKind::AlreadyExists`](tinystoragedrivers_core::ErrorKind::AlreadyExists)
    /// when stored documents violate a new unique index, or a backend error.
    pub(crate) async fn ensure(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let meta = self.raw(META);
        for _ in 0..ENSURE_ATTEMPTS {
            let stored = meta
                .find_one(doc! {"_id": &spec.name})
                .await
                .map_err(errors::failed("read a collection declaration"))?;
            let existing = stored.as_ref().map(decode_meta).transpose()?;
            let merged = match &existing {
                Some((have, _)) => have.merge(spec)?,
                None => spec.clone(),
            };
            self.build_indexes(&merged, existing.as_ref().map(|(have, _)| have))
                .await?;
            if existing.as_ref().is_some_and(|(have, _)| *have == merged) {
                self.specs.store(merged);
                return Ok(());
            }
            let encoded = serde_json::to_string(&merged)?;
            let written = match existing {
                None => match meta
                    .insert_one(doc! {"_id": &spec.name, "spec": &encoded, "rev": 1_i64})
                    .await
                {
                    Ok(_) => true,
                    Err(error) if errors::duplicate_index(&error).is_some() => false,
                    Err(error) => return Err(errors::map(error, "declare a collection")),
                },
                Some((_, rev)) => {
                    meta.update_one(
                        doc! {"_id": &spec.name, "rev": rev},
                        doc! {"$set": {"spec": &encoded, "rev": rev + 1}},
                    )
                    .await
                    .map_err(errors::failed("declare a collection"))?
                    .matched_count
                        == 1
                }
            };
            if written {
                self.specs.store(merged);
                return Ok(());
            }
        }
        Err(StorageError::unavailable(
            "collection declaration kept changing concurrently",
        ))
    }

    async fn build_indexes(
        &self,
        merged: &CollectionSpec,
        previous: Option<&CollectionSpec>,
    ) -> Result<()> {
        let collection = self.raw(&merged.name);
        let mut models = vec![scope_key_model()];
        models.extend(index_models(merged)?);
        // Validate the text index before building anything.
        let text = merged
            .search
            .as_ref()
            .map(text_model)
            .transpose()?
            .flatten();
        let adds_unique = merged
            .indexes
            .iter()
            .any(|index| index.unique && previous.is_none_or(|have| !have.indexes.contains(index)));
        if adds_unique {
            self.retire_expired(merged).await?;
        }
        collection
            .create_indexes(models)
            .await
            .map_err(errors::failed("build the declared indexes"))?;
        self.specs
            .mark_prepared(&format!("{}{}", self.prefix, merged.name));
        // Search fields only accumulate (`CollectionSpec::merge`), so a
        // declaration never goes from searchable to not searchable.
        let Some(model) = text else {
            return Ok(());
        };
        if previous.and_then(|have| have.search.as_ref()) != merged.search.as_ref() {
            // A collection holds one text index; its fields change by
            // rebuilding it. Dropping a missing index is not an error here.
            let _ = collection.drop_index(TEXT_INDEX).await;
        }
        collection
            .create_index(model)
            .await
            .map_err(errors::failed("build the text index"))?;
        Ok(())
    }
}
