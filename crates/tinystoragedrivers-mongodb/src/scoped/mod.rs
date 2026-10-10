//! The one door to a MongoDB collection: every read, write and pipeline is
//! narrowed to a single tenant here.
//!
//! [`ScopedCollection`] owns its [`Collection`] privately and takes only the
//! *inner* filter from callers. [`ScopedCollection::filter`] ANDs the scope
//! equality onto it, inserts and replacements are stamped with the scope, and
//! an aggregation's first stage is a scoped `$match`. No other module holds a
//! collection handle for tenant data, so a call site cannot forget the scope.
//!
//! Two rules keep later steps from undoing that:
//! - an aggregation may only use single-collection stages ([`STAGES`]), so no
//!   `$lookup`, `$unionWith`, `$merge` or `$out` can read or write elsewhere;
//! - an update must be operator-style and must not touch `_scope`
//!   ([`check_update`]), so a document can never be moved out of its scope.
//!
//! GridFS files carry their scope under `metadata`; [`scoped_filter`] is the
//! same rule over an arbitrary scope field.

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::error::{Error, Result};
use mongodb::options::{AggregateOptions, FindOptions};
use mongodb::results::UpdateResult;
use mongodb::{ClientSession, Collection};
use tinystoragedrivers_core::Scope;

use crate::naming::SCOPE;

/// The aggregation stages a scoped pipeline may use: each reads and writes
/// only the documents flowing through it.
pub(crate) const STAGES: [&str; 10] = [
    "$match",
    "$addFields",
    "$set",
    "$project",
    "$unset",
    "$sort",
    "$skip",
    "$limit",
    "$group",
    "$count",
];

/// Refuse a pipeline stage outside [`STAGES`].
///
/// # Errors
///
/// A custom MongoDB error (mapped to a backend error) naming the stage kind.
pub(crate) fn check_stage(stage: &Document) -> Result<()> {
    match stage.keys().next() {
        Some(kind) if stage.len() == 1 && STAGES.contains(&kind.as_str()) => Ok(()),
        _ => Err(Error::custom(
            "aggregation stage not allowed on scoped data".to_owned(),
        )),
    }
}

/// Refuse an update that is not operator-style or that touches the scope
/// field, so no update can move a document to another scope or drop it.
///
/// # Errors
///
/// A custom MongoDB error (mapped to a backend error).
pub(crate) fn check_update(update: &Document) -> Result<()> {
    let touches_scope = |path: &str| path == SCOPE || path.starts_with(&format!("{SCOPE}."));
    for (operator, fields) in update {
        let (true, Bson::Document(fields)) = (operator.starts_with('$'), fields) else {
            return Err(Error::custom(
                "scoped updates must be operator documents".to_owned(),
            ));
        };
        if fields.keys().any(|path| touches_scope(path)) {
            return Err(Error::custom(
                "scoped updates must not change the scope".to_owned(),
            ));
        }
    }
    Ok(())
}

/// `inner`, restricted to documents whose `field` equals `scope`.
pub(crate) fn scoped_filter(field: &str, scope: &Scope, inner: Document) -> Document {
    let equality = doc! {field: scope.as_str()};
    if inner.is_empty() {
        equality
    } else {
        doc! {"$and": [equality, inner]}
    }
}

/// A collection handle that only ever touches one scope's documents.
#[derive(Debug, Clone)]
pub(crate) struct ScopedCollection {
    collection: Collection<Document>,
    scope: Scope,
}

impl ScopedCollection {
    pub(crate) fn new(collection: Collection<Document>, scope: Scope) -> Self {
        Self { collection, scope }
    }

    /// `inner` restricted to this scope.
    pub(crate) fn filter(&self, inner: Document) -> Document {
        scoped_filter(SCOPE, &self.scope, inner)
    }

    /// `doc` carrying this scope.
    pub(crate) fn stamp(&self, mut doc: Document) -> Document {
        doc.insert(SCOPE, self.scope.as_str());
        doc
    }

    /// A pipeline whose first stage selects this scope's documents matching
    /// `inner`.
    pub(crate) fn pipeline(&self, inner: Document, stages: Vec<Document>) -> Vec<Document> {
        let mut pipeline = vec![doc! {"$match": self.filter(inner)}];
        pipeline.extend(stages);
        pipeline
    }

    pub(crate) async fn find_one(
        &self,
        inner: Document,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<Document>> {
        let action = self.collection.find_one(self.filter(inner));
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
    }

    /// Every matching document, in `options`' order.
    pub(crate) async fn find(
        &self,
        inner: Document,
        options: FindOptions,
    ) -> Result<Vec<Document>> {
        self.collection
            .find(self.filter(inner))
            .with_options(options)
            .await?
            .try_collect()
            .await
    }

    /// [`Self::find`], inside `session`'s transaction when there is one.
    pub(crate) async fn find_in(
        &self,
        inner: Document,
        options: FindOptions,
        session: Option<&mut ClientSession>,
    ) -> Result<Vec<Document>> {
        let Some(session) = session else {
            return self.find(inner, options).await;
        };
        let mut cursor = self
            .collection
            .find(self.filter(inner))
            .with_options(options)
            .session(&mut *session)
            .await?;
        cursor.stream(session).try_collect().await
    }

    /// Open a cursor over matching documents, for callers that stop early.
    pub(crate) async fn cursor(
        &self,
        inner: Document,
        options: FindOptions,
    ) -> Result<mongodb::Cursor<Document>> {
        self.collection
            .find(self.filter(inner))
            .with_options(options)
            .await
    }

    pub(crate) async fn aggregate(
        &self,
        inner: Document,
        stages: Vec<Document>,
    ) -> Result<Vec<Document>> {
        stages.iter().try_for_each(check_stage)?;
        self.collection
            .aggregate(self.pipeline(inner, stages))
            .with_options(AggregateOptions::builder().allow_disk_use(true).build())
            .await?
            .try_collect()
            .await
    }

    pub(crate) async fn count(&self, inner: Document) -> Result<u64> {
        self.collection.count_documents(self.filter(inner)).await
    }

    /// Insert `doc`, stamped with this scope.
    pub(crate) async fn insert_one(
        &self,
        doc: Document,
        session: Option<&mut ClientSession>,
    ) -> Result<()> {
        let action = self.collection.insert_one(self.stamp(doc));
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
        .map(|_| ())
    }

    /// Replace the matching document with `doc` (stamped with this scope) and
    /// report whether one matched.
    pub(crate) async fn replace_one(
        &self,
        inner: Document,
        doc: Document,
        session: Option<&mut ClientSession>,
    ) -> Result<bool> {
        let action = self
            .collection
            .replace_one(self.filter(inner), self.stamp(doc));
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
        .map(|result| result.matched_count == 1)
    }

    /// Apply `update` to the first matching document. With `upsert`, the
    /// inserted document takes its scope from the filter's equality.
    ///
    /// # Errors
    ///
    /// An update [`check_update`] refuses, or a server error.
    pub(crate) async fn update_one(
        &self,
        inner: Document,
        update: Document,
        upsert: bool,
        session: Option<&mut ClientSession>,
    ) -> Result<UpdateResult> {
        check_update(&update)?;
        let action = self
            .collection
            .update_one(self.filter(inner), update)
            .upsert(upsert);
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
    }

    /// Apply `update` to every matching document and report how many
    /// changed.
    ///
    /// # Errors
    ///
    /// An update [`check_update`] refuses, or a server error.
    pub(crate) async fn update_many(
        &self,
        inner: Document,
        update: Document,
        session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        check_update(&update)?;
        let action = self.collection.update_many(self.filter(inner), update);
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
        .map(|result| result.modified_count)
    }

    pub(crate) async fn delete_many(
        &self,
        inner: Document,
        session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        let action = self.collection.delete_many(self.filter(inner));
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
        .map(|result| result.deleted_count)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
