//! The one door to a MongoDB collection: every read, write and pipeline is
//! narrowed to a single tenant here.
//!
//! [`ScopedCollection`] owns its [`Collection`] privately and takes only the
//! *inner* filter from callers. [`ScopedCollection::filter`] ANDs the scope
//! equality onto it, inserts are stamped with the scope, and an aggregation's
//! first stage is a scoped `$match`. No other module holds a collection
//! handle for tenant data, so a call site cannot forget the scope.
//!
//! GridFS files carry their scope under `metadata`; [`scoped_filter`] is the
//! same rule over an arbitrary scope field.

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::error::Result;
use mongodb::options::{AggregateOptions, FindOptions, UpdateModifications};
use mongodb::results::UpdateResult;
use mongodb::{ClientSession, Collection};
use tinystoragedrivers_core::Scope;

use crate::naming::SCOPE;

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

    /// The Mongo collection name.
    pub(crate) fn name(&self) -> &str {
        self.collection.name()
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

    pub(crate) async fn distinct(&self, field: &str, inner: Document) -> Result<Vec<Bson>> {
        self.collection.distinct(field, self.filter(inner)).await
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

    /// Update the first matching document. With `upsert`, the inserted
    /// document takes the scope from the filter's equality.
    pub(crate) async fn update_one(
        &self,
        inner: Document,
        update: impl Into<UpdateModifications>,
        upsert: bool,
        session: Option<&mut ClientSession>,
    ) -> Result<UpdateResult> {
        let action = self
            .collection
            .update_one(self.filter(inner), update)
            .upsert(upsert);
        match session {
            Some(session) => action.session(session).await,
            None => action.await,
        }
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
