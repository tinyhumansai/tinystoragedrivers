use std::cmp::Ordering;

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::IndexOptions;
use mongodb::{Client, ClientSession, Collection, Database, IndexModel};
use serde_json::{Map, Number, Value};
use tinystoragedrivers_api::{DocumentStore, DocumentWatch, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, Filter, Meta, Page,
    Precondition, Query, ReadCheck, Result, Scope, SearchHit, Sort, StorageError, Version,
    Versioned, WatchOptions, WriteCondition, WriteOp, WriteOptions, WriteResult,
    sort_documents_with_meta, validate_collection, validate_doc, validate_id,
};

use tinystoragedrivers_connect::MongoConnection;

const SCOPE: &str = "_scope";
const KEY: &str = "_key";
const VERSION: &str = "_v";
const BODY: &str = "d";
const SCHEMA: &str = "_schema";
const CREATED: &str = "_c";
const UPDATED: &str = "_u";
const EXPIRES: &str = "_x";
const DELETED: &str = "_del";
const META_COLLECTION: &str = "_tsd_meta";
const TEXT_INDEX: &str = "_tsd_text";

#[derive(Clone)]
/// MongoDB document adapter bound to one shared connection and opaque scope.
pub struct MongoDocuments {
    pub(super) client: Client,
    pub(super) database: Database,
    pub(super) scope: Scope,
    pub(super) clock: crate::Clock,
    pub(super) transactions: bool,
    pub(super) origin: mongodb::bson::oid::ObjectId,
    pub(super) condition: Option<WriteCondition>,
}

impl std::fmt::Debug for MongoDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDocuments")
            .field("database", &self.database.name())
            .field("scope", &self.scope)
            .field("transactions", &self.transactions)
            .finish_non_exhaustive()
    }
}

impl MongoDocuments {
    /// Bind the v2 document port to a shared Mongo connection and opaque scope.
    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the constructor consumes a shared connection handle by contract"
    )]
    pub fn new(connection: MongoConnection, scope: Scope) -> Self {
        Self {
            client: connection.client(),
            database: connection.database(),
            scope,
            clock: connection.clock(),
            transactions: connection.transactions_available(),
            origin: connection.origin(),
            condition: None,
        }
    }

    /// Bind this document port to a host condition checked in every write
    /// transaction. The connection must support MongoDB transactions.
    ///
    /// # Errors
    ///
    /// Returns `Unsupported` when the connection cannot run transactions.
    pub fn conditioned(
        connection: MongoConnection,
        scope: Scope,
        condition: WriteCondition,
    ) -> Result<Self> {
        if !connection.transactions_available() {
            return Err(StorageError::unsupported(
                Capability::ConditionalWrites,
                "conditioned MongoDB documents require transactions",
            ));
        }
        let mut documents = Self::new(connection, scope);
        documents.condition = Some(condition);
        Ok(documents)
    }

    /// Check and transactionally hold this handle's host write condition.
    ///
    /// This is exposed for sibling native engines that must mutate their own
    /// collection in the same MongoDB transaction as the document guard.
    /// It must only be called with the active transaction session used for
    /// the guarded mutation.
    ///
    /// # Errors
    ///
    /// Returns `Conflict` when the condition is absent, expired, or no longer
    /// matches, and propagates database errors from the transaction.
    pub async fn check_condition(&self, session: &mut ClientSession) -> Result<()> {
        let Some(condition) = &self.condition else {
            return Ok(());
        };
        for clause in condition.clauses() {
            let spec = self.collection_spec(clause.collection()).await?;
            let filter = doc! {"_id": {"s": clause.scope().as_str(), "k": clause.id()}, DELETED: {"$ne": true}};
            let record = self
                .collection(clause.collection())
                .find_one(filter.clone())
                .session(&mut *session)
                .await
                .map_err(|error| transaction_error("read a write condition", error))?
                .ok_or_else(|| StorageError::conflict("write condition document is absent"))?;
            let now_ms = (self.clock)();
            let row = decode(&record)?
                .filter(|row| !expired(&spec, row, now_ms))
                .ok_or_else(|| {
                    StorageError::conflict("write condition document is absent or expired")
                })?;
            if !clause.matches_at(
                &row.id,
                &row.doc,
                &row.meta,
                i64::try_from(now_ms).unwrap_or(i64::MAX),
            ) {
                return Err(StorageError::conflict(
                    "write condition did not match or is expired",
                ));
            }

            // A read-only predicate in a Mongo snapshot transaction can race a
            // concurrent guard replacement. Touch every private guard envelope
            // so each takeover conflicts with this transaction. This field is
            // outside logical document bodies and public metadata.
            let version = i64::try_from(row.version.0)
                .map_err(|_| StorageError::serialization("condition version is out of range"))?;
            let touched = self
                .collection(clause.collection())
                .update_one(
                    doc! {"_id": {"s": clause.scope().as_str(), "k": clause.id()}, VERSION: version, DELETED: {"$ne": true}},
                    doc! {"$inc": {"_tsd_condition_touch": 1_i64}},
                )
                .session(&mut *session)
                .await
                .map_err(|error| transaction_error("hold a write condition", error))?;
            if touched.matched_count != 1 {
                return Err(StorageError::conflict("write condition changed"));
            }
        }
        Ok(())
    }

    fn collection(&self, collection: &str) -> Collection<Document> {
        self.database.collection(collection)
    }

    fn scoped_id(&self, id: &str) -> Document {
        doc! {"s": self.scope.as_str(), "k": id}
    }

    async fn collection_spec(&self, collection: &str) -> Result<CollectionSpec> {
        let stored = self
            .database
            .collection::<Document>(META_COLLECTION)
            .find_one(doc! {"_id": collection})
            .await
            .map_err(mongo_error("read a collection declaration"))?;
        match stored {
            Some(record) => {
                let (spec, _) = decode_spec(&record)?;
                if spec.name != collection {
                    return Err(StorageError::serialization(
                        "collection declaration names a different collection",
                    ));
                }
                spec.validate()?;
                Ok(spec)
            }
            None => Ok(CollectionSpec::new(collection)),
        }
    }

    async fn put_with_session(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        options: WriteOptions,
        mut session: Option<&mut ClientSession>,
    ) -> Result<Version> {
        if let Some(session) = session.as_deref_mut() {
            self.check_condition(session).await?;
        } else if self.condition.is_some() {
            return Err(StorageError::backend(
                "conditioned document write has no transaction",
            ));
        }
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&value)?;
        let spec = self.collection_spec(collection).await?;
        let table = self.collection(collection);
        let filter = doc! {"_id": self.scoped_id(id)};
        let in_transaction = session.is_some();
        let old = match session.as_deref_mut() {
            Some(session) => table
                .find_one(filter.clone())
                .session(session)
                .await
                .map_err(|error| {
                    if in_transaction {
                        transaction_error("read before writing a document", error)
                    } else {
                        mongo_error("read before writing a document")(error)
                    }
                })?,
            None => table
                .find_one(filter.clone())
                .await
                .map_err(mongo_error("read before writing a document"))?,
        };
        let current = old
            .as_ref()
            .map(decode)
            .transpose()?
            .flatten()
            .filter(|row| !expired(&spec, row, (self.clock)()));
        check(precondition, current.as_ref().map(|row| row.version))?;
        let version = match old
            .as_ref()
            .and_then(|row| row.get_i64(VERSION).ok())
            .and_then(|v| u64::try_from(v).ok())
        {
            Some(v) => Version(v)
                .next()
                .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
            None => Version::FIRST,
        };
        let now = i64::try_from((self.clock)()).unwrap_or(i64::MAX);
        let meta = Meta {
            schema: options.schema,
            created_ms: options
                .created_ms
                .unwrap_or_else(|| current.as_ref().map_or(now, |row| row.meta.created_ms)),
            updated_ms: current
                .as_ref()
                .map_or(now, |row| now.max(row.meta.updated_ms)),
            expires_ms: options.expires_ms,
        };
        let record = encode(&self.scope, id, version, &value, &meta)?;
        let expected = old.as_ref().and_then(|row| row.get_i64(VERSION).ok());
        let mut cas = filter;
        if let Some(expected) = expected {
            cas.insert(VERSION, expected);
        } else {
            // Keep the absence check in the atomic write filter. Checking only
            // before the replace lets a concurrent write be overwritten
            // without advancing its version.
            cas.insert(VERSION, doc! {"$exists": false});
        }
        let upsert = old.is_none();
        let result = match session {
            Some(session) => {
                table
                    .replace_one(cas, record)
                    .upsert(upsert)
                    .session(session)
                    .await
            }
            None => table.replace_one(cas, record).upsert(upsert).await,
        }
        .map_err(map_document_write_error)?;
        if result.matched_count == 0 && result.upserted_id.is_none() {
            return Err(StorageError::conflict("document changed during write"));
        }
        Ok(version)
    }

    /// Applies one batch operation inside the batch's transaction. A plain
    /// `Put` writes the default options; `PutWith` writes its own.
    async fn batch_op(&self, op: WriteOp, session: &mut ClientSession) -> Result<WriteResult> {
        Ok(match op {
            WriteOp::Put {
                collection,
                id,
                doc,
                precondition,
            } => WriteResult::Put {
                version: self
                    .put_with_session(
                        &collection,
                        &id,
                        doc,
                        precondition,
                        WriteOptions::default(),
                        Some(session),
                    )
                    .await?,
            },
            WriteOp::PutWith {
                collection,
                id,
                doc,
                precondition,
                options,
            } => WriteResult::Put {
                version: self
                    .put_with_session(&collection, &id, doc, precondition, options, Some(session))
                    .await?,
            },
            WriteOp::Delete {
                collection,
                id,
                precondition,
            } => WriteResult::Delete {
                removed: self
                    .delete_with_session(&collection, &id, precondition, Some(session))
                    .await?,
            },
        })
    }

    async fn delete_with_session(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
        mut session: Option<&mut ClientSession>,
    ) -> Result<bool> {
        if let Some(session) = session.as_deref_mut() {
            self.check_condition(session).await?;
        } else if self.condition.is_some() {
            return Err(StorageError::backend(
                "conditioned document delete has no transaction",
            ));
        }
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.collection_spec(collection).await?;
        let table = self.collection(collection);
        let filter = doc! {"_id": self.scoped_id(id)};
        let in_transaction = session.is_some();
        let old = match session.as_deref_mut() {
            Some(session) => table
                .find_one(filter.clone())
                .session(session)
                .await
                .map_err(|error| {
                    if in_transaction {
                        transaction_error("read before deleting a document", error)
                    } else {
                        mongo_error("read before deleting a document")(error)
                    }
                })?,
            None => table
                .find_one(filter.clone())
                .await
                .map_err(mongo_error("read before deleting a document"))?,
        };
        let Some(old) = old else {
            check(precondition, None)?;
            return Ok(false);
        };
        let live = decode(&old)?.filter(|row| !expired(&spec, row, (self.clock)()));
        check(precondition, live.as_ref().map(|row| row.version))?;
        let Some(version) = old.get_i64(VERSION).ok() else {
            return Err(StorageError::serialization(
                "stored document has no version",
            ));
        };
        let result = match session {
            Some(session) => {
                table
                    .update_one(
                        doc! {"_id": self.scoped_id(id), VERSION: version},
                        doc! {"$set": {BODY: {}, DELETED: true}},
                    )
                    .session(session)
                    .await
            }
            None => {
                table
                    .update_one(
                        doc! {"_id": self.scoped_id(id), VERSION: version},
                        doc! {"$set": {BODY: {}, DELETED: true}},
                    )
                    .await
            }
        }
        .map_err(map_document_write_error)?;
        if result.matched_count == 0 {
            return Err(StorageError::conflict("document changed during delete"));
        }
        Ok(live.is_some())
    }

    async fn rows(&self, collection: &str) -> Result<Vec<Versioned<Value>>> {
        let mut cursor = self
            .collection(collection)
            .find(doc! {SCOPE: self.scope.as_str()})
            .await
            .map_err(mongo_error("read documents"))?;
        let mut rows = Vec::new();
        while let Some(record) = cursor
            .try_next()
            .await
            .map_err(mongo_error("read documents"))?
        {
            if let Some(row) = decode(&record)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn build_indexes(
        &self,
        spec: &CollectionSpec,
        previous: Option<&CollectionSpec>,
    ) -> Result<()> {
        let mut indexes = vec![scope_key_index()];
        indexes.extend(spec_indexes(spec)?);
        self.collection(&spec.name)
            .create_indexes(indexes)
            .await
            .map_err(map_index_build_error)?;
        let Some(text) = spec.search.as_ref().map(text_index).transpose()?.flatten() else {
            return Ok(());
        };
        if previous.and_then(|old| old.search.as_ref()) != spec.search.as_ref() {
            let _ = self.collection(&spec.name).drop_index(TEXT_INDEX).await;
        }
        self.collection(&spec.name)
            .create_index(text)
            .await
            .map_err(mongo_error("build the full-text index"))?;
        Ok(())
    }
}

#[async_trait]
impl DocumentStore for MongoDocuments {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::Documents)
            .with(Capability::Ttl)
            .with(Capability::FullText)
            .with(Capability::RecordMetadata)
            .with(Capability::CollectionEnumeration)
            .with(Capability::CollectionValidation)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas);
        if self.transactions {
            let capabilities = capabilities
                .with(Capability::Transactions)
                .with(Capability::ValidatedBatch)
                .with(Capability::Watch);
            if self.condition.is_some() {
                capabilities.with(Capability::ConditionalWrites)
            } else {
                capabilities
            }
        } else {
            capabilities
        }
    }

    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        self.open_watch(collection, filter, options).await
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        if self.condition.is_some() {
            return Err(StorageError::unsupported(
                Capability::ConditionalWrites,
                "conditioned MongoDB handles cannot mutate collection declarations",
            ));
        }
        spec.validate()?;
        let metadata = self.database.collection::<Document>(META_COLLECTION);
        for _ in 0..16 {
            let stored = metadata
                .find_one(doc! {"_id": &spec.name})
                .await
                .map_err(mongo_error("read a collection declaration"))?;
            let existing = stored.as_ref().map(decode_spec).transpose()?;
            let merged = match &existing {
                Some((old, _)) => old.merge(spec)?,
                None => spec.clone(),
            };
            self.build_indexes(&merged, existing.as_ref().map(|(old, _)| old))
                .await?;
            if existing.as_ref().is_some_and(|(old, _)| old == &merged) {
                return Ok(());
            }
            let encoded = serde_json::to_string(&merged)
                .map_err(|error| StorageError::serialization(error.to_string()))?;
            match existing {
                Some((_, revision)) => {
                    let result = metadata
                        .update_one(
                            doc! {"_id": &spec.name, "rev": revision},
                            doc! {"$set": {"spec": &encoded, "rev": revision + 1}},
                        )
                        .await
                        .map_err(mongo_error("update a collection declaration"))?;
                    if result.matched_count == 1 {
                        return Ok(());
                    }
                }
                None => match metadata
                    .insert_one(doc! {"_id": &spec.name, "spec": &encoded, "rev": 1_i64})
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if error.to_string().contains("E11000") => {}
                    Err(error) => {
                        return Err(mongo_error("store a collection declaration")(error));
                    }
                },
            }
        }
        Err(StorageError::unavailable(
            "collection declaration kept changing concurrently",
        ))
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let found = self
            .database
            .collection::<Document>(META_COLLECTION)
            .find_one(doc! {"_id": &required.name})
            .await
            .map_err(mongo_error("read a collection declaration"))?
            .ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
        let (stored, _) = decode_spec(&found)?;
        if stored.merge(required)? != stored {
            return Err(StorageError::invalid_input(
                "stored declaration lacks required features",
            ));
        }
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.collection_spec(collection).await?;
        let Some(record) = self
            .collection(collection)
            .find_one(doc! {"_id": self.scoped_id(id), DELETED: {"$ne": true}})
            .await
            .map_err(mongo_error("read a document"))?
        else {
            return Ok(None);
        };
        let row = decode(&record)?;
        Ok(row.filter(|found| !expired(&spec, found, (self.clock)())))
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_with(collection, id, value, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        if self.condition.is_some() {
            let mut session = self
                .client
                .start_session()
                .await
                .map_err(mongo_error("start a conditioned document write"))?;
            session
                .start_transaction()
                .await
                .map_err(mongo_error("start a conditioned document write"))?;
            let result = self
                .put_with_session(
                    collection,
                    id,
                    value,
                    precondition,
                    options,
                    Some(&mut session),
                )
                .await;
            return match result {
                Ok(version) => {
                    session.commit_transaction().await.map_err(|error| {
                        transaction_error("commit a conditioned document write", error)
                    })?;
                    Ok(version)
                }
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    Err(error)
                }
            };
        }
        for attempt in 0..64 {
            match self
                .put_with_session(collection, id, value.clone(), precondition, options, None)
                .await
            {
                Err(error)
                    if precondition == Precondition::None
                        && error.kind() == tinystoragedrivers_types::ErrorKind::Conflict
                        && attempt < 63 => {}
                result => return result,
            }
        }
        Err(StorageError::unavailable(
            "MongoDB unconditional write retry limit reached",
        ))
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        if self.condition.is_some() {
            let mut session = self
                .client
                .start_session()
                .await
                .map_err(mongo_error("start a conditioned document delete"))?;
            session
                .start_transaction()
                .await
                .map_err(mongo_error("start a conditioned document delete"))?;
            let result = self
                .delete_with_session(collection, id, precondition, Some(&mut session))
                .await;
            return match result {
                Ok(removed) => {
                    session.commit_transaction().await.map_err(|error| {
                        transaction_error("commit a conditioned document delete", error)
                    })?;
                    Ok(removed)
                }
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    Err(error)
                }
            };
        }
        self.delete_with_session(collection, id, precondition, None)
            .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let spec = self.collection_spec(collection).await?;
        let mut rows = self.rows(collection).await?;
        let now = (self.clock)();
        rows.retain(|row| {
            !expired(&spec, row, now) && query.filter.matches_with(&row.id, &row.doc, &row.meta)
        });
        sort_documents_with_meta(&mut rows, &query.sort, |row| (&row.id, &row.doc, &row.meta));
        let fingerprint = fingerprint(collection, &self.scope, query);
        let after = if let Some(cursor) = &query.cursor {
            let position = cursor.decode_keyset("mongo", fingerprint)?;
            position.validate(&query.sort)?;
            Some(position)
        } else {
            None
        };
        if let Some(after) = after {
            rows.retain(|row| {
                CursorPosition::for_record(&row.id, &row.doc, &row.meta, &query.sort)
                    .compare(&after, &query.sort)
                    == Ordering::Greater
            });
        }
        let limit = query.limit.unwrap_or(rows.len());
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            rows.last()
                .map(|row| {
                    Cursor::keyset(
                        "mongo",
                        fingerprint,
                        &CursorPosition::for_record(&row.id, &row.doc, &row.meta, &query.sort),
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items: rows, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let spec = self.collection_spec(collection).await?;
        Ok(self
            .rows(collection)
            .await?
            .into_iter()
            .filter(|row| {
                !expired(&spec, row, (self.clock)())
                    && filter.matches_with(&row.id, &row.doc, &row.meta)
            })
            .count() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let spec = self.collection_spec(collection).await?;
        let rows = self.rows(collection).await?;
        let mut count = 0;
        for row in rows.into_iter().filter(|row| {
            !expired(&spec, row, (self.clock)())
                && filter.matches_with(&row.id, &row.doc, &row.meta)
        }) {
            count += u64::from(
                self.delete(collection, &row.id, Precondition::Version(row.version))
                    .await?,
            );
        }
        Ok(count)
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        validate_doc(patch)?;
        for _ in 0..64 {
            let query = Query {
                filter: filter.clone(),
                sort: sort.to_vec(),
                limit: Some(1),
                cursor: None,
            };
            let Some(mut first) = self
                .query(collection, &query)
                .await?
                .items
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            tinystoragedrivers_types::value::merge_patch(&mut first.doc, patch);
            let options = WriteOptions {
                schema: first.meta.schema,
                expires_ms: first.meta.expires_ms,
                created_ms: Some(first.meta.created_ms),
            };
            match self
                .put_with(
                    collection,
                    &first.id,
                    first.doc.clone(),
                    Precondition::Version(first.version),
                    options,
                )
                .await
            {
                Ok(version) => {
                    first.version = version;
                    return Ok(Some(first));
                }
                Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {}
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::unavailable(
            "MongoDB claim retry limit reached",
        ))
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        if !self.transactions {
            return Err(StorageError::unsupported(
                Capability::Transactions,
                "MongoDB deployment does not support transactions",
            ));
        }
        self.atomic_batch_checked(Vec::new(), ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        if !self.transactions {
            return Err(StorageError::unsupported(
                Capability::ValidatedBatch,
                "MongoDB deployment does not support validated batches",
            ));
        }
        for check in &checks {
            check.validate()?;
            if self.condition.is_none() {
                self.ensure_collection(&CollectionSpec::new(check.collection.as_str()))
                    .await?;
            }
        }
        for op in &ops {
            let collection = match op {
                WriteOp::Put { collection, .. }
                | WriteOp::PutWith { collection, .. }
                | WriteOp::Delete { collection, .. } => collection,
            };
            validate_collection(collection)?;
            if self.condition.is_none() {
                self.ensure_collection(&CollectionSpec::new(collection.as_str()))
                    .await?;
            }
        }

        let mut session = self
            .client
            .start_session()
            .await
            .map_err(mongo_error("start a document transaction"))?;
        session
            .start_transaction()
            .await
            .map_err(mongo_error("start a document transaction"))?;

        let result = async {
            for dependency in &checks {
                let spec = self.collection_spec(&dependency.collection).await?;
                let record = self
                    .collection(&dependency.collection)
                    .find_one(doc! {"_id": self.scoped_id(&dependency.id)})
                    .session(&mut session)
                    .await
                    .map_err(|error| transaction_error("validate a batch dependency", error))?;
                let current = record
                    .as_ref()
                    .map(decode)
                    .transpose()?
                    .flatten()
                    .filter(|row| !expired(&spec, row, (self.clock)()))
                    .map(|row| row.version);
                dependency.verify(current)?;
            }

            let mut results = Vec::with_capacity(ops.len());
            for op in ops {
                results.push(self.batch_op(op, &mut session).await?);
            }
            Ok::<_, StorageError>(results)
        }
        .await;

        match result {
            Ok(results) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(|error| transaction_error("commit a document transaction", error))?;
                Ok(results)
            }
            Err(error) => {
                let _ = session.abort_transaction().await;
                Err(error)
            }
        }
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        let spec = self.collection_spec(collection).await?;
        let search = spec.search.as_ref().ok_or_else(|| {
            StorageError::invalid_input("this collection declares no search fields")
        })?;
        if search.fields.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let tokens = tinystoragedrivers_types::value::tokens(&Value::String(text.to_owned()));
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let filter = doc! {
            SCOPE: self.scope.as_str(),
            DELETED: {"$ne": true},
            "$text": {"$search": tokens.join(" ")},
        };
        let options = mongodb::options::FindOptions::builder()
            .projection(doc! {
                "score": {"$meta": "textScore"},
                KEY: 1,
                VERSION: 1,
                BODY: 1,
                SCHEMA: 1,
                CREATED: 1,
                UPDATED: 1,
                EXPIRES: 1,
                DELETED: 1,
            })
            .sort(doc! {"score": {"$meta": "textScore"}, KEY: 1})
            .build();
        let mut cursor = self
            .collection(collection)
            .find(filter)
            .with_options(options)
            .await
            .map_err(mongo_error("search documents"))?;
        let now = (self.clock)();
        let mut hits = Vec::new();
        while let Some(record) = cursor
            .try_next()
            .await
            .map_err(mongo_error("search documents"))?
        {
            let Some(row) = decode(&record)? else {
                continue;
            };
            if expired(&spec, &row, now) {
                continue;
            }
            hits.push(SearchHit {
                id: row.id,
                score: record.get_f64("score").unwrap_or_default(),
            });
            if hits.len() == limit {
                break;
            }
        }
        Ok(hits)
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        if self.condition.is_some() {
            let mut session = self
                .client
                .start_session()
                .await
                .map_err(mongo_error("start a conditioned collection drop"))?;
            session
                .start_transaction()
                .await
                .map_err(mongo_error("start a conditioned collection drop"))?;
            let result = async {
                self.check_condition(&mut session).await?;
                let table = self.collection(collection);
                let mut cursor = table
                    .find(doc! {SCOPE: self.scope.as_str(), DELETED: {"$ne": true}})
                    .session(&mut session)
                    .await
                    .map_err(|error| transaction_error("read documents to drop a collection", error))?;
                while let Some(record) = cursor.next(&mut session).await {
                    let record = record
                        .map_err(|error| transaction_error("read documents to drop a collection", error))?;
                    let id = record
                        .get_str(KEY)
                        .map_err(|_| StorageError::serialization("stored document has no key"))?;
                    let version = record
                        .get_i64(VERSION)
                        .map_err(|_| StorageError::serialization("stored document has no version"))?;
                    let result = table
                        .update_one(
                            doc! {"_id": self.scoped_id(id), VERSION: version, DELETED: {"$ne": true}},
                            doc! {"$set": {BODY: {}, DELETED: true}},
                        )
                        .session(&mut session)
                        .await
                        .map_err(|error| transaction_error("drop scoped documents", error))?;
                    if result.matched_count != 1 {
                        return Err(StorageError::conflict("document changed during collection drop"));
                    }
                }
                Ok(())
            }
            .await;
            return match result {
                Ok(()) => session.commit_transaction().await.map_err(|error| {
                    transaction_error("commit a conditioned collection drop", error)
                }),
                Err(error) => {
                    let _ = session.abort_transaction().await;
                    Err(error)
                }
            };
        }
        let table = self.collection(collection);
        let mut cursor = table
            .find(doc! {SCOPE: self.scope.as_str()})
            .await
            .map_err(mongo_error("read documents to drop a collection"))?;
        while let Some(record) = cursor
            .try_next()
            .await
            .map_err(mongo_error("read documents to drop a collection"))?
        {
            if record.get_bool(DELETED).unwrap_or(false) {
                continue;
            }
            let id = record
                .get_str(KEY)
                .map_err(|_| StorageError::serialization("stored document has no key"))?;
            let version = record
                .get_i64(VERSION)
                .map_err(|_| StorageError::serialization("stored document has no version"))?;
            table
                .update_one(
                    doc! {"_id": self.scoped_id(id), VERSION: version},
                    doc! {"$set": {BODY: {}, DELETED: true}},
                )
                .await
                .map_err(mongo_error("drop scoped documents"))?;
        }
        Ok(())
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let mut names = self
            .database
            .list_collection_names()
            .await
            .map_err(mongo_error("list collections"))?;
        let mut scoped = Vec::new();
        for name in names
            .drain(..)
            .filter(|name| name.starts_with(prefix) && !name.starts_with("_tsd_"))
        {
            let has_rows = self
                .collection(&name)
                .find_one(doc! {SCOPE: self.scope.as_str()})
                .await
                .map_err(mongo_error("enumerate scoped collections"))?
                .is_some();
            if has_rows {
                scoped.push(name);
            }
        }
        names = scoped;
        names.sort();
        Ok(names)
    }
}

fn expired(spec: &CollectionSpec, row: &Versioned<Value>, now: u64) -> bool {
    let now = Value::from(now);
    Filter::lte("_expires_ms", now.clone()).matches_with(&row.id, &row.doc, &row.meta)
        || spec.ttl_field.as_ref().is_some_and(|field| {
            Filter::lte(field.clone(), now.clone()).matches_with(&row.id, &row.doc, &row.meta)
        })
}

fn check(precondition: Precondition, found: Option<Version>) -> Result<()> {
    match (precondition, found) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(_)) => Err(StorageError::conflict("document already exists")),
        (Precondition::Version(expected), Some(found)) if expected == found => Ok(()),
        (Precondition::Version(_), _) => Err(StorageError::conflict("document version changed")),
    }
}

fn encode(
    scope: &Scope,
    id: &str,
    version: Version,
    body: &Value,
    meta: &Meta,
) -> Result<Document> {
    let body = json_to_bson(body)?;
    let Bson::Document(body) = body else {
        return Err(StorageError::invalid_input(
            "document body must be an object",
        ));
    };
    let mut record = doc! {"_id": {"s": scope.as_str(), "k": id}, SCOPE: scope.as_str(), KEY: id, VERSION: i64::try_from(version.0).map_err(|_| StorageError::backend("document version space is exhausted"))?, BODY: body, SCHEMA: i64::from(meta.schema), CREATED: meta.created_ms, UPDATED: meta.updated_ms};
    if let Some(expires) = meta.expires_ms {
        record.insert(EXPIRES, expires);
    }
    Ok(record)
}

fn decode(record: &Document) -> Result<Option<Versioned<Value>>> {
    if record.get_bool(DELETED).unwrap_or(false) {
        return Ok(None);
    }
    let id = record
        .get_str(KEY)
        .map_err(|_| StorageError::serialization("stored document has no key"))?
        .to_owned();
    let version = record
        .get_i64(VERSION)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| StorageError::serialization("stored document has invalid version"))?;
    let body = record
        .get_document(BODY)
        .map_err(|_| StorageError::serialization("stored document has no body"))?;
    let meta = Meta {
        schema: record
            .get_i64(SCHEMA)
            .ok()
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0),
        created_ms: record.get_i64(CREATED).unwrap_or(0),
        updated_ms: record.get_i64(UPDATED).unwrap_or(0),
        expires_ms: record.get_i64(EXPIRES).ok(),
    };
    Ok(Some(Versioned {
        id,
        version: Version(version),
        doc: bson_to_json(&Bson::Document(body.clone()))?,
        meta,
    }))
}

pub(super) fn json_to_bson(value: &Value) -> Result<Bson> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(value) => Bson::Boolean(*value),
        Value::String(value) => Bson::String(value.clone()),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Bson::Int64(value)
            } else if let Some(value) = number.as_u64() {
                Bson::Int64(i64::try_from(value).map_err(|_| {
                    StorageError::serialization("MongoDB cannot store integers above i64::MAX")
                })?)
            } else {
                Bson::Double(number.as_f64().unwrap_or_default())
            }
        }
        Value::Array(values) => {
            Bson::Array(values.iter().map(json_to_bson).collect::<Result<_>>()?)
        }
        Value::Object(values) => Bson::Document(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_bson(value)?)))
                .collect::<Result<_>>()?,
        ),
    })
}

pub(super) fn bson_to_json(value: &Bson) -> Result<Value> {
    Ok(match value {
        Bson::Null => Value::Null,
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::String(value) => Value::String(value.clone()),
        Bson::Int32(value) => Value::from(*value),
        Bson::Int64(value) => Value::from(*value),
        Bson::Double(value) => Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| StorageError::serialization("MongoDB stored a non-finite number"))?,
        Bson::Array(values) => {
            Value::Array(values.iter().map(bson_to_json).collect::<Result<_>>()?)
        }
        Bson::Document(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), bson_to_json(value)?)))
                .collect::<Result<Map<_, _>>>()?,
        ),
        _ => {
            return Err(StorageError::serialization(
                "MongoDB stored a non-JSON value",
            ));
        }
    })
}

fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> u64 {
    let binding = serde_json::to_vec(&(collection, scope.as_str(), &query.filter, &query.sort))
        .unwrap_or_default();
    binding.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

pub(super) fn mongo_error(
    operation: &'static str,
) -> impl FnOnce(mongodb::error::Error) -> StorageError {
    move |error| {
        StorageError::unavailable(format!("MongoDB failed to {operation}")).with_source(error)
    }
}

fn map_document_write_error(error: mongodb::error::Error) -> StorageError {
    if let Some(index) = duplicate_key_index(&error) {
        if index.starts_with("_tsd_ix_") {
            return StorageError::already_exists("document violates a unique index")
                .with_source(error);
        }
        if index == "_id_" || index == "_tsd_scope_key" {
            return StorageError::conflict("document changed during write").with_source(error);
        }
    }
    if is_definite_transaction_conflict(
        mongo_error_code(&error),
        error.contains_label("TransientTransactionError"),
        error.contains_label("UnknownTransactionCommitResult"),
    ) {
        return StorageError::conflict("document changed during write").with_source(error);
    }
    mongo_error("write a document")(error)
}

fn transaction_error(operation: &'static str, error: mongodb::error::Error) -> StorageError {
    if is_definite_transaction_conflict(
        mongo_error_code(&error),
        error.contains_label("TransientTransactionError"),
        error.contains_label("UnknownTransactionCommitResult"),
    ) {
        StorageError::conflict("document transaction conflicted").with_source(error)
    } else {
        mongo_error(operation)(error)
    }
}

fn is_definite_transaction_conflict(
    code: Option<i32>,
    transient_transaction: bool,
    unknown_commit_result: bool,
) -> bool {
    !unknown_commit_result && (code == Some(112) || transient_transaction)
}

fn mongo_error_code(error: &mongodb::error::Error) -> Option<i32> {
    use mongodb::error::{ErrorKind as MongoErrorKind, WriteFailure};

    match error.kind.as_ref() {
        MongoErrorKind::Command(command) => Some(command.code),
        MongoErrorKind::Write(WriteFailure::WriteError(write)) => Some(write.code),
        MongoErrorKind::InsertMany(many) => {
            many.write_errors.as_ref()?.first().map(|write| write.code)
        }
        _ => None,
    }
}

fn map_index_build_error(error: mongodb::error::Error) -> StorageError {
    if duplicate_key_index(&error).is_some() {
        return StorageError::already_exists("existing documents violate a unique index")
            .with_source(error);
    }
    mongo_error("build collection indexes")(error)
}

fn duplicate_key_index(error: &mongodb::error::Error) -> Option<String> {
    use mongodb::error::{ErrorKind as MongoErrorKind, WriteFailure};

    let (code, message) = match error.kind.as_ref() {
        MongoErrorKind::Command(command) => (command.code, command.message.as_str()),
        MongoErrorKind::Write(WriteFailure::WriteError(write)) => {
            (write.code, write.message.as_str())
        }
        MongoErrorKind::InsertMany(many) => {
            let first = many.write_errors.as_ref()?.first()?;
            (first.code, first.message.as_str())
        }
        _ => return None,
    };
    if !matches!(code, 11000 | 11001 | 12582) {
        return None;
    }
    message
        .split_once("index: ")
        .and_then(|(_, rest)| rest.split_whitespace().next())
        .map(str::to_owned)
}

fn decode_spec(record: &Document) -> Result<(CollectionSpec, i64)> {
    let malformed = || StorageError::serialization("malformed collection declaration");
    let encoded = record.get_str("spec").map_err(|_| malformed())?;
    let revision = record.get_i64("rev").map_err(|_| malformed())?;
    let spec = serde_json::from_str(encoded)
        .map_err(|error| StorageError::serialization(error.to_string()))?;
    Ok((spec, revision))
}

fn scope_key_index() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {SCOPE: 1, KEY: 1})
        .options(
            IndexOptions::builder()
                .name("_tsd_scope_key".to_owned())
                .build(),
        )
        .build()
}

fn spec_indexes(spec: &CollectionSpec) -> Result<Vec<IndexModel>> {
    let mut indexes = Vec::new();
    for index in &spec.indexes {
        if !index
            .fields
            .iter()
            .all(|field| !field.split('.').any(|part| part.starts_with('$')))
        {
            if index.unique {
                return Err(StorageError::invalid_input(format!(
                    "unique index `{}` names a field MongoDB cannot index",
                    index.name
                )));
            }
            continue;
        }
        let mut keys = doc! {SCOPE: 1};
        let mut present = Document::new();
        for field in &index.fields {
            let path = format!("{BODY}.{field}");
            keys.insert(&path, 1);
            present.insert(path, doc! {"$exists": true});
        }
        let mut options = IndexOptions::builder()
            .name(index_name(&index.name))
            .build();
        if index.unique {
            options.unique = Some(true);
            options.partial_filter_expression = Some(present);
        }
        indexes.push(IndexModel::builder().keys(keys).options(options).build());
    }
    Ok(indexes)
}

fn text_index(search: &tinystoragedrivers_types::SearchSpec) -> Result<Option<IndexModel>> {
    if let Some(field) = search
        .fields
        .iter()
        .find(|field| field.split('.').any(|part| part.starts_with('$')))
    {
        return Err(StorageError::invalid_input(format!(
            "search field `{field}` names a path MongoDB cannot index"
        )));
    }
    if search.fields.is_empty() {
        return Ok(None);
    }
    let mut keys = doc! {SCOPE: 1};
    for field in &search.fields {
        keys.insert(format!("{BODY}.{field}"), "text");
    }
    let options = IndexOptions::builder()
        .name(TEXT_INDEX.to_owned())
        .default_language("none".to_owned())
        .language_override("_tsd_language".to_owned())
        .build();
    Ok(Some(
        IndexModel::builder().keys(keys).options(options).build(),
    ))
}

fn index_name(name: &str) -> String {
    let hash = name
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    format!("_tsd_ix_{hash:016x}")
}

#[cfg(test)]
#[path = "documents_tests.rs"]
mod tests;
