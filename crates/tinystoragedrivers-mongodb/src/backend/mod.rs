//! [`MongoStorage`]: one MongoDB database shared by every tenant.
//!
//! The backend owns the client and, per named database (a collection
//! prefix), a [`Shared`] holding the declared collection specs and which
//! collections already carry the driver's indexes. Port handles are cheap
//! views over a `Shared` bound to one scope.

pub(crate) mod specs;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection, Database};
use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, Result, Scope, ScopedStorage, StorageBackend,
    StorageError, validate_collection, validate_database,
};

use crate::blobs::MongoBlobs;
use crate::documents::MongoDocuments;
use crate::errors;
use crate::naming::{database_prefix, validate_mongo_database};
use crate::scoped::ScopedCollection;
use crate::streams::MongoStreams;

pub(crate) use specs::SpecCache;

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether a `hello` reply describes a deployment that runs multi-document
/// transactions: a replica set member or a `mongos` router, with sessions.
pub(crate) fn supports_transactions(hello: &Document) -> bool {
    let sessions = hello.contains_key("logicalSessionTimeoutMinutes");
    let replica_set = hello.get_str("setName").is_ok();
    let router = hello.get_str("msg").is_ok_and(|msg| msg == "isdbgrid");
    sessions && (replica_set || router)
}

/// What every handle of one named database shares.
pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) db: Database,
    /// Prepended to every Mongo collection name: empty for the root database.
    pub(crate) prefix: String,
    pub(crate) clock: Clock,
    pub(crate) transactions: bool,
    pub(crate) specs: SpecCache,
}

impl Shared {
    fn new(client: Client, db: Database, prefix: String, clock: Clock, transactions: bool) -> Self {
        Self {
            client,
            db,
            prefix,
            clock,
            transactions,
            specs: SpecCache::default(),
        }
    }

    /// Milliseconds since the Unix epoch, from the configured clock.
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// The raw Mongo collection behind a driver or port collection name.
    /// Only collection-wide bookkeeping (specs, indexes) uses this directly.
    pub(crate) fn raw(&self, name: &str) -> Collection<Document> {
        self.db.collection(&format!("{}{name}", self.prefix))
    }

    /// The scoped handle to a port or driver collection.
    pub(crate) fn scoped(&self, name: &str, scope: &Scope) -> ScopedCollection {
        ScopedCollection::new(self.raw(name), scope.clone())
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("prefix", &self.prefix)
            .field("transactions", &self.transactions)
            .finish_non_exhaustive()
    }
}

/// A [`StorageBackend`] on MongoDB.
///
/// Every scope shares the database; each record carries a `_scope` field that
/// every query, write and index includes. [`StorageBackend::database`] maps a
/// name to the collection prefix `<name>:`.
///
/// ```no_run
/// # async fn demo() -> tinystoragedrivers_core::Result<()> {
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_mongodb::MongoStorage;
///
/// let storage = MongoStorage::connect("mongodb://db.internal/openhuman", "openhuman").await?;
/// let alice = storage.database("approvals")?.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "mongodb");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MongoStorage {
    pub(crate) shared: Arc<Shared>,
    databases: Arc<Mutex<BTreeMap<String, Arc<Shared>>>>,
}

impl MongoStorage {
    /// Connect to `uri` and use the database `database`.
    ///
    /// Connecting runs `hello` once, so an unreachable server fails here, and
    /// its reply decides whether the backend reports
    /// [`Capability::Transactions`] (replica sets and sharded clusters only).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for a malformed URI or database name,
    /// [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable)
    /// when no server answers. Messages never include the URI.
    pub async fn connect(uri: &str, database: &str) -> Result<Self> {
        validate_mongo_database(database)?;
        let mut options = ClientOptions::parse(uri).await.map_err(|error| {
            let mapped = errors::map(error, "parse the connection string");
            if mapped.is_retryable() {
                mapped
            } else {
                StorageError::invalid_input("the MongoDB connection string is malformed")
            }
        })?;
        if options.app_name.is_none() {
            options.app_name = Some("tinystoragedrivers".to_owned());
        }
        let client =
            Client::with_options(options).map_err(errors::failed("configure the client"))?;
        let hello = client
            .database("admin")
            .run_command(doc! {"hello": 1})
            .await
            .map_err(errors::failed("reach the server"))?;
        let db = client.database(database);
        let shared = Shared::new(
            client,
            db,
            String::new(),
            Arc::new(system_clock),
            supports_transactions(&hello),
        );
        Ok(Self {
            shared: Arc::new(shared),
            databases: Arc::default(),
        })
    }

    /// Read time from `clock` (milliseconds since the Unix epoch) instead of
    /// the system clock, so document expiry can be driven deterministically.
    #[must_use]
    pub fn with_clock(self, clock: Clock) -> Self {
        self.rebuild(clock, self.shared.transactions)
    }

    /// Never use multi-document transactions, as if the deployment were a
    /// standalone server: [`Capability::Transactions`] is not reported and
    /// [`atomic_batch`](tinystoragedrivers_core::DocumentStore::atomic_batch)
    /// returns `Unsupported`.
    #[must_use]
    pub fn without_transactions(self) -> Self {
        let clock = Arc::clone(&self.shared.clock);
        self.rebuild(clock, false)
    }

    fn rebuild(&self, clock: Clock, transactions: bool) -> Self {
        let shared = Shared::new(
            self.shared.client.clone(),
            self.shared.db.clone(),
            self.shared.prefix.clone(),
            clock,
            transactions,
        );
        Self {
            shared: Arc::new(shared),
            databases: Arc::default(),
        }
    }

    /// Remove the documents of `collection` in `scope` whose expiry time has
    /// passed. Reads already treat them as absent; this reclaims the space.
    /// Their versions are remembered, so a recreated id continues from them.
    ///
    /// # Errors
    ///
    /// An invalid collection name, or a backend error.
    pub async fn sweep_expired(&self, scope: &Scope, collection: &str) -> Result<u64> {
        validate_collection(collection)?;
        MongoDocuments::new(Arc::clone(&self.shared), scope.clone(), None)
            .sweep(collection, None)
            .await
    }
}

impl MongoStorage {
    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fence>>) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(MongoDocuments::new(
                Arc::clone(&self.shared),
                scope.clone(),
                fence.cloned(),
            )),
            Arc::new(MongoStreams::new(
                Arc::clone(&self.shared),
                scope.clone(),
                fence.cloned(),
            )),
            Arc::new(MongoBlobs::new(
                Arc::clone(&self.shared),
                scope.clone(),
                fence.is_some(),
            )),
        )
    }
}

impl fmt::Debug for MongoStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoStorage")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl StorageBackend for MongoStorage {
    fn driver(&self) -> &'static str {
        "mongodb"
    }

    fn capabilities(&self) -> Capabilities {
        let base = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Ttl);
        if self.shared.transactions {
            base.with(Capability::Transactions)
                .with(Capability::Fencing)
        } else {
            base
        }
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fencing needs transactions, which this MongoDB deployment lacks",
            ));
        }
        fence.validate()?;
        Ok(self.handles(scope, Some(&Arc::new(fence.clone()))))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| StorageError::backend("mongodb database registry lock poisoned"))?;
        let shared = databases.entry(name.to_owned()).or_insert_with(|| {
            Arc::new(Shared::new(
                self.shared.client.clone(),
                self.shared.db.clone(),
                database_prefix(name),
                Arc::clone(&self.shared.clock),
                self.shared.transactions,
            ))
        });
        Ok(Arc::new(Self {
            shared: Arc::clone(shared),
            databases: Arc::clone(&self.databases),
        }))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
