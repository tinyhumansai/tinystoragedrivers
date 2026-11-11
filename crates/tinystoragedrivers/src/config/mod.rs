//! Choosing and opening a backend from a URL.
//!
//! A host reads one string from its configuration (`memory`,
//! `sqlite:/var/lib/app`, or `mongodb://db.internal/openhuman`) and hands it to
//! [`StorageConfig::parse`] then [`open`]. A URL for a driver this build did
//! not compile in is rejected with the Cargo feature to enable, so a
//! misconfigured deployment fails at boot with an actionable message.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use tinystoragedrivers_api::{Storage, StorageBackend};
use tinystoragedrivers_types::{Capability, Result, Scope, StorageError, WriteCondition};

/// A parsed storage location.
///
/// ```
/// use tinystoragedrivers::StorageConfig;
///
/// assert_eq!(StorageConfig::parse("memory")?, StorageConfig::Memory);
/// let mongo = StorageConfig::parse("mongodb://app:hunter2@db.internal/openhuman")?;
/// assert_eq!(mongo.to_string(), "mongodb://app:***@db.internal/openhuman");
/// # Ok::<(), tinystoragedrivers::StorageError>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageConfig {
    /// Process memory; nothing survives a restart.
    Memory,
    /// SQLite. A path ending in `.db` or `.sqlite` is one database file; any
    /// other path is a directory holding the one file `storage.db`.
    Sqlite {
        /// File or directory.
        path: PathBuf,
    },
    /// `MongoDB`, one database shared by every scope.
    MongoDb {
        /// The connection string, credentials included.
        uri: String,
        /// The database name taken from the URI path.
        database: String,
    },
    /// JSON and JSONL files under a directory.
    File {
        /// Root directory.
        dir: PathBuf,
    },
}

impl StorageConfig {
    /// Parse a storage URL.
    ///
    /// Accepted forms: `memory`; `sqlite:<path>`; a bare path (absolute, or
    /// starting with `./` or `../`), read as SQLite;
    /// `mongodb://…/<database>` and `mongodb+srv://…/<database>`; and
    /// `file:<dir>`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// empty or unrecognized URL, an empty path, or a `MongoDB` URI without a
    /// database name.
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() {
            return Err(StorageError::invalid_input("storage URL is empty"));
        }
        if url == "memory" || url == "memory:" {
            return Ok(Self::Memory);
        }
        if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
            return Self::parse_mongo(url);
        }
        if let Some(path) = url.strip_prefix("sqlite:") {
            return Ok(Self::Sqlite {
                path: non_empty_path(path.strip_prefix("//").unwrap_or(path), "sqlite")?,
            });
        }
        if let Some(dir) = url.strip_prefix("file:") {
            return Ok(Self::File {
                dir: non_empty_path(dir.strip_prefix("//").unwrap_or(dir), "file")?,
            });
        }
        if url.starts_with('/') || url.starts_with("./") || url.starts_with("../") {
            return Ok(Self::Sqlite {
                path: PathBuf::from(url),
            });
        }
        Err(StorageError::invalid_input(format!(
            "unrecognized storage URL `{}`; expected memory, sqlite:<path>, mongodb://…/<db> or file:<dir>",
            redact_url(url)
        )))
    }

    fn parse_mongo(url: &str) -> Result<Self> {
        let path = split_uri(url).map_or("", |parts| parts.path);
        let database = path.split('/').next().unwrap_or_default();
        if database.is_empty() {
            return Err(StorageError::invalid_input(format!(
                "MongoDB URL `{}` names no database; add one as the path, e.g. mongodb://host/openhuman",
                redact_url(url)
            )));
        }
        Ok(Self::MongoDb {
            uri: url.to_owned(),
            database: database.to_owned(),
        })
    }

    /// The driver this configuration selects.
    #[must_use]
    pub fn driver(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite { .. } => "sqlite",
            Self::MongoDb { .. } => "mongodb",
            Self::File { .. } => "file",
        }
    }
}

fn non_empty_path(path: &str, driver: &str) -> Result<PathBuf> {
    if path.is_empty() {
        Err(StorageError::invalid_input(format!(
            "{driver} storage URL has no path"
        )))
    } else {
        Ok(PathBuf::from(path))
    }
}

/// The pieces of `scheme://[userinfo@]hosts[/path][?query]`.
struct UriParts<'a> {
    path: &'a str,
}

/// Split a connection URI without trusting the credentials to be
/// percent-encoded: the userinfo ends at the last `@` before the query, so a
/// stray `/` or `@` inside a password cannot move the boundary.
fn split_uri(url: &str) -> Option<UriParts<'_>> {
    let (_, rest) = url.split_once("://")?;
    let at = rest.split('?').next().and_then(|head| head.rfind('@'));
    let tail = match at {
        Some(at) => rest.get(at + 1..).unwrap_or_default(),
        None => rest,
    };
    let before_query = tail.split('?').next().unwrap_or(tail);
    let (_, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    Some(UriParts { path })
}

/// Query options whose values are credentials (AWS session tokens ride in
/// `authMechanismProperties`).
const SECRET_OPTIONS: [&str; 4] = [
    "authmechanismproperties",
    "proxypassword",
    "tlscertificatekeyfilepassword",
    "password",
];

/// Hide credentials in a storage connection URI before displaying it.
///
/// The password and credential-valued query options are replaced with `***`.
/// When userinfo and query parsing overlap, both sensitive spans are found in
/// the original input and masked together, preserving conservative redaction
/// for unescaped reserved characters.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let mut sensitive = legacy_userinfo_password_span(url)
        .into_iter()
        .collect::<Vec<_>>();
    sensitive.extend(secret_query_value_spans(url));
    preserve_legacy_query_path_separator(&redact_spans(url, sensitive))
}

/// The legacy renderer inserts `/` when a URI has a query but no path.
fn preserve_legacy_query_path_separator(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let query_end = rest.find('#').unwrap_or(rest.len());
    let Some(query_marker) = rest[..query_end].find('?') else {
        return url.to_owned();
    };
    if rest[..query_marker].contains('/') {
        return url.to_owned();
    }
    format!(
        "{scheme}://{}/{}",
        &rest[..query_marker],
        &rest[query_marker..]
    )
}

/// Locate the legacy userinfo password interval on the original URI. `@`
/// always means the last `@` after the scheme, even when reserved characters
/// make the input look like it has already entered its path or query.
fn legacy_userinfo_password_span(url: &str) -> Option<Range<usize>> {
    let (_, rest) = url.split_once("://")?;
    let at = rest.rfind('@')?;
    let colon = rest[..at].find(':')?;
    let base = url.len() - rest.len();
    Some(base + colon + 1..base + at)
}

/// Locate values for known secret query options using only original input
/// boundaries: `?` starts an option list and `&` separates options. Match
/// legacy behavior through the URI end, including `#` and any query-looking
/// text after it.
fn secret_query_value_spans(url: &str) -> Vec<Range<usize>> {
    scan_secret_query_value_spans(url, || {})
}

/// Each URI byte is visited once by the outer scan. Every query boundary can
/// inspect only the finite, bounded set of secret option names, so query work
/// is linear in URI length even when `?` appears in values or malformed input.
fn scan_secret_query_value_spans(
    url: &str,
    mut visit_input_byte: impl FnMut(),
) -> Vec<Range<usize>> {
    let bytes = url.as_bytes();
    let mut spans = Vec::new();
    let mut secret_starts = Vec::new();
    let mut query_started = false;

    for index in 0..=bytes.len() {
        let byte = bytes.get(index).copied();
        if byte == Some(b'?') {
            query_started = true;
            if let Some(value_start) =
                secret_query_value_start(bytes, index + 1, &mut visit_input_byte)
            {
                secret_starts.push(value_start);
            }
        } else if query_started && (byte == Some(b'&') || byte.is_none()) {
            spans.extend(secret_starts.drain(..).map(|start| start..index));
            if byte == Some(b'&')
                && let Some(value_start) =
                    secret_query_value_start(bytes, index + 1, &mut visit_input_byte)
            {
                secret_starts.push(value_start);
            }
        }
        if byte.is_some() {
            visit_input_byte();
        }
    }
    spans
}

fn secret_query_value_start(
    bytes: &[u8],
    start: usize,
    mut visit_input_byte: impl FnMut(),
) -> Option<usize> {
    for option in SECRET_OPTIONS {
        let mut cursor = start;
        let mut matched = true;
        for expected in option.bytes() {
            let Some(&byte) = bytes.get(cursor) else {
                matched = false;
                break;
            };
            visit_input_byte();
            let (decoded, width) = if byte == b'%'
                && let Some((&high, &low)) = bytes.get(cursor + 1).zip(bytes.get(cursor + 2))
                && let (Some(high), Some(low)) = (hex_value(high), hex_value(low))
            {
                visit_input_byte();
                visit_input_byte();
                (high << 4 | low, 3)
            } else {
                (byte, 1)
            };
            if !decoded.eq_ignore_ascii_case(&expected) {
                matched = false;
                break;
            }
            cursor += width;
        }
        if matched && bytes.get(cursor) == Some(&b'=') {
            visit_input_byte();
            return Some(cursor + 1);
        }
    }
    None
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Replace overlapping sensitive intervals together so processing one kind
/// of credential cannot move or erase boundaries needed to find another.
fn redact_spans(input: &str, mut spans: Vec<Range<usize>>) -> String {
    if spans.is_empty() {
        return input.to_owned();
    }
    spans.sort_by_key(|span| span.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(spans.len());
    for span in spans {
        if let Some(previous) = merged.last_mut()
            && span.start <= previous.end
        {
            previous.end = previous.end.max(span.end);
            continue;
        }
        merged.push(span);
    }

    let mut output = String::with_capacity(input.len());
    let mut cursor = 0;
    for span in merged {
        output.push_str(&input[cursor..span.start]);
        output.push_str("***");
        cursor = span.end;
    }
    output.push_str(&input[cursor..]);
    output
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("memory"),
            Self::Sqlite { path } => write!(f, "sqlite:{}", path.display()),
            Self::MongoDb { uri, .. } => f.write_str(&redact_url(uri)),
            Self::File { dir } => write!(f, "file:{}", dir.display()),
        }
    }
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageConfig({self})")
    }
}

/// Open the backend `config` names.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) naming the
/// Cargo feature to enable when the driver is not compiled in; otherwise the
/// driver's own connection error.
#[allow(
    clippy::unused_async,
    reason = "the network drivers this dispatches to connect asynchronously"
)]
pub async fn open(config: &StorageConfig) -> Result<Arc<dyn StorageBackend>> {
    let engine = match config {
        StorageConfig::Memory => {
            Engine::Memory(tinystoragedrivers_document::memory::MemoryConnection::new())
        }
        #[cfg(feature = "file")]
        StorageConfig::File { dir } => {
            Engine::File(tinystoragedrivers_connect::FileConnection::open(dir)?)
        }
        #[cfg(feature = "sqlite")]
        StorageConfig::Sqlite { path } => {
            Engine::Sqlite(tinystoragedrivers_connect::SqliteConnection::open(path)?)
        }
        #[cfg(feature = "mongodb")]
        StorageConfig::MongoDb { uri, database } => {
            let connection =
                tinystoragedrivers_connect::MongoConnection::connect(uri, database).await?;
            let kv_factory = tinystoragedrivers_kv::MongoKvFactory::new(connection.clone()).await?;
            Engine::Mongo {
                connection,
                kv_factory,
            }
        }
        #[allow(
            unreachable_patterns,
            reason = "reachable unless every driver feature is enabled"
        )]
        other => {
            return Err(StorageError::invalid_input(format!(
                "storage URL `{other}` needs the `{}` driver, which is not available in this build of tinystoragedrivers",
                other.driver()
            )));
        }
    };
    Ok(Arc::new(CompositeBackend::new(engine)))
}

#[derive(Debug)]
enum Engine {
    Memory(tinystoragedrivers_document::memory::MemoryConnection),
    #[cfg(feature = "file")]
    File(tinystoragedrivers_connect::FileConnection),
    #[cfg(feature = "sqlite")]
    Sqlite(tinystoragedrivers_connect::SqliteConnection),
    #[cfg(feature = "mongodb")]
    Mongo {
        connection: tinystoragedrivers_connect::MongoConnection,
        kv_factory: tinystoragedrivers_kv::MongoKvFactory,
    },
}

#[derive(Debug)]
struct CompositeBackend {
    engine: Engine,
    handles: std::sync::Mutex<std::collections::HashMap<String, Storage>>,
}

impl CompositeBackend {
    fn new(engine: Engine) -> Self {
        Self {
            engine,
            handles: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    #[cfg_attr(
        not(any(feature = "sqlite", feature = "mongodb")),
        allow(
            clippy::unnecessary_wraps,
            reason = "engine-enabled builds can fail while opening a scope; keep one backend contract across feature sets"
        )
    )]
    fn open_scope(&self, scope: &tinystoragedrivers_types::Scope) -> Result<Storage> {
        use std::sync::Arc;
        use tinystoragedrivers_api::Storage;

        let (documents, streams, kv, blobs, queues, secrets) = match &self.engine {
            Engine::Memory(connection) => {
                let scoped = connection.for_scope(scope.clone());
                let docs = scoped.documents();
                let streams = scoped.streams();
                (
                    docs.clone(),
                    streams,
                    Arc::new(tinystoragedrivers_kv::MemoryKv::new(scope.clone()))
                        as Arc<dyn tinystoragedrivers_api::KvStore>,
                    Arc::new(tinystoragedrivers_blob::MemoryBlobStore::new(scope.clone()))
                        as Arc<dyn tinystoragedrivers_api::BlobStore>,
                    Arc::new(tinystoragedrivers_queue::MemoryQueue::new())
                        as Arc<dyn tinystoragedrivers_api::QueueStore>,
                    Arc::new(tinystoragedrivers_secrets::MemorySecrets::new())
                        as Arc<dyn tinystoragedrivers_api::SecretStore>,
                )
            }
            #[cfg(feature = "file")]
            Engine::File(connection) => {
                let docs = Arc::new(tinystoragedrivers_document::FileDocuments::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::DocumentStore>;
                let streams = Arc::new(tinystoragedrivers_document::FileStreams::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::StreamStore>;
                let kv = Arc::new(tinystoragedrivers_kv::FileKv::new(
                    connection.clone(),
                    scope.clone(),
                )?) as Arc<dyn tinystoragedrivers_api::KvStore>;
                let blobs = Arc::new(tinystoragedrivers_blob::FileBlobStore::from_connection(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::BlobStore>;
                let queues = Arc::new(tinystoragedrivers_queue::DocumentQueue::new(&docs))
                    as Arc<dyn tinystoragedrivers_api::QueueStore>;
                (docs, streams, kv, blobs, queues, unsupported_secrets())
            }
            #[cfg(feature = "sqlite")]
            Engine::Sqlite(connection) => {
                let docs = Arc::new(tinystoragedrivers_document::sqlite::SqliteDocuments::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::DocumentStore>;
                let streams = Arc::new(tinystoragedrivers_document::sqlite::SqliteStreams::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::StreamStore>;
                let kv = Arc::new(tinystoragedrivers_kv::SqliteKv::new(
                    connection.clone(),
                    scope.clone(),
                )?) as Arc<dyn tinystoragedrivers_api::KvStore>;
                let blobs = Arc::new(tinystoragedrivers_blob::SqliteBlobStore::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::BlobStore>;
                let queues = Arc::new(tinystoragedrivers_queue::DocumentQueue::new(&docs))
                    as Arc<dyn tinystoragedrivers_api::QueueStore>;
                (docs, streams, kv, blobs, queues, unsupported_secrets())
            }
            #[cfg(feature = "mongodb")]
            Engine::Mongo {
                connection,
                kv_factory,
            } => {
                let docs = Arc::new(tinystoragedrivers_document::mongodb::MongoDocuments::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::DocumentStore>;
                let blobs = Arc::new(tinystoragedrivers_blob::MongoBlobStore::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::BlobStore>;
                // Sealing writes the stream's log to a sealed blob in the same
                // scope, so the streams port gets the scope's blob store.
                let streams = Arc::new(
                    tinystoragedrivers_document::mongodb::MongoStreams::new(
                        connection.clone(),
                        scope.clone(),
                    )
                    .with_blob_store(blobs.clone()),
                ) as Arc<dyn tinystoragedrivers_api::StreamStore>;
                let kv = Arc::new(kv_factory.for_scope(scope))
                    as Arc<dyn tinystoragedrivers_api::KvStore>;
                let queues = Arc::new(tinystoragedrivers_queue::DocumentQueue::new(&docs))
                    as Arc<dyn tinystoragedrivers_api::QueueStore>;
                (docs, streams, kv, blobs, queues, unsupported_secrets())
            }
        };
        Ok(Storage::new(documents, streams, kv, blobs, queues, secrets))
    }

    #[cfg(any(feature = "sqlite", feature = "mongodb"))]
    fn open_conditioned_scope(&self, scope: &Scope, condition: &WriteCondition) -> Result<Storage> {
        use std::sync::Arc;
        use tinystoragedrivers_api::Storage;

        let unsupported = Storage::unsupported();
        match &self.engine {
            #[cfg(feature = "sqlite")]
            Engine::Sqlite(connection) => {
                let documents = Arc::new(
                    tinystoragedrivers_document::sqlite::SqliteDocuments::conditioned(
                        connection.clone(),
                        scope.clone(),
                        condition.clone(),
                    ),
                ) as Arc<dyn tinystoragedrivers_api::DocumentStore>;
                let streams = Arc::new(
                    tinystoragedrivers_document::sqlite::SqliteStreams::conditioned(
                        connection.clone(),
                        scope.clone(),
                        condition.clone(),
                    ),
                ) as Arc<dyn tinystoragedrivers_api::StreamStore>;
                let kv = Arc::new(tinystoragedrivers_kv::SqliteKv::conditioned(
                    connection.clone(),
                    scope.clone(),
                    condition.clone(),
                )?) as Arc<dyn tinystoragedrivers_api::KvStore>;
                let blobs = Arc::new(tinystoragedrivers_blob::SqliteBlobStore::conditioned(
                    connection.clone(),
                    scope.clone(),
                    condition.clone(),
                )) as Arc<dyn tinystoragedrivers_api::BlobStore>;
                let queues = Arc::new(tinystoragedrivers_queue::DocumentQueue::new(&documents))
                    as Arc<dyn tinystoragedrivers_api::QueueStore>;
                Ok(Storage::new(
                    documents,
                    streams,
                    kv,
                    blobs,
                    queues,
                    unsupported.secrets().clone(),
                ))
            }
            #[cfg(feature = "mongodb")]
            Engine::Mongo {
                connection,
                kv_factory,
            } => {
                let documents = Arc::new(
                    tinystoragedrivers_document::mongodb::MongoDocuments::conditioned(
                        connection.clone(),
                        scope.clone(),
                        condition.clone(),
                    )?,
                ) as Arc<dyn tinystoragedrivers_api::DocumentStore>;
                // GridFS writes cannot join the condition's transaction, so the
                // conditioned bundle exposes no blob port. Sealing still needs a
                // blob for the sealed log: it is written before the guarded
                // marker transaction and deleted if that transaction fails, so
                // an unconditioned store in the same scope is safe for it.
                let seal_blobs = Arc::new(tinystoragedrivers_blob::MongoBlobStore::new(
                    connection.clone(),
                    scope.clone(),
                )) as Arc<dyn tinystoragedrivers_api::BlobStore>;
                let streams = Arc::new(
                    tinystoragedrivers_document::mongodb::MongoStreams::conditioned(
                        connection.clone(),
                        scope.clone(),
                        condition.clone(),
                    )?
                    .with_blob_store(seal_blobs),
                ) as Arc<dyn tinystoragedrivers_api::StreamStore>;
                let kv = Arc::new(kv_factory.conditioned_for_scope(scope, condition.clone())?)
                    as Arc<dyn tinystoragedrivers_api::KvStore>;
                let queues = Arc::new(tinystoragedrivers_queue::DocumentQueue::new(&documents))
                    as Arc<dyn tinystoragedrivers_api::QueueStore>;
                Ok(Storage::new(
                    documents,
                    streams,
                    kv,
                    unsupported.blobs().clone(),
                    queues,
                    unsupported.secrets().clone(),
                ))
            }
            _ => Err(StorageError::unsupported(
                Capability::ConditionalWrites,
                "this storage composition cannot atomically condition every exposed mutation port",
            )),
        }
    }

    #[cfg(not(any(feature = "sqlite", feature = "mongodb")))]
    #[allow(
        clippy::unused_self,
        reason = "the memory-only build has no engine to condition, while this method must implement the shared backend contract"
    )]
    fn open_conditioned_scope(
        &self,
        _scope: &Scope,
        _condition: &WriteCondition,
    ) -> Result<Storage> {
        Err(StorageError::unsupported(
            Capability::ConditionalWrites,
            "this storage composition cannot atomically condition mutations",
        ))
    }
}

#[cfg(any(feature = "file", feature = "sqlite", feature = "mongodb"))]
fn unsupported_secrets() -> Arc<dyn tinystoragedrivers_api::SecretStore> {
    Storage::unsupported().secrets().clone()
}

impl StorageBackend for CompositeBackend {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        tinystoragedrivers_types::Capabilities::none()
    }

    fn for_scope(&self, scope: &tinystoragedrivers_types::Scope) -> Result<Storage> {
        let key = scope.as_str().to_owned();
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| StorageError::backend("storage scope cache was poisoned"))?;
        if let Some(storage) = handles.get(&key) {
            return Ok(storage.clone());
        }
        let storage = self.open_scope(scope)?;
        handles.insert(key, storage.clone());
        Ok(storage)
    }

    fn for_scope_conditioned(&self, scope: &Scope, condition: &WriteCondition) -> Result<Storage> {
        self.open_conditioned_scope(scope, condition)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(all(test, feature = "mongodb"))]
#[path = "mod_mongo_seal_tests.rs"]
mod mongo_seal_tests;
