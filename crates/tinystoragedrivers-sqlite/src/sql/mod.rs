//! SQL text the driver builds: quoting, JSON paths, and the generic schema.
//!
//! Every name that reaches SQL text is either validated by the core (collection
//! and database names are `[A-Za-z0-9_.-]`) or quoted here; values are always
//! bound as parameters. Index and partial-index expressions are the one place
//! a value is inlined, because SQLite only uses a partial index when the query
//! repeats its `WHERE` term literally.

use rusqlite::Connection;
use tinystoragedrivers_core::Result;

use crate::error::during;

/// Quote an SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote an SQL string literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The SQLite JSON path of a dotted field path: `owner.name` becomes
/// `$."owner"."name"`.
pub(crate) fn json_path(field: &str) -> String {
    let mut path = String::from("$");
    for segment in field.split('.') {
        path.push_str(".\"");
        path.push_str(&segment.replace('\\', "\\\\").replace('"', "\\\""));
        path.push('"');
    }
    path
}

/// The table names of one (possibly prefixed) database.
#[derive(Debug, Clone)]
pub(crate) struct Tables {
    pub(crate) docs: String,
    pub(crate) specs: String,
    pub(crate) fts: String,
    pub(crate) streams: String,
    pub(crate) stream_meta: String,
    pub(crate) blobs: String,
    /// Prefix used for index names.
    pub(crate) prefix: String,
}

impl Tables {
    /// Tables for a database whose names start with `prefix` (empty for a
    /// database that has its file to itself).
    pub(crate) fn new(prefix: &str) -> Self {
        let name = |table: &str| format!("{prefix}_tsd_{table}");
        Self {
            docs: name("docs"),
            specs: name("collections"),
            fts: name("fts"),
            streams: name("streams"),
            stream_meta: name("stream_meta"),
            blobs: name("blobs"),
            prefix: prefix.to_owned(),
        }
    }

    /// Create every table if missing.
    pub(crate) fn ensure(&self, conn: &Connection) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {docs} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 doc TEXT NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {specs} (
                 coll TEXT PRIMARY KEY,
                 spec TEXT NOT NULL
             ) WITHOUT ROWID;
             CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(
                 scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
                 tokenize = 'unicode61'
             );
             CREATE TABLE IF NOT EXISTS {streams} (
                 scope TEXT NOT NULL,
                 stream TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 value TEXT NOT NULL,
                 PRIMARY KEY (scope, stream, seq)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {stream_meta} (
                 scope TEXT NOT NULL,
                 stream TEXT NOT NULL,
                 base INTEGER NOT NULL,
                 len INTEGER NOT NULL,
                 PRIMARY KEY (scope, stream)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {blobs} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 content_type TEXT,
                 bytes BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID;",
            docs = ident(&self.docs),
            specs = ident(&self.specs),
            fts = ident(&self.fts),
            streams = ident(&self.streams),
            stream_meta = ident(&self.stream_meta),
            blobs = ident(&self.blobs),
        );
        conn.execute_batch(&sql).map_err(during("create tables"))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
