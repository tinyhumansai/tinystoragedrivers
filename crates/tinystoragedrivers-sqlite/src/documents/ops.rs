//! Synchronous document operations on one connection.
//!
//! Every function here runs inside the connection mutex and, for writes,
//! inside a transaction the caller opened, so a read-check-write sequence
//! (preconditions, unique indexes, claims) is atomic.

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, Filter, Precondition, Result, Sort, StorageError, Version, Versioned,
    sort_documents, validate_collection, validate_doc, validate_id, value,
};

use super::pushdown;
use crate::error::during;
use crate::sql::{Tables, ident, json_path, literal};

/// Who is asking and when.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ctx<'a> {
    pub(crate) tables: &'a Tables,
    pub(crate) scope: &'a str,
    pub(crate) now_ms: u64,
}

/// The largest version SQLite's signed 64-bit column can hold.
const MAX_STORED_VERSION: u64 = i64::MAX as u64;

fn to_version(stored: i64) -> Version {
    Version(u64::try_from(stored).unwrap_or(0))
}

/// The collection's declaration, or a bare one when never declared.
pub(crate) fn load_spec(
    conn: &Connection,
    tables: &Tables,
    collection: &str,
) -> Result<CollectionSpec> {
    let sql = format!("SELECT spec FROM {} WHERE coll = ?1", ident(&tables.specs));
    let stored: Option<String> = conn
        .query_row(&sql, [collection], |row| row.get(0))
        .optional()
        .map_err(during("load collection"))?;
    match stored {
        Some(text) => Ok(serde_json::from_str(&text)?),
        None => Ok(CollectionSpec::new(collection)),
    }
}

/// Merge `spec` into the stored declaration, create its indexes, and
/// rebuild the search index if the searchable fields changed.
pub(crate) fn declare(
    conn: &Connection,
    tables: &Tables,
    spec: &CollectionSpec,
    now_ms: u64,
) -> Result<()> {
    let existing = load_spec(conn, tables, &spec.name)?;
    let merged = existing.merge(spec)?;
    check_existing_unique(conn, tables, &existing, &merged, now_ms)?;
    let sql = format!(
        "INSERT INTO {} (coll, spec) VALUES (?1, ?2)
         ON CONFLICT (coll) DO UPDATE SET spec = excluded.spec",
        ident(&tables.specs)
    );
    conn.execute(&sql, params![merged.name, serde_json::to_string(&merged)?])
        .map_err(during("declare collection"))?;
    for index in &merged.indexes {
        // An index is a performance hint: skip one whose paths SQLite cannot
        // express; queries on it are evaluated in Rust either way.
        let Some(columns) = index
            .fields
            .iter()
            .map(|field| {
                json_path(field).map(|path| format!("json_extract(doc, {})", literal(&path)))
            })
            .collect::<Option<Vec<String>>>()
        else {
            continue;
        };
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {name} ON {docs} (scope, {columns}) WHERE coll = {coll}",
            name = ident(&format!(
                "{}tsd_ix_{}_{}",
                tables.prefix, merged.name, index.name
            )),
            docs = ident(&tables.docs),
            columns = columns.join(", "),
            coll = literal(&merged.name),
        );
        conn.execute_batch(&sql).map_err(during("create index"))?;
    }
    if existing.search != merged.search {
        reindex(conn, tables, &merged)?;
    }
    Ok(())
}

/// Refuse newly declared unique indexes that the stored live documents
/// already violate, in any scope.
fn check_existing_unique(
    conn: &Connection,
    tables: &Tables,
    existing: &CollectionSpec,
    merged: &CollectionSpec,
    now_ms: u64,
) -> Result<()> {
    let added: Vec<_> = merged
        .indexes
        .iter()
        .filter(|index| index.unique && !existing.indexes.contains(index))
        .collect();
    if added.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "SELECT scope, doc FROM {} WHERE coll = ?1",
        ident(&tables.docs)
    );
    let mut statement = conn.prepare(&sql).map_err(during("check unique"))?;
    let rows: Vec<(String, String)> = statement
        .query_map([&merged.name], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(during("check unique"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("check unique"))?;
    let mut docs = Vec::with_capacity(rows.len());
    for (scope, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !expired(merged, &doc, now_ms) {
            docs.push((scope, doc));
        }
    }
    for index in added {
        let mut keys: Vec<(String, Vec<Value>)> = docs
            .iter()
            .filter_map(|(scope, doc)| {
                let key = index
                    .fields
                    .iter()
                    .map(|field| value::lookup(doc, field).cloned())
                    .collect::<Option<Vec<Value>>>()?;
                Some((scope.clone(), key))
            })
            .collect();
        let order = |a: &(String, Vec<Value>), b: &(String, Vec<Value>)| {
            a.0.cmp(&b.0).then_with(|| {
                value::compare(&Value::Array(a.1.clone()), &Value::Array(b.1.clone()))
            })
        };
        keys.sort_by(order);
        if keys
            .windows(2)
            .any(|pair| order(&pair[0], &pair[1]).is_eq())
        {
            return Err(StorageError::already_exists(
                "stored documents already violate this unique index",
            ));
        }
    }
    Ok(())
}

/// Rebuild the full-text rows of every document in the collection.
fn reindex(conn: &Connection, tables: &Tables, spec: &CollectionSpec) -> Result<()> {
    let sql = format!("DELETE FROM {} WHERE coll = ?1", ident(&tables.fts));
    conn.execute(&sql, [&spec.name])
        .map_err(during("reindex"))?;
    let sql = format!(
        "SELECT scope, id, doc FROM {} WHERE coll = ?1",
        ident(&tables.docs)
    );
    let mut statement = conn.prepare(&sql).map_err(during("reindex"))?;
    let rows: Vec<(String, String, String)> = statement
        .query_map([&spec.name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(during("reindex"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("reindex"))?;
    for (scope, id, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        index_text(conn, tables, spec, &scope, &id, &doc)?;
    }
    Ok(())
}

/// The searchable text of `doc` under `spec`, if the collection is searchable.
fn search_body(spec: &CollectionSpec, doc: &Value) -> Option<String> {
    let search = spec.search.as_ref()?;
    let tokens: Vec<String> = search
        .fields
        .iter()
        .filter_map(|field| value::lookup(doc, field))
        .flat_map(value::tokens)
        .collect();
    Some(tokens.join(" "))
}

fn index_text(
    conn: &Connection,
    tables: &Tables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    if let Some(body) = search_body(spec, doc) {
        let sql = format!(
            "INSERT INTO {} (scope, coll, id, body) VALUES (?1, ?2, ?3, ?4)",
            ident(&tables.fts)
        );
        conn.execute(&sql, params![scope, spec.name, id, body])
            .map_err(during("index text"))?;
    }
    Ok(())
}

fn unindex_text(conn: &Connection, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let table = ident(&ctx.tables.fts);
    match id {
        Some(id) => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2 AND id = ?3"),
            params![ctx.scope, collection, id],
        ),
        None => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2"),
            params![ctx.scope, collection],
        ),
    }
    .map_err(during("unindex text"))?;
    Ok(())
}

/// Whether `doc` has passed its collection's expiry time.
pub(crate) fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
    spec.ttl_field
        .as_deref()
        .and_then(|field| value::lookup(doc, field))
        .and_then(Value::as_f64)
        .is_some_and(|expires| {
            // Epoch milliseconds fit an f64 exactly until the year 287396.
            #[allow(
                clippy::cast_precision_loss,
                reason = "epoch milliseconds are far below 2^53"
            )]
            let now = now_ms as f64;
            expires <= now
        })
}

/// The stored row, live or expired.
fn stored(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
) -> Result<Option<Versioned<Value>>> {
    let sql = format!(
        "SELECT version, doc FROM {} WHERE scope = ?1 AND coll = ?2 AND id = ?3",
        ident(&ctx.tables.docs)
    );
    let row: Option<(i64, String)> = conn
        .query_row(&sql, params![ctx.scope, collection, id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(during("get"))?;
    row.map(|(version, doc)| {
        Ok(Versioned {
            id: id.to_owned(),
            version: to_version(version),
            doc: serde_json::from_str(&doc)?,
        })
    })
    .transpose()
}

/// Read one live document.
pub(crate) fn get(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
) -> Result<Option<Versioned<Value>>> {
    validate_collection(collection)?;
    validate_id(id)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    Ok(stored(conn, ctx, collection, id)?.filter(|found| !expired(&spec, &found.doc, ctx.now_ms)))
}

/// Enforce a precondition against the live document, if any.
fn check(precondition: Precondition, current: Option<&Versioned<Value>>) -> Result<()> {
    match (precondition, current) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(_)) => {
            Err(StorageError::conflict("the document already exists"))
        }
        (Precondition::Version(expected), Some(found)) if found.version == expected => Ok(()),
        (Precondition::Version(_), _) => Err(StorageError::conflict(
            "the document is not at the expected version",
        )),
    }
}

/// Live documents of the collection matching `filter`, in `sort` order.
pub(crate) fn matching(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
) -> Result<Vec<Versioned<Value>>> {
    validate_collection(collection)?;
    filter.validate()?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    let mut sql = format!(
        "SELECT id, version, doc FROM {} WHERE scope = ? AND coll = {}",
        ident(&ctx.tables.docs),
        literal(collection)
    );
    let mut params = vec![SqlValue::Text(ctx.scope.to_owned())];
    if let Some(clause) = pushdown::clause(filter) {
        sql.push_str(" AND (");
        sql.push_str(&clause.sql);
        sql.push(')');
        params.extend(clause.params);
    }
    let mut statement = conn.prepare(&sql).map_err(during("query"))?;
    let rows: Vec<(String, i64, String)> = statement
        .query_map(params_from_iter(params), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(during("query"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("query"))?;
    let mut found = Vec::with_capacity(rows.len());
    for (id, version, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !expired(&spec, &doc, ctx.now_ms) && filter.matches(&id, &doc) {
            found.push(Versioned {
                id,
                version: to_version(version),
                doc,
            });
        }
    }
    sort_documents(&mut found, sort, |item| (item.id.as_str(), &item.doc));
    Ok(found)
}

fn check_unique(
    conn: &Connection,
    ctx: Ctx<'_>,
    spec: &CollectionSpec,
    id: &str,
    doc: &Value,
) -> Result<()> {
    for index in spec.indexes.iter().filter(|index| index.unique) {
        let Some(key) = index
            .fields
            .iter()
            .map(|field| value::lookup(doc, field).cloned())
            .collect::<Option<Vec<Value>>>()
        else {
            continue;
        };
        let same = index
            .fields
            .iter()
            .zip(&key)
            .fold(Filter::All, |filter, (field, value)| {
                filter.and(Filter::eq(field.clone(), value.clone()))
            });
        let clash = matching(conn, ctx, &spec.name, &same, &[])?
            .into_iter()
            .any(|other| other.id != id);
        if clash {
            return Err(StorageError::already_exists(
                "a unique index already holds this value",
            ));
        }
    }
    Ok(())
}

/// Write one document under `precondition` and return its new version.
pub(crate) fn put(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    doc: &Value,
    precondition: Precondition,
) -> Result<Version> {
    validate_collection(collection)?;
    validate_id(id)?;
    validate_doc(doc)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    let previous = stored(conn, ctx, collection, id)?;
    let live = previous
        .as_ref()
        .filter(|found| !expired(&spec, &found.doc, ctx.now_ms));
    check(precondition, live)?;
    check_unique(conn, ctx, &spec, id, doc)?;
    // Versions keep rising across expiry, so a stale CAS on a re-created
    // document still fails.
    let last = match &previous {
        Some(found) => Some(found.version),
        None => buried_version(conn, ctx, collection, id)?,
    };
    let version = match last {
        None => Version::FIRST,
        Some(last) => last
            .next()
            .filter(|next| next.0 <= MAX_STORED_VERSION)
            .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
    };
    let stored_version = i64::try_from(version.0)
        .map_err(|_| StorageError::backend("document version space is exhausted"))?;
    let sql = format!(
        "INSERT INTO {} (scope, coll, id, version, doc) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (scope, coll, id) DO UPDATE SET version = excluded.version, doc = excluded.doc",
        ident(&ctx.tables.docs)
    );
    conn.execute(
        &sql,
        params![
            ctx.scope,
            collection,
            id,
            stored_version,
            serde_json::to_string(doc)?
        ],
    )
    .map_err(during("put"))?;
    unindex_text(conn, ctx, collection, Some(id))?;
    index_text(conn, ctx.tables, &spec, ctx.scope, id, doc)?;
    Ok(version)
}

/// The version a removed document last had, if any.
fn buried_version(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
) -> Result<Option<Version>> {
    let sql = format!(
        "SELECT version FROM {} WHERE scope = ?1 AND coll = ?2 AND id = ?3",
        ident(&ctx.tables.tombstones)
    );
    let found: Option<i64> = conn
        .query_row(&sql, params![ctx.scope, collection, id], |row| row.get(0))
        .optional()
        .map_err(during("read tombstone"))?;
    Ok(found.map(to_version))
}

/// Delete one document (or, with `id` unset, every document of the
/// collection in this scope), remembering each one's version so the id never
/// reuses it.
fn bury(conn: &Connection, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let which = if id.is_some() { " AND id = ?3" } else { "" };
    let remember = format!(
        "INSERT INTO {tombstones} (scope, coll, id, version)
         SELECT scope, coll, id, version FROM {docs} WHERE scope = ?1 AND coll = ?2{which}
         ON CONFLICT (scope, coll, id) DO UPDATE SET version = excluded.version",
        tombstones = ident(&ctx.tables.tombstones),
        docs = ident(&ctx.tables.docs),
    );
    let remove = format!(
        "DELETE FROM {} WHERE scope = ?1 AND coll = ?2{which}",
        ident(&ctx.tables.docs)
    );
    for sql in [remember, remove] {
        match id {
            Some(id) => conn.execute(&sql, params![ctx.scope, collection, id]),
            None => conn.execute(&sql, params![ctx.scope, collection]),
        }
        .map_err(during("delete"))?;
    }
    Ok(())
}

/// Remove one document under `precondition`; report whether a live one went.
pub(crate) fn delete(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    precondition: Precondition,
) -> Result<bool> {
    let current = get(conn, ctx, collection, id)?;
    check(precondition, current.as_ref())?;
    bury(conn, ctx, collection, Some(id))?;
    unindex_text(conn, ctx, collection, Some(id))?;
    Ok(current.is_some())
}

/// Remove every live document matching `filter`.
pub(crate) fn delete_where(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
) -> Result<u64> {
    let doomed = matching(conn, ctx, collection, filter, &[])?;
    for found in &doomed {
        delete(conn, ctx, collection, &found.id, Precondition::None)?;
    }
    Ok(doomed.len() as u64)
}

/// Remove every document of the collection in this scope.
pub(crate) fn drop_collection(conn: &Connection, ctx: Ctx<'_>, collection: &str) -> Result<()> {
    validate_collection(collection)?;
    bury(conn, ctx, collection, None)?;
    unindex_text(conn, ctx, collection, None)
}

/// Full-text matches, best first.
pub(crate) fn search(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    text: &str,
    limit: usize,
) -> Result<Vec<(String, f64)>> {
    validate_collection(collection)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    if spec.search.is_none() {
        return Err(StorageError::invalid_input(
            "this collection declares no search fields",
        ));
    }
    let wanted = value::tokens(&Value::String(text.to_owned()));
    if wanted.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let query = wanted
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ");
    let sql = format!(
        "SELECT f.id, bm25({fts}), d.doc FROM {fts} AS f
         JOIN {docs} AS d ON d.scope = f.scope AND d.coll = f.coll AND d.id = f.id
         WHERE {fts} MATCH ?1 AND f.scope = ?2 AND f.coll = ?3
         ORDER BY bm25({fts}), f.id",
        fts = ident(&ctx.tables.fts),
        docs = ident(&ctx.tables.docs),
    );
    let mut statement = conn.prepare(&sql).map_err(during("search"))?;
    let rows: Vec<(String, f64, String)> = statement
        .query_map(params![query, ctx.scope, collection], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(during("search"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("search"))?;
    let mut hits = Vec::new();
    for (id, rank, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !expired(&spec, &doc, ctx.now_ms) {
            // bm25 is lower-is-better; the port reports larger-is-better.
            hits.push((id, -rank));
        }
        if hits.len() == limit {
            break;
        }
    }
    Ok(hits)
}

#[cfg(test)]
#[path = "ops_tests.rs"]
mod tests;
