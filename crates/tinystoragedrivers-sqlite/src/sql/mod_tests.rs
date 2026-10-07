//! Quoting, JSON paths and schema creation.

use super::*;

#[test]
fn quotes_identifiers_and_literals() {
    assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    assert_eq!(literal("it's"), "'it''s'");
}

#[test]
fn builds_quoted_json_paths() {
    assert_eq!(json_path("a").as_deref(), Some("$.\"a\""));
    assert_eq!(
        json_path("owner.name").as_deref(),
        Some("$.\"owner\".\"name\"")
    );
    assert_eq!(json_path("we\"ird"), None, "quotes cannot be escaped");
    assert_eq!(json_path("back\\slash"), None);
    assert_eq!(json_path("tags.1"), None, "digits may be an array index");
    assert_eq!(json_path("v1.x").as_deref(), Some("$.\"v1\".\"x\""));
}

#[test]
fn json_paths_resolve_in_sqlite() {
    let conn = Connection::open_in_memory().unwrap();
    let path = json_path("owner.na-me").unwrap();
    let found: String = conn
        .query_row(
            "SELECT json_extract(?1, ?2)",
            [r#"{"owner":{"na":"x","na-me":"ada"}}"#, &path],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(found, "ada");
}

#[test]
fn creates_prefixed_tables_idempotently() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("flows__");
    tables.ensure(&conn).unwrap();
    tables.ensure(&conn).unwrap();
    assert_eq!(tables.docs, "flows___tsd_docs");
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                 'flows___tsd_docs', 'flows___tsd_collections', 'flows___tsd_fts',
                 'flows___tsd_streams', 'flows___tsd_stream_meta', 'flows___tsd_blobs',
                 'flows___tsd_tombstones')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 7);
    assert_eq!(Tables::new("").prefix, "");
}
