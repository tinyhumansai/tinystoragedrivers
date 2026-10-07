//! Quoting, JSON paths and schema creation.

use super::*;

#[test]
fn quotes_identifiers_and_literals() {
    assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    assert_eq!(literal("it's"), "'it''s'");
}

#[test]
fn builds_quoted_json_paths() {
    assert_eq!(json_path("a"), "$.\"a\"");
    assert_eq!(json_path("owner.name"), "$.\"owner\".\"name\"");
    assert_eq!(json_path("we\"ird\\k"), "$.\"we\\\"ird\\\\k\"");
}

#[test]
fn json_paths_resolve_in_sqlite() {
    let conn = Connection::open_in_memory().unwrap();
    let found: String = conn
        .query_row(
            "SELECT json_extract(?1, ?2)",
            [
                r#"{"owner":{"na.me":"x","name":"ada"}}"#,
                &json_path("owner.name"),
            ],
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
            "SELECT count(*) FROM sqlite_master WHERE name LIKE 'flows___tsd_%' AND type = 'table' AND name NOT LIKE '%fts_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 6);
    assert_eq!(Tables::new("").prefix, "");
}
