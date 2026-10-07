//! Document operations directly on a connection.

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, IndexSpec};

use super::*;

fn setup() -> (Connection, Tables) {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    (conn, tables)
}

fn ctx(tables: &Tables) -> Ctx<'_> {
    Ctx {
        tables,
        scope: "local",
        now_ms: 0,
    }
}

#[test]
fn an_exhausted_version_fails_the_write() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    conn.execute("UPDATE _tsd_docs SET version = ?1", [i64::MAX])
        .unwrap();
    let error = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(to_version(-3), Version(0));
}

#[test]
fn declaring_search_later_indexes_existing_documents() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "notes",
        "a",
        &json!({"t": "hello world"}),
        Precondition::None,
    )
    .unwrap();
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("notes").searchable(["t"]),
        0,
    )
    .unwrap();
    let hits = search(&conn, ctx(&tables), "notes", "hello", 5).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        search(&conn, ctx(&tables), "notes", "hello", 0)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        search(&conn, ctx(&tables), "notes", "!!", 5).unwrap().len(),
        0
    );
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("notes").index(IndexSpec::new("by_t", ["t"])),
        0,
    )
    .unwrap();
}

#[test]
fn search_limits_and_scores_are_ordered() {
    let (conn, tables) = setup();
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("n").searchable(["t"]),
        0,
    )
    .unwrap();
    for (id, text) in [("a", "cat"), ("b", "cat cat dog"), ("c", "cat")] {
        put(
            &conn,
            ctx(&tables),
            "n",
            id,
            &json!({"t": text}),
            Precondition::None,
        )
        .unwrap();
    }
    let hits = search(&conn, ctx(&tables), "n", "cat dog", 2).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0].0, "b",
        "the document matching both tokens ranks first"
    );
    assert!(hits[0].1 >= hits[1].1);
}

#[test]
fn corrupt_stored_json_is_a_serialization_error() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    conn.execute("UPDATE _tsd_docs SET doc = 'not json'", [])
        .unwrap();
    let error = get(&conn, ctx(&tables), "c", "a").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    conn.execute(
        "INSERT INTO _tsd_collections (coll, spec) VALUES ('bad', '{')",
        [],
    )
    .unwrap();
    assert_eq!(
        load_spec(&conn, &tables, "bad").unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn missing_tables_are_backend_errors() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("absent");
    let error = get(&conn, ctx(&tables), "c", "a").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn versions_survive_deletion_and_drops() {
    let (conn, tables) = setup();
    let v1 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    assert!(delete(&conn, ctx(&tables), "c", "a", Precondition::None).unwrap());
    let v2 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::Absent,
    )
    .unwrap();
    assert!(v2 > v1);
    drop_collection(&conn, ctx(&tables), "c").unwrap();
    let v3 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::Absent,
    )
    .unwrap();
    assert!(v3 > v2);
    assert_eq!(
        buried_version(&conn, ctx(&tables), "c", "zz").unwrap(),
        None
    );
}

#[test]
fn late_unique_indexes_are_checked_per_scope() {
    let (conn, tables) = setup();
    let other = Ctx {
        scope: "other",
        ..ctx(&tables)
    };
    put(
        &conn,
        ctx(&tables),
        "u",
        "a",
        &json!({"e": "x"}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        other,
        "u",
        "b",
        &json!({"e": "x"}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        ctx(&tables),
        "u",
        "c",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    let unique = CollectionSpec::new("u").index(IndexSpec::new("by_e", ["e"]).unique());
    declare(&conn, &tables, &unique, 0).unwrap();
    declare(&conn, &tables, &unique, 0).unwrap();
    put(
        &conn,
        ctx(&tables),
        "w",
        "a",
        &json!({"e": 1}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        ctx(&tables),
        "w",
        "b",
        &json!({"e": 1.0}),
        Precondition::None,
    )
    .unwrap();
    let error = declare(
        &conn,
        &tables,
        &CollectionSpec::new("w").index(IndexSpec::new("by_e", ["e"]).unique()),
        0,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    assert_eq!(load_spec(&conn, &tables, "w").unwrap().indexes.len(), 0);
}
