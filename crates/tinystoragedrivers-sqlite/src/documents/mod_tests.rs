//! Cursor fingerprints and handle rendering.

use tinystoragedrivers_core::{ErrorKind, Filter, Sort};

use super::*;

#[test]
fn fingerprints_are_stable_and_query_specific() {
    let a = fingerprint("c", &Query::all()).unwrap();
    assert_eq!(
        a,
        fingerprint("c", &Query::all().limit(5)).unwrap(),
        "limit does not matter"
    );
    assert_ne!(a, fingerprint("d", &Query::all()).unwrap());
    assert_ne!(
        a,
        fingerprint("c", &Query::filter(Filter::eq("x", 1))).unwrap()
    );
    assert_ne!(
        a,
        fingerprint("c", &Query::all().sort(Sort::asc("x"))).unwrap()
    );
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
}

#[test]
fn rejects_foreign_cursors() {
    let query = Query::all().after(Cursor("mem:0000000000000000:1".into()));
    assert_eq!(
        parse_cursor("c", &query).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(parse_cursor("c", &Query::all()).unwrap(), 0);
}

#[test]
fn handles_render_their_scope_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("d.db")).unwrap();
    let docs = SqliteDocuments::new(
        db,
        Arc::new(Tables::new("")),
        Scope::local(),
        Arc::new(|| 0),
    );
    let shown = format!("{docs:?}");
    assert!(shown.contains("local") && shown.contains("d.db"), "{shown}");
}
