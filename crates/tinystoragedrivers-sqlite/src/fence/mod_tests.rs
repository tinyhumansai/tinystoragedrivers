//! Unit tests for the SQLite fence check.

use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, Filter, Precondition, Scope};

use super::*;

fn conn() -> (Connection, Tables) {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    (conn, tables)
}

fn fencing(epoch: u64) -> Fencing {
    Fencing::new(
        Fence::epoch(
            Scope::new("cluster").unwrap(),
            "leases",
            "l1",
            "epoch",
            epoch,
        ),
        Arc::new(|| 1_000),
    )
}

#[test]
fn an_unfenced_handle_always_passes() {
    let (conn, tables) = conn();
    guard(None, &conn, &tables).unwrap();
}

#[test]
fn checks_the_guard_in_its_own_scope() {
    let (conn, tables) = conn();
    let fenced = fencing(7);
    assert_eq!(
        guard(Some(&fenced), &conn, &tables).unwrap_err().kind(),
        ErrorKind::Fenced
    );
    let ctx = Ctx {
        tables: &tables,
        scope: "cluster",
        now_ms: 1_000,
    };
    ops::put(
        &conn,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 7}),
        Precondition::None,
    )
    .unwrap();
    guard(Some(&fenced), &conn, &tables).unwrap();
    assert_eq!(
        fencing(6).check(&conn, &tables).unwrap_err().kind(),
        ErrorKind::Fenced
    );
    let rendered = format!("{fenced:?}");
    assert!(rendered.contains("leases"), "{rendered}");
}

#[test]
fn an_invalid_guard_address_is_invalid_input() {
    let (conn, tables) = conn();
    let fenced = Fencing::new(
        Fence::new(Scope::local(), "leases", "", Filter::All),
        Arc::new(|| 0),
    );
    assert_eq!(
        fenced.check(&conn, &tables).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
