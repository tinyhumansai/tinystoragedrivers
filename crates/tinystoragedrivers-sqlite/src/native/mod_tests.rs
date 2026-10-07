//! Raw access, transactions and the migration runner.

use super::*;

fn native() -> (tempfile::TempDir, SqliteNative) {
    let dir = tempfile::tempdir().unwrap();
    let native = SqliteNative::open(dir.path().join("own.db")).unwrap();
    (dir, native)
}

#[tokio::test]
async fn migrations_apply_once_in_order() {
    let (_dir, native) = native();
    let steps = [
        "CREATE TABLE items (id TEXT PRIMARY KEY)",
        "ALTER TABLE items ADD COLUMN n INTEGER",
    ];
    assert_eq!(native.migrate("owner", &steps[..1]).await.unwrap(), 1);
    assert_eq!(native.migrate("owner", &steps).await.unwrap(), 2);
    assert_eq!(
        native.migrate("owner", &steps).await.unwrap(),
        2,
        "idempotent"
    );
    native
        .with_connection(|conn| conn.execute("INSERT INTO items (id, n) VALUES ('a', 1)", []))
        .await
        .unwrap();
    let error = native.migrate("owner", &steps[..1]).await.unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::InvalidInput
    );
    assert_eq!(native.migrate("other", &[]).await.unwrap(), 0);
}

#[tokio::test]
async fn a_failing_step_records_nothing() {
    let (_dir, native) = native();
    let error = native
        .migrate("owner", &["CREATE TABLE ok (id INTEGER)", "NOT SQL"])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    // The first step committed; rerunning resumes at the broken one.
    let fixed = native
        .migrate(
            "owner",
            &[
                "CREATE TABLE ok (id INTEGER)",
                "CREATE TABLE b (id INTEGER)",
            ],
        )
        .await
        .unwrap();
    assert_eq!(fixed, 2);
}

#[tokio::test]
async fn transactions_roll_back_on_error() {
    let (_dir, native) = native();
    native
        .with_connection(|conn| conn.execute_batch("CREATE TABLE t (n INTEGER)"))
        .await
        .unwrap();
    let failed = native
        .with_transaction(|tx| {
            tx.execute("INSERT INTO t VALUES (1)", [])?;
            tx.execute("INSERT INTO missing VALUES (1)", [])
        })
        .await;
    assert!(failed.is_err());
    native
        .with_transaction(|tx| tx.execute("INSERT INTO t VALUES (2)", []))
        .await
        .unwrap();
    let rows: i64 = native
        .with_connection(|conn| conn.query_row("SELECT count(*) FROM t", [], |row| row.get(0)))
        .await
        .unwrap();
    assert_eq!(rows, 1);
    assert!(native.path().ends_with("own.db"));
    assert!(format!("{native:?}").contains("own.db"));
}
