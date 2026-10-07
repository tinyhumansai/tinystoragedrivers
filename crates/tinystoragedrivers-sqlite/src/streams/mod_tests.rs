//! Offset conversions at the edges of SQLite's integer range.

use super::*;

#[test]
fn offsets_round_trip_and_overflow_is_reported() {
    assert_eq!(to_offset(5), 5);
    assert_eq!(to_offset(-1), 0);
    assert_eq!(to_stored(7).unwrap(), 7);
    assert_eq!(
        to_stored(u64::MAX).unwrap_err().kind(),
        tinystoragedrivers_core::ErrorKind::Backend
    );
}

#[test]
fn handles_render_their_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("s.db")).unwrap();
    let streams = SqliteStreams::new(db, Arc::new(Tables::new("")), Scope::local());
    assert!(format!("{streams:?}").contains("local"));
}
