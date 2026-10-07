//! Segment encoding and the arithmetic of trimmed segments.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn new_segments_cover_their_batch() {
    let segment = new_segment("log", 0, 4, &[json!(1), json!({"a": 2})]).unwrap();
    assert_eq!(segment.get_str("s").unwrap(), "log");
    assert_eq!(segment.get_i64("o").unwrap(), 4);
    assert_eq!(segment.get_i64("n").unwrap(), 2);
    assert_eq!(segment.get_i64("e").unwrap(), 6);
    assert_eq!(segment.get_i64("t").unwrap(), 0);
    let decoded = Segment::decode(&segment).unwrap();
    assert_eq!((decoded.start, decoded.end, decoded.trimmed), (4, 6, 0));
    assert_eq!(decoded.id, Bson::Null);

    assert_eq!(
        new_segment("log", 0, u64::MAX, &[json!(1)])
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        new_segment("log", 0, u64::MAX - 1, &[json!(1)])
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        new_segment("log", 0, 0, &[json!(u64::MAX)])
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn trimmed_segments_number_their_remaining_entries() {
    let segment = Segment::decode(&doc! {
        "_id": 1, "o": 10_i64, "e": 14_i64, "t": 2_i64, "vs": ["c", "d"],
    })
    .unwrap();
    let offsets: Vec<u64> = segment.entries_from(0).map(|(offset, _)| offset).collect();
    assert_eq!(offsets, [12, 13]);
    let later: Vec<u64> = segment.entries_from(13).map(|(offset, _)| offset).collect();
    assert_eq!(later, [13]);
}

#[test]
fn malformed_segments_are_rejected() {
    for broken in [
        doc! {"o": 0_i64, "e": 1_i64, "t": 0_i64},
        doc! {"o": 0_i64, "e": 2_i64, "t": 0_i64, "vs": [1]},
        doc! {"o": -1_i64, "e": 0_i64, "t": 0_i64, "vs": []},
        doc! {"e": 0_i64, "t": 0_i64, "vs": []},
    ] {
        assert_eq!(
            Segment::decode(&broken).unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }
    assert_eq!(
        stored_offset(u64::MAX).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(segment_models().len(), 2);
}
