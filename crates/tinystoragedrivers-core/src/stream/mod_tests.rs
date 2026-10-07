//! Stream name validation and entry serde.

use serde_json::json;

use super::*;

#[test]
fn stream_names_validate() {
    assert!(validate_stream("journal/run-1").is_ok());
    assert!(validate_stream("").is_err());
    assert!(validate_stream("nul\0").is_err());
    assert!(validate_stream(&"a".repeat(513)).is_err());
}

#[test]
fn entries_serialize_with_their_offset() {
    let entry = StreamEntry {
        offset: 3,
        value: json!({"k": 1}),
    };
    assert_eq!(
        serde_json::to_value(&entry).unwrap(),
        json!({"offset": 3, "value": {"k": 1}})
    );
}
