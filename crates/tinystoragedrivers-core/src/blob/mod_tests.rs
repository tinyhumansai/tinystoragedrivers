//! Blob key validation and range clamping.

use super::*;

#[test]
fn keys_validate() {
    assert!(validate_blob_key("uploads/2026/a.png").is_ok());
    for bad in ["", "/a", "a/", "a//b", "./a", "a/../b", "nul\0"] {
        assert!(validate_blob_key(bad).is_err(), "{bad:?}");
    }
    assert!(validate_blob_key(&"a".repeat(MAX_BLOB_KEY_LEN + 1)).is_err());
}

#[test]
fn ranges_clamp_to_the_blob() {
    assert_eq!(clamp_range(&(2..5), 10).unwrap(), 2..5);
    assert_eq!(clamp_range(&(2..50), 10).unwrap(), 2..10);
    assert_eq!(clamp_range(&(20..50), 10).unwrap(), 10..10);
    assert_eq!(clamp_range(&(0..u64::MAX), 3).unwrap(), 0..3);
    #[allow(
        clippy::reversed_empty_ranges,
        reason = "the inverted range is the input under test"
    )]
    let inverted = 5..1;
    assert!(clamp_range(&inverted, 10).is_err());
}

#[test]
fn metadata_omits_a_missing_content_type() {
    let meta = BlobMeta {
        key: "k".into(),
        len: 1,
        content_type: None,
    };
    assert_eq!(
        serde_json::to_string(&meta).unwrap(),
        r#"{"key":"k","len":1}"#
    );
}
