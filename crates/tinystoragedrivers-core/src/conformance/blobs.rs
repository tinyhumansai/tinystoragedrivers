//! Blob port checks.

use super::{fails, ok, unique};
use crate::backend::ScopedStorage;
use crate::error::ErrorKind;

pub(super) async fn run(storage: &ScopedStorage) {
    let blobs = storage.blobs();
    let base = unique("blob");
    let key = format!("{base}/a.txt");

    assert!(ok(blobs.get(&key).await, "get missing").is_none());
    assert!(ok(blobs.head(&key).await, "head missing").is_none());
    assert!(ok(blobs.get_range(&key, 0..1).await, "range missing").is_none());

    let meta = ok(
        blobs
            .put(&key, b"hello world".to_vec(), Some("text/plain"))
            .await,
        "put",
    );
    assert_eq!(meta.len, 11);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    let blob = ok(blobs.get(&key).await, "get").expect("blob exists");
    assert_eq!(blob.bytes, b"hello world");
    assert_eq!(blob.meta, meta);
    assert_eq!(ok(blobs.head(&key).await, "head"), Some(meta));

    assert_eq!(
        ok(blobs.get_range(&key, 6..11).await, "range").as_deref(),
        Some(&b"world"[..])
    );
    assert_eq!(
        ok(blobs.get_range(&key, 6..999).await, "range clamps").as_deref(),
        Some(&b"world"[..])
    );
    assert_eq!(
        ok(blobs.get_range(&key, 50..60).await, "range past the end").as_deref(),
        Some(&b""[..])
    );
    #[allow(
        clippy::reversed_empty_ranges,
        reason = "the inverted range is the input under test"
    )]
    let inverted = 5..1;
    fails(
        blobs.get_range(&key, inverted).await,
        ErrorKind::InvalidInput,
        "inverted range",
    );

    let replaced = ok(blobs.put(&key, vec![1, 2], None).await, "replace");
    assert_eq!(replaced.len, 2);
    assert_eq!(replaced.content_type, None);

    ok(
        blobs.put(&format!("{base}/b.bin"), vec![0], None).await,
        "second",
    );
    let listed: Vec<_> = ok(blobs.list(&format!("{base}/")).await, "list")
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(listed, [key.clone(), format!("{base}/b.bin")]);

    assert!(ok(blobs.delete(&key).await, "delete"));
    assert!(!ok(blobs.delete(&key).await, "delete missing"));

    for bad in ["", "/abs", "a//b", "a/../b", "a/./b"] {
        fails(
            blobs.put(bad, vec![], None).await,
            ErrorKind::InvalidInput,
            "invalid key",
        );
    }
}
