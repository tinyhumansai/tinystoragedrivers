//! File metadata and chunk arithmetic.

use super::*;
use mongodb::bson::DateTime;

#[test]
fn metadata_carries_scope_and_content_type() {
    let scope = Scope::new("alice").unwrap();
    assert_eq!(
        file_metadata(&scope, Some("text/plain")),
        doc! {"_scope": "alice", "content_type": "text/plain"}
    );
    assert_eq!(file_metadata(&scope, None), doc! {"_scope": "alice"});
}

#[test]
fn files_become_blob_meta() {
    let file: FilesCollectionDocument = mongodb::bson::from_document(doc! {
        "_id": 1,
        "length": 11_i64,
        "chunkSize": 4,
        "uploadDate": DateTime::now(),
        "filename": "a/b",
        "metadata": {"_scope": "s", "content_type": "text/plain"},
    })
    .unwrap();
    assert_eq!(
        blob_meta(&file),
        BlobMeta {
            key: "a/b".into(),
            len: 11,
            content_type: Some("text/plain".into())
        }
    );
    let bare: FilesCollectionDocument = mongodb::bson::from_document(doc! {
        "_id": 1, "length": 0_i64, "chunkSize": 4, "uploadDate": DateTime::now(),
    })
    .unwrap();
    assert_eq!(blob_meta(&bare).content_type, None);
}

#[test]
fn ranges_map_to_the_chunks_they_touch() {
    assert_eq!(chunk_span(&(0..1), 4), (0, 0, 0));
    assert_eq!(chunk_span(&(3..9), 4), (0, 2, 3));
    assert_eq!(chunk_span(&(8..12), 4), (2, 2, 0));
    assert_eq!(chunk_span(&(5..6), 0), (5, 5, 0));
}
