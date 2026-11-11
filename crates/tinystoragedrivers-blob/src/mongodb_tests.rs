use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures_util::stream;
use mongodb::bson::oid::ObjectId;
use tinystoragedrivers_types::ErrorKind;

#[tokio::test]
async fn mongodb_blob_streams_scoped_ttl_and_paged_listing() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "mongodb_blob_streams_scoped_ttl_and_paged_listing: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let database = format!("tsd_blob_{}_{}", std::process::id(), ObjectId::new());
    let now = Arc::new(AtomicU64::new(10_000));
    let clock = Arc::clone(&now);
    let connection = MongoConnection::connect(&url, &database)
        .await
        .unwrap()
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let scope = Scope::new("local").unwrap();
    let store = MongoBlobStore::new(connection.clone(), scope);
    let capabilities = store.capabilities();
    assert!(capabilities.contains(Capability::Blobs));
    assert!(capabilities.contains(Capability::BlobTtl));
    assert!(capabilities.contains(Capability::BlobListing));
    assert_eq!(store.max_key_len(), crate::key::MAX_KEY_LEN);
    assert_eq!(store.get("missing").await.unwrap(), None);
    assert_eq!(store.get_range("missing", 0..1).await.unwrap(), None);
    assert_eq!(store.head("missing").await.unwrap(), None);
    assert!(!store.delete("missing").await.unwrap());

    assert_mongo_reads_ranges_and_pages(&store).await;

    let streamed: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(
        b"streamed",
    ))]));
    store
        .put_stream_expiring(
            "streamed",
            streamed,
            8,
            Some("application/octet-stream"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get("streamed").await.unwrap().unwrap().bytes,
        b"streamed"
    );

    let other_scope = MongoBlobStore::new(connection.clone(), Scope::new("other").unwrap());
    assert_eq!(other_scope.list("").await.unwrap(), Vec::<BlobMeta>::new());

    assert_mongo_sealed_blob_visibility(&store, &other_scope).await;

    assert_failed_mongo_uploads_keep_previous_blob(&connection, &store).await;

    store
        .put_expiring("temporary", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    now.store(10_010, Ordering::SeqCst);
    assert!(store.get("temporary").await.unwrap().is_none());
    assert!(
        store
            .list_page("temporary", None, 10)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.meta.key == "temporary")
    );
    assert_eq!(store.sweep_expired().await.unwrap(), 1);
    assert!(store.get("temporary").await.unwrap().is_none());
    assert!(store.delete("prefix/b").await.unwrap());
    assert!(!store.delete("prefix/b").await.unwrap());

    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_sweep_reclaims_only_up_to_the_newest_expired_version() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "mongodb_sweep_reclaims_only_up_to_the_newest_expired_version: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let database = format!("tsd_blob_sweep_{}_{}", std::process::id(), ObjectId::new());
    let now = Arc::new(AtomicU64::new(10_000));
    let clock = Arc::clone(&now);
    let connection = MongoConnection::connect(&url, &database)
        .await
        .unwrap()
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let store = MongoBlobStore::new(connection.clone(), Scope::new("local").unwrap());

    // An expired newest version over an older live one: the sweep takes both,
    // so the key stays absent instead of falling back to superseded bytes.
    raw_version(&store, "shadowed", b"old", None).await;
    raw_version(&store, "shadowed", b"expired", Some(10_005)).await;
    // A version newer than the expired one, as a writer racing the sweep
    // leaves it, is past the sweep's bound and survives.
    raw_version(&store, "rewritten", b"expired", Some(10_005)).await;
    raw_version(&store, "rewritten", b"fresh", None).await;
    now.store(10_010, Ordering::SeqCst);

    assert!(store.get("shadowed").await.unwrap().is_none());
    assert_eq!(store.sweep_expired().await.unwrap(), 3);
    assert!(store.get("shadowed").await.unwrap().is_none());
    assert!(
        store
            .files(doc! {"filename": "shadowed"})
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.get("rewritten").await.unwrap().unwrap().bytes,
        b"fresh"
    );
    assert_eq!(
        store
            .files(doc! {"filename": "rewritten"})
            .await
            .unwrap()
            .len(),
        1
    );

    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongodb_sweep_keeps_a_blob_rewritten_between_its_passes() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "mongodb_sweep_keeps_a_blob_rewritten_between_its_passes: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let database = format!(
        "tsd_blob_resweep_{}_{}",
        std::process::id(),
        ObjectId::new()
    );
    let now = Arc::new(AtomicU64::new(10_000));
    let clock = Arc::clone(&now);
    let connection = MongoConnection::connect(&url, &database)
        .await
        .unwrap()
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let store = MongoBlobStore::new(connection.clone(), Scope::new("local").unwrap())
        .pause_sweep_for_test(Arc::clone(&started), Arc::clone(&resume));
    store
        .put_expiring(
            "session",
            b"stale".to_vec(),
            None,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
    now.store(10_010, Ordering::SeqCst);

    // The sweep has found `session` expired; the key is written again before
    // the sweep deletes anything.
    let sweeping = {
        let store = store.clone();
        tokio::spawn(async move { store.sweep_expired().await })
    };
    started.notified().await;
    store.put("session", b"fresh".to_vec(), None).await.unwrap();
    resume.notify_one();
    sweeping.await.unwrap().unwrap();

    assert_eq!(
        store.get("session").await.unwrap().unwrap().bytes,
        b"fresh",
        "the version written between the sweep's passes survives"
    );
    connection.database().drop().await.unwrap();
}

/// Writes one GridFS version of `key` without `upload`'s cleanup of older
/// versions: the state a failed cleanup or a racing writer leaves behind.
async fn raw_version(store: &MongoBlobStore, key: &str, bytes: &[u8], expiry: Option<u64>) {
    let written = (store.connection.clock())();
    let mut upload = store
        .bucket()
        .await
        .unwrap()
        .open_upload_stream(key)
        .metadata(store.metadata(None, expiry, written))
        .await
        .unwrap();
    upload.write_all(bytes).await.unwrap();
    upload.close().await.unwrap();
}

async fn assert_mongo_sealed_blob_visibility(store: &MongoBlobStore, other_scope: &MongoBlobStore) {
    let sealed_key = "_tsd/sealed/events/g0";
    assert!(store.supports_sealed_blobs());
    store
        .put_sealed(sealed_key, b"sealed log".to_vec(), Some("application/zstd"))
        .await
        .unwrap();
    assert_eq!(
        store.get_sealed(sealed_key).await.unwrap().unwrap().bytes,
        b"sealed log"
    );
    assert_eq!(
        store.head_sealed(sealed_key).await.unwrap().unwrap().len,
        10
    );
    assert_eq!(
        store
            .list_sealed("_tsd/sealed/events/")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(other_scope.get_sealed(sealed_key).await.unwrap(), None);
    assert_eq!(
        store.get(sealed_key).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        store
            .list("")
            .await
            .unwrap()
            .iter()
            .all(|meta| meta.key != sealed_key)
    );
    assert!(
        store
            .list_page("", None, 100)
            .await
            .unwrap()
            .items
            .iter()
            .all(|item| item.meta.key != sealed_key)
    );
    assert!(store.delete_sealed(sealed_key).await.unwrap());
    assert_eq!(store.head_sealed(sealed_key).await.unwrap(), None);
}

async fn assert_mongo_reads_ranges_and_pages(store: &MongoBlobStore) {
    store.put("prefix/b", b"b".to_vec(), None).await.unwrap();
    store
        .put("prefix/a", b"abcdef".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(
        store.get("prefix/a").await.unwrap().unwrap().bytes,
        b"abcdef"
    );
    assert_eq!(
        store.get_range("prefix/a", 2..5).await.unwrap().unwrap(),
        b"cde"
    );
    assert_eq!(
        store.get_range("prefix/a", 3..3).await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        store
            .head("prefix/a")
            .await
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );

    let first = store.list_page("prefix/", None, 1).await.unwrap();
    assert_eq!(first.items[0].meta.key, "prefix/a");
    assert_eq!(first.items[0].written_at_ms, Some(10_000));
    assert_eq!(first.next_after.as_deref(), Some("prefix/a"));
    let second = store
        .list_page("prefix/", first.next_after.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(second.items[0].meta.key, "prefix/b");
    assert_eq!(second.next_after, None);
    assert_eq!(store.list("prefix/").await.unwrap().len(), 2);

    store
        .put("prefix.a", b"regex-safe".to_vec(), None)
        .await
        .unwrap();
    let metachar_page = store.list_page("prefix.", None, 10).await.unwrap();
    assert_eq!(metachar_page.items.len(), 1);
    assert_eq!(metachar_page.items[0].meta.key, "prefix.a");
    assert!(
        store
            .list_page("", None, MAX_BLOB_LIST_PAGE_ITEMS + 1)
            .await
            .is_err()
    );
}

async fn assert_failed_mongo_uploads_keep_previous_blob(
    connection: &MongoConnection,
    store: &MongoBlobStore,
) {
    store
        .put("replace", b"original".to_vec(), None)
        .await
        .unwrap();
    let short: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(b"short"))]));
    assert_eq!(
        store
            .put_stream("replace", short, 6, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );
    let long: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(b"longer"))]));
    assert_eq!(
        store
            .put_stream("replace", long, 3, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );

    let chunks = connection
        .database()
        .collection::<Document>(&format!("{BUCKET_NAME}.chunks"));
    let chunks_before = chunks.count_documents(doc! {}).await.unwrap();
    let broken: BlobReader = Box::pin(stream::iter(vec![
        Ok(bytes::Bytes::from(vec![7; 512 * 1024])),
        Err(StorageError::backend("input stream failed")),
    ]));
    assert_eq!(
        store
            .put_stream("replace", broken, 1_000_000, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );
    assert_eq!(
        chunks.count_documents(doc! {}).await.unwrap(),
        chunks_before
    );
}
