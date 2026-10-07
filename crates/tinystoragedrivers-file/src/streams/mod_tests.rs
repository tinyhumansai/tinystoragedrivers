//! Stream edge cases: torn appends, interrupted truncation, long lines, and
//! damaged files.

use std::path::Path;

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, StorageBackend};

use super::*;
use crate::FileStorage;

fn open() -> (tempfile::TempDir, FileStorage, Arc<dyn StreamStore>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let streams = Arc::clone(storage.for_scope(&Scope::local()).unwrap().streams());
    (dir, storage, streams)
}

fn files(storage: &FileStorage, name: &str) -> (PathBuf, PathBuf) {
    let dir = storage.dir().join("scopes/local/streams");
    let stem = file_stem(name);
    (
        dir.join(format!("{stem}.jsonl")),
        dir.join(format!("{stem}.meta.json")),
    )
}

fn append_raw(path: &Path, bytes: &[u8]) {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
}

#[tokio::test]
async fn a_torn_final_line_is_ignored_then_replaced() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    append_raw(&data, br#"{"offset":1,"val"#);
    assert_eq!(streams.len("s").await.unwrap(), 1);
    assert_eq!(streams.read_window("s", 0, 10).await.unwrap().len(), 1);
    assert_eq!(streams.append("s", json!(1)).await.unwrap(), 1);
    let text = std::fs::read_to_string(&data).unwrap();
    assert_eq!(
        text,
        "{\"offset\":0,\"value\":0}\n{\"offset\":1,\"value\":1}\n"
    );
}

#[tokio::test]
async fn an_interrupted_truncation_still_hides_dropped_entries() {
    let (_dir, storage, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1), json!(2)])
        .await
        .unwrap();
    // Simulate a crash after raising `base` but before rewriting the file.
    let (_, meta) = files(&storage, "s");
    std::fs::write(&meta, br#"{"name":"s","base":2}"#).unwrap();
    let offsets: Vec<_> = streams
        .read_window("s", 0, 10)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.offset)
        .collect();
    assert_eq!(offsets, [2]);
    assert_eq!(streams.len("s").await.unwrap(), 3);
    assert_eq!(streams.truncate_before("s", 1).await.unwrap(), 0);
}

#[tokio::test]
async fn a_truncated_stream_keeps_its_length_without_a_data_file() {
    let (_dir, storage, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1)])
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("s", 9).await.unwrap(), 2);
    let (data, _) = files(&storage, "s");
    std::fs::remove_file(&data).unwrap();
    assert_eq!(streams.len("s").await.unwrap(), 2);
    assert_eq!(streams.read_window("s", 0, 5).await.unwrap().len(), 0);
    assert_eq!(streams.append_batch("s", vec![]).await.unwrap(), 2);
    assert_eq!(streams.append("s", json!(2)).await.unwrap(), 2);
}

#[tokio::test]
async fn lines_longer_than_a_read_chunk_are_found() {
    let (_dir, _storage, streams) = open();
    let big = "x".repeat(20_000);
    streams
        .append_batch("s", vec![json!(big), json!(big)])
        .await
        .unwrap();
    assert_eq!(streams.len("s").await.unwrap(), 2);
    assert_eq!(streams.append("s", json!(big)).await.unwrap(), 2);
    let window = streams.read_window("s", 1, 1).await.unwrap();
    assert_eq!(window[0].value, json!(big));
}

#[tokio::test]
async fn a_corrupt_line_is_a_serialization_error() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    append_raw(&data, b"garbage\n");
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        streams.read_window("s", 0, 10).await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn a_meta_file_for_another_stream_is_a_collision() {
    let (_dir, storage, streams) = open();
    let name = "y".repeat(300);
    let (_, meta) = files(&storage, &name);
    std::fs::create_dir_all(meta.parent().unwrap()).unwrap();
    std::fs::write(&meta, br#"{"name":"other","base":0}"#).unwrap();
    assert_eq!(
        streams.len(&name).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.streams("").await.unwrap(),
        ["other"],
        "listing reports the name the file holds"
    );
}

#[tokio::test]
async fn reading_an_unopenable_stream_file_fails() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    std::fs::remove_file(&data).unwrap();
    // A directory in the data file's place cannot be read as a stream.
    std::fs::create_dir(&data).unwrap();
    assert_eq!(
        streams.read_window("s", 0, 5).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(format!("{streams:?}").contains("local"));
}

#[test]
fn length_prefers_the_larger_of_base_and_the_last_line() {
    let meta = StreamMeta {
        name: "s".into(),
        base: 5,
    };
    assert_eq!(length(&meta, None).unwrap(), 5);
    assert_eq!(
        length(&meta, Some(br#"{"offset":2,"value":0}"#)).unwrap(),
        5
    );
    assert_eq!(
        length(&meta, Some(br#"{"offset":7,"value":0}"#)).unwrap(),
        8
    );
}

#[tokio::test]
async fn an_exhausted_offset_space_is_an_error() {
    let (_dir, storage, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&storage, "s");
    std::fs::write(
        &data,
        format!("{{\"offset\":{},\"value\":0}}\n", u64::MAX - 1),
    )
    .unwrap();
    assert_eq!(streams.len("s").await.unwrap(), u64::MAX);
    assert_eq!(streams.append_batch("s", vec![]).await.unwrap(), u64::MAX);
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    std::fs::write(&data, format!("{{\"offset\":{},\"value\":0}}\n", u64::MAX)).unwrap();
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn a_new_stream_ignores_an_orphaned_data_file() {
    let (_dir, storage, streams) = open();
    let (data, _) = files(&storage, "s");
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    std::fs::write(&data, "{\"offset\":7,\"value\":0}\n").unwrap();
    assert_eq!(streams.append("s", json!(1)).await.unwrap(), 0);
    assert_eq!(streams.len("s").await.unwrap(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn a_symlinked_data_file_is_refused_not_followed() {
    let (dir, storage, streams) = open();
    let (data, _) = files(&storage, "s");
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, "{\"offset\":0,\"value\":\"secret\"}\n").unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    // Appending must neither write through the link nor truncate its target,
    // and a failed append leaves no stream behind.
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "{\"offset\":0,\"value\":\"secret\"}\n"
    );
    assert_eq!(streams.len("s").await.unwrap(), 0);
    assert_eq!(streams.streams("").await.unwrap().len(), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn reads_and_truncation_refuse_a_symlinked_data_file() {
    let (dir, storage, streams) = open();
    streams.append("s", json!(1)).await.unwrap();
    streams.append("s", json!(2)).await.unwrap();
    let (data, _) = files(&storage, "s");
    let victim = dir.path().join("victim");
    std::fs::rename(&data, &victim).unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    for error in [
        streams.len("s").await.unwrap_err(),
        streams.read_window("s", 0, 10).await.unwrap_err(),
        streams.truncate_before("s", 1).await.unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::Backend);
    }
}
