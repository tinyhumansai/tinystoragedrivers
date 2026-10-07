//! The facade's public surface: open from a URL and pass the conformance suite.

use tinystoragedrivers::{StorageConfig, conformance};

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    conformance::run(backend.as_ref(), false).await;
}

#[cfg(feature = "file")]
#[tokio::test]
async fn a_file_url_opens_a_conforming_backend() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file:{}", dir.path().display());
    let backend = tinystoragedrivers::open(&StorageConfig::parse(&url).unwrap())
        .await
        .unwrap();
    assert_eq!(backend.driver(), "file");
    conformance::run(backend.as_ref(), false).await;
}
