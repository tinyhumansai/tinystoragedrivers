//! The facade's public surface: open from a URL and pass the conformance suite.

use tinystoragedrivers::{StorageConfig, conformance};

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    conformance::run(backend.as_ref(), false).await;
}
