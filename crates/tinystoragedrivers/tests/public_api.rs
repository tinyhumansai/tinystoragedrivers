//! The facade's public surface: open from a URL and pass the conformance suite.

use tinystoragedrivers::{StorageConfig, conformance};

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    conformance::run(backend.as_ref(), false).await;
}

/// With the `mongodb` feature and `TSD_MONGO_URL` set, a MongoDB URL opens a
/// conforming backend too.
#[cfg(feature = "mongodb")]
#[tokio::test]
async fn live_a_mongodb_url_opens_a_conforming_backend() {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!(
            "live_a_mongodb_url_opens_a_conforming_backend: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let backend = tinystoragedrivers::open(&StorageConfig::parse(&url).unwrap())
        .await
        .unwrap();
    assert_eq!(backend.driver(), "mongodb");
    conformance::run(backend.as_ref(), false).await;
}
