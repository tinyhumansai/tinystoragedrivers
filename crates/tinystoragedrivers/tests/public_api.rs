//! The facade's public surface: open from a URL and pass the conformance suite.

use tinystoragedrivers::{StorageConfig, conformance};

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    conformance::run(backend.as_ref(), false).await;
}

#[cfg(feature = "secrets")]
#[tokio::test]
async fn the_secrets_feature_re_exports_the_secret_store_drivers() {
    use std::sync::Arc;
    use tinystoragedrivers::Scope;
    use tinystoragedrivers::secrets::{DerivedKeys, DocumentSecrets, SecretStore, crypto};

    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let secrets =
        DocumentSecrets::new(&storage, Arc::new(DerivedKeys::new(crypto::generate_key())));
    secrets.set("api_key", b"sk-live").await.unwrap();
    assert_eq!(
        secrets.get("api_key").await.unwrap().unwrap().as_slice(),
        b"sk-live"
    );
}
