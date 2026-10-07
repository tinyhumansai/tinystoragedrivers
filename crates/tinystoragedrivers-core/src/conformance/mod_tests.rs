//! The suite's own helpers, and that it catches a broken driver.

use super::*;
use crate::{MemoryStorage, StorageError};

#[test]
fn unique_names_never_repeat() {
    let a = unique("x");
    let b = unique("x");
    assert_ne!(a, b);
    assert!(crate::validate_collection(&a).is_ok());
}

#[test]
fn ok_and_fails_report_mismatches() {
    assert_eq!(ok(Ok::<_, StorageError>(3), "check"), 3);
    fails(
        Err::<(), _>(StorageError::conflict("x")),
        ErrorKind::Conflict,
        "check",
    );
    let caught =
        std::panic::catch_unwind(|| ok(Err::<(), _>(StorageError::backend("boom")), "named"));
    assert!(caught.is_err());
    let caught =
        std::panic::catch_unwind(|| fails(Ok::<_, StorageError>(1), ErrorKind::Conflict, "named"));
    assert!(caught.is_err());
}

#[tokio::test]
async fn single_scope_mode_requires_foreign_scopes_to_fail() {
    let outcome = tokio::spawn(async {
        run(&MemoryStorage::new(), true).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "a multi-scope backend fails the single-scope check"
    );
}

/// A driver with no optional capabilities, built by narrowing the memory
/// driver, so the suite's "without the capability" branches run.
mod baseline {
    use std::sync::Arc;

    use serde_json::Value;

    use crate::{
        Capabilities, Capability, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query,
        Result, Scope, ScopedStorage, Sort, StorageBackend, StorageError, Version, Versioned,
    };

    #[derive(Debug)]
    pub(super) struct Narrow(pub(super) crate::MemoryStorage);

    #[derive(Debug)]
    struct NarrowDocs(Arc<dyn DocumentStore>);

    #[async_trait::async_trait]
    impl DocumentStore for NarrowDocs {
        fn capabilities(&self) -> Capabilities {
            Capabilities::none()
        }
        async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
            if spec.ttl_field.is_some() {
                return Err(StorageError::unsupported(Capability::Ttl, "no expiry"));
            }
            self.0.ensure_collection(spec).await
        }
        async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
            self.0.get(c, id).await
        }
        async fn put(&self, c: &str, id: &str, d: Value, p: Precondition) -> Result<Version> {
            self.0.put(c, id, d, p).await
        }
        async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
            self.0.delete(c, id, p).await
        }
        async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
            self.0.query(c, q).await
        }
        async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.count(c, f).await
        }
        async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.delete_where(c, f).await
        }
        async fn claim(
            &self,
            c: &str,
            f: &Filter,
            s: &[Sort],
            p: &Value,
        ) -> Result<Option<Versioned<Value>>> {
            self.0.claim(c, f, s, p).await
        }
        async fn drop_collection(&self, c: &str) -> Result<()> {
            self.0.drop_collection(c).await
        }
    }

    impl StorageBackend for Narrow {
        fn driver(&self) -> &'static str {
            "narrow"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::none()
        }
        fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
            let inner = self.0.for_scope(scope)?;
            Ok(ScopedStorage::new(
                scope.clone(),
                self.driver(),
                Arc::new(NarrowDocs(Arc::clone(inner.documents()))),
                Arc::clone(inner.streams()),
                Arc::clone(inner.blobs()),
            ))
        }
        fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
            self.0.database(name)
        }
    }
}

#[tokio::test]
async fn a_driver_without_optional_capabilities_passes() {
    run(&baseline::Narrow(MemoryStorage::new()), false).await;
}
