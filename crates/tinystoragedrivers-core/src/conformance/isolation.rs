//! Scope and database isolation checks.

use serde_json::json;

use super::{fails, ok, unique};
use crate::backend::StorageBackend;
use crate::document::Precondition;
use crate::error::ErrorKind;
use crate::filter::Filter;
use crate::scope::Scope;

/// Two scopes on one backend never see each other's data.
pub(super) async fn run(backend: &dyn StorageBackend) {
    let a = ok(backend.for_scope(&Scope::local()), "scope a");
    let b = ok(
        backend.for_scope(&Scope::new("conformance-b").expect("valid scope")),
        "scope b",
    );
    let coll = unique("iso");
    let stream = unique("iso_stream");
    let key = format!("{}/k", unique("iso_blob"));

    ok(
        a.documents()
            .put(&coll, "same", json!({"owner": "a"}), Precondition::None)
            .await,
        "a put",
    );
    assert!(
        ok(b.documents().get(&coll, "same").await, "b get").is_none(),
        "documents leak"
    );
    ok(
        b.documents()
            .put(&coll, "same", json!({"owner": "b"}), Precondition::Absent)
            .await,
        "b may create the same id",
    );
    assert_eq!(
        ok(a.documents().count(&coll, &Filter::All).await, "a count"),
        1
    );
    assert_eq!(
        ok(
            b.documents().delete_where(&coll, &Filter::All).await,
            "b delete_where"
        ),
        1,
        "delete_where stays in its scope"
    );
    let still =
        ok(a.documents().get(&coll, "same").await, "a re-read").expect("a's document survives");
    assert_eq!(still.doc["owner"], "a");
    assert!(
        ok(
            b.documents()
                .claim(&coll, &Filter::All, &[], &json!({"x": 1}))
                .await,
            "b claim"
        )
        .is_none(),
        "claims stay in their scope"
    );
    ok(b.documents().drop_collection(&coll).await, "b drop");
    assert!(ok(a.documents().get(&coll, "same").await, "a after b drop").is_some());

    ok(a.streams().append(&stream, json!(1)).await, "a append");
    assert_eq!(
        ok(b.streams().len(&stream).await, "b len"),
        0,
        "streams leak"
    );
    assert!(ok(b.streams().streams(&stream).await, "b list").is_empty());

    ok(a.blobs().put(&key, vec![1], None).await, "a blob");
    assert!(
        ok(b.blobs().get(&key).await, "b blob").is_none(),
        "blobs leak"
    );
    assert!(ok(b.blobs().list(&key).await, "b list blobs").is_empty());

    assert_eq!(b.scope().as_str(), "conformance-b");
}

/// Named databases on one backend are independent of each other and of the
/// root.
pub(super) async fn databases(backend: &dyn StorageBackend) {
    let first = ok(backend.database("conformance_one"), "database one");
    let again = ok(backend.database("conformance_one"), "database one again");
    let second = ok(backend.database("conformance_two"), "database two");
    let coll = unique("db");

    let one = ok(first.for_scope(&Scope::local()), "scope one");
    let one_again = ok(again.for_scope(&Scope::local()), "scope one again");
    let two = ok(second.for_scope(&Scope::local()), "scope two");
    let root = ok(backend.for_scope(&Scope::local()), "root scope");

    ok(
        one.documents()
            .put(&coll, "x", json!({}), Precondition::None)
            .await,
        "write one",
    );
    assert!(
        ok(
            one_again.documents().get(&coll, "x").await,
            "same name, same data"
        )
        .is_some()
    );
    assert!(ok(two.documents().get(&coll, "x").await, "other database").is_none());
    assert!(ok(root.documents().get(&coll, "x").await, "root").is_none());

    fails(
        backend.database("Bad Name").map(|_| ()),
        ErrorKind::InvalidInput,
        "invalid database",
    );
}
