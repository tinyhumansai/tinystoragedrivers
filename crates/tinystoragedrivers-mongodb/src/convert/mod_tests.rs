//! Round trips and the values that cannot make one.

use super::*;
use mongodb::bson::{doc, oid::ObjectId};
use serde_json::json;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn round_trips_every_json_shape() {
    let value = json!({
        "null": null,
        "flag": true,
        "int": -3,
        "big": i64::MAX,
        "float": 1.5,
        "whole_float": 2.0,
        "text": "hi",
        "list": [1, "two", [3], {"four": 4}],
        "nested": {"$date": "not a date", "$oid": "nor an id"},
    });
    let Value::Object(map) = &value else {
        unreachable!("literal is an object")
    };
    let doc = to_document(map).unwrap();
    assert_eq!(doc.get("int"), Some(&Bson::Int64(-3)));
    assert_eq!(doc.get("whole_float"), Some(&Bson::Double(2.0)));
    assert!(matches!(
        doc.get_document("nested").unwrap().get("$date"),
        Some(Bson::String(_))
    ));
    assert_eq!(from_document(&doc).unwrap(), value);
}

#[test]
fn rejects_integers_beyond_i64() {
    let error = to_bson(&json!({"n": u64::MAX})).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert_eq!(exact_bson(&json!([u64::MAX])), None);
    assert_eq!(exact_bson(&json!(u64::MAX)), None);
    assert_eq!(exact_bson(&json!(7_u64)), Some(Bson::Int64(7)));
}

#[test]
fn reads_int32_and_rejects_foreign_types() {
    assert_eq!(from_bson(&Bson::Int32(4)).unwrap(), json!(4));
    for foreign in [
        Bson::ObjectId(ObjectId::new()),
        Bson::Double(f64::NAN),
        Bson::Document(doc! {"when": Bson::DateTime(mongodb::bson::DateTime::now())}),
        Bson::Array(vec![Bson::Double(f64::INFINITY)]),
    ] {
        let error = from_bson(&foreign).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Serialization, "{foreign:?}");
    }
}
