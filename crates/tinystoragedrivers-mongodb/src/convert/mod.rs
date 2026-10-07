//! JSON to BSON and back, preserving exactly what JSON can say.
//!
//! The `bson` crate's own `From<serde_json::Value>` reads extended JSON, so a
//! user document holding `{"$date": …}` would come back as a date. This module
//! converts structurally instead: objects become documents, integers become
//! `Int64`, other numbers `Double`, and nothing is interpreted.
//!
//! Integers above `i64::MAX` have no BSON integer type. A document holding one
//! is rejected with [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
//! rather than silently rounded to a double; filter values that cannot convert
//! exactly are reported as `None` so the caller can fall back to evaluating the
//! filter in Rust.

use mongodb::bson::{Bson, Document};
use serde_json::{Map, Number, Value};
use tinystoragedrivers_core::{Result, StorageError};

/// Convert a JSON value to BSON.
///
/// # Errors
///
/// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
/// for an integer above `i64::MAX`.
pub(crate) fn to_bson(value: &Value) -> Result<Bson> {
    exact_bson(value).ok_or_else(|| {
        StorageError::serialization(
            "integers above 9223372036854775807 cannot be stored in MongoDB",
        )
    })
}

/// Convert a JSON object to a BSON document.
///
/// # Errors
///
/// As [`to_bson`].
pub(crate) fn to_document(map: &Map<String, Value>) -> Result<Document> {
    map.iter()
        .map(|(key, value)| Ok((key.clone(), to_bson(value)?)))
        .collect()
}

/// The exact BSON form of `value`, or `None` when it holds an integer above
/// `i64::MAX`.
pub(crate) fn exact_bson(value: &Value) -> Option<Bson> {
    Some(match value {
        Value::Null => Bson::Null,
        Value::Bool(flag) => Bson::Boolean(*flag),
        Value::Number(number) => number_bson(number)?,
        Value::String(text) => Bson::String(text.clone()),
        Value::Array(items) => Bson::Array(items.iter().map(exact_bson).collect::<Option<_>>()?),
        Value::Object(map) => Bson::Document(
            map.iter()
                .map(|(key, item)| Some((key.clone(), exact_bson(item)?)))
                .collect::<Option<Document>>()?,
        ),
    })
}

fn number_bson(number: &Number) -> Option<Bson> {
    if let Some(int) = number.as_i64() {
        return Some(Bson::Int64(int));
    }
    if number.is_u64() {
        // Only integers above i64::MAX reach here.
        return None;
    }
    number.as_f64().map(Bson::Double)
}

/// Convert stored BSON back to JSON.
///
/// # Errors
///
/// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
/// for a BSON type JSON has no form for (dates, object ids, binary data) or a
/// non-finite double. This driver never writes those, so they only appear when
/// something else wrote to its collections.
pub(crate) fn from_bson(value: &Bson) -> Result<Value> {
    Ok(match value {
        Bson::Null => Value::Null,
        Bson::Boolean(flag) => Value::Bool(*flag),
        Bson::Int32(int) => Value::from(*int),
        Bson::Int64(int) => Value::from(*int),
        Bson::Double(float) => Number::from_f64(*float).map(Value::Number).ok_or_else(|| {
            StorageError::serialization("stored document holds a non-finite number")
        })?,
        Bson::String(text) => Value::String(text.clone()),
        Bson::Array(items) => Value::Array(items.iter().map(from_bson).collect::<Result<_>>()?),
        Bson::Document(doc) => from_document(doc)?,
        other => {
            return Err(StorageError::serialization(format!(
                "stored document holds a {:?} value, which JSON cannot represent",
                other.element_type()
            )));
        }
    })
}

/// Convert a stored BSON document back to a JSON object.
///
/// # Errors
///
/// As [`from_bson`].
pub(crate) fn from_document(doc: &Document) -> Result<Value> {
    doc.iter()
        .map(|(key, value)| Ok((key.clone(), from_bson(value)?)))
        .collect::<Result<Map<String, Value>>>()
        .map(Value::Object)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
