//! JSON value helpers that every driver must apply identically.
//!
//! Filters, sorts and patches are evaluated by the backend where it can (SQL,
//! MongoDB query operators) and by these functions where it cannot (the memory
//! and file drivers, and post-filtering). Keeping one definition here is what
//! makes "the same query returns the same documents on every driver" testable.

use std::cmp::Ordering;

use serde_json::{Map, Value};

/// Look up a dotted `path` (`"owner.name"`) inside `value`.
///
/// Each segment indexes an object by key, or an array by a decimal position.
/// Returns `None` when any segment is missing.
///
/// ```
/// use serde_json::json;
/// use tinystoragedrivers_core::value::lookup;
///
/// let doc = json!({"owner": {"name": "ada"}, "tags": ["a", "b"]});
/// assert_eq!(lookup(&doc, "owner.name"), Some(&json!("ada")));
/// assert_eq!(lookup(&doc, "tags.1"), Some(&json!("b")));
/// assert_eq!(lookup(&doc, "owner.age"), None);
/// ```
#[must_use]
pub fn lookup<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(value, |current, segment| match current {
            Value::Object(map) => map.get(segment),
            Value::Array(items) => segment.parse::<usize>().ok().and_then(|i| items.get(i)),
            _ => None,
        })
}

/// Rank of a JSON type in the cross-type order: null, booleans, numbers,
/// strings, arrays, objects. Matches MongoDB's BSON comparison order for the
/// types JSON has.
fn type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// A total order over JSON values.
///
/// Values of different types order by type (null < bool < number < string <
/// array < object). Numbers compare numerically, strings by byte order, arrays
/// element by element then by length, and objects by their sorted entries.
///
/// ```
/// use std::cmp::Ordering;
/// use serde_json::json;
/// use tinystoragedrivers_core::value::compare;
///
/// assert_eq!(compare(&json!(2), &json!(10)), Ordering::Less);
/// assert_eq!(compare(&json!("10"), &json!(2)), Ordering::Greater);
/// assert_eq!(compare(&json!(null), &json!(false)), Ordering::Less);
/// ```
#[must_use]
pub fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => compare_numbers(x, y),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => x
            .iter()
            .zip(y)
            .map(|(left, right)| compare(left, right))
            .find(|ordering| ordering.is_ne())
            .unwrap_or_else(|| x.len().cmp(&y.len())),
        (Value::Object(x), Value::Object(y)) => {
            let mut left: Vec<_> = x.iter().collect();
            let mut right: Vec<_> = y.iter().collect();
            left.sort_by(|p, q| p.0.cmp(q.0));
            right.sort_by(|p, q| p.0.cmp(q.0));
            left.iter()
                .zip(&right)
                .map(|((ka, va), (kb, vb))| ka.cmp(kb).then_with(|| compare(va, vb)))
                .find(|ordering| ordering.is_ne())
                .unwrap_or_else(|| left.len().cmp(&right.len()))
        }
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

fn compare_numbers(x: &serde_json::Number, y: &serde_json::Number) -> Ordering {
    if let (Some(a), Some(b)) = (x.as_i64(), y.as_i64()) {
        return a.cmp(&b);
    }
    if let (Some(a), Some(b)) = (x.as_u64(), y.as_u64()) {
        return a.cmp(&b);
    }
    let a = x.as_f64().unwrap_or(f64::NAN);
    let b = y.as_f64().unwrap_or(f64::NAN);
    a.total_cmp(&b)
}

/// Whether two values are equal under [`compare`] (so `1` equals `1.0`).
#[must_use]
pub fn equal(a: &Value, b: &Value) -> bool {
    compare(a, b).is_eq()
}

/// Apply an RFC 7396 JSON merge patch to `target` in place.
///
/// Object members of `patch` are merged recursively, a `null` member deletes
/// the key, and any non-object `patch` replaces `target` outright.
///
/// ```
/// use serde_json::json;
/// use tinystoragedrivers_core::value::merge_patch;
///
/// let mut doc = json!({"state": "queued", "owner": {"name": "a", "pid": 1}});
/// merge_patch(&mut doc, &json!({"state": "running", "owner": {"pid": null}}));
/// assert_eq!(doc, json!({"state": "running", "owner": {"name": "a"}}));
/// ```
pub fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(patch_map) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    if let Value::Object(target_map) = target {
        for (key, member) in patch_map {
            if member.is_null() {
                target_map.remove(key);
            } else {
                merge_patch(target_map.entry(key.clone()).or_insert(Value::Null), member);
            }
        }
    }
}

/// Lower-cased alphanumeric tokens of every string inside `value`.
///
/// This is the tokenizer the reference full-text search uses; drivers with a
/// native text index may tokenize differently, so conformance only checks
/// single-token membership.
#[must_use]
pub fn tokens(value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    collect_tokens(value, &mut out);
    out
}

fn collect_tokens(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.extend(
            text.split(|c: char| !c.is_alphanumeric())
                .filter(|token| !token.is_empty())
                .map(str::to_lowercase),
        ),
        Value::Array(items) => items.iter().for_each(|item| collect_tokens(item, out)),
        Value::Object(map) => map.values().for_each(|item| collect_tokens(item, out)),
        _ => {}
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
