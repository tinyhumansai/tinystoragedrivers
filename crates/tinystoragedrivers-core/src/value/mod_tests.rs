//! Path lookup, cross-type ordering, merge patches and tokenizing.

use std::cmp::Ordering;

use serde_json::json;

use super::*;

#[test]
fn lookup_walks_objects_and_arrays() {
    let doc = json!({"a": {"b": [10, {"c": true}]}});
    assert_eq!(lookup(&doc, "a.b.0"), Some(&json!(10)));
    assert_eq!(lookup(&doc, "a.b.1.c"), Some(&json!(true)));
    assert_eq!(lookup(&doc, "a.b.x"), None);
    assert_eq!(lookup(&doc, "a.b.9"), None);
    assert_eq!(lookup(&doc, "a.b.0.deeper"), None);
}

#[test]
fn orders_across_types() {
    let ascending = [
        json!(null),
        json!(false),
        json!(true),
        json!(-3),
        json!(2.5),
        json!(10_u64),
        json!(""),
        json!("a"),
        json!([]),
        json!([1]),
        json!({}),
    ];
    for pair in ascending.windows(2) {
        assert_eq!(compare(&pair[0], &pair[1]), Ordering::Less, "{pair:?}");
        assert_eq!(compare(&pair[1], &pair[0]), Ordering::Greater, "{pair:?}");
    }
}

#[test]
fn numbers_compare_by_value() {
    assert!(equal(&json!(1), &json!(1.0)));
    assert_eq!(compare(&json!(u64::MAX), &json!(1_u64)), Ordering::Greater);
    assert_eq!(compare(&json!(-1), &json!(0.5)), Ordering::Less);
}

#[test]
fn arrays_and_objects_compare_structurally() {
    assert_eq!(compare(&json!([1, 2]), &json!([1, 3])), Ordering::Less);
    assert_eq!(compare(&json!([1, 2]), &json!([1, 2, 0])), Ordering::Less);
    assert!(equal(&json!({"a": 1, "b": 2}), &json!({"b": 2, "a": 1})));
    assert_eq!(compare(&json!({"a": 1}), &json!({"a": 2})), Ordering::Less);
    assert_eq!(compare(&json!({"a": 1}), &json!({"b": 0})), Ordering::Less);
    assert_eq!(
        compare(&json!({"a": 1}), &json!({"a": 1, "b": 1})),
        Ordering::Less
    );
}

#[test]
fn merge_patch_follows_rfc_7396() {
    let mut doc = json!({"a": "b", "c": {"d": "e", "f": "g"}});
    merge_patch(&mut doc, &json!({"a": "z", "c": {"f": null}}));
    assert_eq!(doc, json!({"a": "z", "c": {"d": "e"}}));

    let mut scalar = json!("old");
    merge_patch(&mut scalar, &json!({"new": 1}));
    assert_eq!(scalar, json!({"new": 1}));

    let mut replaced = json!({"a": 1});
    merge_patch(&mut replaced, &json!([1, 2]));
    assert_eq!(replaced, json!([1, 2]));
}

#[test]
fn tokens_split_and_lowercase_every_string() {
    let doc = json!({"title": "Hello, World", "tags": ["Rust-lang"], "n": 3});
    let mut found = tokens(&doc);
    found.sort();
    assert_eq!(found, ["hello", "lang", "rust", "world"]);
}
