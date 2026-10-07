//! The shape and exactness of every translation.
//!
//! Whether a translation selects the right documents is checked against a
//! live server in `tests/live.rs`, which compares this driver with the memory
//! driver over a set of awkward documents. These tests pin the queries.

use super::*;
use serde_json::json;

fn guard(field: &str) -> Document {
    doc! {field: {"$not": {"$type": "array"}}}
}

#[test]
fn all_and_and_or_combine() {
    assert_eq!(filter(&Filter::All), Translated::everything());
    assert_eq!(
        filter(&Filter::And { filters: vec![] }),
        Translated::everything()
    );
    assert_eq!(
        filter(&Filter::Or { filters: vec![] }),
        Translated::nothing()
    );
    assert_eq!(
        filter(&Filter::All.and(Filter::exists("_id", true))),
        Translated::everything()
    );

    let both = filter(&Filter::eq("a", 1).and(Filter::eq("_id", "x")));
    assert!(both.exact);
    assert_eq!(
        both.query,
        doc! {"$and": [
            {"$and": [guard("d.a"), {"d.a": {"$eq": 1_i64}}]},
            {"_key": "x"},
        ]}
    );

    let single = filter(&Filter::And {
        filters: vec![Filter::All, Filter::eq("_id", "x")],
    });
    assert_eq!(single.query, doc! {"_key": "x"});

    let either = filter(&Filter::eq("_id", "a").or(Filter::eq("_id", "b")));
    assert_eq!(
        either,
        Translated::exact(doc! {"$or": [{"_key": "a"}, {"_key": "b"}]})
    );
    let open = filter(&Filter::eq("_id", "a").or(Filter::All));
    assert_eq!(open, Translated::everything());
}

#[test]
fn equality_guards_every_segment() {
    let nested = filter(&Filter::eq("owner.name", "ada"));
    assert!(nested.exact);
    assert_eq!(
        nested.query,
        doc! {"$and": [
            guard("d.owner"),
            guard("d.owner.name"),
            {"d.owner.name": {"$eq": "ada"}},
        ]}
    );
    let null = filter(&Filter::eq("x", Value::Null));
    assert!(null.exact);
    assert_eq!(
        null.query,
        doc! {"$and": [guard("d.x"), {"d.x": {"$type": "null"}}]}
    );
    for value in [json!(true), json!(1.5), json!("s")] {
        assert!(filter(&Filter::eq("x", value)).exact);
    }
}

#[test]
fn structured_or_unrepresentable_values_only_check_presence() {
    for value in [json!([1]), json!({"a": 1}), json!(u64::MAX)] {
        let translated = filter(&Filter::eq("x", value));
        assert!(!translated.exact);
        assert_eq!(translated.query, doc! {"d.x": {"$exists": true}});
    }
    let nested = filter(&Filter::eq("a.b", json!([1])));
    assert_eq!(
        nested.query,
        doc! {"$and": [guard("d.a"), {"d.a.b": {"$exists": true}}]}
    );
}

#[test]
fn ids_compare_as_strings() {
    assert_eq!(filter(&Filter::eq("_id", 1)), Translated::nothing());
    assert_eq!(
        filter(&Filter::ne("_id", "a")),
        Translated::exact(doc! {"$nor": [{"_key": "a"}]})
    );
    assert_eq!(
        filter(&Filter::gte("_id", "a").and(Filter::lt("_id", "m"))).query,
        doc! {"$and": [{"_key": {"$gte": "a"}}, {"_key": {"$lt": "m"}}]}
    );
    assert_eq!(filter(&Filter::gt("_id", 3)), Translated::nothing());
    assert_eq!(
        filter(&Filter::one_of("_id", ["a"])),
        Translated::exact(doc! {"$or": [{"_key": "a"}]})
    );
    assert_eq!(
        filter(&Filter::one_of("_id", Vec::<String>::new())),
        Translated::nothing()
    );
    assert_eq!(
        filter(&Filter::exists("_id", true)),
        Translated::everything()
    );
    assert_eq!(
        filter(&Filter::exists("_id", false)),
        Translated::exact(doc! {"$nor": [{}]})
    );
}

#[test]
fn ranges_keep_scalar_bounds_and_degrade_otherwise() {
    let rank = filter(&Filter::Range {
        field: "rank".into(),
        gt: Some(json!(1)),
        gte: None,
        lt: Some(json!("z")),
        lte: None,
    });
    assert!(rank.exact);
    assert_eq!(
        rank.query,
        doc! {"$and": [guard("d.rank"), {"d.rank": {"$gt": 1_i64, "$lt": "z"}}]}
    );
    for bound in [json!(null), json!([1]), json!({"a": 1}), json!(u64::MAX)] {
        let translated = filter(&Filter::gte("rank", bound));
        assert!(!translated.exact);
        assert_eq!(translated.query, doc! {"d.rank": {"$exists": true}});
    }
}

#[test]
fn exists_allows_array_values() {
    let present = filter(&Filter::exists("tags", true));
    assert_eq!(
        present,
        Translated::exact(doc! {"d.tags": {"$exists": true}})
    );
    let nested_absent = filter(&Filter::exists("a.b", false));
    assert!(nested_absent.exact);
    assert_eq!(
        nested_absent.query,
        doc! {"$nor": [{"$and": [guard("d.a"), {"d.a.b": {"$exists": true}}]}]}
    );
}

#[test]
fn positional_and_operator_paths_are_supersets() {
    let positional = filter(&Filter::eq("tags.1", "x"));
    assert!(!positional.exact);
    assert_eq!(positional.query, doc! {"d.tags.1": {"$eq": "x"}});
    let first_segment_numeric = filter(&Filter::eq("0.a", "x"));
    assert!(first_segment_numeric.exact);

    let operator = filter(&Filter::eq("a.$b", 1));
    assert_eq!(operator, Translated::superset(Document::new()));
    assert_eq!(
        filter(&Filter::exists("$a", true)),
        Translated::superset(Document::new())
    );
}

#[test]
fn negating_a_superset_defers_to_rust() {
    assert_eq!(
        filter(&Filter::ne("tags.0", "x")),
        Translated::superset(Document::new())
    );
    assert_eq!(
        filter(&Filter::eq("x", json!([1])).negate()),
        Translated::superset(Document::new())
    );
    let mixed = filter(&Filter::eq("x", json!([1])).and(Filter::eq("_id", "a")));
    assert!(!mixed.exact);
    let either = filter(&Filter::one_of("x", [json!([1]), json!(2)]));
    assert!(!either.exact);
    assert!(either.query.contains_key("$or"));
}

#[test]
fn sort_plans_rank_then_value_then_key() {
    let plan = sort_plan(&[Sort::desc("rank"), Sort::asc("_id")]).unwrap();
    assert_eq!(
        plan.sort,
        doc! {"_tsd_r0": -1, "_tsd_k0": -1, "_tsd_r1": 1, "_tsd_k1": 1, "_key": 1}
    );
    assert_eq!(plan.add_fields.get_str("_tsd_k0").unwrap(), "$d.rank");
    assert_eq!(plan.add_fields.get_str("_tsd_k1").unwrap(), "$_key");
    assert!(plan.add_fields.get_document("_tsd_r0").is_ok());
    assert_eq!(plan.unset, ["_tsd_k0", "_tsd_r0", "_tsd_k1", "_tsd_r1"]);
    assert_eq!(
        plan.complex
            .get_document("$expr")
            .unwrap()
            .get_array("$or")
            .unwrap()
            .len(),
        2
    );

    let by_key = sort_plan(&[]).unwrap();
    assert_eq!(by_key.sort, doc! {"_key": 1});
    assert_eq!(by_key.unset, Vec::<String>::new());

    assert_eq!(sort_plan(&[Sort::asc("a.$b")]), None);
    assert_eq!(sort_plan(&[Sort::asc("items.0")]), None);
    assert_eq!(sort_plan(&[Sort::asc("items.0.name")]), None);
    assert!(
        sort_plan(&[Sort::asc("0.name")]).is_some(),
        "the body is an object"
    );
}

#[test]
fn body_fields_live_under_d() {
    assert_eq!(body_field("a.b"), "d.a.b");
}
