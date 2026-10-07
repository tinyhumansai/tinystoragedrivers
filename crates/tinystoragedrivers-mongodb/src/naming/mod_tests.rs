//! Names, hashes, cursors and the parsing of server messages.

use super::*;
use tinystoragedrivers_core::{ErrorKind, Filter, Query, Sort};

#[test]
fn fnv1a_matches_the_reference_vectors() {
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    assert_eq!(fnv1a(b"foobar"), 0x8594_4171_f739_67e8);
}

#[test]
fn index_names_are_stable_and_reserved() {
    let name = index_name("by email");
    assert_eq!(name, index_name("by email"));
    assert_ne!(name, index_name("by_email"));
    assert!(name.starts_with("_tsd_ix_"));
    assert_eq!(name.len(), "_tsd_ix_".len() + 16);
}

#[test]
fn prefixes_and_ids() {
    assert_eq!(database_prefix("approvals"), "approvals:");
    let scope = Scope::new("alice").unwrap();
    assert_eq!(document_id(&scope, "k"), doc! {"s": "alice", "k": "k"});
}

#[test]
fn cursors_bind_to_their_query() {
    let query = Query::filter(Filter::eq("a", 1)).sort(Sort::asc("b"));
    assert_eq!(decode_cursor("c", &query).unwrap(), 0);
    let cursor = encode_cursor("c", &query, 7);
    assert!(cursor.0.starts_with("mongo:"));
    assert_eq!(
        decode_cursor("c", &query.clone().after(cursor.clone())).unwrap(),
        7
    );

    let foreign = [
        decode_cursor("other", &query.clone().after(cursor.clone())),
        decode_cursor("c", &Query::all().after(cursor.clone())),
        decode_cursor("c", &query.clone().sort(Sort::desc("x")).after(cursor)),
        decode_cursor("c", &query.clone().after(Cursor("mongo:zz:1".into()))),
        decode_cursor("c", &query.after(Cursor("forged".into()))),
    ];
    for result in foreign {
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn prefix_regexes_escape_metacharacters() {
    let Bson::RegularExpression(regex) = prefix_regex("a.b*(c)/d") else {
        panic!("expected a regex")
    };
    assert_eq!(regex.pattern, "^a\\.b\\*\\(c\\)/d");
    assert_eq!(regex.options, "");
    let Bson::RegularExpression(empty) = prefix_regex("") else {
        panic!("expected a regex")
    };
    assert_eq!(empty.pattern, "^");
}

#[test]
fn mongo_database_names() {
    for good in ["openhuman", "tsd_test", "a-b", &"x".repeat(63)] {
        assert!(validate_mongo_database(good).is_ok(), "{good}");
    }
    for bad in ["", "a.b", "a b", "a/b", "a$", "a\0", &"x".repeat(64)] {
        assert_eq!(
            validate_mongo_database(bad).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{bad:?}"
        );
    }
}

#[test]
fn reads_the_index_out_of_duplicate_key_messages() {
    assert_eq!(
        duplicate_key_index(
            "E11000 duplicate key error collection: db.c index: _tsd_ix_00ff dup key: { x: 1 }"
        ),
        Some("_tsd_ix_00ff")
    );
    assert_eq!(
        duplicate_key_index("E11000 duplicate key error collection: db.c index: _id_ dup key"),
        Some("_id_")
    );
    assert_eq!(duplicate_key_index("something else"), None);
    assert_eq!(duplicate_key_index("index: "), None);
}
