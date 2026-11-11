use super::*;

#[test]
fn versions_preconditions_and_versioned_values_keep_contract() {
    assert_eq!(Version::FIRST.next(), Some(Version(2)));
    assert_eq!(Version(u64::MAX).next(), None);
    let item = Versioned {
        id: "doc".into(),
        version: Version(4),
        doc: 3,
        meta: Meta::unknown(),
    };
    assert_eq!(item.unchanged(), Precondition::Version(Version(4)));
    assert_eq!(item.map(|n| n.to_string()).doc, "3");
    assert_eq!(
        serde_json::from_str::<Precondition>("{\"kind\":\"none\"}").unwrap(),
        Precondition::None
    );
    assert_eq!(
        serde_json::from_str::<Precondition>("{\"kind\":\"version\",\"version\":4}").unwrap(),
        Precondition::Version(Version(4))
    );
}

#[test]
fn cursors_bind_to_driver_query_and_order_by_sort_then_id() {
    let position = CursorPosition {
        id: "b".into(),
        values: vec![serde_json::json!(2)],
    };
    let cursor = Cursor::keyset("memory", 7, &position).unwrap();
    assert_eq!(cursor.decode_keyset("memory", 7).unwrap(), position);
    assert!(cursor.decode_keyset("other", 7).is_err());
    assert!(cursor.decode_keyset("memory", 8).is_err());
    let sort = [Sort::asc("rank")];
    let before = CursorPosition {
        id: "z".into(),
        values: vec![serde_json::json!(1)],
    };
    assert_eq!(before.compare(&position, &sort), std::cmp::Ordering::Less);
    assert!(position.validate(&sort).is_ok());
    assert!(
        CursorPosition {
            id: "b".into(),
            values: vec![serde_json::json!("b")]
        }
        .validate(&[Sort::asc(crate::ID_FIELD)])
        .is_ok()
    );
    assert!(
        CursorPosition {
            id: "bad\0id".into(),
            values: vec![serde_json::json!(2)]
        }
        .validate(&sort)
        .is_err()
    );
}

#[test]
fn queries_validate_filter_and_pagination_contract() {
    assert!(
        Query::filter(Filter::eq("state", "ready"))
            .sort(Sort::asc("updated"))
            .limit(5)
            .validate()
            .is_ok()
    );
    assert!(Query::all().limit(0).validate().is_err());
    assert!(
        Query::filter(Filter::eq("bad..path", true))
            .validate()
            .is_err()
    );
    let page = Page {
        items: vec!["row"],
        next: Some(Cursor("next".into())),
    };
    assert_eq!(
        serde_json::from_value::<Page<String>>(serde_json::to_value(page).unwrap())
            .unwrap()
            .next
            .unwrap()
            .0,
        "next"
    );
}

#[test]
fn document_write_and_search_values_keep_wire_tags() {
    let put = WriteOp::Put {
        collection: "jobs".into(),
        id: "a".into(),
        doc: serde_json::json!({"x":1}),
        precondition: Precondition::Absent,
    };
    let put_wire = serde_json::to_value(&put).unwrap();
    assert_eq!(put_wire["op"], "put");
    assert_eq!(serde_json::from_value::<WriteOp>(put_wire).unwrap(), put);
    let put_with = WriteOp::PutWith {
        collection: "jobs".into(),
        id: "a".into(),
        doc: serde_json::json!({"x":1}),
        precondition: Precondition::Absent,
        options: WriteOptions::new().schema(3).expires_ms(9),
    };
    let put_with_wire = serde_json::to_value(&put_with).unwrap();
    assert_eq!(put_with_wire["op"], "put_with");
    assert_eq!(put_with_wire["options"]["schema"], 3);
    assert_eq!(
        serde_json::from_value::<WriteOp>(put_with_wire).unwrap(),
        put_with
    );
    // Options default when omitted, so a hand-written op needs only the body.
    let bare: WriteOp = serde_json::from_value(
        serde_json::json!({"op": "put_with", "collection": "jobs", "id": "a", "doc": 1}),
    )
    .unwrap();
    assert!(matches!(bare, WriteOp::PutWith { options, .. } if options.is_default()));
    let result = WriteResult::Put {
        version: Version(2),
    };
    assert_eq!(
        serde_json::from_value::<WriteResult>(serde_json::to_value(result).unwrap()).unwrap(),
        result
    );
    assert_eq!(
        serde_json::from_value::<SearchHit>(serde_json::json!({"id":"a","score":1.0}))
            .unwrap()
            .id,
        "a"
    );
}

#[test]
fn read_dependencies_round_trip_and_reject_invalid_addresses() {
    for (expected, payload) in [
        (
            ExpectedDocument::Absent,
            serde_json::json!({"kind":"absent"}),
        ),
        (
            ExpectedDocument::Version(Version(3)),
            serde_json::json!({"kind":"version","version":3}),
        ),
    ] {
        let check = ReadCheck {
            collection: "dependencies".into(),
            id: "dep".into(),
            expected,
        };
        let encoded = serde_json::to_string(&check).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({"collection":"dependencies","id":"dep","expected":payload}),
            "public read-check field names and tags are a stable wire contract",
        );
        assert_eq!(serde_json::from_str::<ReadCheck>(&encoded).unwrap(), check);
    }
    let invalid = ReadCheck {
        collection: "dependencies".into(),
        id: String::new(),
        expected: ExpectedDocument::Absent,
    };
    assert!(invalid.validate().is_err());
    assert!(invalid.verify(None).is_err());
}

#[test]
fn document_validation_accepts_objects_and_rejects_invalid_ids_and_bodies() {
    assert!(validate_doc(&serde_json::json!({"state":"queued"})).is_ok());
    assert!(validate_doc(&serde_json::json!(null)).is_err());
    assert!(validate_id("valid").is_ok());
    assert!(validate_id("").is_err());
    assert!(validate_id("bad\0id").is_err());
    assert!(validate_id(&"x".repeat(MAX_ID_LEN + 1)).is_err());
}

#[test]
fn cursor_rejects_malformed_payloads_and_positions_for_other_sorts() {
    let malformed = Cursor("memory:keyset1:0000000000000007:not-json".into());
    assert!(malformed.decode_keyset("memory", 7).is_err());
    let sort = [Sort::desc("rank")];
    let low = CursorPosition {
        id: "a".into(),
        values: vec![serde_json::json!(1)],
    };
    let high = CursorPosition {
        id: "z".into(),
        values: vec![serde_json::json!(2)],
    };
    assert_eq!(high.compare(&low, &sort), std::cmp::Ordering::Less);
    assert_eq!(
        low.compare(
            &CursorPosition {
                id: "z".into(),
                values: vec![serde_json::json!(1)]
            },
            &[]
        ),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        CursorPosition::for_document("a", &serde_json::json!({"rank":1}), &sort).values,
        [serde_json::json!(1)]
    );
    assert_eq!(
        CursorPosition::for_document("a", &serde_json::json!({}), &sort).values,
        [serde_json::Value::Null]
    );
    assert_eq!(
        CursorPosition::for_record(
            "a",
            &serde_json::json!({}),
            &Meta {
                created_ms: 3,
                ..Meta::unknown()
            },
            &[Sort::asc(crate::CREATED_FIELD)]
        )
        .values,
        [serde_json::json!(3)]
    );
    assert!(
        CursorPosition {
            id: "a".into(),
            values: vec![]
        }
        .validate(&sort)
        .is_err()
    );
    assert!(
        CursorPosition {
            id: "a".into(),
            values: vec![serde_json::json!("wrong")]
        }
        .validate(&[Sort::asc(crate::ID_FIELD)])
        .is_err()
    );
    assert_eq!(
        high.compare(
            &CursorPosition {
                id: "a".into(),
                values: vec![]
            },
            &[Sort::asc("rank"), Sort::asc("other")]
        ),
        std::cmp::Ordering::Greater
    );
}

#[test]
fn query_after_and_checked_dependencies_handle_success_and_conflict() {
    let cursor = Cursor("continue".into());
    assert_eq!(Query::all().after(cursor.clone()).cursor, Some(cursor));
    assert!(
        Query::all()
            .sort(Sort::asc("bad..field"))
            .validate()
            .is_err()
    );
    let absent = ReadCheck {
        collection: "jobs".into(),
        id: "missing".into(),
        expected: ExpectedDocument::Absent,
    };
    assert!(absent.validate().is_ok());
    assert!(absent.verify(None).is_ok());
    assert!(absent.verify(Some(Version(1))).is_err());
    let existing = ReadCheck {
        collection: "jobs".into(),
        id: "present".into(),
        expected: ExpectedDocument::Version(Version(4)),
    };
    assert!(existing.verify(Some(Version(4))).is_ok());
    assert!(existing.verify(Some(Version(5))).is_err());
}

#[test]
fn delete_write_variants_round_trip() {
    let op = WriteOp::Delete {
        collection: "jobs".into(),
        id: "old".into(),
        precondition: Precondition::Version(Version(2)),
    };
    assert_eq!(
        serde_json::from_value::<WriteOp>(serde_json::to_value(&op).unwrap()).unwrap(),
        op
    );
    let result = WriteResult::Delete { removed: false };
    assert_eq!(
        serde_json::from_value::<WriteResult>(serde_json::to_value(result).unwrap()).unwrap(),
        result
    );
}
