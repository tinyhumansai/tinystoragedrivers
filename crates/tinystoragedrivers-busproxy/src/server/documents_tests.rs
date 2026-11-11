use super::*;

fn put_with(options: WriteOptions) -> WriteOp {
    WriteOp::PutWith {
        collection: "notes".into(),
        id: "id".into(),
        doc: Value::Null,
        precondition: Precondition::None,
        options,
    }
}

#[test]
fn a_module_batch_may_declare_schema_and_expiry_but_never_a_creation_time() {
    let allowed = put_with(WriteOptions::new().schema(2).expires_ms(5));
    assert_eq!(batch_ops(vec![allowed.clone()]).unwrap(), vec![allowed]);
    assert!(batch_ops(vec![put_with(WriteOptions::new().created_ms(1))]).is_err());
}
