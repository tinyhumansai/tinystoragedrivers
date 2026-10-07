//! Classification of MongoDB failures, built without a server.

use super::*;
use mongodb::bson::{doc, from_document};
use mongodb::error::{CommandError, InsertManyError, WriteConcernError, WriteError};

fn write_error(code: i32, message: &str) -> Error {
    let write: WriteError = from_document(doc! {"code": code, "errmsg": message}).unwrap();
    Error::from(MongoKind::Write(WriteFailure::WriteError(write)))
}

fn command_error(code: i32) -> Error {
    let command: CommandError =
        from_document(doc! {"code": code, "codeName": "X", "errmsg": "boom"}).unwrap();
    Error::from(MongoKind::Command(command))
}

#[test]
fn classifies_by_origin_code_and_label() {
    assert_eq!(classify(Origin::Network, None, &[]), ErrorKind::Unavailable);
    assert_eq!(
        classify(Origin::Server, Some(112), &[]),
        ErrorKind::Unavailable
    );
    assert_eq!(
        classify(Origin::Server, Some(1), &["TransientTransactionError"]),
        ErrorKind::Unavailable
    );
    assert_eq!(
        classify(Origin::Server, Some(11000), &[]),
        ErrorKind::AlreadyExists
    );
    assert_eq!(classify(Origin::Server, Some(2), &[]), ErrorKind::Backend);
    assert_eq!(
        classify(Origin::Encoding, None, &[]),
        ErrorKind::Serialization
    );
    assert_eq!(
        classify(Origin::Argument, None, &[]),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        classify(Origin::Other, None, &["Other"]),
        ErrorKind::Backend
    );
}

#[test]
fn maps_real_errors_without_leaking_their_text() {
    let duplicate = write_error(
        11000,
        "E11000 duplicate key error collection: db.c index: _tsd_ix_1 dup key: { d.email: \"secret@x\" }",
    );
    assert_eq!(duplicate_index(&duplicate).as_deref(), Some("_tsd_ix_1"));
    let mapped = map(duplicate, "write a document");
    assert_eq!(mapped.kind(), ErrorKind::AlreadyExists);
    assert!(!mapped.to_string().contains("secret"), "{mapped}");
    assert!(std::error::Error::source(&mapped).is_some());

    assert_eq!(
        duplicate_index(&write_error(11000, "no index named")).as_deref(),
        Some("")
    );
    assert_eq!(duplicate_index(&write_error(2, "index: x dup")), None);
    assert_eq!(map(write_error(2, "bad"), "x").kind(), ErrorKind::Backend);

    let conflict = command_error(112);
    assert_eq!(map(conflict, "commit").kind(), ErrorKind::Unavailable);
    assert_eq!(duplicate_index(&command_error(11000)).as_deref(), Some(""));

    let io = Error::from(std::io::ErrorKind::ConnectionReset);
    let mapped = map(io, "read a document");
    assert_eq!(mapped.kind(), ErrorKind::Unavailable);
    assert!(mapped.is_retryable());
    assert_eq!(
        duplicate_index(&Error::from(std::io::ErrorKind::Other)),
        None
    );

    let custom = Error::custom(1_u8);
    assert_eq!(map(custom, "x").kind(), ErrorKind::Backend);
    assert!(!is_transient(&command_error(112)));
    assert!(!is_unknown_commit(&command_error(112)));
}

#[test]
fn reads_codes_from_bulk_and_concern_failures() {
    let concern: WriteConcernError =
        from_document(doc! {"code": 91, "codeName": "ShutdownInProgress", "errmsg": "x"}).unwrap();
    let error = Error::from(MongoKind::Write(WriteFailure::WriteConcernError(
        concern.clone(),
    )));
    assert_eq!(map(error, "x").kind(), ErrorKind::Unavailable);

    let many: InsertManyError = from_document(doc! {
        "writeErrors": [{"index": 0, "code": 11000, "errmsg": "index: _id_ dup key"}],
    })
    .unwrap();
    let error = Error::from(MongoKind::InsertMany(many));
    assert_eq!(duplicate_index(&error).as_deref(), Some("_id_"));

    let concern_only: InsertManyError = from_document(doc! {
        "writeConcernError": {"code": 91, "codeName": "ShutdownInProgress", "errmsg": "x"},
    })
    .unwrap();
    let error = Error::from(MongoKind::InsertMany(concern_only));
    assert_eq!(map(error, "x").kind(), ErrorKind::Unavailable);
}

#[test]
fn maps_stream_io_errors() {
    let plain = std::io::Error::other("disk");
    assert_eq!(map_io(plain, "read a blob").kind(), ErrorKind::Unavailable);
    let wrapped = std::io::Error::other(write_error(2, "bad"));
    assert_eq!(map_io(wrapped, "read a blob").kind(), ErrorKind::Backend);
}

#[test]
fn adapters_map_like_their_functions() {
    let mapped = failed("x")(command_error(112));
    assert_eq!(mapped.kind(), ErrorKind::Unavailable);
    let mapped = failed_io("x")(std::io::Error::other("disk"));
    assert_eq!(mapped.kind(), ErrorKind::Unavailable);
}

#[test]
fn only_gridfs_failures_mean_a_vanished_file() {
    assert!(!is_gridfs(&command_error(2)));
    assert!(!io_is_gridfs(&std::io::Error::other("disk")));
    assert!(!io_is_gridfs(&std::io::Error::other(command_error(2))));
}
