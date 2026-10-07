//! The bridge runs futures for synchronous callers, inside or outside a runtime.

use super::*;
use crate::ErrorKind;

#[test]
fn runs_futures_from_plain_threads() {
    let bridge = Blocking::new().unwrap();
    assert_eq!(bridge.run(async { 1 + 1 }).unwrap(), 2);
    let clone = bridge.clone();
    let handle = thread::spawn(move || clone.run(async { "other thread" }).unwrap());
    assert_eq!(handle.join().unwrap(), "other thread");
    assert_eq!(format!("{bridge:?}"), "Blocking { .. }");
}

#[tokio::test(flavor = "current_thread")]
async fn runs_from_inside_a_current_thread_runtime() {
    let bridge = Blocking::new().unwrap();
    assert_eq!(bridge.run(async { 7 }).unwrap(), 7);
}

#[test]
fn refuses_reentry_from_the_bridge_thread() {
    let bridge = Blocking::new().unwrap();
    let inner = bridge.clone();
    let nested = bridge.run(async move { inner.run(async { 1 }) }).unwrap();
    assert_eq!(nested.unwrap_err().kind(), ErrorKind::Backend);
}

#[test]
fn reports_a_panicking_future() {
    let bridge = Blocking::new().unwrap();
    let error = bridge
        .run(async {
            panic!("boom");
        })
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(
        bridge.run(async { 3 }).unwrap(),
        3,
        "the bridge survives a panic"
    );
}

#[test]
fn reports_a_stopped_bridge() {
    let (jobs, receiver) = mpsc::unbounded_channel();
    drop(receiver);
    let bridge = Blocking { jobs };
    assert_eq!(
        bridge.run(async { 1 }).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn startup_failures_keep_their_cause() {
    use std::error::Error as _;
    let error = startup_error("cannot start")(std::io::Error::other("no threads"));
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(error.message(), "cannot start");
    assert_eq!(error.source().unwrap().to_string(), "no threads");
}
