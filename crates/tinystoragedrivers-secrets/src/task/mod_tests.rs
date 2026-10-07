//! The blocking bridge and I/O error mapping.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use tinystoragedrivers_core::ErrorKind;

use super::*;

#[tokio::test]
async fn runs_on_the_blocking_pool_inside_a_runtime() {
    assert_eq!(run_blocking(|| Ok(7)).await.unwrap(), 7);
}

#[tokio::test]
async fn a_panicking_task_is_a_backend_error() {
    let error = run_blocking::<(), _>(|| panic!("boom")).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn runs_inline_outside_a_runtime() {
    let mut future = pin!(run_blocking(|| Ok("inline")));
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => assert_eq!(value.unwrap(), "inline"),
        Poll::Pending => panic!("inline work must complete on the first poll"),
    }
}

#[test]
fn transient_io_errors_are_unavailable() {
    for kind in [
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::TimedOut,
    ] {
        let error = io_error("read", std::io::Error::from(kind));
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.is_retryable());
    }
    let error = io_error(
        "read secrets",
        std::io::Error::from(std::io::ErrorKind::NotFound),
    );
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(error.message(), "could not read secrets");
    assert!(std::error::Error::source(&error).is_some());
}
