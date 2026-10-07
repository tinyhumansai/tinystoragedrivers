//! The driver against a real MongoDB, named `live_*`.
//!
//! Set `TSD_MONGO_URL` (for example
//! `mongodb://localhost:27017/tsd_test?directConnection=true`, a single-node
//! replica set) to run them; without it each test prints a note and passes.

mod support;

use support::{connect, skip};
use tinystoragedrivers_core::conformance;

#[tokio::test]
async fn live_conformance_on_a_replica_set() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_on_a_replica_set");
    };
    conformance::run(&backend, false).await;
}

#[tokio::test]
async fn live_conformance_without_transactions() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_without_transactions");
    };
    conformance::run(&backend.without_transactions(), false).await;
}
