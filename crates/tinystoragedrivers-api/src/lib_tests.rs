use super::*;
use bytes::Bytes;
use serde_json::json;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tinystoragedrivers_types::{Capability, ErrorKind};

#[test]
fn api_contract_version_is_pinned() {
    assert_eq!(CONTRACT_VERSION, 6);
    assert!(is_compatible(6));
    assert!(!is_compatible(5));
    assert!(!is_compatible(0));
    assert!(!is_compatible(1));
    assert!(!is_compatible(2));
    assert!(!is_compatible(3));
    assert!(!is_compatible(4));
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "exercise every operation on all six unconfigured primitive handles"
)]
fn unconfigured_bundle_handles_return_their_primitive_capability() {
    let storage = Storage::unsupported();
    let filter = types::Filter::default();
    let query = types::Query::all();
    let precondition = types::Precondition::None;
    let value = json!({});
    let options = types::WriteOptions::default();
    let no_reader = || Box::pin(EmptyChunks) as BlobReader;

    assert!(format!("{storage:?}").contains("Storage"));
    assert_unsupported(
        run_ready(storage.ensure_collection(&types::CollectionSpec::new("items"))),
        Capability::Documents,
    );
    assert!(storage.documents().capabilities().iter().next().is_none());
    assert!(storage.streams().capabilities().iter().next().is_none());
    assert!(storage.kv().capabilities().iter().next().is_none());
    assert!(storage.blobs().capabilities().iter().next().is_none());
    assert_eq!(storage.blobs().max_key_len(), 0);
    assert_eq!(storage.queues().features(), types::QueueFeatures::default());

    assert_unsupported(
        run_ready(
            storage
                .documents()
                .watch("items", &filter, &types::WatchOptions::default()),
        ),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(
            storage
                .documents()
                .ensure_collection(&types::CollectionSpec::new("items")),
        ),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(
            storage
                .documents()
                .validate_collection_spec(&types::CollectionSpec::new("items")),
        ),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().get("items", "one")),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(
            storage
                .documents()
                .put("items", "one", value.clone(), precondition),
        ),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().put_with(
            "items",
            "one",
            value.clone(),
            precondition,
            options,
        )),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().delete("items", "one", precondition)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().query("items", &query)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().count("items", &filter)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().delete_where("items", &filter)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().claim("items", &filter, &[], &value)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().atomic_batch(vec![])),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().atomic_batch_checked(vec![], vec![])),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().search("items", "text", 1)),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().drop_collection("items")),
        Capability::Documents,
    );
    assert_unsupported(
        run_ready(storage.documents().collections("items")),
        Capability::Documents,
    );

    assert_unsupported(
        run_ready(storage.streams().append("events", json!(null))),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().append_batch("events", vec![])),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(
            storage
                .streams()
                .append_batch_if_offset("events", 0, vec![]),
        ),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().read_window("events", 0, 1)),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().len("events")),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().truncate_before("events", 1)),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().delete_stream("events")),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().streams("events")),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().seal("events")),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().delete_sealed_stream("events", 1)),
        Capability::Streams,
    );
    assert_unsupported(
        run_ready(storage.streams().info("events")),
        Capability::Streams,
    );

    assert_unsupported(
        run_ready(storage.kv().put("key", json!(null))),
        Capability::Kv,
    );
    assert_unsupported(run_ready(storage.kv().get("key")), Capability::Kv);
    assert_unsupported(
        run_ready(storage.kv().cas("key", value.clone(), precondition)),
        Capability::Kv,
    );
    assert_unsupported(
        run_ready(storage.kv().delete("key", precondition)),
        Capability::Kv,
    );
    assert_unsupported(run_ready(storage.kv().list("", None, 1)), Capability::Kv);
    assert_unsupported(
        run_ready(
            storage
                .kv()
                .put_with_ttl("key", value.clone(), std::time::Duration::ZERO),
        ),
        Capability::Kv,
    );

    assert_unsupported(
        run_ready(storage.blobs().put("blob", vec![], None)),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(storage.blobs().put_stream("blob", no_reader(), 0, None)),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(
            storage
                .blobs()
                .put_expiring("blob", vec![], None, std::time::Duration::ZERO),
        ),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(storage.blobs().put_stream_expiring(
            "blob",
            no_reader(),
            0,
            None,
            std::time::Duration::ZERO,
        )),
        Capability::Blobs,
    );
    assert_unsupported(run_ready(storage.blobs().get("blob")), Capability::Blobs);
    assert_unsupported(
        run_ready(storage.blobs().get_stream("blob")),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(
            storage
                .blobs()
                .get_range("blob", Range { start: 0, end: 0 }),
        ),
        Capability::Blobs,
    );
    assert_unsupported(run_ready(storage.blobs().head("blob")), Capability::Blobs);
    assert_unsupported(run_ready(storage.blobs().delete("blob")), Capability::Blobs);
    assert_unsupported(run_ready(storage.blobs().list("")), Capability::Blobs);
    assert_unsupported(
        run_ready(storage.blobs().sweep_expired()),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(
            storage
                .blobs()
                .presign_get("blob", std::time::Duration::ZERO),
        ),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(
            storage
                .blobs()
                .presign_put("blob", std::time::Duration::ZERO),
        ),
        Capability::Blobs,
    );
    assert_unsupported(run_ready(storage.blobs().list_dir("")), Capability::Blobs);
    assert_unsupported(
        run_ready(
            storage
                .blobs()
                .put_tree("", vec![types::TreeFile::new("file", vec![])]),
        ),
        Capability::Blobs,
    );
    assert_unsupported(
        run_ready(storage.blobs().delete_tree("dir")),
        Capability::Blobs,
    );

    assert_unsupported(
        run_ready(
            storage
                .queues()
                .ensure_queue("queue", &types::QueueSpec::default()),
        ),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(
            storage
                .queues()
                .enqueue("queue", &[types::NewMessage::new(value)]),
        ),
        Capability::Queues,
    );
    let receipt = types::Receipt {
        id: "id".into(),
        token: "token".into(),
    };
    assert_unsupported(
        run_ready(storage.queues().receive("queue", 1, 1, "worker")),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().ack("queue", &receipt)),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(
            storage
                .queues()
                .nack("queue", &receipt, types::Retry::After(0)),
        ),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().extend("queue", &receipt, 1)),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().stats("queue")),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().dead_letters("queue", 1)),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().redrive("queue", "id")),
        Capability::Queues,
    );
    assert_unsupported(
        run_ready(storage.queues().purge("queue")),
        Capability::Queues,
    );

    assert_unsupported(
        run_ready(storage.secrets().get("secret")),
        Capability::Secrets,
    );
    assert_unsupported(
        run_ready(storage.secrets().set("secret", b"secret")),
        Capability::Secrets,
    );
    assert_unsupported(
        run_ready(storage.secrets().get_versioned("secret")),
        Capability::ConditionalSecretWrites,
    );
    assert_unsupported(
        run_ready(
            storage
                .secrets()
                .set_if("secret", b"secret", types::SecretPrecondition::Absent),
        ),
        Capability::ConditionalSecretWrites,
    );
    assert_unsupported(
        run_ready(storage.secrets().delete("secret")),
        Capability::Secrets,
    );
    assert_unsupported(run_ready(storage.secrets().list("")), Capability::Secrets);
    assert!(!storage.secrets().enumerable());
    assert_eq!(storage.secrets().backend_name(), "unconfigured");
}

struct EmptyChunks;

impl futures_core::Stream for EmptyChunks {
    type Item = types::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

fn assert_unsupported<T: std::fmt::Debug>(
    result: tinystoragedrivers_types::Result<T>,
    capability: Capability,
) {
    assert_eq!(
        result.unwrap_err().kind(),
        ErrorKind::Unsupported(capability)
    );
}

fn run_ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("unconfigured handle operation unexpectedly pended"),
    }
}
