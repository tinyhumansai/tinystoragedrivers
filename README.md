# tinystoragedrivers

Pluggable storage for OpenHuman and the libraries it hosts. Every crate that
persists data (sessions, workflow runs, approvals, schedules) builds its records
on one small set of ports. The host chooses the backend once at boot, from a URL:

| Deployment | URL | Driver |
| --- | --- | --- |
| Tests, stateless embedders | `memory` | in-process maps |
| Desktop and CLI | `sqlite:<workspace dir>` | SQLite, one file per named database *(planned)* |
| Cloud, multi-tenant | `mongodb://…/<db>` | MongoDB, every record scoped to its tenant (feature `mongodb`) |
| Plain files | `file:<dir>` | JSON and JSONL on disk *(planned)* |

Drivers are separate crates. A host compiles in only the ones it enables, as
Cargo features on the `tinystoragedrivers` facade. A desktop build never links
a MongoDB client, and a cloud build never links SQLite.

## The ports

- **`DocumentStore`** holds versioned JSON objects in named collections. It
  supports:
  - filters, sorting, and cursor paging;
  - compare-and-swap through `Precondition`;
  - atomic `claim` for queues and leases;
  - unique indexes;
  - optional expiry, cross-document transactions, and full-text search.
- **`StreamStore`** holds append-only logs with dense offsets, for transcripts,
  journals, and audit trails.
- **`BlobStore`** holds opaque bytes by key, for attachments and artifacts.

A `StorageBackend` hands out a `ScopedStorage` for each `Scope`, the tenant key
every record carries. Two scopes on one backend never see each other's data.
`StorageBackend::database(name)` splits a backend into named databases. Under
SQLite each is its own file; under MongoDB each is a collection prefix.

This repository holds no typed repository for any particular record. Those
belong to the crate that owns the record, which keeps this crate free of every
consumer's schema.

```rust
use serde_json::json;
use tinystoragedrivers::{Filter, Precondition, Scope, StorageConfig};

async fn demo() -> tinystoragedrivers::Result<()> {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory")?).await?;
    let jobs = backend.database("cron")?.for_scope(&Scope::local())?;
    let docs = jobs.documents();

    docs.put("jobs", "nightly", json!({"state": "queued", "run_at": 0}), Precondition::Absent)
        .await?;
    let next = docs
        .claim("jobs", &Filter::eq("state", "queued"), &[], &json!({"state": "running"}))
        .await?;
    assert_eq!(next.unwrap().id, "nightly");
    Ok(())
}
```

## Workspace

```text
crates/
├── tinystoragedrivers-core/    # ports, Scope, filters, errors, memory driver,
│                               # blocking bridge, conformance suite
├── tinystoragedrivers-mongodb/ # the MongoDB driver (feature `mongodb`)
└── tinystoragedrivers/         # facade: StorageConfig URL parsing, open(),
                                # driver features, re-exports the core
```

Each driver gets its own crate (`tinystoragedrivers-sqlite`,
`-mongodb`, `-file`, `-secrets`) and must pass
`tinystoragedrivers_core::conformance::run`. The suite runs the same checks
against every driver:
- precondition conflicts
- unique-index violations
- paging
- claim ordering
- stream offsets
- blob ranges
- scope isolation
- database isolation

## Develop

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-features
.github/scripts/check-file-coverage.sh 90 target/coverage.json
```

The MongoDB driver's `live_*` tests run against the server named by
`TSD_MONGO_URL` and skip without it. The coverage gate needs them, so run it
with a single-node replica set up:

```sh
docker run -d --rm --name tsd-mongo -p 27017:27017 mongo:7 --replSet rs0 --bind_ip_all
docker exec tsd-mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
export TSD_MONGO_URL='mongodb://localhost:27017/tsd_test?directConnection=true'
```

The design and its guarantees are in
[`docs/specs/storage-ports.md`](docs/specs/storage-ports.md). The order in which
drivers land and consumers migrate is in
[`docs/plans/storage-rollout.md`](docs/plans/storage-rollout.md).

## Releases

`.github/workflows/release.yml` (manual dispatch) validates, bumps the single
workspace version, tags `vX.Y.Z`, and creates a GitHub release. Consumers pin
the tag through a git dependency or a submodule gitlink.

## License

GPL-3.0-only. See [`LICENSE`](LICENSE).
