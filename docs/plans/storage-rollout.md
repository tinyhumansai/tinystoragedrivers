# Storage rollout

Implements [`../specs/storage-ports.md`](../specs/storage-ports.md). Each step
ships on its own and is tagged before the next step depends on it.

## This repository

1. **Ports, memory driver, conformance suite.** *(this release)*
   Rename the template. Add `tinystoragedrivers-core` and the
   `tinystoragedrivers` facade with `StorageConfig` and `open`.
2. **`tinystoragedrivers-sqlite`.**
   - **Connection manager.** One writer and a reader pool per file, WAL,
     `busy_timeout`, and `spawn_blocking`.
   - **Generic tables.** Document and stream tables keyed by
     `(scope, collection, id)`, with generated `json_extract` columns for
     indexes, FTS5 for search, and `_tsd_meta` migrations.
   - **Native mode** gives raw SQL access to owners that keep their existing
     tables (`sessions.db`, `flows.db`).
   - **Directory mode**: `database(name)` maps to `<dir>/<name>.db`.
   - **rusqlite.** Pin `=0.40.2` with `bundled` to match OpenHuman, because
     `libsqlite3-sys` is a `links` crate.
3. **`tinystoragedrivers-file`.** JSON documents, JSONL streams and blobs in
   a fixed, human-inspectable layout under one root. OpenHuman's existing
   desktop JSON files are imported into it on first open rather than read in
   place.
4. **`tinystoragedrivers-secrets`.** `SecretStore`, the `enc2:`
   ChaCha20-Poly1305 envelope (moved from OpenHuman with the byte format
   unchanged), `KeyProvider`, an OS keyring driver, and an encrypted-file
   driver. Fixtures must decrypt existing secrets. Contract:
   [`secret-storage.md`](../specs/secret-storage.md).
5. **`tinystoragedrivers-mongodb`.**
   - **Scoping.** `_scope` is the first field of every index and is injected
     into every filter.
   - **Streams.** Dense offsets via a unique index plus retry.
   - **Claims** use `findOneAndUpdate`.
   - **Blobs** use GridFS.
   - **Topology.** Detect it to decide whether to report `Transactions`.
   - **CI.** A replica-set `services:` lane gated on `TSD_MONGO_URL`.

## Consumers

6. **tinyagents.**
   - Add harness `DriverStore` and `DriverAppendStore` adapters behind a
     `storage-drivers` feature.
   - Add `DriverCheckpointer`, and rebuild `SqliteCheckpointer` on native mode.
   - Move the session connection manager, then add a generic session backend
     for MongoDB and memory.
7. **tinyflows.**
   - Turn the adaptive `Storage` URL parsing into a shim over `StorageConfig`.
   - Add a `StateStore` adapter.
   - Move the flows catalog and cron schedule onto native mode, plus generic
     implementations.
8. **OpenHuman.**
   - Add a `storage/` registry installed at boot.
   - Add the `storage-sqlite`, `storage-mongodb`, `storage-file` and
     `storage-keyring` features, forwarded through the host chain.
   - Move the domain stores one PR at a time. Small SQLite stores import
     their legacy tables into document tables in the same file.
