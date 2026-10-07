# tinystoragedrivers-core

The storage ports (`DocumentStore`, `StreamStore`, `BlobStore`), the tenant
`Scope`, the shared filter and value semantics, the in-memory reference driver,
the `Blocking` bridge for synchronous callers, and the `conformance` suite that
every driver runs.

It depends on no database client and no transport, and CI checks this. Driver
crates and consumers both compile against it. Most code should depend on the
`tinystoragedrivers` facade instead, which re-exports everything here.

See [`../../docs/specs/storage-ports.md`](../../docs/specs/storage-ports.md).
