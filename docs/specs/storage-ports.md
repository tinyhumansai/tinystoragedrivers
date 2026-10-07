# Storage ports

Status: accepted (v0.3, ports, memory driver and conformance suite).

## Problem

OpenHuman persists data in more than a dozen places, and each one talks to its
backend directly:
- rusqlite databases opened per call: sessions, run ledger, flows, cron,
  approvals, devices, notifications, task sources, graph checkpoints
- JSON and JSONL files: turn states, threads, artifacts, costs, credentials
- the OS keyring

Some of the libraries it hosts (tinyagents, tinyflows) hard-wire SQLite in the
same way. Nothing can run against another backend, and a multi-tenant cloud
deployment on MongoDB is impossible without rewriting every store.

## Outcome

- One set of backend-neutral ports that every persisting crate builds on.
- A driver per backend: memory, SQLite, MongoDB, files, and secrets. Each is
  compiled in by Cargo feature and chosen at boot by URL.
- The same behavior on every driver, enforced by one conformance suite.
- Tenant isolation as a property of the port handle, not of each call site.

Non-goals:
- typed repositories (they stay with the crates that own the records)
- a query language richer than what both SQL and MongoDB evaluate natively
- cross-backend replication

## Ports

### `DocumentStore`

Versioned JSON **objects** in named collections.

- **Names.** Collection names are 1–120 bytes of `[A-Za-z0-9_.-]`, and the
  prefix `_tsd` is reserved for drivers. Ids are 1–512 bytes without NUL.
- **Versions.** A new document is version 1, and every write produces a
  strictly greater version. A write that would exceed `u64::MAX` fails with
  `ErrorKind::Backend` and changes nothing; a version is never reused. That
  holds across deletion: a deleted (or dropped) id that is written again
  continues from its last version, so a compare-and-swap prepared before the
  deletion fails instead of overwriting the new document.
- **Preconditions.** `Precondition::None` upserts. `Absent` inserts. `Version(v)`
  is a compare-and-swap. A failed precondition is `ErrorKind::Conflict`.
- **Filters.** `Filter` supports `eq`, `ne`, `in`, `range`, `exists`, `and`,
  `or` and `not`, over dotted paths. `_id` addresses the document id.
  - Values compare by the cross-type order in `value::compare`: null < bool <
    number < string < array < object. Numbers compare numerically.
  - A range bound of a different JSON type never matches.
  - `ne` matches documents where the field is absent.
- **Sorting.** Any number of keys. Ties, and an empty sort, fall back to id
  ascending. A missing field sorts as null.
- **Paging.** `Query.limit` caps a page and must be at least 1. Sort fields
  follow the same path rules as filters. `Page.next`
  is an opaque cursor, valid only for the same collection, filter and sort on
  the same driver; any other cursor is `ErrorKind::InvalidInput`.
- **Declarations.** `ensure_collection` merges into what is already declared:
  indexes and search fields accumulate, and an expiry field can be added but
  not changed. Redeclaring an index name with different fields is
  `InvalidInput`. Adding a unique index that stored documents already violate
  (in any scope) is `AlreadyExists`, and nothing is declared. A declaration is schema, written by host code rather than
  tenants, so it applies to the collection in every scope, the way a SQL or
  MongoDB index spans the whole table or collection.
- **Indexes.** Any field is queryable. An `IndexSpec` is a performance hint;
  when it is `unique`, every driver enforces it with `ErrorKind::AlreadyExists`.
  Documents missing an indexed field are unconstrained.
- **Claims.** `claim(filter, sort, patch)` picks the first match, applies an
  RFC 7396 merge patch, and returns the updated document, all as one atomic
  step. Two claims never return the same document when the patch makes it
  stop matching.
- **Optional capabilities.** A capability the driver lacks is
  `ErrorKind::Unsupported(capability)`.
  - `Ttl`: documents whose `ttl_field` (epoch ms) has passed read as absent.
  - `Transactions`: `atomic_batch`, all or nothing.
  - `FullText`: `search` over the declared `SearchSpec` fields. Conformance
    checks single-token membership only, never ranking.

### `StreamStore`

Append-only logs of JSON values.
- Offsets are dense, start at 0, and are never reused.
- `truncate_before` discards old records but leaves `len` unchanged.

### `BlobStore`

Bytes by `/`-separated key, without empty, `.` or `..` segments, and with no
leading `/`. Supports ranged reads clamped to the blob's length, and prefix
listing.

### `StorageBackend` and `Scope`

- `for_scope(scope)` returns handles bound to one tenant. No port method takes
  a scope, so a call site cannot forget it.
- `Scope::local()` is the single-operator scope.
- A backend that serves only one scope (a desktop SQLite file) rejects other
  scopes in `for_scope`.
- `database(name)` returns an independent named database. Names are 1–64 bytes
  of lowercase `[a-z0-9_-]`. The same name always addresses the same data.
- OpenHuman's scope is the agent id. The cloud deployment runs one agent per
  user, so this is the user.

### Errors

`StorageError { kind, message, source }`.
- Callers branch on `ErrorKind` only.
- Only `Unavailable` is retryable.
- Messages never contain credentials. `StorageConfig`'s `Display` and `Debug`
  redact the password in a connection string.

### Synchronous callers

`Blocking` (feature `blocking`) runs a port future on a runtime thread it owns.
It is safe from plain threads and from inside any tokio runtime. A future
running on the bridge must not call back into it; doing so is a `Backend`
error, not a deadlock. The bridge is transitional: synchronous consumer traits
should become async.

## Acceptance

- `conformance::run` passes on every driver. The memory driver passes it in
  this release.
- `tinystoragedrivers-core` depends on no database client or transport, and CI
  asserts this.
- Every source file has at least 90% line coverage.
