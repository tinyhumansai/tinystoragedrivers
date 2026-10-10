# tinystoragedrivers-mongodb

The MongoDB driver for `tinystoragedrivers`: the multi-tenant cloud backend.
One MongoDB database is shared by every tenant, and the driver enforces the
`Scope` on every operation. Hosts usually reach it through the facade:
`tinystoragedrivers = { features = ["mongodb"] }` and a
`mongodb://…/<db>` or `mongodb+srv://…/<db>` URL.

Requires MongoDB 5.0 or newer. The client is the official `mongodb` crate
with rustls (never OpenSSL), matching the copy OpenHuman already links.

## Storage layout

| Port | MongoDB |
| --- | --- |
| Document collection `c` | collection `<prefix>c`, one document `{_id: {s, k}, _scope, _key, _v, d}` per port document; the body is under `d` |
| Declarations | `<prefix>_tsd_meta`, `{_id: c, spec: <json>, rev}`, collection-wide |
| Removed versions | `<prefix>_tsd_tombstones`, `{_id: {c, s, k}, _scope, _v}` |
| Streams | `<prefix>_tsd_streams`, one segment per append: `{_scope, s, o, n, e, t, vs}` |
| Blobs | GridFS bucket `<prefix>_tsd_blobs`, `metadata: {_scope, content_type}` |

`<prefix>` is empty for the root database and `<name>:` for
`database(name)`. Port collection names cannot contain `:`, so a root
collection never collides with a named database's.

## Tenant isolation

All tenant data is reached through `ScopedCollection` (`src/scoped/`), which
owns the collection handle and ANDs `{_scope: <scope>}` into every filter,
stamps it on every insert, and makes it the first `$match` of every pipeline.
Every index leads with `_scope`. Unit tests pin the generated filters, and a
live test plants another tenant's document in the same collection and checks
that no operation sees or deletes it.

Two things are deliberately collection-wide, as the spec says: declarations
(`_tsd_meta`) and the indexes they build. GridFS chunks are fetched by a file
id that was itself resolved under the scope.

## How the reference semantics are kept

- **Filters** (`src/translate/`). Mongo walks arrays in dotted paths, reads
  equality on an array as "contains", and matches missing fields with
  `null`. Each leaf is guarded (`$not: {$type: "array"}` on every segment) to
  make it exact. Where it cannot be exact (array positions such as `tags.1`,
  array/object/`null`/oversized values, `$`-prefixed segments) the query is a
  superset and the driver re-checks with `Filter::matches` in Rust. Negating
  a superset is not a superset, so `ne`/`not` over such a leaf is evaluated in
  Rust entirely.
- **Sorting** uses an aggregation that ranks each key by JSON type
  (null < bool < number < string < array < object, as `value::compare`) and
  then by value, ties by `_key`. If any matching document has an array or
  object sort value, that query is sorted in Rust instead.
- **Paging** is offset-based (`$skip`), like the memory driver. Cursors are
  `mongo:<fnv1a of collection, filter, sort>:<offset>`, stable across
  processes.
- **Writes** read the stored document by `_id`, apply the precondition and
  expiry rules in Rust, then commit with a compare-and-swap on `_v` (or an
  insert, which `_id` makes exclusive), retrying a lost race.
- **Claims** pick the first match and commit it with the same
  compare-and-swap; the merge patch is applied in Rust, so RFC 7396 edge cases
  behave exactly as on the memory driver.
- **Versions** survive removal: `delete`, `delete_where`, `drop_collection`
  and sweeping fold the removed `(key, _v)` into the tombstones with `$max`
  before deleting, and a recreated id continues from there.
- **Expiry** is a read-time rule: a document whose TTL field is a number at or
  below the clock reads as absent. `MongoStorage::sweep_expired` reclaims the
  space. In a collection with expiry and a unique index, writes sweep first,
  because an expired document would otherwise still hold its unique value.
- **Streams** stay dense without transactions: an append inserts one segment
  at the current end, and a unique `(_scope, s, o)` index lets one appender
  win each offset. A segment lands whole or not at all, so a crash cannot
  leave a gap and a batch is always contiguous.
- **Transactions** (`atomic_batch`) run in a session transaction on replica
  sets and sharded clusters, retried on transient aborts.

## Driver-defined behavior and limits

- Integers above `i64::MAX` cannot be stored (`ErrorKind::Serialization`),
  and versions and stream offsets end at `i64::MAX` (`ErrorKind::Backend`).
- One `append_batch` must fit in a 16 MiB BSON document.
- `search` uses MongoDB's text index (`default_language: none`): tokens are
  case- and diacritic-insensitive, and only string (or string array) fields
  are indexed. The spec checks single-token membership only.
- Unique indexes are MongoDB indexes: values in arrays are indexed per
  element (two documents sharing one element clash), objects compare with
  key order, a compound unique index cannot cover two array fields of one
  document, and a field path with a `$`-prefixed segment cannot be unique
  (`InvalidInput`). Expired documents that have not been swept still count
  when a new unique index is built.
- A declaration made by another process is picked up within five seconds.
- **Fencing** (`for_scope_fenced`) needs transactions. Each fenced document
  or stream write runs in a transaction that reads the guard document and
  increments a hidden `_fence` counter on it, so a takeover that rewrites the
  guard conflicts with every fenced write in flight. Fenced writes through
  one fence therefore serialize on the guard: contention is retried up to 64
  times and then returned as `Unavailable`. Fenced blob writes return
  `Unsupported(Fencing)`, because GridFS cannot join a transaction. The
  counter is invisible to reads, and the next ordinary write of the guard
  drops it.
- Queries whose filter or sort needs Rust evaluation fetch every candidate in
  the scope and collection. Keep such filters for small collections.

## Testing

`TSD_MONGO_URL` (for example
`mongodb://localhost:27017/tsd_test?directConnection=true`, a single-node
replica set) enables the `live_*` tests: the conformance suite with and
without transactions, a differential test against the memory driver over
documents chosen to trip MongoDB's semantics, and focused behavior tests.
Without it they print a note and pass; the pure translation, naming and
error-mapping logic is unit-tested without a server.
