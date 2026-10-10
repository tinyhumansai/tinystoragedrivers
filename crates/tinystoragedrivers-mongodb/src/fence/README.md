# `fence`: fenced writes on MongoDB

This module implements `StorageBackend::for_scope_fenced` for the MongoDB
driver. The contract lives in `docs/specs/write-fencing.md`: a fenced write
lands only while the fence's guard document exists, is live, and matches the
fence's filter. Otherwise it fails with `ErrorKind::Fenced` and changes
nothing.

## Design

Each fenced write runs in one multi-document transaction:

1. **Read the guard.** The transaction reads the guard document (`_id` from
   the fence's scope and id, in the fence's collection) and applies
   `Fence::check` to it. A missing or expired guard is passed to `check` as
   `None`. A refusal aborts the transaction and returns `Fenced`.
2. **Hold the guard.** The transaction then increments the hidden `_fence`
   counter on the guard, and only at the version it just read
   (`at_version`). Snapshot isolation catches write-write conflicts but not a
   write racing a read, so the read alone would not be safe. With this extra
   write, a takeover that rewrites the guard conflicts with every fenced
   transaction still in flight:
   - if the takeover commits first, the fenced transaction aborts, and its
     retry reads the new guard and is refused;
   - if the fenced transaction commits first, the takeover waits for it.

   Either way, no fenced write lands after the takeover.
3. **Write.** The caller's write runs on the same session.
4. **Commit.** `settle` decides what the commit result means. A clean
   commit is done. A transient abort is retried from step 1. A failed commit
   is returned as an error. An unknown commit result (`UnknownTransactionCommitResult`)
   is returned as a `Backend` error that tells the caller to read before
   retrying, because the write may have landed.

The counter does not change the guard's version, so a holder that renews its
lease by compare-and-swap on the version is unaffected. Reads do not see the
counter, and the next ordinary write of the guard drops it.

## Surface (crate-private)

| Item | Role |
| --- | --- |
| `in_fence!(shared, fence, session => body)` | Runs `body` inside a fenced transaction and retries transient aborts. The document and stream handles use it for every write. |
| `FencedTxn` | One fenced write's session and attempt counter: `start`, `begin` (checks and holds the guard), `finish` (commits or aborts). |
| `settle` | Maps a commit result to done, retry, or a final error. |
| `FENCE_ATTEMPTS` | 64. The retry budget for one fenced write. |
| `unfenceable(what)` | The `Unsupported(Fencing)` refusal for writes that cannot join a transaction. |

## Operational constraints

- **Transactions are required.** Only replica sets and sharded clusters
  support them. A standalone server reports neither `Capability::Transactions`
  nor `Capability::Fencing`, and `for_scope_fenced` returns
  `Unsupported(Fencing)`.
- **Blobs cannot be fenced.** GridFS cannot join a transaction, so a fenced
  handle's blob `put` and `delete` return `Unsupported(Fencing)` and do not
  run unfenced. Callers that need fenced blobs must use another driver or
  fall back to their own check.
- **Writes through one fence serialize.** Every fenced write holds the same
  guard document, so concurrent writes abort each other with transient
  write conflicts and are retried. After `FENCE_ATTEMPTS` attempts the write
  fails with a retryable `Unavailable` error. Keep fenced write rates for one
  fence modest.
- **Same database.** The guard is read from the handle's own named database.
  A guard stored in another database is not visible, and every write is
  refused as `Fenced`.
