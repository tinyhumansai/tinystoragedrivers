# tinystoragedrivers-sqlite

This is the SQLite driver for `tinystoragedrivers`. It runs the desktop and CLI
backend and passes the shared conformance suite in both layouts.

## Layouts

- **Directory mode** is used for any path that does not end in `.db` or
  `.sqlite`.
  - The root database lives in `<dir>/storage.db`.
  - Each named database lives in its own file, `<dir>/<name>.db`, the same way
    OpenHuman already splits its stores today.
- **File mode** is used for a path ending in `.db` or `.sqlite`.
  - Everything lives in that one file.
  - Each named database's tables are prefixed `<name>__`.

## Storage

- **Documents** are stored in one `WITHOUT ROWID` table per database, keyed by
  `(scope, collection, id)`.
  - Declared indexes become partial `json_extract` expression indexes.
  - Unique constraints are checked in the write transaction with the same
    equality the memory driver uses.
  - Declared search fields are mirrored into an FTS5 table.
- **Filters** push their exact string and boolean equalities down to SQL.
  Everything else is re-checked in Rust.
- **Streams** use dense offsets that are allocated in the inserting
  transaction.
- **Blobs** are stored as rows, and range reads use `substr`.
- **Expiry** is applied at read time against an injectable clock.

## Concurrency

Every handle on the same file within one process shares one connection. Writes
use immediate transactions. Other processes are serialized by WAL plus a
5-second busy timeout. Contention that outlasts the timeout returns a retryable
`Unavailable` error.

## Native access

`SqliteNative` gives raw access to the same file. It exposes
`with_connection`, `with_transaction`, and `migrate`, where `migrate` is a
per-owner, append-only migration runner. Owners such as a session ledger with
its own FTS tables use it to keep their schema while sharing the connection.

## rusqlite version

`rusqlite` is required as `0.40.2` (caret) with `bundled` and is re-exported
as `tinystoragedrivers_sqlite::rusqlite`. `libsqlite3-sys` may appear only once
in a host's dependency graph, so the requirement must unify with the host's
own pin (OpenHuman pins `=0.40.2`).

## Limits

- Versions and offsets stop at `i64::MAX`, because that is the largest value
  SQLite's integer column can hold. A write past that limit fails instead of
  reusing a value.
- Paging is offset-based, with cursors bound to their query.
- Field paths with an all-digit segment (an array index or a numeric key) or
  a `"` / `\\` in a segment cannot be written as a SQLite JSON path. Filters
  and indexes on them are evaluated in Rust instead of SQL.
