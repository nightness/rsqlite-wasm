# rsqlite-wasm Limitations

rsqlite-wasm aims for SQLite compatibility but isn't a 1:1 port. This
document is the truth-source for what's deliberately deferred. Each entry
explains the gap, the workaround if any, and where to follow progress.

## Parser / dialect

Inherited from `sqlparser-rs` 0.55's `SQLiteDialect`:

- **Bitwise shift `<<` and `>>`, complement `~`.** Parse error. The AST
  layer accepts these only under `PostgreSqlDialect` / `GenericDialect`.
  Workaround: do bit shifts in application code, or parse with a
  generic dialect upstream.
- **`x IS TRUE` / `x IS FALSE` / `x IS NOT TRUE` / `x IS NOT FALSE`.**
  Not implemented. Workaround: use `x IS NOT 0` (truthy) / `x IS 0`
  (falsy), or `x IS DISTINCT FROM` for null-safe comparison.

## Schema

- **VIRTUAL generated columns** are computed at write time and stored
  on disk, identical to `STORED`. SQLite's read-time semantics for
  VIRTUAL aren't yet implemented. Functionally correct, slightly
  larger storage footprint.
- **Bare `rowid` references on tables without `INTEGER PRIMARY KEY`.**
  Errors with "bare ROWID reference not yet supported". Workaround:
  add `INTEGER PRIMARY KEY` to the table — bare `rowid` then resolves
  to that column.
- **`sqlite_schema` root-page split.** Schemas with very many CREATE
  TABLE / INDEX statements (large enough that the schema metadata
  itself outgrows page 1) error during DDL. Workaround: keep the
  schema modest, or split across multiple attached databases.

## Indexes

- **Partial indexes** (`CREATE INDEX ... WHERE ...`) build correctly
  and are maintained on INSERT/UPDATE — but the planner skips them at
  query lookup time, falling back to a full table scan. The index
  exists, it just isn't picked. Correctness is unaffected.
- **Expression indexes** (`CREATE INDEX ... ON t(lower(name))`) build
  with NULL placeholders for the expression columns and aren't picked
  at lookup time. Same correctness story.

## DML

- **`UPDATE ... LIMIT` / `UPDATE ... ORDER BY`** is not supported.
  `DELETE` supports both. SQLite gates these behind a compile-time
  flag (`SQLITE_ENABLE_UPDATE_DELETE_LIMIT`) and `sqlparser` doesn't
  expose them under SQLiteDialect. Workaround: rewrite as
  `UPDATE ... WHERE rowid IN (SELECT rowid FROM ... LIMIT N)`.
- **`ON UPDATE` foreign-key actions** are parsed and stored on the
  catalog but not enforced — only `ON DELETE` actions execute.
  Updating a referenced parent key won't cascade, set null, or
  restrict. Workaround: avoid changing PK values; use surrogate keys.

## Maintenance

- **`REINDEX`** is accepted as a no-op. Our btree implementation
  doesn't suffer from the corruption modes (collation changes, etc.)
  that real SQLite uses REINDEX to recover from. Tools that issue
  REINDEX won't error, but no work is done.
- **`ANALYZE`** is accepted as a no-op. Our planner is rule-based,
  not cost-based, so there's no `sqlite_stat1` to populate.

## Not implemented at all

- **Virtual tables / FTS / R-Tree** — out of scope.
- **`LOAD_EXTENSION`** — not safe in WASM.
- **User-defined functions** from JavaScript — not yet wired.
- **`WITHOUT ROWID` tables** — storage layer assumes rowid keys.
- **Covering / index-only scans** — performance optimization, not
  spec compliance.

## Known follow-ups

These are tracked as v0.2 candidates:

1. Predicate-implication analysis for partial-index lookup-time use
2. Expression-index lookup-time use (recognize matching `expr(col)` in WHERE)
3. ON UPDATE FK action enforcement
4. UPDATE LIMIT / ORDER BY (needs custom parser path)
5. Bare `rowid` on tables without an alias (synthetic column or Row.rowid)
6. sqlite_schema root-page split (btree restructure)
