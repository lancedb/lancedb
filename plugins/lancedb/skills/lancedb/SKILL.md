---
name: lancedb
description: Use when writing, reviewing, debugging, or documenting LanceDB pipelines in Python or TypeScript, especially code that should work across local LanceDB OSS tables and remote LanceDB Enterprise/Cloud tables. Helps avoid non-portable full-table materialization, choose idiomatic query/search patterns, and apply LanceDB performance defaults for ingestion, indexing, filtering, and diagnostics.
---

# Building LanceDB Pipelines

Use this skill to produce LanceDB pipelines that are portable between local and remote tables (for LanceDB Enterprise/Cloud) and idiomatic for the selected SDK.

## LanceDB Table Modes

LanceDB has two common execution modes:

- **Local table**: embedded, open source, in-process LanceDB. The client opens data from a local path or object storage URI and executes queries in the application process.
- **Remote table**: LanceDB Enterprise/Cloud table opened through a `db://...` URI. The data may be very large, commonly backed by object storage, and queried through a remote service.

Do NOT assume local-only table helpers exist on remote tables. If the user asks for LanceDB Enterprise, Cloud, `db://...`, production remote access, or a remote table, focus on the remote table path: use `search()` / `query()`, keep reads bounded with `select()` and `limit()`, and avoid table-level full materialization APIs.

## Workflow

1. Identify the SDK: Python, TypeScript, or both.
2. Identify the table mode: local/embedded OSS, remote Enterprise/Cloud, or portable across both. If the user says "LanceDB Enterprise", choose the remote table path.
3. Read the matching language branch before writing or changing code:
   - Python patterns: `references/python/patterns.md`
   - Python API quick reference: `references/python/api_reference.md`
   - Python performance guidance: `references/python/performance.md`
   - TypeScript patterns: `references/typescript/patterns.md`
   - TypeScript API quick reference: `references/typescript/api_reference.md`
   - TypeScript performance guidance: `references/typescript/performance.md`
   - Column metadata authoring (both SDKs): `references/column_metadata.md`
   - Branch operations (both SDKs): `references/branch_ops.md`
4. Start with `patterns.md` for the selected SDK. Read `api_reference.md` when choosing method names or return collectors. Read `performance.md` when the task involves ingestion, indexing, filtering, query tuning, diagnostics, or large datasets. Read `column_metadata.md` when the task is documenting, tagging, classifying, or grouping table columns (field descriptions, `lancedb:tag:*` tags, logical column families). Read `branch_ops.md` when the task involves branch lifecycle (list/create/delete), writing to a non-main branch, or verifying a change stayed off main.
5. For Python schemas, favor Pydantic models and validate records before writing. Use PyArrow schemas when Arrow-native, streaming, or highly dynamic data makes them materially better suited.
6. Prefer `search()` or `query()` builders with explicit `select()` and `limit()` for reads.
7. Avoid table-level full materialization in remote or portable code. This is the main local-vs-remote read pitfall.
8. After a successful embedded OSS ingestion, call `table.optimize()`. Do not call it for Enterprise/Cloud; remote maintenance is automatic.
9. For remote Enterprise/Cloud writes, never drop-then-reuse or `mode="overwrite"` the same table name — see "Enterprise: never drop-then-reuse the same table name" below. This is the main local-vs-remote write pitfall.
10. If reviewing an existing file or repo, run `scripts/check_materialization.py` on the relevant paths and inspect each finding before editing.
11. Cross-check unfamiliar or non-trivial API claims against the source tree instead of relying on memory.

## Core Portability Rule

Do not write code that assumes a local table API will exist on a remote table. Remote tables can be very large, so whole-table materialization helpers are intentionally unavailable or unsafe.

This does **not** mean result conversion is forbidden. Bounded query/search result collection is normal:

- Python: `table.search(...).select([...]).limit(10).to_pandas()`
- TypeScript: `await table.search(...).select([...]).limit(10).toArray()`

The unsafe pattern is table-level or unbounded collection, plus local-only dataset escape hatches in remote code:

- Python: `table.to_pandas()`, `table.to_arrow()`, `table.to_polars()`; `table.to_lance()` is local/OSS-only dataset access, not materialization
- TypeScript: `await table.toArrow()`, `await table.query().toArray()` without `limit()`

## Enterprise: never drop-then-reuse the same table name

LanceDB Enterprise/Cloud splits a **control plane** (DDL: create/drop/rename) from a **data plane** (query nodes that serve reads). Query nodes cache the resolved dataset for a table name for up to `table_cache_ttl` — **default 300 seconds (5 minutes)**. After you drop or overwrite a table, the control plane updates immediately but the data plane keeps serving the *old* dataset until that cache entry expires. During the window the two planes disagree.

The failure this causes: you `drop_table("t")` then immediately `create_table("t", ...)` (or `create_table("t", ..., mode="overwrite")`). The DDL returns success, but every query against `t` returns **`500 Internal Server Error`** (the query node resolves the stale/deleted dataset), and a fresh `describe` may still show the *old* schema/version. It looks like your write silently failed; it didn't — the name is cached.

**`mode="overwrite"` has the same problem** — it is a drop+create of the same name under the hood.

Rules for portable Enterprise ingestion:

1. **Never reuse a table name you just dropped/overwrote within the cache TTL.** Do not use `mode="overwrite"` to replace an existing Enterprise table in place.
2. To (re)load data, **write to a fresh table name** (e.g. `<table>_v2`, or a run-stamped suffix). A brand-new name has no cached data-plane entry, so writes and reads work immediately.
3. Before creating, `list_tables()` and **fail loudly if the name already exists** rather than overwriting — prompt for a new name.
4. To land on a specific final name that is currently occupied by an old table: drop the old table, **wait out the TTL (~5 min), then `rename_table(fresh_name, final_name)`**. Renaming onto a name whose old dataset is still cached hits the same race, so the wait is mandatory. `rename_table` is a supported control-plane op.
5. When you hand a table name back to a human, tell them which step still needs the propagation wait (usually: "the old `t` was dropped; run the rename in ~5 minutes").

This is Enterprise/Cloud-specific. Local/OSS tables have no separate data plane, so `mode="overwrite"` and immediate same-name reuse are fine there.

## Script

Run the scanner when reviewing or modifying an existing codebase:

```bash
python skills/lancedb/scripts/check_materialization.py path/to/file_or_dir
```

The script reports likely unsafe full-table materialization in Python and TypeScript. Treat results as review prompts, not automatic proof of a bug.
