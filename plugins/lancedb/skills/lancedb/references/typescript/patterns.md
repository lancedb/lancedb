# TypeScript Patterns

Use these patterns when writing TypeScript code with `@lancedb/lancedb`.

## Recommended Patterns

### Bounded query

Use this for application reads, scripts, and examples:

```typescript
const rows = await table
  .query()
  .where("status = 'ready'")
  .select(["id", "text"])
  .limit(20)
  .toArray();
```

### Bounded vector search

```typescript
const rows = await table
  .search(queryVector)
  .select(["id", "text"])
  .limit(20)
  .toArray();
```

### Batch streaming for larger reads

When the task needs many rows, avoid collecting everything at once:

```typescript
for await (const batch of table
  .query()
  .where("status = 'ready'")
  .select(["id", "text"])
  .limit(10_000)) {
  process(batch);
}
```

## Anti-Patterns

**Avoid the following anti-patterns in your code.**

### Table-level full materialization

Avoid whole-table collectors in portable or large-table code:

```typescript
const tableArrow = await table.toArrow();
```

Why: local tables expose these whole-table collectors, but remote tables intentionally do not — a remote production table can be far larger than a local development table, so it is easy to accidentally pull the entire table into memory.

### Unbounded result collection

Avoid query/search collection without a meaningful limit:

```typescript
const rows = await table.query().toArray();              // unbounded plain scan
const rows = await table.search(queryVector).toArray();  // unbounded vector search
```

Prefer `select(...).limit(...)` before collecting; for large reads, stream in batches instead.

### Per-row writes

Avoid loops that write one row per call:

```typescript
for (const row of rows) {
  await table.add([row]); // one commit + fragment per row
}
```

Each `add()` creates a new version and fragment. Pass the whole batch in a single call, or chunk very large inputs:

```typescript
await table.add(rows); // single commit
// for very large inputs, add in chunks of several thousand rows
```

### Drop-then-reuse the same table name (Enterprise/Cloud)

Avoid dropping or overwriting a remote table and then reusing that name right away:

```typescript
await db.dropTable("my_table");
const table = await db.createTable("my_table", rows);                    // reads 500 for ~5 min
const table = await db.createTable("my_table", rows, { mode: "overwrite" }); // same problem
```

Why: Enterprise/Cloud splits DDL (control plane) from query serving (data plane). The data plane caches the dataset behind a table name for up to `table_cache_ttl` (default 300s / 5 min), so after a drop/overwrite the DDL succeeds but queries against the reused name return `500 Internal Server Error` until the cache expires — and a fresh `describe` may still show the old schema. Instead, write to a **fresh name**, use `tableNames()` and fail if it already exists, then `renameTable(fresh, final)` onto the final name only after the old table's drop has propagated (~5 min). See the "Enterprise: never drop-then-reuse the same table name" section in `SKILL.md`. Local/OSS tables have no separate data plane — overwrite freely there.

### Guessing performance fixes

Avoid changing `nprobes`, `refineFactor`, `ef`, or index settings before checking `analyzePlan()` and `indexStats(...)`. Diagnose first, then tune one knob at a time.
