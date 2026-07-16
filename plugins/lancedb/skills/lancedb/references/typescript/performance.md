# TypeScript Performance Guidance

Use this when writing TypeScript code that ingests data, queries large tables, builds indexes, or investigates latency.

## Ingestion

- Prefer bulk or batched writes.
- Avoid per-row write loops; they create many small commits/fragments.
- For generated data, accumulate reasonable batches before adding.
- For file-backed data, prefer APIs that stream from Arrow/Parquet-style inputs when available.

## Indexing

- Build a vector index once brute-force vector search becomes too slow. As a rule of thumb, local brute force is fine below roughly 100K vectors; beyond that, build an index.
- Use the general-purpose vector index defaults unless the task has explicit recall/latency requirements.
- Build scalar indexes for filtered columns and merge/upsert keys.
- Use full-text index phrase options only when phrase queries require them.

## Querying

Always be explicit:

```typescript
await table.search(queryVector).select(["id", "title"]).limit(20).toArray();
```

- `select()` reduces bytes read and transferred.
- `limit()` prevents accidental full-table collection.
- Pre-filtering is the default behavior. Use `postfilter()` only when fewer than `limit` results are acceptable.

## Recall Tuning

Tune one knob at a time:

- Quantized indexes: raise `refineFactor(...)` to rescore more candidates on full vectors.
- HNSW-backed indexes: raise `ef(...)`; start around `1.5 * k`, increase toward `10 * k` if recall is short.
- IVF candidate breadth: `nprobes(...)` is usually auto-tuned; override only when a selective pre-filter leaves too few neighbors.

## Maintenance

After every successful embedded OSS/local ingestion, call `table.optimize()`.
Do not add this to LanceDB Enterprise/Cloud remote table code; remote compaction
and cleanup are handled automatically based on the Enterprise cluster
configuration.

Why local maintenance is needed:

- Frequent writes can create many small fragments. Queries then need to scan across more files, which can increase latency.
- Updates, deletes, and appends create new table versions. Old versions are retained for time travel and rollback, which can grow disk usage.
- Indexes may have newly added rows that are not yet fully optimized into the index structure.

For local/OSS tables, run `optimize()` after the final successful ingestion
write. Also run it after later batches of update/delete operations or on a
regular maintenance schedule:

```typescript
await table.optimize();
```

If the user wants more aggressive local disk cleanup, pass a shorter cleanup retention window:

```typescript
const olderThan = new Date(Date.now() - 24 * 60 * 60 * 1000);
await table.optimize({ cleanupOlderThan: olderThan });
```

Do not use very short cleanup windows when the application depends on time travel, rollback, or old versions.

## Diagnostics

Before changing code or indexes, inspect:

```typescript
console.log(await table.search(queryVector).where("year > 2000").limit(10).analyzePlan());
console.log(await table.indexStats("vector_idx"));
```

Look for high scan cost, missing indexes, fragmented data, and unindexed rows.
