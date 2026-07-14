# Python Performance Guidance

Use this when writing Python code that ingests data, queries large tables, builds indexes, or investigates latency.

## Ingestion

### Recommended: validate schemas and records with Pydantic

Favor `LanceModel` for readable Python schema definitions and validate source
records before writing. Use PyArrow directly for Arrow-native or streaming
pipelines where it is the clearer representation.

```python
from lancedb.pydantic import LanceModel, Vector

class Document(LanceModel):
    id: int
    text: str
    vector: Vector(384, nullable=False)

rows = [Document.model_validate(row) for row in source_rows]
table = db.create_table("documents", schema=Document)
table.add(rows)
```

### Recommended: bulk ingestion for materialized data

```python
table.add(arrow_table)
table.add(df)
table.add(pa.dataset("data/", format="parquet"))
```

For very large initial loads, create the table empty first, then call `add(...)`. Passing data directly to `create_table(name, data)` can skip the auto-parallel write path.

### Recommended: iterator ingestion for generated or streamed data

```python
def batches():
    for raw in source:
        vectors = model.encode(raw["text"])
        yield pa.RecordBatch.from_pydict({**raw, "vector": vectors})

table.add(batches())
```

Use chunks of several thousand rows or more when practical. Tiny batches and per-row writes create many small fragments.

### Anti-pattern: per-row `add()`

```python
for row in rows:
    table.add([row])
```

Each call creates a version and fragment. This slows ingestion and later queries.

## Indexing

- Build a vector index once brute-force vector search becomes too slow. As a rule of thumb, local brute force is fine below roughly 100K vectors; beyond that, build an index.
- Use `IVF_PQ` as the general-purpose default. Enterprise builds this automatically.
- Use scalar indexes for filtered columns and merge/upsert keys.
- Use `BTREE` for mostly distinct numeric/string/temporal columns, `BITMAP` for booleans and low-cardinality columns, and `LABEL_LIST` for list membership queries.
- Keep full-text defaults unless phrase queries require position data.

## Querying

Always be explicit:

```python
table.search(query_vector).select(["id", "title"]).limit(20)
```

- `select()` reduces bytes read and transferred.
- `limit()` prevents accidental full-table materialization.
- Pre-filtering is the default and guarantees returned rows satisfy the predicate.
- Use post-filtering only when fewer than `limit` results are acceptable.

## Recall Tuning

Tune one knob at a time:

- Quantized indexes: raise `refine_factor` to rescore more candidates on full vectors.
- HNSW-backed indexes: raise `ef`; start around `1.5 * k`, increase toward `10 * k` if recall is short.
- IVF candidate breadth: `nprobes` is auto-tuned; override only when a selective pre-filter leaves too few neighbors.

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

```python
table.optimize()
```

If the user wants more aggressive local disk cleanup, pass a shorter cleanup retention window:

```python
from datetime import timedelta

table.optimize(cleanup_older_than=timedelta(days=1))
```

Do not use very short cleanup windows when the application depends on time travel, rollback, or old versions.

## Diagnostics

Before changing code or indexes, inspect:

```python
print(table.search(query_vector).where("year > 2000").limit(10).analyze_plan())
print(table.index_stats("vector_idx"))
```

Look for high scan bytes, missing indexes, fragmented data, and unindexed rows.

## Python Multiprocessing

When using multiprocessing, use `spawn` rather than `fork`. LanceDB is multi-threaded internally, and `fork` plus a multi-threaded process is unsafe.
