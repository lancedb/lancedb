# Python API Reference

Quick method reference for Python LanceDB code. Cross-check source for non-trivial claims.

## Connect

```python
import lancedb

db = lancedb.connect("./camelot-db")         # local/OSS
db = lancedb.connect("db://my-db", api_key=api_key, region=region)  # remote
```

**Place the local database directory next to the script/entrypoint that opens it** (i.e. resolve the path relative to the script, `Path(__file__).parent / "camelot-db"`), not buried under a shared `data/` folder. The Lance dataset is the database, not a data file — keeping it beside its code makes ownership obvious and paths stable regardless of the working directory the script is launched from.

**Do not name the directory `lancedb`** (e.g. `./lancedb`, `./data/lancedb`). It collides with the imported `lancedb` package name, which is confusing to read and easy to shadow in scripts. Give it a name derived from the repo or dataset with a clear prefix/suffix — for example `./<dataset>-db`, `./<repo>_lancedb`, or `./vectordb`.

Async:

```python
db = await lancedb.connect_async("./camelot-db")
```

## Table Reads

| Task | Preferred API |
| --- | --- |
| Vector search | `table.search(query_vector).limit(k)` |
| Full scan with filters/projection (sync) | `table.search().where(...).select(...).limit(...)` |
| Full scan with filters/projection (async) | `table.query().where(...).select(...).limit(...)` |
| Filter | `.where("col > 10")` |
| Projection | `.select(["id", "text"])` |
| Bound result count | `.limit(20)` |
| Collect bounded result as Python objects (default, no extra deps) | `.to_list()` on query/search result |
| Collect bounded result as Arrow (default, `pyarrow` always available) | `.to_arrow()` on query/search result |
| Collect bounded result as pandas (only if project uses pandas) | `.to_pandas()` on query/search result |
| Collect bounded result as Polars (only if project uses polars) | `.to_polars()` on query/search result |

## Sync vs Async Scan API

The plain-scan entry point differs between the sync and async clients. **Verified against `lancedb` 0.34.0** — re-check if the pinned version changes:

- **Sync** (`lancedb.connect(...)`): the table has **no `.query()` method**. Use `.search()` with no argument for a plain scan; it returns a query builder that supports `.where()`, `.select()`, `.limit()`, and the `.to_list()` / `.to_arrow()` / `.to_pandas()` / `.to_polars()` collectors.
  ```python
  rows = table.search().where("status = 'ready'").select(["id", "text"]).limit(20).to_list()
  ```
- **Async** (`lancedb.connect_async(...)`): the table has **both** `.query()` and `.search()`. Use `.query()` for a plain scan.
  ```python
  rows = await async_table.query().where("status = 'ready'").select(["id", "text"]).limit(20).to_list()
  ```

Do not call `table.query()` on a sync table — it raises `AttributeError`.

## Local vs Remote Table Methods

| API | Local table | Remote table | Agent guidance |
| --- | --- | --- | --- |
| `table.search(...)` | Yes | Yes | Preferred read path (sync + async) |
| `table.query()` | Async only | Async only | Sync scan path is `table.search()`; `.query()` is the async scan builder |
| `table.to_pandas()` | Yes | No / unsafe for portability | Avoid in portable code |
| `table.to_arrow()` | Yes | No / unsafe for portability | Avoid in portable code |
| `table.to_polars()` | Yes | No / unsafe for portability | Avoid in portable code |
| `table.to_lance()` | Yes | No | Local/OSS escape hatch only |

## Indexes

Use `create_index(...)` for vector indexes and modern index configs. Use scalar indexes for filtered or merge keys.

Common calls:

```python
table.create_index("vector")
table.create_scalar_index("status")
table.create_fts_index("text")
```

Check source docs before specifying advanced index config names or parameters.

## Filtering And Recall Knobs

```python
table.search(query_vector).where("status = 'ready'")  # pre-filter by default
table.search(query_vector).where("status = 'ready'", prefilter=False)
table.search(query_vector).limit(10).refine_factor(20)
table.search(query_vector).limit(10).nprobes(50)
```

Use post-filtering only when fewer than `limit` results are acceptable.

## Diagnostics

```python
print(table.search(query_vector).where("year > 2000").limit(10).analyze_plan())
print(table.index_stats("vector_idx"))
```

Use these before changing indexes or search tuning.

## Maintenance

```python
table.optimize()
```

Call this after every successful local/OSS ingestion. It handles compaction, cleanup of old versions according to retention, and index optimization. Do not add this for LanceDB Enterprise/Cloud remote tables; Enterprise handles compaction and cleanup automatically from cluster configuration.
