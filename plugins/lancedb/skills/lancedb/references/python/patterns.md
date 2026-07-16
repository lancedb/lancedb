# Python Patterns

Use these patterns when writing Python code with `lancedb`.

## Before Writing Code

Choose the output type from what the project actually depends on. **Do not assume `pandas` or `polars` is installed** — they are heavy dependencies that many LanceDB projects do not use. `pyarrow`, by contrast, ships as a LanceDB dependency and is always available, so it is a safe default to lean on.

Default output (after applying `select()` and `limit()`):

- **Python objects**: `.to_list()` — a list of dicts, no extra dependencies. Prefer this for scripts, examples, and agent-generated code unless there is a reason to do otherwise.
- **PyArrow**: `.to_arrow()` — a `pyarrow.Table`, when the surrounding code is Arrow-native or you need columnar/zero-copy handoff.

Only reach for a DataFrame when the project *already* declares that dependency:

- Pandas projects (pandas in `pyproject.toml`/requirements): `.to_pandas()`.
- Polars projects (polars declared): `.to_polars()`.

If unsure, check the dependency manifest or the imports in surrounding files. When in doubt, use `.to_list()` or `.to_arrow()`.

## Schema Design and Validation

Favor `LanceModel` and Pydantic validation for Python schemas. They keep field
types readable, validate source records before a write, and map directly to a
LanceDB schema. Use `Vector(dimension)` for fixed-size vectors:

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

Use PyArrow schemas instead when the pipeline is already Arrow-native, needs
record-batch streaming, or has runtime schema requirements that would make a
Pydantic model harder to understand. Declare Pydantic as a direct project
dependency when application code imports it, even if LanceDB also depends on it.

## Recommended Patterns

### Bounded search or query

Use this for application reads, examples, notebooks, and agent-generated scripts:

```python
results = (
    table.search(query_vector)
    .where("status = 'ready'")
    .select(["id", "text"])
    .limit(20)
    .to_list()  # or .to_arrow(); .to_pandas()/.to_polars() only if the project uses them
)
```

Why: `search()` works across local and remote tables and on both the sync and async clients. `select()` avoids fetching unused columns. `limit()` prevents accidental full-table reads. `.to_list()` and `.to_arrow()` avoid assuming pandas/polars is installed (see "Before Writing Code").

For a **plain scan** (no query vector), the entry point differs by client:

```python
# Sync client: no .query() method — use .search() with no argument.
rows = table.search().where("status = 'ready'").select(["id", "text"]).limit(20).to_list()

# Async client: use .query().
rows = await async_table.query().where("status = 'ready'").select(["id", "text"]).limit(20).to_list()
```

`table.query()` on a sync table raises `AttributeError` (verified on `lancedb` 0.34.0). See the "Sync vs Async Scan API" section in `api_reference.md`.

### Bounded query result conversion

It is fine to collect bounded query/search results:

```python
arrow_table = table.search().select(["id"]).limit(100).to_arrow()  # sync plain scan
rows = table.search(query_vector).limit(10).to_list()
df = table.search(query_vector).limit(10).to_pandas()  # only if pandas is a project dep
```

### Local-only Lance dataset API

`table.to_lance()` does not itself materialize the full dataset. It returns the underlying `lance.LanceDataset`, making the table accessible through the PyLance dataset API. Use it when the task is explicitly local/OSS and needs Lance dataset methods not exposed by LanceDB:

```python
# Local/OSS only: RemoteTable does not expose table.to_lance().
ds = table.to_lance()
for batch in ds.to_batches(columns=["id", "text"], batch_size=10_000):
    process(batch)
```

### Async Python

Keep the same shape and bound the result before collecting:

```python
results = await (
    async_table.query()
    .where("status = 'ready'")
    .select(["id", "text"])
    .limit(20)
    .to_list()  # or .to_arrow()
)
```

## Anti-Patterns

**Avoid the following anti-patterns in your code.**

### Table-level full materialization

Avoid whole-table collectors in portable or large-table code:

```python
df = table.to_pandas()
arrow_table = table.to_arrow()
polars_df = table.to_polars()
```

Why: local tables expose these whole-table collectors, but remote tables intentionally do not — a remote production table can be far larger than a local development table, so it is easy to accidentally pull the entire table into memory.

`table.to_lance()` is different: it is not a full materialization call, but it is still local/OSS-only and should not appear in code meant to run against remote Enterprise tables.

### Unbounded result collection

Avoid query/search collection without a meaningful limit:

```python
rows = table.search().to_list()               # unbounded plain scan
rows = table.search(query_vector).to_list()   # unbounded vector search
```

Prefer `select(...).limit(...)` before collecting; for large reads, stream in batches instead.

### Per-row writes

Avoid loops that write one row per call:

```python
for row in rows:
    table.add([row])  # one commit + fragment per row
```

Each `add()` creates a new version and fragment. Pass the whole batch in a single call, or chunk very large inputs:

```python
table.add(rows)  # single commit
# for very large inputs, add batches of several thousand rows
```

After the final successful write to an embedded OSS table, call
`table.optimize()`. Skip this for Enterprise/Cloud tables because their
maintenance is automatic.

### Drop-then-reuse the same table name (Enterprise/Cloud)

Avoid dropping or overwriting a remote table and then reusing that name right away:

```python
db.drop_table("my_table")
table = db.create_table("my_table", data=rows)          # reads 500 for ~5 min
table = db.create_table("my_table", data=rows, mode="overwrite")  # same problem
```

Why: Enterprise/Cloud splits DDL (control plane) from query serving (data plane). The data plane caches the dataset behind a table name for up to `table_cache_ttl` (default 300s / 5 min), so after a drop/overwrite the DDL succeeds but queries against the reused name return `500 Internal Server Error` until the cache expires — and a fresh `describe` may still show the old schema. Instead, write to a **fresh name**, use `list_tables()` and fail if it already exists, then `rename_table(fresh, final)` onto the final name only after the old table's drop has propagated (~5 min). See the "Enterprise: never drop-then-reuse the same table name" section in `SKILL.md`. Local/OSS tables have no separate data plane — overwrite freely there.

### Guessing performance fixes

Avoid changing `nprobes`, `refine_factor`, or index types before checking the query plan and index stats. Diagnose first, then tune one knob at a time.
