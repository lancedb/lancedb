# Table version history

Every operation that modifies a LanceDB table commits a new **version**, and the whole chain
stays readable. Use this for auditing and forensics on a table: diffing two versions,
answering "what changed since \<date\>", finding which version introduced or dropped a column,
reading the table as it was at some point, or tracing a change back to the background job
behind it.

Version numbers and timestamps are available on local/OSS and remote Enterprise/Cloud tables
through both SDKs. **What changed at a version is only available over REST** — see the
limitation below before you plan an approach.

## The version model

- Versions start at 1 and increase by one per commit. Every mutation commits: creating a
  table, appending or deleting rows, adding or dropping columns, building an index, editing
  column or table metadata.
- A version is a **manifest snapshot, not a diff**. "What changed at version N" is always
  derived by comparing N against N-1 — which is what `include_operations` does server-side.
- Branches have their own version chains. Pass the branch explicitly to read one; see
  `references/branch_ops.md`.

## Listing history

### Through the SDKs — version and timestamp only

```python
t = db.open_table("products")
t.version            # current version number, e.g. 7
t.list_versions()    # [{'version': 1, 'timestamp': datetime(...), 'metadata': {}}, ...]
```

```typescript
const t = await db.openTable("products");
await t.listVersions(); // [{ version, timestamp: Date, metadata }]
```

**Limitation worth planning around:** both SDKs return only `version`, `timestamp`, and
`metadata`. Neither tells you *what happened* at a version — no operation name, no row count,
no schema diff. If the task is "what changed", the SDK list alone cannot answer it. Either
use the REST endpoint below, or read the schema at each version and diff them yourself
(N round trips, and still no operation names or row counts).

### Through REST — the full picture

`POST {base_url}/v1/table/{table}/version/list`, with the usual `x-api-key` /
`x-lancedb-database` headers. Resolve the connection first — see `references/remote_connect.md`.

The options are **query parameters, not body fields**. The body is `{}`.

| query param | effect |
|---|---|
| `include_operations=true` | fill in `operation`, `num_rows`, `added_columns`, `removed_columns` |
| `descending=true` | newest first (ordering is otherwise implementation-defined) |
| `limit=N`, `page_token=...` | paginate |
| `branch=<name>` | that branch's chain instead of main's |

```bash
curl -s -X POST "{base_url}/v1/table/products/version/list?include_operations=true&descending=true" \
  -H "x-api-key: <key>" -H "x-lancedb-database: <database>" \
  -H "content-type: application/json" -d '{}'
```

```json
{
  "versions": [
    {
      "version": 14,
      "timestamp": "2025-03-11T09:22:07Z",
      "timestamp_millis": 1741684927000,
      "manifest_path": "...", "manifest_size": 1313, "e_tag": "...",
      "metadata": {},
      "operation": "Merge",
      "num_rows": 1204,
      "added_columns": [{"name": "price_tier", "type": {"type": "int32"}, "nullable": false}]
    }
  ]
}
```

`added_columns` / `removed_columns` are **omitted entirely** when that version changed no
columns — treat a missing key as "no schema change", not as an error.

## Reading the operation names

`operation` is the Lance transaction name, not the API you called. The mapping you will
actually see:

| operation | what someone did |
|---|---|
| `Overwrite` | table created, or fully rewritten (`mode="overwrite"`) |
| `Append` | rows added |
| `Delete` | rows deleted by predicate |
| `Update` | rows updated in place |
| `Merge` | columns added (`add_columns`) or a merge-insert landed |
| `Project` | columns dropped |
| `CreateIndex` | an index was built |
| `UpdateConfig` | table or column metadata changed (e.g. `update_field_metadata`) |
| `Rewrite` | compaction — data rearranged, nothing logically changed |
| `Restore` | the table was rolled back to an earlier version |

Other names exist for lower-level maintenance. The useful split is
that **data-only operations leave the schema untouched**, so their `added_columns` /
`removed_columns` are empty — that is how you separate a schema change from a data change,
rather than guessing from the operation name alone.

## Common tasks

### Diff two versions

List the range with `include_operations=true` and read the versions **after** the older one,
up to and including the newer one: "what changed between vN and vM" means the commits
vN+1 … vM. Version N's own operation is the baseline — it already happened before the window,
and reporting it as part of the change set is the usual off-by-one here.

Row-count movement comes from `num_rows`; schema movement from the column diffs. For the full
schema at either end, describe that version (below) rather than reconstructing it.

### What changed since a point in time

Compare each version's `timestamp` (ISO-8601 `...Z`) or `timestamp_millis` against the cutoff.

- Timestamps land at **second resolution**. Commits inside the same second are
  indistinguishable in time — order by version number, never by timestamp, when they tie.
- A **schema** change is a version with a non-empty `added_columns` / `removed_columns`.
  An **index** change is `operation == "CreateIndex"`.
- Data-only versions (`Append`, `Delete`, `Update`) are neither. They may well fall inside the
  window; report them as what they are instead of folding them into a schema-change answer.

### Find the version that introduced or dropped a column

Scan the history for the version whose `added_columns` (or `removed_columns`) names it. Note
that **version 1 lists every original column as added**, since it is diffed against an empty
schema — so a base column's "added" version is 1, and only later additions are interesting.

### Read the table as it was

```bash
# schema + stats as of version 9
curl -s -X POST "{base_url}/v1/table/products/describe" \
  -H "x-api-key: <key>" -H "x-lancedb-database: <database>" \
  -H "content-type: application/json" -d '{"version": 9}'
```

```python
t.checkout(9)        # handle becomes a read-only view pinned at v9
t.schema             # ... the schema as of v9
t.checkout_latest()  # back to tracking the newest version
```

`checkout` mutates the handle it is called on, so restore it with `checkout_latest()` (TS:
`checkoutLatest()`) before writing through it again.

Do **not** confuse `version/describe` with `describe`: `POST /v1/table/{table}/version/describe`
returns manifest facts only (path, size, etag, `timestamp_millis`) and no schema. For the
schema at a version, use `describe` with `{"version": N}`.

### Index history

`POST /v1/table/{table}/index/list` returns each index with a `created_at` timestamp in
milliseconds, which is how you date an index without walking the version chain. Cross-check
against the `CreateIndex` versions when you need the version number too.

### Trace a version back to the job behind it

Jobs that were started against a specific table version record it in their spec — for example
a `prewarm_page_cache` job's spec carries `table_version`. So the link from history to the job
registry is: find the version, list the jobs on that table, describe them, and match on
`spec.table_version`. Job listing and describing are in `references/remote_jobs.md`.

## Gotchas

- `include_operations`, `descending`, `limit`, and `branch` are **query params**. Putting them
  in the JSON body does nothing and you silently get the bare response with no operations.
- `include_operations=true` reads a manifest per version, which is why it is opt-in. Pair it
  with `limit` on tables with long histories.
- Without `descending=true` the order is implementation-defined — sort by `version` yourself
  rather than trusting the response order.
- The SDKs' `list_versions()` / `listVersions()` will not gain the operation fields by passing
  extra arguments; the REST endpoint is the only route to them.
- `restore()` (and `POST /v1/table/{table}/restore`) rolls the table back by committing a new
  version. It is a **write** — never reach for it during a read-only investigation.
