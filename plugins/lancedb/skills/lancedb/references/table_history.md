# Table version history

Every operation that modifies a LanceDB table commits a new **version**, and versions stay
readable **until they are pruned**. Use this for auditing and forensics on a table: diffing
two versions, answering "what changed since \<date\>", finding which version introduced or
dropped a column, reading the table as it was at some point, or tracing a change back to the
background job behind it.

What you are auditing is the **retained history**, not necessarily the full history: local
`optimize()` (`OptimizeAction::All`) prunes versions older than seven days as a side effect,
`OptimizeAction::Prune` can prune with any cutoff the caller chose, and remote deployments run
their own retention. Checkout is only promised "as long as the version hasn't been deleted."
So before answering any history question, check whether the versions you need are actually in
the listing — a pruned prefix or a gap means the audit is **incomplete**, and you must say so
rather than presenting what remains as the whole story. Each workflow below notes what to check.

Version numbers and timestamps are available on local/OSS and remote Enterprise/Cloud tables
through both SDKs. **What changed at a version is only available over REST, and only from
servers that implement the `include_operations` extension** — see the limitation and the
capability check below before you plan an approach.

## The version model

- Versions start at 1 and increase by one per commit. Every mutation commits: creating a
  table, appending or deleting rows, adding or dropping columns, building an index, editing
  column or table metadata.
- Pruning removes versions but never renumbers them, so the retained chain may start above 1
  or (after a targeted prune) have gaps. Sort the listing by `version` and check it starts at
  1 and is contiguous; if not, record where retained history actually begins and treat
  anything older as unknown, not as "nothing happened".
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
use the REST endpoint below, or reconstruct it yourself version by version — checkout each
version and read `schema` / `count_rows()` and diff by hand (N round trips, and still no
operation names).

### Through REST — the full picture, where the server supports it

`POST {base_url}/v1/table/{table}/version/list`, with the usual `x-api-key` /
`x-lancedb-database` headers. Resolve the connection first — see `references/remote_connect.md`.

The options are **query parameters, not body fields**. The body is `{}`.

| query param | effect | contract status |
|---|---|---|
| `include_operations=true` | fill in `operation`, `num_rows`, `added_columns`, `removed_columns` | **server extension** — see below |
| `descending=true` | newest first (ordering is otherwise implementation-defined) | pinned |
| `limit=N`, `page_token=...` | paginate | pinned |
| `branch=<name>` | that branch's chain instead of main's | pinned |

`include_operations` and the fields it adds are **not part of the pinned Lance Namespace
contract** (v0.8.6 defines only `branch`, `page_token`, `limit`, `descending`, and its
`TableVersion` has no operation, row-count, or column fields). A compliant namespace-backed
or older server ignores the param and returns the bare response — which looks exactly like
"no operations happened", so you must distinguish the two cases before interpreting anything:

- **Capability check:** send `include_operations=true` and look for the `operation` key on the
  returned versions. Present → the server supports enrichment, and the interpretation rules
  below apply. Absent on every version → the server does not support it; do **not** read the
  missing column arrays as "no schema change".
- **Fallback without enrichment:** schema changes are still recoverable — `describe` each
  version of interest with `{"version": N}` (pinned) and diff adjacent schemas yourself; row
  counts via checkout + count. Then state plainly that operation names and per-version row
  deltas are unavailable from this server, rather than reporting "no changes".

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
      "timestamp": "2025-03-11T09:22:07.482Z",
      "timestamp_millis": 1741684927482,
      "manifest_path": "...", "manifest_size": 1313, "e_tag": "...",
      "metadata": {},
      "operation": "Merge",
      "num_rows": 1204,
      "added_columns": [{"name": "price_tier", "type": {"type": "int32"}, "nullable": false}]
    }
  ]
}
```

On a server that passed the capability check, `added_columns` / `removed_columns` are
**omitted entirely** when that version changed no columns — there, a missing key means
"no schema change", not an error. Without the check, a missing key means nothing.

## Reading the operation names

`operation` is the Lance transaction name, not the API you called, and the mapping is
many-to-many — several APIs share one transaction name, and one API can commit different
names depending on its inputs. Per the pinned Lance transaction contract:

| operation | what it can mean |
|---|---|
| `Overwrite` | table created, or fully rewritten (`mode="overwrite"`) |
| `Append` | rows added |
| `Delete` | rows deleted by predicate |
| `Update` | rows updated in place, **or any `merge_insert`** — whole-schema merge inserts commit `Update` (vertical), and partial-schema merge inserts commit `Update` that can add or modify columns (horizontal) |
| `Merge` | new column data merged in: `add_columns`, or an `alter_columns` cast that rewrites the column. **Not** merge-insert, despite the name |
| `Project` | schema-only projection: columns dropped (`drop_columns`) or renamed / altered without touching data (`alter_columns`) |
| `CreateIndex` | index metadata changed — built, replaced, **or dropped** (`drop_index` commits `CreateIndex` too, with only removed indices) |
| `UpdateConfig` | table or column metadata changed (e.g. `update_field_metadata`) |
| `Rewrite` | compaction — data rearranged, nothing logically changed |
| `Restore` | the table was rolled back to an earlier version |

Other names exist for lower-level maintenance. Because the names are ambiguous, **never
attribute a version from the operation name alone** — disambiguate with the returned details:

- Schema change vs. data change: non-empty `added_columns` / `removed_columns`, whatever the
  name says. An `Update` with column changes is a horizontal merge-insert; a bare `Update` is
  a row-level change. `Merge` vs. `Project` tells you whether column data was written or only
  the schema was reshaped.
- `CreateIndex`: compare the index listing (`index/list`, below) before and after — or across
  versions via checkout — to tell a build from a replacement from a drop.

## Common tasks

### Diff two versions

List the range with `include_operations=true` and read the versions **after** the older one,
up to and including the newer one: "what changed between vN and vM" means the commits
vN+1 … vM. Version N's own operation is the baseline — it already happened before the window,
and reporting it as part of the change set is the usual off-by-one here.

First confirm every version in vN+1 … vM is actually in the listing. If any of them — or the
baseline vN itself — has been pruned, the diff of what remains is not "what changed between
vN and vM"; report which versions are missing and mark the answer incomplete.

Row-count movement comes from `num_rows`; schema movement from the column diffs. For the full
schema at either end, describe that version (below) rather than reconstructing it.

### What changed since a point in time

Compare each version's `timestamp` (ISO-8601 `...Z`) or `timestamp_millis` against the cutoff.

- Check the cutoff falls **inside retained history**: if the oldest retained version is
  already newer than the cutoff, commits between the cutoff and that version have been pruned.
  Report the answer as "changes within retained history (from vK, \<timestamp\>)", not as
  everything since the cutoff.
- Timestamp precision **varies by path**. Local manifests carry nanoseconds, and the SDK's
  `list_versions()` timestamps preserve that. Over REST you get an RFC 3339 `timestamp` that
  may include fractional seconds, and/or an integer `timestamp_millis` (the namespace-backed
  shape). Parse whichever is present at its full precision — truncating to seconds before
  comparing against the cutoff misfiles commits near the boundary.
- Even at full precision, near-simultaneous commits can tie — order by version number, never
  by timestamp, when they do.
- A **schema** change is a version with a non-empty `added_columns` / `removed_columns` —
  judge by the arrays, not the operation name (a horizontal merge-insert changes the schema
  under the name `Update`). An **index** change is `operation == "CreateIndex"`, which covers
  builds, replacements, and drops alike.
- Versions with empty column arrays (`Append`, `Delete`, bare `Update`) are data-only. They
  may well fall inside the window; report them as what they are instead of folding them into
  a schema-change answer.

### Find the version that introduced or dropped a column

Scan the history for the version whose `added_columns` (or `removed_columns`) names it. Note
that **version 1 lists every original column as added**, since it is diffed against an empty
schema — so a base column's "added" version is 1, and only later additions are interesting.

If retained history no longer starts at version 1 and no retained version names the column,
the change predates retained history — say "introduced at or before vK (earliest retained)"
rather than claiming a version. The earliest retained version's own diff is against a pruned
predecessor, so don't trust its `added_columns` as a real change set either.

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

Checkout only works for versions that still exist — a pruned version fails to open. If the
version you were asked about is no longer in the listing, that is the finding: report that it
has been pruned, don't substitute the nearest surviving version without saying so.

Do **not** confuse `version/describe` with `describe`: `POST /v1/table/{table}/version/describe`
returns manifest facts only (path, size, etag, `timestamp_millis`) and no schema. For the
schema at a version, use `describe` with `{"version": N}`.

### Index history

`POST /v1/table/{table}/index/list` returns each index with a **nullable** `created_at`,
which is how you date an index without walking the version chain. On current servers it is
an RFC 3339 date-time with fractional seconds (e.g. `"2026-06-18T21:37:36.637Z"`); legacy
deployments sent an integer unix timestamp in milliseconds instead — handle both shapes.
When it is null or absent, the creation time is unknown: fall back to the `CreateIndex`
versions rather than treating it as epoch zero. Cross-check against those versions when you
need the version number too.

### Trace a version back to the job behind it

Mind the direction of the link. When a job spec carries a `table_version` (specs are
job-type-specific — many don't), it records the version the job **read as input**: a snapshot
pin, not the version the job produced. Read-only jobs pin versions too — a
`prewarm_page_cache` job carries `spec.table_version` and never commits anything — so
matching a version number against job specs is **correlation, not attribution**.

To attribute a commit to a job, in order of strength:

1. **The version's own `metadata`** (in the version listing): writers can stamp commit
   metadata, and a job id or job name there is a documented link.
2. **A documented output link on the job**: an explicit committed-version or manifest
   reference in the job's `status`/output (both are job-type-specific — look for the field,
   don't assume it). Geneva `JobRecord`s carry a `manifest_id` you can compare against the
   version's manifest.
3. **Time-window matching** — the commit's timestamp falls between the job's start and
   completion, on the same table, with an operation type consistent with the job. Report
   this as "consistent with job X", never as "caused by job X".

Route by writer: UDF column backfills and materialized-view refreshes — the usual authors of
`Merge`/`Update` commits — are **Geneva jobs in their own `geneva_jobs` registry**, not
`/v1/jobs`. Check both registries before concluding no job was involved. Both are covered in
`references/remote_jobs.md`.

## Gotchas

- `include_operations`, `descending`, `limit`, and `branch` are **query params**. Putting them
  in the JSON body does nothing and you silently get the bare response with no operations —
  indistinguishable from a server that doesn't support the extension. If the capability check
  fails, rule out this mistake before concluding the server can't do it.
- `include_operations=true` reads a manifest per version, which is why it is opt-in. Pair it
  with `limit` on tables with long histories.
- Without `descending=true` the order is implementation-defined — sort by `version` yourself
  rather than trusting the response order.
- The SDKs' `list_versions()` / `listVersions()` will not gain the operation fields by passing
  extra arguments; the REST endpoint is the only route to them.
- `restore()` (and `POST /v1/table/{table}/restore`) rolls the table back by committing a new
  version. It is a **write** — never reach for it during a read-only investigation.
- On local tables, `optimize()` with no arguments is also a pruner (seven-day default). If an
  audit and maintenance are both on the agenda, do the audit first.
