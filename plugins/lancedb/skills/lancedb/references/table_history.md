# Table version history

Every operation that modifies a LanceDB table commits a new **version**, and versions stay
readable **until they are pruned**. Use this for auditing and forensics on a table: diffing
two versions, answering "what changed since \<date\>", finding which version introduced or
dropped a column, reading the table as it was at some point, or tracing a change back to the
background job behind it.

What you are auditing is the **retained history**, not necessarily the full history: local
`optimize()` prunes versions older than seven days as a side effect, `OptimizeAction::Prune`
takes any cutoff, and remote deployments run their own retention. Before answering any history
question, check that the versions you need are actually in the listing — a pruned prefix or a
gap means the audit is **incomplete**, and you must say so rather than presenting what remains
as the whole story. Each workflow below notes what to check.

Version numbers and timestamps are available on local/OSS and remote Enterprise/Cloud tables
through both SDKs. **What changed at a version is only available over REST, and only from
servers that implement the `include_operations` extension** — see the limitation and the
capability check below before you plan an approach.

## The version model

- Versions start at 1 and increase by one per commit. Every mutation commits: creating a
  table, appending or deleting rows, adding or dropping columns, building an index, editing
  column or table metadata.
- Pruning removes versions but never renumbers them. Sort the listing by `version`: a gap
  means pruned versions — treat what's missing as unknown, not as "nothing happened".
- A start above 1 means pruning **only on the main branch**. A branch is created by
  shallow-cloning a source version, so its chain legitimately begins there — versions below
  it live in the parent, not the branch. Establish the branch's source version first, and
  only read a higher-than-expected start as retention loss relative to that baseline.
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
no schema diff. On remote tables the SDKs also expose **no continuation token**, so if the
server paginated the listing there is no way to fetch the rest — don't present a remote SDK
listing as the complete chain; use REST and follow `page_token` when completeness matters.
If the task is "what changed", the SDK list alone cannot answer it. Either
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

Pagination is part of completeness: a non-null, non-empty `page_token` in the **response** means another page
exists. Keep requesting until it is absent, null, or empty string — every range and retention check in this guide
assumes the full chain was loaded, so a listing cut short by pagination is an incomplete audit.

`include_operations` and the fields it adds are **not part of the pinned Lance Namespace
contract** (v0.8.6 defines only `branch`, `page_token`, `limit`, `descending`, and its
`TableVersion` has no operation, row-count, or column fields). A compliant namespace-backed
or older server ignores the param and returns the bare response — which looks exactly like
"no operations happened", so you must distinguish the two cases before interpreting anything:

- **Capability check — per field:** send `include_operations=true` and check each field you
  need. `operation` present proves only the operation field; it does **not** establish the
  column arrays or their omission semantics — an extension version can emit `operation` while
  never emitting schema fields. Treat a missing array as "no schema change" only when some
  retained version demonstrates the field family (version 1, when retained, always lists its
  original columns as `added_columns`). Otherwise absence means nothing — make adjacent
  per-version schema comparison authoritative for schema questions.
- **Fallback without enrichment:** reconstruct manually, as in the SDK path above — diff
  adjacent per-version schemas (`describe` with `{"version": N}` is pinned), row counts via
  checkout — and state that operation names and row deltas are unavailable from this server,
  rather than reporting "no changes".

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

On a server that demonstrably emits the column arrays, `added_columns` / `removed_columns`
are **omitted entirely** when that version changed no columns — there, a missing key means
"no schema change", not an error. Without that demonstration, a missing key means nothing.

## Reading the operation names

`operation` is the Lance transaction name, not the API you called, and the mapping is
many-to-many — several APIs share one transaction name, and one API can commit different
names depending on its inputs. Per the pinned Lance transaction contract:

| operation | what it can mean |
|---|---|
| `Overwrite` | table created, or fully rewritten (`mode="overwrite"`) |
| `Append` | rows added |
| `Delete` | rows deleted by predicate |
| `Update` | rows updated in place, **or any `merge_insert`** — a partial-schema merge-insert commits `Update` and can even add columns |
| `Merge` | new column data written: `add_columns`, or an `alter_columns` cast. **Not** merge-insert, despite the name |
| `Project` | schema-only reshape: columns dropped, or renamed/altered without touching data |
| `CreateIndex` | index metadata changed — built, replaced, **or dropped** (`drop_index` commits this too) |
| `UpdateConfig` | table or column metadata changed (e.g. `update_field_metadata`) |
| `Rewrite` | compaction — data rearranged, nothing logically changed |
| `Restore` | the table was rolled back to an earlier version |

Other names exist for lower-level maintenance. Because the names are ambiguous, **never
attribute a version from the operation name alone**: a schema change is a non-empty
`added_columns` / `removed_columns`, whatever the name says, and an `Update` with column
changes is a partial-schema merge-insert. For `CreateIndex`, compare the index listing
across versions to tell a build from a replacement from a drop.

## Common tasks

### Diff two versions

"What changed between vN and vM" means the commits vN+1 … vM — version N's own operation is
the baseline, and including it is the usual off-by-one here. Confirm vN and every version in
vN+1 … vM are actually in the listing; if any were pruned, report which are missing and mark
the answer incomplete.

Row-count movement comes from `num_rows`; schema movement from the column diffs. For the full
schema at either end, describe that version (below) rather than reconstructing it.

### What changed since a point in time

Compare each version's `timestamp` (ISO-8601 `...Z`) or `timestamp_millis` against the cutoff.

- If the oldest retained version is newer than the cutoff, decide **why** before reporting
  anything: pruning is only one explanation. Identify the chain's expected baseline first —
  version 1 on main, the source version on a branch. If the baseline is missing — main
  starts at vK > 1, or a branch starts above its source — older commits have been pruned;
  report the answer as "changes within retained history (from vK)", not as everything
  since the cutoff. If the baseline is retained, the start wasn't truncated: the table (or
  branch) simply didn't exist yet at the cutoff, and the answer starts from creation or
  from the fork (for pre-fork changes, continue in the parent chain). A retained baseline
  alone does not make the audit complete, though — pruning can hollow out the middle, so
  also confirm the versions from the baseline through the window are **contiguous**. Any
  numbering gap is pruned history (per the version model above), and the answer stays
  incomplete no matter what the endpoints show.
- Timestamp precision and timezone **vary by surface**. Lance manifests store nanoseconds,
  but Python's `list_versions()` returns a **naive local-time** datetime at microsecond
  precision, and TypeScript returns a JS `Date` (milliseconds). REST returns an RFC 3339
  `timestamp` (may include fractional seconds) and/or integer `timestamp_millis`. Normalize
  both operands to UTC or integer epoch units before comparing — a naive local datetime
  compared against a `Z` cutoff is off by the machine's UTC offset — and keep whatever
  sub-second precision the surface provides. When timestamps tie, order by version number,
  never by timestamp.
- Keep the categories separate when classifying: **schema** (non-empty column arrays),
  **index** (`CreateIndex`), **data** (`Append`, `Delete`, `Update` with empty arrays),
  **creation/replacement** (`Overwrite` — the table created, or its data fully replaced;
  when its column arrays are non-empty it is schema movement too, reported separately),
  **metadata** (`UpdateConfig`), **maintenance/rollback** (`Rewrite`, `Restore`, clone and
  reservation internals), and **unknown** for names not in the table above. Everything inside
  the window is part of the answer — report each version as what it is instead of folding
  them all into a schema-change answer.

### Find the version that introduced or dropped a column

Scan the history for the version whose `added_columns` (or `removed_columns`) names it. Note
that **version 1 lists every original column as added**, since it is diffed against an empty
schema — so a base column's "added" version is 1, and only later additions are interesting.

If retained history doesn't start at version 1 and no retained version names the column, the
change predates what you can see — on a branch, continue the search in the parent chain below
the branch's source version; otherwise say "introduced at or before vK (earliest retained)"
rather than claiming a version. The earliest retained version's own diff is against a missing
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

1. **The version's own `metadata`** (in the version listing) — but only under a producer
   guarantee. The field is contractually arbitrary key-value pairs: anyone can stamp
   anything, so a value that merely *looks like* a job id or job name proves nothing — a
   user-set label can coincide with an unrelated job. It is attribution only when the key
   is one a specific writer documents stamping on its own commits and that guarantee is
   verified for this deployment; a look-alike value under any other key is level-3
   correlation, not a link.
2. **A documented output link on the job**: an explicit committed-version reference in the
   job's `status`/output (job-type-specific — look for the field, don't assume it). Geneva's
   `manifest_id` is **opaque** — there is no documented mapping between it and a version's
   `manifest_path` or `e_tag`, so it is not such a link; Geneva jobs stay at level 3.
3. **Time-window matching** — the commit's timestamp falls between the job's start and
   completion, on the same table, with an operation type consistent with the job. Report
   this as "consistent with job X", never as "caused by job X".

Route by writer: UDF column backfills and materialized-view refreshes — the usual authors of
`Merge`/`Update` commits — are **Geneva jobs in their own `geneva_jobs` registry**, not
`/v1/jobs`. Check both registries before concluding no job was involved. Both are covered in
`references/remote_jobs.md`.

## Gotchas

- `include_operations`, `descending`, `limit`, and `branch` are **query params**. Putting them
  in the JSON body silently gets you the bare response — indistinguishable from a server that
  doesn't support the extension. Rule this mistake out before concluding the server can't do it.
- `include_operations=true` reads a manifest per version, which is why it is opt-in. Pair it
  with `limit` on tables with long histories.
- Without `descending=true` the order is implementation-defined — sort by `version` yourself.
- `restore()` (and `POST /v1/table/{table}/restore`) rolls the table back by committing a new
  version. It is a **write** — never reach for it during a read-only investigation.
- On local tables, `optimize()` with no arguments is also a pruner (seven-day default). If an
  audit and maintenance are both on the agenda, do the audit first.
