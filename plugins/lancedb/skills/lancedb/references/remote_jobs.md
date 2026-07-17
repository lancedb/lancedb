# Job operations over the LanceDB remote server REST API

Jobs are server-side background operations on LanceDB Enterprise/Cloud — index builds,
column backfills, materialized view refreshes, and similar async work. Endpoints that
trigger async work (e.g. the column backfill or materialized view refresh endpoints)
return a `job_id`; these four methods are how you track and manage those jobs.

Resolve the connection first — see `references/remote_connect.md`. All four methods
are **POST** requests under `{base_url}/v1/jobs/` with JSON bodies, and take the usual
`x-api-key` / `x-lancedb-database` headers. If every job call returns `501`, job APIs
are disabled on that deployment (the server has no job registry configured) — report
that rather than retrying.

## 1. List jobs — `POST /v1/jobs/list`

The body is optional; an empty body lists everything. All fields are filters:

```json
{
  "limit": 100,
  "table_name": "my_table",
  "job_type": "...",
  "job_subtype": "...",
  "state": "...",
  "page_token": "..."
}
```

```bash
curl -s -X POST "{base_url}/v1/jobs/list" \
  -H "x-api-key: <key>" -H "x-lancedb-database: <database>" \
  -H "content-type: application/json" \
  -d '{"table_name": "my_table"}'
```

Response:

```json
{
  "jobs": [
    {
      "job_id": "...",
      "table": "my_table",
      "job_type": "...",
      "job_subtype": "...",
      "state": "done",
      "created_at_millis": 1720000000000
    }
  ],
  "page_token": "..."
}
```

A `page_token` in the response means there are more results — pass it back in the next
request to continue. Note list rows use a lowercase `state` string, while describe uses
an uppercase `job_state`.

## 2. Describe a job — `POST /v1/jobs/describe`

Body: `{"job_id": "<id>"}`. Returns full detail for one job:

```json
{
  "job_id": "...",
  "job_type": "...",
  "job_subtype": "...",
  "job_state": "IN_PROGRESS",
  "creation_ms": 1720000000000,
  "spec": {},
  "status": {}
}
```

`job_state` is one of `IN_PROGRESS`, `CANCELLED`, `FAILED`, `DONE`. `spec` and `status`
are job-type-specific JSON objects (the job's input specification and its current
progress/status). Returns `404` for an unknown job id.

## 3. Cancel a job — `POST /v1/jobs/cancel`

Body: `{"job_id": "<id>"}`; response echoes `{"job_id": "<id>"}`. Cancellation is a
service-level operation requiring the same administrative authorization as the
`/admin` routes — a database-scoped API key that can list and describe jobs may still
get a permission error here. Other errors: `404` unknown job, `409` state conflict
(e.g. already in a terminal state), `429` too much write contention (safe to retry).

## 4. Query job event history — `POST /v1/jobs/query_events`

Returns the event history (state transitions, progress updates) for one or more jobs.
Body: `{"job_id": "<id>"}` for one job, or `{"job_ids": ["<id>", ...]}` for a batch.
Optional fields: `limit` (max event rows), `limit_per_job` (per job in a batch query),
and `filter` — a SQL-like expression over the columns `state`, `updated_by`,
`owner_component`, and `claim_entity`. (`full_text_search` is reserved and currently
rejected as not implemented.)

The response is **not JSON** — it is an Arrow IPC stream
(`content-type: application/vnd.apache.arrow.stream`). Decode it, e.g. in Python:

```python
import pyarrow.ipc
import requests

resp = requests.post(
    f"{base_url}/v1/jobs/query_events",
    headers={"x-api-key": key, "x-lancedb-database": database},
    json={"job_id": job_id},
)
resp.raise_for_status()
events = pyarrow.ipc.open_stream(resp.content).read_all()
```

## Feature engineering (Geneva) jobs

Feature engineering jobs — UDF column backfills and materialized view refreshes run
through Geneva — are tracked **separately** from the `/v1/jobs` registry above. Their
records live in a `geneva_jobs` table inside the database itself (in the `__system`
namespace), and you access them through a Python `geneva` connection rather than the
REST endpoints above:

```python
import geneva
from geneva.jobs import JobStateManager

# Same credentials as lancedb.connect / the REST API
conn = geneva.connect("db://<database>", api_key="<key>", host_override="<base_url>")
jsm = JobStateManager(conn)

# List jobs. NOTE: status defaults to "RUNNING"; pass status=None for all jobs.
# Statuses: PENDING | RUNNING | DONE | FAILED | CANCELLED
jobs = jsm.list_jobs(table_name="my_table", status=None)

# Fetch one job by id (returns a list of JobRecord)
records = jsm.get("<job_id>")
```

Each `JobRecord` has `table_name`, `column_name`, `job_id`, `job_type`, `status`,
`launched_at`, `completed_at`, `config`, `launched_by`, `manifest_id`, `cluster_name`,
`metrics` (progress counters), `events` (human-readable history), and `updated_at`.
For filters `list_jobs` doesn't support (e.g. time ranges), query the underlying table
directly: `jsm.get_table(True).search().where("launched_at >= TIMESTAMP '...'")` —
pass `True` to check out the latest version, since other processes update job state.

Stale-status caveat: nothing reaps dead Geneva jobs, so a job can sit in
`RUNNING`/`PENDING` forever if its worker died. Treat a job as effectively `FAILED`
when it has been running longer than ~36 hours, or its `updated_at` is more than ~2
hours old (this matches the heuristic the Geneva console UI applies on read).

## Workflow tips

- To wait for async work (a backfill, an index build), poll `describe` until
  `job_state` leaves `IN_PROGRESS`; on `FAILED`, pull `status` and `query_events` for
  the failure detail.
