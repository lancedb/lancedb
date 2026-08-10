Lance publishes metrics through the [`metrics`](https://docs.rs/metrics) crate
facade. Install any recorder (Prometheus, OpenTelemetry, etc.) in your
application and Lance will emit into it; when no recorder is installed, emission
is a cheap no-op. Metrics are only emitted when Lance is built with the
`metrics` feature.

## Object store metrics

These track I/O against the underlying object store. The `base` label
identifies the store; its cardinality is controlled by the
`LANCE_OBJECT_STORE_METRICS_LABEL` environment variable:

- `scheme` (default) — the scheme only (`s3`, `gs`, `az`, `file`, `memory`);
  low, bounded cardinality.
- `full` — the store's unique prefix (`s3$my-bucket`, `az$container@account`
  where Azure's account also matters), so multiple buckets on the same cloud
  can be told apart. Cardinality grows with the number of stores accessed.
- `off` — omit the `base` label entirely.

`operation` is one of `get`, `put`, `put_part`, `head`, `list`, `delete`,
`copy`, `rename`, `complete_multipart`, or `abort_multipart`.

Request counts are per logical operation: a `list` or `delete` that spans many
objects is one request, matching how backends batch them.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lance_object_store_requests_total` | counter | `operation`, `base` | Object store requests issued. |
| `lance_object_store_request_bytes_total` | counter | `operation`, `base` | Bytes transferred by `get`/`put` requests. A `get` is counted once its response body has been fully read. |
| `lance_object_store_request_duration_seconds` | histogram | `operation`, `base` | Per-request latency, in seconds. For `get` this covers the full body transfer, not just time-to-first-byte. |
| `lance_object_store_errors_total` | counter | `operation`, `base` | Requests that returned an error. |
| `lance_object_store_in_flight_requests` | gauge | `operation`, `base` | Requests currently in flight. |
| `lance_object_store_throttle_total` | counter | `status`, `base` | Throttle responses (HTTP 429 / 503) seen at the HTTP layer, counted per attempt including retries. The `status` label is the numeric HTTP status. |
| `lance_object_store_retryable_responses_total` | counter | `status`, `base` | Retryable responses (HTTP 5xx / 429 / 408) seen at the HTTP layer, counted per attempt including retries. A superset of `throttle_total`; 409 (conflict) is excluded so commit conflicts are not counted. |

`lance_object_store_throttle_total` and
`lance_object_store_retryable_responses_total` are recorded only for the native
cloud stores (S3, GCS, Azure); Opendal-backed stores bypass the HTTP client
where the counters are installed, so they report the other object store metrics
but not throttle/retryable counts.
