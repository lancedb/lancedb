// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Publishes object store metrics via the [`metrics`] crate.
//!
//! Two layers cooperate:
//!
//! * [`MeteredObjectStore`] wraps any [`object_store::ObjectStore`] and records
//!   per-operation request counts, transferred bytes, latency, errors, and the
//!   number of requests currently in flight. It works for every store
//!   regardless of backend.
//! * [`MeteringHttpConnector`] wraps the HTTP client used by the native cloud
//!   stores (S3 / GCS / Azure) and records throttle / retryable responses per
//!   attempt. Because `object_store`'s retry loop re-issues each request
//!   through the [`HttpService`](object_store::client::HttpService), this sees
//!   every retried response, which a store-level wrapper cannot observe.
//!
//! The two layers have different coverage: every store gets the request-level
//! metrics from [`MeteredObjectStore`], but only the native cloud stores get
//! the HTTP-level throttle metrics. Opendal-backed stores (tos, oss, etc.)
//! bypass `object_store`'s HTTP client, so there is no place to install the
//! connector for them.
//!
//! Neither layer sees the optimized local reads and writes ([`LocalObjectReader`],
//! [`LocalWriter`], the io_uring readers, and the local `copy` / recursive delete
//! shortcuts), which go straight to the filesystem. Those publish the same
//! request-level metrics themselves through
//! [`IOTracker::begin_io`](crate::utils::tracking_store::IOTracker::begin_io).
//! The two are installed together, so a store either publishes for all of its
//! IO or for none of it. A store built by calling a provider's `new_store`
//! directly, bypassing both `ObjectStore` constructors — as
//! [`ObjectStore::local`](crate::object_store::ObjectStore::local) and
//! [`ObjectStore::memory`](crate::object_store::ObjectStore::memory) do — is in
//! the "none of it" case.
//!
//! [`LocalObjectReader`]: crate::local::LocalObjectReader
//! [`LocalWriter`]: crate::object_writer::LocalWriter
//!
//! Metrics carry a `base` label identifying the store. Its cardinality is
//! controlled by the `LANCE_OBJECT_STORE_METRICS_LABEL` environment variable
//! ([`BASE_LABEL_ENV_VAR`]):
//!
//! * `scheme` (default) — scheme only, e.g. `s3`; low, bounded cardinality.
//! * `full` — the full store prefix, e.g. `s3$bucket` or `az$container@account`,
//!   so multiple buckets on the same cloud can be told apart.
//! * `off` — omit the `base` label entirely.
//!
//! The metric name constants ([`METRIC_REQUESTS`] etc.) and the recording
//! helpers ([`record_request`], [`record_count`], [`record_error`],
//! [`InFlightGuard`]) are public so custom object stores can emit the same
//! metrics.

use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
    UploadPart,
};

/// Total number of object store requests, labelled by `operation` and `base`.
pub const METRIC_REQUESTS: &str = "lance_object_store_requests_total";
/// Total bytes transferred by object store requests, labelled by `operation` and `base`.
pub const METRIC_BYTES: &str = "lance_object_store_request_bytes_total";
/// Object store request latency in seconds, labelled by `operation` and `base`.
pub const METRIC_DURATION: &str = "lance_object_store_request_duration_seconds";
/// Total number of failed object store requests, labelled by `operation` and `base`.
pub const METRIC_ERRORS: &str = "lance_object_store_errors_total";
/// Total number of throttle responses (HTTP 429 / 503) seen at the HTTP layer,
/// labelled by `status` and `base`. Counts every attempt, including retries.
pub const METRIC_THROTTLE: &str = "lance_object_store_throttle_total";
/// Total number of retryable responses (HTTP 5xx / 429 / 408) seen at the HTTP
/// layer, labelled by `status` and `base`. Counts every attempt, including
/// retries. This is a superset of [`METRIC_THROTTLE`]; 409 (conflict) is
/// deliberately excluded so commit conflicts are not counted as retries.
pub const METRIC_RETRYABLE: &str = "lance_object_store_retryable_responses_total";
/// Number of object store requests currently in flight, labelled by `operation`
/// and `base`.
pub const METRIC_IN_FLIGHT: &str = "lance_object_store_in_flight_requests";

/// Environment variable controlling the cardinality of the `base` label.
pub const BASE_LABEL_ENV_VAR: &str = "LANCE_OBJECT_STORE_METRICS_LABEL";

/// Controls how much of a store's identity the `base` label carries, traded off
/// against metric cardinality. Selected via [`BASE_LABEL_ENV_VAR`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseLabelMode {
    /// Full store prefix, e.g. `s3$bucket` or `az$container@account`. Highest
    /// cardinality: one series family per bucket/container.
    Full,
    /// Scheme only, e.g. `s3`. The default: low, bounded cardinality.
    Scheme,
    /// Omit the `base` label entirely.
    Off,
}

fn parse_base_label_mode(value: Option<&str>) -> BaseLabelMode {
    match value {
        Some("full") => BaseLabelMode::Full,
        Some("off") | Some("none") => BaseLabelMode::Off,
        Some("scheme") | None => BaseLabelMode::Scheme,
        Some(other) => {
            tracing::warn!(
                "Unrecognized {BASE_LABEL_ENV_VAR}={other:?}; \
                 expected one of full, scheme, off. Defaulting to scheme."
            );
            BaseLabelMode::Scheme
        }
    }
}

/// The label mode is read once from the environment and cached for the process.
fn base_label_mode() -> BaseLabelMode {
    static MODE: OnceLock<BaseLabelMode> = OnceLock::new();
    *MODE.get_or_init(|| parse_base_label_mode(std::env::var(BASE_LABEL_ENV_VAR).ok().as_deref()))
}

/// Reduce a full store prefix (`scheme$authority`, or just `scheme` for stores
/// without buckets) to the configured `base` label value, or `None` when the
/// label should be omitted.
fn scoped_base(mode: BaseLabelMode, base: &str) -> Option<String> {
    match mode {
        BaseLabelMode::Full => Some(base.to_owned()),
        BaseLabelMode::Scheme => Some(base.split('$').next().unwrap_or(base).to_owned()),
        BaseLabelMode::Off => None,
    }
}

/// Build the `operation` (+ optional `base`) label set shared by all
/// store-level metrics, honoring the configured label mode.
fn operation_labels(base: &str, operation: &'static str) -> Vec<metrics::Label> {
    let mut labels = vec![metrics::Label::new("operation", operation)];
    if let Some(base) = scoped_base(base_label_mode(), base) {
        labels.push(metrics::Label::new("base", base));
    }
    labels
}

/// Recommended histogram bucket boundaries for [`METRIC_DURATION`], in seconds.
///
/// Object store requests can take anywhere from a few milliseconds to the
/// client timeout (commonly ~120s), so the boundaries are dense below 10s and
/// keep useful resolution through the timeout band out to 5 minutes. Exporters
/// that aggregate into fixed buckets (e.g. the OpenTelemetry bridge in the
/// Python bindings) use these.
pub const REQUEST_DURATION_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, // sub-10s
    10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0, 150.0, 180.0, 240.0, 300.0, // 10s–5min
];

/// Register descriptions (units and help text) for the object store metrics.
///
/// This routes through whatever [`metrics::Recorder`] is currently installed,
/// so it must be called *after* the recorder is set. Exporters that build a
/// catalog of available metrics (such as the OpenTelemetry bridge) rely on
/// these descriptions to discover metric names, kinds, and units up front.
pub fn describe_metrics() {
    metrics::describe_counter!(
        METRIC_REQUESTS,
        metrics::Unit::Count,
        "Total number of object store requests, by operation and scheme."
    );
    metrics::describe_counter!(
        METRIC_BYTES,
        metrics::Unit::Bytes,
        "Total bytes transferred by object store requests, by operation and scheme."
    );
    metrics::describe_histogram!(
        METRIC_DURATION,
        metrics::Unit::Seconds,
        "Object store request latency in seconds, by operation and scheme."
    );
    metrics::describe_counter!(
        METRIC_ERRORS,
        metrics::Unit::Count,
        "Total number of failed object store requests, by operation and scheme."
    );
    metrics::describe_counter!(
        METRIC_THROTTLE,
        metrics::Unit::Count,
        "Total number of throttle responses (HTTP 429 / 503) seen at the HTTP layer, by status and scheme."
    );
    metrics::describe_counter!(
        METRIC_RETRYABLE,
        metrics::Unit::Count,
        "Total number of retryable responses (HTTP 5xx / 429 / 408) seen at the HTTP layer, by status and scheme."
    );
    metrics::describe_gauge!(
        METRIC_IN_FLIGHT,
        metrics::Unit::Count,
        "Number of object store requests currently in flight, by operation and scheme."
    );
}

/// Recommended fixed bucket boundaries for the histogram metrics defined here,
/// as `(metric_name, boundaries)` pairs. Exporters that aggregate histograms
/// into fixed buckets read this to configure each histogram.
pub fn histogram_bounds() -> &'static [(&'static str, &'static [f64])] {
    &[(METRIC_DURATION, REQUEST_DURATION_BOUNDS)]
}

/// Record the outcome of a unary request: count, latency, bytes (on success), and errors.
pub fn record_request<T>(
    base: &str,
    operation: &'static str,
    start: Instant,
    bytes: u64,
    result: &OSResult<T>,
) {
    record_outcome(base, operation, start, bytes, result.is_err());
}

/// Record count, latency, and either transferred bytes or an error for a
/// completed request. Used both for unary requests and for streamed GETs whose
/// bytes are only known once the body finishes.
pub fn record_outcome(
    base: &str,
    operation: &'static str,
    start: Instant,
    bytes: u64,
    is_error: bool,
) {
    let elapsed = start.elapsed().as_secs_f64();
    let labels = operation_labels(base, operation);
    metrics::counter!(METRIC_REQUESTS, labels.clone()).increment(1);
    metrics::histogram!(METRIC_DURATION, labels.clone()).record(elapsed);
    if is_error {
        metrics::counter!(METRIC_ERRORS, labels).increment(1);
    } else if bytes > 0 {
        metrics::counter!(METRIC_BYTES, labels).increment(bytes);
    }
}

/// Record a single request count without latency, used for streaming operations
/// (list / delete) whose work happens lazily as the stream is polled.
pub fn record_count(base: &str, operation: &'static str) {
    metrics::counter!(METRIC_REQUESTS, operation_labels(base, operation)).increment(1);
}

/// Record a single error for an operation.
pub fn record_error(base: &str, operation: &'static str) {
    metrics::counter!(METRIC_ERRORS, operation_labels(base, operation)).increment(1);
}

/// Raises the in-flight gauge for an operation on creation and lowers it on
/// drop, so the count stays balanced even if the request future or stream is
/// cancelled or dropped before completing.
pub struct InFlightGuard {
    labels: Vec<metrics::Label>,
}

impl InFlightGuard {
    pub fn new(base: &str, operation: &'static str) -> Self {
        let labels = operation_labels(base, operation);
        metrics::gauge!(METRIC_IN_FLIGHT, labels.clone()).increment(1.0);
        Self { labels }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        metrics::gauge!(METRIC_IN_FLIGHT, self.labels.clone()).decrement(1.0);
    }
}

#[derive(Debug)]
pub struct MeteredObjectStore {
    target: Arc<dyn object_store::ObjectStore>,
    base: String,
}

impl std::fmt::Display for MeteredObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MeteredObjectStore({})", self.target)
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl object_store::ObjectStore for MeteredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        let size = bytes.content_length() as u64;
        let _in_flight = InFlightGuard::new(&self.base, "put");
        let start = Instant::now();
        let result = self.target.put_opts(location, bytes, opts).await;
        record_request(&self.base, "put", start, size, &result);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        let upload = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(MeteredMultipartUpload {
            target: upload,
            base: self.base.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        // `head()` is implemented as a `get_opts` call with `head = true`, so we
        // distinguish it here to keep HEAD and GET as separate operations.
        let is_head = options.head;
        let operation = if is_head { "head" } else { "get" };
        let in_flight = InFlightGuard::new(&self.base, operation);
        let start = Instant::now();
        let result = self.target.get_opts(location, options).await;

        // A HEAD transfers only metadata, and errors carry no payload, so both
        // are recorded immediately. `get_opts` only resolves once the response
        // headers arrive; the body is streamed afterwards, so for a successful
        // GET we defer recording until the body has been drained (see below).
        if is_head || result.is_err() {
            record_request(&self.base, operation, start, 0, &result);
            return result;
        }

        let result = result.expect("checked to be Ok above");
        Ok(meter_get_result(
            result,
            self.base.clone(),
            start,
            in_flight,
        ))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        let _in_flight = InFlightGuard::new(&self.base, "get");
        let start = Instant::now();
        let result = self.target.get_ranges(location, ranges).await;
        let bytes = match &result {
            Ok(parts) => parts.iter().map(|b| b.len() as u64).sum(),
            Err(_) => 0,
        };
        record_request(&self.base, "get", start, bytes, &result);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        let base = self.base.clone();
        // Count one logical delete request per call, matching `list`: a single
        // `delete_stream` maps to one batched request on stores that support it
        // (e.g. S3's `DeleteObjects`), so counting per yielded path would
        // over-count. Errors are still recorded per failing path.
        record_count(&self.base, "delete");
        let in_flight = InFlightGuard::new(&self.base, "delete");
        self.target
            .delete_stream(locations)
            .map(move |result| {
                // Reference `in_flight` so this `move` closure captures (owns)
                // the guard, keeping the gauge raised until the stream is
                // dropped (a move closure only captures the variables it uses).
                let _in_flight = &in_flight;
                if result.is_err() {
                    record_error(&base, "delete");
                }
                result
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        record_count(&self.base, "list");
        meter_list_stream(
            self.target.list(prefix),
            self.base.clone(),
            InFlightGuard::new(&self.base, "list"),
        )
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        record_count(&self.base, "list");
        meter_list_stream(
            self.target.list_with_offset(prefix, offset),
            self.base.clone(),
            InFlightGuard::new(&self.base, "list"),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        let _in_flight = InFlightGuard::new(&self.base, "list");
        let start = Instant::now();
        let result = self.target.list_with_delimiter(prefix).await;
        record_request(&self.base, "list", start, 0, &result);
        result
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
        let _in_flight = InFlightGuard::new(&self.base, "copy");
        let start = Instant::now();
        let result = self.target.copy_opts(from, to, opts).await;
        record_request(&self.base, "copy", start, 0, &result);
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OSResult<()> {
        let _in_flight = InFlightGuard::new(&self.base, "rename");
        let start = Instant::now();
        let result = self.target.rename_opts(from, to, opts).await;
        record_request(&self.base, "rename", start, 0, &result);
        result
    }
}

/// Count errors yielded while draining a list stream. The request itself is
/// counted once when the stream is created (a single LIST may return many items).
fn meter_list_stream(
    stream: BoxStream<'static, OSResult<ObjectMeta>>,
    base: String,
    in_flight: InFlightGuard,
) -> BoxStream<'static, OSResult<ObjectMeta>> {
    stream
        .map(move |result| {
            // Reference `in_flight` so this `move` closure captures (owns) the
            // guard: a move closure only captures the variables it uses, and
            // holding it here keeps the gauge raised until the stream is dropped.
            let _in_flight = &in_flight;
            if result.is_err() {
                record_error(&base, "list");
            }
            result
        })
        .boxed()
}

/// Wrap a successful GET so the request is recorded once its body has been
/// fully read, capturing the true transfer duration and byte count rather than
/// the time-to-first-byte and declared range. For payloads without a body
/// stream (e.g. a local file handle) the request is recorded immediately.
fn meter_get_result(
    mut result: GetResult,
    base: String,
    start: Instant,
    in_flight: InFlightGuard,
) -> GetResult {
    match result.payload {
        GetResultPayload::Stream(stream) => {
            result.payload = GetResultPayload::Stream(
                MeteredGetStream {
                    inner: stream,
                    base,
                    start,
                    bytes: 0,
                    errored: false,
                    recorded: false,
                    _in_flight: in_flight,
                }
                .boxed(),
            );
            result
        }
        // No body stream to observe (e.g. a local file), so record now.
        other => {
            let bytes = result.range.end - result.range.start;
            record_outcome(&base, "get", start, bytes, false);
            result.payload = other;
            result
        }
    }
}

/// Stream wrapper over a GET body that records the request (count, duration,
/// bytes, errors) once the body is fully drained or the stream is dropped.
struct MeteredGetStream {
    inner: BoxStream<'static, OSResult<Bytes>>,
    base: String,
    start: Instant,
    bytes: u64,
    errored: bool,
    recorded: bool,
    _in_flight: InFlightGuard,
}

impl MeteredGetStream {
    fn record(&mut self) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        record_outcome(&self.base, "get", self.start, self.bytes, self.errored);
    }
}

impl Stream for MeteredGetStream {
    type Item = OSResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.bytes += chunk.len() as u64;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.errored = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.record();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for MeteredGetStream {
    fn drop(&mut self) {
        // Records the partial transfer if the body was dropped before it drained.
        self.record();
    }
}

#[derive(Debug)]
struct MeteredMultipartUpload {
    target: Box<dyn MultipartUpload>,
    base: String,
}

#[async_trait::async_trait]
impl MultipartUpload for MeteredMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Each part upload is a distinct request, recorded under the `put_part`
        // operation with the same count / bytes / latency / error set as a
        // unary put.
        let base = self.base.clone();
        let size = data.content_length() as u64;
        let inner = self.target.put_part(data);
        async move {
            let _in_flight = InFlightGuard::new(&base, "put_part");
            let start = Instant::now();
            let result = inner.await;
            record_request(&base, "put_part", start, size, &result);
            result
        }
        .boxed()
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        // Completing a multipart upload issues its own request that can throttle
        // or fail, so it is metered like any other operation.
        let _in_flight = InFlightGuard::new(&self.base, "complete_multipart");
        let start = Instant::now();
        let result = self.target.complete().await;
        record_request(&self.base, "complete_multipart", start, 0, &result);
        result
    }

    async fn abort(&mut self) -> OSResult<()> {
        let _in_flight = InFlightGuard::new(&self.base, "abort_multipart");
        let start = Instant::now();
        let result = self.target.abort().await;
        record_request(&self.base, "abort_multipart", start, 0, &result);
        result
    }
}

pub trait ObjectStoreMetricsExt {
    /// Wrap this store so its operations publish metrics under the given `base` label.
    fn metered(self, base: String) -> Arc<dyn object_store::ObjectStore>;
}

impl ObjectStoreMetricsExt for Arc<dyn object_store::ObjectStore> {
    fn metered(self, base: String) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(MeteredObjectStore { target: self, base })
    }
}

// --- Layer 2: HTTP-level throttle metrics for native cloud stores ---

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
mod http {
    use super::*;
    use object_store::client::{
        ClientOptions, HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse,
        HttpService, ReqwestConnector,
    };

    /// An [`HttpConnector`] that records throttle and retryable responses
    /// observed by the underlying HTTP client. Install it on the S3 / GCS /
    /// Azure builders via `with_http_connector`.
    #[derive(Debug)]
    pub struct MeteringHttpConnector {
        base: String,
        inner: ReqwestConnector,
    }

    impl MeteringHttpConnector {
        pub fn new(base: String) -> Self {
            Self {
                base,
                inner: ReqwestConnector::default(),
            }
        }
    }

    impl HttpConnector for MeteringHttpConnector {
        fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
            let client = self.inner.connect(options)?;
            Ok(HttpClient::new(MeteringHttpService {
                base: self.base.clone(),
                inner: client,
            }))
        }
    }

    #[derive(Debug)]
    struct MeteringHttpService {
        base: String,
        inner: HttpClient,
    }

    #[async_trait::async_trait]
    impl HttpService for MeteringHttpService {
        async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let response = self.inner.execute(req).await?;
            let status = response.status().as_u16();
            // Each attempt that object_store may retry is recorded with its
            // numeric status. Throttles (429 / 503) are a distinct, narrower
            // signal than the broader set of retryable responses, so they get
            // their own counter. 409 (conflict) is intentionally excluded from
            // the retryable set so commit conflicts are not counted as retries.
            let is_throttle = status == 429 || status == 503;
            let is_retryable = status == 429 || status == 408 || (500..600).contains(&status);
            if is_throttle {
                metrics::counter!(METRIC_THROTTLE, status_labels(&self.base, status)).increment(1);
            }
            if is_retryable {
                metrics::counter!(METRIC_RETRYABLE, status_labels(&self.base, status)).increment(1);
            }
            Ok(response)
        }
    }

    /// Build the `status` (+ optional `base`) label set for HTTP-layer metrics,
    /// honoring the configured label mode.
    fn status_labels(base: &str, status: u16) -> Vec<metrics::Label> {
        let mut labels = vec![metrics::Label::new("status", status.to_string())];
        if let Some(base) = scoped_base(base_label_mode(), base) {
            labels.push(metrics::Label::new("base", base));
        }
        labels
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};
        use object_store::client::{HttpRequestBody, HttpResponseBody};

        /// A mock [`HttpService`] that always responds with a fixed status code.
        #[derive(Debug)]
        struct StaticStatusService {
            status: u16,
        }

        #[async_trait::async_trait]
        impl HttpService for StaticStatusService {
            async fn call(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
                Ok(::http::Response::builder()
                    .status(self.status)
                    .body(HttpResponseBody::from(Bytes::new()))
                    .unwrap())
            }
        }

        fn request() -> HttpRequest {
            ::http::Request::builder()
                .method("GET")
                .uri("http://example.com/obj")
                .body(HttpRequestBody::empty())
                .unwrap()
        }

        fn metric_count(
            metrics: &[(metrics::Key, DebugValue)],
            name: &str,
            base: &str,
            status: &str,
        ) -> u64 {
            for (key, value) in metrics {
                if key.name() != name {
                    continue;
                }
                let labels: std::collections::HashMap<&str, &str> =
                    key.labels().map(|l| (l.key(), l.value())).collect();
                if labels.get("base") == Some(&base)
                    && labels.get("status") == Some(&status)
                    && let DebugValue::Counter(v) = value
                {
                    return *v;
                }
            }
            0
        }

        #[test]
        fn test_throttle_and_retryable_responses_counted_by_status() {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    // Each attempt that object_store retries flows through `call`
                    // again; here we simulate that by issuing several responses.
                    // The base is baked into the connector, so it labels the
                    // metric. Bases here have no `$`, so they are unaffected by
                    // the label mode and this test isolates status handling.
                    for (base, status) in [
                        ("s3", 429u16),
                        ("s3", 503),
                        ("s3", 503),
                        ("s3", 500),
                        ("s3", 408),
                        ("s3", 409),
                        ("s3", 200),
                        ("s3", 404),
                        ("gs", 429),
                    ] {
                        let service = MeteringHttpService {
                            base: base.into(),
                            inner: HttpClient::new(StaticStatusService { status }),
                        };
                        service.call(request()).await.unwrap();
                    }
                });
            });

            let recorded: Vec<_> = snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .map(|(ck, _unit, _desc, value)| (ck.key().clone(), value))
                .collect();

            let throttle = |base, status| metric_count(&recorded, METRIC_THROTTLE, base, status);
            let retryable = |base, status| metric_count(&recorded, METRIC_RETRYABLE, base, status);

            // Throttles are only 429 and 503.
            assert_eq!(throttle("s3", "429"), 1);
            assert_eq!(throttle("s3", "503"), 2);
            assert_eq!(throttle("s3", "500"), 0);
            assert_eq!(throttle("s3", "408"), 0);

            // Retryable is the broader set: 5xx, 429, 408 (but not 409).
            assert_eq!(retryable("s3", "429"), 1);
            assert_eq!(retryable("s3", "503"), 2);
            assert_eq!(retryable("s3", "500"), 1);
            assert_eq!(retryable("s3", "408"), 1);
            // 409 conflict is excluded so commit conflicts are not counted as retries.
            assert_eq!(retryable("s3", "409"), 0);

            // Success and non-retryable client errors count as neither.
            assert_eq!(throttle("s3", "200"), 0);
            assert_eq!(retryable("s3", "404"), 0);

            // The base label is taken from the connector, not shared across stores.
            assert_eq!(throttle("gs", "429"), 1);
            assert_eq!(retryable("gs", "429"), 1);
            assert_eq!(throttle("gs", "503"), 0);
        }
    }
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
pub use http::MeteringHttpConnector;

#[cfg(test)]
mod tests {
    use super::*;

    use lance_core::utils::tempfile::TempStdDir;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use tokio::io::AsyncWriteExt;
    use url::Url;

    use crate::object_store::ObjectStore as LanceObjectStore;
    use crate::traits::Writer;

    fn payload(data: &[u8]) -> PutPayload {
        PutPayload::from_bytes(Bytes::copy_from_slice(data))
    }

    fn metered_store() -> Arc<dyn object_store::ObjectStore> {
        (Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>).metered("memory".into())
    }

    /// A single materialized snapshot of recorded metrics. It must be taken
    /// only once: the snapshotter *drains* histogram samples on every
    /// `snapshot()` call, so a second snapshot would see empty histograms.
    type Metrics = Vec<(metrics::Key, DebugValue)>;

    /// Materialize the current recorder state. Histogram samples are *drained*
    /// on each call, so a metric must be read from a single snapshot.
    fn snapshot(snapshotter: &Snapshotter) -> Metrics {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(ck, _unit, _desc, value)| (ck.key().clone(), value))
            .collect()
    }

    /// Run an async closure with a thread-local metrics recorder installed and
    /// return the resulting metrics. Uses a current-thread runtime so all polls
    /// happen on the thread that holds the recorder guard.
    fn capture_metrics<F, Fut>(f: F) -> Metrics
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(f());
        });
        snapshot(&snapshotter)
    }

    fn key_matches(key: &metrics::Key, name: &str, labels: &[(&str, &str)]) -> bool {
        if key.name() != name {
            return false;
        }
        let actual: std::collections::HashSet<(&str, &str)> =
            key.labels().map(|l| (l.key(), l.value())).collect();
        labels.len() == actual.len() && labels.iter().all(|l| actual.contains(l))
    }

    fn counter_value(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> u64 {
        for (key, value) in metrics {
            if key_matches(key, name, labels)
                && let DebugValue::Counter(v) = value
            {
                return *v;
            }
        }
        0
    }

    fn histogram_count(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> usize {
        for (key, value) in metrics {
            if key_matches(key, name, labels)
                && let DebugValue::Histogram(samples) = value
            {
                return samples.len();
            }
        }
        0
    }

    fn gauge_value(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> f64 {
        for (key, value) in metrics {
            if key_matches(key, name, labels)
                && let DebugValue::Gauge(v) = value
            {
                return v.0;
            }
        }
        0.0
    }

    fn has_metric(metrics: &Metrics, name: &str, labels: &[(&str, &str)]) -> bool {
        metrics
            .iter()
            .any(|(key, _)| key_matches(key, name, labels))
    }

    #[test]
    fn test_parse_base_label_mode() {
        assert_eq!(parse_base_label_mode(None), BaseLabelMode::Scheme);
        assert_eq!(parse_base_label_mode(Some("scheme")), BaseLabelMode::Scheme);
        assert_eq!(parse_base_label_mode(Some("full")), BaseLabelMode::Full);
        assert_eq!(parse_base_label_mode(Some("off")), BaseLabelMode::Off);
        assert_eq!(parse_base_label_mode(Some("none")), BaseLabelMode::Off);
        // Unrecognized values fall back to the conservative default.
        assert_eq!(parse_base_label_mode(Some("bogus")), BaseLabelMode::Scheme);
    }

    #[test]
    fn test_scoped_base() {
        assert_eq!(
            scoped_base(BaseLabelMode::Full, "s3$bucket").as_deref(),
            Some("s3$bucket")
        );
        assert_eq!(
            scoped_base(BaseLabelMode::Scheme, "s3$bucket").as_deref(),
            Some("s3")
        );
        // Azure keeps only the scheme even though its prefix carries the account.
        assert_eq!(
            scoped_base(BaseLabelMode::Scheme, "az$container@account").as_deref(),
            Some("az")
        );
        // A prefix without `$` (e.g. memory/file) is unchanged by scheme mode.
        assert_eq!(
            scoped_base(BaseLabelMode::Scheme, "memory").as_deref(),
            Some("memory")
        );
        assert_eq!(scoped_base(BaseLabelMode::Off, "s3$bucket"), None);
    }

    #[test]
    fn test_base_label_defaults_to_scheme() {
        // No env var is set in the test process, so the default `scheme` mode
        // applies: the full prefix collapses to just the scheme.
        let recorded = capture_metrics(|| async {
            let store = (Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>)
                .metered("s3$my-bucket".into());
            store.put(&Path::from("a"), payload(b"x")).await.unwrap();
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "put"), ("base", "s3")]
            ),
            1
        );
        // The full prefix is not emitted as the label under the default mode.
        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "put"), ("base", "s3$my-bucket")]
            ),
            0
        );
    }

    #[test]
    fn test_put_records_count_bytes_and_latency() {
        let data = b"hello world";
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            store
                .put(&Path::from("a/b.bin"), payload(data))
                .await
                .unwrap();
        });

        let labels = [("operation", "put"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(
            counter_value(&recorded, METRIC_BYTES, &labels),
            data.len() as u64
        );
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
    }

    #[test]
    fn test_get_records_count_and_bytes() {
        let data = b"hello world";
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let path = Path::from("a/b.bin");
            store.put(&path, payload(data)).await.unwrap();
            // The GET is only recorded once its body has been fully drained.
            store.get(&path).await.unwrap().bytes().await.unwrap();
        });

        let labels = [("operation", "get"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(
            counter_value(&recorded, METRIC_BYTES, &labels),
            data.len() as u64
        );
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
    }

    #[test]
    fn test_get_not_recorded_until_body_drained() {
        let data = b"hello world";
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "get"), ("base", "memory")];
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let store = metered_store();
                let path = Path::from("a/b.bin");
                store.put(&path, payload(data)).await.unwrap();

                // Holding the result without reading the body records nothing yet.
                let result = store.get(&path).await.unwrap();
                assert_eq!(
                    counter_value(&snapshot(&snapshotter), METRIC_REQUESTS, &labels),
                    0
                );

                // Draining the body records the request with the true byte count.
                let bytes = result.bytes().await.unwrap();
                assert_eq!(bytes.len(), data.len());
                let recorded = snapshot(&snapshotter);
                assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
                assert_eq!(
                    counter_value(&recorded, METRIC_BYTES, &labels),
                    data.len() as u64
                );
            });
        });
    }

    #[test]
    fn test_head_is_a_separate_operation() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let path = Path::from("a/b.bin");
            store.put(&path, payload(b"hello world")).await.unwrap();
            store.head(&path).await.unwrap();
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "head"), ("base", "memory")]
            ),
            1
        );
        // The head call must not be counted as a get.
        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "get"), ("base", "memory")]
            ),
            0
        );
        // A HEAD transfers only metadata, so it records no payload bytes.
        assert_eq!(
            counter_value(
                &recorded,
                METRIC_BYTES,
                &[("operation", "head"), ("base", "memory")]
            ),
            0
        );
    }

    #[test]
    fn test_delete_records_one_request_per_call() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            for i in 0..3 {
                store
                    .put(&Path::from(format!("a/{i}.bin")), payload(b"x"))
                    .await
                    .unwrap();
            }
            // `delete` drives `delete_stream`; deleting three paths is still one
            // logical delete request (a single batched request on real stores).
            let paths =
                futures::stream::iter((0..3).map(|i| Ok(Path::from(format!("a/{i}.bin"))))).boxed();
            let _: Vec<_> = store.delete_stream(paths).collect().await;
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "delete"), ("base", "memory")]
            ),
            1
        );
    }

    #[test]
    fn test_list_counts_one_request_not_per_item() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            for i in 0..3 {
                store
                    .put(&Path::from(format!("a/{i}.bin")), payload(b"x"))
                    .await
                    .unwrap();
            }
            let _: Vec<_> = store.list(Some(&Path::from("a"))).collect().await;
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "list"), ("base", "memory")]
            ),
            1
        );
    }

    #[test]
    fn test_error_is_counted() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            // Getting a missing object errors.
            let _ = store.get(&Path::from("does/not/exist")).await;
        });

        let labels = [("operation", "get"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &labels), 1);
        // A failed request is still counted as a request, with latency recorded.
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
        // No bytes are transferred on a failed get.
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
    }

    #[test]
    fn test_get_ranges_sums_part_bytes_and_labels_get() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let path = Path::from("a/b.bin");
            store.put(&path, payload(b"hello world")).await.unwrap();
            // Two disjoint ranges of 3 bytes each.
            store.get_ranges(&path, &[2..5, 6..9]).await.unwrap();
        });

        let labels = [("operation", "get"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 6);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
    }

    #[test]
    fn test_copy_and_rename_record_zero_bytes() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            store
                .put(&Path::from("a/src"), payload(b"x"))
                .await
                .unwrap();
            store
                .copy(&Path::from("a/src"), &Path::from("a/copy"))
                .await
                .unwrap();
            store
                .rename(&Path::from("a/copy"), &Path::from("a/moved"))
                .await
                .unwrap();
        });

        for operation in ["copy", "rename"] {
            let labels = [("operation", operation), ("base", "memory")];
            assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
            assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
            assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
        }
    }

    #[test]
    fn test_list_with_delimiter_records_latency() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            store.put(&Path::from("a/b"), payload(b"x")).await.unwrap();
            store
                .list_with_delimiter(Some(&Path::from("a")))
                .await
                .unwrap();
        });

        let labels = [("operation", "list"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
    }

    #[test]
    fn test_list_with_offset_counts_one_request() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            for i in 0..3 {
                store
                    .put(&Path::from(format!("a/{i}")), payload(b"x"))
                    .await
                    .unwrap();
            }
            let _: Vec<_> = store
                .list_with_offset(Some(&Path::from("a")), &Path::from("a/0"))
                .collect()
                .await;
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "list"), ("base", "memory")]
            ),
            1
        );
    }

    #[test]
    fn test_multipart_records_each_part_and_complete() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let mut upload = store.put_multipart(&Path::from("a/big")).await.unwrap();
            upload.put_part(payload(b"hello")).await.unwrap(); // 5 bytes
            upload.put_part(payload(b"world!!")).await.unwrap(); // 7 bytes
            upload.complete().await.unwrap();
        });

        let part_labels = [("operation", "put_part"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &part_labels), 2);
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &part_labels), 12);
        // Each part records its own latency sample, like a unary put.
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &part_labels), 2);
        // A successful part upload records no error.
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &part_labels), 0);

        // Completing the upload is its own metered request.
        let complete_labels = [("operation", "complete_multipart"), ("base", "memory")];
        assert_eq!(
            counter_value(&recorded, METRIC_REQUESTS, &complete_labels),
            1
        );
        assert_eq!(
            histogram_count(&recorded, METRIC_DURATION, &complete_labels),
            1
        );
    }

    #[test]
    fn test_multipart_abort_is_recorded() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let mut upload = store.put_multipart(&Path::from("a/big")).await.unwrap();
            upload.put_part(payload(b"hello")).await.unwrap();
            upload.abort().await.unwrap();
        });

        let labels = [("operation", "abort_multipart"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
    }

    #[test]
    fn test_multipart_part_error_is_counted() {
        let recorded = capture_metrics(|| async {
            let store = (Arc::new(FailingStreamStore) as Arc<dyn object_store::ObjectStore>)
                .metered("memory".into());
            let mut upload = store.put_multipart(&Path::from("a/big")).await.unwrap();
            let _ = upload.put_part(payload(b"data")).await;
        });

        let labels = [("operation", "put_part"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &labels), 1);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
        // A failed part transfers no counted bytes.
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
    }

    #[test]
    fn test_in_flight_guard_tracks_and_releases() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "get"), ("base", "memory")];
        metrics::with_local_recorder(&recorder, || {
            let g1 = InFlightGuard::new("memory", "get");
            let g2 = InFlightGuard::new("memory", "get");
            assert_eq!(
                gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                2.0
            );
            drop(g1);
            assert_eq!(
                gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                1.0
            );
            drop(g2);
            assert_eq!(
                gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                0.0
            );
        });
    }

    #[test]
    fn test_in_flight_gauge_is_wired_and_balances() {
        let recorded = capture_metrics(|| async {
            let store = metered_store();
            let path = Path::from("a/b.bin");
            store.put(&path, payload(b"hello")).await.unwrap();
            store.get(&path).await.unwrap();
        });

        // The gauge is emitted for each operation (guard is wired in) and, once
        // the operation completes, balances back to zero.
        for operation in ["put", "get"] {
            let labels = [("operation", operation), ("base", "memory")];
            assert!(has_metric(&recorded, METRIC_IN_FLIGHT, &labels));
            assert_eq!(gauge_value(&recorded, METRIC_IN_FLIGHT, &labels), 0.0);
        }
    }

    #[test]
    fn test_list_stream_holds_in_flight_until_dropped() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "list"), ("base", "memory")];
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let store = metered_store();
                store.put(&Path::from("a/x"), payload(b"x")).await.unwrap();

                // Creating the stream raises the gauge; it stays raised until the
                // stream is dropped, even before any items are drained.
                let stream = store.list(Some(&Path::from("a")));
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    1.0
                );
                drop(stream);
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    0.0
                );
            });
        });
    }

    #[test]
    fn test_delete_stream_holds_in_flight_until_dropped() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "delete"), ("base", "memory")];
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let store = metered_store();
                let locations = futures::stream::iter(vec![Ok(Path::from("a/b"))]).boxed();

                // Like list, creating the delete stream raises the gauge and holds
                // it until the stream is dropped, before any items are drained.
                let stream = store.delete_stream(locations);
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    1.0
                );
                drop(stream);
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    0.0
                );
            });
        });
    }

    #[test]
    fn test_in_flight_released_when_operation_future_dropped() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "get"), ("base", "memory")];
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let started = Arc::new(tokio::sync::Notify::new());
                // Never signalled: the request stays blocked mid-flight.
                let release = Arc::new(tokio::sync::Notify::new());
                let store = (Arc::new(BlockingStore {
                    started: started.clone(),
                    release,
                }) as Arc<dyn object_store::ObjectStore>)
                    .metered("memory".into());

                let path = Path::from("a/b");
                let mut fut = Box::pin(store.get(&path));
                // Drive the request until it is blocked inside the inner store.
                tokio::select! {
                    _ = &mut fut => unreachable!("the blocking store never returns"),
                    _ = started.notified() => {}
                }

                // The gauge is raised while the request is outstanding, and
                // dropping the future before it completes releases it.
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    1.0
                );
                drop(fut);
                assert_eq!(
                    gauge_value(&snapshot(&snapshotter), METRIC_IN_FLIGHT, &labels),
                    0.0
                );
            });
        });
    }

    #[test]
    fn test_streaming_errors_are_counted() {
        let recorded = capture_metrics(|| async {
            let delete_store = (Arc::new(FailingStreamStore) as Arc<dyn object_store::ObjectStore>)
                .metered("memory".into());
            let _ = delete_store.delete(&Path::from("a/b")).await;

            let list_store = (Arc::new(FailingStreamStore) as Arc<dyn object_store::ObjectStore>)
                .metered("memory".into());
            let _: Vec<_> = list_store.list(None).collect().await;
        });

        // delete_stream counts the item and records an error when it fails.
        let delete_labels = [("operation", "delete"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &delete_labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &delete_labels), 1);

        // A list request is counted once; a failure while draining records an error.
        let list_labels = [("operation", "list"), ("base", "memory")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &list_labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &list_labels), 1);
    }

    /// The optimized local reads and writes talk to the filesystem directly, so
    /// they never reach [`MeteredObjectStore`] and publish these metrics
    /// themselves. They must land under the same `base` label as the store's
    /// metered operations, which for a local store is its scheme.
    #[test]
    fn test_local_filesystem_io_is_metered() {
        let tmp = TempStdDir::default();
        let dir = tmp.join("sub");
        let data = b"hello world";
        let recorded = capture_metrics(|| async {
            // Built through the registry, like any store opened from a URI.
            let (store, path) = LanceObjectStore::from_uri(dir.join("a.bin").to_str().unwrap())
                .await
                .unwrap();
            // Writes go through LocalWriter.
            store.put(&path, data).await.unwrap();

            // Reads go through LocalObjectReader.
            let reader = store.open(&path).await.unwrap();
            assert_eq!(reader.size().await.unwrap(), data.len());
            assert_eq!(reader.get_range(0..5).await.unwrap().len(), 5);
            assert_eq!(reader.get_all().await.unwrap().len(), data.len());
            // The file is smaller than the block size, so it streams as one chunk.
            let chunks: Vec<_> = reader.get_stream().await.unwrap().collect().await;
            assert_eq!(chunks.len(), 1);

            // Copy and recursive delete both shortcut to the filesystem too.
            store
                .copy(&path, &Path::from_absolute_path(dir.join("b.bin")).unwrap())
                .await
                .unwrap();
            store
                .remove_dir_all(Path::from_absolute_path(&dir).unwrap())
                .await
                .unwrap();
        });

        let put_labels = [("operation", "put"), ("base", "file")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &put_labels), 1);
        assert_eq!(
            counter_value(&recorded, METRIC_BYTES, &put_labels),
            data.len() as u64
        );
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &put_labels), 1);
        assert_eq!(gauge_value(&recorded, METRIC_IN_FLIGHT, &put_labels), 0.0);

        // One request each for the range read, the full read and the single
        // streamed chunk.
        let get_labels = [("operation", "get"), ("base", "file")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &get_labels), 3);
        assert_eq!(
            counter_value(&recorded, METRIC_BYTES, &get_labels),
            (5 + 2 * data.len()) as u64
        );
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &get_labels), 3);
        assert_eq!(gauge_value(&recorded, METRIC_IN_FLIGHT, &get_labels), 0.0);

        // The size lookup is the local equivalent of a HEAD, and transfers no
        // payload bytes.
        let head_labels = [("operation", "head"), ("base", "file")];
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &head_labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &head_labels), 0);

        for operation in ["copy", "delete"] {
            let labels = [("operation", operation), ("base", "file")];
            assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
            assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
        }

        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &get_labels), 0);
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &put_labels), 0);
    }

    #[test]
    fn test_local_read_error_is_counted() {
        let tmp = TempStdDir::default();
        let recorded = capture_metrics(|| async {
            let (store, path) = LanceObjectStore::from_uri(tmp.join("a.bin").to_str().unwrap())
                .await
                .unwrap();
            store.put(&path, b"hello").await.unwrap();

            let reader = store.open(&path).await.unwrap();
            // Reading past the end of the file fails.
            assert!(reader.get_range(0..100).await.is_err());
        });

        let labels = [("operation", "get"), ("base", "file")];
        assert_eq!(counter_value(&recorded, METRIC_ERRORS, &labels), 1);
        // A failed read is still counted as a request, with latency recorded.
        assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
        assert_eq!(histogram_count(&recorded, METRIC_DURATION, &labels), 1);
        assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 0);
    }

    /// A local write is reported as a single `put` covering the whole file, so it
    /// stays in flight until the file is persisted under its final path.
    #[test]
    fn test_local_write_is_in_flight_until_persisted() {
        let tmp = TempStdDir::default();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let labels = [("operation", "put"), ("base", "file")];
        metrics::with_local_recorder(&recorder, || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(async {
                let (store, path) = LanceObjectStore::from_uri(tmp.join("a.bin").to_str().unwrap())
                    .await
                    .unwrap();
                let mut writer = store.create(&path).await.unwrap();
                writer.write_all(b"hello").await.unwrap();

                let recorded = snapshot(&snapshotter);
                assert_eq!(gauge_value(&recorded, METRIC_IN_FLIGHT, &labels), 1.0);
                assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 0);

                Writer::shutdown(writer.as_mut()).await.unwrap();
                let recorded = snapshot(&snapshotter);
                assert_eq!(gauge_value(&recorded, METRIC_IN_FLIGHT, &labels), 0.0);
                assert_eq!(counter_value(&recorded, METRIC_REQUESTS, &labels), 1);
                assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), 5);
            });
        });
    }

    /// A store handed in by the caller is metered like one built by the registry.
    #[test]
    fn test_caller_supplied_store_is_metered() {
        let recorded = capture_metrics(|| async {
            #[allow(deprecated)]
            let params = crate::object_store::ObjectStoreParams {
                object_store: Some((
                    Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>,
                    Url::parse("memory:///").unwrap(),
                )),
                ..Default::default()
            };
            let (store, _) = LanceObjectStore::from_uri_and_params(
                Arc::new(crate::object_store::ObjectStoreRegistry::default()),
                "memory:///",
                &params,
            )
            .await
            .unwrap();
            store.put(&Path::from("a"), b"hello").await.unwrap();
        });

        assert_eq!(
            counter_value(
                &recorded,
                METRIC_REQUESTS,
                &[("operation", "put"), ("base", "memory")]
            ),
            1
        );
    }

    /// `ObjectStore::new` is how `DatasetBuilder` wraps a caller-supplied store,
    /// so it must meter both halves of the store: the operations that go through
    /// `inner`, and the local ones that bypass it. Metering only one half would
    /// report a partial picture that reads like a complete one.
    #[test]
    fn test_store_built_from_new_is_metered() {
        let tmp = TempStdDir::default();
        let recorded = capture_metrics(|| async {
            let store = LanceObjectStore::new(
                Arc::new(object_store::local::LocalFileSystem::new()),
                Url::parse("file:///").unwrap(),
                None,
                None,
                false,
                false,
                1,
                3,
                None,
            );
            let path = Path::from_absolute_path(tmp.join("a.bin")).unwrap();
            // put and open bypass `inner` and publish for themselves.
            store.put(&path, b"hello").await.unwrap();
            let reader = store.open(&path).await.unwrap();
            assert_eq!(reader.get_all().await.unwrap().len(), 5);
            // delete goes through `inner`, so only MeteredObjectStore can count it.
            store.delete(&path).await.unwrap();
        });

        for (operation, bytes) in [("put", 5), ("get", 5), ("delete", 0)] {
            let labels = [("operation", operation), ("base", "file")];
            assert_eq!(
                counter_value(&recorded, METRIC_REQUESTS, &labels),
                1,
                "expected one {operation} request"
            );
            assert_eq!(counter_value(&recorded, METRIC_BYTES, &labels), bytes);
        }
    }

    /// A store whose stream-producing operations always yield an error, used to
    /// exercise the error branches of the streaming wrappers.
    #[derive(Debug)]
    struct FailingStreamStore;

    impl std::fmt::Display for FailingStreamStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStreamStore")
        }
    }

    fn test_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "FailingStreamStore",
            source: "injected failure".into(),
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailingStreamStore {
        async fn put_opts(
            &self,
            _location: &Path,
            _bytes: PutPayload,
            _opts: PutOptions,
        ) -> OSResult<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            Ok(Box::new(FailingUpload))
        }

        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> OSResult<GetResult> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            _locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            futures::stream::once(async { Err(test_error()) }).boxed()
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            futures::stream::once(async { Err(test_error()) }).boxed()
        }

        fn list_with_offset(
            &self,
            _prefix: Option<&Path>,
            _offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OSResult<ListResult> {
            unimplemented!()
        }

        async fn copy_opts(&self, _from: &Path, _to: &Path, _opts: CopyOptions) -> OSResult<()> {
            unimplemented!()
        }

        async fn rename_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _opts: RenameOptions,
        ) -> OSResult<()> {
            unimplemented!()
        }
    }

    /// A [`MultipartUpload`] whose part uploads always fail, used to exercise the
    /// error branch of the metered `put_part`.
    #[derive(Debug)]
    struct FailingUpload;

    #[async_trait::async_trait]
    impl MultipartUpload for FailingUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            async { Err(test_error()) }.boxed()
        }

        async fn complete(&mut self) -> OSResult<PutResult> {
            unimplemented!()
        }

        async fn abort(&mut self) -> OSResult<()> {
            unimplemented!()
        }
    }

    /// A store whose `get_opts` blocks after signalling `started`, so a request
    /// can be observed mid-flight and then dropped before it completes.
    #[derive(Debug)]
    struct BlockingStore {
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl std::fmt::Display for BlockingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "BlockingStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for BlockingStore {
        async fn put_opts(
            &self,
            _location: &Path,
            _bytes: PutPayload,
            _opts: PutOptions,
        ) -> OSResult<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> OSResult<GetResult> {
            self.started.notify_one();
            self.release.notified().await;
            unreachable!("release is never signalled in the test")
        }

        fn delete_stream(
            &self,
            _locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            unimplemented!()
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            unimplemented!()
        }

        fn list_with_offset(
            &self,
            _prefix: Option<&Path>,
            _offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OSResult<ListResult> {
            unimplemented!()
        }

        async fn copy_opts(&self, _from: &Path, _to: &Path, _opts: CopyOptions) -> OSResult<()> {
            unimplemented!()
        }

        async fn rename_opts(
            &self,
            _from: &Path,
            _to: &Path,
            _opts: RenameOptions,
        ) -> OSResult<()> {
            unimplemented!()
        }
    }
}
