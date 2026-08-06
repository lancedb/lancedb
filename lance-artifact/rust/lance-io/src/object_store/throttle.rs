// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! AIMD-controlled token bucket rate limiter for ObjectStore operations.
//!
//! Wraps any [`object_store::ObjectStore`] with per-category token buckets
//! whose fill rates are dynamically adjusted by AIMD controllers. When cloud
//! stores return HTTP 429/503, the fill rate decreases multiplicatively. During
//! sustained success windows, it increases additively.
//!
//! Operations are split into four independent categories — **read**, **write**,
//! **delete**, **list** — each with its own AIMD controller and token bucket.
//! This prevents a burst of reads from starving writes, and vice versa.
//!
//! # Example
//!
//! ```ignore
//! use lance_io::object_store::throttle::{AimdThrottleConfig, AimdThrottledStore};
//!
//! let throttled = AimdThrottledStore::new(target, AimdThrottleConfig::default()).unwrap();
//! ```

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance_core::utils::aimd::{AimdConfig, AimdController, RequestOutcome};
use lance_core::utils::tracing::TRACE_OBJECT_STORE_THROTTLE;
#[cfg(test)]
use object_store::ObjectStoreExt;
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use object_store::client::{
    ClientOptions, HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
    HttpResponseBody, HttpService,
};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
    UploadPart,
};
use rand::Rng;
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Check whether an `object_store::Error` represents a throttle response
/// (HTTP 429 / 503) from a cloud object store.
///
/// Regrettably, this information is not fully exposed by the `object_store` crate.
/// There is no generic mechanism for a custom object store to return a throttle error.
///
/// However, the builtin object stores all use RetryError when retries are configured and
/// throttle errors are returned.  Sadly, RetryError is not a public type, so we have to
/// infer it from the error message.  This is potentially dangerous because these errors
/// often include the URI itself and that URI could have any characters in it (e.g. if we
/// look for 429 then we might match a 429 in a UUID).These error messages currently look like:
///
/// ", after ... retries, max_retries: ..., retry_timeout: ..."
///
/// So, as a crude heuristic, which should work for the builtin object stores, but won't
/// work for custom object stores, we simply look for the string "retries, max_retries"
/// in the error message.
pub fn is_throttle_error(err: &object_store::Error) -> bool {
    // Only Generic errors can carry throttle responses
    if let object_store::Error::Generic { source, .. } = err {
        let message = source.to_string();
        let lowercase = message.to_ascii_lowercase();
        lowercase.contains("retries, max_retries")
            || lowercase.contains("serverbusy")
            || lowercase.contains("server busy")
            || lowercase.contains("egress is over the account limit")
            || lowercase.contains("http 429")
            || lowercase.contains("status code: 429")
            || lowercase.contains("429 too many requests")
            || lowercase.contains("too many requests")
            || lowercase.contains("slowdown")
            || lowercase.contains("please reduce your request rate")
            || lowercase.contains("rate limit")
            || lowercase.contains("throttling")
            || lowercase.contains("throttled")
    } else {
        false
    }
}

/// Configuration for the AIMD-throttled ObjectStore wrapper.
///
/// Each operation category (read, write, delete, list) has its own AIMD config.
/// Use [`with_aimd`](AimdThrottleConfig::with_aimd) to set all categories at
/// once, or per-category methods like [`with_read_aimd`](AimdThrottleConfig::with_read_aimd)
/// for fine-grained control.
#[derive(Debug, Clone)]
pub struct AimdThrottleConfig {
    /// AIMD configuration for read operations (get, get_opts, get_range, get_ranges, head).
    pub read: AimdConfig,
    /// AIMD configuration for write operations (put, put_opts, put_multipart, copy, rename, etc.).
    pub write: AimdConfig,
    /// AIMD configuration for delete operations.
    pub delete: AimdConfig,
    /// AIMD configuration for list operations.
    pub list: AimdConfig,
    /// Maximum tokens that can accumulate for bursts (shared across all categories).
    pub burst_capacity: u32,
    /// Maximum number of retries for throttle errors within the AIMD layer.
    pub max_retries: usize,
    /// Minimum backoff in milliseconds between retry attempts.
    pub min_backoff_ms: u64,
    /// Maximum backoff in milliseconds between retry attempts.
    pub max_backoff_ms: u64,
}

impl Default for AimdThrottleConfig {
    fn default() -> Self {
        let aimd = AimdConfig::default();
        Self {
            read: aimd.clone(),
            write: aimd.clone(),
            delete: aimd.clone(),
            list: aimd,
            burst_capacity: 100,
            max_retries: 3,
            min_backoff_ms: 100,
            max_backoff_ms: 300,
        }
    }
}

impl AimdThrottleConfig {
    /// Set the AIMD configuration for all four operation categories at once.
    pub fn with_aimd(self, aimd: AimdConfig) -> Self {
        Self {
            read: aimd.clone(),
            write: aimd.clone(),
            delete: aimd.clone(),
            list: aimd,
            ..self
        }
    }

    /// Set the AIMD configuration for read operations.
    pub fn with_read_aimd(self, aimd: AimdConfig) -> Self {
        Self { read: aimd, ..self }
    }

    /// Set the AIMD configuration for write operations.
    pub fn with_write_aimd(self, aimd: AimdConfig) -> Self {
        Self {
            write: aimd,
            ..self
        }
    }

    /// Set the AIMD configuration for delete operations.
    pub fn with_delete_aimd(self, aimd: AimdConfig) -> Self {
        Self {
            delete: aimd,
            ..self
        }
    }

    /// Set the AIMD configuration for list operations.
    pub fn with_list_aimd(self, aimd: AimdConfig) -> Self {
        Self { list: aimd, ..self }
    }

    /// Returns `true` when the AIMD throttle layer should be bypassed entirely.
    pub fn is_disabled(&self) -> bool {
        self.max_retries == 0
    }

    pub fn with_burst_capacity(self, burst_capacity: u32) -> Self {
        Self {
            burst_capacity,
            ..self
        }
    }

    /// Build an `AimdThrottleConfig` from storage options and environment variables.
    ///
    /// Storage options take precedence over environment variables, which take
    /// precedence over defaults. A single AIMD config is applied to all four
    /// operation categories (read/write/delete/list).
    ///
    /// | Setting              | Storage Option Key               | Env Var                          | Default |
    /// |----------------------|----------------------------------|----------------------------------|---------|
    /// | Initial rate         | `lance_aimd_initial_rate`        | `LANCE_AIMD_INITIAL_RATE`        | 2000    |
    /// | Min rate             | `lance_aimd_min_rate`            | `LANCE_AIMD_MIN_RATE`            | 1       |
    /// | Max rate             | `lance_aimd_max_rate`            | `LANCE_AIMD_MAX_RATE`            | 5000    |
    /// | Decrease factor      | `lance_aimd_decrease_factor`     | `LANCE_AIMD_DECREASE_FACTOR`     | 0.5     |
    /// | Additive increment   | `lance_aimd_additive_increment`  | `LANCE_AIMD_ADDITIVE_INCREMENT`  | 300     |
    /// | Burst capacity       | `lance_aimd_burst_capacity`      | `LANCE_AIMD_BURST_CAPACITY`      | 100     |
    /// | Max retries          | `lance_aimd_max_retries`         | `LANCE_AIMD_MAX_RETRIES`         | 3       |
    /// | Min backoff ms       | `lance_aimd_min_backoff_ms`      | `LANCE_AIMD_MIN_BACKOFF_MS`      | 100     |
    /// | Max backoff ms       | `lance_aimd_max_backoff_ms`      | `LANCE_AIMD_MAX_BACKOFF_MS`      | 300     |
    pub fn from_storage_options(
        storage_options: Option<&HashMap<String, String>>,
    ) -> lance_core::Result<Self> {
        fn resolve_f64(
            key: &str,
            storage_options: Option<&HashMap<String, String>>,
            default: f64,
        ) -> lance_core::Result<f64> {
            let env_key = key.to_ascii_uppercase();
            if let Some(val) = storage_options.and_then(|opts| opts.get(key)) {
                val.parse::<f64>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for storage option '{key}': '{val}'"
                    ))
                })
            } else if let Ok(val) = std::env::var(&env_key) {
                val.parse::<f64>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for env var '{env_key}': '{val}'"
                    ))
                })
            } else {
                Ok(default)
            }
        }

        fn resolve_u32(
            key: &str,
            storage_options: Option<&HashMap<String, String>>,
            default: u32,
        ) -> lance_core::Result<u32> {
            let env_key = key.to_ascii_uppercase();
            if let Some(val) = storage_options.and_then(|opts| opts.get(key)) {
                val.parse::<u32>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for storage option '{key}': '{val}'"
                    ))
                })
            } else if let Ok(val) = std::env::var(&env_key) {
                val.parse::<u32>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for env var '{env_key}': '{val}'"
                    ))
                })
            } else {
                Ok(default)
            }
        }

        fn resolve_usize(
            key: &str,
            storage_options: Option<&HashMap<String, String>>,
            default: usize,
        ) -> lance_core::Result<usize> {
            let env_key = key.to_ascii_uppercase();
            if let Some(val) = storage_options.and_then(|opts| opts.get(key)) {
                val.parse::<usize>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for storage option '{key}': '{val}'"
                    ))
                })
            } else if let Ok(val) = std::env::var(&env_key) {
                val.parse::<usize>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for env var '{env_key}': '{val}'"
                    ))
                })
            } else {
                Ok(default)
            }
        }

        fn resolve_u64(
            key: &str,
            storage_options: Option<&HashMap<String, String>>,
            default: u64,
        ) -> lance_core::Result<u64> {
            let env_key = key.to_ascii_uppercase();
            if let Some(val) = storage_options.and_then(|opts| opts.get(key)) {
                val.parse::<u64>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for storage option '{key}': '{val}'"
                    ))
                })
            } else if let Ok(val) = std::env::var(&env_key) {
                val.parse::<u64>().map_err(|_| {
                    lance_core::Error::invalid_input(format!(
                        "Invalid value for env var '{env_key}': '{val}'"
                    ))
                })
            } else {
                Ok(default)
            }
        }

        let initial_rate = resolve_f64("lance_aimd_initial_rate", storage_options, 2000.0)?;
        let min_rate = resolve_f64("lance_aimd_min_rate", storage_options, 1.0)?;
        let max_rate = resolve_f64("lance_aimd_max_rate", storage_options, 5000.0)?;
        let decrease_factor = resolve_f64("lance_aimd_decrease_factor", storage_options, 0.5)?;
        let additive_increment =
            resolve_f64("lance_aimd_additive_increment", storage_options, 300.0)?;
        let burst_capacity = resolve_u32("lance_aimd_burst_capacity", storage_options, 100)?;
        let max_retries = resolve_usize("lance_aimd_max_retries", storage_options, 3)?;
        let min_backoff_ms = resolve_u64("lance_aimd_min_backoff_ms", storage_options, 100)?;
        let max_backoff_ms = resolve_u64("lance_aimd_max_backoff_ms", storage_options, 300)?;

        let aimd = AimdConfig::default()
            .with_initial_rate(initial_rate)
            .with_min_rate(min_rate)
            .with_max_rate(max_rate)
            .with_decrease_factor(decrease_factor)
            .with_additive_increment(additive_increment);

        Ok(Self {
            max_retries,
            min_backoff_ms,
            max_backoff_ms,
            ..Self::default()
                .with_aimd(aimd)
                .with_burst_capacity(burst_capacity)
        })
    }
}

struct TokenBucketState {
    tokens: f64,
    last_refill: tokio::time::Instant,
    rate: f64,
}

/// Per-category throttle state: an AIMD controller paired with a token bucket.
struct OperationThrottle {
    controller: AimdController,
    bucket: Mutex<TokenBucketState>,
    burst_capacity: f64,
    max_retries: usize,
    min_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl OperationThrottle {
    fn new(
        aimd_config: AimdConfig,
        burst_capacity: f64,
        max_retries: usize,
        min_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> lance_core::Result<Self> {
        let initial_rate = aimd_config.initial_rate;
        let controller = AimdController::new(aimd_config)?;
        Ok(Self {
            controller,
            bucket: Mutex::new(TokenBucketState {
                tokens: burst_capacity,
                last_refill: tokio::time::Instant::now(),
                rate: initial_rate,
            }),
            burst_capacity,
            max_retries,
            min_backoff_ms,
            max_backoff_ms,
        })
    }

    /// Acquire a token from the bucket, sleeping if none are available.
    ///
    /// Each caller reserves a token immediately (allowing `tokens` to go
    /// negative) so that concurrent waiters queue behind each other instead
    /// of all waking at the same instant (thundering herd).
    async fn acquire_token(&self) {
        let sleep_duration = {
            let mut bucket = self.bucket.lock().await;
            let now = tokio::time::Instant::now();
            let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * bucket.rate).min(self.burst_capacity);
            bucket.last_refill = now;

            // Reserve a token (may go negative to queue behind other waiters)
            bucket.tokens -= 1.0;

            if bucket.tokens >= 0.0 {
                // Had a token available, no need to sleep
                return;
            }

            // Sleep proportional to our position in the queue
            std::time::Duration::from_secs_f64(-bucket.tokens / bucket.rate)
        };

        tokio::time::sleep(sleep_duration).await;
    }

    /// Update the bucket's fill rate from the controller.
    async fn update_bucket_rate(&self, new_rate: f64) {
        let mut bucket = self.bucket.lock().await;
        bucket.rate = new_rate;
    }

    /// Classify a result and feed it back to the AIMD controller without
    /// acquiring a token. Uses `try_lock` for the bucket update so that if the
    /// bucket lock is contended the rate update is deferred to the next
    /// `throttled()` call.
    fn observe_outcome<T>(&self, result: &OSResult<T>) {
        let outcome = match result {
            Ok(_) => RequestOutcome::Success,
            Err(err) if is_throttle_error(err) => {
                debug!(
                    target: TRACE_OBJECT_STORE_THROTTLE,
                    error = %err,
                    "Throttle error detected in stream"
                );
                RequestOutcome::Throttled
            }
            Err(_) => RequestOutcome::Success,
        };
        let error = result
            .as_ref()
            .err()
            .map(|error| error as &dyn std::fmt::Display);
        let new_rate = self.record_outcome(outcome, error);
        if let Ok(mut bucket) = self.bucket.try_lock() {
            bucket.rate = new_rate;
        }
    }

    fn record_outcome(
        &self,
        outcome: RequestOutcome,
        error: Option<&dyn std::fmt::Display>,
    ) -> f64 {
        let prev_rate = self.controller.current_rate();
        let new_rate = self.controller.record_outcome(outcome);
        if new_rate < prev_rate {
            if let Some(error) = error {
                warn!(
                    target: TRACE_OBJECT_STORE_THROTTLE,
                    previous_rate = format!("{prev_rate:.1}"),
                    new_rate = format!("{new_rate:.1}"),
                    error = %error,
                    "AIMD throttle: rate reduced due to throttle errors"
                );
            } else {
                warn!(
                    target: TRACE_OBJECT_STORE_THROTTLE,
                    previous_rate = format!("{prev_rate:.1}"),
                    new_rate = format!("{new_rate:.1}"),
                    "AIMD throttle: rate reduced due to throttle errors"
                );
            }
        }
        new_rate
    }

    /// Execute an operation with throttling: acquire token, run, classify result.
    /// On throttle errors, retries up to `max_retries` times with a random
    /// backoff between `min_backoff_ms` and `max_backoff_ms` between attempts.
    async fn throttled<T, F, Fut>(&self, f: F) -> OSResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = OSResult<T>>,
    {
        for attempt in 0..=self.max_retries {
            self.acquire_token().await;
            let result = f().await;
            let outcome = match &result {
                Ok(_) => RequestOutcome::Success,
                Err(err) if is_throttle_error(err) => {
                    debug!(
                        target: TRACE_OBJECT_STORE_THROTTLE,
                        error = %err,
                        "Throttle error detected"
                    );
                    RequestOutcome::Throttled
                }
                Err(_) => RequestOutcome::Success, // Non-throttle errors don't indicate capacity problems
            };
            let error = result
                .as_ref()
                .err()
                .map(|error| error as &dyn std::fmt::Display);
            let new_rate = self.record_outcome(outcome, error);
            self.update_bucket_rate(new_rate).await;

            match &result {
                Err(err) if is_throttle_error(err) && attempt < self.max_retries => {
                    let backoff_ms =
                        rand::rng().random_range(self.min_backoff_ms..=self.max_backoff_ms);
                    debug!(
                        target: TRACE_OBJECT_STORE_THROTTLE,
                        attempt = attempt + 1,
                        max_retries = self.max_retries,
                        backoff_ms,
                        error = %err,
                        "Retrying after throttle error"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                _ => return result,
            }
        }
        unreachable!()
    }
}

impl Debug for OperationThrottle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationThrottle")
            .field("controller", &self.controller)
            .field("burst_capacity", &self.burst_capacity)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct AimdThrottleState {
    read: Arc<OperationThrottle>,
    write: Arc<OperationThrottle>,
    delete: Arc<OperationThrottle>,
    list: Arc<OperationThrottle>,
}

impl AimdThrottleState {
    pub(crate) fn new(config: AimdThrottleConfig) -> lance_core::Result<Self> {
        let burst_capacity = config.burst_capacity as f64;
        let max_retries = config.max_retries;
        let min_backoff_ms = config.min_backoff_ms;
        let max_backoff_ms = config.max_backoff_ms;
        Ok(Self {
            read: Arc::new(OperationThrottle::new(
                config.read,
                burst_capacity,
                max_retries,
                min_backoff_ms,
                max_backoff_ms,
            )?),
            write: Arc::new(OperationThrottle::new(
                config.write,
                burst_capacity,
                max_retries,
                min_backoff_ms,
                max_backoff_ms,
            )?),
            delete: Arc::new(OperationThrottle::new(
                config.delete,
                burst_capacity,
                max_retries,
                min_backoff_ms,
                max_backoff_ms,
            )?),
            list: Arc::new(OperationThrottle::new(
                config.list,
                burst_capacity,
                max_retries,
                min_backoff_ms,
                max_backoff_ms,
            )?),
        })
    }
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
#[derive(Debug)]
pub(crate) struct AimdMultipartUploadConnector<C> {
    inner: C,
    write: Option<Arc<OperationThrottle>>,
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
impl<C> AimdMultipartUploadConnector<C> {
    fn new(inner: C, state: Option<&AimdThrottleState>) -> Self {
        Self {
            inner,
            write: state.map(|state| Arc::clone(&state.write)),
        }
    }
}

#[cfg(all(
    any(feature = "aws", feature = "azure", feature = "gcp"),
    feature = "metrics"
))]
pub(crate) fn cloud_http_connector(
    state: Option<&AimdThrottleState>,
    metrics_base: String,
) -> AimdMultipartUploadConnector<crate::object_store::metrics::MeteringHttpConnector> {
    AimdMultipartUploadConnector::new(
        crate::object_store::metrics::MeteringHttpConnector::new(metrics_base),
        state,
    )
}

#[cfg(all(
    any(feature = "aws", feature = "azure", feature = "gcp"),
    not(feature = "metrics")
))]
pub(crate) fn cloud_http_connector(
    state: Option<&AimdThrottleState>,
    _metrics_base: String,
) -> AimdMultipartUploadConnector<object_store::client::ReqwestConnector> {
    AimdMultipartUploadConnector::new(object_store::client::ReqwestConnector::default(), state)
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
impl<C: HttpConnector> HttpConnector for AimdMultipartUploadConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(AimdMultipartUploadService {
            inner: self.inner.connect(options)?,
            write: self.write.clone(),
        }))
    }
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
#[derive(Debug)]
struct AimdMultipartUploadService {
    inner: HttpClient,
    write: Option<Arc<OperationThrottle>>,
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
fn is_multipart_part_request(request: &HttpRequest) -> bool {
    if request.method() != ::http::Method::PUT {
        return false;
    }
    request.uri().query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes()).any(|(key, value)| {
            key.eq_ignore_ascii_case("partNumber")
                || (key.eq_ignore_ascii_case("comp") && value.eq_ignore_ascii_case("block"))
        })
    })
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
fn is_retryable_http_error(error: &HttpError) -> bool {
    matches!(
        error.kind(),
        HttpErrorKind::Connect
            | HttpErrorKind::Request
            | HttpErrorKind::Timeout
            | HttpErrorKind::Interrupted
    )
}

#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
#[async_trait]
impl HttpService for AimdMultipartUploadService {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let Some(write) = self.write.as_ref() else {
            return self.inner.execute(request).await;
        };
        if !is_multipart_part_request(&request) {
            return self.inner.execute(request).await;
        }

        for attempt in 0..=write.max_retries {
            write.acquire_token().await;
            let mut result = self.inner.execute(request.clone()).await;
            let mut is_retryable = result.as_ref().err().is_some_and(is_retryable_http_error);
            let mut is_throttle = false;
            let mut response_status = None;

            if let Ok(response) = result {
                let status = response.status();
                response_status = Some(status);
                is_retryable = status == ::http::StatusCode::REQUEST_TIMEOUT
                    || status == ::http::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error();
                is_throttle = status == ::http::StatusCode::TOO_MANY_REQUESTS
                    || status == ::http::StatusCode::SERVICE_UNAVAILABLE;

                let (parts, body) = response.into_parts();
                result = match body.bytes().await {
                    Ok(bytes) => {
                        let body = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
                        let is_throttle_body = body.contains("requesttimeout")
                            || body.contains("slowdown")
                            || body.contains("serverbusy")
                            || body.contains("throttl");
                        is_retryable |= is_throttle_body;
                        is_throttle |= is_throttle_body;
                        Ok(HttpResponse::from_parts(
                            parts,
                            HttpResponseBody::from(bytes),
                        ))
                    }
                    Err(error) => {
                        is_retryable = is_retryable_http_error(&error);
                        Err(error)
                    }
                };
            }

            let detail = response_status
                .filter(|status| !status.is_success())
                .map(|status| format!("HTTP status {status}"));
            let error = result
                .as_ref()
                .err()
                .map(|error| error as &dyn std::fmt::Display)
                .or_else(|| {
                    detail
                        .as_ref()
                        .map(|detail| detail as &dyn std::fmt::Display)
                });
            let outcome = if is_throttle {
                RequestOutcome::Throttled
            } else {
                RequestOutcome::Success
            };
            let new_rate = write.record_outcome(outcome, error);
            write.update_bucket_rate(new_rate).await;

            if is_retryable && attempt < write.max_retries {
                let backoff_ms =
                    rand::rng().random_range(write.min_backoff_ms..=write.max_backoff_ms);
                debug!(
                    target: TRACE_OBJECT_STORE_THROTTLE,
                    attempt = attempt + 1,
                    max_retries = write.max_retries,
                    backoff_ms,
                    "Retrying multipart upload part after retryable HTTP response"
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                continue;
            }
            return result;
        }
        unreachable!()
    }
}

/// A [`MultipartUpload`] wrapper that applies the write AIMD controller.
struct ThrottledMultipartUpload {
    target: Box<dyn MultipartUpload>,
    write: Arc<OperationThrottle>,
    parts_throttled_at_http: bool,
}

impl Debug for ThrottledMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThrottledMultipartUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for ThrottledMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Call put_part synchronously to preserve part ordering regardless
        // of which futures are awaited first.
        let fut = self.target.put_part(data);
        if self.parts_throttled_at_http {
            return fut;
        }
        let write = Arc::clone(&self.write);
        Box::pin(async move {
            write.acquire_token().await;
            let result = fut.await;
            write.observe_outcome(&result);
            result
        })
    }

    async fn complete(&mut self) -> OSResult<PutResult> {
        let target = &mut self.target;
        for attempt in 0..=self.write.max_retries {
            self.write.acquire_token().await;
            let result = target.complete().await;
            self.write.observe_outcome(&result);

            match &result {
                Err(err) if is_throttle_error(err) && attempt < self.write.max_retries => {
                    let backoff_ms = rand::rng()
                        .random_range(self.write.min_backoff_ms..=self.write.max_backoff_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                _ => return result,
            }
        }
        unreachable!()
    }

    async fn abort(&mut self) -> OSResult<()> {
        let target = &mut self.target;
        for attempt in 0..=self.write.max_retries {
            self.write.acquire_token().await;
            let result = target.abort().await;
            self.write.observe_outcome(&result);

            match &result {
                Err(err) if is_throttle_error(err) && attempt < self.write.max_retries => {
                    let backoff_ms = rand::rng()
                        .random_range(self.write.min_backoff_ms..=self.write.max_backoff_ms);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                _ => return result,
            }
        }
        unreachable!()
    }
}

/// An ObjectStore wrapper that rate-limits operations using per-category token
/// buckets whose fill rates are controlled by AIMD algorithms.
///
/// Operations are split into four independent categories:
/// - **read**: `get`, `get_opts`, `get_range`, `get_ranges`, `head`
/// - **write**: `put`, `put_opts`, `put_multipart`, `put_multipart_opts`, `copy`, `copy_if_not_exists`, `rename`, `rename_if_not_exists`
/// - **delete**: `delete`
/// - **list**: `list`, `list_with_offset`, `list_with_delimiter`
///
/// Streaming list operations acquire a token before starting the underlying list stream.
/// Streaming operations also observe each yielded item and feed the result back to the
/// AIMD controller so it can adjust the rate for other operations in the same category.
///
/// This is not perfect but probably as close as we can get without moving the throttle into
/// the object_store crate itself.
pub struct AimdThrottledStore {
    target: Arc<dyn ObjectStore>,
    read: Arc<OperationThrottle>,
    write: Arc<OperationThrottle>,
    delete: Arc<OperationThrottle>,
    list: Arc<OperationThrottle>,
    multipart_parts_throttled_at_http: bool,
}

impl Debug for AimdThrottledStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AimdThrottledStore")
            .field("target", &self.target)
            .field("read", &self.read)
            .field("write", &self.write)
            .field("delete", &self.delete)
            .field("list", &self.list)
            .field(
                "multipart_parts_throttled_at_http",
                &self.multipart_parts_throttled_at_http,
            )
            .finish()
    }
}

impl Display for AimdThrottledStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AimdThrottledStore({})", self.target)
    }
}

impl AimdThrottledStore {
    pub fn new(
        target: Arc<dyn ObjectStore>,
        config: AimdThrottleConfig,
    ) -> lance_core::Result<Self> {
        Ok(Self::new_with_state(
            target,
            AimdThrottleState::new(config)?,
            false,
        ))
    }

    pub(crate) fn new_with_state(
        target: Arc<dyn ObjectStore>,
        state: AimdThrottleState,
        multipart_parts_throttled_at_http: bool,
    ) -> Self {
        Self {
            target,
            read: state.read,
            write: state.write,
            delete: state.delete,
            list: state.list,
            multipart_parts_throttled_at_http,
        }
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for AimdThrottledStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.write
            .throttled(|| self.target.put_opts(location, bytes.clone(), opts.clone()))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn MultipartUpload>> {
        let target = self
            .write
            .throttled(|| self.target.put_multipart_opts(location, opts.clone()))
            .await?;
        Ok(Box::new(ThrottledMultipartUpload {
            target,
            write: Arc::clone(&self.write),
            parts_throttled_at_http: self.multipart_parts_throttled_at_http,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        self.read
            .throttled(|| self.target.get_opts(location, options.clone()))
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        self.read
            .throttled(|| self.target.get_ranges(location, ranges))
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        let delete = Arc::clone(&self.delete);
        self.target
            .delete_stream(locations)
            .map(move |item| {
                delete.observe_outcome(&item);
                item
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        let throttle = Arc::clone(&self.list);
        let throttle_for_start = Arc::clone(&throttle);
        let target = Arc::clone(&self.target);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            throttle_for_start.acquire_token().await;
            target.list(prefix.as_ref())
        })
        .flatten()
        .map(move |item| {
            throttle.observe_outcome(&item);
            item
        })
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        let throttle = Arc::clone(&self.list);
        let throttle_for_start = Arc::clone(&throttle);
        let target = Arc::clone(&self.target);
        let prefix = prefix.cloned();
        let offset = offset.clone();
        futures::stream::once(async move {
            throttle_for_start.acquire_token().await;
            target.list_with_offset(prefix.as_ref(), &offset)
        })
        .flatten()
        .map(move |item| {
            throttle.observe_outcome(&item);
            item
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.list
            .throttled(|| self.target.list_with_delimiter(prefix))
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
        self.write
            .throttled(|| self.target.copy_opts(from, to, opts.clone()))
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OSResult<()> {
        self.write
            .throttled(|| self.target.rename_opts(from, to, opts.clone()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rstest::rstest;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    const THROTTLE_ERROR_RESPONSE: &str = "request failed, after 3 retries, max_retries: 3, retry_timeout: 30s - Server returned non-2xx status code: 503: x-ms-request-id: azure-request-id";

    fn make_generic_error(msg: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: msg.into(),
        }
    }

    #[rstest]
    #[case::retry_error("Error after 10 retries, max_retries: 10, retry_timeout: 180s", true)]
    #[case::retries_in_message(
        "request failed, after 3 retries, max_retries: 5, retry_timeout: 60s",
        true
    )]
    #[case::not_found("Object not found", false)]
    #[case::permission_denied("Access denied", false)]
    #[case::timeout("Connection timed out", false)]
    #[case::http_429_without_retries("HTTP 429 Too Many Requests", true)]
    #[case::slowdown_without_retries("SlowDown: Please reduce your request rate", true)]
    #[case::azure_server_busy("Code: ServerBusy", true)]
    #[case::azure_egress_limit("Message: Egress is over the account limit", true)]
    fn test_is_throttle_error(#[case] msg: &str, #[case] expected: bool) {
        let err = make_generic_error(msg);
        assert_eq!(
            is_throttle_error(&err),
            expected,
            "is_throttle_error for '{}' should be {}",
            msg,
            expected
        );
    }

    #[test]
    fn test_non_generic_errors_are_not_throttle() {
        let err = object_store::Error::NotFound {
            path: "test".to_string(),
            source: "not found".into(),
        };
        assert!(!is_throttle_error(&err));
    }

    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    #[rstest]
    #[case::s3("https://bucket/object?partNumber=1&uploadId=id", true)]
    #[case::azure_block("https://account/object?comp=block&blockid=id", true)]
    #[case::azure_block_list("https://account/object?comp=blocklist", false)]
    #[case::ordinary_put("https://bucket/object", false)]
    fn test_is_multipart_part_request(#[case] uri: &str, #[case] expected: bool) {
        let request = ::http::Request::builder()
            .method(::http::Method::PUT)
            .uri(uri)
            .body(object_store::client::HttpRequestBody::empty())
            .unwrap();
        assert_eq!(is_multipart_part_request(&request), expected);
    }

    #[tokio::test]
    async fn test_basic_put_get_through_wrapper() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default();
        let throttled = AimdThrottledStore::new(store, config).unwrap();

        let path = Path::from("test/file.txt");
        let data = PutPayload::from_static(b"hello world");
        throttled.put(&path, data).await.unwrap();

        let result = throttled.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_rate_decreases_on_throttle() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default().with_aimd(
            AimdConfig::default()
                .with_initial_rate(100.0)
                .with_decrease_factor(0.5)
                .with_window_duration(std::time::Duration::from_millis(10)),
        );
        let throttled = AimdThrottledStore::new(store, config).unwrap();

        let initial_rate = throttled.read.controller.current_rate();
        assert_eq!(initial_rate, 100.0);

        // Simulate a throttle outcome directly
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Throttled);

        // Wait for window to expire and trigger evaluation
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Success);

        let new_rate = throttled.read.controller.current_rate();
        assert!(
            new_rate < initial_rate,
            "Rate should decrease after throttle: {} < {}",
            new_rate,
            initial_rate
        );
    }

    #[tokio::test]
    async fn test_rate_recovers_on_success() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default().with_aimd(
            AimdConfig::default()
                .with_initial_rate(100.0)
                .with_decrease_factor(0.5)
                .with_additive_increment(10.0)
                .with_window_duration(std::time::Duration::from_millis(10)),
        );
        let throttled = AimdThrottledStore::new(store, config).unwrap();

        // First decrease via throttle
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Throttled);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Success);
        let decreased_rate = throttled.read.controller.current_rate();
        assert_eq!(decreased_rate, 50.0);

        // Now recover via success
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Success);
        let recovered_rate = throttled.read.controller.current_rate();
        assert_eq!(recovered_rate, 60.0);
    }

    #[tokio::test]
    async fn test_as_dyn_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let throttled: Arc<dyn ObjectStore> =
            Arc::new(AimdThrottledStore::new(store, AimdThrottleConfig::default()).unwrap());

        let path = Path::from("test/data.bin");
        let data = PutPayload::from_static(b"test data");
        throttled.put(&path, data).await.unwrap();

        let result = throttled.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"test data");
    }

    #[tokio::test]
    async fn test_token_bucket_delays_when_exhausted() {
        let store = Arc::new(InMemory::new());
        // Very low rate and burst capacity to force waiting
        let config = AimdThrottleConfig::default()
            .with_burst_capacity(1)
            .with_aimd(AimdConfig::default().with_initial_rate(10.0));
        let throttled = Arc::new(AimdThrottledStore::new(store, config).unwrap());

        let path = Path::from("test/file.txt");
        let data = PutPayload::from_static(b"data");
        throttled.put(&path, data).await.unwrap();

        // After consuming the burst token, the next request should take ~100ms
        // (1 token / 10 tokens-per-sec). We verify it takes at least 50ms.
        let start = std::time::Instant::now();
        let data2 = PutPayload::from_static(b"data2");
        throttled.put(&path, data2).await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "Expected delay for token refill, but elapsed was {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_list_observes_outcomes() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default();
        let throttled = AimdThrottledStore::new(store.clone(), config).unwrap();

        let path = Path::from("prefix/file.txt");
        let data = PutPayload::from_static(b"data");
        store.put(&path, data).await.unwrap();

        let items: Vec<_> = throttled.list(Some(&Path::from("prefix"))).collect().await;
        assert_eq!(items.len(), 1);
        assert!(items[0].is_ok());
    }

    /// A mock store whose `list` stream yields a configurable sequence of
    /// Ok / throttle-error items. Used to verify that the AIMD wrapper
    /// observes errors surfaced inside list streams.
    struct ThrottlingListMockStore {
        inner: InMemory,
        /// Number of throttle errors to inject at the start of each list call.
        throttle_count: usize,
    }

    impl Display for ThrottlingListMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ThrottlingListMockStore")
        }
    }

    impl Debug for ThrottlingListMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ThrottlingListMockStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for ThrottlingListMockStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            let n = self.throttle_count;
            let inner_stream = self.inner.list(prefix);
            let errors = futures::stream::iter((0..n).map(|_| {
                Err(object_store::Error::Generic {
                    store: "ThrottlingListMock",
                    source: "request failed, after 3 retries, max_retries: 5, retry_timeout: 60s"
                        .into(),
                })
            }));
            errors.chain(inner_stream).boxed()
        }
        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[tokio::test]
    async fn test_list_stream_throttle_errors_decrease_rate() {
        let mock = Arc::new(ThrottlingListMockStore {
            inner: InMemory::new(),
            throttle_count: 5,
        });

        // Seed a file so the real items come through after the errors.
        mock.put(
            &Path::from("prefix/file.txt"),
            PutPayload::from_static(b"data"),
        )
        .await
        .unwrap();

        let config = AimdThrottleConfig::default().with_list_aimd(
            AimdConfig::default()
                .with_initial_rate(100.0)
                .with_decrease_factor(0.5)
                .with_window_duration(std::time::Duration::from_millis(10)),
        );
        let throttled = AimdThrottledStore::new(mock as Arc<dyn ObjectStore>, config).unwrap();

        let initial_rate = throttled.list.controller.current_rate();
        assert_eq!(initial_rate, 100.0);

        let items: Vec<_> = throttled.list(Some(&Path::from("prefix"))).collect().await;

        // 5 errors + 1 real item
        assert_eq!(items.len(), 6);
        assert!(items[0].is_err());
        assert!(items[5].is_ok());

        // Wait for the AIMD window to expire and trigger evaluation.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        throttled
            .list
            .controller
            .record_outcome(RequestOutcome::Success);

        let new_rate = throttled.list.controller.current_rate();
        assert!(
            new_rate < initial_rate,
            "List rate should decrease after stream throttle errors: {} < {}",
            new_rate,
            initial_rate
        );
    }

    struct CountingListStartStore {
        inner: InMemory,
        list_calls: AtomicUsize,
        offset_calls: AtomicUsize,
    }

    impl Default for CountingListStartStore {
        fn default() -> Self {
            Self {
                inner: InMemory::new(),
                list_calls: AtomicUsize::new(0),
                offset_calls: AtomicUsize::new(0),
            }
        }
    }

    impl CountingListStartStore {
        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }

        fn offset_calls(&self) -> usize {
            self.offset_calls.load(Ordering::SeqCst)
        }
    }

    impl Display for CountingListStartStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingListStartStore")
        }
    }

    impl Debug for CountingListStartStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CountingListStartStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for CountingListStartStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.offset_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    fn list_start_throttle_config() -> AimdThrottleConfig {
        // Use a low rate (10 tokens/s) so that the token-acquisition sleep is
        // 1/10 = 100 ms — well above the 50 ms timeout used in assertions,
        // avoiding flakiness from coarse OS timer resolution (e.g. Windows ~16 ms).
        AimdThrottleConfig::default()
            .with_burst_capacity(0)
            .with_list_aimd(AimdConfig::default().with_initial_rate(10.0))
    }

    #[tokio::test(start_paused = true)]
    async fn test_list_acquires_token_before_starting_underlying_stream() {
        let store = Arc::new(CountingListStartStore::default());
        store
            .put(
                &Path::from("prefix/file.txt"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();
        let throttled = AimdThrottledStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            list_start_throttle_config(),
        )
        .unwrap();

        let mut stream = throttled.list(Some(&Path::from("prefix")));
        assert_eq!(store.list_calls(), 0);
        // With rate=10 tokens/s and burst_capacity=0, the token acquisition
        // sleeps for 100 ms. A 50 ms timeout must expire before that.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next())
                .await
                .is_err()
        );
        assert_eq!(store.list_calls(), 0);

        let item = tokio::time::timeout(std::time::Duration::from_millis(300), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(item.location, Path::from("prefix/file.txt"));
        assert_eq!(store.list_calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_list_with_offset_acquires_token_before_starting_underlying_stream() {
        let store = Arc::new(CountingListStartStore::default());
        store
            .put(&Path::from("prefix/b"), PutPayload::from_static(b"data"))
            .await
            .unwrap();
        let throttled = AimdThrottledStore::new(
            store.clone() as Arc<dyn ObjectStore>,
            list_start_throttle_config(),
        )
        .unwrap();

        let mut stream =
            throttled.list_with_offset(Some(&Path::from("prefix")), &Path::from("prefix/a"));
        assert_eq!(store.offset_calls(), 0);
        // With rate=10 tokens/s and burst_capacity=0, the token acquisition
        // sleeps for 100 ms. A 50 ms timeout must expire before that.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), stream.next())
                .await
                .is_err()
        );
        assert_eq!(store.offset_calls(), 0);

        let item = tokio::time::timeout(std::time::Duration::from_millis(300), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(item.location, Path::from("prefix/b"));
        assert_eq!(store.offset_calls(), 1);
    }

    #[tokio::test]
    async fn test_per_category_independence() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default().with_aimd(
            AimdConfig::default()
                .with_initial_rate(100.0)
                .with_decrease_factor(0.5)
                .with_window_duration(std::time::Duration::from_millis(10)),
        );
        let throttled = AimdThrottledStore::new(store, config).unwrap();

        // Push the read controller into a throttled state
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Throttled);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        throttled
            .read
            .controller
            .record_outcome(RequestOutcome::Success);

        let read_rate = throttled.read.controller.current_rate();
        let write_rate = throttled.write.controller.current_rate();
        let delete_rate = throttled.delete.controller.current_rate();
        let list_rate = throttled.list.controller.current_rate();

        assert_eq!(read_rate, 50.0, "Read rate should have decreased");
        assert_eq!(write_rate, 100.0, "Write rate should be unaffected");
        assert_eq!(delete_rate, 100.0, "Delete rate should be unaffected");
        assert_eq!(list_rate, 100.0, "List rate should be unaffected");
    }

    #[tokio::test]
    async fn test_per_category_config() {
        let store = Arc::new(InMemory::new());
        let config = AimdThrottleConfig::default()
            .with_read_aimd(AimdConfig::default().with_initial_rate(200.0))
            .with_write_aimd(AimdConfig::default().with_initial_rate(100.0))
            .with_delete_aimd(AimdConfig::default().with_initial_rate(50.0))
            .with_list_aimd(AimdConfig::default().with_initial_rate(25.0));
        let throttled = AimdThrottledStore::new(store, config).unwrap();

        assert_eq!(throttled.read.controller.current_rate(), 200.0);
        assert_eq!(throttled.write.controller.current_rate(), 100.0);
        assert_eq!(throttled.delete.controller.current_rate(), 50.0);
        assert_eq!(throttled.list.controller.current_rate(), 25.0);
    }

    /// A mock [`ObjectStore`] that measures request rate over a sliding window
    /// and returns 503 errors when the rate exceeds a configurable threshold.
    /// Write and metadata-only operations are not rate-limited.
    struct RateLimitingMockStore {
        inner: InMemory,
        /// Timestamps of recent successful (admitted) requests.
        timestamps: std::sync::Mutex<VecDeque<std::time::Instant>>,
        /// Maximum requests allowed within `window`.
        max_per_window: usize,
        /// Sliding window duration.
        window: std::time::Duration,
        success_count: AtomicU64,
        throttle_count: AtomicU64,
    }

    impl RateLimitingMockStore {
        fn new(max_per_window: usize, window: std::time::Duration) -> Self {
            Self {
                inner: InMemory::new(),
                timestamps: std::sync::Mutex::new(VecDeque::new()),
                max_per_window,
                window,
                success_count: AtomicU64::new(0),
                throttle_count: AtomicU64::new(0),
            }
        }

        /// Returns `true` if the request is admitted, `false` if throttled.
        fn check_rate(&self) -> bool {
            let mut ts = self.timestamps.lock().unwrap();
            let now = std::time::Instant::now();
            while let Some(&front) = ts.front() {
                if now.duration_since(front) > self.window {
                    ts.pop_front();
                } else {
                    break;
                }
            }
            if ts.len() >= self.max_per_window {
                self.throttle_count.fetch_add(1, Ordering::Relaxed);
                false
            } else {
                ts.push_back(now);
                self.success_count.fetch_add(1, Ordering::Relaxed);
                true
            }
        }

        fn throttle_error() -> object_store::Error {
            object_store::Error::Generic {
                store: "RateLimitingMock",
                source: THROTTLE_ERROR_RESPONSE.into(),
            }
        }
    }

    impl Display for RateLimitingMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RateLimitingMockStore")
        }
    }

    impl Debug for RateLimitingMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RateLimitingMockStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for RateLimitingMockStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            if self.check_rate() {
                self.inner.get_opts(location, options).await
            } else {
                Err(Self::throttle_error())
            }
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            if self.check_rate() {
                self.inner.get_ranges(location, ranges).await
            } else {
                Err(Self::throttle_error())
            }
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// Verify that multiple concurrent readers sharing an AIMD-throttled store
    /// converge to the backend's actual capacity.
    ///
    /// Setup:
    /// - Mock backend allows 30 requests per 100ms (= 300 req/s).
    /// - 5 reader tasks, each with their own [`AimdThrottledStore`] wrapping
    ///   the shared mock.
    /// - AIMD: 100ms window, initial rate 100 req/s, decrease 0.5, increase 2.
    /// - Readers issue `head()` requests as fast as the throttle allows for 2s.
    ///
    /// Expected behaviour:
    /// - Initial burst (100 burst tokens × 5 readers) overshoots the mock
    ///   capacity, causing many 503s. Each reader's AIMD halves its rate.
    /// - After the transient, each reader converges to ~60 req/s (300/5).
    /// - Over 2 seconds, total successful requests should be in the range
    ///   [300, 900] (theoretical max ≈ 600).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_aimd_throttle_under_concurrent_load() {
        let mock = Arc::new(RateLimitingMockStore::new(
            30,
            std::time::Duration::from_millis(100),
        ));

        // Seed a test file so head() succeeds when admitted.
        let path = Path::from("test/data.bin");
        mock.put(&path, PutPayload::from_static(b"test data"))
            .await
            .unwrap();

        let aimd = AimdConfig::default()
            .with_initial_rate(100.0)
            .with_decrease_factor(0.5)
            .with_additive_increment(2.0)
            .with_window_duration(std::time::Duration::from_millis(100));
        let throttle_config = AimdThrottleConfig::default()
            .with_aimd(aimd)
            .with_burst_capacity(100);

        let num_readers = 5;
        let test_duration = std::time::Duration::from_secs(2);
        let mut handles = Vec::new();

        for _ in 0..num_readers {
            let store = Arc::new(
                AimdThrottledStore::new(
                    mock.clone() as Arc<dyn ObjectStore>,
                    throttle_config.clone(),
                )
                .unwrap(),
            );
            let p = path.clone();
            handles.push(tokio::spawn(async move {
                let deadline = std::time::Instant::now() + test_duration;
                let mut count = 0u64;
                while std::time::Instant::now() < deadline {
                    let _ = store.head(&p).await;
                    count += 1;
                }
                count
            }));
        }

        let mut total_reader_requests = 0u64;
        for handle in handles {
            total_reader_requests += handle.await.unwrap();
        }

        let successes = mock.success_count.load(Ordering::Relaxed);
        let throttled = mock.throttle_count.load(Ordering::Relaxed);
        let total_mock = successes + throttled;

        // Mock-side count >= reader-side count because the AIMD layer retries
        // throttle errors internally, causing multiple mock calls per reader call.
        assert!(
            total_mock >= total_reader_requests,
            "Mock-side count ({total_mock}) should be >= reader-side count ({total_reader_requests})"
        );

        // Mock capacity is 30/100ms = 300 req/s. Over 2s the theoretical max is
        // ~600 successful requests. With AIMD ramp-up, expect somewhat fewer.
        assert!(
            successes >= 300,
            "Expected >= 300 successful requests over 2s, got {successes}"
        );
        assert!(
            successes <= 900,
            "Expected <= 900 successful requests, got {successes}"
        );

        // The initial burst exceeds mock capacity, so throttling must occur.
        assert!(throttled > 0, "Expected some throttled requests but got 0");

        // Without AIMD, raw tokio tasks against InMemory would fire 100k+ req/s.
        // AIMD should keep the total well under 5000 over 2s.
        assert!(
            total_mock <= 5000,
            "AIMD should limit total requests, got {total_mock}"
        );
    }

    /// A mock store that returns a configurable number of throttle errors
    /// before succeeding on `get` operations. Used to test the retry logic
    /// inside `OperationThrottle::throttled()`.
    struct RetryTestMockStore {
        inner: InMemory,
        /// Number of throttle errors remaining before success.
        errors_remaining: std::sync::Mutex<usize>,
        /// Total number of `get` calls observed.
        get_call_count: AtomicU64,
    }

    impl RetryTestMockStore {
        fn new(errors_before_success: usize) -> Self {
            Self {
                inner: InMemory::new(),
                errors_remaining: std::sync::Mutex::new(errors_before_success),
                get_call_count: AtomicU64::new(0),
            }
        }
    }

    impl Display for RetryTestMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RetryTestMockStore")
        }
    }

    impl Debug for RetryTestMockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RetryTestMockStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for RetryTestMockStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.get_call_count.fetch_add(1, Ordering::Relaxed);
            let should_error = {
                let mut remaining = self.errors_remaining.lock().unwrap();
                if *remaining > 0 {
                    *remaining -= 1;
                    true
                } else {
                    false
                }
            };
            if should_error {
                Err(object_store::Error::Generic {
                    store: "RetryTestMock",
                    source: THROTTLE_ERROR_RESPONSE.into(),
                })
            } else {
                self.inner.get_opts(location, options).await
            }
        }
        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[tokio::test]
    async fn test_throttled_retries_on_throttle_error_then_succeeds() {
        // Mock returns 2 throttle errors then succeeds (within MAX_RETRIES=3)
        let mock = Arc::new(RetryTestMockStore::new(2));
        let path = Path::from("test/retry.txt");
        mock.put(&path, PutPayload::from_static(b"retry data"))
            .await
            .unwrap();

        let config = AimdThrottleConfig::default();
        let throttled =
            AimdThrottledStore::new(mock.clone() as Arc<dyn ObjectStore>, config).unwrap();

        let result = throttled.get(&path).await;
        assert!(result.is_ok(), "Expected success after retries");

        let bytes = result.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"retry data");

        // Should have called get 3 times total: 2 failures + 1 success
        assert_eq!(mock.get_call_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_throttled_fails_after_max_retries_exceeded() {
        // Mock returns 4 throttle errors (more than MAX_RETRIES=3),
        // so all 4 attempts (initial + 3 retries) will fail.
        let mock = Arc::new(RetryTestMockStore::new(10));
        let path = Path::from("test/fail.txt");
        mock.put(&path, PutPayload::from_static(b"fail data"))
            .await
            .unwrap();

        let config = AimdThrottleConfig::default();
        let throttled =
            AimdThrottledStore::new(mock.clone() as Arc<dyn ObjectStore>, config).unwrap();

        let result = throttled.get(&path).await;
        assert!(result.is_err(), "Expected error after max retries");
        let err = result.unwrap_err();
        assert!(is_throttle_error(&err));

        let lance_error = lance_core::Error::from(err);
        let error_message = lance_error.to_string();
        assert!(error_message.contains("x-ms-request-id"));
        assert!(error_message.contains("azure-request-id"));

        // Should have called get 4 times: initial attempt + 3 retries
        assert_eq!(mock.get_call_count.load(Ordering::Relaxed), 4);
    }

    #[cfg(feature = "aws")]
    #[derive(Debug)]
    struct MultipartRetryState {
        failures_remaining: AtomicUsize,
        part_uris: std::sync::Mutex<Vec<String>>,
    }

    #[cfg(feature = "aws")]
    #[derive(Debug)]
    struct MultipartRetryConnector {
        state: Arc<MultipartRetryState>,
    }

    #[cfg(feature = "aws")]
    impl HttpConnector for MultipartRetryConnector {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(MultipartRetryService {
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[cfg(feature = "aws")]
    #[derive(Debug)]
    struct MultipartRetryService {
        state: Arc<MultipartRetryState>,
    }

    #[cfg(feature = "aws")]
    #[async_trait]
    impl HttpService for MultipartRetryService {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let method = request.method().clone();
            let query = request.uri().query().unwrap_or_default();
            let (status, body, e_tag) = if method == ::http::Method::POST
                && query
                    .split('&')
                    .any(|part| part == "uploads" || part == "uploads=")
            {
                (
                    ::http::StatusCode::OK,
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>object</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>",
                    None,
                )
            } else if method == ::http::Method::PUT && query.contains("partNumber=") {
                self.state
                    .part_uris
                    .lock()
                    .unwrap()
                    .push(request.uri().to_string());
                let mut remaining = self.state.failures_remaining.load(Ordering::SeqCst);
                let should_fail = loop {
                    let Some(next) = remaining.checked_sub(1) else {
                        break false;
                    };
                    match self.state.failures_remaining.compare_exchange_weak(
                        remaining,
                        next,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break true,
                        Err(actual) => remaining = actual,
                    }
                };
                if should_fail {
                    (
                        ::http::StatusCode::SERVICE_UNAVAILABLE,
                        "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>",
                        None,
                    )
                } else {
                    (::http::StatusCode::OK, "", Some("\"part-etag\""))
                }
            } else if method == ::http::Method::POST && query.contains("uploadId=") {
                (
                    ::http::StatusCode::OK,
                    "<CompleteMultipartUploadResult><Location>https://bucket/object</Location><Bucket>bucket</Bucket><Key>object</Key><ETag>\"object-etag\"</ETag></CompleteMultipartUploadResult>",
                    None,
                )
            } else {
                (::http::StatusCode::BAD_REQUEST, "unexpected request", None)
            };

            let mut response = ::http::Response::builder().status(status);
            if let Some(e_tag) = e_tag {
                response = response.header(::http::header::ETAG, e_tag);
            }
            Ok(response
                .body(HttpResponseBody::from(body.to_string()))
                .unwrap())
        }
    }

    /// Retries must remain inside the original S3 `put_part` call. Re-entering
    /// `MultipartUpload::put_part` would allocate a new part number and leave a
    /// gap that makes `complete` fail with "Missing part".
    #[cfg(feature = "aws")]
    #[tokio::test(start_paused = true)]
    async fn test_multipart_http_retry_reuses_part_number() {
        use object_store::RetryConfig;
        use object_store::aws::AmazonS3Builder;

        let retry_state = Arc::new(MultipartRetryState {
            failures_remaining: AtomicUsize::new(3),
            part_uris: std::sync::Mutex::new(Vec::new()),
        });
        let throttle_state = AimdThrottleState::new(AimdThrottleConfig::default()).unwrap();
        let connector = AimdMultipartUploadConnector::new(
            MultipartRetryConnector {
                state: Arc::clone(&retry_state),
            },
            Some(&throttle_state),
        );
        let store = AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_skip_signature(true)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_http_connector(connector)
            .build()
            .unwrap();

        let mut upload = store.put_multipart(&Path::from("object")).await.unwrap();
        upload
            .put_part(PutPayload::from_static(b"payload"))
            .await
            .unwrap();
        upload.complete().await.unwrap();

        let part_uris = retry_state.part_uris.lock().unwrap();
        assert_eq!(part_uris.len(), 4);
        assert!(part_uris.iter().all(|uri| uri == &part_uris[0]));
        assert!(part_uris[0].contains("partNumber=1"));
    }

    #[tokio::test]
    async fn test_throttled_multipart_reorders_parts() {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = AimdThrottleConfig::default();
        let throttled = AimdThrottledStore::new(store.clone(), config).unwrap();

        let path = Path::from("test/multipart_ordering.bin");
        let mut upload = throttled.put_multipart(&path).await.unwrap();

        // Create futures for two parts in order: A then B.
        let fut_a = upload.put_part(PutPayload::from_static(b"AAAA"));
        let fut_b = upload.put_part(PutPayload::from_static(b"BBBB"));

        // Await in REVERSE order. Part ordering should be determined by
        // creation order (put_part call order), not by await order.
        fut_b.await.unwrap();
        fut_a.await.unwrap();

        upload.complete().await.unwrap();

        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();

        assert_eq!(
            bytes.as_ref(),
            b"AAAABBBB",
            "Parts were reordered! Got {:?} instead of AAAABBBB.",
            std::str::from_utf8(&bytes).unwrap_or("<non-utf8>"),
        );
    }
}
