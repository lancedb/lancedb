// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Cloud blob column listing, whole-byte fetch, and seekable HTTP byte-range handles.

mod aggregate;

use aggregate::BlobAccumulator;
use arrow_buffer::{Buffer, OffsetBuffer};

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, LargeBinaryArray, StructArray, UInt8Array, UInt64Array};
use arrow_schema::DataType;
use bytes::Bytes;
use datafusion_expr::{col, lit};
use futures::{StreamExt, TryStreamExt};
use lance::dataset::ROW_ID;
use lance_core::datatypes::BlobKind;
use reqwest::{Response, StatusCode, header};
use serde::Deserialize;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock};

use crate::Error;
use crate::blob::{BlobDescriptor, BlobDescriptorSource, BlobFile, BlobRangeRequest};
use crate::error::Result;
use crate::query::{QueryFilter, QueryRequest, Select};
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};
use crate::table::{AnyQuery, BaseTable};

use super::{
    FreshnessHeaders, FreshnessState, QUERY_SNAPSHOT_HEADER, ReadSnapshot, RemoteTable,
    VERSION_HEADER, check_query_snapshot_response, freshness_headers_snapshot,
    valid_query_snapshot_identity,
};

// The Cloud route rejects larger row-id lists. Its separate 64 MiB byte limit
// is handled by splitting a rejected request below.
const MAX_FETCH_BLOBS_ROW_IDS: usize = 1024;
const MAX_FETCH_BLOB_RANGE_BYTES: u64 = 64 * 1024 * 1024;
// Refresh shortly before expiry so a request does not start with a URL that
// expires while it is being signed and sent.
const BLOB_PRESIGN_REFRESH_SKEW: Duration = Duration::from_secs(30);

fn is_blob_locations_unavailable(error: &Error) -> bool {
    error.status_code() == Some(StatusCode::SERVICE_UNAVAILABLE)
}

fn is_fetch_blobs_byte_limit_error(error: &Error) -> bool {
    matches!(error, Error::Http {
        source,
        status_code: Some(StatusCode::BAD_REQUEST),
        ..
    } if source.to_string().contains("fetch_blobs accepts at most")
        && source.to_string().contains("total blob bytes"))
}

#[derive(Debug, Clone, Copy)]
enum RangeRequestMode {
    SizeProbe,
    DataRead,
    MetadataRead,
}

#[async_trait::async_trait]
trait BlobRangeRequester: Send + Sync + std::fmt::Debug {
    async fn request_range(
        &self,
        range: Range<u64>,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<BlobRangeResponse>;
}

/// Keep the connection slot until the response body has been consumed or dropped.
#[derive(Debug)]
struct BlobRangeResponse {
    request_id: String,
    response: Response,
    /// Whether the requester already validated the response's status and
    /// `Content-Range` against the logical blob. Direct object-store responses
    /// use physical coordinates, so the outer handle cannot re-check them.
    validated: bool,
    _permit: Option<OwnedSemaphorePermit>,
}

#[cfg(test)]
impl From<(String, Response)> for BlobRangeResponse {
    fn from((request_id, response): (String, Response)) -> Self {
        Self {
            request_id,
            response,
            validated: false,
            _permit: None,
        }
    }
}

#[derive(Debug)]
struct TableBlobRangeRequester<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    path: String,
    version: Option<u64>,
    branch: Option<String>,
    source_uri: Option<String>,
    expected_query_snapshot: Option<String>,
    source_commit_micros: Option<u64>,
    row_is_address: bool,
    freshness: Arc<std::sync::Mutex<FreshnessState>>,
    parent_freshness: Arc<std::sync::Mutex<FreshnessState>>,
    parent_freshness_request: FreshnessHeaders,
    read_consistency_interval: Option<Duration>,
}

#[async_trait::async_trait]
impl<S: HttpSend> BlobRangeRequester for TableBlobRangeRequester<S> {
    async fn request_range(
        &self,
        range: Range<u64>,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<BlobRangeResponse> {
        let range_header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
        let permit = self
            .client
            .blob_request_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("blob request semaphore is never closed");
        let freshness_request =
            freshness_headers_snapshot(&self.freshness, self.read_consistency_interval);
        let mut request = if matches!(mode, RangeRequestMode::MetadataRead) {
            freshness_request.apply(self.client.head(&self.path))
        } else {
            freshness_request
                .apply(self.client.get(&self.path))
                .header(header::RANGE, range_header)
        };
        if let Some(identity) = &self.expected_query_snapshot {
            request = request.header(QUERY_SNAPSHOT_HEADER, identity);
        }
        if let Some(version) = version.or(self.version) {
            request = request.query(&[("version", version)]);
        }
        if let Some(branch) = &self.branch {
            request = request.query(&[("branch", branch)]);
        }
        if let Some(source_uri) = &self.source_uri {
            request = request
                .query(&[("source_uri", source_uri)])
                .query(&[("row_is_address", self.row_is_address)]);
        }
        if let Some(timestamp) = self.source_commit_micros {
            request = request.query(&[("source_commit_micros", timestamp)]);
        }
        let (request_id, response) = self.client.send_with_retry(request, None, true).await?;
        if response.status().is_success() || response.status() == StatusCode::RANGE_NOT_SATISFIABLE
        {
            check_query_snapshot_response(self.expected_query_snapshot.as_deref(), &response)?;
        }
        // Preserve 416 size-probe responses so the caller can detect empty blobs.
        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE
            && matches!(mode, RangeRequestMode::SizeProbe)
        {
            return Ok(BlobRangeResponse {
                request_id,
                response,
                validated: false,
                _permit: Some(permit),
            });
        }
        let response = self.client.check_response(&request_id, response).await?;
        freshness_request.observe_headers(&self.freshness, response.headers());
        if self.source_uri.is_none() {
            self.parent_freshness_request
                .observe_headers(&self.parent_freshness, response.headers());
        }
        Ok(BlobRangeResponse {
            request_id,
            response,
            validated: false,
            _permit: Some(permit),
        })
    }
}

#[derive(Debug)]
struct RemoteBlobState {
    cursor: u64,
    buffer_size: usize,
}

impl Default for RemoteBlobState {
    fn default() -> Self {
        Self {
            cursor: 0,
            buffer_size: BlobFile::DEFAULT_READ_BUFFER_SIZE,
        }
    }
}

#[derive(Debug, Default)]
struct RemoteBlobBuffer {
    start: u64,
    bytes: Bytes,
}

/// Seekable Cloud blob handle over HTTP Range.
///
/// Handles read the table version used to fetch their descriptor or probe their size.
#[derive(Debug)]
pub struct RemoteBlobFile {
    requester: Arc<dyn BlobRangeRequester>,
    state: Mutex<RemoteBlobState>,
    // Never held across I/O: close can release cached bytes even while a read
    // holds the cursor lock. A reader may retain its own prefix until it ends.
    buffer: std::sync::Mutex<RemoteBlobBuffer>,
    closed: AtomicBool,
    size: u64,
    version: Option<u64>,
}

impl RemoteBlobFile {
    fn new(requester: Arc<dyn BlobRangeRequester>, size: u64, version: Option<u64>) -> Self {
        Self {
            requester,
            state: Mutex::new(RemoteBlobState::default()),
            buffer: std::sync::Mutex::new(RemoteBlobBuffer::default()),
            closed: AtomicBool::new(false),
            size,
            version,
        }
    }

    /// Close the handle without waiting for an in-flight read.
    pub(crate) async fn close(&self) -> lance_core::Result<()> {
        self.closed.store(true, Ordering::Release);
        self.buffer.lock().unwrap().bytes = Bytes::new();
        Ok(())
    }

    pub(crate) async fn set_buffer_size(&self, size: usize) -> lance_core::Result<()> {
        self.ensure_open()?;
        let mut state = self.state.lock().await;
        self.ensure_open()?;
        if state.buffer_size != size {
            state.buffer_size = size;
            self.buffer.lock().unwrap().bytes = Bytes::new();
        }
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn ensure_open(&self) -> lance_core::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(lance_core::Error::invalid_input(
                "blob file is already closed",
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn read_range(&self, range: Range<u64>) -> lance_core::Result<Bytes> {
        self.ensure_open()?;
        if range.start > range.end {
            return Err(lance_core::Error::invalid_input(format!(
                "blob range start {} exceeds end {}",
                range.start, range.end
            )));
        }
        if range.end > self.size {
            return Err(lance_core::Error::invalid_input(format!(
                "blob range end {} exceeds blob size {}",
                range.end, self.size
            )));
        }
        if range.is_empty() {
            return Ok(Bytes::new());
        }
        let BlobRangeResponse {
            request_id,
            response,
            validated,
            _permit: permit,
        } = self
            .requester
            .request_range(range.clone(), RangeRequestMode::DataRead, self.version)
            .await
            .map_err(remote_blob_error)?;
        self.ensure_open()?;
        if !validated {
            validate_partial_response(&response, range.clone(), self.size)?;
        }
        let bytes = read_blob_body(response, range.end - range.start, request_id).await?;
        drop(permit);
        self.ensure_open()?;
        Ok(bytes)
    }

    /// Read ranges concurrently while preserving input order.
    pub(crate) async fn read_ranges(
        &self,
        ranges: &[Range<u64>],
    ) -> lance_core::Result<Vec<Bytes>> {
        futures::stream::iter(ranges.iter().cloned().map(|range| self.read_range(range)))
            .buffered(BLOB_REQUEST_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Read from the cursor to the end of the blob.
    ///
    /// Holds the state lock across cursor calculation and reading so a concurrent
    /// seek cannot change the cursor between them.
    pub(crate) async fn read(&self) -> lance_core::Result<Bytes> {
        self.ensure_open()?;
        let mut state = self.state.lock().await;
        self.ensure_open()?;
        let remaining = self.size.saturating_sub(state.cursor);
        let remaining = usize::try_from(remaining).map_err(|_| {
            lance_core::Error::invalid_input("remaining blob length exceeds addressable memory")
        })?;
        // Joining a small cached prefix to a large tail would copy the entire
        // blob while both allocations are live. Match the native reader's
        // bounded reuse rule for whole-blob reads.
        let held_len = cached_prefix(&self.buffer.lock().unwrap(), state.cursor, remaining).len();
        if remaining > held_len.saturating_mul(2) {
            self.buffer.lock().unwrap().bytes = Bytes::new();
        }
        let bytes = self.read_up_to_locked(&mut state, remaining).await?;
        self.buffer.lock().unwrap().bytes = Bytes::new();
        Ok(bytes)
    }

    pub(crate) async fn read_up_to(&self, len: usize) -> lance_core::Result<Bytes> {
        self.ensure_open()?;
        let mut state = self.state.lock().await;
        self.ensure_open()?;
        self.read_up_to_locked(&mut state, len).await
    }

    /// Read up to `len` bytes using caller-validated, locked state.
    async fn read_up_to_locked(
        &self,
        state: &mut RemoteBlobState,
        len: usize,
    ) -> lance_core::Result<Bytes> {
        let target_len = self.size.saturating_sub(state.cursor).min(len as u64) as usize;
        if target_len == 0 {
            return Ok(Bytes::new());
        }

        let prefix = cached_prefix(&self.buffer.lock().unwrap(), state.cursor, target_len);
        if prefix.len() == target_len {
            state.cursor += target_len as u64;
            return Ok(prefix);
        }
        let fetch_start = state.cursor + prefix.len() as u64;
        let still_need = target_len - prefix.len();
        let fetch_len = if state.buffer_size == 0 {
            still_need as u64
        } else {
            (state.buffer_size.max(still_need) as u64).min(self.size - fetch_start)
        };
        // Fully consume the response before retaining only its Bytes. read_range
        // releases both the connection and the concurrency permit before return.
        let data = self
            .read_range(fetch_start..fetch_start + fetch_len)
            .await?;
        self.ensure_open()?;
        let tail = data.slice(..still_need);
        let result = if prefix.is_empty() {
            tail
        } else {
            let mut joined = Vec::new();
            joined.try_reserve_exact(target_len).map_err(|error| {
                remote_blob_error(format!(
                    "cannot allocate blob read of {target_len} bytes: {error}"
                ))
            })?;
            joined.extend_from_slice(&prefix);
            joined.extend_from_slice(&tail);
            Bytes::from(joined)
        };
        {
            let mut buffer = self.buffer.lock().unwrap();
            // Serialize publishing a new window with close clearing the cache.
            self.ensure_open()?;
            if state.buffer_size != 0 {
                buffer.start = fetch_start;
                buffer.bytes = data;
            } else {
                buffer.bytes = Bytes::new();
            }
        }
        state.cursor += target_len as u64;
        Ok(result)
    }

    pub(crate) async fn seek(&self, new_cursor: u64) -> lance_core::Result<()> {
        self.ensure_open()?;
        let mut state = self.state.lock().await;
        self.ensure_open()?;
        state.cursor = new_cursor;
        Ok(())
    }

    pub(crate) async fn tell(&self) -> lance_core::Result<u64> {
        self.ensure_open()?;
        let state = self.state.lock().await;
        self.ensure_open()?;
        Ok(state.cursor)
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }
}

/// Collect a range without retaining a second payload's worth of network chunks.
async fn read_blob_body(
    response: Response,
    expected: u64,
    request_id: String,
) -> lance_core::Result<Bytes> {
    let expected = usize::try_from(expected).map_err(|_| {
        lance_core::Error::invalid_input("blob range length exceeds addressable memory")
    })?;
    let length_error = |received| {
        remote_blob_error(format!(
            "byte range returned {received} bytes, expected {expected}"
        ))
    };
    let mut chunks = response.bytes_stream();
    let first = chunks
        .try_next()
        .await
        .err_to_http(request_id.clone())
        .map_err(remote_blob_error)?
        .ok_or_else(|| length_error(0))?;
    if first.len() > expected {
        return Err(length_error(first.len()));
    }
    if first.len() == expected {
        while let Some(chunk) = chunks
            .try_next()
            .await
            .err_to_http(request_id.clone())
            .map_err(remote_blob_error)?
        {
            if !chunk.is_empty() {
                return Err(length_error(expected.saturating_add(chunk.len())));
            }
        }
        return Ok(first);
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(expected).map_err(|error| {
        remote_blob_error(format!(
            "cannot allocate blob range of {expected} bytes: {error}"
        ))
    })?;
    bytes.extend_from_slice(&first);
    drop(first);
    while let Some(chunk) = chunks
        .try_next()
        .await
        .err_to_http(request_id.clone())
        .map_err(remote_blob_error)?
    {
        let received = bytes.len().saturating_add(chunk.len());
        if received > expected {
            return Err(length_error(received));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected {
        return Err(length_error(bytes.len()));
    }
    Ok(Bytes::from(bytes))
}

fn cached_prefix(buffer: &RemoteBlobBuffer, cursor: u64, length: usize) -> Bytes {
    let Some(offset) = cursor.checked_sub(buffer.start) else {
        return Bytes::new();
    };
    let Ok(offset) = usize::try_from(offset) else {
        return Bytes::new();
    };
    if offset >= buffer.bytes.len() {
        return Bytes::new();
    }
    let end = offset.saturating_add(length).min(buffer.bytes.len());
    buffer.bytes.slice(offset..end)
}

fn remote_blob_error(error: impl std::fmt::Display) -> lance_core::Error {
    lance_core::Error::io(format!("remote blob read failed: {error}"))
}

fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let value = value.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?, total.parse().ok()?))
}

/// Parse the total from an unsatisfied-range header, `bytes */{total}`.
fn parse_unsatisfied_content_range(value: &str) -> Option<u64> {
    value.strip_prefix("bytes */")?.parse().ok()
}

/// Validate a partial response against the requested range and blob size.
///
/// Reject `200 OK` because it may contain the entire object.
fn validate_partial_response(
    response: &Response,
    expected: Range<u64>,
    size: u64,
) -> lance_core::Result<()> {
    validate_content_range(response, expected.start, expected.end - 1, size, true)
}

/// Validate a direct object-store response whose `Content-Range` uses physical
/// offsets in a shared sidecar. The object may be larger than the logical blob;
/// only its total must cover the full logical value.
fn validate_physical_content_range(
    response: &Response,
    expected: Range<u64>,
    base_offset: u64,
    logical_size: u64,
) -> lance_core::Result<()> {
    let start = base_offset
        .checked_add(expected.start)
        .ok_or_else(|| remote_blob_error("blob physical range start overflows u64"))?;
    let end = base_offset
        .checked_add(expected.end)
        .ok_or_else(|| remote_blob_error("blob physical range end overflows u64"))?;
    let minimum_total = base_offset
        .checked_add(logical_size)
        .ok_or_else(|| remote_blob_error("blob physical range total overflows u64"))?;
    validate_content_range(response, start, end - 1, minimum_total, false)
}

fn validate_content_range(
    response: &Response,
    expected_start: u64,
    expected_end: u64,
    expected_total: u64,
    exact_total: bool,
) -> lance_core::Result<()> {
    if response.status() != StatusCode::PARTIAL_CONTENT {
        return Err(remote_blob_error(format!(
            "expected HTTP 206 Partial Content, got {}",
            response.status()
        )));
    }
    let content_range = response
        .headers()
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .ok_or_else(|| remote_blob_error("response is missing a valid Content-Range header"))?;
    if content_range.0 != expected_start
        || content_range.1 != expected_end
        || (exact_total && content_range.2 != expected_total)
        || (!exact_total && content_range.2 < expected_total)
    {
        return Err(remote_blob_error(format!(
            "expected Content-Range 'bytes {expected_start}-{expected_end}/{expected_total}{}', got 'bytes {}-{}/{}'",
            if exact_total { "" } else { " or larger" },
            content_range.0,
            content_range.1,
            content_range.2
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
struct BlobLocationSource {
    url: String,
    #[serde(default)]
    expires_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BlobLocationItem {
    Presigned {
        source_index: usize,
        offset: u64,
        size: u64,
    },
    Inline {
        size: u64,
        data_base64: String,
    },
    Proxy {
        size: u64,
    },
}

#[derive(Debug)]
struct ResolvedBlobLocations {
    sources: Vec<BlobLocationSource>,
    items: Vec<Option<BlobLocationItem>>,
    version: u64,
    identity: Option<String>,
}

#[derive(Deserialize)]
struct BlobLocationsBody {
    sources: Vec<BlobLocationSource>,
    items: Vec<Option<BlobLocationItem>>,
}

/// One pinned table/column position used to resolve or refresh Blob locations.
#[derive(Clone)]
struct BlobLocationsContext<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    identifier: String,
    table_name: String,
    column: String,
    version: Option<u64>,
    branch: Option<String>,
    source_uri: Option<String>,
    source_commit_micros: Option<u64>,
    row_is_address: bool,
    expected_query_snapshot: Option<String>,
    freshness: Option<FreshnessHeaders>,
}

impl<S: HttpSend> BlobLocationsContext<S> {
    async fn resolve(
        &self,
        row_ids: &[u64],
        include_inline_data: bool,
    ) -> Result<ResolvedBlobLocations> {
        let started = Instant::now();
        let mut body = serde_json::json!({
            "column": &self.column,
            "version": self.version,
            "row_ids": row_ids,
            "row_is_address": self.row_is_address,
            "include_inline_data": include_inline_data,
        });
        if let Some(branch) = &self.branch {
            body["branch"] = serde_json::Value::String(branch.clone());
        }
        if let Some(source_uri) = &self.source_uri {
            body["source_uri"] = serde_json::Value::String(source_uri.clone());
        }
        if let Some(timestamp) = self.source_commit_micros {
            body["source_commit_micros"] = serde_json::Value::Number(timestamp.into());
        }

        let mut request = self
            .client
            .post(&format!("/v1/table/{}/blob_locations/", self.identifier))
            .json(&body);
        if let Some(identity) = &self.expected_query_snapshot {
            request = request.header(QUERY_SNAPSHOT_HEADER, identity);
        }
        if let Some(freshness) = self.freshness {
            request = freshness.apply(request);
        }
        // A resolver 503 means a PE lost the plan after describe. Do not spend
        // transport retries on it: the caller falls back to proxy immediately.
        let (request_id, response) = self.client.send(request).await?;
        let response =
            RemoteTable::<S>::handle_table_not_found(&self.table_name, response, &request_id)
                .await?;
        let response = self
            .client
            .check_response(&request_id, response)
            .await
            .map_err(|error| {
                if let Error::Http {
                    source,
                    request_id,
                    status_code: Some(StatusCode::BAD_REQUEST),
                } = error
                {
                    Error::InvalidInput {
                        message: format!("blob_locations (request {request_id}): {source}"),
                    }
                } else {
                    error
                }
            })?;
        check_query_snapshot_response(self.expected_query_snapshot.as_deref(), &response)?;

        let version = response
            .headers()
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|version| *version != 0)
            .ok_or_else(|| Error::Http {
                source: "blob_locations response has no valid x-lancedb-version".into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        if let Some(expected) = self.version
            && expected != version
        {
            return Err(Error::Http {
                source: format!(
                    "blob_locations resolved version {version}, expected pinned version {expected}"
                )
                .into(),
                request_id,
                status_code: None,
            });
        }
        let identity = match &self.expected_query_snapshot {
            Some(expected) => Some(expected.clone()),
            None => Some(
                response
                    .headers()
                    .get(QUERY_SNAPSHOT_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| valid_query_snapshot_identity(value))
                    .map(str::to_owned)
                    .ok_or_else(|| Error::NotSupported {
                        message:
                            "Blob locations require a snapshot identity; upgrade every serving node"
                                .into(),
                    })?,
            ),
        };

        let body = response.text().await.err_to_http(request_id.clone())?;
        let parsed: BlobLocationsBody =
            serde_json::from_str(&body).map_err(|error| Error::Http {
                source: format!("failed to parse blob_locations response: {error}").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        if parsed.items.len() != row_ids.len() {
            return Err(Error::Http {
                source: format!(
                    "blob_locations returned {} items for {} row ids",
                    parsed.items.len(),
                    row_ids.len()
                )
                .into(),
                request_id: request_id.clone(),
                status_code: None,
            });
        }
        for item in parsed.items.iter().flatten() {
            match item {
                BlobLocationItem::Presigned {
                    source_index,
                    offset,
                    size,
                } => {
                    if *source_index >= parsed.sources.len() {
                        return Err(Error::Http {
                            source: format!(
                                "blob_locations item references source {source_index} of {}",
                                parsed.sources.len()
                            )
                            .into(),
                            request_id: request_id.clone(),
                            status_code: None,
                        });
                    }
                    offset.checked_add(*size).ok_or_else(|| Error::Http {
                        source: "blob_locations physical range overflows u64".into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    })?;
                }
                BlobLocationItem::Inline { size, data_base64 } => {
                    if data_base64.len() as u64 > size.saturating_mul(4) {
                        return Err(Error::Http {
                            source: "blob_locations inline payload is larger than its decoded size"
                                .into(),
                            request_id: request_id.clone(),
                            status_code: None,
                        });
                    }
                }
                BlobLocationItem::Proxy { .. } => {}
            }
        }
        log::debug!(
            "blob_locations resolved rows={} version={} elapsed_ms={:.3}",
            row_ids.len(),
            version,
            started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(ResolvedBlobLocations {
            sources: parsed.sources,
            items: parsed.items,
            version,
            identity,
        })
    }
}

fn decode_inline_data(data_base64: &str, expected_size: u64) -> Result<Bytes> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data_base64)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(data_base64))
        .map_err(|error| Error::InvalidInput {
            message: format!("blob_locations returned invalid inline base64: {error}"),
        })?;
    if bytes.len() as u64 != expected_size {
        return Err(Error::InvalidInput {
            message: format!(
                "blob_locations inline item decoded to {} bytes, expected {expected_size}",
                bytes.len()
            ),
        });
    }
    Ok(Bytes::from(bytes))
}

enum PresignedBlobState {
    Presigned {
        url: String,
        expires_at_unix_secs: Option<u64>,
        base_offset: u64,
    },
    Inline {
        bytes: Bytes,
    },
    Proxy,
}

enum PresignedReadError {
    Expired,
    Failed(Error),
}

struct PresignedBlobRangeRequester<S: HttpSend> {
    context: BlobLocationsContext<S>,
    row_id: u64,
    logical_size: u64,
    threshold_bytes: u64,
    proxy: Arc<dyn BlobRangeRequester>,
    state: RwLock<PresignedBlobState>,
    refresh_lock: Mutex<()>,
}

impl<S: HttpSend> std::fmt::Debug for PresignedBlobRangeRequester<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresignedBlobRangeRequester")
            .field("row_id", &self.row_id)
            .field("logical_size", &self.logical_size)
            .finish_non_exhaustive()
    }
}

impl<S: HttpSend> PresignedBlobRangeRequester<S> {
    fn new(
        context: BlobLocationsContext<S>,
        row_id: u64,
        logical_size: u64,
        threshold_bytes: u64,
        proxy: Arc<dyn BlobRangeRequester>,
        source: &BlobLocationSource,
        base_offset: u64,
    ) -> Self {
        Self {
            context,
            row_id,
            logical_size,
            threshold_bytes,
            proxy,
            state: RwLock::new(PresignedBlobState::Presigned {
                url: source.url.clone(),
                expires_at_unix_secs: source.expires_at_unix_secs,
                base_offset,
            }),
            refresh_lock: Mutex::new(()),
        }
    }

    async fn proxy_validated(
        &self,
        range: Range<u64>,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<BlobRangeResponse> {
        let mut response = self
            .proxy
            .request_range(range.clone(), mode, version)
            .await?;
        validate_partial_response(&response.response, range, self.logical_size)?;
        response.validated = true;
        Ok(response)
    }

    async fn refresh(&self) -> Result<()> {
        let _guard = self.refresh_lock.lock().await;
        let resolved = match self.context.resolve(&[self.row_id], false).await {
            Ok(resolved) => resolved,
            Err(error) if is_blob_locations_unavailable(&error) => {
                // The resolver lost its PE plan. Keep this handle usable by
                // switching it to the proxy route instead of failing the read.
                *self.state.write().await = PresignedBlobState::Proxy;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let item =
            resolved
                .items
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| Error::InvalidInput {
                    message: "presigned blob is no longer stored as a non-null value".into(),
                })?;
        let next = match item {
            BlobLocationItem::Presigned {
                source_index,
                offset,
                size,
            } => {
                if size != self.logical_size {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "presigned blob size changed from {} to {size} at the pinned version",
                            self.logical_size
                        ),
                    });
                }
                let source =
                    resolved
                        .sources
                        .get(source_index)
                        .ok_or_else(|| Error::InvalidInput {
                            message: format!(
                                "presigned blob refresh references source {source_index} of {}",
                                resolved.sources.len()
                            ),
                        })?;
                PresignedBlobState::Presigned {
                    url: source.url.clone(),
                    expires_at_unix_secs: source.expires_at_unix_secs,
                    base_offset: offset,
                }
            }
            BlobLocationItem::Inline { size, data_base64 } => PresignedBlobState::Inline {
                bytes: decode_inline_data(&data_base64, size)?,
            },
            BlobLocationItem::Proxy { size } => {
                if size != self.logical_size {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "proxied blob size changed from {} to {size} at the pinned version",
                            self.logical_size
                        ),
                    });
                }
                PresignedBlobState::Proxy
            }
        };
        *self.state.write().await = next;
        Ok(())
    }

    async fn direct_read(
        &self,
        url: &str,
        base_offset: u64,
        range: Range<u64>,
    ) -> std::result::Result<BlobRangeResponse, PresignedReadError> {
        let permit = self
            .context
            .client
            .blob_request_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("blob request semaphore is never closed");
        let start = base_offset.checked_add(range.start).ok_or_else(|| {
            PresignedReadError::Failed(Error::InvalidInput {
                message: "presigned physical range start overflows u64".into(),
            })
        })?;
        let end = base_offset.checked_add(range.end).ok_or_else(|| {
            PresignedReadError::Failed(Error::InvalidInput {
                message: "presigned physical range end overflows u64".into(),
            })
        })?;
        let request = self
            .context
            .client
            .object_store_get(url)
            .header(header::RANGE, format!("bytes={start}-{}", end - 1));
        let (request_id, response) = self
            .context
            .client
            .send_object_store(request)
            .await
            .map_err(PresignedReadError::Failed)?;
        if matches!(
            response.status(),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
        ) {
            return Err(PresignedReadError::Expired);
        }
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(PresignedReadError::Failed(Error::Http {
                source: format!(
                    "expected HTTP 206 Partial Content, got {}",
                    response.status()
                )
                .into(),
                request_id,
                status_code: Some(response.status()),
            }));
        }
        validate_physical_content_range(&response, range.clone(), base_offset, self.logical_size)
            .map_err(|error| PresignedReadError::Failed(error.into()))?;
        let content_range = response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        log::debug!(
            "blob_direct row={} content_range={} logical_range={}..{}",
            self.row_id,
            content_range,
            range.start,
            range.end
        );
        Ok(BlobRangeResponse {
            request_id,
            response,
            validated: true,
            _permit: Some(permit),
        })
    }
}

#[async_trait::async_trait]
impl<S: HttpSend> BlobRangeRequester for PresignedBlobRangeRequester<S> {
    async fn request_range(
        &self,
        range: Range<u64>,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<BlobRangeResponse> {
        if !matches!(mode, RangeRequestMode::DataRead) {
            return Err(Error::InvalidInput {
                message: "presigned blob handles only serve data reads".into(),
            });
        }
        let range_len = range.end.saturating_sub(range.start);
        if range_len < self.threshold_bytes {
            return self.proxy_validated(range, mode, version).await;
        }
        let mut refreshed = false;
        loop {
            let state = self.state.read().await;
            match &*state {
                PresignedBlobState::Proxy => {
                    drop(state);
                    return self.proxy_validated(range, mode, version).await;
                }
                PresignedBlobState::Inline { bytes } => {
                    let bytes = bytes.clone();
                    drop(state);
                    return inline_range_response(bytes, range, self.logical_size);
                }
                PresignedBlobState::Presigned {
                    url,
                    expires_at_unix_secs,
                    base_offset,
                } => {
                    let url = url.clone();
                    let expires = *expires_at_unix_secs;
                    let base_offset = *base_offset;
                    drop(state);
                    let now = unix_now_secs();
                    if expires.is_some_and(|expires| {
                        expires <= now.saturating_add(BLOB_PRESIGN_REFRESH_SKEW.as_secs())
                    }) && !refreshed
                    {
                        self.refresh().await?;
                        refreshed = true;
                        continue;
                    }
                    match self.direct_read(&url, base_offset, range.clone()).await {
                        Ok(response) => return Ok(response),
                        Err(PresignedReadError::Expired) if !refreshed => {
                            self.refresh().await?;
                            refreshed = true;
                        }
                        Err(PresignedReadError::Expired) => {
                            return Err(Error::InvalidInput {
                                message: "presigned blob URL expired after refresh".into(),
                            });
                        }
                        Err(PresignedReadError::Failed(error)) => return Err(error),
                    }
                }
            }
        }
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn inline_range_response(bytes: Bytes, range: Range<u64>, size: u64) -> Result<BlobRangeResponse> {
    let start = usize::try_from(range.start).map_err(|_| Error::InvalidInput {
        message: "inline blob range start exceeds addressable memory".into(),
    })?;
    let end = usize::try_from(range.end).map_err(|_| Error::InvalidInput {
        message: "inline blob range end exceeds addressable memory".into(),
    })?;
    let slice = bytes
        .get(start..end)
        .ok_or_else(|| Error::InvalidInput {
            message: "inline blob range exceeds the resolved value".into(),
        })?
        .to_vec();
    let response = http::Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{size}", range.start, range.end - 1),
        )
        .body(slice)
        .map_err(|error| Error::Other {
            message: format!("failed to build inline blob response: {error}"),
            source: None,
        })?;
    Ok(BlobRangeResponse {
        request_id: "blob-inline".into(),
        response: Response::from(response),
        validated: true,
        _permit: None,
    })
}

impl<S: HttpSend> RemoteTable<S> {
    async fn reject_list_blob_path_if_known(&self, column: &str) -> Result<()> {
        // A dotted path needs client-side validation because the server may
        // report an internal type mismatch when the path crosses a list. Use
        // an already cached schema for top-level list fields as well.
        // Guarded readers must validate the producing identity on the server
        // before consulting any schema resolved from a later logical binding.
        if self.expected_query_snapshot.is_none()
            && (column.contains('.') || self.schema_cache.try_get().is_some())
        {
            let schema = self.schema().await?;
            crate::blob::reject_list_blob_column(schema.as_ref(), column)?;
        }
        Ok(())
    }

    async fn explain_list_blob_bad_request(&self, column: &str, error: Error) -> Error {
        if self.expected_query_snapshot.is_some() {
            return error;
        }
        let is_bad_request = match &error {
            Error::Http {
                status_code: Some(code),
                ..
            } => *code == StatusCode::BAD_REQUEST || *code == StatusCode::UNPROCESSABLE_ENTITY,
            _ => false,
        };
        if is_bad_request
            && let Ok(schema) = self.schema().await
            && let Err(list_error) = crate::blob::reject_list_blob_column(schema.as_ref(), column)
        {
            return list_error;
        }
        error
    }

    /// Blob columns are marked in field metadata, which `describe` returns. Reading
    /// them from the cached schema needs no route of its own and no version gate.
    pub(super) async fn blob_columns_impl(&self) -> Result<Vec<String>> {
        let schema = self.schema().await?;
        Ok(crate::blob::blob_column_names(schema.as_ref()))
    }

    pub(super) async fn fetch_blobs_impl(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<LargeBinaryArray> {
        self.reject_list_blob_path_if_known(column).await?;
        // Empty requests do not require blob-route support.
        if row_ids.is_empty() {
            return Ok(LargeBinaryArray::from(Vec::<Option<&[u8]>>::new()));
        }
        if !self.server_version.support_blobs() {
            return Err(Error::NotSupported {
                message: "fetch_blobs is not supported by this LanceDB server.".into(),
            });
        }
        let mut read_snapshot = self.snapshot_read_state().await;
        let mut identity = self.expected_query_snapshot.clone();
        let mut allow_direct_fallback = true;
        if self.blob_locations_supported.load(Ordering::Relaxed) {
            match self
                .fetch_blobs_via_locations(column, row_ids, &mut read_snapshot, &mut identity)
                .await
            {
                Ok(Some(blobs)) => return Ok(blobs),
                Ok(None) => {
                    log::warn!(
                        "blob_locations is unavailable (503); falling back to the proxy whole-blob path for this batch"
                    );
                    allow_direct_fallback = false;
                }
                Err(error) => return Err(error),
            }
        }
        // A selection spanning requests must use one exact dataset version.
        // Resolve latest before the first chunk; a checked-out version is exact already.
        if row_ids.len() > MAX_FETCH_BLOBS_ROW_IDS && read_snapshot.version.is_none() {
            read_snapshot.version = Some(self.describe_read_snapshot(read_snapshot).await?.version);
        }
        let mut pending: Vec<&[u64]> = row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS).rev().collect();
        let mut blobs = BlobAccumulator::new(row_ids.len())?;
        while let Some(ids) = pending.pop() {
            match self
                .fetch_blobs_chunk(column, ids, read_snapshot, identity.as_deref(), &mut blobs)
                .await
            {
                Ok(()) => {}
                Err(error) if is_fetch_blobs_byte_limit_error(&error) => {
                    // A single-request call may turn into several requests after a
                    // byte-cap error. No bytes from the failed request were used.
                    if read_snapshot.version.is_none() {
                        read_snapshot.version =
                            Some(self.describe_read_snapshot(read_snapshot).await?.version);
                    }
                    if ids.len() == 1 {
                        // The whole-byte route cannot serve this blob. The Range route
                        // has no aggregate response limit and preserves null alignment.
                        let mut files = self
                            .fetch_blob_files_with_snapshot(
                                column,
                                ids,
                                read_snapshot,
                                allow_direct_fallback,
                                identity.clone(),
                            )
                            .await?;
                        let blob = match files.pop().unwrap() {
                            Some(file) => Some(file.read().await?),
                            None => None,
                        };
                        let blob = match blob {
                            Some(bytes) => LargeBinaryArray::new(
                                OffsetBuffer::new(vec![0, bytes.len() as i64].into()),
                                Buffer::from(bytes),
                                None,
                            ),
                            None => LargeBinaryArray::new_null(1),
                        };
                        blobs.append(&blob)?;
                    } else {
                        let mid = ids.len() / 2;
                        pending.push(&ids[mid..]);
                        pending.push(&ids[..mid]);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Ok(blobs.finish()?)
    }

    /// Resolve locations once per batch, read large presigned values directly,
    /// consume inline bytes in place, and proxy only the remaining rows.
    ///
    /// `Ok(None)` means the resolver returned 503 after possibly pinning a
    /// version and identity; the caller must reuse that pinned state on the
    /// proxy path and must not resolve again.
    async fn fetch_blobs_via_locations(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: &mut ReadSnapshot,
        identity: &mut Option<String>,
    ) -> Result<Option<LargeBinaryArray>> {
        let mut pending: Vec<&[u64]> = row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS).rev().collect();
        let mut blobs = BlobAccumulator::new(row_ids.len())?;
        while let Some(ids) = pending.pop() {
            let context = self.blob_locations_context(
                column,
                read_snapshot.version,
                self.branch.clone(),
                None,
                identity.clone(),
                Some(read_snapshot.freshness),
            );
            let resolved = match context.resolve(ids, true).await {
                Ok(resolved) => resolved,
                Err(error) if is_blob_locations_unavailable(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            if read_snapshot.version.is_none() {
                read_snapshot.version = Some(resolved.version);
            }
            if read_snapshot.version != Some(resolved.version) {
                return Err(Error::Http {
                    source: format!(
                        "blob_locations resolved version {} but this read is pinned to {:?}",
                        resolved.version, read_snapshot.version
                    )
                    .into(),
                    request_id: "blob-locations".into(),
                    status_code: None,
                });
            }
            if identity.is_none() {
                *identity = resolved.identity.clone();
            }
            let array = self
                .fetch_blobs_locations_batch(
                    column,
                    ids,
                    &resolved,
                    *read_snapshot,
                    identity.as_deref(),
                )
                .await?;
            blobs.append(&array)?;
        }
        Ok(Some(blobs.finish()?))
    }

    async fn fetch_blobs_locations_batch(
        &self,
        column: &str,
        row_ids: &[u64],
        resolved: &ResolvedBlobLocations,
        read_snapshot: ReadSnapshot,
        identity: Option<&str>,
    ) -> Result<LargeBinaryArray> {
        let mut values: Vec<Option<Bytes>> = vec![None; row_ids.len()];
        let mut nulls = vec![false; row_ids.len()];
        let mut proxy_indices = Vec::new();
        let mut direct = Vec::new();
        for (index, item) in resolved.items.iter().enumerate() {
            match item {
                None => nulls[index] = true,
                Some(BlobLocationItem::Inline { size, data_base64 }) => {
                    values[index] = Some(decode_inline_data(data_base64, *size)?);
                }
                Some(BlobLocationItem::Presigned {
                    source_index,
                    offset,
                    size,
                }) if *size >= self.client.blob_presign_threshold_bytes => {
                    direct.push((index, *source_index, *offset, *size));
                }
                Some(_) => proxy_indices.push(index),
            }
        }

        let proxy_array = if proxy_indices.is_empty() {
            None
        } else {
            let proxy_row_ids = proxy_indices
                .iter()
                .map(|index| row_ids[*index])
                .collect::<Vec<_>>();
            Some(
                self.fetch_blobs_proxy_array(
                    column,
                    &proxy_row_ids,
                    read_snapshot,
                    identity.map(str::to_owned),
                )
                .await?,
            )
        };

        let direct_futures = direct
            .into_iter()
            .map(|(index, source_index, offset, size)| {
                let source = resolved.sources[source_index].clone();
                let row_id = row_ids[index];
                let identity = identity.map(str::to_owned);
                async move {
                    let proxy = self.blob_range_requester_with_identity(
                        column,
                        row_id,
                        &read_snapshot,
                        identity.as_deref(),
                    );
                    let context = self.blob_locations_context(
                        column,
                        Some(resolved.version),
                        self.branch.clone(),
                        None,
                        identity,
                        None,
                    );
                    let requester = Arc::new(PresignedBlobRangeRequester::new(
                        context,
                        row_id,
                        size,
                        self.client.blob_presign_threshold_bytes,
                        proxy,
                        &source,
                        offset,
                    ));
                    let response = requester
                        .request_range(0..size, RangeRequestMode::DataRead, Some(resolved.version))
                        .await?;
                    let BlobRangeResponse {
                        request_id,
                        response,
                        validated: _,
                        _permit,
                    } = response;
                    let bytes = read_blob_body(response, size, request_id).await?;
                    Ok::<_, Error>((index, bytes))
                }
            });
        let direct_values = futures::stream::iter(direct_futures)
            .buffered(BLOB_REQUEST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for (index, bytes) in direct_values {
            values[index] = Some(bytes);
        }

        if let Some(proxy_array) = proxy_array {
            for (proxy_offset, index) in proxy_indices.into_iter().enumerate() {
                if proxy_array.is_null(proxy_offset) {
                    nulls[index] = true;
                } else {
                    values[index] = Some(Bytes::copy_from_slice(proxy_array.value(proxy_offset)));
                }
            }
        }

        let total_bytes = values
            .iter()
            .flatten()
            .map(Bytes::len)
            .try_fold(0usize, |total, length| total.checked_add(length));
        let mut builder = LargeBinaryBuilder::with_capacity(
            row_ids.len(),
            total_bytes.ok_or_else(|| Error::InvalidInput {
                message: "blob batch length exceeds addressable memory".into(),
            })?,
        );
        for index in 0..row_ids.len() {
            if nulls[index] {
                builder.append_null();
            } else {
                let value = values[index].take().ok_or_else(|| Error::Http {
                    source: format!("blob_locations left row {index} without a value").into(),
                    request_id: "blob-locations".into(),
                    status_code: None,
                })?;
                builder.append_value(value);
            }
        }
        Ok(builder.finish())
    }

    async fn fetch_blobs_proxy_array(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
        identity: Option<String>,
    ) -> Result<LargeBinaryArray> {
        let mut pending: Vec<&[u64]> = vec![row_ids];
        let mut parts = Vec::new();
        while let Some(ids) = pending.pop() {
            match self
                .fetch_blobs_array(column, ids, read_snapshot, identity.clone())
                .await
            {
                Ok(array) => parts.push(array),
                Err(error) if is_fetch_blobs_byte_limit_error(&error) && ids.len() > 1 => {
                    let mid = ids.len() / 2;
                    pending.push(&ids[mid..]);
                    pending.push(&ids[..mid]);
                }
                Err(error) if is_fetch_blobs_byte_limit_error(&error) => {
                    let mut files = self
                        .fetch_blob_files_with_snapshot(
                            column,
                            ids,
                            read_snapshot,
                            false,
                            identity.clone(),
                        )
                        .await?;
                    let blob = match files.pop().unwrap() {
                        Some(file) => Some(file.read().await?),
                        None => None,
                    };
                    parts.push(match blob {
                        Some(bytes) => LargeBinaryArray::new(
                            OffsetBuffer::new(vec![0, bytes.len() as i64].into()),
                            Buffer::from(bytes),
                            None,
                        ),
                        None => LargeBinaryArray::new_null(1),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        let mut blobs = BlobAccumulator::new(row_ids.len())?;
        for part in parts {
            blobs.append(&part)?;
        }
        Ok(blobs.finish()?)
    }

    async fn fetch_blobs_array(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
        identity: Option<String>,
    ) -> Result<LargeBinaryArray> {
        let mut blobs = BlobAccumulator::new(row_ids.len())?;
        self.fetch_blobs_chunk(
            column,
            row_ids,
            read_snapshot,
            identity.as_deref(),
            &mut blobs,
        )
        .await?;
        Ok(blobs.finish()?)
    }

    async fn fetch_blobs_chunk(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
        identity: Option<&str>,
        blobs: &mut BlobAccumulator,
    ) -> Result<()> {
        let mut body = serde_json::json!({
            "version": read_snapshot.version,
            "column": column,
            "row_ids": row_ids,
        });
        self.apply_branch_body(&mut body);

        let request = self
            .client
            .post(&format!("/v1/table/{}/fetch_blobs/", self.identifier))
            .json(&body);
        let expected_identity = identity.or(self.expected_query_snapshot.as_deref());
        let request = if let Some(identity) = expected_identity {
            request.header(QUERY_SNAPSHOT_HEADER, identity)
        } else {
            request
        };
        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        if response.status().is_success() {
            check_query_snapshot_response(expected_identity, &response)?;
        }
        let mut stream = match self.read_arrow_response(&request_id, response).await {
            Ok(stream) => stream,
            Err(error) => return Err(self.explain_list_blob_bad_request(column, error).await),
        };

        let mut rows = 0;
        while let Some(batch) = stream.try_next().await? {
            let blob_column = batch.column_by_name(column).ok_or_else(|| Error::Http {
                source: format!("fetch_blobs response is missing the '{column}' column").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
            // The server returns LargeBinary today. Accept the other binary types so a
            // server that switches encodings does not break older clients.
            if !matches!(
                blob_column.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) {
                return Err(Error::Http {
                    source: format!(
                        "fetch_blobs response column has type {}, expected Binary, LargeBinary, or BinaryView",
                        blob_column.data_type()
                    )
                    .into(),
                    request_id: request_id.clone(),
                    status_code: None,
                });
            }
            let array = arrow::compute::cast(blob_column, &DataType::LargeBinary)?;
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("casting to LargeBinary returns LargeBinary");
            rows += array.len();
            blobs.append(array)?;
        }
        if rows != row_ids.len() {
            return Err(Error::Http {
                source: format!(
                    "fetch_blobs returned {} rows for {} row ids",
                    rows,
                    row_ids.len()
                )
                .into(),
                request_id,
                status_code: None,
            });
        }
        Ok(())
    }

    /// Materialize ranges using the protocol advertised by a successful describe.
    pub(super) async fn fetch_blob_ranges_impl(
        &self,
        column: &str,
        requests: &[BlobRangeRequest],
    ) -> Result<LargeBinaryArray> {
        if requests.is_empty() {
            return Ok(LargeBinaryArray::from(Vec::<Option<&[u8]>>::new()));
        }
        for request in requests {
            request
                .offset
                .checked_add(request.length)
                .ok_or_else(|| Error::InvalidInput {
                    message: "blob range offset + length overflows u64".into(),
                })?;
        }
        self.reject_list_blob_path_if_known(column).await?;
        if !self.blob_ranges_supported.load(Ordering::Relaxed) {
            return self.fetch_blob_ranges_legacy(column, requests).await;
        }
        let snapshot = self.snapshot_read_state().await;
        let mut identity = self.expected_query_snapshot.clone();
        if self.blob_locations_supported.load(Ordering::Relaxed)
            && requests
                .iter()
                .any(|request| request.length >= self.client.blob_presign_threshold_bytes)
        {
            let mut pinned = snapshot;
            match self
                .resolve_blob_range_locations(column, requests, &mut pinned, &mut identity)
                .await
            {
                Ok(Some(locations)) => {
                    return self
                        .fetch_blob_ranges_mixed(column, requests, pinned, identity, locations)
                        .await;
                }
                Ok(None) => {
                    // The QN advertised direct locations, but a PE lost the
                    // plan after describe. Reuse any version/identity pinned by
                    // earlier resolver partitions and retry this operation on
                    // the proxy path, exactly once.
                    log::warn!(
                        "blob_locations is unavailable (503); falling back to the proxy range path for this batch"
                    );
                    return self
                        .fetch_blob_ranges_proxy(column, requests, pinned, identity)
                        .await;
                }
                Err(error) => return Err(error),
            }
        }
        self.fetch_blob_ranges_proxy(column, requests, snapshot, identity)
            .await
    }

    /// Resolve the rows that carry at least one individually large range.
    ///
    /// Only these rows need locations; small-only batches never reach this
    /// method, so they keep their existing proxy round trips.
    async fn resolve_blob_range_locations(
        &self,
        column: &str,
        requests: &[BlobRangeRequest],
        snapshot: &mut ReadSnapshot,
        identity: &mut Option<String>,
    ) -> Result<Option<HashMap<u64, BlobRangeLocation>>> {
        let mut row_ids = requests
            .iter()
            .filter(|request| request.length >= self.client.blob_presign_threshold_bytes)
            .map(|request| request.row_id)
            .collect::<Vec<_>>();
        row_ids.sort_unstable();
        row_ids.dedup();
        let mut locations = HashMap::with_capacity(row_ids.len());
        for chunk in row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS) {
            let context = self.blob_locations_context(
                column,
                snapshot.version,
                self.branch.clone(),
                None,
                identity.clone(),
                Some(snapshot.freshness),
            );
            let resolved = match context.resolve(chunk, false).await {
                Ok(resolved) => resolved,
                Err(error) if is_blob_locations_unavailable(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            if snapshot.version.is_none() {
                snapshot.version = Some(resolved.version);
            }
            if snapshot.version != Some(resolved.version) {
                return Err(Error::Http {
                    source: format!(
                        "blob_locations resolved version {} but this read is pinned to {:?}",
                        resolved.version, snapshot.version
                    )
                    .into(),
                    request_id: "blob-locations".into(),
                    status_code: None,
                });
            }
            if identity.is_none() {
                *identity = resolved.identity.clone();
            }
            for (row_id, item) in chunk.iter().zip(resolved.items.iter()) {
                let location = match item {
                    None => BlobRangeLocation::Null,
                    Some(BlobLocationItem::Presigned {
                        source_index,
                        offset,
                        size,
                    }) => {
                        let source =
                            resolved
                                .sources
                                .get(*source_index)
                                .cloned()
                                .ok_or_else(|| Error::InvalidInput {
                                    message: format!(
                                        "blob_locations references source {source_index} of {}",
                                        resolved.sources.len()
                                    ),
                                })?;
                        BlobRangeLocation::Presigned {
                            source,
                            offset: *offset,
                            size: *size,
                        }
                    }
                    Some(BlobLocationItem::Inline { .. })
                    | Some(BlobLocationItem::Proxy { .. }) => BlobRangeLocation::Proxy,
                };
                locations.insert(*row_id, location);
            }
        }
        Ok(Some(locations))
    }

    async fn fetch_blob_ranges_mixed(
        &self,
        column: &str,
        requests: &[BlobRangeRequest],
        snapshot: ReadSnapshot,
        identity: Option<String>,
        locations: HashMap<u64, BlobRangeLocation>,
    ) -> Result<LargeBinaryArray> {
        let threshold = self.client.blob_presign_threshold_bytes;
        let mut values: Vec<Option<Bytes>> = vec![None; requests.len()];
        let mut nulls = vec![false; requests.len()];
        let mut proxy_indices = Vec::new();
        let mut proxy_requests = Vec::new();
        let mut direct = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            if request.length < threshold {
                proxy_indices.push(index);
                proxy_requests.push(*request);
                continue;
            }
            match locations.get(&request.row_id) {
                Some(BlobRangeLocation::Null) => nulls[index] = true,
                Some(BlobRangeLocation::Proxy) => {
                    proxy_indices.push(index);
                    proxy_requests.push(*request);
                }
                Some(BlobRangeLocation::Presigned {
                    source,
                    offset,
                    size,
                }) => {
                    let end = request.offset.checked_add(request.length).ok_or_else(|| {
                        Error::InvalidInput {
                            message: "blob range offset + length overflows u64".into(),
                        }
                    })?;
                    if end > *size {
                        return Err(Error::InvalidInput {
                            message: format!(
                                "blob range end {end} exceeds blob size {size} for row {}",
                                request.row_id
                            ),
                        });
                    }
                    if request.length == 0 {
                        values[index] = Some(Bytes::new());
                    } else {
                        direct.push((index, *request, source.clone(), *offset, *size));
                    }
                }
                None => {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "blob range row {} was not resolved for a large range",
                            request.row_id
                        ),
                    });
                }
            }
        }

        let direct_futures = direct
            .into_iter()
            .map(|(index, request, source, offset, size)| {
                let proxy = self.blob_range_requester_with_identity(
                    column,
                    request.row_id,
                    &snapshot,
                    identity.as_deref(),
                );
                let context = self.blob_locations_context(
                    column,
                    snapshot.version,
                    self.branch.clone(),
                    None,
                    identity.clone(),
                    None,
                );
                async move {
                    let requester = PresignedBlobRangeRequester::new(
                        context,
                        request.row_id,
                        size,
                        threshold,
                        proxy,
                        &source,
                        offset,
                    );
                    let response = requester
                        .request_range(
                            request.offset..request.offset + request.length,
                            RangeRequestMode::DataRead,
                            snapshot.version,
                        )
                        .await?;
                    let BlobRangeResponse {
                        request_id,
                        response,
                        validated: _,
                        _permit,
                    } = response;
                    let bytes = read_blob_body(response, request.length, request_id).await?;
                    Ok::<_, Error>((index, bytes))
                }
            });
        let direct_values = futures::stream::iter(direct_futures)
            .buffered(BLOB_REQUEST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for (index, bytes) in direct_values {
            values[index] = Some(bytes);
        }

        if !proxy_indices.is_empty() {
            let proxy_array = self
                .fetch_blob_ranges_proxy(column, &proxy_requests, snapshot, identity)
                .await?;
            for (position, index) in proxy_indices.into_iter().enumerate() {
                if proxy_array.is_null(position) {
                    nulls[index] = true;
                } else {
                    values[index] = Some(Bytes::copy_from_slice(proxy_array.value(position)));
                }
            }
        }

        let total_bytes = values
            .iter()
            .flatten()
            .map(Bytes::len)
            .try_fold(0usize, |total, length| total.checked_add(length));
        let mut builder = LargeBinaryBuilder::with_capacity(
            requests.len(),
            total_bytes.ok_or_else(|| Error::InvalidInput {
                message: "blob range batch length exceeds addressable memory".into(),
            })?,
        );
        for index in 0..requests.len() {
            if nulls[index] {
                builder.append_null();
            } else {
                let value = values[index].take().ok_or_else(|| Error::Http {
                    source: format!("blob range {index} has no resolved value").into(),
                    request_id: "blob-locations".into(),
                    status_code: None,
                })?;
                builder.append_value(value);
            }
        }
        Ok(builder.finish())
    }

    async fn fetch_blob_ranges_proxy(
        &self,
        column: &str,
        requests: &[BlobRangeRequest],
        mut snapshot: ReadSnapshot,
        mut identity: Option<String>,
    ) -> Result<LargeBinaryArray> {
        let mut blobs = BlobAccumulator::new(requests.len())?;
        if requests.is_empty() {
            return Ok(blobs.finish()?);
        }
        let mut index = 0;
        let mut consumed = 0;
        let mut ranges = Vec::new();
        ranges
            .try_reserve_exact(MAX_FETCH_BLOBS_ROW_IDS)
            .map_err(|error| Error::InvalidInput {
                message: format!("cannot allocate blob range requests: {error}"),
            })?;
        while index < requests.len() {
            ranges.clear();
            let continuation = consumed != 0;
            let mut budget = MAX_FETCH_BLOB_RANGE_BYTES;
            while index < requests.len() && ranges.len() < MAX_FETCH_BLOBS_ROW_IDS {
                let request = requests[index];
                let remaining = request.length - consumed;
                if budget == 0 && remaining != 0 {
                    break;
                }
                let length = remaining.min(budget);
                // The full range was checked above; all subranges fit within it.
                ranges.push(BlobRangeRequest::new(
                    request.row_id,
                    request.offset + consumed,
                    length,
                ));
                consumed += length;
                budget -= length;
                if consumed == request.length {
                    index += 1;
                    consumed = 0;
                } else {
                    break;
                }
            }
            let array = self
                .fetch_blob_ranges_chunk(column, &ranges, &mut snapshot, &mut identity)
                .await?;
            // A null has no bytes to partition. Avoid sending arbitrarily many
            // empty fragments for a null with a very large requested length.
            if consumed != 0 && array.is_null(array.len() - 1) {
                index += 1;
                consumed = 0;
            }
            if continuation {
                blobs.append_continuation(&array)?;
            } else {
                blobs.append(&array)?;
            }
        }
        Ok(blobs.finish()?)
    }

    async fn fetch_blob_ranges_legacy(
        &self,
        column: &str,
        requests: &[BlobRangeRequest],
    ) -> Result<LargeBinaryArray> {
        let row_ids = requests
            .iter()
            .map(|request| request.row_id)
            .collect::<Vec<_>>();
        let files = self
            .fetch_blob_files_with_route(
                column,
                &row_ids,
                false,
                None,
                self.expected_query_snapshot.clone(),
            )
            .await?;
        let mut builder = LargeBinaryBuilder::with_capacity(requests.len(), 0);
        for (file, request) in files.into_iter().zip(requests) {
            match file {
                Some(file) => {
                    // Overflow was checked before selecting either protocol.
                    let bytes = file
                        .read_range(request.offset..request.offset + request.length)
                        .await?;
                    builder.append_value(bytes);
                }
                None => builder.append_null(),
            }
        }
        Ok(builder.finish())
    }

    async fn fetch_blob_ranges_chunk(
        &self,
        column: &str,
        ranges: &[BlobRangeRequest],
        snapshot: &mut ReadSnapshot,
        identity: &mut Option<String>,
    ) -> Result<LargeBinaryArray> {
        let _permit = self
            .client
            .blob_request_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("blob request semaphore is never closed");
        let body = serde_json::json!({
            "column": column,
            "version": snapshot.version,
            "branch": self.branch,
            "ranges": ranges,
        });
        let request = self
            .client
            .post(&format!("/v1/table/{}/fetch_blob_ranges/", self.identifier))
            .json(&body);
        let request = if let Some(expected) = identity.as_deref() {
            request.header(super::QUERY_SNAPSHOT_HEADER, expected)
        } else {
            request
        };
        let (request_id, response) = self
            .send_with_freshness(request, false, snapshot.freshness)
            .await?;
        // The range operation defines 400 as invalid input. Let the established
        // checker decode structured/plain server errors before normalizing it.
        let response = self
            .check_table_response(&request_id, response)
            .await
            .map_err(|error| {
                if let Error::Http {
                    source,
                    request_id,
                    status_code: Some(StatusCode::BAD_REQUEST),
                } = error
                {
                    Error::InvalidInput {
                        message: format!("fetch_blob_ranges (request {request_id}): {source}"),
                    }
                } else {
                    error
                }
            })?;
        let invalid = |message: String| Error::Http {
            source: format!("fetch_blob_ranges: {message}").into(),
            request_id: request_id.clone(),
            status_code: None,
        };
        let version = response.headers().get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|version| *version != 0)
            .ok_or_else(|| invalid("missing or invalid x-lancedb-version; a server supporting the batched range endpoint is required".into()))?;
        if snapshot.version.is_some_and(|expected| expected != version) {
            return Err(invalid(format!(
                "response version {version} differs from requested version {:?}",
                snapshot.version
            )));
        }
        // Do not decode or accept any payload before establishing/verifying the
        // identity. A later partition can never replace the original expectation.
        check_query_snapshot_response(identity.as_deref(), &response)?;
        if identity.is_none() {
            let actual = response.headers().get(super::QUERY_SNAPSHOT_HEADER)
                .and_then(|value| value.to_str().ok())
                .filter(|value| valid_query_snapshot_identity(value))
                .ok_or_else(|| Error::NotSupported {
                    message: "Batched Blob ranges require a snapshot identity; upgrade every serving node".into(),
                })?;
            *identity = Some(actual.to_owned());
        }
        snapshot.version = Some(version);
        // This endpoint's contract is stream framing. Keep the bounded, fallible
        // IPC path from #4979 instead of buffering an Arrow file response.
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case(crate::remote::ARROW_STREAM_CONTENT_TYPE)
        {
            return Err(invalid(format!(
                "expected Arrow IPC stream, got '{content_type}'"
            )));
        }
        let mut stream = super::ipc_stream::read_ipc_stream(
            response.bytes_stream(),
            request_id.clone(),
            Some(version),
        )
        .await?;
        let schema = stream.schema();
        if !matches!(
            schema
                .field_with_name("value")
                .map(|field| field.data_type()),
            Ok(DataType::LargeBinary)
        ) {
            return Err(invalid(
                "response requires a LargeBinary column named 'value'".into(),
            ));
        }
        let mut blobs = BlobAccumulator::new(ranges.len())?;
        let mut rows = 0usize;
        let mut bytes = 0u64;
        while let Some(batch) = stream.try_next().await? {
            let array = batch
                .column_by_name("value")
                .and_then(|column| column.as_any().downcast_ref::<LargeBinaryArray>())
                .ok_or_else(|| {
                    invalid("response requires a LargeBinary column named 'value'".into())
                })?;
            let end = rows
                .checked_add(array.len())
                .filter(|end| *end <= ranges.len())
                .ok_or_else(|| invalid("response has more rows than requested".into()))?;
            let offsets = array.value_offsets();
            bytes = bytes
                .checked_add((offsets[array.len()] - offsets[0]) as u64)
                .filter(|bytes| *bytes <= MAX_FETCH_BLOB_RANGE_BYTES)
                .ok_or_else(|| invalid("response exceeds 64 MiB of range bytes".into()))?;
            for (i, range) in ranges[rows..end].iter().enumerate() {
                if array.is_valid(i) && array.value_length(i) as u64 != range.length {
                    return Err(invalid(format!(
                        "row {} returned {} bytes for requested length {}",
                        rows + i,
                        array.value_length(i),
                        range.length
                    )));
                }
            }
            blobs.append(array)?;
            rows = end;
        }
        if rows != ranges.len() {
            return Err(invalid(format!(
                "returned {rows} rows for {} ranges",
                ranges.len()
            )));
        }
        Ok(blobs.finish()?)
    }

    /// Open seekable handles for `row_ids`.
    ///
    /// Descriptors for the requested rows come from one `_rowid` take query per
    /// [`MAX_FETCH_BLOBS_ROW_IDS`] unique row ids, all read at one exact table
    /// version. Each handle is sized from its descriptor and reads that
    /// version. Only external blobs whose descriptor records no size are
    /// probed, because their length is the whole object's.
    pub(super) async fn fetch_blob_files_impl(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<Vec<Option<BlobFile>>> {
        self.fetch_blob_files_with_route(
            column,
            row_ids,
            true,
            None,
            self.expected_query_snapshot.clone(),
        )
        .await
    }

    async fn fetch_blob_files_with_route(
        &self,
        column: &str,
        row_ids: &[u64],
        use_direct: bool,
        read_snapshot: Option<ReadSnapshot>,
        identity: Option<String>,
    ) -> Result<Vec<Option<BlobFile>>> {
        self.reject_list_blob_path_if_known(column).await?;
        // Empty requests do not require blob-route support.
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_blob_files_supported()?;

        let mut read_snapshot = match read_snapshot {
            Some(read_snapshot) => read_snapshot,
            None => self.snapshot_read_state().await,
        };
        let mut identity = identity.or_else(|| self.expected_query_snapshot.clone());
        let sizes = match self
            .take_blob_descriptor_sizes(column, row_ids, &mut read_snapshot)
            .await
        {
            Ok(sizes) => sizes,
            Err(error) => return Err(self.explain_list_blob_bad_request(column, error).await),
        };
        let direct_candidates =
            if use_direct && self.blob_locations_supported.load(Ordering::Relaxed) {
                let mut candidates = sizes
                    .iter()
                    .filter_map(|(row_id, size)| match size {
                        DescriptorSize::Known { kind, size }
                            if *size >= self.client.blob_presign_threshold_bytes
                                && is_direct_blob_kind(*kind) =>
                        {
                            Some(*row_id)
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                candidates.sort_unstable();
                candidates
            } else {
                Vec::new()
            };
        let resolved_handles = if direct_candidates.is_empty() {
            HashMap::new()
        } else {
            match self
                .resolve_blob_file_candidates(
                    column,
                    &direct_candidates,
                    &mut read_snapshot,
                    &mut identity,
                )
                .await
            {
                Ok(Some(handles)) => handles,
                Ok(None) => {
                    log::warn!(
                        "blob_locations is unavailable (503); keeping this handle batch on the proxy path"
                    );
                    HashMap::new()
                }
                Err(error) => return Err(self.explain_list_blob_bad_request(column, error).await),
            }
        };

        let version = read_snapshot.version;
        let mut files = Vec::with_capacity(row_ids.len());
        let mut probe_indices = Vec::new();
        let mut probe_requesters = Vec::new();
        for (index, row_id) in row_ids.iter().enumerate() {
            if let Some(handle) = resolved_handles.get(row_id) {
                if let DescriptorSize::Known { size, .. } = sizes[row_id]
                    && size != handle.size
                {
                    return Err(Error::Http {
                        source: format!(
                            "blob_locations resolved row {row_id} as {} bytes, descriptor says {size}",
                            handle.size
                        )
                        .into(),
                        request_id: "blob-locations".into(),
                        status_code: None,
                    });
                }
                files.push(
                    handle.requester.clone().map(|requester| {
                        RemoteBlobFile::new(requester, handle.size, version).into()
                    }),
                );
                continue;
            }
            match sizes[row_id] {
                DescriptorSize::Null => files.push(None),
                DescriptorSize::Known { size, .. } => {
                    let requester = self.blob_range_requester_with_identity(
                        column,
                        *row_id,
                        &read_snapshot,
                        identity.as_deref(),
                    );
                    files.push(Some(RemoteBlobFile::new(requester, size, version).into()));
                }
                DescriptorSize::Unresolved => {
                    files.push(None);
                    probe_indices.push(index);
                    probe_requesters.push(self.blob_range_requester_with_identity(
                        column,
                        *row_id,
                        &read_snapshot,
                        identity.as_deref(),
                    ));
                }
            }
        }
        let probed = match probe_blob_files(probe_requesters).await {
            Ok(files) => files,
            Err(error) => return Err(self.explain_list_blob_bad_request(column, error).await),
        };
        for (index, file) in probe_indices.into_iter().zip(probed) {
            files[index] = file;
        }
        Ok(files)
    }

    /// Resolve direct handles for descriptor rows whose logical size warrants
    /// presigning. Each item must agree with the descriptor size.
    async fn resolve_blob_file_candidates(
        &self,
        column: &str,
        candidate_row_ids: &[u64],
        read_snapshot: &mut ReadSnapshot,
        identity: &mut Option<String>,
    ) -> Result<Option<HashMap<u64, ResolvedBlobHandle>>> {
        let mut handles = HashMap::with_capacity(candidate_row_ids.len());
        for chunk in candidate_row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS) {
            let context = self.blob_locations_context(
                column,
                read_snapshot.version,
                self.branch.clone(),
                None,
                identity.clone(),
                Some(read_snapshot.freshness),
            );
            let resolved = match context.resolve(chunk, false).await {
                Ok(resolved) => resolved,
                Err(error) if is_blob_locations_unavailable(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            if read_snapshot.version.is_none() {
                read_snapshot.version = Some(resolved.version);
            }
            if read_snapshot.version != Some(resolved.version) {
                return Err(Error::Http {
                    source: format!(
                        "blob_locations resolved version {} but this read is pinned to {:?}",
                        resolved.version, read_snapshot.version
                    )
                    .into(),
                    request_id: "blob-locations".into(),
                    status_code: None,
                });
            }
            if identity.is_none() {
                *identity = resolved.identity.clone();
            }
            for (row_id, item) in chunk.iter().zip(resolved.items.iter()) {
                let item = item.as_ref().ok_or_else(|| Error::InvalidInput {
                    message: format!(
                        "blob_locations returned null for non-null descriptor row {row_id}"
                    ),
                })?;
                let size = match item {
                    BlobLocationItem::Presigned { size, .. }
                    | BlobLocationItem::Inline { size, .. }
                    | BlobLocationItem::Proxy { size } => *size,
                };
                let requester = match item {
                    BlobLocationItem::Presigned {
                        source_index,
                        offset,
                        size,
                    } => {
                        let source = resolved.sources.get(*source_index).ok_or_else(|| {
                            Error::InvalidInput {
                                message: format!(
                                    "blob_locations references source {source_index} of {}",
                                    resolved.sources.len()
                                ),
                            }
                        })?;
                        let proxy = self.blob_range_requester_with_identity(
                            column,
                            *row_id,
                            read_snapshot,
                            identity.as_deref(),
                        );
                        let context = self.blob_locations_context(
                            column,
                            Some(resolved.version),
                            self.branch.clone(),
                            None,
                            identity.clone(),
                            None,
                        );
                        Some(Arc::new(PresignedBlobRangeRequester::new(
                            context,
                            *row_id,
                            *size,
                            self.client.blob_presign_threshold_bytes,
                            proxy,
                            source,
                            *offset,
                        )) as Arc<dyn BlobRangeRequester>)
                    }
                    BlobLocationItem::Inline { .. } | BlobLocationItem::Proxy { .. } => {
                        Some(self.blob_range_requester_with_identity(
                            column,
                            *row_id,
                            read_snapshot,
                            identity.as_deref(),
                        ))
                    }
                };
                handles.insert(*row_id, ResolvedBlobHandle { size, requester });
            }
        }
        Ok(Some(handles))
    }

    pub(super) async fn open_blob_file_impl(
        &self,
        descriptor: &BlobDescriptor,
    ) -> Result<BlobFile> {
        descriptor.validate()?;
        self.ensure_blob_files_supported()?;
        let source = &descriptor.source;
        let snapshot = ReadSnapshot {
            version: Some(source.version),
            freshness_state: FreshnessState {
                pinned: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let proxy_requester = self.blob_range_requester_with_source(
            &source.column,
            source.row_reference,
            &snapshot,
            Some(source),
        );
        let descriptor_kind = BlobKind::try_from(descriptor.kind).unwrap_or(BlobKind::External);
        let direct_candidate = self.blob_locations_supported.load(Ordering::Relaxed)
            && descriptor.size >= self.client.blob_presign_threshold_bytes
            && is_direct_blob_kind(descriptor_kind);
        if direct_candidate
            && let Some(file) = self
                .open_blob_file_from_locations(descriptor, source, snapshot)
                .await?
        {
            return Ok(file);
        }
        let size = if descriptor.kind == 3 && descriptor.size == 0 {
            let metadata = proxy_requester
                .request_range(0..0, RangeRequestMode::MetadataRead, Some(source.version))
                .await?;
            if metadata.response.status() != StatusCode::OK {
                return Err(Error::InvalidInput {
                    message: "non-null blob descriptor did not resolve to a stored value".into(),
                });
            }
            metadata
                .response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| Error::InvalidInput {
                    message: "blob metadata response has no valid Content-Length".into(),
                })?
        } else {
            descriptor.size
        };
        Ok(RemoteBlobFile::new(proxy_requester, size, Some(source.version)).into())
    }

    async fn open_blob_file_from_locations(
        &self,
        descriptor: &BlobDescriptor,
        source: &BlobDescriptorSource,
        snapshot: ReadSnapshot,
    ) -> Result<Option<BlobFile>> {
        let context = self.blob_locations_context_for_source(source);
        let resolved = match context.resolve(&[source.row_reference], false).await {
            Ok(resolved) => resolved,
            Err(Error::Http {
                status_code: Some(StatusCode::SERVICE_UNAVAILABLE),
                ..
            }) => {
                log::warn!(
                    "blob_locations is unavailable (503); opening this descriptor on the proxy path"
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let proxy_requester = self.blob_range_requester_with_source_and_identity(
            &source.column,
            source.row_reference,
            &snapshot,
            Some(source),
            resolved
                .identity
                .clone()
                .or_else(|| self.expected_query_snapshot.clone()),
        );
        let item =
            resolved
                .items
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| Error::InvalidInput {
                    message: "non-null blob descriptor did not resolve to a stored value".into(),
                })?;
        let size = match &item {
            BlobLocationItem::Presigned { size, .. }
            | BlobLocationItem::Inline { size, .. }
            | BlobLocationItem::Proxy { size } => *size,
        };
        if size != descriptor.size {
            return Err(Error::InvalidInput {
                message: format!(
                    "blob descriptor size {} differs from resolved size {size}",
                    descriptor.size
                ),
            });
        }
        let requester: Arc<dyn BlobRangeRequester> = match item {
            BlobLocationItem::Presigned {
                source_index,
                offset,
                size,
            } => {
                let source_info =
                    resolved
                        .sources
                        .get(source_index)
                        .ok_or_else(|| Error::InvalidInput {
                            message: format!(
                                "blob_locations references source {source_index} of {}",
                                resolved.sources.len()
                            ),
                        })?;
                Arc::new(PresignedBlobRangeRequester::new(
                    self.blob_locations_context_for_source(source),
                    source.row_reference,
                    size,
                    self.client.blob_presign_threshold_bytes,
                    proxy_requester,
                    source_info,
                    offset,
                ))
            }
            BlobLocationItem::Inline { .. } | BlobLocationItem::Proxy { .. } => proxy_requester,
        };
        Ok(Some(
            RemoteBlobFile::new(requester, size, Some(source.version)).into(),
        ))
    }

    /// Probe each row once for its size.
    ///
    /// Serves callers that must not issue a descriptor query, such as a single
    /// blob that the whole-byte route could not return. Direct resolution is
    /// only used when the caller wants it and the server advertises it.
    async fn fetch_blob_files_with_snapshot(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
        prefer_direct: bool,
        identity: Option<String>,
    ) -> Result<Vec<Option<BlobFile>>> {
        if prefer_direct && self.blob_locations_supported.load(Ordering::Relaxed) {
            return self
                .fetch_blob_files_with_route(column, row_ids, true, Some(read_snapshot), identity)
                .await;
        }
        let requesters = row_ids
            .iter()
            .map(|row_id| {
                self.blob_range_requester_with_identity(
                    column,
                    *row_id,
                    &read_snapshot,
                    identity.as_deref(),
                )
            })
            .collect();
        match probe_blob_files(requesters).await {
            Ok(files) => Ok(files),
            Err(error) => Err(self.explain_list_blob_bad_request(column, error).await),
        }
    }

    /// Read the blob descriptor of every unique row in `row_ids`.
    ///
    /// Pins `read_snapshot` to the version the first take read, so later chunks
    /// and the returned handles all see one table version.
    async fn take_blob_descriptor_sizes(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: &mut ReadSnapshot,
    ) -> Result<HashMap<u64, DescriptorSize>> {
        let mut unique_row_ids = row_ids.to_vec();
        unique_row_ids.sort_unstable();
        unique_row_ids.dedup();

        let mut sizes = HashMap::with_capacity(unique_row_ids.len());
        for chunk in unique_row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS) {
            let (mut chunk_sizes, response_version) = self
                .take_blob_descriptor_chunk(column, chunk, *read_snapshot)
                .await?;
            if read_snapshot.version.is_none() {
                match response_version {
                    Some(version) => read_snapshot.version = Some(version),
                    None => {
                        // A server that does not report its read version may have read
                        // an older version than a describe now returns, so the take is
                        // repeated at the described version.
                        read_snapshot.version =
                            Some(self.describe_read_snapshot(*read_snapshot).await?.version);
                        (chunk_sizes, _) = self
                            .take_blob_descriptor_chunk(column, chunk, *read_snapshot)
                            .await?;
                    }
                }
            }
            sizes.extend(chunk_sizes);
        }
        let found = unique_row_ids
            .iter()
            .filter(|row_id| sizes.contains_key(row_id))
            .count();
        if found < unique_row_ids.len() {
            return Err(Error::InvalidInput {
                message: format!(
                    "blob read for column '{column}' requested {} row ids but only {found} exist \
                     in the table; pass row ids collected from this table. If these were row \
                     positions, call take_offsets(offsets, with_row_id=True) first and pass the \
                     resulting row ids",
                    unique_row_ids.len()
                ),
            });
        }
        Ok(sizes)
    }

    /// Take one chunk of descriptors, also returning the version the server
    /// reported reading.
    async fn take_blob_descriptor_chunk(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
    ) -> Result<(HashMap<u64, DescriptorSize>, Option<u64>)> {
        let query = AnyQuery::Query(QueryRequest {
            filter: Some(QueryFilter::Datafusion(
                col(ROW_ID).in_list(row_ids.iter().map(|id| lit(*id)).collect(), false),
            )),
            select: Select::columns(&[column]),
            with_row_id: true,
            // Row ids name base-table rows, and the RTE scanner has no stable `_rowid`.
            use_rte: Some(false),
            ..Default::default()
        });
        let body = self
            .prepare_query_bodies(&query, read_snapshot.version)?
            .pop()
            .expect("a plain query has one body");
        let request = self
            .client
            .post(&format!("/v1/table/{}/query/", self.identifier))
            .json(&body);
        // The query route opens the requested version and reports the identity of
        // the dataset it actually read. It does not validate the request identity,
        // so accept descriptors only after comparing the response echo.
        let request = if let Some(identity) = &self.expected_query_snapshot {
            request.header(QUERY_SNAPSHOT_HEADER, identity)
        } else {
            request
        };
        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        if response.status().is_success() {
            check_query_snapshot_response(self.expected_query_snapshot.as_deref(), &response)?;
        }
        let response_version = response
            .headers()
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        if let (Some(expected), Some(actual)) = (read_snapshot.version, response_version)
            && expected != actual
        {
            return Err(Error::Http {
                source: format!(
                    "blob descriptor take read version {actual}, expected pinned version {expected}"
                )
                .into(),
                request_id,
                status_code: None,
            });
        }
        let mut stream = self.read_arrow_response(&request_id, response).await?;

        let mut sizes = HashMap::with_capacity(row_ids.len());
        let invalid_response = |message: String| Error::Http {
            source: message.into(),
            request_id: request_id.clone(),
            status_code: None,
        };
        while let Some(batch) = stream.try_next().await? {
            let taken_row_ids = batch
                .column_by_name(ROW_ID)
                .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| {
                    invalid_response(format!("blob descriptor take is missing UInt64 '{ROW_ID}'"))
                })?;
            let values = batch.column_by_name(column).ok_or_else(|| {
                invalid_response(format!("blob descriptor take is missing '{column}'"))
            })?;
            // An uncached top-level list can reach the descriptor query. Explain
            // that unsupported path before the generic descriptor type check.
            if matches!(
                values.data_type(),
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
            ) && let Ok(schema) = self.schema().await
            {
                crate::blob::reject_list_blob_column(schema.as_ref(), column)?;
            }
            // Only blob v2 columns read back as descriptors.
            let descriptors = values
                .as_any()
                .downcast_ref::<StructArray>()
                .and_then(BlobDescriptorSizes::try_new)
                .ok_or_else(|| Error::InvalidInput {
                    message: format!("column '{column}' is not a blob column"),
                })?;
            for (index, row_id) in taken_row_ids.values().iter().enumerate() {
                sizes.insert(*row_id, descriptors.size(index)?);
            }
        }
        Ok((sizes, response_version))
    }

    fn ensure_blob_files_supported(&self) -> Result<()> {
        if self.server_version.support_blobs() {
            Ok(())
        } else {
            Err(Error::NotSupported {
                message: "fetch_blob_files requires LanceDB server 0.5.0 or newer.".into(),
            })
        }
    }

    fn blob_locations_context(
        &self,
        column: &str,
        version: Option<u64>,
        branch: Option<String>,
        source: Option<&BlobDescriptorSource>,
        identity: Option<String>,
        freshness: Option<FreshnessHeaders>,
    ) -> BlobLocationsContext<S> {
        BlobLocationsContext {
            client: self.client.clone(),
            identifier: self.identifier.clone(),
            table_name: self.name.clone(),
            column: column.to_string(),
            version,
            branch: source.map_or(branch, |source| source.branch.clone()),
            source_uri: source.map(|source| source.uri.clone()),
            source_commit_micros: source.map(|source| source.committed_at_micros),
            row_is_address: source.is_some_and(|source| source.row_is_address),
            expected_query_snapshot: identity,
            freshness,
        }
    }

    fn blob_locations_context_for_source(
        &self,
        source: &BlobDescriptorSource,
    ) -> BlobLocationsContext<S> {
        self.blob_locations_context(
            &source.column,
            Some(source.version),
            source.branch.clone(),
            Some(source),
            self.expected_query_snapshot.clone(),
            None,
        )
    }

    fn blob_range_requester_with_identity(
        &self,
        column: &str,
        row_id: u64,
        read_snapshot: &ReadSnapshot,
        identity: Option<&str>,
    ) -> Arc<dyn BlobRangeRequester> {
        self.blob_range_requester_with_source_and_identity(
            column,
            row_id,
            read_snapshot,
            None,
            identity.map(str::to_owned),
        )
    }

    fn blob_range_requester_with_source(
        &self,
        column: &str,
        row_id: u64,
        read_snapshot: &ReadSnapshot,
        source: Option<&BlobDescriptorSource>,
    ) -> Arc<dyn BlobRangeRequester> {
        self.blob_range_requester_with_source_and_identity(
            column,
            row_id,
            read_snapshot,
            source,
            self.expected_query_snapshot.clone(),
        )
    }

    fn blob_range_requester_with_source_and_identity(
        &self,
        column: &str,
        row_id: u64,
        read_snapshot: &ReadSnapshot,
        source: Option<&BlobDescriptorSource>,
        identity: Option<String>,
    ) -> Arc<dyn BlobRangeRequester> {
        let path = format!(
            "/v1/table/{}/blob/{}/{row_id}/bytes",
            self.identifier,
            urlencoding::encode(column)
        );
        Arc::new(TableBlobRangeRequester {
            client: self.client.clone(),
            path,
            version: read_snapshot.version,
            branch: source.map_or_else(|| self.branch.clone(), |source| source.branch.clone()),
            source_uri: source.map(|source| source.uri.clone()),
            expected_query_snapshot: identity,
            source_commit_micros: source.map(|source| source.committed_at_micros),
            row_is_address: source.is_some_and(|source| source.row_is_address),
            freshness: Arc::new(std::sync::Mutex::new(read_snapshot.freshness_state)),
            parent_freshness: self.freshness.clone(),
            parent_freshness_request: read_snapshot.freshness,
            read_consistency_interval: self.client.read_consistency_interval,
        })
    }
}

/// What a blob v2 descriptor says about a row's size.
#[derive(Debug, Clone, Copy)]
enum DescriptorSize {
    Null,
    Known {
        kind: BlobKind,
        size: u64,
    },
    /// An external blob that recorded no size. Its length is the whole
    /// object's, which only the server can resolve.
    Unresolved,
}

/// A resolved handle candidate. `requester` is `None` only when the server
/// resolved a non-null descriptor to null, which is a protocol error.
struct ResolvedBlobHandle {
    size: u64,
    requester: Option<Arc<dyn BlobRangeRequester>>,
}

/// A row's resolved backing object for a large range request.
enum BlobRangeLocation {
    Null,
    Presigned {
        source: BlobLocationSource,
        offset: u64,
        size: u64,
    },
    Proxy,
}

/// Blob kinds that M1 may read directly. Inline bytes stay on the proxy route,
/// and External URIs keep their existing authority and lifecycle.
fn is_direct_blob_kind(kind: BlobKind) -> bool {
    matches!(
        kind,
        BlobKind::Dedicated | BlobKind::Packed | BlobKind::Managed | BlobKind::ManagedWithBase
    )
}

/// The `kind` and `size` children of a blob v2 descriptor array.
struct BlobDescriptorSizes<'a> {
    descriptors: &'a StructArray,
    kinds: &'a UInt8Array,
    sizes: &'a UInt64Array,
}

impl<'a> BlobDescriptorSizes<'a> {
    fn try_new(descriptors: &'a StructArray) -> Option<Self> {
        let child = |name: &str| descriptors.column_by_name(name);
        Some(Self {
            descriptors,
            kinds: child("kind")?.as_any().downcast_ref::<UInt8Array>()?,
            sizes: child("size")?.as_any().downcast_ref::<UInt64Array>()?,
        })
    }

    fn size(&self, index: usize) -> Result<DescriptorSize> {
        if self.descriptors.is_null(index) || self.kinds.is_null(index) {
            return Ok(DescriptorSize::Null);
        }
        let kind = BlobKind::try_from(self.kinds.value(index))?;
        let size = self.sizes.value(index);
        Ok(if kind == BlobKind::External && size == 0 {
            DescriptorSize::Unresolved
        } else {
            DescriptorSize::Known { kind, size }
        })
    }
}

/// Probe blob sizes while preserving row order.
async fn probe_blob_files(
    requesters: Vec<Arc<dyn BlobRangeRequester>>,
) -> Result<Vec<Option<BlobFile>>> {
    // Collect before buffering to satisfy the async trait lifetime bounds.
    let probe_futures: Vec<_> = requesters.into_iter().map(probe_blob_file).collect();
    futures::stream::iter(probe_futures)
        .buffered(BLOB_REQUEST_CONCURRENCY)
        .try_collect()
        .await
}

const BLOB_REQUEST_CONCURRENCY: usize = 8;

fn probe_blob_version(response: &Response, request_id: &str) -> Result<u64> {
    response
        .headers()
        .get(&VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| Error::Http {
            source: "blob size probe returned a missing or invalid x-lancedb-version header".into(),
            request_id: request_id.to_string(),
            status_code: Some(response.status()),
        })
}

/// Probe one blob's size.
///
/// `204` represents null. `416` with `bytes */0` represents an empty blob.
async fn probe_blob_file(requester: Arc<dyn BlobRangeRequester>) -> Result<Option<BlobFile>> {
    let BlobRangeResponse {
        request_id,
        response,
        validated: _,
        _permit,
    } = requester
        .request_range(0..1, RangeRequestMode::SizeProbe, None)
        .await?;
    match response.status() {
        StatusCode::NO_CONTENT => {
            response.bytes().await.err_to_http(request_id)?;
            Ok(None)
        }
        StatusCode::RANGE_NOT_SATISFIABLE => {
            let total = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_unsatisfied_content_range);
            match total {
                Some(0) => {}
                Some(total) => {
                    return Err(Error::Http {
                        source: format!(
                            "blob size probe returned HTTP 416 for a {total}-byte blob"
                        )
                        .into(),
                        request_id,
                        status_code: Some(StatusCode::RANGE_NOT_SATISFIABLE),
                    });
                }
                None => {
                    return Err(Error::Http {
                        source: "blob size probe returned an invalid Content-Range header".into(),
                        request_id,
                        status_code: Some(StatusCode::RANGE_NOT_SATISFIABLE),
                    });
                }
            }
            // An empty handle never makes a data range request, so no version is needed.
            Ok(Some(RemoteBlobFile::new(requester, 0, None).into()))
        }
        StatusCode::PARTIAL_CONTENT => {
            let size = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_content_range)
                .and_then(|(start, end, total)| {
                    (start == 0 && end == 0 && total > 0).then_some(total)
                })
                .ok_or_else(|| Error::Http {
                    source: "blob size probe returned an invalid Content-Range header".into(),
                    request_id: request_id.clone(),
                    status_code: Some(StatusCode::PARTIAL_CONTENT),
                })?;
            let version = probe_blob_version(&response, &request_id)?;
            let probe_body = response.bytes().await.err_to_http(request_id.clone())?;
            if probe_body.len() != 1 {
                return Err(Error::Http {
                    source: format!(
                        "blob size probe returned {} bytes, expected 1",
                        probe_body.len()
                    )
                    .into(),
                    request_id,
                    status_code: Some(StatusCode::PARTIAL_CONTENT),
                });
            }
            Ok(Some(
                RemoteBlobFile::new(requester, size, Some(version)).into(),
            ))
        }
        status => Err(Error::Http {
            source: format!("blob size probe expected HTTP 206 Partial Content, got {status}")
                .into(),
            request_id,
            status_code: Some(status),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use reqwest::Request;
    use semver::Version;

    use arrow_array::RecordBatch;

    use super::*;

    const PAYLOAD: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    const TEST_SNAPSHOT_IDENTITY: &str =
        "v1:0000000000000000000000000000000000000000000000000000000000000000";
    const SNAPSHOT_A: &str = "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SNAPSHOT_B: &str = "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn null_blob_response() -> http::Response<Vec<u8>> {
        http::Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Vec::new())
            .unwrap()
    }

    fn empty_blob_response() -> http::Response<Vec<u8>> {
        http::Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::CONTENT_RANGE, "bytes */0")
            .body(Vec::new())
            .unwrap()
    }

    fn range_response(request: &Request, payload: &[u8]) -> http::Response<Vec<u8>> {
        let value = request
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap();
        let (start, end) = value
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start = start.parse::<usize>().unwrap();
        let end = if end.is_empty() {
            payload.len() - 1
        } else {
            end.parse::<usize>().unwrap()
        };
        http::Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", payload.len()),
            )
            .header(VERSION_HEADER, "5")
            .body(payload[start..=end].to_vec())
            .unwrap()
    }

    #[tokio::test]
    async fn open_descriptor_files_are_lazy_and_pin_ranges_for_every_storage_kind() {
        for kind in 0..=5 {
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let captured = requests.clone();
            let table = RemoteTable::new_mock(
                "videos".to_string(),
                move |request| {
                    for name in [
                        "x-lancedb-min-version",
                        "x-lancedb-min-read-version",
                        "x-lancedb-min-timestamp",
                    ] {
                        assert!(request.headers().get(name).is_none());
                    }
                    assert_eq!(
                        request.url().path(),
                        "/v1/table/videos/blob/video/12884901889/bytes"
                    );
                    let parameters = request
                        .url()
                        .query_pairs()
                        .into_owned()
                        .collect::<HashMap<_, _>>();
                    assert_eq!(parameters["version"], "7");
                    assert_eq!(parameters["source_commit_micros"], "1234");
                    assert_eq!(parameters["branch"], "dev");
                    assert_eq!(parameters["row_is_address"], "true");
                    assert_eq!(
                        parameters["source_uri"],
                        "s3://bucket/videos.lance/tree/dev"
                    );
                    captured.lock().unwrap().push(request.method().clone());
                    if request.method() == reqwest::Method::HEAD {
                        assert!(request.headers().get(header::RANGE).is_none());
                        http::Response::builder()
                            .status(200)
                            .header(header::CONTENT_LENGTH, PAYLOAD.len().to_string())
                            .body(Vec::new())
                            .unwrap()
                    } else {
                        range_response(&request, PAYLOAD)
                    }
                },
                Some(Version::new(0, 5, 0)),
            );
            {
                let mut freshness = table.freshness.lock().unwrap();
                freshness.min_version = Some(99);
                freshness.min_read_version = Some(2);
            }
            let descriptor = BlobDescriptor {
                kind,
                position: 0,
                size: if kind == 3 { 0 } else { PAYLOAD.len() as u64 },
                blob_id: 0,
                blob_uri: String::new(),
                source: BlobDescriptorSource {
                    uri: "s3://bucket/videos.lance/tree/dev".into(),
                    version: 7,
                    committed_at_micros: 1234,
                    branch: Some("dev".into()),
                    column: "video".into(),
                    row_reference: 12884901889,
                    row_is_address: true,
                },
            };
            let file = table.open_blob_file(&descriptor).await.unwrap();
            let expected = if kind == 3 {
                vec![reqwest::Method::HEAD]
            } else {
                vec![]
            };
            assert_eq!(*requests.lock().unwrap(), expected);
            assert_eq!(file.size(), PAYLOAD.len() as u64);
            file.seek(4).await.unwrap();
            assert_eq!(file.read_up_to(5).await.unwrap().as_ref(), &PAYLOAD[4..9]);
            assert_eq!(file.tell().await.unwrap(), 9);
            assert_eq!(table.freshness.lock().unwrap().min_read_version, Some(2));
            assert_eq!(file.read_range(0..3).await.unwrap().as_ref(), &PAYLOAD[..3]);
            assert_eq!(file.tell().await.unwrap(), 9);
            assert_eq!(
                requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|method| **method == reqwest::Method::GET)
                    .count(),
                2
            );
        }
    }

    fn mock_remote_blob_table(
        requests: Arc<StdMutex<Vec<String>>>,
    ) -> RemoteTable<crate::remote::client::test_utils::MockSender> {
        RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                assert_eq!(request.method(), reqwest::Method::GET);
                let path = request.url().path();
                assert!(path.starts_with("/v1/table/my_table/blob/image/"));
                requests.lock().unwrap().push(
                    request
                        .headers()
                        .get(header::RANGE)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                );
                if path.contains("/20/bytes") {
                    return null_blob_response();
                }
                if path.contains("/30/bytes") {
                    return empty_blob_response();
                }
                range_response(&request, PAYLOAD)
            },
            Some(Version::new(0, 5, 0)),
        )
    }

    #[tokio::test]
    async fn remote_blob_files_probe_sizes_and_preserve_nulls() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table.probe_files("image", &[10, 20]).await.unwrap();

        assert_eq!(files.len(), 2);
        assert!(files[1].is_none());
        let file = files[0].take().unwrap();
        assert_eq!(file.size(), PAYLOAD.len() as u64);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["bytes=0-0", "bytes=0-0"]
        );
    }

    /// Descriptors as a remote query returns them, one `(kind, size)` per row.
    fn descriptors(rows: &[Option<(BlobKind, u64)>]) -> StructArray {
        let kinds = rows
            .iter()
            .map(|row| row.map_or(0, |(kind, _)| kind as u8))
            .collect::<UInt8Array>();
        let sizes = rows
            .iter()
            .map(|row| row.map_or(0, |(_, size)| size))
            .collect::<UInt64Array>();
        let len = rows.len();
        StructArray::new(
            lance_core::datatypes::BLOB_V2_DESC_FIELDS.clone(),
            vec![
                Arc::new(kinds),
                Arc::new(UInt64Array::from(vec![0; len])),
                Arc::new(sizes),
                Arc::new(arrow_array::UInt32Array::from(vec![0; len])),
                Arc::new(arrow_array::StringArray::from(vec![""; len])),
            ],
            Some(rows.iter().map(Option::is_some).collect()),
        )
    }

    impl<S: HttpSend> RemoteTable<S> {
        /// Open handles through the per-row size probe alone.
        async fn probe_files(
            &self,
            column: &str,
            row_ids: &[u64],
        ) -> Result<Vec<Option<BlobFile>>> {
            let read_snapshot = self.snapshot_read_state().await;
            self.fetch_blob_files_with_snapshot(column, row_ids, read_snapshot, false, None)
                .await
        }
    }

    /// A request the take mock received.
    #[derive(Debug, Clone, PartialEq)]
    enum TakeMockRequest {
        Query {
            row_ids: Vec<u64>,
            version: Option<u64>,
        },
        Describe,
        Range {
            row_id: u64,
            range: String,
            version: Option<String>,
        },
    }

    /// Row ids named by a `_rowid IN (...)` filter.
    fn filter_row_ids(filter: &str) -> Vec<u64> {
        let list = filter.split_once('(').unwrap().1.trim_end_matches(')');
        list.split(',')
            .map(|id| id.trim().parse().unwrap())
            .collect()
    }

    /// A table whose query route answers `_rowid` takes from `rows` and whose
    /// byte routes serve [`PAYLOAD`]. Takes return rows in reverse order, and
    /// report `query_version` in `x-lancedb-version` when it is set.
    fn mock_take_blob_table(
        rows: Vec<(u64, Option<(BlobKind, u64)>)>,
        query_version: Option<&'static str>,
    ) -> (
        RemoteTable<crate::remote::client::test_utils::MockSender>,
        Arc<StdMutex<Vec<TakeMockRequest>>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let captured = requests.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path().to_string();
                if path == "/v1/table/my_table/query/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["columns"], serde_json::json!(["image"]));
                    assert_eq!(body["with_row_id"], serde_json::json!(true));
                    assert_eq!(body["use_rte"], serde_json::json!(false));
                    let requested = filter_row_ids(body["filter"].as_str().unwrap());
                    let requested_version = body["version"].as_u64();
                    captured.lock().unwrap().push(TakeMockRequest::Query {
                        row_ids: requested.clone(),
                        version: body["version"].as_u64(),
                    });
                    let taken = rows
                        .iter()
                        .rev()
                        .filter(|(row_id, _)| requested.contains(row_id))
                        .collect::<Vec<_>>();
                    let batch = RecordBatch::try_from_iter([
                        (
                            "image",
                            Arc::new(descriptors(
                                &taken.iter().map(|(_, row)| *row).collect::<Vec<_>>(),
                            )) as Arc<dyn Array>,
                        ),
                        (
                            ROW_ID,
                            Arc::new(UInt64Array::from_iter_values(
                                taken.iter().map(|(row_id, _)| *row_id),
                            )) as Arc<dyn Array>,
                        ),
                    ])
                    .unwrap();
                    let mut body = Vec::new();
                    let mut writer =
                        arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                    drop(writer);
                    let mut response = http::Response::builder().status(200);
                    if let Some(version) = requested_version {
                        response = response.header(VERSION_HEADER, version.to_string());
                    } else if let Some(version) = query_version {
                        response = response.header(VERSION_HEADER, version);
                    }
                    return response.body(body).unwrap();
                }
                if path == "/v1/table/my_table/describe/" {
                    captured.lock().unwrap().push(TakeMockRequest::Describe);
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version":9,"schema":{"fields":[]}}"#.as_bytes().to_vec())
                        .unwrap();
                }
                let row_id = path
                    .strip_prefix("/v1/table/my_table/blob/image/")
                    .and_then(|rest| rest.strip_suffix("/bytes"))
                    .unwrap_or_else(|| panic!("unexpected path: {path}"))
                    .parse()
                    .unwrap();
                captured.lock().unwrap().push(TakeMockRequest::Range {
                    row_id,
                    range: request.headers()[header::RANGE]
                        .to_str()
                        .unwrap()
                        .to_string(),
                    version: request
                        .url()
                        .query_pairs()
                        .find(|(name, _)| name == "version")
                        .map(|(_, value)| value.into_owned()),
                });
                range_response(&request, PAYLOAD)
            },
            Some(Version::new(0, 5, 0)),
        );
        (table, requests)
    }

    #[tokio::test]
    async fn remote_blob_ranges_preserve_order_nulls_and_empty_ranges() {
        let (table, requests) = mock_take_blob_table(
            vec![
                (10, Some((BlobKind::Inline, PAYLOAD.len() as u64))),
                (20, None),
                (30, Some((BlobKind::Packed, PAYLOAD.len() as u64))),
                (40, Some((BlobKind::Inline, 0))),
            ],
            Some("7"),
        );

        let ranges = table
            .fetch_blob_ranges(
                "image",
                &[
                    BlobRangeRequest::new(30, 6, 5),
                    BlobRangeRequest::new(10, 0, 3),
                    BlobRangeRequest::new(20, 0, 1),
                    BlobRangeRequest::new(10, 2, 2),
                    BlobRangeRequest::new(40, 0, 0),
                    BlobRangeRequest::new(10, PAYLOAD.len() as u64, 0),
                    BlobRangeRequest::new(30, 6, 5),
                ],
            )
            .await
            .unwrap();

        assert_eq!(
            ranges,
            LargeBinaryArray::from(vec![
                Some(&PAYLOAD[6..11]),
                Some(&PAYLOAD[0..3]),
                None,
                Some(&PAYLOAD[2..4]),
                Some(b"".as_slice()),
                Some(b"".as_slice()),
                Some(&PAYLOAD[6..11]),
            ])
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests[0],
            TakeMockRequest::Query {
                row_ids: vec![10, 20, 30, 40],
                version: None,
            }
        );
        // Only nonempty, non-null ranges are read, at the descriptor take's version.
        let reads = requests[1..]
            .iter()
            .map(|request| match request {
                TakeMockRequest::Range {
                    row_id,
                    range,
                    version,
                } => {
                    assert_eq!(version.as_deref(), Some("7"));
                    (*row_id, range.as_str())
                }
                other => panic!("unexpected request: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            reads,
            [
                (30, "bytes=6-10"),
                (10, "bytes=0-2"),
                (10, "bytes=2-3"),
                (30, "bytes=6-10")
            ]
        );
    }

    #[tokio::test]
    async fn remote_blob_ranges_reject_missing_rows_and_invalid_ranges() {
        for (ranges, message) in [
            (
                vec![
                    BlobRangeRequest::new(10, 0, 1),
                    BlobRangeRequest::new(99, 0, 1),
                ],
                "only 1 exist",
            ),
            (vec![BlobRangeRequest::new(10, 35, 2)], "exceeds blob size"),
            (vec![BlobRangeRequest::new(10, 37, 0)], "exceeds blob size"),
            (
                vec![BlobRangeRequest::new(10, u64::MAX, 1)],
                "overflows u64",
            ),
            (
                vec![BlobRangeRequest::new(20, u64::MAX, 1)],
                "overflows u64",
            ),
        ] {
            let (table, requests) = mock_take_blob_table(
                vec![
                    (10, Some((BlobKind::Inline, PAYLOAD.len() as u64))),
                    (20, None),
                ],
                Some("7"),
            );
            let error = table.fetch_blob_ranges("image", &ranges).await.unwrap_err();
            assert!(error.to_string().contains(message), "{error}");
            // Missing rows and invalid ranges fail without reading any bytes.
            assert!(matches!(error, Error::InvalidInput { .. }));
            assert_eq!(
                requests.lock().unwrap().len(),
                usize::from(!message.contains("overflows"))
            );
        }
    }

    #[tokio::test]
    async fn remote_blob_ranges_keep_producing_query_identity_from_first_request() {
        const IDENTITY: &str =
            "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let captured = calls.clone();
        let table = RemoteTable::new_mock(
            "my_table".into(),
            move |request| {
                assert_eq!(
                    request.url().path(),
                    "/v1/table/my_table/fetch_blob_ranges/"
                );
                assert_eq!(
                    request.headers()[super::super::QUERY_SNAPSHOT_HEADER],
                    IDENTITY
                );
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
                assert_eq!(body["version"], 42);
                let rows = body["ranges"].as_array().unwrap().len();
                captured.lock().unwrap().push(rows);
                let batch = RecordBatch::try_from_iter([(
                    "value",
                    Arc::new(LargeBinaryArray::from(vec![Some(b"A".as_slice()); rows]))
                        as Arc<dyn Array>,
                )])
                .unwrap();
                let mut bytes = Vec::new();
                let mut writer =
                    arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
                drop(writer);
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/vnd.apache.arrow.stream")
                    .header(VERSION_HEADER, "42")
                    .header(super::super::QUERY_SNAPSHOT_HEADER, IDENTITY)
                    .body(bytes)
                    .unwrap()
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        table.seed_blob_ranges_capability(&headers);
        let snapshot = table
            .query_snapshot_with_identity(42, IDENTITY)
            .await
            .unwrap();
        let ranges = snapshot
            .fetch_blob_ranges("image", &[BlobRangeRequest::new(10, 0, 1); 1025])
            .await
            .unwrap();
        assert_eq!(
            ranges,
            LargeBinaryArray::from(vec![Some(b"A".as_slice()); 1025])
        );
        assert_eq!(*calls.lock().unwrap(), [1024, 1]);
        assert!(table.snapshot_read_state().await.version.is_none());
    }

    #[tokio::test]
    async fn remote_blob_ranges_cancel_releases_the_only_request_permit() {
        use std::sync::atomic::AtomicUsize;
        let entered = Arc::new(tokio::sync::Notify::new());
        let started = entered.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let captured = calls.clone();
        let table = Arc::new(RemoteTable::new_mock_with_version_and_config(
            "my_table".into(),
            move |request| {
                assert!(request.url().path().ends_with("/fetch_blob_ranges/"));
                if captured.fetch_add(1, Ordering::SeqCst) == 0 {
                    started.notify_one();
                    http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .header(
                            super::super::QUERY_SNAPSHOT_HEADER,
                            format!("v1:{}", "a".repeat(64)),
                        )
                        .body(reqwest::Body::wrap_stream(futures::stream::pending::<
                            std::result::Result<Bytes, std::io::Error>,
                        >()))
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(400)
                        .header("x-lancedb-error-code", "invalid_input")
                        .body(reqwest::Body::from(
                            r#"{"error":"range exceeds blob size"}"#,
                        ))
                        .unwrap()
                }
            },
            Some(Version::new(1, 13, 0)),
            crate::remote::ClientConfig {
                blob_request_concurrency: Some(1),
                ..Default::default()
            },
        ));
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        table.seed_blob_ranges_capability(&headers);
        let reader_table = table.clone();
        let reader = tokio::spawn(async move {
            reader_table
                .fetch_blob_ranges("image", &[BlobRangeRequest::new(10, 0, 1); 1025])
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            table.fetch_blob_ranges("image", &[BlobRangeRequest::new(10, 0, 1)]),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("range exceeds blob size"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn remote_blob_ranges_reject_overflow_before_http() {
        let (table, requests) = mock_take_blob_table(vec![], Some("7"));
        let error = table
            .fetch_blob_ranges("image", &[BlobRangeRequest::new(20, u64::MAX, 1)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("overflows u64"), "{error}");
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn remote_blob_files_reject_unverified_descriptor_identity_before_values() {
        const EXPECTED: &str =
            "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        const OTHER: &str = "v1:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        for response_identity in [None, Some(OTHER)] {
            let range_requests = Arc::new(AtomicUsize::new(0));
            let captured_ranges = range_requests.clone();
            let table = RemoteTable::new_mock(
                "my_table".to_string(),
                move |request| {
                    let path = request.url().path().to_string();
                    if path == "/v1/table/my_table/query/" {
                        // Row 20 is NULL and row 40 is empty, so an unverified
                        // result could escape without any payload request.
                        assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], EXPECTED);
                        let body: serde_json::Value =
                            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                                .unwrap();
                        assert_eq!(filter_row_ids(body["filter"].as_str().unwrap()), [20, 40]);
                        let batch = RecordBatch::try_from_iter([
                            (
                                "image",
                                Arc::new(descriptors(&[None, Some((BlobKind::Inline, 0))]))
                                    as Arc<dyn Array>,
                            ),
                            (
                                ROW_ID,
                                Arc::new(UInt64Array::from_iter_values([20, 40])) as Arc<dyn Array>,
                            ),
                        ])
                        .unwrap();
                        let mut body = Vec::new();
                        let mut writer =
                            arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema())
                                .unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                        drop(writer);
                        let mut response = http::Response::builder()
                            .status(200)
                            .header(VERSION_HEADER, "42");
                        if let Some(identity) = response_identity {
                            response = response.header(QUERY_SNAPSHOT_HEADER, identity);
                        }
                        return response.body(body).unwrap();
                    }
                    if path.starts_with("/v1/table/my_table/blob/") {
                        captured_ranges.fetch_add(1, Ordering::SeqCst);
                        panic!("descriptor verification must fail before any payload read");
                    }
                    panic!("unexpected path: {path}");
                },
                Some(Version::new(0, 5, 0)),
            );
            let snapshot = table
                .query_snapshot_with_identity(42, EXPECTED)
                .await
                .unwrap();

            let error = snapshot
                .fetch_blob_files("image", &[20, 40])
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("did not verify the query snapshot identity"),
                "{error}"
            );
            assert_eq!(range_requests.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn remote_blob_ranges_accept_empty_requests_without_http() {
        let (table, requests) = mock_take_blob_table(vec![], Some("7"));
        let ranges = table.fetch_blob_ranges("image", &[]).await.unwrap();
        assert!(ranges.is_empty());
        assert!(requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn remote_blob_files_are_sized_from_one_descriptor_take() {
        let payload_size = PAYLOAD.len() as u64;
        let (table, requests) = mock_take_blob_table(
            vec![
                (10, Some((BlobKind::Inline, payload_size))),
                (20, None),
                (30, Some((BlobKind::Inline, 0))),
                (40, Some((BlobKind::Packed, payload_size))),
                (50, Some((BlobKind::Dedicated, payload_size))),
                (60, Some((BlobKind::External, 0))),
            ],
            Some("7"),
        );

        // Reordered and duplicated row ids come back in request order.
        let files = table
            .fetch_blob_files_impl("image", &[60, 10, 20, 30, 40, 50, 10])
            .await
            .unwrap();

        let sizes = files
            .iter()
            .map(|file| file.as_ref().map(BlobFile::size))
            .collect::<Vec<_>>();
        assert_eq!(
            sizes,
            [
                Some(payload_size),
                Some(payload_size),
                None,
                Some(0),
                Some(payload_size),
                Some(payload_size),
                Some(payload_size),
            ]
        );
        // One take, then a probe for the external blob that recorded no size.
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                TakeMockRequest::Query {
                    row_ids: vec![10, 20, 30, 40, 50, 60],
                    version: None,
                },
                TakeMockRequest::Range {
                    row_id: 60,
                    range: "bytes=0-0".into(),
                    version: Some("7".into()),
                },
            ]
        );

        // Sized handles read the version the take read.
        assert_eq!(
            files[1].as_ref().unwrap().read_range(5..12).await.unwrap(),
            &PAYLOAD[5..12]
        );
        assert!(files[3].as_ref().unwrap().read().await.unwrap().is_empty());
        assert_eq!(
            requests.lock().unwrap().last().unwrap(),
            &TakeMockRequest::Range {
                row_id: 10,
                range: "bytes=5-11".into(),
                version: Some("7".into()),
            }
        );
    }

    #[tokio::test]
    async fn remote_blob_files_take_every_chunk_at_the_first_version() {
        let row_ids = (0..MAX_FETCH_BLOBS_ROW_IDS as u64 + 1).collect::<Vec<_>>();
        let (table, requests) = mock_take_blob_table(
            row_ids.iter().map(|row_id| (*row_id, None)).collect(),
            Some("7"),
        );

        let files = table
            .fetch_blob_files_impl("image", &row_ids)
            .await
            .unwrap();

        assert_eq!(files.len(), row_ids.len());
        let versions = requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| match request {
                TakeMockRequest::Query { row_ids, version } => (row_ids.len(), *version),
                other => panic!("unexpected request: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(versions, [(MAX_FETCH_BLOBS_ROW_IDS, None), (1, Some(7))]);
    }

    #[tokio::test]
    async fn remote_blob_files_retake_at_the_described_version_when_a_take_does_not_report_it() {
        let payload_size = PAYLOAD.len() as u64;
        let (table, requests) =
            mock_take_blob_table(vec![(10, Some((BlobKind::Inline, payload_size)))], None);

        let file = table
            .fetch_blob_files_impl("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();
        file.read_range(0..4).await.unwrap();

        // The unversioned take is repeated at the described version.
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                TakeMockRequest::Query {
                    row_ids: vec![10],
                    version: None,
                },
                TakeMockRequest::Describe,
                TakeMockRequest::Query {
                    row_ids: vec![10],
                    version: Some(9),
                },
                TakeMockRequest::Range {
                    row_id: 10,
                    range: "bytes=0-3".into(),
                    version: Some("9".into()),
                },
            ]
        );
    }

    #[tokio::test]
    async fn remote_blob_files_take_at_the_checked_out_version() {
        let payload_size = PAYLOAD.len() as u64;
        let (table, requests) = mock_take_blob_table(
            vec![(10, Some((BlobKind::Inline, payload_size)))],
            Some("7"),
        );
        table.checkout(9).await.unwrap();
        requests.lock().unwrap().clear();

        table.fetch_blob_files_impl("image", &[10]).await.unwrap();

        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [TakeMockRequest::Query {
                row_ids: vec![10],
                version: Some(9),
            }]
        );
    }

    #[tokio::test]
    async fn remote_blob_files_reject_row_ids_missing_from_the_take() {
        let (table, requests) = mock_take_blob_table(vec![(10, None)], Some("7"));

        let error = table
            .fetch_blob_files_impl("image", &[10, 99])
            .await
            .unwrap_err();

        assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
        assert!(error.to_string().contains("only 1 exist"), "{error}");
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn remote_blob_files_reject_a_non_blob_column() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |request| {
                assert_eq!(request.url().path(), "/v1/table/my_table/query/");
                let batch = RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(arrow_array::Int64Array::from(vec![1])) as Arc<dyn Array>,
                    ),
                    (
                        ROW_ID,
                        Arc::new(UInt64Array::from(vec![10])) as Arc<dyn Array>,
                    ),
                ])
                .unwrap();
                let mut body = Vec::new();
                let mut writer =
                    arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
                drop(writer);
                http::Response::builder().status(200).body(body).unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );

        let error = table.fetch_blob_files_impl("id", &[10]).await.unwrap_err();

        assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
        assert!(
            error.to_string().contains("'id' is not a blob column"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_file_reads_the_requested_range() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        assert_eq!(file.read_range(5..12).await.unwrap(), &PAYLOAD[5..12]);
        assert!(requests.lock().unwrap().contains(&"bytes=5-11".to_string()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn blob_requests_are_bounded_across_handles() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let table = RemoteTable::new_mock_with_version_and_config(
            "my_table".to_string(),
            {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                move |request| {
                    let range = request.headers().get(header::RANGE).unwrap();
                    if range != "bytes=0-0" {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        max_in_flight.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(20));
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    }
                    range_response(&request, PAYLOAD)
                }
            },
            Some(Version::new(0, 5, 0)),
            crate::remote::ClientConfig {
                blob_request_concurrency: Some(3),
                ..Default::default()
            },
        );
        let files = table
            .probe_files("image", &(0..16).collect::<Vec<_>>())
            .await
            .unwrap();
        let start = Arc::new(tokio::sync::Barrier::new(files.len() + 1));
        let reads = files.into_iter().map(|file| {
            let start = start.clone();
            tokio::spawn(async move {
                start.wait().await;
                file.unwrap().read_range(1..2).await
            })
        });
        let reads = reads.collect::<Vec<_>>();
        start.wait().await;
        for result in futures::future::join_all(reads).await {
            assert_eq!(result.unwrap().unwrap(), &PAYLOAD[1..2]);
        }
        let max = max_in_flight.load(Ordering::SeqCst);
        assert!(max > 1, "reads never overlapped");
        assert!(
            max <= 3,
            "{max} blob requests exceeded the configured limit"
        );
    }

    #[tokio::test]
    async fn partial_reads_release_slots_for_other_handles() {
        let table = RemoteTable::new_mock_with_version_and_config(
            "my_table".to_string(),
            |request| range_response(&request, PAYLOAD),
            Some(Version::new(0, 5, 0)),
            crate::remote::ClientConfig {
                blob_request_concurrency: Some(1),
                ..Default::default()
            },
        );
        let files = table.probe_files("image", &[10, 20]).await.unwrap();
        let reads = files
            .iter()
            .map(|file| file.as_ref().unwrap().read_up_to(1));
        let results =
            tokio::time::timeout(Duration::from_secs(1), futures::future::join_all(reads))
                .await
                .expect("an idle partial response held the only connection slot");
        for result in results {
            assert_eq!(result.unwrap(), &PAYLOAD[..1]);
        }
    }

    #[tokio::test]
    async fn blob_retry_error_includes_last_status() {
        let table = RemoteTable::new_mock_with_version_and_config(
            "my_table".to_string(),
            |request| {
                if request.headers().get(header::RANGE).unwrap() == "bytes=0-0" {
                    range_response(&request, PAYLOAD)
                } else {
                    http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap()
                }
            },
            Some(Version::new(0, 5, 0)),
            crate::remote::ClientConfig {
                retry_config: crate::remote::RetryConfig {
                    retries: Some(1),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let file = table.probe_files("image", &[10]).await.unwrap();
        let error = file[0]
            .as_ref()
            .unwrap()
            .read_range(1..2)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("503"), "got: {error}");
    }

    #[tokio::test]
    async fn remote_blob_file_reads_the_probed_version_after_table_changes() {
        let changed = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            {
                let changed = changed.clone();
                let requests = requests.clone();
                move |request| {
                    let range = request
                        .headers()
                        .get(header::RANGE)
                        .unwrap()
                        .to_str()
                        .unwrap();
                    let version = request
                        .url()
                        .query_pairs()
                        .find(|(name, _)| name == "version")
                        .map(|(_, value)| value.into_owned());
                    requests
                        .lock()
                        .unwrap()
                        .push((range.to_string(), version.clone()));
                    if changed.load(Ordering::SeqCst) && version.as_deref() != Some("5") {
                        return http::Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(b"row id is absent from the latest version".to_vec())
                            .unwrap();
                    }
                    range_response(&request, PAYLOAD)
                }
            },
            Some(Version::new(0, 5, 0)),
        );

        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();
        changed.store(true, Ordering::SeqCst);

        assert_eq!(file.read_range(5..9).await.unwrap(), &PAYLOAD[5..9]);
        assert_eq!(file.read_up_to(4).await.unwrap(), &PAYLOAD[..4]);
        file.seek(10).await.unwrap();
        assert_eq!(file.read().await.unwrap(), &PAYLOAD[10..]);

        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                ("bytes=0-0".to_string(), None),
                ("bytes=5-8".to_string(), Some("5".to_string())),
                (
                    format!("bytes=0-{}", PAYLOAD.len() - 1),
                    Some("5".to_string())
                ),
            ]
        );
    }

    #[tokio::test]
    async fn remote_blob_file_keeps_the_open_timeline_after_parent_checkout() {
        let range_requests = Arc::new(StdMutex::new(Vec::new()));
        let captured = range_requests.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(r#"{"version":5,"schema":{"fields":[]}}"#.as_bytes().to_vec())
                    .unwrap(),
                "/v1/table/my_table/blob/image/10/bytes" => {
                    captured.lock().unwrap().push((
                        request.url().query().unwrap_or_default().to_string(),
                        request.headers().clone(),
                    ));
                    range_response(&request, PAYLOAD)
                }
                path => panic!("unexpected path: {path}"),
            },
            Some(Version::new(0, 5, 0)),
        );

        table.checkout(5).await.unwrap();
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();
        table.checkout_latest().await.unwrap();
        file.read_range(5..12).await.unwrap();

        let requests = range_requests.lock().unwrap();
        let (query, headers) = requests.last().unwrap();
        assert!(query.contains("version=5"));
        assert!(!headers.contains_key("x-lancedb-min-timestamp"));
    }

    #[tokio::test]
    async fn remote_blob_file_reuses_prefetched_bytes_across_reads_and_seeks() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        assert_eq!(file.read_up_to(4).await.unwrap(), b"0123".as_slice());
        assert_eq!(file.read_up_to(3).await.unwrap(), b"456".as_slice());
        file.seek(20).await.unwrap();
        assert_eq!(file.read_up_to(4).await.unwrap(), &PAYLOAD[20..24]);
        assert_eq!(file.tell().await.unwrap(), 24);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["bytes=0-0", &format!("bytes=0-{}", PAYLOAD.len() - 1)]
        );
    }

    #[tokio::test]
    async fn remote_buffer_boundaries_and_disabled_read_ahead() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();
        file.set_buffer_size(4).await.unwrap();
        assert_eq!(file.read_up_to(3).await.unwrap(), &PAYLOAD[..3]);
        assert_eq!(file.read_up_to(3).await.unwrap(), &PAYLOAD[3..6]);
        file.seek(5).await.unwrap();
        assert_eq!(file.read_up_to(2).await.unwrap(), &PAYLOAD[5..7]);
        assert_eq!(file.read_range(5..6).await.unwrap(), &PAYLOAD[5..6]);
        assert_eq!(file.tell().await.unwrap(), 7);
        file.set_buffer_size(0).await.unwrap();
        assert_eq!(file.read_up_to(1).await.unwrap(), &PAYLOAD[7..8]);
        assert_eq!(file.read_up_to(1).await.unwrap(), &PAYLOAD[8..9]);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            [
                "bytes=0-0",
                "bytes=0-3",
                "bytes=4-7",
                "bytes=5-5",
                "bytes=7-7",
                "bytes=8-8"
            ]
        );
    }

    #[tokio::test]
    async fn remote_blob_files_allow_an_empty_blob_without_a_version_header() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table.probe_files("image", &[10, 20, 30]).await.unwrap();

        assert_eq!(files.len(), 3);
        assert!(files[1].is_none());
        let empty = files[2].take().unwrap();
        assert_eq!(empty.size(), 0);
        let probe_requests = requests.lock().unwrap().len();
        assert!(empty.read_range(0..0).await.unwrap().is_empty());
        assert!(empty.read().await.unwrap().is_empty());
        assert_eq!(requests.lock().unwrap().len(), probe_requests);
        let nonempty = files[0].take().unwrap();
        assert_eq!(nonempty.size(), PAYLOAD.len() as u64);
    }

    #[tokio::test]
    async fn remote_blob_files_reject_unsatisfied_probe_for_nonempty_blob() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |_| {
                http::Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, "bytes */36")
                    .body(Vec::new())
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );

        let error = table.probe_files("image", &[10]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("blob size probe returned HTTP 416 for a 36-byte blob"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_files_reject_a_self_contradictory_probe_response() {
        // A zero-length blob must use `416 Content-Range: bytes */0`.
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |_| {
                http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_RANGE, "bytes 0-0/0")
                    .body(vec![0u8])
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );

        let error = table.probe_files("image", &[10]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("blob size probe returned an invalid Content-Range header"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_files_reject_a_probe_without_a_version() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |request| {
                let mut response = range_response(&request, PAYLOAD);
                response.headers_mut().remove(VERSION_HEADER);
                response
            },
            Some(Version::new(0, 5, 0)),
        );

        let error = table.probe_files("image", &[10]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing or invalid x-lancedb-version"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_files_reject_unsupported_server_version() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |_| -> http::Response<String> {
                panic!("old servers must be rejected before a range request")
            },
            Some(Version::new(0, 4, 9)),
        );
        let error = table
            .fetch_blob_files_impl("image", &[10])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
        assert!(error.to_string().contains("0.5.0"));
    }

    #[tokio::test]
    async fn remote_blob_file_rejects_mismatched_content_range() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            {
                let requests = requests.clone();
                move |request| {
                    let range = request
                        .headers()
                        .get(header::RANGE)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string();
                    requests.lock().unwrap().push(range.clone());
                    if range == "bytes=0-0" {
                        return range_response(&request, PAYLOAD);
                    }
                    http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes 6-12/{}", PAYLOAD.len()),
                        )
                        .body(PAYLOAD[6..=12].to_vec())
                        .unwrap()
                }
            },
            Some(Version::new(0, 5, 0)),
        );
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        let error = file.read_range(5..12).await.unwrap_err();
        assert!(
            error.to_string().contains("expected Content-Range"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_file_rejects_short_response_body() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let range = request
                    .headers()
                    .get(header::RANGE)
                    .unwrap()
                    .to_str()
                    .unwrap();
                if range == "bytes=0-0" {
                    return range_response(&request, PAYLOAD);
                }
                http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(
                        header::CONTENT_RANGE,
                        format!("bytes 5-11/{}", PAYLOAD.len()),
                    )
                    .body(PAYLOAD[5..=7].to_vec())
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        let error = file.read_range(5..12).await.unwrap_err();
        assert!(
            error.to_string().contains("returned 3 bytes, expected 7"),
            "got: {error}"
        );
    }

    #[tokio::test]
    async fn remote_blob_file_failed_read_preserves_cursor_and_retries_fresh() {
        let read_requests = Arc::new(AtomicUsize::new(0));
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            {
                let read_requests = read_requests.clone();
                move |request| {
                    let range = request
                        .headers()
                        .get(header::RANGE)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string();
                    if range == "bytes=0-0" {
                        return range_response(&request, PAYLOAD);
                    }
                    let attempt = read_requests.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        // Return too few bytes for the requested range.
                        return http::Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header(
                                header::CONTENT_RANGE,
                                format!("bytes 0-9/{}", PAYLOAD.len()),
                            )
                            .body(PAYLOAD[..5].to_vec())
                            .unwrap();
                    }
                    range_response(&request, PAYLOAD)
                }
            },
            Some(Version::new(0, 5, 0)),
        );
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        file.set_buffer_size(0).await.unwrap();
        let error = file.read_up_to(10).await.unwrap_err();
        assert!(
            error.to_string().contains("returned 5 bytes, expected 10"),
            "got: {error}"
        );
        assert_eq!(file.tell().await.unwrap(), 0);

        // Retry from the last committed cursor with a fresh request.
        let retried = file.read_up_to(4).await.unwrap();
        assert_eq!(retried, b"0123".as_slice());
        assert_eq!(read_requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn remote_blob_file_empty_range_sends_no_request() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        assert!(file.read_range(3..3).await.unwrap().is_empty());
        assert_eq!(requests.lock().unwrap().as_slice(), ["bytes=0-0"]);
    }

    #[derive(Debug)]
    struct CountingProbeRequester {
        index: usize,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl BlobRangeRequester for CountingProbeRequester {
        async fn request_range(
            &self,
            range: Range<u64>,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<BlobRangeResponse> {
            assert_eq!(range, 0..1);
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            // Earlier probes sleep longer, so later probes finish first and the
            // ordered collection has to do real reordering work.
            tokio::time::sleep(Duration::from_millis(20 - self.index as u64)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let response = http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes 0-0/{}", 100 + self.index),
                )
                .header(VERSION_HEADER, "5")
                .body(vec![0u8])
                .unwrap();
            Ok((format!("probe-{}", self.index), Response::from(response)).into())
        }
    }

    #[tokio::test]
    async fn remote_blob_file_probes_are_bounded_and_preserve_order() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let probes = (0..16)
            .map(|index| {
                let requester: Arc<dyn BlobRangeRequester> = Arc::new(CountingProbeRequester {
                    index,
                    in_flight: in_flight.clone(),
                    max_in_flight: max_in_flight.clone(),
                });
                requester
            })
            .collect();

        let files = probe_blob_files(probes).await.unwrap();

        let sizes: Vec<u64> = files.into_iter().map(|file| file.unwrap().size()).collect();
        let expected: Vec<u64> = (0..16).map(|index| 100 + index as u64).collect();
        assert_eq!(sizes, expected);
        let max = max_in_flight.load(Ordering::SeqCst);
        assert!(max > 1, "probes never overlapped");
        assert!(
            max <= BLOB_REQUEST_CONCURRENCY,
            "{max} probes in flight exceeds the bound"
        );
    }

    #[tokio::test]
    async fn remote_blob_file_metadata_reports_none() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table.probe_files("image", &[10]).await.unwrap();
        let file = files.remove(0).unwrap();

        assert_eq!(file.size(), PAYLOAD.len() as u64);
        assert_eq!(file.position(), None);
        assert_eq!(file.kind(), None);
        assert_eq!(file.data_path(), None);
        assert_eq!(file.uri(), None);
    }

    #[tokio::test]
    async fn closed_remote_blob_file_rejects_every_operation_without_requests() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table.probe_files("image", &[10]).await.unwrap();
        let file = files.remove(0).unwrap();
        let probe_requests = requests.lock().unwrap().len();

        file.close().await.unwrap();

        assert!(file.is_closed().await);
        for error in [
            file.read().await.unwrap_err(),
            file.read_range(0..1).await.unwrap_err(),
            file.read_ranges(&[0..1, 1..2]).await.unwrap_err(),
            file.read_up_to(1).await.unwrap_err(),
            file.seek(0).await.unwrap_err(),
            file.tell().await.unwrap_err(),
            file.set_buffer_size(0).await.unwrap_err(),
        ] {
            assert!(error.to_string().contains("already closed"), "got: {error}");
        }
        assert_eq!(requests.lock().unwrap().len(), probe_requests);
    }

    #[tokio::test]
    async fn out_of_range_read_fails_without_a_request() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table.probe_files("image", &[10]).await.unwrap();
        let file = files.remove(0).unwrap();
        let probe_requests = requests.lock().unwrap().len();

        let past_end = file.read_range(0..file.size() + 1).await.unwrap_err();
        assert!(
            past_end.to_string().contains("exceeds blob size"),
            "got: {past_end}"
        );
        let inverted = file
            .read_range(Range { start: 3, end: 1 })
            .await
            .unwrap_err();
        assert!(
            inverted.to_string().contains("exceeds end"),
            "got: {inverted}"
        );
        assert_eq!(requests.lock().unwrap().len(), probe_requests);
    }

    #[tokio::test]
    async fn data_read_rejects_416_response() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |request| {
                let range = request
                    .headers()
                    .get(header::RANGE)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                if range == "bytes=0-0" {
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes 0-0/{}", PAYLOAD.len()),
                        )
                        .header(VERSION_HEADER, "5")
                        .body(vec![PAYLOAD[0]])
                        .unwrap();
                }
                http::Response::builder()
                    .status(StatusCode::RANGE_NOT_SATISFIABLE)
                    .header(header::CONTENT_RANGE, "bytes */0")
                    .body(b"stale range".to_vec())
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );

        let mut files = table.probe_files("image", &[10]).await.unwrap();
        let file = files.remove(0).unwrap();

        let error = file.read_range(1..3).await.unwrap_err();
        assert!(error.to_string().contains("416"), "got: {error}");
    }

    #[tokio::test]
    async fn empty_row_id_lists_bypass_server_version_gate() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |_| -> http::Response<String> { panic!("an empty selection sends no request") },
            Some(Version::new(0, 4, 9)),
        );

        let files = table.fetch_blob_files_impl("image", &[]).await.unwrap();
        assert!(files.is_empty());

        let blobs = table.fetch_blobs_impl("image", &[]).await.unwrap();
        assert_eq!(blobs.len(), 0);
    }

    #[derive(Debug)]
    struct CountingRangeRequester {
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl BlobRangeRequester for CountingRangeRequester {
        async fn request_range(
            &self,
            range: Range<u64>,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<BlobRangeResponse> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let start = range.start as usize;
            let end = (range.end - 1) as usize;
            let response = http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", PAYLOAD.len()),
                )
                .body(PAYLOAD[start..=end].to_vec())
                .unwrap();
            Ok(("range".to_string(), Response::from(response)).into())
        }
    }

    #[tokio::test]
    async fn read_ranges_run_bounded_and_preserve_order() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let requester: Arc<dyn BlobRangeRequester> = Arc::new(CountingRangeRequester {
            in_flight,
            max_in_flight: max_in_flight.clone(),
        });
        let file = RemoteBlobFile::new(requester, PAYLOAD.len() as u64, Some(5));

        let ranges: Vec<_> = (0..16u64).map(|start| start..start + 2).collect();
        let output = file.read_ranges(&ranges).await.unwrap();

        for (range, bytes) in ranges.iter().zip(&output) {
            assert_eq!(
                bytes.as_ref(),
                &PAYLOAD[range.start as usize..range.end as usize]
            );
        }
        let max = max_in_flight.load(Ordering::SeqCst);
        assert!(max > 1, "range reads never overlapped");
        assert!(
            max <= BLOB_REQUEST_CONCURRENCY,
            "{max} range reads in flight exceeds the bound"
        );
    }

    #[tokio::test]
    async fn read_ranges_reject_out_of_bounds_range() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        let oob = file
            .read_ranges(&[0..2, 0..PAYLOAD.len() as u64 + 1])
            .await
            .unwrap_err();
        assert!(oob.to_string().contains("exceeds blob size"), "got: {oob}");
    }

    #[tokio::test]
    async fn range_read_rejects_200_response() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |request| {
                let range = request
                    .headers()
                    .get(header::RANGE)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                if range == "bytes=0-0" {
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(
                            header::CONTENT_RANGE,
                            format!("bytes 0-0/{}", PAYLOAD.len()),
                        )
                        .header(VERSION_HEADER, "5")
                        .body(vec![PAYLOAD[0]])
                        .unwrap();
                }
                http::Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_LENGTH, PAYLOAD.len().to_string())
                    .body(PAYLOAD.to_vec())
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );

        let mut files = table.probe_files("image", &[10]).await.unwrap();
        let file = files.remove(0).unwrap();
        let error = file.read_range(1..3).await.unwrap_err();
        assert!(error.to_string().contains("206"), "got: {error}");
    }

    #[tokio::test]
    async fn impossible_range_capacity_returns_an_error() {
        let size = isize::MAX as u64 + 1;
        let table = RemoteTable::new_mock(
            "videos".to_string(),
            move |request| {
                let range = request
                    .headers()
                    .get(header::RANGE)
                    .unwrap()
                    .to_str()
                    .unwrap();
                let end = if range == "bytes=0-0" { 0 } else { size - 1 };
                http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_RANGE, format!("bytes 0-{end}/{size}"))
                    .header(VERSION_HEADER, "5")
                    .body(vec![1])
                    .unwrap()
            },
            Some(Version::new(0, 5, 0)),
        );
        let file = table
            .probe_files("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();
        let error = file.read().await.unwrap_err();
        assert!(
            error.to_string().contains("cannot allocate blob range"),
            "{error}"
        );
        assert_eq!(file.tell().await.unwrap(), 0);
    }

    #[derive(Debug)]
    struct TrackedRangeBody {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for TrackedRangeBody {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackedRangeBody {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn close_releases_existing_cache_after_in_flight_read_ends() {
        for cancel in [false, true] {
            let release = Arc::new(tokio::sync::Barrier::new(2));
            let started = Arc::new(tokio::sync::Notify::new());
            let dropped = Arc::new(AtomicBool::new(false));
            let file = Arc::new(RemoteBlobFile::new(
                Arc::new(BlockingRangeRequester {
                    release: release.clone(),
                    started: started.clone(),
                    block_start: 4,
                    tracked: Some(dropped.clone()),
                }),
                PAYLOAD.len() as u64,
                Some(5),
            ));
            file.set_buffer_size(4).await.unwrap();
            assert_eq!(file.read_up_to(1).await.unwrap().as_ref(), &PAYLOAD[..1]);
            file.seek(3).await.unwrap();
            assert!(
                !dropped.load(Ordering::Acquire),
                "warm cache was not retained"
            );
            let reader = {
                let file = file.clone();
                tokio::spawn(async move { file.read_up_to(2).await })
            };
            started.notified().await;
            tokio::time::timeout(Duration::from_millis(50), file.close())
                .await
                .expect("close waited for I/O")
                .unwrap();
            if cancel {
                reader.abort();
                assert!(reader.await.unwrap_err().is_cancelled());
            } else {
                release.wait().await;
                let error = reader.await.unwrap().unwrap_err();
                assert!(error.to_string().contains("already closed"), "{error}");
            }
            assert!(
                dropped.load(Ordering::Acquire),
                "closed handle retained its old window (cancel={cancel})"
            );
        }
    }

    #[derive(Debug)]
    struct BlockingRangeRequester {
        release: Arc<tokio::sync::Barrier>,
        started: Arc<tokio::sync::Notify>,
        block_start: usize,
        tracked: Option<Arc<AtomicBool>>,
    }

    #[async_trait::async_trait]
    impl BlobRangeRequester for BlockingRangeRequester {
        async fn request_range(
            &self,
            range: Range<u64>,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<BlobRangeResponse> {
            let start = range.start as usize;
            let end = (range.end - 1) as usize;
            if start == self.block_start {
                self.started.notify_one();
                self.release.wait().await;
            }
            let bytes = match &self.tracked {
                Some(dropped) if start == 0 => Bytes::from_owner(TrackedRangeBody {
                    bytes: PAYLOAD[start..=end].to_vec(),
                    dropped: dropped.clone(),
                }),
                _ => Bytes::copy_from_slice(&PAYLOAD[start..=end]),
            };
            let response = http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", PAYLOAD.len()),
                )
                .body(bytes)
                .unwrap();
            Ok(("hung".to_string(), Response::from(response)).into())
        }
    }

    #[tokio::test]
    async fn close_returns_while_a_sequential_read_is_in_flight() {
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let started = Arc::new(tokio::sync::Notify::new());
        let requester: Arc<dyn BlobRangeRequester> = Arc::new(BlockingRangeRequester {
            release: release.clone(),
            started: started.clone(),
            block_start: 0,
            tracked: None,
        });
        let file = Arc::new(RemoteBlobFile::new(
            requester,
            PAYLOAD.len() as u64,
            Some(5),
        ));

        let reader = {
            let file = file.clone();
            tokio::spawn(async move { file.read_up_to(4).await })
        };
        started.notified().await;

        tokio::time::timeout(Duration::from_millis(50), file.close())
            .await
            .expect("close waited on the hung read")
            .unwrap();
        assert!(file.is_closed());

        release.wait().await;
        let error = reader.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("already closed"), "got: {error}");
        assert!(
            file.read_range(0..1)
                .await
                .unwrap_err()
                .to_string()
                .contains("already closed")
        );
    }

    fn blob_ipc_stream(values: &[Option<&[u8]>], column: &str) -> Vec<u8> {
        let array = LargeBinaryArray::from(values.to_vec());
        let batch =
            RecordBatch::try_from_iter([(column, Arc::new(array) as Arc<dyn Array>)]).unwrap();
        let mut bytes = Vec::new();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    fn blob_locations_json(body: serde_json::Value, version: u64) -> http::Response<Vec<u8>> {
        http::Response::builder()
            .status(200)
            .header(VERSION_HEADER, version.to_string())
            .header(QUERY_SNAPSHOT_HEADER, TEST_SNAPSHOT_IDENTITY)
            .body(body.to_string().into_bytes())
            .unwrap()
    }

    fn direct_blob_file_table<F>(
        kind: BlobKind,
        size: u64,
        object_handler: F,
    ) -> RemoteTable<crate::remote::client::test_utils::MockSender>
    where
        F: Fn(reqwest::Request) -> http::Response<Vec<u8>> + Send + Sync + 'static,
    {
        RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    return object_handler(request);
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/query/" {
                    let batch = RecordBatch::try_from_iter([
                        (
                            "image",
                            Arc::new(descriptors(&[Some((kind, size))])) as Arc<dyn Array>,
                        ),
                        (
                            ROW_ID,
                            Arc::new(UInt64Array::from_iter_values([10])) as Arc<dyn Array>,
                        ),
                    ])
                    .unwrap();
                    let mut body = Vec::new();
                    let mut writer =
                        arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                    drop(writer);
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .body(body)
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob_locations/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["row_ids"], serde_json::json!([10]));
                    assert_eq!(body["version"], serde_json::json!(7));
                    assert_eq!(body["include_inline_data"], serde_json::json!(false));
                    return blob_locations_json(
                        serde_json::json!({
                            "sources": [{
                                "url": "https://objects.example/sidecar.blob?sig=test",
                                "expires_at_unix_secs": 4102444800u64,
                            }],
                            "items": [{
                                "kind": "presigned",
                                "source_index": 0,
                                "offset": 100,
                                "size": size,
                            }],
                        }),
                        7,
                    );
                }
                if path == "/v1/table/my_table/blob/image/10/bytes" {
                    assert_eq!(
                        request
                            .url()
                            .query_pairs()
                            .find(|(name, _)| name == "version")
                            .map(|(_, value)| value.into_owned()),
                        Some("7".to_string())
                    );
                    let response = range_response(&request, PAYLOAD);
                    let content_range = response.headers()[header::CONTENT_RANGE].clone();
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, content_range)
                        .header(QUERY_SNAPSHOT_HEADER, TEST_SNAPSHOT_IDENTITY)
                        .header(VERSION_HEADER, "7")
                        .body(response.into_body())
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        )
    }

    #[tokio::test]
    async fn describe_seeds_blob_locations_capability() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            |_| {
                http::Response::builder()
                    .status(200)
                    .header("x-lancedb-blob-locations", "1")
                    .body(r#"{"version":1,"schema":{"fields":[]}}"#.as_bytes().to_vec())
                    .unwrap()
            },
            Some(Version::new(1, 13, 0)),
        );
        table.describe().await.unwrap();
        assert!(
            table
                .blob_locations_supported
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn direct_blob_file_translates_physical_offsets_and_preserves_nulls() {
        let object_requests = Arc::new(StdMutex::new(Vec::new()));
        let captured = object_requests.clone();
        let table =
            direct_blob_file_table(BlobKind::Packed, PAYLOAD.len() as u64, move |request| {
                assert_eq!(request.method(), reqwest::Method::GET);
                assert_eq!(
                    request.headers().get(header::RANGE).unwrap(),
                    "bytes=106-110"
                );
                assert!(request.headers().get("authorization").is_none());
                assert!(request.headers().get("x-api-key").is_none());
                captured.lock().unwrap().push(request.url().to_string());
                http::Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header(header::CONTENT_RANGE, "bytes 106-110/500")
                    .body(PAYLOAD[6..11].to_vec())
                    .unwrap()
            });
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
        let file = files.pop().unwrap().unwrap();
        assert_eq!(
            file.read_range(6..11).await.unwrap().as_ref(),
            &PAYLOAD[6..11]
        );
        assert_eq!(object_requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn direct_blob_file_small_ranges_stay_on_the_proxy() {
        let table = direct_blob_file_table(BlobKind::Managed, PAYLOAD.len() as u64, |_| {
            panic!("small range must not read from object storage")
        });
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 10;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
        let file = files.pop().unwrap().unwrap();
        assert_eq!(
            file.read_range(6..11).await.unwrap().as_ref(),
            &PAYLOAD[6..11]
        );
    }

    #[tokio::test]
    async fn direct_blob_file_rejects_mismatched_content_range() {
        let table = direct_blob_file_table(BlobKind::Packed, PAYLOAD.len() as u64, |_| {
            http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_RANGE, "bytes 100-104/500")
                .body(PAYLOAD[..5].to_vec())
                .unwrap()
        });
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
        let file = files.pop().unwrap().unwrap();
        let error = file.read_range(6..11).await.unwrap_err();
        assert!(error.to_string().contains("Content-Range"), "{error}");
    }

    #[tokio::test]
    async fn direct_blob_file_refreshes_expired_url_once() {
        const IDENTITY: &str =
            "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let locations = Arc::new(AtomicUsize::new(0));
        let captured_locations = locations.clone();
        let object = Arc::new(AtomicUsize::new(0));
        let captured_object = object.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    let call = captured_object.fetch_add(1, Ordering::SeqCst) + 1;
                    if request
                        .url()
                        .query()
                        .is_some_and(|query| query.contains("sig=old"))
                    {
                        return http::Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Vec::new())
                            .unwrap();
                    }
                    assert_eq!(call, 2);
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 100-104/500")
                        .body(PAYLOAD[..5].to_vec())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/query/" {
                    let batch = RecordBatch::try_from_iter([
                        (
                            "image",
                            Arc::new(descriptors(&[Some((
                                BlobKind::Packed,
                                PAYLOAD.len() as u64,
                            ))])) as Arc<dyn Array>,
                        ),
                        (
                            ROW_ID,
                            Arc::new(UInt64Array::from_iter_values([10])) as Arc<dyn Array>,
                        ),
                    ])
                    .unwrap();
                    let mut body = Vec::new();
                    let mut writer =
                        arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                    drop(writer);
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .body(body)
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob_locations/" {
                    let call = captured_locations.fetch_add(1, Ordering::SeqCst) + 1;
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["version"], serde_json::json!(7));
                    let sig = if call == 1 { "old" } else { "new" };
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": format!("https://objects.example/sidecar.blob?sig={sig}"),
                                }],
                                "items": [{
                                    "kind": "presigned",
                                    "source_index": 0,
                                    "offset": 100,
                                    "size": PAYLOAD.len(),
                                }],
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
        let file = files.pop().unwrap().unwrap();
        assert_eq!(file.read_range(0..5).await.unwrap().as_ref(), &PAYLOAD[..5]);
        assert_eq!(locations.load(Ordering::SeqCst), 2);
        assert_eq!(object.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn fetch_blobs_reads_large_presigned_inline_and_proxy_rows_in_order() {
        const IDENTITY: &str =
            "v1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let proxied = Arc::new(StdMutex::new(Vec::new()));
        let captured_proxied = proxied.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    assert_eq!(
                        request.headers().get(header::RANGE).unwrap(),
                        "bytes=100-135"
                    );
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 100-135/500")
                        .body(PAYLOAD.to_vec())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["row_ids"], serde_json::json!([10, 20, 30, 40]));
                    assert_eq!(body["include_inline_data"], serde_json::json!(true));
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": "https://objects.example/sidecar.blob?sig=test",
                                }],
                                "items": [
                                    { "kind": "presigned", "source_index": 0, "offset": 100, "size": PAYLOAD.len() },
                                    { "kind": "inline", "size": 4, "data_base64": "AQIDBA==" },
                                    { "kind": "proxy", "size": PAYLOAD.len() },
                                    null
                                ],
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blobs/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["row_ids"], serde_json::json!([30]));
                    assert_eq!(body["version"], serde_json::json!(7));
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], IDENTITY);
                    captured_proxied.lock().unwrap().push(30);
                    let body = blob_ipc_stream(&[Some(PAYLOAD)], "image");
                    return http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(body)
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let blobs = table
            .fetch_blobs_impl("image", &[10, 20, 30, 40])
            .await
            .unwrap();
        assert_eq!(
            blobs,
            LargeBinaryArray::from(vec![
                Some(PAYLOAD),
                Some(b"\x01\x02\x03\x04".as_slice()),
                Some(PAYLOAD),
                None,
            ])
        );
        assert_eq!(*proxied.lock().unwrap(), [30]);
    }

    #[tokio::test]
    async fn fetch_blobs_keeps_small_presigned_values_on_the_proxy() {
        let proxied = Arc::new(StdMutex::new(Vec::new()));
        let captured_proxied = proxied.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    panic!("small presigned value must not use object storage");
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, TEST_SNAPSHOT_IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": "https://objects.example/sidecar.blob?sig=test",
                                }],
                                "items": [{
                                    "kind": "presigned",
                                    "source_index": 0,
                                    "offset": 100,
                                    "size": PAYLOAD.len(),
                                }],
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blobs/" {
                    captured_proxied.lock().unwrap().push(10);
                    let body = blob_ipc_stream(&[Some(PAYLOAD)], "image");
                    return http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, TEST_SNAPSHOT_IDENTITY)
                        .body(body)
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 100;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let blobs = table.fetch_blobs_impl("image", &[10]).await.unwrap();
        assert_eq!(blobs, LargeBinaryArray::from(vec![Some(PAYLOAD)]));
        assert_eq!(*proxied.lock().unwrap(), [10]);
    }

    #[tokio::test]
    async fn fetch_blob_ranges_routes_large_ranges_direct_and_small_ranges_proxy() {
        const IDENTITY: &str =
            "v1:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    assert_eq!(
                        request.headers().get(header::RANGE).unwrap(),
                        "bytes=100-109"
                    );
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 100-109/500")
                        .body(PAYLOAD[..10].to_vec())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["row_ids"], serde_json::json!([10, 30, 40]));
                    assert_eq!(body["include_inline_data"], serde_json::json!(false));
                    assert_eq!(body["version"], serde_json::Value::Null);
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": "https://objects.example/sidecar.blob?sig=test",
                                }],
                                "items": [
                                    { "kind": "presigned", "source_index": 0, "offset": 100, "size": PAYLOAD.len() },
                                    { "kind": "proxy", "size": PAYLOAD.len() },
                                    null
                                ],
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blob_ranges/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["version"], serde_json::json!(7));
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], IDENTITY);
                    assert_eq!(
                        body["ranges"],
                        serde_json::json!([
                            { "row_id": 20, "offset": 0, "length": 9 },
                            { "row_id": 30, "offset": 0, "length": 12 },
                        ])
                    );
                    let body =
                        blob_ipc_stream(&[Some(&PAYLOAD[..9]), Some(&PAYLOAD[..12])], "value");
                    return http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(body)
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 10;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_ranges_capability(&headers);
        table.seed_blob_locations_capability(&headers);

        let ranges = table
            .fetch_blob_ranges(
                "image",
                &[
                    BlobRangeRequest::new(10, 0, 10),
                    BlobRangeRequest::new(20, 0, 9),
                    BlobRangeRequest::new(30, 0, 12),
                    BlobRangeRequest::new(40, 0, 10),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            ranges,
            LargeBinaryArray::from(vec![
                Some(&PAYLOAD[..10]),
                Some(&PAYLOAD[..9]),
                Some(&PAYLOAD[..12]),
                None,
            ])
        );
    }

    #[tokio::test]
    async fn fetch_blob_ranges_falls_back_to_proxy_when_locations_lose_their_plan() {
        const IDENTITY: &str =
            "v1:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    return http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blob_ranges/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(
                        body["ranges"],
                        serde_json::json!([
                            { "row_id": 10, "offset": 0, "length": 10 },
                            { "row_id": 20, "offset": 0, "length": 1 },
                        ])
                    );
                    let body =
                        blob_ipc_stream(&[Some(&PAYLOAD[..10]), Some(&PAYLOAD[..1])], "value");
                    return http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(body)
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 10;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_ranges_capability(&headers);
        table.seed_blob_locations_capability(&headers);

        let ranges = table
            .fetch_blob_ranges(
                "image",
                &[
                    BlobRangeRequest::new(10, 0, 10),
                    BlobRangeRequest::new(20, 0, 1),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            ranges,
            LargeBinaryArray::from(vec![Some(&PAYLOAD[..10]), Some(&PAYLOAD[..1])])
        );
    }

    #[tokio::test]
    async fn fetch_blob_ranges_small_only_batches_never_resolve_locations() {
        const IDENTITY: &str =
            "v1:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path();
                assert_ne!(path, "/v1/table/my_table/blob_locations/");
                assert_eq!(path, "/v1/table/my_table/fetch_blob_ranges/");
                let body = blob_ipc_stream(&[Some(&PAYLOAD[..1]), Some(&PAYLOAD[1..2])], "value");
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/vnd.apache.arrow.stream")
                    .header(VERSION_HEADER, "7")
                    .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                    .body(body)
                    .unwrap()
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 10;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_ranges_capability(&headers);
        table.seed_blob_locations_capability(&headers);

        let ranges = table
            .fetch_blob_ranges(
                "image",
                &[
                    BlobRangeRequest::new(10, 0, 1),
                    BlobRangeRequest::new(20, 1, 1),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            ranges,
            LargeBinaryArray::from(vec![Some(&PAYLOAD[..1]), Some(&PAYLOAD[1..2])])
        );
    }

    #[tokio::test]
    async fn fetch_blobs_falls_back_to_proxy_when_locations_lose_their_plan() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    return http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap();
                }
                assert_eq!(path, "/v1/table/my_table/fetch_blobs/");
                let body = blob_ipc_stream(&[Some(PAYLOAD)], "image");
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/vnd.apache.arrow.stream")
                    .header(VERSION_HEADER, "7")
                    .body(body)
                    .unwrap()
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let blobs = table.fetch_blobs_impl("image", &[10]).await.unwrap();
        assert_eq!(blobs, LargeBinaryArray::from(vec![Some(PAYLOAD)]));
    }

    #[tokio::test]
    async fn blob_file_resolver_503_after_pinning_keeps_version_and_identity_on_proxy() {
        const IDENTITY: &str =
            "v1:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path();
                if path == "/v1/table/my_table/query/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    let requested = filter_row_ids(body["filter"].as_str().unwrap());
                    let rows = requested
                        .iter()
                        .map(|row_id| (*row_id, Some((BlobKind::Packed, PAYLOAD.len() as u64))))
                        .collect::<Vec<_>>();
                    let batch = RecordBatch::try_from_iter([
                        (
                            "image",
                            Arc::new(descriptors(
                                &rows.iter().map(|(_, row)| *row).collect::<Vec<_>>(),
                            )) as Arc<dyn Array>,
                        ),
                        (
                            ROW_ID,
                            Arc::new(UInt64Array::from_iter_values(
                                rows.iter().map(|(row_id, _)| *row_id),
                            )) as Arc<dyn Array>,
                        ),
                    ])
                    .unwrap();
                    let mut body = Vec::new();
                    let mut writer =
                        arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
                    writer.write(&batch).unwrap();
                    writer.finish().unwrap();
                    drop(writer);
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .body(body)
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob_locations/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    let requested = body["row_ids"].as_array().unwrap().len();
                    if requested != MAX_FETCH_BLOBS_ROW_IDS {
                        return http::Response::builder()
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .body(Vec::new())
                            .unwrap();
                    }
                    let items = (0..requested)
                        .map(|_| {
                            serde_json::json!({
                                "kind": "presigned",
                                "source_index": 0,
                                "offset": 100,
                                "size": PAYLOAD.len(),
                            })
                        })
                        .collect::<Vec<_>>();
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": "https://objects.example/sidecar.blob?sig=test",
                                }],
                                "items": items,
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob/image/0/bytes" {
                    let parameters = request
                        .url()
                        .query_pairs()
                        .into_owned()
                        .collect::<HashMap<_, _>>();
                    assert_eq!(parameters["version"], "7");
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], IDENTITY);
                    let response = range_response(&request, PAYLOAD);
                    let content_range = response.headers()[header::CONTENT_RANGE].clone();
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, content_range)
                        .header(QUERY_SNAPSHOT_HEADER, IDENTITY)
                        .header(VERSION_HEADER, "7")
                        .body(response.into_body())
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let row_ids = (0..MAX_FETCH_BLOBS_ROW_IDS as u64 + 1).collect::<Vec<_>>();
        let files = table
            .fetch_blob_files_impl("image", &row_ids)
            .await
            .unwrap();
        assert_eq!(files.len(), row_ids.len());
        let file = files[0].as_ref().unwrap();
        assert_eq!(file.read_range(0..1).await.unwrap().as_ref(), &PAYLOAD[..1]);
    }

    #[tokio::test]
    async fn oversized_whole_blob_fallback_after_resolver_503_does_not_resolve_again() {
        let locations = Arc::new(AtomicUsize::new(0));
        let captured_locations = locations.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    panic!("proxy fallback must not use object storage");
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    captured_locations.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap();
                }
                if path == "/v1/table/my_table/describe/" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version":7,"schema":{"fields":[]}}"#.as_bytes().to_vec())
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blobs/" {
                    return http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(
                            "fetch_blobs accepts at most 64 MiB of total blob bytes per request"
                                .as_bytes()
                                .to_vec(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob/image/10/bytes" {
                    return range_response(&request, PAYLOAD);
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let blobs = table.fetch_blobs_impl("image", &[10]).await.unwrap();
        assert_eq!(blobs, LargeBinaryArray::from(vec![Some(PAYLOAD)]));
        assert_eq!(locations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn object_store_503_does_not_masquerade_as_resolver_unavailable() {
        let proxy_calls = Arc::new(AtomicUsize::new(0));
        let captured_proxy = proxy_calls.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    return http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    return http::Response::builder()
                        .status(200)
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, TEST_SNAPSHOT_IDENTITY)
                        .body(
                            serde_json::json!({
                                "sources": [{
                                    "url": "https://objects.example/sidecar.blob?sig=test",
                                }],
                                "items": [{
                                    "kind": "presigned",
                                    "source_index": 0,
                                    "offset": 100,
                                    "size": PAYLOAD.len(),
                                }],
                            })
                            .to_string()
                            .into_bytes(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blobs/" {
                    captured_proxy.fetch_add(1, Ordering::SeqCst);
                    let body = blob_ipc_stream(&[Some(PAYLOAD)], "image");
                    return http::Response::builder()
                        .status(200)
                        .header("content-type", "application/vnd.apache.arrow.stream")
                        .header(VERSION_HEADER, "7")
                        .body(body)
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 13, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);

        let error = table.fetch_blobs_impl("image", &[10]).await.unwrap_err();
        assert_eq!(error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
        assert_eq!(proxy_calls.load(Ordering::SeqCst), 0);
    }

    fn location_response(
        identity: Option<&str>,
        item: serde_json::Value,
    ) -> http::Response<Vec<u8>> {
        location_response_count(identity, item, 1)
    }

    fn location_response_count(
        identity: Option<&str>,
        item: serde_json::Value,
        count: usize,
    ) -> http::Response<Vec<u8>> {
        let mut response = http::Response::builder()
            .status(200)
            .header(VERSION_HEADER, "7");
        if let Some(identity) = identity {
            response = response.header(QUERY_SNAPSHOT_HEADER, identity);
        }
        response
            .body(
                serde_json::json!({
                    "sources": [{"url": "https://objects.example/object"}],
                    "items": vec![item; count],
                })
                .to_string()
                .into_bytes(),
            )
            .unwrap()
    }

    fn descriptor_take_response(version: u8, row_id: u64) -> http::Response<Vec<u8>> {
        let batch = RecordBatch::try_from_iter([
            (
                "image",
                Arc::new(descriptors(&[Some((BlobKind::Packed, 1))])) as Arc<dyn Array>,
            ),
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values([row_id])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();
        let mut body = Vec::new();
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(&mut body, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        http::Response::builder()
            .status(200)
            .header(VERSION_HEADER, version.to_string())
            .body(body)
            .unwrap()
    }

    #[tokio::test]
    async fn resolver_rejects_a_response_without_a_snapshot_identity() {
        let object_calls = Arc::new(AtomicUsize::new(0));
        let captured_object = object_calls.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    captured_object.fetch_add(1, Ordering::SeqCst);
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 0-0/1")
                        .body(b"A".to_vec())
                        .unwrap();
                }
                if request.url().path() == "/v1/table/my_table/blob_locations/" {
                    return location_response(
                        None,
                        serde_json::json!({
                            "kind": "presigned", "source_index": 0, "offset": 0, "size": 1
                        }),
                    );
                }
                panic!("unexpected request: {}", request.url().path());
            },
            Some(Version::new(1, 15, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);
        table.seed_blob_ranges_capability(&headers);

        let error = table
            .fetch_blob_ranges_impl("image", &[BlobRangeRequest::new(10, 0, 1)])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("snapshot identity"), "{error}");
        assert_eq!(object_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn resolver_rejects_a_different_identity_in_a_later_partition() {
        let resolves = Arc::new(AtomicUsize::new(0));
        let captured_resolves = resolves.clone();
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    panic!("no object read should happen after an identity mismatch");
                }
                if request.url().path() == "/v1/table/my_table/blob_locations/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    let count = body["row_ids"].as_array().unwrap().len();
                    let call = captured_resolves.fetch_add(1, Ordering::SeqCst);
                    let identity = if call == 0 { SNAPSHOT_A } else { SNAPSHOT_B };
                    return location_response_count(
                        Some(identity),
                        serde_json::json!({
                            "kind": "presigned", "source_index": 0, "offset": 0, "size": 1
                        }),
                        count,
                    );
                }
                panic!("unexpected request: {}", request.url().path());
            },
            Some(Version::new(1, 15, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);
        table.seed_blob_ranges_capability(&headers);
        let requests = (0..=MAX_FETCH_BLOBS_ROW_IDS as u64)
            .map(|row_id| BlobRangeRequest::new(row_id, 0, 1))
            .collect::<Vec<_>>();
        let error = table
            .fetch_blob_ranges_impl("image", &requests)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not verify the query snapshot identity"),
            "{error}"
        );
    }

    fn refresh_503_table() -> RemoteTable<crate::remote::client::test_utils::MockSender> {
        let resolves = Arc::new(AtomicUsize::new(0));
        let mut table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    return http::Response::builder()
                        .status(StatusCode::FORBIDDEN)
                        .body(Vec::new())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/blob_locations/" {
                    if resolves.fetch_add(1, Ordering::SeqCst) == 0 {
                        return location_response(
                            Some(SNAPSHOT_A),
                            serde_json::json!({
                                "kind": "presigned", "source_index": 0, "offset": 0, "size": 1
                            }),
                        );
                    }
                    return http::Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(Vec::new())
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob/image/10/bytes" {
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], SNAPSHOT_A);
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 0-0/1")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, SNAPSHOT_B)
                        .body(b"B".to_vec())
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 15, 0)),
        );
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        headers.insert("x-lancedb-blob-ranges", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);
        table.seed_blob_ranges_capability(&headers);
        table
    }

    #[tokio::test]
    async fn whole_read_proxy_fallback_after_refresh_503_keeps_resolver_identity() {
        let error = refresh_503_table()
            .fetch_blobs_impl("image", &[10])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not verify the query snapshot identity"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn range_read_proxy_fallback_after_refresh_503_keeps_resolver_identity() {
        let error = refresh_503_table()
            .fetch_blob_ranges_impl("image", &[BlobRangeRequest::new(10, 0, 1)])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not verify the query snapshot identity"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn oversized_proxy_fallback_keeps_resolver_identity() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                let path = request.url().path();
                if path == "/v1/table/my_table/describe/" {
                    return http::Response::builder()
                        .status(200)
                        .body(r#"{"version":7,"schema":{"fields":[]}}"#.as_bytes().to_vec())
                        .unwrap();
                }
                if path == "/v1/table/my_table/fetch_blobs/" {
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], SNAPSHOT_A);
                    return http::Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(
                            b"fetch_blobs accepts at most 64 MiB of total blob bytes per request"
                                .to_vec(),
                        )
                        .unwrap();
                }
                if path == "/v1/table/my_table/blob/image/10/bytes" {
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], SNAPSHOT_A);
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 0-0/1")
                        .header(VERSION_HEADER, "7")
                        .header(QUERY_SNAPSHOT_HEADER, SNAPSHOT_B)
                        .body(b"B".to_vec())
                        .unwrap();
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 15, 0)),
        );
        let snapshot = ReadSnapshot {
            version: Some(7),
            ..Default::default()
        };
        let error = table
            .fetch_blobs_proxy_array("image", &[10], snapshot, Some(SNAPSHOT_A.into()))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("did not verify the query snapshot identity"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn direct_open_keeps_the_supplied_snapshot_when_capability_appears() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().path() == "/v1/table/my_table/query/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["version"], serde_json::json!(7));
                    return descriptor_take_response(8, 20);
                }
                panic!("unexpected request: {}", request.url().path());
            },
            Some(Version::new(1, 15, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);
        let snapshot = ReadSnapshot {
            version: Some(7),
            ..Default::default()
        };
        let error = table
            .fetch_blob_files_with_snapshot("image", &[20], snapshot, true, Some(SNAPSHOT_A.into()))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("expected pinned version 7"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn direct_open_succeeds_at_the_supplied_snapshot() {
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            move |request| {
                if request.url().host_str() == Some("objects.example") {
                    return http::Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header(header::CONTENT_RANGE, "bytes 0-0/1")
                        .body(b"A".to_vec())
                        .unwrap();
                }
                let path = request.url().path();
                if path == "/v1/table/my_table/query/" {
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["version"], serde_json::json!(7));
                    return descriptor_take_response(7, 20);
                }
                if path == "/v1/table/my_table/blob_locations/" {
                    assert_eq!(request.headers()[QUERY_SNAPSHOT_HEADER], SNAPSHOT_A);
                    return location_response(
                        Some(SNAPSHOT_A),
                        serde_json::json!({
                            "kind": "presigned", "source_index": 0, "offset": 0, "size": 1
                        }),
                    );
                }
                panic!("unexpected request: {path}");
            },
            Some(Version::new(1, 15, 0)),
        );
        let mut table = table;
        table.client.blob_presign_threshold_bytes = 1;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-lancedb-blob-locations", "1".parse().unwrap());
        table.seed_blob_locations_capability(&headers);
        let snapshot = ReadSnapshot {
            version: Some(7),
            ..Default::default()
        };
        let mut files = table
            .fetch_blob_files_with_snapshot("image", &[20], snapshot, true, Some(SNAPSHOT_A.into()))
            .await
            .unwrap();
        let file = files.pop().unwrap().unwrap();
        assert_eq!(file.read_range(0..1).await.unwrap().as_ref(), b"A");
    }
}
