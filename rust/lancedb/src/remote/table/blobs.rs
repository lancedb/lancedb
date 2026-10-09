// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Cloud blob column listing, whole-byte fetch, and seekable HTTP byte-range handles.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_array::{Array, LargeBinaryArray, StructArray, UInt8Array, UInt64Array};
use arrow_schema::DataType;
use bytes::{Bytes, BytesMut};
use datafusion_expr::{col, lit};
use futures::{StreamExt, TryStreamExt};
use lance::dataset::ROW_ID;
use lance_core::datatypes::BlobKind;
use reqwest::{Response, StatusCode, header};
use tokio::sync::Mutex;

use crate::Error;
use crate::blob::BlobFile;
use crate::error::Result;
use crate::query::{QueryFilter, QueryRequest, Select};
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};
use crate::table::{AnyQuery, BaseTable};

use super::{
    FreshnessHeaders, FreshnessState, ReadSnapshot, RemoteTable, VERSION_HEADER,
    freshness_headers_snapshot,
};

// The Cloud route rejects larger row-id lists. Its separate 64 MiB byte limit
// is handled by splitting a rejected request below.
const MAX_FETCH_BLOBS_ROW_IDS: usize = 1024;

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
}

#[async_trait::async_trait]
trait BlobRangeRequester: Send + Sync + std::fmt::Debug {
    async fn request_range(
        &self,
        range_header: &str,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<(String, Response)>;
}

#[derive(Debug)]
struct TableBlobRangeRequester<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    path: String,
    version: Option<u64>,
    branch: Option<String>,
    freshness: Arc<std::sync::Mutex<FreshnessState>>,
    parent_freshness: Arc<std::sync::Mutex<FreshnessState>>,
    parent_freshness_request: FreshnessHeaders,
    read_consistency_interval: Option<Duration>,
}

#[async_trait::async_trait]
impl<S: HttpSend> BlobRangeRequester for TableBlobRangeRequester<S> {
    async fn request_range(
        &self,
        range_header: &str,
        mode: RangeRequestMode,
        version: Option<u64>,
    ) -> Result<(String, Response)> {
        let freshness_request =
            freshness_headers_snapshot(&self.freshness, self.read_consistency_interval);
        let mut request = freshness_request
            .apply(self.client.get(&self.path))
            .header(header::RANGE, range_header);
        if let Some(version) = version.or(self.version) {
            request = request.query(&[("version", version)]);
        }
        if let Some(branch) = &self.branch {
            request = request.query(&[("branch", branch)]);
        }
        let (request_id, response) = self.client.send_with_retry(request, None, true).await?;
        // Preserve 416 size-probe responses so the caller can detect empty blobs.
        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE
            && matches!(mode, RangeRequestMode::SizeProbe)
        {
            return Ok((request_id, response));
        }
        let response = self.client.check_response(&request_id, response).await?;
        freshness_request.observe_headers(&self.freshness, response.headers());
        self.parent_freshness_request
            .observe_headers(&self.parent_freshness, response.headers());
        Ok((request_id, response))
    }
}

#[derive(Debug)]
struct SequentialResponse {
    response: Response,
    request_id: String,
    buffered: Bytes,
}

#[derive(Debug, Default)]
struct RemoteBlobState {
    cursor: u64,
    sequential_response: Option<SequentialResponse>,
}

/// Seekable Cloud blob handle over HTTP Range.
///
/// Nonempty handles read the table version returned by their size probe.
#[derive(Debug)]
pub struct RemoteBlobFile {
    requester: Arc<dyn BlobRangeRequester>,
    state: Mutex<RemoteBlobState>,
    closed: AtomicBool,
    size: u64,
    version: Option<u64>,
}

impl RemoteBlobFile {
    fn new(requester: Arc<dyn BlobRangeRequester>, size: u64, version: Option<u64>) -> Self {
        Self {
            requester,
            state: Mutex::new(RemoteBlobState::default()),
            closed: AtomicBool::new(false),
            size,
            version,
        }
    }

    /// Close the handle without waiting for an in-flight read.
    pub(crate) async fn close(&self) -> lance_core::Result<()> {
        self.closed.store(true, Ordering::Release);
        // Drop a retained response when the state lock is immediately available.
        // A reader holding the lock drops it instead once it observes the flag.
        if let Ok(mut state) = self.state.try_lock() {
            state.sequential_response = None;
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
        let range_header = format!("bytes={}-{}", range.start, range.end - 1);
        let (request_id, response) = self
            .requester
            .request_range(&range_header, RangeRequestMode::DataRead, self.version)
            .await
            .map_err(remote_blob_error)?;
        self.ensure_open()?;
        validate_partial_response(&response, range.clone(), self.size)?;
        let bytes = response
            .bytes()
            .await
            .err_to_http(request_id)
            .map_err(remote_blob_error)?;
        self.ensure_open()?;
        if bytes.len() as u64 != range.end - range.start {
            return Err(remote_blob_error(format!(
                "byte range returned {} bytes, expected {}",
                bytes.len(),
                range.end - range.start
            )));
        }
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
        self.read_up_to_locked(&mut state, remaining).await
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

        // Remove the retained response from shared state before awaiting. Failed or
        // cancelled reads leave the committed cursor unchanged and force the next
        // read to open a fresh response.
        let mut sequential_response = state.sequential_response.take();
        let mut cursor = state.cursor;
        let mut output = BytesMut::with_capacity(target_len);
        while output.len() < target_len {
            if sequential_response.is_none() {
                let range_header = format!("bytes={cursor}-");
                let (request_id, response) = self
                    .requester
                    .request_range(&range_header, RangeRequestMode::DataRead, self.version)
                    .await
                    .map_err(remote_blob_error)?;
                self.ensure_open()?;
                validate_partial_response(&response, cursor..self.size, self.size)?;
                sequential_response = Some(SequentialResponse {
                    response,
                    request_id,
                    buffered: Bytes::new(),
                });
            }

            let needed = target_len - output.len();
            let active = sequential_response.as_mut().unwrap();
            if !active.buffered.is_empty() {
                let take = needed.min(active.buffered.len());
                output.extend_from_slice(&active.buffered.split_to(take));
                cursor += take as u64;
                continue;
            }
            let chunk = active
                .response
                .chunk()
                .await
                .err_to_http(active.request_id.clone())
                .map_err(remote_blob_error)?;
            self.ensure_open()?;
            let chunk = chunk.ok_or_else(|| {
                remote_blob_error("response ended before the requested blob range")
            })?;
            active.buffered = chunk;
        }
        self.ensure_open()?;
        state.cursor = cursor;
        if state.cursor < self.size {
            state.sequential_response = sequential_response;
        }
        Ok(output.freeze())
    }

    pub(crate) async fn seek(&self, new_cursor: u64) -> lance_core::Result<()> {
        self.ensure_open()?;
        let mut state = self.state.lock().await;
        self.ensure_open()?;
        state.sequential_response = None;
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
    if content_range != (expected.start, expected.end - 1, size) {
        return Err(remote_blob_error(format!(
            "expected Content-Range 'bytes {}-{}/{}', got 'bytes {}-{}/{}'",
            expected.start,
            expected.end - 1,
            size,
            content_range.0,
            content_range.1,
            content_range.2
        )));
    }
    Ok(())
}

impl<S: HttpSend> RemoteTable<S> {
    async fn reject_list_blob_path_if_known(&self, column: &str) -> Result<()> {
        // A dotted path needs client-side validation because the server may
        // report an internal type mismatch when the path crosses a list. Use
        // an already cached schema for top-level list fields as well.
        if column.contains('.') || self.schema_cache.try_get().is_some() {
            let schema = self.schema().await?;
            crate::blob::reject_list_blob_column(schema.as_ref(), column)?;
        }
        Ok(())
    }

    async fn explain_list_blob_bad_request(&self, column: &str, error: Error) -> Error {
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

    /// Blob v2 columns are marked in field metadata, which `describe` returns. Reading
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
        // A selection spanning requests must use one exact dataset version.
        // Resolve latest before the first chunk; a checked-out version is exact already.
        if row_ids.len() > MAX_FETCH_BLOBS_ROW_IDS && read_snapshot.version.is_none() {
            read_snapshot.version = Some(self.describe_read_snapshot(read_snapshot).await?.version);
        }
        let mut pending: Vec<&[u64]> = row_ids.chunks(MAX_FETCH_BLOBS_ROW_IDS).rev().collect();
        let mut chunks = Vec::new();
        while let Some(ids) = pending.pop() {
            match self.fetch_blobs_chunk(column, ids, read_snapshot).await {
                Ok(blobs) => chunks.push(blobs),
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
                            .fetch_blob_files_with_snapshot(column, ids, read_snapshot)
                            .await?;
                        let blob = match files.pop().unwrap() {
                            Some(file) => Some(file.read().await?),
                            None => None,
                        };
                        chunks.push(LargeBinaryArray::from(vec![blob.as_deref()]));
                    } else {
                        let mid = ids.len() / 2;
                        pending.push(&ids[mid..]);
                        pending.push(&ids[..mid]);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        if chunks.len() == 1 {
            return Ok(chunks.pop().unwrap());
        }
        let chunk_refs: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk as &dyn Array).collect();
        Ok(arrow::compute::concat(&chunk_refs)?
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("concatenating LargeBinary arrays returns LargeBinary")
            .clone())
    }

    async fn fetch_blobs_chunk(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
    ) -> Result<LargeBinaryArray> {
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
        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        let mut stream = match self.read_arrow_response(&request_id, response).await {
            Ok(stream) => stream,
            Err(error) => return Err(self.explain_list_blob_bad_request(column, error).await),
        };

        let mut blob_chunks: Vec<Arc<dyn Array>> = Vec::new();
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
            blob_chunks.push(arrow::compute::cast(blob_column, &DataType::LargeBinary)?);
        }
        let blobs = if blob_chunks.is_empty() {
            LargeBinaryArray::from(Vec::<Option<&[u8]>>::new())
        } else {
            let blob_chunk_refs: Vec<&dyn Array> = blob_chunks.iter().map(AsRef::as_ref).collect();
            arrow::compute::concat(&blob_chunk_refs)?
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| Error::Http {
                    source: "fetch_blobs could not read the concatenated response as LargeBinary"
                        .into(),
                    request_id: request_id.clone(),
                    status_code: None,
                })?
                .clone()
        };
        if blobs.len() != row_ids.len() {
            return Err(Error::Http {
                source: format!(
                    "fetch_blobs returned {} rows for {} row ids",
                    blobs.len(),
                    row_ids.len()
                )
                .into(),
                request_id,
                status_code: None,
            });
        }
        Ok(blobs)
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
        self.reject_list_blob_path_if_known(column).await?;
        // Empty requests do not require blob-route support.
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_blob_files_supported()?;

        let mut read_snapshot = self.snapshot_read_state().await;
        let sizes = self
            .take_blob_descriptor_sizes(column, row_ids, &mut read_snapshot)
            .await?;
        let version = read_snapshot.version;

        let mut files = Vec::with_capacity(row_ids.len());
        let mut probe_indices = Vec::new();
        let mut probe_requesters = Vec::new();
        for (index, row_id) in row_ids.iter().enumerate() {
            match sizes[row_id] {
                DescriptorSize::Null => files.push(None),
                DescriptorSize::Known(size) => {
                    let requester = self.blob_range_requester(column, *row_id, &read_snapshot);
                    files.push(Some(RemoteBlobFile::new(requester, size, version).into()));
                }
                DescriptorSize::Unresolved => {
                    files.push(None);
                    probe_indices.push(index);
                    probe_requesters.push(self.blob_range_requester(
                        column,
                        *row_id,
                        &read_snapshot,
                    ));
                }
            }
        }
        let probed = probe_blob_files(probe_requesters).await?;
        for (index, file) in probe_indices.into_iter().zip(probed) {
            files[index] = file;
        }
        Ok(files)
    }

    /// Probe each row once for its size.
    ///
    /// Serves callers that must not issue a query, such as a single blob that
    /// the whole-byte route could not return.
    async fn fetch_blob_files_with_snapshot(
        &self,
        column: &str,
        row_ids: &[u64],
        read_snapshot: ReadSnapshot,
    ) -> Result<Vec<Option<BlobFile>>> {
        let requesters = row_ids
            .iter()
            .map(|row_id| self.blob_range_requester(column, *row_id, &read_snapshot))
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
                     in the table; pass row ids collected from this table",
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
            // Row ids name base-table rows, and the MemWAL scanner has no stable `_rowid`.
            use_lsm: Some(false),
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
        let (request_id, response) = self
            .send_with_freshness(request, true, read_snapshot.freshness)
            .await?;
        let response_version = response
            .headers()
            .get(&VERSION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
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

    fn blob_range_requester(
        &self,
        column: &str,
        row_id: u64,
        read_snapshot: &ReadSnapshot,
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
            branch: self.branch.clone(),
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
    Known(u64),
    /// An external blob that recorded no size. Its length is the whole
    /// object's, which only the server can resolve.
    Unresolved,
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
            DescriptorSize::Known(size)
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
    let (request_id, response) = requester
        .request_range("bytes=0-0", RangeRequestMode::SizeProbe, None)
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
            self.fetch_blob_files_with_snapshot(column, row_ids, read_snapshot)
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
                    assert_eq!(body["use_lsm"], serde_json::json!(false));
                    let requested = filter_row_ids(body["filter"].as_str().unwrap());
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
                    if let Some(version) = query_version {
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
                ("bytes=0-".to_string(), Some("5".to_string())),
                ("bytes=10-".to_string(), Some("5".to_string())),
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
    async fn remote_blob_file_reuses_sequential_response_until_seek() {
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
            ["bytes=0-0", "bytes=0-", "bytes=20-"]
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
        let sequential_requests = Arc::new(AtomicUsize::new(0));
        let table = RemoteTable::new_mock(
            "my_table".to_string(),
            {
                let sequential_requests = sequential_requests.clone();
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
                    let attempt = sequential_requests.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        // End the response five bytes early to simulate a truncated
                        // sequential read.
                        return http::Response::builder()
                            .status(StatusCode::PARTIAL_CONTENT)
                            .header(
                                header::CONTENT_RANGE,
                                format!("bytes 0-{}/{}", PAYLOAD.len() - 1, PAYLOAD.len()),
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

        let error = file.read_up_to(10).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("response ended before the requested blob range"),
            "got: {error}"
        );
        assert_eq!(file.tell().await.unwrap(), 0);

        // Retry from the last committed cursor with a fresh request.
        let retried = file.read_up_to(4).await.unwrap();
        assert_eq!(retried, b"0123".as_slice());
        assert_eq!(sequential_requests.load(Ordering::SeqCst), 2);
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
            range_header: &str,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<(String, Response)> {
            assert_eq!(range_header, "bytes=0-0");
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
            Ok((format!("probe-{}", self.index), Response::from(response)))
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
            range_header: &str,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<(String, Response)> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let (start, end) = range_header
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start = start.parse::<usize>().unwrap();
            let end = end.parse::<usize>().unwrap();
            let response = http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{}", PAYLOAD.len()),
                )
                .body(PAYLOAD[start..=end].to_vec())
                .unwrap();
            Ok(("range".to_string(), Response::from(response)))
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

    #[derive(Debug)]
    struct BlockingRangeRequester {
        release: Arc<tokio::sync::Barrier>,
        started: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl BlobRangeRequester for BlockingRangeRequester {
        async fn request_range(
            &self,
            range_header: &str,
            _mode: RangeRequestMode,
            _version: Option<u64>,
        ) -> Result<(String, Response)> {
            if range_header.ends_with('-') {
                self.started.notify_one();
                self.release.wait().await;
            }
            let response = http::Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes 0-{}/{}", PAYLOAD.len() - 1, PAYLOAD.len()),
                )
                .body(PAYLOAD.to_vec())
                .unwrap();
            Ok(("hung".to_string(), Response::from(response)))
        }
    }

    #[tokio::test]
    async fn close_returns_while_a_sequential_read_is_in_flight() {
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let started = Arc::new(tokio::sync::Notify::new());
        let requester: Arc<dyn BlobRangeRequester> = Arc::new(BlockingRangeRequester {
            release: release.clone(),
            started: started.clone(),
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
}
