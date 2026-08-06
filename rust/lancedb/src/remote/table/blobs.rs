// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Cloud blob column listing, whole-byte fetch, and seekable HTTP byte-range handles.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{Array, LargeBinaryArray};
use arrow_schema::DataType;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use reqwest::{Response, StatusCode, header};
use tokio::sync::Mutex;

use crate::Error;
use crate::blob::BlobFile;
use crate::error::Result;
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};
use crate::table::BaseTable;

use super::{FreshnessHeaders, RemoteTable};

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
    ) -> Result<(String, Response)>;
}

#[derive(Debug)]
struct TableBlobRangeRequester<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    path: String,
    version: Option<u64>,
    branch: Option<String>,
    freshness: FreshnessHeaders,
}

#[async_trait::async_trait]
impl<S: HttpSend> BlobRangeRequester for TableBlobRangeRequester<S> {
    async fn request_range(
        &self,
        range_header: &str,
        mode: RangeRequestMode,
    ) -> Result<(String, Response)> {
        let mut request = self
            .freshness
            .apply(self.client.get(&self.path))
            .header(header::RANGE, range_header);
        if let Some(version) = self.version {
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
#[derive(Debug)]
pub struct RemoteBlobFile {
    requester: Arc<dyn BlobRangeRequester>,
    state: Mutex<RemoteBlobState>,
    closed: AtomicBool,
    size: u64,
}

impl RemoteBlobFile {
    fn new(requester: Arc<dyn BlobRangeRequester>, size: u64) -> Self {
        Self {
            requester,
            state: Mutex::new(RemoteBlobState::default()),
            closed: AtomicBool::new(false),
            size,
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
            .request_range(&range_header, RangeRequestMode::DataRead)
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
                    .request_range(&range_header, RangeRequestMode::DataRead)
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
        // Empty requests do not require blob-route support.
        if row_ids.is_empty() {
            return Ok(LargeBinaryArray::from(Vec::<Option<&[u8]>>::new()));
        }
        if !self.server_version.support_blobs() {
            return Err(Error::NotSupported {
                message: "fetch_blobs is not supported on this LanceDB Cloud server".into(),
            });
        }
        let version = self.current_version().await;
        let mut body = serde_json::json!({
            "version": version,
            "column": column,
            "row_ids": row_ids,
        });
        self.apply_branch_body(&mut body);

        let request = self
            .post_read(&format!("/v1/table/{}/fetch_blobs/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let mut stream = self.read_arrow_response(&request_id, response).await?;

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
    /// Each non-null row is probed once to determine its size.
    pub(super) async fn fetch_blob_files_impl(
        &self,
        column: &str,
        row_ids: &[u64],
    ) -> Result<Vec<Option<BlobFile>>> {
        // Empty requests do not require blob-route support.
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        if !self.server_version.support_blobs() {
            return Err(Error::NotSupported {
                message: "fetch_blob_files requires LanceDB Cloud server 0.5.0 or newer".into(),
            });
        }

        let version = self.current_version().await;
        let freshness = self.snapshot_freshness_headers();
        let encoded_column = urlencoding::encode(column);
        let requesters = row_ids
            .iter()
            .map(|row_id| {
                let path = format!(
                    "/v1/table/{}/blob/{encoded_column}/{row_id}/bytes",
                    self.identifier
                );
                let requester: Arc<dyn BlobRangeRequester> = Arc::new(TableBlobRangeRequester {
                    client: self.client.clone(),
                    path,
                    version,
                    branch: self.branch.clone(),
                    freshness,
                });
                requester
            })
            .collect();
        probe_blob_files(requesters).await
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

/// Probe one blob's size.
///
/// `204` represents null. `416` with `bytes */0` represents an empty blob.
async fn probe_blob_file(requester: Arc<dyn BlobRangeRequester>) -> Result<Option<BlobFile>> {
    let (request_id, response) = requester
        .request_range("bytes=0-0", RangeRequestMode::SizeProbe)
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
            Ok(Some(RemoteBlobFile::new(requester, 0).into()))
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
            Ok(Some(RemoteBlobFile::new(requester, size).into()))
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use reqwest::Request;
    use semver::Version;

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

        let mut files = table
            .fetch_blob_files_impl("image", &[10, 20])
            .await
            .unwrap();

        assert_eq!(files.len(), 2);
        assert!(files[1].is_none());
        let file = files[0].take().unwrap();
        assert_eq!(file.size(), PAYLOAD.len() as u64);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["bytes=0-0", "bytes=0-0"]
        );
    }

    #[tokio::test]
    async fn remote_blob_file_reads_the_requested_range() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .fetch_blob_files_impl("image", &[10])
            .await
            .unwrap()
            .pop()
            .flatten()
            .unwrap();

        assert_eq!(file.read_range(5..12).await.unwrap(), &PAYLOAD[5..12]);
        assert!(requests.lock().unwrap().contains(&"bytes=5-11".to_string()));
    }

    #[tokio::test]
    async fn remote_blob_file_reuses_sequential_response_until_seek() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());
        let file = table
            .fetch_blob_files_impl("image", &[10])
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
    async fn remote_blob_files_return_a_handle_for_an_empty_blob() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let table = mock_remote_blob_table(requests.clone());

        let mut files = table
            .fetch_blob_files_impl("image", &[10, 20, 30])
            .await
            .unwrap();

        assert_eq!(files.len(), 3);
        assert!(files[1].is_none());
        let empty = files[2].take().unwrap();
        assert_eq!(empty.size(), 0);
        assert!(empty.read_range(0..0).await.unwrap().is_empty());
        assert!(empty.read().await.unwrap().is_empty());
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

        let error = table
            .fetch_blob_files_impl("image", &[10])
            .await
            .unwrap_err();
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

        let error = table
            .fetch_blob_files_impl("image", &[10])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("blob size probe returned an invalid Content-Range header"),
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
            .fetch_blob_files_impl("image", &[10])
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
            .fetch_blob_files_impl("image", &[10])
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
            .fetch_blob_files_impl("image", &[10])
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
            .fetch_blob_files_impl("image", &[10])
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

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
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

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
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

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
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

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
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
        let file = RemoteBlobFile::new(requester, PAYLOAD.len() as u64);

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
            .fetch_blob_files_impl("image", &[10])
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

        let mut files = table.fetch_blob_files_impl("image", &[10]).await.unwrap();
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
        let file = Arc::new(RemoteBlobFile::new(requester, PAYLOAD.len() as u64));

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
