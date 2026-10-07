// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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
    async fn probe_files(&self, column: &str, row_ids: &[u64]) -> Result<Vec<Option<BlobFile>>> {
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
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
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
