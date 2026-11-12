// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

#![cfg(feature = "remote")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use lancedb::Table;
use lancedb::blob::BlobRangeRequest;
use lancedb::remote::{ClientConfig, RetryConfig, TimeoutConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const MIB: u64 = 1024 * 1024;
const ROUTE: &str = "/v1/table/test/fetch_blob_ranges/";

struct Reply {
    status: u16,
    version: Option<u64>,
    body: Vec<u8>,
    declared_extra: usize,
}

fn ipc(column: &str, values: ArrayRef) -> Vec<u8> {
    let batch = RecordBatch::try_from_iter([(column, values)]).unwrap();
    let mut body = Vec::new();
    let mut writer = StreamWriter::try_new(&mut body, &batch.schema()).unwrap();
    // Multiple batches exercise response assembly as well as HTTP partitions.
    let mid = batch.num_rows() / 2;
    writer.write(&batch.slice(0, mid)).unwrap();
    writer
        .write(&batch.slice(mid, batch.num_rows() - mid))
        .unwrap();
    writer.finish().unwrap();
    drop(writer);
    body
}

fn legacy_reply(path: &str, body: &Value, takes: usize) -> Reply {
    use arrow_array::{StructArray, UInt8Array, UInt32Array, UInt64Array};
    let version = body["version"]
        .as_u64()
        .unwrap_or(if takes == 1 { 7 } else { 8 });
    if path.ends_with("/query/") {
        assert_eq!(body["branch"], "experiment");
        assert_eq!(body["columns"], json!(["image"]));
        let filter = body["filter"].as_str().unwrap();
        let ids = filter
            .split_once('(')
            .unwrap()
            .1
            .trim_end_matches(')')
            .split(',')
            .map(|id| id.trim().parse::<u64>().unwrap())
            .filter(|id| [10, 20, 30, 40].contains(id))
            .rev()
            .collect::<Vec<_>>();
        let fields = lance_core::datatypes::BLOB_V2_DESC_FIELDS.clone();
        let descriptors = StructArray::new(
            fields,
            vec![
                Arc::new(UInt8Array::from(vec![0; ids.len()])),
                Arc::new(UInt64Array::from(vec![0; ids.len()])),
                Arc::new(UInt64Array::from(
                    ids.iter()
                        .map(|id| if *id == 40 { 0 } else { 5 })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt32Array::from(vec![0; ids.len()])),
                Arc::new(StringArray::from(vec![""; ids.len()])),
            ],
            Some(ids.iter().map(|id| *id != 20).collect()),
        );
        let batch = RecordBatch::try_from_iter([
            ("image", Arc::new(descriptors) as ArrayRef),
            ("_rowid", Arc::new(UInt64Array::from(ids)) as ArrayRef),
        ])
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        Reply {
            status: 200,
            version: Some(version),
            body: bytes,
            declared_extra: 0,
        }
    } else {
        assert!(path.contains("branch=experiment"), "{path}");
        let version = path
            .split("version=")
            .nth(1)
            .unwrap()
            .split('&')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let range = body["range"]
            .as_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap();
        let (start, end) = range.split_once('-').unwrap();
        let start = start.parse::<usize>().unwrap();
        let end = end.parse::<usize>().unwrap();
        let payload = if version == 8 { b"ALPHA" } else { b"alpha" };
        Reply {
            status: 206,
            version: Some(version),
            body: payload[start..=end].to_vec(),
            declared_extra: 0,
        }
    }
}

fn reply(values: Vec<Option<Vec<u8>>>, version: u64) -> Reply {
    Reply {
        status: 200,
        version: Some(version),
        body: ipc(
            "value",
            Arc::new(LargeBinaryArray::from_iter(
                values.iter().map(|v| v.as_deref()),
            )),
        ),
        declared_extra: 0,
    }
}

fn byte(row: u64, offset: u64, version: u64) -> u8 {
    ((row * 17 + offset % 251 + version) % 251) as u8
}

fn serve_ranges(body: &Value, version: u64) -> Reply {
    assert_eq!(body["column"], "image");
    let ranges = body["ranges"].as_array().unwrap();
    assert!(ranges.len() <= 1024);
    assert!(
        ranges
            .iter()
            .map(|r| r["length"].as_u64().unwrap())
            .sum::<u64>()
            <= 64 * MIB
    );
    let values = ranges
        .iter()
        .map(|r| {
            let row = r["row_id"].as_u64().unwrap();
            let offset = r["offset"].as_u64().unwrap();
            let length = r["length"].as_u64().unwrap();
            (row != 20).then(|| {
                (offset..offset + length)
                    .map(|i| byte(row, i, version))
                    .collect()
            })
        })
        .collect();
    reply(values, version)
}

struct Server {
    url: String,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn new(handler: impl Fn(&Value, usize) -> Reply + Send + Sync + 'static) -> Self {
        Self::with_capability(handler, Some("1")).await
    }

    async fn with_capability(
        handler: impl Fn(&Value, usize) -> Reply + Send + Sync + 'static,
        capability: Option<&'static str>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let task = tokio::spawn(async move {
            let mut fetches = 0;
            let mut takes = 0;
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let captured = captured.clone();
                let result = tokio::time::timeout(Duration::from_secs(20), async {
                    let mut socket = BufReader::new(socket);
                    let mut line = String::new();
                    socket.read_line(&mut line).await?;
                    let path = line.split_whitespace().nth(1).unwrap().to_string();
                    let method = line.split_whitespace().next().unwrap().to_string();
                    let mut range = String::new();
                    let mut length = 0;
                    loop {
                        line.clear();
                        socket.read_line(&mut line).await?;
                        assert!(line.len() <= 16384);
                        if line == "\r\n" { break; }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse::<usize>().unwrap();
                            }
                            if name.eq_ignore_ascii_case("range") { range = value.trim().to_string(); }
                        }
                    }
                    assert!(length <= 256 * 1024);
                    let mut bytes = vec![0; length];
                    socket.read_exact(&mut bytes).await?;
                    let body = if method == "GET" { json!({"range": range}) } else if bytes.is_empty() { json!({}) } else { serde_json::from_slice(&bytes).unwrap() };
                    captured.lock().unwrap().push((path.clone(), body.clone()));
                    let response = if path == "/v1/table/test/describe/" {
                        Reply { status: 200, version: None, body: serde_json::to_vec(&json!({"version": 8, "schema": {"fields": []}})).unwrap(), declared_extra: 0 }
                    } else if path == "/v1/table/test/branches/list/" {
                        Reply { status: 200, version: None, body: br#"{"branches":{"experiment":{"parentBranch":null,"parentVersion":1,"createAt":1,"manifestSize":1}}}"#.to_vec(), declared_extra: 0 }
                    } else if capability != Some("1") {
                        assert_ne!(path, ROUTE, "unknown capability must select old routes first");
                        takes += usize::from(path.ends_with("/query/"));
                        legacy_reply(&path, &body, takes)
                    } else {
                        assert_eq!(path, ROUTE, "unexpected legacy or fallback request");
                        fetches += 1;
                        handler(&body, fetches)
                    };
                    if response.status == 0 { return Ok::<_, std::io::Error>(()); }
                    let version = response.version.map(|v| format!("x-lancedb-version: {v}\r\n")).unwrap_or_default();
                    let capability = if path.ends_with("/describe/") { capability.map(|value| format!("x-lancedb-blob-ranges: {value}\r\n")).unwrap_or_default() } else { String::new() };
                    let content_type = if path == ROUTE || path.ends_with("/query/") { "application/vnd.apache.arrow.stream" } else { "application/json" };
                    let content_range = if method == "GET" { format!("Content-Range: {}/5\r\n", body["range"].as_str().unwrap().replace("bytes=", "bytes ")) } else { String::new() };
                    let header = format!("HTTP/1.1 {} Response\r\nContent-Type: {content_type}\r\nphalanx-version: 1.13.0-beta.0\r\n{capability}{version}{content_range}Content-Length: {}\r\nConnection: close\r\n\r\n", response.status, response.body.len() + response.declared_extra);
                    socket.write_all(header.as_bytes()).await?;
                    for chunk in response.body.chunks(8192) { socket.write_all(chunk).await?; }
                    socket.shutdown().await?;
                    Ok(())
                }).await;
                // A client rejecting a malformed schema may close the body early.
                assert!(result.is_ok(), "loopback request timed out");
            }
        });
        Self { url, calls, task }
    }

    async fn table(&self, version: Option<u64>) -> Table {
        let db = lancedb::connect("db://dev")
            .api_key("fake")
            .region("us-east-1")
            .host_override(&self.url)
            .client_config(ClientConfig {
                retry_config: RetryConfig {
                    retries: Some(2),
                    connect_retries: Some(2),
                    read_retries: Some(2),
                    ..Default::default()
                },
                timeout_config: TimeoutConfig {
                    timeout: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();
        let mut open = db.open_table("test").branch("experiment");
        if let Some(version) = version {
            open = open.version(version);
        }
        open.execute().await.unwrap()
    }
}

#[tokio::test]
async fn public_ranges_work_on_old_routes_only_servers() {
    for capability in [None, Some("0"), Some("2")] {
        for checkout in [None, Some(3)] {
            let server = Server::with_capability(
                |_, _| panic!("new endpoint must not be requested"),
                capability,
            )
            .await;
            let table = server.table(checkout).await;
            let cloned = table.clone();
            let ranges = [
                BlobRangeRequest::new(10, 1, 3),
                BlobRangeRequest::new(20, 0, 1),
                BlobRangeRequest::new(40, 0, 0),
                BlobRangeRequest::new(10, 5, 0),
                BlobRangeRequest::new(10, 1, 3),
            ];
            let result = cloned.fetch_blob_ranges("image", ranges).await.unwrap();
            assert_eq!(
                result,
                LargeBinaryArray::from(vec![
                    Some(b"lph".as_slice()),
                    None,
                    Some(b"".as_slice()),
                    Some(b"".as_slice()),
                    Some(b"lph".as_slice())
                ])
            );
            let result = table
                .fetch_blob_ranges("image", [BlobRangeRequest::new(10, 0, 1)])
                .await
                .unwrap();
            assert_eq!(
                result.value(0),
                if checkout.is_none() { b"A" } else { b"a" }
            );
            for ranges in [
                vec![BlobRangeRequest::new(10, 4, 2)],
                vec![BlobRangeRequest::new(99, 0, 1)],
            ] {
                assert!(matches!(
                    table.fetch_blob_ranges("image", ranges).await.unwrap_err(),
                    lancedb::Error::InvalidInput { .. }
                ));
            }
            assert!(
                !server
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(path, _)| path == ROUTE)
            );
        }
    }
}

#[tokio::test]
async fn public_ranges_keep_order_nulls_and_snapshot_without_pinning_live_handle() {
    for checkout in [None, Some(3)] {
        let server = Server::new(move |body, call| {
            assert_eq!(body["branch"], "experiment");
            let expected = checkout.or((call > 1 && call < 3).then_some(7));
            assert_eq!(body["version"].as_u64(), expected);
            serve_ranges(
                body,
                body["version"]
                    .as_u64()
                    .unwrap_or(if call == 1 { 7 } else { 8 }),
            )
        })
        .await;
        let table = server.table(checkout).await;
        let mut requests = (0..1025)
            .map(|i| BlobRangeRequest::new(if i % 9 == 0 { 20 } else { 10 + i % 7 }, i, i % 4))
            .collect::<Vec<_>>();
        requests.extend([
            BlobRangeRequest::new(10, 0, 0),
            BlobRangeRequest::new(10, 2, 3),
            BlobRangeRequest::new(10, 2, 3),
        ]);
        let result = table
            .fetch_blob_ranges("image", requests.clone())
            .await
            .unwrap();
        assert_eq!(result.len(), requests.len());
        for (i, request) in requests.iter().enumerate() {
            assert_eq!(result.is_null(i), request.row_id == 20);
            if request.row_id != 20 {
                let expected = (request.offset..request.offset + request.length)
                    .map(|j| byte(request.row_id, j, checkout.unwrap_or(7)))
                    .collect::<Vec<_>>();
                assert_eq!(result.value(i), expected);
            }
        }
        let next = table
            .fetch_blob_ranges("image", [BlobRangeRequest::new(10, 0, 1)])
            .await
            .unwrap();
        assert_eq!(next.value(0), [byte(10, 0, checkout.unwrap_or(8))]);
        assert_eq!(
            server
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _)| path == ROUTE)
                .count(),
            3
        );
    }
}

#[tokio::test]
async fn public_ranges_partition_bytes_and_reassemble_large_single_range() {
    let server = Server::new(|body, call| {
        assert_eq!(body["version"].as_u64(), (call > 1).then_some(7));
        serve_ranges(body, 7)
    })
    .await;
    let table = server.table(None).await;
    let requests = [
        BlobRangeRequest::new(10, 3, 33 * MIB),
        BlobRangeRequest::new(30, 7, 33 * MIB),
        BlobRangeRequest::new(40, 11, 70 * MIB),
        BlobRangeRequest::new(20, 0, u64::MAX),
    ];
    let result = table.fetch_blob_ranges("image", requests).await.unwrap();
    assert_eq!(result.len(), 4);
    for (i, request) in requests[..3].iter().enumerate() {
        assert_eq!(result.value_length(i) as u64, request.length);
        assert!(
            result
                .value(i)
                .iter()
                .enumerate()
                .all(|(j, value)| *value == byte(request.row_id, request.offset + j as u64, 7))
        );
    }
    assert!(result.is_null(3));
    assert_eq!(
        server
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == ROUTE)
            .count(),
        3
    );
}

#[tokio::test]
async fn public_ranges_reject_malformed_responses_and_http_failures_without_fallback() {
    for case in 0..16 {
        let server = Server::new(move |body, call| {
            let mut response = serve_ranges(body, 7);
            // Fail after one successful partition to prove no partial success.
            if call == 1 {
                return response;
            }
            match case {
                0 => response.body = ipc("value", Arc::new(StringArray::from(vec!["bad"]))),
                1 => {
                    response.body = ipc(
                        "wrong",
                        Arc::new(LargeBinaryArray::from(vec![Some(b"a".as_slice())])),
                    )
                }
                2 => response = reply(vec![], 7),
                3 => response = reply(vec![Some(vec![1]), Some(vec![2])], 7),
                4 => response.body.truncate(response.body.len() - 8),
                5 => response.body.truncate(response.body.len() / 2),
                6 => response.declared_extra = 10,
                7 => response = reply(vec![Some(vec![])], 7),
                8 => response.version = None,
                9 => response.version = Some(8),
                10..=14 => {
                    response.status = [401, 403, 404, 429, 500][case - 10];
                    response.body = b"ordinary failure".to_vec();
                }
                15 => response.status = 0,
                _ => unreachable!(),
            }
            response
        })
        .await;
        let table = server.table(None).await;
        let requests = vec![BlobRangeRequest::new(10, 0, 1); 1025];
        assert!(
            table.fetch_blob_ranges("image", requests).await.is_err(),
            "case {case}"
        );
        assert_eq!(
            server
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _)| path == ROUTE)
                .count(),
            2,
            "case {case}"
        );
    }
}

#[tokio::test]
async fn public_ranges_validate_locally_and_propagate_server_range_errors() {
    let server = Server::new(|_, _| Reply {
        status: 400,
        version: None,
        body: b"range exceeds blob size or missing row".to_vec(),
        declared_extra: 0,
    })
    .await;
    let table = server.table(None).await;
    let before = server.calls.lock().unwrap().len();
    assert_eq!(table.fetch_blob_ranges("image", []).await.unwrap().len(), 0);
    assert!(
        table
            .fetch_blob_ranges("image", [BlobRangeRequest::new(20, u64::MAX, 1)])
            .await
            .unwrap_err()
            .to_string()
            .contains("overflows u64")
    );
    assert_eq!(server.calls.lock().unwrap().len(), before);
    let error = table
        .fetch_blob_ranges("image", [BlobRangeRequest::new(10, 9, 2)])
        .await
        .unwrap_err();
    assert!(matches!(error, lancedb::Error::InvalidInput { .. }));
    assert!(error.to_string().contains("range exceeds"));
}
