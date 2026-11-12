// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Real HTTP faults with a local Lance table behind the staging fixture. This
//! exercises SDK behavior; it does not model Sophon's asynchronous txn cleanup.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{LargeBinaryArray, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use lance::arrow::json::JsonSchema;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::RemoteTable;
use crate::remote::client::{ClientConfig, RestfulLanceDbClient, RetryConfig, TimeoutConfig};
use crate::remote::db::ServerVersion;
use crate::table::AddDataBuilder;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Close,
    Timeout,
    Status(u16),
}

#[derive(Default)]
struct State {
    creates: usize,
    completes: usize,
    aborts: usize,
    attempts: Vec<(String, Vec<u8>)>,
    parts: HashMap<String, Vec<u8>>,
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        header.push(socket.read_u8().await.unwrap());
        assert!(header.len() < 64 * 1024);
    }
    let header = String::from_utf8(header).unwrap();
    let path = header.split_whitespace().nth(1).unwrap().to_owned();
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(
        !header
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
    );
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await.unwrap();
    (path, body)
}

async fn respond(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} Result\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(response.as_bytes()).await;
}

#[tokio::test]
async fn multipart_http_faults_preserve_committed_blob_payloads() {
    for complete_fault in [false, true] {
        for fault in [
            Fault::Close,
            Fault::Timeout,
            Fault::Status(500),
            Fault::Status(502),
            Fault::Status(503),
            Fault::Status(504),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = crate::connect(dir.path().to_str().unwrap())
                .execute()
                .await
                .unwrap();
            let schema = Arc::new(Schema::new(vec![crate::blob::blob("payload", true)]));
            let stored = db
                .create_empty_table("stored", schema.clone())
                .execute()
                .await
                .unwrap();
            let state = Arc::new(Mutex::new(State::default()));
            let server_state = state.clone();
            let server_table = stored.clone();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut tasks = tokio::task::JoinSet::new();
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let state = server_state.clone();
                    let table = server_table.clone();
                    tasks.spawn(async move {
                        let (path, body) = read_request(&mut socket).await;
                        let mut apply_fault = false;
                        let response;
                        if path.contains("multipart_write/create") {
                            state.lock().unwrap().creates += 1;
                            response = r#"{"upload_id":"00000000-0000-0000-0000-000000000001"}"#
                                .to_string();
                        } else if path.contains("/insert/") {
                            let url = url::Url::parse(&format!("http://localhost{path}")).unwrap();
                            let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
                            assert_eq!(params["upload_id"], "00000000-0000-0000-0000-000000000001");
                            let part = params["upload_part_id"].clone();
                            let mut state = state.lock().unwrap();
                            apply_fault = !complete_fault && state.attempts.is_empty();
                            state.attempts.push((part.clone(), body.clone()));
                            // Match the verified server contract: replace the txn
                            // associated with this part ID before acknowledging it.
                            state.parts.insert(part, body);
                            response = "{}".into();
                        } else if path.contains("multipart_write/complete") {
                            let parts = {
                                let mut state = state.lock().unwrap();
                                state.completes += 1;
                                state.parts.values().cloned().collect::<Vec<_>>()
                            };
                            for body in parts {
                                let reader =
                                    StreamReader::try_new(std::io::Cursor::new(body), None)
                                        .unwrap();
                                let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
                                table.add(batches).execute().await.unwrap();
                            }
                            apply_fault = complete_fault;
                            response = r#"{"version":2}"#.into();
                        } else if path.contains("multipart_write/abort") {
                            state.lock().unwrap().aborts += 1;
                            panic!("abort must never follow a possibly committed complete");
                        } else {
                            panic!("unexpected request {path}");
                        }
                        if apply_fault {
                            match fault {
                                Fault::Close => return,
                                Fault::Timeout => {
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                    return;
                                }
                                Fault::Status(status) => {
                                    respond(
                                        &mut socket,
                                        status,
                                        "gateway fault after staging/commit",
                                    )
                                    .await;
                                    return;
                                }
                            }
                        }
                        respond(&mut socket, 200, &response).await;
                    });
                }
            });
            let client = RestfulLanceDbClient::try_new(
                &crate::remote::client::parse_db_url("db://test").unwrap(),
                "us-east-1",
                Some(format!("http://{address}")),
                Default::default(),
                ClientConfig {
                    timeout_config: TimeoutConfig {
                        read_timeout: Some(Duration::from_secs(1)),
                        ..Default::default()
                    },
                    retry_config: RetryConfig {
                        retries: Some(3),
                        read_retries: Some(3),
                        connect_retries: Some(3),
                        backoff_factor: Some(0.0),
                        backoff_jitter: Some(0.0),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                None,
            )
            .unwrap();
            let remote = Arc::new(RemoteTable::new_with_sql_client(
                client,
                "test".into(),
                "test".into(),
                vec![],
                "test".into(),
                ServerVersion(semver::Version::new(0, 4, 0)),
                None,
            ));
            remote.seed_schema(&serde_json::json!({"version": 1, "schema": JsonSchema::try_from(schema.as_ref()).unwrap()}).to_string());
            let payload_a = vec![17u8; 32 * 1024];
            let payload_b = vec![29u8; 32 * 1024];
            let input = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "payload",
                    DataType::LargeBinary,
                    true,
                )])),
                vec![Arc::new(LargeBinaryArray::from(vec![
                    Some(payload_a.as_slice()),
                    Some(payload_b.as_slice()),
                ]))],
            )
            .unwrap();
            let progress = Arc::new(Mutex::new(Vec::new()));
            let seen = progress.clone();
            let result = AddDataBuilder::new(remote, Box::new(input), None)
                .write_parallelism(2)
                .progress(move |p| {
                    seen.lock()
                        .unwrap()
                        .push((p.output_rows(), p.output_bytes(), p.done()))
                })
                .execute()
                .await;
            if complete_fault {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("commit outcome unknown"),
                    "{fault:?}"
                );
            } else {
                result.unwrap();
            }
            assert_eq!(
                stored.count_rows(None).await.unwrap(),
                2,
                "{fault:?}, complete={complete_fault}"
            );
            let payloads = stored.fetch_blobs("payload", &[0, 1]).await.unwrap();
            assert_eq!(payloads.value(0), payload_a);
            assert_eq!(payloads.value(1), payload_b);
            let state = state.lock().unwrap();
            assert_eq!((state.creates, state.completes, state.aborts), (1, 1, 0));
            assert_eq!(state.parts.len(), 1);
            assert_eq!(state.attempts.len(), if complete_fault { 1 } else { 2 });
            if !complete_fault {
                assert_eq!(state.attempts[0], state.attempts[1]);
            }
            let progress = progress.lock().unwrap();
            let last = progress.last().unwrap();
            assert_eq!(last.0, 2);
            assert!(last.1 > 0 && last.2);
            server.abort();
        }
    }
}

#[tokio::test]
async fn multipart_retries_sixth_part_after_reused_connection_closes() {
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::ExecutionPlan;
    use futures::TryStreamExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut attempts = Vec::new();
        let (mut socket, _) = listener.accept().await.unwrap();
        for n in 0..6 {
            let (path, body) = read_request(&mut socket).await;
            attempts.push((path, body));
            if n == 5 {
                break;
            }
            // Keep one connection pooled for the first five acknowledged parts.
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                .await
                .unwrap();
        }
        // The sixth part was staged, but the peer closes before response headers.
        drop(socket);
        let (mut socket, _) = listener.accept().await.unwrap();
        attempts.push(read_request(&mut socket).await);
        respond(&mut socket, 200, "{}").await;
        attempts
    });
    let client = RestfulLanceDbClient::try_new(
        &crate::remote::client::parse_db_url("db://test").unwrap(),
        "test-region",
        Some(format!("http://{address}")),
        Default::default(),
        ClientConfig {
            retry_config: RetryConfig {
                retries: Some(3),
                read_retries: Some(3),
                connect_retries: Some(3),
                backoff_factor: Some(0.0),
                backoff_jitter: Some(0.0),
                ..Default::default()
            },
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let input = arrow_array::record_batch!(("id", Int32, [1, 2, 3, 4, 5, 6])).unwrap();
    let plan = Arc::new(
        crate::table::datafusion::scannable_exec::ScannableExec::new(Box::new(input), None),
    );
    let exec = super::insert::RemoteWriteExec::new_multipart(
        "test".into(),
        "test".into(),
        client,
        plan,
        false,
        "upload-1".into(),
        None,
        None,
        Some(1),
        None,
    );
    exec.execute(0, Arc::new(TaskContext::default()))
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let attempts = server.await.unwrap();
    assert_eq!(attempts.len(), 7);
    assert_eq!(
        attempts[5], attempts[6],
        "retry must reuse both part ID and bytes"
    );
    let unique: std::collections::HashSet<_> = attempts.iter().map(|(path, _)| path).collect();
    assert_eq!(unique.len(), 6, "the staged sixth txn must be replaced");
}
