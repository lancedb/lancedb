// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

#![cfg(feature = "remote")]

use arrow_array::{Int32Array, RecordBatch};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, mpsc};
use std::time::Duration;

// A real HTTP body is used so the test catches accidental buffering in the
// client transport as well as in the Arrow decoder.
fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut data = Vec::new();
    let mut buf = [0; 4096];
    loop {
        let n = stream.read(&mut buf).unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
        if let Some(end) = data.windows(4).position(|x| x == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&data[..end]);
            let len = headers
                .lines()
                .find_map(|line| {
                    line.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if data.len() >= end + 4 + len {
                return String::from_utf8_lossy(&data).into_owned();
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Framing {
    Stream,
    File,
    MissingContentType,
}

async fn run_query(framing: Framing, timeout: bool) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    let head = writer.get_ref().clone();
    writer.finish().unwrap();
    let all = writer.into_inner().unwrap();
    let tail = all[head.len()..].to_vec();
    let mut writer = FileWriter::try_new(Vec::new(), &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    let file = writer.into_inner().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release, wait) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let request = read_request(&mut socket);
        assert!(
            request.starts_with("POST /v1/table/my_table/describe/"),
            "{request}"
        );
        let body = r#"{"version":1,"schema":{"fields":[{"name":"id","type":{"type":"int32"},"nullable":false}]}}"#;
        write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        drop(socket);
        let (mut socket, _) = listener.accept().unwrap();
        let request = read_request(&mut socket);
        assert!(
            request.starts_with("POST /v1/table/my_table/query/"),
            "{request}"
        );
        assert!(request.to_lowercase().contains("accept: application/vnd.apache.arrow.stream, application/vnd.apache.arrow.file;q=0.9"), "{request}");
        match framing {
            Framing::Stream => {
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apache.arrow.stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n", head.len()).unwrap();
                socket.write_all(&head).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(5));
                if !timeout {
                    write!(socket, "{:x}\r\n", tail.len()).unwrap();
                    socket.write_all(&tail).unwrap();
                    socket.write_all(b"\r\n0\r\n\r\n").unwrap();
                }
            }
            Framing::File | Framing::MissingContentType => {
                let content_type = if matches!(framing, Framing::File) {
                    "Content-Type: application/vnd.apache.arrow.file\r\n"
                } else {
                    ""
                };
                write!(socket, "HTTP/1.1 200 OK\r\n{content_type}Content-Length: {}\r\nConnection: close\r\n\r\n", file.len()).unwrap();
                socket.write_all(&file).unwrap();
            }
        }
    });
    let db = lancedb::connect("db://streaming-test")
        .api_key("local-test-not-a-secret")
        .region("us-east-1")
        .host_override(&format!("http://{address}"))
        .execute()
        .await
        .unwrap();
    let table = db.open_table("my_table").execute().await.unwrap();
    let mut options = QueryExecutionOptions::default();
    if timeout {
        options.timeout = Some(Duration::from_millis(300));
    }
    let mut batches = tokio::time::timeout(
        Duration::from_secs(2),
        table.query().limit(2).execute_with_options(options),
    )
    .await
    .expect("query setup must not buffer the HTTP body")
    .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), batches.try_next())
        .await
        .expect("first batch must not await EOF")
        .unwrap()
        .unwrap();
    assert_eq!(first, batch);
    if timeout {
        let error = tokio::time::timeout(Duration::from_secs(2), batches.try_next())
            .await
            .expect("query body must not hang past its deadline")
            .unwrap_err();
        assert!(error.to_string().contains("Query timeout"), "{error}");
        release.send(()).unwrap();
    } else {
        let _ = release.send(());
        assert!(batches.try_next().await.unwrap().is_none());
    }
    server.join().unwrap();
}

#[tokio::test]
async fn public_query_yields_before_http_body_finishes() {
    run_query(Framing::Stream, false).await;
}

#[tokio::test]
async fn public_query_retains_deadline_during_stream_consumption() {
    run_query(Framing::Stream, true).await;
}

#[tokio::test]
async fn public_query_accepts_legacy_file_responses() {
    run_query(Framing::File, false).await;
}

#[tokio::test]
async fn public_query_accepts_file_responses_without_content_type() {
    run_query(Framing::MissingContentType, false).await;
}
