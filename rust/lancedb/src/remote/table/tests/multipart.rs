// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_add_insert_fails() {
    // Verify that an HTTP error from the insert endpoint is properly
    // surfaced with the status code intact. Use 400 (non-retryable).
    let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
    let describe_body = describe_response(&batch.schema());

    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_body.clone())
            .unwrap(),
        "/v1/table/my_table/insert/" => http::Response::builder()
            .status(400)
            .body("bad request".to_string())
            .unwrap(),
        path => panic!("Unexpected request path: {}", path),
    });

    let result = table.add(batch).execute().await;
    let err = result.unwrap_err();
    match &err {
        Error::Http { status_code, .. } => {
            assert_eq!(*status_code, Some(reqwest::StatusCode::BAD_REQUEST));
        }
        other => panic!("Expected Http error, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_add_retries_on_retryable_status() {
    // Verify that rescannable data retries on retryable status codes (e.g. 502)
    // and eventually succeeds.
    let batch = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
    let describe_body = describe_response(&batch.schema());

    let attempt = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt.clone();

    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_body.clone())
            .unwrap(),
        "/v1/table/my_table/insert/" => {
            let n = attempt_clone.fetch_add(1, Ordering::SeqCst);
            if n < 2 {
                http::Response::builder()
                    .status(502)
                    .body("bad gateway".to_string())
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 3}"#.to_string())
                    .unwrap()
            }
        }
        path => panic!("Unexpected request path: {}", path),
    });

    let result = table.add(batch).execute().await.unwrap();
    assert_eq!(result.version, 3);
    assert_eq!(attempt.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_query_with_datafusion_filter() {
    use datafusion_expr::{col, lit};

    let expected_data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let expected_data_ref = expected_data.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();

        // The Datafusion expression should be serialized to SQL
        let filter = body.get("filter").expect("filter should be present");
        let filter_str = filter.as_str().expect("filter should be a string");
        // col("x") > lit(10) AND col("status") = lit("active")
        assert!(
            filter_str.contains("x") && filter_str.contains("10"),
            "Filter should contain 'x' and '10', got: {}",
            filter_str
        );
        assert!(
            filter_str.contains("status") && filter_str.contains("active"),
            "Filter should contain 'status' and 'active', got: {}",
            filter_str
        );

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    // Use only_if_expr with a Datafusion expression
    let expr = col("x").gt(lit(10)).and(col("status").eq(lit("active")));
    let data = table
        .query()
        .only_if_expr(expr)
        .execute()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;

    assert_eq!(data.len(), 1);
    assert_eq!(data[0].as_ref().unwrap(), &expected_data);
}

#[tokio::test]
async fn test_multipart_write_happy_path() {
    use std::sync::Mutex;

    let create_count = Arc::new(AtomicUsize::new(0));
    let insert_count = Arc::new(AtomicUsize::new(0));
    let complete_count = Arc::new(AtomicUsize::new(0));
    let abort_count = Arc::new(AtomicUsize::new(0));
    let upload_ids = Arc::new(Mutex::new(Vec::<String>::new()));

    let create_count_c = create_count.clone();
    let insert_count_c = insert_count.clone();
    let complete_count_c = complete_count.clone();
    let abort_count_c = abort_count.clone();
    let upload_ids_c = upload_ids.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();
            let query = request.url().query().unwrap_or("");

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path == "/v1/table/my_table/multipart_write/create" {
                create_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"upload_id": "test-upload-123"}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                insert_count_c.fetch_add(1, Ordering::SeqCst);
                let uid = url::form_urlencoded::parse(query.as_bytes())
                    .find(|(k, _)| k == "upload_id")
                    .map(|(_, v)| v.to_string());
                upload_ids_c
                    .lock()
                    .unwrap()
                    .push(uid.expect("missing upload_id on insert"));
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/complete" {
                complete_count_c.fetch_add(1, Ordering::SeqCst);
                let uid = url::form_urlencoded::parse(query.as_bytes())
                    .find(|(k, _)| k == "upload_id")
                    .map(|(_, v)| v.to_string());
                upload_ids_c
                    .lock()
                    .unwrap()
                    .push(uid.expect("missing upload_id on complete"));
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 5}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/abort" {
                abort_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(String::new())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table
        .add(vec![batch])
        .write_parallelism(2)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 5);
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
    assert!(
        insert_count.load(Ordering::SeqCst) > 1,
        "Expected multiple insert calls, got {}",
        insert_count.load(Ordering::SeqCst)
    );
    assert_eq!(complete_count.load(Ordering::SeqCst), 1);
    assert_eq!(abort_count.load(Ordering::SeqCst), 0);

    let ids = upload_ids.lock().unwrap();
    assert!(
        ids.iter().all(|id| id == "test-upload-123"),
        "All requests should use the same upload_id, got: {:?}",
        *ids
    );
}

#[tokio::test]
async fn test_multipart_write_progress() {
    let callback_count = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let last_total_tasks = Arc::new(AtomicUsize::new(0));
    let seen_done = Arc::new(std::sync::Mutex::new(false));

    let cb_count = callback_count.clone();
    let cb_active = max_active.clone();
    let cb_total = last_total_tasks.clone();
    let cb_done = seen_done.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }
            if path == "/v1/table/my_table/multipart_write/create" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"upload_id": "prog-upload"}"#.to_string())
                    .unwrap();
            }
            if path == "/v1/table/my_table/insert/" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }
            if path == "/v1/table/my_table/multipart_write/complete" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 3}"#.to_string())
                    .unwrap();
            }
            panic!("Unexpected request path: {}", path);
        },
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    table
        .add(vec![batch])
        .write_parallelism(2)
        .progress(move |p| {
            cb_count.fetch_add(1, Ordering::SeqCst);
            cb_active.fetch_max(p.active_tasks(), Ordering::SeqCst);
            cb_total.store(p.total_tasks(), Ordering::SeqCst);
            if p.done() {
                *cb_done.lock().unwrap() = true;
            }
        })
        .execute()
        .await
        .unwrap();

    assert!(
        callback_count.load(Ordering::SeqCst) >= 1,
        "expected at least one progress callback"
    );
    assert!(*seen_done.lock().unwrap(), "must see done=true");
    assert_eq!(last_total_tasks.load(Ordering::SeqCst), 2);
    assert!(
        max_active.load(Ordering::SeqCst) >= 1,
        "expected at least one active task"
    );
}

#[tokio::test]
async fn test_multipart_write_fallback_old_server() {
    let insert_count = Arc::new(AtomicUsize::new(0));
    let create_count = Arc::new(AtomicUsize::new(0));

    let insert_count_c = insert_count.clone();
    let create_count_c = create_count.clone();

    // Server version 0.3.0 does not support multipart writes
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 3, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path.contains("multipart_write") {
                create_count_c.fetch_add(1, Ordering::SeqCst);
                panic!("Should not call multipart write endpoints on old server");
            }

            if path == "/v1/table/my_table/insert/" {
                let query = request.url().query().unwrap_or("");
                assert!(
                    !query.contains("upload_id"),
                    "Should not have upload_id for old server"
                );
                insert_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table
        .add(vec![batch])
        .write_parallelism(2)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 2);
    assert_eq!(create_count.load(Ordering::SeqCst), 0);
    assert_eq!(insert_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multipart_write_small_data_single_partition() {
    let insert_count = Arc::new(AtomicUsize::new(0));
    let create_count = Arc::new(AtomicUsize::new(0));

    let insert_count_c = insert_count.clone();
    let create_count_c = create_count.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path.contains("multipart_write") {
                create_count_c.fetch_add(1, Ordering::SeqCst);
                panic!("Should not call multipart write endpoints for small data");
            }

            if path == "/v1/table/my_table/insert/" {
                let query = request.url().query().unwrap_or("");
                assert!(
                    !query.contains("upload_id"),
                    "Should not have upload_id for small data"
                );
                insert_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    // Small data: only 3 rows
    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table.add(vec![batch]).execute().await.unwrap();

    assert_eq!(result.version, 2);
    assert_eq!(create_count.load(Ordering::SeqCst), 0);
    assert_eq!(insert_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multipart_write_empty_overwrite_uses_single_partition() {
    // A multipart write creates its upload session before any partition
    // executes. If the input has no batches at all, every partition would
    // stage nothing (see `send_multipart_chunked`), so completing the write
    // would have nothing to commit and `mode=overwrite` would be silently
    // dropped. An explicit `write_parallelism` must not force the multipart
    // path for empty input; it should fall back to the single-request path,
    // which always sends one schema-only request and carries `mode=overwrite`.
    let insert_count = Arc::new(AtomicUsize::new(0));
    let multipart_count = Arc::new(AtomicUsize::new(0));

    let insert_count_c = insert_count.clone();
    let multipart_count_c = multipart_count.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path.contains("multipart_write") {
                multipart_count_c.fetch_add(1, Ordering::SeqCst);
                panic!("Should not use multipart write endpoints for empty input");
            }

            if path == "/v1/table/my_table/insert/" {
                let query = request.url().query().unwrap_or("");
                assert!(
                    !query.contains("upload_id"),
                    "Should not have upload_id for empty input"
                );
                assert!(
                    query.contains("mode=overwrite"),
                    "Should carry mode=overwrite, got query: {}",
                    query
                );
                insert_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let empty_batches: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> = Vec::new();
    let data: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(empty_batches, schema));
    let result = table
        .add(data)
        .mode(AddDataMode::Overwrite)
        .write_parallelism(4)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 2);
    assert_eq!(multipart_count.load(Ordering::SeqCst), 0);
    assert_eq!(insert_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multipart_write_abort_on_insert_failure() {
    let create_count = Arc::new(AtomicUsize::new(0));
    let insert_count = Arc::new(AtomicUsize::new(0));
    let complete_count = Arc::new(AtomicUsize::new(0));
    let abort_count = Arc::new(AtomicUsize::new(0));

    let create_count_c = create_count.clone();
    let insert_count_c = insert_count.clone();
    let complete_count_c = complete_count.clone();
    let abort_count_c = abort_count.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path == "/v1/table/my_table/multipart_write/create" {
                create_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"upload_id": "test-upload-456"}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                let count = insert_count_c.fetch_add(1, Ordering::SeqCst);
                // Fail on the first insert with non-retryable status
                if count == 0 {
                    return http::Response::builder()
                        .status(400)
                        .body("Bad Request".to_string())
                        .unwrap();
                }
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/complete" {
                complete_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 5}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/abort" {
                abort_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(String::new())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table.add(vec![batch]).write_parallelism(2).execute().await;

    assert!(result.is_err());
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count.load(Ordering::SeqCst), 0);
    assert_eq!(abort_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multipart_write_abort_on_complete_failure() {
    let abort_count = Arc::new(AtomicUsize::new(0));
    let abort_count_c = abort_count.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path == "/v1/table/my_table/multipart_write/create" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"upload_id": "test-upload-789"}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/complete" {
                return http::Response::builder()
                    .status(400)
                    .body("Bad Request".to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/abort" {
                abort_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(String::new())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table.add(vec![batch]).write_parallelism(2).execute().await;

    assert!(result.is_err());
    assert_eq!(abort_count.load(Ordering::SeqCst), 1);
}

fn retry_config_no_backoff() -> ClientConfig {
    ClientConfig {
        retry_config: RetryConfig {
            retries: Some(3),
            connect_retries: Some(3),
            read_retries: Some(3),
            backoff_factor: Some(0.0),
            backoff_jitter: Some(0.0),
            statuses: Some(vec![502, 503]),
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_multipart_write_retry_on_partition_failure() {
    // All inserts for the first upload session return 503 (retryable).
    // After exhausting internal retries, the outer loop retries with a
    // new session and succeeds.
    let create_count = Arc::new(AtomicUsize::new(0));
    let complete_count = Arc::new(AtomicUsize::new(0));
    let abort_count = Arc::new(AtomicUsize::new(0));

    let create_count_c = create_count.clone();
    let complete_count_c = complete_count.clone();
    let abort_count_c = abort_count.clone();

    let table = Table::new_with_handler_version_and_config(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();
            let query = request.url().query().unwrap_or("");

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path == "/v1/table/my_table/multipart_write/create" {
                let n = create_count_c.fetch_add(1, Ordering::SeqCst);
                let body = format!(r#"{{"upload_id": "upload-{}"}}"#, n + 1);
                return http::Response::builder().status(200).body(body).unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                // Fail all inserts for the first session
                if query.contains("upload_id=upload-1") {
                    return http::Response::builder()
                        .status(503)
                        .body("Service Unavailable".to_string())
                        .unwrap();
                }
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/complete" {
                complete_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 7}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/abort" {
                abort_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(String::new())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
        retry_config_no_backoff(),
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table
        .add(vec![batch])
        .write_parallelism(2)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 7);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert_eq!(abort_count.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_multipart_write_retry_on_complete_failure() {
    // Complete returns 503 for the first session, succeeds for the second.
    let create_count = Arc::new(AtomicUsize::new(0));
    let abort_count = Arc::new(AtomicUsize::new(0));

    let create_count_c = create_count.clone();
    let abort_count_c = abort_count.clone();

    let table = Table::new_with_handler_version_and_config(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| {
            let path = request.url().path();
            let query = request.url().query().unwrap_or("");

            if path == "/v1/table/my_table/describe/" {
                return simple_describe_response();
            }

            if path == "/v1/table/my_table/multipart_write/create" {
                let n = create_count_c.fetch_add(1, Ordering::SeqCst);
                let body = format!(r#"{{"upload_id": "upload-{}"}}"#, n + 1);
                return http::Response::builder().status(200).body(body).unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/complete" {
                // Fail complete for first session
                if query.contains("upload_id=upload-1") {
                    return http::Response::builder()
                        .status(503)
                        .body("Service Unavailable".to_string())
                        .unwrap();
                }
                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 9}"#.to_string())
                    .unwrap();
            }

            if path == "/v1/table/my_table/multipart_write/abort" {
                abort_count_c.fetch_add(1, Ordering::SeqCst);
                return http::Response::builder()
                    .status(200)
                    .body(String::new())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        },
        retry_config_no_backoff(),
    );

    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    let result = table
        .add(vec![batch])
        .write_parallelism(2)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 9);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
    assert_eq!(abort_count.load(Ordering::SeqCst), 1);
}
