// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_table_with_namespace_identifier() {
    // Test that a table created with namespace uses the correct identifier in API calls
    let table = Table::new_with_handler("ns1$ns2$table1", |request| {
        assert_eq!(request.method(), "POST");
        // All API calls should use the full identifier in the path
        assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/describe/");

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 1, "schema": { "fields": [] }}"#)
            .unwrap()
    });

    // The name() method should return just the base name, not the full identifier
    assert_eq!(table.name(), "ns1$ns2$table1");

    // API operations should work correctly
    let version = table.version().await.unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn test_query_with_namespace() {
    let table = Table::new_with_handler("analytics$events", |request| {
        match request.url().path() {
            "/v1/table/analytics$events/query/" => {
                assert_eq!(request.method(), "POST");

                // Return empty arrow stream
                let data = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap();
                let body = write_ipc_file(&data);

                http::Response::builder()
                    .status(200)
                    .header("Content-Type", ARROW_FILE_CONTENT_TYPE)
                    .body(body)
                    .unwrap()
            }
            _ => {
                panic!("Unexpected path: {}", request.url().path());
            }
        }
    });

    let results = table.query().execute().await.unwrap();
    let batches = results.try_collect::<Vec<_>>().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}

#[tokio::test]
async fn test_add_data_with_namespace() {
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let describe_body = describe_response(&data.schema());
    let (sender, receiver) = std::sync::mpsc::channel();
    let table = Table::new_with_handler("prod$metrics", move |mut request| {
        match request.url().path() {
            "/v1/table/prod$metrics/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/prod$metrics/insert/" => {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap()
            }
            path => panic!("Unexpected request path: {}", path),
        }
    });

    let result = table.add(data.clone()).execute().await.unwrap();

    assert_eq!(result.version, 2);

    let body = receiver.recv().unwrap();
    let body = collect_body(body).await;
    let expected_body = write_ipc_stream(&data);
    assert_eq!(&body, &expected_body);
}

#[tokio::test]
async fn test_create_index_with_namespace() {
    let table = Table::new_with_handler("dev$users", |request| {
        match request.url().path() {
            "/v1/table/dev$users/create_index/" => {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                // Verify the request body contains the column name
                if let Some(body) = request.body().unwrap().as_bytes() {
                    let body = std::str::from_utf8(body).unwrap();
                    let value: serde_json::Value = serde_json::from_str(body).unwrap();
                    assert_eq!(value["column"], "embedding");
                    assert_eq!(value["index_type"], "IVF_PQ");
                }

                http::Response::builder()
                    .status(200)
                    .body("".to_string())
                    .unwrap()
            }
            "/v1/table/dev$users/describe/" => {
                let schema = Schema::new(vec![Field::new(
                    "embedding",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        8,
                    ),
                    false,
                )]);
                http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap()
            }
            _ => {
                panic!("Unexpected path: {}", request.url().path());
            }
        }
    });

    table
        .create_index(&["embedding"], Index::IvfPq(IvfPqIndexBuilder::default()))
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_drop_columns_with_namespace() {
    let table = Table::new_with_handler("test$schema_ops", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/test$schema_ops/drop_columns/"
        );
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        if let Some(body) = request.body().unwrap().as_bytes() {
            let body = std::str::from_utf8(body).unwrap();
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            let columns = value["columns"].as_array().unwrap();
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0], "old_col1");
            assert_eq!(columns[1], "old_col2");
        }

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 5}"#)
            .unwrap()
    });

    let result = table.drop_columns(&["old_col1", "old_col2"]).await.unwrap();
    assert_eq!(result.version, 5);
}

#[tokio::test]
async fn test_uri() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 1, "schema": {"fields": []}, "location": "s3://bucket/path/to/table"}"#)
            .unwrap()
    });

    let uri = table.uri().await.unwrap();
    assert_eq!(uri, "s3://bucket/path/to/table");
}

#[tokio::test]
async fn test_uri_missing_location() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

        // Server returns response without location field
        http::Response::builder()
            .status(200)
            .body(r#"{"version": 1, "schema": {"fields": []}}"#)
            .unwrap()
    });

    let result = table.uri().await;
    assert!(result.is_err());
    assert!(matches!(&result, Err(Error::NotSupported { .. })));
}

#[tokio::test]
async fn test_uri_caching() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
        call_count_clone.fetch_add(1, Ordering::SeqCst);

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 1, "schema": {"fields": []}, "location": "gs://bucket/table"}"#)
            .unwrap()
    });

    // First call should fetch from server
    let uri1 = table.uri().await.unwrap();
    assert_eq!(uri1, "gs://bucket/table");
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second call should use cached value
    let uri2 = table.uri().await.unwrap();
    assert_eq!(uri2, "gs://bucket/table");
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call
}

/// Test that schema is fetched once and cached for subsequent calls
#[tokio::test]
async fn test_schema_caching() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
        call_count_clone.fetch_add(1, Ordering::SeqCst);

        http::Response::builder()
            .status(200)
            .body(
                r#"{"version": 1, "schema": {"fields": [
                        {"name": "a", "type": { "type": "int32" }, "nullable": false}
                    ]}}"#,
            )
            .unwrap()
    });

    // First call should fetch from server
    let schema1 = table.schema().await.unwrap();
    assert_eq!(schema1.fields().len(), 1);
    assert_eq!(schema1.field(0).name(), "a");
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second call should use cached value
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call

    // Third call should still use cached value
    let schema3 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1); // Still 1, no new call
}

/// Test that schema cache expires after 30 seconds TTL
#[tokio::test]
async fn test_schema_cache_invalidation_after_ttl() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
        call_count_clone.fetch_add(1, Ordering::SeqCst);

        http::Response::builder()
            .status(200)
            .body(
                r#"{"version": 1, "schema": {"fields": [
                        {"name": "a", "type": { "type": "int32" }, "nullable": false}
                    ]}}"#,
            )
            .unwrap()
    });

    // First call should fetch from server
    let _schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second call should use cached value (within TTL)
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&_schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Advance mock time past TTL (no real wait)
    clock::advance_by(Duration::from_secs(31));

    // Third call should re-fetch from server (TTL expired)
    let schema3 = table.schema().await.unwrap();
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&_schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

/// Test that schema cache is invalidated after schema-changing operations
#[rstest]
#[case("overwrite")]
#[case("add_columns")]
#[case("drop_columns")]
#[case("alter_columns")]
#[tokio::test]
async fn test_schema_cache_invalidation_after_operation(#[case] operation: &str) {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false},
                            {"name": "b", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else if path == "/v1/table/my_table/insert/"
            || path == "/v1/table/my_table/add_columns/"
            || path == "/v1/table/my_table/drop_columns/"
            || path == "/v1/table/my_table/alter_columns/"
        {
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 2}"#)
                .unwrap()
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call should fetch from server
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second schema call should use cached value
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Perform the schema-changing operation
    match operation {
        "overwrite" => {
            let data = record_batch!(("a", Int32, [1, 2, 3])).unwrap();
            let _ = table.add(data).mode(AddDataMode::Overwrite).execute().await;
        }
        "add_columns" => {
            let _ = table
                .add_columns()
                .transform(NewColumnTransform::SqlExpressions(vec![(
                    "c".into(),
                    "a + 1".into(),
                )]))
                .execute()
                .await;
        }
        "drop_columns" => {
            let _ = table.drop_columns(&["b"]).await;
        }
        "alter_columns" => {
            let alterations = vec![ColumnAlteration::new("a".into()).rename("new_a".into())];
            let _ = table.alter_columns(&alterations).await;
        }
        _ => panic!("Unknown operation: {}", operation),
    }

    // Schema call after operation should re-fetch from server (cache invalidated)
    let schema3 = table.schema().await.unwrap();
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

/// Test that schema cache is invalidated when server returns certain error codes
#[rstest]
#[case(400, true)] // 400 Bad Request should invalidate cache
#[case(401, false)] // 401 Unauthorized should NOT invalidate cache
#[case(403, false)] // 403 Forbidden should NOT invalidate cache
#[case(404, true)] // 404 Not Found should invalidate (table might be recreated)
#[case(500, true)] // 500 Internal Server Error should invalidate cache
#[case(503, false)] // 503 Service Unavailable should NOT invalidate cache
#[tokio::test]
async fn test_schema_cache_invalidation_on_errors(
    #[case] error_status: u16,
    #[case] should_invalidate: bool,
) {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        let current_count = call_count_clone.load(Ordering::SeqCst);

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else if path == "/v1/table/my_table/count_rows/" {
            // Return error on first count_rows call
            if current_count == 1 {
                http::Response::builder()
                    .status(error_status)
                    .body("error")
                    .unwrap()
            } else {
                http::Response::builder().status(200).body("10").unwrap()
            }
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call should fetch from server
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second schema call should use cached value
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Perform operation that returns error
    let result = table.count_rows(None).await;
    assert!(result.is_err());

    // Schema call after error - check if cache was invalidated
    let schema3 = table.schema().await.unwrap();
    if should_invalidate {
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            2,
            "Cache should be invalidated for {} error",
            error_status
        );
        assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    } else {
        assert_eq!(
            call_count.load(Ordering::SeqCst),
            1,
            "Cache should NOT be invalidated for {} error",
            error_status
        );
        assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    }
}

/// A pinned snapshot should reuse the version and schema returned by its
/// initial describe instead of issuing two more describe requests.
#[tokio::test]
async fn test_checkout_current_seeds_schema_from_single_describe() {
    let describe_calls = Arc::new(AtomicUsize::new(0));
    let calls = describe_calls.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
        calls.fetch_add(1, Ordering::SeqCst);
        http::Response::builder()
            .status(200)
            .body(
                r#"{"version":42,"schema":{"fields":[{"name":"a","type":{"type":"int32"},"nullable":false}]}}"#,
            )
            .unwrap()
    });

    let snapshot = table.checkout_current().await.unwrap();
    assert_eq!(snapshot.schema().await.unwrap().fields().len(), 1);
    assert_eq!(describe_calls.load(Ordering::SeqCst), 1);
}

/// Test that schema cache is invalidated after checkout
#[tokio::test]
async fn test_schema_cache_invalidation_on_checkout() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        call_count_clone.fetch_add(1, Ordering::SeqCst);
        let count = call_count_clone.load(Ordering::SeqCst);

        if path == "/v1/table/my_table/describe/" {
            // Return different schemas for different calls
            if count <= 2 {
                // First schema call and checkout validation
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                    )
                    .unwrap()
            } else {
                // After checkout
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 2, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false},
                                {"name": "b", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                    )
                    .unwrap()
            }
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call
    let schema1 = table.schema().await.unwrap();
    assert_eq!(schema1.fields().len(), 1);

    // Second schema call should use cached value (no new call)
    let call_count_before = call_count.load(Ordering::SeqCst);
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), call_count_before);

    // Checkout to version 2 (makes a describe call to validate)
    let _ = table.checkout(2).await;

    // Schema call after checkout should re-fetch (cache was invalidated)
    let schema3 = table.schema().await.unwrap();
    assert_eq!(schema3.fields().len(), 2);
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_fetch_does_not_cross_checkout_generation() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel::<()>();
    let arrived_tx = Arc::new(std::sync::Mutex::new(arrived_tx));
    let table = Table::new_with_handler("my_table", move |request| {
        let body = request_body_json(&request);
        let pinned = body["version"].as_u64() == Some(5);
        if !pinned {
            arrived_tx.lock().unwrap().send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }
        let field = if pinned { "pinned" } else { "latest" };
        http::Response::builder()
            .status(200)
            .body(format!(
                r#"{{"version":5,"schema":{{"fields":[{{"name":"{field}","type":{{"type":"int32"}},"nullable":false}}]}}}}"#
            ))
            .unwrap()
    });

    let schema_fetch = tokio::spawn({
        let table = table.clone();
        async move { table.schema().await }
    });
    tokio::task::spawn_blocking(move || arrived_rx.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    table.checkout(5).await.unwrap();
    release_tx.send(()).unwrap();

    let schema = schema_fetch.await.unwrap().unwrap();
    assert!(schema.field_with_name("pinned").is_ok());
    let cached = table.schema().await.unwrap();
    assert!(cached.field_with_name("pinned").is_ok());
}

/// Test that schema cache is invalidated after checkout_latest
#[tokio::test]
async fn test_schema_cache_invalidation_on_checkout_latest() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second schema call should use cached value
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Checkout latest
    let _ = table.checkout_latest().await;

    // Schema call after checkout_latest should re-fetch (cache was invalidated)
    let schema3 = table.schema().await.unwrap();
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

/// Test that schema cache is invalidated after checkout_tag
#[tokio::test]
async fn test_schema_cache_invalidation_on_checkout_tag() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else if path == "/v1/table/my_table/tags/list/" {
            http::Response::builder()
                .status(200)
                .body(r#"{"tags": {"v2": {"version": 2}}}"#)
                .unwrap()
        } else if path == "/v1/table/my_table/tags/version/" {
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 2}"#)
                .unwrap()
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second schema call should use cached value
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Checkout tag
    table
        .checkout_tag("v2")
        .await
        .expect("checkout_tag should succeed");

    // Schema call after checkout_tag should re-fetch (cache was invalidated)
    let schema3 = table.schema().await.unwrap();
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        2,
        "Cache should have been invalidated and re-fetched"
    );
    assert_ne!(
        Arc::as_ptr(&schema3),
        Arc::as_ptr(&schema1),
        "Should be different Arc instances"
    );
}

/// Test that restore invalidates cache (via checkout_latest)
#[tokio::test]
async fn test_schema_cache_invalidation_on_restore() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else if path == "/v1/table/my_table/restore/" {
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 1}"#)
                .unwrap()
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    table.checkout(1).await.unwrap();

    // First schema call
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);

    // Second schema call uses cache
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);

    // Restore operation
    table.restore().await.unwrap();

    // Schema call after restore should re-fetch (cache invalidated)
    let schema3 = table.schema().await.unwrap();
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 3);
}

/// Test that centralized error handling invalidates cache on query errors
#[tokio::test]
async fn test_centralized_error_invalidation_on_query() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        let current_count = call_count_clone.load(Ordering::SeqCst);

        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else if path == "/v1/table/my_table/query/" {
            // Return 400 error on first query (could be schema mismatch)
            if current_count == 1 {
                http::Response::builder()
                    .status(400)
                    .body("Bad request")
                    .unwrap()
            } else {
                // Return empty result for successful query
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/vnd.apache.arrow.stream")
                    .body("")
                    .unwrap()
            }
        } else {
            http::Response::builder()
                .status(404)
                .body("not found")
                .unwrap()
        }
    });

    // First schema call
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Second schema call uses cache
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Query that returns 400 error
    let result = table.query().execute().await;
    assert!(result.is_err());

    // Schema call after error should re-fetch (cache invalidated by centralized handler)
    let schema3 = table.schema().await.unwrap();
    assert_ne!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

/// Test that concurrent schema() calls with an empty cache only trigger one fetch.
#[tokio::test]
async fn test_concurrent_schema_calls_single_fetch() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Arc::new(Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else {
            panic!("Unexpected request: {}", path);
        }
    }));

    let mut handles = Vec::new();
    for _ in 0..10 {
        let table = table.clone();
        handles.push(tokio::spawn(async move { table.schema().await.unwrap() }));
    }

    let schemas: Vec<SchemaRef> = futures::future::try_join_all(handles).await.unwrap();

    // All callers should get the same Arc
    for schema in &schemas {
        assert_eq!(Arc::as_ptr(schema), Arc::as_ptr(&schemas[0]));
    }
    // Only one describe call should have been made
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
}

/// Test that a background refresh is triggered in the refresh window and
/// returns the cached value immediately.
#[tokio::test]
async fn test_background_refresh_triggers_in_window() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        if path == "/v1/table/my_table/describe/" {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                    )
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 2, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false},
                                {"name": "b", "type": { "type": "string" }, "nullable": true}
                            ]}}"#,
                    )
                    .unwrap()
            }
        } else {
            panic!("Unexpected request: {}", path);
        }
    });

    // Populate cache and trigger peek transition to Current state
    let schema1 = table.schema().await.unwrap();
    assert_eq!(schema1.fields().len(), 1);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    // Second call transitions cache from Refreshing to Current via peek()
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));

    // Advance into refresh window (TTL=30s, window=5s, so 26s is in window)
    clock::advance_by(Duration::from_secs(26));

    // This call enters the refresh window: returns cached value and creates
    // a background shared future (Refreshing state with previous).
    let schema3 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));
    // Only the initial fetch so far
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    // Advance past TTL so the previous value expires. This forces the next
    // schema() to Wait on the in-flight shared future, driving it to completion.
    clock::advance_by(Duration::from_secs(30));

    let schema4 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    assert_eq!(schema4.fields().len(), 2);
    assert_ne!(Arc::as_ptr(&schema4), Arc::as_ptr(&schema1));
}

/// Test that multiple calls during the refresh window don't trigger
/// duplicate background refreshes.
#[tokio::test]
async fn test_no_duplicate_background_refreshes() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        if path == "/v1/table/my_table/describe/" {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 1, "schema": {"fields": [
                            {"name": "a", "type": { "type": "int32" }, "nullable": false}
                        ]}}"#,
                )
                .unwrap()
        } else {
            panic!("Unexpected request: {}", path);
        }
    });

    // Populate cache and transition to Current state
    let schema1 = table.schema().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    let _ = table.schema().await.unwrap(); // peek transition

    // Advance into refresh window
    clock::advance_by(Duration::from_secs(26));

    // Multiple rapid calls should all return cached. The first one enters
    // the refresh window and starts a background fetch (Refreshing state).
    // Subsequent calls see Refreshing with a valid previous and return it.
    for _ in 0..5 {
        let schema = table.schema().await.unwrap();
        assert_eq!(Arc::as_ptr(&schema), Arc::as_ptr(&schema1));
    }

    // Advance past TTL and drive the shared future to completion
    clock::advance_by(Duration::from_secs(30));
    let _ = table.schema().await.unwrap();

    // Only one additional describe call (the background refresh),
    // not five separate ones
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

/// Test that if a background refresh fails, the previously cached value
/// is preserved and still returned.
#[tokio::test]
async fn test_background_refresh_error_preserves_cache() {
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_clone = call_count.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        let path = request.url().path();
        if path == "/v1/table/my_table/describe/" {
            let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // First call succeeds
                http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version": 1, "schema": {"fields": [
                                {"name": "a", "type": { "type": "int32" }, "nullable": false}
                            ]}}"#,
                    )
                    .unwrap()
            } else {
                // Subsequent calls fail (422 is not retried)
                http::Response::builder()
                    .status(422)
                    .body("Unprocessable Entity")
                    .unwrap()
            }
        } else {
            panic!("Unexpected request: {}", path);
        }
    });

    // Populate cache and transition to Current state
    let schema1 = table.schema().await.unwrap();
    assert_eq!(schema1.fields().len(), 1);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);
    let _ = table.schema().await.unwrap(); // peek transition

    // Advance into refresh window
    clock::advance_by(Duration::from_secs(26));

    // Trigger background refresh (returns cached value). The background
    // fetch will fail but the previous value should be preserved.
    let schema2 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema2), Arc::as_ptr(&schema1));

    // Still in the refresh window: the previous value is valid,
    // so calling schema() should still return it.
    let schema3 = table.schema().await.unwrap();
    assert_eq!(Arc::as_ptr(&schema3), Arc::as_ptr(&schema1));

    // Advance past TTL. The shared future will be driven and fail.
    // The peek() error path should revert to the previous cached value.
    clock::advance_by(Duration::from_secs(30));

    // After the error, the previous is restored but its timestamp is old,
    // so the next call triggers a new fetch which also fails.
    let result = table.schema().await;
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
    // The error from the failed fetch should be propagated
    assert!(result.is_err());
}
