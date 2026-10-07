// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_query_plain() {
    let expected_data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let expected_data_ref = expected_data.clone();

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        let expected_body = serde_json::json!({
            "filter": "`A` > 0",
            "k": isize::MAX as usize,
            "prefilter": true,
            "vector": [], // Empty vector means no vector query.
            "version": null,
        });
        assert_eq!(body, expected_body);

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let query = AnyQuery::Query(QueryRequest {
        filter: Some(QueryFilter::Sql(r#""A" > 0"#.into())),
        ..Default::default()
    });
    let data = table
        .base_table()
        .query(&query, Default::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].as_ref().unwrap(), &expected_data);
}

fn blob_describe_response() -> http::Response<String> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        crate::blob("image", true),
        Field::new("caption", DataType::Utf8, true),
        crate::blob("thumbnail", true),
    ]);
    let json_schema = JsonSchema::try_from(&schema).unwrap();
    http::Response::builder()
        .status(200)
        .body(serde_json::json!({ "version": 1, "schema": json_schema }).to_string())
        .unwrap()
}

#[rstest]
#[case(semver::Version::new(0, 1, 0))]
#[case(semver::Version::new(0, 5, 0))]
#[tokio::test]
async fn test_blob_columns_read_the_schema_on_any_server_version(#[case] version: semver::Version) {
    let table = Table::new_with_handler_version("my_table", version, |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");
        blob_describe_response()
    });

    let columns = table.blob_columns().await.unwrap();
    assert_eq!(columns, vec!["image".to_string(), "thumbnail".to_string()]);
}

#[tokio::test]
async fn test_fetch_blobs_decodes_null_aligned_bytes() {
    let mut builder = LargeBinaryBuilder::new();
    builder.append_value(b"alpha");
    builder.append_null();
    builder.append_value(b"gamma");
    let blobs = builder.finish();
    let expected = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "image",
            DataType::LargeBinary,
            true,
        )])),
        vec![Arc::new(blobs)],
    )
    .unwrap();
    let expected_ref = expected.clone();

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/fetch_blobs/");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body["column"], "image");
            assert_eq!(body["row_ids"], serde_json::json!([10, 20, 30]));

            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .body(write_ipc_stream_uncompressed(&expected_ref))
                .unwrap()
        },
    );

    let blobs = table.fetch_blobs("image", &[10, 20, 30]).await.unwrap();
    assert_eq!(blobs.len(), 3);
    assert_eq!(blobs.value(0), b"alpha");
    assert!(blobs.is_null(1));
    assert_eq!(blobs.value(2), b"gamma");
}

#[tokio::test]
async fn test_fetch_blobs_concatenates_multiple_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "image",
        DataType::LargeBinary,
        true,
    )]));

    let mut first_builder = LargeBinaryBuilder::new();
    first_builder.append_value(b"alpha");
    first_builder.append_null();
    let first_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(first_builder.finish())]).unwrap();

    let mut second_builder = LargeBinaryBuilder::new();
    second_builder.append_value(b"gamma");
    let second_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(second_builder.finish())]).unwrap();

    let mut body = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &schema).unwrap();
        writer.write(&first_batch).unwrap();
        writer.write(&second_batch).unwrap();
        writer.finish().unwrap();
    }

    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| {
            assert_eq!(request.url().path(), "/v1/table/my_table/fetch_blobs/");
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .body(body.clone())
                .unwrap()
        },
    );

    let blobs = table.fetch_blobs("image", &[10, 20, 30]).await.unwrap();
    assert_eq!(blobs.len(), 3);
    assert_eq!(blobs.value(0), b"alpha");
    assert!(blobs.is_null(1));
    assert_eq!(blobs.value(2), b"gamma");
}

#[tokio::test]
async fn test_fetch_blobs_splits_row_ids_at_one_version_and_preserves_order() {
    let request_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = request_sizes.clone();
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| {
            if request.url().path() == "/v1/table/my_table/describe/" {
                return http::Response::builder()
                    .status(200)
                    .body(br#"{"version":7,"schema":{"fields":[]}}"#.to_vec())
                    .unwrap();
            }
            assert_eq!(request.url().path(), "/v1/table/my_table/fetch_blobs/");
            let body = request_body_json(&request);
            assert_eq!(body["version"], 7);
            let ids = body["row_ids"].as_array().unwrap();
            seen.lock().unwrap().push(ids.len());
            if ids.len() > 1024 {
                return http::Response::builder()
                    .status(400)
                    .body(b"fetch_blobs accepts at most 1024 row IDs".to_vec())
                    .unwrap();
            }
            let mut builder = LargeBinaryBuilder::new();
            for id in ids {
                let id = id.as_u64().unwrap();
                if id == 1023 {
                    builder.append_null();
                } else {
                    builder.append_value(id.to_string().as_bytes());
                }
            }
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "image",
                    DataType::LargeBinary,
                    true,
                )])),
                vec![Arc::new(builder.finish())],
            )
            .unwrap();
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .body(write_ipc_stream_uncompressed(&batch))
                .unwrap()
        },
    );

    let ids: Vec<u64> = (0..1024).chain([42]).collect();
    let blobs = table.fetch_blobs("image", &ids).await.unwrap();
    assert_eq!(blobs.len(), 1025);
    assert_eq!(blobs.value(0), b"0");
    assert_eq!(blobs.value(1022), b"1022");
    assert!(blobs.is_null(1023));
    assert_eq!(blobs.value(1024), b"42");
    assert_eq!(request_sizes.lock().unwrap().as_slice(), &[1024, 1]);
}

#[tokio::test]
async fn test_fetch_blobs_splits_byte_limited_requests_and_reads_large_blob_by_range() {
    // Simulate a lower byte cap so this test exercises the same 400 response
    // without allocating 64 MiB of blob data.
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = requests.clone();
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| {
            let path = request.url().path();
            if path == "/v1/table/my_table/describe/" {
                return http::Response::builder()
                    .status(200)
                    .body(br#"{"version":42,"schema":{"fields":[]}}"#.to_vec())
                    .unwrap();
            }
            if path == "/v1/table/my_table/fetch_blobs/" {
                let body = request_body_json(&request);
                let ids = body["row_ids"].as_array().unwrap();
                if body["version"].is_null() {
                    // Only the initial failed request may read live latest.
                    assert_eq!(ids.len(), 5);
                } else {
                    assert_eq!(body["version"], 42);
                }
                seen.lock().unwrap().push(format!("POST {}", ids.len()));
                let mut builder = LargeBinaryBuilder::new();
                let mut total_bytes = 0;
                for id in ids {
                    let value: Option<&[u8]> = match id.as_u64().unwrap() {
                        10 => Some(b"aaaa"),
                        20 => Some(b"bbb"),
                        30 => None,
                        40 => Some(b"0123456789"),
                        id => panic!("unexpected row id {id}"),
                    };
                    if let Some(value) = value {
                        total_bytes += value.len();
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                if total_bytes > 6 {
                    return http::Response::builder()
                        .status(400)
                        .body(br#"{"error":"Bad request: fetch_blobs accepts at most 67108864 total blob bytes"}"#.to_vec())
                        .unwrap();
                }
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "image",
                        DataType::LargeBinary,
                        true,
                    )])),
                    vec![Arc::new(builder.finish())],
                )
                .unwrap();
                return http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                    .body(write_ipc_stream_uncompressed(&batch))
                    .unwrap();
            }
            assert_eq!(path, "/v1/table/my_table/blob/image/40/bytes");
            assert!(request.url().query().unwrap().contains("version=42"));
            let range = request
                .headers()
                .get(reqwest::header::RANGE)
                .unwrap()
                .to_str()
                .unwrap();
            seen.lock().unwrap().push(format!("GET {range}"));
            match range {
                "bytes=0-0" => http::Response::builder()
                    .status(206)
                    .header(reqwest::header::CONTENT_RANGE, "bytes 0-0/10")
                    .header(VERSION_HEADER, "42")
                    .body(b"0".to_vec())
                    .unwrap(),
                "bytes=0-" => http::Response::builder()
                    .status(206)
                    .header(reqwest::header::CONTENT_RANGE, "bytes 0-9/10")
                    .body(b"0123456789".to_vec())
                    .unwrap(),
                _ => panic!("unexpected range: {range}"),
            }
        },
    );

    let blobs = table
        .fetch_blobs("image", &[10, 20, 30, 40, 20])
        .await
        .unwrap();
    assert_eq!(blobs.len(), 5);
    assert_eq!(blobs.value(0), b"aaaa");
    assert_eq!(blobs.value(1), b"bbb");
    assert!(blobs.is_null(2));
    assert_eq!(blobs.value(3), b"0123456789");
    assert_eq!(blobs.value(4), b"bbb");
    let requests = requests.lock().unwrap();
    assert!(requests.contains(&"GET bytes=0-0".to_string()));
    assert!(requests.contains(&"GET bytes=0-".to_string()));
}

#[tokio::test]
async fn test_fetch_blobs_does_not_split_unrelated_bad_requests() {
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let table =
        Table::new_with_handler_version("my_table", semver::Version::new(0, 5, 0), move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(400)
                .body(b"unknown blob column".to_vec())
                .unwrap()
        });

    assert_fetch_blobs_http_error(
        table.fetch_blobs("missing", &[10, 20]).await.unwrap_err(),
        "unknown blob column",
    );
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

fn table_with_fetch_blobs_response(body: Vec<u8>) -> Table {
    table_with_fetch_blobs_content_type(Some(ARROW_STREAM_CONTENT_TYPE), body)
}

fn table_with_fetch_blobs_content_type(content_type: Option<&'static str>, body: Vec<u8>) -> Table {
    Table::new_with_handler_version("my_table", semver::Version::new(0, 5, 0), move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/fetch_blobs/");
        let mut response = http::Response::builder().status(200);
        if let Some(content_type) = content_type {
            response = response.header(CONTENT_TYPE, content_type);
        }
        response.body(body.clone()).unwrap()
    })
}

fn one_row_blob_batch(column: &str) -> RecordBatch {
    let mut builder = LargeBinaryBuilder::new();
    builder.append_value(b"alpha");
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            column,
            DataType::LargeBinary,
            true,
        )])),
        vec![Arc::new(builder.finish())],
    )
    .unwrap()
}

fn one_row_blob_ipc_stream(column: &str) -> Vec<u8> {
    write_ipc_stream_uncompressed(&one_row_blob_batch(column))
}

#[tokio::test]
async fn test_remote_scan_order_is_not_deterministic() {
    // A distributed scan answers in no fixed order, so callers that assign meaning
    // to row position have to sort for themselves.
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(200)
            .body(Vec::new())
            .unwrap()
    });
    assert!(!table.base_table().scan_order_is_deterministic());
}

#[tokio::test]
async fn test_checkout_branch_pins_without_touching_the_original() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = seen.clone();
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(br#"{"version": 42, "schema": {"fields": []}}"#.to_vec())
                .unwrap(),
            "/v1/table/my_table/count_rows/" => {
                let body = request_body_json(&request);
                recorder.lock().unwrap().push(body["version"].clone());
                http::Response::builder()
                    .status(200)
                    .body(b"0".to_vec())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        },
    );

    let pinned = table.checkout_branch("main", Some(42)).await.unwrap();
    pinned.count_rows(None).await.unwrap();
    table.count_rows(None).await.unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0], 42, "the pinned handle must send its version");
    assert!(
        seen[1].is_null(),
        "the original handle must still track latest, got {:?}",
        seen[1]
    );
}

#[tokio::test]
async fn test_fetch_blobs_sends_the_checked_out_version() {
    let ipc = one_row_blob_ipc_stream("image");
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(br#"{"version": 42, "schema": {"fields": []}}"#.to_vec())
                .unwrap(),
            "/v1/table/my_table/fetch_blobs/" => {
                let body = request_body_json(&request);
                assert_eq!(
                    body["version"], 42,
                    "blob reads must use the same snapshot as the query"
                );
                http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                    .body(ipc.clone())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        },
    );

    table.checkout(42).await.unwrap();

    assert_eq!(table.fetch_blobs("image", &[10]).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_fetch_blobs_sends_the_checked_out_branch() {
    use lance::dataset::refs::Ref;
    let ipc = one_row_blob_ipc_stream("image");
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body(b"{}".to_vec())
                .unwrap(),
            "/v1/table/my_table/fetch_blobs/" => {
                let body = request_body_json(&request);
                assert_eq!(body["branch"], "exp", "blob reads must stay on the branch");
                http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                    .body(ipc.clone())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        },
    );

    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();

    assert_eq!(branch.fetch_blobs("image", &[10]).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_fetch_blobs_sends_a_nested_column_as_a_dotted_path() {
    let ipc = one_row_blob_ipc_stream("info.blob");
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        move |request| {
            let body = request_body_json(&request);
            assert_eq!(body["column"], "info.blob");
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .body(ipc.clone())
                .unwrap()
        },
    );

    let blobs = table.fetch_blobs("info.blob", &[10]).await.unwrap();

    assert_eq!(blobs.value(0), b"alpha");
}

fn write_empty_ipc_stream(schema: &Schema) -> Vec<u8> {
    let mut body = Vec::new();
    arrow_ipc::writer::StreamWriter::try_new(&mut body, schema)
        .unwrap()
        .finish()
        .unwrap();
    body
}

fn assert_fetch_blobs_http_error(error: Error, expected: &str) {
    match error {
        Error::Http {
            source, request_id, ..
        } => {
            assert!(source.to_string().contains(expected));
            assert!(!request_id.is_empty());
        }
        error => panic!("expected HTTP error, got {error}"),
    }
}

fn assert_not_supported_error(error: Error, expected: &str) {
    match error {
        Error::NotSupported { message } => assert!(message.contains(expected)),
        error => panic!("expected not-supported error, got {error}"),
    }
}

#[tokio::test]
async fn test_fetch_blobs_rejects_missing_column() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::LargeBinary,
            true,
        )])),
        vec![Arc::new(LargeBinaryArray::from(vec![Some(
            b"value".as_slice(),
        )]))],
    )
    .unwrap();
    let table = table_with_fetch_blobs_response(write_ipc_stream_uncompressed(&batch));

    let error = table.fetch_blobs("image", &[10]).await.unwrap_err();
    assert_fetch_blobs_http_error(error, "missing the 'image' column");
}

#[tokio::test]
async fn test_fetch_blobs_rejects_wrong_column_type() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "image",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    let table = table_with_fetch_blobs_response(write_ipc_stream_uncompressed(&batch));

    let error = table.fetch_blobs("image", &[10]).await.unwrap_err();
    assert_fetch_blobs_http_error(
        error,
        "type Int32, expected Binary, LargeBinary, or BinaryView",
    );
}

#[rstest]
#[case(DataType::Binary)]
#[case(DataType::LargeBinary)]
#[case(DataType::BinaryView)]
#[tokio::test]
async fn test_fetch_blobs_accepts_binary_large_binary_and_binary_view(#[case] data_type: DataType) {
    let binary_values = BinaryArray::from(vec![
        Some(b"alpha".as_slice()),
        None,
        Some(b"gamma".as_slice()),
    ]);
    let typed_column = arrow::compute::cast(&binary_values, &data_type).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("image", data_type, true)])),
        vec![typed_column],
    )
    .unwrap();
    let table = table_with_fetch_blobs_response(write_ipc_stream_uncompressed(&batch));

    let blobs = table.fetch_blobs("image", &[10, 20, 30]).await.unwrap();
    assert_eq!(blobs.len(), 3);
    assert_eq!(blobs.value(0), b"alpha");
    assert!(blobs.is_null(1));
    assert_eq!(blobs.value(2), b"gamma");
}

#[tokio::test]
async fn test_fetch_blobs_rejects_row_count_mismatch() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "image",
            DataType::LargeBinary,
            true,
        )])),
        vec![Arc::new(LargeBinaryArray::from(vec![Some(
            b"value".as_slice(),
        )]))],
    )
    .unwrap();
    let table = table_with_fetch_blobs_response(write_ipc_stream_uncompressed(&batch));

    let error = table.fetch_blobs("image", &[10, 20]).await.unwrap_err();
    assert_fetch_blobs_http_error(error, "returned 1 rows for 2 row ids");
}

#[tokio::test]
async fn test_fetch_blobs_rejects_zero_batches_for_nonempty_row_ids() {
    let schema = Schema::new(vec![Field::new("image", DataType::LargeBinary, true)]);
    let table = table_with_fetch_blobs_response(write_empty_ipc_stream(&schema));

    let error = table.fetch_blobs("image", &[10]).await.unwrap_err();
    assert_fetch_blobs_http_error(error, "returned 0 rows for 1 row ids");
}

#[tokio::test]
async fn test_fetch_blobs_skips_the_request_for_empty_row_ids() {
    let table = Table::new_with_handler("my_table", |_| -> http::Response<String> {
        panic!("fetch_blobs must not call the server for an empty selection");
    });

    let blobs = table.fetch_blobs("image", &[]).await.unwrap();
    assert!(blobs.is_empty());
}

#[tokio::test]
async fn test_blob_byte_apis_not_supported_on_old_server() {
    let table = Table::new_with_handler("my_table", |_| -> http::Response<String> {
        panic!("blob request must not reach a server without blob support");
    });

    assert_not_supported_error(
        table.fetch_blobs("image", &[1]).await.unwrap_err(),
        "fetch_blobs",
    );

    assert_not_supported_error(
        table.fetch_blob_files("image", &[1]).await.unwrap_err(),
        "requires LanceDB Cloud server 0.5.0 or newer",
    );
}

#[rstest]
#[case(ARROW_STREAM_CONTENT_TYPE)]
#[case("application/vnd.apache.arrow.stream; charset=utf-8")]
#[case("APPLICATION/VND.APACHE.ARROW.STREAM")]
#[tokio::test]
async fn test_fetch_blobs_accepts_stream_content_type_variants(#[case] content_type: &'static str) {
    let table = table_with_fetch_blobs_content_type(
        Some(content_type),
        write_ipc_stream_uncompressed(&one_row_blob_batch("image")),
    );

    let blobs = table.fetch_blobs("image", &[10]).await.unwrap();

    assert_eq!(blobs.value(0), b"alpha");
}

// Shared decoder accepts file framing because /query still returns it.
// fetch_blobs wire contract is stream. Enforced on the server.
#[rstest]
#[case(ARROW_FILE_CONTENT_TYPE)]
#[case("application/vnd.apache.arrow.file; charset=utf-8")]
#[case("APPLICATION/VND.APACHE.ARROW.FILE")]
#[tokio::test]
async fn test_fetch_blobs_accepts_file_content_type_variants(#[case] content_type: &'static str) {
    let table = table_with_fetch_blobs_content_type(
        Some(content_type),
        write_ipc_file(&one_row_blob_batch("image")),
    );

    let blobs = table.fetch_blobs("image", &[10]).await.unwrap();

    assert_eq!(blobs.value(0), b"alpha");
}

#[rstest]
#[case(ARROW_STREAM_CONTENT_TYPE, write_ipc_file(&one_row_blob_batch("image")))]
#[case(
    ARROW_FILE_CONTENT_TYPE,
    write_ipc_stream_uncompressed(&one_row_blob_batch("image"))
)]
#[tokio::test]
async fn test_fetch_blobs_fails_when_the_body_contradicts_the_content_type(
    #[case] content_type: &'static str,
    #[case] body: Vec<u8>,
) {
    let table = table_with_fetch_blobs_content_type(Some(content_type), body);

    let error = table.fetch_blobs("image", &[10]).await.unwrap_err();

    assert!(
        matches!(error, Error::Arrow { .. }),
        "expected an Arrow decode failure, got {error}"
    );
}

#[tokio::test]
async fn test_fetch_blobs_rejects_a_response_that_is_not_arrow_ipc() {
    let table =
        table_with_fetch_blobs_content_type(Some("application/json"), br#"{"blobs": []}"#.to_vec());

    let error = table.fetch_blobs("image", &[10]).await.unwrap_err();
    assert_fetch_blobs_http_error(error, "got 'application/json'");
}

#[tokio::test]
async fn test_fetch_blobs_without_content_type_falls_back_to_file_framing() {
    let table =
        table_with_fetch_blobs_content_type(None, write_ipc_file(&one_row_blob_batch("image")));

    let blobs = table.fetch_blobs("image", &[10]).await.unwrap();

    assert_eq!(blobs.value(0), b"alpha");
}
