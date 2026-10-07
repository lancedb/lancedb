// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_not_found() {
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(404)
            .body("table my_table not found")
            .unwrap()
    });

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let example_data_for_add = || batch.clone();
    let example_data_for_merge = || -> Box<dyn RecordBatchReader + Send> {
        Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ))
    };

    // These table operations should translate 404 to TableNotFound.
    let results: Vec<BoxFuture<'_, Result<()>>> = vec![
        Box::pin(table.version().map_ok(|_| ())),
        Box::pin(table.schema().map_ok(|_| ())),
        Box::pin(table.count_rows(None).map_ok(|_| ())),
        Box::pin(table.update().column("a", "a + 1").execute().map_ok(|_| ())),
        Box::pin(table.add(example_data_for_add()).execute().map_ok(|_| ())),
        Box::pin(
            table
                .merge_insert(&["test"])
                .execute(example_data_for_merge())
                .map_ok(|_| ()),
        ),
        Box::pin(table.delete("false").map_ok(|_| ())),
        Box::pin(
            table
                .add_columns()
                .transform(NewColumnTransform::SqlExpressions(vec![(
                    "x".into(),
                    "y".into(),
                )]))
                .execute()
                .map_ok(|_| ()),
        ),
        Box::pin(async {
            let alterations = vec![ColumnAlteration::new("x".into()).rename("y".into())];
            table.alter_columns(&alterations).await.map(|_| ())
        }),
        Box::pin(table.drop_columns(&["a"]).map_ok(|_| ())),
        // TODO: other endpoints.
    ];

    for result in results {
        let result = result.await;
        assert!(result.is_err());
        assert!(
            matches!(&result, &Err(Error::TableNotFound { ref name, .. }) if name == "my_table")
        );
        let full_error_report = snafu::Report::from_error(result.unwrap_err()).to_string();
        assert!(full_error_report.contains("table my_table not found"));
    }
}

#[tokio::test]
async fn test_version() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 42, "schema": { "fields": [] }}"#)
            .unwrap()
    });

    let version = table.version().await.unwrap();
    assert_eq!(version, 42);
}

#[tokio::test]
async fn test_schema() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

        http::Response::builder()
            .status(200)
            .body(
                r#"{"version": 42, "schema": {"fields": [
                    {"name": "a", "type": { "type": "int32" }, "nullable": false},
                    {"name": "b", "type": { "type": "string" }, "nullable": true}
                ], "metadata": {"key": "value"}}}"#,
            )
            .unwrap()
    });

    let expected = Arc::new(
        Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ])
        .with_metadata([("key".into(), "value".into())].into()),
    );

    let schema = table.schema().await.unwrap();
    assert_eq!(schema, expected);
}

#[tokio::test]
async fn test_count_rows() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );
        assert_eq!(
            request.body().unwrap().as_bytes().unwrap(),
            br#"{"version":null}"#
        );

        http::Response::builder().status(200).body("42").unwrap()
    });

    let count = table.count_rows(None).await.unwrap();
    assert_eq!(count, 42);

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );
        assert_eq!(
            request.body().unwrap().as_bytes().unwrap(),
            br#"{"predicate":"`A` > 10","version":null}"#
        );

        http::Response::builder().status(200).body("42").unwrap()
    });

    let count = table
        .base_table()
        .count_rows(Some(Filter::Sql(r#""A" > 10"#.into())))
        .await
        .unwrap();
    assert_eq!(count, 42);
}

#[rstest]
#[case("", 0)]
#[case("{}", 0)]
#[case(r#"{"request_id": "test-request-id"}"#, 0)]
#[case(r#"{"version": 43}"#, 43)]
#[tokio::test]
async fn test_add_append(#[case] response_body: &str, #[case] expected_version: u64) {
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Clone response_body to give it 'static lifetime for the closure
    let response_body = response_body.to_string();

    let describe_body = describe_response(&data.schema());
    let (sender, receiver) = std::sync::mpsc::channel();
    let table =
        Table::new_with_handler("my_table", move |mut request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/my_table/insert/" => {
                assert_eq!(request.method(), "POST");
                assert!(
                    request
                        .url()
                        .query_pairs()
                        .filter(|(k, _)| k == "mode")
                        .all(|(_, v)| v == "append")
                );
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();
                http::Response::builder()
                    .status(200)
                    .body(response_body.clone())
                    .unwrap()
            }
            path => panic!("Unexpected request path: {}", path),
        });
    let result = table.add(data.clone()).execute().await.unwrap();

    // Check version matches expected value
    assert_eq!(result.version, expected_version);

    let body = receiver.recv().unwrap();
    let body = collect_body(body).await;
    let expected_body = write_ipc_stream(&data);
    assert_eq!(&body, &expected_body);
}

#[tokio::test]
async fn add_rejects_external_blob_flag_before_any_request() {
    let table = Table::new_with_handler::<String>("my_table", |request| {
        panic!("Unexpected request: {}", request.url().path())
    });
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let err = table
        .add(data)
        .allow_external_blob_outside_bases(true)
        .execute()
        .await
        .unwrap_err();

    assert!(matches!(err, Error::NotSupported { .. }), "got {err:?}");
    assert!(err.to_string().contains("local tables"));
}

#[tokio::test]
async fn add_string_blob_becomes_uri_struct_without_the_local_flag() {
    let table_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        crate::blob("image", true),
    ]);
    let describe_body = describe_response(&table_schema);
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("s3://bucket/key")])),
        ],
    )
    .unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();
    let table =
        Table::new_with_handler("my_table", move |mut request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/my_table/insert/" => {
                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap()
            }
            path => panic!("Unexpected path: {path}"),
        });

    table.add(input).execute().await.unwrap();

    let body = collect_body(receiver.recv().unwrap()).await;
    let mut reader =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    let image = batch
        .column_by_name("image")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("remote add should send the coerced blob struct");
    let uri: &StringArray = image
        .column_by_name("uri")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(uri.value(0), "s3://bucket/key");
    assert!(image.column_by_name("data").unwrap().is_null(0));
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_add_overwrite(#[case] old_server: bool) {
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let describe_body = describe_response(&data.schema());
    let (sender, receiver) = std::sync::mpsc::channel();
    let table =
        Table::new_with_handler("my_table", move |mut request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/my_table/insert/" => {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request
                        .url()
                        .query_pairs()
                        .find(|(k, _)| k == "mode")
                        .map(|kv| kv.1)
                        .as_deref(),
                    Some("overwrite"),
                    "Expected mode=overwrite"
                );

                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );

                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();

                if old_server {
                    http::Response::builder()
                        .status(200)
                        .body("".to_string())
                        .unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#.to_string())
                        .unwrap()
                }
            }
            path => panic!("Unexpected request path: {}", path),
        });

    let result = table
        .add(data.clone())
        .mode(AddDataMode::Overwrite)
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, if old_server { 0 } else { 43 });

    let body = receiver.recv().unwrap();
    let body = collect_body(body).await;
    let expected_body = write_ipc_stream(&data);
    assert_eq!(&body, &expected_body);
}

#[tokio::test]
async fn test_add_preprocessing() {
    use crate::table::NaNVectorBehavior;
    use arrow_array::{FixedSizeListArray, Float32Array, Int64Array};

    // The table schema: {id: Int64, vec: FixedSizeList<Float32>[3]}
    let table_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            false,
        ),
    ]);
    let json_schema = JsonSchema::try_from(&table_schema).unwrap();
    let describe_body = serde_json::to_string(&json!({
        "version": 1,
        "schema": json_schema,
    }))
    .unwrap();

    // ---- Part 1: NaN vectors should be rejected by default ----
    let nan_data = RecordBatch::try_new(
        Arc::new(table_schema.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    3,
                    Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0])),
                    None,
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    let describe_body_clone = describe_body.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_body_clone.clone())
            .unwrap(),
        "/v1/table/my_table/insert/" => http::Response::builder()
            .status(200)
            .body(r#"{"version": 2}"#.to_string())
            .unwrap(),
        path => panic!("Unexpected path: {path}"),
    });

    let result = table.add(nan_data).execute().await;
    assert!(result.is_err(), "NaN vectors should be rejected by default");
    assert!(
        result.unwrap_err().to_string().contains("NaN"),
        "error should mention NaN"
    );

    // ---- Part 2: With Keep, should handle casting and missing columns ----
    // Input: {id: Int32 (needs cast to Int64), vec: FixedSizeList<Float32>[3] with NaN}
    // Table expects Int64 for id; NaN should be kept.
    let input_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            false,
        ),
    ]);
    let cast_data = RecordBatch::try_new(
        Arc::new(input_schema),
        vec![
            Arc::new(Int32Array::from(vec![42])),
            Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    3,
                    Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0])),
                    None,
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();
    let table =
        Table::new_with_handler("my_table", move |mut request| match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/my_table/insert/" => {
                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap()
            }
            path => panic!("Unexpected path: {path}"),
        });

    table
        .add(cast_data)
        .on_nan_vectors(NaNVectorBehavior::Keep)
        .execute()
        .await
        .unwrap();

    // Verify the data sent to the server was cast to the table schema.
    let body = receiver.recv().unwrap();
    let body = collect_body(body).await;
    let cursor = std::io::Cursor::new(body);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    let ids: &Int64Array = batch.column(0).as_any().downcast_ref().unwrap();
    assert_eq!(ids.value(0), 42);
}

#[rstest]
#[case::old_server("", 0, 0)]
#[case::rows_updated(r#"{"rows_updated": 5, "version": 43}"#, 5, 43)]
#[case::updated_rows(r#"{"updated_rows": 5, "version": 43}"#, 5, 43)]
#[case::zero_updated_rows(r#"{"updated_rows": 0, "version": 43}"#, 0, 43)]
#[case::missing_row_count(r#"{"version": 43}"#, 0, 43)]
#[tokio::test]
async fn test_update(
    #[case] response_body: &'static str,
    #[case] expected_rows_updated: u64,
    #[case] expected_version: u64,
    #[values(true, false)] filtered: bool,
) {
    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/update/" {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            if let Some(body) = request.body().unwrap().as_bytes() {
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let updates = value.get("updates").unwrap().as_array().unwrap();
                assert!(updates.len() == 2);

                let col_name = updates[0][0].as_str().unwrap();
                let expression = updates[0][1].as_str().unwrap();
                assert_eq!(col_name, "a");
                assert_eq!(expression, "a + 1");

                let col_name = updates[1][0].as_str().unwrap();
                let expression = updates[1][1].as_str().unwrap();
                assert_eq!(col_name, "b");
                assert_eq!(expression, "b - 1");

                assert_eq!(
                    value.get("predicate").unwrap(),
                    &serde_json::json!(if filtered { Some("`B` > 10") } else { None })
                );
            }

            http::Response::builder()
                .status(200)
                .body(response_body)
                .unwrap()
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let mut update = table.update().column("a", "a + 1").column("b", "b - 1");
    if filtered {
        update = update.only_if(r#""B" > 10"#);
    }
    let result = table.base_table().update(update).await.unwrap();

    assert_eq!(result.version, expected_version);
    assert_eq!(result.rows_updated, expected_rows_updated);
}

#[tokio::test]
async fn test_alter_columns_rejects_missing_changes_before_request() {
    let table = Table::new_with_handler::<String>("my_table", |request| {
        panic!("Unexpected request: {}", request.url().path())
    });

    for alterations in [
        vec![ColumnAlteration::new("id".into())],
        vec![
            ColumnAlteration::new("id".into()).rename("new_id".into()),
            ColumnAlteration::new("id".into()),
        ],
    ] {
        let err = table.alter_columns(&alterations).await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
        assert!(err.to_string().contains("path 'id'"));
    }
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_alter_columns(#[case] old_server: bool) {
    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/alter_columns/" {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body = std::str::from_utf8(body).unwrap();
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            let alterations = value.get("alterations").unwrap().as_array().unwrap();
            assert!(alterations.len() == 2);

            let path = alterations[0]["path"].as_str().unwrap();
            let data_type = alterations[0]["data_type"]["type"].as_str().unwrap();
            assert_eq!(path, "b.c");
            assert_eq!(data_type, "int32");

            let path = alterations[1]["path"].as_str().unwrap();
            let nullable = alterations[1]["nullable"].as_bool().unwrap();
            let rename = alterations[1]["rename"].as_str().unwrap();
            assert_eq!(path, "x");
            assert!(nullable);
            assert_eq!(rename, "y");

            if old_server {
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 43}"#.to_string())
                    .unwrap()
            }
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let result = table
        .alter_columns(&[
            ColumnAlteration::new("b.c".into()).cast_to(DataType::Int32),
            ColumnAlteration::new("x".into())
                .rename("y".into())
                .set_nullable(true),
        ])
        .await
        .unwrap();

    assert_eq!(result.version, if old_server { 0 } else { 43 });
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_merge_insert(#[case] old_server: bool) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));

    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/merge_insert/" {
            assert_eq!(request.method(), "POST");

            let params = request.url().query_pairs().collect::<HashMap<_, _>>();
            assert_eq!(params["on"], "some_col");
            assert_eq!(params["when_matched_update_all"], "true");
            assert_eq!(params["when_not_matched_insert_all"], "false");
            assert_eq!(params["when_not_matched_by_source_delete"], "false");
            assert_eq!(params["when_matched_update_all_filt"], "target.`A` > 0");
            assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));
            assert!(!params.contains_key("use_index"));

            if old_server {
                http::Response::builder().status(200).body("{}").unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 43, "num_deleted_rows": 0, "num_inserted_rows": 3, "num_updated_rows": 0}"#)
                    .unwrap()
            }
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let mut merge = table.merge_insert(&["some_col"]);
    merge.when_matched_update_all(Some(r#"target."A" > 0"#.into()));
    let result = table.base_table().merge_insert(merge, data).await.unwrap();

    assert_eq!(result.version, if old_server { 0 } else { 43 });
    if !old_server {
        assert_eq!(result.num_deleted_rows, 0);
        assert_eq!(result.num_inserted_rows, 3);
        assert_eq!(result.num_updated_rows, 0);
    }
}

#[rstest]
#[case::no_batches_insert(false, false)]
#[case::empty_batch_insert(true, false)]
#[case::no_batches_delete(false, true)]
#[case::empty_batch_delete(true, true)]
#[tokio::test]
async fn test_merge_insert_empty_source(#[case] has_batch: bool, #[case] delete_unmatched: bool) {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    if !delete_unmatched {
        fields.extend([
            Field::new("k", DataType::Int64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
        ]);
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("source".to_string(), "empty".to_string())]),
    ));
    let batches = if has_batch {
        vec![Ok(RecordBatch::new_empty(schema.clone()))]
    } else {
        vec![]
    };
    let data: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(batches, schema.clone()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_ref = attempts.clone();
    let num_deleted_rows = if delete_unmatched { 3 } else { 0 };

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");
        assert_eq!(request.headers()[CONTENT_TYPE], ARROW_STREAM_CONTENT_TYPE);
        let params = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(params["on"], "id");
        assert_eq!(
            params["when_not_matched_insert_all"],
            (!delete_unmatched).to_string()
        );
        assert_eq!(
            params["when_not_matched_by_source_delete"],
            delete_unmatched.to_string()
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let reader = StreamReader::try_new(Cursor::new(body), None).unwrap();
        assert_eq!(reader.schema(), schema);
        for batch in reader {
            assert_eq!(batch.unwrap().num_rows(), 0);
        }

        // The empty source must retain its schema when replayed after a conflict.
        if attempts_ref.fetch_add(1, Ordering::SeqCst) == 0 {
            http::Response::builder()
                .status(409)
                .body(String::new())
                .unwrap()
        } else {
            http::Response::builder()
                .status(200)
                .body(
                    json!({
                        "version": 43,
                        "num_deleted_rows": num_deleted_rows,
                        "num_inserted_rows": 0,
                        "num_updated_rows": 0,
                    })
                    .to_string(),
                )
                .unwrap()
        }
    });

    let mut merge = table.merge_insert(&["id"]);
    if delete_unmatched {
        merge.when_not_matched_by_source_delete(None);
    } else {
        merge.when_not_matched_insert_all();
    }
    let result = merge.execute(data).await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(result.version, 43);
    assert_eq!(result.num_deleted_rows, num_deleted_rows);
    assert_eq!(result.num_inserted_rows, 0);
    assert_eq!(result.num_updated_rows, 0);
}

#[tokio::test]
async fn test_merge_insert_composite_key() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));

    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");

        // One repeated `on` per column, in the order the caller gave them.
        let on = request
            .url()
            .query_pairs()
            .filter(|(key, _)| key == "on")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        assert_eq!(on, vec!["shard_key".to_string(), "id".to_string()]);

        let params = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(params["when_matched_update_all"], "true");
        assert_eq!(params["when_not_matched_insert_all"], "true");

        http::Response::builder()
            .status(200)
            .body(r#"{"version": 43, "num_deleted_rows": 0, "num_inserted_rows": 3, "num_updated_rows": 0}"#)
            .unwrap()
    });

    let mut merge = table.merge_insert(&["shard_key", "id"]);
    merge.when_matched_update_all(None);
    merge.when_not_matched_insert_all();
    let result = table.base_table().merge_insert(merge, data).await.unwrap();

    assert_eq!(result.num_inserted_rows, 3);
}

#[tokio::test]
async fn test_merge_insert_rejects_repeated_on_column() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));

    let table = Table::new_with_handler::<&str>("my_table", |request| {
        panic!("Unexpected request: {}", request.url());
    });

    let merge = table.merge_insert(&["id", "id"]);
    let err = table
        .base_table()
        .merge_insert(merge, data)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::InvalidInput { message } if message.contains("'id' is repeated")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_merge_insert_retries_on_409() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));

    // Default parameters
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");

        let params = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(params["on"], "some_col");
        assert_eq!(params["when_matched_update_all"], "false");
        assert_eq!(params["when_not_matched_insert_all"], "false");
        assert_eq!(params["when_not_matched_by_source_delete"], "false");
        assert!(!params.contains_key("when_matched_update_all_filt"));
        assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));

        http::Response::builder().status(409).body("").unwrap()
    });

    let e = table
        .merge_insert(&["some_col"])
        .execute(data)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("Hit retry limit"));
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_delete(#[case] old_server: bool) {
    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/delete/" {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let predicate = body.get("predicate").unwrap().as_str().unwrap();
            assert_eq!(predicate, "`ID` in (1, 2, 3)");

            if old_server {
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 43}"#.to_string())
                    .unwrap()
            }
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let result = table
        .base_table()
        .delete(Predicate::String(r#""ID" in (1, 2, 3)"#))
        .await
        .unwrap();
    assert_eq!(result.version, if old_server { 0 } else { 43 });
}

#[tokio::test]
async fn test_delete_expr() {
    use datafusion_expr::{col, lit};

    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/delete/" {
            assert_eq!(request.method(), "POST");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert!(body.get("predicate").unwrap().is_string());

            http::Response::builder()
                .status(200)
                .body(r#"{"num_deleted_rows": 4, "version": 2}"#)
                .unwrap()
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let expr = col("id").gt(lit(5));
    let result = table.delete(&expr).await.unwrap();
    assert_eq!(result.num_deleted_rows, 4);
    assert_eq!(result.version, 2);
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_drop_columns(#[case] old_server: bool) {
    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/drop_columns/" {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body = std::str::from_utf8(body).unwrap();
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            let columns = value.get("columns").unwrap().as_array().unwrap();
            assert!(columns.len() == 2);

            let col1 = columns[0].as_str().unwrap();
            let col2 = columns[1].as_str().unwrap();
            assert_eq!(col1, "a");
            assert_eq!(col2, "b");

            if old_server {
                http::Response::builder().status(200).body("{}").unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 43}"#)
                    .unwrap()
            }
        } else {
            panic!("Unexpected request path: {}", request.url().path());
        }
    });

    let result = table.drop_columns(&["a", "b"]).await.unwrap();
    assert_eq!(result.version, if old_server { 0 } else { 43 });
}
