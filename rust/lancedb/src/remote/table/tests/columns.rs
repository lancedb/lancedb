// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_add_columns(#[case] old_server: bool) {
    let table = Table::new_with_handler("my_table", move |request| {
        if request.url().path() == "/v1/table/my_table/describe/" {
            simple_describe_response()
        } else if request.url().path() == "/v1/table/my_table/add_columns/" {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body = std::str::from_utf8(body).unwrap();
            let value: serde_json::Value = serde_json::from_str(body).unwrap();
            let new_columns = value.get("new_columns").unwrap().as_array().unwrap();
            assert!(new_columns.len() == 2);

            let col_name = new_columns[0]["name"].as_str().unwrap();
            let expression = new_columns[0]["expression"].as_str().unwrap();
            assert_eq!(col_name, "b");
            assert_eq!(expression, "a + 1");

            let col_name = new_columns[1]["name"].as_str().unwrap();
            let expression = new_columns[1]["expression"].as_str().unwrap();
            assert_eq!(col_name, "x");
            assert_eq!(expression, "cast(NULL as int32)");

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
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![
            ("b".into(), "a + 1".into()),
            ("x".into(), "cast(NULL as int32)".into()),
        ]))
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, if old_server { 0 } else { 43 });
}

/// A pyarrow schema goes over the wire as an Arrow IPC schema under an
/// Arrow content type, not as a JSON description of the types, so a
/// field arrives exactly as sent. The branch has no JSON envelope to
/// ride in and goes on the query string.
#[tokio::test]
async fn test_add_columns_sends_an_arrow_schema_for_all_nulls() {
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => simple_describe_response(),
        "/v1/table/my_table/add_columns/" => {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                ARROW_STREAM_CONTENT_TYPE
            );
            assert_eq!(
                request.url().query_pairs().find(|(k, _)| k == "branch"),
                None,
                "the main branch sends no branch parameter"
            );

            // The payload decodes back to the exact schema, types and
            // field metadata included.
            let body = request.body().unwrap().as_bytes().unwrap();
            let reader =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(body), None).unwrap();
            let sent = reader.schema();
            assert_eq!(sent.fields().len(), 2);
            assert_eq!(sent.field(0).name(), "ts");
            assert_eq!(
                sent.field(0).data_type(),
                &DataType::Timestamp(
                    arrow_schema::TimeUnit::Nanosecond,
                    Some("America/New_York".into())
                )
            );
            assert!(sent.field(0).is_nullable());
            assert_eq!(sent.field(1).name(), "price");
            assert_eq!(sent.field(1).data_type(), &DataType::Decimal128(38, 10));
            assert_eq!(
                sent.field(1).metadata().get("unit").map(String::as_str),
                Some("cents")
            );

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 7}"#.to_string())
                .unwrap()
        }
        path => panic!("Unexpected request path: {path}"),
    });

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some("America/New_York".into()),
            ),
            true,
        ),
        Field::new("price", DataType::Decimal128(38, 10), true).with_metadata(
            std::collections::HashMap::from([("unit".to_string(), "cents".to_string())]),
        ),
    ]));

    let result = table
        .add_columns()
        .transform(NewColumnTransform::AllNulls(schema))
        .execute()
        .await
        .unwrap();

    assert_eq!(result.version, 7);
}

/// A declaration is sent as `{name, computed}` for the server to plan; the
/// client never types the expression itself.
#[tokio::test]
async fn test_add_computed_columns_sends_the_expression() {
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => simple_describe_response(),
        "/v1/table/my_table/add_columns/" => {
            assert_eq!(request.method(), "POST");
            let body = request.body().unwrap().as_bytes().unwrap();
            let value: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                value["new_columns"],
                serde_json::json!([{"name": "doubled", "computed": "x * 2"}])
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version": 7}"#.to_string())
                .unwrap()
        }
        path => panic!("Unexpected path: {path}"),
    });

    let result = table
        .add_columns()
        .computed("doubled", "x * 2")
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 7);
}

#[tokio::test]
async fn test_add_scalar_function_column_sends_atomic_null_declaration() {
    let table = Table::new_with_handler("my_table", |request| {
        match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(
                r#"{"version":1,"schema":{"fields":[{"name":"description","nullable":true,"type":{"type":"string"}}]}}"#,
            )
            .unwrap(),
        "/v1/table/my_table/add_columns/" => {
            let actual: serde_json::Value = serde_json::from_slice(
                request.body().unwrap().as_bytes().unwrap(),
            )
            .unwrap();
            let expected: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../tests/fixtures/first_class_functions/v1/remote_scalar_declaration_request.json"
            ))
            .unwrap();
            assert_eq!(actual, expected);
            http::Response::builder()
                .status(200)
                .body(r#"{"version":8}"#)
                .unwrap()
        }
        path => panic!("Unexpected path: {path}"),
    }
    });
    let application = crate::function::FunctionApplication::from_json(
        r#"{
                "function":{"name":"embed","version":"1","object_id":"fixture","location":"memory:///fixture","manifest_digest":"sha256:7e22f815b6648e14f093a3979a8e5a2082fa773ebe1ec84b135cae7e84d6f8e6"},
                "inputs":[{"parameter":"text","kind":"column","value":{"path":"description"}}],
                "output":{"kind":"scalar","arrow_type":"list<float32>","nullable":false}
            }"#,
    )
    .unwrap();

    let result = table
        .add_columns()
        .function_as("embedding", application)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 8);
}

/// The fixture binding's table: `title` and `body` bound as inputs, its
/// two outputs declared, plus an unbound `spare`.
fn fixture_bound_schema() -> Schema {
    let binding = crate::function::FunctionBinding::from_json(include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
    ))
    .unwrap();
    let binding_metadata =
        crate::table::computed_columns::function_bindings_metadata(std::slice::from_ref(&binding))
            .unwrap();
    let mut fields = vec![
        Field::new("title", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ];
    fields.extend(binding.outputs().iter().map(|output| {
        let data_type = match output.arrow_type.as_str() {
            "utf8" => DataType::Utf8,
            "int64" => DataType::Int64,
            other => panic!("unexpected fixture output type {other}"),
        };
        Field::new(&output.output_name, data_type, true).with_metadata(
            crate::table::computed_columns::function_computed_column_metadata(
                binding.binding_id(),
                output.output_ordinal,
                &["title".into(), "body".into()],
            ),
        )
    }));
    fields.push(Field::new("spare", DataType::Int32, true));
    Schema::new_with_metadata(
        fields,
        HashMap::from([(
            crate::table::computed_columns::FUNCTION_BINDINGS_META_KEY.to_string(),
            binding_metadata,
        )]),
    )
}

/// Only a column the binding uses is refused, and it is refused before
/// any request goes out; the rest reach the server as usual.
#[tokio::test]
async fn test_add_columns_scopes_to_the_columns_a_binding_uses() {
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_response(&fixture_bound_schema()))
            .unwrap(),
        "/v1/table/my_table/add_columns/" => http::Response::builder()
            .status(200)
            .body(r#"{"version":10}"#.to_string())
            .unwrap(),
        path => panic!("Unexpected path: {path}"),
    });
    table
        .add_columns()
        .computed("doubled", "spare * 2")
        .execute()
        .await
        .unwrap();
    table
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            "eager".into(),
            "spare + 1".into(),
        )]))
        .execute()
        .await
        .unwrap();

    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_response(&fixture_bound_schema()))
            .unwrap(),
        path => panic!("mutation request must not be sent: {path}"),
    });
    for name in ["title", "search_text"] {
        let err = table
            .add_columns()
            .computed(name, "1")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");
        let err = table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                name.into(),
                "1".into(),
            )]))
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "{err:?}");
    }
}

#[tokio::test]
async fn test_add_function_column_allows_an_existing_binding() {
    let schema = fixture_bound_schema();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(describe_response(&schema))
            .unwrap(),
        "/v1/table/my_table/add_columns/" => {
            let actual: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(
                actual["new_columns"],
                serde_json::json!([
                    {"name":"secondary_text","all_null":true},
                    {"name":"secondary_token_count","all_null":true}
                ])
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version":10}"#.to_string())
                .unwrap()
        }
        path => panic!("Unexpected path: {path}"),
    });
    let application = crate::function::FunctionApplication::from_json(
        r#"{
                "function":{"name":"text_features","version":"1","object_id":"fixture","location":"memory:///fixture","manifest_digest":"sha256:7e22f815b6648e14f093a3979a8e5a2082fa773ebe1ec84b135cae7e84d6f8e6"},
                "inputs":[
                    {"parameter":"title","kind":"column","value":{"path":"title"}},
                    {"parameter":"body","kind":"column","value":{"path":"body"}}
                ],
                "output":{"kind":"named_struct","fields":[
                    {"name":"normalized_text","arrow_type":"utf8","nullable":false},
                    {"name":"token_count","arrow_type":"int64","nullable":false}
                ]},
                "columns":{
                    "normalized_text":"secondary_text",
                    "token_count":"secondary_token_count"
                }
            }"#,
    )
    .unwrap();

    let result = table
        .add_columns()
        .function(application)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 10);
}

#[tokio::test]
async fn test_add_fixed_size_list_function_column_declares_the_vector_type() {
    let table = Table::new_with_handler("my_table", |request| {
        match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(
                r#"{"version":1,"schema":{"fields":[{"name":"description","nullable":true,"type":{"type":"string"}}]}}"#,
            )
            .unwrap(),
        "/v1/table/my_table/add_columns/" => {
            let actual: serde_json::Value = serde_json::from_slice(
                request.body().unwrap().as_bytes().unwrap(),
            )
            .unwrap();
            let expected: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../tests/fixtures/first_class_functions/v1/remote_fixed_size_declaration_request.json"
            ))
            .unwrap();
            assert_eq!(actual, expected);
            http::Response::builder()
                .status(200)
                .body(r#"{"version":8}"#)
                .unwrap()
        }
        path => panic!("Unexpected path: {path}"),
    }
    });
    let application = crate::function::FunctionApplication::from_json(
        r#"{
                "function":{"name":"embed","version":"1","object_id":"fixture","location":"memory:///fixture","manifest_digest":"sha256:7e22f815b6648e14f093a3979a8e5a2082fa773ebe1ec84b135cae7e84d6f8e6"},
                "inputs":[{"parameter":"text","kind":"column","value":{"path":"description"}}],
                "output":{"kind":"scalar","arrow_type":"fixed_size_list<float32, 3>","nullable":false}
            }"#,
    )
    .unwrap();

    let result = table
        .add_columns()
        .function_as("embedding", application)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 8);
}

#[tokio::test]
async fn test_add_named_struct_function_expands_one_atomic_binding() {
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(
                r#"{"version":1,"schema":{"fields":[
                        {"name":"title","nullable":true,"type":{"type":"string"}},
                        {"name":"body","nullable":true,"type":{"type":"string"}}
                    ]}}"#,
            )
            .unwrap(),
        "/v1/table/my_table/add_columns/" => {
            let actual: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            let expected: serde_json::Value = serde_json::from_str(include_str!(
                "../../../../tests/fixtures/first_class_functions/v1/remote_multi_output_declaration_request.json"
            ))
            .unwrap();
            assert_eq!(actual, expected);
            http::Response::builder()
                .status(200)
                .body(r#"{"version":9}"#)
                .unwrap()
        }
        path => panic!("Unexpected path: {path}"),
    });
    let application = crate::function::FunctionApplication::from_json(
        r#"{
                "function":{"name":"text_features","version":"1","object_id":"fixture","location":"memory:///fixture","manifest_digest":"sha256:7e22f815b6648e14f093a3979a8e5a2082fa773ebe1ec84b135cae7e84d6f8e6"},
                "inputs":[
                    {"parameter":"title","kind":"column","value":{"path":"title"}},
                    {"parameter":"body","kind":"column","value":{"path":"body"}}
                ],
                "output":{"kind":"named_struct","fields":[
                    {"name":"normalized_text","arrow_type":"utf8","nullable":false},
                    {"name":"token_count","arrow_type":"int64","nullable":false}
                ]},
                "columns":{"normalized_text":"search_text"}
            }"#,
    )
    .unwrap();

    let result = table
        .add_columns()
        .function(application)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 9);
}

#[tokio::test]
async fn test_add_columns_fails_closed_on_newer_function_binding_metadata() {
    let table = Table::new_with_handler("my_table", |request| {
        match request.url().path() {
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(
                r#"{"version":1,"schema":{"fields":[{"name":"x","nullable":true,"type":{"type":"int32"}}],"metadata":{"lancedb::function_bindings":"{\"version\":2,\"bindings\":[]}"}}}"#,
            )
            .unwrap(),
        path => panic!("mutation request must not be sent: {path}"),
    }
    });

    let err = table
        .add_columns()
        .computed("doubled", "x * 2")
        .execute()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotSupported { .. }));
}

/// A remote refresh is a server job: the async form returns its handle,
/// and the blocking form refuses rather than invent a fill count.
#[tokio::test]
async fn test_refresh_column_async_submits_a_backfill_job() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/backfill_column");
        let body = request.body().unwrap().as_bytes().unwrap();
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value["column"], "doubled");
        http::Response::builder()
            .status(202)
            .body(r#"{"job_id": "j-42"}"#)
            .unwrap()
    });

    let job = table.refresh_column_async("doubled").await.unwrap();
    assert_eq!(job.id(), Some("j-42"));

    let err = table.refresh_column("doubled").await.unwrap_err();
    assert!(
        matches!(&err, Error::NotSupported { message }
            if message.contains("refresh_column_async")),
        "{err:?}"
    );
}

#[rstest]
#[case(false, "Function is unavailable")]
#[case(true, "Function name was not found")]
#[tokio::test]
async fn test_function_column_operations_preserve_dependency_not_found(
    #[case] declare_column: bool,
    #[case] message: &'static str,
) {
    let schema_requests = Arc::new(AtomicUsize::new(0));
    let requests = schema_requests.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => {
            requests.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version":1,"schema":{"fields":[{"name":"description","nullable":true,"type":{"type":"string"}}]}}"#.to_string(),
                    )
                    .unwrap()
        }
        "/v1/table/my_table/backfill_column" | "/v1/table/my_table/add_columns/" => {
            http::Response::builder()
                .status(404)
                .body(json!({"code": 4, "error": message}).to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });
    table.schema().await.unwrap();
    assert_eq!(schema_requests.load(Ordering::SeqCst), 1);

    let error = if declare_column {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/first_class_functions/v1/remote_fixed_size_declaration_request.json"
        ))
        .unwrap();
        let application = crate::function::FunctionApplication::from_json(
            &fixture["function"]["application"].to_string(),
        )
        .unwrap();
        table
            .add_columns()
            .function_as("embedding", application)
            .execute()
            .await
            .unwrap_err()
    } else {
        table.refresh_column_async("embedding").await.unwrap_err()
    };
    let Error::Http {
        source,
        status_code,
        request_id,
    } = error
    else {
        panic!("dependency 404 was misclassified: {error:?}");
    };
    assert_eq!(status_code, Some(StatusCode::NOT_FOUND));
    assert!(source.to_string().contains(message));
    assert!(!request_id.is_empty());

    table.schema().await.unwrap();
    assert_eq!(schema_requests.load(Ordering::SeqCst), 2);
}

/// The error listing is table-addressed with optional job and column
/// filters, mirroring the server's SQL surface, and the two non-record
/// signals come back as their own fields rather than as rows.
#[tokio::test]
async fn test_function_errors_lists_the_rows_a_refresh_skipped() {
    use crate::function::{FunctionErrorFragment, FunctionErrorRecord, FunctionErrorsRequest};

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/errors");
        let body = request.body().unwrap().as_bytes().unwrap();
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"job_id": "j-7", "column": "embedding", "limit": 2})
        );
        http::Response::builder()
            .status(200)
            .body(
                r#"{"records": [{"job_id": "j-7", "fragment_id": 3, "row_offset": 9,
                        "column": "embedding", "function": "embed", "function_version": "2",
                        "table_version": 11, "error_type": "ValueError",
                        "error_message": "bad input 'x'", "created_at_millis": 1700000000000}],
                        "fragments": [{"job_id": "j-7", "fragment_id": 4, "rows_skipped": 500,
                        "rows_recorded": 100}],
                        "truncated": true}"#,
            )
            .unwrap()
    });

    let errors = table
        .function_errors(
            FunctionErrorsRequest::new()
                .job_id("j-7")
                .column("embedding")
                .limit(2),
        )
        .await
        .unwrap();
    assert_eq!(
        errors.records,
        [FunctionErrorRecord {
            job_id: "j-7".into(),
            fragment_id: 3,
            row_offset: Some(9),
            column: "embedding".into(),
            function: "embed".into(),
            function_version: "2".into(),
            table_version: 11,
            error_type: "ValueError".into(),
            error_message: "bad input 'x'".into(),
            created_at_millis: 1_700_000_000_000,
        }]
    );
    assert_eq!(
        errors.fragments,
        [FunctionErrorFragment {
            job_id: "j-7".into(),
            fragment_id: 4,
            rows_skipped: 500,
            rows_recorded: 100,
        }]
    );
    assert!(errors.truncated);

    // No filter sends no filter, and an empty listing reads as such.
    let table = Table::new_with_handler("my_table", |request| {
        let body = request.body().unwrap().as_bytes().unwrap();
        let value: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(value, serde_json::json!({}));
        http::Response::builder()
            .status(200)
            .body(r#"{"records": []}"#)
            .unwrap()
    });
    let errors = table
        .function_errors(FunctionErrorsRequest::new())
        .await
        .unwrap();
    assert_eq!(errors, crate::function::FunctionErrors::default());
}

/// The refresh handle is wrapped for read-freshness tracking, so it has to
/// forward the detail APIs too -- this is the job an operator is holding
/// when a backfill goes quiet.
#[tokio::test]
async fn test_refresh_job_handle_reports_detail_and_events() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "state",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::StringArray::from(vec![
            "claim_complete",
        ]))],
    )
    .unwrap();
    let mut events = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut events, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let table = Table::new_with_handler("my_table", move |request| {
        match request.url().path() {
            "/v1/table/my_table/backfill_column" => http::Response::builder()
                .status(202)
                .body(br#"{"job_id": "j-42"}"#.to_vec())
                .unwrap(),
            "/v1/jobs/describe" => http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id": "j-42", "job_type": "refresh_column", "job_state": "IN_PROGRESS", "creation_ms": 7, "spec": {"column": "doubled"}}"#
                        .as_bytes()
                        .to_vec(),
                )
                .unwrap(),
            "/v1/jobs/query_events" => {
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                        .unwrap();
                assert_eq!(body["job_id"], "j-42");
                http::Response::builder()
                    .status(200)
                    .body(events.clone())
                    .unwrap()
            }
            other => panic!("unexpected path {other}"),
        }
    });

    let job = table.refresh_column_async("doubled").await.unwrap();
    job.refresh().await.unwrap();
    assert_eq!(job.state().as_deref(), Some("running"));
    assert_eq!(job.job_type().as_deref(), Some("refresh_column"));
    assert_eq!(job.creation_ms(), Some(7));
    assert_eq!(job.spec().unwrap()["column"], "doubled");

    let batches = job
        .events(crate::job::JobEventsRequest::default())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_refresh_submission_uses_add_columns_version_fence() {
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/describe/" => simple_describe_response(),
        "/v1/table/my_table/add_columns/" => http::Response::builder()
            .status(200)
            .body(r#"{"version": 7}"#.to_string())
            .unwrap(),
        "/v1/table/my_table/backfill_column" => {
            let min_version = request
                .headers()
                .get("x-lancedb-min-version")
                .and_then(|value| value.to_str().ok());
            if min_version != Some("7") {
                return http::Response::builder()
                    .status(400)
                    .body(r#"{"error":"Column not found: doubled"}"#.to_string())
                    .unwrap();
            }
            http::Response::builder()
                .status(202)
                .body(r#"{"job_id": "j-43"}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });

    let result = table
        .add_columns()
        .computed("doubled", "a * 2")
        .execute()
        .await
        .unwrap();
    assert_eq!(result.version, 7);

    let job = table.refresh_column_async("doubled").await.unwrap();
    assert_eq!(job.id(), Some("j-43"));
}

/// The gate's reproducer: after a successful wait, a same-handle read
/// must carry the exact published version so a stale server cache cannot
/// serve the pre-backfill snapshot.
#[tokio::test]
async fn test_backfill_wait_establishes_read_freshness() {
    let saw_published_version = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw = saw_published_version.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/backfill_column" => http::Response::builder()
            .status(202)
            .body(r#"{"job_id": "j-7"}"#.to_string())
            .unwrap(),
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(refresh_done("j-7"))
            .unwrap(),
        "/v1/table/my_table/count_rows/" => {
            saw.store(
                request
                    .headers()
                    .get("x-lancedb-min-read-version")
                    .and_then(|value| value.to_str().ok())
                    == Some("8"),
                std::sync::atomic::Ordering::SeqCst,
            );
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });

    let job = table.refresh_column_async("doubled").await.unwrap();
    let result = job.wait().await.unwrap();
    assert_eq!(result.rows_assigned, 12);
    assert_eq!(result.published_version, Some(8));
    table.count_rows(None).await.unwrap();
    assert!(
        saw_published_version.load(std::sync::atomic::Ordering::SeqCst),
        "read after wait did not carry the published version"
    );
}

/// A checkout after submission wins over the completion fence: the
/// pinned view must not regain a timestamp floor from the job.
#[tokio::test]
async fn test_checkout_after_submit_beats_the_completion_fence() {
    let saw_min_timestamp = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw = saw_min_timestamp.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/backfill_column" => http::Response::builder()
            .status(202)
            .body(r#"{"job_id": "j-8"}"#.to_string())
            .unwrap(),
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(refresh_done("j-8"))
            .unwrap(),
        "/v1/table/my_table/describe/" => {
            let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
            http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap()
        }
        "/v1/table/my_table/count_rows/" => {
            saw.store(
                request.headers().contains_key("x-lancedb-min-timestamp"),
                std::sync::atomic::Ordering::SeqCst,
            );
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });

    let job = table.refresh_column_async("doubled").await.unwrap();
    table.checkout(3).await.unwrap();
    job.wait().await.unwrap();
    table.count_rows(None).await.unwrap();
    assert!(
        !saw_min_timestamp.load(std::sync::atomic::Ordering::SeqCst),
        "completion fence overrode an explicit checkout"
    );
}

/// Tag checkout resets freshness state wholesale; the fence must not
/// survive it.
#[tokio::test]
async fn test_tag_checkout_after_submit_beats_the_completion_fence() {
    let saw_min_timestamp = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw = saw_min_timestamp.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/backfill_column" => http::Response::builder()
            .status(202)
            .body(r#"{"job_id": "j-9"}"#.to_string())
            .unwrap(),
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(refresh_done("j-9"))
            .unwrap(),
        "/v1/table/my_table/tags/version/" => http::Response::builder()
            .status(200)
            .body(r#"{"version": 5}"#.to_string())
            .unwrap(),
        "/v1/table/my_table/describe/" => {
            let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
            http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap()
        }
        "/v1/table/my_table/count_rows/" => {
            saw.store(
                request.headers().contains_key("x-lancedb-min-timestamp"),
                std::sync::atomic::Ordering::SeqCst,
            );
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });

    let job = table.refresh_column_async("doubled").await.unwrap();
    table.checkout_tag("v1").await.unwrap();
    job.wait().await.unwrap();
    table.count_rows(None).await.unwrap();
    assert!(
        !saw_min_timestamp.load(std::sync::atomic::Ordering::SeqCst),
        "completion fence overrode a tag checkout"
    );
}

/// A checkout landing while the submission request is in flight advances
/// the epoch past the token captured at submit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkout_during_submission_beats_the_completion_fence() {
    let saw_min_timestamp = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw = saw_min_timestamp.clone();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel::<()>();
    let arrived_tx = Arc::new(std::sync::Mutex::new(arrived_tx));
    let table = Table::new_with_handler("my_table", move |request| {
        match request.url().path() {
            "/v1/table/my_table/backfill_column" => {
                // Signal arrival, then hold the response until the
                // test's checkout completes.
                arrived_tx.lock().unwrap().send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                http::Response::builder()
                    .status(202)
                    .body(r#"{"job_id": "j-10"}"#.to_string())
                    .unwrap()
            }
            "/v1/jobs/describe" => http::Response::builder()
                .status(200)
                .body(refresh_done("j-10"))
                .unwrap(),
            "/v1/table/my_table/describe/" => {
                let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
                http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap()
            }
            "/v1/table/my_table/count_rows/" => {
                saw.store(
                    request.headers().contains_key("x-lancedb-min-timestamp"),
                    std::sync::atomic::Ordering::SeqCst,
                );
                http::Response::builder()
                    .status(200)
                    .body("1".to_string())
                    .unwrap()
            }
            path => panic!("unexpected request: {path}"),
        }
    });

    let submit = tokio::spawn({
        let table = table.clone();
        async move { table.refresh_column_async("doubled").await }
    });
    tokio::task::spawn_blocking(move || {
        arrived_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
    })
    .await
    .unwrap();
    table.checkout(7).await.unwrap();
    release_tx.send(()).unwrap();

    let job = submit.await.unwrap().unwrap();
    job.wait().await.unwrap();
    table.count_rows(None).await.unwrap();
    assert!(
        !saw_min_timestamp.load(std::sync::atomic::Ordering::SeqCst),
        "completion fence overrode a checkout that landed mid-submission"
    );
}

/// checkout_latest keeps the handle on latest, so a completed backfill
/// must retain the checkout timestamp and add its exact published version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkout_latest_during_submission_keeps_the_fence() {
    let seen_headers = Arc::new(std::sync::Mutex::new(None::<http::HeaderMap>));
    let saw = seen_headers.clone();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel::<()>();
    let arrived_tx = Arc::new(std::sync::Mutex::new(arrived_tx));
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/backfill_column" => {
            arrived_tx.lock().unwrap().send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            http::Response::builder()
                .status(202)
                .body(r#"{"job_id": "j-11"}"#.to_string())
                .unwrap()
        }
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(refresh_done("j-11"))
            .unwrap(),
        "/v1/table/my_table/count_rows/" => {
            *saw.lock().unwrap() = Some(request.headers().clone());
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected request: {path}"),
    });

    let submit = tokio::spawn({
        let table = table.clone();
        async move { table.refresh_column_async("doubled").await }
    });
    tokio::task::spawn_blocking(move || {
        arrived_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
    })
    .await
    .unwrap();
    table.checkout_latest().await.unwrap();
    release_tx.send(()).unwrap();

    let job = submit.await.unwrap().unwrap();
    job.wait().await.unwrap();
    table.count_rows(None).await.unwrap();
    let headers = seen_headers.lock().unwrap().clone().expect("no request");
    assert!(headers.contains_key("x-lancedb-min-timestamp"));
    assert_eq!(
        headers
            .get("x-lancedb-min-read-version")
            .and_then(|value| value.to_str().ok()),
        Some("8")
    );
}
