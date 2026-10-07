// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[rstest]
#[case(DEFAULT_SERVER_VERSION.clone())]
#[case(semver::Version::new(0, 2, 0))]
#[tokio::test]
async fn test_query_vector_non_finite(#[case] version: semver::Version) {
    let table =
        Table::new_with_handler_version("my_table", version, |_| -> http::Response<String> {
            panic!("non-finite vectors must be rejected before sending a request")
        });

    for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        // Requests built without the query builder must also return an
        // error, including a non-finite vector later in a batch.
        for batched in [false, true] {
            let mut request = table
                .query()
                .nearest_to(&[0.1, 0.2])
                .unwrap()
                .into_request();
            if !batched {
                request.query_vector.clear();
            }
            request
                .query_vector
                .push(Arc::new(arrow_array::Float32Array::from(vec![0.1, value])));
            let result = table
                .base_table()
                .query(
                    &AnyQuery::VectorQuery(request),
                    QueryExecutionOptions::default(),
                )
                .await;
            let Err(err) = result else {
                panic!("non-finite query vector unexpectedly succeeded")
            };
            assert!(matches!(err, Error::InvalidInput { .. }));
            assert!(err.to_string().contains("only finite values"));
        }
    }
}

#[tokio::test]
async fn test_query_vector_default_values() {
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
        let mut expected_body = serde_json::json!({
            "prefilter": true,
            "nprobes": 20,
            "minimum_nprobes": 20,
            "maximum_nprobes": 20,
            "lower_bound": Option::<f32>::None,
            "upper_bound": Option::<f32>::None,
            "k": 10,
            "ef": Option::<usize>::None,
            "refine_factor": null,
            "version": null,
        });
        // Pass vector separately to make sure it matches f32 precision.
        expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
        assert_eq!(body, expected_body);

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let data = table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .execute()
        .await;
    let data = data.unwrap().collect::<Vec<_>>().await;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].as_ref().unwrap(), &expected_data);
}

#[tokio::test]
async fn test_query_vector_approx_mode_sent_when_set() {
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
        let mut expected_body = serde_json::json!({
            "prefilter": true,
            "nprobes": 20,
            "minimum_nprobes": 20,
            "maximum_nprobes": 20,
            "approx_mode": "accurate",
            "lower_bound": Option::<f32>::None,
            "upper_bound": Option::<f32>::None,
            "k": 10,
            "ef": Option::<usize>::None,
            "refine_factor": null,
            "version": null,
        });
        expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
        assert_eq!(body, expected_body);

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let data = table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .approx_mode(crate::ApproxMode::Accurate)
        .execute()
        .await;
    let data = data.unwrap().collect::<Vec<_>>().await;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].as_ref().unwrap(), &expected_data);
}

#[tokio::test]
async fn test_query_fts_default_values() {
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
            "full_text_query": {
                "columns": [],
                "query": "test",
            },
            "prefilter": true,
            "version": null,
            "k": 10,
            "vector": [],
        });
        assert_eq!(body, expected_body);

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let data = table
        .query()
        .full_text_search(FullTextSearchQuery::new("test".to_owned()))
        .execute()
        .await;
    let data = data.unwrap().collect::<Vec<_>>().await;
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].as_ref().unwrap(), &expected_data);
}

#[tokio::test]
async fn test_query_vector_nprobes_zero() {
    let table = Table::new_with_handler::<&str>("my_table", |_| {
        panic!("invalid nprobes must be rejected before sending a request")
    });
    let result = table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .nprobes(0);
    assert!(matches!(
        result,
        Err(Error::InvalidInput { message }) if message == "nprobes must be greater than 0"
    ));
}

#[tokio::test]
async fn test_query_vector_all_params() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        let mut expected_body = serde_json::json!({
            "vector_column": "my_vector",
            "prefilter": false,
            "k": 42,
            "offset": 10,
            "distance_type": "cosine",
            "bypass_vector_index": true,
            "columns": ["a", "b"],
            "order_by": [
                {
                    "column_name": "score",
                    "ascending": false,
                    "nulls_first": true,
                },
                {
                    "column_name": "id",
                    "ascending": true,
                    "nulls_first": false,
                }
            ],
            "nprobes": 12,
            "minimum_nprobes": 12,
            "maximum_nprobes": 12,
            "lower_bound": Option::<f32>::None,
            "upper_bound": Option::<f32>::None,
            "ef": Option::<usize>::None,
            "refine_factor": 2,
            "version": null,
        });
        // Pass vector separately to make sure it matches f32 precision.
        expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
        assert_eq!(body, expected_body);

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let response_body = write_ipc_file(&data);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let _ = table
        .query()
        .limit(42)
        .offset(10)
        .select(Select::columns(&["a", "b"]))
        .order_by(Some(vec![
            ColumnOrdering::desc_nulls_first("score".to_string()),
            ColumnOrdering::asc_nulls_last("id".to_string()),
        ]))
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .column("my_vector")
        .postfilter()
        .distance_type(crate::DistanceType::Cosine)
        .nprobes(12)
        .unwrap()
        .refine_factor(2)
        .bypass_vector_index()
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_vector_nested_field_path() {
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
        let mut expected_body = serde_json::json!({
            "vector_column": "image.embedding",
            "prefilter": true,
            "k": 10,
            "nprobes": 20,
            "minimum_nprobes": 20,
            "maximum_nprobes": 20,
            "lower_bound": Option::<f32>::None,
            "upper_bound": Option::<f32>::None,
            "ef": Option::<usize>::None,
            "refine_factor": Option::<u32>::None,
            "version": null,
        });
        expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
        assert_eq!(body, expected_body);

        let response_body = write_ipc_file(&expected_data_ref);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let _ = table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .column("image.embedding")
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_fts() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        let expected_body = serde_json::json!({
            "full_text_query": {
                "columns": ["a", "b"],
                "query": "hello world",
            },
            "k": 10,
            "vector": [],
            "with_row_id": true,
            "prefilter": true,
            "version": null
        });
        let expected_body_2 = serde_json::json!({
            "full_text_query": {
                "columns": ["b","a"],
                "query": "hello world",
            },
            "k": 10,
            "vector": [],
            "with_row_id": true,
            "prefilter": true,
            "version": null
        });
        assert!(body == expected_body || body == expected_body_2);

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let response_body = write_ipc_file(&data);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let _ = table
        .query()
        .full_text_search(
            FullTextSearchQuery::new("hello world".into())
                .with_columns(&["a".into(), "b".into()])
                .unwrap(),
        )
        .with_row_id()
        .limit(10)
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_analyze_plan_distributed_metrics_query_param() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/analyze_plan/");
        assert_eq!(
            request
                .url()
                .query_pairs()
                .find(|(k, _)| k == "distributed_metrics"),
            Some(("distributed_metrics".into(), "per_worker".into()))
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["k"], serde_json::json!(1));

        http::Response::builder()
            .status(200)
            .body(r#""analyzed plan""#)
            .unwrap()
    });

    let result = table
        .query()
        .limit(1)
        .analyze_plan_with_options(QueryExecutionOptions {
            analyze_plan_distributed_metrics: AnalyzePlanDistributedMetrics::PerWorker,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(result, "analyzed plan");
}

#[tokio::test]
async fn test_take_offsets_explain_plan_does_not_execute_query() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/explain_plan/");

        http::Response::builder()
            .status(200)
            .body(r#""RemoteLookupExec""#)
            .unwrap()
    });

    let explained = table
        .take_offsets(vec![0, 1, 0, 2])
        .select(crate::query::Select::columns(&["id"]))
        .limit(3)
        .explain_plan(false)
        .await
        .unwrap();

    assert!(explained.contains("GlobalLimitExec"));
    assert!(explained.contains("TakeRestoreExec"));
    assert!(!explained.contains("CoalescePartitionsExec"));
    assert!(explained.contains("RemoteLookupExec"));
}

#[tokio::test]
async fn test_converted_take_request_restores_remote_occurrences() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");

        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body["columns"], json!(["id", "_rowoffset"]));

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("_rowoffset", DataType::UInt64, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![5])),
                Arc::new(arrow_array::UInt64Array::from(vec![5])),
            ],
        )
        .unwrap();
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(write_ipc_file(&data))
            .unwrap()
    });

    let request = table
        .take_offsets(vec![5, 5])
        .select(crate::query::Select::columns(&["id"]))
        .into_request();
    let batches = table
        .base_table()
        .query(&AnyQuery::Query(request), QueryExecutionOptions::default())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema().fields().len() == 1)
    );
}

#[tokio::test]
async fn test_take_offsets_analyze_plan_delegates_to_remote() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/analyze_plan/");
        assert_eq!(
            request
                .url()
                .query_pairs()
                .find(|(key, _)| key == "distributed_metrics"),
            Some(("distributed_metrics".into(), "per_worker".into()))
        );

        http::Response::builder()
            .status(200)
            .body(r#""Remote analyzed plan: worker metrics""#)
            .unwrap()
    });

    let analyzed = table
        .take_offsets(vec![0, 1, 0, 2])
        .select(crate::query::Select::columns(&["id"]))
        .limit(3)
        .analyze_plan_with_options(QueryExecutionOptions {
            analyze_plan_distributed_metrics: AnalyzePlanDistributedMetrics::PerWorker,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(analyzed, "Remote analyzed plan: worker metrics");
}

#[tokio::test]
async fn test_query_structured_fts() {
    let table =
        Table::new_with_handler_version("my_table", semver::Version::new(0, 6, 0), |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "full_text_query": {
                    "query": {
                        "match": {
                            "terms": "hello world",
                            "column": "payload.text",
                            "boost": 1.0,
                            "fuzziness": 0,
                            "max_expansions": 50,
                            "operator": "Or",
                            "prefix_length": 0,
                            "document_granularity": "list_element",
                        },
                    }
                },
                "k": 10,
                "vector": [],
                "with_row_id": true,
                "prefilter": true,
                "version": null
            });
            assert_eq!(body, expected_body);

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

    let _ = table
        .query()
        .full_text_search(FullTextSearchQuery::new_query(
            MatchQuery::new("hello world".to_owned())
                .with_column(Some("payload.text".to_owned()))
                .with_document_granularity(DocumentGranularity::ListElement)
                .into(),
        ))
        .with_row_id()
        .limit(10)
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_combined_fields_uses_structured_fts() {
    use lance_index::scalar::inverted::query::CombinedFieldsQuery;

    let table =
        Table::new_with_handler_version("my_table", semver::Version::new(0, 3, 0), |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                body["full_text_query"]["query"],
                serde_json::json!({
                    "combined_fields": {
                        "query": "hello world",
                        "columns": ["title", "text"],
                        "boost": [1.0, 1.0],
                        "operator": "Or"
                    }
                })
            );

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap();
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(write_ipc_file(&data))
                .unwrap()
        });

    table
        .query()
        .full_text_search(FullTextSearchQuery::new_query(
            CombinedFieldsQuery::try_new("hello world".into(), vec!["title".into(), "text".into()])
                .unwrap()
                .into(),
        ))
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_query_row_document_granularity_uses_structured_fts() {
    let table =
        Table::new_with_handler_version("my_table", semver::Version::new(0, 3, 0), |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(
                body["full_text_query"]["query"]["match"]["document_granularity"],
                "row"
            );

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1]))],
            )
            .unwrap();
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(write_ipc_file(&data))
                .unwrap()
        });

    table
        .query()
        .full_text_search(FullTextSearchQuery::new_query(
            MatchQuery::new("hello world".to_owned())
                .with_column(Some("payload.text".to_owned()))
                .with_document_granularity(DocumentGranularity::Row)
                .into(),
        ))
        .execute()
        .await
        .unwrap();
}

#[rstest]
#[case(DEFAULT_SERVER_VERSION.clone())]
#[case(semver::Version::new(0, 3, 0))]
#[case(semver::Version::new(0, 5, 0))]
#[tokio::test]
async fn test_query_document_granularity_requires_server_support(#[case] version: semver::Version) {
    let table =
        Table::new_with_handler_version("my_table", version, |_| -> http::Response<String> {
            panic!("unsupported remote query must fail before sending a request")
        });

    let result = table
        .query()
        .full_text_search(FullTextSearchQuery::new_query(
            MatchQuery::new("hello world".to_owned())
                .with_column(Some("payload.text".to_owned()))
                .with_document_granularity(DocumentGranularity::ListElement)
                .into(),
        ))
        .execute()
        .await;
    let Err(err) = result else {
        panic!("legacy remote query unexpectedly succeeded")
    };

    assert!(
        err.to_string()
            .contains("document granularity requires remote server version 0.6.0 or later")
    );
}

#[rstest]
#[case(DEFAULT_SERVER_VERSION.clone())]
#[case(semver::Version::new(0, 2, 0))]
#[tokio::test]
async fn test_batch_queries(#[case] version: semver::Version) {
    let table = Table::new_with_handler_version("my_table", version.clone(), move |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/query/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        let query_vectors = body["vector"].as_array().unwrap();
        let version = ServerVersion(version.clone());
        let data = if version.support_multivector() {
            assert_eq!(query_vectors.len(), 2);
            assert_eq!(query_vectors[0].as_array().unwrap().len(), 3);
            assert_eq!(query_vectors[1].as_array().unwrap().len(), 3);
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("a", DataType::Int32, false),
                    Field::new("query_index", DataType::Int32, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                    Arc::new(Int32Array::from(vec![0, 0, 0, 1, 1, 1])),
                ],
            )
            .unwrap()
        } else {
            // it's single flat vector, so here the length is dim
            assert_eq!(query_vectors.len(), 3);
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap()
        };

        let response_body = write_ipc_file(&data);
        http::Response::builder()
            .status(200)
            .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
            .body(response_body)
            .unwrap()
    });

    let query = table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .add_query_vector(vec![0.4, 0.5, 0.6])
        .unwrap();

    let results = query
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let results = concat_batches(&results[0].schema(), &results).unwrap();

    let query_index = results["query_index"].as_primitive::<Int32Type>();
    // We don't guarantee order.
    assert!(query_index.values().contains(&0));
    assert!(query_index.values().contains(&1));
}
