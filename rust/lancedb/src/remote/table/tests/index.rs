// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_create_index() {
    let cases = [
        (
            "IVF_FLAT",
            json!({
                "metric_type": "hamming",
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfFlat(IvfFlatIndexBuilder::default().distance_type(DistanceType::Hamming)),
        ),
        (
            "IVF_FLAT",
            json!({
                "metric_type": "hamming",
                "num_partitions": 128,
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfFlat(
                IvfFlatIndexBuilder::default()
                    .distance_type(DistanceType::Hamming)
                    .num_partitions(128),
            ),
        ),
        (
            "IVF_PQ",
            json!({
                "metric_type": "l2",
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfPq(Default::default()),
        ),
        (
            "IVF_PQ",
            json!({
                "metric_type": "cosine",
                "num_partitions": 128,
                "num_bits": 4,
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfPq(
                IvfPqIndexBuilder::default()
                    .distance_type(DistanceType::Cosine)
                    .num_partitions(128)
                    .num_bits(4),
            ),
        ),
        (
            "IVF_PQ",
            json!({
                "metric_type": "l2",
                "num_sub_vectors": 16,
                "sample_rate": 512,
                "max_iterations": 100,
            }),
            Index::IvfPq(
                IvfPqIndexBuilder::default()
                    .num_sub_vectors(16)
                    .sample_rate(512)
                    .max_iterations(100),
            ),
        ),
        (
            "IVF_HNSW_SQ",
            json!({
                "metric_type": "l2",
                "sample_rate": 256,
                "max_iterations": 50,
                "m": 20,
                "ef_construction": 300,
            }),
            Index::IvfHnswSq(Default::default()),
        ),
        (
            "IVF_HNSW_SQ",
            json!({
                "metric_type": "l2",
                "num_partitions": 128,
                "sample_rate": 256,
                "max_iterations": 50,
                "m": 40,
                "ef_construction": 500,
            }),
            Index::IvfHnswSq(
                IvfHnswSqIndexBuilder::default()
                    .distance_type(DistanceType::L2)
                    .num_partitions(128)
                    .num_edges(40)
                    .ef_construction(500),
            ),
        ),
        (
            "IVF_HNSW_FLAT",
            json!({
                "metric_type": "l2",
                "sample_rate": 256,
                "max_iterations": 50,
                "m": 20,
                "ef_construction": 300,
            }),
            Index::IvfHnswFlat(Default::default()),
        ),
        (
            "IVF_HNSW_FLAT",
            json!({
                "metric_type": "cosine",
                "num_partitions": 64,
                "sample_rate": 256,
                "max_iterations": 50,
                "m": 40,
                "ef_construction": 500,
            }),
            Index::IvfHnswFlat(
                IvfHnswFlatIndexBuilder::default()
                    .distance_type(DistanceType::Cosine)
                    .num_partitions(64)
                    .num_edges(40)
                    .ef_construction(500),
            ),
        ),
        (
            "IVF_SQ",
            json!({
                "metric_type": "l2",
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfSq(Default::default()),
        ),
        (
            "IVF_SQ",
            json!({
                "metric_type": "cosine",
                "num_partitions": 64,
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfSq(
                IvfSqIndexBuilder::default()
                    .distance_type(DistanceType::Cosine)
                    .num_partitions(64),
            ),
        ),
        (
            "IVF_RQ",
            json!({
                "metric_type": "l2",
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfRq(Default::default()),
        ),
        (
            "IVF_RQ",
            json!({
                "metric_type": "cosine",
                "num_partitions": 64,
                "num_bits": 8,
                "sample_rate": 256,
                "max_iterations": 50,
            }),
            Index::IvfRq(
                IvfRqIndexBuilder::default()
                    .distance_type(DistanceType::Cosine)
                    .num_partitions(64)
                    .num_bits(8),
            ),
        ),
        // HNSW_PQ isn't yet supported on SaaS
        ("BTREE", json!({}), Index::BTree(Default::default())),
        ("BITMAP", json!({}), Index::Bitmap(Default::default())),
        ("ZONEMAP", json!({}), Index::ZoneMap(Default::default())),
        ("NGRAM", json!({}), Index::NGram(Default::default())),
        (
            "BLOOM_FILTER",
            json!({}),
            Index::BloomFilter(Default::default()),
        ),
        ("RTREE", json!({}), Index::RTree(Default::default())),
        (
            "BLOOM_FILTER",
            json!({"number_of_items": 4096, "probability": 0.01}),
            Index::BloomFilter(
                crate::index::scalar::BloomFilterIndexBuilder::default()
                    .number_of_items(4096)
                    .unwrap()
                    .probability(0.01)
                    .unwrap(),
            ),
        ),
        (
            "RTREE",
            json!({"page_size": 1024}),
            Index::RTree(
                crate::index::scalar::RTreeIndexBuilder::default()
                    .page_size(1024)
                    .unwrap(),
            ),
        ),
        (
            "LABEL_LIST",
            json!({}),
            Index::LabelList(Default::default()),
        ),
        (
            "FTS",
            serde_json::to_value(InvertedIndexParams::default()).unwrap(),
            Index::FTS(Default::default()),
        ),
        (
            "FTS",
            {
                let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
                body["block_size"] = 256.into();
                body
            },
            Index::FTS(InvertedIndexParams::default().block_size(256).unwrap()),
        ),
        (
            "FTS",
            {
                let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
                body["custom_stop_words"] = json!(["cat", " cat ", "CAT"]);
                body
            },
            Index::FTS(InvertedIndexParams::default().custom_stop_words(Some(vec![
                "cat".to_string(),
                " cat ".to_string(),
                "CAT".to_string(),
            ]))),
        ),
        (
            "FTS",
            {
                let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
                body["document_granularity"] = "list_element".into();
                body
            },
            Index::FTS(
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            ),
        ),
    ];

    for (index_type, expected_body, index) in cases {
        let table = Table::new_with_handler_version(
            "my_table",
            semver::Version::new(0, 6, 0),
            move |request| {
                assert_eq!(request.method(), "POST");
                match request.url().path() {
                    "/v1/table/my_table/describe/" => {
                        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
                        http::Response::builder()
                            .status(200)
                            .body(describe_response(&schema))
                            .unwrap()
                    }
                    "/v1/table/my_table/create_index/" => {
                        assert_eq!(
                            request.headers().get("Content-Type").unwrap(),
                            JSON_CONTENT_TYPE
                        );
                        let body = request.body().unwrap().as_bytes().unwrap();
                        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                        let mut expected_body = expected_body.clone();
                        expected_body["column"] = "a".into();
                        expected_body[INDEX_TYPE_KEY] = index_type.into();

                        assert_eq!(body, expected_body);

                        http::Response::builder()
                            .status(200)
                            .body("{}".to_string())
                            .unwrap()
                    }
                    path => panic!("Unexpected path: {}", path),
                }
            },
        );

        table.create_index(&["a"], index).execute().await.unwrap();
    }
}

#[tokio::test]
async fn test_create_index_forwards_replace_false_on_existing_route() {
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => {
                let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
                http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap()
            }
            "/v1/table/my_table/create_index/" => {
                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                assert_eq!(body["replace"], json!(false));

                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("Unexpected path: {}", path),
        }
    });

    table
        .create_index(&["a"], Index::BTree(Default::default()))
        .replace(false)
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_index_returns_job() {
    let describe_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let describe_calls_in_handler = describe_calls.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => {
                let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
                http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap()
            }
            "/v1/table/my_table/create_index/" => http::Response::builder()
                .status(200)
                .body(r#"{"job_id": "job-123"}"#.to_string())
                .unwrap(),
            "/v1/jobs/describe" => {
                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                assert_eq!(body["job_id"], "job-123");
                let state = if describe_calls_in_handler
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    == 0
                {
                    "IN_PROGRESS"
                } else {
                    "DONE"
                };
                http::Response::builder()
                    .status(200)
                    .body(format!(
                        r#"{{"job_id": "job-123", "job_state": "{state}"}}"#
                    ))
                    .unwrap()
            }
            "/v1/jobs/cancel" => {
                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                assert_eq!(body["job_id"], "job-123");
                http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            path => panic!("Unexpected path: {}", path),
        }
    });

    let job = table
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute_async()
        .await
        .unwrap();
    assert_eq!(job.id(), Some("job-123"));
    job.wait().await.unwrap();
    assert_eq!(describe_calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    job.cancel().await.unwrap();
}

/// An unrecognized state is treated as still running, so the client keeps
/// polling rather than reporting a wrong terminal outcome.
#[tokio::test]
async fn test_job_wait_treats_unknown_state_as_running() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_in_handler = calls.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => {
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap()
        }
        "/v1/table/my_table/create_index/" => http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-unknown"}"#.to_string())
            .unwrap(),
        "/v1/jobs/describe" => {
            let state = if calls_in_handler.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                "SOMETHING_NEW"
            } else {
                "DONE"
            };
            http::Response::builder()
                .status(200)
                .body(format!(
                    r#"{{"job_id": "job-unknown", "job_state": "{state}"}}"#
                ))
                .unwrap()
        }
        path => panic!("Unexpected path: {}", path),
    });

    let job = table
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute_async()
        .await
        .unwrap();
    job.wait().await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_job_wait_surfaces_failure() {
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => {
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap()
        }
        "/v1/table/my_table/create_index/" => http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-err"}"#.to_string())
            .unwrap(),
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(r#"{"job_id": "job-err", "job_state": "FAILED"}"#.to_string())
            .unwrap(),
        path => panic!("Unexpected path: {}", path),
    });

    let job = table
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute_async()
        .await
        .unwrap();
    let err = job.wait().await.unwrap_err();
    let crate::Error::JobFailed { failure, .. } = &err else {
        panic!("expected JobFailed, got {err:?}");
    };
    // The server said only that it failed, so nothing may be invented.
    assert!(failure.message.is_none(), "{failure:?}");
    assert!(failure.phase.is_none(), "{failure:?}");
    assert!(failure.retryable.is_none(), "{failure:?}");
    assert_eq!(err.to_string(), "Job job-err failed");
}

/// A server that reports why the job failed has that reason surfaced
/// verbatim rather than replaced with a generic message.
#[tokio::test]
async fn test_job_wait_reports_the_server_failure_reason() {
    let table = Table::new_with_handler("my_table", move |request| {
        match request.url().path() {
            "/v1/table/my_table/describe/" => {
                let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
                http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap()
            }
            "/v1/table/my_table/create_index/" => http::Response::builder()
                .status(200)
                .body(r#"{"job_id": "job-err"}"#.to_string())
                .unwrap(),
            "/v1/jobs/describe" => http::Response::builder()
                .status(200)
                .body(
                    r#"{"job_id": "job-err", "job_state": "FAILED", "failure": {"phase": "commit", "message": "preempted", "retryable": true}}"#
                        .to_string(),
                )
                .unwrap(),
            path => panic!("Unexpected path: {}", path),
        }
    });

    let job = table
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute_async()
        .await
        .unwrap();
    let err = job.wait().await.unwrap_err();
    let crate::Error::JobFailed { failure, .. } = &err else {
        panic!("expected JobFailed, got {err:?}");
    };
    assert_eq!(failure.message.as_deref(), Some("preempted"));
    assert_eq!(failure.phase.as_deref(), Some("commit"));
    assert_eq!(failure.retryable, Some(true));
    assert_eq!(err.to_string(), "Job job-err failed: preempted (in commit)");
}

/// Servers that return no job id (e.g. an empty create-index response)
/// yield an already-done job.
#[tokio::test]
async fn test_create_index_without_job_id_is_done() {
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => {
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap()
        }
        "/v1/table/my_table/create_index/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        path => panic!("Unexpected path: {}", path),
    });

    let job = table
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute_async()
        .await
        .unwrap();
    job.wait().await.unwrap();
    job.cancel().await.unwrap();
}

#[tokio::test]
async fn test_create_index_nested_field_paths() {
    let schema = nested_index_schema();
    let expected_requests = Arc::new(vec![
        json!({
            "column": "rowId",
            "index_type": "BTREE",
        }),
        json!({
            "column": "`row-id`",
            "index_type": "BTREE",
        }),
        json!({
            "column": "userId",
            "index_type": "BTREE",
        }),
        json!({
            "column": "MetaData.userId",
            "index_type": "BTREE",
        }),
        json!({
            "column": "metadata.user_id",
            "index_type": "BTREE",
        }),
        json!({
            "column": "image.embedding",
            "index_type": "IVF_PQ",
            "metric_type": "l2",
        }),
        {
            let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
            body["column"] = "payload.text".into();
            body["index_type"] = "FTS".into();
            body
        },
        {
            let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
            body["column"] = "docs.content".into();
            body["index_type"] = "FTS".into();
            body
        },
        {
            let mut body = serde_json::to_value(InvertedIndexParams::default()).unwrap();
            body["column"] = "docs.content".into();
            body["index_type"] = "FTS".into();
            body["document_granularity"] = "list_element".into();
            body
        },
        json!({
            "column": "`meta-data`.`user-id`",
            "index_type": "BTREE",
        }),
        json!({
            "column": "literal.`a.b`",
            "index_type": "BTREE",
        }),
    ]);
    let request_idx = Arc::new(AtomicUsize::new(0));
    let table = Table::new_with_handler_version("my_table", semver::Version::new(0, 6, 0), {
        let schema = schema.clone();
        let expected_requests = expected_requests.clone();
        let request_idx = request_idx.clone();
        move |request| {
            assert_eq!(request.method(), "POST");
            match request.url().path() {
                "/v1/table/my_table/describe/" => http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap(),
                "/v1/table/my_table/create_index/" => {
                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        JSON_CONTENT_TYPE
                    );
                    let idx = request_idx.fetch_add(1, Ordering::SeqCst);
                    let body = request.body().unwrap().as_bytes().unwrap();
                    let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                    assert_eq!(body, expected_requests[idx]);
                    http::Response::builder()
                        .status(200)
                        .body("{}".to_string())
                        .unwrap()
                }
                path => panic!("Unexpected path: {}", path),
            }
        }
    });

    table
        .create_index(&["rowId"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["`ROW-ID`"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["userId"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["MetaData.userId"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["Metadata.USER_ID"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["Image.Embedding"], Index::Auto)
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["Payload.Text"], Index::FTS(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["Docs.Content"], Index::FTS(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(
            &["Docs.Content"],
            Index::FTS(
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            ),
        )
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["`META-DATA`.`USER-ID`"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(&["literal.`A.B`"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();

    assert_eq!(request_idx.load(Ordering::SeqCst), expected_requests.len());
}

#[tokio::test]
async fn test_create_list_element_fts_requires_server_support() {
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 5, 0),
        |_| -> http::Response<String> {
            panic!("unsupported index creation must fail before sending a request")
        },
    );

    let result = table
        .create_index(
            &["docs.content"],
            Index::FTS(
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            ),
        )
        .execute()
        .await;

    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("document granularity requires remote server version 0.6.0 or later")
    );
}

#[tokio::test]
async fn test_list_indices() {
    let schema = Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8),
            false,
        ),
        Field::new(
            "metadata",
            DataType::Struct(vec![Field::new("my.column", DataType::Utf8, true)].into()),
            false,
        ),
    ]);
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");

        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                return http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap();
            }
            "/v1/table/my_table/index/list/" => {
                serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "vector_idx",
                            "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                            "columns": ["vector"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "my_idx",
                            "index_uuid": "34255f64-5717-4562-b3fc-2c963f66afa6",
                            "columns": ["metadata.`my.column`"],
                            "index_status": "done",
                        },
                    ]
                })
            }
            "/v1/table/my_table/index/vector_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "IVF_PQ",
                    "distance_type": "l2"
                })
            }
            "/v1/table/my_table/index/my_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "LABEL_LIST"
                })
            }
            path => panic!("Unexpected path: {}", path),
        };
        http::Response::builder()
            .status(200)
            .body(serde_json::to_string(&response_body).unwrap())
            .unwrap()
    });

    let indices = table.list_indices().await.unwrap();
    let expected = vec![
        IndexConfig {
            name: "vector_idx".into(),
            index_type: IndexType::IvfPq,
            columns: vec!["vector".into()],
            index_uuid: None,
            type_url: None,
            created_at: None,
            num_indexed_rows: None,
            num_unindexed_rows: None,
            size_bytes: None,
            num_segments: None,
            index_version: None,
            index_details: None,
        },
        IndexConfig {
            name: "my_idx".into(),
            index_type: IndexType::LabelList,
            columns: vec!["metadata.`my.column`".into()],
            index_uuid: None,
            type_url: None,
            created_at: None,
            num_indexed_rows: None,
            num_unindexed_rows: None,
            size_bytes: None,
            num_segments: None,
            index_version: None,
            index_details: None,
        },
    ];
    assert_eq!(indices, expected);
}

#[rstest]
#[case::legacy(false)]
#[case::enriched(true)]
#[tokio::test]
async fn test_list_indices_fts_public_list_path(#[case] enriched: bool) {
    let schema = nested_index_schema();
    let table = Table::new_with_handler("my_table", move |request| {
        let body = match request.url().path() {
            "/v1/table/my_table/describe/" => describe_response(&schema),
            "/v1/table/my_table/index/list/" => serde_json::json!({
                "indexes": [{
                    "index_name": "docs_idx",
                    "columns": ["docs.item.content"],
                    "index_type": enriched.then_some("FTS"),
                }],
            })
            .to_string(),
            "/v1/table/my_table/index/docs_idx/stats/" => {
                assert!(!enriched, "enriched responses must not fetch index stats");
                serde_json::json!({
                    "num_indexed_rows": 1,
                    "num_unindexed_rows": 0,
                    "index_type": "FTS",
                })
                .to_string()
            }
            path => panic!("Unexpected path: {path}"),
        };
        http::Response::builder().status(200).body(body).unwrap()
    });

    let indices = table.list_indices().await.unwrap();
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].index_type, IndexType::FTS);
    assert_eq!(indices[0].columns, vec!["docs.content"]);
}

#[tokio::test]
async fn test_list_indices_nested_field_paths() {
    let schema = nested_index_schema();
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");

        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                return http::Response::builder()
                    .status(200)
                    .body(describe_response(&schema))
                    .unwrap();
            }
            "/v1/table/my_table/index/list/" => {
                serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "row_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000001",
                            "columns": ["rowId"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "row_dash_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000002",
                            "columns": ["`ROW-ID`"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "user_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000003",
                            "columns": ["userId"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "mixed_case_metadata_user_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000004",
                            "columns": ["MetaData.userId"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "metadata_user_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000005",
                            "columns": ["Metadata.USER_ID"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "image_embedding_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000006",
                            "columns": ["Image.Embedding"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "payload_text_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000007",
                            "columns": ["Payload.Text"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "meta_data_user_id_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000008",
                            "columns": ["`META-DATA`.`USER-ID`"],
                            "index_status": "done",
                        },
                        {
                            "index_name": "literal_dot_idx",
                            "index_uuid": "00000000-0000-0000-0000-000000000009",
                            "columns": ["literal.`A.B`"],
                            "index_status": "done",
                        },
                    ]
                })
            }
            "/v1/table/my_table/index/row_id_idx/stats/"
            | "/v1/table/my_table/index/row_dash_id_idx/stats/"
            | "/v1/table/my_table/index/user_id_idx/stats/"
            | "/v1/table/my_table/index/mixed_case_metadata_user_id_idx/stats/"
            | "/v1/table/my_table/index/metadata_user_id_idx/stats/"
            | "/v1/table/my_table/index/meta_data_user_id_idx/stats/"
            | "/v1/table/my_table/index/literal_dot_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "BTREE"
                })
            }
            "/v1/table/my_table/index/image_embedding_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "IVF_PQ",
                    "distance_type": "l2"
                })
            }
            "/v1/table/my_table/index/payload_text_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "FTS"
                })
            }
            path => panic!("Unexpected path: {}", path),
        };
        http::Response::builder()
            .status(200)
            .body(serde_json::to_string(&response_body).unwrap())
            .unwrap()
    });

    let indices = table.list_indices().await.unwrap();
    // The remote path leaves the rich metadata fields None until the server
    // wires them through. See https://github.com/lancedb/lancedb/issues/3494
    let expected: Vec<IndexConfig> = [
        ("row_id_idx", IndexType::BTree, "rowId"),
        ("row_dash_id_idx", IndexType::BTree, "`row-id`"),
        ("user_id_idx", IndexType::BTree, "userId"),
        (
            "mixed_case_metadata_user_id_idx",
            IndexType::BTree,
            "MetaData.userId",
        ),
        ("metadata_user_id_idx", IndexType::BTree, "metadata.user_id"),
        ("image_embedding_idx", IndexType::IvfPq, "image.embedding"),
        ("payload_text_idx", IndexType::FTS, "payload.text"),
        (
            "meta_data_user_id_idx",
            IndexType::BTree,
            "`meta-data`.`user-id`",
        ),
        ("literal_dot_idx", IndexType::BTree, "literal.`a.b`"),
    ]
    .into_iter()
    .map(|(name, index_type, column)| IndexConfig {
        name: name.into(),
        index_type,
        columns: vec![column.into()],
        index_uuid: None,
        type_url: None,
        created_at: None,
        num_indexed_rows: None,
        num_unindexed_rows: None,
        size_bytes: None,
        num_segments: None,
        index_version: None,
        index_details: None,
    })
    .collect();
    assert_eq!(indices, expected);
}

/// Verifies that when the server returns `index_type` in the list response,
/// `list_indices` uses all enriched fields directly and does **not** make a
/// per-index `/index/{name}/stats/` call.
#[tokio::test]
async fn test_list_indices_enriched() {
    let schema = Schema::new(vec![
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8),
            false,
        ),
        Field::new("text", DataType::Utf8, false),
    ]);
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap(),
            "/v1/table/my_table/index/list/" => {
                let body = serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "vector_idx",
                            "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                            "columns": ["vector"],
                            "index_type": "IVF_PQ",
                            "index_status": "done",
                            "num_indexed_rows": 1000,
                            "num_unindexed_rows": 50,
                            "size_bytes": 204800,
                            "num_segments": 2,
                            "index_version": 1,
                            "index_details": "{\"num_partitions\":16}",
                            "created_at": "2026-06-18T21:37:36.637Z",
                            "type_url": "type.googleapis.com/lance.index.vector.IvfPq",
                        },
                        {
                            "index_name": "text_idx",
                            "index_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                            "columns": ["text"],
                            "index_type": "FTS",
                            "index_status": "done",
                            "num_indexed_rows": 1000,
                            "num_unindexed_rows": 0,
                            "size_bytes": 8192,
                            "num_segments": 1,
                        },
                    ]
                });
                http::Response::builder()
                    .status(200)
                    .body(serde_json::to_string(&body).unwrap())
                    .unwrap()
            }
            // stats endpoint must NOT be called for enriched responses
            path => panic!("Unexpected path (stats should not be called): {}", path),
        }
    });

    let indices = table.list_indices().await.unwrap();
    assert_eq!(indices.len(), 2);

    let vec_idx = &indices[0];
    assert_eq!(vec_idx.name, "vector_idx");
    assert_eq!(vec_idx.index_type, IndexType::IvfPq);
    assert_eq!(vec_idx.columns, vec!["vector".to_string()]);
    assert_eq!(
        vec_idx.index_uuid,
        Some("3fa85f64-5717-4562-b3fc-2c963f66afa6".to_string())
    );
    assert_eq!(vec_idx.num_indexed_rows, Some(1000));
    assert_eq!(vec_idx.num_unindexed_rows, Some(50));
    assert_eq!(vec_idx.size_bytes, Some(204800));
    assert_eq!(vec_idx.num_segments, Some(2));
    assert_eq!(vec_idx.index_version, Some(1));
    assert_eq!(
        vec_idx.index_details,
        Some("{\"num_partitions\":16}".to_string())
    );
    assert_eq!(
        vec_idx.type_url,
        Some("type.googleapis.com/lance.index.vector.IvfPq".to_string())
    );
    assert_eq!(
        vec_idx.created_at,
        Some("2026-06-18T21:37:36.637Z".parse::<DateTime<Utc>>().unwrap())
    );

    let text_idx = &indices[1];
    assert_eq!(text_idx.name, "text_idx");
    assert_eq!(text_idx.index_type, IndexType::FTS);
    assert_eq!(text_idx.columns, vec!["text".to_string()]);
    assert_eq!(
        text_idx.index_uuid,
        Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_string())
    );
    assert_eq!(text_idx.num_indexed_rows, Some(1000));
    assert_eq!(text_idx.num_unindexed_rows, Some(0));
    assert_eq!(text_idx.size_bytes, Some(8192));
    assert_eq!(text_idx.num_segments, Some(1));
    // optional fields not present in the response are None
    assert_eq!(text_idx.index_version, None);
    assert_eq!(text_idx.index_details, None);
    assert_eq!(text_idx.type_url, None);
    assert_eq!(text_idx.created_at, None);
}

#[tokio::test]
async fn test_tokenize_uses_remote_index_details() {
    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
    let index_details = serde_json::json!({
        "base_tokenizer": "icu",
        "language": "English",
        "with_position": false,
        "max_token_length": 40,
        "lower_case": true,
        "stem": false,
        "remove_stop_words": true,
        "ascii_folding": true,
        "custom_stop_words": ["hello"],
    })
    .to_string();
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap(),
            "/v1/table/my_table/index/list/" => {
                let body = serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "text_idx",
                            "columns": ["text"],
                            "index_type": "FTS",
                            "index_details": index_details,
                        },
                    ]
                });
                http::Response::builder()
                    .status(200)
                    .body(serde_json::to_string(&body).unwrap())
                    .unwrap()
            }
            path => panic!("Unexpected path: {}", path),
        }
    });

    let tokens = table
        .tokenize("Hello, こんにちは世界!", "text_idx")
        .await
        .unwrap();

    // Positions are relative to the first retained token, so dropping the
    // leading "hello" stop word does not shift the remaining tokens.
    assert_eq!(
        tokens,
        vec![
            FtsToken {
                text: "こんにちは".to_string(),
                position: 0,
            },
            FtsToken {
                text: "世界".to_string(),
                position: 1,
            },
        ]
    );
}

#[tokio::test]
async fn test_tokenize_requires_existing_index_name() {
    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
    let table = Table::new_with_handler("my_table", move |request| -> http::Response<String> {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap(),
            "/v1/table/my_table/index/list/" => {
                let body = serde_json::json!({ "indexes": [] });
                http::Response::builder()
                    .status(200)
                    .body(serde_json::to_string(&body).unwrap())
                    .unwrap()
            }
            path => panic!("Unexpected path: {}", path),
        }
    });

    let err = table.tokenize("hello", "text_idx").await.unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message.contains("No index named 'text_idx'")
    ));
}

#[tokio::test]
async fn test_tokenize_with_column_remote_requires_index_details() {
    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
    let table = Table::new_with_handler("my_table", move |request| {
        assert_eq!(request.method(), "POST");
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_response(&schema))
                .unwrap(),
            "/v1/table/my_table/index/list/" => {
                let body = serde_json::json!({
                    "indexes": [
                        {
                            "index_name": "text_idx",
                            "columns": ["text"],
                            "index_type": "FTS",
                        },
                    ]
                });
                http::Response::builder()
                    .status(200)
                    .body(serde_json::to_string(&body).unwrap())
                    .unwrap()
            }
            path => panic!("Unexpected path: {}", path),
        }
    });

    let err = table
        .tokenize_with_column("hello", "text")
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        Error::InvalidInput { message }
            if message.contains("does not include tokenizer details")
    ));
}

#[test]
fn test_deserialize_created_at() {
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default, deserialize_with = "deserialize_created_at")]
        created_at: Option<DateTime<Utc>>,
    }

    // RFC 3339 string (current server format).
    let w: Wrapper = serde_json::from_str(r#"{"created_at": "2026-06-18T21:37:36.637Z"}"#).unwrap();
    assert_eq!(
        w.created_at,
        Some("2026-06-18T21:37:36.637Z".parse::<DateTime<Utc>>().unwrap())
    );

    // Unix milliseconds (legacy server format).
    let w: Wrapper = serde_json::from_str(r#"{"created_at": 1700000000000}"#).unwrap();
    assert_eq!(w.created_at, DateTime::from_timestamp_millis(1700000000000));

    // Null and missing both yield None.
    let w: Wrapper = serde_json::from_str(r#"{"created_at": null}"#).unwrap();
    assert_eq!(w.created_at, None);
    let w: Wrapper = serde_json::from_str(r#"{}"#).unwrap();
    assert_eq!(w.created_at, None);

    // A malformed string is rejected rather than silently dropped to None.
    assert!(serde_json::from_str::<Wrapper>(r#"{"created_at": "not-a-date"}"#).is_err());
}
