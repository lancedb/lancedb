// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_list_versions() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/version/list/");

        let version1 = lance::dataset::Version {
            version: 1,
            timestamp: "2024-01-01T00:00:00Z".parse().unwrap(),
            metadata: Default::default(),
        };
        let version2 = lance::dataset::Version {
            version: 2,
            timestamp: "2024-02-01T00:00:00Z".parse().unwrap(),
            metadata: Default::default(),
        };
        let response_body = serde_json::json!({
            "versions": [
                version1,
                version2,
            ]
        });
        let response_body = serde_json::to_string(&response_body).unwrap();

        http::Response::builder()
            .status(200)
            .body(response_body)
            .unwrap()
    });

    let versions = table.list_versions().await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 1);
    assert_eq!(
        versions[0].timestamp,
        "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(versions[1].version, 2);
    assert_eq!(
        versions[1].timestamp,
        "2024-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    // assert_eq!(versions, expected);
}

/// Namespace-backed servers report `timestamp_millis` instead of
/// `timestamp`, and may omit `metadata` entirely.
#[tokio::test]
async fn test_list_versions_timestamp_millis() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/version/list/");

        let response_body = serde_json::json!({
            "versions": [
                {
                    "version": 1,
                    "manifest_path": "path/to/_versions/1.manifest",
                    "timestamp_millis": 1704067200000i64,
                },
                {
                    "version": 2,
                    "manifest_path": "path/to/_versions/2.manifest",
                    "timestamp_millis": 1706745600000i64,
                    "metadata": {"key": "value"},
                },
            ]
        });
        let response_body = serde_json::to_string(&response_body).unwrap();

        http::Response::builder()
            .status(200)
            .body(response_body)
            .unwrap()
    });

    let versions = table.list_versions().await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version, 1);
    assert_eq!(
        versions[0].timestamp,
        "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert!(versions[0].metadata.is_empty());
    assert_eq!(versions[1].version, 2);
    assert_eq!(
        versions[1].timestamp,
        "2024-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
    );
    assert_eq!(
        versions[1].metadata.get("key").map(String::as_str),
        Some("value")
    );
}

#[tokio::test]
async fn test_index_stats() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/stats/"
        );

        let response_body = serde_json::json!({
          "num_indexed_rows": 100000,
          "num_unindexed_rows": 0,
          "index_type": "IVF_PQ",
          "distance_type": "l2"
        });
        let response_body = serde_json::to_string(&response_body).unwrap();

        http::Response::builder()
            .status(200)
            .body(response_body)
            .unwrap()
    });
    let indices = table.index_stats("my_index").await.unwrap().unwrap();
    let expected = IndexStatistics {
        num_indexed_rows: 100000,
        num_unindexed_rows: 0,
        index_type: IndexType::IvfPq,
        distance_type: Some(DistanceType::L2),
        num_indices: None,
    };
    assert_eq!(indices, expected);

    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/index/my_index/stats/"
        );

        http::Response::builder().status(404).body("").unwrap()
    });
    let indices = table.index_stats("my_index").await.unwrap();
    assert!(indices.is_none());
}

#[tokio::test]
async fn test_passes_version() {
    let table = Table::new_with_handler("my_table", |request| {
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        let version = body
            .as_object()
            .unwrap()
            .get("version")
            .unwrap()
            .as_u64()
            .unwrap();
        assert_eq!(version, 42);

        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                serde_json::json!({
                    "version": 42,
                    "schema": { "fields": [] }
                })
            }
            "/v1/table/my_table/index/list/" => {
                serde_json::json!({
                    "indexes": []
                })
            }
            "/v1/table/my_table/index/my_idx/stats/" => {
                serde_json::json!({
                    "num_indexed_rows": 100000,
                    "num_unindexed_rows": 0,
                    "index_type": "IVF_PQ",
                    "distance_type": "l2"
                })
            }
            "/v1/table/my_table/count_rows/" => {
                serde_json::json!(1000)
            }
            "/v1/table/my_table/query/" => {
                let expected_data = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap();
                let expected_data_ref = expected_data.clone();
                let response_body = write_ipc_file(&expected_data_ref);
                return http::Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                    .body(response_body)
                    .unwrap();
            }

            path => panic!("Unexpected path: {}", path),
        };

        http::Response::builder()
            .status(200)
            .body(
                serde_json::to_string(&response_body)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            )
            .unwrap()
    });

    table.checkout(42).await.unwrap();

    // ensure that version is passed to the /describe endpoint
    let version = table.version().await.unwrap();
    assert_eq!(version, 42);

    // ensure it's passed to other read API calls
    table.list_indices().await.unwrap();
    table.index_stats("my_idx").await.unwrap();
    table.count_rows(None).await.unwrap();
    table
        .query()
        .nearest_to(vec![0.1, 0.2, 0.3])
        .unwrap()
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_restore_requires_checkout() {
    let request_count = Arc::new(AtomicUsize::new(0));
    let request_count_clone = request_count.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        request_count_clone.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request_body_json(&request)["version"], 42);
        let body = match request.url().path() {
            "/v1/table/my_table/describe/" => r#"{"version":42,"schema":{"fields":[]}}"#,
            "/v1/table/my_table/restore/" => r#"{"version":43}"#,
            path => panic!("unexpected request path: {path}"),
        };
        http::Response::builder().status(200).body(body).unwrap()
    });

    let err = table.restore().await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message }
        if message == "you must run checkout before running restore"));
    assert_eq!(request_count.load(Ordering::SeqCst), 0);

    table.checkout(42).await.unwrap();
    table.checkout_latest().await.unwrap();
    let err = table.restore().await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message }
        if message == "you must run checkout before running restore"));
    assert_eq!(request_count.load(Ordering::SeqCst), 1);

    table.checkout(42).await.unwrap();
    table.restore().await.unwrap();
    assert_eq!(request_count.load(Ordering::SeqCst), 3);

    let err = table.restore().await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { message }
        if message == "you must run checkout before running restore"));
    assert_eq!(request_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_fails_if_checkout_version_doesnt_exist() {
    let table = Table::new_with_handler("my_table", |request| {
        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        let version = body
            .as_object()
            .unwrap()
            .get("version")
            .unwrap()
            .as_u64()
            .unwrap();
        if version != 42 {
            return http::Response::builder()
                .status(404)
                .body(format!("Table my_table (version: {}) not found", version))
                .unwrap();
        }

        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                serde_json::json!({
                    "version": 42,
                    "schema": { "fields": [] }
                })
            }
            _ => panic!("Unexpected path"),
        };

        http::Response::builder()
            .status(200)
            .body(serde_json::to_string(&response_body).unwrap())
            .unwrap()
    });

    let res = table.checkout(43).await;
    println!("{:?}", res);
    assert!(
        matches!(res, Err(Error::TableNotFound { name, .. }) if name == "my_table (version: 43)")
    );
}

#[tokio::test]
async fn test_timetravel_immutable() {
    let table = Table::new_with_handler::<String>("my_table", |request| {
        let response_body = match request.url().path() {
            "/v1/table/my_table/describe/" => {
                serde_json::json!({
                    "version": 42,
                    "schema": { "fields": [] }
                })
            }
            _ => panic!("Should not have made a request: {:?}", request),
        };

        http::Response::builder()
            .status(200)
            .body(serde_json::to_string(&response_body).unwrap())
            .unwrap()
    });

    table.checkout(42).await.unwrap();

    // Ensure that all mutable operations fail.
    let res = table
        .update()
        .column("a", "a + 1")
        .column("b", "b - 1")
        .only_if("b > 10")
        .execute()
        .await;
    assert!(matches!(res, Err(Error::NotSupported { .. })));

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));
    let res = table.merge_insert(&["some_col"]).execute(data).await;
    assert!(matches!(res, Err(Error::NotSupported { .. })));

    let res = table.delete("id in (1, 2, 3)").await;
    assert!(matches!(res, Err(Error::NotSupported { .. })));

    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let res = table.add(data.clone()).execute().await;
    assert!(matches!(res, Err(Error::NotSupported { .. })));

    let res = table
        .create_index(&["a"], Index::IvfPq(Default::default()))
        .execute()
        .await;
    assert!(matches!(res, Err(Error::NotSupported { .. })));
}
