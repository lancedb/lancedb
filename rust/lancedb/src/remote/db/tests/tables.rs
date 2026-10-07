// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[test]
fn test_cache_key_security() {
    // Test that cache keys are unique regardless of delimiter manipulation

    // Case 1: Different delimiters should not affect cache key
    let key1 = build_cache_key("table1", &["ns1".to_string(), "ns2".to_string()]);
    let key2 = build_cache_key("table1", &["ns1$ns2".to_string()]);
    assert_ne!(
        key1, key2,
        "Cache keys should differ for different namespace structures"
    );

    // Case 2: Table name containing delimiter should not cause collision
    let key3 = build_cache_key("ns2$table1", &["ns1".to_string()]);
    assert_ne!(
        key1, key3,
        "Cache key should be different when table name contains delimiter"
    );

    // Case 3: Empty namespace vs namespace with empty string
    let key4 = build_cache_key("table1", &[]);
    let key5 = build_cache_key("table1", &["".to_string()]);
    assert_ne!(
        key4, key5,
        "Empty namespace should differ from namespace with empty string"
    );

    // Case 4: Verify same inputs produce same key (consistency)
    let key6 = build_cache_key("table1", &["ns1".to_string(), "ns2".to_string()]);
    assert_eq!(key1, key6, "Same inputs should produce same cache key");
}

#[tokio::test]
async fn test_create_materialized_view_requires_sql_client() {
    let db = super::super::RemoteDatabase::new_mock(|request| -> http::Response<String> {
        panic!("unexpected REST request: {}", request.url().path())
    });
    let error = db
        .create_materialized_view_async(CreateMaterializedViewRequest {
            name: "adults".into(),
            namespace_path: vec!["analytics".into()],
            query: "SELECT age AS \"age\" FROM \"raw\".\"people\" WHERE age >= 18 LIMIT 10".into(),
            with_no_data: false,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SQL is unavailable"));
}

#[tokio::test]
async fn test_drop_materialized_view_uses_item_route_and_job() {
    let db = super::super::RemoteDatabase::new_mock(|request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/materialized_view/analytics$adults/drop"
        );
        assert!(request.body().is_none());
        http::Response::builder()
            .status(202)
            .body(serde_json::json!({"job_id": "j1-mv-drop"}).to_string())
            .unwrap()
    });
    let job = db
        .drop_materialized_view_async("adults", &["analytics".into()])
        .await
        .unwrap();
    assert_eq!(job.id(), Some("j1-mv-drop"));
}

#[tokio::test]
async fn test_drop_function_async_returns_job() {
    let db = super::super::RemoteDatabase::new_mock(|request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/function/embed/drop");
        http::Response::builder()
            .status(202)
            .body(serde_json::json!({"dropped": true, "job_id": "j1-fn-drop"}).to_string())
            .unwrap()
    });
    let (dropped, job) = db.drop_function_async("embed", "1", &[]).await.unwrap();
    assert!(dropped);
    assert_eq!(job.id(), Some("j1-fn-drop"));
}

/// An unbound name and an inline deletion both answer `200`: the name is gone and
/// nothing is left to wait for, so the job is already finished.
#[tokio::test]
async fn test_drop_function_async_completed_inline() {
    let db = super::super::RemoteDatabase::new_mock(|_| {
        http::Response::builder()
            .status(200)
            .body(serde_json::json!({"dropped": false}).to_string())
            .unwrap()
    });
    let (dropped, job) = db.drop_function_async("embed", "1", &[]).await.unwrap();
    assert!(!dropped);
    assert_eq!(job.id(), None);
    assert_eq!(job.status().await.unwrap(), "finished");
    job.wait().await.unwrap();
}

#[tokio::test]
async fn test_drop_function_async_rejects_incomplete_acceptance() {
    for body in [
        r#"{"dropped":true}"#,
        r#"{"dropped":true,"job_id":""}"#,
        r#"{"dropped":true,"job_id":null}"#,
    ] {
        let db = super::super::RemoteDatabase::new_mock(move |_| {
            http::Response::builder().status(202).body(body).unwrap()
        });
        let error = db
            .drop_function_async("embed", "1", &[])
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("valid job_id"));
    }
}

#[tokio::test]
async fn test_drop_materialized_view_completed_inline() {
    let db = super::super::RemoteDatabase::new_mock(|request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/materialized_view/adults/drop");
        http::Response::builder().status(200).body("{}").unwrap()
    });
    let job = db
        .drop_materialized_view_async("adults", &[])
        .await
        .unwrap();
    assert_eq!(job.id(), None);
    assert_eq!(job.status().await.unwrap(), "finished");
    job.wait().await.unwrap();
}

#[tokio::test]
async fn test_drop_materialized_view_rejects_incomplete_acceptance() {
    for body in ["{}", r#"{"job_id":""}"#, r#"{"job_id":null}"#] {
        let db = super::super::RemoteDatabase::new_mock(move |_| {
            http::Response::builder().status(202).body(body).unwrap()
        });
        let error = db
            .drop_materialized_view_async("adults", &[])
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("valid job_id"));
    }
}

#[tokio::test]
async fn test_drop_materialized_view_rejects_unexpected_success_status() {
    let db = super::super::RemoteDatabase::new_mock(|_| {
        http::Response::builder().status(204).body("").unwrap()
    });
    let error = db
        .drop_materialized_view_async("adults", &[])
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("200 OK or 202 Accepted"));
}

#[tokio::test]
async fn test_list_materialized_views_follows_empty_pages() {
    let page = Arc::new(AtomicUsize::new(0));
    let db = super::super::RemoteDatabase::new_mock({
        let page = page.clone();
        move |request| {
            assert_eq!(request.method(), "GET");
            assert_eq!(
                request.url().path(),
                "/v1/namespace/analytics/materialized_view/list"
            );
            match page.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert!(request.url().query().is_none());
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::json!({"views": [], "page_token": "next"}).to_string())
                        .unwrap()
                }
                1 => {
                    assert_eq!(request.url().query(), Some("page_token=next"));
                    http::Response::builder()
                        .status(200)
                        .body(serde_json::json!({"views": ["adults"]}).to_string())
                        .unwrap()
                }
                _ => panic!("listing requested too many pages"),
            }
        }
    });
    assert_eq!(
        db.list_materialized_views(&["analytics".into()])
            .await
            .unwrap(),
        ["adults"]
    );
}

#[tokio::test]
async fn test_retries() {
    // We'll record the request_id here, to check it matches the one in the error.
    let seen_request_id = Arc::new(OnceLock::new());
    let seen_request_id_ref = seen_request_id.clone();
    let conn = Connection::new_with_handler(move |request| {
        // Request id should be the same on each retry.
        let request_id = request.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_string();
        let seen_id = seen_request_id_ref.get_or_init(|| request_id.clone());
        assert_eq!(&request_id, seen_id);

        http::Response::builder()
            .status(500)
            .body("internal server error")
            .unwrap()
    });
    let result = conn.table_names().execute().await;
    if let Err(Error::Retry {
        request_id,
        request_failures,
        max_request_failures,
        source,
        ..
    }) = result
    {
        let expected_id = seen_request_id.get().unwrap();
        assert_eq!(&request_id, expected_id);
        assert_eq!(request_failures, max_request_failures);
        assert!(
            source.to_string().contains("internal server error"),
            "source: {:?}",
            source
        );
    } else {
        panic!("unexpected result: {:?}", result);
    };
}

#[tokio::test]
async fn test_table_names() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/table/");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["table1", "table2"]}"#)
            .unwrap()
    });
    let names = conn.table_names().execute().await.unwrap();
    assert_eq!(names, vec!["table1", "table2"]);
}

#[tokio::test]
async fn test_table_names_in_a_namespace_never_invents_a_page_token() {
    // The namespace route's token belongs to the store, so `table_names` cannot build one
    // from `start_after`. It walks the namespace on the server's own tokens and applies the
    // name semantics itself, which is what keeps it working either side of the change.
    let page = Arc::new(AtomicUsize::new(0));
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.url().path(), "/v1/namespace/ns/table/list");
        let query = request.url().query().unwrap_or("");
        assert!(
            !query.contains("page_token=users"),
            "a table name must never be sent as a page token: {query}"
        );
        match page.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(
                    !query.contains("page_token"),
                    "the walk starts with no token"
                );
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": ["users", "orders"], "page_token": "opaque-1"}"#)
                    .unwrap()
            }
            _ => {
                assert!(query.contains("page_token=opaque-1"));
                http::Response::builder()
                    .status(200)
                    .body(r#"{"tables": ["widgets"]}"#)
                    .unwrap()
            }
        }
    });

    let names = conn
        .table_names()
        .namespace(vec!["ns".to_string()])
        .start_after("users")
        .execute()
        .await
        .unwrap();
    // Name order, resumed after "users": "orders" sorts before it and is dropped.
    assert_eq!(names, vec!["widgets"]);
}

#[tokio::test]
async fn test_table_names_in_a_namespace_stops_on_a_repeated_token() {
    // A server that handed back the token it was given would never finish the walk.
    let conn = Connection::new_with_handler(|_request| {
        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["a"], "page_token": "same"}"#)
            .unwrap()
    });

    let names = conn
        .table_names()
        .namespace(vec!["ns".to_string()])
        .execute()
        .await
        .unwrap();
    // The guard bounds the walk instead of letting it run forever. The repeat is the
    // server breaking the token contract and is not papered over here.
    assert_eq!(names, vec!["a", "a"]);
}

#[tokio::test]
async fn test_table_names_in_a_namespace_stops_on_an_empty_token() {
    // An empty token ends the listing. Sending it back would ask a server that reads it
    // as "start from the beginning" for the first page a second time, and every name on
    // that page would be collected twice.
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let conn = Connection::new_with_handler(move |request| {
        seen.fetch_add(1, Ordering::SeqCst);
        assert!(
            !request.url().query().unwrap_or("").contains("page_token"),
            "an empty token must never be sent back"
        );
        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["a"], "page_token": ""}"#)
            .unwrap()
    });

    let names = conn
        .table_names()
        .namespace(vec!["ns".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(names, vec!["a"]);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_table_names_pagination() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/table/");
        assert!(request.url().query().unwrap().contains("limit=2"));
        assert!(request.url().query().unwrap().contains("page_token=table2"));

        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["table3", "table4"], "page_token": "token"}"#)
            .unwrap()
    });
    let names = conn
        .table_names()
        .start_after("table2")
        .limit(2)
        .execute()
        .await
        .unwrap();
    assert_eq!(names, vec!["table3", "table4"]);
}

#[tokio::test]
async fn test_open_table() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/describe/");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            .body(r#"{"table": "table1"}"#)
            .unwrap()
    });
    let table = conn.open_table("table1").execute().await.unwrap();
    assert_eq!(table.name(), "table1");

    // Storage options should be ignored.
    let table = conn
        .open_table("table1")
        .storage_option("key", "value")
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "table1");
}

#[tokio::test]
async fn test_open_table_checkout_is_independent_per_handle() {
    let latest = Arc::new(AtomicUsize::new(2));
    let current = latest.clone();
    let mut db = super::super::RemoteDatabase::new_mock(move |request| {
        let body = match request.url().path() {
            "/v1/table/table1/describe/" => {
                let requested_version = request
                    .body()
                    .and_then(|body| body.as_bytes())
                    .and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok())
                    .and_then(|body| body["version"].as_u64());
                let version =
                    requested_version.unwrap_or_else(|| current.load(Ordering::SeqCst) as u64);
                serde_json::json!({
                    "version": version,
                    "schema": {"fields": [
                        {"name": "id", "type": {"type": "int32"}, "nullable": false}
                    ]}
                })
                .to_string()
            }
            "/v1/table/table1/insert/" => {
                let version = current.fetch_add(1, Ordering::SeqCst) + 1;
                serde_json::json!({"version": version}).to_string()
            }
            path => panic!("unexpected request path: {path}"),
        };
        http::Response::builder().status(200).body(body).unwrap()
    });
    db.table_cache = moka::future::Cache::new(10);
    let conn = Connection::new(
        Arc::new(db),
        Arc::new(crate::embeddings::MemoryRegistry::new()),
    );

    let writer = conn.open_table("table1").execute().await.unwrap();
    let reader = conn.open_table("table1").execute().await.unwrap();
    reader.checkout(1).await.unwrap();

    assert_eq!(reader.version().await.unwrap(), 1);
    assert_eq!(writer.version().await.unwrap(), 2);

    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![3]))],
    )
    .unwrap();
    assert_eq!(writer.add(data).execute().await.unwrap().version, 3);
    assert_eq!(writer.version().await.unwrap(), 3);
    assert_eq!(reader.version().await.unwrap(), 1);
}

#[tokio::test]
async fn test_open_table_seeds_the_schema_from_its_describe() {
    let describe_calls = Arc::new(AtomicUsize::new(0));
    let counted = describe_calls.clone();
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.url().path(), "/v1/table/table1/describe/");
        counted.fetch_add(1, Ordering::SeqCst);
        http::Response::builder()
            .status(200)
            .body(
                r#"{"version": 1, "schema": {"fields": [
                        {"name": "id", "type": {"type": "int64"}, "nullable": false}
                    ]}}"#
                    .to_string(),
            )
            .unwrap()
    });

    let table = conn.open_table("table1").execute().await.unwrap();
    let schema = table.schema().await.unwrap();

    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(describe_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_open_table_survives_a_describe_body_it_cannot_parse() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/table/table1/describe/");
        http::Response::builder()
            .status(200)
            .body(r#"{"table": "table1"}"#.to_string())
            .unwrap()
    });

    let table = conn.open_table("table1").execute().await.unwrap();

    assert_eq!(table.name(), "table1");
}

#[tokio::test]
async fn test_open_table_branch_and_version() {
    let conn = Connection::new_with_handler(|request| {
        let body = if request.url().path() == "/v1/table/t/branches/list/" {
            // checkout_branch validates the branch exists via list_branches.
            r#"{"branches":{"exp":{"parentVersion":1,"createAt":1,"manifestSize":1}}}"#
        } else {
            // describe (table open + version/branch validation)
            r#"{"table": "t", "version": 2, "schema": {"fields": [
                    {"name": "a", "type": { "type": "int32" }, "nullable": false}
                ]}}"#
        };
        http::Response::builder().status(200).body(body).unwrap()
    });

    // version-only (and "main" + version) time-travel the main chain
    let v2 = conn.open_table("t").version(2).execute().await.unwrap();
    assert_eq!(v2.current_branch(), None);
    let main_v2 = conn
        .open_table("t")
        .branch("main")
        .version(2)
        .execute()
        .await
        .unwrap();
    assert_eq!(main_v2.current_branch(), None);

    // a non-main branch opens a handle scoped to that branch
    let exp = conn.open_table("t").branch("exp").execute().await.unwrap();
    assert_eq!(exp.current_branch(), Some("exp".to_string()));
    let exp_v2 = conn
        .open_table("t")
        .branch("exp")
        .version(2)
        .execute()
        .await
        .unwrap();
    assert_eq!(exp_v2.current_branch(), Some("exp".to_string()));
}

#[tokio::test]
async fn test_open_table_not_found() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(404)
            .body("table not found")
            .unwrap()
    });
    let result = conn.open_table("table1").execute().await;
    assert!(result.is_err());
    assert!(matches!(result, Err(crate::Error::TableNotFound { .. })));
}

#[tokio::test]
async fn test_create_table() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/create/");
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            ARROW_STREAM_CONTENT_TYPE.as_bytes()
        );

        http::Response::builder().status(200).body("").unwrap()
    });
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = conn.create_table("table1", data).execute().await.unwrap();
    assert_eq!(table.name(), "table1");
}

#[tokio::test]
async fn test_create_table_already_exists() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(400)
            .body("table table1 already exists")
            .unwrap()
    });
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let result = conn.create_table("table1", data).execute().await;
    assert!(result.is_err());
    assert!(matches!(result, Err(crate::Error::TableAlreadyExists { name }) if name == "table1"));
}

#[rstest]
#[case::matching(Field::new("a", DataType::Int32, false), true)]
#[case::different_name(Field::new("x", DataType::Int32, false), false)]
#[case::different_type(Field::new("a", DataType::Int64, false), false)]
#[case::different_nullability(Field::new("a", DataType::Int32, true), false)]
#[tokio::test]
async fn test_create_table_exist_ok_validates_schema(
    #[case] existing_field: Field,
    #[case] matches: bool,
    #[values(false, true)] empty: bool,
    #[values(false, true)] cached: bool,
) {
    let existing_schema = Schema::new(vec![existing_field]);
    let description = serde_json::json!({
        "version": 1,
        "schema": lance::arrow::json::JsonSchema::try_from(&existing_schema).unwrap(),
    })
    .to_string();
    let mut db = super::super::RemoteDatabase::new_mock(move |request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        match request.url().path() {
            "/v1/table/table1/create/" => {
                assert_eq!(request.url().query(), Some("mode=exist_ok"));
                http::Response::builder()
                    .status(400)
                    .body("Table table1 already exists".to_string())
                    .unwrap()
            }
            "/v1/table/table1/describe/" => http::Response::builder()
                .status(200)
                .body(description.clone())
                .unwrap(),
            path => panic!("unexpected path: {path}"),
        }
    });
    db.table_cache = moka::future::Cache::new(10);
    let conn = Connection::new(
        Arc::new(db),
        Arc::new(crate::embeddings::MemoryRegistry::new()),
    );
    if cached {
        conn.open_table("table1").execute().await.unwrap();
    }

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let builder = if empty {
        conn.create_empty_table("table1", schema.clone())
    } else {
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        conn.create_table("table1", data)
    };
    let result = builder
        .mode(CreateTableMode::exist_ok(|b| b))
        .execute()
        .await;
    if matches {
        let table = result.unwrap();
        assert_eq!(table.name(), "table1");
        assert_eq!(table.schema().await.unwrap(), schema);
    } else {
        assert!(matches!(result, Err(Error::Schema { message })
            if message == "Provided schema does not match existing table schema"));
    }
}

#[tokio::test]
async fn test_create_table_modes() {
    let test_cases = [
        (None, "mode=create"),
        (Some(CreateTableMode::Create), "mode=create"),
        (Some(CreateTableMode::Overwrite), "mode=overwrite"),
        (
            Some(CreateTableMode::ExistOk(Box::new(|b| b))),
            "mode=exist_ok",
        ),
    ];

    for (mode, expected_query_string) in test_cases {
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/create/");
            assert_eq!(request.url().query(), Some(expected_query_string));

            http::Response::builder().status(200).body("").unwrap()
        });

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let mut builder = conn.create_table("table1", data.clone());
        if let Some(mode) = mode {
            builder = builder.mode(mode);
        }
        builder.execute().await.unwrap();
    }

    // check that the open table callback is called with exist_ok
    let conn = Connection::new_with_handler(|request| match request.url().path() {
        "/v1/table/table1/create/" => http::Response::builder()
            .status(400)
            .body("Table table1 already exists")
            .unwrap(),
        "/v1/table/table1/describe/" => http::Response::builder()
            .status(200)
            .body(
                r#"{"version": 1, "schema": {"fields": [
                        {"name": "a", "type": {"type": "int32"}, "nullable": false}
                    ]}}"#,
            )
            .unwrap(),
        _ => {
            panic!("unexpected path: {:?}", request.url().path());
        }
    });
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let called: Arc<OnceLock<bool>> = Arc::new(OnceLock::new());
    let called_in_cb = called.clone();
    conn.create_table("table1", data)
        .mode(CreateTableMode::ExistOk(Box::new(move |b| {
            called_in_cb.clone().set(true).unwrap();
            b
        })))
        .execute()
        .await
        .unwrap();

    let called = *called.get().unwrap_or(&false);
    assert!(called);
}

#[tokio::test]
async fn test_create_table_empty() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/create/");
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            ARROW_STREAM_CONTENT_TYPE.as_bytes()
        );

        http::Response::builder().status(200).body("").unwrap()
    });
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    conn.create_empty_table("table1", schema)
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_drop_table() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/drop/");
        assert_eq!(request.url().query(), None);
        assert!(request.body().is_none());

        http::Response::builder().status(200).body("").unwrap()
    });
    conn.drop_table("table1", &[]).await.unwrap();
    // NOTE: the API will return 200 even if the table does not exist. So we shouldn't expect 404.
}

#[tokio::test]
async fn test_drop_table_does_not_read_response_body() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(vec![0xff])
            .unwrap()
    });

    conn.drop_table("table1", &[]).await.unwrap();
}

#[tokio::test]
async fn test_drop_table_async_returns_job() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/drop/");
        http::Response::builder()
            .status(202)
            .body(r#"{"job_id":"drop-job-123"}"#)
            .unwrap()
    });

    let job = conn.drop_table_async("table1", &[]).await.unwrap();
    assert_eq!(job.id(), Some("drop-job-123"));
}

#[tokio::test]
async fn test_drop_table_async_old_server_returns_done_job() {
    let conn =
        Connection::new_with_handler(|_| http::Response::builder().status(200).body("").unwrap());

    let job = conn.drop_table_async("table1", &[]).await.unwrap();
    assert_eq!(job.id(), None);
    assert_eq!(job.status().await.unwrap(), "finished");
}

#[tokio::test]
async fn test_drop_table_async_rejects_accepted_response_without_job_id() {
    let conn =
        Connection::new_with_handler(|_| http::Response::builder().status(202).body("{}").unwrap());

    let error = conn.drop_table_async("table1", &[]).await.err().unwrap();
    assert!(error.to_string().contains("valid job_id"));
}

#[tokio::test]
async fn test_drop_table_async_rejects_empty_job_id() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(202)
            .body(r#"{"job_id":""}"#)
            .unwrap()
    });

    let error = conn.drop_table_async("table1", &[]).await.err().unwrap();
    assert!(error.to_string().contains("valid job_id"));
}

#[tokio::test]
async fn test_rename_table() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/table1/rename/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["new_table_name"], "table2");

        http::Response::builder().status(200).body("").unwrap()
    });
    conn.rename_table("table1", "table2", &[], &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_connect_remote_options() {
    let db_uri = "db://my-container/my-prefix";
    let _ = ConnectBuilder::new(db_uri)
        .region("us-east-1")
        .api_key("my-api-key")
        .storage_options(vec![("azure_storage_account_name", "my-storage-account")])
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_table_names_with_root_namespace() {
    // When namespace is empty (root namespace), should use /v1/table/ for backwards compatibility
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/table/");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["table1", "table2"]}"#)
            .unwrap()
    });
    let names = conn
        .table_names()
        .namespace(vec![])
        .execute()
        .await
        .unwrap();
    assert_eq!(names, vec!["table1", "table2"]);
}

#[tokio::test]
async fn test_table_names_with_namespace() {
    // When namespace is non-empty, should use /v1/namespace/{id}/table/list
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/test/table/list");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            .body(r#"{"tables": ["table1", "table2"]}"#)
            .unwrap()
    });
    let names = conn
        .table_names()
        .namespace(vec!["test".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(names, vec!["table1", "table2"]);
}

#[tokio::test]
async fn test_table_names_with_nested_namespace() {
    // When namespace is vec!["ns1", "ns2"], should use /v1/namespace/ns1$ns2/table/list
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/ns1$ns2/table/list");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            // A namespace listing names the tables in that namespace; the
            // namespace is the route, not part of each name. The client
            // joins the two itself to build each table's identifier, so a
            // listing that repeated the namespace would be joined twice.
            .body(r#"{"tables": ["table1", "table2"]}"#)
            .unwrap()
    });
    let names = conn
        .table_names()
        .namespace(vec!["ns1".to_string(), "ns2".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(names, vec!["table1", "table2"]);
}

#[tokio::test]
async fn test_open_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/describe/");
        assert_eq!(request.url().query(), None);

        http::Response::builder()
            .status(200)
            .body(r#"{"table": "table1"}"#)
            .unwrap()
    });
    let table = conn
        .open_table("table1")
        .namespace(vec!["ns1".to_string(), "ns2".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "table1");
}

#[tokio::test]
async fn test_create_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/ns1$table1/create/");
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            ARROW_STREAM_CONTENT_TYPE.as_bytes()
        );

        http::Response::builder().status(200).body("").unwrap()
    });
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = conn
        .create_table("table1", data)
        .namespace(vec!["ns1".to_string()])
        .execute()
        .await
        .unwrap();
    assert_eq!(table.name(), "table1");
}

#[tokio::test]
async fn test_drop_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/drop/");
        assert_eq!(request.url().query(), None);
        assert!(request.body().is_none());

        http::Response::builder().status(200).body("").unwrap()
    });
    conn.drop_table("table1", &["ns1".to_string(), "ns2".to_string()])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_rename_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/ns1$table1/rename/");
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            JSON_CONTENT_TYPE
        );

        let body = request.body().unwrap().as_bytes().unwrap();
        let body: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(body["new_table_name"], "table2");
        assert_eq!(body["new_namespace"], serde_json::json!(["ns2"]));

        http::Response::builder().status(200).body("").unwrap()
    });
    conn.rename_table(
        "table1",
        "table2",
        &["ns1".to_string()],
        &["ns2".to_string()],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_create_empty_table_with_namespace() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/table/prod$data$metrics/create/");
        assert_eq!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap(),
            ARROW_STREAM_CONTENT_TYPE.as_bytes()
        );

        http::Response::builder().status(200).body("").unwrap()
    });
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    conn.create_empty_table("metrics", schema)
        .namespace(vec!["prod".to_string(), "data".to_string()])
        .execute()
        .await
        .unwrap();
}
