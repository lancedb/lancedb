// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_create_and_alter_secret_send_the_value_in_the_request_body() {
    for (route, call) in [
        ("/v1/secret/openai-prod/create", true),
        ("/v1/secret/openai-prod/alter", false),
    ] {
        let conn = Connection::new_with_handler(move |request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), route);
            // Never a path segment or query parameter, which is what keeps
            // it out of access logs and proxy traces.
            assert!(request.url().query().is_none(), "{:?}", request.url());
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            // The name is the path identifier, so the body is the value alone.
            assert_eq!(body, serde_json::json!({ "value": "sk-live-0001" }));
            http::Response::builder().status(200).body("{}").unwrap()
        });
        if call {
            conn.create_secret("openai-prod", "sk-live-0001", &[])
                .await
                .unwrap();
        } else {
            conn.alter_secret("openai-prod", "sk-live-0001", &[])
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn test_list_secrets_walks_pages_and_returns_names_only() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/$/secret/list");
        let page = request
            .url()
            .query_pairs()
            .find(|(key, _)| key == "page_token")
            .map(|(_, value)| value.into_owned());
        let body = match page.as_deref() {
            None => r#"{"secrets":[{"name":"openai-prod"}],"page_token":"p2"}"#,
            Some("p2") => r#"{"secrets":[{"name":"hf-prod"}]}"#,
            Some(other) => panic!("unexpected page token: {other}"),
        };
        http::Response::builder().status(200).body(body).unwrap()
    });
    assert_eq!(
        conn.list_secrets(&[]).await.unwrap(),
        vec!["openai-prod".to_string(), "hf-prod".to_string()]
    );
}

/// A server that keeps handing back the same token would otherwise spin
/// forever.
#[tokio::test]
async fn test_list_secrets_rejects_a_repeated_page_token() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(r#"{"secrets":[{"name":"openai-prod"}],"page_token":"same"}"#)
            .unwrap()
    });
    let error = conn.list_secrets(&[]).await.unwrap_err();
    assert!(
        error.to_string().contains("repeated a page_token"),
        "{error}"
    );
}

#[tokio::test]
async fn test_drop_and_describe_address_the_secret_in_the_path() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/secret/openai-prod/drop");
        // Nothing is left to say once the path names the Secret.
        assert!(request.body().is_none(), "{:?}", request.body());
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_secret("openai-prod", &[]).await.unwrap();

    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/secret/openai-prod/describe");
        assert!(request.body().is_none(), "{:?}", request.body());
        http::Response::builder()
            .status(200)
            .body(r#"{"name":"openai-prod","created_at_millis":1,"updated_at_millis":2}"#)
            .unwrap()
    });
    let info = conn.describe_secret("openai-prod", &[]).await.unwrap();
    assert_eq!(info.name, "openai-prod");
}

/// The namespace is part of the identifier the path addresses, joined with
/// the client's configured delimiter the way every other object's is. A
/// root Secret is therefore addressed by its bare name, and a namespaced
/// one by the joined path -- there is no body field either way.
#[tokio::test]
async fn test_a_namespace_path_is_addressed_in_the_path() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(
            request.url().path(),
            "/v1/secret/prod$vision$openai-prod/drop"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_secret("openai-prod", &["prod".to_string(), "vision".to_string()])
        .await
        .unwrap();

    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/secret/openai-prod/drop");
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_secret("openai-prod", &[]).await.unwrap();

    // Listing is namespace-scoped, so the namespace is the whole identifier.
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(
            request.url().path(),
            "/v1/namespace/prod$vision/secret/list"
        );
        http::Response::builder()
            .status(200)
            .body(r#"{"secrets":[]}"#)
            .unwrap()
    });
    conn.list_secrets(&["prod".to_string(), "vision".to_string()])
        .await
        .unwrap();
}

/// A view description carries the schema in the namespace spec's JSON
/// encoding, so the body a test asserts against is the one that encoding
/// produces rather than a hand-written guess at it.
fn view_description_body(name: &str, namespace: &[&str], query: &str) -> String {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    serde_json::json!({
        "name": name,
        "namespace": namespace,
        "query": query,
        "default_database": "db",
        "default_namespace": namespace,
        "schema": lance_namespace::schema::arrow_schema_to_json(&schema).unwrap(),
    })
    .to_string()
}

#[tokio::test]
async fn test_create_view_posts_the_query_and_returns_the_resolved_schema() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/view/analytics$adults/create");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        // The name and its namespace address the view in the path, so the
        // body says only what they cannot.
        assert_eq!(body, serde_json::json!({"query": "SELECT id FROM people"}));
        http::Response::builder()
            .status(200)
            .body(view_description_body(
                "adults",
                &["analytics"],
                "SELECT id FROM people",
            ))
            .unwrap()
    });
    let view = conn
        .create_view("adults", "SELECT id FROM people", &["analytics".into()])
        .await
        .unwrap();
    assert_eq!(view.name, "adults");
    assert_eq!(view.namespace_path, vec!["analytics".to_string()]);
    assert_eq!(view.query, "SELECT id FROM people");
    assert_eq!(view.default_database, "db");
    assert_eq!(view.default_namespace_path, vec!["analytics".to_string()]);
    assert_eq!(view.schema.field(0).name(), "id");
}

#[tokio::test]
async fn test_describe_and_drop_address_the_view_in_the_path() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.url().path(), "/v1/view/adults/describe");
        assert!(request.body().is_none(), "{:?}", request.body());
        http::Response::builder()
            .status(200)
            .body(view_description_body(
                "adults",
                &[],
                "SELECT id FROM people",
            ))
            .unwrap()
    });
    let view = conn.describe_view("adults", &[]).await.unwrap();
    assert!(view.namespace_path.is_empty());
    assert_eq!(view.schema.fields().len(), 1);

    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/view/analytics$adults/drop");
        assert!(request.body().is_none(), "{:?}", request.body());
        http::Response::builder().status(200).body("{}").unwrap()
    });
    conn.drop_view("adults", &["analytics".into()])
        .await
        .unwrap();
}

/// An accepted drop hands back the job so a caller can wait on the delete,
/// and the waiting `drop_view` does that for them.
#[tokio::test]
async fn test_drop_view_async_reports_the_cleanup_job() {
    let db = super::super::RemoteDatabase::new_mock(|_| {
        http::Response::builder()
            .status(202)
            .body(r#"{"job_id":"j1-do-abc"}"#)
            .unwrap()
    });
    let job = db.drop_view_async("adults", &[]).await.unwrap();
    assert_eq!(job.id(), Some("j1-do-abc"));
}

/// Nothing was bound, so nothing is being deleted and the job is already done.
#[tokio::test]
async fn test_drop_view_async_reports_a_finished_job_when_nothing_was_bound() {
    let db = super::super::RemoteDatabase::new_mock(|_| {
        http::Response::builder().status(200).body("{}").unwrap()
    });
    let job = db.drop_view_async("adults", &[]).await.unwrap();
    assert_eq!(job.id(), None);
    assert_eq!(job.status().await.unwrap(), "finished");
    job.wait().await.unwrap();
}

#[tokio::test]
async fn test_drop_view_rejects_incomplete_acceptance() {
    for body in ["{}", r#"{"job_id":""}"#, r#"{"job_id":null}"#] {
        let db = super::super::RemoteDatabase::new_mock(move |_| {
            http::Response::builder().status(202).body(body).unwrap()
        });
        let error = db.drop_view_async("adults", &[]).await.err().unwrap();
        assert!(error.to_string().contains("valid job_id"), "{error}");
    }
}

#[tokio::test]
async fn test_drop_view_rejects_unexpected_success_status() {
    let db = super::super::RemoteDatabase::new_mock(|_| {
        http::Response::builder().status(204).body("").unwrap()
    });
    let error = db.drop_view_async("adults", &[]).await.err().unwrap();
    assert!(
        error.to_string().contains("200 OK or 202 Accepted"),
        "{error}"
    );
}

/// A schema the client cannot decode is a broken response, not a view
/// with no columns: reporting it as an error keeps a caller from reading
/// an empty schema as the truth about the view.
#[tokio::test]
async fn test_describe_view_rejects_an_undecodable_schema() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(
                r#"{"name":"adults","query":"SELECT 1","default_database":"db",
                        "schema":{"fields":[{"name":"id","type":{"type":"nonesuch"},
                        "nullable":true}]}}"#,
            )
            .unwrap()
    });
    let error = conn.describe_view("adults", &[]).await.unwrap_err();
    assert!(error.to_string().contains("undecodable schema"), "{error}");
}

#[tokio::test]
async fn test_list_views_walks_pages() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/analytics/view/list");
        let page = request
            .url()
            .query_pairs()
            .find(|(key, _)| key == "page_token")
            .map(|(_, value)| value.into_owned());
        let body = match page.as_deref() {
            None => r#"{"views":[],"page_token":"p2"}"#,
            Some("p2") => r#"{"views":["adults"]}"#,
            Some(other) => panic!("unexpected page token: {other}"),
        };
        http::Response::builder().status(200).body(body).unwrap()
    });
    assert_eq!(
        conn.list_views(&["analytics".into()]).await.unwrap(),
        vec!["adults".to_string()]
    );
}

/// A server that keeps handing back the same token would otherwise spin
/// forever.
#[tokio::test]
async fn test_list_views_rejects_a_repeated_page_token() {
    let conn = Connection::new_with_handler(|_| {
        http::Response::builder()
            .status(200)
            .body(r#"{"views":["adults"],"page_token":"same"}"#)
            .unwrap()
    });
    let error = conn.list_views(&[]).await.unwrap_err();
    assert!(
        error.to_string().contains("repeated a page_token"),
        "{error}"
    );
}

/// A name carrying the delimiter would split back apart as a different
/// view, so it is refused before it reaches a route.
#[tokio::test]
async fn test_view_names_that_would_resplit_are_refused() {
    let conn = Connection::new_with_handler(|_| -> http::Response<String> {
        panic!("an invalid identifier must not reach the service")
    });
    for (name, namespace) in [
        ("analytics$adults", vec![]),
        ("adults", vec!["ana$lytics".to_string()]),
        ("", vec![]),
        ("..", vec![]),
    ] {
        let error = match conn.describe_view(name, &namespace).await {
            Ok(_) => panic!("accepted {name:?} in {namespace:?}"),
            Err(error) => error.to_string(),
        };
        // A view is not a table, so the refusal says so.
        assert!(
            error.contains("view name") || error.contains("view namespace path segment"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn test_create_function_async_sends_canonical_request_and_decodes_typed_job() {
    const REQUEST: &str = include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_registration_request.json"
    );
    const FUNCTION_JOB: &str =
        include_str!("../../../../tests/fixtures/first_class_functions/v1/remote_function_job.json");
    let mut expected: serde_json::Value = serde_json::from_str(REQUEST).unwrap();
    expected.as_object_mut().unwrap().remove("name");
    let conn = Connection::new_with_handler(move |request| match request.url().path() {
        "/v1/function/normalize_score/create" => {
            assert_eq!(request.method(), &reqwest::Method::POST);
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body, expected);
            http::Response::builder()
                .status(202)
                .body(r#"{"job_id":"job-function-1"}"#)
                .unwrap()
        }
        "/v1/jobs/describe" => http::Response::builder()
            .status(200)
            .body(FUNCTION_JOB)
            .unwrap(),
        path => panic!("unexpected path: {path}"),
    });
    let request = crate::function::FunctionRegistrationRequest::from_json(REQUEST).unwrap();
    let job = conn.create_function_async(request, &[]).await.unwrap();
    assert_eq!(job.id(), Some("job-function-1"));
    let version = job.wait().await.unwrap();
    assert_eq!(version.name(), "embed");
    assert_eq!(version.version(), "1");
}

#[tokio::test]
async fn test_get_function_requires_and_sends_exact_version() {
    const VERSION: &str = include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/function/embed/describe");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"version": "1"}));
        http::Response::builder().status(200).body(VERSION).unwrap()
    });
    let version = conn.get_function("embed", "1", &[]).await.unwrap();
    assert_eq!(version.name(), "embed");
    assert_eq!(version.version(), "1");
}

#[tokio::test]
async fn test_list_functions_requests_definitions_and_paginates() {
    const VERSION: &str = include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    let version: serde_json::Value = serde_json::from_str(VERSION).unwrap();
    let page = Arc::new(AtomicUsize::new(0));
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/$/function/list");
        let query = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(query.get("include_definition").unwrap(), "true");
        match page.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(!query.contains_key("page_token"));
                http::Response::builder()
                    .status(200)
                    .body(r#"{"functions": [], "page_token": "next"}"#.to_string())
                    .unwrap()
            }
            _ => {
                assert_eq!(query.get("page_token").unwrap(), "next");
                http::Response::builder()
                    .status(200)
                    .body(
                        serde_json::json!({
                            "functions": [{
                                "name": "embed",
                                "version": "1",
                                "definition": version.clone(),
                            }],
                        })
                        .to_string(),
                    )
                    .unwrap()
            }
        }
    });
    let functions = conn.list_functions(&[]).await.unwrap();
    assert_eq!(functions.len(), 1);
    assert_eq!(functions[0].name(), "embed");
    assert_eq!(functions[0].version(), "1");
}

#[tokio::test]
async fn test_list_functions_stops_on_an_empty_page_token() {
    let requests = Arc::new(AtomicUsize::new(0));
    let seen = requests.clone();
    let conn = Connection::new_with_handler(move |request| {
        seen.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/$/function/list");
        let query = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(query.get("include_definition").unwrap(), "true");
        assert!(!query.contains_key("page_token"));
        http::Response::builder()
            .status(200)
            .body(r#"{"functions": [], "page_token": ""}"#)
            .unwrap()
    });

    let functions = conn.list_functions(&[]).await.unwrap();
    assert!(functions.is_empty());
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_list_functions_rejects_a_page_token_cycle() {
    let page = Arc::new(AtomicUsize::new(0));
    let requests = page.clone();
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.method(), &reqwest::Method::GET);
        assert_eq!(request.url().path(), "/v1/namespace/$/function/list");
        let query = request.url().query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(query.get("include_definition").unwrap(), "true");
        let next_page_token = match page.fetch_add(1, Ordering::SeqCst) {
            0 => {
                assert!(!query.contains_key("page_token"));
                "one"
            }
            1 => {
                assert_eq!(query.get("page_token").unwrap(), "one");
                "two"
            }
            2 => {
                assert_eq!(query.get("page_token").unwrap(), "two");
                "one"
            }
            page => panic!("unexpected page: {page}"),
        };
        http::Response::builder()
            .status(200)
            .body(
                serde_json::json!({
                    "functions": [],
                    "page_token": next_page_token,
                })
                .to_string(),
            )
            .unwrap()
    });

    let error = conn.list_functions(&[]).await.unwrap_err();
    assert!(
        matches!(
            &error,
            Error::Http {
                status_code: Some(http::StatusCode::OK),
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_drop_function_sends_exact_version_and_decodes_replay() {
    let conn = Connection::new_with_handler(|request| {
        assert_eq!(request.method(), &reqwest::Method::POST);
        assert_eq!(request.url().path(), "/v1/function/embed/drop");
        let body: serde_json::Value =
            serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"version": "1"}));
        http::Response::builder()
            .status(200)
            .body(r#"{"dropped":false}"#)
            .unwrap()
    });
    assert!(!conn.drop_function("embed", "1", &[]).await.unwrap());
}

/// A Function's namespace is addressed in the path the way a Secret's is:
/// every route takes the joined identifier and no body carries the
/// namespace, so a namespaced request differs from a root one only in its
/// path.
#[tokio::test]
async fn test_a_function_namespace_path_is_addressed_in_the_path() {
    const REQUEST: &str = include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_registration_request.json"
    );
    const VERSION: &str = include_str!(
        "../../../../tests/fixtures/first_class_functions/v1/remote_function_version.canonical.json"
    );
    let namespace = ["analytics".to_string(), "features".to_string()];
    let mut expected_create: serde_json::Value = serde_json::from_str(REQUEST).unwrap();
    expected_create.as_object_mut().unwrap().remove("name");
    let conn = Connection::new_with_handler(move |request| {
        let body = request
            .body()
            .and_then(|body| body.as_bytes())
            .map(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).unwrap());
        match request.url().path() {
            "/v1/function/analytics$features$normalize_score/create" => {
                assert_eq!(body.unwrap(), expected_create);
                http::Response::builder()
                    .status(202)
                    .body(r#"{"job_id":"job-function-1"}"#.to_string())
                    .unwrap()
            }
            "/v1/function/analytics$features$embed/describe" => {
                assert_eq!(body.unwrap(), serde_json::json!({"version": "1"}));
                http::Response::builder()
                    .status(200)
                    .body(VERSION.to_string())
                    .unwrap()
            }
            "/v1/function/analytics$features$embed/drop" => {
                assert_eq!(body.unwrap(), serde_json::json!({"version": "1"}));
                http::Response::builder()
                    .status(200)
                    .body(r#"{"dropped":true}"#.to_string())
                    .unwrap()
            }
            // Listing is namespace-scoped, so the namespace is the whole
            // identifier.
            "/v1/namespace/analytics$features/function/list" => http::Response::builder()
                .status(200)
                .body(r#"{"functions":[]}"#.to_string())
                .unwrap(),
            path => panic!("unexpected path: {path}"),
        }
    });
    let request = crate::function::FunctionRegistrationRequest::from_json(REQUEST).unwrap();
    let job = conn
        .create_function_async(request, &namespace)
        .await
        .unwrap();
    assert_eq!(job.id(), Some("job-function-1"));
    conn.get_function("embed", "1", &namespace).await.unwrap();
    assert!(conn.list_functions(&namespace).await.unwrap().is_empty());
    assert!(conn.drop_function("embed", "1", &namespace).await.unwrap());
}

/// Each namespace segment is checked before a request is built, so an
/// empty segment cannot collapse the identifier onto the parent namespace.
#[tokio::test]
async fn test_an_unaddressable_function_namespace_segment_is_refused() {
    let conn = Connection::new_with_handler(|request| -> http::Response<String> {
        panic!("reached the transport: {}", request.url().path())
    });
    let namespace = ["analytics".to_string(), String::new()];
    assert!(conn.get_function("embed", "1", &namespace).await.is_err());
    assert!(conn.list_functions(&namespace).await.is_err());
    assert!(conn.drop_function("embed", "1", &namespace).await.is_err());
}

#[tokio::test]
async fn test_conn_job_waits_to_done() {
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_ref = polls.clone();
    let conn = Connection::new_with_handler(move |request| {
        assert_eq!(request.url().path(), "/v1/jobs/describe");
        // Two in-progress answers: one for the load, one for the first
        // status poll.
        let state = if polls_ref.fetch_add(1, Ordering::SeqCst) < 2 {
            "IN_PROGRESS"
        } else {
            "DONE"
        };
        http::Response::builder()
            .status(200)
            .body(format!(
                r#"{{"job_id": "job-1", "job_type": "create_function", "job_state": "{}", "creation_ms": 1, "result": {{"name": "embed", "version": "1"}}}}"#,
                state
            ))
            .unwrap()
    });
    let job = conn.open_job("job-1").await.unwrap();
    assert_eq!(job.id(), Some("job-1"));
    // Opening already answered the state; no extra call needed for it.
    assert_eq!(job.state().as_deref(), Some("running"));
    assert_eq!(job.status().await.unwrap(), "running");
    job.wait().await.unwrap();
    assert_eq!(job.status().await.unwrap(), "finished");
    assert!(polls.load(Ordering::SeqCst) >= 4);
}
