// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[tokio::test]
async fn test_update_field_metadata() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/update_field_metadata/"
        );
        http::Response::builder()
            .status(200)
            .body(r#"{"version": 7, "fields": {"category": {"unit": "label"}}}"#)
            .unwrap()
    });

    let result = table
        .update_field_metadata(&[FieldMetadataUpdate::new("category").set("unit", "label")])
        .await
        .unwrap();
    assert_eq!(result.version, 7);
}

#[tokio::test]
async fn test_create_branch_default_source() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/branches/create/");
        let body = request_body_json(&request);
        assert_eq!(body["name"], "exp");
        assert!(
            body.get("from_branch").is_none(),
            "a main source omits from_branch"
        );
        assert!(
            body.get("from_version").is_none(),
            "a latest source omits from_version"
        );
        http::Response::builder().status(200).body("{}").unwrap()
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    assert_eq!(branch.current_branch(), Some("exp".to_string()));
    assert_eq!(table.current_branch(), None);
}

#[tokio::test]
async fn test_create_branch_from_branch_and_version() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| {
        let body = request_body_json(&request);
        assert_eq!(body["name"], "exp");
        assert_eq!(body["from_branch"], "base");
        assert_eq!(body["from_version"], 3);
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .create_branch("exp", Ref::Version(Some("base".into()), Some(3)))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_branch_from_main_normalizes_to_none() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| {
        let body = request_body_json(&request);
        assert!(
            body.get("from_branch").is_none(),
            "\"main\" normalizes to an absent from_branch"
        );
        assert_eq!(body["from_version"], 7);
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .create_branch("exp", Ref::Version(Some("main".into()), Some(7)))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_branch_from_version_number_on_main() {
    use lance::dataset::refs::Ref;
    // A bare version number on a main handle resolves to (main, version).
    let table = Table::new_with_handler("my_table", |request| {
        let body = request_body_json(&request);
        assert!(body.get("from_branch").is_none());
        assert_eq!(body["from_version"], 5);
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table
        .create_branch("exp", Ref::VersionNumber(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_branch_from_tag_resolves_via_tags_endpoint() {
    use lance::dataset::refs::Ref;
    // A tag source has no from_tag in the create contract; it is resolved to
    // its (branch, version) via the tags/version endpoint first.
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/tags/version/" => {
            assert_eq!(request_body_json(&request)["tag"], "t");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":3,"branch":"base"}"#.to_string())
                .unwrap()
        }
        "/v1/table/my_table/branches/create/" => {
            let body = request_body_json(&request);
            assert_eq!(body["name"], "exp");
            assert_eq!(body["from_branch"], "base");
            assert_eq!(body["from_version"], 3);
            http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    table
        .create_branch("exp", Ref::Tag("t".into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_branch_from_tag_on_main_normalizes() {
    use lance::dataset::refs::Ref;
    // A tag resolving to the main branch collapses from_branch to absent.
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/tags/version/" => http::Response::builder()
            .status(200)
            .body(r#"{"version":4,"branch":"main"}"#.to_string())
            .unwrap(),
        "/v1/table/my_table/branches/create/" => {
            let body = request_body_json(&request);
            assert!(
                body.get("from_branch").is_none(),
                "a resolved \"main\" normalizes to an absent from_branch"
            );
            assert_eq!(body["from_version"], 4);
            http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    table
        .create_branch("exp", Ref::Tag("t".into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_branch_invalid_request_maps_to_invalid_input() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(400)
            .body("unsafe branch name")
            .unwrap()
    });
    let err = table
        .create_branch("../evil", Ref::Version(None, None))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
}

#[tokio::test]
async fn test_create_branch_conflict_maps_to_already_exists() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(409)
            .body("branch already exists")
            .unwrap()
    });
    let err = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::TableAlreadyExists { .. }),
        "409 should map to AlreadyExists, got {err:?}"
    );
}

#[tokio::test]
async fn test_materialized_view_describe_and_refresh_requires_sql() {
    const QUERY: &str =
        "SELECT s.x, d.label FROM analytics.source s JOIN analytics.dim d ON s.id = d.id";
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/materialized_view/my_table/describe" => http::Response::builder()
            .status(200)
            .body(
                json!({
                    "name": "my_table",
                    "query": QUERY
                })
                .to_string(),
            )
            .unwrap(),
        path => panic!("unexpected request: {path}"),
    });
    let view = crate::MaterializedView::from_table(table).await.unwrap();
    assert_eq!(view.definition_sql(), QUERY);
    assert!(view.definition().is_err());
    let error = view.refresh().execute().await.unwrap_err();
    assert!(error.to_string().contains("SQL is unavailable"));
}

#[tokio::test]
async fn test_create_branch_empty_name_rejected_client_side() {
    use lance::dataset::refs::Ref;
    // The empty name is rejected before any request is sent.
    let table = Table::new_with_handler("my_table", |request| -> http::Response<String> {
        panic!("unexpected request: {}", request.url().path())
    });
    let err = table
        .create_branch("", Ref::Version(None, None))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
}

#[tokio::test]
async fn test_list_branches() {
    use lance::dataset::refs::BranchIdentifier;
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/branches/list/");
        // A branch forked off main: the server omits `parentBranch` entirely
        // (skip_serializing_if), not `null`, so this mirrors the real wire.
        http::Response::builder()
            .status(200)
            .body(r#"{"branches":{"exp":{"parentVersion":2,"createAt":1234,"manifestSize":4096}}}"#)
            .unwrap()
    });
    let branches = table.list_branches().await.unwrap();
    let exp = branches.get("exp").expect("exp present");
    assert_eq!(exp.parent_version, 2);
    assert_eq!(exp.create_at, 1234);
    assert_eq!(exp.manifest_size, 4096);
    assert_eq!(exp.parent_branch, None);
    assert!(exp.metadata.is_empty());
    // The server omits the internal lineage token; it defaults to the sentinel.
    assert_eq!(
        exp.identifier,
        BranchIdentifier::missing_identifier_sentinel()
    );
}

#[tokio::test]
async fn test_main_only_metadata_is_unfenced_from_branch_timeline() {
    let requests = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let captured = requests.clone();
    let saw_delete_response_floor = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_high_floor = saw_delete_response_floor.clone();
    let table = RemoteTable::new_mock(
        "my_table".to_string(),
        move |request| {
            let path = request.url().path().to_string();
            captured
                .lock()
                .unwrap()
                .insert(path.clone(), request.headers().clone());
            match path.as_str() {
                "/v1/table/my_table/count_rows/" => {
                    saw_high_floor.store(
                        request
                            .headers()
                            .get("x-lancedb-min-read-version")
                            .and_then(|value| value.to_str().ok())
                            == Some("100"),
                        std::sync::atomic::Ordering::SeqCst,
                    );
                    http::Response::builder()
                        .status(200)
                        .header("x-lancedb-version", "2")
                        .body("1".to_string())
                        .unwrap()
                }
                "/v1/table/my_table/tags/list/" => http::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/tags/version/" => http::Response::builder()
                    .status(200)
                    .body(r#"{"version":1}"#.to_string())
                    .unwrap(),
                "/v1/table/my_table/tags/delete/" => http::Response::builder()
                    .status(200)
                    .header("x-lancedb-version", "100")
                    .body("{}".to_string())
                    .unwrap(),
                "/v1/table/my_table/branches/list/" => http::Response::builder()
                    .status(200)
                    .body(r#"{"branches":{}}"#.to_string())
                    .unwrap(),
                path => panic!("unexpected path: {path}"),
            }
        },
        None,
    );
    let branch = table.with_branch(Some("exp".to_string()));

    branch.count_rows(None).await.unwrap();
    let mut tags = branch.tags().await.unwrap();
    tags.list().await.unwrap();
    tags.get_version("v1").await.unwrap();
    tags.delete("v1").await.unwrap();
    branch.list_branches().await.unwrap();
    branch.count_rows(None).await.unwrap();

    let requests = requests.lock().unwrap();
    for path in [
        "/v1/table/my_table/tags/list/",
        "/v1/table/my_table/tags/version/",
        "/v1/table/my_table/tags/delete/",
        "/v1/table/my_table/branches/list/",
    ] {
        assert!(
            !requests[path].contains_key("x-lancedb-min-read-version"),
            "{path} inherited the branch timeline"
        );
    }
    assert!(
        !saw_delete_response_floor.load(std::sync::atomic::Ordering::SeqCst),
        "tag deletion contaminated the branch timeline"
    );
}

#[tokio::test]
async fn test_delete_branch() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/branches/delete/");
        let body = request_body_json(&request);
        assert_eq!(body["name"], "exp");
        http::Response::builder().status(200).body("{}").unwrap()
    });
    table.delete_branch("exp").await.unwrap();
}

#[tokio::test]
async fn test_delete_branch_not_found() {
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(404)
            .body("no such branch")
            .unwrap()
    });
    let err = table.delete_branch("ghost").await.unwrap_err();
    assert!(matches!(err, Error::TableNotFound { .. }), "got {err:?}");
}

fn sample_branch_diff_json() -> &'static str {
    r#"{
            "fromBranch":"exp",
            "parentVersion":1,
            "mainVersion":1,
            "branchVersion":2,
            "baseMoved":false,
            "rowCountMain":3,
            "rowCountBranch":3,
            "rowSummary":{
                "unchanged":3,
                "newOnBase":0,
                "newOnBranch":0,
                "staleRecompute":0,
                "inputsChanged":0,
                "deltaAvailable":false
            },
            "addedColumns":[{"name":"tag","dataType":"utf8","nullable":true}],
            "removedColumns":[],
            "changedColumns":[],
            "addedIndexes":[],
            "removedIndexes":[],
            "errors":[]
        }"#
}

#[tokio::test]
async fn test_diff_branch() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.url().path(), "/v1/table/my_table/branches/diff/");
        let body = request_body_json(&request);
        assert_eq!(body["from_branch"], "exp");
        http::Response::builder()
            .status(200)
            .body(sample_branch_diff_json())
            .unwrap()
    });
    let diff = table.diff_branch("exp").await.unwrap();
    assert_eq!(diff.from_branch, "exp");
    assert!(diff.errors.is_empty());
    assert_eq!(diff.added_columns.len(), 1);
    assert_eq!(diff.added_columns[0].name, "tag");
}

#[tokio::test]
async fn test_cherry_pick_dry_run() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/branches/cherry_pick/"
        );
        let body = request_body_json(&request);
        assert_eq!(body["from_branch"], "exp");
        assert_eq!(body["dry_run"], true);
        let resp = format!(
            r#"{{"status":"ready","diff":{},"preview":{{"promotedColumns":["tag"]}}}}"#,
            sample_branch_diff_json()
        );
        http::Response::builder().status(200).body(resp).unwrap()
    });
    let result = table.cherry_pick("exp", true).await.unwrap();
    assert_eq!(result.status, crate::table::CherryPickStatus::Ready);
    assert_eq!(result.preview.promoted_columns, vec!["tag".to_string()]);
    assert!(result.main_version_after.is_none());
}

#[tokio::test]
async fn test_successful_cherry_pick_advances_main_read_watermark() {
    let count_headers = Arc::new(std::sync::Mutex::new(None));
    let captured = count_headers.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/cherry_pick/" => {
            let response = serde_json::json!({
                "status": "cherryPicked",
                "diff": serde_json::from_str::<serde_json::Value>(sample_branch_diff_json())
                    .unwrap(),
                "preview": { "promotedColumns": ["tag"] },
                "mainVersionAfter": 2
            });
            http::Response::builder()
                .status(200)
                .body(response.to_string())
                .unwrap()
        }
        "/v1/table/my_table/count_rows/" => {
            *captured.lock().unwrap() = Some(request.headers().clone());
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected path: {path}"),
    });

    let result = table.cherry_pick("exp", false).await.unwrap();
    assert_eq!(result.status, crate::table::CherryPickStatus::CherryPicked);
    table.count_rows(None).await.unwrap();
    assert_eq!(
        count_headers
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get("x-lancedb-min-read-version")
            .and_then(|value| value.to_str().ok()),
        Some("2")
    );
}

#[tokio::test]
async fn test_cherry_pick_failed_returns_ok_with_body() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(
            request.url().path(),
            "/v1/table/my_table/branches/cherry_pick/"
        );
        let body = request_body_json(&request);
        assert_eq!(body["dry_run"], false);
        let mut diff: serde_json::Value = serde_json::from_str(sample_branch_diff_json()).unwrap();
        diff["errors"] = serde_json::json!([{
            "code": "baseMoved",
            "message": "main has advanced"
        }]);
        let resp = serde_json::json!({
            "status": "failed",
            "diff": diff,
            "preview": { "promotedColumns": [] }
        });
        http::Response::builder()
            .status(409)
            .body(resp.to_string())
            .unwrap()
    });
    let result = table.cherry_pick("exp", false).await.unwrap();
    assert_eq!(result.status, crate::table::CherryPickStatus::Failed);
    assert!(!result.diff.errors.is_empty());
    assert_eq!(result.diff.errors.len(), 1);
}

#[tokio::test]
async fn test_cherry_pick_unknown_error_code_parses() {
    let table = Table::new_with_handler("my_table", |_| {
        let mut diff: serde_json::Value = serde_json::from_str(sample_branch_diff_json()).unwrap();
        diff["errors"] = serde_json::json!([{
            "code": "multipleCommits",
            "message": "branch has more than one data commit"
        }]);
        let resp = serde_json::json!({
            "status": "failed",
            "diff": diff,
            "preview": { "operation": "append", "rowsAdded": 2 }
        });
        http::Response::builder()
            .status(409)
            .body(resp.to_string())
            .unwrap()
    });
    let result = table.cherry_pick("exp", false).await.unwrap();
    assert_eq!(result.status, crate::table::CherryPickStatus::Failed);
    assert_eq!(
        result.diff.errors[0].code,
        crate::table::CherryPickErrorCode::Unknown
    );
    assert!(result.preview.promoted_columns.is_empty());
}

#[tokio::test]
async fn test_cherry_pick_unexpected_2xx_is_error() {
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(204)
            .body(String::new())
            .unwrap()
    });
    let err = table.cherry_pick("exp", false).await.unwrap_err();
    match err {
        Error::Http {
            status_code: Some(code),
            ..
        } => assert_eq!(code, reqwest::StatusCode::NO_CONTENT),
        other => panic!("expected Http error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_checkout_branch_validates_via_list() {
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/branches/list/");
        http::Response::builder()
            .status(200)
            .body(
                r#"{"branches":{"exp":{"parentBranch":null,"parentVersion":1,"createAt":1,"manifestSize":1}}}"#,
            )
            .unwrap()
    });
    let branch = table.checkout_branch("exp", None).await.unwrap();
    assert_eq!(branch.current_branch(), Some("exp".to_string()));
}

#[tokio::test]
async fn test_checkout_branch_missing() {
    let table = Table::new_with_handler("my_table", |_| {
        http::Response::builder()
            .status(200)
            .body(r#"{"branches":{}}"#)
            .unwrap()
    });
    let err = table.checkout_branch("ghost", None).await.unwrap_err();
    assert!(matches!(err, Error::TableNotFound { .. }), "got {err:?}");
}

#[tokio::test]
async fn test_checkout_main_returns_main_handle() {
    // "main" yields a main-scoped handle without any validation request.
    let table = Table::new_with_handler("my_table", |request| -> http::Response<String> {
        panic!("unexpected request: {}", request.url().path())
    });
    let main = table.checkout_branch("main", None).await.unwrap();
    assert_eq!(main.current_branch(), None);
}

#[tokio::test]
async fn test_branch_count_rows_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/count_rows/" => {
            let body = request_body_json(&request);
            assert_eq!(body["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body("7".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    assert_eq!(branch.count_rows(None).await.unwrap(), 7);
}

#[tokio::test]
async fn test_main_handle_omits_branch_in_body() {
    // A main handle must not send a branch field (byte-compatible with
    // pre-branch servers and the existing wire format).
    let table = Table::new_with_handler("my_table", |request| {
        assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
        let body = request_body_json(&request);
        assert!(
            body.get("branch").is_none(),
            "main handle must not send a branch field"
        );
        http::Response::builder().status(200).body("0").unwrap()
    });
    table.count_rows(None).await.unwrap();
}

#[tokio::test]
async fn test_branch_update_and_delete_carry_branch() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/update/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":5}"#.to_string())
                .unwrap()
        }
        "/v1/table/my_table/delete/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":6,"num_deleted_rows":1}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch
        .update()
        .column("a", "a + 1")
        .execute()
        .await
        .unwrap();
    branch.delete("a > 1").await.unwrap();
}

#[tokio::test]
async fn test_branch_list_indices_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    // list_indices posts to index/list and then fetches the schema (describe)
    // to resolve column names; both must carry the branch.
    let describe_body =
        describe_response(&Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/index/list/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(r#"{"indexes":[]}"#.to_string())
                .unwrap()
        }
        "/v1/table/my_table/describe/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    assert!(branch.list_indices().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_branch_list_versions_carries_query_param() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/version/list/" => {
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "branch")
                    .map(|(_, v)| v.into_owned()),
                Some("exp".to_string()),
                "version/list must carry ?branch=exp"
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"versions":[]}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    assert!(branch.list_versions().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_branch_drop_index_carries_query_param() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/index/my_idx/drop/" => {
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "branch")
                    .map(|(_, v)| v.into_owned()),
                Some("exp".to_string())
            );
            http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.drop_index("my_idx").await.unwrap();
}

#[tokio::test]
async fn test_branch_insert_carries_query_param() {
    use lance::dataset::refs::Ref;
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let describe_body = describe_response(&data.schema());
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/describe/" => {
            // schema() fetch on the branch handle carries branch in the body.
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap()
        }
        "/v1/table/my_table/insert/" => {
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "branch")
                    .map(|(_, v)| v.into_owned()),
                Some("exp".to_string()),
                "insert must carry ?branch=exp"
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version":2}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.add(data.clone()).execute().await.unwrap();
}

#[tokio::test]
async fn test_branch_tag_create_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/tags/create/" => {
            let body = request_body_json(&request);
            assert_eq!(body["branch"], "exp");
            assert_eq!(body["tag"], "v1");
            http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.tags().await.unwrap().create("v1", 1).await.unwrap();
}

#[tokio::test]
async fn test_checkout_branch_version_forwards_branch() {
    // Branch versions overlap main's, so the version must resolve on the
    // branch's own chain -- the validating describe and reads carry both.
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let describe_body = describe_response(&schema);
    let table = Table::new_with_handler("my_table", move |request| {
        match request.url().path() {
            "/v1/table/my_table/branches/list/" => http::Response::builder()
                .status(200)
                .body(
                    r#"{"branches":{"exp":{"parentBranch":null,"parentVersion":1,"createAt":1,"manifestSize":1}}}"#
                        .to_string(),
                )
                .unwrap(),
            "/v1/table/my_table/describe/" => {
                let body = request_body_json(&request);
                assert_eq!(body["branch"], "exp", "checkout validate carries branch");
                assert_eq!(body["version"], 2, "checkout validate carries version");
                http::Response::builder().status(200).body(describe_body.clone()).unwrap()
            }
            "/v1/table/my_table/count_rows/" => {
                let body = request_body_json(&request);
                assert_eq!(body["branch"], "exp");
                assert_eq!(body["version"], 2, "overlapping version resolves on the branch chain");
                http::Response::builder().status(200).body("3".to_string()).unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        }
    });
    let branch = table.checkout_branch("exp", Some(2)).await.unwrap();
    assert_eq!(branch.current_branch(), Some("exp".to_string()));
    assert_eq!(branch.count_rows(None).await.unwrap(), 3);
}

fn branch_query_param(request: &reqwest::Request) -> Option<String> {
    request
        .url()
        .query_pairs()
        .find(|(k, _)| k == "branch")
        .map(|(_, v)| v.into_owned())
}

#[tokio::test]
async fn test_branch_query_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    // /query/ is the hot read path; the branch must ride in the JSON body.
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data_ref = data.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body(b"{}".to_vec())
            .unwrap(),
        "/v1/table/my_table/query/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(write_ipc_file(&data_ref))
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    let rows: usize = branch
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 3);
}

#[tokio::test]
async fn test_branch_merge_insert_carries_query_param() {
    use lance::dataset::refs::Ref;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let data: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        [Ok(batch.clone())],
        batch.schema(),
    ));
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/merge_insert/" => {
            assert_eq!(
                branch_query_param(&request).as_deref(),
                Some("exp"),
                "merge_insert must carry ?branch=exp"
            );
            http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"version":2,"num_deleted_rows":0,"num_inserted_rows":3,"num_updated_rows":0}"#
                            .to_string(),
                    )
                    .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.merge_insert(&["id"]).execute(data).await.unwrap();
}

#[tokio::test]
async fn test_branch_multipart_write_carries_query_param() {
    use lance::dataset::refs::Ref;
    // The multipart path (create -> insert parts -> complete) must carry
    // ?branch= on every leg; an old server version forces it.
    let table = Table::new_with_handler_version(
        "my_table",
        semver::Version::new(0, 4, 0),
        move |request| match request.url().path() {
            "/v1/table/my_table/branches/create/" => http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap(),
            "/v1/table/my_table/describe/" => simple_describe_response(),
            "/v1/table/my_table/multipart_write/create" => {
                assert_eq!(branch_query_param(&request).as_deref(), Some("exp"));
                http::Response::builder()
                    .status(200)
                    .body(r#"{"upload_id": "u1"}"#.to_string())
                    .unwrap()
            }
            "/v1/table/my_table/insert/" => {
                assert_eq!(
                    branch_query_param(&request).as_deref(),
                    Some("exp"),
                    "multipart insert must carry ?branch=exp"
                );
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 1}"#.to_string())
                    .unwrap()
            }
            "/v1/table/my_table/multipart_write/complete" => {
                assert_eq!(branch_query_param(&request).as_deref(), Some("exp"));
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 5}"#.to_string())
                    .unwrap()
            }
            path => panic!("unexpected request path: {path}"),
        },
    );
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
    branch
        .add(vec![batch])
        .write_parallelism(2)
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_branch_restore_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/describe/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            assert_eq!(request_body_json(&request)["version"], 1);
            http::Response::builder()
                .status(200)
                .body(r#"{"version":1,"schema":{"fields":[]}}"#.to_string())
                .unwrap()
        }
        "/v1/table/my_table/restore/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            assert_eq!(request_body_json(&request)["version"], 1);
            http::Response::builder()
                .status(200)
                .body(r#"{"version":1}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.checkout(1).await.unwrap();
    branch.restore().await.unwrap();
}

#[tokio::test]
async fn test_branch_create_index_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    // create_index fetches the schema (describe) to resolve the column and
    // then posts create_index; both must carry the branch in the body.
    let describe_body =
        describe_response(&Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/describe/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap()
        }
        "/v1/table/my_table/create_index/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body("{}".to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch
        .create_index(&["a"], Index::BTree(Default::default()))
        .execute()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_branch_column_ops_carry_branch_in_body() {
    use lance::dataset::refs::Ref;
    // add_columns / alter_columns / drop_columns all stamp the branch into
    // the JSON body via apply_branch_body.
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/describe/" => simple_describe_response(),
        "/v1/table/my_table/add_columns/"
        | "/v1/table/my_table/alter_columns/"
        | "/v1/table/my_table/drop_columns/" => {
            assert_eq!(
                request_body_json(&request)["branch"],
                "exp",
                "{} must carry the branch",
                request.url().path()
            );
            http::Response::builder()
                .status(200)
                .body(r#"{"version":43}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch
        .add_columns()
        .transform(NewColumnTransform::SqlExpressions(vec![(
            "b".into(),
            "a + 1".into(),
        )]))
        .execute()
        .await
        .unwrap();
    branch
        .alter_columns(&[ColumnAlteration::new("a".into()).rename("b".into())])
        .await
        .unwrap();
    branch.drop_columns(&["a"]).await.unwrap();
}

#[tokio::test]
async fn test_branch_update_field_metadata_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/update_field_metadata/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                .status(200)
                .body(r#"{"version":7,"fields":{}}"#.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch
        .update_field_metadata(&[FieldMetadataUpdate::new("category").set("unit", "label")])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_branch_index_stats_carries_branch_in_body() {
    use lance::dataset::refs::Ref;
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body("{}".to_string())
            .unwrap(),
        "/v1/table/my_table/index/my_index/stats/" => {
            assert_eq!(request_body_json(&request)["branch"], "exp");
            http::Response::builder()
                    .status(200)
                    .body(
                        r#"{"num_indexed_rows":1,"num_unindexed_rows":0,"index_type":"IVF_PQ","distance_type":"l2"}"#
                            .to_string(),
                    )
                    .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    assert!(branch.index_stats("my_index").await.unwrap().is_some());
}

#[tokio::test]
async fn test_branch_stats_attaches_body_while_main_omits_it() {
    use lance::dataset::refs::Ref;
    // stats has a bespoke conditional body: a main handle stays a bodyless
    // POST, while a branch handle attaches {"branch": ...}.
    let stats_body = r#"{"total_bytes":1,"num_rows":3,"num_indices":0,"fragment_stats":{"num_fragments":1,"num_small_fragments":0,"lengths":{"min":3,"max":3,"mean":3,"p25":3,"p50":3,"p75":3,"p99":3}}}"#;
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/branches/create/" => http::Response::builder()
            .status(200)
            .body(stats_body.to_string())
            .unwrap(),
        "/v1/table/my_table/stats/" => {
            match request.body() {
                // main handle: byte-identical to the pre-branch wire format.
                None => {}
                // branch handle: branch travels in the body.
                Some(_) => assert_eq!(request_body_json(&request)["branch"], "exp"),
            }
            http::Response::builder()
                .status(200)
                .body(stats_body.to_string())
                .unwrap()
        }
        path => panic!("unexpected request path: {path}"),
    });
    table.stats().await.unwrap();
    let branch = table
        .create_branch("exp", Ref::Version(None, None))
        .await
        .unwrap();
    branch.stats().await.unwrap();
}
