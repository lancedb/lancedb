// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use clap::Parser;
use lancedb_cli::Args;
use serde_json::{json, Value};

#[tokio::test]
async fn acl_commands_use_catalog_endpoints() {
    let requests = Arc::new(Mutex::new(Vec::<(String, Value, Option<String>)>::new()));
    let captured = requests.clone();
    let app = Router::new().fallback(move |request: Request<axum::body::Body>| {
        let captured = captured.clone();
        async move {
            let path = request.uri().path().to_owned();
            let credential = request
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
            let value: Value = serde_json::from_slice(&body).unwrap();
            captured
                .lock()
                .unwrap()
                .push((path.clone(), value, credential));
            if path.ends_with("/list") {
                Json(json!({
                    "entries": [{
                        "subject_id": "p:alice",
                        "object": "table:analytics:public$events",
                        "privilege": "SELECT"
                    }],
                    "continuation_token": "next"
                }))
                .into_response()
            } else {
                StatusCode::OK.into_response()
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    for command in ["list", "add", "delete"] {
        let mut args = vec![
            "lancedb",
            "acl",
            command,
            "--http-server-url",
            &endpoint,
            "--api-key",
            "test-key",
            "--principal-name",
            "Alice",
            "--object",
            "table:analytics:public$events",
        ];
        if command != "list" {
            args.extend(["--privilege", "SELECT"]);
        }
        lancedb_cli::run(Args::try_parse_from(args).unwrap())
            .await
            .unwrap();
    }

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (index, command) in ["list", "add", "delete"].into_iter().enumerate() {
        let (path, body, credential) = &requests[index];
        assert_eq!(path, &format!("/admin/authz/acl/{command}"));
        assert_eq!(credential.as_deref(), Some("test-key"));
        assert_eq!(body["subject"], "P:Alice");
        assert_eq!(body["object"], "table:analytics:public$events");
        if command == "list" {
            assert!(body.get("privilege").is_none());
        } else {
            assert_eq!(body["privilege"], "SELECT");
        }
    }
    server.abort();
}
