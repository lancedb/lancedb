// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

// ---- Read freshness header tests ------------------------------------

#[test]
fn test_compute_min_timestamp_combines_baseline_and_interval() {
    let now = SystemTime::now();
    let baseline = now - Duration::from_secs(60);

    // No interval, no baseline -> no header.
    assert_eq!(
        compute_min_timestamp(&FreshnessState::default(), None, now),
        None
    );

    // Baseline only -> baseline.
    let state = FreshnessState {
        min_version: None,
        checkout_baseline: Some(baseline),
        min_read_version: None,
        ..FreshnessState::default()
    };
    assert_eq!(compute_min_timestamp(&state, None, now), Some(baseline));

    // ZERO interval, no baseline -> now.
    assert_eq!(
        compute_min_timestamp(&FreshnessState::default(), Some(Duration::ZERO), now),
        Some(now)
    );

    // Positive interval, no baseline -> now - interval.
    assert_eq!(
        compute_min_timestamp(
            &FreshnessState::default(),
            Some(Duration::from_secs(10)),
            now
        ),
        Some(now - Duration::from_secs(10))
    );

    // Both: pick the more-recent (i.e. tighter) constraint.
    // baseline = now-60, now-interval = now-10. now-10 is newer.
    let state = FreshnessState {
        min_version: None,
        checkout_baseline: Some(baseline),
        min_read_version: None,
        ..FreshnessState::default()
    };
    assert_eq!(
        compute_min_timestamp(&state, Some(Duration::from_secs(10)), now),
        Some(now - Duration::from_secs(10))
    );

    // Both, baseline newer: pick baseline.
    let recent_baseline = now - Duration::from_secs(5);
    let state = FreshnessState {
        min_version: None,
        checkout_baseline: Some(recent_baseline),
        min_read_version: None,
        ..FreshnessState::default()
    };
    assert_eq!(
        compute_min_timestamp(&state, Some(Duration::from_secs(60)), now),
        Some(recent_baseline)
    );
}

/// Allowed slop when comparing a header timestamp against a locally
/// captured wall-clock bound. Tests run fast enough that 1s is plenty.
const FRESHNESS_TOLERANCE: Duration = Duration::from_secs(1);

fn capturing_handler<F>(
    body_for: F,
) -> (
    impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
    Arc<std::sync::Mutex<Option<http::HeaderMap>>>,
)
where
    F: Fn(&str) -> String + Clone + Send + Sync + 'static,
{
    let captured = Arc::new(std::sync::Mutex::new(None));
    let captured_c = captured.clone();
    let handler = move |request: reqwest::Request| {
        *captured_c.lock().unwrap() = Some(request.headers().clone());
        let path = request.url().path().to_string();
        http::Response::builder()
            .status(200)
            .body(body_for(&path))
            .unwrap()
    };
    (handler, captured)
}

fn parse_min_timestamp(headers: &http::HeaderMap) -> SystemTime {
    let value = headers
        .get("x-lancedb-min-timestamp")
        .expect("expected x-lancedb-min-timestamp header")
        .to_str()
        .unwrap();
    chrono::DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&chrono::Utc)
        .into()
}

#[tokio::test]
async fn test_freshness_default_sends_no_headers() {
    let (handler, captured) = capturing_handler(|_| "42".to_string());
    let table = Table::new_with_handler("my_table", handler);

    let _ = table.count_rows(None).await.unwrap();

    let headers = captured.lock().unwrap().clone().unwrap();
    assert!(!headers.contains_key("x-lancedb-min-timestamp"));
    assert!(!headers.contains_key("x-lancedb-min-version"));
}

#[tokio::test]
async fn test_freshness_zero_interval_sends_now() {
    let (handler, captured) = capturing_handler(|_| "42".to_string());
    let table =
        Table::new_with_handler_and_interval("my_table", handler, Some(Duration::from_secs(0)));

    let before = SystemTime::now();
    table.count_rows(None).await.unwrap();
    let after = SystemTime::now();

    let headers = captured.lock().unwrap().clone().unwrap();
    let sent = parse_min_timestamp(&headers);
    assert!(
        sent >= before - FRESHNESS_TOLERANCE && sent <= after + FRESHNESS_TOLERANCE,
        "expected timestamp roughly equal to wall clock"
    );
    assert!(!headers.contains_key("x-lancedb-min-version"));
}

#[tokio::test]
async fn test_checkout_disables_read_consistency_interval() {
    let (handler, captured) = capturing_handler(|path| match path {
        "/v1/table/my_table/describe/" => r#"{"version":5,"schema":{"fields":[]}}"#.to_string(),
        "/v1/table/my_table/count_rows/" => "42".to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    let table =
        Table::new_with_handler_and_interval("my_table", handler, Some(Duration::from_secs(0)));

    table.checkout(5).await.unwrap();
    table.count_rows(None).await.unwrap();

    let headers = captured.lock().unwrap().clone().unwrap();
    assert!(!headers.contains_key("x-lancedb-min-timestamp"));
    assert!(!headers.contains_key("x-lancedb-min-version"));
    assert!(!headers.contains_key("x-lancedb-min-read-version"));
}

#[tokio::test]
async fn test_read_snapshot_keeps_selector_and_freshness_generation_bound() {
    let table = RemoteTable::new_mock_with_consistency_interval(
        "my_table".to_string(),
        |_| {
            http::Response::builder()
                .status(200)
                .body(r#"{"version":5,"schema":{"fields":[]}}"#.to_string())
                .unwrap()
        },
        Some(Duration::ZERO),
    );

    let latest = table.snapshot_read_state().await;
    table.checkout(5).await.unwrap();

    let latest_request = latest
        .freshness
        .apply(
            table
                .client
                .post("/v1/table/my_table/count_rows/")
                .json(&serde_json::json!({ "version": latest.version })),
        )
        .build()
        .unwrap();
    assert!(request_body_json(&latest_request)["version"].is_null());
    assert!(latest_request.headers().contains_key(MIN_TIMESTAMP_HEADER));

    let pinned = table.snapshot_read_state().await;
    let pinned_request = pinned
        .freshness
        .apply(
            table
                .client
                .post("/v1/table/my_table/count_rows/")
                .json(&serde_json::json!({ "version": pinned.version })),
        )
        .build()
        .unwrap();
    assert_eq!(request_body_json(&pinned_request)["version"], 5);
    assert!(!pinned_request.headers().contains_key(MIN_TIMESTAMP_HEADER));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelled_checkout_keeps_latest_freshness_enabled() {
    let (described_tx, described_rx) = std::sync::mpsc::channel::<()>();
    let table = Arc::new(RemoteTable::new_mock_with_consistency_interval(
        "my_table".to_string(),
        move |_| {
            described_tx.send(()).unwrap();
            http::Response::builder()
                .status(200)
                .body(r#"{"version":5,"schema":{"fields":[]}}"#.to_string())
                .unwrap()
        },
        Some(Duration::from_secs(0)),
    ));

    let version_guard = table.version.write().await;
    let checkout = tokio::spawn({
        let table = table.clone();
        async move { table.checkout(5).await }
    });
    tokio::task::spawn_blocking(move || {
        described_rx.recv_timeout(Duration::from_secs(10)).unwrap()
    })
    .await
    .unwrap();
    for _ in 0..100 {
        if table.freshness.lock().unwrap().pinned {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!checkout.is_finished());

    checkout.abort();
    assert!(checkout.await.unwrap_err().is_cancelled());
    drop(version_guard);

    assert_eq!(*table.version.read().await, None);
    assert!(table.snapshot_freshness_headers().min_timestamp.is_some());
}

#[tokio::test]
async fn test_freshness_positive_interval_sends_now_minus_interval() {
    let (handler, captured) = capturing_handler(|_| "42".to_string());
    let interval = Duration::from_secs(30);
    let table = Table::new_with_handler_and_interval("my_table", handler, Some(interval));

    let before = SystemTime::now();
    table.count_rows(None).await.unwrap();
    let after = SystemTime::now();

    let headers = captured.lock().unwrap().clone().unwrap();
    let sent = parse_min_timestamp(&headers);
    assert!(
        sent >= before - interval - FRESHNESS_TOLERANCE
            && sent <= after - interval + FRESHNESS_TOLERANCE,
        "expected timestamp roughly equal to now - interval"
    );
}

#[tokio::test]
async fn test_freshness_checkout_latest_sets_baseline() {
    let (handler, captured) = capturing_handler(|path| match path {
        "/v1/table/my_table/count_rows/" => "42".to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    // No interval — only the baseline should drive the timestamp.
    let table = Table::new_with_handler_and_interval("my_table", handler, None);

    let before_checkout = SystemTime::now();
    table.checkout_latest().await.unwrap();
    let after_checkout = SystemTime::now();

    table.count_rows(None).await.unwrap();

    let headers = captured.lock().unwrap().clone().unwrap();
    let sent = parse_min_timestamp(&headers);
    assert!(
        sent >= before_checkout - FRESHNESS_TOLERANCE
            && sent <= after_checkout + FRESHNESS_TOLERANCE,
        "expected timestamp captured at checkout_latest() time"
    );
    assert!(!headers.contains_key("x-lancedb-min-version"));
}

#[tokio::test]
async fn test_freshness_min_version_tracked_after_write() {
    let (handler, captured) = capturing_handler(|path| match path {
        "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
        "/v1/table/my_table/count_rows/" => "42".to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    let table = Table::new_with_handler("my_table", handler);

    let _ = table.update().column("a", "a + 1").execute().await.unwrap();
    // Update headers also pass through captured; reset by reading after.
    table.count_rows(None).await.unwrap();

    let headers = captured.lock().unwrap().clone().unwrap();
    assert_eq!(
        headers
            .get("x-lancedb-min-version")
            .unwrap()
            .to_str()
            .unwrap(),
        "7"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_inflight_write_result_cannot_cross_checkout_generation() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel::<()>();
    let arrived_tx = Arc::new(std::sync::Mutex::new(arrived_tx));
    let count_headers = Arc::new(std::sync::Mutex::new(None));
    let captured = count_headers.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/update/" => {
            arrived_tx.lock().unwrap().send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
            http::Response::builder()
                .status(200)
                .body(r#"{"rows_updated":1,"version":100}"#.to_string())
                .unwrap()
        }
        "/v1/table/my_table/describe/" => http::Response::builder()
            .status(200)
            .body(r#"{"version":5,"schema":{"fields":[]}}"#.to_string())
            .unwrap(),
        "/v1/table/my_table/count_rows/" => {
            *captured.lock().unwrap() = Some(request.headers().clone());
            http::Response::builder()
                .status(200)
                .body("1".to_string())
                .unwrap()
        }
        path => panic!("unexpected path: {path}"),
    });

    let update = tokio::spawn({
        let table = table.clone();
        async move { table.update().column("a", "a + 1").execute().await }
    });
    tokio::task::spawn_blocking(move || arrived_rx.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    table.checkout(5).await.unwrap();
    release_tx.send(()).unwrap();
    update.await.unwrap().unwrap();
    table.count_rows(None).await.unwrap();

    let headers = count_headers.lock().unwrap();
    let headers = headers.as_ref().unwrap();
    assert!(!headers.contains_key("x-lancedb-min-version"));
    assert!(!headers.contains_key("x-lancedb-min-read-version"));
}

/// A handler that records every request's headers and answers each read with
/// an `x-lancedb-version` response header taken from `versions` (by call
/// index, saturating at the last entry). An empty string means "no header".
fn read_version_handler(
    versions: &'static [&'static str],
) -> (
    impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
    Arc<std::sync::Mutex<Vec<http::HeaderMap>>>,
) {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let requests_c = requests.clone();
    let call = Arc::new(AtomicUsize::new(0));
    let handler = move |request: reqwest::Request| {
        requests_c.lock().unwrap().push(request.headers().clone());
        let i = call.fetch_add(1, Ordering::SeqCst).min(versions.len() - 1);
        let mut builder = http::Response::builder().status(200);
        if !versions[i].is_empty() {
            builder = builder.header("x-lancedb-version", versions[i]);
        }
        builder.body("42".to_string()).unwrap()
    };
    (handler, requests)
}

#[tokio::test]
async fn test_read_version_watermark_tracked_and_sent() {
    let (handler, requests) = read_version_handler(&["100", "100"]);
    let table = Table::new_with_handler("my_table", handler);

    // First read has no watermark yet; the response advertises version 100,
    // so the second read must floor the server at 100.
    table.count_rows(None).await.unwrap();
    table.count_rows(None).await.unwrap();

    let reqs = requests.lock().unwrap();
    assert!(!reqs[0].contains_key("x-lancedb-min-read-version"));
    assert_eq!(
        reqs[1]
            .get("x-lancedb-min-read-version")
            .unwrap()
            .to_str()
            .unwrap(),
        "100"
    );
}

#[tokio::test]
async fn test_schema_response_advances_read_watermark() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = requests.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        captured
            .lock()
            .unwrap()
            .push((request.url().path().to_string(), request.headers().clone()));
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .header("x-lancedb-version", "100")
                .body(r#"{"version":100,"schema":{"fields":[]}}"#.to_string())
                .unwrap(),
            "/v1/table/my_table/count_rows/" => http::Response::builder()
                .status(200)
                .body("42".to_string())
                .unwrap(),
            path => panic!("unexpected path: {path}"),
        }
    });

    table.schema().await.unwrap();
    assert_eq!(table.count_rows(None).await.unwrap(), 42);

    let requests = requests.lock().unwrap();
    let count_headers = &requests[1].1;
    assert_eq!(
        count_headers
            .get("x-lancedb-min-read-version")
            .and_then(|value| value.to_str().ok()),
        Some("100")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_inflight_schema_response_cannot_cross_checkout_generation() {
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel::<()>();
    let arrived_tx = Arc::new(std::sync::Mutex::new(arrived_tx));
    let count_headers = Arc::new(std::sync::Mutex::new(None));
    let captured = count_headers.clone();
    let table = Table::new_with_handler("my_table", move |request| match request.url().path() {
        "/v1/table/my_table/describe/" => {
            let body = request_body_json(&request);
            if body["version"].is_null() {
                arrived_tx.lock().unwrap().send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .unwrap();
                http::Response::builder()
                    .status(200)
                    .header("x-lancedb-version", "100")
                    .body(r#"{"version":100,"schema":{"fields":[]}}"#.to_string())
                    .unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version":5,"schema":{"fields":[]}}"#.to_string())
                    .unwrap()
            }
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

    let schema = tokio::spawn({
        let table = table.clone();
        async move { table.schema().await }
    });
    tokio::task::spawn_blocking(move || arrived_rx.recv_timeout(Duration::from_secs(10)).unwrap())
        .await
        .unwrap();
    table.checkout(5).await.unwrap();
    release_tx.send(()).unwrap();
    schema.await.unwrap().unwrap();
    table.count_rows(None).await.unwrap();

    assert!(
        !count_headers
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .contains_key("x-lancedb-min-read-version")
    );
}

#[tokio::test]
async fn test_streaming_write_uses_and_advances_read_watermark() {
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let describe_body = serde_json::to_string(&json!({
        "version": 7,
        "schema": JsonSchema::try_from(data.schema().as_ref()).unwrap(),
    }))
    .unwrap();
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = requests.clone();
    let table = Table::new_with_handler("my_table", move |request| {
        captured
            .lock()
            .unwrap()
            .push((request.url().path().to_string(), request.headers().clone()));
        match request.url().path() {
            "/v1/table/my_table/describe/" => http::Response::builder()
                .status(200)
                .body(describe_body.clone())
                .unwrap(),
            "/v1/table/my_table/insert/" => http::Response::builder()
                .status(200)
                .header("x-lancedb-version", "8")
                .body(r#"{"version":8}"#.to_string())
                .unwrap(),
            "/v1/table/my_table/count_rows/" => http::Response::builder()
                .status(200)
                .body("3".to_string())
                .unwrap(),
            path => panic!("unexpected path: {path}"),
        }
    });

    assert_eq!(table.add(data).execute().await.unwrap().version, 8);
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    let requests = requests.lock().unwrap();
    let insert_headers = &requests[1].1;
    assert_eq!(
        insert_headers
            .get("x-lancedb-min-read-version")
            .and_then(|value| value.to_str().ok()),
        Some("7")
    );
    let count_headers = &requests[2].1;
    assert_eq!(
        count_headers
            .get("x-lancedb-min-read-version")
            .and_then(|value| value.to_str().ok()),
        Some("8")
    );
}

#[tokio::test]
async fn test_read_version_watermark_keeps_max() {
    // Server reports 100 then a stale 50; the watermark must not regress.
    let (handler, requests) = read_version_handler(&["100", "50", "50"]);
    let table = Table::new_with_handler("my_table", handler);

    table.count_rows(None).await.unwrap();
    table.count_rows(None).await.unwrap();
    table.count_rows(None).await.unwrap();

    let reqs = requests.lock().unwrap();
    assert_eq!(
        reqs[2]
            .get("x-lancedb-min-read-version")
            .unwrap()
            .to_str()
            .unwrap(),
        "100"
    );
}

#[tokio::test]
async fn test_read_version_absent_header_no_watermark() {
    // An old server that doesn't return the version header leaves the
    // watermark unset, preserving backward compatibility.
    let (handler, requests) = read_version_handler(&[""]);
    let table = Table::new_with_handler("my_table", handler);

    table.count_rows(None).await.unwrap();
    table.count_rows(None).await.unwrap();

    let reqs = requests.lock().unwrap();
    assert!(!reqs[1].contains_key("x-lancedb-min-read-version"));
}

#[tokio::test]
async fn test_read_version_watermark_reset_on_checkout_latest() {
    let (handler, requests) = read_version_handler(&["100", "100"]);
    let table = Table::new_with_handler("my_table", handler);

    table.count_rows(None).await.unwrap();
    table.checkout_latest().await.unwrap();
    table.count_rows(None).await.unwrap();

    // The read after checkout_latest starts from a clean slate.
    let reqs = requests.lock().unwrap();
    assert!(
        !reqs
            .last()
            .unwrap()
            .contains_key("x-lancedb-min-read-version")
    );
}

/// Like `capturing_handler`, but keeps a per-path snapshot of the headers
/// from every request so tests can assert on a specific endpoint.
#[allow(clippy::type_complexity)]
fn path_capturing_handler<F>(
    body_for: F,
) -> (
    impl Fn(reqwest::Request) -> http::Response<String> + Clone + Send + Sync + 'static,
    Arc<std::sync::Mutex<HashMap<String, http::HeaderMap>>>,
)
where
    F: Fn(&str) -> String + Clone + Send + Sync + 'static,
{
    let captured: Arc<std::sync::Mutex<HashMap<String, http::HeaderMap>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let captured_c = captured.clone();
    let handler = move |request: reqwest::Request| {
        let path = request.url().path().to_string();
        captured_c
            .lock()
            .unwrap()
            .insert(path.clone(), request.headers().clone());
        http::Response::builder()
            .status(200)
            .body(body_for(&path))
            .unwrap()
    };
    (handler, captured)
}

#[tokio::test]
async fn test_freshness_checkout_validation_sends_no_min_version() {
    // After a write bumps min_version, calling checkout(v) must not let
    // that stale header ride along on the validating /describe/ request.
    let (handler, captured) = path_capturing_handler(|path| match path {
        "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
        "/v1/table/my_table/describe/" => r#"{"version":5,"schema":{"fields":[]}}"#.to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    let table = Table::new_with_handler("my_table", handler);

    table.update().column("a", "a + 1").execute().await.unwrap();
    table.checkout(5).await.unwrap();

    let captured = captured.lock().unwrap();
    let describe_headers = captured
        .get("/v1/table/my_table/describe/")
        .expect("describe should have been called by checkout(v)");
    assert!(
        !describe_headers.contains_key("x-lancedb-min-version"),
        "checkout(v) describe must not carry stale min_version",
    );
    assert!(!describe_headers.contains_key("x-lancedb-min-timestamp"));
}

#[tokio::test]
async fn test_freshness_checkout_tag_resolve_sends_no_min_version() {
    // Same invariant for checkout_tag: the tag-resolve request must not
    // pick up a stale min_version from a prior write.
    let (handler, captured) = path_capturing_handler(|path| match path {
        "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
        "/v1/table/my_table/tags/version/" => r#"{"version":5}"#.to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    let table = Table::new_with_handler("my_table", handler);

    table.update().column("a", "a + 1").execute().await.unwrap();
    table.checkout_tag("v_initial").await.unwrap();

    let captured = captured.lock().unwrap();
    let resolve_headers = captured
        .get("/v1/table/my_table/tags/version/")
        .expect("tags/version should have been called by checkout_tag");
    assert!(
        !resolve_headers.contains_key("x-lancedb-min-version"),
        "checkout_tag resolve must not carry stale min_version",
    );
    assert!(!resolve_headers.contains_key("x-lancedb-min-timestamp"));
}

#[tokio::test]
async fn test_freshness_checkout_clears_min_version() {
    let (handler, captured) = capturing_handler(|path| match path {
        "/v1/table/my_table/update/" => r#"{"rows_updated":1,"version":7}"#.to_string(),
        // checkout(5) needs to describe version 5 first
        "/v1/table/my_table/describe/" => r#"{"version":5,"schema":{"fields":[]}}"#.to_string(),
        "/v1/table/my_table/count_rows/" => "42".to_string(),
        _ => panic!("unexpected path: {}", path),
    });
    let table = Table::new_with_handler("my_table", handler);

    table.update().column("a", "a + 1").execute().await.unwrap();
    table.checkout(5).await.unwrap();
    table.count_rows(None).await.unwrap();

    let headers = captured.lock().unwrap().clone().unwrap();
    assert!(!headers.contains_key("x-lancedb-min-version"));
    assert!(!headers.contains_key("x-lancedb-min-timestamp"));
}
