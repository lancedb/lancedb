// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Prewarm run calls over the table REST API.

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::{Error, Result};
use crate::prewarm_run::{
    CallError, PrewarmRun, PrewarmRunOptions, PrewarmRunTransport, error_codes,
};
use crate::remote::client::{HttpSend, RestfulLanceDbClient};

/// The JSON error body the server returns for a refused call.
#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_code: Option<String>,
}

/// Marks a query as cache-only.
pub(super) const CACHE_ONLY_HEADER: &str = "x-lancedb-cache-only";

/// Turn a cache-only query's `412 not_resident` into [`Error::NotResident`].
/// Any other response passes through for the usual handling.
pub(super) async fn reject_not_resident(
    request_id: &str,
    response: reqwest::Response,
) -> Result<reqwest::Response> {
    let status = response.status();
    if status != StatusCode::PRECONDITION_FAILED {
        return Ok(response);
    }
    let text = response.text().await.unwrap_or_default();
    match serde_json::from_str::<ErrorBody>(&text) {
        Ok(ErrorBody {
            error,
            error_code: Some(code),
        }) if code == error_codes::NOT_RESIDENT => Err(Error::NotResident {
            message: error.unwrap_or(text),
            request_id: request_id.to_string(),
        }),
        _ => Err(Error::Http {
            source: format!("{status}: {text}").into(),
            request_id: request_id.to_string(),
            status_code: Some(status),
        }),
    }
}

#[derive(Debug)]
pub(super) struct RemotePrewarmRunTransport<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    identifier: String,
}

/// Open a run with `POST /v1/table/{id}/prewarm_run/`.
pub(super) async fn open<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    mut body: Value,
    options: &PrewarmRunOptions,
) -> Result<PrewarmRun> {
    #[derive(Deserialize)]
    struct Reply {
        run_id: String,
        version: u64,
    }
    body["run_key"] = json!(options.run_key);
    body["version"] = json!(options.version);
    body["columns"] = json!(options.columns);
    body["world_size"] = json!(options.world_size);
    let request = client
        .post(&format!("/v1/table/{identifier}/prewarm_run/"))
        .json(&body);
    let (request_id, response) = client.send(request).await?;
    let status = response.status();
    if status.is_success() {
        let reply: Reply = response.json().await.map_err(|error| Error::Http {
            source: Box::new(error),
            request_id,
            status_code: Some(status),
        })?;
        let transport = RemotePrewarmRunTransport {
            client: client.clone(),
            identifier: identifier.to_string(),
        };
        return Ok(PrewarmRun::new(
            Arc::new(transport),
            reply.run_id,
            reply.version,
        ));
    }
    let text = response.text().await.unwrap_or_default();
    let body = serde_json::from_str::<ErrorBody>(&text).ok();
    let code = body.as_ref().and_then(|body| body.error_code.clone());
    match code {
        // A 404 without a code is a server that does not know the route.
        None if status == StatusCode::NOT_FOUND => Err(Error::NotSupported {
            message: "this server does not support prewarm runs".into(),
        }),
        Some(code) => Err(Error::PrewarmRun {
            run_id: String::new(),
            code,
            message: body.and_then(|body| body.error).unwrap_or(text),
        }),
        None => Err(Error::Http {
            source: format!("{status}: {text}").into(),
            request_id,
            status_code: Some(status),
        }),
    }
}

#[async_trait]
impl<S: HttpSend> PrewarmRunTransport for RemotePrewarmRunTransport<S> {
    async fn call(
        &self,
        run_id: &str,
        call: &str,
        body: Value,
    ) -> std::result::Result<Value, CallError> {
        let run_id = urlencoding::encode(run_id);
        let request = self
            .client
            .post(&format!(
                "/v1/table/{}/prewarm_run/{run_id}/{call}/",
                self.identifier
            ))
            .json(&body);
        let (request_id, response) = self
            .client
            .send(request)
            .await
            .map_err(CallError::Transient)?;
        let status = response.status();
        let text = response.text().await.map_err(|error| {
            CallError::Transient(Error::Http {
                source: Box::new(error),
                request_id: request_id.clone(),
                status_code: Some(status),
            })
        })?;
        if status.is_success() {
            if text.trim().is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_str(&text).map_err(|error| {
                CallError::Other(Error::Runtime {
                    message: format!("unexpected `{call}` reply ({request_id}): {error}"),
                })
            });
        }
        if let Ok(ErrorBody {
            error,
            error_code: Some(code),
        }) = serde_json::from_str::<ErrorBody>(&text)
        {
            return Err(CallError::Run {
                status: status.as_u16(),
                code,
                message: error.unwrap_or(text),
            });
        }
        let error = Error::Http {
            source: format!("{status}: {text}").into(),
            request_id,
            status_code: Some(status),
        };
        match status {
            StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Err(CallError::Transient(error)),
            _ => Err(CallError::Other(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::Table;
    use crate::prewarm_run::{RowRange, WaveAnnouncement};

    /// Status and body replies queued for one call.
    type Replies = VecDeque<(u16, String)>;

    /// A recorded request: the call name (last path segment) and its body.
    type Seen = Arc<Mutex<Vec<(String, Value)>>>;

    /// Replies per call name, served in order; the last one repeats.
    #[derive(Clone, Default)]
    struct Script {
        replies: Arc<Mutex<Vec<(String, Replies)>>>,
        seen: Seen,
    }

    impl Script {
        fn reply(self, call: &str, status: u16, body: Value) -> Self {
            {
                let mut replies = self.replies.lock().unwrap();
                let body = body.to_string();
                match replies.iter_mut().find(|(name, _)| name == call) {
                    Some((_, queue)) => queue.push_back((status, body)),
                    None => replies.push((call.to_string(), VecDeque::from([(status, body)]))),
                }
            }
            self
        }

        fn table(&self) -> Table {
            let script = self.clone();
            Table::new_with_handler("my_table", move |request| {
                let path = request.url().path().trim_end_matches('/').to_string();
                let call = if path == "/v1/table/my_table/prewarm_run" {
                    "open".to_string()
                } else {
                    let prefix = "/v1/table/my_table/prewarm_run/run-1/";
                    path.strip_prefix(prefix)
                        .unwrap_or_else(|| panic!("unexpected request: {path}"))
                        .to_string()
                };
                let body: Value = request
                    .body()
                    .and_then(|body| body.as_bytes())
                    .map(|bytes| serde_json::from_slice(bytes).unwrap())
                    .unwrap_or(Value::Null);
                script.seen.lock().unwrap().push((call.clone(), body));
                let mut replies = script.replies.lock().unwrap();
                let (_, queue) = replies
                    .iter_mut()
                    .find(|(name, _)| *name == call)
                    .unwrap_or_else(|| panic!("no reply scripted for {call}"));
                let (status, body) = if queue.len() > 1 {
                    queue.pop_front().unwrap()
                } else {
                    queue.front().unwrap().clone()
                };
                http::Response::builder().status(status).body(body).unwrap()
            })
        }

        fn calls(&self, call: &str) -> Vec<Value> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, _)| name == call)
                .map(|(_, body)| body.clone())
                .collect()
        }
    }

    fn view(heartbeat_interval_ms: u64) -> Value {
        json!({
            "status": "open",
            "reading_wave": 0,
            "highest_announced": null,
            "live_ranks": 1,
            "horizon": 2,
            "wave_positions": 64,
            "heartbeat_interval_ms": heartbeat_interval_ms,
        })
    }

    fn error(code: &str) -> Value {
        json!({ "code": 0, "error": format!("server says {code}"), "error_code": code })
    }

    fn options() -> PrewarmRunOptions {
        PrewarmRunOptions {
            run_key: "job-7-attempt-0-epoch-0".into(),
            version: 42,
            columns: vec!["image".into()],
            world_size: 2,
        }
    }

    /// A script with an open run and a join whose heartbeats stay quiet.
    fn opened() -> Script {
        Script::default()
            .reply("open", 200, json!({ "run_id": "run-1", "version": 42 }))
            .reply("join", 200, view(3_600_000))
    }

    #[tokio::test]
    async fn open_sends_the_run_options() {
        let script = opened();
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        assert_eq!(run.run_id(), "run-1");
        assert_eq!(run.version(), 42);
        assert_eq!(
            script.calls("open"),
            vec![json!({
                "run_key": "job-7-attempt-0-epoch-0",
                "version": 42,
                "columns": ["image"],
                "world_size": 2,
            })]
        );
    }

    #[tokio::test]
    async fn open_without_run_support_is_not_supported() {
        let script = Script::default().reply("open", 404, json!({ "error": "no route" }));
        let error = script
            .table()
            .open_prewarm_run(options())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn open_reports_a_run_key_conflict() {
        let script = Script::default().reply("open", 409, error(error_codes::RUN_KEY_CONFLICT));
        let error = script
            .table()
            .open_prewarm_run(options())
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::PrewarmRun { code, .. } if code == error_codes::RUN_KEY_CONFLICT),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn wait_ready_polls_until_the_wave_is_ready() {
        let script = opened()
            .reply(
                "wave_status",
                200,
                json!({ "wave": 0, "status": "not_announced" }),
            )
            .reply(
                "wave_status",
                200,
                json!({ "wave": 0, "status": "warming" }),
            )
            .reply("wave_status", 200, json!({ "wave": 0, "status": "ready" }));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1).await.unwrap();
        rank.wait_ready(0, Duration::from_secs(60)).await.unwrap();
        let polls = script.calls("wave_status");
        assert_eq!(polls.len(), 3);
        assert_eq!(polls[0]["rank"], 1);
        assert_eq!(polls[0]["wave"], 0);
        assert!(polls[0]["wait_ms"].as_u64().unwrap() <= 20_000);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_retries_not_ready_and_gateway_errors() {
        let script = opened()
            .reply("wave_status", 503, error(error_codes::NOT_READY))
            .reply("wave_status", 502, json!("bad gateway"))
            .reply("wave_status", 200, json!({ "wave": 3, "status": "ready" }));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        rank.wait_ready(3, Duration::from_secs(60)).await.unwrap();
        assert_eq!(script.calls("wave_status").len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_times_out_while_the_supervisor_is_away() {
        let script = opened().reply("wave_status", 503, error(error_codes::NOT_READY));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        let error = rank
            .wait_ready(0, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn a_dropped_rank_joins_again_and_retries() {
        let script = opened()
            .reply("wave_status", 409, error(error_codes::NOT_JOINED))
            .reply("wave_status", 200, json!({ "wave": 0, "status": "ready" }));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1).await.unwrap();
        rank.wait_ready(0, Duration::from_secs(60)).await.unwrap();
        assert_eq!(script.calls("join").len(), 2);
        assert_eq!(script.calls("wave_status").len(), 2);
    }

    #[tokio::test]
    async fn a_failed_run_raises_with_the_server_diagnostic() {
        let script = opened().reply("wave_status", 410, error(error_codes::RUN_FAILED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        let error = rank
            .wait_ready(0, Duration::from_secs(60))
            .await
            .unwrap_err();
        match error {
            Error::PrewarmRun {
                run_id,
                code,
                message,
            } => {
                assert_eq!(run_id, "run-1");
                assert_eq!(code, error_codes::RUN_FAILED);
                assert_eq!(message, "server says run_failed");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_consumed_wave_raises() {
        let script = opened().reply(
            "wave_status",
            200,
            json!({ "wave": 0, "status": "consumed" }),
        );
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        let error = rank
            .wait_ready(0, Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::PrewarmRun { code, .. } if code == error_codes::WAVE_CONSUMED),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn announce_and_rewarm_send_row_ranges() {
        let script = opened()
            .reply("announce", 200, json!({ "highest_announced": 1 }))
            .reply("rewarm", 200, json!({}));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        let range = RowRange {
            fragment_id: 3,
            start: 10,
            end: 20,
        };
        let highest = rank
            .announce(vec![
                WaveAnnouncement {
                    wave: 0,
                    ranges: vec![range],
                },
                WaveAnnouncement {
                    wave: 1,
                    ranges: vec![],
                },
            ])
            .await
            .unwrap();
        assert_eq!(highest, Some(1));
        rank.rewarm(0, vec![range]).await.unwrap();
        let range = json!({ "fragment_id": 3, "start": 10, "end": 20 });
        assert_eq!(
            script.calls("announce"),
            vec![json!({
                "rank": 0,
                "waves": [{ "wave": 0, "ranges": [range] }, { "wave": 1, "ranges": [] }],
            })]
        );
        assert_eq!(
            script.calls("rewarm"),
            vec![json!({ "rank": 0, "wave": 0, "ranges": [range] })]
        );
    }

    #[tokio::test]
    async fn a_rejected_announcement_raises() {
        let script = opened().reply("announce", 409, error(error_codes::BEYOND_HORIZON));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        let error = rank.announce(vec![]).await.unwrap_err();
        assert!(
            matches!(&error, Error::PrewarmRun { code, .. } if code == error_codes::BEYOND_HORIZON),
            "{error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_report_the_wave_being_read() {
        let script = Script::default()
            .reply("open", 200, json!({ "run_id": "run-1", "version": 42 }))
            .reply("join", 200, view(1_000))
            .reply("wave_status", 200, json!({ "wave": 4, "status": "ready" }))
            .reply("heartbeat", 200, view(1_000));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1).await.unwrap();
        rank.wait_ready(4, Duration::from_secs(60)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        let beats = script.calls("heartbeat");
        assert_eq!(beats.len(), 2, "{beats:?}");
        assert_eq!(beats[1], json!({ "rank": 1, "current_wave": 4 }));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_stop_when_the_run_ends() {
        let script = Script::default()
            .reply("open", 200, json!({ "run_id": "run-1", "version": 42 }))
            .reply("join", 200, view(1_000))
            .reply("heartbeat", 410, error(error_codes::RUN_RELEASED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let _rank = run.join(0).await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(script.calls("heartbeat").len(), 1);
    }

    #[tokio::test]
    async fn closing_an_ended_run_succeeds() {
        let script = opened().reply("close", 410, error(error_codes::RUN_RELEASED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0).await.unwrap();
        rank.close().await.unwrap();
        assert_eq!(script.calls("close"), vec![json!({ "rank": 0 })]);
    }

    /// A table whose queries answer 412 `not_resident` when cache-only and
    /// 400 otherwise, recording each query's cache-only header.
    fn cache_only_table(headers: Arc<Mutex<Vec<Option<String>>>>) -> Table {
        Table::new_with_handler("my_table", move |request| match request.url().path() {
            "/v1/table/my_table/describe/" => {
                let describe = json!({
                    "version": 42,
                    "schema": {"fields": [
                        {"name": "id", "type": {"type": "int64"}, "nullable": true}
                    ]},
                });
                http::Response::builder()
                    .status(200)
                    .body(describe.to_string())
                    .unwrap()
            }
            "/v1/table/my_table/query/" => {
                let header = request
                    .headers()
                    .get(CACHE_ONLY_HEADER)
                    .map(|value| value.to_str().unwrap().to_string());
                let (status, body) = if header.is_some() {
                    (412, error(error_codes::NOT_RESIDENT))
                } else {
                    (400, json!({ "error": "not a cache-only test" }))
                };
                headers.lock().unwrap().push(header);
                http::Response::builder()
                    .status(status)
                    .body(body.to_string())
                    .unwrap()
            }
            path => panic!("unexpected request: {path}"),
        })
    }

    #[tokio::test]
    async fn cache_only_queries_raise_not_resident() {
        use crate::query::ExecutableQuery;

        let headers = Arc::new(Mutex::new(Vec::new()));
        let table = cache_only_table(headers.clone());
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();
        assert_eq!(cache_only.version().await.unwrap(), 42);

        let error = cache_only.query().execute().await.err().unwrap();
        match error {
            Error::NotResident { message, .. } => assert_eq!(message, "server says not_resident"),
            other => panic!("unexpected error: {other:?}"),
        }
        // The original handle still reads normally.
        let error = table.query().execute().await.err().unwrap();
        assert!(!matches!(error, Error::NotResident { .. }), "{error:?}");
        assert_eq!(
            *headers.lock().unwrap(),
            vec![Some("true".to_string()), None]
        );
    }

    #[tokio::test]
    async fn cache_only_needs_a_checked_out_version() {
        let table = cache_only_table(Arc::new(Mutex::new(Vec::new())));
        let error = table.cache_only().await.err().unwrap();
        assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
    }
}
