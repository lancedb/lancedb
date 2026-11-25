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

/// How many times `open` is sent before a gateway or connection failure is
/// returned, and the wait before the second attempt, which doubles after.
const OPEN_ATTEMPTS: u32 = 4;
const OPEN_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// Marks a query as cache-only.
pub(super) const CACHE_ONLY_HEADER: &str = "x-lancedb-cache-only";

/// Turn a cache-only query's `412 not_resident` into [`Error::NotResident`]
/// and its `412 plan_executor_unavailable` into
/// [`Error::PlanExecutorUnavailable`]. Any other response passes through for
/// the usual handling.
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
        Ok(ErrorBody {
            error,
            error_code: Some(code),
        }) if code == error_codes::PLAN_EXECUTOR_UNAVAILABLE => {
            Err(Error::PlanExecutorUnavailable {
                message: error.unwrap_or(text),
                request_id: request_id.to_string(),
            })
        }
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

/// A handle to a run opened elsewhere, without contacting the server.
pub(super) fn attach<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    run_id: String,
    version: u64,
    wave_positions: Option<u64>,
) -> PrewarmRun {
    let transport = RemotePrewarmRunTransport {
        client: client.clone(),
        identifier: identifier.to_string(),
    };
    PrewarmRun::new(Arc::new(transport), run_id, version, wave_positions)
}

/// Open a run with `POST /v1/table/{id}/prewarm_run/`.
pub(super) async fn open<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    mut body: Value,
    options: &PrewarmRunOptions,
) -> Result<PrewarmRun> {
    body["run_key"] = json!(options.run_key);
    body["version"] = json!(options.version);
    body["columns"] = json!(options.columns);
    body["world_size"] = json!(options.world_size);
    // Sent only when set, so a server that predates the field sees the body
    // it always has.
    if options.row_addresses {
        body["row_addresses"] = json!(true);
    }
    if let Some(num_splits) = options.num_splits {
        body["num_splits"] = json!(num_splits);
    }
    // Opening is safe to repeat: the same `run_key` finds the same run. It
    // does not reach the run's coordinator, so a few attempts cover a
    // gateway blip and the long deadline of later calls is not needed.
    let mut backoff = OPEN_RETRY_BACKOFF;
    let mut attempt = 1;
    loop {
        match open_once(client, identifier, &body).await {
            Err((error, true)) if attempt < OPEN_ATTEMPTS => {
                log::warn!("opening a prewarm run failed, retrying: {error}");
                tokio::time::sleep(backoff).await;
                backoff *= 2;
                attempt += 1;
            }
            Err((error, _)) => return Err(error),
            Ok(run) => return Ok(run),
        }
    }
}

/// One attempt at `open`. The flag on an error says whether another attempt
/// may succeed.
///
/// The client's own retries are not used here: they repeat a 409 until they
/// give up and return a retry error, which would hide `run_key_conflict`.
async fn open_once<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    identifier: &str,
    body: &Value,
) -> std::result::Result<PrewarmRun, (Error, bool)> {
    #[derive(Deserialize)]
    struct Reply {
        run_id: String,
        version: u64,
        wave_positions: u64,
    }
    let request = client
        .post(&format!("/v1/table/{identifier}/prewarm_run/"))
        .json(body);
    let (request_id, response) = client.send(request).await.map_err(|error| (error, true))?;
    let status = response.status();
    if status.is_success() {
        let reply: Reply = response.json().await.map_err(|error| {
            let error = Error::Http {
                source: Box::new(error),
                request_id,
                status_code: Some(status),
            };
            (error, false)
        })?;
        return Ok(attach(
            client,
            identifier,
            reply.run_id,
            reply.version,
            Some(reply.wave_positions),
        ));
    }
    let text = response.text().await.unwrap_or_default();
    let body = serde_json::from_str::<ErrorBody>(&text).ok();
    let code = body.as_ref().and_then(|body| body.error_code.clone());
    Err(match code {
        // A 404 without a code is a server that does not know the route.
        None if status == StatusCode::NOT_FOUND => (
            Error::NotSupported {
                message: "this server does not support prewarm runs".into(),
            },
            false,
        ),
        Some(code) => (
            Error::PrewarmRun {
                run_id: String::new(),
                code,
                message: body.and_then(|body| body.error).unwrap_or(text),
            },
            false,
        ),
        None => (
            Error::Http {
                source: format!("{status}: {text}").into(),
                request_id,
                status_code: Some(status),
            },
            is_gateway_error(status),
        ),
    })
}

/// A status a gateway returns when it could not reach the server, as
/// opposed to one the server chose.
fn is_gateway_error(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
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
        if is_gateway_error(status) {
            Err(CallError::Transient(error))
        } else {
            Err(CallError::Other(error))
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
        PrewarmRunOptions::new("job-7-attempt-0-epoch-0", 42, 2).with_columns(["image"])
    }

    /// A script with an open run and a join whose heartbeat ticks an hour
    /// apart, so the only heartbeats are the ones a newly read wave sends.
    fn opened() -> Script {
        Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(3_600_000))
            .reply("heartbeat", 200, view(3_600_000))
    }

    #[tokio::test]
    async fn open_sends_the_run_options() {
        let script = opened();
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        assert_eq!(run.run_id(), "run-1");
        assert_eq!(run.version(), 42);
        assert_eq!(run.wave_positions(), Some(64));
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
    async fn open_says_when_ranges_are_cut_from_row_addresses() {
        let script = opened();
        let options = options().with_row_addresses(true);
        script.table().open_prewarm_run(options).await.unwrap();
        assert_eq!(script.calls("open")[0]["row_addresses"], json!(true));
    }

    #[tokio::test]
    async fn open_sends_the_split_count_only_when_given() {
        let script = opened();
        script.table().open_prewarm_run(options()).await.unwrap();
        assert!(script.calls("open")[0].get("num_splits").is_none());

        let script = opened();
        let options = options().with_num_splits(Some(128));
        script.table().open_prewarm_run(options).await.unwrap();
        assert_eq!(script.calls("open")[0]["num_splits"], json!(128));
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
        // No run was opened, so the message names none.
        assert_eq!(
            error.to_string(),
            "Prewarm run: server says run_key_conflict"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn open_retries_a_gateway_error() {
        let script = Script::default()
            .reply("open", 502, json!("bad gateway"))
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 4096 }),
            );
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        assert_eq!(run.run_id(), "run-1");
        assert_eq!(script.calls("open").len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn open_gives_up_on_a_gateway_that_stays_down() {
        let script = Script::default().reply("open", 502, json!("bad gateway"));
        let error = script
            .table()
            .open_prewarm_run(options())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Http { .. }), "{error:?}");
        assert_eq!(script.calls("open").len(), 4);
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
            // A state from a newer server is not ready, so the wait goes on.
            .reply(
                "wave_status",
                200,
                json!({ "wave": 0, "status": "a_state_added_later" }),
            )
            .reply("wave_status", 200, json!({ "wave": 0, "status": "ready" }));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1, 0).await.unwrap();
        rank.wait_ready(0, Duration::from_secs(60)).await.unwrap();
        let polls = script.calls("wave_status");
        assert_eq!(polls.len(), 4);
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
        let rank = run.join(0, 0).await.unwrap();
        rank.wait_ready(3, Duration::from_secs(60)).await.unwrap();
        assert_eq!(script.calls("wave_status").len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_times_out_while_the_supervisor_is_away() {
        let script = opened().reply("wave_status", 503, error(error_codes::NOT_READY));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
        let error = rank
            .wait_ready(0, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }), "{error:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn close_does_not_wait_out_a_supervisor_that_is_away() {
        let script = opened().reply("close", 503, error(error_codes::NOT_READY));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
        let started = tokio::time::Instant::now();
        let error = rank.close().await.unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }), "{error:?}");
        assert!(started.elapsed() <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_dropped_rank_joins_again_and_retries() {
        let script = opened()
            .reply("wave_status", 409, error(error_codes::NOT_JOINED))
            .reply("wave_status", 200, json!({ "wave": 0, "status": "ready" }));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1, 3).await.unwrap();
        rank.wait_ready(5, Duration::from_secs(60)).await.unwrap();
        // The first join carries the rank's first wave, and the second the
        // wave it had reached when the server dropped it.
        assert_eq!(
            script.calls("join"),
            vec![
                json!({ "rank": 1, "current_wave": 3 }),
                json!({ "rank": 1, "current_wave": 5 }),
            ]
        );
        assert_eq!(script.calls("wave_status").len(), 2);
    }

    #[tokio::test]
    async fn a_failed_run_raises_with_the_server_diagnostic() {
        let script = opened().reply("wave_status", 410, error(error_codes::RUN_FAILED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
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
        let rank = run.join(0, 0).await.unwrap();
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
        let rank = run.join(0, 0).await.unwrap();
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
        let rank = run.join(0, 0).await.unwrap();
        let error = rank.announce(vec![]).await.unwrap_err();
        assert!(
            matches!(&error, Error::PrewarmRun { code, .. } if code == error_codes::BEYOND_HORIZON),
            "{error:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_report_the_wave_being_read() {
        let script = Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(1_000))
            .reply("wave_status", 200, json!({ "wave": 4, "status": "ready" }))
            .reply("heartbeat", 200, view(1_000));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(1, 0).await.unwrap();
        rank.wait_ready(4, Duration::from_secs(60)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        // One sent when the wave was first read, then one a tick.
        let beats = script.calls("heartbeat");
        assert_eq!(beats.len(), 3, "{beats:?}");
        for beat in beats {
            assert_eq!(beat, json!({ "rank": 1, "current_wave": 4 }));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeats_stop_when_the_run_ends() {
        let script = Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(1_000))
            .reply("heartbeat", 410, error(error_codes::RUN_RELEASED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let _rank = run.join(0, 0).await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(script.calls("heartbeat").len(), 1);
    }

    #[tokio::test]
    async fn closing_an_ended_run_succeeds() {
        let script = opened().reply("close", 410, error(error_codes::RUN_RELEASED));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
        rank.close().await.unwrap();
        assert_eq!(script.calls("close"), vec![json!({ "rank": 0 })]);
    }

    /// A table whose queries answer 412 `not_resident` when cache-only and
    /// 400 otherwise, recording each query's cache-only header.
    fn cache_only_table(headers: Arc<Mutex<Vec<Option<String>>>>) -> Table {
        cache_only_table_refusing(headers, error_codes::NOT_RESIDENT)
    }

    /// Like [`cache_only_table`], with `code` as the 412's error code.
    fn cache_only_table_refusing(
        headers: Arc<Mutex<Vec<Option<String>>>>,
        code: &'static str,
    ) -> Table {
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
                    (412, error(code))
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

    /// The server serves only takes cache-only and refuses a scan, so
    /// setting up a reader over a cache-only table must not send one. The
    /// handler panics on any query.
    #[tokio::test]
    async fn a_permutation_reader_is_set_up_over_a_cache_only_table_without_a_query() {
        use crate::dataloader::permutation::reader::PermutationReader;
        use crate::query::Select;

        let table = Table::new_with_handler("my_table", |request| {
            let body = match request.url().path() {
                "/v1/table/my_table/describe/" => json!({
                    "version": 42,
                    "schema": {"fields": [
                        {"name": "id", "type": {"type": "int64"}, "nullable": true},
                        {"name": "v", "type": {"type": "int64"}, "nullable": true}
                    ]},
                }),
                "/v1/table/my_table/count_rows/" => json!(10),
                path => panic!("unexpected request: {path}"),
            };
            http::Response::builder()
                .status(200)
                .body(body.to_string())
                .unwrap()
        });
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();
        let reader = PermutationReader::identity(cache_only.base_table().clone())
            .await
            .unwrap();

        let names = |schema: arrow_schema::SchemaRef| -> Vec<String> {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        };
        let all = reader.output_schema(Select::All).await.unwrap();
        assert_eq!(names(all), ["id", "v"]);
        let columns = Select::Columns(vec!["v".to_string(), "id".to_string()]);
        assert_eq!(
            names(reader.output_schema(columns).await.unwrap()),
            ["v", "id"]
        );
        // The form the Python bindings send for a plain column.
        let dynamic = Select::Dynamic(vec![("v".to_string(), "v".to_string())]);
        assert_eq!(names(reader.output_schema(dynamic).await.unwrap()), ["v"]);
    }

    #[tokio::test]
    async fn a_read_no_plan_executor_can_serve_is_not_reported_as_a_miss() {
        use crate::query::ExecutableQuery;

        let table = cache_only_table_refusing(
            Arc::new(Mutex::new(Vec::new())),
            error_codes::PLAN_EXECUTOR_UNAVAILABLE,
        );
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();

        let error = cache_only.query().execute().await.err().unwrap();
        match error {
            Error::PlanExecutorUnavailable { message, .. } => {
                assert_eq!(message, "server says plan_executor_unavailable")
            }
            other => panic!("unexpected error: {other:?}"),
        }
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
    async fn a_cache_only_handle_cannot_be_moved() {
        let table = cache_only_table(Arc::new(Mutex::new(Vec::new())));
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();

        for error in [
            cache_only.checkout(41).await.err().unwrap(),
            cache_only.checkout_latest().await.err().unwrap(),
            cache_only.checkout_tag("v1").await.err().unwrap(),
            cache_only.checkout_branch("exp", None).await.err().unwrap(),
        ] {
            assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
        }
        assert_eq!(cache_only.version().await.unwrap(), 42);
        // The handle it was made from still moves.
        table.checkout_latest().await.unwrap();
    }

    #[tokio::test]
    async fn a_cache_only_handle_can_be_checked_out_where_it_already_is() {
        use crate::query::ExecutableQuery;

        let table = cache_only_table(Arc::new(Mutex::new(Vec::new())));
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();

        let same = cache_only.checkout_branch("main", Some(42)).await.unwrap();
        assert_eq!(same.version().await.unwrap(), 42);
        // Still cache-only: its query is the one the handler refuses.
        let error = same.query().execute().await.err().unwrap();
        assert!(matches!(error, Error::NotResident { .. }), "{error:?}");

        for error in [
            cache_only.checkout_branch("main", Some(41)).await,
            cache_only.checkout_branch("main", None).await,
            cache_only.checkout_branch("exp", Some(42)).await,
        ] {
            let error = error.err().unwrap();
            assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
        }
    }

    /// A permutation records the version it was built against, and its
    /// reader checks the base table out there. Under a run that base table
    /// is a cache-only handle already at that version.
    #[tokio::test]
    async fn a_permutation_reader_pins_a_cache_only_table_to_the_version_it_reads() {
        use crate::dataloader::permutation::builder::BASE_VERSION_CONFIG_KEY;
        use crate::dataloader::permutation::reader::PermutationReader;
        use arrow_array::{RecordBatch, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};

        let permutation = |base_version: u64| async move {
            let metadata = [(
                BASE_VERSION_CONFIG_KEY.to_string(),
                base_version.to_string(),
            )];
            let schema = Schema::new(vec![
                Field::new("row_id", DataType::UInt64, false),
                Field::new("split_id", DataType::UInt64, false),
            ])
            .with_metadata(metadata.into());
            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(UInt64Array::from(vec![0, 1, 2])),
                    Arc::new(UInt64Array::from(vec![0, 0, 0])),
                ],
            )
            .unwrap();
            let db = crate::connect("memory:///").execute().await.unwrap();
            db.create_table("permutation", batch)
                .execute()
                .await
                .unwrap()
        };

        let table = cache_only_table(Arc::new(Mutex::new(Vec::new())));
        table.checkout(42).await.unwrap();
        let cache_only = table.cache_only().await.unwrap();

        PermutationReader::try_from_tables(
            cache_only.base_table().clone(),
            permutation(42).await.base_table().clone(),
            0,
        )
        .await
        .unwrap();

        // A permutation built at another version names rows the run did not warm.
        let error = PermutationReader::try_from_tables(
            cache_only.base_table().clone(),
            permutation(41).await.base_table().clone(),
            0,
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn cache_only_needs_a_checked_out_version() {
        let table = cache_only_table(Arc::new(Mutex::new(Vec::new())));
        let error = table.cache_only().await.err().unwrap();
        assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn a_reader_waits_without_joining() {
        // No join or open is scripted: attaching and reading must not call them.
        let script = Script::default()
            .reply(
                "wave_status",
                200,
                json!({ "wave": 2, "status": "warming" }),
            )
            .reply("wave_status", 200, json!({ "wave": 2, "status": "ready" }))
            .reply("rewarm", 200, json!({}));
        let table = script.table();
        let reader = table.attach_prewarm_run("run-1", 42).unwrap().reader(3);
        reader.wait_ready(2, Duration::from_secs(60)).await.unwrap();
        let range = RowRange {
            fragment_id: 1,
            start: 0,
            end: 4,
        };
        reader.rewarm(2, vec![range]).await.unwrap();
        assert_eq!(script.calls("wave_status")[0]["rank"], 3);
        assert_eq!(script.calls("rewarm").len(), 1);
        assert!(script.calls("join").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reader_retries_while_its_rank_is_dropped() {
        let script = Script::default()
            .reply("wave_status", 409, error(error_codes::NOT_JOINED))
            .reply("wave_status", 409, error(error_codes::NOT_JOINED))
            .reply("wave_status", 200, json!({ "wave": 0, "status": "ready" }));
        let table = script.table();
        let reader = table.attach_prewarm_run("run-1", 42).unwrap().reader(1);
        reader.wait_ready(0, Duration::from_secs(60)).await.unwrap();
        assert_eq!(script.calls("wave_status").len(), 3);
        assert!(script.calls("join").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn reported_waves_reach_the_heartbeat() {
        let script = Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(1_000))
            .reply("heartbeat", 200, view(1_000));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
        rank.report_wave(5);
        rank.report_wave(3);
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        // One sent when wave 5 was reported, then the tick.
        let beat = json!({ "rank": 0, "current_wave": 5 });
        assert_eq!(script.calls("heartbeat"), vec![beat.clone(), beat]);
    }

    const HOUR_MS: u64 = 3_600_000;

    #[tokio::test(start_paused = true)]
    async fn a_higher_reported_wave_is_sent_without_waiting_for_the_tick() {
        let script = Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(HOUR_MS))
            .reply("heartbeat", 200, view(HOUR_MS));
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();

        rank.report_wave(3);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            script.calls("heartbeat"),
            vec![json!({ "rank": 0, "current_wave": 3 })]
        );

        // The same wave, or a lower one, is nothing new to send.
        rank.report_wave(3);
        rank.report_wave(1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(script.calls("heartbeat").len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn refreshing_the_view_sends_a_heartbeat_and_records_its_reply() {
        let mut moved_on = view(HOUR_MS);
        moved_on["reading_wave"] = json!(7);
        let script = Script::default()
            .reply(
                "open",
                200,
                json!({ "run_id": "run-1", "version": 42, "wave_positions": 64 }),
            )
            .reply("join", 200, view(HOUR_MS))
            .reply("heartbeat", 200, moved_on);
        let run = script.table().open_prewarm_run(options()).await.unwrap();
        let rank = run.join(0, 0).await.unwrap();
        let before = rank.view();

        let refreshed = rank.refresh_view().await.unwrap();
        assert_eq!(script.calls("heartbeat").len(), 1);
        assert_ne!(refreshed, before);
        assert_eq!(rank.view(), refreshed);
    }
}
