// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use tokio::time::sleep;

use serde::Deserialize;

use crate::database::JobDescription;
use crate::error::{Error, JobFailure, Result};
use crate::job::{JobEventsRequest, JobHandle, JobProgressCallback, TerminalResult};
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};

/// Delay before the second job-state poll; doubles up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// A job's completion is seen at most this long after it happens. A 5 s cap
/// added 2.5 s on average to every short refresh, as much as the job itself.
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
enum JobState {
    InProgress,
    Cancelled,
    Failed,
    Done,
    /// A state this client version does not know; treated as still running
    /// and reported as-is if the job never settles.
    Other(String),
}

impl JobState {
    /// The client vocabulary label for this state.
    fn client_label(&self) -> String {
        match self {
            Self::InProgress => "running".to_string(),
            Self::Done => "finished".to_string(),
            Self::Failed => "failed".to_string(),
            Self::Cancelled => "cancelled".to_string(),
            Self::Other(state) => state.clone(),
        }
    }
}

impl From<&str> for JobState {
    fn from(state: &str) -> Self {
        match state {
            "IN_PROGRESS" | "in_progress" => Self::InProgress,
            "CANCELLED" | "cancelled" | "canceled" => Self::Cancelled,
            // The server reports a timed-out job as FAILED on describe;
            // accept the raw registry state too in case a future server
            // stops folding it.
            "FAILED" | "failed" | "TIMED_OUT" | "timed_out" => Self::Failed,
            "DONE" | "done" | "succeeded" => Self::Done,
            other => Self::Other(other.to_string()),
        }
    }
}

pub(super) fn job_state_to_client(state: &str) -> String {
    JobState::from(state).client_label()
}

/// The server's account of why a job failed. Absent from older servers, which
/// report only the terminal state.
#[derive(Clone, Deserialize)]
pub(super) struct ReportedFailure {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    traceback: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
    #[serde(default)]
    sql_job_id: Option<String>,
}

/// Forward-compatible `/v1/jobs/pause` wire envelope.
#[derive(Deserialize)]
pub(super) struct PauseJobResponse {
    /// False when the job was already paused, so a repeated pause changed
    /// nothing.
    #[serde(default)]
    pub(super) paused: bool,
    /// The job is finalizing its results and cannot be parked right now.
    #[serde(default)]
    pub(super) committing: bool,
}

/// Forward-compatible `/v1/jobs/resume` wire envelope.
#[derive(Deserialize)]
pub(super) struct ResumeJobResponse {
    #[serde(default)]
    pub(super) resumed: bool,
    /// The pause's worker drain is not confirmed yet.
    #[serde(default)]
    pub(super) still_pausing: bool,
}

/// Forward-compatible `/v1/jobs/describe` wire envelope.
#[derive(Clone, Deserialize)]
pub(super) struct DescribeJobResponse {
    #[serde(default)]
    pub(super) job_id: String,
    #[serde(default)]
    pub(super) job_type: String,
    pub(super) job_state: String,
    #[serde(default)]
    pub(super) creation_ms: i64,
    #[serde(default)]
    pub(super) progress: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) phase: Option<String>,
    #[serde(default)]
    pub(super) queue_reason: Option<String>,
    #[serde(default)]
    pub(super) waiting_for_job_id: Option<String>,
    #[serde(default)]
    pub(super) paused_at_millis: Option<i64>,
    #[serde(default)]
    pub(super) spec: serde_json::Value,
    #[serde(default)]
    pub(super) result: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) failure: Option<ReportedFailure>,
}

impl ReportedFailure {
    pub(super) fn into_job_failure(self) -> JobFailure {
        JobFailure {
            phase: self.phase,
            message: self.message,
            traceback: self.traceback,
            retryable: self.retryable,
            sql_job_id: self.sql_job_id,
            source: None,
        }
    }
}

impl DescribeJobResponse {
    fn state(&self) -> JobState {
        JobState::from(self.job_state.as_str())
    }

    fn into_terminal_result(self, request_id: String) -> TerminalResult {
        TerminalResult::remote(self.result, request_id)
    }

    /// The public description this wire envelope stands for.
    pub(super) fn into_description(self) -> JobDescription {
        JobDescription {
            job_id: self.job_id,
            job_type: self.job_type,
            state: JobState::from(self.job_state.as_str()).client_label(),
            creation_ms: self.creation_ms,
            progress: self.progress,
            phase: self.phase,
            queue_reason: self.queue_reason,
            waiting_for_job_id: self.waiting_for_job_id,
            paused_at_millis: self.paused_at_millis,
            spec: self.spec,
            result: self.result,
            failure: self.failure.map(ReportedFailure::into_job_failure),
        }
    }
}

/// One `/v1/jobs/query_events` round trip.
pub(super) async fn fetch_job_events<S: HttpSend>(
    client: &RestfulLanceDbClient<S>,
    body: serde_json::Value,
) -> Result<Vec<RecordBatch>> {
    let request = client.post("/v1/jobs/query_events").json(&body);
    let (request_id, response) = client.send(request).await?;
    let response = client.check_response(&request_id, response).await?;
    let bytes = response.bytes().await.err_to_http(request_id)?;
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let mut batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    // A query that matched nothing still describes the event columns.
    // Keep that schema so callers can build a typed empty result.
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(schema));
    }
    Ok(batches)
}

pub struct RemoteJob<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    job_id: String,
}

impl<S: HttpSend> RemoteJob<S> {
    pub fn new(client: RestfulLanceDbClient<S>, job_id: String) -> Self {
        Self { client, job_id }
    }

    /// One `/v1/jobs/describe` round trip.
    async fn fetch_description(&self) -> Result<(String, DescribeJobResponse)> {
        let request = self
            .client
            .post("/v1/jobs/describe")
            .json(&serde_json::json!({ "job_id": self.job_id }));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let description: DescribeJobResponse =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("failed to parse job description: {}", e).into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        Ok((request_id, description))
    }
}

#[async_trait]
impl<S: HttpSend> JobHandle for RemoteJob<S> {
    fn id(&self) -> Option<&str> {
        Some(&self.job_id)
    }

    async fn status(&self) -> Result<String> {
        Ok(self.fetch_description().await?.1.state().client_label())
    }

    async fn describe(&self) -> Result<JobDescription> {
        Ok(self.fetch_description().await?.1.into_description())
    }

    async fn timeline(&self) -> Result<serde_json::Value> {
        let request = self
            .client
            .post("/v1/jobs/timeline")
            .json(&serde_json::json!({"job_id": self.job_id}));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let trace: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| Error::Http {
                source: format!("failed to parse job timeline: {error}").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        if !trace
            .get("traceEvents")
            .is_some_and(serde_json::Value::is_array)
        {
            return Err(Error::Http {
                source: "server response is not Chrome Trace Event JSON; job timeline export requires server support".into(),
                request_id, status_code: None,
            });
        }
        Ok(trace)
    }

    async fn events(&self, request: JobEventsRequest) -> Result<Vec<RecordBatch>> {
        let mut body = serde_json::json!({ "job_id": self.job_id });
        if let Some(limit) = request.limit {
            body["limit"] = serde_json::Value::from(limit);
        }
        if let Some(filter) = request.filter {
            body["filter"] = serde_json::Value::String(filter);
        }
        fetch_job_events(&self.client, body).await
    }

    async fn wait(&self) -> Result<TerminalResult> {
        self.wait_with_progress(None).await
    }

    async fn wait_with_progress(
        &self,
        progress: Option<JobProgressCallback>,
    ) -> Result<TerminalResult> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let (request_id, description) = self.fetch_description().await?;
            if let Some(progress) = &progress {
                if let Ok(mut callback) = progress.lock() {
                    callback(&description.clone().into_description());
                } else {
                    log::warn!("job {} progress callback lock was poisoned", self.job_id);
                }
            }
            match description.state() {
                JobState::Done => return Ok(description.into_terminal_result(request_id)),
                JobState::Failed => {
                    let mut failure = description
                        .failure
                        .map(ReportedFailure::into_job_failure)
                        .unwrap_or_default();
                    // The responding query node may have upgraded since this job
                    // failed. Only its persisted terminal writer identifies the failure.
                    if let Some(server) = description
                        .result
                        .as_ref()
                        .and_then(|result| result.get("terminal_server"))
                        .and_then(|server| {
                            serde_json::from_value::<crate::server_info::ServerBuildInfo>(
                                server.clone(),
                            )
                            .ok()
                        })
                    {
                        let provenance = format!("[server: {server}]");
                        failure.message = Some(match failure.message {
                            Some(message) => format!("{message} {provenance}"),
                            None => provenance,
                        });
                    }
                    return Err(Error::JobFailed {
                        job_id: Some(self.job_id.clone()),
                        failure: Box::new(failure),
                    });
                }
                JobState::Cancelled => {
                    return Err(Error::JobCancelled {
                        job_id: Some(self.job_id.clone()),
                    });
                }
                JobState::InProgress => {}
                JobState::Other(ref state) => {
                    log::debug!("job {} is in unrecognized state {state}", self.job_id)
                }
            }
            sleep(interval).await;
            interval = (interval * 2).min(MAX_POLL_INTERVAL);
        }
    }

    async fn cancel(&self) -> Result<()> {
        let request = self
            .client
            .post("/v1/jobs/cancel")
            .json(&serde_json::json!({ "job_id": self.job_id }));
        let (request_id, response) = self.client.send(request).await?;
        self.client
            .check_response(&request_id, response)
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::Result;
    use crate::function::{FunctionVersion, RefreshColumnResult};
    use crate::job::{Job, JobHandle, JobProgressCallback, TerminalResult};
    use crate::remote::client::test_utils::client_with_handler;

    use super::{DescribeJobResponse, RemoteJob};

    const FUNCTION_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_function_job.json");
    const REFRESH_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_refresh_job.json");
    const UNIT_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_unit_job.json");
    const MISSING_RESULT_JOB: &str = r#"{"job_state":"DONE"}"#;

    struct FixtureRemoteJob(&'static str);

    #[async_trait]
    impl JobHandle for FixtureRemoteJob {
        async fn status(&self) -> Result<String> {
            Ok("finished".to_string())
        }

        async fn wait(&self) -> Result<TerminalResult> {
            let description: DescribeJobResponse =
                serde_json::from_str(self.0).expect("remote job fixture");
            Ok(description.into_terminal_result("fixture-request".to_string()))
        }

        async fn cancel(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn typed_remote_job_fixtures_decode_terminal_results() {
        let function = Job::<FunctionVersion>::new_typed(Box::new(FixtureRemoteJob(FUNCTION_JOB)));
        let result = function.wait().await.expect("typed FunctionVersion result");
        assert_eq!(result.version(), "1");

        let refresh =
            Job::<RefreshColumnResult>::new_typed(Box::new(FixtureRemoteJob(REFRESH_JOB)));
        let result = refresh.wait().await.expect("typed RefreshColumnResult");
        assert_eq!(result.rows_assigned, 999_998_800);
        assert_eq!(result.rows_filled(), result.rows_assigned);

        let unit = Job::new(Box::new(FixtureRemoteJob(UNIT_JOB)));
        unit.wait()
            .await
            .expect("unit result ignores additive remote payloads");
    }

    #[tokio::test]
    async fn typed_remote_job_requires_a_terminal_result() {
        let typed =
            Job::<RefreshColumnResult>::new_typed(Box::new(FixtureRemoteJob(MISSING_RESULT_JOB)));
        let error = typed.wait().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("successful typed job response did not contain a result")
        );
    }

    #[tokio::test]
    async fn reopened_job_preserves_its_description_and_decodes_the_terminal_result() {
        let description: DescribeJobResponse = serde_json::from_str(REFRESH_JOB).unwrap();
        let job = Job::opened(
            Box::new(FixtureRemoteJob(REFRESH_JOB)),
            description.into_description(),
        );
        let raw_result = job.result();
        let job = job.into_typed::<RefreshColumnResult>().unwrap();

        assert_eq!(job.job_type().as_deref(), Some("refresh_function_columns"));
        assert_eq!(job.state().as_deref(), Some("finished"));
        assert_eq!(job.creation_ms(), Some(1787270400001));
        assert_eq!(job.result(), raw_result);
        assert_eq!(job.wait().await.unwrap().rows_assigned, 999_998_800);
        assert_eq!(job.wait().await.unwrap().published_version, Some(919));

        let missing = Job::new(Box::new(FixtureRemoteJob(MISSING_RESULT_JOB)))
            .into_typed::<RefreshColumnResult>()
            .unwrap();
        assert!(missing.wait().await.is_err());
    }

    /// Five in-progress polls then done: 0.2 + 0.4 + 0.8 + 1 + 1 s of waiting.
    /// Under the old 5 s cap the same job would have waited 10 s.
    #[tokio::test(start_paused = true)]
    async fn wait_polls_at_most_every_second() {
        let describes = Arc::new(AtomicUsize::new(0));
        let seen = describes.clone();
        let client = client_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            let n = seen.fetch_add(1, Ordering::SeqCst);
            let body = if n < 5 {
                r#"{"job_state":"IN_PROGRESS"}"#.to_string()
            } else {
                UNIT_JOB.to_string()
            };
            http::Response::builder().status(200).body(body).unwrap()
        });
        let started = tokio::time::Instant::now();
        RemoteJob::new(client, "job-1".to_string())
            .wait()
            .await
            .expect("unit job completes");
        assert_eq!(describes.load(Ordering::SeqCst), 6);
        assert_eq!(started.elapsed(), Duration::from_millis(3400));
    }

    #[test]
    fn remote_wire_unknown_fields_are_forward_decodable() {
        let response: DescribeJobResponse =
            serde_json::from_str(FUNCTION_JOB).expect("function job fixture");
        assert_eq!(response.job_state, "DONE");
    }

    #[test]
    fn describe_decodes_progress_queue_and_pause_fields() {
        let response: DescribeJobResponse = serde_json::from_str(
            r#"{
                "job_id":"job-1",
                "job_type":"refresh_column",
                "job_state":"IN_PROGRESS",
                "creation_ms":10,
                "progress":{"rows_planned":10,"rows_computed":4},
                "phase":"initializing",
                "queue_reason":"waiting for capacity",
                "waiting_for_job_id":"job-0",
                "paused_at_millis":20
            }"#,
        )
        .expect("progress fields");
        let description = response.into_description();
        let snapshot = description.progress_snapshot();
        assert_eq!(snapshot["state"], "running");
        assert_eq!(snapshot["phase"], "initializing");
        assert_eq!(snapshot["rows_planned"], 10);
        assert_eq!(snapshot["rows_computed"], 4);
        assert_eq!(snapshot["queue_reason"], "waiting for capacity");
        assert_eq!(snapshot["waiting_for_job_id"], "job-0");
        assert_eq!(snapshot["paused"], true);
        assert_eq!(snapshot["paused_at_millis"], 20);
    }

    #[test]
    fn describe_progress_reports_stable_unpaused_state() {
        let response: DescribeJobResponse = serde_json::from_str(
            r#"{
                "job_id":"job-1",
                "job_type":"refresh_column",
                "job_state":"IN_PROGRESS",
                "creation_ms":10
            }"#,
        )
        .expect("job description");
        let snapshot = response.into_description().progress_snapshot();
        assert_eq!(snapshot["paused"], false);
        assert!(snapshot.get("paused_at_millis").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn wait_progress_observes_the_existing_poll_loop() {
        let describes = Arc::new(AtomicUsize::new(0));
        let seen = describes.clone();
        let client = client_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            let n = seen.fetch_add(1, Ordering::SeqCst);
            let body = if n == 0 {
                r#"{"job_state":"IN_PROGRESS","job_type":"refresh_column","progress":{"rows_planned":10,"rows_computed":4}}"#
            } else {
                r#"{"job_state":"DONE","job_type":"refresh_column","progress":{"rows_planned":10,"rows_computed":10},"result":null}"#
            };
            http::Response::builder().status(200).body(body).unwrap()
        });
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        let observed = snapshots.clone();
        let progress: JobProgressCallback = Arc::new(Mutex::new(
            move |description: &crate::database::JobDescription| {
                observed
                    .lock()
                    .expect("snapshot lock")
                    .push(description.progress_snapshot());
            },
        ));

        RemoteJob::new(client, "job-1".to_string())
            .wait_with_progress(Some(progress))
            .await
            .expect("unit job completes");

        let snapshots = snapshots.lock().expect("snapshot lock");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0]["rows_computed"], 4);
        assert_eq!(snapshots[1]["state"], "finished");
        assert_eq!(snapshots[1]["rows_computed"], 10);
    }

    #[test]
    fn reported_failure_fields_are_passed_through() {
        let response: DescribeJobResponse = serde_json::from_str(
            r#"{
                "job_state":"FAILED",
                "failure":{
                    "phase":"initialize",
                    "message":"worker initialization timed out",
                    "retryable":true,
                    "sql_job_id":"job-1"
                }
            }"#,
        )
        .expect("failure description");
        let failure = response
            .failure
            .expect("the wire response has a failure")
            .into_job_failure();
        assert_eq!(failure.phase.as_deref(), Some("initialize"));
        assert_eq!(
            failure.message.as_deref(),
            Some("worker initialization timed out")
        );
        assert_eq!(failure.retryable, Some(true));
        assert_eq!(failure.sql_job_id.as_deref(), Some("job-1"));
    }

    #[tokio::test]
    async fn wait_failure_uses_persisted_terminal_build() {
        let server = serde_json::json!({
            "release": "1.13.0-beta.0",
            "git_sha": "2a7874ecd589",
            "build_date": "2026-11-12T22:45:43Z"
        });
        for result in [
            serde_json::Value::Null,
            serde_json::json!({"terminal_server": null}),
            serde_json::json!({"terminal_server": {"release": "incomplete"}}),
            serde_json::json!({"terminal_server": server}),
        ] {
            let known_build = result["terminal_server"] == server;
            let response = serde_json::json!({
                "job_id": "job-1",
                "job_state": "FAILED",
                "failure": {
                    "phase": "function_refresh_attempt_failed",
                    "message": "ValueError: corrupt frame at frame_index=207",
                    "traceback": "Traceback: original user code",
                    "retryable": false
                },
                "result": result
            });
            let client = client_with_handler(move |request| {
                assert_eq!(request.url().path(), "/v1/jobs/describe");
                http::Response::builder()
                    .status(200)
                    .header(
                        "x-lancedb-server-build",
                        r#"{"release":"2.0.0","git_sha":"new-query-node"}"#,
                    )
                    .body(response.to_string())
                    .unwrap()
            });
            let job = RemoteJob::new(client, "job-1".to_string());
            let description = job.describe().await.unwrap();
            assert_eq!(
                description.result.as_ref(),
                (!result.is_null()).then_some(&result)
            );
            assert_eq!(
                description.failure.unwrap().message.as_deref(),
                Some("ValueError: corrupt frame at frame_index=207")
            );
            let error = job.wait().await.err().expect("failed job");
            let mut expected =
                "Job job-1 failed: ValueError: corrupt frame at frame_index=207".to_string();
            if known_build {
                expected.push_str(
                    " [server: 1.13.0-beta.0 (git 2a7874ecd589, built 2026-11-12T22:45:43Z)]",
                );
            }
            expected.push_str(" (in function_refresh_attempt_failed)");
            assert_eq!(error.to_string(), expected);
            let crate::error::Error::JobFailed { job_id, failure } = error else {
                panic!("expected JobFailed");
            };
            assert_eq!(job_id.as_deref(), Some("job-1"));
            assert_eq!(
                failure.traceback.as_deref(),
                Some("Traceback: original user code")
            );
            assert_eq!(failure.retryable, Some(false));
        }
    }
}
