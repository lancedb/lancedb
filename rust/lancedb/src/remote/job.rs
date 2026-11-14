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
            "IN_PROGRESS" | "in_progress" | "created" | "CREATED" | "queued" | "QUEUED" => {
                Self::InProgress
            }
            "CANCELLED" | "cancelled" | "canceled" => Self::Cancelled,
            // The server reports a timed-out job as FAILED on describe;
            // accept the raw registry state too in case a future server
            // stops folding it.
            "FAILED" | "failed" | "TIMED_OUT" | "timed_out" => Self::Failed,
            "DONE" | "done" | "succeeded" | "SUCCEEDED" => Self::Done,
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
    pub(super) operation_id: Option<String>,
    #[serde(default)]
    pub(super) attempt: Option<u32>,
    #[serde(default)]
    pub(super) retry_of: Option<String>,
    #[serde(default)]
    pub(super) lifecycle_state: Option<String>,
    #[serde(default)]
    pub(super) source_table_version: Option<u64>,
    #[serde(default)]
    pub(super) execution_epoch: Option<i64>,
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
        JobState::from(self.lifecycle_state.as_deref().unwrap_or(&self.job_state))
    }

    fn into_terminal_result(self, request_id: String) -> TerminalResult {
        TerminalResult::remote(self.result, request_id)
    }

    /// The public description this wire envelope stands for.
    pub(super) fn into_description(self) -> JobDescription {
        let state = self.state().client_label();
        JobDescription {
            job_id: self.job_id,
            operation_id: self.operation_id,
            attempt: self.attempt,
            retry_of: self.retry_of,
            lifecycle_state: self
                .lifecycle_state
                .as_deref()
                .map(crate::job::JobState::from),
            source_table_version: self.source_table_version,
            execution_epoch: self.execution_epoch,
            job_type: self.job_type,
            state,
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

    async fn stats(&self) -> Result<Option<String>> {
        let request = self
            .client
            .post("/v1/jobs/stats")
            .json(&serde_json::json!({"job_id": self.job_id}));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let response: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| Error::Http {
                source: format!("failed to parse job statistics: {error}").into(),
                request_id: request_id.clone(),
                status_code: None,
            })?;
        if response.get("job_id").and_then(serde_json::Value::as_str) == Some(self.job_id.as_str())
        {
            match response.get("report") {
                Some(serde_json::Value::Null) => return Ok(None),
                Some(serde_json::Value::String(report)) => return Ok(Some(report.clone())),
                _ => {}
            }
        }
        Err(Error::Http {
            source: "server response is not a Function job run report; job statistics require server support".into(),
            request_id,
            status_code: None,
        })
    }

    async fn events(&self, request: JobEventsRequest) -> Result<Vec<RecordBatch>> {
        let mut body = serde_json::json!({ "job_id": self.job_id });
        if let Some(limit) = request.limit {
            body["limit"] = serde_json::Value::from(limit);
        }
        if let Some(filter) = request.filter {
            body["filter"] = serde_json::Value::String(filter);
        }
        if let Some(token) = request.page_token {
            body["page_token"] = serde_json::Value::String(token);
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
                    return Err(Error::JobFailed {
                        job_id: Some(self.job_id.clone()),
                        failure: Box::new(
                            description
                                .failure
                                .map(ReportedFailure::into_job_failure)
                                .unwrap_or_default(),
                        ),
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

    #[tokio::test]
    async fn local_and_remote_jobs_share_terminal_state_contract() {
        for remote in [false, true] {
            for state in [
                crate::JobState::Succeeded,
                crate::JobState::Failed,
                crate::JobState::Canceled,
            ] {
                let job = if remote {
                    let response_state = state.as_str().to_string();
                    let client = client_with_handler(move |request| {
                        let body = if request.url().path() == "/v1/jobs/cancel" {
                            "{}".to_string()
                        } else {
                            serde_json::json!({
                                "job_id": "attempt-2", "operation_id": "attempt-1", "attempt": 2,
                                "retry_of": "attempt-1", "job_state": "IN_PROGRESS",
                                "lifecycle_state": response_state, "source_table_version": 7, "execution_epoch": 3,
                            }).to_string()
                        };
                        http::Response::builder().status(200).body(body).unwrap()
                    });
                    Job::new(Box::new(RemoteJob::new(client, "attempt-2".to_string())))
                } else {
                    match state {
                        crate::JobState::Succeeded => Job::spawned(tokio::spawn(async { Ok(()) })),
                        crate::JobState::Failed => Job::spawned(tokio::spawn(async {
                            Err::<(), _>(crate::Error::Runtime {
                                message: "failed".to_string(),
                            })
                        })),
                        _ => Job::spawned(tokio::spawn(std::future::pending::<Result<()>>())),
                    }
                };
                if state == crate::JobState::Canceled {
                    job.cancel().await.unwrap();
                }
                for _ in 0..2 {
                    let result = job.wait().await;
                    match state {
                        crate::JobState::Succeeded => result.unwrap(),
                        crate::JobState::Failed => {
                            assert!(matches!(result, Err(crate::Error::JobFailed { .. })))
                        }
                        _ => assert!(matches!(result, Err(crate::Error::JobCancelled { .. }))),
                    }
                    assert_eq!(job.lifecycle_state(), Some(state.clone()));
                    assert_eq!(job.status_typed().await.unwrap(), state);
                }
                let identity = job.job_id().unwrap();
                let operation = job.operation_id();
                let mapped = job.map(|()| 7);
                assert_eq!(mapped.job_id().as_deref(), Some(identity.as_str()));
                assert_eq!(mapped.operation_id(), operation);
                if remote {
                    assert_eq!(mapped.retry_of().as_deref(), Some("attempt-1"));
                    assert_eq!(mapped.attempt(), Some(2));
                    assert_eq!(mapped.source_table_version(), Some(7));
                } else {
                    uuid::Uuid::parse_str(&identity).unwrap();
                    assert_eq!(mapped.attempt(), Some(1));
                }
            }
        }
    }

    #[tokio::test]
    async fn unknown_detailed_state_never_inherits_legacy_success() {
        let client = client_with_handler(|_| {
            http::Response::builder().status(200).body(
            r#"{"job_id":"job-1","job_state":"DONE","lifecycle_state":"publishing_in_future","result":null}"#
        ).unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".to_string())));
        assert_eq!(job.status().await.unwrap(), "publishing_in_future");
        let state = job.status_typed().await.unwrap();
        assert_eq!(state.as_str(), "publishing_in_future");
        assert!(!state.is_success());
        assert!(!state.is_terminal());
        assert!(matches!(
            job.wait_timeout(Duration::from_millis(1)).await,
            Err(crate::Error::Timeout { .. })
        ));
        assert_eq!(job.lifecycle_state(), Some(state));
    }

    #[tokio::test]
    async fn local_and_remote_wait_timeouts_leave_jobs_waitable() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let local = Job::spawned(tokio::spawn(async move {
            receiver.await.map_err(|error| crate::Error::Runtime {
                message: error.to_string(),
            })
        }));
        assert!(matches!(
            local.wait_timeout(Duration::from_millis(1)).await,
            Err(crate::Error::Timeout { .. })
        ));
        sender.send(7).unwrap();
        assert_eq!(local.wait().await.unwrap(), 7);

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = done.clone();
        let client = client_with_handler(move |request| {
            assert_eq!(request.url().path(), "/v1/jobs/describe");
            http::Response::builder().status(200).body(serde_json::json!({
                "job_state": if observed.load(Ordering::SeqCst) { "DONE" } else { "IN_PROGRESS" }, "result": 7
            }).to_string()).unwrap()
        });
        let remote = Job::<u64>::new_typed(Box::new(RemoteJob::new(client, "job-1".to_string())));
        assert!(matches!(
            remote.wait_timeout(Duration::from_millis(1)).await,
            Err(crate::Error::Timeout { .. })
        ));
        done.store(true, Ordering::SeqCst);
        assert_eq!(remote.wait().await.unwrap(), 7);
    }

    #[tokio::test]
    async fn stats_reads_report_verbatim_and_preserves_unavailable_without_waiting() {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let report = "Function job: finished\nWorker utilization: unavailable\n";
        let client = client_with_handler(move |request| {
            assert_eq!(request.method(), reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/jobs/stats");
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            assert_eq!(body, serde_json::json!({"job_id":"job-1"}));
            let response = if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                serde_json::json!({"job_id":"job-1","profile":null,"report":null})
            } else {
                serde_json::json!({"job_id":"job-1","profile":{"future_field":17},"report":report})
            };
            http::Response::builder()
                .status(200)
                .body(response.to_string())
                .unwrap()
        });
        let job = Job::new(Box::new(RemoteJob::new(client, "job-1".to_string())));
        assert_eq!(job.stats().await.unwrap(), None);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(job.stats().await.unwrap().as_deref(), Some(report));
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stats_rejects_malformed_or_unrelated_responses() {
        for body in [
            "{",
            r#"{"job_id":"job-1"}"#,
            r#"{"job_id":"job-1","report":7}"#,
            r#"{"job_id":"another-job","report":"a report"}"#,
        ] {
            let client = client_with_handler(move |_| {
                http::Response::builder().status(200).body(body).unwrap()
            });
            let error = RemoteJob::new(client, "job-1".to_string())
                .stats()
                .await
                .unwrap_err();
            assert!(matches!(error, crate::Error::Http { .. }));
            assert!(error.to_string().contains("job statistics"));
        }
    }

    #[tokio::test]
    async fn stats_preserves_not_found_errors() {
        let client = client_with_handler(|_| {
            http::Response::builder()
                .status(404)
                .body("job not found")
                .unwrap()
        });
        let error = RemoteJob::new(client, "missing".to_string())
            .stats()
            .await
            .unwrap_err();
        match error {
            crate::Error::Http { status_code, .. } => {
                assert_eq!(status_code, Some(reqwest::StatusCode::NOT_FOUND));
            }
            other => panic!("expected HTTP 404, got {other:?}"),
        }
    }

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
}
