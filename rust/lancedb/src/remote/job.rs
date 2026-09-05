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
use crate::job::{JobEventsRequest, JobHandle, TerminalResult};
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};

/// Delay before the second job-state poll; doubles up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

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
#[derive(Deserialize)]
pub(super) struct ReportedFailure {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
}

/// Forward-compatible `/v1/jobs/describe` wire envelope.
#[derive(Deserialize)]
pub(super) struct DescribeJobResponse {
    #[serde(default)]
    pub(super) job_id: String,
    #[serde(default)]
    pub(super) job_type: String,
    pub(super) job_state: String,
    #[serde(default)]
    pub(super) creation_ms: i64,
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
            retryable: self.retryable,
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
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let (request_id, description) = self.fetch_description().await?;
            match description.state() {
                JobState::Done => return Ok(description.into_terminal_result(request_id)),
                JobState::Failed => {
                    return Err(Error::JobFailed {
                        job_id: Some(self.job_id.clone()),
                        failure: description
                            .failure
                            .map(ReportedFailure::into_job_failure)
                            .unwrap_or_default(),
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
    use async_trait::async_trait;

    use crate::Result;
    use crate::function::{FunctionVersion, RefreshColumnResult};
    use crate::job::{Job, JobHandle, TerminalResult};

    use super::DescribeJobResponse;

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
        assert_eq!(result.version(), "fv_01K3EXACT");

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

    #[test]
    fn remote_wire_unknown_fields_are_forward_decodable() {
        let response: DescribeJobResponse =
            serde_json::from_str(FUNCTION_JOB).expect("function job fixture");
        assert_eq!(response.job_state, "DONE");
    }
}
