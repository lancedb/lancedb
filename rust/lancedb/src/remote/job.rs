// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::marker::PhantomData;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, JobFailure, Result};
use crate::job::JobHandle;
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
            "IN_PROGRESS" => Self::InProgress,
            "CANCELLED" => Self::Cancelled,
            // The server reports a timed-out job as FAILED on describe;
            // accept the raw registry state too in case a future server
            // stops folding it.
            "FAILED" | "TIMED_OUT" => Self::Failed,
            "DONE" => Self::Done,
            other => Self::Other(other.to_string()),
        }
    }
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
    pub(super) job_state: String,
    #[serde(default)]
    result: Option<serde_json::Value>,
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

    fn decode_result<T: DeserializeOwned>(&self, request_id: &str) -> Result<T> {
        serde_json::from_value(self.result.clone().unwrap_or(serde_json::Value::Null)).map_err(
            |error| Error::Http {
                source: format!("failed to parse typed job result: {error}").into(),
                request_id: request_id.to_string(),
                status_code: None,
            },
        )
    }
}

pub struct RemoteJob<S: HttpSend, T = ()> {
    client: RestfulLanceDbClient<S>,
    job_id: String,
    result: PhantomData<fn() -> T>,
}

impl<S: HttpSend, T> RemoteJob<S, T> {
    pub fn new(client: RestfulLanceDbClient<S>, job_id: String) -> Self {
        Self {
            client,
            job_id,
            result: PhantomData,
        }
    }

    /// One `/v1/jobs/describe` round trip.
    async fn describe(&self) -> Result<(String, DescribeJobResponse)> {
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
impl<S, T> JobHandle<T> for RemoteJob<S, T>
where
    S: HttpSend,
    T: Clone + DeserializeOwned + Send + Sync + 'static,
{
    fn id(&self) -> Option<&str> {
        Some(&self.job_id)
    }

    async fn status(&self) -> Result<String> {
        Ok(self.describe().await?.1.state().client_label())
    }

    async fn wait(&self) -> Result<T> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let (request_id, description) = self.describe().await?;
            match description.state() {
                JobState::Done => return description.decode_result(&request_id),
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
    use crate::function::{FunctionVersion, RefreshColumnResult};

    use super::DescribeJobResponse;

    const FUNCTION_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_function_job.json");
    const REFRESH_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_refresh_job.json");
    const UNIT_JOB: &str =
        include_str!("../../tests/fixtures/first_class_functions/v1/remote_unit_job.json");

    #[test]
    fn typed_remote_job_fixtures_decode_terminal_results() {
        let function: DescribeJobResponse =
            serde_json::from_str(FUNCTION_JOB).expect("function job fixture");
        let result: FunctionVersion = function
            .decode_result("fixture-request")
            .expect("typed FunctionVersion result");
        assert_eq!(result.version(), "fv_01K3EXACT");

        let refresh: DescribeJobResponse =
            serde_json::from_str(REFRESH_JOB).expect("refresh job fixture");
        let result: RefreshColumnResult = refresh
            .decode_result("fixture-request")
            .expect("typed RefreshColumnResult");
        assert_eq!(result.rows_assigned, 999_998_800);
        assert_eq!(result.rows_filled(), result.rows_assigned);

        let unit: DescribeJobResponse = serde_json::from_str(UNIT_JOB).expect("unit job fixture");
        let result: () = unit
            .decode_result("fixture-request")
            .expect("missing unit result remains compatible");
        assert_eq!(result, ());
    }

    #[test]
    fn remote_wire_unknown_fields_are_forward_decodable() {
        let response: DescribeJobResponse =
            serde_json::from_str(FUNCTION_JOB).expect("function job fixture");
        assert_eq!(response.job_state, "DONE");
    }
}
