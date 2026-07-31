// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use serde::{Deserialize, Deserializer};

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

impl<'de> Deserialize<'de> for JobState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::from(String::deserialize(deserializer)?.as_str()))
    }
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
struct ReportedFailure {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    retryable: Option<bool>,
}

#[derive(Deserialize)]
struct DescribeJobResponse {
    job_state: JobState,
    #[serde(default)]
    failure: Option<ReportedFailure>,
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
    async fn describe(&self) -> Result<DescribeJobResponse> {
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
                request_id,
                status_code: None,
            })?;
        Ok(description)
    }
}

#[async_trait]
impl<S: HttpSend> JobHandle for RemoteJob<S> {
    fn id(&self) -> Option<&str> {
        Some(&self.job_id)
    }

    async fn status(&self) -> Result<String> {
        Ok(self.describe().await?.job_state.client_label())
    }

    async fn wait(&self) -> Result<()> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let description = self.describe().await?;
            match description.job_state {
                JobState::Done => return Ok(()),
                JobState::Failed => {
                    return Err(Error::JobFailed {
                        job_id: Some(self.job_id.clone()),
                        failure: description
                            .failure
                            .map(|reported| JobFailure {
                                phase: reported.phase,
                                message: reported.message,
                                retryable: reported.retryable,
                                source: None,
                            })
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
