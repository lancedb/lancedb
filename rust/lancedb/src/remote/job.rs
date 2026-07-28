// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::job::JobHandle;
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};

/// Delay before the second job-state poll; doubles up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum JobState {
    #[serde(rename = "IN_PROGRESS")]
    InProgress,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "FAILED")]
    Failed,
    #[serde(rename = "DONE")]
    Done,
    /// A state this client version does not know; treated as still running.
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct DescribeJobResponse {
    job_state: JobState,
}

pub struct RemoteJob<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    job_id: String,
}

impl<S: HttpSend> RemoteJob<S> {
    pub fn new(client: RestfulLanceDbClient<S>, job_id: String) -> Self {
        Self { client, job_id }
    }

    /// One `/v1/jobs/describe` round trip, returning the job state.
    async fn describe_state(&self) -> Result<JobState> {
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
        Ok(description.job_state)
    }
}

#[async_trait]
impl<S: HttpSend> JobHandle for RemoteJob<S> {
    async fn wait(&self) -> Result<()> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            match self.describe_state().await? {
                JobState::Done => return Ok(()),
                JobState::Failed => {
                    return Err(Error::JobFailed {
                        job_id: self.job_id.clone(),
                        message: "job reached the FAILED state".to_string(),
                    });
                }
                JobState::Cancelled => {
                    return Err(Error::JobCancelled {
                        job_id: self.job_id.clone(),
                    });
                }
                JobState::InProgress | JobState::Other => {}
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
