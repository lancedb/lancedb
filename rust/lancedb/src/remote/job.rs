// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Tracking for server-side jobs through the `/v1/jobs` API.

use std::time::Duration;

use async_trait::async_trait;
use tokio::time::sleep;

use crate::error::{Error, Result};
use crate::job::JobHandle;
use crate::remote::client::{HttpSend, RequestResultExt, RestfulLanceDbClient};

/// Delay before the second job-state poll; doubles up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct RemoteJob<S: HttpSend> {
    client: RestfulLanceDbClient<S>,
    job_id: String,
}

impl<S: HttpSend> RemoteJob<S> {
    pub fn new(client: RestfulLanceDbClient<S>, job_id: String) -> Self {
        Self { client, job_id }
    }

    /// One `/v1/jobs/describe` round trip, returning the job state string.
    async fn describe_state(&self) -> Result<String> {
        let request = self
            .client
            .post("/v1/jobs/describe")
            .json(&serde_json::json!({ "job_id": self.job_id }));
        let (request_id, response) = self.client.send(request).await?;
        let response = self.client.check_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("failed to parse job description: {}", e).into(),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        value
            .get("job_state")
            .and_then(|state| state.as_str())
            .map(str::to_string)
            .ok_or_else(|| Error::Http {
                source: format!("job description has no job_state: {}", body).into(),
                request_id,
                status_code: None,
            })
    }
}

#[async_trait]
impl<S: HttpSend> JobHandle for RemoteJob<S> {
    async fn wait(&self) -> Result<()> {
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            match self.describe_state().await?.as_str() {
                "DONE" => return Ok(()),
                "FAILED" => {
                    return Err(Error::JobFailed {
                        message: "job reached the FAILED state".to_string(),
                    });
                }
                "CANCELLED" => return Err(Error::JobCancelled),
                _ => {}
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
