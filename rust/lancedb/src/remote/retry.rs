// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::remote::RetryConfig;
use crate::Error;
use log::debug;
use std::time::Duration;

pub struct RetryCounter<'a> {
    pub request_failures: u8,
    pub connect_failures: u8,
    pub read_failures: u8,
    pub config: &'a ResolvedRetryConfig,
    pub request_id: String,
}

impl<'a> RetryCounter<'a> {
    pub(crate) fn new(config: &'a ResolvedRetryConfig, request_id: String) -> Self {
        Self {
            request_failures: 0,
            connect_failures: 0,
            read_failures: 0,
            config,
            request_id,
        }
    }

    fn check_out_of_retries(
        &self,
        source: Box<dyn std::error::Error + Send + Sync>,
        status_code: Option<reqwest::StatusCode>,
    ) -> crate::Result<()> {
        if self.request_failures >= self.config.retries
            || self.connect_failures >= self.config.connect_retries
            || self.read_failures >= self.config.read_retries
        {
            Err(Error::Retry {
                request_id: self.request_id.clone(),
                request_failures: self.request_failures,
                max_request_failures: self.config.retries,
                connect_failures: self.connect_failures,
                max_connect_failures: self.config.connect_retries,
                read_failures: self.read_failures,
                max_read_failures: self.config.read_retries,
                source,
                status_code,
            })
        } else {
            Ok(())
        }
    }

    pub fn increment_request_failures(&mut self, source: crate::Error) -> crate::Result<()> {
        self.request_failures += 1;
        let status_code = if let crate::Error::Http { status_code, .. } = &source {
            *status_code
        } else {
            None
        };
        self.check_out_of_retries(Box::new(source), status_code)
    }

    pub fn increment_connect_failures(&mut self, source: reqwest::Error) -> crate::Result<()> {
        self.connect_failures += 1;
        let status_code = source.status();
        self.check_out_of_retries(Box::new(source), status_code)
    }

    pub fn increment_read_failures(&mut self, source: reqwest::Error) -> crate::Result<()> {
        self.read_failures += 1;
        let status_code = source.status();
        self.check_out_of_retries(Box::new(source), status_code)
    }

    pub fn next_sleep_time(&self) -> Duration {
        let backoff = self.config.backoff_factor * (2.0f32.powi(self.request_failures as i32));
        let jitter = rand::random::<f32>() * self.config.backoff_jitter;
        let sleep_time = Duration::from_secs_f32(backoff + jitter);
        debug!(
            "Retrying request {:?} ({}/{} connect, {}/{} read, {}/{} read) in {:?}",
            self.request_id,
            self.connect_failures,
            self.config.connect_retries,
            self.request_failures,
            self.config.retries,
            self.read_failures,
            self.config.read_retries,
            sleep_time
        );
        sleep_time
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedRetryConfig {
    pub retries: u8,
    pub connect_retries: u8,
    pub read_retries: u8,
    pub backoff_factor: f32,
    pub backoff_jitter: f32,
    pub statuses: Vec<reqwest::StatusCode>,
}

impl TryFrom<RetryConfig> for ResolvedRetryConfig {
    type Error = Error;

    fn try_from(retry_config: RetryConfig) -> crate::Result<Self> {
        Ok(Self {
            retries: retry_config.retries.unwrap_or(3),
            connect_retries: retry_config.connect_retries.unwrap_or(3),
            read_retries: retry_config.read_retries.unwrap_or(3),
            backoff_factor: retry_config.backoff_factor.unwrap_or(0.25),
            backoff_jitter: retry_config.backoff_jitter.unwrap_or(0.25),
            statuses: retry_config
                .statuses
                .unwrap_or_else(|| vec![409, 429, 500, 502, 503, 504])
                .into_iter()
                .map(|status| reqwest::StatusCode::from_u16(status).unwrap())
                .collect(),
        })
    }
}
