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

    /// Increment the appropriate failure counter based on the error type.
    ///
    /// For `Error::Http` whose source is a connect error, increments
    /// `connect_failures`. For read errors (`is_body` or `is_decode`),
    /// increments `read_failures`. For all other errors, increments
    /// `request_failures`. Calls `check_out_of_retries` to enforce global limits.
    pub fn increment_from_error(&mut self, source: crate::Error) -> crate::Result<()> {
        let reqwest_err = match &source {
            crate::Error::Http { source, .. } => source.downcast_ref::<reqwest::Error>(),
            _ => None,
        };

        if reqwest_err.is_some_and(|e| e.is_connect()) {
            self.connect_failures += 1;
        } else if reqwest_err.is_some_and(|e| e.is_body() || e.is_decode()) {
            self.read_failures += 1;
        } else {
            self.request_failures += 1;
        }

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
            "Retrying request {:?} ({}/{} connect, {}/{} request, {}/{} read) in {:?}",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ResolvedRetryConfig {
        ResolvedRetryConfig {
            retries: 3,
            connect_retries: 2,
            read_retries: 3,
            backoff_factor: 0.0,
            backoff_jitter: 0.0,
            statuses: vec![reqwest::StatusCode::BAD_GATEWAY],
        }
    }

    /// Get a real reqwest connect error by trying to connect to a refused port.
    async fn make_connect_error() -> reqwest::Error {
        // Port 1 is almost always refused/unavailable.
        reqwest::Client::new()
            .get("http://127.0.0.1:1")
            .send()
            .await
            .unwrap_err()
    }

    #[tokio::test]
    async fn test_increment_from_error_connect() {
        let config = test_config();
        let mut counter = RetryCounter::new(&config, "test".to_string());

        let connect_err = make_connect_error().await;
        assert!(connect_err.is_connect());

        let http_err = crate::Error::Http {
            source: Box::new(connect_err),
            request_id: "test".to_string(),
            status_code: None,
        };

        // First connect failure: should be ok (1 < 2)
        counter.increment_from_error(http_err).unwrap();
        assert_eq!(counter.connect_failures, 1);
        assert_eq!(counter.request_failures, 0);

        // Second connect failure: should hit the limit (2 >= 2)
        let connect_err2 = make_connect_error().await;
        let http_err2 = crate::Error::Http {
            source: Box::new(connect_err2),
            request_id: "test".to_string(),
            status_code: None,
        };
        let result = counter.increment_from_error(http_err2);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Retry {
                connect_failures: 2,
                max_connect_failures: 2,
                ..
            }
        ));
    }

    #[test]
    fn test_increment_from_error_request() {
        let config = test_config();
        let mut counter = RetryCounter::new(&config, "test".to_string());

        let http_err = crate::Error::Http {
            source: "bad gateway".into(),
            request_id: "test".to_string(),
            status_code: Some(reqwest::StatusCode::BAD_GATEWAY),
        };

        counter.increment_from_error(http_err).unwrap();
        assert_eq!(counter.request_failures, 1);
        assert_eq!(counter.connect_failures, 0);
    }

    #[tokio::test]
    async fn test_increment_from_error_respects_global_limits() {
        // If request_failures is already at max, a connect error should still
        // trigger the global limit check.
        let config = test_config();
        let mut counter = RetryCounter::new(&config, "test".to_string());
        counter.request_failures = 3; // at max

        let connect_err = make_connect_error().await;
        let http_err = crate::Error::Http {
            source: Box::new(connect_err),
            request_id: "test".to_string(),
            status_code: None,
        };

        // Even though connect_failures would be 1 (under limit of 2),
        // request_failures is already at 3 (>= limit of 3), so this should fail.
        let result = counter.increment_from_error(http_err);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::Error::Retry {
                request_failures: 3,
                connect_failures: 1,
                ..
            }
        ));
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
