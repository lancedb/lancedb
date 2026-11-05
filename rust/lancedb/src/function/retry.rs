// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Client configuration for retries inside an Enterprise Function invocation.

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Retry selected user-code errors without consuming a worker retry attempt.
///
/// A retry repeats the entire failed call, so opt in only when repeating its
/// partial work is safe. Matchers are alternatives. With a batch Function and
/// `skip`, an exhausted call skips the whole batch rather than bisecting it.
///
/// ```
/// use lancedb::function::FunctionRetryPolicy;
/// let policy = FunctionRetryPolicy::transient();
/// policy.validate().unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct FunctionRetryPolicy {
    /// Total calls, including the first; 1 disables retries, maximum 100.
    pub max_attempts: u32,
    /// Initial exponential-backoff ceiling, with full jitter; default 0.2 seconds.
    pub initial_backoff_seconds: f64,
    /// Maximum backoff, including Retry-After; default 30 seconds, maximum 3600.
    pub max_backoff_seconds: f64,
    /// Exception class names, preferably qualified; subclasses also match.
    pub exception_types: Vec<String>,
    /// Regular expression matched against the exception class and message.
    pub message_pattern: Option<String>,
    /// Structured provider error codes or HTTP status codes, represented as strings.
    pub error_codes: Vec<String>,
    /// Match connection/timeouts, HTTP 429/503, SlowDown and ServiceUnavailable.
    pub transient: bool,
    /// Honor Retry-After within the configured maximum; default true.
    pub honor_retry_after: bool,
}

impl Default for FunctionRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_seconds: 0.2,
            max_backoff_seconds: 30.0,
            exception_types: Vec::new(),
            message_pattern: None,
            error_codes: Vec::new(),
            transient: false,
            honor_retry_after: true,
        }
    }
}

impl FunctionRetryPolicy {
    /// Retry known transient errors up to three calls with bounded backoff.
    pub fn transient() -> Self {
        Self {
            transient: true,
            ..Self::default()
        }
    }

    /// Validate before registration or refresh submission.
    pub fn validate(&self) -> Result<()> {
        let invalid = |message: String| Error::InvalidInput { message };
        if !(1..=100).contains(&self.max_attempts) {
            return Err(invalid(format!(
                "retry_policy.max_attempts must be between 1 and 100, got {}",
                self.max_attempts
            )));
        }
        for (name, value) in [
            ("initial_backoff_seconds", self.initial_backoff_seconds),
            ("max_backoff_seconds", self.max_backoff_seconds),
        ] {
            if !value.is_finite() || !(0.0..=3600.0).contains(&value) {
                return Err(invalid(format!(
                    "retry_policy.{name} must be finite and between 0 and 3600, got {value}"
                )));
            }
        }
        if self.initial_backoff_seconds > self.max_backoff_seconds {
            return Err(invalid(
                "retry_policy.initial_backoff_seconds exceeds max_backoff_seconds".into(),
            ));
        }
        if self
            .exception_types
            .iter()
            .chain(&self.error_codes)
            .any(String::is_empty)
        {
            return Err(invalid(
                "retry_policy exception_types and error_codes must contain non-empty strings"
                    .into(),
            ));
        }
        if let Some(pattern) = &self.message_pattern {
            regex::Regex::new(pattern).map_err(|error| {
                invalid(format!("invalid retry_policy.message_pattern: {error}"))
            })?;
        }
        if self.max_attempts > 1
            && !self.transient
            && self.message_pattern.is_none()
            && self.exception_types.is_empty()
            && self.error_codes.is_empty()
        {
            return Err(invalid(
                "retry_policy requires at least one error matcher".into(),
            ));
        }
        Ok(())
    }
}

/// What a Function refresh does after a call fails or exhausts retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionErrorPolicy {
    /// Fail the refresh job.
    Fail,
    /// Write null and record the error, subject to the server's failure limits.
    Skip,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(json!({"max_attempts": 0, "transient": true}))]
    #[case(json!({"max_attempts": 101, "transient": true}))]
    #[case(json!({"max_attempts": 3}))]
    #[case(json!({"initial_backoff_seconds": -1, "transient": true}))]
    #[case(json!({"max_backoff_seconds": 0, "transient": true}))]
    #[case(json!({"exception_types": [""]}))]
    #[case(json!({"message_pattern": "["}))]
    fn reject_invalid_policy(#[case] value: serde_json::Value) {
        let policy: FunctionRetryPolicy = serde_json::from_value(value).unwrap();
        assert!(policy.validate().is_err());
    }

    #[test]
    fn policy_defaults_and_disabled_policy() {
        assert!(FunctionRetryPolicy::transient().validate().is_ok());
        let policy: FunctionRetryPolicy =
            serde_json::from_value(json!({"max_attempts":1})).unwrap();
        assert!(policy.validate().is_ok());
        let mut invalid = policy;
        invalid.initial_backoff_seconds = f64::NAN;
        assert!(invalid.validate().is_err());
    }
}
