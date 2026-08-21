// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::Error;
use crate::remote::RetryConfig;
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
        // `Duration::from_secs_f32` panics once the exponential exceeds what a
        // `Duration` can hold, which the default backoff factor reaches at 66
        // request failures -- within reach of `retries`, a `u8`. Saturate
        // instead: a retry sleep this long is indistinguishable from a hang,
        // and the panic it replaces was strictly worse.
        let sleep_time = Duration::try_from_secs_f32(backoff + jitter).unwrap_or(MAX_RETRY_SLEEP);
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
    /// Resolve against a fixed set of "environment" values, touching no
    /// process-global state.
    fn resolve_with(cfg: RetryConfig, env: &[(&str, &str)]) -> crate::Result<ResolvedRetryConfig> {
        let env: std::collections::HashMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ResolvedRetryConfig::resolve(cfg, |name| env.get(name).cloned())
    }

    #[test]
    fn test_resolve_retry_defaults_when_unset() {
        let resolved = resolve_with(RetryConfig::default(), &[]).unwrap();

        assert_eq!(resolved.retries, 3);
        assert_eq!(resolved.connect_retries, 3);
        assert_eq!(resolved.read_retries, 3);
        assert_eq!(resolved.backoff_factor, 0.25);
        assert_eq!(resolved.backoff_jitter, 0.25);
        assert_eq!(
            resolved.statuses,
            vec![
                reqwest::StatusCode::CONFLICT,
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                reqwest::StatusCode::BAD_GATEWAY,
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                reqwest::StatusCode::GATEWAY_TIMEOUT,
            ]
        );
    }

    #[test]
    fn test_resolve_retry_from_env() {
        let resolved = resolve_with(
            RetryConfig::default(),
            &[
                ("LANCE_CLIENT_MAX_RETRIES", "7"),
                ("LANCE_CLIENT_CONNECT_RETRIES", "5"),
                ("LANCE_CLIENT_READ_RETRIES", "0"),
                ("LANCE_CLIENT_RETRY_BACKOFF_FACTOR", "1.5"),
                ("LANCE_CLIENT_RETRY_BACKOFF_JITTER", "0"),
                ("LANCE_CLIENT_RETRY_STATUSES", "500,503"),
            ],
        )
        .unwrap();

        assert_eq!(resolved.retries, 7);
        assert_eq!(resolved.connect_retries, 5);
        assert_eq!(resolved.read_retries, 0);
        assert_eq!(resolved.backoff_factor, 1.5);
        assert_eq!(resolved.backoff_jitter, 0.0);
        assert_eq!(
            resolved.statuses,
            vec![
                reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
            ]
        );
    }

    #[test]
    fn test_resolve_retry_passed_value_wins_over_env() {
        let config = RetryConfig {
            retries: Some(1),
            backoff_factor: Some(0.5),
            statuses: Some(vec![429]),
            ..Default::default()
        };
        let resolved = resolve_with(
            config,
            &[
                ("LANCE_CLIENT_MAX_RETRIES", "7"),
                ("LANCE_CLIENT_RETRY_BACKOFF_FACTOR", "1.5"),
                ("LANCE_CLIENT_RETRY_STATUSES", "500,503"),
            ],
        )
        .unwrap();

        assert_eq!(resolved.retries, 1);
        assert_eq!(resolved.backoff_factor, 0.5);
        assert_eq!(
            resolved.statuses,
            vec![reqwest::StatusCode::TOO_MANY_REQUESTS]
        );
        // Fields left unset still fall through to the default.
        assert_eq!(resolved.connect_retries, 3);
    }

    #[test]
    fn test_resolve_retry_statuses_tolerates_spacing_and_blanks() {
        let resolved = resolve_with(
            RetryConfig::default(),
            &[("LANCE_CLIENT_RETRY_STATUSES", " 429 , 503 ,")],
        )
        .unwrap();

        assert_eq!(
            resolved.statuses,
            vec![
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
            ]
        );
    }

    #[test]
    fn test_resolve_retry_statuses_empty_env_disables() {
        let resolved = resolve_with(
            RetryConfig::default(),
            &[("LANCE_CLIENT_RETRY_STATUSES", "")],
        )
        .unwrap();

        assert!(resolved.statuses.is_empty());
    }

    /// Every invalid value is rejected, and the message names both the
    /// variable the user has to fix and the value they set.
    #[rstest::rstest]
    #[case("LANCE_CLIENT_MAX_RETRIES", "not-a-number")]
    #[case("LANCE_CLIENT_MAX_RETRIES", "300")]
    #[case("LANCE_CLIENT_CONNECT_RETRIES", "-1")]
    #[case("LANCE_CLIENT_READ_RETRIES", "1.5")]
    #[case("LANCE_CLIENT_RETRY_BACKOFF_FACTOR", "not-a-number")]
    #[case("LANCE_CLIENT_RETRY_BACKOFF_FACTOR", "-1")]
    #[case("LANCE_CLIENT_RETRY_BACKOFF_JITTER", "inf")]
    #[case("LANCE_CLIENT_RETRY_STATUSES", "500,not-a-number")]
    #[case("LANCE_CLIENT_RETRY_STATUSES", "42")]
    fn test_resolve_retry_invalid_env(#[case] var: &str, #[case] value: &str) {
        let err = resolve_with(RetryConfig::default(), &[(var, value)])
            .expect_err("expected invalid env var to be rejected");

        let crate::Error::InvalidInput { message } = &err else {
            panic!("expected InvalidInput, got {:?}", err);
        };
        assert!(
            message.contains(var),
            "message should name the env var to fix, got '{}'",
            message
        );
        assert!(
            message.contains(value),
            "message should echo the offending value, got '{}'",
            message
        );
    }

    #[test]
    fn test_resolve_retry_rejects_invalid_passed_values() {
        // A negative backoff would otherwise produce a nonsensical sleep.
        let config = RetryConfig {
            backoff_factor: Some(-1.0),
            ..Default::default()
        };
        assert!(matches!(
            resolve_with(config, &[]).unwrap_err(),
            crate::Error::InvalidInput { .. }
        ));

        // An out-of-range status code would otherwise panic on `unwrap`.
        let config = RetryConfig {
            statuses: Some(vec![42]),
            ..Default::default()
        };
        assert!(matches!(
            resolve_with(config, &[]).unwrap_err(),
            crate::Error::InvalidInput { .. }
        ));
    }

    /// `retries` is a `u8`, so the exponential in `next_sleep_time` can exceed
    /// what a `Duration` holds -- at 66 failures with the default backoff
    /// factor. That used to panic.
    #[test]
    fn test_next_sleep_time_saturates_instead_of_panicking() {
        let config = ResolvedRetryConfig {
            retries: u8::MAX,
            connect_retries: u8::MAX,
            read_retries: u8::MAX,
            backoff_factor: 0.25,
            backoff_jitter: 0.25,
            statuses: vec![],
        };
        let mut counter = RetryCounter::new(&config, "test".to_string());

        // Below the overflow point the computed backoff is used as-is.
        counter.request_failures = 10;
        assert_eq!(counter.next_sleep_time().as_secs(), 256);

        // At and beyond it, the sleep saturates rather than panicking.
        for failures in [66, 128, u8::MAX] {
            counter.request_failures = failures;
            assert_eq!(
                counter.next_sleep_time(),
                MAX_RETRY_SLEEP,
                "should saturate at {} failures",
                failures
            );
        }
    }

    // The two tests below are the only ones that touch process-global
    // environment state, so they confirm `try_from` is actually wired to the
    // environment. Everything else resolves through an injected lookup.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_try_from_reads_the_environment() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: This is only called in tests, under `ENV_MUTEX`.
        unsafe {
            std::env::set_var("LANCE_CLIENT_MAX_RETRIES", "9");
        }
        let resolved: crate::Result<ResolvedRetryConfig> = RetryConfig::default().try_into();
        // SAFETY: This is only called in tests, under `ENV_MUTEX`.
        unsafe {
            std::env::remove_var("LANCE_CLIENT_MAX_RETRIES");
        }

        assert_eq!(resolved.unwrap().retries, 9);
    }

    #[test]
    fn test_try_from_uses_defaults_when_environment_is_unset() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: This is only called in tests, under `ENV_MUTEX`.
        unsafe {
            std::env::remove_var("LANCE_CLIENT_MAX_RETRIES");
        }
        let resolved: ResolvedRetryConfig = RetryConfig::default().try_into().unwrap();

        assert_eq!(resolved.retries, 3);
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

const DEFAULT_RETRIES: u8 = 3;
const DEFAULT_CONNECT_RETRIES: u8 = 3;
const DEFAULT_READ_RETRIES: u8 = 3;
const DEFAULT_BACKOFF_FACTOR: f32 = 0.25;
const DEFAULT_BACKOFF_JITTER: f32 = 0.25;
const DEFAULT_RETRY_STATUSES: [u16; 6] = [409, 429, 500, 502, 503, 504];

/// Ceiling for a single retry sleep, applied when the exponential backoff
/// overflows what a `Duration` can represent. Far above any sleep a sane
/// configuration produces -- the default factor and retry count peak at a
/// couple of seconds -- so this only engages where the computation would
/// otherwise have panicked.
const MAX_RETRY_SLEEP: Duration = Duration::from_secs(60 * 60);

/// Resolve a retry count from the config value, the environment, or a default.
///
/// An explicit config value always wins; `env` is only consulted when the
/// caller left the field unset. `env_var` names the variable `env` was read
/// from, for error messages.
fn resolve_retries(
    passed: Option<u8>,
    env_var: &str,
    env: Option<&str>,
    default: u8,
) -> crate::Result<u8> {
    if let Some(value) = passed {
        Ok(value)
    } else if let Some(env) = env {
        env.trim().parse::<u8>().map_err(|_| Error::InvalidInput {
            message: format!(
                "{} must be an integer between 0 and 255, got '{}'",
                env_var, env
            ),
        })
    } else {
        Ok(default)
    }
}

/// Resolve a backoff factor from the config value, the environment, or a
/// default.
///
/// Negative and non-finite values are rejected here rather than left to
/// produce a nonsensical sleep time later in `next_sleep_time`.
fn resolve_backoff(
    passed: Option<f32>,
    env_var: &str,
    env: Option<&str>,
    default: f32,
) -> crate::Result<f32> {
    let value = if let Some(value) = passed {
        value
    } else if let Some(env) = env {
        env.trim().parse::<f32>().map_err(|_| Error::InvalidInput {
            message: format!(
                "{} must be a non-negative number of seconds, got '{}'",
                env_var, env
            ),
        })?
    } else {
        default
    };
    if !value.is_finite() || value < 0.0 {
        return Err(Error::InvalidInput {
            message: format!(
                "{} must be a non-negative, finite number of seconds, got '{}'",
                env_var, value
            ),
        });
    }
    Ok(value)
}

/// Resolve the retryable status codes from the config value, the environment,
/// or a default.
///
/// The environment value is a comma-separated list of integers; empty entries
/// are ignored, so a blank value disables status-based retries just like
/// passing an empty vector.
fn resolve_statuses(
    passed: Option<Vec<u16>>,
    env_var: &str,
    env: Option<&str>,
) -> crate::Result<Vec<reqwest::StatusCode>> {
    // `Some(env_var)` when the values came from the environment, so an
    // out-of-range code can point the user at the variable to fix.
    let (raw, from_env) = if let Some(statuses) = passed {
        (statuses, None)
    } else if let Some(env) = env {
        let parsed = env
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse::<u16>().map_err(|_| Error::InvalidInput {
                    message: format!(
                        "{} must be a comma-separated list of integer status codes, got '{}'",
                        env_var, env
                    ),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        (parsed, Some(env_var))
    } else {
        (DEFAULT_RETRY_STATUSES.to_vec(), None)
    };

    raw.into_iter()
        .map(|status| {
            reqwest::StatusCode::from_u16(status).map_err(|_| Error::InvalidInput {
                message: match from_env {
                    Some(env_var) => format!(
                        "{} contains an invalid HTTP status code: '{}'",
                        env_var, status
                    ),
                    None => format!(
                        "RetryConfig.statuses contains an invalid HTTP status code: '{}'",
                        status
                    ),
                },
            })
        })
        .collect()
}

impl ResolvedRetryConfig {
    /// Resolve a `RetryConfig`, reading unset fields through `lookup`.
    ///
    /// The environment is reached only through `lookup`, so tests can exercise
    /// every parsing and precedence rule without mutating process-global
    /// state that sibling tests read concurrently.
    fn resolve(
        retry_config: RetryConfig,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> crate::Result<Self> {
        let max_retries = lookup("LANCE_CLIENT_MAX_RETRIES");
        let connect_retries = lookup("LANCE_CLIENT_CONNECT_RETRIES");
        let read_retries = lookup("LANCE_CLIENT_READ_RETRIES");
        let backoff_factor = lookup("LANCE_CLIENT_RETRY_BACKOFF_FACTOR");
        let backoff_jitter = lookup("LANCE_CLIENT_RETRY_BACKOFF_JITTER");
        let statuses = lookup("LANCE_CLIENT_RETRY_STATUSES");

        Ok(Self {
            retries: resolve_retries(
                retry_config.retries,
                "LANCE_CLIENT_MAX_RETRIES",
                max_retries.as_deref(),
                DEFAULT_RETRIES,
            )?,
            connect_retries: resolve_retries(
                retry_config.connect_retries,
                "LANCE_CLIENT_CONNECT_RETRIES",
                connect_retries.as_deref(),
                DEFAULT_CONNECT_RETRIES,
            )?,
            read_retries: resolve_retries(
                retry_config.read_retries,
                "LANCE_CLIENT_READ_RETRIES",
                read_retries.as_deref(),
                DEFAULT_READ_RETRIES,
            )?,
            backoff_factor: resolve_backoff(
                retry_config.backoff_factor,
                "LANCE_CLIENT_RETRY_BACKOFF_FACTOR",
                backoff_factor.as_deref(),
                DEFAULT_BACKOFF_FACTOR,
            )?,
            backoff_jitter: resolve_backoff(
                retry_config.backoff_jitter,
                "LANCE_CLIENT_RETRY_BACKOFF_JITTER",
                backoff_jitter.as_deref(),
                DEFAULT_BACKOFF_JITTER,
            )?,
            statuses: resolve_statuses(
                retry_config.statuses,
                "LANCE_CLIENT_RETRY_STATUSES",
                statuses.as_deref(),
            )?,
        })
    }
}

impl TryFrom<RetryConfig> for ResolvedRetryConfig {
    type Error = Error;

    fn try_from(retry_config: RetryConfig) -> crate::Result<Self> {
        Self::resolve(retry_config, |name| std::env::var(name).ok())
    }
}
