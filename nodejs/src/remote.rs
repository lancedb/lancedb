// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use napi_derive::*;

/// Timeout configuration for remote HTTP client.
#[napi(object)]
#[derive(Debug)]
pub struct TimeoutConfig {
    /// The timeout for establishing a connection in seconds. Default is 120
    /// seconds (2 minutes). This can also be set via the environment variable
    /// `LANCE_CLIENT_CONNECT_TIMEOUT`, as an integer number of seconds.
    pub connect_timeout: Option<f64>,
    /// The timeout for reading data from the server in seconds. Default is 300
    /// seconds (5 minutes). This can also be set via the environment variable
    /// `LANCE_CLIENT_READ_TIMEOUT`, as an integer number of seconds.
    pub read_timeout: Option<f64>,
    /// The timeout for keeping idle connections in the connection pool in seconds.
    /// Default is 300 seconds (5 minutes). This can also be set via the
    /// environment variable `LANCE_CLIENT_CONNECTION_TIMEOUT`, as an integer
    /// number of seconds.
    pub pool_idle_timeout: Option<f64>,
}

/// Retry configuration for the remote HTTP client.
#[napi(object)]
#[derive(Debug)]
pub struct RetryConfig {
    /// The maximum number of retries for a request. Default is 3. You can also
    /// set this via the environment variable `LANCE_CLIENT_MAX_RETRIES`.
    pub retries: Option<u8>,
    /// The maximum number of retries for connection errors. Default is 3. You
    /// can also set this via the environment variable `LANCE_CLIENT_CONNECT_RETRIES`.
    pub connect_retries: Option<u8>,
    /// The maximum number of retries for read errors. Default is 3. You can also
    /// set this via the environment variable `LANCE_CLIENT_READ_RETRIES`.
    pub read_retries: Option<u8>,
    /// The backoff factor to apply between retries. Default is 0.25. Between each retry
    /// the client will wait for the amount of seconds:
    /// `{backoff factor} * (2 ** ({number of previous retries}))`. So for the default
    /// of 0.25, the first retry will wait 0.25 seconds, the second retry will wait 0.5
    /// seconds, the third retry will wait 1 second, etc.
    ///
    /// You can also set this via the environment variable
    /// `LANCE_CLIENT_RETRY_BACKOFF_FACTOR`.
    pub backoff_factor: Option<f64>,
    /// The jitter to apply to the backoff factor, in seconds. Default is 0.25.
    ///
    /// A random value between 0 and `backoff_jitter` will be added to the backoff
    /// factor in seconds. So for the default of 0.25 seconds, between 0 and 250
    /// milliseconds will be added to the sleep between each retry.
    ///
    /// You can also set this via the environment variable
    /// `LANCE_CLIENT_RETRY_BACKOFF_JITTER`.
    pub backoff_jitter: Option<f64>,
    /// The HTTP status codes for which to retry the request. Default is
    /// [429, 500, 502, 503].
    ///
    /// You can also set this via the environment variable
    /// `LANCE_CLIENT_RETRY_STATUSES`. Use a comma-separated list of integers.
    pub statuses: Option<Vec<u16>>,
}

#[napi(object)]
#[derive(Debug, Default)]
pub struct ClientConfig {
    pub user_agent: Option<String>,
    pub retry_config: Option<RetryConfig>,
    pub timeout_config: Option<TimeoutConfig>,
}

impl From<TimeoutConfig> for lancedb::remote::TimeoutConfig {
    fn from(config: TimeoutConfig) -> Self {
        Self {
            connect_timeout: config
                .connect_timeout
                .map(std::time::Duration::from_secs_f64),
            read_timeout: config.read_timeout.map(std::time::Duration::from_secs_f64),
            pool_idle_timeout: config
                .pool_idle_timeout
                .map(std::time::Duration::from_secs_f64),
        }
    }
}

impl From<RetryConfig> for lancedb::remote::RetryConfig {
    fn from(config: RetryConfig) -> Self {
        Self {
            retries: config.retries,
            connect_retries: config.connect_retries,
            read_retries: config.read_retries,
            backoff_factor: config.backoff_factor.map(|v| v as f32),
            backoff_jitter: config.backoff_jitter.map(|v| v as f32),
            statuses: config.statuses,
        }
    }
}

impl From<ClientConfig> for lancedb::remote::ClientConfig {
    fn from(config: ClientConfig) -> Self {
        Self {
            user_agent: config
                .user_agent
                .unwrap_or(concat!("LanceDB-Node-Client/", env!("CARGO_PKG_VERSION")).to_string()),
            retry_config: config.retry_config.map(Into::into).unwrap_or_default(),
            timeout_config: config.timeout_config.map(Into::into).unwrap_or_default(),
        }
    }
}
