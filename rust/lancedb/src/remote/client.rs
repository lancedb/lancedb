// Copyright 2024 LanceDB Developers.
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

use std::{future::Future, time::Duration};

use log::debug;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Request, RequestBuilder, Response,
};

use crate::error::{Error, Result};

const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Debug)]
pub struct ClientConfig {
    timeout_config: TimeoutConfig,
    retry_config: RetryConfig,
    user_agent: String,
    // TODO: how to configure request ids?
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout_config: TimeoutConfig::default(),
            retry_config: RetryConfig::default(),
            user_agent: concat!("LanceDB-Rust-Client/{}", env!("CARGO_PKG_VERSION")).into(),
        }
    }
}

#[derive(Default, Debug)]
pub struct TimeoutConfig {
    /// The timeout for creating a connection to the server.
    ///
    /// You can also set the `LANCE_CLIENT_CONNECT_TIMEOUT` environment variable
    /// to set this value. Use an integer value in seconds.
    ///
    /// The default is 120 seconds (2 minutes).
    pub connect_timeout: Option<Duration>,
    /// The timeout for reading a response from the server.
    ///
    /// You can also set the `LANCE_CLIENT_READ_TIMEOUT` environment variable
    /// to set this value. Use an integer value in seconds.
    ///
    /// The default is 300 seconds (5 minutes).
    pub read_timeout: Option<Duration>,
    /// The timeout for keeping idle connections alive.
    ///
    /// You can also set the `LANCE_CLIENT_CONNECTION_TIMEOUT` environment variable
    /// to set this value. Use an integer value in seconds.
    ///
    /// The default is 300 seconds (5 minutes).
    pub pool_idle_timeout: Option<Duration>,
}

#[derive(Default, Debug)]
pub struct RetryConfig {
    /// The number of times to retry a request if it fails.
    ///
    /// You can also set the `LANCE_CLIENT_MAX_RETRIES` environment variable
    /// to set this value. Use an integer value.
    ///
    /// The default is 3 retries.
    pub retries: Option<u8>,
    /// The number of times to retry a request if it fails to connect.
    ///
    /// You can also set the `LANCE_CLIENT_CONNECT_RETRIES` environment variable
    /// to set this value. Use an integer value.
    ///
    /// The default is 3 retries.
    pub connect_retries: Option<u8>,
    /// The number of times to retry a request if it fails to read.
    ///
    /// You can also set the `LANCE_CLIENT_READ_RETRIES` environment variable
    /// to set this value. Use an integer value.
    ///
    /// The default is 3 retries.
    pub read_retries: Option<u8>,
    /// The exponential backoff factor to use when retrying requests.
    ///
    /// Between each retry, the client will wait for the amount of seconds:
    ///
    /// ```text
    /// {backoff factor} * (2 ** ({number of previous retries}))
    /// ```
    ///
    /// You can also set the `LANCE_CLIENT_RETRY_BACKOFF_FACTOR` environment variable
    /// to set this value. Use a float value.
    ///
    /// The default is 0.25. So the first retry will wait 0.25 seconds, the second
    /// retry will wait 0.5 seconds, the third retry will wait 1 second, etc.
    pub backoff_factor: Option<f32>,
    /// The backoff jitter factor to use when retrying requests.
    ///
    /// The backoff jitter is a random value between 0 and the jitter factor in
    /// seconds.
    ///
    /// You can also set the `LANCE_CLIENT_RETRY_BACKOFF_JITTER` environment variable
    /// to set this value. Use a float value.
    ///
    /// The default is 0.25. So between 0 and 0.25 seconds will be added to the
    /// sleep time between retries.
    pub backoff_jitter: Option<f32>,
    /// The set of status codes to retry on.
    ///
    /// You can also set the `LANCE_CLIENT_RETRY_STATUSES` environment variable
    /// to set this value. Use a comma-separated list of integer values.
    ///
    /// The default is 429, 500, 502, 503.
    pub statuses: Option<Vec<u16>>,
    // TODO: should we allow customizing methods?
}

#[derive(Debug, Clone)]
struct ResolvedRetryConfig {
    retries: u8,
    connect_retries: u8,
    read_retries: u8,
    backoff_factor: f32,
    backoff_jitter: f32,
    statuses: Vec<reqwest::StatusCode>,
}

impl TryFrom<RetryConfig> for ResolvedRetryConfig {
    type Error = Error;

    fn try_from(retry_config: RetryConfig) -> Result<Self> {
        Ok(Self {
            retries: retry_config.retries.unwrap_or(3),
            connect_retries: retry_config.connect_retries.unwrap_or(3),
            read_retries: retry_config.read_retries.unwrap_or(3),
            backoff_factor: retry_config.backoff_factor.unwrap_or(0.25),
            backoff_jitter: retry_config.backoff_jitter.unwrap_or(0.25),
            statuses: retry_config
                .statuses
                .unwrap_or_else(|| vec![429, 500, 502, 503])
                .into_iter()
                .map(|status| reqwest::StatusCode::from_u16(status).unwrap())
                .collect(),
        })
    }
}

// We use the `HttpSend` trait to abstract over the `reqwest::Client` so that
// we can mock responses in tests. Based on the patterns from this blog post:
// https://write.as/balrogboogie/testing-reqwest-based-clients
#[derive(Clone, Debug)]
pub struct RestfulLanceDbClient<S: HttpSend = Sender> {
    client: reqwest::Client,
    host: String,
    retry_config: ResolvedRetryConfig,
    sender: S,
}

pub trait HttpSend: Clone + Send + Sync + std::fmt::Debug + 'static {
    fn send(
        &self,
        client: &reqwest::Client,
        request: reqwest::Request,
    ) -> impl Future<Output = reqwest::Result<Response>> + Send;
}

// Default implementation of HttpSend which sends the request normally with reqwest
#[derive(Clone, Debug)]
pub struct Sender;
impl HttpSend for Sender {
    async fn send(
        &self,
        client: &reqwest::Client,
        request: reqwest::Request,
    ) -> reqwest::Result<reqwest::Response> {
        client.execute(request).await
    }
}

impl RestfulLanceDbClient<Sender> {
    fn get_timeout(passed: Option<Duration>, env_var: &str, default: Duration) -> Result<Duration> {
        if let Some(passed) = passed {
            Ok(passed)
        } else if let Ok(timeout) = std::env::var(env_var) {
            let timeout = timeout.parse::<u64>().map_err(|_| Error::InvalidInput {
                message: format!(
                    "Invalid value for {} environment variable: '{}'",
                    env_var, timeout
                ),
            })?;
            Ok(Duration::from_secs(timeout))
        } else {
            Ok(default)
        }
    }

    pub fn try_new(
        db_url: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        client_config: ClientConfig,
    ) -> Result<Self> {
        let parsed_url = url::Url::parse(db_url)?;
        debug_assert_eq!(parsed_url.scheme(), "db");
        if !parsed_url.has_host() {
            return Err(Error::Http {
                message: format!("Invalid database URL (missing host) '{}'", db_url),
            });
        }
        let db_name = parsed_url.host_str().unwrap();

        // Get the timeouts
        let connect_timeout = Self::get_timeout(
            client_config.timeout_config.connect_timeout,
            "LANCE_CLIENT_CONNECT_TIMEOUT",
            Duration::from_secs(120),
        )?;
        let read_timeout = Self::get_timeout(
            client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Duration::from_secs(300),
        )?;
        let pool_idle_timeout = Self::get_timeout(
            client_config.timeout_config.pool_idle_timeout,
            // Though it's confusing with the connect_timeout name, this is the
            // legacy name for this in the Python sync client. So we keep as-is.
            "LANCE_CLIENT_CONNECTION_TIMEOUT",
            Duration::from_secs(300),
        )?;

        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .pool_idle_timeout(pool_idle_timeout)
            .default_headers(Self::default_headers(
                api_key,
                region,
                db_name,
                host_override.is_some(),
            )?)
            .user_agent(client_config.user_agent)
            .build()?;
        let host = match host_override {
            Some(host_override) => host_override,
            None => format!("https://{}.{}.api.lancedb.com", db_name, region),
        };
        let retry_config = client_config.retry_config.try_into()?;
        Ok(Self {
            client,
            host,
            retry_config,
            sender: Sender,
        })
    }
}

impl<S: HttpSend> RestfulLanceDbClient<S> {
    pub fn host(&self) -> &str {
        &self.host
    }

    fn default_headers(
        api_key: &str,
        region: &str,
        db_name: &str,
        has_host_override: bool,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-api-key",
            HeaderValue::from_str(api_key).map_err(|_| Error::Http {
                message: "non-ascii api key provided".to_string(),
            })?,
        );
        if region == "local" {
            let host = format!("{}.local.api.lancedb.com", db_name);
            headers.insert(
                "Host",
                HeaderValue::from_str(&host).map_err(|_| Error::Http {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }
        if has_host_override {
            headers.insert(
                "x-lancedb-database",
                HeaderValue::from_str(db_name).map_err(|_| Error::Http {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }

        Ok(headers)
    }

    pub fn get(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        self.client.get(full_uri)
    }

    pub fn post(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        self.client.post(full_uri)
    }

    pub async fn send(&self, req: RequestBuilder, with_retry: bool) -> Result<Response> {
        let (client, request) = req.build_split();
        let mut request = request.unwrap();

        // Set a request id.
        // TODO: allow the user to supply this, through middleware?
        if request.headers().get(REQUEST_ID_HEADER).is_none() {
            let request_id = uuid::Uuid::new_v4();
            let request_id = HeaderValue::from_str(&request_id.to_string()).unwrap();
            request.headers_mut().insert(REQUEST_ID_HEADER, request_id);
        }

        if with_retry {
            self.send_with_retry_impl(client, request).await
        } else {
            Ok(self.sender.send(&client, request).await?)
        }
    }

    async fn send_with_retry_impl(
        &self,
        client: reqwest::Client,
        req: Request,
    ) -> Result<Response> {
        let mut request_failures = 0;
        let mut connect_failures = 0;
        let mut read_failures = 0;

        loop {
            // This only works if the request body is not a stream. If it is
            // a stream, we can't use the retry path. We would need to implement
            // an outer retry.
            let request = req.try_clone().ok_or_else(|| Error::Http {
                message: "Attempted to retry a request that cannot be cloned".to_string(),
            })?;
            let response = self.sender.send(&client, request).await;
            let status_code = response.as_ref().map(|r| r.status());
            match status_code {
                Ok(status) if status.is_success() => return Ok(response?),
                Ok(status) if self.retry_config.statuses.contains(&status) => {
                    request_failures += 1;
                    if request_failures >= self.retry_config.retries {
                        // TODO: better error
                        return Err(Error::Runtime {
                            message: format!(
                                "Request failed after {} retries with status code {}",
                                request_failures, status
                            ),
                        });
                    }
                }
                Err(err) if err.is_connect() => {
                    connect_failures += 1;
                    if connect_failures >= self.retry_config.connect_retries {
                        return Err(Error::Runtime {
                            message: format!(
                                "Request failed after {} connect retries with error: {}",
                                connect_failures, err
                            ),
                        });
                    }
                }
                Err(err) if err.is_timeout() || err.is_body() || err.is_decode() => {
                    read_failures += 1;
                    if read_failures >= self.retry_config.read_retries {
                        return Err(Error::Runtime {
                            message: format!(
                                "Request failed after {} read retries with error: {}",
                                read_failures, err
                            ),
                        });
                    }
                }
                Ok(_) | Err(_) => return Ok(response?),
            }

            let backoff = self.retry_config.backoff_factor * (2.0f32.powi(request_failures as i32));
            let jitter = rand::random::<f32>() * self.retry_config.backoff_jitter;
            let sleep_time = Duration::from_secs_f32(backoff + jitter);
            debug!(
                "Retrying request {:?} ({}/{} connect, {}/{} read, {}/{} read) in {:?}",
                req.headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok()),
                connect_failures,
                self.retry_config.connect_retries,
                request_failures,
                self.retry_config.retries,
                read_failures,
                self.retry_config.read_retries,
                sleep_time
            );
            tokio::time::sleep(sleep_time).await;
        }
    }

    async fn rsp_to_str(response: Response) -> String {
        let status = response.status();
        response.text().await.unwrap_or_else(|_| status.to_string())
    }

    pub async fn check_response(&self, response: Response) -> Result<Response> {
        let status_int: u16 = u16::from(response.status());
        if (400..500).contains(&status_int) {
            Err(Error::InvalidInput {
                message: Self::rsp_to_str(response).await,
            })
        } else if status_int != 200 {
            Err(Error::Runtime {
                message: Self::rsp_to_str(response).await,
            })
        } else {
            Ok(response)
        }
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::sync::Arc;

    use super::*;

    #[derive(Clone)]
    pub struct MockSender {
        f: Arc<dyn Fn(reqwest::Request) -> reqwest::Response + Send + Sync + 'static>,
    }

    impl std::fmt::Debug for MockSender {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockSender")
        }
    }

    impl HttpSend for MockSender {
        async fn send(
            &self,
            _client: &reqwest::Client,
            request: reqwest::Request,
        ) -> reqwest::Result<reqwest::Response> {
            let response = (self.f)(request);
            Ok(response)
        }
    }

    pub fn client_with_handler<T>(
        handler: impl Fn(reqwest::Request) -> http::response::Response<T> + Send + Sync + 'static,
    ) -> RestfulLanceDbClient<MockSender>
    where
        T: Into<reqwest::Body>,
    {
        let wrapper = move |req: reqwest::Request| {
            let response = handler(req);
            response.into()
        };

        RestfulLanceDbClient {
            client: reqwest::Client::new(),
            host: "http://localhost".to_string(),
            retry_config: RetryConfig::default().try_into().unwrap(),
            sender: MockSender {
                f: Arc::new(wrapper),
            },
        }
    }
}
