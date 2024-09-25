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

use reqwest::{
    header::{HeaderMap, HeaderValue},
    RequestBuilder, Response,
};

use crate::error::{Error, Result};

#[derive(Default, Debug)]
pub struct ClientConfig {
    timeout_config: TimeoutConfig,
    retry_config: RetryConfig,
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
    /// You can also set the `LANCE_CLIENT_RETRY_BACKOFF_FACTOR` environment variable
    /// to set this value. Use a float value.
    ///
    /// The default is 0.25.
    pub backoff_factor: Option<f32>,
    /// The backoff jitter factor to use when retrying requests.
    ///
    /// You can also set the `LANCE_CLIENT_RETRY_BACKOFF_JITTER` environment variable
    /// to set this value. Use a float value.
    ///
    /// The default is 0.25.
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

// We use the `HttpSend` trait to abstract over the `reqwest::Client` so that
// we can mock responses in tests. Based on the patterns from this blog post:
// https://write.as/balrogboogie/testing-reqwest-based-clients
#[derive(Clone, Debug)]
pub struct RestfulLanceDbClient<S: HttpSend = Sender> {
    client: reqwest::Client,
    host: String,
    sender: S,
}

pub trait HttpSend: Clone + Send + Sync + std::fmt::Debug + 'static {
    fn send(&self, req: RequestBuilder) -> impl Future<Output = Result<Response>> + Send;
}

// Default implementation of HttpSend which sends the request normally with reqwest
#[derive(Clone, Debug)]
pub struct Sender;
impl HttpSend for Sender {
    async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        Ok(request.send().await?)
    }
}

impl RestfulLanceDbClient<Sender> {
    pub fn try_new(
        db_url: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        _client_config: ClientConfig,
    ) -> Result<Self> {
        let parsed_url = url::Url::parse(db_url)?;
        debug_assert_eq!(parsed_url.scheme(), "db");
        if !parsed_url.has_host() {
            return Err(Error::Http {
                message: format!("Invalid database URL (missing host) '{}'", db_url),
            });
        }
        let db_name = parsed_url.host_str().unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .default_headers(Self::default_headers(
                api_key,
                region,
                db_name,
                host_override.is_some(),
            )?)
            .build()?;
        let host = match host_override {
            Some(host_override) => host_override,
            None => format!("https://{}.{}.api.lancedb.com", db_name, region),
        };
        Ok(Self {
            client,
            host,
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

    pub async fn send(&self, req: RequestBuilder) -> Result<Response> {
        self.sender.send(req).await
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
        async fn send(&self, request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
            let request = request.build().unwrap();
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
            sender: MockSender {
                f: Arc::new(wrapper),
            },
        }
    }
}
