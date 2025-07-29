// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use http::HeaderName;
use log::debug;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Body, Request, RequestBuilder, Response,
};
use std::{collections::HashMap, future::Future, str::FromStr, time::Duration};

use crate::error::{Error, Result};
use crate::remote::db::RemoteOptions;
use crate::remote::retry::{ResolvedRetryConfig, RetryCounter};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Configuration for the LanceDB Cloud HTTP client.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub timeout_config: TimeoutConfig,
    pub retry_config: RetryConfig,
    /// User agent to use for requests. The default provides the library
    /// name and version.
    pub user_agent: String,
    // TODO: how to configure request ids?
    pub extra_headers: HashMap<String, String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout_config: TimeoutConfig::default(),
            retry_config: RetryConfig::default(),
            user_agent: concat!("LanceDB-Rust-Client/", env!("CARGO_PKG_VERSION")).into(),
            extra_headers: HashMap::new(),
        }
    }
}

/// How to handle timeouts for HTTP requests.
#[derive(Clone, Default, Debug)]
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

/// How to handle retries for HTTP requests.
#[derive(Clone, Default, Debug)]
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
    /// Note that write operations will never be retried on 5xx errors as this may
    /// result in duplicated writes.
    ///
    /// The default is 409, 429, 500, 502, 503, 504.
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
    pub(crate) retry_config: ResolvedRetryConfig,
    pub(crate) sender: S,
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
        options: &RemoteOptions,
    ) -> Result<Self> {
        let parsed_url = url::Url::parse(db_url).map_err(|err| Error::InvalidInput {
            message: format!("db_url is not a valid URL. '{db_url}'. Error: {err}"),
        })?;
        debug_assert_eq!(parsed_url.scheme(), "db");
        if !parsed_url.has_host() {
            return Err(Error::InvalidInput {
                message: format!("Invalid database URL (missing host) '{}'", db_url),
            });
        }
        let db_name = parsed_url.host_str().unwrap();
        let db_prefix = {
            let prefix = parsed_url.path().trim_start_matches('/');
            if prefix.is_empty() {
                None
            } else {
                Some(prefix)
            }
        };

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
                options,
                db_prefix,
                &client_config,
            )?)
            .user_agent(client_config.user_agent)
            .build()
            .map_err(|err| Error::Other {
                message: "Failed to build HTTP client".into(),
                source: Some(Box::new(err)),
            })?;

        let host = match host_override {
            Some(host_override) => host_override,
            None => format!("https://{}.{}.api.lancedb.com", db_name, region),
        };
        debug!("Created client for host: {}", host);
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
        options: &RemoteOptions,
        db_prefix: Option<&str>,
        config: &ClientConfig,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_str(api_key).map_err(|_| Error::InvalidInput {
                message: "non-ascii api key provided".to_string(),
            })?,
        );
        if region == "local" {
            let host = format!("{}.local.api.lancedb.com", db_name);
            headers.insert(
                http::header::HOST,
                HeaderValue::from_str(&host).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }
        if has_host_override {
            headers.insert(
                HeaderName::from_static("x-lancedb-database"),
                HeaderValue::from_str(db_name).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii database name '{}' provided", db_name),
                })?,
            );
        }
        if db_prefix.is_some() {
            headers.insert(
                HeaderName::from_static("x-lancedb-database-prefix"),
                HeaderValue::from_str(db_prefix.unwrap()).map_err(|_| Error::InvalidInput {
                    message: format!(
                        "non-ascii database prefix '{}' provided",
                        db_prefix.unwrap()
                    ),
                })?,
            );
        }

        if let Some(v) = options.0.get("account_name") {
            headers.insert(
                HeaderName::from_static("x-azure-storage-account-name"),
                HeaderValue::from_str(v).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii storage account name '{}' provided", db_name),
                })?,
            );
        }
        if let Some(v) = options.0.get("azure_storage_account_name") {
            headers.insert(
                HeaderName::from_static("x-azure-storage-account-name"),
                HeaderValue::from_str(v).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii storage account name '{}' provided", db_name),
                })?,
            );
        }

        for (key, value) in &config.extra_headers {
            let key_parsed = HeaderName::from_str(key).map_err(|_| Error::InvalidInput {
                message: format!("non-ascii value for header '{}' provided", key),
            })?;
            headers.insert(
                key_parsed,
                HeaderValue::from_str(value).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii value for header '{}' provided", key),
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

    pub fn put(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        self.client.put(full_uri)
    }

    pub async fn send(&self, req: RequestBuilder) -> Result<(String, Response)> {
        let (client, request) = req.build_split();
        let mut request = request.unwrap();
        let request_id = self.extract_request_id(&mut request);
        self.log_request(&request, &request_id);

        let response = self
            .sender
            .send(&client, request)
            .await
            .err_to_http(request_id.clone())?;
        debug!(
            "Received response for request_id={}: {:?}",
            request_id, &response
        );
        Ok((request_id, response))
    }

    /// Send the request using retries configured in the RetryConfig.
    /// If retry_5xx is false, 5xx requests will not be retried regardless of the statuses configured
    /// in the RetryConfig.
    /// Since this requires arrow serialization, this is implemented here instead of in RestfulLanceDbClient
    pub async fn send_with_retry(
        &self,
        req_builder: RequestBuilder,
        mut make_body: Option<Box<dyn FnMut() -> Result<Body> + Send + 'static>>,
        retry_5xx: bool,
    ) -> Result<(String, Response)> {
        let retry_config = &self.retry_config;
        let non_5xx_statuses = retry_config
            .statuses
            .iter()
            .filter(|s| !s.is_server_error())
            .cloned()
            .collect::<Vec<_>>();

        // clone and build the request to extract the request id
        let tmp_req = req_builder.try_clone().ok_or_else(|| Error::Runtime {
            message: "Attempted to retry a request that cannot be cloned".to_string(),
        })?;
        let (_, r) = tmp_req.build_split();
        let mut r = r.unwrap();
        let request_id = self.extract_request_id(&mut r);
        let mut retry_counter = RetryCounter::new(retry_config, request_id.clone());

        loop {
            let mut req_builder = req_builder.try_clone().ok_or_else(|| Error::Runtime {
                message: "Attempted to retry a request that cannot be cloned".to_string(),
            })?;

            // set the streaming body on the request builder after clone
            if let Some(body_gen) = make_body.as_mut() {
                let body = body_gen()?;
                req_builder = req_builder.body(body);
            }

            let (c, request) = req_builder.build_split();
            let mut request = request.unwrap();
            self.set_request_id(&mut request, &request_id.clone());
            self.log_request(&request, &request_id);

            let response = self.sender.send(&c, request).await.map(|r| (r.status(), r));

            match response {
                Ok((status, response)) if status.is_success() => {
                    debug!(
                        "Received response for request_id={}: {:?}",
                        retry_counter.request_id, &response
                    );
                    return Ok((retry_counter.request_id, response));
                }
                Ok((status, response))
                    if (retry_5xx && retry_config.statuses.contains(&status))
                        || non_5xx_statuses.contains(&status) =>
                {
                    let source = self
                        .check_response(&retry_counter.request_id, response)
                        .await
                        .unwrap_err();
                    retry_counter.increment_request_failures(source)?;
                }
                Err(err) if err.is_connect() => {
                    retry_counter.increment_connect_failures(err)?;
                }
                Err(err) if err.is_timeout() || err.is_body() || err.is_decode() => {
                    retry_counter.increment_read_failures(err)?;
                }
                Err(err) => {
                    let status_code = err.status();
                    return Err(Error::Http {
                        source: Box::new(err),
                        request_id: retry_counter.request_id,
                        status_code,
                    });
                }
                Ok((_, response)) => return Ok((retry_counter.request_id, response)),
            }

            let sleep_time = retry_counter.next_sleep_time();
            tokio::time::sleep(sleep_time).await;
        }
    }

    fn log_request(&self, request: &Request, request_id: &String) {
        if log::log_enabled!(log::Level::Debug) {
            let content_type = request
                .headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap());
            if content_type == Some("application/json") {
                let body = request.body().as_ref().unwrap().as_bytes().unwrap();
                let body = String::from_utf8_lossy(body);
                debug!(
                    "Sending request_id={}: {:?} with body {}",
                    request_id, request, body
                );
            } else {
                debug!("Sending request_id={}: {:?}", request_id, request);
            }
        }
    }

    /// Extract the request ID from the request headers.
    /// If the request ID header is not set, this will generate a new one and set
    /// it on the request headers
    pub fn extract_request_id(&self, request: &mut Request) -> String {
        // Set a request id.
        // TODO: allow the user to supply this, through middleware?
        let request_id = if let Some(request_id) = request.headers().get(REQUEST_ID_HEADER) {
            request_id.to_str().unwrap().to_string()
        } else {
            let request_id = uuid::Uuid::new_v4().to_string();
            self.set_request_id(request, &request_id);
            request_id
        };
        request_id
    }

    /// Set the request ID header
    pub fn set_request_id(&self, request: &mut Request, request_id: &str) {
        let header = HeaderValue::from_str(request_id).unwrap();
        request.headers_mut().insert(REQUEST_ID_HEADER, header);
    }

    pub async fn check_response(&self, request_id: &str, response: Response) -> Result<Response> {
        // Try to get the response text, but if that fails, just return the status code
        let status = response.status();
        if status.is_success() {
            Ok(response)
        } else {
            let response_text = response.text().await.ok();
            let message = if let Some(response_text) = response_text {
                format!("{}: {}", status, response_text)
            } else {
                status.to_string()
            };
            Err(Error::Http {
                source: message.into(),
                request_id: request_id.into(),
                status_code: Some(status),
            })
        }
    }
}

pub trait RequestResultExt {
    type Output;
    fn err_to_http(self, request_id: String) -> Result<Self::Output>;
}

impl<T> RequestResultExt for reqwest::Result<T> {
    type Output = T;
    fn err_to_http(self, request_id: String) -> Result<T> {
        self.map_err(|err| {
            let status_code = err.status();
            Error::Http {
                source: Box::new(err),
                request_id,
                status_code,
            }
        })
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
