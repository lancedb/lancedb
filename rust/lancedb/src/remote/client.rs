// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use http::HeaderName;
use log::debug;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Body, Request, RequestBuilder, Response,
};
use std::{collections::HashMap, future::Future, str::FromStr, sync::Arc, time::Duration};

use crate::error::{Error, Result};
use crate::remote::db::RemoteOptions;
use crate::remote::retry::{ResolvedRetryConfig, RetryCounter};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// Configuration for TLS/mTLS settings.
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    /// Path to the client certificate file (PEM format)
    pub cert_file: Option<String>,
    /// Path to the client private key file (PEM format)
    pub key_file: Option<String>,
    /// Path to the CA certificate file for server verification (PEM format)
    pub ssl_ca_cert: Option<String>,
    /// Whether to verify the hostname in the server's certificate
    pub assert_hostname: bool,
}

/// Trait for providing custom headers for each request
#[async_trait::async_trait]
pub trait HeaderProvider: Send + Sync + std::fmt::Debug {
    /// Get the latest headers to be added to the request
    async fn get_headers(&self) -> Result<HashMap<String, String>>;
}

/// Configuration for the LanceDB Cloud HTTP client.
#[derive(Clone)]
pub struct ClientConfig {
    pub timeout_config: TimeoutConfig,
    pub retry_config: RetryConfig,
    /// User agent to use for requests. The default provides the library
    /// name and version.
    pub user_agent: String,
    // TODO: how to configure request ids?
    pub extra_headers: HashMap<String, String>,
    /// The delimiter to use when constructing object identifiers.
    /// If not default, passes as query parameter.
    pub id_delimiter: Option<String>,
    /// TLS configuration for mTLS support
    pub tls_config: Option<TlsConfig>,
    /// Provider for custom headers to be added to each request
    pub header_provider: Option<Arc<dyn HeaderProvider>>,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("timeout_config", &self.timeout_config)
            .field("retry_config", &self.retry_config)
            .field("user_agent", &self.user_agent)
            .field("extra_headers", &self.extra_headers)
            .field("id_delimiter", &self.id_delimiter)
            .field("tls_config", &self.tls_config)
            .field(
                "header_provider",
                &self.header_provider.as_ref().map(|_| "Some(...)"),
            )
            .finish()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout_config: TimeoutConfig::default(),
            retry_config: RetryConfig::default(),
            user_agent: concat!("LanceDB-Rust-Client/", env!("CARGO_PKG_VERSION")).into(),
            extra_headers: HashMap::new(),
            id_delimiter: None,
            tls_config: None,
            header_provider: None,
        }
    }
}

/// How to handle timeouts for HTTP requests.
#[derive(Clone, Default, Debug)]
pub struct TimeoutConfig {
    /// The overall timeout for the entire request.
    ///
    /// This includes connection, send, and read time. If the entire request
    /// doesn't complete within this time, it will fail.
    ///
    /// You can also set the `LANCE_CLIENT_TIMEOUT` environment variable
    /// to set this value. Use an integer value in seconds.
    ///
    /// By default, no overall timeout is set.
    pub timeout: Option<Duration>,
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
#[derive(Clone)]
pub struct RestfulLanceDbClient<S: HttpSend = Sender> {
    client: reqwest::Client,
    host: String,
    pub(crate) retry_config: ResolvedRetryConfig,
    pub(crate) sender: S,
    pub(crate) id_delimiter: String,
    pub(crate) header_provider: Option<Arc<dyn HeaderProvider>>,
}

impl<S: HttpSend> std::fmt::Debug for RestfulLanceDbClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestfulLanceDbClient")
            .field("host", &self.host)
            .field("retry_config", &self.retry_config)
            .field("sender", &self.sender)
            .field("id_delimiter", &self.id_delimiter)
            .field(
                "header_provider",
                &self.header_provider.as_ref().map(|_| "Some(...)"),
            )
            .finish()
    }
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
    fn get_timeout(passed: Option<Duration>, env_var: &str) -> Result<Option<Duration>> {
        if let Some(passed) = passed {
            Ok(Some(passed))
        } else if let Ok(timeout) = std::env::var(env_var) {
            let timeout = timeout.parse::<u64>().map_err(|_| Error::InvalidInput {
                message: format!(
                    "Invalid value for {} environment variable: '{}'",
                    env_var, timeout
                ),
            })?;
            Ok(Some(Duration::from_secs(timeout)))
        } else {
            Ok(None)
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
        let timeout =
            Self::get_timeout(client_config.timeout_config.timeout, "LANCE_CLIENT_TIMEOUT")?;
        let connect_timeout = Self::get_timeout(
            client_config.timeout_config.connect_timeout,
            "LANCE_CLIENT_CONNECT_TIMEOUT",
        )?
        .unwrap_or_else(|| Duration::from_secs(120));
        let read_timeout = Self::get_timeout(
            client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
        )?
        .unwrap_or_else(|| Duration::from_secs(300));
        let pool_idle_timeout = Self::get_timeout(
            client_config.timeout_config.pool_idle_timeout,
            // Though it's confusing with the connect_timeout name, this is the
            // legacy name for this in the Python sync client. So we keep as-is.
            "LANCE_CLIENT_CONNECTION_TIMEOUT",
        )?
        .unwrap_or_else(|| Duration::from_secs(300));

        let mut client_builder = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .read_timeout(read_timeout)
            .pool_idle_timeout(pool_idle_timeout);
        if let Some(timeout) = timeout {
            client_builder = client_builder.timeout(timeout);
        }

        // Configure mTLS if TlsConfig is provided
        if let Some(tls_config) = &client_config.tls_config {
            // Load client certificate and key for mTLS
            if let (Some(cert_file), Some(key_file)) = (&tls_config.cert_file, &tls_config.key_file)
            {
                let cert = std::fs::read(cert_file).map_err(|err| Error::Other {
                    message: format!("Failed to read certificate file: {}", cert_file),
                    source: Some(Box::new(err)),
                })?;
                let key = std::fs::read(key_file).map_err(|err| Error::Other {
                    message: format!("Failed to read key file: {}", key_file),
                    source: Some(Box::new(err)),
                })?;

                let identity = reqwest::Identity::from_pem(&[&cert[..], &key[..]].concat())
                    .map_err(|err| Error::Other {
                        message: "Failed to create client identity from certificate and key".into(),
                        source: Some(Box::new(err)),
                    })?;
                client_builder = client_builder.identity(identity);
            }

            // Load CA certificate for server verification
            if let Some(ca_cert_file) = &tls_config.ssl_ca_cert {
                let ca_cert = std::fs::read(ca_cert_file).map_err(|err| Error::Other {
                    message: format!("Failed to read CA certificate file: {}", ca_cert_file),
                    source: Some(Box::new(err)),
                })?;

                let ca_cert =
                    reqwest::Certificate::from_pem(&ca_cert).map_err(|err| Error::Other {
                        message: "Failed to create CA certificate from PEM".into(),
                        source: Some(Box::new(err)),
                    })?;
                client_builder = client_builder.add_root_certificate(ca_cert);
            }

            // Configure hostname verification
            client_builder =
                client_builder.danger_accept_invalid_hostnames(!tls_config.assert_hostname);
        }

        let client = client_builder
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
        let retry_config = client_config.retry_config.clone().try_into()?;
        Ok(Self {
            client,
            host,
            retry_config,
            sender: Sender,
            id_delimiter: client_config
                .id_delimiter
                .clone()
                .unwrap_or("$".to_string()),
            header_provider: client_config.header_provider,
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
        let builder = self.client.get(full_uri);
        self.add_id_delimiter_query_param(builder)
    }

    pub fn post(&self, uri: &str) -> RequestBuilder {
        let full_uri = format!("{}{}", self.host, uri);
        let builder = self.client.post(full_uri);
        self.add_id_delimiter_query_param(builder)
    }

    fn add_id_delimiter_query_param(&self, req: RequestBuilder) -> RequestBuilder {
        if self.id_delimiter != "$" {
            req.query(&[("delimiter", self.id_delimiter.clone())])
        } else {
            req
        }
    }

    /// Apply dynamic headers from the header provider if configured
    async fn apply_dynamic_headers(&self, mut request: Request) -> Result<Request> {
        if let Some(ref provider) = self.header_provider {
            let headers = provider.get_headers().await?;
            let request_headers = request.headers_mut();
            for (key, value) in headers {
                if let Ok(header_name) = HeaderName::from_str(&key) {
                    if let Ok(header_value) = HeaderValue::from_str(&value) {
                        request_headers.insert(header_name, header_value);
                    } else {
                        debug!("Invalid header value for key {}: {}", key, value);
                    }
                } else {
                    debug!("Invalid header name: {}", key);
                }
            }
        }
        Ok(request)
    }

    pub async fn send(&self, req: RequestBuilder) -> Result<(String, Response)> {
        let (client, request) = req.build_split();
        let mut request = request.unwrap();
        let request_id = self.extract_request_id(&mut request);

        // Apply dynamic headers before sending
        request = self.apply_dynamic_headers(request).await?;

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

            // Apply dynamic headers before each retry attempt
            request = self.apply_dynamic_headers(request).await?;

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
    use std::convert::TryInto;
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
            id_delimiter: "$".to_string(),
            header_provider: None,
        }
    }

    pub fn client_with_handler_and_config<T>(
        handler: impl Fn(reqwest::Request) -> http::response::Response<T> + Send + Sync + 'static,
        config: ClientConfig,
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
            retry_config: config.retry_config.try_into().unwrap(),
            sender: MockSender {
                f: Arc::new(wrapper),
            },
            id_delimiter: config.id_delimiter.unwrap_or_else(|| "$".to_string()),
            header_provider: config.header_provider,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_timeout_config_default() {
        let config = TimeoutConfig::default();
        assert!(config.timeout.is_none());
        assert!(config.connect_timeout.is_none());
        assert!(config.read_timeout.is_none());
        assert!(config.pool_idle_timeout.is_none());
    }

    #[test]
    fn test_timeout_config_with_overall_timeout() {
        let config = TimeoutConfig {
            timeout: Some(Duration::from_secs(60)),
            connect_timeout: Some(Duration::from_secs(10)),
            read_timeout: Some(Duration::from_secs(30)),
            pool_idle_timeout: Some(Duration::from_secs(300)),
        };

        assert_eq!(config.timeout, Some(Duration::from_secs(60)));
        assert_eq!(config.connect_timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.read_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.pool_idle_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_client_config_with_timeout() {
        let timeout_config = TimeoutConfig {
            timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        };

        let client_config = ClientConfig {
            timeout_config,
            ..Default::default()
        };

        assert_eq!(
            client_config.timeout_config.timeout,
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(config.cert_file.is_none());
        assert!(config.key_file.is_none());
        assert!(config.ssl_ca_cert.is_none());
        assert!(!config.assert_hostname);
    }

    #[test]
    fn test_tls_config_with_mtls() {
        let tls_config = TlsConfig {
            cert_file: Some("/path/to/cert.pem".to_string()),
            key_file: Some("/path/to/key.pem".to_string()),
            ssl_ca_cert: Some("/path/to/ca.pem".to_string()),
            assert_hostname: true,
        };

        assert_eq!(tls_config.cert_file, Some("/path/to/cert.pem".to_string()));
        assert_eq!(tls_config.key_file, Some("/path/to/key.pem".to_string()));
        assert_eq!(tls_config.ssl_ca_cert, Some("/path/to/ca.pem".to_string()));
        assert!(tls_config.assert_hostname);
    }

    #[test]
    fn test_client_config_with_tls() {
        let tls_config = TlsConfig {
            cert_file: Some("/path/to/cert.pem".to_string()),
            key_file: Some("/path/to/key.pem".to_string()),
            ssl_ca_cert: None,
            assert_hostname: false,
        };

        let client_config = ClientConfig {
            tls_config: Some(tls_config.clone()),
            ..Default::default()
        };

        assert!(client_config.tls_config.is_some());
        let config_tls = client_config.tls_config.unwrap();
        assert_eq!(config_tls.cert_file, Some("/path/to/cert.pem".to_string()));
        assert_eq!(config_tls.key_file, Some("/path/to/key.pem".to_string()));
        assert!(config_tls.ssl_ca_cert.is_none());
        assert!(!config_tls.assert_hostname);
    }

    // Test implementation of HeaderProvider
    #[derive(Debug, Clone)]
    struct TestHeaderProvider {
        headers: HashMap<String, String>,
    }

    impl TestHeaderProvider {
        fn new(headers: HashMap<String, String>) -> Self {
            Self { headers }
        }
    }

    #[async_trait::async_trait]
    impl HeaderProvider for TestHeaderProvider {
        async fn get_headers(&self) -> Result<HashMap<String, String>> {
            Ok(self.headers.clone())
        }
    }

    // Test implementation that returns an error
    #[derive(Debug)]
    struct ErrorHeaderProvider;

    #[async_trait::async_trait]
    impl HeaderProvider for ErrorHeaderProvider {
        async fn get_headers(&self) -> Result<HashMap<String, String>> {
            Err(Error::Runtime {
                message: "Failed to get headers".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn test_client_config_with_header_provider() {
        let mut headers = HashMap::new();
        headers.insert("X-API-Key".to_string(), "secret-key".to_string());

        let provider = TestHeaderProvider::new(headers);
        let client_config = ClientConfig {
            header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
            ..Default::default()
        };

        assert!(client_config.header_provider.is_some());
    }

    #[tokio::test]
    async fn test_apply_dynamic_headers() {
        // Create a mock client with header provider
        let mut headers = HashMap::new();
        headers.insert("X-Dynamic".to_string(), "dynamic-value".to_string());

        let provider = TestHeaderProvider::new(headers);

        // Create a simple request
        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://example.com/test".parse().unwrap(),
        );

        // Create client with header provider
        let client = RestfulLanceDbClient {
            client: reqwest::Client::new(),
            host: "https://example.com".to_string(),
            retry_config: RetryConfig::default().try_into().unwrap(),
            sender: Sender,
            id_delimiter: "+".to_string(),
            header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        };

        // Apply dynamic headers
        let updated_request = client.apply_dynamic_headers(request).await.unwrap();

        // Check that the header was added
        assert_eq!(
            updated_request.headers().get("X-Dynamic").unwrap(),
            "dynamic-value"
        );
    }

    #[tokio::test]
    async fn test_apply_dynamic_headers_merge() {
        // Test that dynamic headers override existing headers
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer new-token".to_string());
        headers.insert("X-Custom".to_string(), "custom-value".to_string());

        let provider = TestHeaderProvider::new(headers);

        // Create request with existing Authorization header
        let mut request_builder = reqwest::Client::new().get("https://example.com/test");
        request_builder = request_builder.header("Authorization", "Bearer old-token");
        request_builder = request_builder.header("X-Existing", "existing-value");
        let request = request_builder.build().unwrap();

        // Create client with header provider
        let client = RestfulLanceDbClient {
            client: reqwest::Client::new(),
            host: "https://example.com".to_string(),
            retry_config: RetryConfig::default().try_into().unwrap(),
            sender: Sender,
            id_delimiter: "+".to_string(),
            header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        };

        // Apply dynamic headers
        let updated_request = client.apply_dynamic_headers(request).await.unwrap();

        // Check that dynamic headers override existing ones
        assert_eq!(
            updated_request.headers().get("Authorization").unwrap(),
            "Bearer new-token"
        );
        assert_eq!(
            updated_request.headers().get("X-Custom").unwrap(),
            "custom-value"
        );
        // Existing headers should still be present
        assert_eq!(
            updated_request.headers().get("X-Existing").unwrap(),
            "existing-value"
        );
    }

    #[tokio::test]
    async fn test_apply_dynamic_headers_with_error_provider() {
        let provider = ErrorHeaderProvider;

        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://example.com/test".parse().unwrap(),
        );

        let client = RestfulLanceDbClient {
            client: reqwest::Client::new(),
            host: "https://example.com".to_string(),
            retry_config: RetryConfig::default().try_into().unwrap(),
            sender: Sender,
            id_delimiter: "+".to_string(),
            header_provider: Some(Arc::new(provider) as Arc<dyn HeaderProvider>),
        };

        // Header provider errors should fail the request
        // This is important for security - if auth headers can't be fetched, don't proceed
        let result = client.apply_dynamic_headers(request).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::Runtime { message } => {
                assert_eq!(message, "Failed to get headers");
            }
            _ => panic!("Expected Runtime error"),
        }
    }
}
