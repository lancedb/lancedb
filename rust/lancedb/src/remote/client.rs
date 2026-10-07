// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use http::HeaderName;
use log::debug;
use reqwest::{
    Body, Request, RequestBuilder, Response,
    header::{HeaderMap, HeaderValue},
};
use std::{collections::HashMap, future::Future, str::FromStr, sync::Arc, time::Duration};

use crate::error::{Error, Result};
use crate::remote::db::RemoteOptions;
use crate::remote::retry::{ResolvedRetryConfig, RetryCounter};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

pub fn redact_sensitive_headers(headers: &mut HeaderMap) {
    const SENSITIVE_HEADERS: [&str; 5] = [
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
    ];

    for (name, value) in headers.iter_mut() {
        if SENSITIVE_HEADERS
            .iter()
            .any(|sensitive| name.as_str().eq_ignore_ascii_case(sensitive))
        {
            value.set_sensitive(true);
        }
    }
}

/// Configuration for TLS/mTLS settings.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    /// Path to the client certificate file (PEM format)
    pub cert_file: Option<String>,
    /// Path to the client private key file (PEM format)
    pub key_file: Option<String>,
    /// Path to the CA certificate file for server verification (PEM format)
    pub ssl_ca_cert: Option<String>,
    /// Whether to verify the hostname in the server's certificate.
    /// Defaults to `true`.
    pub assert_hostname: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_file: None,
            key_file: None,
            ssl_ca_cert: None,
            assert_hostname: true,
        }
    }
}

/// Trait for providing custom headers for each request
#[async_trait::async_trait]
pub trait HeaderProvider: Send + Sync + std::fmt::Debug {
    /// Get the latest headers to be added to the request
    async fn get_headers(&self) -> Result<HashMap<String, String>>;
}

/// Default maximum bytes per insert request (8 GiB).
///
/// Sized so a multipart part can hold at least one full Lance data file (the
/// default is 1M rows / 90 GB per file), which keeps fragments from being split
/// into undersized files across parts. The time-based cut
/// ([`DEFAULT_MAX_REQUEST_DURATION_DIVISOR`]) bounds request duration on slow
/// uploads, so a large byte budget does not risk the read timeout.
const DEFAULT_MAX_BYTES_PER_REQUEST: u64 = 8 * 1024 * 1024 * 1024;

/// The default max request duration is the read timeout divided by this, leaving
/// headroom for the server to finalize and acknowledge a part before the read
/// timeout (which also covers the request-body upload) fires.
const DEFAULT_MAX_REQUEST_DURATION_DIVISOR: u32 = 2;

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
    /// The delimiter joining a namespace path and a name into one object
    /// identifier. [`ID_DELIMITER`] is the only accepted value; any other is
    /// refused by [`ClientConfig::validate`].
    pub id_delimiter: Option<String>,
    /// TLS configuration for mTLS support
    pub tls_config: Option<TlsConfig>,
    /// Provider for custom headers to be added to each request
    pub header_provider: Option<Arc<dyn HeaderProvider>>,
    /// User identifier for tracking purposes.
    ///
    /// This is sent as the `x-lancedb-user-id` header in requests to LanceDB Cloud/Enterprise.
    /// It can be set directly, or via the `LANCEDB_USER_ID` environment variable.
    /// Alternatively, set `LANCEDB_USER_ID_ENV_KEY` to specify another environment
    /// variable that contains the user ID value.
    pub user_id: Option<String>,
    /// Maximum number of bytes to send in a single insert HTTP request.
    ///
    /// During a multipart write, each partition's data is split into one or more
    /// parts of at most this many (Arrow IPC, compressed) bytes, each uploaded as
    /// a separate request under the shared upload id. This bounds how long any
    /// one request stays open, so large bulk ingests do not exceed the client
    /// read timeout while the server streams the part to object storage.
    ///
    /// The request body is still streamed (not buffered), so this does not
    /// increase peak memory. Set to `Some(0)` to disable splitting (one request
    /// per partition). You can also set the `LANCE_CLIENT_MAX_BYTES_PER_REQUEST`
    /// environment variable. Defaults to 8 GiB.
    pub max_bytes_per_request: Option<u64>,
    /// Maximum wall-clock time to spend uploading a single insert HTTP request.
    ///
    /// Complements [`Self::max_bytes_per_request`]: during a multipart write a
    /// part is cut when it reaches either the byte budget or this duration,
    /// whichever comes first. The client read timeout also covers the
    /// request-body upload, so a slow or throttled upload of a large part can
    /// hit that timeout before the byte budget is reached; cutting by time keeps
    /// each request short enough that it completes (and the server acknowledges
    /// the part) within the read timeout.
    ///
    /// Set to `Some(Duration::ZERO)` to disable the time-based cut. You can also
    /// set the `LANCE_CLIENT_MAX_REQUEST_DURATION` environment variable (integer
    /// seconds). Defaults to half the resolved read timeout.
    pub max_request_duration: Option<Duration>,
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
            .field("user_id", &self.user_id)
            .field("max_bytes_per_request", &self.max_bytes_per_request)
            .field("max_request_duration", &self.max_request_duration)
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
            user_id: None,
            max_bytes_per_request: None,
            max_request_duration: None,
        }
    }
}

impl ClientConfig {
    /// Resolve the user ID from the config or environment variables.
    ///
    /// Resolution order:
    /// 1. If `user_id` is set in the config, use that value
    /// 2. If `LANCEDB_USER_ID` environment variable is set, use that value
    /// 3. If `LANCEDB_USER_ID_ENV_KEY` is set, read the env var it points to
    /// 4. Otherwise, return None
    pub fn resolve_user_id(&self) -> Option<String> {
        if self.user_id.is_some() {
            return self.user_id.clone();
        }

        if let Ok(user_id) = std::env::var("LANCEDB_USER_ID")
            && !user_id.is_empty()
        {
            return Some(user_id);
        }

        if let Ok(env_key) = std::env::var("LANCEDB_USER_ID_ENV_KEY")
            && let Ok(user_id) = std::env::var(&env_key)
            && !user_id.is_empty()
        {
            return Some(user_id);
        }

        None
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
    pub(crate) header_provider: Option<Arc<dyn HeaderProvider>>,
    /// Connection-level read consistency interval. Drives the
    /// `x-lancedb-min-timestamp` freshness header sent on read requests.
    pub(crate) read_consistency_interval: Option<Duration>,
    // Note the `Option` here means the opposite of the same-named
    // `ClientConfig` fields: those are pre-resolution, where `None` means "fall
    // back to env var / default". These are post-resolution (see
    // `resolve_max_bytes_per_request` / `resolve_max_request_duration`), where a
    // default has already been applied and `None` means the feature is disabled.
    /// Maximum bytes per insert request. `None` disables request splitting.
    pub(crate) max_bytes_per_request: Option<u64>,
    /// Maximum wall-clock time per insert request. `None` disables the
    /// time-based part cut.
    pub(crate) max_request_duration: Option<Duration>,
}

impl<S: HttpSend> std::fmt::Debug for RestfulLanceDbClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestfulLanceDbClient")
            .field("host", &self.host)
            .field("retry_config", &self.retry_config)
            .field("sender", &self.sender)
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

/// Parsed components from a database URL (db://...)
pub struct ParsedDbUrl {
    pub db_name: String,
    pub db_prefix: Option<String>,
}

/// Parse a database URL and extract the database name and optional prefix.
///
/// Expected format: `db://db_name` or `db://db_name/prefix`
pub fn parse_db_url(db_url: &str) -> Result<ParsedDbUrl> {
    let parsed_url = url::Url::parse(db_url).map_err(|err| Error::InvalidInput {
        message: format!("db_url is not a valid URL. '{db_url}'. Error: {err}"),
    })?;
    debug_assert_eq!(parsed_url.scheme(), "db");
    if !parsed_url.has_host() {
        return Err(Error::InvalidInput {
            message: format!("Invalid database URL (missing host) '{}'", db_url),
        });
    }
    let db_name = urlencoding::decode(parsed_url.host_str().unwrap())
        .map_err(|err| Error::InvalidInput {
            message: format!("Invalid encoded database name: {err}"),
        })?
        .into_owned();
    let db_prefix = {
        let prefix = parsed_url.path().trim_start_matches('/');
        if prefix.is_empty() {
            None
        } else {
            Some(prefix.to_string())
        }
    };

    Ok(ParsedDbUrl { db_name, db_prefix })
}

fn validate_dns_hostname(hostname: &str) -> Result<()> {
    let ascii_hostname = match url::Host::parse(hostname) {
        Ok(url::Host::Domain(hostname)) => hostname,
        Ok(_) => {
            return Err(Error::InvalidInput {
                message: "LanceDB Cloud database URI or region produced a non-DNS hostname"
                    .to_string(),
            });
        }
        Err(err) => {
            return Err(Error::InvalidInput {
                message: format!(
                    "LanceDB Cloud database URI or region produced an invalid hostname: {err}"
                ),
            });
        }
    };

    if ascii_hostname.len() > 253
        || ascii_hostname
            .split('.')
            .any(|label| label.is_empty() || label.len() > 63)
    {
        return Err(Error::InvalidInput {
            message: "LanceDB Cloud database URI or region produced an invalid hostname: DNS labels must contain 1 to 63 bytes and the full hostname must not exceed 253 bytes".to_string(),
        });
    }

    Ok(())
}

/// Whether a request's body may appear in a debug log.
///
/// The API that built the body decides. The transport cannot know which
/// payloads are credentials, and a list of routes here would have to be kept in
/// step with endpoints defined elsewhere -- so the knowledge lives with the
/// call that has it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BodyLogging {
    /// Log the body at debug. The default: a request body is diagnostic unless
    /// the call that built it says otherwise.
    Allowed,
    /// Never log the body. For a request whose body is a credential.
    Suppressed,
}

/// The delimiter joining a namespace path and a name into the `{id}` a route
/// addresses, and the only one a LanceDB service splits on.
///
/// `$` is outside the character set object names admit, so a joined identifier
/// always splits back into the parts that made it. The configuration field
/// exists because the identifier grammar comes from the Lance REST catalog
/// standard, which carries a delimiter setting for other catalogs to adopt.
pub const ID_DELIMITER: &str = "$";

fn validate_id_delimiter(delimiter: &str) -> Result<()> {
    if delimiter != ID_DELIMITER {
        return Err(Error::InvalidInput {
            message: format!(
                "id_delimiter '{delimiter}' is not supported: '{ID_DELIMITER}' is the only \
                 delimiter LanceDB services split an identifier on"
            ),
        });
    }
    Ok(())
}

impl ClientConfig {
    /// Check the settings a request cannot be built correctly without, so a
    /// mistake is reported where it was made rather than as a confusing
    /// response later. Public so a caller can ask without connecting.
    pub fn validate(&self) -> Result<()> {
        if let Some(delimiter) = &self.id_delimiter {
            validate_id_delimiter(delimiter)?;
        }
        Ok(())
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
        parsed_url: &ParsedDbUrl,
        region: &str,
        host_override: Option<String>,
        default_headers: HeaderMap,
        client_config: ClientConfig,
        read_consistency_interval: Option<Duration>,
    ) -> Result<Self> {
        // Before anything is built from it, so the error names the caller's
        // configuration rather than a request.
        client_config.validate()?;

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
            .default_headers(default_headers)
            .user_agent(client_config.user_agent)
            .build()
            .map_err(|err| Error::Other {
                message: "Failed to build HTTP client".into(),
                source: Some(Box::new(err)),
            })?;

        let host = match host_override {
            Some(host_override) => host_override,
            None => {
                let hostname = format!("{}.{}.api.lancedb.com", parsed_url.db_name, region);
                validate_dns_hostname(&hostname)?;
                format!("https://{hostname}")
            }
        };
        debug!("Created client for host: {}", host);
        let retry_config = client_config.retry_config.clone().try_into()?;
        let max_bytes_per_request =
            Self::resolve_max_bytes_per_request(client_config.max_bytes_per_request)?;
        let max_request_duration =
            Self::resolve_max_request_duration(client_config.max_request_duration, read_timeout)?;
        Ok(Self {
            client,
            host,
            retry_config,
            sender: Sender,
            header_provider: client_config.header_provider,
            read_consistency_interval,
            max_bytes_per_request,
            max_request_duration,
        })
    }

    /// Resolve the max bytes per insert request from config, environment, or the
    /// default. A value of `0` (from either source) disables request splitting.
    fn resolve_max_bytes_per_request(passed: Option<u64>) -> Result<Option<u64>> {
        let value = if let Some(value) = passed {
            value
        } else if let Ok(env) = std::env::var("LANCE_CLIENT_MAX_BYTES_PER_REQUEST") {
            env.parse::<u64>().map_err(|_| Error::InvalidInput {
                message: format!(
                    "LANCE_CLIENT_MAX_BYTES_PER_REQUEST must be a non-negative integer, got '{}'",
                    env
                ),
            })?
        } else {
            DEFAULT_MAX_BYTES_PER_REQUEST
        };
        Ok((value > 0).then_some(value))
    }

    /// Resolve the max request duration from config, environment, or a default
    /// derived from the read timeout. A zero duration (from either source)
    /// disables the time-based cut.
    fn resolve_max_request_duration(
        passed: Option<Duration>,
        read_timeout: Duration,
    ) -> Result<Option<Duration>> {
        let value = if let Some(value) = passed {
            value
        } else if let Ok(env) = std::env::var("LANCE_CLIENT_MAX_REQUEST_DURATION") {
            let secs = env.parse::<u64>().map_err(|_| Error::InvalidInput {
                message: format!(
                    "LANCE_CLIENT_MAX_REQUEST_DURATION must be a non-negative integer \
                     number of seconds, got '{}'",
                    env
                ),
            })?;
            Duration::from_secs(secs)
        } else {
            read_timeout / DEFAULT_MAX_REQUEST_DURATION_DIVISOR
        };
        Ok((!value.is_zero()).then_some(value))
    }
}

impl<S: HttpSend> RestfulLanceDbClient<S> {
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Maximum bytes per insert request, or `None` if request splitting is
    /// disabled.
    pub(crate) fn max_bytes_per_request(&self) -> Option<u64> {
        self.max_bytes_per_request
    }

    /// Maximum wall-clock time per insert request, or `None` if the time-based
    /// cut is disabled.
    pub(crate) fn max_request_duration(&self) -> Option<Duration> {
        self.max_request_duration
    }

    pub fn default_headers(
        api_key: &str,
        region: &str,
        db_name: &str,
        has_host_override: bool,
        options: &RemoteOptions,
        db_prefix: Option<&str>,
        config: &ClientConfig,
    ) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if !api_key.is_empty() {
            // `log_request` prints the request's Debug, which prints headers.
            // Marking the value sensitive is what makes that print `Sensitive`
            // instead of the key itself.
            let mut key = HeaderValue::from_str(api_key).map_err(|_| Error::InvalidInput {
                message: "non-ascii api key provided".to_string(),
            })?;
            key.set_sensitive(true);
            headers.insert(HeaderName::from_static("x-api-key"), key);
        }
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
        if let Some(prefix) = db_prefix {
            headers.insert(
                HeaderName::from_static("x-lancedb-database-prefix"),
                HeaderValue::from_str(prefix).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii database prefix '{}' provided", prefix),
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

        if let Some(user_id) = config.resolve_user_id() {
            headers.insert(
                HeaderName::from_static("x-lancedb-user-id"),
                HeaderValue::from_str(&user_id).map_err(|_| Error::InvalidInput {
                    message: format!("non-ascii user_id '{}' provided", user_id),
                })?,
            );
        }

        redact_sensitive_headers(&mut headers);
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

    /// Apply dynamic headers from the header provider if configured
    pub(crate) async fn apply_dynamic_headers(&self, mut request: Request) -> Result<Request> {
        if let Some(ref provider) = self.header_provider {
            let headers = provider.get_headers().await?;
            let request_headers = request.headers_mut();
            for (key, value) in headers {
                if let Ok(header_name) = HeaderName::from_str(&key) {
                    if let Ok(header_value) = HeaderValue::from_str(&value) {
                        request_headers.insert(header_name, header_value);
                    } else {
                        debug!("Invalid header value for key {}", key);
                    }
                } else {
                    debug!("Invalid header name: {}", key);
                }
            }
        }
        redact_sensitive_headers(request.headers_mut());
        Ok(request)
    }

    pub async fn send(&self, req: RequestBuilder) -> Result<(String, Response)> {
        self.send_logging(req, BodyLogging::Allowed).await
    }

    /// Send a request whose body must never reach a debug log.
    ///
    /// The body is built by the caller, so only the caller knows it holds a
    /// credential; `log_request` sees serialized bytes and cannot tell.
    pub async fn send_suppressing_body(&self, req: RequestBuilder) -> Result<(String, Response)> {
        self.send_logging(req, BodyLogging::Suppressed).await
    }

    async fn send_logging(
        &self,
        req: RequestBuilder,
        body_logging: BodyLogging,
    ) -> Result<(String, Response)> {
        let (client, request) = req.build_split();
        let mut request = request.unwrap();
        let request_id = self.extract_request_id(&mut request);

        // Apply dynamic headers before sending
        request = self.apply_dynamic_headers(request).await?;

        self.log_request(&request, &request_id, body_logging);

        let response = self
            .sender
            .send(&client, request)
            .await
            .err_to_http(request_id.clone())?;
        debug!(
            "Received response for request_id={}: {:?}",
            request_id, response
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
        let mut r = r.map_err(|e| Error::Runtime {
            message: format!("Failed to build request: {}", e),
        })?;
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
            let mut request = request.map_err(|e| Error::Runtime {
                message: format!("Failed to build request: {}", e),
            })?;
            self.set_request_id(&mut request, &request_id.clone());

            // Apply dynamic headers before each retry attempt
            request = self.apply_dynamic_headers(request).await?;

            self.log_request(&request, &request_id, BodyLogging::Allowed);

            let response = self.sender.send(&c, request).await.map(|r| (r.status(), r));

            match response {
                Ok((status, response)) if status.is_success() => {
                    debug!(
                        "Received response for request_id={}: {:?}",
                        retry_counter.request_id, response
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

    fn log_request(&self, request: &Request, request_id: &String, body_logging: BodyLogging) {
        if log::log_enabled!(log::Level::Debug) {
            let content_type = request
                .headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap());
            if body_logging == BodyLogging::Suppressed {
                debug!(
                    "Sending request_id={}: {:?} with body suppressed",
                    request_id, request
                );
            } else if content_type == Some("application/json") {
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
        if let Some(request_id) = request.headers().get(REQUEST_ID_HEADER) {
            request_id.to_str().unwrap().to_string()
        } else {
            let request_id = uuid::Uuid::new_v4().to_string();
            self.set_request_id(request, &request_id);
            request_id
        }
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

    /// Consume a reqwest body into bytes, returning an error if the body
    /// stream fails. This is used by MockSender to materialize streaming
    /// bodies so that data pipeline errors (e.g. NaN rejection) are triggered
    /// during mock sends just as they would be during a real HTTP upload.
    pub async fn try_collect_body(body: reqwest::Body) -> std::result::Result<Vec<u8>, String> {
        use http_body::Body;
        use std::pin::Pin;

        let mut body = body;
        let mut data = Vec::new();
        let mut body_pin = Pin::new(&mut body);
        while let Some(frame) = futures::StreamExt::next(&mut futures::stream::poll_fn(|cx| {
            body_pin.as_mut().poll_frame(cx)
        }))
        .await
        {
            match frame {
                Ok(frame) => {
                    if let Some(bytes) = frame.data_ref() {
                        data.extend_from_slice(bytes);
                    }
                }
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(data)
    }

    impl HttpSend for MockSender {
        async fn send(
            &self,
            _client: &reqwest::Client,
            mut request: reqwest::Request,
        ) -> reqwest::Result<reqwest::Response> {
            // Consume any streaming body to materialize it into bytes.
            // This triggers data pipeline errors (e.g. NaN rejection) that
            // would otherwise only fire when a real HTTP client reads the body.
            if let Some(body) = request.body_mut().take() {
                match try_collect_body(body).await {
                    Ok(bytes) => {
                        *request.body_mut() = Some(reqwest::Body::from(bytes));
                    }
                    Err(msg) => {
                        // Simulate a failed request by returning a 500 response.
                        return Ok(http::Response::builder()
                            .status(500)
                            .body(msg)
                            .unwrap()
                            .into());
                    }
                }
            }
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
        client_with_handler_and_interval(handler, None)
    }

    pub fn client_with_handler_and_interval<T>(
        handler: impl Fn(reqwest::Request) -> http::response::Response<T> + Send + Sync + 'static,
        read_consistency_interval: Option<Duration>,
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
            header_provider: None,
            read_consistency_interval,
            max_bytes_per_request: None,
            max_request_duration: None,
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
            header_provider: config.header_provider,
            read_consistency_interval: None,
            max_bytes_per_request: config
                .max_bytes_per_request
                .and_then(|v| (v > 0).then_some(v)),
            max_request_duration: config
                .max_request_duration
                .and_then(|v| (!v.is_zero()).then_some(v)),
        }
    }
}

#[cfg(test)]
mod tests;
