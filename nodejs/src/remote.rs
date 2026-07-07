// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use lancedb::error::Error;
use napi_derive::*;

/// Timeout configuration for remote HTTP client.
#[napi(object)]
#[derive(Debug)]
pub struct TimeoutConfig {
    /// The overall timeout for the entire request in seconds. This includes
    /// connection, send, and read time. If the entire request doesn't complete
    /// within this time, it will fail. Default is None (no overall timeout).
    /// This can also be set via the environment variable `LANCE_CLIENT_TIMEOUT`,
    /// as an integer number of seconds.
    pub timeout: Option<f64>,
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

/// TLS/mTLS configuration for the remote HTTP client.
#[napi(object)]
#[derive(Debug, Default)]
pub struct TlsConfig {
    /// Path to the client certificate file (PEM format) for mTLS authentication.
    pub cert_file: Option<String>,
    /// Path to the client private key file (PEM format) for mTLS authentication.
    pub key_file: Option<String>,
    /// Path to the CA certificate file (PEM format) for server verification.
    pub ssl_ca_cert: Option<String>,
    /// Whether to verify the hostname in the server's certificate.
    pub assert_hostname: Option<bool>,
}

#[napi(object)]
#[derive(Debug, Default)]
pub struct ClientConfig {
    pub user_agent: Option<String>,
    pub retry_config: Option<RetryConfig>,
    pub timeout_config: Option<TimeoutConfig>,
    pub extra_headers: Option<HashMap<String, String>>,
    pub id_delimiter: Option<String>,
    pub tls_config: Option<TlsConfig>,
    /// User identifier for tracking purposes.
    ///
    /// This is sent as the `x-lancedb-user-id` header in requests to LanceDB Cloud/Enterprise.
    /// It can be set directly, or via the `LANCEDB_USER_ID` environment variable.
    /// Alternatively, set `LANCEDB_USER_ID_ENV_KEY` to specify another environment
    /// variable that contains the user ID value.
    pub user_id: Option<String>,
}

impl From<TimeoutConfig> for lancedb::remote::TimeoutConfig {
    fn from(config: TimeoutConfig) -> Self {
        Self {
            timeout: config.timeout.map(std::time::Duration::from_secs_f64),
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

impl From<TlsConfig> for lancedb::remote::TlsConfig {
    fn from(config: TlsConfig) -> Self {
        Self {
            cert_file: config.cert_file,
            key_file: config.key_file,
            ssl_ca_cert: config.ssl_ca_cert,
            assert_hostname: config.assert_hostname.unwrap_or(true),
        }
    }
}

/// OAuth configuration for LanceDB authentication.
///
/// This is the generated napi-rs binding shape. TypeScript users should prefer
/// the public `OAuthConfig` type exported from `@lancedb/lancedb`.
///
/// All token acquisition and refresh is handled in the Rust layer.
#[napi(object)]
#[derive(Clone)]
pub struct OAuthConfig {
    /// OIDC issuer URL or OAuth authority URL.
    /// For Azure: `https://login.microsoftonline.com/{tenant_id}/v2.0`
    pub issuer_url: String,
    /// Application / Client ID.
    pub client_id: String,
    /// OAuth scopes to request. For Azure managed identity, exactly one scope
    /// or resource is required. For example: `["api://{app_id}/.default"]`
    pub scopes: Vec<String>,
    /// Authentication flow: "client_credentials" or "azure_managed_identity"
    pub flow: Option<String>,
    /// Client secret (required for client_credentials).
    pub client_secret: Option<String>,
    /// Client ID for user-assigned managed identity (azure_managed_identity).
    pub managed_identity_client_id: Option<String>,
    /// Seconds before expiry to trigger proactive refresh (default: 300).
    /// Keep this well below the token TTL; if it is greater than or equal to
    /// the TTL, each request refreshes the token.
    pub refresh_buffer_secs: Option<u32>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("issuer_url", &self.issuer_url)
            .field("client_id", &self.client_id)
            .field("scopes", &self.scopes)
            .field("flow", &self.flow)
            .field(
                "client_secret",
                &self.client_secret.as_deref().map(|_| "<redacted>"),
            )
            .field(
                "managed_identity_client_id",
                &self.managed_identity_client_id,
            )
            .field("refresh_buffer_secs", &self.refresh_buffer_secs)
            .finish()
    }
}

impl TryFrom<OAuthConfig> for lancedb::remote::oauth::OAuthConfig {
    type Error = Error;

    fn try_from(config: OAuthConfig) -> Result<Self, Self::Error> {
        use lancedb::remote::oauth::OAuthFlow;

        let flow = match config.flow.as_deref().unwrap_or("client_credentials") {
            "client_credentials" => OAuthFlow::ClientCredentials,
            "azure_managed_identity" => OAuthFlow::AzureManagedIdentity {
                client_id: config.managed_identity_client_id,
            },
            other => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown OAuth flow type: {other}"),
                });
            }
        };

        Ok(Self {
            issuer_url: config.issuer_url,
            client_id: config.client_id,
            client_secret: config.client_secret,
            scopes: config.scopes,
            flow,
            refresh_buffer_secs: config.refresh_buffer_secs.map(|v| v as u64),
        })
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
            extra_headers: config.extra_headers.unwrap_or_default(),
            id_delimiter: config.id_delimiter,
            tls_config: config.tls_config.map(Into::into),
            header_provider: None, // the header provider is set separately later
            user_id: config.user_id,
            // Resolved from LANCE_CLIENT_MAX_BYTES_PER_REQUEST or the default.
            max_bytes_per_request: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_oauth_flow_returns_invalid_input() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["scope".to_string()],
            flow: Some("typo".to_string()),
            client_secret: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
        };

        let err = lancedb::remote::oauth::OAuthConfig::try_from(config).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "Unknown OAuth flow type: typo"
        ));
    }

    #[test]
    fn test_oauth_config_debug_redacts_client_secret() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["scope".to_string()],
            flow: Some("client_credentials".to_string()),
            client_secret: Some("super-secret".to_string()),
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
        };

        let debug = format!("{config:?}");
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
    }
}
