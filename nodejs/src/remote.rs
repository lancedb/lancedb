// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use lancedb::error::Error;
use napi_derive::*;

use crate::error::NapiErrorExt;

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
    /// [409, 429, 500, 502, 503, 504].
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
    /// The delimiter joining a namespace path and a name into one object
    /// identifier. `"$"` is the only supported value, and leaving this unset is
    /// how to get it; anything else is rejected when the connection is created.
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

/// Options for the persistent OAuth token cache.
///
/// The cache is opt-in: it is only used when set as `tokenCache` on
/// `OAuthConfig`. Only refresh tokens are persisted, in a private directory
/// with owner-only permissions, so short-lived processes can reuse an
/// authenticated session instead of re-prompting on every start.
#[napi(object)]
#[derive(Clone, Debug, Default)]
pub struct TokenCacheOptions {
    /// Directory that holds cached credentials. Defaults to
    /// `$XDG_CACHE_HOME/lancedb/oauth`, `$HOME/.cache/lancedb/oauth` on Unix,
    /// or `%LOCALAPPDATA%\lancedb\oauth` on Windows. The directory is created
    /// with owner-only permissions (`0700`) when missing.
    pub cache_dir: Option<String>,
    /// How long to wait for the cross-process refresh lock before failing,
    /// in seconds (default: 30).
    pub lock_timeout_secs: Option<u32>,
}

impl From<TokenCacheOptions> for lancedb::remote::TokenCacheOptions {
    fn from(options: TokenCacheOptions) -> Self {
        Self {
            cache_dir: options.cache_dir.map(std::path::PathBuf::from),
            lock_timeout_secs: options.lock_timeout_secs.map(|secs| secs as u64),
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
    /// Optional resource indicator for authorization and token requests.
    pub resource: Option<String>,
    /// Optional provider-specific audience for authorization and token requests.
    pub audience: Option<String>,
    /// Authentication flow: "client_credentials", "authorization_code",
    /// "device_code", or "azure_managed_identity"
    pub flow: Option<String>,
    /// Client secret (required for client_credentials).
    pub client_secret: Option<String>,
    /// How the client authenticates to the token endpoint: "none",
    /// "client_secret_basic", or "client_secret_post". Defaults to
    /// "client_secret_basic" when a client secret is set, and "none" for
    /// public clients.
    pub client_auth_method: Option<String>,
    /// Loopback redirect URI for authorization_code.
    pub redirect_uri: Option<String>,
    /// Port for the authorization_code loopback callback server.
    pub callback_port: Option<u16>,
    /// Whether authorization_code uses S256 PKCE (default: true).
    pub use_pkce: Option<bool>,
    /// Client ID for user-assigned managed identity (azure_managed_identity).
    pub managed_identity_client_id: Option<String>,
    /// Seconds before expiry to trigger proactive refresh (default: 300).
    /// Keep this well below the token TTL; if it is greater than or equal to
    /// the TTL, each request refreshes the token.
    pub refresh_buffer_secs: Option<u32>,
    /// Opt in to the persistent token cache so short-lived processes reuse
    /// one session. Only refresh tokens are persisted.
    pub token_cache: Option<TokenCacheOptions>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("issuer_url", &self.issuer_url)
            .field("client_id", &self.client_id)
            .field("scopes", &self.scopes)
            .field("resource", &self.resource)
            .field("audience", &self.audience)
            .field("flow", &self.flow)
            .field(
                "client_secret",
                &self.client_secret.as_deref().map(|_| "<redacted>"),
            )
            .field("client_auth_method", &self.client_auth_method)
            .field("redirect_uri", &self.redirect_uri)
            .field("callback_port", &self.callback_port)
            .field("use_pkce", &self.use_pkce)
            .field(
                "managed_identity_client_id",
                &self.managed_identity_client_id,
            )
            .field("refresh_buffer_secs", &self.refresh_buffer_secs)
            .field("token_cache", &self.token_cache)
            .finish()
    }
}

impl TryFrom<OAuthConfig> for lancedb::remote::oauth::OAuthConfig {
    type Error = Error;

    fn try_from(config: OAuthConfig) -> Result<Self, Self::Error> {
        use lancedb::remote::oauth::{AuthorizationCodeOptions, OAuthFlow};

        let flow = match config.flow.as_deref().unwrap_or("client_credentials") {
            "client_credentials" => OAuthFlow::ClientCredentials,
            "authorization_code" => {
                let mut options =
                    AuthorizationCodeOptions::new().use_pkce(config.use_pkce.unwrap_or(true));
                if let Some(redirect_uri) = config.redirect_uri {
                    options = options.redirect_uri(redirect_uri);
                }
                if let Some(callback_port) = config.callback_port {
                    options = options.callback_port(callback_port);
                }
                OAuthFlow::AuthorizationCode(options)
            }
            "device_code" => OAuthFlow::DeviceCode,
            "azure_managed_identity" => OAuthFlow::AzureManagedIdentity {
                client_id: config.managed_identity_client_id,
            },
            other => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown OAuth flow type: {other}"),
                });
            }
        };

        let client_auth_method = match config.client_auth_method.as_deref() {
            Some("none") => Some(lancedb::remote::oauth::ClientAuthMethod::None),
            Some("client_secret_basic") => {
                Some(lancedb::remote::oauth::ClientAuthMethod::ClientSecretBasic)
            }
            Some("client_secret_post") => {
                Some(lancedb::remote::oauth::ClientAuthMethod::ClientSecretPost)
            }
            None => None,
            Some(other) => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown OAuth client auth method: {other}"),
                });
            }
        };

        Ok(Self {
            issuer_url: config.issuer_url,
            client_id: config.client_id,
            client_secret: config.client_secret,
            client_auth_method,
            scopes: config.scopes,
            resource: config.resource,
            audience: config.audience,
            flow,
            refresh_buffer_secs: config.refresh_buffer_secs.map(|v| v as u64),
            token_cache: config.token_cache.map(Into::into),
        })
    }
}

/// Safe, non-secret view of a cached OAuth session, returned by
/// `OAuthSession.status()` and `OAuthSession.login()`.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct SessionStatus {
    /// Whether a cached session exists that can obtain tokens without
    /// interactive authentication.
    pub refreshable: bool,
    /// Canonical issuer URL of the cached session.
    pub issuer_url: String,
    /// Client ID of the cached session.
    pub client_id: String,
    /// Canonical (sorted, de-duplicated) scopes of the cached session.
    pub scopes: Vec<String>,
    /// Optional resource indicator for authorization and token requests.
    pub resource: Option<String>,
    /// Optional provider-specific audience for authorization and token requests.
    pub audience: Option<String>,
    /// Flow that produced the cached session.
    pub flow: String,
    /// When the cached session was obtained, as Unix seconds.
    pub obtained_at: Option<f64>,
}

/// Result of `OAuthSession.logout()`.
#[napi(object)]
#[derive(Clone, Debug)]
pub struct SessionLogout {
    /// Whether a cached credential was removed. `false` means no matching
    /// session was cached; logout is idempotent.
    pub removed: bool,
}

/// Explicit OAuth session lifecycle for the persistent token cache: eager
/// `login`, non-secret `status`, and local `logout`.
///
/// A session is built from the same `OAuthConfig` used to connect (including
/// its `tokenCache` options). A connection created with the same
/// configuration shares the cache, so logging in here prepares tokens for
/// later processes without any database request.
#[napi]
pub struct OAuthSession {
    inner: lancedb::remote::OAuthSession,
}

#[napi]
impl OAuthSession {
    /// Create a session manager for the given OAuth configuration.
    ///
    /// The configuration must enable `tokenCache` options and use a flow that
    /// supports persistent sessions (authorization code or device code).
    #[napi(constructor)]
    pub fn new(config: OAuthConfig) -> napi::Result<Self> {
        let config: lancedb::remote::oauth::OAuthConfig = config.try_into().default_error()?;
        let inner = lancedb::remote::OAuthSession::new(config).default_error()?;
        Ok(Self { inner })
    }

    /// Eagerly run the configured authentication flow and store the session.
    ///
    /// A successful login always replaces any prior cached session for this
    /// identity; if the provider does not issue a refresh token (for example
    /// without `offline_access`), the previous record is removed and the
    /// status reports `refreshable == false`.
    #[napi(catch_unwind)]
    pub async fn login(&self) -> napi::Result<SessionStatus> {
        let status = self.inner.login().await.default_error()?;
        Ok(SessionStatus::from(status))
    }

    /// Report whether a matching cached session exists, with safe metadata.
    ///
    /// This never contacts the identity provider and never exposes token
    /// values.
    #[napi(catch_unwind)]
    pub async fn status(&self) -> napi::Result<SessionStatus> {
        let status = self.inner.status().await.default_error()?;
        Ok(SessionStatus::from(status))
    }

    /// Remove the matching local cached credential.
    ///
    /// This only deletes the local cache entry. It does not revoke the
    /// refresh token with the provider and does not sign out of a browser
    /// SSO session. Repeated calls succeed; `removed` reports whether a
    /// credential existed.
    #[napi(catch_unwind)]
    pub async fn logout(&self) -> napi::Result<SessionLogout> {
        let logout = self.inner.logout().await.default_error()?;
        Ok(SessionLogout {
            removed: logout.removed,
        })
    }
}

impl From<lancedb::remote::SessionStatus> for SessionStatus {
    fn from(status: lancedb::remote::SessionStatus) -> Self {
        Self {
            refreshable: status.refreshable,
            issuer_url: status.issuer_url,
            client_id: status.client_id,
            scopes: status.scopes,
            resource: status.resource,
            audience: status.audience,
            flow: status.flow,
            obtained_at: status.obtained_at.map(|secs| secs as f64),
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
            extra_headers: config.extra_headers.unwrap_or_default(),
            id_delimiter: config.id_delimiter,
            tls_config: config.tls_config.map(Into::into),
            header_provider: None, // the header provider is set separately later
            user_id: config.user_id,
            // Resolved from LANCE_CLIENT_MAX_BYTES_PER_REQUEST or the default.
            max_bytes_per_request: None,
            // Resolved from LANCE_CLIENT_MAX_REQUEST_DURATION or the read timeout.
            max_request_duration: None,
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
            client_auth_method: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            resource: None,
            audience: None,
            token_cache: None,
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
            client_auth_method: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            resource: None,
            audience: None,
            token_cache: None,
        };

        let debug = format!("{config:?}");
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
    }

    #[test]
    fn test_authorization_code_conversion_preserves_options() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["openid".to_string()],
            flow: Some("authorization_code".to_string()),
            client_secret: Some("secret".to_string()),
            client_auth_method: None,
            redirect_uri: Some("http://127.0.0.1:9000/callback".to_string()),
            callback_port: Some(9000),
            use_pkce: Some(false),
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            resource: Some("urn:resource".into()),
            audience: Some("audience".into()),
            token_cache: None,
        };

        let converted = lancedb::remote::oauth::OAuthConfig::try_from(config).unwrap();
        let lancedb::remote::oauth::OAuthFlow::AuthorizationCode(options) = converted.flow else {
            panic!("expected authorization code flow");
        };
        assert_eq!(
            options.redirect_uri.as_deref(),
            Some("http://127.0.0.1:9000/callback")
        );
        assert_eq!(options.callback_port, Some(9000));
        assert!(!options.use_pkce);
        assert_eq!(converted.resource.as_deref(), Some("urn:resource"));
        assert_eq!(converted.audience.as_deref(), Some("audience"));
    }

    #[test]
    fn test_device_code_conversion() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["openid".to_string()],
            flow: Some("device_code".to_string()),
            client_secret: None,
            client_auth_method: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            resource: None,
            audience: None,
            token_cache: None,
        };

        let converted = lancedb::remote::oauth::OAuthConfig::try_from(config).unwrap();
        assert!(matches!(
            converted.flow,
            lancedb::remote::oauth::OAuthFlow::DeviceCode
        ));
    }

    #[test]
    fn test_client_auth_method_conversion() {
        use lancedb::remote::oauth::ClientAuthMethod;

        for (value, expected) in [
            ("none", ClientAuthMethod::None),
            ("client_secret_basic", ClientAuthMethod::ClientSecretBasic),
            ("client_secret_post", ClientAuthMethod::ClientSecretPost),
        ] {
            let config = OAuthConfig {
                issuer_url: "https://issuer.example.com".to_string(),
                client_id: "client-id".to_string(),
                scopes: vec!["openid".to_string()],
                flow: Some("device_code".to_string()),
                client_secret: None,
                client_auth_method: Some(value.to_string()),
                resource: None,
                audience: None,
                redirect_uri: None,
                callback_port: None,
                use_pkce: None,
                managed_identity_client_id: None,
                refresh_buffer_secs: None,
                token_cache: None,
            };

            let converted = lancedb::remote::oauth::OAuthConfig::try_from(config).unwrap();
            assert_eq!(converted.client_auth_method, Some(expected));
        }
    }

    #[test]
    fn test_unknown_client_auth_method_returns_invalid_input() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            scopes: vec!["openid".to_string()],
            flow: Some("device_code".to_string()),
            client_secret: None,
            client_auth_method: Some("typo".to_string()),
            resource: None,
            audience: None,
            redirect_uri: None,
            callback_port: None,
            use_pkce: None,
            managed_identity_client_id: None,
            refresh_buffer_secs: None,
            token_cache: None,
        };

        let err = lancedb::remote::oauth::OAuthConfig::try_from(config).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "Unknown OAuth client auth method: typo"
        ));
    }
}
