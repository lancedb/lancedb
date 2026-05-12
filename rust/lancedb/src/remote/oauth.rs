// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::debug;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::remote::client::HeaderProvider;

const DEFAULT_REFRESH_BUFFER_SECS: u64 = 300;
const DEFAULT_TOKEN_TTL_SECS: u64 = 3600;
const AZURE_IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const AZURE_IMDS_API_VERSION: &str = "2018-02-01";

/// OAuth authentication flow configuration.
#[derive(Debug, Clone)]
pub enum OAuthFlow {
    /// Client Credentials grant (service-to-service / M2M).
    /// Requires `client_secret` in [`OAuthConfig`].
    ClientCredentials,

    /// Azure Managed Identity via IMDS.
    /// Works on Azure VMs, AKS, App Service, and Azure Functions.
    AzureManagedIdentity {
        /// Client ID for user-assigned managed identity.
        /// Omit for system-assigned managed identity.
        client_id: Option<String>,
    },
}

/// OAuth configuration for LanceDB authentication.
///
/// All token acquisition and refresh is handled in the Rust layer.
/// Python and TypeScript bindings expose this as a plain config object.
#[derive(Clone)]
pub struct OAuthConfig {
    /// OIDC issuer URL or OAuth authority URL.
    /// For Azure: `https://login.microsoftonline.com/{tenant_id}/v2.0`
    pub issuer_url: String,

    /// Application / Client ID.
    pub client_id: String,

    /// Client secret (required for `ClientCredentials`, optional for others).
    pub client_secret: Option<String>,

    /// OAuth scopes to request.
    /// For Azure managed identity, exactly one scope or resource is required.
    /// For example: `["api://{app_id}/.default"]`
    pub scopes: Vec<String>,

    /// Authentication flow to use.
    pub flow: OAuthFlow,

    /// Seconds before token expiry to trigger proactive refresh (default: 300).
    /// Keep this well below the token TTL; if it is greater than or equal to
    /// the TTL, each request refreshes the token.
    pub refresh_buffer_secs: Option<u64>,
}

impl std::fmt::Debug for OAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthConfig")
            .field("issuer_url", &self.issuer_url)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_deref().map(|_| "<redacted>"),
            )
            .field("scopes", &self.scopes)
            .field("flow", &self.flow)
            .field("refresh_buffer_secs", &self.refresh_buffer_secs)
            .finish()
    }
}

// -- OIDC Discovery --

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    token_endpoint: String,
}

// -- Token Response --

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Token lifetime in seconds.
    /// Some providers (Azure IMDS) return this as a string, so we accept both.
    #[serde(default, deserialize_with = "deserialize_optional_u64_or_string")]
    expires_in: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    token_type: Option<String>,
}

impl std::fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .field("token_type", &self.token_type)
            .finish()
    }
}

fn deserialize_optional_u64_or_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct U64OrString;
    impl<'de> de::Visitor<'de> for U64OrString {
        type Value = Option<u64>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("an integer, an integer-valued float, a numeric string, or null")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
            if v < 0 {
                return Err(E::custom(format!("invalid expires_in value: {v}")));
            }
            Ok(Some(v as u64))
        }

        fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Self::Value, E> {
            if !v.is_finite() || v < 0.0 || v.fract() != 0.0 || v > u64::MAX as f64 {
                return Err(E::custom(format!("invalid expires_in value: {v}")));
            }
            Ok(Some(v as u64))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
            v.parse::<u64>().map(Some).map_err(de::Error::custom)
        }

        fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_any(U64OrString)
}

// -- Internal Token State --

struct TokenState {
    access_token: Option<String>,
    expires_at: Option<Instant>,
}

impl TokenState {
    fn new() -> Self {
        Self {
            access_token: None,
            expires_at: None,
        }
    }

    fn is_expired(&self, buffer: Duration) -> bool {
        match (self.access_token.as_ref(), self.expires_at) {
            (Some(_), Some(expires_at)) => Instant::now() + buffer >= expires_at,
            (None, _) => true,
            (Some(_), None) => true,
        }
    }

    fn update(&mut self, resp: &TokenResponse) {
        self.access_token = Some(resp.access_token.clone());
        let expires_in = resp.expires_in.unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        self.expires_at = Some(Instant::now() + Duration::from_secs(expires_in));
    }
}

/// OAuth header provider that manages the full token lifecycle.
///
/// Implements [`HeaderProvider`] to inject `Authorization: Bearer <token>`
/// headers into every LanceDB request, with automatic token refresh.
pub struct OAuthHeaderProvider {
    config: OAuthConfig,
    http_client: Client,
    token_state: Arc<RwLock<TokenState>>,
    /// Cached OIDC discovery document
    discovery: Arc<RwLock<Option<OidcDiscovery>>>,
    refresh_buffer: Duration,
}

impl std::fmt::Debug for OAuthHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthHeaderProvider")
            .field("issuer_url", &self.config.issuer_url)
            .field("client_id", &self.config.client_id)
            .field("flow", &self.config.flow)
            .finish()
    }
}

impl OAuthHeaderProvider {
    /// Create a new OAuth header provider from configuration.
    pub fn new(config: OAuthConfig) -> Result<Self> {
        // Validate config upfront
        if matches!(config.flow, OAuthFlow::ClientCredentials) && config.client_secret.is_none() {
            return Err(Error::InvalidInput {
                message: "client_secret is required for ClientCredentials flow".to_string(),
            });
        }
        if config.scopes.is_empty() {
            return Err(Error::InvalidInput {
                message: "At least one OAuth scope is required".to_string(),
            });
        }
        if matches!(config.flow, OAuthFlow::AzureManagedIdentity { .. }) && config.scopes.len() != 1
        {
            return Err(Error::InvalidInput {
                message: "AzureManagedIdentity flow requires exactly one OAuth scope or resource"
                    .to_string(),
            });
        }
        Self::validate_issuer_transport(&config)?;

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create HTTP client for OAuth: {e}"),
            })?;

        let refresh_buffer = Duration::from_secs(
            config
                .refresh_buffer_secs
                .unwrap_or(DEFAULT_REFRESH_BUFFER_SECS),
        );

        Ok(Self {
            config,
            http_client,
            token_state: Arc::new(RwLock::new(TokenState::new())),
            discovery: Arc::new(RwLock::new(None)),
            refresh_buffer,
        })
    }

    fn validate_issuer_transport(config: &OAuthConfig) -> Result<()> {
        if !matches!(config.flow, OAuthFlow::ClientCredentials) {
            return Ok(());
        }

        let issuer = url::Url::parse(&config.issuer_url).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth issuer_url: {e}"),
        })?;

        match issuer.scheme() {
            "https" => Ok(()),
            "http" if Self::is_loopback_issuer(&issuer) => Ok(()),
            _ => Err(Error::InvalidInput {
                message:
                    "ClientCredentials OAuth issuer_url must use https, except for loopback hosts"
                        .to_string(),
            }),
        }
    }

    fn is_loopback_issuer(issuer: &url::Url) -> bool {
        let Some(host) = issuer.host_str() else {
            return false;
        };

        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .map(|addr| addr.is_loopback())
                .unwrap_or(false)
    }

    /// Get a valid access token, refreshing if necessary.
    async fn get_valid_token(&self) -> Result<String> {
        // Fast path: check if current token is still valid
        {
            let state = self.token_state.read().await;
            if !state.is_expired(self.refresh_buffer)
                && let Some(ref token) = state.access_token
            {
                return Ok(token.clone());
            }
        }

        // Slow path: acquire or refresh token
        let mut state = self.token_state.write().await;

        // Double-check after acquiring write lock
        if !state.is_expired(self.refresh_buffer)
            && let Some(ref token) = state.access_token
        {
            return Ok(token.clone());
        }

        debug!("Acquiring new OAuth token via {:?} flow", self.config.flow);
        let resp = self.acquire_token().await?;

        state.update(&resp);
        Ok(resp.access_token)
    }

    /// Acquire a new token using the configured flow.
    async fn acquire_token(&self) -> Result<TokenResponse> {
        match &self.config.flow {
            OAuthFlow::ClientCredentials => self.acquire_client_credentials().await,
            OAuthFlow::AzureManagedIdentity { client_id } => {
                self.acquire_managed_identity(client_id.as_deref()).await
            }
        }
    }

    // -- OIDC Discovery --

    async fn get_discovery(&self) -> Result<OidcDiscovery> {
        {
            let cached = self.discovery.read().await;
            if let Some(ref disc) = *cached {
                return Ok(OidcDiscovery {
                    token_endpoint: disc.token_endpoint.clone(),
                });
            }
        }

        let mut cache = self.discovery.write().await;
        // Double-check
        if let Some(ref disc) = *cache {
            return Ok(OidcDiscovery {
                token_endpoint: disc.token_endpoint.clone(),
            });
        }

        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer_url.trim_end_matches('/')
        );

        debug!("Fetching OIDC discovery from {}", discovery_url);

        let resp = self
            .http_client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to fetch OIDC discovery document: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "OIDC discovery failed with status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ),
            });
        }

        let disc: OidcDiscovery = resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse OIDC discovery document: {e}"),
        })?;

        let result = OidcDiscovery {
            token_endpoint: disc.token_endpoint.clone(),
        };

        *cache = Some(disc);
        Ok(result)
    }

    async fn get_token_endpoint(&self) -> Result<String> {
        self.get_discovery().await.map(|disc| disc.token_endpoint)
    }

    fn scopes_string(&self) -> String {
        self.config.scopes.join(" ")
    }

    fn managed_identity_resource(&self) -> Result<String> {
        let [scope] = self.config.scopes.as_slice() else {
            return Err(Error::InvalidInput {
                message: "AzureManagedIdentity flow requires exactly one OAuth scope or resource"
                    .to_string(),
            });
        };

        Ok(scope.strip_suffix("/.default").unwrap_or(scope).to_string())
    }

    // -- Client Credentials Flow --

    async fn acquire_client_credentials(&self) -> Result<TokenResponse> {
        let client_secret = self
            .config
            .client_secret
            .as_ref()
            .ok_or(Error::InvalidInput {
                message: "client_secret is required for ClientCredentials flow".to_string(),
            })?;

        let token_endpoint = self.get_token_endpoint().await?;

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.config.client_id),
            ("client_secret", client_secret),
            ("scope", &self.scopes_string()),
        ];

        self.post_token_request(&token_endpoint, &params).await
    }

    // -- Azure Managed Identity Flow --

    async fn acquire_managed_identity(&self, mi_client_id: Option<&str>) -> Result<TokenResponse> {
        let resource = self.managed_identity_resource()?;

        let mut url = format!(
            "{AZURE_IMDS_ENDPOINT}?api-version={AZURE_IMDS_API_VERSION}&resource={}",
            urlencoding::encode(&resource),
        );
        if let Some(cid) = mi_client_id {
            url.push_str(&format!("&client_id={}", urlencoding::encode(cid)));
        }

        let resp = self
            .http_client
            .get(&url)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Azure IMDS request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "Azure IMDS returned status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ),
            });
        }

        resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse IMDS token response: {e}"),
        })
    }

    // -- Shared Helpers --

    async fn post_token_request(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<TokenResponse> {
        let resp = self
            .http_client
            .post(endpoint)
            .form(params)
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Token request to {endpoint} failed: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "Token request failed with status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ),
            });
        }

        resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse token response: {e}"),
        })
    }
}

#[async_trait::async_trait]
impl HeaderProvider for OAuthHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        let token = self.get_valid_token().await?;
        Ok(HashMap::from([(
            "authorization".to_string(),
            format!("Bearer {token}"),
        )]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinHandle;

    #[test]
    fn test_token_state_expiry() {
        let mut state = TokenState::new();
        assert!(state.is_expired(Duration::from_secs(0)));

        state.access_token = Some("tok".to_string());
        state.expires_at = Some(Instant::now() + Duration::from_secs(600));
        assert!(!state.is_expired(Duration::from_secs(300)));
        assert!(state.is_expired(Duration::from_secs(601)));

        state.expires_at = None;
        assert!(state.is_expired(Duration::from_secs(0)));
    }

    #[test]
    fn test_token_state_uses_default_expiry() {
        let mut state = TokenState::new();
        let response = TokenResponse {
            access_token: "tok".to_string(),
            expires_in: None,
            token_type: None,
        };

        state.update(&response);

        assert!(!state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS - 1)));
        assert!(state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS + 1)));
    }

    #[test]
    fn test_token_response_accepts_float_expires_in() {
        let response: TokenResponse =
            serde_json::from_str(r#"{"access_token":"tok","expires_in":3600.0}"#).unwrap();

        assert_eq!(response.expires_in, Some(3600));
    }

    #[test]
    fn test_token_response_rejects_negative_expires_in() {
        let err =
            serde_json::from_str::<TokenResponse>(r#"{"access_token":"tok","expires_in":-1}"#)
                .unwrap_err();

        assert!(err.to_string().contains("invalid expires_in value: -1"));
    }

    #[test]
    fn test_token_response_debug_redacts_access_token() {
        let response = TokenResponse {
            access_token: "secret-token".to_string(),
            expires_in: Some(3600),
            token_type: Some("Bearer".to_string()),
        };

        let debug = format!("{response:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("access_token: \"<redacted>\""));
    }

    #[test]
    fn test_scopes_string() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["scope1".to_string(), "scope2".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();
        assert_eq!(provider.scopes_string(), "scope1 scope2");
    }

    #[test]
    fn test_oauth_config_debug_redacts_client_secret() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            client_secret: Some("super-secret".to_string()),
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };

        let debug = format!("{config:?}");
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
    }

    #[test]
    fn test_managed_identity_resource_from_default_scope() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec!["api://test/.default".to_string()],
            flow: OAuthFlow::AzureManagedIdentity { client_id: None },
            refresh_buffer_secs: None,
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();
        assert_eq!(provider.managed_identity_resource().unwrap(), "api://test");
    }

    #[test]
    fn test_managed_identity_resource_without_default_suffix() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec!["api://test".to_string()],
            flow: OAuthFlow::AzureManagedIdentity { client_id: None },
            refresh_buffer_secs: None,
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();
        assert_eq!(provider.managed_identity_resource().unwrap(), "api://test");
    }

    #[test]
    fn test_managed_identity_rejects_multiple_scopes() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec![
                "api://test-a/.default".to_string(),
                "api://test-b/.default".to_string(),
            ],
            flow: OAuthFlow::AzureManagedIdentity { client_id: None },
            refresh_buffer_secs: None,
        };
        assert!(OAuthHeaderProvider::new(config).is_err());
    }

    #[tokio::test]
    async fn test_token_endpoint_requires_discovery_success() {
        let (issuer_url, server) = spawn_discovery_error_server().await;
        let config = OAuthConfig {
            issuer_url,
            client_id: "client-id".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();

        let err = provider.get_token_endpoint().await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message.contains("OIDC discovery failed with status 503")
        ));
        server.await.unwrap();
    }

    #[test]
    fn test_client_credentials_requires_secret() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };
        assert!(OAuthHeaderProvider::new(config).is_err());
    }

    #[test]
    fn test_client_credentials_rejects_insecure_non_loopback_issuer() {
        let config = OAuthConfig {
            issuer_url: "http://issuer.example.com".to_string(),
            client_id: "app-id".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };

        let err = OAuthHeaderProvider::new(config).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "ClientCredentials OAuth issuer_url must use https, except for loopback hosts"
        ));
    }

    #[test]
    fn test_empty_scopes_rejected() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec![],
            flow: OAuthFlow::AzureManagedIdentity { client_id: None },
            refresh_buffer_secs: None,
        };
        assert!(OAuthHeaderProvider::new(config).is_err());
    }

    #[tokio::test]
    async fn test_client_credentials_token_lifecycle() {
        let (issuer_url, token_requests, server) = spawn_oauth_server().await;
        let config = OAuthConfig {
            issuer_url,
            client_id: "client-id".to_string(),
            client_secret: Some("secret".to_string()),
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: Some(0),
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();

        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.get("authorization").unwrap(), "Bearer token-1");
        assert_eq!(token_requests.load(Ordering::SeqCst), 1);

        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.get("authorization").unwrap(), "Bearer token-1");
        assert_eq!(token_requests.load(Ordering::SeqCst), 1);

        provider.token_state.write().await.expires_at =
            Some(Instant::now() - Duration::from_secs(1));

        let headers = provider.get_headers().await.unwrap();
        assert_eq!(headers.get("authorization").unwrap(), "Bearer token-2");
        assert_eq!(token_requests.load(Ordering::SeqCst), 2);

        server.await.unwrap();
    }

    async fn spawn_oauth_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");
        let token_requests = Arc::new(AtomicUsize::new(0));
        let server_token_requests = Arc::clone(&token_requests);

        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, body) = read_http_request(&mut stream).await;

                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                } else if request_line.starts_with("POST /token ") {
                    assert!(body.contains("grant_type=client_credentials"));
                    assert!(body.contains("client_id=client-id"));
                    assert!(body.contains("client_secret=secret"));
                    assert!(body.contains("scope=scope"));

                    let token_num = server_token_requests.fetch_add(1, Ordering::SeqCst) + 1;
                    let token = format!(
                        r#"{{"access_token":"token-{token_num}","expires_in":3600,"token_type":"Bearer"}}"#
                    );
                    write_json_response(&mut stream, "200 OK", &token).await;
                } else {
                    write_json_response(&mut stream, "404 Not Found", "{}").await;
                }
            }
        });

        (issuer_url, token_requests, server)
    }

    async fn spawn_discovery_error_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (request_line, _) = read_http_request(&mut stream).await;
            assert!(request_line.starts_with("GET /.well-known/openid-configuration "));
            write_json_response(&mut stream, "503 Service Unavailable", "{}").await;
        });

        (issuer_url, server)
    }

    async fn read_http_request(stream: &mut TcpStream) -> (String, String) {
        let mut buffer = Vec::new();
        let mut header_end = None;

        while header_end.is_none() {
            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "connection closed before request headers");
            buffer.extend_from_slice(&chunk[..read]);
            header_end = find_subsequence(&buffer, b"\r\n\r\n").map(|pos| pos + 4);
        }

        let header_end = header_end.unwrap();
        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
        let request_line = headers.lines().next().unwrap_or_default().to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);

        while buffer.len() < header_end + content_length {
            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "connection closed before request body");
            buffer.extend_from_slice(&chunk[..read]);
        }

        let body =
            String::from_utf8_lossy(&buffer[header_end..header_end + content_length]).to_string();

        (request_line, body)
    }

    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    async fn write_json_response(stream: &mut TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    }
}
