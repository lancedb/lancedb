// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::remote::client::HeaderProvider;

const DEFAULT_REFRESH_BUFFER_SECS: u64 = 300;
const DEFAULT_CALLBACK_PORT: u16 = 8400;
const AZURE_IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const AZURE_IMDS_API_VERSION: &str = "2018-02-01";

/// OAuth authentication flow configuration.
#[derive(Debug, Clone)]
pub enum OAuthFlow {
    /// Client Credentials grant (service-to-service / M2M).
    /// Requires `client_secret` in [`OAuthConfig`].
    ClientCredentials,

    /// Authorization Code with PKCE (interactive browser-based auth).
    AuthorizationCodePKCE {
        /// Redirect URI (default: `http://localhost:{callback_port}/callback`)
        redirect_uri: Option<String>,
        /// Port for the local HTTP callback server (default: 8400)
        callback_port: Option<u16>,
    },

    /// Device Code grant (CLI / headless environments).
    /// Displays a verification URL and user code for out-of-band authentication.
    DeviceCode,

    /// Azure Managed Identity via IMDS.
    /// Works on Azure VMs, AKS, App Service, and Azure Functions.
    AzureManagedIdentity {
        /// Client ID for user-assigned managed identity.
        /// Omit for system-assigned managed identity.
        client_id: Option<String>,
    },

    /// Workload Identity Federation.
    /// Exchanges a platform token (K8s service account, GitHub OIDC) for an IdP token.
    WorkloadIdentity {
        /// Path to the federated token file
        /// (e.g. `AZURE_FEDERATED_TOKEN_FILE`).
        token_file: String,
    },
}

/// OAuth configuration for LanceDB authentication.
///
/// All token acquisition and refresh is handled in the Rust layer.
/// Python and TypeScript bindings expose this as a plain config object.
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    /// OIDC issuer URL or OAuth authority URL.
    /// For Azure: `https://login.microsoftonline.com/{tenant_id}/v2.0`
    pub issuer_url: String,

    /// Application / Client ID.
    pub client_id: String,

    /// Client secret (required for `ClientCredentials`, optional for others).
    pub client_secret: Option<String>,

    /// OAuth scopes to request.
    /// For Azure: `["api://{app_id}/.default"]`
    pub scopes: Vec<String>,

    /// Authentication flow to use.
    pub flow: OAuthFlow,

    /// Seconds before token expiry to trigger proactive refresh (default: 300).
    pub refresh_buffer_secs: Option<u64>,
}

// -- OIDC Discovery --

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    token_endpoint: String,
    authorization_endpoint: Option<String>,
    device_authorization_endpoint: Option<String>,
}

// -- Token Response --

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    /// Token lifetime in seconds.
    /// Some providers (Azure IMDS) return this as a string, so we accept both.
    #[serde(default, deserialize_with = "deserialize_optional_u64_or_string")]
    expires_in: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    token_type: Option<String>,
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
            formatter.write_str("a u64, a numeric string, or null")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Self::Value, E> {
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

// -- Device Code Response --

#[derive(Debug, Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: u64,
    interval: Option<u64>,
}

// -- Internal Token State --

struct TokenState {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_at: Option<Instant>,
}

impl TokenState {
    fn new() -> Self {
        Self {
            access_token: None,
            refresh_token: None,
            expires_at: None,
        }
    }

    fn is_expired(&self, buffer: Duration) -> bool {
        match (self.access_token.as_ref(), self.expires_at) {
            (Some(_), Some(expires_at)) => Instant::now() + buffer >= expires_at,
            (None, _) => true,
            (Some(_), None) => false, // no expiry info, assume valid
        }
    }

    fn update(&mut self, resp: &TokenResponse) {
        self.access_token = Some(resp.access_token.clone());
        if resp.refresh_token.is_some() {
            self.refresh_token = resp.refresh_token.clone();
        }
        self.expires_at = resp
            .expires_in
            .map(|secs| Instant::now() + Duration::from_secs(secs));
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

        let uses_refresh_token = !matches!(
            self.config.flow,
            OAuthFlow::ClientCredentials
                | OAuthFlow::AzureManagedIdentity { .. }
                | OAuthFlow::WorkloadIdentity { .. }
        );

        let resp = if let Some(ref refresh_token) = state.refresh_token
            && uses_refresh_token
        {
            debug!("Refreshing OAuth token using refresh_token");
            self.refresh_with_token(refresh_token).await?
        } else {
            debug!("Acquiring new OAuth token via {:?} flow", self.config.flow);
            self.acquire_token().await?
        };

        state.update(&resp);
        Ok(resp.access_token)
    }

    /// Acquire a new token using the configured flow.
    async fn acquire_token(&self) -> Result<TokenResponse> {
        match &self.config.flow {
            OAuthFlow::ClientCredentials => self.acquire_client_credentials().await,
            OAuthFlow::AuthorizationCodePKCE {
                redirect_uri,
                callback_port,
            } => {
                self.acquire_authorization_code_pkce(
                    redirect_uri.as_deref(),
                    callback_port.unwrap_or(DEFAULT_CALLBACK_PORT),
                )
                .await
            }
            OAuthFlow::DeviceCode => self.acquire_device_code().await,
            OAuthFlow::AzureManagedIdentity { client_id } => {
                self.acquire_managed_identity(client_id.as_deref()).await
            }
            OAuthFlow::WorkloadIdentity { token_file } => {
                self.acquire_workload_identity(token_file).await
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
                    authorization_endpoint: disc.authorization_endpoint.clone(),
                    device_authorization_endpoint: disc.device_authorization_endpoint.clone(),
                });
            }
        }

        let mut cache = self.discovery.write().await;
        // Double-check
        if let Some(ref disc) = *cache {
            return Ok(OidcDiscovery {
                token_endpoint: disc.token_endpoint.clone(),
                authorization_endpoint: disc.authorization_endpoint.clone(),
                device_authorization_endpoint: disc.device_authorization_endpoint.clone(),
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
            authorization_endpoint: disc.authorization_endpoint.clone(),
            device_authorization_endpoint: disc.device_authorization_endpoint.clone(),
        };

        *cache = Some(disc);
        Ok(result)
    }

    fn get_token_endpoint_from_issuer(&self) -> String {
        // Derive Azure v2.0 token endpoint from issuer URL
        // issuer: https://login.microsoftonline.com/{tenant}/v2.0
        // token:  https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token
        let base = self.config.issuer_url.trim_end_matches("/v2.0");
        format!("{base}/oauth2/v2.0/token")
    }

    async fn get_token_endpoint(&self) -> Result<String> {
        match self.get_discovery().await {
            Ok(disc) => Ok(disc.token_endpoint),
            Err(e) => {
                warn!("OIDC discovery failed, falling back to derived endpoint: {e}");
                Ok(self.get_token_endpoint_from_issuer())
            }
        }
    }

    fn scopes_string(&self) -> String {
        self.config.scopes.join(" ")
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

    // -- Authorization Code + PKCE Flow --

    async fn acquire_authorization_code_pkce(
        &self,
        redirect_uri: Option<&str>,
        callback_port: u16,
    ) -> Result<TokenResponse> {
        use rand::Rng;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let discovery = self.get_discovery().await?;
        let auth_endpoint = discovery.authorization_endpoint.ok_or(Error::Runtime {
            message: "OIDC discovery did not provide authorization_endpoint".to_string(),
        })?;

        let default_redirect = format!("http://localhost:{callback_port}/callback");
        let redirect = redirect_uri.unwrap_or(&default_redirect);

        // Generate PKCE code verifier and challenge (S256)
        const PKCE_CHARSET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
        let code_verifier: String = {
            let mut rng = rand::rng();
            (0..128)
                .map(|_| {
                    let idx = rng.random_range(0..PKCE_CHARSET.len());
                    PKCE_CHARSET[idx] as char
                })
                .collect()
        };
        let code_challenge = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(code_verifier.as_bytes());
            base64_url_encode(&hash)
        };

        let state: String = {
            let mut rng = rand::rng();
            (0..32)
                .map(|_| {
                    let idx = rng.random_range(0..16u8);
                    b"0123456789abcdef"[idx as usize] as char
                })
                .collect()
        };

        // Build authorization URL
        let auth_url = format!(
            "{auth_endpoint}?response_type=code&client_id={}&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&state={state}",
            urlencoding::encode(&self.config.client_id),
            urlencoding::encode(redirect),
            urlencoding::encode(&self.scopes_string()),
            urlencoding::encode(&code_challenge),
        );

        info!("Opening browser for OAuth login...");
        info!("If the browser doesn't open, visit: {auth_url}");

        // Try to open browser
        let _ = open::that(&auth_url);

        // Start local callback server
        let listener = TcpListener::bind(format!("127.0.0.1:{callback_port}"))
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to bind callback server on port {callback_port}: {e}"),
            })?;

        info!("Waiting for OAuth callback on port {callback_port}...");

        let (mut stream, _) = listener.accept().await.map_err(|e| Error::Runtime {
            message: format!("Failed to accept callback connection: {e}"),
        })?;

        // Read the HTTP request
        let mut buf = vec![0u8; 4096];
        let n = tokio::io::AsyncReadExt::read(&mut stream, &mut buf)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to read callback request: {e}"),
            })?;
        let request_str = String::from_utf8_lossy(&buf[..n]);

        // Extract authorization code from query params
        let code = extract_query_param(&request_str, "code").ok_or(Error::Runtime {
            message: "No authorization code in callback".to_string(),
        })?;

        let returned_state = extract_query_param(&request_str, "state");
        if returned_state.as_deref() != Some(&state) {
            return Err(Error::Runtime {
                message: "OAuth state mismatch — possible CSRF attack".to_string(),
            });
        }

        // Send success response to browser
        let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body><h2>Authentication successful!</h2><p>You can close this window.</p></body></html>";
        let _ = stream.write_all(response.as_bytes()).await;

        // Exchange code for tokens
        let token_endpoint = self.get_token_endpoint().await?;
        let mut params = vec![
            ("grant_type", "authorization_code"),
            ("client_id", self.config.client_id.as_str()),
            ("code", &code),
            ("redirect_uri", redirect),
            ("code_verifier", &code_verifier),
        ];
        if let Some(ref secret) = self.config.client_secret {
            params.push(("client_secret", secret));
        }

        self.post_token_request(&token_endpoint, &params).await
    }

    // -- Device Code Flow --

    async fn acquire_device_code(&self) -> Result<TokenResponse> {
        let discovery = self.get_discovery().await?;
        let device_endpoint = discovery
            .device_authorization_endpoint
            .ok_or(Error::Runtime {
                message: "OIDC discovery did not provide device_authorization_endpoint".to_string(),
            })?;

        let params = [
            ("client_id", self.config.client_id.as_str()),
            ("scope", &self.scopes_string()),
        ];

        let resp = self
            .http_client
            .post(&device_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Device code request failed: {e}"),
            })?;

        if !resp.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "Device code request failed with status {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                ),
            });
        }

        let device_resp: DeviceCodeResponse = resp.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse device code response: {e}"),
        })?;

        // Display instructions to user
        info!(
            "To sign in, visit {} and enter code: {}",
            device_resp.verification_uri, device_resp.user_code
        );
        if let Some(ref uri) = device_resp.verification_uri_complete {
            info!("Or visit: {uri}");
        }

        // Poll token endpoint
        let token_endpoint = self.get_token_endpoint().await?;
        let poll_interval = Duration::from_secs(device_resp.interval.unwrap_or(5));
        let deadline = Instant::now() + Duration::from_secs(device_resp.expires_in);

        loop {
            if Instant::now() >= deadline {
                return Err(Error::Runtime {
                    message: "Device code flow timed out waiting for user authentication"
                        .to_string(),
                });
            }

            tokio::time::sleep(poll_interval).await;

            let poll_params = [
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", self.config.client_id.as_str()),
                ("device_code", &device_resp.device_code),
            ];

            let poll_resp = self
                .http_client
                .post(&token_endpoint)
                .form(&poll_params)
                .send()
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Device code poll failed: {e}"),
                })?;

            if poll_resp.status().is_success() {
                return poll_resp.json().await.map_err(|e| Error::Runtime {
                    message: format!("Failed to parse token response: {e}"),
                });
            }

            // Check for pending / slow_down errors
            let body = poll_resp.text().await.unwrap_or_default();
            if body.contains("authorization_pending") {
                continue;
            }
            if body.contains("slow_down") {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            return Err(Error::Runtime {
                message: format!("Device code poll failed: {body}"),
            });
        }
    }

    // -- Azure Managed Identity Flow --

    async fn acquire_managed_identity(&self, mi_client_id: Option<&str>) -> Result<TokenResponse> {
        let resource = self.scopes_string().replace("/.default", "");

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

    // -- Workload Identity Federation Flow --

    async fn acquire_workload_identity(&self, token_file: &str) -> Result<TokenResponse> {
        let federated_token =
            tokio::fs::read_to_string(token_file)
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to read federated token file '{token_file}': {e}"),
                })?;

        let token_endpoint = self.get_token_endpoint().await?;

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", self.config.client_id.as_str()),
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", federated_token.trim()),
            ("scope", &self.scopes_string()),
        ];

        self.post_token_request(&token_endpoint, &params).await
    }

    // -- Refresh Token Flow --

    async fn refresh_with_token(&self, refresh_token: &str) -> Result<TokenResponse> {
        let token_endpoint = self.get_token_endpoint().await?;

        let mut params = vec![
            ("grant_type", "refresh_token"),
            ("client_id", self.config.client_id.as_str()),
            ("refresh_token", refresh_token),
        ];
        if let Some(ref secret) = self.config.client_secret {
            params.push(("client_secret", secret.as_str()));
        }

        self.post_token_request(&token_endpoint, &params).await
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

// -- Utility functions --

fn base64_url_encode(input: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(input)
}

/// Extract a query parameter value from a raw HTTP GET request line.
fn extract_query_param(request: &str, param: &str) -> Option<String> {
    let first_line = request.lines().next()?;
    let path = first_line.split_whitespace().nth(1)?;
    let query = path.split('?').nth(1)?;
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (kv.next(), kv.next())
            && key == param
        {
            return Some(urlencoding::decode(value).ok()?.into_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_query_param() {
        let request = "GET /callback?code=abc123&state=xyz HTTP/1.1\r\nHost: localhost\r\n";
        assert_eq!(
            extract_query_param(request, "code"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_query_param(request, "state"),
            Some("xyz".to_string())
        );
        assert_eq!(extract_query_param(request, "missing"), None);
    }

    #[test]
    fn test_extract_query_param_encoded() {
        let request = "GET /callback?code=abc%20123&state=x%26y HTTP/1.1\r\n";
        assert_eq!(
            extract_query_param(request, "code"),
            Some("abc 123".to_string())
        );
        assert_eq!(
            extract_query_param(request, "state"),
            Some("x&y".to_string())
        );
    }

    #[test]
    fn test_token_state_expiry() {
        let mut state = TokenState::new();
        assert!(state.is_expired(Duration::from_secs(0)));

        state.access_token = Some("tok".to_string());
        state.expires_at = Some(Instant::now() + Duration::from_secs(600));
        assert!(!state.is_expired(Duration::from_secs(300)));
        assert!(state.is_expired(Duration::from_secs(601)));
    }

    #[test]
    fn test_base64_url_encode() {
        let input = b"hello world";
        let encoded = base64_url_encode(input);
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
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
    fn test_token_endpoint_derivation() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/my-tenant/v2.0".to_string(),
            client_id: "id".to_string(),
            client_secret: None,
            scopes: vec!["api://test/.default".to_string()],
            flow: OAuthFlow::DeviceCode,
            refresh_buffer_secs: None,
        };
        let provider = OAuthHeaderProvider::new(config).unwrap();
        assert_eq!(
            provider.get_token_endpoint_from_issuer(),
            "https://login.microsoftonline.com/my-tenant/oauth2/v2.0/token"
        );
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
    fn test_empty_scopes_rejected() {
        let config = OAuthConfig {
            issuer_url: "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            client_id: "app-id".to_string(),
            client_secret: None,
            scopes: vec![],
            flow: OAuthFlow::DeviceCode,
            refresh_buffer_secs: None,
        };
        assert!(OAuthHeaderProvider::new(config).is_err());
    }
}
