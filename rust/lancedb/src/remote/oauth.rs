// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use log::{debug, info, warn};
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::Instant as TokioInstant;
use url::Url;

use crate::error::{Error, Result};
use crate::remote::client::HeaderProvider;

const DEFAULT_REFRESH_BUFFER_SECS: u64 = 300;
const DEFAULT_TOKEN_TTL_SECS: u64 = 3600;
const DEFAULT_CALLBACK_PORT: u16 = 8400;
const AUTHORIZATION_CALLBACK_TIMEOUT_SECS: u64 = 300;
const AZURE_IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const AZURE_IMDS_API_VERSION: &str = "2018-02-01";

/// Options for the interactive OAuth Authorization Code flow.
///
/// The built-in callback server accepts only loopback HTTP redirect URIs. PKCE
/// with the S256 challenge method is enabled by default and should be disabled
/// only for providers that do not support it.
///
/// # Example
///
/// ```
/// use lancedb::remote::{AuthorizationCodeOptions, OAuthFlow};
///
/// let flow = OAuthFlow::AuthorizationCode(
///     AuthorizationCodeOptions::new()
///         .callback_port(8400)
///         .use_pkce(true),
/// );
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AuthorizationCodeOptions {
    /// Redirect URI registered with the identity provider.
    ///
    /// Defaults to `http://127.0.0.1:{callback_port}/callback`.
    pub redirect_uri: Option<String>,

    /// Port for the built-in loopback callback server.
    ///
    /// Defaults to 8400. When `redirect_uri` contains an explicit port, this
    /// option must either be omitted or match that port.
    pub callback_port: Option<u16>,

    /// Whether to protect the authorization code exchange with S256 PKCE.
    pub use_pkce: bool,
}

impl Default for AuthorizationCodeOptions {
    fn default() -> Self {
        Self {
            redirect_uri: None,
            callback_port: None,
            use_pkce: true,
        }
    }
}

impl AuthorizationCodeOptions {
    /// Create authorization-code options with S256 PKCE enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the loopback redirect URI registered with the identity provider.
    pub fn redirect_uri(mut self, redirect_uri: impl Into<String>) -> Self {
        self.redirect_uri = Some(redirect_uri.into());
        self
    }

    /// Set the port for the built-in loopback callback server.
    pub fn callback_port(mut self, callback_port: u16) -> Self {
        self.callback_port = Some(callback_port);
        self
    }

    /// Enable or disable S256 PKCE.
    pub fn use_pkce(mut self, use_pkce: bool) -> Self {
        self.use_pkce = use_pkce;
        self
    }
}

/// OAuth authentication flow configuration.
#[derive(Debug, Clone)]
pub enum OAuthFlow {
    /// Client Credentials grant (service-to-service / M2M).
    /// Requires `client_secret` in [`OAuthConfig`].
    ClientCredentials,

    /// Authorization Code grant using an interactive browser and a built-in
    /// loopback callback server.
    AuthorizationCode(AuthorizationCodeOptions),

    /// Device Authorization grant for CLI and headless environments.
    DeviceCode,

    /// Azure Managed Identity via IMDS.
    /// Works on Azure VMs, AKS, App Service, and Azure Functions.
    /// IMDS requests bypass proxy settings because the endpoint is link-local.
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

#[derive(Clone, Debug, Deserialize)]
struct OidcDiscovery {
    token_endpoint: String,
    authorization_endpoint: Option<String>,
    device_authorization_endpoint: Option<String>,
}

// -- Token Response --

#[derive(Deserialize)]
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

impl std::fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"<redacted>")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
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
            (Some(_), None) => true,
        }
    }

    fn update(&mut self, resp: &TokenResponse) {
        self.access_token = Some(resp.access_token.clone());
        if resp.refresh_token.is_some() {
            self.refresh_token = resp.refresh_token.clone();
        }
        let expires_in = resp.expires_in.unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        self.expires_at = Some(Instant::now() + Duration::from_secs(expires_in));
    }
}

#[async_trait]
trait TokenSource: Send + Sync + std::fmt::Debug {
    async fn fetch_token(&self) -> Result<TokenResponse>;

    async fn refresh_token(&self, _refresh_token: &str) -> Result<RefreshResult> {
        Ok(RefreshResult::Unsupported)
    }
}

#[derive(Debug)]
enum RefreshResult {
    Refreshed(TokenResponse),
    Reauthenticate,
    Unsupported,
}

struct OidcClient {
    issuer_url: String,
    client_id: String,
    client_secret: Option<String>,
    scopes: Vec<String>,
    http_client: Client,
    discovery: RwLock<Option<OidcDiscovery>>,
}

impl std::fmt::Debug for OidcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcClient")
            .field("issuer_url", &self.issuer_url)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("scopes", &self.scopes)
            .finish()
    }
}

impl OidcClient {
    fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        scopes: Vec<String>,
    ) -> Result<Self> {
        Self::validate_issuer_transport(&issuer_url)?;

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create HTTP client for OAuth: {e}"),
            })?;

        Ok(Self {
            issuer_url,
            client_id,
            client_secret,
            scopes,
            http_client,
            discovery: RwLock::new(None),
        })
    }

    fn validate_issuer_transport(issuer_url: &str) -> Result<()> {
        let issuer = url::Url::parse(issuer_url).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth issuer_url: {e}"),
        })?;

        match issuer.scheme() {
            "https" => Ok(()),
            "http" if Self::is_loopback_issuer(&issuer) => Ok(()),
            _ => Err(Error::InvalidInput {
                message: "OAuth issuer_url must use https, except for loopback hosts".to_string(),
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

    async fn get_discovery(&self) -> Result<OidcDiscovery> {
        {
            let cached = self.discovery.read().await;
            if let Some(ref disc) = *cached {
                return Ok(disc.clone());
            }
        }

        let mut cache = self.discovery.write().await;
        // Double-check
        if let Some(ref disc) = *cache {
            return Ok(disc.clone());
        }

        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            self.issuer_url.trim_end_matches('/')
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

        let result = disc.clone();

        *cache = Some(disc);
        Ok(result)
    }

    async fn get_token_endpoint(&self) -> Result<String> {
        self.get_discovery().await.map(|disc| disc.token_endpoint)
    }

    fn scopes_string(&self) -> String {
        self.scopes.join(" ")
    }

    async fn post_token_request(
        &self,
        endpoint: &str,
        params: &[(String, String)],
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

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        let endpoint = self.get_token_endpoint().await?;
        let mut params = vec![
            ("grant_type".to_string(), "refresh_token".to_string()),
            ("client_id".to_string(), self.client_id.clone()),
            ("refresh_token".to_string(), refresh_token.to_string()),
        ];
        if let Some(secret) = self.client_secret.as_ref() {
            params.push(("client_secret".to_string(), secret.clone()));
        }
        let response = self
            .http_client
            .post(&endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Refresh token request to {endpoint} failed: {e}"),
            })?;
        if response.status().is_success() {
            return response
                .json()
                .await
                .map(RefreshResult::Refreshed)
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to parse refresh token response: {e}"),
                });
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let error_code = serde_json::from_str::<OAuthErrorResponse>(&body)
            .ok()
            .map(|error| error.error);
        if matches!(
            error_code.as_deref(),
            Some("invalid_grant" | "invalid_token")
        ) {
            return Ok(RefreshResult::Reauthenticate);
        }
        Err(Error::Runtime {
            message: format!("Refresh token request failed with status {status}: {body}"),
        })
    }
}

struct ClientCredentialsSource {
    oidc: OidcClient,
}

impl std::fmt::Debug for ClientCredentialsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCredentialsSource")
            .field("oidc", &self.oidc)
            .finish()
    }
}

impl ClientCredentialsSource {
    fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        scopes: Vec<String>,
    ) -> Result<Self> {
        if client_secret.is_none() {
            return Err(Error::InvalidInput {
                message: "client_secret is required for ClientCredentials flow".to_string(),
            });
        }
        Ok(Self {
            oidc: OidcClient::new(issuer_url, client_id, client_secret, scopes)?,
        })
    }
}

#[async_trait]
impl TokenSource for ClientCredentialsSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let token_endpoint = self.oidc.get_token_endpoint().await?;
        let params = [
            ("grant_type".to_string(), "client_credentials".to_string()),
            ("client_id".to_string(), self.oidc.client_id.clone()),
            (
                "client_secret".to_string(),
                self.oidc.client_secret.clone().expect("validated in new"),
            ),
            ("scope".to_string(), self.oidc.scopes_string()),
        ];

        self.oidc.post_token_request(&token_endpoint, &params).await
    }
}

#[derive(Debug)]
struct ResolvedRedirect {
    uri: String,
    bind_addr: SocketAddr,
    callback_path: String,
}

impl ResolvedRedirect {
    fn new(options: &AuthorizationCodeOptions) -> Result<Self> {
        let uri = options.redirect_uri.clone().unwrap_or_else(|| {
            format!(
                "http://127.0.0.1:{}/callback",
                options.callback_port.unwrap_or(DEFAULT_CALLBACK_PORT)
            )
        });
        let parsed = Url::parse(&uri).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth redirect_uri: {e}"),
        })?;

        if parsed.scheme() != "http" {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri must use http with a loopback host".to_string(),
            });
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri must not contain a query or fragment".to_string(),
            });
        }

        let ip = match parsed.host() {
            Some(url::Host::Domain(host)) if host.eq_ignore_ascii_case("localhost") => {
                IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            }
            Some(url::Host::Ipv4(ip)) if ip.is_loopback() => IpAddr::V4(ip),
            Some(url::Host::Ipv6(ip)) if ip.is_loopback() => IpAddr::V6(ip),
            Some(_) => {
                return Err(Error::InvalidInput {
                    message: "OAuth redirect_uri must use a loopback host".to_string(),
                });
            }
            None => {
                return Err(Error::InvalidInput {
                    message: "OAuth redirect_uri must include a loopback host".to_string(),
                });
            }
        };
        let port = parsed.port().ok_or(Error::InvalidInput {
            message: "OAuth redirect_uri must include a port".to_string(),
        })?;
        if port == 0 {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri port must be greater than zero".to_string(),
            });
        }
        if let Some(callback_port) = options.callback_port
            && callback_port != port
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "OAuth callback_port {callback_port} does not match redirect_uri port {port}"
                ),
            });
        }

        Ok(Self {
            uri,
            bind_addr: SocketAddr::new(ip, port),
            callback_path: parsed.path().to_string(),
        })
    }
}

#[derive(Debug)]
struct AuthorizationRequest {
    url: Url,
    state: String,
    code_verifier: Option<String>,
}

#[derive(Debug, PartialEq)]
enum AuthorizationCallback {
    Code(String),
    ProviderError(String),
}

struct AuthorizationCodeSource {
    oidc: OidcClient,
    options: AuthorizationCodeOptions,
    redirect: ResolvedRedirect,
}

impl std::fmt::Debug for AuthorizationCodeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationCodeSource")
            .field("oidc", &self.oidc)
            .field("options", &self.options)
            .field("redirect", &self.redirect)
            .finish()
    }
}

impl AuthorizationCodeSource {
    fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        scopes: Vec<String>,
        options: AuthorizationCodeOptions,
    ) -> Result<Self> {
        let redirect = ResolvedRedirect::new(&options)?;
        Ok(Self {
            oidc: OidcClient::new(issuer_url, client_id, client_secret, scopes)?,
            options,
            redirect,
        })
    }

    async fn build_authorization_request(&self) -> Result<AuthorizationRequest> {
        let endpoint = self
            .oidc
            .get_discovery()
            .await?
            .authorization_endpoint
            .ok_or(Error::Runtime {
                message: "OIDC discovery did not provide authorization_endpoint".to_string(),
            })?;
        let mut url = Url::parse(&endpoint).map_err(|e| Error::Runtime {
            message: format!("Invalid authorization_endpoint in OIDC discovery: {e}"),
        })?;
        let state = random_urlsafe_string(32);
        let code_verifier = self.options.use_pkce.then(|| random_urlsafe_string(64));

        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("response_type", "code")
                .append_pair("client_id", &self.oidc.client_id)
                .append_pair("redirect_uri", &self.redirect.uri)
                .append_pair("scope", &self.oidc.scopes_string())
                .append_pair("state", &state);
            if let Some(verifier) = code_verifier.as_ref() {
                let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(Sha256::digest(verifier.as_bytes()));
                query
                    .append_pair("code_challenge", &challenge)
                    .append_pair("code_challenge_method", "S256");
            }
        }

        Ok(AuthorizationRequest {
            url,
            state,
            code_verifier,
        })
    }

    async fn wait_for_callback(
        &self,
        listener: &TcpListener,
        expected_state: &str,
    ) -> Result<String> {
        let deadline =
            TokioInstant::now() + Duration::from_secs(AUTHORIZATION_CALLBACK_TIMEOUT_SECS);
        loop {
            let (mut stream, _) = tokio::time::timeout_at(deadline, listener.accept())
                .await
                .map_err(|_| Error::Runtime {
                    message: "Timed out waiting for the OAuth authorization callback".to_string(),
                })?
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to accept OAuth callback connection: {e}"),
                })?;
            match read_authorization_callback(
                &mut stream,
                &self.redirect.callback_path,
                expected_state,
                deadline,
            )
            .await
            {
                Ok(AuthorizationCallback::Code(code)) => {
                    write_callback_response(&mut stream, true).await;
                    return Ok(code);
                }
                Ok(AuthorizationCallback::ProviderError(message)) => {
                    write_callback_response(&mut stream, false).await;
                    return Err(Error::Runtime { message });
                }
                Err(error) => {
                    if TokioInstant::now() >= deadline {
                        return Err(Error::Runtime {
                            message: "Timed out waiting for the OAuth authorization callback"
                                .to_string(),
                        });
                    }
                    debug!("Ignoring unrelated OAuth callback connection: {error}");
                    write_callback_response(&mut stream, false).await;
                }
            }
        }
    }

    async fn exchange_code(
        &self,
        code: &str,
        code_verifier: Option<&str>,
    ) -> Result<TokenResponse> {
        let endpoint = self.oidc.get_token_endpoint().await?;
        let mut params = vec![
            ("grant_type".to_string(), "authorization_code".to_string()),
            ("client_id".to_string(), self.oidc.client_id.clone()),
            ("code".to_string(), code.to_string()),
            ("redirect_uri".to_string(), self.redirect.uri.clone()),
        ];
        if let Some(verifier) = code_verifier {
            params.push(("code_verifier".to_string(), verifier.to_string()));
        }
        if let Some(secret) = self.oidc.client_secret.as_ref() {
            params.push(("client_secret".to_string(), secret.clone()));
        }
        self.oidc.post_token_request(&endpoint, &params).await
    }
}

#[async_trait]
impl TokenSource for AuthorizationCodeSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let listener = TcpListener::bind(self.redirect.bind_addr)
            .await
            .map_err(|e| Error::Runtime {
                message: format!(
                    "Failed to bind OAuth callback server at {}: {e}",
                    self.redirect.bind_addr
                ),
            })?;
        let request = self.build_authorization_request().await?;
        info!("Open this URL to authenticate with OAuth: {}", request.url);
        launch_browser(request.url.clone());
        let code = self.wait_for_callback(&listener, &request.state).await?;
        self.exchange_code(&code, request.code_verifier.as_deref())
            .await
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        self.oidc.refresh_token(refresh_token).await
    }
}

#[derive(Deserialize)]
struct DeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default)]
    interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OAuthErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

struct DeviceCodeSource {
    oidc: OidcClient,
}

impl std::fmt::Debug for DeviceCodeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceCodeSource")
            .field("oidc", &self.oidc)
            .finish()
    }
}

impl DeviceCodeSource {
    fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        scopes: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            oidc: OidcClient::new(issuer_url, client_id, client_secret, scopes)?,
        })
    }

    async fn request_device_authorization(&self) -> Result<DeviceAuthorizationResponse> {
        let endpoint = self
            .oidc
            .get_discovery()
            .await?
            .device_authorization_endpoint
            .ok_or(Error::Runtime {
                message: "OIDC discovery did not provide device_authorization_endpoint".to_string(),
            })?;
        let mut params = vec![
            ("client_id".to_string(), self.oidc.client_id.clone()),
            ("scope".to_string(), self.oidc.scopes_string()),
        ];
        if let Some(secret) = self.oidc.client_secret.as_ref() {
            params.push(("client_secret".to_string(), secret.clone()));
        }
        let response = self
            .oidc
            .http_client
            .post(&endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Device authorization request to {endpoint} failed: {e}"),
            })?;
        if !response.status().is_success() {
            return Err(Error::Runtime {
                message: format!(
                    "Device authorization request failed with status {}: {}",
                    response.status(),
                    response.text().await.unwrap_or_default()
                ),
            });
        }
        response.json().await.map_err(|e| Error::Runtime {
            message: format!("Failed to parse device authorization response: {e}"),
        })
    }

    async fn poll_for_token(&self, device: &DeviceAuthorizationResponse) -> Result<TokenResponse> {
        let endpoint = self.oidc.get_token_endpoint().await?;
        let deadline = TokioInstant::now() + Duration::from_secs(device.expires_in);
        let mut interval = Duration::from_secs(device.interval.unwrap_or(5).max(1));

        loop {
            let now = TokioInstant::now();
            if now >= deadline {
                return Err(Error::Runtime {
                    message: "Device authorization expired before authentication completed"
                        .to_string(),
                });
            }
            tokio::time::sleep_until(std::cmp::min(now + interval, deadline)).await;
            if TokioInstant::now() >= deadline {
                return Err(Error::Runtime {
                    message: "Device authorization expired before authentication completed"
                        .to_string(),
                });
            }

            let mut params = vec![
                (
                    "grant_type".to_string(),
                    "urn:ietf:params:oauth:grant-type:device_code".to_string(),
                ),
                ("client_id".to_string(), self.oidc.client_id.clone()),
                ("device_code".to_string(), device.device_code.clone()),
            ];
            if let Some(secret) = self.oidc.client_secret.as_ref() {
                params.push(("client_secret".to_string(), secret.clone()));
            }

            let response = match self
                .oidc
                .http_client
                .post(&endpoint)
                .form(&params)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    warn!("Device token request to {endpoint} failed; retrying: {error}");
                    continue;
                }
            };
            if response.status().is_success() {
                return response.json().await.map_err(|e| Error::Runtime {
                    message: format!("Failed to parse device token response: {e}"),
                });
            }

            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let oauth_error = serde_json::from_str::<OAuthErrorResponse>(&body).ok();
            match oauth_error.as_ref().map(|error| error.error.as_str()) {
                Some("authorization_pending") => continue,
                Some("slow_down") => {
                    interval += Duration::from_secs(5);
                    continue;
                }
                Some("temporarily_unavailable") => continue,
                Some("access_denied") => {
                    return Err(Error::Runtime {
                        message: "Device authorization was denied by the user".to_string(),
                    });
                }
                Some("expired_token") => {
                    return Err(Error::Runtime {
                        message: "Device authorization expired before authentication completed"
                            .to_string(),
                    });
                }
                _ if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error() =>
                {
                    warn!("Device token endpoint returned {status}; retrying");
                    continue;
                }
                _ => {
                    let detail = oauth_error
                        .and_then(|error| error.error_description)
                        .unwrap_or(body);
                    return Err(Error::Runtime {
                        message: format!(
                            "Device token request failed with status {status}: {detail}"
                        ),
                    });
                }
            }
        }
    }
}

#[async_trait]
impl TokenSource for DeviceCodeSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let device = self.request_device_authorization().await?;
        info!(
            "To authenticate with OAuth, visit {} and enter code {}",
            device.verification_uri, device.user_code
        );
        let browser_url = device
            .verification_uri_complete
            .as_deref()
            .unwrap_or(&device.verification_uri);
        launch_browser(Url::parse(browser_url).map_err(|e| Error::Runtime {
            message: format!("Invalid device verification URI: {e}"),
        })?);
        self.poll_for_token(&device).await
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        self.oidc.refresh_token(refresh_token).await
    }
}

fn random_urlsafe_string(length: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

fn launch_browser(url: Url) {
    drop(tokio::task::spawn_blocking(move || {
        let result = if let Some(browser) = std::env::var_os("LANCEDB_OAUTH_BROWSER") {
            Command::new(browser).arg(url.as_str()).status().map(drop)
        } else {
            webbrowser::open(url.as_str())
        };
        if let Err(error) = result {
            warn!("Could not open an OAuth browser automatically: {error}");
        }
    }));
}

async fn read_authorization_callback(
    stream: &mut TcpStream,
    expected_path: &str,
    expected_state: &str,
    overall_deadline: TokioInstant,
) -> Result<AuthorizationCallback> {
    const MAX_CALLBACK_REQUEST_BYTES: usize = 16 * 1024;
    let deadline = std::cmp::min(
        overall_deadline,
        TokioInstant::now() + Duration::from_secs(10),
    );
    let mut request = Vec::with_capacity(1024);
    loop {
        let mut buffer = [0; 1024];
        let count = tokio::time::timeout_at(deadline, stream.read(&mut buffer))
            .await
            .map_err(|_| Error::Runtime {
                message: "Timed out reading the OAuth authorization callback".to_string(),
            })?
            .map_err(|e| Error::Runtime {
                message: format!("Failed to read OAuth authorization callback: {e}"),
            })?;
        if count == 0 {
            return Err(Error::Runtime {
                message: "OAuth authorization callback closed before sending a request".to_string(),
            });
        }
        request.extend_from_slice(&buffer[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() >= MAX_CALLBACK_REQUEST_BYTES {
            return Err(Error::Runtime {
                message: "OAuth authorization callback request was too large".to_string(),
            });
        }
    }
    let request = std::str::from_utf8(&request).map_err(|e| Error::Runtime {
        message: format!("OAuth authorization callback was not valid UTF-8: {e}"),
    })?;
    parse_authorization_callback(request, expected_path, expected_state)
}

fn parse_authorization_callback(
    request: &str,
    expected_path: &str,
    expected_state: &str,
) -> Result<AuthorizationCallback> {
    let request_target = request
        .lines()
        .next()
        .and_then(|line| {
            let mut parts = line.split_whitespace();
            (parts.next() == Some("GET"))
                .then(|| parts.next())
                .flatten()
        })
        .ok_or(Error::Runtime {
            message: "OAuth authorization callback was not a valid HTTP GET request".to_string(),
        })?;
    let callback =
        Url::parse(&format!("http://loopback{request_target}")).map_err(|e| Error::Runtime {
            message: format!("OAuth authorization callback URL was invalid: {e}"),
        })?;
    if callback.path() != expected_path {
        return Err(Error::Runtime {
            message: format!(
                "OAuth authorization callback used unexpected path {}",
                callback.path()
            ),
        });
    }
    let params: HashMap<_, _> = callback.query_pairs().into_owned().collect();
    if params.get("state").map(String::as_str) != Some(expected_state) {
        return Err(Error::Runtime {
            message: "OAuth authorization callback state did not match".to_string(),
        });
    }
    if let Some(error) = params.get("error") {
        let description = params
            .get("error_description")
            .map(String::as_str)
            .unwrap_or(error);
        return Ok(AuthorizationCallback::ProviderError(format!(
            "OAuth authorization failed: {description}"
        )));
    }
    params
        .get("code")
        .cloned()
        .map(AuthorizationCallback::Code)
        .ok_or(Error::Runtime {
            message: "OAuth authorization callback did not contain a code".to_string(),
        })
}

async fn write_callback_response(stream: &mut TcpStream, success: bool) {
    let (status, body) = if success {
        (
            "200 OK",
            "<html><body><h2>Authentication successful</h2><p>You can close this window.</p></body></html>",
        )
    } else {
        (
            "400 Bad Request",
            "<html><body><h2>Authentication failed</h2><p>Return to the application for details.</p></body></html>",
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

struct AzureImdsSource {
    client_id: Option<String>,
    resource: String,
    http_client: Client,
}

impl std::fmt::Debug for AzureImdsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureImdsSource")
            .field("client_id", &self.client_id)
            .field("resource", &self.resource)
            .finish()
    }
}

impl AzureImdsSource {
    fn new(scopes: Vec<String>, client_id: Option<String>) -> Result<Self> {
        let resource = Self::resource_from_scopes(&scopes)?;
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .no_proxy()
            .build()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create HTTP client for Azure IMDS OAuth: {e}"),
            })?;

        Ok(Self {
            client_id,
            resource,
            http_client,
        })
    }

    fn resource_from_scopes(scopes: &[String]) -> Result<String> {
        let [scope] = scopes else {
            return Err(Error::InvalidInput {
                message: "AzureManagedIdentity flow requires exactly one OAuth scope or resource"
                    .to_string(),
            });
        };

        Ok(scope.strip_suffix("/.default").unwrap_or(scope).to_string())
    }
}

#[async_trait]
impl TokenSource for AzureImdsSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let mut url = format!(
            "{AZURE_IMDS_ENDPOINT}?api-version={AZURE_IMDS_API_VERSION}&resource={}",
            urlencoding::encode(&self.resource),
        );
        if let Some(cid) = self.client_id.as_deref() {
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
}

/// OAuth header provider that manages the full token lifecycle.
///
/// Implements [`HeaderProvider`] to inject `Authorization: Bearer <token>`
/// headers into every LanceDB request, with automatic token refresh. It also
/// identifies the bearer credential as OIDC so LanceDB's SQL service selects
/// OIDC validation instead of API-key validation.
pub struct OAuthHeaderProvider {
    token_source: Box<dyn TokenSource>,
    token_state: Arc<RwLock<TokenState>>,
    refresh_buffer: Duration,
}

impl std::fmt::Debug for OAuthHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthHeaderProvider")
            .field("token_source", &self.token_source)
            .finish()
    }
}

impl OAuthHeaderProvider {
    /// Create a new OAuth header provider from configuration.
    pub fn new(config: OAuthConfig) -> Result<Self> {
        let OAuthConfig {
            issuer_url,
            client_id,
            client_secret,
            scopes,
            flow,
            refresh_buffer_secs,
        } = config;

        if scopes.is_empty() {
            return Err(Error::InvalidInput {
                message: "At least one OAuth scope is required".to_string(),
            });
        }

        let refresh_buffer =
            Duration::from_secs(refresh_buffer_secs.unwrap_or(DEFAULT_REFRESH_BUFFER_SECS));
        let token_source: Box<dyn TokenSource> = match flow {
            OAuthFlow::ClientCredentials => Box::new(ClientCredentialsSource::new(
                issuer_url,
                client_id,
                client_secret,
                scopes,
            )?),
            OAuthFlow::AuthorizationCode(options) => Box::new(AuthorizationCodeSource::new(
                issuer_url,
                client_id,
                client_secret,
                scopes,
                options,
            )?),
            OAuthFlow::DeviceCode => Box::new(DeviceCodeSource::new(
                issuer_url,
                client_id,
                client_secret,
                scopes,
            )?),
            OAuthFlow::AzureManagedIdentity { client_id } => {
                Box::new(AzureImdsSource::new(scopes, client_id)?)
            }
        };

        Ok(Self {
            token_source,
            token_state: Arc::new(RwLock::new(TokenState::new())),
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

        let refresh_token = state.refresh_token.clone();
        let resp = if let Some(refresh_token) = refresh_token.as_deref() {
            debug!("Refreshing OAuth access token via {:?}", self.token_source);
            match self.token_source.refresh_token(refresh_token).await? {
                RefreshResult::Refreshed(response) => response,
                RefreshResult::Unsupported => self.token_source.fetch_token().await?,
                RefreshResult::Reauthenticate => {
                    warn!(
                        "OAuth refresh token was rejected; acquiring a new token via {:?}",
                        self.token_source
                    );
                    state.refresh_token = None;
                    self.token_source.fetch_token().await?
                }
            }
        } else {
            debug!("Acquiring new OAuth token via {:?}", self.token_source);
            self.token_source.fetch_token().await?
        };

        state.update(&resp);
        Ok(resp.access_token)
    }
}

#[async_trait]
impl HeaderProvider for OAuthHeaderProvider {
    async fn get_headers(&self) -> Result<HashMap<String, String>> {
        let token = self.get_valid_token().await?;
        Ok(HashMap::from([
            ("authorization".to_string(), format!("Bearer {token}")),
            ("x-lancedb-credential-type".to_string(), "oidc".to_string()),
        ]))
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
            refresh_token: None,
            expires_in: None,
            token_type: None,
        };

        state.update(&response);

        assert!(!state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS - 1)));
        assert!(state.is_expired(Duration::from_secs(DEFAULT_TOKEN_TTL_SECS + 1)));
    }

    #[test]
    fn test_token_state_retains_refresh_token_when_not_rotated() {
        let mut state = TokenState::new();
        state.update(&TokenResponse {
            access_token: "token-1".to_string(),
            refresh_token: Some("refresh-1".to_string()),
            expires_in: Some(60),
            token_type: None,
        });
        state.update(&TokenResponse {
            access_token: "token-2".to_string(),
            refresh_token: None,
            expires_in: Some(60),
            token_type: None,
        });

        assert_eq!(state.refresh_token.as_deref(), Some("refresh-1"));
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
            refresh_token: Some("secret-refresh-token".to_string()),
            expires_in: Some(3600),
            token_type: Some("Bearer".to_string()),
        };

        let debug = format!("{response:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("secret-refresh-token"));
        assert!(debug.contains("access_token: \"<redacted>\""));
    }

    #[test]
    fn test_scopes_string() {
        let source = ClientCredentialsSource::new(
            "https://login.microsoftonline.com/tenant/v2.0".to_string(),
            "app-id".to_string(),
            Some("secret".to_string()),
            vec!["scope1".to_string(), "scope2".to_string()],
        )
        .unwrap();

        assert_eq!(source.oidc.scopes_string(), "scope1 scope2");
    }

    #[test]
    fn test_authorization_code_options_default_to_pkce() {
        let options = AuthorizationCodeOptions::new();

        assert!(options.use_pkce);
        assert!(options.redirect_uri.is_none());
        assert!(options.callback_port.is_none());
    }

    #[test]
    fn test_authorization_redirect_defaults_to_ipv4_loopback() {
        let redirect = ResolvedRedirect::new(&AuthorizationCodeOptions::new()).unwrap();

        assert_eq!(
            redirect.uri,
            format!("http://127.0.0.1:{DEFAULT_CALLBACK_PORT}/callback")
        );
        assert!(redirect.bind_addr.ip().is_loopback());
        assert_eq!(redirect.bind_addr.port(), DEFAULT_CALLBACK_PORT);
        assert_eq!(redirect.callback_path, "/callback");
    }

    #[test]
    fn test_authorization_redirect_rejects_non_loopback_host() {
        let options = AuthorizationCodeOptions::new()
            .redirect_uri("https://client.example.com/oauth/callback");

        let err = ResolvedRedirect::new(&options).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "OAuth redirect_uri must use http with a loopback host"
        ));
    }

    #[test]
    fn test_authorization_redirect_rejects_mismatched_port() {
        let options = AuthorizationCodeOptions::new()
            .redirect_uri("http://127.0.0.1:8401/callback")
            .callback_port(8400);

        let err = ResolvedRedirect::new(&options).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message.contains("does not match redirect_uri port")
        ));
    }

    #[test]
    fn test_authorization_redirect_requires_explicit_port() {
        let options = AuthorizationCodeOptions::new().redirect_uri("http://127.0.0.1/callback");

        let err = ResolvedRedirect::new(&options).unwrap_err();
        assert!(matches!(
            err,
            Error::InvalidInput { message }
                if message == "OAuth redirect_uri must include a port"
        ));
    }

    #[test]
    fn test_authorization_redirect_accepts_ipv6_loopback() {
        let options = AuthorizationCodeOptions::new().redirect_uri("http://[::1]:8400/callback");

        let redirect = ResolvedRedirect::new(&options).unwrap();
        assert_eq!(
            redirect.bind_addr,
            "[::1]:8400".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn test_authorization_callback_parsing() {
        let callback = parse_authorization_callback(
            "GET /callback?code=abc%20123&state=expected HTTP/1.1\r\nHost: localhost\r\n",
            "/callback",
            "expected",
        )
        .unwrap();

        assert_eq!(callback, AuthorizationCallback::Code("abc 123".to_string()));
    }

    #[test]
    fn test_authorization_callback_rejects_state_mismatch() {
        let err = parse_authorization_callback(
            "GET /callback?code=abc&state=wrong HTTP/1.1\r\nHost: localhost\r\n",
            "/callback",
            "expected",
        )
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "OAuth authorization callback state did not match"
        ));
    }

    #[test]
    fn test_authorization_callback_reports_provider_error() {
        let callback = parse_authorization_callback(
            "GET /callback?error=access_denied&error_description=user+cancelled&state=expected HTTP/1.1\r\nHost: localhost\r\n",
            "/callback",
            "expected",
        )
        .unwrap();

        assert_eq!(
            callback,
            AuthorizationCallback::ProviderError(
                "OAuth authorization failed: user cancelled".to_string()
            )
        );
    }

    #[tokio::test]
    async fn test_authorization_callback_ignores_unrelated_connection_and_partial_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let source = AuthorizationCodeSource::new(
            "http://127.0.0.1:1".to_string(),
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
            AuthorizationCodeOptions::new()
                .redirect_uri(format!("http://127.0.0.1:{port}/callback")),
        )
        .unwrap();

        let browser = tokio::spawn(async move {
            let mut unrelated = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            unrelated
                .write_all(b"GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let mut response = Vec::new();
            unrelated.read_to_end(&mut response).await.unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .starts_with("HTTP/1.1 400")
            );

            let mut callback = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            callback
                .write_all(b"GET /callback?code=auth")
                .await
                .unwrap();
            tokio::task::yield_now().await;
            callback
                .write_all(b"-code&state=expected HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
        });

        assert_eq!(
            source
                .wait_for_callback(&listener, "expected")
                .await
                .unwrap(),
            "auth-code"
        );
        browser.await.unwrap();
    }

    #[tokio::test]
    async fn test_authorization_callback_read_respects_overall_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = tokio::spawn(async move {
            let _stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let (mut stream, _) = listener.accept().await.unwrap();

        let err = read_authorization_callback(
            &mut stream,
            "/callback",
            "expected",
            TokioInstant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "Timed out reading the OAuth authorization callback"
        ));
        client.abort();
    }

    #[tokio::test]
    async fn test_authorization_request_uses_pkce_by_default() {
        let (issuer_url, server) = spawn_discovery_server(1).await;
        let source = AuthorizationCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string(), "profile".to_string()],
            AuthorizationCodeOptions::new(),
        )
        .unwrap();

        let request = source.build_authorization_request().await.unwrap();
        let params: HashMap<_, _> = request.url.query_pairs().into_owned().collect();
        assert_eq!(
            params.get("response_type").map(String::as_str),
            Some("code")
        );
        assert_eq!(
            params.get("scope").map(String::as_str),
            Some("openid profile")
        );
        assert_eq!(
            params.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert!(params.contains_key("code_challenge"));
        assert!(request.code_verifier.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_authorization_request_can_disable_pkce() {
        let (issuer_url, server) = spawn_discovery_server(1).await;
        let source = AuthorizationCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            Some("secret".to_string()),
            vec!["openid".to_string()],
            AuthorizationCodeOptions::new().use_pkce(false),
        )
        .unwrap();

        let request = source.build_authorization_request().await.unwrap();
        let params: HashMap<_, _> = request.url.query_pairs().into_owned().collect();
        assert!(!params.contains_key("code_challenge"));
        assert!(!params.contains_key("code_challenge_method"));
        assert!(request.code_verifier.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_authorization_code_exchange_includes_optional_credentials() {
        let (issuer_url, request_body, server) = spawn_token_exchange_server().await;
        let source = AuthorizationCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            Some("secret".to_string()),
            vec!["openid".to_string()],
            AuthorizationCodeOptions::new(),
        )
        .unwrap();

        let response = source
            .exchange_code("auth-code", Some("verifier"))
            .await
            .unwrap();
        assert_eq!(response.access_token, "token");
        let body = request_body.lock().unwrap().clone().unwrap();
        assert!(body.contains("grant_type=authorization_code"));
        assert!(body.contains("code=auth-code"));
        assert!(body.contains("code_verifier=verifier"));
        assert!(body.contains("client_secret=secret"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_refresh_invalid_grant_requires_reauthentication() {
        let (issuer_url, server) =
            spawn_refresh_error_server("400 Bad Request", r#"{"error":"invalid_grant"}"#).await;
        let source = AuthorizationCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
            AuthorizationCodeOptions::new(),
        )
        .unwrap();

        assert!(matches!(
            source.oidc.refresh_token("revoked").await.unwrap(),
            RefreshResult::Reauthenticate
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_refresh_transient_failure_remains_retryable() {
        let (issuer_url, server) = spawn_refresh_error_server(
            "503 Service Unavailable",
            r#"{"error":"temporarily_unavailable"}"#,
        )
        .await;
        let source = AuthorizationCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
            AuthorizationCodeOptions::new(),
        )
        .unwrap();

        let err = source.oidc.refresh_token("still-valid").await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message.contains("503 Service Unavailable")
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_device_authorization_polls_until_success() {
        let (issuer_url, token_requests, server) = spawn_device_server().await;
        let source = DeviceCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            Some("secret".to_string()),
            vec!["openid".to_string()],
        )
        .unwrap();

        let device = source.request_device_authorization().await.unwrap();
        let response = source.poll_for_token(&device).await.unwrap();

        assert_eq!(response.access_token, "device-token");
        assert_eq!(response.refresh_token.as_deref(), Some("device-refresh"));
        assert_eq!(token_requests.load(Ordering::SeqCst), 3);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_device_authorization_retries_transient_failures() {
        let (issuer_url, token_requests, server) = spawn_device_transient_server().await;
        let source = DeviceCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
        )
        .unwrap();
        let device = test_device_authorization_response(10, 1);

        let response = source.poll_for_token(&device).await.unwrap();

        assert_eq!(response.access_token, "device-token");
        assert_eq!(token_requests.load(Ordering::SeqCst), 4);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_device_authorization_reports_access_denied() {
        let (issuer_url, server) = spawn_device_error_server("access_denied").await;
        let source = DeviceCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
        )
        .unwrap();
        let device = test_device_authorization_response(60, 1);

        let err = source.poll_for_token(&device).await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "Device authorization was denied by the user"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_device_authorization_reports_provider_expiry() {
        let (issuer_url, server) = spawn_device_error_server("expired_token").await;
        let source = DeviceCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
        )
        .unwrap();
        let device = test_device_authorization_response(60, 1);

        let err = source.poll_for_token(&device).await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "Device authorization expired before authentication completed"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_device_authorization_stops_at_local_deadline() {
        let (issuer_url, server) = spawn_discovery_server(1).await;
        let source = DeviceCodeSource::new(
            issuer_url,
            "client-id".to_string(),
            None,
            vec!["openid".to_string()],
        )
        .unwrap();
        let device = test_device_authorization_response(1, 5);

        let err = source.poll_for_token(&device).await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "Device authorization expired before authentication completed"
        ));
        server.await.unwrap();
    }

    #[derive(Debug)]
    struct RefreshingTokenSource {
        fetches: Arc<AtomicUsize>,
        refreshes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TokenSource for RefreshingTokenSource {
        async fn fetch_token(&self) -> Result<TokenResponse> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(TokenResponse {
                access_token: "initial".to_string(),
                refresh_token: Some("refresh".to_string()),
                expires_in: Some(3600),
                token_type: Some("Bearer".to_string()),
            })
        }

        async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
            assert_eq!(refresh_token, "refresh");
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            Ok(RefreshResult::Refreshed(TokenResponse {
                access_token: "refreshed".to_string(),
                refresh_token: None,
                expires_in: Some(3600),
                token_type: Some("Bearer".to_string()),
            }))
        }
    }

    #[tokio::test]
    async fn test_header_provider_uses_and_retains_refresh_token() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let refreshes = Arc::new(AtomicUsize::new(0));
        let provider = OAuthHeaderProvider {
            token_source: Box::new(RefreshingTokenSource {
                fetches: Arc::clone(&fetches),
                refreshes: Arc::clone(&refreshes),
            }),
            token_state: Arc::new(RwLock::new(TokenState::new())),
            refresh_buffer: Duration::ZERO,
        };

        assert_eq!(provider.get_valid_token().await.unwrap(), "initial");
        provider.token_state.write().await.expires_at =
            Some(Instant::now() - Duration::from_secs(1));
        assert_eq!(provider.get_valid_token().await.unwrap(), "refreshed");
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.token_state.read().await.refresh_token.as_deref(),
            Some("refresh")
        );
    }

    #[derive(Debug)]
    struct FailedRefreshTokenSource {
        fetches: Arc<AtomicUsize>,
        refreshes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TokenSource for FailedRefreshTokenSource {
        async fn fetch_token(&self) -> Result<TokenResponse> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(TokenResponse {
                access_token: "reauthenticated".to_string(),
                refresh_token: Some("new-refresh".to_string()),
                expires_in: Some(3600),
                token_type: Some("Bearer".to_string()),
            })
        }

        async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
            assert_eq!(refresh_token, "revoked-refresh");
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            Ok(RefreshResult::Reauthenticate)
        }
    }

    #[tokio::test]
    async fn test_header_provider_reauthenticates_after_refresh_failure() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let refreshes = Arc::new(AtomicUsize::new(0));
        let provider = OAuthHeaderProvider {
            token_source: Box::new(FailedRefreshTokenSource {
                fetches: Arc::clone(&fetches),
                refreshes: Arc::clone(&refreshes),
            }),
            token_state: Arc::new(RwLock::new(TokenState {
                access_token: Some("expired".to_string()),
                refresh_token: Some("revoked-refresh".to_string()),
                expires_at: Some(Instant::now() - Duration::from_secs(1)),
            })),
            refresh_buffer: Duration::ZERO,
        };

        assert_eq!(provider.get_valid_token().await.unwrap(), "reauthenticated");
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.token_state.read().await.refresh_token.as_deref(),
            Some("new-refresh")
        );
    }

    #[derive(Debug)]
    struct TransientRefreshFailureSource {
        fetches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TokenSource for TransientRefreshFailureSource {
        async fn fetch_token(&self) -> Result<TokenResponse> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            unreachable!("a transient refresh failure must not start an interactive flow")
        }

        async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
            assert_eq!(refresh_token, "valid-refresh");
            Err(Error::Runtime {
                message: "token endpoint temporarily unavailable".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn test_header_provider_preserves_refresh_token_after_transient_failure() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let provider = OAuthHeaderProvider {
            token_source: Box::new(TransientRefreshFailureSource {
                fetches: Arc::clone(&fetches),
            }),
            token_state: Arc::new(RwLock::new(TokenState {
                access_token: Some("expired".to_string()),
                refresh_token: Some("valid-refresh".to_string()),
                expires_at: Some(Instant::now() - Duration::from_secs(1)),
            })),
            refresh_buffer: Duration::ZERO,
        };

        let err = provider.get_valid_token().await.unwrap_err();
        assert!(matches!(
            err,
            Error::Runtime { message }
                if message == "token endpoint temporarily unavailable"
        ));
        assert_eq!(fetches.load(Ordering::SeqCst), 0);
        assert_eq!(
            provider.token_state.read().await.refresh_token.as_deref(),
            Some("valid-refresh")
        );
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
    fn test_oauth_header_provider_debug_redacts_client_secret() {
        let config = OAuthConfig {
            issuer_url: "https://issuer.example.com".to_string(),
            client_id: "client-id".to_string(),
            client_secret: Some("super-secret".to_string()),
            scopes: vec!["scope".to_string()],
            flow: OAuthFlow::ClientCredentials,
            refresh_buffer_secs: None,
        };

        let provider = OAuthHeaderProvider::new(config).unwrap();
        let debug = format!("{provider:?}");
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("client_secret: Some(\"<redacted>\")"));
    }

    #[test]
    fn test_managed_identity_resource_from_default_scope() {
        assert_eq!(
            AzureImdsSource::resource_from_scopes(&["api://test/.default".to_string()]).unwrap(),
            "api://test"
        );
    }

    #[test]
    fn test_managed_identity_resource_without_default_suffix() {
        assert_eq!(
            AzureImdsSource::resource_from_scopes(&["api://test".to_string()]).unwrap(),
            "api://test"
        );
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
        let source = ClientCredentialsSource::new(
            issuer_url,
            "client-id".to_string(),
            Some("secret".to_string()),
            vec!["scope".to_string()],
        )
        .unwrap();

        let err = source.oidc.get_token_endpoint().await.unwrap_err();
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
                if message == "OAuth issuer_url must use https, except for loopback hosts"
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

    async fn spawn_discovery_server(expected_requests: usize) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, _) = read_http_request(&mut stream).await;
                assert!(request_line.starts_with("GET /.well-known/openid-configuration "));
                let discovery = format!(
                    r#"{{"token_endpoint":"http://{addr}/token","authorization_endpoint":"http://{addr}/authorize","device_authorization_endpoint":"http://{addr}/device"}}"#
                );
                write_json_response(&mut stream, "200 OK", &discovery).await;
            }
        });

        (issuer_url, server)
    }

    async fn spawn_token_exchange_server() -> (
        String,
        Arc<std::sync::Mutex<Option<String>>>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");
        let request_body = Arc::new(std::sync::Mutex::new(None));
        let server_request_body = Arc::clone(&request_body);

        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, body) = read_http_request(&mut stream).await;
                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(
                        r#"{{"token_endpoint":"http://{addr}/token","authorization_endpoint":"http://{addr}/authorize"}}"#
                    );
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                } else if request_line.starts_with("POST /token ") {
                    *server_request_body.lock().unwrap() = Some(body);
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        r#"{"access_token":"token","refresh_token":"refresh","expires_in":3600}"#,
                    )
                    .await;
                } else {
                    write_json_response(&mut stream, "404 Not Found", "{}").await;
                }
            }
        });

        (issuer_url, request_body, server)
    }

    async fn spawn_refresh_error_server(
        status: &'static str,
        response_body: &'static str,
    ) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, body) = read_http_request(&mut stream).await;
                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                } else if request_line.starts_with("POST /token ") {
                    assert!(body.contains("grant_type=refresh_token"));
                    assert!(body.contains("refresh_token="));
                    write_json_response(&mut stream, status, response_body).await;
                } else {
                    write_json_response(&mut stream, "404 Not Found", "{}").await;
                }
            }
        });

        (issuer_url, server)
    }

    async fn spawn_device_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");
        let token_requests = Arc::new(AtomicUsize::new(0));
        let server_token_requests = Arc::clone(&token_requests);

        let server = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, body) = read_http_request(&mut stream).await;
                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(
                        r#"{{"token_endpoint":"http://{addr}/token","device_authorization_endpoint":"http://{addr}/device"}}"#
                    );
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                } else if request_line.starts_with("POST /device ") {
                    assert!(body.contains("client_id=client-id"));
                    assert!(body.contains("client_secret=secret"));
                    assert!(body.contains("scope=openid"));
                    let device = format!(
                        r#"{{"device_code":"device-code","user_code":"ABCD-EFGH","verification_uri":"http://{addr}/verify","verification_uri_complete":"http://{addr}/verify?user_code=ABCD-EFGH","expires_in":60,"interval":1}}"#
                    );
                    write_json_response(&mut stream, "200 OK", &device).await;
                } else if request_line.starts_with("POST /token ") {
                    assert!(body.contains(
                        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Adevice_code"
                    ));
                    assert!(body.contains("device_code=device-code"));
                    assert!(body.contains("client_secret=secret"));
                    let request = server_token_requests.fetch_add(1, Ordering::SeqCst);
                    match request {
                        0 => {
                            write_json_response(
                                &mut stream,
                                "400 Bad Request",
                                r#"{"error":"authorization_pending"}"#,
                            )
                            .await;
                        }
                        1 => {
                            write_json_response(
                                &mut stream,
                                "400 Bad Request",
                                r#"{"error":"slow_down"}"#,
                            )
                            .await;
                        }
                        _ => {
                            write_json_response(
                                &mut stream,
                                "200 OK",
                                r#"{"access_token":"device-token","refresh_token":"device-refresh","expires_in":3600}"#,
                            )
                            .await;
                        }
                    }
                } else {
                    write_json_response(&mut stream, "404 Not Found", "{}").await;
                }
            }
        });

        (issuer_url, token_requests, server)
    }

    async fn spawn_device_transient_server() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");
        let token_requests = Arc::new(AtomicUsize::new(0));
        let server_token_requests = Arc::clone(&token_requests);

        let server = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, _) = read_http_request(&mut stream).await;
                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                    continue;
                }

                assert!(request_line.starts_with("POST /token "));
                match server_token_requests.fetch_add(1, Ordering::SeqCst) {
                    0 => drop(stream),
                    1 => {
                        write_json_response(
                            &mut stream,
                            "503 Service Unavailable",
                            r#"{"error":"server_error"}"#,
                        )
                        .await;
                    }
                    2 => {
                        write_json_response(
                            &mut stream,
                            "400 Bad Request",
                            r#"{"error":"temporarily_unavailable"}"#,
                        )
                        .await;
                    }
                    _ => {
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            r#"{"access_token":"device-token","expires_in":3600}"#,
                        )
                        .await;
                    }
                }
            }
        });

        (issuer_url, token_requests, server)
    }

    fn test_device_authorization_response(
        expires_in: u64,
        interval: u64,
    ) -> DeviceAuthorizationResponse {
        DeviceAuthorizationResponse {
            device_code: "device-code".to_string(),
            user_code: "ABCD-EFGH".to_string(),
            verification_uri: "http://127.0.0.1/verify".to_string(),
            verification_uri_complete: None,
            expires_in,
            interval: Some(interval),
        }
    }

    async fn spawn_device_error_server(error: &'static str) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let issuer_url = format!("http://{addr}");

        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, _) = read_http_request(&mut stream).await;
                if request_line.starts_with("GET /.well-known/openid-configuration ") {
                    let discovery = format!(r#"{{"token_endpoint":"http://{addr}/token"}}"#);
                    write_json_response(&mut stream, "200 OK", &discovery).await;
                } else if request_line.starts_with("POST /token ") {
                    write_json_response(
                        &mut stream,
                        "400 Bad Request",
                        &format!(r#"{{"error":"{error}"}}"#),
                    )
                    .await;
                } else {
                    write_json_response(&mut stream, "404 Not Found", "{}").await;
                }
            }
        });

        (issuer_url, server)
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
