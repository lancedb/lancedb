// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! OAuth authentication for LanceDB Cloud connections.
//!
//! Protocol mechanics (authorization URL and CSRF state, PKCE, token
//! exchanges, refresh, device authorization and polling, standard response
//! parsing, and token-endpoint client authentication) are delegated to the
//! [`oauth2`] crate. LanceDB owns the orchestration: OIDC discovery, endpoint
//! validation, the loopback callback server, browser and terminal
//! interaction, timeouts, token caching, and the Azure managed-identity
//! (IMDS) flow.

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use log::{debug, warn};
use oauth2::basic::BasicTokenType;
use oauth2::http::{Method, StatusCode};
use oauth2::{
    AccessToken, AuthType, AuthUrl, ClientId, ClientSecret, CsrfToken, DeviceAuthorizationUrl,
    DeviceCodeErrorResponseType, EndpointNotSet, EndpointSet, HttpRequest, HttpResponse,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, RefreshToken, RequestTokenError, Scope,
    StandardDeviceAuthorizationResponse, StandardTokenIntrospectionResponse, TokenUrl,
};
use reqwest::Client;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::time::Instant as TokioInstant;
use url::Url;

use crate::error::{Error, Result};
use crate::remote::client::HeaderProvider;

mod azure;
mod interactive;

use azure::*;
use interactive::*;

const DEFAULT_REFRESH_BUFFER_SECS: u64 = 300;
const DEFAULT_TOKEN_TTL_SECS: u64 = 3600;
const DEFAULT_CALLBACK_PORT: u16 = 8400;
const AUTHORIZATION_CALLBACK_TIMEOUT_SECS: u64 = 300;
const AZURE_IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const AZURE_IMDS_API_VERSION: &str = "2018-02-01";

fn oauth_url_uses_secure_transport(url: &Url) -> bool {
    url.scheme() == "https"
        || (url.scheme() == "http"
            && match url.host() {
                Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                None => false,
            })
}

fn validate_oauth_url(value: &str, name: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|e| Error::InvalidInput {
        message: format!("Invalid OAuth {name}: {e}"),
    })?;
    if oauth_url_uses_secure_transport(&url) {
        Ok(url)
    } else {
        Err(Error::InvalidInput {
            message: format!("OAuth {name} must use https, except for http on a loopback host"),
        })
    }
}

fn authorization_prompt(url: &Url) -> String {
    format!("Open this URL to authenticate with OAuth: {url}")
}

fn device_prompt(verification_uri: &str, user_code: &str) -> String {
    format!("To authenticate with OAuth, visit {verification_uri} and enter code {user_code}")
}

fn write_oauth_prompt(mut output: impl std::io::Write, prompt: &str) {
    let _ = writeln!(output, "{prompt}");
}

fn show_oauth_prompt(prompt: &str) {
    write_oauth_prompt(std::io::stderr().lock(), prompt);
}

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
    /// loopback callback server. The authorization URL is also written to
    /// stderr so it remains available when the browser cannot be opened.
    AuthorizationCode(AuthorizationCodeOptions),

    /// Device Authorization grant for CLI and headless environments. The
    /// verification URI and user code are written to stderr.
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

/// How the client authenticates to the OAuth token endpoint.
///
/// The method applies to every OAuth request that carries client
/// authentication: client-credentials, authorization-code exchange,
/// refresh-token, and device-authorization requests. The Azure managed
/// identity flow ignores this option because it uses its own IMDS protocol.
///
/// The default (`None` in [`OAuthConfig`]) resolves to
/// [`ClientAuthMethod::ClientSecretBasic`] when a `client_secret` is
/// configured, matching [RFC 6749 section 2.3.1](https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1)
/// and the default configuration of Okta confidential applications, and to
/// [`ClientAuthMethod::None`] for public clients (no secret), such as
/// authorization-code-with-PKCE or typical device applications.
///
/// # Example
///
/// ```
/// use lancedb::remote::{AuthorizationCodeOptions, ClientAuthMethod, OAuthConfig, OAuthFlow};
///
/// let config = OAuthConfig {
///     issuer_url: "https://idp.example.com".to_string(),
///     client_id: "client-id".to_string(),
///     client_secret: Some("secret".to_string()),
///     client_auth_method: Some(ClientAuthMethod::ClientSecretPost),
///     scopes: vec!["openid".to_string()],
///     resource: None,
///     audience: None,
///     flow: OAuthFlow::AuthorizationCode(AuthorizationCodeOptions::new()),
///     refresh_buffer_secs: None,
///     token_cache: None,
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuthMethod {
    /// No client authentication (`none`). For public clients such as
    /// browser/CLI applications using PKCE or the device flow. Requires that
    /// no `client_secret` is configured.
    None,

    /// HTTP Basic authentication (`client_secret_basic`), the RFC 6749
    /// recommended method and the normal default for confidential clients,
    /// including default Okta applications. Requires a `client_secret`.
    ClientSecretBasic,

    /// Credentials in the request body (`client_secret_post`). Some
    /// providers are configured to require this method. Requires a
    /// `client_secret`.
    ClientSecretPost,
}

impl ClientAuthMethod {
    fn auth_type(self) -> AuthType {
        match self {
            // Without a secret the crate always falls back to sending the
            // client_id in the request body, which is the desired behavior
            // for public clients.
            Self::None | Self::ClientSecretPost => AuthType::RequestBody,
            Self::ClientSecretBasic => AuthType::BasicAuth,
        }
    }
}

fn resolve_client_auth_method(
    method: Option<ClientAuthMethod>,
    client_secret: Option<&str>,
) -> Result<ClientAuthMethod> {
    match (method, client_secret) {
        (Some(ClientAuthMethod::None), Some(_)) => Err(Error::InvalidInput {
            message: "client_auth_method None cannot be combined with client_secret".to_string(),
        }),
        (
            Some(
                method @ (ClientAuthMethod::ClientSecretBasic | ClientAuthMethod::ClientSecretPost),
            ),
            None,
        ) => Err(Error::InvalidInput {
            message: format!("client_auth_method {method:?} requires client_secret to be set"),
        }),
        (Some(method), _) => Ok(method),
        (None, Some(_)) => Ok(ClientAuthMethod::ClientSecretBasic),
        (None, None) => Ok(ClientAuthMethod::None),
    }
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

    /// Resource indicator sent to the authorization and token endpoints (RFC 8707).
    /// The value is forwarded verbatim, including on refresh requests, and must
    /// be an absolute URI without a fragment.
    /// Not supported for Azure managed identity.
    pub resource: Option<String>,

    /// Provider-specific audience sent to the authorization and token endpoints,
    /// including refresh requests.
    /// Not supported for Azure managed identity.
    pub audience: Option<String>,

    /// Authentication flow to use.
    pub flow: OAuthFlow,

    /// How the client authenticates to the token endpoint. See
    /// [`ClientAuthMethod`] for the resolution rules that apply when this is
    /// `None` (the default).
    pub client_auth_method: Option<ClientAuthMethod>,

    /// Seconds before token expiry to trigger proactive refresh (default: 300).
    /// Keep this well below the token TTL; if it is greater than or equal to
    /// the TTL, each request refreshes the token.
    pub refresh_buffer_secs: Option<u64>,

    /// Opt in to the persistent token cache so short-lived processes can
    /// reuse an authenticated session instead of re-prompting.
    ///
    /// When unset (the default), tokens stay in process memory only. Only
    /// refresh tokens are persisted; see
    /// [`TokenCacheOptions`](crate::remote::TokenCacheOptions).
    pub token_cache: Option<crate::remote::token_cache::TokenCacheOptions>,
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
            .field("resource", &self.resource)
            .field("audience", &self.audience)
            .field("flow", &self.flow)
            .field("client_auth_method", &self.client_auth_method)
            .field("refresh_buffer_secs", &self.refresh_buffer_secs)
            .field("token_cache", &self.token_cache)
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

/// A token endpoint success response.
///
/// This implements [`oauth2::TokenResponse`] so the `oauth2` crate can parse
/// provider responses directly, while keeping LanceDB's lenient field
/// handling: `expires_in` may be an integer, an integer-valued float, or a
/// numeric string, and `token_type` is optional.
#[derive(Deserialize)]
pub(crate) struct TokenResponse {
    pub(crate) access_token: AccessToken,
    #[serde(default)]
    pub(crate) refresh_token: Option<RefreshToken>,
    /// Token lifetime in seconds.
    /// Some providers (Azure IMDS) return this as a string, so we accept both.
    #[serde(default, deserialize_with = "deserialize_optional_u64_or_string")]
    pub(crate) expires_in: Option<u64>,
    #[serde(default)]
    pub(crate) token_type: Option<BasicTokenType>,
}

const BEARER: BasicTokenType = BasicTokenType::Bearer;

impl oauth2::TokenResponse for TokenResponse {
    type TokenType = BasicTokenType;

    fn access_token(&self) -> &AccessToken {
        &self.access_token
    }

    fn token_type(&self) -> &BasicTokenType {
        self.token_type.as_ref().unwrap_or(&BEARER)
    }

    fn expires_in(&self) -> Option<Duration> {
        self.expires_in.map(Duration::from_secs)
    }

    fn refresh_token(&self) -> Option<&RefreshToken> {
        self.refresh_token.as_ref()
    }

    fn scopes(&self) -> Option<&Vec<Scope>> {
        None
    }
}

// The oauth2::TokenResponse trait requires Serialize; LanceDB never
// serializes token responses, so redact every credential-bearing field rather
// than risk leaking one through an accidental serialization.
impl serde::Serialize for TokenResponse {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("TokenResponse", 4)?;
        state.serialize_field("access_token", "<redacted>")?;
        state.serialize_field(
            "refresh_token",
            &self.refresh_token.as_ref().map(|_| "<redacted>"),
        )?;
        state.serialize_field("expires_in", &self.expires_in)?;
        state.serialize_field("token_type", &self.token_type)?;
        state.end()
    }
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
        self.access_token = Some(resp.access_token.secret().clone());
        if let Some(token) = resp.refresh_token.as_ref() {
            self.refresh_token = Some(token.secret().clone());
        }
        let expires_in = resp.expires_in.unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        self.expires_at = Some(Instant::now() + Duration::from_secs(expires_in));
    }
}

#[async_trait]
pub(crate) trait TokenSource: Send + Sync + std::fmt::Debug {
    async fn fetch_token(&self) -> Result<TokenResponse>;

    async fn refresh_token(&self, _refresh_token: &str) -> Result<RefreshResult> {
        Ok(RefreshResult::Unsupported)
    }
}

#[derive(Debug)]
pub(crate) enum RefreshResult {
    Refreshed(TokenResponse),
    Reauthenticate,
    Unsupported,
}

// -- OAuth HTTP transport --

/// Errors raised by [`OAuthHttpClient`].
#[derive(Debug)]
enum OAuthHttpError {
    /// The request could not be built (invalid method, URL, or headers).
    Build(String),
    /// The request failed at the transport layer. This includes redirects
    /// rejected by the hardened client redirect policy.
    Transport(reqwest::Error),
    /// The server reported a transient condition: HTTP 429, a 5xx status, or
    /// an OAuth `temporarily_unavailable` error. The device-code poll loop
    /// treats these as retryable; single-shot requests surface them as errors.
    Transient(StatusCode),
}

impl std::fmt::Display for OAuthHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(message) => write!(f, "could not build OAuth request: {message}"),
            Self::Transport(error) => {
                write!(f, "OAuth HTTP request failed: {error}")?;
                // Include the underlying cause (e.g. a redirect rejected by
                // the hardened client policy) without ever including bodies.
                if let Some(source) = std::error::Error::source(error) {
                    write!(f, ": {source}")?;
                }
                Ok(())
            }
            Self::Transient(status) => {
                write!(f, "OAuth server returned a transient response ({status})")
            }
        }
    }
}

impl std::error::Error for OAuthHttpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct OAuthHttpClient {
    inner: Client,
}

impl OAuthHttpClient {
    fn is_retryable_status_or_body(status: StatusCode, body: &[u8]) -> bool {
        if status.as_u16() == 429 || status.is_server_error() {
            return true;
        }
        serde_json::from_slice::<OAuthErrorResponse>(body)
            .map(|error| error.error == "temporarily_unavailable")
            .unwrap_or(false)
    }
}

impl<'c> oauth2::AsyncHttpClient<'c> for OAuthHttpClient {
    type Error = OAuthHttpError;
    type Future = Pin<
        Box<
            dyn Future<Output = std::result::Result<HttpResponse, OAuthHttpError>>
                + Send
                + Sync
                + 'c,
        >,
    >;

    fn call(&'c self, request: HttpRequest) -> Self::Future {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let method = Method::from_bytes(parts.method.as_str().as_bytes())
                .map_err(|e| OAuthHttpError::Build(e.to_string()))?;
            let url: Url = parts
                .uri
                .to_string()
                .parse()
                .map_err(|e| OAuthHttpError::Build(format!("invalid request URL: {e}")))?;

            let response = self
                .inner
                .request(method, url)
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .map_err(OAuthHttpError::Transport)?;

            let status = response.status();
            let mut builder = oauth2::http::Response::builder().status(status);
            for (name, value) in response.headers().iter() {
                builder = builder.header(name, value);
            }
            let body = response
                .bytes()
                .await
                .map_err(OAuthHttpError::Transport)?
                .to_vec();
            let response = builder
                .body(body)
                .map_err(|e| OAuthHttpError::Build(e.to_string()))?;

            if !status.is_success() && Self::is_retryable_status_or_body(status, response.body()) {
                debug!("OAuth token endpoint returned a transient response ({status})");
                return Err(OAuthHttpError::Transient(status));
            }
            Ok(response)
        })
    }
}

// -- OAuth client construction --

type OauthClient<HasAuthUrl, HasDeviceAuthUrl, HasTokenUrl> = oauth2::Client<
    oauth2::basic::BasicErrorResponse,
    TokenResponse,
    StandardTokenIntrospectionResponse<oauth2::EmptyExtraTokenFields, BasicTokenType>,
    oauth2::StandardRevocableToken,
    oauth2::basic::BasicErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    EndpointNotSet,
    EndpointNotSet,
    HasTokenUrl,
>;

type BaseOauthClient = OauthClient<EndpointNotSet, EndpointNotSet, EndpointNotSet>;

type TokenEndpointClient = OauthClient<EndpointNotSet, EndpointNotSet, EndpointSet>;

fn token_error_context<T: oauth2::ErrorResponse>(context: &str, response: &T) -> String {
    // StandardErrorResponse's Display renders only the provider's error code,
    // description, and error URI; it never includes credential material.
    format!("{context} failed: {response}")
}

fn map_token_error(
    error: RequestTokenError<OAuthHttpError, oauth2::basic::BasicErrorResponse>,
    context: &str,
) -> Error {
    match error {
        RequestTokenError::ServerResponse(response) => Error::Runtime {
            message: token_error_context(context, &response),
        },
        RequestTokenError::Request(error) => Error::Runtime {
            message: format!("{context} failed: {error}"),
        },
        // Never include the raw body: it may contain credential material.
        RequestTokenError::Parse(error, _) => Error::Runtime {
            message: format!("{context} response could not be parsed: {error}"),
        },
        RequestTokenError::Other(message) => Error::Runtime {
            message: format!("{context} failed: {message}"),
        },
    }
}

fn map_device_token_error(
    error: RequestTokenError<OAuthHttpError, oauth2::DeviceCodeErrorResponse>,
) -> Error {
    match error {
        RequestTokenError::ServerResponse(response) => match response.error() {
            DeviceCodeErrorResponseType::AccessDenied => Error::Runtime {
                message: "Device authorization was denied by the user".to_string(),
            },
            DeviceCodeErrorResponseType::ExpiredToken => Error::Runtime {
                message: "Device authorization expired before authentication completed".to_string(),
            },
            _ => Error::Runtime {
                message: token_error_context("Device token request", &response),
            },
        },
        RequestTokenError::Request(error) => Error::Runtime {
            message: format!("Device token request failed: {error}"),
        },
        RequestTokenError::Parse(error, _) => Error::Runtime {
            message: format!("Device token response could not be parsed: {error}"),
        },
        RequestTokenError::Other(message) => Error::Runtime {
            message: format!("Device token request failed: {message}"),
        },
    }
}

struct OidcClient {
    issuer_url: String,
    client_id: String,
    client_secret: Option<String>,
    client_auth_method: ClientAuthMethod,
    scopes: Vec<String>,
    resource: Option<String>,
    audience: Option<String>,
    http_client: OAuthHttpClient,
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
            .field("client_auth_method", &self.client_auth_method)
            .field("scopes", &self.scopes)
            .field("resource", &self.resource)
            .field("audience", &self.audience)
            .finish()
    }
}

impl OidcClient {
    fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        client_auth_method: ClientAuthMethod,
        scopes: Vec<String>,
        resource: Option<String>,
        audience: Option<String>,
    ) -> Result<Self> {
        Self::validate_issuer_transport(&issuer_url)?;

        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if oauth_url_uses_secure_transport(attempt.url()) {
                    attempt.follow()
                } else {
                    attempt
                        .error("OAuth redirects must use https, except for http on a loopback host")
                }
            }))
            .build()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create HTTP client for OAuth: {e}"),
            })?;

        Ok(Self {
            issuer_url,
            client_id,
            client_secret,
            client_auth_method,
            scopes,
            resource,
            audience,
            http_client: OAuthHttpClient { inner: http_client },
            discovery: RwLock::new(None),
        })
    }

    fn validate_issuer_transport(issuer_url: &str) -> Result<()> {
        validate_oauth_url(issuer_url, "issuer_url").map(drop)
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
            .inner
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
        validate_oauth_url(&disc.token_endpoint, "token_endpoint")?;
        if let Some(endpoint) = disc.authorization_endpoint.as_deref() {
            validate_oauth_url(endpoint, "authorization_endpoint")?;
        }
        if let Some(endpoint) = disc.device_authorization_endpoint.as_deref() {
            validate_oauth_url(endpoint, "device_authorization_endpoint")?;
        }

        let result = disc.clone();

        *cache = Some(disc);
        Ok(result)
    }

    async fn get_token_endpoint(&self) -> Result<String> {
        self.get_discovery().await.map(|disc| disc.token_endpoint)
    }

    /// Resource/audience parameters forwarded to every authorization and
    /// token request.
    fn target_params(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.resource
            .as_deref()
            .map(|value| ("resource", value))
            .into_iter()
            .chain(self.audience.as_deref().map(|value| ("audience", value)))
    }

    fn base_client(&self) -> BaseOauthClient {
        let mut client = oauth2::Client::new(ClientId::new(self.client_id.clone()))
            .set_auth_type(self.client_auth_method.auth_type());
        if let Some(secret) = self.client_secret.as_ref() {
            client = client.set_client_secret(ClientSecret::new(secret.clone()));
        }
        client
    }

    async fn token_client(&self) -> Result<(TokenEndpointClient, String)> {
        let endpoint = self.get_token_endpoint().await?;
        let token_url = TokenUrl::new(endpoint.clone()).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth token_endpoint: {e}"),
        })?;
        Ok((self.base_client().set_token_uri(token_url), endpoint))
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        let (client, _) = self.token_client().await?;
        let refresh_token = RefreshToken::new(refresh_token.to_string());
        let mut request = client.exchange_refresh_token(&refresh_token);
        for (name, value) in self.target_params() {
            request = request.add_extra_param(name, value);
        }
        match request.request_async(&self.http_client).await {
            Ok(response) => Ok(RefreshResult::Refreshed(response)),
            Err(RequestTokenError::ServerResponse(response))
                if matches!(response.error().as_ref(), "invalid_grant" | "invalid_token") =>
            {
                Ok(RefreshResult::Reauthenticate)
            }
            Err(error) => Err(map_token_error(error, "Refresh token request")),
        }
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
        client_auth_method: ClientAuthMethod,
        scopes: Vec<String>,
        resource: Option<String>,
        audience: Option<String>,
    ) -> Result<Self> {
        if client_secret.is_none() {
            return Err(Error::InvalidInput {
                message: "client_secret is required for ClientCredentials flow".to_string(),
            });
        }
        Ok(Self {
            oidc: OidcClient::new(
                issuer_url,
                client_id,
                client_secret,
                client_auth_method,
                scopes,
                resource,
                audience,
            )?,
        })
    }
}

#[async_trait]
impl TokenSource for ClientCredentialsSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let (client, endpoint) = self.oidc.token_client().await?;
        let mut request = client.exchange_client_credentials();
        for scope in &self.oidc.scopes {
            request = request.add_scope(Scope::new(scope.clone()));
        }
        for (name, value) in self.oidc.target_params() {
            request = request.add_extra_param(name, value);
        }

        request
            .request_async(&self.oidc.http_client)
            .await
            .map_err(|e| map_token_error(e, &format!("Token request to {endpoint}")))
    }
}

/// Build the token source for a configuration.
///
/// Shared by [`OAuthHeaderProvider`] and
/// [`OAuthSession`](crate::remote::OAuthSession).
pub(crate) fn build_token_source(config: &OAuthConfig) -> Result<Box<dyn TokenSource>> {
    if matches!(config.flow, OAuthFlow::AzureManagedIdentity { .. })
        && (config.resource.is_some() || config.audience.is_some())
    {
        return Err(Error::InvalidInput {
            message: "resource and audience are not supported for AzureManagedIdentity; configure its resource through scopes".to_string(),
        });
    }
    if config.scopes.is_empty() {
        return Err(Error::InvalidInput {
            message: "At least one OAuth scope is required".to_string(),
        });
    }
    let client_auth_method =
        resolve_client_auth_method(config.client_auth_method, config.client_secret.as_deref())?;
    Ok(match &config.flow {
        OAuthFlow::ClientCredentials => Box::new(ClientCredentialsSource::new(
            config.issuer_url.clone(),
            config.client_id.clone(),
            config.client_secret.clone(),
            client_auth_method,
            config.scopes.clone(),
            config.resource.clone(),
            config.audience.clone(),
        )?),
        OAuthFlow::AuthorizationCode(options) => Box::new(AuthorizationCodeSource::new(
            config.issuer_url.clone(),
            config.client_id.clone(),
            config.client_secret.clone(),
            client_auth_method,
            config.scopes.clone(),
            config.resource.clone(),
            config.audience.clone(),
            options.clone(),
        )?),
        OAuthFlow::DeviceCode => Box::new(DeviceCodeSource::new(
            config.issuer_url.clone(),
            config.client_id.clone(),
            config.client_secret.clone(),
            client_auth_method,
            config.scopes.clone(),
            config.resource.clone(),
            config.audience.clone(),
        )?),
        OAuthFlow::AzureManagedIdentity { client_id } => Box::new(AzureImdsSource::new(
            config.scopes.clone(),
            client_id.clone(),
        )?),
    })
}

/// OAuth header provider that manages the full token lifecycle.
///
/// Implements [`HeaderProvider`] to inject `Authorization: Bearer <token>`
/// headers into every LanceDB request, with automatic token refresh. It also
/// identifies the bearer credential as OIDC so LanceDB's SQL service selects
/// OIDC validation instead of API-key validation.
///
/// When the configuration enables
/// [`token_cache`](OAuthConfig::token_cache), tokens are additionally shared
/// through a hardened on-disk cache so separate processes reuse one session;
/// see [`crate::remote::token_cache`].
pub struct OAuthHeaderProvider {
    token_source: Box<dyn TokenSource>,
    token_state: Arc<RwLock<TokenState>>,
    refresh_buffer: Duration,
    token_cache: Option<Arc<crate::remote::token_cache::TokenCache>>,
}

impl std::fmt::Debug for OAuthHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthHeaderProvider")
            .field("token_source", &self.token_source)
            .field("token_cache", &self.token_cache)
            .finish()
    }
}

impl OAuthHeaderProvider {
    /// Create a new OAuth header provider from configuration.
    pub fn new(config: OAuthConfig) -> Result<Self> {
        let refresh_buffer = Duration::from_secs(
            config
                .refresh_buffer_secs
                .unwrap_or(DEFAULT_REFRESH_BUFFER_SECS),
        );
        let token_source = build_token_source(&config)?;
        let token_cache = crate::remote::token_cache::token_cache_for_config(&config)?;

        Ok(Self {
            token_source,
            token_state: Arc::new(RwLock::new(TokenState::new())),
            refresh_buffer,
            token_cache,
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

        if let Some(cache) = &self.token_cache {
            // Cross-process critical section: serialize with other processes,
            // reread the durable record, refresh or acquire exactly once, and
            // persist the rotated refresh token.
            let resp = cache.refresh_or_acquire(self.token_source.as_ref()).await?;
            state.update(&resp);
            return Ok(resp.access_token.secret().clone());
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
        Ok(resp.access_token.secret().clone())
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
mod tests;
