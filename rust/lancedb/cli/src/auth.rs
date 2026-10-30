// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! CLI configuration and transport adapters for LanceDB's OAuth implementation.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{anyhow, ensure, Context, Result};
use clap::{Args, Subcommand};
use lancedb::remote::{
    AuthorizationCodeOptions, ClientAuthMethod, HeaderProvider, OAuthConfig, OAuthFlow,
    OAuthHeaderProvider, OAuthSession, TokenCacheOptions,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;

use crate::server_config::{Authentication, Config, DEFAULT_PROFILE};

const CLIENT_SECRET_ENV: &str = "LANCEDB_OIDC_CLIENT_SECRET";
pub const OIDC_CREDENTIAL_HEADER: &str = "x-lancedb-credential-type";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OidcFlow {
    ClientCredentials,
    AuthorizationCode,
    DeviceCode,
}

impl std::fmt::Display for OidcFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ClientCredentials => "client_credentials",
            Self::AuthorizationCode => "authorization_code",
            Self::DeviceCode => "device_code",
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OidcClientAuthMethod {
    None,
    ClientSecretBasic,
    ClientSecretPost,
}

impl From<OidcClientAuthMethod> for ClientAuthMethod {
    fn from(method: OidcClientAuthMethod) -> Self {
        match method {
            OidcClientAuthMethod::None => Self::None,
            OidcClientAuthMethod::ClientSecretBasic => Self::ClientSecretBasic,
            OidcClientAuthMethod::ClientSecretPost => Self::ClientSecretPost,
        }
    }
}

/// Nonsecret configuration stored inside a connection profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    pub flow: OidcFlow,
    pub issuer_url: String,
    pub client_id: String,
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret_file: Option<PathBuf>,
    /// When omitted, the SDK selects authentication based on the client secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_auth_method: Option<OidcClientAuthMethod>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// Registered loopback callback URI for authorization-code login.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_uri: Option<String>,
    /// Defaults to the SDK's callback port (8400).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_port: Option<u16>,
    /// Defaults to the SDK's refresh buffer (300 seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_buffer_secs: Option<u64>,
}

/// One SDK provider is shared by all transports using the resolved profile.
#[derive(Debug)]
pub struct ManagedOidc {
    config: OidcConfig,
    cache_dir: PathBuf,
    provider: OnceCell<OAuthHeaderProvider>,
}

impl PartialEq for ManagedOidc {
    fn eq(&self, other: &Self) -> bool {
        // Acquired tokens do not change which authentication configuration was selected.
        self.config == other.config && self.cache_dir == other.cache_dir
    }
}
impl Eq for ManagedOidc {}

/// A resolved credential, with no provider state attached.
pub enum Credential {
    ApiKey(String),
    OidcAccessToken(String),
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ApiKey(_) => "ApiKey(<redacted>)",
            Self::OidcAccessToken(_) => "OidcAccessToken(<redacted>)",
        })
    }
}

impl Credential {
    pub fn headers(&self) -> Result<http::HeaderMap> {
        let mut headers = http::HeaderMap::new();
        let (name, value) = match self {
            Self::ApiKey(key) => ("x-api-key", key.clone()),
            Self::OidcAccessToken(token) => {
                headers.insert(
                    OIDC_CREDENTIAL_HEADER,
                    http::HeaderValue::from_static("oidc"),
                );
                ("authorization", format!("Bearer {token}"))
            }
        };
        let mut value = http::HeaderValue::from_str(&value)
            .map_err(|_| anyhow!("Invalid authentication credential: not a valid HTTP header"))?;
        value
            .to_str()
            .map_err(|_| anyhow!("Invalid authentication credential: expected an ASCII header"))?;
        value.set_sensitive(true);
        headers.insert(name, value);
        Ok(headers)
    }
}

impl Authentication {
    pub async fn headers(&self) -> Result<http::HeaderMap> {
        match self {
            Self::ApiKey(key) => Credential::ApiKey(key.clone()).headers(),
            Self::OidcAccessToken(token) => Credential::OidcAccessToken(token.clone()).headers(),
            Self::OidcAccessTokenFile(path) => {
                let token = tokio::fs::read_to_string(path).await.with_context(|| {
                    format!("Failed to read OIDC access token file {}", path.display())
                })?;
                let token = token.trim();
                ensure!(!token.is_empty(), "OIDC access token file is empty");
                Credential::OidcAccessToken(token.to_owned()).headers()
            }
            Self::ManagedOidc(session) => session.headers().await,
        }
    }
}

#[async_trait::async_trait]
impl HeaderProvider for Authentication {
    async fn get_headers(&self) -> lancedb::Result<HashMap<String, String>> {
        let headers = self
            .headers()
            .await
            .map_err(|error| lancedb::Error::Runtime {
                message: format!("Authentication failed: {error:#}"),
            })?;
        Ok(headers
            .iter()
            .map(|(key, value)| {
                (
                    key.as_str().to_owned(),
                    value
                        .to_str()
                        .expect("validated credential header")
                        .to_owned(),
                )
            })
            .collect())
    }
}

impl ManagedOidc {
    pub fn for_profile(profile: &str, config: OidcConfig) -> Result<Self> {
        let root = Config::default_config_path()?;
        // Profile names are arbitrary strings; hashing prevents paths from escaping the cache.
        let profile_key = format!("{:x}", Sha256::digest(profile.as_bytes()));
        Self::new(
            config,
            root.parent().unwrap().join("auth").join(profile_key),
        )
    }

    fn new(config: OidcConfig, cache_dir: PathBuf) -> Result<Self> {
        ensure!(
            !config.client_id.trim().is_empty(),
            "OIDC client_id cannot be empty"
        );
        ensure!(
            !config.scopes.is_empty(),
            "OIDC scopes must contain at least one scope"
        );
        ensure!(
            config.flow == OidcFlow::AuthorizationCode
                || (config.redirect_uri.is_none() && config.callback_port.is_none()),
            "redirect_uri and callback_port require flow = authorization_code"
        );
        Ok(Self {
            config,
            cache_dir,
            provider: OnceCell::new(),
        })
    }

    pub fn flow(&self) -> &OidcFlow {
        &self.config.flow
    }

    async fn sdk_config(&self) -> Result<OAuthConfig> {
        let flow = match self.config.flow {
            OidcFlow::ClientCredentials => OAuthFlow::ClientCredentials,
            OidcFlow::DeviceCode => OAuthFlow::DeviceCode,
            OidcFlow::AuthorizationCode => {
                let mut options = AuthorizationCodeOptions::new();
                if let Some(uri) = &self.config.redirect_uri {
                    options = options.redirect_uri(uri);
                }
                if let Some(port) = self.config.callback_port {
                    options = options.callback_port(port);
                }
                OAuthFlow::AuthorizationCode(options)
            }
        };
        Ok(OAuthConfig {
            issuer_url: self.config.issuer_url.clone(),
            client_id: self.config.client_id.clone(),
            client_secret: self.client_secret().await?,
            scopes: self.config.scopes.clone(),
            resource: self.config.resource.clone(),
            audience: self.config.audience.clone(),
            flow,
            client_auth_method: self.config.client_auth_method.map(Into::into),
            refresh_buffer_secs: self.config.refresh_buffer_secs,
            token_cache: (self.config.flow != OidcFlow::ClientCredentials)
                .then(|| TokenCacheOptions::new().cache_dir(&self.cache_dir)),
        })
    }

    async fn provider(&self) -> Result<&OAuthHeaderProvider> {
        self.provider
            .get_or_try_init(|| async {
                OAuthHeaderProvider::new(self.sdk_config().await?).map_err(sdk_error)
            })
            .await
    }

    async fn session(&self) -> Result<OAuthSession> {
        OAuthSession::new(self.sdk_config().await?).map_err(sdk_error)
    }

    async fn headers(&self) -> Result<http::HeaderMap> {
        let values = self
            .provider()
            .await?
            .get_headers()
            .await
            .map_err(sdk_error)?;
        let mut headers = http::HeaderMap::new();
        for (name, value) in values {
            let name = http::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| anyhow!("Invalid authentication header name"))?;
            let mut value = http::HeaderValue::from_str(&value).map_err(|_| {
                anyhow!("Invalid authentication credential: not a valid HTTP header")
            })?;
            value.to_str().map_err(|_| {
                anyhow!("Invalid authentication credential: expected an ASCII header")
            })?;
            if name == http::header::AUTHORIZATION || name == "x-api-key" {
                value.set_sensitive(true);
            }
            headers.insert(name, value);
        }
        Ok(headers)
    }

    async fn client_secret(&self) -> Result<Option<String>> {
        if let Some(secret) = std::env::var_os(CLIENT_SECRET_ENV) {
            let secret = secret
                .into_string()
                .map_err(|_| anyhow!("{CLIENT_SECRET_ENV} must be UTF-8"))?;
            ensure!(!secret.is_empty(), "{CLIENT_SECRET_ENV} cannot be empty");
            return Ok(Some(secret));
        }
        let Some(path) = self.config.client_secret_file.as_ref() else {
            ensure!(
                self.config.flow != OidcFlow::ClientCredentials,
                "Client credentials require LANCEDB_OIDC_CLIENT_SECRET or oidc.client_secret_file"
            );
            return Ok(None);
        };
        let secret = tokio::fs::read_to_string(path)
            .await
            .context("Failed to read OIDC client secret file")?;
        let secret = secret.trim();
        ensure!(!secret.is_empty(), "OIDC client secret file is empty");
        Ok(Some(secret.to_owned()))
    }
}

fn sdk_error(error: lancedb::Error) -> anyhow::Error {
    // SDK runtime errors can contain provider response bodies and credentials.
    // Configuration errors are local; never attach the original runtime error as a cause.
    match error {
        lancedb::Error::InvalidInput { message } => anyhow!("Invalid OIDC configuration: {message}"),
        _ => anyhow!("OIDC authentication failed; check the issuer, client credentials, scopes, and provider availability. For a cached interactive session, try lancedb auth logout followed by lancedb auth login."),
    }
}

#[derive(Args, Debug)]
pub struct AuthArgs {
    #[command(subcommand)]
    pub command: AuthCommand,
}

#[derive(Subcommand, Debug)]
pub enum AuthCommand {
    /// Log in using the profile's OAuth flow; browser/device sessions cache refresh tokens.
    Login(ProfileArgs),
    /// Report whether an interactive session is cached, without contacting the provider.
    Status(ProfileArgs),
    /// Clear the profile's local interactive session; does not revoke provider credentials.
    Logout(ProfileArgs),
}

#[derive(Args, Debug)]
pub struct ProfileArgs {
    #[arg(short = 'p', long, value_name = "PROFILE")]
    pub profile: Option<String>,
}

pub async fn run(args: AuthArgs, parent_profile: Option<String>) -> Result<()> {
    let profile_args = match &args.command {
        AuthCommand::Login(args) | AuthCommand::Status(args) | AuthCommand::Logout(args) => args,
    };
    let config = Config::load_default()?;
    let profile_name = profile_args
        .profile
        .clone()
        .or(parent_profile)
        .or(config.default_profile.clone())
        .unwrap_or_else(|| DEFAULT_PROFILE.to_owned());
    let profile = config
        .profiles
        .get(&profile_name)
        .context("Profile does not exist")?;
    profile.validate_authentication()?;
    let oidc = profile
        .oidc
        .clone()
        .context("Profile has no managed OIDC configuration")?;
    let managed = ManagedOidc::for_profile(&profile_name, oidc)?;
    match args.command {
        AuthCommand::Login(_) => {
            if managed.config.flow == OidcFlow::ClientCredentials {
                managed.headers().await?;
                println!("Authenticated service account for profile '{profile_name}'. Access tokens are cached only within each process.");
            } else {
                let status = managed.session().await?.login().await.map_err(sdk_error)?;
                println!(
                    "Authenticated profile '{profile_name}' using {}",
                    managed.flow()
                );
                if !status.refreshable {
                    eprintln!("The provider returned no refresh token; later commands will need another login. Check the provider's offline-access scope and consent settings.");
                }
            }
            if [
                "LANCEDB_API_KEY",
                "LANCEDB_OIDC_ACCESS_TOKEN",
                "LANCEDB_OIDC_ACCESS_TOKEN_FILE",
            ]
            .iter()
            .any(|key| std::env::var_os(key).is_some())
            {
                eprintln!("Authentication environment overrides are set; ordinary commands will use those credentials instead of this session.");
            }
        }
        AuthCommand::Status(_) => {
            if managed.config.flow == OidcFlow::ClientCredentials {
                println!("Profile '{profile_name}' uses client credentials; no persistent session is stored.");
            } else {
                let status = managed.session().await?.status().await.map_err(sdk_error)?;
                println!(
                    "Profile '{profile_name}' uses {}; cached refresh token: {}",
                    managed.flow(),
                    if status.refreshable {
                        "present"
                    } else {
                        "not present"
                    }
                );
            }
        }
        AuthCommand::Logout(_) => {
            if managed.config.flow == OidcFlow::ClientCredentials {
                println!("Profile '{profile_name}' uses client credentials; no persistent session to clear. Configured credentials remain usable.");
            } else {
                managed.session().await?.logout().await.map_err(sdk_error)?;
                println!("Cleared the local session for profile '{profile_name}'. Provider credentials remain valid.");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
