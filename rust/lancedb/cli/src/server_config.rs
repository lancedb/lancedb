// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use directories::UserDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// The default profile name used when no profile is specified
pub const DEFAULT_PROFILE: &str = "local";

/// Port assumed for a Flight SQL address that carries neither scheme nor port.
pub const DEFAULT_SQL_PORT: u16 = 10025;

/// Port assumed for a REST address that carries neither scheme nor port.
pub const DEFAULT_HTTP_PORT: u16 = 10024;

const DEFAULT_API_KEY: &str = "sk_localtest";
const API_KEY_ENV: &str = "LANCEDB_API_KEY";
const OIDC_ACCESS_TOKEN_ENV: &str = "LANCEDB_OIDC_ACCESS_TOKEN";
const OIDC_ACCESS_TOKEN_FILE_ENV: &str = "LANCEDB_OIDC_ACCESS_TOKEN_FILE";

/// Authentication material selected for a CLI connection.
#[derive(Clone, PartialEq, Eq)]
pub enum Authentication {
    ApiKey(String),
    OidcAccessToken(String),
    OidcAccessTokenFile(PathBuf),
    ManagedOidc(std::sync::Arc<crate::auth::ManagedOidc>),
}

impl std::fmt::Debug for Authentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ApiKey(_) => f.write_str("ApiKey(<redacted>)"),
            Self::OidcAccessToken(_) => f.write_str("OidcAccessToken(<redacted>)"),
            Self::OidcAccessTokenFile(path) => {
                f.debug_tuple("OidcAccessTokenFile").field(path).finish()
            }
            Self::ManagedOidc(session) => f.debug_tuple("ManagedOidc").field(session).finish(),
        }
    }
}

/// Transport scheme of a server address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Http,
    Https,
}

impl Scheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    /// Port to use when the address names this scheme but no port.
    fn default_port(&self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
}

impl std::fmt::Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A server address split into the parts needed to rebuild a connection URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub scheme: Scheme,
    /// Host without IPv6 brackets, as it would appear in a certificate.
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    /// `host:port`, bracketing IPv6 literals.
    pub fn host_port(&self) -> String {
        crate::format_host_port(&self.host, self.port)
    }

    /// Full `scheme://host:port` URL.
    pub fn url(&self) -> String {
        format!("{}://{}", self.scheme, self.host_port())
    }

    pub fn is_tls(&self) -> bool {
        self.scheme == Scheme::Https
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url())
    }
}

/// Parse a user-supplied REST address into scheme, host, and port.
///
/// An explicit `https://` prefix is how a user asks for TLS; without a scheme
/// the address is assumed to be plaintext. `default_port` applies only when the
/// address names neither a scheme nor a port, so `https://host` still resolves
/// to 443 rather than to a LanceDB-specific default.
pub fn parse_endpoint(addr: &str, default_port: u16) -> Result<Endpoint> {
    parse_endpoint_with_schemes(addr, default_port, &["http", "https"])
}

/// Parse a user-supplied Flight SQL address, which additionally accepts the
/// gRPC scheme names. `grpc://` and `grpcs://` are aliases for `http://` and
/// `https://`: gRPC runs over the same HTTP/2 transport, and only the TLS
/// distinction reaches the connection.
pub fn parse_grpc_endpoint(addr: &str, default_port: u16) -> Result<Endpoint> {
    parse_endpoint_with_schemes(addr, default_port, &["http", "https", "grpc", "grpcs"])
}

fn parse_endpoint_with_schemes(
    addr: &str,
    default_port: u16,
    accepted: &[&str],
) -> Result<Endpoint> {
    let addr = addr.trim();

    let (scheme, rest, scheme_was_explicit) = match addr.split_once("://") {
        Some((scheme, rest)) => {
            let scheme = scheme.to_ascii_lowercase();
            if !accepted.contains(&scheme.as_str()) {
                bail!(
                    "Unsupported scheme '{scheme}' in server address '{addr}'; expected one of {}",
                    accepted.join(", ")
                );
            }
            let scheme = match scheme.as_str() {
                "https" | "grpcs" => Scheme::Https,
                _ => Scheme::Http,
            };
            (scheme, rest, true)
        }
        None => (Scheme::Http, addr, false),
    };

    // Drop any path, query, or fragment: only the authority is dialed.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        bail!("Server address '{addr}' is missing a host");
    }

    let (host, port) = split_host_port(authority, addr)?;
    let port = port.unwrap_or(if scheme_was_explicit {
        scheme.default_port()
    } else {
        default_port
    });

    Ok(Endpoint { scheme, host, port })
}

/// Split an authority into host and optional port, accepting bracketed IPv6
/// (`[::1]:10025`) and bare IPv6 literals (`::1`).
fn split_host_port(authority: &str, addr: &str) -> Result<(String, Option<u16>)> {
    let parse_port = |port: &str| -> Result<u16> {
        port.parse::<u16>()
            .with_context(|| format!("Invalid port '{port}' in server address '{addr}'"))
    };

    if let Some(rest) = authority.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| anyhow!("Unterminated IPv6 address in server address '{addr}'"))?;
        let port = match rest {
            "" => None,
            _ => Some(parse_port(rest.strip_prefix(':').ok_or_else(|| {
                anyhow!("Expected ':port' after IPv6 address in server address '{addr}'")
            })?)?),
        };
        return Ok((host.to_string(), port));
    }

    // More than one colon means a bare IPv6 literal; a port would require brackets.
    match authority.split_once(':') {
        Some((host, port)) if !port.contains(':') => {
            Ok((host.to_string(), Some(parse_port(port)?)))
        }
        _ => Ok((authority.to_string(), None)),
    }
}

/// Client-side TLS material, shared by every transport the CLI dials.
///
/// The two settings are independent: `ca_cert` decides whether the client
/// trusts the *server's* certificate, while `client_cert`/`client_key` are what
/// the client presents so the *server* can authenticate it (mTLS). A deployment
/// using a private CA for both directions needs both.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsOptions {
    /// PEM CA certificate trusted in addition to the platform roots.
    pub ca_cert: Option<String>,
    /// PEM client certificate presented for mTLS.
    pub client_cert: Option<String>,
    /// PKCS#8 PEM private key for `client_cert`.
    pub client_key: Option<String>,
}

impl TlsOptions {
    /// The client certificate and key, read from disk. `None` when no client
    /// identity is configured.
    pub fn read_client_identity(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let (Some(cert), Some(key)) = (&self.client_cert, &self.client_key) else {
            return Ok(None);
        };
        let cert_pem = fs::read(cert)
            .with_context(|| format!("Failed to read client certificate '{cert}'"))?;
        let key_pem =
            fs::read(key).with_context(|| format!("Failed to read client key '{key}'"))?;
        Ok(Some((cert_pem, key_pem)))
    }

    /// The CA certificate, read from disk.
    pub fn read_ca_cert(&self) -> Result<Option<Vec<u8>>> {
        let Some(ca_cert) = &self.ca_cert else {
            return Ok(None);
        };
        let pem = fs::read(ca_cert)
            .with_context(|| format!("Failed to read CA certificate '{ca_cert}'"))?;
        Ok(Some(pem))
    }
}

/// Resolved connection settings with guaranteed values.
///
/// This is returned by `ConnectionArgs::resolve()` after applying
/// the precedence rules: CLI/env args > profile > defaults.
#[derive(Debug, Clone)]
pub struct ResolvedConnection {
    pub sql_server_url: String,
    pub http_server_url: String,
    pub database: String,
    pub authentication: Authentication,
    pub azure_storage_account_name: Option<String>,
    pub tls: TlsOptions,
    pub profile_name: String,
}

/// Common connection arguments that can be embedded in command structs.
///
/// Use `#[command(flatten)]` to embed these in your command's args struct.
#[derive(Args, Debug, Clone, Default)]
pub struct ConnectionArgs {
    /// Connection profile to use (from ~/.lancedb/config.toml)
    #[arg(short = 'p', long, value_name = "PROFILE")]
    pub profile: Option<String>,

    /// SQL Server URL (Flight SQL endpoint), as host:port or grpc(s)://host:port - overrides profile
    #[arg(long, value_name = "URL")]
    pub sql_server_url: Option<String>,

    /// HTTP Server URL, as host:port or http(s)://host:port - overrides profile
    #[arg(long, value_name = "URL")]
    pub http_server_url: Option<String>,

    /// Database name - overrides profile
    #[arg(short = 'd', long)]
    pub database: Option<String>,

    /// API key - overrides profile and authentication environment variables
    ///
    /// Without explicit credential flags, environment precedence is LANCEDB_API_KEY,
    /// LANCEDB_OIDC_ACCESS_TOKEN, then LANCEDB_OIDC_ACCESS_TOKEN_FILE.
    #[arg(long, conflicts_with = "oidc_access_token_file")]
    pub api_key: Option<String>,

    /// File containing an OIDC access token - overrides profile and authentication environment variables
    #[arg(long, value_name = "PATH", conflicts_with = "api_key")]
    pub oidc_access_token_file: Option<String>,

    /// Azure storage account name - overrides profile
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
    pub azure_storage_account_name: Option<String>,

    /// Path to a PEM CA certificate that signed the server's TLS certificate,
    /// trusted in addition to the platform roots - overrides profile
    #[arg(long, value_name = "PATH", env = "LANCEDB_CA_CERT")]
    pub ca_cert: Option<String>,

    /// Path to a PEM client certificate to present for mTLS; requires
    /// --client-key - overrides profile
    #[arg(long, value_name = "PATH", env = "LANCEDB_CLIENT_CERT")]
    pub client_cert: Option<String>,

    /// Path to the PKCS#8 PEM private key for --client-cert - overrides profile
    #[arg(long, value_name = "PATH", env = "LANCEDB_CLIENT_KEY")]
    pub client_key: Option<String>,
}

/// The database a connection uses when none is configured: the system username.
pub fn default_database_name() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "default".to_string())
}

impl ConnectionArgs {
    /// Resolve connection settings from profile with a custom default database.
    ///
    /// Precedence: CLI/env args > profile > defaults
    pub fn resolve(&self, default_database: Option<&str>) -> Result<ResolvedConnection> {
        let config = Config::load_default()?;

        // Determine the effective profile name
        let profile_name = self
            .profile
            .clone()
            .or_else(|| config.default_profile.clone())
            .unwrap_or_else(|| DEFAULT_PROFILE.to_string());

        let profile = config.get_profile(self.profile.as_deref());
        let authentication = self.resolve_authentication(&profile, &profile_name)?;

        let sql_server_url = self
            .sql_server_url
            .clone()
            .or(profile.sql_server_url)
            .unwrap_or_else(|| "localhost:10025".to_string());

        let http_server_url = self
            .http_server_url
            .clone()
            .or(profile.http_server_url)
            .unwrap_or_else(|| "localhost:10024".to_string());

        let database = self
            .database
            .clone()
            .or(profile.database)
            .unwrap_or_else(|| {
                default_database
                    .map(|s| s.to_string())
                    .unwrap_or_else(default_database_name)
            });

        let azure_storage_account_name = self
            .azure_storage_account_name
            .clone()
            .or(profile.azure_storage_account_name);

        // A client certificate is unusable without its key, so resolve the pair
        // together rather than letting one side come from the profile and the
        // other from a flag.
        let (client_cert, client_key) = match (&self.client_cert, &self.client_key) {
            (None, None) => (profile.client_cert, profile.client_key),
            (cert, key) => (cert.clone(), key.clone()),
        };
        if client_cert.is_some() != client_key.is_some() {
            bail!("--client-cert and --client-key must be provided together");
        }

        let tls = TlsOptions {
            ca_cert: self.ca_cert.clone().or(profile.ca_cert),
            client_cert,
            client_key,
        };

        Ok(ResolvedConnection {
            sql_server_url,
            http_server_url,
            database,
            authentication,
            azure_storage_account_name,
            tls,
            profile_name,
        })
    }

    fn resolve_authentication(
        &self,
        profile: &Profile,
        profile_name: &str,
    ) -> Result<Authentication> {
        self.resolve_authentication_with_env(profile, profile_name, |name| std::env::var_os(name))
    }

    fn resolve_authentication_with_env(
        &self,
        profile: &Profile,
        profile_name: &str,
        getenv: impl Fn(&str) -> Option<std::ffi::OsString>,
    ) -> Result<Authentication> {
        if self.api_key.is_some() && self.oidc_access_token_file.is_some() {
            bail!("--api-key and --oidc-access-token-file are mutually exclusive");
        }
        profile.validate_authentication()?;

        if let Some(api_key) = &self.api_key {
            return Ok(Authentication::ApiKey(nonempty_secret(api_key, "API key")?));
        }
        if let Some(path) = &self.oidc_access_token_file {
            return Ok(Authentication::OidcAccessTokenFile(path.into()));
        }
        if let Some(api_key) = getenv(API_KEY_ENV) {
            return Ok(Authentication::ApiKey(nonempty_os_secret(
                api_key,
                API_KEY_ENV,
            )?));
        }
        if let Some(token) = getenv(OIDC_ACCESS_TOKEN_ENV) {
            return Ok(Authentication::OidcAccessToken(nonempty_os_secret(
                token,
                OIDC_ACCESS_TOKEN_ENV,
            )?));
        }
        if let Some(path) = getenv(OIDC_ACCESS_TOKEN_FILE_ENV) {
            let path = path
                .into_string()
                .map_err(|_| anyhow!("{OIDC_ACCESS_TOKEN_FILE_ENV} is not valid UTF-8"))?;
            return Ok(Authentication::OidcAccessTokenFile(path.into()));
        }
        if let Some(api_key) = &profile.api_key {
            return Ok(Authentication::ApiKey(nonempty_secret(api_key, "API key")?));
        }
        if let Some(path) = &profile.oidc_access_token_file {
            return Ok(Authentication::OidcAccessTokenFile(path.into()));
        }
        if let Some(oidc) = &profile.oidc {
            return Ok(Authentication::ManagedOidc(std::sync::Arc::new(
                crate::auth::ManagedOidc::for_profile(profile_name, oidc.clone())?,
            )));
        }
        Ok(Authentication::ApiKey(DEFAULT_API_KEY.to_string()))
    }
}

fn nonempty_secret(value: &str, description: &str) -> Result<String> {
    if value.is_empty() {
        bail!("{description} cannot be empty");
    }
    Ok(value.to_string())
}

fn nonempty_os_secret(value: std::ffi::OsString, name: &str) -> Result<String> {
    let value = value
        .into_string()
        .map_err(|_| anyhow!("{name} is not valid UTF-8"))?;
    nonempty_secret(&value, name)
}

/// A connection profile for the LanceDB CLI
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Profile {
    /// SQL server URL (Flight SQL endpoint)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_server_url: Option<String>,
    /// HTTP server URL (REST endpoint)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_server_url: Option<String>,
    /// API key for authentication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    /// Path to a file containing an OIDC access token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc_access_token_file: Option<String>,
    /// Managed OAuth authentication using the LanceDB SDK.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc: Option<crate::auth::OidcConfig>,
    /// Default database name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// Azure storage account name for Azure-backed databases
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure_storage_account_name: Option<String>,
    /// Path to a PEM CA certificate trusted in addition to the platform roots
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert: Option<String>,
    /// Path to a PEM client certificate presented for mTLS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cert: Option<String>,
    /// Path to the PKCS#8 PEM private key for `client_cert`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
}

impl Profile {
    pub(crate) fn validate_authentication(&self) -> Result<()> {
        if self.api_key.is_some() && self.oidc_access_token_file.is_some() {
            bail!("profile cannot specify both api_key and oidc_access_token_file");
        }
        if self.oidc.is_some() && (self.api_key.is_some() || self.oidc_access_token_file.is_some())
        {
            bail!("profile cannot combine oidc with api_key or oidc_access_token_file");
        }
        Ok(())
    }

    /// Create the default "local" profile for use with `lancedb server`
    pub fn local_default() -> Self {
        Self {
            sql_server_url: Some("localhost:10025".to_string()),
            http_server_url: Some("localhost:10024".to_string()),
            api_key: None,
            oidc_access_token_file: None,
            oidc: None,
            database: None,
            azure_storage_account_name: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        }
    }
}

/// Configuration file structure for ~/.lancedb/config.toml
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Data directory for LanceDB server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,

    /// Default profile name to use when none is specified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,

    /// Named connection profiles
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub profiles: HashMap<String, Profile>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: None,
            default_profile: Some(DEFAULT_PROFILE.to_string()),
            profiles: HashMap::new(),
        }
    }
}

impl Config {
    /// The database a client uses without `--database`: the default profile's, else the username.
    pub fn default_database(&self) -> String {
        self.get_profile(None)
            .database
            .unwrap_or_else(default_database_name)
    }

    /// Get the default config file path (~/.lancedb/config.toml)
    pub fn default_config_path() -> Result<PathBuf> {
        let user_dirs =
            UserDirs::new().ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;

        let config_dir = user_dirs.home_dir().join(".lancedb");
        Ok(config_dir.join("config.toml"))
    }

    /// Get the default data directory path (~/.lancedb/databases)
    pub fn default_data_dir() -> Result<String> {
        let user_dirs =
            UserDirs::new().ok_or_else(|| anyhow::anyhow!("Could not determine home directory"))?;

        let data_dir = user_dirs.home_dir().join(".lancedb").join("databases");
        Ok(data_dir.to_string_lossy().to_string())
    }

    /// Load config from the default location, or return default config if not found
    pub fn load_default() -> Result<Self> {
        let config_path = Self::default_config_path()?;

        if !config_path.exists() {
            return Ok(Self::default());
        }

        Self::load_from_path(&config_path)
    }

    /// Load config from the default location, returning None if not found
    pub fn load_default_optional() -> Result<Option<Self>> {
        let config_path = Self::default_config_path()?;

        if !config_path.exists() {
            return Ok(None);
        }

        Self::load_from_path(&config_path).map(Some)
    }

    /// Load config from a specific path
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let (config, unused) = Self::load_from_path_impl(path)?;
        for unused_key in unused {
            println!(
                "While parsing {}, ignored unused configuration key: {}",
                path.to_string_lossy(),
                unused_key
            );
        }
        Ok(config)
    }

    pub fn load_from_path_impl(path: &Path) -> Result<(Self, Vec<String>)> {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;

        let mut unused = Vec::new();
        match serde_ignored::deserialize(toml::de::Deserializer::new(&contents), |path| {
            unused.push(path.to_string())
        }) {
            Err(err) => bail!("Failed to parse config file: {:?}: {}", path, err),
            Ok(config) => Ok((config, unused)),
        }
    }

    /// Save config to the default location
    pub fn save_default(&self) -> Result<()> {
        let config_path = Self::default_config_path()?;
        self.save_to_path(&config_path)
    }

    /// Save config to a specific path
    pub fn save_to_path(&self, path: &Path) -> Result<()> {
        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config directory: {:?}", parent))?;
        }

        let contents = toml::to_string_pretty(self).context("Failed to serialize config")?;

        fs::write(path, contents)
            .with_context(|| format!("Failed to write config file: {:?}", path))?;

        Ok(())
    }

    /// Get a profile by name, falling back to built-in "local" profile
    pub fn get_profile(&self, name: Option<&str>) -> Profile {
        let profile_name = name
            .map(|s| s.to_string())
            .or_else(|| self.default_profile.clone())
            .unwrap_or_else(|| DEFAULT_PROFILE.to_string());

        self.profiles
            .get(&profile_name)
            .cloned()
            .unwrap_or_else(|| {
                if profile_name == DEFAULT_PROFILE {
                    Profile::local_default()
                } else {
                    Profile::default()
                }
            })
    }

    /// Add or update a profile
    pub fn set_profile(&mut self, name: &str, profile: Profile) {
        self.profiles.insert(name.to_string(), profile);
    }

    /// Remove a profile
    pub fn remove_profile(&mut self, name: &str) -> Option<Profile> {
        self.profiles.remove(name)
    }

    /// List all profile names
    pub fn list_profiles(&self) -> Vec<&str> {
        self.profiles.keys().map(|s| s.as_str()).collect()
    }

    /// Set the default profile name
    pub fn set_default_profile(&mut self, name: &str) {
        self.default_profile = Some(name.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parse_endpoint_defaults_to_plaintext() {
        let endpoint = parse_endpoint("localhost:10024", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(endpoint.scheme, Scheme::Http);
        assert_eq!(endpoint.url(), "http://localhost:10024");
        assert!(!endpoint.is_tls());
    }

    #[test]
    fn test_parse_endpoint_applies_default_port_without_scheme() {
        let endpoint = parse_endpoint("localhost", DEFAULT_SQL_PORT).unwrap();
        assert_eq!(endpoint.url(), "http://localhost:10025");
    }

    #[test]
    fn test_parse_endpoint_preserves_https() {
        let endpoint = parse_endpoint("https://db.example.com:8443", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(endpoint.scheme, Scheme::Https);
        assert_eq!(endpoint.url(), "https://db.example.com:8443");
        assert!(endpoint.is_tls());
    }

    #[test]
    fn test_parse_endpoint_scheme_implies_standard_port() {
        assert_eq!(
            parse_endpoint("https://db.example.com", DEFAULT_HTTP_PORT)
                .unwrap()
                .url(),
            "https://db.example.com:443"
        );
        assert_eq!(
            parse_endpoint("http://db.example.com", DEFAULT_HTTP_PORT)
                .unwrap()
                .url(),
            "http://db.example.com:80"
        );
    }

    #[test]
    fn test_parse_endpoint_ignores_trailing_path() {
        let endpoint = parse_endpoint("https://db.example.com:8443/", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(endpoint.url(), "https://db.example.com:8443");
    }

    #[test]
    fn test_parse_endpoint_ipv6() {
        let bracketed = parse_endpoint("https://[fd00::1]:8443", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(bracketed.host, "fd00::1");
        assert_eq!(bracketed.url(), "https://[fd00::1]:8443");

        // A bare IPv6 literal has no port; the default applies.
        let bare = parse_endpoint("::1", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(bare.host, "::1");
        assert_eq!(bare.url(), "http://[::1]:10024");

        let bracketed_no_port = parse_endpoint("[::1]", DEFAULT_HTTP_PORT).unwrap();
        assert_eq!(bracketed_no_port.url(), "http://[::1]:10024");
    }

    #[test]
    fn test_parse_grpc_endpoint_maps_grpc_schemes() {
        // grpc:// is plaintext HTTP/2; grpcs:// is the TLS form.
        assert_eq!(
            parse_grpc_endpoint("grpc://lancedb-query-node:10025", DEFAULT_SQL_PORT)
                .unwrap()
                .url(),
            "http://lancedb-query-node:10025"
        );
        let secure =
            parse_grpc_endpoint("grpcs://sql.example.com:10025", DEFAULT_SQL_PORT).unwrap();
        assert!(secure.is_tls());
        assert_eq!(secure.url(), "https://sql.example.com:10025");
    }

    #[test]
    fn test_parse_endpoint_rejects_invalid_addresses() {
        // gRPC schemes belong to the Flight SQL address only.
        assert!(parse_endpoint("grpc://localhost:10025", DEFAULT_SQL_PORT).is_err());
        assert!(parse_endpoint("ftp://localhost:10025", DEFAULT_SQL_PORT).is_err());
        assert!(parse_grpc_endpoint("ftp://localhost:10025", DEFAULT_SQL_PORT).is_err());
        assert!(parse_endpoint("localhost:notaport", DEFAULT_SQL_PORT).is_err());
        assert!(parse_endpoint("https://", DEFAULT_HTTP_PORT).is_err());
        assert!(parse_endpoint("[::1:10024", DEFAULT_HTTP_PORT).is_err());
    }

    #[test]
    fn test_save_and_load_config() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.toml");

        let mut config = Config {
            data_dir: Some("/test/data".to_string()),
            ..Config::default()
        };
        config.set_profile(
            "test",
            Profile {
                sql_server_url: Some("http://test.example.com".to_string()),
                http_server_url: Some("http://test.example.com".to_string()),
                api_key: Some("test-key".to_string()),
                database: Some("testdb".to_string()),
                azure_storage_account_name: Some("teststorageacct".to_string()),
                ..Profile::default()
            },
        );

        // Save config
        config.save_to_path(&config_path).unwrap();

        // Load config
        let loaded = Config::load_from_path(&config_path).unwrap();
        assert_eq!(loaded.data_dir, config.data_dir);
        assert!(loaded.profiles.contains_key("test"));
        assert_eq!(
            loaded
                .profiles
                .get("test")
                .unwrap()
                .azure_storage_account_name,
            Some("teststorageacct".to_string())
        );
    }

    #[test]
    fn test_config_serialization() {
        let config = Config {
            data_dir: Some("/test/data".to_string()),
            ..Config::default()
        };

        let serialized = toml::to_string_pretty(&config).unwrap();
        println!("Config TOML output:\n{}", serialized);
        assert!(serialized.contains("data_dir = \"/test/data\""));

        let deserialized: Config = toml::from_str(&serialized).unwrap();
        assert_eq!(deserialized.data_dir, config.data_dir);
    }

    #[test]
    fn test_profile_management() {
        let mut config = Config::default();

        // Test default profile (local)
        let profile = config.get_profile(None);
        assert_eq!(profile.sql_server_url, Some("localhost:10025".to_string()));

        // Add a custom profile
        config.set_profile(
            "production",
            Profile {
                sql_server_url: Some("http://sql.example.com".to_string()),
                http_server_url: Some("http://api.example.com".to_string()),
                api_key: Some("prod-key".to_string()),
                database: Some("proddb".to_string()),
                azure_storage_account_name: Some("prodstorageacct".to_string()),
                ..Profile::default()
            },
        );

        // Get custom profile
        let prod_profile = config.get_profile(Some("production"));
        assert_eq!(
            prod_profile.sql_server_url,
            Some("http://sql.example.com".to_string())
        );
        assert_eq!(prod_profile.api_key, Some("prod-key".to_string()));
        assert_eq!(
            prod_profile.azure_storage_account_name,
            Some("prodstorageacct".to_string())
        );

        // Set as default
        config.set_default_profile("production");
        let default_profile = config.get_profile(None);
        assert_eq!(
            default_profile.sql_server_url,
            Some("http://sql.example.com".to_string())
        );

        // List profiles
        let profiles = config.list_profiles();
        assert!(profiles.contains(&"production"));

        // Remove profile
        config.remove_profile("production");
        assert!(!config.profiles.contains_key("production"));
    }

    #[test]
    fn test_backward_compatible_config() {
        // Test that old config format (just data_dir) still loads
        let old_config = r#"data_dir = "/old/data/path""#;
        let config: Config = toml::from_str(old_config).unwrap();
        assert_eq!(config.data_dir, Some("/old/data/path".to_string()));
        assert!(config.profiles.is_empty());
    }

    #[test]
    fn test_full_config_format() {
        let full_config = r#"
data_dir = "/data/lancedb"
default_profile = "dev"

[profiles.dev]
sql_server_url = "http://localhost:8080"
http_server_url = "http://localhost:8081"
database = "devdb"
unknown_config_key = "blah"

[profiles.prod]
sql_server_url = "http://sql.prod.example.com"
http_server_url = "http://api.prod.example.com"
api_key = "secret-key"
database = "proddb"
azure_storage_account_name = "prodstorageacct"
"#;
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        fs::write(&config_path, full_config).unwrap();
        let (config, unused) = Config::load_from_path_impl(&config_path).unwrap();
        assert_eq!(vec!["profiles.dev.unknown_config_key"], unused);
        assert_eq!(config.data_dir, Some("/data/lancedb".to_string()));
        assert_eq!(config.default_profile, Some("dev".to_string()));
        assert_eq!(config.profiles.len(), 2);

        let dev = config.get_profile(Some("dev"));
        assert_eq!(
            dev.sql_server_url,
            Some("http://localhost:8080".to_string())
        );
        assert_eq!(dev.api_key, None);

        let prod = config.get_profile(Some("prod"));
        assert_eq!(prod.api_key, Some("secret-key".to_string()));
        assert_eq!(
            prod.azure_storage_account_name,
            Some("prodstorageacct".to_string())
        );
    }

    #[test]
    fn authentication_resolution_follows_flag_env_profile_order() {
        let dir = tempdir().unwrap();
        let flag_token = dir.path().join("flag-token");
        let env_token = dir.path().join("env-token");
        fs::write(&flag_token, "flag-oidc\n").unwrap();
        fs::write(&env_token, "env-file-oidc\n").unwrap();

        let profile = Profile {
            api_key: Some("profile-api-key".to_string()),
            ..Profile::default()
        };
        let args = ConnectionArgs {
            api_key: Some("flag-api-key".to_string()),
            ..ConnectionArgs::default()
        };
        let all_env = |name: &str| match name {
            API_KEY_ENV => Some("env-api-key".into()),
            OIDC_ACCESS_TOKEN_ENV => Some("env-oidc".into()),
            OIDC_ACCESS_TOKEN_FILE_ENV => Some(env_token.as_os_str().to_owned()),
            _ => None,
        };
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, all_env)
                .unwrap(),
            Authentication::ApiKey("flag-api-key".to_string())
        );

        let args = ConnectionArgs {
            oidc_access_token_file: Some(flag_token.to_string_lossy().into_owned()),
            ..ConnectionArgs::default()
        };
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, all_env)
                .unwrap(),
            Authentication::OidcAccessTokenFile(flag_token.clone())
        );

        let args = ConnectionArgs::default();
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, all_env)
                .unwrap(),
            Authentication::ApiKey("env-api-key".to_string())
        );
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, |name| match name {
                OIDC_ACCESS_TOKEN_ENV => Some("env-oidc".into()),
                OIDC_ACCESS_TOKEN_FILE_ENV => Some(env_token.as_os_str().to_owned()),
                _ => None,
            })
            .unwrap(),
            Authentication::OidcAccessToken("env-oidc".to_string())
        );
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, |name| match name {
                OIDC_ACCESS_TOKEN_FILE_ENV => Some(env_token.as_os_str().to_owned()),
                _ => None,
            })
            .unwrap(),
            Authentication::OidcAccessTokenFile(env_token.clone())
        );
        assert_eq!(
            args.resolve_authentication_with_env(&profile, DEFAULT_PROFILE, |_| None)
                .unwrap(),
            Authentication::ApiKey("profile-api-key".to_string())
        );
        assert_eq!(
            args.resolve_authentication_with_env(&Profile::default(), DEFAULT_PROFILE, |_| None)
                .unwrap(),
            Authentication::ApiKey(DEFAULT_API_KEY.to_string())
        );
    }

    #[test]
    fn profile_cannot_contain_two_authentication_methods() {
        let profile = Profile {
            api_key: Some("sk_test".to_string()),
            oidc_access_token_file: Some("token-file".to_string()),
            ..Profile::default()
        };
        let err = ConnectionArgs::default()
            .resolve_authentication_with_env(&profile, DEFAULT_PROFILE, |_| None)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "profile cannot specify both api_key and oidc_access_token_file"
        );
    }
    #[test]
    fn native_oidc_profile_and_credential_overrides() {
        let config: Config = toml::from_str(
            r#"
[profiles.automation.oidc]
flow = "client_credentials"
issuer_url = "https://identity.example.com"
client_id = "automation-client"
scopes = ["lancedb"]
client_secret_file = "/run/secrets/lancedb"
"#,
        )
        .unwrap();
        let profile = config.get_profile(Some("automation"));
        let args = ConnectionArgs::default();
        assert!(matches!(
            args.resolve_authentication_with_env(&profile, "automation", |_| None)
                .unwrap(),
            Authentication::ManagedOidc(_)
        ));
        let oidc = args
            .resolve_authentication_with_env(&profile, "automation", |name| {
                (name == OIDC_ACCESS_TOKEN_ENV).then(|| "external-token".into())
            })
            .unwrap();
        assert_eq!(
            oidc,
            Authentication::OidcAccessToken("external-token".into())
        );
        let key = args
            .resolve_authentication_with_env(&profile, "automation", |name| match name {
                API_KEY_ENV => Some("api-key".into()),
                OIDC_ACCESS_TOKEN_ENV => Some("external-token".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(key, Authentication::ApiKey("api-key".into()));
        let invalid = Profile {
            api_key: Some("key".into()),
            ..profile
        };
        assert!(args
            .resolve_authentication_with_env(&invalid, "automation", |_| None)
            .is_err());
    }
}
