// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! GCP credential vending using downscoped OAuth2 tokens.
//!
//! This module provides credential vending for GCP Cloud Storage by obtaining
//! OAuth2 access tokens and downscoping them using Credential Access Boundaries (CAB).

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lance_core::Result;
use lance_io::object_store::uri_to_url;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::Identity;
use log::{debug, info, warn};
use reqwest::Client;
use ring::signature::RsaKeyPair;
use rustls_pki_types::PrivateKeyDer;
use rustls_pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{CredentialVendor, VendedCredentials, VendedPermission, redact_credential};

/// GCP STS token exchange endpoint for downscoping credentials.
const STS_TOKEN_EXCHANGE_URL: &str = "https://sts.googleapis.com/v1/token";
const IAM_GENERATE_ACCESS_TOKEN_URL: &str =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts";
const DEFAULT_GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";
const DEFAULT_METADATA_IP: &str = "169.254.169.254";
const METADATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Configuration for GCP credential vending.
#[derive(Debug, Clone, Default)]
pub struct GcpCredentialVendorConfig {
    /// Optional service account to impersonate.
    ///
    /// When set, the vendor will impersonate this service account using the
    /// IAM Credentials API's generateAccessToken endpoint before downscoping.
    pub service_account: Option<String>,

    /// Permission level for vended credentials.
    /// Default: Read
    /// Permissions are enforced via Credential Access Boundaries (CAB).
    pub permission: VendedPermission,

    /// Workload Identity Provider resource name for OIDC token exchange.
    /// Required when using auth_token identity for Workload Identity Federation.
    pub workload_identity_provider: Option<String>,

    /// Service account to impersonate after Workload Identity Federation.
    /// Optional - if set, the exchanged token will be used to generate an
    /// access token for this service account.
    pub impersonation_service_account: Option<String>,

    /// Salt for API key hashing.
    /// Required when using API key authentication.
    /// API keys are hashed as: SHA256(api_key + ":" + salt)
    pub api_key_salt: Option<String>,

    /// Map of SHA256(api_key + ":" + salt) -> permission level.
    /// When an API key is provided, its hash is looked up in this map.
    /// If found, the mapped permission is used instead of the default permission.
    pub api_key_hash_permissions: HashMap<String, VendedPermission>,
}

impl GcpCredentialVendorConfig {
    /// Create a new default config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the service account to impersonate.
    pub fn with_service_account(mut self, service_account: impl Into<String>) -> Self {
        self.service_account = Some(service_account.into());
        self
    }

    /// Set the permission level for vended credentials.
    pub fn with_permission(mut self, permission: VendedPermission) -> Self {
        self.permission = permission;
        self
    }

    /// Set the Workload Identity Provider for OIDC token exchange.
    pub fn with_workload_identity_provider(mut self, provider: impl Into<String>) -> Self {
        self.workload_identity_provider = Some(provider.into());
        self
    }

    /// Set the service account to impersonate after Workload Identity Federation.
    pub fn with_impersonation_service_account(
        mut self,
        service_account: impl Into<String>,
    ) -> Self {
        self.impersonation_service_account = Some(service_account.into());
        self
    }

    /// Set the API key salt for hashing.
    pub fn with_api_key_salt(mut self, salt: impl Into<String>) -> Self {
        self.api_key_salt = Some(salt.into());
        self
    }

    /// Add an API key hash to permission mapping.
    pub fn with_api_key_hash_permission(
        mut self,
        key_hash: impl Into<String>,
        permission: VendedPermission,
    ) -> Self {
        self.api_key_hash_permissions
            .insert(key_hash.into(), permission);
        self
    }

    /// Replace all API key hash mappings.
    pub fn with_api_key_hash_permissions(
        mut self,
        permissions: HashMap<String, VendedPermission>,
    ) -> Self {
        self.api_key_hash_permissions = permissions;
        self
    }
}

#[derive(Debug, Serialize)]
struct CredentialAccessBoundary {
    access_boundary: AccessBoundaryInner,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AccessBoundaryInner {
    access_boundary_rules: Vec<AccessBoundaryRule>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AccessBoundaryRule {
    available_resource: String,
    available_permissions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    availability_condition: Option<AvailabilityCondition>,
}

#[derive(Debug, Serialize, Clone)]
struct AvailabilityCondition {
    expression: String,
}

/// Response from STS token exchange.
#[derive(Debug, Deserialize)]
struct TokenExchangeResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Response from refresh-token and service-account OAuth flows.
#[derive(Debug, Deserialize)]
struct OAuthAccessTokenResponse {
    access_token: String,
    #[allow(dead_code)]
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Response from metadata server.
#[derive(Debug, Deserialize)]
struct MetadataAccessTokenResponse {
    access_token: String,
}

/// Response from IAM generateAccessToken API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenerateAccessTokenResponse {
    access_token: String,
    #[allow(dead_code)]
    expire_time: String,
}

#[derive(Debug, Serialize)]
struct ServiceAccountClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Default, Serialize)]
struct JwtHeader<'a> {
    alg: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    typ: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kid: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ApplicationDefaultCredentials {
    #[serde(rename = "service_account")]
    ServiceAccount(ServiceAccountCredentials),
    #[serde(rename = "authorized_user")]
    AuthorizedUser(AuthorizedUserCredentials),
}

impl ApplicationDefaultCredentials {
    const CREDENTIALS_PATH: &'static str = if cfg!(windows) {
        "gcloud/application_default_credentials.json"
    } else {
        ".config/gcloud/application_default_credentials.json"
    };

    fn read(path: Option<&str>) -> Result<Option<Self>> {
        if let Some(path) = path {
            return Ok(Some(read_credentials_file(path)?));
        }

        let home_var = if cfg!(windows) { "APPDATA" } else { "HOME" };
        if let Some(home) = env::var_os(home_var) {
            let path = Path::new(&home).join(Self::CREDENTIALS_PATH);
            if path.exists() {
                return Ok(Some(read_credentials_file(path)?));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
struct AuthorizedUserCredentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(default)]
    token_uri: Option<String>,
}

impl AuthorizedUserCredentials {
    fn token_uri(&self) -> &str {
        self.token_uri
            .as_deref()
            .unwrap_or(DEFAULT_GOOGLE_TOKEN_URI)
    }

    async fn fetch_access_token(&self, http_client: &Client) -> Result<String> {
        let response = http_client
            .post(self.token_uri())
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("refresh_token", self.refresh_token.as_str()),
            ])
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to refresh GCP authorized user token from '{}': {}",
                        self.token_uri(),
                        err
                    ),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "GCP authorized user token request to '{}' failed with status {}: {}",
                    self.token_uri(),
                    status,
                    body
                ),
            }
            .into());
        }

        let token_response: OAuthAccessTokenResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to parse GCP authorized user token response: {}",
                    err
                ),
            })
        })?;

        Ok(token_response.access_token)
    }
}

#[derive(Debug, Deserialize)]
struct ServiceAccountCredentials {
    client_email: String,
    private_key: String,
    private_key_id: String,
    #[serde(default)]
    token_uri: Option<String>,
}

struct ServiceAccountKey(RsaKeyPair);

impl std::fmt::Debug for ServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ServiceAccountKey([redacted])")
    }
}

impl ServiceAccountKey {
    fn from_pem(encoded: &[u8]) -> Result<Self> {
        match PrivateKeyDer::from_pem_slice(encoded).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to parse GCP service account PEM key: {}", err),
            })
        })? {
            PrivateKeyDer::Pkcs8(key) => Self::from_pkcs8(key.secret_pkcs8_der()),
            PrivateKeyDer::Pkcs1(key) => Self::from_der(key.secret_pkcs1_der()),
            _ => Err(NamespaceError::Internal {
                message: "Unsupported GCP service account private key encoding".to_string(),
            }
            .into()),
        }
    }

    fn from_pkcs8(key: &[u8]) -> Result<Self> {
        Ok(Self(RsaKeyPair::from_pkcs8(key).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Invalid PKCS#8 GCP service account key: {}", err),
            })
        })?))
    }

    fn from_der(key: &[u8]) -> Result<Self> {
        Ok(Self(RsaKeyPair::from_der(key).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Invalid RSA GCP service account key: {}", err),
            })
        })?))
    }

    fn sign(&self, message: &str) -> Result<String> {
        let mut signature = vec![0; self.0.public().modulus_len()];
        self.0
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                message.as_bytes(),
                &mut signature,
            )
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to sign GCP service account JWT assertion: {}", err),
                })
            })?;
        Ok(URL_SAFE_NO_PAD.encode(signature))
    }
}

#[derive(Debug)]
struct ServiceAccountAuth {
    client_email: String,
    private_key_id: String,
    private_key: ServiceAccountKey,
    token_uri: String,
}

impl ServiceAccountAuth {
    fn from_credentials(credentials: ServiceAccountCredentials) -> Result<Self> {
        Ok(Self {
            client_email: credentials.client_email,
            private_key_id: credentials.private_key_id,
            private_key: ServiceAccountKey::from_pem(credentials.private_key.as_bytes())?,
            token_uri: credentials
                .token_uri
                .unwrap_or_else(|| DEFAULT_GOOGLE_TOKEN_URI.to_string()),
        })
    }

    fn build_jwt_assertion(&self) -> Result<String> {
        let now = current_unix_epoch_seconds()?;
        let claims = ServiceAccountClaims {
            iss: &self.client_email,
            scope: GOOGLE_CLOUD_PLATFORM_SCOPE,
            aud: &self.token_uri,
            iat: now,
            exp: now + 3600,
        };

        let header = JwtHeader {
            alg: "RS256",
            typ: Some("JWT"),
            kid: Some(&self.private_key_id),
        };

        let encoded_header = encode_json_to_base64(&header)?;
        let encoded_claims = encode_json_to_base64(&claims)?;
        let message = format!("{}.{}", encoded_header, encoded_claims);
        let signature = self.private_key.sign(&message)?;

        Ok(format!("{}.{}", message, signature))
    }

    async fn fetch_access_token(&self, http_client: &Client) -> Result<String> {
        let assertion = self.build_jwt_assertion()?;
        let response = http_client
            .post(&self.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to exchange GCP service account JWT at '{}': {}",
                        self.token_uri, err
                    ),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "GCP service account token request to '{}' failed with status {}: {}",
                    self.token_uri, status, body
                ),
            }
            .into());
        }

        let token_response: OAuthAccessTokenResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to parse GCP service account token response: {}",
                    err
                ),
            })
        })?;

        Ok(token_response.access_token)
    }
}

#[derive(Debug)]
enum GcpAuthSource {
    ServiceAccount(Box<ServiceAccountAuth>),
    AuthorizedUser(AuthorizedUserCredentials),
    MetadataServer,
}

impl GcpAuthSource {
    fn name(&self) -> &'static str {
        match self {
            Self::ServiceAccount(_) => "service_account",
            Self::AuthorizedUser(_) => "authorized_user",
            Self::MetadataServer => "metadata_server",
        }
    }

    fn load() -> Result<Self> {
        let adc_path = env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
        if let Some(credentials) = ApplicationDefaultCredentials::read(adc_path.as_deref())? {
            return match credentials {
                ApplicationDefaultCredentials::ServiceAccount(credentials) => {
                    Ok(Self::ServiceAccount(Box::new(
                        ServiceAccountAuth::from_credentials(credentials)?,
                    )))
                }
                ApplicationDefaultCredentials::AuthorizedUser(credentials) => {
                    Ok(Self::AuthorizedUser(credentials))
                }
            };
        }

        Ok(Self::MetadataServer)
    }

    async fn fetch_access_token(&self, http_client: &Client) -> Result<String> {
        match self {
            Self::ServiceAccount(credentials) => credentials.fetch_access_token(http_client).await,
            Self::AuthorizedUser(credentials) => credentials.fetch_access_token(http_client).await,
            Self::MetadataServer => fetch_metadata_access_token(http_client).await,
        }
    }
}

/// GCP credential vendor that provides downscoped OAuth2 tokens.
pub struct GcpCredentialVendor {
    config: GcpCredentialVendorConfig,
    http_client: Client,
    auth_source: GcpAuthSource,
}

impl std::fmt::Debug for GcpCredentialVendor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpCredentialVendor")
            .field("config", &self.config)
            .field("auth_source", &self.auth_source.name())
            .finish()
    }
}

impl GcpCredentialVendor {
    /// Create a new GCP credential vendor with the specified configuration.
    pub fn new(config: GcpCredentialVendorConfig) -> Result<Self> {
        Ok(Self {
            config,
            http_client: Client::new(),
            auth_source: GcpAuthSource::load()?,
        })
    }

    /// Parse a GCS URI to extract bucket and prefix.
    fn parse_gcs_uri(uri: &str) -> Result<(String, String)> {
        let url = uri_to_url(uri)?;

        if url.scheme() != "gs" {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Unsupported GCS URI scheme '{}', expected 'gs'",
                    url.scheme()
                ),
            }
            .into());
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("GCS URI '{}' missing bucket", uri),
                })
            })?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        Ok((bucket, prefix))
    }

    /// Get a source token for downscoping.
    async fn get_source_token(&self) -> Result<String> {
        let base_token = self
            .auth_source
            .fetch_access_token(&self.http_client)
            .await?;

        if let Some(ref service_account) = self.config.service_account {
            return self
                .impersonate_service_account(&base_token, service_account)
                .await;
        }

        Ok(base_token)
    }

    /// Impersonate a service account using the IAM Credentials API.
    async fn impersonate_service_account(
        &self,
        base_token: &str,
        service_account: &str,
    ) -> Result<String> {
        let url = format!(
            "{}/{}:generateAccessToken",
            IAM_GENERATE_ACCESS_TOKEN_URL, service_account
        );

        let body = serde_json::json!({
            "scope": [GOOGLE_CLOUD_PLATFORM_SCOPE]
        });

        let response = self
            .http_client
            .post(&url)
            .bearer_auth(base_token)
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to call IAM generateAccessToken: {}", err),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(NamespaceError::Internal {
                message: format!(
                    "IAM generateAccessToken failed for '{}' with status {}: {}",
                    service_account, status, body
                ),
            }
            .into());
        }

        let token_response: GenerateAccessTokenResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to parse generateAccessToken response: {}", err),
            })
        })?;

        Ok(token_response.access_token)
    }

    /// Build Credential Access Boundary for the specified bucket/prefix and permission.
    fn build_access_boundary(
        bucket: &str,
        prefix: &str,
        permission: VendedPermission,
    ) -> CredentialAccessBoundary {
        let bucket_resource = format!("//storage.googleapis.com/projects/_/buckets/{}", bucket);

        let mut rules = vec![];

        let condition = if prefix.is_empty() {
            None
        } else {
            let prefix_trimmed = prefix.trim_end_matches('/');
            let list_prefix_attr =
                "api.getAttribute('storage.googleapis.com/objectListPrefix', '')";
            let expr = format!(
                "resource.name.startsWith('projects/_/buckets/{}/objects/{}/') || \
                 {list_attr} == '{prefix}' || {list_attr}.startsWith('{prefix}/')",
                bucket,
                prefix_trimmed,
                list_attr = list_prefix_attr,
                prefix = prefix_trimmed
            );
            Some(AvailabilityCondition { expression: expr })
        };

        rules.push(AccessBoundaryRule {
            available_resource: bucket_resource.clone(),
            available_permissions: vec![
                "inRole:roles/storage.legacyObjectReader".to_string(),
                "inRole:roles/storage.objectViewer".to_string(),
            ],
            availability_condition: condition.clone(),
        });

        if permission.can_write() {
            rules.push(AccessBoundaryRule {
                available_resource: bucket_resource.clone(),
                available_permissions: vec![
                    "inRole:roles/storage.legacyBucketWriter".to_string(),
                    "inRole:roles/storage.objectCreator".to_string(),
                ],
                availability_condition: condition.clone(),
            });
        }

        if permission.can_delete() {
            rules.push(AccessBoundaryRule {
                available_resource: bucket_resource,
                available_permissions: vec!["inRole:roles/storage.objectAdmin".to_string()],
                availability_condition: condition,
            });
        }

        CredentialAccessBoundary {
            access_boundary: AccessBoundaryInner {
                access_boundary_rules: rules,
            },
        }
    }

    /// Exchange source token for a downscoped token using STS.
    async fn downscope_token(
        &self,
        source_token: &str,
        access_boundary: &CredentialAccessBoundary,
    ) -> Result<(String, u64)> {
        let options_json = serde_json::to_string(access_boundary).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to serialize access boundary: {}", err),
            })
        })?;

        let params = [
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", source_token),
            ("options", &options_json),
        ];

        let response = self
            .http_client
            .post(STS_TOKEN_EXCHANGE_URL)
            .form(&params)
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to call STS token exchange: {}", err),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unknown error".to_string());
            return Err(NamespaceError::Internal {
                message: format!("STS token exchange failed with status {}: {}", status, body),
            }
            .into());
        }

        let token_response: TokenExchangeResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to parse STS response: {}", err),
            })
        })?;

        let expires_in_secs = token_response.expires_in.unwrap_or(3600);
        let expires_at_millis = current_unix_epoch_millis()? + expires_in_secs * 1000;

        Ok((token_response.access_token, expires_at_millis))
    }

    /// Hash an API key using SHA-256 with salt (Polaris pattern).
    /// Format: SHA256(api_key + ":" + salt) as hex string.
    pub fn hash_api_key(api_key: &str, salt: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}", api_key, salt));
        format!("{:x}", hasher.finalize())
    }

    fn normalize_workload_identity_audience(provider: &str) -> String {
        const IAM_PREFIX: &str = "//iam.googleapis.com/";
        if provider.starts_with(IAM_PREFIX) {
            provider.to_string()
        } else {
            format!("{}{}", IAM_PREFIX, provider)
        }
    }

    /// Exchange an OIDC token for GCP access token using Workload Identity Federation.
    async fn exchange_oidc_for_gcp_token(&self, oidc_token: &str) -> Result<String> {
        let workload_identity_provider = self
            .config
            .workload_identity_provider
            .as_ref()
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message:
                        "gcp_workload_identity_provider must be configured for OIDC token exchange"
                            .to_string(),
                })
            })?;

        let audience = Self::normalize_workload_identity_audience(workload_identity_provider);

        let params = [
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("subject_token", oidc_token),
            ("audience", audience.as_str()),
            ("scope", GOOGLE_CLOUD_PLATFORM_SCOPE),
        ];

        let response = self
            .http_client
            .post(STS_TOKEN_EXCHANGE_URL)
            .form(&params)
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to exchange OIDC token for GCP token: {}", err),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "GCP STS token exchange failed with status {}: {}",
                    status, body
                ),
            }
            .into());
        }

        let token_response: TokenExchangeResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to parse GCP STS token response: {}", err),
            })
        })?;

        let federated_token = token_response.access_token;

        if let Some(ref service_account) = self.config.impersonation_service_account {
            return self
                .impersonate_service_account(&federated_token, service_account)
                .await;
        }

        Ok(federated_token)
    }

    /// Vend credentials using Workload Identity Federation (for auth_token).
    async fn vend_with_web_identity(
        &self,
        bucket: &str,
        prefix: &str,
        auth_token: &str,
    ) -> Result<VendedCredentials> {
        debug!(
            "GCP vend_with_web_identity: bucket={}, prefix={}",
            bucket, prefix
        );

        let gcp_token = self.exchange_oidc_for_gcp_token(auth_token).await?;

        let access_boundary = Self::build_access_boundary(bucket, prefix, self.config.permission);
        let (downscoped_token, expires_at_millis) =
            self.downscope_token(&gcp_token, &access_boundary).await?;

        let mut storage_options = HashMap::new();
        storage_options.insert("google_storage_token".to_string(), downscoped_token.clone());
        storage_options.insert(
            "expires_at_millis".to_string(),
            expires_at_millis.to_string(),
        );

        info!(
            "GCP credentials vended (web identity): bucket={}, prefix={}, permission={}, expires_at={}, token={}",
            bucket,
            prefix,
            self.config.permission,
            expires_at_millis,
            redact_credential(&downscoped_token)
        );

        Ok(VendedCredentials::new(storage_options, expires_at_millis))
    }

    /// Vend credentials using API key validation.
    async fn vend_with_api_key(
        &self,
        bucket: &str,
        prefix: &str,
        api_key: &str,
    ) -> Result<VendedCredentials> {
        let salt = self.config.api_key_salt.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "api_key_salt must be configured to use API key authentication"
                    .to_string(),
            })
        })?;

        let key_hash = Self::hash_api_key(api_key, salt);

        let permission = self
            .config
            .api_key_hash_permissions
            .get(&key_hash)
            .copied()
            .ok_or_else(|| {
                warn!(
                    "Invalid API key: hash {} not found in permissions map",
                    &key_hash[..8]
                );
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: "Invalid API key".to_string(),
                })
            })?;

        debug!(
            "GCP vend_with_api_key: bucket={}, prefix={}, permission={}",
            bucket, prefix, permission
        );

        let source_token = self.get_source_token().await?;
        let access_boundary = Self::build_access_boundary(bucket, prefix, permission);
        let (downscoped_token, expires_at_millis) = self
            .downscope_token(&source_token, &access_boundary)
            .await?;

        let mut storage_options = HashMap::new();
        storage_options.insert("google_storage_token".to_string(), downscoped_token.clone());
        storage_options.insert(
            "expires_at_millis".to_string(),
            expires_at_millis.to_string(),
        );

        info!(
            "GCP credentials vended (api_key): bucket={}, prefix={}, permission={}, expires_at={}, token={}",
            bucket,
            prefix,
            permission,
            expires_at_millis,
            redact_credential(&downscoped_token)
        );

        Ok(VendedCredentials::new(storage_options, expires_at_millis))
    }
}

#[async_trait]
impl CredentialVendor for GcpCredentialVendor {
    async fn vend_credentials(
        &self,
        table_location: &str,
        identity: Option<&Identity>,
    ) -> Result<VendedCredentials> {
        debug!(
            "GCP credential vending: location={}, permission={}, identity={:?}",
            table_location,
            self.config.permission,
            identity.map(|i| format!(
                "api_key={}, auth_token={}",
                i.api_key.is_some(),
                i.auth_token.is_some()
            ))
        );

        let (bucket, prefix) = Self::parse_gcs_uri(table_location)?;

        match identity {
            Some(id) if id.auth_token.is_some() => {
                let auth_token = id.auth_token.as_ref().unwrap();
                self.vend_with_web_identity(&bucket, &prefix, auth_token)
                    .await
            }
            Some(id) if id.api_key.is_some() => {
                let api_key = id.api_key.as_ref().unwrap();
                self.vend_with_api_key(&bucket, &prefix, api_key).await
            }
            Some(_) => Err(NamespaceError::InvalidInput {
                message: "Identity provided but neither auth_token nor api_key is set".to_string(),
            }
            .into()),
            None => {
                let source_token = self.get_source_token().await?;
                let access_boundary =
                    Self::build_access_boundary(&bucket, &prefix, self.config.permission);
                let (downscoped_token, expires_at_millis) = self
                    .downscope_token(&source_token, &access_boundary)
                    .await?;

                let mut storage_options = HashMap::new();
                storage_options
                    .insert("google_storage_token".to_string(), downscoped_token.clone());
                storage_options.insert(
                    "expires_at_millis".to_string(),
                    expires_at_millis.to_string(),
                );

                info!(
                    "GCP credentials vended (static): bucket={}, prefix={}, permission={}, expires_at={}, token={}",
                    bucket,
                    prefix,
                    self.config.permission,
                    expires_at_millis,
                    redact_credential(&downscoped_token)
                );

                Ok(VendedCredentials::new(storage_options, expires_at_millis))
            }
        }
    }

    fn provider_name(&self) -> &'static str {
        "gcp"
    }

    fn permission(&self) -> VendedPermission {
        self.config.permission
    }
}

fn read_credentials_file<T>(path: impl AsRef<Path>) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let path = path.as_ref().to_owned();
    let file = File::open(&path).map_err(|err| {
        lance_core::Error::from(NamespaceError::Internal {
            message: format!(
                "Failed to open GCP credentials file '{}': {}",
                path.display(),
                err
            ),
        })
    })?;

    serde_json::from_reader(BufReader::new(file)).map_err(|err| {
        lance_core::Error::from(NamespaceError::Internal {
            message: format!(
                "Failed to deserialize GCP credentials file '{}': {}",
                path.display(),
                err
            ),
        })
    })
}

fn encode_json_to_base64<T: Serialize>(value: &T) -> Result<String> {
    let json = serde_json::to_vec(value).map_err(|err| {
        lance_core::Error::from(NamespaceError::Internal {
            message: format!("Failed to serialize GCP JWT payload: {}", err),
        })
    })?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn current_unix_epoch_seconds() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("System clock is before Unix epoch: {}", err),
            })
        })?
        .as_secs())
}

fn current_unix_epoch_millis() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("System clock is before Unix epoch: {}", err),
            })
        })?
        .as_millis() as u64)
}

async fn fetch_metadata_access_token(http_client: &Client) -> Result<String> {
    let metadata_host = env::var("GCE_METADATA_HOST")
        .or_else(|_| env::var("GCE_METADATA_ROOT"))
        .unwrap_or_else(|_| DEFAULT_METADATA_HOST.to_string());
    let metadata_ip =
        env::var("GCE_METADATA_IP").unwrap_or_else(|_| DEFAULT_METADATA_IP.to_string());

    let host_error = match request_metadata_access_token(http_client, &metadata_host).await {
        Ok(token) => return Ok(token),
        Err(err) => err,
    };

    request_metadata_access_token(http_client, &metadata_ip)
        .await
        .map_err(|fallback_err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to fetch GCP metadata token from '{}' ({}) and fallback '{}' ({})",
                    metadata_host, host_error, metadata_ip, fallback_err
                ),
            })
        })
}

async fn request_metadata_access_token(http_client: &Client, hostname: &str) -> Result<String> {
    let url = format!(
        "http://{}/computeMetadata/v1/instance/service-accounts/default/token",
        hostname
    );
    let response = http_client
        .get(&url)
        .header("Metadata-Flavor", "Google")
        .timeout(METADATA_REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to call GCP metadata server '{}': {}", url, err),
            })
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(NamespaceError::Internal {
            message: format!(
                "GCP metadata server request to '{}' failed with status {}: {}",
                url, status, body
            ),
        }
        .into());
    }

    let token_response: MetadataAccessTokenResponse = response.json().await.map_err(|err| {
        lance_core::Error::from(NamespaceError::Internal {
            message: format!("Failed to parse GCP metadata token response: {}", err),
        })
    })?;

    Ok(token_response.access_token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_RSA_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC/QbkYm6bSbdGm
IEm5QbgQ52izdzcEpNq2cs0jIXpQ4rklGXThDrScB9krmcZy8prCcBtwQLp+zDfr
z7iktQ6ZRHitgfRcUqMpd7S9wX30rPpSm6UGlg1+kl0+b90sixZqTI1T+OAfbeNF
/aXO2gdNdKn2vPMOIelCQFEInHzozWYmx91jpwVtAvSENKDvOImicG7mtiazrGFc
tbLuKhKGNdnE7RKF4qZRaZ0EX5lgbPDyajkJ/mOd7KI632k1+GbT16eWhegR2Crw
D8JCNO7w5olk146/mM3EM2gts5jPpbiwEe6rElOSwEDayn9hNPmLStDmHEmmJFZc
5o4hhnB5AgMBAAECggEADSGCh0V8fRsMF0dFOIJiFEsG/bdUIC3/VCJqohxUzQPb
6UenpiH/1WyWhO9QWCj+5hWTVLAk/bqgpkCDMU+6+lvgmyz+bW5BBIJS9uo3bxqH
Ly+/c0XPFF8RJs3AViQQfGjYFSlTneTKA06oWRzP/onhd26+kzxRyvomdhxkWQlO
tU270fDKUoKi6UO+JPZ+jLRsPYBj3NKAC64avlHLmP8VpUMClS8T76RbiOJ8pVBc
xeJSXCzu2pZrsZr3PJo8kGNQNzI4j9ryrZd0P5FkQz313cIerpR6CL0L8p3pFyrm
+i01yu4yS1vy0tydN+EwdXmH8MMp+Ku4/b45Jnf1/wKBgQDeILnFrC+n2GHBdnGB
Rc93jYzIck7FBy7FMEQFIuDWlsAGAzCnUALdVApf1WL+BDoAj1dzR+vawoMr6ToF
pZvbs0mqbba+2uNl01JgIjWl8ze7Z/a22JqlCaIoO2W4dkO/ocdLjxbsM1p/2V5O
3MtLopE8yYblnsFDDbqyVcPSkwKBgQDca99RV3MGzFxjnpSIHBMofKrtxvOM9X66
HmHlEnWz01Cx6bIvvnElQuGatJqaZ34B3kZG959ghGdQBue1kxDHLEgzdyEMBely
KCPGfKSh2omPgSHK6zj0LH7VZqJwddSqVjmSARmEW4sRGKr4BUkk+z6wki4VjSL5
SBqnYnDcQwKBgQCc3auT53d4Jx1SDJ03198d5L7JR8BM8DedVeqTXgA+SxOsq1AO
uDhtqU3yQ7W3AbEceB4f8Wikgr0zo28wUbXxv3mEfBqUSexRGp2P+li8qzhuhor6
sZj0eAsmMlwxmoNZr5wYxiJACDwfEZjCRLbk4ReEQCWdvzFocyenjV3PNQKBgQCP
zGQlSdrF7Z68cuFNppstB5/vfaK4LBRf0aBl9FQLW+nCF8bidOiVuXs7FWXjI29G
Qr8wXy1/pwFLaSXTBD2m4pG72ZUapeS1T9B/FiPFX6/sif8Exc4jJcAc8lc47PYv
pg7q3ILMIXipT6GCKticIri0MrmT376YSFzzJDqixwKBgBgEX7SFpXe24DFfxRk7
d1BFAgrF9sVOuALJ4Af40YJ/jGs8e6JEXpSJD+CzUIi5x5Tjb42mHBNsGtgJDGaS
OdQGUgjzRG0WEXA7DLt8T3TyhC8ZQD/uMpSPjVKVjA5cOwYM82X7tqCMC3Yy7CxP
4PgiEjqQXha6B8smzs0z0OVO
-----END PRIVATE KEY-----"#;

    #[test]
    fn test_parse_gcs_uri() {
        let (bucket, prefix) = GcpCredentialVendor::parse_gcs_uri("gs://my-bucket/path/to/table")
            .expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/table");

        let (bucket, prefix) =
            GcpCredentialVendor::parse_gcs_uri("gs://my-bucket/").expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");

        let (bucket, prefix) =
            GcpCredentialVendor::parse_gcs_uri("gs://my-bucket").expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_gcs_uri_invalid() {
        let result = GcpCredentialVendor::parse_gcs_uri("s3://bucket/path");
        assert!(result.is_err());

        let result = GcpCredentialVendor::parse_gcs_uri("gs:///path");
        assert!(result.is_err());

        let result = GcpCredentialVendor::parse_gcs_uri("not-a-uri");
        assert!(result.is_err());

        let result = GcpCredentialVendor::parse_gcs_uri("");
        assert!(result.is_err());
    }

    #[test]
    fn test_config_builder() {
        let config = GcpCredentialVendorConfig::new()
            .with_service_account("my-sa@project.iam.gserviceaccount.com")
            .with_permission(VendedPermission::Write);

        assert_eq!(
            config.service_account,
            Some("my-sa@project.iam.gserviceaccount.com".to_string())
        );
        assert_eq!(config.permission, VendedPermission::Write);
    }

    #[test]
    fn test_build_access_boundary_read() {
        let boundary = GcpCredentialVendor::build_access_boundary(
            "my-bucket",
            "path/to/data",
            VendedPermission::Read,
        );

        let rules = &boundary.access_boundary.access_boundary_rules;
        assert_eq!(rules.len(), 1, "Read should have 1 rule");

        let permissions = &rules[0].available_permissions;
        assert!(permissions.contains(&"inRole:roles/storage.legacyObjectReader".to_string()));
        assert!(permissions.contains(&"inRole:roles/storage.objectViewer".to_string()));
        assert!(rules[0].availability_condition.is_some());
    }

    #[test]
    fn test_build_access_boundary_write() {
        let boundary = GcpCredentialVendor::build_access_boundary(
            "my-bucket",
            "path/to/data",
            VendedPermission::Write,
        );

        let rules = &boundary.access_boundary.access_boundary_rules;
        assert_eq!(rules.len(), 2, "Write should have 2 rules");

        let permissions: Vec<_> = rules
            .iter()
            .flat_map(|rule| rule.available_permissions.iter())
            .collect();
        assert!(permissions.contains(&&"inRole:roles/storage.legacyObjectReader".to_string()));
        assert!(permissions.contains(&&"inRole:roles/storage.objectViewer".to_string()));
        assert!(permissions.contains(&&"inRole:roles/storage.legacyBucketWriter".to_string()));
        assert!(permissions.contains(&&"inRole:roles/storage.objectCreator".to_string()));
    }

    #[test]
    fn test_build_access_boundary_admin() {
        let boundary = GcpCredentialVendor::build_access_boundary(
            "my-bucket",
            "path/to/data",
            VendedPermission::Admin,
        );

        let rules = &boundary.access_boundary.access_boundary_rules;
        assert_eq!(rules.len(), 3, "Admin should have 3 rules");
        assert_eq!(
            rules[2].available_permissions,
            vec!["inRole:roles/storage.objectAdmin".to_string()]
        );
    }

    #[test]
    fn test_build_access_boundary_no_prefix() {
        let boundary =
            GcpCredentialVendor::build_access_boundary("my-bucket", "", VendedPermission::Read);
        assert!(
            boundary.access_boundary.access_boundary_rules[0]
                .availability_condition
                .is_none()
        );
    }

    #[test]
    fn test_normalize_workload_identity_audience() {
        let short =
            "projects/123456/locations/global/workloadIdentityPools/my-pool/providers/my-provider";
        let normalized = GcpCredentialVendor::normalize_workload_identity_audience(short);
        assert_eq!(
            normalized,
            "//iam.googleapis.com/projects/123456/locations/global/workloadIdentityPools/my-pool/providers/my-provider"
        );

        let full = "//iam.googleapis.com/projects/123456/locations/global/workloadIdentityPools/my-pool/providers/my-provider";
        let normalized = GcpCredentialVendor::normalize_workload_identity_audience(full);
        assert_eq!(normalized, full);
    }

    #[test]
    fn test_application_default_credentials_read_from_explicit_path() {
        let temp_dir = tempdir().unwrap();
        let credentials_path = temp_dir.path().join("application_default_credentials.json");
        std::fs::write(
            &credentials_path,
            r#"{
                "type": "authorized_user",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "refresh_token": "refresh-token"
            }"#,
        )
        .unwrap();

        let credentials =
            ApplicationDefaultCredentials::read(Some(credentials_path.to_str().unwrap())).unwrap();
        assert!(matches!(
            credentials,
            Some(ApplicationDefaultCredentials::AuthorizedUser(_))
        ));
    }

    #[test]
    fn test_service_account_build_jwt_assertion() {
        let auth = ServiceAccountAuth::from_credentials(ServiceAccountCredentials {
            client_email: "svc@example.iam.gserviceaccount.com".to_string(),
            private_key: TEST_RSA_PRIVATE_KEY.to_string(),
            private_key_id: "key-id-123".to_string(),
            token_uri: Some("https://oauth2.googleapis.com/token".to_string()),
        })
        .unwrap();

        let assertion = auth.build_jwt_assertion().unwrap();
        let parts: Vec<&str> = assertion.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = String::from_utf8(URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert!(header.contains("\"alg\":\"RS256\""));
        assert!(header.contains("\"kid\":\"key-id-123\""));

        let claims = String::from_utf8(URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        assert!(claims.contains("\"iss\":\"svc@example.iam.gserviceaccount.com\""));
        assert!(claims.contains(&format!("\"aud\":\"{}\"", DEFAULT_GOOGLE_TOKEN_URI)));
        assert!(claims.contains("\"scope\":\"https://www.googleapis.com/auth/cloud-platform\""));
    }

    #[tokio::test]
    async fn test_authorized_user_fetch_access_token_uses_reqwest_form_flow() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("client_id=client-id"))
            .and(body_string_contains("client_secret=client-secret"))
            .and(body_string_contains("refresh_token=refresh-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ya29.authorized-user-token",
                "expires_in": 3600
            })))
            .mount(&server)
            .await;

        let credentials = AuthorizedUserCredentials {
            client_id: "client-id".to_string(),
            client_secret: "client-secret".to_string(),
            refresh_token: "refresh-token".to_string(),
            token_uri: Some(format!("{}/token", server.uri())),
        };

        let token = credentials
            .fetch_access_token(&Client::new())
            .await
            .unwrap();
        assert_eq!(token, "ya29.authorized-user-token");
    }

    #[tokio::test]
    async fn test_service_account_fetch_access_token_uses_jwt_bearer_flow() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer",
            ))
            .and(body_string_contains("assertion="))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ya29.service-account-token",
                "expires_in": 3600
            })))
            .mount(&server)
            .await;

        let auth = ServiceAccountAuth::from_credentials(ServiceAccountCredentials {
            client_email: "svc@example.iam.gserviceaccount.com".to_string(),
            private_key: TEST_RSA_PRIVATE_KEY.to_string(),
            private_key_id: "key-id-123".to_string(),
            token_uri: Some(format!("{}/token", server.uri())),
        })
        .unwrap();

        let token = auth.fetch_access_token(&Client::new()).await.unwrap();
        assert_eq!(token, "ya29.service-account-token");
    }
}
