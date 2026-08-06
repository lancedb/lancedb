// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Azure credential vending using SAS tokens.
//!
//! This module provides credential vending for Azure Blob Storage by generating
//! SAS (Shared Access Signature) tokens with user delegation keys.

use std::collections::HashMap;
use std::env;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use lance_core::Result;
use lance_io::object_store::uri_to_url;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::Identity;
use log::{debug, info, warn};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::{Url, form_urlencoded};

use super::{
    CredentialVendor, DEFAULT_CREDENTIAL_DURATION_MILLIS, VendedCredentials, VendedPermission,
    redact_credential,
};

const AZURE_STORAGE_SCOPE: &str = "https://storage.azure.com/.default";
const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com";
const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";
const DEFAULT_MANAGED_IDENTITY_ENDPOINT: &str =
    "http://169.254.169.254/metadata/identity/oauth2/token";
const MANAGED_IDENTITY_API_VERSION: &str = "2019-08-01";
const SAS_VERSION: &str = "2022-11-02";
const MAX_SAS_DURATION_DAYS: i64 = 7;
const USER_DELEGATION_SKEW_BUFFER_SECONDS: i64 = 60;

/// Configuration for Azure credential vending.
#[derive(Debug, Clone)]
pub struct AzureCredentialVendorConfig {
    /// Optional tenant ID for authentication.
    pub tenant_id: Option<String>,

    /// Storage account name. Required for credential vending.
    pub account_name: Option<String>,

    /// Duration for vended credentials in milliseconds.
    /// Default: 3600000 (1 hour). Azure allows up to 7 days for SAS tokens.
    pub duration_millis: u64,

    /// Permission level for vended credentials.
    /// Default: Read (full read access)
    /// Used to generate SAS permissions for all credential flows.
    pub permission: VendedPermission,

    /// Client ID of the Azure AD App Registration for Workload Identity Federation.
    /// Required when using auth_token identity for OIDC token exchange.
    pub federated_client_id: Option<String>,

    /// Salt for API key hashing.
    /// Required when using API key authentication.
    /// API keys are hashed as: SHA256(api_key + ":" + salt)
    pub api_key_salt: Option<String>,

    /// Map of SHA256(api_key + ":" + salt) -> permission level.
    /// When an API key is provided, its hash is looked up in this map.
    /// If found, the mapped permission is used instead of the default permission.
    pub api_key_hash_permissions: HashMap<String, VendedPermission>,
}

impl Default for AzureCredentialVendorConfig {
    fn default() -> Self {
        Self {
            tenant_id: None,
            account_name: None,
            duration_millis: DEFAULT_CREDENTIAL_DURATION_MILLIS,
            permission: VendedPermission::default(),
            federated_client_id: None,
            api_key_salt: None,
            api_key_hash_permissions: HashMap::new(),
        }
    }
}

impl AzureCredentialVendorConfig {
    /// Create a new default config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the tenant ID.
    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Set the storage account name.
    pub fn with_account_name(mut self, account_name: impl Into<String>) -> Self {
        self.account_name = Some(account_name.into());
        self
    }

    /// Set the credential duration in milliseconds.
    pub fn with_duration_millis(mut self, millis: u64) -> Self {
        self.duration_millis = millis;
        self
    }

    /// Set the permission level for vended credentials.
    pub fn with_permission(mut self, permission: VendedPermission) -> Self {
        self.permission = permission;
        self
    }

    /// Set the federated client ID for Workload Identity Federation.
    pub fn with_federated_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.federated_client_id = Some(client_id.into());
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

#[derive(Debug, Deserialize)]
struct AzureTokenResponse {
    access_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AzureSasPermissions {
    read: bool,
    add: bool,
    create: bool,
    write: bool,
    delete: bool,
    list: bool,
}

impl std::fmt::Display for AzureSasPermissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.read {
            write!(f, "r")?;
        }
        if self.add {
            write!(f, "a")?;
        }
        if self.create {
            write!(f, "c")?;
        }
        if self.write {
            write!(f, "w")?;
        }
        if self.delete {
            write!(f, "d")?;
        }
        if self.list {
            write!(f, "l")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AzureSignedResource {
    Container,
    Directory,
}

impl std::fmt::Display for AzureSignedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Container => write!(f, "c"),
            Self::Directory => write!(f, "d"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AzureUserDelegationKey {
    signed_oid: String,
    signed_tid: String,
    signed_start: String,
    signed_expiry: String,
    signed_service: String,
    signed_version: String,
    value: String,
}

/// Azure credential vendor that generates SAS tokens.
#[derive(Debug)]
pub struct AzureCredentialVendor {
    config: AzureCredentialVendorConfig,
    http_client: reqwest::Client,
}

impl AzureCredentialVendor {
    /// Create a new Azure credential vendor with the specified configuration.
    pub fn new(config: AzureCredentialVendorConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Hash an API key using SHA-256 with salt (Polaris pattern).
    /// Format: SHA256(api_key + ":" + salt) as hex string.
    pub fn hash_api_key(api_key: &str, salt: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}", api_key, salt));
        format!("{:x}", hasher.finalize())
    }

    fn build_sas_permissions(permission: VendedPermission) -> AzureSasPermissions {
        AzureSasPermissions {
            read: true,
            add: permission.can_write(),
            create: permission.can_write(),
            write: permission.can_write(),
            delete: permission.can_delete(),
            list: true,
        }
    }

    fn authority_host() -> String {
        env::var("AZURE_AUTHORITY_HOST").unwrap_or_else(|_| DEFAULT_AUTHORITY_HOST.to_string())
    }

    fn tenant_id_for_static_auth(&self) -> Option<String> {
        self.config
            .tenant_id
            .clone()
            .or_else(|| env::var("AZURE_TENANT_ID").ok())
    }

    /// Resolve the federated client ID from config or `AZURE_CLIENT_ID` env var.
    /// Note: `AZURE_CLIENT_ID` is also used in the client-secret flow. The precedence
    /// in `fetch_static_access_token` ensures the federated token file flow is checked
    /// first, so when both `AZURE_CLIENT_SECRET` and `AZURE_FEDERATED_TOKEN_FILE` are
    /// set, the federated flow wins.
    fn federated_client_id_for_static_auth(&self) -> Option<String> {
        self.config
            .federated_client_id
            .clone()
            .or_else(|| env::var("AZURE_CLIENT_ID").ok())
    }

    fn effective_expiry(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let requested = now
            .checked_add_signed(Duration::milliseconds(self.config.duration_millis as i64))
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!(
                        "azure_duration_millis value '{}' overflows expiration time calculation",
                        self.config.duration_millis
                    ),
                })
            })?;
        let max_expiry = now
            .checked_add_signed(Duration::days(MAX_SAS_DURATION_DAYS))
            .and_then(|value| {
                value.checked_sub_signed(Duration::seconds(USER_DELEGATION_SKEW_BUFFER_SECONDS))
            })
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: "Failed to calculate maximum Azure SAS expiration time".to_string(),
                })
            })?;
        Ok(requested.min(max_expiry))
    }

    fn format_azure_time(value: DateTime<Utc>) -> String {
        value.to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    /// Resolve a static Azure access token using the following precedence
    /// (matches object_store's credential resolution order):
    /// 1. Workload Identity Federation (AZURE_FEDERATED_TOKEN_FILE + tenant + client ID)
    /// 2. Client Secret OAuth (AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + tenant)
    /// 3. Managed Identity (IMDS)
    async fn fetch_static_access_token(&self) -> Result<String> {
        if let (Some(tenant_id), Some(client_id), Ok(federated_token_file)) = (
            self.tenant_id_for_static_auth(),
            self.federated_client_id_for_static_auth(),
            env::var("AZURE_FEDERATED_TOKEN_FILE"),
        ) {
            debug!(
                "Azure static auth: using federated token file flow with '{}'",
                federated_token_file
            );
            let federated_token =
                std::fs::read_to_string(&federated_token_file).map_err(|err| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Failed to read Azure federated token file '{}': {}",
                            federated_token_file, err
                        ),
                    })
                })?;

            return self
                .exchange_federated_token_for_azure_token(
                    &tenant_id,
                    &client_id,
                    federated_token.trim(),
                )
                .await;
        }

        if let (Some(tenant_id), Ok(client_id), Ok(client_secret)) = (
            self.tenant_id_for_static_auth(),
            env::var("AZURE_CLIENT_ID"),
            env::var("AZURE_CLIENT_SECRET"),
        ) {
            debug!("Azure static auth: using client secret flow");
            return self
                .exchange_client_secret_for_azure_token(&tenant_id, &client_id, &client_secret)
                .await;
        }

        debug!("Azure static auth: falling back to managed identity flow");
        self.fetch_managed_identity_token().await
    }

    async fn exchange_client_secret_for_azure_token(
        &self,
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<String> {
        let params = [
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", AZURE_STORAGE_SCOPE),
            ("grant_type", "client_credentials"),
        ];
        self.exchange_for_azure_token(tenant_id, &params).await
    }

    async fn exchange_federated_token_for_azure_token(
        &self,
        tenant_id: &str,
        client_id: &str,
        federated_token: &str,
    ) -> Result<String> {
        let params = [
            ("client_id", client_id),
            (
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            ),
            ("client_assertion", federated_token),
            ("scope", AZURE_STORAGE_SCOPE),
            ("grant_type", "client_credentials"),
        ];
        self.exchange_for_azure_token(tenant_id, &params).await
    }

    async fn exchange_for_azure_token(
        &self,
        tenant_id: &str,
        params: &[(&str, &str)],
    ) -> Result<String> {
        let token_url = format!(
            "{}/{}/oauth2/v2.0/token",
            Self::authority_host().trim_end_matches('/'),
            tenant_id
        );

        let response = self
            .http_client
            .post(&token_url)
            .form(params)
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to fetch Azure AD token from '{}': {}",
                        token_url, err
                    ),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "Azure AD token request to '{}' failed with status {}: {}",
                    token_url, status, body
                ),
            }
            .into());
        }

        let token_response: AzureTokenResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to parse Azure AD token response: {}", err),
            })
        })?;

        Ok(token_response.access_token)
    }

    async fn fetch_managed_identity_token(&self) -> Result<String> {
        let endpoint = env::var("IDENTITY_ENDPOINT")
            .or_else(|_| env::var("MSI_ENDPOINT"))
            .unwrap_or_else(|_| DEFAULT_MANAGED_IDENTITY_ENDPOINT.to_string());

        let client_id = self.federated_client_id_for_static_auth();

        let mut query = vec![
            ("api-version", MANAGED_IDENTITY_API_VERSION.to_string()),
            ("resource", AZURE_STORAGE_RESOURCE.to_string()),
        ];
        if let Some(client_id) = client_id {
            query.push(("client_id", client_id));
        }

        let mut request = self
            .http_client
            .get(&endpoint)
            .header("metadata", "true")
            .query(&query);

        if let Ok(identity_header) = env::var("IDENTITY_HEADER") {
            request = request.header("x-identity-header", identity_header);
        }

        let response = request.send().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to fetch Azure managed identity token from '{}': {}",
                    endpoint, err
                ),
            })
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "Azure managed identity token request to '{}' failed with status {}: {}",
                    endpoint, status, body
                ),
            }
            .into());
        }

        let token_response: AzureTokenResponse = response.json().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to parse Azure managed identity token response: {}",
                    err
                ),
            })
        })?;

        Ok(token_response.access_token)
    }

    async fn get_user_delegation_key(
        &self,
        access_token: &str,
        account: &str,
    ) -> Result<(AzureUserDelegationKey, DateTime<Utc>)> {
        let now = Utc::now();
        let expiry = self.effective_expiry(now)?;
        let start = Self::format_azure_time(now);
        let expiry_string = Self::format_azure_time(expiry);
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<KeyInfo>\n    <Start>{}</Start>\n    <Expiry>{}</Expiry>\n</KeyInfo>",
            start, expiry_string
        );

        let endpoint = format!("https://{}.blob.core.windows.net/", account);
        let response = self
            .http_client
            .post(&endpoint)
            .query(&[("restype", "service"), ("comp", "userdelegationkey")])
            .header(AUTHORIZATION, format!("Bearer {}", access_token))
            .header("x-ms-version", SAS_VERSION)
            .header(CONTENT_TYPE, "application/xml")
            .body(body)
            .send()
            .await
            .map_err(|err| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to request Azure user delegation key for account '{}': {}",
                        account, err
                    ),
                })
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(NamespaceError::Internal {
                message: format!(
                    "Azure user delegation key request for account '{}' failed with status {}: {}",
                    account, status, body
                ),
            }
            .into());
        }

        let body = response.text().await.map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to read Azure user delegation key response for account '{}': {}",
                    account, err
                ),
            })
        })?;

        let delegation_key = Self::parse_user_delegation_key_xml(&body)?;
        Ok((delegation_key, expiry))
    }

    fn parse_user_delegation_key_xml(xml: &str) -> Result<AzureUserDelegationKey> {
        let mut reader = Reader::from_str(xml);
        let mut fields: HashMap<String, String> = HashMap::new();
        let mut current_tag: Option<String> = None;

        loop {
            match reader.read_event() {
                Ok(Event::Start(e)) => {
                    current_tag =
                        Some(String::from_utf8_lossy(e.local_name().as_ref()).to_string());
                }
                Ok(Event::Text(e)) => {
                    if let Some(ref tag) = current_tag {
                        let text = String::from_utf8_lossy(&e).trim().to_string();
                        fields.insert(tag.clone(), text);
                    }
                }
                Ok(Event::End(_)) => {
                    current_tag = None;
                }
                Ok(Event::Eof) => break,
                Err(err) => {
                    return Err(NamespaceError::Internal {
                        message: format!(
                            "Failed to parse Azure user delegation key XML response: {}",
                            err
                        ),
                    }
                    .into());
                }
                _ => {}
            }
        }

        let mut get_field = |name: &str| -> Result<String> {
            fields.remove(name).ok_or_else(|| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Azure user delegation key response missing '{}'", name),
                })
            })
        };

        Ok(AzureUserDelegationKey {
            signed_oid: get_field("SignedOid")?,
            signed_tid: get_field("SignedTid")?,
            signed_start: get_field("SignedStart")?,
            signed_expiry: get_field("SignedExpiry")?,
            signed_service: get_field("SignedService")?,
            signed_version: get_field("SignedVersion")?,
            value: get_field("Value")?,
        })
    }

    fn base64_hmac_sha256(secret: &str, message: &str) -> Result<String> {
        let key = STANDARD.decode(secret).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to decode Azure user delegation key: {}", err),
            })
        })?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|err| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to create HMAC key for Azure SAS signing: {}", err),
            })
        })?;
        mac.update(message.as_bytes());

        Ok(STANDARD.encode(mac.finalize().into_bytes()))
    }

    fn build_sas_query(
        delegation_key: &AzureUserDelegationKey,
        canonicalized_resource: &str,
        permissions: &AzureSasPermissions,
        expiry: &str,
        resource: AzureSignedResource,
        signed_directory_depth: Option<usize>,
    ) -> Result<String> {
        // User delegation SAS string-to-sign fields (version 2020-12-06+):
        // https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas
        let sp = permissions.to_string();
        let se = expiry.to_string();
        let sv = SAS_VERSION.to_string();
        let sr = resource.to_string();
        let string_to_sign = [
            sp.as_str(), // sp  (signed permissions)
            "",          // st  (signed start — empty = immediate)
            se.as_str(), // se  (signed expiry)
            canonicalized_resource,
            &delegation_key.signed_oid,     // skoid
            &delegation_key.signed_tid,     // sktid
            &delegation_key.signed_start,   // skt
            &delegation_key.signed_expiry,  // ske
            &delegation_key.signed_service, // sks
            &delegation_key.signed_version, // skv
            "",                             // saoid (signed authorized OID)
            "",                             // suoid (signed unauthorized OID)
            "",                             // scid  (signed correlation ID)
            "",                             // sip   (signed IP)
            "",                             // spr   (signed protocol)
            sv.as_str(),                    // sv    (signed version)
            sr.as_str(),                    // sr    (signed resource)
            "",                             // snapshot time
            "",                             // ses   (signed encryption scope)
            "",                             // rscc  (response cache-control)
            "",                             // rscd  (response content-disposition)
            "",                             // rsce  (response content-encoding)
            "",                             // rscl  (response content-language)
            "",                             // rsct  (response content-type)
        ]
        .join("\n");

        let signature = Self::base64_hmac_sha256(&delegation_key.value, &string_to_sign)?;

        let mut serializer = form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("skoid", &delegation_key.signed_oid);
        serializer.append_pair("sktid", &delegation_key.signed_tid);
        serializer.append_pair("skt", &delegation_key.signed_start);
        serializer.append_pair("ske", &delegation_key.signed_expiry);
        serializer.append_pair("sks", &delegation_key.signed_service);
        serializer.append_pair("skv", &delegation_key.signed_version);
        serializer.append_pair("sv", SAS_VERSION);
        serializer.append_pair("sp", &permissions.to_string());
        serializer.append_pair("sr", &resource.to_string());
        serializer.append_pair("se", expiry);
        if let Some(depth) = signed_directory_depth {
            serializer.append_pair("sdd", &depth.to_string());
        }
        serializer.append_pair("sig", &signature);
        Ok(serializer.finish())
    }

    fn parse_container_and_path(url: &Url, table_location: &str) -> Result<(String, String)> {
        let container = if !url.username().is_empty() {
            url.username().to_string()
        } else {
            url.host_str()
                .ok_or_else(|| {
                    lance_core::Error::from(NamespaceError::InvalidInput {
                        message: format!("Azure URI '{}' missing container", table_location),
                    })
                })?
                .to_string()
        };

        let path = url
            .path()
            .trim_start_matches('/')
            .trim_end_matches('/')
            .to_string();
        Ok((container, path))
    }

    async fn generate_sas_with_azure_token(
        &self,
        azure_token: &str,
        account: &str,
        container: &str,
        path: &str,
        permission: VendedPermission,
    ) -> Result<(String, u64)> {
        let (delegation_key, expiry_time) =
            self.get_user_delegation_key(azure_token, account).await?;
        let expires_at_millis = expiry_time.timestamp_millis() as u64;
        let expiry = Self::format_azure_time(expiry_time);
        let permissions = Self::build_sas_permissions(permission);
        let normalized_path = path.trim_matches('/');

        let (canonicalized_resource, resource, signed_directory_depth) =
            if normalized_path.is_empty() {
                (
                    format!("/blob/{}/{}", account, container),
                    AzureSignedResource::Container,
                    None,
                )
            } else {
                (
                    format!("/blob/{}/{}/{}", account, container, normalized_path),
                    AzureSignedResource::Directory,
                    Some(normalized_path.split('/').count()),
                )
            };

        let sas_token = Self::build_sas_query(
            &delegation_key,
            &canonicalized_resource,
            &permissions,
            &expiry,
            resource,
            signed_directory_depth,
        )?;

        Ok((sas_token, expires_at_millis))
    }

    /// Fetch a static access token and generate a SAS token.
    async fn generate_sas_with_static_token(
        &self,
        account: &str,
        container: &str,
        path: &str,
        permission: VendedPermission,
    ) -> Result<(String, u64)> {
        let access_token = self.fetch_static_access_token().await?;
        self.generate_sas_with_azure_token(&access_token, account, container, path, permission)
            .await
    }

    /// Exchange an OIDC token for Azure AD access token using Workload Identity Federation.
    async fn exchange_oidc_for_azure_token(&self, oidc_token: &str) -> Result<String> {
        let tenant_id = self.config.tenant_id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "azure_tenant_id must be configured for OIDC token exchange".to_string(),
            })
        })?;

        let client_id = self.config.federated_client_id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "azure_federated_client_id must be configured for OIDC token exchange"
                    .to_string(),
            })
        })?;

        self.exchange_federated_token_for_azure_token(tenant_id, client_id, oidc_token)
            .await
    }

    /// Vend credentials using Workload Identity Federation (for auth_token).
    async fn vend_with_web_identity(
        &self,
        account: &str,
        container: &str,
        path: &str,
        auth_token: &str,
    ) -> Result<VendedCredentials> {
        debug!(
            "Azure vend_with_web_identity: account={}, container={}, path={}",
            account, container, path
        );

        let azure_token = self.exchange_oidc_for_azure_token(auth_token).await?;
        let (sas_token, expires_at_millis) = self
            .generate_sas_with_azure_token(
                &azure_token,
                account,
                container,
                path,
                self.config.permission,
            )
            .await?;

        let mut storage_options = HashMap::new();
        storage_options.insert("azure_storage_sas_token".to_string(), sas_token.clone());
        storage_options.insert(
            "azure_storage_account_name".to_string(),
            account.to_string(),
        );
        storage_options.insert(
            "expires_at_millis".to_string(),
            expires_at_millis.to_string(),
        );

        info!(
            "Azure credentials vended (web identity): account={}, container={}, path={}, permission={}, expires_at={}, sas_token={}",
            account,
            container,
            path,
            self.config.permission,
            expires_at_millis,
            redact_credential(&sas_token)
        );

        Ok(VendedCredentials::new(storage_options, expires_at_millis))
    }

    /// Vend credentials using API key validation.
    async fn vend_with_api_key(
        &self,
        account: &str,
        container: &str,
        path: &str,
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
            "Azure vend_with_api_key: account={}, container={}, path={}, permission={}",
            account, container, path, permission
        );

        let (sas_token, expires_at_millis) = self
            .generate_sas_with_static_token(account, container, path, permission)
            .await?;

        let mut storage_options = HashMap::new();
        storage_options.insert("azure_storage_sas_token".to_string(), sas_token.clone());
        storage_options.insert(
            "azure_storage_account_name".to_string(),
            account.to_string(),
        );
        storage_options.insert(
            "expires_at_millis".to_string(),
            expires_at_millis.to_string(),
        );

        info!(
            "Azure credentials vended (api_key): account={}, container={}, path={}, permission={}, expires_at={}, sas_token={}",
            account,
            container,
            path,
            permission,
            expires_at_millis,
            redact_credential(&sas_token)
        );

        Ok(VendedCredentials::new(storage_options, expires_at_millis))
    }
}

#[async_trait]
impl CredentialVendor for AzureCredentialVendor {
    async fn vend_credentials(
        &self,
        table_location: &str,
        identity: Option<&Identity>,
    ) -> Result<VendedCredentials> {
        debug!(
            "Azure credential vending: location={}, permission={}, identity={:?}",
            table_location,
            self.config.permission,
            identity.map(|i| format!(
                "api_key={}, auth_token={}",
                i.api_key.is_some(),
                i.auth_token.is_some()
            ))
        );

        let url = uri_to_url(table_location)?;
        let (container, path) = Self::parse_container_and_path(&url, table_location)?;

        let account = self.config.account_name.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Azure credential vending requires 'credential_vendor.azure_account_name' to be set in configuration".to_string(),
            })
        })?;

        match identity {
            Some(id) if id.auth_token.is_some() => {
                self.vend_with_web_identity(
                    account,
                    &container,
                    &path,
                    id.auth_token.as_ref().unwrap(),
                )
                .await
            }
            Some(id) if id.api_key.is_some() => {
                self.vend_with_api_key(account, &container, &path, id.api_key.as_ref().unwrap())
                    .await
            }
            Some(_) => Err(NamespaceError::InvalidInput {
                message: "Identity provided but neither auth_token nor api_key is set".to_string(),
            }
            .into()),
            None => {
                let (sas_token, expires_at_millis) = self
                    .generate_sas_with_static_token(
                        account,
                        &container,
                        &path,
                        self.config.permission,
                    )
                    .await?;

                let mut storage_options = HashMap::new();
                storage_options.insert("azure_storage_sas_token".to_string(), sas_token.clone());
                storage_options.insert("azure_storage_account_name".to_string(), account.clone());
                storage_options.insert(
                    "expires_at_millis".to_string(),
                    expires_at_millis.to_string(),
                );

                info!(
                    "Azure credentials vended (static): account={}, container={}, path={}, permission={}, expires_at={}, sas_token={}",
                    account,
                    container,
                    path,
                    self.config.permission,
                    expires_at_millis,
                    redact_credential(&sas_token)
                );

                Ok(VendedCredentials::new(storage_options, expires_at_millis))
            }
        }
    }

    fn provider_name(&self) -> &'static str {
        "azure"
    }

    fn permission(&self) -> VendedPermission {
        self.config.permission
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = AzureCredentialVendorConfig::new()
            .with_tenant_id("my-tenant-id")
            .with_account_name("myaccount")
            .with_duration_millis(7200000);

        assert_eq!(config.tenant_id, Some("my-tenant-id".to_string()));
        assert_eq!(config.account_name, Some("myaccount".to_string()));
        assert_eq!(config.duration_millis, 7200000);
    }

    #[test]
    fn test_build_sas_permissions_read() {
        let permissions = AzureCredentialVendor::build_sas_permissions(VendedPermission::Read);

        assert!(permissions.read, "Read permission should have read=true");
        assert!(permissions.list, "Read permission should have list=true");
        assert!(
            !permissions.write,
            "Read permission should have write=false"
        );
        assert!(!permissions.add, "Read permission should have add=false");
        assert!(
            !permissions.create,
            "Read permission should have create=false"
        );
        assert!(
            !permissions.delete,
            "Read permission should have delete=false"
        );
        assert_eq!(permissions.to_string(), "rl");
    }

    #[test]
    fn test_build_sas_permissions_write() {
        let permissions = AzureCredentialVendor::build_sas_permissions(VendedPermission::Write);

        assert!(permissions.read, "Write permission should have read=true");
        assert!(permissions.list, "Write permission should have list=true");
        assert!(permissions.write, "Write permission should have write=true");
        assert!(permissions.add, "Write permission should have add=true");
        assert!(
            permissions.create,
            "Write permission should have create=true"
        );
        assert!(
            !permissions.delete,
            "Write permission should have delete=false"
        );
        assert_eq!(permissions.to_string(), "racwl");
    }

    #[test]
    fn test_build_sas_permissions_admin() {
        let permissions = AzureCredentialVendor::build_sas_permissions(VendedPermission::Admin);

        assert!(permissions.read, "Admin permission should have read=true");
        assert!(permissions.list, "Admin permission should have list=true");
        assert!(permissions.write, "Admin permission should have write=true");
        assert!(permissions.add, "Admin permission should have add=true");
        assert!(
            permissions.create,
            "Admin permission should have create=true"
        );
        assert!(
            permissions.delete,
            "Admin permission should have delete=true"
        );
        assert_eq!(permissions.to_string(), "racwdl");
    }

    #[test]
    fn test_parse_user_delegation_key_xml() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<UserDelegationKey>
    <SignedOid>11111111-1111-1111-1111-111111111111</SignedOid>
    <SignedTid>22222222-2222-2222-2222-222222222222</SignedTid>
    <SignedStart>2024-01-01T00:00:00Z</SignedStart>
    <SignedExpiry>2024-01-02T00:00:00Z</SignedExpiry>
    <SignedService>b</SignedService>
    <SignedVersion>2022-11-02</SignedVersion>
    <Value>YmFzZTY0LXNlY3JldA==</Value>
</UserDelegationKey>"#;

        let key = AzureCredentialVendor::parse_user_delegation_key_xml(xml).unwrap();
        assert_eq!(key.signed_oid, "11111111-1111-1111-1111-111111111111");
        assert_eq!(key.signed_tid, "22222222-2222-2222-2222-222222222222");
        assert_eq!(key.signed_start, "2024-01-01T00:00:00Z");
        assert_eq!(key.signed_expiry, "2024-01-02T00:00:00Z");
        assert_eq!(key.signed_service, "b");
        assert_eq!(key.signed_version, "2022-11-02");
        assert_eq!(key.value, "YmFzZTY0LXNlY3JldA==");
    }

    #[test]
    fn test_build_directory_sas_query() {
        let key = AzureUserDelegationKey {
            signed_oid: "11111111-1111-1111-1111-111111111111".to_string(),
            signed_tid: "22222222-2222-2222-2222-222222222222".to_string(),
            signed_start: "2024-01-01T00:00:00Z".to_string(),
            signed_expiry: "2024-01-08T00:00:00Z".to_string(),
            signed_service: "b".to_string(),
            signed_version: "2022-11-02".to_string(),
            value: "c2VjcmV0LWtleS1mb3ItdGVzdHM=".to_string(),
        };

        let token = AzureCredentialVendor::build_sas_query(
            &key,
            "/blob/account/container/path/to/table",
            &AzureCredentialVendor::build_sas_permissions(VendedPermission::Read),
            "2024-01-08T00:00:00Z",
            AzureSignedResource::Directory,
            Some(3),
        )
        .unwrap();

        assert_eq!(
            token,
            "skoid=11111111-1111-1111-1111-111111111111&sktid=22222222-2222-2222-2222-222222222222&skt=2024-01-01T00%3A00%3A00Z&ske=2024-01-08T00%3A00%3A00Z&sks=b&skv=2022-11-02&sv=2022-11-02&sp=rl&sr=d&se=2024-01-08T00%3A00%3A00Z&sdd=3&sig=EjSMl8%2FGXSZ7qPkykdtXiog9DtWdqnec%2Bzrh%2FkU70v0%3D"
        );
    }

    #[test]
    fn test_parse_container_and_path_from_userinfo_uri() {
        let url = Url::parse("az://container@account.blob.core.windows.net/path/to/table").unwrap();
        let (container, path) =
            AzureCredentialVendor::parse_container_and_path(&url, "unused").unwrap();
        assert_eq!(container, "container");
        assert_eq!(path, "path/to/table");
    }

    mod integration_tests {
        use super::*;

        fn integration_test_enabled() -> bool {
            matches!(
                env::var("AZURE_CREDENTIALS_VENDING_INTEG_TEST_ENABLED").as_deref(),
                Ok("1")
            )
        }

        fn maybe_account_name() -> Result<Option<String>> {
            if !integration_test_enabled() {
                return Ok(None);
            }

            let account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|err| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!(
                        "AZURE_STORAGE_ACCOUNT_NAME must be set for Azure credentials vending integration tests: {}",
                        err
                    ),
                })
            })?;

            Ok(Some(account_name))
        }

        const TEST_CONTAINER: &str = "lance-integ-test";
        const TEST_PREFIX: &str = "cv-test";

        #[tokio::test]
        async fn test_fetch_static_access_token() -> Result<()> {
            let Some(account_name) = maybe_account_name()? else {
                return Ok(());
            };

            let vendor = AzureCredentialVendor::new(
                AzureCredentialVendorConfig::new().with_account_name(account_name),
            );

            let token = vendor.fetch_static_access_token().await?;
            assert!(!token.is_empty(), "Azure access token should not be empty");
            assert!(
                token.len() > 100,
                "Azure access token should look like a JWT"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_vend_directory_scoped_sas() -> Result<()> {
            let Some(account_name) = maybe_account_name()? else {
                return Ok(());
            };

            let vendor = AzureCredentialVendor::new(
                AzureCredentialVendorConfig::new()
                    .with_account_name(account_name.clone())
                    .with_duration_millis(5 * 60 * 1000),
            );

            let location = format!("az://{}/{}", TEST_CONTAINER, TEST_PREFIX);
            let credentials = vendor.vend_credentials(&location, None).await?;

            assert_eq!(
                credentials
                    .storage_options
                    .get("azure_storage_account_name"),
                Some(&account_name)
            );

            let sas_token = credentials
                .storage_options
                .get("azure_storage_sas_token")
                .expect("Azure SAS token should be present");
            assert!(sas_token.contains("sig="));
            assert!(sas_token.contains("sr=d"), "Expected directory-scoped SAS");
            assert!(credentials.expires_at_millis > 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_vend_container_scoped_sas() -> Result<()> {
            let Some(account_name) = maybe_account_name()? else {
                return Ok(());
            };

            let vendor = AzureCredentialVendor::new(
                AzureCredentialVendorConfig::new()
                    .with_account_name(account_name.clone())
                    .with_duration_millis(5 * 60 * 1000),
            );

            let location = format!("az://{}", TEST_CONTAINER);
            let credentials = vendor.vend_credentials(&location, None).await?;

            let sas_token = credentials
                .storage_options
                .get("azure_storage_sas_token")
                .expect("Azure SAS token should be present");
            assert!(sas_token.contains("sr=c"), "Expected container-scoped SAS");

            Ok(())
        }

        #[tokio::test]
        async fn test_vended_sas_can_write_read_delete() -> Result<()> {
            let Some(account_name) = maybe_account_name()? else {
                return Ok(());
            };

            let vendor = AzureCredentialVendor::new(
                AzureCredentialVendorConfig::new()
                    .with_account_name(account_name.clone())
                    .with_permission(VendedPermission::Admin)
                    .with_duration_millis(5 * 60 * 1000),
            );

            let location = format!("az://{}/{}", TEST_CONTAINER, TEST_PREFIX);
            let credentials = vendor.vend_credentials(&location, None).await?;

            let sas_token = credentials
                .storage_options
                .get("azure_storage_sas_token")
                .unwrap();

            let blob_url = format!(
                "https://{}.blob.core.windows.net/{}/{}/e2e-test.txt?{}",
                account_name, TEST_CONTAINER, TEST_PREFIX, sas_token
            );

            let client = reqwest::Client::new();
            let test_content = format!("lance-integ-{}", chrono::Utc::now().timestamp());

            // Write
            let put_resp = client
                .put(&blob_url)
                .header("x-ms-blob-type", "BlockBlob")
                .header("x-ms-version", SAS_VERSION)
                .body(test_content.clone())
                .send()
                .await
                .unwrap();
            assert!(
                put_resp.status().is_success(),
                "PUT failed: {}",
                put_resp.text().await.unwrap_or_default()
            );

            // Read
            let get_resp = client
                .get(&blob_url)
                .header("x-ms-version", SAS_VERSION)
                .send()
                .await
                .unwrap();
            assert!(get_resp.status().is_success(), "GET failed");
            assert_eq!(get_resp.text().await.unwrap(), test_content);

            // Delete
            let del_resp = client
                .delete(&blob_url)
                .header("x-ms-version", SAS_VERSION)
                .send()
                .await
                .unwrap();
            assert!(del_resp.status().is_success(), "DELETE failed");

            Ok(())
        }

        #[tokio::test]
        async fn test_read_only_sas_cannot_write() -> Result<()> {
            let Some(account_name) = maybe_account_name()? else {
                return Ok(());
            };

            let vendor = AzureCredentialVendor::new(
                AzureCredentialVendorConfig::new()
                    .with_account_name(account_name.clone())
                    .with_permission(VendedPermission::Read)
                    .with_duration_millis(5 * 60 * 1000),
            );

            let location = format!("az://{}/{}", TEST_CONTAINER, TEST_PREFIX);
            let credentials = vendor.vend_credentials(&location, None).await?;

            let sas_token = credentials
                .storage_options
                .get("azure_storage_sas_token")
                .unwrap();

            let blob_url = format!(
                "https://{}.blob.core.windows.net/{}/{}/should-not-exist.txt?{}",
                account_name, TEST_CONTAINER, TEST_PREFIX, sas_token
            );

            let client = reqwest::Client::new();
            let put_resp = client
                .put(&blob_url)
                .header("x-ms-blob-type", "BlockBlob")
                .header("x-ms-version", SAS_VERSION)
                .body("should fail")
                .send()
                .await
                .map_err(|err| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!("Failed to send PUT request: {}", err),
                    })
                })?;

            assert!(
                put_resp.status().is_client_error(),
                "Read-only SAS should not allow writes, got status {}",
                put_resp.status()
            );

            Ok(())
        }
    }
}
