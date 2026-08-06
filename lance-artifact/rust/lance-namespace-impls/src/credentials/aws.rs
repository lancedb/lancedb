// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! AWS credential vending using STS AssumeRole.
//!
//! This module provides credential vending for AWS S3 storage by assuming
//! an IAM role using AWS STS (Security Token Service).

use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_sts::Client as StsClient;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use lance_core::Result;
use lance_io::object_store::uri_to_url;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::Identity;
use log::{debug, info, warn};
use sha2::{Digest, Sha256};

use super::{
    CredentialVendor, DEFAULT_CREDENTIAL_DURATION_MILLIS, VendedCredentials, VendedPermission,
    redact_credential,
};

/// Render an error together with its full `source()` chain.
fn full_error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Configuration for AWS credential vending.
#[derive(Debug, Clone)]
pub struct AwsCredentialVendorConfig {
    /// The IAM role ARN to assume.
    /// Used for both AssumeRole (static/api_key) and AssumeRoleWithWebIdentity (auth_token).
    pub role_arn: String,

    /// Optional external ID for the assume role request.
    pub external_id: Option<String>,

    /// Duration for vended credentials in milliseconds.
    /// Default: 3600000 (1 hour).
    /// AWS STS allows 900-43200 seconds (15 min - 12 hours).
    /// Values outside this range will be clamped.
    pub duration_millis: u64,

    /// Optional role session name. Defaults to "lance-credential-vending".
    pub role_session_name: Option<String>,

    /// Optional AWS region for the STS client.
    pub region: Option<String>,

    /// Permission level for vended credentials.
    /// Default: Read (full read access)
    /// Used to generate scoped IAM policy for all credential flows.
    pub permission: VendedPermission,

    /// Salt for API key hashing.
    /// Required when using API key authentication.
    /// API keys are hashed as: SHA256(api_key + ":" + salt)
    pub api_key_salt: Option<String>,

    /// Map of SHA256(api_key + ":" + salt) -> permission level.
    /// When an API key is provided, its hash is looked up in this map.
    /// If found, the mapped permission is used instead of the default permission.
    pub api_key_hash_permissions: HashMap<String, VendedPermission>,

    /// Optional path to the pod's projected service-account OIDC token file (typically the
    /// EKS-injected `AWS_WEB_IDENTITY_TOKEN_FILE`).
    /// This is the recommended method when running in Kubernetes.
    /// When set, the scoped assume is performed with `AssumeRoleWithWebIdentity`
    /// using this token instead of a role-chained `AssumeRole` from the process's
    /// ambient credentials. A web-identity assumption is *not* role chaining, so
    /// STS honors the role's `MaxSessionDuration` (up to 12h) and the returned
    /// expiration is accurate. When `None`, the `AssumeRole` path
    /// is used and `duration_millis` is subject to the source role duration and may be invalid
    pub pod_web_identity_token_file: Option<String>,
}

impl AwsCredentialVendorConfig {
    /// Create a new config with the specified role ARN.
    pub fn new(role_arn: impl Into<String>) -> Self {
        Self {
            role_arn: role_arn.into(),
            external_id: None,
            duration_millis: DEFAULT_CREDENTIAL_DURATION_MILLIS,
            role_session_name: None,
            region: None,
            permission: VendedPermission::default(),
            api_key_salt: None,
            api_key_hash_permissions: HashMap::new(),
            pod_web_identity_token_file: None,
        }
    }

    /// Set the external ID for the assume role request.
    pub fn with_external_id(mut self, external_id: impl Into<String>) -> Self {
        self.external_id = Some(external_id.into());
        self
    }

    /// Set the credential duration in milliseconds.
    pub fn with_duration_millis(mut self, millis: u64) -> Self {
        self.duration_millis = millis;
        self
    }

    /// Set the role session name.
    pub fn with_role_session_name(mut self, name: impl Into<String>) -> Self {
        self.role_session_name = Some(name.into());
        self
    }

    /// Set the AWS region for the STS client.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the pod web-identity token file, enabling direct (non-chained)
    /// `AssumeRoleWithWebIdentity` for the scoped assume. See
    /// [`AwsCredentialVendorConfig::pod_web_identity_token_file`].
    pub fn with_pod_web_identity_token_file(mut self, path: impl Into<String>) -> Self {
        self.pod_web_identity_token_file = Some(path.into());
        self
    }

    /// Set the permission level for vended credentials.
    pub fn with_permission(mut self, permission: VendedPermission) -> Self {
        self.permission = permission;
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

    /// Set the entire API key hash permissions map.
    pub fn with_api_key_hash_permissions(
        mut self,
        permissions: HashMap<String, VendedPermission>,
    ) -> Self {
        self.api_key_hash_permissions = permissions;
        self
    }
}

/// AWS credential vendor that uses STS AssumeRole.
#[derive(Debug)]
pub struct AwsCredentialVendor {
    config: AwsCredentialVendorConfig,
    sts_client: StsClient,
}

impl AwsCredentialVendor {
    /// Create a new AWS credential vendor with the specified configuration.
    pub async fn new(config: AwsCredentialVendorConfig) -> Result<Self> {
        // Load AWS config in a spawn_blocking context to avoid "Cannot drop a runtime"
        // panic. aws_config::load() may internally create a nested tokio runtime for
        // credential resolution (e.g., IMDS, SSO), which panics if dropped inside
        // an existing async context.
        let region = config.region.clone();
        let sts_client = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(region) = region {
                    aws_config_loader = aws_config_loader.region(aws_config::Region::new(region));
                }
                let aws_config = aws_config_loader.load().await;
                StsClient::new(&aws_config)
            })
        })
        .await
        .map_err(|e| lance_core::Error::io(format!("Failed to initialize AWS config: {:?}", e)))?;

        Ok(Self { config, sts_client })
    }

    /// Create a new AWS credential vendor with an existing STS client.
    pub fn with_sts_client(config: AwsCredentialVendorConfig, sts_client: StsClient) -> Self {
        Self { config, sts_client }
    }

    /// Parse an S3 URI to extract bucket and prefix.
    fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
        let url = uri_to_url(uri)?;

        let bucket = url
            .host_str()
            .ok_or_else(|| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("S3 URI '{}' missing bucket", uri),
                })
            })?
            .to_string();

        let prefix = url.path().trim_start_matches('/').to_string();

        Ok((bucket, prefix))
    }

    /// Build a scoped IAM policy for the specified location and permission level.
    ///
    /// Permission levels:
    /// - `Read`: Full read access to all content (metadata, indices, data files)
    /// - `Write`: Full read and write access (no delete)
    /// - `Admin`: Full read, write, and delete access
    fn build_policy(bucket: &str, prefix: &str, permission: VendedPermission) -> String {
        let prefix_trimmed = prefix.trim_end_matches('/');
        let base_path = if prefix.is_empty() {
            format!("arn:aws:s3:::{}/*", bucket)
        } else {
            format!("arn:aws:s3:::{}/{}/*", bucket, prefix_trimmed)
        };
        let bucket_arn = format!("arn:aws:s3:::{}", bucket);

        let mut statements = vec![];

        // List bucket permission (always needed)
        statements.push(serde_json::json!({
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": bucket_arn,
            "Condition": {
                "StringLike": {
                    "s3:prefix": if prefix.is_empty() {
                        "*".to_string()
                    } else {
                        format!("{}/*", prefix_trimmed)
                    }
                }
            }
        }));

        // Get bucket location (always needed)
        statements.push(serde_json::json!({
            "Effect": "Allow",
            "Action": "s3:GetBucketLocation",
            "Resource": bucket_arn
        }));

        // Read access (all permission levels have full read)
        statements.push(serde_json::json!({
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
            "Resource": base_path
        }));

        // Write access (Write and Admin)
        if permission.can_write() {
            statements.push(serde_json::json!({
                "Effect": "Allow",
                "Action": "s3:PutObject",
                "Resource": base_path
            }));
        }

        // Delete access (Admin only)
        if permission.can_delete() {
            statements.push(serde_json::json!({
                "Effect": "Allow",
                "Action": "s3:DeleteObject",
                "Resource": base_path
            }));
        }

        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": statements
        });

        policy.to_string()
    }

    /// Hash an API key using SHA-256 with salt (Polaris pattern).
    /// Format: SHA256(api_key + ":" + salt) as hex string.
    pub fn hash_api_key(api_key: &str, salt: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(format!("{}:{}", api_key, salt));
        format!("{:x}", hasher.finalize())
    }

    /// Extract a session name from a JWT token (best effort, no validation).
    /// Decodes the payload and extracts 'sub' or 'email' claim.
    /// Falls back to "lance-web-identity" if parsing fails.
    fn derive_session_name_from_token(token: &str) -> String {
        // JWT format: header.payload.signature
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return "lance-web-identity".to_string();
        }

        // Decode the payload (second part)
        let payload = match URL_SAFE_NO_PAD.decode(parts[1]) {
            Ok(bytes) => bytes,
            Err(_) => {
                // Try standard base64 as fallback
                match base64::engine::general_purpose::STANDARD_NO_PAD.decode(parts[1]) {
                    Ok(bytes) => bytes,
                    Err(_) => return "lance-web-identity".to_string(),
                }
            }
        };

        // Parse as JSON and extract 'sub' or 'email'
        let json: serde_json::Value = match serde_json::from_slice(&payload) {
            Ok(v) => v,
            Err(_) => return "lance-web-identity".to_string(),
        };

        let subject = json
            .get("sub")
            .or_else(|| json.get("email"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Sanitize for role session name (alphanumeric, =, @, -, .)
        let sanitized: String = subject
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '=' || *c == '@' || *c == '-' || *c == '.')
            .collect();

        let session_name = format!("lance-{}", sanitized);

        // Cap to 64 chars (AWS limit)
        if session_name.len() > 64 {
            session_name[..64].to_string()
        } else {
            session_name
        }
    }

    /// Cap a session name to 64 characters (AWS limit).
    fn cap_session_name(name: &str) -> String {
        if name.len() > 64 {
            name[..64].to_string()
        } else {
            name.to_string()
        }
    }

    /// Extract credentials from an STS Credentials response.
    fn extract_credentials(
        &self,
        credentials: Option<&aws_sdk_sts::types::Credentials>,
        bucket: &str,
        prefix: &str,
        permission: VendedPermission,
    ) -> Result<VendedCredentials> {
        let credentials = credentials.ok_or_else(|| {
            lance_core::Error::from(NamespaceError::Internal {
                message: "STS response missing credentials".to_string(),
            })
        })?;

        let access_key_id = credentials.access_key_id().to_string();
        let secret_access_key = credentials.secret_access_key().to_string();
        let session_token = credentials.session_token().to_string();

        let expiration = credentials.expiration();
        let expires_at_millis =
            (expiration.secs() as u64) * 1000 + (expiration.subsec_nanos() / 1_000_000) as u64;

        info!(
            "AWS credentials vended: bucket={}, prefix={}, permission={}, expires_at={}, access_key_id={}",
            bucket,
            prefix,
            permission,
            expires_at_millis,
            redact_credential(&access_key_id)
        );

        let mut storage_options = HashMap::new();
        storage_options.insert("aws_access_key_id".to_string(), access_key_id);
        storage_options.insert("aws_secret_access_key".to_string(), secret_access_key);
        storage_options.insert("aws_session_token".to_string(), session_token);
        storage_options.insert(
            "expires_at_millis".to_string(),
            expires_at_millis.to_string(),
        );

        // Include region if configured
        if let Some(ref region) = self.config.region {
            storage_options.insert("aws_region".to_string(), region.clone());
        }

        Ok(VendedCredentials::new(storage_options, expires_at_millis))
    }

    /// Vend credentials using AssumeRoleWithWebIdentity (for auth_token).
    async fn vend_with_web_identity(
        &self,
        bucket: &str,
        prefix: &str,
        auth_token: &str,
        policy: &str,
    ) -> Result<VendedCredentials> {
        let session_name = Self::derive_session_name_from_token(auth_token);
        let duration_secs = self.config.duration_millis.div_ceil(1000).clamp(900, 43200) as i32;

        debug!(
            "AWS AssumeRoleWithWebIdentity: role={}, session={}, permission={}",
            self.config.role_arn, session_name, self.config.permission
        );

        let response = self
            .sts_client
            .assume_role_with_web_identity()
            .role_arn(&self.config.role_arn)
            .web_identity_token(auth_token)
            .role_session_name(&session_name)
            .policy(policy)
            .duration_seconds(duration_secs)
            .send()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "AssumeRoleWithWebIdentity failed for role '{}': {}",
                        self.config.role_arn,
                        full_error_chain(&e)
                    ),
                })
            })?;

        self.extract_credentials(
            response.credentials(),
            bucket,
            prefix,
            self.config.permission,
        )
    }

    /// Vend credentials using AssumeRole with API key validation.
    /// Perform the scoped assume for `(bucket, prefix, permission)`, attaching the
    /// per-table session policy.
    ///
    /// When [`AwsCredentialVendorConfig::pod_web_identity_token_file`] is set this
    /// uses `AssumeRoleWithWebIdentity` with the pod's projected SA OIDC token -- a
    /// *direct*, non-chained assumption that honors the role's `MaxSessionDuration`
    /// (so `expires_at_millis` is accurate). Otherwise it falls back to the legacy
    /// role-chained `AssumeRole` from ambient credentials (STS-capped at 1h).
    ///
    /// `external_id` is only applied on the chained `AssumeRole` path;
    /// `AssumeRoleWithWebIdentity` has no external-id parameter (the OIDC
    /// `sub`/`aud` trust condition is the binding instead).
    async fn assume_scoped(
        &self,
        bucket: &str,
        prefix: &str,
        permission: VendedPermission,
        session_name: &str,
        external_id: Option<&str>,
    ) -> Result<VendedCredentials> {
        let policy = Self::build_policy(bucket, prefix, permission);
        let duration_secs = self.config.duration_millis.div_ceil(1000).clamp(900, 43200) as i32;

        let credentials = if let Some(token_file) = &self.config.pod_web_identity_token_file {
            // DIRECT (non-chained): AssumeRoleWithWebIdentity with the pod's SA
            // OIDC token. Re-read the file every vend -- kubelet rotates it.
            let token = tokio::fs::read_to_string(token_file).await.map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "failed to read pod web identity token '{}': {}",
                        token_file, e
                    ),
                })
            })?;
            debug!(
                "AWS AssumeRoleWithWebIdentity (pod): role={}, session={}, permission={}",
                self.config.role_arn, session_name, permission
            );
            let response = self
                .sts_client
                .assume_role_with_web_identity()
                .role_arn(&self.config.role_arn)
                .web_identity_token(token.trim())
                .role_session_name(session_name)
                .policy(&policy)
                .duration_seconds(duration_secs)
                .send()
                .await
                .map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "AssumeRoleWithWebIdentity (pod) failed for role '{}': {}",
                            self.config.role_arn,
                            full_error_chain(&e)
                        ),
                    })
                })?;
            response.credentials().cloned()
        } else {
            // LEGACY chained path: AssumeRole from ambient credentials (1h cap).
            debug!(
                "AWS AssumeRole (chained): role={}, session={}, permission={}",
                self.config.role_arn, session_name, permission
            );
            let mut request = self
                .sts_client
                .assume_role()
                .role_arn(&self.config.role_arn)
                .role_session_name(session_name)
                .policy(&policy)
                .duration_seconds(duration_secs);
            if let Some(external_id) = external_id {
                request = request.external_id(external_id);
            }
            let response = request.send().await.map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "AssumeRole failed for role '{}': {}",
                        self.config.role_arn,
                        full_error_chain(&e)
                    ),
                })
            })?;
            response.credentials().cloned()
        };

        self.extract_credentials(credentials.as_ref(), bucket, prefix, permission)
    }

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

        // Look up permission from hash mapping
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

        // The api_key authorizes the client and picks the permission; the AWS
        // assume itself goes through `assume_scoped` (pod web-identity when
        // configured, else chained AssumeRole with the key hash as external_id).
        let session_name = Self::cap_session_name(&format!("lance-api-{}", &key_hash[..16]));
        self.assume_scoped(bucket, prefix, permission, &session_name, Some(&key_hash))
            .await
    }

    /// Vend credentials using the vendor's static (default) permission.
    async fn vend_with_static_config(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<VendedCredentials> {
        let role_session_name = self
            .config
            .role_session_name
            .clone()
            .unwrap_or_else(|| "lance-credential-vending".to_string());
        let role_session_name = Self::cap_session_name(&role_session_name);

        self.assume_scoped(
            bucket,
            prefix,
            self.config.permission,
            &role_session_name,
            self.config.external_id.as_deref(),
        )
        .await
    }
}

#[async_trait]
impl CredentialVendor for AwsCredentialVendor {
    async fn vend_credentials(
        &self,
        table_location: &str,
        identity: Option<&Identity>,
    ) -> Result<VendedCredentials> {
        debug!(
            "AWS credential vending: location={}, permission={}, has_identity={}",
            table_location,
            self.config.permission,
            identity.is_some()
        );

        let (bucket, prefix) = Self::parse_s3_uri(table_location)?;

        match identity {
            Some(id) if id.auth_token.is_some() => {
                // Use AssumeRoleWithWebIdentity with configured permission
                let policy = Self::build_policy(&bucket, &prefix, self.config.permission);
                self.vend_with_web_identity(
                    &bucket,
                    &prefix,
                    id.auth_token.as_ref().unwrap(),
                    &policy,
                )
                .await
            }
            Some(id) if id.api_key.is_some() => {
                // Use AssumeRole with API key validation and mapped permission
                self.vend_with_api_key(&bucket, &prefix, id.api_key.as_ref().unwrap())
                    .await
            }
            Some(_) => {
                // Identity provided but neither api_key nor auth_token set
                Err(NamespaceError::InvalidInput {
                    message: "Identity provided but neither api_key nor auth_token is set"
                        .to_string(),
                }
                .into())
            }
            None => {
                // Use the vendor's static (default) permission
                self.vend_with_static_config(&bucket, &prefix).await
            }
        }
    }

    fn provider_name(&self) -> &'static str {
        "aws"
    }

    fn permission(&self) -> VendedPermission {
        self.config.permission
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri() {
        let (bucket, prefix) = AwsCredentialVendor::parse_s3_uri("s3://my-bucket/path/to/table")
            .expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/table");

        let (bucket, prefix) =
            AwsCredentialVendor::parse_s3_uri("s3://my-bucket/").expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");

        let (bucket, prefix) =
            AwsCredentialVendor::parse_s3_uri("s3://my-bucket").expect("should parse");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_build_policy_read() {
        let policy =
            AwsCredentialVendor::build_policy("my-bucket", "path/to/table", VendedPermission::Read);
        let parsed: serde_json::Value = serde_json::from_str(&policy).expect("valid json");

        let statements = parsed["Statement"].as_array().expect("statements array");
        assert_eq!(statements.len(), 3); // ListBucket, GetBucketLocation, GetObject

        // Verify no write actions
        for stmt in statements {
            let actions = stmt["Action"].clone();
            let action_list: Vec<String> = if actions.is_array() {
                actions
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|a| a.as_str().unwrap().to_string())
                    .collect()
            } else {
                vec![actions.as_str().unwrap().to_string()]
            };
            assert!(!action_list.contains(&"s3:PutObject".to_string()));
            assert!(!action_list.contains(&"s3:DeleteObject".to_string()));
        }
    }

    #[test]
    fn test_build_policy_write() {
        let policy = AwsCredentialVendor::build_policy(
            "my-bucket",
            "path/to/table",
            VendedPermission::Write,
        );
        let parsed: serde_json::Value = serde_json::from_str(&policy).expect("valid json");

        let statements = parsed["Statement"].as_array().expect("statements array");
        // ListBucket, GetBucketLocation, GetObject, PutObject
        assert_eq!(statements.len(), 4);

        // Verify PutObject is present
        let write_stmt = statements
            .iter()
            .find(|s| {
                let action = &s["Action"];
                action.as_str() == Some("s3:PutObject")
            })
            .expect("should have PutObject statement");
        assert!(write_stmt["Effect"].as_str() == Some("Allow"));

        // Verify DeleteObject is NOT present (Write doesn't have delete)
        let delete_stmt = statements.iter().find(|s| {
            let action = &s["Action"];
            action.as_str() == Some("s3:DeleteObject")
        });
        assert!(delete_stmt.is_none(), "Write should not have DeleteObject");

        // Verify no Deny statements
        let deny_stmt = statements
            .iter()
            .find(|s| s["Effect"].as_str() == Some("Deny"));
        assert!(deny_stmt.is_none(), "Write should not have Deny statements");
    }

    #[test]
    fn test_build_policy_admin() {
        let policy = AwsCredentialVendor::build_policy(
            "my-bucket",
            "path/to/table",
            VendedPermission::Admin,
        );
        let parsed: serde_json::Value = serde_json::from_str(&policy).expect("valid json");

        let statements = parsed["Statement"].as_array().expect("statements array");
        // ListBucket, GetBucketLocation, GetObject, PutObject, DeleteObject
        assert_eq!(statements.len(), 5);

        // Verify read actions
        let read_stmt = statements
            .iter()
            .find(|s| {
                let actions = s["Action"].clone();
                if actions.is_array() {
                    actions
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|a| a.as_str().unwrap() == "s3:GetObject")
                } else {
                    false
                }
            })
            .expect("should have read statement");
        assert!(read_stmt["Effect"].as_str() == Some("Allow"));

        // Verify PutObject
        let write_stmt = statements
            .iter()
            .find(|s| s["Action"].as_str() == Some("s3:PutObject"))
            .expect("should have PutObject statement");
        assert!(write_stmt["Effect"].as_str() == Some("Allow"));

        // Verify DeleteObject (Admin only)
        let delete_stmt = statements
            .iter()
            .find(|s| s["Action"].as_str() == Some("s3:DeleteObject"))
            .expect("should have DeleteObject statement");
        assert!(delete_stmt["Effect"].as_str() == Some("Allow"));

        // Verify no Deny statements
        let deny_stmt = statements
            .iter()
            .find(|s| s["Effect"].as_str() == Some("Deny"));
        assert!(deny_stmt.is_none(), "Admin should not have Deny statements");
    }

    #[test]
    fn test_config_builder() {
        let config = AwsCredentialVendorConfig::new("arn:aws:iam::123456789012:role/MyRole")
            .with_external_id("my-external-id")
            .with_duration_millis(7200000)
            .with_role_session_name("my-session")
            .with_region("us-west-2");

        assert_eq!(config.role_arn, "arn:aws:iam::123456789012:role/MyRole");
        assert_eq!(config.external_id, Some("my-external-id".to_string()));
        assert_eq!(config.duration_millis, 7200000);
        assert_eq!(config.role_session_name, Some("my-session".to_string()));
        assert_eq!(config.region, Some("us-west-2".to_string()));
        // Defaults to the legacy chained AssumeRole path.
        assert_eq!(config.pod_web_identity_token_file, None);

        let pod_config = AwsCredentialVendorConfig::new("arn:aws:iam::123456789012:role/MyRole")
            .with_pod_web_identity_token_file("/var/run/secrets/.../token");
        assert_eq!(
            pod_config.pod_web_identity_token_file,
            Some("/var/run/secrets/.../token".to_string())
        );
    }

    #[tokio::test]
    async fn test_pod_web_identity_path_reads_token_file() {
        // When pod_web_identity_token_file is set, the scoped assume takes the
        // AssumeRoleWithWebIdentity branch and reads the token file first. Point
        // it at a missing file so we deterministically hit the read error without
        // needing a live STS -- this proves the branch selection + file read.
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new("us-east-2"))
            .build();
        let sts_client = StsClient::new(&sdk_config);
        let config = AwsCredentialVendorConfig::new("arn:aws:iam::123456789012:role/MyRole")
            .with_pod_web_identity_token_file("/nonexistent/pod/web-identity-token");
        let vendor = AwsCredentialVendor::with_sts_client(config, sts_client);

        let err = vendor
            .vend_credentials("s3://bucket/prefix", None)
            .await
            .expect_err("missing token file must fail before any STS call");
        assert!(
            err.to_string()
                .contains("failed to read pod web identity token"),
            "unexpected error: {err}"
        );
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    /// Integration tests for AWS credential vending.
    ///
    /// These tests require:
    /// - Valid AWS credentials (via environment, IAM role, or credential file)
    /// - The `LANCE_TEST_AWS_ROLE_ARN` environment variable set to a role ARN that
    ///   can be assumed by the current credentials
    /// - Access to the S3 bucket `jack-lancedb-devland-us-east-1`
    ///
    /// Run with: `cargo test --features credential-vendor-aws -- --ignored`
    #[cfg(test)]
    mod integration {
        use super::*;
        use crate::DirectoryNamespaceBuilder;
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use bytes::Bytes;
        use lance_namespace::LanceNamespace;
        use lance_namespace::models::*;
        use std::sync::Arc;

        const TEST_BUCKET: &str = "jack-lancedb-devland-us-east-1";

        /// Helper to create Arrow IPC data for testing
        fn create_test_arrow_data() -> Bytes {
            let schema = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                ],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            Bytes::from(buffer)
        }

        /// Generate a unique test path for each test run to avoid conflicts
        fn unique_test_path() -> String {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            format!("lance-test/credential-vending-{}", timestamp)
        }

        /// Get the role ARN from environment variable
        fn get_test_role_arn() -> Option<String> {
            std::env::var("LANCE_TEST_AWS_ROLE_ARN").ok()
        }

        #[tokio::test]
        #[ignore = "requires AWS credentials and LANCE_TEST_AWS_ROLE_ARN env var"]
        async fn test_aws_credential_vending_basic() {
            let role_arn = get_test_role_arn()
                .expect("LANCE_TEST_AWS_ROLE_ARN must be set for integration tests");

            let test_path = unique_test_path();
            let table_location = format!("s3://{}/{}/test_table", TEST_BUCKET, test_path);

            // Test Read permission
            let read_config = AwsCredentialVendorConfig::new(&role_arn)
                .with_duration_millis(900_000) // 15 minutes (minimum)
                .with_region("us-east-1")
                .with_permission(VendedPermission::Read);

            let read_vendor = AwsCredentialVendor::new(read_config)
                .await
                .expect("should create read vendor");

            let read_creds = read_vendor
                .vend_credentials(&table_location, None)
                .await
                .expect("should vend read credentials");

            assert!(
                read_creds.storage_options.contains_key("aws_access_key_id"),
                "should have access key id"
            );
            assert!(
                read_creds
                    .storage_options
                    .contains_key("aws_secret_access_key"),
                "should have secret access key"
            );
            assert!(
                read_creds.storage_options.contains_key("aws_session_token"),
                "should have session token"
            );
            assert!(
                !read_creds.is_expired(),
                "credentials should not be expired"
            );
            assert_eq!(
                read_vendor.permission(),
                VendedPermission::Read,
                "permission should be Read"
            );

            // Test Admin permission
            let admin_config = AwsCredentialVendorConfig::new(&role_arn)
                .with_duration_millis(900_000)
                .with_region("us-east-1")
                .with_permission(VendedPermission::Admin);

            let admin_vendor = AwsCredentialVendor::new(admin_config)
                .await
                .expect("should create admin vendor");

            let admin_creds = admin_vendor
                .vend_credentials(&table_location, None)
                .await
                .expect("should vend admin credentials");

            assert!(
                admin_creds
                    .storage_options
                    .contains_key("aws_access_key_id"),
                "should have access key id"
            );
            assert!(
                !admin_creds.is_expired(),
                "credentials should not be expired"
            );
            assert_eq!(
                admin_vendor.permission(),
                VendedPermission::Admin,
                "permission should be Admin"
            );
        }

        #[tokio::test]
        #[ignore = "requires AWS credentials and LANCE_TEST_AWS_ROLE_ARN env var"]
        async fn test_directory_namespace_with_aws_credential_vending() {
            let role_arn = get_test_role_arn()
                .expect("LANCE_TEST_AWS_ROLE_ARN must be set for integration tests");

            let test_path = unique_test_path();
            let root = format!("s3://{}/{}", TEST_BUCKET, test_path);

            // Build DirectoryNamespace with credential vending using short property names
            let namespace = DirectoryNamespaceBuilder::new(&root)
                .manifest_enabled(true)
                .credential_vendor_property("enabled", "true")
                .credential_vendor_property("aws_role_arn", &role_arn)
                .credential_vendor_property("aws_duration_millis", "900000") // 15 minutes
                .credential_vendor_property("aws_region", "us-east-1")
                .credential_vendor_property("permission", "admin")
                .build()
                .await
                .expect("should build namespace");

            // Create a child namespace
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["test_ns".to_string()]),
                ..Default::default()
            };
            namespace
                .create_namespace(create_ns_req)
                .await
                .expect("should create namespace");

            // Create a table with data
            let table_data = create_test_arrow_data();
            let create_table_req = CreateTableRequest {
                id: Some(vec!["test_ns".to_string(), "test_table".to_string()]),
                mode: Some("Create".to_string()),
                ..Default::default()
            };
            let create_response = namespace
                .create_table(create_table_req, table_data)
                .await
                .expect("should create table");

            assert!(
                create_response.location.is_some(),
                "should have location in response"
            );
            assert_eq!(create_response.version, Some(1), "should be version 1");

            // Describe the table (this should use vended credentials)
            let describe_req = DescribeTableRequest {
                id: Some(vec!["test_ns".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            let describe_response = namespace
                .describe_table(describe_req)
                .await
                .expect("should describe table");

            assert!(describe_response.location.is_some(), "should have location");
            assert!(
                describe_response.storage_options.is_some(),
                "should have storage_options with vended credentials"
            );

            let storage_options = describe_response.storage_options.unwrap();
            assert!(
                storage_options.contains_key("aws_access_key_id"),
                "should have vended aws_access_key_id"
            );
            assert!(
                storage_options.contains_key("aws_secret_access_key"),
                "should have vended aws_secret_access_key"
            );
            assert!(
                storage_options.contains_key("aws_session_token"),
                "should have vended aws_session_token"
            );
            assert!(
                storage_options.contains_key("expires_at_millis"),
                "should have expires_at_millis"
            );

            // Verify expiration is in the future
            let expires_at: u64 = storage_options
                .get("expires_at_millis")
                .unwrap()
                .parse()
                .expect("should parse expires_at_millis");
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            assert!(
                expires_at > now_millis,
                "expiration should be in the future"
            );

            // List tables to verify the table was created
            let list_req = ListTablesRequest {
                id: Some(vec!["test_ns".to_string()]),
                ..Default::default()
            };
            let list_response = namespace
                .list_tables(list_req)
                .await
                .expect("should list tables");
            assert!(
                list_response.tables.contains(&"test_table".to_string()),
                "should contain test_table"
            );

            // Clean up: drop the table
            let drop_req = DropTableRequest {
                id: Some(vec!["test_ns".to_string(), "test_table".to_string()]),
                ..Default::default()
            };
            namespace
                .drop_table(drop_req)
                .await
                .expect("should drop table");

            // Clean up: drop the namespace
            let mut drop_ns_req = DropNamespaceRequest::new();
            drop_ns_req.id = Some(vec!["test_ns".to_string()]);
            namespace
                .drop_namespace(drop_ns_req)
                .await
                .expect("should drop namespace");
        }

        #[tokio::test]
        #[ignore = "requires AWS credentials and LANCE_TEST_AWS_ROLE_ARN env var"]
        async fn test_credential_refresh_on_expiration() {
            let role_arn = get_test_role_arn()
                .expect("LANCE_TEST_AWS_ROLE_ARN must be set for integration tests");

            let test_path = unique_test_path();
            let table_location = format!("s3://{}/{}/refresh_test", TEST_BUCKET, test_path);

            // Create vendor with minimum duration and Admin permission
            let config = AwsCredentialVendorConfig::new(&role_arn)
                .with_duration_millis(900_000) // 15 minutes
                .with_region("us-east-1")
                .with_permission(VendedPermission::Admin);

            let vendor = AwsCredentialVendor::new(config)
                .await
                .expect("should create vendor");

            // Vend credentials multiple times to verify consistent behavior
            let creds1 = vendor
                .vend_credentials(&table_location, None)
                .await
                .expect("should vend credentials first time");

            let creds2 = vendor
                .vend_credentials(&table_location, None)
                .await
                .expect("should vend credentials second time");

            // Both should be valid (not expired)
            assert!(!creds1.is_expired(), "first credentials should be valid");
            assert!(!creds2.is_expired(), "second credentials should be valid");

            // Both should have access keys (they may be different due to new STS calls)
            assert!(
                creds1.storage_options.contains_key("aws_access_key_id"),
                "first creds should have access key"
            );
            assert!(
                creds2.storage_options.contains_key("aws_access_key_id"),
                "second creds should have access key"
            );
        }

        #[tokio::test]
        #[ignore = "requires AWS credentials and LANCE_TEST_AWS_ROLE_ARN env var"]
        async fn test_scoped_policy_permissions() {
            let role_arn = get_test_role_arn()
                .expect("LANCE_TEST_AWS_ROLE_ARN must be set for integration tests");

            let test_path = unique_test_path();

            // Create two different table locations
            let table1_location = format!("s3://{}/{}/table1", TEST_BUCKET, test_path);
            let table2_location = format!("s3://{}/{}/table2", TEST_BUCKET, test_path);

            let config = AwsCredentialVendorConfig::new(&role_arn)
                .with_duration_millis(900_000)
                .with_region("us-east-1")
                .with_permission(VendedPermission::Admin);

            let vendor = AwsCredentialVendor::new(config)
                .await
                .expect("should create vendor");

            // Vend credentials for table1
            let creds1 = vendor
                .vend_credentials(&table1_location, None)
                .await
                .expect("should vend credentials for table1");

            // Vend credentials for table2
            let creds2 = vendor
                .vend_credentials(&table2_location, None)
                .await
                .expect("should vend credentials for table2");

            // Both should be valid
            assert!(!creds1.is_expired(), "table1 credentials should be valid");
            assert!(!creds2.is_expired(), "table2 credentials should be valid");

            // The credentials are scoped to their respective paths via IAM policy
            // (the policy restricts access to specific S3 paths)
        }

        #[tokio::test]
        #[ignore = "requires AWS credentials and LANCE_TEST_AWS_ROLE_ARN env var"]
        async fn test_from_properties_builder() {
            let role_arn = get_test_role_arn()
                .expect("LANCE_TEST_AWS_ROLE_ARN must be set for integration tests");

            let test_path = unique_test_path();
            let root = format!("s3://{}/{}", TEST_BUCKET, test_path);

            // Build namespace using from_properties (simulating config from external source)
            // Properties use the "credential_vendor." prefix which gets stripped
            let mut properties = HashMap::new();
            properties.insert("root".to_string(), root.clone());
            properties.insert("manifest_enabled".to_string(), "true".to_string());
            properties.insert("credential_vendor.enabled".to_string(), "true".to_string());
            properties.insert(
                "credential_vendor.aws_role_arn".to_string(),
                role_arn.clone(),
            );
            properties.insert(
                "credential_vendor.aws_duration_millis".to_string(),
                "900000".to_string(),
            );
            properties.insert(
                "credential_vendor.aws_region".to_string(),
                "us-east-1".to_string(),
            );
            properties.insert(
                "credential_vendor.permission".to_string(),
                "admin".to_string(),
            );

            let namespace = DirectoryNamespaceBuilder::from_properties(properties, None)
                .expect("should parse properties")
                .build()
                .await
                .expect("should build namespace");

            // Verify namespace works
            let create_ns_req = CreateNamespaceRequest {
                id: Some(vec!["props_test".to_string()]),
                ..Default::default()
            };
            namespace
                .create_namespace(create_ns_req)
                .await
                .expect("should create namespace");

            // Clean up
            let mut drop_ns_req = DropNamespaceRequest::new();
            drop_ns_req.id = Some(vec!["props_test".to_string()]);
            namespace
                .drop_namespace(drop_ns_req)
                .await
                .expect("should drop namespace");
        }
    }
}
