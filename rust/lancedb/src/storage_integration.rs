// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Database-owned cloud-storage access configurations managed by the remote service.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A Secret object, optionally qualified by its owning database and namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default)]
    pub namespace_path: Vec<String>,
    pub name: String,
}
impl SecretReference {
    /// Reference a Secret in the integration's database.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            database: None,
            namespace_path: vec![],
            name: name.into(),
        }
    }
    /// Qualify the database that owns the Secret.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }
    /// Qualify the namespace that owns the Secret.
    pub fn with_namespace(mut self, namespace: Vec<String>) -> Self {
        self.namespace_path = namespace;
        self
    }
}

/// Configuration for registering a cloud storage integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CreateStorageIntegrationRequest {
    pub storage_provider: StorageProvider,
    pub storage_prefix: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_role_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_access_key_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_secret_access_key_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_storage_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_tenant_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_secret_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcp_credentials_json_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub huggingface_api_key_secret: Option<SecretReference>,
}

/// Cloud storage providers supported by storage integrations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageProvider {
    #[serde(rename = "S3")]
    S3,
    #[serde(rename = "AZURE")]
    Azure,
    #[serde(rename = "GCP")]
    Gcp,
    #[serde(rename = "HUGGINGFACE")]
    HuggingFace,
}

impl CreateStorageIntegrationRequest {
    /// Register a prefix and the customer-owned IAM role authorized to access it.
    pub fn s3(storage_prefix: impl Into<String>, aws_role_arn: impl Into<String>) -> Self {
        Self {
            storage_provider: StorageProvider::S3,
            storage_prefix: storage_prefix.into(),
            aws_role_arn: Some(aws_role_arn.into()),
            aws_access_key_id_secret: None,
            aws_secret_access_key_secret: None,
            aws_region: None,
            azure_storage_account: None,
            azure_tenant_id_secret: None,
            azure_client_id_secret: None,
            azure_client_secret_secret: None,
            gcp_credentials_json_secret: None,
            huggingface_api_key_secret: None,
        }
    }

    /// Register static S3 credentials by Secret reference, without accepting key values.
    pub fn s3_with_secrets(
        storage_prefix: impl Into<String>,
        access_key: SecretReference,
        secret_key: SecretReference,
    ) -> Self {
        Self {
            storage_provider: StorageProvider::S3,
            storage_prefix: storage_prefix.into(),
            aws_role_arn: None,
            aws_access_key_id_secret: Some(access_key),
            aws_secret_access_key_secret: Some(secret_key),
            aws_region: None,
            azure_storage_account: None,
            azure_tenant_id_secret: None,
            azure_client_id_secret: None,
            azure_client_secret_secret: None,
            gcp_credentials_json_secret: None,
            huggingface_api_key_secret: None,
        }
    }

    /// Register Azure Blob access using three Secret references and an explicit account.
    ///
    /// ```
    /// use lancedb::storage_integration::{CreateStorageIntegrationRequest, SecretReference};
    /// let config = CreateStorageIntegrationRequest::azure(
    ///     "az://customer-data/raw/", "customeraccount",
    ///     SecretReference::new("tenant"), SecretReference::new("client"),
    ///     SecretReference::new("credential"),
    /// );
    /// ```
    pub fn azure(
        storage_prefix: impl Into<String>,
        storage_account: impl Into<String>,
        tenant_id: SecretReference,
        client_id: SecretReference,
        client_secret: SecretReference,
    ) -> Self {
        Self {
            storage_provider: StorageProvider::Azure,
            storage_prefix: storage_prefix.into(),
            azure_storage_account: Some(storage_account.into()),
            azure_tenant_id_secret: Some(tenant_id),
            azure_client_id_secret: Some(client_id),
            azure_client_secret_secret: Some(client_secret),
            gcp_credentials_json_secret: None,
            huggingface_api_key_secret: None,
            aws_role_arn: None,
            aws_access_key_id_secret: None,
            aws_secret_access_key_secret: None,
            aws_region: None,
        }
    }

    /// Register Google Cloud Storage access using a Secret containing credentials JSON.
    ///
    /// ```
    /// use lancedb::storage_integration::{CreateStorageIntegrationRequest, SecretReference};
    /// let config = CreateStorageIntegrationRequest::gcp(
    ///     "gs://customer-data/raw/", SecretReference::new("gcp_credentials"),
    /// );
    /// ```
    pub fn gcp(storage_prefix: impl Into<String>, credentials_json: SecretReference) -> Self {
        Self {
            storage_provider: StorageProvider::Gcp,
            storage_prefix: storage_prefix.into(),
            gcp_credentials_json_secret: Some(credentials_json),
            huggingface_api_key_secret: None,
            aws_role_arn: None,
            aws_access_key_id_secret: None,
            aws_secret_access_key_secret: None,
            aws_region: None,
            azure_storage_account: None,
            azure_tenant_id_secret: None,
            azure_client_id_secret: None,
            azure_client_secret_secret: None,
        }
    }

    /// Register Hugging Face repository access using a Secret containing an API key.
    ///
    /// ```
    /// use lancedb::storage_integration::{CreateStorageIntegrationRequest, SecretReference};
    /// let config = CreateStorageIntegrationRequest::huggingface(
    ///     "hf://datasets/acme/private/data/", SecretReference::new("hf_api_key"),
    /// );
    /// ```
    pub fn huggingface(storage_prefix: impl Into<String>, api_key: SecretReference) -> Self {
        Self {
            storage_provider: StorageProvider::HuggingFace,
            storage_prefix: storage_prefix.into(),
            huggingface_api_key_secret: Some(api_key),
            gcp_credentials_json_secret: None,
            aws_role_arn: None,
            aws_access_key_id_secret: None,
            aws_secret_access_key_secret: None,
            aws_region: None,
            azure_storage_account: None,
            azure_tenant_id_secret: None,
            azure_client_id_secret: None,
            azure_client_secret_secret: None,
        }
    }

    /// Set the AWS region of the storage prefix.
    pub fn with_aws_region(mut self, region: impl Into<String>) -> Self {
        self.aws_region = Some(region.into());
        self
    }
}

/// Persisted configuration and cloud setup instructions.
///
/// Registration does not verify AWS access or bind the integration to a storage
/// operation. The policies and external ID are configuration metadata, never
/// AWS access credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StorageIntegrationInfo {
    pub name: String,
    pub database: String,
    pub namespace_path: Vec<String>,
    pub storage_provider: StorageProvider,
    pub storage_prefix: String,
    #[serde(default)]
    pub aws_role_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_access_key_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_secret_access_key_secret: Option<SecretReference>,
    pub aws_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_storage_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_tenant_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_id_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_client_secret_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcp_credentials_json_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub huggingface_api_key_secret: Option<SecretReference>,
    pub aws_account_id: String,
    pub aws_iam_principal_arn: String,
    pub aws_external_id: String,
    pub created_at_millis: i64,
    pub access_verified: bool,
    pub trust_policy: Value,
    pub permissions_policy: Value,
    pub configuration_instructions: String,
}
