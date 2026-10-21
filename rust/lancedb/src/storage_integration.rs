// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Named cloud-storage access configurations managed by the remote service.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for registering an AWS S3 storage integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CreateStorageIntegrationRequest {
    pub storage_provider: StorageProvider,
    pub storage_prefix: String,
    pub aws_role_arn: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,
}

/// Cloud storage providers supported by storage integrations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageProvider {
    #[serde(rename = "S3")]
    S3,
}

impl CreateStorageIntegrationRequest {
    /// Register a prefix and the customer-owned IAM role authorized to access it.
    pub fn s3(storage_prefix: impl Into<String>, aws_role_arn: impl Into<String>) -> Self {
        Self {
            storage_provider: StorageProvider::S3,
            storage_prefix: storage_prefix.into(),
            aws_role_arn: aws_role_arn.into(),
            aws_region: None,
        }
    }

    /// Set the AWS region of the storage prefix.
    pub fn with_aws_region(mut self, region: impl Into<String>) -> Self {
        self.aws_region = Some(region.into());
        self
    }
}

/// Persisted configuration and complete AWS setup instructions.
///
/// Registration does not verify AWS access or bind the integration to a storage
/// operation. The policies and external ID are configuration metadata, never
/// AWS access credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StorageIntegrationInfo {
    pub name: String,
    #[serde(default)]
    pub namespace_path: Vec<String>,
    pub storage_provider: StorageProvider,
    pub storage_prefix: String,
    pub aws_role_arn: String,
    pub aws_region: Option<String>,
    pub aws_account_id: String,
    pub aws_iam_principal_arn: String,
    pub aws_external_id: String,
    pub created_at_millis: i64,
    pub access_verified: bool,
    pub trust_policy: Value,
    pub permissions_policy: Value,
    pub configuration_instructions: String,
}
