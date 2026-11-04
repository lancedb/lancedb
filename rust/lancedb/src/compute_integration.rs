// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Database-owned remote compute configurations. Registration does not submit a job.

use serde::{Deserialize, Serialize};

use crate::storage_integration::SecretReference;

/// Compute backends supported by the remote service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComputeProvider {
    #[serde(rename = "KUBERNETES")]
    Kubernetes,
}

/// Register a Kubernetes API endpoint using references to credential Secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CreateComputeIntegrationRequest {
    pub compute_provider: ComputeProvider,
    pub job_submission_endpoint: String,
    pub kubernetes_namespace: String,
    pub kubernetes_bearer_token_secret: SecretReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubernetes_ca_cert_secret: Option<SecretReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubernetes_service_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kubernetes_image_pull_secret: Option<String>,
}

impl CreateComputeIntegrationRequest {
    /// Configure a Kubernetes API base URL, target namespace and bearer-token Secret.
    pub fn kubernetes(
        endpoint: impl Into<String>,
        namespace: impl Into<String>,
        bearer_token: SecretReference,
    ) -> Self {
        Self {
            compute_provider: ComputeProvider::Kubernetes,
            job_submission_endpoint: endpoint.into(),
            kubernetes_namespace: namespace.into(),
            kubernetes_bearer_token_secret: bearer_token,
            kubernetes_ca_cert_secret: None,
            kubernetes_service_account: None,
            kubernetes_image_pull_secret: None,
        }
    }

    /// Use a Secret containing PEM CA certificates instead of only system trust roots.
    pub fn with_kubernetes_ca_cert_secret(mut self, secret: SecretReference) -> Self {
        self.kubernetes_ca_cert_secret = Some(secret);
        self
    }

    /// Set the service account for workloads in the target Kubernetes namespace.
    pub fn with_kubernetes_service_account(mut self, name: impl Into<String>) -> Self {
        self.kubernetes_service_account = Some(name.into());
        self
    }

    /// Name an image-pull Secret already installed in the target Kubernetes namespace.
    pub fn with_kubernetes_image_pull_secret(mut self, name: impl Into<String>) -> Self {
        self.kubernetes_image_pull_secret = Some(name.into());
        self
    }
}

/// Persisted compute configuration. Contains Secret references, never credential values.
///
/// Registration does not contact Kubernetes or verify permissions. Cluster and job
/// execution are separate from this management API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ComputeIntegrationInfo {
    pub name: String,
    pub database: String,
    pub namespace_path: Vec<String>,
    #[serde(flatten)]
    pub configuration: CreateComputeIntegrationRequest,
    pub created_at_millis: i64,
    pub access_verified: bool,
}
