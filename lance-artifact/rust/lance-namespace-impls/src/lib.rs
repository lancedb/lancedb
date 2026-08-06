// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Namespace implementations.
//!
//! This crate provides various implementations of the Lance Namespace trait.
//!
//! ## Features
//!
//! - `rest`: REST API-based namespace implementation
//! - `rest-adapter`: REST server adapter that exposes any namespace via HTTP
//! - `dir-aws`, `dir-azure`, `dir-gcp`, `dir-oss`: Cloud storage backend support for directory namespace (via lance-io)
//! - `credential-vendor-aws`, `credential-vendor-gcp`, `credential-vendor-azure`: Credential vending for cloud storage
//!
//! ## Implementations
//!
//! - `DirectoryNamespace`: Directory-based implementation (always available)
//! - `RestNamespace`: REST API-based implementation (requires `rest` feature)
//!
//! ## Credential Vending
//!
//! The `credentials` module provides temporary credential vending for cloud storage:
//! - AWS: STS AssumeRole with scoped IAM policies (requires `credential-vendor-aws` feature)
//! - GCP: OAuth2 tokens with access boundaries (requires `credential-vendor-gcp` feature)
//! - Azure: SAS tokens with user delegation keys (requires `credential-vendor-azure` feature)
//!
//! The credential vendor is automatically selected based on the table location URI scheme:
//! - `s3://` for AWS
//! - `gs://` for GCP
//! - `az://` for Azure
//!
//! Configuration properties (prefixed with `credential_vendor.`, prefix is stripped):
//!
//! ```text
//! # Required to enable credential vending
//! credential_vendor.enabled = "true"
//!
//! # Common properties (apply to all providers)
//! credential_vendor.permission = "read"          # read, write, or admin (default: read)
//!
//! # AWS-specific properties (for s3:// locations)
//! credential_vendor.aws_role_arn = "arn:aws:iam::123456789012:role/MyRole"  # required for AWS
//! credential_vendor.aws_duration_millis = "3600000"  # 1 hour (default, range: 15min-12hrs)
//!
//! # GCP-specific properties (for gs:// locations)
//! # Note: GCP uses ADC; set GOOGLE_APPLICATION_CREDENTIALS env var for service account key
//! # Note: GCP token duration cannot be configured; it's determined by the STS endpoint
//! credential_vendor.gcp_service_account = "my-sa@project.iam.gserviceaccount.com"
//! credential_vendor.gcp_workload_identity_provider = "projects/123456/locations/global/workloadIdentityPools/pool/providers/provider"
//! credential_vendor.gcp_impersonation_service_account = "my-sa@project.iam.gserviceaccount.com"
//!
//! # Azure-specific properties (for az:// locations)
//! credential_vendor.azure_account_name = "mystorageaccount"  # required for Azure
//! credential_vendor.azure_tenant_id = "my-tenant-id"
//! credential_vendor.azure_federated_client_id = "my-app-client-id"
//! credential_vendor.azure_duration_millis = "3600000"  # 1 hour (default, up to 7 days)
//! ```
//!
//! ## Usage
//!
//! The recommended way to connect to a namespace is using [`ConnectBuilder`]:
//!
//! ```no_run
//! # use lance_namespace_impls::ConnectBuilder;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let namespace = ConnectBuilder::new("dir")
//!     .property("root", "/path/to/data")
//!     .connect()
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod connect;
pub mod context;
pub mod credentials;
pub mod dir;

#[cfg(feature = "rest")]
pub mod rest;

#[cfg(feature = "rest-adapter")]
pub mod rest_adapter;

// Re-export connect builder
pub use connect::ConnectBuilder;
pub use context::{DynamicContextProvider, OperationInfo};
pub use dir::{
    DirectoryNamespace, DirectoryNamespaceBuilder, OpsMetrics, manifest::ManifestNamespace,
};

// Re-export credential vending
pub use credentials::{
    CredentialVendor, DEFAULT_CREDENTIAL_DURATION_MILLIS, VendedCredentials,
    create_credential_vendor_for_location, detect_provider_from_uri, has_credential_vendor_config,
    redact_credential,
};

#[cfg(feature = "credential-vendor-aws")]
pub use credentials::aws::{AwsCredentialVendor, AwsCredentialVendorConfig};
#[cfg(feature = "credential-vendor-aws")]
pub use credentials::aws_props;

#[cfg(feature = "credential-vendor-gcp")]
pub use credentials::gcp::{GcpCredentialVendor, GcpCredentialVendorConfig};
#[cfg(feature = "credential-vendor-gcp")]
pub use credentials::gcp_props;

#[cfg(feature = "credential-vendor-azure")]
pub use credentials::azure::{AzureCredentialVendor, AzureCredentialVendorConfig};
#[cfg(feature = "credential-vendor-azure")]
pub use credentials::azure_props;

#[cfg(feature = "rest")]
pub use rest::{RestNamespace, RestNamespaceBuilder};

#[cfg(feature = "rest-adapter")]
pub use rest_adapter::{RestAdapter, RestAdapterConfig, RestAdapterHandle};
