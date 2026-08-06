// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Directory-based Lance Namespace implementation.
//!
//! This module provides a directory-based implementation of the Lance namespace
//! that stores tables as Lance datasets in a filesystem directory structure.

pub mod manifest;
pub mod manifest_feature_flags;

use arrow::array::Float32Array;
use arrow::record_batch::RecordBatchIterator;
use arrow_ipc::reader::StreamReader;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::refs::check_valid_branch;
use lance::dataset::scanner::Scanner;
use lance::dataset::statistics::DatasetStatisticsExt;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{
    Dataset, MergeInsertBuilder, UpdateBuilder, WhenMatched, WhenNotMatched,
    WhenNotMatchedBySource, WriteMode, WriteParams,
};
use lance::index::{DatasetIndexExt, IndexParams, vector::VectorIndexParams};
use lance::session::Session;
use lance_index::scalar::{
    BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
};
use lance_index::vector::{
    bq::RQBuildParams, hnsw::builder::HnswBuildParams, ivf::IvfBuildParams, pq::PQBuildParams,
    sq::builder::SQBuildParams,
};
use lance_index::{IndexType, is_system_index};
use lance_io::object_store::throttle::is_throttle_error;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_linalg::distance::MetricType;
use lance_table::io::commit::{ManifestNamingScheme, VERSIONS_DIR};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore as OSObjectStore, PutMode, PutOptions,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

use crate::context::DynamicContextProvider;
use lance_namespace::models::{
    AlterTableAddColumnsRequest, AlterTableAddColumnsResponse, AlterTableAlterColumnsRequest,
    AlterTableAlterColumnsResponse, AlterTableDropColumnsRequest, AlterTableDropColumnsResponse,
    AlterTransactionRequest, AlterTransactionResponse, AnalyzeTableQueryPlanRequest,
    BatchDeleteTableVersionsRequest, BatchDeleteTableVersionsResponse,
    BranchContents as ModelBranchContents, CountTableRowsRequest, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableBranchRequest, CreateTableBranchResponse,
    CreateTableIndexRequest, CreateTableIndexResponse, CreateTableRequest, CreateTableResponse,
    CreateTableScalarIndexResponse, CreateTableTagRequest, CreateTableTagResponse,
    CreateTableVersionRequest, CreateTableVersionResponse, DeclareTableRequest,
    DeclareTableResponse, DeleteFromTableRequest, DeleteFromTableResponse,
    DeleteTableBranchRequest, DeleteTableBranchResponse, DeleteTableTagRequest,
    DeleteTableTagResponse, DescribeNamespaceRequest, DescribeNamespaceResponse,
    DescribeTableIndexStatsRequest, DescribeTableIndexStatsResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTableVersionRequest, DescribeTableVersionResponse,
    DescribeTransactionRequest, DescribeTransactionResponse, DropNamespaceRequest,
    DropNamespaceResponse, DropTableIndexRequest, DropTableIndexResponse, DropTableRequest,
    DropTableResponse, ExplainTableQueryPlanRequest, FragmentStats, FragmentSummary,
    GetTableStatsRequest, GetTableStatsResponse, GetTableTagVersionRequest,
    GetTableTagVersionResponse, Identity, IndexContent, InsertIntoTableRequest,
    InsertIntoTableResponse, ListNamespacesRequest, ListNamespacesResponse,
    ListTableBranchesRequest, ListTableBranchesResponse, ListTableIndicesRequest,
    ListTableIndicesResponse, ListTableTagsRequest, ListTableTagsResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    MergeInsertIntoTableRequest, MergeInsertIntoTableResponse, NamespaceExistsRequest,
    QueryTableRequest, QueryTableRequestColumns, QueryTableRequestVector, RestoreTableRequest,
    RestoreTableResponse, TableExistsRequest, TableVersion, TagContents as ModelTagContents,
    UpdateTableRequest, UpdateTableResponse, UpdateTableSchemaMetadataRequest,
    UpdateTableSchemaMetadataResponse, UpdateTableTagRequest, UpdateTableTagResponse,
};

use lance_core::{Error, Result, box_error};
use lance_index::scalar::inverted::query::{
    BooleanQuery, BoostQuery, FtsQuery, MatchQuery, MultiMatchQuery, Occur, Operator, PhraseQuery,
};
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::schema::arrow_schema_to_json;

use crate::credentials::{
    CredentialVendor, create_credential_vendor_for_location, has_credential_vendor_config,
};

/// Thread-safe metrics tracker for namespace operations.
///
/// Tracks the count of each API operation when `ops_metrics_enabled` is true.
/// Use `retrieve()` to get a snapshot of all operation counts.
#[derive(Debug, Default)]
pub struct OpsMetrics {
    counters: Mutex<HashMap<String, u64>>,
}

impl OpsMetrics {
    /// Increment the counter for an operation.
    pub fn increment(&self, operation: &str) {
        if let Ok(mut counters) = self.counters.lock() {
            *counters.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    /// Get a snapshot of all operation counts.
    pub fn retrieve(&self) -> HashMap<String, u64> {
        self.counters.lock().map(|c| c.clone()).unwrap_or_default()
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        if let Ok(mut counters) = self.counters.lock() {
            counters.clear();
        }
    }
}

/// Build SQL expression list for the add_columns operation.
/// Returns an explicit error when the expression is missing, instead of silently using an empty string.
pub(crate) fn build_sql_expressions(
    new_columns: &[lance_namespace::models::AddColumnsEntry],
) -> Result<Vec<(String, String)>> {
    new_columns
        .iter()
        .map(|col| {
            // expression is Option<Option<String>>: outer Option means whether the
            // field is present, inner Option means whether the value is JSON null.
            let expression = col.expression.clone().and_then(|opt| opt).ok_or_else(|| {
                Error::invalid_input(format!(
                    "Expression is required for new column '{}'",
                    col.name
                ))
            })?;
            Ok((col.name.clone(), expression))
        })
        .collect()
}

/// Build column alteration list for the alter_columns operation.
/// Returns an explicit error when data_type conversion fails, instead of silently ignoring it.
pub(crate) fn build_column_alterations(
    alterations: &[lance_namespace::models::AlterColumnsEntry],
) -> Result<Vec<lance::dataset::ColumnAlteration>> {
    alterations
        .iter()
        .map(|entry| {
            let mut alteration = lance::dataset::ColumnAlteration::new(entry.path.clone());
            // rename is Option<Option<String>>: flatten to get the actual rename value.
            if let Some(Some(rename)) = &entry.rename {
                alteration = alteration.rename(rename.clone());
            }
            // nullable is Option<Option<bool>>: flatten to get the actual nullable value.
            if let Some(Some(nullable)) = entry.nullable {
                alteration = alteration.set_nullable(nullable);
            }
            // data_type is Option<serde_json::Value>: only process when present and not null.
            if let Some(data_type) = &entry.data_type
                && !data_type.is_null()
            {
                let type_str = data_type.as_str().ok_or_else(|| {
                    Error::invalid_input(format!(
                        "data_type for column '{}' must be a JSON string, got: {}",
                        entry.path, data_type
                    ))
                })?;
                let json_type =
                    lance_namespace::models::JsonArrowDataType::new(type_str.to_string());
                let dt =
                    lance_namespace::schema::convert_json_arrow_type(&json_type).map_err(|e| {
                        Error::invalid_input(format!(
                            "Failed to parse data_type '{}' for column '{}': {}",
                            type_str, entry.path, e
                        ))
                    })?;
                alteration = alteration.cast_to(dt);
            }
            Ok(alteration)
        })
        .collect()
}

/// Result of checking table status atomically.
///
/// This struct captures the state of a table directory in a single snapshot,
/// avoiding race conditions between checking existence and other status flags.
pub(crate) struct TableStatus {
    /// Whether the table directory exists (has any files)
    pub(crate) exists: bool,
    /// Whether the table has a `.lance-deregistered` marker file
    pub(crate) is_deregistered: bool,
    /// Whether the table has a `.lance-reserved` marker file (declared but not written)
    pub(crate) has_reserved_file: bool,
}

enum DirectoryIndexParams {
    Scalar {
        index_type: IndexType,
        params: ScalarIndexParams,
    },
    Inverted(InvertedIndexParams),
    Vector {
        index_type: IndexType,
        params: VectorIndexParams,
    },
}

impl DirectoryIndexParams {
    fn index_type(&self) -> IndexType {
        match self {
            Self::Scalar { index_type, .. } | Self::Vector { index_type, .. } => *index_type,
            Self::Inverted(_) => IndexType::Inverted,
        }
    }

    fn params(&self) -> &dyn IndexParams {
        match self {
            Self::Scalar { params, .. } => params,
            Self::Inverted(params) => params,
            Self::Vector { params, .. } => params,
        }
    }
}

/// Builder for creating a DirectoryNamespace.
///
/// This builder provides a fluent API for configuring and establishing
/// connections to directory-based Lance namespaces.
///
/// # Examples
///
/// ```no_run
/// # use lance_namespace_impls::DirectoryNamespaceBuilder;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a local directory namespace
/// let namespace = DirectoryNamespaceBuilder::new("/path/to/data")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ```no_run
/// # use lance_namespace_impls::DirectoryNamespaceBuilder;
/// # use lance::session::Session;
/// # use std::sync::Arc;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create with custom storage options and session
/// let session = Arc::new(Session::default());
/// let namespace = DirectoryNamespaceBuilder::new("s3://bucket/path")
///     .storage_option("region", "us-west-2")
///     .storage_option("access_key_id", "key")
///     .session(session)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DirectoryNamespaceBuilder {
    root: String,
    storage_options: Option<HashMap<String, String>>,
    session: Option<Arc<Session>>,
    manifest_enabled: bool,
    dir_listing_enabled: bool,
    inline_optimization_enabled: bool,
    table_version_tracking_enabled: bool,
    /// When true, enables migration mode where the namespace checks the manifest first
    /// before falling back to directory listing for root-level tables. When false (default),
    /// root-level tables use directory listing directly without checking the manifest,
    /// avoiding extra object store calls.
    dir_listing_to_manifest_migration_enabled: bool,
    credential_vendor_properties: HashMap<String, String>,
    context_provider: Option<Arc<dyn DynamicContextProvider>>,
    commit_retries: Option<u32>,
    /// When true, returns input storage options in describe_table/declare_table responses
    /// when no credential vendor is configured. Useful for testing. Default: false.
    vend_input_storage_options: bool,
    /// When set, adds expires_at_millis to vended storage options. The value is calculated
    /// as current_time_millis + this interval. This allows clients to know when to refresh
    /// credentials by calling describe_table again. Only effective when vend_input_storage_options
    /// is true.
    vend_input_storage_options_refresh_interval_millis: Option<u64>,
    /// When true, tracks operation metrics. Default: false.
    ops_metrics_enabled: bool,
}

impl std::fmt::Debug for DirectoryNamespaceBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectoryNamespaceBuilder")
            .field("root", &self.root)
            .field("storage_options", &self.storage_options)
            .field("manifest_enabled", &self.manifest_enabled)
            .field("dir_listing_enabled", &self.dir_listing_enabled)
            .field(
                "inline_optimization_enabled",
                &self.inline_optimization_enabled,
            )
            .field(
                "table_version_tracking_enabled",
                &self.table_version_tracking_enabled,
            )
            .field(
                "dir_listing_to_manifest_migration_enabled",
                &self.dir_listing_to_manifest_migration_enabled,
            )
            .field(
                "context_provider",
                &self.context_provider.as_ref().map(|_| "Some(...)"),
            )
            .field(
                "vend_input_storage_options",
                &self.vend_input_storage_options,
            )
            .field(
                "vend_input_storage_options_refresh_interval_millis",
                &self.vend_input_storage_options_refresh_interval_millis,
            )
            .field("ops_metrics_enabled", &self.ops_metrics_enabled)
            .finish()
    }
}

impl DirectoryNamespaceBuilder {
    /// Create a new DirectoryNamespaceBuilder with the specified root path.
    ///
    /// # Arguments
    ///
    /// * `root` - Root directory path (local path or cloud URI like s3://bucket/path)
    pub fn new(root: impl Into<String>) -> Self {
        Self {
            root: root.into().trim_end_matches('/').to_string(),
            storage_options: None,
            session: None,
            manifest_enabled: true,
            dir_listing_enabled: true, // Default to enabled for backwards compatibility
            inline_optimization_enabled: true,
            table_version_tracking_enabled: false, // Default to disabled
            dir_listing_to_manifest_migration_enabled: false, // Default to disabled
            credential_vendor_properties: HashMap::new(),
            context_provider: None,
            commit_retries: None,
            vend_input_storage_options: false,
            vend_input_storage_options_refresh_interval_millis: None,
            ops_metrics_enabled: false,
        }
    }

    /// Enable or disable manifest-based listing.
    ///
    /// When enabled (default), the namespace uses a `__manifest` table to track tables.
    /// When disabled, relies solely on directory scanning.
    pub fn manifest_enabled(mut self, enabled: bool) -> Self {
        self.manifest_enabled = enabled;
        self
    }

    /// Enable or disable directory-based listing fallback.
    ///
    /// When enabled (default), falls back to directory scanning for tables not in the manifest.
    /// When disabled, only consults the manifest table.
    pub fn dir_listing_enabled(mut self, enabled: bool) -> Self {
        self.dir_listing_enabled = enabled;
        self
    }

    /// Enable or disable migration mode from directory listing to manifest.
    ///
    /// When enabled, root-level table operations check the manifest first before
    /// falling back to directory listing. When disabled (default), root-level tables
    /// use directory listing directly, avoiding extra object store calls.
    /// Only relevant when both `manifest_enabled` and `dir_listing_enabled` are true.
    pub fn dir_listing_to_manifest_migration_enabled(mut self, enabled: bool) -> Self {
        self.dir_listing_to_manifest_migration_enabled = enabled;
        self
    }

    /// Enable or disable replacement index maintenance for the __manifest table.
    ///
    /// When enabled (default), copy-on-write manifest rewrites build replacement indices
    /// for fast reads. When disabled, rewrites only replace data files.
    pub fn inline_optimization_enabled(mut self, enabled: bool) -> Self {
        self.inline_optimization_enabled = enabled;
        self
    }

    /// Enable or disable table version tracking through the namespace.
    ///
    /// When enabled, `describe_table` returns `managed_versioning: true` to indicate
    /// that commits should go through the namespace's table version APIs rather than
    /// direct object store operations.
    ///
    /// When disabled (default), `managed_versioning` is not set.
    pub fn table_version_tracking_enabled(mut self, enabled: bool) -> Self {
        self.table_version_tracking_enabled = enabled;
        self
    }

    /// Create a DirectoryNamespaceBuilder from properties HashMap.
    ///
    /// This method parses a properties map into builder configuration.
    /// It expects:
    /// - `root`: The root directory path (required)
    /// - `manifest_enabled`: Enable manifest-based table tracking (optional, default: true)
    /// - `dir_listing_enabled`: Enable directory listing for table discovery (optional, default: true)
    /// - `inline_optimization_enabled`: Enable replacement indices on __manifest rewrites (optional, default: true)
    /// - `storage.*`: Storage options (optional, prefix will be stripped)
    ///
    /// Credential vendor properties (prefixed with `credential_vendor.`, prefix is stripped):
    /// - `credential_vendor.enabled`: Set to "true" to enable credential vending (required)
    /// - `credential_vendor.permission`: Permission level: read, write, or admin (default: read)
    ///
    /// AWS-specific properties (for s3:// locations):
    /// - `credential_vendor.aws_role_arn`: AWS IAM role ARN (required for AWS)
    /// - `credential_vendor.aws_external_id`: AWS external ID (optional)
    /// - `credential_vendor.aws_region`: AWS region (optional)
    /// - `credential_vendor.aws_role_session_name`: AWS role session name (optional)
    /// - `credential_vendor.aws_duration_millis`: Credential duration in ms (default: 3600000, range: 15min-12hrs)
    ///
    /// GCP-specific properties (for gs:// locations):
    /// - `credential_vendor.gcp_service_account`: Service account to impersonate (optional)
    /// - `credential_vendor.gcp_workload_identity_provider`: Workload Identity Provider for OIDC token exchange (optional)
    /// - `credential_vendor.gcp_impersonation_service_account`: Service account to impersonate after workload identity exchange (optional)
    ///
    /// Note: GCP uses Application Default Credentials (ADC). To use a service account key file,
    /// set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable before starting.
    /// GCP token duration cannot be configured; it's determined by the STS endpoint (typically 1 hour).
    ///
    /// Azure-specific properties (for az:// locations):
    /// - `credential_vendor.azure_account_name`: Azure storage account name (required for Azure)
    /// - `credential_vendor.azure_tenant_id`: Azure tenant ID (optional)
    /// - `credential_vendor.azure_federated_client_id`: Client ID used for workload identity federation (optional)
    /// - `credential_vendor.azure_duration_millis`: Credential duration in ms (default: 3600000, up to 7 days)
    ///
    /// # Arguments
    ///
    /// * `properties` - Configuration properties
    /// * `session` - Optional Lance session to reuse object store registry
    ///
    /// # Returns
    ///
    /// Returns a `DirectoryNamespaceBuilder` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the `root` property is missing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use lance_namespace_impls::DirectoryNamespaceBuilder;
    /// # use std::collections::HashMap;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut properties = HashMap::new();
    /// properties.insert("root".to_string(), "/path/to/data".to_string());
    /// properties.insert("manifest_enabled".to_string(), "true".to_string());
    /// properties.insert("dir_listing_enabled".to_string(), "false".to_string());
    /// properties.insert("storage.region".to_string(), "us-west-2".to_string());
    ///
    /// let namespace = DirectoryNamespaceBuilder::from_properties(properties, None)?
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_properties(
        properties: HashMap<String, String>,
        session: Option<Arc<Session>>,
    ) -> Result<Self> {
        // Extract root from properties (required)
        let root = properties.get("root").cloned().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Missing required property 'root' for directory namespace".to_string(),
            })
        })?;

        // Extract storage options (properties prefixed with "storage.")
        let storage_options: HashMap<String, String> = properties
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("storage.")
                    .map(|key| (key.to_string(), v.clone()))
            })
            .collect();

        let storage_options = if storage_options.is_empty() {
            None
        } else {
            Some(storage_options)
        };

        // Extract manifest_enabled (default: true)
        let manifest_enabled = properties
            .get("manifest_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        // Extract dir_listing_enabled (default: true)
        let dir_listing_enabled = properties
            .get("dir_listing_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        // Extract inline_optimization_enabled (default: true)
        let inline_optimization_enabled = properties
            .get("inline_optimization_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        // Extract table_version_tracking_enabled (default: false)
        let table_version_tracking_enabled = properties
            .get("table_version_tracking_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        // Extract dir_listing_to_manifest_migration_enabled (default: false)
        let dir_listing_to_manifest_migration_enabled = properties
            .get("dir_listing_to_manifest_migration_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        // Extract credential vendor properties (properties prefixed with "credential_vendor.")
        // The prefix is stripped to get short property names
        // The build() method will check if enabled=true before creating the vendor
        let credential_vendor_properties: HashMap<String, String> = properties
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix("credential_vendor.")
                    .map(|key| (key.to_string(), v.clone()))
            })
            .collect();

        let commit_retries = properties
            .get("commit_retries")
            .and_then(|v| v.parse::<u32>().ok());

        // Extract vend_input_storage_options (default: false)
        let vend_input_storage_options = properties
            .get("vend_input_storage_options")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        // Extract vend_input_storage_options_refresh_interval_millis (optional)
        let vend_input_storage_options_refresh_interval_millis = properties
            .get("vend_input_storage_options_refresh_interval_millis")
            .and_then(|v| v.parse::<u64>().ok());

        // Extract ops_metrics_enabled (default: false)
        let ops_metrics_enabled = properties
            .get("ops_metrics_enabled")
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(false);

        Ok(Self {
            root: root.trim_end_matches('/').to_string(),
            storage_options,
            session,
            manifest_enabled,
            dir_listing_enabled,
            inline_optimization_enabled,
            table_version_tracking_enabled,
            dir_listing_to_manifest_migration_enabled,
            credential_vendor_properties,
            context_provider: None,
            commit_retries,
            vend_input_storage_options,
            vend_input_storage_options_refresh_interval_millis,
            ops_metrics_enabled,
        })
    }

    /// Add a storage option.
    ///
    /// # Arguments
    ///
    /// * `key` - Storage option key (e.g., "region", "access_key_id")
    /// * `value` - Storage option value
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.storage_options
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value.into());
        self
    }

    /// Add multiple storage options.
    ///
    /// # Arguments
    ///
    /// * `options` - HashMap of storage options to add
    pub fn storage_options(mut self, options: HashMap<String, String>) -> Self {
        self.storage_options
            .get_or_insert_with(HashMap::new)
            .extend(options);
        self
    }

    /// Set the Lance session to use for this namespace.
    ///
    /// When a session is provided, the namespace will reuse the session's
    /// object store registry, allowing multiple namespaces and datasets
    /// to share the same underlying storage connections.
    ///
    /// # Arguments
    ///
    /// * `session` - Arc-wrapped Lance session
    pub fn session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set the number of retries for commit operations on the manifest table.
    /// If not set, defaults to [`lance_table::io::commit::CommitConfig`] default (20).
    pub fn commit_retries(mut self, retries: u32) -> Self {
        self.commit_retries = Some(retries);
        self
    }

    /// Add a credential vendor property.
    ///
    /// Use short property names without the `credential_vendor.` prefix.
    /// Common properties: `enabled`, `permission`.
    /// AWS properties: `aws_role_arn`, `aws_external_id`, `aws_region`, `aws_role_session_name`, `aws_duration_millis`.
    /// GCP properties: `gcp_service_account`, `gcp_workload_identity_provider`, `gcp_impersonation_service_account`.
    /// Azure properties: `azure_account_name`, `azure_tenant_id`, `azure_federated_client_id`, `azure_duration_millis`.
    ///
    /// # Arguments
    ///
    /// * `key` - Property key (e.g., "enabled", "aws_role_arn")
    /// * `value` - Property value
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lance_namespace_impls::DirectoryNamespaceBuilder;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let namespace = DirectoryNamespaceBuilder::new("s3://my-bucket/data")
    ///     .credential_vendor_property("enabled", "true")
    ///     .credential_vendor_property("aws_role_arn", "arn:aws:iam::123456789012:role/MyRole")
    ///     .credential_vendor_property("permission", "read")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn credential_vendor_property(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.credential_vendor_properties
            .insert(key.into(), value.into());
        self
    }

    /// Add multiple credential vendor properties.
    ///
    /// Use short property names without the `credential_vendor.` prefix.
    ///
    /// # Arguments
    ///
    /// * `properties` - HashMap of credential vendor properties to add
    pub fn credential_vendor_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.credential_vendor_properties.extend(properties);
        self
    }

    /// Set a dynamic context provider for per-request context.
    ///
    /// The provider can be used to generate additional context for operations.
    /// For DirectoryNamespace, the context is stored but not directly used
    /// in operations (unlike RestNamespace where it's converted to HTTP headers).
    ///
    /// # Arguments
    ///
    /// * `provider` - The context provider implementation
    pub fn context_provider(mut self, provider: Arc<dyn DynamicContextProvider>) -> Self {
        self.context_provider = Some(provider);
        self
    }

    /// Enable or disable returning input storage options in responses.
    ///
    /// When enabled, `describe_table` and `declare_table` will return the storage
    /// options passed to the builder when no credential vendor is configured.
    /// This is useful for testing scenarios where you want to pass storage options
    /// through to clients.
    ///
    /// Default is false (storage options are not returned unless credential vending is configured).
    pub fn vend_input_storage_options(mut self, enabled: bool) -> Self {
        self.vend_input_storage_options = enabled;
        self
    }

    /// Set the refresh interval for vended input storage options.
    ///
    /// When set, vended storage options will include an `expires_at_millis` field
    /// calculated as `current_time_millis + interval_millis`. This allows clients
    /// to know when to refresh credentials by calling `describe_table` again.
    ///
    /// This only has effect when `vend_input_storage_options` is enabled.
    ///
    /// # Arguments
    ///
    /// * `interval_millis` - The refresh interval in milliseconds
    pub fn vend_input_storage_options_refresh_interval_millis(
        mut self,
        interval_millis: u64,
    ) -> Self {
        self.vend_input_storage_options_refresh_interval_millis = Some(interval_millis);
        self
    }

    /// Enable or disable operation metrics tracking.
    ///
    /// When enabled, the namespace will track how many times each API operation
    /// is called. Use `retrieve_ops_metrics()` on the built namespace to get
    /// the current counts.
    ///
    /// Default is false.
    pub fn ops_metrics_enabled(mut self, enabled: bool) -> Self {
        self.ops_metrics_enabled = enabled;
        self
    }

    /// Build the DirectoryNamespace.
    ///
    /// # Returns
    ///
    /// Returns a `DirectoryNamespace` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The root path is invalid
    /// - Connection to the storage backend fails
    /// - Storage options are invalid
    pub async fn build(self) -> Result<DirectoryNamespace> {
        let (object_store, base_path) =
            Self::initialize_object_store(&self.root, &self.storage_options, &self.session).await?;

        let manifest_ns = if self.manifest_enabled {
            match manifest::ManifestNamespace::open_from_directory(
                self.root.clone(),
                self.storage_options.clone(),
                self.session.clone(),
                object_store.clone(),
                base_path.clone(),
                self.dir_listing_enabled,
                self.inline_optimization_enabled,
                self.commit_retries,
            )
            .await
            {
                Ok(ns) => Some(Arc::new(ns)),
                Err(e) if manifest_feature_flags::is_incompatible_manifest_error(&e) => {
                    // The manifest exists but was written with a feature flag this
                    // build does not understand. Refuse rather than silently
                    // degrading to a directory-listing view that ignores it.
                    return Err(e);
                }
                Err(e) if manifest::ManifestNamespace::is_not_found_load_error(&e) => {
                    log::debug!("Manifest namespace does not exist yet: {}", e);
                    None
                }
                Err(e) => return Err(e),
            }
        } else {
            None
        };
        let manifest_cell = OnceCell::new();
        if let Some(manifest_ns) = manifest_ns {
            let _ = manifest_cell.set(manifest_ns);
        }

        // Create credential vendor once during initialization if enabled
        let credential_vendor = if has_credential_vendor_config(&self.credential_vendor_properties)
        {
            create_credential_vendor_for_location(&self.root, &self.credential_vendor_properties)
                .await?
                .map(Arc::from)
        } else {
            None
        };

        let ops_metrics = if self.ops_metrics_enabled {
            Some(Arc::new(OpsMetrics::default()))
        } else {
            None
        };

        Ok(DirectoryNamespace {
            root: self.root,
            storage_options: self.storage_options,
            session: self.session,
            object_store,
            base_path,
            manifest_ns: manifest_cell,
            write_manifest_ns: OnceCell::new(),
            manifest_enabled: self.manifest_enabled,
            dir_listing_enabled: self.dir_listing_enabled,
            inline_optimization_enabled: self.inline_optimization_enabled,
            commit_retries: self.commit_retries,
            dir_listing_to_manifest_migration_enabled: self
                .dir_listing_to_manifest_migration_enabled,
            table_version_tracking_enabled: self.table_version_tracking_enabled,
            credential_vendor,
            context_provider: self.context_provider,
            vend_input_storage_options: self.vend_input_storage_options,
            vend_input_storage_options_refresh_interval_millis: self
                .vend_input_storage_options_refresh_interval_millis,
            ops_metrics,
        })
    }

    /// Initialize the Lance ObjectStore based on the configuration
    async fn initialize_object_store(
        root: &str,
        storage_options: &Option<HashMap<String, String>>,
        session: &Option<Arc<Session>>,
    ) -> Result<(Arc<ObjectStore>, Path)> {
        // Build ObjectStoreParams from storage options
        let accessor = storage_options.clone().map(|opts| {
            Arc::new(lance_io::object_store::StorageOptionsAccessor::with_static_options(opts))
        });
        let params = ObjectStoreParams {
            storage_options_accessor: accessor,
            ..Default::default()
        };

        // Use object store registry from session if provided, otherwise create a new one
        let registry = if let Some(session) = session {
            session.store_registry()
        } else {
            Arc::new(ObjectStoreRegistry::default())
        };

        // Use Lance's object store factory to create from URI
        let (object_store, base_path) = ObjectStore::from_uri_and_params(registry, root, &params)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to create object store: {:?}", e),
                })
            })?;

        Ok((object_store, base_path))
    }
}

/// Directory-based implementation of Lance Namespace.
///
/// This implementation stores tables as Lance datasets in a directory structure.
/// It supports local filesystems and cloud storage backends through Lance's object store.
///
/// ## Manifest-based Listing
///
/// When `manifest_enabled=true`, the namespace uses a special `__manifest` Lance table to track tables
/// instead of scanning the filesystem. This provides:
/// - Better performance for listing operations
/// - Ability to track table metadata
/// - Foundation for future features like namespaces and table renaming
///
/// When `dir_listing_enabled=true`, the namespace falls back to directory scanning for tables not
/// found in the manifest, enabling gradual migration.
///
/// ## Credential Vending
///
/// When credential vendor properties are configured, `describe_table` will vend temporary
/// credentials based on the table location URI. The vendor type is auto-selected:
/// - `s3://` locations use AWS STS AssumeRole
/// - `gs://` locations use GCP OAuth2 tokens
/// - `az://` locations use Azure SAS tokens
pub struct DirectoryNamespace {
    root: String,
    storage_options: Option<HashMap<String, String>>,
    session: Option<Arc<Session>>,
    object_store: Arc<ObjectStore>,
    base_path: Path,
    manifest_ns: OnceCell<Arc<manifest::ManifestNamespace>>,
    write_manifest_ns: OnceCell<Arc<manifest::ManifestNamespace>>,
    manifest_enabled: bool,
    dir_listing_enabled: bool,
    inline_optimization_enabled: bool,
    commit_retries: Option<u32>,
    /// When true, root-level table operations check the manifest first before
    /// falling back to directory listing. When false, root-level tables skip
    /// the manifest check and use directory listing directly.
    dir_listing_to_manifest_migration_enabled: bool,
    /// When true, `describe_table` returns `managed_versioning: true` to indicate
    /// commits should go through namespace table version APIs.
    table_version_tracking_enabled: bool,
    /// Credential vendor created once during initialization.
    /// Used to vend temporary credentials for table access.
    credential_vendor: Option<Arc<dyn CredentialVendor>>,
    /// Dynamic context provider for per-request context.
    /// Stored but not directly used in operations (available for future extensions).
    #[allow(dead_code)]
    context_provider: Option<Arc<dyn DynamicContextProvider>>,
    /// When true, returns input storage options in responses when no credential vendor is configured.
    vend_input_storage_options: bool,
    /// Refresh interval in milliseconds for vended input storage options.
    /// When set, expires_at_millis is added to storage options.
    vend_input_storage_options_refresh_interval_millis: Option<u64>,
    /// Operation metrics tracker, created when ops_metrics_enabled is true.
    ops_metrics: Option<Arc<OpsMetrics>>,
}

impl std::fmt::Debug for DirectoryNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.namespace_id())
    }
}

impl std::fmt::Display for DirectoryNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.namespace_id())
    }
}

/// Inputs for resolving an already-published `create_table_version` target.
struct ExistingTableVersionResolve<'a> {
    staging_path: &'a Path,
    final_path: &'a Path,
    version: u64,
    table_uri: &'a str,
    final_meta: &'a ObjectMeta,
    request_manifest_size: Option<i64>,
}

/// Describes the version ranges to delete for a single table.
/// Used by `batch_delete_table_versions` and `delete_physical_version_files`.
struct TableDeleteEntry {
    table_id: Option<Vec<String>>,
    ranges: Vec<(i64, i64)>,
}

/// Persistent record of `alter_transaction` outcomes for a single transaction.
///
/// Lance's transaction file is immutable once written, so we record any
/// modifications (status transitions, extra properties, tombstoned properties)
/// in a namespace-owned sidecar file. The sidecar is then merged into the
/// response of subsequent `describe_transaction` / `alter_transaction` calls.
///
/// Serialization is implemented manually via `serde_json::Value` to avoid
/// pulling in `serde`'s `derive` feature for this crate.
#[derive(Debug, Clone, Default)]
struct TransactionAlteration {
    /// The most recently applied status, if any.
    status: Option<String>,
    /// User-defined properties layered on top of the immutable transaction
    /// properties. Values here take precedence over the transaction's own
    /// properties when both are present.
    properties: HashMap<String, String>,
    /// Names of transaction properties that have been tombstoned via
    /// `unset_property_action`. A tombstoned key is hidden from the response
    /// even when the immutable transaction still carries it.
    removed_properties: std::collections::HashSet<String>,
}

impl TransactionAlteration {
    /// JSON field names used for the sidecar on-disk representation.
    const F_STATUS: &'static str = "status";
    const F_PROPERTIES: &'static str = "properties";
    const F_REMOVED_PROPERTIES: &'static str = "removed_properties";

    /// Serialize this alteration to a JSON byte vector.
    ///
    /// Uses the same pattern as `dir/manifest.rs`: rely on the built-in
    /// `Serialize` impls for `Option<String>`, `HashMap<String, String>` and
    /// `HashSet<String>` provided by the `serde` crate (transitively pulled in
    /// by `serde_json`), so no `serde` derive nor extra dependency is needed.
    fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&serde_json::json!({
            Self::F_STATUS: self.status,
            Self::F_PROPERTIES: self.properties,
            Self::F_REMOVED_PROPERTIES: self.removed_properties,
        }))
    }

    /// Deserialize an alteration from JSON bytes, mirroring the
    /// `serde_json::from_slice::<HashMap<String, String>>(...)` idiom already
    /// used in `dir/manifest.rs`. Missing / null fields fall back to defaults
    /// so that the sidecar format stays forward-compatible.
    fn from_json_slice(bytes: &[u8]) -> serde_json::Result<Self> {
        let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)?;
        Ok(Self {
            status: serde_json::from_value(
                obj.remove(Self::F_STATUS)
                    .unwrap_or(serde_json::Value::Null),
            )?,
            properties: serde_json::from_value(
                obj.remove(Self::F_PROPERTIES)
                    .unwrap_or(serde_json::Value::Null),
            )
            .unwrap_or_default(),
            removed_properties: serde_json::from_value(
                obj.remove(Self::F_REMOVED_PROPERTIES)
                    .unwrap_or(serde_json::Value::Null),
            )
            .unwrap_or_default(),
        })
    }
}

impl DirectoryNamespace {
    fn manifest_ns_for_read(&self) -> Option<&Arc<manifest::ManifestNamespace>> {
        self.write_manifest_ns
            .get()
            .or_else(|| self.manifest_ns.get())
    }

    async fn manifest_ns_for_write(&self) -> Result<Option<Arc<manifest::ManifestNamespace>>> {
        if !self.manifest_enabled {
            return Ok(None);
        }

        let manifest_ns = self
            .write_manifest_ns
            .get_or_try_init(|| async {
                manifest::ManifestNamespace::from_directory(
                    self.root.clone(),
                    self.storage_options.clone(),
                    self.session.clone(),
                    self.object_store.clone(),
                    self.base_path.clone(),
                    self.dir_listing_enabled,
                    self.inline_optimization_enabled,
                    self.commit_retries,
                )
                .await
                .map(Arc::new)
            })
            .await?;
        Ok(Some(manifest_ns.clone()))
    }

    /// Lazily open the `__manifest` dataset (read-only) into the read cell.
    ///
    /// `manifest_ns` is populated at construction only if `__manifest` already
    /// existed then. A manifest created afterwards -- e.g. by another
    /// connection's write, since Phalanx caches a connection per db and the
    /// first op on a fresh db is usually a read -- would otherwise stay
    /// invisible to this connection's reads forever, so describe/list/exists
    /// report "not found" for a table that is in fact registered. Re-open on
    /// demand so reads self-heal once the manifest exists. Idempotent and cheap
    /// once the cell is populated; unlike `manifest_ns_for_write` it never
    /// creates the manifest.
    async fn ensure_read_manifest(&self) -> Result<()> {
        if !self.manifest_enabled
            || self.manifest_ns.get().is_some()
            || self.write_manifest_ns.get().is_some()
        {
            return Ok(());
        }
        match self
            .manifest_ns
            .get_or_try_init(|| async {
                manifest::ManifestNamespace::open_from_directory(
                    self.root.clone(),
                    self.storage_options.clone(),
                    self.session.clone(),
                    self.object_store.clone(),
                    self.base_path.clone(),
                    self.dir_listing_enabled,
                    self.inline_optimization_enabled,
                    self.commit_retries,
                )
                .await
                .map(Arc::new)
            })
            .await
        {
            Ok(_) => Ok(()),
            // Manifest still doesn't exist: leave the cell empty so a later read
            // retries once it does. A genuinely absent table is still reported
            // not-found by the callers' existing `manifest_ns_for_read() == None`
            // branch, exactly as before.
            Err(e) if manifest::ManifestNamespace::is_not_found_load_error(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn child_namespace_requires_manifest_error(&self) -> Error {
        if self.manifest_enabled {
            NamespaceError::NamespaceNotFound {
                message: "Child namespace reads require an existing __manifest dataset".to_string(),
            }
            .into()
        } else {
            NamespaceError::Unsupported {
                message: "Child namespaces are only supported when manifest mode is enabled"
                    .to_string(),
            }
            .into()
        }
    }

    /// Apply pagination to a list of table names
    ///
    /// Sorts the list alphabetically and applies pagination using page_token (start_after) and limit.
    ///
    /// # Arguments
    /// * `names` - The vector of table names to paginate
    /// * `page_token` - Skip items until finding one greater than this value (start_after semantics)
    /// * `limit` - Maximum number of items to keep
    ///
    /// # Returns
    /// The next page token (last item in this page) if more results exist beyond the limit,
    /// or `None` if this is the last page.
    fn apply_pagination(
        names: &mut Vec<String>,
        page_token: Option<String>,
        limit: Option<i32>,
    ) -> Option<String> {
        // Sort alphabetically for consistent ordering
        names.sort();

        // Apply page_token filtering (start_after semantics)
        if let Some(start_after) = page_token {
            if let Some(index) = names
                .iter()
                .position(|name| name.as_str() > start_after.as_str())
            {
                names.drain(0..index);
            } else {
                names.clear();
            }
        }

        // Apply limit and compute next page token
        if let Some(limit) = limit
            && limit >= 0
        {
            let limit = limit as usize;
            if names.len() > limit {
                let next_page_token = if limit > 0 {
                    Some(names[limit - 1].clone())
                } else {
                    None
                };
                names.truncate(limit);
                return next_page_token;
            }
        }

        None
    }

    /// List tables using directory scanning (fallback method)
    async fn list_directory_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let entries = self
            .object_store
            .read_dir(self.base_path.clone())
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to list directory: {:?}", e),
                })
            })?;

        for entry in entries {
            let path = entry.trim_end_matches('/');
            if !path.ends_with(".lance") {
                continue;
            }

            let table_name = &path[..path.len() - 6];

            // Use atomic check to skip deregistered tables.
            let status = self.check_table_status(table_name).await?;
            if status.is_deregistered {
                continue;
            }

            tables.push(table_name.to_string());
        }

        Ok(tables)
    }

    /// Validate that the namespace ID represents the root namespace
    fn validate_root_namespace_id(id: &Option<Vec<String>>) -> Result<()> {
        if let Some(id) = id
            && !id.is_empty()
        {
            return Err(NamespaceError::Unsupported {
                message: format!(
                    "Directory namespace only supports root namespace operations, but got namespace ID: {:?}. Expected empty ID.",
                    id
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Extract table name from table ID
    fn table_name_from_id(id: &Option<Vec<String>>) -> Result<String> {
        let id = id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Directory namespace table ID cannot be empty".to_string(),
            })
        })?;

        if id.len() != 1 {
            return Err(NamespaceError::Unsupported {
                message: format!(
                    "Multi-level table IDs are only supported when manifest mode is enabled, but got: {:?}",
                    id
                ),
            }
            .into());
        }

        Ok(id[0].clone())
    }

    fn format_table_id(table_id: &[String]) -> String {
        format!(
            "table id '{}'",
            manifest::ManifestNamespace::str_object_id(table_id)
        )
    }

    fn format_table_id_from_request(id: &Option<Vec<String>>) -> String {
        id.as_ref()
            .map(|table_id| Self::format_table_id(table_id))
            .unwrap_or_else(|| "table id '<unknown>'".to_string())
    }

    async fn resolve_table_location(&self, id: &Option<Vec<String>>) -> Result<String> {
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = id.clone();
        describe_req.load_detailed_metadata = Some(false);

        // Use internal impl to avoid counting this as an external API call
        let describe_resp = self.describe_table_impl(describe_req).await?;

        describe_resp.location.ok_or_else(|| {
            lance_core::Error::from(NamespaceError::TableNotFound {
                message: format!("Table location not found for: {:?}", id),
            })
        })
    }

    /// Map a Lance ref-related error returned by `Dataset::tags()` operations into
    /// the appropriate `NamespaceError` for tag APIs (create/get/update/delete).
    fn map_tag_error(err: lance_core::Error, tag: &str, table_uri: &str) -> lance_core::Error {
        match err {
            lance_core::Error::RefNotFound { .. } => NamespaceError::TableTagNotFound {
                message: format!("tag '{}' for table at '{}'", tag, table_uri),
            }
            .into(),
            lance_core::Error::RefConflict { .. } => NamespaceError::TableTagAlreadyExists {
                message: format!("tag '{}' for table at '{}'", tag, table_uri),
            }
            .into(),
            lance_core::Error::InvalidRef { message } => NamespaceError::InvalidInput {
                message: format!("invalid tag '{}': {}", tag, message),
            }
            .into(),
            lance_core::Error::VersionNotFound { message } => {
                NamespaceError::TableVersionNotFound {
                    message: format!(
                        "version referenced by tag '{}' not found for table at '{}': {}",
                        tag, table_uri, message
                    ),
                }
                .into()
            }
            other => NamespaceError::Internal {
                message: format!(
                    "tag operation failed for tag '{}' on table at '{}': {}",
                    tag, table_uri, other
                ),
            }
            .into(),
        }
    }

    /// Map lance-core ref errors from branch operations to namespace errors.
    ///
    /// `RefConflict` is intentionally not handled here: create-time duplicates are rejected by
    /// the existence pre-check before `create_branch` runs, and delete maps its own `RefConflict`
    /// (branch still has dependents) inline.
    fn map_branch_error(
        err: lance_core::Error,
        branch: &str,
        table_uri: &str,
    ) -> lance_core::Error {
        match err {
            lance_core::Error::RefNotFound { .. } => NamespaceError::TableBranchNotFound {
                message: format!("branch '{}' for table at '{}'", branch, table_uri),
            }
            .into(),
            lance_core::Error::InvalidRef { message } => NamespaceError::InvalidInput {
                message: format!("invalid branch '{}': {}", branch, message),
            }
            .into(),
            lance_core::Error::VersionNotFound { message } => {
                NamespaceError::TableVersionNotFound {
                    message: format!(
                        "source version for branch '{}' not found for table at '{}': {}",
                        branch, table_uri, message
                    ),
                }
                .into()
            }
            other => NamespaceError::Internal {
                message: format!(
                    "branch operation failed for branch '{}' on table at '{}': {}",
                    branch, table_uri, other
                ),
            }
            .into(),
        }
    }

    /// Map a Lance error from a table mutation (update / delete / merge-insert) into the most
    /// specific `NamespaceError` we can determine from the underlying variant.
    ///
    /// Collapsing every failure into `InvalidInput`/`Internal` hides the real cause from callers;
    /// mapping per variant lets them branch on a meaningful error code (e.g. retry on
    /// `ConcurrentModification`, surface `TableNotFound` to the user).
    ///
    /// Commit-conflict variants are mapped consistently with `convert_lance_commit_error` in
    /// `manifest.rs`: `CommitConflict` (retries exhausted, safe to retry) -> `Throttling`, while
    /// semantic conflicts (`TooMuchWriteContention` / `RetryableCommitConflict` /
    /// `IncompatibleTransaction` / `VersionConflict`) -> `ConcurrentModification`.
    fn map_mutation_error(
        err: lance_core::Error,
        operation: &str,
        table_uri: &str,
    ) -> lance_core::Error {
        let detail = err.to_string();
        let ns_err = match &err {
            lance_core::Error::InvalidInput { .. }
            | lance_core::Error::Unprocessable { .. }
            | lance_core::Error::InvalidRef { .. } => NamespaceError::InvalidInput {
                message: format!(
                    "Invalid input for {} on table at '{}': {}",
                    operation, table_uri, detail
                ),
            },
            lance_core::Error::NotFound { .. } | lance_core::Error::DatasetNotFound { .. } => {
                NamespaceError::TableNotFound {
                    message: format!(
                        "Table at '{}' not found while running {}: {}",
                        table_uri, operation, detail
                    ),
                }
            }
            lance_core::Error::SchemaMismatch { .. } | lance_core::Error::Schema { .. } => {
                NamespaceError::TableSchemaValidationError {
                    message: format!(
                        "Schema validation failed for {} on table at '{}': {}",
                        operation, table_uri, detail
                    ),
                }
            }
            // `CommitConflict` means the version-collision retries were exhausted; the operation
            // is safe to retry as-is, so surface it as `Throttling` (kept aligned with
            // `convert_lance_commit_error` in manifest.rs).
            lance_core::Error::CommitConflict { .. } => NamespaceError::Throttling {
                message: format!(
                    "Too many concurrent writes for {} on table at '{}', please retry later: {}",
                    operation, table_uri, detail
                ),
            },
            // Semantic conflicts: a concurrent change is incompatible with this one and retrying
            // as-is would not help, so surface them as `ConcurrentModification` (kept aligned with
            // `convert_lance_commit_error` in manifest.rs).
            lance_core::Error::TooMuchWriteContention { .. }
            | lance_core::Error::RetryableCommitConflict { .. }
            | lance_core::Error::IncompatibleTransaction { .. }
            | lance_core::Error::VersionConflict { .. } => NamespaceError::ConcurrentModification {
                message: format!(
                    "Concurrent modification detected for {} on table at '{}': {}",
                    operation, table_uri, detail
                ),
            },
            lance_core::Error::NotSupported { .. } => NamespaceError::Unsupported {
                message: format!(
                    "{} is not supported on table at '{}': {}",
                    operation, table_uri, detail
                ),
            },
            _ => NamespaceError::Internal {
                message: format!(
                    "Failed to run {} on table at '{}': {}",
                    operation, table_uri, detail
                ),
            },
        };
        ns_err.into()
    }

    async fn table_has_actual_manifests(&self, table_name: &str) -> Result<bool> {
        manifest::ManifestNamespace::path_has_actual_manifests(
            &self.object_store,
            &self.table_path(table_name),
        )
        .await
    }

    async fn filter_declared_tables(
        &self,
        tables: Vec<String>,
        include_declared: bool,
    ) -> Result<Vec<String>> {
        if include_declared {
            return Ok(tables);
        }

        let mut stream = futures::stream::iter(tables.into_iter().map(|table_name| async move {
            // `include_declared=false` is an explicit opt-in. We still pay one `_versions/` probe
            // per table here so declared-state is derived from actual manifests. This is linear in
            // the total number of listed tables, but we probe a bounded number concurrently.
            if self.table_has_actual_manifests(&table_name).await? {
                Ok::<Option<String>, Error>(Some(table_name))
            } else {
                Ok::<Option<String>, Error>(None)
            }
        }))
        .buffered(manifest::DECLARED_FILTER_CONCURRENCY);

        let mut filtered = Vec::new();
        while let Some(result) = stream.next().await {
            if let Some(table_name) = result? {
                filtered.push(table_name);
            }
        }
        Ok(filtered)
    }

    fn ipc_reader_from_request_data(
        request_data: &Bytes,
        operation: &str,
    ) -> Result<(
        Box<dyn arrow::record_batch::RecordBatchReader + Send>,
        usize,
    )> {
        if request_data.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Request data (Arrow IPC stream) is required for {}",
                    operation
                ),
            }
            .into());
        }

        let cursor = Cursor::new(request_data.as_ref());
        let stream_reader =
            StreamReader::try_new(cursor, None).map_err(|e| NamespaceError::InvalidInput {
                message: format!("Invalid Arrow IPC stream: {}", e),
            })?;
        let arrow_schema = stream_reader.schema();

        let mut num_rows = 0usize;
        let mut batches = Vec::new();
        for batch_result in stream_reader {
            let batch = batch_result.map_err(|e| NamespaceError::Internal {
                message: format!("Failed to read batch from IPC stream: {}", e),
            })?;
            num_rows += batch.num_rows();
            batches.push(batch);
        }

        let reader: Box<dyn arrow::record_batch::RecordBatchReader + Send> = if batches.is_empty() {
            let batch = arrow::record_batch::RecordBatch::new_empty(arrow_schema.clone());
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], arrow_schema))
        } else {
            let batch_results: Vec<_> = batches.into_iter().map(Ok).collect();
            Box::new(RecordBatchIterator::new(batch_results, arrow_schema))
        };

        Ok((reader, num_rows))
    }

    async fn table_uri_has_actual_manifests(&self, table_uri: &str) -> Result<bool> {
        let table_path = self.object_store_path_from_uri(table_uri)?;
        manifest::ManifestNamespace::path_has_actual_manifests(&self.object_store, &table_path)
            .await
    }

    fn object_store_path_from_uri(&self, uri: &str) -> Result<Path> {
        let registry = self
            .session
            .as_ref()
            .map(|session| session.store_registry())
            .unwrap_or_else(|| Arc::new(ObjectStoreRegistry::default()));
        ObjectStore::extract_path_from_uri(registry, uri)
    }

    /// Normalize and validate a branch selector: `None`, empty, and `main` mean
    /// the main branch; any other name is validated with lance's
    /// `check_valid_branch` (lance skips this on the open path) so it cannot
    /// escape the table root via `..`.
    fn normalized_branch(branch: Option<&str>) -> Result<Option<&str>> {
        match branch.filter(|b| !b.is_empty() && *b != "main") {
            Some(branch) => {
                check_valid_branch(branch).map_err(|e| {
                    lance_core::Error::from(NamespaceError::InvalidInput {
                        message: format!("invalid branch name '{}': {}", branch, e),
                    })
                })?;
                Ok(Some(branch))
            }
            None => Ok(None),
        }
    }

    async fn open_validated_branch(&self, table_uri: &str, branch: &str) -> Result<Dataset> {
        let dataset = self
            .configured_builder(table_uri)
            .with_branch(branch, None)
            .load()
            .await
            .map_err(|e| {
                let message = format!(
                    "branch '{}' not found for table at '{}': {}",
                    branch, table_uri, e
                );
                Self::map_open_error(e, NamespaceError::TableNotFound { message })
            })?;
        dataset.branches().get(branch).await.map_err(|e| {
            Self::map_open_error(
                e,
                NamespaceError::TableNotFound {
                    message: format!("branch '{}' not found for table at '{}'", branch, table_uri),
                },
            )
        })?;
        Ok(dataset)
    }

    async fn resolve_branch_location(&self, table_uri: &str, branch: &str) -> Result<String> {
        Ok(self
            .open_validated_branch(table_uri, branch)
            .await?
            .branch_location()
            .uri)
    }

    /// Resolves a branch to its `(uri, object-store path, parent_version)` for
    /// `create_table_version`.
    ///
    /// `BranchContents` is the source of truth, so check the ref first: a
    /// registered branch commits directly and returns its `parent_version` for
    /// empty-chain CAS. With no ref, accept the commit only on an empty chain
    /// (the `create_branch` bootstrap, whose first commit precedes its ref) and
    /// return `parent_version = None`; reject a chain that already holds
    /// committed versions as a zombie.
    async fn resolve_branch_for_commit(
        &self,
        table_uri: &str,
        branch: &str,
    ) -> Result<(String, Path, Option<u64>)> {
        let main = self
            .configured_builder(table_uri)
            .load()
            .await
            .map_err(|e| {
                let message = format!("table at '{}' not found: {}", table_uri, e);
                Self::map_open_error(e, NamespaceError::TableNotFound { message })
            })?;
        let branch_location = main.branch_location().find_branch(Some(branch))?;
        match main.branches().get(branch).await {
            Ok(contents) => Ok((
                branch_location.uri,
                branch_location.path,
                Some(contents.parent_version),
            )),
            Err(lance_core::Error::RefNotFound { .. }) => {
                if self
                    .branch_has_committed_versions(&branch_location.path)
                    .await?
                {
                    return Err(NamespaceError::TableNotFound {
                        message: format!(
                            "branch '{}' not found for table at '{}'",
                            branch, table_uri
                        ),
                    }
                    .into());
                }
                Ok((branch_location.uri, branch_location.path, None))
            }
            Err(e) => Err(e),
        }
    }

    async fn branch_has_committed_versions(&self, branch_path: &Path) -> Result<bool> {
        Ok(!self
            .list_versions_under(branch_path, false, Some(1))
            .await?
            .is_empty())
    }

    fn validate_dir_only_properties(
        properties: Option<&HashMap<String, String>>,
        operation: &str,
    ) -> Result<()> {
        // Dir-only mode has no metadata catalog, so non-empty table properties would be accepted
        // and then lost. Reject them instead. Request-level storage options are different: they
        // directly affect the current write and remain supported in dir-only mode.
        if properties.is_some_and(|properties| !properties.is_empty()) {
            return Err(NamespaceError::Unsupported {
                message: format!(
                    "{} with non-empty table properties requires manifest_enabled=true",
                    operation
                ),
            }
            .into());
        }
        Ok(())
    }

    async fn write_reader_to_table(
        &self,
        table_uri: &str,
        reader: Box<dyn arrow::record_batch::RecordBatchReader + Send>,
        mode: WriteMode,
        extra_storage_options: Option<HashMap<String, String>>,
    ) -> Result<Dataset> {
        // Insert and merge-insert request models do not carry request-level storage options,
        // so these writes intentionally use the namespace-level storage options only.
        let mut merged_storage_options = self.storage_options.clone().unwrap_or_default();
        if let Some(extra_storage_options) = extra_storage_options {
            merged_storage_options.extend(extra_storage_options);
        }
        let store_params = (!merged_storage_options.is_empty()).then(|| ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                lance_io::object_store::StorageOptionsAccessor::with_static_options(
                    merged_storage_options,
                ),
            )),
            ..Default::default()
        });

        let write_params = WriteParams {
            mode,
            store_params,
            session: self.session.clone(),
            ..Default::default()
        };

        let dataset = Dataset::write(reader, table_uri, Some(write_params))
            .await
            .map_err(|e| NamespaceError::Internal {
                message: format!("Failed to write table at '{}': {}", table_uri, e),
            })?;

        Ok(dataset)
    }

    /// Logical table version parsed from a manifest filename, or `None` for
    /// non-manifest / detached entries. Delegates to lance's scheme detection so
    /// version listing and deletion stay consistent with the on-disk format.
    fn manifest_version_from_filename(filename: &str) -> Option<u64> {
        ManifestNamingScheme::detect_scheme(filename)?.parse_version(filename)
    }

    /// Build a successful `CreateTableVersionResponse` from an existing final manifest.
    fn create_table_version_response(
        version: u64,
        final_path: &Path,
        final_meta: &ObjectMeta,
    ) -> CreateTableVersionResponse {
        CreateTableVersionResponse {
            transaction_id: None,
            version: Some(Box::new(TableVersion {
                version: version as i64,
                manifest_path: final_path.to_string(),
                manifest_size: Some(final_meta.size as i64),
                e_tag: final_meta.e_tag.clone(),
                timestamp_millis: None,
                metadata: None,
            })),
        }
    }

    /// Whether the staging blob matches the already-published version blob.
    ///
    /// Used for idempotent retries of `create_table_version`. Object-store
    /// `e_tag` is opaque metadata (not a validated content hash) and may also
    /// change across Create/rename materialize, so it is never used for
    /// identity. Size mismatch is a cheap negative check; byte equality is the
    /// durable success condition.
    async fn staging_matches_final_manifest(
        &self,
        staging_path: &Path,
        final_path: &Path,
        final_meta: &ObjectMeta,
        request_manifest_size: Option<i64>,
    ) -> Result<bool> {
        if let Some(size) = request_manifest_size
            && size != final_meta.size as i64
        {
            return Ok(false);
        }

        let staging_bytes = match self.object_store.inner.get(staging_path).await {
            Ok(r) => r.bytes().await.map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read staging manifest at '{}': {}",
                        staging_path, e
                    ),
                })
            })?,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(false),
            Err(e) => {
                return Err(lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read staging manifest at '{}': {}",
                        staging_path, e
                    ),
                }));
            }
        };

        let final_bytes = self
            .object_store
            .inner
            .get(final_path)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read existing version manifest at '{}': {}",
                        final_path, e
                    ),
                })
            })?
            .bytes()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read existing version manifest bytes at '{}': {}",
                        final_path, e
                    ),
                })
            })?;

        Ok(staging_bytes.as_ref() == final_bytes.as_ref())
    }

    /// Idempotent success or conflict when the target version path already exists.
    async fn resolve_existing_table_version(
        &self,
        args: ExistingTableVersionResolve<'_>,
    ) -> Result<CreateTableVersionResponse> {
        if self
            .staging_matches_final_manifest(
                args.staging_path,
                args.final_path,
                args.final_meta,
                args.request_manifest_size,
            )
            .await?
        {
            // Best-effort cleanup of a retry's staging blob.
            if let Err(e) = self.object_store.inner.delete(args.staging_path).await {
                log::warn!(
                    "Failed to delete staging manifest at '{}': {:?}",
                    args.staging_path,
                    e
                );
            }
            return Ok(Self::create_table_version_response(
                args.version,
                args.final_path,
                args.final_meta,
            ));
        }

        Err(lance_core::Error::from(
            NamespaceError::ConcurrentModification {
                message: format!(
                    "Version {} already exists for table at '{}' with different content",
                    args.version, args.table_uri
                ),
            },
        ))
    }

    /// Enforce version CAS: requested version must be `latest + 1` (or bootstrap).
    ///
    /// Empty-chain bootstrap:
    /// - main must start at v1
    /// - a registered branch must start at `BranchContents.parent_version` (the
    ///   shallow-clone fork version, which may be > 1)
    /// - an unregistered branch (create_branch phase-1, ref not written yet)
    ///   accepts the requested version because `parent_version` is not known yet
    async fn enforce_create_table_version_cas(
        &self,
        table_path: &Path,
        version: u64,
        table_uri: &str,
        is_branch: bool,
        branch_parent_version: Option<u64>,
    ) -> Result<()> {
        let latest = self.list_versions_under(table_path, true, Some(1)).await?;
        let expected = match latest.first() {
            Some(v) => (v.version as u64).checked_add(1).ok_or_else(|| {
                lance_core::Error::from(NamespaceError::ConcurrentModification {
                    message: format!(
                        "Version overflow computing next version for table at '{}': \
                             latest version {} cannot advance",
                        table_uri, v.version
                    ),
                })
            })?,
            None => {
                if is_branch {
                    // Prefer BranchContents.parent_version when the ref exists so a
                    // branch forked at v5 cannot bootstrap at an arbitrary version.
                    match branch_parent_version {
                        Some(parent_version) => parent_version,
                        None => version,
                    }
                } else {
                    1
                }
            }
        };
        if version != expected {
            let latest_display = latest
                .first()
                .map(|v| v.version.to_string())
                .unwrap_or_else(|| "none".to_string());
            return Err(lance_core::Error::from(
                NamespaceError::ConcurrentModification {
                    message: format!(
                        "Version CAS failed for table at '{}': requested {}, expected {} (latest {})",
                        table_uri, version, expected, latest_display
                    ),
                },
            ));
        }
        Ok(())
    }

    /// Materialize staging → final with Create semantics only (never overwrite).
    async fn materialize_version_manifest_create(
        &self,
        staging_path: &Path,
        final_path: &Path,
        staging_manifest_path: &str,
    ) -> std::result::Result<(), ObjectStoreError> {
        match self
            .object_store
            .inner
            .copy_if_not_exists(staging_path, final_path)
            .await
        {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::NotImplemented { .. })
            | Err(ObjectStoreError::NotSupported { .. }) => {
                let manifest_data = self
                    .object_store
                    .inner
                    .get(staging_path)
                    .await?
                    .bytes()
                    .await
                    .map_err(|e| ObjectStoreError::Generic {
                        store: "DirectoryNamespace",
                        source: Box::new(std::io::Error::other(format!(
                            "Failed to read staging manifest bytes at '{}': {}",
                            staging_manifest_path, e
                        ))),
                    })?;
                self.object_store
                    .inner
                    .put_opts(
                        final_path,
                        manifest_data.into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    async fn list_table_versions_from_storage(
        &self,
        table_uri: &str,
        descending: bool,
        limit: Option<i32>,
    ) -> Result<Vec<TableVersion>> {
        let table_path = self.object_store_path_from_uri(table_uri)?;
        self.list_versions_under(&table_path, descending, limit)
            .await
    }

    /// List committed manifest versions under `table_path/_versions/`.
    /// `table_path` must be an object-store `Path`; converting a URI to a path
    /// can miss manifests on Windows.
    async fn list_versions_under(
        &self,
        table_path: &Path,
        descending: bool,
        limit: Option<i32>,
    ) -> Result<Vec<TableVersion>> {
        let versions_dir = table_path.clone().join(VERSIONS_DIR);
        let manifest_metas: Vec<_> = self
            .object_store
            .read_dir_all(&versions_dir, None)
            .try_collect()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to list manifest files under '{}': {}",
                        versions_dir, e
                    ),
                })
            })?;

        let is_v2_naming = manifest_metas
            .first()
            .is_some_and(|meta| meta.location.filename().is_some_and(|f| f.len() == 29));

        let mut table_versions: Vec<TableVersion> = manifest_metas
            .into_iter()
            .filter_map(|meta| {
                let filename = meta.location.filename()?;
                let actual_version = Self::manifest_version_from_filename(filename)?;

                Some(TableVersion {
                    version: actual_version as i64,
                    manifest_path: meta.location.to_string(),
                    manifest_size: Some(meta.size as i64),
                    e_tag: meta.e_tag,
                    timestamp_millis: Some(meta.last_modified.timestamp_millis()),
                    metadata: None,
                })
            })
            .collect();

        let list_is_ordered = self.object_store.list_is_lexically_ordered;

        let needs_sort = if list_is_ordered {
            if is_v2_naming {
                !descending
            } else {
                descending
            }
        } else {
            true
        };

        if needs_sort {
            if descending {
                table_versions.sort_by_key(|v| std::cmp::Reverse(v.version));
            } else {
                table_versions.sort_by_key(|v| v.version);
            }
        }

        if let Some(limit) = limit {
            table_versions.truncate(limit as usize);
        }

        Ok(table_versions)
    }

    /// Internal describe_table implementation that doesn't record metrics.
    /// Used by both the public describe_table (which records metrics) and
    /// internal callers like resolve_table_location (which shouldn't).
    async fn describe_table_impl(
        &self,
        request: DescribeTableRequest,
    ) -> Result<DescribeTableResponse> {
        let is_root_level = request.id.as_ref().is_some_and(|id| id.len() == 1);
        let is_child_table = request.id.as_ref().is_some_and(|id| id.len() > 1);
        let skip_manifest_for_root = self.dir_listing_enabled
            && is_root_level
            && !self.dir_listing_to_manifest_migration_enabled;
        // Self-heal the manifest wherever it can be authoritative: a child table
        // (no dir-listing fallback), a manifest-only namespace, or migration mode
        // (manifest-first -- it can hold registered_table -> external .lance
        // aliases that dir-listing cannot resolve, so a reader built before
        // __manifest must re-probe to see them). The bypass -- skipping the probe
        // -- applies ONLY to migration-disabled directory-backed root reads,
        // which are served entirely from the directory listing.
        if is_child_table
            || !self.dir_listing_enabled
            || self.dir_listing_to_manifest_migration_enabled
        {
            self.ensure_read_manifest().await?;
        }
        if let Some(manifest_ns) = self.manifest_ns_for_read()
            && !skip_manifest_for_root
        {
            match manifest_ns.describe_table(request.clone()).await {
                Ok(mut response) => {
                    if let Some(ref table_uri) = response.table_uri {
                        // For backwards compatibility, only skip vending credentials when explicitly set to false
                        let vend = request.vend_credentials.unwrap_or(true);
                        let identity = request.identity.as_deref();
                        response.storage_options = self
                            .get_storage_options_for_table(table_uri, vend, identity)
                            .await?;
                    }
                    // Set managed_versioning flag when table_version_tracking_enabled
                    if self.table_version_tracking_enabled {
                        response.managed_versioning = Some(true);
                    }
                    return Ok(response);
                }
                Err(e) if manifest_feature_flags::is_incompatible_manifest_error(&e) => {
                    // An incompatible manifest must surface "please upgrade"
                    // rather than degrading to a directory-listing view.
                    return Err(e);
                }
                Err(e) if self.dir_listing_enabled && is_root_level => {
                    // Only a genuinely-absent table (e.g. an unmigrated on-disk
                    // table) may fall through to the directory check; any other
                    // manifest error must propagate rather than be read as missing.
                    if !Self::is_manifest_table_absent_error(&e) {
                        return Err(Self::classify_storage_error(e));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if is_child_table {
            return Err(self.child_namespace_requires_manifest_error());
        }

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_id = Self::format_table_id_from_request(&request.id);
        if !self.dir_listing_enabled {
            return Err(NamespaceError::TableNotFound { message: table_id }.into());
        }

        let table_uri = self.table_full_uri(&table_name);

        // Atomically check table existence and deregistration status
        let status = self.check_table_status(&table_name).await?;

        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_id.clone(),
            }
            .into());
        }

        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is deregistered: {}", table_id),
            }
            .into());
        }

        let load_detailed_metadata = request.load_detailed_metadata.unwrap_or(false);
        let should_check_declared =
            load_detailed_metadata || request.check_declared.unwrap_or(false);
        // For backwards compatibility, only skip vending credentials when explicitly set to false
        let vend_credentials = request.vend_credentials.unwrap_or(true);
        let identity = request.identity.as_deref();
        let is_only_declared = if should_check_declared {
            if status.has_reserved_file {
                Some(!self.table_has_actual_manifests(&table_name).await?)
            } else {
                Some(false)
            }
        } else {
            None
        };

        if !load_detailed_metadata {
            let storage_options = self
                .get_storage_options_for_table(&table_uri, vend_credentials, identity)
                .await?;
            return Ok(DescribeTableResponse {
                table: Some(table_name),
                namespace: request.id.as_ref().map(|id| {
                    if id.len() > 1 {
                        id[..id.len() - 1].to_vec()
                    } else {
                        vec![]
                    }
                }),
                location: Some(table_uri.clone()),
                table_uri: Some(table_uri),
                storage_options,
                is_only_declared,
                managed_versioning: if self.table_version_tracking_enabled {
                    Some(true)
                } else {
                    None
                },
                ..Default::default()
            });
        }

        if is_only_declared == Some(true) {
            let storage_options = self
                .get_storage_options_for_table(&table_uri, vend_credentials, identity)
                .await?;
            return Ok(DescribeTableResponse {
                table: Some(table_name),
                namespace: request.id.as_ref().map(|id| {
                    if id.len() > 1 {
                        id[..id.len() - 1].to_vec()
                    } else {
                        vec![]
                    }
                }),
                location: Some(table_uri.clone()),
                table_uri: Some(table_uri),
                storage_options,
                is_only_declared,
                managed_versioning: if self.table_version_tracking_enabled {
                    Some(true)
                } else {
                    None
                },
                ..Default::default()
            });
        }

        // Try to load the dataset to get real information
        // Use DatasetBuilder with storage options to support S3 with custom endpoints
        let mut builder = DatasetBuilder::from_uri(&table_uri);
        if let Some(opts) = &self.storage_options {
            builder = builder.with_storage_options(opts.clone());
        }
        if let Some(sess) = &self.session {
            builder = builder.with_session(sess.clone());
        }
        match builder.load().await {
            Ok(mut dataset) => {
                // If a specific version is requested, checkout that version
                if let Some(requested_version) = request.version {
                    dataset = dataset
                        .checkout_version(requested_version as u64)
                        .await
                        .map_err(|e| {
                            let message = format!(
                                "Version {} not found for table '{}': {}",
                                requested_version, table_name, e
                            );
                            Self::map_open_error(
                                e,
                                NamespaceError::TableVersionNotFound { message },
                            )
                        })?;
                }

                let version_info = dataset.version();
                let lance_schema = dataset.schema();
                let arrow_schema: arrow_schema::Schema = lance_schema.into();
                let json_schema = arrow_schema_to_json(&arrow_schema)?;
                let storage_options = self
                    .get_storage_options_for_table(&table_uri, vend_credentials, identity)
                    .await?;

                // Convert BTreeMap to HashMap for the response
                let metadata: std::collections::HashMap<String, String> =
                    version_info.metadata.into_iter().collect();

                Ok(DescribeTableResponse {
                    table: Some(table_name),
                    namespace: request.id.as_ref().map(|id| {
                        if id.len() > 1 {
                            id[..id.len() - 1].to_vec()
                        } else {
                            vec![]
                        }
                    }),
                    version: Some(version_info.version as i64),
                    location: Some(table_uri.clone()),
                    table_uri: Some(table_uri),
                    schema: Some(Box::new(json_schema)),
                    storage_options,
                    metadata: Some(metadata),
                    is_only_declared,
                    managed_versioning: if self.table_version_tracking_enabled {
                        Some(true)
                    } else {
                        None
                    },
                    ..Default::default()
                })
            }
            Err(err) => {
                if manifest::ManifestNamespace::is_not_found_load_error(&err)
                    && is_only_declared == Some(true)
                {
                    let storage_options = self
                        .get_storage_options_for_table(&table_uri, vend_credentials, identity)
                        .await?;
                    Ok(DescribeTableResponse {
                        table: Some(table_name),
                        namespace: request.id.as_ref().map(|id| {
                            if id.len() > 1 {
                                id[..id.len() - 1].to_vec()
                            } else {
                                vec![]
                            }
                        }),
                        location: Some(table_uri.clone()),
                        table_uri: Some(table_uri),
                        storage_options,
                        is_only_declared,
                        managed_versioning: if self.table_version_tracking_enabled {
                            Some(true)
                        } else {
                            None
                        },
                        ..Default::default()
                    })
                } else {
                    Err(NamespaceError::Internal {
                        message: format!(
                            "Table directory exists but cannot load dataset {}: {:?}",
                            table_name, err
                        ),
                    }
                    .into())
                }
            }
        }
    }

    /// Build a `DatasetBuilder` for `table_uri` with this namespace's storage
    /// options and session applied. Callers add version/branch scoping.
    fn configured_builder(&self, table_uri: &str) -> DatasetBuilder {
        let mut builder = DatasetBuilder::from_uri(table_uri);
        if let Some(opts) = &self.storage_options {
            builder = builder.with_storage_options(opts.clone());
        }
        if let Some(sess) = &self.session {
            builder = builder.with_session(sess.clone());
        }
        builder
    }

    async fn load_dataset(
        &self,
        table_uri: &str,
        version: Option<i64>,
        operation: &str,
    ) -> Result<Dataset> {
        if let Some(version) = version
            && version < 0
        {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Table version for {} must be non-negative, got {}",
                    operation, version
                ),
            }
            .into());
        }

        let builder = self.configured_builder(table_uri);

        let dataset = builder.load().await.map_err(|e| {
            let message = format!(
                "Failed to open table at '{}' for {}: {}",
                table_uri, operation, e
            );
            Self::map_open_error(e, NamespaceError::TableNotFound { message })
        })?;

        if let Some(version) = version {
            return dataset.checkout_version(version as u64).await.map_err(|e| {
                let message = format!(
                    "Failed to checkout version {} for table at '{}' during {}: {}",
                    version, table_uri, operation, e
                );
                Self::map_open_error(e, NamespaceError::TableVersionNotFound { message })
            });
        }

        Ok(dataset)
    }

    fn parse_index_type(index_type: &str) -> Result<IndexType> {
        match index_type.trim().to_ascii_uppercase().as_str() {
            "SCALAR" | "BTREE" => Ok(IndexType::BTree),
            "BITMAP" => Ok(IndexType::Bitmap),
            "LABEL_LIST" | "LABELLIST" => Ok(IndexType::LabelList),
            "INVERTED" | "FTS" => Ok(IndexType::Inverted),
            "NGRAM" => Ok(IndexType::NGram),
            "ZONEMAP" | "ZONE_MAP" => Ok(IndexType::ZoneMap),
            "BLOOMFILTER" | "BLOOM_FILTER" => Ok(IndexType::BloomFilter),
            "RTREE" | "R_TREE" => Ok(IndexType::RTree),
            "VECTOR" | "IVF_PQ" => Ok(IndexType::IvfPq),
            "IVF_FLAT" => Ok(IndexType::IvfFlat),
            "IVF_SQ" => Ok(IndexType::IvfSq),
            "IVF_RQ" => Ok(IndexType::IvfRq),
            "IVF_HNSW_FLAT" => Ok(IndexType::IvfHnswFlat),
            "IVF_HNSW_SQ" => Ok(IndexType::IvfHnswSq),
            "IVF_HNSW_PQ" => Ok(IndexType::IvfHnswPq),
            other => Err(NamespaceError::InvalidInput {
                message: format!("Unsupported index_type '{}'", other),
            }
            .into()),
        }
    }

    fn parse_metric_type(distance_type: Option<&str>) -> Result<MetricType> {
        let distance_type = distance_type.unwrap_or("l2");
        MetricType::try_from(distance_type).map_err(|e| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: format!(
                    "Unsupported distance_type '{}' for vector index: {}",
                    distance_type, e
                ),
            })
        })
    }

    fn build_index_params(request: &CreateTableIndexRequest) -> Result<DirectoryIndexParams> {
        let index_type = Self::parse_index_type(&request.index_type)?;
        Ok(match index_type {
            IndexType::BTree => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
            },
            IndexType::Bitmap => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap),
            },
            IndexType::LabelList => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::LabelList),
            },
            IndexType::NGram => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::NGram),
            },
            IndexType::ZoneMap => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap),
            },
            IndexType::BloomFilter => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter),
            },
            IndexType::RTree => DirectoryIndexParams::Scalar {
                index_type,
                params: ScalarIndexParams::for_builtin(BuiltinIndexType::RTree),
            },
            IndexType::Inverted => {
                let mut params = InvertedIndexParams::default();
                if let Some(with_position) = request.with_position {
                    params = params.with_position(with_position);
                }
                if let Some(base_tokenizer) = &request.base_tokenizer {
                    params = params.base_tokenizer(base_tokenizer.clone());
                }
                if let Some(language) = &request.language {
                    params = params.language(language)?;
                }
                if let Some(max_token_length) = request.max_token_length {
                    if max_token_length < 0 {
                        return Err(NamespaceError::InvalidInput {
                            message: format!(
                                "FTS max_token_length must be non-negative, got {}",
                                max_token_length
                            ),
                        }
                        .into());
                    }
                    params = params.max_token_length(Some(max_token_length as usize));
                }
                if let Some(lower_case) = request.lower_case {
                    params = params.lower_case(lower_case);
                }
                if let Some(stem) = request.stem {
                    params = params.stem(stem);
                }
                if let Some(remove_stop_words) = request.remove_stop_words {
                    params = params.remove_stop_words(remove_stop_words);
                }
                if let Some(ascii_folding) = request.ascii_folding {
                    params = params.ascii_folding(ascii_folding);
                }
                DirectoryIndexParams::Inverted(params)
            }
            IndexType::IvfFlat => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_flat_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                ),
            },
            IndexType::IvfPq => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_pq_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    PQBuildParams::default(),
                ),
            },
            IndexType::IvfSq => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_sq_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    SQBuildParams::default(),
                ),
            },
            IndexType::IvfRq => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_rq_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    RQBuildParams::default(),
                ),
            },
            IndexType::IvfHnswFlat => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::ivf_hnsw(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    HnswBuildParams::default(),
                ),
            },
            IndexType::IvfHnswSq => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_hnsw_sq_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    HnswBuildParams::default(),
                    SQBuildParams::default(),
                ),
            },
            IndexType::IvfHnswPq => DirectoryIndexParams::Vector {
                index_type,
                params: VectorIndexParams::with_ivf_hnsw_pq_params(
                    Self::parse_metric_type(request.distance_type.as_deref())?,
                    IvfBuildParams::default(),
                    HnswBuildParams::default(),
                    PQBuildParams::default(),
                ),
            },
            other => {
                return Err(NamespaceError::InvalidInput {
                    message: format!("Unsupported index type for namespace API: {}", other),
                }
                .into());
            }
        })
    }

    fn paginate_indices(
        indices: &mut Vec<IndexContent>,
        page_token: Option<String>,
        limit: Option<i32>,
    ) -> Option<String> {
        indices.sort_by(|a, b| a.index_name.cmp(&b.index_name));

        if let Some(start_after) = page_token {
            if let Some(index) = indices
                .iter()
                .position(|index| index.index_name.as_str() > start_after.as_str())
            {
                indices.drain(0..index);
            } else {
                indices.clear();
            }
        }

        let mut next_page_token = None;
        if let Some(limit) = limit
            && limit >= 0
        {
            let limit = limit as usize;
            if limit > 0 && indices.len() > limit {
                next_page_token = Some(indices[limit - 1].index_name.clone());
            }
            indices.truncate(limit);
        }
        if indices.is_empty() {
            None
        } else {
            next_page_token
        }
    }

    fn transaction_operation_name(transaction: &Transaction) -> String {
        match &transaction.operation {
            Operation::CreateIndex {
                new_indices,
                removed_indices,
            } if new_indices.is_empty() && !removed_indices.is_empty() => "DropIndex".to_string(),
            _ => transaction.operation.to_string(),
        }
    }

    fn transaction_response(
        version: u64,
        transaction: &Transaction,
        alteration: Option<TransactionAlteration>,
    ) -> DescribeTransactionResponse {
        let mut properties = transaction
            .transaction_properties
            .as_ref()
            .map(|properties| (**properties).clone())
            .unwrap_or_default();

        // Apply persisted alterations on top of the immutable transaction
        // properties so callers see the current effective state.
        let mut effective_status = "SUCCEEDED".to_string();
        if let Some(alteration) = alteration {
            for key in &alteration.removed_properties {
                properties.remove(key);
            }
            for (key, value) in alteration.properties {
                properties.insert(key, value);
            }
            if let Some(status) = alteration.status {
                effective_status = status;
            }
        }

        properties.insert("uuid".to_string(), transaction.uuid.clone());
        properties.insert("version".to_string(), version.to_string());
        properties.insert(
            "read_version".to_string(),
            transaction.read_version.to_string(),
        );
        properties.insert(
            "operation".to_string(),
            Self::transaction_operation_name(transaction),
        );
        if let Some(tag) = &transaction.tag {
            properties.insert("tag".to_string(), tag.clone());
        }

        DescribeTransactionResponse {
            status: effective_status,
            properties: Some(properties),
        }
    }

    fn describe_table_index_stats_response(
        stats: &serde_json::Value,
    ) -> DescribeTableIndexStatsResponse {
        let get_i64 = |key: &str| {
            stats.get(key).and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
            })
        };

        DescribeTableIndexStatsResponse {
            distance_type: stats
                .get("distance_type")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            index_type: stats
                .get("index_type")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            num_indexed_rows: get_i64("num_indexed_rows"),
            num_unindexed_rows: get_i64("num_unindexed_rows"),
            num_indices: get_i64("num_indices").and_then(|value| i32::try_from(value).ok()),
        }
    }

    /// When transaction_id is not parseable as a version number (i.e. it's a UUID),
    /// find_transaction iterates through every version in reverse, reading each
    /// transaction file from storage. For tables with many versions this will
    /// be extremely slow — each iteration is a separate I/O call.
    async fn find_transaction(&self, dataset: &Dataset, id: &str) -> Result<(u64, Transaction)> {
        if let Ok(version) = id.parse::<u64>() {
            let transaction = dataset
                .read_transaction_by_version(version)
                .await
                .map_err(|e| {
                    lance_core::Error::from(NamespaceError::TransactionNotFound {
                        message: format!(
                            "Failed to read transaction for version {}: {}",
                            version, e
                        ),
                    })
                })?
                .ok_or_else(|| {
                    lance_core::Error::from(NamespaceError::TransactionNotFound {
                        message: format!("version {}", version),
                    })
                })?;
            return Ok((version, transaction));
        }

        let versions = dataset.versions().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to list table versions while resolving transaction '{}': {}",
                    id, e
                ),
            })
        })?;

        for version in versions.into_iter().rev() {
            if let Some(transaction) = dataset
                .read_transaction_by_version(version.version)
                .await
                .map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Failed to read transaction for version {} while resolving '{}': {}",
                            version.version, id, e
                        ),
                    })
                })?
                && transaction.uuid == id
            {
                return Ok((version.version, transaction));
            }
        }

        Err(NamespaceError::TransactionNotFound {
            message: id.to_string(),
        }
        .into())
    }

    /// Relative directory (under a table's Lance root) used to persist
    /// alter_transaction outcomes. The Lance transaction file itself is
    /// immutable, so we keep alterations in a namespace-owned sidecar.
    const TRANSACTION_ALTERATIONS_DIR: &'static str = "_alter_transactions";

    fn transaction_alteration_path(&self, table_uri: &str, txn_uuid: &str) -> Result<Path> {
        let table_path = self.object_store_path_from_uri(table_uri)?;
        Ok(table_path
            .join(Self::TRANSACTION_ALTERATIONS_DIR)
            .join(format!("{}.json", txn_uuid).as_str()))
    }

    async fn load_transaction_alteration(
        &self,
        table_uri: &str,
        txn_uuid: &str,
    ) -> Result<Option<TransactionAlteration>> {
        let path = self.transaction_alteration_path(table_uri, txn_uuid)?;
        match self.object_store.inner.get(&path).await {
            Ok(get_result) => {
                let bytes = get_result.bytes().await.map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Failed to read alter_transaction sidecar for '{}': {}",
                            txn_uuid, e
                        ),
                    })
                })?;
                let alteration = TransactionAlteration::from_json_slice(&bytes).map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Failed to parse alter_transaction sidecar for '{}': {}",
                            txn_uuid, e
                        ),
                    })
                })?;
                Ok(Some(alteration))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to load alter_transaction sidecar for '{}': {}",
                    txn_uuid, e
                ),
            })),
        }
    }

    async fn save_transaction_alteration(
        &self,
        table_uri: &str,
        txn_uuid: &str,
        alteration: &TransactionAlteration,
    ) -> Result<()> {
        let path = self.transaction_alteration_path(table_uri, txn_uuid)?;
        let bytes = alteration.to_json_bytes().map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to serialize alter_transaction sidecar for '{}': {}",
                    txn_uuid, e
                ),
            })
        })?;
        self.object_store
            .inner
            .put(&path, bytes.into())
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to persist alter_transaction sidecar for '{}': {}",
                        txn_uuid, e
                    ),
                })
            })?;
        Ok(())
    }

    fn table_full_uri(&self, table_name: &str) -> String {
        format!("{}/{}.lance", self.root, table_name)
    }

    /// Get the object store path for a table (relative to base_path)
    fn table_path(&self, table_name: &str) -> Path {
        self.base_path
            .clone()
            .join(format!("{}.lance", table_name).as_str())
    }

    /// Get the reserved file path for a table
    fn table_reserved_file_path(&self, table_name: &str) -> Path {
        self.base_path
            .clone()
            .join(format!("{}.lance", table_name).as_str())
            .join(".lance-reserved")
    }

    /// Get the deregistered marker file path for a table
    fn table_deregistered_file_path(&self, table_name: &str) -> Path {
        self.base_path
            .clone()
            .join(format!("{}.lance", table_name).as_str())
            .join(".lance-deregistered")
    }

    /// Atomically check table existence and deregistration status.
    ///
    /// This performs a single directory listing to get a consistent snapshot of the
    /// table's state, avoiding race conditions between checking existence and
    /// checking deregistration status.
    pub(crate) async fn check_table_status(&self, table_name: &str) -> Result<TableStatus> {
        let table_path = self.table_path(table_name);
        match self.object_store.read_dir(table_path).await {
            Ok(entries) => {
                let exists = !entries.is_empty();
                let is_deregistered = entries.iter().any(|e| e.ends_with(".lance-deregistered"));
                let has_reserved_file = entries.iter().any(|e| e.ends_with(".lance-reserved"));
                Ok(TableStatus {
                    exists,
                    is_deregistered,
                    has_reserved_file,
                })
            }
            // Local filesystems error on a missing directory where object stores
            // return an empty listing; both mean the table does not exist.
            Err(e) if manifest::ManifestNamespace::is_not_found_load_error(&e) => Ok(TableStatus {
                exists: false,
                is_deregistered: false,
                has_reserved_file: false,
            }),
            // Any other failure must propagate: collapsing it to "does not exist"
            // lets a transient error overwrite a live table via create/exist-ok
            // callers and destroys the retry evidence classifiers depend on.
            Err(e) => Err(Self::classify_storage_error(e)),
        }
    }

    /// Classify a storage error into a typed [`NamespaceError`]. The full source
    /// text is embedded in the message because the pyo3 layer flattens namespace
    /// errors to message-only (no `__cause__`), so that is the only place the
    /// 429/503 evidence survives to Python.
    fn classify_storage_error(err: Error) -> Error {
        if matches!(&err, Error::Namespace { .. }) {
            return err;
        }
        let detail = err.to_string();
        if let Error::IO { source, .. } = &err
            && let Some(os_err) = source.downcast_ref::<ObjectStoreError>()
        {
            if is_throttle_error(os_err) {
                return NamespaceError::Throttling {
                    message: format!(
                        "Storage request was throttled while resolving table: {detail}"
                    ),
                }
                .into();
            }
            if Self::is_service_unavailable_error(os_err) {
                return NamespaceError::ServiceUnavailable {
                    message: format!("Storage service unavailable while resolving table: {detail}"),
                }
                .into();
            }
        }
        NamespaceError::Internal {
            message: format!("Storage error while resolving table: {detail}"),
        }
        .into()
    }

    /// Detect a clearly-transient 5xx not already caught by [`is_throttle_error`].
    /// `object_store` does not expose HTTP status codes, so match the (deliberately
    /// narrow) canonical status phrases in the message.
    fn is_service_unavailable_error(err: &ObjectStoreError) -> bool {
        if let ObjectStoreError::Generic { source, .. } = err {
            let message = source.to_string().to_ascii_lowercase();
            message.contains("503 service unavailable")
                || message.contains("502 bad gateway")
                || message.contains("504 gateway timeout")
        } else {
            false
        }
    }

    /// Whether a manifest error means the table is genuinely absent (rather than a
    /// storage failure while consulting the manifest). Only such errors may fall
    /// through to the directory listing; anything else must propagate.
    fn is_manifest_table_absent_error(err: &Error) -> bool {
        if manifest::ManifestNamespace::is_not_found_load_error(err) {
            return true;
        }
        if let Error::Namespace { source, .. } = err
            && let Some(ns_err) = source.downcast_ref::<NamespaceError>()
        {
            return matches!(ns_err, NamespaceError::TableNotFound { .. });
        }
        false
    }

    /// Map a dataset/version/branch open error: a transient IO error propagates
    /// typed via [`classify_storage_error`], while a genuine not-found (missing
    /// dataset, version, or ref) keeps the caller's `not_found` variant.
    fn map_open_error(err: Error, not_found: NamespaceError) -> Error {
        if matches!(&err, Error::IO { .. })
            && !manifest::ManifestNamespace::is_not_found_load_error(&err)
        {
            return Self::classify_storage_error(err);
        }
        not_found.into()
    }

    /// Get storage options for a table, using credential vending if configured.
    ///
    /// If credential vendor properties are configured and the table location matches
    /// a supported cloud provider, this will create an appropriate vendor and vend
    /// temporary credentials scoped to the table location. Otherwise, returns the
    /// static storage options.
    ///
    /// The vendor type is auto-selected based on the table URI:
    /// - `s3://` locations use AWS STS AssumeRole
    /// - `gs://` locations use GCP OAuth2 tokens
    /// - `az://` locations use Azure SAS tokens
    ///
    /// The permission level (Read, Write, Admin) is configured at namespace
    /// initialization time via the `credential_vendor_permission` property.
    ///
    /// # Arguments
    ///
    /// * `table_uri` - The full URI of the table
    /// * `identity` - Optional identity from the request for identity-based credential vending
    async fn get_storage_options_for_table(
        &self,
        table_uri: &str,
        vend_credentials: bool,
        identity: Option<&Identity>,
    ) -> Result<Option<HashMap<String, String>>> {
        if vend_credentials && let Some(ref vendor) = self.credential_vendor {
            let vended = vendor.vend_credentials(table_uri, identity).await?;
            return Ok(Some(vended.storage_options));
        }
        // When vend_input_storage_options is enabled and no credential vendor is configured,
        // return the input storage options. This is useful for testing.
        if self.vend_input_storage_options {
            let mut options = self.storage_options.clone().unwrap_or_default();
            // Add expires_at_millis if refresh interval is configured
            if let Some(refresh_interval_millis) =
                self.vend_input_storage_options_refresh_interval_millis
            {
                let now_millis = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expires_at_millis = now_millis + refresh_interval_millis;
                options.insert(
                    "expires_at_millis".to_string(),
                    expires_at_millis.to_string(),
                );
            }
            return Ok(Some(options));
        }
        // When no credential vendor is configured, return None to avoid
        // leaking the namespace's own static credentials to clients.
        Ok(None)
    }

    /// Migrate directory-based tables to the manifest.
    ///
    /// This is a one-time migration operation that:
    /// 1. Scans the directory for existing `.lance` tables
    /// 2. Registers any unmigrated tables in the manifest
    /// 3. Returns the count of tables that were migrated
    ///
    /// This method is safe to run multiple times - it will skip tables that are already
    /// registered in the manifest.
    ///
    /// # Usage
    ///
    /// After creating tables in directory-only mode or dual mode, you can migrate them
    /// to the manifest to enable manifest-only mode:
    ///
    /// ```no_run
    /// #![recursion_limit = "256"]
    /// # use lance_namespace_impls::DirectoryNamespaceBuilder;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Create namespace with dual mode (manifest + directory listing)
    /// let namespace = DirectoryNamespaceBuilder::new("/path/to/data")
    ///     .manifest_enabled(true)
    ///     .dir_listing_enabled(true)
    ///     .build()
    ///     .await?;
    ///
    /// // ... tables are created and used ...
    ///
    /// // Migrate existing directory tables to manifest
    /// let migrated_count = namespace.migrate().await?;
    /// println!("Migrated {} tables", migrated_count);
    ///
    /// // Now you can disable directory listing for better performance:
    /// // (requires rebuilding the namespace)
    /// let namespace = DirectoryNamespaceBuilder::new("/path/to/data")
    ///     .manifest_enabled(true)
    ///     .dir_listing_enabled(false)  // All tables now in manifest
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Returns
    ///
    /// Returns the number of tables that were migrated to the manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Manifest is not enabled
    /// - Directory listing fails
    /// - Manifest registration fails
    pub async fn migrate(&self) -> Result<usize> {
        // We only care about tables in the root namespace
        let Some(manifest_ns) = self.manifest_ns_for_write().await? else {
            return Ok(0); // No manifest, nothing to migrate
        };

        // Get all table locations already in the manifest
        let manifest_locations = manifest_ns.list_manifest_table_locations().await?;

        // Get all tables from directory and skip declared-only tables that have not
        // written any actual version manifests yet.
        let dir_tables = self
            .filter_declared_tables(self.list_directory_tables().await?, false)
            .await?;

        // Register each directory table that doesn't have an overlapping location
        // If a directory name already exists in the manifest,
        // that means the table must have already been migrated or created
        // in the manifest, so we can skip it.
        let mut migrated_count = 0;
        for table_name in dir_tables {
            // For root namespace tables, the directory name is "table_name.lance"
            let dir_name = format!("{}.lance", table_name);
            if !manifest_locations.contains(&dir_name) {
                manifest_ns.register_table(&table_name, dir_name).await?;
                migrated_count += 1;
            }
        }

        Ok(migrated_count)
    }

    /// Delete physical manifest files for the given table version ranges.
    ///
    /// This helper backs `batch_delete_table_versions`. It resolves each table's storage
    /// location, computes the version file paths, and deletes them, returning an error on
    /// the first failure.
    ///
    /// Returns the number of files successfully deleted.
    async fn delete_physical_version_files(
        &self,
        table_entries: &[TableDeleteEntry],
        branch: Option<&str>,
    ) -> Result<i64> {
        let mut deleted_count = 0i64;
        for te in table_entries {
            let table_uri = self.resolve_table_location(&te.table_id).await?;
            let table_uri = match branch {
                Some(b) => self.resolve_branch_location(&table_uri, b).await?,
                None => table_uri,
            };
            let table_path = self.object_store_path_from_uri(&table_uri)?;
            let versions_dir_path = table_path.clone().join(VERSIONS_DIR);

            // Match listed files, not constructed names (`{version}.manifest` misses V2).
            let manifest_metas: Vec<_> = self
                .object_store
                .read_dir_all(&versions_dir_path, None)
                .try_collect()
                .await
                .map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Failed to list manifest files for table at '{}': {}",
                            table_uri, e
                        ),
                    })
                })?;
            let location_by_version: HashMap<u64, Path> = manifest_metas
                .into_iter()
                .filter_map(|meta| {
                    let version = Self::manifest_version_from_filename(meta.location.filename()?)?;
                    Some((version, meta.location))
                })
                .collect();

            for (&v, version_path) in &location_by_version {
                let vi = v as i64;
                if !te.ranges.iter().any(|&(s, e)| vi >= s && (e < 0 || vi < e)) {
                    continue;
                }
                match self.object_store.inner.delete(version_path).await {
                    Ok(_) => {
                        deleted_count += 1;
                    }
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        return Err(NamespaceError::Internal {
                            message: format!(
                                "Failed to delete version {} for table at '{}': {}",
                                v, table_uri, e
                            ),
                        }
                        .into());
                    }
                }
            }
        }
        Ok(deleted_count)
    }

    /// Apply all query parameters from a `QueryTableRequest`-like source onto a `Scanner`.
    ///
    /// This covers vector search, filters, column projection, limits, and ANN tuning knobs so
    /// that `explain_table_query_plan` / `analyze_table_query_plan` produce an accurate plan.
    #[allow(clippy::too_many_arguments)]
    fn apply_query_params_to_scanner(
        scanner: &mut Scanner,
        filter: Option<&str>,
        columns: Option<&QueryTableRequestColumns>,
        vector_column: Option<&str>,
        vector: &QueryTableRequestVector,
        k: i32,
        offset: Option<i32>,
        prefilter: Option<bool>,
        bypass_vector_index: Option<bool>,
        nprobes: Option<i32>,
        ef: Option<i32>,
        refine_factor: Option<i32>,
        distance_type: Option<&str>,
        fast_search_flag: Option<bool>,
        with_row_id: Option<bool>,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        operation: &str,
    ) -> Result<()> {
        // prefilter must be set before nearest() so the fragment-scan guard sees it.
        if let Some(pf) = prefilter {
            scanner.prefilter(pf);
        }

        if let Some(filter) = filter {
            scanner.filter(filter).map_err(|e| {
                Error::invalid_input_source(
                    format!("Invalid filter expression for {}: {}", operation, e).into(),
                )
            })?;
        }

        if let Some(cols) = columns {
            if let Some(ref names) = cols.column_names {
                scanner.project(names.as_slice()).map_err(|e| {
                    Error::invalid_input_source(
                        format!("Invalid column projection for {}: {}", operation, e).into(),
                    )
                })?;
            } else if let Some(ref aliases) = cols.column_aliases {
                // aliases maps output_alias -> source_column
                let pairs: Vec<(&str, &str)> = aliases
                    .iter()
                    .map(|(alias, src)| (alias.as_str(), src.as_str()))
                    .collect();
                scanner.project_with_transform(&pairs).map_err(|e| {
                    Error::invalid_input_source(
                        format!("Invalid column aliases for {}: {}", operation, e).into(),
                    )
                })?;
            }
        }

        // Resolve query vector: prefer single_vector, fall back to first row of multi_vector.
        let query_vec: Option<Vec<f32>> = vector
            .single_vector
            .as_ref()
            .filter(|v| !v.is_empty())
            .cloned()
            .or_else(|| {
                vector
                    .multi_vector
                    .as_ref()
                    .and_then(|mv| mv.first())
                    .filter(|v| !v.is_empty())
                    .cloned()
            });

        if let Some(q_vec) = query_vec {
            let col = vector_column.unwrap_or("vector");
            let q = Arc::new(Float32Array::from(q_vec));
            scanner
                .nearest(col, q.as_ref(), k.max(1) as usize)
                .map_err(|e| {
                    Error::invalid_input_source(
                        format!("Invalid vector query for {}: {}", operation, e).into(),
                    )
                })?;

            // ANN parameters — must be applied after nearest().
            if let Some(n) = nprobes {
                scanner.nprobes(n.max(1) as usize);
            }
            if let Some(e) = ef {
                scanner.ef(e.max(1) as usize);
            }
            if let Some(rf) = refine_factor {
                scanner.refine(rf.max(0) as u32);
            }
            // bypass_vector_index and fast_search are mutually exclusive; apply in order.
            if let Some(true) = bypass_vector_index {
                scanner.use_index(false);
            }
            if let Some(true) = fast_search_flag {
                scanner.fast_search();
            }
            if lower_bound.is_some() || upper_bound.is_some() {
                scanner.distance_range(lower_bound, upper_bound);
            }
            if let Some(dt) = distance_type {
                let metric = Self::parse_metric_type(Some(dt))?;
                scanner.distance_metric(metric);
            }
            // Apply offset on top of the k nearest results.
            if let Some(off) = offset.filter(|&o| o > 0) {
                scanner.limit(None, Some(off as i64)).map_err(|e| {
                    Error::invalid_input_source(
                        format!("Invalid offset for {}: {}", operation, e).into(),
                    )
                })?;
            }
        } else {
            // Scalar (non-vector) query: treat k as a row LIMIT.
            let limit = if k > 0 { Some(k as i64) } else { None };
            scanner
                .limit(limit, offset.map(|o| o as i64))
                .map_err(|e| {
                    Error::invalid_input_source(
                        format!("Invalid limit/offset for {}: {}", operation, e).into(),
                    )
                })?;
        }

        if let Some(true) = with_row_id {
            scanner.with_row_id();
        }

        Ok(())
    }

    /// Retrieve a snapshot of operation metrics.
    ///
    /// Returns a HashMap where keys are operation names (e.g., "list_tables", "describe_table")
    /// and values are the number of times each operation was called.
    ///
    /// Returns an empty HashMap if `ops_metrics_enabled` was false when building the namespace.
    pub fn retrieve_ops_metrics(&self) -> HashMap<String, u64> {
        self.ops_metrics
            .as_ref()
            .map(|m| m.retrieve())
            .unwrap_or_default()
    }

    /// Reset all operation metrics counters to zero.
    ///
    /// Does nothing if `ops_metrics_enabled` was false when building the namespace.
    pub fn reset_ops_metrics(&self) {
        if let Some(ref metrics) = self.ops_metrics {
            metrics.reset();
        }
    }

    /// Increment the counter for an operation.
    fn record_op(&self, operation: &str) {
        if let Some(ref metrics) = self.ops_metrics {
            metrics.increment(operation);
        }
    }
}

#[async_trait]
impl LanceNamespace for DirectoryNamespace {
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        self.record_op("list_namespaces");
        self.ensure_read_manifest().await?;
        if let Some(manifest_ns) = self.manifest_ns_for_read() {
            return manifest_ns.list_namespaces(request).await;
        }

        if request.id.as_ref().is_some_and(|id| !id.is_empty()) {
            return Err(self.child_namespace_requires_manifest_error());
        }
        Self::validate_root_namespace_id(&request.id)?;
        Ok(ListNamespacesResponse::new(vec![]))
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        self.record_op("describe_namespace");
        self.ensure_read_manifest().await?;
        if let Some(manifest_ns) = self.manifest_ns_for_read() {
            return manifest_ns.describe_namespace(request).await;
        }

        if request.id.as_ref().is_some_and(|id| !id.is_empty()) {
            return Err(self.child_namespace_requires_manifest_error());
        }
        Self::validate_root_namespace_id(&request.id)?;
        #[allow(clippy::needless_update)]
        Ok(DescribeNamespaceResponse {
            properties: Some(HashMap::new()),
            ..Default::default()
        })
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        self.record_op("create_namespace");
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.create_namespace(request).await;
        }

        if request.id.is_none() || request.id.as_ref().unwrap().is_empty() {
            return Err(NamespaceError::NamespaceAlreadyExists {
                message: "root namespace".to_string(),
            }
            .into());
        }

        Err(NamespaceError::Unsupported {
            message: "Child namespaces are only supported when manifest mode is enabled"
                .to_string(),
        }
        .into())
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        self.record_op("drop_namespace");
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.drop_namespace(request).await;
        }

        if request.id.is_none() || request.id.as_ref().unwrap().is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "Root namespace cannot be dropped".to_string(),
            }
            .into());
        }

        Err(NamespaceError::Unsupported {
            message: "Child namespaces are only supported when manifest mode is enabled"
                .to_string(),
        }
        .into())
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
        self.record_op("namespace_exists");
        self.ensure_read_manifest().await?;
        if let Some(manifest_ns) = self.manifest_ns_for_read() {
            return manifest_ns.namespace_exists(request).await;
        }

        if request.id.is_none() || request.id.as_ref().unwrap().is_empty() {
            return Ok(());
        }

        Err(self.child_namespace_requires_manifest_error())
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.record_op("list_tables");
        // Validate that namespace ID is provided
        let namespace_id = request.id.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Namespace ID is required".to_string(),
            })
        })?;

        // Self-heal the manifest wherever it can be authoritative: a child
        // namespace, a manifest-only namespace, or migration mode (which merges
        // manifest entries -- including registered aliases -- into the root
        // listing). The bypass applies only to migration-disabled directory-backed
        // root lists.
        if !namespace_id.is_empty()
            || !self.dir_listing_enabled
            || self.dir_listing_to_manifest_migration_enabled
        {
            self.ensure_read_manifest().await?;
        }

        // For child namespaces, always delegate to manifest (if enabled)
        if !namespace_id.is_empty() {
            if let Some(manifest_ns) = self.manifest_ns_for_read() {
                return manifest_ns.list_tables(request).await;
            }
            return Err(self.child_namespace_requires_manifest_error());
        }

        // When only manifest is enabled (no directory listing), delegate directly to manifest
        if let Some(manifest_ns) = self.manifest_ns_for_read()
            && !self.dir_listing_enabled
        {
            return manifest_ns.list_tables(request).await;
        }
        if !self.dir_listing_enabled {
            return Ok(ListTablesResponse::new(vec![]));
        }

        // When both manifest and directory listing are enabled with migration mode,
        // we need to merge and deduplicate
        let mut tables = if self.manifest_ns_for_read().is_some()
            && self.dir_listing_enabled
            && self.dir_listing_to_manifest_migration_enabled
        {
            // Get all manifest table locations (for deduplication)
            let manifest_locations = if let Some(manifest_ns) = self.manifest_ns_for_read() {
                manifest_ns.list_manifest_table_locations().await?
            } else {
                std::collections::HashSet::new()
            };

            // Get all manifest tables (without pagination for merging)
            let mut manifest_request = request.clone();
            manifest_request.limit = None;
            manifest_request.page_token = None;
            let manifest_tables = if let Some(manifest_ns) = self.manifest_ns_for_read() {
                let manifest_response = manifest_ns.list_tables(manifest_request).await?;
                manifest_response.tables
            } else {
                vec![]
            };

            // Start with all manifest table names
            // Add directory tables that aren't already in the manifest (by location)
            let mut all_tables: Vec<String> = manifest_tables;
            let dir_tables = self.list_directory_tables().await?;
            for table_name in dir_tables {
                // Check if this table's location is already in the manifest
                // Manifest stores full URIs, so we need to check both formats
                let full_location = format!("{}/{}.lance", self.root, table_name);
                let relative_location = format!("{}.lance", table_name);
                if !manifest_locations.contains(&full_location)
                    && !manifest_locations.contains(&relative_location)
                {
                    all_tables.push(table_name);
                }
            }

            all_tables
        } else {
            self.list_directory_tables().await?
        };

        tables = self
            .filter_declared_tables(tables, request.include_declared.unwrap_or(true))
            .await?;

        // Apply sorting and pagination
        let next_page_token =
            Self::apply_pagination(&mut tables, request.page_token, request.limit);
        let mut response = ListTablesResponse::new(tables);
        response.page_token = next_page_token;
        Ok(response)
    }

    async fn describe_table(&self, request: DescribeTableRequest) -> Result<DescribeTableResponse> {
        self.record_op("describe_table");
        self.describe_table_impl(request).await
    }

    async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
        self.record_op("table_exists");
        let is_root_level = request.id.as_ref().is_some_and(|id| id.len() == 1);
        let is_child_table = request.id.as_ref().is_some_and(|id| id.len() > 1);
        let skip_manifest_for_root = self.dir_listing_enabled
            && is_root_level
            && !self.dir_listing_to_manifest_migration_enabled;
        // Child table, manifest-only, or migration mode (see describe_table_impl).
        // Only a migration-disabled directory-backed root read bypasses the probe.
        if is_child_table
            || !self.dir_listing_enabled
            || self.dir_listing_to_manifest_migration_enabled
        {
            self.ensure_read_manifest().await?;
        }
        if let Some(manifest_ns) = self.manifest_ns_for_read()
            && !skip_manifest_for_root
        {
            match manifest_ns.table_exists(request.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if manifest_feature_flags::is_incompatible_manifest_error(&e) => {
                    // An incompatible manifest must surface "please upgrade"
                    // rather than degrading to a directory-listing view.
                    return Err(e);
                }
                Err(e) if self.dir_listing_enabled && is_root_level => {
                    // Only a genuinely-absent table (e.g. an unmigrated on-disk
                    // table) may fall through to the directory check; any other
                    // manifest error must propagate rather than be read as missing.
                    if !Self::is_manifest_table_absent_error(&e) {
                        return Err(Self::classify_storage_error(e));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if is_child_table {
            return Err(self.child_namespace_requires_manifest_error());
        }

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_id = Self::format_table_id_from_request(&request.id);
        if !self.dir_listing_enabled {
            return Err(NamespaceError::TableNotFound { message: table_id }.into());
        }

        // Atomically check table existence and deregistration status
        let status = self.check_table_status(&table_name).await?;

        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_id.clone(),
            }
            .into());
        }

        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is deregistered: {}", table_id),
            }
            .into());
        }

        Ok(())
    }

    async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
        self.record_op("drop_table");
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.drop_table(request).await;
        }

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);
        let table_path = self.table_path(&table_name);

        self.object_store
            .remove_dir_all(table_path)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to drop table {}: {:?}", table_name, e),
                })
            })?;

        Ok(DropTableResponse {
            id: request.id,
            location: Some(table_uri),
            ..Default::default()
        })
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        request_data: Bytes,
    ) -> Result<CreateTableResponse> {
        self.record_op("create_table");
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.create_table(request, request_data).await;
        }

        Self::validate_dir_only_properties(request.properties.as_ref(), "create_table")?;

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);
        let status = self.check_table_status(&table_name).await?;
        let (reader, _num_rows) =
            Self::ipc_reader_from_request_data(&request_data, "create_table")?;

        if status.exists && self.table_has_actual_manifests(&table_name).await? {
            return Err(NamespaceError::TableAlreadyExists {
                message: table_name,
            }
            .into());
        }

        let write_result = self
            .write_reader_to_table(
                &table_uri,
                reader,
                WriteMode::Create,
                request.storage_options.clone(),
            )
            .await;
        if let Err(err) = write_result {
            if self.table_uri_has_actual_manifests(&table_uri).await? {
                return Err(NamespaceError::TableAlreadyExists {
                    message: table_name,
                }
                .into());
            }
            return Err(err);
        }
        Ok(CreateTableResponse {
            version: Some(1),
            location: Some(table_uri),
            storage_options: self.storage_options.clone(),
            properties: request.properties,
            ..Default::default()
        })
    }

    async fn declare_table(&self, request: DeclareTableRequest) -> Result<DeclareTableResponse> {
        self.record_op("declare_table");
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            let mut response = manifest_ns.declare_table(request.clone()).await?;
            if let Some(ref location) = response.location {
                // For backwards compatibility, only skip vending credentials when explicitly set to false
                let vend = request.vend_credentials.unwrap_or(true);
                let identity = request.identity.as_deref();
                response.storage_options = self
                    .get_storage_options_for_table(location, vend, identity)
                    .await?;
            }
            // Set managed_versioning when table_version_tracking_enabled
            if self.table_version_tracking_enabled {
                response.managed_versioning = Some(true);
            }
            return Ok(response);
        }

        Self::validate_dir_only_properties(request.properties.as_ref(), "declare_table")?;

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);

        // Validate location if provided
        if let Some(location) = &request.location {
            let location = location.trim_end_matches('/');
            if location != table_uri {
                return Err(NamespaceError::InvalidInput {
                    message: format!(
                        "Cannot declare table {} at location {}, must be at location {}",
                        table_name, location, table_uri
                    ),
                }
                .into());
            }
        }

        // Check if table already has data (created via create_table).
        // The atomic put only prevents races between concurrent declare_table calls,
        // not between declare_table and existing data.
        let status = self.check_table_status(&table_name).await?;
        if status.exists && !status.has_reserved_file {
            // Table has data but no reserved file - it was created with data
            return Err(NamespaceError::TableAlreadyExists {
                message: table_name.to_string(),
            }
            .into());
        }

        // Atomically create the .lance-reserved file to mark the table as declared.
        // This uses put_if_not_exists semantics to avoid race conditions between
        // concurrent declare_table calls.
        let reserved_file_path = self.table_reserved_file_path(&table_name);

        put_marker_file_atomic(
            &self.object_store,
            &reserved_file_path,
            &format!("table {}", table_name),
        )
        .await
        .map_err(|e| match e {
            MarkerFileError::AlreadyExists { .. } => {
                lance_core::Error::from(NamespaceError::TableAlreadyExists {
                    message: table_name.to_string(),
                })
            }
            MarkerFileError::Other { message } => {
                lance_core::Error::from(NamespaceError::Internal { message })
            }
        })?;

        // For backwards compatibility, only skip vending credentials when explicitly set to false
        let vend_credentials = request.vend_credentials.unwrap_or(true);
        let identity = request.identity.as_deref();
        let storage_options = self
            .get_storage_options_for_table(&table_uri, vend_credentials, identity)
            .await?;

        Ok(DeclareTableResponse {
            location: Some(table_uri),
            storage_options,
            properties: request.properties,
            managed_versioning: if self.table_version_tracking_enabled {
                Some(true)
            } else {
                None
            },
            ..Default::default()
        })
    }

    async fn register_table(
        &self,
        request: lance_namespace::models::RegisterTableRequest,
    ) -> Result<lance_namespace::models::RegisterTableResponse> {
        self.record_op("register_table");
        // If manifest is enabled, delegate to manifest namespace
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return LanceNamespace::register_table(manifest_ns.as_ref(), request).await;
        }

        // Without manifest, register_table is not supported
        Err(NamespaceError::Unsupported {
            message: "register_table is only supported when manifest mode is enabled".to_string(),
        }
        .into())
    }

    async fn deregister_table(
        &self,
        request: lance_namespace::models::DeregisterTableRequest,
    ) -> Result<lance_namespace::models::DeregisterTableResponse> {
        self.record_op("deregister_table");
        // If manifest is enabled, delegate to manifest namespace
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return LanceNamespace::deregister_table(manifest_ns.as_ref(), request).await;
        }

        // V1 mode: create a .lance-deregistered marker file in the table directory
        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);

        // Check table existence and deregistration status.
        // This provides better error messages for common cases.
        let status = self.check_table_status(&table_name).await?;

        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_name.to_string(),
            }
            .into());
        }

        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is already deregistered: {}", table_name),
            }
            .into());
        }

        // Atomically create the .lance-deregistered marker file.
        // This uses put_if_not_exists semantics to prevent race conditions
        // when multiple processes try to deregister the same table concurrently.
        // If a race occurs and another process already created the file,
        // we'll get an AlreadyExists error which we convert to a proper message.
        let deregistered_path = self.table_deregistered_file_path(&table_name);
        put_marker_file_atomic(
            &self.object_store,
            &deregistered_path,
            &format!("deregistration marker for table {}", table_name),
        )
        .await
        .map_err(|e| match e {
            MarkerFileError::AlreadyExists { .. } => {
                lance_core::Error::from(NamespaceError::InvalidTableState {
                    message: format!("Table is already deregistered: {}", table_name),
                })
            }
            MarkerFileError::Other { message } => {
                lance_core::Error::from(NamespaceError::Internal { message })
            }
        })?;

        Ok(lance_namespace::models::DeregisterTableResponse {
            id: request.id,
            location: Some(table_uri),
            ..Default::default()
        })
    }

    async fn alter_table_add_columns(
        &self,
        request: AlterTableAddColumnsRequest,
    ) -> Result<AlterTableAddColumnsResponse> {
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.alter_table_add_columns(request).await;
        }

        // Non-manifest mode: open Dataset directly via table URI and perform the operation
        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);

        // Check table existence and deregistration status before opening the dataset
        let status = self.check_table_status(&table_name).await?;
        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_name,
            }
            .into());
        }
        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is deregistered: {}", table_name),
            }
            .into());
        }

        let mut dataset = self
            .configured_builder(&table_uri)
            .load()
            .await
            .map_err(|e| {
                Error::io_source(box_error(std::io::Error::other(format!(
                    "Failed to open dataset: {}",
                    e
                ))))
            })?;

        let sql_expressions = build_sql_expressions(&request.new_columns)?;

        dataset
            .add_columns(
                lance::dataset::NewColumnTransform::SqlExpressions(sql_expressions),
                None,
                None,
            )
            .await
            .map_err(|e| {
                Error::io_source(box_error(std::io::Error::other(format!(
                    "Failed to add columns: {}",
                    e
                ))))
            })?;

        let version = dataset.version().version as i64;
        Ok(AlterTableAddColumnsResponse::new(version))
    }

    async fn alter_table_alter_columns(
        &self,
        request: AlterTableAlterColumnsRequest,
    ) -> Result<AlterTableAlterColumnsResponse> {
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.alter_table_alter_columns(request).await;
        }

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);

        // Check table existence and deregistration status before opening the dataset
        let status = self.check_table_status(&table_name).await?;
        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_name,
            }
            .into());
        }
        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is deregistered: {}", table_name),
            }
            .into());
        }

        let mut dataset = self
            .configured_builder(&table_uri)
            .load()
            .await
            .map_err(|e| {
                Error::io_source(box_error(std::io::Error::other(format!(
                    "Failed to open dataset: {}",
                    e
                ))))
            })?;

        let alterations = build_column_alterations(&request.alterations)?;

        dataset.alter_columns(&alterations).await.map_err(|e| {
            Error::io_source(box_error(std::io::Error::other(format!(
                "Failed to alter columns: {}",
                e
            ))))
        })?;

        let version = dataset.version().version as i64;
        Ok(AlterTableAlterColumnsResponse::new(version))
    }

    async fn alter_table_drop_columns(
        &self,
        request: AlterTableDropColumnsRequest,
    ) -> Result<AlterTableDropColumnsResponse> {
        if let Some(manifest_ns) = self.manifest_ns_for_write().await? {
            return manifest_ns.alter_table_drop_columns(request).await;
        }

        let table_name = Self::table_name_from_id(&request.id)?;
        let table_uri = self.table_full_uri(&table_name);

        // Check table existence and deregistration status before opening the dataset
        let status = self.check_table_status(&table_name).await?;
        if !status.exists {
            return Err(NamespaceError::TableNotFound {
                message: table_name,
            }
            .into());
        }
        if status.is_deregistered {
            return Err(NamespaceError::TableNotFound {
                message: format!("Table is deregistered: {}", table_name),
            }
            .into());
        }

        let mut dataset = self
            .configured_builder(&table_uri)
            .load()
            .await
            .map_err(|e| {
                Error::io_source(box_error(std::io::Error::other(format!(
                    "Failed to open dataset: {}",
                    e
                ))))
            })?;

        let columns: Vec<&str> = request.columns.iter().map(|s| s.as_str()).collect();
        dataset.drop_columns(&columns).await.map_err(|e| {
            Error::io_source(box_error(std::io::Error::other(format!(
                "Failed to drop columns: {}",
                e
            ))))
        })?;

        let version = dataset.version().version as i64;
        Ok(AlterTableDropColumnsResponse::new(version))
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> Result<ListTableVersionsResponse> {
        self.record_op("list_table_versions");
        let branch = Self::normalized_branch(request.branch.as_deref())?;
        let table_uri = self.resolve_table_location(&request.id).await?;
        let table_uri = match branch {
            Some(b) => self.resolve_branch_location(&table_uri, b).await?,
            None => table_uri,
        };
        let want_descending = request.descending == Some(true);
        let table_versions = self
            .list_table_versions_from_storage(&table_uri, want_descending, request.limit)
            .await?;

        Ok(ListTableVersionsResponse {
            versions: table_versions,
            page_token: None,
        })
    }

    async fn create_table_version(
        &self,
        request: CreateTableVersionRequest,
    ) -> Result<CreateTableVersionResponse> {
        self.record_op("create_table_version");
        let branch = Self::normalized_branch(request.branch.as_deref())?;
        let table_uri = self.resolve_table_location(&request.id).await?;
        let (table_uri, table_path, branch_parent_version) = match branch {
            Some(b) => self.resolve_branch_for_commit(&table_uri, b).await?,
            None => {
                let table_path = self.object_store_path_from_uri(&table_uri)?;
                (table_uri, table_path, None)
            }
        };

        let staging_manifest_path = &request.manifest_path;
        let version = request.version as u64;

        // Determine naming scheme from request, default to V2
        let naming_scheme = match request.naming_scheme.as_deref() {
            Some("V1") => ManifestNamingScheme::V1,
            _ => ManifestNamingScheme::V2,
        };

        // Compute final path using the naming scheme
        let final_path = naming_scheme.manifest_path(&table_path, version);

        let staging_path = Path::parse(staging_manifest_path).map_err(|e| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: format!(
                    "Invalid staging manifest path '{}': {}",
                    staging_manifest_path, e
                ),
            })
        })?;

        // Idempotent retry: version path already published with the same content.
        match self.object_store.inner.head(&final_path).await {
            Ok(existing_meta) => {
                return self
                    .resolve_existing_table_version(ExistingTableVersionResolve {
                        staging_path: &staging_path,
                        final_path: &final_path,
                        version,
                        table_uri: &table_uri,
                        final_meta: &existing_meta,
                        request_manifest_size: request.manifest_size,
                    })
                    .await;
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => {
                return Err(lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to stat version {} for table at '{}': {}",
                        version, table_uri, e
                    ),
                }));
            }
        }

        // Strict CAS: only allow appending latest+1 (or the empty-chain bootstrap
        // version: v1 on main, BranchContents.parent_version on a registered branch).
        let is_branch = branch.is_some();
        self.enforce_create_table_version_cas(
            &table_path,
            version,
            &table_uri,
            is_branch,
            branch_parent_version,
        )
        .await?;

        // Materialize with Create / copy_if_not_exists only — never overwrite.
        let copy_result = self
            .materialize_version_manifest_create(&staging_path, &final_path, staging_manifest_path)
            .await;

        match copy_result {
            Ok(()) => {}
            Err(ObjectStoreError::AlreadyExists { .. })
            | Err(ObjectStoreError::Precondition { .. }) => {
                // Lost a Create race: succeed only if the winner published identical bytes.
                let existing_meta = self.object_store.inner.head(&final_path).await.map_err(|e| {
                    lance_core::Error::from(NamespaceError::Internal {
                        message: format!(
                            "Version {} conflict for table at '{}' but failed to stat winner: {}",
                            version, table_uri, e
                        ),
                    })
                })?;
                return self
                    .resolve_existing_table_version(ExistingTableVersionResolve {
                        staging_path: &staging_path,
                        final_path: &final_path,
                        version,
                        table_uri: &table_uri,
                        final_meta: &existing_meta,
                        request_manifest_size: request.manifest_size,
                    })
                    .await;
            }
            Err(ObjectStoreError::NotFound { .. }) => {
                return Err(lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!(
                        "Staging manifest not found at '{}' for version {} of table at '{}'",
                        staging_manifest_path, version, table_uri
                    ),
                }));
            }
            Err(e) => {
                return Err(lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to create version {} for table at '{}': {}",
                        version, table_uri, e
                    ),
                }));
            }
        }

        let final_meta = self
            .object_store
            .inner
            .head(&final_path)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to stat created version {} for table at '{}': {}",
                        version, table_uri, e
                    ),
                })
            })?;

        // Delete the staging manifest after successful copy
        if let Err(e) = self.object_store.inner.delete(&staging_path).await {
            log::warn!(
                "Failed to delete staging manifest at '{}': {:?}",
                staging_path,
                e
            );
        }

        Ok(Self::create_table_version_response(
            version,
            &final_path,
            &final_meta,
        ))
    }

    async fn describe_table_version(
        &self,
        request: DescribeTableVersionRequest,
    ) -> Result<DescribeTableVersionResponse> {
        self.record_op("describe_table_version");
        let branch = Self::normalized_branch(request.branch.as_deref())?;
        let table_uri = self.resolve_table_location(&request.id).await?;
        let table_uri = match branch {
            Some(b) => self.resolve_branch_location(&table_uri, b).await?,
            None => table_uri,
        };
        let versions = self
            .list_table_versions_from_storage(&table_uri, true, None)
            .await?;
        let table_version = if let Some(requested_version) = request.version {
            versions
                .into_iter()
                .find(|version| version.version == requested_version)
                .ok_or_else(|| {
                    lance_core::Error::from(NamespaceError::TableVersionNotFound {
                        message: format!(
                            "version {} for table {}",
                            requested_version,
                            Self::format_table_id_from_request(&request.id)
                        ),
                    })
                })?
        } else {
            versions.into_iter().next().ok_or_else(|| {
                lance_core::Error::from(NamespaceError::TableVersionNotFound {
                    message: format!(
                        "latest version for table {}",
                        Self::format_table_id_from_request(&request.id)
                    ),
                })
            })?
        };

        Ok(DescribeTableVersionResponse {
            version: Box::new(table_version),
        })
    }

    async fn batch_delete_table_versions(
        &self,
        request: BatchDeleteTableVersionsRequest,
    ) -> Result<BatchDeleteTableVersionsResponse> {
        self.record_op("batch_delete_table_versions");
        let branch = Self::normalized_branch(request.branch.as_deref())?;
        // Single-table mode: use `id` (from path parameter) + `ranges` to delete
        // versions from one table.
        let ranges: Vec<(i64, i64)> = request
            .ranges
            .iter()
            .map(|r| (r.start_version, r.end_version))
            .collect();

        // Reject pathological bounded ranges up front: an explicit huge bounded
        // range like (0, i64::MAX) is almost certainly a mistake. A through-latest
        // range (end < 0) is bounded by the manifests that actually exist on storage.
        const MAX_VERSIONS_PER_REQUEST: i128 = 1_000_000;
        let requested: i128 = ranges
            .iter()
            .map(|(s, e)| {
                if *e < 0 {
                    0
                } else {
                    (*e as i128 - *s as i128).max(0)
                }
            })
            .sum();
        if requested > MAX_VERSIONS_PER_REQUEST {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "batch_delete requested {} versions; limit is {}",
                    requested, MAX_VERSIONS_PER_REQUEST
                ),
            }
            .into());
        }

        let table_entries = vec![TableDeleteEntry {
            table_id: request.id.clone(),
            ranges,
        }];

        let total_deleted_count = self
            .delete_physical_version_files(&table_entries, branch)
            .await?;

        Ok(BatchDeleteTableVersionsResponse {
            deleted_count: Some(total_deleted_count),
            transaction_id: None,
        })
    }

    async fn create_table_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableIndexResponse> {
        self.record_op("create_table_index");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "create_table_index")
            .await?;
        let index_request = Self::build_index_params(&request)?;

        dataset
            .create_index(
                &[request.column.as_str()],
                index_request.index_type(),
                request.name.clone(),
                index_request.params(),
                false,
            )
            .await
            .map_err(|e| {
                let err_msg = format!("{}", e);
                let ns_err = if err_msg.contains("already exists") {
                    NamespaceError::TableIndexAlreadyExists {
                        message: format!(
                            "Index '{}' already exists on table '{}': {:?}",
                            request.name.as_deref().unwrap_or("<auto-generated>"),
                            table_uri,
                            e
                        ),
                    }
                } else if err_msg.contains("not found") || err_msg.contains("does not exist") {
                    NamespaceError::TableColumnNotFound {
                        message: format!(
                            "Column '{}' not found for table '{}': {:?}",
                            request.column, table_uri, e
                        ),
                    }
                } else {
                    NamespaceError::Internal {
                        message: format!(
                            "Failed to create {} index '{}' on column '{}' for table '{}': {:?}",
                            request.index_type,
                            request.name.as_deref().unwrap_or("<auto-generated>"),
                            request.column,
                            table_uri,
                            e
                        ),
                    }
                };
                lance_core::Error::from(ns_err)
            })?;

        let transaction_id = dataset
            .read_transaction()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read committed transaction after creating index on '{}': {}",
                        table_uri, e
                    ),
                })
            })?
            .map(|transaction| transaction.uuid);

        Ok(CreateTableIndexResponse { transaction_id })
    }

    async fn list_table_indices(
        &self,
        request: ListTableIndicesRequest,
    ) -> Result<ListTableIndicesResponse> {
        self.record_op("list_table_indices");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, request.version, "list_table_indices")
            .await?;
        let total_rows = dataset.count_rows(None).await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to count rows for table '{}': {:?}", table_uri, e),
            })
        })? as u64;
        let mut indices = dataset
            .describe_indices(None)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!("Failed to describe table indices for '{}': {:?}", table_uri, e),
                })
            })?
            .into_iter()
            .filter(|description| {
                description
                    .metadata()
                    .first()
                    .map(|metadata| !is_system_index(metadata))
                    .unwrap_or(false)
            })
            .map(|description| {
                let columns = description
                    .field_ids()
                    .iter()
                        .map(|field_id| {
                        dataset
                            .schema()
                            .field_path_minimal(i32::try_from(*field_id).map_err(|e| {
                                lance_core::Error::from(NamespaceError::Internal {
                                    message: format!(
                                        "Field id {} does not fit in i32 for table '{}': {}",
                                        field_id, table_uri, e
                                    ),
                                })
                            })?)
                            .map_err(|e| {
                            lance_core::Error::from(NamespaceError::Internal {
                                message: format!(
                                    "Failed to resolve field path for field_id {} in table '{}': {}",
                                    field_id, table_uri, e
                                ),
                            })
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let segments = description.segments();
                let created_at = segments
                    .iter()
                    .filter_map(|segment| segment.created_at)
                    .min()
                    .map(|ts| ts.to_rfc3339());

                // `..Default::default()` keeps this tolerant of additive reqwest
                // client model changes (see #7212).
                #[allow(clippy::needless_update)]
                let content = IndexContent {
                    index_name: description.name().to_string(),
                    index_uuid: description.metadata()[0].uuid.to_string(),
                    columns,
                    status: "SUCCEEDED".to_string(),
                    index_type: Some(description.index_type().to_string()),
                    type_url: Some(description.type_url().to_string()),
                    num_indexed_rows: Some(description.rows_indexed() as i64),
                    num_unindexed_rows: Some(
                        total_rows.saturating_sub(description.rows_indexed()) as i64,
                    ),
                    size_bytes: description.total_size_bytes().map(|size| size as i64),
                    num_segments: Some(segments.len() as i32),
                    created_at,
                    index_version: segments.first().map(|segment| segment.index_version),
                    index_details: description.details().ok(),
                    ..Default::default()
                };
                Ok(content)
            })
            .collect::<Result<Vec<_>>>()?;

        let page_token = Self::paginate_indices(&mut indices, request.page_token, request.limit);
        Ok(ListTableIndicesResponse {
            indexes: indices,
            page_token,
        })
    }

    async fn describe_table_index_stats(
        &self,
        request: DescribeTableIndexStatsRequest,
    ) -> Result<DescribeTableIndexStatsResponse> {
        self.record_op("describe_table_index_stats");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, request.version, "describe_table_index_stats")
            .await?;
        let index_name = request.index_name.as_deref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Index name is required for describe_table_index_stats".to_string(),
            })
        })?;
        let metadatas = dataset
            .load_indices_by_name(index_name)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::TableIndexNotFound {
                    message: format!(
                        "Failed to load index '{}' metadata for table '{}': {}",
                        index_name, table_uri, e
                    ),
                })
            })?;
        if metadatas.first().is_some_and(is_system_index) {
            return Err(NamespaceError::Unsupported {
                message: format!("System index '{}' is not exposed by this API", index_name),
            }
            .into());
        }

        let stats = <Dataset as DatasetIndexExt>::index_statistics(&dataset, index_name)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::TableIndexNotFound {
                    message: format!(
                        "Failed to describe index statistics for '{}' on table '{}': {}",
                        index_name, table_uri, e
                    ),
                })
            })?;
        let stats: serde_json::Value = serde_json::from_str(&stats).map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to parse index statistics for '{}' on table '{}': {}",
                    index_name, table_uri, e
                ),
            })
        })?;

        Ok(Self::describe_table_index_stats_response(&stats))
    }

    async fn describe_transaction(
        &self,
        request: DescribeTransactionRequest,
    ) -> Result<DescribeTransactionResponse> {
        self.record_op("describe_transaction");
        let mut request_id = request.id.ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Transaction id must include table id and transaction identifier"
                    .to_string(),
            })
        })?;
        if request_id.len() < 2 {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Transaction request id must include table id and transaction identifier, got {:?}",
                    request_id
                ),
            }
            .into());
        }

        let id = request_id.pop().expect("request_id len checked above");
        let table_id = Some(request_id);
        let table_uri = self.resolve_table_location(&table_id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "describe_transaction")
            .await?;
        let (version, transaction) = self.find_transaction(&dataset, &id).await?;

        // Merge any persisted alter_transaction changes stored in the sidecar
        // so that describe_transaction reflects the latest altered state.
        let sidecar = self
            .load_transaction_alteration(&table_uri, &transaction.uuid)
            .await?;

        Ok(Self::transaction_response(version, &transaction, sidecar))
    }

    async fn alter_transaction(
        &self,
        request: AlterTransactionRequest,
    ) -> Result<AlterTransactionResponse> {
        self.record_op("alter_transaction");

        // Parse the request ID: must include table id and transaction identifier
        let mut request_id = request.id.ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Transaction id must include table id and transaction identifier"
                    .to_string(),
            })
        })?;
        if request_id.len() < 2 {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "Transaction request id must include table id and transaction identifier, got {:?}",
                    request_id
                ),
            }
            .into());
        }

        let txn_id = request_id.pop().expect("request_id len checked above");
        let table_id = Some(request_id);
        let table_uri = self.resolve_table_location(&table_id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "alter_transaction")
            .await?;
        let (version, transaction) = self.find_transaction(&dataset, &txn_id).await?;

        // Reserved keys are derived from the immutable Transaction metadata and
        // must not be modified via alter_transaction. They are only surfaced in
        // the response for the caller's convenience.
        const RESERVED_KEYS: &[&str] = &["uuid", "version", "read_version", "operation", "tag"];
        let is_reserved = |key: &str| RESERVED_KEYS.contains(&key);

        // Load the existing sidecar (if any) so alterations accumulate across
        // successive alter_transaction calls.
        let mut sidecar = self
            .load_transaction_alteration(&table_uri, &transaction.uuid)
            .await?
            .unwrap_or_default();

        for action in &request.actions {
            if let Some(ref set_status) = action.set_status_action
                && let Some(ref status) = set_status.status
            {
                // Validate the status value (case-insensitive)
                let normalized = status.to_lowercase().replace('_', "");
                match normalized.as_str() {
                    "queued" | "running" | "succeeded" | "failed" | "canceled" => {
                        sidecar.status = Some(status.clone());
                    }
                    _ => {
                        return Err(NamespaceError::InvalidInput {
                            message: format!(
                                "Invalid transaction status '{}'. Valid values are: Queued, Running, Succeeded, Failed, Canceled",
                                status
                            ),
                        }
                        .into());
                    }
                }
            }

            if let Some(ref set_property) = action.set_property_action
                && let (Some(key), Some(value)) = (&set_property.key, &set_property.value)
            {
                if is_reserved(key) {
                    return Err(NamespaceError::InvalidInput {
                        message: format!("Property '{}' is reserved and cannot be modified", key),
                    }
                    .into());
                }
                let mode = set_property
                    .mode
                    .as_deref()
                    .unwrap_or("Overwrite")
                    .to_lowercase();
                match mode.as_str() {
                    "overwrite" => {
                        sidecar.properties.insert(key.clone(), value.clone());
                    }
                    "fail" => {
                        // Consider both the immutable transaction properties
                        // and any values previously written to the sidecar.
                        let exists = sidecar.properties.contains_key(key)
                            || transaction
                                .transaction_properties
                                .as_ref()
                                .is_some_and(|props| props.contains_key(key));
                        if exists {
                            return Err(NamespaceError::ConcurrentModification {
                                message: format!(
                                    "Property '{}' already exists and mode is 'Fail'",
                                    key
                                ),
                            }
                            .into());
                        }
                        sidecar.properties.insert(key.clone(), value.clone());
                    }
                    "skip" => {
                        let exists = sidecar.properties.contains_key(key)
                            || transaction
                                .transaction_properties
                                .as_ref()
                                .is_some_and(|props| props.contains_key(key));
                        if !exists {
                            sidecar.properties.insert(key.clone(), value.clone());
                        }
                    }
                    _ => {
                        return Err(NamespaceError::InvalidInput {
                            message: format!(
                                "Invalid set_property mode '{}'. Valid values are: Overwrite, Fail, Skip",
                                mode
                            ),
                        }
                        .into());
                    }
                }
            }

            if let Some(ref unset_property) = action.unset_property_action
                && let Some(ref key) = unset_property.key
            {
                if is_reserved(key) {
                    return Err(NamespaceError::InvalidInput {
                        message: format!("Property '{}' is reserved and cannot be modified", key),
                    }
                    .into());
                }
                let mode = unset_property
                    .mode
                    .as_deref()
                    .unwrap_or("Skip")
                    .to_lowercase();
                let exists_in_transaction = transaction
                    .transaction_properties
                    .as_ref()
                    .is_some_and(|props| props.contains_key(key));
                match mode.as_str() {
                    "skip" => {
                        sidecar.properties.remove(key);
                        if exists_in_transaction {
                            // Track a tombstone so describe_transaction can
                            // hide the immutable property from the response.
                            sidecar.removed_properties.insert(key.clone());
                        }
                    }
                    "fail" => {
                        if !sidecar.properties.contains_key(key) && !exists_in_transaction {
                            return Err(NamespaceError::InvalidInput {
                                message: format!(
                                    "Property '{}' does not exist and mode is 'Fail'",
                                    key
                                ),
                            }
                            .into());
                        }
                        sidecar.properties.remove(key);
                        if exists_in_transaction {
                            sidecar.removed_properties.insert(key.clone());
                        }
                    }
                    _ => {
                        return Err(NamespaceError::InvalidInput {
                            message: format!(
                                "Invalid unset_property mode '{}'. Valid values are: Skip, Fail",
                                mode
                            ),
                        }
                        .into());
                    }
                }
            }
        }

        // Persist the accumulated alterations so subsequent calls observe
        // them. The transaction file itself is immutable in Lance, so we
        // record alter_transaction outcomes in a namespace-owned sidecar.
        self.save_transaction_alteration(&table_uri, &transaction.uuid, &sidecar)
            .await?;

        // Assemble the response by merging the immutable transaction metadata
        // with the persisted alterations.
        let final_status = sidecar
            .status
            .clone()
            .unwrap_or_else(|| "SUCCEEDED".to_string());
        let response = Self::transaction_response(version, &transaction, Some(sidecar));
        Ok(AlterTransactionResponse {
            status: final_status,
            properties: response.properties,
        })
    }

    async fn create_table_scalar_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> Result<CreateTableScalarIndexResponse> {
        self.record_op("create_table_scalar_index");
        let index_type = Self::parse_index_type(&request.index_type)?;
        if !index_type.is_scalar() {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "create_table_scalar_index only supports scalar index types, got {}",
                    request.index_type
                ),
            }
            .into());
        }

        let response = self.create_table_index(request).await?;
        Ok(CreateTableScalarIndexResponse {
            transaction_id: response.transaction_id,
        })
    }

    async fn drop_table_index(
        &self,
        request: DropTableIndexRequest,
    ) -> Result<DropTableIndexResponse> {
        self.record_op("drop_table_index");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let index_name = request.index_name.as_deref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "Index name is required for drop_table_index".to_string(),
            })
        })?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "drop_table_index")
            .await?;
        let metadatas = dataset
            .load_indices_by_name(index_name)
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::TableIndexNotFound {
                    message: format!(
                        "Failed to load index '{}' before dropping it from table '{}': {}",
                        index_name, table_uri, e
                    ),
                })
            })?;
        if metadatas.first().is_some_and(is_system_index) {
            return Err(NamespaceError::Unsupported {
                message: format!(
                    "System index '{}' cannot be dropped via this API",
                    index_name
                ),
            }
            .into());
        }

        dataset.drop_index(index_name).await.map_err(|e| {
            lance_core::Error::from(NamespaceError::TableIndexNotFound {
                message: format!(
                    "Failed to drop index '{}' from table '{}': {}",
                    index_name, table_uri, e
                ),
            })
        })?;

        let transaction_id = dataset
            .read_transaction()
            .await
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::Internal {
                    message: format!(
                        "Failed to read committed transaction after dropping index '{}' from '{}': {}",
                        index_name, table_uri, e
                    ),
                })
            })?
            .map(|transaction| transaction.uuid);

        Ok(DropTableIndexResponse { transaction_id })
    }

    async fn list_all_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        // In dir-only mode there are no child namespaces, so all tables live in the
        // root directory. This is equivalent to listing the root namespace.
        let mut tables = self.list_directory_tables().await?;
        tables = self
            .filter_declared_tables(tables, request.include_declared.unwrap_or(true))
            .await?;
        Self::apply_pagination(&mut tables, request.page_token, request.limit);
        Ok(ListTablesResponse::new(tables))
    }

    async fn restore_table(&self, request: RestoreTableRequest) -> Result<RestoreTableResponse> {
        let version = request.version;
        if version < 0 {
            return Err(Error::invalid_input_source(
                format!(
                    "Table version for restore_table must be non-negative, got {}",
                    version
                )
                .into(),
            ));
        }

        let branch = Self::normalized_branch(request.branch.as_deref())?;
        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = match branch {
            Some(branch) => self.open_validated_branch(&table_uri, branch).await?,
            None => self.load_dataset(&table_uri, None, "restore_table").await?,
        };

        dataset = dataset
            .checkout_version(version as u64)
            .await
            .map_err(|e| {
                Error::namespace_source(
                    format!(
                        "Failed to checkout version {} for restore at '{}': {}",
                        version, table_uri, e
                    )
                    .into(),
                )
            })?;

        dataset.restore().await.map_err(|e| {
            Error::namespace_source(
                format!(
                    "Failed to restore table at '{}' to version {}: {}",
                    table_uri, version, e
                )
                .into(),
            )
        })?;

        let transaction_id = dataset
            .read_transaction()
            .await
            .map_err(|e| {
                Error::namespace_source(
                    format!(
                        "Failed to read transaction after restoring '{}': {}",
                        table_uri, e
                    )
                    .into(),
                )
            })?
            .map(|t| t.uuid);

        Ok(RestoreTableResponse { transaction_id })
    }

    async fn update_table_schema_metadata(
        &self,
        request: UpdateTableSchemaMetadataRequest,
    ) -> Result<UpdateTableSchemaMetadataResponse> {
        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "update_table_schema_metadata")
            .await?;

        let new_metadata = request.metadata.unwrap_or_default();
        let updated_metadata = dataset
            .update_schema_metadata(new_metadata.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .await
            .map_err(|e| {
                Error::namespace_source(
                    format!(
                        "Failed to update schema metadata for table at '{}': {}",
                        table_uri, e
                    )
                    .into(),
                )
            })?;

        let transaction_id = dataset
            .read_transaction()
            .await
            .map_err(|e| {
                Error::namespace_source(
                    format!(
                        "Failed to read transaction after updating metadata for '{}': {}",
                        table_uri, e
                    )
                    .into(),
                )
            })?
            .map(|t| t.uuid);

        Ok(UpdateTableSchemaMetadataResponse {
            metadata: Some(updated_metadata),
            transaction_id,
        })
    }

    async fn get_table_stats(
        &self,
        request: GetTableStatsRequest,
    ) -> Result<GetTableStatsResponse> {
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = Arc::new(
            self.load_dataset(&table_uri, None, "get_table_stats")
                .await?,
        );

        // Compute total bytes on disk using field-level statistics
        let data_stats = dataset.calculate_data_stats().await.map_err(|e| {
            Error::namespace_source(
                format!(
                    "Failed to calculate data statistics for table at '{}': {}",
                    table_uri, e
                )
                .into(),
            )
        })?;
        let total_bytes: i64 = data_stats
            .fields
            .iter()
            .map(|f| f.bytes_on_disk as i64)
            .sum();

        // Collect per-fragment row counts
        let fragment_row_futures: Vec<_> = dataset
            .get_fragments()
            .into_iter()
            .map(|f| async move { f.physical_rows().await })
            .collect();
        let fragment_row_results = futures::future::join_all(fragment_row_futures).await;
        let mut fragment_row_counts: Vec<i64> = fragment_row_results
            .into_iter()
            .filter_map(|r| r.ok())
            .map(|r| r as i64)
            .collect();

        let num_fragments = fragment_row_counts.len() as i64;
        let num_rows: i64 = fragment_row_counts.iter().sum();

        // Fragments with fewer rows than the compaction target are considered "small",
        // consistent with CompactionOptions::target_rows_per_fragment default.
        const SMALL_FRAGMENT_THRESHOLD: i64 = 1024 * 1024;
        let num_small_fragments = fragment_row_counts
            .iter()
            .filter(|&&r| r < SMALL_FRAGMENT_THRESHOLD)
            .count() as i64;

        // Compute length summary statistics
        fragment_row_counts.sort_unstable();
        let lengths = if fragment_row_counts.is_empty() {
            FragmentSummary::new(0, 0, 0, 0, 0, 0, 0)
        } else {
            let len = fragment_row_counts.len();
            let min = fragment_row_counts[0];
            let max = fragment_row_counts[len - 1];
            let mean = num_rows / num_fragments;
            let pct = |p: f64| fragment_row_counts[((len - 1) as f64 * p) as usize];
            FragmentSummary::new(min, max, mean, pct(0.25), pct(0.50), pct(0.75), pct(0.99))
        };

        // Count non-system indices
        let indices = dataset.load_indices().await.map_err(|e| {
            Error::namespace_source(
                format!("Failed to load indices for table at '{}': {}", table_uri, e).into(),
            )
        })?;
        let num_indices = indices.iter().filter(|m| !is_system_index(m)).count() as i64;

        let fragment_stats = FragmentStats::new(num_fragments, num_small_fragments, lengths);
        Ok(GetTableStatsResponse::new(
            total_bytes,
            num_rows,
            num_indices,
            fragment_stats,
        ))
    }

    async fn explain_table_query_plan(
        &self,
        request: ExplainTableQueryPlanRequest,
    ) -> Result<String> {
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(
                &table_uri,
                request.query.version,
                "explain_table_query_plan",
            )
            .await?;
        let verbose = request.verbose.unwrap_or(false);

        let mut scanner = dataset.scan();
        Self::apply_query_params_to_scanner(
            &mut scanner,
            request.query.filter.as_deref(),
            request.query.columns.as_deref(),
            request.query.vector_column.as_deref(),
            &request.query.vector,
            request.query.k,
            request.query.offset,
            request.query.prefilter,
            request.query.bypass_vector_index,
            request.query.nprobes,
            request.query.ef,
            request.query.refine_factor,
            request.query.distance_type.as_deref(),
            request.query.fast_search,
            request.query.with_row_id,
            request.query.lower_bound,
            request.query.upper_bound,
            "explain_table_query_plan",
        )?;

        scanner.explain_plan(verbose).await.map_err(|e| {
            Error::namespace_source(
                format!(
                    "Failed to explain query plan for table at '{}': {}",
                    table_uri, e
                )
                .into(),
            )
        })
    }

    async fn analyze_table_query_plan(
        &self,
        request: AnalyzeTableQueryPlanRequest,
    ) -> Result<String> {
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, request.version, "analyze_table_query_plan")
            .await?;

        let mut scanner = dataset.scan();
        Self::apply_query_params_to_scanner(
            &mut scanner,
            request.filter.as_deref(),
            request.columns.as_deref(),
            request.vector_column.as_deref(),
            &request.vector,
            request.k,
            request.offset,
            request.prefilter,
            request.bypass_vector_index,
            request.nprobes,
            request.ef,
            request.refine_factor,
            request.distance_type.as_deref(),
            request.fast_search,
            request.with_row_id,
            request.lower_bound,
            request.upper_bound,
            "analyze_table_query_plan",
        )?;

        scanner.analyze_plan().await.map_err(|e| {
            Error::namespace_source(
                format!(
                    "Failed to analyze query plan for table at '{}': {}",
                    table_uri, e
                )
                .into(),
            )
        })
    }

    async fn count_table_rows(&self, request: CountTableRowsRequest) -> Result<i64> {
        self.record_op("count_table_rows");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, request.version, "count_table_rows")
            .await?;

        let count =
            dataset
                .count_rows(request.predicate)
                .await
                .map_err(|e| NamespaceError::Internal {
                    message: format!("Failed to count rows for table at '{}': {:?}", table_uri, e),
                })?;

        Ok(count as i64)
    }

    async fn insert_into_table(
        &self,
        request: InsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<InsertIntoTableResponse> {
        self.record_op("insert_into_table");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let (reader, _num_rows) =
            Self::ipc_reader_from_request_data(&request_data, "insert_into_table")?;

        let mode = match request.mode.as_deref() {
            Some(m) if m.eq_ignore_ascii_case("overwrite") => WriteMode::Overwrite,
            Some(m) if m.eq_ignore_ascii_case("append") => WriteMode::Append,
            None => WriteMode::Append,
            Some(m) => {
                return Err(lance_namespace::error::NamespaceError::InvalidInput {
                    message: format!(
                        "Unsupported write mode '{}'. Supported modes are: 'append', 'overwrite'",
                        m
                    ),
                }
                .into());
            }
        };

        if !self.table_uri_has_actual_manifests(&table_uri).await? {
            self.write_reader_to_table(&table_uri, reader, WriteMode::Create, None)
                .await?;
        } else {
            self.write_reader_to_table(&table_uri, reader, mode, None)
                .await?;
        }

        Ok(InsertIntoTableResponse {
            transaction_id: None,
        })
    }

    async fn merge_insert_into_table(
        &self,
        request: MergeInsertIntoTableRequest,
        request_data: Bytes,
    ) -> Result<MergeInsertIntoTableResponse> {
        self.record_op("merge_insert_into_table");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let on = request.on.as_ref().ok_or_else(|| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: "'on' field is required for merge_insert_into_table".to_string(),
            })
        })?;

        let table_has_manifests = self.table_uri_has_actual_manifests(&table_uri).await?;
        let (reader, num_rows) =
            Self::ipc_reader_from_request_data(&request_data, "merge_insert_into_table")?;

        if !table_has_manifests {
            let dataset = self
                .write_reader_to_table(&table_uri, reader, WriteMode::Create, None)
                .await?;
            let version = dataset.version().version as i64;
            return Ok(MergeInsertIntoTableResponse {
                transaction_id: None,
                num_updated_rows: Some(0),
                num_inserted_rows: Some(num_rows as i64),
                num_deleted_rows: Some(0),
                version: Some(version),
            });
        }

        let dataset = Arc::new(
            self.load_dataset(&table_uri, None, "merge_insert_into_table")
                .await?,
        );

        let mut merge_builder = MergeInsertBuilder::try_new(dataset.clone(), vec![on.clone()])
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("Failed to create merge_insert_into_table builder: {}", e),
                })
            })?;

        if let Some(filter) = request.when_matched_update_all_filt.as_deref() {
            let behavior = WhenMatched::update_if(dataset.as_ref(), filter).map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!(
                        "Invalid when_matched_update_all_filt for merge_insert_into_table: {}",
                        e
                    ),
                })
            })?;
            merge_builder.when_matched(behavior);
        } else if request.when_matched_update_all.unwrap_or(false) {
            merge_builder.when_matched(WhenMatched::UpdateAll);
        }

        if matches!(request.when_not_matched_insert_all, Some(false)) {
            merge_builder.when_not_matched(WhenNotMatched::DoNothing);
        } else {
            merge_builder.when_not_matched(WhenNotMatched::InsertAll);
        }

        if let Some(filter) = request.when_not_matched_by_source_delete_filt.as_deref() {
            let behavior = WhenNotMatchedBySource::delete_if(dataset.as_ref(), filter).map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!(
                        "Invalid when_not_matched_by_source_delete_filt for merge_insert_into_table: {}",
                        e
                    ),
                })
            })?;
            merge_builder.when_not_matched_by_source(behavior);
        } else if request.when_not_matched_by_source_delete.unwrap_or(false) {
            merge_builder.when_not_matched_by_source(WhenNotMatchedBySource::Delete);
        }

        if let Some(use_index) = request.use_index {
            merge_builder.use_index(use_index);
        }

        let (dataset, stats) = merge_builder
            .try_build()
            .map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("Failed to build merge_insert_into_table job: {}", e),
                })
            })?
            .execute_reader(reader)
            .await
            .map_err(|e| Self::map_mutation_error(e, "merge_insert_into_table", &table_uri))?;

        Ok(MergeInsertIntoTableResponse {
            transaction_id: None,
            num_updated_rows: Some(stats.num_updated_rows as i64),
            num_inserted_rows: Some(stats.num_inserted_rows as i64),
            num_deleted_rows: Some(stats.num_deleted_rows as i64),
            version: Some(dataset.version().version as i64),
        })
    }

    async fn update_table(&self, request: UpdateTableRequest) -> Result<UpdateTableResponse> {
        self.record_op("update_table");

        if request.updates.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "update_table requires at least one [column, expression] pair".to_string(),
            }
            .into());
        }

        // Validate every update pair shape and detect duplicate columns up front so we
        // surface a clean error instead of failing deep inside the planner.
        let mut seen_columns: HashMap<String, ()> = HashMap::with_capacity(request.updates.len());
        for (idx, pair) in request.updates.iter().enumerate() {
            if pair.len() != 2 {
                return Err(NamespaceError::InvalidInput {
                    message: format!(
                        "update_table updates[{}] must be a [column, expression] pair, got {} elements",
                        idx,
                        pair.len()
                    ),
                }
                .into());
            }
            let column = &pair[0];
            if column.trim().is_empty() {
                return Err(NamespaceError::InvalidInput {
                    message: format!("update_table updates[{}] has an empty column name", idx),
                }
                .into());
            }
            if seen_columns.insert(column.clone(), ()).is_some() {
                return Err(NamespaceError::InvalidInput {
                    message: format!(
                        "update_table cannot update column '{}' more than once",
                        column
                    ),
                }
                .into());
            }
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = Arc::new(self.load_dataset(&table_uri, None, "update_table").await?);

        let mut builder = UpdateBuilder::new(dataset);
        for pair in &request.updates {
            // Indexing by 0/1 is safe due to the length check above.
            builder = builder.set(&pair[0], &pair[1]).map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid update expression for column '{}': {}", pair[0], e),
                })
            })?;
        }
        if let Some(predicate) = request.predicate.as_deref()
            && !predicate.trim().is_empty()
        {
            builder = builder.update_where(predicate).map_err(|e| {
                lance_core::Error::from(NamespaceError::InvalidInput {
                    message: format!("Invalid update_table predicate '{}': {}", predicate, e),
                })
            })?;
        }

        let job = builder.build().map_err(|e| {
            lance_core::Error::from(NamespaceError::InvalidInput {
                message: format!("Failed to build update_table job: {}", e),
            })
        })?;

        let result = job
            .execute()
            .await
            .map_err(|e| Self::map_mutation_error(e, "update_table", &table_uri))?;

        let version = result.new_dataset.version().version as i64;
        Ok(UpdateTableResponse {
            transaction_id: None,
            updated_rows: result.rows_updated as i64,
            version,
            properties: None,
        })
    }

    async fn delete_from_table(
        &self,
        request: DeleteFromTableRequest,
    ) -> Result<DeleteFromTableResponse> {
        self.record_op("delete_from_table");

        if request.predicate.trim().is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "delete_from_table requires a non-empty predicate".to_string(),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "delete_from_table")
            .await?;

        let result = dataset
            .delete(&request.predicate)
            .await
            .map_err(|e| Self::map_mutation_error(e, "delete_from_table", &table_uri))?;

        Ok(DeleteFromTableResponse {
            transaction_id: None,
            version: Some(result.new_dataset.version().version as i64),
        })
    }

    async fn query_table(&self, request: QueryTableRequest) -> Result<Bytes> {
        use arrow::ipc::writer::FileWriter;

        self.record_op("query_table");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, request.version, "query_table")
            .await?;

        // Build scanner
        let mut scanner = dataset.scan();

        // Check if this is a vector search query
        // vector is Box<QueryTableRequestVector>, not Option
        let has_vector_query = request
            .vector
            .single_vector
            .as_ref()
            .map(|sv| !sv.is_empty())
            .unwrap_or(false)
            || request
                .vector
                .multi_vector
                .as_ref()
                .map(|mv| !mv.is_empty())
                .unwrap_or(false);

        // Apply prefilter setting (must be set before nearest)
        if let Some(prefilter) = request.prefilter {
            scanner.prefilter(prefilter);
        }

        // Apply vector search if query vector is provided
        if has_vector_query {
            let vector_column = request.vector_column.as_deref().unwrap_or("vector");

            // Get the query vector(s)
            let query_vector: Vec<f32> = request
                .vector
                .single_vector
                .clone()
                .or_else(|| {
                    request
                        .vector
                        .multi_vector
                        .as_ref()
                        .and_then(|mv| mv.first().cloned())
                })
                .unwrap_or_default();

            if !query_vector.is_empty() {
                let k = if request.k > 0 {
                    request.k as usize
                } else {
                    10
                };
                let query_array = Float32Array::from(query_vector);
                scanner
                    .nearest(vector_column, &query_array, k)
                    .map_err(|e| NamespaceError::InvalidInput {
                        message: format!("Invalid vector search: {:?}", e),
                    })?;

                // Apply distance type if specified
                if let Some(ref distance_type) = request.distance_type {
                    let metric = match distance_type.to_lowercase().as_str() {
                        "l2" | "euclidean" => MetricType::L2,
                        "cosine" => MetricType::Cosine,
                        "dot" | "inner_product" => MetricType::Dot,
                        "hamming" => MetricType::Hamming,
                        _ => {
                            return Err(NamespaceError::InvalidInput {
                                message: format!("Unknown distance type: {}", distance_type),
                            }
                            .into());
                        }
                    };
                    scanner.distance_metric(metric);
                }

                // Apply nprobes if specified (maps to minimum_nprobes, matching lancedb behavior)
                if let Some(nprobes) = request.nprobes {
                    scanner.minimum_nprobes(nprobes as usize);
                }

                // Apply ef (HNSW search effort) if specified
                if let Some(ef) = request.ef {
                    scanner.ef(ef as usize);
                }

                // Apply refine_factor if specified
                if let Some(refine_factor) = request.refine_factor {
                    scanner.refine(refine_factor as u32);
                }

                // Apply distance bounds if specified
                if request.lower_bound.is_some() || request.upper_bound.is_some() {
                    scanner.distance_range(request.lower_bound, request.upper_bound);
                }

                // Apply use_index (inverse of bypass_vector_index)
                if let Some(bypass) = request.bypass_vector_index {
                    scanner.use_index(!bypass);
                }

                // Apply fast_search if specified
                if request.fast_search == Some(true) {
                    scanner.fast_search();
                }
            }
        }

        // Apply full text search if specified
        if let Some(ref fts_query) = request.full_text_query {
            // Handle string_query (simple string FTS)
            if let Some(ref string_query) = fts_query.string_query {
                let mut fts = FullTextSearchQuery::new(string_query.query.clone());

                // Apply column filter if specified
                if let Some(ref columns) = string_query.columns
                    && !columns.is_empty()
                {
                    fts = fts
                        .with_columns(columns)
                        .map_err(|e| NamespaceError::InvalidInput {
                            message: format!("Invalid FTS columns: {:?}", e),
                        })?;
                }

                scanner
                    .full_text_search(fts)
                    .map_err(|e| NamespaceError::InvalidInput {
                        message: format!("Invalid full text search: {:?}", e),
                    })?;
            } else if let Some(ref structured_query) = fts_query.structured_query {
                // Structured FTS: map the namespace query model into the engine FtsQuery.
                let engine_query = build_engine_fts_query(&structured_query.query)?;
                let fts = FullTextSearchQuery::new_query(engine_query);
                scanner
                    .full_text_search(fts)
                    .map_err(|e| NamespaceError::InvalidInput {
                        message: format!("Invalid full text search: {:?}", e),
                    })?;
            }
        }

        // Apply column projection if specified
        if let Some(ref columns) = request.columns {
            if let Some(ref column_names) = columns.column_names
                && !column_names.is_empty()
            {
                scanner
                    .project(column_names)
                    .map_err(|e| NamespaceError::InvalidInput {
                        message: format!("Invalid column projection: {:?}", e),
                    })?;
            } else if let Some(ref column_aliases) = columns.column_aliases
                && !column_aliases.is_empty()
            {
                // column_aliases is HashMap<String, String> where key is alias, value is SQL expression
                let transform_pairs: Vec<(String, String)> = column_aliases
                    .iter()
                    .map(|(alias, sql)| (alias.clone(), sql.clone()))
                    .collect();
                scanner
                    .project_with_transform(
                        &transform_pairs
                            .iter()
                            .map(|(a, s)| (a.as_str(), s.as_str()))
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|e| NamespaceError::InvalidInput {
                        message: format!("Invalid column alias expression: {:?}", e),
                    })?;
            }
        }

        // Apply filter if specified
        if let Some(ref filter) = request.filter
            && !filter.is_empty()
        {
            scanner
                .filter(filter)
                .map_err(|e| NamespaceError::InvalidInput {
                    message: format!("Invalid filter expression: {:?}", e),
                })?;
        }

        // Apply with_row_id if requested
        if request.with_row_id == Some(true) {
            scanner.with_row_id();
        }

        // Apply limit if specified (k is the number of results to return)
        // k == 0 means no limit
        // Note: For vector search, limit is already applied via nearest()
        if !has_vector_query && request.k > 0 {
            let offset = request.offset.map(|o| o as i64);
            scanner.limit(Some(request.k as i64), offset).map_err(|e| {
                NamespaceError::InvalidInput {
                    message: format!("Invalid limit/offset: {:?}", e),
                }
            })?;
        } else if has_vector_query && request.offset.is_some() {
            // For vector search, offset is handled separately
            let offset = request.offset.map(|o| o as i64);
            scanner
                .limit(None, offset)
                .map_err(|e| NamespaceError::InvalidInput {
                    message: format!("Invalid offset: {:?}", e),
                })?;
        }

        // Execute the scan and collect results
        let batch = scanner
            .try_into_batch()
            .await
            .map_err(|e| NamespaceError::Internal {
                message: format!("Failed to execute query: {:?}", e),
            })?;

        // Serialize to Arrow IPC file format
        let schema = batch.schema();
        let mut buffer = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buffer, &schema).map_err(|e| {
                NamespaceError::Internal {
                    message: format!("Failed to create IPC writer: {:?}", e),
                }
            })?;
            writer.write(&batch).map_err(|e| NamespaceError::Internal {
                message: format!("Failed to write batch to IPC: {:?}", e),
            })?;
            writer.finish().map_err(|e| NamespaceError::Internal {
                message: format!("Failed to finish IPC writer: {:?}", e),
            })?;
        }

        Ok(Bytes::from(buffer))
    }

    async fn list_table_tags(
        &self,
        request: ListTableTagsRequest,
    ) -> Result<ListTableTagsResponse> {
        self.record_op("list_table_tags");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "list_table_tags")
            .await?;

        let raw_tags = dataset.tags().list().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!("Failed to list tags for table at '{}': {}", table_uri, e),
            })
        })?;

        let tags = raw_tags
            .into_iter()
            .map(|(name, contents)| {
                let mut tag_model =
                    ModelTagContents::new(contents.version as i64, contents.manifest_size as i64);
                tag_model.branch = contents.branch;
                (name, tag_model)
            })
            .collect();

        Ok(ListTableTagsResponse {
            tags,
            page_token: None,
        })
    }

    async fn get_table_tag_version(
        &self,
        request: GetTableTagVersionRequest,
    ) -> Result<GetTableTagVersionResponse> {
        self.record_op("get_table_tag_version");
        if request.tag.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "tag name must not be empty for get_table_tag_version".to_string(),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "get_table_tag_version")
            .await?;

        let contents = dataset
            .tags()
            .get(&request.tag)
            .await
            .map_err(|e| Self::map_tag_error(e, &request.tag, &table_uri))?;

        Ok(GetTableTagVersionResponse {
            version: contents.version as i64,
            branch: contents.branch,
        })
    }

    async fn create_table_tag(
        &self,
        request: CreateTableTagRequest,
    ) -> Result<CreateTableTagResponse> {
        self.record_op("create_table_tag");
        if request.tag.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "tag name must not be empty for create_table_tag".to_string(),
            }
            .into());
        }
        if request.version <= 0 {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "tag version must be a positive integer, got {} for create_table_tag",
                    request.version
                ),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "create_table_tag")
            .await?;

        dataset
            .tags()
            .create(&request.tag, request.version as u64)
            .await
            .map_err(|e| Self::map_tag_error(e, &request.tag, &table_uri))?;

        Ok(CreateTableTagResponse {
            transaction_id: None,
        })
    }

    async fn delete_table_tag(
        &self,
        request: DeleteTableTagRequest,
    ) -> Result<DeleteTableTagResponse> {
        self.record_op("delete_table_tag");
        if request.tag.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "tag name must not be empty for delete_table_tag".to_string(),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "delete_table_tag")
            .await?;

        dataset
            .tags()
            .delete(&request.tag)
            .await
            .map_err(|e| Self::map_tag_error(e, &request.tag, &table_uri))?;

        Ok(DeleteTableTagResponse {
            transaction_id: None,
        })
    }

    async fn update_table_tag(
        &self,
        request: UpdateTableTagRequest,
    ) -> Result<UpdateTableTagResponse> {
        self.record_op("update_table_tag");
        if request.tag.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "tag name must not be empty for update_table_tag".to_string(),
            }
            .into());
        }
        if request.version <= 0 {
            return Err(NamespaceError::InvalidInput {
                message: format!(
                    "tag version must be a positive integer, got {} for update_table_tag",
                    request.version
                ),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "update_table_tag")
            .await?;

        dataset
            .tags()
            .update(&request.tag, request.version as u64)
            .await
            .map_err(|e| Self::map_tag_error(e, &request.tag, &table_uri))?;

        Ok(UpdateTableTagResponse {
            transaction_id: None,
        })
    }

    async fn create_table_branch(
        &self,
        request: CreateTableBranchRequest,
    ) -> Result<CreateTableBranchResponse> {
        self.record_op("create_table_branch");
        if request.name.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "branch name must not be empty for create_table_branch".to_string(),
            }
            .into());
        }
        let from_version = match request.from_version {
            Some(v) if v <= 0 => {
                return Err(NamespaceError::InvalidInput {
                    message: format!(
                        "from_version must be a positive integer, got {} for create_table_branch",
                        v
                    ),
                }
                .into());
            }
            Some(v) => Some(v as u64),
            None => None,
        };

        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "create_table_branch")
            .await?;

        // Best-effort pre-check: a duplicate returns a clean TableBranchAlreadyExists conflict
        // instead of the opaque Internal error create_branch raises on a pre-existing branch. A
        // concurrent create can still race past this window. Remove once lance-core create_branch
        // returns RefConflict up front.
        if dataset.branches().get(&request.name).await.is_ok() {
            return Err(NamespaceError::TableBranchAlreadyExists {
                message: format!("branch '{}' for table at '{}'", request.name, table_uri),
            }
            .into());
        }

        dataset
            .create_branch(
                &request.name,
                (request.from_branch.as_deref(), from_version),
                None,
            )
            .await
            .map_err(|e| {
                // After load_dataset + the dup pre-check, a DatasetNotFound from create_branch
                // means the requested fork source (from_branch/from_version) doesn't exist.
                if matches!(e, lance_core::Error::DatasetNotFound { .. }) {
                    NamespaceError::InvalidInput {
                        message: format!(
                            "from_branch/from_version for branch '{}' refers to a source that does not exist: {}",
                            request.name, e
                        ),
                    }
                    .into()
                } else {
                    Self::map_branch_error(e, &request.name, &table_uri)
                }
            })?;

        Ok(CreateTableBranchResponse {
            transaction_id: None,
        })
    }

    async fn list_table_branches(
        &self,
        request: ListTableBranchesRequest,
    ) -> Result<ListTableBranchesResponse> {
        self.record_op("list_table_branches");
        let table_uri = self.resolve_table_location(&request.id).await?;
        let dataset = self
            .load_dataset(&table_uri, None, "list_table_branches")
            .await?;

        let raw_branches = dataset.list_branches().await.map_err(|e| {
            lance_core::Error::from(NamespaceError::Internal {
                message: format!(
                    "Failed to list branches for table at '{}': {}",
                    table_uri, e
                ),
            })
        })?;

        let branches = raw_branches
            .into_iter()
            .map(|(name, contents)| {
                // The namespace `BranchContents` model has no `identifier` field, so the
                // lance-core branch identifier is intentionally dropped here.
                let mut branch_model = ModelBranchContents::new(
                    contents.parent_version as i64,
                    contents.create_at as i64,
                    contents.manifest_size as i64,
                );
                branch_model.parent_branch = contents.parent_branch;
                branch_model.metadata = if contents.metadata.is_empty() {
                    None
                } else {
                    Some(contents.metadata)
                };
                (name, branch_model)
            })
            .collect();

        Ok(ListTableBranchesResponse {
            branches,
            page_token: None,
        })
    }

    async fn delete_table_branch(
        &self,
        request: DeleteTableBranchRequest,
    ) -> Result<DeleteTableBranchResponse> {
        self.record_op("delete_table_branch");
        if request.name.is_empty() {
            return Err(NamespaceError::InvalidInput {
                message: "branch name must not be empty for delete_table_branch".to_string(),
            }
            .into());
        }

        let table_uri = self.resolve_table_location(&request.id).await?;
        let mut dataset = self
            .load_dataset(&table_uri, None, "delete_table_branch")
            .await?;

        dataset
            .delete_branch(&request.name)
            .await
            .map_err(|e| match e {
                lance_core::Error::RefConflict { message } => NamespaceError::InvalidInput {
                    message: format!(
                        "branch '{}' for table at '{}': {}",
                        request.name, table_uri, message
                    ),
                }
                .into(),
                other => Self::map_branch_error(other, &request.name, &table_uri),
            })?;

        Ok(DeleteTableBranchResponse {
            transaction_id: None,
        })
    }

    fn namespace_id(&self) -> String {
        format!("DirectoryNamespace {{ root: {:?} }}", self.root)
    }
}

/// Error from [`put_marker_file_atomic`].
#[derive(Debug)]
pub(crate) enum MarkerFileError {
    /// The final marker path is already present (Create / rename race).
    AlreadyExists { description: String },
    /// Staging or publish failed for a non-conflict reason.
    Other { message: String },
}

impl std::fmt::Display for MarkerFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { description } => {
                write!(f, "{} already exists", description)
            }
            Self::Other { message } => write!(f, "{}", message),
        }
    }
}

/// Atomically create a marker file (e.g. `.lance-reserved`) with Create semantics.
///
/// Some object stores implement `PutMode::Create` via temp+rename that reuses the
/// final basename. Dotfile targets such as `.lance-reserved` therefore produce
/// temp names containing `..`, which these stores reject. Stage under a non-dot
/// sibling, then claim the final path with `rename_if_not_exists`.
///
/// When `rename_if_not_exists` is unavailable, fall back to
/// `copy_if_not_exists(staging → target)`, then `PutMode::Create` on the target.
/// That Create path is only for stores whose Create is a true conditional PUT
/// (not basename-derived temp+rename); such stores are exactly the ones that
/// typically omit rename/copy conditionals.
///
/// Some object stores also fail to flush empty objects, so the conditional rename
/// can fail with NotFound. Use a tiny non-empty payload.
///
/// Staging cleanup is best-effort with a few short retries. A delete that still
/// fails after retries leaves a tiny `lance-marker.staging.*` orphan. Async Drop
/// cannot await object-store I/O, so RAII is not used here. Each call uses a
/// unique staging UUID, so concurrent callers never contend on the same cleanup.
pub(crate) async fn put_marker_file_atomic(
    object_store: &ObjectStore,
    path: &Path,
    file_description: &str,
) -> std::result::Result<(), MarkerFileError> {
    let staging_name = format!("lance-marker.staging.{}", uuid::Uuid::new_v4().simple());
    let path_str = path.as_ref();
    let staging_path = match path_str.rfind('/') {
        Some(idx) => Path::from(format!("{}/{}", &path_str[..idx], staging_name)),
        None => Path::from(staging_name.as_str()),
    };

    object_store
        .inner
        .put(&staging_path, bytes::Bytes::from_static(b"reserved").into())
        .await
        .map_err(|e| MarkerFileError::Other {
            message: format!("Failed to stage {}: {:?}", file_description, e),
        })?;

    // Successful rename consumes the staging object; every other path must
    // delete it (best-effort) so conflict/fallback races do not accumulate.
    let mut staging_consumed = false;
    let publish_result = match object_store
        .inner
        .rename_if_not_exists(&staging_path, path)
        .await
    {
        Ok(()) => {
            staging_consumed = true;
            Ok(())
        }
        Err(ObjectStoreError::NotImplemented { .. })
        | Err(ObjectStoreError::NotSupported { .. }) => {
            match object_store
                .inner
                .copy_if_not_exists(&staging_path, path)
                .await
            {
                Ok(()) => Ok(()),
                Err(ObjectStoreError::NotImplemented { .. })
                | Err(ObjectStoreError::NotSupported { .. }) => object_store
                    .inner
                    .put_opts(
                        path,
                        bytes::Bytes::from_static(b"reserved").into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ()),
                Err(e) => Err(e),
            }
        }
        Err(e) => Err(e),
    };

    if !staging_consumed {
        delete_staging_marker_best_effort(object_store, &staging_path).await;
    }

    match publish_result {
        Ok(()) => Ok(()),
        Err(ObjectStoreError::AlreadyExists { .. })
        | Err(ObjectStoreError::Precondition { .. }) => Err(MarkerFileError::AlreadyExists {
            description: file_description.to_string(),
        }),
        Err(e) => Err(MarkerFileError::Other {
            message: format!("Failed to create {}: {:?}", file_description, e),
        }),
    }
}

/// Best-effort delete of a per-call staging marker, with short retries for
/// transient store errors. `NotFound` is treated as success (delete may have
/// succeeded despite an earlier ambiguous failure).
async fn delete_staging_marker_best_effort(object_store: &ObjectStore, staging_path: &Path) {
    const MAX_ATTEMPTS: u32 = 3;
    const BACKOFF_MS: [u64; 2] = [20, 50];

    let mut last_err: Option<ObjectStoreError> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match object_store.inner.delete(staging_path).await {
            Ok(()) => return,
            Err(ObjectStoreError::NotFound { .. }) => return,
            Err(e) => {
                last_err = Some(e);
                if let Some(&delay_ms) = BACKOFF_MS.get(attempt as usize) {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
    if let Some(del_err) = last_err {
        log::warn!(
            "Failed to delete staging marker at '{}' after {} attempts: {:?}",
            staging_path,
            MAX_ATTEMPTS,
            del_err
        );
    }
}

/// Maps a namespace structured `FtsQuery` model into the engine `FtsQuery`. Mirrors the mapping the
/// JNI scanner performs, so the local `queryTable` path honors `structured_query` the same way a
/// `fragment.newScan(fullTextQuery)` does.
fn build_engine_fts_query(
    query: &lance_namespace::models::FtsQuery,
) -> std::result::Result<FtsQuery, NamespaceError> {
    if let Some(ref m) = query.r#match {
        Ok(FtsQuery::Match(build_engine_match_query(m)?))
    } else if let Some(ref p) = query.phrase {
        let mut phrase = PhraseQuery::new(p.terms.clone());
        if let Some(ref column) = p.column {
            phrase = phrase.with_column(Some(column.clone()));
        }
        if let Some(slop) = p.slop {
            phrase = phrase.with_slop(slop as u32);
        }
        Ok(FtsQuery::Phrase(phrase))
    } else if let Some(ref mm) = query.multi_match {
        let match_queries = mm
            .match_queries
            .iter()
            .map(build_engine_match_query)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(FtsQuery::MultiMatch(MultiMatchQuery { match_queries }))
    } else if let Some(ref b) = query.boolean {
        let mut clauses: Vec<(Occur, FtsQuery)> = Vec::new();
        for clause in &b.must {
            clauses.push((Occur::Must, build_engine_fts_query(clause)?));
        }
        for clause in &b.should {
            clauses.push((Occur::Should, build_engine_fts_query(clause)?));
        }
        for clause in &b.must_not {
            clauses.push((Occur::MustNot, build_engine_fts_query(clause)?));
        }
        Ok(FtsQuery::Boolean(BooleanQuery::new(clauses)))
    } else if let Some(ref boost) = query.boost {
        let positive = build_engine_fts_query(&boost.positive)?;
        let negative = build_engine_fts_query(&boost.negative)?;
        Ok(FtsQuery::Boost(BoostQuery::new(
            positive,
            negative,
            boost.negative_boost,
        )))
    } else {
        Err(NamespaceError::InvalidInput {
            message: "structured_query.query must set exactly one of match, phrase, multi_match, \
                      boolean, or boost"
                .to_string(),
        })
    }
}

fn build_engine_match_query(
    m: &lance_namespace::models::MatchQuery,
) -> std::result::Result<MatchQuery, NamespaceError> {
    let mut match_query = MatchQuery::new(m.terms.clone());
    if let Some(ref column) = m.column {
        match_query = match_query.with_column(Some(column.clone()));
    }
    if let Some(boost) = m.boost {
        match_query = match_query.with_boost(boost);
    }
    if let Some(fuzziness) = m.fuzziness {
        match_query = match_query.with_fuzziness(Some(fuzziness as u32));
    }
    if let Some(max_expansions) = m.max_expansions {
        match_query = match_query.with_max_expansions(max_expansions as usize);
    }
    if let Some(ref operator) = m.operator {
        let op =
            Operator::try_from(operator.as_str()).map_err(|e| NamespaceError::InvalidInput {
                message: format!("Invalid FTS operator: {:?}", e),
            })?;
        match_query = match_query.with_operator(op);
    }
    if let Some(prefix_length) = m.prefix_length {
        match_query = match_query.with_prefix_length(prefix_length as u32);
    }
    Ok(match_query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::reader::{FileReader, StreamReader};

    #[test]
    fn test_build_engine_fts_query_match() {
        let mut ns_match = lance_namespace::models::MatchQuery::new("hello world".to_string());
        ns_match.column = Some("body".to_string());
        ns_match.operator = Some("AND".to_string());
        ns_match.fuzziness = Some(1);
        ns_match.max_expansions = Some(30);
        ns_match.boost = Some(2.0);
        ns_match.prefix_length = Some(2);

        let mut ns_query = lance_namespace::models::FtsQuery::new();
        ns_query.r#match = Some(Box::new(ns_match));

        match build_engine_fts_query(&ns_query).unwrap() {
            FtsQuery::Match(m) => {
                assert_eq!(m.terms, "hello world");
                assert_eq!(m.column, Some("body".to_string()));
                assert_eq!(m.operator, Operator::And);
                assert_eq!(m.fuzziness, Some(1));
                assert_eq!(m.max_expansions, 30);
                assert_eq!(m.boost, 2.0);
                assert_eq!(m.prefix_length, 2);
            }
            other => panic!("expected Match, got {:?}", other),
        }
    }

    /// Wraps a namespace `MatchQuery` (with a column) as an `FtsQuery` for use as a clause in
    /// compound queries (boolean / boost).
    fn ns_match_query(terms: &str, column: &str) -> lance_namespace::models::FtsQuery {
        let mut m = lance_namespace::models::MatchQuery::new(terms.to_string());
        m.column = Some(column.to_string());
        let mut q = lance_namespace::models::FtsQuery::new();
        q.r#match = Some(Box::new(m));
        q
    }

    #[test]
    fn test_build_engine_fts_query_phrase() {
        let mut ns_phrase = lance_namespace::models::PhraseQuery::new("hello world".to_string());
        ns_phrase.column = Some("body".to_string());
        ns_phrase.slop = Some(2);

        let mut ns_query = lance_namespace::models::FtsQuery::new();
        ns_query.phrase = Some(Box::new(ns_phrase));

        match build_engine_fts_query(&ns_query).unwrap() {
            FtsQuery::Phrase(p) => {
                assert_eq!(p.terms, "hello world");
                assert_eq!(p.column, Some("body".to_string()));
                assert_eq!(p.slop, 2);
            }
            other => panic!("expected Phrase, got {:?}", other),
        }
    }

    #[test]
    fn test_build_engine_fts_query_multi_match() {
        let mut m1 = lance_namespace::models::MatchQuery::new("hello".to_string());
        m1.column = Some("title".to_string());
        let mut m2 = lance_namespace::models::MatchQuery::new("hello".to_string());
        m2.column = Some("body".to_string());
        m2.boost = Some(2.0);

        let ns_multi = lance_namespace::models::MultiMatchQuery::new(vec![m1, m2]);
        let mut ns_query = lance_namespace::models::FtsQuery::new();
        ns_query.multi_match = Some(Box::new(ns_multi));

        match build_engine_fts_query(&ns_query).unwrap() {
            FtsQuery::MultiMatch(mm) => {
                assert_eq!(mm.match_queries.len(), 2);
                assert_eq!(mm.match_queries[0].terms, "hello");
                assert_eq!(mm.match_queries[0].column, Some("title".to_string()));
                assert_eq!(mm.match_queries[1].column, Some("body".to_string()));
                assert_eq!(mm.match_queries[1].boost, 2.0);
            }
            other => panic!("expected MultiMatch, got {:?}", other),
        }
    }

    #[test]
    fn test_build_engine_fts_query_boolean() {
        // BooleanQuery::new(must, must_not, should)
        let ns_boolean = lance_namespace::models::BooleanQuery::new(
            vec![ns_match_query("must-term", "body")],
            vec![ns_match_query("must-not-term", "body")],
            vec![ns_match_query("should-term", "body")],
        );
        let mut ns_query = lance_namespace::models::FtsQuery::new();
        ns_query.boolean = Some(Box::new(ns_boolean));

        match build_engine_fts_query(&ns_query).unwrap() {
            FtsQuery::Boolean(b) => {
                assert!(matches!(&b.must[..], [FtsQuery::Match(m)] if m.terms == "must-term"));
                assert!(
                    matches!(&b.must_not[..], [FtsQuery::Match(m)] if m.terms == "must-not-term")
                );
                assert!(matches!(&b.should[..], [FtsQuery::Match(m)] if m.terms == "should-term"));
            }
            other => panic!("expected Boolean, got {:?}", other),
        }
    }

    #[test]
    fn test_build_engine_fts_query_boost() {
        let mut ns_boost = lance_namespace::models::BoostQuery::new(
            ns_match_query("positive-term", "body"),
            ns_match_query("negative-term", "body"),
        );
        ns_boost.negative_boost = Some(0.25);

        let mut ns_query = lance_namespace::models::FtsQuery::new();
        ns_query.boost = Some(Box::new(ns_boost));

        match build_engine_fts_query(&ns_query).unwrap() {
            FtsQuery::Boost(b) => {
                assert!(
                    matches!(b.positive.as_ref(), FtsQuery::Match(m) if m.terms == "positive-term")
                );
                assert!(
                    matches!(b.negative.as_ref(), FtsQuery::Match(m) if m.terms == "negative-term")
                );
                assert_eq!(b.negative_boost, 0.25);
            }
            other => panic!("expected Boost, got {:?}", other),
        }
    }

    #[test]
    fn test_build_engine_fts_query_requires_a_variant() {
        // An FtsQuery with no variant set is rejected rather than silently ignored.
        let empty = lance_namespace::models::FtsQuery::new();
        assert!(build_engine_fts_query(&empty).is_err());
    }
    use lance::dataset::Dataset;
    use lance::index::DatasetIndexExt;
    use lance_core::utils::tempfile::{TempStdDir, TempStrDir};
    use lance_core::utils::testing::CountingObjectStore;
    use lance_io::object_store::{providers::local::FileStoreProvider, uri_to_url};
    use lance_namespace::error::ErrorCode;
    use lance_namespace::models::{
        CreateTableRequest, JsonArrowDataType, JsonArrowField, JsonArrowSchema, ListTablesRequest,
        QueryTableRequestColumns,
    };
    use lance_namespace::schema::convert_json_arrow_schema;
    use std::io::Cursor;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use url::Url;

    fn assert_plan_contains_all(plan: &str, expected_fragments: &[&str], context: &str) {
        for expected_fragment in expected_fragments {
            assert!(
                plan.contains(expected_fragment),
                "{}. Missing fragment: '{}'. Plan:\n{}",
                context,
                expected_fragment,
                plan
            );
        }
    }

    fn mutation_error_code(err: lance_core::Error) -> ErrorCode {
        match err {
            lance_core::Error::Namespace { source, .. } => source
                .downcast_ref::<NamespaceError>()
                .expect("mutation error should wrap a NamespaceError")
                .code(),
            other => panic!("expected Namespace error, got: {other:?}"),
        }
    }

    /// `map_mutation_error` must classify commit-conflict variants the same way as
    /// `convert_lance_commit_error` in `manifest.rs`: `CommitConflict` is a retries-exhausted
    /// version collision that is safe to retry (`Throttling`), while the semantic-conflict variants
    /// map to `ConcurrentModification`.
    #[test]
    fn test_map_mutation_error_commit_conflict_alignment() {
        let boxed = || -> Box<dyn std::error::Error + Send + Sync + 'static> {
            Box::<dyn std::error::Error + Send + Sync>::from("inner conflict")
        };

        let throttling_cases = vec![lance_core::Error::commit_conflict_source(1, boxed())];
        for err in throttling_cases {
            let code = mutation_error_code(DirectoryNamespace::map_mutation_error(
                err,
                "update",
                "memory://t",
            ));
            assert_eq!(code, ErrorCode::Throttling);
        }

        let concurrent_cases = vec![
            lance_core::Error::too_much_write_contention("contention"),
            lance_core::Error::retryable_commit_conflict_source(1, boxed()),
            lance_core::Error::incompatible_transaction_source(boxed()),
            lance_core::Error::version_conflict("conflict", 0, 3),
        ];
        for err in concurrent_cases {
            let code = mutation_error_code(DirectoryNamespace::map_mutation_error(
                err,
                "update",
                "memory://t",
            ));
            assert_eq!(code, ErrorCode::ConcurrentModification);
        }
    }

    /// Helper to create a test DirectoryNamespace with a temporary directory
    async fn create_test_namespace() -> (DirectoryNamespace, TempStdDir) {
        let temp_dir = TempStdDir::default();

        let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
            .build()
            .await
            .unwrap();
        (namespace, temp_dir)
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    struct CountingFileStoreProvider {
        listing_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl lance_io::object_store::ObjectStoreProvider for CountingFileStoreProvider {
        async fn new_store(
            &self,
            base_path: Url,
            params: &ObjectStoreParams,
        ) -> Result<ObjectStore> {
            let provider = FileStoreProvider;
            let mut store = provider.new_store(base_path, params).await?;
            store.inner = Arc::new(CountingObjectStore::new(
                store.inner.clone(),
                self.listing_count.clone(),
            ));
            Ok(store)
        }

        fn extract_path(&self, url: &Url) -> Result<Path> {
            let provider = FileStoreProvider;
            provider.extract_path(url)
        }

        fn calculate_object_store_prefix(
            &self,
            url: &Url,
            storage_options: Option<&HashMap<String, String>>,
        ) -> Result<String> {
            let provider = FileStoreProvider;
            provider.calculate_object_store_prefix(url, storage_options)
        }
    }

    #[allow(dead_code)]
    fn file_object_store_uri(path: &str) -> String {
        let file_url = uri_to_url(path).unwrap();
        let mut url = Url::parse("file-object-store:///").unwrap();
        url.set_path(file_url.path());
        url.to_string()
    }

    #[allow(dead_code)]
    fn build_listing_counting_session(listing_count: Arc<AtomicUsize>) -> Arc<Session> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "file-object-store",
            Arc::new(CountingFileStoreProvider { listing_count }),
        );
        Arc::new(Session::new(0, 0, registry))
    }

    // Fault-injection store: returns a runtime-toggleable result from
    // `list_with_delimiter` (the call `check_table_status` makes) and delegates
    // everything else, so a table can be created before failures are injected.
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutPayload, PutResult, Result as OSResult,
    };
    use std::ops::Range;

    #[derive(Debug, Clone, Copy)]
    enum ListBehavior {
        Throttle,
        ServiceUnavailable,
        Internal,
        NotFound,
        EmptyListing,
    }

    #[derive(Debug)]
    struct FailingListStore {
        target: Arc<dyn OSObjectStore>,
        behavior: Arc<Mutex<Option<ListBehavior>>>,
    }

    impl std::fmt::Display for FailingListStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingListStore({})", self.target)
        }
    }

    #[async_trait]
    impl OSObjectStore for FailingListStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.target.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.target.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.target.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.target.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.target.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            let behavior = *self.behavior.lock().unwrap();
            match behavior {
                None => self.target.list_with_delimiter(prefix).await,
                Some(ListBehavior::EmptyListing) => Ok(ListResult {
                    common_prefixes: Vec::new(),
                    objects: Vec::new(),
                }),
                // Mirrors the object_store retry-exhaustion message shape for an
                // Azure ServerBusy response, which is what the incident produced.
                Some(ListBehavior::Throttle) => Err(ObjectStoreError::Generic {
                    store: "test",
                    source: "Error performing list request: response error, after 3 retries, \
                             max_retries: 3, retry_timeout: 180s - HTTP status server error \
                             (503 Service Unavailable): ServerBusy: The server is busy"
                        .into(),
                }),
                Some(ListBehavior::ServiceUnavailable) => Err(ObjectStoreError::Generic {
                    store: "test",
                    source: "Error performing list request: 503 Service Unavailable".into(),
                }),
                Some(ListBehavior::Internal) => Err(ObjectStoreError::Generic {
                    store: "test",
                    source: "Error performing list request: catastrophic unclassified failure"
                        .into(),
                }),
                Some(ListBehavior::NotFound) => Err(ObjectStoreError::NotFound {
                    path: "test_table.lance".to_string(),
                    source: "entity not found".into(),
                }),
            }
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.target.copy_opts(from, to, opts).await
        }
    }

    #[derive(Debug)]
    struct FailingListStoreProvider {
        behavior: Arc<Mutex<Option<ListBehavior>>>,
    }

    #[async_trait]
    impl lance_io::object_store::ObjectStoreProvider for FailingListStoreProvider {
        async fn new_store(
            &self,
            base_path: Url,
            params: &ObjectStoreParams,
        ) -> Result<ObjectStore> {
            let mut store = FileStoreProvider.new_store(base_path, params).await?;
            store.inner = Arc::new(FailingListStore {
                target: store.inner.clone(),
                behavior: self.behavior.clone(),
            });
            Ok(store)
        }

        fn extract_path(&self, url: &Url) -> Result<Path> {
            FileStoreProvider.extract_path(url)
        }

        fn calculate_object_store_prefix(
            &self,
            url: &Url,
            storage_options: Option<&HashMap<String, String>>,
        ) -> Result<String> {
            FileStoreProvider.calculate_object_store_prefix(url, storage_options)
        }
    }

    fn build_failing_list_session(behavior: Arc<Mutex<Option<ListBehavior>>>) -> Arc<Session> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "file-object-store",
            Arc::new(FailingListStoreProvider { behavior }),
        );
        Arc::new(Session::new(0, 0, registry))
    }

    /// Build a dir-listing namespace whose object store's listing calls follow a
    /// shared, runtime-toggleable behavior. Returns the namespace, the temp dir
    /// (kept alive for the store), and the behavior toggle.
    async fn failing_list_namespace() -> (
        DirectoryNamespace,
        TempStdDir,
        Arc<Mutex<Option<ListBehavior>>>,
    ) {
        let temp_dir = TempStdDir::default();
        let root_uri = file_object_store_uri(temp_dir.to_str().unwrap());
        let behavior = Arc::new(Mutex::new(None));
        let session = build_failing_list_session(behavior.clone());
        let namespace = DirectoryNamespaceBuilder::new(root_uri)
            .session(session)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        (namespace, temp_dir, behavior)
    }

    async fn create_named_dir_table(namespace: &DirectoryNamespace, name: &str) {
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec![name.to_string()]);
        namespace
            .create_table(create_req, Bytes::from(ipc_data))
            .await
            .unwrap();
    }

    /// Regression test for the throttling-induced TableNotFound bug: a storage
    /// error while resolving a table must surface as a typed storage error
    /// (Throttling / ServiceUnavailable / Internal) carrying the underlying
    /// evidence in its message — never as TableNotFound.
    #[tokio::test]
    async fn test_table_resolution_propagates_storage_errors_not_table_not_found() {
        for (behavior, expected_code, evidence) in [
            (ListBehavior::Throttle, ErrorCode::Throttling, "serverbusy"),
            (
                ListBehavior::ServiceUnavailable,
                ErrorCode::ServiceUnavailable,
                "503 service unavailable",
            ),
            (ListBehavior::Internal, ErrorCode::Internal, "catastrophic"),
        ] {
            let (namespace, _temp_dir, toggle) = failing_list_namespace().await;
            create_named_dir_table(&namespace, "checkpoint").await;
            *toggle.lock().unwrap() = Some(behavior);

            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["checkpoint".to_string()]);
            let err = namespace.describe_table(describe_req).await.unwrap_err();
            let msg = err.to_string();
            assert_eq!(
                mutation_error_code(err),
                expected_code,
                "describe_table under {behavior:?}; msg: {msg}"
            );
            assert!(
                msg.to_ascii_lowercase().contains(evidence),
                "describe_table message must carry storage evidence '{evidence}', got: {msg}"
            );

            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["checkpoint".to_string()]);
            let err = namespace.table_exists(exists_req).await.unwrap_err();
            let msg = err.to_string();
            assert_eq!(
                mutation_error_code(err),
                expected_code,
                "table_exists under {behavior:?}; msg: {msg}"
            );
            assert!(
                msg.to_ascii_lowercase().contains(evidence),
                "table_exists message must carry storage evidence '{evidence}', got: {msg}"
            );
        }
    }

    /// A genuine not-found error and an empty listing must both still resolve to
    /// TableNotFound (the local-FS and object-store representations of "missing").
    #[tokio::test]
    async fn test_table_resolution_missing_table_yields_table_not_found() {
        for behavior in [ListBehavior::NotFound, ListBehavior::EmptyListing] {
            let (namespace, _temp_dir, toggle) = failing_list_namespace().await;
            *toggle.lock().unwrap() = Some(behavior);

            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["missing".to_string()]);
            let err = namespace.describe_table(describe_req).await.unwrap_err();
            assert_eq!(
                mutation_error_code(err),
                ErrorCode::TableNotFound,
                "describe_table under {behavior:?} should be TableNotFound"
            );

            let mut exists_req = TableExistsRequest::new();
            exists_req.id = Some(vec!["missing".to_string()]);
            let err = namespace.table_exists(exists_req).await.unwrap_err();
            assert_eq!(
                mutation_error_code(err),
                ErrorCode::TableNotFound,
                "table_exists under {behavior:?} should be TableNotFound"
            );
        }
    }

    /// Hybrid (manifest + directory) resolution must exercise the
    /// manifest→directory fall-through for a table that exists on disk but is not
    /// registered in the manifest: the fall-through must succeed normally, and
    /// must not degrade a storage error into TableNotFound.
    ///
    /// A `__manifest` table must actually exist for the manifest branch to run;
    /// otherwise `manifest_ns_for_read()` is None and the manifest branch (and its
    /// fall-through arm) is skipped entirely. We therefore create a *separate*
    /// table through a manifest-enabled namespace first so `__manifest` exists.
    #[tokio::test]
    async fn test_hybrid_resolution_falls_through_and_does_not_mask_throttle() {
        let temp_dir = TempStdDir::default();
        let root_uri = file_object_store_uri(temp_dir.to_str().unwrap());
        let behavior = Arc::new(Mutex::new(None));
        let session = build_failing_list_session(behavior.clone());

        // Seed table via a manifest-enabled namespace so `__manifest` exists.
        let manifest_ns = DirectoryNamespaceBuilder::new(root_uri.clone())
            .session(session.clone())
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        create_named_dir_table(&manifest_ns, "seed").await;

        // The table under test: on disk but never registered in the manifest.
        let dir_ns = DirectoryNamespaceBuilder::new(root_uri.clone())
            .session(session.clone())
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        create_named_dir_table(&dir_ns, "checkpoint").await;

        // Migration enabled so root-level reads consult the manifest and fall
        // through to the directory check on a manifest miss.
        let hybrid_ns = DirectoryNamespaceBuilder::new(root_uri)
            .session(session)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();

        // (a) Healthy fall-through: the manifest reports "checkpoint" absent and it
        // resolves via the directory listing (guards the migration lookup).
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["checkpoint".to_string()]);
        hybrid_ns
            .describe_table(describe_req)
            .await
            .expect("unregistered on-disk table should resolve via the manifest fall-through");

        // (b) The throttle here surfaces from the directory check after the manifest
        // reports absent; the fall-through arm's own storage-error guard is covered
        // by the classify_storage_error / is_manifest_table_absent_error unit tests.
        *behavior.lock().unwrap() = Some(ListBehavior::Throttle);
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["checkpoint".to_string()]);
        let err = hybrid_ns.describe_table(describe_req).await.unwrap_err();
        let code = mutation_error_code(err);
        assert_ne!(
            code,
            ErrorCode::TableNotFound,
            "hybrid resolution masked a throttle as TableNotFound"
        );
        assert!(
            matches!(
                code,
                ErrorCode::Throttling | ErrorCode::ServiceUnavailable | ErrorCode::Internal
            ),
            "hybrid resolution should surface a storage error, got {code:?}"
        );
    }

    #[test]
    fn test_classify_storage_error_maps_variants_and_preserves_evidence() {
        let throttle: Error = ObjectStoreError::Generic {
            store: "test",
            source: "list request failed, after 3 retries, max_retries: 3 - 503 ServerBusy".into(),
        }
        .into();
        assert!(matches!(throttle, Error::IO { .. }));
        let classified = DirectoryNamespace::classify_storage_error(throttle);
        let msg = classified.to_string();
        assert_eq!(mutation_error_code(classified), ErrorCode::Throttling);
        assert!(
            msg.to_ascii_lowercase().contains("serverbusy"),
            "throttle evidence lost: {msg}"
        );

        let service: Error = ObjectStoreError::Generic {
            store: "test",
            source: "504 Gateway Timeout".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::classify_storage_error(service)),
            ErrorCode::ServiceUnavailable
        );

        let internal: Error = ObjectStoreError::Generic {
            store: "test",
            source: "disk caught fire".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::classify_storage_error(internal)),
            ErrorCode::Internal
        );

        // A pre-existing namespace error keeps its own code rather than being reclassified.
        let preexisting: Error = NamespaceError::TableAlreadyExists {
            message: "t".to_string(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::classify_storage_error(preexisting)),
            ErrorCode::TableAlreadyExists
        );
    }

    #[test]
    fn test_is_manifest_table_absent_error() {
        let table_not_found: Error = NamespaceError::TableNotFound {
            message: "t".to_string(),
        }
        .into();
        assert!(DirectoryNamespace::is_manifest_table_absent_error(
            &table_not_found
        ));
        let raw_not_found: Error = ObjectStoreError::NotFound {
            path: "t".to_string(),
            source: "x".into(),
        }
        .into();
        assert!(DirectoryNamespace::is_manifest_table_absent_error(
            &raw_not_found
        ));

        let throttle: Error = ObjectStoreError::Generic {
            store: "test",
            source: "after 3 retries, max_retries: 3 ServerBusy".into(),
        }
        .into();
        assert!(!DirectoryNamespace::is_manifest_table_absent_error(
            &throttle
        ));
        let internal: Error = NamespaceError::Internal {
            message: "boom".to_string(),
        }
        .into();
        assert!(!DirectoryNamespace::is_manifest_table_absent_error(
            &internal
        ));
    }

    #[test]
    fn test_map_open_error() {
        let not_found = || NamespaceError::TableNotFound {
            message: "table at 'x' not found: ...".to_string(),
        };

        let throttle: Error = ObjectStoreError::Generic {
            store: "test",
            source: "after 3 retries, max_retries: 3 - 503 ServerBusy".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(throttle, not_found())),
            ErrorCode::Throttling
        );

        let generic_io: Error = ObjectStoreError::Generic {
            store: "test",
            source: "connection reset".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(generic_io, not_found())),
            ErrorCode::Internal
        );

        let io_not_found: Error = ObjectStoreError::NotFound {
            path: "x".to_string(),
            source: "missing".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(
                io_not_found,
                not_found()
            )),
            ErrorCode::TableNotFound
        );

        let dataset_not_found = Error::dataset_not_found("x".to_string(), "missing".into());
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(
                dataset_not_found,
                not_found()
            )),
            ErrorCode::TableNotFound
        );

        // RefNotFound is not an IO error, so it is not reclassified as a storage error.
        let ref_not_found = Error::RefNotFound {
            message: "branch 'b' does not exist".to_string(),
        };
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(
                ref_not_found,
                not_found()
            )),
            ErrorCode::TableNotFound
        );

        // The caller's not-found variant is honored, but a throttle still propagates.
        let version_miss = Error::RefNotFound {
            message: "version 5 does not exist".to_string(),
        };
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(
                version_miss,
                NamespaceError::TableVersionNotFound {
                    message: "version 5 not found".to_string(),
                },
            )),
            ErrorCode::TableVersionNotFound
        );
        let version_throttle: Error = ObjectStoreError::Generic {
            store: "test",
            source: "after 3 retries, max_retries: 3 - 503 ServerBusy".into(),
        }
        .into();
        assert_eq!(
            mutation_error_code(DirectoryNamespace::map_open_error(
                version_throttle,
                NamespaceError::TableVersionNotFound {
                    message: "version 5 not found".to_string(),
                },
            )),
            ErrorCode::Throttling
        );
    }

    /// Helper to create test IPC data from a schema
    fn create_test_ipc_data(schema: &JsonArrowSchema) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;

        let arrow_schema = convert_json_arrow_schema(schema).unwrap();
        let arrow_schema = Arc::new(arrow_schema);
        let batch = arrow::record_batch::RecordBatch::new_empty(arrow_schema.clone());
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    fn create_ipc_data_from_batches(
        schema: Arc<arrow_schema::Schema>,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).unwrap();
            for batch in &batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }
        buffer
    }

    fn create_non_empty_test_ipc_data() -> Vec<u8> {
        use arrow::array::{Int32Array, StringArray};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(convert_json_arrow_schema(&create_test_schema()).unwrap());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
            ],
        )
        .unwrap();
        create_ipc_data_from_batches(schema, vec![batch])
    }

    fn create_single_row_test_ipc_data() -> Vec<u8> {
        use arrow::array::{Int32Array, StringArray};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(convert_json_arrow_schema(&create_test_schema()).unwrap());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(StringArray::from(vec![Some("carol")])),
            ],
        )
        .unwrap();
        create_ipc_data_from_batches(schema, vec![batch])
    }

    /// Helper to create a simple test schema
    fn create_test_schema() -> JsonArrowSchema {
        let int_type = JsonArrowDataType::new("int32".to_string());
        let string_type = JsonArrowDataType::new("utf8".to_string());

        let id_field = JsonArrowField {
            name: "id".to_string(),
            r#type: Box::new(int_type),
            nullable: false,
            metadata: None,
        };

        let name_field = JsonArrowField {
            name: "name".to_string(),
            r#type: Box::new(string_type),
            nullable: true,
            metadata: None,
        };

        JsonArrowSchema {
            fields: vec![id_field, name_field],
            metadata: None,
        }
    }

    fn create_scalar_table_ipc_data() -> Vec<u8> {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "cory"])),
            ],
        )
        .unwrap();
        create_ipc_data_from_batches(schema, vec![batch])
    }

    async fn create_legacy_manifest_without_primary_key_metadata(root: &str) {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::{RecordBatch, RecordBatchIterator};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("object_id", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
            Field::new(
                "base_objects",
                DataType::List(Arc::new(Field::new("object_id", DataType::Utf8, true))),
                true,
            ),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        Dataset::write(Box::new(reader), &format!("{}/__manifest", root), None)
            .await
            .unwrap();
    }

    async fn manifest_has_primary_key_metadata(root: &str) -> bool {
        let dataset = Dataset::open(&format!("{}/__manifest", root))
            .await
            .unwrap();
        dataset
            .schema()
            .field("object_id")
            .map(|field| {
                field
                    .metadata
                    .contains_key(lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
            })
            .unwrap_or(false)
    }

    fn create_vector_table_ipc_data() -> Vec<u8> {
        use arrow::array::{FixedSizeListArray, Float32Array, Int32Array};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let vector_field = Arc::new(Field::new("item", DataType::Float32, true));
        let vectors = FixedSizeListArray::try_new(
            vector_field,
            2,
            Arc::new(Float32Array::from(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6])),
            None,
        )
        .unwrap();
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(vectors)],
        )
        .unwrap();
        create_ipc_data_from_batches(schema, vec![batch])
    }

    async fn create_scalar_table(namespace: &DirectoryNamespace, table_name: &str) {
        let mut create_table_request = CreateTableRequest::new();
        create_table_request.id = Some(vec![table_name.to_string()]);
        namespace
            .create_table(
                create_table_request,
                Bytes::from(create_scalar_table_ipc_data()),
            )
            .await
            .unwrap();
    }

    async fn create_vector_table(namespace: &DirectoryNamespace, table_name: &str) {
        let mut create_table_request = CreateTableRequest::new();
        create_table_request.id = Some(vec![table_name.to_string()]);
        namespace
            .create_table(
                create_table_request,
                Bytes::from(create_vector_table_ipc_data()),
            )
            .await
            .unwrap();
    }

    async fn open_dataset(namespace: &DirectoryNamespace, table_name: &str) -> Dataset {
        let mut describe_request = DescribeTableRequest::new();
        describe_request.id = Some(vec![table_name.to_string()]);
        let table_uri = namespace
            .describe_table(describe_request)
            .await
            .unwrap()
            .location
            .expect("table location should exist");
        Dataset::open(&table_uri).await.unwrap()
    }

    async fn create_scalar_index(
        namespace: &DirectoryNamespace,
        table_name: &str,
        index_name: &str,
    ) -> Option<String> {
        use lance_namespace::models::CreateTableIndexRequest;

        let mut create_index_request =
            CreateTableIndexRequest::new("id".to_string(), "BTREE".to_string());
        create_index_request.id = Some(vec![table_name.to_string()]);
        create_index_request.name = Some(index_name.to_string());
        namespace
            .create_table_scalar_index(create_index_request)
            .await
            .unwrap()
            .transaction_id
    }

    /// Fork `branch_name` from the table's current version and append
    /// `extra_versions` commits to it (each a new version on the branch, written
    /// with the default V2 naming). The main branch is left untouched. Returns
    /// the branch's storage URI (`<root>/tree/<branch>`).
    async fn create_branch_with_commits(
        namespace: &DirectoryNamespace,
        table_name: &str,
        branch_name: &str,
        extra_versions: usize,
    ) -> String {
        let mut main = open_dataset(namespace, table_name).await;
        let fork_version = main.version().version;
        let branch = main
            .create_branch(branch_name, fork_version, None)
            .await
            .unwrap();
        let branch_uri = branch.uri().to_string();
        for i in 0..extra_versions {
            append_scalar_version(&branch_uri, (i as i32 + 1) * 100).await;
        }
        branch_uri
    }

    /// Append one scalar-schema batch to the dataset at `uri`, creating a new
    /// version (default V2 naming). Shared by branch and main chain setup.
    async fn append_scalar_version(uri: &str, seed: i32) {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![seed, seed + 1])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    }

    /// List a table's versions on `branch` (None == main) via the namespace.
    async fn list_versions(
        namespace: &DirectoryNamespace,
        table_name: &str,
        branch: Option<&str>,
    ) -> Result<Vec<TableVersion>> {
        let req = ListTableVersionsRequest {
            id: Some(vec![table_name.to_string()]),
            branch: branch.map(|b| b.to_string()),
            ..Default::default()
        };
        namespace.list_table_versions(req).await.map(|r| r.versions)
    }

    #[tokio::test]
    async fn test_list_table_versions_on_branch() {
        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        create_branch_with_commits(&namespace, "users", "exp", 2).await;

        // The branch lists its own chain, and every version resolves to a
        // manifest under the branch's tree path.
        let branch_versions = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        assert!(branch_versions.len() >= 2);
        assert!(
            branch_versions
                .iter()
                .all(|v| v.manifest_path.contains("tree/exp")),
            "branch versions must resolve to branch manifests: {:?}",
            branch_versions
        );

        // Unset and "main" behave identically and never see the tree path.
        let main_versions = list_versions(&namespace, "users", None).await.unwrap();
        let main_explicit = list_versions(&namespace, "users", Some("main"))
            .await
            .unwrap();
        assert_eq!(main_versions.len(), main_explicit.len());
        assert!(
            main_versions
                .iter()
                .all(|v| !v.manifest_path.contains("tree/"))
        );

        // A non-existent branch is a clean not-found, not an empty list.
        let missing = list_versions(&namespace, "users", Some("does-not-exist")).await;
        assert!(missing.is_err());
        assert!(missing.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_describe_table_version_on_branch() {
        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        create_branch_with_commits(&namespace, "users", "exp", 2).await;

        let branch_versions = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        let latest = branch_versions.iter().map(|v| v.version).max().unwrap();

        // Describe latest on the branch returns the branch's manifest_path.
        let req = DescribeTableVersionRequest {
            id: Some(vec!["users".to_string()]),
            branch: Some("exp".to_string()),
            ..Default::default()
        };
        let resp = namespace.describe_table_version(req).await.unwrap();
        assert_eq!(resp.version.version, latest);
        assert!(resp.version.manifest_path.contains("tree/exp"));

        // A specific existing branch version resolves.
        let req = DescribeTableVersionRequest {
            id: Some(vec!["users".to_string()]),
            version: Some(latest),
            branch: Some("exp".to_string()),
            ..Default::default()
        };
        assert!(namespace.describe_table_version(req).await.is_ok());

        // A version absent on the branch is not found.
        let req = DescribeTableVersionRequest {
            id: Some(vec!["users".to_string()]),
            version: Some(999_999),
            branch: Some("exp".to_string()),
            ..Default::default()
        };
        assert!(namespace.describe_table_version(req).await.is_err());

        // A non-existent branch is not found.
        let req = DescribeTableVersionRequest {
            id: Some(vec!["users".to_string()]),
            branch: Some("nope".to_string()),
            ..Default::default()
        };
        let err = namespace.describe_table_version(req).await;
        assert!(err.is_err() && err.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_restore_table_on_branch() {
        use lance_namespace::models::RestoreTableRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        create_branch_with_commits(&namespace, "users", "exp", 2).await;

        let before = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        let branch_latest = before.iter().map(|v| v.version).max().unwrap();
        let earliest = before.iter().map(|v| v.version).min().unwrap();
        let main_before = list_versions(&namespace, "users", None)
            .await
            .unwrap()
            .len();

        // Restoring the branch to an earlier version commits a NEW version on
        // the branch (restore is itself a commit), and must not touch main.
        let req = RestoreTableRequest {
            id: Some(vec!["users".to_string()]),
            version: earliest,
            branch: Some("exp".to_string()),
            ..Default::default()
        };
        let resp = namespace.restore_table(req).await.unwrap();
        assert!(resp.transaction_id.is_some());

        let after = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        let new_latest = after.iter().map(|v| v.version).max().unwrap();
        assert!(
            new_latest > branch_latest,
            "restore should add a branch version"
        );

        let main_after = list_versions(&namespace, "users", None)
            .await
            .unwrap()
            .len();
        assert_eq!(main_after, main_before, "main must be unaffected");
    }

    #[tokio::test]
    async fn test_batch_delete_table_versions_on_branch() {
        use lance_namespace::models::{BatchDeleteTableVersionsRequest, VersionRange};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        create_branch_with_commits(&namespace, "users", "exp", 2).await;

        let before = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        let main_before = list_versions(&namespace, "users", None).await.unwrap();

        // Delete the branch's whole history with a through-latest range (end = -1).
        // The branch manifests use V2 naming (inverted, zero-padded), so a nonzero
        // deleted_count proves the V2 fix: the old code constructed
        // "{version}.manifest" and silently matched nothing.
        let req = BatchDeleteTableVersionsRequest {
            id: Some(vec!["users".to_string()]),
            branch: Some("exp".to_string()),
            ranges: vec![VersionRange::new(0, -1)],
            ..Default::default()
        };
        let resp = namespace.batch_delete_table_versions(req).await.unwrap();
        assert_eq!(
            resp.deleted_count,
            Some(before.len() as i64),
            "every branch manifest should be physically deleted"
        );

        // The emptied branch now reads as not-found, and main is untouched.
        assert!(
            list_versions(&namespace, "users", Some("exp"))
                .await
                .is_err()
        );
        let main_after = list_versions(&namespace, "users", None).await.unwrap();
        assert_eq!(
            main_after.len(),
            main_before.len(),
            "main must be untouched"
        );
    }

    #[tokio::test]
    async fn test_create_table_version_on_branch() {
        use futures::TryStreamExt;
        use lance_namespace::models::CreateTableVersionRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let branch_uri = create_branch_with_commits(&namespace, "users", "exp", 1).await;

        // Stage a manifest by copying one of the branch's existing manifests.
        let branch_ds = Dataset::open(&branch_uri).await.unwrap();
        let versions_dir = branch_ds.versions_dir();
        let store = branch_ds.object_store(None).await.unwrap();
        let existing = store
            .inner
            .list(Some(&versions_dir))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("a branch manifest");
        let bytes = store
            .inner
            .get(&existing.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let staging = versions_dir.join("staging_manifest");
        store.inner.put(&staging, bytes.into()).await.unwrap();

        let main_before = list_versions(&namespace, "users", None)
            .await
            .unwrap()
            .len();
        let new_version = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap()
            .iter()
            .map(|v| v.version)
            .max()
            .unwrap()
            + 1;

        let req = CreateTableVersionRequest {
            id: Some(vec!["users".to_string()]),
            version: new_version,
            manifest_path: staging.to_string(),
            naming_scheme: Some("V2".to_string()),
            branch: Some("exp".to_string()),
            ..Default::default()
        };
        let resp = namespace.create_table_version(req).await.unwrap();
        let info = resp.version.expect("version info");
        // The new manifest must land under the branch's tree path.
        assert!(
            info.manifest_path.contains("tree/exp"),
            "got {}",
            info.manifest_path
        );

        // It is visible on the branch, and main did not gain a version.
        let after = list_versions(&namespace, "users", Some("exp"))
            .await
            .unwrap();
        assert!(after.iter().any(|v| v.version == new_version));
        let main_after = list_versions(&namespace, "users", None)
            .await
            .unwrap()
            .len();
        assert_eq!(main_after, main_before, "main must be unaffected");
    }

    /// The namespace-managed commit store derives the branch a request targets
    /// from the base path it is handed, so a single store serves every branch of
    /// the table: a branch-qualified base resolves and commits against the
    /// branch chain while the table root targets main.
    #[tokio::test]
    async fn test_external_manifest_store_resolves_branch_from_base_path() {
        use futures::TryStreamExt;
        use lance::io::commit::namespace_manifest::LanceNamespaceExternalManifestStore;
        use lance_table::io::commit::external_manifest::ExternalManifestStore;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await; // main: version 1
        let branch_uri = create_branch_with_commits(&namespace, "users", "exp", 2).await;

        let namespace = Arc::new(namespace);
        let table_id = vec!["users".to_string()];
        let branch_ds = Dataset::open(&branch_uri).await.unwrap();
        let branch_base = branch_ds.branch_location().path;
        let root_base = branch_ds.branch_location().find_main().unwrap().path;
        let store = LanceNamespaceExternalManifestStore::new(
            namespace.clone(),
            table_id.clone(),
            root_base.clone(),
        );

        // The branch-qualified base resolves the branch chain, the root base
        // resolves main: proof the base path reaches list_table_versions.
        let (branch_latest, branch_path) = store
            .get_latest_version(branch_base.as_ref())
            .await
            .unwrap()
            .expect("branch has versions");
        let (_main_latest, main_path) = store
            .get_latest_version(root_base.as_ref())
            .await
            .unwrap()
            .expect("main has versions");
        assert!(
            branch_path.contains("tree/exp"),
            "branch latest must resolve to the branch tree: {}",
            branch_path
        );
        assert!(
            !main_path.contains("tree/exp"),
            "main latest must not resolve to a branch tree: {}",
            main_path
        );

        // describe (get) with the branch base also resolves to the branch tree.
        let described = store
            .get(branch_base.as_ref(), branch_latest)
            .await
            .unwrap();
        assert!(
            described.contains("tree/exp"),
            "describe on the branch must resolve to the branch tree: {}",
            described
        );

        // A base that is neither the root nor a branch chain is rejected.
        assert!(store.get_latest_version("somewhere/else").await.is_err());

        // Commit (put) with the branch base: the new version must land on the
        // branch chain. Stage a manifest by copying an existing branch manifest.
        let versions_dir = branch_ds.versions_dir();
        let obj = branch_ds.object_store(None).await.unwrap();
        let existing = obj
            .inner
            .list(Some(&versions_dir))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("a branch manifest");
        let bytes = obj
            .inner
            .get(&existing.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let size = bytes.len() as u64;
        let staging = versions_dir.clone().join("staging_manifest");
        obj.inner.put(&staging, bytes.into()).await.unwrap();

        let committed = store
            .put(
                &branch_base,
                branch_latest + 1,
                &staging,
                size,
                None,
                obj.inner.as_ref(),
                ManifestNamingScheme::V2,
            )
            .await
            .unwrap();
        assert!(
            committed.path.to_string().contains("tree/exp"),
            "a commit through a branch-qualified base must land on the branch tree: {}",
            committed.path
        );
    }

    /// write_into_namespace_on_branch must append against the branch chain
    /// THROUGH the managed commit handler: the version is registered with the
    /// namespace (create_table_version), lands on the branch tree, and main's
    /// catalog is untouched. The ops-metrics assertions exist because a
    /// physical-only commit is invisible to DirectoryNamespace branch listing
    /// (it lists storage), while a catalog-authoritative namespace would
    /// silently lose the version.
    #[tokio::test]
    async fn test_write_into_namespace_on_branch_appends_to_branch() {
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableBranchRequest;

        let temp = TempStdDir::default();
        let namespace = Arc::new(
            DirectoryNamespaceBuilder::new(temp.to_str().unwrap())
                .manifest_enabled(true)
                .table_version_tracking_enabled(true)
                .ops_metrics_enabled(true)
                .build()
                .await
                .unwrap(),
        );
        let ns: Arc<dyn LanceNamespace> = namespace.clone();
        let table_id = vec!["t".to_string()];
        create_managed_table(&ns, &table_id).await; // main: v1 (id=1), v2 (id=2)
        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "exp".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        let main_chain_len = |ns: Arc<dyn LanceNamespace>, table_id: Vec<String>| async move {
            ns.list_table_versions(ListTableVersionsRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .unwrap()
            .versions
            .len()
        };
        let main_before = main_chain_len(ns.clone(), table_id.clone()).await;
        let commits_before = namespace
            .retrieve_ops_metrics()
            .get("create_table_version")
            .copied()
            .unwrap_or(0);

        let branch_ds = Dataset::write_into_namespace_on_branch(
            RecordBatchIterator::new(vec![Ok(single_int_batch(3))], single_int_schema()),
            ns.clone(),
            table_id.clone(),
            "exp",
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(branch_ds.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&branch_ds).await, vec![1, 2, 3]);

        // The append must commit through the namespace, not just write a
        // physical manifest under the branch tree.
        let commits_after = namespace
            .retrieve_ops_metrics()
            .get("create_table_version")
            .copied()
            .unwrap_or(0);
        assert_eq!(
            commits_after,
            commits_before + 1,
            "the branch append must register its version via create_table_version"
        );
        let exp_versions = ns
            .list_table_versions(ListTableVersionsRequest {
                id: Some(table_id.clone()),
                branch: Some("exp".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .versions;
        assert!(
            exp_versions
                .iter()
                .all(|v| v.manifest_path.contains("tree/exp")),
            "branch versions must resolve to the branch tree: {:?}",
            exp_versions
        );
        assert_eq!(
            main_chain_len(ns.clone(), table_id.clone()).await,
            main_before,
            "main's catalog must be untouched by the branch append"
        );

        // A managed main append through the same entry point must register in
        // the catalog too, so a fresh managed open resolves the new latest.
        Dataset::write_into_namespace(
            RecordBatchIterator::new(vec![Ok(single_int_batch(100))], single_int_schema()),
            ns.clone(),
            table_id.clone(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            main_chain_len(ns.clone(), table_id.clone()).await,
            main_before + 1,
            "a managed main append must register its version in the catalog"
        );
        let fresh = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(
            scan_id_column(&fresh).await,
            vec![1, 2, 100],
            "a fresh managed open must resolve the appended version, not a stale latest"
        );
    }

    /// CREATE on a branch is rejected: a branch forks from an existing version.
    #[tokio::test]
    async fn test_write_into_namespace_on_branch_rejects_create() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

        let (namespace, _temp_dir) = create_test_namespace().await;
        let namespace = Arc::new(namespace);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let result = Dataset::write_into_namespace_on_branch(
            reader,
            namespace.clone(),
            vec!["new_table".to_string()],
            "exp",
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await;
        assert!(result.is_err(), "create on a branch must be rejected");
        assert!(
            result.unwrap_err().to_string().contains("branch"),
            "error should mention the branch restriction"
        );
    }

    #[tokio::test]
    async fn test_branch_name_validation_rejects_traversal() {
        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        // A traversal-style branch name is rejected as invalid input before any
        // storage path is built from it.
        let err = list_versions(&namespace, "users", Some("../evil")).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("invalid branch name"));
    }

    #[tokio::test]
    async fn test_branch_ops_reject_zombie_branch() {
        use futures::TryStreamExt;
        use lance_namespace::models::{
            BatchDeleteTableVersionsRequest, CreateTableVersionRequest, RestoreTableRequest,
            VersionRange,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        let dataset = open_dataset(&namespace, "users").await;
        let store = dataset.object_store(None).await.unwrap();
        let manifest = store
            .inner
            .list(Some(&dataset.versions_dir()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("a manifest");
        let bytes = store
            .inner
            .get(&manifest.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let zombie = dataset
            .branch_location()
            .find_branch(Some("ghost"))
            .unwrap()
            .path
            .join(VERSIONS_DIR)
            .join(manifest.location.filename().unwrap());
        store.inner.put(&zombie, bytes.into()).await.unwrap();

        assert!(dataset.branches().get("ghost").await.is_err());

        fn rejected<T: std::fmt::Debug>(label: &str, r: Result<T>) {
            match r {
                Ok(v) => panic!("{label} must reject the zombie branch, got Ok({v:?})"),
                Err(e) => assert!(e.to_string().contains("not found"), "{label}: {e}"),
            }
        }

        rejected(
            "list",
            list_versions(&namespace, "users", Some("ghost")).await,
        );
        rejected(
            "describe",
            namespace
                .describe_table_version(DescribeTableVersionRequest {
                    id: Some(vec!["users".to_string()]),
                    branch: Some("ghost".to_string()),
                    ..Default::default()
                })
                .await,
        );
        rejected(
            "create",
            namespace
                .create_table_version(CreateTableVersionRequest {
                    id: Some(vec!["users".to_string()]),
                    version: 2,
                    manifest_path: zombie.to_string(),
                    branch: Some("ghost".to_string()),
                    ..Default::default()
                })
                .await,
        );
        rejected(
            "restore",
            namespace
                .restore_table(RestoreTableRequest {
                    id: Some(vec!["users".to_string()]),
                    version: 1,
                    branch: Some("ghost".to_string()),
                    ..Default::default()
                })
                .await,
        );
        rejected(
            "batch_delete",
            namespace
                .batch_delete_table_versions(BatchDeleteTableVersionsRequest {
                    id: Some(vec!["users".to_string()]),
                    branch: Some("ghost".to_string()),
                    ranges: vec![VersionRange::new(1, 1)],
                    ..Default::default()
                })
                .await,
        );
    }

    /// V2 is the default naming scheme, and the pre-rewrite delete path
    /// constructed `{version}.manifest` (a V1 name) and silently matched nothing
    /// on a V2 table, returning deleted_count 0. This pins the fix on the main
    /// chain (branch=None), which previously had no batch_delete coverage at all.
    #[tokio::test]
    async fn test_batch_delete_table_versions_main_v2() {
        use lance_namespace::models::{BatchDeleteTableVersionsRequest, VersionRange};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await; // version 1
        let main_uri = open_dataset(&namespace, "users").await.uri().to_string();
        append_scalar_version(&main_uri, 100).await; // version 2
        append_scalar_version(&main_uri, 200).await; // version 3

        let before = list_versions(&namespace, "users", None).await.unwrap();
        assert!(before.len() >= 3);
        // Confirm these really are V2-named manifests (20-digit inverted version
        // + ".manifest" == 29 chars), i.e. the case the old code skipped.
        assert!(
            before
                .iter()
                .all(|v| v.manifest_path.rsplit('/').next().unwrap().len() == 29),
            "expected V2-named manifests: {:?}",
            before
        );
        let min_v = before.iter().map(|v| v.version).min().unwrap();
        let max_v = before.iter().map(|v| v.version).max().unwrap();

        // Delete everything except the latest version. end is exclusive, so
        // [min_v, max_v) keeps max_v.
        let req = BatchDeleteTableVersionsRequest {
            id: Some(vec!["users".to_string()]),
            ranges: vec![VersionRange::new(min_v, max_v)],
            ..Default::default()
        };
        let resp = namespace.batch_delete_table_versions(req).await.unwrap();
        assert_eq!(
            resp.deleted_count,
            Some((before.len() - 1) as i64),
            "V2 manifests must actually be deleted (was 0 before the fix)"
        );

        let after = list_versions(&namespace, "users", None).await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].version, max_v);
    }

    /// Pins the exclusive end of VersionRange: [v, v+1) must match only v.
    #[tokio::test]
    async fn test_batch_delete_end_is_exclusive() {
        use lance_namespace::models::{BatchDeleteTableVersionsRequest, VersionRange};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await; // version 1
        let main_uri = open_dataset(&namespace, "users").await.uri().to_string();
        append_scalar_version(&main_uri, 100).await; // version 2
        append_scalar_version(&main_uri, 200).await; // version 3

        let before = list_versions(&namespace, "users", None).await.unwrap();
        let min_v = before.iter().map(|v| v.version).min().unwrap();

        let req = BatchDeleteTableVersionsRequest {
            id: Some(vec!["users".to_string()]),
            ranges: vec![VersionRange::new(min_v, min_v + 1)],
            ..Default::default()
        };
        let resp = namespace.batch_delete_table_versions(req).await.unwrap();
        assert_eq!(
            resp.deleted_count,
            Some(1),
            "only min_v is in [min_v, min_v+1)"
        );

        let after = list_versions(&namespace, "users", None).await.unwrap();
        assert!(
            !after.iter().any(|v| v.version == min_v),
            "min_v must be deleted"
        );
        assert_eq!(after.len(), before.len() - 1, "exactly one version removed");
    }

    #[tokio::test]
    async fn test_batch_delete_rejects_unbounded_range() {
        use lance_namespace::models::{BatchDeleteTableVersionsRequest, VersionRange};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        // An unbounded range must be rejected up front, not turned into ~10^19
        // iterations / an unbounded id list.
        let req = BatchDeleteTableVersionsRequest {
            id: Some(vec!["users".to_string()]),
            ranges: vec![VersionRange::new(0, i64::MAX)],
            ..Default::default()
        };
        let err = namespace.batch_delete_table_versions(req).await;
        assert!(err.is_err());
        assert!(
            err.unwrap_err().to_string().contains("limit"),
            "expected a range-too-large error"
        );
    }

    /// Build a managed (manifest-tracked) namespace over `path`.
    async fn create_managed_namespace(path: &str) -> Arc<dyn LanceNamespace> {
        Arc::new(
            DirectoryNamespaceBuilder::new(path)
                .manifest_enabled(true)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        )
    }

    fn single_int_schema() -> Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]))
    }

    fn single_int_batch(seed: i32) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int32Array;
        arrow::record_batch::RecordBatch::try_new(
            single_int_schema(),
            vec![Arc::new(Int32Array::from(vec![seed]))],
        )
        .unwrap()
    }

    /// Create a managed table with versions v1 (id=1) and v2 (id=2) on main and
    /// return the main dataset handle.
    async fn create_managed_table(ns: &Arc<dyn LanceNamespace>, table_id: &[String]) -> Dataset {
        let mut ds = Dataset::write_into_namespace(
            RecordBatchIterator::new(vec![Ok(single_int_batch(1))], single_int_schema()),
            ns.clone(),
            table_id.to_vec(),
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        ds.append(
            RecordBatchIterator::new(vec![Ok(single_int_batch(2))], single_int_schema()),
            None,
        )
        .await
        .unwrap();
        ds
    }

    /// Sorted values of the `id` column across a full scan.
    async fn scan_id_column(ds: &Dataset) -> Vec<i32> {
        use arrow::array::Int32Array;
        use futures::TryStreamExt;
        let batches: Vec<arrow::record_batch::RecordBatch> = ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort();
        ids
    }

    /// E2e for the managed branch path through the builder: create a branch via the
    /// namespace op, open it with `from_namespace(managed).with_branch`, commit on
    /// it, and confirm the dataset is rooted at the branch chain (manifest, base
    /// path and data placement) while main's catalog is untouched.
    #[tokio::test]
    async fn test_managed_branch_open_and_commit() {
        use futures::TryStreamExt;
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableBranchRequest;

        let temp = TempStdDir::default();
        let ns = create_managed_namespace(temp.to_str().unwrap()).await;
        let table_id = vec!["t".to_string()];
        create_managed_table(&ns, &table_id).await;
        let main_before = ns
            .list_table_versions(ListTableVersionsRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .versions
            .len();

        // Create a branch via the namespace op (the FS-handler path, which succeeds
        // on a managed table).
        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "exp".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        // Open the managed table on the branch: the base path is qualified up
        // front and the manifest store derives the branch from it.
        let mut branch_ds = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_branch("exp", None)
            .load()
            .await
            .unwrap();
        assert_eq!(
            branch_ds.manifest.branch.as_deref(),
            Some("exp"),
            "with_branch on a managed table must open the branch chain"
        );
        let branch_base = branch_ds.branch_location().path;
        assert!(
            branch_base.as_ref().ends_with("tree/exp"),
            "the branch dataset must be rooted at the branch chain: {}",
            branch_base
        );
        let branch_v_before = branch_ds.version().version;

        // Commit on the branch.
        branch_ds
            .append(
                RecordBatchIterator::new(vec![Ok(single_int_batch(3))], single_int_schema()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            branch_ds.manifest.branch.as_deref(),
            Some("exp"),
            "the commit must stay on the branch"
        );
        assert!(
            branch_ds.version().version > branch_v_before,
            "the branch version must advance after the commit"
        );
        assert_eq!(scan_id_column(&branch_ds).await, vec![1, 2, 3]);

        // The committed data files live under the branch chain, not main's data
        // dir, so unmanaged readers of the branch and main's cleanup see a
        // consistent layout.
        let store = branch_ds.object_store(None).await.unwrap();
        let branch_data = branch_base.clone().join("data");
        let branch_files = store
            .inner
            .list(Some(&branch_data))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            !branch_files.is_empty(),
            "the branch commit must place data files under the branch chain"
        );

        // The same branch is readable through the unmanaged (path-based) open.
        let table_uri = ns
            .describe_table(DescribeTableRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .location
            .unwrap();
        let fs_branch_ds = DatasetBuilder::from_uri(&table_uri)
            .with_branch("exp", None)
            .load()
            .await
            .unwrap();
        assert_eq!(fs_branch_ds.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&fs_branch_ds).await, vec![1, 2, 3]);

        // Main's catalog is untouched (branches are not tracked in __manifest),
        // and main still reads its own data.
        let main_after = ns
            .list_table_versions(ListTableVersionsRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .versions
            .len();
        assert_eq!(
            main_after, main_before,
            "committing on the branch must not change main's chain"
        );
        let main_ds = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(main_ds.manifest.branch, None);
        assert_eq!(scan_id_column(&main_ds).await, vec![1, 2]);
    }

    /// Branch-pointing tags on a managed table: create them through the normal
    /// API (from both the main and the branch handle), open the table at the
    /// tag, and check the tag out from an already-open dataset. All of these
    /// must resolve the branch chain, never main's chain.
    #[tokio::test]
    async fn test_managed_branch_tags() {
        use lance::dataset::builder::DatasetBuilder;
        use lance::dataset::refs::Ref;
        use lance_namespace::models::CreateTableBranchRequest;

        let temp = TempStdDir::default();
        let ns = create_managed_namespace(temp.to_str().unwrap()).await;
        let table_id = vec!["t".to_string()];
        let main_ds = create_managed_table(&ns, &table_id).await;
        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "exp".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        let mut branch_ds = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_branch("exp", None)
            .load()
            .await
            .unwrap();
        branch_ds
            .append(
                RecordBatchIterator::new(vec![Ok(single_int_batch(3))], single_int_schema()),
                None,
            )
            .await
            .unwrap();
        let branch_version = branch_ds.version().version;

        // A branch-pointing tag created from the main handle must validate
        // against the branch chain (the version does not exist on main).
        main_ds
            .tags()
            .create("exp-tag", ("exp", Some(branch_version)))
            .await
            .unwrap();
        let tag = main_ds.tags().get("exp-tag").await.unwrap();
        assert_eq!(tag.branch.as_deref(), Some("exp"));
        assert_eq!(tag.version, branch_version);

        // A tag created from the branch handle resolves the branch implicitly.
        branch_ds
            .tags()
            .create("exp-tag2", branch_version)
            .await
            .unwrap();
        let tag2 = branch_ds.tags().get("exp-tag2").await.unwrap();
        assert_eq!(tag2.branch.as_deref(), Some("exp"));

        // Opening the managed table at the branch-pointing tag checks out the
        // branch chain.
        let tag_open = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_tag("exp-tag")
            .load()
            .await
            .unwrap();
        assert_eq!(tag_open.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(tag_open.version().version, branch_version);
        assert_eq!(scan_id_column(&tag_open).await, vec![1, 2, 3]);

        // So does checking the tag out from an already-open main dataset.
        let tag_checkout = main_ds
            .checkout_version(Ref::Tag("exp-tag".to_string()))
            .await
            .unwrap();
        assert_eq!(tag_checkout.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&tag_checkout).await, vec![1, 2, 3]);

        // A missing tag on a managed table errors at open.
        let err = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_tag("no-such-tag")
            .load()
            .await;
        assert!(err.is_err(), "a missing tag must error");
    }

    /// Cross-branch checkout on a managed table, including version numbers that
    /// exist on both chains (branch numbering continues from the fork point, so
    /// overlap is the common case). Every checkout must land on the requested
    /// chain and read that chain's data.
    #[tokio::test]
    async fn test_managed_cross_branch_checkout() {
        use lance::dataset::builder::DatasetBuilder;
        use lance::dataset::refs::Ref;
        use lance_namespace::models::CreateTableBranchRequest;

        let temp = TempStdDir::default();
        let ns = create_managed_namespace(temp.to_str().unwrap()).await;
        let table_id = vec!["t".to_string()];
        let mut main_ds = create_managed_table(&ns, &table_id).await;
        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "exp".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

        // exp gets id=3 at its tip; main gets id=100 at the same version number.
        let mut branch_ds = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_branch("exp", None)
            .load()
            .await
            .unwrap();
        branch_ds
            .append(
                RecordBatchIterator::new(vec![Ok(single_int_batch(3))], single_int_schema()),
                None,
            )
            .await
            .unwrap();
        let overlap_version = branch_ds.version().version;
        while main_ds.version().version < overlap_version {
            main_ds
                .append(
                    RecordBatchIterator::new(vec![Ok(single_int_batch(100))], single_int_schema()),
                    None,
                )
                .await
                .unwrap();
        }

        // main -> branch at the overlapping version number: must read the
        // branch's data, not main's same-numbered version.
        let on_branch = main_ds
            .checkout_version(Ref::Version(Some("exp".to_string()), Some(overlap_version)))
            .await
            .unwrap();
        assert_eq!(on_branch.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&on_branch).await, vec![1, 2, 3]);

        // main -> branch latest.
        let mut on_branch_latest = main_ds.checkout_branch("exp").await.unwrap();
        assert_eq!(on_branch_latest.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(on_branch_latest.version().version, overlap_version);

        // A commit through the checked-out handle (which shares main's commit
        // handler) must land on the branch chain, not main's.
        let main_chain_len = |ns: Arc<dyn LanceNamespace>, table_id: Vec<String>| async move {
            ns.list_table_versions(ListTableVersionsRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .unwrap()
            .versions
            .len()
        };
        let main_before = main_chain_len(ns.clone(), table_id.clone()).await;
        on_branch_latest
            .append(
                RecordBatchIterator::new(vec![Ok(single_int_batch(4))], single_int_schema()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(on_branch_latest.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&on_branch_latest).await, vec![1, 2, 3, 4]);
        assert_eq!(
            main_chain_len(ns.clone(), table_id.clone()).await,
            main_before,
            "a commit on the checked-out branch must not advance main's chain"
        );

        // branch -> main at a specific version.
        let on_main = branch_ds
            .checkout_version(Ref::Version(None, Some(1)))
            .await
            .unwrap();
        assert_eq!(on_main.manifest.branch, None);
        assert_eq!(scan_id_column(&on_main).await, vec![1]);

        // branch -> another branch.
        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "exp2".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        let on_branch2 = branch_ds.checkout_branch("exp2").await.unwrap();
        assert_eq!(on_branch2.manifest.branch.as_deref(), Some("exp2"));

        // A version missing from the branch chain errors loudly.
        let err = main_ds
            .checkout_version(Ref::Version(Some("exp".to_string()), Some(999)))
            .await;
        assert!(err.is_err(), "a version missing from the branch must error");
    }

    /// CommitBuilder must honor an explicitly supplied commit handler for a
    /// Dataset destination: a managed-versioning commit through a dataset that
    /// was opened without the namespace handler (as the Java and Python commit
    /// APIs allow) must still register with the catalog instead of silently
    /// writing a physical manifest the catalog never sees.
    #[tokio::test]
    async fn test_commit_builder_honors_explicit_handler_for_dataset_dest() {
        use lance::dataset::write::{CommitBuilder, InsertBuilder};
        use lance::dataset::{WriteDestination, builder::DatasetBuilder};
        use lance::io::commit::namespace_manifest::LanceNamespaceExternalManifestStore;
        use lance_table::io::commit::external_manifest::ExternalManifestCommitHandler;

        let temp = TempStdDir::default();
        let namespace = Arc::new(
            DirectoryNamespaceBuilder::new(temp.to_str().unwrap())
                .manifest_enabled(true)
                .table_version_tracking_enabled(true)
                .ops_metrics_enabled(true)
                .build()
                .await
                .unwrap(),
        );
        let ns: Arc<dyn LanceNamespace> = namespace.clone();
        let table_id = vec!["t".to_string()];
        create_managed_table(&ns, &table_id).await; // main: v1 (id=1), v2 (id=2)

        // Open WITHOUT the namespace handler, the way a binding caller can.
        let table_uri = ns
            .describe_table(DescribeTableRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .location
            .unwrap();
        let plain_ds = Arc::new(Dataset::open(&table_uri).await.unwrap());

        let transaction = InsertBuilder::new(WriteDestination::Dataset(plain_ds.clone()))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute_uncommitted(vec![single_int_batch(3)])
            .await
            .unwrap();

        let handler = Arc::new(ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(
                LanceNamespaceExternalManifestStore::for_table_uri(
                    ns.clone(),
                    table_id.clone(),
                    &table_uri,
                )
                .unwrap(),
            ),
        });
        let commits_before = namespace
            .retrieve_ops_metrics()
            .get("create_table_version")
            .copied()
            .unwrap_or(0);
        let committed = CommitBuilder::new(WriteDestination::Dataset(plain_ds))
            .with_commit_handler(handler)
            .execute(transaction)
            .await
            .unwrap();
        assert_eq!(scan_id_column(&committed).await, vec![1, 2, 3]);

        let commits_after = namespace
            .retrieve_ops_metrics()
            .get("create_table_version")
            .copied()
            .unwrap_or(0);
        assert_eq!(
            commits_after,
            commits_before + 1,
            "the explicit handler must route the commit through create_table_version"
        );
        let fresh = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(
            scan_id_column(&fresh).await,
            vec![1, 2, 3],
            "a fresh managed open must resolve the committed version"
        );
    }

    /// A branch forked from a non-latest version opens on its own chain.
    #[tokio::test]
    async fn test_managed_branch_from_non_latest_fork() {
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableBranchRequest;

        let temp = TempStdDir::default();
        let ns = create_managed_namespace(temp.to_str().unwrap()).await;
        let table_id = vec!["t".to_string()];
        create_managed_table(&ns, &table_id).await; // main: v1 (id=1), v2 (id=2)

        ns.create_table_branch(CreateTableBranchRequest {
            id: Some(table_id.clone()),
            name: "old".to_string(),
            from_version: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();

        let old_ds = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_branch("old", None)
            .load()
            .await
            .unwrap();
        assert_eq!(old_ds.manifest.branch.as_deref(), Some("old"));
        assert_eq!(
            scan_id_column(&old_ds).await,
            vec![1],
            "the fork must contain only the fork-point data"
        );
    }

    /// The shared parser must decode both naming schemes; this is the cheap
    /// V1 no-regression guard (creating a real V1 table is not exposed here).
    #[test]
    fn test_manifest_version_from_filename() {
        // V1: the plain version number.
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename("5.manifest"),
            Some(5)
        );
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename("0.manifest"),
            Some(0)
        );
        // V2: version stored as u64::MAX - version, zero-padded to 20 digits.
        let v2_five = format!("{:020}.manifest", u64::MAX - 5);
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename(&v2_five),
            Some(5)
        );
        let v2_zero = format!("{:020}.manifest", u64::MAX);
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename(&v2_zero),
            Some(0)
        );
        // Non-manifest and detached (`d`-prefixed) entries are ignored.
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename("data.lance"),
            None
        );
        assert_eq!(
            DirectoryNamespace::manifest_version_from_filename("d5.manifest"),
            None
        );
    }

    #[tokio::test]
    async fn test_create_table() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        assert!(response.location.is_some());
        assert!(response.location.unwrap().ends_with("test_table.lance"));
        assert_eq!(response.version, Some(1));
    }

    #[tokio::test]
    async fn test_create_table_without_data() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let result = namespace.create_table(request, bytes::Bytes::new()).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Arrow IPC stream) is required")
        );
    }

    #[tokio::test]
    async fn test_create_table_with_invalid_id() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Test with empty ID
        let mut request = CreateTableRequest::new();
        request.id = Some(vec![]);

        let result = namespace
            .create_table(request, bytes::Bytes::from(ipc_data.clone()))
            .await;
        assert!(result.is_err());

        // Test with multi-level ID - should now work with manifest enabled
        // First create the parent namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_namespace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Now create table in the namespace
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_namespace".to_string(), "table".to_string()]);

        let result = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await;
        // Should succeed with manifest enabled
        assert!(
            result.is_ok(),
            "Multi-level table IDs should work with manifest enabled"
        );
    }

    #[tokio::test]
    async fn test_list_tables() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Initially, no tables
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0);

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Create a table
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["table1".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data.clone()))
            .await
            .unwrap();

        // Create another table
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["table2".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // List tables should return both
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = namespace.list_tables(request).await.unwrap();
        let tables = response.tables;
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
    }

    #[tokio::test]
    async fn test_list_tables_pagination() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        for name in ["alpha", "bravo", "charlie"] {
            let mut req = CreateTableRequest::new();
            req.id = Some(vec![name.to_string()]);
            namespace
                .create_table(req, bytes::Bytes::from(ipc_data.clone()))
                .await
                .unwrap();
        }

        // First page: limit=2, no page_token
        let first_page = namespace
            .list_tables(ListTablesRequest {
                id: Some(vec![]),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(first_page.tables, vec!["alpha", "bravo"]);
        assert_eq!(first_page.page_token.as_deref(), Some("bravo"));

        // Second page: use page_token from first response
        let second_page = namespace
            .list_tables(ListTablesRequest {
                id: Some(vec![]),
                limit: Some(2),
                page_token: first_page.page_token.clone(),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(second_page.tables, vec!["charlie"]);
        assert!(second_page.page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_tables_pagination_limit_zero() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut req = CreateTableRequest::new();
        req.id = Some(vec!["alpha".to_string()]);
        namespace
            .create_table(req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        let response = namespace
            .list_tables(ListTablesRequest {
                id: Some(vec![]),
                limit: Some(0),
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(response.tables.is_empty());
        assert!(response.page_token.is_none());
    }

    #[tokio::test]
    async fn test_list_tables_with_namespace_id() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // First create a child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_namespace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Now list tables in the child namespace
        let mut request = ListTablesRequest::new();
        request.id = Some(vec!["test_namespace".to_string()]);

        let result = namespace.list_tables(request).await;
        // Should succeed (with manifest enabled) and return empty list (no tables yet)
        assert!(
            result.is_ok(),
            "list_tables should work with child namespace when manifest is enabled"
        );
        let response = result.unwrap();
        assert_eq!(
            response.tables.len(),
            0,
            "Namespace should have no tables yet"
        );
    }

    #[tokio::test]
    async fn test_create_scalar_index() {
        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        let transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;
        let dataset = open_dataset(&namespace, "users").await;
        let expected_transaction_id = dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(transaction_id, expected_transaction_id);
        let indices = dataset.load_indices().await.unwrap();
        assert!(indices.iter().any(|index| index.name == "users_id_idx"));
    }

    #[tokio::test]
    async fn test_create_vector_index() {
        use lance_namespace::models::CreateTableIndexRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_vector_table(&namespace, "vectors").await;

        let mut create_index_request =
            CreateTableIndexRequest::new("vector".to_string(), "IVF_FLAT".to_string());
        create_index_request.id = Some(vec!["vectors".to_string()]);
        create_index_request.name = Some("vector_idx".to_string());
        create_index_request.distance_type = Some("l2".to_string());
        let transaction_id = namespace
            .create_table_index(create_index_request)
            .await
            .unwrap()
            .transaction_id;

        let dataset = open_dataset(&namespace, "vectors").await;
        let expected_transaction_id = dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(transaction_id, expected_transaction_id);
        let indices = dataset.load_indices().await.unwrap();
        assert!(indices.iter().any(|index| index.name == "vector_idx"));
    }

    #[tokio::test]
    async fn test_list_table_indices() {
        use lance_namespace::models::{CreateTableIndexRequest, ListTableIndicesRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        create_scalar_index(&namespace, "users", "a_idx").await;
        create_scalar_index(&namespace, "users", "b_idx").await;
        let transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;

        let response = namespace
            .list_table_indices(ListTableIndicesRequest {
                id: Some(vec!["users".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(response.indexes.len(), 3);
        assert_eq!(response.indexes[0].index_name, "a_idx");
        assert_eq!(response.indexes[1].index_name, "b_idx");
        assert_eq!(response.indexes[2].index_name, "users_id_idx");
        assert!(response.page_token.is_none());
        let users_id_idx = response
            .indexes
            .iter()
            .find(|index| index.index_name == "users_id_idx")
            .unwrap();
        assert_eq!(users_id_idx.columns, vec!["id"]);
        assert_eq!(users_id_idx.status, "SUCCEEDED");

        // Enriched fields populated from the index metadata for a scalar index.
        assert_eq!(users_id_idx.index_type.as_deref(), Some("BTree"));
        assert!(
            users_id_idx
                .type_url
                .as_deref()
                .is_some_and(|s| !s.is_empty())
        );
        assert_eq!(users_id_idx.num_indexed_rows, Some(3));
        assert_eq!(users_id_idx.num_unindexed_rows, Some(0));
        assert_eq!(users_id_idx.num_segments, Some(1));
        assert!(users_id_idx.size_bytes.is_some_and(|size| size > 0));
        assert!(users_id_idx.created_at.is_some());
        assert!(users_id_idx.index_version.is_some());
        assert!(users_id_idx.index_details.is_some());

        let dataset = open_dataset(&namespace, "users").await;
        let expected_transaction_id = dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(transaction_id, expected_transaction_id);
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(
            indices
                .iter()
                .filter(|index| index.name == "users_id_idx")
                .count(),
            1
        );

        let first_page = namespace
            .list_table_indices(ListTableIndicesRequest {
                id: Some(vec!["users".to_string()]),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(first_page.indexes.len(), 2);
        assert_eq!(first_page.indexes[0].index_name, "a_idx");
        assert_eq!(first_page.indexes[1].index_name, "b_idx");
        assert_eq!(first_page.page_token.as_deref(), Some("b_idx"));

        let second_page = namespace
            .list_table_indices(ListTableIndicesRequest {
                id: Some(vec!["users".to_string()]),
                page_token: first_page.page_token.clone(),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(second_page.indexes.len(), 1);
        assert_eq!(second_page.indexes[0].index_name, "users_id_idx");
        assert!(second_page.page_token.is_none());

        // A vector index exercises a different type_url, index_type, and details payload.
        create_vector_table(&namespace, "vectors").await;
        let mut create_index_request =
            CreateTableIndexRequest::new("vector".to_string(), "IVF_FLAT".to_string());
        create_index_request.id = Some(vec!["vectors".to_string()]);
        create_index_request.name = Some("vector_idx".to_string());
        create_index_request.distance_type = Some("l2".to_string());
        namespace
            .create_table_index(create_index_request)
            .await
            .unwrap();

        let vector_response = namespace
            .list_table_indices(ListTableIndicesRequest {
                id: Some(vec!["vectors".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(vector_response.indexes.len(), 1);
        let vector_idx = &vector_response.indexes[0];
        assert_eq!(vector_idx.index_name, "vector_idx");
        assert_eq!(vector_idx.columns, vec!["vector"]);
        assert_eq!(vector_idx.index_type.as_deref(), Some("IVF_FLAT"));
        assert!(
            vector_idx
                .type_url
                .as_deref()
                .is_some_and(|s| !s.is_empty())
        );
        assert!(vector_idx.num_indexed_rows.is_some());
        assert!(vector_idx.num_unindexed_rows.is_some());
        assert_eq!(vector_idx.num_segments, Some(1));
        assert!(vector_idx.created_at.is_some());
        assert!(vector_idx.index_version.is_some());
        assert!(vector_idx.index_details.is_some());
    }

    #[tokio::test]
    async fn test_describe_table_index_stats() {
        use lance_namespace::models::DescribeTableIndexStatsRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;

        let response = namespace
            .describe_table_index_stats(DescribeTableIndexStatsRequest {
                id: Some(vec!["users".to_string()]),
                index_name: Some("users_id_idx".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(response.index_type, Some("BTree".to_string()));
        assert_eq!(response.num_indices, Some(1));
        assert_eq!(response.num_indexed_rows, Some(3));
        assert_eq!(response.num_unindexed_rows, Some(0));

        let dataset = open_dataset(&namespace, "users").await;
        let expected_transaction_id = dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(transaction_id, expected_transaction_id);
        let stats: serde_json::Value =
            serde_json::from_str(&dataset.index_statistics("users_id_idx").await.unwrap()).unwrap();
        assert_eq!(stats["index_type"], "BTree");
        assert_eq!(stats["num_indices"], 1);
        assert_eq!(stats["num_indexed_rows"], 3);
        assert_eq!(stats["num_unindexed_rows"], 0);
    }

    #[tokio::test]
    async fn test_describe_transaction() {
        use lance_namespace::models::DescribeTransactionRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;
        let dataset = open_dataset(&namespace, "users").await;
        let latest_transaction = dataset.read_transaction().await.unwrap();
        assert_eq!(
            transaction_id,
            latest_transaction
                .as_ref()
                .map(|transaction| transaction.uuid.clone())
        );

        if let Some(transaction_id) = transaction_id {
            let response = namespace
                .describe_transaction(DescribeTransactionRequest {
                    id: Some(vec!["users".to_string(), transaction_id.clone()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(response.status, "SUCCEEDED");
            assert_eq!(
                response
                    .properties
                    .as_ref()
                    .and_then(|props| props.get("operation")),
                Some(&"CreateIndex".to_string())
            );
            assert_eq!(
                response
                    .properties
                    .as_ref()
                    .and_then(|props| props.get("uuid")),
                Some(&transaction_id)
            );
        } else {
            assert!(latest_transaction.is_none());
        }
    }

    #[tokio::test]
    async fn test_drop_table_index() {
        use lance_namespace::models::{DropTableIndexRequest, ListTableIndicesRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let create_transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;

        let drop_transaction_id = namespace
            .drop_table_index(DropTableIndexRequest {
                id: Some(vec!["users".to_string()]),
                index_name: Some("users_id_idx".to_string()),
                ..Default::default()
            })
            .await
            .unwrap()
            .transaction_id;

        let dataset = open_dataset(&namespace, "users").await;
        let previous_dataset = dataset
            .checkout_version(dataset.version().version - 1)
            .await
            .unwrap();
        let previous_transaction_id = previous_dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(create_transaction_id, previous_transaction_id);
        let expected_drop_transaction_id = dataset
            .read_transaction()
            .await
            .unwrap()
            .map(|transaction| transaction.uuid);
        assert_eq!(drop_transaction_id, expected_drop_transaction_id);
        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.iter().any(|index| index.name == "users_id_idx"));

        let list_response = namespace
            .list_table_indices(ListTableIndicesRequest {
                id: Some(vec!["users".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(list_response.indexes.is_empty());
    }

    #[tokio::test]
    async fn test_describe_table() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table first
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Describe the table
        let mut request = DescribeTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);
        let response = namespace.describe_table(request).await.unwrap();

        assert!(response.location.is_some());
        assert!(response.location.unwrap().ends_with("test_table.lance"));
    }

    #[tokio::test]
    async fn test_describe_nonexistent_table() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let mut request = DescribeTableRequest::new();
        request.id = Some(vec!["nonexistent".to_string()]);

        let result = namespace.describe_table(request).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Table not found"));
    }

    #[tokio::test]
    async fn test_table_exists() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["existing_table".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Check existing table
        let mut request = TableExistsRequest::new();
        request.id = Some(vec!["existing_table".to_string()]);
        let result = namespace.table_exists(request).await;
        assert!(result.is_ok());

        // Check non-existent table
        let mut request = TableExistsRequest::new();
        request.id = Some(vec!["nonexistent".to_string()]);
        let result = namespace.table_exists(request).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Table not found"));
    }

    #[tokio::test]
    async fn test_drop_table() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["table_to_drop".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Verify it exists
        let mut exists_request = TableExistsRequest::new();
        exists_request.id = Some(vec!["table_to_drop".to_string()]);
        assert!(namespace.table_exists(exists_request.clone()).await.is_ok());

        // Drop the table
        let mut drop_request = DropTableRequest::new();
        drop_request.id = Some(vec!["table_to_drop".to_string()]);
        let response = namespace.drop_table(drop_request).await.unwrap();
        assert!(response.location.is_some());

        // Verify it no longer exists
        assert!(namespace.table_exists(exists_request).await.is_err());
    }

    #[tokio::test]
    async fn test_drop_nonexistent_table() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let mut request = DropTableRequest::new();
        request.id = Some(vec!["nonexistent".to_string()]);

        // Should not fail when dropping non-existent table (idempotent)
        let result = namespace.drop_table(request).await;
        // The operation might succeed or fail depending on implementation
        // But it should not panic
        let _ = result;
    }

    #[tokio::test]
    async fn test_root_namespace_operations() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Test list_namespaces - should return empty list for root
        let mut request = ListNamespacesRequest::new();
        request.id = Some(vec![]);
        let result = namespace.list_namespaces(request).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().namespaces.len(), 0);

        // Test describe_namespace - should succeed for root
        let mut request = DescribeNamespaceRequest::new();
        request.id = Some(vec![]);
        let result = namespace.describe_namespace(request).await;
        assert!(result.is_ok());

        // Test namespace_exists - root always exists
        let mut request = NamespaceExistsRequest::new();
        request.id = Some(vec![]);
        let result = namespace.namespace_exists(request).await;
        assert!(result.is_ok());

        // Test create_namespace - root cannot be created
        let mut request = CreateNamespaceRequest::new();
        request.id = Some(vec![]);
        let result = namespace.create_namespace(request).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));

        // Test drop_namespace - root cannot be dropped
        let mut request = DropNamespaceRequest::new();
        request.id = Some(vec![]);
        let result = namespace.drop_namespace(request).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot be dropped")
        );
    }

    #[tokio::test]
    async fn test_non_root_namespace_operations() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // With manifest enabled (default), child namespaces are now supported
        // Test create_namespace for non-root - should succeed with manifest
        let mut request = CreateNamespaceRequest::new();
        request.id = Some(vec!["child".to_string()]);
        let result = namespace.create_namespace(request).await;
        assert!(
            result.is_ok(),
            "Child namespace creation should succeed with manifest enabled"
        );

        // Test namespace_exists for non-root - should exist after creation
        let mut request = NamespaceExistsRequest::new();
        request.id = Some(vec!["child".to_string()]);
        let result = namespace.namespace_exists(request).await;
        assert!(
            result.is_ok(),
            "Child namespace should exist after creation"
        );

        // Test drop_namespace for non-root - should succeed
        let mut request = DropNamespaceRequest::new();
        request.id = Some(vec!["child".to_string()]);
        let result = namespace.drop_namespace(request).await;
        assert!(
            result.is_ok(),
            "Child namespace drop should succeed with manifest enabled"
        );

        // Verify namespace no longer exists
        let mut request = NamespaceExistsRequest::new();
        request.id = Some(vec!["child".to_string()]);
        let result = namespace.namespace_exists(request).await;
        assert!(
            result.is_err(),
            "Child namespace should not exist after drop"
        );
    }

    #[tokio::test]
    async fn test_config_custom_root() {
        let temp_dir = TempStdDir::default();
        let custom_path = temp_dir.join("custom");
        std::fs::create_dir(&custom_path).unwrap();

        let namespace = DirectoryNamespaceBuilder::new(custom_path.to_string_lossy().to_string())
            .build()
            .await
            .unwrap();

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Create a table and verify location
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        assert!(response.location.unwrap().contains("custom"));
    }

    #[tokio::test]
    async fn test_config_storage_options() {
        let temp_dir = TempStdDir::default();

        let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
            .storage_option("option1", "value1")
            .storage_option("option2", "value2")
            .build()
            .await
            .unwrap();

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Create a table and check storage options are included
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        let storage_options = response.storage_options.unwrap();
        assert_eq!(storage_options.get("option1"), Some(&"value1".to_string()));
        assert_eq!(storage_options.get("option2"), Some(&"value2".to_string()));
    }

    /// When no credential vendor is configured, `describe_table` and
    /// `declare_table` must strip credential keys from storage options
    /// while preserving non-credential config (region, endpoint, etc.).
    #[tokio::test]
    async fn test_no_storage_options_without_vendor() {
        use lance_namespace::models::DeclareTableRequest;

        let temp_dir = TempStdDir::default();

        // No manifest, no credential vendor, but storage options with credentials
        let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
            .manifest_enabled(false)
            .storage_option("aws_access_key_id", "AKID")
            .storage_option("aws_secret_access_key", "SECRET")
            .storage_option("region", "us-east-1")
            .build()
            .await
            .unwrap();

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // create_table
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["t1".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // describe_table should not return storage options without a vendor
        let mut desc_req = DescribeTableRequest::new();
        desc_req.id = Some(vec!["t1".to_string()]);
        let resp = namespace.describe_table(desc_req).await.unwrap();
        assert!(resp.storage_options.is_none());

        // declare_table should not return storage options without a vendor
        let mut decl_req = DeclareTableRequest::new();
        decl_req.id = Some(vec!["t2".to_string()]);
        let resp = namespace.declare_table(decl_req).await.unwrap();
        assert!(resp.storage_options.is_none());
    }

    /// Same test with manifest mode enabled.
    #[tokio::test]
    async fn test_no_storage_options_without_vendor_manifest() {
        let temp_dir = TempStdDir::default();

        let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
            .storage_option("aws_access_key_id", "AKID")
            .storage_option("aws_secret_access_key", "SECRET")
            .storage_option("region", "us-east-1")
            .build()
            .await
            .unwrap();

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["t1".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // describe_table through manifest should not return storage options without a vendor
        let mut desc_req = DescribeTableRequest::new();
        desc_req.id = Some(vec!["t1".to_string()]);
        let resp = namespace.describe_table(desc_req).await.unwrap();
        assert!(resp.storage_options.is_none());
    }

    #[tokio::test]
    async fn test_from_properties_manifest_enabled() {
        let temp_dir = TempStdDir::default();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), temp_dir.to_str().unwrap().to_string());
        properties.insert("manifest_enabled".to_string(), "true".to_string());
        properties.insert("dir_listing_enabled".to_string(), "false".to_string());

        let builder = DirectoryNamespaceBuilder::from_properties(properties, None).unwrap();
        assert!(builder.manifest_enabled);
        assert!(!builder.dir_listing_enabled);

        let namespace = builder.build().await.unwrap();

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Create a table
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        assert!(response.location.is_some());
    }

    #[tokio::test]
    async fn test_from_properties_dir_listing_enabled() {
        let temp_dir = TempStdDir::default();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), temp_dir.to_str().unwrap().to_string());
        properties.insert("manifest_enabled".to_string(), "false".to_string());
        properties.insert("dir_listing_enabled".to_string(), "true".to_string());

        let builder = DirectoryNamespaceBuilder::from_properties(properties, None).unwrap();
        assert!(!builder.manifest_enabled);
        assert!(builder.dir_listing_enabled);

        let namespace = builder.build().await.unwrap();

        // Create test IPC data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        // Create a table
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        assert!(response.location.is_some());
    }

    #[tokio::test]
    async fn test_from_properties_defaults() {
        let temp_dir = TempStdDir::default();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), temp_dir.to_str().unwrap().to_string());

        let builder = DirectoryNamespaceBuilder::from_properties(properties, None).unwrap();
        // Both should default to true
        assert!(builder.manifest_enabled);
        assert!(builder.dir_listing_enabled);
    }

    #[tokio::test]
    async fn test_from_properties_with_storage_options() {
        let temp_dir = TempStdDir::default();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), temp_dir.to_str().unwrap().to_string());
        properties.insert("manifest_enabled".to_string(), "true".to_string());
        properties.insert("storage.region".to_string(), "us-west-2".to_string());
        properties.insert("storage.bucket".to_string(), "my-bucket".to_string());

        let builder = DirectoryNamespaceBuilder::from_properties(properties, None).unwrap();
        assert!(builder.manifest_enabled);
        assert!(builder.storage_options.is_some());

        let storage_options = builder.storage_options.unwrap();
        assert_eq!(
            storage_options.get("region"),
            Some(&"us-west-2".to_string())
        );
        assert_eq!(
            storage_options.get("bucket"),
            Some(&"my-bucket".to_string())
        );
    }

    #[tokio::test]
    async fn test_various_arrow_types() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create schema with various types
        let fields = vec![
            JsonArrowField {
                name: "bool_col".to_string(),
                r#type: Box::new(JsonArrowDataType::new("bool".to_string())),
                nullable: true,
                metadata: None,
            },
            JsonArrowField {
                name: "int8_col".to_string(),
                r#type: Box::new(JsonArrowDataType::new("int8".to_string())),
                nullable: true,
                metadata: None,
            },
            JsonArrowField {
                name: "float64_col".to_string(),
                r#type: Box::new(JsonArrowDataType::new("float64".to_string())),
                nullable: true,
                metadata: None,
            },
            JsonArrowField {
                name: "binary_col".to_string(),
                r#type: Box::new(JsonArrowDataType::new("binary".to_string())),
                nullable: true,
                metadata: None,
            },
        ];

        let schema = JsonArrowSchema {
            fields,
            metadata: None,
        };

        // Create IPC data
        let ipc_data = create_test_ipc_data(&schema);

        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["complex_table".to_string()]);

        let response = namespace
            .create_table(request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        assert!(response.location.is_some());
    }

    #[tokio::test]
    async fn test_connect_dir() {
        let temp_dir = TempStdDir::default();

        let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
            .build()
            .await
            .unwrap();

        // Test basic operation through the concrete type
        let mut request = ListTablesRequest::new();
        request.id = Some(vec![]);
        let response = namespace.list_tables(request).await.unwrap();
        assert_eq!(response.tables.len(), 0);
    }

    #[tokio::test]
    async fn test_create_table_with_ipc_data() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::ipc::writer::StreamWriter;

        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a schema with some fields
        let schema = create_test_schema();

        // Create some test data that matches the schema
        let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
        let arrow_schema = Arc::new(arrow_schema);

        // Create a RecordBatch with actual data
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        // Write the batch to an IPC stream
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Create table with the IPC data
        let mut request = CreateTableRequest::new();
        request.id = Some(vec!["test_table_with_data".to_string()]);

        let response = namespace
            .create_table(request, Bytes::from(buffer))
            .await
            .unwrap();

        assert_eq!(response.version, Some(1));
        assert!(
            response
                .location
                .unwrap()
                .contains("test_table_with_data.lance")
        );

        // Verify table exists
        let mut exists_request = TableExistsRequest::new();
        exists_request.id = Some(vec!["test_table_with_data".to_string()]);
        namespace.table_exists(exists_request).await.unwrap();
    }

    #[tokio::test]
    async fn test_child_namespace_create_and_list() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create multiple child namespaces
        for i in 1..=3 {
            let mut create_req = CreateNamespaceRequest::new();
            create_req.id = Some(vec![format!("ns{}", i)]);
            let result = namespace.create_namespace(create_req).await;
            assert!(result.is_ok(), "Failed to create child namespace ns{}", i);
        }

        // List child namespaces
        let list_req = ListNamespacesRequest {
            id: Some(vec![]),
            ..Default::default()
        };
        let result = namespace.list_namespaces(list_req).await;
        assert!(result.is_ok());
        let namespaces = result.unwrap().namespaces;
        assert_eq!(namespaces.len(), 3);
        assert!(namespaces.contains(&"ns1".to_string()));
        assert!(namespaces.contains(&"ns2".to_string()));
        assert!(namespaces.contains(&"ns3".to_string()));
    }

    #[tokio::test]
    async fn test_nested_namespace_hierarchy() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create parent namespace
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        // Create nested children
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string(), "child1".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["parent".to_string(), "child2".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        // List children of parent
        let list_req = ListNamespacesRequest {
            id: Some(vec!["parent".to_string()]),
            ..Default::default()
        };
        let result = namespace.list_namespaces(list_req).await;
        assert!(result.is_ok());
        let children = result.unwrap().namespaces;
        assert_eq!(children.len(), 2);
        assert!(children.contains(&"child1".to_string()));
        assert!(children.contains(&"child2".to_string()));

        // List root should only show parent
        let list_req = ListNamespacesRequest {
            id: Some(vec![]),
            ..Default::default()
        };
        let result = namespace.list_namespaces(list_req).await;
        assert!(result.is_ok());
        let root_namespaces = result.unwrap().namespaces;
        assert_eq!(root_namespaces.len(), 1);
        assert_eq!(root_namespaces[0], "parent");
    }

    #[tokio::test]
    async fn test_table_in_child_namespace() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create table in child namespace
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let result = namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await;
        assert!(result.is_ok(), "Failed to create table in child namespace");

        // List tables in child namespace
        let list_req = ListTablesRequest {
            id: Some(vec!["test_ns".to_string()]),
            ..Default::default()
        };
        let result = namespace.list_tables(list_req).await;
        assert!(result.is_ok());
        let tables = result.unwrap().tables;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "table1");

        // Verify table exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let result = namespace.table_exists(exists_req).await;
        assert!(result.is_ok());

        // Describe table in child namespace
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let result = namespace.describe_table(describe_req).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.location.is_some());
    }

    /// Regression: a connection built before `__manifest` exists must still
    /// resolve a child-namespaced table that a *different* connection registers
    /// afterwards. Phalanx caches one DirectoryNamespace per db and the first op
    /// on a fresh db is usually a read, so without a self-healing read path the
    /// cached reader pins an empty manifest cell and every describe/exists/list
    /// on the table reports "not found" forever -- even though `create_table`
    /// reports it already exists. This is the geneva `__system$geneva_jobs`
    /// open->create->open livelock.
    #[tokio::test]
    async fn test_read_self_heals_after_manifest_created_by_other_connection() {
        let temp_dir = TempStdDir::default();
        let root = temp_dir.to_str().unwrap();

        // Reader is built while no `__manifest` exists yet -> its read cell is
        // empty and, before the fix, stays empty forever.
        let reader = DirectoryNamespaceBuilder::new(root).build().await.unwrap();

        // A *separate* connection creates the child namespace + table, which
        // lazily creates `__manifest` and registers the entry.
        let writer = DirectoryNamespaceBuilder::new(root).build().await.unwrap();
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        writer.create_namespace(create_ns_req).await.unwrap();
        let ipc_data = create_test_ipc_data(&create_test_schema());
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        writer
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // The reader, though built before the manifest existed, must now resolve
        // the table on every read path (was TableNotFound before the fix).
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let resp = reader
            .describe_table(describe_req)
            .await
            .expect("describe_table must resolve a table registered after build");
        assert!(resp.location.is_some());

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        reader
            .table_exists(exists_req)
            .await
            .expect("table_exists must resolve a table registered after build");

        let list_req = ListTablesRequest {
            id: Some(vec!["test_ns".to_string()]),
            ..Default::default()
        };
        let tables = reader.list_tables(list_req).await.unwrap().tables;
        assert_eq!(tables, vec!["table1".to_string()]);
    }

    /// Migration mode promises manifest-first lookup even at the root, so a
    /// reader built before `__manifest` existed must still resolve a
    /// manifest-only alias (`registered_table` -> `external_table.lance`) that
    /// dir-listing cannot produce. Before the read path self-healed in migration
    /// mode, the root gate bypassed the manifest probe and fell back to
    /// dir-listing, which sees `external_table` but never `registered_table` ->
    /// permanent TableNotFound for every registration made after build.
    #[tokio::test]
    async fn test_migration_root_read_self_heals_registered_alias() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Reader built while the root is empty -> no `__manifest`, read cell
        // empty (and, before the fix, frozen empty forever).
        let reader = DirectoryNamespaceBuilder::new(temp_path)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();

        // A separate connection writes an external dataset and registers it in
        // the manifest under a *different* logical name -- an alias dir-listing
        // cannot resolve. This is what lazily creates `__manifest`.
        let writer = DirectoryNamespaceBuilder::new(temp_path)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();
        let ipc_data = create_test_ipc_data(&create_test_schema());
        let table_uri = format!("{}/external_table.lance", temp_path);
        let cursor = Cursor::new(ipc_data);
        let stream_reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = stream_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let schema = batches[0].schema();
        let batch_results: Vec<_> = batches.into_iter().map(Ok).collect();
        let batch_reader = RecordBatchIterator::new(batch_results, schema);
        Dataset::write(Box::new(batch_reader), &table_uri, None)
            .await
            .unwrap();
        let mut register_req = RegisterTableRequest::new("external_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);
        writer.register_table(register_req).await.unwrap();

        // The reader, built before `__manifest` existed, must now resolve the
        // manifest-only alias on every read path.
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["registered_table".to_string()]);
        reader
            .describe_table(describe_req)
            .await
            .expect("describe_table must resolve a manifest alias registered after build");

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["registered_table".to_string()]);
        reader
            .table_exists(exists_req)
            .await
            .expect("table_exists must resolve a manifest alias registered after build");

        let list_req = ListTablesRequest {
            id: Some(vec![]),
            ..Default::default()
        };
        let tables = reader.list_tables(list_req).await.unwrap().tables;
        assert!(
            tables.contains(&"registered_table".to_string()),
            "list_tables must include the manifest alias registered after build, got {:?}",
            tables
        );
    }

    #[tokio::test]
    async fn test_multiple_tables_in_child_namespace() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create multiple tables
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        for i in 1..=3 {
            let mut create_table_req = CreateTableRequest::new();
            create_table_req.id = Some(vec!["test_ns".to_string(), format!("table{}", i)]);
            namespace
                .create_table(create_table_req, bytes::Bytes::from(ipc_data.clone()))
                .await
                .unwrap();
        }

        // List tables
        let list_req = ListTablesRequest {
            id: Some(vec!["test_ns".to_string()]),
            ..Default::default()
        };
        let result = namespace.list_tables(list_req).await;
        assert!(result.is_ok());
        let tables = result.unwrap().tables;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
        assert!(tables.contains(&"table3".to_string()));
    }

    #[tokio::test]
    async fn test_drop_table_in_child_namespace() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Drop table
        let mut drop_req = DropTableRequest::new();
        drop_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let result = namespace.drop_table(drop_req).await;
        assert!(result.is_ok(), "Failed to drop table in child namespace");

        // Verify table no longer exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        let result = namespace.table_exists(exists_req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_deeply_nested_namespace() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create deeply nested namespace hierarchy
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["level1".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["level1".to_string(), "level2".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ]);
        namespace.create_namespace(create_req).await.unwrap();

        // Create table in deeply nested namespace
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
            "table1".to_string(),
        ]);
        let result = namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await;
        assert!(
            result.is_ok(),
            "Failed to create table in deeply nested namespace"
        );

        // Verify table exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
            "table1".to_string(),
        ]);
        let result = namespace.table_exists(exists_req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_with_properties() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create namespace with properties
        let mut properties = HashMap::new();
        properties.insert("owner".to_string(), "test_user".to_string());
        properties.insert("description".to_string(), "Test namespace".to_string());

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["test_ns".to_string()]);
        create_req.properties = Some(properties.clone());
        namespace.create_namespace(create_req).await.unwrap();

        // Describe namespace and verify properties
        let describe_req = DescribeNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            ..Default::default()
        };
        let result = namespace.describe_namespace(describe_req).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.properties.is_some());
        let props = response.properties.unwrap();
        assert_eq!(props.get("owner"), Some(&"test_user".to_string()));
        assert_eq!(
            props.get("description"),
            Some(&"Test namespace".to_string())
        );
    }

    #[tokio::test]
    async fn test_cannot_drop_namespace_with_tables() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create table in namespace
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["test_ns".to_string(), "table1".to_string()]);
        namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Try to drop namespace - should fail
        let mut drop_req = DropNamespaceRequest::new();
        drop_req.id = Some(vec!["test_ns".to_string()]);
        let result = namespace.drop_namespace(drop_req).await;
        assert!(
            result.is_err(),
            "Should not be able to drop namespace with tables"
        );
    }

    #[tokio::test]
    async fn test_isolation_between_namespaces() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create two namespaces
        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns1".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        let mut create_req = CreateNamespaceRequest::new();
        create_req.id = Some(vec!["ns2".to_string()]);
        namespace.create_namespace(create_req).await.unwrap();

        // Create table with same name in both namespaces
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["ns1".to_string(), "table1".to_string()]);
        namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data.clone()))
            .await
            .unwrap();

        let mut create_table_req = CreateTableRequest::new();
        create_table_req.id = Some(vec!["ns2".to_string(), "table1".to_string()]);
        namespace
            .create_table(create_table_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // List tables in each namespace
        let list_req = ListTablesRequest {
            id: Some(vec!["ns1".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let result = namespace.list_tables(list_req).await.unwrap();
        assert_eq!(result.tables.len(), 1);
        assert_eq!(result.tables[0], "table1");

        let list_req = ListTablesRequest {
            id: Some(vec!["ns2".to_string()]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let result = namespace.list_tables(list_req).await.unwrap();
        assert_eq!(result.tables.len(), 1);
        assert_eq!(result.tables[0], "table1");

        // Drop table in ns1 shouldn't affect ns2
        let mut drop_req = DropTableRequest::new();
        drop_req.id = Some(vec!["ns1".to_string(), "table1".to_string()]);
        namespace.drop_table(drop_req).await.unwrap();

        // Verify ns1 table is gone but ns2 table still exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["ns1".to_string(), "table1".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_err());

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["ns2".to_string(), "table1".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_ok());
    }

    #[tokio::test]
    async fn test_migrate_directory_tables() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Step 1: Create tables in directory-only mode
        let dir_only_ns = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create some tables
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        for i in 1..=3 {
            let mut create_req = CreateTableRequest::new();
            create_req.id = Some(vec![format!("table{}", i)]);
            dir_only_ns
                .create_table(create_req, bytes::Bytes::from(ipc_data.clone()))
                .await
                .unwrap();
        }

        drop(dir_only_ns);

        // Step 2: Create namespace with dual mode (manifest + directory listing)
        let dual_mode_ns = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Before migration, tables should be visible (via directory listing fallback)
        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let tables = dual_mode_ns.list_tables(list_req).await.unwrap().tables;
        assert_eq!(tables.len(), 3);

        // Run migration
        let migrated_count = dual_mode_ns.migrate().await.unwrap();
        assert_eq!(migrated_count, 3, "Should migrate all 3 tables");

        // Verify tables are now in manifest
        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let tables = dual_mode_ns.list_tables(list_req).await.unwrap().tables;
        assert_eq!(tables.len(), 3);

        // Run migration again - should be idempotent
        let migrated_count = dual_mode_ns.migrate().await.unwrap();
        assert_eq!(
            migrated_count, 0,
            "Should not migrate already-migrated tables"
        );

        drop(dual_mode_ns);

        // Step 3: Create namespace with manifest-only mode
        let manifest_only_ns = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        // Tables should still be accessible (now from manifest only)
        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let tables = manifest_only_ns.list_tables(list_req).await.unwrap().tables;
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));
        assert!(tables.contains(&"table3".to_string()));
    }

    #[tokio::test]
    async fn test_migrate_without_manifest() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace without manifest
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // migrate() should return 0 when manifest is not enabled
        let migrated_count = namespace.migrate().await.unwrap();
        assert_eq!(migrated_count, 0);
    }

    #[tokio::test]
    async fn test_register_table() {
        use lance_namespace::models::{RegisterTableRequest, TableExistsRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a physical table first using lance directly
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let table_uri = format!("{}/external_table.lance", temp_path);
        let cursor = Cursor::new(ipc_data);
        let stream_reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = stream_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let schema = batches[0].schema();
        let batch_results: Vec<_> = batches.into_iter().map(Ok).collect();
        let reader = RecordBatchIterator::new(batch_results, schema);
        Dataset::write(Box::new(reader), &table_uri, None)
            .await
            .unwrap();

        // Register the table
        let mut register_req = RegisterTableRequest::new("external_table.lance".to_string());
        register_req.id = Some(vec!["registered_table".to_string()]);

        let response = namespace.register_table(register_req).await.unwrap();
        assert_eq!(response.location, Some("external_table.lance".to_string()));

        // Verify table exists in namespace
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["registered_table".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_ok());

        // Verify we can list the table
        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let tables = namespace.list_tables(list_req).await.unwrap();
        assert!(tables.tables.contains(&"registered_table".to_string()));
    }

    #[tokio::test]
    async fn test_register_table_duplicate_fails() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .build()
            .await
            .unwrap();

        // Register a table
        let mut register_req = RegisterTableRequest::new("test_table.lance".to_string());
        register_req.id = Some(vec!["test_table".to_string()]);

        namespace
            .register_table(register_req.clone())
            .await
            .unwrap();

        // Try to register again - should fail
        let result = namespace.register_table(register_req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_deregister_table() {
        use lance_namespace::models::{DeregisterTableRequest, TableExistsRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace with manifest-only mode (no directory listing fallback)
        // This ensures deregistered tables are truly invisible
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Verify table exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.table_exists(exists_req.clone()).await.is_ok());

        // Deregister the table
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_table".to_string()]);
        let response = namespace.deregister_table(deregister_req).await.unwrap();

        // Should return location and id
        assert!(
            response.location.is_some(),
            "Deregister should return location"
        );
        let location = response.location.as_ref().unwrap();
        // Location should be a proper file:// URI with the temp path
        // Use uri_to_url to normalize the temp path to a URL for comparison
        let expected_url = lance_io::object_store::uri_to_url(temp_path)
            .expect("Failed to convert temp path to URL");
        let expected_prefix = expected_url.to_string();
        assert!(
            location.starts_with(&expected_prefix),
            "Location should start with '{}', got: {}",
            expected_prefix,
            location
        );
        assert!(
            location.contains("test_table"),
            "Location should contain table name: {}",
            location
        );
        assert_eq!(response.id, Some(vec!["test_table".to_string()]));

        // Verify table no longer exists in namespace (removed from manifest)
        assert!(namespace.table_exists(exists_req).await.is_err());

        // Verify physical data still exists at the returned location
        let dataset = Dataset::open(location).await;
        assert!(
            dataset.is_ok(),
            "Physical table data should still exist at {}",
            location
        );
    }

    #[tokio::test]
    async fn test_deregister_table_in_child_namespace() {
        use lance_namespace::models::{
            CreateNamespaceRequest, DeregisterTableRequest, TableExistsRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .build()
            .await
            .unwrap();

        // Create child namespace
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["test_ns".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create a table in the child namespace
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_ns".to_string(), "test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Deregister the table
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_ns".to_string(), "test_table".to_string()]);
        let response = namespace.deregister_table(deregister_req).await.unwrap();

        // Should return location and id in child namespace
        assert!(
            response.location.is_some(),
            "Deregister should return location"
        );
        let location = response.location.as_ref().unwrap();
        // Location should be a proper file:// URI with the temp path
        // Use uri_to_url to normalize the temp path to a URL for comparison
        let expected_url = lance_io::object_store::uri_to_url(temp_path)
            .expect("Failed to convert temp path to URL");
        let expected_prefix = expected_url.to_string();
        assert!(
            location.starts_with(&expected_prefix),
            "Location should start with '{}', got: {}",
            expected_prefix,
            location
        );
        assert!(
            location.contains("test_ns") && location.contains("test_table"),
            "Location should contain namespace and table name: {}",
            location
        );
        assert_eq!(
            response.id,
            Some(vec!["test_ns".to_string(), "test_table".to_string()])
        );

        // Verify table no longer exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_ns".to_string(), "test_table".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_err());
    }

    #[tokio::test]
    async fn test_register_without_manifest_fails() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace without manifest
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        // Try to register - should fail (register requires manifest)
        let mut register_req = RegisterTableRequest::new("test_table.lance".to_string());
        register_req.id = Some(vec!["test_table".to_string()]);
        let result = namespace.register_table(register_req).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("manifest mode is enabled")
        );

        // Note: deregister_table now works in V1 mode via .lance-deregistered marker files
        // See test_deregister_table_v1_mode for that test case
    }

    #[tokio::test]
    async fn test_register_table_rejects_absolute_uri() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .build()
            .await
            .unwrap();

        // Try to register with absolute URI - should fail
        let mut register_req = RegisterTableRequest::new("s3://bucket/table.lance".to_string());
        register_req.id = Some(vec!["test_table".to_string()]);
        let result = namespace.register_table(register_req).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Absolute URIs are not allowed"));
    }

    #[tokio::test]
    async fn test_register_table_rejects_absolute_path() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .build()
            .await
            .unwrap();

        // Try to register with absolute path - should fail
        let mut register_req = RegisterTableRequest::new("/tmp/table.lance".to_string());
        register_req.id = Some(vec!["test_table".to_string()]);
        let result = namespace.register_table(register_req).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Absolute paths are not allowed"));
    }

    #[tokio::test]
    async fn test_register_table_rejects_path_traversal() {
        use lance_namespace::models::RegisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .build()
            .await
            .unwrap();

        // Try to register with path traversal - should fail
        let mut register_req = RegisterTableRequest::new("../outside/table.lance".to_string());
        register_req.id = Some(vec!["test_table".to_string()]);
        let result = namespace.register_table(register_req).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Path traversal is not allowed"));
    }

    #[tokio::test]
    async fn test_namespace_write() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use arrow::record_batch::{RecordBatch, RecordBatchIterator};
        use lance::dataset::{Dataset, WriteMode, WriteParams};
        use lance_namespace::LanceNamespace;

        let (namespace, _temp_dir) = create_test_namespace().await;
        let namespace = Arc::new(namespace) as Arc<dyn LanceNamespace>;

        // Use child namespace instead of root
        let table_id = vec!["test_ns".to_string(), "test_table".to_string()];
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, false),
            ArrowField::new("b", DataType::Int32, false),
        ]));

        // Test 1: CREATE mode
        let data1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let reader1 = RecordBatchIterator::new(vec![data1].into_iter().map(Ok), schema.clone());
        let dataset =
            Dataset::write_into_namespace(reader1, namespace.clone(), table_id.clone(), None)
                .await
                .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
        assert_eq!(dataset.version().version, 1);

        // Test 2: APPEND mode
        let data2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(Int32Array::from(vec![40, 50])),
            ],
        )
        .unwrap();

        let params_append = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };

        let reader2 = RecordBatchIterator::new(vec![data2].into_iter().map(Ok), schema.clone());
        let dataset = Dataset::write_into_namespace(
            reader2,
            namespace.clone(),
            table_id.clone(),
            Some(params_append),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
        assert_eq!(dataset.version().version, 2);

        // Test 3: OVERWRITE mode
        let data3 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100, 200])),
                Arc::new(Int32Array::from(vec![1000, 2000])),
            ],
        )
        .unwrap();

        let params_overwrite = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        let reader3 = RecordBatchIterator::new(vec![data3].into_iter().map(Ok), schema.clone());
        let dataset = Dataset::write_into_namespace(
            reader3,
            namespace.clone(),
            table_id.clone(),
            Some(params_overwrite),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 2);
        assert_eq!(dataset.version().version, 3);

        // Verify old data was replaced
        let result = dataset.scan().try_into_batch().await.unwrap();
        let a_col = result
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a_col.values(), &[100, 200]);
    }

    // ============================================================
    // Tests for declare_table
    // ============================================================

    #[tokio::test]
    async fn test_declare_table_v1_mode() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, ListTablesRequest, TableExistsRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace in V1 mode (no manifest)
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        // Declare a table
        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        let response = namespace.declare_table(declare_req).await.unwrap();

        // Should return location
        assert!(response.location.is_some());
        let location = response.location.as_ref().unwrap();
        assert!(location.ends_with("test_table.lance"));

        // Table should exist (via reserved file)
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_ok());

        // Describe should work but return no version/schema (not written yet)
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert!(describe_response.location.is_some());
        assert!(describe_response.version.is_none()); // Not written yet
        assert!(describe_response.schema.is_none()); // Not written yet
        assert_eq!(describe_response.is_only_declared, None);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.check_declared = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(true));

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let list_response = namespace.list_tables(list_req.clone()).await.unwrap();
        assert_eq!(list_response.tables, vec!["test_table".to_string()]);

        list_req.include_declared = Some(false);
        let list_response = namespace.list_tables(list_req).await.unwrap();
        assert!(list_response.tables.is_empty());
    }

    #[tokio::test]
    async fn test_insert_into_declared_table_promotes_it_from_declared_state() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, InsertIntoTableRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut insert_req = InsertIntoTableRequest::new();
        insert_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .insert_into_table(insert_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();

        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));
        assert!(describe_response.schema.is_some());

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_create_table_after_declare_table_v1_mode_creates_table() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, ListTablesRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        let response = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(response.version, Some(1));

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_insert_into_declared_table_with_manifest_promotes_it() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, InsertIntoTableRequest, ListTablesRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        let mut insert_req = InsertIntoTableRequest::new();
        insert_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .insert_into_table(
                insert_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_create_table_after_declare_table_with_manifest_creates_table() {
        use lance_namespace::models::{
            CreateTableRequest, DeclareTableRequest, DescribeTableRequest, ListTablesRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(HashMap::from([("owner".to_string(), "alice".to_string())]));
        namespace.declare_table(declare_req).await.unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.mode = Some("Overwrite".to_string());
        let response = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(response.version, Some(1));
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));
        assert_eq!(
            describe_response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_create_table_after_declare_table_with_manifest_rejects_new_properties() {
        use lance_namespace::models::{CreateTableRequest, DeclareTableRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(HashMap::from([("owner".to_string(), "alice".to_string())]));
        namespace.declare_table(declare_req).await.unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(HashMap::from([("owner".to_string(), "bob".to_string())]));

        let result = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("cannot set properties for already declared table")
        );
    }

    #[tokio::test]
    async fn test_create_table_with_manifest_exist_ok_keeps_existing_table() {
        use lance_namespace::models::{CreateTableRequest, DescribeTableRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(HashMap::from([("owner".to_string(), "alice".to_string())]));
        namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.mode = Some("ExistOk".to_string());
        create_req.properties = Some(HashMap::from([("owner".to_string(), "bob".to_string())]));
        let response = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_single_row_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );
        assert_eq!(
            open_dataset(&namespace, "test_table")
                .await
                .count_rows(None)
                .await
                .unwrap(),
            2
        );

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(
            describe_response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );
    }

    #[tokio::test]
    async fn test_create_table_with_manifest_overwrite_replaces_existing_table() {
        use lance_namespace::models::{CreateTableRequest, DescribeTableRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.properties = Some(HashMap::from([("owner".to_string(), "alice".to_string())]));
        namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.mode = Some("overwrite".to_string());
        create_req.properties = Some(HashMap::from([("owner".to_string(), "bob".to_string())]));
        let response = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_single_row_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(response.version, Some(2));
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"bob".to_string())
        );
        assert_eq!(
            open_dataset(&namespace, "test_table")
                .await
                .count_rows(None)
                .await
                .unwrap(),
            1
        );

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(
            describe_response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"bob".to_string())
        );
    }

    #[tokio::test]
    async fn test_create_table_with_manifest_invalid_mode_rejected() {
        use lance_namespace::models::CreateTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        create_req.mode = Some("append".to_string());
        let result = namespace
            .create_table(
                create_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported create_table mode")
        );
    }

    #[tokio::test]
    async fn test_merge_insert_into_declared_table_v1_mode_creates_table() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, ListTablesRequest,
            MergeInsertIntoTableRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        let mut merge_req = MergeInsertIntoTableRequest::new();
        merge_req.id = Some(vec!["test_table".to_string()]);
        merge_req.on = Some("id".to_string());
        let response = namespace
            .merge_insert_into_table(
                merge_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(response.num_inserted_rows, Some(2));
        assert_eq!(response.num_updated_rows, Some(0));

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_merge_insert_into_declared_table_with_manifest_creates_table() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, ListTablesRequest,
            MergeInsertIntoTableRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        let mut merge_req = MergeInsertIntoTableRequest::new();
        merge_req.id = Some(vec!["test_table".to_string()]);
        merge_req.on = Some("id".to_string());
        let response = namespace
            .merge_insert_into_table(
                merge_req,
                bytes::Bytes::from(create_non_empty_test_ipc_data()),
            )
            .await
            .unwrap();

        assert_eq!(response.num_inserted_rows, Some(2));
        assert_eq!(response.num_updated_rows, Some(0));

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(false));
        assert_eq!(describe_response.version, Some(1));

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        list_req.include_declared = Some(false);
        assert_eq!(
            namespace.list_tables(list_req).await.unwrap().tables,
            vec!["test_table".to_string()]
        );
    }

    #[tokio::test]
    async fn test_declare_table_with_manifest() {
        use lance_namespace::models::{
            DeclareTableRequest, DescribeTableRequest, ListTablesRequest, TableExistsRequest,
        };

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace with manifest
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();

        // Declare a table
        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        declare_req.properties = Some(HashMap::from([("owner".to_string(), "alice".to_string())]));
        let response = namespace.declare_table(declare_req).await.unwrap();

        // Should return location
        assert!(response.location.is_some());
        assert_eq!(
            response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );

        // Table should exist in manifest
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.table_exists(exists_req).await.is_ok());

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, None);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.check_declared = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();
        assert_eq!(describe_response.is_only_declared, Some(true));
        assert_eq!(
            describe_response
                .properties
                .as_ref()
                .and_then(|properties| properties.get("owner")),
            Some(&"alice".to_string())
        );

        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        assert_eq!(
            namespace
                .list_tables(list_req.clone())
                .await
                .unwrap()
                .tables,
            vec!["test_table".to_string()]
        );
        list_req.include_declared = Some(false);
        assert!(
            namespace
                .list_tables(list_req)
                .await
                .unwrap()
                .tables
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_declare_table_with_manifest_marker_already_exists() {
        // Pre-existing .lance-reserved (concurrent/incomplete declare) must map to
        // TableAlreadyExists, not Internal.
        use lance_namespace::error::ErrorCode;
        use lance_namespace::models::DeclareTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        let table_dir = temp_dir.join("test_table.lance");
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join(".lance-reserved"), b"reserved").unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        let err = namespace
            .declare_table(declare_req)
            .await
            .expect_err("declare with existing marker must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("already exists") || msg.contains("TableAlreadyExists"),
            "expected TableAlreadyExists, got: {msg}"
        );
        assert_eq!(
            mutation_error_code(err),
            ErrorCode::TableAlreadyExists,
            "expected TableAlreadyExists error code"
        );
    }

    #[tokio::test]
    async fn test_declare_table_when_table_exists() {
        use lance_namespace::models::DeclareTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .build()
            .await
            .unwrap();

        // First create a table with actual data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Try to declare the same table - should fail because it already has data
        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        let result = namespace.declare_table(declare_req).await;
        assert!(result.is_err());
    }

    // ============================================================
    // Tests for deregister_table in V1 mode
    // ============================================================

    #[tokio::test]
    async fn test_deregister_table_v1_mode() {
        use lance_namespace::models::{DeregisterTableRequest, TableExistsRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace in V1 mode (no manifest, with dir listing)
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table with data
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Verify table exists
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.table_exists(exists_req.clone()).await.is_ok());

        // Deregister the table
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_table".to_string()]);
        let response = namespace.deregister_table(deregister_req).await.unwrap();

        // Should return location
        assert!(response.location.is_some());
        let location = response.location.as_ref().unwrap();
        assert!(location.contains("test_table"));

        // Table should no longer exist (deregistered)
        let result = namespace.table_exists(exists_req).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("deregistered"));

        // Physical data should still exist
        let dataset = Dataset::open(location).await;
        assert!(dataset.is_ok(), "Physical table data should still exist");
    }

    #[tokio::test]
    async fn test_deregister_table_v1_already_deregistered() {
        use lance_namespace::models::DeregisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Deregister once
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .deregister_table(deregister_req.clone())
            .await
            .unwrap();

        // Try to deregister again - should fail
        let result = namespace.deregister_table(deregister_req).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already deregistered")
        );
    }

    // ============================================================
    // Tests for list_tables skipping deregistered tables
    // ============================================================

    #[tokio::test]
    async fn test_list_tables_skips_deregistered_v1() {
        use lance_namespace::models::DeregisterTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create two tables
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);

        let mut create_req1 = CreateTableRequest::new();
        create_req1.id = Some(vec!["table1".to_string()]);
        namespace
            .create_table(create_req1, bytes::Bytes::from(ipc_data.clone()))
            .await
            .unwrap();

        let mut create_req2 = CreateTableRequest::new();
        create_req2.id = Some(vec!["table2".to_string()]);
        namespace
            .create_table(create_req2, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // List tables - should see both (root namespace = empty vec)
        let mut list_req = ListTablesRequest::new();
        list_req.id = Some(vec![]);
        let list_response = namespace.list_tables(list_req.clone()).await.unwrap();
        assert_eq!(list_response.tables.len(), 2);

        // Deregister table1
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["table1".to_string()]);
        namespace.deregister_table(deregister_req).await.unwrap();

        // List tables - should only see table2
        let list_response = namespace.list_tables(list_req).await.unwrap();
        assert_eq!(list_response.tables.len(), 1);
        assert!(list_response.tables.contains(&"table2".to_string()));
        assert!(!list_response.tables.contains(&"table1".to_string()));
    }

    // ============================================================
    // Tests for describe_table and table_exists with deregistered tables
    // ============================================================

    #[tokio::test]
    async fn test_describe_table_fails_for_deregistered_v1() {
        use lance_namespace::models::{DeregisterTableRequest, DescribeTableRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Describe should work before deregistration
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.describe_table(describe_req.clone()).await.is_ok());

        // Deregister
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_table".to_string()]);
        namespace.deregister_table(deregister_req).await.unwrap();

        // Describe should fail after deregistration
        let result = namespace.describe_table(describe_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("deregistered"));
        assert!(err_msg.contains("table id 'test_table'"));
    }

    #[tokio::test]
    async fn test_table_exists_fails_for_deregistered_v1() {
        use lance_namespace::models::{DeregisterTableRequest, TableExistsRequest};

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Table exists should work before deregistration
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        assert!(namespace.table_exists(exists_req.clone()).await.is_ok());

        // Deregister
        let mut deregister_req = DeregisterTableRequest::new();
        deregister_req.id = Some(vec!["test_table".to_string()]);
        namespace.deregister_table(deregister_req).await.unwrap();

        // Table exists should fail after deregistration
        let result = namespace.table_exists(exists_req).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("deregistered"));
        assert!(err_msg.contains("table id 'test_table'"));
    }

    #[tokio::test]
    async fn test_atomic_table_status_check() {
        // This test verifies that the TableStatus check is atomic
        // by ensuring a single directory listing is used

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Table status should show exists=true, is_deregistered=false
        let status = namespace.check_table_status("test_table").await.unwrap();
        assert!(status.exists);
        assert!(!status.is_deregistered);
        assert!(!status.has_reserved_file);
    }

    #[tokio::test]
    async fn test_table_version_tracking_enabled_managed_versioning() {
        use lance_namespace::models::DescribeTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace with table_version_tracking_enabled=true
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .table_version_tracking_enabled(true)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Describe table should return managed_versioning=true
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_resp = namespace.describe_table(describe_req).await.unwrap();

        // managed_versioning should be true
        assert_eq!(
            describe_resp.managed_versioning,
            Some(true),
            "managed_versioning should be true when table_version_tracking_enabled=true"
        );
    }

    #[tokio::test]
    async fn test_table_version_tracking_disabled_no_managed_versioning() {
        use lance_namespace::models::DescribeTableRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        // Create namespace with table_version_tracking_enabled=false (default)
        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .table_version_tracking_enabled(false)
            .build()
            .await
            .unwrap();

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Describe table should not have managed_versioning set
        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_resp = namespace.describe_table(describe_req).await.unwrap();

        // managed_versioning should be None when table_version_tracking_enabled=false
        assert!(
            describe_resp.managed_versioning.is_none(),
            "managed_versioning should be None when table_version_tracking_enabled=false, got: {:?}",
            describe_resp.managed_versioning
        );
    }

    #[tokio::test]
    async fn test_list_table_versions() {
        use arrow::array::{Int32Array, RecordBatchIterator};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance::dataset::{Dataset, WriteMode, WriteParams};
        use lance_namespace::models::{CreateNamespaceRequest, ListTableVersionsRequest};

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        // Create parent namespace first
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["workspace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create a table using write_into_namespace (version 1)
        let table_id = vec!["workspace".to_string(), "test_table".to_string()];
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch.clone())], arrow_schema.clone());
        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };
        let mut dataset = Dataset::write_into_namespace(
            batches,
            namespace.clone(),
            table_id.clone(),
            Some(write_params),
        )
        .await
        .unwrap();

        // Append to create version 2
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch2)], arrow_schema.clone());
        dataset.append(batches, None).await.unwrap();

        // Append to create version 3
        let batch3 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![300, 400]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch3)], arrow_schema);
        dataset.append(batches, None).await.unwrap();

        // List versions - should have versions 1, 2, and 3
        let mut list_req = ListTableVersionsRequest::new();
        list_req.id = Some(table_id.clone());
        let list_resp = namespace.list_table_versions(list_req).await.unwrap();

        assert_eq!(
            list_resp.versions.len(),
            3,
            "Should have 3 versions, got: {:?}",
            list_resp.versions
        );

        // Verify each version
        for expected_version in 1..=3 {
            let version = list_resp
                .versions
                .iter()
                .find(|v| v.version == expected_version)
                .unwrap_or_else(|| panic!("Expected version {}", expected_version));

            assert!(
                !version.manifest_path.is_empty(),
                "manifest_path should be set for version {}",
                expected_version
            );
            assert!(
                version.manifest_path.contains(".manifest"),
                "manifest_path should contain .manifest for version {}",
                expected_version
            );
            assert!(
                version.manifest_size.is_some(),
                "manifest_size should be set for version {}",
                expected_version
            );
            assert!(
                version.manifest_size.unwrap() > 0,
                "manifest_size should be > 0 for version {}",
                expected_version
            );
            assert!(
                version.timestamp_millis.is_some(),
                "timestamp_millis should be set for version {}",
                expected_version
            );
        }
    }

    #[tokio::test]
    async fn test_describe_table_version() {
        use arrow::array::{Int32Array, RecordBatchIterator};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance::dataset::{Dataset, WriteMode, WriteParams};
        use lance_namespace::models::{CreateNamespaceRequest, DescribeTableVersionRequest};

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        // Create parent namespace first
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["workspace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create a table using write_into_namespace (version 1)
        let table_id = vec!["workspace".to_string(), "test_table".to_string()];
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema.clone());
        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };
        let mut dataset = Dataset::write_into_namespace(
            batches,
            namespace.clone(),
            table_id.clone(),
            Some(write_params),
        )
        .await
        .unwrap();

        // Append data to create version 2
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch2)], arrow_schema);
        dataset.append(batches, None).await.unwrap();

        // Describe version 1
        let mut describe_req = DescribeTableVersionRequest::new();
        describe_req.id = Some(table_id.clone());
        describe_req.version = Some(1);
        let describe_resp = namespace
            .describe_table_version(describe_req)
            .await
            .unwrap();

        let version = &describe_resp.version;
        assert_eq!(version.version, 1);
        assert!(version.timestamp_millis.is_some());
        assert!(
            !version.manifest_path.is_empty(),
            "manifest_path should be set"
        );
        assert!(
            version.manifest_path.contains(".manifest"),
            "manifest_path should contain .manifest"
        );
        assert!(
            version.manifest_size.is_some(),
            "manifest_size should be set"
        );
        assert!(
            version.manifest_size.unwrap() > 0,
            "manifest_size should be > 0"
        );

        // Describe version 2
        let mut describe_req = DescribeTableVersionRequest::new();
        describe_req.id = Some(table_id.clone());
        describe_req.version = Some(2);
        let describe_resp = namespace
            .describe_table_version(describe_req)
            .await
            .unwrap();

        let version = &describe_resp.version;
        assert_eq!(version.version, 2);
        assert!(version.timestamp_millis.is_some());
        assert!(
            !version.manifest_path.is_empty(),
            "manifest_path should be set"
        );
        assert!(
            version.manifest_size.is_some(),
            "manifest_size should be set"
        );
        assert!(
            version.manifest_size.unwrap() > 0,
            "manifest_size should be > 0"
        );
    }

    #[tokio::test]
    async fn test_describe_table_version_latest() {
        use arrow::array::{Int32Array, RecordBatchIterator};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance::dataset::{Dataset, WriteMode, WriteParams};
        use lance_namespace::models::{CreateNamespaceRequest, DescribeTableVersionRequest};

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        // Create parent namespace first
        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["workspace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        // Create a table using write_into_namespace (version 1)
        let table_id = vec!["workspace".to_string(), "test_table".to_string()];
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema.clone());
        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };
        let mut dataset = Dataset::write_into_namespace(
            batches,
            namespace.clone(),
            table_id.clone(),
            Some(write_params),
        )
        .await
        .unwrap();

        // Append to create version 2
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch2)], arrow_schema.clone());
        dataset.append(batches, None).await.unwrap();

        // Append to create version 3
        let batch3 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![300, 400]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch3)], arrow_schema);
        dataset.append(batches, None).await.unwrap();

        // Describe latest version (no version specified)
        let mut describe_req = DescribeTableVersionRequest::new();
        describe_req.id = Some(table_id.clone());
        describe_req.version = None;
        let describe_resp = namespace
            .describe_table_version(describe_req)
            .await
            .unwrap();

        // Should return version 3 as it's the latest
        assert_eq!(describe_resp.version.version, 3);
    }

    #[tokio::test]
    async fn test_create_table_version() {
        use futures::TryStreamExt;
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableVersionRequest;

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Open the dataset using from_namespace to get proper object_store and paths
        let table_id = vec!["test_table".to_string()];
        let dataset = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();

        // Use dataset's object_store to find and copy the manifest
        let versions_path = dataset.versions_dir();
        let manifest_metas: Vec<_> = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .list(Some(&versions_path))
            .try_collect()
            .await
            .unwrap();

        let manifest_meta = manifest_metas
            .iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("No manifest file found");

        // Read the existing manifest data
        let manifest_data = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .get(&manifest_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // Write to a staging location using the dataset's object_store
        let staging_path = dataset.versions_dir().join("staging_manifest");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(&staging_path, manifest_data.into())
            .await
            .unwrap();

        // Create version 2 from staging manifest
        // Use the same naming scheme as the existing dataset (V2)
        let mut create_version_req = CreateTableVersionRequest::new(2, staging_path.to_string());
        create_version_req.id = Some(table_id.clone());
        create_version_req.naming_scheme = Some("V2".to_string());

        let result = namespace.create_table_version(create_version_req).await;
        assert!(
            result.is_ok(),
            "create_table_version should succeed: {:?}",
            result
        );

        // Verify version 2 was created at the path returned in the response
        let response = result.unwrap();
        let version_info = response
            .version
            .expect("response should contain version info");
        let version_2_path = Path::parse(&version_info.manifest_path).unwrap();
        let head_result = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .head(&version_2_path)
            .await;
        assert!(
            head_result.is_ok(),
            "Version 2 manifest should exist at {}",
            version_2_path
        );

        // Verify the staging file has been deleted
        let staging_head_result = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .head(&staging_path)
            .await;
        assert!(
            staging_head_result.is_err(),
            "Staging manifest should have been deleted after create_table_version"
        );
    }

    #[tokio::test]
    async fn test_create_table_version_idempotent() {
        // A network retry of create_table_version with the same staging content
        // must succeed (not ConcurrentModification) once the version is published.
        use futures::TryStreamExt;
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableVersionRequest;

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        let table_id = vec!["test_table".to_string()];
        let dataset = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();

        let versions_path = dataset.versions_dir();
        let manifest_metas: Vec<_> = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .list(Some(&versions_path))
            .try_collect()
            .await
            .unwrap();

        let manifest_meta = manifest_metas
            .iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("No manifest file found");

        let manifest_data = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .get(&manifest_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let staging_path = dataset.versions_dir().join("staging_manifest");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(&staging_path, manifest_data.clone().into())
            .await
            .unwrap();

        let mut create_version_req = CreateTableVersionRequest::new(2, staging_path.to_string());
        create_version_req.id = Some(table_id.clone());
        create_version_req.naming_scheme = Some("V2".to_string());
        let first = namespace
            .create_table_version(create_version_req)
            .await
            .expect("first create_table_version should succeed");

        // Re-stage identical bytes (simulates Lance commit retry rewriting staging).
        let retry_staging = dataset.versions_dir().join("staging_manifest_retry");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(&retry_staging, manifest_data.into())
            .await
            .unwrap();

        let mut retry_req = CreateTableVersionRequest::new(2, retry_staging.to_string());
        retry_req.id = Some(table_id.clone());
        retry_req.naming_scheme = Some("V2".to_string());
        let second = namespace
            .create_table_version(retry_req)
            .await
            .expect("idempotent retry must succeed");

        assert_eq!(
            first.version.as_ref().map(|v| v.version),
            second.version.as_ref().map(|v| v.version)
        );
        assert_eq!(
            first.version.as_ref().map(|v| &v.manifest_path),
            second.version.as_ref().map(|v| &v.manifest_path)
        );
    }

    #[tokio::test]
    async fn test_create_table_version_conflict() {
        // Same version with different content must fail ConcurrentModification.
        use futures::TryStreamExt;
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableVersionRequest;

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Open the dataset using from_namespace to get proper object_store and paths
        let table_id = vec!["test_table".to_string()];
        let dataset = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();

        // Use dataset's object_store to find and copy the manifest
        let versions_path = dataset.versions_dir();
        let manifest_metas: Vec<_> = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .list(Some(&versions_path))
            .try_collect()
            .await
            .unwrap();

        let manifest_meta = manifest_metas
            .iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("No manifest file found");

        // Read the existing manifest data
        let manifest_data = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .get(&manifest_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        // Write to a staging location using the dataset's object_store
        let staging_path = dataset.versions_dir().join("staging_manifest");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(&staging_path, manifest_data.into())
            .await
            .unwrap();

        // First create version 2 (should succeed)
        let mut create_version_req = CreateTableVersionRequest::new(2, staging_path.to_string());
        create_version_req.id = Some(table_id.clone());
        create_version_req.naming_scheme = Some("V2".to_string());
        let first_result = namespace.create_table_version(create_version_req).await;
        assert!(
            first_result.is_ok(),
            "First create_table_version for version 2 should succeed: {:?}",
            first_result
        );

        // Get the path from the response for verification
        let version_2_path = Path::parse(
            &first_result
                .unwrap()
                .version
                .expect("response should contain version info")
                .manifest_path,
        )
        .unwrap();

        // Different content for the same version number must conflict.
        let conflict_staging = dataset.versions_dir().join("staging_manifest_conflict");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(
                &conflict_staging,
                bytes::Bytes::from_static(b"not-a-real-manifest").into(),
            )
            .await
            .unwrap();

        let mut create_version_req =
            CreateTableVersionRequest::new(2, conflict_staging.to_string());
        create_version_req.id = Some(table_id.clone());
        create_version_req.naming_scheme = Some("V2".to_string());

        let result = namespace.create_table_version(create_version_req).await;
        assert!(
            result.is_err(),
            "create_table_version should fail for existing version with different content"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("already exists") || err.contains("ConcurrentModification"),
            "expected ConcurrentModification, got: {err}"
        );

        // Verify version 2 still exists using the dataset's object_store
        let head_result = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .head(&version_2_path)
            .await;
        assert!(
            head_result.is_ok(),
            "Version 2 manifest should still exist at {}",
            version_2_path
        );
    }

    #[tokio::test]
    async fn test_create_table_version_cas_rejects_gap() {
        // Strict CAS: version must be latest+1; skipping ahead is ConcurrentModification.
        use futures::TryStreamExt;
        use lance::dataset::builder::DatasetBuilder;
        use lance_namespace::models::CreateTableVersionRequest;

        let temp_dir = TempStrDir::default();
        let temp_path: &str = &temp_dir;

        let namespace: Arc<dyn LanceNamespace> = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .build()
                .await
                .unwrap(),
        );

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_req, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        let table_id = vec!["test_table".to_string()];
        let dataset = DatasetBuilder::from_namespace(namespace.clone(), table_id.clone())
            .await
            .unwrap()
            .load()
            .await
            .unwrap();

        let versions_path = dataset.versions_dir();
        let manifest_metas: Vec<_> = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .list(Some(&versions_path))
            .try_collect()
            .await
            .unwrap();
        let manifest_meta = manifest_metas
            .iter()
            .find(|m| {
                m.location
                    .filename()
                    .map(|f| f.ends_with(".manifest"))
                    .unwrap_or(false)
            })
            .expect("No manifest file found");
        let manifest_data = dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .get(&manifest_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let staging_path = dataset.versions_dir().join("staging_gap");
        dataset
            .object_store(None)
            .await
            .unwrap()
            .inner
            .put(&staging_path, manifest_data.into())
            .await
            .unwrap();

        // After create_table, latest is 1; requesting 5 must fail CAS.
        let mut req = CreateTableVersionRequest::new(5, staging_path.to_string());
        req.id = Some(table_id);
        req.naming_scheme = Some("V2".to_string());
        let err = namespace
            .create_table_version(req)
            .await
            .expect_err("gap create must fail CAS");
        let msg = err.to_string();
        assert!(
            msg.contains("CAS") || msg.contains("ConcurrentModification"),
            "expected CAS ConcurrentModification, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_create_table_version_branch_cas_requires_parent_version() {
        // Empty branch chain with BranchContents must bootstrap at parent_version,
        // not an arbitrary version (e.g. 1 when forked from v2).
        use futures::TryStreamExt;
        use lance_namespace::models::CreateTableVersionRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let main_uri = open_dataset(&namespace, "users").await.uri().to_string();
        append_scalar_version(&main_uri, 10).await; // main -> v2

        let mut main = open_dataset(&namespace, "users").await;
        let fork_version = main.version().version;
        assert_eq!(fork_version, 2);
        let branch_uri = main
            .create_branch("exp", fork_version, None)
            .await
            .unwrap()
            .uri()
            .to_string();

        let branch_ds = Dataset::open(&branch_uri).await.unwrap();
        let versions_dir = branch_ds.versions_dir();
        let store = branch_ds.object_store(None).await.unwrap();
        let manifests: Vec<_> = store
            .inner
            .list(Some(&versions_dir))
            .try_collect()
            .await
            .unwrap();
        for meta in &manifests {
            if meta
                .location
                .filename()
                .is_some_and(|f| f.ends_with(".manifest"))
            {
                store.inner.delete(&meta.location).await.unwrap();
            }
        }
        // Confirm the branch object-store chain is empty (do not open the dataset:
        // with no manifests, Dataset::open would fail).
        let remaining_manifests = store
            .inner
            .list(Some(&versions_dir))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .filter(|m| {
                m.location
                    .filename()
                    .is_some_and(|f| f.ends_with(".manifest"))
            })
            .count();
        assert_eq!(
            remaining_manifests, 0,
            "branch version chain should be empty after deleting manifests"
        );

        // Stage bytes from a main manifest.
        let main_ds = open_dataset(&namespace, "users").await;
        let main_versions = main_ds.versions_dir();
        let main_store = main_ds.object_store(None).await.unwrap();
        let source_meta = main_store
            .inner
            .list(Some(&main_versions))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .find(|m| {
                m.location
                    .filename()
                    .is_some_and(|f| f.ends_with(".manifest"))
            })
            .expect("main should have a manifest");
        let source_bytes = main_store
            .inner
            .get(&source_meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let staging_wrong = versions_dir.clone().join("staging_wrong");
        store
            .inner
            .put(&staging_wrong, source_bytes.clone().into())
            .await
            .unwrap();
        let err = namespace
            .create_table_version(CreateTableVersionRequest {
                id: Some(vec!["users".to_string()]),
                version: 1,
                manifest_path: staging_wrong.to_string(),
                naming_scheme: Some("V2".to_string()),
                branch: Some("exp".to_string()),
                ..Default::default()
            })
            .await
            .expect_err("bootstrap at v1 must fail when parent_version is 2");
        let msg = err.to_string();
        assert!(
            msg.contains("CAS") || msg.contains("ConcurrentModification"),
            "expected CAS ConcurrentModification, got: {msg}"
        );

        let staging_ok = versions_dir.join("staging_ok");
        store
            .inner
            .put(&staging_ok, source_bytes.into())
            .await
            .unwrap();
        let resp = namespace
            .create_table_version(CreateTableVersionRequest {
                id: Some(vec!["users".to_string()]),
                version: 2,
                manifest_path: staging_ok.to_string(),
                naming_scheme: Some("V2".to_string()),
                branch: Some("exp".to_string()),
                ..Default::default()
            })
            .await
            .expect("bootstrap at parent_version must succeed");
        assert_eq!(resp.version.as_ref().map(|v| v.version), Some(2));
    }

    #[tokio::test]
    async fn test_create_table_version_table_not_found() {
        use lance_namespace::models::CreateTableVersionRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .table_version_tracking_enabled(true)
            .build()
            .await
            .unwrap();

        // Try to create version for non-existent table
        let mut create_version_req =
            CreateTableVersionRequest::new(1, "/some/staging/path".to_string());
        create_version_req.id = Some(vec!["non_existent_table".to_string()]);

        let result = namespace.create_table_version(create_version_req).await;
        assert!(
            result.is_err(),
            "create_table_version should fail for non-existent table"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Table not found"),
            "Error should mention table not found, got: {}",
            err_msg
        );
    }

    /// End-to-end integration test module for table version tracking.
    mod e2e_table_version_tracking {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Tracking wrapper around a namespace that counts method invocations.
        struct TrackingNamespace {
            inner: DirectoryNamespace,
            create_table_version_count: AtomicUsize,
            describe_table_version_count: AtomicUsize,
            list_table_versions_count: AtomicUsize,
        }

        impl TrackingNamespace {
            fn new(inner: DirectoryNamespace) -> Self {
                Self {
                    inner,
                    create_table_version_count: AtomicUsize::new(0),
                    describe_table_version_count: AtomicUsize::new(0),
                    list_table_versions_count: AtomicUsize::new(0),
                }
            }

            fn create_table_version_calls(&self) -> usize {
                self.create_table_version_count.load(Ordering::SeqCst)
            }

            fn describe_table_version_calls(&self) -> usize {
                self.describe_table_version_count.load(Ordering::SeqCst)
            }

            fn list_table_versions_calls(&self) -> usize {
                self.list_table_versions_count.load(Ordering::SeqCst)
            }
        }

        impl std::fmt::Debug for TrackingNamespace {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TrackingNamespace")
                    .field(
                        "create_table_version_calls",
                        &self.create_table_version_calls(),
                    )
                    .finish()
            }
        }

        #[async_trait]
        impl LanceNamespace for TrackingNamespace {
            async fn create_namespace(
                &self,
                request: CreateNamespaceRequest,
            ) -> Result<CreateNamespaceResponse> {
                self.inner.create_namespace(request).await
            }

            async fn describe_namespace(
                &self,
                request: DescribeNamespaceRequest,
            ) -> Result<DescribeNamespaceResponse> {
                self.inner.describe_namespace(request).await
            }

            async fn namespace_exists(&self, request: NamespaceExistsRequest) -> Result<()> {
                self.inner.namespace_exists(request).await
            }

            async fn list_namespaces(
                &self,
                request: ListNamespacesRequest,
            ) -> Result<ListNamespacesResponse> {
                self.inner.list_namespaces(request).await
            }

            async fn drop_namespace(
                &self,
                request: DropNamespaceRequest,
            ) -> Result<DropNamespaceResponse> {
                self.inner.drop_namespace(request).await
            }

            async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
                self.inner.list_tables(request).await
            }

            async fn describe_table(
                &self,
                request: DescribeTableRequest,
            ) -> Result<DescribeTableResponse> {
                self.inner.describe_table(request).await
            }

            async fn table_exists(&self, request: TableExistsRequest) -> Result<()> {
                self.inner.table_exists(request).await
            }

            async fn drop_table(&self, request: DropTableRequest) -> Result<DropTableResponse> {
                self.inner.drop_table(request).await
            }

            async fn create_table(
                &self,
                request: CreateTableRequest,
                request_data: Bytes,
            ) -> Result<CreateTableResponse> {
                self.inner.create_table(request, request_data).await
            }

            async fn declare_table(
                &self,
                request: DeclareTableRequest,
            ) -> Result<DeclareTableResponse> {
                self.inner.declare_table(request).await
            }

            async fn list_table_versions(
                &self,
                request: ListTableVersionsRequest,
            ) -> Result<ListTableVersionsResponse> {
                self.list_table_versions_count
                    .fetch_add(1, Ordering::SeqCst);
                self.inner.list_table_versions(request).await
            }

            async fn create_table_version(
                &self,
                request: CreateTableVersionRequest,
            ) -> Result<CreateTableVersionResponse> {
                self.create_table_version_count
                    .fetch_add(1, Ordering::SeqCst);
                self.inner.create_table_version(request).await
            }

            async fn describe_table_version(
                &self,
                request: DescribeTableVersionRequest,
            ) -> Result<DescribeTableVersionResponse> {
                self.describe_table_version_count
                    .fetch_add(1, Ordering::SeqCst);
                self.inner.describe_table_version(request).await
            }

            async fn batch_delete_table_versions(
                &self,
                request: BatchDeleteTableVersionsRequest,
            ) -> Result<BatchDeleteTableVersionsResponse> {
                self.inner.batch_delete_table_versions(request).await
            }

            fn namespace_id(&self) -> String {
                self.inner.namespace_id()
            }
        }

        #[tokio::test]
        async fn test_describe_table_returns_managed_versioning() {
            use lance_namespace::models::{CreateNamespaceRequest, DescribeTableRequest};

            let temp_dir = TempStdDir::default();
            let temp_path = temp_dir.to_str().unwrap();

            // Create namespace with table_version_tracking_enabled and manifest_enabled
            let ns = DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .manifest_enabled(true)
                .build()
                .await
                .unwrap();

            // Create parent namespace
            let mut create_ns_req = CreateNamespaceRequest::new();
            create_ns_req.id = Some(vec!["workspace".to_string()]);
            ns.create_namespace(create_ns_req).await.unwrap();

            // Create a table with multi-level ID (namespace + table)
            let schema = create_test_schema();
            let ipc_data = create_test_ipc_data(&schema);
            let mut create_req = CreateTableRequest::new();
            create_req.id = Some(vec!["workspace".to_string(), "test_table".to_string()]);
            ns.create_table(create_req, bytes::Bytes::from(ipc_data))
                .await
                .unwrap();

            // Describe table should return managed_versioning=true
            let mut describe_req = DescribeTableRequest::new();
            describe_req.id = Some(vec!["workspace".to_string(), "test_table".to_string()]);
            let describe_resp = ns.describe_table(describe_req).await.unwrap();

            // managed_versioning should be true
            assert_eq!(
                describe_resp.managed_versioning,
                Some(true),
                "managed_versioning should be true when table_version_tracking_enabled=true"
            );
        }

        #[tokio::test]
        async fn test_external_manifest_store_invokes_namespace_apis() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use lance::Dataset;
            use lance::dataset::builder::DatasetBuilder;
            use lance::dataset::{WriteMode, WriteParams};
            use lance_namespace::models::CreateNamespaceRequest;

            let temp_dir = TempStdDir::default();
            let temp_path = temp_dir.to_str().unwrap();

            // Create namespace with table_version_tracking_enabled and manifest_enabled
            let inner_ns = DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .manifest_enabled(true)
                .build()
                .await
                .unwrap();

            let tracking_ns = Arc::new(TrackingNamespace::new(inner_ns));
            let ns: Arc<dyn LanceNamespace> = tracking_ns.clone();

            // Create parent namespace
            let mut create_ns_req = CreateNamespaceRequest::new();
            create_ns_req.id = Some(vec!["workspace".to_string()]);
            ns.create_namespace(create_ns_req).await.unwrap();

            // Create a table with multi-level ID (namespace + table)
            let table_id = vec!["workspace".to_string(), "test_table".to_string()];

            // Create some initial data
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();

            // Create a table using write_into_namespace
            let batches = RecordBatchIterator::new(vec![Ok(batch.clone())], arrow_schema.clone());
            let write_params = WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            };
            let mut dataset = Dataset::write_into_namespace(
                batches,
                ns.clone(),
                table_id.clone(),
                Some(write_params),
            )
            .await
            .unwrap();
            assert_eq!(dataset.version().version, 1);

            // Verify create_table_version was called once during initial write_into_namespace
            assert_eq!(
                tracking_ns.create_table_version_calls(),
                1,
                "create_table_version should have been called once during initial write_into_namespace"
            );

            // Append data - this should call create_table_version again
            let append_batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(StringArray::from(vec!["d", "e", "f"])),
                ],
            )
            .unwrap();
            let append_batches = RecordBatchIterator::new(vec![Ok(append_batch)], arrow_schema);
            dataset.append(append_batches, None).await.unwrap();

            assert_eq!(
                tracking_ns.create_table_version_calls(),
                2,
                "create_table_version should have been called twice (once for create, once for append)"
            );

            // checkout_latest should call list_table_versions exactly once
            let initial_list_calls = tracking_ns.list_table_versions_calls();
            let latest_dataset = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
                .await
                .unwrap()
                .load()
                .await
                .unwrap();
            assert_eq!(latest_dataset.version().version, 2);
            assert_eq!(
                tracking_ns.list_table_versions_calls(),
                initial_list_calls + 1,
                "list_table_versions should have been called exactly once during checkout_latest"
            );

            // checkout to specific version should call describe_table_version exactly once
            let initial_describe_calls = tracking_ns.describe_table_version_calls();
            let v1_dataset = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
                .await
                .unwrap()
                .with_version(1)
                .load()
                .await
                .unwrap();
            assert_eq!(v1_dataset.version().version, 1);
            assert_eq!(
                tracking_ns.describe_table_version_calls(),
                initial_describe_calls + 1,
                "describe_table_version should have been called exactly once during checkout to version 1"
            );
        }

        #[tokio::test]
        async fn test_dataset_commit_with_external_manifest_store() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
            use arrow::record_batch::RecordBatch;
            use futures::TryStreamExt;
            use lance::dataset::{Dataset, WriteMode, WriteParams};
            use lance_namespace::models::CreateNamespaceRequest;
            use lance_table::io::commit::ManifestNamingScheme;

            let temp_dir = TempStdDir::default();
            let temp_path = temp_dir.to_str().unwrap();

            // Create namespace with table_version_tracking_enabled and manifest_enabled
            let inner_ns = DirectoryNamespaceBuilder::new(temp_path)
                .table_version_tracking_enabled(true)
                .manifest_enabled(true)
                .build()
                .await
                .unwrap();

            let tracking_ns: Arc<dyn LanceNamespace> = Arc::new(TrackingNamespace::new(inner_ns));

            // Create parent namespace
            let mut create_ns_req = CreateNamespaceRequest::new();
            create_ns_req.id = Some(vec!["workspace".to_string()]);
            tracking_ns.create_namespace(create_ns_req).await.unwrap();

            // Create a table using write_into_namespace
            let table_id = vec!["workspace".to_string(), "test_table".to_string()];
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();
            let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema.clone());
            let write_params = WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            };
            let dataset = Dataset::write_into_namespace(
                batches,
                tracking_ns.clone(),
                table_id.clone(),
                Some(write_params),
            )
            .await
            .unwrap();
            assert_eq!(dataset.version().version, 1);

            // Append data using write_into_namespace (APPEND mode)
            let batch2 = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5, 6])),
                    Arc::new(StringArray::from(vec!["d", "e", "f"])),
                ],
            )
            .unwrap();
            let batches = RecordBatchIterator::new(vec![Ok(batch2)], arrow_schema);
            let write_params = WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            };
            Dataset::write_into_namespace(
                batches,
                tracking_ns.clone(),
                table_id.clone(),
                Some(write_params),
            )
            .await
            .unwrap();

            // Verify version 2 was created using the dataset's object_store
            // List manifests in the versions directory to find the V2 named manifest
            let manifest_metas: Vec<_> = dataset
                .object_store(None)
                .await
                .unwrap()
                .inner
                .list(Some(&dataset.versions_dir()))
                .try_collect()
                .await
                .unwrap();
            let version_2_found = manifest_metas.iter().any(|m| {
                m.location
                    .filename()
                    .map(|f| {
                        f.ends_with(".manifest")
                            && ManifestNamingScheme::V2.parse_version(f) == Some(2)
                    })
                    .unwrap_or(false)
            });
            assert!(
                version_2_found,
                "Version 2 manifest should exist in versions directory"
            );
        }

        /// Helper: create a namespace and a table with some rows, returning (namespace, table_id)
        async fn create_ns_with_table() -> (DirectoryNamespace, TempStdDir, Vec<String>) {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, temp_dir) = create_test_namespace().await;

            let schema = create_test_schema();
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let mut request = CreateTableRequest::new();
            let table_id = vec!["test_ops_table".to_string()];
            request.id = Some(table_id.clone());

            namespace
                .create_table(request, Bytes::from(buffer))
                .await
                .unwrap();

            (namespace, temp_dir, table_id)
        }

        #[tokio::test]
        async fn test_count_table_rows_basic() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };

            let count = namespace.count_table_rows(request).await.unwrap();
            assert_eq!(count, 3);
        }

        #[tokio::test]
        async fn test_count_table_rows_with_predicate() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: Some("id > 1".to_string()),
                ..Default::default()
            };

            let count = namespace.count_table_rows(request).await.unwrap();
            assert_eq!(count, 2);
        }

        #[tokio::test]
        async fn test_query_table_invalid_distance_type() {
            let (namespace, _temp_dir, table_id) = create_ns_with_vector_table().await;

            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: Some(vec![1.0, 0.0, 0.0, 0.0]),
                multi_vector: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 2,
                vector,
                vector_column: Some("vector".to_string()),
                distance_type: Some("invalid_metric".to_string()),
                filter: None,
                offset: None,
                version: None,
                ..Default::default()
            };

            let result = namespace.query_table(request).await;
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("Unknown distance type"),
                "Expected error about unknown distance type, got: {}",
                err_msg
            );
        }

        #[tokio::test]
        async fn test_insert_into_table_append() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // Prepare new data to insert
            let schema = create_test_schema();
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![4, 5]);
            let name_array = StringArray::from(vec!["Dave", "Eve"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let request = InsertIntoTableRequest {
                id: Some(table_id.clone()),
                mode: Some("append".to_string()),
                ..Default::default()
            };

            let response = namespace
                .insert_into_table(request, Bytes::from(buffer))
                .await
                .unwrap();
            assert!(response.transaction_id.is_none());

            // Verify total rows
            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };
            let count = namespace.count_table_rows(count_req).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_insert_into_table_overwrite() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let schema = create_test_schema();
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![10, 20]);
            let name_array = StringArray::from(vec!["X", "Y"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let request = InsertIntoTableRequest {
                id: Some(table_id.clone()),
                mode: Some("overwrite".to_string()),
                ..Default::default()
            };

            namespace
                .insert_into_table(request, Bytes::from(buffer))
                .await
                .unwrap();

            // Verify overwrite: only 2 rows remain
            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };
            let count = namespace.count_table_rows(count_req).await.unwrap();
            assert_eq!(count, 2);
        }

        #[tokio::test]
        async fn test_insert_into_table_empty_data() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = InsertIntoTableRequest {
                id: Some(table_id),
                mode: None,
                ..Default::default()
            };

            let result = namespace.insert_into_table(request, Bytes::new()).await;
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Arrow IPC stream) is required")
            );
        }

        #[tokio::test]
        async fn test_insert_into_table_with_storage_options() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let temp_dir = TempStdDir::default();

            // Build namespace with a (no-op) storage option so self.storage_options is Some
            let namespace = DirectoryNamespaceBuilder::new(temp_dir.to_str().unwrap())
                .storage_option("allow_http", "true")
                .build()
                .await
                .unwrap();

            // Create a table first
            let schema = create_test_schema();
            let ipc_data = create_test_ipc_data(&schema);
            let mut create_req = CreateTableRequest::new();
            let table_id = vec!["so_table".to_string()];
            create_req.id = Some(table_id.clone());
            namespace
                .create_table(create_req, Bytes::from(ipc_data))
                .await
                .unwrap();

            // Insert with storage_options present — covers store_params closure
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![10, 20]);
            let name_array = StringArray::from(vec!["X", "Y"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let request = InsertIntoTableRequest {
                id: Some(table_id.clone()),
                mode: Some("append".to_string()),
                ..Default::default()
            };

            let response = namespace
                .insert_into_table(request, Bytes::from(buffer))
                .await
                .unwrap();
            assert!(response.transaction_id.is_none());

            // Verify rows were inserted
            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };
            let count = namespace.count_table_rows(count_req).await.unwrap();
            assert_eq!(count, 2);
        }

        #[tokio::test]
        async fn test_query_table_basic() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 10,
                filter: None,
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            // Decode IPC and verify
            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);
        }

        #[tokio::test]
        async fn test_query_table_with_filter() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 10,
                filter: Some("id <= 2".to_string()),
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);
        }

        #[tokio::test]
        async fn test_query_table_with_limit_and_offset() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 2,
                filter: None,
                offset: Some(1),
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);
        }

        #[tokio::test]
        async fn test_query_table_no_limit() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // k=0 means no limit
            let request = QueryTableRequest {
                id: Some(table_id),
                k: 0,
                filter: None,
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);
        }

        #[tokio::test]
        async fn test_query_table_with_columns() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let columns = Box::new(lance_namespace::models::QueryTableRequestColumns {
                column_names: Some(vec!["id".to_string()]),
                column_aliases: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 10,
                filter: None,
                offset: None,
                version: None,
                columns: Some(columns),
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let schema = reader.schema();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "id");
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);
        }

        #[tokio::test]
        async fn test_count_table_rows_with_version() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // Insert more data to create version 2
            let schema = create_test_schema();
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![4, 5]);
            let name_array = StringArray::from(vec!["Dave", "Eve"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let request = InsertIntoTableRequest {
                id: Some(table_id.clone()),
                mode: None,
                ..Default::default()
            };
            namespace
                .insert_into_table(request, Bytes::from(buffer))
                .await
                .unwrap();

            // Version 1 should have 3 rows
            let count_req = CountTableRowsRequest {
                id: Some(table_id.clone()),
                version: Some(1),
                predicate: None,
                ..Default::default()
            };
            let count = namespace.count_table_rows(count_req).await.unwrap();
            assert_eq!(count, 3);

            // Latest version should have 5 rows
            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };
            let count = namespace.count_table_rows(count_req).await.unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_query_table_with_version() {
            use arrow::array::{Int32Array, StringArray};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // Insert more data to create version 2
            let schema = create_test_schema();
            let arrow_schema = convert_json_arrow_schema(&schema).unwrap();
            let arrow_schema = Arc::new(arrow_schema);

            let id_array = Int32Array::from(vec![4, 5]);
            let name_array = StringArray::from(vec!["Dave", "Eve"]);
            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(name_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            let request = InsertIntoTableRequest {
                id: Some(table_id.clone()),
                mode: None,
                ..Default::default()
            };
            namespace
                .insert_into_table(request, Bytes::from(buffer))
                .await
                .unwrap();

            // Query version 1 should return 3 rows
            let request = QueryTableRequest {
                id: Some(table_id.clone()),
                k: 100,
                filter: None,
                offset: None,
                version: Some(1),
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();
            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);

            // Query latest version should return 5 rows
            let request = QueryTableRequest {
                id: Some(table_id),
                k: 100,
                filter: None,
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();
            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 5);
        }

        /// Helper to create a namespace with a table that has a vector column for
        /// vector search tests.
        async fn create_ns_with_vector_table() -> (DirectoryNamespace, TempStdDir, Vec<String>) {
            use arrow::array::{FixedSizeListArray, Float32Array, Int32Array};
            use arrow::ipc::writer::StreamWriter;

            let (namespace, temp_dir) = create_test_namespace().await;

            // Build schema: id (int32), vector (fixed_size_list<float32>[4])
            let arrow_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
                arrow::datatypes::Field::new(
                    "vector",
                    arrow::datatypes::DataType::FixedSizeList(
                        Arc::new(arrow::datatypes::Field::new(
                            "item",
                            arrow::datatypes::DataType::Float32,
                            true,
                        )),
                        4,
                    ),
                    true,
                ),
            ]));

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let values = Float32Array::from(vec![
                1.0, 0.0, 0.0, 0.0, // vector for id=1
                0.0, 1.0, 0.0, 0.0, // vector for id=2
                0.0, 0.0, 1.0, 0.0, // vector for id=3
            ]);
            let vector_array = FixedSizeListArray::try_new(
                Arc::new(arrow::datatypes::Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    true,
                )),
                4,
                Arc::new(values),
                None,
            )
            .unwrap();

            let batch = arrow::record_batch::RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(id_array), Arc::new(vector_array)],
            )
            .unwrap();

            let mut buffer = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buffer, &arrow_schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }

            // Write as a Lance dataset directly
            let table_name = "vector_table";
            let table_uri = format!("{}/{}.lance", temp_dir.to_str().unwrap(), table_name);
            let reader = arrow::record_batch::RecordBatchIterator::new(
                vec![Ok(batch)],
                arrow_schema.clone(),
            );
            Dataset::write(reader, &table_uri, None).await.unwrap();

            let table_id = vec![table_name.to_string()];
            (namespace, temp_dir, table_id)
        }

        #[tokio::test]
        async fn test_query_table_vector_search() {
            let (namespace, _temp_dir, table_id) = create_ns_with_vector_table().await;

            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: Some(vec![1.0, 0.0, 0.0, 0.0]),
                multi_vector: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 2,
                vector,
                filter: None,
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);
        }

        #[tokio::test]
        async fn test_query_table_vector_search_with_distance_type() {
            let (namespace, _temp_dir, table_id) = create_ns_with_vector_table().await;

            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: Some(vec![1.0, 0.0, 0.0, 0.0]),
                multi_vector: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 3,
                vector,
                filter: None,
                offset: None,
                version: None,
                distance_type: Some("cosine".to_string()),
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);
        }

        #[tokio::test]
        async fn test_query_table_vector_search_with_filter() {
            let (namespace, _temp_dir, table_id) = create_ns_with_vector_table().await;

            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: Some(vec![1.0, 0.0, 0.0, 0.0]),
                multi_vector: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 10,
                vector,
                filter: Some("id <= 2".to_string()),
                offset: None,
                version: None,
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total_rows <= 2);
        }

        #[tokio::test]
        async fn test_query_table_vector_search_with_nprobes_and_refine() {
            let (namespace, _temp_dir, table_id) = create_ns_with_vector_table().await;

            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: Some(vec![0.0, 1.0, 0.0, 0.0]),
                multi_vector: None,
            });

            let request = QueryTableRequest {
                id: Some(table_id),
                k: 2,
                vector,
                filter: None,
                offset: None,
                version: None,
                nprobes: Some(1),
                refine_factor: Some(1),
                prefilter: Some(true),
                ..Default::default()
            };

            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);
        }

        #[tokio::test]
        async fn test_namespace_id() {
            let (namespace, _temp_dir) = create_test_namespace().await;
            let id = namespace.namespace_id();
            assert!(id.contains("DirectoryNamespace"));
            assert!(id.contains("root"));
        }

        #[tokio::test]
        async fn test_query_table_empty_table() {
            let (namespace, _temp_dir) = create_test_namespace().await;

            // Create table with empty IPC data (schema only, no rows)
            let schema = create_test_schema();
            let ipc_data = create_test_ipc_data(&schema);
            let mut create_request = CreateTableRequest::new();
            create_request.id = Some(vec!["empty_table".to_string()]);
            namespace
                .create_table(create_request, bytes::Bytes::from(ipc_data))
                .await
                .unwrap();

            // Query the empty table — should hit the "no batches" else branch
            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: None,
                multi_vector: None,
            });
            let request = QueryTableRequest {
                id: Some(vec!["empty_table".to_string()]),
                k: 10,
                vector,
                ..Default::default()
            };
            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 0, "empty table should yield no rows");
        }

        #[tokio::test]
        async fn test_query_table_with_plain_filter_no_vector() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // Query with filter but no vector (plain scan path + filter)
            let vector = Box::new(lance_namespace::models::QueryTableRequestVector {
                single_vector: None,
                multi_vector: None,
            });
            let request = QueryTableRequest {
                id: Some(table_id),
                k: 0,
                vector,
                filter: Some("id > 1".to_string()),
                ..Default::default()
            };
            let bytes = namespace.query_table(request).await.unwrap();

            let cursor = Cursor::new(bytes.to_vec());
            let reader = FileReader::try_new(cursor, None).unwrap();
            let batches: Vec<_> = reader.into_iter().map(|b| b.unwrap()).collect();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total_rows > 0);
            assert!(total_rows < 3);
        }

        // ---------------------- update_table / delete_from_table ----------------------

        #[tokio::test]
        async fn test_update_full_table() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // Capture base version so we can assert the update bumped it.
            let base_version = open_dataset(&namespace, &table_id[0])
                .await
                .version()
                .version;

            let request = UpdateTableRequest {
                id: Some(table_id.clone()),
                updates: vec![vec!["name".to_string(), "'updated'".to_string()]],
                predicate: None,
                ..Default::default()
            };

            let response = namespace.update_table(request).await.unwrap();
            assert_eq!(response.updated_rows, 3);
            assert!(response.version as u64 > base_version);

            // Validate that all rows now carry the new value.
            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: Some("name = 'updated'".to_string()),
                ..Default::default()
            };
            assert_eq!(namespace.count_table_rows(count_req).await.unwrap(), 3);
        }

        #[tokio::test]
        async fn test_update_with_predicate() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = UpdateTableRequest {
                id: Some(table_id.clone()),
                updates: vec![vec!["name".to_string(), "'matched'".to_string()]],
                predicate: Some("id > 1".to_string()),
                ..Default::default()
            };

            let response = namespace.update_table(request).await.unwrap();
            assert_eq!(response.updated_rows, 2);

            // Rows that did not match the predicate must remain unchanged.
            let untouched = CountTableRowsRequest {
                id: Some(table_id.clone()),
                version: None,
                predicate: Some("name = 'Alice'".to_string()),
                ..Default::default()
            };
            assert_eq!(namespace.count_table_rows(untouched).await.unwrap(), 1);

            let touched = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: Some("name = 'matched'".to_string()),
                ..Default::default()
            };
            assert_eq!(namespace.count_table_rows(touched).await.unwrap(), 2);
        }

        #[tokio::test]
        async fn test_update_invalid_expression_returns_invalid_input() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = UpdateTableRequest {
                id: Some(table_id),
                // Reference an unknown column on the right-hand side.
                updates: vec![vec!["name".to_string(), "no_such_column + 1".to_string()]],
                predicate: None,
                ..Default::default()
            };

            let err = namespace.update_table(request).await.unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("Invalid input"),
                "expected Invalid input error, got: {}",
                msg
            );
        }

        #[tokio::test]
        async fn test_update_rejects_duplicate_columns() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = UpdateTableRequest {
                id: Some(table_id),
                updates: vec![
                    vec!["name".to_string(), "'a'".to_string()],
                    vec!["name".to_string(), "'b'".to_string()],
                ],
                predicate: None,
                ..Default::default()
            };

            let err = namespace.update_table(request).await.unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("Invalid input") && msg.contains("more than once"),
                "expected duplicate column InvalidInput error, got: {}",
                msg
            );
        }

        #[tokio::test]
        async fn test_delete_with_predicate() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = DeleteFromTableRequest {
                id: Some(table_id.clone()),
                predicate: "id > 1".to_string(),
                ..Default::default()
            };

            let response = namespace.delete_from_table(request).await.unwrap();
            assert!(response.version.is_some());

            let count_req = CountTableRowsRequest {
                id: Some(table_id),
                version: None,
                predicate: None,
                ..Default::default()
            };
            // Original rows = 3; after deleting `id > 1` only row id=1 remains.
            assert_eq!(namespace.count_table_rows(count_req).await.unwrap(), 1);
        }

        #[tokio::test]
        async fn test_delete_empty_predicate_returns_invalid_input() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            let request = DeleteFromTableRequest {
                id: Some(table_id),
                predicate: "   ".to_string(),
                ..Default::default()
            };

            let err = namespace.delete_from_table(request).await.unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("Invalid input") && msg.contains("non-empty predicate"),
                "expected non-empty predicate InvalidInput error, got: {}",
                msg
            );
        }

        #[tokio::test]
        async fn test_delete_table_not_found() {
            let (namespace, _temp_dir) = create_test_namespace().await;

            let request = DeleteFromTableRequest {
                id: Some(vec!["does_not_exist".to_string()]),
                predicate: "id = 1".to_string(),
                ..Default::default()
            };

            let err = namespace.delete_from_table(request).await.unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("Table not found"),
                "expected TableNotFound for missing table, got: {}",
                msg
            );
        }

        #[tokio::test]
        async fn test_delete_invalid_predicate_returns_invalid_input() {
            let (namespace, _temp_dir, table_id) = create_ns_with_table().await;

            // A predicate referencing a column that does not exist reaches `Dataset::delete`
            // and surfaces as `Error::InvalidInput`, which must map to `InvalidInput` rather
            // than a generic `Internal`.
            let request = DeleteFromTableRequest {
                id: Some(table_id),
                predicate: "no_such_column = 1".to_string(),
                ..Default::default()
            };

            let err = namespace.delete_from_table(request).await.unwrap_err();
            let lance_core::Error::Namespace { source, .. } = &err else {
                panic!("expected a Namespace error, got: {}", err);
            };
            let ns_err = source
                .downcast_ref::<NamespaceError>()
                .expect("expected a NamespaceError source");
            assert_eq!(
                ns_err.code(),
                lance_namespace::ErrorCode::InvalidInput,
                "expected InvalidInput for an invalid delete predicate, got: {}",
                err
            );
        }
    }

    #[tokio::test]
    async fn test_list_all_tables() {
        use lance_namespace::models::ListTablesRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "alpha").await;
        create_scalar_table(&namespace, "beta").await;

        let request = ListTablesRequest {
            id: Some(vec![]),
            page_token: None,
            limit: None,
            ..Default::default()
        };
        let response = namespace.list_all_tables(request).await.unwrap();
        let mut tables = response.tables;
        tables.sort();
        assert_eq!(tables, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_restore_table() {
        use lance_namespace::models::RestoreTableRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        // Create a second version by creating a scalar index (this adds a new version)
        create_scalar_index(&namespace, "users", "users_id_idx").await;

        let dataset = open_dataset(&namespace, "users").await;
        let current_version = dataset.version().version;
        assert!(current_version >= 2, "Should have at least 2 versions");

        // Restore to version 1
        let mut restore_req = RestoreTableRequest::new(1);
        restore_req.id = Some(vec!["users".to_string()]);
        let response = namespace.restore_table(restore_req).await.unwrap();

        // transaction_id should be present (the restore operation)
        assert!(
            response.transaction_id.is_some(),
            "restore_table should return a transaction_id"
        );

        // Verify the dataset now has a new version (restore creates a new version)
        let dataset_after = open_dataset(&namespace, "users").await;
        assert!(
            dataset_after.version().version > current_version,
            "Restore should create a new version"
        );
    }

    #[tokio::test]
    async fn test_alter_table_add_columns() {
        use lance_namespace::models::{
            AddColumnsEntry, AlterTableAddColumnsRequest, DescribeTableRequest,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Add a new column
        let mut new_col = AddColumnsEntry::new("doubled_id".to_string());
        new_col.expression = Some(Some("id * 2".to_string()));
        let mut add_request = AlterTableAddColumnsRequest::new(vec![new_col]);
        add_request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .alter_table_add_columns(add_request)
            .await
            .unwrap();
        assert!(
            response.version > 1,
            "Version should increment after adding columns"
        );

        // Verify via describe_table
        let mut describe_request = DescribeTableRequest::new();
        describe_request.id = Some(vec!["test_table".to_string()]);
        describe_request.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_request).await.unwrap();
        assert!(describe_response.schema.is_some());

        let resp_schema = describe_response.schema.unwrap();
        let field_names: Vec<&str> = resp_schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            field_names.contains(&"doubled_id"),
            "Column 'doubled_id' should exist, got: {:?}",
            field_names
        );
    }

    #[tokio::test]
    async fn test_update_table_schema_metadata() {
        use lance_namespace::models::UpdateTableSchemaMetadataRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "products").await;

        let mut metadata = HashMap::new();
        metadata.insert("owner".to_string(), "team_a".to_string());
        metadata.insert("version".to_string(), "1.0".to_string());

        let mut req = UpdateTableSchemaMetadataRequest::new();
        req.id = Some(vec!["products".to_string()]);
        req.metadata = Some(metadata.clone());

        let response = namespace.update_table_schema_metadata(req).await.unwrap();

        assert!(response.metadata.is_some());
        let returned = response.metadata.unwrap();
        assert_eq!(returned.get("owner"), Some(&"team_a".to_string()));
        assert_eq!(returned.get("version"), Some(&"1.0".to_string()));
        assert!(
            response.transaction_id.is_some(),
            "update_table_schema_metadata should return a transaction_id"
        );
    }

    #[tokio::test]
    async fn test_alter_table_add_columns_missing_id() {
        use lance_namespace::models::{AddColumnsEntry, AlterTableAddColumnsRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;

        let new_col = AddColumnsEntry::new("col".to_string());
        let request = AlterTableAddColumnsRequest::new(vec![new_col]);
        let result = namespace.alter_table_add_columns(request).await;
        assert!(result.is_err(), "Should fail when table ID is missing");
    }

    #[tokio::test]
    async fn test_alter_table_alter_columns_rename() {
        use lance_namespace::models::{
            AlterColumnsEntry, AlterTableAlterColumnsRequest, DescribeTableRequest,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Rename "name" to "full_name"
        let mut entry = AlterColumnsEntry::new("name".to_string());
        entry.rename = Some(Some("full_name".to_string()));
        let mut alter_request = AlterTableAlterColumnsRequest::new(vec![entry]);
        alter_request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .alter_table_alter_columns(alter_request)
            .await
            .unwrap();
        assert!(
            response.version > 1,
            "Version should increment after altering columns"
        );

        // Verify the rename
        let mut describe_request = DescribeTableRequest::new();
        describe_request.id = Some(vec!["test_table".to_string()]);
        describe_request.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_request).await.unwrap();
        assert!(describe_response.schema.is_some());

        let resp_schema = describe_response.schema.unwrap();
        let field_names: Vec<&str> = resp_schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            field_names.contains(&"full_name"),
            "Column should be renamed to 'full_name', got: {:?}",
            field_names
        );
        assert!(
            !field_names.contains(&"name"),
            "Old column 'name' should not exist, got: {:?}",
            field_names
        );
    }

    #[tokio::test]
    async fn test_get_table_stats() {
        use lance_namespace::models::GetTableStatsRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "items").await;
        create_scalar_index(&namespace, "items", "items_id_idx").await;

        let mut req = GetTableStatsRequest::new();
        req.id = Some(vec!["items".to_string()]);

        let response = namespace.get_table_stats(req).await.unwrap();
        assert_eq!(response.num_rows, 3);
        assert_eq!(response.num_indices, 1);
    }

    #[tokio::test]
    async fn test_explain_table_query_plan() {
        use lance_namespace::models::QueryTableRequestVector;
        use lance_namespace::models::{ExplainTableQueryPlanRequest, QueryTableRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "catalog").await;

        let mut query = QueryTableRequest::new(1, QueryTableRequestVector::new());
        query.filter = Some("id > 1".to_string());
        query.columns = Some(Box::new(QueryTableRequestColumns {
            column_names: Some(vec!["id".to_string(), "name".to_string()]),
            column_aliases: None,
        }));
        query.with_row_id = Some(true);

        let mut req = ExplainTableQueryPlanRequest::new(query);
        req.id = Some(vec!["catalog".to_string()]);

        let plan_str = namespace.explain_table_query_plan(req).await.unwrap();
        assert_plan_contains_all(
            &plan_str,
            &[
                "ProjectionExec: expr=[id@0 as id, name@2 as name",
                "projection=[name], source=stream(_rowid)",
                "LanceRead: uri=",
                "projection=[id]",
                "row_id=true, row_addr=false",
                "full_filter=id > Int32(1)",
                "refine_filter=id > Int32(1)",
            ],
            "Filtered explain plan should preserve late materialization and filter pushdown",
        );
    }

    #[tokio::test]
    async fn test_alter_table_alter_columns_missing_id() {
        use lance_namespace::models::{AlterColumnsEntry, AlterTableAlterColumnsRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;

        let entry = AlterColumnsEntry::new("name".to_string());
        let request = AlterTableAlterColumnsRequest::new(vec![entry]);
        let result = namespace.alter_table_alter_columns(request).await;
        assert!(result.is_err(), "Should fail when table ID is missing");
    }

    #[tokio::test]
    async fn test_alter_table_drop_columns() {
        use lance_namespace::models::{AlterTableDropColumnsRequest, DescribeTableRequest};

        let (namespace, _temp_dir) = create_test_namespace().await;

        // Create a table
        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_request = CreateTableRequest::new();
        create_request.id = Some(vec!["test_table".to_string()]);
        namespace
            .create_table(create_request, bytes::Bytes::from(ipc_data))
            .await
            .unwrap();

        // Drop the "name" column
        let mut drop_request = AlterTableDropColumnsRequest::new(vec!["name".to_string()]);
        drop_request.id = Some(vec!["test_table".to_string()]);

        let response = namespace
            .alter_table_drop_columns(drop_request)
            .await
            .unwrap();
        assert!(
            response.version > 1,
            "Version should increment after dropping columns"
        );

        // Verify column was dropped
        let mut describe_request = DescribeTableRequest::new();
        describe_request.id = Some(vec!["test_table".to_string()]);
        describe_request.load_detailed_metadata = Some(true);
        let describe_response = namespace.describe_table(describe_request).await.unwrap();
        assert!(describe_response.schema.is_some());

        let resp_schema = describe_response.schema.unwrap();
        let field_names: Vec<&str> = resp_schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            !field_names.contains(&"name"),
            "Column 'name' should be dropped, got: {:?}",
            field_names
        );
        assert!(
            field_names.contains(&"id"),
            "Column 'id' should still exist, got: {:?}",
            field_names
        );
    }

    #[tokio::test]
    async fn test_analyze_table_query_plan() {
        use lance_namespace::models::AnalyzeTableQueryPlanRequest;
        use lance_namespace::models::QueryTableRequestVector;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "catalog").await;

        let mut req = AnalyzeTableQueryPlanRequest::new(1, QueryTableRequestVector::new());
        req.id = Some(vec!["catalog".to_string()]);
        req.filter = Some("id > 0".to_string());
        req.columns = Some(Box::new(QueryTableRequestColumns {
            column_names: Some(vec!["id".to_string(), "name".to_string()]),
            column_aliases: None,
        }));
        req.with_row_id = Some(true);

        let analysis_str = namespace.analyze_table_query_plan(req).await.unwrap();
        assert_plan_contains_all(
            &analysis_str,
            &[
                "AnalyzeExec verbose=true",
                "ProjectionExec: elapsed=",
                "expr=[id@0 as id, name@2 as name",
                "projection=[name], source=stream(_rowid)",
                "LanceRead: elapsed=",
                "projection=[id]",
                "row_id=true, row_addr=false",
                "full_filter=id > Int32(0)",
                "refine_filter=id > Int32(0)",
                "metrics=[output_rows=",
            ],
            "Filtered analyze plan should preserve late materialization and filter pushdown",
        );
    }

    #[tokio::test]
    async fn test_dir_listing_no_extra_calls_without_migration() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let root_uri = file_object_store_uri(temp_path);
        let listing_count = Arc::new(AtomicUsize::new(0));
        let session = build_listing_counting_session(listing_count.clone());

        // Create a table using dir-listing-only namespace
        let dir_only_ns = DirectoryNamespaceBuilder::new(root_uri.clone())
            .session(session.clone())
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        dir_only_ns
            .create_table(create_req, Bytes::from(ipc_data))
            .await
            .unwrap();

        // Build a namespace with both enabled but migration disabled (default)
        let hybrid_ns = DirectoryNamespaceBuilder::new(root_uri)
            .session(session)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(false)
            .build()
            .await
            .unwrap();

        // Reset counter before the operation we want to measure
        listing_count.store(0, Ordering::SeqCst);

        // table_exists should use dir listing directly, making only 1 listing call
        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        hybrid_ns.table_exists(exists_req).await.unwrap();

        let count = listing_count.load(Ordering::SeqCst);
        assert_eq!(
            count, 1,
            "Expected exactly 1 listing call for table_exists \
             without migration mode, but got {}",
            count
        );

        // Reset and test describe_table
        listing_count.store(0, Ordering::SeqCst);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        hybrid_ns.describe_table(describe_req).await.unwrap();

        let count = listing_count.load(Ordering::SeqCst);
        assert_eq!(
            count, 1,
            "Expected exactly 1 listing call for describe_table \
             without migration mode, but got {}",
            count
        );
    }

    #[tokio::test]
    async fn test_build_and_root_reads_do_not_create_manifest() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let manifest_path = std::path::Path::new(temp_path).join("__manifest");

        let dir_only_ns = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        create_scalar_table(&dir_only_ns, "catalog").await;
        assert!(!manifest_path.exists());

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        assert!(!manifest_path.exists());

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["catalog".to_string()]);
        namespace.table_exists(exists_req).await.unwrap();
        assert!(!manifest_path.exists());

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["catalog".to_string()]);
        namespace.describe_table(describe_req).await.unwrap();
        assert!(!manifest_path.exists());

        let list_response = namespace
            .list_tables(ListTablesRequest {
                id: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(list_response.tables, vec!["catalog".to_string()]);
        assert!(!manifest_path.exists());

        let mut list_namespaces_req = ListNamespacesRequest::new();
        list_namespaces_req.id = Some(vec!["workspace".to_string()]);
        let err = namespace
            .list_namespaces(list_namespaces_req)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("__manifest"));
        assert!(!manifest_path.exists());

        let err = namespace
            .list_tables(ListTablesRequest {
                id: Some(vec!["workspace".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("__manifest"));
        assert!(!manifest_path.exists());

        let mut child_describe_req = DescribeTableRequest::new();
        child_describe_req.id = Some(vec!["workspace".to_string(), "catalog".to_string()]);
        let err = namespace
            .describe_table(child_describe_req)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("__manifest"));
        assert!(!manifest_path.exists());

        let mut child_exists_req = TableExistsRequest::new();
        child_exists_req.id = Some(vec!["workspace".to_string(), "catalog".to_string()]);
        let err = namespace.table_exists(child_exists_req).await.unwrap_err();
        assert!(err.to_string().contains("__manifest"));
        assert!(!manifest_path.exists());

        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["workspace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();
        assert!(manifest_path.exists());
    }

    #[tokio::test]
    async fn test_migrate_updates_read_opened_legacy_manifest() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        create_legacy_manifest_without_primary_key_metadata(temp_path).await;
        assert!(!manifest_has_primary_key_metadata(temp_path).await);

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();
        assert!(!manifest_has_primary_key_metadata(temp_path).await);

        let migrated = namespace.migrate().await.unwrap();
        assert_eq!(migrated, 0);
        assert!(manifest_has_primary_key_metadata(temp_path).await);
    }

    #[tokio::test]
    async fn test_describe_declared_table_checks_versions_only_when_requested() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let root_uri = file_object_store_uri(temp_path);
        let listing_count = Arc::new(AtomicUsize::new(0));
        let session = build_listing_counting_session(listing_count.clone());

        let namespace = DirectoryNamespaceBuilder::new(root_uri)
            .session(session)
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        let mut declare_req = DeclareTableRequest::new();
        declare_req.id = Some(vec!["test_table".to_string()]);
        namespace.declare_table(declare_req).await.unwrap();

        listing_count.store(0, Ordering::SeqCst);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();

        assert_eq!(describe_response.is_only_declared, None);
        assert_eq!(
            listing_count.load(Ordering::SeqCst),
            1,
            "Default describe_table should only list the table directory"
        );

        listing_count.store(0, Ordering::SeqCst);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        describe_req.check_declared = Some(true);
        let describe_response = namespace.describe_table(describe_req).await.unwrap();

        assert_eq!(describe_response.is_only_declared, Some(true));
        assert_eq!(
            listing_count.load(Ordering::SeqCst),
            2,
            "check_declared describe_table should list the table directory and _versions"
        );
    }

    #[tokio::test]
    async fn test_dir_listing_extra_calls_with_migration() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();
        let root_uri = file_object_store_uri(temp_path);
        let listing_count = Arc::new(AtomicUsize::new(0));
        let session = build_listing_counting_session(listing_count.clone());

        // Create a table using dir-listing-only namespace so it exists physically but is absent from __manifest.
        let dir_only_ns = DirectoryNamespaceBuilder::new(root_uri.clone())
            .session(session.clone())
            .manifest_enabled(false)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        let schema = create_test_schema();
        let ipc_data = create_test_ipc_data(&schema);
        let mut create_req = CreateTableRequest::new();
        create_req.id = Some(vec!["test_table".to_string()]);
        dir_only_ns
            .create_table(create_req, Bytes::from(ipc_data))
            .await
            .unwrap();

        let hybrid_ns = DirectoryNamespaceBuilder::new(root_uri)
            .session(session)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();

        // In migration mode the manifest is authoritative, so table_exists first
        // probes __manifest via ensure_read_manifest() to self-heal any
        // manifest-registered aliases (why the gate now admits this mode). Here
        // the table is dir-only and __manifest does not exist yet, so that probe
        // costs one list to confirm absence; the table-directory fallback is the
        // second. (When __manifest exists the probe uses the version hint and
        // adds no list, so a real self-heal is free.)
        listing_count.store(0, Ordering::SeqCst);

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["test_table".to_string()]);
        hybrid_ns.table_exists(exists_req).await.unwrap();

        let count = listing_count.load(Ordering::SeqCst);
        assert_eq!(
            count, 2,
            "Expected 2 listing calls for table_exists with migration mode \
             (absent-__manifest probe + table directory fallback), but got {}",
            count
        );

        // describe_table follows the same path: an ensure_read_manifest() probe
        // of the (absent) __manifest, then the table-directory fallback.
        listing_count.store(0, Ordering::SeqCst);

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["test_table".to_string()]);
        hybrid_ns.describe_table(describe_req).await.unwrap();

        let count = listing_count.load(Ordering::SeqCst);
        assert_eq!(
            count, 2,
            "Expected 2 listing calls for describe_table with migration mode \
             (absent-__manifest probe + table directory fallback), but got {}",
            count
        );
    }

    #[tokio::test]
    async fn test_manifest_reload_observes_new_version_from_other_namespace() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace_a = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();
        create_scalar_table(&namespace_a, "alpha").await;

        let namespace_b = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(false)
            .build()
            .await
            .unwrap();
        create_scalar_table(&namespace_b, "beta").await;

        let response = namespace_a
            .list_tables(ListTablesRequest {
                id: Some(vec![]),
                ..Default::default()
            })
            .await
            .unwrap();

        let mut tables = response.tables;
        tables.sort();
        assert_eq!(tables, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn test_migration_not_found_errors_include_table_id() {
        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .dir_listing_to_manifest_migration_enabled(true)
            .build()
            .await
            .unwrap();

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(vec!["missing_table".to_string()]);
        let err = namespace.table_exists(exists_req).await.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Table not found"));
        assert!(err_msg.contains("table id 'missing_table'"));

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(vec!["missing_table".to_string()]);
        let err = namespace.describe_table(describe_req).await.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Table not found"));
        assert!(err_msg.contains("table id 'missing_table'"));
    }

    #[tokio::test]
    async fn test_manifest_not_found_errors_include_full_table_id() {
        use lance_namespace::models::CreateNamespaceRequest;

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = DirectoryNamespaceBuilder::new(temp_path)
            .manifest_enabled(true)
            .dir_listing_enabled(true)
            .build()
            .await
            .unwrap();

        let mut create_ns_req = CreateNamespaceRequest::new();
        create_ns_req.id = Some(vec!["workspace".to_string()]);
        namespace.create_namespace(create_ns_req).await.unwrap();

        let missing_table_id = vec!["workspace".to_string(), "missing_table".to_string()];

        let mut exists_req = TableExistsRequest::new();
        exists_req.id = Some(missing_table_id.clone());
        let err = namespace.table_exists(exists_req).await.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Table not found"));
        assert!(err_msg.contains("table id 'workspace$missing_table'"));

        let mut describe_req = DescribeTableRequest::new();
        describe_req.id = Some(missing_table_id);
        let err = namespace.describe_table(describe_req).await.unwrap_err();
        assert!(matches!(err, Error::Namespace { .. }));
        let err_msg = err.to_string();
        assert!(err_msg.contains("Table not found"));
        assert!(err_msg.contains("table id 'workspace$missing_table'"));
    }

    /// Helper used by tag tests: creates a table with `versions` total versions
    /// (1 create + N-1 appends) and returns the namespace plus the table id.
    async fn create_tagged_test_table(
        versions: u32,
    ) -> (Arc<DirectoryNamespace>, TempStdDir, Vec<String>) {
        use arrow::array::{Int32Array, RecordBatchIterator};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance::dataset::{Dataset, WriteMode, WriteParams};

        assert!(versions >= 1, "versions must be at least 1");

        let temp_dir = TempStdDir::default();
        let temp_path = temp_dir.to_str().unwrap();

        let namespace = Arc::new(
            DirectoryNamespaceBuilder::new(temp_path)
                .build()
                .await
                .unwrap(),
        );
        let table_id = vec!["tag_table".to_string()];
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let initial_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(initial_batch)], arrow_schema.clone());
        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };

        let mut dataset = Dataset::write_into_namespace(
            batches,
            namespace.clone() as Arc<dyn LanceNamespace>,
            table_id.clone(),
            Some(write_params),
        )
        .await
        .unwrap();

        for i in 1..versions {
            let value_start = (i as i32) * 10;
            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from(vec![
                    value_start,
                    value_start + 1,
                ]))],
            )
            .unwrap();
            let batches = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema.clone());
            dataset.append(batches, None).await.unwrap();
        }

        (namespace, temp_dir, table_id)
    }

    /// Downcast a lance-core error to its NamespaceError code for precise assertions.
    fn namespace_code(err: &Error) -> Option<ErrorCode> {
        match err {
            Error::Namespace { source, .. } => {
                source.downcast_ref::<NamespaceError>().map(|e| e.code())
            }
            _ => None,
        }
    }

    #[tokio::test]
    async fn test_create_and_list_branches() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(3).await;

        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "dev".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "staging".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let resp = namespace
            .list_table_branches(ListTableBranchesRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            resp.branches.len(),
            2,
            "expected 2 branches, got: {:?}",
            resp.branches
        );
        assert!(resp.branches.contains_key("dev"));
        assert!(resp.branches.contains_key("staging"));
        assert!(resp.page_token.is_none());

        // Deleting one branch is reflected in a subsequent list.
        namespace
            .delete_table_branch(DeleteTableBranchRequest {
                id: Some(table_id.clone()),
                name: "dev".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let resp = namespace
            .list_table_branches(ListTableBranchesRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(resp.branches.len(), 1, "expected 1 branch after delete");
        assert!(!resp.branches.contains_key("dev"));
        assert!(resp.branches.contains_key("staging"));
    }

    #[tokio::test]
    async fn test_create_branch_from_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(3).await;

        // Fork explicitly from version 1 of main.
        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "from-v1".to_string(),
                from_version: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();

        let resp = namespace
            .list_table_branches(ListTableBranchesRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .unwrap();
        let branch = resp
            .branches
            .get("from-v1")
            .expect("forked branch should be listed");
        assert_eq!(
            branch.parent_version, 1,
            "branch should fork from version 1"
        );
        assert!(
            branch.parent_branch.is_none(),
            "a branch forked from main has no parent branch"
        );
    }

    /// Forking from a NON-main source branch must clone that branch's chain.
    /// Both chains are given a version 2 with diverged content, so a clone that
    /// wrongly resolves the version under main succeeds silently with main's
    /// data instead of erroring.
    #[tokio::test]
    async fn test_create_branch_from_other_branch() {
        use lance::dataset::builder::DatasetBuilder;

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await; // main v1: ids [1, 2, 3]
        // dev: forked at v1, one append (ids 100, 101) -> dev v2
        create_branch_with_commits(&namespace, "users", "dev", 1).await;
        // Diverge main to the same version number with different content.
        let main_ds = open_dataset(&namespace, "users").await;
        append_scalar_version(main_ds.uri(), 500).await; // main v2: + ids [500, 501]

        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(vec!["users".to_string()]),
                name: "child".to_string(),
                from_branch: Some("dev".to_string()),
                from_version: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        let child_ds = DatasetBuilder::from_uri(main_ds.uri())
            .with_branch("child", None)
            .load()
            .await
            .unwrap();
        let ids = scan_id_column(&child_ds).await;
        assert!(
            ids.contains(&100) && ids.contains(&101),
            "child must contain dev's appended rows, got: {:?}",
            ids
        );
        assert!(
            !ids.contains(&500),
            "child must not contain main's diverged rows, got: {:?}",
            ids
        );

        // The recorded metadata and the cloned data must agree on the parent.
        let listed = namespace
            .list_table_branches(ListTableBranchesRequest {
                id: Some(vec!["users".to_string()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            listed
                .branches
                .get("child")
                .unwrap()
                .parent_branch
                .as_deref(),
            Some("dev")
        );
    }

    #[tokio::test]
    async fn test_create_existing_branch_conflict() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "dev".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let err = namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id),
                name: "dev".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&err),
            Some(ErrorCode::TableBranchAlreadyExists),
            "expected TableBranchAlreadyExists, got: {}",
            err
        );
        assert!(
            err.to_string().to_lowercase().contains("already exists"),
            "expected already-exists message, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_unknown_branch() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let err = namespace
            .delete_table_branch(DeleteTableBranchRequest {
                id: Some(table_id),
                name: "does-not-exist".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&err),
            Some(ErrorCode::TableBranchNotFound),
            "expected TableBranchNotFound, got: {}",
            err
        );
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "expected not-found message, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_referenced_branch_conflict() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        // A child forked from `parent` (via from_branch) makes `parent` a referenced branch.
        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "parent".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: "child".to_string(),
                from_branch: Some("parent".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();

        // from_branch resolution: the child records its parent branch as its fork point.
        let listed = namespace
            .list_table_branches(ListTableBranchesRequest {
                id: Some(table_id.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        let child = listed
            .branches
            .get("child")
            .expect("child branch should be listed");
        assert_eq!(
            child.parent_branch.as_deref(),
            Some("parent"),
            "child should record parent branch as its fork point"
        );
        assert!(
            child.parent_version >= 1,
            "child should record the parent version it forked from, got {}",
            child.parent_version
        );

        // Deleting a branch that still has dependents is refused. The delete spec has no 409,
        // so it surfaces as a documented InvalidInput (400), not a conflict status.
        let err = namespace
            .delete_table_branch(DeleteTableBranchRequest {
                id: Some(table_id),
                name: "parent".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&err),
            Some(ErrorCode::InvalidInput),
            "expected InvalidInput for deleting a referenced branch, got: {}",
            err
        );
        assert!(
            err.to_string().to_lowercase().contains("referenced"),
            "error should explain the branch is still referenced, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_branch_name_required() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let create_err = namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id.clone()),
                name: String::new(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&create_err),
            Some(ErrorCode::InvalidInput),
            "empty name on create should be InvalidInput, got: {}",
            create_err
        );
        assert!(
            create_err
                .to_string()
                .to_lowercase()
                .contains("must not be empty")
        );

        let delete_err = namespace
            .delete_table_branch(DeleteTableBranchRequest {
                id: Some(table_id),
                name: String::new(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&delete_err),
            Some(ErrorCode::InvalidInput),
            "empty name on delete should be InvalidInput, got: {}",
            delete_err
        );
        assert!(
            delete_err
                .to_string()
                .to_lowercase()
                .contains("must not be empty")
        );
    }

    #[tokio::test]
    async fn test_create_branch_rejects_negative_from_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let err = namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id),
                name: "dev".to_string(),
                from_version: Some(-1),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&err),
            Some(ErrorCode::InvalidInput),
            "negative from_version should be InvalidInput, got: {}",
            err
        );
        assert!(err.to_string().to_lowercase().contains("from_version"));
    }

    #[tokio::test]
    async fn test_create_branch_nonexistent_from_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        // Version 999 does not exist (the table has 2 versions). create_branch's clone phase
        // raises DatasetNotFound, which we map to a documented InvalidInput (400).
        let err = namespace
            .create_table_branch(CreateTableBranchRequest {
                id: Some(table_id),
                name: "dev".to_string(),
                from_version: Some(999),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(
            namespace_code(&err),
            Some(ErrorCode::InvalidInput),
            "non-existent from_version should map to InvalidInput, got: {}",
            err
        );
        assert!(
            err.to_string().to_lowercase().contains("does not exist"),
            "error should name the missing source, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_create_and_list_tags() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(3).await;

        let mut req = CreateTableTagRequest::new("v1".to_string(), 1);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut req = CreateTableTagRequest::new("v2".to_string(), 2);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut list_req = ListTableTagsRequest::new();
        list_req.id = Some(table_id);
        let resp = namespace.list_table_tags(list_req).await.unwrap();

        assert_eq!(resp.tags.len(), 2, "expected 2 tags, got: {:?}", resp.tags);
        assert_eq!(resp.tags.get("v1").unwrap().version, 1);
        assert_eq!(resp.tags.get("v2").unwrap().version, 2);
        assert!(resp.page_token.is_none());
    }

    #[tokio::test]
    async fn test_create_existing_tag_conflict() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let mut req = CreateTableTagRequest::new("v1".to_string(), 1);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut req = CreateTableTagRequest::new("v1".to_string(), 2);
        req.id = Some(table_id);
        let err = namespace.create_table_tag(req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("already exists"),
            "expected already-exists error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_get_tag_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(3).await;

        let mut req = CreateTableTagRequest::new("release".to_string(), 2);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut get_req = GetTableTagVersionRequest::new("release".to_string());
        get_req.id = Some(table_id);
        let resp = namespace.get_table_tag_version(get_req).await.unwrap();
        assert_eq!(resp.version, 2);
        assert_eq!(resp.branch, None);
    }

    #[tokio::test]
    async fn test_get_unknown_tag() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let mut get_req = GetTableTagVersionRequest::new("does-not-exist".to_string());
        get_req.id = Some(table_id);
        let err = namespace.get_table_tag_version(get_req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "expected not-found error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_update_tag_to_new_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(3).await;

        let mut req = CreateTableTagRequest::new("rolling".to_string(), 1);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut update_req = UpdateTableTagRequest::new("rolling".to_string(), 3);
        update_req.id = Some(table_id.clone());
        namespace.update_table_tag(update_req).await.unwrap();

        let mut get_req = GetTableTagVersionRequest::new("rolling".to_string());
        get_req.id = Some(table_id);
        let resp = namespace.get_table_tag_version(get_req).await.unwrap();
        assert_eq!(resp.version, 3);
    }

    #[tokio::test]
    async fn test_update_unknown_tag() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let mut update_req = UpdateTableTagRequest::new("ghost".to_string(), 1);
        update_req.id = Some(table_id);
        let err = namespace.update_table_tag(update_req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "expected not-found error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_delete_tag() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let mut req = CreateTableTagRequest::new("doomed".to_string(), 1);
        req.id = Some(table_id.clone());
        namespace.create_table_tag(req).await.unwrap();

        let mut delete_req = DeleteTableTagRequest::new("doomed".to_string());
        delete_req.id = Some(table_id.clone());
        namespace.delete_table_tag(delete_req).await.unwrap();

        let mut list_req = ListTableTagsRequest::new();
        list_req.id = Some(table_id.clone());
        let resp = namespace.list_table_tags(list_req).await.unwrap();
        assert!(resp.tags.is_empty(), "tag should be removed after delete");

        // A second get should return NotFound.
        let mut get_req = GetTableTagVersionRequest::new("doomed".to_string());
        get_req.id = Some(table_id);
        let err = namespace.get_table_tag_version(get_req).await.unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not found"));
    }

    #[tokio::test]
    async fn test_delete_unknown_tag() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        let mut delete_req = DeleteTableTagRequest::new("nope".to_string());
        delete_req.id = Some(table_id);
        let err = namespace.delete_table_tag(delete_req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("not found"),
            "expected not-found error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_create_tag_invalid_version() {
        let (namespace, _temp_dir, table_id) = create_tagged_test_table(2).await;

        // version 0 should be rejected as InvalidInput before reaching the dataset.
        let mut req = CreateTableTagRequest::new("v0".to_string(), 0);
        req.id = Some(table_id.clone());
        let err = namespace.create_table_tag(req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("positive"),
            "expected positive-version error, got: {}",
            err
        );

        // empty tag name should also be rejected.
        let mut req = CreateTableTagRequest::new(String::new(), 1);
        req.id = Some(table_id);
        let err = namespace.create_table_tag(req).await.unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("must not be empty"),
            "expected empty-tag-name error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_create_tag_table_not_found() {
        let (namespace, _temp_dir) = create_test_namespace().await;

        let mut req = CreateTableTagRequest::new("v1".to_string(), 1);
        req.id = Some(vec!["does_not_exist".to_string()]);
        let err = namespace.create_table_tag(req).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Table") && msg.to_lowercase().contains("not found"),
            "expected TableNotFound error, got: {}",
            err
        );
    }
    #[tokio::test]
    async fn test_alter_table_drop_columns_missing_id() {
        use lance_namespace::models::AlterTableDropColumnsRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;

        let request = AlterTableDropColumnsRequest::new(vec!["col".to_string()]);
        let result = namespace.alter_table_drop_columns(request).await;
        assert!(result.is_err(), "Should fail when table ID is missing");
    }

    #[tokio::test]
    async fn test_alter_table_drop_columns_nonexistent_table() {
        use lance_namespace::models::AlterTableDropColumnsRequest;

        let (namespace, _temp_dir) = create_test_namespace().await;

        let mut request = AlterTableDropColumnsRequest::new(vec!["col".to_string()]);
        request.id = Some(vec!["nonexistent".to_string()]);
        let result = namespace.alter_table_drop_columns(request).await;
        assert!(result.is_err(), "Should fail when table does not exist");
    }

    #[tokio::test]
    async fn test_create_branch_on_managed_dataset_succeeds() {
        use lance::dataset::builder::DatasetBuilder;

        let temp = TempStdDir::default();
        let ns = create_managed_namespace(temp.to_str().unwrap()).await;
        let table_id = vec!["t".to_string()];
        let mut main = create_managed_table(&ns, &table_id).await;

        let fork_version = main.version().version;
        let branch = main
            .create_branch("exp", fork_version, None)
            .await
            .expect("create_branch failed");
        assert_eq!(branch.manifest.branch.as_deref(), Some("exp"));
        assert_eq!(scan_id_column(&branch).await, vec![1, 2]);

        let reopened = DatasetBuilder::from_namespace(ns.clone(), table_id.clone())
            .await
            .unwrap()
            .with_branch("exp", None)
            .load()
            .await
            .expect("reopen branch failed");
        assert_eq!(scan_id_column(&reopened).await, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_alter_transaction_set_status() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetStatus,
            DescribeTransactionRequest,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let txn_id = create_scalar_index(&namespace, "users", "users_id_idx")
            .await
            .expect("create_scalar_index should return a transaction id");

        // First verify the transaction exists
        let describe_resp = namespace
            .describe_transaction(DescribeTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(describe_resp.status, "SUCCEEDED");

        // Alter the transaction status
        let response = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: Some(Box::new(AlterTransactionSetStatus {
                        status: Some("Canceled".to_string()),
                    })),
                    set_property_action: None,
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(response.status, "Canceled");
        assert!(response.properties.is_some());
        let props = response.properties.unwrap();
        assert_eq!(props.get("uuid"), Some(&txn_id));
        assert_eq!(props.get("operation"), Some(&"CreateIndex".to_string()));
    }

    #[tokio::test]
    async fn test_alter_transaction_set_property() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetProperty,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let txn_id = create_scalar_index(&namespace, "users", "users_id_idx")
            .await
            .expect("create_scalar_index should return a transaction id");

        let response = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: None,
                    set_property_action: Some(Box::new(AlterTransactionSetProperty {
                        key: Some("custom_key".to_string()),
                        value: Some("custom_value".to_string()),
                        mode: None,
                    })),
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(response.status, "SUCCEEDED");
        let props = response.properties.unwrap();
        assert_eq!(props.get("custom_key"), Some(&"custom_value".to_string()));
    }

    #[tokio::test]
    async fn test_alter_transaction_set_property_fail_mode() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetProperty,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let txn_id = create_scalar_index(&namespace, "users", "users_id_idx")
            .await
            .expect("create_scalar_index should return a transaction id");

        // First, set a non-reserved property so it exists in the sidecar.
        namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: None,
                    set_property_action: Some(Box::new(AlterTransactionSetProperty {
                        key: Some("custom_key".to_string()),
                        value: Some("initial_value".to_string()),
                        mode: None,
                    })),
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await
            .unwrap();

        // Now try to set the same property again with Fail mode, which must
        // exercise the mode='Fail' branch (not the reserved-key guard).
        let result = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: None,
                    set_property_action: Some(Box::new(AlterTransactionSetProperty {
                        key: Some("custom_key".to_string()),
                        value: Some("new_value".to_string()),
                        mode: Some("Fail".to_string()),
                    })),
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_alter_transaction_unset_property() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetProperty,
            AlterTransactionUnsetProperty,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let txn_id = create_scalar_index(&namespace, "users", "users_id_idx")
            .await
            .expect("create_scalar_index should return a transaction id");

        // First set a custom property, then unset it
        let response = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![
                    AlterTransactionAction {
                        set_status_action: None,
                        set_property_action: Some(Box::new(AlterTransactionSetProperty {
                            key: Some("temp_key".to_string()),
                            value: Some("temp_value".to_string()),
                            mode: None,
                        })),
                        unset_property_action: None,
                    },
                    AlterTransactionAction {
                        set_status_action: None,
                        set_property_action: None,
                        unset_property_action: Some(Box::new(AlterTransactionUnsetProperty {
                            key: Some("temp_key".to_string()),
                            mode: None,
                        })),
                    },
                ],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(response.status, "SUCCEEDED");
        let props = response.properties.unwrap();
        assert!(!props.contains_key("temp_key"));
    }

    #[tokio::test]
    async fn test_alter_transaction_invalid_status() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetStatus,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let txn_id = create_scalar_index(&namespace, "users", "users_id_idx")
            .await
            .expect("create_scalar_index should return a transaction id");

        let result = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: Some(Box::new(AlterTransactionSetStatus {
                        status: Some("InvalidStatus".to_string()),
                    })),
                    set_property_action: None,
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_alter_transaction_not_found() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetStatus,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;

        // Try to alter a non-existent transaction
        let result = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), "non_existent_txn".to_string()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: Some(Box::new(AlterTransactionSetStatus {
                        status: Some("Canceled".to_string()),
                    })),
                    set_property_action: None,
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_alter_transaction_missing_id() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetStatus,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;

        // Try with missing id
        let result = namespace
            .alter_transaction(AlterTransactionRequest {
                id: None,
                actions: vec![AlterTransactionAction {
                    set_status_action: Some(Box::new(AlterTransactionSetStatus {
                        status: Some("Canceled".to_string()),
                    })),
                    set_property_action: None,
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await;
        assert!(result.is_err());

        // Try with insufficient id parts
        let result = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string()]),
                actions: vec![AlterTransactionAction {
                    set_status_action: Some(Box::new(AlterTransactionSetStatus {
                        status: Some("Canceled".to_string()),
                    })),
                    set_property_action: None,
                    unset_property_action: None,
                }],
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_alter_transaction_persists_changes() {
        use lance_namespace::models::{
            AlterTransactionAction, AlterTransactionRequest, AlterTransactionSetProperty,
            AlterTransactionSetStatus, DescribeTransactionRequest,
        };

        let (namespace, _temp_dir) = create_test_namespace().await;
        create_scalar_table(&namespace, "users").await;
        let transaction_id = create_scalar_index(&namespace, "users", "users_id_idx").await;

        let txn_id = transaction_id.expect("scalar index should produce a transaction id");

        // Alter status and set a custom property.
        namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![
                    AlterTransactionAction {
                        set_status_action: Some(Box::new(AlterTransactionSetStatus {
                            status: Some("Canceled".to_string()),
                        })),
                        set_property_action: None,
                        unset_property_action: None,
                    },
                    AlterTransactionAction {
                        set_status_action: None,
                        set_property_action: Some(Box::new(AlterTransactionSetProperty {
                            key: Some("owner".to_string()),
                            value: Some("alice".to_string()),
                            mode: None,
                        })),
                        unset_property_action: None,
                    },
                ],
                ..Default::default()
            })
            .await
            .unwrap();

        // The changes must survive across a fresh describe_transaction call,
        // proving the alteration was persisted to the transaction file.
        let describe_resp = namespace
            .describe_transaction(DescribeTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                ..Default::default()
            })
            .await
            .unwrap();
        let props = describe_resp.properties.expect("properties should be set");
        assert_eq!(props.get("owner"), Some(&"alice".to_string()));
        // The internal `_status` marker should not leak into the response but
        // must be present on disk so subsequent alter_transaction calls can
        // observe the previously set status.
        assert!(!props.contains_key("_status"));

        let follow_up = namespace
            .alter_transaction(AlterTransactionRequest {
                id: Some(vec!["users".to_string(), txn_id.clone()]),
                actions: vec![],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(follow_up.status, "Canceled");
        let follow_up_props = follow_up.properties.unwrap();
        assert_eq!(follow_up_props.get("owner"), Some(&"alice".to_string()));
    }
}
