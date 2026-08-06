// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Namespace base interface and implementations.

use async_trait::async_trait;
use bytes::Bytes;
use lance_core::{Error, Result};

use lance_namespace_reqwest_client::models::{
    AlterTableAddColumnsRequest, AlterTableAddColumnsResponse, AlterTableAlterColumnsRequest,
    AlterTableAlterColumnsResponse, AlterTableBackfillColumnsRequest,
    AlterTableBackfillColumnsResponse, AlterTableDropColumnsRequest, AlterTableDropColumnsResponse,
    AlterTransactionRequest, AlterTransactionResponse, AnalyzeTableQueryPlanRequest,
    BatchDeleteTableVersionsRequest, BatchDeleteTableVersionsResponse, CountTableRowsRequest,
    CreateMaterializedViewRequest, CreateMaterializedViewResponse, CreateNamespaceRequest,
    CreateNamespaceResponse, CreateTableBranchRequest, CreateTableBranchResponse,
    CreateTableIndexRequest, CreateTableIndexResponse, CreateTableRequest, CreateTableResponse,
    CreateTableScalarIndexResponse, CreateTableTagRequest, CreateTableTagResponse,
    CreateTableVersionRequest, CreateTableVersionResponse, DeclareTableRequest,
    DeclareTableResponse, DeleteFromTableRequest, DeleteFromTableResponse,
    DeleteTableBranchRequest, DeleteTableBranchResponse, DeleteTableTagRequest,
    DeleteTableTagResponse, DeregisterTableRequest, DeregisterTableResponse,
    DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableIndexStatsRequest,
    DescribeTableIndexStatsResponse, DescribeTableRequest, DescribeTableResponse,
    DescribeTableVersionRequest, DescribeTableVersionResponse, DescribeTransactionRequest,
    DescribeTransactionResponse, DropNamespaceRequest, DropNamespaceResponse,
    DropTableIndexRequest, DropTableIndexResponse, DropTableRequest, DropTableResponse,
    ExplainTableQueryPlanRequest, GetTableStatsRequest, GetTableStatsResponse,
    GetTableTagVersionRequest, GetTableTagVersionResponse, InsertIntoTableRequest,
    InsertIntoTableResponse, ListNamespacesRequest, ListNamespacesResponse,
    ListTableBranchesRequest, ListTableBranchesResponse, ListTableIndicesRequest,
    ListTableIndicesResponse, ListTableTagsRequest, ListTableTagsResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, ListTablesRequest, ListTablesResponse,
    MergeInsertIntoTableRequest, MergeInsertIntoTableResponse, NamespaceExistsRequest,
    QueryTableRequest, RefreshMaterializedViewRequest, RefreshMaterializedViewResponse,
    RegisterTableRequest, RegisterTableResponse, RenameTableRequest, RenameTableResponse,
    RestoreTableRequest, RestoreTableResponse, TableExistsRequest, UpdateTableRequest,
    UpdateTableResponse, UpdateTableSchemaMetadataRequest, UpdateTableSchemaMetadataResponse,
    UpdateTableTagRequest, UpdateTableTagResponse,
};

/// Base trait for Lance Namespace implementations.
///
/// This trait defines the interface that all Lance namespace implementations
/// must provide. Each method corresponds to a specific operation on namespaces
/// or tables.
///
/// # Error Handling
///
/// All operations may return the following common errors (via [`crate::NamespaceError`]):
///
/// - [`crate::ErrorCode::Unsupported`] - Operation not supported by this backend
/// - [`crate::ErrorCode::InvalidInput`] - Invalid request parameters
/// - [`crate::ErrorCode::PermissionDenied`] - Insufficient permissions
/// - [`crate::ErrorCode::Unauthenticated`] - Invalid credentials
/// - [`crate::ErrorCode::ServiceUnavailable`] - Service temporarily unavailable
/// - [`crate::ErrorCode::Internal`] - Unexpected internal error
///
/// See individual method documentation for operation-specific errors.
#[async_trait]
pub trait LanceNamespace: Send + Sync + std::fmt::Debug {
    /// List namespaces.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorCode::NamespaceNotFound`] if the parent namespace does not exist.
    async fn list_namespaces(
        &self,
        _request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        Err(Error::not_supported("list_namespaces not implemented"))
    }

    /// Describe a namespace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorCode::NamespaceNotFound`] if the namespace does not exist.
    async fn describe_namespace(
        &self,
        _request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        Err(Error::not_supported("describe_namespace not implemented"))
    }

    /// Create a new namespace.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorCode::NamespaceAlreadyExists`] if a namespace with the same name already exists.
    async fn create_namespace(
        &self,
        _request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        Err(Error::not_supported("create_namespace not implemented"))
    }

    /// Drop a namespace.
    ///
    /// # Errors
    ///
    /// - [`crate::ErrorCode::NamespaceNotFound`] if the namespace does not exist.
    /// - [`crate::ErrorCode::NamespaceNotEmpty`] if the namespace contains tables or child namespaces.
    async fn drop_namespace(
        &self,
        _request: DropNamespaceRequest,
    ) -> Result<DropNamespaceResponse> {
        Err(Error::not_supported("drop_namespace not implemented"))
    }

    /// Check if a namespace exists.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorCode::NamespaceNotFound`] if the namespace does not exist.
    async fn namespace_exists(&self, _request: NamespaceExistsRequest) -> Result<()> {
        Err(Error::not_supported("namespace_exists not implemented"))
    }

    /// List tables in a namespace.
    async fn list_tables(&self, _request: ListTablesRequest) -> Result<ListTablesResponse> {
        Err(Error::not_supported("list_tables not implemented"))
    }

    /// Describe a table.
    async fn describe_table(
        &self,
        _request: DescribeTableRequest,
    ) -> Result<DescribeTableResponse> {
        Err(Error::not_supported("describe_table not implemented"))
    }

    /// Register a table.
    async fn register_table(
        &self,
        _request: RegisterTableRequest,
    ) -> Result<RegisterTableResponse> {
        Err(Error::not_supported("register_table not implemented"))
    }

    /// Check if a table exists.
    async fn table_exists(&self, _request: TableExistsRequest) -> Result<()> {
        Err(Error::not_supported("table_exists not implemented"))
    }

    /// Drop a table.
    async fn drop_table(&self, _request: DropTableRequest) -> Result<DropTableResponse> {
        Err(Error::not_supported("drop_table not implemented"))
    }

    /// Deregister a table.
    async fn deregister_table(
        &self,
        _request: DeregisterTableRequest,
    ) -> Result<DeregisterTableResponse> {
        Err(Error::not_supported("deregister_table not implemented"))
    }

    /// Count rows in a table.
    async fn count_table_rows(&self, _request: CountTableRowsRequest) -> Result<i64> {
        Err(Error::not_supported("count_table_rows not implemented"))
    }

    /// Create a new table with data from Arrow IPC stream.
    async fn create_table(
        &self,
        _request: CreateTableRequest,
        _request_data: Bytes,
    ) -> Result<CreateTableResponse> {
        Err(Error::not_supported("create_table not implemented"))
    }

    /// Declare a table (metadata only operation).
    async fn declare_table(&self, _request: DeclareTableRequest) -> Result<DeclareTableResponse> {
        Err(Error::not_supported("declare_table not implemented"))
    }

    /// Insert data into a table.
    async fn insert_into_table(
        &self,
        _request: InsertIntoTableRequest,
        _request_data: Bytes,
    ) -> Result<InsertIntoTableResponse> {
        Err(Error::not_supported("insert_into_table not implemented"))
    }

    /// Merge insert data into a table.
    async fn merge_insert_into_table(
        &self,
        _request: MergeInsertIntoTableRequest,
        _request_data: Bytes,
    ) -> Result<MergeInsertIntoTableResponse> {
        Err(Error::not_supported(
            "merge_insert_into_table not implemented",
        ))
    }

    /// Update a table.
    async fn update_table(&self, _request: UpdateTableRequest) -> Result<UpdateTableResponse> {
        Err(Error::not_supported("update_table not implemented"))
    }

    /// Delete from a table.
    async fn delete_from_table(
        &self,
        _request: DeleteFromTableRequest,
    ) -> Result<DeleteFromTableResponse> {
        Err(Error::not_supported("delete_from_table not implemented"))
    }

    /// Query a table.
    async fn query_table(&self, _request: QueryTableRequest) -> Result<Bytes> {
        Err(Error::not_supported("query_table not implemented"))
    }

    /// Create a table index.
    async fn create_table_index(
        &self,
        _request: CreateTableIndexRequest,
    ) -> Result<CreateTableIndexResponse> {
        Err(Error::not_supported("create_table_index not implemented"))
    }

    /// List table indices.
    async fn list_table_indices(
        &self,
        _request: ListTableIndicesRequest,
    ) -> Result<ListTableIndicesResponse> {
        Err(Error::not_supported("list_table_indices not implemented"))
    }

    /// Describe table index statistics.
    async fn describe_table_index_stats(
        &self,
        _request: DescribeTableIndexStatsRequest,
    ) -> Result<DescribeTableIndexStatsResponse> {
        Err(Error::not_supported(
            "describe_table_index_stats not implemented",
        ))
    }

    /// Describe a transaction.
    async fn describe_transaction(
        &self,
        _request: DescribeTransactionRequest,
    ) -> Result<DescribeTransactionResponse> {
        Err(Error::not_supported("describe_transaction not implemented"))
    }

    /// Alter a transaction.
    async fn alter_transaction(
        &self,
        _request: AlterTransactionRequest,
    ) -> Result<AlterTransactionResponse> {
        Err(Error::not_supported("alter_transaction not implemented"))
    }

    /// Create a scalar index on a table.
    async fn create_table_scalar_index(
        &self,
        _request: CreateTableIndexRequest,
    ) -> Result<CreateTableScalarIndexResponse> {
        Err(Error::not_supported(
            "create_table_scalar_index not implemented",
        ))
    }

    /// Drop a table index.
    async fn drop_table_index(
        &self,
        _request: DropTableIndexRequest,
    ) -> Result<DropTableIndexResponse> {
        Err(Error::not_supported("drop_table_index not implemented"))
    }

    /// List all tables across all namespaces.
    async fn list_all_tables(&self, _request: ListTablesRequest) -> Result<ListTablesResponse> {
        Err(Error::not_supported("list_all_tables not implemented"))
    }

    /// Restore a table to a specific version.
    async fn restore_table(&self, _request: RestoreTableRequest) -> Result<RestoreTableResponse> {
        Err(Error::not_supported("restore_table not implemented"))
    }

    /// Rename a table.
    async fn rename_table(&self, _request: RenameTableRequest) -> Result<RenameTableResponse> {
        Err(Error::not_supported("rename_table not implemented"))
    }

    /// List all versions of a table.
    async fn list_table_versions(
        &self,
        _request: ListTableVersionsRequest,
    ) -> Result<ListTableVersionsResponse> {
        Err(Error::not_supported("list_table_versions not implemented"))
    }

    /// Create a new table version entry.
    ///
    /// This operation supports `put_if_not_exists` semantics, where the operation
    /// fails if the version already exists. This is used to coordinate concurrent
    /// writes to a table through an external manifest store.
    ///
    /// # Arguments
    ///
    /// * `request` - Contains the table identifier, version number, manifest path,
    ///   and optional metadata like size and ETag.
    ///
    /// # Errors
    ///
    /// - Returns an error if the version already exists (conflict).
    /// - Returns [`crate::ErrorCode::TableNotFound`] if the table does not exist.
    async fn create_table_version(
        &self,
        _request: CreateTableVersionRequest,
    ) -> Result<CreateTableVersionResponse> {
        Err(Error::not_supported("create_table_version not implemented"))
    }

    /// Describe a specific table version.
    ///
    /// Returns metadata about a specific version of a table, including the
    /// manifest path, size, ETag, and timestamp.
    ///
    /// # Arguments
    ///
    /// * `request` - Contains the table identifier and optionally the version
    ///   number. If version is not specified, returns the latest version.
    ///
    /// # Errors
    ///
    /// - Returns [`crate::ErrorCode::TableNotFound`] if the table does not exist.
    /// - Returns an error if the specified version does not exist.
    async fn describe_table_version(
        &self,
        _request: DescribeTableVersionRequest,
    ) -> Result<DescribeTableVersionResponse> {
        Err(Error::not_supported(
            "describe_table_version not implemented",
        ))
    }

    /// Batch delete table versions.
    ///
    /// Deletes version records for a single table using `request.id` + `request.ranges`.
    ///
    /// # Arguments
    ///
    /// * `request` - Contains the table identifier and version ranges to delete.
    ///
    /// # Errors
    ///
    /// - Returns [`crate::ErrorCode::TableNotFound`] if the table does not exist.
    async fn batch_delete_table_versions(
        &self,
        _request: BatchDeleteTableVersionsRequest,
    ) -> Result<BatchDeleteTableVersionsResponse> {
        Err(Error::not_supported(
            "batch_delete_table_versions not implemented",
        ))
    }

    /// Update table schema metadata.
    async fn update_table_schema_metadata(
        &self,
        _request: UpdateTableSchemaMetadataRequest,
    ) -> Result<UpdateTableSchemaMetadataResponse> {
        Err(Error::not_supported(
            "update_table_schema_metadata not implemented",
        ))
    }

    /// Get table statistics.
    async fn get_table_stats(
        &self,
        _request: GetTableStatsRequest,
    ) -> Result<GetTableStatsResponse> {
        Err(Error::not_supported("get_table_stats not implemented"))
    }

    /// Explain a table query plan.
    async fn explain_table_query_plan(
        &self,
        _request: ExplainTableQueryPlanRequest,
    ) -> Result<String> {
        Err(Error::not_supported(
            "explain_table_query_plan not implemented",
        ))
    }

    /// Analyze a table query plan.
    async fn analyze_table_query_plan(
        &self,
        _request: AnalyzeTableQueryPlanRequest,
    ) -> Result<String> {
        Err(Error::not_supported(
            "analyze_table_query_plan not implemented",
        ))
    }

    /// Add columns to a table.
    async fn alter_table_add_columns(
        &self,
        _request: AlterTableAddColumnsRequest,
    ) -> Result<AlterTableAddColumnsResponse> {
        Err(Error::not_supported(
            "alter_table_add_columns not implemented",
        ))
    }

    /// Alter columns in a table.
    async fn alter_table_alter_columns(
        &self,
        _request: AlterTableAlterColumnsRequest,
    ) -> Result<AlterTableAlterColumnsResponse> {
        Err(Error::not_supported(
            "alter_table_alter_columns not implemented",
        ))
    }

    /// Drop columns from a table.
    async fn alter_table_drop_columns(
        &self,
        _request: AlterTableDropColumnsRequest,
    ) -> Result<AlterTableDropColumnsResponse> {
        Err(Error::not_supported(
            "alter_table_drop_columns not implemented",
        ))
    }

    /// Trigger an async backfill job for a computed column.
    async fn alter_table_backfill_columns(
        &self,
        _request: AlterTableBackfillColumnsRequest,
    ) -> Result<AlterTableBackfillColumnsResponse> {
        Err(Error::not_supported(
            "alter_table_backfill_columns not implemented",
        ))
    }

    /// Trigger an async materialized view refresh.
    async fn refresh_materialized_view(
        &self,
        _request: RefreshMaterializedViewRequest,
    ) -> Result<RefreshMaterializedViewResponse> {
        Err(Error::not_supported(
            "refresh_materialized_view not implemented",
        ))
    }

    /// Create a materialized view (query / UDTF / chunker) backed by a
    /// stored UDTF/chunker spec and an optional initial refresh.
    async fn create_materialized_view(
        &self,
        _request: CreateMaterializedViewRequest,
    ) -> Result<CreateMaterializedViewResponse> {
        Err(Error::not_supported(
            "create_materialized_view not implemented",
        ))
    }

    /// List all tags for a table.
    async fn list_table_tags(
        &self,
        _request: ListTableTagsRequest,
    ) -> Result<ListTableTagsResponse> {
        Err(Error::not_supported("list_table_tags not implemented"))
    }

    /// Get the version for a specific tag.
    async fn get_table_tag_version(
        &self,
        _request: GetTableTagVersionRequest,
    ) -> Result<GetTableTagVersionResponse> {
        Err(Error::not_supported(
            "get_table_tag_version not implemented",
        ))
    }

    /// Create a tag for a table.
    async fn create_table_tag(
        &self,
        _request: CreateTableTagRequest,
    ) -> Result<CreateTableTagResponse> {
        Err(Error::not_supported("create_table_tag not implemented"))
    }

    /// Delete a tag from a table.
    async fn delete_table_tag(
        &self,
        _request: DeleteTableTagRequest,
    ) -> Result<DeleteTableTagResponse> {
        Err(Error::not_supported("delete_table_tag not implemented"))
    }

    /// Update a tag for a table.
    async fn update_table_tag(
        &self,
        _request: UpdateTableTagRequest,
    ) -> Result<UpdateTableTagResponse> {
        Err(Error::not_supported("update_table_tag not implemented"))
    }

    /// Create a branch for a table.
    ///
    /// The new branch forks from the source ref selected by `from_branch` and
    /// `from_version`, defaulting to the latest version of the main branch when
    /// both are omitted.
    ///
    /// # Errors
    ///
    /// - Returns [`crate::ErrorCode::TableBranchAlreadyExists`] if a branch with the same name already exists.
    /// - Returns [`crate::ErrorCode::TableNotFound`] if the table does not exist.
    /// - Returns [`crate::ErrorCode::InvalidInput`] if `from_branch` or `from_version` references a source that does not exist.
    async fn create_table_branch(
        &self,
        _request: CreateTableBranchRequest,
    ) -> Result<CreateTableBranchResponse> {
        Err(Error::not_supported("create_table_branch not implemented"))
    }

    /// List all branches for a table.
    async fn list_table_branches(
        &self,
        _request: ListTableBranchesRequest,
    ) -> Result<ListTableBranchesResponse> {
        Err(Error::not_supported("list_table_branches not implemented"))
    }

    /// Delete a branch from a table.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorCode::TableBranchNotFound`] if the branch does not exist.
    async fn delete_table_branch(
        &self,
        _request: DeleteTableBranchRequest,
    ) -> Result<DeleteTableBranchResponse> {
        Err(Error::not_supported("delete_table_branch not implemented"))
    }

    /// Return a human-readable unique identifier for this namespace instance.
    ///
    /// This is used for equality comparison and hashing when the namespace is
    /// used as part of a storage options provider. Two namespace instances with
    /// the same ID are considered equal and will share cached resources.
    ///
    /// The ID should be human-readable for debugging and logging purposes.
    /// For example:
    /// - REST namespace: `"rest(endpoint=https://api.example.com)"`
    /// - Directory namespace: `"dir(root=/path/to/data)"`
    ///
    /// Implementations should include all configuration that uniquely identifies
    /// the namespace to provide semantic equality.
    fn namespace_id(&self) -> String;
}
