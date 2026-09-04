// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace-based database implementation that delegates table management to lance-namespace

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lance::io::commit::namespace_manifest::LanceNamespaceExternalManifestStore;
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use lance_namespace::{
    LanceNamespace,
    error::{ErrorCode, NamespaceError},
    models::{
        CreateNamespaceRequest, CreateNamespaceResponse, DeclareTableRequest,
        DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableRequest,
        DropNamespaceRequest, DropNamespaceResponse, DropTableRequest, ListNamespacesRequest,
        ListNamespacesResponse, ListTablesRequest, ListTablesResponse, RenameTableRequest,
    },
};
use lance_namespace_impls::ConnectBuilder;
use lance_table::io::commit::CommitHandler;
use lance_table::io::commit::external_manifest::ExternalManifestCommitHandler;

use crate::blob::ensure_blob_storage_version;
use crate::connection::NamespaceClientPushdownOperation;
use crate::database::ReadConsistency;
use crate::database::listing::{
    LANCE_FILE_EXTENSION, NewTableConfig, take_request_creation_overrides,
};
use crate::database::read_freshness::{
    FreshnessBaselines, ReadFreshnessContextProvider, TableFreshness,
};
use crate::error::{Error, Result};
use crate::table::{NativeTable, map_namespace_lance_error};
use lance::dataset::WriteMode;

use super::{
    BaseTable, CloneTableRequest, CreateTableMode, CreateTableRequest as DbCreateTableRequest,
    Database, OpenTableRequest, RepairDatabaseResponse, TableNamesRequest,
};

/// Returns true if the given `lance::Error` (anywhere in its source chain) is a
/// `NamespaceError::TableAlreadyExists`.
fn is_table_already_exists_namespace_error(err: &lance::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(ns_err) = e.downcast_ref::<NamespaceError>() {
            return ns_err.code() == ErrorCode::TableAlreadyExists;
        }
        current = e.source();
    }
    false
}

/// Object-id delimiter default (matches `RestNamespaceBuilder`'s); overridable
/// via the `delimiter` property.
const DEFAULT_NAMESPACE_DELIMITER: &str = "$";

/// A database implementation that uses lance-namespace for table management
pub struct LanceNamespaceDatabase {
    namespace: Arc<dyn LanceNamespace>,
    // Storage options to be inherited by tables
    storage_options: HashMap<String, String>,
    // Read consistency interval for tables
    read_consistency_interval: Option<std::time::Duration>,
    // Optional session for object stores and caching
    session: Option<Arc<lance::session::Session>>,
    // database URI
    uri: String,
    // Operations to push down to the namespace server
    pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
    // Namespace implementation type (e.g., "dir", "rest")
    ns_impl: String,
    // Namespace properties used to construct the namespace client
    ns_properties: HashMap<String, String>,
    // Options for tables created by this connection
    new_table_config: NewTableConfig,
    // Per-table read-freshness baselines, shared with the context provider.
    freshness_baselines: FreshnessBaselines,
    // Delimiter for building freshness keys; see `table_freshness`.
    delimiter: String,
}

fn resolve_delimiter(ns_properties: &HashMap<String, String>) -> String {
    ns_properties
        .get("delimiter")
        .cloned()
        .unwrap_or_else(|| DEFAULT_NAMESPACE_DELIMITER.to_string())
}

impl LanceNamespaceDatabase {
    pub fn from_namespace_client(
        namespace_client: Arc<dyn LanceNamespace>,
        namespace_client_impl: String,
        namespace_client_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
        namespace_client_pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
    ) -> Self {
        // Client is pre-built, so we can't install the freshness provider here;
        // baselines are still tracked for a uniform bump path.
        let delimiter = resolve_delimiter(&namespace_client_properties);
        Self {
            namespace: namespace_client,
            storage_options,
            read_consistency_interval,
            session,
            uri: format!("namespace://{}", namespace_client_impl),
            pushdown_operations: namespace_client_pushdown_operations,
            ns_impl: namespace_client_impl,
            ns_properties: namespace_client_properties,
            new_table_config: NewTableConfig::default(),
            freshness_baselines: Arc::new(Mutex::new(HashMap::new())),
            delimiter,
        }
    }

    pub(crate) fn with_uri(mut self, uri: impl Into<String>) -> Self {
        self.uri = uri.into();
        self
    }

    pub async fn connect(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
    ) -> Result<Self> {
        Self::connect_with_new_table_config(
            ns_impl,
            ns_properties,
            storage_options,
            read_consistency_interval,
            session,
            pushdown_operations,
            NewTableConfig::default(),
        )
        .await
    }

    pub(crate) async fn connect_with_new_table_config(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
        new_table_config: NewTableConfig,
    ) -> Result<Self> {
        let mut builder = ConnectBuilder::new(ns_impl);
        for (key, value) in ns_properties.clone() {
            builder = builder.property(key, value);
        }
        if let Some(ref sess) = session {
            builder = builder.session(sess.clone());
        }

        // Install the read-freshness provider before building the client.
        let freshness_baselines: FreshnessBaselines = Arc::new(Mutex::new(HashMap::new()));
        builder = builder.context_provider(Arc::new(ReadFreshnessContextProvider::new(
            freshness_baselines.clone(),
            read_consistency_interval,
        )));

        let namespace = builder.connect().await.map_err(|e| Error::InvalidInput {
            message: format!("Failed to connect to namespace: {:?}", e),
        })?;

        let delimiter = resolve_delimiter(&ns_properties);
        Ok(Self {
            namespace,
            storage_options,
            read_consistency_interval,
            session,
            uri: format!("namespace://{}", ns_impl),
            pushdown_operations,
            ns_impl: ns_impl.to_string(),
            ns_properties,
            new_table_config,
            freshness_baselines,
            delimiter,
        })
    }

    /// Build a table's freshness handle, keyed to match the `object_id` the
    /// namespace client sends on reads (table-id parts joined by the delimiter).
    fn table_freshness(&self, namespace_path: &[String], name: &str) -> TableFreshness {
        let mut parts = namespace_path.to_vec();
        parts.push(name.to_string());
        let key = parts.join(&self.delimiter);
        TableFreshness::new(self.freshness_baselines.clone(), key)
    }

    fn apply_new_table_config(
        &self,
        params: &mut lance::dataset::WriteParams,
        request: &DbCreateTableRequest,
    ) -> Result<()> {
        let overrides = take_request_creation_overrides(params)?;

        params.data_storage_version = overrides
            .data_storage_version
            .or(params.data_storage_version)
            .or(self.new_table_config.data_storage_version);

        if let Some(enable_v2_manifest_paths) = overrides
            .enable_v2_manifest_paths
            .or(self.new_table_config.enable_v2_manifest_paths)
        {
            params.enable_v2_manifest_paths = enable_v2_manifest_paths;
        }

        let data_schema = request.data.schema();
        if let Some(enable_stable_row_ids) = overrides
            .enable_stable_row_ids
            .or(self.new_table_config.enable_stable_row_ids)
        {
            params.enable_stable_row_ids = enable_stable_row_ids;
        }

        ensure_blob_storage_version(data_schema.as_ref(), params);

        Ok(())
    }

    /// Returns `true` if the table's storage location contains `.lance` data files.
    ///
    /// Defaults to `true` on any error (describe failure, storage access failure, etc.)
    /// so that we never accidentally purge a table we simply couldn't inspect.
    async fn table_has_raw_data(&self, table_name: &str, namespace_path: &[String]) -> bool {
        use lance::io::ObjectStore;

        let table_id = [namespace_path, &[table_name.to_string()]].concat();

        let Ok(resp) = self
            .namespace
            .describe_table(DescribeTableRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
        else {
            return true;
        };

        let Some(location) = resp.location else {
            return true;
        };

        let storage_opts = resp
            .storage_options
            .unwrap_or_else(|| self.storage_options.clone());

        let session = self
            .session
            .clone()
            .unwrap_or_else(|| Arc::new(lance::session::Session::default()));

        let Ok((store, base)) = ObjectStore::from_uri_and_params(
            session.store_registry(),
            &location,
            &ObjectStoreParams {
                storage_options_accessor: (!storage_opts.is_empty())
                    .then(|| Arc::new(StorageOptionsAccessor::with_static_options(storage_opts))),
                ..Default::default()
            },
        )
        .await
        else {
            return true;
        };

        use futures::StreamExt;

        let mut stream = store.list(Some(base.join("data")));
        while let Some(res) = stream.next().await {
            match res {
                Ok(m) => {
                    if m.location.extension() == Some(LANCE_FILE_EXTENSION) && m.size > 0 {
                        return true;
                    }
                }
                Err(_) => return true,
            }
        }
        false
    }

    pub(crate) async fn repair_namespace(&self, namespace_path: &[String]) -> Result<Vec<String>> {
        let mut purged = vec![];
        let mut page_token = None;
        loop {
            let response = self
                .list_tables(ListTablesRequest {
                    id: Some(namespace_path.to_vec()),
                    page_token: page_token.clone(),
                    limit: None,
                    ..Default::default()
                })
                .await?;

            for table in &response.tables {
                let open_table_req = OpenTableRequest {
                    name: table.clone(),
                    namespace_path: namespace_path.to_vec(),
                    index_cache_size: Some(0),
                    ..Default::default()
                };

                match self.open_table(open_table_req).await {
                    Ok(table) => drop(table),
                    Err(Error::TableNotFound { .. })
                    | Err(Error::TableCorrupted { .. })
                    | Err(Error::Lance {
                        source: lance::Error::CorruptFile { .. },
                    }) => {
                        let has_data = self.table_has_raw_data(table, namespace_path).await;
                        if has_data {
                            log::warn!(
                                "Table '{}' in namespace '{:?}' is corrupted but contains raw files. Skipping automatic purge.",
                                table,
                                namespace_path
                            );
                        } else {
                            match self.drop_table(table, namespace_path).await {
                                Ok(_) => {
                                    let full_path = if namespace_path.is_empty() {
                                        table.clone()
                                    } else {
                                        format!("{}/{}", namespace_path.join("/"), table)
                                    };
                                    purged.push(full_path);
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Failed to drop table '{}' in namespace '{:?}': {}",
                                        table,
                                        namespace_path,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(other_err) => {
                        log::warn!(
                            "Unexpected error checking table '{}' in namespace '{:?}': {}",
                            table,
                            namespace_path,
                            other_err
                        );
                    }
                }
            }

            if response.page_token.is_none() || response.page_token.as_deref() == Some("") {
                break;
            }
            page_token = response.page_token;
        }
        Ok(purged)
    }

    pub(crate) async fn repair_child_namespaces(&self, parent: &[String]) -> Result<Vec<String>> {
        let mut purged_tables = vec![];
        let mut namespaces_to_visit = vec![];

        let mut page_token = None;
        loop {
            let resp = self
                .list_namespaces(ListNamespacesRequest {
                    id: Some(parent.to_vec()),
                    page_token: page_token.clone(),
                    ..Default::default()
                })
                .await?;

            namespaces_to_visit.extend(resp.namespaces.into_iter().map(|child| {
                let mut path = parent.to_vec();
                path.push(child);
                path
            }));

            if resp.page_token.is_none() || resp.page_token.as_deref() == Some("") {
                break;
            }
            page_token = resp.page_token;
        }

        while let Some(ns_path) = namespaces_to_visit.pop() {
            purged_tables.extend(self.repair_namespace(&ns_path).await?);

            let mut child_page_token = None;
            loop {
                let resp = self
                    .list_namespaces(ListNamespacesRequest {
                        id: Some(ns_path.clone()),
                        page_token: child_page_token.clone(),
                        ..Default::default()
                    })
                    .await?;

                namespaces_to_visit.extend(resp.namespaces.into_iter().map(|child| {
                    let mut next = ns_path.clone();
                    next.push(child);
                    next
                }));

                if resp.page_token.is_none() || resp.page_token.as_deref() == Some("") {
                    break;
                }
                child_page_token = resp.page_token;
            }
        }

        Ok(purged_tables)
    }
}

impl std::fmt::Debug for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceNamespaceDatabase")
            .field("storage_options", &self.storage_options)
            .field("read_consistency_interval", &self.read_consistency_interval)
            .field("pushdown_operations", &self.pushdown_operations)
            .finish()
    }
}

impl std::fmt::Display for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LanceNamespaceDatabase")
    }
}

#[async_trait]
impl Database for LanceNamespaceDatabase {
    fn uri(&self) -> &str {
        &self.uri
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        if let Some(read_consistency_inverval) = self.read_consistency_interval {
            if read_consistency_inverval.is_zero() {
                Ok(ReadConsistency::Strong)
            } else {
                Ok(ReadConsistency::Eventual(read_consistency_inverval))
            }
        } else {
            Ok(ReadConsistency::Manual)
        }
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        Ok(self.namespace.list_namespaces(request).await?)
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        Ok(self.namespace.create_namespace(request).await?)
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        Ok(self.namespace.drop_namespace(request).await?)
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        Ok(self.namespace.describe_namespace(request).await?)
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let ns_request = ListTablesRequest {
            id: Some(request.namespace_path),
            page_token: request.start_after,
            limit: request.limit.map(|l| l as i32),
            ..Default::default()
        };

        let response = self.namespace.list_tables(ns_request).await?;

        Ok(response.tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        Ok(self.namespace.list_tables(request).await?)
    }

    async fn create_table(&self, request: DbCreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace_path.clone();
        table_id.push(request.name.clone());
        let mut existing_table = None;

        match request.mode {
            CreateTableMode::Create => {}
            CreateTableMode::Overwrite => {
                let describe_request = DescribeTableRequest {
                    id: Some(table_id.clone()),
                    ..Default::default()
                };
                existing_table = self.namespace.describe_table(describe_request).await.ok();
            }
            CreateTableMode::ExistOk(_) => {
                let describe_request = DescribeTableRequest {
                    id: Some(table_id.clone()),
                    ..Default::default()
                };
                let describe_result = self.namespace.describe_table(describe_request).await;
                if describe_result.is_ok() {
                    let native_table = NativeTable::open_from_namespace(
                        self.namespace.clone(),
                        &request.name,
                        request.namespace_path.clone(),
                        None,
                        None,
                        self.read_consistency_interval,
                        self.pushdown_operations.clone(),
                        self.session.clone(),
                    )
                    .await?
                    .with_freshness(self.table_freshness(&request.namespace_path, &request.name));

                    return Ok(Arc::new(native_table));
                }
            }
        }

        let mut table_id = request.namespace_path.clone();
        table_id.push(request.name.clone());

        let declare_request = DeclareTableRequest {
            id: Some(table_id.clone()),
            ..Default::default()
        };

        let (location, initial_storage_options, managed_versioning) = {
            if let Some(response) = existing_table {
                let loc = response.location.ok_or_else(|| Error::Runtime {
                    message: "Table location is missing from describe_table response".to_string(),
                })?;
                let opts = response
                    .storage_options
                    .or_else(|| Some(self.storage_options.clone()))
                    .filter(|o| !o.is_empty());
                (loc, opts, response.managed_versioning)
            } else {
                match self.namespace.declare_table(declare_request).await {
                    Ok(response) => {
                        let loc = response.location.ok_or_else(|| Error::Runtime {
                            message: "Table location is missing from declare_table response"
                                .to_string(),
                        })?;
                        let opts = response
                            .storage_options
                            .or_else(|| Some(self.storage_options.clone()))
                            .filter(|o: &HashMap<String, String>| !o.is_empty());
                        (loc, opts, response.managed_versioning)
                    }
                    Err(e)
                        if matches!(request.mode, CreateTableMode::Create)
                            && is_table_already_exists_namespace_error(&e) =>
                    {
                        // A declare conflict can either mean (a) the table was previously
                        // *declared* but never written (in which case we should proceed and
                        // create it), or (b) the table is fully realized (in which case the
                        // user is creating something that already exists and we should
                        // surface TableAlreadyExists). Disambiguate by describing the table
                        // and checking whether it has both a version and a schema.
                        let response = self
                            .namespace
                            .describe_table(DescribeTableRequest {
                                id: Some(table_id.clone()),
                                ..Default::default()
                            })
                            .await
                            .map_err(|describe_err| {
                                map_namespace_lance_error(describe_err, &request.name)
                            })?;

                        if response.version.is_some() && response.schema.is_some() {
                            return Err(Error::TableAlreadyExists {
                                name: request.name.clone(),
                            });
                        }

                        let loc = response.location.ok_or_else(|| Error::Runtime {
                            message: "Table location is missing from describe_table response"
                                .to_string(),
                        })?;
                        let opts = response
                            .storage_options
                            .or_else(|| Some(self.storage_options.clone()))
                            .filter(|o: &HashMap<String, String>| !o.is_empty());
                        (loc, opts, response.managed_versioning)
                    }
                    Err(e) => {
                        return Err(map_namespace_lance_error(e, &request.name));
                    }
                }
            }
        };

        // Build write params with storage options and commit handler
        let mut params = request
            .write_options
            .lance_write_params
            .clone()
            .unwrap_or_default();
        self.apply_new_table_config(&mut params, &request)?;

        if matches!(request.mode, CreateTableMode::Overwrite) {
            params.mode = WriteMode::Overwrite;
        }

        // Set up storage options if provided
        if let Some(storage_opts) = initial_storage_options {
            let store_params = params
                .store_params
                .get_or_insert_with(ObjectStoreParams::default);
            store_params.storage_options_accessor = Some(Arc::new(
                StorageOptionsAccessor::with_static_options(storage_opts),
            ));
        }

        // Set up commit handler when managed_versioning is enabled
        if managed_versioning == Some(true) {
            let external_store = LanceNamespaceExternalManifestStore::for_table_uri(
                self.namespace.clone(),
                table_id.clone(),
                &location,
            )?;
            let commit_handler: Arc<dyn CommitHandler> = Arc::new(ExternalManifestCommitHandler {
                external_manifest_store: Arc::new(external_store),
            });
            params.commit_handler = Some(commit_handler);
        }

        let write_params = Some(params);

        let native_table = NativeTable::create_from_namespace(
            self.namespace.clone(),
            &location,
            &request.name,
            request.namespace_path.clone(),
            request.data,
            None, // write_store_wrapper not used for namespace connections
            write_params,
            self.read_consistency_interval,
            self.pushdown_operations.clone(),
            self.session.clone(),
        )
        .await?
        .with_freshness(self.table_freshness(&request.namespace_path, &request.name));

        Ok(Arc::new(native_table))
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let native_table = NativeTable::open_from_namespace(
            self.namespace.clone(),
            &request.name,
            request.namespace_path.clone(),
            None, // write_store_wrapper not used for namespace connections
            request.lance_read_params,
            self.read_consistency_interval,
            self.pushdown_operations.clone(),
            self.session.clone(),
        )
        .await?
        .with_freshness(self.table_freshness(&request.namespace_path, &request.name));

        Ok(Arc::new(native_table))
    }

    async fn clone_table(&self, _request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        Err(Error::NotSupported {
            message: "clone_table is not supported for namespace connections".to_string(),
        })
    }

    async fn rename_table(
        &self,
        cur_name: &str,
        new_name: &str,
        cur_namespace_path: &[String],
        new_namespace_path: &[String],
    ) -> Result<()> {
        let mut cur_table_id = cur_namespace_path.to_vec();
        cur_table_id.push(cur_name.to_string());

        let new_namespace_id = if new_namespace_path.is_empty() {
            None
        } else {
            Some(new_namespace_path.to_vec())
        };

        let rename_request = RenameTableRequest {
            id: Some(cur_table_id),
            new_table_name: new_name.to_string(),
            new_namespace_id,
            ..Default::default()
        };
        self.namespace
            .rename_table(rename_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to rename table: {}", e),
            })?;

        Ok(())
    }

    async fn drop_table(&self, name: &str, namespace_path: &[String]) -> Result<()> {
        let mut table_id = namespace_path.to_vec();
        table_id.push(name.to_string());

        let drop_request = DropTableRequest {
            id: Some(table_id),
            ..Default::default()
        };
        self.namespace
            .drop_table(drop_request)
            .await
            .map_err(|e| map_namespace_lance_error(e, name))?;

        Ok(())
    }

    #[allow(deprecated)]
    async fn drop_all_tables(&self, namespace_path: &[String]) -> Result<()> {
        let tables = self
            .table_names(TableNamesRequest {
                namespace_path: namespace_path.to_vec(),
                start_after: None,
                limit: None,
            })
            .await?;

        for table in tables {
            self.drop_table(&table, namespace_path).await?;
        }

        Ok(())
    }

    async fn repair(&self) -> Result<RepairDatabaseResponse> {
        let mut purged_tables = self.repair_namespace(&[]).await?;
        purged_tables.extend(self.repair_child_namespaces(&[]).await?);
        Ok(RepairDatabaseResponse { purged_tables })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn namespace_client(&self) -> Result<Arc<dyn LanceNamespace>> {
        Ok(self.namespace.clone())
    }

    async fn namespace_client_config(&self) -> Result<(String, HashMap<String, String>)> {
        Ok((self.ns_impl.clone(), self.ns_properties.clone()))
    }
}

#[cfg(test)]
#[cfg(not(windows))] // TODO: support windows for lance-namespace
mod tests {
    use super::*;
    use crate::connect_namespace;
    use crate::query::ExecutableQuery;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    /// Helper function to create test data
    fn create_test_data() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    /// The shared parse-and-sanitize boundary is wired into this path: the
    /// request-level creation key must act as an override (the strip itself
    /// is covered by the listing tests).
    #[tokio::test]
    async fn request_level_creation_keys_are_taken_as_overrides() {
        use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;

        let tmp_dir = tempdir().unwrap();
        let mut properties = HashMap::new();
        properties.insert(
            "root".to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
        );
        let db = connect_namespace("dir", properties)
            .execute()
            .await
            .unwrap();

        let store_params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([(
                    OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS.to_string(),
                    "true".to_string(),
                )]),
            ))),
            ..Default::default()
        };
        let table = db
            .create_table("t", create_test_data())
            .write_options(crate::table::WriteOptions {
                lance_write_params: Some(lance::dataset::WriteParams {
                    store_params: Some(store_params),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();
        let native = table.as_native().unwrap();
        assert!(
            native
                .dataset
                .get()
                .await
                .unwrap()
                .manifest
                .uses_stable_row_ids(),
            "the creation key must be honored as an override"
        );

        let table = db.open_table("t").execute().await.unwrap();
        let rows: usize = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 5);
    }

    /// Sanitation on this path: apply must strip the creation keys from the
    /// store options while genuine options and the provider survive.
    #[tokio::test]
    async fn apply_new_table_config_sanitizes_request_store_options() {
        use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;
        use lance_io::object_store::StorageOptionsProvider;

        #[derive(Debug)]
        struct EmptyProvider;

        #[async_trait::async_trait]
        impl StorageOptionsProvider for EmptyProvider {
            async fn fetch_storage_options(
                &self,
            ) -> lance_core::Result<Option<HashMap<String, String>>> {
                Ok(Some(HashMap::new()))
            }

            fn provider_id(&self) -> String {
                "empty-test-provider".into()
            }
        }

        let tmp_dir = tempdir().unwrap();
        let mut properties = HashMap::new();
        properties.insert(
            "root".to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
        );
        let db = LanceNamespaceDatabase::connect_with_new_table_config(
            "dir",
            properties,
            HashMap::new(),
            None,
            None,
            HashSet::new(),
            NewTableConfig::default(),
        )
        .await
        .unwrap();

        let request = DbCreateTableRequest::new("t".to_string(), Box::new(create_test_data()));
        let mut params = lance::dataset::WriteParams {
            store_params: Some(ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    StorageOptionsAccessor::with_initial_and_provider(
                        HashMap::from([
                            ("region".to_string(), "us-west-2".to_string()),
                            (
                                OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS.to_string(),
                                "true".to_string(),
                            ),
                        ]),
                        Arc::new(EmptyProvider),
                    ),
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        db.apply_new_table_config(&mut params, &request).unwrap();

        assert!(params.enable_stable_row_ids);
        let store_params = params.store_params.unwrap();
        let options = store_params.storage_options().cloned().unwrap();
        assert!(!options.contains_key(OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS));
        assert_eq!(options.get("region").map(String::as_str), Some("us-west-2"));
        assert!(
            store_params
                .storage_options_accessor
                .unwrap()
                .has_provider()
        );
    }

    /// Helper to create a temp directory and connect a directory-based namespace database
    async fn setup_dir_conn() -> (tempfile::TempDir, crate::connection::Connection) {
        let tmp_dir = tempdir().unwrap();
        let mut properties = HashMap::new();
        properties.insert(
            "root".to_string(),
            tmp_dir.path().to_str().unwrap().to_string(),
        );

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        (tmp_dir, conn)
    }

    /// Helper to simulate corruption by deleting `_versions/` (manifests)
    /// and optionally `data/` (raw data files).
    async fn corrupt_table_on_disk(
        conn: &crate::connection::Connection,
        table_id: Vec<String>,
        remove_data: bool,
    ) -> std::path::PathBuf {
        let describe_res = conn
            .database()
            .namespace_client()
            .await
            .unwrap()
            .describe_table(lance_namespace::models::DescribeTableRequest {
                id: Some(table_id),
                ..Default::default()
            })
            .await
            .unwrap();

        let loc = describe_res.location.expect("table location not found");
        let path = if let Ok(url) = url::Url::parse(&loc) {
            url.to_file_path()
                .unwrap_or_else(|_| std::path::PathBuf::from(&loc))
        } else {
            std::path::PathBuf::from(loc)
        };

        let versions_path = path.join("_versions");
        if versions_path.exists() {
            std::fs::remove_dir_all(versions_path).unwrap();
        }
        if remove_data {
            let data_path = path.join("data");
            if data_path.exists() {
                std::fs::remove_dir_all(data_path).unwrap();
            }
        }
        path
    }

    #[tokio::test]
    async fn test_namespace_connection_simple() {
        // Test that namespace connections work with simple connect_namespace(impl_type, properties)
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        // This should succeed with directory-based namespace
        let result = connect_namespace("dir", properties).execute().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_connection_with_storage_options() {
        // Test namespace connections with storage options
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        // This should succeed with directory-based namespace and storage options
        let result = connect_namespace("dir", properties)
            .storage_option("timeout", "30s")
            .execute()
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_connection_with_all_options() {
        use crate::embeddings::MemoryRegistry;
        use std::time::Duration;

        // Test namespace connections with all configuration options
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let embedding_registry = Arc::new(MemoryRegistry::new());
        let session = Arc::new(lance::session::Session::default());

        // Test with all options set
        let result = connect_namespace("dir", properties)
            .storage_option("timeout", "30s")
            .storage_options([("cache_size", "1gb"), ("region", "us-east-1")])
            .read_consistency_interval(Duration::from_secs(5))
            .embedding_registry(embedding_registry.clone())
            .session(session.clone())
            .execute()
            .await;

        assert!(result.is_ok());

        let conn = result.unwrap();

        // Verify embedding registry is set correctly
        assert!(std::ptr::eq(
            conn.embedding_registry() as *const _,
            embedding_registry.as_ref() as *const _
        ));
    }

    #[tokio::test]
    async fn test_namespace_connection_with_namespace_client_properties() {
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .namespace_client_property("table_version_tracking_enabled", "true")
            .namespace_client_property("manifest_enabled", "true")
            .execute()
            .await
            .expect("Failed to connect to namespace");

        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        let test_data = create_test_data();
        conn.create_table("test_table", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        let namespace_client = conn.namespace_client().await.unwrap();
        let describe = namespace_client
            .describe_table(DescribeTableRequest {
                id: Some(vec!["test_ns".into(), "test_table".into()]),
                ..Default::default()
            })
            .await
            .expect("Failed to describe table");

        assert_eq!(describe.managed_versioning, Some(true));
    }

    #[tokio::test]
    async fn test_namespace_create_table_basic() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        // Connect to namespace using DirectoryNamespace
        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Test: Create a table in the child namespace
        let test_data = create_test_data();
        let table = conn
            .create_table("test_table", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Verify: Table was created and can be queried
        let results = table
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify: Table namespace is correct
        assert_eq!(table.namespace(), &["test_ns"]);
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.id(), "test_ns$test_table");

        // Verify: Table appears in table_names for the child namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names.contains(&"test_table".to_string()));
    }

    #[tokio::test]
    async fn test_namespace_branch_query_under_pushdown_stays_local() {
        // With QueryTable pushdown enabled, a query on the main branch routes to
        // the namespace server, but a branch handle must run locally: the
        // server-side request carries no branch and would return main's rows.
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .pushdown_operation(NamespaceClientPushdownOperation::QueryTable)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // main has 5 rows
        let table = conn
            .create_table("ref_test", create_test_data())
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");
        let main_version = table.version().await.unwrap();

        // fork a branch off main, then add 5 more rows so it differs from main
        let branch = table
            .create_branch("exp", main_version)
            .await
            .expect("Failed to create branch");
        branch
            .add(create_test_data())
            .execute()
            .await
            .expect("Failed to append to branch");

        // the branch query must run locally and see the branch's 10 rows --
        // not get routed to the server (which carries no branch) and see main's 5
        let results = branch
            .query()
            .execute()
            .await
            .expect("Failed to query branch")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");
        let count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn test_namespace_describe_table() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        // Connect to namespace
        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create a table in child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("describe_test", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Test: Open the table (which internally uses describe_table)
        let opened_table = conn
            .open_table("describe_test")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table");

        // Verify: Can query the opened table
        let results = opened_table
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify schema matches
        let schema = opened_table.schema().await.expect("Failed to get schema");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        // Verify namespace and id
        assert_eq!(opened_table.namespace(), &["test_ns"]);
        assert_eq!(opened_table.id(), "test_ns$describe_test");
    }

    #[tokio::test]
    async fn test_namespace_create_table_overwrite_mode() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with 5 rows in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("overwrite_test", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Create new data with 3 rows
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int32Array::from(vec![10, 20, 30]);
        let name_array = StringArray::from(vec!["New1", "New2", "New3"]);
        let test_data2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        // Test: Overwrite the table
        let table2 = conn
            .create_table("overwrite_test", test_data2)
            .namespace(vec!["test_ns".into()])
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await
            .expect("Failed to overwrite table");

        // Verify: Table has new data (3 rows instead of 5)
        let results = table2
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);

        // Verify the data is actually the new data
        let id_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 10);
        assert_eq!(id_col.value(1), 20);
        assert_eq!(id_col.value(2), 30);
    }

    #[tokio::test]
    async fn test_namespace_create_table_after_declare_conflict() {
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        let namespace_client = conn.namespace_client().await.unwrap();
        namespace_client
            .declare_table(DeclareTableRequest {
                id: Some(vec!["test_ns".into(), "declared_test".into()]),
                ..Default::default()
            })
            .await
            .expect("Failed to declare table");

        let test_data = create_test_data();
        let table = conn
            .create_table("declared_test", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table after declare conflict");

        let results = table
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        assert_eq!(table.namespace(), &["test_ns"]);
        assert_eq!(table.id(), "test_ns$declared_test");
    }

    #[tokio::test]
    async fn test_namespace_create_table_exist_ok_mode() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with test data in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("exist_ok_test", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Try to create again with exist_ok mode
        let test_data2 = create_test_data();
        let table2 = conn
            .create_table("exist_ok_test", test_data2)
            .namespace(vec!["test_ns".into()])
            .mode(CreateTableMode::exist_ok(|req| req))
            .execute()
            .await
            .expect("Failed with exist_ok mode");

        // Verify: Table still has original data (5 rows)
        let results = table2
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_namespace_create_multiple_tables() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create first table in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create first table");

        // Create second table in child namespace
        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create second table");

        // Verify: Both tables appear in table list for the child namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));

        // Verify: Can open both tables
        let opened_table1 = conn
            .open_table("table1")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table1");

        let opened_table2 = conn
            .open_table("table2")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table2");

        // Verify both tables work
        let count1 = opened_table1
            .count_rows(None)
            .await
            .expect("Failed to count rows in table1");
        assert_eq!(count1, 5);

        let count2 = opened_table2
            .count_rows(None)
            .await
            .expect("Failed to count rows in table2");
        assert_eq!(count2, 5);
    }

    #[tokio::test]
    async fn test_namespace_table_not_found() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Test: Try to open a non-existent table in the child namespace
        let result = conn
            .open_table("non_existent_table")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await;

        // Verify: Should return TableNotFound — not a generic Runtime/internal error
        // (regression test for ENT-1235: open_table on missing table previously surfaced as
        // a generic 500/Runtime error rather than TableNotFound).
        match result {
            Err(Error::TableNotFound { name, .. }) => {
                assert_eq!(name, "non_existent_table");
            }
            Err(other) => panic!("Expected TableNotFound, got: {:?}", other),
            Ok(_) => panic!("Expected open_table to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn test_namespace_open_table_not_found_at_root() {
        // Same as above, but at the root namespace (no parent namespace creation).
        // Covers the common code path used by `db.open_table("foo")` without a namespace.
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        let result = conn.open_table("missing_at_root").execute().await;

        match result {
            Err(Error::TableNotFound { name, .. }) => {
                assert_eq!(name, "missing_at_root");
            }
            Err(other) => panic!("Expected TableNotFound, got: {:?}", other),
            Ok(_) => panic!("Expected open_table to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn test_namespace_create_table_already_exists() {
        // Regression test for ENT-1235: create_table on an existing table (in default
        // Create mode) should return TableAlreadyExists, not a generic Runtime/500 error.
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create the table once.
        conn.create_table("dup_table", create_test_data())
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table the first time");

        // Try to create it again with the default Create mode.
        let result = conn
            .create_table("dup_table", create_test_data())
            .namespace(vec!["test_ns".into()])
            .execute()
            .await;

        match result {
            Err(Error::TableAlreadyExists { name }) => {
                assert_eq!(name, "dup_table");
            }
            Err(other) => panic!("Expected TableAlreadyExists, got: {:?}", other),
            Ok(_) => panic!("Expected create_table to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn test_namespace_create_table_already_exists_at_root() {
        // Same as above, but at the root namespace.
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        conn.create_table("dup_root", create_test_data())
            .execute()
            .await
            .expect("Failed to create table the first time");

        let result = conn
            .create_table("dup_root", create_test_data())
            .execute()
            .await;

        match result {
            Err(Error::TableAlreadyExists { name }) => {
                assert_eq!(name, "dup_root");
            }
            Err(other) => panic!("Expected TableAlreadyExists, got: {:?}", other),
            Ok(_) => panic!("Expected create_table to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn test_namespace_drop_table() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(vec!["test_ns".into()]),
            ..Default::default()
        })
        .await
        .expect("Failed to create namespace");

        // Create a table in child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("drop_test", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Verify table exists in child namespace
        let table_names_before = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names_before.contains(&"drop_test".to_string()));

        // Test: Drop the table
        conn.drop_table("drop_test", &["test_ns".into()])
            .await
            .expect("Failed to drop table");

        // Verify: Table no longer exists
        let table_names_after = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(!table_names_after.contains(&"drop_test".to_string()));

        let error = conn
            .drop_table("drop_test", &["test_ns".into()])
            .await
            .expect_err("dropping a missing table should fail");
        assert!(
            matches!(error, Error::TableNotFound { ref name, .. } if name == "drop_test"),
            "expected TableNotFound, got: {error:?}"
        );

        // Verify: Cannot open dropped table
        let open_result = conn.open_table("drop_test").execute().await;
        assert!(open_result.is_err());
    }

    #[tokio::test]
    async fn test_table_names_at_root() {
        // Test that table_names at root (empty namespace) works correctly
        // This is a regression test for a bug where empty namespace was converted to None
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create multiple tables at root namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .execute()
            .await
            .expect("Failed to create table1 at root");

        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .execute()
            .await
            .expect("Failed to create table2 at root");

        // List tables at root using table_names (empty namespace means root)
        let table_names = conn
            .table_names()
            .execute()
            .await
            .expect("Failed to list tables at root");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));
        assert_eq!(table_names.len(), 2);
    }

    #[tokio::test]
    async fn test_namespace_repair() {
        let (_tmp_dir, conn) = setup_dir_conn().await;
        let test_data = create_test_data();

        // 1. Root namespace: 1 valid table + 1 corrupted stub (to purge)
        conn.create_table("root_valid", test_data.clone())
            .execute()
            .await
            .unwrap();
        conn.create_table("root_stub", test_data.clone())
            .execute()
            .await
            .unwrap();
        corrupt_table_on_disk(&conn, vec!["root_stub".to_string()], true).await;

        // 2. Nested namespace: valid, empty stub (to purge), and corrupted with data (to preserve)
        let ns_path = vec!["nested_ns".to_string()];
        conn.create_namespace(CreateNamespaceRequest {
            id: Some(ns_path.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

        conn.create_table("nested_valid", test_data.clone())
            .namespace(ns_path.clone())
            .execute()
            .await
            .unwrap();
        conn.create_table("nested_stub", test_data.clone())
            .namespace(ns_path.clone())
            .execute()
            .await
            .unwrap();
        conn.create_table("nested_with_data", test_data)
            .namespace(ns_path.clone())
            .execute()
            .await
            .unwrap();

        corrupt_table_on_disk(&conn, vec!["nested_ns".into(), "nested_stub".into()], true).await;
        let preserved_path = corrupt_table_on_disk(
            &conn,
            vec!["nested_ns".into(), "nested_with_data".into()],
            false,
        )
        .await;

        // Run repair across all namespaces
        let resp = conn.repair().await.expect("Failed to run repair");
        assert_eq!(
            resp.purged_tables,
            vec!["root_stub".to_string(), "nested_ns/nested_stub".to_string()]
        );
        assert!(preserved_path.exists(), "Table with data must be preserved");

        // Verify root tables post repair
        let root_tables = conn.table_names().execute().await.unwrap();
        assert_eq!(root_tables, vec!["root_valid".to_string()]);

        // Verify nested tables post repair
        let nested_tables = conn
            .table_names()
            .namespace(ns_path)
            .execute()
            .await
            .unwrap();
        assert_eq!(
            nested_tables,
            vec!["nested_valid".to_string(), "nested_with_data".to_string()]
        );
    }

    #[derive(Debug, Default)]
    struct FailingListNamespaceClient;

    #[async_trait::async_trait]
    impl LanceNamespace for FailingListNamespaceClient {
        fn namespace_id(&self) -> String {
            "failing_list".to_string()
        }

        async fn list_tables(
            &self,
            _request: ListTablesRequest,
        ) -> lance::Result<ListTablesResponse> {
            Ok(ListTablesResponse {
                tables: vec![],
                page_token: None,
                context: None,
            })
        }

        async fn list_namespaces(
            &self,
            _request: ListNamespacesRequest,
        ) -> lance::Result<ListNamespacesResponse> {
            Err(lance::Error::io_source(Box::new(std::io::Error::other(
                "injected list namespaces error",
            ))))
        }
    }

    #[tokio::test]
    async fn test_repair_child_namespaces_propagates_error() {
        let client = Arc::new(FailingListNamespaceClient);
        let db = LanceNamespaceDatabase::from_namespace_client(
            client,
            "failing_mock".to_string(),
            HashMap::new(),
            HashMap::new(),
            None,
            None,
            HashSet::new(),
        );

        let res = db.repair().await;
        assert!(res.is_err(), "repair must propagate list_namespaces error");
    }

    #[derive(Debug)]
    struct PaginatedMockNamespaceClient {
        dropped_tables: Arc<Mutex<Vec<String>>>,
        tmp_dir: String,
    }

    #[async_trait::async_trait]
    impl LanceNamespace for PaginatedMockNamespaceClient {
        fn namespace_id(&self) -> String {
            "paginated_mock".to_string()
        }

        async fn list_namespaces(
            &self,
            request: ListNamespacesRequest,
        ) -> lance::Result<ListNamespacesResponse> {
            let ns_path = request.id.unwrap_or_default();
            if ns_path.is_empty() {
                match request.page_token.as_deref() {
                    None => Ok(ListNamespacesResponse {
                        namespaces: vec!["child1".to_string()],
                        page_token: Some("ns_p2".to_string()),
                        context: None,
                    }),
                    Some("ns_p2") => Ok(ListNamespacesResponse {
                        namespaces: vec!["child2".to_string()],
                        page_token: None,
                        context: None,
                    }),
                    _ => Ok(ListNamespacesResponse::default()),
                }
            } else {
                Ok(ListNamespacesResponse::default())
            }
        }

        async fn list_tables(
            &self,
            request: ListTablesRequest,
        ) -> lance::Result<ListTablesResponse> {
            let ns_path = request.id.unwrap_or_default();
            let prefix = if ns_path.is_empty() {
                "root".to_string()
            } else {
                ns_path.join("_")
            };

            match request.page_token.as_deref() {
                None => Ok(ListTablesResponse {
                    tables: vec![format!("{}_t1", prefix)],
                    page_token: Some("tbl_p2".to_string()),
                    context: None,
                }),
                Some("tbl_p2") => Ok(ListTablesResponse {
                    tables: vec![format!("{}_t2", prefix)],
                    page_token: None,
                    context: None,
                }),
                _ => Ok(ListTablesResponse::default()),
            }
        }

        async fn describe_table(
            &self,
            request: DescribeTableRequest,
        ) -> lance::Result<lance_namespace::models::DescribeTableResponse> {
            let id = request.id.unwrap_or_default();
            let table_name = id.last().cloned().unwrap_or_default();
            let table_dir = format!("{}/{}", self.tmp_dir, table_name);
            std::fs::create_dir_all(&table_dir).ok();
            Ok(lance_namespace::models::DescribeTableResponse {
                location: Some(table_dir),
                is_only_declared: Some(true),
                ..Default::default()
            })
        }

        async fn drop_table(
            &self,
            request: DropTableRequest,
        ) -> lance::Result<lance_namespace::models::DropTableResponse> {
            let id = request.id.unwrap_or_default();
            self.dropped_tables.lock().unwrap().push(id.join("/"));
            Ok(lance_namespace::models::DropTableResponse::default())
        }
    }

    #[tokio::test]
    async fn test_repair_paginated_namespaces_and_tables() {
        let tmp = tempdir().unwrap();
        let dropped = Arc::new(Mutex::new(Vec::new()));
        let client = Arc::new(PaginatedMockNamespaceClient {
            dropped_tables: dropped.clone(),
            tmp_dir: tmp.path().to_str().unwrap().to_string(),
        });

        let db = LanceNamespaceDatabase::from_namespace_client(
            client,
            "paginated_mock".to_string(),
            HashMap::new(),
            HashMap::new(),
            None,
            None,
            HashSet::new(),
        );

        let resp = db.repair().await.unwrap();

        assert_eq!(
            resp.purged_tables,
            vec![
                "root_t1".to_string(),
                "root_t2".to_string(),
                "child2/child2_t1".to_string(),
                "child2/child2_t2".to_string(),
                "child1/child1_t1".to_string(),
                "child1/child1_t2".to_string(),
            ]
        );

        let dropped_lock = dropped.lock().unwrap();
        assert_eq!(
            *dropped_lock,
            vec![
                "root_t1".to_string(),
                "root_t2".to_string(),
                "child2/child2_t1".to_string(),
                "child2/child2_t2".to_string(),
                "child1/child1_t1".to_string(),
                "child1/child1_t2".to_string(),
            ]
        );
    }
}
