// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Manifest table management for v2 listing databases

use crate::database::listing::{
    apply_pagination, apply_pagination_with_key, ListingDatabase, NewTableConfig,
    LANCE_FILE_EXTENSION,
};
use crate::database::{
    CloneTableRequest, CreateNamespaceRequest, CreateTableMode, Database, DropNamespaceRequest,
    ListNamespacesRequest, ReadConsistency,
};
use crate::error::{Error, Result};
use crate::table::dataset::DatasetConsistencyWrapper;
use crate::table::{BaseTable, NativeTable};
use crate::utils::{validate_namespace, validate_table_name};
use arrow_array::{Array, ListArray, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::refs::Ref;
use lance::dataset::{builder::DatasetBuilder, InsertBuilder, ReadParams, WriteMode, WriteParams};
use lance::session::Session;
use lance::Dataset;
use lance_datafusion::utils::StreamingWriteSource;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use lance_table::io::commit::commit_handler_from_url;
use rand::Rng;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the manifest database
#[derive(Debug, Clone)]
pub struct ManifestDatabaseConfig {
    /// Read consistency interval for the manifest table
    pub read_consistency_interval: Option<Duration>,
    /// Whether to enable inline optimization (compaction and indexing) on the manifest table
    pub inline_optimization_enabled: bool,
    /// Whether directory listing is enabled in the parent database
    /// If true, new tables in root namespace will keep the <table_name>.lance naming.
    /// If false, they can use hash-based names without .lance extension.
    pub parent_dir_listing_enabled: bool,
}

impl Default for ManifestDatabaseConfig {
    fn default() -> Self {
        Self {
            read_consistency_interval: None,
            inline_optimization_enabled: true,
            parent_dir_listing_enabled: true, // Default to true for backward compatibility
        }
    }
}

/// Delimiter used in object IDs to separate namespace components and table names
const DELIMITER: &str = "$";
/// Manifest Lance table name
const MANIFEST_TABLE_NAME: &str = "__manifest";

/// Type of object stored in the manifest
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectType {
    Namespace,
    Table,
}

impl ObjectType {
    /// Convert to string representation for storage
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Namespace => "namespace",
            Self::Table => "table",
        }
    }
}

impl std::str::FromStr for ObjectType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "namespace" => Ok(Self::Namespace),
            "table" => Ok(Self::Table),
            _ => Err(Error::Runtime {
                message: format!("Unknown object type: {}", s),
            }),
        }
    }
}

/// Information about a namespace in the manifest table
#[derive(Debug, Clone)]
pub struct NamespaceInfo {
    /// Parent namespace path (empty for root-level namespaces)
    pub namespace: Vec<String>,
    /// The name of this namespace (last component)
    pub name: String,
    /// Optional metadata as key-value pairs
    pub metadata: Option<HashMap<String, String>>,
}

/// Information about a table in the manifest table
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Parent namespace path (empty for root-level tables)
    pub namespace: Vec<String>,
    /// The name of the table
    pub name: String,
    /// The relative path of the table directory to the root directory
    /// Note that it is called location to reserve the right to store full URI if necessary.
    pub location: String,
}

/// A manifest-based listing database that uses __manifest (without .lance extension) for metadata
#[derive(Debug)]
pub struct ManifestDatabase {
    /// Base URI of the database
    uri: String,
    /// Query string for the URI
    query_string: Option<String>,
    /// Lance session for dataset operations
    session: Arc<Session>,
    /// The manifest dataset
    manifest_dataset: DatasetConsistencyWrapper,
    /// Configuration for the manifest database
    manifest_config: ManifestDatabaseConfig,
    /// Object store for file operations
    object_store: Arc<ObjectStore>,
    /// Storage options to be inherited by tables
    storage_options: HashMap<String, String>,
    /// Configuration for new tables
    new_table_config: NewTableConfig,
    /// Object store wrapper
    store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    /// Read consistency interval for tables
    read_consistency_interval: Option<Duration>,
}

impl ManifestDatabase {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        uri: String,
        query_string: Option<String>,
        session: Arc<Session>,
        manifest_config: ManifestDatabaseConfig,
        object_store: Arc<ObjectStore>,
        storage_options: HashMap<String, String>,
        new_table_config: NewTableConfig,
        store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        read_consistency_interval: Option<Duration>,
    ) -> Result<Self> {
        let manifest_uri =
            Self::build_object_uri(&uri, MANIFEST_TABLE_NAME, query_string.as_deref()).unwrap();

        // Try to load existing manifest
        let read_params = ReadParams {
            store_options: Some(ObjectStoreParams {
                storage_options: Some(storage_options.clone()),
                ..Default::default()
            }),
            session: Some(session.clone()),
            ..Default::default()
        };

        let manifest_dataset = match DatasetBuilder::from_uri(&manifest_uri)
            .with_read_params(read_params)
            .load()
            .await
        {
            Ok(dataset) => {
                // Manifest exists, wrap it
                DatasetConsistencyWrapper::new_latest(
                    dataset,
                    manifest_config.read_consistency_interval,
                )
            }
            Err(_) => {
                // Manifest doesn't exist, create it
                let schema = Self::manifest_schema();
                let write_params = WriteParams {
                    mode: WriteMode::Create,
                    session: Some(session.clone()),
                    store_params: Some(ObjectStoreParams {
                        storage_options: Some(storage_options.clone()),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                let insert_builder = InsertBuilder::new(&manifest_uri).with_params(&write_params);

                let mut dataset = insert_builder
                    .execute_stream(Box::new(RecordBatchIterator::new(
                        vec![Ok(RecordBatch::new_empty(schema.clone()))],
                        schema.clone(),
                    )))
                    .await
                    .map_err(|e| Error::Lance { source: e })?;

                if let Err(e) = Self::create_manifest_index_object_id_btree(&mut dataset).await {
                    log::warn!(
                        "Failed to create BTree index on object_id: {}. Query performance may be impacted.",
                        e
                    );
                }

                if let Err(e) = Self::create_manifest_index_object_type_bitmap(&mut dataset).await {
                    log::warn!(
                        "Failed to create Bitmap index on object_type: {}. Query performance may be impacted.",
                        e
                    );
                }

                if let Err(e) =
                    Self::create_manifest_index_base_objects_label_list(&mut dataset).await
                {
                    log::warn!(
                        "Failed to create LabelList index on base_objects: {}. Query performance may be impacted.",
                        e
                    );
                }

                DatasetConsistencyWrapper::new_latest(
                    dataset,
                    manifest_config.read_consistency_interval,
                )
            }
        };

        Ok(Self {
            uri,
            query_string,
            session,
            manifest_dataset,
            manifest_config,
            object_store,
            storage_options,
            new_table_config,
            store_wrapper,
            read_consistency_interval,
        })
    }

    pub fn build_object_id(namespace: &[String], name: &str) -> String {
        if namespace.is_empty() {
            name.to_string()
        } else {
            let mut id = namespace.join(DELIMITER);
            id.push_str(DELIMITER);
            id.push_str(name);
            id
        }
    }

    pub fn parse_object_id(object_id: &str) -> (Vec<String>, String) {
        let parts: Vec<&str> = object_id.split(DELIMITER).collect();
        if parts.len() == 1 {
            (Vec::new(), parts[0].to_string())
        } else {
            let namespace = parts[..parts.len() - 1]
                .iter()
                .map(|s| s.to_string())
                .collect();
            let name = parts[parts.len() - 1].to_string();
            (namespace, name)
        }
    }

    fn build_object_dir_name(
        &self,
        name: &str,
        namespace: &[String],
        object_type: ObjectType,
        object_id: &str,
    ) -> String {
        if namespace.is_empty()
            && self.manifest_config.parent_dir_listing_enabled
            && object_type == ObjectType::Table
        {
            // For table in root namespace with dir listing enabled, use the table name + .lance
            format!("{}.{}", name, LANCE_FILE_EXTENSION)
        } else {
            Self::generate_dir_name_prefix(object_id)
        }
    }

    fn manifest_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("object_id", DataType::Utf8, false),
            Field::new("object_type", DataType::Utf8, false),
            Field::new("location", DataType::Utf8, true), // Optional: namespaces don't have location
            Field::new("metadata", DataType::Utf8, true), // Optional: tables don't have metadata
            Field::new(
                "base_objects",
                DataType::List(Arc::new(Field::new("object_id", DataType::Utf8, true))),
                true,
            ), // Optional: mainly for objects like view to record dependency
        ]))
    }

    async fn create_manifest_index_object_id_btree(dataset: &mut Dataset) -> lance::Result<()> {
        let btree_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["object_id"],
                IndexType::BTree,
                Some("object_id_btree".to_string()),
                &btree_params,
                true,
            )
            .await
    }

    async fn create_manifest_index_object_type_bitmap(dataset: &mut Dataset) -> lance::Result<()> {
        let bitmap_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["object_type"],
                IndexType::Bitmap,
                Some("object_type_bitmap".to_string()),
                &bitmap_params,
                true,
            )
            .await
    }

    async fn create_manifest_index_base_objects_label_list(
        dataset: &mut Dataset,
    ) -> lance::Result<()> {
        let label_list_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["base_objects"],
                IndexType::LabelList,
                Some("base_objects_label_list".to_string()),
                &label_list_params,
                true,
            )
            .await
    }

    /// Get a scanner for the manifest dataset
    async fn manifest_scanner(&self) -> Result<lance::dataset::scanner::Scanner> {
        let dataset_guard = self.manifest_dataset.get().await?;
        Ok(dataset_guard.scan())
    }

    /// Helper to execute a scanner and collect results into a Vec
    async fn execute_scanner(
        scanner: lance::dataset::scanner::Scanner,
    ) -> Result<Vec<RecordBatch>> {
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.map_err(|e| Error::Lance { source: e })?);
        }

        Ok(batches)
    }

    async fn manifest_contains_object(&self, object_id: &str) -> Result<bool> {
        let filter = format!("object_id = '{}'", object_id);
        let dataset = &self.manifest_dataset;

        let dataset_guard = dataset.get().await?;
        let mut scanner = dataset_guard.scan();

        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;

        // Project no columns and enable row IDs for count_rows to work
        scanner
            .project::<&str>(&[])
            .map_err(|e| Error::Lance { source: e })?;

        scanner.with_row_id();

        let count = scanner
            .count_rows()
            .await
            .map_err(|e| Error::Lance { source: e })?;

        Ok(count > 0)
    }

    async fn query_manifest_for_table(&self, object_id: &str) -> Result<Option<TableInfo>> {
        let filter = format!("object_id = '{}' AND object_type = 'table'", object_id);
        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id", "location"])
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;

        let mut found_result: Option<TableInfo> = None;
        let mut total_rows = 0;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            total_rows += batch.num_rows();
            if total_rows > 1 {
                return Err(Error::Runtime {
                    message: format!(
                        "Expected exactly 1 table with id '{}', found {}",
                        object_id, total_rows
                    ),
                });
            }

            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            let location_array = Self::get_string_column(&batch, "location")?;
            let location = location_array.value(0).to_string();
            let (namespace, name) = Self::parse_object_id(object_id_array.value(0));
            found_result = Some(TableInfo {
                namespace,
                name,
                location,
            });
        }

        Ok(found_result)
    }

    async fn query_manifest_for_namespace(&self, object_id: &str) -> Result<Option<NamespaceInfo>> {
        let filter = format!("object_id = '{}' AND object_type = 'namespace'", object_id);
        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id", "metadata"])
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;

        let mut found_result: Option<NamespaceInfo> = None;
        let mut total_rows = 0;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            total_rows += batch.num_rows();
            if total_rows > 1 {
                return Err(Error::Runtime {
                    message: format!(
                        "Expected exactly 1 namespace with id '{}', found {}",
                        object_id, total_rows
                    ),
                });
            }

            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            let metadata_array = Self::get_string_column(&batch, "metadata")?;

            let object_id_str = object_id_array.value(0);
            let metadata = if metadata_array.is_null(0) {
                None
            } else {
                let metadata_str = metadata_array.value(0);
                match serde_json::from_str::<HashMap<String, String>>(metadata_str) {
                    Ok(map) => Some(map),
                    Err(e) => {
                        return Err(Error::Runtime {
                            message: format!(
                                "Failed to deserialize metadata for namespace '{}': {}",
                                object_id, e
                            ),
                        });
                    }
                }
            };

            let (namespace, name) = Self::parse_object_id(object_id_str);
            found_result = Some(NamespaceInfo {
                namespace,
                name,
                metadata,
            });
        }

        Ok(found_result)
    }

    /// Insert an entry into the manifest table
    /// TODO: pending https://github.com/lancedb/lance/pull/4787 for true object_id uniqueness
    /// TODO: pending https://github.com/lancedb/lance/issues/4944 for not updating existing row
    async fn insert_into_manifest(
        &self,
        object_id: String,
        object_type: ObjectType,
        location: Option<String>,
        metadata: Option<String>,
        base_objects: Option<Vec<String>>,
    ) -> Result<()> {
        let dataset = &self.manifest_dataset;

        let schema = Self::manifest_schema();

        // Create base_objects array from the provided list
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(Arc::new(Field::new(
            "object_id",
            DataType::Utf8,
            true,
        )));

        match base_objects {
            Some(objects) => {
                for obj in objects {
                    list_builder.values().append_value(obj);
                }
                list_builder.append(true);
            }
            None => {
                list_builder.append_null();
            }
        }

        let base_objects_array = list_builder.finish();

        // Create arrays with optional values
        let location_array = match location {
            Some(loc) => Arc::new(StringArray::from(vec![Some(loc)])),
            None => Arc::new(StringArray::from(vec![None::<String>])),
        };

        let metadata_array = match metadata {
            Some(meta) => Arc::new(StringArray::from(vec![Some(meta)])),
            None => Arc::new(StringArray::from(vec![None::<String>])),
        };

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![object_id.as_str()])),
                Arc::new(StringArray::from(vec![object_type.as_str()])),
                location_array,
                metadata_array,
                Arc::new(base_objects_array),
            ],
        )
        .map_err(|e| Error::Runtime {
            message: format!("Failed to create manifest entry: {}", e),
        })?;

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        // Use MergeInsert to ensure uniqueness on object_id
        let dataset_guard = dataset.get().await?;
        let dataset_arc = Arc::new(dataset_guard.clone());
        drop(dataset_guard); // Drop read guard before merge insert

        let mut merge_builder =
            lance::dataset::MergeInsertBuilder::try_new(dataset_arc, vec!["object_id".to_string()])
                .map_err(|e| Error::Lance { source: e })?;

        // When matched: update all fields
        merge_builder.when_matched(lance::dataset::WhenMatched::UpdateAll);
        // When not matched: insert new row
        merge_builder.when_not_matched(lance::dataset::WhenNotMatched::InsertAll);

        let (new_dataset_arc, _merge_stats) = merge_builder
            .try_build()
            .map_err(|e| Error::Lance { source: e })?
            .execute_reader(reader)
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let new_dataset = Arc::try_unwrap(new_dataset_arc).unwrap_or_else(|arc| (*arc).clone());
        dataset.set_latest(new_dataset).await;

        if self.manifest_config.inline_optimization_enabled {
            if let Err(e) = self.optimize_manifest_table().await {
                log::warn!(
                    "Failed to optimize manifest table after insert: {:?}. This may impact query performance.",
                    e
                );
            }
        }

        Ok(())
    }

    pub async fn delete_from_manifest(&self, object_id: &str) -> Result<()> {
        let dataset = &self.manifest_dataset;

        {
            let predicate = format!("object_id = '{}'", object_id);
            let mut dataset_guard = dataset.get_mut().await?;
            dataset_guard
                .delete(&predicate)
                .await
                .map_err(|e| Error::Lance { source: e })?;
        } // Drop the guard here

        dataset.reload().await?;
        Ok(())
    }

    async fn optimize_manifest_table(&self) -> Result<()> {
        let dataset = &self.manifest_dataset;

        let dataset_guard = dataset.get().await?;
        let mut ds = (*dataset_guard).clone();
        drop(dataset_guard);

        if let Err(e) = compact_files(&mut ds, CompactionOptions::default(), None).await {
            log::warn!(
                "Failed to compact manifest table files: {:?}. This may impact query performance.",
                e
            );
        }

        if let Err(e) = ds.optimize_indices(&OptimizeOptions::default()).await {
            log::warn!("Failed to optimize manifest table indices: {:?}. This may impact query performance.", e);
        }

        dataset.set_latest(ds).await;
        Ok(())
    }

    /// Generate a new directory prefix in format: <hash>_<object_id>
    /// The hash is used to (1) optimize object store throughput,
    /// (2) have high enough entropy in a short period of time to prevent issues like
    /// failed table creation, delete and create new table of the same name, etc.
    /// The object_id is added after the hash to ensure
    /// dir name uniqueness and make debugging easier.
    fn generate_dir_name_prefix(object_id: &str) -> String {
        // Generate a random number for uniqueness
        let random_num: u64 = rand::rng().random();

        // Create hash from random number + object_id
        let mut hasher = DefaultHasher::new();
        random_num.hash(&mut hasher);
        object_id.hash(&mut hasher);
        let hash = hasher.finish();

        // Format as lowercase hex (8 characters - sufficient entropy for uniqueness)
        format!("{:08x}_{}", (hash & 0xFFFFFFFF) as u32, object_id)
    }

    fn build_object_uri(
        base_uri: &str,
        dir_name: &str,
        query_string: Option<&str>,
    ) -> Result<String> {
        let mut uri = base_uri.to_string();
        if !uri.ends_with('/') {
            uri.push('/');
        }

        uri.push_str(dir_name);

        if let Some(query) = query_string {
            uri.push('?');
            uri.push_str(query);
        }

        Ok(uri)
    }

    async fn validate_namespace_levels_exist(&self, namespace: &[String]) -> Result<()> {
        if !namespace.is_empty() {
            for i in 1..=namespace.len() {
                let ns_id = namespace[..i].join(DELIMITER);
                if !self.manifest_contains_object(&ns_id).await? {
                    return Err(Error::NamespaceNotFound { name: ns_id });
                }
            }
        }
        Ok(())
    }

    fn get_string_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a StringArray> {
        batch
            .column_by_name(column_name)
            .ok_or_else(|| Error::Runtime {
                message: format!("Missing {} column", column_name),
            })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Runtime {
                message: format!("Invalid {} type", column_name),
            })
    }

    #[allow(dead_code)]
    fn get_list_column<'a>(batch: &'a RecordBatch, column_name: &str) -> Result<&'a ListArray> {
        batch
            .column_by_name(column_name)
            .ok_or_else(|| Error::Runtime {
                message: format!("Missing {} column", column_name),
            })?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Error::Runtime {
                message: format!("Invalid {} type", column_name),
            })
    }

    /// Register a table in the manifest without creating the physical table
    /// TODO: this could be in public database API
    pub async fn register_table(
        &self,
        name: &str,
        namespace: &[String],
        location: String,
    ) -> Result<()> {
        validate_table_name(name)?;
        validate_namespace(namespace)?;
        self.validate_namespace_levels_exist(namespace).await?;

        let object_id = Self::build_object_id(namespace, name);
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::TableAlreadyExists {
                name: name.to_string(),
            });
        }

        self.insert_into_manifest(object_id, ObjectType::Table, Some(location), None, None)
            .await
    }

    /// List all tables in a namespace with their metadata
    /// TODO: this could be in public database API
    pub(crate) async fn list_tables(
        &self,
        request: crate::database::TableNamesRequest,
    ) -> Result<Vec<TableInfo>> {
        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let filter = if request.namespace.is_empty() {
            "object_type = 'table'".to_string()
        } else {
            let mut prefix = request.namespace.join(DELIMITER);
            prefix.push_str(DELIMITER);
            format!(
                "object_type = 'table' AND starts_with(object_id, '{}')",
                prefix
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id", "location"])
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;
        let mut results = Vec::new();

        for batch in batches {
            let object_id_array = Self::get_string_column(&batch, "object_id")?;
            let location_array = Self::get_string_column(&batch, "location")?;

            for i in 0..batch.num_rows() {
                let object_id = object_id_array.value(i);
                let location = location_array.value(i);

                // Parse object_id to extract namespace and name
                let (namespace, name) = Self::parse_object_id(object_id);

                // Check that it's a direct child by comparing namespace length
                // Direct child means namespace should match exactly the request namespace
                if namespace == request.namespace {
                    results.push(TableInfo {
                        namespace,
                        name,
                        location: location.to_string(),
                    });
                }
            }
        }

        // Sort by name and apply pagination
        apply_pagination_with_key(
            &mut results,
            request.start_after.as_deref(),
            request.limit,
            |t| t.name.as_str(),
        );

        Ok(results)
    }

    /// Describe a namespace, returning its metadata
    /// TODO: this could be in public database API
    pub async fn describe_namespace(&self, namespace: &[String]) -> Result<Option<NamespaceInfo>> {
        validate_namespace(namespace)?;
        // Validate parent namespaces exist (not the namespace being described itself)
        if namespace.len() > 1 {
            self.validate_namespace_levels_exist(&namespace[..namespace.len() - 1])
                .await?;
        }

        let object_id = namespace.join(DELIMITER);
        self.query_manifest_for_namespace(&object_id).await
    }
}

/// Database trait implementation for ManifestDatabase
///
/// This implementation ONLY uses the manifest dataset and does not fall back
/// to directory listing. The calling code (ListingDatabase) is responsible
/// for handling fallback to directory-based operations when needed.
#[async_trait::async_trait]
impl Database for ManifestDatabase {
    fn uri(&self) -> &str {
        &self.uri
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        if let Some(read_consistency_interval) = self.read_consistency_interval {
            if read_consistency_interval.is_zero() {
                Ok(ReadConsistency::Strong)
            } else {
                Ok(ReadConsistency::Eventual(read_consistency_interval))
            }
        } else {
            Ok(ReadConsistency::Manual)
        }
    }

    async fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<Vec<String>> {
        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let filter = if request.namespace.is_empty() {
            "object_type = 'namespace'".to_string()
        } else {
            let mut prefix = request.namespace.join(DELIMITER);
            prefix.push_str(DELIMITER);
            format!(
                "object_type = 'namespace' AND starts_with(object_id, '{}')",
                prefix
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id"])
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;
        let mut namespaces: Vec<String> = Vec::new();
        for batch in batches {
            let object_id_array = Self::get_string_column(&batch, "object_id")?;

            for i in 0..batch.num_rows() {
                let object_id = object_id_array.value(i);

                // Parse object_id to extract namespace and name
                let (namespace, name) = Self::parse_object_id(object_id);

                // Check that it's a direct child by comparing namespace length
                // Direct child means namespace should match exactly the request namespace
                if namespace == request.namespace {
                    namespaces.push(name);
                }
            }
        }

        apply_pagination(&mut namespaces, request.page_token, request.limit);
        Ok(namespaces)
    }

    async fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<()> {
        validate_namespace(request.namespace.as_slice())?;

        // Validate that parent namespaces exist (but not the namespace being created)
        if request.namespace.len() > 1 {
            self.validate_namespace_levels_exist(&request.namespace[..request.namespace.len() - 1])
                .await?;
        }

        let object_id = request.namespace.join(DELIMITER);
        if self.manifest_contains_object(&object_id).await? {
            return Err(Error::DatabaseAlreadyExists { name: object_id });
        }
        self.insert_into_manifest(object_id, ObjectType::Namespace, None, None, None)
            .await
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<()> {
        let object_id = request.namespace.join(DELIMITER);

        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let filter = format!("starts_with(object_id, '{}{}')", object_id, DELIMITER);
        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id"])
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .limit(Some(1), None)
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;

        for batch in batches {
            if batch.num_rows() > 0 {
                return Err(Error::Runtime {
                    message: format!("Namespace {} is not empty", object_id),
                });
            }
        }

        self.delete_from_manifest(&object_id).await
    }

    async fn table_names(
        &self,
        request: crate::database::TableNamesRequest,
    ) -> Result<Vec<String>> {
        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let filter = if request.namespace.is_empty() {
            "object_type = 'table'".to_string()
        } else {
            let mut prefix = request.namespace.join(DELIMITER);
            prefix.push_str(DELIMITER);
            format!(
                "object_type = 'table' AND starts_with(object_id, '{}')",
                prefix
            )
        };

        let mut scanner = self.manifest_scanner().await?;
        scanner
            .filter(&filter)
            .map_err(|e| Error::Lance { source: e })?;
        scanner
            .project(&["object_id"])
            .map_err(|e| Error::Lance { source: e })?;
        let batches = Self::execute_scanner(scanner).await?;

        let mut table_names: Vec<String> = Vec::new();
        for batch in batches {
            let object_id_array = Self::get_string_column(&batch, "object_id")?;

            for i in 0..batch.num_rows() {
                let object_id = object_id_array.value(i);

                // Parse object_id to extract namespace and name
                let (namespace, name) = Self::parse_object_id(object_id);

                // Check that it's a direct child by comparing namespace length
                // Direct child means namespace should match exactly the request namespace
                if namespace == request.namespace {
                    table_names.push(name);
                }
            }
        }

        apply_pagination(&mut table_names, request.start_after, request.limit);
        Ok(table_names)
    }

    async fn create_table(
        &self,
        request: crate::database::CreateTableRequest,
    ) -> Result<Arc<dyn BaseTable>> {
        validate_table_name(&request.name)?;
        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let object_id = Self::build_object_id(&request.namespace, &request.name);

        // do an early handling of existing table first by checking the manifest
        if self.manifest_contains_object(&object_id).await? {
            match request.mode {
                CreateTableMode::Create | CreateTableMode::ExistOk(_) => {
                    return ListingDatabase::handle_table_exists(
                        self,
                        &request.name,
                        request.namespace.clone(),
                        request.mode,
                        &request.data.schema(),
                    )
                    .await;
                }
                CreateTableMode::Overwrite => {}
            }
        }

        let table_dir_name = self.build_object_dir_name(
            &request.name,
            &request.namespace,
            ObjectType::Table,
            &object_id,
        );
        let table_uri =
            Self::build_object_uri(&self.uri, &table_dir_name, self.query_string.as_deref())?;

        let (storage_version_override, v2_manifest_override) =
            ListingDatabase::extract_storage_overrides(&request)?;

        let write_params = ListingDatabase::prepare_write_params(
            &request,
            storage_version_override,
            v2_manifest_override,
            &self.storage_options,
            &self.new_table_config,
            self.session.clone(),
        );

        let data_schema = request.data.arrow_schema();
        let create_result = NativeTable::create(
            &table_uri,
            &request.name,
            request.data,
            self.store_wrapper.clone(),
            Some(write_params),
            self.read_consistency_interval,
        )
        .await;

        if self.manifest_config.parent_dir_listing_enabled && request.namespace.is_empty() {
            match create_result {
                Ok(table) => {
                    let result = Arc::new(table);
                    // Best effort insert into manifest.
                    // If fails, to this ManifestDatabase, this table creation has failed.
                    // But to the caller ListingDatabase, it can just log a warning and succeed,
                    // and return the table that has been created here.
                    self.insert_into_manifest(
                        object_id,
                        ObjectType::Table,
                        Some(table_dir_name),
                        None,
                        None,
                    )
                    .await
                    .map_err(|e| Error::Manifest {
                        table: Some(result.clone()),
                        message: format!("Fail to insert into manifest after table created: {e:?}"),
                    })?;
                    Ok(result)
                }
                Err(Error::TableAlreadyExists { .. }) => {
                    ListingDatabase::handle_table_exists(
                        self,
                        &request.name,
                        request.namespace.clone(),
                        request.mode,
                        &data_schema,
                    )
                    .await
                }
                Err(err) => Err(err),
            }
        } else {
            // the table creation must succeed because concurrent creations
            // write to different directories with different hash names and we
            // already ensured the object id didn't exist above.
            let table = create_result?;

            match self
                .insert_into_manifest(
                    object_id,
                    ObjectType::Table,
                    Some(table_dir_name),
                    None,
                    None,
                )
                .await
            {
                Ok(_) => Ok(Arc::new(table)),
                Err(Error::Lance { source }) => match (request.mode, source) {
                    (CreateTableMode::ExistOk(callback), lance::Error::CommitConflict { .. }) => {
                        ListingDatabase::handle_table_exists(
                            self,
                            &request.name,
                            request.namespace.clone(),
                            CreateTableMode::ExistOk(callback),
                            &data_schema,
                        )
                        .await
                    }
                    _ => Err(Error::TableAlreadyExists {
                        name: request.name.clone(),
                    }),
                },
                Err(err) => Err(err),
            }
        }
    }

    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        validate_table_name(&request.target_table_name)?;
        validate_namespace(&request.target_namespace)?;
        self.validate_namespace_levels_exist(&request.target_namespace)
            .await?;

        // TODO: support deep clone
        if !request.is_shallow {
            return Err(Error::NotSupported {
                message: "Deep clone is not yet implemented".to_string(),
            });
        }

        let target_object_id =
            Self::build_object_id(&request.target_namespace, &request.target_table_name);

        // Check if target table already exists
        if self.manifest_contains_object(&target_object_id).await? {
            return Err(Error::TableAlreadyExists {
                name: request.target_table_name.clone(),
            });
        }

        let storage_params = ObjectStoreParams {
            storage_options: Some(self.storage_options.clone()),
            ..Default::default()
        };
        let read_params = ReadParams {
            store_options: Some(storage_params.clone()),
            session: Some(self.session.clone()),
            ..Default::default()
        };

        let mut source_dataset = DatasetBuilder::from_uri(&request.source_uri)
            .with_read_params(read_params.clone())
            .load()
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let version_ref = match (request.source_version, request.source_tag) {
            (Some(v), None) => Ok(Ref::Version(None, Some(v))),
            (None, Some(tag)) => Ok(Ref::Tag(tag)),
            (None, None) => Ok(Ref::Version(None, Some(source_dataset.version().version))),
            _ => Err(Error::InvalidInput {
                message: "Cannot specify both source_version and source_tag".to_string(),
            }),
        }?;

        let table_dir_name = self.build_object_dir_name(
            &request.target_table_name,
            &request.target_namespace,
            ObjectType::Table,
            &target_object_id,
        );
        let target_uri =
            Self::build_object_uri(&self.uri, &table_dir_name, self.query_string.as_deref())?;

        source_dataset
            .shallow_clone(&target_uri, version_ref, Some(storage_params))
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let cloned_table = NativeTable::open_with_params(
            &target_uri,
            &request.target_table_name,
            self.store_wrapper.clone(),
            None,
            self.read_consistency_interval,
        )
        .await?;

        if self.manifest_config.parent_dir_listing_enabled && request.target_namespace.is_empty() {
            let result = Arc::new(cloned_table);
            // Best effort insert into manifest.
            // If fails, to this ManifestDatabase, this table cloning has failed.
            // But to the caller ListingDatabase, it can just log a warning and succeed,
            // and return the table that has been cloned here.
            self.insert_into_manifest(
                target_object_id,
                ObjectType::Table,
                Some(table_dir_name),
                None,
                None,
            )
            .await
            .map_err(|e| Error::Manifest {
                table: Some(result.clone()),
                message: format!("Fail to insert into manifest after table cloned: {e:?}"),
            })?;
            Ok(result)
        } else {
            // The table cloning must succeed because concurrent clones
            // write to different directories with different hash names.
            match self
                .insert_into_manifest(
                    target_object_id,
                    ObjectType::Table,
                    Some(table_dir_name),
                    None,
                    None,
                )
                .await
            {
                Ok(_) => Ok(Arc::new(cloned_table)),
                Err(Error::Lance { source }) => match source {
                    lance::Error::CommitConflict { .. } => Err(Error::TableAlreadyExists {
                        name: request.target_table_name.clone(),
                    }),
                    _ => Err(Error::TableAlreadyExists {
                        name: request.target_table_name.clone(),
                    }),
                },
                Err(err) => Err(err),
            }
        }
    }

    async fn open_table(
        &self,
        mut request: crate::database::OpenTableRequest,
    ) -> Result<Arc<dyn crate::table::BaseTable>> {
        validate_table_name(&request.name)?;
        validate_namespace(&request.namespace)?;
        self.validate_namespace_levels_exist(&request.namespace)
            .await?;

        let object_id = Self::build_object_id(&request.namespace, &request.name);

        let table_info = self
            .query_manifest_for_table(&object_id)
            .await?
            .ok_or_else(|| Error::TableNotFound {
                name: request.name.clone(),
            })?;

        let table_uri = Self::build_object_uri(
            &self.uri,
            &table_info.location,
            self.query_string.as_deref(),
        )?;

        // Only modify the storage options if we actually have something to
        // inherit. There is a difference between storage_options=None and
        // storage_options=Some({}). Using storage_options=None will cause the
        // connection's session store registry to be used. Supplying Some({})
        // will cause a new connection to be created, and that connection will
        // be dropped from the cache when python GCs the table object, which
        // confounds reuse across tables.
        if !self.storage_options.is_empty() {
            let storage_options = request
                .lance_read_params
                .get_or_insert_with(Default::default)
                .store_options
                .get_or_insert_with(Default::default)
                .storage_options
                .get_or_insert_with(Default::default);
            ListingDatabase::inherit_storage_options(&self.storage_options, storage_options);
        }

        let mut read_params = request.lance_read_params.unwrap_or_else(|| {
            let mut default_params = ReadParams::default();
            if let Some(index_cache_size) = request.index_cache_size {
                #[allow(deprecated)]
                default_params.index_cache_size(index_cache_size as usize);
            }
            default_params
        });
        read_params.session(self.session.clone());

        let native_table = std::sync::Arc::new(
            crate::table::NativeTable::open_with_params(
                &table_uri,
                &request.name,
                self.store_wrapper.clone(),
                Some(read_params),
                self.read_consistency_interval,
            )
            .await?,
        );
        Ok(native_table)
    }

    async fn rename_table(
        &self,
        cur_name: &str,
        new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        validate_table_name(cur_name)?;
        validate_table_name(new_name)?;
        validate_namespace(cur_namespace)?;
        validate_namespace(new_namespace)?;
        self.validate_namespace_levels_exist(cur_namespace).await?;
        self.validate_namespace_levels_exist(new_namespace).await?;

        let cur_object_id = Self::build_object_id(cur_namespace, cur_name);
        let new_object_id = Self::build_object_id(new_namespace, new_name);

        // Check if source table exists
        let table_info = self
            .query_manifest_for_table(&cur_object_id)
            .await?
            .ok_or_else(|| Error::TableNotFound {
                name: cur_name.to_string(),
            })?;

        let location = table_info.location;

        // Check if target already exists
        if self.manifest_contains_object(&new_object_id).await? {
            return Err(Error::TableAlreadyExists {
                name: new_name.to_string(),
            });
        }

        // Check if target namespace exists (if not root)
        if !new_namespace.is_empty() {
            for i in 1..=new_namespace.len() {
                let ns_id = new_namespace[..i].join(DELIMITER);
                if i < new_namespace.len() && !self.manifest_contains_object(&ns_id).await? {
                    return Err(Error::NamespaceNotFound { name: ns_id });
                }
            }
        }

        // Generate new directory name (always use hash-based name for renamed tables)
        let new_dir_name = Self::generate_dir_name_prefix(&new_object_id);
        let new_uri =
            Self::build_object_uri(&self.uri, &new_dir_name, self.query_string.as_deref())?;

        // Build old URI from old directory name
        let old_uri = Self::build_object_uri(&self.uri, &location, self.query_string.as_deref())?;

        // For local filesystem, use simple directory rename/move
        // For remote object stores, we'd need to copy all objects
        use std::path::Path;
        let old_path = Path::new(&old_uri);
        let new_path = Path::new(&new_uri);

        // Check if old path exists
        if !old_path.exists() {
            return Err(Error::TableNotFound {
                name: cur_name.to_string(),
            });
        }

        // Try to rename/move the directory
        std::fs::rename(old_path, new_path).map_err(|e| Error::Runtime {
            message: format!("Failed to rename dataset directory: {}", e),
        })?;

        // Update manifest: delete old entry and insert new one with new directory name
        // TODO: perform this atomically requires batch commit transaction
        self.delete_from_manifest(&cur_object_id).await?;
        self.insert_into_manifest(
            new_object_id,
            ObjectType::Table,
            Some(new_dir_name),
            None,
            None,
        )
        .await
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        validate_table_name(name)?;
        validate_namespace(namespace)?;
        self.validate_namespace_levels_exist(namespace).await?;

        let object_id = Self::build_object_id(namespace, name);
        let table_info = self
            .query_manifest_for_table(&object_id)
            .await?
            .ok_or_else(|| Error::TableNotFound {
                name: name.to_string(),
            })?;

        let table_dir_name = table_info.location;

        // Remove from manifest first
        self.delete_from_manifest(&object_id).await?;

        // Best-effort delete data, log warning if fails
        let table_uri =
            Self::build_object_uri(&self.uri, &table_dir_name, self.query_string.as_deref())?;
        let object_store_params = ObjectStoreParams {
            storage_options: Some(self.storage_options.clone()),
            ..Default::default()
        };
        let (_, full_path) = ObjectStore::from_uri_and_params(
            self.session.store_registry(),
            &table_uri,
            &object_store_params,
        )
        .await?;

        let mut uri = self.uri.clone();
        if let Some(query_string) = &self.query_string {
            uri.push_str(&format!("?{}", query_string));
        }

        if let Ok(commit_handler) = commit_handler_from_url(&uri, &Some(object_store_params)).await
        {
            if let Err(e) = commit_handler.delete(&full_path).await {
                log::warn!(
                    "Failed to delete with commit handler for table '{}': {:?}",
                    name,
                    e
                );
            }
        } else {
            log::warn!("Failed to create commit handler for table '{}'", name);
        }

        if let Err(e) = self.object_store.remove_dir_all(full_path.clone()).await {
            log::warn!(
                "Failed to delete data directory for table '{}': {:?}",
                name,
                e
            );
        }

        Ok(())
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        validate_namespace(namespace)?;
        self.validate_namespace_levels_exist(namespace).await?;

        // Check that the namespace does not contain child namespaces
        let child_namespaces = self
            .list_namespaces(ListNamespacesRequest {
                namespace: namespace.to_vec(),
                page_token: None,
                limit: Some(1),
            })
            .await?;

        if !child_namespaces.is_empty() {
            return Err(Error::InvalidInput {
                message: format!(
                    "Cannot drop all tables in namespace '{}' because it contains child namespaces",
                    namespace.join(".")
                ),
            });
        }

        let tables = self
            .table_names(crate::database::TableNamesRequest {
                namespace: namespace.to_vec(),
                start_after: None,
                limit: None,
            })
            .await?;

        // Drop each table
        for table in tables {
            self.drop_table(&table, namespace).await?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Display for ManifestDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ManifestDatabase({})", self.uri)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{
        CreateNamespaceRequest, CreateTableData, CreateTableMode, CreateTableRequest, Database,
        DropNamespaceRequest, ListNamespacesRequest, OpenTableRequest, TableNamesRequest,
    };
    use crate::table::TableDefinition;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use rstest::rstest;
    use tempfile::tempdir;

    async fn setup_manifest_database(
        inline_optimization_enabled: bool,
    ) -> (tempfile::TempDir, ManifestDatabase) {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap().to_string();

        let config = ManifestDatabaseConfig {
            inline_optimization_enabled,
            ..Default::default()
        };

        let db = ManifestDatabase::new(
            uri,
            None,
            Arc::new(Session::default()),
            config,
            Arc::new(ObjectStore::local()),
            HashMap::new(),
            NewTableConfig::default(),
            None,
            None,
        )
        .await
        .unwrap();

        (tempdir, db)
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cannot_create_table_with_namespace_name(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Try to create a table in root with the same name as the namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let result = db
            .create_table(CreateTableRequest {
                name: "ns1".to_string(),
                namespace: vec![],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await;

        // Should fail because "ns1" already exists as a namespace
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::TableAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cannot_create_namespace_with_table_name(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create table in root
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Try to create a namespace with the same name
        let result = db
            .create_namespace(CreateNamespaceRequest {
                namespace: vec!["table1".to_string()],
            })
            .await;

        // Should fail because "table1" already exists as a table
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::DatabaseAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_namespace_create_and_list(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create a namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // List namespaces
        let namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec![],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(namespaces, vec!["ns1"]);

        // Create nested namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string(), "ns2".to_string()],
        })
        .await
        .unwrap();

        // List nested namespaces
        let nested_namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec!["ns1".to_string()],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(nested_namespaces, vec!["ns2"]);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_describe_namespace(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Describe non-existent namespace
        let result = db
            .describe_namespace(&["nonexistent".to_string()])
            .await
            .unwrap();
        assert!(result.is_none());

        // Create a namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Describe the namespace
        let namespace_info = db
            .describe_namespace(&["ns1".to_string()])
            .await
            .unwrap()
            .expect("Namespace should exist");

        assert_eq!(namespace_info.namespace, Vec::<String>::new());
        assert_eq!(namespace_info.name, "ns1");
        assert!(namespace_info.metadata.is_none());

        // Create nested namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string(), "ns2".to_string()],
        })
        .await
        .unwrap();

        // Describe nested namespace
        let nested_info = db
            .describe_namespace(&["ns1".to_string(), "ns2".to_string()])
            .await
            .unwrap()
            .expect("Nested namespace should exist");

        assert_eq!(nested_info.namespace, vec!["ns1".to_string()]);
        assert_eq!(nested_info.name, "ns2");
        assert!(nested_info.metadata.is_none());
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_namespace_drop(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Drop namespace
        db.drop_namespace(DropNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Verify it's gone
        let namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec![],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert!(namespaces.is_empty());
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_namespace_cannot_drop_with_tables(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create table in namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Try to drop namespace (should fail)
        let result = db
            .drop_namespace(DropNamespaceRequest {
                namespace: vec!["ns1".to_string()],
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cannot_create_table_in_nonexistent_namespace(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Try to create table in non-existent namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let result = db
            .create_table(CreateTableRequest {
                name: "table1".to_string(),
                namespace: vec!["nonexistent".to_string()],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await;

        // Should fail because namespace doesn't exist
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NamespaceNotFound { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cannot_create_child_namespace_without_parent(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Try to create nested namespace without creating parent first
        let result = db
            .create_namespace(CreateNamespaceRequest {
                namespace: vec!["parent".to_string(), "child".to_string()],
            })
            .await;

        // Should fail because parent namespace doesn't exist
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NamespaceNotFound { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_create_deeply_nested_namespaces(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create parent namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["level1".to_string()],
        })
        .await
        .unwrap();

        // Create child namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["level1".to_string(), "level2".to_string()],
        })
        .await
        .unwrap();

        // Create grandchild namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec![
                "level1".to_string(),
                "level2".to_string(),
                "level3".to_string(),
            ],
        })
        .await
        .unwrap();

        // Verify all levels exist
        let root_namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec![],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(root_namespaces, vec!["level1"]);

        let level1_namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec!["level1".to_string()],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(level1_namespaces, vec!["level2"]);

        let level2_namespaces = db
            .list_namespaces(ListNamespacesRequest {
                namespace: vec!["level1".to_string(), "level2".to_string()],
                page_token: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(level2_namespaces, vec!["level3"]);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_table_in_namespace(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create table in namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // List tables in namespace
        let tables = db
            .table_names(TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(tables, vec!["table1"]);

        // Open table from namespace
        let table = db
            .open_table(OpenTableRequest {
                name: "table1".to_string(),
                namespace: vec!["ns1".to_string()],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.schema().await.unwrap().fields().len(), 1);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_rename_table_in_root(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Rename table
        db.rename_table("table1", "table2", &[], &[]).await.unwrap();

        // Verify old name doesn't exist
        let result = db
            .open_table(OpenTableRequest {
                name: "table1".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(result.is_err());

        // Verify new name exists
        let table = db
            .open_table(OpenTableRequest {
                name: "table2".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.schema().await.unwrap().fields().len(), 1);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_rename_table_to_namespace(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create table in root
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Rename table to namespace
        db.rename_table("table1", "table2", &[], &["ns1".to_string()])
            .await
            .unwrap();

        // Verify old location doesn't exist
        let result = db
            .open_table(OpenTableRequest {
                name: "table1".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(result.is_err());

        // Verify new location exists
        let table = db
            .open_table(OpenTableRequest {
                name: "table2".to_string(),
                namespace: vec!["ns1".to_string()],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.schema().await.unwrap().fields().len(), 1);
    }

    #[tokio::test]
    async fn test_manifest_table_inline_optimization() {
        let (_tempdir, db) = setup_manifest_database(true).await;

        // Create a namespace to trigger manifest updates
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["test_ns".to_string()],
        })
        .await
        .unwrap();

        // Create a table to trigger manifest updates
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as _],
        )
        .unwrap()];

        let reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema.clone(),
        ));

        db.create_table(CreateTableRequest {
            namespace: vec!["test_ns".to_string()],
            name: "test_table".to_string(),
            mode: CreateTableMode::Create,
            data: CreateTableData::Data(reader),
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Verify that manifest table exists and has expected indexes
        let dataset = db.manifest_dataset.get().await.unwrap();
        let indices = dataset.load_indices().await.unwrap();

        let has_object_id_btree = indices.iter().any(|idx| idx.name == "object_id_btree");
        let has_object_type_bitmap = indices.iter().any(|idx| idx.name == "object_type_bitmap");
        let has_base_objects_label_list = indices
            .iter()
            .any(|idx| idx.name == "base_objects_label_list");

        assert!(
            has_object_id_btree,
            "Manifest table should have object_id_btree index after optimization"
        );
        assert!(
            has_object_type_bitmap,
            "Manifest table should have object_type_bitmap index after optimization"
        );
        assert!(
            has_base_objects_label_list,
            "Manifest table should have base_objects_label_list index after optimization"
        );

        // Verify that all indexes cover all fragments (no unindexed rows)
        for index_name in [
            "object_id_btree",
            "object_type_bitmap",
            "base_objects_label_list",
        ] {
            let stats_json = dataset.index_statistics(index_name).await.unwrap();
            let stats: serde_json::Value = serde_json::from_str(&stats_json).unwrap();
            let num_unindexed_rows = stats["num_unindexed_rows"].as_u64().unwrap();
            assert_eq!(
                num_unindexed_rows, 0,
                "Index '{}' should cover all fragments (no unindexed rows), but has {} unindexed rows",
                index_name, num_unindexed_rows
            );
        }
    }

    #[test]
    fn test_generate_hash_path_format() {
        // Generate a few directory names and verify format: <hash>_<tablename>
        for i in 0..10 {
            let table_name = format!("test_table_{}", i);
            let object_id = format!("namespace$test_table_{}", i);
            let dir_name = ManifestDatabase::generate_dir_name_prefix(&object_id);

            // Should NOT end with .lance
            assert!(!dir_name.ends_with(".lance"));

            // Should contain underscore separator
            assert!(dir_name.contains('_'));

            // Should end with the table name
            assert!(
                dir_name.ends_with(&table_name),
                "Directory name should end with table name, got: {}",
                dir_name
            );

            // Hash part should be 8 characters (hex format of u32)
            let parts: Vec<&str> = dir_name.splitn(2, '_').collect();
            assert_eq!(parts.len(), 2, "Should have hash and table name parts");
            assert_eq!(parts[0].len(), 8, "Hash should be 8 hex characters");
            assert!(
                parts[0].chars().all(|c| c.is_ascii_hexdigit()),
                "Hash should be hexadecimal"
            );
        }
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_basic(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "source_table".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema.clone())),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Get the source table directory name from manifest and convert to full URI
        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source_table")
            .unwrap();
        let dir_name = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), dir_name);

        // Clone the table to the same namespace
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_table".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify both tables exist
        let table_names = db
            .table_names(TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(table_names.contains(&"source_table".to_string()));
        assert!(table_names.contains(&"cloned_table".to_string()));

        // Verify schemas match
        assert_eq!(
            source_table.schema().await.unwrap(),
            cloned_table.schema().await.unwrap()
        );
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_data(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with actual data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "source_with_data".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Get source URI
        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source_with_data")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone the table
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_with_data".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify data counts match
        let source_count = source_table.count_rows(None).await.unwrap();
        let cloned_count = cloned_table.count_rows(None).await.unwrap();
        assert_eq!(source_count, cloned_count);
        assert_eq!(source_count, 3);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_deep_not_supported(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Try deep clone (should fail)
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: false, // Request deep clone
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NotSupported { message } if message.contains("Deep clone")
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_namespace_not_found(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Try clone to non-existent namespace (should fail)
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["nonexistent".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NamespaceNotFound { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_invalid_target_name(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Try clone with invalid target name
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "invalid/name".to_string(), // Invalid name with slash
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_source_not_found(#[case] inline_optimization_enabled: bool) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Try to clone from non-existent source
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri: "/nonexistent/table.lance".to_string(),
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_version_and_tag_error(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Try clone with both version and tag (should fail)
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: Some(1),
                source_tag: Some("v1.0".to_string()),
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidInput { message } if message.contains("Cannot specify both source_version and source_tag")
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_target_already_exists(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema.clone())),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Create target table (that will conflict)
        db.create_table(CreateTableRequest {
            name: "target".to_string(),
            namespace: vec!["ns1".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Try clone to existing table (should fail)
        let result = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "target".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::TableAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_to_root_namespace(#[case] inline_optimization_enabled: bool) {
        let (tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table in namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "source".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone to root namespace
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_in_root".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify both exist
        let ns_tables = db
            .table_names(TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(ns_tables.contains(&"source".to_string()));

        let root_tables = db
            .table_names(TableNamesRequest {
                namespace: vec![],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(root_tables.contains(&"cloned_in_root".to_string()));

        // Verify schemas match
        assert_eq!(
            source_table.schema().await.unwrap(),
            cloned_table.schema().await.unwrap()
        );
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_specific_version(#[case] inline_optimization_enabled: bool) {
        use crate::table::Table;

        let (tempdir, db_raw) = setup_manifest_database(inline_optimization_enabled).await;
        let db = Arc::new(db_raw);
        let db_clone = db.clone();

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch1)], schema.clone()));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "versioned_source".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Get the initial version
        let initial_version = source_table.version().await.unwrap();

        // Add more data to create a new version
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(arrow_array::StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone(), db_clone.clone());
        source_table_obj
            .add(Box::new(RecordBatchIterator::new(
                vec![Ok(batch2)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Verify source table now has 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "versioned_source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone from the initial version (should have only 2 rows)
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_from_version".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: Some(initial_version),
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify cloned table has only the initial 2 rows
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);

        // Source table should still have 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_tag(#[case] inline_optimization_enabled: bool) {
        use crate::table::Table;

        let (tempdir, db_raw) = setup_manifest_database(inline_optimization_enabled).await;
        let db = Arc::new(db_raw);
        let db_clone = db.clone();

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch1)], schema.clone()));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "tagged_source".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a tag for the current version
        let source_table_obj = Table::new(source_table.clone(), db_clone.clone());
        let mut tags = source_table_obj.tags().await.unwrap();
        tags.create("v1.0", source_table.version().await.unwrap())
            .await
            .unwrap();

        // Add more data after the tag
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(arrow_array::StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone(), db_clone.clone());
        source_table_obj
            .add(Box::new(RecordBatchIterator::new(
                vec![Ok(batch2)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Source table should have 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "tagged_source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone from the tag (should have only 2 rows)
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_from_tag".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: Some("v1.0".to_string()),
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify cloned table has only the tagged version's 2 rows
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cloned_tables_evolve_independently(#[case] inline_optimization_enabled: bool) {
        use crate::table::Table;

        let (tempdir, db_raw) = setup_manifest_database(inline_optimization_enabled).await;
        let db = Arc::new(db_raw);
        let db_clone = db.clone();

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch1)], schema.clone()));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "independent_source".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "independent_source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone the table
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "independent_clone".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Both should start with 2 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 2);
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);

        // Add data to the cloned table
        let batch_clone = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4, 5])),
                Arc::new(arrow_array::StringArray::from(vec!["c", "d", "e"])),
            ],
        )
        .unwrap();

        let cloned_table_obj = Table::new(cloned_table.clone(), db_clone.clone());
        cloned_table_obj
            .add(Box::new(RecordBatchIterator::new(
                vec![Ok(batch_clone)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Add different data to the source table
        let batch_source = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 11])),
                Arc::new(arrow_array::StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone(), db_clone.clone());
        source_table_obj
            .add(Box::new(RecordBatchIterator::new(
                vec![Ok(batch_source)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Verify they have evolved independently
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4); // 2 + 2
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 5); // 2 + 3
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_latest_version(#[case] inline_optimization_enabled: bool) {
        use crate::table::Table;

        let (tempdir, db_raw) = setup_manifest_database(inline_optimization_enabled).await;
        let db = Arc::new(db_raw);
        let db_clone = db.clone();

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["ns1".to_string()],
        })
        .await
        .unwrap();

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch1)], schema.clone()));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "latest_version_source".to_string(),
                namespace: vec!["ns1".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Add more data to create new versions
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
            )
            .unwrap();

            let source_table_obj = Table::new(source_table.clone(), db_clone.clone());
            source_table_obj
                .add(Box::new(RecordBatchIterator::new(
                    vec![Ok(batch)],
                    schema.clone(),
                )))
                .execute()
                .await
                .unwrap();
        }

        // Source should have 8 rows total (2 + 2 + 2 + 2)
        let source_count = source_table.count_rows(None).await.unwrap();
        assert_eq!(source_count, 8);

        let tables = db
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["ns1".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["ns1".to_string()] && t.name == "latest_version_source")
            .unwrap();
        let location = &table_info.location;
        // Convert directory name to full URI
        let source_uri = format!("{}/{}", tempdir.path().to_str().unwrap(), location);

        // Clone without specifying version or tag (should get latest)
        let cloned_table = db
            .clone_table(crate::database::CloneTableRequest {
                target_table_name: "cloned_latest".to_string(),
                target_namespace: vec!["ns1".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Cloned table should have all 8 rows from the latest version
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 8);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_parent_dir_listing_disabled_root_namespace_uses_uuid(
        #[case] inline_optimization_enabled: bool,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with dir_listing_enabled=false
        let mut options = std::collections::HashMap::new();
        options.insert("manifest_enabled".to_string(), "true".to_string());
        options.insert("dir_listing_enabled".to_string(), "false".to_string());
        options.insert(
            "manifest_inline_optimization_enabled".to_string(),
            inline_optimization_enabled.to_string(),
        );

        let request = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options,
            read_consistency_interval: None,
            session: None,
        };

        let db = super::super::ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        // Create a table in root namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        db.create_table(CreateTableRequest {
            name: "test_table".to_string(),
            namespace: vec![],
            data: CreateTableData::Data(reader),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Check that the table location does NOT have .lance extension
        let tables = db
            .manifest_db
            .as_ref()
            .unwrap()
            .list_tables(crate::database::TableNamesRequest::default())
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace.is_empty() && t.name == "test_table")
            .unwrap();

        assert_eq!(table_info.name, "test_table");
        assert!(
            !table_info.location.ends_with(".lance"),
            "Expected UUID without .lance extension, got: {}",
            table_info.location
        );

        // location is already just the directory name (no path prefix)
        let filename = &table_info.location;

        // Verify the filename has format: <8hex>_<tablename>
        assert!(
            filename.contains('_'),
            "Filename should contain underscore separator"
        );
        assert!(
            filename.ends_with("test_table"),
            "Filename should end with table name"
        );
        let parts: Vec<&str> = filename.splitn(2, '_').collect();
        assert_eq!(parts.len(), 2, "Should have hash and table name parts");
        assert_eq!(parts[0].len(), 8, "Hash should be 8 hex characters");
        assert!(
            parts[0].chars().all(|c| c.is_ascii_hexdigit()),
            "Expected base32 characters only, got: {}",
            filename
        );
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_parent_dir_listing_disabled_namespaced_uses_uuid(
        #[case] inline_optimization_enabled: bool,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with dir_listing_enabled=false
        let mut options = std::collections::HashMap::new();
        options.insert("manifest_enabled".to_string(), "true".to_string());
        options.insert("dir_listing_enabled".to_string(), "false".to_string());
        options.insert(
            "manifest_inline_optimization_enabled".to_string(),
            inline_optimization_enabled.to_string(),
        );

        let request = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options,
            read_consistency_interval: None,
            session: None,
        };

        let db = super::super::ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        // Create namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["myspace".to_string()],
        })
        .await
        .unwrap();

        // Create a table in namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        db.create_table(CreateTableRequest {
            name: "namespaced_table".to_string(),
            namespace: vec!["myspace".to_string()],
            data: CreateTableData::Data(reader),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Check that the table location does NOT have .lance extension
        let tables = db
            .manifest_db
            .as_ref()
            .unwrap()
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["myspace".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        let table_info = tables
            .iter()
            .find(|t| t.namespace == vec!["myspace".to_string()] && t.name == "namespaced_table")
            .unwrap();
        let ns = &table_info.namespace;
        let name = &table_info.name;
        let location = &table_info.location;

        assert_eq!(ns, &vec!["myspace".to_string()]);
        assert_eq!(name, "namespaced_table");
        assert!(
            !location.ends_with(".lance"),
            "Expected UUID without .lance extension, got: {}",
            location
        );

        // location is already just the directory name (no path prefix)
        let filename = &table_info.location;

        // Verify the filename has format: <8hex>_<tablename>
        assert!(
            filename.contains('_'),
            "Filename should contain underscore separator"
        );
        assert!(
            filename.ends_with("namespaced_table"),
            "Filename should end with table name"
        );
        let parts: Vec<&str> = filename.splitn(2, '_').collect();
        assert_eq!(parts.len(), 2, "Should have hash and table name parts");
        assert_eq!(parts[0].len(), 8, "Hash should be 8 hex characters");
        assert!(
            parts[0].chars().all(|c| c.is_ascii_hexdigit()),
            "Hash should be hexadecimal, got: {}",
            filename
        );
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_parent_dir_listing_disabled_old_clients_cannot_see(
        #[case] inline_optimization_enabled: bool,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with dir_listing_enabled=false (new client)
        let mut options_new = std::collections::HashMap::new();
        options_new.insert("manifest_enabled".to_string(), "true".to_string());
        options_new.insert("dir_listing_enabled".to_string(), "false".to_string());
        options_new.insert(
            "manifest_inline_optimization_enabled".to_string(),
            inline_optimization_enabled.to_string(),
        );

        let request_new = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_new,
            read_consistency_interval: None,
            session: None,
        };

        let db_new = super::super::ListingDatabase::connect_with_options(&request_new)
            .await
            .unwrap();

        // Create a table with new client
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        db_new
            .create_table(CreateTableRequest {
                name: "hidden_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create old client (manifest disabled)
        let mut options_old = std::collections::HashMap::new();
        options_old.insert("manifest_enabled".to_string(), "false".to_string());

        let request_old = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_old,
            read_consistency_interval: None,
            session: None,
        };

        let db_old = super::super::ListingDatabase::connect_with_options(&request_old)
            .await
            .unwrap();

        // Old client should NOT see the table (no .lance extension)
        let table_names = db_old
            .table_names(super::super::TableNamesRequest::default())
            .await
            .unwrap();
        assert!(
            !table_names.contains(&"hidden_table".to_string()),
            "Old client should not see table without .lance extension"
        );

        // Old client should NOT be able to open the table
        let result = db_old
            .open_table(super::super::OpenTableRequest {
                name: "hidden_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(
            result.is_err(),
            "Old client should not be able to open hidden table"
        );
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_parent_dir_listing_enabled_backward_compatible(
        #[case] inline_optimization_enabled: bool,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with dir_listing_enabled=true (default, backward compatible)
        let mut options_new = std::collections::HashMap::new();
        options_new.insert("manifest_enabled".to_string(), "true".to_string());
        options_new.insert("dir_listing_enabled".to_string(), "true".to_string());
        options_new.insert(
            "manifest_inline_optimization_enabled".to_string(),
            inline_optimization_enabled.to_string(),
        );

        let request_new = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_new,
            read_consistency_interval: None,
            session: None,
        };

        let db_new = super::super::ListingDatabase::connect_with_options(&request_new)
            .await
            .unwrap();

        // Create a table in root namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new(
            vec![Ok(batch.clone())],
            schema.clone(),
        ));

        db_new
            .create_table(CreateTableRequest {
                name: "root_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create namespace and table in namespace
        db_new
            .create_namespace(CreateNamespaceRequest {
                namespace: vec!["myspace".to_string()],
            })
            .await
            .unwrap();

        let reader2 = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        db_new
            .create_table(CreateTableRequest {
                name: "ns_table".to_string(),
                namespace: vec!["myspace".to_string()],
                data: CreateTableData::Data(reader2),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Check locations
        // Get root namespace tables
        let root_tables = db_new
            .manifest_db
            .as_ref()
            .unwrap()
            .list_tables(crate::database::TableNamesRequest::default())
            .await
            .unwrap();

        // Root namespace table should use table_name.lance directory name (backward compatible)
        let root_table_info = root_tables
            .iter()
            .find(|t| t.namespace.is_empty() && t.name == "root_table")
            .unwrap();
        assert_eq!(root_table_info.name, "root_table");
        assert_eq!(
            root_table_info.location, "root_table.lance",
            "Root table location should be table_name.lance directory name for backward compatibility, got: {}",
            root_table_info.location
        );

        // Get myspace namespace tables
        let myspace_tables = db_new
            .manifest_db
            .as_ref()
            .unwrap()
            .list_tables(crate::database::TableNamesRequest {
                namespace: vec!["myspace".to_string()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Namespaced table should use UUID without .lance (old clients don't support namespaces anyway)
        let table_info = myspace_tables
            .iter()
            .find(|t| t.namespace == vec!["myspace".to_string()] && t.name == "ns_table")
            .unwrap();
        let ns = &table_info.namespace;
        let name = &table_info.name;
        let location = &table_info.location;
        assert_eq!(ns, &vec!["myspace".to_string()]);
        assert_eq!(name, "ns_table");
        assert!(
            !location.ends_with(".lance"),
            "Namespaced table location should NOT have .lance extension (old clients don't support namespaces), got: {}",
            location
        );
        // location is already just the directory name (no path prefix)
        // Verify the filename has format: <8hex>_<tablename>
        let filename = location;
        assert!(
            filename.contains('_'),
            "Filename should contain underscore separator"
        );
        assert!(
            filename.ends_with("ns_table"),
            "Filename should end with table name"
        );
        let parts: Vec<&str> = filename.splitn(2, '_').collect();
        assert_eq!(parts.len(), 2, "Should have hash and table name parts");
        assert_eq!(parts[0].len(), 8, "Hash should be 8 hex characters");

        // Old client should be able to see root table
        let mut options_old = std::collections::HashMap::new();
        options_old.insert("manifest_enabled".to_string(), "false".to_string());

        let request_old = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_old,
            read_consistency_interval: None,
            session: None,
        };

        let db_old = super::super::ListingDatabase::connect_with_options(&request_old)
            .await
            .unwrap();

        let table_names = db_old
            .table_names(super::super::TableNamesRequest::default())
            .await
            .unwrap();
        assert!(
            table_names.contains(&"root_table".to_string()),
            "Old client should see root table with .lance extension"
        );

        // Old client should be able to open the root table
        let table = db_old
            .open_table(super::super::OpenTableRequest {
                name: "root_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_parent_dir_listing_new_manifest_client_can_access_hidden_tables(
        #[case] inline_optimization_enabled: bool,
    ) {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with dir_listing_enabled=false
        let mut options = std::collections::HashMap::new();
        options.insert("manifest_enabled".to_string(), "true".to_string());
        options.insert("dir_listing_enabled".to_string(), "false".to_string());
        options.insert(
            "manifest_inline_optimization_enabled".to_string(),
            inline_optimization_enabled.to_string(),
        );

        let request = crate::connection::ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options,
            read_consistency_interval: None,
            session: None,
        };

        let db = super::super::ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        // Create a table (will be hidden from old clients)
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        db.create_table(CreateTableRequest {
            name: "hidden_table".to_string(),
            namespace: vec![],
            data: CreateTableData::Data(reader),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // New manifest client should be able to list the table
        let table_names = db
            .table_names(super::super::TableNamesRequest::default())
            .await
            .unwrap();
        assert!(
            table_names.contains(&"hidden_table".to_string()),
            "New manifest client should see hidden table"
        );

        // New manifest client should be able to open the table
        let table = db
            .open_table(super::super::OpenTableRequest {
                name: "hidden_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        // Verify the table can be queried and operated on correctly
        let row_count = table.count_rows(None).await.unwrap();
        assert_eq!(
            row_count, 3,
            "New manifest client should be able to count rows"
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn test_drop_all_tables_fails_with_child_namespaces(
        #[case] inline_optimization_enabled: bool,
    ) {
        let (_tempdir, db) = setup_manifest_database(inline_optimization_enabled).await;

        // Create parent namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["parent".to_string()],
        })
        .await
        .unwrap();

        // Create child namespace
        db.create_namespace(CreateNamespaceRequest {
            namespace: vec!["parent".to_string(), "child".to_string()],
        })
        .await
        .unwrap();

        // Create a table in parent namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        db.create_table(CreateTableRequest {
            name: "table1".to_string(),
            namespace: vec!["parent".to_string()],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Try to drop all tables in parent namespace (should fail because it has a child namespace)
        let result = db.drop_all_tables(&["parent".to_string()]).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidInput { .. }));

        // Verify the table still exists
        let tables = db
            .table_names(TableNamesRequest {
                namespace: vec!["parent".to_string()],
                start_after: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "table1");
    }

    #[tokio::test]
    async fn test_drop_all_tables_with_memory_storage() {
        // Test that drop_all_tables works correctly with memory:// storage
        // and allows recreating tables with the same name
        let uri = "memory://test_memory".to_string();

        let config = ManifestDatabaseConfig {
            inline_optimization_enabled: false,
            ..Default::default()
        };

        let db = ManifestDatabase::new(
            uri,
            None,
            Arc::new(Session::default()),
            config,
            Arc::new(ObjectStore::memory()),
            HashMap::new(),
            NewTableConfig::default(),
            None,
            None,
        )
        .await
        .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Create a table
        db.create_table(CreateTableRequest {
            name: "test_table".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema.clone())),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        // Verify table exists
        let tables = db
            .table_names(TableNamesRequest {
                namespace: vec![],
                start_after: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "test_table");

        // Drop all tables
        db.drop_all_tables(&[]).await.unwrap();

        // Verify table is gone from manifest
        let tables = db
            .table_names(TableNamesRequest {
                namespace: vec![],
                start_after: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(tables.len(), 0);

        // Create a new database instance with a fresh Session to clear metadata cache
        // This is necessary because the Session caches dataset metadata by URI,
        // and after deletion the cache still has entries even though files are deleted
        let uri = "memory://test_memory".to_string();
        let config = ManifestDatabaseConfig {
            inline_optimization_enabled: false,
            ..Default::default()
        };
        let db = ManifestDatabase::new(
            uri,
            None,
            Arc::new(Session::default()), // Fresh session
            config,
            Arc::new(ObjectStore::memory()),
            HashMap::new(),
            NewTableConfig::default(),
            None,
            None,
        )
        .await
        .unwrap();

        // Try to create table with same name again - this should succeed
        let result = db
            .create_table(CreateTableRequest {
                name: "test_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await;

        assert!(
            result.is_ok(),
            "Should be able to recreate table after drop_all_tables, but got error: {:?}",
            result.err()
        );

        // Verify table exists again
        let tables = db
            .table_names(TableNamesRequest {
                namespace: vec![],
                start_after: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "test_table");
    }
}
