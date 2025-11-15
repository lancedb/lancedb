// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace-based database implementation that delegates table management to lance-namespace

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::types::{Float32Type, UInt8Type};
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use lance::dataset::scanner::Scanner;
use lance_namespace::{
    models::{
        CreateEmptyTableRequest, CreateNamespaceRequest, CreateNamespaceResponse,
        DescribeTableRequest, DropNamespaceRequest, DropNamespaceResponse, DropTableRequest,
        ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    },
    LanceNamespace,
};
use lance_namespace_impls::ConnectBuilder;

use crate::database::ReadConsistency;
use crate::error::{Error, Result};
use crate::query::{QueryFilter, Select, VectorQueryRequest, DEFAULT_TOP_K};
use crate::table::{AnyQuery, Table};
use crate::utils::default_vector_column;

use super::{
    BaseTable, CloneTableRequest, CreateTableMode, CreateTableRequest as DbCreateTableRequest,
    Database, OpenTableRequest, TableNamesRequest,
};

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
}

impl LanceNamespaceDatabase {
    pub async fn connect(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
    ) -> Result<Self> {
        let mut builder = ConnectBuilder::new(ns_impl);
        for (key, value) in ns_properties.clone() {
            builder = builder.property(key, value);
        }
        if let Some(ref sess) = session {
            builder = builder.session(sess.clone());
        }
        let namespace = builder.connect().await.map_err(|e| Error::InvalidInput {
            message: format!("Failed to connect to namespace: {:?}", e),
        })?;

        Ok(Self {
            namespace,
            storage_options,
            read_consistency_interval,
            session,
            uri: format!("namespace://{}", ns_impl),
        })
    }
}

impl std::fmt::Debug for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceNamespaceDatabase")
            .field("storage_options", &self.storage_options)
            .field("read_consistency_interval", &self.read_consistency_interval)
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
        self.namespace
            .list_namespaces(request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to list namespaces: {}", e),
            })
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        self.namespace
            .create_namespace(request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create namespace: {}", e),
            })
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        self.namespace
            .drop_namespace(request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to drop namespace: {}", e),
            })
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let ns_request = ListTablesRequest {
            id: Some(request.namespace),
            page_token: request.start_after,
            limit: request.limit.map(|l| l as i32),
        };

        let response =
            self.namespace
                .list_tables(ns_request)
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to list tables: {}", e),
                })?;

        Ok(response.tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.namespace
            .list_tables(request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to list tables: {}", e),
            })
    }

    async fn create_table(&self, request: DbCreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        let describe_request = DescribeTableRequest {
            id: Some(table_id.clone()),
            version: None,
        };

        let describe_result = self.namespace.describe_table(describe_request).await;

        match request.mode {
            CreateTableMode::Create => {
                if describe_result.is_ok() {
                    return Err(Error::TableAlreadyExists {
                        name: request.name.clone(),
                    });
                }
            }
            CreateTableMode::Overwrite => {
                if describe_result.is_ok() {
                    // Drop the existing table - must succeed
                    let drop_request = DropTableRequest {
                        id: Some(table_id.clone()),
                    };
                    self.namespace
                        .drop_table(drop_request)
                        .await
                        .map_err(|e| Error::Runtime {
                            message: format!("Failed to drop existing table for overwrite: {}", e),
                        })?;
                }
            }
            CreateTableMode::ExistOk(_) => {
                if describe_result.is_ok() {
                    // Table already exists, open it using NamespaceTable
                    use lance::dataset::ReadParams;

                    // Build read params with session, matching ListingDatabase behavior
                    let mut read_params = ReadParams::default();
                    if let Some(ref sess) = self.session {
                        read_params.session(sess.clone());
                    }

                    let namespace_table = NamespaceTable::open(
                        self.namespace.clone(),
                        table_id.clone(),
                        Some(read_params),
                        self.read_consistency_interval,
                    )
                    .await?;

                    return Ok(Arc::new(namespace_table));
                }
            }
        }

        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        let create_empty_request = CreateEmptyTableRequest {
            id: Some(table_id.clone()),
            location: None,
            properties: if self.storage_options.is_empty() {
                None
            } else {
                Some(self.storage_options.clone())
            },
        };

        let create_empty_response = self
            .namespace
            .create_empty_table(create_empty_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create empty table: {}", e),
            })?;

        let location = create_empty_response
            .location
            .ok_or_else(|| Error::Runtime {
                message: "Table location is missing from create_empty_table response".to_string(),
            })?;

        // For manifest-backed tables, the location IS the full table URI (no .lance suffix)
        // We need to create the table directly at this location using NamespaceTable

        let mut write_params = request.write_options.lance_write_params.unwrap_or_default();
        // Disable auto_cleanup to prevent accidental enabling
        write_params.auto_cleanup = None;

        // Merge storage_options from the namespace response
        // These contain write credentials/parameters for the manifest location
        if let Some(storage_opts) = create_empty_response.storage_options {
            if !storage_opts.is_empty() {
                let target_opts = write_params
                    .store_params
                    .get_or_insert_with(Default::default)
                    .storage_options
                    .get_or_insert_with(Default::default);
                target_opts.extend(storage_opts);
            }
        }

        let namespace_table = NamespaceTable::create(
            self.namespace.clone(),
            table_id,
            request.data,
            Some(write_params),
            self.read_consistency_interval,
        )
        .await?;

        Ok(Arc::new(namespace_table))
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        use lance::dataset::ReadParams;

        // Build read params with session, matching ListingDatabase behavior
        let mut read_params = request.lance_read_params.unwrap_or_else(|| {
            let mut default_params = ReadParams::default();
            if let Some(index_cache_size) = request.index_cache_size {
                #[allow(deprecated)]
                default_params.index_cache_size(index_cache_size as usize);
            }
            default_params
        });
        if let Some(ref sess) = self.session {
            read_params.session(sess.clone());
        }

        let namespace_table = NamespaceTable::open(
            self.namespace.clone(),
            table_id,
            Some(read_params),
            self.read_consistency_interval,
        )
        .await?;

        Ok(Arc::new(namespace_table))
    }

    async fn clone_table(&self, _request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        Err(Error::NotSupported {
            message: "clone_table is not supported for namespace connections".to_string(),
        })
    }

    async fn rename_table(
        &self,
        _cur_name: &str,
        _new_name: &str,
        _cur_namespace: &[String],
        _new_namespace: &[String],
    ) -> Result<()> {
        Err(Error::NotSupported {
            message: "rename_table is not supported for namespace connections".to_string(),
        })
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        let mut table_id = namespace.to_vec();
        table_id.push(name.to_string());

        let drop_request = DropTableRequest { id: Some(table_id) };
        self.namespace
            .drop_table(drop_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to drop table: {}", e),
            })?;

        Ok(())
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        let tables = self
            .table_names(TableNamesRequest {
                namespace: namespace.to_vec(),
                start_after: None,
                limit: None,
            })
            .await?;

        for table in tables {
            self.drop_table(&table, namespace).await?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A table implementation that uses lance-namespace for table management
///
/// Unlike NativeTable which derives table location from file paths, NamespaceTable
/// obtains table locations from the namespace implementation and uses
/// DatasetBuilder::from_namespace() which supports dynamic credential refresh
/// via the StorageOptionsProvider pattern.
#[derive(Clone)]
pub struct NamespaceTable {
    /// The namespace implementation that manages this table
    namespace_impl: Arc<dyn LanceNamespace>,
    /// The table identifier (namespace path + table name)
    table_id: Vec<String>,
    /// The table name (last component of table_id)
    name: String,
    /// The namespace path (all but last component of table_id)
    namespace: Vec<String>,
    /// The underlying dataset, wrapped for consistency checking
    dataset: crate::table::dataset::DatasetConsistencyWrapper,
    /// How often to check for updates from other processes
    read_consistency_interval: Option<std::time::Duration>,
}

impl std::fmt::Debug for NamespaceTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceTable")
            .field("name", &self.name)
            .field("namespace", &self.namespace)
            .field("table_id", &self.table_id)
            .finish()
    }
}

impl std::fmt::Display for NamespaceTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NamespaceTable(name={}, namespace={:?}, read_consistency_interval={})",
            self.name,
            self.namespace,
            match self.read_consistency_interval {
                None => "None".to_string(),
                Some(duration) => format!("{}s", duration.as_secs_f64()),
            }
        )
    }
}

impl NamespaceTable {
    /// Open a table from a namespace
    pub async fn open(
        namespace_impl: Arc<dyn LanceNamespace>,
        table_id: Vec<String>,
        params: Option<lance::dataset::ReadParams>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        use lance::dataset::builder::DatasetBuilder;

        let params = params.unwrap_or_default();

        // Use DatasetBuilder::from_namespace which handles storage options provider
        let mut builder =
            DatasetBuilder::from_namespace(namespace_impl.clone(), table_id.clone(), false)
                .await
                .map_err(|e| Error::Lance { source: e })?;

        // Apply the read params
        builder = builder.with_read_params(params);

        let dataset = builder.load().await.map_err(|e| {
            let name = table_id.last().unwrap_or(&"unknown".to_string()).clone();
            match e {
                lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                    name,
                    source: Box::new(e),
                },
                source => Error::Lance { source },
            }
        })?;

        let dataset_wrapper = crate::table::dataset::DatasetConsistencyWrapper::new_latest(
            dataset,
            read_consistency_interval,
        );

        // Extract name (last component) and namespace (all but last)
        let name = table_id.last().unwrap_or(&"unknown".to_string()).clone();
        let namespace = if table_id.len() > 1 {
            table_id[..table_id.len() - 1].to_vec()
        } else {
            vec![]
        };

        Ok(Self {
            namespace_impl,
            table_id,
            name,
            namespace,
            dataset: dataset_wrapper,
            read_consistency_interval,
        })
    }

    /// Create a new table in a namespace
    pub async fn create(
        namespace_impl: Arc<dyn LanceNamespace>,
        table_id: Vec<String>,
        batches: impl lance_datafusion::utils::StreamingWriteSource,
        params: Option<lance::dataset::WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        use lance::dataset::InsertBuilder;

        // First, describe the table to get its location
        let describe_request = DescribeTableRequest {
            id: Some(table_id.clone()),
            version: None,
        };

        let describe_response = namespace_impl
            .describe_table(describe_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to describe table: {}", e),
            })?;

        let location = describe_response.location.ok_or_else(|| Error::Runtime {
            message: "Table location is missing from describe_table response".to_string(),
        })?;

        let params = params.unwrap_or_default();

        // Create the dataset at the specified location
        let insert_builder = InsertBuilder::new(&location).with_params(&params);
        let dataset = insert_builder.execute_stream(batches).await.map_err(|e| {
            let name = table_id.last().unwrap_or(&"unknown".to_string()).clone();
            match e {
                lance::Error::DatasetAlreadyExists { .. } => Error::TableAlreadyExists { name },
                source => Error::Lance { source },
            }
        })?;

        let dataset_wrapper = crate::table::dataset::DatasetConsistencyWrapper::new_latest(
            dataset,
            read_consistency_interval,
        );

        // Extract name (last component) and namespace (all but last)
        let name = table_id.last().unwrap_or(&"unknown".to_string()).clone();
        let namespace = if table_id.len() > 1 {
            table_id[..table_id.len() - 1].to_vec()
        } else {
            vec![]
        };

        Ok(Self {
            namespace_impl,
            table_id,
            name,
            namespace,
            dataset: dataset_wrapper,
            read_consistency_interval,
        })
    }
}

// Implement BaseTable for NamespaceTable
#[async_trait::async_trait]
impl crate::table::BaseTable for NamespaceTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn namespace(&self) -> &[String] {
        &self.namespace
    }

    fn id(&self) -> &str {
        // For namespace tables, ID is just the name (similar to NativeTable)
        // In a full implementation, we'd cache namespace.join(".") + "." + name
        self.name.as_str()
    }

    async fn schema(&self) -> crate::error::Result<arrow_schema::SchemaRef> {
        let lance_schema = self.dataset.get().await?.schema().clone();
        Ok(std::sync::Arc::new(arrow_schema::Schema::from(
            &lance_schema,
        )))
    }

    async fn count_rows(
        &self,
        filter: Option<crate::table::Filter>,
    ) -> crate::error::Result<usize> {
        use crate::table::Filter;
        let dataset = self.dataset.get().await?;
        match filter {
            None => Ok(dataset.count_rows(None).await?),
            Some(Filter::Sql(sql)) => Ok(dataset.count_rows(Some(sql)).await?),
            Some(Filter::Datafusion(_)) => Err(crate::error::Error::NotSupported {
                message: "Datafusion filters are not yet supported".to_string(),
            }),
        }
    }

    async fn create_plan(
        &self,
        query: &crate::table::AnyQuery,
        options: crate::query::QueryExecutionOptions,
    ) -> crate::error::Result<std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>> {
        // Implementation adapted from NativeTable
        let query = match query {
            AnyQuery::VectorQuery(query) => query.clone(),
            AnyQuery::Query(query) => VectorQueryRequest::from_plain_query(query.clone()),
        };

        let ds_ref = self.dataset.get().await?;
        let schema = ds_ref.schema();
        let mut column = query.column.clone();

        let mut query_vector = query.query_vector.first().cloned();
        if query.query_vector.len() > 1 {
            if column.is_none() {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(ds_ref.schema());
                column = Some(default_vector_column(
                    &arrow_schema,
                    Some(query.query_vector[0].len() as i32),
                )?);
            }
            let vector_field = schema.field(column.as_ref().unwrap()).unwrap();
            if let DataType::List(_) = vector_field.data_type() {
                // it's multivector, then the vectors should be treated as single query
                // concatenate the vectors into a FixedSizeList<FixedSizeList<_>>
                let vectors = query
                    .query_vector
                    .iter()
                    .map(|arr| arr.as_ref())
                    .collect::<Vec<_>>();
                let dim = vectors[0].len();
                let mut fsl_builder = FixedSizeListBuilder::with_capacity(
                    Float32Builder::with_capacity(dim),
                    dim as i32,
                    vectors.len(),
                );
                for vec in vectors {
                    fsl_builder
                        .values()
                        .append_slice(vec.as_primitive::<Float32Type>().values());
                    fsl_builder.append(true);
                }
                query_vector = Some(Arc::new(fsl_builder.finish()));
            } else {
                // If there are multiple query vectors, create a plan for each of them and union them.
                let query_vecs = query.query_vector.clone();
                let plan_futures = query_vecs
                    .into_iter()
                    .map(|query_vector| {
                        let mut sub_query = query.clone();
                        sub_query.query_vector = vec![query_vector];
                        let options_ref = options.clone();
                        async move {
                            self.create_plan(&AnyQuery::VectorQuery(sub_query), options_ref)
                                .await
                        }
                    })
                    .collect::<Vec<_>>();
                let plans = futures::future::try_join_all(plan_futures).await?;
                return Table::multi_vector_plan(plans);
            }
        }

        let mut scanner: Scanner = ds_ref.scan();

        if let Some(query_vector) = query_vector {
            // If there is a vector query, default to limit=10 if unspecified
            let column = if let Some(col) = column {
                col
            } else {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(ds_ref.schema());
                default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
            };

            let (_, element_type) = lance::index::vector::utils::get_vector_type(schema, &column)?;
            let is_binary = matches!(element_type, DataType::UInt8);
            let top_k = query.base.limit.unwrap_or(DEFAULT_TOP_K) + query.base.offset.unwrap_or(0);
            if is_binary {
                let query_vector = arrow::compute::cast(&query_vector, &DataType::UInt8)?;
                let query_vector = query_vector.as_primitive::<UInt8Type>();
                scanner.nearest(&column, query_vector, top_k)?;
            } else {
                scanner.nearest(&column, query_vector.as_ref(), top_k)?;
            }
            scanner.minimum_nprobes(query.minimum_nprobes);
            if let Some(maximum_nprobes) = query.maximum_nprobes {
                scanner.maximum_nprobes(maximum_nprobes);
            }
        }
        scanner.limit(
            query.base.limit.map(|limit| limit as i64),
            query.base.offset.map(|offset| offset as i64),
        )?;
        if let Some(ef) = query.ef {
            scanner.ef(ef);
        }
        scanner.distance_range(query.lower_bound, query.upper_bound);
        scanner.use_index(query.use_index);
        scanner.prefilter(query.base.prefilter);
        match query.base.select {
            Select::Columns(ref columns) => {
                scanner.project(columns.as_slice())?;
            }
            Select::Dynamic(ref select_with_transform) => {
                scanner.project_with_transform(select_with_transform.as_slice())?;
            }
            Select::All => {}
        }

        if query.base.with_row_id {
            scanner.with_row_id();
        }

        scanner.batch_size(options.max_batch_length as usize);

        if query.base.fast_search {
            scanner.fast_search();
        }

        match &query.base.select {
            Select::Columns(select) => {
                scanner.project(select.as_slice())?;
            }
            Select::Dynamic(select_with_transform) => {
                scanner.project_with_transform(select_with_transform.as_slice())?;
            }
            Select::All => { /* Do nothing */ }
        }

        if let Some(filter) = &query.base.filter {
            match filter {
                QueryFilter::Sql(sql) => {
                    scanner.filter(&sql)?;
                }
                QueryFilter::Substrait(substrait) => {
                    scanner.filter_substrait(&substrait)?;
                }
                QueryFilter::Datafusion(expr) => {
                    scanner.filter_expr(expr.clone());
                }
            }
        }

        if let Some(fts) = &query.base.full_text_search {
            scanner.full_text_search(fts.clone())?;
        }

        if let Some(refine_factor) = query.refine_factor {
            scanner.refine(refine_factor);
        }

        if let Some(distance_type) = query.distance_type {
            scanner.distance_metric(distance_type.into());
        }

        if query.base.disable_scoring_autoprojection {
            scanner.disable_scoring_autoprojection();
        }

        Ok(scanner.create_plan().await?)
    }

    async fn query(
        &self,
        query: &crate::table::AnyQuery,
        options: crate::query::QueryExecutionOptions,
    ) -> crate::error::Result<crate::table::DatasetRecordBatchStream> {
        use lance_datafusion::exec::{execute_plan, LanceExecutionOptions};
        let plan = self.create_plan(query, options).await?;
        let lance_options = LanceExecutionOptions::default();
        let stream = execute_plan(plan, lance_options)?;
        Ok(crate::table::DatasetRecordBatchStream::new(stream))
    }

    async fn analyze_plan(
        &self,
        query: &crate::table::AnyQuery,
        options: crate::query::QueryExecutionOptions,
    ) -> crate::error::Result<String> {
        use lance_datafusion::exec::{analyze_plan as lance_analyze_plan, LanceExecutionOptions};
        let plan = self.create_plan(query, options).await?;
        let lance_options = LanceExecutionOptions::default();
        Ok(lance_analyze_plan(plan, lance_options).await?)
    }

    async fn add(
        &self,
        add: crate::table::AddDataBuilder<crate::connection::NoData>,
        data: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> crate::error::Result<crate::table::AddResult> {
        use crate::embeddings::MaybeEmbedded;
        use crate::table::AddDataMode;
        use lance::dataset::{InsertBuilder, WriteMode, WriteParams};

        let data = Box::new(MaybeEmbedded::try_new(
            data,
            self.table_definition().await?,
            add.embedding_registry,
        )?) as Box<dyn arrow_array::RecordBatchReader + Send>;

        let lance_params = add.write_options.lance_write_params.unwrap_or(WriteParams {
            mode: match add.mode {
                AddDataMode::Append => WriteMode::Append,
                AddDataMode::Overwrite => WriteMode::Overwrite,
            },
            ..Default::default()
        });

        let dataset = {
            let ds = self.dataset.get_mut().await?;
            InsertBuilder::new(std::sync::Arc::new(ds.clone()))
                .with_params(&lance_params)
                .execute_stream(data)
                .await?
        };
        let version = dataset.manifest().version;
        self.dataset.set_latest(dataset).await;
        Ok(crate::table::AddResult { version })
    }

    async fn delete(&self, predicate: &str) -> crate::error::Result<crate::table::DeleteResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.delete(predicate).await?;
        Ok(crate::table::DeleteResult {
            version: dataset.version().version,
        })
    }

    async fn update(
        &self,
        update: crate::table::UpdateBuilder,
    ) -> crate::error::Result<crate::table::UpdateResult> {
        use lance::dataset::UpdateBuilder as LanceUpdateBuilder;

        let dataset = self.dataset.get().await?.clone();
        let mut builder = LanceUpdateBuilder::new(std::sync::Arc::new(dataset));
        if let Some(predicate) = update.filter {
            builder = builder.update_where(&predicate)?;
        }

        for (column, value) in update.columns {
            builder = builder.set(column, &value)?;
        }

        let operation = builder.build()?;
        let res = operation.execute().await?;
        self.dataset
            .set_latest(res.new_dataset.as_ref().clone())
            .await;
        Ok(crate::table::UpdateResult {
            rows_updated: res.rows_updated,
            version: res.new_dataset.version().version,
        })
    }

    async fn create_index(&self, _opts: crate::index::IndexBuilder) -> crate::error::Result<()> {
        Err(crate::error::Error::NotSupported {
            message: "create_index for NamespaceTable not yet implemented".to_string(),
        })
    }

    async fn list_indices(&self) -> crate::error::Result<Vec<crate::index::IndexConfig>> {
        // Simplified implementation for now
        Ok(vec![])
    }

    async fn drop_index(&self, index_name: &str) -> crate::error::Result<()> {
        use lance_index::DatasetIndexExt;
        use std::ops::DerefMut;

        let mut dataset = self.dataset.get_mut().await?;
        dataset.deref_mut().drop_index(index_name).await?;
        Ok(())
    }

    async fn prewarm_index(&self, index_name: &str) -> crate::error::Result<()> {
        use lance_index::DatasetIndexExt;
        use std::ops::Deref;

        let dataset = self.dataset.get().await?;
        Ok(dataset.deref().prewarm_index(index_name).await?)
    }

    async fn index_stats(
        &self,
        _index_name: &str,
    ) -> crate::error::Result<Option<crate::index::IndexStatistics>> {
        // Simplified for now
        Ok(None)
    }

    async fn merge_insert(
        &self,
        _params: crate::table::merge::MergeInsertBuilder,
        _new_data: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> crate::error::Result<crate::table::MergeResult> {
        Err(crate::error::Error::NotSupported {
            message: "merge_insert() not yet implemented for NamespaceTable".to_string(),
        })
    }

    async fn tags(&self) -> crate::error::Result<Box<dyn crate::table::Tags + '_>> {
        Err(crate::error::Error::NotSupported {
            message: "tags() not yet implemented for NamespaceTable".to_string(),
        })
    }

    async fn optimize(
        &self,
        _action: crate::table::OptimizeAction,
    ) -> crate::error::Result<crate::table::OptimizeStats> {
        Err(crate::error::Error::NotSupported {
            message: "optimize() not yet implemented for NamespaceTable".to_string(),
        })
    }

    async fn add_columns(
        &self,
        transforms: crate::table::NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> crate::error::Result<crate::table::AddColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.add_columns(transforms, read_columns, None).await?;
        Ok(crate::table::AddColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn alter_columns(
        &self,
        alterations: &[crate::table::ColumnAlteration],
    ) -> crate::error::Result<crate::table::AlterColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.alter_columns(alterations).await?;
        Ok(crate::table::AlterColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn drop_columns(
        &self,
        columns: &[&str],
    ) -> crate::error::Result<crate::table::DropColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.drop_columns(columns).await?;
        Ok(crate::table::DropColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn version(&self) -> crate::error::Result<u64> {
        Ok(self.dataset.get().await?.version().version)
    }

    async fn checkout(&self, version: u64) -> crate::error::Result<()> {
        self.dataset.as_time_travel(version).await
    }

    async fn checkout_tag(&self, tag: &str) -> crate::error::Result<()> {
        self.dataset.as_time_travel(tag).await
    }

    async fn checkout_latest(&self) -> crate::error::Result<()> {
        self.dataset
            .as_latest(self.read_consistency_interval)
            .await?;
        self.dataset.reload().await
    }

    async fn restore(&self) -> crate::error::Result<()> {
        let version = self.dataset.time_travel_version().await.ok_or_else(|| {
            crate::error::Error::InvalidInput {
                message: "you must run checkout before running restore".to_string(),
            }
        })?;
        {
            let mut dataset = self.dataset.get_mut_unchecked().await?;
            debug_assert_eq!(dataset.version().version, version);
            dataset.restore().await?;
        }
        self.dataset
            .as_latest(self.read_consistency_interval)
            .await?;
        Ok(())
    }

    async fn list_versions(&self) -> crate::error::Result<Vec<crate::table::Version>> {
        Ok(self.dataset.get().await?.versions().await?)
    }

    async fn table_definition(&self) -> crate::error::Result<crate::table::TableDefinition> {
        let schema = self.schema().await?;
        crate::table::TableDefinition::try_from_rich_schema(schema)
    }

    fn dataset_uri(&self) -> &str {
        // For namespace tables, we don't have a simple URI
        // Return empty string for now
        ""
    }

    async fn storage_options(&self) -> Option<std::collections::HashMap<String, String>> {
        // Namespace tables use StorageOptionsProvider, not static storage options
        None
    }

    async fn wait_for_index(
        &self,
        index_names: &[&str],
        timeout: std::time::Duration,
    ) -> crate::error::Result<()> {
        use crate::index::waiter::wait_for_index;
        wait_for_index(self, index_names, timeout).await
    }

    async fn stats(&self) -> crate::error::Result<crate::table::TableStatistics> {
        use std::ops::Deref;

        let dataset = self.dataset.get().await?;
        let lance_stats = dataset.deref().count_rows(None).await?;
        Ok(crate::table::TableStatistics {
            total_bytes: 0,
            num_rows: lance_stats,
            num_indices: 0,
            fragment_stats: crate::table::FragmentStatistics {
                num_fragments: 0,
                num_small_fragments: 0,
                lengths: crate::table::FragmentSummaryStats {
                    min: 0,
                    max: 0,
                    mean: 0,
                    p25: 0,
                    p50: 0,
                    p75: 0,
                    p99: 0,
                },
            },
        })
    }
}

#[cfg(test)]
#[cfg(not(windows))] // TODO: support windows for lance-namespace
mod tests {
    use super::*;
    use crate::connect_namespace;
    use crate::query::ExecutableQuery;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    /// Helper function to create test data
    fn create_test_data() -> RecordBatchIterator<
        std::vec::IntoIter<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    > {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();
        RecordBatchIterator::new(vec![std::result::Result::Ok(batch)].into_iter(), schema)
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Test: Create a table in the child namespace
        let test_data = create_test_data();
        let table = conn
            .create_table("test_table", test_data)
            .namespace(vec!["test_ns".to_string()])
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

        // Verify: Table appears in table_names for the namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names.contains(&"test_table".to_string()));
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create a table first in the child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("describe_test", test_data)
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to create table");

        // Test: Open the table (which internally uses describe_table)
        let opened_table = conn
            .open_table("describe_test")
            .namespace(vec!["test_ns".to_string()])
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with 5 rows in the child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("overwrite_test", test_data1)
            .namespace(vec!["test_ns".to_string()])
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
            .create_table(
                "overwrite_test",
                RecordBatchIterator::new(
                    vec![std::result::Result::Ok(test_data2)].into_iter(),
                    schema,
                ),
            )
            .namespace(vec!["test_ns".to_string()])
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with test data in the child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("exist_ok_test", test_data1)
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to create table");

        // Try to create again with exist_ok mode
        let test_data2 = create_test_data();
        let table2 = conn
            .create_table("exist_ok_test", test_data2)
            .namespace(vec!["test_ns".to_string()])
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create first table in the child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to create first table");

        // Create second table in the child namespace
        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to create second table");

        // Verify: Both tables appear in table list for the namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to list tables");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));

        // Verify: Can open both tables from the namespace
        let opened_table1 = conn
            .open_table("table1")
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to open table1");

        let opened_table2 = conn
            .open_table("table2")
            .namespace(vec!["test_ns".to_string()])
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Test: Try to open a non-existent table in the child namespace
        let result = conn
            .open_table("non_existent_table")
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await;

        // Verify: Should return an error
        assert!(result.is_err());
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

        // Create a child namespace
        conn.create_namespace(crate::database::CreateNamespaceRequest {
            id: Some(vec!["test_ns".to_string()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create a table first in the child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("drop_test", test_data)
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to create table");

        // Verify table exists in the namespace
        let table_names_before = conn
            .table_names()
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names_before.contains(&"drop_test".to_string()));

        // Test: Drop the table
        conn.drop_table("drop_test", &["test_ns".to_string()])
            .await
            .expect("Failed to drop table");

        // Verify: Table no longer exists
        let table_names_after = conn
            .table_names()
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(!table_names_after.contains(&"drop_test".to_string()));

        // Verify: Cannot open dropped table
        let open_result = conn
            .open_table("drop_test")
            .namespace(vec!["test_ns".to_string()])
            .execute()
            .await;
        assert!(open_result.is_err());
    }
}
