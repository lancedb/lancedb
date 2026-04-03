// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace utilities for Python bindings

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use lance_io::object_store::{LanceNamespaceStorageOptionsProvider, StorageOptionsProvider};
use lance_namespace::LanceNamespace as LanceNamespaceTrait;
use lance_namespace::models::*;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Wrapper that allows any Python object implementing LanceNamespace protocol
/// to be used as a Rust LanceNamespace.
///
/// This is similar to PyLanceNamespace in lance's Python bindings - it wraps a Python
/// object and calls back into Python when namespace methods are invoked.
pub struct PyLanceNamespace {
    py_namespace: Arc<Py<PyAny>>,
    namespace_id: String,
}

impl PyLanceNamespace {
    /// Create a new PyLanceNamespace wrapper around a Python namespace object.
    pub fn new(_py: Python<'_>, py_namespace: &Bound<'_, PyAny>) -> PyResult<Self> {
        let namespace_id = py_namespace
            .call_method0("namespace_id")?
            .extract::<String>()?;

        Ok(Self {
            py_namespace: Arc::new(py_namespace.clone().unbind()),
            namespace_id,
        })
    }

    /// Create an Arc<dyn LanceNamespace> from a Python namespace object.
    pub fn create_arc(
        py: Python<'_>,
        py_namespace: &Bound<'_, PyAny>,
    ) -> PyResult<Arc<dyn LanceNamespaceTrait>> {
        let wrapper = Self::new(py, py_namespace)?;
        Ok(Arc::new(wrapper))
    }
}

impl std::fmt::Debug for PyLanceNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyLanceNamespace {{ id: {} }}", self.namespace_id)
    }
}

/// Get or create the DictWithModelDump class in Python.
/// This class acts like a dict but also has model_dump() method.
/// This allows it to work with both:
/// - depythonize (which expects a dict/Mapping)
/// - Python code that calls .model_dump() (like DirectoryNamespace wrapper)
fn get_dict_with_model_dump_class(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    // Use a module-level cache via __builtins__
    let builtins = py.import("builtins")?;
    if builtins.hasattr("_DictWithModelDump")? {
        return builtins.getattr("_DictWithModelDump");
    }

    // Create the class using exec
    let locals = PyDict::new(py);
    py.run(
        c"class DictWithModelDump(dict):
    def model_dump(self):
        return dict(self)",
        None,
        Some(&locals),
    )?;
    let class = locals.get_item("DictWithModelDump")?.ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err("Failed to create DictWithModelDump class")
    })?;

    // Cache it
    builtins.setattr("_DictWithModelDump", &class)?;
    Ok(class)
}

/// Helper to call a Python namespace method with JSON serialization.
/// For methods that take a request and return a response.
/// Uses DictWithModelDump to pass a dict that also has model_dump() method,
/// making it compatible with both depythonize and Python wrappers.
async fn call_py_method<Req, Resp>(
    py_namespace: Arc<Py<PyAny>>,
    method_name: &'static str,
    request: Req,
) -> lance_core::Result<Resp>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let request_json = serde_json::to_string(&request).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to serialize request for {}: {}",
            method_name, e
        ))
    })?;

    let response_json = tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let json_module = py.import("json")?;
            let request_dict = json_module.call_method1("loads", (&request_json,))?;

            // Wrap dict in DictWithModelDump so it works with both depythonize and .model_dump()
            let dict_class = get_dict_with_model_dump_class(py)?;
            let request_arg = dict_class.call1((request_dict,))?;

            // Call the Python method
            let result = py_namespace.call_method1(py, method_name, (request_arg,))?;

            // Convert response to dict, then to JSON
            // Pydantic models have model_dump() method
            let result_dict = if result.bind(py).hasattr("model_dump")? {
                result.call_method0(py, "model_dump")?
            } else {
                result
            };
            let response_json: String = json_module
                .call_method1("dumps", (result_dict,))?
                .extract()?;
            Ok::<_, PyErr>(response_json)
        })
    })
    .await
    .map_err(|e| lance_core::Error::io(format!("Task join error for {}: {}", method_name, e)))?
    .map_err(|e: PyErr| lance_core::Error::io(format!("Python error in {}: {}", method_name, e)))?;

    serde_json::from_str(&response_json).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to deserialize response from {}: {}",
            method_name, e
        ))
    })
}

/// Helper for methods that return () on success
async fn call_py_method_unit<Req>(
    py_namespace: Arc<Py<PyAny>>,
    method_name: &'static str,
    request: Req,
) -> lance_core::Result<()>
where
    Req: serde::Serialize + Send + 'static,
{
    let request_json = serde_json::to_string(&request).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to serialize request for {}: {}",
            method_name, e
        ))
    })?;

    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let json_module = py.import("json")?;
            let request_dict = json_module.call_method1("loads", (&request_json,))?;

            // Wrap dict in DictWithModelDump
            let dict_class = get_dict_with_model_dump_class(py)?;
            let request_arg = dict_class.call1((request_dict,))?;

            // Call the Python method
            py_namespace.call_method1(py, method_name, (request_arg,))?;
            Ok::<_, PyErr>(())
        })
    })
    .await
    .map_err(|e| lance_core::Error::io(format!("Task join error for {}: {}", method_name, e)))?
    .map_err(|e: PyErr| lance_core::Error::io(format!("Python error in {}: {}", method_name, e)))
}

/// Helper for methods that return a primitive type
async fn call_py_method_primitive<Req, Resp>(
    py_namespace: Arc<Py<PyAny>>,
    method_name: &'static str,
    request: Req,
) -> lance_core::Result<Resp>
where
    Req: serde::Serialize + Send + 'static,
    Resp: for<'py> pyo3::FromPyObject<'py> + Send + 'static,
{
    let request_json = serde_json::to_string(&request).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to serialize request for {}: {}",
            method_name, e
        ))
    })?;

    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let json_module = py.import("json")?;
            let request_dict = json_module.call_method1("loads", (&request_json,))?;

            // Wrap dict in DictWithModelDump
            let dict_class = get_dict_with_model_dump_class(py)?;
            let request_arg = dict_class.call1((request_dict,))?;

            // Call the Python method
            let result = py_namespace.call_method1(py, method_name, (request_arg,))?;
            let value: Resp = result.extract(py)?;
            Ok::<_, PyErr>(value)
        })
    })
    .await
    .map_err(|e| lance_core::Error::io(format!("Task join error for {}: {}", method_name, e)))?
    .map_err(|e: PyErr| lance_core::Error::io(format!("Python error in {}: {}", method_name, e)))
}

/// Helper for methods that return Bytes
async fn call_py_method_bytes<Req>(
    py_namespace: Arc<Py<PyAny>>,
    method_name: &'static str,
    request: Req,
) -> lance_core::Result<Bytes>
where
    Req: serde::Serialize + Send + 'static,
{
    let request_json = serde_json::to_string(&request).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to serialize request for {}: {}",
            method_name, e
        ))
    })?;

    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let json_module = py.import("json")?;
            let request_dict = json_module.call_method1("loads", (&request_json,))?;

            // Wrap dict in DictWithModelDump
            let dict_class = get_dict_with_model_dump_class(py)?;
            let request_arg = dict_class.call1((request_dict,))?;

            // Call the Python method
            let result = py_namespace.call_method1(py, method_name, (request_arg,))?;
            let bytes_data: Vec<u8> = result.extract(py)?;
            Ok::<_, PyErr>(Bytes::from(bytes_data))
        })
    })
    .await
    .map_err(|e| lance_core::Error::io(format!("Task join error for {}: {}", method_name, e)))?
    .map_err(|e: PyErr| lance_core::Error::io(format!("Python error in {}: {}", method_name, e)))
}

/// Helper for methods that take request + data and return a response
async fn call_py_method_with_data<Req, Resp>(
    py_namespace: Arc<Py<PyAny>>,
    method_name: &'static str,
    request: Req,
    data: Bytes,
) -> lance_core::Result<Resp>
where
    Req: serde::Serialize + Send + 'static,
    Resp: serde::de::DeserializeOwned + Send + 'static,
{
    let request_json = serde_json::to_string(&request).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to serialize request for {}: {}",
            method_name, e
        ))
    })?;

    let response_json = tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let json_module = py.import("json")?;
            let request_dict = json_module.call_method1("loads", (&request_json,))?;

            // Wrap dict in DictWithModelDump
            let dict_class = get_dict_with_model_dump_class(py)?;
            let request_arg = dict_class.call1((request_dict,))?;

            // Pass request and bytes to Python method
            let py_bytes = pyo3::types::PyBytes::new(py, &data);
            let result = py_namespace.call_method1(py, method_name, (request_arg, py_bytes))?;

            // Convert response dict to JSON
            let response_json: String = json_module.call_method1("dumps", (result,))?.extract()?;
            Ok::<_, PyErr>(response_json)
        })
    })
    .await
    .map_err(|e| lance_core::Error::io(format!("Task join error for {}: {}", method_name, e)))?
    .map_err(|e: PyErr| lance_core::Error::io(format!("Python error in {}: {}", method_name, e)))?;

    serde_json::from_str(&response_json).map_err(|e| {
        lance_core::Error::io(format!(
            "Failed to deserialize response from {}: {}",
            method_name, e
        ))
    })
}

#[async_trait]
impl LanceNamespaceTrait for PyLanceNamespace {
    fn namespace_id(&self) -> String {
        self.namespace_id.clone()
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> lance_core::Result<ListNamespacesResponse> {
        call_py_method(self.py_namespace.clone(), "list_namespaces", request).await
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> lance_core::Result<DescribeNamespaceResponse> {
        call_py_method(self.py_namespace.clone(), "describe_namespace", request).await
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> lance_core::Result<CreateNamespaceResponse> {
        call_py_method(self.py_namespace.clone(), "create_namespace", request).await
    }

    async fn drop_namespace(
        &self,
        request: DropNamespaceRequest,
    ) -> lance_core::Result<DropNamespaceResponse> {
        call_py_method(self.py_namespace.clone(), "drop_namespace", request).await
    }

    async fn namespace_exists(&self, request: NamespaceExistsRequest) -> lance_core::Result<()> {
        call_py_method_unit(self.py_namespace.clone(), "namespace_exists", request).await
    }

    async fn list_tables(
        &self,
        request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        call_py_method(self.py_namespace.clone(), "list_tables", request).await
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> lance_core::Result<DescribeTableResponse> {
        call_py_method(self.py_namespace.clone(), "describe_table", request).await
    }

    async fn register_table(
        &self,
        request: RegisterTableRequest,
    ) -> lance_core::Result<RegisterTableResponse> {
        call_py_method(self.py_namespace.clone(), "register_table", request).await
    }

    async fn table_exists(&self, request: TableExistsRequest) -> lance_core::Result<()> {
        call_py_method_unit(self.py_namespace.clone(), "table_exists", request).await
    }

    async fn drop_table(&self, request: DropTableRequest) -> lance_core::Result<DropTableResponse> {
        call_py_method(self.py_namespace.clone(), "drop_table", request).await
    }

    async fn deregister_table(
        &self,
        request: DeregisterTableRequest,
    ) -> lance_core::Result<DeregisterTableResponse> {
        call_py_method(self.py_namespace.clone(), "deregister_table", request).await
    }

    async fn count_table_rows(&self, request: CountTableRowsRequest) -> lance_core::Result<i64> {
        call_py_method_primitive(self.py_namespace.clone(), "count_table_rows", request).await
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<CreateTableResponse> {
        call_py_method_with_data(
            self.py_namespace.clone(),
            "create_table",
            request,
            request_data,
        )
        .await
    }

    async fn declare_table(
        &self,
        request: DeclareTableRequest,
    ) -> lance_core::Result<DeclareTableResponse> {
        call_py_method(self.py_namespace.clone(), "declare_table", request).await
    }

    async fn insert_into_table(
        &self,
        request: InsertIntoTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<InsertIntoTableResponse> {
        call_py_method_with_data(
            self.py_namespace.clone(),
            "insert_into_table",
            request,
            request_data,
        )
        .await
    }

    async fn merge_insert_into_table(
        &self,
        request: MergeInsertIntoTableRequest,
        request_data: Bytes,
    ) -> lance_core::Result<MergeInsertIntoTableResponse> {
        call_py_method_with_data(
            self.py_namespace.clone(),
            "merge_insert_into_table",
            request,
            request_data,
        )
        .await
    }

    async fn update_table(
        &self,
        request: UpdateTableRequest,
    ) -> lance_core::Result<UpdateTableResponse> {
        call_py_method(self.py_namespace.clone(), "update_table", request).await
    }

    async fn delete_from_table(
        &self,
        request: DeleteFromTableRequest,
    ) -> lance_core::Result<DeleteFromTableResponse> {
        call_py_method(self.py_namespace.clone(), "delete_from_table", request).await
    }

    async fn query_table(&self, request: QueryTableRequest) -> lance_core::Result<Bytes> {
        call_py_method_bytes(self.py_namespace.clone(), "query_table", request).await
    }

    async fn create_table_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableIndexResponse> {
        call_py_method(self.py_namespace.clone(), "create_table_index", request).await
    }

    async fn list_table_indices(
        &self,
        request: ListTableIndicesRequest,
    ) -> lance_core::Result<ListTableIndicesResponse> {
        call_py_method(self.py_namespace.clone(), "list_table_indices", request).await
    }

    async fn describe_table_index_stats(
        &self,
        request: DescribeTableIndexStatsRequest,
    ) -> lance_core::Result<DescribeTableIndexStatsResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "describe_table_index_stats",
            request,
        )
        .await
    }

    async fn describe_transaction(
        &self,
        request: DescribeTransactionRequest,
    ) -> lance_core::Result<DescribeTransactionResponse> {
        call_py_method(self.py_namespace.clone(), "describe_transaction", request).await
    }

    async fn alter_transaction(
        &self,
        request: AlterTransactionRequest,
    ) -> lance_core::Result<AlterTransactionResponse> {
        call_py_method(self.py_namespace.clone(), "alter_transaction", request).await
    }

    async fn create_table_scalar_index(
        &self,
        request: CreateTableIndexRequest,
    ) -> lance_core::Result<CreateTableScalarIndexResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "create_table_scalar_index",
            request,
        )
        .await
    }

    async fn drop_table_index(
        &self,
        request: DropTableIndexRequest,
    ) -> lance_core::Result<DropTableIndexResponse> {
        call_py_method(self.py_namespace.clone(), "drop_table_index", request).await
    }

    async fn list_all_tables(
        &self,
        request: ListTablesRequest,
    ) -> lance_core::Result<ListTablesResponse> {
        call_py_method(self.py_namespace.clone(), "list_all_tables", request).await
    }

    async fn restore_table(
        &self,
        request: RestoreTableRequest,
    ) -> lance_core::Result<RestoreTableResponse> {
        call_py_method(self.py_namespace.clone(), "restore_table", request).await
    }

    async fn rename_table(
        &self,
        request: RenameTableRequest,
    ) -> lance_core::Result<RenameTableResponse> {
        call_py_method(self.py_namespace.clone(), "rename_table", request).await
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> lance_core::Result<ListTableVersionsResponse> {
        call_py_method(self.py_namespace.clone(), "list_table_versions", request).await
    }

    async fn create_table_version(
        &self,
        request: CreateTableVersionRequest,
    ) -> lance_core::Result<CreateTableVersionResponse> {
        call_py_method(self.py_namespace.clone(), "create_table_version", request).await
    }

    async fn describe_table_version(
        &self,
        request: DescribeTableVersionRequest,
    ) -> lance_core::Result<DescribeTableVersionResponse> {
        call_py_method(self.py_namespace.clone(), "describe_table_version", request).await
    }

    async fn batch_delete_table_versions(
        &self,
        request: BatchDeleteTableVersionsRequest,
    ) -> lance_core::Result<BatchDeleteTableVersionsResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "batch_delete_table_versions",
            request,
        )
        .await
    }

    async fn update_table_schema_metadata(
        &self,
        request: UpdateTableSchemaMetadataRequest,
    ) -> lance_core::Result<UpdateTableSchemaMetadataResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "update_table_schema_metadata",
            request,
        )
        .await
    }

    async fn get_table_stats(
        &self,
        request: GetTableStatsRequest,
    ) -> lance_core::Result<GetTableStatsResponse> {
        call_py_method(self.py_namespace.clone(), "get_table_stats", request).await
    }

    async fn explain_table_query_plan(
        &self,
        request: ExplainTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        call_py_method_primitive(
            self.py_namespace.clone(),
            "explain_table_query_plan",
            request,
        )
        .await
    }

    async fn analyze_table_query_plan(
        &self,
        request: AnalyzeTableQueryPlanRequest,
    ) -> lance_core::Result<String> {
        call_py_method_primitive(
            self.py_namespace.clone(),
            "analyze_table_query_plan",
            request,
        )
        .await
    }

    async fn alter_table_add_columns(
        &self,
        request: AlterTableAddColumnsRequest,
    ) -> lance_core::Result<AlterTableAddColumnsResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "alter_table_add_columns",
            request,
        )
        .await
    }

    async fn alter_table_alter_columns(
        &self,
        request: AlterTableAlterColumnsRequest,
    ) -> lance_core::Result<AlterTableAlterColumnsResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "alter_table_alter_columns",
            request,
        )
        .await
    }

    async fn alter_table_drop_columns(
        &self,
        request: AlterTableDropColumnsRequest,
    ) -> lance_core::Result<AlterTableDropColumnsResponse> {
        call_py_method(
            self.py_namespace.clone(),
            "alter_table_drop_columns",
            request,
        )
        .await
    }

    async fn list_table_tags(
        &self,
        request: ListTableTagsRequest,
    ) -> lance_core::Result<ListTableTagsResponse> {
        call_py_method(self.py_namespace.clone(), "list_table_tags", request).await
    }

    async fn create_table_tag(
        &self,
        request: CreateTableTagRequest,
    ) -> lance_core::Result<CreateTableTagResponse> {
        call_py_method(self.py_namespace.clone(), "create_table_tag", request).await
    }

    async fn delete_table_tag(
        &self,
        request: DeleteTableTagRequest,
    ) -> lance_core::Result<DeleteTableTagResponse> {
        call_py_method(self.py_namespace.clone(), "delete_table_tag", request).await
    }

    async fn update_table_tag(
        &self,
        request: UpdateTableTagRequest,
    ) -> lance_core::Result<UpdateTableTagResponse> {
        call_py_method(self.py_namespace.clone(), "update_table_tag", request).await
    }

    async fn get_table_tag_version(
        &self,
        request: GetTableTagVersionRequest,
    ) -> lance_core::Result<GetTableTagVersionResponse> {
        call_py_method(self.py_namespace.clone(), "get_table_tag_version", request).await
    }
}

/// Convert Python dict to HashMap<String, String>
#[allow(dead_code)]
fn dict_to_hashmap(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        let value_str: String = value.extract()?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}

/// Extract an Arc<dyn LanceNamespace> from a Python namespace object.
///
/// This function wraps any Python namespace object with PyLanceNamespace.
/// The PyLanceNamespace wrapper uses DictWithModelDump to pass requests,
/// which works with both:
/// - Native namespaces (DirectoryNamespace, RestNamespace) that use depythonize (expects dict)
/// - Custom Python implementations that call .model_dump() on the request
pub fn extract_namespace_arc(
    py: Python<'_>,
    ns: Py<PyAny>,
) -> PyResult<Arc<dyn LanceNamespaceTrait>> {
    let ns_ref = ns.bind(py);
    PyLanceNamespace::create_arc(py, ns_ref)
}

/// Create a LanceNamespaceStorageOptionsProvider from a namespace client and table ID.
///
/// This creates a Rust storage options provider that fetches credentials from the
/// namespace's describe_table() method, enabling automatic credential refresh.
///
/// # Arguments
/// * `namespace_client` - The namespace client (wrapped PyLanceNamespace)
/// * `table_id` - Full table identifier (namespace_path + table_name)
pub fn create_namespace_storage_options_provider(
    namespace_client: Arc<dyn LanceNamespaceTrait>,
    table_id: Vec<String>,
) -> Arc<dyn StorageOptionsProvider> {
    Arc::new(LanceNamespaceStorageOptionsProvider::new(
        namespace_client,
        table_id,
    ))
}
