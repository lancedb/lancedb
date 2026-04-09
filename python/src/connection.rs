// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::{datatypes::Schema, ffi_stream::ArrowArrayStreamReader, pyarrow::FromPyArrow};
use lancedb::{
    connection::Connection as LanceConnection,
    database::{CreateTableMode, Database, ReadConsistency},
};
use pyo3::{
    Bound, FromPyObject, Py, PyAny, PyRef, PyResult, Python,
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pyfunction, pymethods,
    types::{PyDict, PyDictMethods},
};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{
    error::PythonErrorExt,
    namespace::{create_namespace_storage_options_provider, extract_namespace_arc},
    table::Table,
};

#[pyclass]
pub struct Connection {
    inner: Option<LanceConnection>,
}

impl Connection {
    pub(crate) fn new(inner: LanceConnection) -> Self {
        Self { inner: Some(inner) }
    }

    pub(crate) fn get_inner(&self) -> PyResult<&LanceConnection> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Connection is closed"))
    }
}

impl Connection {
    fn parse_create_mode_str(mode: &str) -> PyResult<CreateTableMode> {
        match mode {
            "create" => Ok(CreateTableMode::Create),
            "overwrite" => Ok(CreateTableMode::Overwrite),
            "exist_ok" => Ok(CreateTableMode::exist_ok(|builder| builder)),
            _ => Err(PyValueError::new_err(format!("Invalid mode {}", mode))),
        }
    }

    pub fn database(&self) -> PyResult<Arc<dyn Database>> {
        Ok(self.get_inner()?.database().clone())
    }
}

#[pymethods]
impl Connection {
    fn __repr__(&self) -> String {
        match &self.inner {
            Some(inner) => inner.to_string(),
            None => "ClosedConnection".to_string(),
        }
    }

    fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    fn close(&mut self) {
        self.inner.take();
    }

    #[getter]
    pub fn uri(&self) -> PyResult<String> {
        self.get_inner().map(|inner| inner.uri().to_string())
    }

    #[pyo3(signature = ())]
    pub fn get_read_consistency_interval(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(self_.py(), async move {
            Ok(match inner.read_consistency().await.infer_error()? {
                ReadConsistency::Manual => None,
                ReadConsistency::Eventual(duration) => Some(duration.as_secs_f64()),
                ReadConsistency::Strong => Some(0.0_f64),
            })
        })
    }

    #[pyo3(signature = (namespace_path=None, start_after=None, limit=None))]
    pub fn table_names(
        self_: PyRef<'_, Self>,
        namespace_path: Option<Vec<String>>,
        start_after: Option<String>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let mut op = inner.table_names();
        op = op.namespace(namespace_path.unwrap_or_default());
        if let Some(start_after) = start_after {
            op = op.start_after(start_after);
        }
        if let Some(limit) = limit {
            op = op.limit(limit);
        }
        future_into_py(self_.py(), async move { op.execute().await.infer_error() })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, mode, data, namespace_path=None, storage_options=None, location=None, namespace_client=None))]
    pub fn create_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        data: Bound<'_, PyAny>,
        namespace_path: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
        location: Option<String>,
        namespace_client: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();

        let mode = Self::parse_create_mode_str(mode)?;

        let batches: Box<dyn arrow::array::RecordBatchReader + Send> =
            Box::new(ArrowArrayStreamReader::from_pyarrow_bound(&data)?);

        let ns_path = namespace_path.clone().unwrap_or_default();
        let mut builder = inner.create_table(name.clone(), batches).mode(mode);

        builder = builder.namespace(ns_path.clone());
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        // Auto-create storage options provider from namespace_client
        if let Some(ns_obj) = namespace_client {
            let ns_client = extract_namespace_arc(py, ns_obj)?;
            // Create table_id by combining namespace_path with table name
            let mut table_id = ns_path;
            table_id.push(name);
            let provider = create_namespace_storage_options_provider(ns_client, table_id);
            builder = builder.storage_options_provider(provider);
        }

        if let Some(location) = location {
            builder = builder.location(location);
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, mode, schema, namespace_path=None, storage_options=None, location=None, namespace_client=None))]
    pub fn create_empty_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        schema: Bound<'_, PyAny>,
        namespace_path: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
        location: Option<String>,
        namespace_client: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();

        let mode = Self::parse_create_mode_str(mode)?;

        let schema = Schema::from_pyarrow_bound(&schema)?;

        let ns_path = namespace_path.clone().unwrap_or_default();
        let mut builder = inner
            .create_empty_table(name.clone(), Arc::new(schema))
            .mode(mode);

        builder = builder.namespace(ns_path.clone());
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        // Auto-create storage options provider from namespace_client
        if let Some(ns_obj) = namespace_client {
            let ns_client = extract_namespace_arc(py, ns_obj)?;
            // Create table_id by combining namespace_path with table name
            let mut table_id = ns_path;
            table_id.push(name);
            let provider = create_namespace_storage_options_provider(ns_client, table_id);
            builder = builder.storage_options_provider(provider);
        }

        if let Some(location) = location {
            builder = builder.location(location);
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, namespace_path=None, storage_options=None, index_cache_size=None, location=None, namespace_client=None, managed_versioning=None))]
    pub fn open_table(
        self_: PyRef<'_, Self>,
        name: String,
        namespace_path: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
        location: Option<String>,
        namespace_client: Option<Py<PyAny>>,
        managed_versioning: Option<bool>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();

        let ns_path = namespace_path.clone().unwrap_or_default();
        let mut builder = inner.open_table(name.clone());
        builder = builder.namespace(ns_path.clone());
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        // Auto-create storage options provider from namespace_client
        if let Some(ns_obj) = namespace_client {
            let ns_client = extract_namespace_arc(py, ns_obj)?;
            // Create table_id by combining namespace_path with table name
            let mut table_id = ns_path;
            table_id.push(name);
            let provider = create_namespace_storage_options_provider(ns_client.clone(), table_id);
            builder = builder.storage_options_provider(provider);
            builder = builder.namespace_client(ns_client);
        }

        if let Some(index_cache_size) = index_cache_size {
            builder = builder.index_cache_size(index_cache_size);
        }
        if let Some(location) = location {
            builder = builder.location(location);
        }
        // Pass managed_versioning if provided to avoid redundant describe_table call
        if let Some(enabled) = managed_versioning {
            builder = builder.managed_versioning(enabled);
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[pyo3(signature = (target_table_name, source_uri, target_namespace_path=None, source_version=None, source_tag=None, is_shallow=true))]
    pub fn clone_table(
        self_: PyRef<'_, Self>,
        target_table_name: String,
        source_uri: String,
        target_namespace_path: Option<Vec<String>>,
        source_version: Option<u64>,
        source_tag: Option<String>,
        is_shallow: bool,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();

        let mut builder = inner.clone_table(target_table_name, source_uri);
        builder = builder.target_namespace(target_namespace_path.unwrap_or_default());
        if let Some(version) = source_version {
            builder = builder.source_version(version);
        }
        if let Some(tag) = source_tag {
            builder = builder.source_tag(tag);
        }
        builder = builder.is_shallow(is_shallow);

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[pyo3(signature = (cur_name, new_name, cur_namespace_path=None, new_namespace_path=None))]
    pub fn rename_table(
        self_: PyRef<'_, Self>,
        cur_name: String,
        new_name: String,
        cur_namespace_path: Option<Vec<String>>,
        new_namespace_path: Option<Vec<String>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let cur_ns_path = cur_namespace_path.unwrap_or_default();
        let new_ns_path = new_namespace_path.unwrap_or_default();
        future_into_py(self_.py(), async move {
            inner
                .rename_table(cur_name, new_name, &cur_ns_path, &new_ns_path)
                .await
                .infer_error()
        })
    }

    #[pyo3(signature = (name, namespace_path=None))]
    pub fn drop_table(
        self_: PyRef<'_, Self>,
        name: String,
        namespace_path: Option<Vec<String>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let ns_path = namespace_path.unwrap_or_default();
        future_into_py(self_.py(), async move {
            inner.drop_table(name, &ns_path).await.infer_error()
        })
    }

    #[pyo3(signature = (namespace_path=None,))]
    pub fn drop_all_tables(
        self_: PyRef<'_, Self>,
        namespace_path: Option<Vec<String>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let ns_path = namespace_path.unwrap_or_default();
        future_into_py(self_.py(), async move {
            inner.drop_all_tables(&ns_path).await.infer_error()
        })
    }

    // Namespace management methods

    #[pyo3(signature = (namespace_path=None, page_token=None, limit=None))]
    pub fn list_namespaces(
        self_: PyRef<'_, Self>,
        namespace_path: Option<Vec<String>>,
        page_token: Option<String>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            use lance_namespace::models::ListNamespacesRequest;
            let request = ListNamespacesRequest {
                id: namespace_path,
                page_token,
                limit: limit.map(|l| l as i32),
                ..Default::default()
            };
            let response = inner.list_namespaces(request).await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("namespaces", response.namespaces)?;
                dict.set_item("page_token", response.page_token)?;
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (namespace_path, mode=None, properties=None))]
    pub fn create_namespace(
        self_: PyRef<'_, Self>,
        namespace_path: Vec<String>,
        mode: Option<String>,
        properties: Option<std::collections::HashMap<String, String>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            use lance_namespace::models::CreateNamespaceRequest;
            // Mode is now a string field
            let mode_str = mode.and_then(|m| match m.to_lowercase().as_str() {
                "create" => Some("Create".to_string()),
                "exist_ok" => Some("ExistOk".to_string()),
                "overwrite" => Some("Overwrite".to_string()),
                _ => None,
            });
            let request = CreateNamespaceRequest {
                id: Some(namespace_path),
                mode: mode_str,
                properties,
                ..Default::default()
            };
            let response = inner.create_namespace(request).await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("properties", response.properties)?;
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (namespace_path, mode=None, behavior=None))]
    pub fn drop_namespace(
        self_: PyRef<'_, Self>,
        namespace_path: Vec<String>,
        mode: Option<String>,
        behavior: Option<String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            use lance_namespace::models::DropNamespaceRequest;
            // Mode and Behavior are now string fields
            let mode_str = mode.and_then(|m| match m.to_uppercase().as_str() {
                "SKIP" => Some("Skip".to_string()),
                "FAIL" => Some("Fail".to_string()),
                _ => None,
            });
            let behavior_str = behavior.and_then(|b| match b.to_uppercase().as_str() {
                "RESTRICT" => Some("Restrict".to_string()),
                "CASCADE" => Some("Cascade".to_string()),
                _ => None,
            });
            let request = DropNamespaceRequest {
                id: Some(namespace_path),
                mode: mode_str,
                behavior: behavior_str,
                ..Default::default()
            };
            let response = inner.drop_namespace(request).await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("properties", response.properties)?;
                dict.set_item("transaction_id", response.transaction_id)?;
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (namespace_path,))]
    pub fn describe_namespace(
        self_: PyRef<'_, Self>,
        namespace_path: Vec<String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            use lance_namespace::models::DescribeNamespaceRequest;
            let request = DescribeNamespaceRequest {
                id: Some(namespace_path),
                ..Default::default()
            };
            let response = inner.describe_namespace(request).await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("properties", response.properties)?;
                Ok(dict.unbind())
            })
        })
    }

    #[pyo3(signature = (namespace_path=None, page_token=None, limit=None))]
    pub fn list_tables(
        self_: PyRef<'_, Self>,
        namespace_path: Option<Vec<String>>,
        page_token: Option<String>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            use lance_namespace::models::ListTablesRequest;
            let request = ListTablesRequest {
                id: namespace_path,
                page_token,
                limit: limit.map(|l| l as i32),
                ..Default::default()
            };
            let response = inner.list_tables(request).await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("tables", response.tables)?;
                dict.set_item("page_token", response.page_token)?;
                Ok(dict.unbind())
            })
        })
    }

    /// Get the configuration for constructing an equivalent namespace client.
    /// Returns a dict with:
    /// - "impl": "dir" for DirectoryNamespace, "rest" for RestNamespace
    /// - "properties": configuration properties for the namespace
    #[pyo3(signature = ())]
    pub fn namespace_client_config(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            let (impl_type, properties) = inner.namespace_client_config().await.infer_error()?;
            Python::attach(|py| -> PyResult<Py<PyDict>> {
                let dict = PyDict::new(py);
                dict.set_item("impl", impl_type)?;
                dict.set_item("properties", properties)?;
                Ok(dict.unbind())
            })
        })
    }
}

#[pyfunction]
#[pyo3(signature = (uri, api_key=None, region=None, host_override=None, read_consistency_interval=None, client_config=None, storage_options=None, session=None))]
#[allow(clippy::too_many_arguments)]
pub fn connect(
    py: Python<'_>,
    uri: String,
    api_key: Option<String>,
    region: Option<String>,
    host_override: Option<String>,
    read_consistency_interval: Option<f64>,
    client_config: Option<PyClientConfig>,
    storage_options: Option<HashMap<String, String>>,
    session: Option<crate::session::Session>,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let mut builder = lancedb::connect(&uri);
        if let Some(api_key) = api_key {
            builder = builder.api_key(&api_key);
        }
        if let Some(region) = region {
            builder = builder.region(&region);
        }
        if let Some(host_override) = host_override {
            builder = builder.host_override(&host_override);
        }
        if let Some(read_consistency_interval) = read_consistency_interval {
            let read_consistency_interval = Duration::from_secs_f64(read_consistency_interval);
            builder = builder.read_consistency_interval(read_consistency_interval);
        }
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }
        #[cfg(feature = "remote")]
        if let Some(client_config) = client_config {
            builder = builder.client_config(client_config.into());
        }
        if let Some(session) = session {
            builder = builder.session(session.inner.clone());
        }
        Ok(Connection::new(builder.execute().await.infer_error()?))
    })
}

#[derive(FromPyObject)]
pub struct PyClientConfig {
    user_agent: String,
    retry_config: Option<PyClientRetryConfig>,
    timeout_config: Option<PyClientTimeoutConfig>,
    extra_headers: Option<HashMap<String, String>>,
    id_delimiter: Option<String>,
    tls_config: Option<PyClientTlsConfig>,
    header_provider: Option<Py<PyAny>>,
    user_id: Option<String>,
}

#[derive(FromPyObject)]
pub struct PyClientRetryConfig {
    retries: Option<u8>,
    connect_retries: Option<u8>,
    read_retries: Option<u8>,
    backoff_factor: Option<f32>,
    backoff_jitter: Option<f32>,
    statuses: Option<Vec<u16>>,
}

#[derive(FromPyObject)]
pub struct PyClientTimeoutConfig {
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    pool_idle_timeout: Option<Duration>,
}

#[derive(FromPyObject)]
pub struct PyClientTlsConfig {
    cert_file: Option<String>,
    key_file: Option<String>,
    ssl_ca_cert: Option<String>,
    assert_hostname: bool,
}

#[cfg(feature = "remote")]
impl From<PyClientRetryConfig> for lancedb::remote::RetryConfig {
    fn from(value: PyClientRetryConfig) -> Self {
        Self {
            retries: value.retries,
            connect_retries: value.connect_retries,
            read_retries: value.read_retries,
            backoff_factor: value.backoff_factor,
            backoff_jitter: value.backoff_jitter,
            statuses: value.statuses,
        }
    }
}

#[cfg(feature = "remote")]
impl From<PyClientTimeoutConfig> for lancedb::remote::TimeoutConfig {
    fn from(value: PyClientTimeoutConfig) -> Self {
        Self {
            timeout: value.timeout,
            connect_timeout: value.connect_timeout,
            read_timeout: value.read_timeout,
            pool_idle_timeout: value.pool_idle_timeout,
        }
    }
}

#[cfg(feature = "remote")]
impl From<PyClientTlsConfig> for lancedb::remote::TlsConfig {
    fn from(value: PyClientTlsConfig) -> Self {
        Self {
            cert_file: value.cert_file,
            key_file: value.key_file,
            ssl_ca_cert: value.ssl_ca_cert,
            assert_hostname: value.assert_hostname,
        }
    }
}

#[cfg(feature = "remote")]
impl From<PyClientConfig> for lancedb::remote::ClientConfig {
    fn from(value: PyClientConfig) -> Self {
        use crate::header::PyHeaderProvider;

        let header_provider = value.header_provider.map(|provider| {
            let py_provider = PyHeaderProvider::new(provider);
            Arc::new(py_provider) as Arc<dyn lancedb::remote::HeaderProvider>
        });

        Self {
            user_agent: value.user_agent,
            retry_config: value.retry_config.map(Into::into).unwrap_or_default(),
            timeout_config: value.timeout_config.map(Into::into).unwrap_or_default(),
            extra_headers: value.extra_headers.unwrap_or_default(),
            id_delimiter: value.id_delimiter,
            tls_config: value.tls_config.map(Into::into),
            header_provider,
            user_id: value.user_id,
        }
    }
}
