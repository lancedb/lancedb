// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::{datatypes::Schema, ffi_stream::ArrowArrayStreamReader, pyarrow::FromPyArrow};
use lancedb::{connection::Connection as LanceConnection, database::CreateTableMode};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pyfunction, pymethods, Bound, FromPyObject, PyAny, PyRef, PyResult, Python,
};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{error::PythonErrorExt, table::Table};

#[pyclass]
pub struct Connection {
    inner: Option<LanceConnection>,
}

impl Connection {
    pub(crate) fn new(inner: LanceConnection) -> Self {
        Self { inner: Some(inner) }
    }

    fn get_inner(&self) -> PyResult<&LanceConnection> {
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

    #[pyo3(signature = (start_after=None, limit=None))]
    pub fn table_names(
        self_: PyRef<'_, Self>,
        start_after: Option<String>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let mut op = inner.table_names();
        if let Some(start_after) = start_after {
            op = op.start_after(start_after);
        }
        if let Some(limit) = limit {
            op = op.limit(limit);
        }
        future_into_py(self_.py(), async move { op.execute().await.infer_error() })
    }

    #[pyo3(signature = (name, mode, data, storage_options=None))]
    pub fn create_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        data: Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let batches = ArrowArrayStreamReader::from_pyarrow_bound(&data)?;
        let mut builder = inner.create_table(name, batches).mode(mode);

        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[pyo3(signature = (name, mode, schema, storage_options=None))]
    pub fn create_empty_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        schema: Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let schema = Schema::from_pyarrow_bound(&schema)?;

        let mut builder = inner.create_empty_table(name, Arc::new(schema)).mode(mode);

        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[pyo3(signature = (name, storage_options = None, index_cache_size = None))]
    pub fn open_table(
        self_: PyRef<'_, Self>,
        name: String,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        let mut builder = inner.open_table(name);
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }
        if let Some(index_cache_size) = index_cache_size {
            builder = builder.index_cache_size(index_cache_size);
        }
        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    pub fn rename_table(
        self_: PyRef<'_, Self>,
        old_name: String,
        new_name: String,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(self_.py(), async move {
            inner.rename_table(old_name, new_name).await.infer_error()
        })
    }

    pub fn drop_table(self_: PyRef<'_, Self>, name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(self_.py(), async move {
            inner.drop_table(name).await.infer_error()
        })
    }

    pub fn drop_all_tables(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(self_.py(), async move {
            inner.drop_all_tables().await.infer_error()
        })
    }
}

#[pyfunction]
#[pyo3(signature = (uri, api_key=None, region=None, host_override=None, read_consistency_interval=None, client_config=None, storage_options=None))]
#[allow(clippy::too_many_arguments)]
pub fn connect(
    py: Python,
    uri: String,
    api_key: Option<String>,
    region: Option<String>,
    host_override: Option<String>,
    read_consistency_interval: Option<f64>,
    client_config: Option<PyClientConfig>,
    storage_options: Option<HashMap<String, String>>,
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
            builder = builder.read_consistency_interval(Some(read_consistency_interval));
        } else {
            builder = builder.read_consistency_interval(None);
        }
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }
        #[cfg(feature = "remote")]
        if let Some(client_config) = client_config {
            builder = builder.client_config(client_config.into());
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
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    pool_idle_timeout: Option<Duration>,
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
            connect_timeout: value.connect_timeout,
            read_timeout: value.read_timeout,
            pool_idle_timeout: value.pool_idle_timeout,
        }
    }
}

#[cfg(feature = "remote")]
impl From<PyClientConfig> for lancedb::remote::ClientConfig {
    fn from(value: PyClientConfig) -> Self {
        Self {
            user_agent: value.user_agent,
            retry_config: value.retry_config.map(Into::into).unwrap_or_default(),
            timeout_config: value.timeout_config.map(Into::into).unwrap_or_default(),
            extra_headers: value.extra_headers.unwrap_or_default(),
        }
    }
}
