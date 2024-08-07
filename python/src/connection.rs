// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use arrow::{datatypes::Schema, ffi_stream::ArrowArrayStreamReader, pyarrow::FromPyArrow};
use lancedb::connection::{Connection as LanceConnection, CreateTableMode, LanceFileVersion};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pyfunction, pymethods, Bound, PyAny, PyRef, PyResult, Python,
};
use pyo3_asyncio_0_21::tokio::future_into_py;

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

    pub fn create_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        data: Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
        data_storage_version: Option<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let batches = ArrowArrayStreamReader::from_pyarrow_bound(&data)?;
        let mut builder = inner.create_table(name, batches).mode(mode);

        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        if let Some(data_storage_version) = data_storage_version.as_ref() {
            builder = builder.data_storage_version(
                LanceFileVersion::from_str(data_storage_version)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            );
        }

        future_into_py(self_.py(), async move {
            let table = builder.execute().await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    pub fn create_empty_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        schema: Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
        data_storage_version: Option<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.get_inner()?.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let schema = Schema::from_pyarrow_bound(&schema)?;

        let mut builder = inner.create_empty_table(name, Arc::new(schema)).mode(mode);

        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }

        if let Some(data_storage_version) = data_storage_version.as_ref() {
            builder = builder.data_storage_version(
                LanceFileVersion::from_str(data_storage_version)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?,
            );
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

    pub fn drop_table(self_: PyRef<'_, Self>, name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(self_.py(), async move {
            inner.drop_table(name).await.infer_error()
        })
    }

    pub fn drop_db(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.get_inner()?.clone();
        future_into_py(
            self_.py(),
            async move { inner.drop_db().await.infer_error() },
        )
    }
}

#[pyfunction]
pub fn connect(
    py: Python,
    uri: String,
    api_key: Option<String>,
    region: Option<String>,
    host_override: Option<String>,
    read_consistency_interval: Option<f64>,
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
            builder = builder.read_consistency_interval(read_consistency_interval);
        }
        if let Some(storage_options) = storage_options {
            builder = builder.storage_options(storage_options);
        }
        Ok(Connection::new(builder.execute().await.infer_error()?))
    })
}
