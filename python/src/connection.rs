// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{sync::Arc, time::Duration};

use arrow::{datatypes::Schema, ffi_stream::ArrowArrayStreamReader, pyarrow::FromPyArrow};
use lancedb::connection::{Connection as LanceConnection, CreateTableMode};
use pyo3::{
    exceptions::PyValueError, pyclass, pyfunction, pymethods, PyAny, PyRef, PyResult, Python,
};
use pyo3_asyncio::tokio::future_into_py;

use crate::{error::PythonErrorExt, table::Table};

#[pyclass]
pub struct Connection {
    inner: LanceConnection,
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
    pub fn table_names(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.table_names().await.infer_error()
        })
    }

    pub fn create_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        data: &PyAny,
    ) -> PyResult<&'a PyAny> {
        let inner = self_.inner.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let batches = Box::new(ArrowArrayStreamReader::from_pyarrow(data)?);
        future_into_py(self_.py(), async move {
            let table = inner
                .create_table(name, batches)
                .mode(mode)
                .execute()
                .await
                .infer_error()?;
            Ok(Table::new(table))
        })
    }

    pub fn create_empty_table<'a>(
        self_: PyRef<'a, Self>,
        name: String,
        mode: &str,
        schema: &PyAny,
    ) -> PyResult<&'a PyAny> {
        let inner = self_.inner.clone();

        let mode = Self::parse_create_mode_str(mode)?;

        let schema = Schema::from_pyarrow(schema)?;

        future_into_py(self_.py(), async move {
            let table = inner
                .create_empty_table(name, Arc::new(schema))
                .mode(mode)
                .execute()
                .await
                .infer_error()?;
            Ok(Table::new(table))
        })
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
) -> PyResult<&PyAny> {
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
        Ok(Connection {
            inner: builder.execute().await.infer_error()?,
        })
    })
}
