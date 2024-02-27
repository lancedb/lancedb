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

use std::time::Duration;

use lancedb::connection::Connection as LanceConnection;
use pyo3::{pyclass, pyfunction, pymethods, PyAny, PyRef, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;

use crate::error::PythonErrorExt;

#[pyclass]
pub struct Connection {
    inner: LanceConnection,
}

#[pymethods]
impl Connection {
    pub fn table_names(self_: PyRef<'_, Self>) -> PyResult<&PyAny> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.table_names().await.infer_error()
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
