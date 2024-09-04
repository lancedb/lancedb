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

use arrow::array::make_array;
use arrow::array::ArrayData;
use arrow::pyarrow::FromPyArrow;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::QueryExecutionOptions;
use lancedb::query::{
    ExecutableQuery, Query as LanceDbQuery, QueryBase, Select, VectorQuery as LanceDbVectorQuery,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{PyAnyMethods, PyDictMethods};
use pyo3::pymethods;
use pyo3::types::PyDict;
use pyo3::Bound;
use pyo3::PyAny;
use pyo3::PyRef;
use pyo3::PyResult;
use pyo3::{pyclass, PyErr};
use pyo3_asyncio_0_21::tokio::future_into_py;

use crate::arrow::RecordBatchStream;
use crate::error::PythonErrorExt;
use crate::util::parse_distance_type;

#[pyclass]
pub struct Query {
    inner: LanceDbQuery,
}

impl Query {
    pub fn new(query: LanceDbQuery) -> Self {
        Self { inner: query }
    }
}

#[pymethods]
impl Query {
    pub fn r#where(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    pub fn nearest_to(&mut self, vector: Bound<'_, PyAny>) -> PyResult<VectorQuery> {
        let data: ArrayData = ArrayData::from_pyarrow_bound(&vector)?;
        let array = make_array(data);
        let inner = self.inner.clone().nearest_to(array).infer_error()?;
        Ok(VectorQuery { inner })
    }

    pub fn nearest_to_text(&mut self, query: Bound<'_, PyDict>) -> PyResult<()> {
        let query_text = query
            .get_item("query")?
            .ok_or(PyErr::new::<PyRuntimeError, _>(
                "Query text is required for nearest_to_text",
            ))?
            .extract::<String>()?;
        let columns = query
            .get_item("columns")?
            .map(|columns| columns.extract::<Vec<String>>())
            .transpose()?;

        let fts_query = FullTextSearchQuery::new(query_text).columns(columns);
        self.inner = self.inner.clone().full_text_search(fts_query);

        Ok(())
    }

    pub fn execute(
        self_: PyRef<'_, Self>,
        max_batch_length: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut opts = QueryExecutionOptions::default();
            if let Some(max_batch_length) = max_batch_length {
                opts.max_batch_length = max_batch_length;
            }
            let inner_stream = inner.execute_with_options(opts).await.infer_error()?;
            Ok(RecordBatchStream::new(inner_stream))
        })
    }

    fn explain_plan(self_: PyRef<'_, Self>, verbose: bool) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .explain_plan(verbose)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}

#[pyclass]
pub struct VectorQuery {
    inner: LanceDbVectorQuery,
}

#[pymethods]
impl VectorQuery {
    pub fn r#where(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    pub fn column(&mut self, column: String) {
        self.inner = self.inner.clone().column(&column);
    }

    pub fn distance_type(&mut self, distance_type: String) -> PyResult<()> {
        let distance_type = parse_distance_type(distance_type)?;
        self.inner = self.inner.clone().distance_type(distance_type);
        Ok(())
    }

    pub fn postfilter(&mut self) {
        self.inner = self.inner.clone().postfilter();
    }

    pub fn refine_factor(&mut self, refine_factor: u32) {
        self.inner = self.inner.clone().refine_factor(refine_factor);
    }

    pub fn nprobes(&mut self, nprobe: u32) {
        self.inner = self.inner.clone().nprobes(nprobe as usize);
    }

    pub fn bypass_vector_index(&mut self) {
        self.inner = self.inner.clone().bypass_vector_index()
    }

    pub fn execute(
        self_: PyRef<'_, Self>,
        max_batch_length: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut opts = QueryExecutionOptions::default();
            if let Some(max_batch_length) = max_batch_length {
                opts.max_batch_length = max_batch_length;
            }
            let inner_stream = inner.execute_with_options(opts).await.infer_error()?;
            Ok(RecordBatchStream::new(inner_stream))
        })
    }

    fn explain_plan(self_: PyRef<'_, Self>, verbose: bool) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .explain_plan(verbose)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }
}
