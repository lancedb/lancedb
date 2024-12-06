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
    HasQuery,
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

    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    pub fn postfilter(&mut self) {
        self.inner = self.inner.clone().postfilter();
    }

    pub fn nearest_to(&mut self, vector: Bound<'_, PyAny>) -> PyResult<VectorQuery> {
        let data: ArrayData = ArrayData::from_pyarrow_bound(&vector)?;
        let array = make_array(data);
        let inner = self.inner.clone().nearest_to(array).infer_error()?;
        Ok(VectorQuery { inner })
    }

    pub fn nearest_to_text(&mut self, query: Bound<'_, PyDict>) -> PyResult<FTSQuery> {
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
        // self.inner = self.inner.clone().full_text_search(fts_query);

        Ok(FTSQuery{
            fts_query: fts_query,
            inner: self.inner.clone(),
        })
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
#[derive(Clone)]
pub struct FTSQuery {
    inner: LanceDbQuery,
    fts_query: FullTextSearchQuery,
}

#[pymethods]
impl FTSQuery {
    pub fn r#where(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    pub fn postfilter(&mut self) {
        self.inner = self.inner.clone().postfilter();
    }

    pub fn execute(
        self_: PyRef<'_, Self>,
        max_batch_length: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone().full_text_search(self_.fts_query.clone());

        future_into_py(self_.py(), async move {
            let mut opts = QueryExecutionOptions::default();
            if let Some(max_batch_length) = max_batch_length {
                opts.max_batch_length = max_batch_length;
            }
            let inner_stream = inner.execute_with_options(opts).await.infer_error()?;
            Ok(RecordBatchStream::new(inner_stream))
        })

    }

    pub fn nearest_to(&mut self, vector: Bound<'_, PyAny>) -> PyResult<HybridQuery> {
        let vector_query = Query::new(self.inner.clone()).nearest_to(vector)?;
        Ok(HybridQuery{
            inner_fts: self.clone(),
            inner_vec: vector_query,
        })
    }

    pub fn get_query(&self) -> String {
        self.fts_query.query.clone()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct VectorQuery {
    inner: LanceDbVectorQuery,
}

#[pymethods]
impl VectorQuery {
    pub fn r#where(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    pub fn add_query_vector(&mut self, vector: Bound<'_, PyAny>) -> PyResult<()> {
        let data: ArrayData = ArrayData::from_pyarrow_bound(&vector)?;
        let array = make_array(data);
        self.inner = self.inner.clone().add_query_vector(array).infer_error()?;
        Ok(())
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

    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
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

    pub fn ef(&mut self, ef: u32) {
        self.inner = self.inner.clone().ef(ef as usize);
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

    pub fn nearest_to_text(&mut self, query: Bound<'_, PyDict>) -> PyResult<HybridQuery> {
        let fts_query = Query::new(self.inner.mut_query().clone()).nearest_to_text(query)?;
        Ok(HybridQuery{
            inner_vec: self.clone(),
            inner_fts: fts_query,
        })
    }
}

#[pyclass]
pub struct HybridQuery {
    inner_vec: VectorQuery,
    inner_fts: FTSQuery,
}

#[pymethods]
impl HybridQuery {
    pub fn r#where(&mut self, predicate: String) {
        self.inner_vec.r#where(predicate.clone());
        self.inner_fts.r#where(predicate);
    }

    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner_vec.select(columns.clone());
        self.inner_fts.select(columns);
    }

    pub fn limit(&mut self, limit: u32) {
        self.inner_vec.limit(limit);
        self.inner_fts.limit(limit);
    }

    pub fn offset(&mut self, offset: u32) {
        self.inner_vec.offset(offset);
        self.inner_fts.offset(offset);
    }

    pub fn fast_search(&mut self) {
        self.inner_vec.fast_search();
        self.inner_fts.fast_search();
    }

    pub fn with_row_id(&mut self) {
        self.inner_fts.with_row_id();
        self.inner_vec.with_row_id();
    }

    pub fn postfilter(&mut self) {
        self.inner_vec.postfilter();
        self.inner_fts.postfilter();
    }
   
    pub fn add_query_vector(&mut self, vector: Bound<'_, PyAny>) -> PyResult<()> {
        self.inner_vec.add_query_vector(vector)
    }

    pub fn column(&mut self, column: String) {
        self.inner_vec.column(column);
    }

    pub fn distance_type(&mut self, distance_type: String) -> PyResult<()> {
        self.inner_vec.distance_type(distance_type)
    }

    pub fn refine_factor(&mut self, refine_factor: u32) {
        self.inner_vec.refine_factor(refine_factor);
    }

    pub fn nprobes(&mut self, nprobe: u32) {
        self.inner_vec.nprobes(nprobe);
    }

    pub fn ef(&mut self, ef: u32) {
        self.inner_vec.ef(ef);
    }

    pub fn bypass_vector_index(&mut self) {
        self.inner_vec.bypass_vector_index();
    }

    pub fn to_vector_query(&mut self) -> PyResult<VectorQuery> {
        Ok(VectorQuery {
            inner: self.inner_vec.inner.clone()
        })
    }

    pub fn to_fts_query(&mut self) -> PyResult<FTSQuery> {
        Ok(FTSQuery {
            inner: self.inner_fts.inner.clone(),
            fts_query: self.inner_fts.fts_query.clone()
        })
    }

    pub fn get_limit(&mut self) -> Option<u32> {
        self.inner_fts.inner.limit.map(|i| i as u32)
    }

    pub fn get_with_row_id(&mut self) -> bool {
        self.inner_fts.inner.with_row_id
    }
}