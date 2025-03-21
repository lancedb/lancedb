// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::array::make_array;
use arrow::array::Array;
use arrow::array::ArrayData;
use arrow::pyarrow::FromPyArrow;
use arrow::pyarrow::IntoPyArrow;
use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::QueryExecutionOptions;
use lancedb::query::QueryFilter;
use lancedb::query::{
    ExecutableQuery, Query as LanceDbQuery, QueryBase, Select, VectorQuery as LanceDbVectorQuery,
};
use lancedb::table::AnyQuery;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{PyAnyMethods, PyDictMethods};
use pyo3::pymethods;
use pyo3::types::PyDict;
use pyo3::types::PyList;
use pyo3::Bound;
use pyo3::IntoPyObject;
use pyo3::PyAny;
use pyo3::PyRef;
use pyo3::PyResult;
use pyo3::{pyclass, PyErr};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::arrow::RecordBatchStream;
use crate::error::PythonErrorExt;
use crate::util::parse_distance_type;

// Python representation of full text search parameters
#[derive(Clone)]
#[pyclass(get_all)]
pub struct PyFullTextSearchQuery {
    pub columns: Vec<String>,
    pub query: String,
    pub limit: Option<i64>,
    pub wand_factor: Option<f32>,
}

impl From<FullTextSearchQuery> for PyFullTextSearchQuery {
    fn from(query: FullTextSearchQuery) -> Self {
        PyFullTextSearchQuery {
            columns: query.columns,
            query: query.query,
            limit: query.limit,
            wand_factor: query.wand_factor,
        }
    }
}

// Python representation of query vector(s)
#[derive(Clone)]
pub struct PyQueryVectors(Vec<Arc<dyn Array>>);

impl<'py> IntoPyObject<'py> for PyQueryVectors {
    type Target = PyList;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> PyResult<Self::Output> {
        let py_objs = self
            .0
            .into_iter()
            .map(|v| v.to_data().into_pyarrow(py))
            .collect::<Result<Vec<_>, _>>()?;
        PyList::new(py, py_objs)
    }
}

// Python representation of a query
#[pyclass(get_all)]
pub struct PyQueryRequest {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub filter: Option<PyQueryFilter>,
    pub full_text_search: Option<PyFullTextSearchQuery>,
    pub select: PySelect,
    pub fast_search: Option<bool>,
    pub with_row_id: Option<bool>,
    pub column: Option<String>,
    pub query_vector: Option<PyQueryVectors>,
    pub nprobes: Option<usize>,
    pub lower_bound: Option<f32>,
    pub upper_bound: Option<f32>,
    pub ef: Option<usize>,
    pub refine_factor: Option<u32>,
    pub distance_type: Option<String>,
    pub bypass_vector_index: Option<bool>,
    pub postfilter: Option<bool>,
    pub norm: Option<String>,
}

impl From<AnyQuery> for PyQueryRequest {
    fn from(query: AnyQuery) -> Self {
        match query {
            AnyQuery::Query(query_request) => PyQueryRequest {
                limit: query_request.limit,
                offset: query_request.offset,
                filter: query_request.filter.map(PyQueryFilter),
                full_text_search: query_request
                    .full_text_search
                    .map(PyFullTextSearchQuery::from),
                select: PySelect(query_request.select),
                fast_search: Some(query_request.fast_search),
                with_row_id: Some(query_request.with_row_id),
                column: None,
                query_vector: None,
                nprobes: None,
                lower_bound: None,
                upper_bound: None,
                ef: None,
                refine_factor: None,
                distance_type: None,
                bypass_vector_index: None,
                postfilter: None,
                norm: None,
            },
            AnyQuery::VectorQuery(vector_query) => PyQueryRequest {
                limit: vector_query.base.limit,
                offset: vector_query.base.offset,
                filter: vector_query.base.filter.map(PyQueryFilter),
                full_text_search: None,
                select: PySelect(vector_query.base.select),
                fast_search: Some(vector_query.base.fast_search),
                with_row_id: Some(vector_query.base.with_row_id),
                column: vector_query.column,
                query_vector: Some(PyQueryVectors(vector_query.query_vector)),
                nprobes: Some(vector_query.nprobes),
                lower_bound: vector_query.lower_bound,
                upper_bound: vector_query.upper_bound,
                ef: vector_query.ef,
                refine_factor: vector_query.refine_factor,
                distance_type: vector_query.distance_type.map(|d| d.to_string()),
                bypass_vector_index: Some(!vector_query.use_index),
                postfilter: Some(!vector_query.base.prefilter),
                norm: vector_query.base.norm.map(|n| n.to_string()),
            },
        }
    }
}

// Python representation of query selection
#[derive(Clone)]
pub struct PySelect(Select);

impl<'py> IntoPyObject<'py> for PySelect {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Select::All => Ok(py.None().into_bound(py).into_any()),
            Select::Columns(columns) => Ok(columns.into_pyobject(py)?.into_any()),
            Select::Dynamic(columns) => Ok(columns.into_pyobject(py)?.into_any()),
        }
    }
}

// Python representation of query filter
#[derive(Clone)]
pub struct PyQueryFilter(QueryFilter);

impl<'py> IntoPyObject<'py> for PyQueryFilter {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: pyo3::Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            QueryFilter::Datafusion(_) => Err(PyNotImplementedError::new_err(
                "Datafusion filter has no conversion to Python",
            )),
            QueryFilter::Sql(sql) => Ok(sql.into_pyobject(py)?.into_any()),
            QueryFilter::Substrait(substrait) => Ok(substrait.into_pyobject(py)?.into_any()),
        }
    }
}

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

        Ok(FTSQuery {
            fts_query,
            inner: self.inner.clone(),
        })
    }

    #[pyo3(signature = (max_batch_length=None))]
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

    pub fn to_query_request(&self) -> PyQueryRequest {
        PyQueryRequest::from(AnyQuery::Query(self.inner.clone().into_request()))
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

    #[pyo3(signature = (max_batch_length=None))]
    pub fn execute(
        self_: PyRef<'_, Self>,
        max_batch_length: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_
            .inner
            .clone()
            .full_text_search(self_.fts_query.clone());

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
        Ok(HybridQuery {
            inner_fts: self.clone(),
            inner_vec: vector_query,
        })
    }

    pub fn explain_plan(self_: PyRef<'_, Self>, verbose: bool) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .explain_plan(verbose)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
    }

    pub fn get_query(&self) -> String {
        self.fts_query.query.clone()
    }

    pub fn to_query_request(&self) -> PyQueryRequest {
        let mut req = self.inner.clone().into_request();
        req.full_text_search = Some(self.fts_query.clone());
        PyQueryRequest::from(AnyQuery::Query(req))
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

    #[pyo3(signature = (lower_bound=None, upper_bound=None))]
    pub fn distance_range(&mut self, lower_bound: Option<f32>, upper_bound: Option<f32>) {
        self.inner = self.inner.clone().distance_range(lower_bound, upper_bound);
    }

    pub fn ef(&mut self, ef: u32) {
        self.inner = self.inner.clone().ef(ef as usize);
    }

    pub fn bypass_vector_index(&mut self) {
        self.inner = self.inner.clone().bypass_vector_index()
    }

    #[pyo3(signature = (max_batch_length=None))]
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
        let base_query = self.inner.clone().into_plain();
        let fts_query = Query::new(base_query).nearest_to_text(query)?;
        Ok(HybridQuery {
            inner_vec: self.clone(),
            inner_fts: fts_query,
        })
    }

    pub fn to_query_request(&self) -> PyQueryRequest {
        PyQueryRequest::from(AnyQuery::VectorQuery(self.inner.clone().into_request()))
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

    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner_vec.select_columns(columns.clone());
        self.inner_fts.select_columns(columns);
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
            inner: self.inner_vec.inner.clone(),
        })
    }

    pub fn to_fts_query(&mut self) -> PyResult<FTSQuery> {
        Ok(FTSQuery {
            inner: self.inner_fts.inner.clone(),
            fts_query: self.inner_fts.fts_query.clone(),
        })
    }

    pub fn get_limit(&mut self) -> Option<u32> {
        self.inner_fts
            .inner
            .current_request()
            .limit
            .map(|i| i as u32)
    }

    pub fn get_with_row_id(&mut self) -> bool {
        self.inner_fts.inner.current_request().with_row_id
    }

    pub fn to_query_request(&self) -> PyQueryRequest {
        let mut req = self.inner_fts.to_query_request();
        let vec_req = self.inner_vec.to_query_request();
        req.query_vector = vec_req.query_vector;
        req.column = vec_req.column;
        req.distance_type = vec_req.distance_type;
        req.ef = vec_req.ef;
        req.refine_factor = vec_req.refine_factor;
        req.lower_bound = vec_req.lower_bound;
        req.upper_bound = vec_req.upper_bound;
        req
    }
}
