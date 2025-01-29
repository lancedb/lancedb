// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lancedb::index::scalar::FullTextSearchQuery;
use lancedb::query::ExecutableQuery;
use lancedb::query::Query as LanceDbQuery;
use lancedb::query::QueryBase;
use lancedb::query::QueryExecutionOptions;
use lancedb::query::Select;
use lancedb::query::VectorQuery as LanceDbVectorQuery;
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::error::convert_error;
use crate::error::NapiErrorExt;
use crate::iterator::RecordBatchIterator;
use crate::rerankers::Reranker;
use crate::rerankers::RerankerCallbacks;
use crate::util::parse_distance_type;

#[napi]
pub struct Query {
    inner: LanceDbQuery,
}

#[napi]
impl Query {
    pub fn new(query: LanceDbQuery) -> Self {
        Self { inner: query }
    }

    // We cannot call this r#where because NAPI gets confused by the r#
    #[napi]
    pub fn only_if(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    #[napi]
    pub fn full_text_search(&mut self, query: String, columns: Option<Vec<String>>) {
        let query = FullTextSearchQuery::new(query).columns(columns);
        self.inner = self.inner.clone().full_text_search(query);
    }

    #[napi]
    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    #[napi]
    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    #[napi]
    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    #[napi]
    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    #[napi]
    pub fn nearest_to(&mut self, vector: Float32Array) -> Result<VectorQuery> {
        let inner = self
            .inner
            .clone()
            .nearest_to(vector.as_ref())
            .default_error()?;
        Ok(VectorQuery { inner })
    }

    #[napi]
    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    #[napi]
    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    #[napi(catch_unwind)]
    pub async fn execute(
        &self,
        max_batch_length: Option<u32>,
    ) -> napi::Result<RecordBatchIterator> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        let inner_stream = self
            .inner
            .execute_with_options(execution_opts)
            .await
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "Failed to execute query stream: {}",
                    convert_error(&e)
                ))
            })?;
        Ok(RecordBatchIterator::new(inner_stream))
    }

    #[napi]
    pub async fn explain_plan(&self, verbose: bool) -> napi::Result<String> {
        self.inner.explain_plan(verbose).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to retrieve the query plan: {}",
                convert_error(&e)
            ))
        })
    }
}

#[napi]
pub struct VectorQuery {
    inner: LanceDbVectorQuery,
}

#[napi]
impl VectorQuery {
    #[napi]
    pub fn column(&mut self, column: String) {
        self.inner = self.inner.clone().column(&column);
    }

    #[napi]
    pub fn add_query_vector(&mut self, vector: Float32Array) -> Result<()> {
        self.inner = self
            .inner
            .clone()
            .add_query_vector(vector.as_ref())
            .default_error()?;
        Ok(())
    }

    #[napi]
    pub fn distance_type(&mut self, distance_type: String) -> napi::Result<()> {
        let distance_type = parse_distance_type(distance_type)?;
        self.inner = self.inner.clone().distance_type(distance_type);
        Ok(())
    }

    #[napi]
    pub fn postfilter(&mut self) {
        self.inner = self.inner.clone().postfilter();
    }

    #[napi]
    pub fn refine_factor(&mut self, refine_factor: u32) {
        self.inner = self.inner.clone().refine_factor(refine_factor);
    }

    #[napi]
    pub fn nprobes(&mut self, nprobe: u32) {
        self.inner = self.inner.clone().nprobes(nprobe as usize);
    }

    #[napi]
    pub fn distance_range(&mut self, lower_bound: Option<f64>, upper_bound: Option<f64>) {
        // napi doesn't support f32, so we have to convert to f32
        self.inner = self
            .inner
            .clone()
            .distance_range(lower_bound.map(|v| v as f32), upper_bound.map(|v| v as f32));
    }

    #[napi]
    pub fn ef(&mut self, ef: u32) {
        self.inner = self.inner.clone().ef(ef as usize);
    }

    #[napi]
    pub fn bypass_vector_index(&mut self) {
        self.inner = self.inner.clone().bypass_vector_index()
    }

    #[napi]
    pub fn only_if(&mut self, predicate: String) {
        self.inner = self.inner.clone().only_if(predicate);
    }

    #[napi]
    pub fn full_text_search(&mut self, query: String, columns: Option<Vec<String>>) {
        let query = FullTextSearchQuery::new(query).columns(columns);
        self.inner = self.inner.clone().full_text_search(query);
    }

    #[napi]
    pub fn select(&mut self, columns: Vec<(String, String)>) {
        self.inner = self.inner.clone().select(Select::dynamic(&columns));
    }

    #[napi]
    pub fn select_columns(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(Select::columns(&columns));
    }

    #[napi]
    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    #[napi]
    pub fn offset(&mut self, offset: u32) {
        self.inner = self.inner.clone().offset(offset as usize);
    }

    #[napi]
    pub fn fast_search(&mut self) {
        self.inner = self.inner.clone().fast_search();
    }

    #[napi]
    pub fn with_row_id(&mut self) {
        self.inner = self.inner.clone().with_row_id();
    }

    #[napi]
    pub fn rerank(&mut self, callbacks: RerankerCallbacks) {
        self.inner = self
            .inner
            .clone()
            .rerank(Arc::new(Reranker::new(callbacks)));
    }

    #[napi(catch_unwind)]
    pub async fn execute(
        &self,
        max_batch_length: Option<u32>,
    ) -> napi::Result<RecordBatchIterator> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        let inner_stream = self
            .inner
            .execute_with_options(execution_opts)
            .await
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "Failed to execute query stream: {}",
                    convert_error(&e)
                ))
            })?;
        Ok(RecordBatchIterator::new(inner_stream))
    }

    #[napi]
    pub async fn explain_plan(&self, verbose: bool) -> napi::Result<String> {
        self.inner.explain_plan(verbose).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to retrieve the query plan: {}",
                convert_error(&e)
            ))
        })
    }
}
