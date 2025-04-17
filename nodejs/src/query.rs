// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lancedb::index::scalar::{
    BoostQuery, FtsQuery, FullTextSearchQuery, MatchQuery, MultiMatchQuery, PhraseQuery,
};
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
    pub fn full_text_search(&mut self, query: napi::JsObject) -> napi::Result<()> {
        let query = parse_fts_query(query)?;
        self.inner = self.inner.clone().full_text_search(query);
        Ok(())
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
        timeout_ms: Option<u32>,
    ) -> napi::Result<RecordBatchIterator> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        if let Some(timeout_ms) = timeout_ms {
            execution_opts.timeout = Some(std::time::Duration::from_millis(timeout_ms as u64))
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

    #[napi(catch_unwind)]
    pub async fn analyze_plan(&self) -> napi::Result<String> {
        self.inner.analyze_plan().await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute analyze plan: {}",
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
    pub fn full_text_search(&mut self, query: napi::JsObject) -> napi::Result<()> {
        let query = parse_fts_query(query)?;
        self.inner = self.inner.clone().full_text_search(query);
        Ok(())
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
        timeout_ms: Option<u32>,
    ) -> napi::Result<RecordBatchIterator> {
        let mut execution_opts = QueryExecutionOptions::default();
        if let Some(max_batch_length) = max_batch_length {
            execution_opts.max_batch_length = max_batch_length;
        }
        if let Some(timeout_ms) = timeout_ms {
            execution_opts.timeout = Some(std::time::Duration::from_millis(timeout_ms as u64))
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

    #[napi(catch_unwind)]
    pub async fn analyze_plan(&self) -> napi::Result<String> {
        self.inner.analyze_plan().await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to execute analyze plan: {}",
                convert_error(&e)
            ))
        })
    }
}

#[napi]
#[derive(Debug, Clone)]
pub struct JsFullTextQuery {
    pub(crate) inner: FtsQuery,
}

#[napi]
impl JsFullTextQuery {
    #[napi(factory)]
    pub fn match_query(
        query: String,
        column: String,
        boost: f64,
        fuzziness: Option<u32>,
        max_expansions: u32,
    ) -> napi::Result<Self> {
        Ok(Self {
            inner: MatchQuery::new(query)
                .with_column(Some(column))
                .with_boost(boost as f32)
                .with_fuzziness(fuzziness)
                .with_max_expansions(max_expansions as usize)
                .into(),
        })
    }

    #[napi(factory)]
    pub fn phrase_query(query: String, column: String) -> napi::Result<Self> {
        Ok(Self {
            inner: PhraseQuery::new(query).with_column(Some(column)).into(),
        })
    }

    #[napi(factory)]
    #[allow(clippy::use_self)] // NAPI doesn't allow Self here but clippy reports it
    pub fn boost_query(
        positive: &JsFullTextQuery,
        negative: &JsFullTextQuery,
        negative_boost: Option<f64>,
    ) -> napi::Result<Self> {
        Ok(Self {
            inner: BoostQuery::new(
                positive.inner.clone(),
                negative.inner.clone(),
                negative_boost.map(|v| v as f32),
            )
            .into(),
        })
    }

    #[napi(factory)]
    pub fn multi_match_query(
        query: String,
        columns: Vec<String>,
        boosts: Option<Vec<f64>>,
    ) -> napi::Result<Self> {
        let q = match boosts {
            Some(boosts) => MultiMatchQuery::try_new(
                query,
                columns,
            ).and_then(|q| q.try_with_boosts(boosts.into_iter().map(|v| v as f32).collect())),
            None => MultiMatchQuery::try_new(query, columns),
        }
        .map_err(|e| {
            napi::Error::from_reason(format!("Failed to create multi match query: {}", e))
        })?;

        Ok(Self { inner: q.into() })
    }
}

fn parse_fts_query(query: napi::JsObject) -> napi::Result<FullTextSearchQuery> {
    if let Ok(Some(query)) = query.get::<_, &JsFullTextQuery>("query") {
        Ok(FullTextSearchQuery::new_query(query.inner.clone()))
    } else if let Ok(Some(query_text)) = query.get::<_, String>("query") {
        let mut query_text = query_text;
        let columns = query.get::<_, Option<Vec<String>>>("columns")?.flatten();

        let is_phrase =
            query_text.len() >= 2 && query_text.starts_with('"') && query_text.ends_with('"');
        let is_multi_match = columns.as_ref().map(|cols| cols.len() > 1).unwrap_or(false);

        if is_phrase {
            // Remove the surrounding quotes for phrase queries
            query_text = query_text[1..query_text.len() - 1].to_string();
        }

        let query: FtsQuery = match (is_phrase, is_multi_match) {
            (false, _) => MatchQuery::new(query_text).into(),
            (true, false) => PhraseQuery::new(query_text).into(),
            (true, true) => {
                return Err(napi::Error::from_reason(
                    "Phrase queries cannot be used with multiple columns.",
                ));
            }
        };
        let mut query = FullTextSearchQuery::new_query(query);
        if let Some(cols) = columns {
            if !cols.is_empty() {
                query = query.with_columns(&cols).map_err(|e| {
                    napi::Error::from_reason(format!(
                        "Failed to set full text search columns: {}",
                        e
                    ))
                })?;
            }
        }
        Ok(query)
    } else {
        Err(napi::Error::from_reason(
            "Invalid full text search query object".to_string(),
        ))
    }
}
