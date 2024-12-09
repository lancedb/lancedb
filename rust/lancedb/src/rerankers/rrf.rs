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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::downcast_array;
use arrow_array::{Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use lance::dataset::ROW_ID;

use crate::error::{Error, Result};
use crate::rerankers::{Reranker, RELEVANCE_SCORE};

#[derive(Debug)]
pub struct RRFReranker {
    k: f32,
}

impl RRFReranker {
    pub fn new(k: f32) -> Self {
        Self { k }
    }
}

impl Default for RRFReranker {
    fn default() -> Self {
        Self { k: 60.0 }
    }
}

#[async_trait]
impl Reranker for RRFReranker {
    /// TODO api comments
    async fn rerank_vector(
        &self,
        _query: &str,
        _vector_results: RecordBatch,
    ) -> Result<RecordBatch> {
        Err(Error::NotSupported {
            message:
                "RRFReranker is not supported for vector search (only Hybrid search is supporrted)"
                    .to_string(),
        })
    }

    /// TODO api comments
    async fn rerank_fts(&self, _query: &str, _fts_results: RecordBatch) -> Result<RecordBatch> {
        Err(Error::NotSupported {
            message:
                "RRFReranker is not supported for vector search (only Hybrid search is supported)"
                    .to_string(),
        })
    }

    /// TODO api comments
    async fn rerank_hybrid(
        &self,
        query: &str,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> Result<RecordBatch> {
        let vector_ids = vector_results
            .column_by_name(ROW_ID)
            .ok_or(Error::InvalidInput {
                message: format!(
                    "expected column {} not found in vector_results. found columns {:?}",
                    ROW_ID,
                    vector_results
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                ),
            })?;
        let fts_ids = fts_results
            .column_by_name(ROW_ID)
            .ok_or(Error::InvalidInput {
                message: format!(
                    "expected column {} not found in fts_results. found columns {:?}",
                    ROW_ID,
                    fts_results
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                ),
            })?;

        let vector_ids: UInt64Array = downcast_array(&vector_ids);
        let fts_ids: UInt64Array = downcast_array(&fts_ids);

        let mut rrf_score_map = BTreeMap::new();
        let mut update_score_map = |(i, result_id)| {
            let score = 1.0 / (i as f32 + self.k);
            rrf_score_map
                .entry(result_id)
                .and_modify(|e| *e += score)
                .or_insert(score);
        };
        vector_ids
            .values()
            .iter()
            .enumerate()
            .for_each(&mut update_score_map);
        fts_ids
            .values()
            .iter()
            .enumerate()
            .for_each(&mut update_score_map);

        let combined_results = self.merge_results(vector_results, fts_results)?;

        let combined_row_ids: UInt64Array =
            downcast_array(combined_results.column_by_name(ROW_ID).unwrap());
        let relevance_scores = Float32Array::from_iter_values(
            combined_row_ids
                .values()
                .iter()
                .map(|row_id| rrf_score_map.get(row_id).unwrap())
                .copied(),
        );

        let mut columns = combined_results.columns().to_vec();
        columns.push(Arc::new(relevance_scores));

        let mut fields = combined_results.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            RELEVANCE_SCORE,
            DataType::Float32,
            false,
        )));

        let schema = Schema::new(fields);
        let combined_results = RecordBatch::try_new(Arc::new(schema), columns)?;

        Ok(combined_results)
    }
}
