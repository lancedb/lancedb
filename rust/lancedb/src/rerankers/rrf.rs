// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::{
    array::downcast_array,
    compute::{sort_to_indices, take},
};
use arrow_array::{Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use async_trait::async_trait;
use lance::dataset::ROW_ID;

use crate::error::{Error, Result};
use crate::rerankers::{Reranker, RELEVANCE_SCORE};

/// Reranks the results using Reciprocal Rank Fusion(RRF) algorithm based
/// on the scores of vector and FTS search.
///
#[derive(Debug)]
pub struct RRFReranker {
    k: f32,
}

impl RRFReranker {
    /// Create a new RRFReranker
    ///
    /// The parameter k is a constant used in the RRF formula (default is 60).
    /// Experiments indicate that k = 60 was near-optimal, but that the choice
    /// is not critical. See paper:
    /// https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
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
    async fn rerank_hybrid(
        &self,
        _query: &str,
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

        // keep track of indices sorted by the relevance column
        let sort_indices = sort_to_indices(
            &relevance_scores,
            Some(SortOptions {
                descending: true,
                ..Default::default()
            }),
            None,
        )
        .unwrap();

        // add relevance scores to columns
        let mut columns = combined_results.columns().to_vec();
        columns.push(Arc::new(relevance_scores));

        // sort by the relevance scores
        let columns = columns
            .iter()
            .map(|c| take(c, &sort_indices, None).unwrap())
            .collect();

        // add relevance score to schema
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

#[cfg(test)]
pub mod test {
    use super::*;
    use arrow_array::StringArray;

    #[tokio::test]
    async fn test_rrf_reranker() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let vec_results = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["foo", "bar", "baz", "bean", "dog"])),
                Arc::new(UInt64Array::from(vec![1, 4, 2, 5, 3])),
            ],
        )
        .unwrap();

        let fts_results = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["bar", "bean", "dog"])),
                Arc::new(UInt64Array::from(vec![4, 5, 3])),
            ],
        )
        .unwrap();

        // scores should be calculated as:
        // - foo = 1/1        = 1.0
        // - bar = 1/2 + 1/1  = 1.5
        // - baz = 1/3        = 0.333
        // - bean = 1/4 + 1/2 = 0.75
        // - dog = 1/5 + 1/3  = 0.533
        // then we should get the result ranked in descending order

        let reranker = RRFReranker::new(1.0);

        let result = reranker
            .rerank_hybrid("", vec_results, fts_results)
            .await
            .unwrap();

        assert_eq!(3, result.schema().fields().len());
        assert_eq!("name", result.schema().fields().first().unwrap().name());
        assert_eq!(ROW_ID, result.schema().fields().get(1).unwrap().name());
        assert_eq!(
            RELEVANCE_SCORE,
            result.schema().fields().get(2).unwrap().name()
        );

        let names: StringArray = downcast_array(result.column(0));
        assert_eq!(
            names.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec!["bar", "foo", "bean", "dog", "baz"]
        );

        let ids: UInt64Array = downcast_array(result.column(1));
        assert_eq!(
            ids.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![4, 1, 5, 3, 2]
        );

        let scores: Float32Array = downcast_array(result.column(2));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![1.5, 1.0, 0.75, 1.0 / 5.0 + 1.0 / 3.0, 1.0 / 3.0]
        );
    }
}
