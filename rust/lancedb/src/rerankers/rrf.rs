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
use crate::rerankers::{RELEVANCE_SCORE, Reranker};

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
    /// Each result contributes `1 / (k + rank)`, with ranks starting at 1.
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
            let score = 1.0 / ((i + 1) as f32 + self.k);
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
        // With k = 1 and ranks starting at 1:
        // - foo = 1/2
        // - bar = 1/3 + 1/2
        // - baz = 1/4
        // - bean = 1/5 + 1/3
        // - dog = 1/6 + 1/4
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
            vec!["bar", "bean", "foo", "dog", "baz"]
        );

        let ids: UInt64Array = downcast_array(result.column(1));
        assert_eq!(
            ids.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![4, 5, 1, 3, 2]
        );

        let scores: Float32Array = downcast_array(result.column(2));
        assert_eq!(
            scores.iter().map(|e| e.unwrap()).collect::<Vec<_>>(),
            vec![
                1.0 / 3.0 + 1.0 / 2.0,
                1.0 / 5.0 + 1.0 / 3.0,
                1.0 / 2.0,
                1.0 / 6.0 + 1.0 / 4.0,
                1.0 / 4.0,
            ]
        );
    }

    #[tokio::test]
    async fn test_rrf_default_rank_order() {
        // Only rows 0 and 1 occur in both lists. Their ranks are (9, 6)
        // and (2, 14), respectively. Zero-based ranks reverse their order
        // even with the default k = 60.
        let vector_ids = vec![2, 1, 3, 4, 5, 6, 7, 8, 0];
        let fts_ids = vec![9, 10, 11, 12, 13, 0, 14, 15, 16, 17, 18, 19, 20, 1];
        let schema = Arc::new(Schema::new(vec![Field::new(
            ROW_ID,
            DataType::UInt64,
            false,
        )]));
        let vector_results = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vector_ids))],
        )
        .unwrap();
        let fts_results =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(fts_ids))]).unwrap();

        let result = RRFReranker::default()
            .rerank_hybrid("", vector_results, fts_results)
            .await
            .unwrap();
        let ids: UInt64Array = downcast_array(result.column_by_name(ROW_ID).unwrap());
        let scores: Float32Array = downcast_array(result.column_by_name(RELEVANCE_SCORE).unwrap());

        assert_eq!(result.num_rows(), 21);
        assert_eq!(&ids.values()[..2], &[0, 1]);
        assert_eq!(scores.value(0), 1.0 / 69.0 + 1.0 / 66.0);
        assert_eq!(scores.value(1), 1.0 / 62.0 + 1.0 / 74.0);
    }
}
