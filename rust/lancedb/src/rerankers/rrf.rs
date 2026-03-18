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
use crate::rerankers::{RELEVANCE_SCORE, Reranker, ReturnScore};

/// Reranks the results using Reciprocal Rank Fusion(RRF) algorithm based
/// on the scores of vector and FTS search.
///
/// # Parameters
///
/// - `k`: A constant used in the RRF formula (default `60`). Experiments
///   indicate that `k = 60` was near-optimal, but the choice is not critical.
///   See paper: <https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf>
/// - `return_score`: Controls which score columns appear in the output.
///   - [`ReturnScore::Relevance`] (default): preserves the original merge
///     behavior — the output contains `_relevance_score` and `_score` (FTS),
///     but not `_distance` (vector), because the merge uses the FTS schema
///     as its base.
///   - [`ReturnScore::All`]: retains every raw score column. Missing columns
///     (`_distance` for FTS rows, `_score` for vector-only rows) are filled
///     with `null` so both result sets can be concatenated.
#[derive(Debug)]
pub struct RRFReranker {
    k: f32,
    return_score: ReturnScore,
}

impl RRFReranker {
    /// Create a new [`RRFReranker`] with the given `k` value and
    /// [`ReturnScore::Relevance`] (default behavior, preserves original merge
    /// output).
    ///
    /// The parameter `k` is a constant used in the RRF formula (default is
    /// `60`). Experiments indicate that `k = 60` was near-optimal, but that
    /// the choice is not critical. See paper:
    /// <https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf>
    pub fn new(k: f32) -> Self {
        Self {
            k,
            return_score: ReturnScore::Relevance,
        }
    }

    /// Create a new [`RRFReranker`] specifying both `k` and `return_score`.
    ///
    /// ```
    /// # use lancedb::rerankers::rrf::RRFReranker;
    /// # use lancedb::rerankers::ReturnScore;
    /// let reranker = RRFReranker::new_with_score(60.0, ReturnScore::All);
    /// ```
    pub fn new_with_score(k: f32, return_score: ReturnScore) -> Self {
        Self { k, return_score }
    }

    /// Merge vector and FTS results keeping all score columns.
    ///
    /// Before concatenating, each side is padded with null-filled columns for
    /// scores it doesn't already carry:
    /// - vector results get a null `_score` column if absent
    /// - FTS results get a null `_distance` column if absent
    ///
    /// This ensures both `_distance` and `_score` survive the merge.
    fn merge_results_all_scores(
        &self,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> Result<RecordBatch> {
        use arrow_array::new_null_array;
        use arrow_schema::SchemaBuilder;

        let add_null_column =
            |batch: &RecordBatch, col_name: &str, dtype: DataType| -> Result<RecordBatch> {
                if batch.schema().column_with_name(col_name).is_some() {
                    return Ok(batch.clone());
                }
                let null_col = new_null_array(&dtype, batch.num_rows());
                let mut builder = SchemaBuilder::from(batch.schema().fields());
                builder.push(Arc::new(Field::new(col_name, dtype, true)));
                let new_schema = Arc::new(builder.finish());
                let mut cols = batch.columns().to_vec();
                cols.push(null_col);
                Ok(RecordBatch::try_new(new_schema, cols)?)
            };

        let vector_results = add_null_column(&vector_results, "_score", DataType::Float32)?;
        let fts_results = add_null_column(&fts_results, "_distance", DataType::Float32)?;

        // Reorder fts columns to match vector schema so concat_batches is happy
        let vec_schema = vector_results.schema();
        let fts_reordered = {
            let indices: std::result::Result<Vec<usize>, _> = vec_schema
                .fields()
                .iter()
                .map(|f| {
                    fts_results
                        .schema()
                        .index_of(f.name())
                        .map_err(|_| Error::InvalidInput {
                            message: format!(
                                "column '{}' missing from fts_results after padding",
                                f.name()
                            ),
                        })
                })
                .collect();
            let indices = indices?;
            let cols: Vec<_> = indices
                .iter()
                .map(|&i| fts_results.column(i).clone())
                .collect();
            RecordBatch::try_new(vec_schema.clone(), cols)?
        };

        self.merge_results(vector_results, fts_reordered)
    }
}

impl Default for RRFReranker {
    fn default() -> Self {
        Self {
            k: 60.0,
            return_score: ReturnScore::Relevance,
        }
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

        // For ReturnScore::All, merge while preserving all score columns.
        // For ReturnScore::Relevance, use the original merge (FTS schema as base).
        let combined_results = if self.return_score == ReturnScore::All {
            self.merge_results_all_scores(vector_results, fts_results)?
        } else {
            self.merge_results(vector_results, fts_results)?
        };

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
    use arrow_array::{Float32Array, StringArray};

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

    /// `ReturnScore::Relevance` (default) preserves the original merge behavior:
    /// `_score` (FTS) is kept, `_distance` is not present (FTS schema is used as
    /// the merge base so the vector `_distance` column is dropped naturally).
    #[tokio::test]
    async fn test_rrf_return_score_relevance_preserves_original_behavior() {
        let vec_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
            Field::new("_distance", DataType::Float32, true),
        ]));
        let fts_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
            Field::new("_score", DataType::Float32, true),
        ]));

        let vec_results = RecordBatch::try_new(
            vec_schema,
            vec![
                Arc::new(StringArray::from(vec!["foo", "bar"])),
                Arc::new(UInt64Array::from(vec![1u64, 2u64])),
                Arc::new(Float32Array::from(vec![0.1f32, 0.2f32])),
            ],
        )
        .unwrap();

        let fts_results = RecordBatch::try_new(
            fts_schema,
            vec![
                Arc::new(StringArray::from(vec!["bar", "baz"])),
                Arc::new(UInt64Array::from(vec![2u64, 3u64])),
                Arc::new(Float32Array::from(vec![0.9f32, 0.8f32])),
            ],
        )
        .unwrap();

        // default constructor → ReturnScore::Relevance = no extra processing
        let reranker = RRFReranker::new(1.0);
        let result = reranker
            .rerank_hybrid("", vec_results, fts_results)
            .await
            .unwrap();

        let schema = result.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // original behavior: _score survives, _distance is dropped by merge
        assert!(
            field_names.contains(&"_score"),
            "_score should be present: {:?}",
            field_names
        );
        assert!(
            !field_names.contains(&"_distance"),
            "_distance should not be present (original behavior): {:?}",
            field_names
        );
        assert!(
            field_names.contains(&RELEVANCE_SCORE),
            "_relevance_score should be present: {:?}",
            field_names
        );
    }

    /// `ReturnScore::All` retains both `_distance` (vector) and `_score` (FTS)
    /// by padding missing columns with nulls before merging.
    #[tokio::test]
    async fn test_rrf_return_score_all_keeps_both_raw_scores() {
        let vec_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
            Field::new("_distance", DataType::Float32, true),
        ]));
        let fts_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
            Field::new("_score", DataType::Float32, true),
        ]));

        let vec_results = RecordBatch::try_new(
            vec_schema,
            vec![
                Arc::new(StringArray::from(vec!["foo", "bar"])),
                Arc::new(UInt64Array::from(vec![1u64, 2u64])),
                Arc::new(Float32Array::from(vec![0.1f32, 0.2f32])),
            ],
        )
        .unwrap();

        let fts_results = RecordBatch::try_new(
            fts_schema,
            vec![
                Arc::new(StringArray::from(vec!["bar", "baz"])),
                Arc::new(UInt64Array::from(vec![2u64, 3u64])),
                Arc::new(Float32Array::from(vec![0.9f32, 0.8f32])),
            ],
        )
        .unwrap();

        let reranker = RRFReranker::new_with_score(1.0, ReturnScore::All);
        let result = reranker
            .rerank_hybrid("", vec_results, fts_results)
            .await
            .unwrap();

        let schema = result.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(
            field_names.contains(&"_distance"),
            "_distance should be present: {:?}",
            field_names
        );
        assert!(
            field_names.contains(&"_score"),
            "_score should be present: {:?}",
            field_names
        );
        assert!(
            field_names.contains(&RELEVANCE_SCORE),
            "_relevance_score should be present: {:?}",
            field_names
        );
    }
}
