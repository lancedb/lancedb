// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::BTreeSet, str::FromStr};

use arrow::{
    array::downcast_array,
    compute::{concat_batches, filter_record_batch},
};
use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
use async_trait::async_trait;
use lance::dataset::ROW_ID;

use crate::error::{Error, Result};

pub mod rrf;

/// column name for reranker relevance score
const RELEVANCE_SCORE: &str = "_relevance_score";

/// Controls which scores are returned in the reranker output.
///
/// - [`ReturnScore::Relevance`]: Only the `_relevance_score` column is kept.
///   The raw `_distance` and `_score` columns (from vector and FTS search
///   respectively) are dropped.
/// - [`ReturnScore::All`]: All score columns are retained alongside the
///   `_relevance_score`, which is useful for debugging.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ReturnScore {
    /// Return only the `_relevance_score` column (default).
    #[default]
    Relevance,
    /// Return `_relevance_score` plus all original score columns (`_distance`,
    /// `_score`).
    All,
}

impl FromStr for ReturnScore {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "relevance" => Ok(Self::Relevance),
            "all" => Ok(Self::All),
            _ => Err(Error::InvalidInput {
                message: format!(
                    "invalid return_score value: \"{}\". Expected \"relevance\" or \"all\"",
                    s
                ),
            }),
        }
    }
}

impl std::fmt::Display for ReturnScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relevance => write!(f, "relevance"),
            Self::All => write!(f, "all"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NormalizeMethod {
    Score,
    Rank,
}

impl FromStr for NormalizeMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "score" => Ok(Self::Score),
            "rank" => Ok(Self::Rank),
            _ => Err(Error::InvalidInput {
                message: format!("invalid normalize method: {}", s),
            }),
        }
    }
}

impl std::fmt::Display for NormalizeMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Score => write!(f, "score"),
            Self::Rank => write!(f, "rank"),
        }
    }
}

/// Interface for a reranker. A reranker is used to rerank the results from a
/// vector and FTS search. This is useful for combining the results from both
/// search methods.
#[async_trait]
pub trait Reranker: std::fmt::Debug + Sync + Send {
    // TODO support vector reranking and FTS reranking. Currently only hybrid reranking is supported.

    /// Rerank function receives the individual results from the vector and FTS search
    /// results. You can choose to use any of the results to generate the final results,
    /// allowing maximum flexibility.
    async fn rerank_hybrid(
        &self,
        query: &str,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> Result<RecordBatch>;

    /// Drop the raw `_distance` and `_score` columns from `batch`, keeping
    /// only the `_relevance_score` produced by the reranker.
    ///
    /// Call this at the end of [`Reranker::rerank_hybrid`] when
    /// `return_score == ReturnScore::Relevance`.
    fn keep_relevance_score(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let columns_to_drop = ["_distance", "_score"];
        let keep_indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !columns_to_drop.contains(&f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        let new_schema = arrow_schema::Schema::new(
            keep_indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect::<Vec<_>>(),
        );
        let new_columns = keep_indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(
            std::sync::Arc::new(new_schema),
            new_columns,
        )?)
    }

    fn merge_results(
        &self,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> Result<RecordBatch> {
        let combined = concat_batches(&fts_results.schema(), [vector_results, fts_results].iter())?;

        let mut mask = BooleanArray::builder(combined.num_rows());
        let mut unique_ids = BTreeSet::new();
        let row_ids = combined.column_by_name(ROW_ID).ok_or(Error::InvalidInput {
            message: format!(
                "could not find expected column {} while merging results. found columns {:?}",
                ROW_ID,
                combined
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ),
        })?;
        let row_ids: UInt64Array = downcast_array(row_ids);
        row_ids.values().iter().for_each(|id| {
            mask.append_value(unique_ids.insert(id));
        });

        let combined = filter_record_batch(&combined, &mask.finish())?;

        Ok(combined)
    }
}

pub fn check_reranker_result(result: &RecordBatch) -> Result<()> {
    if result.schema().column_with_name(RELEVANCE_SCORE).is_none() {
        return Err(Error::Schema {
            message: format!(
                "rerank_hybrid must return a RecordBatch with a column named {}",
                RELEVANCE_SCORE
            ),
        });
    }

    Ok(())
}
