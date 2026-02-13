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

/// Interface for a reranker. A reranker is used to rerank search results
/// to improve relevance.
///
/// Rerankers can be applied to:
/// - Vector search results only ([`rerank_vector`])
/// - Full-text search (FTS) results only ([`rerank_fts`])
/// - Hybrid search results combining both ([`rerank_hybrid`])
///
/// # Default Implementations
///
/// The trait provides default implementations for [`rerank_vector`] and
/// [`rerank_fts`] that return a [`Error::NotSupported`] error. Implementors
/// can choose to override these methods to provide custom reranking logic.
///
/// All rerankers must implement [`rerank_hybrid`] for combining vector and
/// FTS search results.
///
/// # Examples
///
/// Using RRF reranker for vector search:
///
/// ```
/// use lancedb::rerankers::rrf::RRFReranker;
/// use lancedb::rerankers::Reranker;
///
/// # use arrow_array::RecordBatch;
/// # async fn example(vector_results: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
/// let reranker = RRFReranker::default();
/// let reranked = reranker.rerank_vector("query", vector_results).await?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait Reranker: std::fmt::Debug + Sync + Send {
    /// Rerank vector search results.
    ///
    /// This method receives vector search results and returns them reranked
    /// with a `_relevance_score` column added.
    ///
    /// # Default Implementation
    ///
    /// Returns [`Error::NotSupported`] by default. Override to provide
    /// vector-only reranking.
    async fn rerank_vector(
        &self,
        _query: &str,
        _vector_results: RecordBatch,
    ) -> Result<RecordBatch> {
        Err(Error::NotSupported {
            message: format!(
                "{:?} does not implement vector-only reranking. Only hybrid reranking is supported.",
                self
            ),
        })
    }

    /// Rerank full-text search (FTS) results.
    ///
    /// This method receives FTS search results and returns them reranked
    /// with a `_relevance_score` column added.
    ///
    /// # Default Implementation
    ///
    /// Returns [`Error::NotSupported`] by default. Override to provide
    /// FTS-only reranking.
    async fn rerank_fts(
        &self,
        _query: &str,
        _fts_results: RecordBatch,
    ) -> Result<RecordBatch> {
        Err(Error::NotSupported {
            message: format!(
                "{:?} does not implement FTS-only reranking. Only hybrid reranking is supported.",
                self
            ),
        })
    }

    /// Rerank function receives the individual results from the vector and FTS search
    /// results. You can choose to use any of the results to generate the final results,
    /// allowing maximum flexibility.
    async fn rerank_hybrid(
        &self,
        query: &str,
        vector_results: RecordBatch,
        fts_results: RecordBatch,
    ) -> Result<RecordBatch>;

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
