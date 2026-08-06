// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

pub mod builder;
mod cache_codec;
mod compound;
mod documents;
mod encoding;
mod impact;
mod index;
mod iter;
pub mod json;
pub mod parser;
pub mod query;
mod scorer;
pub mod tokenizer;
mod wand;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use async_trait::async_trait;
pub use builder::InvertedIndexBuilder;
pub use compound::{compound_search, compound_search_with_base_scorer};
use datafusion::execution::SendableRecordBatchStream;
pub use index::*;
use lance_core::{Result, cache::LanceCache};
pub use lance_tokenizer::Language;
pub use scorer::{MemBM25Scorer, Scorer};
pub use tokenizer::*;

use crate::scalar::inverted::query::{FtsSearchParams, Tokens};

/// Collect the unique terms needed to build a shared BM25 scorer.
///
/// The scorer only needs corpus-level document frequencies, so we keep a
/// deduplicated term list here instead of constructing a full `Tokens`
/// object with positions. When fuzziness is enabled, each segment may
/// contribute additional terms (via `expand_fuzzy_tokens`); the union of
/// those terms is what the global scorer must cover.
fn scorer_terms(
    indices: &[Arc<InvertedIndex>],
    query_tokens: &Tokens,
    params: &FtsSearchParams,
) -> Result<Vec<String>> {
    let mut terms = Vec::new();
    let mut seen = HashSet::new();

    if !matches!(params.fuzziness, Some(n) if n != 0) {
        for token in query_tokens {
            if seen.insert(token.to_string()) {
                terms.push(token.to_string());
            }
        }
        return Ok(terms);
    }

    for index in indices {
        let expanded = index.expand_fuzzy_tokens(query_tokens, params)?;
        for idx in 0..expanded.len() {
            let token = expanded.get_token(idx);
            if seen.insert(token.to_string()) {
                terms.push(token.to_string());
            }
        }
    }
    Ok(terms)
}

/// Build a shared [`MemBM25Scorer`] across a set of FTS index segments.
///
/// Aggregates each segment's `(total_tokens, num_docs, per_term_doc_freq)`
/// statistics — obtained via [`InvertedIndex::bm25_stats_for_terms`] — into a
/// single corpus-wide scorer, so that BM25 IDF scoring uses *global*
/// statistics rather than per-segment statistics. Computes the union of
/// fuzzy-expanded terms when `params.fuzziness` is set.
///
/// `metrics`, when provided, is forwarded to the per-token metadata cache
/// boundary on each segment so callers running under an `ExecutionPlan`
/// (e.g. `MatchQueryExec`) see the reads triggered here in their per-query
/// `index_cache_hits`/`index_cache_misses` counters.
///
/// Public as the canonical producer paired with the `with_base_scorer`
/// consumer on FTS exec types: callers holding `Arc<InvertedIndex>` segment
/// handles locally can construct an injectable scorer without reimplementing
/// per-segment stat aggregation, term deduplication, and fuzzy-expansion
/// union. Keeps a single source of truth for BM25 IDF arithmetic across
/// segments.
pub async fn build_global_bm25_scorer(
    indices: &[Arc<InvertedIndex>],
    query_tokens: &Tokens,
    params: &FtsSearchParams,
    metrics: Option<&dyn crate::scalar::MetricsCollector>,
) -> Result<MemBM25Scorer> {
    let terms = scorer_terms(indices, query_tokens, params)?;
    let first_index = indices.first().ok_or_else(|| {
        lance_core::Error::invalid_input("FTS index requires at least one segment")
    })?;
    let (mut total_tokens, mut num_docs, first_token_docs) =
        first_index.bm25_stats_for_terms(&terms, metrics).await?;
    let mut token_docs = HashMap::with_capacity(terms.len());
    for (term, count) in terms.iter().cloned().zip(first_token_docs) {
        token_docs.insert(term, count);
    }

    for index in indices.iter().skip(1) {
        let (segment_total_tokens, segment_num_docs, segment_token_docs) =
            index.bm25_stats_for_terms(&terms, metrics).await?;
        total_tokens += segment_total_tokens;
        num_docs += segment_num_docs;
        for (term, count) in terms.iter().zip(segment_token_docs) {
            *token_docs
                .get_mut(term)
                .expect("global scorer terms should already be initialized") += count;
        }
    }

    Ok(MemBM25Scorer::new(total_tokens, num_docs, token_docs))
}

use lance_core::Error;

use crate::pbold;
use crate::progress::IndexBuildProgress;
use crate::scalar::{
    CreatedIndex, RowIdRemapper, ScalarIndex,
    expression::{FtsQueryParser, ScalarQueryParser},
    registry::{
        BasicTrainer, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, TrainingRequest,
    },
};

use super::IndexStore;

#[derive(Debug, Default)]
pub struct InvertedIndexPlugin;

impl InvertedIndexPlugin {
    pub async fn train_inverted_index(
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        params: InvertedIndexParams,
        fragment_ids: Option<Vec<u32>>,
        progress: Arc<dyn IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let fragment_mask = fragment_ids.as_ref().and_then(|frag_ids| {
            if !frag_ids.is_empty() {
                // Create a mask with fragment_id in high 32 bits for distributed indexing
                // This mask is used to filter partitions belonging to specific fragments
                // If multiple fragments processed, use first fragment_id <<32 as mask
                Some((frag_ids[0] as u64) << 32)
            } else {
                None
            }
        });

        params.validate_format_version()?;
        let format_version = params.resolved_format_version();
        let is_element_document = params.get_document_granularity().is_list_element();
        let details = pbold::InvertedIndexDetails::try_from(&params)?;
        let mut inverted_index =
            InvertedIndexBuilder::new_with_fragment_mask(params, fragment_mask)
                .with_progress(progress);
        let files = inverted_index.update(data, index_store, None).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: if is_element_document {
                INVERTED_INDEX_VERSION_V3
            } else {
                format_version.index_version()
            },
            files,
        })
    }

    /// Return true if the query can be used to speed up contains_tokens queries
    fn can_accelerate_queries(details: &pbold::InvertedIndexDetails) -> bool {
        details.base_tokenizer == Some("simple".to_string())
            && details.max_token_length.is_none()
            && details.language == serde_json::to_string(&Language::English).unwrap()
            && !details.stem
    }
}

struct InvertedIndexTrainingRequest {
    parameters: InvertedIndexParams,
    criteria: TrainingCriteria,
}

impl InvertedIndexTrainingRequest {
    pub fn new(parameters: InvertedIndexParams) -> Self {
        Self {
            parameters,
            criteria: TrainingCriteria::new(TrainingOrdering::None).with_row_id(),
        }
    }
}

impl TrainingRequest for InvertedIndexTrainingRequest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

#[async_trait]
impl BasicTrainer for InvertedIndexPlugin {
    fn new_training_request(
        &self,
        params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::LargeBinary => (),
            DataType::List(f) if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8) => (),
            DataType::LargeList(f) if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8) => (),

            _ => return Err(Error::invalid_input_source(format!(
                "A inverted index can only be created on a Utf8 or LargeUtf8 field/list or LargeBinary field. Column has type {:?}",
                field.data_type()
            )
                .into()))
        }

        let params = InvertedIndexParams::from_training_json(params)?;
        Ok(Box::new(InvertedIndexTrainingRequest::new(params)))
    }

    /// Train a new index
    ///
    /// The provided data must fulfill all the criteria returned by `training_criteria`.
    /// It is the caller's responsibility to ensure this.
    ///
    /// Returns index details that describe the index.  These details can potentially be
    /// useful for planning (although this will currently require inside information on
    /// the index type) and they will need to be provided when loading the index.
    ///
    /// It is the caller's responsibility to store these details somewhere.
    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        fragment_ids: Option<Vec<u32>>,
        progress: Arc<dyn IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let request = (request as Box<dyn std::any::Any>)
            .downcast::<InvertedIndexTrainingRequest>()
            .map_err(|_| {
                Error::invalid_input_source(
                    "must provide training request created by new_training_request".into(),
                )
            })?;
        Self::train_inverted_index(
            data,
            index_store,
            request.parameters.clone(),
            fragment_ids,
            progress,
        )
        .await
    }
}

#[async_trait]
impl ScalarIndexPlugin for InvertedIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "Inverted"
    }

    fn provides_exact_answer(&self) -> bool {
        false
    }

    fn version(&self) -> u32 {
        INVERTED_INDEX_VERSION_V3
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        let Ok(index_details) = _index_details.to_msg::<pbold::InvertedIndexDetails>() else {
            return None;
        };

        if Self::can_accelerate_queries(&index_details) {
            Some(Box::new(FtsQueryParser::new(
                index_name,
                self.name().to_string(),
            )))
        } else {
            None
        }
    }

    /// Load an index from storage
    ///
    /// The index details should match the details that were returned when the index was
    /// originally trained.
    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        let index = InvertedIndex::load(index_store, frag_reuse_index, cache).await?;
        let details = index_details.to_msg::<pbold::InvertedIndexDetails>()?;
        let expected_granularity = DocumentGranularity::try_from(details.document_granularity)?;
        let physical_granularity = index.params().get_document_granularity();
        if physical_granularity != expected_granularity {
            return Err(Error::index(format!(
                "FTS document granularity in index details is {expected_granularity:?}, but the physical document schema implies {physical_granularity:?}"
            )));
        }
        Ok(index as Arc<dyn ScalarIndex>)
    }

    fn details_as_json(&self, details: &prost_types::Any) -> Result<serde_json::Value> {
        let index_details = details.to_msg::<pbold::InvertedIndexDetails>()?;
        let index_params = InvertedIndexParams::try_from(&index_details)?;
        Ok(index_params.to_details_json()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::{BuiltinIndexType, ScalarIndexParams};

    #[test]
    fn test_plugin_version_tracks_v3_capability_gate() {
        let plugin = InvertedIndexPlugin;
        assert_eq!(plugin.version(), INVERTED_INDEX_VERSION_V3);
    }

    #[test]
    fn test_details_json_includes_document_granularity() {
        let details = pbold::InvertedIndexDetails {
            document_granularity: pbold::inverted_index_details::DocumentGranularity::ListElement
                as i32,
            ..Default::default()
        };
        let details = prost_types::Any::from_msg(&details).unwrap();

        let json = InvertedIndexPlugin.details_as_json(&details).unwrap();

        assert_eq!(json["document_granularity"], "list_element");
    }

    #[test]
    fn test_new_training_request_defaults_missing_block_size_to_128() {
        let plugin = InvertedIndexPlugin;
        let field = Field::new("text", DataType::Utf8, true);

        let cases = [
            (
                ScalarIndexParams::for_builtin(BuiltinIndexType::Inverted),
                false,
            ),
            (ScalarIndexParams::new("inverted".to_string()), false),
            (
                ScalarIndexParams::new("inverted".to_string())
                    .with_params(&serde_json::json!({ "with_position": true })),
                true,
            ),
        ];

        for (params, expected_with_position) in cases {
            let request = plugin
                .new_training_request(params.params.as_deref().unwrap_or("{}"), &field)
                .unwrap();
            let request = request
                .as_any()
                .downcast_ref::<InvertedIndexTrainingRequest>()
                .unwrap();

            assert_eq!(request.parameters.posting_block_size(), DEFAULT_BLOCK_SIZE);
            assert_eq!(request.parameters.has_positions(), expected_with_position);
        }
    }
}
