// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::any::Any;
use std::collections::BTreeMap;
use std::iter::once;
use std::pin::Pin;
use std::time::Instant;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::lance_format::LanceIndexStore;
use super::{
    AnyQuery, BuiltinIndexType, IndexFile, IndexReader, IndexStore, IndexWriter, MetricsCollector,
    ScalarIndex, ScalarIndexParams, SearchResult, TextQuery,
};
use crate::frag_reuse::FragReuseIndex;
use crate::metrics::NoOpMetricsCollector;
use crate::pbold;
use crate::scalar::expression::{ScalarQueryParser, TextQueryParser};
use crate::scalar::registry::{
    BasicTrainer, DefaultTrainingRequest, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering,
    TrainingRequest, VALUE_COLUMN_NAME,
};
use crate::scalar::{CreatedIndex, RowIdRemapper, UpdateCriteria};
use crate::{Index, IndexType};
use arrow::array::{AsArray, UInt32Builder};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{UInt32Type, UInt64Type};
use arrow_array::{BinaryArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use lance_arrow::iter_str_array;
use lance_core::cache::{CacheKey, CacheKeySchema, KeyBuilder, LanceCache, WeakLanceCache};
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::LanceOptionExt;
use lance_core::utils::address::RowAddress;
use lance_core::utils::tempfile::TempDir;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::utils::tracing::{IO_TYPE_LOAD_SCALAR_PART, TRACE_IO_EVENTS};
use lance_core::{Error, ROW_ID, Result};
use lance_io::object_store::ObjectStore;
use lance_select::{RowAddrTreeMap, RowSetOps};
use lance_tokenizer::{
    AlphaNumOnlyFilter, AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, TextAnalyzer,
};
use log::info;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::Serialize;
use tracing::instrument;

mod ngram_regex;
pub(crate) use ngram_regex::regex_can_use_index;

const TOKENS_COL: &str = "tokens";
const POSTING_LIST_COL: &str = "posting_list";
pub const POSTINGS_FILENAME: &str = "ngram_postings.lance";
const NGRAM_INDEX_VERSION: u32 = 0;

/// An i32-offset Binary array can hold at most i32::MAX bytes of values in total,
/// so a spill state whose serialized posting lists exceed that must be written as
/// multiple record batches (same approach as the bitmap index).  Leave headroom.
const MAX_POSTING_LIST_BATCH_BYTES: usize = i32::MAX as usize - 1024 * 1024;
const POSTING_LIST_STREAM_BATCH_ROWS: usize = 64;

use std::sync::LazyLock;

pub static TOKENS_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::new(TOKENS_COL, DataType::UInt32, true));
pub static POSTINGS_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(POSTING_LIST_COL, DataType::Binary, false).with_metadata(HashMap::from([(
        lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
        "none".to_string(),
    )]))
});
pub static POSTINGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        TOKENS_FIELD.clone(),
        POSTINGS_FIELD.clone(),
    ]))
});
pub static TEXT_PREPPER: LazyLock<TextAnalyzer> = LazyLock::new(|| {
    TextAnalyzer::builder(RawTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build()
});
/// Currently we ALWAYS use trigrams with ascii folding and lower casing.  We may want to make this configurable in the future.
pub static NGRAM_TOKENIZER: LazyLock<TextAnalyzer> = LazyLock::new(|| {
    TextAnalyzer::builder(NgramTokenizer::all_ngrams(3, 3).unwrap())
        .filter(AlphaNumOnlyFilter)
        .build()
});

// Helper function to apply a function to each token in a text
fn tokenize_visitor(tokenizer: &TextAnalyzer, text: &str, mut visitor: impl FnMut(&String)) {
    // The token_stream method is mutable.  As far as I can tell this is to enforce exclusivity and not
    // true mutability.  For example, the object returned by `token_stream` has thread-local state but
    // it is reset each time `token_stream` is called.
    //
    // However, I don't see this documented anywhere and I'm not sure about relying on it.  For now, we
    // make a clone as that seems to be the safer option.  All the tokenizers we use here should be trivially
    // cloneable (although it requires a heap allocation so may be worth investigating in the future)
    let mut prepper = TEXT_PREPPER.clone();
    let mut tokenizer = tokenizer.clone();
    let mut raw_stream = prepper.token_stream(text);
    while raw_stream.advance() {
        let mut token_stream = tokenizer.token_stream(&raw_stream.token().text);
        while token_stream.advance() {
            visitor(&token_stream.token().text);
        }
    }
}

const ALPHA_SPAN: usize = 37;
const MAX_TOKEN: usize = ALPHA_SPAN.pow(2) + ALPHA_SPAN;
const MIN_TOKEN: usize = 0;
/// The width, in characters, of the tokens stored in an ngram index.
///
/// Trigrams are the standard choice for substring search: they are selective
/// enough to prune well while keeping the token space small enough
/// (`ALPHA_SPAN^3`) to address with a `u32`.  This must match the tokenizer
/// configured in [`NGRAM_TOKENIZER`].
const NGRAM_N: usize = 3;

// Convert an ngram (string) to a token (u32).  This helps avoid heap allocations
// and it makes it easier to partition the tokens for shuffling
//
// There are 36 alphanumeric values and we add 1 for the NULL token giving us 37^3
// potential tokens.
//
// "" => 0
// "?" => 37^2 * ?
// "?$" => 37^2 * ? + 37 * $
// "?$#" => 37^2 * ? + 37 * $ + #
// ...
//
// The ?,$,# represent the position in the alphabet (+1 to distinguish from NULL)
//
// Small strings get the larger multipliers because those ngrams are
// less likely to be unique and will have larger bitmaps.  We want to
// spread those out.
//
// NOTE: Today we hard-code trigrams and we do not include 1-grams or 2-grams so this
// function is more general than it needs to be...just in case.
fn ngram_to_token(ngram: &str, ngram_length: usize) -> u32 {
    let mut token = 0;
    // Empty string will get 0
    for (idx, byte) in ngram.bytes().enumerate() {
        let pos = if byte <= b'9' {
            byte - b'0'
        } else if byte <= b'z' {
            byte - b'a' + 10
        } else {
            unreachable!()
        } + 1;
        debug_assert!(pos < ALPHA_SPAN as u8);
        let mult = ALPHA_SPAN.pow(ngram_length as u32 - idx as u32 - 1) as u32;
        token += pos as u32 * mult;
    }
    token
}

/// Basic stats about an ngram index
#[derive(Serialize)]
struct NGramStatistics {
    num_ngrams: usize,
}

/// The row ids that contain a given ngram
#[derive(Debug)]
pub struct NGramPostingList {
    bitmap: RoaringTreemap,
}

impl DeepSizeOf for NGramPostingList {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        self.bitmap.serialized_size()
    }
}

// Cache key implementation for type-safe cache access
#[derive(Debug, Clone)]
pub struct NGramPostingListKey {
    pub row_offset: u32,
}

impl CacheKey for NGramPostingListKey {
    type ValueType = NGramPostingList;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("posting-list-{}", self.row_offset).into()
    }

    fn type_name() -> &'static str {
        "NGramPostingList"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.ngram-posting-list-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.row_offset);
    }
}

impl NGramPostingList {
    fn try_from_batch(
        batch: RecordBatch,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        let bitmap_bytes = batch.column(0).as_binary::<i32>().value(0);
        let mut bitmap = RoaringTreemap::deserialize_from(bitmap_bytes)
            .map_err(|e| Error::internal(format!("Error deserializing ngram list: {}", e)))?;
        if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
            bitmap = frag_reuse_index_ref.remap_row_ids_roaring_tree_map(&bitmap);
        }
        Ok(Self { bitmap })
    }

    fn intersect<'a>(lists: impl IntoIterator<Item = &'a Self>) -> RoaringTreemap {
        let mut iter = lists.into_iter();
        let mut result = iter
            .next()
            .map(|list| list.bitmap.clone())
            .unwrap_or_default();
        for list in iter {
            result &= &list.bitmap;
        }
        result
    }
}

/// Reads on-demand ngram posting lists from storage (and stores them in a cache)
struct NGramPostingListReader {
    reader: Arc<dyn IndexReader>,
    frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    index_cache: WeakLanceCache,
}

impl DeepSizeOf for NGramPostingListReader {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        0
    }
}

impl std::fmt::Debug for NGramPostingListReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NGramListReader").finish()
    }
}

impl NGramPostingListReader {
    #[instrument(level = "debug", skip(self, metrics))]
    pub async fn ngram_list(
        &self,
        row_offset: u32,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<NGramPostingList>> {
        let result = self.index_cache.get_or_insert_with_key_hit(NGramPostingListKey { row_offset }, || async move {
            metrics.record_part_load();
                tracing::info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="ngram", part_id=row_offset);
                let batch = self
                    .reader
                    .read_range(
                        row_offset as usize..row_offset as usize + 1,
                        Some(&[POSTING_LIST_COL]),
                    )
                    .await?;
                NGramPostingList::try_from_batch(batch, self.frag_reuse_index.clone())
        }).await;
        match &result {
            Ok((_, true)) => metrics.record_index_cache_hit(),
            _ => metrics.record_index_cache_miss(),
        }
        result.map(|(v, _)| v)
    }
}

/// An ngram index
///
/// At a high level this is an inverted index that maps ngrams (small fixed size substrings) to the
/// row ids that contain them.
///
/// As a simple example consider a 1-gram index.  It would basically be a mapping from
/// each letter to the row ids that contain that letter.  Then, if the user searches for
/// "cat", the index would look up the row ids for "c", "a", and "t", and return the intersection
/// of those row ids because only rows have at least one c, a, and t could possible contain "cat".
///
/// This is an in-exact index, similar to a bloom filter.  It can return false positives and a
/// recheck step is needed to confirm the results.
///
/// Note that it cannot return false negatives.
pub struct NGramIndex {
    /// The mapping from tokens to row offsets
    tokens: HashMap<u32, u32>,
    /// The reader for the posting lists
    list_reader: Arc<NGramPostingListReader>,
    /// The tokenizer used to tokenize text.  Note: not all tokenizers can be used with this index.  For
    /// example, a stemming tokenizer would not work well because "dozing" would stem to "doze" and if the
    /// search term is "zing" it would not match.  As a result, this tokenizer is not as configurable as the
    /// tokenizers used in an inverted index.
    tokenizer: TextAnalyzer,
    io_parallelism: usize,
    /// The store that owns the index
    store: Arc<dyn IndexStore>,
}

impl std::fmt::Debug for NGramIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NGramIndex")
            .field("tokens", &self.tokens)
            .field("list_reader", &self.list_reader)
            .finish()
    }
}

impl DeepSizeOf for NGramIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.tokens.deep_size_of_children(context)
    }
}

impl NGramIndex {
    async fn from_store(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Self> {
        let tokens = store.open_index_file(POSTINGS_FILENAME).await?;
        let tokens = tokens
            .read_range(0..tokens.num_rows(), Some(&[TOKENS_COL]))
            .await?;

        let tokens_map = HashMap::from_iter(
            tokens
                .column(0)
                .as_primitive::<UInt32Type>()
                .values()
                .iter()
                .copied()
                .enumerate()
                .map(|(idx, token)| (token, idx as u32)),
        );

        let posting_reader = Arc::new(NGramPostingListReader {
            reader: store.open_index_file(POSTINGS_FILENAME).await?,
            frag_reuse_index,
            index_cache: WeakLanceCache::from(index_cache),
        });

        Ok(Self {
            io_parallelism: store.io_parallelism(),
            tokens: tokens_map,
            list_reader: posting_reader,
            tokenizer: NGRAM_TOKENIZER.clone(),
            store,
        })
    }

    fn remap_state(
        &self,
        state: NGramIndexSpillState,
        mapping: &RowAddrRemap,
    ) -> Result<Vec<RecordBatch>> {
        let bitmaps = state
            .bitmaps
            .into_iter()
            .map(|posting_list| {
                RoaringTreemap::from_iter(posting_list.into_iter().filter_map(|row_id| {
                    match mapping.get(row_id) {
                        Some(Some(new_row_id)) => Some(new_row_id),
                        Some(None) => None,
                        None => Some(row_id),
                    }
                }))
            })
            .collect();

        NGramIndexSpillState {
            tokens: state.tokens,
            bitmaps,
        }
        .try_into_batches()
    }

    async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>>
    where
        Self: Sized,
    {
        Ok(Arc::new(
            Self::from_store(store, frag_reuse_index, index_cache).await?,
        ))
    }

    /// Merge several built NGram segments (and optional new data) into a single
    /// canonical segment in `dest_store`, unioning their posting lists by token
    /// without rescanning the dataset.
    pub async fn merge_segments(
        segment_stores: &[Arc<dyn IndexStore>],
        new_data: Option<SendableRecordBatchStream>,
        dest_store: &dyn IndexStore,
        old_data_filters: &[Option<super::OldIndexDataFilter>],
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<CreatedIndex> {
        let mut builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default())?;
        // Pure consolidation has no new rows, so skip `train` (and its
        // worker-per-partition tokenization pipeline) entirely.
        let new_data_spills = match new_data {
            Some(new_data) => builder.train(new_data).await?,
            None => Vec::new(),
        };
        let file = builder
            .merge_indices(
                new_data_spills,
                segment_stores,
                old_data_filters,
                frag_reuse_index,
                dest_store,
            )
            .await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::NGramIndexDetails::default())?,
            index_version: NGRAM_INDEX_VERSION,
            files: vec![file],
        })
    }
}

#[async_trait]
impl Index for NGramIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let ngram_stats = NGramStatistics {
            num_ngrams: self.tokens.len(),
        };
        serde_json::to_value(ngram_stats)
            .map_err(|e| Error::internal(format!("Error serializing statistics: {}", e)))
    }

    async fn prewarm(&self) -> Result<()> {
        // TODO: NGram index can pre-warm by loading all posting lists into memory
        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::NGram
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frag_ids = RoaringBitmap::new();
        for row_offset in self.tokens.values() {
            let list = self
                .list_reader
                .ngram_list(*row_offset, &NoOpMetricsCollector)
                .await?;
            frag_ids.extend(
                list.bitmap
                    .iter()
                    .map(|row_addr| RowAddress::from(row_addr).fragment_id()),
            );
        }
        Ok(frag_ids)
    }
}

#[async_trait]
impl ScalarIndex for NGramIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query
            .as_any()
            .downcast_ref::<TextQuery>()
            .ok_or_else(|| Error::invalid_input_source("Query is not a TextQuery".into()))?;
        match query {
            TextQuery::StringContains(substr) => {
                // Count characters, not bytes: the tokenizer splits on
                // characters, so a two character multi-byte needle is just as
                // untokenizable as a two byte one.
                if substr.chars().count() < NGRAM_N {
                    // We know nothing on short searches, need to recheck all
                    return Ok(SearchResult::at_least(RowAddrTreeMap::new()));
                }

                let mut row_offsets = Vec::with_capacity(substr.len() * 3);
                let mut missing = false;
                tokenize_visitor(&self.tokenizer, substr, |ngram| {
                    let token = ngram_to_token(ngram, NGRAM_N);
                    if let Some(row_offset) = self.tokens.get(&token) {
                        row_offsets.push(*row_offset);
                    } else {
                        missing = true;
                    }
                });
                // At least one token was missing, so we know there are zero results
                if missing {
                    return Ok(SearchResult::exact(RowAddrTreeMap::new()));
                }
                // The needle was long enough but still yielded no tokens (e.g. the
                // tokenizer's filters dropped every character).  As with a short
                // needle, the index knows nothing and every row must be rechecked.
                if row_offsets.is_empty() {
                    return Ok(SearchResult::at_least(RowAddrTreeMap::new()));
                }
                let posting_lists = futures::stream::iter(
                    row_offsets
                        .into_iter()
                        .map(|row_offset| self.list_reader.ngram_list(row_offset, metrics)),
                )
                .buffer_unordered(self.io_parallelism)
                .try_collect::<Vec<_>>()
                .await?;
                metrics.record_comparisons(posting_lists.len());
                let list_refs = posting_lists.iter().map(|list| list.as_ref());
                let row_ids = NGramPostingList::intersect(list_refs);
                Ok(SearchResult::at_most(RowAddrTreeMap::from(row_ids)))
            }
            TextQuery::Regex(pattern) => {
                let trigram_query = ngram_regex::regex_to_trigram_query(pattern);
                match &trigram_query {
                    // No usable trigram structure (e.g. `a.b`, `.*`): the index
                    // cannot prune, so every row must be rechecked.
                    ngram_regex::TrigramQuery::All => {
                        Ok(SearchResult::at_least(RowAddrTreeMap::new()))
                    }
                    // The pattern is provably unsatisfiable.
                    ngram_regex::TrigramQuery::None => {
                        Ok(SearchResult::exact(RowAddrTreeMap::new()))
                    }
                    _ => {
                        let mut tokens = HashSet::new();
                        ngram_regex::collect_tokens(&trigram_query, &mut tokens);
                        // Fetch the posting list for every trigram the condition
                        // references; a token absent from the index contributes
                        // an empty list, which `eval_trigram_query` handles.
                        let present = tokens.into_iter().filter_map(|token| {
                            self.tokens.get(&token).map(|offset| (token, *offset))
                        });
                        let lists = futures::stream::iter(present.map(|(token, offset)| {
                            self.list_reader
                                .ngram_list(offset, metrics)
                                .map(move |result| result.map(|list| (token, list)))
                        }))
                        .buffer_unordered(self.io_parallelism)
                        .try_collect::<Vec<(u32, Arc<NGramPostingList>)>>()
                        .await?;
                        metrics.record_comparisons(lists.len());
                        let bitmaps: HashMap<u32, RoaringTreemap> = lists
                            .into_iter()
                            .map(|(token, list)| (token, list.bitmap.clone()))
                            .collect();
                        let row_ids = ngram_regex::eval_trigram_query(&trigram_query, &bitmaps);
                        Ok(SearchResult::at_most(RowAddrTreeMap::from(row_ids)))
                    }
                }
            }
        }
    }

    fn can_remap(&self) -> bool {
        true
    }

    async fn remap(
        &self,
        mapping: &RowAddrRemap,
        dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        let reader = self.store.open_index_file(POSTINGS_FILENAME).await?;
        let mut writer = dest_store
            .new_index_file(POSTINGS_FILENAME, POSTINGS_SCHEMA.clone())
            .await?;

        let mut spill_stream =
            NGramIndexBuilder::stream_spill_reader(reader, MAX_POSTING_LIST_BATCH_BYTES)?;
        while let Some(state) = spill_stream.try_next().await? {
            for batch in self.remap_state(state, mapping)? {
                writer.write_record_batch(batch).await?;
            }
        }

        let file = writer.finish().await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::NGramIndexDetails::default())?,
            index_version: NGRAM_INDEX_VERSION,
            files: vec![file],
        })
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        _old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let mut builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default())?;
        let spill_files = builder.train(new_data).await?;

        let file = builder
            .write_index(dest_store, spill_files, Some(self.store.clone()))
            .await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::NGramIndexDetails::default())?,
            index_version: NGRAM_INDEX_VERSION,
            files: vec![file],
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(TrainingCriteria::new(TrainingOrdering::None).with_row_id())
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::NGram))
    }
}

#[derive(Debug, Clone)]
pub struct NGramIndexBuilderOptions {
    tokens_per_spill: usize,
}

// A higher value will use more RAM.  A lower value will have to do more spilling
static DEFAULT_TOKENS_PER_SPILL: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_NGRAM_TOKENS_PER_SPILL")
        .unwrap_or_else(|_| "1000000000".to_string())
        .parse()
        .expect("failed to parse LANCE_NGRAM_TOKENS_PER_SPILL")
});
// How many partitions to use for shuffling out the work.  We slightly
// over-allocate this since the amount of work per-partition is not uniform.
//
// Increasing this may increase the performance but it could increase RAM (since we will spill less often)
// and could hurt performance (since there will be more files at the end for the final spill)
static DEFAULT_NUM_PARTITIONS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_NGRAM_NUM_PARTITIONS")
        .map(|s| s.parse().expect("failed to parse LANCE_NGRAM_PARALLELISM"))
        .unwrap_or((get_num_compute_intensive_cpus() * 4).max(128))
});
// Just enough so that tokenizing is faster than I/O
static DEFAULT_TOKENIZE_PARALLELISM: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_NGRAM_TOKENIZE_PARALLELISM")
        .map(|s| {
            s.parse()
                .expect("failed to parse LANCE_NGRAM_TOKENIZE_PARALLELISM")
        })
        .unwrap_or(8)
});

impl Default for NGramIndexBuilderOptions {
    fn default() -> Self {
        Self {
            tokens_per_spill: *DEFAULT_TOKENS_PER_SPILL,
        }
    }
}

// An ordered list of tokens and bitmaps
//
// The `tokens` list is ordered by token value.  This makes it easier to merge spill files.
struct NGramIndexSpillState {
    tokens: UInt32Array,
    bitmaps: Vec<RoaringTreemap>,
}

struct NGramIndexSpillStateBuilder {
    tokens: UInt32Builder,
    bitmaps: Vec<RoaringTreemap>,
    serialized_bytes: usize,
}

impl NGramIndexSpillStateBuilder {
    fn new() -> Self {
        Self {
            tokens: UInt32Builder::with_capacity(0),
            bitmaps: Vec::new(),
            serialized_bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.bitmaps.is_empty()
    }

    fn len(&self) -> usize {
        self.bitmaps.len()
    }

    fn push(
        &mut self,
        token: u32,
        bitmap: RoaringTreemap,
        max_batch_bytes: usize,
    ) -> Result<Option<NGramIndexSpillState>> {
        let posting_size = bitmap.serialized_size();
        if posting_size > max_batch_bytes {
            return Err(Error::invalid_input(format!(
                "posting list for ngram token {} serializes to {} bytes, which exceeds the {} bytes that fit in a single binary array",
                token, posting_size, max_batch_bytes,
            )));
        }

        let new_size = self
            .serialized_bytes
            .checked_add(posting_size)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "posting list byte size overflowed while adding ngram token {}",
                    token
                ))
            })?;
        let full_state = if !self.is_empty() && new_size > max_batch_bytes {
            Some(self.finish())
        } else {
            None
        };

        self.tokens.append_value(token);
        self.bitmaps.push(bitmap);
        self.serialized_bytes = posting_size
            .checked_add(if full_state.is_some() {
                0
            } else {
                self.serialized_bytes
            })
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "posting list byte size overflowed while adding ngram token {}",
                    token
                ))
            })?;

        Ok(full_state)
    }

    fn finish(&mut self) -> NGramIndexSpillState {
        self.serialized_bytes = 0;
        NGramIndexSpillState {
            tokens: std::mem::replace(&mut self.tokens, UInt32Builder::with_capacity(0)).finish(),
            bitmaps: std::mem::take(&mut self.bitmaps),
        }
    }
}

impl NGramIndexSpillState {
    fn try_from_batch(batch: RecordBatch) -> Result<Self> {
        let tokens = batch
            .column_by_name(TOKENS_COL)
            .expect_ok()?
            .as_primitive::<UInt32Type>()
            .clone();
        let postings = batch
            .column_by_name(POSTING_LIST_COL)
            .expect_ok()?
            .as_binary::<i32>();

        let bitmaps = postings
            .into_iter()
            .map(|bytes| {
                RoaringTreemap::deserialize_from(bytes.expect_ok()?)
                    .map_err(|e| Error::internal(format!("Error deserializing ngram list: {}", e)))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { tokens, bitmaps })
    }

    fn try_into_batches(self) -> Result<Vec<RecordBatch>> {
        self.try_into_batches_impl(MAX_POSTING_LIST_BATCH_BYTES)
    }

    // Split into multiple batches so that the cumulative serialized posting bytes
    // of each batch stay under `max_batch_bytes`, avoiding i32 offset overflow in
    // the Binary posting array.  Postings are serialized straight into each batch's
    // values buffer to avoid a second contiguous copy of multi-GiB payloads.
    fn try_into_batches_impl(self, max_batch_bytes: usize) -> Result<Vec<RecordBatch>> {
        debug_assert_eq!(self.tokens.len(), self.bitmaps.len());
        debug_assert!(max_batch_bytes <= i32::MAX as usize);
        let make_batch =
            |tokens: UInt32Array, values: Vec<u8>, offsets: Vec<i32>| -> Result<RecordBatch> {
                let posting_array = BinaryArray::new(
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    Buffer::from_vec(values),
                    None,
                );
                Ok(RecordBatch::try_new(
                    POSTINGS_SCHEMA.clone(),
                    vec![Arc::new(tokens), Arc::new(posting_array)],
                )?)
            };

        let mut batches = Vec::new();
        let mut values: Vec<u8> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut batch_start = 0;
        for (idx, bitmap) in self.bitmaps.into_iter().enumerate() {
            let posting_size = bitmap.serialized_size();
            if posting_size > max_batch_bytes {
                return Err(Error::invalid_input(format!(
                    "posting list for ngram token {} serializes to {} bytes, which exceeds the {} bytes that fit in a single binary array",
                    self.tokens.value(idx),
                    posting_size,
                    max_batch_bytes,
                )));
            }
            if values.len() + posting_size > max_batch_bytes {
                batches.push(make_batch(
                    self.tokens.slice(batch_start, idx - batch_start),
                    std::mem::take(&mut values),
                    std::mem::replace(&mut offsets, vec![0]),
                )?);
                batch_start = idx;
            }
            bitmap.serialize_into(&mut values)?;
            offsets.push(values.len() as i32);
        }
        if offsets.len() > 1 || batches.is_empty() {
            batches.push(make_batch(
                self.tokens.slice(batch_start, offsets.len() - 1),
                values,
                offsets,
            )?);
        }
        Ok(batches)
    }

    fn remap_and_filter_rows(
        self,
        frag_reuse_index: Option<&Arc<FragReuseIndex>>,
        filter: Option<&super::OldIndexDataFilter>,
    ) -> Self {
        if let Some(fri) = frag_reuse_index {
            return self.remap_then_keep(fri, filter);
        }
        match filter {
            None => self,
            Some(filter) => self.retain_rows(filter),
        }
    }

    /// Remap stale addresses per row, keeping only rows still selected by
    /// `filter`. Used only under a pending deferred-remap compaction.
    fn remap_then_keep(
        self,
        fri: &Arc<FragReuseIndex>,
        filter: Option<&super::OldIndexDataFilter>,
    ) -> Self {
        let mut tokens = UInt32Builder::with_capacity(self.tokens.len());
        let mut bitmaps = Vec::with_capacity(self.bitmaps.len());
        for (token, bitmap) in self.tokens.values().iter().zip(self.bitmaps) {
            let remapped = fri.remap_row_ids_roaring_tree_map(&bitmap);
            let kept = match filter {
                Some(filter) => RoaringTreemap::from_iter(
                    remapped
                        .into_iter()
                        .filter(|row_id| old_filter_keeps(filter, *row_id)),
                ),
                None => remapped,
            };
            if !kept.is_empty() {
                tokens.append_value(*token);
                bitmaps.push(kept);
            }
        }
        Self {
            tokens: tokens.finish(),
            bitmaps,
        }
    }

    fn retain_rows(self, filter: &super::OldIndexDataFilter) -> Self {
        if let super::OldIndexDataFilter::Fragments { to_keep, .. } = filter
            && self.bitmaps.iter().all(|bitmap| {
                bitmap
                    .bitmaps()
                    .all(|(fragment, _)| to_keep.contains(fragment))
            })
        {
            return self;
        }
        let mut tokens = UInt32Builder::with_capacity(self.tokens.len());
        let mut bitmaps = Vec::with_capacity(self.bitmaps.len());
        for (token, bitmap) in self.tokens.values().iter().zip(self.bitmaps) {
            let kept = RoaringTreemap::from_iter(
                bitmap
                    .into_iter()
                    .filter(|row_id| old_filter_keeps(filter, *row_id)),
            );
            if !kept.is_empty() {
                tokens.append_value(*token);
                bitmaps.push(kept);
            }
        }
        Self {
            tokens: tokens.finish(),
            bitmaps,
        }
    }
}

fn old_filter_keeps(filter: &super::OldIndexDataFilter, row_id: u64) -> bool {
    match filter {
        super::OldIndexDataFilter::Fragments { to_keep, .. } => {
            to_keep.contains((row_id >> 32) as u32)
        }
        super::OldIndexDataFilter::RowIds(valid) => valid.contains(row_id),
    }
}

// As we're building we create a map from ngram to row ids.  When this map gets too large
// we spill it to disk.
struct NGramIndexBuildState {
    tokens_map: BTreeMap<u32, RoaringTreemap>,
}

impl NGramIndexBuildState {
    fn starting() -> Self {
        Self {
            tokens_map: BTreeMap::new(),
        }
    }

    fn take(&mut self) -> Self {
        let mut taken = Self::starting();
        std::mem::swap(&mut self.tokens_map, &mut taken.tokens_map);
        taken
    }

    fn into_spill(self) -> NGramIndexSpillState {
        // We can rely on these being in token order because of BTreeMap
        let tokens = UInt32Array::from_iter_values(self.tokens_map.keys().copied());
        let bitmaps = Vec::from_iter(self.tokens_map.into_values());

        NGramIndexSpillState { bitmaps, tokens }
    }
}

/// A builder for an ngram index
///
/// The builder is a small pipeline.  First, we read in the data and tokenize it.  This
/// stage uses fan-out parallelism to tokenize the data because tokenization may be a little
/// slower than I/O.
///
/// The second stage fans out much wider.  It partitions the tokens into a number of partitions.
/// Each partition has a BTreemap that maps tokens to row ids.  The partitions then build up
/// roaring treemaps.  When a partition gets too full it will spill to disk.
///
/// Once all the data is processed we spill all the parititons to disk and then we merge the
/// spill files into a single index file.
pub struct NGramIndexBuilder {
    tokenizer: TextAnalyzer,
    options: NGramIndexBuilderOptions,
    tmpdir: Arc<TempDir>,
    spill_store: Arc<dyn IndexStore>,

    tokens_seen: usize,
    worker_number: usize,
    has_flushed: bool,

    state: NGramIndexBuildState,
}

impl NGramIndexBuilder {
    pub fn try_new(options: NGramIndexBuilderOptions) -> Result<Self> {
        Self::from_state(NGramIndexBuildState::starting(), options)
    }

    fn clone_worker(&self, worker_number: usize) -> Self {
        let mut bitmaps = Vec::with_capacity(36 * 36 * 36 + 1);
        // Token 0 is always the NULL bitmap
        bitmaps.push(RoaringTreemap::new());
        Self {
            tokenizer: self.tokenizer.clone(),
            state: NGramIndexBuildState::starting(),
            tmpdir: self.tmpdir.clone(),
            spill_store: self.spill_store.clone(),
            options: self.options.clone(),
            tokens_seen: 0,
            worker_number,
            has_flushed: false,
        }
    }

    fn from_state(state: NGramIndexBuildState, options: NGramIndexBuilderOptions) -> Result<Self> {
        let tokenizer = NGRAM_TOKENIZER.clone();

        let tmpdir = Arc::new(TempDir::default());
        let spill_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        Ok(Self {
            tokenizer,
            state,
            tmpdir,
            spill_store,
            options,
            tokens_seen: 0,
            worker_number: 0,
            has_flushed: false,
        })
    }

    fn validate_schema(schema: &Schema) -> Result<()> {
        if schema.fields().len() != 2 {
            return Err(Error::invalid_input_source(
                "Ngram index schema must have exactly two fields".into(),
            ));
        }
        let values_field = schema.field_with_name(VALUE_COLUMN_NAME)?;
        if *values_field.data_type() != DataType::Utf8
            && *values_field.data_type() != DataType::LargeUtf8
        {
            return Err(Error::invalid_input_source(
                "First field in ngram index schema must be of type Utf8/LargeUtf8".into(),
            ));
        }
        let row_id_field = schema.field_with_name(ROW_ID)?;
        if *row_id_field.data_type() != DataType::UInt64 {
            return Err(Error::invalid_input_source(
                "Second field in ngram index schema must be of type UInt64".into(),
            ));
        }
        Ok(())
    }

    async fn process_batch(&mut self, tokens_and_ids: Vec<(u32, u64)>) -> Result<()> {
        let mut tokens_seen = 0;
        for (token, row_id) in tokens_and_ids {
            tokens_seen += 1;
            // This would be a bit simpler with entry API but, at scale, the vast majority
            // of cases will be a hit and we want to avoid cloning the string if we can.  So
            // for now we do the double-hash.  We can simplify in the future with raw_entry
            // when it stabilizes.
            self.state
                .tokens_map
                .entry(token)
                .or_default()
                .insert(row_id);
        }
        self.tokens_seen += tokens_seen;
        if self.tokens_seen >= self.options.tokens_per_spill {
            let state = self.state.take();
            self.flush(state).await?;
        }
        Ok(())
    }

    fn spill_filename(id: usize) -> String {
        format!("spill-{}.lance", id)
    }

    fn tmp_spill_filename(id: usize) -> String {
        format!("spill-{}.lance.tmp", id)
    }

    async fn flush(&mut self, state: NGramIndexBuildState) -> Result<bool> {
        if self.tokens_seen == 0 {
            assert!(state.tokens_map.is_empty());
            return Ok(self.has_flushed);
        }
        self.tokens_seen = 0;
        let spill_state = state.into_spill();
        let flush_start = Instant::now();
        // The primary builder should never flush
        debug_assert_ne!(self.worker_number, 0);
        if self.has_flushed {
            info!("Merging flush for worker {}", self.worker_number);
            // If we have flushed before then we need to merge with the spill file
            let mut writer = self
                .spill_store
                .new_index_file(
                    &Self::tmp_spill_filename(self.worker_number),
                    POSTINGS_SCHEMA.clone(),
                )
                .await?;

            let left_stream = stream::once(std::future::ready(Ok(spill_state)));
            let right_stream =
                Self::stream_spill(self.spill_store.clone(), self.worker_number).await?;
            Self::merge_spill_streams(left_stream, right_stream, writer.as_mut()).await?;
            drop(writer);
            self.spill_store
                .rename_index_file(
                    &Self::tmp_spill_filename(self.worker_number),
                    &Self::spill_filename(self.worker_number),
                )
                .await?;
        } else {
            // If we haven't flushed before we can just write to the spill file
            info!("Initial flush for worker {}", self.worker_number);
            self.has_flushed = true;
            let writer = self
                .spill_store
                .new_index_file(
                    &Self::spill_filename(self.worker_number),
                    POSTINGS_SCHEMA.clone(),
                )
                .await?;
            self.write(writer, spill_state).await?;
        }
        let flush_time = flush_start.elapsed();
        info!(
            "Flushed worker {} in {}ms",
            self.worker_number,
            flush_time.as_millis()
        );
        Ok(true)
    }

    fn tokenize_and_partition(
        tokenizer: &TextAnalyzer,
        batch: RecordBatch,
        num_workers: usize,
    ) -> Result<Vec<Vec<(u32, u64)>>> {
        let text_iter = iter_str_array(batch.column_by_name(VALUE_COLUMN_NAME).expect_ok()?);
        let row_id_col = batch
            .column_by_name(ROW_ID)
            .expect_ok()?
            .as_primitive::<UInt64Type>();
        // Guessing 1000 tokens per row to at least avoid some of the earlier allocations
        let mut partitions = vec![Vec::with_capacity(batch.num_rows() * 1000); num_workers];
        let divisor = (MAX_TOKEN - MIN_TOKEN) / num_workers;
        for (text, row_id) in text_iter.zip(row_id_col.values()) {
            if let Some(text) = text {
                tokenize_visitor(tokenizer, text, |token| {
                    let token = ngram_to_token(token, NGRAM_N);
                    let partition_id = (token as usize).saturating_sub(MIN_TOKEN) / divisor;
                    partitions[partition_id % num_workers].push((token, *row_id));
                });
            } else {
                partitions[0].push((0, *row_id));
            }
        }
        Ok(partitions)
    }

    pub async fn train(&mut self, data: SendableRecordBatchStream) -> Result<Vec<usize>> {
        let schema = data.schema();
        Self::validate_schema(schema.as_ref())?;

        let num_workers = *DEFAULT_NUM_PARTITIONS;
        let mut senders = Vec::with_capacity(num_workers);
        let mut builders = Vec::with_capacity(num_workers);
        for worker_idx in 0..num_workers {
            let (send, mut recv) = tokio::sync::mpsc::channel(2);
            senders.push(send);

            let mut builder = self.clone_worker(worker_idx + 1);
            let future = tokio::spawn(async move {
                while let Some(partition) = recv.recv().await {
                    builder.process_batch(partition).await?;
                }
                Result::Ok(builder)
            });
            builders.push(future);
        }

        let mut partitions_stream = data
            .and_then(|batch| {
                let tokenizer = self.tokenizer.clone();
                std::future::ready(Ok(tokio::task::spawn(async move {
                    Ok(Self::tokenize_and_partition(
                        &tokenizer,
                        batch,
                        num_workers,
                    )?)
                })
                .map(|res| res.unwrap())))
            })
            .try_buffer_unordered(*DEFAULT_TOKENIZE_PARALLELISM);

        while let Some(partitions) = partitions_stream.try_next().await? {
            for (part_idx, partition) in partitions.into_iter().enumerate() {
                senders[part_idx].send(partition).await.unwrap();
            }
        }

        std::mem::drop(senders);
        let builders = futures::future::try_join_all(builders).await?;

        // Final flush is serialized.  If we kick this off in parallel it can
        // use a lot of memory.

        let mut to_spill = Vec::with_capacity(builders.len());

        for builder in builders {
            let mut builder = builder?;
            let state = builder.state.take();
            if builder.flush(state).await? {
                to_spill.push(builder.worker_number);
            }
        }

        Ok(to_spill)
    }

    async fn write(
        &mut self,
        mut writer: Box<dyn IndexWriter>,
        state: NGramIndexSpillState,
    ) -> Result<()> {
        Self::write_state(writer.as_mut(), state).await?;
        writer.finish().await?;

        Ok(())
    }

    async fn write_state(writer: &mut dyn IndexWriter, state: NGramIndexSpillState) -> Result<()> {
        for batch in state.try_into_batches()? {
            writer.write_record_batch(batch).await?;
        }
        Ok(())
    }

    fn stream_spill_reader(
        reader: Arc<dyn IndexReader>,
        max_batch_bytes: usize,
    ) -> Result<impl Stream<Item = Result<NGramIndexSpillState>>> {
        let num_rows = reader.num_rows();

        Ok(stream::try_unfold(
            (0, NGramIndexSpillStateBuilder::new()),
            move |(mut offset, mut builder)| {
                let reader = reader.clone();
                async move {
                    loop {
                        if offset >= num_rows {
                            return if builder.is_empty() {
                                Ok(None)
                            } else {
                                let state = builder.finish();
                                Ok(Some((state, (offset, builder))))
                            };
                        }

                        let state = NGramIndexSpillState::try_from_batch(
                            reader.read_range(offset..offset + 1, None).await?,
                        )?;
                        offset += 1;
                        for (token, bitmap) in
                            state.tokens.values().iter().copied().zip(state.bitmaps)
                        {
                            if let Some(full) = builder.push(token, bitmap, max_batch_bytes)? {
                                return Ok(Some((full, (offset, builder))));
                            }
                            if builder.len() >= POSTING_LIST_STREAM_BATCH_ROWS {
                                let state = builder.finish();
                                return Ok(Some((state, (offset, builder))));
                            }
                        }
                    }
                }
                .boxed()
            },
        ))
    }

    async fn stream_spill(
        spill_store: Arc<dyn IndexStore>,
        id: usize,
    ) -> Result<impl Stream<Item = Result<NGramIndexSpillState>>> {
        let reader = spill_store
            .open_index_file(&Self::spill_filename(id))
            .await?;
        Self::stream_spill_reader(reader, MAX_POSTING_LIST_BATCH_BYTES)
    }

    fn merge_spill_states(
        left_opt: &mut Option<NGramIndexSpillState>,
        right_opt: &mut Option<NGramIndexSpillState>,
    ) -> NGramIndexSpillState {
        let left = left_opt.take().unwrap();
        let right = right_opt.take().unwrap();

        let item_capacity = left.tokens.len() + right.tokens.len();
        let mut merged_tokens = UInt32Builder::with_capacity(item_capacity);
        let mut merged_bitmaps = Vec::with_capacity(left.bitmaps.len() + right.bitmaps.len());

        let mut left_tokens = left.tokens.values().iter().copied();
        let mut left_bitmaps = left.bitmaps.into_iter();
        let mut right_tokens = right.tokens.values().iter().copied();
        let mut right_bitmaps = right.bitmaps.into_iter();

        let mut left_token = left_tokens.next();
        let mut left_bitmap = left_bitmaps.next();
        let mut right_token = right_tokens.next();
        let mut right_bitmap = right_bitmaps.next();

        while left_token.is_some() && right_token.is_some() {
            let left_token_val = left_token.unwrap();
            let right_token_val = right_token.unwrap();
            match left_token_val.cmp(&right_token_val) {
                std::cmp::Ordering::Less => {
                    merged_tokens.append_value(left_token_val);
                    merged_bitmaps.push(left_bitmap.unwrap());
                    left_token = left_tokens.next();
                    left_bitmap = left_bitmaps.next();
                }
                std::cmp::Ordering::Greater => {
                    merged_tokens.append_value(right_token_val);
                    merged_bitmaps.push(right_bitmap.unwrap());
                    right_token = right_tokens.next();
                    right_bitmap = right_bitmaps.next();
                }
                std::cmp::Ordering::Equal => {
                    merged_tokens.append_value(left_token_val);
                    merged_bitmaps.push(left_bitmap.unwrap() | &right_bitmap.unwrap());
                    left_token = left_tokens.next();
                    left_bitmap = left_bitmaps.next();
                    right_token = right_tokens.next();
                    right_bitmap = right_bitmaps.next();
                }
            }
        }

        let collect_remaining = |cur_token, tokens, cur_bitmap, bitmaps| {
            let tokens = UInt32Array::from_iter_values(once(cur_token).chain(tokens));
            let bitmaps = once(cur_bitmap).chain(bitmaps).collect::<Vec<_>>();
            NGramIndexSpillState { tokens, bitmaps }
        };

        if let Some(left_token) = left_token {
            *left_opt = Some(collect_remaining(
                left_token,
                left_tokens,
                left_bitmap.unwrap(),
                left_bitmaps,
            ));
        } else {
            *left_opt = None;
        }
        if let Some(right_token) = right_token {
            *right_opt = Some(collect_remaining(
                right_token,
                right_tokens,
                right_bitmap.unwrap(),
                right_bitmaps,
            ));
        } else {
            *right_opt = None;
        }

        NGramIndexSpillState {
            tokens: merged_tokens.finish(),
            bitmaps: merged_bitmaps,
        }
    }

    async fn merge_spill_streams(
        mut left_stream: impl Stream<Item = Result<NGramIndexSpillState>> + Unpin,
        mut right_stream: impl Stream<Item = Result<NGramIndexSpillState>> + Unpin,
        writer: &mut dyn IndexWriter,
    ) -> Result<IndexFile> {
        let mut left_state = left_stream.try_next().await?;
        let mut right_state = right_stream.try_next().await?;

        while left_state.is_some() || right_state.is_some() {
            if left_state.is_none() {
                // Left is done, full drain right
                let state = right_state.take().expect_ok()?;
                Self::write_state(writer, state).await?;
                while let Some(state) = right_stream.try_next().await? {
                    Self::write_state(writer, state).await?;
                }
            } else if right_state.is_none() {
                // Right is done, full drain left
                let state = left_state.take().expect_ok()?;
                Self::write_state(writer, state).await?;
                while let Some(state) = left_stream.try_next().await? {
                    Self::write_state(writer, state).await?;
                }
            } else {
                // There is a batch from both left and right.  Need to merge them
                let merged = Self::merge_spill_states(&mut left_state, &mut right_state);
                Self::write_state(writer, merged).await?;
                if left_state.is_none() {
                    left_state = left_stream.try_next().await?;
                }
                if right_state.is_none() {
                    right_state = right_stream.try_next().await?;
                }
            }
        }

        writer.finish().await
    }

    async fn merge_spill_files(
        spill_store: Arc<dyn IndexStore>,
        index_of_left: usize,
        index_of_right: usize,
        output_index: usize,
    ) -> Result<()> {
        // We fully load the small file into memory and then stream the large file
        info!(
            "Merge spill files {} and {} into {}",
            index_of_left, index_of_right, output_index
        );

        let mut writer = spill_store
            .new_index_file(&Self::spill_filename(output_index), POSTINGS_SCHEMA.clone())
            .await?;

        let (left_stream, right_stream) = futures::try_join!(
            Self::stream_spill(spill_store.clone(), index_of_left),
            Self::stream_spill(spill_store.clone(), index_of_right)
        )?;

        Self::merge_spill_streams(left_stream, right_stream, writer.as_mut()).await?;

        spill_store
            .delete_index_file(&Self::spill_filename(index_of_left))
            .await?;
        spill_store
            .delete_index_file(&Self::spill_filename(index_of_right))
            .await?;

        Ok(())
    }

    // Can potentially parallelize in the future if this step becomes a bottleneck
    //
    // We can also merge in a more balanced fashion (e.g. binary tree) to reduce the size of
    // intermediate files
    //
    // Note: worker indices start at 1 and not 0 (hence all the +1's)
    async fn merge_spills(&mut self, mut spill_files: Vec<usize>) -> Result<usize> {
        info!(
            "Merging {} index files into one combined index",
            spill_files.len()
        );

        let mut spill_counter = spill_files.iter().max().expect_ok()? + 1;
        while spill_files.len() > 1 {
            let mut new_spills = Vec::with_capacity(spill_files.len() / 2);
            while spill_files.len() >= 2 {
                let left = spill_files.pop().expect_ok()?;
                let right = spill_files.pop().expect_ok()?;
                new_spills.push(tokio::spawn(Self::merge_spill_files(
                    self.spill_store.clone(),
                    left,
                    right,
                    spill_counter + new_spills.len(),
                )));
            }
            for i in 0..new_spills.len() {
                spill_files.push(spill_counter + i);
            }
            spill_counter += new_spills.len();
            for result in futures::future::try_join_all(new_spills).await? {
                result?;
            }
        }

        spill_files.pop().expect_ok()
    }

    async fn merge_old_index(
        &mut self,
        new_data_num: usize,
        old_index: Arc<dyn IndexStore>,
    ) -> Result<usize> {
        info!("Merging old index into new index");
        let final_num = new_data_num + 1;

        let mut writer = self
            .spill_store
            .new_index_file(&Self::spill_filename(final_num), POSTINGS_SCHEMA.clone())
            .await?;

        let left_stream = Self::stream_spill(self.spill_store.clone(), new_data_num).await?;
        let old_reader = old_index.open_index_file(POSTINGS_FILENAME).await?;
        let right_stream = Self::stream_spill_reader(old_reader, MAX_POSTING_LIST_BATCH_BYTES)?;

        Self::merge_spill_streams(left_stream, right_stream, writer.as_mut()).await?;

        self.spill_store
            .delete_index_file(&Self::spill_filename(new_data_num))
            .await?;

        Ok(final_num)
    }

    pub async fn write_index(
        mut self,
        store: &dyn IndexStore,
        spill_files: Vec<usize>,
        old_index: Option<Arc<dyn IndexStore>>,
    ) -> Result<IndexFile> {
        let mut writer = store
            .new_index_file(POSTINGS_FILENAME, POSTINGS_SCHEMA.clone())
            .await?;

        if spill_files.is_empty() {
            if let Some(old_index) = old_index {
                // An update with no new data, just copy the old index to the new store
                return old_index.copy_index_file(POSTINGS_FILENAME, store).await;
            } else {
                // Training an index with no data, make an empty index
                let mut writer = store
                    .new_index_file(POSTINGS_FILENAME, POSTINGS_SCHEMA.clone())
                    .await?;
                return writer.finish().await;
            }
        }

        let mut index_to_copy = self.merge_spills(spill_files).await?;

        if let Some(old_index) = old_index {
            index_to_copy = self.merge_old_index(index_to_copy, old_index).await?;
        }

        let reader = self
            .spill_store
            .open_index_file(&Self::spill_filename(index_to_copy))
            .await?;

        let mut spill_stream = Self::stream_spill_reader(reader, MAX_POSTING_LIST_BATCH_BYTES)?;
        while let Some(state) = spill_stream.try_next().await? {
            Self::write_state(writer.as_mut(), state).await?;
        }

        writer.finish().await
    }

    async fn open_segment_stream(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
        filter: Option<super::OldIndexDataFilter>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<NGramIndexSpillState>> + Send>>> {
        let reader = store.open_index_file(POSTINGS_FILENAME).await?;
        let stream =
            Self::stream_spill_reader(reader, MAX_POSTING_LIST_BATCH_BYTES)?.map(move |res| {
                res.map(|state| {
                    state.remap_and_filter_rows(frag_reuse_index.as_ref(), filter.as_ref())
                })
            });
        Ok(Box::pin(stream))
    }

    /// Merge the source segments' posting files with any new-data spills into a
    /// single canonical posting file in `dest_store`.
    async fn merge_indices(
        mut self,
        new_data_spills: Vec<usize>,
        segment_stores: &[Arc<dyn IndexStore>],
        old_data_filters: &[Option<super::OldIndexDataFilter>],
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
        dest_store: &dyn IndexStore,
    ) -> Result<IndexFile> {
        if old_data_filters.len() != segment_stores.len() {
            return Err(Error::invalid_input(format!(
                "NGram merge: expected one old-data filter per source segment \
                 ({} segments) but got {}",
                segment_stores.len(),
                old_data_filters.len()
            )));
        }

        let mut writer = dest_store
            .new_index_file(POSTINGS_FILENAME, POSTINGS_SCHEMA.clone())
            .await?;

        // No segments and no new data: write an empty index.
        if segment_stores.is_empty() && new_data_spills.is_empty() {
            return writer.finish().await;
        }

        // Fast path
        if segment_stores.len() == 1 {
            let seg_stream = Self::open_segment_stream(
                segment_stores[0].clone(),
                frag_reuse_index,
                old_data_filters[0].clone(),
            )
            .await?;
            let new_data_stream: Pin<Box<dyn Stream<Item = Result<NGramIndexSpillState>> + Send>> =
                if new_data_spills.is_empty() {
                    Box::pin(stream::empty::<Result<NGramIndexSpillState>>())
                } else {
                    let new_data = self.merge_spills(new_data_spills).await?;
                    Box::pin(Self::stream_spill(self.spill_store.clone(), new_data).await?)
                };
            return Self::merge_spill_streams(seg_stream, new_data_stream, writer.as_mut()).await;
        }

        let mut spills = new_data_spills;
        let next_spill = spills.iter().copied().max().map_or(0, |id| id + 1);
        let mut tasks = Vec::new();
        for (offset, (stores, filters)) in segment_stores
            .chunks(2)
            .zip(old_data_filters.chunks(2))
            .enumerate()
        {
            let out = next_spill + offset;
            spills.push(out);
            let spill_store = self.spill_store.clone();
            let fri = frag_reuse_index.clone();
            let s0 = stores[0].clone();
            let f0 = filters[0].clone();
            let second = (stores.len() == 2).then(|| (stores[1].clone(), filters[1].clone()));
            tasks.push(tokio::spawn(async move {
                let mut writer = spill_store
                    .new_index_file(&Self::spill_filename(out), POSTINGS_SCHEMA.clone())
                    .await?;
                let mut left = Self::open_segment_stream(s0, fri.clone(), f0).await?;
                match second {
                    Some((s1, f1)) => {
                        let right = Self::open_segment_stream(s1, fri, f1).await?;
                        // `merge_spill_streams` finishes the writer itself.
                        Self::merge_spill_streams(left, right, writer.as_mut()).await?;
                    }
                    None => {
                        // Odd segment out: materialize it alone (cost of one segment).
                        while let Some(state) = left.try_next().await? {
                            Self::write_state(writer.as_mut(), state).await?;
                        }
                        writer.finish().await?;
                    }
                }
                Result::Ok(())
            }));
        }
        for result in futures::future::try_join_all(tasks).await? {
            result?;
        }

        // Balanced parallel union of every spill (new data + paired segments).
        let final_spill = self.merge_spills(spills).await?;
        let reader = self
            .spill_store
            .open_index_file(&Self::spill_filename(final_spill))
            .await?;
        let mut spill_stream = Self::stream_spill_reader(reader, MAX_POSTING_LIST_BATCH_BYTES)?;
        while let Some(state) = spill_stream.try_next().await? {
            Self::write_state(writer.as_mut(), state).await?;
        }
        writer.finish().await
    }
}

#[derive(Debug, Default)]
pub struct NGramIndexPlugin;

impl NGramIndexPlugin {
    pub async fn train_ngram_index(
        batches_source: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
    ) -> Result<IndexFile> {
        let mut builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default())?;

        let spill_files = builder.train(batches_source).await?;

        builder.write_index(index_store, spill_files, None).await
    }
}

#[async_trait]
impl BasicTrainer for NGramIndexPlugin {
    fn new_training_request(
        &self,
        _params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        if !matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            return Err(Error::invalid_input_source(format!(
                "A ngram index can only be created on a Utf8 or LargeUtf8 field.  Column has type {:?}",
                field.data_type()
            )
            .into()));
        }
        Ok(Box::new(DefaultTrainingRequest::new(
            TrainingCriteria::new(TrainingOrdering::None).with_row_id(),
        )))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        _request: Box<dyn TrainingRequest>,
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        // `fragment_ids` only scopes the rows scanned into `data`; the builder
        // always writes one canonical file, so a per-fragment build is a segment.
        let file = Self::train_ngram_index(data, index_store).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::NGramIndexDetails::default())?,
            index_version: NGRAM_INDEX_VERSION,
            files: vec![file],
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for NGramIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "NGram"
    }

    fn provides_exact_answer(&self) -> bool {
        false
    }

    fn version(&self) -> u32 {
        NGRAM_INDEX_VERSION
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(TextQueryParser::new(
            index_name,
            self.name().to_string(),
            // needs_recheck: ngram results are an inexact candidate superset.
            true,
            // supports_regex: the ngram index can answer regex queries.
            true,
            // min_contains_chars: a needle shorter than one trigram yields no
            // tokens, so the index cannot narrow the search at all.
            NGRAM_N,
        )))
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(NGramIndex::load(index_store, frag_reuse_index, cache).await? as Arc<dyn ScalarIndex>)
    }
}

#[cfg(test)]
mod tests {
    use lance_core::utils::row_addr_remap::RowAddrRemap;
    use rstest::rstest;
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use arrow::array::AsArray;
    use arrow::datatypes::{UInt32Type, UInt64Type};
    use arrow_array::{Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::{
        execution::SendableRecordBatchStream, physical_plan::stream::RecordBatchStreamAdapter,
    };
    use datafusion_common::DataFusionError;
    use futures::{TryStreamExt, stream};
    use itertools::Itertools;
    use lance_core::{Error, ROW_ID, Result, cache::LanceCache, utils::tempfile::TempDir};
    use lance_datagen::{BatchCount, ByteCount, RowCount};
    use lance_io::object_store::ObjectStore;
    use lance_select::RowAddrTreeMap;
    use lance_tokenizer::TextAnalyzer;
    use roaring::RoaringTreemap;

    use crate::scalar::{
        IndexReader, IndexStore, ScalarIndex, SearchResult, TextQuery,
        lance_format::LanceIndexStore,
        ngram::{NGramIndex, NGramIndexBuilder, NGramIndexBuilderOptions},
    };
    use crate::{metrics::NoOpMetricsCollector, scalar::registry::VALUE_COLUMN_NAME};

    use super::{
        NGRAM_TOKENIZER, NGramIndexSpillState, POSTINGS_FILENAME, POSTINGS_SCHEMA, ngram_to_token,
        tokenize_visitor,
    };

    struct MaxReadRangeReader {
        inner: Arc<dyn IndexReader>,
        max_rows: usize,
    }

    #[async_trait]
    impl IndexReader for MaxReadRangeReader {
        async fn read_record_batch(&self, n: u64, batch_size: u64) -> Result<RecordBatch> {
            self.inner.read_record_batch(n, batch_size).await
        }

        async fn read_range(
            &self,
            range: std::ops::Range<usize>,
            projection: Option<&[&str]>,
        ) -> Result<RecordBatch> {
            let rows = range.end - range.start;
            if rows > self.max_rows {
                return Err(Error::invalid_input(format!(
                    "read_range requested {} rows, max is {}",
                    rows, self.max_rows,
                )));
            }
            self.inner.read_range(range, projection).await
        }

        async fn num_batches(&self, batch_size: u64) -> u32 {
            self.inner.num_batches(batch_size).await
        }

        fn num_rows(&self) -> usize {
            self.inner.num_rows()
        }

        fn schema(&self) -> &lance_core::datatypes::Schema {
            self.inner.schema()
        }

        fn file_size_bytes(&self) -> Option<u64> {
            self.inner.file_size_bytes()
        }
    }

    fn collect_tokens(analyzer: &TextAnalyzer, text: &str) -> Vec<String> {
        let mut tokens = Vec::with_capacity(text.len() * 3);
        tokenize_visitor(analyzer, text, |token| tokens.push(token.to_owned()));
        tokens
    }

    #[test]
    fn test_tokenizer() {
        let tokenizer = NGRAM_TOKENIZER.clone();

        // ASCII folding
        let tokens = collect_tokens(&tokenizer, "café");
        assert_eq!(
            tokens,
            vec!["caf", "afe"] // spellchecker:disable-line
        );

        // Allow numbers
        let tokens = collect_tokens(&tokenizer, "a1b2");
        assert_eq!(tokens, vec!["a1b", "1b2"]);

        // Remove symbols and UTF-8 that doesn't map to characters
        let tokens = collect_tokens(&tokenizer, "abc👍b!c24");

        assert_eq!(tokens, vec!["abc", "c24"]);

        let tokens = collect_tokens(&tokenizer, "anstoß");

        assert_eq!(tokens, vec!["ans", "nst", "sto", "tos", "oss"]);

        // Lower casing
        let tokens = collect_tokens(&tokenizer, "ABC");
        assert_eq!(tokens, vec!["abc"]);

        // Duplicate tokens
        let tokens = collect_tokens(&tokenizer, "ababab");
        // Confirming that the tokenizer doesn't deduplicate tokens (this can be taken into consideration
        // when training the index)
        assert_eq!(
            tokens,
            vec!["aba", "bab", "aba", "bab"] // spellchecker:disable-line
        );
    }

    async fn do_train(
        mut builder: NGramIndexBuilder,
        data: SendableRecordBatchStream,
    ) -> (NGramIndex, Arc<TempDir>) {
        let spill_files = builder.train(data).await.unwrap();

        let tmpdir = Arc::new(TempDir::default());
        let test_store = LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        );

        builder
            .write_index(&test_store, spill_files, None)
            .await
            .unwrap();

        (
            NGramIndex::from_store(Arc::new(test_store), None, &LanceCache::no_cache())
                .await
                .unwrap(),
            tmpdir,
        )
    }

    async fn get_posting_list_for_trigram(index: &NGramIndex, trigram: &str) -> Vec<u64> {
        let token = ngram_to_token(trigram, 3);
        let row_offset = index.tokens[&token];
        let list = index
            .list_reader
            .ngram_list(row_offset, &NoOpMetricsCollector)
            .await
            .unwrap();
        list.bitmap.iter().sorted().collect()
    }

    async fn get_null_posting_list(index: &NGramIndex) -> Vec<u64> {
        let row_offset = index.tokens[&0];
        let list = index
            .list_reader
            .ngram_list(row_offset, &NoOpMetricsCollector)
            .await
            .unwrap();
        list.bitmap.iter().sorted().collect()
    }

    #[test_log::test(tokio::test)]
    async fn test_basic_ngram_index() {
        let data = StringArray::from_iter_values([
            "cat",
            "dog",
            "cat dog",
            "dog cat",
            "elephant",
            "mouse",
            "rhino",
            "giraffe",
            "rhinos nose",
        ]);
        let row_ids = UInt64Array::from_iter_values((0..data.len()).map(|i| i as u64));
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(data), Arc::new(row_ids)]).unwrap();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));

        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();

        let (index, _tmpdir) = do_train(builder, data).await;
        assert_eq!(index.tokens.len(), 21);

        // Basic search
        let res = index
            .search(
                &TextQuery::StringContains("cat".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();

        let expected = SearchResult::at_most(RowAddrTreeMap::from_iter([0, 2, 3]));

        assert_eq!(expected, res);

        // Whitespace in query
        let res = index
            .search(
                &TextQuery::StringContains("nos nos".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_most(RowAddrTreeMap::from_iter([8]));
        assert_eq!(expected, res);

        // No matches
        let res = index
            .search(
                &TextQuery::StringContains("tdo".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::exact(RowAddrTreeMap::new());
        assert_eq!(expected, res);

        // False positive
        let res = index
            .search(
                &TextQuery::StringContains("inose".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_most(RowAddrTreeMap::from_iter([8]));
        assert_eq!(expected, res);

        // Too short, don't know anything
        let res = index
            .search(
                &TextQuery::StringContains("ab".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_least(RowAddrTreeMap::new());
        assert_eq!(expected, res);

        // Two characters but four bytes: still too short to tokenize, so the
        // length check must count characters and not bytes.
        let res = index
            .search(
                &TextQuery::StringContains("éé".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_least(RowAddrTreeMap::new());
        assert_eq!(expected, res);

        // Long enough to tokenize, but the tokenizer's alphanumeric filter drops
        // every trigram, so again we know nothing.
        let res = index
            .search(
                &TextQuery::StringContains("---".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_least(RowAddrTreeMap::new());
        assert_eq!(expected, res);

        // One short string but we still get at least one trigram, this is ok
        let res = index
            .search(
                &TextQuery::StringContains("no nos".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_most(RowAddrTreeMap::from_iter([8]));
        assert_eq!(expected, res);
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_regex_search() {
        // Same corpus as test_basic_ngram_index.
        let data = StringArray::from_iter_values([
            "cat",         // 0
            "dog",         // 1
            "cat dog",     // 2
            "dog cat",     // 3
            "elephant",    // 4
            "mouse",       // 5
            "rhino",       // 6
            "giraffe",     // 7
            "rhinos nose", // 8
        ]);
        let row_ids = UInt64Array::from_iter_values((0..data.len()).map(|i| i as u64));
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, false),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(data), Arc::new(row_ids)]).unwrap();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));

        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, data).await;

        async fn search(index: &NGramIndex, pattern: &str) -> SearchResult {
            index
                .search(
                    &TextQuery::Regex(pattern.to_string()),
                    &NoOpMetricsCollector,
                )
                .await
                .unwrap()
        }

        // A plain literal yields the same candidates as contains("cat").
        assert_eq!(
            search(&index, "cat").await,
            SearchResult::at_most(RowAddrTreeMap::from_iter([0, 2, 3]))
        );

        // Alternation -> union of each branch's rows.
        assert_eq!(
            search(&index, "(cat|dog)").await,
            SearchResult::at_most(RowAddrTreeMap::from_iter([0, 1, 2, 3]))
        );

        // AND across `.*`: must contain both the `rhino` and `nose` trigrams, so
        // row 6 ("rhino") is correctly excluded and only row 8 survives.
        assert_eq!(
            search(&index, "rhino.*nose").await,
            SearchResult::at_most(RowAddrTreeMap::from_iter([8]))
        );

        // No derivable trigram -> recheck everything.
        assert_eq!(
            search(&index, "a.b").await,
            SearchResult::at_least(RowAddrTreeMap::new())
        );

        // A trigram that is absent from the index -> empty candidate set.
        assert_eq!(
            search(&index, "zzz").await,
            SearchResult::at_most(RowAddrTreeMap::new())
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_regex_search_nulls() {
        // Rows: cat(0), dog(1), NULL(2), NULL(3), cat dog(4).
        let data = simple_data_with_nulls();
        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, data).await;

        // The NULL rows (2, 3) must never appear in the candidate set.
        let res = index
            .search(&TextQuery::Regex("cat".to_string()), &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            res,
            SearchResult::at_most(RowAddrTreeMap::from_iter([0, 4]))
        );

        let res = index
            .search(
                &TextQuery::Regex("(cat|dog)".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        assert_eq!(
            res,
            SearchResult::at_most(RowAddrTreeMap::from_iter([0, 1, 4]))
        );
    }

    fn test_data_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]))
    }

    fn simple_data_with_nulls() -> SendableRecordBatchStream {
        let data = StringArray::from_iter(&[Some("cat"), Some("dog"), None, None, Some("cat dog")]);
        let row_ids = UInt64Array::from_iter_values((0..data.len()).map(|i| i as u64));
        let schema = test_data_schema();
        let data =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(data), Arc::new(row_ids)]).unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ))
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_nulls() {
        let data = simple_data_with_nulls();

        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();

        let (index, _tmpdir) = do_train(builder, data).await;
        assert_eq!(index.tokens.len(), 3);

        let res = index
            .search(
                &TextQuery::StringContains("cat".to_string()),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let expected = SearchResult::at_most(RowAddrTreeMap::from_iter([0, 4]));
        assert_eq!(expected, res);

        let null_posting_list = get_null_posting_list(&index).await;
        assert_eq!(null_posting_list, vec![2, 3]);

        // TODO: Support IS NULL queries
    }

    fn empty_data() -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            test_data_schema(),
            stream::empty::<lance_core::error::DataFusionResult<RecordBatch>>(),
        ))
    }

    #[test_log::test(tokio::test)]
    async fn test_train_empty() {
        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();

        let (index, _tmpdir) = do_train(builder, empty_data()).await;
        assert_eq!(index.tokens.len(), 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_update_empty() {
        let data = simple_data_with_nulls();

        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, empty_data()).await;

        let new_tmpdir = Arc::new(TempDir::default());
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        index.update(data, test_store.as_ref(), None).await.unwrap();

        let index = NGramIndex::from_store(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(index.tokens.len(), 3);
    }

    async fn row_ids_in_index(index: &NGramIndex) -> Vec<u64> {
        let mut row_ids = HashSet::new();
        for row_offset in index.tokens.values() {
            let list = index
                .list_reader
                .ngram_list(*row_offset, &NoOpMetricsCollector)
                .await
                .unwrap();
            row_ids.extend(list.bitmap.iter());
        }
        row_ids.into_iter().sorted().collect()
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_index_remap() {
        let data = simple_data_with_nulls();
        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, data).await;

        let row_ids = row_ids_in_index(&index).await;
        assert_eq!(row_ids, vec![0, 1, 2, 3, 4]);

        let new_tmpdir = Arc::new(TempDir::default());
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        let remapping = HashMap::from([(2, Some(100)), (3, None), (4, Some(101))]);
        index
            .remap(&RowAddrRemap::direct(remapping), test_store.as_ref())
            .await
            .unwrap();

        let index = NGramIndex::from_store(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let row_ids = row_ids_in_index(&index).await;
        assert_eq!(row_ids, vec![0, 1, 100, 101]);

        let null_posting_list = get_null_posting_list(&index).await;
        assert_eq!(null_posting_list, vec![100]);
    }

    // Like `test_ngram_index_remap` but covering both RowAddrRemap modes: rows
    // 0..4 of frag 0 are rewritten into frag 10; row 4 is deleted.
    fn ngram_remap_compact() -> RowAddrRemap {
        use lance_core::utils::row_addr_remap::GroupInput;
        use roaring::RoaringTreemap;
        RowAddrRemap::compact([GroupInput {
            rewritten_old_row_addrs: RoaringTreemap::from_iter(0u64..4),
            old_frag_ids: vec![0],
            new_frags: vec![(10, 4)],
        }])
        .unwrap()
    }

    fn ngram_remap_explicit() -> RowAddrRemap {
        RowAddrRemap::direct(
            (0u64..4)
                .map(|i| (i, Some((10u64 << 32) | i)))
                .chain(std::iter::once((4u64, None)))
                .collect(),
        )
    }

    #[rstest]
    #[case(ngram_remap_compact())]
    #[case(ngram_remap_explicit())]
    #[tokio::test]
    async fn test_ngram_index_remap_compact(#[case] remap: RowAddrRemap) {
        let data = simple_data_with_nulls();
        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, data).await;

        let row_ids = row_ids_in_index(&index).await;
        assert_eq!(row_ids, vec![0, 1, 2, 3, 4]);

        let new_tmpdir = Arc::new(TempDir::default());
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        index.remap(&remap, test_store.as_ref()).await.unwrap();

        let index = NGramIndex::from_store(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let addr = |offset: u64| (10u64 << 32) | offset;
        let row_ids = row_ids_in_index(&index).await;
        assert_eq!(row_ids, vec![addr(0), addr(1), addr(2), addr(3)]);

        // rows 2 and 3 are the null docs; both are rewritten into frag 10.
        let null_posting_list = get_null_posting_list(&index).await;
        assert_eq!(null_posting_list, vec![addr(2), addr(3)]);
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_index_merge() {
        let data = simple_data_with_nulls();
        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions::default()).unwrap();
        let (index, _tmpdir) = do_train(builder, data).await;

        let data = StringArray::from_iter(&[Some("giraffe"), Some("cat"), None]);
        let row_ids = UInt64Array::from_iter_values((0..data.len()).map(|i| i as u64 + 100));
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let data =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(data), Arc::new(row_ids)]).unwrap();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(std::future::ready(Ok(data))),
        ));

        let posting_list = get_posting_list_for_trigram(&index, "cat").await;
        assert_eq!(posting_list, vec![0, 4]);

        let new_tmpdir = Arc::new(TempDir::default());
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        ));

        index.update(data, test_store.as_ref(), None).await.unwrap();

        let index = NGramIndex::from_store(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let row_ids = row_ids_in_index(&index).await;
        assert_eq!(row_ids, vec![0, 1, 2, 3, 4, 100, 101, 102]);

        let posting_list = get_posting_list_for_trigram(&index, "cat").await;
        assert_eq!(posting_list, vec![0, 4, 101]);

        let posting_list = get_posting_list_for_trigram(&index, "ffe").await;
        assert_eq!(posting_list, vec![100]);

        let posting_list = get_null_posting_list(&index).await;
        assert_eq!(posting_list, vec![2, 3, 102]);
    }

    #[test]
    fn test_retain_row_ids_filters_by_allow_list() {
        use arrow_array::UInt32Array;
        use roaring::RoaringTreemap;

        use super::NGramIndexSpillState;
        use crate::scalar::OldIndexDataFilter;

        let addr = |frag: u32, local: u32| ((frag as u64) << 32) | local as u64;

        // Three tokens whose postings span fragments 0, 1 and 2.
        let state = NGramIndexSpillState {
            tokens: UInt32Array::from(vec![10u32, 20, 30]),
            bitmaps: vec![
                RoaringTreemap::from_iter([addr(0, 0), addr(0, 1), addr(1, 0), addr(2, 0)]),
                RoaringTreemap::from_iter([addr(2, 5)]),
                RoaringTreemap::from_iter([addr(1, 0), addr(1, 1), addr(1, 2)]),
            ],
        };

        // Allow-list: fragment 0 fully live, fragment 1 keeps only local row 0,
        // fragment 2 absent (fully retired).
        let mut valid = RowAddrTreeMap::new();
        valid.insert_fragment(0);
        valid.insert(addr(1, 0));

        let filtered = state.remap_and_filter_rows(None, Some(&OldIndexDataFilter::RowIds(valid)));

        // Token 20 lived only in retired fragment 2, so it is dropped entirely.
        assert_eq!(filtered.tokens.values().to_vec(), vec![10u32, 30]);
        // Token 10: fragment 0 kept whole, fragment 1 trimmed to local 0,
        // fragment 2 dropped.
        assert_eq!(
            filtered.bitmaps[0].iter().collect::<Vec<_>>(),
            vec![addr(0, 0), addr(0, 1), addr(1, 0)],
        );
        // Token 30: only the allow-listed local row 0 survives.
        assert_eq!(
            filtered.bitmaps[1].iter().collect::<Vec<_>>(),
            vec![addr(1, 0)]
        );
    }

    #[test]
    fn test_remap_then_keep_relocates_and_filters() {
        use arrow_array::UInt32Array;
        use roaring::{RoaringBitmap, RoaringTreemap};
        use uuid::Uuid;

        use super::NGramIndexSpillState;
        use crate::frag_reuse::{FragReuseIndex, FragReuseIndexDetails};
        use crate::scalar::OldIndexDataFilter;

        let addr = |frag: u32, local: u32| ((frag as u64) << 32) | local as u64;

        // One token spanning fragments 0 and 1.
        let state = NGramIndexSpillState {
            tokens: UInt32Array::from(vec![10u32]),
            bitmaps: vec![RoaringTreemap::from_iter([
                addr(0, 0),
                addr(0, 1),
                addr(1, 0),
            ])],
        };

        // Compaction fused fragment 0 into fragment 2: row (0,0) survives at
        // (2,0), row (0,1) was deleted (maps to None). Row (1,0) isn't in the
        // map, so remap passes it through unchanged.
        let fri = Arc::new(FragReuseIndex::new(
            Uuid::new_v4(),
            vec![HashMap::from([
                (addr(0, 0), Some(addr(2, 0))),
                (addr(0, 1), None),
            ])],
            FragReuseIndexDetails { versions: vec![] },
        ));

        // After remap the live rows sit in fragments 2 and 1; fragment 1 is retired.
        let filter = OldIndexDataFilter::Fragments {
            to_keep: RoaringBitmap::from_iter([2u32]),
            to_remove: RoaringBitmap::from_iter([1u32]),
        };

        let filtered = state.remap_and_filter_rows(Some(&fri), Some(&filter));

        // Only the relocated, still-live row survives: (0,0) -> (2,0). (0,1) was
        // dropped by remap; (1,0) was dropped by the retired-fragment filter.
        assert_eq!(filtered.tokens.values().to_vec(), vec![10u32]);
        assert_eq!(
            filtered.bitmaps[0].iter().collect::<Vec<_>>(),
            vec![addr(2, 0)]
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_ngram_index_with_spill() {
        let (data, schema) = lance_datagen::gen_batch()
            .col(
                VALUE_COLUMN_NAME,
                lance_datagen::array::rand_utf8(ByteCount::from(50), false),
            )
            .col(ROW_ID, lance_datagen::array::step::<UInt64Type>())
            .into_reader_stream(RowCount::from(128), BatchCount::from(32));

        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            data.map_err(|arrow_err| DataFusionError::ArrowError(Box::new(arrow_err), None)),
        ));

        let builder = NGramIndexBuilder::try_new(NGramIndexBuilderOptions {
            tokens_per_spill: 100,
        })
        .unwrap();

        let (index, _tmpdir) = do_train(builder, data).await;

        assert_eq!(index.tokens.len(), 29012);
    }

    #[test]
    fn test_spill_state_chunks_by_byte_size() {
        let bitmaps = (0..8u64)
            .map(|i| RoaringTreemap::from_iter(0..(i + 1) * 100))
            .collect::<Vec<_>>();
        let tokens = UInt32Array::from_iter_values(0..8);
        let state = NGramIndexSpillState {
            tokens: tokens.clone(),
            bitmaps: bitmaps.clone(),
        };

        // Small enough that several splits are required, large enough that some
        // batches hold more than one posting
        let max_batch_bytes = bitmaps.iter().map(|b| b.serialized_size()).max().unwrap() * 2;
        let batches = state.try_into_batches_impl(max_batch_bytes).unwrap();
        assert!(batches.len() > 1);

        // Token order and posting contents survive the chunking
        let mut row = 0;
        for batch in &batches {
            let batch_tokens = batch["tokens"].as_primitive::<UInt32Type>();
            let batch_postings = batch["posting_list"].as_binary::<i32>();
            let mut batch_bytes = 0;
            for i in 0..batch.num_rows() {
                assert_eq!(batch_tokens.value(i), tokens.value(row));
                let posting = batch_postings.value(i);
                batch_bytes += posting.len();
                assert_eq!(
                    RoaringTreemap::deserialize_from(posting).unwrap(),
                    bitmaps[row]
                );
                row += 1;
            }
            assert!(batch_bytes <= max_batch_bytes || batch.num_rows() == 1);
        }
        assert_eq!(row, 8);
    }

    #[test_log::test(tokio::test)]
    async fn test_spill_reader_does_not_materialize_multirow_posting_batches() {
        let bitmaps = (0..8u64)
            .map(|i| RoaringTreemap::from_iter(0..(i + 1) * 100))
            .collect::<Vec<_>>();
        let tokens = UInt32Array::from_iter_values(0..8);
        let state = NGramIndexSpillState {
            tokens: tokens.clone(),
            bitmaps: bitmaps.clone(),
        };
        let max_batch_bytes = bitmaps.iter().map(|b| b.serialized_size()).max().unwrap() * 2;

        let tmpdir = Arc::new(TempDir::default());
        let store = LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.obj_path(),
            Arc::new(LanceCache::no_cache()),
        );
        let mut writer = store
            .new_index_file(POSTINGS_FILENAME, POSTINGS_SCHEMA.clone())
            .await
            .unwrap();
        for batch in state.try_into_batches().unwrap() {
            writer.write_record_batch(batch).await.unwrap();
        }
        writer.finish().await.unwrap();

        let reader = store.open_index_file(POSTINGS_FILENAME).await.unwrap();
        let reader = Arc::new(MaxReadRangeReader {
            inner: reader,
            max_rows: 1,
        });
        let states = NGramIndexBuilder::stream_spill_reader(reader, max_batch_bytes)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(states.len() > 1);

        let mut row = 0;
        for state in states {
            let batch_bytes = state
                .bitmaps
                .iter()
                .map(RoaringTreemap::serialized_size)
                .sum::<usize>();
            assert!(batch_bytes <= max_batch_bytes || state.bitmaps.len() == 1);
            for (token, bitmap) in state.tokens.values().iter().zip(state.bitmaps) {
                assert_eq!(*token, tokens.value(row));
                assert_eq!(bitmap, bitmaps[row]);
                row += 1;
            }
        }
        assert_eq!(row, 8);
    }

    #[test]
    fn test_spill_state_rejects_oversized_posting() {
        let bitmap = RoaringTreemap::from_iter(0..1000u64);
        let too_small = bitmap.serialized_size() - 1;
        let state = NGramIndexSpillState {
            tokens: UInt32Array::from_iter_values([42]),
            bitmaps: vec![bitmap],
        };
        let err = state.try_into_batches_impl(too_small).unwrap_err();
        assert!(err.to_string().contains("token 42"), "{}", err);
    }

    #[test]
    fn test_empty_spill_state_yields_one_empty_batch() {
        let state = NGramIndexSpillState {
            tokens: UInt32Array::from_iter_values([]),
            bitmaps: vec![],
        };
        let batches = state.try_into_batches().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    // Reproduces https://linear.app/lancedb/issue/ENT-874: serialized posting lists
    // totalling more than i32::MAX bytes used to panic with "byte array offset
    // overflow" when packed into a single Binary array.
    #[test]
    #[ignore = "needs ~8 GiB of RAM and a couple of minutes; run manually"]
    fn test_spill_state_over_i32_max_bytes() {
        // Every 16th value keeps each container an array container (4096 entries,
        // 2 bytes per value, immune to run compression), so the treemap serializes
        // to ~450 MiB.  Six copies exceed i32::MAX total bytes.
        let bitmap = RoaringTreemap::from_sorted_iter((0..225_000_000u64).map(|v| v * 16)).unwrap();
        assert!(bitmap.serialized_size() > 400 * 1024 * 1024);
        let bitmaps = vec![bitmap; 6];
        let tokens = UInt32Array::from_iter_values(0..6);
        let state = NGramIndexSpillState { tokens, bitmaps };

        let batches = state.try_into_batches().unwrap();
        assert!(batches.len() > 1);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
        for batch in &batches {
            let postings = batch["posting_list"].as_binary::<i32>();
            assert!(postings.value_data().len() <= i32::MAX as usize);
        }
    }
}
