// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::{
    cmp::{Reverse, min},
    collections::BinaryHeap,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Range,
    time::Instant,
};

use crate::metrics::NoOpMetricsCollector;
use crate::prefilter::NoFilter;
use crate::scalar::registry::{TrainingCriteria, TrainingOrdering};
use crate::vector::graph::OrderedFloat;
use arrow::array::{BooleanBuilder, FixedSizeListBuilder, Float32Builder, Int32Builder};
use arrow::datatypes::{self, Float32Type, Int32Type, UInt64Type};
use arrow::{
    array::{
        AsArray, LargeBinaryBuilder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
    },
    buffer::{Buffer, OffsetBuffer},
};
use arrow::{buffer::ScalarBuffer, datatypes::UInt32Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, LargeBinaryArray, ListArray, OffsetSizeTrait,
    RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use fst::{Automaton, IntoStreamer, Streamer};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use itertools::{Either, Itertools};
use lance_arrow::{RecordBatchExt, iter_str_array};
use lance_core::cache::{
    CacheCodec, CacheKey, CacheKeySchema, KeyBuilder, LanceCache, WeakLanceCache,
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::{DataFusionResult, LanceOptionExt};
use lance_core::utils::address::RowAddress;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::utils::tracing::{IO_TYPE_LOAD_SCALAR_PART, TRACE_IO_EVENTS};
use lance_core::{Error, ROW_ID, ROW_ID_FIELD, Result};
use lance_select::{RowAddrMask, RowAddrTreeMap};
use roaring::RoaringBitmap;
use std::sync::LazyLock;
use tokio::{
    sync::{Mutex, OnceCell},
    task::spawn_blocking,
};
use tracing::{info, instrument, warn};

use super::documents::{
    DocId, DocLengths, DocVisibility, PartitionDocumentStore, PartitionDocuments,
};
use super::encoding::{MAX_POSTING_BLOCK_SIZE, PositionBlockBuilder};
use super::impact::{IMPACT_LEVEL1_BLOCKS, ImpactSkipData, ImpactSkipDataBuilder};
use super::iter::PostingListIterator;
use super::tokenizer::{LEGACY_BLOCK_SIZE, validate_block_size};
use super::{DocumentGranularity, InvertedIndexBuilder, InvertedIndexParams, wand::*};
use super::{
    builder::{
        BLOCK_SIZE, ScoredDoc, doc_file_path,
        inverted_list_schema_for_version_with_block_size_and_impacts, posting_file_path,
        token_file_path,
    },
    iter::PlainPostingListIterator,
    query::*,
    scorer::{B, IndexBM25Scorer, K1, Scorer, idf},
};
use super::{
    builder::{InnerBuilder, PositionRecorder},
    iter::CompressedPostingListIterator,
};
use crate::pbold;
use crate::progress::IndexBuildProgress;
use crate::scalar::inverted::scorer::MemBM25Scorer;
use crate::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use crate::scalar::{
    AnyQuery, BuiltinIndexType, CreatedIndex, IndexReader, IndexStore, MetricsCollector,
    OldIndexDataFilter, RowIdRemapper, ScalarIndex, ScalarIndexParams, SearchResult, TokenQuery,
    UpdateCriteria,
};
use crate::{FtsPrewarmOptions, Index};
use crate::{prefilter::PreFilter, scalar::inverted::iter::take_fst_keys};
use std::str::FromStr;

// Version 0: Arrow TokenSetFormat (legacy)
// Version 1: Fst TokenSetFormat with per-doc compressed positions
// Version 2: Fst TokenSetFormat with shared posting-list position streams.
// Version 3: Reader capability for configurable posting blocks, analyzer
// metadata, and element-document coordinates.
pub const INVERTED_INDEX_VERSION_V1: u32 = 1;
pub const INVERTED_INDEX_VERSION_V2: u32 = 2;
pub const INVERTED_INDEX_VERSION_V3: u32 = 3;
pub const TOKENS_FILE: &str = "tokens.lance";
pub const INVERT_LIST_FILE: &str = "invert.lance";
pub const DOCS_FILE: &str = "docs.lance";
pub const METADATA_FILE: &str = "metadata.lance";

/// Partitions searched per CPU-pool task. Each chunk loads concurrently and
/// then scores sequentially so query concurrency does not flood the pool with
/// one small task per partition. `LANCE_FTS_SEARCH_CHUNK=1` restores the
/// per-partition task shape.
fn fts_search_chunk() -> usize {
    static CHUNK: LazyLock<usize> = LazyLock::new(|| {
        std::env::var("LANCE_FTS_SEARCH_CHUNK")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|&value| value >= 1)
            .unwrap_or(16)
    });
    *CHUNK
}

pub const TOKEN_COL: &str = "_token";
pub const TOKEN_ID_COL: &str = "_token_id";
pub const TOKEN_FST_BYTES_COL: &str = "_token_fst_bytes";
pub const TOKEN_NEXT_ID_COL: &str = "_token_next_id";
pub const TOKEN_TOTAL_LENGTH_COL: &str = "_token_total_length";
pub const FREQUENCY_COL: &str = "_frequency";
pub const POSITION_COL: &str = "_position";
pub const COMPRESSED_POSITION_COL: &str = "_compressed_position";
pub const POSITION_BLOCK_OFFSET_COL: &str = "_position_block_offset";
pub const POSTING_COL: &str = "_posting";
pub const IMPACT_COL: &str = "_impacts";
pub const MAX_SCORE_COL: &str = "_max_score";
pub const LENGTH_COL: &str = "_length";
pub const BLOCK_MAX_SCORE_COL: &str = "_block_max_score";
pub const NUM_TOKEN_COL: &str = "_num_tokens";
pub const DOC_INDEX_COL: &str = "_doc_index";
pub const DOC_INDEX_STORAGE_PREFIX: &str = "_doc_index_";
pub const SCORE_COL: &str = "_score";
pub const TOKEN_SET_FORMAT_KEY: &str = "token_set_format";
pub const POSTING_TAIL_CODEC_KEY: &str = "posting_tail_codec";
pub const FTS_FORMAT_VERSION_KEY: &str = "format_version";
pub const POSITIONS_LAYOUT_KEY: &str = "positions_layout";
pub const POSITIONS_CODEC_KEY: &str = "positions_codec";
pub const POSTING_BLOCK_SIZE_KEY: &str = "posting_block_size";
pub const POSTING_TAIL_CODEC_FIXED32_V1: &str = "fixed32_v1";
pub const POSTING_TAIL_CODEC_VARINT_DELTA_V1: &str = "varint_delta_v1";
pub const POSITIONS_LAYOUT_SHARED_STREAM_V2: &str = "shared_stream_v2";
pub const POSITIONS_CODEC_VARINT_DOC_DELTA_V2: &str = "varint_doc_delta_v2";
pub const POSITIONS_CODEC_PACKED_DELTA_V1: &str = "packed_delta_v1";
pub const DELETED_FRAGMENTS_COL: &str = "deleted_fragments";

// Just a heuristic when we need to pre-allocate memory for tokens
pub const ESTIMATED_MAX_TOKENS_PER_ROW: usize = 4 * 1024;

pub static SCORE_FIELD: LazyLock<Field> =
    LazyLock::new(|| Field::new(SCORE_COL, DataType::Float32, true));
pub static DOC_INDEX_FIELD: LazyLock<Field> = LazyLock::new(|| {
    Field::new(
        DOC_INDEX_COL,
        DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
        false,
    )
});
pub static FTS_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone(), SCORE_FIELD.clone()])));
pub static ELEMENT_FTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        ROW_ID_FIELD.clone(),
        DOC_INDEX_FIELD.clone(),
        SCORE_FIELD.clone(),
    ]))
});

pub fn fts_schema(document_granularity: DocumentGranularity) -> SchemaRef {
    if document_granularity.is_list_element() {
        ELEMENT_FTS_SCHEMA.clone()
    } else {
        FTS_SCHEMA.clone()
    }
}
static ROW_ID_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()])));

pub fn resolve_fts_format_version(
    value: Option<&str>,
) -> std::result::Result<InvertedListFormatVersion, Error> {
    match value {
        Some(value) => value.parse(),
        None => Ok(default_fts_format_version()),
    }
}

pub fn default_fts_format_version() -> InvertedListFormatVersion {
    InvertedListFormatVersion::V2
}

pub fn current_fts_format_version() -> InvertedListFormatVersion {
    default_fts_format_version()
}

pub fn max_supported_fts_format_version() -> InvertedListFormatVersion {
    InvertedListFormatVersion::V3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum InvertedListFormatVersion {
    V1,
    #[default]
    V2,
    V3,
}

impl InvertedListFormatVersion {
    pub fn from_posting_tail_codec(codec: PostingTailCodec) -> Self {
        match codec {
            PostingTailCodec::Fixed32 => Self::V1,
            PostingTailCodec::VarintDelta => Self::V2,
        }
    }

    pub fn from_posting_tail_codec_and_block_size(
        codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        validate_block_size(block_size)?;
        let format_version = match (codec, block_size) {
            (PostingTailCodec::Fixed32, LEGACY_BLOCK_SIZE) => Self::V1,
            (PostingTailCodec::VarintDelta, LEGACY_BLOCK_SIZE) => Self::V2,
            (PostingTailCodec::VarintDelta, 256) => Self::V3,
            (PostingTailCodec::Fixed32, 256) => {
                return Err(Error::invalid_input(
                    "FTS format_version=3 requires the varint-delta posting tail codec".to_string(),
                ));
            }
            _ => unreachable!("validate_block_size limits supported block sizes"),
        };
        validate_format_version_block_size(format_version, block_size)?;
        Ok(format_version)
    }

    pub fn index_version(self) -> u32 {
        match self {
            Self::V1 => INVERTED_INDEX_VERSION_V1,
            Self::V2 => INVERTED_INDEX_VERSION_V2,
            Self::V3 => INVERTED_INDEX_VERSION_V3,
        }
    }

    pub fn posting_tail_codec(self) -> PostingTailCodec {
        match self {
            Self::V1 => PostingTailCodec::Fixed32,
            Self::V2 | Self::V3 => PostingTailCodec::VarintDelta,
        }
    }

    pub fn position_codec(self) -> Option<PositionStreamCodec> {
        match self {
            Self::V1 => None,
            Self::V2 | Self::V3 => Some(PositionStreamCodec::PackedDelta),
        }
    }

    pub fn uses_shared_position_stream(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }
}

impl FromStr for InvertedListFormatVersion {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim() {
            "1" | "v1" | "V1" => Ok(Self::V1),
            "2" | "v2" | "V2" => Ok(Self::V2),
            "3" | "v3" | "V3" => Ok(Self::V3),
            other => Err(Error::index(format!(
                "unsupported FTS format version {}, expected 1, 2, or 3",
                other
            ))),
        }
    }
}

pub fn default_fts_format_version_for_block_size(
    block_size: usize,
) -> Result<InvertedListFormatVersion> {
    validate_block_size(block_size)?;
    match block_size {
        LEGACY_BLOCK_SIZE => Ok(InvertedListFormatVersion::V2),
        256 => Ok(InvertedListFormatVersion::V3),
        _ => unreachable!("validate_block_size limits supported block sizes"),
    }
}

pub fn validate_format_version_block_size(
    format_version: InvertedListFormatVersion,
    block_size: usize,
) -> Result<()> {
    validate_block_size(block_size)?;
    match (format_version, block_size) {
        (InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)
        | (InvertedListFormatVersion::V3, _) => Ok(()),
        (InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2, 256) => {
            Err(Error::invalid_input(format!(
                "FTS format_version={} is incompatible with block_size=256; use format_version=3",
                format_version.index_version()
            )))
        }
        _ => unreachable!("validate_block_size limits supported block sizes"),
    }
}

#[derive(Debug)]
struct PartitionCandidates<C> {
    tokens_by_position: Vec<String>,
    grouped_expansions: Vec<GroupedExpansionTerms>,
    candidates: Vec<DocCandidate<C>>,
}

struct ModernSearchRequest<'a> {
    tokens: Arc<Tokens>,
    params: Arc<FtsSearchParams>,
    operator: Operator,
    mask: Arc<RowAddrMask>,
    metrics: Arc<dyn MetricsCollector>,
    scorer: &'a MemBM25Scorer,
    impact_scorer: Arc<MemBM25Scorer>,
    limit: usize,
}

/// Typed identity for one modern candidate after partition-local scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionDocId {
    partition_ordinal: u32,
    doc_id: DocId,
}

impl PartitionDocId {
    fn try_new(partition_ordinal: usize, doc_id: DocId) -> Result<Self> {
        Ok(Self {
            partition_ordinal: u32::try_from(partition_ordinal).map_err(|_| {
                Error::index(format!(
                    "FTS partition ordinal {partition_ordinal} exceeds candidate identity capacity"
                ))
            })?,
            doc_id,
        })
    }

    fn partition_ordinal(self) -> usize {
        self.partition_ordinal as usize
    }
}

#[derive(Debug, Clone)]
struct ScoredPartitionDoc {
    document: PartitionDocId,
    score: OrderedFloat,
}

impl ScoredPartitionDoc {
    fn new(document: PartitionDocId, score: f32) -> Self {
        Self {
            document,
            score: OrderedFloat(score),
        }
    }
}

impl PartialEq for ScoredPartitionDoc {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredPartitionDoc {}

impl PartialOrd for ScoredPartitionDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredPartitionDoc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.cmp(&other.score)
    }
}

const MAX_CONCURRENT_ADDRESS_READ_BYTES: usize = 64 * 1024 * 1024;

fn address_read_concurrency(io_parallelism: usize, largest_read_bytes: usize) -> usize {
    let io_parallelism = io_parallelism.max(1);
    if largest_read_bytes == 0 {
        return io_parallelism;
    }
    io_parallelism.min(
        MAX_CONCURRENT_ADDRESS_READ_BYTES
            .checked_div(largest_read_bytes)
            .unwrap_or(0)
            .max(1),
    )
}

fn push_scored_key(
    candidates: &mut BinaryHeap<Reverse<ScoredDoc>>,
    limit: usize,
    key: u64,
    score: f32,
) {
    if candidates.len() < limit {
        candidates.push(Reverse(ScoredDoc::new(key, score)));
    } else if candidates
        .peek()
        .is_some_and(|candidate| candidate.0.score.0 < score)
    {
        candidates.pop();
        candidates.push(Reverse(ScoredDoc::new(key, score)));
    }
}

fn push_scored_partition_doc(
    candidates: &mut BinaryHeap<Reverse<ScoredPartitionDoc>>,
    limit: usize,
    document: PartitionDocId,
    score: f32,
) {
    if candidates.len() < limit {
        candidates.push(Reverse(ScoredPartitionDoc::new(document, score)));
    } else if candidates
        .peek()
        .is_some_and(|candidate| candidate.0.score.0 < score)
    {
        candidates.pop();
        candidates.push(Reverse(ScoredPartitionDoc::new(document, score)));
    }
}

fn rescore_partition_candidates<C>(
    partition: PartitionCandidates<C>,
    scorer: &MemBM25Scorer,
    idf_cache: &mut HashMap<String, f32>,
) -> Vec<(C, f32)> {
    let PartitionCandidates {
        tokens_by_position,
        grouped_expansions,
        candidates,
    } = partition;
    let idf_by_position = tokens_by_position
        .iter()
        .map(|token| {
            *idf_cache
                .entry(token.clone())
                .or_insert_with(|| scorer.query_weight(token))
        })
        .collect::<Vec<_>>();
    let grouped_positions = grouped_expansions
        .iter()
        .map(|group| group.position)
        .collect::<HashSet<_>>();

    candidates
        .into_iter()
        .map(
            |DocCandidate {
                 document,
                 posting_doc_id,
                 freqs,
                 doc_length,
             }| {
                let mut score = 0.0;
                for (term_index, freq) in freqs {
                    if grouped_positions.contains(&term_index) {
                        continue;
                    }
                    debug_assert!((term_index as usize) < idf_by_position.len());
                    score +=
                        idf_by_position[term_index as usize] * scorer.doc_weight(freq, doc_length);
                }
                for group in &grouped_expansions {
                    for term in group.terms.iter() {
                        let Some(freq) = term.frequency(posting_doc_id) else {
                            continue;
                        };
                        score += term.query_weight() * scorer.doc_weight(freq, doc_length);
                    }
                }
                (document, score)
            },
        )
        .collect()
}

#[derive(Debug)]
pub(super) struct LoadedPostings {
    pub(super) postings: Vec<PostingIterator>,
    grouped_expansions: Vec<GroupedExpansionTerms>,
    impact_safe: bool,
    exact_scoring_required: bool,
}

enum LoadedDocLengths {
    Legacy(Arc<DocSet>),
    Modern(Arc<DocLengths>),
}

impl LoadedDocLengths {
    fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        match self {
            Self::Legacy(docs) => docs.scoring_num_tokens(doc_id),
            Self::Modern(lengths) => lengths.scoring(DocId::new(doc_id)),
        }
    }

    fn num_tokens_by_row_id(&self, row_id: u64) -> u32 {
        match self {
            Self::Legacy(docs) => docs.num_tokens_by_row_id(row_id),
            Self::Modern(_) => unreachable!("modern posting lists use dense DocIds"),
        }
    }
}

impl LoadedPostings {
    fn empty() -> Self {
        Self {
            postings: Vec::new(),
            grouped_expansions: Vec::new(),
            impact_safe: false,
            exact_scoring_required: false,
        }
    }
}

#[derive(Debug)]
struct GroupedExpansionTerms {
    position: u32,
    terms: Arc<[GroupedTermScorer]>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum TokenSetFormat {
    Arrow,
    #[default]
    Fst,
}

impl Display for TokenSetFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow => f.write_str("arrow"),
            Self::Fst => f.write_str("fst"),
        }
    }
}

impl FromStr for TokenSetFormat {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim() {
            "" => Ok(Self::Arrow),
            "arrow" => Ok(Self::Arrow),
            "fst" => Ok(Self::Fst),
            other => Err(Error::index(format!(
                "unsupported token set format {}",
                other
            ))),
        }
    }
}

impl DeepSizeOf for TokenSetFormat {
    fn deep_size_of_children(&self, _: &mut lance_core::deepsize::Context) -> usize {
        0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PositionStreamCodec {
    VarintDocDelta,
    #[default]
    PackedDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PostingTailCodec {
    Fixed32,
    #[default]
    VarintDelta,
}

impl PostingTailCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fixed32 => POSTING_TAIL_CODEC_FIXED32_V1,
            Self::VarintDelta => POSTING_TAIL_CODEC_VARINT_DELTA_V1,
        }
    }

    fn from_metadata_value(value: &str) -> Result<Self> {
        match value.trim() {
            POSTING_TAIL_CODEC_FIXED32_V1 => Ok(Self::Fixed32),
            POSTING_TAIL_CODEC_VARINT_DELTA_V1 => Ok(Self::VarintDelta),
            other => Err(Error::index(format!(
                "unsupported posting tail codec {}",
                other
            ))),
        }
    }
}

pub(super) fn parse_posting_tail_codec(
    metadata: &HashMap<String, String>,
) -> Result<PostingTailCodec> {
    Ok(metadata
        .get(POSTING_TAIL_CODEC_KEY)
        .map(|codec| PostingTailCodec::from_metadata_value(codec))
        .transpose()?
        .unwrap_or(PostingTailCodec::Fixed32))
}

pub(super) fn parse_posting_block_size(metadata: &HashMap<String, String>) -> Result<usize> {
    metadata
        .get(POSTING_BLOCK_SIZE_KEY)
        .map(|value| {
            let block_size = value.parse::<usize>().map_err(|err| {
                Error::index(format!(
                    "invalid {POSTING_BLOCK_SIZE_KEY} metadata value {value:?}: {err}"
                ))
            })?;
            validate_block_size(block_size)
        })
        .transpose()
        .map(|block_size| block_size.unwrap_or(LEGACY_BLOCK_SIZE))
}

impl PositionStreamCodec {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::VarintDocDelta => POSITIONS_CODEC_VARINT_DOC_DELTA_V2,
            Self::PackedDelta => POSITIONS_CODEC_PACKED_DELTA_V1,
        }
    }

    fn from_metadata_value(value: &str) -> Result<Self> {
        match value.trim() {
            POSITIONS_CODEC_VARINT_DOC_DELTA_V2 => Ok(Self::VarintDocDelta),
            POSITIONS_CODEC_PACKED_DELTA_V1 => Ok(Self::PackedDelta),
            other => Err(Error::index(format!(
                "unsupported positions codec {}",
                other
            ))),
        }
    }
}

fn parse_shared_position_codec(metadata: &HashMap<String, String>) -> Result<PositionStreamCodec> {
    if let Some(codec) = metadata.get(POSITIONS_CODEC_KEY) {
        return PositionStreamCodec::from_metadata_value(codec);
    }

    match metadata
        .get(POSITIONS_LAYOUT_KEY)
        .map(|layout| layout.as_str())
    {
        Some(POSITIONS_LAYOUT_SHARED_STREAM_V2) => Ok(PositionStreamCodec::VarintDocDelta),
        _ => Ok(PositionStreamCodec::VarintDocDelta),
    }
}

pub(super) fn parse_format_version_from_metadata(
    metadata: &HashMap<String, String>,
) -> Result<InvertedListFormatVersion> {
    if let Some(value) = metadata.get(FTS_FORMAT_VERSION_KEY) {
        let format_version = InvertedListFormatVersion::from_str(value)?;
        let block_size = parse_posting_block_size(metadata)?;
        validate_format_version_block_size(format_version, block_size)?;
        return Ok(format_version);
    }
    let block_size = parse_posting_block_size(metadata)?;
    if block_size == 256 {
        if metadata
            .get(POSTING_TAIL_CODEC_KEY)
            .map(|_| parse_posting_tail_codec(metadata))
            .transpose()?
            .is_some_and(|posting_tail_codec| posting_tail_codec != PostingTailCodec::VarintDelta)
        {
            return Err(Error::index(
                "FTS block_size=256 requires the varint-delta posting tail codec".to_string(),
            ));
        }
        return Ok(InvertedListFormatVersion::V3);
    }
    if metadata.contains_key(POSITIONS_CODEC_KEY) || metadata.contains_key(POSITIONS_LAYOUT_KEY) {
        return Ok(InvertedListFormatVersion::V2);
    }
    if parse_posting_tail_codec(metadata)? == PostingTailCodec::VarintDelta {
        Ok(InvertedListFormatVersion::V2)
    } else {
        Ok(InvertedListFormatVersion::V1)
    }
}

#[derive(Debug, Default)]
struct InvertedPrewarmState {
    query_ready: bool,
    positions_ready: bool,
}

impl InvertedPrewarmState {
    fn satisfies(&self, with_position: bool) -> bool {
        self.query_ready && (!with_position || self.positions_ready)
    }
}

#[derive(Clone)]
pub struct InvertedIndex {
    params: InvertedIndexParams,
    store: Arc<dyn IndexStore>,
    tokenizer: Box<dyn LanceTokenizer>,
    token_set_format: TokenSetFormat,
    format_version: InvertedListFormatVersion,
    pub(crate) partitions: Vec<Arc<InvertedPartition>>,
    corpus_stats: Arc<OnceCell<(u64, usize)>>,
    prewarm_state: Arc<Mutex<InvertedPrewarmState>>,
    /// Optimistic fast-path hint. Cache eviction can make it stale; the
    /// resident resolver clears it when a weak projection upgrade misses.
    document_projections_resident: Arc<AtomicBool>,
    // Fragments which are contained in the index, but no longer in the dataset.
    // These should be pruned at search time since we don't prune them at update time.
    deleted_fragments: RoaringBitmap,
}

impl Debug for InvertedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvertedIndex")
            .field("params", &self.params)
            .field("token_set_format", &self.token_set_format)
            .field("format_version", &self.format_version)
            .field("partitions", &self.partitions)
            .field("deleted_fragments", &self.deleted_fragments)
            .finish()
    }
}

impl DeepSizeOf for InvertedIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.partitions.deep_size_of_children(context)
    }
}

impl InvertedIndex {
    fn format_version(&self) -> InvertedListFormatVersion {
        self.format_version
    }

    fn index_version(&self) -> u32 {
        if self.params.get_document_granularity().is_list_element() {
            return INVERTED_INDEX_VERSION_V3;
        }
        match (self.token_set_format, self.format_version()) {
            (
                TokenSetFormat::Arrow,
                InvertedListFormatVersion::V1 | InvertedListFormatVersion::V2,
            ) => 0,
            (_, format_version) => format_version.index_version(),
        }
    }

    fn posting_tail_codec(&self) -> PostingTailCodec {
        self.partitions
            .first()
            .map(|partition| partition.inverted_list.posting_tail_codec())
            .unwrap_or_default()
    }

    fn to_builder(&self) -> InvertedIndexBuilder {
        self.to_builder_with_offset(None)
    }

    fn to_builder_with_offset(&self, fragment_mask: Option<u64>) -> InvertedIndexBuilder {
        if self.is_legacy() {
            // for legacy format, we re-create the index in the new format
            InvertedIndexBuilder::from_existing_index(
                self.params.clone(),
                None,
                Vec::new(),
                self.token_set_format,
                fragment_mask,
                self.deleted_fragments.clone(),
            )
            .with_posting_tail_codec(self.posting_tail_codec())
        } else {
            let partitions = match fragment_mask {
                Some(fragment_mask) => self
                    .partitions
                    .iter()
                    // Filter partitions that belong to the specified fragment
                    // The mask contains fragment_id in high 32 bits, we check if partition's
                    // fragment_id matches by comparing the masked result with the original mask
                    .filter(|part| part.belongs_to_fragment(fragment_mask))
                    .map(|part| part.id())
                    .collect(),
                None => self.partitions.iter().map(|part| part.id()).collect(),
            };

            InvertedIndexBuilder::from_existing_index(
                self.params.clone(),
                Some(self.store.clone()),
                partitions,
                self.token_set_format,
                fragment_mask,
                self.deleted_fragments.clone(),
            )
            .with_format_version(self.format_version())
        }
    }

    pub fn tokenizer(&self) -> Box<dyn LanceTokenizer> {
        self.tokenizer.clone()
    }

    pub fn params(&self) -> &InvertedIndexParams {
        &self.params
    }

    /// Returns the number of partitions in this inverted index.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    /// Returns the set of fragments which are contained in the index, but no longer in the dataset.
    ///
    /// Most other indices remove data from deleted fragments when the index updates (copy-on-write).
    /// However, this would require an expensive copy of the FTS index.  Instead, we track the deleted
    /// fragments and prune them at search time (merge-on-read).
    pub fn deleted_fragments(&self) -> &RoaringBitmap {
        &self.deleted_fragments
    }

    pub async fn merge_segments(
        segments: &[Arc<Self>],
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<OldIndexDataFilter>,
        progress: Arc<dyn IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let Some(first) = segments.first() else {
            return Err(Error::invalid_input(
                "cannot merge inverted index without at least one source segment".to_string(),
            ));
        };

        for segment in segments.iter().skip(1) {
            if segment.params != first.params {
                return Err(Error::index(
                    "cannot merge inverted index segments with different parameters".to_string(),
                ));
            }
            if segment.token_set_format != first.token_set_format {
                return Err(Error::index(
                    "cannot merge inverted index segments with different token set formats"
                        .to_string(),
                ));
            }
            if segment.format_version() != first.format_version() {
                return Err(Error::index(
                    "cannot merge inverted index segments with different format versions"
                        .to_string(),
                ));
            }
            if segment.posting_tail_codec() != first.posting_tail_codec() {
                return Err(Error::index(
                    "cannot merge inverted index segments with different posting tail codecs"
                        .to_string(),
                ));
            }
        }

        let mut builder = InvertedIndexBuilder::new(first.params.clone()).with_progress(progress);
        builder = builder
            .with_token_set_format(first.token_set_format)
            .with_format_version(first.format_version());
        let files = builder
            .update_from_segments(new_data, dest_store, segments, old_data_filter)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&first.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: first.index_version(),
            files,
        })
    }

    /// Build a single-segment [`MemBM25Scorer`] whose per-term IDF table
    /// covers every token that the per-partition scoring loop will look
    /// up. For fuzzy queries that means the union of Levenshtein
    /// expansions, not just the raw query tokens — otherwise
    /// `query_weight(expanded_token)` returns 0 and the BM25 contribution
    /// of every expanded match is discarded.
    pub async fn bm25_base_scorer(
        &self,
        query_tokens: &Tokens,
        params: &FtsSearchParams,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<MemBM25Scorer> {
        if matches!(params.fuzziness, Some(n) if n != 0) {
            let expanded = self.expand_fuzzy_tokens(query_tokens, params)?;
            self.bm25_scorer_for_final_tokens(&expanded, metrics).await
        } else {
            self.bm25_scorer_for_final_tokens(query_tokens, metrics)
                .await
        }
    }

    /// Scorer for a token list that needs no further fuzzy expansion: dedup
    /// the terms and pull their document frequencies. `bm25_search` calls
    /// this with the tokens it already expanded, so the expansion runs once
    /// per query rather than once for the scorer and once per partition.
    async fn bm25_scorer_for_final_tokens(
        &self,
        tokens: &Tokens,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<MemBM25Scorer> {
        let (total_tokens, num_docs) = self.aggregate_corpus_stats().await?;
        let mut terms: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for token in tokens {
            if seen.insert(token.to_string()) {
                terms.push(token.to_string());
            }
        }
        let mut token_docs = HashMap::with_capacity(terms.len());
        for term in &terms {
            let df = self.df_for_term(term, metrics).await?;
            token_docs.insert(term.clone(), df);
        }
        Ok(MemBM25Scorer::new(total_tokens, num_docs, token_docs))
    }

    pub async fn bm25_stats_for_terms(
        &self,
        terms: &[String],
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<(u64, usize, Vec<usize>)> {
        let (total_tokens, num_docs) = self.aggregate_corpus_stats().await?;
        let token_docs =
            futures::future::try_join_all(terms.iter().map(|term| self.df_for_term(term, metrics)))
                .await?;
        Ok((total_tokens, num_docs, token_docs))
    }

    /// Aggregate immutable per-partition corpus statistics.  New modern files
    /// read both values from the already-opened docs footer; older partitioned
    /// files scan `_num_tokens` once as a compatibility fallback.
    async fn aggregate_corpus_stats(&self) -> Result<(u64, usize)> {
        self.corpus_stats
            .get_or_try_init(|| async {
                let io_parallelism = self.store.io_parallelism();
                let futures = self
                    .partitions
                    .iter()
                    .map(|p| {
                        let part = p.clone();
                        async move { part.docs.stats().await }
                    })
                    .collect::<Vec<_>>();
                let stats = stream::iter(futures)
                    .buffer_unordered(io_parallelism)
                    .try_collect::<Vec<_>>()
                    .await?;
                let mut total_tokens = 0_u64;
                let mut num_docs = 0_usize;
                for stat in stats {
                    total_tokens = total_tokens
                        .checked_add(stat.total_tokens)
                        .ok_or_else(|| Error::index("FTS corpus token count overflows u64"))?;
                    num_docs = num_docs
                        .checked_add(stat.num_docs)
                        .ok_or_else(|| Error::index("FTS corpus document count overflows usize"))?;
                }
                Ok((total_tokens, num_docs))
            })
            .await
            .copied()
    }

    /// Sum the posting-list length for `term` across this index's partitions
    /// via single-row reads, with partition lookups bounded by the store's
    /// `io_parallelism()`.
    async fn df_for_term(
        &self,
        term: &str,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<usize> {
        let io_parallelism = self.store.io_parallelism();
        let futures = self
            .partitions
            .iter()
            .map(|part| {
                let part = part.clone();
                async move {
                    match part.tokens.get(term) {
                        Some(token_id) => {
                            part.inverted_list
                                .posting_len_for_token(token_id, metrics)
                                .await
                        }
                        None => Ok(0),
                    }
                }
            })
            .collect::<Vec<_>>();
        let dfs: Vec<usize> = stream::iter(futures)
            .buffer_unordered(io_parallelism)
            .try_collect()
            .await?;
        Ok(dfs.into_iter().sum())
    }

    /// Expand fuzzy query tokens against all partitions in this segment.
    ///
    /// `params.max_expansions` caps the whole query's expansion, not any
    /// single partition's: for each query token the per-partition candidates
    /// (each streamed in FST key order) merge into one lexicographically
    /// ordered set, and the remaining budget takes a prefix of it. The
    /// selected terms are a pure function of the segment's vocabulary, so
    /// splitting the same corpus into more partitions cannot change which
    /// terms a fuzzy query matches.
    pub fn expand_fuzzy_tokens(&self, tokens: &Tokens, params: &FtsSearchParams) -> Result<Tokens> {
        let mut expanded_tokens = Vec::new();
        let mut expanded_positions = Vec::new();
        let mut seen = HashSet::new();
        for token_idx in 0..tokens.len() {
            let remaining = params.max_expansions.saturating_sub(expanded_tokens.len());
            if remaining == 0 {
                break;
            }
            let token = tokens.get_token(token_idx);
            let position = tokens.position(token_idx);
            // Each partition contributes at most its `remaining`
            // lexicographically smallest candidates, so the global
            // lex-smallest `remaining` selection below is unaffected by the
            // per-partition truncation.
            let mut candidates = BTreeSet::new();
            let base_prefix_len = tokens.token_type().prefix_len(token) as u32;
            for partition in &self.partitions {
                partition.collect_fuzzy_candidates(
                    token,
                    base_prefix_len,
                    params,
                    remaining,
                    &mut candidates,
                )?;
            }
            for candidate in candidates {
                if expanded_tokens.len() >= params.max_expansions {
                    break;
                }
                if seen.insert((candidate.clone(), position)) {
                    expanded_tokens.push(candidate);
                    expanded_positions.push(position);
                }
            }
        }
        Ok(Tokens::with_positions(
            expanded_tokens,
            expanded_positions,
            tokens.token_type().clone(),
        ))
    }

    /// Search documents that match the query and return row ids sorted by BM25 score.
    ///
    /// When `base_scorer` is provided, search uses those corpus-level BM25 statistics
    /// instead of deriving them from this segment alone.
    #[instrument(level = "debug", skip_all)]
    pub async fn bm25_search(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        prefilter: Arc<dyn PreFilter>,
        metrics: Arc<dyn MetricsCollector>,
        base_scorer: Option<&MemBM25Scorer>,
    ) -> Result<(Vec<u64>, Vec<f32>)> {
        let documents = self
            .bm25_search_documents(tokens, params, operator, prefilter, metrics, base_scorer)
            .await?;
        Ok(documents
            .into_iter()
            .map(|document| (document.row_id, document.score.0))
            .unzip())
    }

    /// Search logical FTS documents, retaining element coordinates when present.
    #[instrument(level = "debug", skip_all)]
    pub async fn bm25_search_documents(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        prefilter: Arc<dyn PreFilter>,
        metrics: Arc<dyn MetricsCollector>,
        base_scorer: Option<&MemBM25Scorer>,
    ) -> Result<Vec<ScoredDoc>> {
        // Fuzzy expansion runs once here, with the global `max_expansions`
        // budget, instead of once per partition: partitions receive the
        // final token list, so the matched terms cannot depend on how the
        // corpus happens to be partitioned.
        let tokens = if matches!(params.fuzziness, Some(n) if n != 0) {
            let expanded = Arc::new(self.expand_fuzzy_tokens(tokens.as_ref(), params.as_ref())?);
            if operator == Operator::And || params.phrase_slop.is_some() {
                // AND/phrase semantics require every original token position
                // to keep at least one expansion; a position that expands to
                // nothing anywhere in the segment can never be matched.
                let surviving = (0..expanded.len())
                    .map(|idx| expanded.position(idx))
                    .collect::<HashSet<_>>();
                if (0..tokens.len()).any(|idx| !surviving.contains(&tokens.position(idx))) {
                    return Ok(Vec::new());
                }
            }
            expanded
        } else {
            tokens
        };

        // The wand only consults `scorer.doc_weight`, which is metadata-free.
        // The outer aggregation below consults `scorer.query_weight`, which
        // hits per-token `posting_len`; building a `MemBM25Scorer` with
        // precomputed per-term IDFs avoids the v2 bulk metadata pull.
        let local_scorer;
        let scorer: &MemBM25Scorer = if let Some(base_scorer) = base_scorer {
            base_scorer
        } else {
            local_scorer = self
                .bm25_scorer_for_final_tokens(tokens.as_ref(), Some(metrics.as_ref()))
                .await?;
            &local_scorer
        };
        let impact_scorer = Arc::new(scorer.clone());

        let limit = params.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mask = prefilter.mask();
        if self.is_legacy() {
            let (row_ids, scores) = self
                .bm25_search_legacy(
                    tokens,
                    params,
                    operator,
                    mask,
                    metrics,
                    scorer,
                    impact_scorer,
                    limit,
                )
                .await?;
            Ok(row_ids
                .into_iter()
                .zip(scores)
                .map(|(row_id, score)| ScoredDoc::new(row_id, score))
                .collect())
        } else {
            self.bm25_search_modern(ModernSearchRequest {
                tokens,
                params,
                operator,
                mask,
                metrics,
                scorer,
                impact_scorer,
                limit,
            })
            .await
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn bm25_search_legacy(
        &self,
        tokens: Arc<Tokens>,
        params: Arc<FtsSearchParams>,
        operator: Operator,
        mask: Arc<RowAddrMask>,
        metrics: Arc<dyn MetricsCollector>,
        scorer: &MemBM25Scorer,
        impact_scorer: Arc<MemBM25Scorer>,
        limit: usize,
    ) -> Result<(Vec<u64>, Vec<f32>)> {
        let impact_shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));
        let io_parallelism = self.store.io_parallelism();
        let parts = self
            .partitions
            .chunks(fts_search_chunk())
            .map(|chunk| {
                let chunk = chunk.to_vec();
                let tokens = tokens.clone();
                let params = params.clone();
                let mask = mask.clone();
                let metrics = metrics.clone();
                let impact_scorer = impact_scorer.clone();
                let impact_shared_threshold = impact_shared_threshold.clone();
                async move {
                    let loads = chunk.into_iter().map(|part| {
                        let tokens = tokens.clone();
                        let params = params.clone();
                        let metrics = metrics.clone();
                        let impact_scorer = impact_scorer.clone();
                        let impact_shared_threshold = impact_shared_threshold.clone();
                        async move {
                            let LoadedPostings {
                                postings,
                                grouped_expansions,
                                impact_safe,
                                exact_scoring_required,
                            } = part
                                .load_posting_lists(
                                    tokens.as_ref(),
                                    params.as_ref(),
                                    operator,
                                    impact_scorer.as_ref(),
                                    metrics.as_ref(),
                                    false,
                                )
                                .await?;
                            if postings.is_empty() {
                                return Result::Ok(None);
                            }
                            let max_position = postings
                                .iter()
                                .map(|posting| posting.term_index() as usize)
                                .max()
                                .unwrap_or_default();
                            let mut tokens_by_position = vec![String::new(); max_position + 1];
                            for posting in &postings {
                                tokens_by_position[posting.term_index() as usize] =
                                    posting.token().to_owned();
                            }
                            let docs = part.docs.legacy().cloned().ok_or_else(|| {
                                Error::internal("legacy index contains modern partition documents")
                            })?;
                            let use_global_scorer = impact_safe || exact_scoring_required;
                            let threshold = if use_global_scorer {
                                impact_shared_threshold
                            } else {
                                Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()))
                            };
                            let wand_scorer = use_global_scorer.then(|| impact_scorer.clone());
                            Result::Ok(Some((
                                part,
                                docs,
                                postings,
                                wand_scorer,
                                threshold,
                                tokens_by_position,
                                grouped_expansions,
                            )))
                        }
                    });
                    let loaded = stream::iter(loads)
                        .buffer_unordered(io_parallelism)
                        .try_collect::<Vec<_>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    if loaded.is_empty() {
                        return Result::Ok(Vec::new());
                    }

                    let results = spawn_cpu(move || {
                        let mut results = Vec::with_capacity(loaded.len());
                        for (
                            part,
                            docs,
                            postings,
                            wand_scorer,
                            threshold,
                            tokens_by_position,
                            grouped_expansions,
                        ) in loaded
                        {
                            let candidates = part.bm25_search_legacy(
                                docs.as_ref(),
                                params.as_ref(),
                                operator,
                                mask.as_ref(),
                                postings,
                                wand_scorer,
                                metrics.as_ref(),
                                threshold,
                            )?;
                            results.push(PartitionCandidates {
                                tokens_by_position,
                                grouped_expansions,
                                candidates,
                            });
                        }
                        Result::Ok(results)
                    })
                    .await?;
                    Result::Ok(results)
                }
            })
            .collect::<Vec<_>>();

        let mut ranked = BinaryHeap::new();
        let mut idf_cache = HashMap::new();
        let mut parts = stream::iter(parts)
            .buffer_unordered(get_num_compute_intensive_cpus().min(32))
            .map_ok(|results| stream::iter(results.into_iter().map(Result::Ok)))
            .try_flatten();
        while let Some(partition) = parts.try_next().await? {
            for (row_id, score) in rescore_partition_candidates(partition, scorer, &mut idf_cache) {
                push_scored_key(&mut ranked, limit, row_id, score);
            }
        }
        Ok(ranked
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(doc)| (doc.row_id, doc.score.0))
            .unzip())
    }

    async fn bm25_search_modern(&self, request: ModernSearchRequest<'_>) -> Result<Vec<ScoredDoc>> {
        // Select a concrete completion path before candidate search.  The
        // fully resident future never builds deferred address-read state, while
        // a cold query keeps DocIds until its final bounded I/O phase.
        if self.has_resident_document_projections() {
            self.bm25_search_modern_resident(request).await
        } else {
            self.bm25_search_modern_deferred(request).await
        }
    }

    fn has_resident_document_projections(&self) -> bool {
        if self.document_projections_resident.load(Ordering::Acquire) {
            return true;
        }
        let resident = self.document_projections_resident_now();
        if resident {
            self.document_projections_resident
                .store(true, Ordering::Release);
        }
        resident
    }

    fn document_projections_resident_now(&self) -> bool {
        self.partitions.iter().all(|partition| {
            partition
                .docs
                .modern()
                .is_some_and(|documents| documents.projection_resident())
        })
    }

    async fn bm25_search_modern_resident(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<ScoredDoc>> {
        let ranked = self.bm25_search_modern_candidates(request).await?;
        if let Some(result) = self.resolve_resident_modern_candidates(&ranked)? {
            return Ok(result);
        }
        self.document_projections_resident
            .store(false, Ordering::Release);
        self.resolve_deferred_modern_candidates(ranked).await
    }

    async fn bm25_search_modern_deferred(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<ScoredDoc>> {
        // Old partitioned files without persisted stats populate their
        // fallback stats before deferred candidate orchestration.  A resident
        // search can skip this full-index synchronization: standard prewarm
        // has already initialized it, while any independently resident
        // partition loads its lengths before constructing a local scorer.
        if self.corpus_stats.get().is_none() {
            self.aggregate_corpus_stats().await?;
        }
        // For new-format indexes, aggregate_corpus_stats reads corpus stats from
        // persisted schema metadata (O(1)) without loading doc lengths as a side
        // effect. Pre-load lengths in parallel now for partitions that contain at
        // least one query token, so the scoring phase gets cache hits instead of
        // issuing sequential per-partition IO.  Partitions with no matching terms
        // are skipped to preserve the no-load optimization for no-hit queries.
        let io_parallelism = self.store.io_parallelism();
        let uncached_lengths = self
            .partitions
            .iter()
            .filter_map(|part| {
                let docs = part.docs.modern()?.clone();
                if docs.cached_lengths().is_some() {
                    return None;
                }
                let has_match = (0..request.tokens.len())
                    .any(|i| part.tokens.get(request.tokens.get_token(i)).is_some());
                has_match.then_some(async move { docs.lengths().await.map(|_| ()) })
            })
            .collect::<Vec<_>>();
        if !uncached_lengths.is_empty() {
            stream::iter(uncached_lengths)
                .buffer_unordered(io_parallelism)
                .try_collect::<Vec<_>>()
                .await?;
        }
        let ranked = self.bm25_search_modern_candidates(request).await?;
        self.resolve_deferred_modern_candidates(ranked).await
    }

    async fn bm25_search_modern_candidates(
        &self,
        request: ModernSearchRequest<'_>,
    ) -> Result<Vec<Reverse<ScoredPartitionDoc>>> {
        let ModernSearchRequest {
            tokens,
            params,
            operator,
            mask,
            metrics,
            scorer,
            impact_scorer,
            limit,
        } = request;
        if self.partitions.len() > u32::MAX as usize {
            return Err(Error::index(format!(
                "FTS partition count {} exceeds candidate identity capacity",
                self.partitions.len()
            )));
        }
        let impact_shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));
        let io_parallelism = self.store.io_parallelism();
        let parts = self
            .partitions
            .chunks(fts_search_chunk())
            .enumerate()
            .map(|(chunk_ordinal, chunk)| {
                let first_partition_ordinal = chunk_ordinal * fts_search_chunk();
                let chunk = chunk
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(offset, part)| (first_partition_ordinal + offset, part))
                    .collect::<Vec<_>>();
                let tokens = tokens.clone();
                let params = params.clone();
                let mask = mask.clone();
                let metrics = metrics.clone();
                let impact_scorer = impact_scorer.clone();
                let impact_shared_threshold = impact_shared_threshold.clone();
                async move {
                    let loads = chunk.into_iter().map(|(partition_ordinal, part)| {
                        let tokens = tokens.clone();
                        let params = params.clone();
                        let mask = mask.clone();
                        let metrics = metrics.clone();
                        let impact_scorer = impact_scorer.clone();
                        let impact_shared_threshold = impact_shared_threshold.clone();
                        async move {
                            let LoadedPostings {
                                postings,
                                grouped_expansions,
                                impact_safe,
                                exact_scoring_required,
                            } = part
                                .load_posting_lists(
                                    tokens.as_ref(),
                                    params.as_ref(),
                                    operator,
                                    impact_scorer.as_ref(),
                                    metrics.as_ref(),
                                    false,
                                )
                                .await?;
                            if postings.is_empty() {
                                return Result::Ok(None);
                            }
                            let documents = part.docs.modern().cloned().ok_or_else(|| {
                                Error::internal("modern index contains legacy partition documents")
                            })?;
                            let materialize_selected = operator == Operator::Or
                                && mask.max_len().is_some_and(|selected| {
                                    u128::from(selected).saturating_mul(100)
                                        <= u128::from(*FLAT_SEARCH_PERCENT_THRESHOLD)
                                            .saturating_mul(documents.len() as u128)
                                });
                            let visibility = match documents
                                .immediate_visibility(mask.clone(), materialize_selected)
                            {
                                Some(visibility) => visibility,
                                None => {
                                    documents
                                        .visibility(mask.clone(), materialize_selected)
                                        .await?
                                }
                            };
                            if visibility.is_empty() {
                                return Result::Ok(None);
                            }
                            let lengths = match documents.cached_lengths() {
                                Some(lengths) => lengths,
                                None => documents.lengths().await?,
                            };
                            let max_position = postings
                                .iter()
                                .map(|posting| posting.term_index() as usize)
                                .max()
                                .unwrap_or_default();
                            let mut tokens_by_position = vec![String::new(); max_position + 1];
                            for posting in &postings {
                                tokens_by_position[posting.term_index() as usize] =
                                    posting.token().to_owned();
                            }
                            let use_global_scorer = impact_safe || exact_scoring_required;
                            let threshold = if use_global_scorer {
                                impact_shared_threshold
                            } else {
                                Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()))
                            };
                            let wand_scorer = use_global_scorer.then(|| impact_scorer.clone());
                            Result::Ok(Some((
                                partition_ordinal,
                                part,
                                lengths,
                                visibility,
                                postings,
                                wand_scorer,
                                threshold,
                                tokens_by_position,
                                grouped_expansions,
                            )))
                        }
                    });
                    let loaded = stream::iter(loads)
                        .buffer_unordered(io_parallelism)
                        .try_collect::<Vec<_>>()
                        .await?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>();
                    if loaded.is_empty() {
                        return Result::Ok(Vec::new());
                    }

                    let results = spawn_cpu(move || {
                        let mut results = Vec::with_capacity(loaded.len());
                        for (
                            partition_ordinal,
                            part,
                            lengths,
                            visibility,
                            postings,
                            wand_scorer,
                            threshold,
                            tokens_by_position,
                            grouped_expansions,
                        ) in loaded
                        {
                            let candidates = part.bm25_search_modern(
                                lengths.as_ref(),
                                &visibility,
                                params.as_ref(),
                                operator,
                                postings,
                                wand_scorer,
                                metrics.as_ref(),
                                threshold,
                            )?;
                            results.push((
                                partition_ordinal,
                                PartitionCandidates {
                                    tokens_by_position,
                                    grouped_expansions,
                                    candidates,
                                },
                            ));
                        }
                        Result::Ok(results)
                    })
                    .await?;
                    Result::Ok(results)
                }
            })
            .collect::<Vec<_>>();

        let mut ranked = BinaryHeap::new();
        let mut idf_cache = HashMap::new();
        let mut parts = stream::iter(parts)
            .buffer_unordered(get_num_compute_intensive_cpus().min(32))
            .map_ok(|results| stream::iter(results.into_iter().map(Result::Ok)))
            .try_flatten();
        while let Some((partition_ordinal, partition)) = parts.try_next().await? {
            for (doc_id, score) in rescore_partition_candidates(partition, scorer, &mut idf_cache) {
                push_scored_partition_doc(
                    &mut ranked,
                    limit,
                    PartitionDocId::try_new(partition_ordinal, doc_id)?,
                    score,
                );
            }
        }

        Ok(ranked.into_sorted_vec())
    }

    fn resolve_resident_modern_candidates(
        &self,
        ranked: &[Reverse<ScoredPartitionDoc>],
    ) -> Result<Option<Vec<ScoredDoc>>> {
        if self.partitions.iter().any(|partition| {
            partition
                .docs
                .modern()
                .is_some_and(|documents| documents.coordinate_rank() > 0)
        }) {
            return Ok(None);
        }
        let mut resolved_documents = ranked
            .iter()
            .map(|Reverse(candidate)| ScoredDoc::new(0, candidate.score.0))
            .collect::<Vec<_>>();
        let mut by_partition = BTreeMap::<usize, Vec<(usize, DocId)>>::new();
        for (rank, Reverse(candidate)) in ranked.iter().enumerate() {
            let partition_ordinal = candidate.document.partition_ordinal();
            let doc_id = candidate.document.doc_id;
            by_partition
                .entry(partition_ordinal)
                .or_default()
                .push((rank, doc_id));
        }
        for (partition_ordinal, entries) in by_partition {
            let documents = self
                .partitions
                .get(partition_ordinal)
                .and_then(|partition| partition.docs.modern())
                .ok_or_else(|| {
                    Error::internal(format!(
                        "resident FTS candidates reference missing modern partition ordinal {partition_ordinal}"
                    ))
                })?;
            let doc_ids = entries
                .iter()
                .map(|(_, doc_id)| *doc_id)
                .collect::<Vec<_>>();
            let Some(resolved) = documents.cached_row_addresses(&doc_ids)? else {
                return Ok(None);
            };
            for ((rank, _), address) in entries.into_iter().zip(resolved) {
                resolved_documents[rank].row_id = address;
            }
        }
        Ok(Some(resolved_documents))
    }

    async fn resolve_deferred_modern_candidates(
        &self,
        ranked: Vec<Reverse<ScoredPartitionDoc>>,
    ) -> Result<Vec<ScoredDoc>> {
        let mut resolved_documents = ranked
            .iter()
            .map(|Reverse(candidate)| ScoredDoc::new(0, candidate.score.0))
            .collect::<Vec<_>>();
        let mut by_partition = BTreeMap::<usize, Vec<(usize, DocId)>>::new();
        for (rank, Reverse(candidate)) in ranked.iter().enumerate() {
            let partition_ordinal = candidate.document.partition_ordinal();
            let doc_id = candidate.document.doc_id;
            by_partition
                .entry(partition_ordinal)
                .or_default()
                .push((rank, doc_id));
        }
        let mut address_reads = Vec::with_capacity(by_partition.len());
        let mut largest_read_bytes = 0;
        for (partition_ordinal, entries) in by_partition {
            let documents = self
                .partitions
                .get(partition_ordinal)
                .and_then(|partition| partition.docs.modern())
                .cloned()
                .ok_or_else(|| {
                    Error::internal(format!(
                        "deferred FTS candidates reference missing modern partition ordinal {partition_ordinal}"
                    ))
                })?;
            let doc_ids = entries
                .iter()
                .map(|(_, doc_id)| *doc_id)
                .collect::<Vec<_>>();
            largest_read_bytes =
                largest_read_bytes.max(documents.estimated_address_read_bytes(&doc_ids));
            address_reads.push(async move {
                let resolved = documents.resolve_document_keys(&doc_ids).await?;
                Result::Ok((entries, resolved))
            });
        }
        let concurrency = address_read_concurrency(self.store.io_parallelism(), largest_read_bytes);
        let mut address_reads = stream::iter(address_reads).buffer_unordered(concurrency);
        while let Some((entries, resolved)) = address_reads.try_next().await? {
            for ((rank, _), (row_id, doc_index)) in entries.into_iter().zip(resolved) {
                resolved_documents[rank].row_id = row_id;
                resolved_documents[rank].doc_index = doc_index;
            }
        }
        Ok(resolved_documents)
    }

    async fn load_legacy_index(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        log::warn!("loading legacy FTS index");
        let tokens_fut = tokio::spawn({
            let store = store.clone();
            async move {
                let token_reader = store.open_index_file(TOKENS_FILE).await?;
                let tokenizer = token_reader
                    .schema()
                    .metadata
                    .get("tokenizer")
                    .map(|s| serde_json::from_str::<InvertedIndexParams>(s))
                    .transpose()?
                    .unwrap_or_default();
                let tokens = TokenSet::load(token_reader, TokenSetFormat::Arrow).await?;
                Result::Ok((tokenizer, tokens))
            }
        });
        let invert_list_fut = tokio::spawn({
            let store = store.clone();
            let index_cache_clone = index_cache.clone();
            async move {
                let invert_list_reader = store.open_index_file(INVERT_LIST_FILE).await?;
                let invert_list =
                    PostingListReader::try_new(invert_list_reader, &index_cache_clone).await?;
                Result::Ok(Arc::new(invert_list))
            }
        });
        let docs_fut = tokio::spawn({
            let store = store.clone();
            async move {
                let docs_reader = store.open_index_file(DOCS_FILE).await?;
                let docs = DocSet::load(docs_reader, true, frag_reuse_index).await?;
                Result::Ok(docs)
            }
        });

        let (tokenizer_config, tokens) = tokens_fut.await??;
        let inverted_list = invert_list_fut.await??;
        let docs = docs_fut.await??;

        let tokenizer = tokenizer_config.build()?;

        Ok(Arc::new(Self {
            params: tokenizer_config,
            store: store.clone(),
            tokenizer,
            token_set_format: TokenSetFormat::Arrow,
            format_version: InvertedListFormatVersion::V1,
            partitions: vec![Arc::new(InvertedPartition {
                id: 0,
                store,
                tokens,
                inverted_list,
                docs: PartitionDocumentStore::Legacy(Arc::new(docs)),
                token_set_format: TokenSetFormat::Arrow,
            })],
            corpus_stats: Arc::new(OnceCell::new()),
            prewarm_state: Arc::new(Mutex::new(InvertedPrewarmState::default())),
            document_projections_resident: Arc::new(AtomicBool::new(false)),
            deleted_fragments: RoaringBitmap::new(),
        }))
    }

    pub fn is_legacy(&self) -> bool {
        self.partitions.len() == 1 && self.partitions[0].docs.legacy().is_some()
    }

    /// Read only the index's [`InvertedIndexParams`],
    /// Contains more complete info than manifest's lossy `InvertedIndexDetails`.
    pub async fn load_params(store: &dyn IndexStore) -> Result<InvertedIndexParams> {
        match store.open_index_file(METADATA_FILE).await {
            Ok(reader) => {
                let params = reader
                    .schema()
                    .metadata
                    .get("params")
                    .ok_or(Error::index("params not found in metadata".to_owned()))?;
                Ok(serde_json::from_str::<InvertedIndexParams>(params)?)
            }
            Err(metadata_error) => {
                // Legacy format: params live in the tokens file (see
                // `load_legacy_index`). Some S3 configurations return 403 for
                // a missing object, so the readable legacy file is the
                // authoritative format probe.
                let Ok(reader) = store.open_index_file(TOKENS_FILE).await else {
                    return Err(metadata_error);
                };
                Ok(reader
                    .schema()
                    .metadata
                    .get("tokenizer")
                    .map(|s| serde_json::from_str::<InvertedIndexParams>(s))
                    .transpose()?
                    .unwrap_or_default())
            }
        }
    }

    pub async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>>
    where
        Self: Sized,
    {
        // for new index format, there is a metadata file and multiple partitions,
        // each partition is a separate index containing tokens, inverted list and docs.
        // for old index format, there is no metadata file, and it's just like a single partition

        match store.open_index_file(METADATA_FILE).await {
            Ok(reader) => {
                let params = reader
                    .schema()
                    .metadata
                    .get("params")
                    .ok_or(Error::index("params not found in metadata".to_owned()))?;
                let mut params = serde_json::from_str::<InvertedIndexParams>(params)?;
                let partitions = reader
                    .schema()
                    .metadata
                    .get("partitions")
                    .ok_or(Error::index("partitions not found in metadata".to_owned()))?;
                let partitions: Vec<u64> = serde_json::from_str(partitions)?;
                let token_set_format = reader
                    .schema()
                    .metadata
                    .get(TOKEN_SET_FORMAT_KEY)
                    .map(|name| TokenSetFormat::from_str(name))
                    .transpose()?
                    .unwrap_or(TokenSetFormat::Arrow);
                let format_version = parse_format_version_from_metadata(&reader.schema().metadata)?;

                // Load deleted_fragments if present (optional for backward compatibility)
                let deleted_fragments = if reader.num_rows() > 0 {
                    let metadata_batch = reader.read_range(0..1, None).await?;
                    if let Some(col) = metadata_batch.column_by_name(DELETED_FRAGMENTS_COL) {
                        let arr = col.as_binary_opt::<i32>().expect_ok()?;
                        RoaringBitmap::deserialize_from(arr.value(0))?
                    } else {
                        RoaringBitmap::new()
                    }
                } else {
                    RoaringBitmap::new()
                };

                let format = token_set_format;
                let partitions = partitions.into_iter().enumerate().map(|(priority, id)| {
                    let store = store.with_io_priority(priority as u64);
                    let frag_reuse_index_clone = frag_reuse_index.clone();
                    let index_cache_for_part =
                        index_cache.with_key_prefix(format!("part-{}", id).as_str());
                    let token_set_format = format;
                    async move {
                        Result::Ok(Arc::new(
                            InvertedPartition::load(
                                store,
                                id,
                                frag_reuse_index_clone,
                                &index_cache_for_part,
                                token_set_format,
                            )
                            .await?,
                        ))
                    }
                });
                let partitions = stream::iter(partitions)
                    .buffer_unordered(store.io_parallelism())
                    .try_collect::<Vec<_>>()
                    .await?;

                let coordinate_rank = partitions
                    .first()
                    .map(|partition| partition.docs.coordinate_rank())
                    .unwrap_or(0);
                if partitions
                    .iter()
                    .any(|partition| partition.docs.coordinate_rank() != coordinate_rank)
                {
                    return Err(Error::index(
                        "FTS partitions have inconsistent document coordinate ranks".to_string(),
                    ));
                }
                params.document_granularity = if coordinate_rank == 0 {
                    DocumentGranularity::Row
                } else {
                    DocumentGranularity::ListElement
                };

                let tokenizer = params.build()?;
                Ok(Arc::new(Self {
                    params,
                    store,
                    tokenizer,
                    token_set_format,
                    format_version,
                    partitions,
                    corpus_stats: Arc::new(OnceCell::new()),
                    prewarm_state: Arc::new(Mutex::new(InvertedPrewarmState::default())),
                    document_projections_resident: Arc::new(AtomicBool::new(false)),
                    deleted_fragments,
                }))
            }
            Err(_) => {
                // old index format
                Self::load_legacy_index(store, frag_reuse_index, index_cache).await
            }
        }
    }
}

#[async_trait]
impl Index for InvertedIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let num_tokens = self
            .partitions
            .iter()
            .map(|part| part.tokens.len())
            .sum::<usize>();
        let num_docs = self
            .partitions
            .iter()
            .map(|part| part.docs.len())
            .sum::<usize>();
        Ok(serde_json::json!({
            "params": self.params,
            "num_tokens": num_tokens,
            "num_docs": num_docs,
        }))
    }

    async fn prewarm(&self) -> Result<()> {
        self.prewarm_with_options(&FtsPrewarmOptions::default())
            .await
    }

    fn index_type(&self) -> crate::IndexType {
        crate::IndexType::Inverted
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

/// Target on-disk size of one prewarm chunk. Keep this large enough that cloud
/// stores do not spend prewarm time on thousands of tiny range reads, but still
/// bounded so one large partition is not materialized all at once.
const PREWARM_CHUNK_TARGET_BYTES: u64 = 128 << 20;

/// Cap on token rows per chunk, bounding the built `Vec` when posting lists are tiny.
const PREWARM_MAX_CHUNK_TOKENS: usize = 256 * 1024;

/// Floor on token rows per chunk, so a partition always makes progress.
const PREWARM_MIN_CHUNK_TOKENS: usize = 1;

/// Maximum number of posting lists in a runtime synthetic cache group. This is
/// deliberately token-count based so grouping works for old v2 indexes without
/// scanning posting lengths or requiring index rebuilds.
static LANCE_FTS_POSTING_GROUP_MAX_TOKENS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LANCE_FTS_POSTING_GROUP_MAX_TOKENS")
        .unwrap_or_else(|_| "128".to_string())
        .parse()
        .expect("failed to parse LANCE_FTS_POSTING_GROUP_MAX_TOKENS")
});

fn runtime_posting_group_tokens() -> usize {
    (*LANCE_FTS_POSTING_GROUP_MAX_TOKENS).max(1)
}

/// Runtime posting-list cache grouping. Non-empty v2 indexes synthesize fixed
/// groups at read time so prewarm and queries share group cache entries without
/// persisted grouping metadata or index rebuilds.
#[derive(Debug, Clone, DeepSizeOf)]
enum PostingGrouping {
    /// Leaves legacy or empty partitions ungrouped.
    None,
    /// Uses a fixed runtime cache group size measured in token rows, not posting bytes.
    SyntheticFixed { group_size: u32 },
}

impl PostingGrouping {
    fn for_reader(is_legacy_layout: bool, token_count: usize) -> Self {
        if is_legacy_layout || token_count == 0 {
            return Self::None;
        }

        let group_size = u32::try_from(runtime_posting_group_tokens())
            .unwrap_or(u32::MAX)
            .max(1);
        Self::SyntheticFixed { group_size }
    }

    fn is_grouped(&self) -> bool {
        !matches!(self, Self::None)
    }

    fn range_for_token(&self, token_id: u32, token_count: usize) -> Option<(u32, u32)> {
        match self {
            Self::None => None,
            Self::SyntheticFixed { group_size } => {
                let token_count = u32::try_from(token_count).unwrap_or(u32::MAX);
                let start = (token_id / *group_size) * *group_size;
                let end = start.saturating_add(*group_size).min(token_count);
                Some((start, end))
            }
        }
    }

    fn aligned_chunk_end(&self, token_count: usize, tok_start: usize, desired_end: usize) -> usize {
        match self {
            Self::None => desired_end,
            Self::SyntheticFixed { group_size } => synthetic_group_aligned_chunk_end(
                usize::try_from(*group_size).unwrap_or(usize::MAX).max(1),
                token_count,
                tok_start,
                desired_end,
            ),
        }
    }

    fn ranges_for_chunk(
        &self,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
    ) -> Vec<(u32, u32)> {
        match self {
            Self::None => Vec::new(),
            Self::SyntheticFixed { group_size } => synthetic_group_ranges_for_chunk(
                usize::try_from(*group_size).unwrap_or(usize::MAX).max(1),
                tok_start,
                tok_end,
                token_count,
            ),
        }
    }
}

/// Token rows per chunk: byte target / average bytes-per-token, clamped to `[MIN, MAX]`.
fn prewarm_chunk_tokens(token_count: usize, file_size_bytes: u64) -> usize {
    if token_count == 0 {
        return PREWARM_MIN_CHUNK_TOKENS;
    }
    let bytes_per_token = (file_size_bytes / token_count as u64).max(1); // >= 1: no div-by-zero
    let by_bytes = (PREWARM_CHUNK_TARGET_BYTES / bytes_per_token) as usize;
    by_bytes.clamp(PREWARM_MIN_CHUNK_TOKENS, PREWARM_MAX_CHUNK_TOKENS)
}

fn synthetic_group_aligned_chunk_end(
    group_size: usize,
    token_count: usize,
    tok_start: usize,
    desired_end: usize,
) -> usize {
    if desired_end >= token_count {
        return token_count;
    }

    let boundary = desired_end - (desired_end % group_size);
    if boundary > tok_start {
        boundary
    } else {
        tok_start.saturating_add(group_size).min(token_count)
    }
}

fn synthetic_group_ranges_for_chunk(
    group_size: usize,
    tok_start: usize,
    tok_end: usize,
    token_count: usize,
) -> Vec<(u32, u32)> {
    let mut ranges = Vec::new();
    let mut start = tok_start - (tok_start % group_size);
    if start < tok_start {
        start = start.saturating_add(group_size).min(token_count);
    }
    while start < tok_end {
        let end = start.saturating_add(group_size).min(token_count);
        ranges.push((
            u32::try_from(start).unwrap_or(u32::MAX),
            u32::try_from(end).unwrap_or(u32::MAX),
        ));
        start = end;
    }
    ranges
}

fn prewarm_chunk_ranges(
    grouping: &PostingGrouping,
    token_count: usize,
    chunk_tokens: usize,
) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut tok_start = 0usize;
    while tok_start < token_count {
        let mut tok_end = (tok_start + chunk_tokens).min(token_count);
        // `tok_start` is always a group boundary; snap `tok_end` back to one too.
        if grouping.is_grouped() {
            tok_end = grouping.aligned_chunk_end(token_count, tok_start, tok_end);
        }
        ranges.push((tok_start, tok_end));
        tok_start = tok_end;
    }
    ranges
}

impl InvertedIndex {
    pub async fn prewarm_with_options(&self, options: &FtsPrewarmOptions) -> Result<()> {
        let mut state = self.prewarm_state.lock().await;
        if state.satisfies(options.with_position)
            && (self.is_legacy() || self.document_projections_resident_now())
        {
            return Ok(());
        }
        let with_position = options.with_position || state.positions_ready;
        state.query_ready = false;
        state.positions_ready = false;
        self.document_projections_resident
            .store(false, Ordering::Release);
        self.prewarm_query_state(with_position).await?;
        state.query_ready = true;
        state.positions_ready = with_position;
        Ok(())
    }

    async fn prewarm_query_state(&self, with_position: bool) -> Result<()> {
        let chunk_concurrency = self.store.io_parallelism().max(1);
        let prewarm_started = Instant::now();
        info!(
            partition_count = self.partitions.len(),
            with_position, chunk_concurrency, "fts index prewarm started"
        );
        for part in &self.partitions {
            let partition_started = Instant::now();
            info!(
                partition_id = part.id(),
                token_count = part.tokens.len(),
                with_position,
                chunk_concurrency,
                "fts partition prewarm started"
            );
            if let Err(err) = part
                .inverted_list
                .prewarm_posting_lists(with_position, chunk_concurrency)
                .await
            {
                warn!(
                    partition_id = part.id(),
                    error = %err,
                    elapsed_ms = partition_started.elapsed().as_millis() as u64,
                    "fts partition posting list prewarm failed"
                );
                return Err(err);
            }
            info!(
                partition_id = part.id(),
                elapsed_ms = partition_started.elapsed().as_millis() as u64,
                "fts partition posting lists prewarmed"
            );
            let docs_started = Instant::now();
            if let Err(err) = part.docs.prewarm().await {
                warn!(
                    partition_id = part.id(),
                    error = %err,
                    elapsed_ms = docs_started.elapsed().as_millis() as u64,
                    total_elapsed_ms = partition_started.elapsed().as_millis() as u64,
                    "fts partition docset prewarm failed"
                );
                return Err(err);
            }
            info!(
                partition_id = part.id(),
                docset_elapsed_ms = docs_started.elapsed().as_millis() as u64,
                elapsed_ms = partition_started.elapsed().as_millis() as u64,
                "fts partition prewarm finished"
            );
        }
        self.aggregate_corpus_stats().await?;
        let query_ready = self.partitions.iter().all(|partition| {
            partition.docs.query_ready()
                && partition.inverted_list.modern_posting_validation_ready()
        });
        if !query_ready {
            return Err(Error::internal(
                "FTS prewarm completed without publishing a query-ready document and posting state"
                    .to_owned(),
            ));
        }
        self.document_projections_resident
            .store(true, Ordering::Release);
        info!(
            partition_count = self.partitions.len(),
            query_ready,
            elapsed_ms = prewarm_started.elapsed().as_millis() as u64,
            "fts index prewarm finished"
        );
        Ok(())
    }
    /// Search docs match the input text.
    async fn do_search(&self, text: &str) -> Result<RecordBatch> {
        let params = FtsSearchParams::new();
        let mut tokenizer = self.tokenizer.clone();
        let tokens = collect_query_tokens(text, &mut tokenizer);

        let (doc_ids, _) = self
            .bm25_search(
                Arc::new(tokens),
                params.into(),
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .boxed()
            .await?;

        Ok(RecordBatch::try_new(
            ROW_ID_SCHEMA.clone(),
            vec![Arc::new(UInt64Array::from(doc_ids))],
        )?)
    }
}

#[async_trait]
impl ScalarIndex for InvertedIndex {
    // return the row ids of the documents that contain the query
    #[instrument(level = "debug", skip_all)]
    async fn search(
        &self,
        query: &dyn AnyQuery,
        _metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<TokenQuery>().unwrap();

        match query {
            TokenQuery::TokensContains(text) => {
                let records = self.do_search(text).await?;
                let row_ids = records
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let row_ids = row_ids.iter().flatten().collect_vec();
                Ok(SearchResult::at_most(RowAddrTreeMap::from_iter(row_ids)))
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
        let files = self
            .to_builder()
            .remap(mapping, self.store.clone(), dest_store)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&self.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: self.index_version(),
            files,
        })
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<crate::scalar::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let files = self
            .to_builder()
            .update(new_data, dest_store, old_data_filter)
            .await?;

        let details = pbold::InvertedIndexDetails::try_from(&self.params)?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&details).unwrap(),
            index_version: self.index_version(),
            files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        let criteria = TrainingCriteria::new(TrainingOrdering::None).with_row_id();
        if self.is_legacy() {
            UpdateCriteria::requires_old_data(criteria)
        } else {
            UpdateCriteria::only_new_data(criteria)
        }
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        let mut params = self.params.clone();
        if params.base_tokenizer.is_empty() {
            // Empty tokenizer metadata only appears in legacy simple-tokenizer indexes.
            params.base_tokenizer = "simple".to_string();
        }
        params = params.format_version(self.format_version());

        let params_json = params.to_training_json()?.to_string();

        Ok(ScalarIndexParams {
            index_type: BuiltinIndexType::Inverted.as_str().to_string(),
            params: Some(params_json),
        })
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub struct InvertedPartition {
    // 0 for legacy format
    id: u64,
    store: Arc<dyn IndexStore>,
    pub(crate) tokens: TokenSet,
    pub(crate) inverted_list: Arc<PostingListReader>,
    /// Legacy documents stay in their original complete `DocSet`; modern
    /// documents use typed, independently-loaded lengths and addresses.
    pub(super) docs: PartitionDocumentStore,
    token_set_format: TokenSetFormat,
}

impl InvertedPartition {
    /// Check if this partition belongs to the specified fragment.
    ///
    /// This method encapsulates the bit manipulation logic for fragment filtering
    /// in distributed indexing scenarios.
    ///
    /// # Arguments
    /// * `fragment_mask` - A mask with fragment_id in high 32 bits
    ///
    /// # Returns
    /// * `true` if the partition belongs to the fragment, `false` otherwise
    pub fn belongs_to_fragment(&self, fragment_mask: u64) -> bool {
        (self.id() & fragment_mask) == fragment_mask
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn store(&self) -> &dyn IndexStore {
        self.store.as_ref()
    }

    pub fn is_legacy(&self) -> bool {
        self.inverted_list.is_legacy_layout()
    }

    pub async fn load(
        store: Arc<dyn IndexStore>,
        id: u64,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
        token_set_format: TokenSetFormat,
    ) -> Result<Self> {
        let token_file = store.open_index_file(&token_file_path(id)).await?;
        let tokens = TokenSet::load(token_file, token_set_format).await?;
        let invert_list_file = store.open_index_file(&posting_file_path(id)).await?;
        let mut inverted_list = PostingListReader::try_new(invert_list_file, index_cache).await?;
        let docs_path = doc_file_path(id);
        let docs_reader = store.open_index_file(&docs_path).await?;
        let docs = PartitionDocuments::try_new(
            store.clone(),
            docs_path,
            id,
            WeakLanceCache::from(index_cache),
            docs_reader.as_ref(),
            frag_reuse_index,
            // 256-document blocks score with quantized document lengths.
            inverted_list.block_size() == MAX_POSTING_BLOCK_SIZE,
        )?;
        inverted_list.modern_num_docs = Some(docs.len());

        Ok(Self {
            id,
            store,
            tokens,
            inverted_list: Arc::new(inverted_list),
            docs: PartitionDocumentStore::Modern(Arc::new(docs)),
            token_set_format,
        })
    }

    fn map(&self, token: &str) -> Option<u32> {
        self.tokens.get(token)
    }

    pub fn expand_fuzzy(&self, tokens: &Tokens, params: &FtsSearchParams) -> Result<Tokens> {
        let mut new_tokens = Vec::with_capacity(min(tokens.len(), params.max_expansions));
        let mut new_positions = Vec::with_capacity(new_tokens.capacity());
        let mut seen = HashSet::new();
        for token_idx in 0..tokens.len() {
            let remaining = params.max_expansions.saturating_sub(new_tokens.len());
            if remaining == 0 {
                break;
            }
            let token = tokens.get_token(token_idx);
            let position = tokens.position(token_idx);
            let base_prefix_len = tokens.token_type().prefix_len(token) as u32;
            let mut candidates = BTreeSet::new();
            self.collect_fuzzy_candidates(
                token,
                base_prefix_len,
                params,
                remaining,
                &mut candidates,
            )?;
            for candidate in candidates {
                if new_tokens.len() >= params.max_expansions {
                    break;
                }
                if seen.insert((candidate.clone(), position)) {
                    new_tokens.push(candidate);
                    new_positions.push(position);
                }
            }
        }
        Ok(Tokens::with_positions(
            new_tokens,
            new_positions,
            tokens.token_type().clone(),
        ))
    }

    /// Collect up to `limit` fuzzy candidates for one query token from this
    /// partition's token FST, in key (lexicographic) order. Callers merge
    /// candidates across partitions and apply the query-wide
    /// `max_expansions` budget; truncating each partition at `limit` is
    /// lossless for that selection because any term among the merged
    /// lexicographically-smallest `limit` is also among its own partition's
    /// smallest `limit`.
    fn collect_fuzzy_candidates(
        &self,
        token: &str,
        base_prefix_len: u32,
        params: &FtsSearchParams,
        limit: usize,
        candidates: &mut BTreeSet<String>,
    ) -> Result<()> {
        let fuzziness = match params.fuzziness {
            Some(fuzziness) => fuzziness,
            None => MatchQuery::auto_fuzziness(token),
        };
        let lev = fst::automaton::Levenshtein::new(token, fuzziness)
            .map_err(|e| Error::index(format!("failed to construct the fuzzy query: {}", e)))?;

        if let TokenMap::Fst(ref map) = self.tokens.tokens {
            let mut expanded = Vec::new();
            match base_prefix_len + params.prefix_length {
                0 => take_fst_keys(map.search(lev), &mut expanded, limit),
                prefix_length => {
                    let prefix = &token[..min(prefix_length as usize, token.len())];
                    let prefix = fst::automaton::Str::new(prefix).starts_with();
                    take_fst_keys(map.search(lev.intersection(prefix)), &mut expanded, limit)
                }
            }
            candidates.extend(expanded);
            Ok(())
        } else {
            Err(Error::index(
                "tokens is not fst, which is not expected".to_owned(),
            ))
        }
    }

    #[inline]
    fn grouped_score_upper_bound(
        query_weight: f32,
        union_freq: u32,
        doc_length: u32,
        scorer: &MemBM25Scorer,
    ) -> f32 {
        // BM25's document weight is monotonic in frequency and every IDF is
        // non-negative. Scoring the summed frequency with the summed IDF is
        // therefore an upper bound on the sum of the individual term scores.
        query_weight * scorer.doc_weight(union_freq, doc_length)
    }

    fn grouped_block_max_scores(
        doc_ids: &[u32],
        frequencies: &[u32],
        block_size: usize,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Vec<f32> {
        doc_ids
            .chunks(block_size)
            .zip(frequencies.chunks(block_size))
            .map(|(doc_ids, frequencies)| {
                doc_ids
                    .iter()
                    .zip(frequencies)
                    .map(|(doc_id, freq)| {
                        Self::grouped_score_upper_bound(
                            query_weight,
                            *freq,
                            docs.scoring_num_tokens(*doc_id),
                            scorer,
                        )
                    })
                    .fold(0.0, f32::max)
            })
            .collect()
    }

    fn union_plain_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let mut freqs_by_row_id = BTreeMap::new();
        for posting in postings {
            for (row_id, freq, _) in posting.iter() {
                let entry = freqs_by_row_id.entry(row_id).or_insert(0u32);
                *entry = entry.checked_add(freq).ok_or_else(|| {
                    Error::index(format!("posting frequency overflow for row id {}", row_id))
                })?;
            }
        }
        let mut row_ids = Vec::with_capacity(freqs_by_row_id.len());
        let mut frequencies = Vec::with_capacity(freqs_by_row_id.len());
        let mut max_score = 0.0_f32;
        for (row_id, freq) in freqs_by_row_id {
            max_score = max_score.max(Self::grouped_score_upper_bound(
                query_weight,
                freq,
                docs.num_tokens_by_row_id(row_id),
                scorer,
            ));
            row_ids.push(row_id);
            frequencies.push(freq as f32);
        }
        Ok(PostingList::Plain(PlainPostingList::new(
            ScalarBuffer::from(row_ids),
            ScalarBuffer::from(frequencies),
            Some(max_score),
            None,
        )))
    }

    fn union_plain_posting_lists_with_positions(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let mut positions_by_row_id = BTreeMap::<u64, Vec<u32>>::new();
        for posting in postings {
            for (row_id, _, positions) in posting.iter() {
                let positions = positions.ok_or_else(|| {
                    Error::index("cannot union grouped phrase terms without positions".to_string())
                })?;
                positions_by_row_id
                    .entry(row_id)
                    .or_default()
                    .extend(positions);
            }
        }
        if positions_by_row_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut row_ids = Vec::with_capacity(positions_by_row_id.len());
        let mut frequencies = Vec::with_capacity(positions_by_row_id.len());
        let mut positions_builder = ListBuilder::new(Int32Builder::new());
        let mut max_score = 0.0_f32;
        for (row_id, mut positions) in positions_by_row_id {
            positions.sort_unstable();
            let frequency = positions.len() as u32;
            max_score = max_score.max(Self::grouped_score_upper_bound(
                query_weight,
                frequency,
                docs.num_tokens_by_row_id(row_id),
                scorer,
            ));
            row_ids.push(row_id);
            frequencies.push(frequency as f32);
            for position in positions {
                positions_builder.values().append_value(position as i32);
            }
            positions_builder.append(true);
        }

        Ok(PostingList::Plain(PlainPostingList::new(
            ScalarBuffer::from(row_ids),
            ScalarBuffer::from(frequencies),
            Some(max_score),
            Some(positions_builder.finish()),
        )))
    }

    fn union_compressed_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let block_size = postings
            .iter()
            .find_map(|posting| match posting {
                PostingList::Compressed(posting) => Some(posting.block_size),
                PostingList::Plain(_) => None,
            })
            .unwrap_or(LEGACY_BLOCK_SIZE);
        let mut freqs_by_doc_id = BTreeMap::new();
        for posting in postings {
            for (doc_id, freq, _) in posting.iter() {
                let doc_id = u32::try_from(doc_id).map_err(|_| {
                    Error::index(format!(
                        "compressed posting doc id {} exceeds u32::MAX",
                        doc_id
                    ))
                })?;
                let entry = freqs_by_doc_id.entry(doc_id).or_insert(0u32);
                *entry = entry.checked_add(freq).ok_or_else(|| {
                    Error::index(format!("posting frequency overflow for doc id {}", doc_id))
                })?;
            }
        }
        if freqs_by_doc_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut builder = PostingListBuilder::new_with_block_size(false, block_size);
        let mut doc_ids = Vec::with_capacity(freqs_by_doc_id.len());
        let mut frequencies = Vec::with_capacity(freqs_by_doc_id.len());
        for (doc_id, freq) in freqs_by_doc_id {
            builder.add(doc_id, PositionRecorder::Count(freq));
            doc_ids.push(doc_id);
            frequencies.push(freq);
        }
        let block_max_scores = Self::grouped_block_max_scores(
            &doc_ids,
            &frequencies,
            block_size,
            docs,
            query_weight,
            scorer,
        );
        let batch = builder.to_batch(block_max_scores)?;
        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        PostingList::from_batch(&batch, Some(max_score), Some(length))
    }

    fn union_compressed_posting_lists_with_positions(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let block_size = postings
            .iter()
            .find_map(|posting| match posting {
                PostingList::Compressed(posting) => Some(posting.block_size),
                PostingList::Plain(_) => None,
            })
            .unwrap_or(LEGACY_BLOCK_SIZE);
        let mut positions_by_doc_id = BTreeMap::<u32, Vec<u32>>::new();
        for posting in postings {
            for (doc_id, _, positions) in posting.iter() {
                let doc_id = u32::try_from(doc_id).map_err(|_| {
                    Error::index(format!(
                        "compressed posting doc id {} exceeds u32::MAX",
                        doc_id
                    ))
                })?;
                let positions = positions.ok_or_else(|| {
                    Error::index("cannot union grouped phrase terms without positions".to_string())
                })?;
                positions_by_doc_id
                    .entry(doc_id)
                    .or_default()
                    .extend(positions);
            }
        }
        if positions_by_doc_id.is_empty() {
            return Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            )));
        }

        let mut builder = PostingListBuilder::new_with_block_size(true, block_size);
        let mut doc_ids = Vec::with_capacity(positions_by_doc_id.len());
        let mut frequencies = Vec::with_capacity(positions_by_doc_id.len());
        for (doc_id, mut positions) in positions_by_doc_id {
            positions.sort_unstable();
            let frequency = positions.len() as u32;
            builder.add(doc_id, PositionRecorder::Position(positions.into()));
            doc_ids.push(doc_id);
            frequencies.push(frequency);
        }
        let block_max_scores = Self::grouped_block_max_scores(
            &doc_ids,
            &frequencies,
            block_size,
            docs,
            query_weight,
            scorer,
        );
        let batch = builder.to_batch(block_max_scores)?;
        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        PostingList::from_batch(&batch, Some(max_score), Some(length))
    }

    fn union_posting_lists(
        postings: Vec<PostingList>,
        docs: &LoadedDocLengths,
        with_positions: bool,
        query_weight: f32,
        scorer: &MemBM25Scorer,
    ) -> Result<PostingList> {
        let has_plain = postings
            .iter()
            .any(|posting| matches!(posting, PostingList::Plain(_)));
        let has_compressed = postings
            .iter()
            .any(|posting| matches!(posting, PostingList::Compressed(_)));
        match (has_plain, has_compressed) {
            (true, true) => Err(Error::index(
                "cannot union mixed plain and compressed posting lists".to_owned(),
            )),
            (true, false) if with_positions => {
                Self::union_plain_posting_lists_with_positions(postings, docs, query_weight, scorer)
            }
            (true, false) => Self::union_plain_posting_lists(postings, docs, query_weight, scorer),
            (false, true) if with_positions => Self::union_compressed_posting_lists_with_positions(
                postings,
                docs,
                query_weight,
                scorer,
            ),
            (false, true) => {
                Self::union_compressed_posting_lists(postings, docs, query_weight, scorer)
            }
            (false, false) => Ok(PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from(Vec::<u64>::new()),
                ScalarBuffer::from(Vec::<f32>::new()),
                None,
                None,
            ))),
        }
    }

    // search the documents that contain the query
    // return the doc info and the doc length
    // ref: https://en.wikipedia.org/wiki/Okapi_BM25
    //
    // `force_global_scorer` is used by compound search, where leaf scores and
    // bounds must share corpus-level statistics before the global collector
    // can safely propagate its threshold. Old posting formats without impacts
    // fall back to a scorer-derived global upper bound in that mode.
    #[instrument(level = "debug", skip_all)]
    pub(super) async fn load_posting_lists(
        &self,
        tokens: &Tokens,
        params: &FtsSearchParams,
        operator: Operator,
        impact_scorer: &MemBM25Scorer,
        metrics: &dyn MetricsCollector,
        force_global_scorer: bool,
    ) -> Result<LoadedPostings> {
        let is_phrase_query = params.phrase_slop.is_some();
        let is_and_query = operator == Operator::And;
        let required_positions = (is_and_query || is_phrase_query).then(|| {
            (0..tokens.len())
                .map(|index| tokens.position(index))
                .collect::<HashSet<_>>()
        });
        // Fuzzy expansion already ran once at the index level (see
        // `InvertedIndex::bm25_search`) under the global `max_expansions`
        // budget. Positions identify alternatives that must share one posting
        // iterator, including code identifier subwords and fuzzy expansions.
        let tokens = tokens.clone();
        let token_positions = (0..tokens.len())
            .map(|index| tokens.position(index))
            .collect::<Vec<_>>();
        let mut seen_positions = HashSet::with_capacity(token_positions.len());
        let exact_scoring_required = token_positions
            .iter()
            .any(|position| !seen_positions.insert(*position));
        let mut token_ids = Vec::with_capacity(tokens.len());
        let mut matched_positions = required_positions.as_ref().map(|_| HashSet::new());
        for (index, token) in tokens.into_iter().enumerate() {
            let token_id = self.map(&token);
            if let Some(token_id) = token_id {
                let position = token_positions[index];
                if let Some(matched_positions) = matched_positions.as_mut() {
                    matched_positions.insert(position);
                }
                token_ids.push((token_id, token, position));
            }
        }
        if token_ids.is_empty() {
            return Ok(LoadedPostings::empty());
        }
        if let Some(required_positions) = required_positions.as_ref()
            && let Some(matched_positions) = matched_positions.as_ref()
            && !required_positions.is_subset(matched_positions)
        {
            return Ok(LoadedPostings::empty());
        }

        token_ids.sort_unstable_by_key(|(token_id, _, position)| (*position, *token_id));
        token_ids.dedup_by(|lhs, rhs| lhs.0 == rhs.0 && lhs.2 == rhs.2);

        let num_docs = self.docs.len();
        let loaded_postings = stream::iter(token_ids)
            .map(|(token_id, token, position)| async move {
                let posting = self
                    .inverted_list
                    .posting_list(token_id, is_phrase_query, metrics)
                    .await?;

                Result::Ok((token_id, token, position, posting))
            })
            .buffered(self.store.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;

        let needs_union = loaded_postings
            .windows(2)
            .any(|window| window[0].2 == window[1].2);
        if (is_and_query || is_phrase_query)
            && !needs_union
            && loaded_postings
                .iter()
                .any(|(_, _, _, posting)| posting.is_empty())
        {
            return Ok(LoadedPostings::empty());
        }

        if !needs_union {
            let impact_safe = loaded_postings
                .iter()
                .all(|(_, _, _, posting)| posting.has_impacts());
            return Ok(LoadedPostings {
                postings: loaded_postings
                    .into_iter()
                    .map(|(token_id, token, position, posting)| {
                        let needs_scorer_upper_bound = (exact_scoring_required
                            || force_global_scorer)
                            && !posting.has_impacts();
                        let query_weight =
                            if impact_safe || exact_scoring_required || force_global_scorer {
                                impact_scorer.query_weight(&token)
                            } else {
                                idf(posting.len(), num_docs)
                            };
                        let posting = PostingIterator::with_query_weight(
                            token,
                            token_id,
                            position,
                            query_weight,
                            posting,
                            num_docs,
                        );
                        if needs_scorer_upper_bound {
                            posting.with_scorer_upper_bound()
                        } else {
                            posting
                        }
                    })
                    .collect(),
                grouped_expansions: Vec::new(),
                impact_safe,
                exact_scoring_required,
            });
        }

        let docs_for_union = if needs_union {
            Some(match &self.docs {
                PartitionDocumentStore::Legacy(docs) => LoadedDocLengths::Legacy(docs.clone()),
                PartitionDocumentStore::Modern(documents) => {
                    LoadedDocLengths::Modern(documents.lengths().await?)
                }
            })
        } else {
            None
        };

        // WAND's AND mode treats every iterator as required, so expansions from
        // one original query position must be merged before scoring.
        let mut grouped_postings = Vec::new();
        let mut grouped_expansions = Vec::new();
        let mut iter = loaded_postings.into_iter().peekable();
        while let Some((token_id, token, position, posting)) = iter.next() {
            let mut group = vec![(token_id, token, posting)];
            while matches!(iter.peek(), Some((_, _, next_position, _)) if *next_position == position)
            {
                let (token_id, token, _, posting) = iter.next().expect("peeked item must exist");
                group.push((token_id, token, posting));
            }

            let (token_id, token, posting) = if group.len() == 1 {
                group.pop().expect("single-item group must exist")
            } else {
                let token_id = group[0].0;
                let token = group[0].1.clone();
                let terms = group
                    .iter()
                    .map(|(_, token, posting)| {
                        GroupedTermScorer::new(impact_scorer.query_weight(token), posting)
                    })
                    .collect::<Vec<_>>();
                let terms = Arc::<[GroupedTermScorer]>::from(terms);
                let query_weight = terms.iter().map(GroupedTermScorer::query_weight).sum();
                grouped_expansions.push(GroupedExpansionTerms {
                    position,
                    terms: terms.clone(),
                });
                let postings = group
                    .into_iter()
                    .map(|(_, _, posting)| posting)
                    .collect::<Vec<_>>();
                let docs = docs_for_union.as_ref().ok_or_else(|| {
                    Error::index("union docs were not loaded for grouped query terms".to_string())
                })?;
                let posting = Self::union_posting_lists(
                    postings,
                    docs,
                    is_phrase_query,
                    query_weight,
                    impact_scorer,
                )?;
                if posting.is_empty() && (is_and_query || is_phrase_query) {
                    return Ok(LoadedPostings::empty());
                }
                grouped_postings.push(
                    PostingIterator::with_query_weight(
                        token,
                        token_id,
                        position,
                        query_weight,
                        posting,
                        num_docs,
                    )
                    .with_grouped_terms(terms),
                );
                continue;
            };
            if posting.is_empty() {
                if is_and_query || is_phrase_query {
                    return Ok(LoadedPostings::empty());
                }
                continue;
            }

            let query_weight = impact_scorer.query_weight(&token);
            let needs_scorer_upper_bound = !posting.has_impacts();
            let posting = PostingIterator::with_query_weight(
                token,
                token_id,
                position,
                query_weight,
                posting,
                num_docs,
            );
            grouped_postings.push(if needs_scorer_upper_bound {
                posting.with_scorer_upper_bound()
            } else {
                posting
            });
        }

        Ok(LoadedPostings {
            postings: grouped_postings,
            grouped_expansions,
            impact_safe: false,
            exact_scoring_required: true,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn bm25_search_legacy(
        &self,
        docs: &DocSet,
        params: &FtsSearchParams,
        operator: Operator,
        mask: &RowAddrMask,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<u64>>> {
        let documents = LegacyWandDocuments::new(docs, mask);
        self.bm25_search_with_documents(
            &documents,
            params,
            operator,
            postings,
            impact_scorer,
            metrics,
            shared_threshold,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bm25_search_modern(
        &self,
        lengths: &DocLengths,
        visibility: &DocVisibility,
        params: &FtsSearchParams,
        operator: Operator,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<DocId>>> {
        if visibility.is_all() {
            let documents = ModernWandDocuments::all(lengths);
            self.bm25_search_with_documents(
                &documents,
                params,
                operator,
                postings,
                impact_scorer,
                metrics,
                shared_threshold,
            )
        } else {
            let documents = ModernWandDocuments::filtered(lengths, visibility);
            self.bm25_search_with_documents(
                &documents,
                params,
                operator,
                postings,
                impact_scorer,
                metrics,
                shared_threshold,
            )
        }
    }

    #[instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_arguments)]
    fn bm25_search_with_documents<D: WandDocuments>(
        &self,
        documents: &D,
        params: &FtsSearchParams,
        operator: Operator,
        postings: Vec<PostingIterator>,
        impact_scorer: Option<Arc<MemBM25Scorer>>,
        metrics: &dyn MetricsCollector,
        shared_threshold: Arc<AtomicU32>,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        if postings.is_empty() {
            return Ok(Vec::new());
        }

        let hits = if let Some(scorer) = impact_scorer {
            let mut wand = Wand::new(operator, postings.into_iter(), documents, scorer)
                .with_shared_threshold(shared_threshold);
            wand.search(params, metrics)?
        } else {
            let scorer = IndexBM25Scorer::new(std::iter::once(self));
            let mut wand = Wand::new(operator, postings.into_iter(), documents, scorer)
                .with_shared_threshold(shared_threshold);
            wand.search(params, metrics)?
        };
        Ok(hits)
    }

    pub async fn into_builder(self) -> Result<InnerBuilder> {
        let mut builder = InnerBuilder::new_with_posting_tail_codec_and_block_size(
            self.id,
            self.inverted_list.has_positions(),
            self.token_set_format,
            self.inverted_list.posting_tail_codec(),
            self.inverted_list.block_size(),
        );
        builder.tokens = self.tokens.into_mutable();
        builder.docs = self.docs.load_build_docset().await?;

        builder
            .posting_lists
            .reserve_exact(self.inverted_list.len());
        for posting_list in self
            .inverted_list
            .read_all(self.inverted_list.has_positions())
            .await?
        {
            let posting_list = posting_list?;
            builder
                .posting_lists
                .push(posting_list.into_builder(&builder.docs));
        }
        Ok(builder)
    }
}

// at indexing, we use HashMap because we need it to be mutable,
// at searching, we use fst::Map because it's more efficient
#[derive(Debug, Clone)]
pub enum TokenMap {
    HashMap(HashMap<String, u32>),
    Fst(fst::Map<Vec<u8>>),
}

impl Default for TokenMap {
    fn default() -> Self {
        Self::HashMap(HashMap::new())
    }
}

impl DeepSizeOf for TokenMap {
    fn deep_size_of_children(&self, ctx: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::HashMap(map) => map.deep_size_of_children(ctx),
            Self::Fst(map) => map.as_fst().size(),
        }
    }
}

impl TokenMap {
    pub fn len(&self) -> usize {
        match self {
            Self::HashMap(map) => map.len(),
            Self::Fst(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// TokenSet is a mapping from tokens to token ids
#[derive(Debug, Clone, Default, DeepSizeOf)]
pub struct TokenSet {
    // token -> token_id
    pub(crate) tokens: TokenMap,
    pub(crate) next_id: u32,
    total_length: usize,
}

impl TokenSet {
    pub fn into_mut(self) -> Self {
        let tokens = match self.tokens {
            TokenMap::HashMap(map) => map,
            TokenMap::Fst(map) => {
                let mut new_map = HashMap::with_capacity(map.len());
                let mut stream = map.into_stream();
                while let Some((token, token_id)) = stream.next() {
                    new_map.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }

                new_map
            }
        };

        Self {
            tokens: TokenMap::HashMap(tokens),
            next_id: self.next_id,
            total_length: self.total_length,
        }
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_batch(self, format: TokenSetFormat) -> Result<RecordBatch> {
        match format {
            TokenSetFormat::Arrow => self.into_arrow_batch(),
            TokenSetFormat::Fst => self.into_fst_batch(),
        }
    }

    fn into_arrow_batch(self) -> Result<RecordBatch> {
        let mut token_builder = StringBuilder::with_capacity(self.tokens.len(), self.total_length);
        let mut token_id_builder = UInt32Builder::with_capacity(self.tokens.len());

        match self.tokens {
            TokenMap::Fst(map) => {
                let mut stream = map.stream();
                while let Some((token, token_id)) = stream.next() {
                    token_builder.append_value(String::from_utf8_lossy(token));
                    token_id_builder.append_value(token_id as u32);
                }
            }
            TokenMap::HashMap(map) => {
                for (token, token_id) in map.into_iter().sorted_unstable() {
                    token_builder.append_value(token);
                    token_id_builder.append_value(token_id);
                }
            }
        }

        let token_col = token_builder.finish();
        let token_id_col = token_id_builder.finish();

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(TOKEN_COL, DataType::Utf8, false),
            arrow_schema::Field::new(TOKEN_ID_COL, DataType::UInt32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(token_col) as ArrayRef,
                Arc::new(token_id_col) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn into_fst_batch(mut self) -> Result<RecordBatch> {
        let fst_map = match std::mem::take(&mut self.tokens) {
            TokenMap::Fst(map) => map,
            TokenMap::HashMap(map) => Self::build_fst_from_map(map)?,
        };
        let bytes = fst_map.into_fst().into_inner();

        let mut fst_builder = LargeBinaryBuilder::with_capacity(1, bytes.len());
        fst_builder.append_value(bytes);
        let fst_col = fst_builder.finish();

        let mut next_id_builder = UInt32Builder::with_capacity(1);
        next_id_builder.append_value(self.next_id);
        let next_id_col = next_id_builder.finish();

        let mut total_length_builder = UInt64Builder::with_capacity(1);
        total_length_builder.append_value(self.total_length as u64);
        let total_length_col = total_length_builder.finish();

        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(TOKEN_FST_BYTES_COL, DataType::LargeBinary, false),
            arrow_schema::Field::new(TOKEN_NEXT_ID_COL, DataType::UInt32, false),
            arrow_schema::Field::new(TOKEN_TOTAL_LENGTH_COL, DataType::UInt64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(fst_col) as ArrayRef,
                Arc::new(next_id_col) as ArrayRef,
                Arc::new(total_length_col) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn build_fst_from_map(map: HashMap<String, u32>) -> Result<fst::Map<Vec<u8>>> {
        let mut entries: Vec<_> = map.into_iter().collect();
        entries.sort_unstable_by(|(lhs, _), (rhs, _)| lhs.cmp(rhs));
        let mut builder = fst::MapBuilder::memory();
        for (token, token_id) in entries {
            builder
                .insert(&token, token_id as u64)
                .map_err(|e| Error::index(format!("failed to insert token {}: {}", token, e)))?;
        }
        Ok(builder.into_map())
    }

    pub async fn load(reader: Arc<dyn IndexReader>, format: TokenSetFormat) -> Result<Self> {
        match format {
            TokenSetFormat::Arrow => Self::load_arrow(reader).await,
            TokenSetFormat::Fst => Self::load_fst(reader).await,
        }
    }

    async fn load_arrow(reader: Arc<dyn IndexReader>) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;

        let (tokens, next_id, total_length) = spawn_blocking(move || {
            let mut next_id = 0;
            let mut total_length = 0;
            let mut tokens = fst::MapBuilder::memory();

            let token_col = batch[TOKEN_COL].as_string::<i32>();
            let token_id_col = batch[TOKEN_ID_COL].as_primitive::<datatypes::UInt32Type>();

            for (token, &token_id) in token_col.iter().zip(token_id_col.values().iter()) {
                let token =
                    token.ok_or(Error::index("found null token in token set".to_owned()))?;
                next_id = next_id.max(token_id + 1);
                total_length += token.len();
                tokens.insert(token, token_id as u64).map_err(|e| {
                    Error::index(format!("failed to insert token {}: {}", token, e))
                })?;
            }

            Ok::<_, Error>((tokens.into_map(), next_id, total_length))
        })
        .await
        .map_err(|err| Error::execution(format!("failed to spawn blocking task: {}", err)))??;

        Ok(Self {
            tokens: TokenMap::Fst(tokens),
            next_id,
            total_length,
        })
    }

    async fn load_fst(reader: Arc<dyn IndexReader>) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;
        if batch.num_rows() == 0 {
            return Err(Error::index("token set batch is empty".to_owned()));
        }

        let fst_col = batch[TOKEN_FST_BYTES_COL].as_binary::<i64>();
        let bytes = fst_col.value(0);
        let map = fst::Map::new(bytes.to_vec())
            .map_err(|e| Error::index(format!("failed to load fst tokens: {}", e)))?;

        let total_length_col =
            batch[TOKEN_TOTAL_LENGTH_COL].as_primitive::<datatypes::UInt64Type>();

        // Token ids are dense `[0, len)`, so `next_id` must equal the token count. Recompute
        // it instead of trusting the persisted value, which writers before #7115 could leave
        // stale. Mirrors `load_arrow`.
        let next_id = map.len() as u32;

        let total_length = total_length_col
            .values()
            .first()
            .copied()
            .ok_or(Error::index(
                "token total length column is empty".to_owned(),
            ))?;

        Ok(Self {
            tokens: TokenMap::Fst(map),
            next_id,
            total_length: usize::try_from(total_length).map_err(|_| {
                Error::index(format!(
                    "token total length {} overflows usize",
                    total_length
                ))
            })?,
        })
    }

    pub fn add(&mut self, token: String) -> u32 {
        let next_id = self.next_id();
        let len = token.len();
        let token_id = match self.tokens {
            TokenMap::HashMap(ref mut map) => *map.entry(token).or_insert(next_id),
            _ => unreachable!("tokens must be HashMap while indexing"),
        };

        // add token if it doesn't exist
        if token_id == next_id {
            self.next_id += 1;
            self.total_length += len;
        }

        token_id
    }

    pub(crate) fn get_or_add(&mut self, token: &str) -> u32 {
        let next_id = self.next_id;
        match self.tokens {
            TokenMap::HashMap(ref mut map) => {
                if let Some(&token_id) = map.get(token) {
                    return token_id;
                }

                map.insert(token.to_owned(), next_id);
            }
            _ => unreachable!("tokens must be HashMap while indexing"),
        }

        self.next_id += 1;
        self.total_length += token.len();
        next_id
    }

    pub(crate) fn into_mutable(self) -> Self {
        let Self {
            tokens,
            next_id,
            total_length,
        } = self;
        match tokens {
            TokenMap::HashMap(_) => Self {
                tokens,
                next_id,
                total_length,
            },
            TokenMap::Fst(map) => {
                let mut mutable = HashMap::new();
                let mut stream = map.stream();
                while let Some((token, token_id)) = stream.next() {
                    mutable.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }
                Self {
                    tokens: TokenMap::HashMap(mutable),
                    next_id,
                    total_length,
                }
            }
        }
    }

    pub fn get(&self, token: &str) -> Option<u32> {
        match self.tokens {
            TokenMap::HashMap(ref map) => map.get(token).copied(),
            TokenMap::Fst(ref map) => map.get(token).map(|id| id as u32),
        }
    }

    // the `removed_token_ids` must be sorted
    pub fn remap(&mut self, removed_token_ids: &[u32]) {
        if removed_token_ids.is_empty() {
            return;
        }

        let mut map = match std::mem::take(&mut self.tokens) {
            TokenMap::HashMap(map) => map,
            TokenMap::Fst(map) => {
                let mut new_map = HashMap::with_capacity(map.len());
                let mut stream = map.into_stream();
                while let Some((token, token_id)) = stream.next() {
                    new_map.insert(String::from_utf8_lossy(token).into_owned(), token_id as u32);
                }

                new_map
            }
        };

        let mut retained_length = 0;
        map.retain(
            |token, token_id| match removed_token_ids.binary_search(token_id) {
                Ok(_) => false,
                Err(index) => {
                    *token_id -= index as u32;
                    retained_length += token.len();
                    true
                }
            },
        );

        self.tokens = TokenMap::HashMap(map);

        // The retain above compacts the surviving token ids into a dense `[0, len)`
        // range, so `next_id` (handed to the next new token) must follow them down.
        // `total_length` likewise must drop the removed tokens' bytes; it is persisted
        // and feeds memory accounting, so a stale value drifts across remap/merge cycles.
        self.next_id = self.tokens.len() as u32;
        self.total_length = retained_length;
    }

    pub fn next_id(&self) -> u32 {
        self.next_id
    }

    pub(crate) fn memory_size(&self) -> usize {
        match &self.tokens {
            TokenMap::HashMap(map) => {
                self.total_length
                    + map.capacity()
                        * (std::mem::size_of::<String>()
                            + std::mem::size_of::<u32>()
                            + std::mem::size_of::<usize>())
            }
            TokenMap::Fst(map) => map.as_fst().size(),
        }
    }
}

pub struct PostingListReader {
    reader: Arc<dyn IndexReader>,

    /// Layout-specific metadata. V2 keeps its per-token max-score and
    /// length columns lazy so opening a partition doesn't drag O(num_tokens)
    /// bytes off cold storage when the caller only needs `df` for a few terms.
    metadata: PostingMetadata,

    has_position: bool,
    has_impacts: bool,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    positions_layout: PositionsLayout,

    /// Runtime posting-list cache grouping. Non-empty v2 indexes use synthetic
    /// fixed groups so prewarm can improve cache density without rebuilding the
    /// index or relying on persisted grouping metadata.
    grouping: PostingGrouping,

    /// Modern postings contain dense DocIds into the partition document table.
    /// Cache successful boundary validation per immutable token so repeated
    /// queries do not decode the final posting block again.
    modern_doc_id_validations: Option<Arc<[OnceCell<()>]>>,
    /// Skips per-token readiness checks once the whole immutable table is validated.
    modern_postings_validated: AtomicBool,
    modern_num_docs: Option<usize>,

    index_cache: WeakLanceCache,
}

/// Per-token metadata (max_score, length) needed by the BM25 query and stats
/// paths. The legacy and v2 formats store this metadata in different
/// places, with very different cost profiles for cold-load: the variants
/// surface that asymmetry so callers can choose a per-token or bulk access
/// pattern.
enum PostingMetadata {
    /// Legacy v1: offsets and max_scores are encoded in the file's schema
    /// metadata, so they are already in memory by the time `try_new` returns.
    LegacyV1 {
        offsets: Vec<usize>,
        max_scores: Option<Vec<f32>>,
    },
    /// V2: per-token `max_score` and `length` live as columns in the
    /// posting file. The bulk vectors are filled lazily by
    /// `ensure_metadata_loaded`, and the stats path can also fetch a single
    /// token via `posting_len_for_token` without forcing the bulk load.
    V2 {
        metadata: OnceCell<LoadedPostingMetadata>,
    },
}

#[derive(Debug, Clone)]
struct LoadedPostingMetadata {
    max_scores: Vec<f32>,
    lengths: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PositionsLayout {
    None,
    LegacyPerDoc,
    SharedStream(PositionStreamCodec),
}

impl std::fmt::Debug for PostingListReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("InvertedListReader");
        match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => {
                s.field("layout", &"legacy_v1")
                    .field("offsets", offsets)
                    .field("max_scores", max_scores);
            }
            PostingMetadata::V2 { metadata } => {
                s.field("layout", &"v2")
                    .field("metadata_loaded", &metadata.initialized());
            }
        }
        s.finish()
    }
}

impl DeepSizeOf for PostingListReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let metadata_size = match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => offsets.deep_size_of_children(context) + max_scores.deep_size_of_children(context),
            PostingMetadata::V2 { metadata } => metadata
                .get()
                .map(|loaded| {
                    loaded.max_scores.deep_size_of_children(context)
                        + loaded.lengths.deep_size_of_children(context)
                })
                .unwrap_or(0),
        };
        let validation_size = self
            .modern_doc_id_validations
            .as_ref()
            .map(|validations| {
                validations
                    .len()
                    .saturating_mul(std::mem::size_of::<OnceCell<()>>())
            })
            .unwrap_or(0);
        metadata_size + self.grouping.deep_size_of_children(context) + validation_size
    }
}

impl PostingListReader {
    pub(crate) async fn try_new(
        reader: Arc<dyn IndexReader>,
        index_cache: &LanceCache,
    ) -> Result<Self> {
        let positions_layout = if reader.schema().field(COMPRESSED_POSITION_COL).is_some() {
            PositionsLayout::SharedStream(parse_shared_position_codec(&reader.schema().metadata)?)
        } else if reader.schema().field(POSITION_COL).is_some() {
            PositionsLayout::LegacyPerDoc
        } else {
            PositionsLayout::None
        };
        let posting_tail_codec = parse_posting_tail_codec(&reader.schema().metadata)?;
        let block_size = parse_posting_block_size(&reader.schema().metadata)?;
        let has_position = positions_layout != PositionsLayout::None;
        let has_impacts = reader.schema().field(IMPACT_COL).is_some();
        let metadata = if reader.schema().field(POSTING_COL).is_none() {
            let (offsets, max_scores) = Self::load_metadata(reader.schema())?;
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            }
        } else {
            PostingMetadata::V2 {
                metadata: OnceCell::new(),
            }
        };

        let is_legacy_layout = matches!(&metadata, PostingMetadata::LegacyV1 { .. });
        let grouping = PostingGrouping::for_reader(is_legacy_layout, reader.num_rows());
        let modern_doc_id_validations = (!is_legacy_layout).then(|| {
            (0..reader.num_rows())
                .map(|_| OnceCell::new())
                .collect::<Vec<_>>()
                .into()
        });

        Ok(Self {
            reader,
            metadata,
            has_position,
            has_impacts,
            posting_tail_codec,
            block_size,
            positions_layout,
            grouping,
            modern_doc_id_validations,
            modern_postings_validated: AtomicBool::new(false),
            modern_num_docs: None,
            index_cache: WeakLanceCache::from(index_cache),
        })
    }

    // for legacy format
    // returns the offsets and max scores
    fn load_metadata(
        schema: &lance_core::datatypes::Schema,
    ) -> Result<(Vec<usize>, Option<Vec<f32>>)> {
        let offsets = schema
            .metadata
            .get("offsets")
            .ok_or(Error::index("offsets not found in metadata".to_owned()))?;
        let offsets = serde_json::from_str(offsets)?;

        let max_scores = schema
            .metadata
            .get("max_scores")
            .map(|max_scores| serde_json::from_str(max_scores))
            .transpose()?;
        Ok((offsets, max_scores))
    }

    // the number of posting lists
    pub fn len(&self) -> usize {
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => offsets.len(),
            PostingMetadata::V2 { .. } => self.reader.num_rows(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn has_positions(&self) -> bool {
        self.has_position
    }

    pub(crate) fn posting_tail_codec(&self) -> PostingTailCodec {
        self.posting_tail_codec
    }

    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }

    fn is_legacy_layout(&self) -> bool {
        matches!(self.metadata, PostingMetadata::LegacyV1 { .. })
    }

    /// Sync access to `posting_len`. Requires v2 metadata to already be
    /// loaded via [`ensure_metadata_loaded`]; the bm25 scoring path enforces
    /// that contract before kicking off wand. The stats path uses
    /// [`Self::posting_len_for_token`] instead, which avoids the bulk load.
    pub(crate) fn posting_len(&self, token_id: u32) -> usize {
        let token_id = token_id as usize;
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let next_offset = offsets
                    .get(token_id + 1)
                    .copied()
                    .unwrap_or(self.reader.num_rows());
                next_offset - offsets[token_id]
            }
            PostingMetadata::V2 { metadata } => {
                let metadata = metadata
                    .get()
                    .expect("v2 posting metadata must be bulk-loaded before sync posting_len; call ensure_metadata_loaded first");
                metadata.lengths[token_id] as usize
            }
        }
    }

    /// Async access to a single token's posting list length. For v2
    /// indexes this reads one row of posting metadata if the bulk metadata has
    /// not been loaded yet, and never triggers the bulk load itself. The stats
    /// path uses this so a single-term `df` lookup costs O(1) bytes rather
    /// than O(num_unique_tokens).
    pub(crate) async fn posting_len_for_token(
        &self,
        token_id: u32,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<usize> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { .. } => Ok(self.posting_len(token_id)),
            PostingMetadata::V2 { metadata } => {
                if let Some(metadata) = metadata.get() {
                    return Ok(metadata.lengths[token_id as usize] as usize);
                }
                let (_, length) = self.posting_metadata_for_token(token_id, metrics).await?;
                length
                    .map(|len| len as usize)
                    .ok_or_else(|| Error::index("posting length metadata missing".to_string()))
            }
        }
    }

    /// Async access to a single token's `(max_score, length)` pair. Mirrors
    /// [`Self::posting_len_for_token`] but covers both columns the scoring
    /// path needs, in one read. For v2 indexes that have not been
    /// bulk-loaded this issues one `read_range(token..token+1, [MAX_SCORE,
    /// LENGTH])`; for legacy v1 the values come from in-memory schema
    /// metadata.
    pub(crate) async fn posting_metadata_for_token(
        &self,
        token_id: u32,
        metrics: Option<&dyn MetricsCollector>,
    ) -> Result<(Option<f32>, Option<u32>)> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { max_scores, .. } => {
                Ok((max_scores.as_ref().map(|m| m[token_id as usize]), None))
            }
            PostingMetadata::V2 { metadata } => {
                if let Some(loaded) = metadata.get() {
                    return Ok((
                        Some(loaded.max_scores[token_id as usize]),
                        Some(loaded.lengths[token_id as usize]),
                    ));
                }
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(PostingMetadataKey { token_id }, || async move {
                        let token_id = token_id as usize;
                        let batch = self
                            .reader
                            .read_range(token_id..token_id + 1, Some(&[MAX_SCORE_COL, LENGTH_COL]))
                            .await?;
                        let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
                        let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
                        Ok(PostingMetadataValue { max_score, length })
                    })
                    .await;
                if let Some(metrics) = metrics {
                    match &result {
                        Ok((_, true)) => metrics.record_index_cache_hit(),
                        _ => metrics.record_index_cache_miss(),
                    }
                }
                let metadata = result.map(|(value, _)| value)?;
                Ok((Some(metadata.max_score), Some(metadata.length)))
            }
        }
    }

    /// Force the v2 bulk metadata (`max_scores`, `lengths`) into
    /// memory. Cheap to call repeatedly; no-op for legacy v1 indexes whose
    /// metadata is already populated from schema metadata at `try_new` time.
    pub(crate) async fn ensure_metadata_loaded(&self) -> Result<()> {
        let PostingMetadata::V2 { metadata } = &self.metadata else {
            return Ok(());
        };
        metadata
            .get_or_try_init(|| async {
                let batch = self
                    .reader
                    .read_range(
                        0..self.reader.num_rows(),
                        Some(&[MAX_SCORE_COL, LENGTH_COL]),
                    )
                    .await?;
                let max_scores = batch[MAX_SCORE_COL]
                    .as_primitive::<Float32Type>()
                    .values()
                    .to_vec();
                let lengths = batch[LENGTH_COL]
                    .as_primitive::<UInt32Type>()
                    .values()
                    .to_vec();
                Ok::<LoadedPostingMetadata, Error>(LoadedPostingMetadata {
                    max_scores,
                    lengths,
                })
            })
            .await?;
        Ok(())
    }

    pub(crate) async fn posting_batch(
        &self,
        token_id: u32,
        with_position: bool,
    ) -> Result<RecordBatch> {
        if self.is_legacy_layout() {
            self.posting_batch_legacy(token_id, with_position).await
        } else {
            let token_id = token_id as usize;
            let mut columns = if with_position {
                match self.positions_layout {
                    PositionsLayout::SharedStream(_) => {
                        vec![
                            POSTING_COL,
                            COMPRESSED_POSITION_COL,
                            POSITION_BLOCK_OFFSET_COL,
                        ]
                    }
                    PositionsLayout::LegacyPerDoc => vec![POSTING_COL, POSITION_COL],
                    PositionsLayout::None => vec![POSTING_COL],
                }
            } else {
                vec![POSTING_COL]
            };
            if self.has_impacts {
                columns.push(IMPACT_COL);
            }
            let batch = self
                .reader
                .read_range(token_id..token_id + 1, Some(&columns))
                .await?;
            Ok(batch)
        }
    }

    async fn posting_batch_legacy(
        &self,
        token_id: u32,
        with_position: bool,
    ) -> Result<RecordBatch> {
        let mut columns = vec![ROW_ID, FREQUENCY_COL];
        if with_position {
            columns.push(POSITION_COL);
        }

        let length = self.posting_len(token_id);
        let PostingMetadata::LegacyV1 { offsets, .. } = &self.metadata else {
            unreachable!("posting_batch_legacy is only reachable on legacy v1 layout");
        };
        let token_id = token_id as usize;
        let offset = offsets[token_id];
        let batch = self
            .reader
            .read_range(offset..offset + length, Some(&columns))
            .await?;
        Ok(batch)
    }

    #[instrument(level = "debug", skip(self, metrics))]
    pub(crate) async fn posting_list(
        &self,
        token_id: u32,
        is_phrase_query: bool,
        metrics: &dyn MetricsCollector,
    ) -> Result<PostingList> {
        let mut posting = match self.group_range_for_token(token_id) {
            // Grouped path (issue #7040): one cache entry covers rows
            // [start, end), so neighbouring rare terms share a single read.
            Some((start, end)) => {
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(
                        posting_list_group_cache_key(start, end, self.has_impacts),
                        || async move {
                            metrics.record_part_load();
                            info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="inverted", part_id=start);
                            self.load_posting_list_group(start, end).await
                        },
                    )
                    .await;
                match &result {
                    Ok((_, true)) => metrics.record_index_cache_hit(),
                    _ => metrics.record_index_cache_miss(),
                }
                let (group, _) = result?;
                let (max_score, length) = if group.needs_external_metadata() {
                    self.posting_metadata_for_token(token_id, Some(metrics))
                        .await?
                } else {
                    (None, None)
                };
                let slot = (token_id - start) as usize;
                group
                    .posting_list(slot, max_score, length)?
                    .ok_or_else(|| {
                        Error::index(format!(
                            "token {token_id} maps to slot {slot} outside posting group [{start}, {end})"
                        ))
                    })?
            }
            // Fallback for layouts that cannot use row-based groups: one cache
            // entry per token.
            None => {
                let result = self
                    .index_cache
                    .get_or_insert_with_key_hit(
                        posting_list_cache_key(token_id, self.has_impacts),
                        || async move {
                            metrics.record_part_load();
                            info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="inverted", part_id=token_id);
                            // Fetch the posting batch and this token's (max_score,
                            // length) in parallel; for cold v2 partitions this is one
                            // single-row metadata read plus one posting-row read,
                            // instead of pulling the full per-token metadata table.
                            let (batch, (max_score, length)) = futures::try_join!(
                                self.posting_batch(token_id, false),
                                self.posting_metadata_for_token(token_id, Some(metrics)),
                            )?;
                            self.posting_list_from_batch(&batch, max_score, length)
                        },
                    )
                    .await;
                match &result {
                    Ok((_, true)) => metrics.record_index_cache_hit(),
                    _ => metrics.record_index_cache_miss(),
                }
                result?.0.as_ref().clone()
            }
        };

        if !self.modern_posting_is_validated(token_id)? {
            self.ensure_modern_posting_validated(token_id, &posting)
                .await?;
        }

        if is_phrase_query && !posting.has_position() {
            // hit the cache and when the cache was populated, the positions column was not loaded
            let positions = self.read_positions(token_id, metrics).await?;
            posting.set_positions(positions);
        }

        Ok(posting)
    }

    async fn ensure_modern_posting_validated(
        &self,
        token_id: u32,
        posting: &PostingList,
    ) -> Result<()> {
        let (Some(validations), Some(num_docs)) =
            (&self.modern_doc_id_validations, self.modern_num_docs)
        else {
            return Ok(());
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        validation
            .get_or_try_init(|| async {
                Self::validate_modern_posting(token_id, posting, num_docs)
            })
            .await
            .map(|_| ())
    }

    #[inline]
    fn modern_posting_is_validated(&self, token_id: u32) -> Result<bool> {
        if self.modern_postings_validated.load(Ordering::Acquire) {
            return Ok(true);
        }
        let (Some(validations), Some(_)) = (&self.modern_doc_id_validations, self.modern_num_docs)
        else {
            return Ok(true);
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        Ok(validation.get().is_some())
    }

    fn validate_modern_posting(
        token_id: u32,
        posting: &PostingList,
        num_docs: usize,
    ) -> Result<()> {
        validate_modern_posting_doc_ids(posting, &format!("token id {token_id}"), num_docs)
    }

    async fn publish_modern_posting_validated(&self, token_id: u32) -> Result<()> {
        let Some(validations) = &self.modern_doc_id_validations else {
            return Ok(());
        };
        let validation = validations.get(token_id as usize).ok_or_else(|| {
            Error::index(format!(
                "modern FTS token id {token_id} is outside validation state [0, {})",
                validations.len()
            ))
        })?;
        validation
            .get_or_try_init(|| async { Result::Ok(()) })
            .await
            .map(|_| ())
    }

    fn modern_posting_validation_ready(&self) -> bool {
        if self.modern_postings_validated.load(Ordering::Acquire) {
            return true;
        }
        let ready = self
            .modern_doc_id_validations
            .as_ref()
            .is_none_or(|validations| validations.iter().all(|state| state.get().is_some()));
        if ready {
            self.modern_postings_validated
                .store(true, Ordering::Release);
        }
        ready
    }

    /// Map a token id to its cache group's row range `[start, end)`, or `None`
    /// when grouping is not available so the caller falls back to the per-token
    /// path. In v2 the token id is the row offset, so the group range is also
    /// the physical row range.
    fn group_range_for_token(&self, token_id: u32) -> Option<(u32, u32)> {
        self.grouping.range_for_token(token_id, self.len())
    }

    /// Read rows `[start, end)` into one compact Arrow-backed cache value.
    /// Positions are excluded; phrase queries load them on demand via
    /// [`Self::read_positions`].
    async fn load_posting_list_group(&self, start: u32, end: u32) -> Result<PostingListGroup> {
        let mut columns = vec![POSTING_COL, MAX_SCORE_COL, LENGTH_COL];
        if self.has_impacts {
            columns.push(IMPACT_COL);
        }
        let batch = self
            .reader
            .read_range(start as usize..end as usize, Some(&columns))
            .await?;
        PostingListGroup::new_packed_with_block_size(
            batch.shrink_to_fit()?,
            self.posting_tail_codec,
            self.block_size,
        )
    }

    fn posting_list_from_batch_parts(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions_layout: PositionsLayout,
    ) -> Result<PostingList> {
        let posting_list = PostingList::from_batch_with_tail_codec_and_positions_layout(
            batch,
            max_score,
            length,
            posting_tail_codec,
            block_size,
            positions_layout,
        )?;
        Ok(posting_list)
    }

    pub(crate) fn posting_list_from_batch(
        &self,
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<PostingList> {
        Self::posting_list_from_batch_parts(
            batch,
            max_score,
            length,
            self.posting_tail_codec,
            self.block_size,
            self.positions_layout,
        )
    }

    /// Build posting lists for one chunk's token range from `chunk_batch`, rebasing
    /// global offsets to chunk-local rows. Returns `(global token_id, PostingList)`
    /// pairs identical to the whole-file path, only bounded to one chunk.
    fn build_prewarm_posting_lists_chunk(
        chunk_batch: RecordBatch,
        chunk: PrewarmChunk<'_>,
        ctx: &PrewarmBuildCtx<'_>,
    ) -> Result<Vec<(u32, PostingList)>> {
        let mut posting_lists = Vec::with_capacity(chunk.token_count);
        for local in 0..chunk.token_count {
            let global = chunk.tok_start + local;
            let row_batch = if let Some(chunk_offsets) = chunk.offsets {
                // Legacy v1: rebase global offsets to chunk row 0; the last token
                // ends at `chunk.end_row` (no trailing sentinel in chunk_offsets).
                let base = chunk_offsets[0];
                let start = chunk_offsets[local] - base;
                let end = if local + 1 < chunk_offsets.len() {
                    chunk_offsets[local + 1] - base
                } else {
                    chunk.end_row - base
                };
                chunk_batch.slice(start, end - start)
            } else {
                // V2: one posting row per token; row `local` within the chunk.
                chunk_batch.slice(local, 1)
            };
            let row_batch = row_batch.shrink_to_fit()?;
            let posting_list = Self::posting_list_from_batch_parts(
                &row_batch,
                ctx.max_scores.map(|scores| scores[global]),
                ctx.lengths.map(|lengths| lengths[global]),
                ctx.posting_tail_codec,
                ctx.block_size,
                ctx.positions_layout,
            )?;
            posting_lists.push((global as u32, posting_list));
        }

        Ok(posting_lists)
    }

    /// Read the posting rows for token ids `[tok_start, tok_end)` into one RecordBatch.
    /// For v2 the token range is the row range; for v1 it's derived from the offsets.
    async fn read_chunk_batch(
        &self,
        tok_start: usize,
        tok_end: usize,
        with_position: bool,
    ) -> Result<RecordBatch> {
        let columns = self.posting_columns(with_position);
        let row_range = match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let start = offsets[tok_start];
                let end = offsets
                    .get(tok_end)
                    .copied()
                    .unwrap_or_else(|| self.reader.num_rows());
                start..end
            }
            PostingMetadata::V2 { .. } => tok_start..tok_end,
        };
        let batch = self.reader.read_range(row_range, Some(&columns)).await?;
        Ok(batch)
    }

    async fn prewarm_posting_lists(
        &self,
        with_position: bool,
        chunk_concurrency: usize,
    ) -> Result<()> {
        self.prewarm_posting_lists_chunked(with_position, None, chunk_concurrency)
            .await?;
        Ok(())
    }

    /// Stream the partition's posting lists into the cache in bounded token-row chunks
    /// (read -> build -> insert -> drop), so peak resident set is ~one chunk. Returns
    /// the chunk count (tests assert it split). `chunk_tokens_override` is test-only.
    async fn prewarm_posting_lists_chunked(
        &self,
        with_position: bool,
        chunk_tokens_override: Option<usize>,
        chunk_concurrency: usize,
    ) -> Result<usize> {
        if with_position && !self.has_positions() {
            return Err(Error::invalid_input(
                "cannot prewarm positions for an inverted index that was built without positions; recreate the index with with_position=true".to_owned(),
            ));
        }

        // Make max_scores/lengths available for query-local packed views. The
        // materialized fallback also clones them into its blocking build task.
        self.ensure_metadata_loaded().await?;

        // With grouping the cache stores one entry per group, so a group's
        // posting lists must all be resident at once: align chunk boundaries to
        // whole groups. Without grouping, chunks are plain token ranges.
        let grouping = self.grouping.clone();
        let use_packed_groups = grouping.is_grouped() && !with_position;
        // Packed groups reuse the reader's bulk metadata at query time, so they
        // do not need the temporary full-partition metadata clones used by the
        // materialized fallback.
        let state = (!use_packed_groups).then(|| self.chunk_build_state());
        let token_count = self.len();
        let posting_data_size_bytes = self.posting_data_size_bytes();
        let chunk_tokens = chunk_tokens_override
            .unwrap_or_else(|| prewarm_chunk_tokens(token_count, posting_data_size_bytes))
            .max(1);
        let chunk_ranges = prewarm_chunk_ranges(&grouping, token_count, chunk_tokens);
        let chunk_count = chunk_ranges.len();
        let chunk_concurrency = chunk_concurrency.max(1);

        let read_build_start = Instant::now();
        stream::iter(chunk_ranges)
            .map(|(tok_start, tok_end)| {
                let state = state.as_ref();
                let grouping = &grouping;
                async move {
                    if use_packed_groups {
                        let groups = self
                            .build_packed_chunk_groups(tok_start, tok_end, token_count, grouping)
                            .await?;
                        for (start, end, group) in groups {
                            self.index_cache
                                .insert_with_key(
                                    &posting_list_group_cache_key(start, end, self.has_impacts),
                                    Arc::new(group),
                                )
                                .await;
                        }
                    } else {
                        let state = state.expect(
                            "materialized prewarm must initialize posting-list build state",
                        );
                        let posting_lists = self
                            .build_chunk_postings(tok_start, tok_end, with_position, state)
                            .await?;
                        self.publish_chunk_postings(
                            posting_lists,
                            grouping,
                            tok_start,
                            tok_end,
                            token_count,
                            with_position,
                        )
                        .await;
                    }
                    Result::Ok(())
                }
            })
            .buffer_unordered(chunk_concurrency)
            .try_collect::<()>()
            .await?;
        let read_build_elapsed = read_build_start.elapsed();

        info!(
            legacy_layout = self.is_legacy_layout(),
            with_position,
            token_count,
            chunk_count,
            chunk_tokens,
            chunk_concurrency,
            posting_data_size_bytes,
            read_build_ms = read_build_elapsed.as_secs_f64() * 1000.0,
            "posting list prewarm timing"
        );

        Ok(chunk_count)
    }

    /// Loop-invariant inputs shared by every chunk build: the metadata vecs
    /// (`Arc`d so chunks share them without re-cloning) plus codec/layout.
    fn chunk_build_state(&self) -> ChunkBuildState {
        let (offsets, max_scores, lengths) = match &self.metadata {
            PostingMetadata::LegacyV1 {
                offsets,
                max_scores,
            } => (Some(offsets.clone()), max_scores.clone(), None),
            PostingMetadata::V2 { metadata } => (
                None,
                metadata.get().map(|loaded| loaded.max_scores.clone()),
                metadata.get().map(|loaded| loaded.lengths.clone()),
            ),
        };
        ChunkBuildState {
            offsets: offsets.map(Arc::new),
            max_scores: max_scores.map(Arc::new),
            lengths: lengths.map(Arc::new),
            posting_tail_codec: self.posting_tail_codec,
            block_size: self.block_size,
            positions_layout: self.positions_layout,
        }
    }

    /// Read one token-row chunk and build its posting lists off the runtime thread.
    /// The large batch is dropped inside the blocking task once built, bounding
    /// resident memory to one chunk.
    async fn build_chunk_postings(
        &self,
        tok_start: usize,
        tok_end: usize,
        with_position: bool,
        state: &ChunkBuildState,
    ) -> Result<Vec<(u32, PostingList)>> {
        let chunk_token_count = tok_end - tok_start;
        let chunk_batch = self
            .read_chunk_batch(tok_start, tok_end, with_position)
            .await?;

        let (chunk_offsets, chunk_end_row) = match state.offsets.as_ref() {
            Some(offsets) => {
                let end_row = offsets
                    .get(tok_end)
                    .copied()
                    .unwrap_or_else(|| self.reader.num_rows());
                (Some(offsets[tok_start..tok_end].to_vec()), end_row)
            }
            // V2 doesn't use chunk_end_row (one row per token); pass tok_end.
            None => (None, tok_end),
        };
        let max_scores = state.max_scores.clone();
        let lengths = state.lengths.clone();
        let posting_tail_codec = state.posting_tail_codec;
        let block_size = state.block_size;
        let positions_layout = state.positions_layout;
        let num_docs = self.modern_num_docs;
        let posting_lists = spawn_blocking(move || {
            let ctx = PrewarmBuildCtx {
                max_scores: max_scores.as_deref().map(|v| v.as_slice()),
                lengths: lengths.as_deref().map(|v| v.as_slice()),
                posting_tail_codec,
                block_size,
                positions_layout,
            };
            let chunk = PrewarmChunk {
                tok_start,
                token_count: chunk_token_count,
                offsets: chunk_offsets.as_deref(),
                end_row: chunk_end_row,
            };
            let posting_lists = Self::build_prewarm_posting_lists_chunk(chunk_batch, chunk, &ctx)?;
            if let Some(num_docs) = num_docs {
                for (token_id, posting) in &posting_lists {
                    Self::validate_modern_posting(*token_id, posting, num_docs)?;
                }
            }
            Result::Ok(posting_lists)
        })
        .await
        .map_err(|err| {
            Error::internal(format!(
                "Failed to build prewarm posting lists in blocking task: {err}"
            ))
        })??;
        for (token_id, _) in &posting_lists {
            self.publish_modern_posting_validated(*token_id).await?;
        }
        // The chunk yields its token range as contiguous ascending ids from
        // `tok_start`; the group publish path relies on this to index the lists.
        debug_assert_eq!(posting_lists.len(), chunk_token_count);
        debug_assert!(
            posting_lists
                .iter()
                .enumerate()
                .all(|(i, (token_id, _))| *token_id as usize == tok_start + i)
        );
        Ok(posting_lists)
    }

    /// Build compact v2 groups directly from one posting-row chunk. Each group
    /// slice is deep-copied once, so it owns only its Arrow buffers without
    /// materializing a `Vec<PostingList>` or retaining the full chunk.
    async fn build_packed_chunk_groups(
        &self,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
        grouping: &PostingGrouping,
    ) -> Result<Vec<(u32, u32, PostingListGroup)>> {
        debug_assert!(grouping.is_grouped());
        debug_assert!(!self.is_legacy_layout());

        let chunk_batch = self.read_chunk_batch(tok_start, tok_end, false).await?;
        let ranges = grouping.ranges_for_chunk(tok_start, tok_end, token_count);
        let posting_tail_codec = self.posting_tail_codec;
        let block_size = self.block_size;
        let num_docs = self.modern_num_docs;
        let (chunk_max_scores, chunk_lengths) = match &self.metadata {
            PostingMetadata::V2 { metadata } => {
                let loaded = metadata.get().ok_or_else(|| {
                    Error::internal("packed prewarm requires loaded posting metadata".to_owned())
                })?;
                (
                    loaded.max_scores[tok_start..tok_end].to_vec(),
                    loaded.lengths[tok_start..tok_end].to_vec(),
                )
            }
            PostingMetadata::LegacyV1 { .. } => {
                return Err(Error::internal(
                    "packed prewarm is not supported for legacy posting metadata".to_owned(),
                ));
            }
        };

        let groups = spawn_blocking(move || {
            let mut groups = Vec::with_capacity(ranges.len());
            for (start, end) in ranges {
                let start_usize = start as usize;
                let end_usize = end as usize;
                let local_start = start_usize - tok_start;
                let group_len = end_usize - start_usize;
                let group_batch = chunk_batch.slice(local_start, group_len).shrink_to_fit()?;
                let group = PostingListGroup::new_packed_with_block_size(
                    group_batch,
                    posting_tail_codec,
                    block_size,
                )?;
                if let Some(num_docs) = num_docs {
                    for token_id in start..end {
                        let chunk_slot = token_id as usize - tok_start;
                        let posting = group
                            .posting_list(
                                (token_id - start) as usize,
                                Some(chunk_max_scores[chunk_slot]),
                                Some(chunk_lengths[chunk_slot]),
                            )?
                            .ok_or_else(|| {
                                Error::index(format!(
                                    "token {token_id} is missing from prewarm posting group [{start}, {end})"
                                ))
                            })?;
                        Self::validate_modern_posting(token_id, &posting, num_docs)?;
                    }
                }
                groups.push((start, end, group));
            }
            Result::Ok(groups)
        })
        .await
        .map_err(|err| {
            Error::internal(format!(
                "Failed to build packed prewarm posting groups in blocking task: {err}"
            ))
        })??;
        for (start, end, _) in &groups {
            for token_id in *start..*end {
                self.publish_modern_posting_validated(token_id).await?;
            }
        }
        Ok(groups)
    }

    /// Strip positions into their own per-token cache entries (the posting cache
    /// holds positions-free lists), then populate the same cache keys the read
    /// path uses: grouped entries when grouping is active, per-token entries
    /// otherwise. Called once per chunk; the chunk's lists drop on return.
    async fn publish_chunk_postings(
        &self,
        posting_lists: Vec<(u32, PostingList)>,
        grouping: &PostingGrouping,
        tok_start: usize,
        tok_end: usize,
        token_count: usize,
        with_position: bool,
    ) {
        match grouping {
            PostingGrouping::None => {
                for (token_id, mut posting_list) in posting_lists {
                    self.cache_positions(&mut posting_list, token_id, with_position)
                        .await;
                    self.index_cache
                        .insert_with_key(
                            &posting_list_cache_key(token_id, self.has_impacts),
                            Arc::new(posting_list),
                        )
                        .await;
                }
            }
            PostingGrouping::SyntheticFixed { .. } => {
                let mut chunk_postings = Vec::with_capacity(posting_lists.len());
                for (token_id, mut posting_list) in posting_lists {
                    self.cache_positions(&mut posting_list, token_id, with_position)
                        .await;
                    chunk_postings.push(posting_list);
                }
                // Chunk is group-aligned, so every group starting in it also ends
                // in it; `chunk_postings[i]` is token `tok_start + i`. The last
                // group's `end` derives from `token_count`, matching the read path
                // so both produce identical `PostingListGroupKey`s.
                for (start, end) in grouping.ranges_for_chunk(tok_start, tok_end, token_count) {
                    let start_usize = start as usize;
                    let lo = start_usize - tok_start;
                    let hi = end as usize - tok_start;
                    let group = PostingListGroup::new(chunk_postings[lo..hi].to_vec());
                    self.index_cache
                        .insert_with_key(
                            &posting_list_group_cache_key(start, end, self.has_impacts),
                            Arc::new(group),
                        )
                        .await;
                }
            }
        }
    }

    /// Move a posting list's positions (when present and requested) into the
    /// dedicated per-token position cache, leaving the posting list positions-free.
    async fn cache_positions(
        &self,
        posting_list: &mut PostingList,
        token_id: u32,
        with_position: bool,
    ) {
        if with_position && let Some(positions) = posting_list.take_positions() {
            self.index_cache
                .insert_with_key(&PositionKey { token_id }, Arc::new(Positions(positions)))
                .await;
        }
    }

    /// Cheap `invert.lance` size estimate (file length from object metadata, no
    /// data read), used only to size prewarm chunks. Falls back to a row-count
    /// proxy when the reader can't surface the length (legacy v1).
    pub(crate) fn posting_data_size_bytes(&self) -> u64 {
        if let Some(size) = self.reader.file_size_bytes() {
            return size;
        }
        // Fallback proxy for readers that don't cache their file length: just needs
        // to be monotonic in partition size.
        const ESTIMATED_BYTES_PER_ROW: u64 = 16;
        (self.reader.num_rows() as u64).saturating_mul(ESTIMATED_BYTES_PER_ROW)
    }

    pub(crate) async fn read_batch(&self, with_position: bool) -> Result<RecordBatch> {
        let columns = self.posting_columns(with_position);
        let batch = self
            .reader
            .read_range(0..self.reader.num_rows(), Some(&columns))
            .await?;
        Ok(batch)
    }

    pub(crate) async fn read_all(
        &self,
        with_position: bool,
    ) -> Result<impl Iterator<Item = Result<PostingList>> + '_> {
        // read_all walks every posting list; the bulk metadata is paid for
        // unconditionally, so just load it once up front and index into it
        // synchronously below.
        self.ensure_metadata_loaded().await?;
        let batch = self.read_batch(with_position).await?;
        Ok((0..self.len()).map(move |i| {
            let token_id = i as u32;
            let range = self.posting_list_range(token_id);
            let batch = batch.slice(i, range.end - range.start);
            let (max_score, length) = self.bulk_metadata_for_token(token_id);
            self.posting_list_from_batch(&batch, max_score, length)
        }))
    }

    /// Sync lookup of `(max_score, length)` from the bulk-loaded metadata.
    /// Only safe after [`Self::ensure_metadata_loaded`]; callers that hold
    /// the OnceCell-loaded reference (e.g. read_all, prewarm) use this to
    /// avoid the per-token IO path.
    fn bulk_metadata_for_token(&self, token_id: u32) -> (Option<f32>, Option<u32>) {
        match &self.metadata {
            PostingMetadata::LegacyV1 { max_scores, .. } => {
                (max_scores.as_ref().map(|m| m[token_id as usize]), None)
            }
            PostingMetadata::V2 { metadata } => {
                let loaded = metadata.get().expect(
                    "v2 metadata must be bulk-loaded before bulk_metadata_for_token; call ensure_metadata_loaded first",
                );
                (
                    Some(loaded.max_scores[token_id as usize]),
                    Some(loaded.lengths[token_id as usize]),
                )
            }
        }
    }

    async fn read_positions(
        &self,
        token_id: u32,
        metrics: &dyn MetricsCollector,
    ) -> Result<CompressedPositionStorage> {
        let result = self.index_cache.get_or_insert_with_key_hit(PositionKey { token_id }, || async move {
            let positions = match self.positions_layout {
                PositionsLayout::None => {
                    return Err(Error::invalid_input(
                        "position is not found but required for phrase queries, try recreating the index with position".to_owned(),
                    ));
                }
                PositionsLayout::LegacyPerDoc => {
                    let batch = self
                        .reader
                        .read_range(self.posting_list_range(token_id), Some(&[POSITION_COL]))
                        .await
                        .map_err(|e| match e {
                            Error::Schema { .. } => Error::invalid_input("position is not found but required for phrase queries, try recreating the index with position".to_owned()),
                            e => e,
                        })?;
                    CompressedPositionStorage::LegacyPerDoc(
                        batch[POSITION_COL].as_list::<i32>().value(0).as_list::<i32>().clone(),
                    )
                }
                PositionsLayout::SharedStream(codec) => {
                    let batch = self
                        .reader
                        .read_range(
                            self.posting_list_range(token_id),
                            Some(&[COMPRESSED_POSITION_COL, POSITION_BLOCK_OFFSET_COL]),
                        )
                        .await
                        .map_err(|e| match e {
                            Error::Schema { .. } => Error::invalid_input("position is not found but required for phrase queries, try recreating the index with position".to_owned()),
                            e => e,
                        })?;
                    let bytes = bytes::Bytes::from(
                        batch[COMPRESSED_POSITION_COL]
                            .as_binary::<i64>()
                            .value(0)
                            .to_vec(),
                    );
                    let block_offsets = batch[POSITION_BLOCK_OFFSET_COL]
                        .as_list::<i32>()
                        .value(0)
                        .as_primitive::<UInt32Type>()
                        .values()
                        .to_vec();
                    CompressedPositionStorage::SharedStream(SharedPositionStream::new(
                        codec,
                        block_offsets,
                        bytes,
                    ))
                }
            };
            Result::Ok(Positions(positions))
        }).await;
        match &result {
            Ok((_, true)) => metrics.record_index_cache_hit(),
            _ => metrics.record_index_cache_miss(),
        }
        let (positions, _) = result?;
        Ok(positions.0.clone())
    }

    fn posting_list_range(&self, token_id: u32) -> Range<usize> {
        match &self.metadata {
            PostingMetadata::LegacyV1 { offsets, .. } => {
                let offset = offsets[token_id as usize];
                let posting_len = self.posting_len(token_id);
                offset..offset + posting_len
            }
            PostingMetadata::V2 { .. } => {
                let token_id = token_id as usize;
                token_id..token_id + 1
            }
        }
    }

    fn posting_columns(&self, with_position: bool) -> Vec<&'static str> {
        let mut base_columns = if self.is_legacy_layout() {
            vec![ROW_ID, FREQUENCY_COL]
        } else {
            vec![POSTING_COL]
        };
        if with_position {
            match self.positions_layout {
                PositionsLayout::None => {}
                PositionsLayout::LegacyPerDoc => base_columns.push(POSITION_COL),
                PositionsLayout::SharedStream(_) => {
                    base_columns.push(COMPRESSED_POSITION_COL);
                    base_columns.push(POSITION_BLOCK_OFFSET_COL);
                }
            }
        }
        if self.has_impacts {
            base_columns.push(IMPACT_COL);
        }
        base_columns
    }
}

/// Loop-invariant state for [`InvertedPartition::build_chunk_postings`]. The
/// metadata vecs are `Arc`d so each chunk's blocking build shares them cheaply.
struct ChunkBuildState {
    offsets: Option<Arc<Vec<usize>>>,
    max_scores: Option<Arc<Vec<f32>>>,
    lengths: Option<Arc<Vec<u32>>>,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    positions_layout: PositionsLayout,
}

/// Chunk-invariant inputs to [`InvertedPartition::build_prewarm_posting_lists_chunk`]:
/// the per-partition codec/layout and the (shared, whole-partition) metadata
/// slices indexed by global token id. These don't change across chunks.
struct PrewarmBuildCtx<'a> {
    max_scores: Option<&'a [f32]>,
    lengths: Option<&'a [u32]>,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    positions_layout: PositionsLayout,
}

/// Per-chunk inputs to [`InvertedPartition::build_prewarm_posting_lists_chunk`]:
/// the token sub-range `[tok_start, tok_start + token_count)` and, for legacy
/// v1, the rebased offset slice plus the chunk's end row.
struct PrewarmChunk<'a> {
    tok_start: usize,
    token_count: usize,
    /// Legacy v1 only: `offsets[tok_start..tok_start+token_count]` (no sentinel).
    offsets: Option<&'a [usize]>,
    /// Legacy v1 only: global row at which this chunk's posting rows end.
    end_row: usize,
}

/// New type just to allow Positions implement DeepSizeOf so it can be put
/// in the cache.
#[derive(Clone)]
pub struct Positions(pub(super) CompressedPositionStorage);

/// Slice-aware cache-size charge for the Arrow array shapes stored in posting
/// caches. [`Array::get_buffer_memory_size`] reports the full capacity of shared
/// backing buffers; cached posting lists often reference only a small slice of a
/// group read. Count the referenced span for the known posting-list types and
/// fall back to Arrow's full-buffer size for anything else.
fn sliced_cache_bytes(array: &dyn Array) -> usize {
    let validity_bytes = array
        .nulls()
        .map(|nulls| nulls.len().div_ceil(8))
        .unwrap_or(0);
    match array.data_type() {
        DataType::LargeBinary => {
            let array = array.as_binary::<i64>();
            let data_bytes = if array.is_empty() {
                0
            } else {
                let offsets = array.value_offsets();
                (offsets[array.len()] - offsets[0]) as usize
            };
            data_bytes + (array.len() + 1) * std::mem::size_of::<i64>() + validity_bytes
        }
        DataType::List(_) => {
            let array = array.as_list::<i32>();
            let (child_start, child_end) = if array.is_empty() {
                (0, 0)
            } else {
                let offsets = array.value_offsets();
                (offsets[0] as usize, offsets[array.len()] as usize)
            };
            let offset_bytes = (array.len() + 1) * std::mem::size_of::<i32>();
            let child = array.values().slice(child_start, child_end - child_start);
            offset_bytes + validity_bytes + sliced_cache_bytes(child.as_ref())
        }
        // Fixed-width primitives hold exactly `len * width` bytes regardless of
        // buffer capacity, so this is already slice-aware. Any other type falls
        // back to the full-buffer size.
        other => match other.primitive_width() {
            Some(width) => array.len() * width + validity_bytes,
            None => array.get_buffer_memory_size(),
        },
    }
}

impl DeepSizeOf for Positions {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

// Cache key implementations for type-safe cache access
#[derive(Debug, Clone)]
pub struct PostingListKey {
    pub token_id: u32,
}

impl CacheKey for PostingListKey {
    type ValueType = PostingList;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("postings-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "PostingList"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-list-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<PostingList>())
    }
}

/// Cache key for a group of consecutive posting lists stored as a single
/// entry, covering rows `[start, end)` (issue #7040). The range, not a token
/// id, is the key so a runtime group-size change simply misses old entries
/// instead of serving a differently-shaped group.
#[derive(Debug, Clone)]
pub struct PostingListGroupKey {
    pub start: u32,
    pub end: u32,
}

impl CacheKey for PostingListGroupKey {
    type ValueType = PostingListGroup;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("postings-{}-{}", self.start, self.end).into()
    }

    fn type_name() -> &'static str {
        "PostingListGroup"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-list-group-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.start);
        builder.write_u32(self.end);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<PostingListGroup>())
    }
}

/// Internal cache-key decorator that isolates impact-bearing posting values
/// without changing the source-compatible public posting key structs.
#[derive(Debug, Clone)]
struct ImpactAwareCacheKey<K> {
    inner: K,
    has_impacts: bool,
}

impl<K: CacheKey> CacheKey for ImpactAwareCacheKey<K> {
    type ValueType = K::ValueType;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        if self.has_impacts {
            format!("{}-impacts", self.inner.key()).into()
        } else {
            self.inner.key()
        }
    }

    fn type_name() -> &'static str {
        K::type_name()
    }

    fn stable_type_id() -> &'static str {
        K::stable_type_id()
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.impact-aware-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        let inner_schema = K::schema();
        builder.write_str(K::stable_type_id());
        builder.write_str(inner_schema.id());
        builder.write_u32(inner_schema.version());
        builder.write_variant(if self.has_impacts { 1 } else { 0 });
        self.inner.write_key(builder);
    }

    fn codec() -> Option<CacheCodec> {
        K::codec()
    }
}

fn posting_list_cache_key(token_id: u32, has_impacts: bool) -> ImpactAwareCacheKey<PostingListKey> {
    ImpactAwareCacheKey {
        inner: PostingListKey { token_id },
        has_impacts,
    }
}

fn posting_list_group_cache_key(
    start: u32,
    end: u32,
    has_impacts: bool,
) -> ImpactAwareCacheKey<PostingListGroupKey> {
    ImpactAwareCacheKey {
        inner: PostingListGroupKey { start, end },
        has_impacts,
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
struct PostingMetadataValue {
    max_score: f32,
    length: u32,
}

#[derive(Debug, Clone)]
struct PostingMetadataKey {
    token_id: u32,
}

impl CacheKey for PostingMetadataKey {
    type ValueType = PostingMetadataValue;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("posting-metadata-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "PostingMetadata"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.posting-metadata-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }
}

#[derive(Debug, Clone)]
pub struct PositionKey {
    pub token_id: u32,
}

impl CacheKey for PositionKey {
    type ValueType = Positions;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("positions-{}", self.token_id).into()
    }

    fn type_name() -> &'static str {
        "Position"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.inverted.position-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.token_id);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<Positions>())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompressedPositionStorage {
    LegacyPerDoc(ListArray),
    SharedStream(SharedPositionStream),
}

impl DeepSizeOf for CompressedPositionStorage {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::LegacyPerDoc(positions) => sliced_cache_bytes(positions),
            Self::SharedStream(stream) => stream.size(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SharedPositionStream {
    codec: PositionStreamCodec,
    block_offsets: Arc<[u32]>,
    // Stored with shared ownership so cache hits can clone position streams
    // without copying either offsets or bytes.
    bytes: bytes::Bytes,
}

impl SharedPositionStream {
    pub fn new(codec: PositionStreamCodec, block_offsets: Vec<u32>, bytes: bytes::Bytes) -> Self {
        Self {
            codec,
            block_offsets: Arc::from(block_offsets.into_boxed_slice()),
            bytes,
        }
    }

    pub fn codec(&self) -> PositionStreamCodec {
        self.codec
    }

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }

    pub fn block_range(&self, index: usize) -> Range<usize> {
        let start = self.block_offsets[index] as usize;
        let end = self
            .block_offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        start..end
    }

    pub fn block(&self, index: usize) -> &[u8] {
        let range = self.block_range(index);
        &self.bytes[range]
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn block_offsets(&self) -> &[u32] {
        self.block_offsets.as_ref()
    }

    pub fn size(&self) -> usize {
        self.block_offsets.len() * std::mem::size_of::<u32>() + self.bytes.len()
    }
}

/// A group of consecutive posting lists held in a single cache entry, in row
/// order (issue #7040). Prewarmed modern groups without positions retain only
/// the compact Arrow posting rows read from `invert.lance`; max-score/length
/// metadata stays in the reader and is injected when a query creates a
/// posting-list view. Cold-loaded groups may keep inline metadata to preserve
/// one-read query loading. Legacy and position-bearing prewarm paths use the
/// materialized fallback.
#[derive(Debug, Clone)]
pub struct PostingListGroup {
    pub(super) storage: PostingListGroupStorage,
}

#[derive(Debug, Clone)]
pub(super) enum PostingListGroupStorage {
    Packed(PackedPostingListGroup),
    Materialized(Vec<PostingList>),
}

#[derive(Debug, Clone)]
pub(super) struct PackedPostingListGroup {
    pub(super) batch: RecordBatch,
    pub(super) posting_tail_codec: PostingTailCodec,
    pub(super) block_size: usize,
    first_docs_states: Arc<[OnceLock<Box<[u32]>>]>,
    first_docs_state_capacity_bytes: usize,
    impact_states: Option<Arc<[OnceLock<Box<ImpactSkipData>>]>>,
    impact_state_capacity_bytes: usize,
}

impl DeepSizeOf for PostingListGroup {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => group
                .batch
                .columns()
                .iter()
                .map(|column| sliced_cache_bytes(column.as_ref()))
                .sum::<usize>()
                .saturating_add(group.first_docs_state_capacity_bytes)
                .saturating_add(group.impact_state_capacity_bytes),
            PostingListGroupStorage::Materialized(posting_lists) => {
                posting_lists.deep_size_of_children(context)
            }
        }
    }
}

impl PostingListGroup {
    pub(super) fn new(posting_lists: Vec<PostingList>) -> Self {
        Self {
            storage: PostingListGroupStorage::Materialized(posting_lists),
        }
    }

    pub(super) fn new_packed(
        batch: RecordBatch,
        posting_tail_codec: PostingTailCodec,
    ) -> Result<Self> {
        let block_size = parse_posting_block_size(batch.schema_ref().metadata())?;
        Self::new_packed_with_block_size(batch, posting_tail_codec, block_size)
    }

    fn new_packed_with_block_size(
        batch: RecordBatch,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        validate_block_size(block_size)?;
        if let Some(encoded_block_size) = batch.schema_ref().metadata().get(POSTING_BLOCK_SIZE_KEY)
        {
            let encoded_block_size = encoded_block_size.parse::<usize>().map_err(|err| {
                Error::index(format!(
                    "invalid {POSTING_BLOCK_SIZE_KEY} metadata value {encoded_block_size:?}: {err}"
                ))
            })?;
            if encoded_block_size != block_size {
                return Err(Error::index(format!(
                    "packed posting group {POSTING_BLOCK_SIZE_KEY}={encoded_block_size} does not match block_size={block_size}"
                )));
            }
        }

        // Projected reads may drop schema metadata. Restore the reader's
        // validated block size before the batch enters the packed cache so IPC
        // roundtrips remain self-describing. Older packed cache entries omit
        // the key and enter through new_packed with the legacy 128-doc default.
        let mut schema = batch.schema().as_ref().clone();
        schema
            .metadata
            .insert(POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string());
        let batch = batch.with_schema(Arc::new(schema))?;
        let postings = batch
            .column_by_name(POSTING_COL)
            .and_then(|column| column.as_list_opt::<i32>())
            .ok_or_else(|| {
                Error::index(format!(
                    "packed posting group column {POSTING_COL} must be List<LargeBinary>"
                ))
            })?;
        if postings.values().data_type() != &DataType::LargeBinary {
            return Err(Error::index(format!(
                "packed posting group column {POSTING_COL} must contain LargeBinary values, got {}",
                postings.values().data_type()
            )));
        }
        if postings.null_count() != 0 {
            return Err(Error::index(
                "packed posting group column must not contain nulls".to_string(),
            ));
        }
        let total_posting_blocks = (0..batch.num_rows())
            .map(|slot| postings.value_length(slot) as usize)
            .sum::<usize>();
        let first_docs_states: Arc<[OnceLock<Box<[u32]>>]> = (0..batch.num_rows())
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>()
            .into();
        // Reserve the compact per-slot state slab and the block-head arrays it
        // can lazily retain, so warming these derived values cannot grow the
        // cache beyond its admission charge.
        let first_docs_state_capacity_bytes = first_docs_states
            .len()
            .saturating_mul(std::mem::size_of::<OnceLock<Box<[u32]>>>())
            .saturating_add(total_posting_blocks.saturating_mul(std::mem::size_of::<u32>()));
        let (impact_states, impact_state_capacity_bytes) = if let Some(impacts) =
            batch.column_by_name(IMPACT_COL)
        {
            let impacts = impacts.as_list_opt::<i32>().ok_or_else(|| {
                Error::index(format!(
                    "packed posting group column {IMPACT_COL} must be List<LargeBinary>"
                ))
            })?;
            if impacts.values().data_type() != &DataType::LargeBinary {
                return Err(Error::index(format!(
                    "packed posting group column {IMPACT_COL} must contain LargeBinary values, got {}",
                    impacts.values().data_type()
                )));
            }
            if impacts.null_count() != 0 {
                return Err(Error::index(format!(
                    "packed posting group column {IMPACT_COL} must not contain nulls"
                )));
            }
            let mut derived_cache_bytes = 0usize;
            for slot in 0..batch.num_rows() {
                let posting_blocks = postings.value_length(slot) as usize;
                let impact_entries = impacts.value_length(slot) as usize;
                let expected_impact_entries =
                    posting_blocks.saturating_add(posting_blocks.div_ceil(IMPACT_LEVEL1_BLOCKS));
                if impact_entries != expected_impact_entries {
                    return Err(Error::index(format!(
                        "packed posting group impact slot {slot} has {impact_entries} entries, expected {expected_impact_entries} for {posting_blocks} posting blocks"
                    )));
                }
                derived_cache_bytes = derived_cache_bytes.saturating_add(
                    ImpactSkipData::derived_cache_bytes_for_entries(impact_entries),
                );
            }

            let states: Arc<[OnceLock<Box<ImpactSkipData>>]> = (0..batch.num_rows())
                .map(|_| OnceLock::new())
                .collect::<Vec<_>>()
                .into();
            // Account up front for every allocation that the lazy states can
            // eventually retain. The impact entry bytes themselves remain in
            // `batch` and are already charged exactly once above.
            let per_slot_bytes = std::mem::size_of::<OnceLock<Box<ImpactSkipData>>>()
                .saturating_add(std::mem::size_of::<ImpactSkipData>());
            let capacity_bytes = states
                .len()
                .saturating_mul(per_slot_bytes)
                .saturating_add(derived_cache_bytes);
            (Some(states), capacity_bytes)
        } else {
            (None, 0)
        };
        match (
            batch.column_by_name(MAX_SCORE_COL),
            batch.column_by_name(LENGTH_COL),
        ) {
            (None, None) => {}
            (Some(max_scores), Some(lengths)) => {
                let max_scores = max_scores
                    .as_primitive_opt::<Float32Type>()
                    .ok_or_else(|| {
                        Error::index(format!(
                            "packed posting group column {MAX_SCORE_COL} must be Float32"
                        ))
                    })?;
                let lengths = lengths.as_primitive_opt::<UInt32Type>().ok_or_else(|| {
                    Error::index(format!(
                        "packed posting group column {LENGTH_COL} must be UInt32"
                    ))
                })?;
                if max_scores.null_count() != 0 || lengths.null_count() != 0 {
                    return Err(Error::index(
                        "packed posting group metadata columns must not contain nulls".to_string(),
                    ));
                }
            }
            _ => {
                return Err(Error::index(format!(
                    "packed posting group must contain both {MAX_SCORE_COL} and {LENGTH_COL}, or neither"
                )));
            }
        }

        Ok(Self {
            storage: PostingListGroupStorage::Packed(PackedPostingListGroup {
                batch,
                posting_tail_codec,
                block_size,
                first_docs_states,
                first_docs_state_capacity_bytes,
                impact_states,
                impact_state_capacity_bytes,
            }),
        })
    }

    pub(super) fn len(&self) -> usize {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => group.batch.num_rows(),
            PostingListGroupStorage::Materialized(posting_lists) => posting_lists.len(),
        }
    }

    #[cfg(test)]
    pub(super) fn is_packed(&self) -> bool {
        matches!(&self.storage, PostingListGroupStorage::Packed(_))
    }

    fn needs_external_metadata(&self) -> bool {
        match &self.storage {
            PostingListGroupStorage::Packed(group) => {
                group.batch.column_by_name(MAX_SCORE_COL).is_none()
            }
            PostingListGroupStorage::Materialized(_) => false,
        }
    }

    /// Build an owned posting-list view for `slot`. Packed groups clone only
    /// Arrow array metadata; the compressed posting bytes remain shared with
    /// the group's `List<LargeBinary>` child buffers.
    pub(super) fn posting_list(
        &self,
        slot: usize,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<Option<PostingList>> {
        match &self.storage {
            PostingListGroupStorage::Materialized(posting_lists) => {
                Ok(posting_lists.get(slot).cloned())
            }
            PostingListGroupStorage::Packed(group) => {
                if slot >= group.batch.num_rows() {
                    return Ok(None);
                }
                let postings = group
                    .batch
                    .column_by_name(POSTING_COL)
                    .and_then(|column| column.as_list_opt::<i32>())
                    .ok_or_else(|| {
                        Error::index(format!(
                            "packed posting group column {POSTING_COL} must be List<LargeBinary>"
                        ))
                    })?;
                let blocks = postings.value(slot);
                let blocks = blocks.as_binary_opt::<i64>().ok_or_else(|| {
                    Error::index(format!(
                        "packed posting group slot {slot} is not LargeBinary"
                    ))
                })?;
                let max_score = match group.batch.column_by_name(MAX_SCORE_COL) {
                    Some(column) => column
                        .as_primitive_opt::<Float32Type>()
                        .expect("packed group metadata was validated at construction")
                        .value(slot),
                    None => max_score.ok_or_else(|| {
                        Error::index("packed posting group requires max-score metadata".to_string())
                    })?,
                };
                let length = match group.batch.column_by_name(LENGTH_COL) {
                    Some(column) => column
                        .as_primitive_opt::<UInt32Type>()
                        .expect("packed group metadata was validated at construction")
                        .value(slot),
                    None => length.ok_or_else(|| {
                        Error::index("packed posting group requires length metadata".to_string())
                    })?,
                };
                let impacts = match (
                    group.impact_states.as_ref(),
                    group.batch.column_by_name(IMPACT_COL),
                ) {
                    (Some(states), Some(column)) => {
                        let state = states.get(slot).ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group impact state missing slot {slot}"
                            ))
                        })?;
                        let impact_lists = column.as_list_opt::<i32>().ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group column {IMPACT_COL} must be List<LargeBinary>"
                            ))
                        })?;
                        let entries = impact_lists.value(slot);
                        let entries = entries.as_binary_opt::<i64>().ok_or_else(|| {
                            Error::index(format!(
                                "packed posting group impact slot {slot} is not LargeBinary"
                            ))
                        })?;
                        let impacts =
                            state.get_or_init(|| {
                                Box::new(ImpactSkipData::new(entries.clone(), blocks.len()).expect(
                                    "packed impact entry count was validated at construction",
                                ))
                            });
                        Some(impacts.as_ref().clone())
                    }
                    (None, None) => None,
                    _ => {
                        return Err(Error::internal(
                            "packed posting group impact column/state mismatch".to_string(),
                        ));
                    }
                };
                Ok(Some(PostingList::Compressed(
                    CompressedPostingList::new(
                        blocks.clone(),
                        max_score,
                        length,
                        group.posting_tail_codec,
                        group.block_size,
                        None,
                        impacts,
                    )
                    .with_packed_first_docs(group.first_docs_states.clone(), slot),
                )))
            }
        }
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
#[allow(clippy::large_enum_variant)]
pub enum PostingList {
    Plain(PlainPostingList),
    Compressed(CompressedPostingList),
}

impl PostingList {
    pub fn from_batch(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
    ) -> Result<Self> {
        let posting_tail_codec = parse_posting_tail_codec(batch.schema_ref().metadata())?;
        let block_size = parse_posting_block_size(batch.schema_ref().metadata())?;
        Self::from_batch_with_tail_codec(batch, max_score, length, posting_tail_codec, block_size)
    }

    pub fn from_batch_with_tail_codec(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<Self> {
        let positions_layout = if batch.column_by_name(COMPRESSED_POSITION_COL).is_some() {
            PositionsLayout::SharedStream(parse_shared_position_codec(
                batch.schema_ref().metadata(),
            )?)
        } else if batch.column_by_name(POSITION_COL).is_some() {
            PositionsLayout::LegacyPerDoc
        } else {
            PositionsLayout::None
        };
        Self::from_batch_with_tail_codec_and_positions_layout(
            batch,
            max_score,
            length,
            posting_tail_codec,
            block_size,
            positions_layout,
        )
    }

    fn from_batch_with_tail_codec_and_positions_layout(
        batch: &RecordBatch,
        max_score: Option<f32>,
        length: Option<u32>,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions_layout: PositionsLayout,
    ) -> Result<Self> {
        match batch.column_by_name(POSTING_COL) {
            Some(_) => {
                debug_assert!(max_score.is_some() && length.is_some());
                let shared_position_codec = match positions_layout {
                    PositionsLayout::SharedStream(codec) => Some(codec),
                    _ => None,
                };
                let posting = CompressedPostingList::from_batch(
                    batch,
                    max_score.unwrap(),
                    length.unwrap(),
                    posting_tail_codec,
                    block_size,
                    shared_position_codec,
                )?;
                Ok(Self::Compressed(posting))
            }
            None => {
                let posting = PlainPostingList::from_batch(batch, max_score);
                Ok(Self::Plain(posting))
            }
        }
    }

    pub fn iter(&self) -> PostingListIterator<'_> {
        PostingListIterator::new(self)
    }

    pub fn has_position(&self) -> bool {
        match self {
            Self::Plain(posting) => posting.positions.is_some(),
            Self::Compressed(posting) => posting.positions.is_some(),
        }
    }

    pub fn has_impacts(&self) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Compressed(posting) => posting.impacts.is_some(),
        }
    }

    pub fn set_positions(&mut self, positions: CompressedPositionStorage) {
        match self {
            Self::Plain(posting) => match positions {
                CompressedPositionStorage::LegacyPerDoc(positions) => {
                    posting.positions = Some(positions)
                }
                CompressedPositionStorage::SharedStream(_) => {
                    unreachable!("shared position stream is not supported for plain postings")
                }
            },
            Self::Compressed(posting) => {
                posting.positions = Some(positions);
            }
        }
    }

    pub fn take_positions(&mut self) -> Option<CompressedPositionStorage> {
        match self {
            Self::Plain(posting) => posting
                .positions
                .take()
                .map(CompressedPositionStorage::LegacyPerDoc),
            Self::Compressed(posting) => posting.positions.take(),
        }
    }

    pub fn max_score(&self) -> Option<f32> {
        match self {
            Self::Plain(posting) => posting.max_score,
            Self::Compressed(posting) => Some(posting.max_score),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Plain(posting) => posting.len(),
            Self::Compressed(posting) => posting.length as usize,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_builder(self, docs: &DocSet) -> PostingListBuilder {
        let posting_tail_codec = match &self {
            Self::Plain(_) => PostingTailCodec::Fixed32,
            Self::Compressed(posting) => posting.posting_tail_codec,
        };
        let block_size = match &self {
            Self::Plain(_) => LEGACY_BLOCK_SIZE,
            Self::Compressed(posting) => posting.block_size,
        };
        let mut builder = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            self.has_position(),
            posting_tail_codec,
            block_size,
        );
        match self {
            // legacy format
            Self::Plain(posting) => {
                // convert the posting list to the new format:
                // 1. map row ids to doc ids
                // 2. sort the posting list by doc ids
                struct Item {
                    doc_id: u32,
                    positions: PositionRecorder,
                }
                let doc_ids = docs
                    .row_ids
                    .iter()
                    .enumerate()
                    .map(|(doc_id, row_id)| (*row_id, doc_id as u32))
                    .collect::<HashMap<_, _>>();
                let mut items = Vec::with_capacity(posting.len());
                for (row_id, freq, positions) in posting.iter() {
                    let freq = freq as u32;
                    let positions = match positions {
                        Some(positions) => {
                            PositionRecorder::Position(positions.collect::<Vec<_>>().into())
                        }
                        None => PositionRecorder::Count(freq),
                    };
                    items.push(Item {
                        doc_id: doc_ids[&row_id],
                        positions,
                    });
                }
                items.sort_unstable_by_key(|item| item.doc_id);
                for item in items {
                    builder.add(item.doc_id, item.positions);
                }
            }
            Self::Compressed(posting) => {
                posting.iter().for_each(|(doc_id, freq, positions)| {
                    let positions = match positions {
                        Some(positions) => {
                            PositionRecorder::Position(positions.collect::<Vec<_>>().into())
                        }
                        None => PositionRecorder::Count(freq),
                    };
                    builder.add(doc_id, positions);
                });
            }
        }
        builder
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PlainPostingList {
    pub row_ids: ScalarBuffer<u64>,
    pub frequencies: ScalarBuffer<f32>,
    pub max_score: Option<f32>,
    pub positions: Option<ListArray>, // List of Int32
}

impl DeepSizeOf for PlainPostingList {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.row_ids.len() * std::mem::size_of::<u64>()
            + self.frequencies.len() * std::mem::size_of::<f32>()
            + self
                .positions
                .as_ref()
                .map(|positions| sliced_cache_bytes(positions))
                .unwrap_or(0)
    }
}

impl PlainPostingList {
    pub fn new(
        row_ids: ScalarBuffer<u64>,
        frequencies: ScalarBuffer<f32>,
        max_score: Option<f32>,
        positions: Option<ListArray>,
    ) -> Self {
        Self {
            row_ids,
            frequencies,
            max_score,
            positions,
        }
    }

    pub fn from_batch(batch: &RecordBatch, max_score: Option<f32>) -> Self {
        let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().values().clone();
        let frequencies = batch[FREQUENCY_COL]
            .as_primitive::<Float32Type>()
            .values()
            .clone();
        let positions = batch
            .column_by_name(POSITION_COL)
            .map(|col| col.as_list::<i32>().clone());

        Self::new(row_ids, frequencies, max_score, positions)
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> PlainPostingListIterator<'_> {
        Box::new(
            self.row_ids
                .iter()
                .zip(self.frequencies.iter())
                .enumerate()
                .map(|(idx, (doc_id, freq))| {
                    (
                        *doc_id,
                        *freq,
                        self.positions.as_ref().map(|p| {
                            let start = p.value_offsets()[idx] as usize;
                            let end = p.value_offsets()[idx + 1] as usize;
                            Box::new(
                                p.values().as_primitive::<Int32Type>().values()[start..end]
                                    .iter()
                                    .map(|pos| *pos as u32),
                            ) as _
                        }),
                    )
                }),
        )
    }

    #[inline]
    pub fn doc(&self, i: usize) -> LocatedDocInfo {
        LocatedDocInfo::new(self.row_ids[i], self.frequencies[i])
    }

    pub fn positions(&self, index: usize) -> Option<Arc<dyn Array>> {
        self.positions
            .as_ref()
            .map(|positions| positions.value(index))
    }

    pub fn max_score(&self) -> Option<f32> {
        self.max_score
    }

    pub fn row_id(&self, i: usize) -> u64 {
        self.row_ids[i]
    }
}

#[derive(Debug, Clone)]
enum FirstDocsState {
    Standalone(Arc<OnceLock<Box<[u32]>>>),
    Packed {
        states: Arc<[OnceLock<Box<[u32]>>]>,
        slot: usize,
    },
}

impl FirstDocsState {
    fn standalone() -> Self {
        Self::Standalone(Arc::new(OnceLock::new()))
    }

    fn state(&self) -> &OnceLock<Box<[u32]>> {
        match self {
            Self::Standalone(state) => state,
            Self::Packed { states, slot } => &states[*slot],
        }
    }

    fn get_or_init(&self, initialize: impl FnOnce() -> Box<[u32]>) -> &[u32] {
        self.state().get_or_init(initialize)
    }

    fn capacity_bytes(
        &self,
        block_count: usize,
        context: &mut lance_core::deepsize::Context,
    ) -> usize {
        if context.mark_seen(self.state() as *const _ as usize) {
            std::mem::size_of::<OnceLock<Box<[u32]>>>()
                .saturating_add(block_count.saturating_mul(std::mem::size_of::<u32>()))
        } else {
            0
        }
    }

    #[cfg(test)]
    fn shares_state_with(&self, other: &Self) -> bool {
        std::ptr::eq(self.state(), other.state())
    }
}

#[derive(Debug, Clone)]
pub struct CompressedPostingList {
    pub max_score: f32,
    pub length: u32,
    // each binary is a block of compressed data
    // that contains `block_size` doc ids and then `block_size` frequencies,
    // packed by the physical bitpacker matching that block size.
    pub blocks: LargeBinaryArray,
    pub posting_tail_codec: PostingTailCodec,
    pub block_size: usize,
    pub positions: Option<CompressedPositionStorage>,
    pub(crate) impacts: Option<ImpactSkipData>,
    // First doc id per block, baked lazily and shared across per-query clones
    // of the cached list. See `block_first_docs`.
    first_docs: FirstDocsState,
}

impl PartialEq for CompressedPostingList {
    fn eq(&self, other: &Self) -> bool {
        self.max_score == other.max_score
            && self.length == other.length
            && self.blocks == other.blocks
            && self.posting_tail_codec == other.posting_tail_codec
            && self.block_size == other.block_size
            && self.positions == other.positions
            && self.impacts == other.impacts
    }
}

impl DeepSizeOf for CompressedPostingList {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        sliced_cache_bytes(&self.blocks)
            + self
                .positions
                .as_ref()
                .map(|positions| positions.deep_size_of_children(context))
                .unwrap_or(0)
            + self
                .impacts
                .as_ref()
                .map(|impacts| {
                    sliced_cache_bytes(impacts.entries())
                        .saturating_add(impacts.derived_cache_bytes())
                })
                .unwrap_or(0)
            + self.first_docs.capacity_bytes(self.blocks.len(), context)
    }
}

impl CompressedPostingList {
    pub(crate) fn new(
        blocks: LargeBinaryArray,
        max_score: f32,
        length: u32,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        positions: Option<CompressedPositionStorage>,
        impacts: Option<ImpactSkipData>,
    ) -> Self {
        debug_assert!(block_size.is_power_of_two());
        Self {
            max_score,
            length,
            blocks,
            posting_tail_codec,
            block_size,
            positions,
            impacts,
            first_docs: FirstDocsState::standalone(),
        }
    }

    fn with_packed_first_docs(mut self, states: Arc<[OnceLock<Box<[u32]>>]>, slot: usize) -> Self {
        debug_assert!(slot < states.len());
        self.first_docs = FirstDocsState::Packed { states, slot };
        self
    }

    /// Block sizes are validated powers of two, so per-doc hot loops derive
    /// block indices with shift/mask instead of runtime division, which is
    /// measurably slower in the iterator advance path.
    #[inline]
    pub(crate) fn block_shift(&self) -> u32 {
        self.block_size.trailing_zeros()
    }

    #[inline]
    pub(crate) fn block_mask(&self) -> usize {
        self.block_size - 1
    }

    pub fn from_batch(
        batch: &RecordBatch,
        max_score: f32,
        length: u32,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        shared_position_codec: Option<PositionStreamCodec>,
    ) -> Result<Self> {
        debug_assert_eq!(batch.num_rows(), 1);
        let blocks = batch[POSTING_COL]
            .as_list::<i32>()
            .value(0)
            .as_binary::<i64>()
            .clone();
        let positions = if let Some(col) = batch.column_by_name(COMPRESSED_POSITION_COL) {
            let bytes = bytes::Bytes::from(col.as_binary::<i64>().value(0).to_vec());
            let block_offsets = batch[POSITION_BLOCK_OFFSET_COL]
                .as_list::<i32>()
                .value(0)
                .as_primitive::<UInt32Type>()
                .values()
                .to_vec();
            let codec = shared_position_codec.unwrap_or_else(|| {
                parse_shared_position_codec(batch.schema_ref().metadata())
                    .expect("shared position stream codec metadata should be valid")
            });
            Some(CompressedPositionStorage::SharedStream(
                SharedPositionStream::new(codec, block_offsets, bytes),
            ))
        } else {
            batch.column_by_name(POSITION_COL).map(|col| {
                CompressedPositionStorage::LegacyPerDoc(
                    col.as_list::<i32>().value(0).as_list::<i32>().clone(),
                )
            })
        };
        let impacts = batch
            .column_by_name(IMPACT_COL)
            .map(|col| {
                let entries = col.as_list::<i32>().value(0).as_binary::<i64>().clone();
                ImpactSkipData::new(entries, blocks.len())
            })
            .transpose()?;

        Ok(Self {
            max_score,
            length,
            blocks,
            posting_tail_codec,
            block_size,
            positions,
            impacts,
            first_docs: FirstDocsState::standalone(),
        })
    }

    pub fn iter(&self) -> CompressedPostingListIterator {
        CompressedPostingListIterator::new(
            self.length as usize,
            self.blocks.clone(),
            self.posting_tail_codec,
            self.positions.clone(),
            self.block_size,
        )
    }

    pub fn block_max_score(&self, block_idx: usize) -> f32 {
        // 256-document blocks store no per-block max score: their impact
        // skip data supplies the tight per-block bound, so callers on that
        // path never reach here. Fall back to the list-level max, which is
        // still a valid (looser) bound for any block.
        if super::encoding::posting_block_score_prefix_len(self.block_size) == 0 {
            return self.max_score;
        }
        let block = self.blocks.value(block_idx);
        block[0..4].try_into().map(f32::from_le_bytes).unwrap()
    }

    #[inline]
    pub fn block_least_doc_id(&self, block_idx: usize) -> u32 {
        self.block_first_docs()[block_idx]
    }

    /// First doc id of every block, decoded once per cached list and shared by
    /// the per-query clones. Block boundary lookups (window bounds, block
    /// binary searches) are hot enough that re-reading the block headers —
    /// and re-decoding the tail block — shows up in profiles.
    pub(crate) fn block_first_docs(&self) -> &[u32] {
        self.first_docs.get_or_init(|| {
            (0..self.blocks.len())
                .map(|block_idx| {
                    let block = self.blocks.value(block_idx);
                    let remainder = self.length as usize % self.block_size;
                    if block_idx + 1 == self.blocks.len() && remainder > 0 {
                        return super::encoding::read_posting_tail_first_doc(
                            block,
                            self.posting_tail_codec,
                            self.block_size,
                        );
                    }
                    let prefix = super::encoding::posting_block_score_prefix_len(self.block_size);
                    block[prefix..prefix + 4]
                        .try_into()
                        .map(u32::from_le_bytes)
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        })
    }

    #[cfg(test)]
    fn shares_first_docs_with(&self, other: &Self) -> bool {
        self.first_docs.shares_state_with(&other.first_docs)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct EncodedBlocks {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}

impl EncodedBlocks {
    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn size(&self) -> usize {
        self.offsets.capacity() * std::mem::size_of::<u32>() + self.bytes.capacity()
    }

    fn push_full_block(&mut self, doc_ids: &[u32], frequencies: &[u32]) -> Result<usize> {
        let start = self.bytes.len();
        self.offsets.push(start as u32);
        super::encoding::encode_full_posting_block_into(doc_ids, frequencies, &mut self.bytes)?;
        Ok(self.bytes.len() - start)
    }

    fn block(&self, index: usize) -> &[u8] {
        let (start, end) = self.block_range(index);
        &self.bytes[start..end]
    }

    fn block_range(&self, index: usize) -> (usize, usize) {
        let start = self.offsets[index] as usize;
        let end = self
            .offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        (start, end)
    }

    fn set_block_score(&mut self, index: usize, score: f32) {
        let (start, _) = self.block_range(index);
        self.bytes[start..start + 4].copy_from_slice(&score.to_le_bytes());
    }

    fn append_remainder_block_with_codec(
        &mut self,
        doc_ids: &[u32],
        frequencies: &[u32],
        codec: PostingTailCodec,
        block_size: usize,
    ) -> Result<()> {
        self.offsets.push(self.bytes.len() as u32);
        super::encoding::encode_remainder_posting_block_into(
            doc_ids,
            frequencies,
            codec,
            block_size,
            &mut self.bytes,
        )
    }

    fn into_array(mut self) -> LargeBinaryArray {
        let mut offsets = Vec::with_capacity(self.offsets.len() + 1);
        offsets.extend(self.offsets.into_iter().map(i64::from));
        offsets.push(self.bytes.len() as i64);
        LargeBinaryArray::new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Buffer::from_vec(std::mem::take(&mut self.bytes)),
            None,
        )
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|index| self.block(index))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct EncodedPositionBlocks {
    offsets: Vec<u32>,
    bytes: Vec<u8>,
}

impl EncodedPositionBlocks {
    fn size(&self) -> usize {
        self.offsets.capacity() * std::mem::size_of::<u32>() + self.bytes.capacity()
    }

    fn block(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self
            .offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.bytes.len());
        &self.bytes[start..end]
    }

    fn push_encoded_block(&mut self, block: &[u8]) -> usize {
        let start = self.bytes.len();
        self.offsets.push(start as u32);
        self.bytes.extend_from_slice(block);
        self.bytes.len() - start
    }

    fn into_stream(self) -> SharedPositionStream {
        SharedPositionStream::new(
            PositionStreamCodec::PackedDelta,
            self.offsets,
            bytes::Bytes::from(self.bytes),
        )
    }
}

#[derive(Debug)]
pub struct PostingListBuilder {
    with_positions: bool,
    posting_tail_codec: PostingTailCodec,
    encoded_blocks: Option<Box<EncodedBlocks>>,
    encoded_position_blocks: Option<Box<EncodedPositionBlocks>>,
    tail_entries: Vec<RawDocInfo>,
    tail_positions: PositionBlockBuilder,
    open_doc_id: Option<u32>,
    open_doc_frequency: u32,
    open_doc_last_position: Option<u32>,
    block_size: usize,
    memory_size_bytes: u32,
    len: u32,
}

pub(super) struct PostingListBatchBuilder {
    schema: SchemaRef,
    postings: ListBuilder<LargeBinaryBuilder>,
    impacts: Option<ListBuilder<LargeBinaryBuilder>>,
    max_scores: Float32Builder,
    lengths: UInt32Builder,
    positions: BatchPositionsBuilder,
    len: usize,
}

enum BatchPositionsBuilder {
    None,
    Legacy(ListBuilder<ListBuilder<LargeBinaryBuilder>>),
    Shared {
        bytes: LargeBinaryBuilder,
        block_offsets: ListBuilder<UInt32Builder>,
    },
}

struct PostingListParts<'a> {
    with_positions: bool,
    posting_tail_codec: PostingTailCodec,
    block_size: usize,
    length: usize,
    encoded_blocks: EncodedBlocks,
    encoded_position_blocks: EncodedPositionBlocks,
    tail_entries: &'a [RawDocInfo],
    tail_position_block: Option<Vec<u8>>,
}

impl PostingListBatchBuilder {
    pub fn new(
        schema: SchemaRef,
        with_positions: bool,
        format_version: InvertedListFormatVersion,
        capacity: usize,
    ) -> Self {
        let positions = if !with_positions {
            BatchPositionsBuilder::None
        } else if format_version.uses_shared_position_stream() {
            BatchPositionsBuilder::Shared {
                bytes: LargeBinaryBuilder::with_capacity(capacity, 0),
                block_offsets: ListBuilder::with_capacity(UInt32Builder::new(), capacity),
            }
        } else {
            BatchPositionsBuilder::Legacy(ListBuilder::with_capacity(
                ListBuilder::new(LargeBinaryBuilder::new()),
                capacity,
            ))
        };
        let impacts = schema
            .field_with_name(IMPACT_COL)
            .ok()
            .map(|_| ListBuilder::with_capacity(LargeBinaryBuilder::new(), capacity));
        Self {
            schema,
            postings: ListBuilder::with_capacity(LargeBinaryBuilder::new(), capacity),
            impacts,
            max_scores: Float32Builder::with_capacity(capacity),
            lengths: UInt32Builder::with_capacity(capacity),
            positions,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn append(
        &mut self,
        compressed: LargeBinaryArray,
        impacts: Option<&ImpactSkipData>,
        max_score: f32,
        length: u32,
        positions: Option<&CompressedPositionStorage>,
    ) -> Result<()> {
        {
            let values = self.postings.values();
            for index in 0..compressed.len() {
                values.append_value(compressed.value(index));
            }
        }
        self.postings.append(true);
        if let Some(impacts_builder) = &mut self.impacts {
            let impacts = impacts.ok_or_else(|| {
                Error::index(format!(
                    "impacts builder missing impact data for posting length {}",
                    length
                ))
            })?;
            let values = impacts_builder.values();
            for index in 0..impacts.entries().len() {
                values.append_value(impacts.entries().value(index));
            }
            impacts_builder.append(true);
        }
        self.max_scores.append_value(max_score);
        self.lengths.append_value(length);

        match &mut self.positions {
            BatchPositionsBuilder::None => {}
            BatchPositionsBuilder::Shared {
                bytes,
                block_offsets,
            } => {
                let positions = positions.ok_or_else(|| {
                    Error::index(format!(
                        "positions builder missing position data for posting length {}",
                        length
                    ))
                })?;
                let CompressedPositionStorage::SharedStream(positions) = positions else {
                    return Err(Error::index(
                        "shared positions builder received legacy positions".to_owned(),
                    ));
                };
                bytes.append_value(positions.bytes());
                let offsets_builder = block_offsets.values();
                for &offset in positions.block_offsets() {
                    offsets_builder.append_value(offset);
                }
                block_offsets.append(true);
            }
            BatchPositionsBuilder::Legacy(position_lists) => {
                let positions = positions.ok_or_else(|| {
                    Error::index(format!(
                        "positions builder missing position data for posting length {}",
                        length
                    ))
                })?;
                let CompressedPositionStorage::LegacyPerDoc(positions) = positions else {
                    return Err(Error::index(
                        "legacy positions builder received shared position stream".to_owned(),
                    ));
                };
                let docs_builder = position_lists.values();
                for doc_idx in 0..positions.len() {
                    let doc_positions = positions.value(doc_idx);
                    let compressed_positions = doc_positions.as_binary::<i64>();
                    for block_idx in 0..compressed_positions.len() {
                        docs_builder
                            .values()
                            .append_value(compressed_positions.value(block_idx));
                    }
                    docs_builder.append(true);
                }
                position_lists.append(true);
            }
        }

        self.len += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let mut columns = vec![
            Arc::new(self.postings.finish()) as ArrayRef,
            Arc::new(self.max_scores.finish()) as ArrayRef,
            Arc::new(self.lengths.finish()) as ArrayRef,
        ];
        if let Some(impacts) = &mut self.impacts {
            columns.push(Arc::new(impacts.finish()) as ArrayRef);
        }
        match &mut self.positions {
            BatchPositionsBuilder::None => {}
            BatchPositionsBuilder::Legacy(position_lists) => {
                columns.push(Arc::new(position_lists.finish()) as ArrayRef);
            }
            BatchPositionsBuilder::Shared {
                bytes,
                block_offsets,
            } => {
                columns.push(Arc::new(bytes.finish()) as ArrayRef);
                columns.push(Arc::new(block_offsets.finish()) as ArrayRef);
            }
        }
        self.len = 0;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(Error::from)
    }
}

impl PostingListBuilder {
    pub fn size(&self) -> u64 {
        self.memory_size_bytes as u64
    }

    pub fn has_positions(&self) -> bool {
        self.with_positions
    }

    pub fn new(with_position: bool) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            current_fts_format_version().posting_tail_codec(),
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_posting_tail_codec(
        with_position: bool,
        posting_tail_codec: PostingTailCodec,
    ) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            posting_tail_codec,
            LEGACY_BLOCK_SIZE,
        )
    }

    pub fn new_with_block_size(with_position: bool, block_size: usize) -> Self {
        Self::new_with_posting_tail_codec_and_block_size(
            with_position,
            current_fts_format_version().posting_tail_codec(),
            block_size,
        )
    }

    pub fn new_with_posting_tail_codec_and_block_size(
        with_position: bool,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
    ) -> Self {
        validate_block_size(block_size).expect("invalid posting list block size");
        Self {
            with_positions: with_position,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            len: 0,
            memory_size_bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> std::vec::IntoIter<(u32, u32, Option<Vec<u32>>)> {
        self.collect_entries().into_iter()
    }

    pub fn for_each_entry<E>(
        &self,
        mut visit: impl FnMut(u32, u32, Option<Vec<u32>>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let mut doc_ids = Vec::with_capacity(self.block_size);
        let mut frequencies = Vec::with_capacity(self.block_size);
        let mut decoded_positions = Vec::new();
        let mut position_block_index = 0usize;

        if let Some(encoded_blocks) = self.encoded_blocks.as_deref() {
            for block in encoded_blocks.iter() {
                doc_ids.clear();
                frequencies.clear();
                super::encoding::decode_full_posting_block(
                    block,
                    &mut doc_ids,
                    &mut frequencies,
                    self.block_size,
                );
                decoded_positions.clear();
                if self.with_positions {
                    let position_blocks = self
                        .encoded_position_blocks
                        .as_deref()
                        .expect("positions must exist for posting list");
                    super::encoding::decode_position_stream_block(
                        position_blocks.block(position_block_index),
                        &frequencies,
                        PositionStreamCodec::PackedDelta,
                        &mut decoded_positions,
                    )
                    .expect("position stream decoding should succeed");
                    position_block_index += 1;
                }
                let mut offset = 0usize;
                for (doc_id, frequency) in doc_ids.iter().copied().zip(frequencies.iter().copied())
                {
                    let positions = self.with_positions.then(|| {
                        let end = offset + frequency as usize;
                        let doc_positions = decoded_positions[offset..end].to_vec();
                        offset = end;
                        doc_positions
                    });
                    visit(doc_id, frequency, positions)?;
                }
            }
        }

        let mut decoded_tail_positions = Vec::new();
        if self.with_positions && !self.tail_entries.is_empty() {
            let tail_frequencies = self
                .tail_entries
                .iter()
                .map(|entry| entry.frequency)
                .collect::<Vec<_>>();
            self.tail_positions
                .decode_into(tail_frequencies.as_slice(), &mut decoded_tail_positions)
                .expect("tail position stream decoding should succeed");
        }
        let mut tail_offset = 0usize;
        for entry in &self.tail_entries {
            let positions = self.with_positions.then(|| {
                let end = tail_offset + entry.frequency as usize;
                let doc_positions = decoded_tail_positions[tail_offset..end].to_vec();
                tail_offset = end;
                doc_positions
            });
            visit(entry.doc_id, entry.frequency, positions)?;
        }

        Ok(())
    }

    pub fn add(&mut self, doc_id: u32, term_positions: PositionRecorder) {
        debug_assert!(
            self.open_doc_id.is_none(),
            "cannot add closed doc while a positions doc is still open"
        );
        let tail_entries_capacity_before = self.tail_entries.capacity();
        self.tail_entries
            .push(RawDocInfo::new(doc_id, term_positions.len()));
        let tail_entries_capacity_after = self.tail_entries.capacity();
        if tail_entries_capacity_after > tail_entries_capacity_before {
            self.add_memory_bytes(
                (tail_entries_capacity_after - tail_entries_capacity_before)
                    * std::mem::size_of::<RawDocInfo>(),
            );
        }
        if let PositionRecorder::Position(positions_in_doc) = term_positions {
            debug_assert!(self.with_positions);
            let old_size = self.tail_positions.size();
            self.tail_positions
                .append_doc_positions(positions_in_doc.as_slice())
                .expect("position stream encoding should succeed");
            self.adjust_tail_positions_size(old_size);
        }
        self.len += 1;

        if self.tail_entries.len() == self.block_size {
            self.flush_tail_block()
                .expect("posting list block compression should succeed");
        }
    }

    pub fn add_occurrence(&mut self, doc_id: u32, position: u32) -> Result<bool> {
        if !self.with_positions {
            return Err(Error::index(
                "cannot append streamed positions to a posting list without positions".to_owned(),
            ));
        }

        match self.open_doc_id {
            Some(open_doc_id) if open_doc_id == doc_id => {
                let old_size = self.tail_positions.size();
                self.tail_positions
                    .append_position(position, self.open_doc_last_position)?;
                self.adjust_tail_positions_size(old_size);
                self.open_doc_frequency += 1;
                self.open_doc_last_position = Some(position);
                Ok(false)
            }
            Some(open_doc_id) => Err(Error::index(format!(
                "posting list received doc {} before finishing open doc {}",
                doc_id, open_doc_id
            ))),
            None => {
                let old_size = self.tail_positions.size();
                self.tail_positions.append_position(position, None)?;
                self.adjust_tail_positions_size(old_size);
                self.open_doc_id = Some(doc_id);
                self.open_doc_frequency = 1;
                self.open_doc_last_position = Some(position);
                self.len += 1;
                Ok(true)
            }
        }
    }

    pub fn finish_open_doc(&mut self, doc_id: u32) -> Result<()> {
        if !self.with_positions {
            return Ok(());
        }
        match self.open_doc_id {
            Some(open_doc_id) if open_doc_id == doc_id => {
                let tail_entries_capacity_before = self.tail_entries.capacity();
                self.tail_entries
                    .push(RawDocInfo::new(doc_id, self.open_doc_frequency));
                let tail_entries_capacity_after = self.tail_entries.capacity();
                if tail_entries_capacity_after > tail_entries_capacity_before {
                    self.add_memory_bytes(
                        (tail_entries_capacity_after - tail_entries_capacity_before)
                            * std::mem::size_of::<RawDocInfo>(),
                    );
                }
                self.open_doc_id = None;
                self.open_doc_frequency = 0;
                self.open_doc_last_position = None;
                if self.tail_entries.len() == self.block_size {
                    self.flush_tail_block()?;
                }
                Ok(())
            }
            Some(open_doc_id) => Err(Error::index(format!(
                "attempted to finish doc {} while doc {} is still open",
                doc_id, open_doc_id
            ))),
            None => Ok(()),
        }
    }

    fn collect_entries(&self) -> Vec<(u32, u32, Option<Vec<u32>>)> {
        let mut entries = Vec::with_capacity(self.len());
        self.for_each_entry(|doc_id, frequency, positions| {
            entries.push((doc_id, frequency, positions));
            Ok::<(), ()>(())
        })
        .expect("collecting posting list entries should not fail");
        entries
    }

    fn encoded_blocks_mut(&mut self) -> &mut EncodedBlocks {
        if self.encoded_blocks.is_none() {
            self.encoded_blocks = Some(Box::default());
            self.add_memory_bytes(std::mem::size_of::<EncodedBlocks>());
        }
        self.encoded_blocks
            .as_deref_mut()
            .expect("encoded blocks must exist")
    }

    fn encoded_position_blocks_mut(&mut self) -> &mut EncodedPositionBlocks {
        if self.encoded_position_blocks.is_none() {
            self.encoded_position_blocks = Some(Box::default());
            self.add_memory_bytes(std::mem::size_of::<EncodedPositionBlocks>());
        }
        self.encoded_position_blocks
            .as_deref_mut()
            .expect("encoded position blocks must exist")
    }

    fn flush_tail_block(&mut self) -> Result<()> {
        if self.tail_entries.is_empty() {
            return Ok(());
        }
        debug_assert!(
            self.open_doc_id.is_none(),
            "cannot flush a posting block while a document is still open"
        );
        debug_assert_eq!(self.tail_entries.len(), self.block_size);
        let doc_ids = self
            .tail_entries
            .iter()
            .map(|entry| entry.doc_id)
            .collect::<Vec<_>>();
        let frequencies = self
            .tail_entries
            .iter()
            .map(|entry| entry.frequency)
            .collect::<Vec<_>>();
        let encoded_blocks_size_before = self
            .encoded_blocks
            .as_ref()
            .map(|encoded_blocks| encoded_blocks.size())
            .unwrap_or(0usize);
        self.encoded_blocks_mut()
            .push_full_block(&doc_ids, &frequencies)?;
        let encoded_blocks_size_after = self
            .encoded_blocks
            .as_ref()
            .map(|encoded_blocks| encoded_blocks.size())
            .unwrap_or(0usize);
        if encoded_blocks_size_after > encoded_blocks_size_before {
            self.add_memory_bytes(encoded_blocks_size_after - encoded_blocks_size_before);
        }
        if self.with_positions {
            let encoded_positions_size_before = self
                .encoded_position_blocks
                .as_ref()
                .map(|encoded| encoded.size())
                .unwrap_or(0usize);
            let released_tail_positions_bytes = self.tail_positions.size();
            let tail_position_block = std::mem::take(&mut self.tail_positions).finish();
            self.encoded_position_blocks_mut()
                .push_encoded_block(tail_position_block.as_slice());
            let encoded_positions_size_after = self
                .encoded_position_blocks
                .as_ref()
                .map(|encoded| encoded.size())
                .unwrap_or(0usize);
            if released_tail_positions_bytes > 0 {
                self.subtract_memory_bytes(released_tail_positions_bytes);
            }
            if encoded_positions_size_after > encoded_positions_size_before {
                self.add_memory_bytes(encoded_positions_size_after - encoded_positions_size_before);
            }
        }
        self.tail_entries.clear();
        Ok(())
    }

    fn adjust_tail_positions_size(&mut self, old_size: usize) {
        let new_size = self.tail_positions.size();
        if new_size > old_size {
            self.add_memory_bytes(new_size - old_size);
        } else if old_size > new_size {
            self.subtract_memory_bytes(old_size - new_size);
        }
    }

    fn add_memory_bytes(&mut self, bytes: usize) {
        self.memory_size_bytes = self
            .memory_size_bytes
            .checked_add(
                u32::try_from(bytes).expect("posting list memory size delta overflowed u32"),
            )
            .expect("posting list memory size overflowed u32");
    }

    fn subtract_memory_bytes(&mut self, bytes: usize) {
        self.memory_size_bytes = self
            .memory_size_bytes
            .checked_sub(
                u32::try_from(bytes).expect("posting list memory size delta overflowed u32"),
            )
            .expect("posting list memory size underflowed u32");
    }

    fn build_position_columns(
        positions: Option<CompressedPositionStorage>,
    ) -> Result<Vec<ArrayRef>> {
        let Some(positions) = positions else {
            return Ok(Vec::new());
        };
        match positions {
            CompressedPositionStorage::LegacyPerDoc(positions) => {
                Ok(vec![Arc::new(ListArray::try_new(
                    Arc::new(Field::new("item", positions.data_type().clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, positions.len() as i32])),
                    Arc::new(positions) as ArrayRef,
                    None,
                )?) as ArrayRef])
            }
            CompressedPositionStorage::SharedStream(positions) => {
                let mut columns = Vec::with_capacity(2);
                columns.push(
                    Arc::new(LargeBinaryArray::from(vec![Some(positions.bytes())])) as ArrayRef,
                );

                let mut offsets_builder = ListBuilder::new(UInt32Builder::new());
                for &offset in positions.block_offsets() {
                    offsets_builder.values().append_value(offset);
                }
                offsets_builder.append(true);
                columns.push(Arc::new(offsets_builder.finish()) as ArrayRef);
                Ok(columns)
            }
        }
    }

    fn build_batch(
        self,
        compressed: LargeBinaryArray,
        impacts: Option<ImpactSkipData>,
        max_score: f32,
        schema: SchemaRef,
        positions: Option<CompressedPositionStorage>,
    ) -> Result<RecordBatch> {
        let length = self.len();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, compressed.len() as i32]));
        let mut columns = vec![
            Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", datatypes::DataType::LargeBinary, true)),
                offsets,
                Arc::new(compressed),
                None,
            )?) as ArrayRef,
            Arc::new(Float32Array::from_iter_values(std::iter::once(max_score))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(std::iter::once(
                length as u32,
            ))) as ArrayRef,
        ];
        if schema.field_with_name(IMPACT_COL).is_ok() {
            let impacts = impacts.ok_or_else(|| {
                Error::index(format!(
                    "impact column requested without impact data for posting length {}",
                    length
                ))
            })?;
            let impact_offsets =
                OffsetBuffer::new(ScalarBuffer::from(vec![0, impacts.entries().len() as i32]));
            columns.push(Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", datatypes::DataType::LargeBinary, true)),
                impact_offsets,
                Arc::new(impacts.entries().clone()),
                None,
            )?) as ArrayRef);
        }
        columns.extend(Self::build_position_columns(positions)?);

        let batch = RecordBatch::try_new(schema, columns)?;
        Ok(batch)
    }

    fn build_legacy_positions(&self) -> Result<ListArray> {
        let mut positions_builder = ListBuilder::new(LargeBinaryBuilder::new());
        self.for_each_entry(|_doc_id, frequency, positions| {
            let positions = positions.ok_or_else(|| {
                Error::index(format!(
                    "legacy position writer missing positions for frequency {}",
                    frequency
                ))
            })?;
            let compressed = super::encoding::compress_positions(positions.as_slice())?;
            for block_idx in 0..compressed.len() {
                positions_builder
                    .values()
                    .append_value(compressed.value(block_idx));
            }
            positions_builder.append(true);
            Ok::<(), Error>(())
        })?;
        Ok(positions_builder.finish())
    }

    pub(super) fn append_to_batch_with_docs(
        self,
        docs: &DocSet,
        batch_builder: &mut PostingListBatchBuilder,
        format_version: InvertedListFormatVersion,
    ) -> Result<()> {
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let parts = PostingListParts {
            with_positions,
            posting_tail_codec,
            block_size,
            length: len as usize,
            encoded_blocks: encoded_blocks
                .map(|encoded_blocks| *encoded_blocks)
                .unwrap_or_default(),
            encoded_position_blocks: encoded_position_blocks
                .map(|encoded_positions| *encoded_positions)
                .unwrap_or_default(),
            tail_entries: tail_entries.as_slice(),
            tail_position_block: with_positions.then(|| tail_positions.finish()),
        };
        let (compressed, shared_positions, max_score, impacts) =
            Self::build_compressed_with_scores_from_parts(parts, docs)?;
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        batch_builder.append(
            compressed,
            Some(&impacts),
            max_score,
            len,
            positions.as_ref(),
        )
    }

    fn extend_tail_components(
        tail_entries: &[RawDocInfo],
        doc_ids: &mut Vec<u32>,
        frequencies: &mut Vec<u32>,
    ) {
        doc_ids.clear();
        frequencies.clear();
        doc_ids.extend(tail_entries.iter().map(|entry| entry.doc_id));
        frequencies.extend(tail_entries.iter().map(|entry| entry.frequency));
    }

    fn build_compressed_with_scores_from_parts(
        parts: PostingListParts<'_>,
        docs: &DocSet,
    ) -> Result<(
        LargeBinaryArray,
        Option<SharedPositionStream>,
        f32,
        ImpactSkipData,
    )> {
        let PostingListParts {
            with_positions,
            posting_tail_codec,
            length,
            block_size,
            mut encoded_blocks,
            mut encoded_position_blocks,
            tail_entries,
            tail_position_block,
        } = parts;
        let avgdl = docs.average_length();
        let idf_scale = idf(length, docs.len()) * (K1 + 1.0);
        let mut max_score = f32::MIN;
        let mut doc_ids = Vec::with_capacity(block_size);
        let mut frequencies = Vec::with_capacity(block_size);
        let mut impact_block = Vec::with_capacity(block_size);
        let mut impact_builder =
            ImpactSkipDataBuilder::with_capacity(length.div_ceil(block_size), block_size);

        for index in 0..encoded_blocks.len() {
            let block = encoded_blocks.block(index);
            doc_ids.clear();
            frequencies.clear();
            super::encoding::decode_full_posting_block(
                block,
                &mut doc_ids,
                &mut frequencies,
                block_size,
            );
            let block_score = compute_block_score_and_impact_block(
                docs,
                avgdl,
                idf_scale,
                doc_ids.iter().copied(),
                frequencies.iter().copied(),
                &mut impact_block,
            );
            impact_builder.append_block(impact_block.as_slice())?;
            max_score = max_score.max(block_score);
            if super::encoding::posting_block_score_prefix_len(block_size) > 0 {
                encoded_blocks.set_block_score(index, block_score);
            }
        }

        if !tail_entries.is_empty() {
            Self::extend_tail_components(tail_entries, &mut doc_ids, &mut frequencies);
            let block_score = compute_block_score_and_impact_block(
                docs,
                avgdl,
                idf_scale,
                doc_ids.iter().copied(),
                frequencies.iter().copied(),
                &mut impact_block,
            );
            impact_builder.append_block(impact_block.as_slice())?;
            max_score = max_score.max(block_score);
            encoded_blocks.append_remainder_block_with_codec(
                doc_ids.as_slice(),
                frequencies.as_slice(),
                posting_tail_codec,
                block_size,
            )?;
            if super::encoding::posting_block_score_prefix_len(block_size) > 0 {
                encoded_blocks.set_block_score(encoded_blocks.len() - 1, block_score);
            }
            if with_positions {
                encoded_position_blocks.push_encoded_block(
                    tail_position_block
                        .as_deref()
                        .expect("tail position block must exist for postings with positions"),
                );
            }
        }

        let impacts = impact_builder.finish()?;
        Ok((
            encoded_blocks.into_array(),
            with_positions.then(|| encoded_position_blocks.into_stream()),
            max_score,
            impacts,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build_compressed_with_block_scores_from_parts(
        with_positions: bool,
        posting_tail_codec: PostingTailCodec,
        block_size: usize,
        mut encoded_blocks: EncodedBlocks,
        mut encoded_position_blocks: EncodedPositionBlocks,
        tail_entries: &[RawDocInfo],
        tail_position_block: Option<Vec<u8>>,
        mut block_max_scores: impl Iterator<Item = f32>,
    ) -> Result<(LargeBinaryArray, Option<SharedPositionStream>, f32)> {
        let has_score_prefix = super::encoding::posting_block_score_prefix_len(block_size) > 0;
        let mut max_score = f32::MIN;
        let mut doc_ids = Vec::with_capacity(BLOCK_SIZE);
        let mut frequencies = Vec::with_capacity(BLOCK_SIZE);

        for index in 0..encoded_blocks.len() {
            let block_score = block_max_scores
                .next()
                .ok_or_else(|| Error::index("missing block max score".to_owned()))?;
            max_score = max_score.max(block_score);
            if has_score_prefix {
                encoded_blocks.set_block_score(index, block_score);
            }
        }

        if !tail_entries.is_empty() {
            let block_score = block_max_scores
                .next()
                .ok_or_else(|| Error::index("missing tail block max score".to_owned()))?;
            max_score = max_score.max(block_score);
            Self::extend_tail_components(tail_entries, &mut doc_ids, &mut frequencies);
            encoded_blocks.append_remainder_block_with_codec(
                doc_ids.as_slice(),
                frequencies.as_slice(),
                posting_tail_codec,
                block_size,
            )?;
            if has_score_prefix {
                encoded_blocks.set_block_score(encoded_blocks.len() - 1, block_score);
            }
            if with_positions {
                encoded_position_blocks.push_encoded_block(
                    tail_position_block
                        .as_deref()
                        .expect("tail position block must exist for postings with positions"),
                );
            }
        }

        Ok((
            encoded_blocks.into_array(),
            with_positions.then(|| encoded_position_blocks.into_stream()),
            max_score,
        ))
    }

    pub fn to_batch(self, block_max_scores: Vec<f32>) -> Result<RecordBatch> {
        let format_version = InvertedListFormatVersion::from_posting_tail_codec_and_block_size(
            self.posting_tail_codec,
            self.block_size,
        )?;
        let schema = inverted_list_schema_for_version_with_block_size_and_impacts(
            self.has_positions(),
            format_version,
            self.block_size,
            false,
        );
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let (compressed, shared_positions, max_score) =
            Self::build_compressed_with_block_scores_from_parts(
                with_positions,
                posting_tail_codec,
                block_size,
                encoded_blocks
                    .map(|encoded_blocks| *encoded_blocks)
                    .unwrap_or_default(),
                encoded_position_blocks
                    .map(|encoded_positions| *encoded_positions)
                    .unwrap_or_default(),
                tail_entries.as_slice(),
                with_positions.then(|| tail_positions.finish()),
                block_max_scores.into_iter(),
            )?;
        let builder = Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            memory_size_bytes: 0,
            len,
        };
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        builder.build_batch(compressed, None, max_score, schema, positions)
    }

    pub fn to_batch_with_docs(self, docs: &DocSet, schema: SchemaRef) -> Result<RecordBatch> {
        let format_version = parse_format_version_from_metadata(schema.metadata())?;
        let legacy_positions =
            if self.with_positions && !format_version.uses_shared_position_stream() {
                Some(self.build_legacy_positions()?)
            } else {
                None
            };
        let Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks,
            encoded_position_blocks,
            tail_entries,
            tail_positions,
            open_doc_id,
            open_doc_frequency,
            open_doc_last_position,
            block_size,
            len,
            ..
        } = self;
        debug_assert!(open_doc_id.is_none());
        debug_assert_eq!(open_doc_frequency, 0);
        debug_assert!(open_doc_last_position.is_none());
        let parts = PostingListParts {
            with_positions,
            posting_tail_codec,
            block_size,
            length: len as usize,
            encoded_blocks: encoded_blocks
                .map(|encoded_blocks| *encoded_blocks)
                .unwrap_or_default(),
            encoded_position_blocks: encoded_position_blocks
                .map(|encoded_positions| *encoded_positions)
                .unwrap_or_default(),
            tail_entries: tail_entries.as_slice(),
            tail_position_block: with_positions.then(|| tail_positions.finish()),
        };
        let (compressed, shared_positions, max_score, impacts) =
            Self::build_compressed_with_scores_from_parts(parts, docs)?;
        let builder = Self {
            with_positions,
            posting_tail_codec,
            encoded_blocks: None,
            encoded_position_blocks: None,
            tail_entries: Vec::new(),
            tail_positions: PositionBlockBuilder::default(),
            open_doc_id: None,
            open_doc_frequency: 0,
            open_doc_last_position: None,
            block_size,
            memory_size_bytes: 0,
            len,
        };
        let positions = match legacy_positions {
            Some(positions) => Some(CompressedPositionStorage::LegacyPerDoc(positions)),
            None => shared_positions.map(CompressedPositionStorage::SharedStream),
        };
        builder.build_batch(compressed, Some(impacts), max_score, schema, positions)
    }

    pub fn remap(&mut self, removed: &[u32]) {
        let mut cursor = 0;
        let mut new_builder = Self::new_with_posting_tail_codec_and_block_size(
            self.has_positions(),
            self.posting_tail_codec,
            self.block_size,
        );
        for (doc_id, freq, positions) in self.iter() {
            while cursor < removed.len() && removed[cursor] < doc_id {
                cursor += 1;
            }
            if cursor < removed.len() && removed[cursor] == doc_id {
                continue;
            }
            let positions = match positions {
                Some(positions) => PositionRecorder::Position(positions.into()),
                None => PositionRecorder::Count(freq),
            };
            new_builder.add(doc_id - cursor as u32, positions);
        }

        *self = new_builder;
    }
}

fn compute_block_score_and_impact_block(
    docs: &DocSet,
    avgdl: f32,
    idf_scale: f32,
    doc_ids: impl Iterator<Item = u32>,
    frequencies: impl Iterator<Item = u32>,
    impact_block: &mut Vec<(u32, u32, u32)>,
) -> f32 {
    impact_block.clear();
    let mut block_max_score = f32::MIN;
    for (doc_id, freq) in doc_ids.zip(frequencies) {
        let doc_len = docs.num_tokens(doc_id);
        let doc_norm = K1 * (1.0 - B + B * doc_len as f32 / avgdl);
        let freq_f32 = freq as f32;
        let score = freq_f32 / (freq_f32 + doc_norm);
        block_max_score = block_max_score.max(score);
        impact_block.push((doc_id, freq, doc_len));
    }
    block_max_score * idf_scale
}

#[derive(Debug, Clone, DeepSizeOf, Copy)]
pub enum DocInfo {
    Located(LocatedDocInfo),
    Raw(RawDocInfo),
}

impl DocInfo {
    pub fn doc_id(&self) -> u64 {
        match self {
            Self::Raw(info) => info.doc_id as u64,
            Self::Located(info) => info.row_id,
        }
    }

    pub fn frequency(&self) -> u32 {
        match self {
            Self::Raw(info) => info.frequency,
            Self::Located(info) => info.frequency as u32,
        }
    }
}

impl Eq for DocInfo {}

impl PartialEq for DocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id() == other.doc_id()
    }
}

impl PartialOrd for DocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.doc_id().cmp(&other.doc_id())
    }
}

#[derive(Debug, Clone, Default, DeepSizeOf, Copy)]
pub struct LocatedDocInfo {
    pub row_id: u64,
    pub frequency: f32,
}

impl LocatedDocInfo {
    pub fn new(row_id: u64, frequency: f32) -> Self {
        Self { row_id, frequency }
    }
}

impl Eq for LocatedDocInfo {}

impl PartialEq for LocatedDocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.row_id == other.row_id
    }
}

impl PartialOrd for LocatedDocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LocatedDocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.row_id.cmp(&other.row_id)
    }
}

#[derive(Debug, Clone, Default, DeepSizeOf, Copy)]
pub struct RawDocInfo {
    pub doc_id: u32,
    pub frequency: u32,
}

impl RawDocInfo {
    pub fn new(doc_id: u32, frequency: u32) -> Self {
        Self { doc_id, frequency }
    }
}

impl Eq for RawDocInfo {}

impl PartialEq for RawDocInfo {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
    }
}

impl PartialOrd for RawDocInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawDocInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.doc_id.cmp(&other.doc_id)
    }
}

/// Lucene SmallFloat-style document-length quantization for 256-document-block scoring and impact
/// norms: a 4-mantissa-bit float-like byte code. Values 0-7 are exact; larger
/// values keep their top four significand bits (relative error <= 6.25%) and
/// decode to their bucket floor. The floor only ever shortens a doc, so impact
/// bounds remain conservative for exact scoring as well as quantized scoring.
pub(super) fn quantize_doc_length(value: u32) -> u8 {
    let num_bits = 32 - value.leading_zeros();
    if num_bits < 4 {
        value as u8
    } else {
        let shift = num_bits - 4;
        (((value >> shift) as u8) & 0x07) | (((shift + 1) as u8) << 3)
    }
}

#[inline]
pub(super) fn dequantize_doc_length(code: u8) -> u32 {
    DEQUANTIZED_DOC_LENGTHS[code as usize]
}

pub(super) static DEQUANTIZED_DOC_LENGTHS: [u32; 256] = build_dequantized_doc_lengths();

const fn build_dequantized_doc_lengths() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut code = 0usize;
    while code < 256 {
        let bits = (code & 0x07) as u64;
        let shift = (code >> 3) as i64 - 1;
        let decoded = if shift < 0 {
            bits
        } else {
            (bits | 0x08) << shift
        };
        // Codes past the largest u32 encoding are never produced; saturate so
        // the table stays total.
        table[code] = if decoded > u32::MAX as u64 {
            u32::MAX
        } else {
            decoded as u32
        };
        code += 1;
    }
    table
}

#[derive(Debug, Clone)]
enum NumTokens {
    Owned(Vec<u32>),
    Shared(ScalarBuffer<u32>),
}

impl Default for NumTokens {
    fn default() -> Self {
        Self::Owned(Vec::new())
    }
}

impl std::ops::Deref for NumTokens {
    type Target = [u32];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(values) => values,
            Self::Shared(values) => values,
        }
    }
}

impl DeepSizeOf for NumTokens {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Owned(values) => values.deep_size_of_children(context),
            Self::Shared(values) => values.deep_size_of_children(context),
        }
    }
}

impl NumTokens {
    fn with_capacity(capacity: usize) -> Self {
        Self::Owned(Vec::with_capacity(capacity))
    }

    fn into_owned(self) -> Vec<u32> {
        match self {
            Self::Owned(values) => values,
            Self::Shared(values) => values.to_vec(),
        }
    }

    fn push(&mut self, value: u32) {
        match self {
            Self::Owned(values) => values.push(value),
            Self::Shared(values) => {
                let mut owned = values.to_vec();
                owned.push(value);
                *self = Self::Owned(owned);
            }
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            Self::Owned(values) => values.capacity() * std::mem::size_of::<u32>(),
            Self::Shared(values) => values.inner().capacity(),
        }
    }
}

// DocSet is a mapping from row ids to the number of tokens in the document
// It's used to sort the documents by the bm25 score
#[derive(Debug, Clone, Default)]
pub struct DocSet {
    row_ids: Vec<u64>,
    num_tokens: NumTokens,
    // One flat u32 column per list boundary. This avoids a Vec allocation per
    // document while preserving the full logical document coordinate.
    doc_indices: Vec<Vec<u32>>,
    // (row_id, doc_id) pairs sorted by row_id
    inv: Vec<(u64, u32)>,

    total_tokens: u64,

    // 256-document-block partitions score with quantized document lengths: the
    // flag is set at partition load and the byte-norm slab bakes lazily on
    // first scoring use (shared by clones of the loaded set). 128-block
    // partitions never set the flag and keep exact scoring.
    scoring_quantized: bool,
    norms: Arc<std::sync::OnceLock<Box<[u8]>>>,
}

impl DeepSizeOf for DocSet {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.row_ids.deep_size_of_children(context)
            + self.num_tokens.deep_size_of_children(context)
            + self.doc_indices.deep_size_of_children(context)
            + self.inv.deep_size_of_children(context)
            + self
                .norms
                .get()
                .map(|slab| std::mem::size_of_val(slab.as_ref()))
                .unwrap_or(0)
    }
}

impl DocSet {
    pub(crate) fn with_coordinate_rank(coordinate_rank: usize) -> Self {
        Self {
            doc_indices: (0..coordinate_rank).map(|_| Vec::new()).collect(),
            ..Default::default()
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        // Use num_tokens instead of row_ids so the deferred-row_ids
        // scoring path (which constructs a DocSet via
        // [`Self::from_num_tokens_only`]) still reports the right doc
        // count.
        self.num_tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True iff the per-doc `row_id` array is populated. The
    /// deferred-row_id scoring path constructs DocSets with the array
    /// left empty so wand can skip the load; callers that need to do
    /// row_id lookups in the inner loop must check this and fall back
    /// to async resolution otherwise.
    #[inline]
    pub fn has_row_ids(&self) -> bool {
        !self.row_ids.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &u32)> {
        self.row_ids.iter().zip(self.num_tokens.iter())
    }

    pub fn row_id(&self, doc_id: u32) -> u64 {
        self.row_ids[doc_id as usize]
    }

    pub fn doc_index(&self, doc_id: u32) -> Vec<u32> {
        self.doc_indices
            .iter()
            .map(|coordinates| coordinates[doc_id as usize])
            .collect()
    }

    pub fn coordinate(&self, doc_id: u32, rank: usize) -> u32 {
        self.doc_indices[rank][doc_id as usize]
    }

    pub fn coordinate_rank(&self) -> usize {
        self.doc_indices.len()
    }

    /// Resolve a `row_id` to every `doc_id` it owns.
    ///
    /// Row-document indexes map each row to a single document. Element-document
    /// indexes (and older list indexes) can map one row to several documents,
    /// so a single `row_id` may own multiple `doc_id`s sharing that key in `inv`.
    /// The prefilter path (`flat_search`) walks an allow-list of row_ids and
    /// must evaluate all legacy documents for that row.
    pub fn doc_ids(&self, row_id: u64) -> impl Iterator<Item = u64> + '_ {
        if self.inv.is_empty() {
            // in legacy format, the row id is doc id (one document per row)
            let found = self.row_ids.binary_search(&row_id).is_ok();
            Either::Left(found.then_some(row_id).into_iter())
        } else {
            // `inv` is sorted by row_id, so the entries sharing this key form a
            // contiguous run; yield the doc_id of each.
            let lo = self.inv.partition_point(|entry| entry.0 < row_id);
            let hi = self.inv.partition_point(|entry| entry.0 <= row_id);
            Either::Right(self.inv[lo..hi].iter().map(|entry| entry.1 as u64))
        }
    }
    pub fn total_tokens_num(&self) -> u64 {
        self.total_tokens
    }

    #[inline]
    pub fn average_length(&self) -> f32 {
        self.total_tokens as f32 / self.len() as f32
    }

    pub fn calculate_block_max_scores<'a>(
        &self,
        doc_ids: impl Iterator<Item = &'a u32>,
        freqs: impl Iterator<Item = &'a u32>,
    ) -> Vec<f32> {
        self.calculate_block_max_scores_with_block_size(doc_ids, freqs, LEGACY_BLOCK_SIZE)
    }

    pub fn calculate_block_max_scores_with_block_size<'a>(
        &self,
        doc_ids: impl Iterator<Item = &'a u32>,
        freqs: impl Iterator<Item = &'a u32>,
        block_size: usize,
    ) -> Vec<f32> {
        validate_block_size(block_size).expect("invalid posting list block size");
        let avgdl = self.average_length();
        let length = doc_ids.size_hint().0;
        let num_blocks = length.div_ceil(block_size);
        let mut block_max_scores = Vec::with_capacity(num_blocks);
        let idf_scale = idf(length, self.len()) * (K1 + 1.0);
        let mut max_score = f32::MIN;
        for (i, (doc_id, freq)) in doc_ids.zip(freqs).enumerate() {
            let doc_norm = K1 * (1.0 - B + B * self.num_tokens(*doc_id) as f32 / avgdl);
            let freq = *freq as f32;
            let score = freq / (freq + doc_norm);
            if score > max_score {
                max_score = score;
            }
            if (i + 1) % block_size == 0 {
                max_score *= idf_scale;
                block_max_scores.push(max_score);
                max_score = f32::MIN;
            }
        }
        if !length.is_multiple_of(block_size) {
            max_score *= idf_scale;
            block_max_scores.push(max_score);
        }
        block_max_scores
    }

    pub fn to_batch(&self) -> Result<RecordBatch> {
        let row_id_col = UInt64Array::from_iter_values(self.row_ids.iter().cloned());
        let num_tokens_col = UInt32Array::from_iter_values(self.num_tokens.iter().cloned());

        let mut fields = vec![
            arrow_schema::Field::new(ROW_ID, DataType::UInt64, false),
            arrow_schema::Field::new(NUM_TOKEN_COL, DataType::UInt32, false),
        ];
        let mut columns = vec![
            Arc::new(row_id_col) as ArrayRef,
            Arc::new(num_tokens_col) as ArrayRef,
        ];
        for (rank, coordinates) in self.doc_indices.iter().enumerate() {
            fields.push(arrow_schema::Field::new(
                doc_index_storage_column(rank),
                DataType::UInt32,
                false,
            ));
            columns.push(
                Arc::new(UInt32Array::from_iter_values(coordinates.iter().copied())) as ArrayRef,
            );
        }
        let schema = arrow_schema::Schema::new(fields);

        let batch = RecordBatch::try_new(Arc::new(schema), columns)?;
        Ok(batch)
    }

    pub async fn load(
        reader: Arc<dyn IndexReader>,
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        let batch = reader.read_range(0..reader.num_rows(), None).await?;
        let row_id_col = batch[ROW_ID].as_primitive::<datatypes::UInt64Type>();
        let num_tokens_col = batch[NUM_TOKEN_COL].as_primitive::<datatypes::UInt32Type>();
        let mut doc_indices = Vec::new();
        for rank in 0.. {
            let column_name = doc_index_storage_column(rank);
            let Some(column) = batch.column_by_name(&column_name) else {
                break;
            };
            doc_indices.push(column.as_primitive::<datatypes::UInt32Type>());
        }
        Self::from_columns_with_doc_indices(
            row_id_col,
            num_tokens_col,
            &doc_indices,
            is_legacy,
            frag_reuse_index,
        )
    }

    /// Build a `DocSet` carrying only the per-doc `num_tokens` array;
    /// `row_ids` and `inv` are left empty. Used by the deferred-row_id
    /// scoring path: wand checks `has_row_ids()` to skip `row_id` /
    /// `num_tokens_by_row_id` calls, and the per-partition caller
    /// resolves doc_id → row_id for the surviving top-K post-wand.
    pub fn from_num_tokens_only(num_tokens_col: &arrow_array::UInt32Array) -> Self {
        let total_tokens = num_tokens_col.values().iter().map(|&n| n as u64).sum();
        Self::from_cached_num_tokens(num_tokens_col, total_tokens)
    }

    /// Build a zero-copy num-tokens-only view from an Arrow column and its
    /// already-computed total. The caller must guarantee that `total_tokens`
    /// is the sum of `num_tokens_col`.
    pub(crate) fn from_cached_num_tokens(
        num_tokens_col: &arrow_array::UInt32Array,
        total_tokens: u64,
    ) -> Self {
        Self {
            row_ids: Vec::new(),
            num_tokens: NumTokens::Shared(num_tokens_col.values().clone()),
            doc_indices: Vec::new(),
            inv: Vec::new(),
            total_tokens,
            scoring_quantized: false,
            norms: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Build a `DocSet` from already-loaded `row_id` and `num_tokens`
    /// Arrow columns without re-reading either column.
    pub fn from_columns(
        row_id_col: &UInt64Array,
        num_tokens_col: &arrow_array::UInt32Array,
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        Self::from_columns_with_doc_indices(
            row_id_col,
            num_tokens_col,
            &[],
            is_legacy,
            frag_reuse_index,
        )
    }

    pub fn from_columns_with_doc_indices(
        row_id_col: &UInt64Array,
        num_tokens_col: &arrow_array::UInt32Array,
        doc_index_cols: &[&arrow_array::UInt32Array],
        is_legacy: bool,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        if doc_index_cols
            .iter()
            .any(|column| column.len() != row_id_col.len())
        {
            return Err(Error::index(
                "FTS document coordinate columns must have the same length as row ids".to_string(),
            ));
        }
        let doc_indices = doc_index_cols
            .iter()
            .map(|column| column.values().to_vec())
            .collect::<Vec<_>>();
        // for legacy format, the row id is doc id; sorting keeps binary search viable
        if is_legacy {
            let (row_ids, num_tokens): (Vec<_>, Vec<_>) = row_id_col
                .values()
                .iter()
                .filter_map(|id| {
                    if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
                        frag_reuse_index_ref.remap_row_id(*id)
                    } else {
                        Some(*id)
                    }
                })
                .zip(num_tokens_col.values().iter())
                .sorted_unstable_by_key(|x| x.0)
                .unzip();

            let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
            return Ok(Self {
                row_ids,
                num_tokens: NumTokens::Owned(num_tokens),
                doc_indices,
                inv: Vec::new(),
                total_tokens,
                scoring_quantized: false,
                norms: Arc::new(std::sync::OnceLock::new()),
            });
        }

        // If frag reuse happened, remap the row_ids through it. Crucially we
        // must NOT drop the rows the reuse index deleted, because the posting
        // lists reference doc_ids *positionally* (a doc_id is an index into
        // these arrays, fixed at build time). Dropping deleted rows would
        // renumber every later doc_id and desync the posting lists, so wand
        // would index `num_tokens`/`row_ids` out of bounds or score the wrong
        // doc. Instead we tombstone deleted rows in place: their slot survives
        // (so doc_ids stay aligned with the posting lists) carrying
        // `RowAddress::TOMBSTONE_ROW`, which wand skips, and they are left out
        // of `inv` so a row_id lookup never resolves to a deleted doc. The
        // heavyweight physical remap (`DocSet::remap`) is what actually
        // renumbers and compacts; this load-time path only has to stay
        // consistent until then.
        if let Some(frag_reuse_index_ref) = frag_reuse_index.as_ref() {
            let mut row_ids = Vec::with_capacity(row_id_col.len());
            let num_tokens = num_tokens_col.values().to_vec();
            let mut inv = Vec::with_capacity(row_id_col.len());
            for (doc_id, row_id) in row_id_col.values().iter().enumerate() {
                match frag_reuse_index_ref.remap_row_id(*row_id) {
                    Some(new_row_id) => {
                        row_ids.push(new_row_id);
                        inv.push((new_row_id, doc_id as u32));
                    }
                    None => {
                        // Deleted: keep the slot (doc_ids must not shift) but
                        // tombstone it and leave it out of `inv`.
                        row_ids.push(RowAddress::TOMBSTONE_ROW);
                    }
                }
            }
            inv.sort_unstable_by_key(|entry| entry.0);

            let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
            return Ok(Self {
                row_ids,
                num_tokens: NumTokens::Owned(num_tokens),
                doc_indices,
                inv,
                total_tokens,
                scoring_quantized: false,
                norms: Arc::new(std::sync::OnceLock::new()),
            });
        }

        let row_ids = row_id_col.values().to_vec();
        let num_tokens = num_tokens_col.values().to_vec();
        let mut inv: Vec<(u64, u32)> = row_ids
            .iter()
            .enumerate()
            .map(|(doc_id, row_id)| (*row_id, doc_id as u32))
            .collect();
        if !row_ids.is_sorted() {
            inv.sort_unstable_by_key(|entry| entry.0);
        }
        let total_tokens = num_tokens.iter().map(|&x| x as u64).sum();
        Ok(Self {
            row_ids,
            num_tokens: NumTokens::Owned(num_tokens),
            doc_indices,
            inv,
            total_tokens,
            scoring_quantized: false,
            norms: Arc::new(std::sync::OnceLock::new()),
        })
    }

    // remap the row ids to the new row ids
    // returns the removed doc ids
    pub fn remap(&mut self, mapping: &RowAddrRemap) -> Vec<u32> {
        let mut removed = Vec::new();
        let len = self.len();
        let row_ids = std::mem::replace(&mut self.row_ids, Vec::with_capacity(len));
        let num_tokens =
            std::mem::replace(&mut self.num_tokens, NumTokens::with_capacity(len)).into_owned();
        let doc_indices = std::mem::take(&mut self.doc_indices);
        self.doc_indices = doc_indices
            .iter()
            .map(|_| Vec::with_capacity(len))
            .collect();
        self.invalidate_norms();
        self.total_tokens = 0;
        for (doc_id, (row_id, num_token)) in std::iter::zip(row_ids, num_tokens).enumerate() {
            match mapping.get(row_id) {
                Some(Some(new_row_id)) => {
                    self.row_ids.push(new_row_id);
                    self.num_tokens.push(num_token);
                    for (new_coordinates, old_coordinates) in
                        self.doc_indices.iter_mut().zip(&doc_indices)
                    {
                        new_coordinates.push(old_coordinates[doc_id]);
                    }
                    self.total_tokens += num_token as u64;
                }
                Some(None) => {
                    removed.push(doc_id as u32);
                }
                None => {
                    self.row_ids.push(row_id);
                    self.num_tokens.push(num_token);
                    for (new_coordinates, old_coordinates) in
                        self.doc_indices.iter_mut().zip(&doc_indices)
                    {
                        new_coordinates.push(old_coordinates[doc_id]);
                    }
                    self.total_tokens += num_token as u64;
                }
            }
        }
        removed
    }

    #[inline]
    pub fn num_tokens(&self, doc_id: u32) -> u32 {
        self.num_tokens[doc_id as usize]
    }

    /// Enable quantized document-length scoring for 256-document-block partitions.
    pub fn set_quantized_scoring(&mut self, quantized: bool) {
        self.scoring_quantized = quantized;
    }

    /// The quantized document-length slab when this set scores quantized,
    /// baked on first use; `None` for exact-scoring sets.
    pub fn scoring_norms(&self) -> Option<&[u8]> {
        if !self.scoring_quantized {
            return None;
        }
        Some(
            self.norms
                .get_or_init(|| {
                    self.num_tokens
                        .iter()
                        .map(|&n| quantize_doc_length(n))
                        .collect()
                })
                .as_ref(),
        )
    }

    /// Document length as scoring sees it: the quantized bucket floor for
    /// 256-document-block partitions, the exact value otherwise.
    #[inline]
    pub fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        match self.scoring_norms() {
            Some(norms) => dequantize_doc_length(norms[doc_id as usize]),
            None => self.num_tokens[doc_id as usize],
        }
    }

    // this can be used only if it's a legacy format,
    // which store the sorted row ids so that we can use binary search
    #[inline]
    pub fn num_tokens_by_row_id(&self, row_id: u64) -> u32 {
        self.row_ids
            .binary_search(&row_id)
            .map(|idx| self.num_tokens[idx])
            .unwrap_or(0)
    }

    // append a document to the doc set
    // returns the doc_id (the number of documents before appending)
    pub fn append(&mut self, row_id: u64, num_tokens: u32) -> u32 {
        self.row_ids.push(row_id);
        self.num_tokens.push(num_tokens);
        self.total_tokens += num_tokens as u64;
        self.invalidate_norms();
        self.row_ids.len() as u32 - 1
    }

    pub fn append_with_doc_index(
        &mut self,
        row_id: u64,
        num_tokens: u32,
        doc_index: &[u32],
    ) -> Result<u32> {
        if self.row_ids.is_empty() && self.doc_indices.is_empty() {
            self.doc_indices = (0..doc_index.len()).map(|_| Vec::new()).collect();
        }
        if self.doc_indices.len() != doc_index.len() {
            return Err(Error::index(format!(
                "all documents in an FTS partition must have the same coordinate rank: expected {}, got {}",
                self.doc_indices.len(),
                doc_index.len()
            )));
        }
        self.row_ids.push(row_id);
        self.num_tokens.push(num_tokens);
        for (coordinates, value) in self.doc_indices.iter_mut().zip(doc_index) {
            coordinates.push(*value);
        }
        self.total_tokens += num_tokens as u64;
        self.invalidate_norms();
        Ok(self.row_ids.len() as u32 - 1)
    }

    // Drop the baked norm slab after a mutation; it re-bakes on the next
    // scoring use.
    fn invalidate_norms(&mut self) {
        if self.norms.get().is_some() {
            self.norms = Arc::new(std::sync::OnceLock::new());
        }
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.row_ids.capacity() * std::mem::size_of::<u64>()
            + self.num_tokens.memory_size()
            + self
                .doc_indices
                .iter()
                .map(|coordinates| coordinates.capacity() * std::mem::size_of::<u32>())
                .sum::<usize>()
            + self.inv.capacity() * std::mem::size_of::<(u64, u32)>()
    }
}

pub fn doc_index_storage_column(rank: usize) -> String {
    format!("{DOC_INDEX_STORAGE_PREFIX}{rank}")
}

pub fn document_coordinate_rank(schema: &arrow_schema::Schema) -> usize {
    (0..)
        .take_while(|rank| {
            schema
                .column_with_name(&doc_index_storage_column(*rank))
                .is_some()
        })
        .count()
}

pub fn flat_full_text_search(
    batches: &[&RecordBatch],
    doc_col: &str,
    query: &str,
    tokenizer: Option<Box<dyn LanceTokenizer>>,
) -> Result<Vec<u64>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let (query, phrase_slop) = match phrase_query_text(query) {
        Some(query) => (query, Some(0)),
        None => (query, None),
    };

    match batches[0][doc_col].data_type() {
        DataType::Utf8 => {
            do_flat_full_text_search::<i32>(batches, doc_col, query, tokenizer, phrase_slop)
        }
        DataType::LargeUtf8 => {
            do_flat_full_text_search::<i64>(batches, doc_col, query, tokenizer, phrase_slop)
        }
        DataType::List(_) => {
            do_flat_full_text_search_list::<i32>(batches, doc_col, query, tokenizer, phrase_slop)
        }
        DataType::LargeList(_) => {
            do_flat_full_text_search_list::<i64>(batches, doc_col, query, tokenizer, phrase_slop)
        }
        data_type => Err(Error::invalid_input(format!(
            "unsupported data type {} for inverted index",
            data_type
        ))),
    }
}

fn do_flat_full_text_search<Offset: OffsetSizeTrait>(
    batches: &[&RecordBatch],
    doc_col: &str,
    query: &str,
    tokenizer: Option<Box<dyn LanceTokenizer>>,
    phrase_slop: Option<u32>,
) -> Result<Vec<u64>> {
    let mut results = Vec::new();
    let mut tokenizer =
        tokenizer.unwrap_or_else(|| InvertedIndexParams::default().build().unwrap());
    let query_tokens = collect_query_tokens(query, &mut tokenizer);

    for batch in batches {
        let row_id_array = batch[ROW_ID].as_primitive::<UInt64Type>();
        let doc_array = batch[doc_col].as_string::<Offset>();
        for i in 0..row_id_array.len() {
            let doc = doc_array.value(i);
            if document_matches_flat_query(doc, &mut tokenizer, &query_tokens, phrase_slop)? {
                results.push(row_id_array.value(i));
            }
        }
    }

    Ok(results)
}

fn do_flat_full_text_search_list<ListOffset: OffsetSizeTrait>(
    batches: &[&RecordBatch],
    doc_col: &str,
    query: &str,
    tokenizer: Option<Box<dyn LanceTokenizer>>,
    phrase_slop: Option<u32>,
) -> Result<Vec<u64>> {
    let mut results = Vec::new();
    let mut tokenizer =
        tokenizer.unwrap_or_else(|| InvertedIndexParams::default().build().unwrap());
    let query_tokens = collect_query_tokens(query, &mut tokenizer);

    for batch in batches {
        let row_id_array = batch[ROW_ID].as_primitive::<UInt64Type>();
        let doc_array = batch[doc_col].as_list::<ListOffset>();
        match doc_array.value_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {}
            data_type => {
                return Err(Error::invalid_input(format!(
                    "unsupported list item data type {} for inverted index",
                    data_type
                )));
            }
        }
        for i in 0..row_id_array.len() {
            if doc_array.is_null(i) {
                continue;
            }
            let elements = doc_array.value(i);
            let matches = if phrase_slop.is_some() {
                let document = iter_str_array(elements.as_ref())
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" ");
                document_matches_flat_query(&document, &mut tokenizer, &query_tokens, phrase_slop)?
            } else {
                iter_str_array(elements.as_ref())
                    .flatten()
                    .any(|element| has_query_token(element, &mut tokenizer, &query_tokens))
            };
            if matches {
                results.push(row_id_array.value(i));
            }
        }
    }

    Ok(results)
}

fn document_matches_flat_query(
    document: &str,
    tokenizer: &mut Box<dyn LanceTokenizer>,
    query_tokens: &Tokens,
    phrase_slop: Option<u32>,
) -> Result<bool> {
    let Some(slop) = phrase_slop else {
        return Ok(has_query_token(document, tokenizer, query_tokens));
    };

    let mut document_positions = (0..query_tokens.len())
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    let mut stream = tokenizer.token_stream_for_doc(document);
    while let Some(token) = stream.next() {
        let position = u32::try_from(token.position).map_err(|_| {
            Error::invalid_input(format!(
                "flat FTS token position exceeds u32: {}",
                token.position
            ))
        })?;
        for (query_index, positions) in document_positions.iter_mut().enumerate() {
            if query_tokens.get_token(query_index) == token.text {
                positions.push(position);
            }
        }
    }
    Ok(phrase_matches_positions(
        query_tokens,
        &document_positions,
        slop,
    ))
}

const FLAT_ALL_TOKENS_COL: &str = "all_tokens";
const FLAT_QUERY_TOKEN_COUNTS_COL: &str = "query_token_counts";
const FLAT_PHRASE_MATCH_COL: &str = "phrase_match";

fn phrase_matches_positions(
    query_tokens: &Tokens,
    document_positions: &[Vec<u32>],
    slop: u32,
) -> bool {
    let Some(first_positions) = document_positions.first() else {
        return false;
    };
    if first_positions.is_empty() {
        return false;
    }

    let mut candidates = first_positions.clone();
    debug_assert_eq!(query_tokens.len(), document_positions.len());
    for (query_index, positions) in document_positions.iter().enumerate().skip(1) {
        let Some(query_delta) = query_tokens
            .position(query_index)
            .checked_sub(query_tokens.position(query_index - 1))
        else {
            return false;
        };
        let mut next_candidates = Vec::new();
        for &position in positions {
            let position = u64::from(position);
            if candidates.iter().any(|candidate| {
                let least = u64::from(*candidate) + u64::from(query_delta);
                least <= position && position <= least + u64::from(slop)
            }) {
                next_candidates.push(position as u32);
            }
        }
        if next_candidates.is_empty() {
            return false;
        }
        candidates = next_candidates;
    }
    true
}

/// If we accumulate this many bytes we warn the user they probably want to use an FTS index instead.
const BYTES_ACCUMULATED_WARNING_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1GB

/// Consumes a stream of record batches and produces token counts
///
/// The resulting batch will have three columns:
/// - row_id: the row id of the document
/// - all_tokens: the total number of tokens in the document
/// - query_token_counts: a fixed size list of the count of each query token in the document
///
/// This is an unbounded accumulation, however, for most queries, the per-row
/// growth will be fairly small.  As a result we can process millions of tokens
/// with fairly modest memory usage.
///
/// However, it is unwise to do a flat search across billions of rows.  An FTS
/// index should be created instead.
async fn tokenize_and_count(
    input: impl Stream<Item = DataFusionResult<RecordBatch>> + Send,
    tokenizer: Box<dyn LanceTokenizer>,
    query_tokens: Arc<Tokens>,
    doc_col_idx: usize,
    elapsed_compute: Option<Time>,
    coordinate_rank: usize,
    phrase_slop: Option<u32>,
) -> DataFusionResult<RecordBatch> {
    let mut output_fields = vec![ROW_ID_FIELD.clone()];
    output_fields.extend(
        (0..coordinate_rank)
            .map(|rank| Field::new(doc_index_storage_column(rank), DataType::UInt32, false)),
    );
    output_fields.extend([
        Field::new(FLAT_ALL_TOKENS_COL, DataType::UInt64, false),
        Field::new(
            FLAT_QUERY_TOKEN_COUNTS_COL,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt64, true)),
                query_tokens.len() as i32,
            ),
            false,
        ),
    ]);
    if phrase_slop.is_some() {
        output_fields.push(Field::new(FLAT_PHRASE_MATCH_COL, DataType::Boolean, false));
    }
    let output_schema = Arc::new(Schema::new(output_fields));
    let output_schema_clone = output_schema.clone();
    let query_token_indices = Arc::new(query_token_indices(query_tokens.as_ref()));
    let bytes_accumulated = Arc::new(AtomicU64::new(0));
    let bytes_warning_emitted = Arc::new(AtomicBool::new(false));

    let batches = input
        .map(move |batch| {
            let mut tokenizer = tokenizer.box_clone();
            let output_schema = output_schema.clone();
            let query_tokens = query_tokens.clone();
            let query_token_indices = query_token_indices.clone();
            let bytes_accumulated = bytes_accumulated.clone();
            let bytes_warning_emitted = bytes_warning_emitted.clone();
            let elapsed_compute = elapsed_compute.clone();
            spawn_cpu(move || {
                // Time the per-batch CPU work so callers can attribute it to
                // `elapsed_compute` on a metric handle (the spawn_cpu worker
                // thread is invisible to the caller's poll timer otherwise).
                let start = std::time::Instant::now();
                let batch = batch?;
                let row_id_array = batch[ROW_ID].as_primitive::<UInt64Type>();
                let input_doc_indices = (0..coordinate_rank)
                    .map(|rank| {
                        let column_name = doc_index_storage_column(rank);
                        batch
                            .column_by_name(&column_name)
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Internal(format!(
                                    "flat ListElement FTS is missing {column_name}"
                                ))
                            })
                            .map(|column| column.as_primitive::<UInt32Type>())
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;
                let mut row_ids = UInt64Builder::with_capacity(batch.num_rows());
                let mut doc_indices = (0..coordinate_rank)
                    .map(|_| UInt32Builder::with_capacity(batch.num_rows()))
                    .collect::<Vec<_>>();
                let mut all_token_counts = UInt64Builder::with_capacity(batch.num_rows());
                let mut query_token_counts = FixedSizeListBuilder::with_capacity(
                    UInt64Builder::with_capacity(batch.num_rows() * query_tokens.len()),
                    query_tokens.len() as i32,
                    batch.num_rows(),
                );
                let mut temp_query_token_counts = Vec::with_capacity(query_tokens.len());
                let mut temp_query_positions = (0..query_tokens.len())
                    .map(|_| Vec::new())
                    .collect::<Vec<_>>();
                let mut phrase_matches = phrase_slop
                    .map(|_| BooleanBuilder::with_capacity(batch.num_rows()));
                let mut count_text = |doc: &str,
                                      temp_query_token_counts: &mut Vec<u64>|
                 -> DataFusionResult<(u64, bool)> {
                    for positions in &mut temp_query_positions {
                        positions.clear();
                    }
                    let mut stream = tokenizer.token_stream_for_doc(doc);
                    let mut all_tokens = 0;
                    while let Some(token) = stream.next() {
                        all_tokens += 1;
                        if let Some(token_indices) = query_token_indices.get(&token.text) {
                            for token_index in token_indices {
                                temp_query_token_counts[*token_index] += 1;
                                if phrase_slop.is_some() {
                                    temp_query_positions[*token_index].push(
                                        u32::try_from(token.position).map_err(|_| {
                                            datafusion_common::DataFusionError::Execution(format!(
                                                "flat FTS token position exceeds u32: {}",
                                                token.position
                                            ))
                                        })?,
                                    );
                                }
                            }
                        }
                    }
                    let matches = phrase_slop.is_none_or(|slop| {
                        phrase_matches_positions(
                            query_tokens.as_ref(),
                            &temp_query_positions,
                            slop,
                        )
                    });
                    Ok((all_tokens, matches))
                };
                let mut append_counts = |row_index: usize,
                                         row_id: u64,
                                         all_tokens: u64,
                                         temp_query_token_counts: &[u64],
                                         phrase_match: bool|
                 -> DataFusionResult<()> {
                        row_ids.append_value(row_id);
                        for (builder, input) in
                            doc_indices.iter_mut().zip(input_doc_indices.iter())
                        {
                            builder.append_value(input.value(row_index));
                        }
                        all_token_counts.append_value(all_tokens);
                        for count in temp_query_token_counts.iter().copied() {
                            query_token_counts.values().append_value(count);
                        }
                        query_token_counts.append(true);
                        if let Some(builder) = phrase_matches.as_mut() {
                            builder.append_value(phrase_match);
                        }
                        Ok(())
                    };
                match batch.column(doc_col_idx).data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        let doc_iter = iter_str_array(batch.column(doc_col_idx));
                        for (row_index, (doc, row_id)) in
                            doc_iter.zip(row_id_array.values().iter()).enumerate()
                        {
                            temp_query_token_counts.clear();
                            temp_query_token_counts
                                .extend(std::iter::repeat_n(0, query_tokens.len()));

                            let (all_tokens, phrase_match) = match doc {
                                Some(doc) => count_text(doc, &mut temp_query_token_counts)?,
                                None => (0, false),
                            };
                            if coordinate_rank > 0 || all_tokens > 0 {
                                append_counts(
                                    row_index,
                                    *row_id,
                                    all_tokens,
                                    &temp_query_token_counts,
                                    phrase_match,
                                )?;
                            }
                        }
                    }
                    DataType::List(_) => {
                        if coordinate_rank != 0 {
                            return DataFusionResult::Err(
                                datafusion_common::DataFusionError::Internal(
                                    "ListElement flat FTS input must be expanded to string documents"
                                        .to_string(),
                                ),
                            );
                        }
                        tokenize_and_count_list::<i32>(
                            batch.column(doc_col_idx),
                            row_id_array,
                            &mut count_text,
                            &mut append_counts,
                            &mut temp_query_token_counts,
                            query_tokens.len(),
                            phrase_slop.is_some(),
                        )?;
                    }
                    DataType::LargeList(_) => {
                        if coordinate_rank != 0 {
                            return DataFusionResult::Err(
                                datafusion_common::DataFusionError::Internal(
                                    "ListElement flat FTS input must be expanded to string documents"
                                        .to_string(),
                                ),
                            );
                        }
                        tokenize_and_count_list::<i64>(
                            batch.column(doc_col_idx),
                            row_id_array,
                            &mut count_text,
                            &mut append_counts,
                            &mut temp_query_token_counts,
                            query_tokens.len(),
                            phrase_slop.is_some(),
                        )?;
                    }
                    data_type => {
                        return DataFusionResult::Err(datafusion_common::DataFusionError::Execution(
                            format!("unsupported data type {} for flat full text search", data_type),
                        ));
                    }
                }
                let row_ids = row_ids.finish();
                let doc_indices = doc_indices
                    .into_iter()
                    .map(|mut builder| builder.finish())
                    .collect::<Vec<_>>();
                let all_token_counts = all_token_counts.finish();
                let query_token_counts = query_token_counts.finish();
                let mut columns = vec![Arc::new(row_ids) as ArrayRef];
                for doc_indices in doc_indices {
                    columns.push(Arc::new(doc_indices) as ArrayRef);
                }
                columns.extend([
                    Arc::new(all_token_counts) as ArrayRef,
                    Arc::new(query_token_counts) as ArrayRef,
                ]);
                if let Some(mut builder) = phrase_matches {
                    columns.push(Arc::new(builder.finish()) as ArrayRef);
                }
                let result_batch = RecordBatch::try_new(output_schema, columns)?;
                let bytes_accumulated = bytes_accumulated.fetch_add(result_batch.get_array_memory_size() as u64, Ordering::Relaxed);
                if bytes_accumulated > BYTES_ACCUMULATED_WARNING_THRESHOLD && !bytes_warning_emitted.swap(true, Ordering::Relaxed) {
                    tracing::warn!("Flat full text search is accumulating a large number of bytes.  Consider using an FTS index instead.");
                }

                if let Some(t) = &elapsed_compute {
                    t.add_duration(start.elapsed());
                }
                DataFusionResult::Ok(result_batch)
            })
        })
        .buffered(get_num_compute_intensive_cpus())
        .try_collect::<Vec<_>>()
        .await?;

    Ok(arrow::compute::concat_batches(
        &output_schema_clone,
        &batches,
    )?)
}

fn tokenize_and_count_list<ListOffset: OffsetSizeTrait>(
    doc_col: &ArrayRef,
    row_id_array: &arrow_array::PrimitiveArray<UInt64Type>,
    count_text: &mut impl FnMut(&str, &mut Vec<u64>) -> DataFusionResult<(u64, bool)>,
    append_counts: &mut impl FnMut(usize, u64, u64, &[u64], bool) -> DataFusionResult<()>,
    temp_query_token_counts: &mut Vec<u64>,
    query_tokens_len: usize,
    match_phrase: bool,
) -> DataFusionResult<()> {
    let doc_array = doc_col.as_list::<ListOffset>();
    match doc_array.value_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {}
        data_type => {
            return Err(datafusion_common::DataFusionError::Execution(format!(
                "unsupported list item data type {} for flat full text search",
                data_type
            )));
        }
    }

    for i in 0..row_id_array.len() {
        temp_query_token_counts.clear();
        temp_query_token_counts.extend(std::iter::repeat_n(0, query_tokens_len));
        let mut all_tokens = 0;
        let mut phrase_match = false;
        if !doc_array.is_null(i) {
            let elements = doc_array.value(i);
            if match_phrase {
                let mut document = String::new();
                for element in iter_str_array(elements.as_ref()).flatten() {
                    if !document.is_empty() {
                        document.push(' ');
                    }
                    document.push_str(element);
                }
                (all_tokens, phrase_match) = count_text(&document, temp_query_token_counts)?;
            } else {
                for element in iter_str_array(elements.as_ref()).flatten() {
                    all_tokens += count_text(element, temp_query_token_counts)?.0;
                }
            }
        }
        if all_tokens > 0 {
            append_counts(
                i,
                row_id_array.value(i),
                all_tokens,
                temp_query_token_counts,
                phrase_match,
            )?;
        }
    }

    Ok(())
}

fn query_token_indices(query_tokens: &Tokens) -> HashMap<String, Vec<usize>> {
    let mut indices = HashMap::new();
    for idx in 0..query_tokens.len() {
        indices
            .entry(query_tokens.get_token(idx).to_string())
            .or_insert_with(Vec::new)
            .push(idx);
    }
    indices
}

/// Initialize the BM25 scorer
///
/// In order to calculate BM25 scores we need to know token counts for the entire corpus.  We extract these from the
/// counted input of the flat search combined with any counts recorded for the indexed portion.
fn initialize_scorer(
    base_scorer: Option<&MemBM25Scorer>,
    query_tokens: &Tokens,
    counted_input: &RecordBatch,
) -> MemBM25Scorer {
    let mut total_tokens = 0;
    let mut num_docs = 0;
    let mut all_token_counts = vec![0; query_tokens.len()];

    if let Some(base_scorer) = base_scorer {
        total_tokens += base_scorer.total_tokens;
        num_docs += base_scorer.num_docs;
        for (token_index, token) in query_tokens.into_iter().enumerate() {
            all_token_counts[token_index] = base_scorer.num_docs_containing_token(token) as u64;
        }
    }

    num_docs += counted_input.num_rows();
    total_tokens +=
        arrow::compute::sum(counted_input[FLAT_ALL_TOKENS_COL].as_primitive::<UInt64Type>())
            .unwrap_or_default();

    let mut input_token_counters = counted_input[FLAT_QUERY_TOKEN_COUNTS_COL]
        .as_fixed_size_list()
        .values()
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied();

    for _ in 0..counted_input.num_rows() {
        for token_count in all_token_counts.iter_mut() {
            if input_token_counters.next().unwrap_or_default() > 0 {
                *token_count += 1;
            }
        }
    }

    let token_counts_map = all_token_counts
        .into_iter()
        .enumerate()
        .map(|(token_index, count)| {
            (
                query_tokens.get_token(token_index).to_string(),
                count as usize,
            )
        })
        .collect::<HashMap<String, usize>>();
    MemBM25Scorer::new(total_tokens, num_docs, token_counts_map)
}

fn flat_bm25_score(
    query_tokens: &Tokens,
    counted_input: &RecordBatch,
    scorer: &MemBM25Scorer,
    document_granularity: DocumentGranularity,
    operator: Operator,
    boost: f32,
    phrase_slop: Option<u32>,
) -> Result<RecordBatch> {
    let mut row_ids_builder = UInt64Builder::with_capacity(counted_input.num_rows());
    let mut scores_builder = Float32Builder::with_capacity(counted_input.num_rows());
    let coordinate_rank = document_coordinate_rank(counted_input.schema().as_ref());
    let input_doc_indices = (0..coordinate_rank)
        .map(|rank| {
            let coordinate_column = doc_index_storage_column(rank);
            counted_input
                .column_by_name(&coordinate_column)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "flat ListElement FTS is missing {coordinate_column}"
                    ))
                })
                .map(|column| column.as_primitive::<UInt32Type>())
        })
        .collect::<Result<Vec<_>>>()?;
    if document_granularity.is_list_element() != (coordinate_rank > 0) {
        return Err(Error::internal(
            "flat FTS document granularity does not match its coordinate columns".to_string(),
        ));
    }
    let mut doc_indices_builder = document_granularity.is_list_element().then(|| {
        ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
            "item",
            DataType::UInt32,
            false,
        ))
    });
    let query_groups = query_position_groups(query_tokens);

    let mut row_ids_iter = counted_input[ROW_ID]
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied();
    let mut all_token_counts_iter = counted_input[FLAT_ALL_TOKENS_COL]
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied();
    let mut query_token_counts_iter = counted_input[FLAT_QUERY_TOKEN_COUNTS_COL]
        .as_fixed_size_list()
        .values()
        .as_primitive::<UInt64Type>()
        .values()
        .iter()
        .copied();
    let phrase_matches = phrase_slop
        .map(|_| {
            counted_input
                .column_by_name(FLAT_PHRASE_MATCH_COL)
                .ok_or_else(|| Error::internal("flat phrase FTS is missing phrase matches"))
                .and_then(|column| {
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            Error::internal("flat phrase matches must be a Boolean column")
                        })
                })
        })
        .transpose()?;
    for input_index in 0..counted_input.num_rows() {
        let num_tokens_in_doc = all_token_counts_iter.next().expect_ok()?;
        let row_id = row_ids_iter.next().expect_ok()?;
        let mut query_token_counts = Vec::with_capacity(query_tokens.len());
        for _ in query_tokens {
            query_token_counts.push(query_token_counts_iter.next().expect_ok()?);
        }
        if num_tokens_in_doc == 0 {
            continue;
        }
        if operator == Operator::And
            && !query_groups
                .iter()
                .all(|group| group.iter().any(|idx| query_token_counts[*idx] > 0))
        {
            continue;
        }
        if phrase_matches.is_some_and(|matches| !matches.value(input_index)) {
            continue;
        }
        let doc_norm = K1 * (1.0 - B + B * num_tokens_in_doc as f32 / scorer.avg_doc_length());
        let mut score = 0.0;
        for (token, freq) in query_tokens.into_iter().zip(query_token_counts) {
            let freq = freq as f32;
            let idf = idf(scorer.num_docs_containing_token(token), scorer.num_docs());
            score += idf * (freq * (K1 + 1.0) / (freq + doc_norm));
        }
        if score > 0.0 {
            row_ids_builder.append_value(row_id);
            if let Some(builder) = doc_indices_builder.as_mut() {
                for input_doc_index in &input_doc_indices {
                    builder
                        .values()
                        .append_value(input_doc_index.value(input_index));
                }
                builder.append(true);
            }
            scores_builder.append_value(score * boost);
        }
    }

    let row_ids = row_ids_builder.finish();
    let scores = scores_builder.finish();
    let mut columns = vec![Arc::new(row_ids) as ArrayRef];
    if let Some(mut builder) = doc_indices_builder {
        columns.push(Arc::new(builder.finish()) as ArrayRef);
    }
    columns.push(Arc::new(scores) as ArrayRef);
    let batch = RecordBatch::try_new(fts_schema(document_granularity), columns)?;
    Ok(batch)
}

fn query_position_groups(query_tokens: &Tokens) -> Vec<Vec<usize>> {
    let mut groups = Vec::new();
    let mut current_position = None;
    for idx in 0..query_tokens.len() {
        let position = query_tokens.position(idx);
        if current_position != Some(position) {
            current_position = Some(position);
            groups.push(Vec::new());
        }
        groups
            .last_mut()
            .expect("a group should exist after pushing for position")
            .push(idx);
    }
    groups
}

#[deprecated(
    note = "use `flat_bm25_search_stream_with_metrics` to record CPU compute \
            time on a metric handle; pass `None` for the old behavior"
)]
pub async fn flat_bm25_search_stream(
    input: SendableRecordBatchStream,
    doc_col: String,
    query: String,
    tokenizer: Box<dyn LanceTokenizer>,
    base_scorer: Option<MemBM25Scorer>,
    target_batch_size: usize,
) -> DataFusionResult<SendableRecordBatchStream> {
    flat_bm25_search_stream_with_metrics(
        input,
        doc_col,
        query,
        tokenizer,
        base_scorer,
        target_batch_size,
        None,
    )
    .await
}

/// Same as [`flat_bm25_search_stream`] but accepts an optional `Time` handle
/// that, if provided, will receive the CPU time spent in (a) per-batch
/// tokenization on the `spawn_cpu` worker threads and (b) the synchronous
/// scoring phase. This lets a calling `ExecutionPlan` report accurate
/// `elapsed_compute` without double-counting upstream poll time.
pub async fn flat_bm25_search_stream_with_metrics(
    input: SendableRecordBatchStream,
    doc_col: String,
    query: String,
    tokenizer: Box<dyn LanceTokenizer>,
    base_scorer: Option<MemBM25Scorer>,
    target_batch_size: usize,
    elapsed_compute: Option<Time>,
) -> DataFusionResult<SendableRecordBatchStream> {
    flat_bm25_search_stream_with_metrics_and_operator(
        input,
        doc_col,
        query,
        tokenizer,
        base_scorer,
        target_batch_size,
        Operator::Or,
        elapsed_compute,
    )
    .await
}

/// Same as [`flat_bm25_search_stream_with_metrics`] but applies the provided
/// match operator when deciding whether a flat-scanned row is a hit.
///
/// # Examples
///
/// ```no_run
/// # async fn example(
/// #     input: datafusion::execution::SendableRecordBatchStream,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// use lance_index::scalar::inverted::{
///     flat_bm25_search_stream_with_metrics_and_operator, query::Operator, InvertedIndexParams,
/// };
///
/// let tokenizer = InvertedIndexParams::code().build()?;
/// let _stream = flat_bm25_search_stream_with_metrics_and_operator(
///     input,
///     "code".to_string(),
///     "Result".to_string(),
///     tokenizer,
///     None,
///     1024,
///     Operator::And,
///     None,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn flat_bm25_search_stream_with_metrics_and_operator(
    input: SendableRecordBatchStream,
    doc_col: String,
    query: String,
    tokenizer: Box<dyn LanceTokenizer>,
    base_scorer: Option<MemBM25Scorer>,
    target_batch_size: usize,
    operator: Operator,
    elapsed_compute: Option<Time>,
) -> DataFusionResult<SendableRecordBatchStream> {
    Ok(flat_bm25_search_stream_with_options_and_scorer(
        input,
        doc_col,
        query,
        tokenizer,
        base_scorer,
        FlatBm25SearchOptions {
            target_batch_size,
            elapsed_compute,
            document_granularity: DocumentGranularity::Row,
            operator,
            boost: 1.0,
            phrase_slop: None,
        },
    )
    .await?
    .0)
}

/// Options for flat BM25 search execution.
pub struct FlatBm25SearchOptions {
    /// Maximum output record batch size.
    pub target_batch_size: usize,
    /// Optional DataFusion metric tracking compute time.
    pub elapsed_compute: Option<Time>,
    /// Logical FTS document boundary.
    pub document_granularity: DocumentGranularity,
    /// Match operator applied within each logical document.
    pub operator: Operator,
    /// Score multiplier for the match query.
    pub boost: f32,
    /// Phrase slop. When set, documents must contain the query tokens in order.
    pub phrase_slop: Option<u32>,
}

/// Run a flat BM25 search and return the scorer initialized from both the
/// optional indexed corpus and the flat input corpus.
pub async fn flat_bm25_search_stream_with_options_and_scorer(
    input: SendableRecordBatchStream,
    doc_col: String,
    query: String,
    tokenizer: Box<dyn LanceTokenizer>,
    base_scorer: Option<MemBM25Scorer>,
    options: FlatBm25SearchOptions,
) -> DataFusionResult<(SendableRecordBatchStream, MemBM25Scorer)> {
    let FlatBm25SearchOptions {
        target_batch_size,
        elapsed_compute,
        document_granularity,
        operator,
        boost,
        phrase_slop,
    } = options;
    let mut tokenizer = tokenizer;
    let output_schema = fts_schema(document_granularity);

    // Pre-await synchronous work: query tokenization + chunk-stream setup.
    let pre_await_start = std::time::Instant::now();
    let query_tokens = Arc::new(collect_query_tokens(&query, &mut tokenizer));

    // A query that tokenizes to no terms (e.g. only stop words) has no
    // searchable content and matches nothing. Return early rather than
    // proceeding. This mirrors the indexed search path, which already
    // short-circuits on empty query tokens.
    if query_tokens.is_empty() {
        let scorer = base_scorer.unwrap_or_else(|| MemBM25Scorer::new(0, 0, HashMap::new()));
        return Ok((
            Box::pin(RecordBatchStreamAdapter::new(
                output_schema,
                stream::empty::<DataFusionResult<RecordBatch>>(),
            )),
            scorer,
        ));
    }

    let input_schema = input.schema();
    let doc_col_idx = input_schema.index_of(&doc_col)?;
    let coordinate_rank = document_coordinate_rank(input_schema.as_ref());
    if document_granularity.is_list_element() != (coordinate_rank > 0) {
        return Err(datafusion_common::DataFusionError::Execution(
            "flat FTS document granularity does not match its input coordinate columns".to_string(),
        ));
    }

    // Accumulate small batches until this threshold before dispatching a task.
    const ACCUMULATE_BYTES: usize = 256 * 1024;
    // Slice oversized batches down to roughly this size.
    const SLICE_BYTES: usize = 512 * 1024;

    // Phase 1 - rechunk the input stream into appropriately sized chunks.  Tokenization is
    // fairly CPU-intensive, and we don't need too much data to justify a new thread task.
    let chunked = lance_arrow::stream::rechunk_stream_by_size(
        input,
        input_schema,
        ACCUMULATE_BYTES,
        SLICE_BYTES,
    );
    if let Some(t) = &elapsed_compute {
        t.add_duration(pre_await_start.elapsed());
    }

    // Phase 2 - For each row we need to know the total number of tokens and the count of each
    // of the query tokens.  For example, if the query is "book" and the row is "the book shop"
    // and we are tokenizing with a whitespace tokenizer, we need to know that there are 3 tokens
    // and the token book appears once.
    let counted_input = tokenize_and_count(
        chunked,
        tokenizer,
        query_tokens.clone(),
        doc_col_idx,
        elapsed_compute.clone(),
        coordinate_rank,
        phrase_slop,
    )
    .await?;

    // Phase 3 - Calculate final scores (this is fairly cheap, probably don't need to parallelize).
    // All post-await work is synchronous; time the scorer + score + slicing loop together.
    let post_await_start = std::time::Instant::now();
    let scorer = initialize_scorer(base_scorer.as_ref(), query_tokens.as_ref(), &counted_input);
    let scores = flat_bm25_score(
        query_tokens.as_ref(),
        &counted_input,
        &scorer,
        document_granularity,
        operator,
        boost,
        phrase_slop,
    )?;

    // Finally we emit batches according to the target batch size
    let num_out_batches = scores.num_rows().div_ceil(target_batch_size);
    let mut batches = Vec::with_capacity(num_out_batches);
    for i in 0..num_out_batches {
        let start = i * target_batch_size;
        let len = (scores.num_rows() - start).min(target_batch_size);
        batches.push(Ok(scores.slice(start, len)));
    }
    if let Some(t) = &elapsed_compute {
        t.add_duration(post_await_start.elapsed());
    }
    Ok((
        Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream::iter(batches),
        )),
        scorer,
    ))
}

pub fn is_phrase_query(query: &str) -> bool {
    phrase_query_text(query).is_some()
}

fn phrase_query_text(query: &str) -> Option<&str> {
    query.strip_prefix('\"')?.strip_suffix('\"')
}

#[cfg(test)]
mod tests {
    use crate::scalar::inverted::document_tokenizer::DocType;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use lance_core::cache::{LanceCache, QuickCacheBackend};
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::object_store::ObjectStore;

    use crate::metrics::{LocalMetricsCollector, NoOpMetricsCollector};
    use crate::prefilter::NoFilter;
    use crate::scalar::ScalarIndex;
    use crate::scalar::inverted::builder::{
        InnerBuilder, InvertedIndexBuilder, PositionRecorder, doc_file_path, inverted_list_schema,
        inverted_list_schema_for_version_with_block_size,
        inverted_list_schema_for_version_with_block_size_and_impacts, posting_file_path,
        token_file_path,
    };
    use crate::scalar::inverted::encoding::{
        compress_positions, compress_posting_list_with_tail_codec,
        decompress_posting_list_with_tail_codec, encode_position_stream_block_into,
    };
    use crate::scalar::inverted::query::{FtsSearchParams, Operator};
    use crate::scalar::lance_format::LanceIndexStore;
    use arrow::array::{
        AsArray, GenericListBuilder, GenericStringBuilder, Int32Builder, LargeBinaryBuilder,
        ListBuilder, UInt32Builder,
    };
    use arrow::datatypes::{Float32Type, UInt32Type};
    use arrow_array::{ArrayRef, Float32Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::scalar::inverted::tokenizer::document_tokenizer::TextTokenizer;
    use lance_tokenizer::{Language, SimpleTokenizer, StopWordFilter, TextAnalyzer};

    use super::*;

    #[test]
    fn address_read_concurrency_respects_payload_budget() {
        assert_eq!(address_read_concurrency(64, 0), 64);
        assert_eq!(address_read_concurrency(64, 8 * 1024 * 1024), 8);
        assert_eq!(address_read_concurrency(64, 16 * 1024 * 1024), 4);
        assert_eq!(
            address_read_concurrency(64, 2 * MAX_CONCURRENT_ADDRESS_READ_BYTES),
            1
        );
    }

    #[derive(Debug)]
    struct MetadataAccessDeniedStore {
        inner: Arc<dyn IndexStore>,
    }

    impl DeepSizeOf for MetadataAccessDeniedStore {
        fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
            self.inner.deep_size_of_children(context)
        }
    }

    #[async_trait]
    impl IndexStore for MetadataAccessDeniedStore {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.clone(),
            })
        }

        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }

        async fn new_index_file(
            &self,
            name: &str,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn crate::scalar::IndexWriter>> {
            self.inner.new_index_file(name, schema).await
        }

        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
            if name == METADATA_FILE {
                Err(Error::io("metadata access denied"))
            } else {
                self.inner.open_index_file(name).await
            }
        }

        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.with_io_priority(io_priority),
            })
        }

        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<crate::scalar::IndexFile> {
            self.inner.copy_index_file(name, dest_store).await
        }

        async fn rename_index_file(
            &self,
            name: &str,
            new_name: &str,
        ) -> Result<crate::scalar::IndexFile> {
            self.inner.rename_index_file(name, new_name).await
        }

        async fn delete_index_file(&self, name: &str) -> Result<()> {
            self.inner.delete_index_file(name).await
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<crate::scalar::IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    #[tokio::test]
    async fn params_legacy_fallback_probes_tokens_after_metadata_access_denied() {
        let tmpdir = TempObjDir::default();
        let inner: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let expected = InvertedIndexParams::default();
        let metadata = HashMap::from([(
            "tokenizer".to_owned(),
            serde_json::to_string(&expected).unwrap(),
        )]);
        let mut writer = inner
            .new_index_file(TOKENS_FILE, Arc::new(Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();
        let store = MetadataAccessDeniedStore { inner };

        let actual = InvertedIndex::load_params(&store).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[tokio::test]
    async fn params_legacy_probe_preserves_metadata_error_when_tokens_are_missing() {
        let tmpdir = TempObjDir::default();
        let inner: Arc<dyn IndexStore> = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let store = MetadataAccessDeniedStore { inner };

        let error = InvertedIndex::load_params(&store).await.unwrap_err();
        assert!(matches!(error, Error::IO { .. }));
        assert!(error.to_string().contains("metadata access denied"));
    }

    #[tokio::test]
    async fn params_metadata_ignores_unknown_fields() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let expected = InvertedIndexParams::default();
        let mut params = serde_json::to_value(&expected).unwrap();
        let params = params.as_object_mut().unwrap();
        params.insert("skip_merge".to_owned(), true.into());
        params.insert(
            "future_parameter".to_owned(),
            serde_json::json!({ "enabled": true }),
        );
        let metadata =
            HashMap::from([("params".to_owned(), serde_json::to_string(params).unwrap())]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let actual = InvertedIndex::load_params(store.as_ref()).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    async fn write_single_partition_index(
        store: Arc<LanceIndexStore>,
        params: InvertedIndexParams,
        token_set_format: TokenSetFormat,
        token: &str,
        row_id: u64,
    ) -> Result<Arc<InvertedIndex>> {
        let block_size = params.posting_block_size();
        let format_version = params.resolved_format_version();
        let mut partition = InnerBuilder::new_with_format_version_and_block_size(
            0,
            false,
            token_set_format,
            format_version,
            block_size,
        );
        partition.tokens.add(token.to_owned());
        let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            format_version.posting_tail_codec(),
            block_size,
        );
        posting_list.add(0, PositionRecorder::Count(1));
        partition.posting_lists.push(posting_list);
        partition.docs.append(row_id, 1);
        partition.write(store.as_ref()).await?;

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0_u64]).unwrap(),
            ),
            ("params".to_owned(), serde_json::to_string(&params).unwrap()),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                token_set_format.to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                format_version.posting_tail_codec().as_str().to_owned(),
            ),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                format_version.index_version().to_string(),
            ),
            (POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string()),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await?;
        writer.finish_with_metadata(metadata).await?;

        InvertedIndex::load(store, None, &LanceCache::no_cache()).await
    }

    fn empty_doc_stream() -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(Vec::<datafusion::error::Result<RecordBatch>>::new()),
        ))
    }

    #[test]
    fn test_posting_block_size_schema_metadata() {
        assert_eq!(parse_posting_block_size(&HashMap::new()).unwrap(), 128);

        let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "512".to_owned())]);
        let err = parse_posting_block_size(&metadata).unwrap_err();
        assert!(err.to_string().contains("block_size"));

        let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "129".to_owned())]);
        let err = parse_posting_block_size(&metadata).unwrap_err();
        assert!(err.to_string().contains("block_size"));
    }

    #[test]
    fn test_num_tokens_only_reuses_sliced_arrow_storage() {
        let docs = {
            let source = UInt32Array::from(vec![999, 7, 16, 1024, 888]);
            let sliced = source.slice(1, 3);
            let mut docs = DocSet::from_num_tokens_only(&sliced);

            let NumTokens::Shared(values) = &docs.num_tokens else {
                panic!("num-tokens-only DocSet must retain shared Arrow storage");
            };
            assert!(values.ptr_eq(sliced.values()));
            assert_eq!(values.as_ref(), &[7, 16, 1024]);
            assert_eq!(docs.total_tokens_num(), 1047);
            docs.set_quantized_scoring(true);
            assert_eq!(docs.scoring_norms().unwrap().len(), 3);
            assert_eq!(
                docs.scoring_num_tokens(0),
                dequantize_doc_length(quantize_doc_length(7))
            );
            assert_eq!(
                docs.scoring_num_tokens(2),
                dequantize_doc_length(quantize_doc_length(1024))
            );
            docs
        };

        assert_eq!(docs.len(), 3);
        assert_eq!(docs.num_tokens(0), 7);
        assert_eq!(docs.num_tokens(2), 1024);
    }

    #[test]
    fn test_cached_num_tokens_uses_supplied_total_and_full_stays_owned() {
        const CACHED_TOTAL_MARKER: u64 = 123_456;

        let num_tokens = UInt32Array::from(vec![3, 5, 8]);
        let docs = DocSet::from_cached_num_tokens(&num_tokens, CACHED_TOTAL_MARKER);
        assert_eq!(docs.total_tokens_num(), CACHED_TOTAL_MARKER);
        assert!(matches!(&docs.num_tokens, NumTokens::Shared(_)));

        let row_ids = UInt64Array::from(vec![10, 20, 30]);
        let full = DocSet::from_columns(&row_ids, &num_tokens, false, None).unwrap();
        assert!(matches!(&full.num_tokens, NumTokens::Owned(_)));
        assert_eq!(full.total_tokens_num(), 16);
        assert_eq!(full.row_id(1), 20);
    }

    #[test]
    fn test_posting_builder_writes_impacts_for_supported_block_sizes() {
        for block_size in [128, 256] {
            let format_version = default_fts_format_version_for_block_size(block_size).unwrap();
            let num_docs = block_size * 33 + 1;
            let mut docs = DocSet::default();
            let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                false,
                format_version.posting_tail_codec(),
                block_size,
            );
            for doc_id in 0..num_docs {
                docs.append(doc_id as u64, (doc_id % 5 + 1) as u32);
                posting.add(
                    doc_id as u32,
                    PositionRecorder::Count((doc_id % 3 + 1) as u32),
                );
            }
            let schema =
                inverted_list_schema_for_version_with_block_size(false, format_version, block_size);
            let batch = posting.to_batch_with_docs(&docs, schema).unwrap();
            assert!(batch.column_by_name(IMPACT_COL).is_some());
            let max_score = batch[MAX_SCORE_COL].as_primitive::<Float32Type>().value(0);
            let length = batch[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
            let posting = PostingList::from_batch(&batch, Some(max_score), Some(length)).unwrap();
            let PostingList::Compressed(posting) = posting else {
                panic!("expected compressed posting list");
            };
            let impacts = posting.impacts.expect("posting should include impacts");
            assert_eq!(impacts.level0_len(), posting.blocks.len());
            assert_eq!(impacts.level1_len(), posting.blocks.len().div_ceil(32));
            assert_eq!(
                impacts.entries().len(),
                impacts.level0_len() + impacts.level1_len()
            );
        }
    }

    #[test]
    fn test_posting_builder_without_impact_column_roundtrips_without_impacts() {
        let mut posting = PostingListBuilder::new(false);
        for doc_id in 0..BLOCK_SIZE + 3 {
            posting.add(doc_id as u32, PositionRecorder::Count(1));
        }
        let batch = posting.to_batch(vec![1.0, 1.0]).unwrap();
        assert!(batch.column_by_name(IMPACT_COL).is_none());
        let posting =
            PostingList::from_batch(&batch, Some(1.0), Some((BLOCK_SIZE + 3) as u32)).unwrap();
        assert!(!posting.has_impacts());
    }

    #[tokio::test]
    async fn test_build_search_uses_configured_posting_block_size() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let params = InvertedIndexParams::default().block_size(256).unwrap();
        let format_version = params.resolved_format_version();
        let block_size = params.posting_block_size();
        let num_docs = block_size + 7;

        let mut builder = InnerBuilder::new_with_format_version_and_block_size(
            0,
            false,
            TokenSetFormat::default(),
            format_version,
            block_size,
        );
        builder.tokens.add("needle".to_owned());
        let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            format_version.posting_tail_codec(),
            block_size,
        );
        for doc_id in 0..num_docs {
            posting_list.add(doc_id as u32, PositionRecorder::Count(1));
            builder.docs.append(1_000 + doc_id as u64, 1);
        }
        builder.posting_lists.push(posting_list);
        builder.write(store.as_ref()).await.unwrap();
        write_test_metadata(&store, vec![0], params).await;

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        assert_eq!(index.partitions[0].inverted_list.block_size(), block_size);

        let posting = index.partitions[0]
            .inverted_list
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        let PostingList::Compressed(posting) = posting else {
            panic!("expected compressed posting list");
        };
        assert_eq!(posting.block_size, block_size);
        assert_eq!(posting.blocks.len(), num_docs.div_ceil(block_size));
        let impacts = posting
            .impacts
            .as_ref()
            .expect("newly written posting list should include impacts");
        assert_eq!(impacts.level0_len(), posting.blocks.len());
        assert_eq!(impacts.level1_len(), posting.blocks.len().div_ceil(32));
        assert_eq!(
            impacts.entries().len(),
            impacts.level0_len() + impacts.level1_len()
        );

        let tokens = Arc::new(Tokens::new(vec!["needle".to_owned()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);
        let (row_ids, scores) = index
            .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
            .await
            .unwrap();

        assert_eq!(row_ids.len(), 10);
        assert_eq!(scores.len(), 10);
        assert!(row_ids.iter().all(|row_id| *row_id >= 1_000));
    }

    #[tokio::test]
    async fn test_posting_builder_remap() {
        let posting_tail_codec = PostingTailCodec::Fixed32;
        let mut builder =
            PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
        let n = BLOCK_SIZE + 3;
        for i in 0..n {
            builder.add(i as u32, PositionRecorder::Count(1));
        }
        let removed = vec![5, 7];
        builder.remap(&removed);

        let mut expected =
            PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
        for i in 0..n - removed.len() {
            expected.add(i as u32, PositionRecorder::Count(1));
        }
        let expected_entries = expected.iter().collect::<Vec<_>>();
        let actual_entries = builder.iter().collect::<Vec<_>>();
        assert_eq!(actual_entries, expected_entries);

        // BLOCK_SIZE + 3 elements should be reduced to BLOCK_SIZE + 1,
        // there are still 2 blocks.
        let batch = builder.to_batch(vec![1.0, 2.0]).unwrap();
        let (doc_ids, freqs) = decompress_posting_list_with_tail_codec(
            (n - removed.len()) as u32,
            batch[POSTING_COL]
                .as_list::<i32>()
                .value(0)
                .as_binary::<i64>(),
            posting_tail_codec,
        )
        .unwrap();
        assert!(
            doc_ids
                .iter()
                .zip(expected_entries.iter().map(|(doc_id, _, _)| doc_id))
                .all(|(a, b)| a == b)
        );
        assert!(
            freqs
                .iter()
                .zip(expected_entries.iter().map(|(_, freq, _)| freq))
                .all(|(a, b)| a == b)
        );
    }

    #[test]
    fn test_posting_builder_size_tracking_matches_structure() {
        fn tracked_memory_size(builder: &PostingListBuilder) -> u64 {
            let encoded_blocks_size = builder
                .encoded_blocks
                .iter()
                .map(|encoded_blocks| std::mem::size_of::<EncodedBlocks>() + encoded_blocks.size())
                .sum::<usize>();
            let encoded_positions_size = builder
                .encoded_position_blocks
                .as_ref()
                .map(|positions| std::mem::size_of::<EncodedPositionBlocks>() + positions.size())
                .unwrap_or(0usize);
            (encoded_blocks_size
                + builder.tail_entries.capacity() * std::mem::size_of::<RawDocInfo>()
                + builder.tail_positions.size()
                + encoded_positions_size) as u64
        }

        let mut builder = PostingListBuilder::new(true);
        for doc_id in 0..(BLOCK_SIZE + 5) as u32 {
            builder.add(
                doc_id,
                PositionRecorder::Position(smallvec::smallvec![1, 3, 5]),
            );
        }

        assert_eq!(builder.size(), tracked_memory_size(&builder));
    }

    #[test]
    fn test_posting_builder_flush_releases_tail_position_capacity() {
        let mut builder = PostingListBuilder::new(true);
        let positions = smallvec::SmallVec::<[u32; 2]>::from_vec((0..1024).collect());
        for doc_id in 0..BLOCK_SIZE as u32 {
            builder.add(doc_id, PositionRecorder::Position(positions.clone()));
        }

        assert_eq!(builder.tail_positions.size(), 0);
        assert_eq!(builder.size(), {
            let encoded_blocks_size = builder
                .encoded_blocks
                .iter()
                .map(|encoded_blocks| std::mem::size_of::<EncodedBlocks>() + encoded_blocks.size())
                .sum::<usize>();
            let encoded_positions_size = builder
                .encoded_position_blocks
                .as_ref()
                .map(|positions| std::mem::size_of::<EncodedPositionBlocks>() + positions.size())
                .unwrap_or(0usize);
            (encoded_blocks_size
                + builder.tail_entries.capacity() * std::mem::size_of::<RawDocInfo>()
                + builder.tail_positions.size()
                + encoded_positions_size) as u64
        });
    }

    #[test]
    fn test_posting_builder_streamed_positions_roundtrip() {
        let mut builder = PostingListBuilder::new(true);
        assert!(builder.add_occurrence(0, 1).unwrap());
        assert!(!builder.add_occurrence(0, 4).unwrap());
        assert!(!builder.add_occurrence(0, 9).unwrap());
        builder.finish_open_doc(0).unwrap();

        assert!(builder.add_occurrence(2, 3).unwrap());
        builder.finish_open_doc(2).unwrap();

        let entries = builder.iter().collect::<Vec<_>>();
        assert_eq!(
            entries,
            vec![
                (0_u32, 3_u32, Some(vec![1_u32, 4_u32, 9_u32])),
                (2_u32, 1_u32, Some(vec![3_u32])),
            ]
        );
    }

    #[test]
    fn test_shared_position_stream_clone_shares_block_offsets() {
        let stream = SharedPositionStream::new(
            PositionStreamCodec::PackedDelta,
            vec![0_u32, 4, 11],
            bytes::Bytes::from_static(b"shared position bytes"),
        );
        let original_offsets = stream.block_offsets().as_ptr();

        let cloned = stream.clone();

        assert_eq!(cloned.block_offsets(), stream.block_offsets());
        assert_eq!(cloned.block_offsets().as_ptr(), original_offsets);
    }

    #[test]
    fn test_posting_builder_roundtrip_shared_positions() {
        let entries = vec![
            (0_u32, vec![1_u32, 5]),
            (2, vec![0, 4, 9]),
            (4, vec![7]),
            (8, vec![3, 10]),
            (13, vec![2, 11, 30]),
        ];
        let mut builder =
            PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::VarintDelta);
        for (doc_id, positions) in &entries {
            builder.add(
                *doc_id,
                PositionRecorder::Position(positions.clone().into()),
            );
        }

        let batch = builder.to_batch(vec![1.0]).unwrap();
        assert!(batch.column_by_name(COMPRESSED_POSITION_COL).is_some());
        assert!(batch.column_by_name(POSITION_COL).is_none());
        assert_eq!(
            batch.schema_ref().metadata().get(POSTING_TAIL_CODEC_KEY),
            Some(&PostingTailCodec::VarintDelta.as_str().to_owned())
        );
        assert_eq!(
            batch.schema_ref().metadata().get(POSITIONS_LAYOUT_KEY),
            Some(&POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned())
        );
        assert_eq!(
            batch.schema_ref().metadata().get(POSITIONS_CODEC_KEY),
            Some(&PositionStreamCodec::PackedDelta.as_str().to_owned())
        );

        let posting =
            PostingList::from_batch(&batch, Some(1.0), Some(entries.len() as u32)).unwrap();
        let actual = posting
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let expected = entries
            .iter()
            .map(|(doc_id, positions)| (*doc_id, positions.len() as u32, positions.clone()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_posting_builder_roundtrip_legacy_positions() {
        let entries = vec![(0_u32, vec![1_u32, 5]), (2, vec![0, 4, 9]), (4, vec![7])];
        let mut builder =
            PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::Fixed32);
        for (doc_id, positions) in &entries {
            builder.add(
                *doc_id,
                PositionRecorder::Position(positions.clone().into()),
            );
        }

        let batch = builder.to_batch(vec![1.0]).unwrap();
        assert!(batch.column_by_name(POSITION_COL).is_some());
        assert!(batch.column_by_name(COMPRESSED_POSITION_COL).is_none());
        assert_eq!(
            batch.schema_ref().metadata().get(POSTING_TAIL_CODEC_KEY),
            None
        );
        assert_eq!(
            batch.schema_ref().metadata().get(POSITIONS_LAYOUT_KEY),
            None
        );
        assert_eq!(batch.schema_ref().metadata().get(POSITIONS_CODEC_KEY), None);

        let posting =
            PostingList::from_batch(&batch, Some(1.0), Some(entries.len() as u32)).unwrap();
        let actual = posting
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let expected = entries
            .iter()
            .map(|(doc_id, positions)| (*doc_id, positions.len() as u32, positions.clone()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_resolve_fts_format_version_defaults_to_v2() {
        assert_eq!(
            resolve_fts_format_version(None).unwrap(),
            InvertedListFormatVersion::V2
        );
        assert_eq!(
            resolve_fts_format_version(Some("2")).unwrap(),
            InvertedListFormatVersion::V2
        );
        assert_eq!(
            resolve_fts_format_version(Some("3")).unwrap(),
            InvertedListFormatVersion::V3
        );
        assert!(resolve_fts_format_version(Some("4")).is_err());
    }

    #[test]
    fn test_block_size_256_metadata_resolves_to_v3() {
        let metadata = HashMap::from([(POSTING_BLOCK_SIZE_KEY.to_owned(), "256".to_owned())]);
        assert_eq!(
            parse_format_version_from_metadata(&metadata).unwrap(),
            InvertedListFormatVersion::V3
        );
    }

    #[test]
    fn test_legacy_compressed_positions_still_readable() {
        let doc_ids = [1_u32, 3_u32];
        let frequencies = [2_u32, 3_u32];
        let posting = compress_posting_list_with_tail_codec(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(1.0_f32),
            PostingTailCodec::Fixed32,
        )
        .unwrap();

        let mut posting_builder = ListBuilder::new(LargeBinaryBuilder::new());
        for idx in 0..posting.len() {
            posting_builder.values().append_value(posting.value(idx));
        }
        posting_builder.append(true);

        let mut positions_builder = ListBuilder::new(ListBuilder::new(LargeBinaryBuilder::new()));
        for positions in [vec![1_u32, 5_u32], vec![0_u32, 4_u32, 9_u32]] {
            let compressed = compress_positions(&positions).unwrap();
            let doc_builder = positions_builder.values();
            for idx in 0..compressed.len() {
                doc_builder.values().append_value(compressed.value(idx));
            }
            doc_builder.append(true);
        }
        positions_builder.append(true);

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                POSTING_COL,
                DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
                false,
            ),
            Field::new(MAX_SCORE_COL, DataType::Float32, false),
            Field::new(LENGTH_COL, DataType::UInt32, false),
            Field::new(
                POSITION_COL,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
                    true,
                ))),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(posting_builder.finish()) as ArrayRef,
                Arc::new(Float32Array::from(vec![1.0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![doc_ids.len() as u32])) as ArrayRef,
                Arc::new(positions_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        let posting =
            PostingList::from_batch(&batch, Some(1.0), Some(doc_ids.len() as u32)).unwrap();
        let actual = posting
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, 2, vec![1, 5]), (3, 3, vec![0, 4, 9]),]);
    }

    #[test]
    fn test_shared_stream_v2_without_codec_still_readable() {
        let doc_ids = [1_u32, 3_u32];
        let frequencies = [2_u32, 3_u32];
        let posting = compress_posting_list_with_tail_codec(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(1.0_f32),
            PostingTailCodec::Fixed32,
        )
        .unwrap();

        let mut posting_builder = ListBuilder::new(LargeBinaryBuilder::new());
        for idx in 0..posting.len() {
            posting_builder.values().append_value(posting.value(idx));
        }
        posting_builder.append(true);

        let positions = vec![1_u32, 5_u32, 0_u32, 4_u32, 9_u32];
        let mut encoded_positions = Vec::new();
        encode_position_stream_block_into(
            &positions,
            &frequencies,
            PositionStreamCodec::VarintDocDelta,
            &mut encoded_positions,
        )
        .unwrap();

        let mut position_offsets = ListBuilder::new(UInt32Builder::new());
        position_offsets.values().append_value(0);
        position_offsets.append(true);

        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(
                    POSTING_COL,
                    DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, true))),
                    false,
                ),
                Field::new(MAX_SCORE_COL, DataType::Float32, false),
                Field::new(LENGTH_COL, DataType::UInt32, false),
                Field::new(COMPRESSED_POSITION_COL, DataType::LargeBinary, false),
                Field::new(
                    POSITION_BLOCK_OFFSET_COL,
                    DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                    false,
                ),
            ],
            HashMap::from([(
                POSITIONS_LAYOUT_KEY.to_owned(),
                POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
            )]),
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(posting_builder.finish()) as ArrayRef,
                Arc::new(Float32Array::from(vec![1.0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![doc_ids.len() as u32])) as ArrayRef,
                Arc::new(arrow_array::LargeBinaryArray::from(vec![Some(
                    encoded_positions.as_slice(),
                )])) as ArrayRef,
                Arc::new(position_offsets.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        let posting =
            PostingList::from_batch(&batch, Some(1.0), Some(doc_ids.len() as u32)).unwrap();
        let actual = posting
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, 2, vec![1, 5]), (3, 3, vec![0, 4, 9]),]);
    }

    #[test]
    fn test_shared_position_stream_is_smaller_for_sparse_positions() {
        let mut builder =
            PostingListBuilder::new_with_posting_tail_codec(true, PostingTailCodec::VarintDelta);
        let mut legacy_positions = Vec::with_capacity(BLOCK_SIZE * 4);
        for doc_id in 0..(BLOCK_SIZE * 4) as u32 {
            let mut positions = vec![doc_id * 3 + 1];
            if doc_id % 8 == 0 {
                positions.push(doc_id * 3 + 2);
            }
            builder.add(doc_id, PositionRecorder::Position(positions.clone().into()));
            legacy_positions.push(positions);
        }

        let batch = builder.to_batch(vec![1.0; 4]).unwrap();
        let shared_positions_size = batch[COMPRESSED_POSITION_COL].get_buffer_memory_size()
            + batch[POSITION_BLOCK_OFFSET_COL].get_buffer_memory_size();

        let mut positions_builder = ListBuilder::new(ListBuilder::new(LargeBinaryBuilder::new()));
        for positions in legacy_positions {
            let compressed = compress_positions(&positions).unwrap();
            let doc_builder = positions_builder.values();
            for idx in 0..compressed.len() {
                doc_builder.values().append_value(compressed.value(idx));
            }
            doc_builder.append(true);
        }
        positions_builder.append(true);
        let legacy_positions_size = positions_builder.finish().get_buffer_memory_size();

        assert!(
            shared_positions_size < legacy_positions_size,
            "expected shared position stream to be smaller than legacy per-doc storage, shared={shared_positions_size}, legacy={legacy_positions_size}",
        );
    }

    #[test]
    fn test_posting_list_batch_matches_docset_scoring() {
        let mut docs = DocSet::default();
        let num_docs = BLOCK_SIZE + 3;
        for doc_id in 0..num_docs as u32 {
            docs.append(doc_id as u64, doc_id % 7 + 1);
        }

        let doc_ids = (0..num_docs as u32).collect::<Vec<_>>();
        let freqs = doc_ids
            .iter()
            .map(|doc_id| doc_id % 5 + 1)
            .collect::<Vec<_>>();

        let mut builder_scores = PostingListBuilder::new(false);
        let mut builder_docs = PostingListBuilder::new(false);
        for (&doc_id, &freq) in doc_ids.iter().zip(freqs.iter()) {
            builder_scores.add(doc_id, PositionRecorder::Count(freq));
            builder_docs.add(doc_id, PositionRecorder::Count(freq));
        }

        let block_max_scores = docs.calculate_block_max_scores(doc_ids.iter(), freqs.iter());
        let batch_scores = builder_scores.to_batch(block_max_scores).unwrap();
        let batch_docs = builder_docs
            .to_batch_with_docs(&docs, inverted_list_schema(false))
            .unwrap();

        let scores_posting = batch_scores[POSTING_COL].as_list::<i32>().value(0);
        let scores_posting = scores_posting.as_binary::<i64>();
        let docs_posting = batch_docs[POSTING_COL].as_list::<i32>().value(0);
        let docs_posting = docs_posting.as_binary::<i64>();
        assert_eq!(scores_posting, docs_posting);

        let score_left = batch_scores[MAX_SCORE_COL]
            .as_primitive::<Float32Type>()
            .value(0);
        let score_right = batch_docs[MAX_SCORE_COL]
            .as_primitive::<Float32Type>()
            .value(0);
        assert!((score_left - score_right).abs() < 1e-6);

        let len_left = batch_scores[LENGTH_COL]
            .as_primitive::<UInt32Type>()
            .value(0);
        let len_right = batch_docs[LENGTH_COL].as_primitive::<UInt32Type>().value(0);
        assert_eq!(len_left, len_right);
    }

    #[tokio::test]
    async fn test_remap_to_empty_posting_list() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());

        // index of docs:
        // 0: lance
        // 1: lake lake
        // 2: lake lake lake
        builder.tokens.add("lance".to_owned());
        builder.tokens.add("lake".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.posting_lists[1].add(1, PositionRecorder::Count(2));
        builder.posting_lists[1].add(2, PositionRecorder::Count(3));
        builder.docs.append(0, 1);
        builder.docs.append(1, 1);
        builder.docs.append(2, 1);
        builder.write(store.as_ref()).await.unwrap();

        let index = InvertedPartition::load(
            store.clone(),
            0,
            None,
            &LanceCache::no_cache(),
            TokenSetFormat::default(),
        )
        .await
        .unwrap();
        let mut builder = index.into_builder().await.unwrap();

        let mapping = HashMap::from([(0, None), (2, Some(3))]);
        builder.remap(&RowAddrRemap::direct(mapping)).await.unwrap();

        // after remap, the doc 0 is removed, and the doc 2 is updated to 3
        assert_eq!(builder.tokens.len(), 1);
        assert_eq!(builder.tokens.get("lake"), Some(0));
        assert_eq!(builder.posting_lists.len(), 1);
        assert_eq!(builder.posting_lists[0].len(), 2);
        assert_eq!(builder.docs.len(), 2);
        assert_eq!(builder.docs.row_id(0), 1);
        assert_eq!(builder.docs.row_id(1), 3);

        builder.write(store.as_ref()).await.unwrap();

        // remap to delete all docs
        let mapping = HashMap::from([(1, None), (3, None)]);
        builder.remap(&RowAddrRemap::direct(mapping)).await.unwrap();

        assert_eq!(builder.tokens.len(), 0);
        assert_eq!(builder.posting_lists.len(), 0);
        assert_eq!(builder.docs.len(), 0);

        builder.write(store.as_ref()).await.unwrap();
    }

    #[test]
    fn test_docset_remap_preserves_element_coordinates() {
        let mut docs = DocSet::default();
        docs.append_with_doc_index(10, 2, &[0]).unwrap();
        docs.append_with_doc_index(10, 3, &[3]).unwrap();
        docs.append_with_doc_index(11, 1, &[1]).unwrap();

        let removed = docs.remap(&RowAddrRemap::direct(HashMap::from([
            (10, Some(20)),
            (11, None),
        ])));

        assert_eq!(removed, vec![2]);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs.row_id(0), 20);
        assert_eq!(docs.row_id(1), 20);
        assert_eq!(docs.doc_index(0), vec![0]);
        assert_eq!(docs.doc_index(1), vec![3]);
    }

    #[tokio::test]
    async fn test_posting_cache_conflict_across_partitions() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create first partition with one token and posting list length 1
        let mut builder1 = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder1.tokens.add("test".to_owned());
        builder1.posting_lists.push(PostingListBuilder::new(false));
        builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder1.docs.append(100, 1); // row_id=100, num_tokens=1
        builder1.write(store.as_ref()).await.unwrap();

        // Create second partition with one token and posting list length 4
        let mut builder2 = InnerBuilder::new(1, false, TokenSetFormat::default());
        builder2.tokens.add("test".to_owned()); // Use same token to test cache prefix fix
        builder2.posting_lists.push(PostingListBuilder::new(false));
        builder2.posting_lists[0].add(0, PositionRecorder::Count(2));
        builder2.posting_lists[0].add(1, PositionRecorder::Count(1));
        builder2.posting_lists[0].add(2, PositionRecorder::Count(3));
        builder2.posting_lists[0].add(3, PositionRecorder::Count(1));
        builder2.docs.append(200, 2); // row_id=200, num_tokens=2
        builder2.docs.append(201, 1); // row_id=201, num_tokens=1
        builder2.docs.append(202, 3); // row_id=202, num_tokens=3
        builder2.docs.append(203, 1); // row_id=203, num_tokens=1
        builder2.write(store.as_ref()).await.unwrap();

        // Create metadata file with both partitions
        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64, 1u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        // Load the inverted index
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        // Verify the index structure
        assert_eq!(index.partitions.len(), 2);
        assert_eq!(index.partitions[0].tokens.len(), 1);
        assert_eq!(index.partitions[1].tokens.len(), 1);

        // Verify the partitions were loaded correctly

        // Verify posting list lengths (note: partition order may differ from creation order).
        // `posting_len_for_token` works for both legacy and v2 layouts without
        // forcing the V2-only bulk metadata load.
        let pl_0_0 = index.partitions[0]
            .inverted_list
            .posting_len_for_token(0, None)
            .await
            .unwrap();
        let pl_1_0 = index.partitions[1]
            .inverted_list
            .posting_len_for_token(0, None)
            .await
            .unwrap();
        if index.partitions[0].id() == 0 {
            assert_eq!(pl_0_0, 1);
            assert_eq!(pl_1_0, 4);
            assert_eq!(index.partitions[0].docs.len(), 1);
            assert_eq!(index.partitions[1].docs.len(), 4);
        } else {
            assert_eq!(pl_0_0, 4);
            assert_eq!(pl_1_0, 1);
            assert_eq!(index.partitions[0].docs.len(), 4);
            assert_eq!(index.partitions[1].docs.len(), 1);
        }

        // Prewarm the inverted index (this loads posting lists into cache)
        index.prewarm().await.unwrap();

        let tokens = Arc::new(Tokens::new(vec!["test".to_string()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);

        let (row_ids, scores) = index
            .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
            .await
            .unwrap();

        // Verify that we got search results
        // Expected to find 5 documents: 1 from first partition, 4 from second partition
        assert_eq!(row_ids.len(), 5, "row_ids: {:?}", row_ids);
        assert!(!row_ids.is_empty(), "Should find at least some documents");
        assert_eq!(row_ids.len(), scores.len());

        // All scores should be positive since all documents contain the search token
        for &score in &scores {
            assert!(score > 0.0, "All scores should be positive");
        }

        // Check that we got results from both partitions
        assert!(
            row_ids.contains(&100),
            "Should contain row_id from partition 0"
        );
        assert!(
            row_ids.iter().any(|&id| id >= 200),
            "Should contain row_id from partition 1"
        );
    }

    #[tokio::test]
    async fn test_modern_prewarm_packs_group_with_shared_posting_buffer() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("alpha".to_owned());
        builder.tokens.add("beta".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.posting_lists[0].add(1, PositionRecorder::Count(2));
        builder.posting_lists[1].add(2, PositionRecorder::Count(3));
        builder.posting_lists[1].add(3, PositionRecorder::Count(4));
        builder.docs.append(100, 1);
        builder.docs.append(101, 2);
        builder.docs.append(102, 3);
        builder.docs.append(103, 4);
        builder.write(store.as_ref()).await.unwrap();

        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        let inverted_list = &index.partitions[0].inverted_list;
        assert!(
            !inverted_list.is_legacy_layout(),
            "test should use modern posting layout"
        );
        assert!(
            inverted_list.has_impacts,
            "modern posting fixture should include impact skip data"
        );

        inverted_list.prewarm_posting_lists(false, 2).await.unwrap();

        // The two tiny tokens land in a single cache group [0, 2) (issue
        // #7040); both postings are read out of that group entry.
        let (start, end) = inverted_list.group_range_for_token(0).unwrap();
        let group = inverted_list
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                inverted_list.has_impacts,
            ))
            .await
            .unwrap();

        assert!(
            group.is_packed(),
            "no-position prewarm should pack v2 groups"
        );
        assert!(
            group.needs_external_metadata(),
            "prewarmed packed groups must not duplicate reader score/length metadata"
        );
        let (alpha_score, alpha_len) = inverted_list.bulk_metadata_for_token(0);
        let PostingList::Compressed(alpha) = group
            .posting_list(0, alpha_score, alpha_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list for token 0");
        };
        let PostingList::Compressed(alpha_again) = group
            .posting_list(0, alpha_score, alpha_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list for repeated token 0 access");
        };
        let (beta_score, beta_len) = inverted_list.bulk_metadata_for_token(1);
        let PostingList::Compressed(beta) = group
            .posting_list(1, beta_score, beta_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list for token 1");
        };

        assert!(
            alpha.impacts.is_some() && beta.impacts.is_some(),
            "packed prewarm must preserve impact skip data"
        );
        assert!(
            alpha
                .impacts
                .as_ref()
                .unwrap()
                .shares_derived_state_with(alpha_again.impacts.as_ref().unwrap()),
            "repeated packed slot access must share decoded impact state"
        );
        assert!(
            alpha.shares_first_docs_with(&alpha_again),
            "repeated packed slot access must share decoded block heads"
        );
        assert_eq!(
            alpha.block_first_docs().as_ptr(),
            alpha_again.block_first_docs().as_ptr(),
            "packed block heads should be decoded only once per slot"
        );
        assert_eq!(
            alpha.blocks.values().as_ptr(),
            beta.blocks.values().as_ptr(),
            "packed posting views should share the group's values buffer"
        );
    }

    #[tokio::test]
    async fn test_packed_prewarm_groups_do_not_retain_the_full_chunk() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for token_id in 0..4u32 {
            builder.tokens.add(format!("t{token_id}"));
            let mut posting = PostingListBuilder::new(false);
            posting.add(token_id, PositionRecorder::Count(1));
            builder.posting_lists.push(posting);
            builder.docs.append(1000 + token_id as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let cache = LanceCache::with_capacity(1 << 20);
        let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
        posting_reader.grouping = PostingGrouping::SyntheticFixed { group_size: 2 };

        assert_eq!(
            posting_reader
                .prewarm_posting_lists_chunked(false, Some(4), 1)
                .await
                .unwrap(),
            1,
            "the test must read both groups in one prewarm chunk"
        );

        let first_group = posting_reader
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                0,
                2,
                posting_reader.has_impacts,
            ))
            .await
            .unwrap();
        let second_group = posting_reader
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                2,
                4,
                posting_reader.has_impacts,
            ))
            .await
            .unwrap();
        let (first_score, first_len) = posting_reader.bulk_metadata_for_token(0);
        let PostingList::Compressed(first) = first_group
            .posting_list(0, first_score, first_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list in first group");
        };
        let (neighbor_score, neighbor_len) = posting_reader.bulk_metadata_for_token(1);
        let PostingList::Compressed(first_neighbor) = first_group
            .posting_list(1, neighbor_score, neighbor_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list in first group");
        };
        let (second_score, second_len) = posting_reader.bulk_metadata_for_token(2);
        let PostingList::Compressed(second) = second_group
            .posting_list(0, second_score, second_len)
            .unwrap()
            .unwrap()
        else {
            panic!("expected compressed posting list in second group");
        };

        assert_eq!(
            first.blocks.values().as_ptr(),
            first_neighbor.blocks.values().as_ptr(),
            "postings in one group should share the group's values buffer"
        );
        assert_ne!(
            first.blocks.values().as_ptr(),
            second.blocks.values().as_ptr(),
            "each group must own a compact buffer instead of retaining the full chunk"
        );
    }

    #[test]
    fn test_prewarm_chunk_ranges_preserve_group_boundaries() {
        let grouping = PostingGrouping::SyntheticFixed { group_size: 4 };
        assert_eq!(
            prewarm_chunk_ranges(&grouping, 13, 5),
            vec![(0, 4), (4, 8), (8, 13)],
            "grouped chunks may contain multiple groups but must never split one"
        );
        assert_eq!(
            prewarm_chunk_ranges(&PostingGrouping::None, 13, 5),
            vec![(0, 5), (5, 10), (10, 13)],
            "ungrouped chunk ranges should use plain token ranges"
        );
    }

    #[test]
    fn test_synthetic_grouping_preserves_fixed_boundaries() {
        let grouping = PostingGrouping::SyntheticFixed { group_size: 4 };
        assert_eq!(
            grouping.range_for_token(5, 10),
            Some((4, 8)),
            "synthetic token groups should be fixed-size ranges"
        );
        assert_eq!(
            grouping.range_for_token(9, 10),
            Some((8, 10)),
            "the final synthetic group should end at token_count"
        );
        assert_eq!(
            prewarm_chunk_ranges(&grouping, 10, 6),
            vec![(0, 4), (4, 10)],
            "prewarm chunks may contain multiple synthetic groups but must not split one"
        );
        assert_eq!(
            grouping.ranges_for_chunk(4, 10, 10),
            vec![(4, 8), (8, 10)],
            "publish selection should enumerate synthetic groups in a chunk"
        );
    }

    /// Prewarming a large partition in multiple chunks must end up holding exactly the
    /// same per-token posting lists (doc ids and frequencies) as the whole-file path.
    /// Parametrized over layout: the legacy-v1 chunk path rebases global offsets to
    /// chunk-local rows, while the modern one-row-per-token path covers both
    /// legacy-sized v2 and 256-doc v3 posting blocks.
    #[rstest::rstest]
    #[case::v1(InvertedListFormatVersion::V1, LEGACY_BLOCK_SIZE)]
    #[case::v2(InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)]
    #[case::v3(InvertedListFormatVersion::V3, 256)]
    #[tokio::test]
    async fn test_prewarm_streams_in_chunks_preserves_content(
        #[case] format_version: InvertedListFormatVersion,
        #[case] block_size: usize,
    ) {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // One partition with enough tokens to span multiple runtime synthetic
        // groups and several docs per token.
        let num_tokens = runtime_posting_group_tokens() as u32 + 4;
        const DOCS_PER_TOKEN: u32 = 3;
        let posting_tail_codec = format_version.posting_tail_codec();
        let mut builder = InnerBuilder::new_with_format_version_and_block_size(
            0,
            false,
            TokenSetFormat::default(),
            format_version,
            block_size,
        );
        // expected[token] = [(doc_id, frequency)] in stored (doc-id) order.
        let mut expected: Vec<Vec<(u32, u32)>> = Vec::new();
        let mut doc_id = 0u64;
        for t in 0..num_tokens {
            builder.tokens.add(format!("tok_{t:03}"));
            let mut posting = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                false,
                posting_tail_codec,
                block_size,
            );
            let mut docs = Vec::new();
            for _ in 0..DOCS_PER_TOKEN {
                posting.add(doc_id as u32, PositionRecorder::Count(1));
                builder.docs.append(doc_id, 1);
                docs.push((doc_id as u32, 1));
                doc_id += 1;
            }
            expected.push(docs);
            builder.posting_lists.push(posting);
        }
        builder.write(store.as_ref()).await.unwrap();

        let params = InvertedIndexParams::default()
            .block_size(block_size)
            .unwrap();
        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            ("params".to_owned(), serde_json::to_string(&params).unwrap()),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                posting_tail_codec.as_str().to_owned(),
            ),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                format_version.index_version().to_string(),
            ),
            (POSTING_BLOCK_SIZE_KEY.to_owned(), block_size.to_string()),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(1 << 20));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        let inverted_list = &index.partitions[0].inverted_list;
        assert_eq!(inverted_list.len(), num_tokens as usize);
        assert_eq!(inverted_list.block_size(), block_size);

        // Force a small target chunk. Since CHUNK_TOKENS is below the runtime
        // group size, synthetic group alignment should still split only at
        // group boundaries.
        const CHUNK_TOKENS: usize = 6;
        let chunk_count = inverted_list
            .prewarm_posting_lists_chunked(false, Some(CHUNK_TOKENS), 2)
            .await
            .unwrap();

        // (1) The partition was streamed in multiple chunks. The exact count is
        // group-alignment-dependent (chunks snap to whole groups), so just
        // require more than one.
        assert!(
            chunk_count > 1,
            "single partition must be streamed in more than one chunk, got {chunk_count}"
        );

        if block_size == 256 {
            let (start, end) = inverted_list.group_range_for_token(0).unwrap();
            let group = inverted_list
                .index_cache
                .get_with_key(&posting_list_group_cache_key(
                    start,
                    end,
                    inverted_list.has_impacts,
                ))
                .await
                .expect("256-document blocks should populate the packed group cache");
            assert!(group.is_packed());
            let (max_score, length) = inverted_list.bulk_metadata_for_token(0);
            let PostingList::Compressed(posting) =
                group.posting_list(0, max_score, length).unwrap().unwrap()
            else {
                panic!("expected compressed posting list");
            };
            assert_eq!(posting.block_size, 256);
            assert!(
                posting.impacts.is_some(),
                "packed prewarm must preserve impact skip data"
            );
        }

        // (2) Correctness: every token's posting list round-trips with exactly
        // the doc ids and frequencies of the whole-file path.
        for token_id in 0..num_tokens {
            let actual = inverted_list
                .posting_list(token_id, false, &NoOpMetricsCollector)
                .await
                .unwrap()
                .iter()
                .map(|(doc_id, freq, _positions)| (doc_id as u32, freq))
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected[token_id as usize],
                "token {token_id} posting list mismatch after chunked prewarm"
            );
        }
    }

    /// With positions, the chunked prewarm must strip positions into their own
    /// per-token cache entries (leaving the posting cache positions-free) and still
    /// round-trip exact doc ids, frequencies, and positions across chunk boundaries.
    #[tokio::test]
    async fn test_prewarm_streams_in_chunks_with_positions() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let format_version = InvertedListFormatVersion::V2;
        let posting_tail_codec = format_version.posting_tail_codec();
        let num_tokens = runtime_posting_group_tokens() as u32 + 4;
        const DOCS_PER_TOKEN: u32 = 3;
        let mut builder = InnerBuilder::new_with_format_version(
            0,
            true,
            TokenSetFormat::default(),
            format_version,
        );
        // expected[token] = [(doc_id, frequency, positions)].
        let mut expected: Vec<Vec<(u32, u32, Vec<u32>)>> = Vec::new();
        let mut doc_id = 0u64;
        for t in 0..num_tokens {
            builder.tokens.add(format!("tok_{t:03}"));
            let mut posting =
                PostingListBuilder::new_with_posting_tail_codec(true, posting_tail_codec);
            let mut docs = Vec::new();
            for _ in 0..DOCS_PER_TOKEN {
                let positions = vec![t % 3, t % 3 + 2, t % 3 + 5];
                posting.add(
                    doc_id as u32,
                    PositionRecorder::Position(positions.clone().into()),
                );
                builder.docs.append(doc_id, positions.len() as u32);
                docs.push((doc_id as u32, positions.len() as u32, positions));
                doc_id += 1;
            }
            expected.push(docs);
            builder.posting_lists.push(posting);
        }
        builder.write(store.as_ref()).await.unwrap();

        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                posting_tail_codec.as_str().to_owned(),
            ),
            (
                POSITIONS_LAYOUT_KEY.to_owned(),
                POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
            ),
            (
                POSITIONS_CODEC_KEY.to_owned(),
                PositionStreamCodec::PackedDelta.as_str().to_owned(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(1 << 20));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        let inverted_list = &index.partitions[0].inverted_list;

        const CHUNK_TOKENS: usize = 5;
        let chunk_count = inverted_list
            .prewarm_posting_lists_chunked(true, Some(CHUNK_TOKENS), 2)
            .await
            .unwrap();
        assert!(
            chunk_count > 1,
            "partition must be streamed in more than one chunk, got {chunk_count}"
        );

        for token_id in 0..num_tokens {
            // The prewarmed posting cache entry is positions-free.
            let (start, end) = inverted_list.group_range_for_token(token_id).unwrap();
            let group = inverted_list
                .index_cache
                .get_with_key(&posting_list_group_cache_key(
                    start,
                    end,
                    inverted_list.has_impacts,
                ))
                .await
                .unwrap();
            let slot = (token_id - start) as usize;
            assert!(
                !group.is_packed(),
                "with-position prewarm should retain the materialized fallback"
            );
            assert!(
                !group
                    .posting_list(slot, None, None)
                    .unwrap()
                    .unwrap()
                    .has_position(),
                "token {token_id} posting cache entry must be positions-free after prewarm"
            );

            // Full content (doc ids, frequencies, positions) round-trips; the
            // positions come from the dedicated per-token cache prewarm populated.
            let actual = inverted_list
                .posting_list(token_id, true, &NoOpMetricsCollector)
                .await
                .unwrap()
                .iter()
                .map(|(doc_id, freq, positions)| {
                    (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected[token_id as usize],
                "token {token_id} posting list / positions mismatch after chunked prewarm"
            );
        }
    }

    /// IO accounting for the IO-counting stats test below: tracks bytes
    /// pulled from the posting file so we can assert that the stats path is
    /// O(1) in num_unique_tokens.
    #[derive(Debug, Default)]
    struct PostingMetadataCounter {
        rows_read: std::sync::atomic::AtomicUsize,
        metadata_rows_read: std::sync::atomic::AtomicUsize,
        read_range_calls: std::sync::atomic::AtomicUsize,
    }

    impl PostingMetadataCounter {
        fn rows_read(&self) -> usize {
            self.rows_read.load(std::sync::atomic::Ordering::Relaxed)
        }
        fn metadata_rows_read(&self) -> usize {
            self.metadata_rows_read
                .load(std::sync::atomic::Ordering::Relaxed)
        }
        fn read_range_calls(&self) -> usize {
            self.read_range_calls
                .load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    struct CountingPostingReader {
        inner: Arc<dyn IndexReader>,
        counter: Arc<PostingMetadataCounter>,
    }

    #[async_trait]
    impl IndexReader for CountingPostingReader {
        async fn read_record_batch(&self, n: u64, batch_size: u64) -> Result<RecordBatch> {
            self.inner.read_record_batch(n, batch_size).await
        }
        async fn read_global_buffer(&self, index: u32) -> Result<bytes::Bytes> {
            self.inner.read_global_buffer(index).await
        }
        async fn read_range(
            &self,
            range: std::ops::Range<usize>,
            projection: Option<&[&str]>,
        ) -> Result<RecordBatch> {
            let n = range.end - range.start;
            self.counter
                .read_range_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.counter
                .rows_read
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            let touches_metadata = projection
                .map(|cols| cols.contains(&MAX_SCORE_COL) || cols.contains(&LENGTH_COL))
                .unwrap_or(false);
            if touches_metadata {
                self.counter
                    .metadata_rows_read
                    .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
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
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn IndexStore>,
        posting_file: String,
        counter: Arc<PostingMetadataCounter>,
    }

    impl DeepSizeOf for CountingStore {
        fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
            self.inner.deep_size_of_children(context)
        }
    }

    #[async_trait]
    impl IndexStore for CountingStore {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.clone(),
                posting_file: self.posting_file.clone(),
                counter: self.counter.clone(),
            })
        }
        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }
        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.with_io_priority(io_priority),
                posting_file: self.posting_file.clone(),
                counter: self.counter.clone(),
            })
        }
        async fn new_index_file(
            &self,
            name: &str,
            schema: Arc<arrow_schema::Schema>,
        ) -> Result<Box<dyn crate::scalar::IndexWriter>> {
            self.inner.new_index_file(name, schema).await
        }
        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
            let reader = self.inner.open_index_file(name).await?;
            if name == self.posting_file {
                Ok(Arc::new(CountingPostingReader {
                    inner: reader,
                    counter: self.counter.clone(),
                }))
            } else {
                Ok(reader)
            }
        }
        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<crate::scalar::IndexFile> {
            self.inner.copy_index_file(name, dest_store).await
        }
        async fn copy_index_file_to(
            &self,
            name: &str,
            new_name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<crate::scalar::IndexFile> {
            self.inner
                .copy_index_file_to(name, new_name, dest_store)
                .await
        }
        async fn rename_index_file(
            &self,
            name: &str,
            new_name: &str,
        ) -> Result<crate::scalar::IndexFile> {
            self.inner.rename_index_file(name, new_name).await
        }
        async fn delete_index_file(&self, name: &str) -> Result<()> {
            self.inner.delete_index_file(name).await
        }
        async fn list_files_with_sizes(&self) -> Result<Vec<crate::scalar::IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    // Returns the `TempObjDir` guard so callers keep the backing store alive
    // for the index's lifetime: the deferred DocSet re-opens the docs file on
    // demand (it does not pin an open handle), so the files must still exist
    // when the test exercises a scoring path.
    async fn load_counted_v2_index(
        num_tokens: usize,
        cache: LanceCache,
    ) -> (Arc<InvertedIndex>, Arc<PostingMetadataCounter>, TempObjDir) {
        let tmpdir = TempObjDir::default();
        let inner_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for i in 0..num_tokens {
            builder.tokens.add(format!("t{}", i));
            let mut pl = PostingListBuilder::new(false);
            pl.add(i as u32, PositionRecorder::Count(1));
            builder.posting_lists.push(pl);
            builder.docs.append(i as u64, 1);
        }
        builder.write(inner_store.as_ref()).await.unwrap();

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = inner_store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let counter = Arc::new(PostingMetadataCounter::default());
        let counting_store: Arc<dyn IndexStore> = Arc::new(CountingStore {
            inner: inner_store,
            posting_file: posting_file_path(0),
            counter: counter.clone(),
        });
        let index = InvertedIndex::load(counting_store, None, &cache)
            .await
            .unwrap();
        (index, counter, tmpdir)
    }

    /// IO regression test for the lazy posting-metadata refactor. Builds a
    /// v2 InvertedIndex with `num_tokens` tokens in a single partition,
    /// wraps the IndexStore so reads against the posting file are counted,
    /// then asserts:
    ///
    /// * `InvertedIndex::load` does not touch the posting file at all
    ///   (`InvertedPartition::load` only needs the token file and docs file).
    /// * `bm25_stats_for_terms(["t0"])` reads exactly one metadata row from
    ///   the posting file for token 0 regardless of how many unique tokens the
    ///   partition has.
    ///
    /// Before this refactor, `PostingListReader::try_new` did
    /// `read_range(0..num_rows, [MAX_SCORE_COL, LENGTH_COL])`, so the
    /// `metadata_rows_read` figure scaled linearly with `num_tokens` even
    /// when nobody asked for those stats. The cases below exercise that
    /// scaling explicitly.
    #[rstest::rstest]
    #[case::tokens_10(10)]
    #[case::tokens_100(100)]
    #[case::tokens_1000(1000)]
    #[tokio::test]
    async fn test_bm25_stats_for_terms_is_lazy(#[case] num_tokens: usize) {
        let (index, counter, _tmpdir) =
            load_counted_v2_index(num_tokens, LanceCache::no_cache()).await;
        assert!(
            !index.partitions[0].inverted_list.is_legacy_layout(),
            "this test only proves the lazy path for v2 indexes",
        );

        // Opening the partition must not pull anything from the posting file.
        // Pre-fix, `PostingListReader::try_new` issued one read_range here for
        // [MAX_SCORE_COL, LENGTH_COL] covering every unique token.
        assert_eq!(
            counter.read_range_calls(),
            0,
            "InvertedIndex::load must not read the posting file (was {} calls)",
            counter.read_range_calls(),
        );
        assert_eq!(counter.rows_read(), 0);

        let (total_tokens, num_docs, dfs) = index
            .bm25_stats_for_terms(&["t0".to_string()], None)
            .await
            .unwrap();
        assert_eq!(total_tokens, num_tokens as u64);
        assert_eq!(num_docs, num_tokens);
        assert_eq!(dfs, vec![1]);

        // Stats must pull a constant number of metadata rows from the posting
        // file regardless of how many tokens the partition has. One term, one
        // partition, one row.
        assert_eq!(
            counter.metadata_rows_read(),
            1,
            "stats path should read exactly 1 metadata row per (term, partition); \
             got {} (read_range_calls={}, rows_read={}, num_tokens={})",
            counter.metadata_rows_read(),
            counter.read_range_calls(),
            counter.rows_read(),
            num_tokens,
        );
    }

    #[tokio::test]
    async fn test_bm25_stats_for_terms_reuses_posting_metadata_cache() {
        let cache = LanceCache::with_capacity(1024 * 1024);
        let (index, counter, _tmpdir) = load_counted_v2_index(100, cache.clone()).await;

        let terms = ["t0".to_string()];
        let first = index.bm25_stats_for_terms(&terms, None).await.unwrap();
        assert_eq!(first, (100, 100, vec![1]));
        assert_eq!(counter.metadata_rows_read(), 1);

        let second = index.bm25_stats_for_terms(&terms, None).await.unwrap();
        assert_eq!(second, first);
        assert_eq!(
            counter.metadata_rows_read(),
            1,
            "repeated stats for the same token should reuse cached posting metadata",
        );
    }

    #[tokio::test]
    async fn test_bm25_stats_for_terms_records_metadata_cache_stats() {
        let cache = LanceCache::with_capacity(1024 * 1024);
        let (index, _counter, _tmpdir) = load_counted_v2_index(100, cache.clone()).await;
        assert!(
            !index.partitions[0].inverted_list.is_legacy_layout(),
            "this test only proves the v2 metadata boundary",
        );

        let terms = ["t0".to_string(), "t1".to_string(), "t2".to_string()];
        let cold = LocalMetricsCollector::default();
        let cold_stats = index
            .bm25_stats_for_terms(&terms, Some(&cold))
            .await
            .unwrap();
        assert_eq!(cold_stats.2, vec![1, 1, 1]);
        assert_eq!(cold.index_cache_misses(), terms.len());
        assert_eq!(cold.index_cache_hits(), 0);

        let warm = LocalMetricsCollector::default();
        let warm_stats = index
            .bm25_stats_for_terms(&terms, Some(&warm))
            .await
            .unwrap();
        assert_eq!(warm_stats, cold_stats);
        assert_eq!(warm.index_cache_misses(), 0);
        assert_eq!(warm.index_cache_hits(), terms.len());
    }

    #[tokio::test]
    async fn test_aggregate_corpus_stats_reuses_cached_value() {
        let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
        assert!(index.corpus_stats.get().is_none());

        let first = index.aggregate_corpus_stats().await.unwrap();
        assert_eq!(first, (100, 100));
        assert_eq!(index.corpus_stats.get().copied(), Some(first));

        let second = index.aggregate_corpus_stats().await.unwrap();
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn test_persisted_stats_do_not_load_document_columns() {
        let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
        assert!(!index.is_legacy());
        let partition = index.partitions[0].clone();
        let documents = partition.docs.modern().unwrap();

        assert_eq!(index.aggregate_corpus_stats().await.unwrap(), (100, 100));
        assert_eq!(documents.cached_stats().unwrap().total_tokens, 100);
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());

        let views = futures::future::join_all((0..8).map(|_| documents.lengths()))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let first = &views[0];
        assert!(views.iter().all(|view| Arc::ptr_eq(first, view)));
        assert_eq!(first.total_tokens(), 100);

        let all_rows = RowAddrMask::all_rows();
        assert!(matches!(
            documents
                .visibility(Arc::new(all_rows), false)
                .await
                .unwrap(),
            DocVisibility::All
        ));
        assert!(!documents.projection_loaded());

        let filtered = RowAddrMask::allow_nothing();
        let visibility = documents
            .visibility(Arc::new(filtered), true)
            .await
            .unwrap();
        assert!(visibility.is_empty());
        assert!(!documents.projection_loaded());
        assert_eq!(
            documents
                .resolve_addresses(&[DocId::new(0), DocId::new(99)])
                .await
                .unwrap(),
            [0, 99]
        );
    }

    #[tokio::test]
    async fn test_no_hit_partition_does_not_load_document_columns() {
        let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
        let documents = index.partitions[0].docs.modern().unwrap();
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());

        let tokens = Arc::new(Tokens::new(vec!["missing-token".to_owned()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let (row_ids, scores) = index
            .bm25_search(
                tokens,
                params,
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();

        assert!(row_ids.is_empty());
        assert!(scores.is_empty());
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());
    }

    #[tokio::test]
    async fn test_concurrent_stats_and_lengths_initialization() {
        let (index, _counter, _tmpdir) = load_counted_v2_index(100, LanceCache::no_cache()).await;
        let docs = index.partitions[0].docs.modern().unwrap().clone();

        let stats = futures::future::join_all((0..8).map(|_| docs.stats()));
        let views = futures::future::join_all((0..8).map(|_| docs.lengths()));
        let (stats, views) = tokio::join!(stats, views);

        let stats = stats.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert!(stats.iter().all(|stats| stats.total_tokens == 100));
        let views = views.into_iter().collect::<Result<Vec<_>>>().unwrap();
        let first = &views[0];
        assert!(views.iter().all(|view| Arc::ptr_eq(first, view)));
        assert_eq!(docs.cached_stats().unwrap().total_tokens, 100);
    }

    #[tokio::test]
    async fn test_grouped_posting_lists_read_one_group_per_neighborhood() {
        // Cold-start scoring must not bulk-read the full `0..num_tokens`
        // metadata table. With small-posting grouping (issue #7040), scoring
        // K adjacent cold tokens shares a single group cache entry: one
        // read_range bounded by the group size, independent of the partition's
        // total token count.
        let runtime_group_size = runtime_posting_group_tokens().max(1);
        let queried_token_count = runtime_group_size.min(4);
        let queried_tokens = (0..queried_token_count as u32).collect::<Vec<_>>();
        let num_tokens = runtime_group_size
            .saturating_mul(2)
            .max(queried_token_count + 1)
            .min(1024);
        let (index, counter, _tmpdir) =
            load_counted_v2_index(num_tokens, LanceCache::no_cache()).await;
        let inverted_list = index.partitions[0].inverted_list.clone();
        assert!(
            !inverted_list.is_legacy_layout(),
            "this test only proves the lazy path for v2 indexes",
        );
        assert!(
            matches!(
                &inverted_list.grouping,
                PostingGrouping::SyntheticFixed { .. }
            ),
            "freshly written v2 index should use runtime synthetic groups",
        );

        // This fixture uses a no-op cache, so each call re-reads; that isolates
        // the per-query read shape. Each posting_list call reads exactly its
        // own group — bounded by the group size, never the full token table.
        let metrics = Arc::new(NoOpMetricsCollector);
        for &token_id in &queried_tokens {
            inverted_list
                .posting_list(token_id, false, metrics.as_ref())
                .await
                .unwrap();
        }

        let (start, end) = inverted_list.group_range_for_token(0).unwrap();
        let group_len = (end - start) as usize;
        assert!(
            (queried_tokens.len()..=num_tokens).contains(&group_len),
            "group [{start}, {end}) should cover the queried neighborhood and \
             stay bounded by the {num_tokens}-token table",
        );
        assert_eq!(
            counter.read_range_calls(),
            queried_tokens.len(),
            "each cold token should read exactly its own group, no bulk read",
        );
        assert_eq!(
            counter.metadata_rows_read(),
            queried_tokens.len() * group_len,
            "each query reads one group's metadata rows ({group_len}), not the \
             full {num_tokens}-row table",
        );
    }

    /// Build a single-partition v2 index where every token's posting list spans
    /// `docs_per_token` docs. Runtime grouping packs consecutive token rows
    /// into shared cache groups.
    async fn load_v2_index_with_grouped_postings(
        num_tokens: usize,
        docs_per_token: usize,
    ) -> (Arc<InvertedIndex>, Arc<LanceCache>) {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let num_docs = num_tokens * docs_per_token;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for token_id in 0..num_tokens {
            builder.tokens.add(format!("t{token_id}"));
            let mut pl = PostingListBuilder::new(false);
            for d in 0..docs_per_token {
                let doc_id = (token_id * docs_per_token + d) as u32;
                pl.add(doc_id, PositionRecorder::Count(1));
            }
            builder.posting_lists.push(pl);
        }
        for doc in 0..num_docs {
            builder.docs.append(doc as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        // The inverted list keeps only a `WeakLanceCache`, so the caller must
        // hold this `Arc<LanceCache>` alive for the cache to stay usable.
        let cache = Arc::new(LanceCache::with_capacity(1 << 30));
        let index = InvertedIndex::load(store, None, cache.as_ref())
            .await
            .unwrap();
        (index, cache)
    }

    /// Packed groups charge their Arrow buffers and contiguous metadata once,
    /// avoiding the per-member enum/array object graph of a materialized group.
    #[tokio::test]
    async fn test_packed_group_deep_size_is_smaller_than_materialized_graph() {
        let (index, _cache) = load_v2_index_with_grouped_postings(512, 1).await;
        let inverted_list = index.partitions[0].inverted_list.clone();
        assert!(!inverted_list.is_legacy_layout(), "expected v2 layout");
        assert!(
            matches!(
                &inverted_list.grouping,
                PostingGrouping::SyntheticFixed { .. }
            ),
            "expected grouped posting lists"
        );

        // Populate the group cache via the same path a query uses.
        inverted_list
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        let (start, end) = inverted_list.group_range_for_token(0).unwrap();
        let group = inverted_list
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                inverted_list.has_impacts,
            ))
            .await
            .unwrap();
        assert!(group.is_packed(), "cold v2 group should use packed storage");
        inverted_list.ensure_metadata_loaded().await.unwrap();

        let mut distinct_buffers = std::collections::HashSet::new();
        let mut materialized = Vec::with_capacity(group.len());
        for slot in 0..group.len() {
            let (max_score, length) = inverted_list.bulk_metadata_for_token(start + slot as u32);
            let posting = group
                .posting_list(slot, max_score, length)
                .unwrap()
                .unwrap();
            let PostingList::Compressed(compressed) = posting else {
                panic!("expected compressed posting lists");
            };
            distinct_buffers.insert(compressed.blocks.values().as_ptr());
            materialized.push(PostingList::Compressed(compressed));
        }
        let posting_count = materialized.len();

        assert!(
            posting_count > 1,
            "default grouping should pack multiple tiny postings into one group"
        );
        assert_eq!(
            distinct_buffers.len(),
            1,
            "read-path postings in a group should share one backing buffer"
        );
        let packed_size = group.deep_size_of();
        let materialized_size = PostingListGroup::new(materialized).deep_size_of();
        assert!(
            packed_size * 4 < materialized_size * 3,
            "packed group deep_size_of {packed_size}B should be at least 25% smaller than the \
             {materialized_size}B materialized graph for {posting_count} postings"
        );
    }

    // ===========================================================================
    // Regression tests for index-cache size accounting of cached posting lists.
    //
    // A cached posting list is a *slice* of a buffer read for a whole posting-list
    // group, so its `DeepSizeOf` impl must charge only the bytes the slice
    // references, not the full shared backing buffer. These lock that in: each
    // builds an array that references a small slice of a much larger buffer and
    // asserts `deep_size_of()` tracks the slice, not the buffer.
    // ===========================================================================

    /// Build a `List<Int32>` of `num_sublists` x `ints_per_sublist`, then return
    /// the slice `[off, off + len)`. The returned array shares the full backing
    /// buffers, so `values().get_buffer_memory_size()` still reports the whole
    /// thing — the slicing-unaware over-count the fix targets.
    fn sliced_int32_list(
        num_sublists: usize,
        ints_per_sublist: usize,
        off: usize,
        len: usize,
    ) -> ListArray {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for s in 0..num_sublists {
            for i in 0..ints_per_sublist {
                builder
                    .values()
                    .append_value((s * ints_per_sublist + i) as i32);
            }
            builder.append(true);
        }
        builder.finish().slice(off, len)
    }

    #[test]
    fn test_compressed_posting_deep_size_counts_only_referenced_blocks_slice() {
        const ELEM_BYTES: usize = 256;
        const TOTAL_ELEMS: usize = 64;
        const SLICE_OFF: usize = 10;
        const SLICE_LEN: usize = 2;

        let mut builder = LargeBinaryBuilder::new();
        for _ in 0..TOTAL_ELEMS {
            builder.append_value(vec![7u8; ELEM_BYTES]);
        }
        let full = builder.finish();
        let blocks = full.slice(SLICE_OFF, SLICE_LEN);

        let posting = CompressedPostingList::new(
            blocks,
            1.0,
            SLICE_LEN as u32,
            PostingTailCodec::Fixed32,
            LEGACY_BLOCK_SIZE,
            None,
            None,
        );

        let full_backing = full.get_buffer_memory_size();
        let slice_bytes = SLICE_LEN * ELEM_BYTES;
        let reported = posting.deep_size_of();

        assert!(
            reported < full_backing / 4,
            "deep_size_of {reported}B must not count the {full_backing}B shared buffer"
        );
        assert!(
            reported <= slice_bytes * 2,
            "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
        );
    }

    #[test]
    fn test_plain_posting_deep_size_counts_only_referenced_positions_slice() {
        const SUBLISTS: usize = 64;
        const INTS: usize = 64;
        const SLICE_LEN: usize = 2;

        let positions = sliced_int32_list(SUBLISTS, INTS, 10, SLICE_LEN);
        let row_ids = ScalarBuffer::from(vec![0u64, 1]);
        let frequencies = ScalarBuffer::from(vec![1.0f32, 1.0]);
        let posting =
            PlainPostingList::new(row_ids, frequencies, Some(1.0), Some(positions.clone()));

        let full_backing = positions.values().get_buffer_memory_size();
        let slice_bytes = SLICE_LEN * INTS * std::mem::size_of::<i32>();
        let reported = posting.deep_size_of();

        assert!(
            reported < full_backing / 4,
            "deep_size_of {reported}B must not count the {full_backing}B shared positions buffer"
        );
        assert!(
            reported <= slice_bytes * 2 + 64,
            "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
        );
    }

    #[test]
    fn test_legacy_per_doc_positions_deep_size_counts_only_referenced_slice() {
        const SUBLISTS: usize = 64;
        const INTS: usize = 64;
        const SLICE_LEN: usize = 2;

        let positions = sliced_int32_list(SUBLISTS, INTS, 10, SLICE_LEN);
        let full_backing = positions.values().get_buffer_memory_size();
        let slice_bytes = SLICE_LEN * INTS * std::mem::size_of::<i32>();

        let storage = CompressedPositionStorage::LegacyPerDoc(positions);
        let reported = storage.deep_size_of();
        assert!(
            reported < full_backing / 4,
            "CompressedPositionStorage deep_size_of {reported}B must not count the \
             {full_backing}B shared buffer"
        );
        assert!(
            reported <= slice_bytes * 2 + 64,
            "deep_size_of {reported}B should track the ~{slice_bytes}B referenced slice"
        );

        // The `Positions` cache wrapper must report the same slice-aware size.
        let wrapped = Positions(storage).deep_size_of();
        assert!(
            wrapped < full_backing / 4,
            "Positions deep_size_of {wrapped}B must not count the {full_backing}B shared buffer"
        );
    }

    #[tokio::test]
    async fn test_prewarm_with_positions_populates_separate_position_cache() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new_with_format_version(
            0,
            true,
            TokenSetFormat::default(),
            InvertedListFormatVersion::V1,
        );
        builder.tokens.add("hello".to_owned());
        builder.tokens.add("world".to_owned());
        builder
            .posting_lists
            .push(PostingListBuilder::new_with_posting_tail_codec(
                true,
                PostingTailCodec::Fixed32,
            ));
        builder
            .posting_lists
            .push(PostingListBuilder::new_with_posting_tail_codec(
                true,
                PostingTailCodec::Fixed32,
            ));
        builder.posting_lists[0].add(0, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[1].add(0, PositionRecorder::Position(vec![1].into()));
        builder.posting_lists[0].add(1, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[1].add(1, PositionRecorder::Position(vec![2].into()));
        builder.docs.append(100, 2);
        builder.docs.append(101, 2);
        builder.write(store.as_ref()).await.unwrap();

        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0_u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_backend(Arc::new(
            QuickCacheBackend::with_capacity(4096),
        )));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        index
            .prewarm_with_options(&FtsPrewarmOptions::new().with_position(true))
            .await
            .unwrap();

        let inverted_list = &index.partitions[0].inverted_list;
        // The posting cache entry is grouped (issue #7040); the group holds
        // positions-free lists while positions live in their own per-token
        // entries.
        let (start, end) = inverted_list.group_range_for_token(0).unwrap();
        let group = inverted_list
            .index_cache
            .get_with_key(&posting_list_group_cache_key(
                start,
                end,
                inverted_list.has_impacts,
            ))
            .await
            .unwrap();
        assert!(
            !group.is_packed(),
            "with-position prewarm should retain the materialized fallback"
        );
        assert!(
            !group
                .posting_list(0, None, None)
                .unwrap()
                .unwrap()
                .has_position(),
            "posting cache should remain positions-free after prewarm"
        );

        let positions = inverted_list
            .index_cache
            .get_with_key(&PositionKey { token_id: 0 })
            .await
            .unwrap();
        assert!(
            matches!(
                positions.as_ref().0,
                CompressedPositionStorage::LegacyPerDoc(_)
            ),
            "positions should be stored in the dedicated position cache"
        );

        drop(positions);
        drop(group);
        cache.clear().await;
        assert!(
            inverted_list
                .index_cache
                .get_with_key(&PositionKey { token_id: 0 })
                .await
                .is_none()
        );

        index
            .prewarm_with_options(&FtsPrewarmOptions::default())
            .await
            .unwrap();
        assert!(index.prewarm_state.lock().await.satisfies(true));
        assert!(
            inverted_list
                .index_cache
                .get_with_key(&PositionKey { token_id: 0 })
                .await
                .is_some(),
            "re-prewarm after eviction must preserve the strongest requested mode"
        );
    }

    #[tokio::test]
    async fn test_prewarm_with_v2_positions_preserves_shared_stream_codec() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let format_version = InvertedListFormatVersion::V2;
        let posting_tail_codec = format_version.posting_tail_codec();
        let mut builder = InnerBuilder::new_with_format_version(
            0,
            true,
            TokenSetFormat::default(),
            format_version,
        );
        builder.tokens.add("body".to_owned());

        let mut posting_list =
            PostingListBuilder::new_with_posting_tail_codec(true, posting_tail_codec);
        let expected = (0..(BLOCK_SIZE + 5) as u32)
            .map(|doc_id| {
                let positions = vec![doc_id % 3, doc_id % 3 + 2, doc_id % 3 + 5];
                posting_list.add(doc_id, PositionRecorder::Position(positions.clone().into()));
                builder.docs.append(30_000 + doc_id as u64, 20 + doc_id % 7);
                (doc_id, positions.len() as u32, positions)
            })
            .collect::<Vec<_>>();
        builder.posting_lists.push(posting_list);
        builder.write(store.as_ref()).await.unwrap();

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0_u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                posting_tail_codec.as_str().to_owned(),
            ),
            (
                POSITIONS_LAYOUT_KEY.to_owned(),
                POSITIONS_LAYOUT_SHARED_STREAM_V2.to_owned(),
            ),
            (
                POSITIONS_CODEC_KEY.to_owned(),
                PositionStreamCodec::PackedDelta.as_str().to_owned(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store, None, cache.as_ref())
            .await
            .unwrap();
        index
            .prewarm_with_options(&FtsPrewarmOptions::new().with_position(true))
            .await
            .unwrap();

        let actual = index.partitions[0]
            .inverted_list
            .posting_list(0, true, &NoOpMetricsCollector)
            .await
            .unwrap()
            .iter()
            .map(|(doc_id, freq, positions)| {
                (doc_id as u32, freq, positions.unwrap().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_block_max_scores_capacity_matches_block_count() {
        let mut docs = DocSet::default();
        let num_docs = BLOCK_SIZE * 3 + 7;
        let doc_ids = (0..num_docs as u32).collect::<Vec<_>>();
        for doc_id in &doc_ids {
            docs.append(*doc_id as u64, 1);
        }

        let freqs = vec![1_u32; doc_ids.len()];
        let block_max_scores = docs.calculate_block_max_scores(doc_ids.iter(), freqs.iter());
        let expected_blocks = doc_ids.len().div_ceil(BLOCK_SIZE);

        assert_eq!(block_max_scores.len(), expected_blocks);
        assert_eq!(block_max_scores.capacity(), expected_blocks);
    }

    #[tokio::test]
    async fn test_bm25_search_uses_global_idf() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Partition 0: 3 docs, only one contains "alpha".
        let mut builder0 = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder0.tokens.add("alpha".to_owned());
        builder0.tokens.add("beta".to_owned());
        builder0.posting_lists.push(PostingListBuilder::new(false));
        builder0.posting_lists.push(PostingListBuilder::new(false));
        builder0.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder0.posting_lists[1].add(1, PositionRecorder::Count(1));
        builder0.posting_lists[1].add(2, PositionRecorder::Count(1));
        builder0.docs.append(100, 1);
        builder0.docs.append(101, 1);
        builder0.docs.append(102, 1);
        builder0.write(store.as_ref()).await.unwrap();

        // Partition 1: 1 doc, contains "alpha".
        let mut builder1 = InnerBuilder::new(1, false, TokenSetFormat::default());
        builder1.tokens.add("alpha".to_owned());
        builder1.posting_lists.push(PostingListBuilder::new(false));
        builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder1.docs.append(200, 1);
        builder1.write(store.as_ref()).await.unwrap();

        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64, 1u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);

        let (row_ids, scores) = index
            .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
            .await
            .unwrap();

        assert_eq!(row_ids.len(), 2);
        assert!(row_ids.contains(&100));
        assert!(row_ids.contains(&200));
        assert_eq!(row_ids.len(), scores.len());

        let expected_idf = idf(2, 4);
        for score in scores {
            assert!(
                (score - expected_idf).abs() < 1e-6,
                "score: {}, expected: {}",
                score,
                expected_idf
            );
        }
    }

    async fn write_test_metadata(
        store: &Arc<LanceIndexStore>,
        partition_ids: Vec<u64>,
        params: InvertedIndexParams,
    ) {
        let format_version = params.resolved_format_version();
        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&partition_ids).unwrap(),
            ),
            ("params".to_owned(), serde_json::to_string(&params).unwrap()),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                format_version.posting_tail_codec().as_str().to_owned(),
            ),
            (
                FTS_FORMAT_VERSION_KEY.to_owned(),
                format_version.index_version().to_string(),
            ),
            (
                POSTING_BLOCK_SIZE_KEY.to_owned(),
                params.posting_block_size().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();
    }

    async fn write_test_partition_with_optional_impacts(
        store: &Arc<LanceIndexStore>,
        partition_id: u64,
        mut builder: InnerBuilder,
        token_set_format: TokenSetFormat,
        with_impacts: bool,
    ) {
        let format_version = InvertedListFormatVersion::V1;
        let block_size = LEGACY_BLOCK_SIZE;
        let docs = std::mem::take(&mut builder.docs);
        let schema = inverted_list_schema_for_version_with_block_size_and_impacts(
            false,
            format_version,
            block_size,
            with_impacts,
        );

        let mut posting_writer = store
            .new_index_file(&posting_file_path(partition_id), schema.clone())
            .await
            .unwrap();
        for posting_list in std::mem::take(&mut builder.posting_lists) {
            let batch = posting_list
                .to_batch_with_docs(&docs, schema.clone())
                .unwrap();
            posting_writer.write_record_batch(batch).await.unwrap();
        }
        posting_writer.finish().await.unwrap();

        let token_batch = std::mem::take(&mut builder.tokens)
            .to_batch(token_set_format)
            .unwrap();
        let mut token_writer = store
            .new_index_file(&token_file_path(partition_id), token_batch.schema())
            .await
            .unwrap();
        token_writer.write_record_batch(token_batch).await.unwrap();
        token_writer.finish().await.unwrap();

        let doc_batch = docs.to_batch().unwrap();
        let mut doc_writer = store
            .new_index_file(&doc_file_path(partition_id), doc_batch.schema())
            .await
            .unwrap();
        doc_writer.write_record_batch(doc_batch).await.unwrap();
        doc_writer.finish().await.unwrap();
    }

    async fn load_global_scoring_test_index(
        first_partition_has_impacts: bool,
        second_partition_has_impacts: bool,
    ) -> (TempObjDir, Arc<LanceCache>, Arc<InvertedIndex>) {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let partition_specs = [
            (0, 100, 5_000, 101..111, 5_000, first_partition_has_impacts),
            (1, 200, 1_000, 201..301, 1, second_partition_has_impacts),
        ];
        for (
            partition_id,
            matching_row_id,
            matching_doc_length,
            other_row_ids,
            other_doc_length,
            with_impacts,
        ) in partition_specs
        {
            let mut builder = InnerBuilder::new_with_format_version(
                partition_id,
                false,
                TokenSetFormat::default(),
                InvertedListFormatVersion::V1,
            );
            builder.tokens.add("alpha".to_owned());
            builder
                .posting_lists
                .push(PostingListBuilder::new_with_posting_tail_codec(
                    false,
                    InvertedListFormatVersion::V1.posting_tail_codec(),
                ));
            builder.posting_lists[0].add(0, PositionRecorder::Count(1));
            builder.docs.append(matching_row_id, matching_doc_length);
            for row_id in other_row_ids {
                builder.docs.append(row_id, other_doc_length);
            }
            write_test_partition_with_optional_impacts(
                &store,
                partition_id,
                builder,
                TokenSetFormat::default(),
                with_impacts,
            )
            .await;
        }

        write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_backend(Arc::new(
            QuickCacheBackend::with_capacity(4096),
        )));
        let index = InvertedIndex::load(store, None, cache.as_ref())
            .await
            .unwrap();
        (tmpdir, cache, index)
    }

    #[tokio::test]
    async fn test_chunked_modern_search_preserves_cold_and_prewarmed_results() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let matching_partitions = 17_u64;
        for partition_id in 0..matching_partitions {
            let mut builder = InnerBuilder::new(partition_id, false, TokenSetFormat::default());
            builder.tokens.add("pipeline".to_owned());
            builder.posting_lists.push(PostingListBuilder::new(false));
            builder.posting_lists[0].add(0, PositionRecorder::Count(1));
            builder.docs.append(partition_id * 1_000 + 7, 1);
            builder.write(store.as_ref()).await.unwrap();
        }
        let unmatched_partition = matching_partitions;
        let mut builder = InnerBuilder::new(unmatched_partition, false, TokenSetFormat::default());
        builder.tokens.add("unrelated".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.docs.append(999_999, 1);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(
            &store,
            (0..=unmatched_partition).collect(),
            InvertedIndexParams::default(),
        )
        .await;
        let cache = Arc::new(LanceCache::with_capacity(64 * 1024 * 1024));
        let index = InvertedIndex::load(store, None, cache.as_ref())
            .await
            .unwrap();
        let tokens = Arc::new(Tokens::new(vec!["pipeline".to_owned()], DocType::Text));
        let params =
            Arc::new(FtsSearchParams::new().with_limit(Some(matching_partitions as usize)));

        let search = || {
            index.bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
        };
        let (mut cold_row_ids, cold_scores) = search().await.unwrap();
        cold_row_ids.sort_unstable();
        let expected = (0..matching_partitions)
            .map(|partition_id| partition_id * 1_000 + 7)
            .collect::<Vec<_>>();
        assert_eq!(cold_row_ids, expected);
        assert_eq!(cold_scores.len(), expected.len());

        index
            .prewarm_with_options(&FtsPrewarmOptions::default())
            .await
            .unwrap();
        let (mut prewarmed_row_ids, prewarmed_scores) = search().await.unwrap();
        prewarmed_row_ids.sort_unstable();
        assert_eq!(prewarmed_row_ids, expected);
        assert_eq!(prewarmed_scores, cold_scores);
    }

    #[tokio::test]
    async fn test_prewarmed_modern_search_uses_resident_address_projection() {
        let (_tmpdir, cache, index) = load_global_scoring_test_index(true, true).await;
        let tokens = Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(2)));

        assert!(!index.has_resident_document_projections());
        let deferred = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert!(!index.has_resident_document_projections());

        index.partitions[0]
            .docs
            .modern()
            .unwrap()
            .prewarm()
            .await
            .unwrap();
        assert!(index.partitions[0].docs.query_ready());
        assert!(!index.has_resident_document_projections());
        let partially_resident = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(partially_resident, deferred);

        let prewarm_options = FtsPrewarmOptions::default();
        futures::future::join_all((0..8).map(|_| index.prewarm_with_options(&prewarm_options)))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(index.document_projections_resident.load(Ordering::Acquire));
        assert!(index.has_resident_document_projections());
        assert!(index.corpus_stats.initialized());
        assert!(index.partitions.iter().all(|partition| {
            partition.docs.query_ready()
                && partition.inverted_list.modern_posting_validation_ready()
        }));
        assert!(index.prewarm_state.lock().await.satisfies(false));

        let resident = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(resident, deferred);
        assert_eq!(resident.0.len(), 2);
        assert!(resident.0.contains(&100));
        assert!(resident.0.contains(&200));

        cache.clear().await;
        assert!(index.document_projections_resident.load(Ordering::Acquire));
        assert_eq!(cache.size().await, 0);
        let resident_address_owners = index
            .partitions
            .iter()
            .map(|partition| {
                partition
                    .docs
                    .modern()
                    .unwrap()
                    .address_buffer_handle()
                    .strong_count()
            })
            .collect::<Vec<_>>();
        assert_eq!(resident_address_owners, vec![0, 0]);
        assert!(
            index
                .partitions
                .iter()
                .all(|partition| { !partition.docs.modern().unwrap().projection_resident() })
        );

        let after_eviction = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(after_eviction, deferred);
        assert!(!index.document_projections_resident.load(Ordering::Acquire));

        cache.clear().await;
        index.prewarm_with_options(&prewarm_options).await.unwrap();
        assert!(index.document_projections_resident_now());
        assert!(index.document_projections_resident.load(Ordering::Acquire));

        let re_prewarms_after_eviction = index
            .bm25_search(
                tokens,
                params,
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(re_prewarms_after_eviction, deferred);
    }

    #[tokio::test]
    async fn test_resident_modern_search_loads_partition_stats_without_global_stats() {
        let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, false).await;
        assert!(index.corpus_stats.get().is_none());
        assert!(
            index
                .partitions
                .iter()
                .all(|partition| partition.docs.cached_stats().is_none())
        );

        for partition in &index.partitions {
            partition
                .docs
                .modern()
                .unwrap()
                .address_projection()
                .await
                .unwrap();
        }
        assert!(index.has_resident_document_projections());

        let scorer = MemBM25Scorer::new(56_100, 112, HashMap::from([("alpha".to_owned(), 2)]));
        let result = index
            .bm25_search(
                Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text)),
                Arc::new(FtsSearchParams::new().with_limit(Some(2))),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                Some(&scorer),
            )
            .await
            .unwrap();

        assert_eq!(result.0.len(), 2);
        assert!(result.0.contains(&100));
        assert!(result.0.contains(&200));
        assert!(index.corpus_stats.get().is_none());
        assert!(
            index
                .partitions
                .iter()
                .all(|partition| partition.docs.cached_stats().is_some())
        );
    }

    async fn search_test_impact_partition(
        partition: &InvertedPartition,
        tokens: &Tokens,
        params: &FtsSearchParams,
        scorer: Arc<MemBM25Scorer>,
        shared_threshold: Arc<AtomicU32>,
    ) -> Vec<DocCandidate<DocId>> {
        let LoadedPostings {
            postings,
            grouped_expansions,
            impact_safe,
            exact_scoring_required,
        } = partition
            .load_posting_lists(
                tokens,
                params,
                Operator::Or,
                scorer.as_ref(),
                &NoOpMetricsCollector,
                false,
            )
            .await
            .unwrap();
        assert!(impact_safe);
        assert!(!exact_scoring_required);
        assert!(grouped_expansions.is_empty());

        let documents = partition.docs.modern().unwrap();
        let lengths = documents.lengths().await.unwrap();
        let visibility = documents.visibility(NoFilter.mask(), false).await.unwrap();
        partition
            .bm25_search_modern(
                lengths.as_ref(),
                &visibility,
                params,
                Operator::Or,
                postings,
                Some(scorer),
                &NoOpMetricsCollector,
                shared_threshold,
            )
            .unwrap()
    }

    #[tokio::test]
    async fn test_impact_partitions_share_global_threshold_without_pruning_winner() {
        // Partition 0 wins under its local corpus statistics but loses under
        // the global statistics. If its local score escapes into the shared
        // floor, partition 1 will incorrectly prune the real global winner.
        let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, true).await;
        let first_partition = index
            .partitions
            .iter()
            .find(|partition| partition.id() == 0)
            .unwrap();
        let second_partition = index
            .partitions
            .iter()
            .find(|partition| partition.id() == 1)
            .unwrap();

        let tokens = Arc::new(Tokens::new(vec!["alpha".to_owned()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
        let scorer = Arc::new(
            index
                .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
                .await
                .unwrap(),
        );
        first_partition
            .inverted_list
            .ensure_metadata_loaded()
            .await
            .unwrap();
        second_partition
            .inverted_list
            .ensure_metadata_loaded()
            .await
            .unwrap();
        let first_local_scorer = IndexBM25Scorer::new(std::iter::once(first_partition.as_ref()));
        let second_local_scorer = IndexBM25Scorer::new(std::iter::once(second_partition.as_ref()));
        let first_local_score =
            first_local_scorer.query_weight("alpha") * first_local_scorer.doc_weight(1, 5_000);
        let second_local_score =
            second_local_scorer.query_weight("alpha") * second_local_scorer.doc_weight(1, 1_000);
        assert!(first_local_score > second_local_score);
        let shared_threshold = Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));

        // Search sequentially so partition 0 deterministically publishes its
        // score before partition 1 evaluates its impact upper bound.
        let first_candidates = search_test_impact_partition(
            first_partition,
            tokens.as_ref(),
            params.as_ref(),
            scorer.clone(),
            shared_threshold.clone(),
        )
        .await;
        assert_eq!(first_candidates.len(), 1);
        assert_eq!(first_candidates[0].document, DocId::new(0));
        let first_score =
            scorer.query_weight("alpha") * scorer.doc_weight(1, first_candidates[0].doc_length);
        let published_threshold = f32::from_bits(shared_threshold.load(Ordering::Relaxed));
        assert!(
            (published_threshold - first_score).abs() < 1e-6,
            "published threshold: {published_threshold}, expected global score: {first_score}"
        );

        let second_candidates = search_test_impact_partition(
            second_partition,
            tokens.as_ref(),
            params.as_ref(),
            scorer.clone(),
            shared_threshold.clone(),
        )
        .await;
        assert_eq!(second_candidates.len(), 1);
        assert_eq!(second_candidates[0].document, DocId::new(0));
        let second_score =
            scorer.query_weight("alpha") * scorer.doc_weight(1, second_candidates[0].doc_length);
        assert!(
            second_score > first_score,
            "second score: {second_score}, first score: {first_score}"
        );
        assert!(
            (f32::from_bits(shared_threshold.load(Ordering::Relaxed)) - second_score).abs() < 1e-6
        );

        let (row_ids, scores) = index
            .bm25_search(
                tokens,
                params,
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert_eq!(row_ids, vec![200]);
        assert_eq!(scores.len(), 1);
        assert!((scores[0] - second_score).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_mixed_impact_and_legacy_partitions_use_global_final_scores() {
        let (_tmpdir, _cache, index) = load_global_scoring_test_index(true, false).await;

        let impact_partition = index
            .partitions
            .iter()
            .find(|partition| partition.id() == 0)
            .unwrap();
        let legacy_partition = index
            .partitions
            .iter()
            .find(|partition| partition.id() == 1)
            .unwrap();

        let impact_posting = impact_partition
            .inverted_list
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert!(impact_posting.has_impacts());

        let legacy_posting = legacy_partition
            .inverted_list
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert!(!legacy_posting.has_impacts());

        let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
        let (row_ids, scores) = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();

        assert_eq!(row_ids, vec![200]);
        assert_eq!(row_ids.len(), scores.len());

        let scorer = index
            .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
            .await
            .unwrap();
        let expected_score = scorer.query_weight("alpha") * scorer.doc_weight(1, 1_000);
        assert!(
            (scores[0] - expected_score).abs() < 1e-6,
            "score: {}, expected: {}",
            scores[0],
            expected_score
        );
    }

    #[tokio::test]
    async fn test_two_legacy_partitions_keep_private_thresholds() {
        // Legacy BM25 scores use partition-local statistics, so sharing one
        // pruning floor across partitions can discard the global winner.
        let (_tmpdir, _cache, index) = load_global_scoring_test_index(false, false).await;
        for partition in index.partitions.iter() {
            let posting = partition
                .inverted_list
                .posting_list(0, false, &NoOpMetricsCollector)
                .await
                .unwrap();
            assert!(!posting.has_impacts());
        }

        let tokens = Arc::new(Tokens::new(vec!["alpha".to_string()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
        let (row_ids, scores) = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();

        assert_eq!(row_ids, vec![200]);
        assert_eq!(scores.len(), 1);
        let scorer = index
            .bm25_base_scorer(tokens.as_ref(), params.as_ref(), None)
            .await
            .unwrap();
        let expected_score = scorer.query_weight("alpha") * scorer.doc_weight(1, 1_000);
        assert!(
            (scores[0] - expected_score).abs() < 1e-6,
            "score: {}, expected global score: {}",
            scores[0],
            expected_score
        );
    }

    #[tokio::test]
    async fn test_and_query_returns_empty_when_exact_term_missing() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("alpha".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.docs.append(100, 1);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(
            vec!["alpha".to_owned(), "missing".to_owned()],
            DocType::Text,
        ));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);

        let (and_row_ids, _) = index
            .bm25_search(
                tokens.clone(),
                params.clone(),
                Operator::And,
                prefilter.clone(),
                metrics.clone(),
                None,
            )
            .await
            .unwrap();
        assert!(
            and_row_ids.is_empty(),
            "AND must not match when any required term is missing"
        );

        let (or_row_ids, _) = index
            .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
            .await
            .unwrap();
        assert_eq!(
            or_row_ids,
            vec![100],
            "OR should still match the present term"
        );
    }

    #[tokio::test]
    async fn test_and_query_accepts_same_position_alternatives() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for token in ["getusername", "get", "user", "name"] {
            builder.tokens.add(token.to_owned());
            builder.posting_lists.push(PostingListBuilder::new(false));
        }
        // Doc 0 only has the split words. Doc 1 has both the complete
        // identifier and split words. A grouped AND query should accept either
        // `getusername` or `get` at position 0.
        builder.posting_lists[1].add(0, PositionRecorder::Count(1));
        builder.posting_lists[2].add(0, PositionRecorder::Count(1));
        builder.posting_lists[3].add(0, PositionRecorder::Count(1));
        builder.docs.append(100, 3);

        builder.posting_lists[0].add(1, PositionRecorder::Count(1));
        builder.posting_lists[1].add(1, PositionRecorder::Count(1));
        builder.posting_lists[2].add(1, PositionRecorder::Count(1));
        builder.posting_lists[3].add(1, PositionRecorder::Count(1));
        builder.docs.append(101, 4);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::code()).await;
        let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::with_positions(
            vec![
                "getusername".to_string(),
                "get".to_string(),
                "user".to_string(),
                "name".to_string(),
            ],
            vec![0, 0, 1, 2],
            DocType::Text,
        ));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let (mut row_ids, _) = index
            .bm25_search(
                tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        row_ids.sort_unstable();
        assert_eq!(row_ids, vec![100, 101]);
    }

    #[tokio::test]
    async fn test_phrase_query_accepts_same_position_alternatives() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, true, TokenSetFormat::default());
        for token in ["getusername", "get", "user", "name"] {
            builder.tokens.add(token.to_owned());
            builder.posting_lists.push(PostingListBuilder::new(true));
        }
        // Doc 0 only has split words. Doc 1 has both the complete identifier
        // and split words at the same position. Doc 2 has the terms but not as
        // an exact phrase.
        builder.posting_lists[1].add(0, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[2].add(0, PositionRecorder::Position(vec![1].into()));
        builder.posting_lists[3].add(0, PositionRecorder::Position(vec![2].into()));
        builder.docs.append(100, 3);

        builder.posting_lists[0].add(1, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[1].add(1, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[2].add(1, PositionRecorder::Position(vec![1].into()));
        builder.posting_lists[3].add(1, PositionRecorder::Position(vec![2].into()));
        builder.docs.append(101, 3);

        builder.posting_lists[0].add(2, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[2].add(2, PositionRecorder::Position(vec![2].into()));
        builder.posting_lists[3].add(2, PositionRecorder::Position(vec![3].into()));
        builder.docs.append(102, 3);

        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(
            &store,
            vec![0],
            InvertedIndexParams::code().with_position(true),
        )
        .await;
        let index = InvertedIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::with_positions(
            vec![
                "getusername".to_string(),
                "get".to_string(),
                "user".to_string(),
                "name".to_string(),
            ],
            vec![0, 0, 1, 2],
            DocType::Text,
        ));
        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(10))
                .with_phrase_slop(Some(0)),
        );
        let (mut row_ids, _) = index
            .bm25_search(
                tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        row_ids.sort_unstable();
        assert_eq!(row_ids, vec![100, 101]);
    }

    // Enough distinct tokens that `write_posting_lists` emits several posting-list
    // batches (the default batch size is 256 rows), exercising the restructured
    // producer and async send path.
    const MANY_BATCH_TOKENS: u64 = 1000;
    const MANY_BATCH_ROW_ID_BASE: u64 = 1000;

    // Writes a single partition whose posting lists span many output batches. Each
    // token `tok{i:05}` maps to row id `MANY_BATCH_ROW_ID_BASE + i`.
    async fn write_partition_spanning_many_batches(store: &dyn IndexStore) {
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for i in 0..MANY_BATCH_TOKENS {
            // Zero-padded so tokens are inserted in sorted order, as the set expects.
            builder.tokens.add(format!("tok{i:05}"));
            let doc_id = builder.docs.append(MANY_BATCH_ROW_ID_BASE + i, 1);
            let mut posting_list = PostingListBuilder::new(false);
            posting_list.add(doc_id, PositionRecorder::Count(1));
            builder.posting_lists.push(posting_list);
        }
        builder
            .write(store)
            .await
            .expect("writing posting lists should succeed");
    }

    // Correctness guard for the restructured posting-list writer. The producer now
    // builds each batch in its own `spawn_cpu` call, handing the builder and the
    // remaining posting lists back out so state (the cross-batch cache-group
    // accumulator) is preserved, and dispatches with an async `send().await`. This
    // verifies that path over many batches by checking representative tokens after
    // they cross the producer/consumer boundary.
    //
    // Note: this does not reproduce the single-thread-pool deadlock the async send
    // fixes -- that requires a 1-thread CPU pool (a process-global singleton) plus
    // ~8MB of buffered posting data to trigger a consumer-side encoder flush, which
    // is impractical as a lightweight unit test.
    #[tokio::test]
    async fn test_write_many_posting_list_batches_preserves_all_batches() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        write_partition_spanning_many_batches(store.as_ref()).await;

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        // Probe tokens from the first, a middle, and the last batch to confirm they
        // remain queryable after crossing the producer/consumer boundary.
        for token_idx in [0u64, MANY_BATCH_TOKENS / 2, MANY_BATCH_TOKENS - 1] {
            let tokens = Arc::new(Tokens::new(
                vec![format!("tok{token_idx:05}")],
                DocType::Text,
            ));
            let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
            let (row_ids, _) = index
                .bm25_search(
                    tokens,
                    params,
                    Operator::Or,
                    Arc::new(NoFilter),
                    Arc::new(NoOpMetricsCollector),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                row_ids,
                vec![MANY_BATCH_ROW_ID_BASE + token_idx],
                "token tok{token_idx:05} should map to its single document"
            );
        }
    }

    #[tokio::test]
    async fn test_and_query_skips_partition_missing_required_term() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder0 = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder0.tokens.add("alpha".to_owned());
        builder0.posting_lists.push(PostingListBuilder::new(false));
        builder0.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder0.docs.append(100, 1);
        builder0.write(store.as_ref()).await.unwrap();

        let mut builder1 = InnerBuilder::new(1, false, TokenSetFormat::default());
        builder1.tokens.add("alpha".to_owned());
        builder1.tokens.add("beta".to_owned());
        builder1.posting_lists.push(PostingListBuilder::new(false));
        builder1.posting_lists.push(PostingListBuilder::new(false));
        builder1.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder1.posting_lists[1].add(0, PositionRecorder::Count(1));
        builder1.docs.append(200, 2);
        builder1.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(
            vec!["alpha".to_owned(), "beta".to_owned()],
            DocType::Text,
        ));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let (mut row_ids, _) = index
            .bm25_search(
                tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        row_ids.sort_unstable();
        assert_eq!(
            row_ids,
            vec![200],
            "partition missing beta must not contribute alpha-only hits"
        );
    }

    #[tokio::test]
    async fn test_fuzzy_and_groups_expansions_by_original_position() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("alpha".to_owned());
        builder.tokens.add("alphi".to_owned());
        builder.tokens.add("beta".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.posting_lists[1].add(1, PositionRecorder::Count(1));
        builder.posting_lists[2].add(0, PositionRecorder::Count(1));
        builder.posting_lists[2].add(1, PositionRecorder::Count(1));
        builder.docs.append(100, 2);
        builder.docs.append(101, 2);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(10))
                .with_fuzziness(Some(1)),
        );

        let missing_position_tokens = Arc::new(Tokens::new(
            vec!["betx".to_owned(), "zzzzz".to_owned()],
            DocType::Text,
        ));
        let (missing_and_row_ids, _) = index
            .bm25_search(
                missing_position_tokens.clone(),
                params.clone(),
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        assert!(
            missing_and_row_ids.is_empty(),
            "fuzzy AND must require at least one expansion for every original position"
        );

        let (mut or_row_ids, _) = index
            .bm25_search(
                missing_position_tokens,
                params.clone(),
                Operator::Or,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        or_row_ids.sort_unstable();
        assert_eq!(
            or_row_ids,
            vec![100, 101],
            "OR should still match present fuzzy expansions"
        );

        let grouped_tokens = Arc::new(Tokens::new(
            vec!["alphx".to_owned(), "betx".to_owned()],
            DocType::Text,
        ));
        let (mut grouped_row_ids, _) = index
            .bm25_search(
                grouped_tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();
        grouped_row_ids.sort_unstable();
        assert_eq!(
            grouped_row_ids,
            vec![100, 101],
            "each original fuzzy position should match any one of its expansions"
        );
    }

    #[tokio::test]
    async fn test_fuzzy_expansion_cap_applies_to_whole_query() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for token in ["alpha", "alphi", "beta", "beti"] {
            builder.tokens.add(token.to_owned());
            builder.posting_lists.push(PostingListBuilder::new(false));
        }
        for token_id in 0..4 {
            builder.posting_lists[token_id].add(token_id as u32, PositionRecorder::Count(1));
            builder.docs.append(100 + token_id as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        let partition = index.partitions[0].clone();
        let params = FtsSearchParams::new()
            .with_fuzziness(Some(1))
            .with_max_expansions(3);
        let tokens = Tokens::new(vec!["alphx".to_owned(), "betx".to_owned()], DocType::Text);

        let expanded = partition.expand_fuzzy(&tokens, &params).unwrap();
        let expanded_terms = (0..expanded.len())
            .map(|idx| (expanded.get_token(idx).to_owned(), expanded.position(idx)))
            .collect::<Vec<_>>();

        assert_eq!(
            expanded_terms,
            vec![
                ("alpha".to_owned(), 0),
                ("alphi".to_owned(), 0),
                ("beta".to_owned(), 1),
            ],
            "max_expansions should cap the whole fuzzy query, not each token"
        );
    }

    /// Write one partition holding `variants` in order, with one
    /// single-token doc per variant taken from `row_ids`.
    async fn write_variant_partition(
        store: &Arc<LanceIndexStore>,
        partition_id: u64,
        variants: &[&str],
        row_ids: &[u64],
    ) {
        let mut builder = InnerBuilder::new(partition_id, false, TokenSetFormat::default());
        for token in variants {
            builder.tokens.add((*token).to_owned());
            builder.posting_lists.push(PostingListBuilder::new(false));
        }
        for (local_idx, row_id) in row_ids.iter().enumerate() {
            builder.posting_lists[local_idx].add(local_idx as u32, PositionRecorder::Count(1));
            builder.docs.append(*row_id, 1);
        }
        builder.write(store.as_ref()).await.unwrap();
    }

    #[tokio::test]
    async fn test_fuzzy_expansion_cap_is_global_across_partitions() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        write_variant_partition(&store, 0, &["alpha", "alphb"], &[100, 101]).await;
        write_variant_partition(&store, 1, &["alphc", "alphd"], &[102, 103]).await;
        write_test_metadata(&store, vec![0, 1], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let params = FtsSearchParams::new()
            .with_fuzziness(Some(1))
            .with_max_expansions(3);
        let tokens = Tokens::new(vec!["alphx".to_owned()], DocType::Text);

        let expanded = index.expand_fuzzy_tokens(&tokens, &params).unwrap();
        let expanded_terms = (0..expanded.len())
            .map(|idx| expanded.get_token(idx).to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            expanded_terms,
            vec!["alpha".to_owned(), "alphb".to_owned(), "alphc".to_owned()],
            "max_expansions must cap the whole query across partitions, \
             in lexicographic order"
        );
    }

    #[tokio::test]
    async fn test_fuzzy_results_independent_of_partition_shape() {
        // The same four single-variant docs, laid out as one partition and
        // as two. With a binding max_expansions the two shapes must still
        // match the same documents with the same scores.
        let single_dir = TempObjDir::default();
        let single_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            single_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        write_variant_partition(
            &single_store,
            0,
            &["alpha", "alphb", "alphc", "alphd"],
            &[100, 101, 102, 103],
        )
        .await;
        write_test_metadata(&single_store, vec![0], InvertedIndexParams::default()).await;

        let split_dir = TempObjDir::default();
        let split_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            split_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        write_variant_partition(&split_store, 0, &["alpha", "alphb"], &[100, 101]).await;
        write_variant_partition(&split_store, 1, &["alphc", "alphd"], &[102, 103]).await;
        write_test_metadata(&split_store, vec![0, 1], InvertedIndexParams::default()).await;

        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(10))
                .with_fuzziness(Some(1))
                .with_max_expansions(3),
        );

        let mut results = Vec::new();
        for store in [single_store, split_store] {
            let cache = LanceCache::with_capacity(4096);
            let index = InvertedIndex::load(store, None, &cache).await.unwrap();
            let tokens = Arc::new(Tokens::new(vec!["alphx".to_owned()], DocType::Text));
            let (row_ids, scores) = index
                .bm25_search(
                    tokens,
                    params.clone(),
                    Operator::Or,
                    Arc::new(NoFilter),
                    Arc::new(NoOpMetricsCollector),
                    None,
                )
                .await
                .unwrap();
            let mut scored = row_ids.into_iter().zip(scores).collect::<Vec<_>>();
            scored.sort_unstable_by_key(|(row_id, _)| *row_id);
            results.push(scored);
        }

        assert_eq!(
            results[0]
                .iter()
                .map(|(row_id, _)| *row_id)
                .collect::<Vec<_>>(),
            vec![100, 101, 102],
            "a binding cap keeps the three lexicographically smallest variants"
        );
        assert_eq!(
            results[0], results[1],
            "fuzzy results must not depend on the partition shape"
        );
    }

    #[tokio::test]
    async fn test_fuzzy_and_scores_grouped_expansions_by_matched_token() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("alpha".to_owned());
        builder.tokens.add("alphi".to_owned());
        builder.tokens.add("beta".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.posting_lists[0].add(2, PositionRecorder::Count(1));
        builder.posting_lists[0].add(3, PositionRecorder::Count(1));
        builder.posting_lists[0].add(4, PositionRecorder::Count(1));
        builder.posting_lists[0].add(5, PositionRecorder::Count(1));
        builder.posting_lists[1].add(1, PositionRecorder::Count(1));
        builder.posting_lists[2].add(0, PositionRecorder::Count(1));
        builder.posting_lists[2].add(1, PositionRecorder::Count(1));
        builder.docs.append(100, 2);
        builder.docs.append(101, 2);
        builder.docs.append(102, 1);
        builder.docs.append(103, 1);
        builder.docs.append(104, 1);
        builder.docs.append(105, 1);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(
            vec!["alphx".to_owned(), "betx".to_owned()],
            DocType::Text,
        ));
        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(1))
                .with_fuzziness(Some(1)),
        );
        let (row_ids, _scores) = index
            .bm25_search(
                tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            row_ids,
            vec![101],
            "the rare matched expansion should outrank the common expansion"
        );
    }

    #[rstest::rstest]
    #[case::and(Operator::And)]
    #[case::or(Operator::Or)]
    #[tokio::test]
    async fn test_grouped_scoring_keeps_exact_winner_outside_proxy_window(
        #[case] operator: Operator,
    ) {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("common".to_owned());
        builder.tokens.add("rare".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        for doc_id in 0..3 {
            builder.posting_lists[0].add(doc_id, PositionRecorder::Count(1));
            builder.docs.append(100 + doc_id as u64, 1);
        }
        builder.posting_lists[1].add(3, PositionRecorder::Count(1));
        builder.docs.append(103, 2);
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::with_positions(
            vec!["common".to_owned(), "rare".to_owned()],
            vec![0, 0],
            DocType::Text,
        ));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(1)));
        let (row_ids, _scores) = index
            .bm25_search(
                tokens,
                params,
                operator,
                Arc::new(NoFilter),
                Arc::new(NoOpMetricsCollector),
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            row_ids,
            vec![103],
            "the rare term's exact IDF must win even when proxy scoring ranks it outside the old candidate cushion"
        );
    }

    #[tokio::test]
    async fn test_fuzzy_and_grouped_rescore_keeps_wand_limit_bounded() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let num_docs = BLOCK_SIZE * 2 + 4;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("alpha".to_owned());
        builder.tokens.add("alphi".to_owned());
        builder.tokens.add("beta".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists.push(PostingListBuilder::new(false));

        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.posting_lists[1].add(1, PositionRecorder::Count(1));
        for doc_id in 0..num_docs {
            builder.posting_lists[2].add(doc_id as u32, PositionRecorder::Count(1));
            if doc_id >= 2 {
                builder.posting_lists[0].add(doc_id as u32, PositionRecorder::Count(1));
            }
            let num_tokens = if doc_id < 2 { 2 } else { 100 };
            builder.docs.append(100 + doc_id as u64, num_tokens);
        }
        builder.write(store.as_ref()).await.unwrap();

        write_test_metadata(&store, vec![0], InvertedIndexParams::default()).await;
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(
            vec!["alphx".to_owned(), "betx".to_owned()],
            DocType::Text,
        ));
        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(1))
                .with_fuzziness(Some(1)),
        );
        let metrics = Arc::new(LocalMetricsCollector::default());
        let (row_ids, _scores) = index
            .bm25_search(
                tokens,
                params,
                Operator::And,
                Arc::new(NoFilter),
                metrics.clone(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(
            row_ids,
            vec![101],
            "final rescoring should still rank by the matched expansion"
        );
        let comparisons = metrics.comparisons.load(Ordering::Relaxed);
        assert!(
            comparisons < num_docs,
            "grouped fuzzy AND should not clear the WAND top-k bound and scan every candidate; comparisons={comparisons}, num_docs={num_docs}"
        );
    }

    #[tokio::test]
    async fn test_phrase_query_reads_legacy_per_doc_positions() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new_with_format_version(
            0,
            true,
            TokenSetFormat::default(),
            InvertedListFormatVersion::V1,
        );
        builder.tokens.add("hello".to_owned());
        builder.tokens.add("world".to_owned());
        builder
            .posting_lists
            .push(PostingListBuilder::new_with_posting_tail_codec(
                true,
                PostingTailCodec::Fixed32,
            ));
        builder
            .posting_lists
            .push(PostingListBuilder::new_with_posting_tail_codec(
                true,
                PostingTailCodec::Fixed32,
            ));
        builder.posting_lists[0].add(0, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[1].add(0, PositionRecorder::Position(vec![1].into()));
        builder.posting_lists[0].add(1, PositionRecorder::Position(vec![0].into()));
        builder.posting_lists[1].add(1, PositionRecorder::Position(vec![2].into()));
        builder.docs.append(100, 2);
        builder.docs.append(101, 2);
        builder.write(store.as_ref()).await.unwrap();

        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0_u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default().with_position(true)).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        let tokens = Arc::new(Tokens::new(
            vec!["hello".to_owned(), "world".to_owned()],
            DocType::Text,
        ));
        let params = Arc::new(
            FtsSearchParams::new()
                .with_limit(Some(10))
                .with_phrase_slop(Some(0)),
        );
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);

        let (row_ids, _scores) = index
            .bm25_search(tokens, params, Operator::And, prefilter, metrics, None)
            .await
            .unwrap();

        assert_eq!(row_ids, vec![100]);
    }

    /// Build a multi-partition inverted index in `store` with `num_partitions`
    /// partitions, each carrying a handful of tokens/docs.
    async fn build_multi_partition_index(
        store: &Arc<LanceIndexStore>,
        num_partitions: u64,
    ) -> (Arc<InvertedIndex>, Arc<LanceCache>) {
        for id in 0..num_partitions {
            let mut builder = InnerBuilder::new_with_format_version(
                id,
                false,
                TokenSetFormat::default(),
                InvertedListFormatVersion::V1,
            );
            // A few distinct tokens per partition so each posting file has real
            // content to read and materialize during prewarm.
            for t in 0..4u32 {
                builder.tokens.add(format!("tok_{id}_{t}"));
                let mut posting = PostingListBuilder::new_with_posting_tail_codec(
                    false,
                    PostingTailCodec::Fixed32,
                );
                let base = id * 1000 + t as u64 * 10;
                for d in 0..5u32 {
                    posting.add(d, PositionRecorder::Count(1));
                    builder.docs.append(base + d as u64, 4);
                }
                builder.posting_lists.push(posting);
            }
            builder.write(store.as_ref()).await.unwrap();
        }

        let partition_ids: Vec<u64> = (0..num_partitions).collect();
        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&partition_ids).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        // Keep the cache alive and return it: the partition readers hold only a
        // WeakLanceCache, so the prewarmed entries vanish if this Arc is dropped.
        let cache = Arc::new(LanceCache::with_capacity(1 << 20));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();
        (index, cache)
    }

    /// The prewarm cost estimate must come from cheap object metadata (the
    /// posting file length) without reading the posting data, and must be
    /// monotonic in the partition's content.
    #[tokio::test]
    async fn test_posting_data_size_bytes_uses_file_length() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let (index, _cache) = build_multi_partition_index(&store, 3).await;
        for part in &index.partitions {
            // File length is reported by object metadata at open time; it must be
            // non-trivial for a partition that actually holds postings.
            let est = part.inverted_list.posting_data_size_bytes();
            assert!(
                est > 0,
                "expected a non-zero posting-data size estimate, got {est}"
            );
        }
    }

    /// Each partition must read through the shared scheduler at a distinct base
    /// priority. Tied priorities (every partition at 0) break the scheduler's
    /// backpressure deadlock-break — which admits the lowest-priority in-flight
    /// request — because there is no unique lowest request to advance, so a
    /// concurrent multi-partition read (e.g. prewarm) can wedge. Distinct
    /// per-partition priorities keep the in-flight set totally ordered.
    #[tokio::test]
    async fn test_partitions_load_with_distinct_priorities() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let (index, _cache) = build_multi_partition_index(&store, 5).await;

        let mut priorities: Vec<u64> = index
            .partitions
            .iter()
            .map(|part| {
                part.store
                    .as_any()
                    .downcast_ref::<LanceIndexStore>()
                    .expect("partition store should be a LanceIndexStore")
                    .io_priority()
            })
            .collect();

        // Distinct and dense (0..N): every partition reads at its own priority,
        // so the shared scheduler sees a total order across all partitions. The
        // partitions may finish loading in any order, so sort before comparing —
        // what matters is that the priorities form a contiguous, collision-free
        // set, not which partition ended up at which slot.
        priorities.sort_unstable();
        assert_eq!(
            priorities,
            (0..index.partitions.len() as u64).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_update_preserves_v2_format_version() -> Result<()> {
        let src_dir = TempObjDir::default();
        let dest_dir = TempObjDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let format_version = InvertedListFormatVersion::V2;
        let posting_tail_codec = format_version.posting_tail_codec();
        let mut partition = InnerBuilder::new_with_format_version(
            0,
            false,
            TokenSetFormat::default(),
            format_version,
        );
        partition.tokens.add("hello".to_owned());
        let mut posting_list =
            PostingListBuilder::new_with_posting_tail_codec(false, posting_tail_codec);
        posting_list.add(0, PositionRecorder::Count(1));
        partition.posting_lists.push(posting_list);
        partition.docs.append(100, 1);
        partition.write(src_store.as_ref()).await?;

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0_u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
            (
                POSTING_TAIL_CODEC_KEY.to_owned(),
                posting_tail_codec.as_str().to_owned(),
            ),
        ]);
        let mut writer = src_store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let index = InvertedIndex::load(src_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(index.format_version(), format_version);
        assert_eq!(index.index_version(), INVERTED_INDEX_VERSION_V2);

        let schema = Arc::new(Schema::new(vec![
            Field::new("doc", DataType::Utf8, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let docs = Arc::new(StringArray::from(vec![Some("hello again")]));
        let row_ids = Arc::new(UInt64Array::from(vec![101u64]));
        let batch = RecordBatch::try_new(schema.clone(), vec![docs, row_ids])?;
        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
        let created = index
            .update(Box::pin(stream), dest_store.as_ref(), None)
            .await?;

        assert_eq!(created.index_version, INVERTED_INDEX_VERSION_V2);

        let updated = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(updated.format_version(), format_version);
        assert_eq!(updated.index_version(), INVERTED_INDEX_VERSION_V2);
        assert_eq!(updated.partitions.len(), 2);
        for partition in &updated.partitions {
            assert_eq!(
                partition.inverted_list.posting_tail_codec(),
                posting_tail_codec
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_block_size_256_writes_v3_metadata_and_index_version() -> Result<()> {
        let src_dir = TempObjDir::default();
        let dest_dir = TempObjDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let params = InvertedIndexParams::default().block_size(256)?;
        let format_version = params.resolved_format_version();
        assert_eq!(format_version, InvertedListFormatVersion::V3);

        let mut partition = InnerBuilder::new_with_format_version_and_block_size(
            0,
            false,
            TokenSetFormat::default(),
            format_version,
            params.posting_block_size(),
        );
        partition.tokens.add("hello".to_owned());
        let mut posting_list = PostingListBuilder::new_with_posting_tail_codec_and_block_size(
            false,
            format_version.posting_tail_codec(),
            params.posting_block_size(),
        );
        posting_list.add(0, PositionRecorder::Count(1));
        partition.posting_lists.push(posting_list);
        partition.docs.append(100, 1);
        partition.write(src_store.as_ref()).await?;

        write_test_metadata(&src_store, vec![0], params).await;

        let index = InvertedIndex::load(src_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(index.format_version(), InvertedListFormatVersion::V3);
        assert_eq!(index.index_version(), INVERTED_INDEX_VERSION_V3);

        let created = index
            .update(empty_doc_stream(), dest_store.as_ref(), None)
            .await?;
        assert_eq!(created.index_version, INVERTED_INDEX_VERSION_V3);

        let updated = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(updated.format_version(), InvertedListFormatVersion::V3);
        assert_eq!(updated.index_version(), INVERTED_INDEX_VERSION_V3);

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_segments_preserves_arrow_token_set_format() -> Result<()> {
        let src_dir = TempObjDir::default();
        let dest_dir = TempObjDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let index = write_single_partition_index(
            src_store,
            InvertedIndexParams::default().format_version(InvertedListFormatVersion::V2),
            TokenSetFormat::Arrow,
            "hello",
            100,
        )
        .await?;
        assert_eq!(index.index_version(), 0);
        let created = InvertedIndex::merge_segments(
            &[index],
            empty_doc_stream(),
            dest_store.as_ref(),
            None,
            crate::progress::noop_progress(),
        )
        .await?;

        assert_eq!(created.index_version, 0);
        let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(merged.index_version(), 0);
        assert_eq!(merged.token_set_format, TokenSetFormat::Arrow);

        let tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));
        let params = Arc::new(FtsSearchParams::new().with_limit(Some(10)));
        let prefilter = Arc::new(NoFilter);
        let metrics = Arc::new(NoOpMetricsCollector);
        let (row_ids, _) = merged
            .bm25_search(tokens, params, Operator::Or, prefilter, metrics, None)
            .await?;
        assert_eq!(row_ids, vec![100]);

        Ok(())
    }

    #[rstest::rstest]
    #[case::v1(InvertedListFormatVersion::V1, LEGACY_BLOCK_SIZE)]
    #[case::v2(InvertedListFormatVersion::V2, LEGACY_BLOCK_SIZE)]
    #[case::v3_128(InvertedListFormatVersion::V3, LEGACY_BLOCK_SIZE)]
    #[case::v3_256(InvertedListFormatVersion::V3, 256)]
    #[tokio::test]
    async fn test_merge_segments_preserves_format_version(
        #[case] format_version: InvertedListFormatVersion,
        #[case] block_size: usize,
    ) -> Result<()> {
        let src_dir = TempObjDir::default();
        let dest_dir = TempObjDir::default();
        let src_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let params = InvertedIndexParams::default()
            .block_size(block_size)?
            .format_version(format_version);

        let index =
            write_single_partition_index(src_store, params, TokenSetFormat::Fst, "hello", 100)
                .await?;
        assert_eq!(index.format_version(), format_version);

        let created = InvertedIndex::merge_segments(
            &[index],
            empty_doc_stream(),
            dest_store.as_ref(),
            None,
            crate::progress::noop_progress(),
        )
        .await?;
        assert_eq!(created.index_version, format_version.index_version());

        let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(merged.format_version(), format_version);
        assert_eq!(merged.index_version(), format_version.index_version());

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_segments_uses_memory_limit_for_old_partitions() -> Result<()> {
        let src_dir_1 = TempObjDir::default();
        let src_dir_2 = TempObjDir::default();
        let dest_dir = TempObjDir::default();
        let src_store_1 = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir_1.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let src_store_2 = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            src_dir_2.clone(),
            Arc::new(LanceCache::no_cache()),
        ));
        let dest_store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            dest_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let params = InvertedIndexParams::default().memory_limit_mb(0);
        let first = write_single_partition_index(
            src_store_1,
            params.clone(),
            TokenSetFormat::default(),
            "alpha",
            100,
        )
        .await?;
        let second = write_single_partition_index(
            src_store_2,
            params,
            TokenSetFormat::default(),
            "beta",
            200,
        )
        .await?;

        let mut builder =
            InvertedIndexBuilder::new(InvertedIndexParams::default().memory_limit_mb(0))
                .with_token_set_format(TokenSetFormat::default());
        builder
            .update_from_segments(
                empty_doc_stream(),
                dest_store.as_ref(),
                &[first, second],
                None,
            )
            .await?;

        let merged = InvertedIndex::load(dest_store, None, &LanceCache::no_cache()).await?;
        assert_eq!(merged.partitions.len(), 2);
        let mut partition_ids = merged
            .partitions
            .iter()
            .map(|partition| partition.id())
            .collect::<Vec<_>>();
        partition_ids.sort_unstable();
        assert_eq!(partition_ids, vec![0, 1]);

        Ok(())
    }

    #[tokio::test]
    async fn test_modern_index_without_deleted_col_has_empty_bitmap() {
        // An index created before the deleted_fragments feature was added
        // will have a metadata file with num_rows=0 (no record batch data).
        // The load path should gracefully handle this with an empty bitmap.
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("test".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        builder.posting_lists[0].add(0, PositionRecorder::Count(1));
        builder.docs.append(100, 1);
        builder.write(store.as_ref()).await.unwrap();

        // Write a metadata file WITHOUT the deleted_fragments column
        // (simulates an older index version)
        let metadata = std::collections::HashMap::from_iter(vec![
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let index = InvertedIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(
            index.deleted_fragments().is_empty(),
            "index without deleted_fragments column should have empty bitmap"
        );
    }

    #[tokio::test]
    async fn flat_bm25_search_stream_with_metrics_records_elapsed_compute() {
        use crate::scalar::inverted::tokenizer::document_tokenizer::TextTokenizer;
        use arrow_array::{StringArray, UInt64Array};
        use lance_tokenizer::{SimpleTokenizer, TextAnalyzer};

        // Tiny stream of one batch containing the query term in two rows.
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "the quick brown fox",
                    "lazy dog sleeps",
                    "the brown fox jumps over",
                    "completely unrelated text",
                ])),
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));

        let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
            TextAnalyzer::builder(SimpleTokenizer::default()).build(),
        ));

        let elapsed_compute = Time::default();
        let result_stream = flat_bm25_search_stream_with_metrics(
            input,
            "text".to_string(),
            "fox".to_string(),
            tokenizer,
            None,
            100,
            Some(elapsed_compute.clone()),
        )
        .await
        .unwrap();

        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        assert!(!batches.is_empty(), "expected at least one scored batch");

        // Both phase 1 (tokenize_and_count's spawn_cpu) and phase 2 (sync
        // scoring) call `add_duration` on the metric; verify the handle
        // was actually populated.
        assert!(
            elapsed_compute.value() > 0,
            "elapsed_compute should have been populated; got 0"
        );
    }

    #[tokio::test]
    async fn flat_bm25_phrase_honors_positions_slop_and_repeated_terms() {
        async fn search(query: &str, slop: u32) -> Vec<u64> {
            let schema = Arc::new(Schema::new(vec![
                ROW_ID_FIELD.clone(),
                Field::new("text", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(0..5)),
                    Arc::new(StringArray::from(vec![
                        "alpha beta",
                        "alpha gap beta",
                        "alpha gap gap beta",
                        "alpha alpha beta",
                        "beta alpha",
                    ])),
                ],
            )
            .unwrap();
            let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::iter(vec![Ok(batch)]),
            ));
            let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
                TextAnalyzer::builder(SimpleTokenizer::default()).build(),
            ));
            let (stream, _) = flat_bm25_search_stream_with_options_and_scorer(
                input,
                "text".to_string(),
                query.to_string(),
                tokenizer,
                None,
                FlatBm25SearchOptions {
                    target_batch_size: 100,
                    elapsed_compute: None,
                    document_granularity: DocumentGranularity::Row,
                    operator: Operator::And,
                    boost: 1.0,
                    phrase_slop: Some(slop),
                },
            )
            .await
            .unwrap();
            stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| batch[ROW_ID].as_primitive::<UInt64Type>().values())
                .copied()
                .collect()
        }

        assert_eq!(search("alpha beta", 0).await, vec![0, 3]);
        assert_eq!(search("alpha beta", 1).await, vec![0, 1, 3]);
        assert_eq!(search("alpha alpha", 0).await, vec![3]);
    }

    #[test]
    fn flat_full_text_search_supports_phrase_queries() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from_iter_values(0..4)),
                Arc::new(StringArray::from(vec![
                    "alpha beta",
                    "alpha gap beta",
                    "alpha alpha beta",
                    "beta alpha",
                ])),
            ],
        )
        .unwrap();

        assert_eq!(
            flat_full_text_search(&[&batch], "text", "\"alpha beta\"", None).unwrap(),
            vec![0, 2]
        );
        assert_eq!(
            flat_full_text_search(&[&batch], "text", "\"alpha alpha\"", None).unwrap(),
            vec![2]
        );
        assert!(!is_phrase_query("\""));
    }

    #[tokio::test]
    async fn flat_bm25_skips_zero_token_documents_from_corpus_stats() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![0_u64, 1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("   "),
                    Some("the"),
                    Some("overlength"),
                    None,
                    Some("hello"),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let params = InvertedIndexParams::new("whitespace".to_string(), Language::English)
            .remove_stop_words(true)
            .stem(false)
            .max_token_length(Some(6));
        let query_tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));

        let counted_input = tokenize_and_count(
            stream::iter(vec![Ok(batch)]),
            params.build().unwrap(),
            query_tokens.clone(),
            1,
            None,
            0,
            None,
        )
        .await
        .unwrap();

        assert_eq!(counted_input.num_rows(), 1);
        assert_eq!(
            counted_input[ROW_ID].as_primitive::<UInt64Type>().values(),
            &[5]
        );
        let scorer = initialize_scorer(None, query_tokens.as_ref(), &counted_input);
        let expected_scorer = MemBM25Scorer::new(1, 1, HashMap::from([("hello".to_string(), 1)]));
        assert_eq!(scorer.total_tokens, 1);
        assert_eq!(scorer.num_docs(), 1);
        assert_eq!(scorer.num_docs_containing_token("hello"), 1);
        assert_eq!(scorer.avg_doc_length(), expected_scorer.avg_doc_length());
        assert_eq!(
            scorer.query_weight("hello"),
            expected_scorer.query_weight("hello")
        );
    }

    #[tokio::test]
    async fn flat_bm25_preserves_zero_token_list_element_documents() {
        let coordinate_column = doc_index_storage_column(0);
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new(&coordinate_column, DataType::UInt32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![7_u64; 6])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3, 4, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    None,
                    Some(""),
                    Some("   "),
                    Some("the"),
                    Some("overlength"),
                    Some("hello"),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let params = InvertedIndexParams::new("whitespace".to_string(), Language::English)
            .remove_stop_words(true)
            .stem(false)
            .max_token_length(Some(6));
        let query_tokens = Arc::new(Tokens::new(vec!["hello".to_string()], DocType::Text));

        let counted_input = tokenize_and_count(
            stream::iter(vec![Ok(batch)]),
            params.build().unwrap(),
            query_tokens.clone(),
            2,
            None,
            1,
            None,
        )
        .await
        .unwrap();

        assert_eq!(counted_input.num_rows(), 6);
        assert_eq!(
            counted_input[&coordinate_column]
                .as_primitive::<UInt32Type>()
                .values(),
            &[0, 1, 2, 3, 4, 5]
        );
        let scorer = initialize_scorer(None, query_tokens.as_ref(), &counted_input);
        assert_eq!(scorer.total_tokens, 1);
        assert_eq!(scorer.num_docs(), 6);
        assert_eq!(scorer.num_docs_containing_token("hello"), 1);
    }

    #[tokio::test]
    async fn flat_bm25_search_uses_full_document_length_for_normalization() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1])),
                Arc::new(StringArray::from(vec![
                    "alpha",
                    "alpha filler filler filler filler filler filler filler filler filler",
                ])),
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));
        let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
            TextAnalyzer::builder(SimpleTokenizer::default()).build(),
        ));

        let result_stream = flat_bm25_search_stream_with_metrics(
            input,
            "text".to_string(),
            "alpha".to_string(),
            tokenizer,
            None,
            100,
            None,
        )
        .await
        .unwrap();
        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
        let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>();
        let scores = scored[SCORE_COL].as_primitive::<Float32Type>();

        assert_eq!(row_ids.values(), &[0, 1]);
        assert!(
            scores.value(0) > scores.value(1),
            "same term frequency should score shorter document higher; short={}, long={}",
            scores.value(0),
            scores.value(1)
        );
    }

    #[tokio::test]
    async fn flat_bm25_search_treats_string_lists_as_row_documents() {
        let mut docs_builder =
            GenericListBuilder::<i32, _>::new(GenericStringBuilder::<i32>::new());
        docs_builder.values().append_value("alpha");
        docs_builder.values().append_value("alpha beta");
        docs_builder.append(true);
        docs_builder.values().append_value("beta");
        docs_builder.append(true);
        docs_builder.append(true);
        docs_builder.values().append_null();
        docs_builder.append(true);
        docs_builder.append(false);

        let docs = Arc::new(docs_builder.finish()) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", docs.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3, 4])) as ArrayRef,
                docs,
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));
        let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
            TextAnalyzer::builder(SimpleTokenizer::default()).build(),
        ));

        let result_stream = flat_bm25_search_stream_with_metrics(
            input,
            "text".to_string(),
            "alpha".to_string(),
            tokenizer,
            None,
            100,
            None,
        )
        .await
        .unwrap();
        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
        let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>();

        assert_eq!(row_ids.values(), &[0]);
    }

    #[tokio::test]
    async fn flat_bm25_search_code_and_uses_position_groups() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("code", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "get user name",
                    "getUserName",
                    "get user",
                    "username",
                ])),
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));
        let tokenizer = InvertedIndexParams::code()
            .split_identifiers(true)
            .build()
            .unwrap();

        let result_stream = flat_bm25_search_stream_with_metrics_and_operator(
            input,
            "code".to_string(),
            "getUserName".to_string(),
            tokenizer,
            None,
            100,
            Operator::And,
            None,
        )
        .await
        .unwrap();

        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
        let mut row_ids = scored[ROW_ID]
            .as_primitive::<UInt64Type>()
            .values()
            .to_vec();
        row_ids.sort_unstable();

        assert_eq!(row_ids, vec![0, 1]);
    }

    #[tokio::test]
    async fn flat_bm25_search_code_and_counts_repeated_subwords() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("code", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1])),
                Arc::new(StringArray::from(vec![
                    "pub fn edge_flat_generic_return<T>() -> Result<T, EdgeFlatError> where T: TryFrom<String> { todo!() }",
                    "pub fn edge_flat_generic_return<T>() -> Result<T> { todo!() }",
                ])),
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));
        let tokenizer = InvertedIndexParams::code().build().unwrap();

        let result_stream = flat_bm25_search_stream_with_metrics_and_operator(
            input,
            "code".to_string(),
            "edge_flat_generic_return TryFrom EdgeFlatError Result".to_string(),
            tokenizer,
            None,
            100,
            Operator::And,
            None,
        )
        .await
        .unwrap();

        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        let scored = arrow::compute::concat_batches(&FTS_SCHEMA, &batches).unwrap();
        let row_ids = scored[ROW_ID].as_primitive::<UInt64Type>().values();

        assert_eq!(row_ids, &[0]);
    }

    fn posting_entries(posting: &PostingList) -> Vec<(u64, u32)> {
        posting.iter().map(|(doc, freq, _)| (doc, freq)).collect()
    }

    #[tokio::test]
    async fn test_modern_posting_validation_is_cached_per_token() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.tokens.add("term".to_owned());
        let mut valid_builder = PostingListBuilder::new(false);
        valid_builder.add(0, PositionRecorder::Count(1));
        builder.posting_lists.push(valid_builder);
        builder.docs.append(1000, 1);
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let mut posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
            .await
            .unwrap();
        posting_reader.modern_num_docs = Some(1);
        let validation = &posting_reader
            .modern_doc_id_validations
            .as_ref()
            .expect("modern readers have per-token validation state")[0];
        assert!(validation.get().is_none());
        assert!(!posting_reader.modern_posting_is_validated(0).unwrap());

        let mut corrupt_builder = PostingListBuilder::new(false);
        corrupt_builder.add(1, PositionRecorder::Count(1));
        let corrupt_batch = corrupt_builder.to_batch(vec![1.0]).unwrap();
        let corrupt_posting = PostingList::from_batch(&corrupt_batch, Some(1.0), Some(1)).unwrap();
        let error = posting_reader
            .ensure_modern_posting_validated(0, &corrupt_posting)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Index { .. }));
        assert!(error.to_string().contains("DocId 1"));
        assert!(error.to_string().contains("[0, 1)"));
        assert!(validation.get().is_none());
        assert!(!posting_reader.modern_posting_is_validated(0).unwrap());

        let first = posting_reader
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(posting_entries(&first), vec![(0, 1)]);
        assert!(validation.get().is_some());
        assert!(posting_reader.modern_posting_is_validated(0).unwrap());

        let second = posting_reader
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(posting_entries(&second), vec![(0, 1)]);
        assert!(validation.get().is_some());
    }

    /// Runtime synthetic grouping must return correct posting lists for every
    /// token, including across synthetic group boundaries.
    #[tokio::test]
    async fn test_posting_list_synthetic_grouping_reads_group_boundaries() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let num_tokens = runtime_posting_group_tokens() as u32 + 4;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for t in 0..num_tokens {
            builder.tokens.add(format!("t{t}"));
            let mut pl = PostingListBuilder::new(false);
            pl.add(t, PositionRecorder::Count(1));
            builder.posting_lists.push(pl);
            builder.docs.append(1000 + t as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let cache = LanceCache::no_cache();
        let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
        posting_reader.modern_num_docs = Some(num_tokens as usize);
        assert!(
            matches!(
                &posting_reader.grouping,
                PostingGrouping::SyntheticFixed { .. }
            ),
            "v2 reader must synthesize runtime posting groups",
        );

        let metrics = NoOpMetricsCollector;
        for token in 0..num_tokens {
            let posting = posting_reader
                .posting_list(token, false, &metrics)
                .await
                .unwrap();
            assert_eq!(
                posting_entries(&posting),
                vec![(token as u64, 1)],
                "synthetic grouping mismatch for token {token}",
            );
            assert_eq!(posting.len(), 1, "length mismatch for token {token}");
        }
    }

    /// Prewarm must populate exactly the `PostingListGroupKey`s the read path
    /// looks up — in particular the final group, whose `end` both paths derive
    /// from `self.len()`. If those derivations drifted (e.g. one used
    /// `num_rows()` and the other the loaded posting count), the last group's
    /// warm entry would be missing and prewarm silently wasted (issue #7040).
    #[tokio::test]
    async fn test_prewarm_group_keys_match_read_path() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let num_tokens = runtime_posting_group_tokens() as u32 + 4;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for t in 0..num_tokens {
            builder.tokens.add(format!("t{t}"));
            let mut pl = PostingListBuilder::new(false);
            pl.add(t, PositionRecorder::Count(1));
            builder.posting_lists.push(pl);
            builder.docs.append(1000 + t as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        // A real (strong) cache must outlive the reader's weak handle so the
        // prewarmed entries are still resolvable below.
        let cache = LanceCache::with_capacity(1 << 20);
        let mut posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
        posting_reader.modern_num_docs = Some(num_tokens as usize);
        assert!(
            matches!(
                &posting_reader.grouping,
                PostingGrouping::SyntheticFixed { .. }
            ),
            "v2 reader should use runtime synthetic groups",
        );

        posting_reader
            .prewarm_posting_lists(false, 2)
            .await
            .unwrap();
        assert!(posting_reader.modern_posting_validation_ready());
        assert!(
            posting_reader
                .modern_postings_validated
                .load(Ordering::Acquire)
        );

        for token in 0..num_tokens {
            let (start, end) = posting_reader.group_range_for_token(token).unwrap();
            assert!(
                posting_reader
                    .index_cache
                    .get_with_key(&posting_list_group_cache_key(
                        start,
                        end,
                        posting_reader.has_impacts,
                    ))
                    .await
                    .is_some(),
                "prewarm did not populate group [{start}, {end}) that the read \
                 path requests for token {token}",
            );
        }

        let (_, last_end) = posting_reader
            .group_range_for_token(num_tokens - 1)
            .unwrap();
        assert_eq!(
            last_end, num_tokens,
            "the last group must end at the posting count ({num_tokens})",
        );
    }

    /// An empty partition has no synthetic groups because there are no token
    /// rows to cache.
    #[tokio::test]
    async fn test_empty_partition_has_no_synthetic_groups() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(
            matches!(&posting_reader.grouping, PostingGrouping::None),
            "reader for an empty partition must not create cache groups",
        );
        assert!(posting_reader.is_empty());
    }

    /// A large posting list can share a runtime synthetic group with neighbors;
    /// grouping is token-count based and should still read every member intact.
    #[tokio::test]
    async fn test_large_posting_reads_inside_synthetic_group() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        let big_docs = (BLOCK_SIZE * 3 + 5) as u32;
        builder.tokens.add("big".to_owned());
        let mut big = PostingListBuilder::new(false);
        for d in 0..big_docs {
            big.add(d, PositionRecorder::Count(1));
        }
        builder.posting_lists.push(big);
        for t in 1..5u32 {
            builder.tokens.add(format!("t{t}"));
            let mut pl = PostingListBuilder::new(false);
            pl.add(0, PositionRecorder::Count(1));
            builder.posting_lists.push(pl);
        }
        for d in 0..big_docs as u64 {
            builder.docs.append(1000 + d, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let posting_reader = PostingListReader::try_new(reader, &LanceCache::no_cache())
            .await
            .unwrap();
        let expected_end = runtime_posting_group_tokens().min(5) as u32;

        assert_eq!(
            posting_reader.group_range_for_token(0),
            Some((0, expected_end)),
            "runtime synthetic grouping should group by token count, not posting bytes",
        );
        let big = posting_reader
            .posting_list(0, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(big.len(), big_docs as usize);
        // A trailing tiny term (in the next, multi-token group) still reads back.
        let tiny = posting_reader
            .posting_list(2, false, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(tiny.len(), 1);
    }

    /// Non-empty v2 indexes should prewarm synthetic `PostingListGroupKey`
    /// entries, matching what the read path then looks up without persisted
    /// grouping metadata.
    #[tokio::test]
    async fn test_prewarm_synthetic_grouping_populates_group_entries() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let num_tokens = 3u32;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for t in 0..num_tokens {
            builder.tokens.add(format!("t{t}"));
            let mut pl = PostingListBuilder::new(false);
            pl.add(t, PositionRecorder::Count(1));
            builder.posting_lists.push(pl);
            builder.docs.append(1000 + t as u64, 1);
        }
        builder.write(store.as_ref()).await.unwrap();

        let reader = store.open_index_file(&posting_file_path(0)).await.unwrap();
        let cache = LanceCache::with_capacity(1 << 20);
        let posting_reader = PostingListReader::try_new(reader, &cache).await.unwrap();
        assert!(matches!(
            &posting_reader.grouping,
            PostingGrouping::SyntheticFixed { .. }
        ));

        posting_reader
            .prewarm_posting_lists(false, 2)
            .await
            .unwrap();

        for token_id in 0..num_tokens {
            let (start, end) = posting_reader.group_range_for_token(token_id).unwrap();
            let group = posting_reader
                .index_cache
                .get_with_key(&posting_list_group_cache_key(
                    start,
                    end,
                    posting_reader.has_impacts,
                ))
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "synthetic prewarm should populate group [{start}, {end}) for token {token_id}"
                    )
                });
            assert!(
                group.is_packed(),
                "no-position synthetic prewarm should insert a packed group"
            );
            assert!(
                posting_reader
                    .index_cache
                    .get_with_key(&posting_list_cache_key(
                        token_id,
                        posting_reader.has_impacts,
                    ))
                    .await
                    .is_none(),
                "synthetic prewarm should not populate per-token entry {token_id}",
            );
        }
    }

    /// End-to-end BM25 search over a grouped multi-group index must return the
    /// correct documents, and a warm-cache query must match the cold-cache
    /// result exactly (issue #7040).
    #[tokio::test]
    async fn test_grouped_bm25_search_correct_and_cache_stable() {
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Rare tokens (one doc each) plus one common token in every doc. The
        // token count exceeds the runtime group size so scoring must index
        // into the right synthetic group slot.
        let num_rare = runtime_posting_group_tokens() as u32 + 2;
        let mut builder = InnerBuilder::new(0, false, TokenSetFormat::default());
        for t in 0..num_rare {
            builder.tokens.add(format!("t{t}"));
            builder.posting_lists.push(PostingListBuilder::new(false));
        }
        let common_id = builder.tokens.add("common".to_owned());
        builder.posting_lists.push(PostingListBuilder::new(false));
        for d in 0..num_rare {
            builder.posting_lists[d as usize].add(d, PositionRecorder::Count(1));
            builder.posting_lists[common_id as usize].add(d, PositionRecorder::Count(1));
            builder.docs.append(1000 + d as u64, 2);
        }
        builder.write(store.as_ref()).await.unwrap();

        let metadata = HashMap::from([
            (
                "partitions".to_owned(),
                serde_json::to_string(&vec![0u64]).unwrap(),
            ),
            (
                "params".to_owned(),
                serde_json::to_string(&InvertedIndexParams::default()).unwrap(),
            ),
            (
                TOKEN_SET_FORMAT_KEY.to_owned(),
                TokenSetFormat::default().to_string(),
            ),
        ]);
        let mut writer = store
            .new_index_file(METADATA_FILE, Arc::new(arrow_schema::Schema::empty()))
            .await
            .unwrap();
        writer.finish_with_metadata(metadata).await.unwrap();

        let cache = Arc::new(LanceCache::with_capacity(1 << 20));
        let index = InvertedIndex::load(store.clone(), None, cache.as_ref())
            .await
            .unwrap();

        // A rare token in the middle of a group must resolve to its one doc.
        let query = |term: &str| {
            let index = index.clone();
            let term = term.to_string();
            async move {
                index
                    .bm25_search(
                        Arc::new(Tokens::new(vec![term], DocType::Text)),
                        Arc::new(FtsSearchParams::new().with_limit(Some(num_rare as usize))),
                        Operator::Or,
                        Arc::new(NoFilter),
                        Arc::new(NoOpMetricsCollector),
                        None,
                    )
                    .await
                    .unwrap()
            }
        };

        let rare_query_id = num_rare / 2;
        let (rare_rows, _) = query(&format!("t{rare_query_id}")).await;
        assert_eq!(
            rare_rows,
            vec![1000 + rare_query_id as u64],
            "rare token must map to its single doc",
        );

        // Cold vs warm cache must agree for the common (large) token.
        let (cold_rows, cold_scores) = query("common").await;
        let (warm_rows, warm_scores) = query("common").await;
        assert_eq!(cold_rows.len(), num_rare as usize);
        assert_eq!(cold_rows, warm_rows, "warm-cache rows must match cold");
        assert_eq!(
            cold_scores, warm_scores,
            "warm-cache scores must match cold"
        );
    }

    #[tokio::test]
    async fn flat_bm25_search_stop_word_query_over_unindexed_rows_returns_empty() {
        let schema = Arc::new(Schema::new(vec![
            ROW_ID_FIELD.clone(),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64, 1, 2])),
                Arc::new(StringArray::from(vec![
                    "the quick brown fox",
                    "a lazy dog",
                    "for the win",
                ])),
            ],
        )
        .unwrap();

        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            stream::iter(vec![Ok(batch)]),
        ));

        // Analyzer with an English stop-word filter, so the query "the"
        // tokenizes to zero terms -- exactly the production trigger.
        let tokenizer: Box<dyn LanceTokenizer> = Box::new(TextTokenizer::new(
            TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(StopWordFilter::new(Language::English).unwrap())
                .build(),
        ));

        let result_stream = flat_bm25_search_stream_with_metrics(
            input,
            "text".to_string(),
            "the".to_string(),
            tokenizer,
            None,
            100,
            None,
        )
        .await
        .unwrap();

        let batches: Vec<_> = result_stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "a stop-word-only query has no searchable terms and must match nothing"
        );
    }
}
