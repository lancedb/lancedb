// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::{
    cell::{RefCell, UnsafeCell},
    collections::{BinaryHeap, VecDeque},
};
use std::{cmp::Reverse, fmt::Debug};

use arrow::array::AsArray;
use arrow::datatypes::Int32Type;
use arrow_array::Array;
use itertools::Itertools;
use lance_core::utils::address::RowAddress;
use lance_core::{Error, Result};
use lance_select::RowAddrMask;

use crate::metrics::MetricsCollector;

use super::{
    CompressedPositionStorage,
    documents::{DocId, DocLengths, DocVisibility},
    impact::{IMPACT_LEVEL1_BLOCKS, ImpactScoreCache, ImpactSkipData},
    index::{PositionStreamCodec, dequantize_doc_length},
    query::Operator,
    scorer::{K1, MemBM25Scorer, bm25_doc_weight_with_norm, idf},
};
use super::{
    CompressedPostingList, DocSet, PostingList, RawDocInfo,
    builder::ScoredDoc,
    encoding::{
        MAX_POSTING_BLOCK_SIZE, decode_position_stream_block, decompress_positions,
        decompress_posting_block, decompress_posting_remainder, seek_packed_doc_positions,
    },
    query::FtsSearchParams,
    scorer::Scorer,
};
use super::{DocInfo, builder::BLOCK_SIZE};

const TERMINATED_DOC_ID: u64 = u64::MAX;

/// Top-k heap entry: (scored doc, doc length, posting doc id, frequency slot).
/// The (term, freq) pairs live outside the heap so heap churn moves a compact
/// tuple instead of a `Vec`.
type TopKHeap = BinaryHeap<Reverse<(ScoredDoc, u32, u64, u32)>>;

/// Reusable (term, freq) storage for live heap candidates. Replacing the k-th
/// result reuses its slot and retained `Vec` capacity, so memory stays bounded
/// by the live top-k rather than every historical heap admission.
struct FrequencySlots {
    slots: Vec<Vec<(u32, u32)>>,
}

impl FrequencySlots {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, pairs: impl Iterator<Item = (u32, u32)>) -> Result<u32> {
        let slot = u32::try_from(self.slots.len()).map_err(|_| {
            Error::internal(format!(
                "FTS top-k frequency slot count {} exceeds u32::MAX",
                self.slots.len()
            ))
        })?;
        self.slots.push(pairs.collect());
        Ok(slot)
    }

    fn replace(&mut self, slot: u32, pairs: impl Iterator<Item = (u32, u32)>) -> Result<()> {
        let num_slots = self.slots.len();
        let slot = self.slots.get_mut(slot as usize).ok_or_else(|| {
            Error::internal(format!(
                "FTS top-k frequency slot {slot} is out of bounds for {num_slots} slots"
            ))
        })?;
        slot.clear();
        slot.extend(pairs);
        Ok(())
    }

    fn take(&mut self, slot: u32) -> Result<Vec<(u32, u32)>> {
        let num_slots = self.slots.len();
        self.slots
            .get_mut(slot as usize)
            .map(std::mem::take)
            .ok_or_else(|| {
                Error::internal(format!(
                    "FTS top-k frequency slot {slot} is out of bounds for {num_slots} slots"
                ))
            })
    }
}

/// Owns the top-k heap and the frequency slots referenced by its entries.
struct TopKCollector {
    limit: usize,
    heap: TopKHeap,
    frequency_slots: FrequencySlots,
}

impl TopKCollector {
    fn new(limit: usize, initial_capacity: usize) -> Self {
        let initial_capacity = initial_capacity.min(limit);
        Self {
            limit,
            heap: BinaryHeap::with_capacity(initial_capacity),
            frequency_slots: FrequencySlots::with_capacity(initial_capacity),
        }
    }

    /// Insert a competitive result. When the heap is full, its evicted entry's
    /// frequency slot is cleared and reused before the replacement is pushed.
    fn insert(
        &mut self,
        doc: ScoredDoc,
        doc_length: u32,
        posting_doc_id: u64,
        pairs: impl Iterator<Item = (u32, u32)>,
    ) -> Result<bool> {
        if self.limit == 0 {
            return Ok(false);
        }
        if self.heap.len() > self.limit {
            return Err(Error::internal(format!(
                "FTS top-k heap length {} exceeds limit {}",
                self.heap.len(),
                self.limit
            )));
        }

        let frequency_slot = if self.heap.len() == self.limit {
            let Some(kth_score) = self.heap.peek().map(|entry| entry.0.0.score.0) else {
                return Err(Error::internal(
                    "FTS top-k heap is empty while its nonzero limit is reached",
                ));
            };
            // Preserve the existing collector semantics for non-finite custom
            // scorer output: only a strictly greater raw f32 replaces k-th.
            if doc.score.0.partial_cmp(&kth_score) != Some(std::cmp::Ordering::Greater) {
                return Ok(false);
            }
            let Some(Reverse((_, _, _, frequency_slot))) = self.heap.pop() else {
                return Err(Error::internal(
                    "FTS top-k heap entry disappeared during replacement",
                ));
            };
            self.frequency_slots.replace(frequency_slot, pairs)?;
            frequency_slot
        } else {
            self.frequency_slots.push(pairs)?
        };

        self.heap
            .push(Reverse((doc, doc_length, posting_doc_id, frequency_slot)));
        Ok(true)
    }

    fn kth_score_if_full(&self) -> Option<f32> {
        if self.heap.len() == self.limit {
            self.heap.peek().map(|entry| entry.0.0.score.0)
        } else {
            None
        }
    }

    fn into_candidates<C>(
        self,
        mut to_candidate: impl FnMut(u64) -> C,
    ) -> Result<Vec<DocCandidate<C>>> {
        let Self {
            heap,
            mut frequency_slots,
            ..
        } = self;
        heap.into_iter()
            .map(
                |Reverse((doc, doc_length, posting_doc_id, frequency_slot))| {
                    Ok(DocCandidate {
                        document: to_candidate(doc.row_id),
                        posting_doc_id,
                        freqs: frequency_slots.take(frequency_slot)?,
                        doc_length,
                    })
                },
            )
            .collect()
    }

    #[cfg(test)]
    fn num_frequency_slots(&self) -> usize {
        self.frequency_slots.slots.len()
    }
}
const LINEAR_BLOCK_SKIP_LIMIT: usize = 8;
pub static FLAT_SEARCH_PERCENT_THRESHOLD: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("LANCE_FLAT_SEARCH_PERCENT_THRESHOLD")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<u64>()
        .unwrap_or(10)
});
// Bulk MAXSCORE path for top-k disjunctions (Lucene MaxScoreBulkScorer
// style). Default on: with right-sized partitions it wins by a wide margin
// (Lucene-parity latency) and its results are score-identical to the classic
// WAND loop. LANCE_FTS_MAXSCORE=0 opts back into the classic loop.
static USE_MAXSCORE_SEARCH: LazyLock<bool> =
    LazyLock::new(|| std::env::var("LANCE_FTS_MAXSCORE").as_deref() != Ok("0"));
// Bulk conjunction path for top-k AND / phrase queries: block-max window
// skipping plus a slice-level merge over decompressed blocks, replacing the
// per-doc `next()` leapfrog. Results are identical to the classic AND loop.
// LANCE_FTS_BULK_AND accepts auto (default), on/1, or off/0. Auto enables the
// bulk path only for its consistently faster two- and three-clause kernels.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum BulkAndMode {
    #[default]
    Auto,
    On,
    Off,
}

impl BulkAndMode {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("auto") {
            Some(Self::Auto)
        } else if value.eq_ignore_ascii_case("on") || value == "1" {
            Some(Self::On)
        } else if value.eq_ignore_ascii_case("off") || value == "0" {
            Some(Self::Off)
        } else {
            None
        }
    }

    const fn enabled_for(self, num_clauses: usize) -> bool {
        match self {
            Self::Auto => matches!(num_clauses, 2 | 3),
            Self::On => true,
            Self::Off => false,
        }
    }
}

fn bulk_and_mode_from_env() -> BulkAndMode {
    match std::env::var("LANCE_FTS_BULK_AND") {
        Ok(value) => BulkAndMode::parse(&value).unwrap_or_else(|| {
            log::warn!(
                "Invalid LANCE_FTS_BULK_AND value {value:?}; expected auto, on/1, or off/0; \
                 falling back to auto"
            );
            BulkAndMode::Auto
        }),
        Err(std::env::VarError::NotPresent) => BulkAndMode::Auto,
        Err(std::env::VarError::NotUnicode(value)) => {
            log::warn!(
                "Invalid non-Unicode LANCE_FTS_BULK_AND value {value:?}; expected auto, on/1, \
                 or off/0; falling back to auto"
            );
            BulkAndMode::Auto
        }
    }
}

static BULK_AND_MODE: LazyLock<BulkAndMode> = LazyLock::new(bulk_and_mode_from_env);

#[cfg(target_arch = "x86_64")]
static HAS_AVX2: LazyLock<bool> = LazyLock::new(|| std::arch::is_x86_feature_detected!("avx2"));

/// First index in `[pos, end)` where `docs[index] >= target` (scalar).
/// Posting-block doc ids stay below 2^31, which the AVX2 variant relies on.
#[inline]
unsafe fn find_next_geq_scalar(docs: *const u32, mut pos: usize, end: usize, target: u32) -> usize {
    unsafe {
        while pos < end && *docs.add(pos) < target {
            pos += 1;
        }
    }
    pos
}

/// AVX2 `find_next_geq` (the analogue of Lucene's VectorUtil.findNextGEQ):
/// branchless 8-wide compare+movemask kills the mispredicted exits that
/// dominate the scalar catch-up scan on irregular doc gaps.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_next_geq_avx2(docs: *const u32, mut pos: usize, end: usize, target: u32) -> usize {
    use core::arch::x86_64::*;
    debug_assert!(target <= i32::MAX as u32);
    unsafe {
        let target_lanes = _mm256_set1_epi32(target as i32);
        while pos + 8 <= end {
            let docs_lanes = _mm256_loadu_si256(docs.add(pos) as *const __m256i);
            // lane mask of docs[i] < target; doc ids < 2^31 keep the signed
            // compare equivalent to unsigned.
            let below = _mm256_cmpgt_epi32(target_lanes, docs_lanes);
            let mask = _mm256_movemask_ps(_mm256_castsi256_ps(below)) as u32;
            if mask != 0xFF {
                return pos + mask.trailing_ones() as usize;
            }
            pos += 8;
        }
        find_next_geq_scalar(docs, pos, end, target)
    }
}

#[inline]
unsafe fn find_next_geq(docs: *const u32, pos: usize, end: usize, target: u32) -> usize {
    #[cfg(target_arch = "x86_64")]
    if *HAS_AVX2 {
        return unsafe { find_next_geq_avx2(docs, pos, end, target) };
    }
    unsafe { find_next_geq_scalar(docs, pos, end, target) }
}

#[inline]
fn conservative_bm25_upper_bound(query_weight: f32) -> f32 {
    if query_weight <= 0.0 {
        0.0
    } else {
        query_weight * (K1 + 1.0)
    }
}

#[inline]
fn scorer_upper_bound<S: Scorer + ?Sized>(query_weight: f32, scorer: &S) -> f32 {
    if query_weight.is_nan() {
        return f32::INFINITY;
    }
    if query_weight <= 0.0 {
        return 0.0;
    }
    match scorer.doc_weight_upper_bound() {
        Some(bound) if bound.is_finite() && bound >= 0.0 => query_weight * bound,
        _ => f32::INFINITY,
    }
}

/// Per-term data retained after same-position postings are unioned.
#[derive(Debug)]
pub(super) struct GroupedTermScorer {
    query_weight: f32,
    freqs_by_posting_doc_id: Vec<(u64, u32)>,
}

impl GroupedTermScorer {
    pub(super) fn new(query_weight: f32, posting: &PostingList) -> Self {
        let freqs_by_posting_doc_id = posting
            .iter()
            .map(|(posting_doc_id, freq, _)| (posting_doc_id, freq))
            .collect();
        Self {
            query_weight,
            freqs_by_posting_doc_id,
        }
    }

    pub(super) fn query_weight(&self) -> f32 {
        self.query_weight
    }

    pub(super) fn frequency(&self, posting_doc_id: u64) -> Option<u32> {
        self.freqs_by_posting_doc_id
            .binary_search_by_key(&posting_doc_id, |(doc_id, _)| *doc_id)
            .ok()
            .map(|index| self.freqs_by_posting_doc_id[index].1)
    }

    fn score<S: Scorer + ?Sized>(&self, posting_doc_id: u64, doc_length: u32, scorer: &S) -> f32 {
        self.frequency(posting_doc_id)
            .map(|freq| self.query_weight * scorer.doc_weight(freq, doc_length))
            .unwrap_or_default()
    }
}

pub struct PostingIterator {
    token: String,
    token_id: u32,
    position: u32,
    query_weight: f32,
    list: PostingList,
    // the index of current doc, this can be changed only by `next()`
    index: usize,
    // the index of current block, this can be changed by `next() and shallow_next()`
    block_idx: usize,
    current_doc: Option<DocInfo>,
    approximate_upper_bound: f32,
    // Stored block maxima may use partition-local statistics. Queries scored
    // with corpus-wide statistics must use a scorer-provided bound instead.
    use_scorer_upper_bound: bool,
    // The union posting drives iteration and pruning; these terms produce the
    // exact score for each candidate document.
    grouped_terms: Option<Arc<[GroupedTermScorer]>>,
    // Position cursors temporarily own this buffer and return it on drop. This
    // keeps repeated cursor creation allocation-free without lending a slice
    // out of the interior-mutable compressed state.
    position_scratch: RefCell<Option<Vec<u32>>>,

    // for compressed posting list
    compressed: Option<UnsafeCell<CompressedState>>,
}

#[derive(Clone)]
struct CompressedState {
    block_idx: usize,
    doc_ids: Vec<u32>,
    freqs: Vec<u32>,
    buffer: Box<[u32; MAX_POSTING_BLOCK_SIZE]>,
    position_block_idx: Option<usize>,
    position_values: Vec<u32>,
    position_offsets: Vec<usize>,
    // Seek state for PackedDelta position blocks: the lazily-built group
    // header index, the last unpacked group (memoized), the decoded varint
    // tail, and the block's total delta count. Together these let a phrase
    // check decode just the candidate doc's positions instead of the whole
    // 256-doc position block.
    position_group_offsets: Vec<usize>,
    position_unpacked_group: Box<[u32; BLOCK_SIZE]>,
    position_unpacked_group_idx: Option<usize>,
    position_tail: Vec<u32>,
    position_total_deltas: usize,
    block_max_window: BlockMaxWindow,
    // Lucene-style anchored impact score caches: one slot per level, keyed by
    // the entry the block cursor currently sits in. Each holds
    // (entry_idx, doc_up_to, max_score). See `impact_level0`/`impact_level1`.
    level0_cache: Option<(usize, u32, f32)>,
    level1_cache: Option<(usize, u32, f32)>,
}

impl CompressedState {
    fn new(block_size: usize) -> Self {
        Self {
            block_idx: 0,
            doc_ids: Vec::with_capacity(block_size),
            freqs: Vec::with_capacity(block_size),
            buffer: Box::new([0; MAX_POSTING_BLOCK_SIZE]),
            position_block_idx: None,
            position_values: Vec::new(),
            position_offsets: Vec::new(),
            position_group_offsets: Vec::new(),
            position_unpacked_group: Box::new([0; BLOCK_SIZE]),
            position_unpacked_group_idx: None,
            position_tail: Vec::new(),
            position_total_deltas: 0,
            block_max_window: BlockMaxWindow::new(),
            level0_cache: None,
            level1_cache: None,
        }
    }

    #[inline]
    fn decompress(
        &mut self,
        block: &[u8],
        block_idx: usize,
        num_blocks: usize,
        length: u32,
        tail_codec: super::PostingTailCodec,
        block_size: usize,
    ) {
        self.doc_ids.clear();
        self.freqs.clear();

        let remainder = length as usize % block_size;
        if block_idx + 1 == num_blocks && remainder != 0 {
            decompress_posting_remainder(
                block,
                remainder,
                tail_codec,
                block_size,
                &mut self.doc_ids,
                &mut self.freqs,
            );
        } else {
            decompress_posting_block(
                block,
                &mut self.buffer[..],
                &mut self.doc_ids,
                &mut self.freqs,
                block_size,
            );
        }
        self.block_idx = block_idx;
        self.position_block_idx = None;
        self.position_values.clear();
        self.position_offsets.clear();
    }
}

#[derive(Clone)]
struct BlockMaxWindow {
    // Sliding block range used for Lucene-style getMaxScore(upTo). The deque is
    // monotonic by score and covers blocks in [start_block_idx, next_block_idx).
    // Only used for compressed lists without impact skip data; impact lists
    // answer window max scores from the anchored level caches instead.
    start_block_idx: usize,
    next_block_idx: usize,
    max_scores: VecDeque<(usize, f32)>,
    impact_score_cache: ImpactScoreCache,
}

struct BlockMaxScore {
    score: f32,
    blocks_scanned: usize,
}

impl BlockMaxWindow {
    fn new() -> Self {
        Self {
            start_block_idx: 0,
            next_block_idx: 0,
            max_scores: VecDeque::new(),
            impact_score_cache: ImpactScoreCache::default(),
        }
    }

    fn reset(&mut self, start_block_idx: usize) {
        self.start_block_idx = start_block_idx;
        self.next_block_idx = start_block_idx;
        self.max_scores.clear();
    }

    fn max_score_up_to<S: Scorer + ?Sized>(
        &mut self,
        list: &CompressedPostingList,
        start_block_idx: usize,
        up_to: u64,
        query_weight: f32,
        scorer: &S,
    ) -> BlockMaxScore {
        if start_block_idx >= list.blocks.len() {
            self.reset(start_block_idx);
            return BlockMaxScore {
                score: 0.0,
                blocks_scanned: 0,
            };
        }
        if start_block_idx < self.start_block_idx || start_block_idx > self.next_block_idx {
            self.reset(start_block_idx);
        }
        self.start_block_idx = start_block_idx;
        while matches!(self.max_scores.front(), Some((block_idx, _)) if *block_idx < start_block_idx)
        {
            self.max_scores.pop_front();
        }

        if list.block_least_doc_id(start_block_idx) as u64 > up_to {
            self.reset(start_block_idx);
            return BlockMaxScore {
                score: 0.0,
                blocks_scanned: 0,
            };
        }

        // 256-document-block postings score quantized document lengths, which can be shorter than
        // the exact lengths used to bake a legacy max score. Without impacts,
        // use the score-independent BM25 ceiling instead of that stale bound.
        if list.block_size == MAX_POSTING_BLOCK_SIZE {
            self.reset(start_block_idx);
            return BlockMaxScore {
                score: scorer_upper_bound(query_weight, scorer),
                blocks_scanned: 0,
            };
        }

        self.next_block_idx = self.next_block_idx.max(start_block_idx);
        let mut blocks_scanned = 0;
        while self.next_block_idx < list.blocks.len()
            && list.block_least_doc_id(self.next_block_idx) as u64 <= up_to
        {
            let score = match list.impacts.as_ref() {
                Some(impacts) => impacts.level0_score_cached(
                    self.next_block_idx,
                    query_weight,
                    scorer,
                    &mut self.impact_score_cache,
                ),
                None => list.block_max_score(self.next_block_idx),
            };
            while matches!(self.max_scores.back(), Some((_, old_score)) if *old_score <= score) {
                self.max_scores.pop_back();
            }
            self.max_scores.push_back((self.next_block_idx, score));
            self.next_block_idx += 1;
            blocks_scanned += 1;
        }

        let score = self
            .max_scores
            .front()
            .map(|(_, score)| *score)
            .unwrap_or(0.0);
        BlockMaxScore {
            score,
            blocks_scanned,
        }
    }
}

impl Debug for PostingIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingIterator")
            .field(
                "doc",
                &self
                    .doc()
                    .map(|doc| doc.doc_id())
                    .unwrap_or(TERMINATED_DOC_ID),
            )
            .field("approximate_upper_bound", &self.approximate_upper_bound)
            .field("token_id", &self.token_id)
            .finish()
    }
}

impl PartialEq for PostingIterator {
    fn eq(&self, other: &Self) -> bool {
        self.token_id == other.token_id && self.position == other.position
    }
}

impl Eq for PostingIterator {}

impl PartialOrd for PostingIterator {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostingIterator {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.doc(), other.doc()) {
            (Some(doc1), Some(doc2)) => doc1
                .cmp(&doc2)
                .then(
                    self.approximate_upper_bound
                        .total_cmp(&other.approximate_upper_bound),
                )
                .then(self.token_id.cmp(&other.token_id))
                .then(self.position.cmp(&other.position)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => self
                .approximate_upper_bound
                .total_cmp(&other.approximate_upper_bound)
                .then(self.token_id.cmp(&other.token_id))
                .then(self.position.cmp(&other.position)),
        }
    }
}

impl PostingIterator {
    fn block_idx_for_doc(
        &self,
        list: &CompressedPostingList,
        mut block_idx: usize,
        least_id: u32,
    ) -> usize {
        let mut linear_skips = 0;
        while block_idx + 1 < list.blocks.len() && linear_skips < LINEAR_BLOCK_SKIP_LIMIT {
            if list.block_least_doc_id(block_idx + 1) > least_id {
                return block_idx;
            }
            block_idx += 1;
            linear_skips += 1;
        }

        if block_idx + 1 >= list.blocks.len() {
            return block_idx;
        }

        if let Some(impacts) = list.impacts.as_ref()
            && let Some(block_idx) =
                self.block_idx_for_doc_with_impacts(list, impacts, block_idx, least_id)
        {
            return block_idx;
        }

        self.block_idx_for_doc_by_least_doc_id(list, block_idx, least_id, list.blocks.len())
    }

    fn block_idx_for_doc_with_impacts(
        &self,
        list: &CompressedPostingList,
        impacts: &ImpactSkipData,
        mut block_idx: usize,
        least_id: u32,
    ) -> Option<usize> {
        while block_idx + 1 < list.blocks.len() {
            let group_idx = (block_idx + 1) / IMPACT_LEVEL1_BLOCKS;
            let group_end = ((group_idx + 1) * IMPACT_LEVEL1_BLOCKS).min(list.blocks.len());
            let group_doc_up_to = impacts.level1_doc_up_to(group_idx)?;
            if group_doc_up_to < least_id {
                block_idx = group_end - 1;
                continue;
            }
            if group_doc_up_to == least_id {
                return Some(group_end - 1);
            }
            return Some(
                self.block_idx_for_doc_by_least_doc_id(list, block_idx, least_id, group_end),
            );
        }
        Some(block_idx)
    }

    fn block_idx_for_doc_by_least_doc_id(
        &self,
        list: &CompressedPostingList,
        block_idx: usize,
        least_id: u32,
        right: usize,
    ) -> usize {
        let mut left = block_idx + 1;
        let mut right = right;
        while left < right {
            let mid = left + (right - left) / 2;
            if list.block_least_doc_id(mid) <= least_id {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left - 1
    }

    #[inline]
    fn block_end_doc(&self) -> u64 {
        self.next_block_first_doc()
            .map(|doc| doc.saturating_sub(1))
            .unwrap_or(TERMINATED_DOC_ID)
    }

    /// Level1 bound of the group holding the current block, for group-wide
    /// skipping: (group doc_up_to, group max score). `None` when the list has
    /// no impact skip data or the group entry is missing/malformed.
    fn impact_group_bound<S: Scorer + ?Sized>(&self, scorer: &S) -> Option<(u64, f32)> {
        match self.list {
            PostingList::Compressed(ref list) => {
                let impacts = list.impacts.as_ref()?;
                let (doc_up_to, score) = self.impact_level1(impacts, scorer);
                if doc_up_to == u32::MAX {
                    return None;
                }
                Some((u64::from(doc_up_to), score))
            }
            PostingList::Plain(_) => None,
        }
    }

    #[inline]
    fn compressed_state_ptr(&self) -> *mut CompressedState {
        debug_assert!(self.compressed.is_some());
        // this method is called very frequently, so we prefer to use `UnsafeCell` instead of
        // `RefCell` to avoid the overhead of runtime borrow checking
        self.compressed.as_ref().unwrap().get()
    }

    #[inline]
    fn ensure_compressed_block_ptr(
        &self,
        list: &CompressedPostingList,
        block_idx: usize,
    ) -> *mut CompressedState {
        let compressed = unsafe { &mut *self.compressed_state_ptr() };
        if compressed.block_idx != block_idx || compressed.doc_ids.is_empty() {
            let block = list.blocks.value(block_idx);
            compressed.decompress(
                block,
                block_idx,
                list.blocks.len(),
                list.length,
                list.posting_tail_codec,
                list.block_size,
            );
        }
        compressed as *mut CompressedState
    }

    #[cfg(test)]
    pub(crate) fn new(
        token: String,
        token_id: u32,
        position: u32,
        list: PostingList,
        num_doc: usize,
    ) -> Self {
        Self::with_query_weight(token, token_id, position, 1.0, list, num_doc)
    }

    pub(crate) fn with_query_weight(
        token: String,
        token_id: u32,
        position: u32,
        query_weight: f32,
        list: PostingList,
        num_doc: usize,
    ) -> Self {
        // BM25's doc weight is bounded by K1 + 1 for any freq and doc length,
        // so query_weight * (K1 + 1) is a valid global bound even when index
        // stats drift after appends. Keeping it finite matters: an INFINITY
        // bound can never park the iterator in the WAND tail, forcing a deep
        // advance on every candidate.
        let approximate_upper_bound = match &list {
            PostingList::Compressed(posting) if posting.impacts.is_some() => f32::INFINITY,
            PostingList::Compressed(posting) if posting.block_size == MAX_POSTING_BLOCK_SIZE => {
                conservative_bm25_upper_bound(query_weight)
            }
            _ => match list.max_score() {
                Some(max_score) => max_score,
                None => idf(list.len(), num_doc) * (K1 + 1.0),
            },
        };
        let compressed = match &list {
            PostingList::Compressed(list) => {
                Some(UnsafeCell::new(CompressedState::new(list.block_size)))
            }
            PostingList::Plain(_) => None,
        };

        let mut posting = Self {
            token,
            token_id,
            position,
            query_weight,
            list,
            index: 0,
            block_idx: 0,
            current_doc: None,
            approximate_upper_bound,
            use_scorer_upper_bound: false,
            grouped_terms: None,
            position_scratch: RefCell::new(Some(Vec::new())),
            compressed,
        };
        posting.refresh_current_doc();
        posting
    }

    pub(super) fn with_scorer_upper_bound(mut self) -> Self {
        self.use_scorer_upper_bound = true;
        self
    }

    pub(super) fn with_grouped_terms(mut self, terms: Arc<[GroupedTermScorer]>) -> Self {
        self.grouped_terms = Some(terms);
        self
    }

    #[inline]
    pub(crate) fn term_index(&self) -> u32 {
        self.position
    }

    #[inline]
    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    #[inline]
    fn approximate_upper_bound(&self) -> f32 {
        self.approximate_upper_bound
    }

    /// Tightest known list-wide score bound. Impact lists answer from the
    /// baked doc-weight slab (the data-driven equivalent of the max_score the
    /// non-impact format bakes at build time); everything else falls back to
    /// `approximate_upper_bound`. A finite, tight global bound lets lagging
    /// iterators park in the WAND tail instead of being force-advanced.
    #[inline]
    fn global_upper_bound<S: Scorer + ?Sized>(&self, scorer: &S) -> f32 {
        if self.query_weight <= 0.0 {
            return 0.0;
        }
        if let PostingList::Compressed(ref list) = self.list
            && let Some(impacts) = list.impacts.as_ref()
        {
            let compressed = unsafe { &mut *self.compressed_state_ptr() };
            return self.query_weight
                * impacts.global_max_doc_weight_cached(
                    scorer,
                    &mut compressed.block_max_window.impact_score_cache,
                );
        }
        if self.use_scorer_upper_bound {
            return scorer_upper_bound(self.query_weight, scorer);
        }
        if let PostingList::Compressed(ref list) = self.list
            && list.block_size == MAX_POSTING_BLOCK_SIZE
        {
            return scorer_upper_bound(self.query_weight, scorer);
        }
        self.approximate_upper_bound
    }

    #[inline]
    fn score<S: Scorer + ?Sized>(&self, scorer: &S, freq: u32, doc_length: u32) -> f32 {
        if let (Some(grouped_terms), Some(doc)) = (&self.grouped_terms, self.current_doc) {
            return grouped_terms
                .iter()
                .map(|term| term.score(doc.doc_id(), doc_length, scorer))
                .sum();
        }
        self.query_weight * scorer.doc_weight(freq, doc_length)
    }

    #[inline]
    fn has_grouped_terms(&self) -> bool {
        self.grouped_terms.is_some()
    }

    #[inline]
    fn cost(&self) -> usize {
        self.list.len()
    }

    #[inline]
    fn empty(&self) -> bool {
        self.index >= self.list.len()
    }

    #[inline]
    fn doc(&self) -> Option<DocInfo> {
        self.current_doc
    }

    fn refresh_current_doc(&mut self) {
        if self.empty() {
            self.current_doc = None;
            return;
        }

        let current_doc = match self.list {
            PostingList::Compressed(ref list) => {
                let block_idx = self.index >> list.block_shift();
                let block_offset = self.index & list.block_mask();
                let compressed = unsafe { &mut *self.ensure_compressed_block_ptr(list, block_idx) };

                // Read from the decompressed block
                let doc_id = compressed.doc_ids[block_offset];
                let frequency = compressed.freqs[block_offset];
                let doc = DocInfo::Raw(RawDocInfo { doc_id, frequency });
                Some(doc)
            }
            PostingList::Plain(ref list) => Some(DocInfo::Located(list.doc(self.index))),
        };
        self.current_doc = current_doc;
    }

    fn position_cursor(&self) -> Result<PositionCursor<'_>> {
        match self.list {
            PostingList::Plain(ref list) => {
                let positions = list.positions.as_ref().ok_or_else(|| {
                    Error::index(format!(
                        "positions are missing for token {:?} (token id {}, query position {})",
                        self.token, self.token_id, self.position
                    ))
                })?;
                let start = positions.value_offsets()[self.index] as usize;
                let end = positions.value_offsets()[self.index + 1] as usize;
                Ok(PositionCursor::new(
                    PositionValues::Owned(
                        positions.values().as_primitive::<Int32Type>().values()[start..end]
                            .iter()
                            .map(|value| *value as u32)
                            .collect(),
                    ),
                    self.position as i32,
                ))
            }
            PostingList::Compressed(ref list) => match list.positions.as_ref().ok_or_else(|| {
                Error::index(format!(
                    "positions are missing for token {:?} (token id {}, query position {})",
                    self.token, self.token_id, self.position
                ))
            })? {
                CompressedPositionStorage::LegacyPerDoc(positions) => {
                    let positions = positions.value(self.index);
                    let positions = decompress_positions(positions.as_binary());
                    Ok(PositionCursor::new(
                        PositionValues::Owned(positions),
                        self.position as i32,
                    ))
                }
                CompressedPositionStorage::SharedStream(stream) => {
                    let block_idx = self.index >> list.block_shift();
                    let block_offset = self.index & list.block_mask();
                    let compressed =
                        unsafe { &mut *self.ensure_compressed_block_ptr(list, block_idx) };
                    match stream.codec() {
                        PositionStreamCodec::PackedDelta => {
                            // Seekable layout: decode only the candidate doc's
                            // positions. Per-block seek state resets when the
                            // block cursor moves; the group header index and
                            // varint tail fill in lazily as candidates touch
                            // them.
                            if compressed.position_block_idx != Some(block_idx) {
                                compressed.position_group_offsets.clear();
                                compressed.position_group_offsets.push(0);
                                compressed.position_tail.clear();
                                compressed.position_unpacked_group_idx = None;
                                compressed.position_offsets.clear();
                                compressed
                                    .position_offsets
                                    .reserve(compressed.freqs.len() + 1);
                                compressed.position_offsets.push(0);
                                let mut offset = 0usize;
                                for &freq in &compressed.freqs {
                                    offset += freq as usize;
                                    compressed.position_offsets.push(offset);
                                }
                                compressed.position_total_deltas = offset;
                                compressed.position_block_idx = Some(block_idx);
                            }
                            let delta_start = compressed.position_offsets[block_offset];
                            let delta_end = compressed.position_offsets[block_offset + 1];
                            let mut position_values = self
                                .position_scratch
                                .borrow_mut()
                                .take()
                                .unwrap_or_default();
                            if let Err(error) = seek_packed_doc_positions(
                                stream.block(block_idx),
                                compressed.position_total_deltas,
                                delta_start..delta_end,
                                &mut compressed.position_group_offsets,
                                &mut compressed.position_unpacked_group,
                                &mut compressed.position_unpacked_group_idx,
                                &mut compressed.position_tail,
                                &mut position_values,
                            ) {
                                *self.position_scratch.borrow_mut() = Some(position_values);
                                return Err(Error::index(format!(
                                    "failed to decode positions for token {:?} (token id {}, query position {}) at posting index {}: {error}",
                                    self.token, self.token_id, self.position, self.index
                                )));
                            }
                            Ok(PositionCursor::new(
                                PositionValues::Recycled(RecycledPositionValues::new(
                                    position_values,
                                    &self.position_scratch,
                                )),
                                self.position as i32,
                            ))
                        }
                        PositionStreamCodec::VarintDocDelta => {
                            if compressed.position_block_idx != Some(block_idx) {
                                compressed.position_values.clear();
                                decode_position_stream_block(
                                    stream.block(block_idx),
                                    compressed.freqs.as_slice(),
                                    stream.codec(),
                                    &mut compressed.position_values,
                                )
                                .map_err(|error| {
                                    Error::index(format!(
                                        "failed to decode positions for token {:?} (token id {}, query position {}) in block {block_idx}: {error}",
                                        self.token, self.token_id, self.position
                                    ))
                                })?;
                                compressed.position_offsets.clear();
                                compressed
                                    .position_offsets
                                    .reserve(compressed.freqs.len() + 1);
                                compressed.position_offsets.push(0);
                                let mut offset = 0usize;
                                for &freq in &compressed.freqs {
                                    offset += freq as usize;
                                    compressed.position_offsets.push(offset);
                                }
                                compressed.position_block_idx = Some(block_idx);
                            }
                            let start = compressed.position_offsets[block_offset];
                            let end = compressed.position_offsets[block_offset + 1];
                            let mut position_values = self
                                .position_scratch
                                .borrow_mut()
                                .take()
                                .unwrap_or_default();
                            position_values.clear();
                            position_values
                                .extend_from_slice(&compressed.position_values[start..end]);
                            Ok(PositionCursor::new(
                                PositionValues::Recycled(RecycledPositionValues::new(
                                    position_values,
                                    &self.position_scratch,
                                )),
                                self.position as i32,
                            ))
                        }
                    }
                }
            },
        }
    }

    // move to the next doc id that is greater than or equal to least_id
    fn next(&mut self, least_id: u64) {
        match self.list {
            PostingList::Compressed(ref list) => {
                debug_assert!(least_id <= u32::MAX as u64);
                let least_id = least_id as u32;
                let shift = list.block_shift();
                let block_idx = self.block_idx_for_doc(list, self.index >> shift, least_id);
                self.index = self.index.max(block_idx << shift);
                let length = list.length as usize;
                while self.index < length {
                    let block_idx = self.index >> shift;
                    let block_offset = self.index & list.block_mask();
                    let compressed =
                        unsafe { &mut *self.ensure_compressed_block_ptr(list, block_idx) };
                    let in_block = &compressed.doc_ids[block_offset..];
                    let offset_in_block = in_block.partition_point(|&doc_id| doc_id < least_id);
                    let new_offset = block_offset + offset_in_block;
                    if new_offset < compressed.doc_ids.len() {
                        self.index = (block_idx << shift) + new_offset;
                        self.block_idx = block_idx;
                        self.current_doc = Some(DocInfo::Raw(RawDocInfo {
                            doc_id: compressed.doc_ids[new_offset],
                            frequency: compressed.freqs[new_offset],
                        }));
                        return;
                    }
                    if block_idx + 1 >= list.blocks.len() {
                        self.index = length;
                        self.block_idx = self.index >> shift;
                        self.current_doc = None;
                        break;
                    }
                    self.index = (block_idx + 1) << shift;
                }
                self.block_idx = self.index >> shift;
                self.current_doc = None;
            }
            PostingList::Plain(ref list) => {
                self.index += list.row_ids[self.index..].partition_point(|&id| id < least_id);
                self.current_doc = (!self.empty()).then(|| DocInfo::Located(list.doc(self.index)));
            }
        }
    }

    fn shallow_next(&mut self, least_id: u64) {
        match self.list {
            PostingList::Compressed(ref list) => {
                debug_assert!(least_id <= u32::MAX as u64);
                let least_id = least_id as u32;
                self.block_idx = self.block_idx_for_doc(list, self.block_idx, least_id);
            }
            PostingList::Plain(_) => {
                // we don't have block max score for legacy index,
                // and no compression, so just do nothing
            }
        }
    }

    /// Anchored level0 impact bound of the current block: (doc_up_to, max_score),
    /// memoized until the block cursor moves. Malformed entries degrade to
    /// (u32::MAX, INFINITY), which keeps pruning safe by making the block look
    /// unskippable.
    #[inline]
    fn impact_level0<S: Scorer + ?Sized>(
        &self,
        impacts: &ImpactSkipData,
        scorer: &S,
    ) -> (u32, f32) {
        let compressed = unsafe { &mut *self.compressed_state_ptr() };
        if let Some((block_idx, doc_up_to, score)) = compressed.level0_cache
            && block_idx == self.block_idx
        {
            return (doc_up_to, score);
        }
        let doc_up_to = impacts.level0_doc_up_to(self.block_idx).unwrap_or(u32::MAX);
        let score = impacts.level0_score_cached(
            self.block_idx,
            self.query_weight,
            scorer,
            &mut compressed.block_max_window.impact_score_cache,
        );
        compressed.level0_cache = Some((self.block_idx, doc_up_to, score));
        (doc_up_to, score)
    }

    /// Anchored level1 impact bound of the group holding the current block,
    /// memoized until the cursor crosses a group boundary.
    #[inline]
    fn impact_level1<S: Scorer + ?Sized>(
        &self,
        impacts: &ImpactSkipData,
        scorer: &S,
    ) -> (u32, f32) {
        let group_idx = self.block_idx / IMPACT_LEVEL1_BLOCKS;
        let compressed = unsafe { &mut *self.compressed_state_ptr() };
        if let Some((cached_group_idx, doc_up_to, score)) = compressed.level1_cache
            && cached_group_idx == group_idx
        {
            return (doc_up_to, score);
        }
        let doc_up_to = impacts.level1_doc_up_to(group_idx).unwrap_or(u32::MAX);
        let score = impacts.level1_score_cached(
            group_idx,
            self.query_weight,
            scorer,
            &mut compressed.block_max_window.impact_score_cache,
        );
        compressed.level1_cache = Some((group_idx, doc_up_to, score));
        (doc_up_to, score)
    }

    #[inline]
    fn block_max_score<S: Scorer + ?Sized>(&self, scorer: &S) -> f32 {
        match self.list {
            PostingList::Compressed(ref list) => {
                if let Some(impacts) = list.impacts.as_ref() {
                    return self.impact_level0(impacts, scorer).1;
                }
                if self.use_scorer_upper_bound {
                    return scorer_upper_bound(self.query_weight, scorer);
                }
                if list.block_size == MAX_POSTING_BLOCK_SIZE {
                    return scorer_upper_bound(self.query_weight, scorer);
                }
                list.block_max_score(self.block_idx)
            }
            PostingList::Plain(_) if self.use_scorer_upper_bound => {
                scorer_upper_bound(self.query_weight, scorer)
            }
            PostingList::Plain(_) => self.approximate_upper_bound,
        }
    }

    /// Tight max-score bound over `[current block, up_to]`. The common case —
    /// a window ending inside the current block — answers from the anchored
    /// level0 memo; wider windows fall back to the sliding block-max deque,
    /// which scores each block once as it slides forward.
    #[inline]
    fn block_max_score_up_to_with_stats<S: Scorer + ?Sized>(
        &self,
        up_to: u64,
        scorer: &S,
    ) -> BlockMaxScore {
        match self.list {
            PostingList::Compressed(ref list) => {
                if let Some(impacts) = list.impacts.as_ref() {
                    let (level0_up_to, level0_score) = self.impact_level0(impacts, scorer);
                    if up_to <= u64::from(level0_up_to) {
                        return BlockMaxScore {
                            score: level0_score,
                            blocks_scanned: 0,
                        };
                    }
                }
                if self.use_scorer_upper_bound {
                    return BlockMaxScore {
                        score: scorer_upper_bound(self.query_weight, scorer),
                        blocks_scanned: 0,
                    };
                }
                let compressed = unsafe { &mut *self.compressed_state_ptr() };
                compressed.block_max_window.max_score_up_to(
                    list,
                    self.block_idx,
                    up_to,
                    self.query_weight,
                    scorer,
                )
            }
            PostingList::Plain(_) => BlockMaxScore {
                score: if self.use_scorer_upper_bound {
                    scorer_upper_bound(self.query_weight, scorer)
                } else {
                    self.approximate_upper_bound
                },
                blocks_scanned: 0,
            },
        }
    }

    fn window_max_score<S: Scorer + ?Sized>(&self, up_to: Option<u64>, scorer: &S) -> f32 {
        if let Some(up_to) = up_to
            && let PostingList::Compressed(ref list) = self.list
            && list.impacts.is_some()
        {
            return self.block_max_score_up_to_with_stats(up_to, scorer).score;
        }
        self.block_max_score(scorer)
    }

    #[inline]
    fn is_compressed(&self) -> bool {
        matches!(self.list, PostingList::Compressed(_))
    }

    #[inline]
    fn has_next_compressed_block(&self) -> bool {
        match self.list {
            PostingList::Compressed(ref list) => self.block_idx + 1 < list.blocks.len(),
            PostingList::Plain(_) => false,
        }
    }

    fn block_first_doc(&self) -> Option<u64> {
        match self.list {
            PostingList::Compressed(ref list) => {
                Some(list.block_least_doc_id(self.block_idx) as u64)
            }
            PostingList::Plain(ref plain) => plain.row_ids.get(self.index).cloned(),
        }
    }

    /// Bulk-score every posting in `[current doc, up_to]` into the window
    /// accumulator (slot = doc - window_min) and leave the iterator on the
    /// first doc beyond `up_to`. This is the Lucene `nextDocsAndScores`
    /// equivalent: it walks the decompressed block arrays directly, with no
    /// per-doc heap traffic.
    // The norm cache arg tips this hot-path fn over the limit; bundling the
    // scoring inputs isn't worth the churn here.
    #[allow(clippy::too_many_arguments)]
    fn collect_window_scores<S: Scorer + ?Sized, D: WandDocuments>(
        &mut self,
        window_min: u64,
        up_to: u64,
        clause_idx: usize,
        docs: &D,
        scorer: &S,
        norm_k: Option<(&[u8], &[f32; 256])>,
        acc: &mut WindowAccumulator,
    ) {
        if self.doc().is_some_and(|doc| doc.doc_id() < window_min) {
            self.next(window_min);
        }
        match self.list {
            PostingList::Compressed(ref list) => {
                let shift = list.block_shift();
                let mask = list.block_mask();
                'blocks: while let Some(doc) = self.current_doc {
                    if doc.doc_id() > up_to {
                        break;
                    }
                    let block_idx = self.index >> shift;
                    let block_offset = self.index & mask;
                    let compressed =
                        unsafe { &mut *self.ensure_compressed_block_ptr(list, block_idx) };
                    for offset in block_offset..compressed.doc_ids.len() {
                        let doc_id = compressed.doc_ids[offset];
                        if u64::from(doc_id) > up_to {
                            self.index = (block_idx << shift) + offset;
                            self.block_idx = block_idx;
                            self.current_doc = Some(DocInfo::Raw(RawDocInfo {
                                doc_id,
                                frequency: compressed.freqs[offset],
                            }));
                            break 'blocks;
                        }
                        let freq = compressed.freqs[offset];
                        // One byte-norm load plus a cached addend replaces
                        // recomputing the BM25 denominator per doc.
                        let doc_weight = match norm_k {
                            Some((norms, cache)) => bm25_doc_weight_with_norm(
                                freq,
                                cache[norms[doc_id as usize] as usize],
                            ),
                            None => scorer.doc_weight(freq, docs.scoring_num_tokens(doc_id)),
                        };
                        let score = self.query_weight * doc_weight;
                        let slot = (u64::from(doc_id) - window_min) as usize;
                        acc.add(clause_idx, slot, score, freq);
                    }
                    // Block exhausted: step into the next block (or finish).
                    let next_start = (block_idx + 1) << shift;
                    if next_start >= list.length as usize {
                        self.index = list.length as usize;
                        self.block_idx = self.index >> shift;
                        self.current_doc = None;
                        break;
                    }
                    self.index = next_start;
                    self.block_idx = block_idx + 1;
                    let compressed =
                        unsafe { &mut *self.ensure_compressed_block_ptr(list, block_idx + 1) };
                    self.current_doc = Some(DocInfo::Raw(RawDocInfo {
                        doc_id: compressed.doc_ids[0],
                        frequency: compressed.freqs[0],
                    }));
                }
            }
            PostingList::Plain(_) => {
                while let Some(doc) = self.doc() {
                    let doc_id = doc.doc_id();
                    if doc_id > up_to {
                        break;
                    }
                    let doc_length = docs.doc_length(&doc);
                    let score = self.score(scorer, doc.frequency(), doc_length);
                    let slot = (doc_id - window_min) as usize;
                    acc.add(clause_idx, slot, score, doc.frequency());
                    self.next(doc_id + 1);
                }
            }
        }
    }

    #[inline]
    fn next_block_first_doc(&self) -> Option<u64> {
        match self.list {
            PostingList::Compressed(ref list) => {
                if self.block_idx + 1 >= list.blocks.len() {
                    return None;
                }
                Some(list.block_least_doc_id(self.block_idx + 1) as u64)
            }
            PostingList::Plain(ref plain) => plain.row_ids.get(self.index + 1).cloned(),
        }
    }

    #[cfg(test)]
    fn validate_modern_doc_ids(&self, num_docs: usize) -> Result<()> {
        validate_modern_posting_doc_ids(&self.list, &self.token, num_docs)
    }
}

/// Validate the dense-DocId boundary before any document-length lookup.
/// Modern postings are sorted, so decoding only the final physical block is
/// sufficient to validate their maximum DocId.
pub(super) fn validate_modern_posting_doc_ids(
    posting: &PostingList,
    token: &str,
    num_docs: usize,
) -> Result<()> {
    let PostingList::Compressed(list) = posting else {
        return Err(Error::index(format!(
            "modern FTS posting for token {token:?} uses a legacy row-address layout"
        )));
    };
    if list.length == 0 {
        return Ok(());
    }
    let last_block_idx = list.blocks.len().checked_sub(1).ok_or_else(|| {
        Error::index(format!(
            "modern FTS posting for token {token:?} has length {} but no blocks",
            list.length
        ))
    })?;
    let mut state = CompressedState::new(list.block_size);
    state.decompress(
        list.blocks.value(last_block_idx),
        last_block_idx,
        list.blocks.len(),
        list.length,
        list.posting_tail_codec,
        list.block_size,
    );
    let max_doc_id = state.doc_ids.last().copied().ok_or_else(|| {
        Error::index(format!(
            "modern FTS posting for token {token:?} has an empty final block"
        ))
    })?;
    if max_doc_id as usize >= num_docs {
        return Err(Error::index(format!(
            "modern FTS posting for token {token:?} contains DocId {max_doc_id}, outside [0, {num_docs})"
        )));
    }
    Ok(())
}

/// Inner window span (in doc ids) of the bulk MAXSCORE path. Same as Lucene's
/// `MaxScoreBulkScorer.INNER_WINDOW_SIZE`.
const MAXSCORE_INNER_WINDOW: usize = 1 << 12;

/// Lucene's `MathUtil.sumUpperBound` factor, adapted to Lance's `f32` score
/// accumulation. Prefix bounds are summed in `f64`, then widened enough to
/// cover any recursive `f32` summation order of the same non-negative values.
#[inline]
fn score_sum_upper_bound_factor(num_values: usize) -> f64 {
    if num_values <= 2 {
        1.0
    } else {
        let relative_error_bound = (num_values - 1) as f64 * f64::from(f32::EPSILON);
        1.0 + 2.0 * relative_error_bound
    }
}

#[inline]
fn score_sum_cannot_exceed(
    partial_score: f32,
    remaining_upper_bound: f64,
    threshold: f32,
    upper_bound_factor: f64,
) -> bool {
    ((f64::from(partial_score) + remaining_upper_bound) * upper_bound_factor) as f32 <= threshold
}

/// Per-window score/frequency accumulator for the bulk MAXSCORE path. Slot i
/// covers doc id `window_min + i`; `freqs` is laid out clause-major so accept
/// paths can recover (term, freq) pairs of the essential clauses.
struct WindowAccumulator {
    scores: Vec<f32>,
    freqs: Vec<u32>,
    words: Vec<u64>,
    num_clauses: usize,
}

impl WindowAccumulator {
    fn new(num_clauses: usize) -> Self {
        Self {
            scores: vec![0.0; MAXSCORE_INNER_WINDOW],
            freqs: vec![0; num_clauses * MAXSCORE_INNER_WINDOW],
            words: vec![0; MAXSCORE_INNER_WINDOW / 64],
            num_clauses,
        }
    }

    #[inline]
    fn add(&mut self, clause_idx: usize, slot: usize, score: f32, freq: u32) {
        self.scores[slot] += score;
        // Doc-major layout: one slot's clause frequencies share a cache line.
        self.freqs[slot * self.num_clauses + clause_idx] = freq;
        self.words[slot >> 6] |= 1u64 << (slot & 63);
    }

    #[inline]
    fn clause_freq(&self, clause_idx: usize, slot: usize) -> u32 {
        self.freqs[slot * self.num_clauses + clause_idx]
    }

    #[inline]
    fn clear_slot(&mut self, slot: usize) {
        self.scores[slot] = 0.0;
        self.freqs[slot * self.num_clauses..(slot + 1) * self.num_clauses].fill(0);
    }
}

#[derive(Debug)]
pub struct DocCandidate<C> {
    pub document: C,
    /// The document key used by the posting lists: doc_id for compressed
    /// postings, row_id for legacy plain postings.
    pub posting_doc_id: u64,
    /// (term_index, freq)
    pub freqs: Vec<(u32, u32)>,
    pub doc_length: u32,
}

/// Document-side contract consumed by WAND.  Implementations fix candidate
/// identity and visibility before the CPU executor starts.
type FlatDocuments<'a> = (usize, Box<dyn Iterator<Item = (u64, u64)> + 'a>);

pub(super) trait WandDocuments {
    type Candidate: Copy + Debug;

    fn len(&self) -> usize;
    fn scoring_norms(&self) -> Option<&[u8]>;
    fn scoring_num_tokens(&self, doc_id: u32) -> u32;
    fn doc_length(&self, doc: &DocInfo) -> u32;
    fn document_key(&self, doc: &DocInfo) -> Option<u64>;
    fn document_key_for_doc_id(&self, doc_id: u32) -> Option<u64>;
    fn candidate_from_key(&self, key: u64) -> Self::Candidate;
    fn flat_documents(&self) -> Option<FlatDocuments<'_>>;
    fn flat_doc_length(&self, doc_id: u64, document_key: u64, compressed: bool) -> u32;
}

pub(super) trait ModernVisibility {
    fn selected(&self, doc_id: DocId) -> bool;
    fn len(&self, total_docs: usize) -> usize;
    fn iter(&self) -> Option<Box<dyn Iterator<Item = DocId> + '_>>;
}

pub(super) struct AllModernDocuments;

impl ModernVisibility for AllModernDocuments {
    #[inline]
    fn selected(&self, _doc_id: DocId) -> bool {
        true
    }

    fn len(&self, total_docs: usize) -> usize {
        total_docs
    }

    fn iter(&self) -> Option<Box<dyn Iterator<Item = DocId> + '_>> {
        None
    }
}

impl ModernVisibility for &DocVisibility {
    #[inline]
    fn selected(&self, doc_id: DocId) -> bool {
        DocVisibility::selected(self, doc_id)
    }

    fn len(&self, total_docs: usize) -> usize {
        DocVisibility::len(self, total_docs)
    }

    fn iter(&self) -> Option<Box<dyn Iterator<Item = DocId> + '_>> {
        DocVisibility::iter(self)
            .map(|doc_ids| Box::new(doc_ids) as Box<dyn Iterator<Item = DocId>>)
    }
}

pub(super) struct ModernWandDocuments<'a, V> {
    lengths: &'a DocLengths,
    visibility: V,
}

impl<'a> ModernWandDocuments<'a, AllModernDocuments> {
    pub(crate) fn all(lengths: &'a DocLengths) -> Self {
        Self {
            lengths,
            visibility: AllModernDocuments,
        }
    }
}

impl<'a> ModernWandDocuments<'a, &'a DocVisibility> {
    pub(crate) fn filtered(lengths: &'a DocLengths, visibility: &'a DocVisibility) -> Self {
        Self {
            lengths,
            visibility,
        }
    }
}

impl<V: ModernVisibility> WandDocuments for ModernWandDocuments<'_, V> {
    type Candidate = DocId;

    fn len(&self) -> usize {
        self.lengths.len()
    }

    fn scoring_norms(&self) -> Option<&[u8]> {
        self.lengths.scoring_norms()
    }

    fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        self.lengths.scoring(DocId::new(doc_id))
    }

    fn doc_length(&self, doc: &DocInfo) -> u32 {
        match doc {
            DocInfo::Raw(doc) => self.scoring_num_tokens(doc.doc_id),
            DocInfo::Located(_) => unreachable!("modern posting lists contain dense DocIds"),
        }
    }

    fn document_key(&self, doc: &DocInfo) -> Option<u64> {
        match doc {
            DocInfo::Raw(doc) if self.visibility.selected(DocId::new(doc.doc_id)) => {
                Some(u64::from(doc.doc_id))
            }
            DocInfo::Raw(_) => None,
            DocInfo::Located(_) => unreachable!("modern posting lists contain dense DocIds"),
        }
    }

    fn document_key_for_doc_id(&self, doc_id: u32) -> Option<u64> {
        self.visibility
            .selected(DocId::new(doc_id))
            .then_some(u64::from(doc_id))
    }

    fn candidate_from_key(&self, key: u64) -> Self::Candidate {
        DocId::new(key as u32)
    }

    fn flat_documents(&self) -> Option<(usize, Box<dyn Iterator<Item = (u64, u64)> + '_>)> {
        self.visibility.iter().map(|doc_ids| {
            let len = self.visibility.len(self.lengths.len());
            let docs = doc_ids.map(|doc_id| {
                let value = u64::from(doc_id.get());
                (value, value)
            });
            (len, Box::new(docs) as Box<dyn Iterator<Item = (u64, u64)>>)
        })
    }

    fn flat_doc_length(&self, doc_id: u64, _document_key: u64, _compressed: bool) -> u32 {
        self.scoring_num_tokens(doc_id as u32)
    }
}

pub(super) struct LegacyWandDocuments<'a> {
    docs: &'a DocSet,
    mask: &'a RowAddrMask,
}

impl<'a> LegacyWandDocuments<'a> {
    pub(crate) fn new(docs: &'a DocSet, mask: &'a RowAddrMask) -> Self {
        Self { docs, mask }
    }
}

impl WandDocuments for LegacyWandDocuments<'_> {
    type Candidate = u64;

    fn len(&self) -> usize {
        self.docs.len()
    }

    fn scoring_norms(&self) -> Option<&[u8]> {
        self.docs.scoring_norms()
    }

    fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        self.docs.scoring_num_tokens(doc_id)
    }

    fn doc_length(&self, doc: &DocInfo) -> u32 {
        match doc {
            DocInfo::Raw(doc) => self.docs.scoring_num_tokens(doc.doc_id),
            DocInfo::Located(doc) => self.docs.num_tokens_by_row_id(doc.row_id),
        }
    }

    fn document_key(&self, doc: &DocInfo) -> Option<u64> {
        let row_id = match doc {
            DocInfo::Raw(doc) => self.docs.row_id(doc.doc_id),
            DocInfo::Located(doc) => doc.row_id,
        };
        (row_id != RowAddress::TOMBSTONE_ROW && self.mask.selected(row_id)).then_some(row_id)
    }

    fn document_key_for_doc_id(&self, doc_id: u32) -> Option<u64> {
        let row_id = self.docs.row_id(doc_id);
        (row_id != RowAddress::TOMBSTONE_ROW && self.mask.selected(row_id)).then_some(row_id)
    }

    fn candidate_from_key(&self, key: u64) -> Self::Candidate {
        key
    }

    fn flat_documents(&self) -> Option<(usize, Box<dyn Iterator<Item = (u64, u64)> + '_>)> {
        let count = self.mask.max_len()? as usize;
        let row_ids = self.mask.iter_addrs()?;
        let docs = row_ids.flat_map(|row_addr| {
            let row_id: u64 = row_addr.into();
            self.docs
                .doc_ids(row_id)
                .map(move |doc_id| (doc_id, row_id))
        });
        Some((count, Box::new(docs)))
    }

    fn flat_doc_length(&self, doc_id: u64, document_key: u64, compressed: bool) -> u32 {
        if compressed {
            self.docs.scoring_num_tokens(doc_id as u32)
        } else {
            self.docs.num_tokens_by_row_id(document_key)
        }
    }
}

// Most unit tests exercise WAND in isolation with an in-memory complete
// DocSet.  Production code must select one of the explicit modern/legacy
// adapters above, so this convenience implementation is test-only.
#[cfg(test)]
impl WandDocuments for DocSet {
    type Candidate = u64;

    fn len(&self) -> usize {
        self.len()
    }

    fn scoring_norms(&self) -> Option<&[u8]> {
        self.scoring_norms()
    }

    fn scoring_num_tokens(&self, doc_id: u32) -> u32 {
        self.scoring_num_tokens(doc_id)
    }

    fn doc_length(&self, doc: &DocInfo) -> u32 {
        match doc {
            DocInfo::Raw(doc) => self.scoring_num_tokens(doc.doc_id),
            DocInfo::Located(doc) => self.num_tokens_by_row_id(doc.row_id),
        }
    }

    fn document_key(&self, doc: &DocInfo) -> Option<u64> {
        Some(match doc {
            DocInfo::Raw(doc) if self.has_row_ids() => self.row_id(doc.doc_id),
            DocInfo::Raw(doc) => u64::from(doc.doc_id),
            DocInfo::Located(doc) => doc.row_id,
        })
    }

    fn document_key_for_doc_id(&self, doc_id: u32) -> Option<u64> {
        Some(if self.has_row_ids() {
            self.row_id(doc_id)
        } else {
            u64::from(doc_id)
        })
    }

    fn candidate_from_key(&self, key: u64) -> Self::Candidate {
        key
    }

    fn flat_documents(&self) -> Option<(usize, Box<dyn Iterator<Item = (u64, u64)> + '_>)> {
        None
    }

    fn flat_doc_length(&self, doc_id: u64, document_key: u64, compressed: bool) -> u32 {
        if compressed {
            self.scoring_num_tokens(doc_id as u32)
        } else {
            self.num_tokens_by_row_id(document_key)
        }
    }
}

struct HeadPosting {
    // Iterators that are already positioned on or after the next candidate doc.
    // The heap is ordered by smallest doc id so the top element determines
    // the next target doc to consider.
    doc_id: u64,
    posting: Box<PostingIterator>,
}

impl HeadPosting {
    fn new(posting: Box<PostingIterator>) -> Self {
        let doc_id = posting
            .doc()
            .map(|doc| doc.doc_id())
            .unwrap_or(TERMINATED_DOC_ID);
        Self { doc_id, posting }
    }

    fn doc_id(&self) -> u64 {
        self.doc_id
    }
}

impl PartialEq for HeadPosting {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id
            && self.posting.approximate_upper_bound().to_bits()
                == other.posting.approximate_upper_bound().to_bits()
            && self.posting.token_id == other.posting.token_id
            && self.posting.position == other.posting.position
    }
}

impl Eq for HeadPosting {}

impl PartialOrd for HeadPosting {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeadPosting {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .doc_id
            .cmp(&self.doc_id)
            .then_with(|| {
                self.posting
                    .approximate_upper_bound()
                    .total_cmp(&other.posting.approximate_upper_bound())
            })
            .then_with(|| self.posting.token_id.cmp(&other.posting.token_id))
            .then_with(|| self.posting.position.cmp(&other.posting.position))
    }
}

struct TailPosting {
    // Iterators that lag behind the current target doc but may still help the
    // target beat the threshold if advanced to that doc.
    upper_bound: f32,
    // Used as a tie-breaker when upper bounds are equal. Lower-cost iterators
    // are cheaper to advance, so they are preferred.
    cost: usize,
    posting: Box<PostingIterator>,
}

impl TailPosting {
    fn new(upper_bound: f32, cost: usize, posting: Box<PostingIterator>) -> Self {
        Self {
            upper_bound,
            cost,
            posting,
        }
    }
}

impl PartialEq for TailPosting {
    fn eq(&self, other: &Self) -> bool {
        self.upper_bound.to_bits() == other.upper_bound.to_bits()
            && self.cost == other.cost
            && self.posting.token_id == other.posting.token_id
            && self.posting.position == other.posting.position
    }
}

#[derive(Default)]
struct AndWindowStats {
    windows_wide: usize,
    windows_narrow: usize,
    windows_skipped: usize,
    range_blocks_scanned: usize,
    candidates_returned: usize,
}

#[derive(Default)]
struct AndSearchStats {
    pruned_before_return_start: usize,
    candidates_seen: usize,
    full_scores: usize,
    freqs_collected: usize,
}

impl Eq for TailPosting {}

impl PartialOrd for TailPosting {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TailPosting {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.upper_bound
            .total_cmp(&other.upper_bound)
            .then_with(|| other.cost.cmp(&self.cost))
            .then_with(|| other.posting.token_id.cmp(&self.posting.token_id))
            .then_with(|| other.posting.position.cmp(&self.posting.position))
    }
}

pub struct Wand<'a, S: Scorer, D: WandDocuments> {
    threshold: f32, // multiple of factor and the minimum score of the top-k documents
    operator: Operator,
    num_terms: usize,
    // Posting iterators whose current doc id is >= the next target doc.
    // The heap top gives the smallest current doc id.
    head: BinaryHeap<HeadPosting>,
    #[allow(clippy::vec_box)]
    // Posting iterators that already match the current target doc.
    // Only these iterators participate in scoring / phrase checks for the
    // current candidate.
    lead: Vec<Box<PostingIterator>>,
    // Posting iterators that are behind the current target doc but still kept
    // in play because their score upper bound could affect the decision for the
    // current candidate.
    tail: BinaryHeap<TailPosting>,
    // Sum of upper bounds for all iterators currently held in `tail`.
    // This lets us cheaply decide whether the current candidate can still beat
    // the threshold before fully advancing every lagging iterator.
    tail_max_score: f32,
    // Block-max scores are valid for all candidate docs up to this doc id.
    // `None` means the window has not been initialized yet and the next
    // candidate must refresh block-max state before making pruning decisions.
    up_to: Option<u64>,
    // For conjunctions, this is the maximum attainable score for the current
    // block-max window `[target, up_to]`.
    and_max_score: f32,
    // Last conjunction doc returned to the caller. The next conjunction search
    // resumes strictly after this doc, like Lucene's `nextDoc()/advance()`.
    and_last_doc: Option<u64>,
    and_window_stats: AndWindowStats,
    and_candidates_pruned_before_return: usize,
    // Test-only override for comparing bulk and classic conjunctions without
    // mutating the process-wide environment.
    bulk_and_mode_override: Option<BulkAndMode>,
    #[cfg(test)]
    bulk_and_searches: usize,
    documents: &'a D,
    scorer: S,
    // Shared cross-partition top-k floor. Each partition publishes its local
    // k-th score (`atomic_store_max_f32`) and prunes against the running value
    // -- a lower bound on the global k-th, so it never drops a real top-k doc.
    shared_threshold: Option<Arc<AtomicU32>>,
}

/// Monotonically raise an f32 stored in an `AtomicU32` to `val`. CAS loop (not a
/// bit-max) so it stays correct for negative scores -- BM25 idf can go negative.
fn atomic_store_max_f32(slot: &AtomicU32, val: f32) {
    let mut cur = slot.load(Ordering::Relaxed);
    while val > f32::from_bits(cur) {
        match slot.compare_exchange_weak(cur, val.to_bits(), Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

// we were using row id as doc id in the past, which is u64,
// but now we are using the index as doc id, which is u32.
// so here WAND is a generic struct that can be used for both u32 and u64 doc ids.
impl<'a, S: Scorer, D: WandDocuments> Wand<'a, S, D> {
    pub(crate) fn new(
        operator: Operator,
        postings: impl Iterator<Item = PostingIterator>,
        documents: &'a D,
        scorer: S,
    ) -> Self {
        let mut head = BinaryHeap::new();
        let mut lead = Vec::new();
        for posting in postings {
            if posting.doc().is_none() {
                continue;
            }
            let posting = Box::new(posting);
            if operator == Operator::And {
                lead.push(posting);
            } else {
                head.push(HeadPosting::new(posting));
            }
        }
        if operator == Operator::And {
            lead.sort_unstable_by_key(|posting| posting.cost());
        }

        Self {
            threshold: 0.0,
            operator,
            num_terms: if operator == Operator::And {
                lead.len()
            } else {
                head.len()
            },
            head,
            lead,
            tail: BinaryHeap::new(),
            tail_max_score: 0.0,
            up_to: None,
            and_max_score: f32::INFINITY,
            and_last_doc: None,
            and_window_stats: AndWindowStats::default(),
            and_candidates_pruned_before_return: 0,
            bulk_and_mode_override: None,
            #[cfg(test)]
            bulk_and_searches: 0,
            documents,
            scorer,
            shared_threshold: None,
        }
    }

    /// Test hook: force one conjunction mode so parity tests can compare bulk
    /// and classic search within one process.
    #[cfg(test)]
    fn with_bulk_and_mode(mut self, mode: BulkAndMode) -> Self {
        self.bulk_and_mode_override = Some(mode);
        self
    }

    /// Share one cross-partition top-k floor across a query's partitions.
    pub(crate) fn with_shared_threshold(mut self, shared: Arc<AtomicU32>) -> Self {
        self.shared_threshold = Some(shared);
        self
    }

    /// Per-search norm→BM25-denominator cache (Lucene's norm cache): the doc
    /// byte-norm slab plus the 256 possible denominator addends. Available
    /// when the DocSet scores quantized (256-document-block partitions) and the scorer
    /// factors `doc_weight` as `(K1+1)*freq/(freq + addend)`. Scoring through
    /// the cache is bit-identical to `scorer.doc_weight`, because both
    /// evaluate the same expressions on the same quantized lengths.
    fn norm_k_cache(&self) -> Option<(&'a [u8], Box<[f32; 256]>)> {
        let norms = self.documents.scoring_norms()?;
        let mut cache = Box::new([0f32; 256]);
        for (code, slot) in cache.iter_mut().enumerate() {
            *slot = self.scorer.doc_norm(dequantize_doc_length(code as u8))?;
        }
        Some((norms, cache))
    }

    /// Set the pruning threshold from this partition's k-th best, raised to the
    /// shared cross-partition floor when one is attached.
    fn update_threshold(&mut self, local_kth: f32, wand_factor: f32) {
        let mut t = local_kth * wand_factor;
        if let Some(shared) = self.shared_threshold.as_ref() {
            atomic_store_max_f32(shared, local_kth);
            let g = f32::from_bits(shared.load(Ordering::Relaxed)) * wand_factor;
            if g > t {
                t = g;
            }
        }
        self.threshold = t;
    }

    /// Raise the local threshold to the shared cross-partition floor, picking up
    /// updates published by sibling partitions.
    fn raise_to_shared_floor(&mut self, wand_factor: f32) {
        if let Some(shared) = self.shared_threshold.as_ref() {
            let g = f32::from_bits(shared.load(Ordering::Relaxed)) * wand_factor;
            if g > self.threshold {
                self.threshold = g;
            }
        }
    }

    // search the top-k documents that contain the query
    // returns the row_id, frequency and doc length
    pub(crate) fn search(
        &mut self,
        params: &FtsSearchParams,
        metrics: &dyn MetricsCollector,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        let limit = params.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(vec![]);
        }

        if self.operator == Operator::Or
            && let Some((num_docs_selected, documents)) = self.documents.flat_documents()
            && num_docs_selected.saturating_mul(100)
                <= (*FLAT_SEARCH_PERCENT_THRESHOLD as usize).saturating_mul(self.documents.len())
        {
            return self.flat_search(params, documents, metrics);
        }

        // Top-k disjunctions over compressed lists can opt into the bulk
        // MAXSCORE path (Lucene MaxScoreBulkScorer style): it streams whole
        // blocks of the essential clauses into a window accumulator instead of
        // advancing doc-at-a-time through a heap.
        if *USE_MAXSCORE_SEARCH
            && self.operator == Operator::Or
            && params.phrase_slop.is_none()
            && !self.head.is_empty()
            && self.head.iter().all(|posting| {
                posting.posting.is_compressed() && !posting.posting.has_grouped_terms()
            })
        {
            return self.maxscore_search(params, metrics);
        }

        // Top-k conjunctions (AND and phrase) over compressed lists use the
        // bulk path: the same block-max window pruning, but candidates come
        // from a slice-level merge over decompressed blocks instead of per-doc
        // `next()` leapfrogging through boxed iterators.
        if self.operator == Operator::And
            && !self.lead.is_empty()
            && self
                .lead
                .iter()
                .all(|posting| posting.is_compressed() && !posting.has_grouped_terms())
            && self
                .bulk_and_mode_override
                .unwrap_or_else(|| *BULK_AND_MODE)
                .enabled_for(self.lead.len())
        {
            #[cfg(test)]
            {
                self.bulk_and_searches += 1;
            }
            return self.and_bulk_search(params, metrics);
        }

        let mut candidates = TopKCollector::new(limit, std::cmp::min(limit, BLOCK_SIZE * 10));
        let mut num_comparisons = 0;
        let mut and_search_stats = (self.operator == Operator::And).then_some(AndSearchStats {
            pruned_before_return_start: self.and_candidates_pruned_before_return,
            ..Default::default()
        });
        loop {
            self.raise_to_shared_floor(params.wand_factor);
            let Some((doc, mut score)) = self.next()? else {
                break;
            };
            num_comparisons += 1;
            if let Some(and_stats) = and_search_stats.as_mut() {
                and_stats.candidates_seen += 1;
            }

            let posting_doc_id = doc.doc_id();
            let Some(document_key) = self.documents.document_key(&doc) else {
                if self.operator == Operator::Or {
                    self.push_back_leads(doc.doc_id() + 1);
                }
                continue;
            };

            let doc_length = self.documents.doc_length(&doc);

            let score = if self.operator == Operator::Or {
                self.advance_all_tail(doc.doc_id(), Some(doc_length), Some(&mut score));
                if params.phrase_slop.is_some()
                    && !self.check_positions(params.phrase_slop.unwrap() as i32)?
                {
                    self.push_back_leads(doc.doc_id() + 1);
                    continue;
                }
                score
            } else {
                self.advance_all_tail(doc.doc_id(), None, None);
                if params.phrase_slop.is_some()
                    && !self.check_positions(params.phrase_slop.unwrap() as i32)?
                {
                    continue;
                }
                if let Some(and_stats) = and_search_stats.as_mut() {
                    and_stats.full_scores += 1;
                }
                self.score(doc_length)
            };

            if candidates.insert(
                ScoredDoc::new(document_key, score),
                doc_length,
                posting_doc_id,
                self.iter_term_freqs(),
            )? {
                if let Some(and_stats) = and_search_stats.as_mut() {
                    and_stats.freqs_collected += 1;
                }
                if let Some(kth) = candidates.kth_score_if_full() {
                    self.update_threshold(kth, params.wand_factor);
                }
            }
            if self.operator == Operator::Or {
                self.push_back_leads(doc.doc_id() + 1);
            }
        }
        if self.operator == Operator::And {
            tracing::debug!(
                and_windows_wide = self.and_window_stats.windows_wide,
                and_windows_narrow = self.and_window_stats.windows_narrow,
                and_windows_skipped = self.and_window_stats.windows_skipped,
                and_range_blocks_scanned = self.and_window_stats.range_blocks_scanned,
                and_candidates_returned = self.and_window_stats.candidates_returned,
                "fts conjunction block-max window stats"
            );
        }
        metrics.record_comparisons(num_comparisons);
        if let Some(and_stats) = and_search_stats {
            let and_candidates_pruned_before_return = self
                .and_candidates_pruned_before_return
                .saturating_sub(and_stats.pruned_before_return_start);
            metrics.record_and_candidates_seen(and_stats.candidates_seen);
            metrics.record_and_candidates_pruned_before_return(and_candidates_pruned_before_return);
            metrics.record_and_full_scores(and_stats.full_scores);
            metrics.record_freqs_collected(and_stats.freqs_collected);
        }

        candidates.into_candidates(|key| self.documents.candidate_from_key(key))
    }

    fn flat_search(
        &mut self,
        params: &FtsSearchParams,
        documents: Box<dyn Iterator<Item = (u64, u64)> + '_>,
        metrics: &dyn MetricsCollector,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        let limit = params.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(vec![]);
        }

        // Posting iterators are forward-only, so selected DocIds are sorted
        // before driving the sparse executor.
        let documents = documents.sorted_unstable().collect::<Vec<_>>();
        let is_compressed = self
            .head
            .peek()
            .map(|posting| matches!(posting.posting.list, PostingList::Compressed(_)))
            .or_else(|| {
                self.lead
                    .first()
                    .map(|posting| matches!(posting.list, PostingList::Compressed(_)))
            })
            .unwrap_or(false);

        let mut num_comparisons = 0;
        let mut candidates = TopKCollector::new(limit, 0);
        for (doc_id, document_key) in documents {
            num_comparisons += 1;
            self.move_head_before_target_to_tail(doc_id);
            self.move_head_doc_to_lead(doc_id);

            if self.lead.is_empty() && self.tail.is_empty() {
                continue;
            }

            if !self.can_target_beat_threshold(doc_id) {
                self.advance_tail_and_lead_to_head(doc_id + 1);
                continue;
            }

            self.collect_tail_matches(doc_id);

            if self.operator == Operator::And && self.lead.len() < self.num_terms {
                self.advance_lead_to_head(doc_id + 1);
                continue;
            }

            // check positions
            if params.phrase_slop.is_some()
                && !self.check_positions(params.phrase_slop.unwrap() as i32)?
            {
                self.advance_lead_to_head(doc_id + 1);
                continue;
            }

            // score the doc
            let doc_length = self
                .documents
                .flat_doc_length(doc_id, document_key, is_compressed);
            if self.operator == Operator::Or && !self.refine_or_candidate(doc_id, doc_length) {
                // `flat_search` evaluates an explicit allow-list of doc ids. Unlike the
                // regular WAND path, skipping to the next block boundary is unsafe here
                // because later doc ids from the same block may still be present in the
                // allow-list and need to be evaluated individually.
                self.advance_tail_and_lead_to_head(doc_id + 1);
                continue;
            }

            self.collect_tail_matches(doc_id);
            let score = self.score(doc_length);

            if candidates.insert(
                ScoredDoc::new(document_key, score),
                doc_length,
                doc_id,
                self.iter_term_freqs(),
            )? && let Some(kth) = candidates.kth_score_if_full()
            {
                self.update_threshold(kth, params.wand_factor);
            }

            self.advance_lead_to_head(doc_id + 1);
        }
        metrics.record_comparisons(num_comparisons);

        candidates.into_candidates(|key| self.documents.candidate_from_key(key))
    }

    /// Bulk MAXSCORE top-k disjunction, mirroring Lucene's MaxScoreBulkScorer.
    ///
    /// Per outer window (bounded by the essential clauses' block boundaries):
    /// clauses are partitioned into a non-essential prefix — sorted by window
    /// max score, as many as fit under the threshold — and the essential rest.
    /// Essential clauses stream their postings into a window accumulator in
    /// bulk; only accumulated candidates that could still beat the threshold
    /// probe the non-essential clauses. No per-doc heap maintenance happens
    /// anywhere on this path.
    fn maxscore_search(
        &mut self,
        params: &FtsSearchParams,
        metrics: &dyn MetricsCollector,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        struct MaxScoreClause {
            posting: Box<PostingIterator>,
            bound: f32,
            prefix_bound: f64,
        }

        let limit = params.limit.unwrap_or(usize::MAX);
        let mut clauses = std::mem::take(&mut self.head)
            .into_vec()
            .into_iter()
            .map(|head| MaxScoreClause {
                posting: head.posting,
                bound: 0.0,
                prefix_bound: 0.0,
            })
            .collect::<Vec<_>>();
        let total_sum_upper_bound_factor = score_sum_upper_bound_factor(clauses.len());

        let mut acc = WindowAccumulator::new(clauses.len());
        let mut candidates = TopKCollector::new(limit, std::cmp::min(limit, BLOCK_SIZE * 10));
        let norm_k = self.norm_k_cache();
        let norm_k_ref = norm_k
            .as_ref()
            .map(|(norms, cache)| (*norms, cache.as_ref()));
        let mut num_comparisons = 0usize;
        // Adaptive minimum window size (Lucene): grow windows when they yield
        // too few candidates to amortize the per-window bound computations.
        let mut min_window_size = 1u64;
        let mut num_windows = 0u64;
        let mut prev_first_essential = 0usize;

        let mut window_min = clauses
            .iter()
            .filter_map(|clause| clause.posting.doc().map(|doc| doc.doc_id()))
            .min()
            .unwrap_or(TERMINATED_DOC_ID);

        while window_min < TERMINATED_DOC_ID {
            clauses.retain(|clause| clause.posting.doc().is_some());
            if clauses.is_empty() {
                break;
            }
            self.raise_to_shared_floor(params.wand_factor);

            // Window boundary from the previous window's essential clauses
            // only: dense non-essential clauses must not fragment the window.
            let first_window_lead = prev_first_essential.min(clauses.len() - 1);
            let mut window_max = TERMINATED_DOC_ID;
            for clause in &mut clauses {
                let doc = clause
                    .posting
                    .doc()
                    .map(|doc| doc.doc_id())
                    .expect("exhausted clauses were retained out");
                clause.posting.shallow_next(doc.max(window_min));
            }
            for clause in &clauses[first_window_lead..] {
                window_max = window_max.min(clause.posting.block_end_doc());
            }
            if clauses.len() > 1 {
                // Target at least 32 candidates per clause per window on
                // average before shrinking windows back to block granularity.
                if (num_comparisons as u64) < num_windows * 32 * clauses.len() as u64 {
                    min_window_size = (min_window_size * 2).min(MAXSCORE_INNER_WINDOW as u64);
                } else {
                    min_window_size = 1;
                }
                window_max = window_max.max(window_min.saturating_add(min_window_size - 1));
            }

            for clause in &mut clauses {
                let doc = clause
                    .posting
                    .doc()
                    .map(|doc| doc.doc_id())
                    .expect("exhausted clauses were retained out");
                clause.bound = if doc > window_max {
                    0.0
                } else {
                    clause
                        .posting
                        .block_max_score_up_to_with_stats(window_max, &self.scorer)
                        .score
                };
            }
            clauses.sort_unstable_by(|a, b| a.bound.total_cmp(&b.bound));
            let mut first_essential = 0;
            let mut prefix = 0.0_f64;
            if self.threshold > 0.0 {
                for (i, clause) in clauses.iter_mut().enumerate() {
                    let next_prefix = prefix + f64::from(clause.bound);
                    let widened = (next_prefix * score_sum_upper_bound_factor(i + 1)) as f32;
                    if widened > self.threshold {
                        break;
                    }
                    prefix = next_prefix;
                    clause.prefix_bound = prefix;
                    first_essential = i + 1;
                }
            }
            prev_first_essential = first_essential;
            num_windows += 1;

            if first_essential == clauses.len() {
                // No clause combination inside this window can beat the
                // threshold: skip it wholesale.
                window_min = match window_max {
                    TERMINATED_DOC_ID => TERMINATED_DOC_ID,
                    max => max + 1,
                };
                // Single live clause: instead of re-running the window
                // machinery once per block, scan the baked per-block bounds
                // for the next block that can beat the threshold. This is the
                // slab form of Lucene's getSkipUpTo and turns dead stretches
                // of a dominant term into a tight load-mul-compare loop.
                if clauses.len() == 1 && window_min != TERMINATED_DOC_ID && self.threshold > 0.0 {
                    let posting = &clauses[0].posting;
                    if let PostingList::Compressed(ref list) = posting.list
                        && let Some(impacts) = list.impacts.as_ref()
                    {
                        let compressed = unsafe { &mut *posting.compressed_state_ptr() };
                        let bounds = impacts.level0_doc_weight_bounds_cached(
                            &self.scorer,
                            &mut compressed.block_max_window.impact_score_cache,
                        );
                        let query_weight = posting.query_weight;
                        // Position by binary search on the first-doc slab: the
                        // deep cursor lags arbitrarily far behind during long
                        // skip runs, and walking from it re-scans the same
                        // blocks on every dead window.
                        let first_docs = list.block_first_docs();
                        let mut block_idx = first_docs
                            .partition_point(|&first| u64::from(first) <= window_min)
                            .saturating_sub(1);
                        while block_idx < bounds.len()
                            && query_weight * bounds[block_idx] <= self.threshold
                        {
                            block_idx += 1;
                        }
                        window_min = if block_idx < bounds.len() {
                            window_min.max(u64::from(list.block_least_doc_id(block_idx)))
                        } else {
                            TERMINATED_DOC_ID
                        };
                    }
                }
                continue;
            }

            let total_non_essential_bound = if first_essential > 0 {
                clauses[first_essential - 1].prefix_bound
            } else {
                0.0
            };

            // Single essential clause (the common case once the threshold is
            // competitive): stream it directly against the non-essential
            // prefix, skipping the accumulator entirely.
            if first_essential + 1 == clauses.len() {
                let (non_essential, essential) = clauses.split_at_mut(first_essential);
                let posting = &mut essential[0].posting;
                if posting.doc().is_some_and(|doc| doc.doc_id() < window_min) {
                    posting.next(window_min);
                }
                let essential_term = posting.term_index();
                let essential_weight = posting.query_weight;

                macro_rules! consider_candidate {
                    ($doc:expr, $freq:expr) => {{
                        let doc = $doc;
                        let freq = $freq;
                        num_comparisons += 1;
                        // One byte-norm load + cached addend when available;
                        // the exact doc length is only needed at insert time.
                        let norm_addend =
                            norm_k_ref.map(|(norms, cache)| cache[norms[doc as usize] as usize]);
                        let score = match norm_addend {
                            Some(addend) => {
                                essential_weight * bm25_doc_weight_with_norm(freq, addend)
                            }
                            None => {
                                essential_weight
                                    * self.scorer.doc_weight(
                                        freq,
                                        self.documents.scoring_num_tokens(doc as u32),
                                    )
                            }
                        };
                        if !(self.threshold > 0.0
                            && score_sum_cannot_exceed(
                                score,
                                total_non_essential_bound,
                                self.threshold,
                                total_sum_upper_bound_factor,
                            ))
                        {
                            if let Some(document_key) =
                                self.documents.document_key_for_doc_id(doc as u32)
                            {
                                let mut total = score;
                                let mut rejected = false;
                                for i in (0..non_essential.len()).rev() {
                                    if self.threshold > 0.0
                                        && score_sum_cannot_exceed(
                                            total,
                                            non_essential[i].prefix_bound,
                                            self.threshold,
                                            total_sum_upper_bound_factor,
                                        )
                                    {
                                        rejected = true;
                                        break;
                                    }
                                    let probe = &mut non_essential[i].posting;
                                    if probe.doc().is_some_and(|d| d.doc_id() < doc) {
                                        probe.next(doc);
                                    }
                                    if let Some(d) = probe.doc()
                                        && d.doc_id() == doc
                                    {
                                        total += match norm_addend {
                                            Some(addend) => {
                                                probe.query_weight
                                                    * bm25_doc_weight_with_norm(
                                                        d.frequency(),
                                                        addend,
                                                    )
                                            }
                                            None => probe.score(
                                                &self.scorer,
                                                d.frequency(),
                                                self.documents.scoring_num_tokens(doc as u32),
                                            ),
                                        };
                                    }
                                }

                                // Match the classic path's emission rule: a
                                // candidate must beat the running threshold,
                                // which drops zero-score matches (e.g. terms
                                // with idf 0) exactly like Wand::next does.
                                if !rejected && total > self.threshold {
                                    let doc_length = self.documents.scoring_num_tokens(doc as u32);
                                    if candidates.insert(
                                        ScoredDoc::new(document_key, total),
                                        doc_length,
                                        doc,
                                        std::iter::once((essential_term, freq)).chain(
                                            non_essential.iter().filter_map(|clause| {
                                                clause.posting.doc().and_then(|d| {
                                                    (d.doc_id() == doc).then(|| {
                                                        (clause.posting.term_index(), d.frequency())
                                                    })
                                                })
                                            }),
                                        ),
                                    )? && let Some(kth) = candidates.kth_score_if_full()
                                    {
                                        self.update_threshold(kth, params.wand_factor);
                                    }
                                }
                            }
                        }
                    }};
                }

                match posting.list {
                    PostingList::Compressed(ref list) => {
                        let shift = list.block_shift();
                        let mask = list.block_mask();
                        'stream: while let Some(cur) = posting.current_doc {
                            if cur.doc_id() > window_max {
                                break;
                            }
                            let block_idx = posting.index >> shift;
                            let block_offset = posting.index & mask;
                            let compressed = unsafe {
                                &mut *posting.ensure_compressed_block_ptr(list, block_idx)
                            };
                            for offset in block_offset..compressed.doc_ids.len() {
                                let doc_id = compressed.doc_ids[offset];
                                if u64::from(doc_id) > window_max {
                                    posting.index = (block_idx << shift) + offset;
                                    posting.block_idx = block_idx;
                                    posting.current_doc = Some(DocInfo::Raw(RawDocInfo {
                                        doc_id,
                                        frequency: compressed.freqs[offset],
                                    }));
                                    break 'stream;
                                }
                                consider_candidate!(u64::from(doc_id), compressed.freqs[offset]);
                            }
                            let next_start = (block_idx + 1) << shift;
                            if next_start >= list.length as usize {
                                posting.index = list.length as usize;
                                posting.block_idx = posting.index >> shift;
                                posting.current_doc = None;
                                break;
                            }
                            posting.index = next_start;
                            posting.block_idx = block_idx + 1;
                            let compressed = unsafe {
                                &mut *posting.ensure_compressed_block_ptr(list, block_idx + 1)
                            };
                            posting.current_doc = Some(DocInfo::Raw(RawDocInfo {
                                doc_id: compressed.doc_ids[0],
                                frequency: compressed.freqs[0],
                            }));
                        }
                    }
                    PostingList::Plain(_) => {
                        while let Some(cur) = posting.doc() {
                            let doc = cur.doc_id();
                            if doc > window_max {
                                break;
                            }
                            consider_candidate!(doc, cur.frequency());
                            posting.next(doc + 1);
                        }
                    }
                }

                window_min = match window_max {
                    TERMINATED_DOC_ID => TERMINATED_DOC_ID,
                    max => max + 1,
                };
                continue;
            }

            // Stream the essential clauses through inner windows.
            let mut inner_min = window_min;
            loop {
                let mut next_essential_doc = TERMINATED_DOC_ID;
                for clause in &clauses[first_essential..] {
                    if let Some(doc) = clause.posting.doc() {
                        next_essential_doc = next_essential_doc.min(doc.doc_id());
                    }
                }
                inner_min = inner_min.max(next_essential_doc);
                if inner_min == TERMINATED_DOC_ID || inner_min > window_max {
                    break;
                }
                let inner_max =
                    window_max.min(inner_min.saturating_add(MAXSCORE_INNER_WINDOW as u64 - 1));

                for (clause_idx, clause) in clauses.iter_mut().enumerate().skip(first_essential) {
                    clause.posting.collect_window_scores(
                        inner_min,
                        inner_max,
                        clause_idx,
                        self.documents,
                        &self.scorer,
                        norm_k_ref,
                        &mut acc,
                    );
                }

                // Drain candidates in doc order, completing them with the
                // non-essential clauses ordered by descending bound.
                for word_idx in 0..acc.words.len() {
                    let mut word = acc.words[word_idx];
                    if word == 0 {
                        continue;
                    }
                    acc.words[word_idx] = 0;
                    while word != 0 {
                        let bit = word.trailing_zeros() as usize;
                        word &= word - 1;
                        let slot = (word_idx << 6) | bit;
                        let doc = inner_min + slot as u64;
                        let mut score = acc.scores[slot];
                        num_comparisons += 1;

                        if self.threshold > 0.0
                            && score_sum_cannot_exceed(
                                score,
                                total_non_essential_bound,
                                self.threshold,
                                total_sum_upper_bound_factor,
                            )
                        {
                            acc.clear_slot(slot);
                            continue;
                        }

                        let Some(document_key) = self.documents.document_key_for_doc_id(doc as u32)
                        else {
                            acc.clear_slot(slot);
                            continue;
                        };

                        // Doc length is only needed at heap-insert time; the
                        // non-essential completion scores go through the norm
                        // cache when available.
                        let norm_addend =
                            norm_k_ref.map(|(norms, cache)| cache[norms[doc as usize] as usize]);
                        let mut doc_length_cell: Option<u32> = None;
                        let mut rejected = false;
                        for i in (0..first_essential).rev() {
                            if self.threshold > 0.0
                                && score_sum_cannot_exceed(
                                    score,
                                    clauses[i].prefix_bound,
                                    self.threshold,
                                    total_sum_upper_bound_factor,
                                )
                            {
                                rejected = true;
                                break;
                            }
                            let posting = &mut clauses[i].posting;
                            if posting.doc().is_some_and(|d| d.doc_id() < doc) {
                                posting.next(doc);
                            }
                            if let Some(d) = posting.doc()
                                && d.doc_id() == doc
                            {
                                score += match norm_addend {
                                    Some(addend) => {
                                        posting.query_weight
                                            * bm25_doc_weight_with_norm(d.frequency(), addend)
                                    }
                                    None => {
                                        let doc_length =
                                            *doc_length_cell.get_or_insert_with(|| {
                                                self.documents.scoring_num_tokens(doc as u32)
                                            });
                                        posting.score(&self.scorer, d.frequency(), doc_length)
                                    }
                                };
                            }
                        }

                        if !rejected && score > self.threshold {
                            let doc_length = doc_length_cell
                                .unwrap_or_else(|| self.documents.scoring_num_tokens(doc as u32));
                            if candidates.insert(
                                ScoredDoc::new(document_key, score),
                                doc_length,
                                doc,
                                clauses.iter().enumerate().filter_map(|(i, clause)| {
                                    if i >= first_essential {
                                        let freq = acc.clause_freq(i, slot);
                                        (freq > 0).then(|| (clause.posting.term_index(), freq))
                                    } else {
                                        clause.posting.doc().and_then(|d| {
                                            (d.doc_id() == doc).then(|| {
                                                (clause.posting.term_index(), d.frequency())
                                            })
                                        })
                                    }
                                }),
                            )? && let Some(kth) = candidates.kth_score_if_full()
                            {
                                self.update_threshold(kth, params.wand_factor);
                            }
                        }
                        acc.clear_slot(slot);
                    }
                }
                if inner_max >= window_max {
                    break;
                }
                inner_min = inner_max + 1;
            }

            window_min = match window_max {
                TERMINATED_DOC_ID => TERMINATED_DOC_ID,
                max => max + 1,
            };
        }

        metrics.record_comparisons(num_comparisons);

        candidates.into_candidates(|key| self.documents.candidate_from_key(key))
    }

    // calculate the score of the current document
    fn score(&self, doc_length: u32) -> f32 {
        let mut score = 0.0;
        for posting in &self.lead {
            if let Some(doc) = posting.doc() {
                score += posting.score(&self.scorer, doc.frequency(), doc_length);
            }
        }
        score
    }

    // iterate over all the preceding terms and collect the term index and frequency
    fn iter_term_freqs(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.lead.iter().filter_map(|posting| {
            posting
                .doc()
                .map(|doc| (posting.term_index(), doc.frequency()))
        })
    }

    fn and_candidate_cannot_beat_threshold(&self, doc_length: u32) -> bool {
        if self.operator != Operator::And
            || self.threshold <= 0.0
            || self.num_terms < 2
            || self.lead.len() != self.num_terms
        {
            return false;
        }

        let Some((first, remaining)) = self.lead.split_first() else {
            return false;
        };
        let Some(doc) = first.doc() else {
            return false;
        };

        let remaining_upper_bound = remaining
            .iter()
            .map(|posting| posting.block_max_score(&self.scorer))
            .sum::<f32>();
        first.score(&self.scorer, doc.frequency(), doc_length) + remaining_upper_bound
            <= self.threshold
    }

    // find the next doc candidate
    // Find the next term-level candidate doc. The returned score is the exact
    // contribution from the current `lead` set; additional score can still come
    // from `tail` iterators that are advanced to the same doc later.
    fn next(&mut self) -> Result<Option<(DocInfo, f32)>> {
        if self.operator == Operator::And {
            let candidate = self.next_and_candidate();
            if candidate.is_some() {
                self.and_window_stats.candidates_returned += 1;
            }
            return Ok(candidate.map(|doc| (doc, 0.0)));
        }

        loop {
            let Some(target) = self.head_doc() else {
                if self.advance_tail_to_next_or_window() {
                    continue;
                }
                return Ok(None);
            };

            if self.up_to.is_none_or(|up_to| target > up_to) {
                self.update_max_scores(target);
            }
            self.move_head_doc_to_lead(target);
            if self.lead.is_empty() {
                continue;
            }

            // Block-Max WAND pruning: skip the whole window when its score upper
            // bound cannot reach the top-k threshold.
            if self.threshold > 0.0 && self.or_block_window_max() <= self.threshold {
                // On the final block `up_to` is the `u64::MAX` sentinel; step once
                // there to avoid seeking past the valid doc id range.
                let mut skip_to = match self.up_to {
                    Some(up_to) if up_to < u32::MAX as u64 => up_to + 1,
                    _ => target + 1,
                };
                // The narrow window is dead; if the whole level1 group is dead
                // too, hop over it in one advance.
                let group_skip = self.or_group_skip_to();
                if let Some(group_skip_to) = group_skip {
                    skip_to = skip_to.max(group_skip_to);
                }
                self.push_back_leads(skip_to);
                continue;
            }

            let Some(first_doc) = self.lead.first().and_then(|posting| posting.doc()) else {
                self.push_back_leads(target + 1);
                continue;
            };
            let doc_length = self.documents.doc_length(&first_doc);
            let mut lead_score = 0.0;
            if let Some(first_posting) = self.lead.first() {
                lead_score += first_posting.score(&self.scorer, first_doc.frequency(), doc_length);
            }
            for posting in self.lead.iter().skip(1) {
                if let Some(lead_doc) = posting.doc() {
                    lead_score += posting.score(&self.scorer, lead_doc.frequency(), doc_length);
                }
            }

            while lead_score <= self.threshold {
                if lead_score + self.tail_max_score <= self.threshold {
                    self.push_back_leads(first_doc.doc_id() + 1);
                    break;
                }
                if !self.advance_tail_top(target, doc_length, &mut lead_score) {
                    self.push_back_leads(first_doc.doc_id() + 1);
                    break;
                }
            }

            if !self.lead.is_empty() {
                return Ok(Some((first_doc, lead_score)));
            }
        }
    }
    fn next_and_candidate(&mut self) -> Option<DocInfo> {
        if self.lead.len() < self.num_terms {
            return None;
        }
        if let Some(last_doc) = self.and_last_doc
            && self
                .lead
                .first()
                .and_then(|posting| posting.doc())
                .map(|doc| doc.doc_id())
                == Some(last_doc)
        {
            let next_target = self.and_advance_target(last_doc + 1);
            if next_target == TERMINATED_DOC_ID {
                return None;
            }
            self.lead[0].next(next_target);
        }

        'advance_head: loop {
            let doc = self
                .lead
                .first()
                .and_then(|posting| posting.doc())?
                .doc_id();
            if self.up_to.is_none_or(|up_to| doc > up_to) {
                let next_target = self.and_advance_target(doc);
                if next_target == TERMINATED_DOC_ID {
                    return None;
                }
                if next_target != doc {
                    self.lead[0].next(next_target);
                    continue;
                }
            }

            for posting in self.lead.iter_mut().skip(1) {
                if posting.doc()?.doc_id() < doc {
                    posting.next(doc);
                }
                let next = posting.doc()?.doc_id();
                if next > doc {
                    let next_target = self.and_advance_target(next);
                    if next_target == TERMINATED_DOC_ID {
                        return None;
                    }
                    self.lead[0].next(next_target);
                    continue 'advance_head;
                }
            }

            let lead_doc = self.lead.first().and_then(|posting| posting.doc())?;
            let doc_length = self.documents.doc_length(&lead_doc);
            if self.and_candidate_cannot_beat_threshold(doc_length) {
                self.and_candidates_pruned_before_return += 1;
                let next_target = self.and_advance_target(doc.saturating_add(1));
                if next_target == TERMINATED_DOC_ID {
                    return None;
                }
                self.lead[0].next(next_target);
                continue;
            }

            self.and_last_doc = Some(doc);
            return Some(lead_doc);
        }
    }

    fn posting_block_up_to(posting: &PostingIterator, target: u64) -> u64 {
        posting
            .next_block_first_doc()
            .map(|doc| doc.saturating_sub(1))
            .unwrap_or(TERMINATED_DOC_ID)
            .max(target)
    }

    /// Bulk conjunction search. The window ends at the nearest next-block
    /// boundary across the clauses, so within a window every clause
    /// contributes exactly one decompressed block and the intersection is a
    /// plain merge over `u32` slices — the per-candidate cost drops from a
    /// full `PostingIterator::next` call per clause to a couple of loads.
    /// Window skipping, per-candidate pruning, scoring, and heap semantics
    /// mirror the classic loop exactly, so results are identical.
    fn and_bulk_search(
        &mut self,
        params: &FtsSearchParams,
        metrics: &dyn MetricsCollector,
    ) -> Result<Vec<DocCandidate<D::Candidate>>> {
        let limit = params.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(vec![]);
        }
        let num_lists = self.lead.len();
        let phrase_slop = params.phrase_slop;

        // Per-window view of one clause's current block. Raw pointers into the
        // clause's `CompressedState`; valid for the whole window because the
        // block cursor does not move within a window (position decoding writes
        // to separate fields of the same state).
        struct WindowList {
            docs: *const u32,
            freqs: *const u32,
            // Cursor and exclusive end, as offsets within the block.
            pos: usize,
            end: usize,
            // Absolute posting index of the block's first entry.
            block_start: usize,
        }

        // Merge kernels: intersect the window's per-clause slices and record
        // each match as (doc, per-clause block offsets). Hand-specialized for
        // two and three clauses so cursors and bounds stay in registers; the
        // generic kernel covers other widths. Offsets fit u8 because a block
        // holds at most `MAX_POSTING_BLOCK_SIZE` (256) entries. The macro
        // stamps a scalar and an AVX2 variant — `#[target_feature]` must
        // cover the whole kernel for the vector catch-up scans to inline.
        // Kernels prune a lead doc before any follower advance when its
        // frequency-bucketed score bound (plus the other clauses' block
        // maxes) cannot beat the threshold — the score-first ordering
        // Lucene's conjunction scorer uses, with doc length dropped from the
        // bound so no doc-length load is involved. The bound is monotone
        // (doc length only shrinks a BM25 weight, and the last bucket holds
        // the clause sup), so every skipped doc would also fail the exact
        // per-candidate prune: results are unchanged, the work never happens.
        macro_rules! merge_kernels {
            ($name2:ident, $name3:ident, $geq:ident $(, #[$feat:meta])?) => {
                $(#[$feat])?
                unsafe fn $name2(
                    wins: &[WindowList],
                    lut: &[f32; FREQ_LUT_BUCKETS],
                    others_block_max: f32,
                    threshold: f32,
                    docs_out: &mut Vec<u32>,
                    offs_out: &mut Vec<u8>,
                ) {
                    let (d0, mut p0, e0) = (wins[0].docs, wins[0].pos, wins[0].end);
                    let (d1, mut p1, e1) = (wins[1].docs, wins[1].pos, wins[1].end);
                    let f0 = wins[0].freqs;
                    let prune = threshold > f32::NEG_INFINITY;
                    unsafe {
                        while p0 < e0 {
                            let doc = *d0.add(p0);
                            if prune {
                                let freq = (*f0.add(p0) as usize).min(FREQ_LUT_BUCKETS - 1);
                                if lut[freq] + others_block_max <= threshold {
                                    p0 += 1;
                                    continue;
                                }
                            }
                            p1 = $geq(d1, p1, e1, doc);
                            if p1 >= e1 {
                                return;
                            }
                            let second = *d1.add(p1);
                            if second > doc {
                                p0 = $geq(d0, p0 + 1, e0, second);
                                continue;
                            }
                            docs_out.push(doc);
                            offs_out.push(p0 as u8);
                            offs_out.push(p1 as u8);
                            p0 += 1;
                        }
                    }
                }

                $(#[$feat])?
                unsafe fn $name3(
                    wins: &[WindowList],
                    lut: &[f32; FREQ_LUT_BUCKETS],
                    others_block_max: f32,
                    threshold: f32,
                    docs_out: &mut Vec<u32>,
                    offs_out: &mut Vec<u8>,
                ) {
                    let (d0, mut p0, e0) = (wins[0].docs, wins[0].pos, wins[0].end);
                    let (d1, mut p1, e1) = (wins[1].docs, wins[1].pos, wins[1].end);
                    let (d2, mut p2, e2) = (wins[2].docs, wins[2].pos, wins[2].end);
                    let f0 = wins[0].freqs;
                    let prune = threshold > f32::NEG_INFINITY;
                    unsafe {
                        'outer: while p0 < e0 {
                            let doc = *d0.add(p0);
                            if prune {
                                let freq = (*f0.add(p0) as usize).min(FREQ_LUT_BUCKETS - 1);
                                if lut[freq] + others_block_max <= threshold {
                                    p0 += 1;
                                    continue 'outer;
                                }
                            }
                            p1 = $geq(d1, p1, e1, doc);
                            if p1 >= e1 {
                                return;
                            }
                            let second = *d1.add(p1);
                            if second > doc {
                                p0 = $geq(d0, p0 + 1, e0, second);
                                continue 'outer;
                            }
                            p2 = $geq(d2, p2, e2, doc);
                            if p2 >= e2 {
                                return;
                            }
                            let third = *d2.add(p2);
                            if third > doc {
                                p0 = $geq(d0, p0 + 1, e0, third);
                                continue 'outer;
                            }
                            docs_out.push(doc);
                            offs_out.push(p0 as u8);
                            offs_out.push(p1 as u8);
                            offs_out.push(p2 as u8);
                            p0 += 1;
                        }
                    }
                }
            };
        }
        merge_kernels!(merge_window_2, merge_window_3, find_next_geq_scalar);
        #[cfg(target_arch = "x86_64")]
        merge_kernels!(
            merge_window_2_avx2,
            merge_window_3_avx2,
            find_next_geq_avx2,
            #[target_feature(enable = "avx2")]
        );

        #[inline]
        fn merge_window_1(wins: &[WindowList], docs_out: &mut Vec<u32>, offs_out: &mut Vec<u8>) {
            let win = &wins[0];
            for pos in win.pos..win.end {
                docs_out.push(unsafe { *win.docs.add(pos) });
                offs_out.push(pos as u8);
            }
        }

        #[inline]
        #[allow(clippy::too_many_arguments)]
        fn merge_window_n(
            wins: &[WindowList],
            lut: &[f32; FREQ_LUT_BUCKETS],
            others_block_max: f32,
            threshold: f32,
            cursors: &mut Vec<usize>,
            docs_out: &mut Vec<u32>,
            offs_out: &mut Vec<u8>,
        ) {
            let prune = threshold > f32::NEG_INFINITY;
            cursors.clear();
            cursors.extend(wins.iter().map(|win| win.pos));
            'outer: while cursors[0] < wins[0].end {
                let doc = unsafe { *wins[0].docs.add(cursors[0]) };
                if prune {
                    let freq = unsafe { *wins[0].freqs.add(cursors[0]) as usize }
                        .min(FREQ_LUT_BUCKETS - 1);
                    if lut[freq] + others_block_max <= threshold {
                        cursors[0] += 1;
                        continue 'outer;
                    }
                }
                for j in 1..wins.len() {
                    let win = &wins[j];
                    let pos = unsafe { find_next_geq(win.docs, cursors[j], win.end, doc) };
                    cursors[j] = pos;
                    if pos >= win.end {
                        return;
                    }
                    let clause_doc = unsafe { *win.docs.add(pos) };
                    if clause_doc > doc {
                        cursors[0] = unsafe {
                            find_next_geq(wins[0].docs, cursors[0] + 1, wins[0].end, clause_doc)
                        };
                        continue 'outer;
                    }
                }
                docs_out.push(doc);
                for &pos in cursors.iter() {
                    offs_out.push(pos as u8);
                }
                cursors[0] += 1;
            }
        }

        let mut candidates = TopKCollector::new(limit, std::cmp::min(limit, BLOCK_SIZE * 10));
        let mut num_comparisons: usize = 0;
        let mut stats = AndSearchStats {
            pruned_before_return_start: self.and_candidates_pruned_before_return,
            ..Default::default()
        };
        let mut wins: Vec<WindowList> = Vec::with_capacity(num_lists);
        // Per-window candidate batch. The merge kernel only records matches;
        // scoring then runs in two passes so the doc-length gather issues
        // independent loads (their cache misses overlap) instead of
        // serializing behind per-candidate branching. A window spans at most
        // one block per clause, so a batch holds at most one block's worth.
        let mut batch_docs: Vec<u32> = Vec::with_capacity(MAX_POSTING_BLOCK_SIZE);
        let mut batch_offs: Vec<u8> = Vec::with_capacity(MAX_POSTING_BLOCK_SIZE * num_lists);
        let mut batch_lens: Vec<u32> = Vec::with_capacity(MAX_POSTING_BLOCK_SIZE);
        let mut batch_norms: Vec<u8> = Vec::with_capacity(MAX_POSTING_BLOCK_SIZE);
        let mut cursor_scratch: Vec<usize> = Vec::with_capacity(num_lists);
        // Norm cache: one byte-norm load plus a cached addend replaces the
        // per-clause BM25 denominator recompute in pass B.
        let norm_k = self.norm_k_cache();
        let norm_k_ref = norm_k
            .as_ref()
            .map(|(norms, cache)| (*norms, cache.as_ref()));

        // Per-window prune LUT for the merge kernels: an upper bound of the
        // first (rarest) clause's score by clamped frequency. Lead docs whose
        // bound plus the remaining clauses' block maxes cannot beat the
        // threshold are skipped before any follower advances — the same
        // score-first ordering Lucene's conjunction scorer uses, with the
        // doc-length dropped from the bound so no doc-length load is needed.
        // The last bucket holds the frequency-independent sup, so clamping
        // stays a valid upper bound. Skips only avoid work; every emitted
        // candidate still goes through the exact per-candidate prune below.
        const FREQ_LUT_BUCKETS: usize = 64;
        let mut freq_bound_lut = [f32::INFINITY; FREQ_LUT_BUCKETS];
        for (freq, slot) in freq_bound_lut
            .iter_mut()
            .enumerate()
            .take(FREQ_LUT_BUCKETS - 1)
        {
            *slot = self.lead[0].score(&self.scorer, freq as u32, 0);
        }
        // The clamp bucket must bound every frequency it absorbs; the
        // clause-wide sup does.
        freq_bound_lut[FREQ_LUT_BUCKETS - 1] = self.lead[0].approximate_upper_bound();

        // The conjunction can only start at the max of the clauses' first docs.
        let mut target: u64 = 0;
        for posting in &self.lead {
            match posting.doc() {
                Some(doc) => target = target.max(doc.doc_id()),
                None => return Ok(vec![]),
            }
        }

        'window: loop {
            self.raise_to_shared_floor(params.wand_factor);
            if self.threshold > 0.0 {
                let advanced = self.and_advance_target(target);
                if advanced == TERMINATED_DOC_ID {
                    break;
                }
                target = advanced;
            }
            debug_assert!(target <= u32::MAX as u64);
            let target32 = target as u32;

            // Position every clause's block cursor at the block that can hold
            // `target`, and end the window at the nearest next-block boundary.
            let mut win_end = TERMINATED_DOC_ID;
            for j in 0..num_lists {
                let (block_idx, block_up_to) = {
                    let posting = &self.lead[j];
                    let PostingList::Compressed(ref list) = posting.list else {
                        unreachable!("bulk AND requires compressed postings");
                    };
                    let block_idx = posting.block_idx_for_doc(list, posting.block_idx, target32);
                    let block_up_to = if block_idx + 1 < list.blocks.len() {
                        u64::from(list.block_least_doc_id(block_idx + 1)).saturating_sub(1)
                    } else {
                        TERMINATED_DOC_ID
                    };
                    (block_idx, block_up_to.max(target))
                };
                self.lead[j].block_idx = block_idx;
                win_end = win_end.min(block_up_to);
            }
            let win_end32 = u32::try_from(win_end).unwrap_or(u32::MAX);

            // Decompress each clause's block and slice it to [target, win_end].
            wins.clear();
            let mut skip_window = false;
            let mut exhausted = false;
            for posting in &self.lead {
                let PostingList::Compressed(ref list) = posting.list else {
                    unreachable!("bulk AND requires compressed postings");
                };
                let block_idx = posting.block_idx;
                let state = unsafe { &mut *posting.ensure_compressed_block_ptr(list, block_idx) };
                let lo = state.doc_ids.partition_point(|&doc| doc < target32);
                let hi = if win_end32 == u32::MAX {
                    state.doc_ids.len()
                } else {
                    lo + state.doc_ids[lo..].partition_point(|&doc| doc <= win_end32)
                };
                if lo == hi {
                    // No docs of this clause in the window: the whole window
                    // has no conjunction match. If this was the clause's last
                    // block and it is fully behind the target, the clause is
                    // exhausted and the conjunction is done.
                    if block_idx + 1 >= list.blocks.len()
                        && state.doc_ids.last().is_none_or(|&doc| doc < target32)
                    {
                        exhausted = true;
                    }
                    skip_window = true;
                    break;
                }
                wins.push(WindowList {
                    docs: state.doc_ids.as_ptr(),
                    freqs: state.freqs.as_ptr(),
                    pos: lo,
                    end: hi,
                    block_start: block_idx << list.block_shift(),
                });
            }
            if exhausted {
                break 'window;
            }
            if !skip_window {
                // Constant within the window (block-anchored); mirrors
                // `and_candidate_cannot_beat_threshold`'s remaining-clause
                // bound of first-clause-exact + rest-block-max.
                let others_block_max: f32 = self.lead[1..]
                    .iter()
                    .map(|posting| posting.block_max_score(&self.scorer))
                    .sum();

                batch_docs.clear();
                batch_offs.clear();
                // NEG_INFINITY disables the kernel-level freq-bound prune
                // (single clause, or no threshold yet).
                let kernel_threshold = if self.threshold > 0.0 && num_lists >= 2 {
                    self.threshold
                } else {
                    f32::NEG_INFINITY
                };
                #[cfg(target_arch = "x86_64")]
                let use_avx2 = *HAS_AVX2;
                #[cfg(not(target_arch = "x86_64"))]
                let use_avx2 = false;
                match (num_lists, use_avx2) {
                    (1, _) => merge_window_1(&wins, &mut batch_docs, &mut batch_offs),
                    #[cfg(target_arch = "x86_64")]
                    (2, true) => unsafe {
                        merge_window_2_avx2(
                            &wins,
                            &freq_bound_lut,
                            others_block_max,
                            kernel_threshold,
                            &mut batch_docs,
                            &mut batch_offs,
                        )
                    },
                    #[cfg(target_arch = "x86_64")]
                    (3, true) => unsafe {
                        merge_window_3_avx2(
                            &wins,
                            &freq_bound_lut,
                            others_block_max,
                            kernel_threshold,
                            &mut batch_docs,
                            &mut batch_offs,
                        )
                    },
                    (2, _) => unsafe {
                        merge_window_2(
                            &wins,
                            &freq_bound_lut,
                            others_block_max,
                            kernel_threshold,
                            &mut batch_docs,
                            &mut batch_offs,
                        )
                    },
                    (3, _) => unsafe {
                        merge_window_3(
                            &wins,
                            &freq_bound_lut,
                            others_block_max,
                            kernel_threshold,
                            &mut batch_docs,
                            &mut batch_offs,
                        )
                    },
                    _ => merge_window_n(
                        &wins,
                        &freq_bound_lut,
                        others_block_max,
                        kernel_threshold,
                        &mut cursor_scratch,
                        &mut batch_docs,
                        &mut batch_offs,
                    ),
                }

                // Pass A: gather doc norms/lengths for the whole batch up
                // front so the loads issue back-to-back and their cache
                // misses overlap. With the norm cache, only the byte code is
                // gathered; the exact length decodes from a tiny LUT.
                batch_lens.clear();
                batch_norms.clear();
                match norm_k_ref {
                    Some((norms, _)) => {
                        for &doc in batch_docs.iter() {
                            batch_norms.push(norms[doc as usize]);
                        }
                    }
                    None => match self.documents.scoring_norms() {
                        Some(norms) => {
                            for &doc in batch_docs.iter() {
                                batch_lens.push(dequantize_doc_length(norms[doc as usize]));
                            }
                        }
                        None => {
                            for &doc in batch_docs.iter() {
                                batch_lens.push(self.documents.scoring_num_tokens(doc));
                            }
                        }
                    },
                }

                // Pass B: prune / verify / score / insert, in doc order, with
                // exactly the classic loop's semantics.
                for (index, &doc) in batch_docs.iter().enumerate() {
                    let (norm_addend, doc_length) = match norm_k_ref {
                        Some((_, cache)) => {
                            let code = batch_norms[index];
                            (Some(cache[code as usize]), dequantize_doc_length(code))
                        }
                        None => (None, batch_lens[index]),
                    };
                    let offs = &batch_offs[index * num_lists..(index + 1) * num_lists];
                    if self.threshold > 0.0 && num_lists >= 2 {
                        let first_freq = unsafe { *wins[0].freqs.add(offs[0] as usize) };
                        let first_score = match norm_addend {
                            Some(addend) => {
                                self.lead[0].query_weight
                                    * bm25_doc_weight_with_norm(first_freq, addend)
                            }
                            None => self.lead[0].score(&self.scorer, first_freq, doc_length),
                        };
                        if first_score + others_block_max <= self.threshold {
                            self.and_candidates_pruned_before_return += 1;
                            continue;
                        }
                    }
                    stats.candidates_seen += 1;
                    self.and_window_stats.candidates_returned += 1;
                    num_comparisons += 1;

                    let Some(document_key) = self.documents.document_key_for_doc_id(doc) else {
                        continue;
                    };

                    if let Some(slop) = phrase_slop {
                        // Park every clause's iterator on this doc so
                        // `position_cursor` reads the right posting entry. The
                        // window block is already decompressed; position blocks
                        // decode lazily and are cached per block.
                        for ((win, posting), &off) in
                            wins.iter().zip(self.lead.iter_mut()).zip(offs.iter())
                        {
                            posting.index = win.block_start + off as usize;
                            posting.current_doc = Some(DocInfo::Raw(RawDocInfo {
                                doc_id: doc,
                                frequency: unsafe { *win.freqs.add(off as usize) },
                            }));
                        }
                        let matched = if slop == 0 {
                            self.check_exact_positions_bulk()?
                        } else {
                            self.check_positions(slop as i32)?
                        };
                        if !matched {
                            continue;
                        }
                    }
                    stats.full_scores += 1;

                    let mut score = 0.0f32;
                    for ((win, posting), &off) in wins.iter().zip(self.lead.iter()).zip(offs.iter())
                    {
                        let freq = unsafe { *win.freqs.add(off as usize) };
                        score += match norm_addend {
                            Some(addend) => {
                                posting.query_weight * bm25_doc_weight_with_norm(freq, addend)
                            }
                            None => posting.score(&self.scorer, freq, doc_length),
                        };
                    }

                    if candidates.insert(
                        ScoredDoc::new(document_key, score),
                        doc_length,
                        u64::from(doc),
                        wins.iter().zip(self.lead.iter()).zip(offs.iter()).map(
                            |((win, posting), &off)| {
                                (posting.term_index(), unsafe {
                                    *win.freqs.add(off as usize)
                                })
                            },
                        ),
                    )? {
                        stats.freqs_collected += 1;
                        if let Some(kth) = candidates.kth_score_if_full() {
                            self.update_threshold(kth, params.wand_factor);
                        }
                    }
                }
            }

            if win_end == TERMINATED_DOC_ID {
                break;
            }
            target = win_end + 1;
        }

        tracing::debug!(
            and_windows_wide = self.and_window_stats.windows_wide,
            and_windows_narrow = self.and_window_stats.windows_narrow,
            and_windows_skipped = self.and_window_stats.windows_skipped,
            and_range_blocks_scanned = self.and_window_stats.range_blocks_scanned,
            and_candidates_returned = self.and_window_stats.candidates_returned,
            "fts conjunction block-max window stats (bulk)"
        );
        metrics.record_comparisons(num_comparisons);
        let pruned_before_return = self
            .and_candidates_pruned_before_return
            .saturating_sub(stats.pruned_before_return_start);
        metrics.record_and_candidates_seen(stats.candidates_seen);
        metrics.record_and_candidates_pruned_before_return(pruned_before_return);
        metrics.record_and_full_scores(stats.full_scores);
        metrics.record_freqs_collected(stats.freqs_collected);

        candidates.into_candidates(|key| self.documents.candidate_from_key(key))
    }

    fn and_move_to_next_block(&mut self, target: u64) {
        if self.threshold <= 0.0 {
            self.up_to = Some(target);
            self.and_max_score = f32::INFINITY;
            return;
        }

        if self.lead.is_empty() {
            self.up_to = Some(TERMINATED_DOC_ID);
            self.and_max_score = 0.0;
            return;
        }

        for posting in &mut self.lead {
            posting.shallow_next(target);
        }

        let narrow_up_to = self
            .lead
            .iter()
            .map(|posting| Self::posting_block_up_to(posting, target))
            .min()
            .unwrap_or(TERMINATED_DOC_ID);
        let narrow_max_score = self
            .lead
            .iter()
            .map(|posting| posting.block_max_score(&self.scorer))
            .sum::<f32>();

        if narrow_max_score >= self.threshold {
            self.up_to = Some(narrow_up_to);
            self.and_max_score = narrow_max_score;
            self.and_window_stats.windows_narrow += 1;
            return;
        }

        let lead_up_to = self
            .lead
            .first()
            .map(|posting| Self::posting_block_up_to(posting, target))
            .unwrap_or(TERMINATED_DOC_ID);
        let can_try_wide = lead_up_to > narrow_up_to
            && lead_up_to != TERMINATED_DOC_ID
            && self.lead.iter().all(|posting| posting.is_compressed());

        if can_try_wide {
            let mut wide_max_score = 0.0;
            let mut range_blocks_scanned = 0;
            for posting in &mut self.lead {
                let block_max = posting.block_max_score_up_to_with_stats(lead_up_to, &self.scorer);
                wide_max_score += block_max.score;
                range_blocks_scanned += block_max.blocks_scanned;
            }
            self.and_window_stats.range_blocks_scanned += range_blocks_scanned;

            if wide_max_score < self.threshold {
                self.up_to = Some(lead_up_to);
                self.and_max_score = wide_max_score;
                self.and_window_stats.windows_wide += 1;
                return;
            }
        }

        self.up_to = Some(narrow_up_to);
        self.and_max_score = narrow_max_score;
        self.and_window_stats.windows_narrow += 1;
    }

    fn and_advance_target(&mut self, mut target: u64) -> u64 {
        if self.up_to.is_none_or(|up_to| target > up_to) {
            self.and_move_to_next_block(target);
        }

        loop {
            let Some(up_to) = self.up_to else {
                return TERMINATED_DOC_ID;
            };
            if self.and_max_score >= self.threshold {
                return target;
            }
            self.and_window_stats.windows_skipped += 1;
            if up_to == TERMINATED_DOC_ID {
                return TERMINATED_DOC_ID;
            }
            target = up_to + 1;
            self.and_move_to_next_block(target);
        }
    }

    #[allow(clippy::vec_box)]
    fn head_doc(&self) -> Option<u64> {
        self.head.peek().map(HeadPosting::doc_id)
    }

    fn push_head(&mut self, posting: Box<PostingIterator>) {
        if posting.doc().is_some() {
            self.head.push(HeadPosting::new(posting));
        }
    }

    fn move_head_doc_to_lead(&mut self, target: u64) {
        while self.head_doc() == Some(target) {
            if let Some(posting) = self.head.pop() {
                self.lead.push(posting.posting);
            }
        }
    }

    // Move all head iterators that are already known to be behind `target`
    // into `tail`, possibly overflowing low-value entries back into `head`.
    fn move_head_before_target_to_tail(&mut self, target: u64) {
        if self.threshold <= 0.0 {
            while matches!(self.head_doc(), Some(doc_id) if doc_id < target) {
                if let Some(mut posting) = self.head.pop().map(|posting| posting.posting) {
                    posting.next(target);
                    self.push_head(posting);
                }
            }
            return;
        }

        while matches!(self.head_doc(), Some(doc_id) if doc_id < target) {
            if let Some(posting) = self.head.pop() {
                let upper_bound = posting.posting.global_upper_bound(&self.scorer);
                if let Some(mut evicted) =
                    self.insert_tail_with_overflow(posting.posting, upper_bound)
                {
                    evicted.next(target);
                    self.push_head(evicted);
                }
            }
        }
    }

    /// Upper bound on the score of any document in the window `[target, up_to]`
    /// for a disjunction. Sums the block-max of every overlapping iterator:
    /// `lead`, `head` (later docs still in the window, which
    /// `can_target_beat_threshold` omits), and the tail via `tail_max_score`.
    fn or_block_window_max(&self) -> f32 {
        let lead: f32 = self
            .lead
            .iter()
            .map(|posting| posting.window_max_score(self.up_to, &self.scorer))
            .sum();
        let head: f32 = self
            .head
            .iter()
            .map(|posting| posting.posting.window_max_score(self.up_to, &self.scorer))
            .sum();
        lead + head + self.tail_max_score
    }

    fn can_target_beat_threshold(&mut self, target: u64) -> bool {
        if self.up_to.is_none_or(|up_to| target > up_to) {
            self.update_max_scores(target);
        }

        let mut sum = self
            .lead
            .iter()
            .map(|posting| posting.window_max_score(self.up_to, &self.scorer))
            .sum::<f32>();
        let mut possible_matches = self.lead.len();
        for posting in &self.tail {
            if matches!(posting.posting.block_first_doc(), Some(block_doc) if block_doc <= target) {
                sum += posting.posting.window_max_score(self.up_to, &self.scorer);
                possible_matches += 1;
            }
        }

        match self.operator {
            Operator::And => possible_matches >= self.num_terms && sum > self.threshold,
            Operator::Or => sum > self.threshold,
        }
    }

    fn update_max_scores(&mut self, target: u64) {
        // Refresh the block-max window for the current target. The resulting
        // `up_to` is the furthest doc id for which this block-max view remains
        // valid. Like Lucene's WANDScorer, the boundary comes from the cheap
        // clauses only, and the refresh avoids allocating: heaps are recycled
        // through their backing vectors (shallow_next never changes the doc a
        // head entry is ordered by, so heapify restores the same shape).
        let lead_cost = self
            .lead
            .iter()
            .map(|posting| posting.cost())
            .min()
            .unwrap_or(usize::MAX);
        let mut narrow_up_to = TERMINATED_DOC_ID;
        for posting in &mut self.lead {
            posting.shallow_next(target);
            narrow_up_to = narrow_up_to.min(posting.block_end_doc());
        }

        let mut head_postings = std::mem::take(&mut self.head).into_vec();
        for posting in &mut head_postings {
            // Unlike Lucene, every head clause participates in the boundary:
            // the refresh is allocation-free and answers from the anchored
            // level caches, so frequent refreshes are cheap, while keeping the
            // window inside every clause's current block keeps all the bounds
            // at tight level0 values.
            let doc_id = posting.doc_id();
            posting.posting.shallow_next(doc_id);
            narrow_up_to = narrow_up_to.min(posting.posting.block_end_doc());
        }

        let mut tail_postings = std::mem::take(&mut self.tail).into_vec();
        for tail_posting in &mut tail_postings {
            tail_posting.posting.shallow_next(target);
        }

        if narrow_up_to == TERMINATED_DOC_ID
            && let Some(top) = tail_postings
                .iter()
                .min_by_key(|posting| posting.posting.cost())
            && top.posting.cost() <= lead_cost
        {
            narrow_up_to = narrow_up_to.min(top.posting.block_end_doc().max(target));
        }

        self.up_to = Some(narrow_up_to);
        self.head = BinaryHeap::from(head_postings);

        self.tail_max_score = 0.0;
        for tail_posting in tail_postings {
            let posting = tail_posting.posting;
            let upper_bound = match posting.block_first_doc() {
                Some(block_doc) if block_doc <= target => {
                    posting.window_max_score(self.up_to, &self.scorer)
                }
                _ => 0.0,
            };
            if let Some(mut evicted) = self.insert_tail_with_overflow(posting, upper_bound) {
                evicted.next(target);
                self.push_head(evicted);
            }
        }
    }

    /// After the narrow window proved skippable, try widening the skip to the
    /// level1 group boundary, in the spirit of Lucene's `getSkipUpTo`. All
    /// bounds come from the anchored level caches, so a failed attempt costs a
    /// few loads and float adds — unlike an eager wide-window probe.
    ///
    /// The group boundary is the minimum current-group end over every live
    /// iterator, so each iterator's level1 score is a valid bound over
    /// `[target, group_up_to]`.
    fn or_group_skip_to(&self) -> Option<u64> {
        let mut group_up_to = TERMINATED_DOC_ID;
        for posting in &self.lead {
            let (doc_up_to, _) = posting.impact_group_bound(&self.scorer)?;
            group_up_to = group_up_to.min(doc_up_to);
        }
        for posting in self.head.iter() {
            let (doc_up_to, _) = posting.posting.impact_group_bound(&self.scorer)?;
            group_up_to = group_up_to.min(doc_up_to);
        }
        for tail_posting in self.tail.iter() {
            let (doc_up_to, _) = tail_posting.posting.impact_group_bound(&self.scorer)?;
            group_up_to = group_up_to.min(doc_up_to);
        }
        if self.up_to.is_some_and(|up_to| group_up_to <= up_to) {
            // No gain over the narrow window skip.
            return None;
        }

        // Second pass over the memoized bounds: sum only the iterators that
        // can produce a doc inside the skipped range.
        let mut bounds_sum = 0.0_f32;
        for posting in &self.lead {
            let (_, score) = posting.impact_group_bound(&self.scorer)?;
            bounds_sum += score;
        }
        for posting in self.head.iter() {
            if posting.doc_id() > group_up_to {
                continue;
            }
            let (_, score) = posting.posting.impact_group_bound(&self.scorer)?;
            bounds_sum += score;
        }
        for tail_posting in self.tail.iter() {
            if !matches!(
                tail_posting.posting.block_first_doc(),
                Some(block_doc) if block_doc <= group_up_to
            ) {
                continue;
            }
            let (_, score) = tail_posting.posting.impact_group_bound(&self.scorer)?;
            bounds_sum += score;
        }
        (bounds_sum <= self.threshold).then_some(group_up_to.saturating_add(1))
    }

    fn refine_or_candidate(&mut self, target: u64, doc_length: u32) -> bool {
        if self.threshold <= 0.0 {
            return true;
        }

        let mut lead_score = self
            .lead
            .iter()
            .filter_map(|posting| {
                posting
                    .doc()
                    .map(|doc| posting.score(&self.scorer, doc.frequency(), doc_length))
            })
            .sum::<f32>();

        while lead_score <= self.threshold {
            if lead_score + self.tail_max_score <= self.threshold {
                return false;
            }
            if !self.advance_tail_top(target, doc_length, &mut lead_score) {
                return false;
            }
        }

        true
    }

    fn collect_tail_matches(&mut self, target: u64) {
        let mut remaining = Vec::with_capacity(self.tail.len());
        let tail = std::mem::take(&mut self.tail);
        self.tail_max_score = 0.0;
        for tail_posting in tail.into_vec() {
            let mut posting = tail_posting.posting;
            posting.next(target);
            match posting.doc().map(|doc| doc.doc_id()) {
                Some(doc_id) if doc_id == target => self.lead.push(posting),
                Some(_) => remaining.push(posting),
                None => {}
            }
        }

        for posting in remaining {
            self.push_head(posting);
        }
    }

    fn advance_tail_and_lead_to_head(&mut self, least_id: u64) {
        let mut postings = Vec::with_capacity(self.tail.len() + self.lead.len());
        while let Some(tail) = self.tail.pop() {
            postings.push(tail.posting);
        }
        self.tail_max_score = 0.0;
        postings.append(&mut self.lead);
        for mut posting in postings {
            posting.next(least_id);
            self.push_head(posting);
        }
    }

    fn advance_lead_to_head(&mut self, least_id: u64) {
        let lead = std::mem::take(&mut self.lead);
        for mut posting in lead {
            posting.next(least_id);
            self.push_head(posting);
        }
        // In the flat-search path this is only called after `collect_tail_matches`,
        // which drains the current tail into either `lead` or `head`. At this
        // point `tail` is expected to be empty, so clearing it is a no-op that
        // just resets the cached `tail_max_score`.
        debug_assert!(self.tail.is_empty());
        self.clear_tail();
    }

    fn clear_tail(&mut self) {
        self.tail.clear();
        self.tail_max_score = 0.0;
    }

    fn insert_tail(&mut self, posting: Box<PostingIterator>, upper_bound: f32) {
        self.tail_max_score += upper_bound;
        self.tail
            .push(TailPosting::new(upper_bound, posting.cost(), posting));
    }

    fn insert_tail_with_overflow(
        &mut self,
        posting: Box<PostingIterator>,
        upper_bound: f32,
    ) -> Option<Box<PostingIterator>> {
        // Keep only the lagging iterators that are most useful for deciding the
        // current candidate. If a stronger tail entry arrives, evict the weakest
        // one back to the caller so it can be advanced into `head`.
        if self.threshold <= 0.0 || upper_bound <= 0.0 {
            return Some(posting);
        }

        if self.tail_max_score + upper_bound < self.threshold {
            self.insert_tail(posting, upper_bound);
            return None;
        }

        if self.tail.is_empty() {
            return Some(posting);
        }

        let candidate = TailPosting::new(upper_bound, posting.cost(), posting);
        if let Some(top) = self.tail.peek()
            && top > &candidate
        {
            let evicted = self.tail.pop().expect("peeked tail posting should exist");
            self.tail_max_score = self.tail_max_score - evicted.upper_bound + upper_bound;
            self.tail.push(candidate);
            return Some(evicted.posting);
        }

        Some(candidate.posting)
    }

    fn lead_to_tail_upper_bound(&self, posting: &PostingIterator, target: u64) -> f32 {
        if self.operator == Operator::Or
            && posting.is_compressed()
            && self.up_to.is_some_and(|up_to| target <= up_to)
        {
            posting.window_max_score(self.up_to, &self.scorer)
        } else {
            posting.global_upper_bound(&self.scorer)
        }
    }

    fn advance_tail_to_next_or_window(&mut self) -> bool {
        if self.operator != Operator::Or || self.tail.is_empty() {
            return false;
        }

        let Some(up_to) = self.up_to else {
            return false;
        };
        if up_to >= u32::MAX as u64 {
            return false;
        }
        if !self
            .tail
            .iter()
            .any(|tail| tail.posting.has_next_compressed_block())
        {
            return false;
        }

        // A low-scoring tail can be the only iterator left in the current
        // window. Move to the next window so a later high-scoring block is still
        // reachable instead of ending the disjunction early.
        self.update_max_scores(up_to + 1);
        true
    }

    fn push_back_leads(&mut self, target: u64) {
        // After finishing a candidate doc, convert the aligned iterators back
        // into lagging iterators. Entries that do not stay in `tail` are
        // advanced to `target` and returned to `head`.
        // pop() drains in place, keeping self.lead's capacity for reuse.
        if self.threshold <= 0.0 {
            while let Some(mut posting) = self.lead.pop() {
                posting.next(target);
                self.push_head(posting);
            }
            return;
        }

        while let Some(posting) = self.lead.pop() {
            let upper_bound = self.lead_to_tail_upper_bound(&posting, target);
            if let Some(mut evicted) = self.insert_tail_with_overflow(posting, upper_bound) {
                evicted.next(target);
                self.push_head(evicted);
            }
        }
    }

    fn advance_tail_top(&mut self, target: u64, doc_length: u32, lead_score: &mut f32) -> bool {
        // Advance the most promising lagging iterator to the current target.
        // If it lands on the target, fold its exact contribution into
        // `lead_score`; otherwise put it back into `head`.
        let Some(TailPosting {
            upper_bound,
            cost: _,
            mut posting,
        }) = self.tail.pop()
        else {
            return false;
        };
        self.tail_max_score -= upper_bound;
        posting.next(target);
        match posting.doc() {
            Some(doc) if doc.doc_id() == target => {
                *lead_score += posting.score(&self.scorer, doc.frequency(), doc_length);
                self.lead.push(posting);
            }
            Some(_) => self.push_head(posting),
            None => {}
        }
        true
    }

    fn advance_all_tail(
        &mut self,
        target: u64,
        doc_length: Option<u32>,
        mut score: Option<&mut f32>,
    ) {
        // Materialize all remaining lagging iterators for `target`. This is
        // only done once we have already decided to fully score / validate the
        // candidate.
        let tail = std::mem::take(&mut self.tail);
        self.tail_max_score = 0.0;
        for tail_posting in tail.into_vec() {
            let mut posting = tail_posting.posting;
            posting.next(target);
            match posting.doc() {
                Some(doc) if doc.doc_id() == target => {
                    if let (Some(doc_length), Some(score)) = (doc_length, score.as_deref_mut()) {
                        *score += posting.score(&self.scorer, doc.frequency(), doc_length);
                    }
                    self.lead.push(posting)
                }
                Some(_) => self.push_head(posting),
                None => {}
            }
        }
    }

    fn current_doc_postings(&self) -> Vec<&PostingIterator> {
        if !self.lead.is_empty() {
            return self.lead.iter().map(|posting| posting.as_ref()).collect();
        }

        let Some(target) = self.head_doc() else {
            return Vec::new();
        };
        self.head
            .iter()
            .filter(|posting| posting.doc_id() == target)
            .map(|posting| posting.posting.as_ref())
            .collect()
    }

    fn check_positions(&self, slop: i32) -> Result<bool> {
        if slop == 0 {
            return self.check_exact_positions();
        }

        let mut position_iters = self
            .current_doc_postings()
            .into_iter()
            .map(PostingIterator::position_cursor)
            .collect::<Result<Vec<_>>>()?;
        position_iters.sort_unstable_by_key(|iter| iter.position_in_query);

        loop {
            let mut max_relative_pos = None;
            let mut all_same = true;
            for window in position_iters.windows(2) {
                let last = window[0].relative_position();
                let next = window[1].relative_position();
                let (Some(last), Some(next)) = (last, next) else {
                    return Ok(false);
                };

                let move_to = if last > next {
                    last
                } else {
                    std::cmp::max(last + 1, next - slop)
                };
                max_relative_pos = max_relative_pos.max(Some(move_to));
                if !(last <= next && next <= last + slop) {
                    all_same = false;
                    break;
                }
            }

            if all_same {
                return Ok(true);
            }

            position_iters.iter_mut().for_each(|iter| {
                iter.advance_to_relative(max_relative_pos.unwrap());
            });
        }
    }

    /// Allocation-free exact-phrase check for the bulk conjunction path,
    /// where every clause is a parked `lead` iterator. Semantically identical
    /// to [`Self::check_exact_positions`] — some base position must align all
    /// clauses at their query offsets — without the per-candidate cursor vec
    /// and sort.
    fn check_exact_positions_bulk(&self) -> Result<bool> {
        const MAX_INLINE_CLAUSES: usize = 16;
        let num_clauses = self.lead.len();
        if num_clauses > MAX_INLINE_CLAUSES {
            return self.check_exact_positions();
        }
        // Cursors stay alive in the stack array so owned position buffers
        // (legacy per-doc storage) remain valid while we scan.
        let mut cursors: [Option<PositionCursor<'_>>; MAX_INLINE_CLAUSES] =
            std::array::from_fn(|_| None);
        let mut anchor_idx = 0usize;
        let mut anchor_len = usize::MAX;
        for (index, (slot, posting)) in cursors.iter_mut().zip(self.lead.iter()).enumerate() {
            let cursor = posting.position_cursor()?;
            if cursor.len() < anchor_len {
                anchor_len = cursor.len();
                anchor_idx = index;
            }
            *slot = Some(cursor);
        }

        let anchor = cursors[anchor_idx]
            .as_ref()
            .expect("anchor cursor was just populated");
        let anchor_offset = anchor.position_in_query as u32;
        'anchor: for &anchor_position in anchor.positions.as_slice() {
            let Some(base) = anchor_position.checked_sub(anchor_offset) else {
                continue;
            };
            for (index, slot) in cursors[..num_clauses].iter().enumerate() {
                if index == anchor_idx {
                    continue;
                }
                let cursor = slot.as_ref().expect("clause cursor was just populated");
                let Some(target) = base.checked_add(cursor.position_in_query as u32) else {
                    return Ok(false);
                };
                if cursor.positions.as_slice().binary_search(&target).is_err() {
                    continue 'anchor;
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn check_exact_positions(&self) -> Result<bool> {
        let mut position_iters = self
            .current_doc_postings()
            .into_iter()
            .map(PostingIterator::position_cursor)
            .collect::<Result<Vec<_>>>()?;
        position_iters.sort_unstable_by_key(|iter| iter.len());
        let Some(lead) = position_iters.first() else {
            return Ok(false);
        };
        let lead_position = lead.position_in_query;

        loop {
            let Some(anchor) = position_iters[0].absolute_position() else {
                return Ok(false);
            };
            let Some(base) = anchor.checked_sub(lead_position as u32) else {
                position_iters[0].advance_next();
                continue;
            };

            let mut next_lead_relative = None;
            let mut matched = true;
            for follower in position_iters.iter_mut().skip(1) {
                let Some(target) = base.checked_add(follower.position_in_query as u32) else {
                    return Ok(false);
                };
                let Some(position) = follower.advance_to_absolute(target) else {
                    return Ok(false);
                };
                if position != target {
                    next_lead_relative = Some(position as i32 - follower.position_in_query);
                    matched = false;
                    break;
                }
            }

            if matched {
                return Ok(true);
            }

            position_iters[0].advance_to_relative(next_lead_relative.unwrap());
        }
    }
}

#[derive(Debug)]
struct RecycledPositionValues<'a> {
    values: Option<Vec<u32>>,
    pool: &'a RefCell<Option<Vec<u32>>>,
}

impl<'a> RecycledPositionValues<'a> {
    fn new(values: Vec<u32>, pool: &'a RefCell<Option<Vec<u32>>>) -> Self {
        Self {
            values: Some(values),
            pool,
        }
    }

    fn as_slice(&self) -> &[u32] {
        self.values
            .as_deref()
            .expect("position values are present until drop")
    }
}

impl Drop for RecycledPositionValues<'_> {
    fn drop(&mut self) {
        let values = self
            .values
            .take()
            .expect("position values are present until drop");
        let mut pool = self.pool.borrow_mut();
        if pool.is_none() {
            *pool = Some(values);
        }
    }
}

#[derive(Debug)]
enum PositionValues<'a> {
    Recycled(RecycledPositionValues<'a>),
    Owned(Vec<u32>),
}

impl<'a> PositionValues<'a> {
    fn as_slice(&self) -> &[u32] {
        match self {
            Self::Recycled(values) => values.as_slice(),
            Self::Owned(values) => values.as_slice(),
        }
    }

    fn len(&self) -> usize {
        self.as_slice().len()
    }
}

#[derive(Debug)]
struct PositionCursor<'a> {
    positions: PositionValues<'a>,
    pub position_in_query: i32,
    index: usize,
}

impl<'a> PositionCursor<'a> {
    fn new(positions: PositionValues<'a>, position_in_query: i32) -> Self {
        Self {
            positions,
            position_in_query,
            index: 0,
        }
    }

    fn len(&self) -> usize {
        self.positions.len()
    }

    fn absolute_position(&self) -> Option<u32> {
        self.positions.as_slice().get(self.index).copied()
    }

    fn relative_position(&self) -> Option<i32> {
        self.positions
            .as_slice()
            .get(self.index)
            .map(|position| *position as i32 - self.position_in_query)
    }

    fn advance_to_relative(&mut self, least_relative_pos: i32) {
        if self.index >= self.len() {
            return;
        }
        let least_pos = least_relative_pos + self.position_in_query;
        let least_pos = least_pos.max(0) as u32;
        let values = self.positions.as_slice();
        self.index += values[self.index..].partition_point(|&pos| pos < least_pos);
    }

    fn advance_to_absolute(&mut self, least_pos: u32) -> Option<u32> {
        if self.index >= self.len() {
            return None;
        }
        let values = self.positions.as_slice();
        self.index += values[self.index..].partition_point(|&pos| pos < least_pos);
        self.absolute_position()
    }

    fn advance_next(&mut self) {
        self.index = self.index.saturating_add(1).min(self.len());
    }
}

/// Incremental posting-backed scorer used by compound FTS.
///
/// Unlike [`Wand::search`], this cursor does not own a top-k heap. It exposes
/// exact candidates and conservative block bounds to the compound collector,
/// which owns the only competitive score for the whole scorer tree.
pub(super) struct WandCursor<'a, D: WandDocuments> {
    wand: Wand<'a, Arc<MemBM25Scorer>, D>,
    phrase_slop: Option<u32>,
    wand_factor: f32,
    cost: usize,
    current_doc: Option<DocInfo>,
    current_document_key: Option<u64>,
    current_score: f32,
    confirmation: Option<bool>,
    shallow: Option<(u64, u64, f32)>,
    comparisons: usize,
    metrics_recorded: bool,
    metrics: &'a dyn MetricsCollector,
}

impl<'a, D: WandDocuments> WandCursor<'a, D> {
    pub(super) fn new(
        operator: Operator,
        postings: Vec<PostingIterator>,
        documents: &'a D,
        scorer: Arc<MemBM25Scorer>,
        params: &FtsSearchParams,
        metrics: &'a dyn MetricsCollector,
    ) -> Self {
        let cost = match operator {
            Operator::And => postings.iter().map(PostingIterator::cost).min(),
            Operator::Or => Some(
                postings
                    .iter()
                    .map(PostingIterator::cost)
                    .fold(0, usize::saturating_add),
            ),
        }
        .unwrap_or_default();
        Self {
            wand: Wand::new(operator, postings.into_iter(), documents, scorer),
            phrase_slop: params.phrase_slop,
            wand_factor: params.wand_factor,
            cost,
            current_doc: None,
            current_document_key: None,
            current_score: 0.0,
            confirmation: None,
            shallow: None,
            comparisons: 0,
            metrics_recorded: false,
            metrics,
        }
    }

    pub(super) fn doc(&self) -> Option<u64> {
        self.current_doc.map(|doc| doc.doc_id())
    }

    pub(super) fn document_key(&self) -> Option<u64> {
        self.current_document_key
    }

    fn clear_current(&mut self) {
        self.current_doc = None;
        self.current_document_key = None;
        self.current_score = 0.0;
        self.confirmation = None;
        self.shallow = None;
    }

    fn record_metrics(&mut self) {
        if !self.metrics_recorded {
            self.metrics.record_comparisons(self.comparisons);
            self.metrics_recorded = true;
        }
    }

    fn position_next(&mut self) -> Result<Option<u64>> {
        loop {
            let Some((doc, mut score)) = self.wand.next()? else {
                self.clear_current();
                self.record_metrics();
                return Ok(None);
            };
            self.comparisons += 1;
            let doc_id = doc.doc_id();
            let Some(document_key) = self.wand.documents.document_key(&doc) else {
                if self.wand.operator == Operator::Or {
                    self.wand.push_back_leads(doc_id.saturating_add(1));
                }
                continue;
            };
            let doc_length = self.wand.documents.doc_length(&doc);
            if self.wand.operator == Operator::Or {
                self.wand
                    .advance_all_tail(doc_id, Some(doc_length), Some(&mut score));
            } else {
                self.wand.advance_all_tail(doc_id, None, None);
                score = self.wand.score(doc_length);
            }
            self.current_doc = Some(doc);
            self.current_document_key = Some(document_key);
            self.current_score = score;
            self.confirmation = self.phrase_slop.is_none().then_some(true);
            self.shallow = None;
            return Ok(Some(doc_id));
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<u64>> {
        if let Some(doc) = self.current_doc
            && self.wand.operator == Operator::Or
        {
            self.wand.push_back_leads(doc.doc_id().saturating_add(1));
        }
        self.clear_current();
        self.position_next()
    }

    pub(super) fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if self.doc().is_some_and(|doc| doc >= target) {
            return Ok(self.doc());
        }
        self.clear_current();
        self.wand.seek(target);
        self.position_next()
    }

    pub(super) fn cost(&self) -> usize {
        self.cost
    }

    pub(super) fn current_score(&self) -> Result<f32> {
        self.current_doc
            .map(|_| self.current_score)
            .ok_or_else(|| Error::internal("posting FTS scorer is not positioned on a document"))
    }

    pub(super) fn matches(&mut self) -> Result<bool> {
        let Some(_) = self.current_doc else {
            return Ok(false);
        };
        if let Some(confirmed) = self.confirmation {
            return Ok(confirmed);
        }
        let phrase_slop = self.phrase_slop.ok_or_else(|| {
            Error::internal("posting FTS scorer requires phrase slop for position confirmation")
        })?;
        let confirmed = self.wand.check_positions(phrase_slop as i32)?;
        self.confirmation = Some(confirmed);
        Ok(confirmed)
    }

    pub(super) fn match_cost(&self) -> Option<f32> {
        self.phrase_slop.map(|_| self.wand.num_terms.max(1) as f32)
    }

    pub(super) fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let (up_to, upper) = self.wand.compound_shallow_bound(target);
        self.shallow = Some((target, up_to, upper));
        Ok(up_to)
    }

    pub(super) fn score_upper_bound(&self, up_to: u64) -> Result<f32> {
        let (target, shallow_up_to, upper) = self.shallow.ok_or_else(|| {
            Error::internal("score bound requires advance_shallow on the posting FTS scorer")
        })?;
        if up_to < target || up_to > shallow_up_to {
            return Err(Error::internal(format!(
                "posting FTS score bound up_to={up_to} is outside shallow range [{target}, {shallow_up_to}]"
            )));
        }
        Ok(upper)
    }

    pub(super) fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        if min_score.is_nan() {
            return Err(Error::invalid_input(
                "minimum competitive FTS score cannot be NaN",
            ));
        }
        let threshold = next_down_f32(min_score) * self.wand_factor;
        if threshold > self.wand.threshold {
            self.wand.threshold = threshold;
        }
        Ok(())
    }
}

impl<D: WandDocuments> Drop for WandCursor<'_, D> {
    fn drop(&mut self) {
        self.record_metrics();
    }
}

impl<S: Scorer, D: WandDocuments> Wand<'_, S, D> {
    fn seek(&mut self, target: u64) {
        self.up_to = None;
        self.and_max_score = f32::INFINITY;
        self.and_last_doc = None;
        if self.operator == Operator::And {
            for posting in &mut self.lead {
                if posting.doc().is_some_and(|doc| doc.doc_id() < target) {
                    posting.next(target);
                }
            }
            return;
        }

        let mut postings = std::mem::take(&mut self.head)
            .into_vec()
            .into_iter()
            .map(|posting| posting.posting)
            .chain(self.lead.drain(..))
            .chain(
                std::mem::take(&mut self.tail)
                    .into_vec()
                    .into_iter()
                    .map(|posting| posting.posting),
            )
            .collect::<Vec<_>>();
        self.tail_max_score = 0.0;
        for posting in &mut postings {
            if posting.doc().is_some_and(|doc| doc.doc_id() < target) {
                posting.next(target);
            }
        }
        self.head = postings
            .into_iter()
            .filter(|posting| posting.doc().is_some())
            .map(HeadPosting::new)
            .collect();
    }

    fn compound_shallow_bound(&mut self, target: u64) -> (u64, f32) {
        if self.operator == Operator::Or {
            self.update_max_scores(target);
            return (
                self.up_to.unwrap_or(TERMINATED_DOC_ID),
                conservative_score_sum(
                    self.lead
                        .iter()
                        .map(|posting| posting.window_max_score(self.up_to, &self.scorer))
                        .chain(self.head.iter().map(|posting| {
                            posting.posting.window_max_score(self.up_to, &self.scorer)
                        }))
                        .chain(self.tail.iter().map(|posting| posting.upper_bound)),
                ),
            );
        }

        let mut up_to = TERMINATED_DOC_ID;
        for posting in &mut self.lead {
            posting.shallow_next(target);
            up_to = up_to.min(Self::posting_block_up_to(posting, target));
        }
        let upper = conservative_score_sum(
            self.lead
                .iter()
                .map(|posting| posting.block_max_score(&self.scorer)),
        );
        (up_to, upper)
    }
}

fn conservative_score_sum(scores: impl Iterator<Item = f32>) -> f32 {
    let exact = scores.map(f64::from).sum::<f64>();
    let rounded = exact as f32;
    if f64::from(rounded) < exact {
        next_up_f32(rounded)
    } else {
        rounded
    }
}

fn next_up_f32(value: f32) -> f32 {
    if !value.is_finite() {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits(1);
    }
    let bits = value.to_bits();
    if value > 0.0 {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    }
}

fn next_down_f32(value: f32) -> f32 {
    if !value.is_finite() {
        return value;
    }
    if value == 0.0 {
        return f32::from_bits((1_u32 << 31) | 1);
    }
    let bits = value.to_bits();
    if value > 0.0 {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    }
}

#[cfg(test)]
mod tests {
    use arrow::buffer::ScalarBuffer;
    use rstest::rstest;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::impact::build_impact_skip_data;
    use super::*;
    use crate::scalar::inverted::scorer::{IndexBM25Scorer, MemBM25Scorer};
    use crate::{
        metrics::{MetricsCollector, NoOpMetricsCollector},
        scalar::inverted::{
            CompressedPostingList, PlainPostingList, PostingListBuilder, SharedPositionStream,
            builder::PositionRecorder,
            encoding::{
                compress_posting_list, compress_posting_list_with_tail_codec_and_block_size,
                encode_position_stream_block_into,
            },
        },
    };

    #[test]
    fn test_maxscore_prefix_bound_covers_f32_summation_rounding() {
        let remaining_bounds = [6.286_838_4e-7_f32, 0.015_441_144_f32];
        let essential_score = 2.762_496_2_f32;
        let threshold = f32::from_bits(0x4031_c9bc);

        // Raw f32 prefix accumulation rounds down to an apparent tie, while
        // scoring the clauses individually produces a competitive document.
        let raw_prefix = remaining_bounds.into_iter().sum::<f32>();
        assert_eq!(essential_score + raw_prefix, threshold);
        let actual_score = remaining_bounds
            .into_iter()
            .rev()
            .fold(essential_score, |score, bound| score + bound);
        assert!(actual_score > threshold);

        let prefix_bound = remaining_bounds.into_iter().map(f64::from).sum::<f64>();
        assert!(!score_sum_cannot_exceed(
            essential_score,
            prefix_bound,
            threshold,
            score_sum_upper_bound_factor(3),
        ));
    }

    struct UnitScorer;

    impl Scorer for UnitScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            1.0
        }

        fn doc_weight(&self, freq: u32, _doc_tokens: u32) -> f32 {
            freq as f32
        }
    }

    struct PartialNormScorer;

    impl Scorer for PartialNormScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            1.0
        }

        fn doc_weight(&self, freq: u32, _doc_tokens: u32) -> f32 {
            freq as f32
        }

        fn doc_norm(&self, doc_tokens: u32) -> Option<f32> {
            (doc_tokens == 0).then_some(0.0)
        }
    }

    #[test]
    fn test_norm_cache_requires_all_norm_codes() {
        let mut docs = DocSet::default();
        docs.append(0, 1);
        docs.set_quantized_scoring(true);
        let wand = Wand::new(Operator::Or, std::iter::empty(), &docs, PartialNormScorer);

        assert!(wand.norm_k_cache().is_none());
    }

    #[test]
    fn test_top_k_collector_reuses_frequency_slots() -> Result<()> {
        const LIMIT: usize = 8;
        const NUM_DOCS: usize = 10_000;
        let mut collector = TopKCollector::new(LIMIT, LIMIT);

        for doc in 0..NUM_DOCS {
            let num_terms = doc % 4 + 1;
            let inserted = collector.insert(
                ScoredDoc::new(doc as u64, doc as f32),
                num_terms as u32,
                doc as u64,
                (0..num_terms).map(|term| (term as u32, doc as u32)),
            )?;
            assert!(inserted);
            assert!(collector.num_frequency_slots() <= LIMIT);
        }
        assert_eq!(collector.num_frequency_slots(), LIMIT);

        let mut candidates = collector.into_candidates(|key| key)?;
        candidates.sort_unstable_by_key(|candidate| candidate.posting_doc_id);
        assert_eq!(candidates.len(), LIMIT);
        for (candidate, expected_doc) in candidates.iter().zip(NUM_DOCS - LIMIT..NUM_DOCS) {
            assert_eq!(candidate.posting_doc_id, expected_doc as u64);
            assert_eq!(candidate.document, expected_doc as u64);
            let expected_freqs = (0..expected_doc % 4 + 1)
                .map(|term| (term as u32, expected_doc as u32))
                .collect::<Vec<_>>();
            assert_eq!(candidate.freqs, expected_freqs);
        }
        Ok(())
    }

    #[rstest]
    #[case::auto("auto", Some(BulkAndMode::Auto))]
    #[case::auto_case_and_whitespace(" AUTO ", Some(BulkAndMode::Auto))]
    #[case::on("on", Some(BulkAndMode::On))]
    #[case::on_legacy("1", Some(BulkAndMode::On))]
    #[case::off("off", Some(BulkAndMode::Off))]
    #[case::off_legacy("0", Some(BulkAndMode::Off))]
    #[case::invalid("true", None)]
    #[case::empty("", None)]
    fn test_bulk_and_mode_parse(#[case] value: &str, #[case] expected: Option<BulkAndMode>) {
        assert_eq!(BulkAndMode::parse(value), expected);
    }

    #[rstest]
    #[case::auto_one(BulkAndMode::Auto, 1, false)]
    #[case::auto_two(BulkAndMode::Auto, 2, true)]
    #[case::auto_three(BulkAndMode::Auto, 3, true)]
    #[case::auto_four(BulkAndMode::Auto, 4, false)]
    #[case::on_one(BulkAndMode::On, 1, true)]
    #[case::on_five(BulkAndMode::On, 5, true)]
    #[case::off_two(BulkAndMode::Off, 2, false)]
    #[case::off_five(BulkAndMode::Off, 5, false)]
    fn test_bulk_and_mode_enabled_for(
        #[case] mode: BulkAndMode,
        #[case] num_clauses: usize,
        #[case] expected: bool,
    ) {
        assert_eq!(mode.enabled_for(num_clauses), expected);
    }

    struct PanicQueryWeightScorer;

    impl Scorer for PanicQueryWeightScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            panic!("query_weight should be precomputed before WAND construction");
        }

        fn doc_weight(&self, freq: u32, _doc_tokens: u32) -> f32 {
            freq as f32
        }
    }

    struct InverseDocLengthScorer;

    impl Scorer for InverseDocLengthScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            1.0
        }

        fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
            freq as f32 / doc_tokens as f32
        }
    }

    // Inverse-doc-length scorer that counts scored documents, so a test can
    // assert that block-max pruning skipped blocks.
    struct CountingScorer {
        scored: Arc<AtomicUsize>,
    }

    impl Scorer for CountingScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            1.0
        }

        fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
            self.scored.fetch_add(1, Ordering::Relaxed);
            freq as f32 / doc_tokens as f32
        }
    }

    #[derive(Default)]
    struct CountAndSearchStats {
        comparisons: AtomicUsize,
        candidates_seen: AtomicUsize,
        candidates_pruned_before_return: AtomicUsize,
        full_scores: AtomicUsize,
        freqs_collected: AtomicUsize,
    }

    impl MetricsCollector for CountAndSearchStats {
        fn record_parts_loaded(&self, _: usize) {}

        fn record_index_loads(&self, _: usize) {}

        fn record_comparisons(&self, n: usize) {
            self.comparisons.fetch_add(n, Ordering::Relaxed);
        }

        fn record_and_candidates_seen(&self, n: usize) {
            self.candidates_seen.fetch_add(n, Ordering::Relaxed);
        }

        fn record_and_candidates_pruned_before_return(&self, n: usize) {
            self.candidates_pruned_before_return
                .fetch_add(n, Ordering::Relaxed);
        }

        fn record_and_full_scores(&self, n: usize) {
            self.full_scores.fetch_add(n, Ordering::Relaxed);
        }

        fn record_freqs_collected(&self, n: usize) {
            self.freqs_collected.fetch_add(n, Ordering::Relaxed);
        }
    }

    struct PanicOnAndMetrics {
        comparisons: AtomicUsize,
    }

    impl PanicOnAndMetrics {
        fn new() -> Self {
            Self {
                comparisons: AtomicUsize::new(0),
            }
        }
    }

    impl MetricsCollector for PanicOnAndMetrics {
        fn record_parts_loaded(&self, _: usize) {}

        fn record_index_loads(&self, _: usize) {}

        fn record_comparisons(&self, n: usize) {
            self.comparisons.fetch_add(n, Ordering::Relaxed);
        }

        fn record_and_candidates_seen(&self, _: usize) {
            panic!("OR search should not record AND candidate metrics");
        }

        fn record_and_candidates_pruned_before_return(&self, _: usize) {
            panic!("OR search should not record AND prune metrics");
        }

        fn record_and_full_scores(&self, _: usize) {
            panic!("OR search should not record AND scoring metrics");
        }

        fn record_freqs_collected(&self, _: usize) {
            panic!("OR search should not record AND frequency metrics");
        }
    }

    fn generate_posting_list(
        doc_ids: Vec<u32>,
        max_score: f32,
        block_max_scores: Option<Vec<f32>>,
        is_compressed: bool,
    ) -> PostingList {
        let freqs = vec![1; doc_ids.len()];
        generate_posting_list_with_freqs(doc_ids, freqs, max_score, block_max_scores, is_compressed)
    }

    fn generate_posting_list_with_freqs(
        doc_ids: Vec<u32>,
        freqs: Vec<u32>,
        max_score: f32,
        block_max_scores: Option<Vec<f32>>,
        is_compressed: bool,
    ) -> PostingList {
        assert_eq!(doc_ids.len(), freqs.len());
        let block_max_scores = block_max_scores.unwrap_or_else(|| vec![max_score; doc_ids.len()]);
        if is_compressed {
            let blocks = compress_posting_list(
                doc_ids.len(),
                doc_ids.iter(),
                freqs.iter(),
                block_max_scores.into_iter(),
            )
            .unwrap();
            PostingList::Compressed(CompressedPostingList::new(
                blocks,
                max_score,
                doc_ids.len() as u32,
                crate::scalar::inverted::PostingTailCodec::VarintDelta,
                crate::scalar::inverted::LEGACY_BLOCK_SIZE,
                None,
                None,
            ))
        } else {
            PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from_iter(doc_ids.iter().map(|id| *id as u64)),
                ScalarBuffer::from_iter(freqs.iter().map(|freq| *freq as f32)),
                Some(max_score),
                None,
            ))
        }
    }

    fn generate_impact_posting_list_with_freqs(
        doc_ids: Vec<u32>,
        freqs: Vec<u32>,
        doc_lengths: Vec<u32>,
    ) -> PostingList {
        generate_impact_posting_list_with_freqs_and_block_size(
            doc_ids,
            freqs,
            doc_lengths,
            crate::scalar::inverted::LEGACY_BLOCK_SIZE,
        )
    }

    fn generate_impact_posting_list_with_freqs_and_block_size(
        doc_ids: Vec<u32>,
        freqs: Vec<u32>,
        doc_lengths: Vec<u32>,
        block_size: usize,
    ) -> PostingList {
        assert_eq!(doc_ids.len(), freqs.len());
        assert_eq!(doc_ids.len(), doc_lengths.len());
        let block_max_scores = vec![0.0; doc_ids.len().div_ceil(block_size)];
        let blocks = compress_posting_list_with_tail_codec_and_block_size(
            doc_ids.len(),
            doc_ids.iter(),
            freqs.iter(),
            block_max_scores.into_iter(),
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            block_size,
        )
        .unwrap();
        let impact_blocks = doc_ids
            .chunks(block_size)
            .zip(freqs.chunks(block_size))
            .zip(doc_lengths.chunks(block_size))
            .map(|((doc_ids, freqs), doc_lengths)| {
                doc_ids
                    .iter()
                    .copied()
                    .zip(freqs.iter().copied())
                    .zip(doc_lengths.iter().copied())
                    .map(|((doc_id, freq), doc_length)| (doc_id, freq, doc_length))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let impacts = build_impact_skip_data(impact_blocks.as_slice()).unwrap();
        PostingList::Compressed(CompressedPostingList::new(
            blocks,
            0.0,
            doc_ids.len() as u32,
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            block_size,
            None,
            Some(impacts),
        ))
    }

    fn generate_contiguous_impact_posting_list_with_block_size(
        total: usize,
        block_size: usize,
    ) -> PostingList {
        generate_impact_posting_list_with_freqs_and_block_size(
            (0..total as u32).collect(),
            vec![1; total],
            vec![1; total],
            block_size,
        )
    }

    fn generate_posting_list_with_positions(
        doc_ids: Vec<u32>,
        positions_by_doc: Vec<Vec<u32>>,
        max_score: f32,
        is_compressed: bool,
    ) -> PostingList {
        let freqs = positions_by_doc
            .iter()
            .map(|positions| positions.len() as u32)
            .collect::<Vec<_>>();
        if is_compressed {
            let mut builder = PostingListBuilder::new(true);
            for (doc_id, positions) in doc_ids.iter().copied().zip(positions_by_doc) {
                builder.add(doc_id, PositionRecorder::Position(positions.into()));
            }
            let batch = builder
                .to_batch(vec![max_score; doc_ids.len().div_ceil(BLOCK_SIZE)])
                .unwrap();
            PostingList::from_batch(&batch, Some(max_score), Some(doc_ids.len() as u32)).unwrap()
        } else {
            let mut position_builder =
                arrow::array::ListBuilder::new(arrow::array::Int32Builder::new());
            for positions in positions_by_doc {
                for position in positions {
                    position_builder.values().append_value(position as i32);
                }
                position_builder.append(true);
            }
            PostingList::Plain(PlainPostingList::new(
                ScalarBuffer::from_iter(doc_ids.iter().map(|id| *id as u64)),
                ScalarBuffer::from_iter(freqs.iter().map(|freq| *freq as f32)),
                Some(max_score),
                Some(position_builder.finish()),
            ))
        }
    }

    #[rstest]
    #[case::packed_delta(PositionStreamCodec::PackedDelta)]
    #[case::varint_doc_delta(PositionStreamCodec::VarintDocDelta)]
    fn test_shared_position_cursors_use_independent_scratch(
        #[case] codec: PositionStreamCodec,
    ) -> Result<()> {
        let mut posting_list =
            generate_posting_list_with_positions(vec![0], vec![vec![1_u32, 3, 10]], 1.0, true);
        let PostingList::Compressed(ref mut list) = posting_list else {
            unreachable!("the helper was asked for a compressed posting list");
        };
        let mut encoded = Vec::new();
        encode_position_stream_block_into(&[1, 3, 10], &[3], codec, &mut encoded)?;
        list.positions = Some(CompressedPositionStorage::SharedStream(
            SharedPositionStream::new(codec, vec![0], bytes::Bytes::from(encoded)),
        ));

        let posting = PostingIterator::new(String::from("term"), 0, 0, posting_list, 1);
        let first = posting.position_cursor()?;
        let second = posting.position_cursor()?;

        assert_eq!(second.positions.as_slice(), &[1, 3, 10]);
        assert_eq!(first.positions.as_slice(), &[1, 3, 10]);
        assert!(posting.position_scratch.borrow().is_none());
        drop(second);
        assert!(posting.position_scratch.borrow().is_some());
        drop(first);
        assert!(posting.position_scratch.borrow().is_some());
        Ok(())
    }

    #[test]
    fn test_phrase_search_propagates_corrupt_packed_positions() {
        let mut docs = DocSet::default();
        docs.append(0, BLOCK_SIZE as u32 + 1);

        let mut corrupt_list = generate_posting_list_with_positions(
            vec![0],
            vec![(0..BLOCK_SIZE as u32).collect()],
            1.0,
            true,
        );
        let PostingList::Compressed(ref mut list) = corrupt_list else {
            unreachable!("the helper was asked for a compressed posting list");
        };
        // A bit width of one requires a 16-byte payload. Keep only the header
        // to verify malformed on-disk data becomes a search error, not a panic.
        list.positions = Some(CompressedPositionStorage::SharedStream(
            SharedPositionStream::new(
                PositionStreamCodec::PackedDelta,
                vec![0],
                bytes::Bytes::from_static(&[1]),
            ),
        ));

        let postings = vec![
            PostingIterator::new(String::from("corrupt"), 0, 0, corrupt_list, docs.len()),
            PostingIterator::new(
                String::from("valid"),
                1,
                1,
                generate_posting_list_with_positions(
                    vec![0],
                    vec![vec![BLOCK_SIZE as u32]],
                    1.0,
                    true,
                ),
                docs.len(),
            ),
        ];
        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        let mut params = FtsSearchParams::default().with_limit(Some(10));
        params.phrase_slop = Some(0);

        let error = wand
            .search(&params, &NoOpMetricsCollector)
            .expect_err("corrupt packed positions should fail the phrase search");
        let message = error.to_string();
        assert!(
            message.contains("packed position group payload"),
            "{message}"
        );
        assert!(message.contains("corrupt"), "{message}");
    }

    fn sorted_candidate_row_ids(candidates: Vec<DocCandidate<u64>>) -> Vec<u64> {
        let mut row_ids = candidates
            .into_iter()
            .map(|candidate| candidate.document)
            .collect::<Vec<_>>();
        row_ids.sort_unstable();
        row_ids
    }

    #[rstest]
    #[tokio::test]
    async fn test_wand(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        for i in 0..2 * BLOCK_SIZE {
            docs.append(i as u64, 1);
        }

        // when the pivot is greater than 0, and the first posting list is exhausted after shallow_next
        let postings = vec![
            PostingIterator::new(
                String::from("test"),
                0,
                0,
                generate_posting_list(
                    Vec::from_iter(0..=BLOCK_SIZE as u32 + 1),
                    1.0,
                    None,
                    is_compressed,
                ),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("full"),
                1,
                1,
                generate_posting_list(vec![BLOCK_SIZE as u32 + 2], 1.0, None, is_compressed),
                docs.len(),
            ),
        ];

        let bm25 = IndexBM25Scorer::new(std::iter::empty());
        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, bm25);
        // This should trigger the bug when the second posting list becomes empty
        let result = wand
            .search(&FtsSearchParams::default(), &NoOpMetricsCollector)
            .unwrap();
        assert_eq!(result.len(), 0); // Should not panic
    }

    /// The shared floor prunes partitions that can't reach the global top-k: a
    /// high-scoring partition sets the floor and the rest skip their blocks.
    /// (Result correctness is covered by the FTS search tests, since sharing is
    /// always on.)
    #[test]
    fn cross_partition_threshold_sharing_prunes() {
        use crate::metrics::MetricsCollector;
        use std::sync::atomic::AtomicUsize;

        #[derive(Default)]
        struct CountComparisons(AtomicUsize);
        impl MetricsCollector for CountComparisons {
            fn record_parts_loaded(&self, _: usize) {}
            fn record_index_loads(&self, _: usize) {}
            fn record_comparisons(&self, n: usize) {
                self.0.fetch_add(n, Ordering::Relaxed);
            }
        }

        let params = FtsSearchParams::default().with_limit(Some(10));
        let part_docs = 4 * BLOCK_SIZE as u32;
        // One high-scoring partition (weight 10) then 7 low-scoring ones.
        let parts: Vec<(f32, std::ops::Range<u32>)> = std::iter::once((10.0, 0..part_docs))
            .chain((1..8).map(|i| (1.0, i * part_docs..(i + 1) * part_docs)))
            .collect();

        let new_floor = || Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits()));

        // Total comparisons across all partitions. `Some(floor)` makes every
        // partition share that one floor; `None` gives each its own.
        let total_comparisons = |shared_floor: Option<&Arc<AtomicU32>>| -> usize {
            let metrics = CountComparisons::default();
            for (qw, rows) in &parts {
                let mut docs = DocSet::default();
                for d in rows.clone() {
                    docs.append(d as u64, 1);
                }
                let postings = vec![PostingIterator::with_query_weight(
                    String::from("t"),
                    0,
                    0,
                    *qw,
                    generate_posting_list(rows.clone().collect(), *qw, None, false),
                    docs.len(),
                )];
                let floor = shared_floor.cloned().unwrap_or_else(new_floor);
                Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer)
                    .with_shared_threshold(floor)
                    .search(&params, &metrics)
                    .unwrap();
            }
            metrics.0.load(Ordering::Relaxed)
        };

        let one_floor = new_floor();
        let with_shared_floor = total_comparisons(Some(&one_floor));
        let with_private_floors = total_comparisons(None);
        assert!(
            with_shared_floor < with_private_floors,
            "shared floor should prune comparisons: \
             shared={with_shared_floor} private={with_private_floors}"
        );
    }

    #[test]
    fn test_posting_iterator_next_compressed_partition_point() {
        let mut docs = DocSet::default();
        let num_docs = (BLOCK_SIZE * 2 + 5) as u32;
        for i in 0..num_docs {
            docs.append(i as u64, 1);
        }

        let doc_ids = (0..num_docs).collect::<Vec<_>>();
        let posting = generate_posting_list(doc_ids, 1.0, None, true);
        let mut iter = PostingIterator::new(String::from("term"), 0, 0, posting, docs.len());

        iter.next(10);
        assert_eq!(iter.doc().unwrap().doc_id(), 10);

        let target = BLOCK_SIZE as u64 + 3;
        iter.next(target);
        assert_eq!(iter.doc().unwrap().doc_id(), target);

        iter.next(num_docs as u64 + 10);
        assert!(iter.doc().is_none());
    }

    #[test]
    fn test_wand_skip_to_next_block() {
        let mut docs = DocSet::default();
        for i in 0..201 {
            docs.append(i as u64, 1);
        }

        let large_posting_docs1: Vec<u32> = (0..=200).collect();

        let postings = vec![
            PostingIterator::new(
                String::from("full"),
                0,
                0,
                generate_posting_list(large_posting_docs1, 1.0, Some(vec![0.5, 0.5]), true),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("text"),
                1,
                1,
                generate_posting_list(vec![0], 1.0, Some(vec![0.5]), true),
                docs.len(),
            ),
        ];

        let bm25 = IndexBM25Scorer::new(std::iter::empty());
        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, bm25);

        // set a threshold that the sum of max scores can hit,
        // but the sum of block max scores is less than the threshold,
        wand.threshold = 1.5;

        let result = wand.search(&FtsSearchParams::default(), &NoOpMetricsCollector);

        assert!(result.is_ok());
    }

    #[test]
    fn test_or_single_term_block_skip_matches_and() {
        // Hot docs occupy the middle block; the flanking blocks score far below
        // the threshold. A single-term disjunction must skip them yet return what
        // the conjunctive path returns.
        let total = 3 * BLOCK_SIZE as u32;
        let hot = BLOCK_SIZE as u32..BLOCK_SIZE as u32 + 12;

        let mut docs = DocSet::default();
        for row_id in 0..total {
            // hot docs get distinct scores 1/1..1/12; the rest score 0.001
            let doc_tokens = if hot.contains(&row_id) {
                row_id - hot.start + 1
            } else {
                1000
            };
            docs.append(row_id as u64, doc_tokens);
        }

        let params = FtsSearchParams::new().with_limit(Some(10));
        let run = |operator| {
            let scored = Arc::new(AtomicUsize::new(0));
            let posting = PostingIterator::with_query_weight(
                String::from("term"),
                0,
                0,
                1.0,
                generate_posting_list(
                    (0..total).collect(),
                    1.0,
                    Some(vec![0.001, 1.0, 0.001]),
                    true,
                ),
                docs.len(),
            );
            let mut wand = Wand::new(
                operator,
                std::iter::once(posting),
                &docs,
                CountingScorer {
                    scored: scored.clone(),
                },
            );
            let hits = wand.search(&params, &NoOpMetricsCollector).unwrap();
            let mut row_ids = hits.iter().map(|hit| hit.document).collect::<Vec<_>>();
            row_ids.sort_unstable();
            (row_ids, scored.load(Ordering::Relaxed))
        };

        let (or_hits, or_scored) = run(Operator::Or);
        let (and_hits, _) = run(Operator::And);

        let expected = (hot.start..hot.start + 10)
            .map(u64::from)
            .collect::<Vec<_>>();
        assert_eq!(or_hits, expected, "OR must return the top-k");
        assert_eq!(or_hits, and_hits, "OR and AND must agree for a single term");
        // Without pruning OR scores all `total` docs; with it the cold blocks are skipped.
        assert!(
            or_scored <= 2 * BLOCK_SIZE,
            "expected pruning to skip a block, but scored {or_scored} of {total}",
        );
    }

    #[rstest]
    fn test_or_search_does_not_record_and_metrics(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        for row_id in 0..6 {
            docs.append(row_id, 1);
        }

        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("alpha"),
                0,
                0,
                1.0,
                generate_posting_list(vec![0, 1, 4], 1.0, None, is_compressed),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("beta"),
                1,
                1,
                1.0,
                generate_posting_list(vec![1, 2, 5], 1.0, None, is_compressed),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        let metrics = PanicOnAndMetrics::new();
        let candidates = wand.search(&FtsSearchParams::default(), &metrics).unwrap();

        assert_eq!(sorted_candidate_row_ids(candidates), vec![0, 1, 2, 4, 5]);
        assert!(metrics.comparisons.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_wand_new_uses_precomputed_query_weight() {
        let mut docs = DocSet::default();
        docs.append(1, 1);

        let postings = vec![PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            2.0,
            generate_posting_list(vec![0], 1.0, None, false),
            docs.len(),
        )];

        let wand = Wand::new(
            Operator::Or,
            postings.into_iter(),
            &docs,
            PanicQueryWeightScorer,
        );
        assert_eq!(wand.head.len(), 1);
    }

    #[test]
    fn test_and_search_terminates_for_disjoint_postings() {
        let mut docs = DocSet::default();
        for i in 0..6 {
            docs.append(i, 1);
        }

        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("a"),
                0,
                0,
                1.0,
                generate_posting_list(vec![0, 2, 4], 1.0, None, false),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("b"),
                1,
                1,
                1.0,
                generate_posting_list(vec![1, 3, 5], 1.0, None, false),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        assert!(wand.next().unwrap().is_none());
    }

    #[test]
    fn test_up_to_refreshes_on_first_candidate() {
        let mut docs = DocSet::default();
        for i in 0..=(BLOCK_SIZE as u64 + 1) {
            docs.append(i, 1);
        }

        let postings = vec![PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            generate_posting_list(
                (0..=(BLOCK_SIZE as u32 + 1)).collect(),
                1.0,
                Some(vec![1.0, 1.0]),
                true,
            ),
            docs.len(),
        )];

        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        assert!(wand.up_to.is_none());
        let _ = wand.next().unwrap();
        assert!(wand.up_to.is_some());
    }

    #[test]
    fn test_or_push_back_lead_uses_current_block_max_for_tail_bound() {
        let total = 2 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for doc_id in 0..total {
            docs.append(doc_id as u64, 1);
        }

        let postings = vec![PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            generate_posting_list((0..total).collect(), 10.0, Some(vec![1.0, 10.0]), true),
            docs.len(),
        )];
        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 1.5;

        wand.update_max_scores(0);
        wand.move_head_doc_to_lead(0);
        assert_eq!(wand.up_to, Some((BLOCK_SIZE - 1) as u64));

        wand.push_back_leads(1);

        assert_eq!(wand.tail.len(), 1);
        assert!(
            (wand.tail_max_score - 1.0).abs() < 1e-6,
            "tail should use the current block max, got {}",
            wand.tail_max_score
        );
        assert!(wand.head_doc().is_none());
    }

    #[test]
    fn test_or_push_back_lead_falls_back_after_block_window_expires() {
        let total = 2 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for doc_id in 0..total {
            docs.append(doc_id as u64, 1);
        }

        let freqs = (0..total)
            .map(|doc_id| if doc_id >= BLOCK_SIZE as u32 { 10 } else { 1 })
            .collect::<Vec<_>>();
        let mut posting = PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            generate_posting_list_with_freqs(
                (0..total).collect(),
                freqs,
                10.0,
                Some(vec![1.0, 10.0]),
                true,
            ),
            docs.len(),
        );
        posting.next((BLOCK_SIZE - 1) as u64);
        let mut wand = Wand::new(Operator::Or, std::iter::once(posting), &docs, UnitScorer);
        wand.threshold = 1.5;

        let block_end = (BLOCK_SIZE - 1) as u64;
        wand.update_max_scores(block_end);
        wand.move_head_doc_to_lead(block_end);
        assert_eq!(wand.up_to, Some(block_end));

        wand.push_back_leads(BLOCK_SIZE as u64);

        assert!(wand.tail.is_empty());
        assert_eq!(wand.head_doc(), Some(BLOCK_SIZE as u64));
        let candidate = wand.next().unwrap().unwrap();
        assert_eq!(candidate.0.doc_id(), BLOCK_SIZE as u64);
    }

    #[test]
    fn test_non_positive_threshold_advances_without_impact_bound_scoring() {
        let mut docs = DocSet::default();
        for doc_id in 0..3 {
            docs.append(doc_id, 1);
        }
        let make_posting = || {
            PostingIterator::with_query_weight(
                String::from("term"),
                0,
                0,
                1.0,
                generate_impact_posting_list_with_freqs(
                    vec![0, 1, 2],
                    vec![1, 1, 1],
                    vec![1, 1, 1],
                ),
                docs.len(),
            )
        };
        let scored = Arc::new(AtomicUsize::new(0));

        let mut wand = Wand::new(
            Operator::Or,
            std::iter::once(make_posting()),
            &docs,
            CountingScorer {
                scored: scored.clone(),
            },
        );
        wand.move_head_before_target_to_tail(1);
        assert_eq!(wand.head_doc(), Some(1));
        assert!(wand.tail.is_empty());
        assert_eq!(scored.load(Ordering::Relaxed), 0);

        let mut wand = Wand::new(
            Operator::Or,
            std::iter::once(make_posting()),
            &docs,
            CountingScorer {
                scored: scored.clone(),
            },
        );
        wand.move_head_doc_to_lead(0);
        wand.push_back_leads(1);
        assert_eq!(wand.head_doc(), Some(1));
        assert!(wand.tail.is_empty());
        assert_eq!(scored.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_or_plain_tail_does_not_advance_headless_window() {
        let mut docs = DocSet::default();
        for doc_id in 0..4 {
            docs.append(doc_id, 1);
        }

        let postings = vec![PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            generate_posting_list(vec![0, 1, 2, 3], 1.0, None, false),
            docs.len(),
        )];
        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 2.0;

        wand.update_max_scores(0);
        wand.move_head_doc_to_lead(0);
        wand.push_back_leads(1);

        assert_eq!(wand.tail.len(), 1);
        assert!(wand.head_doc().is_none());
        assert!(!wand.advance_tail_to_next_or_window());
    }

    #[test]
    fn test_or_headless_tail_window_scans_past_final_top_tail() {
        let total = 3 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for doc_id in 0..total {
            docs.append(doc_id as u64, 1);
        }

        let future_docs = (0..BLOCK_SIZE as u32)
            .chain(2 * BLOCK_SIZE as u32..3 * BLOCK_SIZE as u32)
            .collect::<Vec<_>>();
        let mut future_freqs = vec![1; future_docs.len()];
        future_freqs[0] = 4;
        future_freqs[BLOCK_SIZE] = 20;
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("future"),
                0,
                0,
                1.0,
                generate_posting_list_with_freqs(
                    future_docs,
                    future_freqs,
                    20.0,
                    Some(vec![4.0, 20.0]),
                    true,
                ),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("final_tail"),
                1,
                1,
                1.0,
                generate_posting_list_with_freqs(vec![0], vec![6], 6.0, Some(vec![6.0]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("booster"),
                2,
                2,
                1.0,
                generate_posting_list_with_freqs(vec![0], vec![7], 7.0, Some(vec![7.0]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        let result = wand
            .search(
                &FtsSearchParams::new().with_limit(Some(1)),
                &NoOpMetricsCollector,
            )
            .unwrap();

        assert_eq!(
            sorted_candidate_row_ids(result),
            vec![(2 * BLOCK_SIZE) as u64]
        );
    }

    #[test]
    fn test_and_search_prunes_with_threshold_and_keeps_candidate() {
        let mut docs = DocSet::default();
        for i in 0..(2 * BLOCK_SIZE as u64) {
            let doc_tokens = if i < BLOCK_SIZE as u64 { 100 } else { 1 };
            docs.append(i, doc_tokens);
        }
        let all_docs = (0..2 * BLOCK_SIZE as u32).collect::<Vec<_>>();

        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("a"),
                0,
                0,
                1.0,
                generate_posting_list(all_docs.clone(), 1.0, Some(vec![0.02, 1.0]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("b"),
                1,
                1,
                1.0,
                generate_posting_list(all_docs, 1.0, Some(vec![0.02, 1.0]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(
            Operator::And,
            postings.into_iter(),
            &docs,
            InverseDocLengthScorer,
        );
        wand.threshold = 0.5;

        let candidate = wand.next().unwrap().unwrap();
        assert_eq!(candidate.0.doc_id(), BLOCK_SIZE as u64);
    }

    #[test]
    fn test_and_advance_falls_back_to_narrow_when_range_max_loosens_bound() {
        let total = 4 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for i in 0..total {
            docs.append(i as u64, 1);
        }

        let lead_docs = (0..total).step_by(2).collect::<Vec<_>>();
        let follower_docs = (0..total).collect::<Vec<_>>();
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("lead"),
                0,
                0,
                1.0,
                generate_posting_list(lead_docs, 1.0, Some(vec![1.0, 1.0]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("follower"),
                1,
                1,
                1.0,
                generate_posting_list(follower_docs, 10.0, Some(vec![0.1, 10.0, 0.1, 0.1]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 5.0;

        let target = wand.and_advance_target(0);

        assert_eq!(target, BLOCK_SIZE as u64);
        assert_eq!(wand.up_to, Some((2 * BLOCK_SIZE - 1) as u64));
        assert!(
            (wand.and_max_score - 11.0).abs() < 1e-6,
            "expected the second narrow window to include the high follower block, got {}",
            wand.and_max_score
        );
        assert_eq!(wand.and_window_stats.windows_wide, 0);
        assert_eq!(wand.and_window_stats.windows_narrow, 2);
        assert_eq!(wand.and_window_stats.windows_skipped, 1);
    }

    #[test]
    fn test_and_advance_uses_narrow_window_for_candidate_ranges() {
        let total = 4 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for i in 0..total {
            docs.append(i as u64, 1);
        }

        let lead_docs = (0..total).step_by(2).collect::<Vec<_>>();
        let follower_docs = (0..total).collect::<Vec<_>>();
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("lead"),
                0,
                0,
                1.0,
                generate_posting_list(lead_docs, 1.0, Some(vec![1.0, 1.0]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("follower"),
                1,
                1,
                1.0,
                generate_posting_list(follower_docs, 1.0, Some(vec![1.0, 1.0, 1.0, 1.0]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 1.5;

        let target = wand.and_advance_target(0);

        assert_eq!(target, 0);
        assert_eq!(wand.up_to, Some((BLOCK_SIZE - 1) as u64));
        assert!((wand.and_max_score - 2.0).abs() < 1e-6);
        assert_eq!(wand.and_window_stats.windows_wide, 0);
        assert_eq!(wand.and_window_stats.windows_narrow, 1);
        assert_eq!(wand.and_window_stats.range_blocks_scanned, 0);
    }

    #[test]
    fn test_and_wide_window_only_skips_and_does_not_return_candidates() {
        let total = 4 * BLOCK_SIZE as u32;
        let mut docs = DocSet::default();
        for i in 0..total {
            docs.append(i as u64, 1);
        }

        let lead_docs = (0..total).step_by(2).collect::<Vec<_>>();
        let follower_docs = (0..total).collect::<Vec<_>>();
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("lead"),
                0,
                0,
                1.0,
                generate_posting_list(lead_docs, 3.0, Some(vec![1.0, 3.0]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("follower"),
                1,
                1,
                1.0,
                generate_posting_list(follower_docs, 3.0, Some(vec![0.1, 0.1, 3.0, 3.0]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 2.0;

        let candidate = wand.next().unwrap().unwrap();

        assert_eq!(candidate.0.doc_id(), (2 * BLOCK_SIZE) as u64);
        assert_eq!(wand.up_to, Some((3 * BLOCK_SIZE - 1) as u64));
        assert_eq!(wand.and_window_stats.windows_wide, 1);
        assert_eq!(wand.and_window_stats.windows_skipped, 1);
        assert_eq!(wand.and_window_stats.windows_narrow, 1);
        assert_eq!(wand.and_window_stats.candidates_returned, 1);
    }

    #[test]
    fn test_and_range_max_preserves_exact_top_k() {
        let total = 4 * BLOCK_SIZE as u32;
        let hot = BLOCK_SIZE as u32..BLOCK_SIZE as u32 + 16;
        let mut docs = DocSet::default();
        for doc_id in 0..total {
            let doc_tokens = if hot.contains(&doc_id) { 1 } else { 1000 };
            docs.append(doc_id as u64, doc_tokens);
        }

        let params = FtsSearchParams::new().with_limit(Some(8));
        let run = |is_compressed: bool| {
            let lead_docs = (0..total).step_by(2).collect::<Vec<_>>();
            let follower_docs = (0..total).collect::<Vec<_>>();
            let lead_scores = is_compressed.then_some(vec![1.0, 0.001]);
            let follower_scores = is_compressed.then_some(vec![0.001, 1.0, 0.001, 0.001]);
            let postings = vec![
                PostingIterator::with_query_weight(
                    String::from("lead"),
                    0,
                    0,
                    1.0,
                    generate_posting_list(lead_docs, 1.0, lead_scores, is_compressed),
                    docs.len(),
                ),
                PostingIterator::with_query_weight(
                    String::from("follower"),
                    1,
                    1,
                    1.0,
                    generate_posting_list(follower_docs, 1.0, follower_scores, is_compressed),
                    docs.len(),
                ),
            ];
            let mut wand = Wand::new(
                Operator::And,
                postings.into_iter(),
                &docs,
                InverseDocLengthScorer,
            );
            sorted_candidate_row_ids(wand.search(&params, &NoOpMetricsCollector).unwrap())
        };

        let compressed = run(true);
        let plain = run(false);
        let expected = hot.step_by(2).map(u64::from).collect::<Vec<_>>();
        assert_eq!(compressed, expected);
        assert_eq!(compressed, plain);
    }

    #[test]
    fn test_block_max_score_up_to_slides_and_expires_old_max() {
        let total = 5 * BLOCK_SIZE as u32;
        let posting = generate_posting_list(
            (0..total).collect(),
            5.0,
            Some(vec![1.0, 4.0, 2.0, 5.0, 3.0]),
            true,
        );
        let mut posting = PostingIterator::new(String::from("term"), 0, 0, posting, total as usize);

        posting.shallow_next(0);
        assert_eq!(
            posting
                .block_max_score_up_to_with_stats((3 * BLOCK_SIZE - 1) as u64, &UnitScorer)
                .score,
            4.0
        );

        posting.shallow_next((2 * BLOCK_SIZE) as u64);
        assert_eq!(
            posting
                .block_max_score_up_to_with_stats((4 * BLOCK_SIZE - 1) as u64, &UnitScorer)
                .score,
            5.0
        );

        posting.shallow_next((4 * BLOCK_SIZE) as u64);
        assert_eq!(
            posting
                .block_max_score_up_to_with_stats((5 * BLOCK_SIZE - 1) as u64, &UnitScorer)
                .score,
            3.0
        );
    }

    #[test]
    fn test_impact_level1_skip_keeps_boundary_equality_in_group() {
        for block_size in [crate::scalar::inverted::LEGACY_BLOCK_SIZE, 256] {
            let total = (IMPACT_LEVEL1_BLOCKS + 1) * block_size;
            let mut posting = PostingIterator::new(
                String::from("term"),
                0,
                0,
                generate_contiguous_impact_posting_list_with_block_size(total, block_size),
                total,
            );
            let target = (IMPACT_LEVEL1_BLOCKS * block_size - 1) as u64;

            posting.shallow_next(target);
            assert_eq!(posting.block_idx, IMPACT_LEVEL1_BLOCKS - 1);

            posting.next(target);
            assert_eq!(posting.block_idx, IMPACT_LEVEL1_BLOCKS - 1);
            assert_eq!(posting.doc().map(|doc| doc.doc_id()), Some(target));
        }
    }

    #[test]
    fn test_impact_level1_skip_handles_partial_final_group() {
        for block_size in [crate::scalar::inverted::LEGACY_BLOCK_SIZE, 256] {
            let total = (IMPACT_LEVEL1_BLOCKS + 3) * block_size + 17;
            let mut posting = PostingIterator::new(
                String::from("term"),
                0,
                0,
                generate_contiguous_impact_posting_list_with_block_size(total, block_size),
                total,
            );
            let target = (total - 1) as u64;
            let expected_block = total.div_ceil(block_size) - 1;

            posting.shallow_next(target);
            assert_eq!(posting.block_idx, expected_block);

            posting.next(target);
            assert_eq!(posting.block_idx, expected_block);
            assert_eq!(posting.doc().map(|doc| doc.doc_id()), Some(target));
        }
    }

    #[test]
    fn test_impact_level1_skip_reaches_far_target_doc() {
        for block_size in [crate::scalar::inverted::LEGACY_BLOCK_SIZE, 256] {
            let total = (IMPACT_LEVEL1_BLOCKS * 3 + 5) * block_size;
            let target_block = IMPACT_LEVEL1_BLOCKS * 2 + 2;
            let target = (target_block * block_size + 17) as u64;
            let mut posting = PostingIterator::new(
                String::from("term"),
                0,
                0,
                generate_contiguous_impact_posting_list_with_block_size(total, block_size),
                total,
            );

            posting.shallow_next(target);
            assert_eq!(posting.block_idx, target_block);

            posting.next(target);
            assert_eq!(posting.block_idx, target_block);
            assert_eq!(posting.doc().map(|doc| doc.doc_id()), Some(target));
        }
    }

    #[test]
    fn test_or_impact_level1_window_skips_low_group_with_single_score() {
        let total = (IMPACT_LEVEL1_BLOCKS + 1) * BLOCK_SIZE;
        let target = (IMPACT_LEVEL1_BLOCKS * BLOCK_SIZE) as u64;
        let mut docs = DocSet::default();
        for doc_id in 0..total as u64 {
            docs.append(doc_id, 1);
        }

        let doc_ids = (0..total as u32).collect::<Vec<_>>();
        let freqs = doc_ids
            .iter()
            .map(|doc_id| if u64::from(*doc_id) < target { 1 } else { 10 })
            .collect::<Vec<_>>();
        let posting_list = generate_impact_posting_list_with_freqs(doc_ids, freqs, vec![1; total]);
        let mut probe = PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            posting_list.clone(),
            docs.len(),
        );
        probe.shallow_next(0);
        let counting_scorer = CountingScorer {
            scored: Arc::new(AtomicUsize::new(0)),
        };
        let (group_up_to, group_score) = probe.impact_group_bound(&counting_scorer).unwrap();
        assert_eq!(group_up_to, target - 1);
        assert_eq!(group_score, 1.0);
        // A window ending past the current block must answer from level1.
        assert_eq!(
            probe.window_max_score(Some(target - 1), &counting_scorer),
            1.0
        );

        let posting = PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            posting_list,
            docs.len(),
        );
        let scored = Arc::new(AtomicUsize::new(0));
        let mut wand = Wand::new(
            Operator::Or,
            std::iter::once(posting),
            &docs,
            CountingScorer {
                scored: scored.clone(),
            },
        );
        wand.threshold = 2.0;

        let (candidate, score) = wand.next().unwrap().unwrap();
        assert_eq!(candidate.doc_id(), target);
        assert_eq!(score, 10.0);
        // The doc-weight bounds bake exactly once (one doc_weight call per
        // frontier pair across all entries); beyond that only the returned
        // candidate is scored.
        let total_entries = (IMPACT_LEVEL1_BLOCKS + 1) + 2;
        assert!(
            scored.load(Ordering::Relaxed) <= total_entries + 8,
            "bounds should be baked once instead of recomputed per window; scored={}",
            scored.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn test_compressed_impact_block_max_score_memoizes_current_block() {
        let total = 2 * BLOCK_SIZE as u32;
        let doc_ids = (0..total).collect::<Vec<_>>();
        let freqs = doc_ids
            .iter()
            .map(|doc_id| if *doc_id < BLOCK_SIZE as u32 { 1 } else { 2 })
            .collect::<Vec<_>>();
        let doc_lengths = vec![1; total as usize];
        let posting_list = generate_impact_posting_list_with_freqs(doc_ids, freqs, doc_lengths);
        let mut posting =
            PostingIterator::new(String::from("term"), 0, 0, posting_list, total as usize);
        let scored = Arc::new(AtomicUsize::new(0));
        let scorer = CountingScorer {
            scored: scored.clone(),
        };

        let first_score = posting.block_max_score(&scorer);
        assert_eq!(first_score, 1.0);
        // Baking the query-local doc-weight bounds visits every frontier pair
        // once (two level0 entries plus one level1 entry for this list).
        let baked = scored.load(Ordering::Relaxed);
        assert!(baked >= 2);
        {
            let compressed = unsafe { &mut *posting.compressed_state_ptr() };
            assert_eq!(
                compressed.level0_cache,
                Some((0, BLOCK_SIZE as u32 - 1, first_score))
            );
        }

        let second_score = posting.block_max_score(&scorer);
        assert_eq!(second_score, first_score);
        assert_eq!(
            scored.load(Ordering::Relaxed),
            baked,
            "repeated block max scores must not recompute doc weights"
        );

        posting.shallow_next(BLOCK_SIZE as u64);
        let next_block_score = posting.block_max_score(&scorer);
        assert_eq!(next_block_score, 2.0);
        assert_eq!(
            scored.load(Ordering::Relaxed),
            baked,
            "other blocks answer from the baked bounds without rescoring"
        );
    }

    #[rstest]
    #[case(0.0)]
    #[case(-1.0)]
    fn test_non_positive_query_weight_skips_global_impact_bound(#[case] query_weight: f32) {
        let posting = PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            query_weight,
            generate_impact_posting_list_with_freqs(vec![0], vec![1], vec![1]),
            1,
        );
        let scored = Arc::new(AtomicUsize::new(0));
        let scorer = CountingScorer {
            scored: scored.clone(),
        };

        assert_eq!(posting.global_upper_bound(&scorer), 0.0);
        assert_eq!(scored.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_and_candidate_prune_scores_first_term_before_full_score() {
        let total_docs = 2 * BLOCK_SIZE as u32 + 1;
        let mut docs = DocSet::default();
        for doc_id in 0..total_docs {
            let doc_tokens = if doc_id == 0 { 1 } else { 1000 };
            docs.append(doc_id as u64, doc_tokens);
        }

        let first_docs = (0..2 * BLOCK_SIZE as u32).collect::<Vec<_>>();
        let second_docs = (0..total_docs).collect::<Vec<_>>();
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("a"),
                0,
                0,
                1.0,
                generate_posting_list(first_docs, 1.0, Some(vec![1.0, 0.001]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("b"),
                1,
                1,
                1.0,
                generate_posting_list(second_docs, 1.0, Some(vec![1.0, 0.001, 0.001]), true),
                docs.len(),
            ),
        ];

        let scored = Arc::new(AtomicUsize::new(0));
        let mut wand = Wand::new(
            Operator::And,
            postings.into_iter(),
            &docs,
            CountingScorer {
                scored: scored.clone(),
            },
        );

        let result = wand
            .search(
                &FtsSearchParams::new().with_limit(Some(1)),
                &NoOpMetricsCollector,
            )
            .unwrap();

        let addrs = result
            .into_iter()
            .map(|doc| doc.document)
            .collect::<Vec<_>>();
        assert_eq!(addrs, vec![0]);
        let scored = scored.load(Ordering::Relaxed);
        // The bulk path evaluates 63 doc weights up front to fill its
        // frequency-bound prune LUT; those are bound computations, not
        // per-candidate scoring.
        assert!(
            scored <= BLOCK_SIZE + 1 + 63,
            "expected candidate pruning to avoid full scoring in the first block, scored {scored}"
        );
    }

    #[test]
    fn test_and_candidate_prune_records_scoring_counters() {
        let total_docs = 2 * BLOCK_SIZE as u32 + 1;
        let mut docs = DocSet::default();
        for doc_id in 0..total_docs {
            let doc_tokens = if doc_id == 0 { 1 } else { 1000 };
            docs.append(doc_id as u64, doc_tokens);
        }

        let first_docs = (0..2 * BLOCK_SIZE as u32).collect::<Vec<_>>();
        let second_docs = (0..total_docs).collect::<Vec<_>>();
        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("a"),
                0,
                0,
                1.0,
                generate_posting_list(first_docs, 1.0, Some(vec![1.0, 0.001]), true),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("b"),
                1,
                1,
                1.0,
                generate_posting_list(second_docs, 1.0, Some(vec![1.0, 0.001, 0.001]), true),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(
            Operator::And,
            postings.into_iter(),
            &docs,
            InverseDocLengthScorer,
        );
        let metrics = CountAndSearchStats::default();
        let result = wand
            .search(&FtsSearchParams::new().with_limit(Some(1)), &metrics)
            .unwrap();

        let addrs = result
            .into_iter()
            .map(|doc| doc.document)
            .collect::<Vec<_>>();
        assert_eq!(addrs, vec![0]);

        let candidates_seen = metrics.candidates_seen.load(Ordering::Relaxed);
        let candidates_pruned_before_return = metrics
            .candidates_pruned_before_return
            .load(Ordering::Relaxed);
        let full_scores = metrics.full_scores.load(Ordering::Relaxed);
        assert_eq!(metrics.comparisons.load(Ordering::Relaxed), 1);
        assert_eq!(candidates_seen, 1);
        assert!(candidates_pruned_before_return > 0);
        assert_eq!(full_scores, 1);
        assert_eq!(metrics.freqs_collected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_and_candidate_prune_keeps_later_high_score_candidate() {
        let mut docs = DocSet::default();
        for doc_id in 0..3 {
            docs.append(doc_id, 1);
        }

        let postings = vec![
            PostingIterator::with_query_weight(
                String::from("a"),
                0,
                0,
                1.0,
                generate_posting_list_with_freqs(
                    vec![0, 1],
                    vec![10, 1],
                    10.0,
                    Some(vec![10.0]),
                    true,
                ),
                docs.len(),
            ),
            PostingIterator::with_query_weight(
                String::from("b"),
                1,
                1,
                1.0,
                generate_posting_list_with_freqs(
                    vec![0, 1, 2],
                    vec![1, 20, 1],
                    20.0,
                    Some(vec![20.0]),
                    true,
                ),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        let result = wand
            .search(
                &FtsSearchParams::new().with_limit(Some(1)),
                &NoOpMetricsCollector,
            )
            .unwrap();

        let addrs = result
            .into_iter()
            .map(|doc| doc.document)
            .collect::<Vec<_>>();
        assert_eq!(addrs, vec![1]);
    }

    #[rstest]
    fn test_wand_batches_lagging_iterators(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        for i in 0..16 {
            docs.append(i as u64, 1);
        }

        let postings = vec![
            PostingIterator::new(
                String::from("a"),
                0,
                0,
                generate_posting_list(vec![1, 10], 1.0, None, is_compressed),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("b"),
                1,
                1,
                generate_posting_list(vec![2, 10], 1.0, None, is_compressed),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("c"),
                2,
                2,
                generate_posting_list(vec![10], 1.0, None, is_compressed),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::Or, postings.into_iter(), &docs, UnitScorer);
        wand.threshold = 2.5;

        let candidate = wand.next().unwrap().unwrap();
        assert_eq!(candidate.0.doc_id(), 10);
        assert_eq!(wand.lead.len(), 3);
    }

    #[test]
    fn test_flat_search_or_keeps_masked_docs_in_same_block() {
        let mut docs = DocSet::default();
        for i in 0..=(BLOCK_SIZE as u64 + 1) {
            let doc_tokens = if i == 1 { 100 } else { 1 };
            docs.append(i, doc_tokens);
        }

        let posting = PostingIterator::with_query_weight(
            String::from("term"),
            0,
            0,
            1.0,
            generate_posting_list(
                (1..=(BLOCK_SIZE as u32 + 1)).collect(),
                1.0,
                Some(vec![1.0, 1.0]),
                true,
            ),
            docs.len(),
        );

        let mut wand = Wand::new(
            Operator::Or,
            vec![posting].into_iter(),
            &docs,
            InverseDocLengthScorer,
        );
        wand.threshold = 0.5;

        let selected = vec![(1_u64, 1_u64), (2_u64, 2_u64)];
        let result = wand
            .flat_search(
                &FtsSearchParams::default(),
                Box::new(selected.into_iter()),
                &NoOpMetricsCollector,
            )
            .unwrap();

        let matched = result
            .into_iter()
            .map(|doc| doc.document)
            .collect::<Vec<_>>();
        assert_eq!(matched, vec![2]);
    }

    #[test]
    fn test_doc_ids_resolves_every_document_a_row_owns() {
        // A list<string> column indexes each element as its own document, so
        // one row id owns several doc ids. row 100 -> {0, 1}, row 101 -> {2}.
        let row_id_col = arrow_array::UInt64Array::from(vec![100_u64, 100, 101]);
        let num_tokens_col = arrow_array::UInt32Array::from(vec![1_u32, 1, 1]);
        let docs = DocSet::from_columns(&row_id_col, &num_tokens_col, false, None).unwrap();

        assert_eq!(docs.doc_ids(100).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(docs.doc_ids(101).collect::<Vec<_>>(), vec![2]);
        assert!(docs.doc_ids(999).next().is_none());

        // legacy shape (row id == doc id) still resolves to a single document.
        let mut legacy = DocSet::default();
        legacy.append(7, 1);
        assert_eq!(legacy.doc_ids(7).collect::<Vec<_>>(), vec![7]);
        assert!(legacy.doc_ids(8).next().is_none());
    }

    #[rstest]
    fn test_flat_search_finds_list_row_with_match_at_non_last_position(
        #[values(false, true)] is_compressed: bool,
    ) {
        // row 100 owns two element-documents (doc 0, doc 1) that share its row
        // id; row 101 owns doc 2. The query term lives only in doc 0 — the
        // *non-last* element of row 100. Resolving the row to a single doc id
        // would evaluate doc 1, miss the term, and drop the row (lancedb#3352).
        let row_id_col = arrow_array::UInt64Array::from(vec![100_u64, 100, 101]);
        let num_tokens_col = arrow_array::UInt32Array::from(vec![1_u32, 1, 1]);
        let docs = DocSet::from_columns(&row_id_col, &num_tokens_col, false, None).unwrap();

        let posting = PostingIterator::with_query_weight(
            String::from("needle"),
            0,
            0,
            1.0,
            generate_posting_list(vec![0], 1.0, None, is_compressed),
            docs.len(),
        );

        let mut wand = Wand::new(
            Operator::Or,
            vec![posting].into_iter(),
            &docs,
            InverseDocLengthScorer,
        );
        wand.threshold = 0.5;

        let selected = docs
            .doc_ids(100)
            .map(|doc_id| (doc_id, 100_u64))
            .collect::<Vec<_>>();
        let result = wand
            .flat_search(
                &FtsSearchParams::default(),
                Box::new(selected.into_iter()),
                &NoOpMetricsCollector,
            )
            .unwrap();

        // The legacy adapter resolves the prefilter to every owned document,
        // while the candidate identity remains row 100.
        let addrs = result
            .into_iter()
            .map(|doc| doc.document)
            .collect::<Vec<_>>();
        assert!(
            addrs.as_slice() == [100],
            "expected exactly row 100, got {addrs:?}"
        );
    }

    #[test]
    fn test_block_max_score_matches_stored_value() {
        let doc_ids = vec![0_u32];
        let block_max_scores = vec![0.7_f32];
        let posting_list = generate_posting_list(doc_ids, 0.7, Some(block_max_scores), true);
        let expected = match &posting_list {
            PostingList::Compressed(list) => list.block_max_score(0),
            PostingList::Plain(_) => unreachable!("expected compressed posting list"),
        };

        let posting = PostingIterator::new(String::from("test"), 0, 0, posting_list, 1);

        let actual = posting.block_max_score(&UnitScorer);
        assert!(
            (actual - expected).abs() < 1e-6,
            "block max score should match stored value"
        );
    }

    #[test]
    fn test_modern_doc_id_validation_checks_layout_and_upper_bound() {
        let compressed = generate_posting_list(vec![0, 4], 1.0, None, true);
        let posting = PostingIterator::new(String::from("term"), 0, 0, compressed, 5);
        posting
            .validate_modern_doc_ids(5)
            .expect("largest DocId is inside the document table");
        let error = posting.validate_modern_doc_ids(4).unwrap_err();
        assert!(error.to_string().contains("DocId 4"));
        assert!(error.to_string().contains("[0, 4)"));

        let plain = generate_posting_list(vec![0], 1.0, None, false);
        let posting = PostingIterator::new(String::from("legacy"), 0, 0, plain, 1);
        let error = posting.validate_modern_doc_ids(1).unwrap_err();
        assert!(error.to_string().contains("legacy row-address layout"));
    }

    #[test]
    fn test_256_document_blocks_without_impacts_use_conservative_quantized_score_bound() {
        let exact_doc_length = 300;
        let quantized_doc_length = super::super::index::dequantize_doc_length(
            super::super::index::quantize_doc_length(exact_doc_length),
        );
        assert!(quantized_doc_length < exact_doc_length);

        let scorer = Arc::new(MemBM25Scorer::new(100, 1, Default::default()));
        let stored_exact_score = scorer.doc_weight(1, exact_doc_length);
        let quantized_score = scorer.doc_weight(1, quantized_doc_length);
        assert!(quantized_score > stored_exact_score);

        let doc_ids = [0_u32];
        let frequencies = [1_u32];
        let blocks = compress_posting_list_with_tail_codec_and_block_size(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(stored_exact_score),
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            MAX_POSTING_BLOCK_SIZE,
        )
        .unwrap();
        let posting_list = PostingList::Compressed(CompressedPostingList::new(
            blocks,
            stored_exact_score,
            doc_ids.len() as u32,
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            MAX_POSTING_BLOCK_SIZE,
            None,
            None,
        ));
        let posting = PostingIterator::new(String::from("term"), 0, 0, posting_list, doc_ids.len());
        let expected_bound = K1 + 1.0;

        assert_eq!(posting.approximate_upper_bound(), expected_bound);
        assert_eq!(posting.global_upper_bound(&scorer), expected_bound);
        assert_eq!(posting.block_max_score(&scorer), expected_bound);
        assert_eq!(
            posting.block_max_score_up_to_with_stats(0, &scorer).score,
            expected_bound
        );
        assert!(expected_bound >= quantized_score);
    }

    #[test]
    fn test_256_document_blocks_without_impacts_unknown_scorer_uses_infinite_bound() {
        let doc_ids = [0_u32];
        let frequencies = [10_u32];
        let blocks = compress_posting_list_with_tail_codec_and_block_size(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(10.0),
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            MAX_POSTING_BLOCK_SIZE,
        )
        .unwrap();
        let posting_list = PostingList::Compressed(CompressedPostingList::new(
            blocks,
            10.0,
            doc_ids.len() as u32,
            crate::scalar::inverted::PostingTailCodec::VarintDelta,
            MAX_POSTING_BLOCK_SIZE,
            None,
            None,
        ));
        let posting = PostingIterator::new(String::from("term"), 0, 0, posting_list, doc_ids.len());

        assert!(posting.global_upper_bound(&UnitScorer).is_infinite());
        assert!(posting.block_max_score(&UnitScorer).is_infinite());
        assert!(
            posting
                .block_max_score_up_to_with_stats(0, &UnitScorer)
                .score
                .is_infinite()
        );
    }

    #[rstest]
    fn test_exact_phrase_with_repeated_terms(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        docs.append(0, 16);

        let token_a_positions = vec![vec![1_u32, 3, 10]];
        let token_b_positions = vec![vec![2_u32, 11]];
        let postings = vec![
            PostingIterator::new(
                String::from("a"),
                0,
                0,
                generate_posting_list_with_positions(
                    vec![0],
                    token_a_positions.clone(),
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("b"),
                1,
                1,
                generate_posting_list_with_positions(
                    vec![0],
                    token_b_positions,
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("a"),
                2,
                2,
                generate_posting_list_with_positions(
                    vec![0],
                    token_a_positions,
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
        ];

        let bm25 = IndexBM25Scorer::new(std::iter::empty());
        let wand = Wand::new(Operator::And, postings.into_iter(), &docs, bm25);
        assert!(wand.check_exact_positions().unwrap());
        assert!(wand.check_positions(0).unwrap());
    }

    #[rstest]
    fn test_exact_phrase_respects_query_position_gaps(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        docs.append(0, 16);

        let postings = vec![
            PostingIterator::new(
                String::from("want"),
                0,
                0,
                generate_posting_list_with_positions(
                    vec![0],
                    vec![vec![0_u32]],
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("apple"),
                1,
                2,
                generate_posting_list_with_positions(
                    vec![0],
                    vec![vec![2_u32]],
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
        ];

        let bm25 = IndexBM25Scorer::new(std::iter::empty());
        let wand = Wand::new(Operator::And, postings.into_iter(), &docs, bm25);
        assert!(wand.check_exact_positions().unwrap());
        assert!(wand.check_positions(0).unwrap());
    }

    #[rstest]
    fn test_and_phrase_miss_advances_to_next_candidate(#[values(false, true)] is_compressed: bool) {
        let mut docs = DocSet::default();
        docs.append(0, 8);
        docs.append(1, 8);

        let postings = vec![
            PostingIterator::new(
                String::from("a"),
                0,
                0,
                generate_posting_list_with_positions(
                    vec![0, 1],
                    vec![vec![1_u32], vec![10_u32]],
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
            PostingIterator::new(
                String::from("b"),
                1,
                1,
                generate_posting_list_with_positions(
                    vec![0, 1],
                    vec![vec![3_u32], vec![11_u32]],
                    1.0,
                    is_compressed,
                ),
                docs.len(),
            ),
        ];

        let mut wand = Wand::new(Operator::And, postings.into_iter(), &docs, UnitScorer);
        let first = wand.next().unwrap().unwrap();
        assert_eq!(first.0.doc_id(), 0);
        assert!(!wand.check_positions(0).unwrap());

        wand.threshold = 1.5;
        let second = wand.next().unwrap().unwrap();
        assert_eq!(second.0.doc_id(), 1);
        assert!(wand.check_positions(0).unwrap());
    }

    /// The bulk conjunction path must return exactly the classic loop's
    /// results — same docs, freqs, and doc lengths — for both plain AND and
    /// phrase queries, across multi-block lists with heap/threshold pruning
    /// in play.
    #[rstest]
    #[case::and_k10(false, 0, 10, 3)]
    #[case::and_k3(false, 0, 3, 3)]
    #[case::and_two_clauses(false, 0, 10, 2)]
    #[case::and_four_clauses(false, 0, 10, 4)]
    #[case::and_five_clauses(false, 0, 10, 5)]
    #[case::and_six_clauses(false, 0, 10, 6)]
    #[case::phrase_k10(true, 0, 10, 3)]
    #[case::phrase_k3(true, 0, 3, 3)]
    #[case::phrase_slop_three(true, 3, 10, 3)]
    #[case::phrase_two_clauses(true, 0, 10, 2)]
    #[case::phrase_four_clauses(true, 0, 10, 4)]
    #[case::phrase_five_clauses(true, 0, 10, 5)]
    #[case::phrase_six_clauses(true, 0, 10, 6)]
    fn test_bulk_and_matches_classic(
        #[case] phrase: bool,
        #[case] slop: u32,
        #[case] limit: usize,
        #[case] num_clauses: usize,
    ) {
        let num_docs = (BLOCK_SIZE * 8 + 37) as u32;
        let mut docs = DocSet::default();
        for doc_id in 0..num_docs {
            docs.append(u64::from(doc_id), 32 + doc_id % 57);
        }

        // Clauses with different densities; membership comes from a cheap
        // deterministic mix so docs scatter across blocks. Clause count picks
        // the dedicated (1-3) or generic (4+) merge kernel.
        let clause_docs = |modulus: u32, salt: u32| -> Vec<u32> {
            (0..num_docs)
                .filter(|doc| (doc.wrapping_mul(2654435761).wrapping_add(salt)) % modulus < 2)
                .collect()
        };
        let clauses = [
            clause_docs(3, 7),
            clause_docs(4, 13),
            clause_docs(5, 29),
            clause_docs(3, 41),
            clause_docs(3, 7),
            clause_docs(4, 13),
        ][..num_clauses]
            .to_vec();

        let build_postings = || {
            clauses
                .iter()
                .enumerate()
                .map(|(term_pos, doc_ids)| {
                    let list = if phrase {
                        // Roughly half of each clause's docs put the token at
                        // position base+term_pos (forming the phrase); the
                        // rest scatter so the position check has misses.
                        let positions = doc_ids
                            .iter()
                            .map(|&doc| {
                                if doc % 2 == 0 {
                                    vec![5 + term_pos as u32, 40 + (doc % 3)]
                                } else {
                                    vec![20 + (term_pos as u32) * 4]
                                }
                            })
                            .collect::<Vec<_>>();
                        generate_posting_list_with_positions(doc_ids.clone(), positions, 8.0, true)
                    } else {
                        generate_posting_list(doc_ids.clone(), 8.0, None, true)
                    };
                    PostingIterator::with_query_weight(
                        format!("t{term_pos}"),
                        term_pos as u32,
                        term_pos as u32,
                        1.0 + term_pos as f32 * 0.5,
                        list,
                        docs.len(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut params = FtsSearchParams::default().with_limit(Some(limit));
        if phrase {
            params.phrase_slop = Some(slop);
        }

        let normalize = |result: Vec<DocCandidate<u64>>| {
            let mut rows = result
                .into_iter()
                .map(|candidate| {
                    (
                        candidate.posting_doc_id,
                        candidate.doc_length,
                        candidate.freqs,
                        candidate.document,
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_unstable();
            rows
        };

        let run = |mode| {
            let mut wand = Wand::new(
                Operator::And,
                build_postings().into_iter(),
                &docs,
                UnitScorer,
            )
            .with_bulk_and_mode(mode);
            let rows = normalize(wand.search(&params, &NoOpMetricsCollector).unwrap());
            let used_bulk = wand.bulk_and_searches > 0;
            (rows, used_bulk)
        };

        let (bulk, bulk_used) = run(BulkAndMode::On);
        let (classic, classic_used) = run(BulkAndMode::Off);
        let (auto, auto_used) = run(BulkAndMode::Auto);
        assert!(bulk_used, "on should use bulk conjunction search");
        assert!(!classic_used, "off should use classic conjunction search");
        assert_eq!(auto_used, matches!(num_clauses, 2 | 3));
        assert!(!bulk.is_empty(), "test corpus should produce matches");
        assert_eq!(bulk, classic);
        assert_eq!(auto, classic);
    }
}
