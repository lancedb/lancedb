// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! In-memory Full-Text Search (FTS) index with single-writer / multi-reader
//! semantics.
//!
//! # Concurrency model
//!
//! - **One writer** (`insert` / `insert_with_batch_position`) at a time per
//!   index. Callers are responsible for that invariant; this is consistent
//!   with `IndexStore`'s usage from `ShardWriter`.
//! - **Many readers** (`search*`, `expand_fuzzy`, `to_index_builder`)
//!   in parallel with the writer. Reads are lock-free aside from a brief
//!   tokenizer-pool checkout.
//! - **Per-batch monotonic visibility**: a reader either sees every row of
//!   batch `b` or none of them. A reader never sees a batch numbered above
//!   the published `visible_count`. BM25 statistics observed by a reader
//!   (`doc_count`, `total_tokens`, per-term `df`) are mutually consistent
//!   with the postings the reader walks.
//!
//! # Partition-structured layout
//!
//! The index is a set of immutable [`Partition`]s plus a bounded mutable
//! `tail` ([`TailIndex`]). Each partition holds a frozen slice of inserts as
//! on-disk-shaped posting lists and is queried with block-max WAND in
//! ≈O(matches). The tail accumulates recent inserts in a per-batch-chunk
//! layout and is searched in place; the writer freezes it into a new
//! partition once it crosses `freeze_threshold_rows`. The `{partitions, tail}`
//! pair lives behind one [`ArcSwap`] ([`IndexState`]) so a freeze publishes
//! atomically — a reader never sees a doc twice or misses one across a
//! freeze.
//!
//! Within the tail, visibility is published atomically by replacing a single
//! `Snapshot` via `ArcSwap`. The writer first installs all term chunks into
//! the per-term `ArcSwap<TermSlice>` slots, then atomically swaps in a new
//! `Snapshot` whose `visible_count` covers the new batch. Readers load the
//! `Snapshot` first and filter every term chunk by `batch_position <
//! snapshot.visible_count`. Frozen partitions are fully visible by
//! construction.
//!
//! # On-disk format
//!
//! At flush time we hand off to `lance_index::scalar::inverted::builder::InnerBuilder`
//! via `to_index_builder`, which merges every partition and the tail
//! into one builder. The on-disk format is unchanged from Lance's existing
//! inverted index.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arc_swap::ArcSwap;
use arrow_array::RecordBatch;
use crossbeam_skiplist::SkipMap;
use fst::{Map, Streamer};
use lance_bitpacking::{BitPacker, BitPacker4x};
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_index::scalar::InvertedIndexParams;
use lance_index::scalar::inverted::query::{Operator, Tokens};
use lance_index::scalar::inverted::tokenizer::document_tokenizer::{DocType, LanceTokenizer};
use lance_index::scalar::inverted::{DocSet, MemBM25Scorer, Scorer, TokenSet};
use lance_tokenizer::TokenStream;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use super::RowPosition;
use crate::index::scalar::inverted::{ResolvedFtsField, resolve_fts_field};

// ============================================================================
// Public types preserved from previous API
// ============================================================================

/// In-memory FTS index entry returned from search.
#[derive(Debug, Clone)]
pub struct FtsEntry {
    /// Row position in MemTable.
    pub row_position: RowPosition,
    /// Root-to-leaf physical list ordinals for a ListElement document.
    pub doc_index: Option<Vec<u32>>,
    /// BM25 score for this document.
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct DocumentKey {
    row_position: RowPosition,
    doc_index: Vec<u32>,
}

impl FtsEntry {
    fn key(&self) -> DocumentKey {
        DocumentKey {
            row_position: self.row_position,
            doc_index: self.doc_index.clone().unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
struct DocumentMetadata {
    key: DocumentKey,
    num_tokens: u32,
}

fn doc_set_key(docs: &DocSet, doc_id: u32) -> DocumentKey {
    DocumentKey {
        row_position: docs.row_id(doc_id),
        doc_index: (0..docs.coordinate_rank())
            .map(|rank| docs.coordinate(doc_id, rank))
            .collect(),
    }
}

fn public_doc_index(coordinates: &[u32]) -> Option<Vec<u32>> {
    (!coordinates.is_empty()).then(|| coordinates.to_vec())
}

/// Full-text search query expression for composable queries.
#[derive(Debug, Clone)]
pub enum FtsQueryExpr {
    /// Simple term match query.
    Match {
        /// The search query string.
        query: String,
        /// The operator used to combine tokenized query terms.
        operator: Operator,
        /// Boost factor applied to the score (default 1.0).
        boost: f32,
    },
    /// Phrase query with optional slop.
    Phrase {
        /// The phrase to search for.
        query: String,
        /// Maximum allowed distance between consecutive tokens.
        slop: u32,
        /// Boost factor applied to the score (default 1.0).
        boost: f32,
    },
    /// Fuzzy match query with typo tolerance.
    Fuzzy {
        /// The search query string.
        query: String,
        /// Maximum edit distance (Levenshtein distance).
        /// `None` means auto-fuzziness based on token length.
        fuzziness: Option<u32>,
        /// Number of initial characters that must match exactly.
        prefix_length: u32,
        /// Maximum number of terms to expand to.
        max_expansions: usize,
        /// Boost factor applied to the score (default 1.0).
        boost: f32,
    },
    /// Boolean combination of queries.
    Boolean {
        /// All MUST clauses must match and contribute to the score.
        must: Vec<Self>,
        /// At least one SHOULD clause should match (adds to score).
        should: Vec<Self>,
        /// No MUST_NOT clause may match (excludes documents).
        must_not: Vec<Self>,
    },
    /// Boosting query with a positive and an optional negative component.
    Boost {
        /// Documents must match this query.
        positive: Box<Self>,
        /// Optional query whose matches are demoted.
        negative: Option<Box<Self>>,
        /// Multiplier applied to documents matching `negative` (typically
        /// `< 1.0` to demote).
        negative_boost: f32,
    },
}

/// Default maximum number of fuzzy expansions.
pub const DEFAULT_MAX_EXPANSIONS: usize = 50;

/// Default WAND factor for full recall (no early termination).
pub const DEFAULT_WAND_FACTOR: f32 = 1.0;

/// Search options for controlling performance/recall tradeoffs.
#[derive(Debug, Clone)]
pub struct SearchOptions {
    /// WAND factor for early termination (0.0 to 1.0).
    /// 1.0 = full recall (default).
    pub wand_factor: f32,
    /// Maximum number of results to return. None means unlimited.
    pub limit: Option<usize>,
    /// Whether to also search the mutable tail (rows written since the last
    /// freeze). `true` (default) is read-your-writes: every completed batch is
    /// visible, at the cost of scanning the un-indexed tail on each query.
    /// `false` searches only the immutable frozen partitions — the Lucene
    /// model (a reader sees only flushed segments), which removes the tail
    /// scan from the hot path. Trades read-recency for query latency.
    pub include_tail: bool,
    /// Whether to skip the whole tail scan when its score upper bound cannot
    /// beat the current top-k threshold (block-max-style tail pruning). `true`
    /// (default) prunes; `false` always scans the visible tail. Only affects
    /// top-k term queries; exposed mainly to A/B the optimization.
    pub tail_skip: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            wand_factor: DEFAULT_WAND_FACTOR,
            limit: None,
            include_tail: true,
            tail_skip: true,
        }
    }
}

impl SearchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the WAND factor for early termination.
    pub fn with_wand_factor(mut self, wand_factor: f32) -> Self {
        self.wand_factor = wand_factor.clamp(0.0, 1.0);
        self
    }

    /// Set the maximum number of results to return.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set whether to search the mutable tail (see [`Self::include_tail`]).
    pub fn with_include_tail(mut self, include_tail: bool) -> Self {
        self.include_tail = include_tail;
        self
    }

    /// Set whether to prune the tail scan by score bound (see [`Self::tail_skip`]).
    pub fn with_tail_skip(mut self, tail_skip: bool) -> Self {
        self.tail_skip = tail_skip;
        self
    }
}

impl FtsQueryExpr {
    pub fn match_query(query: impl Into<String>) -> Self {
        Self::match_query_with_operator(query, Operator::Or)
    }

    pub fn match_query_with_operator(query: impl Into<String>, operator: Operator) -> Self {
        Self::Match {
            query: query.into(),
            operator,
            boost: 1.0,
        }
    }

    pub fn phrase(query: impl Into<String>) -> Self {
        Self::Phrase {
            query: query.into(),
            slop: 0,
            boost: 1.0,
        }
    }

    pub fn phrase_with_slop(query: impl Into<String>, slop: u32) -> Self {
        Self::Phrase {
            query: query.into(),
            slop,
            boost: 1.0,
        }
    }

    pub fn fuzzy(query: impl Into<String>) -> Self {
        Self::Fuzzy {
            query: query.into(),
            fuzziness: None,
            prefix_length: 0,
            max_expansions: DEFAULT_MAX_EXPANSIONS,
            boost: 1.0,
        }
    }

    pub fn fuzzy_with_distance(query: impl Into<String>, fuzziness: u32) -> Self {
        Self::Fuzzy {
            query: query.into(),
            fuzziness: Some(fuzziness),
            prefix_length: 0,
            max_expansions: DEFAULT_MAX_EXPANSIONS,
            boost: 1.0,
        }
    }

    pub fn fuzzy_with_options(
        query: impl Into<String>,
        fuzziness: Option<u32>,
        prefix_length: u32,
        max_expansions: usize,
    ) -> Self {
        Self::Fuzzy {
            query: query.into(),
            fuzziness,
            prefix_length,
            max_expansions,
            boost: 1.0,
        }
    }

    pub fn boolean() -> BooleanQueryBuilder {
        BooleanQueryBuilder::new()
    }

    pub fn boosting(positive: Self) -> Self {
        Self::Boost {
            positive: Box::new(positive),
            negative: None,
            negative_boost: 1.0,
        }
    }

    pub fn boosting_with_negative(positive: Self, negative: Self, negative_boost: f32) -> Self {
        Self::Boost {
            positive: Box::new(positive),
            negative: Some(Box::new(negative)),
            negative_boost,
        }
    }

    pub fn with_boost(self, boost: f32) -> Self {
        match self {
            Self::Match {
                query, operator, ..
            } => Self::Match {
                query,
                operator,
                boost,
            },
            Self::Phrase { query, slop, .. } => Self::Phrase { query, slop, boost },
            Self::Fuzzy {
                query,
                fuzziness,
                prefix_length,
                max_expansions,
                ..
            } => Self::Fuzzy {
                query,
                fuzziness,
                prefix_length,
                max_expansions,
                boost,
            },
            // Boolean and Boost don't carry a top-level boost field today.
            // Preserved as-is to keep behavior identical to the previous impl.
            other @ (Self::Boolean { .. } | Self::Boost { .. }) => other,
        }
    }
}

/// Auto-fuzziness based on token length:
/// 0–2 chars → 0, 3–5 chars → 1, 6+ chars → 2.
pub fn auto_fuzziness(token: &str) -> u32 {
    match token.chars().count() {
        0..=2 => 0,
        3..=5 => 1,
        _ => 2,
    }
}

/// Levenshtein distance using two-row dynamic programming.
pub fn levenshtein_distance(a: &str, b: &str) -> u32 {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let m = a_chars.len();
    let n = b_chars.len();

    if m == 0 {
        return n as u32;
    }
    if n == 0 {
        return m as u32;
    }

    let mut prev_row: Vec<u32> = (0..=n as u32).collect();
    let mut curr_row: Vec<u32> = vec![0; n + 1];

    for (i, a_char) in a_chars.iter().enumerate() {
        curr_row[0] = (i + 1) as u32;
        for (j, b_char) in b_chars.iter().enumerate() {
            let cost = if a_char == b_char { 0 } else { 1 };
            curr_row[j + 1] = (prev_row[j + 1] + 1)
                .min(curr_row[j] + 1)
                .min(prev_row[j] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[n]
}

fn char_prefix(term: &str, prefix_length: u32) -> &str {
    if prefix_length == 0 {
        return "";
    }
    term.char_indices()
        .nth(prefix_length as usize)
        .map(|(idx, _)| &term[..idx])
        .unwrap_or(term)
}

fn query_tokens_to_vec(tokens: &Tokens) -> Vec<String> {
    (0..tokens.len())
        .map(|idx| tokens.get_token(idx).to_string())
        .collect()
}

fn has_grouped_positions(tokens: &Tokens) -> bool {
    let mut seen = HashSet::new();
    (0..tokens.len()).any(|idx| !seen.insert(tokens.position(idx)))
}

fn query_position_groups(tokens: &Tokens) -> Vec<Vec<String>> {
    let mut groups = Vec::new();
    let mut current_position = None;
    for idx in 0..tokens.len() {
        let position = tokens.position(idx);
        if current_position != Some(position) {
            current_position = Some(position);
            groups.push(Vec::new());
        }
        let group = groups
            .last_mut()
            .expect("a group should exist after pushing for position");
        let token = tokens.get_token(idx).to_string();
        if !group.contains(&token) {
            group.push(token);
        }
    }
    groups
}

fn position_groups_to_tokens(groups: &[Vec<String>]) -> Vec<String> {
    groups.iter().flatten().cloned().collect()
}

/// Builder for constructing Boolean queries.
#[derive(Debug, Clone, Default)]
pub struct BooleanQueryBuilder {
    must: Vec<FtsQueryExpr>,
    should: Vec<FtsQueryExpr>,
    must_not: Vec<FtsQueryExpr>,
}

impl BooleanQueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn must(mut self, query: FtsQueryExpr) -> Self {
        self.must.push(query);
        self
    }

    pub fn should(mut self, query: FtsQueryExpr) -> Self {
        self.should.push(query);
        self
    }

    pub fn must_not(mut self, query: FtsQueryExpr) -> Self {
        self.must_not.push(query);
        self
    }

    pub fn build(self) -> FtsQueryExpr {
        FtsQueryExpr::Boolean {
            must: self.must,
            should: self.should,
            must_not: self.must_not,
        }
    }
}

// ============================================================================
// Internal types
// ============================================================================

/// Compressed sparse row (CSR) layout for per-document positions.
///
/// `offsets[i]..offsets[i+1]` is the slice of `data` belonging to the i-th
/// document in this `TermChunk`. `offsets.len() == row_positions.len() + 1`.
#[derive(Debug)]
struct Positions {
    offsets: Vec<u32>,
    data: Vec<u32>,
}

impl Positions {
    fn doc_positions(&self, doc_idx: usize) -> &[u32] {
        let start = self.offsets[doc_idx] as usize;
        let end = self.offsets[doc_idx + 1] as usize;
        &self.data[start..end]
    }

    fn memory_size(&self) -> usize {
        self.offsets.capacity() * std::mem::size_of::<u32>()
            + self.data.capacity() * std::mem::size_of::<u32>()
    }
}

/// Postings produced by a single batch insert for a single term.
///
/// SoA layout for cache locality and trivial handoff to
/// `PostingListBuilder`. `row_positions` is sorted ascending.
#[derive(Debug)]
struct TermChunk {
    batch_position: usize,
    row_positions: Vec<u64>,
    frequencies: Vec<u32>,
    /// Largest frequency in the chunk; `doc_weight(max_freq, 1)` bounds any
    /// doc's tf-component, used to skip the whole tail scan when it cannot beat
    /// the top-k threshold.
    max_freq: u32,
    /// Per-doc token positions. Always `Some` in the current
    /// implementation (the writer tracks positions unconditionally so
    /// in-memory phrase queries work even when `params.has_positions()`
    /// is false). Kept as `Option` for forward compatibility with a
    /// future "no-positions" mode that drops the allocation.
    positions: Option<Positions>,
}

impl TermChunk {
    fn doc_count(&self) -> usize {
        self.row_positions.len()
    }

    fn memory_size(&self) -> usize {
        let base = std::mem::size_of::<Self>()
            + self.row_positions.capacity() * std::mem::size_of::<u64>()
            + self.frequencies.capacity() * std::mem::size_of::<u32>();
        base + self.positions.as_ref().map_or(0, Positions::memory_size)
    }
}

/// Append-only list of `TermChunk`s for a single term, replaced atomically
/// via `ArcSwap`.
#[derive(Debug, Default)]
/// One term's tail postings as a persistent singly-linked list of chunks,
/// newest first. Appending shares the existing list (the new head points at the
/// old slice), so a reader holding an older `Arc<TermSlice>` keeps a consistent
/// view while the writer extends it — and append is O(1) instead of the O(n)
/// copy-per-append that made tail inserts quadratic in batches-per-generation.
/// Order is newest-first; every consumer either sorts by doc id or is
/// order-agnostic.
struct TermSlice {
    /// This node's chunk; `None` only for the empty root.
    chunk: Option<Arc<TermChunk>>,
    /// The slice before this chunk was appended (older chunks).
    prev: Option<Arc<Self>>,
}

impl TermSlice {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            chunk: None,
            prev: None,
        })
    }

    /// O(1) append: a fresh head holding `chunk` and linking to `prev`. `prev`
    /// is left unchanged so any reader holding it sees a consistent state.
    fn push(prev: Arc<Self>, chunk: Arc<TermChunk>) -> Arc<Self> {
        Arc::new(Self {
            chunk: Some(chunk),
            prev: Some(prev),
        })
    }

    /// Iterate the term's chunks, newest first.
    fn chunks(&self) -> TermChunkIter<'_> {
        TermChunkIter { cur: Some(self) }
    }

    fn memory_size(&self) -> usize {
        // Each node: the struct itself plus its chunk's payload.
        self.chunks()
            .map(|c| std::mem::size_of::<Self>() + c.memory_size())
            .sum::<usize>()
            + std::mem::size_of::<Self>() // empty root node
    }
}

/// Newest-first iterator over a [`TermSlice`] cons-list.
struct TermChunkIter<'a> {
    cur: Option<&'a TermSlice>,
}

impl<'a> Iterator for TermChunkIter<'a> {
    type Item = &'a Arc<TermChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        // Walk links; the empty root has no chunk and ends iteration.
        while let Some(node) = self.cur {
            self.cur = node.prev.as_deref();
            if let Some(chunk) = &node.chunk {
                return Some(chunk);
            }
        }
        None
    }
}

/// Per-batch document metadata.
#[derive(Debug)]
struct BatchMeta {
    batch_position: usize,
    /// Dense tail-local document position of `documents[0]`.
    document_position_start: u64,
    documents: Vec<DocumentMetadata>,
}

impl BatchMeta {
    fn document(&self, document_position: u64) -> Option<&DocumentMetadata> {
        if document_position < self.document_position_start {
            return None;
        }
        let idx = (document_position - self.document_position_start) as usize;
        self.documents.get(idx)
    }

    fn dl(&self, document_position: u64) -> Option<u32> {
        self.document(document_position).map(|doc| doc.num_tokens)
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.documents.capacity() * std::mem::size_of::<DocumentMetadata>()
    }
}

/// Size of a sealed batch block. Small enough that copying the partial tail
/// block on append stays cheap; large enough that sealing (which clones the
/// block-pointer vec) is rare.
const BATCH_BLOCK: usize = 64;

/// Append-only, structurally-shared log of visible batch metadata. Sealed full
/// blocks are immutable and shared across snapshots; only the current partial
/// block is copied on append, so publishing a batch is amortized O(1) (instead
/// of copying every batch pointer per publish — O(batches²) per generation)
/// while keeping O(1) index for `batch_for`.
#[derive(Debug, Clone)]
struct BatchLog {
    /// Immutable full blocks (each `BATCH_BLOCK` long), shared across snapshots.
    sealed: Arc<Vec<Arc<[Arc<BatchMeta>]>>>,
    /// The current partial block (`< BATCH_BLOCK` entries).
    tail: Arc<[Arc<BatchMeta>]>,
    len: usize,
}

impl BatchLog {
    fn empty() -> Self {
        Self {
            sealed: Arc::new(Vec::new()),
            tail: Arc::from(Vec::<Arc<BatchMeta>>::new().into_boxed_slice()),
            len: 0,
        }
    }

    /// A new log with `meta` appended; shares every sealed block with `self`,
    /// copying only the partial tail block.
    fn pushed(&self, meta: Arc<BatchMeta>) -> Self {
        let mut tail: Vec<Arc<BatchMeta>> = self.tail.to_vec();
        tail.push(meta);
        if tail.len() == BATCH_BLOCK {
            let mut sealed = (*self.sealed).clone();
            sealed.push(Arc::from(tail.into_boxed_slice()));
            Self {
                sealed: Arc::new(sealed),
                tail: Arc::from(Vec::<Arc<BatchMeta>>::new().into_boxed_slice()),
                len: self.len + 1,
            }
        } else {
            Self {
                sealed: Arc::clone(&self.sealed),
                tail: Arc::from(tail.into_boxed_slice()),
                len: self.len + 1,
            }
        }
    }

    fn get(&self, i: usize) -> Option<&Arc<BatchMeta>> {
        let block = i / BATCH_BLOCK;
        let off = i % BATCH_BLOCK;
        match self.sealed.get(block) {
            Some(b) => b.get(off),
            None if block == self.sealed.len() => self.tail.get(off),
            None => None,
        }
    }

    fn iter(&self) -> impl Iterator<Item = &Arc<BatchMeta>> {
        self.sealed
            .iter()
            .flat_map(|b| b.iter())
            .chain(self.tail.iter())
    }
}

/// Atomic snapshot of the visible state. Replaced via `ArcSwap` after each
/// batch is fully linked.
#[derive(Debug)]
struct Snapshot {
    /// Number of batches visible to readers. `0` means empty index.
    visible_count: usize,
    /// Visible-batch metadata in publish order. `batches.len() ==
    /// visible_count` for any snapshot the writer has stored (each publish
    /// appends one entry and bumps `visible_count`).
    batches: BatchLog,
    /// `Σ batches[i].documents.len()` for `i < visible_count`.
    cumulative_doc_count: u64,
    /// `Σ batches[i].documents[*].num_tokens` for `i < visible_count`.
    cumulative_total_tokens: u64,
}

impl Snapshot {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            visible_count: 0,
            batches: BatchLog::empty(),
            cumulative_doc_count: 0,
            cumulative_total_tokens: 0,
        })
    }

    fn batch_for(&self, batch_position: usize) -> Option<&Arc<BatchMeta>> {
        // Fast path: batch_position equals the index (true when callers use the
        // no-arg `insert()` and let the index assign sequential positions). With
        // explicit, possibly sparse positions, fall back to a linear search.
        self.batches
            .get(batch_position)
            .filter(|m| m.batch_position == batch_position)
            .or_else(|| {
                self.batches
                    .iter()
                    .find(|m| m.batch_position == batch_position)
            })
    }
}

/// Bounded pool of reader tokenizers with a writer-dedicated slot.
///
/// `LanceTokenizer::token_stream_for_*` takes `&mut self`, so each concurrent
/// caller needs its own tokenizer instance. Builds are cheap for English but
/// can load dictionaries for CJK tokenizers, so we amortize via the pool.
struct TokenizerPool {
    template: Box<dyn LanceTokenizer>,
    free: Mutex<Vec<Box<dyn LanceTokenizer>>>,
    cap: usize,
}

impl std::fmt::Debug for TokenizerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenizerPool")
            .field("cap", &self.cap)
            .finish()
    }
}

impl TokenizerPool {
    fn new(params: &InvertedIndexParams, cap: usize) -> Result<Self> {
        let template = params.build()?;
        Ok(Self {
            template,
            free: Mutex::new(Vec::new()),
            cap: cap.max(1),
        })
    }

    /// Acquire a tokenizer. Pops from the free list, otherwise clones the
    /// template.
    fn acquire(&self) -> Box<dyn LanceTokenizer> {
        if let Some(t) = self.free.lock().ok().and_then(|mut g| g.pop()) {
            return t;
        }
        self.template.box_clone()
    }

    /// Return a tokenizer to the pool, dropping it if the pool is at cap.
    fn release(&self, tokenizer: Box<dyn LanceTokenizer>) {
        if let Ok(mut g) = self.free.lock()
            && g.len() < self.cap
        {
            g.push(tokenizer);
        }
    }
}

/// RAII guard that returns the tokenizer to the pool on drop.
struct PooledTokenizer<'a> {
    pool: &'a TokenizerPool,
    inner: Option<Box<dyn LanceTokenizer>>,
}

impl<'a> PooledTokenizer<'a> {
    fn new(pool: &'a TokenizerPool) -> Self {
        Self {
            pool,
            inner: Some(pool.acquire()),
        }
    }

    fn get_mut(&mut self) -> &mut dyn LanceTokenizer {
        self.inner.as_mut().expect("tokenizer in scope").as_mut()
    }
}

impl<'a> Drop for PooledTokenizer<'a> {
    fn drop(&mut self) {
        if let Some(t) = self.inner.take() {
            self.pool.release(t);
        }
    }
}

// ============================================================================
// FtsMemIndex
// ============================================================================

/// In-memory full-text search index. See module docs for the concurrency
/// model and visibility contract.
/// The bounded mutable accumulator: recent inserts since the last freeze, in
/// the per-batch-chunk layout. Searched in place; frozen into a [`Partition`]
/// once it crosses the freeze threshold.
struct TailIndex {
    /// Per-term posting slices. `Arc<str>` interns the term so a single
    /// allocation backs every chunk that mentions it.
    terms: SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    /// Atomically-swapped visibility snapshot.
    snapshot: ArcSwap<Snapshot>,
    /// Strictly-monotonic, dense, 0-based batch position counter. Dense
    /// positions are required by the `batch_position < visible_count`
    /// visibility filter; the tail therefore assigns its own.
    next_batch_position: AtomicUsize,
    /// Writer-only fast path: term -> its posting slot. Only a term's *first*
    /// appearance in this tail generation pays the sorted `SkipMap` lookup;
    /// later batches reuse the cached handle, so the per-batch cost is a flat
    /// hash probe instead of a skiplist search. Reset implicitly when the tail
    /// is replaced on freeze. Uncontended — the single writer holds it briefly.
    writer_term_cache: Mutex<FxHashMap<Arc<str>, Arc<ArcSwap<TermSlice>>>>,
}

impl TailIndex {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            terms: SkipMap::new(),
            snapshot: ArcSwap::from(Snapshot::empty()),
            next_batch_position: AtomicUsize::new(0),
            writer_term_cache: Mutex::new(FxHashMap::default()),
        })
    }

    fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.load_full()
    }

    fn doc_count(&self) -> u64 {
        self.snapshot.load().cumulative_doc_count
    }

    fn visible_count(&self) -> usize {
        self.snapshot.load().visible_count
    }

    /// Next dense batch position for an incoming batch.
    fn next_position(&self) -> usize {
        self.next_batch_position.fetch_add(1, Ordering::Relaxed)
    }

    /// Install one batch's term chunks then publish a new visibility snapshot.
    #[allow(clippy::too_many_arguments)] // one batch's fields, threaded together
    fn append_batch(
        &self,
        batch_position: usize,
        document_position_start: u64,
        documents: Vec<DocumentMetadata>,
        total_tokens: u64,
        term_builders: FxHashMap<Arc<str>, BatchTermBuilder>,
        with_position: bool,
    ) {
        let mut cache = self
            .writer_term_cache
            .lock()
            .expect("writer term cache poisoned — single-writer invariant violated");
        for (term, builder) in term_builders {
            let chunk = builder.build(batch_position, with_position);
            // First sight of the term this generation populates the SkipMap
            // (so readers can find it) and caches the slot; later batches hit
            // only the cache.
            let slot = cache.entry(term).or_insert_with_key(|term| {
                self.terms
                    .get_or_insert_with(term.clone(), TermSlice::empty_arc_swap)
                    .value()
                    .clone()
            });
            let cur = slot.load_full();
            slot.store(TermSlice::push(cur, chunk));
        }
        drop(cache);
        let document_count = documents.len() as u64;
        let new_meta = Arc::new(BatchMeta {
            batch_position,
            document_position_start,
            documents,
        });
        let cur = self.snapshot.load();
        debug_assert_eq!(document_position_start, cur.cumulative_doc_count);
        self.snapshot.store(Arc::new(Snapshot {
            visible_count: cur.visible_count + 1,
            batches: cur.batches.pushed(new_meta),
            cumulative_doc_count: cur.cumulative_doc_count + document_count,
            cumulative_total_tokens: cur.cumulative_total_tokens + total_tokens,
        }));
    }

    fn memory_size(&self) -> usize {
        let mut total = std::mem::size_of::<Self>();
        for entry in self.terms.iter() {
            let term: &Arc<str> = entry.key();
            total += std::mem::size_of::<Arc<str>>() + term.len() + 32;
            total += entry.value().load().memory_size();
        }
        total += self
            .snapshot
            .load()
            .batches
            .iter()
            .map(|b| b.memory_size())
            .sum::<usize>();
        total
    }
}

/// Atomically-published index state: the immutable partitions plus the live
/// mutable tail. The pair lives behind one `ArcSwap` so a freeze (which both
/// appends a partition and resets the tail) is observed atomically.
struct IndexState {
    partitions: Arc<[Arc<Partition>]>,
    tail: Arc<TailIndex>,
}

impl IndexState {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            partitions: Arc::from(Vec::<Arc<Partition>>::new().into_boxed_slice()),
            tail: TailIndex::new(),
        })
    }
}

/// In-memory full-text search index. See module docs for the concurrency
/// model and visibility contract.
pub struct FtsMemIndex {
    field_id: i32,
    source_column_name: String,
    params: InvertedIndexParams,
    resolved_field: OnceLock<ResolvedFtsField>,

    tokenizer_pool: Arc<TokenizerPool>,
    /// Writer-only tokenizer slot. Held under a Mutex purely so `insert`
    /// can take `&self`. Single-writer assumption means this is uncontested.
    writer_tokenizer: Mutex<Box<dyn LanceTokenizer>>,

    /// `{partitions, tail}` published atomically. The tail mutates in place
    /// between freezes; the whole state is swapped on freeze.
    state: ArcSwap<IndexState>,

    /// The tail freezes into a partition once it reaches this many docs.
    freeze_threshold_rows: usize,

    /// Background tiered-merge slot. `None` = idle; `Some` with `result: None`
    /// = a merge is running on a worker thread; `Some` with `result: Some` =
    /// the merged partition is ready for the writer to install. Only the
    /// writer mutates `state`; the worker is read-only and just fills `result`,
    /// so the single-writer / lock-free-reader contract is preserved.
    merge: Arc<Mutex<Option<PendingMerge>>>,
}

/// A tiered merge dispatched to a background worker.
struct PendingMerge {
    /// `Arc::as_ptr` of each source partition, for identity-matching the
    /// merged-away partitions when the writer installs the result.
    sources: Vec<usize>,
    /// Filled by the worker when the merge completes.
    result: Option<Arc<Partition>>,
}

fn publish_pending_merge(slot: &Mutex<Option<PendingMerge>>, merged: Result<Partition>) {
    let Ok(mut guard) = slot.lock() else {
        return;
    };
    match merged {
        Ok(merged) => {
            if let Some(pending) = guard.as_mut() {
                pending.result = Some(Arc::new(merged));
            }
        }
        Err(error) => {
            tracing::error!(?error, "background FTS tiered merge failed");
            *guard = None;
        }
    }
}

impl std::fmt::Debug for FtsMemIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.state.load();
        f.debug_struct("FtsMemIndex")
            .field("field_id", &self.field_id)
            .field("source_column_name", &self.source_column_name)
            .field("doc_count", &self.doc_count())
            .field("partitions", &st.partitions.len())
            .field("params", &self.params)
            .finish()
    }
}

impl FtsMemIndex {
    /// Default reader-tokenizer pool capacity. Small but enough to absorb
    /// short bursts of concurrent search calls without thrashing.
    const DEFAULT_TOKENIZER_POOL_CAP: usize = 8;

    /// Default tail freeze threshold (docs). Analogous to Lucene's
    /// `ramBufferSizeMB`: large enough that the per-query tail scan stays
    /// cheap, small enough to bound it independent of corpus size.
    const DEFAULT_FREEZE_THRESHOLD_ROWS: usize = 50_000;

    /// Hard cap on live partitions: when a freeze would exceed it, all
    /// partitions are merged into one synchronously. This is only a safety net;
    /// the background tiered merge normally keeps the count far below it.
    const MAX_PARTITIONS: usize = 32;

    /// Background tiered-merge factor: once a size tier (partitions bucketed by
    /// `floor(log2(doc_count))`) accumulates this many partitions, they are
    /// merged into one larger partition off the writer thread. Mirrors Lucene's
    /// tiered merge — it bounds the live partition count to ~`O(log n)` (so
    /// per-query overhead and posting fragmentation stay low) while amortizing
    /// total merge work, and runs in the background so write throughput is
    /// unaffected.
    const MERGE_FACTOR: usize = 8;

    /// Create a new FTS index for the given field with default parameters.
    pub fn new(field_id: i32, column_name: String) -> Self {
        Self::with_params(field_id, column_name, InvertedIndexParams::default())
    }

    /// Create a new FTS index with custom tokenizer parameters.
    pub fn with_params(field_id: i32, column_name: String, params: InvertedIndexParams) -> Self {
        Self::try_with_params(field_id, column_name, params)
            .expect("invalid MemWAL FTS index parameters")
    }

    /// Try to create a new FTS index with custom tokenizer parameters.
    pub fn try_with_params(
        field_id: i32,
        column_name: String,
        params: InvertedIndexParams,
    ) -> Result<Self> {
        params.validate_format_version()?;
        let pool = TokenizerPool::new(&params, Self::DEFAULT_TOKENIZER_POOL_CAP)?;
        let writer_tokenizer = pool.template.box_clone();
        Ok(Self {
            field_id,
            source_column_name: column_name,
            params,
            resolved_field: OnceLock::new(),
            tokenizer_pool: Arc::new(pool),
            writer_tokenizer: Mutex::new(writer_tokenizer),
            state: ArcSwap::from(IndexState::empty()),
            freeze_threshold_rows: Self::DEFAULT_FREEZE_THRESHOLD_ROWS,
            merge: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) fn try_with_resolved_field(
        field_id: i32,
        column_name: String,
        params: InvertedIndexParams,
        resolved_field: ResolvedFtsField,
    ) -> Result<Self> {
        let index = Self::try_with_params(field_id, column_name, params)?;
        index.resolved_field.set(resolved_field).map_err(|_| {
            Error::internal("MemWAL FTS traversal was initialized twice".to_string())
        })?;
        Ok(index)
    }

    /// Override the tail freeze threshold (docs) — the analogue of Lucene's
    /// `ramBufferSizeMB`. Larger keeps more rows in the un-indexed mutable tail
    /// (cheaper writes, costlier read-your-writes scans); smaller freezes into
    /// block-max-searchable partitions sooner.
    pub fn with_freeze_threshold_rows(mut self, rows: usize) -> Self {
        self.freeze_threshold_rows = rows.max(1);
        self
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn column_name(&self) -> &str {
        &self.source_column_name
    }

    pub fn source_column_name(&self) -> &str {
        &self.source_column_name
    }

    pub fn params(&self) -> &InvertedIndexParams {
        &self.params
    }

    pub fn document_granularity(&self) -> lance_index::scalar::inverted::DocumentGranularity {
        self.params.get_document_granularity()
    }

    /// Number of visible documents across all partitions and the tail.
    pub fn doc_count(&self) -> usize {
        let st = self.state.load();
        st.partitions.iter().map(|p| p.doc_count()).sum::<usize>() + st.tail.doc_count() as usize
    }

    /// Whether there are any visible documents.
    pub fn is_empty(&self) -> bool {
        let st = self.state.load();
        st.partitions.is_empty() && st.tail.doc_count() == 0
    }

    /// Total number of visible (term, doc) postings.
    pub fn entry_count(&self) -> usize {
        let st = self.state.load_full();
        let part: usize = st.partitions.iter().map(|p| p.entry_count()).sum();
        let visible = st.tail.visible_count();
        let tail: usize = st
            .tail
            .terms
            .iter()
            .map(|e| {
                e.value()
                    .load()
                    .chunks()
                    .filter(|c| c.batch_position < visible)
                    .map(|c| c.doc_count())
                    .sum::<usize>()
            })
            .sum();
        part + tail
    }

    /// Estimated bytes of heap memory held by this index.
    pub fn memory_usage(&self) -> usize {
        let st = self.state.load_full();
        let mut total = std::mem::size_of::<Self>();
        total += st.partitions.iter().map(|p| p.memory_size()).sum::<usize>();
        total += st.tail.memory_size();
        total
    }

    /// Component memory breakdown (bytes), for diagnostics:
    /// `(num_partitions, term_strings, postings_meta, block_meta, doc_freq, pos, docs, tail)`.
    pub fn memory_breakdown(&self) -> (usize, usize, usize, usize, usize, usize, usize, usize) {
        let st = self.state.load_full();
        let (mut terms, mut postings, mut blocks, mut df, mut pos, mut docs) = (0, 0, 0, 0, 0, 0);
        for p in st.partitions.iter() {
            terms += p.term_fst.as_fst().as_bytes().len();
            postings += p.postings.len() * std::mem::size_of::<PostingRef>();
            blocks += p.block_bytes.len();
            df += p.doc_freq_data.len();
            pos += p.pos_data.len();
            docs += p.docs.len() * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        }
        (
            st.partitions.len(),
            terms,
            postings,
            blocks,
            df,
            pos,
            docs,
            st.tail.memory_size(),
        )
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    /// Insert a batch into the mutable tail.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        self.insert_batch(batch, row_offset)
    }

    /// Insert a batch. The explicit `batch_position` is accepted for API
    /// compatibility but not used for placement: the tail assigns its own
    /// dense, 0-based positions (required by the visibility filter).
    pub fn insert_with_batch_position(
        &self,
        batch: &RecordBatch,
        row_offset: u64,
        _batch_position: usize,
    ) -> Result<()> {
        self.insert_batch(batch, row_offset)
    }

    fn insert_batch(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        let st = self.state.load_full();
        let document_position_start = st.tail.doc_count();
        if self.resolved_field.get().is_none() {
            let schema = LanceSchema::try_from(batch.schema().as_ref())?;
            let resolved = resolve_fts_field(
                &schema,
                &self.source_column_name,
                self.params.get_document_granularity(),
            )?;
            let _ = self.resolved_field.set(resolved);
        }
        let resolved = self.resolved_field.get().ok_or_else(|| {
            Error::internal("MemWAL FTS traversal was not initialized".to_string())
        })?;
        let extracted_documents = resolved.documents_from_batch(batch)?;

        let mut tok_guard = self
            .writer_tokenizer
            .lock()
            .expect("writer tokenizer poisoned — single-writer invariant violated");
        let tokenizer: &mut dyn LanceTokenizer = tok_guard.as_mut();

        // Per-term postings for this batch. Tokens are appended directly
        // (`observe`) as they stream out of the tokenizer, avoiding the
        // per-document map and per-`(term, doc)` `Vec` allocation that
        // dominated insert cost. `FxHashMap` skips SipHash on the hot lookup.
        let mut term_builders: FxHashMap<Arc<str>, BatchTermBuilder> = FxHashMap::default();
        let mut documents: Vec<DocumentMetadata> = Vec::with_capacity(batch.num_rows());
        let mut total_tokens: u64 = 0;
        let preserve_zero_token_documents =
            self.params.get_document_granularity().is_list_element();
        let mut index_document = |key: DocumentKey, text: &str| -> Result<()> {
            let document_position = document_position_start + documents.len() as u64;
            let num_tokens = index_text(text, document_position, tokenizer, &mut term_builders)?;
            if preserve_zero_token_documents || num_tokens > 0 {
                documents.push(DocumentMetadata { key, num_tokens });
                total_tokens += num_tokens as u64;
            }
            Ok(())
        };

        for document in extracted_documents {
            index_document(
                DocumentKey {
                    row_position: row_offset + document.row_index as u64,
                    doc_index: document.doc_index,
                },
                &document.text,
            )?;
        }

        if documents.is_empty() {
            return Ok(());
        }

        // Drop the tokenizer guard before publishing so we don't hold it
        // across the snapshot install.
        drop(tok_guard);

        let batch_position = st.tail.next_position();
        st.tail.append_batch(
            batch_position,
            document_position_start,
            documents,
            total_tokens,
            term_builders,
            self.params.has_positions(),
        );

        if st.tail.doc_count() >= self.freeze_threshold_rows as u64 {
            self.freeze(&st)?;
        }
        Ok(())
    }

    /// Freeze the current tail into a new immutable partition and publish a
    /// fresh empty tail. Only the writer calls this; readers snapshotting the
    /// old `IndexState` keep a consistent view across the freeze.
    fn freeze(&self, st: &IndexState) -> Result<()> {
        let Some(partition) = Partition::from_tail(&st.tail)? else {
            return Ok(());
        };
        let mut partitions: Vec<Arc<Partition>> = st.partitions.iter().cloned().collect();
        partitions.push(Arc::new(partition));
        // Fold in any completed background merge before re-evaluating tiers.
        self.install_pending_merge(&mut partitions)?;
        if partitions.len() > Self::MAX_PARTITIONS {
            partitions = vec![Arc::new(Partition::merge(&partitions)?)];
        }
        self.state.store(Arc::new(IndexState {
            partitions: Arc::from(partitions.into_boxed_slice()),
            tail: TailIndex::new(),
        }));
        // Kick off a background merge if a size tier is now over-full.
        self.maybe_start_merge()?;
        Ok(())
    }

    /// Install a completed background merge into `partitions` (in place):
    /// drop the merged-away source partitions and append the merged one.
    /// No-op while a merge is still running or none is pending.
    fn install_pending_merge(&self, partitions: &mut Vec<Arc<Partition>>) -> Result<()> {
        let mut guard = self
            .merge
            .lock()
            .map_err(|_| Error::internal("FTS merge slot mutex poisoned"))?;
        let Some(pending) = guard.as_mut() else {
            return Ok(());
        };
        let Some(merged) = pending.result.take() else {
            return Ok(()); // still running
        };
        let sources: HashSet<usize> = pending.sources.iter().copied().collect();
        *guard = None;
        // Install only if every source is still live. If a synchronous
        // `MAX_PARTITIONS` collapse merged the sources away while this merge
        // ran, the merged docs are already present — appending it would
        // double-count, so discard the stale result instead.
        let present = partitions
            .iter()
            .filter(|p| sources.contains(&(Arc::as_ptr(p) as usize)))
            .count();
        if present == sources.len() {
            partitions.retain(|p| !sources.contains(&(Arc::as_ptr(p) as usize)));
            partitions.push(merged);
        }
        Ok(())
    }

    /// If no merge is in flight and some size tier holds at least
    /// `MERGE_FACTOR` partitions, dispatch their merge to a background thread.
    fn maybe_start_merge(&self) -> Result<()> {
        let mut guard = self
            .merge
            .lock()
            .map_err(|_| Error::internal("FTS merge slot mutex poisoned"))?;
        if guard.is_some() {
            return Ok(()); // one merge at a time
        }
        let partitions = self.state.load();
        let Some(group) = select_merge_group(&partitions.partitions, Self::MERGE_FACTOR) else {
            return Ok(());
        };
        let sources: Vec<usize> = group.iter().map(|p| Arc::as_ptr(p) as usize).collect();
        *guard = Some(PendingMerge {
            sources,
            result: None,
        });
        drop(guard);
        let slot = Arc::clone(&self.merge);
        // Read-only merge off the writer thread; the writer installs the result
        // on a later freeze. The worker shares ownership of `slot` (and, via
        // `group`, the source partitions), so it is safe even if the index is
        // dropped mid-merge.
        std::thread::spawn(move || {
            publish_pending_merge(slot.as_ref(), Partition::merge(&group));
        });
        Ok(())
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /// Search for documents containing a term.
    ///
    /// The term is tokenized using the configured tokenizer. Returns all
    /// matching documents with BM25 scores. Result order is unspecified;
    /// use `search_with_options` for sorted/limited output.
    pub fn search(&self, term: &str) -> Vec<FtsEntry> {
        let st = self.state.load_full();
        let tokens = self.analyze_for_search(term);
        self.search_match(&st, &tokens, Operator::Or, None, true, true)
    }

    /// Search for documents containing an exact phrase, optionally allowing
    /// `slop` intervening tokens between consecutive query tokens.
    pub fn search_phrase(&self, phrase: &str, slop: u32) -> Vec<FtsEntry> {
        let st = self.state.load_full();
        let tokens = self.analyze_for_search(phrase);
        self.search_phrase_tokens(&st, &tokens, slop, true)
    }

    /// Freeze the current mutable tail into an immutable partition, so a
    /// subsequent `include_tail = false` search sees all rows written so far
    /// (the analogue of a Lucene commit/flush before opening a reader). No-op
    /// when the tail is empty. Writer-side: callers hold the single-writer role.
    pub fn flush(&self) {
        let st = self.state.load_full();
        if st.tail.visible_count() > 0
            && let Err(error) = self.freeze(&st)
        {
            tracing::error!(?error, "failed to freeze the FTS MemWAL tail");
        }
    }

    /// Expand a term to fuzzy matches within the specified edit distance.
    /// Returns `(matched_term, distance)` pairs sorted by distance, capped
    /// at `max_expansions`.
    pub fn expand_fuzzy(
        &self,
        term: &str,
        max_distance: u32,
        max_expansions: usize,
    ) -> Vec<(String, u32)> {
        let st = self.state.load_full();
        self.expand_fuzzy_term(&st, term, max_distance, 0, max_expansions, true)
    }

    /// Search for documents using fuzzy matching on each query token.
    pub fn search_fuzzy(
        &self,
        query: &str,
        fuzziness: Option<u32>,
        max_expansions: usize,
    ) -> Vec<FtsEntry> {
        let st = self.state.load_full();
        let tokens = self.tokenize_for_search(query);
        self.search_fuzzy_tokens(&st, &tokens, fuzziness, 0, max_expansions, true)
    }

    /// BM25 OR-search over the query tokens, scored with one corpus-wide
    /// [`MemBM25Scorer`]. With a result limit, all partitions and the tail
    /// feed a single shared top-k heap: the tail is scanned first to warm the
    /// pruning threshold, then each partition's WAND prunes against the
    /// shared rising threshold (instead of every partition cold-starting).
    /// Without a limit, an exact O(matches) scan across partitions + tail.
    fn search_match(
        &self,
        st: &IndexState,
        query_tokens: &Tokens,
        operator: Operator,
        limit: Option<usize>,
        include_tail: bool,
        tail_skip: bool,
    ) -> Vec<FtsEntry> {
        if operator == Operator::And && has_grouped_positions(query_tokens) {
            return self.search_grouped_and(st, query_tokens, limit, include_tail, tail_skip);
        }
        let tokens = query_tokens_to_vec(query_tokens);
        self.search_match_strings(st, &tokens, operator, limit, include_tail, tail_skip)
    }

    fn search_match_strings(
        &self,
        st: &IndexState,
        tokens: &[String],
        operator: Operator,
        limit: Option<usize>,
        include_tail: bool,
        tail_skip: bool,
    ) -> Vec<FtsEntry> {
        if tokens.is_empty() {
            return Vec::new();
        }
        // Snapshot the tail once so the scorer's stats and the scanned tail
        // postings are from the same visibility point.
        let tail_snap = st.tail.snapshot();
        let scan_tail = include_tail && tail_snap.visible_count > 0;
        let scorer = build_scorer(st, &tail_snap, tokens, include_tail);
        if scorer.num_docs() == 0 {
            return Vec::new();
        }
        match (operator, limit) {
            (Operator::Or, Some(k)) if k > 0 => {
                let mut topk = TopK::new(k);
                // Scan the block-max partitions first to warm the shared
                // threshold, then the (un-skippable) tail last — so the tail
                // scan can be skipped wholesale when its score bound can't beat
                // the threshold. `tail_skip = false` forces a full tail scan.
                for p in st.partitions.iter() {
                    p.or_topk_into(tokens, &scorer, &mut topk);
                }
                if scan_tail {
                    let theta = if tail_skip {
                        topk.threshold()
                    } else {
                        f32::NEG_INFINITY
                    };
                    for e in score_terms(
                        &tail_snap,
                        &st.tail.terms,
                        tokens,
                        Operator::Or,
                        &scorer,
                        theta,
                    ) {
                        topk.offer(e.score, e.key());
                    }
                }
                topk.into_entries()
            }
            _ => {
                let mut results = Vec::new();
                for p in st.partitions.iter() {
                    results.extend(p.search_match(tokens, operator, &scorer));
                }
                if scan_tail {
                    results.extend(score_terms(
                        &tail_snap,
                        &st.tail.terms,
                        tokens,
                        operator,
                        &scorer,
                        f32::NEG_INFINITY,
                    ));
                }
                results
            }
        }
    }

    fn search_grouped_and(
        &self,
        st: &IndexState,
        query_tokens: &Tokens,
        limit: Option<usize>,
        include_tail: bool,
        tail_skip: bool,
    ) -> Vec<FtsEntry> {
        let mut result_map: Option<HashMap<DocumentKey, f32>> = None;
        for group in query_position_groups(query_tokens) {
            let group_results =
                self.search_match_strings(st, &group, Operator::Or, None, include_tail, tail_skip);
            let group_map = group_results
                .into_iter()
                .map(|entry| (entry.key(), entry.score))
                .collect::<HashMap<_, _>>();
            let Some(current) = result_map.as_mut() else {
                result_map = Some(group_map);
                continue;
            };
            current.retain(|key, score| {
                if let Some(group_score) = group_map.get(key) {
                    *score += group_score;
                    true
                } else {
                    false
                }
            });
            if current.is_empty() {
                return Vec::new();
            }
        }

        let mut results = result_map
            .unwrap_or_default()
            .into_iter()
            .map(|(key, score)| FtsEntry {
                row_position: key.row_position,
                doc_index: public_doc_index(&key.doc_index),
                score,
            })
            .collect::<Vec<_>>();
        if let Some(limit) = limit {
            results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(limit);
        }
        results
    }

    fn search_phrase_tokens(
        &self,
        st: &IndexState,
        query_tokens: &Tokens,
        slop: u32,
        include_tail: bool,
    ) -> Vec<FtsEntry> {
        if query_tokens.is_empty() {
            return Vec::new();
        }
        let groups = query_position_groups(query_tokens);
        if groups.is_empty() {
            return Vec::new();
        }
        if groups.len() == 1 {
            // A single-token phrase reduces to a regular term search.
            return self.search_match_strings(
                st,
                &groups[0],
                Operator::Or,
                None,
                include_tail,
                true,
            );
        }
        // A multi-token phrase needs token positions; without them (the index
        // was built `with_position = false`) phrase search is unsupported, as
        // on disk and in Lucene (`DOCS_AND_FREQS`).
        if !self.params.has_positions() {
            return Vec::new();
        }
        let has_grouped_terms = groups.iter().any(|group| group.len() > 1);
        let tokens = position_groups_to_tokens(&groups);
        let tail_snap = st.tail.snapshot();
        let scan_tail = include_tail && tail_snap.visible_count > 0;
        let scorer = build_scorer(st, &tail_snap, &tokens, include_tail);
        if scorer.num_docs() == 0 {
            return Vec::new();
        }
        let mut results = Vec::new();
        for p in st.partitions.iter() {
            if has_grouped_terms {
                results.extend(p.search_phrase_groups(&groups, slop, &scorer));
            } else {
                results.extend(p.search_phrase(&tokens, slop, &scorer));
            }
        }
        if scan_tail {
            if has_grouped_terms {
                results.extend(phrase_search_tail_groups(
                    &tail_snap,
                    &st.tail.terms,
                    &groups,
                    slop,
                    &scorer,
                ));
            } else {
                results.extend(phrase_search_tail(
                    &tail_snap,
                    &st.tail.terms,
                    &tokens,
                    slop,
                    &scorer,
                ));
            }
        }
        results
    }

    fn search_fuzzy_tokens(
        &self,
        st: &IndexState,
        tokens: &[String],
        fuzziness: Option<u32>,
        prefix_length: u32,
        max_expansions: usize,
        include_tail: bool,
    ) -> Vec<FtsEntry> {
        if tokens.is_empty() {
            return Vec::new();
        }
        let mut expanded: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for tok in tokens {
            let max_dist = fuzziness.unwrap_or_else(|| auto_fuzziness(tok));
            for (matched, _) in self.expand_fuzzy_term(
                st,
                tok,
                max_dist,
                prefix_length,
                max_expansions,
                include_tail,
            ) {
                if seen.insert(matched.clone()) {
                    expanded.push(matched);
                }
            }
        }
        if expanded.is_empty() {
            return Vec::new();
        }
        self.search_match_strings(st, &expanded, Operator::Or, None, include_tail, true)
    }

    /// Expand `term` against the term dictionaries of every partition (and the
    /// visible tail, when `include_tail`).
    fn expand_fuzzy_term(
        &self,
        st: &IndexState,
        term: &str,
        max_distance: u32,
        prefix_length: u32,
        max_expansions: usize,
        include_tail: bool,
    ) -> Vec<(String, u32)> {
        let tail_snap = st.tail.snapshot();
        if max_distance == 0 {
            let in_tail = include_tail
                && st
                    .tail
                    .terms
                    .get(term)
                    .map(|e| has_visible_chunk(&e.value().load(), tail_snap.visible_count))
                    .unwrap_or(false);
            let in_partition = st.partitions.iter().any(|p| p.contains_token(term));
            return if in_tail || in_partition {
                vec![(term.to_string(), 0)]
            } else {
                Vec::new()
            };
        }
        let mut matches: Vec<(String, u32)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let prefix = char_prefix(term, prefix_length);
        for entry in st.tail.terms.iter() {
            if !include_tail || !has_visible_chunk(&entry.value().load(), tail_snap.visible_count) {
                continue;
            }
            let key: &Arc<str> = entry.key();
            if !key.starts_with(prefix) {
                continue;
            }
            let dist = levenshtein_distance(term, key);
            if dist <= max_distance && seen.insert(key.to_string()) {
                matches.push((key.to_string(), dist));
            }
        }
        for p in st.partitions.iter() {
            for key in p.collect_terms() {
                if !key.starts_with(prefix) {
                    continue;
                }
                let dist = levenshtein_distance(term, key.as_ref());
                if dist <= max_distance && seen.insert(key.to_string()) {
                    matches.push((key.to_string(), dist));
                }
            }
        }
        matches.sort_by_key(|(_, d)| *d);
        matches.truncate(max_expansions);
        matches
    }

    /// Execute a query expression and return matching documents with scores.
    ///
    /// Snapshots the index state once at entry so the entire compound
    /// query — including every leaf invoked recursively from `Boolean` /
    /// `Boost` — sees the same `{partitions, tail}` view. This preserves the
    /// per-batch monotonic visibility contract for compound queries.
    pub fn search_query(&self, query: &FtsQueryExpr) -> Vec<FtsEntry> {
        let st = self.state.load_full();
        self.search_query_with_state(query, &st, None, true, true)
    }

    /// `limit` is the caller's top-k, threaded down so a top-level `Match`
    /// leaf can prune with WAND. Compound branches (`Boolean`/`Boost`) need
    /// their children's full result sets, so they pass `None` downward.
    /// `include_tail` selects read-your-writes vs immutable-only (see
    /// [`SearchOptions::include_tail`]) and is threaded uniformly to every leaf.
    fn search_query_with_state(
        &self,
        query: &FtsQueryExpr,
        st: &IndexState,
        limit: Option<usize>,
        include_tail: bool,
        tail_skip: bool,
    ) -> Vec<FtsEntry> {
        match query {
            FtsQueryExpr::Match {
                query,
                operator,
                boost,
            } => {
                let tokens = self.analyze_for_search(query);
                let mut results =
                    self.search_match(st, &tokens, *operator, limit, include_tail, tail_skip);
                apply_boost(&mut results, *boost);
                results
            }
            FtsQueryExpr::Phrase { query, slop, boost } => {
                let tokens = self.analyze_for_search(query);
                let mut results = self.search_phrase_tokens(st, &tokens, *slop, include_tail);
                apply_boost(&mut results, *boost);
                results
            }
            FtsQueryExpr::Fuzzy {
                query,
                fuzziness,
                prefix_length,
                max_expansions,
                boost,
            } => {
                let tokens = self.tokenize_for_search(query);
                let mut results = self.search_fuzzy_tokens(
                    st,
                    &tokens,
                    *fuzziness,
                    *prefix_length,
                    *max_expansions,
                    include_tail,
                );
                apply_boost(&mut results, *boost);
                results
            }
            FtsQueryExpr::Boolean {
                must,
                should,
                must_not,
            } => self.search_boolean(must, should, must_not, st, include_tail),
            FtsQueryExpr::Boost {
                positive,
                negative,
                negative_boost,
            } => self.search_boost(
                positive,
                negative.as_deref(),
                *negative_boost,
                st,
                include_tail,
            ),
        }
    }

    /// Execute a query with options (sort + WAND prune + limit).
    pub fn search_with_options(
        &self,
        query: &FtsQueryExpr,
        options: SearchOptions,
    ) -> Vec<FtsEntry> {
        if options.limit == Some(0) {
            return Vec::new();
        }

        let st = self.state.load_full();
        let mut results = self.search_query_with_state(
            query,
            &st,
            options.limit,
            options.include_tail,
            options.tail_skip,
        );
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if options.wand_factor < 1.0 {
            if let Some(limit) = options.limit {
                if results.len() > limit {
                    let top_k_score = results[limit - 1].score;
                    let threshold = top_k_score * options.wand_factor;
                    results.retain(|e| e.score >= threshold);
                }
            } else if let Some(max_entry) = results.first() {
                let threshold = max_entry.score * options.wand_factor;
                results.retain(|e| e.score >= threshold);
            }
        }
        if let Some(limit) = options.limit {
            results.truncate(limit);
        }
        results
    }

    fn search_boost(
        &self,
        positive: &FtsQueryExpr,
        negative: Option<&FtsQueryExpr>,
        negative_boost: f32,
        st: &IndexState,
        include_tail: bool,
    ) -> Vec<FtsEntry> {
        let mut results = self.search_query_with_state(positive, st, None, include_tail, true);
        let Some(neg) = negative else {
            return results;
        };
        let negative_results = self.search_query_with_state(neg, st, None, include_tail, true);
        let negative_set: HashSet<DocumentKey> =
            negative_results.iter().map(FtsEntry::key).collect();
        for entry in &mut results {
            if negative_set.contains(&entry.key()) {
                entry.score *= negative_boost;
            }
        }
        results
    }

    fn search_boolean(
        &self,
        must: &[FtsQueryExpr],
        should: &[FtsQueryExpr],
        must_not: &[FtsQueryExpr],
        st: &IndexState,
        include_tail: bool,
    ) -> Vec<FtsEntry> {
        let excluded: HashSet<DocumentKey> = must_not
            .iter()
            .flat_map(|q| self.search_query_with_state(q, st, None, include_tail, true))
            .map(|entry| entry.key())
            .collect();

        let mut result_map: HashMap<DocumentKey, f32> = if must.is_empty() {
            let mut map: HashMap<DocumentKey, f32> = HashMap::new();
            for q in should {
                for entry in self.search_query_with_state(q, st, None, include_tail, true) {
                    *map.entry(entry.key()).or_default() += entry.score;
                }
            }
            map
        } else {
            let first_results =
                self.search_query_with_state(&must[0], st, None, include_tail, true);
            let mut map: HashMap<DocumentKey, f32> = first_results
                .into_iter()
                .map(|entry| (entry.key(), entry.score))
                .collect();
            for q in must.iter().skip(1) {
                let results = self.search_query_with_state(q, st, None, include_tail, true);
                let result_set: HashMap<DocumentKey, f32> = results
                    .into_iter()
                    .map(|entry| (entry.key(), entry.score))
                    .collect();
                map = map
                    .into_iter()
                    .filter_map(|(pos, score)| result_set.get(&pos).map(|s| (pos, score + s)))
                    .collect();
            }
            for q in should {
                for entry in self.search_query_with_state(q, st, None, include_tail, true) {
                    if let Some(score) = map.get_mut(&entry.key()) {
                        *score += entry.score;
                    }
                }
            }
            map
        };

        for pos in &excluded {
            result_map.remove(pos);
        }

        result_map
            .into_iter()
            .map(|(key, score)| FtsEntry {
                row_position: key.row_position,
                doc_index: public_doc_index(&key.doc_index),
                score,
            })
            .collect()
    }

    fn tokenize_for_search(&self, text: &str) -> Vec<String> {
        query_tokens_to_vec(&self.analyze_for_search(text))
    }

    fn analyze_for_search(&self, text: &str) -> Tokens {
        let mut tok = PooledTokenizer::new(&self.tokenizer_pool);
        let mut stream = tok.get_mut().token_stream_for_search(text);
        let mut tokens = Vec::new();
        let mut positions = Vec::new();
        while let Some(t) = stream.next() {
            tokens.push(t.text.clone());
            positions.push(t.position as u32);
        }
        Tokens::with_positions(tokens, positions, DocType::Text)
    }

    // ------------------------------------------------------------------
    // Flush to Lance inverted index format
    // ------------------------------------------------------------------

    /// Export the in-memory FTS index to an `InnerBuilder` ready to be
    /// written to disk.
    ///
    /// Stored documents are kept in insert order and retain their positions in
    /// the forward-written flush data file. `total_rows` is used only to
    /// validate positions.
    pub fn to_index_builder(
        &self,
        partition_id: u64,
        total_rows: usize,
    ) -> Result<lance_index::scalar::inverted::builder::InnerBuilder> {
        use lance_index::scalar::inverted::PostingListBuilder;
        use lance_index::scalar::inverted::builder::{InnerBuilder, PositionRecorder};

        let st = self.state.load_full();
        let with_position = self.params.has_positions();
        let block_size = self.params.posting_block_size();
        let format_version = self.params.resolved_format_version();
        let posting_tail_codec = format_version.posting_tail_codec();
        let total_rows_u64 = total_rows as u64;

        // Step 1: collect (document key, num_tokens) for every doc across all
        // immutable partitions and the visible tail.
        let mut all_docs: Vec<(DocumentKey, u32)> = Vec::new();
        for p in st.partitions.iter() {
            for (doc_id, (_, num_tokens)) in p.docs.iter().enumerate() {
                all_docs.push((doc_set_key(&p.docs, doc_id as u32), *num_tokens));
            }
        }
        let tail_snap = st.tail.snapshot();
        for batch in tail_snap.batches.iter().take(tail_snap.visible_count) {
            for document in &batch.documents {
                all_docs.push((document.key.clone(), document.num_tokens));
            }
        }
        if all_docs.is_empty() {
            return Ok(InnerBuilder::new_with_format_version_and_block_size(
                partition_id,
                with_position,
                Default::default(),
                format_version,
                block_size,
            ));
        }

        // Step 2: assign doc_ids in ascending insert-position order while
        // preserving each document's position in the forward-written data file.
        let mut entries: Vec<(DocumentKey, u32)> = Vec::with_capacity(all_docs.len());
        for (key, num_tokens) in &all_docs {
            if key.row_position >= total_rows_u64 {
                return Err(Error::io(format!(
                    "FTS flush: row position {} >= total_rows {}",
                    key.row_position, total_rows
                )));
            }
            entries.push((key.clone(), *num_tokens));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut docs = DocSet::default();
        let mut original_to_doc_id: HashMap<DocumentKey, u32> =
            HashMap::with_capacity(entries.len());
        for (key, num_tokens) in &entries {
            let doc_id = if !key.doc_index.is_empty() {
                docs.append_with_doc_index(key.row_position, *num_tokens, &key.doc_index)?
            } else {
                docs.append(key.row_position, *num_tokens)
            };
            original_to_doc_id.insert(key.clone(), doc_id);
        }

        // Step 3: merge per-term postings across every partition and the tail.
        let mut term_postings: HashMap<String, Vec<(u32, u32, Option<Vec<u32>>)>> = HashMap::new();
        for p in st.partitions.iter() {
            for (term_id, term) in p.collect_terms().into_iter().enumerate() {
                let term_id = term_id as u32;
                let bucket = term_postings.entry(term.to_string()).or_default();
                let mut cursor = PostingCursor::new(p, term_id);
                while let Some(local_doc) = cursor.doc() {
                    let key = doc_set_key(&p.docs, local_doc);
                    if let Some(&doc_id) = original_to_doc_id.get(&key) {
                        let pos = if with_position {
                            Some(cursor.positions().to_vec())
                        } else {
                            None
                        };
                        bucket.push((doc_id, cursor.freq(), pos));
                    }
                    cursor.advance();
                }
            }
        }
        for entry in st.tail.terms.iter() {
            let token: &Arc<str> = entry.key();
            let slice = entry.value().load();
            let bucket = term_postings.entry(token.to_string()).or_default();
            for chunk in slice.chunks() {
                if chunk.batch_position >= tail_snap.visible_count {
                    continue;
                }
                for (i, document_position) in chunk.row_positions.iter().enumerate() {
                    let Some(document) = lookup_document(&tail_snap, *document_position) else {
                        continue;
                    };
                    let Some(&doc_id) = original_to_doc_id.get(&document.key) else {
                        continue;
                    };
                    let pos = if with_position {
                        Some(
                            chunk
                                .positions
                                .as_ref()
                                .map(|p| p.doc_positions(i).to_vec())
                                .unwrap_or_default(),
                        )
                    } else {
                        None
                    };
                    bucket.push((doc_id, chunk.frequencies[i], pos));
                }
            }
        }

        // Step 4: emit posting lists; tokens in sorted order for determinism.
        let mut sorted_tokens: Vec<String> = term_postings.keys().cloned().collect();
        sorted_tokens.sort_unstable();
        let mut tokens = TokenSet::default();
        let mut posting_lists: Vec<PostingListBuilder> = Vec::with_capacity(sorted_tokens.len());
        for token in sorted_tokens {
            let mut docs_for_term = term_postings.remove(&token).unwrap_or_default();
            if docs_for_term.is_empty() {
                continue;
            }
            docs_for_term.sort_by_key(|(doc_id, _, _)| *doc_id);
            let token_id = tokens.add(token) as usize;
            debug_assert_eq!(token_id, posting_lists.len());
            posting_lists.push(
                PostingListBuilder::new_with_posting_tail_codec_and_block_size(
                    with_position,
                    posting_tail_codec,
                    block_size,
                ),
            );
            let plb = &mut posting_lists[token_id];
            for (doc_id, freq, pos) in docs_for_term {
                let recorder = if with_position {
                    PositionRecorder::Position(pos.unwrap_or_default().into())
                } else {
                    PositionRecorder::Count(freq)
                };
                plb.add(doc_id, recorder);
            }
        }

        let mut builder = InnerBuilder::new_with_format_version_and_block_size(
            partition_id,
            with_position,
            Default::default(),
            format_version,
            block_size,
        );
        builder.set_tokens(tokens);
        builder.set_docs(docs);
        builder.set_posting_lists(posting_lists);
        Ok(builder)
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

impl TermSlice {
    fn empty_arc_swap() -> Arc<ArcSwap<Self>> {
        Arc::new(ArcSwap::from(Self::empty()))
    }
}

/// Accumulates one batch's postings for a single term in a flat,
/// allocation-light layout. Tokens are appended directly as they stream out
/// of the tokenizer (`observe`), so there is no intermediate per-document map
/// or per-`(term, doc)` `Vec` — the dominant insert-path allocation cost.
///
/// `row_positions[i]`/`frequencies[i]` describe the i-th document this term
/// appears in (documents are observed in ascending row order, so
/// `row_positions` is sorted). `pos_data` is every position concatenated in
/// document order; document `i`'s slice is delimited by the `frequencies`
/// prefix sum, materialized into a CSR `Positions` at `build`.
struct BatchTermBuilder {
    row_positions: Vec<u64>,
    frequencies: Vec<u32>,
    pos_data: Vec<u32>,
}

impl BatchTermBuilder {
    /// Start a builder seeded with the term's first observed occurrence.
    fn with_first(row_position: u64, position: u32) -> Self {
        Self {
            row_positions: vec![row_position],
            frequencies: vec![1],
            pos_data: vec![position],
        }
    }

    /// Record one token occurrence of this term at `position` in document
    /// `row_position`. Documents must be observed in non-decreasing row order.
    fn observe(&mut self, row_position: u64, position: u32) {
        if self.row_positions.last().copied() != Some(row_position) {
            self.row_positions.push(row_position);
            self.frequencies.push(0);
        }
        *self.frequencies.last_mut().expect("seeded on construction") += 1;
        self.pos_data.push(position);
    }

    /// Freeze the accumulated postings into a `TermChunk`. Positions are stored
    /// only when `with_position` (the column's FTS index config) is set — the
    /// same contract as the on-disk format and Lucene's
    /// `DOCS_AND_FREQS_AND_POSITIONS` vs `DOCS_AND_FREQS`. Without positions,
    /// term/BM25 search works but phrase search is unsupported.
    fn build(self, batch_position: usize, with_position: bool) -> Arc<TermChunk> {
        let positions = with_position.then(|| {
            // CSR offsets are the frequency prefix sum.
            let mut offsets = Vec::with_capacity(self.frequencies.len() + 1);
            offsets.push(0u32);
            let mut acc = 0u32;
            for &f in &self.frequencies {
                acc += f;
                offsets.push(acc);
            }
            Positions {
                offsets,
                data: self.pos_data,
            }
        });
        let max_freq = self.frequencies.iter().copied().max().unwrap_or(0);
        Arc::new(TermChunk {
            batch_position,
            row_positions: self.row_positions,
            frequencies: self.frequencies,
            max_freq,
            positions,
        })
    }
}

fn index_text(
    text: &str,
    document_position: u64,
    tokenizer: &mut dyn LanceTokenizer,
    term_builders: &mut FxHashMap<Arc<str>, BatchTermBuilder>,
) -> Result<u32> {
    let mut stream = tokenizer.token_stream_for_doc(text);
    let mut num_tokens = 0u32;
    while let Some(token) = stream.next() {
        let position = u32::try_from(token.position).map_err(|_| {
            Error::invalid_input(format!(
                "token position overflow for document_position={document_position}: token_position={}",
                token.position
            ))
        })?;
        let term = token.text.as_str();
        if let Some(builder) = term_builders.get_mut(term) {
            builder.observe(document_position, position);
        } else {
            term_builders.insert(
                Arc::<str>::from(term),
                BatchTermBuilder::with_first(document_position, position),
            );
        }
        num_tokens = num_tokens.checked_add(1).ok_or_else(|| {
            Error::invalid_input(format!(
                "token count overflow for document_position={document_position}"
            ))
        })?;
    }
    Ok(num_tokens)
}

fn has_visible_chunk(slice: &TermSlice, visible_count: usize) -> bool {
    slice.chunks().any(|c| c.batch_position < visible_count)
}

fn lookup_document(snap: &Snapshot, document_position: u64) -> Option<DocumentMetadata> {
    snap.batches
        .iter()
        .find_map(|batch| batch.document(document_position).cloned())
}

fn lookup_dl(snap: &Snapshot, document_position: u64) -> Option<u32> {
    lookup_document(snap, document_position).map(|document| document.num_tokens)
}

fn find_doc_in_chunks(
    chunks: &[Arc<TermChunk>],
    document_position: u64,
) -> Option<(&Arc<TermChunk>, usize)> {
    for chunk in chunks {
        if let Ok(idx) = chunk.row_positions.binary_search(&document_position) {
            return Some((chunk, idx));
        }
    }
    None
}

/// Build a corpus-wide BM25 scorer for the (deduplicated) query tokens. A
/// single scorer makes partition-WAND scores and tail-scan scores directly
/// comparable. The corpus stats span exactly what the caller searches: the
/// frozen partitions always, plus the tail when `include_tail` is set (in
/// which case `tail_snap` must be the *same* snapshot the caller scans, so the
/// stats and the scanned postings stay mutually consistent).
fn build_scorer(
    st: &IndexState,
    tail_snap: &Snapshot,
    tokens: &[String],
    include_tail: bool,
) -> MemBM25Scorer {
    let (mut total_tokens, mut num_docs) = if include_tail {
        (
            tail_snap.cumulative_total_tokens,
            tail_snap.cumulative_doc_count as usize,
        )
    } else {
        (0, 0)
    };
    for p in st.partitions.iter() {
        total_tokens += p.total_tokens();
        num_docs += p.doc_count();
    }
    let mut token_docs: HashMap<String, usize> = HashMap::new();
    for token in tokens {
        if token_docs.contains_key(token) {
            continue;
        }
        let mut df = if include_tail {
            tail_token_df(&st.tail.terms, token, tail_snap.visible_count)
        } else {
            0
        };
        for p in st.partitions.iter() {
            df += p.token_df(token);
        }
        token_docs.insert(token.clone(), df);
    }
    MemBM25Scorer::new(total_tokens, num_docs, token_docs)
}

/// Number of visible tail docs containing `token`.
fn tail_token_df(
    terms: &SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    token: &str,
    visible_count: usize,
) -> usize {
    match terms.get(token) {
        Some(e) => e
            .value()
            .load()
            .chunks()
            .filter(|c| c.batch_position < visible_count)
            .map(|c| c.doc_count())
            .sum(),
        None => 0,
    }
}

/// Score `tokens` against the visible tail, summing each token's BM25
/// contribution per document. Uses the shared corpus-wide `scorer`.
fn score_terms(
    snap: &Snapshot,
    terms: &SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    tokens: &[String],
    operator: Operator,
    scorer: &MemBM25Scorer,
    theta: f32,
) -> Vec<FtsEntry> {
    // Per-token tail data + its score upper bound (max freq over visible chunks,
    // scored at the most generous doc length of 1). If even the sum of those
    // bounds cannot beat the current top-k threshold, no tail doc can enter the
    // results, so skip the whole tail scan. Sound: `doc_weight` is monotone.
    let mut tail_ub = 0.0f32;
    let mut tail_terms: Vec<(f32, Arc<TermSlice>)> = Vec::with_capacity(tokens.len());
    for token in tokens {
        let Some(entry) = terms.get(token.as_str()) else {
            if operator == Operator::And {
                return Vec::new();
            }
            continue;
        };
        let qw = scorer.query_weight(token);
        if qw == 0.0 {
            continue;
        }
        let slice = entry.value().load_full();
        let max_freq = slice
            .chunks()
            .filter(|c| c.batch_position < snap.visible_count)
            .map(|c| c.max_freq)
            .max()
            .unwrap_or(0);
        tail_ub += qw * scorer.doc_weight(max_freq, 1);
        tail_terms.push((qw, slice));
    }
    if tail_ub <= theta {
        return Vec::new();
    }
    let mut doc_scores: HashMap<u64, f32> = HashMap::new();
    let mut doc_hits: Option<HashMap<u64, u32>> = (operator == Operator::And).then(HashMap::new);
    for (qw, slice) in tail_terms {
        for chunk in slice.chunks() {
            if chunk.batch_position >= snap.visible_count {
                continue;
            }
            let Some(meta) = snap.batch_for(chunk.batch_position) else {
                continue;
            };
            for (i, &document_position) in chunk.row_positions.iter().enumerate() {
                let dl = meta.dl(document_position).unwrap_or(1);
                let score = qw * scorer.doc_weight(chunk.frequencies[i], dl);
                *doc_scores.entry(document_position).or_default() += score;
                if let Some(doc_hits) = &mut doc_hits {
                    *doc_hits.entry(document_position).or_default() += 1;
                }
            }
        }
    }
    doc_scores
        .into_iter()
        .filter(|(document_position, _)| {
            operator == Operator::Or
                || doc_hits
                    .as_ref()
                    .and_then(|doc_hits| doc_hits.get(document_position))
                    .is_some_and(|hits| *hits >= tokens.len() as u32)
        })
        .filter_map(|(document_position, score)| {
            lookup_document(snap, document_position).map(|document| FtsEntry {
                row_position: document.key.row_position,
                doc_index: public_doc_index(&document.key.doc_index),
                score,
            })
        })
        .collect()
}

/// Phrase-search the visible tail. Callers shortcut the single-token case, so
/// `tokens.len() >= 2` here. Scored with the shared corpus-wide `scorer`.
fn phrase_search_tail(
    snap: &Snapshot,
    terms: &SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    tokens: &[String],
    slop: u32,
    scorer: &MemBM25Scorer,
) -> Vec<FtsEntry> {
    // Gather visible chunks per query token; any missing token => no match.
    let mut per_token_chunks: Vec<Vec<Arc<TermChunk>>> = Vec::with_capacity(tokens.len());
    for tok in tokens {
        match terms.get(tok.as_str()) {
            Some(entry) => {
                let slice = entry.value().load_full();
                let visible: Vec<Arc<TermChunk>> = slice
                    .chunks()
                    .filter(|c| c.batch_position < snap.visible_count)
                    .cloned()
                    .collect();
                if visible.is_empty() {
                    return Vec::new();
                }
                per_token_chunks.push(visible);
            }
            None => return Vec::new(),
        }
    }
    // Drive the intersection from the rarest token to bound work.
    let smallest_idx = (0..per_token_chunks.len())
        .min_by_key(|&i| {
            per_token_chunks[i]
                .iter()
                .map(|c| c.doc_count())
                .sum::<usize>()
        })
        .unwrap();

    let mut results = Vec::new();
    for chunk in &per_token_chunks[smallest_idx] {
        for (doc_idx, &document_position) in chunk.row_positions.iter().enumerate() {
            let Some(pos) = chunk
                .positions
                .as_ref()
                .map(|p| p.doc_positions(doc_idx).to_vec())
            else {
                continue;
            };
            let mut all_positions: Vec<Vec<u32>> = vec![Vec::new(); tokens.len()];
            all_positions[smallest_idx] = pos;
            let mut frequencies = vec![0u32; tokens.len()];
            frequencies[smallest_idx] = chunk.frequencies[doc_idx];
            let mut all_present = true;
            for (ti, chunks) in per_token_chunks.iter().enumerate() {
                if ti == smallest_idx {
                    continue;
                }
                match find_doc_in_chunks(chunks, document_position) {
                    Some((c, other_idx)) => {
                        frequencies[ti] = c.frequencies[other_idx];
                        all_positions[ti] = c
                            .positions
                            .as_ref()
                            .map(|p| p.doc_positions(other_idx).to_vec())
                            .unwrap_or_default();
                    }
                    None => {
                        all_present = false;
                        break;
                    }
                }
            }
            if !all_present || !phrase_matches(&all_positions, slop) {
                continue;
            }
            let dl = lookup_dl(snap, document_position).unwrap_or(1);
            let score: f32 = tokens
                .iter()
                .enumerate()
                .map(|(ti, tok)| scorer.query_weight(tok) * scorer.doc_weight(frequencies[ti], dl))
                .sum();
            if let Some(document) = lookup_document(snap, document_position) {
                results.push(FtsEntry {
                    row_position: document.key.row_position,
                    doc_index: public_doc_index(&document.key.doc_index),
                    score,
                });
            }
        }
    }
    results
}

#[derive(Default, Clone)]
struct PhraseGroupDoc {
    positions: Vec<u32>,
    score: f32,
}

struct PhraseCandidate {
    positions_by_group: Vec<Vec<u32>>,
    score: f32,
}

fn merge_phrase_group<K>(
    candidates: &mut Option<HashMap<K, PhraseCandidate>>,
    group_idx: usize,
    group_count: usize,
    group_docs: HashMap<K, PhraseGroupDoc>,
) -> bool
where
    K: Eq + std::hash::Hash,
{
    let Some(current) = candidates.as_mut() else {
        *candidates = Some(
            group_docs
                .into_iter()
                .map(|(doc, group_doc)| {
                    let mut positions_by_group = vec![Vec::new(); group_count];
                    positions_by_group[group_idx] = group_doc.positions;
                    (
                        doc,
                        PhraseCandidate {
                            positions_by_group,
                            score: group_doc.score,
                        },
                    )
                })
                .collect(),
        );
        return true;
    };

    current.retain(|doc, candidate| {
        if let Some(group_doc) = group_docs.get(doc) {
            candidate.positions_by_group[group_idx] = group_doc.positions.clone();
            candidate.score += group_doc.score;
            true
        } else {
            false
        }
    });
    !current.is_empty()
}

fn phrase_search_tail_groups(
    snap: &Snapshot,
    terms: &SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    groups: &[Vec<String>],
    slop: u32,
    scorer: &MemBM25Scorer,
) -> Vec<FtsEntry> {
    let mut candidates: Option<HashMap<DocumentKey, PhraseCandidate>> = None;
    for (group_idx, group) in groups.iter().enumerate() {
        let group_docs = tail_phrase_group_docs(snap, terms, group, scorer);
        if group_docs.is_empty()
            || !merge_phrase_group(&mut candidates, group_idx, groups.len(), group_docs)
        {
            return Vec::new();
        }
    }

    candidates
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, candidate)| phrase_matches(&candidate.positions_by_group, slop))
        .map(|(key, candidate)| FtsEntry {
            row_position: key.row_position,
            doc_index: public_doc_index(&key.doc_index),
            score: candidate.score,
        })
        .collect()
}

fn tail_phrase_group_docs(
    snap: &Snapshot,
    terms: &SkipMap<Arc<str>, Arc<ArcSwap<TermSlice>>>,
    group: &[String],
    scorer: &MemBM25Scorer,
) -> HashMap<DocumentKey, PhraseGroupDoc> {
    let mut docs: HashMap<DocumentKey, PhraseGroupDoc> = HashMap::new();
    for token in group {
        let Some(entry) = terms.get(token.as_str()) else {
            continue;
        };
        let qw = scorer.query_weight(token);
        let slice = entry.value().load_full();
        for chunk in slice.chunks() {
            if chunk.batch_position >= snap.visible_count {
                continue;
            }
            let Some(meta) = snap.batch_for(chunk.batch_position) else {
                continue;
            };
            let Some(positions) = &chunk.positions else {
                continue;
            };
            for (i, &document_position) in chunk.row_positions.iter().enumerate() {
                let Some(document) = meta.document(document_position) else {
                    continue;
                };
                let entry = docs.entry(document.key.clone()).or_default();
                entry
                    .positions
                    .extend_from_slice(positions.doc_positions(i));
                let dl = document.num_tokens;
                entry.score += qw * scorer.doc_weight(chunk.frequencies[i], dl);
            }
        }
    }
    for doc in docs.values_mut() {
        doc.positions.sort_unstable();
        doc.positions.dedup();
    }
    docs
}

fn phrase_matches<T: AsRef<[u32]>>(positions: &[T], slop: u32) -> bool {
    if positions.is_empty() {
        return false;
    }
    for &first_pos in positions[0].as_ref() {
        if phrase_from_position(positions, first_pos, slop) {
            return true;
        }
    }
    false
}

fn phrase_from_position<T: AsRef<[u32]>>(positions: &[T], first_pos: u32, slop: u32) -> bool {
    let mut expected = first_pos;
    for token_positions in positions.iter().skip(1) {
        let min = expected.saturating_add(1);
        let max = expected.saturating_add(1 + slop);
        match token_positions
            .as_ref()
            .iter()
            .filter(|&&p| p >= min && p <= max)
            .min()
        {
            Some(&p) => expected = p,
            None => return false,
        }
    }
    true
}

fn apply_boost(results: &mut [FtsEntry], boost: f32) {
    if boost == 1.0 {
        return;
    }
    for r in results.iter_mut() {
        r.score *= boost;
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for a Full-Text Search index.
#[derive(Debug, Clone)]
pub struct FtsIndexConfig {
    pub name: String,
    pub field_id: i32,
    pub column: String,
    pub params: InvertedIndexParams,
    pub(crate) resolved_field: Option<Arc<ResolvedFtsField>>,
}

impl FtsIndexConfig {
    pub fn new(name: String, field_id: i32, column: String) -> Self {
        Self {
            name,
            field_id,
            column,
            params: InvertedIndexParams::default(),
            resolved_field: None,
        }
    }

    pub fn with_params(
        name: String,
        field_id: i32,
        column: String,
        params: InvertedIndexParams,
    ) -> Self {
        Self::try_with_params(name, field_id, column, params)
            .expect("invalid MemWAL FTS index config parameters")
    }

    pub fn try_with_params(
        name: String,
        field_id: i32,
        column: String,
        params: InvertedIndexParams,
    ) -> Result<Self> {
        params.validate_format_version()?;
        Ok(Self {
            name,
            field_id,
            column,
            params,
            resolved_field: None,
        })
    }

    pub(crate) fn with_resolved_field(mut self, resolved_field: ResolvedFtsField) -> Self {
        self.resolved_field = Some(Arc::new(resolved_field));
        self
    }
}

// ============================================================================
// Immutable partition (compressed posting storage)
// ============================================================================

/// Docs per posting block. Blocks bound WAND skip granularity and carry the
/// per-block score bound; a block's payload is self-delimiting.
const POSTING_BLOCK: usize = 128;

/// Number of docs in block `b` (0-based within a term) of a `doc_count`-long
/// posting list.
fn block_len(doc_count: u32, b: u32) -> usize {
    (doc_count as usize - b as usize * POSTING_BLOCK).min(POSTING_BLOCK)
}

/// Append `v` as an unsigned LEB128 varint.
fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// Read an unsigned LEB128 varint from `buf` at `*pos`, advancing `*pos`.
fn read_varint(buf: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    v
}

/// Bit width needed to represent `v` (0 for `v == 0`).
fn bit_width(v: u32) -> u8 {
    (32 - v.leading_zeros()) as u8
}

/// Pack `values` (each `< 2^width`) into `buf` as little-endian `width`-bit
/// fields. `width == 0` writes nothing.
fn bitpack_put(buf: &mut Vec<u8>, values: &[u32], width: u8) {
    if width == 0 {
        return;
    }
    let mut acc: u64 = 0;
    let mut bits: u32 = 0;
    for &v in values {
        acc |= (v as u64) << bits;
        bits += width as u32;
        while bits >= 8 {
            buf.push(acc as u8);
            acc >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        buf.push(acc as u8);
    }
}

/// Unpack `n` little-endian `width`-bit values starting at `buf[start]`,
/// appending them to `out`.
fn bitpack_get(buf: &[u8], start: usize, n: usize, width: u8, out: &mut Vec<u32>) {
    if width == 0 {
        out.resize(out.len() + n, 0);
        return;
    }
    let mask: u64 = if width >= 32 {
        u32::MAX as u64
    } else {
        (1u64 << width) - 1
    };
    let mut acc: u64 = 0;
    let mut bits: u32 = 0;
    let mut byte = start;
    for _ in 0..n {
        while bits < width as u32 {
            acc |= (buf[byte] as u64) << bits;
            byte += 1;
            bits += 8;
        }
        out.push((acc & mask) as u32);
        acc >>= width;
        bits -= width as u32;
    }
}

/// Number of bytes a width-`width` bit-packed block of `n` values occupies.
/// Identical for the scalar and `BitPacker4x` codecs.
fn bitpack_len(n: usize, width: u8) -> usize {
    (n * width as usize).div_ceil(8)
}

/// Bit-pack `values` into `buf`: SIMD `BitPacker4x` for a full 128-element
/// block (the common-term hot path), scalar `bitpack_put` for a shorter
/// final block.
fn pack_block(bp: &BitPacker4x, buf: &mut Vec<u8>, values: &[u32], width: u8) {
    if values.len() == POSTING_BLOCK && width > 0 {
        let mut input = [0u32; POSTING_BLOCK];
        input.copy_from_slice(values);
        let mut out = [0u8; POSTING_BLOCK * 4];
        let n = bp.compress(&input, &mut out, width);
        buf.extend_from_slice(&out[..n]);
    } else {
        bitpack_put(buf, values, width);
    }
}

/// Unpack `n` values of `width` bits from `buf[start..]` into `out` (cleared
/// first) — SIMD for a full 128-element block, scalar otherwise.
fn unpack_block(
    bp: &BitPacker4x,
    buf: &[u8],
    start: usize,
    n: usize,
    width: u8,
    out: &mut Vec<u32>,
) {
    out.clear();
    if n == POSTING_BLOCK && width > 0 {
        let mut decoded = [0u32; POSTING_BLOCK];
        let bytes = bitpack_len(POSTING_BLOCK, width);
        bp.decompress(&buf[start..start + bytes], &mut decoded, width);
        out.extend_from_slice(&decoded);
    } else {
        bitpack_get(buf, start, n, width, out);
    }
}

/// Bit width for a block of ascending doc ids encoded as consecutive gaps from
/// `first` (`docs[0] == first`, so the leading gap is 0). This is far smaller
/// than a frame-of-reference width (`bit_width(last - first)`) for dense terms —
/// the width tracks the largest gap, not the block's span.
fn doc_block_width(bp: &BitPacker4x, docs: &[u32], first: u32) -> u8 {
    if docs.len() == POSTING_BLOCK {
        let mut input = [0u32; POSTING_BLOCK];
        input.copy_from_slice(docs);
        bp.num_bits_sorted(first, &input)
    } else {
        let mut prev = first;
        let mut max_gap = 0u32;
        for &d in docs {
            max_gap = max_gap.max(d - prev);
            prev = d;
        }
        bit_width(max_gap)
    }
}

/// Pack ascending doc ids as consecutive gaps from `first`: SIMD
/// `compress_sorted` for a full block, scalar gaps otherwise.
fn pack_doc_block(bp: &BitPacker4x, buf: &mut Vec<u8>, docs: &[u32], first: u32, width: u8) {
    if docs.len() == POSTING_BLOCK && width > 0 {
        let mut input = [0u32; POSTING_BLOCK];
        input.copy_from_slice(docs);
        let mut out = [0u8; POSTING_BLOCK * 4];
        let n = bp.compress_sorted(first, &input, &mut out, width);
        buf.extend_from_slice(&out[..n]);
    } else {
        let mut gaps: Vec<u32> = Vec::with_capacity(docs.len());
        let mut prev = first;
        for &d in docs {
            gaps.push(d - prev);
            prev = d;
        }
        bitpack_put(buf, &gaps, width);
    }
}

/// Inverse of `pack_doc_block`: reconstruct ascending doc ids into `out`.
fn unpack_doc_block(
    bp: &BitPacker4x,
    buf: &[u8],
    start: usize,
    n: usize,
    width: u8,
    first: u32,
    out: &mut Vec<u32>,
) {
    out.clear();
    if n == POSTING_BLOCK && width > 0 {
        let mut decoded = [0u32; POSTING_BLOCK];
        let bytes = bitpack_len(POSTING_BLOCK, width);
        bp.decompress_sorted(first, &buf[start..start + bytes], &mut decoded, width);
        out.extend_from_slice(&decoded);
    } else {
        bitpack_get(buf, start, n, width, out); // gaps
        let mut acc = first;
        for g in out.iter_mut() {
            acc += *g;
            *g = acc;
        }
    }
}

/// Random-access read of `n` width-`width` values at logical index `s` from a
/// bit-packed stream whose first value starts at byte `base` of `buf`. Used to
/// decode one document's positions without touching the rest of the block.
fn bitpack_read_at(buf: &[u8], base: usize, s: usize, n: usize, width: u8, out: &mut Vec<u32>) {
    if n == 0 {
        return;
    }
    if width == 0 {
        out.resize(out.len() + n, 0);
        return;
    }
    let w = width as u32;
    let mask: u64 = if w >= 32 {
        u32::MAX as u64
    } else {
        (1u64 << w) - 1
    };
    let start_bit = s * width as usize;
    let mut byte = base + start_bit / 8;
    let skip = (start_bit % 8) as u32;
    let mut acc = (buf[byte] as u64) >> skip;
    let mut bits = 8 - skip;
    byte += 1;
    for _ in 0..n {
        while bits < w {
            acc |= (buf[byte] as u64) << bits;
            byte += 1;
            bits += 8;
        }
        out.push((acc & mask) as u32);
        acc >>= w;
        bits -= w;
    }
}

/// Per-128-doc-block metadata — enough to skip and score-bound a block
/// without decoding its payload.
/// One block's metadata, parsed on demand from the varint `block_bytes` stream
/// by [`BlockReader`]. `df_offset`/`pos_offset`/`n` are reconstructed by the
/// reader (the doc/freq offset is derived from per-block widths; the position
/// offset accumulates the stored per-block byte length), so they are not stored.
#[derive(Clone, Copy)]
struct BlockMeta<'a> {
    /// first / last doc id in the block (block doc ids are ascending).
    first_doc: u32,
    last_doc: u32,
    /// start offset of the block's doc/freq payload in `doc_freq_data`.
    df_offset: u32,
    /// start offset of the block's position payload in `pos_data`.
    pos_offset: u32,
    /// number of docs in this block.
    n: usize,
    /// bit width of the packed doc-id gaps and freqs.
    doc_width: u8,
    freq_width: u8,
    /// bit width of the packed position deltas.
    pos_width: u8,
    /// Block-local BM25 "impacts": the Pareto `(freq, dl)` frontier of the
    /// block's docs (no doc dominated by another with both higher freq and
    /// shorter length), stored as `impacts_len` varint pairs. Because
    /// `doc_weight` rises with freq and falls with dl, the block's max score is
    /// `max` over this frontier — a tight (exact) bound used to skip
    /// non-competitive blocks (block-max WAND). A loose single `(max_freq,
    /// min_dl)` pair would inflate the bound and defeat skipping.
    impacts: &'a [u8],
    impacts_len: u32,
}

impl BlockMeta<'_> {
    /// Tight block-max BM25 bound: `qw * max_d doc_weight(freq_d, dl_d)` over the
    /// block, evaluated at the Pareto frontier (the only docs that can be the max
    /// for any `avgdl`). Sound: every block doc is dominated by a frontier point.
    fn block_ub(&self, scorer: &MemBM25Scorer, qw: f32) -> f32 {
        let mut pos = 0usize;
        let mut best = 0.0f32;
        for _ in 0..self.impacts_len {
            let freq = read_varint(self.impacts, &mut pos) as u32;
            let dl = read_varint(self.impacts, &mut pos) as u32;
            best = best.max(scorer.doc_weight(freq, dl));
        }
        qw * best
    }
}

/// Per-term locator. The term's per-block metadata lives in the partition's
/// `block_bytes` varint stream starting at `meta_offset`.
#[derive(Clone, Copy)]
struct PostingRef {
    /// byte offset of the term's metadata in `block_bytes`.
    meta_offset: u32,
    /// number of blocks the term spans.
    block_count: u32,
    /// number of docs (postings) for the term.
    doc_count: u32,
}

/// Forward-sequential parser over a term's blocks in `block_bytes`. The cursor
/// only ever moves forward (new -> block 0; advance -> next; skip_to walks
/// forward), so block metadata is parsed on demand instead of being stored as a
/// 32-byte struct per block.
struct BlockReader<'a> {
    bytes: &'a [u8],
    pos: usize,
    doc_count: u32,
    block_count: u32,
    block_idx: u32,
    prev_last_doc: u32,
    df_offset: usize,
    pos_offset: usize,
}

impl<'a> BlockReader<'a> {
    fn new(part: &'a Partition, pref: &PostingRef) -> Self {
        let mut pos = pref.meta_offset as usize;
        let df_offset = read_varint(&part.block_bytes, &mut pos) as usize;
        let pos_offset = read_varint(&part.block_bytes, &mut pos) as usize;
        Self {
            bytes: &part.block_bytes,
            pos,
            doc_count: pref.doc_count,
            block_count: pref.block_count,
            block_idx: 0,
            prev_last_doc: 0,
            df_offset,
            pos_offset,
        }
    }

    /// Parse the next block's metadata (without decoding its payload), advancing
    /// the derived doc/freq and position offsets. `None` once exhausted.
    fn next_meta(&mut self) -> Option<BlockMeta<'a>> {
        if self.block_idx >= self.block_count {
            return None;
        }
        let n = block_len(self.doc_count, self.block_idx);
        let first_field = read_varint(self.bytes, &mut self.pos) as u32;
        let first_doc = if self.block_idx == 0 {
            first_field
        } else {
            self.prev_last_doc + first_field
        };
        let last_doc = first_doc + read_varint(self.bytes, &mut self.pos) as u32;
        let doc_width = self.bytes[self.pos];
        let freq_width = self.bytes[self.pos + 1];
        let pos_width = self.bytes[self.pos + 2];
        self.pos += 3;
        let pos_bytes = read_varint(self.bytes, &mut self.pos) as usize;
        // Pareto `(freq, dl)` frontier: a count followed by that many varint
        // pairs. Capture the pair bytes as a slice and skip past them.
        let impacts_len = read_varint(self.bytes, &mut self.pos) as u32;
        let impacts_start = self.pos;
        for _ in 0..impacts_len {
            read_varint(self.bytes, &mut self.pos);
            read_varint(self.bytes, &mut self.pos);
        }
        let bm = BlockMeta {
            first_doc,
            last_doc,
            df_offset: self.df_offset as u32,
            pos_offset: self.pos_offset as u32,
            n,
            doc_width,
            freq_width,
            pos_width,
            impacts: &self.bytes[impacts_start..self.pos],
            impacts_len,
        };
        self.df_offset += bitpack_len(n, doc_width) + bitpack_len(n, freq_width);
        self.pos_offset += pos_bytes;
        self.prev_last_doc = last_doc;
        self.block_idx += 1;
        Some(bm)
    }
}

/// An immutable, frozen FTS partition. Posting lists are byte-compressed
/// (VByte + delta, 128-doc blocks) into three shared buffers, so per-term
/// overhead is one `PostingRef`. See `compress-fts-partition-memory/DESIGN.md`.
struct Partition {
    /// Term dictionary: a compact FST mapping each term's bytes to its local
    /// term id (0-based, dense, in sorted order). Replaces a sorted
    /// `Box<[Arc<str>]>` searched by binary search; smaller (prefix-shared, no
    /// per-term pointers) with O(term length) lookup.
    term_fst: Map<Vec<u8>>,
    /// per term, indexed by the FST's term id.
    postings: Box<[PostingRef]>,
    /// per-term, per-block metadata as a varint stream (see [`BlockReader`]) —
    /// replaces a 32-byte `BlockMeta` struct per block.
    block_bytes: Box<[u8]>,
    /// VByte(doc-id gaps) then VByte(freqs) for every block, concatenated.
    doc_freq_data: Box<[u8]>,
    /// per doc per block: VByte(count) then VByte(delta positions), concatenated.
    pos_data: Box<[u8]>,
    /// local doc id -> (MemTable row position, token count).
    docs: DocSet,
}

/// Maximum stored `(freq, dl)` impacts per block. The frontier can't exceed the
/// number of distinct freqs in a block, so it is naturally small; beyond this a
/// block falls back to the single loose `(max_freq, min_dl)` point (still a sound
/// bound), capping storage for rare high-freq-variance blocks.
const MAX_BLOCK_IMPACTS: usize = 16;

/// Pareto `(freq, dl)` frontier of a block's docs: the points not dominated by
/// another with both `freq' >= freq` and `dl' <= dl`. Since `doc_weight` rises
/// with freq and falls with dl, the block's max BM25 score for ANY `avgdl` is
/// reached on this frontier — so `max` over it is a tight (exact) block bound.
/// `pairs` is sorted in place.
fn block_impacts(pairs: &mut [(u32, u32)]) -> Vec<(u32, u32)> {
    // dl ascending, then freq descending: sweep keeping a running max freq; a
    // point joins the frontier iff its freq exceeds every point with a
    // smaller-or-equal dl seen so far (each added point has a strictly larger
    // freq, so |frontier| <= distinct freqs).
    pairs.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)));
    let mut frontier: Vec<(u32, u32)> = Vec::new();
    let mut max_f = 0u32;
    for &(f, d) in pairs.iter() {
        if frontier.is_empty() || f > max_f {
            frontier.push((f, d.max(1)));
            max_f = f;
        }
    }
    if frontier.len() > MAX_BLOCK_IMPACTS {
        let max_freq = frontier.iter().map(|&(f, _)| f).max().unwrap_or(0);
        let min_dl = frontier.iter().map(|&(_, d)| d).min().unwrap_or(1).max(1);
        return vec![(max_freq, min_dl)];
    }
    frontier
}

/// Build a partition from `(term, sorted (doc, freq, positions))` entries.
/// Each term's docs must already be sorted ascending by doc id.
fn build_partition(
    mut entries: Vec<(Arc<str>, Vec<(u32, u32, Vec<u32>)>)>,
    docs: DocSet,
) -> Partition {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let bp = BitPacker4x::new();
    // Terms are inserted in sorted order; the FST value is the dense term id.
    let mut term_builder = fst::MapBuilder::memory();
    let mut postings = Vec::with_capacity(entries.len());
    let mut block_bytes: Vec<u8> = Vec::new();
    let mut doc_freq_data: Vec<u8> = Vec::new();
    let mut pos_data: Vec<u8> = Vec::new();
    for (term, docs_for_term) in entries {
        let doc_count = docs_for_term.len() as u32;
        let meta_offset = block_bytes.len() as u32;
        // Term-level start offsets; per-block doc/freq offsets are derived from
        // widths, position offsets accumulate the stored per-block byte length.
        put_varint(&mut block_bytes, doc_freq_data.len() as u64);
        put_varint(&mut block_bytes, pos_data.len() as u64);
        let mut block_count = 0u32;
        let mut prev_last_doc = 0u32;
        for chunk in docs_for_term.chunks(POSTING_BLOCK) {
            let pos_offset_before = pos_data.len();
            let first_doc = chunk[0].0;
            let last_doc = chunk[chunk.len() - 1].0;
            // doc ids: bit-pack consecutive gaps from `first_doc` (width tracks
            // the largest gap, not the block span — much tighter for dense terms).
            let docs_block: Vec<u32> = chunk.iter().map(|&(d, _, _)| d).collect();
            let doc_width = doc_block_width(&bp, &docs_block, first_doc);
            pack_doc_block(&bp, &mut doc_freq_data, &docs_block, first_doc, doc_width);
            // frequencies: bit-pack at a fixed block width.
            let blk_max_freq = chunk.iter().map(|&(_, f, _)| f).max().unwrap_or(0);
            let freq_width = bit_width(blk_max_freq);
            let freqs: Vec<u32> = chunk.iter().map(|&(_, f, _)| f).collect();
            pack_block(&bp, &mut doc_freq_data, &freqs, freq_width);
            // positions: one bit-packed delta stream for the whole block.
            // A doc's position count equals its frequency, so no count is
            // stored — doc `i`'s slice is found from the freq prefix sum.
            let mut pos_deltas: Vec<u32> = Vec::new();
            let mut pairs: Vec<(u32, u32)> = Vec::with_capacity(chunk.len());
            for &(d, f, ref positions) in chunk {
                let mut prev_p = 0u32;
                for &p in positions {
                    pos_deltas.push(p - prev_p);
                    prev_p = p;
                }
                pairs.push((f, docs.num_tokens(d)));
            }
            let pos_width = bit_width(pos_deltas.iter().copied().max().unwrap_or(0));
            bitpack_put(&mut pos_data, &pos_deltas, pos_width);
            // Emit the block's varint metadata record.
            let first_field = if block_count == 0 {
                first_doc
            } else {
                first_doc - prev_last_doc
            };
            put_varint(&mut block_bytes, first_field as u64);
            put_varint(&mut block_bytes, (last_doc - first_doc) as u64);
            block_bytes.push(doc_width);
            block_bytes.push(freq_width);
            block_bytes.push(pos_width);
            put_varint(
                &mut block_bytes,
                (pos_data.len() - pos_offset_before) as u64,
            );
            // Block-max impacts: the Pareto (freq, dl) frontier, as a count then
            // that many varint pairs (read back by `BlockReader::next_meta`).
            let impacts = block_impacts(&mut pairs);
            put_varint(&mut block_bytes, impacts.len() as u64);
            for (f, d) in impacts {
                put_varint(&mut block_bytes, f as u64);
                put_varint(&mut block_bytes, d as u64);
            }
            prev_last_doc = last_doc;
            block_count += 1;
        }
        let term_id = postings.len() as u64;
        postings.push(PostingRef {
            meta_offset,
            block_count,
            doc_count,
        });
        term_builder
            .insert(term.as_bytes(), term_id)
            .expect("terms inserted in sorted, unique order");
    }
    let term_fst =
        Map::new(term_builder.into_inner().expect("fst build")).expect("valid fst bytes");
    Partition {
        term_fst,
        postings: postings.into_boxed_slice(),
        block_bytes: block_bytes.into_boxed_slice(),
        doc_freq_data: doc_freq_data.into_boxed_slice(),
        pos_data: pos_data.into_boxed_slice(),
        docs,
    }
}

/// Pick a size tier (partitions bucketed by `floor(log2(doc_count))`) holding
/// at least `factor` partitions and return its members to merge. Prefers the
/// smallest such tier, so a merge fuses similarly-sized partitions and total
/// merge work stays amortized (a partition is re-merged only as it climbs
/// tiers). `None` when no tier is over-full.
fn select_merge_group(partitions: &[Arc<Partition>], factor: usize) -> Option<Vec<Arc<Partition>>> {
    let mut buckets: HashMap<u32, Vec<Arc<Partition>>> = HashMap::new();
    for p in partitions {
        let tier = u64::BITS - (p.doc_count().max(1) as u64).leading_zeros();
        buckets.entry(tier).or_default().push(p.clone());
    }
    buckets
        .into_iter()
        .filter(|(_, g)| g.len() >= factor)
        .min_by_key(|(tier, _)| *tier)
        .map(|(_, g)| g)
}

impl Partition {
    fn doc_count(&self) -> usize {
        self.docs.len()
    }

    fn total_tokens(&self) -> u64 {
        self.docs.total_tokens_num()
    }

    fn entry_count(&self) -> usize {
        self.postings.iter().map(|p| p.doc_count as usize).sum()
    }

    /// Local term id of `token`, via the FST term dictionary.
    fn term_id(&self, token: &str) -> Option<u32> {
        self.term_fst.get(token.as_bytes()).map(|v| v as u32)
    }

    fn contains_token(&self, token: &str) -> bool {
        self.term_id(token).is_some()
    }

    /// All terms, indexed by term id (recovered from the FST). Used by the
    /// flush, merge, and fuzzy-expansion paths; not on the hot search path.
    fn collect_terms(&self) -> Vec<Arc<str>> {
        let mut out: Vec<Arc<str>> = vec![Arc::from(""); self.term_fst.len()];
        let mut stream = self.term_fst.stream();
        while let Some((key, id)) = stream.next() {
            out[id as usize] = Arc::from(std::str::from_utf8(key).expect("utf8 term"));
        }
        out
    }

    /// Exact upper bound on a term's BM25 contribution to any doc: the max over
    /// its per-block impact frontiers. Walks block metadata (no payload decode),
    /// so it is cheap relative to scoring; used as the MaxScore lane bound.
    fn term_ub(&self, term_id: u32, scorer: &MemBM25Scorer, qw: f32) -> f32 {
        let pref = self.postings[term_id as usize];
        let mut reader = BlockReader::new(self, &pref);
        let mut ub = 0.0f32;
        while let Some(bm) = reader.next_meta() {
            ub = ub.max(bm.block_ub(scorer, qw));
        }
        ub
    }

    /// Number of docs in this partition containing `token`.
    fn token_df(&self, token: &str) -> usize {
        self.term_id(token)
            .map(|id| self.postings[id as usize].doc_count as usize)
            .unwrap_or(0)
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.term_fst.as_fst().as_bytes().len()
            + self.postings.len() * std::mem::size_of::<PostingRef>()
            + self.block_bytes.len()
            + self.doc_freq_data.len()
            + self.pos_data.len()
            + self.docs.len() * (std::mem::size_of::<u64>() + 2 * std::mem::size_of::<u32>())
    }

    /// Freeze the visible contents of `tail` into a new partition. Returns
    /// `None` if the tail has no visible docs.
    fn from_tail(tail: &TailIndex) -> Result<Option<Self>> {
        let snap = tail.snapshot();
        if snap.visible_count == 0 {
            return Ok(None);
        }
        // Assign dense local doc ids in row-position order.
        let mut docs = DocSet::default();
        let mut pos_to_doc: HashMap<u64, u32> = HashMap::new();
        for batch in snap.batches.iter().take(snap.visible_count) {
            for (offset, document) in batch.documents.iter().enumerate() {
                let doc_id = if !document.key.doc_index.is_empty() {
                    docs.append_with_doc_index(
                        document.key.row_position,
                        document.num_tokens,
                        &document.key.doc_index,
                    )?
                } else {
                    docs.append(document.key.row_position, document.num_tokens)
                };
                pos_to_doc.insert(batch.document_position_start + offset as u64, doc_id);
            }
        }
        if docs.is_empty() {
            return Ok(None);
        }
        // Snapshot the term slices (a cheap sequential skip-list walk of Arc
        // clones), then build each term's sorted posting list in parallel — the
        // per-term work (chunk traversal + sort) dominates the freeze and is
        // independent across terms. `pos_to_doc`/`snap` are shared read-only.
        let term_slices: Vec<(Arc<str>, Arc<TermSlice>)> = tail
            .terms
            .iter()
            .map(|e| (e.key().clone(), e.value().load_full()))
            .collect();
        let entries: Vec<(Arc<str>, Vec<(u32, u32, Vec<u32>)>)> = term_slices
            .into_par_iter()
            .filter_map(|(key, slice)| {
                let mut docs_for_term: Vec<(u32, u32, Vec<u32>)> = Vec::new();
                for chunk in slice.chunks() {
                    if chunk.batch_position >= snap.visible_count {
                        continue;
                    }
                    for (i, rp) in chunk.row_positions.iter().enumerate() {
                        let Some(&doc_id) = pos_to_doc.get(rp) else {
                            continue;
                        };
                        let pos = chunk
                            .positions
                            .as_ref()
                            .map(|p| p.doc_positions(i).to_vec())
                            .unwrap_or_default();
                        docs_for_term.push((doc_id, chunk.frequencies[i], pos));
                    }
                }
                if docs_for_term.is_empty() {
                    return None;
                }
                docs_for_term.sort_by_key(|(d, _, _)| *d);
                Some((key, docs_for_term))
            })
            .collect();
        Ok(Some(build_partition(entries, docs)))
    }

    /// Merge several partitions into one. Local doc ids are reassigned by
    /// concatenation, which keeps each merged per-term posting list sorted.
    fn merge(parts: &[Arc<Self>]) -> Result<Self> {
        let mut merged: HashMap<Arc<str>, Vec<(u32, u32, Vec<u32>)>> = HashMap::new();
        let mut docs = DocSet::default();
        let mut doc_offset: u32 = 0;
        let coordinate_rank = parts
            .first()
            .map_or(0, |partition| partition.docs.coordinate_rank());
        for p in parts {
            if p.docs.coordinate_rank() != coordinate_rank {
                return Err(Error::index(format!(
                    "cannot merge MemWAL FTS partitions with coordinate ranks {coordinate_rank} and {}",
                    p.docs.coordinate_rank()
                )));
            }
            for (doc_id, (row_position, num_tokens)) in p.docs.iter().enumerate() {
                if coordinate_rank > 0 {
                    docs.append_with_doc_index(
                        *row_position,
                        *num_tokens,
                        &p.docs.doc_index(doc_id as u32),
                    )?;
                } else {
                    docs.append(*row_position, *num_tokens);
                }
            }
            let terms = p.collect_terms();
            for (term_id, term) in terms.into_iter().enumerate() {
                let term_id = term_id as u32;
                let bucket = merged.entry(term).or_default();
                let mut cursor = PostingCursor::new(p, term_id);
                while let Some(doc) = cursor.doc() {
                    let positions = cursor.positions().to_vec();
                    bucket.push((doc + doc_offset, cursor.freq(), positions));
                    cursor.advance();
                }
            }
            doc_offset += p.docs.len() as u32;
        }
        Ok(build_partition(merged.into_iter().collect(), docs))
    }

    /// Exact O(matches) BM25 OR/AND-search of the partition by direct posting
    /// scan. The pruned top-k path is `or_topk_into`; this is the unbounded and
    /// AND path. Scored with `scorer`, so scores match `score_terms`.
    fn search_match(
        &self,
        tokens: &[String],
        operator: Operator,
        scorer: &MemBM25Scorer,
    ) -> Vec<FtsEntry> {
        // Resolve present query tokens to (local term id, query weight).
        // Repeated query tokens are kept so scores match `score_terms`.
        let mut terms: Vec<(u32, f32)> = Vec::with_capacity(tokens.len());
        let mut all_present = true;
        for token in tokens {
            match self.term_id(token) {
                Some(id) => terms.push((id, scorer.query_weight(token))),
                None => all_present = false,
            }
        }
        if terms.is_empty() || (operator == Operator::And && !all_present) {
            return Vec::new();
        }
        self.scan_match(&terms, operator, tokens.len() as u32, scorer)
    }

    /// Exact direct scan: accumulate every match into flat arrays indexed by
    /// the dense local doc id. Used for unbounded and AND queries.
    fn scan_match(
        &self,
        terms: &[(u32, f32)],
        operator: Operator,
        need: u32,
        scorer: &MemBM25Scorer,
    ) -> Vec<FtsEntry> {
        let n = self.docs.len();
        let mut scores = vec![0.0f32; n];
        let mut hits = vec![0u32; n];
        for &(id, qw) in terms {
            let mut cursor = PostingCursor::new(self, id);
            while let Some(doc) = cursor.doc() {
                let dl = self.docs.num_tokens(doc);
                scores[doc as usize] += qw * scorer.doc_weight(cursor.freq(), dl);
                hits[doc as usize] += 1;
                cursor.advance();
            }
        }
        let mut results = Vec::new();
        for doc in 0..n {
            // AND requires the doc to be hit once per query-token slot.
            if hits[doc] == 0 || (operator == Operator::And && hits[doc] < need) {
                continue;
            }
            let key = doc_set_key(&self.docs, doc as u32);
            results.push(FtsEntry {
                row_position: key.row_position,
                doc_index: public_doc_index(&key.doc_index),
                score: scores[doc],
            });
        }
        results
    }

    /// Single-term top-k with block-max skipping. For each 128-doc block, the
    /// per-block upper bound `qw * doc_weight(block.max_freq, block.min_dl)`
    /// bounds every doc's score in the block; when the top-k heap is full and
    /// that bound cannot beat the current k-th score, the block is skipped
    /// without decoding. This keeps cost ~O(competitive blocks) instead of
    /// O(df), the gap behind term-query latency scaling with corpus size.
    /// Exactness is preserved: the bound is sound, so no top-k doc is dropped.
    fn score_single_into(&self, term_id: u32, qw: f32, scorer: &MemBM25Scorer, topk: &mut TopK) {
        let pref = self.postings[term_id as usize];
        let bp = BitPacker4x::new();
        let mut docs: Vec<u32> = Vec::new();
        let mut freqs: Vec<u32> = Vec::new();
        let mut reader = BlockReader::new(self, &pref);
        while let Some(bm) = reader.next_meta() {
            let block_ub = bm.block_ub(scorer, qw);
            if block_ub <= topk.threshold() {
                continue; // no doc in this block can enter the top-k
            }
            let n = bm.n;
            let df_start = bm.df_offset as usize;
            unpack_doc_block(
                &bp,
                &self.doc_freq_data,
                df_start,
                n,
                bm.doc_width,
                bm.first_doc,
                &mut docs,
            );
            let freq_start = df_start + bitpack_len(n, bm.doc_width);
            unpack_block(
                &bp,
                &self.doc_freq_data,
                freq_start,
                n,
                bm.freq_width,
                &mut freqs,
            );
            for i in 0..n {
                let doc = docs[i];
                let dl = self.docs.num_tokens(doc);
                let score = qw * scorer.doc_weight(freqs[i], dl);
                topk.offer(score, doc_set_key(&self.docs, doc));
            }
        }
    }

    /// Top-k OR over the query tokens, contributing into the caller's shared
    /// [`TopK`]. Single-term uses block-max skipping; multi-term uses MaxScore
    /// (see [`Self::maxscore_loop`]). Exact, and because the threshold is shared
    /// across all partitions and the tail, a partition processed late prunes
    /// against an already-warm threshold instead of cold-starting.
    fn or_topk_into(&self, tokens: &[String], scorer: &MemBM25Scorer, topk: &mut TopK) {
        // Single-term top-k: block-max skip. Multi-term: MaxScore below.
        if tokens.len() == 1 {
            if let Some(id) = self.term_id(&tokens[0]) {
                let qw = scorer.query_weight(&tokens[0]);
                self.score_single_into(id, qw, scorer, topk);
            }
            return;
        }
        let mut lanes: Vec<WandLane> = Vec::with_capacity(tokens.len());
        for token in tokens {
            if let Some(id) = self.term_id(token) {
                let qw = scorer.query_weight(token);
                let cursor = PostingCursor::new(self, id);
                let doc = cursor.doc();
                lanes.push(WandLane {
                    cursor,
                    qw,
                    // Exact term max contribution (tightest sound bound): the
                    // max over the term's per-block impact frontiers. A loose
                    // `doc_weight(max_freq, min_dl)` would over-bound and keep
                    // terms "essential" longer, shrinking MaxScore's prune.
                    ub: self.term_ub(id, scorer, qw),
                    doc,
                });
            }
        }
        lanes.retain(|l| l.doc.is_some());
        if lanes.is_empty() {
            return;
        }
        self.maxscore_loop(lanes, scorer, topk);
    }

    /// MaxScore top-k over an OR query. Terms are sorted by their max
    /// contribution (`ub`) ascending; the longest prefix whose `ub` sum can't
    /// reach the current threshold is "non-essential" — a doc matching only
    /// those can't enter the top-k, so candidates are generated from the
    /// "essential" suffix alone (iterating fewer lists than WAND) and the
    /// non-essential lists are only probed per candidate, highest-`ub` first,
    /// with an early exit once the remaining bound can't beat the threshold.
    /// Exact: same bounds as WAND, just a different traversal.
    fn maxscore_loop(&self, mut lanes: Vec<WandLane>, scorer: &MemBM25Scorer, topk: &mut TopK) {
        // Fixed ascending-`ub` order so the non-essential prefix is well-defined.
        lanes.sort_by(|a, b| a.ub.partial_cmp(&b.ub).unwrap_or(std::cmp::Ordering::Equal));
        loop {
            let theta = topk.threshold();
            // Non-essential prefix [0..ne): cumulative `ub` sum <= theta.
            let mut ne = 0;
            let mut cum = 0.0f32;
            while ne < lanes.len() && cum + lanes[ne].ub <= theta {
                cum += lanes[ne].ub;
                ne += 1;
            }
            if ne == lanes.len() {
                break; // no remaining doc can reach theta
            }
            // Candidate: min current doc among the essential lanes.
            let mut cand: Option<u32> = None;
            for l in &lanes[ne..] {
                if let Some(d) = l.doc {
                    cand = Some(cand.map_or(d, |c| c.min(d)));
                }
            }
            let Some(cand) = cand else {
                break; // essential lanes exhausted
            };
            let dl = self.docs.num_tokens(cand);
            let mut score = 0.0f32;
            for l in lanes[ne..].iter_mut() {
                if l.doc == Some(cand) {
                    score += l.qw * scorer.doc_weight(l.cursor.freq(), dl);
                }
            }
            // Probe non-essential lanes high-`ub` first; stop once even the max
            // remaining contribution can't lift the score past theta.
            let mut rem = cum;
            let mut alive = true;
            for i in (0..ne).rev() {
                if score + rem <= theta {
                    alive = false;
                    break;
                }
                rem -= lanes[i].ub;
                lanes[i].cursor.skip_to(cand);
                lanes[i].doc = lanes[i].cursor.doc();
                if lanes[i].doc == Some(cand) {
                    score += lanes[i].qw * scorer.doc_weight(lanes[i].cursor.freq(), dl);
                }
            }
            if alive {
                topk.offer(score, doc_set_key(&self.docs, cand));
            }
            // Advance the essential lanes that were positioned at the candidate.
            for l in lanes[ne..].iter_mut() {
                if l.doc == Some(cand) {
                    l.cursor.advance();
                    l.doc = l.cursor.doc();
                }
            }
            lanes.retain(|l| l.doc.is_some());
            if lanes.is_empty() {
                break;
            }
        }
    }

    /// Phrase-search by intersecting posting lists: drive from the rarest
    /// token, require every other token to contain the doc, and verify the
    /// token positions satisfy the phrase. `tokens.len() >= 2`.
    fn search_phrase(&self, tokens: &[String], slop: u32, scorer: &MemBM25Scorer) -> Vec<FtsEntry> {
        let mut term_ids: Vec<u32> = Vec::with_capacity(tokens.len());
        for token in tokens {
            match self.term_id(token) {
                Some(id) => term_ids.push(id),
                // A phrase needs every token present in this partition.
                None => return Vec::new(),
            }
        }
        let rarest = (0..term_ids.len())
            .min_by_key(|&i| self.postings[term_ids[i] as usize].doc_count)
            .unwrap();
        let mut cursors: Vec<PostingCursor> = term_ids
            .iter()
            .map(|&id| PostingCursor::new(self, id))
            .collect();
        let mut results = Vec::new();
        // Reused across candidate docs to avoid per-doc allocation (the hot
        // phrase cost when the rarest term still has a high doc count).
        let mut all_positions: Vec<Vec<u32>> = vec![Vec::new(); tokens.len()];
        let mut freqs = vec![0u32; tokens.len()];
        while let Some(doc) = cursors[rarest].cursor_doc() {
            let mut present = true;
            for ti in 0..tokens.len() {
                if ti != rarest {
                    cursors[ti].skip_to(doc);
                }
                if cursors[ti].doc() == Some(doc) {
                    freqs[ti] = cursors[ti].freq();
                    all_positions[ti].clear();
                    all_positions[ti].extend_from_slice(cursors[ti].positions());
                } else {
                    present = false;
                    break;
                }
            }
            if present && phrase_matches(&all_positions, slop) {
                let dl = self.docs.num_tokens(doc);
                let score: f32 = tokens
                    .iter()
                    .zip(&freqs)
                    .map(|(t, &f)| scorer.query_weight(t) * scorer.doc_weight(f, dl))
                    .sum();
                let key = doc_set_key(&self.docs, doc);
                results.push(FtsEntry {
                    row_position: key.row_position,
                    doc_index: public_doc_index(&key.doc_index),
                    score,
                });
            }
            cursors[rarest].advance();
        }
        results
    }

    fn search_phrase_groups(
        &self,
        groups: &[Vec<String>],
        slop: u32,
        scorer: &MemBM25Scorer,
    ) -> Vec<FtsEntry> {
        let mut candidates: Option<HashMap<u32, PhraseCandidate>> = None;
        for (group_idx, group) in groups.iter().enumerate() {
            let group_docs = self.phrase_group_docs(group, scorer);
            if group_docs.is_empty()
                || !merge_phrase_group(&mut candidates, group_idx, groups.len(), group_docs)
            {
                return Vec::new();
            }
        }

        candidates
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, candidate)| phrase_matches(&candidate.positions_by_group, slop))
            .map(|(doc, candidate)| FtsEntry {
                row_position: self.docs.row_id(doc),
                doc_index: public_doc_index(&self.docs.doc_index(doc)),
                score: candidate.score,
            })
            .collect()
    }

    fn phrase_group_docs(
        &self,
        group: &[String],
        scorer: &MemBM25Scorer,
    ) -> HashMap<u32, PhraseGroupDoc> {
        let mut docs: HashMap<u32, PhraseGroupDoc> = HashMap::new();
        for token in group {
            let Some(term_id) = self.term_id(token) else {
                continue;
            };
            let qw = scorer.query_weight(token);
            let mut cursor = PostingCursor::new(self, term_id);
            while let Some(doc) = cursor.cursor_doc() {
                let positions = cursor.positions().to_vec();
                let freq = cursor.freq();
                let dl = self.docs.num_tokens(doc);
                let entry = docs.entry(doc).or_default();
                entry.positions.extend_from_slice(&positions);
                entry.score += qw * scorer.doc_weight(freq, dl);
                cursor.advance();
            }
        }
        for doc in docs.values_mut() {
            doc.positions.sort_unstable();
            doc.positions.dedup();
        }
        docs
    }
}

/// A scored MemTable row, ordered by score then row position (`total_cmp`,
/// so a stray non-finite score cannot panic the heap).
struct ScoredEntry {
    score: f32,
    key: DocumentKey,
}

impl PartialEq for ScoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for ScoredEntry {}
impl PartialOrd for ScoredEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then(self.key.cmp(&other.key))
    }
}

/// A bounded top-k collector shared across all partitions and the tail of a
/// single query. `threshold()` is the score of the weakest entry once full —
/// the WAND pruning bound — and rises monotonically as entries are offered.
struct TopK {
    k: usize,
    heap: BinaryHeap<Reverse<ScoredEntry>>,
}

impl TopK {
    fn new(k: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k + 1),
        }
    }

    /// Score a doc must beat to enter the top-k (`-inf` until the heap fills).
    fn threshold(&self) -> f32 {
        if self.heap.len() >= self.k {
            self.heap.peek().unwrap().0.score
        } else {
            f32::NEG_INFINITY
        }
    }

    fn offer(&mut self, score: f32, key: DocumentKey) {
        if self.heap.len() < self.k {
            self.heap.push(Reverse(ScoredEntry { score, key }));
        } else if score > self.heap.peek().unwrap().0.score {
            self.heap.pop();
            self.heap.push(Reverse(ScoredEntry { score, key }));
        }
    }

    fn into_entries(self) -> Vec<FtsEntry> {
        self.heap
            .into_iter()
            .map(|Reverse(e)| FtsEntry {
                row_position: e.key.row_position,
                doc_index: public_doc_index(&e.key.doc_index),
                score: e.score,
            })
            .collect()
    }
}

/// One WAND lane: a posting cursor plus its query weight and score bound.
struct WandLane<'a> {
    cursor: PostingCursor<'a>,
    qw: f32,
    /// Upper bound on this term's contribution to any doc's score.
    ub: f32,
    /// Cached `cursor.doc()`, refreshed only when the lane moves. The WAND loop
    /// reads this field (not `cursor.doc()`) in its per-iteration retain / sort /
    /// pivot, which dominates OR latency (~20 iterations per scored doc).
    doc: Option<u32>,
}

/// A decoding cursor over one term's compressed posting list. Decodes a
/// 128-doc block at a time; `skip_to` jumps whole blocks via `BlockMeta`
/// without decoding them.
struct PostingCursor<'a> {
    part: &'a Partition,
    /// Forward-only parser over the term's block metadata.
    reader: BlockReader<'a>,
    /// Metadata of the block currently decoded into `docs`/`freqs`, or `None`
    /// when the posting list is exhausted.
    cur: Option<BlockMeta<'a>>,
    /// Whether `docs`/`freqs` hold `cur`'s payload (false while `skip_to` walks
    /// past whole blocks without decoding them).
    decoded: bool,
    docs: Vec<u32>,
    freqs: Vec<u32>,
    /// freq prefix sum of the current block (`len == freqs.len() + 1`), indexing
    /// the position stream for random per-doc access. Valid when `prefix_valid`.
    prefix: Vec<u32>,
    prefix_valid: bool,
    /// scratch for the most recently decoded doc's positions.
    pos_scratch: Vec<u32>,
    /// index within the current block.
    i: usize,
    /// SIMD bit-(un)packer for full 128-doc blocks.
    bp: BitPacker4x,
}

impl<'a> PostingCursor<'a> {
    fn new(part: &'a Partition, term_id: u32) -> Self {
        let pref = part.postings[term_id as usize];
        let mut reader = BlockReader::new(part, &pref);
        let cur = reader.next_meta();
        let mut cursor = Self {
            part,
            reader,
            cur,
            decoded: false,
            docs: Vec::new(),
            freqs: Vec::new(),
            prefix: Vec::new(),
            prefix_valid: false,
            pos_scratch: Vec::new(),
            i: 0,
            bp: BitPacker4x::new(),
        };
        if let Some(bm) = cursor.cur {
            cursor.decode(bm);
        }
        cursor
    }

    /// Decode the doc ids + freqs of block `bm` into `docs`/`freqs`.
    fn decode(&mut self, bm: BlockMeta<'a>) {
        let df_start = bm.df_offset as usize;
        unpack_doc_block(
            &self.bp,
            &self.part.doc_freq_data,
            df_start,
            bm.n,
            bm.doc_width,
            bm.first_doc,
            &mut self.docs,
        );
        let freq_start = df_start + bitpack_len(bm.n, bm.doc_width);
        unpack_block(
            &self.bp,
            &self.part.doc_freq_data,
            freq_start,
            bm.n,
            bm.freq_width,
            &mut self.freqs,
        );
        self.decoded = true;
        self.prefix_valid = false;
    }

    /// Move to the next block (parse + decode it), or exhaust.
    fn next_block(&mut self) {
        self.cur = self.reader.next_meta();
        self.i = 0;
        self.prefix_valid = false;
        self.decoded = false;
        if let Some(bm) = self.cur {
            self.decode(bm);
        }
    }

    /// Ensure `prefix` holds the freq prefix sum of the current block.
    fn ensure_prefix(&mut self) {
        if self.prefix_valid {
            return;
        }
        self.prefix.clear();
        self.prefix.push(0);
        let mut sum = 0u32;
        for &f in &self.freqs {
            sum += f;
            self.prefix.push(sum);
        }
        self.prefix_valid = true;
    }

    /// Current doc id, or `None` once the list is exhausted.
    fn doc(&self) -> Option<u32> {
        self.cur?;
        self.docs.get(self.i).copied()
    }

    /// `doc()` under a `&mut` receiver — for use as a loop condition while the
    /// cursor is borrowed mutably elsewhere.
    fn cursor_doc(&mut self) -> Option<u32> {
        self.doc()
    }

    /// Frequency of the current posting.
    fn freq(&self) -> u32 {
        self.freqs[self.i]
    }

    /// Positions of the current posting — decoded on demand for this one
    /// document only (random access into the block's position stream).
    fn positions(&mut self) -> &[u32] {
        self.ensure_prefix();
        let bm = self.cur.expect("positions() on exhausted cursor");
        let s = self.prefix[self.i] as usize;
        let n = self.prefix[self.i + 1] as usize - s;
        self.pos_scratch.clear();
        bitpack_read_at(
            &self.part.pos_data,
            bm.pos_offset as usize,
            s,
            n,
            bm.pos_width,
            &mut self.pos_scratch,
        );
        // un-delta in place.
        let mut last = 0u32;
        for d in &mut self.pos_scratch {
            last += *d;
            *d = last;
        }
        &self.pos_scratch
    }

    /// Step to the next posting.
    fn advance(&mut self) {
        self.i += 1;
        if self.i >= self.docs.len() {
            self.next_block();
        }
    }

    /// Advance to the first posting with `doc_id >= target` (or exhaust),
    /// skipping whole blocks via their `last_doc` without decoding them.
    fn skip_to(&mut self, target: u32) {
        if self.doc().is_some_and(|d| d >= target) {
            return;
        }
        loop {
            let Some(bm) = self.cur else {
                return; // exhausted
            };
            if bm.last_doc >= target {
                if !self.decoded {
                    self.decode(bm);
                }
                // `last_doc >= target`, so this block holds a doc >= target.
                self.i += self.docs[self.i..].partition_point(|&d| d < target);
                return;
            }
            // Whole block precedes `target`; skip it without decoding.
            self.cur = self.reader.next_meta();
            self.i = 0;
            self.decoded = false;
            self.prefix_valid = false;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{Array, Int32Array, ListArray, StringArray, StructArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use lance_index::scalar::inverted::DocumentGranularity;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, true),
        ]))
    }

    /// A position-enabled index — required for phrase search. The default
    /// `InvertedIndexParams` (`with_position = false`) indexes no positions, so
    /// phrase tests must opt in, mirroring the on-disk / Lucene contract.
    fn position_index(field_id: i32, column: &str) -> FtsMemIndex {
        FtsMemIndex::with_params(
            field_id,
            column.to_string(),
            InvertedIndexParams::default().with_position(true),
        )
    }

    fn create_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec![
                    "hello world",
                    "goodbye world",
                    "hello again",
                ])),
            ],
        )
        .unwrap()
    }

    fn create_element_test_batch() -> RecordBatch {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("alpha beta");
        tags.values().append_null();
        tags.values().append_value("");
        tags.values().append_value("beta gamma");
        tags.append(true);
        tags.values().append_value("alpha");
        tags.values().append_value("beta");
        tags.append(true);
        let tags = tags.finish();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1])), Arc::new(tags)],
        )
        .unwrap()
    }

    #[test]
    fn test_element_document_search_and_flush_identity() {
        let params = InvertedIndexParams::default()
            .with_position(true)
            .document_granularity(DocumentGranularity::ListElement);
        let index = FtsMemIndex::try_with_params(1, "tags".to_string(), params)
            .unwrap()
            .with_freeze_threshold_rows(1);
        assert_eq!(index.column_name(), "tags");
        assert_eq!(index.source_column_name(), "tags");

        index.insert(&create_element_test_batch(), 0).unwrap();
        assert_eq!(index.doc_count(), 6);

        let mut beta = index.search("beta");
        beta.sort_by_key(|entry| (entry.row_position, entry.doc_index.clone()));
        assert_eq!(
            beta.iter()
                .map(|entry| (entry.row_position, entry.doc_index.clone()))
                .collect::<Vec<_>>(),
            vec![(0, Some(vec![0])), (0, Some(vec![3])), (1, Some(vec![1]))]
        );

        let phrase = index.search_phrase("beta gamma", 0);
        assert_eq!(phrase.len(), 1);
        assert_eq!(
            (phrase[0].row_position, phrase[0].doc_index.clone()),
            (0, Some(vec![3]))
        );

        let conjunctive = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::match_query("alpha"))
            .must(FtsQueryExpr::match_query("beta"))
            .build();
        let results = index.search_query(&conjunctive);
        assert_eq!(results.len(), 1);
        assert_eq!(
            (results[0].row_position, results[0].doc_index.clone()),
            (0, Some(vec![0]))
        );

        index.to_index_builder(7, 2).unwrap();
    }

    #[test]
    fn test_nested_element_documents_keep_all_list_ordinals() {
        let doc_fields = Fields::from(vec![Field::new("content", DataType::Utf8, true)]);
        let doc_values = StructArray::new(
            doc_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["alpha", "beta", "alpha"]))],
            None,
        );
        let doc_item = Arc::new(Field::new("item", DataType::Struct(doc_fields), true));
        let docs_type = DataType::List(doc_item.clone());
        let docs = ListArray::new(
            doc_item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3])),
            Arc::new(doc_values),
            None,
        );
        let group_fields = Fields::from(vec![Field::new("docs", docs_type, true)]);
        let group_values = StructArray::new(group_fields.clone(), vec![Arc::new(docs)], None);
        let group_item = Arc::new(Field::new("item", DataType::Struct(group_fields), true));
        let groups = ListArray::new(
            group_item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2])),
            Arc::new(group_values),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "groups",
                groups.data_type().clone(),
                true,
            )])),
            vec![Arc::new(groups)],
        )
        .unwrap();
        let index = FtsMemIndex::try_with_params(
            1,
            "groups.docs.content".to_string(),
            InvertedIndexParams::default().document_granularity(DocumentGranularity::ListElement),
        )
        .unwrap()
        .with_freeze_threshold_rows(1);

        index.insert(&batch, 7).unwrap();
        let mut entries = index.search("alpha");
        entries.sort_by_key(|entry| entry.doc_index.clone());
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.row_position, entry.doc_index.clone()))
                .collect::<Vec<_>>(),
            vec![(7, Some(vec![0, 0])), (7, Some(vec![1, 1]))]
        );

        index.insert(&batch, 8).unwrap();
        let state = index.state.load_full();
        assert_eq!(state.partitions.len(), 2);
        let merged = Partition::merge(&state.partitions).unwrap();
        assert_eq!(merged.docs.coordinate_rank(), 2);
        assert_eq!(
            (0..merged.docs.len() as u32)
                .map(|doc_id| doc_set_key(&merged.docs, doc_id))
                .collect::<Vec<_>>(),
            vec![
                DocumentKey {
                    row_position: 7,
                    doc_index: vec![0, 0],
                },
                DocumentKey {
                    row_position: 7,
                    doc_index: vec![1, 0],
                },
                DocumentKey {
                    row_position: 7,
                    doc_index: vec![1, 1],
                },
                DocumentKey {
                    row_position: 8,
                    doc_index: vec![0, 0],
                },
                DocumentKey {
                    row_position: 8,
                    doc_index: vec![1, 0],
                },
                DocumentKey {
                    row_position: 8,
                    doc_index: vec![1, 1],
                },
            ]
        );
        index.to_index_builder(8, 9).unwrap();
    }

    #[test]
    fn test_failed_background_merge_releases_pending_slot() {
        let slot = Mutex::new(Some(PendingMerge {
            sources: vec![1],
            result: None,
        }));

        publish_pending_merge(
            &slot,
            Err(Error::index("inconsistent test partitions".to_string())),
        );

        assert!(slot.lock().unwrap().is_none());
    }

    #[test]
    fn test_zero_token_element_documents_remain_addressable_after_flush() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_null();
        tags.values().append_value("");
        tags.values().append_value("!!!");
        tags.append(true);
        let tags = tags.finish();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "tags",
                tags.data_type().clone(),
                true,
            )])),
            vec![Arc::new(tags)],
        )
        .unwrap();
        let index = FtsMemIndex::try_with_params(
            1,
            "tags".to_string(),
            InvertedIndexParams::default().document_granularity(DocumentGranularity::ListElement),
        )
        .unwrap();

        index.insert(&batch, 0).unwrap();
        index.flush();

        assert!(!index.is_empty());
        assert_eq!(index.doc_count(), 3);
        assert!(index.search("missing").is_empty());
    }

    #[test]
    fn test_fts_index_insert_and_search() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        assert_eq!(index.doc_count(), 3);

        let entries = index.search("hello");
        assert_eq!(entries.len(), 2);

        let entries = index.search("world");
        assert_eq!(entries.len(), 2);

        let entries = index.search("goodbye");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 1);

        let entries = index.search("nonexistent");
        assert!(entries.is_empty());
    }

    #[test]
    fn test_code_analyzer_and_query_uses_position_alternatives() {
        let schema = create_test_schema();
        let index = FtsMemIndex::with_params(
            1,
            "description".to_string(),
            InvertedIndexParams::code().split_identifiers(true),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["get user name", "getUserName"])),
            ],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query_with_operator("getUserName", Operator::And);
        let mut rows = index
            .search_query(&query)
            .into_iter()
            .map(|entry| entry.row_position)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 1]);
    }

    #[test]
    fn test_code_analyzer_queries_keep_element_document_identity() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("get");
        tags.values().append_value("x user name");
        tags.append(true);
        tags.values().append_value("getUserName");
        tags.append(true);
        let tags = tags.finish();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "tags",
                tags.data_type().clone(),
                true,
            )])),
            vec![Arc::new(tags)],
        )
        .unwrap();
        let index = FtsMemIndex::try_with_params(
            1,
            "tags".to_string(),
            InvertedIndexParams::code()
                .with_position(true)
                .split_identifiers(true)
                .document_granularity(DocumentGranularity::ListElement),
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        let and_query = FtsQueryExpr::match_query_with_operator("getUserName", Operator::And);
        let phrase_query = FtsQueryExpr::phrase("getUserName");
        for entries in [
            index.search_query(&and_query),
            index.search_query(&phrase_query),
        ] {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row_position, 1);
            assert_eq!(entries[0].doc_index, Some(vec![0]));
        }

        index.flush();
        let partition_only = SearchOptions::new().with_include_tail(false);
        for entries in [
            index.search_with_options(&and_query, partition_only.clone()),
            index.search_with_options(&phrase_query, partition_only),
        ] {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row_position, 1);
            assert_eq!(entries[0].doc_index, Some(vec![0]));
        }
    }

    fn tail_positions_for(index: &FtsMemIndex, term: &str, row_position: RowPosition) -> Vec<u32> {
        let st = index.state.load_full();
        let snap = st.tail.snapshot();
        let entry = st.tail.terms.get(term).expect("term should be indexed");
        let slice = entry.value().load();
        for chunk in slice.chunks() {
            if chunk.batch_position >= snap.visible_count {
                continue;
            }
            if let Ok(doc_idx) = chunk.row_positions.binary_search(&row_position) {
                return chunk
                    .positions
                    .as_ref()
                    .expect("test index stores positions")
                    .doc_positions(doc_idx)
                    .to_vec();
            }
        }
        panic!("term {term} should be present in row {row_position}");
    }

    #[test]
    fn test_code_analyzer_phrase_uses_token_positions_and_alternatives() {
        let schema = create_test_schema();
        let index = FtsMemIndex::with_params(
            1,
            "description".to_string(),
            InvertedIndexParams::code()
                .with_position(true)
                .split_identifiers(true),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "get user name",
                    "getUserName",
                    "get user",
                    "get fast user name",
                ])),
            ],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        assert_eq!(tail_positions_for(&index, "getusername", 1), vec![0]);
        assert_eq!(tail_positions_for(&index, "get", 1), vec![0]);
        assert_eq!(tail_positions_for(&index, "user", 1), vec![1]);
        assert_eq!(tail_positions_for(&index, "name", 1), vec![2]);

        let query = FtsQueryExpr::phrase("getUserName");
        assert_eq!(rows(index.search_phrase("getUserName", 0)), vec![0, 1]);
        assert_eq!(rows(index.search_query(&query)), vec![0, 1]);

        index.flush();
        let partition_only = SearchOptions::new().with_include_tail(false);
        assert_eq!(
            rows(index.search_with_options(&query, partition_only)),
            vec![0, 1]
        );
    }

    #[test]
    fn test_zero_token_documents_are_skipped_across_memwal_paths() {
        let params =
            InvertedIndexParams::new("whitespace".to_string(), lance_tokenizer::Language::English)
                .remove_stop_words(true)
                .stem(false)
                .max_token_length(Some(6));
        let schema = create_test_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some(""),
                    Some("   "),
                    Some("the"),
                    Some("overlength"),
                    None,
                    Some("hello"),
                ])),
            ],
        )
        .unwrap();
        let index = FtsMemIndex::with_params(1, "description".to_string(), params.clone());
        index.insert(&batch, 0).unwrap();

        assert_eq!(index.doc_count(), 1);
        let st = index.state.load_full();
        let tail_snap = st.tail.snapshot();
        let tokens = vec!["hello".to_string()];
        let tail_scorer = build_scorer(&st, &tail_snap, &tokens, true);
        let expected_scorer = MemBM25Scorer::new(1, 1, HashMap::from([("hello".to_string(), 1)]));
        assert_eq!(tail_scorer.total_tokens, 1);
        assert_eq!(tail_scorer.num_docs(), 1);
        assert_eq!(tail_scorer.num_docs_containing_token("hello"), 1);
        assert_eq!(
            tail_scorer.avg_doc_length(),
            expected_scorer.avg_doc_length()
        );
        assert_eq!(
            tail_scorer.query_weight("hello"),
            expected_scorer.query_weight("hello")
        );
        let tail_results = index.search("hello");
        assert_eq!(rows(tail_results.clone()), vec![5]);
        let tail_score = tail_results[0].score;
        assert!(!index.to_index_builder(0, 6).unwrap().is_empty());

        index.flush();
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 1);
        assert_eq!(
            st.partitions[0]
                .docs
                .iter()
                .map(|(row_id, num_tokens)| (*row_id, *num_tokens))
                .collect::<Vec<_>>(),
            vec![(5, 1)]
        );
        let frozen_scorer = build_scorer(&st, &st.tail.snapshot(), &tokens, true);
        assert_eq!(frozen_scorer.total_tokens, 1);
        assert_eq!(frozen_scorer.num_docs(), 1);
        assert_eq!(frozen_scorer.num_docs_containing_token("hello"), 1);
        assert_eq!(
            frozen_scorer.avg_doc_length(),
            expected_scorer.avg_doc_length()
        );
        assert_eq!(
            frozen_scorer.query_weight("hello"),
            expected_scorer.query_weight("hello")
        );
        let frozen_results = index.search("hello");
        assert_eq!(rows(frozen_results.clone()), vec![5]);
        assert!((frozen_results[0].score - tail_score).abs() < f32::EPSILON);

        let all_zero_batch = batch.slice(0, 5);
        let all_zero_index = FtsMemIndex::with_params(1, "description".to_string(), params);
        all_zero_index.insert(&all_zero_batch, 0).unwrap();
        assert!(all_zero_index.is_empty());
        assert_eq!(all_zero_index.doc_count(), 0);
        assert!(all_zero_index.to_index_builder(0, 5).unwrap().is_empty());
        all_zero_index.flush();
        assert!(all_zero_index.state.load().partitions.is_empty());
    }

    fn create_phrase_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "alpha beta gamma",
                    "beta alpha gamma",
                    "alpha delta beta gamma",
                    "alpha gamma",
                    "alpha delta epsilon beta gamma",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_phrase_search_exact_match() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_phrase_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_phrase("alpha beta", 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 0);

        let batch2 = create_test_batch(&schema);
        let index2 = position_index(1, "description");
        index2.insert(&batch2, 0).unwrap();

        let entries = index2.search_phrase("hello world", 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 0);

        let entries = index2.search_phrase("goodbye world", 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 1);
    }

    #[test]
    fn test_phrase_search_with_slop() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_phrase_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_phrase("alpha beta", 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 0);

        let entries = index.search_phrase("alpha beta", 1);
        assert_eq!(entries.len(), 2);
        let positions: Vec<_> = entries.iter().map(|e| e.row_position).collect();
        assert!(positions.contains(&0));
        assert!(positions.contains(&2));

        let entries = index.search_phrase("alpha beta", 2);
        assert_eq!(entries.len(), 3);

        let entries = index.search_phrase("alpha gamma", 0);
        assert_eq!(entries.len(), 2);

        let entries = index.search_phrase("alpha gamma", 1);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_phrase_search_no_match() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_phrase_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_phrase("beta alpha", 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 1);

        let entries = index.search_phrase("nonexistent phrase", 0);
        assert!(entries.is_empty());

        let entries = index.search_phrase("alpha hello", 0);
        assert!(entries.is_empty());

        let entries = index.search_phrase("gamma alpha", 0);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_phrase_search_single_token() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_phrase_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let phrase_entries = index.search_phrase("alpha", 0);
        let search_entries = index.search("alpha");
        assert_eq!(phrase_entries.len(), search_entries.len());
    }

    #[test]
    fn test_phrase_search_empty() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_phrase("", 0);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_without_positions_disables_phrase_and_skips_storage() {
        // Default params (`with_position = false`): the index stores no token
        // positions, so term/BM25 search works but phrase search does not — the
        // same contract as the on-disk format and Lucene `DOCS_AND_FREQS`.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(1);
        index.insert(&create_test_batch(&schema), 0).unwrap();

        assert_eq!(index.search("hello").len(), 2);
        assert!(
            index.search_phrase("hello world", 0).is_empty(),
            "phrase search requires positions"
        );
        // The frozen partition stored no positions.
        let (_, _, _, _, _, pos, _, _) = index.memory_breakdown();
        assert_eq!(pos, 0, "no positions stored when with_position = false");

        // With positions enabled the same phrase matches.
        let with_pos = position_index(1, "description").with_freeze_threshold_rows(1);
        with_pos.insert(&create_test_batch(&schema), 0).unwrap();
        assert_eq!(with_pos.search_phrase("hello world", 0).len(), 1);
        let (_, _, _, _, _, pos2, _, _) = with_pos.memory_breakdown();
        assert!(pos2 > 0, "positions stored when with_position = true");
    }

    fn create_boolean_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "rust programming language",
                    "python programming language",
                    "rust web server",
                    "python web framework",
                    "javascript programming",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_boolean_must_only() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let rust = FtsQueryExpr::match_query("rust").with_boost(2.0);
        let programming = FtsQueryExpr::match_query("programming").with_boost(3.0);
        let rust_score = index
            .search_query(&rust)
            .into_iter()
            .find(|entry| entry.row_position == 0)
            .unwrap()
            .score;
        let programming_score = index
            .search_query(&programming)
            .into_iter()
            .find(|entry| entry.row_position == 0)
            .unwrap()
            .score;
        let query = FtsQueryExpr::boolean()
            .must(rust.clone())
            .must(programming.clone())
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 0);
        let expected_score = rust_score + programming_score;
        assert!((entries[0].score - expected_score).abs() < 1e-6);

        let reversed = FtsQueryExpr::boolean().must(programming).must(rust).build();
        let reversed_entries = index.search_query(&reversed);
        assert_eq!(reversed_entries.len(), 1);
        assert!((reversed_entries[0].score - expected_score).abs() < 1e-6);
    }

    #[test]
    fn test_boolean_should_only() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .should(FtsQueryExpr::match_query("rust"))
            .should(FtsQueryExpr::match_query("python"))
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 4);
        let positions: Vec<_> = entries.iter().map(|e| e.row_position).collect();
        assert!(positions.contains(&0));
        assert!(positions.contains(&1));
        assert!(positions.contains(&2));
        assert!(positions.contains(&3));
    }

    #[test]
    fn test_boolean_must_not_only() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must_not(FtsQueryExpr::match_query("rust"))
            .build();

        let entries = index.search_query(&query);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_boolean_must_with_should() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::match_query("programming"))
            .should(FtsQueryExpr::match_query("rust"))
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 3);

        let doc0 = entries.iter().find(|e| e.row_position == 0).unwrap();
        let doc1 = entries.iter().find(|e| e.row_position == 1).unwrap();
        assert!(doc0.score > doc1.score);
    }

    #[test]
    fn test_boolean_must_with_must_not() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::match_query("programming"))
            .must_not(FtsQueryExpr::match_query("python"))
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 2);

        let positions: Vec<_> = entries.iter().map(|e| e.row_position).collect();
        assert!(positions.contains(&0));
        assert!(positions.contains(&4));
        assert!(!positions.contains(&1));
    }

    #[test]
    fn test_boolean_combined() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::match_query("web"))
            .should(FtsQueryExpr::match_query("rust"))
            .must_not(FtsQueryExpr::match_query("framework"))
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 2);
    }

    #[test]
    fn test_boolean_nested_phrase() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_boolean_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::phrase("programming language"))
            .build();

        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 2);
        let positions: Vec<_> = entries.iter().map(|e| e.row_position).collect();
        assert!(positions.contains(&0));
        assert!(positions.contains(&1));
    }

    #[test]
    fn test_search_query_match() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("hello");
        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_search_query_phrase() {
        let schema = create_test_schema();
        let index = position_index(1, "description");

        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::phrase("hello world");
        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].row_position, 0);
    }

    #[test]
    fn test_search_query_with_boost() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query_no_boost = FtsQueryExpr::match_query("hello");
        let query_with_boost = FtsQueryExpr::match_query("hello").with_boost(2.0);

        let entries_no_boost = index.search_query(&query_no_boost);
        let entries_with_boost = index.search_query(&query_with_boost);

        assert_eq!(entries_no_boost.len(), entries_with_boost.len());
        for e1 in &entries_no_boost {
            let e2 = entries_with_boost
                .iter()
                .find(|e| e.row_position == e1.row_position)
                .unwrap();
            let expected = e1.score * 2.0;
            assert!((e2.score - expected).abs() < 0.001);
        }
    }

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("hello", "hello"), 0);
        assert_eq!(levenshtein_distance("hello", "hallo"), 1);
        assert_eq!(levenshtein_distance("hello", "hell"), 1);
        assert_eq!(levenshtein_distance("hello", "helloo"), 1);
        assert_eq!(levenshtein_distance("hello", "hxllo"), 1);
        assert_eq!(levenshtein_distance("hello", "hxxlo"), 2);
        assert_eq!(levenshtein_distance("abc", "xyz"), 3);
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("hello", ""), 5);
        assert_eq!(levenshtein_distance("", "hello"), 5);
        assert_eq!(levenshtein_distance("Hello", "hello"), 1);
    }

    #[test]
    fn test_auto_fuzziness() {
        assert_eq!(auto_fuzziness(""), 0);
        assert_eq!(auto_fuzziness("a"), 0);
        assert_eq!(auto_fuzziness("ab"), 0);
        assert_eq!(auto_fuzziness("abc"), 1);
        assert_eq!(auto_fuzziness("abcd"), 1);
        assert_eq!(auto_fuzziness("abcde"), 1);
        assert_eq!(auto_fuzziness("abcdef"), 2);
        assert_eq!(auto_fuzziness("programming"), 2);
    }

    fn create_fuzzy_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "alpha beta gamma",
                    "alpho beta delta",
                    "alpha delta epsilon",
                    "omega zeta",
                    "alphax gamma",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_expand_fuzzy_exact_match() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let matches = index.expand_fuzzy("alpha", 0, 50);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "alpha");
        assert_eq!(matches[0].1, 0);

        let matches = index.expand_fuzzy("nonexistent", 0, 50);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_expand_fuzzy_single_edit() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let matches = index.expand_fuzzy("alpho", 1, 50);
        assert!(matches.iter().any(|(t, d)| t == "alpha" && *d == 1));
        assert!(matches.iter().any(|(t, _)| t == "alpho"));
    }

    #[test]
    fn test_expand_fuzzy_max_expansions() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let matches = index.expand_fuzzy("a", 10, 3);
        assert!(matches.len() <= 3);
    }

    #[test]
    fn test_search_fuzzy_basic() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_fuzzy("alpho", Some(1), 50);
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_search_fuzzy_auto_fuzziness() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_fuzzy("alpho", None, 50);
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_search_fuzzy_no_match() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let entries = index.search_fuzzy("xyz", Some(0), 50);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_search_query_fuzzy() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::fuzzy("alpho");
        let entries = index.search_query(&query);
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_search_query_fuzzy_with_distance() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::fuzzy_with_distance("alpho", 1);
        let entries = index.search_query(&query);
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_search_query_fuzzy_respects_prefix_length() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::fuzzy_with_options("alpha", Some(5), 2, 50);
        let entries = index.search_query(&query);
        assert!(!entries.is_empty());
        assert!(
            entries.iter().all(|entry| entry.row_position != 3),
            "the omega row is within edit distance but does not share the required prefix"
        );
    }

    #[test]
    fn test_search_query_fuzzy_prefix_length_uses_char_boundaries() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["éclair", "xclair"])),
            ],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::fuzzy_with_options("éclair", Some(2), 1, 50);
        let entries = index.search_query(&query);
        assert!(!entries.is_empty());
        assert!(
            entries.iter().all(|entry| entry.row_position != 1),
            "the ASCII-prefixed row is within edit distance but not the character prefix"
        );
    }

    #[test]
    fn test_search_query_fuzzy_with_boost() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query_no_boost = FtsQueryExpr::fuzzy("alpho");
        let query_with_boost = FtsQueryExpr::fuzzy("alpho").with_boost(2.0);

        let entries_no_boost = index.search_query(&query_no_boost);
        let entries_with_boost = index.search_query(&query_with_boost);
        assert_eq!(entries_no_boost.len(), entries_with_boost.len());

        for e1 in &entries_no_boost {
            let e2 = entries_with_boost
                .iter()
                .find(|e| e.row_position == e1.row_position)
                .unwrap();
            let expected = e1.score * 2.0;
            assert!((e2.score - expected).abs() < 0.001);
        }
    }

    #[test]
    fn test_boolean_with_fuzzy() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_fuzzy_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .must(FtsQueryExpr::fuzzy_with_distance("alpho", 1))
            .must_not(FtsQueryExpr::match_query("delta"))
            .build();

        let entries = index.search_query(&query);
        let positions: Vec<_> = entries.iter().map(|e| e.row_position).collect();
        assert!(!positions.contains(&1));
        assert!(!positions.contains(&2));
        assert!(positions.contains(&0));
    }

    fn create_boost_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "rust programming language",
                    "python programming language",
                    "rust web server",
                    "python web framework",
                    "javascript programming",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_boost_query_positive_only() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boost_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boosting(FtsQueryExpr::match_query("programming"));
        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_boost_query_with_negative() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boost_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boosting_with_negative(
            FtsQueryExpr::match_query("programming"),
            FtsQueryExpr::match_query("python"),
            0.5,
        );
        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 3);

        let doc0 = entries.iter().find(|e| e.row_position == 0).unwrap();
        let doc1 = entries.iter().find(|e| e.row_position == 1).unwrap();
        let doc4 = entries.iter().find(|e| e.row_position == 4).unwrap();
        assert!(doc1.score < doc0.score);
        assert!(doc1.score < doc4.score);
    }

    #[test]
    fn test_boost_query_negative_boost_factor() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boost_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query_no_demote = FtsQueryExpr::boosting_with_negative(
            FtsQueryExpr::match_query("programming"),
            FtsQueryExpr::match_query("python"),
            1.0,
        );
        let query_half_demote = FtsQueryExpr::boosting_with_negative(
            FtsQueryExpr::match_query("programming"),
            FtsQueryExpr::match_query("python"),
            0.5,
        );
        let query_zero_demote = FtsQueryExpr::boosting_with_negative(
            FtsQueryExpr::match_query("programming"),
            FtsQueryExpr::match_query("python"),
            0.0,
        );

        let r_no = index.search_query(&query_no_demote);
        let r_half = index.search_query(&query_half_demote);
        let r_zero = index.search_query(&query_zero_demote);

        let s_no = r_no.iter().find(|e| e.row_position == 1).unwrap().score;
        let s_half = r_half.iter().find(|e| e.row_position == 1).unwrap().score;
        let s_zero = r_zero.iter().find(|e| e.row_position == 1).unwrap().score;

        assert!((s_half - s_no * 0.5).abs() < 0.001);
        assert!(s_zero.abs() < 0.001);
    }

    #[test]
    fn test_boost_query_no_negative_match() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_boost_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boosting_with_negative(
            FtsQueryExpr::match_query("rust"),
            FtsQueryExpr::match_query("python"),
            0.1,
        );
        let entries = index.search_query(&query);
        assert_eq!(entries.len(), 2);

        let baseline = index.search_query(&FtsQueryExpr::match_query("rust"));
        for entry in &entries {
            let b = baseline
                .iter()
                .find(|e| e.row_position == entry.row_position)
                .unwrap();
            assert!((entry.score - b.score).abs() < 0.001);
        }
    }

    #[test]
    fn test_search_options_default() {
        let options = SearchOptions::default();
        assert_eq!(options.wand_factor, 1.0);
        assert!(options.limit.is_none());
    }

    #[test]
    fn test_search_options_builder() {
        let options = SearchOptions::new().with_wand_factor(0.5).with_limit(10);
        assert_eq!(options.wand_factor, 0.5);
        assert_eq!(options.limit, Some(10));
    }

    #[test]
    fn test_search_options_wand_factor_clamped() {
        let options = SearchOptions::new().with_wand_factor(2.0);
        assert_eq!(options.wand_factor, 1.0);
        let options = SearchOptions::new().with_wand_factor(-0.5);
        assert_eq!(options.wand_factor, 0.0);
    }

    fn create_wand_test_batch(schema: &ArrowSchema) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "alpha alpha alpha beta",
                    "alpha beta gamma",
                    "beta gamma delta",
                    "alpha alpha",
                    "alpha",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_search_with_options_full_recall() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("alpha");
        let results = index.search_with_options(&query, SearchOptions::default());
        assert_eq!(results.len(), 4);
        for i in 1..results.len() {
            assert!(results[i - 1].score >= results[i].score);
        }
    }

    #[test]
    fn test_search_with_options_with_limit() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("alpha");
        let options = SearchOptions::new().with_limit(2);
        let results = index.search_with_options(&query, options);
        assert_eq!(results.len(), 2);

        let mut full = index.search_query(&query);
        full.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        assert_eq!(results[0].row_position, full[0].row_position);
        assert_eq!(results[1].row_position, full[1].row_position);
    }

    #[test]
    fn test_search_with_options_wand_factor_pruning() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("alpha");
        let mut full = index.search_query(&query);
        full.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

        let options = SearchOptions::new().with_wand_factor(0.5);
        let results = index.search_with_options(&query, options);

        if !results.is_empty() {
            let max_score = full[0].score;
            let threshold = max_score * 0.5;
            for result in &results {
                assert!(result.score >= threshold - 0.001);
            }
        }
    }

    #[test]
    fn test_search_with_options_wand_factor_with_limit() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("alpha");
        let options = SearchOptions::new().with_limit(2).with_wand_factor(0.5);
        let results = index.search_with_options(&query, options);
        assert!(results.len() <= 2);
        if results.len() > 1 {
            assert!(results[0].score >= results[1].score);
        }
    }

    #[test]
    fn test_search_with_options_zero_limit_with_wand_factor() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("alpha");
        let options = SearchOptions::new().with_limit(0).with_wand_factor(0.5);
        let results = index.search_with_options(&query, options);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_with_options_empty_results() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::match_query("nonexistent");
        let options = SearchOptions::new().with_limit(10).with_wand_factor(0.5);
        let results = index.search_with_options(&query, options);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_with_options_boolean_query() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = create_wand_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let query = FtsQueryExpr::boolean()
            .should(FtsQueryExpr::match_query("alpha"))
            .should(FtsQueryExpr::match_query("beta"))
            .build();

        let options = SearchOptions::new().with_limit(3);
        let results = index.search_with_options(&query, options);
        assert!(results.len() <= 3);
        for i in 1..results.len() {
            assert!(results[i - 1].score >= results[i].score);
        }
    }

    // ====== New tests for SWMR semantics, Utf8View, memory accounting ======

    #[test]
    fn test_utf8view_insert_and_search() {
        // Mirror of test_fts_index_insert_and_search but with a Utf8View column.
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8View, true),
        ]));
        let index = FtsMemIndex::new(1, "description".to_string());

        let view =
            arrow_array::StringViewArray::from(vec!["hello world", "goodbye world", "hello again"]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1, 2])), Arc::new(view)],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        assert_eq!(index.doc_count(), 3);
        assert_eq!(index.search("hello").len(), 2);
        assert_eq!(index.search("world").len(), 2);
        assert_eq!(index.search("goodbye").len(), 1);
    }

    #[test]
    fn test_memory_usage_grows_with_inserts() {
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());

        let empty = index.memory_usage();
        index.insert(&create_test_batch(&schema), 0).unwrap();
        let after_one = index.memory_usage();
        index
            .insert(&create_phrase_test_batch(&schema), 100)
            .unwrap();
        let after_two = index.memory_usage();

        assert!(after_one > empty, "memory should grow after first insert");
        assert!(
            after_two > after_one,
            "memory should grow after second insert"
        );
    }

    #[test]
    fn test_partial_doc_never_visible_phrase() {
        // A phrase query inside a single document must either match fully
        // (both phrase tokens present) or not match at all — readers must
        // never observe a half-inserted doc that contains only one of the
        // phrase tokens. Since `search_phrase` only returns rows where
        // every token's position constraint holds, any returned entry
        // implicitly proves both tokens were visible together.
        let schema = create_test_schema();
        let index = Arc::new(position_index(1, "description"));

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let idx = index.clone();
            let stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let entries = idx.search_phrase("hello world", 0);
                    for e in &entries {
                        // "hello world" appears as the doc at offset 0 of
                        // every inserted batch, whose row_offset is a
                        // multiple of 100 — so any matched row_position
                        // must be a multiple of 100 (the row_offset itself,
                        // not offset+1 or offset+2).
                        assert_eq!(
                            e.row_position % 100,
                            0,
                            "phrase 'hello world' should only match the row_offset row of each batch, but got row_position {}",
                            e.row_position
                        );
                        assert!(e.score.is_finite() && e.score >= 0.0);
                    }
                }
            }));
        }

        for i in 0..50 {
            let batch = create_test_batch(&schema);
            index.insert(&batch, (i * 100) as u64).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        stop.store(true, Ordering::Relaxed);

        for r in readers {
            r.join().unwrap();
        }
    }

    #[test]
    fn test_swmr_visibility_torture() {
        let schema = create_test_schema();
        let index = Arc::new(FtsMemIndex::new(1, "description".to_string()));
        let stop = Arc::new(AtomicBool::new(false));

        // 8 reader threads issuing match queries.
        let mut readers = Vec::new();
        for _ in 0..8 {
            let idx = index.clone();
            let stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let r = idx.search("hello");
                    // BM25 scores must be finite and non-negative.
                    for e in &r {
                        assert!(e.score.is_finite());
                        assert!(e.score >= 0.0);
                    }
                }
            }));
        }

        // One writer thread.
        let writer = {
            let idx = index.clone();
            std::thread::spawn(move || {
                for i in 0..200 {
                    let batch = create_test_batch(&schema);
                    idx.insert(&batch, (i * 100) as u64).unwrap();
                }
            })
        };

        writer.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }

        // After all inserts, doc_count must be 200 batches × 3 rows.
        assert_eq!(index.doc_count(), 600);
    }

    #[test]
    fn test_to_index_builder_smoke() {
        // Ensure flush works on a minimal input.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string());
        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let builder = index.to_index_builder(42, 3).unwrap();
        // The builder can be consumed by callers; we just check it built.
        assert!(builder.id() > 0 || builder.id() == 42);
    }

    #[test]
    fn test_to_index_builder_supports_block_size_256() {
        let schema = create_test_schema();
        let params = InvertedIndexParams::default().block_size(256).unwrap();
        let index = FtsMemIndex::try_with_params(1, "description".to_string(), params).unwrap();
        let batch = create_test_batch(&schema);
        index.insert(&batch, 0).unwrap();

        let builder = index.to_index_builder(42, 3).unwrap();
        assert_eq!(builder.id(), 42);
    }

    #[test]
    fn test_unsupported_column_type_errors() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Int32, true),
        ]));
        let index = FtsMemIndex::new(1, "description".to_string());

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let err = index.insert(&batch, 0).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("must resolve to Utf8, LargeUtf8, Utf8View, or JSON")
                && message.contains("got Int32"),
            "{err}"
        );
    }

    // ===== Partition-structured redesign =====

    /// Sorted row positions of a result set.
    fn rows(mut entries: Vec<FtsEntry>) -> Vec<u64> {
        entries.sort_by_key(|e| e.row_position);
        entries.into_iter().map(|e| e.row_position).collect()
    }

    /// An index whose 4 docs (row positions 100..104) have all been frozen
    /// into a single partition (`freeze_threshold_rows = 1`).
    fn build_test_partition_index() -> FtsMemIndex {
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(1);
        let batch = RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "apple banana",
                    "apple cherry",
                    "banana",
                    "apple apple date",
                ])),
            ],
        )
        .unwrap();
        index.insert(&batch, 100).unwrap();
        index
    }

    #[test]
    fn test_partition_build_and_search() {
        let index = build_test_partition_index();
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 1, "the batch should have frozen");
        assert_eq!(st.tail.visible_count(), 0, "the tail should be empty");

        let p = &st.partitions[0];
        assert_eq!(p.doc_count(), 4);
        assert_eq!(p.total_tokens(), 8); // 2 + 2 + 1 + 3
        // Term dictionary stores the *tokenized* form, so probe via the
        // configured tokenizer rather than the raw word.
        let tok = |w: &str| index.tokenize_for_search(w).pop().unwrap();
        assert_eq!(p.token_df(&tok("apple")), 3);
        assert_eq!(p.token_df(&tok("banana")), 2);
        assert_eq!(p.token_df("definitely_missing"), 0);

        // Searching the index must return the partition's row positions.
        assert_eq!(rows(index.search("apple")), vec![100, 101, 103]);
        assert_eq!(rows(index.search("banana")), vec![100, 102]);
        assert_eq!(rows(index.search("date")), vec![103]);
        assert!(index.search("definitely_missing").is_empty());
    }

    #[test]
    fn test_partition_and_query_short_circuits_missing_term() {
        let index = build_test_partition_index();
        let st = index.state.load_full();
        let p = &st.partitions[0];
        let tail_snap = st.tail.snapshot();
        let apple = index.tokenize_for_search("apple").pop().unwrap();
        // "apple" is present -> an OR search over it matches.
        let or_scorer = build_scorer(&st, &tail_snap, std::slice::from_ref(&apple), true);
        let or_hits = p.search_match(std::slice::from_ref(&apple), Operator::Or, &or_scorer);
        assert_eq!(or_hits.len(), 3);
        // Adding an absent term to an AND query short-circuits to nothing.
        let and_tokens = vec![apple, "definitely_missing".to_string()];
        let and_scorer = build_scorer(&st, &tail_snap, &and_tokens, true);
        let and_hits = p.search_match(&and_tokens, Operator::And, &and_scorer);
        assert!(and_hits.is_empty());
    }

    #[test]
    fn test_freeze_produces_multiple_partitions() {
        // freeze_threshold_rows = 3 with 3-row batches => one partition per
        // batch insert.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(3);
        for i in 0..5 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 5, "every batch should have frozen");
        assert_eq!(st.tail.visible_count(), 0);
        assert_eq!(index.doc_count(), 15);

        // "hello" appears in docs 0 and 2 of every batch; "world" in 0 and 1.
        assert_eq!(
            rows(index.search("hello")),
            vec![0, 2, 100, 102, 200, 202, 300, 302, 400, 402]
        );
        assert_eq!(
            rows(index.search("world")),
            vec![0, 1, 100, 101, 200, 201, 300, 301, 400, 401]
        );
        assert!(index.search("definitely_missing").is_empty());
        // Every score must be finite and positive.
        for e in index.search("world") {
            assert!(e.score.is_finite() && e.score > 0.0);
        }
    }

    #[test]
    fn test_search_spans_partitions_and_tail() {
        // freeze_threshold_rows = 5, 3-row batches: batch 1 -> tail(3);
        // batch 2 -> tail(6) freezes -> partition(6); batch 3 -> tail(3).
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(5);
        index.insert(&create_test_batch(&schema), 0).unwrap();
        index.insert(&create_test_batch(&schema), 100).unwrap();
        index.insert(&create_test_batch(&schema), 200).unwrap();
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 1, "the first two batches froze");
        assert_eq!(st.tail.visible_count(), 1, "the third batch is in the tail");
        assert_eq!(index.doc_count(), 9);

        // Results must merge the partition (rows 0..200) and the tail (200..).
        assert_eq!(rows(index.search("hello")), vec![0, 2, 100, 102, 200, 202]);
        assert_eq!(rows(index.search("goodbye")), vec![1, 101, 201]);
    }

    #[test]
    fn test_flush_merges_partitions_and_tail() {
        // A frozen partition plus a live tail must all reach the builder.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(5);
        index.insert(&create_test_batch(&schema), 0).unwrap();
        index.insert(&create_test_batch(&schema), 100).unwrap();
        index.insert(&create_test_batch(&schema), 200).unwrap();
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 1);
        assert_eq!(st.tail.visible_count(), 1);

        let builder = index.to_index_builder(7, 300).unwrap();
        assert_eq!(builder.id(), 7);
        // A non-empty builder proves the partition and the tail both reached
        // the flush; end-to-end flush correctness is covered by the MemTable
        // flush integration tests.
        assert!(
            !builder.is_empty(),
            "9 docs across partition + tail must flush"
        );
    }

    #[test]
    fn test_phrase_across_partitions() {
        let schema = create_test_schema();
        let index = position_index(1, "description").with_freeze_threshold_rows(3);
        // Each batch has "hello world" at its row_offset row.
        for i in 0..4 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        let st = index.state.load_full();
        assert_eq!(st.partitions.len(), 4);
        // The phrase must be found in every partition.
        assert_eq!(
            rows(index.search_phrase("hello world", 0)),
            vec![0, 100, 200, 300]
        );
        // A phrase not present anywhere returns nothing.
        assert!(index.search_phrase("world hello", 0).is_empty());
    }

    #[test]
    fn test_merge_caps_partition_count() {
        // freeze_threshold_rows = 1 freezes every 3-row batch; past
        // MAX_PARTITIONS (32) the partitions are merged into one.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(1);
        for i in 0..40 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        let st = index.state.load_full();
        assert!(
            st.partitions.len() <= FtsMemIndex::MAX_PARTITIONS,
            "partition count {} should be capped",
            st.partitions.len()
        );
        assert_eq!(index.doc_count(), 120);
        // Merge must preserve every posting: "hello" hits 2 docs per batch.
        assert_eq!(index.search("hello").len(), 80);
        for e in index.search("hello") {
            assert!(e.score.is_finite() && e.score > 0.0);
        }
    }

    #[test]
    fn test_include_tail_option_and_flush() {
        let schema = create_test_schema();
        // threshold 5 with 3-row batches: batch@0 stays in tail; batch@100
        // pushes the tail to 6 and freezes both; batch@200 stays in the tail.
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(5);
        index.insert(&create_test_batch(&schema), 0).unwrap();
        index.insert(&create_test_batch(&schema), 100).unwrap();
        index.insert(&create_test_batch(&schema), 200).unwrap();
        let immutable = SearchOptions::new().with_include_tail(false);
        let match_hello = FtsQueryExpr::match_query("hello");

        // Read-your-writes (default) sees all three batches (2 "hello"/batch).
        assert_eq!(index.search("hello").len(), 6);
        // Immutable-only sees only the two frozen batches.
        assert_eq!(
            index
                .search_with_options(&match_hello, immutable.clone())
                .len(),
            4
        );
        // Flushing the tail makes it immutable; immutable-only now sees all.
        index.flush();
        assert_eq!(index.search_with_options(&match_hello, immutable).len(), 6);
        assert_eq!(index.search("hello").len(), 6);
    }

    #[test]
    fn test_block_impacts_frontier_is_tight_and_sound() {
        // The Pareto (freq, dl) frontier must dominate every input pair (sound
        // upper bound) and contain no dominated pair (minimal/tight).
        let dominates = |a: (u32, u32), b: (u32, u32)| a.0 >= b.0 && a.1 <= b.1;
        let cases: Vec<Vec<(u32, u32)>> = vec![
            vec![(3, 10), (1, 5), (2, 8), (5, 20), (1, 4)],
            vec![(1, 100); 5], // all identical => single frontier point
            vec![(9, 2)],      // single doc
            vec![(1, 9), (2, 8), (3, 7), (4, 6), (5, 5)], // fully Pareto-optimal
        ];
        for input in cases {
            let mut pairs = input.clone();
            let frontier = block_impacts(&mut pairs);
            assert!(!frontier.is_empty());
            // Soundness: every input pair is dominated by some frontier pair.
            for &p in &input {
                let dl = (p.0, p.1.max(1));
                assert!(
                    frontier.iter().any(|&f| dominates(f, dl)),
                    "{p:?} not dominated by frontier {frontier:?}"
                );
            }
            // Tightness: no frontier pair dominates another.
            for (i, &a) in frontier.iter().enumerate() {
                for (j, &b) in frontier.iter().enumerate() {
                    assert!(
                        i == j || !dominates(a, b),
                        "frontier not minimal: {frontier:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_multi_term_wand_and_tail_skip_match_exhaustive() {
        // Multi-term OR across frozen partitions + a non-empty tail. The
        // block-max-pruned WAND (limited) + tail-skip must return the same
        // top-k scores as the exhaustive (unlimited) scan.
        let schema = create_test_schema();
        // threshold 5 with 3-row batches leaves the last batch in the tail.
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(5);
        for i in 0..5 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        let query = FtsQueryExpr::match_query("hello world");
        let mut exhaustive = index.search_query(&query);
        exhaustive.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        for k in [1usize, 3, 10] {
            // Tail-skip is only an optimization: pruning the tail (default) and
            // forcing a full tail scan must both equal the exhaustive top-k.
            for tail_skip in [true, false] {
                let limited = index.search_with_options(
                    &query,
                    SearchOptions::new().with_limit(k).with_tail_skip(tail_skip),
                );
                let mut got: Vec<f32> = limited.iter().map(|e| e.score).collect();
                got.sort_by(|a, b| b.partial_cmp(a).unwrap());
                let want: Vec<f32> = exhaustive.iter().take(k).map(|e| e.score).collect();
                assert_eq!(got.len(), want.len(), "k={k} tail_skip={tail_skip}");
                for (g, w) in got.iter().zip(&want) {
                    assert!(
                        (g - w).abs() < 1e-4,
                        "k={k} tail_skip={tail_skip}: {g} vs {w}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_multi_term_wand_prune_keeps_multi_match_docs() {
        // Regression: the block-max prune must bound a pivot doc by *every* lane
        // positioned at it (the same set the scorer sums), not just lanes up to
        // the pivot index. A doc matching several query terms is high-scoring
        // but has contributing lanes that can sort *after* the pivot; bounding
        // only `lanes[..=pivot]` under-counts its score and unsoundly skips it,
        // dropping true top-k results (silent recall loss on OR queries).
        //
        // Needs a corpus rich in multi-term-match docs across frozen partitions
        // (so block-max applies) with a warm threshold — a small fixed corpus
        // never exercises the faulty branch.
        let schema = create_test_schema();
        // A 12-word vocab with Zipfian inclusion + skewed per-term frequencies:
        // query terms have very different df/idf and per-doc tf, so a contributing
        // lane's `ub` can be small enough to sort it *after* the pivot. Generated
        // from a deterministic LCG so the case is reproducible without `rand`.
        let vocab: Vec<String> = (0..12).map(|i| format!("w{i}")).collect();
        let make_batch = |start: i32, count: i32| -> RecordBatch {
            let ids: Vec<i32> = (start..start + count).collect();
            let texts: Vec<String> = (start..start + count)
                .map(|i| {
                    let mut rng = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
                    let mut next = || {
                        rng = rng
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        (rng >> 33) as u32
                    };
                    let mut words: Vec<&str> = Vec::new();
                    for (j, w) in vocab.iter().enumerate() {
                        // Rarer words (higher j) included less often; when present,
                        // a skewed term frequency (larger for low j).
                        if next() % (j as u32 + 2) == 0 {
                            let reps = 1 + (next() % (8u32.saturating_sub(j as u32 / 2).max(1)));
                            for _ in 0..reps {
                                words.push(w);
                            }
                        }
                    }
                    if words.is_empty() {
                        words.push(&vocab[0]);
                    }
                    words.join(" ")
                })
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(texts)),
                ],
            )
            .unwrap()
        };

        // 40 freezes of 100 docs (threshold 250) => multiple multi-block
        // partitions (so a block's max_freq differs from the partition max,
        // making the block bound straddle the threshold) plus a live tail.
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(250);
        for b in 0..40 {
            index
                .insert(&make_batch(b * 100, 100), (b * 100) as u64)
                .unwrap();
        }
        assert!(index.state.load_full().partitions.len() >= 2);

        // Sweep every common+common and common+rare pair/triple so the lanes have
        // a wide spread of upper bounds — the condition that exposes the prune.
        let mut queries: Vec<String> = Vec::new();
        for a in 0..6 {
            for b in (a + 1)..8 {
                queries.push(format!("w{a} w{b}"));
                for c in (b + 1)..10 {
                    queries.push(format!("w{a} w{b} w{c}"));
                }
            }
        }
        for query_text in &queries {
            let query = FtsQueryExpr::match_query(query_text.as_str());
            let mut exhaustive = index.search_query(&query);
            exhaustive.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
            for k in [1usize, 5, 10, 25] {
                let limited = index.search_with_options(&query, SearchOptions::new().with_limit(k));
                let mut got: Vec<f32> = limited.iter().map(|e| e.score).collect();
                got.sort_by(|a, b| b.partial_cmp(a).unwrap());
                let want: Vec<f32> = exhaustive.iter().take(k).map(|e| e.score).collect();
                assert_eq!(got.len(), want.len(), "q={query_text:?} k={k}");
                for (g, w) in got.iter().zip(&want) {
                    assert!(
                        (g - w).abs() < 1e-4,
                        "q={query_text:?} k={k}: WAND returned {g}, exhaustive top-k has {w} \
                         (block-max prune dropped a higher-scoring multi-term-match doc)"
                    );
                }
            }
        }
    }

    #[test]
    fn test_background_tiered_merge_reduces_partition_count() {
        // 16 single-batch freezes (freeze_threshold_rows = 1) make 16 tiny
        // same-tier partitions. The tiered merge (MERGE_FACTOR = 8) runs in the
        // background and the writer installs it on a later freeze. Total inserts
        // stay below MAX_PARTITIONS (32), so any reduction is from the tier
        // merge — not the synchronous safety-net collapse.
        let schema = create_test_schema();
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(1);
        for i in 0..16 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut total = 16u64;
        while index.state.load().partitions.len() >= 16
            && total < 30
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
            index
                .insert(&create_test_batch(&schema), total * 100)
                .unwrap();
            total += 1;
        }
        let parts = index.state.load().partitions.len();
        assert!(
            parts < 16,
            "background tier merge should reduce partitions below 16, got {parts}"
        );
        // Merge is doc-preserving: counts are exact regardless of timing.
        assert_eq!(index.doc_count(), total as usize * 3);
        assert_eq!(index.search("hello").len(), total as usize * 2);
        for e in index.search("hello") {
            assert!(e.score.is_finite() && e.score > 0.0);
        }
    }

    #[test]
    fn test_freeze_during_concurrent_search() {
        // Searches running concurrently with freezes must never observe a
        // doc twice or miss one: every result row is valid and scored.
        let schema = create_test_schema();
        let index =
            Arc::new(FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(6));
        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let idx = index.clone();
            let stop = stop.clone();
            readers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for e in idx.search("world") {
                        // "world" only ever appears in docs 0 and 1 of a
                        // batch, whose row_offset is a multiple of 100.
                        assert!(e.row_position % 100 < 2);
                        assert!(e.score.is_finite() && e.score >= 0.0);
                    }
                }
            }));
        }
        for i in 0..60 {
            index
                .insert(&create_test_batch(&schema), (i * 100) as u64)
                .unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(index.doc_count(), 180);
        assert_eq!(index.search("world").len(), 120);
    }

    #[test]
    fn test_limited_search_returns_exact_top_k_across_partitions() {
        // Regression: a limited `search_with_options` over many partitions
        // must still return the *exact* global top-k. Per-partition WAND must
        // not prune by a limit it cannot bound (in-memory posting lists carry
        // no `max_score`), or valid top-k docs are silently dropped.
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(2);
        // 8 docs (4 batches of 2) -> 4 partitions. "lion" appears with tf
        // 1..=6, so the 6 matching docs have strictly distinct BM25 scores.
        let texts = [
            ["lion", "lion lion"],
            ["lion lion lion", "lion lion lion lion"],
            ["cat", "lion lion lion lion lion"],
            ["lion lion lion lion lion lion", "dog"],
        ];
        for (b, pair) in texts.iter().enumerate() {
            let batch = RecordBatch::try_new(
                create_test_schema(),
                vec![
                    Arc::new(Int32Array::from(vec![0, 1])),
                    Arc::new(StringArray::from(pair.to_vec())),
                ],
            )
            .unwrap();
            index.insert(&batch, (b * 100) as u64).unwrap();
        }
        assert_eq!(index.state.load_full().partitions.len(), 4);

        // Exact full ranking via the unbounded search path.
        let mut full = index.search("lion");
        full.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        assert_eq!(full.len(), 6);

        // The limited top-3 must equal the first 3 of the full ranking.
        let limited = index.search_with_options(
            &FtsQueryExpr::match_query("lion"),
            SearchOptions::new().with_limit(3),
        );
        assert_eq!(limited.len(), 3);
        let got: Vec<u64> = limited.iter().map(|e| e.row_position).collect();
        let expected: Vec<u64> = full.iter().take(3).map(|e| e.row_position).collect();
        assert_eq!(got, expected, "limited search must return the exact top-3");
    }

    #[test]
    fn test_wand_topk_matches_exact_full_scan() {
        // WAND must produce the exact same top-k as an unbounded scan, at a
        // scale where pruning genuinely engages. 40 docs across ~8 partitions;
        // "alpha" appears with tf 1..=40, giving 40 strictly distinct scores.
        let index = FtsMemIndex::new(1, "description".to_string()).with_freeze_threshold_rows(5);
        for i in 0..40u64 {
            let text = "alpha ".repeat((i + 1) as usize);
            let batch = RecordBatch::try_new(
                create_test_schema(),
                vec![
                    Arc::new(Int32Array::from(vec![0])),
                    Arc::new(StringArray::from(vec![text.as_str()])),
                ],
            )
            .unwrap();
            index.insert(&batch, i).unwrap();
        }
        assert!(index.state.load_full().partitions.len() >= 2);

        // Exact ranking from the unbounded path.
        let mut full = index.search("alpha");
        assert_eq!(full.len(), 40);
        full.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap()
                .then(a.row_position.cmp(&b.row_position))
        });

        // For several k, WAND-pruned top-k must equal the exact top-k.
        for k in [1usize, 3, 10, 25, 40, 50] {
            let limited = index.search_with_options(
                &FtsQueryExpr::match_query("alpha"),
                SearchOptions::new().with_limit(k),
            );
            let expect_len = k.min(40);
            assert_eq!(limited.len(), expect_len, "k={k}");
            let got: Vec<u64> = limited.iter().map(|e| e.row_position).collect();
            let expected: Vec<u64> = full
                .iter()
                .take(expect_len)
                .map(|e| e.row_position)
                .collect();
            assert_eq!(got, expected, "WAND top-{k} must equal the exact top-{k}");
        }
    }

    #[test]
    fn test_bitpack_read_at_random_access() {
        // Pack a stream, then random-access arbitrary [s, s+n) ranges.
        let vals: Vec<u32> = (0..200u32).map(|i| (i * 7 + 3) % 1000).collect();
        let width = bit_width(*vals.iter().max().unwrap());
        let mut buf = Vec::new();
        bitpack_put(&mut buf, &vals, width);
        for &(s, n) in &[(0usize, 5usize), (1, 1), (63, 10), (130, 70), (199, 1)] {
            let mut out = Vec::new();
            bitpack_read_at(&buf, 0, s, n, width, &mut out);
            assert_eq!(out, &vals[s..s + n], "s={s} n={n}");
        }
    }

    #[test]
    fn test_bitpack_roundtrip() {
        // Cover width 0 (all-zero), sub-byte, byte-crossing, and full widths.
        let cases: [(Vec<u32>, u8); 6] = [
            (vec![0u32; 5], 0),
            (vec![0, 1, 0, 1, 1, 0, 1], 1),
            (vec![3, 0, 7, 5, 2, 6, 1, 4], 3),
            (vec![200, 17, 255, 0, 130], 8),
            (vec![1000, 5, 65535, 42], 16),
            (vec![1, u32::MAX, 0, 123_456_789], 32),
        ];
        for (values, width) in &cases {
            let mut buf = Vec::new();
            bitpack_put(&mut buf, values, *width);
            assert_eq!(
                buf.len(),
                bitpack_len(values.len(), *width),
                "width={width}"
            );
            let mut out = Vec::new();
            bitpack_get(&buf, 0, values.len(), *width, &mut out);
            assert_eq!(&out, values, "width={width}");
        }
    }

    #[test]
    fn test_posting_cursor_codec_roundtrip() {
        // One term spanning 3 blocks (300 docs), varied freqs + positions.
        let mut docs = DocSet::default();
        let mut docs_for_term: Vec<(u32, u32, Vec<u32>)> = Vec::new();
        for d in 0..300u32 {
            docs.append(1000 + d as u64, (d % 7) + 1);
            let freq = (d % 5) + 1;
            let positions: Vec<u32> = (0..freq).map(|p| p * 3).collect();
            docs_for_term.push((d, freq, positions));
        }
        let part = build_partition(vec![(Arc::from("term"), docs_for_term.clone())], docs);

        // Full iteration must reproduce the input exactly.
        let mut c = PostingCursor::new(&part, 0);
        for (d, f, pos) in &docs_for_term {
            assert_eq!(c.doc(), Some(*d));
            assert_eq!(c.freq(), *f);
            assert_eq!(c.positions(), pos.as_slice());
            c.advance();
        }
        assert_eq!(c.doc(), None, "cursor exhausted after the last doc");

        // skip_to must land correctly across and within block boundaries.
        for &(target, expect) in &[
            (5u32, Some(5u32)), // within block 0
            (128, Some(128)),   // exact block-1 boundary
            (200, Some(200)),   // within block 1
            (299, Some(299)),   // last doc
            (300, None),        // past the end
        ] {
            let mut c = PostingCursor::new(&part, 0);
            c.skip_to(target);
            assert_eq!(c.doc(), expect, "skip_to({target})");
        }
        // Sequential skips on one cursor.
        let mut c = PostingCursor::new(&part, 0);
        c.skip_to(100);
        assert_eq!(c.doc(), Some(100));
        c.skip_to(260);
        assert_eq!(c.doc(), Some(260));
    }
}
