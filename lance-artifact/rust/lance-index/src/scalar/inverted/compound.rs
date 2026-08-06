// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

use futures::{StreamExt, TryStreamExt, stream};
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::{Error, Result};
use lance_select::RowAddrMask;
use lance_tokenizer::{SimpleTokenizer, TextAnalyzer};

use super::{
    InvertedIndex, build_global_bm25_scorer,
    document_tokenizer::{DocType, JsonTokenizer, LanceTokenizer},
    documents::{DocId, DocLengths, DocVisibility, PartitionDocuments, ResidentAddressProjection},
    index::{DocSet, InvertedPartition},
    query::{
        FtsQuery, FtsSearchParams, MatchQuery, Operator, PhraseQuery, Tokens, collect_query_tokens,
    },
    scorer::MemBM25Scorer,
    tokenizer::document_tokenizer::TextTokenizer,
    wand::{
        FLAT_SEARCH_PERCENT_THRESHOLD, LegacyWandDocuments, ModernWandDocuments, PostingIterator,
        WandCursor, WandDocuments,
    },
};
use crate::{metrics::MetricsCollector, prefilter::PreFilter};

const DEFAULT_BLOCK_SIZE: usize = 128;
const SCORE_FLOOR_RESOLUTION_BATCH_SIZE: usize = DEFAULT_BLOCK_SIZE;

/// One exact FTS result in a compound collector's candidate domain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ScoredRow<K = u64> {
    pub row_id: K,
    pub score: f32,
}

#[cfg(test)]
impl ScoredRow<u64> {
    pub(super) fn new(row_id: u64, score: f32) -> Result<Self> {
        if !score.is_finite() {
            return Err(Error::invalid_input(format!(
                "FTS score for row_id={row_id} must be finite, got {score}"
            )));
        }
        Ok(Self { row_id, score })
    }
}

/// Conservative score bounds for a document range.
///
/// The lower bound is needed by signed compositions such as [`BoostScorer`].
/// Arithmetic widens both sides by one representable `f32` so nested
/// operations cannot round an upper bound below an exact score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ScoreBounds {
    lower: f32,
    upper: f32,
}

impl ScoreBounds {
    const ZERO: Self = Self {
        lower: 0.0,
        upper: 0.0,
    };
    const UNBOUNDED: Self = Self {
        lower: f32::NEG_INFINITY,
        upper: f32::INFINITY,
    };

    #[cfg(test)]
    fn point(score: f32) -> Result<Self> {
        if !score.is_finite() {
            return Err(Error::invalid_input(format!(
                "FTS score bounds require a finite score, got {score}"
            )));
        }
        Ok(Self {
            lower: score,
            upper: score,
        })
    }

    fn scale_non_negative(self, factor: f32) -> Self {
        debug_assert!(factor.is_finite() && factor >= 0.0);
        if factor == 0.0 {
            return Self::ZERO;
        }
        if !self.lower.is_finite() || !self.upper.is_finite() {
            return Self::UNBOUNDED;
        }
        Self {
            lower: next_down(self.lower * factor),
            upper: next_up(self.upper * factor),
        }
    }

    fn include_zero(self) -> Self {
        Self {
            lower: self.lower.min(0.0),
            upper: self.upper.max(0.0),
        }
    }

    fn add(self, other: Self) -> Self {
        if !self.lower.is_finite()
            || !self.upper.is_finite()
            || !other.lower.is_finite()
            || !other.upper.is_finite()
        {
            return Self::UNBOUNDED;
        }
        Self {
            lower: next_down(self.lower + other.lower),
            upper: next_up(self.upper + other.upper),
        }
    }

    fn subtract_scaled(self, other: Self, factor: f32) -> Self {
        let penalty = other.scale_non_negative(factor);
        if !self.lower.is_finite()
            || !self.upper.is_finite()
            || !penalty.lower.is_finite()
            || !penalty.upper.is_finite()
        {
            return Self::UNBOUNDED;
        }
        Self {
            lower: next_down(self.lower - penalty.upper),
            upper: next_up(self.upper - penalty.lower),
        }
    }
}

fn next_up(value: f32) -> f32 {
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

fn next_down(value: f32) -> f32 {
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

fn checked_score(score: f32, context: &str) -> Result<f32> {
    if score.is_finite() {
        Ok(score)
    } else {
        Err(Error::invalid_input(format!(
            "{context} produced a non-finite FTS score: {score}"
        )))
    }
}

/// Internal document-at-a-time scorer protocol for compound FTS.
///
/// Implementations iterate matching partition-local document ids in ascending
/// order and expose the corresponding candidate key separately. A collector
/// may shallow-advance independently of the exact iterator, inspect a
/// conservative range bound, and monotonically raise the competitive score.
/// `matches` is the optional two-phase confirmation hook: cheap approximations
/// return a candidate from `next` / `advance` and defer expensive checks such
/// as phrase positions until confirmation.
pub(super) trait ComposableScorer: Send {
    fn doc(&self) -> Option<u64>;
    fn document_key(&self) -> Option<u64> {
        self.doc()
    }
    fn next(&mut self) -> Result<Option<u64>>;
    fn advance(&mut self, target: u64) -> Result<Option<u64>>;
    fn cost(&self) -> usize;
    fn score(&mut self) -> Result<f32>;
    fn advance_shallow(&mut self, target: u64) -> Result<u64>;
    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds>;
    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()>;

    fn matches(&mut self) -> Result<bool> {
        Ok(true)
    }

    fn match_cost(&self) -> Option<f32> {
        None
    }

    fn scores_non_negative(&self) -> bool {
        false
    }
}

type BoxScorer<'a> = Box<dyn ComposableScorer + 'a>;

#[derive(Debug, Clone)]
enum CompoundScorerPlan {
    Leaf {
        index: usize,
        boost: f32,
    },
    Boost {
        positive: Box<Self>,
        negative: Box<Self>,
        negative_boost: f32,
    },
    MultiMatch(Vec<Self>),
    Boolean {
        should: Vec<Self>,
        must: Vec<Self>,
        must_not: Vec<Self>,
    },
}

impl CompoundScorerPlan {
    fn from_query(query: &FtsQuery, num_leaves: &mut usize) -> Result<Self> {
        match query {
            FtsQuery::Match(query) => {
                let index = *num_leaves;
                *num_leaves += 1;
                Ok(Self::Leaf {
                    index,
                    boost: query.boost,
                })
            }
            FtsQuery::Phrase(_) => {
                let index = *num_leaves;
                *num_leaves += 1;
                Ok(Self::Leaf { index, boost: 1.0 })
            }
            FtsQuery::Boost(query) => Ok(Self::Boost {
                positive: Box::new(Self::from_query(&query.positive, num_leaves)?),
                negative: Box::new(Self::from_query(&query.negative, num_leaves)?),
                negative_boost: query.negative_boost,
            }),
            FtsQuery::MultiMatch(query) => Ok(Self::MultiMatch(
                query
                    .match_queries
                    .iter()
                    .map(|query| Self::from_query(&FtsQuery::Match(query.clone()), num_leaves))
                    .collect::<Result<Vec<_>>>()?,
            )),
            FtsQuery::Boolean(query) => Ok(Self::Boolean {
                should: query
                    .should
                    .iter()
                    .map(|query| Self::from_query(query, num_leaves))
                    .collect::<Result<Vec<_>>>()?,
                must: query
                    .must
                    .iter()
                    .map(|query| Self::from_query(query, num_leaves))
                    .collect::<Result<Vec<_>>>()?,
                must_not: query
                    .must_not
                    .iter()
                    .map(|query| Self::from_query(query, num_leaves))
                    .collect::<Result<Vec<_>>>()?,
            }),
        }
    }

    fn build<'a>(&self, leaves: &mut [Option<BoxScorer<'a>>]) -> Result<BoxScorer<'a>> {
        match self {
            Self::Leaf { index, boost } => {
                let leaf = leaves
                    .get_mut(*index)
                    .and_then(Option::take)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "compound FTS scorer references missing leaf index {index}"
                        ))
                    })?;
                Ok(Box::new(ScaleScorer::try_new(leaf, *boost)?))
            }
            Self::Boost {
                positive,
                negative,
                negative_boost,
            } => Ok(Box::new(BoostScorer::try_new(
                positive.build(leaves)?,
                negative.build(leaves)?,
                *negative_boost,
            )?)),
            Self::MultiMatch(children) => Ok(Box::new(DisjunctionScorer::try_new(
                children
                    .iter()
                    .map(|child| child.build(leaves))
                    .collect::<Result<Vec<_>>>()?,
                DisjunctionScore::Max,
            )?)),
            Self::Boolean {
                should,
                must,
                must_not,
            } => Ok(Box::new(BooleanScorer::try_new(
                should
                    .iter()
                    .map(|child| child.build(leaves))
                    .collect::<Result<Vec<_>>>()?,
                must.iter()
                    .map(|child| child.build(leaves))
                    .collect::<Result<Vec<_>>>()?,
                must_not
                    .iter()
                    .map(|child| child.build(leaves))
                    .collect::<Result<Vec<_>>>()?,
            )?)),
        }
    }
}

impl<D: WandDocuments + Sync> ComposableScorer for WandCursor<'_, D> {
    fn doc(&self) -> Option<u64> {
        self.doc()
    }

    fn document_key(&self) -> Option<u64> {
        self.document_key()
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.next()
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        self.advance(target)
    }

    fn cost(&self) -> usize {
        self.cost()
    }

    fn score(&mut self) -> Result<f32> {
        self.current_score()
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        self.advance_shallow(target)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        Ok(ScoreBounds {
            lower: 0.0,
            upper: self.score_upper_bound(up_to)?,
        })
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        self.set_min_competitive_score(min_score)
    }

    fn matches(&mut self) -> Result<bool> {
        self.matches()
    }

    fn match_cost(&self) -> Option<f32> {
        self.match_cost()
    }

    fn scores_non_negative(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg(test)]
struct ShallowRange {
    target: u64,
    up_to: u64,
    start: usize,
    end: usize,
}

/// Exact in-memory scorer used to unit-test compound nodes and the collector.
#[cfg(test)]
struct MaterializedScorer {
    rows: Vec<ScoredRow>,
    block_size: usize,
    index: Option<usize>,
    shallow: Option<ShallowRange>,
    min_competitive_score: f32,
    scores_non_negative: bool,
}

#[cfg(test)]
impl MaterializedScorer {
    fn try_new(mut rows: Vec<ScoredRow>) -> Result<Self> {
        rows.sort_unstable_by_key(|row| row.row_id);
        for pair in rows.windows(2) {
            if pair[0].row_id == pair[1].row_id {
                return Err(Error::internal(format!(
                    "FTS leaf scorer produced duplicate row_id={}",
                    pair[0].row_id
                )));
            }
        }
        let scores_non_negative = rows.iter().all(|row| row.score >= 0.0);
        Ok(Self {
            rows,
            block_size: DEFAULT_BLOCK_SIZE,
            index: None,
            shallow: None,
            min_competitive_score: f32::NEG_INFINITY,
            scores_non_negative,
        })
    }

    #[cfg(test)]
    fn with_block_size(mut self, block_size: usize) -> Self {
        assert!(block_size > 0);
        self.block_size = block_size;
        self
    }

    fn block_bounds(&self, start: usize, end: usize) -> Result<ScoreBounds> {
        let Some(first) = self.rows.get(start) else {
            return Ok(ScoreBounds::ZERO);
        };
        let mut bounds = ScoreBounds::point(first.score)?;
        for row in &self.rows[start + 1..end] {
            bounds.lower = bounds.lower.min(row.score);
            bounds.upper = bounds.upper.max(row.score);
        }
        Ok(bounds)
    }

    fn position_at(&mut self, mut index: usize) -> Result<Option<u64>> {
        while index < self.rows.len() {
            let block_start = (index / self.block_size) * self.block_size;
            let block_end = (block_start + self.block_size).min(self.rows.len());
            if self.block_bounds(block_start, block_end)?.upper < self.min_competitive_score {
                index = block_end;
                continue;
            }
            self.index = Some(index);
            self.shallow = None;
            return Ok(Some(self.rows[index].row_id));
        }
        self.index = None;
        self.shallow = None;
        Ok(None)
    }
}

#[cfg(test)]
impl ComposableScorer for MaterializedScorer {
    fn doc(&self) -> Option<u64> {
        self.index.map(|index| self.rows[index].row_id)
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.position_at(self.index.map_or(0, |index| index + 1))
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if self.doc().is_some_and(|doc| doc >= target) {
            return Ok(self.doc());
        }
        let start = self.index.map_or(0, |index| index + 1);
        let offset = self.rows[start..].partition_point(|row| row.row_id < target);
        self.position_at(start + offset)
    }

    fn cost(&self) -> usize {
        self.rows.len()
    }

    fn score(&mut self) -> Result<f32> {
        self.index
            .map(|index| self.rows[index].score)
            .ok_or_else(|| Error::internal("FTS scorer is not positioned on a document"))
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let start = self.rows.partition_point(|row| row.row_id < target);
        if start == self.rows.len() {
            self.shallow = Some(ShallowRange {
                target,
                up_to: u64::MAX,
                start,
                end: start,
            });
            return Ok(u64::MAX);
        }
        let block_start = (start / self.block_size) * self.block_size;
        let end = (block_start + self.block_size).min(self.rows.len());
        let up_to = self
            .rows
            .get(end)
            .map(|next| next.row_id.saturating_sub(1))
            .unwrap_or(u64::MAX);
        self.shallow = Some(ShallowRange {
            target,
            up_to,
            start,
            end,
        });
        Ok(up_to)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        let shallow = self.shallow.ok_or_else(|| {
            Error::internal("score_bounds requires advance_shallow on the FTS scorer")
        })?;
        if up_to < shallow.target || up_to > shallow.up_to {
            return Err(Error::internal(format!(
                "FTS score bound up_to={up_to} is outside shallow range [{}, {}]",
                shallow.target, shallow.up_to
            )));
        }
        let end = shallow.start
            + self.rows[shallow.start..shallow.end].partition_point(|row| row.row_id <= up_to);
        self.block_bounds(shallow.start, end)
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        if min_score.is_nan() {
            return Err(Error::invalid_input(
                "minimum competitive FTS score cannot be NaN",
            ));
        }
        if min_score > self.min_competitive_score {
            self.min_competitive_score = min_score;
        }
        Ok(())
    }

    fn scores_non_negative(&self) -> bool {
        self.scores_non_negative
    }
}

/// Monotonic score-only floor shared by partition-local top-k collectors.
///
/// Equal-score candidates are never pruned because final ordering also uses
/// row id. The score-only floor is therefore a safe lower bound even when
/// partitions encounter ties in different orders.
#[derive(Debug)]
pub(super) struct CompetitiveScore {
    bits: AtomicU32,
}

impl Default for CompetitiveScore {
    fn default() -> Self {
        Self {
            bits: AtomicU32::new(f32::NEG_INFINITY.to_bits()),
        }
    }
}

impl CompetitiveScore {
    fn get(&self) -> f32 {
        f32::from_bits(self.bits.load(AtomicOrdering::Relaxed))
    }

    fn raise(&self, score: f32) {
        debug_assert!(!score.is_nan());
        let mut current = self.bits.load(AtomicOrdering::Relaxed);
        while score > f32::from_bits(current) {
            match self.bits.compare_exchange_weak(
                current,
                score.to_bits(),
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HeapRow<K>(ScoredRow<K>);

impl<K: PartialEq> PartialEq for HeapRow<K> {
    fn eq(&self, other: &Self) -> bool {
        self.0.row_id == other.0.row_id && self.0.score.to_bits() == other.0.score.to_bits()
    }
}

impl<K: Eq> Eq for HeapRow<K> {}

impl<K: Ord> PartialOrd for HeapRow<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for HeapRow<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        // The worst result is the heap maximum: lower score, then higher row id.
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then_with(|| self.0.row_id.cmp(&other.0.row_id))
    }
}

fn compare_scored_rows<K: Ord>(left: &ScoredRow<K>, right: &ScoredRow<K>) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.row_id.cmp(&right.row_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CollectionStatus {
    Complete,
    ScoreFloorOverflow,
}

#[derive(Debug, Clone, Copy)]
enum TieHandling {
    ResolveByKey,
    RetainScoreFloor { max_buffered: usize },
}

/// The sole owner of top-k state for a compound scorer tree.
///
/// Resolved document keys use the normal `(score DESC, row_id ASC)` ordering
/// and keep exactly `limit` rows. An unresolved partition temporarily retains
/// its kth-score floor, but stops once the bounded resolution buffer fills.
pub(super) struct TopKCollector<K = u64> {
    limit: usize,
    heap: BinaryHeap<HeapRow<K>>,
    competitive_score: Arc<CompetitiveScore>,
    tie_handling: TieHandling,
    peak_buffered: usize,
}

impl<K: Copy + Ord> TopKCollector<K> {
    pub(super) fn new(limit: usize) -> Self {
        Self::with_competitive_score(limit, Arc::new(CompetitiveScore::default()))
    }

    pub(super) fn with_competitive_score(
        limit: usize,
        competitive_score: Arc<CompetitiveScore>,
    ) -> Self {
        Self::with_tie_handling(limit, competitive_score, TieHandling::ResolveByKey)
    }

    fn retaining_score_floor(
        limit: usize,
        competitive_score: Arc<CompetitiveScore>,
        max_buffered: usize,
    ) -> Self {
        debug_assert!(max_buffered >= limit);
        Self::with_tie_handling(
            limit,
            competitive_score,
            TieHandling::RetainScoreFloor { max_buffered },
        )
    }

    fn with_tie_handling(
        limit: usize,
        competitive_score: Arc<CompetitiveScore>,
        tie_handling: TieHandling,
    ) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit.min(DEFAULT_BLOCK_SIZE)),
            competitive_score,
            tie_handling,
            peak_buffered: 0,
        }
    }

    fn insert(&mut self, row: ScoredRow<K>) -> CollectionStatus {
        if self.limit == 0 {
            return CollectionStatus::Complete;
        }
        if self.heap.len() < self.limit {
            self.heap.push(HeapRow(row));
        } else {
            let worst = self.heap.peek().expect("a full top-k heap is non-empty").0;
            match row.score.total_cmp(&worst.score) {
                Ordering::Less => {}
                Ordering::Equal => match self.tie_handling {
                    TieHandling::ResolveByKey => {
                        if row.row_id < worst.row_id {
                            self.heap.pop();
                            self.heap.push(HeapRow(row));
                        }
                    }
                    TieHandling::RetainScoreFloor { max_buffered } => {
                        if self.heap.len() >= max_buffered {
                            self.raise_competitive_score();
                            return CollectionStatus::ScoreFloorOverflow;
                        }
                        self.heap.push(HeapRow(row));
                    }
                },
                Ordering::Greater => {
                    match self.tie_handling {
                        TieHandling::ResolveByKey => {
                            self.heap.pop();
                            self.heap.push(HeapRow(row));
                        }
                        TieHandling::RetainScoreFloor { max_buffered } => {
                            self.heap.push(HeapRow(row));
                            self.prune_obsolete_score_floors();
                            if self.heap.len() > max_buffered {
                                // This collector is discarded and the partition is
                                // retried with resolved keys. Keep its observable
                                // working set within the advertised bound meanwhile.
                                self.heap.pop();
                                self.raise_competitive_score();
                                return CollectionStatus::ScoreFloorOverflow;
                            }
                        }
                    }
                }
            }
        }
        self.peak_buffered = self.peak_buffered.max(self.heap.len());
        self.raise_competitive_score();
        CollectionStatus::Complete
    }

    fn raise_competitive_score(&self) {
        if self.heap.len() >= self.limit
            && let Some(worst) = self.heap.peek()
        {
            self.competitive_score.raise(worst.0.score);
        }
    }

    fn prune_obsolete_score_floors(&mut self) {
        while self.heap.len() > self.limit {
            let floor = self
                .heap
                .peek()
                .expect("a non-empty top-k heap has a score floor")
                .0
                .score;
            let floor_count = self
                .heap
                .iter()
                .filter(|row| row.0.score.total_cmp(&floor) == Ordering::Equal)
                .count();
            if self.heap.len() - floor_count < self.limit {
                break;
            }
            self.heap
                .retain(|row| row.0.score.total_cmp(&floor) != Ordering::Equal);
        }
    }

    pub(super) fn collect_mapped(
        &mut self,
        scorer: &mut dyn ComposableScorer,
        mut map_document: impl FnMut(u64) -> Result<K>,
    ) -> Result<CollectionStatus> {
        if self.limit == 0 {
            return Ok(CollectionStatus::Complete);
        }
        let capacity_limit = match self.tie_handling {
            TieHandling::ResolveByKey => self.limit,
            TieHandling::RetainScoreFloor { max_buffered } => max_buffered,
        };
        let expected = scorer.cost().min(capacity_limit);
        self.heap
            .reserve(expected.saturating_sub(self.heap.capacity()));

        scorer.set_min_competitive_score(self.competitive_score.get())?;
        let mut doc = scorer.next()?;
        while let Some(doc_id) = doc {
            let min_score = self.competitive_score.get();
            scorer.set_min_competitive_score(min_score)?;
            let up_to = scorer.advance_shallow(doc_id)?;
            let bounds = scorer.score_bounds(up_to)?;
            if bounds.upper < min_score {
                doc = if up_to == u64::MAX {
                    None
                } else {
                    scorer.advance(up_to + 1)?
                };
                continue;
            }

            if let Some(match_cost) = scorer.match_cost()
                && (!match_cost.is_finite() || match_cost < 0.0)
            {
                return Err(Error::internal(format!(
                    "FTS scorer reported invalid two-phase match cost: {match_cost}"
                )));
            }
            if scorer.matches()? {
                let score = checked_score(scorer.score()?, "compound scorer")?;
                // A shared partition floor is already known to be globally
                // competitive. Scores strictly below it cannot enter final top-k.
                if score >= self.competitive_score.get() {
                    let document_key = scorer.document_key().ok_or_else(|| {
                        Error::internal(
                            "compound FTS scorer did not expose its current document key",
                        )
                    })?;
                    let status = self.insert(ScoredRow {
                        row_id: map_document(document_key)?,
                        score,
                    });
                    if status == CollectionStatus::ScoreFloorOverflow {
                        return Ok(status);
                    }
                }
            }
            doc = scorer.next()?;
        }

        Ok(CollectionStatus::Complete)
    }

    fn into_candidates(self) -> Vec<ScoredRow<K>> {
        let mut rows = self.heap.into_iter().map(|row| row.0).collect::<Vec<_>>();
        rows.sort_unstable_by(compare_scored_rows);
        rows
    }

    fn into_rows(self) -> Vec<ScoredRow<K>> {
        let limit = self.limit;
        let mut rows = self.into_candidates();
        rows.truncate(limit);
        rows
    }
}

impl TopKCollector<u64> {
    #[cfg(test)]
    fn collect(mut self, scorer: &mut dyn ComposableScorer) -> Result<Vec<ScoredRow>> {
        self.collect_mapped(scorer, Ok)?;
        Ok(self.into_rows())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum DisjunctionScore {
    Sum,
    Max,
}

struct EmptyScorer;

impl ComposableScorer for EmptyScorer {
    fn doc(&self) -> Option<u64> {
        None
    }

    fn next(&mut self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn advance(&mut self, _target: u64) -> Result<Option<u64>> {
        Ok(None)
    }

    fn cost(&self) -> usize {
        0
    }

    fn score(&mut self) -> Result<f32> {
        Err(Error::internal(
            "score requested from an empty compound FTS scorer",
        ))
    }

    fn advance_shallow(&mut self, _target: u64) -> Result<u64> {
        Ok(u64::MAX)
    }

    fn score_bounds(&mut self, _up_to: u64) -> Result<ScoreBounds> {
        Ok(ScoreBounds::ZERO)
    }

    fn set_min_competitive_score(&mut self, _min_score: f32) -> Result<()> {
        Ok(())
    }

    fn scores_non_negative(&self) -> bool {
        true
    }
}

struct ScaleScorer<'a> {
    child: BoxScorer<'a>,
    factor: f32,
}

impl<'a> ScaleScorer<'a> {
    fn try_new(child: BoxScorer<'a>, factor: f32) -> Result<Self> {
        if !factor.is_finite() || factor < 0.0 {
            return Err(Error::invalid_input(format!(
                "MatchQuery boost must be finite and non-negative, got {factor}"
            )));
        }
        Ok(Self { child, factor })
    }
}

impl ComposableScorer for ScaleScorer<'_> {
    fn doc(&self) -> Option<u64> {
        self.child.doc()
    }

    fn document_key(&self) -> Option<u64> {
        self.child.document_key()
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.child.next()
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        self.child.advance(target)
    }

    fn cost(&self) -> usize {
        self.child.cost()
    }

    fn score(&mut self) -> Result<f32> {
        checked_score(self.child.score()? * self.factor, "MatchQuery boost")
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        self.child.advance_shallow(target)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        Ok(self
            .child
            .score_bounds(up_to)?
            .scale_non_negative(self.factor))
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        if self.factor > 0.0 {
            self.child
                .set_min_competitive_score(next_down(min_score / self.factor))?;
        }
        Ok(())
    }

    fn matches(&mut self) -> Result<bool> {
        self.child.matches()
    }

    fn match_cost(&self) -> Option<f32> {
        self.child.match_cost()
    }

    fn scores_non_negative(&self) -> bool {
        self.child.scores_non_negative()
    }
}

/// Union scorer used for Boolean SHOULD sums and MultiMatch DisMax.
pub(super) struct DisjunctionScorer<'a> {
    children: Vec<BoxScorer<'a>>,
    mode: DisjunctionScore,
    current: Option<u64>,
    confirmed_doc: Option<u64>,
    confirmed: Vec<bool>,
    min_competitive_score: f32,
}

impl<'a> DisjunctionScorer<'a> {
    pub(super) fn try_new(children: Vec<BoxScorer<'a>>, mode: DisjunctionScore) -> Result<Self> {
        if children.is_empty() {
            return Err(Error::internal(
                "FTS disjunction scorer requires at least one child",
            ));
        }
        let confirmed = vec![false; children.len()];
        Ok(Self {
            children,
            mode,
            current: None,
            confirmed_doc: None,
            confirmed,
            min_competitive_score: f32::NEG_INFINITY,
        })
    }

    fn set_current_from_children(&mut self) -> Option<u64> {
        self.current = self.children.iter().filter_map(|child| child.doc()).min();
        self.confirmed_doc = None;
        self.confirmed.fill(false);
        self.current
    }

    fn ensure_confirmed(&mut self) -> Result<bool> {
        let Some(current) = self.current else {
            return Ok(false);
        };
        if self.confirmed_doc == Some(current) {
            return Ok(self.confirmed.iter().any(|matched| *matched));
        }
        self.confirmed.fill(false);
        for (matched, child) in self.confirmed.iter_mut().zip(&mut self.children) {
            if child.doc() == Some(current) {
                *matched = child.matches()?;
            }
        }
        self.confirmed_doc = Some(current);
        Ok(self.confirmed.iter().any(|matched| *matched))
    }
}

impl ComposableScorer for DisjunctionScorer<'_> {
    fn doc(&self) -> Option<u64> {
        self.current
    }

    fn document_key(&self) -> Option<u64> {
        let current = self.current?;
        self.children
            .iter()
            .find(|child| child.doc() == Some(current))
            .and_then(|child| child.document_key())
    }

    fn next(&mut self) -> Result<Option<u64>> {
        match self.current {
            None => {
                for child in &mut self.children {
                    child.next()?;
                }
            }
            Some(current) => {
                for child in &mut self.children {
                    if child.doc() == Some(current) {
                        child.next()?;
                    }
                }
            }
        }
        Ok(self.set_current_from_children())
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        for child in &mut self.children {
            if child.doc().is_none_or(|doc| doc < target) {
                child.advance(target)?;
            }
        }
        Ok(self.set_current_from_children())
    }

    fn cost(&self) -> usize {
        self.children
            .iter()
            .map(|child| child.cost())
            .fold(0, usize::saturating_add)
    }

    fn score(&mut self) -> Result<f32> {
        if !self.ensure_confirmed()? {
            return Err(Error::internal(
                "FTS disjunction score requested for an unconfirmed document",
            ));
        }
        let mut score = match self.mode {
            DisjunctionScore::Sum => 0.0_f32,
            DisjunctionScore::Max => f32::NEG_INFINITY,
        };
        for (matched, child) in self.confirmed.iter().zip(&mut self.children) {
            if !matched {
                continue;
            }
            let child_score = child.score()?;
            score = match self.mode {
                DisjunctionScore::Sum => score + child_score,
                DisjunctionScore::Max => score.max(child_score),
            };
        }
        checked_score(score, "FTS disjunction")
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let mut up_to = u64::MAX;
        for child in &mut self.children {
            if let Some(doc) = child.doc() {
                up_to = up_to.min(child.advance_shallow(target.max(doc))?);
            }
        }
        Ok(up_to)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        let mut bounds = match self.mode {
            DisjunctionScore::Sum => ScoreBounds::ZERO,
            DisjunctionScore::Max => ScoreBounds {
                lower: f32::INFINITY,
                upper: f32::NEG_INFINITY,
            },
        };
        for child in &mut self.children {
            let child_bounds = if child.doc().is_some_and(|doc| doc <= up_to) {
                child.score_bounds(up_to)?
            } else {
                ScoreBounds::ZERO
            };
            bounds = match self.mode {
                DisjunctionScore::Sum => bounds.add(child_bounds.include_zero()),
                DisjunctionScore::Max => ScoreBounds {
                    lower: bounds.lower.min(child_bounds.lower),
                    upper: bounds.upper.max(child_bounds.upper),
                },
            };
        }
        if bounds.lower == f32::INFINITY {
            Ok(ScoreBounds::ZERO)
        } else {
            Ok(bounds)
        }
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        if min_score.is_nan() {
            return Err(Error::invalid_input(
                "minimum competitive FTS score cannot be NaN",
            ));
        }
        if min_score <= self.min_competitive_score {
            return Ok(());
        }
        self.min_competitive_score = min_score;
        // A child below a DisMax threshold cannot affect a competitive max.
        // Sum scorers need sibling-global bounds before translating the floor,
        // so they keep it at this node and prune from their combined block bound.
        if matches!(self.mode, DisjunctionScore::Max) {
            for child in &mut self.children {
                child.set_min_competitive_score(min_score)?;
            }
        }
        Ok(())
    }

    fn matches(&mut self) -> Result<bool> {
        self.ensure_confirmed()
    }

    fn match_cost(&self) -> Option<f32> {
        self.children
            .iter()
            .filter_map(|child| child.match_cost())
            .reduce(|left, right| left + right)
    }

    fn scores_non_negative(&self) -> bool {
        self.children
            .iter()
            .all(|child| child.scores_non_negative())
    }
}

/// Intersection scorer that requires and scores every Boolean MUST child.
pub(super) struct RequiredConjunctionScorer<'a> {
    children: Vec<BoxScorer<'a>>,
    /// Child indices sorted by approximation cost, omitted when query order is
    /// already cheapest-first. `children` remains in query order so scoring and
    /// score-bound arithmetic stay bit-for-bit stable.
    approximation_order: Option<Vec<usize>>,
    current: Option<u64>,
    confirmed_doc: Option<u64>,
    confirmed: bool,
}

fn align_conjunction_children(
    children: &mut [BoxScorer<'_>],
    mut target: u64,
    child_index: impl Fn(usize) -> usize,
) -> Result<Option<u64>> {
    loop {
        for position in 0..children.len() {
            let child = &mut children[child_index(position)];
            if child.doc().is_none_or(|doc| doc < target) {
                let Some(doc) = child.advance(target)? else {
                    return Ok(None);
                };
                target = target.max(doc);
            }
        }
        let min_doc = children.iter().filter_map(|child| child.doc()).min();
        let max_doc = children.iter().filter_map(|child| child.doc()).max();
        if min_doc == max_doc {
            return Ok(min_doc);
        }
        target = max_doc.ok_or_else(|| {
            Error::internal("FTS conjunction lost a child while aligning scorers")
        })?;
    }
}

impl<'a> RequiredConjunctionScorer<'a> {
    pub(super) fn try_new(children: Vec<BoxScorer<'a>>) -> Result<Self> {
        if children.is_empty() {
            return Err(Error::internal(
                "FTS conjunction scorer requires at least one child",
            ));
        }
        let approximation_order = if children
            .windows(2)
            .all(|pair| pair[0].cost() <= pair[1].cost())
        {
            None
        } else {
            let mut order = (0..children.len()).collect::<Vec<_>>();
            order.sort_by_key(|&index| (children[index].cost(), index));
            Some(order)
        };
        Ok(Self {
            children,
            approximation_order,
            current: None,
            confirmed_doc: None,
            confirmed: false,
        })
    }

    fn align(&mut self, target: u64) -> Result<Option<u64>> {
        self.current = if let Some(order) = &self.approximation_order {
            align_conjunction_children(&mut self.children, target, |position| order[position])?
        } else {
            align_conjunction_children(&mut self.children, target, |position| position)?
        };
        if self.current.is_some() {
            self.confirmed_doc = None;
            self.confirmed = false;
        }
        Ok(self.current)
    }

    fn ensure_confirmed(&mut self) -> Result<bool> {
        let Some(current) = self.current else {
            return Ok(false);
        };
        if self.confirmed_doc == Some(current) {
            return Ok(self.confirmed);
        }
        self.confirmed = true;
        for child in &mut self.children {
            if !child.matches()? {
                self.confirmed = false;
            }
        }
        self.confirmed_doc = Some(current);
        Ok(self.confirmed)
    }
}

impl ComposableScorer for RequiredConjunctionScorer<'_> {
    fn doc(&self) -> Option<u64> {
        self.current
    }

    fn document_key(&self) -> Option<u64> {
        self.children.first().and_then(|child| child.document_key())
    }

    fn next(&mut self) -> Result<Option<u64>> {
        let target = match self.current {
            None => 0,
            Some(u64::MAX) => return Ok(None),
            Some(current) => current + 1,
        };
        self.align(target)
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        self.align(target)
    }

    fn cost(&self) -> usize {
        self.children
            .iter()
            .map(|child| child.cost())
            .min()
            .unwrap_or(0)
    }

    fn score(&mut self) -> Result<f32> {
        if !self.ensure_confirmed()? {
            return Err(Error::internal(
                "FTS conjunction score requested for an unconfirmed document",
            ));
        }
        let mut score = 0.0_f32;
        for child in &mut self.children {
            score += child.score()?;
        }
        checked_score(score, "FTS conjunction")
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let mut up_to = u64::MAX;
        for child in &mut self.children {
            let child_target = child.doc().map_or(target, |doc| target.max(doc));
            up_to = up_to.min(child.advance_shallow(child_target)?);
        }
        Ok(up_to)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        let mut bounds = ScoreBounds::ZERO;
        for child in &mut self.children {
            bounds = bounds.add(child.score_bounds(up_to)?);
        }
        Ok(bounds)
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        if min_score.is_nan() {
            return Err(Error::invalid_input(
                "minimum competitive FTS score cannot be NaN",
            ));
        }
        // Propagating the full conjunction floor to one child is unsafe because
        // individually sub-threshold MUST scores may sum to a competitive hit.
        if self.children.len() == 1 {
            self.children[0].set_min_competitive_score(min_score)?;
        }
        Ok(())
    }

    fn matches(&mut self) -> Result<bool> {
        self.ensure_confirmed()
    }

    fn match_cost(&self) -> Option<f32> {
        self.children
            .iter()
            .filter_map(|child| child.match_cost())
            .reduce(|left, right| left + right)
    }

    fn scores_non_negative(&self) -> bool {
        self.children
            .iter()
            .all(|child| child.scores_non_negative())
    }
}

/// Positive-driven Boost scorer with signed conservative bounds.
pub(super) struct BoostScorer<'a> {
    positive: BoxScorer<'a>,
    negative: BoxScorer<'a>,
    negative_boost: f32,
    negative_matches_doc: Option<u64>,
    negative_matches: bool,
}

impl<'a> BoostScorer<'a> {
    pub(super) fn try_new(
        positive: BoxScorer<'a>,
        negative: BoxScorer<'a>,
        negative_boost: f32,
    ) -> Result<Self> {
        if !negative_boost.is_finite() || negative_boost < 0.0 {
            return Err(Error::invalid_input(format!(
                "BoostQuery negative_boost must be finite and non-negative, got {negative_boost}"
            )));
        }
        Ok(Self {
            positive,
            negative,
            negative_boost,
            negative_matches_doc: None,
            negative_matches: false,
        })
    }

    fn reset_confirmation(&mut self) {
        self.negative_matches_doc = None;
        self.negative_matches = false;
    }

    fn confirm_negative(&mut self) -> Result<bool> {
        let Some(current) = self.positive.doc() else {
            return Ok(false);
        };
        if self.negative_matches_doc == Some(current) {
            return Ok(self.negative_matches);
        }
        self.negative_matches =
            self.negative.advance(current)? == Some(current) && self.negative.matches()?;
        self.negative_matches_doc = Some(current);
        Ok(self.negative_matches)
    }
}

impl ComposableScorer for BoostScorer<'_> {
    fn doc(&self) -> Option<u64> {
        self.positive.doc()
    }

    fn document_key(&self) -> Option<u64> {
        self.positive.document_key()
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.reset_confirmation();
        self.positive.next()
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        self.reset_confirmation();
        self.positive.advance(target)
    }

    fn cost(&self) -> usize {
        self.positive.cost()
    }

    fn score(&mut self) -> Result<f32> {
        let positive = self.positive.score()?;
        let score = if self.confirm_negative()? {
            positive - self.negative_boost * self.negative.score()?
        } else {
            positive
        };
        checked_score(score, "BoostQuery scorer")
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let mut up_to = self.positive.advance_shallow(target)?;
        if self.negative.doc().is_none_or(|doc| doc < target) {
            self.negative.advance(target)?;
        }
        if let Some(doc) = self.negative.doc() {
            up_to = up_to.min(self.negative.advance_shallow(target.max(doc))?);
        }
        Ok(up_to)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        let positive = self.positive.score_bounds(up_to)?;
        let negative = if self.negative.doc().is_some_and(|doc| doc <= up_to) {
            self.negative.score_bounds(up_to)?.include_zero()
        } else {
            ScoreBounds::ZERO
        };
        Ok(positive.subtract_scaled(negative, self.negative_boost))
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        // With a non-negative negative scorer, Boost can only demote the
        // positive score, so the parent's floor is safe for the positive side.
        if self.negative.scores_non_negative() {
            self.positive.set_min_competitive_score(min_score)?;
        }
        Ok(())
    }

    fn matches(&mut self) -> Result<bool> {
        self.positive.matches()
    }

    fn match_cost(&self) -> Option<f32> {
        self.positive.match_cost()
    }
}

/// Boolean scorer preserving the current membership and score semantics.
pub(super) struct BooleanScorer<'a> {
    driver: BoxScorer<'a>,
    optional: Option<BoxScorer<'a>>,
    prohibited: Option<BoxScorer<'a>>,
    current: Option<u64>,
    optional_matches: bool,
}

impl<'a> BooleanScorer<'a> {
    pub(super) fn try_new(
        should: Vec<BoxScorer<'a>>,
        must: Vec<BoxScorer<'a>>,
        must_not: Vec<BoxScorer<'a>>,
    ) -> Result<Self> {
        let mut optional = if should.is_empty() {
            None
        } else {
            Some(
                Box::new(DisjunctionScorer::try_new(should, DisjunctionScore::Sum)?)
                    as BoxScorer<'a>,
            )
        };
        let driver = if must.is_empty() {
            optional.take().ok_or_else(|| {
                Error::invalid_input("boolean query must have at least one should/must query")
            })?
        } else {
            Box::new(RequiredConjunctionScorer::try_new(must)?) as BoxScorer<'a>
        };
        let prohibited = if must_not.is_empty() {
            None
        } else {
            Some(
                Box::new(DisjunctionScorer::try_new(must_not, DisjunctionScore::Max)?)
                    as BoxScorer<'a>,
            )
        };
        Ok(Self {
            driver,
            optional,
            prohibited,
            current: None,
            optional_matches: false,
        })
    }

    fn accept_driver_doc(&mut self) -> Result<bool> {
        let Some(current) = self.driver.doc() else {
            return Ok(false);
        };
        if !self.driver.matches()? {
            return Ok(false);
        }
        if let Some(prohibited) = &mut self.prohibited
            && prohibited.advance(current)? == Some(current)
            && prohibited.matches()?
        {
            return Ok(false);
        }
        self.optional_matches = if let Some(optional) = &mut self.optional {
            optional.advance(current)? == Some(current) && optional.matches()?
        } else {
            false
        };
        self.current = Some(current);
        Ok(true)
    }

    fn next_accepted(&mut self, target: Option<u64>) -> Result<Option<u64>> {
        let mut doc = match target {
            Some(target) => self.driver.advance(target)?,
            None => self.driver.next()?,
        };
        while doc.is_some() {
            if self.accept_driver_doc()? {
                return Ok(self.current);
            }
            doc = self.driver.next()?;
        }
        self.current = None;
        self.optional_matches = false;
        Ok(None)
    }
}

impl ComposableScorer for BooleanScorer<'_> {
    fn doc(&self) -> Option<u64> {
        self.current
    }

    fn document_key(&self) -> Option<u64> {
        self.driver.document_key()
    }

    fn next(&mut self) -> Result<Option<u64>> {
        self.next_accepted(None)
    }

    fn advance(&mut self, target: u64) -> Result<Option<u64>> {
        if self.current.is_some_and(|current| current >= target) {
            return Ok(self.current);
        }
        self.next_accepted(Some(target))
    }

    fn cost(&self) -> usize {
        self.driver.cost()
    }

    fn score(&mut self) -> Result<f32> {
        if self.current.is_none() {
            return Err(Error::internal(
                "Boolean FTS scorer is not positioned on a document",
            ));
        }
        let mut score = self.driver.score()?;
        if self.optional_matches
            && let Some(optional) = &mut self.optional
        {
            score += optional.score()?;
        }
        checked_score(score, "BooleanQuery scorer")
    }

    fn advance_shallow(&mut self, target: u64) -> Result<u64> {
        let mut up_to = self.driver.advance_shallow(target)?;
        if let Some(optional) = &mut self.optional
            && let Some(doc) = optional.doc()
        {
            up_to = up_to.min(optional.advance_shallow(target.max(doc))?);
        }
        Ok(up_to)
    }

    fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
        let mut bounds = self.driver.score_bounds(up_to)?;
        if let Some(optional) = &mut self.optional
            && optional.doc().is_some_and(|doc| doc <= up_to)
        {
            bounds = bounds.add(optional.score_bounds(up_to)?.include_zero());
        }
        Ok(bounds)
    }

    fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
        // When SHOULD is also present, a global sibling bound is required to
        // translate the parent threshold safely. The combined block bound still
        // prunes at this node. Without SHOULD, driver score is the full score.
        if self.optional.is_none() {
            self.driver.set_min_competitive_score(min_score)?;
        }
        Ok(())
    }

    fn matches(&mut self) -> Result<bool> {
        Ok(self.current.is_some())
    }

    fn scores_non_negative(&self) -> bool {
        self.driver.scores_non_negative()
            && self
                .optional
                .as_ref()
                .is_none_or(|optional| optional.scores_non_negative())
    }
}

#[derive(Clone)]
enum LeafQuery {
    Match(MatchQuery),
    Phrase(PhraseQuery),
}

impl LeafQuery {
    fn terms(&self) -> &str {
        match self {
            Self::Match(query) => &query.terms,
            Self::Phrase(query) => &query.terms,
        }
    }

    fn operator(&self) -> Operator {
        match self {
            Self::Match(query) => query.operator,
            Self::Phrase(_) => Operator::And,
        }
    }

    fn effective_params(&self, params: &FtsSearchParams) -> FtsSearchParams {
        match self {
            Self::Match(query) => params
                .clone()
                .with_limit(None)
                .with_phrase_slop(None)
                .with_fuzziness(query.fuzziness)
                .with_max_expansions(query.max_expansions)
                .with_prefix_length(query.prefix_length),
            Self::Phrase(query) => params
                .clone()
                .with_limit(None)
                .with_phrase_slop(Some(query.slop)),
        }
    }
}

fn collect_leaf_queries(query: &FtsQuery, leaves: &mut Vec<LeafQuery>) -> Result<()> {
    match query {
        FtsQuery::Match(query) => leaves.push(LeafQuery::Match(query.clone())),
        FtsQuery::Phrase(query) => leaves.push(LeafQuery::Phrase(query.clone())),
        FtsQuery::Boost(query) => {
            collect_leaf_queries(&query.positive, leaves)?;
            collect_leaf_queries(&query.negative, leaves)?;
        }
        FtsQuery::MultiMatch(query) => {
            leaves.extend(query.match_queries.iter().cloned().map(LeafQuery::Match));
        }
        FtsQuery::Boolean(query) => {
            for child in query
                .should
                .iter()
                .chain(&query.must)
                .chain(&query.must_not)
            {
                collect_leaf_queries(child, leaves)?;
            }
        }
    }
    Ok(())
}

struct PreparedLeaf {
    tokens_by_segment: Vec<Arc<Tokens>>,
    params: Arc<FtsSearchParams>,
    operator: Operator,
    scorer: Arc<MemBM25Scorer>,
}

fn tokenize_leaf(index: &InvertedIndex, leaf: &LeafQuery, params: &FtsSearchParams) -> Tokens {
    let is_fuzzy_match = matches!(leaf, LeafQuery::Match(_))
        && matches!(params.fuzziness, Some(distance) if distance != 0);
    let mut tokenizer = if is_fuzzy_match {
        let analyzer = TextAnalyzer::from(SimpleTokenizer::default());
        match index.tokenizer().doc_type() {
            DocType::Text => Box::new(TextTokenizer::new(analyzer)) as Box<dyn LanceTokenizer>,
            DocType::Json => Box::new(JsonTokenizer::new(analyzer)) as Box<dyn LanceTokenizer>,
        }
    } else {
        index.tokenizer()
    };
    collect_query_tokens(leaf.terms(), &mut tokenizer)
}

fn expanded_leaf_tokens(
    index: &InvertedIndex,
    tokens: &Tokens,
    params: &FtsSearchParams,
    operator: Operator,
) -> Result<Tokens> {
    if !matches!(params.fuzziness, Some(distance) if distance != 0) {
        return Ok(tokens.clone());
    }
    let expanded = index.expand_fuzzy_tokens(tokens, params)?;
    if operator == Operator::And || params.phrase_slop.is_some() {
        let surviving = (0..expanded.len())
            .map(|index| expanded.position(index))
            .collect::<HashSet<_>>();
        if (0..tokens.len()).any(|index| !surviving.contains(&tokens.position(index))) {
            return Ok(Tokens::with_positions(
                Vec::new(),
                Vec::new(),
                tokens.token_type().clone(),
            ));
        }
    }
    Ok(expanded)
}

fn validate_injected_scorer_tokens(scorer: &MemBM25Scorer, tokens: &Tokens) -> Result<()> {
    for token in tokens {
        if !scorer.token_docs.contains_key(token) {
            return Err(Error::invalid_input(format!(
                "injected BM25 scorer is missing compound FTS token '{token}'"
            )));
        }
    }
    Ok(())
}

async fn prepare_compound_query(
    indices: &[Arc<InvertedIndex>],
    query: &FtsQuery,
    params: &FtsSearchParams,
    metrics: &dyn MetricsCollector,
    base_scorer: Option<Arc<MemBM25Scorer>>,
) -> Result<(CompoundScorerPlan, Vec<PreparedLeaf>)> {
    let first_index = indices
        .first()
        .ok_or_else(|| Error::invalid_input("compound FTS requires at least one index segment"))?;
    let mut leaf_queries = Vec::new();
    collect_leaf_queries(query, &mut leaf_queries)?;
    let mut num_plan_leaves = 0;
    let plan = CompoundScorerPlan::from_query(query, &mut num_plan_leaves)?;
    if num_plan_leaves != leaf_queries.len() {
        return Err(Error::internal(format!(
            "compound FTS planned {num_plan_leaves} leaves but prepared {}",
            leaf_queries.len()
        )));
    }

    let mut leaves = Vec::with_capacity(leaf_queries.len());
    for leaf in leaf_queries {
        let effective_params = leaf.effective_params(params);
        let tokens = tokenize_leaf(first_index, &leaf, &effective_params);
        let scorer = match &base_scorer {
            Some(scorer) => scorer.clone(),
            None => Arc::new(
                build_global_bm25_scorer(indices, &tokens, &effective_params, Some(metrics))
                    .await?,
            ),
        };
        let mut tokens_by_segment = Vec::with_capacity(indices.len());
        for index in indices {
            let expanded_tokens =
                expanded_leaf_tokens(index, &tokens, &effective_params, leaf.operator())?;
            if base_scorer.is_some() {
                validate_injected_scorer_tokens(&scorer, &expanded_tokens)?;
            }
            tokens_by_segment.push(Arc::new(expanded_tokens));
        }
        leaves.push(PreparedLeaf {
            tokens_by_segment,
            params: Arc::new(effective_params),
            operator: leaf.operator(),
            scorer,
        });
    }
    Ok((plan, leaves))
}

struct LoadedLeaf {
    postings: Vec<PostingIterator>,
    params: Arc<FtsSearchParams>,
    operator: Operator,
    scorer: Arc<MemBM25Scorer>,
}

enum LoadedDocuments {
    Legacy(Arc<DocSet>),
    Modern {
        documents: Arc<PartitionDocuments>,
        lengths: Arc<DocLengths>,
        visibility: DocVisibility,
        projection: Option<ResidentAddressProjection>,
    },
}

struct LoadedPartition {
    segment_ordinal: usize,
    partition_ordinal: usize,
    partition: Arc<InvertedPartition>,
    documents: LoadedDocuments,
    leaves: Vec<LoadedLeaf>,
}

async fn load_compound_partition(
    segment_ordinal: usize,
    partition_ordinal: usize,
    partition: Arc<InvertedPartition>,
    leaves: &[PreparedLeaf],
    mask: Arc<RowAddrMask>,
    metrics: Arc<dyn MetricsCollector>,
) -> Result<Option<LoadedPartition>> {
    let leaf_loads = leaves.iter().map(|leaf| {
        let partition = partition.clone();
        let tokens = leaf.tokens_by_segment[segment_ordinal].clone();
        let params = leaf.params.clone();
        let scorer = leaf.scorer.clone();
        let metrics = metrics.clone();
        let operator = leaf.operator;
        async move {
            let postings = if tokens.is_empty() {
                Vec::new()
            } else {
                partition
                    .load_posting_lists(
                        tokens.as_ref(),
                        params.as_ref(),
                        operator,
                        scorer.as_ref(),
                        metrics.as_ref(),
                        true,
                    )
                    .await?
                    .postings
            };
            Result::Ok(LoadedLeaf {
                postings,
                params,
                operator,
                scorer,
            })
        }
    });
    let leaves = futures::future::try_join_all(leaf_loads).await?;

    let documents = if let Some(docs) = partition.docs.legacy() {
        LoadedDocuments::Legacy(docs.clone())
    } else {
        let documents = partition.docs.modern().cloned().ok_or_else(|| {
            Error::internal("FTS partition contains neither legacy nor modern documents")
        })?;
        let materialize_selected = mask.max_len().is_some_and(|selected| {
            u128::from(selected).saturating_mul(100)
                <= u128::from(*FLAT_SEARCH_PERCENT_THRESHOLD)
                    .saturating_mul(documents.len() as u128)
        });
        let visibility = match documents.immediate_visibility(mask.clone(), materialize_selected) {
            Some(visibility) => visibility,
            None => {
                documents
                    .visibility(mask.clone(), materialize_selected)
                    .await?
            }
        };
        if visibility.is_empty() {
            return Ok(None);
        }
        let lengths = match documents.cached_lengths() {
            Some(lengths) => lengths,
            None => documents.lengths().await?,
        };
        let projection = documents.resident_address_projection();
        LoadedDocuments::Modern {
            documents,
            lengths,
            visibility,
            projection,
        }
    };

    Ok(Some(LoadedPartition {
        segment_ordinal,
        partition_ordinal,
        partition,
        documents,
        leaves,
    }))
}

struct DeferredCompoundRows {
    documents: Arc<PartitionDocuments>,
    rows: Vec<ScoredRow<DocId>>,
}

struct OverflowedCompoundPartition {
    segment_ordinal: usize,
    partition_ordinal: usize,
    partition: Arc<InvertedPartition>,
    documents: Arc<PartitionDocuments>,
}

enum PartitionCollectionBoundary {
    Deferred(DeferredCompoundRows),
    Overflow(OverflowedCompoundPartition),
}

struct CollectedPartitions {
    collector: TopKCollector<u64>,
    remaining: Vec<LoadedPartition>,
    boundary: Option<PartitionCollectionBoundary>,
}

fn collect_partition_with_documents<D, K>(
    documents: &D,
    leaves: Vec<LoadedLeaf>,
    plan: &CompoundScorerPlan,
    metrics: &dyn MetricsCollector,
    collector: &mut TopKCollector<K>,
    mut map_document: impl FnMut(u64) -> Result<K>,
) -> Result<CollectionStatus>
where
    D: WandDocuments + Sync,
    K: Copy + Ord,
{
    let mut leaf_scorers = leaves
        .into_iter()
        .map(|leaf| {
            let scorer: BoxScorer<'_> = if leaf.postings.is_empty() {
                Box::new(EmptyScorer)
            } else {
                Box::new(WandCursor::new(
                    leaf.operator,
                    leaf.postings,
                    documents,
                    leaf.scorer,
                    leaf.params.as_ref(),
                    metrics,
                ))
            };
            Some(scorer)
        })
        .collect::<Vec<_>>();
    let mut scorer = plan.build(&mut leaf_scorers)?;
    if leaf_scorers.iter().any(Option::is_some) {
        return Err(Error::internal(
            "compound FTS scorer did not consume every prepared leaf",
        ));
    }
    collector.collect_mapped(scorer.as_mut(), &mut map_document)
}

fn collect_loaded_partitions(
    partitions: Vec<LoadedPartition>,
    plan: &CompoundScorerPlan,
    mask: &RowAddrMask,
    metrics: &dyn MetricsCollector,
    mut collector: TopKCollector<u64>,
) -> Result<CollectedPartitions> {
    let mut partitions = partitions.into_iter();
    while let Some(partition) = partitions.next() {
        let LoadedPartition {
            segment_ordinal,
            partition_ordinal,
            partition: source,
            documents,
            leaves,
        } = partition;
        match documents {
            LoadedDocuments::Legacy(docs) => {
                let documents = LegacyWandDocuments::new(docs.as_ref(), mask);
                let status = collect_partition_with_documents(
                    &documents,
                    leaves,
                    plan,
                    metrics,
                    &mut collector,
                    Ok,
                )?;
                debug_assert_eq!(status, CollectionStatus::Complete);
            }
            LoadedDocuments::Modern {
                documents: partition_documents,
                lengths,
                visibility,
                projection,
            } => {
                let documents = ModernWandDocuments::filtered(lengths.as_ref(), &visibility);
                if let Some(projection) = projection {
                    let mut addresses_resolved = 0;
                    let status = collect_partition_with_documents(
                        &documents,
                        leaves,
                        plan,
                        metrics,
                        &mut collector,
                        |doc_id| {
                            let doc_id = DocId::new(u32::try_from(doc_id).map_err(|_| {
                                Error::index(format!(
                                    "FTS DocId {doc_id} exceeds the modern u32 domain"
                                ))
                            })?);
                            let row_id = projection.address(doc_id).ok_or_else(|| {
                                Error::internal(format!(
                                    "compound FTS scorer returned non-visible DocId {} in segment {segment_ordinal}, partition {partition_ordinal}",
                                    doc_id.get()
                                ))
                            })?;
                            addresses_resolved += 1;
                            Ok(row_id)
                        },
                    )?;
                    debug_assert_eq!(status, CollectionStatus::Complete);
                    metrics.record_compound_addresses_resolved(addresses_resolved);
                } else {
                    let max_buffered = collector
                        .limit
                        .saturating_add(SCORE_FLOOR_RESOLUTION_BATCH_SIZE);
                    let mut local_collector = TopKCollector::retaining_score_floor(
                        collector.limit,
                        collector.competitive_score.clone(),
                        max_buffered,
                    );
                    let status = collect_partition_with_documents(
                        &documents,
                        leaves,
                        plan,
                        metrics,
                        &mut local_collector,
                        |doc_id| {
                            Ok(DocId::new(u32::try_from(doc_id).map_err(|_| {
                                Error::index(format!(
                                    "FTS DocId {doc_id} exceeds the modern u32 domain"
                                ))
                            })?))
                        },
                    )?;
                    metrics.record_compound_peak_buffered_candidates(local_collector.peak_buffered);
                    let boundary = match status {
                        CollectionStatus::Complete => {
                            PartitionCollectionBoundary::Deferred(DeferredCompoundRows {
                                documents: partition_documents,
                                rows: local_collector.into_candidates(),
                            })
                        }
                        CollectionStatus::ScoreFloorOverflow => {
                            metrics.record_compound_score_floor_overflows(1);
                            PartitionCollectionBoundary::Overflow(OverflowedCompoundPartition {
                                segment_ordinal,
                                partition_ordinal,
                                partition: source,
                                documents: partition_documents,
                            })
                        }
                    };
                    metrics.record_compound_peak_buffered_candidates(collector.peak_buffered);
                    return Ok(CollectedPartitions {
                        collector,
                        remaining: partitions.collect(),
                        boundary: Some(boundary),
                    });
                }
            }
        }
    }
    metrics.record_compound_peak_buffered_candidates(collector.peak_buffered);
    Ok(CollectedPartitions {
        collector,
        remaining: Vec::new(),
        boundary: None,
    })
}

async fn merge_resolved_compound_rows(
    collector: &mut TopKCollector<u64>,
    deferred: DeferredCompoundRows,
    metrics: &dyn MetricsCollector,
) -> Result<()> {
    for rows in deferred.rows.chunks(SCORE_FLOOR_RESOLUTION_BATCH_SIZE) {
        let doc_ids = rows.iter().map(|row| row.row_id).collect::<Vec<_>>();
        let addresses = deferred.documents.resolve_addresses(&doc_ids).await?;
        if addresses.len() != rows.len() {
            return Err(Error::internal(format!(
                "compound FTS resolved {} addresses for {} DocIds",
                addresses.len(),
                rows.len()
            )));
        }
        metrics.record_compound_address_resolution_batches(1);
        metrics.record_compound_peak_address_resolution_batch_size(rows.len());
        metrics.record_compound_addresses_resolved(addresses.len());
        for (row, row_id) in rows.iter().zip(addresses) {
            let status = collector.insert(ScoredRow {
                row_id,
                score: row.score,
            });
            debug_assert_eq!(status, CollectionStatus::Complete);
        }
    }
    metrics.record_compound_peak_buffered_candidates(collector.peak_buffered);
    Ok(())
}

async fn reload_compound_partition_with_projection(
    overflow: OverflowedCompoundPartition,
    leaves: &[PreparedLeaf],
    mask: Arc<RowAddrMask>,
    metrics: Arc<dyn MetricsCollector>,
) -> Result<LoadedPartition> {
    let projection = overflow.documents.address_projection().await?;
    let mut loaded = load_compound_partition(
        overflow.segment_ordinal,
        overflow.partition_ordinal,
        overflow.partition,
        leaves,
        mask,
        metrics,
    )
    .await?
    .ok_or_else(|| {
        Error::internal(format!(
            "compound FTS retry lost visible documents in segment {}, partition {}",
            overflow.segment_ordinal, overflow.partition_ordinal
        ))
    })?;
    match &mut loaded.documents {
        LoadedDocuments::Modern {
            projection: loaded_projection,
            ..
        } => *loaded_projection = Some(projection),
        LoadedDocuments::Legacy(_) => {
            return Err(Error::internal(format!(
                "compound FTS retry changed segment {}, partition {} from modern to legacy documents",
                overflow.segment_ordinal, overflow.partition_ordinal
            )));
        }
    }
    Ok(loaded)
}

/// Search one-column compound FTS directly over posting-backed scorers.
///
/// The caller must provide all committed index segments for the column and a
/// ready prefilter. One collector owns the global top-k heap and propagates its
/// score floor through every partition-local scorer tree. Modern partitions
/// resolve candidates in bounded batches; an oversized kth-score tie is retried
/// against a resident row-address projection so final row-id ordering stays exact.
pub async fn compound_search(
    indices: &[Arc<InvertedIndex>],
    query: &FtsQuery,
    params: &FtsSearchParams,
    prefilter: Arc<dyn PreFilter>,
    metrics: Arc<dyn MetricsCollector>,
) -> Result<(Vec<u64>, Vec<f32>)> {
    compound_search_impl(indices, query, params, prefilter, metrics, None).await
}

/// Search one-column compound FTS with caller-supplied corpus-wide BM25 statistics.
///
/// The scorer must contain an entry for every token used by every query leaf,
/// including terms produced by fuzzy expansion. An incomplete scorer is rejected
/// instead of treating missing token statistics as zero.
pub async fn compound_search_with_base_scorer(
    indices: &[Arc<InvertedIndex>],
    query: &FtsQuery,
    params: &FtsSearchParams,
    prefilter: Arc<dyn PreFilter>,
    metrics: Arc<dyn MetricsCollector>,
    base_scorer: Arc<MemBM25Scorer>,
) -> Result<(Vec<u64>, Vec<f32>)> {
    compound_search_impl(
        indices,
        query,
        params,
        prefilter,
        metrics,
        Some(base_scorer),
    )
    .await
}

async fn compound_search_impl(
    indices: &[Arc<InvertedIndex>],
    query: &FtsQuery,
    params: &FtsSearchParams,
    prefilter: Arc<dyn PreFilter>,
    metrics: Arc<dyn MetricsCollector>,
    base_scorer: Option<Arc<MemBM25Scorer>>,
) -> Result<(Vec<u64>, Vec<f32>)> {
    let limit = params.limit.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok((Vec::new(), Vec::new()));
    }
    let (plan, leaves) =
        prepare_compound_query(indices, query, params, metrics.as_ref(), base_scorer).await?;
    prefilter.wait_for_ready().await?;
    let mask = prefilter.mask();
    let mut collector = TopKCollector::new(limit);

    for (segment_ordinal, index) in indices.iter().enumerate() {
        let loads =
            index
                .partitions
                .iter()
                .cloned()
                .enumerate()
                .map(|(partition_ordinal, partition)| {
                    load_compound_partition(
                        segment_ordinal,
                        partition_ordinal,
                        partition,
                        &leaves,
                        mask.clone(),
                        metrics.clone(),
                    )
                });
        let mut partitions = stream::iter(loads)
            .buffer_unordered(get_num_compute_intensive_cpus().clamp(1, 32))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        while !partitions.is_empty() {
            let cpu_plan = plan.clone();
            let cpu_mask = mask.clone();
            let cpu_metrics = metrics.clone();
            let collected = spawn_cpu(move || {
                collect_loaded_partitions(
                    partitions,
                    &cpu_plan,
                    cpu_mask.as_ref(),
                    cpu_metrics.as_ref(),
                    collector,
                )
            })
            .await?;
            collector = collected.collector;
            partitions = collected.remaining;
            match collected.boundary {
                Some(PartitionCollectionBoundary::Deferred(deferred)) => {
                    merge_resolved_compound_rows(&mut collector, deferred, metrics.as_ref())
                        .await?;
                }
                Some(PartitionCollectionBoundary::Overflow(overflow)) => {
                    let retry = reload_compound_partition_with_projection(
                        overflow,
                        &leaves,
                        mask.clone(),
                        metrics.clone(),
                    )
                    .await?;
                    partitions.insert(0, retry);
                }
                None => debug_assert!(partitions.is_empty()),
            }
        }
    }

    let rows = collector.into_rows();
    Ok(rows.into_iter().map(|row| (row.row_id, row.score)).unzip())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn rows(values: &[(u64, f32)]) -> Vec<ScoredRow> {
        values
            .iter()
            .map(|(row_id, score)| ScoredRow::new(*row_id, *score).unwrap())
            .collect()
    }

    fn materialized(values: &[(u64, f32)]) -> Box<dyn ComposableScorer> {
        Box::new(MaterializedScorer::try_new(rows(values)).unwrap())
    }

    #[test]
    fn score_bounds_are_conservative_under_nested_sum_and_boost() {
        let should = DisjunctionScorer::try_new(
            vec![
                materialized(&[(0, 0.1), (2, 2.0)]),
                materialized(&[(0, 0.2)]),
            ],
            DisjunctionScore::Sum,
        )
        .unwrap();
        let negative = materialized(&[(0, 0.3), (2, 5.0)]);
        let mut scorer = BoostScorer::try_new(Box::new(should), negative, 0.5).unwrap();

        assert_eq!(scorer.next().unwrap(), Some(0));
        let up_to = scorer.advance_shallow(0).unwrap();
        let bounds = scorer.score_bounds(up_to).unwrap();
        let first_score = scorer.score().unwrap();
        assert!(bounds.lower <= first_score);
        assert!(bounds.upper >= first_score);

        assert_eq!(scorer.next().unwrap(), Some(2));
        let second_score = scorer.score().unwrap();
        assert!(second_score.is_sign_negative());
        let up_to = scorer.advance_shallow(2).unwrap();
        let bounds = scorer.score_bounds(up_to).unwrap();
        assert!(bounds.lower <= second_score);
        assert!(bounds.upper >= second_score);
    }

    #[test]
    fn collector_propagates_threshold_across_partitions_and_keeps_ties() {
        let mut collector = TopKCollector::new(2);
        let mut first = MaterializedScorer::try_new(rows(&[(8, 9.0), (4, 10.0), (3, 9.0)]))
            .unwrap()
            .with_block_size(1);
        collector.collect_mapped(&mut first, Ok).unwrap();
        assert_eq!(collector.competitive_score.get(), 9.0);

        let mut second = MaterializedScorer::try_new(rows(&[(1, 1.0), (2, 9.0)]))
            .unwrap()
            .with_block_size(1);
        collector.collect_mapped(&mut second, Ok).unwrap();
        assert_eq!(
            collector.into_rows(),
            vec![
                ScoredRow {
                    row_id: 4,
                    score: 10.0
                },
                ScoredRow {
                    row_id: 2,
                    score: 9.0
                }
            ]
        );
    }

    #[test]
    fn collector_bounds_equal_score_candidates() {
        let limit = 1;
        let num_candidates = DEFAULT_BLOCK_SIZE * 4;
        let values = (0..num_candidates)
            .map(|row_id| (row_id as u64, 1.0))
            .collect::<Vec<_>>();
        let mut scorer = MaterializedScorer::try_new(rows(&values)).unwrap();
        let max_buffered = limit + SCORE_FLOOR_RESOLUTION_BATCH_SIZE;
        let mut collector = TopKCollector::retaining_score_floor(
            limit,
            Arc::new(CompetitiveScore::default()),
            max_buffered,
        );

        let status = collector.collect_mapped(&mut scorer, Ok).unwrap();

        assert_eq!(status, CollectionStatus::ScoreFloorOverflow);
        assert_eq!(collector.heap.len(), max_buffered);
    }

    #[test]
    fn collector_reclaims_obsolete_score_floor_before_overflowing() {
        let limit = 2;
        let max_buffered = 4;
        let values = [
            (0, 1.0),
            (1, 1.0),
            (2, 1.0),
            (3, 2.0),
            (4, 2.0),
            (5, 2.0),
            (6, 2.0),
            (7, 2.0),
        ];
        let mut scorer = MaterializedScorer::try_new(rows(&values)).unwrap();
        let competitive_score = Arc::new(CompetitiveScore::default());
        let mut collector =
            TopKCollector::retaining_score_floor(limit, competitive_score.clone(), max_buffered);

        let status = collector.collect_mapped(&mut scorer, Ok).unwrap();

        assert_eq!(status, CollectionStatus::ScoreFloorOverflow);
        assert_eq!(collector.heap.len(), max_buffered);
        assert!(collector.heap.iter().all(|row| row.0.score == 2.0));
        assert_eq!(competitive_score.get(), 2.0);
    }

    struct TwoPhaseScorer {
        inner: MaterializedScorer,
        accepted: Vec<u64>,
        confirmations: usize,
    }

    impl ComposableScorer for TwoPhaseScorer {
        fn doc(&self) -> Option<u64> {
            self.inner.doc()
        }

        fn next(&mut self) -> Result<Option<u64>> {
            self.inner.next()
        }

        fn advance(&mut self, target: u64) -> Result<Option<u64>> {
            self.inner.advance(target)
        }

        fn cost(&self) -> usize {
            self.inner.cost()
        }

        fn score(&mut self) -> Result<f32> {
            self.inner.score()
        }

        fn advance_shallow(&mut self, target: u64) -> Result<u64> {
            self.inner.advance_shallow(target)
        }

        fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
            self.inner.score_bounds(up_to)
        }

        fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
            self.inner.set_min_competitive_score(min_score)
        }

        fn matches(&mut self) -> Result<bool> {
            self.confirmations += 1;
            Ok(self
                .doc()
                .is_some_and(|doc| self.accepted.binary_search(&doc).is_ok()))
        }

        fn scores_non_negative(&self) -> bool {
            true
        }
    }

    struct CountingScorer {
        inner: MaterializedScorer,
        cost: usize,
        advance_calls: Arc<AtomicUsize>,
    }

    impl ComposableScorer for CountingScorer {
        fn doc(&self) -> Option<u64> {
            self.inner.doc()
        }

        fn next(&mut self) -> Result<Option<u64>> {
            self.inner.next()
        }

        fn advance(&mut self, target: u64) -> Result<Option<u64>> {
            self.advance_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.advance(target)
        }

        fn cost(&self) -> usize {
            self.cost
        }

        fn score(&mut self) -> Result<f32> {
            self.inner.score()
        }

        fn advance_shallow(&mut self, target: u64) -> Result<u64> {
            self.inner.advance_shallow(target)
        }

        fn score_bounds(&mut self, up_to: u64) -> Result<ScoreBounds> {
            self.inner.score_bounds(up_to)
        }

        fn set_min_competitive_score(&mut self, min_score: f32) -> Result<()> {
            self.inner.set_min_competitive_score(min_score)
        }

        fn matches(&mut self) -> Result<bool> {
            self.inner.matches()
        }

        fn scores_non_negative(&self) -> bool {
            self.inner.scores_non_negative()
        }
    }

    fn counting(
        values: &[(u64, f32)],
        cost: usize,
    ) -> (Box<dyn ComposableScorer>, Arc<AtomicUsize>) {
        let advance_calls = Arc::new(AtomicUsize::new(0));
        let scorer = CountingScorer {
            inner: MaterializedScorer::try_new(rows(values)).unwrap(),
            cost,
            advance_calls: advance_calls.clone(),
        };
        (Box::new(scorer), advance_calls)
    }

    #[test]
    fn collector_confirms_two_phase_matches_without_a_cost_hint() {
        let mut scorer = TwoPhaseScorer {
            inner: MaterializedScorer::try_new(rows(&[(1, 100.0), (2, 2.0), (3, 1.0)])).unwrap(),
            accepted: vec![2, 3],
            confirmations: 0,
        };
        let results = TopKCollector::new(2).collect(&mut scorer).unwrap();
        assert_eq!(results, rows(&[(2, 2.0), (3, 1.0)]));
        assert_eq!(scorer.confirmations, 3);
        assert_eq!(scorer.match_cost(), None);
    }

    #[test]
    fn required_conjunction_uses_all_must_scores_for_competitive_bounds() {
        let left = Box::new(
            MaterializedScorer::try_new(rows(&[(1, 3.0), (3, 1.0)]))
                .unwrap()
                .with_block_size(1),
        );
        let right = Box::new(
            MaterializedScorer::try_new(rows(&[(1, 30.0), (3, 10.0)]))
                .unwrap()
                .with_block_size(1),
        );
        let mut scorer = RequiredConjunctionScorer::try_new(vec![left, right]).unwrap();
        let competitive_score = Arc::new(CompetitiveScore::default());
        competitive_score.raise(10.0);

        let results = TopKCollector::with_competitive_score(10, competitive_score)
            .collect(&mut scorer)
            .unwrap();

        assert_eq!(results, rows(&[(1, 33.0), (3, 11.0)]));
    }

    #[test]
    fn required_conjunction_aligns_cheapest_clause_first() {
        let dense_rows = (0..=100).map(|row_id| (row_id, 1.0)).collect::<Vec<_>>();
        let (dense, dense_advance_calls) = counting(&dense_rows, 101);
        let (rare, rare_advance_calls) = counting(&[(50, 1.0)], 1);
        let mut scorer = RequiredConjunctionScorer::try_new(vec![dense, rare]).unwrap();
        assert_eq!(scorer.approximation_order.as_deref(), Some(&[1, 0][..]));

        assert_eq!(scorer.next().unwrap(), Some(50));
        assert_eq!(scorer.next().unwrap(), None);
        assert_eq!(dense_advance_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(rare_advance_calls.load(AtomicOrdering::Relaxed), 2);

        let (rare, _) = counting(&[(50, 1.0)], 1);
        let (dense, _) = counting(&dense_rows, 101);
        let scorer = RequiredConjunctionScorer::try_new(vec![rare, dense]).unwrap();
        assert!(scorer.approximation_order.is_none());
    }

    #[test]
    fn required_conjunction_preserves_query_score_order() {
        let (large, _) = counting(&[(0, 16_777_216.0)], 3);
        let (first_small, _) = counting(&[(0, 1.0)], 1);
        let (second_small, _) = counting(&[(0, 1.0)], 2);
        let mut scorer =
            RequiredConjunctionScorer::try_new(vec![large, first_small, second_small]).unwrap();

        assert_eq!(scorer.next().unwrap(), Some(0));
        assert_eq!(scorer.score().unwrap(), 16_777_216.0);
    }

    #[test]
    fn boolean_sums_all_matching_clause_scores() {
        let must = vec![
            materialized(&[(1, 3.0), (2, 2.0), (3, 1.0)]),
            materialized(&[(1, 30.0), (3, 10.0)]),
        ];
        let should = vec![
            materialized(&[(1, 0.5), (3, 4.0)]),
            materialized(&[(3, 2.0)]),
        ];
        let must_not = vec![materialized(&[(1, 9.0)])];
        let mut boolean = BooleanScorer::try_new(should, must, must_not).unwrap();
        let results = TopKCollector::new(10).collect(&mut boolean).unwrap();
        assert_eq!(
            results,
            vec![ScoredRow {
                row_id: 3,
                score: 17.0
            }]
        );

        let mut dismax = DisjunctionScorer::try_new(
            vec![
                materialized(&[(1, 2.0), (3, 3.0)]),
                materialized(&[(1, 4.0), (2, 4.0)]),
            ],
            DisjunctionScore::Max,
        )
        .unwrap();
        let results = TopKCollector::new(2).collect(&mut dismax).unwrap();
        assert_eq!(results, rows(&[(1, 4.0), (2, 4.0)]));
    }
}
