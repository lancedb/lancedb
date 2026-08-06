// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::mem::size_of;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, LargeBinaryArray};
use lance_core::{Error, Result};

use super::scorer::Scorer;

pub const IMPACT_LEVEL1_BLOCKS: usize = 32;
const SMALL_FRONTIER_FREQ_LIMIT: usize = 256;

/// On-disk encoding of one impact entry, shared by every posting block size.
///
/// Entries contain `[doc_up_to varint][pair_count varint][pairs...]`. Each
/// pair stores a varint whose high bits are `freq_delta - 1` and whose low bit
/// reports whether a one-byte norm delta follows. The norm itself is the
/// quantized `u8` document-length code; the common `norm_delta == 1` case needs
/// no norm byte.
#[derive(Debug, Clone)]
pub struct ImpactSkipData {
    entries: LargeBinaryArray,
    level0_len: usize,
    // Last doc id covered by each entry (level0 entries then level1 entries),
    // decoded once at construction. Level1 markers are fully validated because
    // WAND may use them to skip a group; u32::MAX marks malformed entries.
    entry_doc_up_tos: Arc<[u32]>,
    // The most recently baked bounds with a stable scorer key. Each query holds
    // its own Arc in ImpactScoreCache, so replacing this slot for another scorer
    // cannot change bounds already in use. Scorers without a key never enter the
    // shared slot. Malformed entries bake to INFINITY so pruning stays safe.
    last_keyed_bounds: Arc<Mutex<LastKeyedImpactBounds>>,
}

impl PartialEq for ImpactSkipData {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries && self.level0_len == other.level0_len
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct ImpactScore {
    pub score: f32,
    pub entries_scanned: usize,
}

#[derive(Debug)]
struct ImpactBounds {
    per_entry: Box<[f32]>,
    global: f32,
}

type LastKeyedImpactBounds = Option<(u64, Arc<ImpactBounds>)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImpactBoundsCacheKey {
    Keyed(u64),
    QueryLocal,
}

#[derive(Debug, Default, Clone)]
pub struct ImpactScoreCache {
    key: Option<ImpactBoundsCacheKey>,
    bounds: Option<Arc<ImpactBounds>>,
}

impl ImpactScoreCache {
    fn bounds<'a, S: Scorer + ?Sized>(
        &'a mut self,
        impacts: &ImpactSkipData,
        scorer: &S,
    ) -> &'a ImpactBounds {
        let scorer_key = scorer.doc_weight_cache_key();
        let cache_key = scorer_key
            .map(ImpactBoundsCacheKey::Keyed)
            .unwrap_or(ImpactBoundsCacheKey::QueryLocal);
        if self.key != Some(cache_key) {
            self.key = Some(cache_key);
            self.bounds = None;
        }

        self.bounds
            .get_or_insert_with(|| impacts.bounds_for_scorer(scorer, scorer_key))
    }

    fn entry_score<S: Scorer + ?Sized>(
        &mut self,
        impacts: &ImpactSkipData,
        entry_idx: usize,
        query_weight: f32,
        scorer: &S,
    ) -> f32 {
        if query_weight <= 0.0 {
            return 0.0;
        }
        query_weight * self.bounds(impacts, scorer).per_entry[entry_idx]
    }
}

impl ImpactSkipData {
    pub fn new(entries: LargeBinaryArray, level0_len: usize) -> Result<Self> {
        let expected_len = level0_len + level1_len(level0_len);
        if entries.len() != expected_len {
            return Err(Error::index(format!(
                "impact entry count mismatch: got {}, expected {} for {} level0 blocks",
                entries.len(),
                expected_len,
                level0_len
            )));
        }
        let entry_doc_up_tos = (0..entries.len())
            .map(|entry_idx| {
                if entries.is_null(entry_idx) {
                    return u32::MAX;
                }
                let bytes = entries.value(entry_idx);
                let doc_up_to = if entry_idx < level0_len {
                    decode_level0_entry_doc_up_to(bytes)
                } else {
                    decode_entry_doc_up_to(bytes)
                };
                doc_up_to.unwrap_or(u32::MAX)
            })
            .collect::<Arc<[u32]>>();
        Ok(Self {
            entries,
            level0_len,
            entry_doc_up_tos,
            last_keyed_bounds: Arc::new(Mutex::new(None)),
        })
    }

    fn keyed_bounds_guard(&self) -> MutexGuard<'_, LastKeyedImpactBounds> {
        match self.last_keyed_bounds.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn bounds_for_scorer<S: Scorer + ?Sized>(
        &self,
        scorer: &S,
        scorer_key: Option<u64>,
    ) -> Arc<ImpactBounds> {
        let Some(scorer_key) = scorer_key else {
            return Arc::new(self.compute_bounds(scorer));
        };

        {
            let cached = self.keyed_bounds_guard();
            if let Some((cached_key, bounds)) = cached.as_ref()
                && *cached_key == scorer_key
            {
                return bounds.clone();
            }
        }

        // Compute outside the mutex. Concurrent misses may duplicate this work,
        // but only the short publication/check below holds the shared lock.
        let computed = Arc::new(self.compute_bounds(scorer));
        let mut cached = self.keyed_bounds_guard();
        if let Some((cached_key, bounds)) = cached.as_ref()
            && *cached_key == scorer_key
        {
            return bounds.clone();
        }
        *cached = Some((scorer_key, computed.clone()));
        computed
    }

    fn compute_bounds<S: Scorer + ?Sized>(&self, scorer: &S) -> ImpactBounds {
        let per_entry = (0..self.entries.len())
            .map(|entry_idx| {
                if self.entries.is_null(entry_idx) {
                    return f32::INFINITY;
                }
                let bytes = self.entries.value(entry_idx);
                let mut max_doc_weight = 0.0_f32;
                match for_each_entry_pair(bytes, |freq, doc_len| {
                    max_doc_weight = max_doc_weight.max(scorer.doc_weight(freq, doc_len));
                }) {
                    Ok(()) => max_doc_weight,
                    Err(_) => f32::INFINITY,
                }
            })
            .collect::<Box<[f32]>>();
        // The level1 entries cover every block, so their max is the list-wide
        // max doc weight; zero-entry lists fall back to the empty level0 slab.
        let global = if per_entry.len() > self.level0_len {
            per_entry[self.level0_len..]
                .iter()
                .copied()
                .fold(0.0_f32, f32::max)
        } else {
            per_entry.iter().copied().fold(0.0_f32, f32::max)
        };
        ImpactBounds { per_entry, global }
    }

    /// List-wide max doc weight, from the scorer-specific cached bounds. The
    /// tightest valid global score bound is `query_weight * this`, matching what
    /// the non-impact format stores as `max_score` at build time.
    pub fn global_max_doc_weight_cached<S: Scorer + ?Sized>(
        &self,
        scorer: &S,
        cache: &mut ImpactScoreCache,
    ) -> f32 {
        cache.bounds(self, scorer).global
    }

    /// Cached per-block max doc weights (level0 entries only), for bulk skip
    /// scans over dead ranges without per-block window bookkeeping.
    pub(crate) fn level0_doc_weight_bounds_cached<'a, S: Scorer + ?Sized>(
        &self,
        scorer: &S,
        cache: &'a mut ImpactScoreCache,
    ) -> &'a [f32] {
        &cache.bounds(self, scorer).per_entry[..self.level0_len]
    }

    pub fn entries(&self) -> &LargeBinaryArray {
        &self.entries
    }

    /// Conservative heap charge for query-independent derived state and one
    /// shared keyed-bound slab, whether or not that slab has been initialized
    /// yet. The Arrow impact entries are owned by the enclosing batch and are
    /// deliberately excluded so packed-group cache accounting counts them once.
    pub(crate) fn derived_cache_bytes(&self) -> usize {
        Self::derived_cache_bytes_for_entries(self.entries.len())
    }

    pub(crate) fn derived_cache_bytes_for_entries(entry_count: usize) -> usize {
        entry_count * size_of::<u32>()
            + size_of::<Mutex<LastKeyedImpactBounds>>()
            + size_of::<ImpactBounds>()
            + entry_count * size_of::<f32>()
    }

    #[cfg(test)]
    pub(crate) fn shares_derived_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entry_doc_up_tos, &other.entry_doc_up_tos)
            && Arc::ptr_eq(&self.last_keyed_bounds, &other.last_keyed_bounds)
    }

    #[cfg(test)]
    pub fn level0_len(&self) -> usize {
        self.level0_len
    }

    #[cfg(test)]
    pub fn level1_len(&self) -> usize {
        level1_len(self.level0_len)
    }

    pub(crate) fn level1_doc_up_to(&self, group_idx: usize) -> Option<u32> {
        if group_idx >= level1_len(self.level0_len) {
            return None;
        }
        match self.entry_doc_up_tos[self.level0_len + group_idx] {
            u32::MAX => None,
            doc_up_to => Some(doc_up_to),
        }
    }

    /// Last doc id covered by the level0 entry of `block_idx`, or `None` when
    /// the entry is missing or malformed.
    pub(crate) fn level0_doc_up_to(&self, block_idx: usize) -> Option<u32> {
        if block_idx >= self.level0_len {
            return None;
        }
        match self.entry_doc_up_tos[block_idx] {
            u32::MAX => None,
            doc_up_to => Some(doc_up_to),
        }
    }

    pub fn level0_score_cached<S: Scorer + ?Sized>(
        &self,
        block_idx: usize,
        query_weight: f32,
        scorer: &S,
        cache: &mut ImpactScoreCache,
    ) -> f32 {
        if block_idx >= self.level0_len {
            return 0.0;
        }
        cache.entry_score(self, block_idx, query_weight, scorer)
    }

    /// Max score of the docs covered by the level1 entry of `group_idx`,
    /// answered from the scorer-specific cached bounds slab.
    pub(crate) fn level1_score_cached<S: Scorer + ?Sized>(
        &self,
        group_idx: usize,
        query_weight: f32,
        scorer: &S,
        cache: &mut ImpactScoreCache,
    ) -> f32 {
        if group_idx >= level1_len(self.level0_len) {
            return 0.0;
        }
        cache.entry_score(self, self.level0_len + group_idx, query_weight, scorer)
    }

    #[cfg(test)]
    pub fn max_score_up_to_cached<S>(
        &self,
        start_block_idx: usize,
        up_to: u64,
        query_weight: f32,
        scorer: &S,
        cache: &mut ImpactScoreCache,
    ) -> ImpactScore
    where
        S: Scorer + ?Sized,
    {
        let mut block_idx = start_block_idx;
        let mut max_score = 0.0_f32;
        let mut entries_scanned = 0usize;

        while block_idx < self.level0_len {
            let group_idx = block_idx / IMPACT_LEVEL1_BLOCKS;
            let group_start = group_idx * IMPACT_LEVEL1_BLOCKS;
            let group_end = ((group_idx + 1) * IMPACT_LEVEL1_BLOCKS).min(self.level0_len);
            if block_idx == group_start {
                let level1_entry_idx = self.level0_len + group_idx;
                match self.entry_doc_up_tos[level1_entry_idx] {
                    u32::MAX => {
                        return ImpactScore {
                            score: f32::INFINITY,
                            entries_scanned: entries_scanned + 1,
                        };
                    }
                    doc_up_to if u64::from(doc_up_to) <= up_to => {
                        max_score = max_score.max(cache.entry_score(
                            self,
                            level1_entry_idx,
                            query_weight,
                            scorer,
                        ));
                        entries_scanned += 1;
                        block_idx = group_end;
                        continue;
                    }
                    _ => {}
                }
            }

            max_score = max_score.max(cache.entry_score(self, block_idx, query_weight, scorer));
            entries_scanned += 1;
            match self.entry_doc_up_tos[block_idx] {
                u32::MAX => {
                    return ImpactScore {
                        score: f32::INFINITY,
                        entries_scanned,
                    };
                }
                doc_up_to if u64::from(doc_up_to) >= up_to => break,
                _ => {}
            }
            block_idx += 1;
        }

        ImpactScore {
            score: max_score,
            entries_scanned,
        }
    }
}

pub struct ImpactSkipDataBuilder {
    entries: LargeBinaryBuilder,
    level0_len: usize,
    level1_entries: Vec<Vec<u8>>,
    level1_docs: Vec<(u32, u32, u32)>,
}

impl ImpactSkipDataBuilder {
    pub fn with_capacity(level0_blocks: usize, block_size: usize) -> Self {
        Self {
            entries: LargeBinaryBuilder::with_capacity(
                level0_blocks + level1_len(level0_blocks),
                0,
            ),
            level0_len: 0,
            level1_entries: Vec::with_capacity(level1_len(level0_blocks)),
            level1_docs: Vec::with_capacity(IMPACT_LEVEL1_BLOCKS * block_size),
        }
    }

    pub fn append_block(&mut self, docs: &[(u32, u32, u32)]) -> Result<()> {
        let bytes = encode_impact_entry(docs)?;
        self.entries.append_value(bytes.as_slice());
        self.level0_len += 1;
        self.level1_docs.extend_from_slice(docs);
        if self.level0_len.is_multiple_of(IMPACT_LEVEL1_BLOCKS) {
            self.flush_level1()?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ImpactSkipData> {
        if !self.level1_docs.is_empty() {
            self.flush_level1()?;
        }
        for entry in self.level1_entries {
            self.entries.append_value(entry.as_slice());
        }
        ImpactSkipData::new(self.entries.finish(), self.level0_len)
    }

    fn flush_level1(&mut self) -> Result<()> {
        let bytes = encode_impact_entry(self.level1_docs.as_slice())?;
        self.level1_entries.push(bytes);
        self.level1_docs.clear();
        Ok(())
    }
}

#[cfg(test)]
pub fn build_impact_skip_data(blocks: &[Vec<(u32, u32, u32)>]) -> Result<ImpactSkipData> {
    let block_size = blocks.iter().map(Vec::len).max().unwrap_or(0).max(1);
    let mut builder = ImpactSkipDataBuilder::with_capacity(blocks.len(), block_size);
    for block in blocks {
        builder.append_block(block)?;
    }
    builder.finish()
}

fn encode_impact_entry(docs: &[(u32, u32, u32)]) -> Result<Vec<u8>> {
    if docs.is_empty() {
        return Err(Error::index(
            "cannot encode an empty impact entry".to_owned(),
        ));
    }
    let doc_up_to = docs
        .last()
        .map(|(doc_id, _, _)| *doc_id)
        .expect("non-empty impact entry was validated above");
    let frontier = quantized_impact_frontier(docs);
    let pair_count = u32::try_from(frontier.len()).map_err(|_| {
        Error::index("impact frontier too large to encode as u32 pair count".to_string())
    })?;
    let mut bytes = Vec::with_capacity(5 + frontier.len() * 2);
    super::encoding::encode_varint_u32(&mut bytes, doc_up_to);
    super::encoding::encode_varint_u32(&mut bytes, pair_count);
    let mut previous_freq = 0u32;
    let mut previous_norm = 0u8;
    for (pair_idx, (freq, norm)) in frontier.into_iter().enumerate() {
        let freq_delta_minus_one = freq
            .checked_sub(previous_freq)
            .and_then(|delta| delta.checked_sub(1))
            .ok_or_else(|| {
                Error::index(format!(
                    "impact frequencies must be positive and strictly increasing: previous={previous_freq}, current={freq}"
                ))
            })?;
        let norm_delta = norm.checked_sub(previous_norm).ok_or_else(|| {
            Error::index(format!(
                "impact norms must be non-decreasing: previous={previous_norm}, current={norm}"
            ))
        })?;
        if pair_idx > 0 && norm_delta == 0 {
            return Err(Error::index(format!(
                "impact norms must be strictly increasing after quantization: norm={norm}"
            )));
        }

        let has_explicit_norm_delta = norm_delta != 1;
        let packed_freq_delta =
            (u64::from(freq_delta_minus_one) << 1) | u64::from(has_explicit_norm_delta);
        encode_varint_u64(&mut bytes, packed_freq_delta);
        if has_explicit_norm_delta {
            bytes.push(norm_delta);
        }
        previous_freq = freq;
        previous_norm = norm;
    }
    Ok(bytes)
}

fn decode_entry_doc_up_to(bytes: &[u8]) -> Result<u32> {
    let mut offset = 0usize;
    let doc_up_to = super::encoding::decode_varint_u32(bytes, &mut offset)?;
    // Level-1 doc ids drive whole-group skips, so only publish a doc id after
    // validating the complete entry. A truncated entry may still have a valid
    // first varint.
    for_each_entry_pair(bytes, |_, _| {})?;
    Ok(doc_up_to)
}

fn decode_level0_entry_doc_up_to(bytes: &[u8]) -> Result<u32> {
    // A malformed level0 frontier bakes an INFINITY score before this marker
    // can terminate a range scan, so avoid parsing every frontier twice on the
    // query-load path. Level1 entries are fully validated.
    let mut offset = 0usize;
    super::encoding::decode_varint_u32(bytes, &mut offset)
}

/// Walk an entry's (freq, doc_len) frontier pairs, validating the layout.
fn for_each_entry_pair(bytes: &[u8], mut visit: impl FnMut(u32, u32)) -> Result<()> {
    let mut offset = 0usize;
    let _doc_up_to = super::encoding::decode_varint_u32(bytes, &mut offset)?;
    let pair_count = super::encoding::decode_varint_u32(bytes, &mut offset)?;
    if pair_count == 0 {
        return Err(Error::index(
            "impact entry must contain at least one frontier pair".to_owned(),
        ));
    }

    let mut previous_freq = 0u32;
    let mut previous_norm = 0u16;
    for pair_idx in 0..pair_count {
        let packed_freq_delta = decode_varint_u64(bytes, &mut offset)?;
        let freq_delta_minus_one = u32::try_from(packed_freq_delta >> 1)
            .map_err(|_| Error::index("impact freq delta exceeds u32".to_owned()))?;
        let freq_delta = freq_delta_minus_one
            .checked_add(1)
            .ok_or_else(|| Error::index("impact freq delta overflow".to_owned()))?;
        let freq = previous_freq
            .checked_add(freq_delta)
            .ok_or_else(|| Error::index("impact frequency overflow".to_owned()))?;

        let has_explicit_norm_delta = packed_freq_delta & 1 != 0;
        let norm_delta = if has_explicit_norm_delta {
            let norm_delta = bytes.get(offset).copied().ok_or_else(|| {
                Error::index("unexpected EOF while decoding impact norm delta".to_owned())
            })?;
            offset += 1;
            norm_delta
        } else {
            1
        };
        if pair_idx > 0 && norm_delta == 0 {
            return Err(Error::index(
                "impact norms must be strictly increasing".to_owned(),
            ));
        }

        let norm = previous_norm
            .checked_add(u16::from(norm_delta))
            .filter(|norm| *norm <= u16::from(u8::MAX))
            .ok_or_else(|| Error::index("impact norm delta overflow".to_owned()))?;
        let norm = norm as u8;
        visit(freq, super::index::dequantize_doc_length(norm));
        previous_freq = freq;
        previous_norm = u16::from(norm);
    }
    if offset != bytes.len() {
        return Err(Error::index(format!(
            "impact entry has {} trailing bytes",
            bytes.len() - offset
        )));
    }
    Ok(())
}

#[inline]
fn encode_varint_u64(dst: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

#[inline]
fn decode_varint_u64(src: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    while *offset < src.len() {
        let byte = src[*offset];
        *offset += 1;
        if shift == 63 && byte & 0xFE != 0 {
            return Err(Error::index(
                "invalid u64 varint in impact entry".to_owned(),
            ));
        }
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err(Error::index(
                "invalid u64 varint in impact entry".to_owned(),
            ));
        }
    }
    Err(Error::index(
        "unexpected EOF while decoding impact entry".to_owned(),
    ))
}

fn quantized_impact_frontier(docs: &[(u32, u32, u32)]) -> Vec<(u32, u8)> {
    let raw_frontier = impact_frontier(docs);
    let mut frontier: Vec<(u32, u8)> = Vec::with_capacity(raw_frontier.len());
    for (freq, doc_len) in raw_frontier {
        let norm = super::index::quantize_doc_length(doc_len);
        match frontier.last_mut() {
            Some((last_freq, last_norm)) if *last_norm == norm => {
                // At the same quantized norm, the larger frequency dominates.
                *last_freq = freq;
            }
            Some((_, last_norm)) => {
                debug_assert!(
                    *last_norm < norm,
                    "raw impact frontier document lengths must be increasing"
                );
                frontier.push((freq, norm));
            }
            None => frontier.push((freq, norm)),
        }
    }
    frontier
}

fn impact_frontier(docs: &[(u32, u32, u32)]) -> Vec<(u32, u32)> {
    let max_freq = docs.iter().map(|(_, freq, _)| *freq).max().unwrap_or(0) as usize;
    if max_freq <= SMALL_FRONTIER_FREQ_LIMIT {
        return impact_frontier_small_freq(docs, max_freq);
    }

    impact_frontier_sparse_freq(docs)
}

fn impact_frontier_small_freq(docs: &[(u32, u32, u32)], max_freq: usize) -> Vec<(u32, u32)> {
    let mut min_doc_len_by_freq = [u32::MAX; SMALL_FRONTIER_FREQ_LIMIT + 1];
    for (_, freq, doc_len) in docs {
        min_doc_len_by_freq[*freq as usize] = min_doc_len_by_freq[*freq as usize].min(*doc_len);
    }

    let min_doc_lens = min_doc_len_by_freq[..=max_freq]
        .iter()
        .enumerate()
        .filter_map(|(freq, doc_len)| (*doc_len != u32::MAX).then_some((freq as u32, *doc_len)))
        .collect::<Vec<_>>();
    frontier_from_min_doc_lens(min_doc_lens)
}

fn impact_frontier_sparse_freq(docs: &[(u32, u32, u32)]) -> Vec<(u32, u32)> {
    let mut pairs = docs
        .iter()
        .map(|(_, freq, doc_len)| (*freq, *doc_len))
        .collect::<Vec<_>>();
    pairs.sort_unstable_by_key(|(freq, _)| *freq);

    let mut min_doc_lens: Vec<(u32, u32)> = Vec::with_capacity(pairs.len());
    for (freq, doc_len) in pairs {
        match min_doc_lens.last_mut() {
            Some((last_freq, last_doc_len)) if *last_freq == freq => {
                *last_doc_len = (*last_doc_len).min(doc_len);
            }
            _ => min_doc_lens.push((freq, doc_len)),
        }
    }

    frontier_from_min_doc_lens(min_doc_lens)
}

fn frontier_from_min_doc_lens(min_doc_lens: Vec<(u32, u32)>) -> Vec<(u32, u32)> {
    let mut best_doc_len = u32::MAX;
    let mut frontier = Vec::with_capacity(min_doc_lens.len());
    for (freq, doc_len) in min_doc_lens.into_iter().rev() {
        if doc_len < best_doc_len {
            frontier.push((freq, doc_len));
            best_doc_len = doc_len;
        }
    }
    frontier.reverse();
    frontier
}

fn level1_len(level0_len: usize) -> usize {
    level0_len.div_ceil(IMPACT_LEVEL1_BLOCKS)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};

    use super::*;
    use crate::scalar::inverted::scorer::{MemBM25Scorer, Scorer};

    struct KeyedCountingScorer {
        key: u64,
        calls: Arc<AtomicUsize>,
    }

    impl Scorer for KeyedCountingScorer {
        fn query_weight(&self, _token: &str) -> f32 {
            1.0
        }

        fn doc_weight(&self, freq: u32, doc_tokens: u32) -> f32 {
            self.calls.fetch_add(1, Ordering::Relaxed);
            freq as f32 / doc_tokens as f32
        }

        fn doc_weight_cache_key(&self) -> Option<u64> {
            Some(self.key)
        }
    }

    #[test]
    fn impact_entry_frontier_drops_dominated_pairs() {
        let docs = vec![(0, 1, 10), (1, 1, 8), (2, 2, 9), (3, 3, 20)];
        assert_eq!(impact_frontier(&docs), vec![(1, 8), (2, 9), (3, 20)]);
    }

    #[test]
    fn impact_entry_frontier_handles_sparse_large_frequencies() {
        let docs = vec![
            (0, 1, 100),
            (1, 1, 80),
            (2, 512, 90),
            (3, 1_000, 120),
            (4, 1_000, 110),
        ];
        assert_eq!(
            impact_frontier(&docs),
            vec![(1, 80), (512, 90), (1_000, 110)]
        );
    }

    #[test]
    fn quantized_impact_frontier_drops_equal_norms() {
        let docs = vec![(0, 1, 16), (1, 2, 17), (2, 3, 24)];
        assert_eq!(
            quantized_impact_frontier(&docs),
            vec![
                (2, super::super::index::quantize_doc_length(17)),
                (3, super::super::index::quantize_doc_length(24)),
            ]
        );
    }

    #[test]
    fn impact_max_score_can_use_level1_entry() {
        let blocks = (0..40)
            .map(|block| vec![(block as u32, 1 + block as u32 % 3, 10)])
            .collect::<Vec<_>>();
        let impacts = build_impact_skip_data(&blocks).unwrap();
        assert_eq!(impacts.level0_len(), 40);
        assert_eq!(impacts.level1_len(), 2);
        let scorer = MemBM25Scorer::new(400, 40, HashMap::from([(String::from("token"), 40usize)]));
        let mut cache = ImpactScoreCache::default();
        let score = impacts.max_score_up_to_cached(0, 31, 1.0, &scorer, &mut cache);
        assert!(score.entries_scanned < IMPACT_LEVEL1_BLOCKS);
        assert!(score.score > 0.0);
    }

    #[test]
    fn impact_level1_doc_up_to_reports_full_and_partial_groups() {
        let blocks = (0..40)
            .map(|block| vec![(block as u32, 1, 10)])
            .collect::<Vec<_>>();
        let impacts = build_impact_skip_data(&blocks).unwrap();

        assert_eq!(
            impacts.level1_doc_up_to(0),
            Some((IMPACT_LEVEL1_BLOCKS - 1) as u32)
        );
        assert_eq!(impacts.level1_doc_up_to(1), Some(39));
        assert_eq!(impacts.level1_doc_up_to(2), None);
    }

    #[test]
    fn impact_level1_doc_up_to_returns_none_for_malformed_entry() {
        let level0 = encode_impact_entry(&[(0, 1, 10)]).unwrap();
        let malformed_level1 = vec![1, 2, 3];
        let entries = LargeBinaryArray::from_opt_vec(vec![
            Some(level0.as_slice()),
            Some(malformed_level1.as_slice()),
        ]);
        let impacts = ImpactSkipData::new(entries, 1).unwrap();

        assert_eq!(impacts.level1_doc_up_to(0), None);
    }

    #[test]
    fn impact_level1_doc_up_to_validates_complete_entry() {
        let level0 = encode_impact_entry(&[(0, 1, 10)]).unwrap();
        // A complete first varint is not enough: the pair count and frontier
        // are required before this doc id can safely drive a group skip.
        let truncated_level1 = [31_u8];
        let entries = LargeBinaryArray::from_opt_vec(vec![
            Some(level0.as_slice()),
            Some(truncated_level1.as_slice()),
        ]);
        let impacts = ImpactSkipData::new(entries, 1).unwrap();

        assert_eq!(impacts.level1_doc_up_to(0), None);
    }

    #[test]
    fn empty_impact_frontiers_are_malformed_bounds() {
        let scorer = MemBM25Scorer::new(10, 1, HashMap::new());
        let level0 = encode_impact_entry(&[(0, 1, 10)]).unwrap();
        let empty_level1 = [0, 0];
        let entries = LargeBinaryArray::from_opt_vec(vec![
            Some(level0.as_slice()),
            Some(empty_level1.as_slice()),
        ]);
        let impacts = ImpactSkipData::new(entries, 1).unwrap();
        let mut cache = ImpactScoreCache::default();

        assert_eq!(impacts.level1_doc_up_to(0), None);
        assert!(
            impacts
                .global_max_doc_weight_cached(&scorer, &mut cache)
                .is_infinite()
        );
        assert!(
            ImpactSkipDataBuilder::with_capacity(1, 128)
                .append_block(&[])
                .is_err()
        );
    }

    #[test]
    fn null_impact_entry_is_an_infinite_bound_even_with_hidden_bytes() {
        let level0 = encode_impact_entry(&[(0, 1, 10)]).unwrap();
        let level1 = encode_impact_entry(&[(0, 2, 8)]).unwrap();
        let mut values = level0.clone();
        values.extend_from_slice(&level1);
        let entries = LargeBinaryArray::new(
            OffsetBuffer::new(ScalarBuffer::from(vec![
                0_i64,
                level0.len() as i64,
                values.len() as i64,
            ])),
            Buffer::from_vec(values),
            Some(NullBuffer::from(vec![true, false])),
        );
        let impacts = ImpactSkipData::new(entries, 1).unwrap();
        let scorer = MemBM25Scorer::new(10, 1, HashMap::new());
        let mut cache = ImpactScoreCache::default();

        assert_eq!(impacts.level1_doc_up_to(0), None);
        assert!(
            impacts
                .global_max_doc_weight_cached(&scorer, &mut cache)
                .is_infinite()
        );
    }

    #[test]
    fn impact_bounds_follow_changed_bm25_average_doc_length() {
        let impacts = build_impact_skip_data(&[vec![(0, 1, 100)]]).unwrap();
        let low_avgdl = MemBM25Scorer::new(1, 1, HashMap::new());
        let high_avgdl = MemBM25Scorer::new(100, 1, HashMap::new());
        let mut low_cache = ImpactScoreCache::default();
        let mut high_cache = ImpactScoreCache::default();

        let low_bound = impacts.global_max_doc_weight_cached(&low_avgdl, &mut low_cache);
        let high_bound = impacts.global_max_doc_weight_cached(&high_avgdl, &mut high_cache);
        let quantized_doc_length = super::super::index::dequantize_doc_length(
            super::super::index::quantize_doc_length(100),
        );

        assert!((low_bound - low_avgdl.doc_weight(1, quantized_doc_length)).abs() < 1e-6);
        assert!((high_bound - high_avgdl.doc_weight(1, quantized_doc_length)).abs() < 1e-6);
        assert!(low_bound >= low_avgdl.doc_weight(1, 100));
        assert!(high_bound >= high_avgdl.doc_weight(1, 100));
        assert!(
            high_bound > low_bound,
            "larger avgdl must recompute a larger bound: low={low_bound}, high={high_bound}"
        );
    }

    #[test]
    fn impact_bounds_reuse_same_scorer_key_across_queries() {
        let impacts = build_impact_skip_data(&[vec![(0, 2, 10)]]).unwrap();
        let cloned = impacts.clone();
        assert!(impacts.shares_derived_state_with(&cloned));
        let calls = Arc::new(AtomicUsize::new(0));
        let scorer = KeyedCountingScorer {
            key: 7,
            calls: calls.clone(),
        };
        let mut first_query = ImpactScoreCache::default();
        let mut second_query = ImpactScoreCache::default();

        let first = impacts.global_max_doc_weight_cached(&scorer, &mut first_query);
        let baked_calls = calls.load(Ordering::Relaxed);
        assert!(baked_calls > 0);
        let second = cloned.global_max_doc_weight_cached(&scorer, &mut second_query);

        assert_eq!(second, first);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            baked_calls,
            "the same keyed bounds should be shared across query caches"
        );
    }

    #[test]
    fn malformed_unscanned_entry_does_not_poison_range_score() {
        let level0_0 = encode_impact_entry(&[(0, 1, 10)]).unwrap();
        let malformed_level0_1 = vec![1, 2, 3];
        let level1 = encode_impact_entry(&[(0, 1, 10), (1, 1, 10)]).unwrap();
        let entries = LargeBinaryArray::from_opt_vec(vec![
            Some(level0_0.as_slice()),
            Some(malformed_level0_1.as_slice()),
            Some(level1.as_slice()),
        ]);
        let impacts = ImpactSkipData::new(entries, 2).unwrap();
        let scorer = MemBM25Scorer::new(10, 10, HashMap::from([(String::from("token"), 2usize)]));
        let mut cache = ImpactScoreCache::default();

        let score = impacts.max_score_up_to_cached(0, 0, 1.0, &scorer, &mut cache);
        assert!(score.score.is_finite());
        assert_eq!(score.entries_scanned, 1);

        assert_eq!(
            impacts.level0_score_cached(1, 1.0, &scorer, &mut cache),
            f32::INFINITY
        );
    }

    #[test]
    fn impact_entries_store_quantized_norm_deltas() {
        let docs = vec![(7, 1, 1), (9, 2, 2), (12, 3, 5)];
        let encoded = encode_impact_entry(&docs).unwrap();

        // doc_up_to=12, pair_count=3, two implicit +1 norm deltas, then an
        // explicit +3 norm delta folded behind the frequency varint's low bit.
        assert_eq!(encoded, vec![12, 3, 0, 0, 1, 3]);

        let mut pairs = Vec::new();
        for_each_entry_pair(&encoded, |freq, doc_len| pairs.push((freq, doc_len))).unwrap();
        assert_eq!(pairs, vec![(1, 1), (2, 2), (3, 5)]);
    }

    #[test]
    fn malformed_norm_deltas_are_rejected() {
        // doc_up_to=0, pair_count=1, and the pair flag promises a norm byte
        // that is not present.
        let truncated = [0, 1, 1];
        let error = for_each_entry_pair(&truncated, |_, _| {}).unwrap_err();
        assert!(matches!(&error, Error::Index { .. }));
        assert!(error.to_string().contains("impact norm delta"));

        // The first pair reaches norm 255, so an implicit +1 on the second
        // pair must fail instead of wrapping back to zero.
        let overflowing = [0, 2, 1, 255, 0];
        let error = for_each_entry_pair(&overflowing, |_, _| {}).unwrap_err();
        assert!(matches!(&error, Error::Index { .. }));
        assert!(error.to_string().contains("impact norm delta overflow"));
    }

    #[test]
    fn impact_entries_roundtrip_quantized_frontier() {
        let docs = vec![(3, 1, 100), (9, 2, 40), (200, 7, 80), (4095, 130, 900)];
        let encoded = encode_impact_entry(&docs).unwrap();
        assert_eq!(decode_entry_doc_up_to(&encoded).unwrap(), 4095);
        let mut decoded_pairs = Vec::new();
        for_each_entry_pair(&encoded, |freq, doc_len| {
            decoded_pairs.push((freq, doc_len))
        })
        .unwrap();
        let expected_pairs = quantized_impact_frontier(&docs)
            .into_iter()
            .map(|(freq, norm)| (freq, super::super::index::dequantize_doc_length(norm)))
            .collect::<Vec<_>>();
        assert_eq!(decoded_pairs, expected_pairs);
        assert!(!decoded_pairs.is_empty());

        // A 256-doc-block skip data goes through the shared codec end to end.
        let blocks: Vec<Vec<(u32, u32, u32)>> = (0..3)
            .map(|b| (0..256).map(|i| (b * 256 + i, 1 + i % 5, 10)).collect())
            .collect();
        let impacts = build_impact_skip_data(&blocks).unwrap();
        assert_eq!(impacts.level1_doc_up_to(0), Some(767));
        let scorer = MemBM25Scorer::new(400, 768, HashMap::from([(String::from("t"), 768usize)]));
        let mut cache = ImpactScoreCache::default();
        assert!(
            impacts
                .level0_score_cached(0, 1.0, &scorer, &mut cache)
                .is_finite()
        );
        let level1 = impacts.max_score_up_to_cached(0, 767, 1.0, &scorer, &mut cache);
        assert!(level1.score.is_finite() && level1.score > 0.0);
    }

    #[test]
    fn posting_block_sizes_use_identical_impact_encoding() {
        let docs = vec![(3, 1, 100), (9, 2, 40), (200, 7, 80)];
        let mut v2_builder = ImpactSkipDataBuilder::with_capacity(1, 128);
        v2_builder.append_block(&docs).unwrap();
        let v2 = v2_builder.finish().unwrap();
        let mut v3_builder = ImpactSkipDataBuilder::with_capacity(1, 256);
        v3_builder.append_block(&docs).unwrap();
        let v3 = v3_builder.finish().unwrap();

        assert_eq!(v2.entries(), v3.entries());
    }

    #[test]
    fn impact_upper_bound_covers_real_scores() {
        let blocks = vec![
            vec![(0, 1, 100), (3, 2, 40), (7, 4, 80)],
            vec![(9, 3, 15), (10, 1, 5), (12, 5, 30)],
            vec![(16, 2, 10), (18, 6, 70), (21, 3, 12)],
            vec![(24, 1, 4), (28, 7, 100), (30, 2, 8)],
        ];
        let impacts = build_impact_skip_data(&blocks).unwrap();
        let scorer = MemBM25Scorer::new(474, 31, HashMap::from([(String::from("token"), 4usize)]));
        let query_weight = scorer.query_weight("token");
        let mut cache = ImpactScoreCache::default();

        for start_block_idx in 0..blocks.len() {
            let up_to = blocks
                .iter()
                .skip(start_block_idx)
                .take(2)
                .flatten()
                .map(|(doc_id, _, _)| *doc_id)
                .max()
                .unwrap();
            let upper_bound = impacts.max_score_up_to_cached(
                start_block_idx,
                u64::from(up_to),
                query_weight,
                &scorer,
                &mut cache,
            );
            let exact_max = blocks
                .iter()
                .skip(start_block_idx)
                .flatten()
                .take_while(|(doc_id, _, _)| *doc_id <= up_to)
                .map(|(_, freq, doc_len)| query_weight * scorer.doc_weight(*freq, *doc_len))
                .fold(0.0_f32, f32::max);
            assert!(
                upper_bound.score + 1e-6 >= exact_max,
                "upper bound {} should cover exact max {} from block {} up to doc {}",
                upper_bound.score,
                exact_max,
                start_block_idx,
                up_to
            );
        }
    }
}
