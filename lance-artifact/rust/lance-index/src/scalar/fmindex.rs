// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! FM-Index for exact substring search (following the Infini-gram Mini paper)
//!
//! The FM-Index is a compressed full-text index based on the Burrows-Wheeler Transform (BWT).
//! It supports exact substring matching via backward search and returns exact row ids.
//!
//! Architecture (matching the paper):
//!   - Huffman-shaped Wavelet Tree over BWT for entropy-compressed rank queries (~0.26N)
//!   - Sampled Suffix Array every D-th position for locate (~N/D × 8 bytes)
//!   - doc_start_positions for mapping text positions to documents (tiny)
//!   - No doc_array — documents are resolved via SA sampling + LF-mapping + binary search
//!
//! Total index size: ~0.44N (matching paper's claim)
//!
//! Storage layout (v10 - blocked, partitioned):
//!   - BWT wavelet tree bitvectors in blocks of BLOCK_WORDS (32KB each)
//!   - SA samples stored as packed binary blocks after wavelet blocks
//!   - Row IDs and doc_start_positions in metadata
//!   - File metadata: c_table, huffman_codes, tree topology

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use lance_core::cache::LanceCache;
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::{Error, ROW_ADDR, Result};
use roaring::RoaringBitmap;

use crate::metrics::MetricsCollector;
use crate::pb;
use crate::scalar::expression::{ScalarQueryParser, TextQueryParser};
use crate::scalar::registry::{
    BasicTrainer, DefaultTrainingRequest, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering,
    TrainingRequest, VALUE_COLUMN_NAME,
};
use crate::scalar::{
    AnyQuery, BuiltinIndexType, CreatedIndex, IndexFile, IndexStore, OldIndexDataFilter,
    RowIdRemapper, ScalarIndex, ScalarIndexParams, SearchResult, TextQuery, UpdateCriteria,
};
use crate::{Index, IndexType};

const FMINDEX_INDEX_VERSION: u32 = 10;
const BLOCK_WORDS: usize = 4096;
const PARTITION_SIZE: usize = 10_000;
const DEFAULT_PARTITION_SIZE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_DEMAND_PAGE_TARGET_BYTES: usize = 512 * 1024;
const DEFAULT_PREWARM_CHUNK_TARGET_BYTES: usize = 8 * 1024 * 1024;
const FMINDEX_PARTITION_FINGERPRINT_KEY: &str = "partition_fingerprint";
const SENTINEL_BYTE: u8 = 0xFF;

/// SA sampling rate. Store every D-th SA entry. Locate walks at most D LF steps.
const SA_SAMPLE_RATE: usize = 32;

static LANCE_FMINDEX_NUM_WORKERS: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("LANCE_FMINDEX_NUM_WORKERS")
        .unwrap_or_else(|_| get_num_compute_intensive_cpus().to_string())
        .parse()
        .expect("failed to parse LANCE_FMINDEX_NUM_WORKERS")
});
static LANCE_FMINDEX_PARTITION_ROWS: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("LANCE_FMINDEX_PARTITION_ROWS")
        .unwrap_or_else(|_| PARTITION_SIZE.to_string())
        .parse()
        .expect("failed to parse LANCE_FMINDEX_PARTITION_ROWS")
});
static LANCE_FMINDEX_PARTITION_BYTES: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::env::var("LANCE_FMINDEX_PARTITION_BYTES")
        .unwrap_or_else(|_| DEFAULT_PARTITION_SIZE_BYTES.to_string())
        .parse()
        .expect("failed to parse LANCE_FMINDEX_PARTITION_BYTES")
});
static LANCE_FMINDEX_WRITE_QUEUE_SIZE: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| {
        std::env::var("LANCE_FMINDEX_WRITE_QUEUE_SIZE")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .expect("failed to parse LANCE_FMINDEX_WRITE_QUEUE_SIZE")
    });
static LANCE_FMINDEX_RESUME_EXISTING_PARTITIONS: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| {
        std::env::var("LANCE_FMINDEX_RESUME_EXISTING_PARTITIONS")
            .map(|value| {
                matches!(
                    value.as_str(),
                    "1" | "true" | "TRUE" | "True" | "yes" | "YES"
                )
            })
            .unwrap_or(false)
    });
static LANCE_FMINDEX_PREWARM_CHUNK_BYTES: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| {
        std::env::var("LANCE_FMINDEX_PREWARM_CHUNK_BYTES")
            .unwrap_or_else(|_| DEFAULT_PREWARM_CHUNK_TARGET_BYTES.to_string())
            .parse()
            .expect("failed to parse LANCE_FMINDEX_PREWARM_CHUNK_BYTES")
    });
static LANCE_FMINDEX_DEMAND_PAGE_BYTES: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| {
        std::env::var("LANCE_FMINDEX_DEMAND_PAGE_BYTES")
            .unwrap_or_else(|_| DEFAULT_DEMAND_PAGE_TARGET_BYTES.to_string())
            .parse()
            .expect("failed to parse LANCE_FMINDEX_DEMAND_PAGE_BYTES")
    });
static LANCE_FMINDEX_PREWARM_CHUNK_CONCURRENCY: std::sync::LazyLock<usize> =
    std::sync::LazyLock::new(|| {
        std::env::var("LANCE_FMINDEX_PREWARM_CHUNK_CONCURRENCY")
            .unwrap_or_else(|_| "1".to_string())
            .parse()
            .expect("failed to parse LANCE_FMINDEX_PREWARM_CHUNK_CONCURRENCY")
    });

fn fmindex_partition_path(partition_id: u64) -> String {
    format!("part_{partition_id}_fm.lance")
}

fn fmindex_partition_id_from_path(path: &str) -> Option<u64> {
    path.strip_prefix("part_")
        .and_then(|r| r.strip_suffix("_fm.lance"))
        .and_then(|s| s.parse::<u64>().ok())
}

fn fmindex_num_workers() -> usize {
    (*LANCE_FMINDEX_NUM_WORKERS).max(1)
}

fn fmindex_partition_rows() -> usize {
    (*LANCE_FMINDEX_PARTITION_ROWS).max(1)
}

fn fmindex_partition_bytes() -> usize {
    (*LANCE_FMINDEX_PARTITION_BYTES).max(1)
}

fn fmindex_write_queue_size() -> usize {
    (*LANCE_FMINDEX_WRITE_QUEUE_SIZE).max(1)
}

fn fmindex_resume_existing_partitions() -> bool {
    *LANCE_FMINDEX_RESUME_EXISTING_PARTITIONS
}

fn fmindex_prewarm_chunk_bytes() -> usize {
    (*LANCE_FMINDEX_PREWARM_CHUNK_BYTES).max(BLOCK_WORDS * 8)
}

fn fmindex_demand_page_bytes() -> usize {
    (*LANCE_FMINDEX_DEMAND_PAGE_BYTES).max(BLOCK_WORDS * 8)
}

fn fmindex_demand_page_rows() -> usize {
    (fmindex_demand_page_bytes() / (BLOCK_WORDS * 8)).max(1)
}

fn fmindex_prewarm_chunk_concurrency() -> usize {
    (*LANCE_FMINDEX_PREWARM_CHUNK_CONCURRENCY).max(1)
}

// ── Bitvector with O(1) rank ─────────────────────────────────────────────────

const SUPERBLOCK_BITS: usize = 512;
const WORDS_PER_SUPERBLOCK: usize = SUPERBLOCK_BITS / 64;

#[derive(Debug, Clone)]
struct RankBitVec {
    words: Vec<u64>,
    superblocks: Vec<u32>,
    len: usize,
}

impl RankBitVec {
    fn new(len: usize) -> Self {
        Self {
            words: vec![0u64; len.div_ceil(64)],
            superblocks: Vec::new(),
            len,
        }
    }

    #[inline]
    fn set(&mut self, pos: usize) {
        self.words[pos / 64] |= 1u64 << (pos % 64);
    }

    fn build_rank_index(&mut self) {
        let num_sb = self.words.len().div_ceil(WORDS_PER_SUPERBLOCK) + 1;
        self.superblocks = Vec::with_capacity(num_sb);
        let mut cum = 0u32;
        for (i, chunk) in self.words.chunks(WORDS_PER_SUPERBLOCK).enumerate() {
            self.superblocks.push(if i == 0 { 0 } else { cum });
            for &w in chunk {
                cum += w.count_ones();
            }
        }
        self.superblocks.push(cum);
    }

    fn deep_size(&self) -> usize {
        self.words.len() * 8 + self.superblocks.len() * 4
    }
}

// ── Huffman-shaped Wavelet Tree ──────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct HuffmanCode {
    bits: u32,
    length: u8,
    node_path: Vec<usize>,
}

#[derive(Debug, Clone)]
enum WaveletChild {
    Node(usize),
    Leaf(u8),
}

#[derive(Debug, Clone)]
struct HuffmanWaveletTree {
    nodes: Vec<RankBitVec>,
    codes: [HuffmanCode; 256],
    children: Vec<(WaveletChild, WaveletChild)>,
    len: usize,
}

#[derive(Debug)]
enum HuffNode {
    Leaf(u8),
    Internal { left: Box<Self>, right: Box<Self> },
}

impl PartialEq for HuffNode {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for HuffNode {}
impl PartialOrd for HuffNode {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HuffNode {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl HuffmanWaveletTree {
    fn build(data: &[u8]) -> Self {
        let n = data.len();
        if n == 0 {
            return Self {
                nodes: Vec::new(),
                codes: std::array::from_fn(|_| HuffmanCode::default()),
                children: Vec::new(),
                len: 0,
            };
        }

        let mut freq = [0u64; 256];
        for &b in data {
            freq[b as usize] += 1;
        }

        let mut heap: BinaryHeap<(Reverse<u64>, Reverse<usize>, Box<HuffNode>)> = BinaryHeap::new();
        let mut tie = 0;
        for (v, &f) in freq.iter().enumerate() {
            if f > 0 {
                heap.push((Reverse(f), Reverse(tie), Box::new(HuffNode::Leaf(v as u8))));
                tie += 1;
            }
        }
        if heap.len() == 1 {
            let (f, _, node) = heap.pop().unwrap();
            heap.push((Reverse(0), Reverse(tie), Box::new(HuffNode::Leaf(255))));
            tie += 1;
            heap.push((f, Reverse(tie), node));
            tie += 1;
        }
        while heap.len() > 1 {
            let (Reverse(f1), _, l) = heap.pop().unwrap();
            let (Reverse(f2), _, r) = heap.pop().unwrap();
            heap.push((
                Reverse(f1 + f2),
                Reverse(tie),
                Box::new(HuffNode::Internal { left: l, right: r }),
            ));
            tie += 1;
        }
        let root = heap.pop().unwrap().2;

        let mut codes: [HuffmanCode; 256] = std::array::from_fn(|_| HuffmanCode::default());
        let mut node_count = 0;
        let mut children_map: Vec<(WaveletChild, WaveletChild)> = Vec::new();

        fn assign(
            node: &HuffNode,
            bits: u32,
            len: u8,
            path: &mut Vec<usize>,
            nid: &mut usize,
            codes: &mut [HuffmanCode; 256],
            cm: &mut Vec<(WaveletChild, WaveletChild)>,
        ) -> WaveletChild {
            match node {
                HuffNode::Leaf(b) => {
                    codes[*b as usize] = HuffmanCode {
                        bits,
                        length: len,
                        node_path: path.clone(),
                    };
                    WaveletChild::Leaf(*b)
                }
                HuffNode::Internal { left, right } => {
                    let my = *nid;
                    *nid += 1;
                    path.push(my);
                    cm.push((WaveletChild::Leaf(0), WaveletChild::Leaf(0)));
                    let lc = assign(left, bits << 1, len + 1, path, nid, codes, cm);
                    let rc = assign(right, (bits << 1) | 1, len + 1, path, nid, codes, cm);
                    cm[my] = (lc, rc);
                    path.pop();
                    WaveletChild::Node(my)
                }
            }
        }
        assign(
            &root,
            0,
            0,
            &mut Vec::new(),
            &mut node_count,
            &mut codes,
            &mut children_map,
        );

        let mut node_sizes = vec![0usize; node_count];
        for &b in data {
            for &nid in &codes[b as usize].node_path {
                node_sizes[nid] += 1;
            }
        }
        let mut nodes: Vec<RankBitVec> = node_sizes.iter().map(|&sz| RankBitVec::new(sz)).collect();
        let mut cursors = vec![0usize; node_count];
        for &b in data {
            let code = &codes[b as usize];
            for (level, &nid) in code.node_path.iter().enumerate() {
                if (code.bits >> (code.length - 1 - level as u8)) & 1 == 1 {
                    nodes[nid].set(cursors[nid]);
                }
                cursors[nid] += 1;
            }
        }
        for n in &mut nodes {
            n.build_rank_index();
        }
        Self {
            nodes,
            codes,
            children: children_map,
            len: n,
        }
    }

    fn deep_size(&self) -> usize {
        self.nodes.iter().map(|n| n.deep_size()).sum::<usize>()
            + self
                .codes
                .iter()
                .map(|c| c.node_path.len() * 8)
                .sum::<usize>()
            + self.children.len() * 24
    }
}

// ── Suffix Array ─────────────────────────────────────────────────────────────

fn build_suffix_array(text: &[u8]) -> Vec<usize> {
    let n = text.len();
    if n == 0 {
        return Vec::new();
    }
    if n > i32::MAX as usize {
        let mut sa = vec![0i64; n];
        assert_eq!(libsais_rs::libsais64(text, &mut sa, 0, None), 0);
        sa.iter().map(|&x| x as usize).collect()
    } else {
        let mut sa = vec![0i32; n];
        assert_eq!(libsais_rs::libsais(text, &mut sa, 0, None), 0);
        sa.iter().map(|&x| x as usize).collect()
    }
}

// ── Lazy Block Loading ───────────────────────────────────────────────────────

const BLOCK_BITS: usize = BLOCK_WORDS * 64;

struct LazyRankBitVec {
    prefix_ranks: Vec<u64>,
    blocks: Vec<OnceLock<Vec<u64>>>,
    reader: Arc<dyn crate::scalar::IndexReader>,
    block_row_offset: usize,
    len: usize,
}

impl std::fmt::Debug for LazyRankBitVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyRankBitVec")
            .field("len", &self.len)
            .finish()
    }
}

impl LazyRankBitVec {
    fn new(
        prefix_ranks: Vec<u64>,
        num_blocks: usize,
        reader: Arc<dyn crate::scalar::IndexReader>,
        offset: usize,
        len: usize,
    ) -> Self {
        Self {
            prefix_ranks,
            blocks: (0..num_blocks).map(|_| OnceLock::new()).collect(),
            reader,
            block_row_offset: offset,
            len,
        }
    }

    async fn load_block_if_needed(&self, idx: usize) -> Result<()> {
        if idx >= self.blocks.len() {
            return Err(Error::index(format!(
                "FM-Index block {idx} is out of range for {} blocks",
                self.blocks.len()
            )));
        }
        if self.blocks[idx].get().is_none() {
            self.load_page_containing(idx).await?;
        }
        Ok(())
    }

    async fn load_page_containing(&self, idx: usize) -> Result<()> {
        let page_rows = fmindex_demand_page_rows();
        let start = (idx / page_rows) * page_rows;
        let end = (start + page_rows).min(self.blocks.len());
        if self.blocks[start..end]
            .iter()
            .all(|block| block.get().is_some())
        {
            return Ok(());
        }

        let batch = self
            .reader
            .read_range(
                self.block_row_offset + start..self.block_row_offset + end,
                Some(&["words"]),
            )
            .await?;
        if batch.num_rows() != end - start {
            return Err(Error::index(format!(
                "expected {} FM-Index block rows, got {}",
                end - start,
                batch.num_rows()
            )));
        }

        let col = Self::words_column(&batch)?;
        for row in 0..batch.num_rows() {
            let block_idx = start + row;
            if self.blocks[block_idx].get().is_none() {
                let words = Self::decode_words(col.value(row));
                let _ = self.blocks[block_idx].set(words);
            }
        }
        Ok(())
    }

    async fn load_block_for_rank(&self, pos: usize) -> Result<()> {
        if pos > self.len {
            return Err(Error::invalid_input(format!(
                "FM-Index rank position {pos} exceeds bitvector length {}",
                self.len
            )));
        }
        if pos == 0 {
            return Ok(());
        }
        if pos == self.len && pos.is_multiple_of(BLOCK_BITS) {
            if let Some(last_idx) = self.blocks.len().checked_sub(1) {
                return self.load_block_if_needed(last_idx).await;
            }
            return Ok(());
        }
        if pos.is_multiple_of(BLOCK_BITS) {
            return Ok(());
        }
        self.load_block_if_needed(pos / BLOCK_BITS).await
    }

    async fn load_block_for_access(&self, pos: usize) -> Result<()> {
        self.load_block_if_needed(pos / BLOCK_BITS).await
    }

    #[inline]
    fn ensure_block(&self, idx: usize) -> &[u64] {
        self.blocks[idx].get_or_init(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.load_block(idx))
            })
            .unwrap_or_else(|e| panic!("FM-Index block load failed: {e}"))
        })
    }

    async fn load_block(&self, idx: usize) -> Result<Vec<u64>> {
        let row = self.block_row_offset + idx;
        let batch = self
            .reader
            .read_range(row..row + 1, Some(&["words"]))
            .await?;
        let col = Self::words_column(&batch)?;
        Ok(Self::decode_words(col.value(0)))
    }

    fn words_column(batch: &RecordBatch) -> Result<&arrow_array::LargeBinaryArray> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .ok_or_else(|| Error::invalid_input("expected LargeBinary words column"))
    }

    fn decode_words(raw: &[u8]) -> Vec<u64> {
        raw.chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    fn terminal_rank1(&self) -> usize {
        if self.len == 0 {
            return 0;
        }
        let Some(last_idx) = self.blocks.len().checked_sub(1) else {
            return 0;
        };
        let mut count = self.prefix_ranks.get(last_idx).copied().unwrap_or(0) as usize;
        let block = self.ensure_block(last_idx);
        let local = self.len - last_idx * BLOCK_BITS;
        let full_words = local / 64;
        let trailing_bits = local % 64;
        for w in &block[..full_words] {
            count += w.count_ones() as usize;
        }
        if trailing_bits > 0 {
            count += (block[full_words] & ((1u64 << trailing_bits) - 1)).count_ones() as usize;
        }
        count
    }

    #[inline]
    fn rank1(&self, pos: usize) -> usize {
        debug_assert!(pos <= self.len);
        if pos == 0 {
            return 0;
        }
        let bi = pos / BLOCK_BITS;
        let local = pos % BLOCK_BITS;
        if local == 0 {
            if let Some(prefix_rank) = self.prefix_ranks.get(bi) {
                return *prefix_rank as usize;
            }
            if pos == self.len {
                return self.terminal_rank1();
            }
        }
        let mut count = self.prefix_ranks[bi] as usize;
        let block = self.ensure_block(bi);
        let wi = local / 64;
        let bit = local % 64;
        for w in &block[..wi] {
            count += w.count_ones() as usize;
        }
        if bit > 0 {
            count += (block[wi] & ((1u64 << bit) - 1)).count_ones() as usize;
        }
        count
    }

    #[inline]
    fn rank0(&self, pos: usize) -> usize {
        pos - self.rank1(pos)
    }

    #[inline]
    fn get(&self, pos: usize) -> bool {
        debug_assert!(pos < self.len);
        let bi = pos / BLOCK_BITS;
        let local = pos % BLOCK_BITS;
        let block = self.ensure_block(bi);
        (block[local / 64] >> (local % 64)) & 1 != 0
    }

    fn deep_size(&self) -> usize {
        let loaded: usize = self
            .blocks
            .iter()
            .filter_map(|b| b.get())
            .map(|w| w.len() * 8)
            .sum();
        self.prefix_ranks.len() * 8 + loaded
    }
}

struct LazyHuffmanWaveletTree {
    nodes: Vec<LazyRankBitVec>,
    codes: [HuffmanCode; 256],
    children: Vec<(WaveletChild, WaveletChild)>,
    len: usize,
}

impl std::fmt::Debug for LazyHuffmanWaveletTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyHuffmanWaveletTree")
            .field("len", &self.len)
            .finish()
    }
}

impl LazyHuffmanWaveletTree {
    /// Pre-load all wavelet tree blocks into memory.
    async fn load_all(&self) -> Result<()> {
        if self.nodes.is_empty() {
            return Ok(());
        }

        #[derive(Clone, Copy)]
        struct RowRange {
            start: usize,
            end: usize,
            node_idx: usize,
        }

        let mut ranges = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.blocks.is_empty())
            .map(|(node_idx, node)| RowRange {
                start: node.block_row_offset,
                end: node.block_row_offset + node.blocks.len(),
                node_idx,
            })
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.start);
        let Some(total_rows) = ranges.iter().map(|range| range.end).max() else {
            return Ok(());
        };
        let ranges = Arc::new(ranges);

        let chunk_loaded = |start: usize, end: usize| {
            ranges.iter().all(|range| {
                let overlap_start = start.max(range.start);
                let overlap_end = end.min(range.end);
                if overlap_start >= overlap_end {
                    return true;
                }
                let node = &self.nodes[range.node_idx];
                node.blocks[overlap_start - range.start..overlap_end - range.start]
                    .iter()
                    .all(|block| block.get().is_some())
            })
        };

        let chunk_rows = (fmindex_prewarm_chunk_bytes() / (BLOCK_WORDS * 8)).max(1);
        let reader = Arc::clone(&self.nodes[0].reader);
        let chunks = (0..total_rows)
            .step_by(chunk_rows)
            .map(|start| {
                let end = (start + chunk_rows).min(total_rows);
                (start, end)
            })
            .filter(|(start, end)| !chunk_loaded(*start, *end))
            .collect::<Vec<_>>();

        futures::stream::iter(chunks)
            .map(|(start, end)| {
                let reader = Arc::clone(&reader);
                async move {
                    let batch = reader.read_range(start..end, Some(&["words"])).await?;
                    Result::<_>::Ok((start, end, batch))
                }
            })
            .buffer_unordered(fmindex_prewarm_chunk_concurrency())
            .try_for_each(|(start, end, batch)| {
                let ranges = Arc::clone(&ranges);
                let nodes = &self.nodes;
                async move {
                    if batch.num_rows() != end - start {
                        return Err(Error::index(format!(
                            "expected {} FM-Index block rows, got {}",
                            end - start,
                            batch.num_rows()
                        )));
                    }

                    let col = LazyRankBitVec::words_column(&batch)?;
                    let mut range_idx = ranges.partition_point(|range| range.end <= start);
                    for row in 0..batch.num_rows() {
                        let absolute_row = start + row;
                        while range_idx < ranges.len() && absolute_row >= ranges[range_idx].end {
                            range_idx += 1;
                        }
                        if range_idx == ranges.len() || absolute_row < ranges[range_idx].start {
                            continue;
                        }

                        let range = ranges[range_idx];
                        let block_idx = absolute_row - range.start;
                        let node = &nodes[range.node_idx];
                        if node.blocks[block_idx].get().is_none() {
                            let words = LazyRankBitVec::decode_words(col.value(row));
                            let _ = node.blocks[block_idx].set(words);
                        }
                    }

                    Ok(())
                }
            })
            .await?;

        Ok(())
    }

    #[inline]
    fn access(&self, mut pos: usize) -> u8 {
        if self.nodes.is_empty() {
            return 0;
        }
        let mut node_idx = 0;
        loop {
            let bit = self.nodes[node_idx].get(pos);
            let (ref left, ref right) = self.children[node_idx];
            if bit {
                pos = self.nodes[node_idx].rank1(pos);
                match right {
                    WaveletChild::Leaf(b) => return *b,
                    WaveletChild::Node(next) => node_idx = *next,
                }
            } else {
                pos = self.nodes[node_idx].rank0(pos);
                match left {
                    WaveletChild::Leaf(b) => return *b,
                    WaveletChild::Node(next) => node_idx = *next,
                }
            }
        }
    }

    async fn access_async(&self, mut pos: usize) -> Result<u8> {
        if self.nodes.is_empty() {
            return Ok(0);
        }
        let mut node_idx = 0;
        loop {
            self.nodes[node_idx].load_block_for_access(pos).await?;
            let bit = self.nodes[node_idx].get(pos);
            let (ref left, ref right) = self.children[node_idx];
            if bit {
                pos = self.nodes[node_idx].rank1(pos);
                match right {
                    WaveletChild::Leaf(b) => return Ok(*b),
                    WaveletChild::Node(next) => node_idx = *next,
                }
            } else {
                pos = self.nodes[node_idx].rank0(pos);
                match left {
                    WaveletChild::Leaf(b) => return Ok(*b),
                    WaveletChild::Node(next) => node_idx = *next,
                }
            }
        }
    }

    #[inline]
    fn rank(&self, c: u8, pos: usize) -> usize {
        let code = &self.codes[c as usize];
        if code.length == 0 {
            return 0;
        }
        let (mut lo, mut hi) = (0, pos);
        for (level, &nid) in code.node_path.iter().enumerate() {
            if (code.bits >> (code.length - 1 - level as u8)) & 1 == 0 {
                lo = self.nodes[nid].rank0(lo);
                hi = self.nodes[nid].rank0(hi);
            } else {
                lo = self.nodes[nid].rank1(lo);
                hi = self.nodes[nid].rank1(hi);
            }
        }
        hi - lo
    }

    async fn rank_async(&self, c: u8, pos: usize) -> Result<usize> {
        let code = &self.codes[c as usize];
        if code.length == 0 {
            return Ok(0);
        }
        let (mut lo, mut hi) = (0, pos);
        for (level, &nid) in code.node_path.iter().enumerate() {
            self.nodes[nid].load_block_for_rank(lo).await?;
            self.nodes[nid].load_block_for_rank(hi).await?;
            if (code.bits >> (code.length - 1 - level as u8)) & 1 == 0 {
                lo = self.nodes[nid].rank0(lo);
                hi = self.nodes[nid].rank0(hi);
            } else {
                lo = self.nodes[nid].rank1(lo);
                hi = self.nodes[nid].rank1(hi);
            }
        }
        Ok(hi - lo)
    }

    #[inline]
    fn rank_pair(&self, c: u8, lo: usize, hi: usize) -> (usize, usize) {
        let code = &self.codes[c as usize];
        if code.length == 0 {
            return (0, 0);
        }
        let (mut s, mut l, mut h) = (0, lo, hi);
        for (level, &nid) in code.node_path.iter().enumerate() {
            if (code.bits >> (code.length - 1 - level as u8)) & 1 == 0 {
                s = self.nodes[nid].rank0(s);
                l = self.nodes[nid].rank0(l);
                h = self.nodes[nid].rank0(h);
            } else {
                s = self.nodes[nid].rank1(s);
                l = self.nodes[nid].rank1(l);
                h = self.nodes[nid].rank1(h);
            }
        }
        (l - s, h - s)
    }

    async fn rank_pair_async(&self, c: u8, lo: usize, hi: usize) -> Result<(usize, usize)> {
        let code = &self.codes[c as usize];
        if code.length == 0 {
            return Ok((0, 0));
        }
        let (mut s, mut l, mut h) = (0, lo, hi);
        for (level, &nid) in code.node_path.iter().enumerate() {
            self.nodes[nid].load_block_for_rank(s).await?;
            self.nodes[nid].load_block_for_rank(l).await?;
            self.nodes[nid].load_block_for_rank(h).await?;
            if (code.bits >> (code.length - 1 - level as u8)) & 1 == 0 {
                s = self.nodes[nid].rank0(s);
                l = self.nodes[nid].rank0(l);
                h = self.nodes[nid].rank0(h);
            } else {
                s = self.nodes[nid].rank1(s);
                l = self.nodes[nid].rank1(l);
                h = self.nodes[nid].rank1(h);
            }
        }
        Ok((l - s, h - s))
    }

    fn deep_size(&self) -> usize {
        self.nodes.iter().map(|n| n.deep_size()).sum::<usize>()
            + self
                .codes
                .iter()
                .map(|c| c.node_path.len() * 8)
                .sum::<usize>()
    }
}

// ── FM-Index (in-memory, build-time) ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FMIndex {
    wavelet: HuffmanWaveletTree,
    row_ids: Vec<u64>,
    /// Sampled SA: sa_samples[i] = SA[i * SA_SAMPLE_RATE]. Size: N/D × 8 bytes.
    sa_samples: Vec<u64>,
    /// Starting byte offset of each document in the concatenated text.
    doc_start_positions: Vec<u64>,
    c_table: Vec<usize>,
    alphabet_size: usize,
}

impl DeepSizeOf for FMIndex {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.wavelet.deep_size()
            + self.row_ids.len() * 8
            + self.sa_samples.len() * 8
            + self.doc_start_positions.len() * 8
            + self.c_table.len() * std::mem::size_of::<usize>()
    }
}

impl FMIndex {
    fn build(texts: &[(u64, &[u8])]) -> Result<Self> {
        if texts.is_empty() {
            return Ok(Self {
                wavelet: HuffmanWaveletTree {
                    nodes: Vec::new(),
                    codes: std::array::from_fn(|_| HuffmanCode::default()),
                    children: Vec::new(),
                    len: 0,
                },
                row_ids: Vec::new(),
                sa_samples: Vec::new(),
                doc_start_positions: Vec::new(),
                c_table: vec![0; 257],
                alphabet_size: 256,
            });
        }

        let mut concat = Vec::new();
        let mut doc_row_ids = Vec::new();
        let mut doc_starts: Vec<u64> = Vec::new();
        for (row_id, text) in texts {
            doc_starts.push(concat.len() as u64);
            doc_row_ids.push(*row_id);
            concat.extend_from_slice(text);
            concat.push(SENTINEL_BYTE); // \xFF separator between documents
        }
        // Append unique terminator \x00 so SA-IS produces a proper suffix array
        // with a single-cycle LF-mapping permutation.
        concat.push(0x00);
        let n = concat.len();
        let sa = build_suffix_array(&concat);

        let bwt: Vec<u8> = sa
            .iter()
            .map(|&pos| {
                if pos == 0 {
                    concat[n - 1]
                } else {
                    concat[pos - 1]
                }
            })
            .collect();

        let mut counts = vec![0usize; 257];
        for &b in &concat {
            counts[b as usize + 1] += 1;
        }
        for i in 1..257 {
            counts[i] += counts[i - 1];
        }

        // Sampled SA: store every D-th entry
        let sa_samples: Vec<u64> = sa
            .iter()
            .step_by(SA_SAMPLE_RATE)
            .map(|&pos| pos as u64)
            .collect();

        let wavelet = HuffmanWaveletTree::build(&bwt);

        Ok(Self {
            wavelet,
            row_ids: doc_row_ids,
            sa_samples,
            doc_start_positions: doc_starts,
            c_table: counts,
            alphabet_size: 256,
        })
    }

    fn serialize_huffman_codes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for code in &self.wavelet.codes {
            buf.extend_from_slice(&code.bits.to_le_bytes());
            buf.push(code.length);
            buf.extend_from_slice(&(code.node_path.len() as u16).to_le_bytes());
            for &nid in &code.node_path {
                buf.extend_from_slice(&(nid as u32).to_le_bytes());
            }
        }
        buf
    }

    fn deserialize_huffman_codes(data: &[u8]) -> [HuffmanCode; 256] {
        let mut codes: [HuffmanCode; 256] = std::array::from_fn(|_| HuffmanCode::default());
        let mut cur = 0;
        for code in &mut codes {
            let bits = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap());
            cur += 4;
            let length = data[cur];
            cur += 1;
            let plen = u16::from_le_bytes(data[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            let mut node_path = Vec::with_capacity(plen);
            for _ in 0..plen {
                node_path.push(u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize);
                cur += 4;
            }
            *code = HuffmanCode {
                bits,
                length,
                node_path,
            };
        }
        codes
    }

    fn serialize_tree_topology(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.wavelet.children.len() as u32).to_le_bytes());
        for (left, right) in &self.wavelet.children {
            for child in [left, right] {
                match child {
                    WaveletChild::Node(id) => {
                        buf.push(0);
                        buf.extend_from_slice(&(*id as u32).to_le_bytes());
                    }
                    WaveletChild::Leaf(b) => {
                        buf.push(1);
                        buf.extend_from_slice(&(*b as u32).to_le_bytes());
                    }
                }
            }
        }
        buf
    }

    fn deserialize_tree_topology(data: &[u8]) -> Vec<(WaveletChild, WaveletChild)> {
        let mut cur = 0;
        let count = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            let mut read_child = || {
                let t = data[cur];
                cur += 1;
                let v = u32::from_le_bytes(data[cur..cur + 4].try_into().unwrap());
                cur += 4;
                if t == 0 {
                    WaveletChild::Node(v as usize)
                } else {
                    WaveletChild::Leaf(v as u8)
                }
            };
            let l = read_child();
            let r = read_child();
            children.push((l, r));
        }
        children
    }

    fn serialize_c_table(&self) -> Vec<u8> {
        self.c_table
            .iter()
            .flat_map(|&v| (v as u64).to_le_bytes())
            .collect()
    }

    fn deserialize_c_table(data: &[u8]) -> Vec<usize> {
        data.chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()) as usize)
            .collect()
    }

    fn u64_to_bytes(data: &[u64]) -> Vec<u8> {
        data.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn build_wavelet_batch(&self) -> Result<RecordBatch> {
        use arrow_array::{LargeBinaryArray, UInt32Array, UInt64Array};
        let mut nid_b = Vec::new();
        let mut bid_b = Vec::new();
        let mut words_b: Vec<Vec<u8>> = Vec::new();
        let mut pr_b = Vec::new();
        let mut bl_b = Vec::new();

        for (i, node) in self.wavelet.nodes.iter().enumerate() {
            let mut pr: u64 = 0;
            if node.words.is_empty() {
                nid_b.push(i as u32);
                bid_b.push(0u32);
                words_b.push(Vec::new());
                pr_b.push(0u64);
                bl_b.push(node.len as u64);
            } else {
                for (bi, chunk) in node.words.chunks(BLOCK_WORDS).enumerate() {
                    nid_b.push(i as u32);
                    bid_b.push(bi as u32);
                    words_b.push(Self::u64_to_bytes(chunk));
                    pr_b.push(pr);
                    bl_b.push(node.len as u64);
                    pr += chunk.iter().map(|w| w.count_ones() as u64).sum::<u64>();
                }
            }
        }
        let refs: Vec<&[u8]> = words_b.iter().map(|v| v.as_slice()).collect();
        let schema = Arc::new(Self::block_schema());
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(nid_b)),
                Arc::new(UInt32Array::from(bid_b)),
                Arc::new(LargeBinaryArray::from(refs)),
                Arc::new(UInt64Array::from(pr_b)),
                Arc::new(UInt64Array::from(bl_b)),
            ],
        )?)
    }

    fn block_schema() -> arrow_schema::Schema {
        arrow_schema::Schema::new(vec![
            Field::new("node_id", DataType::UInt32, false),
            Field::new("block_id", DataType::UInt32, false),
            Field::new("words", DataType::LargeBinary, false),
            Field::new("prefix_rank", DataType::UInt64, false),
            Field::new("bit_len", DataType::UInt64, false),
        ])
    }
}

// ── Lazy FM-Index ────────────────────────────────────────────────────────────

#[derive(Debug)]
struct LazyFMIndex {
    wavelet: LazyHuffmanWaveletTree,
    row_ids: Vec<u64>,
    sa_samples: Vec<u64>,
    doc_start_positions: Vec<u64>,
    c_table: Vec<usize>,
    fully_prewarmed: AtomicBool,
}

impl LazyFMIndex {
    /// Pre-load all wavelet tree blocks before sync search operations.
    async fn prewarm(&self) -> Result<()> {
        if self.fully_prewarmed.load(Ordering::Acquire) {
            return Ok(());
        }
        self.wavelet.load_all().await.inspect(|_| {
            self.fully_prewarmed.store(true, Ordering::Release);
        })
    }

    fn backward_search(&self, pattern: &[u8]) -> (usize, usize) {
        if pattern.is_empty() || self.wavelet.len == 0 {
            return (0, 0);
        }
        let (mut lo, mut hi) = (0, self.wavelet.len);
        for &b in pattern.iter().rev() {
            let c = self.c_table[b as usize];
            let (occ_lo, occ_hi) = self.wavelet.rank_pair(b, lo, hi);
            lo = c + occ_lo;
            hi = c + occ_hi;
            if lo >= hi {
                return (0, 0);
            }
        }
        (lo, hi)
    }

    async fn backward_search_async(&self, pattern: &[u8]) -> Result<(usize, usize)> {
        if pattern.is_empty() || self.wavelet.len == 0 {
            return Ok((0, 0));
        }
        let (mut lo, mut hi) = (0, self.wavelet.len);
        for &b in pattern.iter().rev() {
            let c = self.c_table[b as usize];
            let (occ_lo, occ_hi) = self.wavelet.rank_pair_async(b, lo, hi).await?;
            lo = c + occ_lo;
            hi = c + occ_hi;
            if lo >= hi {
                return Ok((0, 0));
            }
        }
        Ok((lo, hi))
    }

    #[inline]
    fn locate(&self, mut pos: usize) -> usize {
        let mut steps = 0;
        let n = self.wavelet.len;
        loop {
            if pos.is_multiple_of(SA_SAMPLE_RATE) && (pos / SA_SAMPLE_RATE) < self.sa_samples.len()
            {
                return (self.sa_samples[pos / SA_SAMPLE_RATE] as usize + steps) % n;
            }
            let c = self.wavelet.access(pos);
            pos = self.c_table[c as usize] + self.wavelet.rank(c, pos);
            steps += 1;
            if steps >= n {
                log::warn!("FM-Index SA locate exceeded {n} steps, possible index corruption");
                return 0;
            }
        }
    }

    async fn locate_async(&self, mut pos: usize) -> Result<usize> {
        let mut steps = 0;
        let n = self.wavelet.len;
        loop {
            if pos.is_multiple_of(SA_SAMPLE_RATE) && (pos / SA_SAMPLE_RATE) < self.sa_samples.len()
            {
                return Ok((self.sa_samples[pos / SA_SAMPLE_RATE] as usize + steps) % n);
            }
            let c = self.wavelet.access_async(pos).await?;
            pos = self.c_table[c as usize] + self.wavelet.rank_async(c, pos).await?;
            steps += 1;
            if steps >= n {
                log::warn!("FM-Index SA locate exceeded {n} steps, possible index corruption");
                return Ok(0);
            }
        }
    }

    #[inline]
    fn doc_for_position(&self, text_pos: usize) -> usize {
        let tp = text_pos as u64;
        match self.doc_start_positions.binary_search(&tp) {
            Ok(idx) => idx,
            Err(idx) => idx - 1,
        }
    }

    #[cfg(test)]
    fn search(&self, pattern: &[u8]) -> RoaringBitmap {
        let (lo, hi) = self.backward_search(pattern);
        if lo >= hi {
            return RoaringBitmap::new();
        }
        let mut result = RoaringBitmap::new();
        for i in lo..hi {
            let text_pos = self.locate(i);
            let doc_idx = self.doc_for_position(text_pos);
            result.insert(self.row_ids[doc_idx] as u32);
        }
        result
    }

    /// Search returning full u64 row addresses (preserving fragment ID in upper bits).
    fn search_row_addrs(&self, pattern: &[u8]) -> Vec<u64> {
        let (lo, hi) = self.backward_search(pattern);
        if lo >= hi {
            return Vec::new();
        }
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for i in lo..hi {
            let text_pos = self.locate(i);
            let doc_idx = self.doc_for_position(text_pos);
            let row_addr = self.row_ids[doc_idx];
            if seen.insert(row_addr) {
                result.push(row_addr);
            }
        }
        result
    }

    async fn search_row_addrs_async(&self, pattern: &[u8]) -> Result<Vec<u64>> {
        let (lo, hi) = self.backward_search_async(pattern).await?;
        if lo >= hi {
            return Ok(Vec::new());
        }
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for i in lo..hi {
            let text_pos = self.locate_async(i).await?;
            let doc_idx = self.doc_for_position(text_pos);
            let row_addr = self.row_ids[doc_idx];
            if seen.insert(row_addr) {
                result.push(row_addr);
            }
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn from_reader(
        reader: Arc<dyn crate::scalar::IndexReader>,
        num_bwt_nodes: usize,
        huffman_codes: [HuffmanCode; 256],
        children: Vec<(WaveletChild, WaveletChild)>,
        c_table: Vec<usize>,
        bwt_len: usize,
        total_wavelet_rows: usize,
        num_sa_blocks: usize,
        sa_samples_len: usize,
        row_ids: Vec<u64>,
        doc_start_positions: Vec<u64>,
    ) -> Result<Self> {
        use arrow_array::UInt64Array;

        let meta = reader
            .read_range(
                0..total_wavelet_rows,
                Some(&["node_id", "prefix_rank", "bit_len"]),
            )
            .await?;
        let nid_col = meta
            .column_by_name("node_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .unwrap();
        let pr_col = meta
            .column_by_name("prefix_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let bl_col = meta
            .column_by_name("bit_len")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        struct NM {
            prs: Vec<u64>,
            offset: usize,
            blen: usize,
        }
        let mut nms: Vec<NM> = (0..num_bwt_nodes)
            .map(|_| NM {
                prs: Vec::new(),
                offset: 0,
                blen: 0,
            })
            .collect();
        for row in 0..meta.num_rows() {
            let nid = nid_col.value(row) as usize;
            if nid >= num_bwt_nodes {
                continue;
            }
            let nm = &mut nms[nid];
            if nm.prs.is_empty() {
                nm.offset = row;
            }
            nm.prs.push(pr_col.value(row));
            nm.blen = bl_col.value(row) as usize;
        }

        let mut bwt_nodes = Vec::with_capacity(num_bwt_nodes);
        for nm in &nms {
            bwt_nodes.push(LazyRankBitVec::new(
                nm.prs.clone(),
                nm.prs.len(),
                reader.clone(),
                nm.offset,
                nm.blen,
            ));
        }
        let wavelet = LazyHuffmanWaveletTree {
            nodes: bwt_nodes,
            codes: huffman_codes,
            children,
            len: bwt_len,
        };

        // Read SA samples from packed binary blocks
        let mut sa_samples = Vec::with_capacity(sa_samples_len);
        let sa_batch = reader
            .read_range(
                total_wavelet_rows..total_wavelet_rows + num_sa_blocks,
                Some(&["words"]),
            )
            .await?;
        let words_col = sa_batch
            .column_by_name("words")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        for i in 0..sa_batch.num_rows() {
            let raw = words_col.value(i);
            for chunk in raw.chunks_exact(8) {
                sa_samples.push(u64::from_le_bytes(chunk.try_into().unwrap()));
            }
        }
        sa_samples.truncate(sa_samples_len);

        Ok(Self {
            wavelet,
            row_ids,
            sa_samples,
            doc_start_positions,
            c_table,
            fully_prewarmed: AtomicBool::new(false),
        })
    }

    fn deep_size(&self) -> usize {
        self.wavelet.deep_size()
            + self.row_ids.len() * 8
            + self.sa_samples.len() * 8
            + self.doc_start_positions.len() * 8
            + self.c_table.len() * std::mem::size_of::<usize>()
    }
}

// ── FMIndexScalarIndex ───────────────────────────────────────────────────────

#[derive(Debug)]
struct FMIndexPartition {
    #[allow(dead_code)]
    id: u64,
    fm: LazyFMIndex,
}

#[derive(Debug)]
pub struct FMIndexScalarIndex {
    partitions: Vec<Arc<FMIndexPartition>>,
    io_parallelism: usize,
}

impl DeepSizeOf for FMIndexScalarIndex {
    fn deep_size_of_children(&self, _ctx: &mut lance_core::deepsize::Context) -> usize {
        self.partitions.iter().map(|p| p.fm.deep_size()).sum()
    }
}

impl FMIndexScalarIndex {
    async fn load_partition(
        store: &dyn IndexStore,
        filename: &str,
        pid: u64,
    ) -> Result<FMIndexPartition> {
        let reader = store.open_index_file(filename).await?;
        let md = &reader.schema().metadata;

        let parse = |key: &str| -> Result<usize> {
            md.get(key)
                .ok_or_else(|| Error::invalid_input(format!("missing {key}")))?
                .parse()
                .map_err(|e| Error::invalid_input(format!("invalid {key}: {e}")))
        };

        let num_bwt_nodes = parse("num_bwt_nodes")?;
        let bwt_len = parse("bwt_len")?;
        let num_sa_blocks = parse("num_sa_blocks")?;
        let sa_samples_len = parse("sa_samples_len")?;
        let total_wavelet_rows = parse("total_wavelet_rows")?;

        let c_table = FMIndex::deserialize_c_table(&hex_decode(
            md.get("c_table")
                .ok_or_else(|| Error::invalid_input("missing c_table"))?,
        )?);
        let huffman_codes = FMIndex::deserialize_huffman_codes(&hex_decode(
            md.get("huffman_codes")
                .ok_or_else(|| Error::invalid_input("missing huffman_codes"))?,
        )?);
        let children = FMIndex::deserialize_tree_topology(&hex_decode(
            md.get("tree_topology")
                .ok_or_else(|| Error::invalid_input("missing tree_topology"))?,
        )?);

        // row_ids and doc_start_positions stored in metadata (small)
        let row_ids_hex = md
            .get("row_ids")
            .ok_or_else(|| Error::invalid_input("missing row_ids"))?;
        let row_ids_bytes = hex_decode(row_ids_hex)?;
        let row_ids: Vec<u64> = row_ids_bytes
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let doc_starts_hex = md
            .get("doc_start_positions")
            .ok_or_else(|| Error::invalid_input("missing doc_start_positions"))?;
        let doc_starts_bytes = hex_decode(doc_starts_hex)?;
        let doc_start_positions: Vec<u64> = doc_starts_bytes
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let fm = Box::pin(LazyFMIndex::from_reader(
            reader,
            num_bwt_nodes,
            huffman_codes,
            children,
            c_table,
            bwt_len,
            total_wavelet_rows,
            num_sa_blocks,
            sa_samples_len,
            row_ids,
            doc_start_positions,
        ))
        .await?;
        Ok(FMIndexPartition { id: pid, fm })
    }

    async fn load(
        store: Arc<dyn IndexStore>,
        _fri: Option<Arc<dyn RowIdRemapper>>,
        _cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        let files = store.list_files_with_sizes().await?;
        let mut pfiles: Vec<(u64, String)> = Vec::new();
        for f in &files {
            if let Some(id) = fmindex_partition_id_from_path(&f.path) {
                pfiles.push((id, f.path.clone()));
            }
        }
        if pfiles.is_empty() {
            return Err(Error::invalid_input("no FM-Index partition files found"));
        }
        pfiles.sort_by_key(|(id, _)| *id);
        let io_parallelism = store.io_parallelism().max(1);
        let mut parts = futures::stream::iter(pfiles)
            .map(|(id, name)| {
                let store = Arc::clone(&store);
                async move {
                    let partition = Self::load_partition(store.as_ref(), &name, id).await?;
                    Result::<_>::Ok((id, Arc::new(partition)))
                }
            })
            .buffer_unordered(io_parallelism)
            .try_collect::<Vec<_>>()
            .await?;
        parts.sort_by_key(|(id, _)| *id);
        let parts = parts
            .into_iter()
            .map(|(_, partition)| partition)
            .collect::<Vec<_>>();
        Ok(Arc::new(Self {
            partitions: parts,
            io_parallelism,
        }))
    }

    fn partition_parallelism(&self) -> usize {
        self.io_parallelism.max(1).min(self.partitions.len().max(1))
    }

    async fn prewarm_partitions(&self) -> Result<()> {
        futures::stream::iter(self.partitions.iter().cloned())
            .map(|partition| async move { partition.fm.prewarm().await })
            .buffer_unordered(self.partition_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn search_string_contains(&self, pattern: &[u8]) -> Result<SearchResult> {
        use lance_select::RowAddrTreeMap;

        let pattern: Arc<[u8]> = Arc::from(pattern);
        let tree = futures::stream::iter(self.partitions.iter().cloned())
            .map(|partition| {
                let pattern = Arc::clone(&pattern);
                async move {
                    if partition.fm.fully_prewarmed.load(Ordering::Acquire) {
                        spawn_cpu(move || {
                            Result::<Vec<u64>>::Ok(partition.fm.search_row_addrs(pattern.as_ref()))
                        })
                        .await
                    } else {
                        partition.fm.search_row_addrs_async(pattern.as_ref()).await
                    }
                }
            })
            .buffer_unordered(self.partition_parallelism())
            .try_fold(RowAddrTreeMap::new(), |mut tree, row_addrs| async move {
                for row_addr in row_addrs {
                    tree.insert(row_addr);
                }
                Result::Ok(tree)
            })
            .await?;

        Ok(SearchResult::Exact(lance_select::NullableRowAddrSet::new(
            tree,
            Default::default(),
        )))
    }
}

#[async_trait]
impl Index for FMIndexScalarIndex {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }
    async fn prewarm(&self) -> Result<()> {
        self.prewarm_partitions().await
    }
    fn statistics(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "type": "Fm",
            "num_partitions": self.partitions.len(),
            "total_bwt_len": self.partitions.iter().map(|p| p.fm.wavelet.len).sum::<usize>(),
            "total_docs": self.partitions.iter().map(|p| p.fm.row_ids.len()).sum::<usize>(),
        }))
    }
    fn index_type(&self) -> IndexType {
        IndexType::Fm
    }
    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frags = RoaringBitmap::new();
        for p in &self.partitions {
            for &rid in &p.fm.row_ids {
                frags.insert((rid >> 32) as u32);
            }
        }
        Ok(frags)
    }
}

#[async_trait]
impl ScalarIndex for FMIndexScalarIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        _metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let tq = query
            .as_any()
            .downcast_ref::<TextQuery>()
            .ok_or_else(|| Error::invalid_input("Fm only supports TextQuery"))?;
        match tq {
            TextQuery::StringContains(pattern) => {
                self.search_string_contains(pattern.as_bytes()).await
            }
            // Regex queries are routed only to the ngram index (the FM-index's
            // query parser advertises `supports_regex = false`), so this is
            // unreachable in practice; reject it explicitly rather than silently.
            TextQuery::Regex(_) => Err(Error::invalid_input(
                "FMIndex does not support regular expression queries",
            )),
        }
    }
    fn can_remap(&self) -> bool {
        false
    }
    async fn remap(&self, _: &RowAddrRemap, _: &dyn IndexStore) -> Result<CreatedIndex> {
        Err(Error::not_supported("Fm does not support remap"))
    }
    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest: &dyn IndexStore,
        _old_data_filter: Option<OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        let files = write_partitioned_fmindex_stream(new_data, dest).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::FmIndexDetails {}).unwrap(),
            index_version: FMINDEX_INDEX_VERSION,
            files,
        })
    }
    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::requires_old_data(
            TrainingCriteria::new(TrainingOrdering::None).with_row_addr(),
        )
    }
    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::Fm))
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct FMIndexPartitionJob {
    partition_id: u64,
    texts: Vec<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy)]
struct FMIndexPartitionConfig {
    num_workers: usize,
    max_rows: usize,
    max_bytes: usize,
    queue_size: usize,
    resume_existing: bool,
}

impl FMIndexPartitionConfig {
    fn from_env() -> Self {
        Self {
            num_workers: fmindex_num_workers(),
            max_rows: fmindex_partition_rows(),
            max_bytes: fmindex_partition_bytes(),
            queue_size: fmindex_write_queue_size(),
            resume_existing: fmindex_resume_existing_partitions(),
        }
    }

    fn normalized(self) -> Self {
        Self {
            num_workers: self.num_workers.max(1),
            max_rows: self.max_rows.max(1),
            max_bytes: self.max_bytes.max(1),
            queue_size: self.queue_size.max(1),
            resume_existing: self.resume_existing,
        }
    }
}

async fn write_partitioned_fmindex_stream(
    stream: SendableRecordBatchStream,
    store: &dyn IndexStore,
) -> Result<Vec<IndexFile>> {
    write_partitioned_fmindex_stream_with_config(stream, store, FMIndexPartitionConfig::from_env())
        .await
}

async fn write_partitioned_fmindex_stream_with_config(
    mut stream: SendableRecordBatchStream,
    store: &dyn IndexStore,
    config: FMIndexPartitionConfig,
) -> Result<Vec<IndexFile>> {
    let config = config.normalized();
    log::info!(
        "building FMIndex with {} workers, partition rows {}, partition bytes {}",
        config.num_workers,
        config.max_rows,
        config.max_bytes
    );

    let (sender, receiver): (
        async_channel::Sender<FMIndexPartitionJob>,
        async_channel::Receiver<FMIndexPartitionJob>,
    ) = async_channel::bounded(config.queue_size);
    let store = store.clone_arc();
    let mut completed_files = if config.resume_existing {
        store
            .list_files_with_sizes()
            .await?
            .into_iter()
            .filter_map(|file| fmindex_partition_id_from_path(&file.path).map(|id| (id, file)))
            .collect::<HashMap<_, _>>()
    } else {
        HashMap::new()
    };
    if !completed_files.is_empty() {
        log::info!(
            "resuming FMIndex build with {} existing partition files",
            completed_files.len()
        );
    }
    let mut files = Vec::new();
    let mut worker_tasks = Vec::with_capacity(config.num_workers);
    for _ in 0..config.num_workers {
        let receiver = receiver.clone();
        let store = store.clone();
        worker_tasks.push(tokio::task::spawn(async move {
            let mut files = Vec::new();
            while let Ok(job) = receiver.recv().await {
                files.push(
                    write_fmindex_partition_owned(job.texts, store.clone(), job.partition_id)
                        .await?,
                );
            }
            Result::Ok(files)
        }));
    }
    drop(receiver);

    let producer_result = async {
        let mut partition = Vec::with_capacity(config.max_rows.min(PARTITION_SIZE));
        let mut partition_bytes = 0usize;
        let mut partition_id = 0;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            // Prefer _rowaddr (global row address) over _rowid to ensure stable,
            // globally unique identifiers across segments.
            let row_addrs: &arrow_array::UInt64Array = batch
                .column_by_name(ROW_ADDR)
                .or_else(|| batch.column_by_name("_rowid"))
                .and_then(|c| c.as_any().downcast_ref())
                .ok_or_else(|| {
                    Error::invalid_input("Fm training data must include _rowaddr or _rowid column")
                })?;
            // Use the named value column; fall back to column(0) for legacy streams
            let value_col = batch
                .column_by_name(VALUE_COLUMN_NAME)
                .unwrap_or_else(|| batch.column(0));
            for i in 0..batch.num_rows() {
                let rid = row_addrs.value(i);
                if let Some(bytes) = extract_sanitized_text_bytes(value_col.as_ref(), i)? {
                    partition_bytes = partition_bytes.saturating_add(bytes.len().saturating_add(1));
                    partition.push((rid, bytes));
                    if fmindex_partition_limit_reached(
                        partition.len(),
                        partition_bytes,
                        config.max_rows,
                        config.max_bytes,
                    ) {
                        finish_fmindex_partition(
                            &sender,
                            store.as_ref(),
                            &mut partition,
                            &mut partition_bytes,
                            partition_id,
                            config.max_rows,
                            &mut FMIndexPartitionResumeState {
                                completed_files: &mut completed_files,
                                output_files: &mut files,
                            },
                        )
                        .await?;
                        partition_id += 1;
                    }
                }
            }
        }

        if !partition.is_empty() {
            finish_fmindex_partition(
                &sender,
                store.as_ref(),
                &mut partition,
                &mut partition_bytes,
                partition_id,
                config.max_rows,
                &mut FMIndexPartitionResumeState {
                    completed_files: &mut completed_files,
                    output_files: &mut files,
                },
            )
            .await?;
        }

        Result::Ok(())
    }
    .await;
    drop(sender);

    let mut worker_error = None;
    for worker_task in worker_tasks {
        match worker_task.await {
            Ok(Ok(worker_files)) => files.extend(worker_files),
            Ok(Err(err)) => {
                if worker_error.is_none() {
                    worker_error = Some(err);
                }
            }
            Err(err) => {
                if worker_error.is_none() {
                    worker_error = Some(Error::execution(format!(
                        "FMIndex partition worker failed: {err}"
                    )));
                }
            }
        }
    }
    if let Some(err) = worker_error {
        return Err(err);
    }
    producer_result?;

    for (partition_id, file) in completed_files.drain() {
        log::info!(
            "deleting stale FMIndex partition {partition_id} from previous build: {}",
            file.path
        );
        store.delete_index_file(&file.path).await?;
    }
    if files.is_empty() {
        return Ok(vec![write_empty_fmindex_partition(store.as_ref()).await?]);
    }

    files.sort_unstable_by_key(|(partition_id, _)| *partition_id);
    let files = files.into_iter().map(|(_, file)| file).collect();

    Ok(files)
}

fn fmindex_partition_limit_reached(
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
) -> bool {
    // Byte limits are soft for a single oversized document because FMIndex
    // partitions cannot split a document without changing search semantics.
    rows >= max_rows || bytes >= max_bytes
}

struct FMIndexPartitionResumeState<'a> {
    completed_files: &'a mut HashMap<u64, IndexFile>,
    output_files: &'a mut Vec<(u64, IndexFile)>,
}

async fn finish_fmindex_partition(
    sender: &async_channel::Sender<FMIndexPartitionJob>,
    store: &dyn IndexStore,
    partition: &mut Vec<(u64, Vec<u8>)>,
    partition_bytes: &mut usize,
    partition_id: u64,
    max_rows: usize,
    resume_state: &mut FMIndexPartitionResumeState<'_>,
) -> Result<()> {
    let texts = std::mem::replace(partition, Vec::with_capacity(max_rows.min(PARTITION_SIZE)));
    *partition_bytes = 0;
    if let Some(file) = resume_state.completed_files.remove(&partition_id) {
        let fingerprint = fmindex_partition_fingerprint(&texts);
        if fmindex_partition_matches(store, &file, &fingerprint).await? {
            resume_state.output_files.push((partition_id, file));
            return Ok(());
        }
        log::info!(
            "rebuilding stale FMIndex partition {partition_id}: {}",
            file.path
        );
        store.delete_index_file(&file.path).await?;
    }
    sender
        .send(FMIndexPartitionJob {
            partition_id,
            texts,
        })
        .await
        .map_err(|err| {
            Error::execution(format!(
                "failed to schedule FMIndex partition {partition_id}: {err}"
            ))
        })
}

fn fmindex_partition_fingerprint(texts: &[(u64, Vec<u8>)]) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    fn update(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= *byte as u64;
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    let mut hash = FNV_OFFSET;
    update(&mut hash, b"lance-fmindex-partition-v1");
    update(&mut hash, &(texts.len() as u64).to_le_bytes());
    for (row_id, bytes) in texts {
        update(&mut hash, &row_id.to_le_bytes());
        update(&mut hash, &(bytes.len() as u64).to_le_bytes());
        update(&mut hash, bytes);
    }
    format!("{hash:016x}")
}

async fn fmindex_partition_matches(
    store: &dyn IndexStore,
    file: &IndexFile,
    expected_fingerprint: &str,
) -> Result<bool> {
    let reader = store.open_index_file(&file.path).await?;
    Ok(reader
        .schema()
        .metadata
        .get(FMINDEX_PARTITION_FINGERPRINT_KEY)
        .is_some_and(|fingerprint| fingerprint == expected_fingerprint))
}

fn sanitize_text_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .map(|&b| {
            if b == SENTINEL_BYTE || b == 0x00 {
                b' '
            } else {
                b
            }
        })
        .collect()
}

fn extract_sanitized_text_bytes(
    array: &dyn arrow_array::Array,
    index: usize,
) -> Result<Option<Vec<u8>>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(sanitize_text_bytes(
            array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .value(index)
                .as_bytes(),
        ))),
        DataType::LargeUtf8 => Ok(Some(sanitize_text_bytes(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap()
                .value(index)
                .as_bytes(),
        ))),
        DataType::Binary => Ok(Some(sanitize_text_bytes(
            array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap()
                .value(index),
        ))),
        DataType::LargeBinary => Ok(Some(sanitize_text_bytes(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .unwrap()
                .value(index),
        ))),
        _ => Err(Error::invalid_input(format!(
            "Fm does not support data type: {:?}",
            array.data_type()
        ))),
    }
}

#[cfg(test)]
fn extract_text_bytes(array: &dyn arrow_array::Array, index: usize) -> Result<Option<Vec<u8>>> {
    if array.is_null(index) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .value(index)
                .as_bytes()
                .to_vec(),
        )),
        DataType::LargeUtf8 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap()
                .value(index)
                .as_bytes()
                .to_vec(),
        )),
        DataType::Binary => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap()
                .value(index)
                .to_vec(),
        )),
        DataType::LargeBinary => Ok(Some(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .unwrap()
                .value(index)
                .to_vec(),
        )),
        _ => Err(Error::invalid_input(format!(
            "Fm does not support data type: {:?}",
            array.data_type()
        ))),
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}
fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(Error::invalid_input("invalid hex length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| Error::invalid_input(format!("invalid hex: {e}")))
        })
        .collect()
}

/// Write an FM-Index partition to storage.
///
/// Layout:
///   - Wavelet block rows (BWT nodes)
///   - SA sample blocks (packed u64 in LargeBinary)
///   - Metadata: c_table, huffman_codes, tree_topology, row_ids, doc_start_positions
async fn write_fmindex(
    fm: &FMIndex,
    store: &dyn IndexStore,
    filename: &str,
    partition_fingerprint: Option<&str>,
) -> Result<IndexFile> {
    let schema = Arc::new(FMIndex::block_schema());

    let mut writer = store.new_index_file(filename, schema.clone()).await?;

    // 1. Wavelet blocks
    let wb = fm.build_wavelet_batch()?;
    let nw = wb.num_rows();
    writer.write_record_batch(wb).await?;

    // 2. SA samples packed as binary blocks
    let u64s_per_block = BLOCK_WORDS; // 4096 u64s per block = 32KB
    let mut sa_nid = Vec::new();
    let mut sa_bid = Vec::new();
    let mut sa_words: Vec<Vec<u8>> = Vec::new();
    let mut sa_pr = Vec::new();
    let mut sa_bl = Vec::new();
    for (bi, chunk) in fm.sa_samples.chunks(u64s_per_block).enumerate() {
        sa_nid.push(u32::MAX);
        sa_bid.push(bi as u32);
        sa_words.push(FMIndex::u64_to_bytes(chunk));
        sa_pr.push(0u64);
        sa_bl.push(fm.sa_samples.len() as u64);
    }
    let num_sa_blocks = sa_nid.len();
    if num_sa_blocks > 0 {
        let refs: Vec<&[u8]> = sa_words.iter().map(|v| v.as_slice()).collect();
        writer
            .write_record_batch(RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow_array::UInt32Array::from(sa_nid)),
                    Arc::new(arrow_array::UInt32Array::from(sa_bid)),
                    Arc::new(arrow_array::LargeBinaryArray::from(refs)),
                    Arc::new(arrow_array::UInt64Array::from(sa_pr)),
                    Arc::new(arrow_array::UInt64Array::from(sa_bl)),
                ],
            )?)
            .await?;
    }

    // Metadata
    let mut metadata = HashMap::new();
    metadata.insert("num_bwt_nodes".into(), fm.wavelet.nodes.len().to_string());
    metadata.insert("bwt_len".into(), fm.wavelet.len.to_string());
    metadata.insert("num_sa_blocks".into(), num_sa_blocks.to_string());
    metadata.insert("sa_samples_len".into(), fm.sa_samples.len().to_string());
    metadata.insert("total_wavelet_rows".into(), nw.to_string());
    metadata.insert("sa_sample_rate".into(), SA_SAMPLE_RATE.to_string());
    metadata.insert("alphabet_size".into(), fm.alphabet_size.to_string());
    if let Some(fingerprint) = partition_fingerprint {
        metadata.insert(
            FMINDEX_PARTITION_FINGERPRINT_KEY.into(),
            fingerprint.to_string(),
        );
    }
    metadata.insert("c_table".into(), hex_encode(&fm.serialize_c_table()));
    metadata.insert(
        "huffman_codes".into(),
        hex_encode(&fm.serialize_huffman_codes()),
    );
    metadata.insert(
        "tree_topology".into(),
        hex_encode(&fm.serialize_tree_topology()),
    );
    // row_ids in metadata (10K × 8 = 80KB per partition — small)
    let row_ids_bytes: Vec<u8> = fm.row_ids.iter().flat_map(|&v| v.to_le_bytes()).collect();
    metadata.insert("row_ids".into(), hex_encode(&row_ids_bytes));
    // doc_start_positions in metadata (10K × 8 = 80KB per partition — small)
    let doc_starts_bytes: Vec<u8> = fm
        .doc_start_positions
        .iter()
        .flat_map(|&v| v.to_le_bytes())
        .collect();
    metadata.insert("doc_start_positions".into(), hex_encode(&doc_starts_bytes));

    writer.finish_with_metadata(metadata).await
}

#[cfg(test)]
async fn write_partitioned_fmindex(
    texts: &[(u64, Vec<u8>)],
    store: &dyn IndexStore,
) -> Result<Vec<IndexFile>> {
    if texts.is_empty() {
        return Ok(vec![write_empty_fmindex_partition(store).await?]);
    }
    let mut files = Vec::new();
    for (pid, chunk) in texts.chunks(PARTITION_SIZE).enumerate() {
        files.push(write_fmindex_partition(chunk, store, pid as u64).await?);
    }
    Ok(files)
}

#[cfg(test)]
async fn write_fmindex_partition(
    texts: &[(u64, Vec<u8>)],
    store: &dyn IndexStore,
    partition_id: u64,
) -> Result<IndexFile> {
    let fingerprint = fmindex_partition_fingerprint(texts);
    let refs: Vec<(u64, &[u8])> = texts.iter().map(|(id, t)| (*id, t.as_slice())).collect();
    let fm = FMIndex::build(&refs)?;
    write_fmindex(
        &fm,
        store,
        &fmindex_partition_path(partition_id),
        Some(&fingerprint),
    )
    .await
}

async fn write_fmindex_partition_owned(
    texts: Vec<(u64, Vec<u8>)>,
    store: Arc<dyn IndexStore>,
    partition_id: u64,
) -> Result<(u64, IndexFile)> {
    let fingerprint = fmindex_partition_fingerprint(&texts);
    let fm = spawn_cpu(move || {
        let refs: Vec<(u64, &[u8])> = texts.iter().map(|(id, t)| (*id, t.as_slice())).collect();
        FMIndex::build(&refs)
    })
    .await?;
    let file = write_fmindex(
        &fm,
        store.as_ref(),
        &fmindex_partition_path(partition_id),
        Some(&fingerprint),
    )
    .await?;
    Ok((partition_id, file))
}

async fn write_empty_fmindex_partition(store: &dyn IndexStore) -> Result<IndexFile> {
    let fm = FMIndex::build(&[])?;
    let fingerprint = fmindex_partition_fingerprint(&[]);
    write_fmindex(&fm, store, &fmindex_partition_path(0), Some(&fingerprint)).await
}

// ── Plugin ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct FMIndexPlugin;

#[async_trait]
impl BasicTrainer for FMIndexPlugin {
    fn new_training_request(
        &self,
        _params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {}
            _ => {
                return Err(Error::invalid_input(format!(
                    "FM-Index does not support {:?}",
                    field.data_type()
                )));
            }
        }
        Ok(Box::new(DefaultTrainingRequest::new(
            TrainingCriteria::new(TrainingOrdering::None).with_row_addr(),
        )))
    }
    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        store: &dyn IndexStore,
        _req: Box<dyn TrainingRequest>,
        _fids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let files = write_partitioned_fmindex_stream(data, store).await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pb::FmIndexDetails {}).unwrap(),
            index_version: FMINDEX_INDEX_VERSION,
            files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for FMIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "Fm"
    }
    fn provides_exact_answer(&self) -> bool {
        true
    }
    fn version(&self) -> u32 {
        FMINDEX_INDEX_VERSION
    }
    fn new_query_parser(
        &self,
        index_name: String,
        _details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(TextQueryParser::new(
            index_name,
            self.name().to_string(),
            // needs_recheck: the FM-index returns exact substring matches.
            false,
            // supports_regex: regex acceleration is only implemented for ngram.
            false,
            // min_contains_chars: the FM-index can match a needle of any length.
            0,
        )))
    }
    async fn load_index(
        &self,
        store: Arc<dyn IndexStore>,
        details: &prost_types::Any,
        fri: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        let _ = details.to_msg::<pb::FmIndexDetails>().unwrap_or_default();
        Ok(FMIndexScalarIndex::load(store, fri, cache).await? as Arc<dyn ScalarIndex>)
    }
    async fn load_statistics(
        &self,
        _: Arc<dyn IndexStore>,
        _: &prost_types::Any,
    ) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray, UInt64Array};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use lance_core::{ROW_ADDR, cache::LanceCache};
    use lance_io::object_store::ObjectStore;
    use object_store::path::Path;
    use std::sync::Arc;

    use crate::scalar::lance_format::LanceIndexStore;
    use crate::scalar::registry::BasicTrainer;

    #[derive(Debug, Clone)]
    struct FailNewFileStore {
        inner: Arc<dyn IndexStore>,
    }

    impl DeepSizeOf for FailNewFileStore {
        fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
            0
        }
    }

    #[async_trait::async_trait]
    impl IndexStore for FailNewFileStore {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(self.clone())
        }

        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }

        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.with_io_priority(io_priority),
            })
        }

        async fn new_index_file(
            &self,
            _name: &str,
            _schema: Arc<arrow_schema::Schema>,
        ) -> Result<Box<dyn crate::scalar::IndexWriter>> {
            Err(Error::execution(
                "injected FMIndex write failure".to_string(),
            ))
        }

        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn crate::scalar::IndexReader>> {
            self.inner.open_index_file(name).await
        }

        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner.copy_index_file(name, dest_store).await
        }

        async fn rename_index_file(&self, name: &str, new_name: &str) -> Result<IndexFile> {
            self.inner.rename_index_file(name, new_name).await
        }

        async fn delete_index_file(&self, name: &str) -> Result<()> {
            self.inner.delete_index_file(name).await
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    fn loaded_wavelet_blocks(index: &FMIndexScalarIndex) -> usize {
        index
            .partitions
            .iter()
            .flat_map(|partition| partition.fm.wavelet.nodes.iter())
            .map(|node| {
                node.blocks
                    .iter()
                    .filter(|block| block.get().is_some())
                    .count()
            })
            .sum()
    }

    fn total_wavelet_blocks(index: &FMIndexScalarIndex) -> usize {
        index
            .partitions
            .iter()
            .flat_map(|partition| partition.fm.wavelet.nodes.iter())
            .map(|node| node.blocks.len())
            .sum()
    }

    #[test]
    fn test_index_size_ratio() {
        let docs: Vec<Vec<u8>> = (0..200)
            .map(|i| {
                format!(
                    "document {} with enough text to test size ratio properly end",
                    i
                )
                .into_bytes()
            })
            .collect();
        let texts: Vec<(u64, &[u8])> = docs
            .iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d.as_slice()))
            .collect();
        let fm = FMIndex::build(&texts).unwrap();

        let text_size: usize = docs.iter().map(|d| d.len()).sum();
        let wavelet_size = fm.wavelet.deep_size();
        let sa_size = fm.sa_samples.len() * 8;
        let total = wavelet_size + sa_size;

        let ratio = total as f64 / text_size as f64;
        assert!(
            ratio < 1.5,
            "index should be much smaller than text, got ratio={ratio:.2}"
        );
    }

    #[test]
    fn test_serialization_roundtrip() {
        let texts: Vec<(u64, &[u8])> = vec![
            (10, b"alpha beta gamma"),
            (20, b"beta gamma delta"),
            (30, b"gamma delta epsilon"),
        ];
        let fm = FMIndex::build(&texts).unwrap();

        // Test huffman codes roundtrip
        let hc_bytes = fm.serialize_huffman_codes();
        let hc = FMIndex::deserialize_huffman_codes(&hc_bytes);
        for (i, (loaded, original)) in hc.iter().zip(fm.wavelet.codes.iter()).enumerate() {
            assert_eq!(loaded.bits, original.bits, "bits mismatch at {i}");
            assert_eq!(loaded.length, original.length, "length mismatch at {i}");
            assert_eq!(loaded.node_path, original.node_path, "path mismatch at {i}");
        }

        // Test tree topology roundtrip
        let topo_bytes = fm.serialize_tree_topology();
        let topo = FMIndex::deserialize_tree_topology(&topo_bytes);
        assert_eq!(topo.len(), fm.wavelet.children.len());

        // Test c_table roundtrip
        let ct_bytes = fm.serialize_c_table();
        let ct = FMIndex::deserialize_c_table(&ct_bytes);
        assert_eq!(ct, fm.c_table);
    }

    #[test]
    fn test_hex_roundtrip() {
        let data = vec![0u8, 1, 127, 255, 42];
        let encoded = hex_encode(&data);
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn test_partition_limit_reached_by_rows_or_bytes() {
        assert!(fmindex_partition_limit_reached(10, 5, 10, 100));
        assert!(fmindex_partition_limit_reached(3, 100, 10, 100));
        assert!(!fmindex_partition_limit_reached(3, 99, 10, 100));
    }

    #[test]
    fn test_large_sa_sampling() {
        // Test with enough documents to have multiple SA sample points
        let docs: Vec<Vec<u8>> = (0..50)
            .map(|i| {
                format!(
                    "document number {} with lots of text to ensure we have enough bytes for multiple SA samples across the suffix array positions",
                    i
                )
                .into_bytes()
            })
            .collect();
        let texts: Vec<(u64, &[u8])> = docs
            .iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d.as_slice()))
            .collect();
        let fm = FMIndex::build(&texts).unwrap();

        assert!(fm.sa_samples.len() > 1, "should have multiple SA samples");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_and_load_roundtrip() {
        let texts: Vec<(u64, &[u8])> = vec![
            (0, b"hello world foo bar"),
            (1, b"hello rust baz qux"),
            (2, b"goodbye world quux"),
        ];
        let fm = FMIndex::build(&texts).unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        // Write
        write_fmindex(&fm, store.as_ref(), &fmindex_partition_path(0), None)
            .await
            .unwrap();

        // Load
        let part =
            FMIndexScalarIndex::load_partition(store.as_ref(), &fmindex_partition_path(0), 0)
                .await
                .unwrap();

        // Verify search results match
        let r = part.fm.search(b"hello");
        assert!(r.contains(0));
        assert!(r.contains(1));
        assert!(!r.contains(2));

        let r = part.fm.search(b"world");
        assert!(r.contains(0));
        assert!(!r.contains(1));
        assert!(r.contains(2));

        assert!(part.fm.search(b"xyz").is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_partitioned_write_and_load() {
        let docs: Vec<Vec<u8>> = (0..30)
            .map(|i| format!("document {i} hello world test data").into_bytes())
            .collect();
        let texts: Vec<(u64, Vec<u8>)> = docs
            .into_iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d))
            .collect();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        write_partitioned_fmindex(&texts, store.as_ref())
            .await
            .unwrap();

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Search across partitions
        let r = index
            .search(
                &TextQuery::StringContains("hello world".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(30));
            }
            _ => panic!("expected exact result"),
        }

        let r = index
            .search(
                &TextQuery::StringContains("document 15".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(1));
            }
            _ => panic!("expected exact result"),
        }

        let r = index
            .search(
                &TextQuery::StringContains("nonexistent".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(0));
            }
            _ => panic!("expected exact result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_public_prewarm_loads_lazy_blocks() {
        let docs: Vec<Vec<u8>> = (0..30)
            .map(|i| format!("document {i} hello world test data").into_bytes())
            .collect();
        let texts: Vec<(u64, Vec<u8>)> = docs
            .into_iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d))
            .collect();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        write_partitioned_fmindex(&texts, store.as_ref())
            .await
            .unwrap();

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let total_blocks = total_wavelet_blocks(index.as_ref());
        assert!(total_blocks > 0);
        assert_eq!(loaded_wavelet_blocks(index.as_ref()), 0);

        index.prewarm().await.unwrap();
        assert_eq!(loaded_wavelet_blocks(index.as_ref()), total_blocks);

        let r = index
            .search(
                &TextQuery::StringContains("hello world".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(30));
            }
            _ => panic!("expected exact result"),
        }
        assert_eq!(loaded_wavelet_blocks(index.as_ref()), total_blocks);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lazy_rank_terminal_block_boundary() {
        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "words",
            DataType::LargeBinary,
            false,
        )]));
        let words = vec![u64::MAX; BLOCK_WORDS];
        let bytes = FMIndex::u64_to_bytes(&words);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeBinaryArray::from(vec![bytes.as_slice()]))],
        )
        .unwrap();
        let mut writer = store.new_index_file("rank.lance", schema).await.unwrap();
        writer.write_record_batch(batch).await.unwrap();
        writer.finish().await.unwrap();

        let reader = store.open_index_file("rank.lance").await.unwrap();
        let bitvec = LazyRankBitVec::new(vec![0], 1, reader, 0, BLOCK_BITS);
        bitvec.load_block_for_rank(BLOCK_BITS).await.unwrap();

        assert_eq!(bitvec.rank1(BLOCK_BITS), BLOCK_BITS);
        assert_eq!(bitvec.rank0(BLOCK_BITS), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_contains_search_does_not_full_prewarm_partitions() {
        let docs: Vec<Vec<u8>> = (0..30)
            .map(|i| format!("document {i} hello world test data").into_bytes())
            .collect();
        let texts: Vec<(u64, Vec<u8>)> = docs
            .into_iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d))
            .collect();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        write_partitioned_fmindex(&texts, store.as_ref())
            .await
            .unwrap();

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert_eq!(loaded_wavelet_blocks(index.as_ref()), 0);
        assert!(
            index
                .partitions
                .iter()
                .all(|partition| !partition.fm.fully_prewarmed.load(Ordering::Acquire))
        );

        let r = index
            .search(
                &TextQuery::StringContains("document 15".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(1));
            }
            _ => panic!("expected exact result"),
        }

        assert!(
            index
                .partitions
                .iter()
                .all(|partition| !partition.fm.fully_prewarmed.load(Ordering::Acquire))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plugin_train_and_load() {
        let docs = vec!["hello world", "hello rust", "goodbye world"];
        let row_addrs: Vec<u64> = vec![0, 1, 2];
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(docs)),
                Arc::new(UInt64Array::from(row_addrs)),
            ],
        )
        .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        let stream = RecordBatchStreamAdapter::new(batch.schema(), stream::iter(vec![Ok(batch)]));
        let req = FMIndexPlugin
            .new_training_request("", &arrow_schema::Field::new("val", DataType::Utf8, false))
            .unwrap();
        let created = FMIndexPlugin
            .train_index(
                Box::pin(stream),
                store.as_ref(),
                req,
                None,
                Arc::new(crate::progress::NoopIndexBuildProgress),
            )
            .await
            .unwrap();

        let index = FMIndexPlugin
            .load_index(store, &created.index_details, None, &LanceCache::no_cache())
            .await
            .unwrap();

        let r = index
            .search(
                &TextQuery::StringContains("hello".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(2));
            }
            _ => panic!("expected exact result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_train_splits_partition_by_bytes() {
        let docs = vec!["abcd", "efgh", "ijkl"];
        let row_addrs: Vec<u64> = vec![0, 1, 2];
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(docs)),
                Arc::new(UInt64Array::from(row_addrs)),
            ],
        )
        .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
        let files = write_partitioned_fmindex_stream_with_config(
            Box::pin(stream),
            store.as_ref(),
            FMIndexPartitionConfig {
                num_workers: 2,
                max_rows: 100,
                max_bytes: 5,
                queue_size: 1,
                resume_existing: false,
            },
        )
        .await
        .unwrap();

        assert_eq!(files.len(), 3);
        assert_eq!(files[0].path, fmindex_partition_path(0));
        assert_eq!(files[1].path, fmindex_partition_path(1));
        assert_eq!(files[2].path, fmindex_partition_path(2));

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let r = index
            .search(
                &TextQuery::StringContains("efgh".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(1));
            }
            _ => panic!("expected exact result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_train_resumes_existing_partitions() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        let first_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["first partition"])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let first_stream =
            RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(first_batch)]));
        let first_files = write_partitioned_fmindex_stream_with_config(
            Box::pin(first_stream),
            store.as_ref(),
            FMIndexPartitionConfig {
                num_workers: 1,
                max_rows: 1,
                max_bytes: 1024,
                queue_size: 1,
                resume_existing: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(first_files.len(), 1);
        assert_eq!(first_files[0].path, fmindex_partition_path(0));

        let resumed_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "first partition",
                    "second partition",
                ])),
                Arc::new(UInt64Array::from(vec![0, 1])),
            ],
        )
        .unwrap();
        let resumed_stream =
            RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(resumed_batch)]));
        let resumed_files = write_partitioned_fmindex_stream_with_config(
            Box::pin(resumed_stream),
            store.as_ref(),
            FMIndexPartitionConfig {
                num_workers: 1,
                max_rows: 1,
                max_bytes: 1024,
                queue_size: 1,
                resume_existing: true,
            },
        )
        .await
        .unwrap();

        let resumed_paths = resumed_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            resumed_paths,
            vec![fmindex_partition_path(0), fmindex_partition_path(1)]
        );

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let r = index
            .search(
                &TextQuery::StringContains("partition".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(2));
            }
            _ => panic!("expected exact result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_train_rebuilds_stale_resume_partitions() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        let stale_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["stale zero", "stale one"])),
                Arc::new(UInt64Array::from(vec![0, 1])),
            ],
        )
        .unwrap();
        let stale_stream =
            RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(stale_batch)]));
        write_partitioned_fmindex_stream_with_config(
            Box::pin(stale_stream),
            store.as_ref(),
            FMIndexPartitionConfig {
                num_workers: 1,
                max_rows: 1,
                max_bytes: 1024,
                queue_size: 1,
                resume_existing: false,
            },
        )
        .await
        .unwrap();

        let fresh_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["fresh zero"])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let fresh_stream =
            RecordBatchStreamAdapter::new(schema.clone(), stream::iter(vec![Ok(fresh_batch)]));
        let fresh_files = write_partitioned_fmindex_stream_with_config(
            Box::pin(fresh_stream),
            store.as_ref(),
            FMIndexPartitionConfig {
                num_workers: 1,
                max_rows: 10,
                max_bytes: 1024,
                queue_size: 1,
                resume_existing: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(fresh_files.len(), 1);
        assert_eq!(fresh_files[0].path, fmindex_partition_path(0));
        let remaining_paths = store
            .list_files_with_sizes()
            .await
            .unwrap()
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        assert_eq!(remaining_paths, vec![fmindex_partition_path(0)]);

        let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let r = index
            .search(
                &TextQuery::StringContains("fresh zero".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => assert_eq!(set.len(), Some(1)),
            _ => panic!("expected exact result"),
        }

        let r = index
            .search(
                &TextQuery::StringContains("stale one".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => assert_eq!(set.len(), Some(0)),
            _ => panic!("expected exact result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_train_propagates_worker_write_error() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
        )
        .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let inner = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        )) as Arc<dyn IndexStore>;
        let store = FailNewFileStore { inner };

        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(vec![Ok(batch)]));
        let err = write_partitioned_fmindex_stream_with_config(
            Box::pin(stream),
            &store,
            FMIndexPartitionConfig {
                num_workers: 1,
                max_rows: 1,
                max_bytes: 1024,
                queue_size: 1,
                resume_existing: false,
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{err}").contains("injected FMIndex write failure"));
    }

    #[tokio::test]
    async fn test_fail_new_file_store_with_io_priority_preserves_failure() {
        // `with_io_priority` re-wraps in `FailNewFileStore`, so the reprioritized
        // store must keep injecting the `new_index_file` failure.
        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let inner = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        )) as Arc<dyn IndexStore>;
        let store = FailNewFileStore { inner };

        let reprioritized = store.with_io_priority(7);

        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            DataType::UInt64,
            false,
        )]));
        let err = reprioritized
            .new_index_file("test", schema)
            .await
            .err()
            .expect("new_index_file should fail");
        assert!(format!("{err}").contains("injected FMIndex write failure"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_plugin_train_streams_multiple_partitions() {
        fn training_batch(
            schema: Arc<arrow_schema::Schema>,
            start: usize,
            len: usize,
        ) -> RecordBatch {
            let docs = vec!["x"; len];
            let row_addrs: Vec<u64> = (start..start + len).map(|i| i as u64).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(docs)),
                    Arc::new(UInt64Array::from(row_addrs)),
                ],
            )
            .unwrap()
        }

        let total_rows = PARTITION_SIZE + 5;
        let first_batch_rows = PARTITION_SIZE - 3;
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(
                crate::scalar::registry::VALUE_COLUMN_NAME,
                DataType::Utf8,
                false,
            ),
            arrow_schema::Field::new(ROW_ADDR, DataType::UInt64, false),
        ]));
        let batches = vec![
            Ok(training_batch(schema.clone(), 0, first_batch_rows)),
            Ok(training_batch(
                schema.clone(),
                first_batch_rows,
                total_rows - first_batch_rows,
            )),
        ];

        let tempdir = tempfile::tempdir().unwrap();
        let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ));

        let stream = RecordBatchStreamAdapter::new(schema, stream::iter(batches));
        let req = FMIndexPlugin
            .new_training_request("", &arrow_schema::Field::new("val", DataType::Utf8, false))
            .unwrap();
        let created = FMIndexPlugin
            .train_index(
                Box::pin(stream),
                store.as_ref(),
                req,
                None,
                Arc::new(crate::progress::NoopIndexBuildProgress),
            )
            .await
            .unwrap();

        assert_eq!(created.files.len(), 2);
        assert_eq!(created.files[0].path, fmindex_partition_path(0));
        assert_eq!(created.files[1].path, fmindex_partition_path(1));

        let index = FMIndexPlugin
            .load_index(store, &created.index_details, None, &LanceCache::no_cache())
            .await
            .unwrap();
        let r = index
            .search(
                &TextQuery::StringContains("x".to_string()),
                &crate::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        match r {
            SearchResult::Exact(set) => {
                assert_eq!(set.len(), Some(total_rows as u64));
            }
            _ => panic!("expected exact result"),
        }
    }

    #[test]
    fn test_build_wavelet_batch() {
        let texts: Vec<(u64, &[u8])> = vec![(0, b"hello world"), (1, b"test data")];
        let fm = FMIndex::build(&texts).unwrap();
        let batch = fm.build_wavelet_batch().unwrap();
        assert!(batch.num_rows() > 0);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_extract_text_bytes_types() {
        let utf8 = StringArray::from(vec!["hello"]);
        assert_eq!(
            extract_text_bytes(&utf8, 0).unwrap(),
            Some(b"hello".to_vec())
        );

        let large_utf8 = LargeStringArray::from(vec!["world"]);
        assert_eq!(
            extract_text_bytes(&large_utf8, 0).unwrap(),
            Some(b"world".to_vec())
        );

        let binary = BinaryArray::from(vec![b"bytes" as &[u8]]);
        assert_eq!(
            extract_text_bytes(&binary, 0).unwrap(),
            Some(b"bytes".to_vec())
        );
        let binary_with_sentinels = BinaryArray::from(vec![b"a\xFFb\0c" as &[u8]]);
        assert_eq!(
            extract_sanitized_text_bytes(&binary_with_sentinels, 0).unwrap(),
            Some(b"a b c".to_vec())
        );

        let large_binary = LargeBinaryArray::from(vec![b"large" as &[u8]]);
        assert_eq!(
            extract_text_bytes(&large_binary, 0).unwrap(),
            Some(b"large".to_vec())
        );

        // Null handling
        let nullable = StringArray::from(vec![None::<&str>]);
        assert_eq!(extract_text_bytes(&nullable, 0).unwrap(), None);
    }

    #[test]
    fn test_fmindex_statistics() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let docs: Vec<Vec<u8>> = (0..10).map(|i| format!("doc {i}").into_bytes()).collect();
            let texts: Vec<(u64, Vec<u8>)> = docs
                .into_iter()
                .enumerate()
                .map(|(i, d)| (i as u64, d))
                .collect();

            let tempdir = tempfile::tempdir().unwrap();
            let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
            let store = Arc::new(LanceIndexStore::new(
                Arc::new(ObjectStore::local()),
                index_dir,
                Arc::new(LanceCache::no_cache()),
            ));

            write_partitioned_fmindex(&texts, store.as_ref())
                .await
                .unwrap();
            let index = FMIndexScalarIndex::load(store, None, &LanceCache::no_cache())
                .await
                .unwrap();

            let stats = index.statistics().unwrap();
            assert_eq!(stats["type"], "Fm");
            assert_eq!(stats["total_docs"], 10);
            assert!(stats["total_bwt_len"].as_u64().unwrap() > 0);
        });
    }
}
