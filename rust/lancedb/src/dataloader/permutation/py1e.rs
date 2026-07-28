// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A bounded, block-oriented shuffle planner modelled on MosaicML Streaming's
//! `py1e` algorithm.
//!
//! # Motivation
//!
//! Random IOPS are expensive on cloud storage.  The two-stage I/O pipeline (see
//! issue #3708) works around this by downloading data in large *sequential
//! blocks* (cloud -> local NVMe) and only performing *random* reads against the
//! local copy.  To make that viable the shuffle must be **bounded**: a row may
//! only travel a limited distance from its original position, so that only a
//! small, sliding window of blocks has to be resident before emission can
//! begin.
//!
//! `py1e` achieves this by spreading each block's rows across a range of
//! approximately `shuffle_block_size` rows and then sorting by the resulting
//! (noisy) positions.  Compared to a global shuffle this trades a small amount
//! of shuffle quality for a bounded working set and balanced, sequential
//! downloads.
//!
//! # What this module produces
//!
//! [`Py1ePlanner::plan`] returns a [`BlockPlan`] describing, for a single split:
//!
//! * `blocks` -- the contiguous physical row ranges to download, in *download
//!   order*.  We call this contiguous position space the "layout": it is the
//!   order in which blocks are fetched and laid out on local storage.  Each
//!   block is an internally contiguous range of the split's row offsets, so
//!   downloading one block is a single sequential read.
//! * `order` -- the emission order: for each output slot, the original row
//!   offset (within the split) to emit.
//! * `order_block` -- parallel to `order`, the index into `blocks` of the block
//!   that holds each emitted offset.  A consumer downloads blocks lazily in
//!   download (layout) order and can evict a block as soon as all of its rows
//!   have been emitted (tracked via [`BlockPlan::block_sizes`]).
//!
//! The plan is pure integer/float math over block sizes -- it needs **no
//! data** -- so it can be computed cheaply up front and then drive a streaming
//! reader.
//!
//! This follows the *construction* of MosaicML's `py1e` (see
//! <https://docs.mosaicml.com/projects/streaming/en/stable/dataset_configuration/shuffling.html>).
//! In MDS the bounded shuffle runs independently within each *canonical node*
//! and never moves a row across a canonical-node boundary, which is what keeps
//! the shuffle deterministic under elastic resumption and rescaling.  Here each
//! split *is* its own canonical node: we read one split at a time and never
//! shuffle rows across split boundaries, so no cross-node behaviour has been
//! removed or specialised away.  We do not reproduce NumPy's PCG64 bitstream;
//! determinism is provided by a seeded `SmallRng` instead.

use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::{Error, Result};

/// Configuration for the [`Py1ePlanner`].
#[derive(Debug, Clone)]
pub struct Py1eConfig {
    /// The download unit: the maximum number of contiguous rows in a single
    /// block.  Blocks are the granularity at which data is fetched from cloud
    /// storage, so larger blocks mean larger (cheaper) sequential reads but a
    /// larger minimum working set.
    pub block_size: u64,
    /// The shuffle window: the approximate maximum distance (in rows) that any
    /// single row may travel from its original position.  For a high-quality shuffle this should
    /// be several times larger than `block_size`.
    pub shuffle_block_size: u64,
    /// Seed for the shuffle.  The caller (for example `StreamingDataset`) is
    /// responsible for folding the epoch and any run-level base seed into this
    /// value so that each epoch produces a different ordering.
    pub seed: u64,
}

impl Py1eConfig {
    fn validate(&self) -> Result<()> {
        if self.block_size == 0 {
            return Err(Error::InvalidInput {
                message: "block_size must be greater than 0".to_string(),
            });
        }
        if self.shuffle_block_size == 0 {
            return Err(Error::InvalidInput {
                message: "shuffle_block_size must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

/// A concrete shuffle plan for a single split.
///
/// See the [module docs](self) for the meaning of each field.  All row offsets
/// are relative to the start of the split (i.e. in `0..num_rows`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockPlan {
    /// Contiguous physical row ranges `[begin, end)` (end exclusive), in the
    /// order they should be downloaded.  `blocks[i]` is the `i`-th block in
    /// download (layout) order.
    pub blocks: Vec<(u64, u64)>,
    /// Emission order: `order[k]` is the original split-relative row offset to
    /// emit at output slot `k`.  This is a permutation of `0..num_rows`.
    pub order: Vec<u64>,
    /// `order_block[k]` is the index into [`blocks`](Self::blocks) of the block
    /// that contains `order[k]`.
    pub order_block: Vec<u32>,
}

impl BlockPlan {
    /// Total number of rows in the split.  Equal to `order.len()`.
    pub fn num_rows(&self) -> u64 {
        self.order.len() as u64
    }

    /// The number of rows in each layout block, indexed by layout block id.
    ///
    /// A streaming consumer can initialise a per-block "remaining" counter from
    /// this, decrement it as each row is emitted, and evict a block from the
    /// NVMe cache once its counter reaches zero.
    pub fn block_sizes(&self) -> Vec<u64> {
        self.blocks.iter().map(|(b, e)| e - b).collect()
    }

    /// The number of layout blocks.
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }
}

/// Builds [`BlockPlan`]s using the `py1e` construction.
#[derive(Debug, Clone)]
pub struct Py1ePlanner {
    config: Py1eConfig,
}

impl Py1ePlanner {
    pub fn new(config: Py1eConfig) -> Self {
        Self { config }
    }

    /// Compute the shuffle plan for a split of `num_rows` rows.
    pub fn plan(&self, num_rows: u64) -> Result<BlockPlan> {
        self.config.validate()?;

        if num_rows == 0 {
            return Ok(BlockPlan {
                blocks: Vec::new(),
                order: Vec::new(),
                order_block: Vec::new(),
            });
        }

        let block_size = self.config.block_size;
        let num_blocks = num_rows.div_ceil(block_size);

        // 1. Physical spans: contiguous row ranges in original offset space.
        let mut physical_spans: Vec<(u64, u64)> = (0..num_blocks)
            .map(|b| {
                let begin = b * block_size;
                let end = ((b + 1) * block_size).min(num_rows);
                (begin, end)
            })
            .collect();

        // A single seeded RNG drives the whole plan.  The caller folds the
        // epoch into `seed`, so each epoch already gets a distinct stream.
        let mut rng = SmallRng::seed_from_u64(self.config.seed);

        // 2. Shuffle the block ordering.  The resulting order of
        //    `physical_spans` is the download (layout) order.
        shuffle_slice(&mut physical_spans, &mut rng);

        // `blocks` is now the download order.  Record each block's layout id and
        // its base position in the (contiguous) layout space.
        let blocks = physical_spans;

        // 3. Build the base sample sequence in layout order.  Within each block
        //    we shuffle the row offsets so that rows from the same block do not
        //    emit in physical (on-disk) order; this is the intra-block half of
        //    the shuffle.  The bounded *cross-block* spreading is added
        //    separately as position jitter in step 4.  We also remember, for
        //    every sample, its layout block id and its integer layout position.
        let mut base_offsets: Vec<u64> = Vec::with_capacity(num_rows as usize);
        let mut base_block: Vec<u32> = Vec::with_capacity(num_rows as usize);
        // `span_layout_bounds[i]` = (layout_begin, layout_end) for block i, i.e.
        // the half-open range of layout positions occupied by that block.
        let mut span_layout_bounds: Vec<(u64, u64)> = Vec::with_capacity(blocks.len());

        let mut layout_pos: u64 = 0;
        for (block_idx, &(begin, end)) in blocks.iter().enumerate() {
            let mut within: Vec<u64> = (begin..end).collect();
            shuffle_slice(&mut within, &mut rng);
            let span_begin = layout_pos;
            for off in within {
                base_offsets.push(off);
                base_block.push(block_idx as u32);
                layout_pos += 1;
            }
            span_layout_bounds.push((span_begin, layout_pos));
        }

        // 4. Assign a (noisy) real-valued position to each sample.  Start from
        //    the integer layout position and add a bounded uniform shift whose
        //    magnitude is controlled by `shuffle_block_size`.  Sorting by these
        //    positions yields the bounded shuffle.
        let mut positions: Vec<f64> = (0..num_rows).map(|p| p as f64).collect();
        let b = self.config.shuffle_block_size as f64;
        let lo_block = (0.75 * b) as i64;
        let hi_block = (1.25 * b) as i64;

        for &(span_begin, span_end) in &span_layout_bounds {
            let span_size = (span_end - span_begin) as f64;
            // Randomise the effective window per block so that, across many
            // nodes, downloads stay balanced (mirrors the reference).
            let rand_block_size = if hi_block > lo_block {
                rng.random_range(lo_block..hi_block) as f64
            } else {
                lo_block as f64
            };
            // Each side of the span can spread by at most this much while
            // keeping the sample within a window of ~rand_block_size.
            let cutoff = ((rand_block_size - span_size) / 2.0).max(0.0);
            for pos in span_begin..span_end {
                // Clip the shift so a sample never leaves `0..num_rows` (the
                // single canonical node's bounds).
                let lower = (-cutoff).max(-(pos as f64));
                let upper = cutoff.min((num_rows - 1 - pos) as f64);
                let shift = if upper > lower {
                    rng.random_range(lower..upper)
                } else {
                    0.0
                };
                positions[pos as usize] += shift;
            }
        }

        // 5. Argsort by position (stable on ties) to get the emission order.
        let mut sort_indices: Vec<u32> = (0..num_rows as u32).collect();
        sort_indices.sort_by(|&a, &b| {
            positions[a as usize]
                .partial_cmp(&positions[b as usize])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(&b))
        });

        let mut order = Vec::with_capacity(num_rows as usize);
        let mut order_block = Vec::with_capacity(num_rows as usize);
        for &layout_idx in &sort_indices {
            order.push(base_offsets[layout_idx as usize]);
            order_block.push(base_block[layout_idx as usize]);
        }

        Ok(BlockPlan {
            blocks,
            order,
            order_block,
        })
    }
}

/// Fisher-Yates shuffle over a slice using the provided RNG.  Used instead of
/// `SliceRandom::shuffle` so the shuffle is explicit and stable regardless of
/// the `rand` version's internal algorithm.
fn shuffle_slice<T>(slice: &mut [T], rng: &mut SmallRng) {
    let len = slice.len();
    if len <= 1 {
        return;
    }
    for i in (1..len).rev() {
        let j = rng.random_range(0..=i);
        slice.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn cfg(block_size: u64, shuffle_block_size: u64, seed: u64) -> Py1eConfig {
        Py1eConfig {
            block_size,
            shuffle_block_size,
            seed,
        }
    }

    #[test]
    fn test_rejects_zero_block_size() {
        let planner = Py1ePlanner::new(cfg(0, 100, 1));
        assert!(planner.plan(10).is_err());
    }

    #[test]
    fn test_rejects_zero_shuffle_block_size() {
        let planner = Py1ePlanner::new(cfg(4, 0, 1));
        assert!(planner.plan(10).is_err());
    }

    #[test]
    fn test_empty_split() {
        let planner = Py1ePlanner::new(cfg(4, 16, 1));
        let plan = planner.plan(0).unwrap();
        assert_eq!(plan.num_rows(), 0);
        assert!(plan.blocks.is_empty());
        assert!(plan.order.is_empty());
        assert!(plan.order_block.is_empty());
    }

    /// The emission order must be a permutation of 0..num_rows (every row
    /// emitted exactly once, nothing invented or dropped).
    #[test]
    fn test_order_is_a_permutation() {
        for &num_rows in &[1u64, 7, 64, 1000, 4096, 5000] {
            let planner = Py1ePlanner::new(cfg(64, 512, 42));
            let plan = planner.plan(num_rows).unwrap();
            assert_eq!(plan.order.len() as u64, num_rows);
            assert_eq!(plan.order_block.len() as u64, num_rows);
            let seen: HashSet<u64> = plan.order.iter().copied().collect();
            assert_eq!(seen.len() as u64, num_rows, "num_rows={num_rows}");
            assert_eq!(*seen.iter().min().unwrap(), 0);
            assert_eq!(*seen.iter().max().unwrap(), num_rows - 1);
        }
    }

    /// Blocks must exactly tile the row space (contiguous, non-overlapping,
    /// covering every row), regardless of the shuffled download order.
    #[test]
    fn test_blocks_tile_row_space() {
        let planner = Py1ePlanner::new(cfg(64, 512, 7));
        let plan = planner.plan(5000).unwrap();
        let total: u64 = plan.block_sizes().iter().sum();
        assert_eq!(total, 5000);
        let mut covered: Vec<(u64, u64)> = plan.blocks.clone();
        covered.sort();
        let mut expect = 0;
        for (begin, end) in covered {
            assert_eq!(begin, expect);
            assert!(end > begin);
            expect = end;
        }
        assert_eq!(expect, 5000);
    }

    /// order_block[k] must correctly identify the block that physically
    /// contains order[k].
    #[test]
    fn test_order_block_matches_offset() {
        let planner = Py1ePlanner::new(cfg(50, 400, 99));
        let plan = planner.plan(1234).unwrap();
        for (k, &off) in plan.order.iter().enumerate() {
            let (begin, end) = plan.blocks[plan.order_block[k] as usize];
            assert!(begin <= off && off < end, "offset {off} not in its block");
        }
    }

    /// Same seed => identical plan.  Determinism is required for the
    /// elastic-resumability guarantees of StreamingDataset.
    #[test]
    fn test_deterministic_same_seed() {
        let a = Py1ePlanner::new(cfg(64, 512, 12345)).plan(4096).unwrap();
        let b = Py1ePlanner::new(cfg(64, 512, 12345)).plan(4096).unwrap();
        assert_eq!(a, b);
    }

    /// Different seeds => different ordering.  (Per-epoch variation is produced
    /// by the caller folding the epoch into the seed, so a different epoch is a
    /// different seed here.)
    #[test]
    fn test_different_seed_differs() {
        let a = Py1ePlanner::new(cfg(64, 512, 12345)).plan(4096).unwrap();
        let b = Py1ePlanner::new(cfg(64, 512, 12346)).plan(4096).unwrap();
        assert_ne!(a.order, b.order);
    }

    /// The defining property of py1e: displacement is bounded.  No row may move
    /// further than ~shuffle_block_size from its layout position, so the
    /// resident block window stays small.  We verify the *block* window: while
    /// emitting, the span between the lowest still-needed block and the highest
    /// already-touched block never exceeds a small multiple of
    /// shuffle_block_size / block_size.
    #[test]
    fn test_bounded_block_window() {
        let block_size = 64u64;
        let shuffle_block_size = 512u64;
        let planner = Py1ePlanner::new(cfg(block_size, shuffle_block_size, 5));
        let num_rows = 10_000u64;
        let plan = planner.plan(num_rows).unwrap();

        // Simulate the cache: track, at each emission step, how many distinct
        // blocks must be resident (downloaded and not yet fully drained).
        let mut remaining = plan.block_sizes();
        let mut resident: HashSet<u32> = HashSet::new();
        let mut max_downloaded: i64 = -1;
        let mut max_resident = 0usize;

        for k in 0..plan.order.len() {
            let needed = plan.order_block[k];
            // Download forward until the needed block is present.
            while max_downloaded < needed as i64 {
                max_downloaded += 1;
                resident.insert(max_downloaded as u32);
            }
            max_resident = max_resident.max(resident.len());
            // Emit and possibly evict.
            let b = needed;
            remaining[b as usize] -= 1;
            if remaining[b as usize] == 0 {
                resident.remove(&b);
            }
        }

        // Upper bound: window should be within a few multiples of
        // ceil(shuffle_block_size / block_size).  Use a generous 4x factor plus
        // slack; the point is that it is O(shuffle_block_size/block_size) and
        // NOT O(num_blocks).
        let ratio = shuffle_block_size.div_ceil(block_size) as usize;
        let bound = 4 * ratio + 4;
        assert!(
            max_resident <= bound,
            "max resident blocks {max_resident} exceeded bound {bound} (ratio={ratio})"
        );
        // Sanity: it should also be much smaller than the total block count.
        assert!(max_resident < plan.num_blocks());
    }

    /// A shuffle_block_size <= block_size degrades to (essentially) no
    /// intra-layout movement but must still be correct.
    #[test]
    fn test_small_shuffle_block_size_still_valid() {
        let planner = Py1ePlanner::new(cfg(100, 10, 3));
        let plan = planner.plan(1000).unwrap();
        let seen: HashSet<u64> = plan.order.iter().copied().collect();
        assert_eq!(seen.len(), 1000);
    }

    /// Actually shuffles: the emission order should not be the identity for a
    /// reasonable configuration.
    #[test]
    fn test_not_identity() {
        let planner = Py1ePlanner::new(cfg(64, 512, 1));
        let plan = planner.plan(4096).unwrap();
        let identity: Vec<u64> = (0..4096).collect();
        assert_ne!(plan.order, identity);
    }
}
