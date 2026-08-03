// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Live per-bucket LSM state — the shape [`crate::Table::get_lsm_stats`]
//! returns and the one [`super::checkpoint`] polls for its completion
//! predicate.
//!
//! Nothing here is derived. Sums and differences (total L0 bytes, WAL lag)
//! are the caller's to compute from measured fields, and there is no
//! "WAL is off" shape — that case is `None`, because a struct of zeros
//! would read as measurements.

use serde::Deserialize;

/// One flushed L0 generation.
#[derive(Debug, Clone, Deserialize)]
pub struct GenerationStats {
    pub generation: u64,
    pub bytes: u64,
    /// Present only when `include_generation_rows` was requested. Off by
    /// default because each count opens an uncached Lance dataset, and the
    /// checkpoint loop polls this route needing only generation numbers.
    #[serde(default)]
    pub rows: Option<u64>,
}

/// One in-memory memtable.
#[derive(Debug, Clone, Deserialize)]
pub struct MemtableStats {
    pub generation: u64,
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
    /// Names of the indexes this memtable carries. An absent name is the
    /// whole answer to "why is my fresh-tier search on that column
    /// brute-force"; cross-reference `list_indices` for its kind and column.
    pub indexes: Vec<String>,
}

/// Live state of one bucket. A table is N buckets on one node; flattening
/// to a single number hides the one hot bucket that is usually why someone
/// opened this endpoint.
///
/// Nothing here is derived — sums and differences (total L0 bytes, WAL lag)
/// are the caller's to compute from measured fields.
#[derive(Debug, Clone, Deserialize)]
pub struct BucketStats {
    pub shard_id: String,
    /// `Active` | `Sealed` (drop-table 2PC in flight).
    pub status: String,
    pub writer_epoch: u64,
    pub manifest_version: u64,
    pub current_generation: u64,
    pub replay_after_wal_entry_position: u64,
    pub wal_entry_position_last_seen: u64,
    pub generations: Vec<GenerationStats>,
    /// Whether a pass owns this bucket's compaction latch right now. Says
    /// *a* driver is running, not *whose*: the server's periodic trigger
    /// competes for the same latch, so a caller that dispatched a compact
    /// cannot read this as "mine is progressing", only as "do not pile on".
    pub compacting: bool,
    /// Oldest first, active last. Absent for a `Sealed` bucket, whose
    /// in-memory state is torn down.
    #[serde(default)]
    pub memtables: Option<Vec<MemtableStats>>,
}

impl BucketStats {
    /// The newest flushed generation, or `None` when L0 is empty.
    pub(crate) fn newest_generation(&self) -> Option<u64> {
        self.generations.iter().map(|g| g.generation).max()
    }

    /// How many generations at or below `target` are still in L0.
    ///
    /// A *count*, not a boolean: it is the checkpoint's progress metric,
    /// and one pass drains a bounded prefix rather than the whole target
    /// set. A per-bucket boolean would read as "no progress" for every
    /// pass but the last, so a bucket needing more passes than the idle
    /// bound would fail a checkpoint that was working correctly.
    /// Compaction drains oldest-first, so this decreases monotonically.
    pub(crate) fn outstanding_generations(&self, target: u64) -> usize {
        self.generations
            .iter()
            .filter(|g| g.generation <= target)
            .count()
    }
}

/// Live LSM state, one entry per bucket.
///
/// Every field is measured. There is no "WAL is off" shape here — that
/// case is `None` from [`crate::Table::get_lsm_stats`], because a struct of
/// zeros would read as measurements.
#[derive(Debug, Clone, Deserialize)]
pub struct LsmStats {
    pub buckets: Vec<BucketStats>,
}

/// Server-side JSON envelope for `get_lsm_stats`. `lsm_stats` is null when
/// the table has no LSM write path.
#[derive(Debug, Deserialize)]
pub(crate) struct GetLsmStatsResponse {
    #[serde(default)]
    pub lsm_stats: Option<LsmStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(shard: &str, generations: &[u64], compacting: bool) -> BucketStats {
        BucketStats {
            shard_id: shard.into(),
            status: "Active".into(),
            writer_epoch: 1,
            manifest_version: 1,
            current_generation: generations.iter().max().copied().unwrap_or(0) + 1,
            replay_after_wal_entry_position: 0,
            wal_entry_position_last_seen: 0,
            generations: generations
                .iter()
                .map(|g| GenerationStats {
                    generation: *g,
                    bytes: 1,
                    rows: None,
                })
                .collect(),
            compacting,
            memtables: None,
        }
    }

    /// The target watermark is the newest generation at the start, and a
    /// generation created *after* it must not hold the loop open — that is
    /// the whole reason the predicate terminates under write load.
    #[test]
    fn newer_generations_do_not_extend_the_target() {
        let start = bucket("b0", &[7, 8], false);
        let target = start.newest_generation().expect("L0 is non-empty");
        assert_eq!(target, 8);

        // Compaction drained 7 and 8; 9 and 10 arrived while it ran.
        let later = bucket("b0", &[9, 10], false);
        assert_eq!(
            later.outstanding_generations(target),
            0,
            "generations above the target are somebody else's problem"
        );

        // Still holding 8 means still outstanding.
        assert_eq!(
            bucket("b0", &[8, 9], false).outstanding_generations(target),
            1
        );
    }

    /// The progress metric counts generations, not buckets. A pass drains
    /// a bounded prefix, so one bucket going 3 → 2 → 1 → 0 must read as
    /// three steps of progress, not three idle polls.
    #[test]
    fn progress_is_measured_in_generations() {
        let target = 3;
        let counts: Vec<usize> = [&[1u64, 2, 3][..], &[2, 3][..], &[3][..], &[][..]]
            .iter()
            .map(|gens| bucket("b0", gens, false).outstanding_generations(target))
            .collect();
        assert_eq!(counts, vec![3, 2, 1, 0]);
    }

    #[test]
    fn empty_l0_has_no_target() {
        assert!(bucket("b0", &[], false).newest_generation().is_none());
    }
}
