// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Live per-table_shard LSM state — the shape [`crate::Table::get_lsm_stats`]
//! returns and [`super::checkpoint`] polls.
//!
//! Nothing here is derived: sums and differences (total SSTable bytes, WAL lag)
//! are the caller's to compute. There is no "WAL is off" shape — that case is
//! `None`, because a struct of zeros would read as measurements.

use serde::Deserialize;

/// One SSTable.
#[derive(Debug, Clone, Deserialize)]
pub struct SsTableStats {
    pub generation: u64,
    pub bytes: u64,
    /// Present only when `include_sstable_rows` was requested. Off by
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
    /// Names of the indexes this memtable carries. An absent name is the whole
    /// answer to "why is my fresh-tier search on that column brute-force".
    pub indexes: Vec<String>,
}

/// Live state of one table_shard. A table is N table_shards on one node; flattening to
/// a single number hides the one hot table_shard that is usually why someone
/// opened this endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct TableShardStats {
    pub shard_id: String,
    /// `Active` | `Sealed` (drop-table 2PC in flight).
    pub status: String,
    pub writer_epoch: u64,
    pub manifest_version: u64,
    pub current_generation: u64,
    pub replay_after_wal_entry_position: u64,
    pub wal_entry_position_last_seen: u64,
    pub sstables: Vec<SsTableStats>,
    /// Whether a pass owns this table_shard's compaction latch right now. Says *a*
    /// driver is running, not *whose*, and the latch is held from dispatch —
    /// including while the pass queues for a pod-wide compactor permit. Read
    /// it as "do not pile on", never as "mine is progressing".
    pub compacting: bool,
    /// Oldest first, active last. Absent for a `Sealed` table_shard, whose
    /// in-memory state is torn down.
    #[serde(default)]
    pub memtables: Option<Vec<MemtableStats>>,
}

impl TableShardStats {
    /// The newest SSTable generation, or `None` when the tier is empty.
    pub(crate) fn newest_sstable_generation(&self) -> Option<u64> {
        self.sstables.iter().map(|g| g.generation).max()
    }

    /// How many SSTables at or below `target` are still uncompacted.
    ///
    /// A count, not a boolean: one pass drains a bounded prefix rather than
    /// the whole target set, so a boolean would read as "no progress" for
    /// every pass but the last. Compaction drains oldest-first, so this
    /// decreases monotonically.
    pub(crate) fn outstanding_sstables(&self, target: u64) -> usize {
        self.sstables
            .iter()
            .filter(|g| g.generation <= target)
            .count()
    }
}

/// Live LSM state, one entry per table_shard.
#[derive(Debug, Clone, Deserialize)]
pub struct LsmStats {
    pub table_shards: Vec<TableShardStats>,
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

    fn table_shard(shard: &str, sstables: &[u64], compacting: bool) -> TableShardStats {
        TableShardStats {
            shard_id: shard.into(),
            status: "Active".into(),
            writer_epoch: 1,
            manifest_version: 1,
            current_generation: sstables.iter().max().copied().unwrap_or(0) + 1,
            replay_after_wal_entry_position: 0,
            wal_entry_position_last_seen: 0,
            sstables: sstables
                .iter()
                .map(|g| SsTableStats {
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
    /// generation created after it must not hold the loop open — that is why
    /// the predicate terminates under write load.
    #[test]
    fn newer_sstables_do_not_extend_the_target() {
        let start = table_shard("b0", &[7, 8], false);
        let target = start
            .newest_sstable_generation()
            .expect("the SSTable tier is non-empty");
        assert_eq!(target, 8);

        // Compaction drained 7 and 8; 9 and 10 arrived while it ran.
        let later = table_shard("b0", &[9, 10], false);
        assert_eq!(
            later.outstanding_sstables(target),
            0,
            "sstables above the target are somebody else's problem"
        );

        // Still holding 8 means still outstanding.
        assert_eq!(
            table_shard("b0", &[8, 9], false).outstanding_sstables(target),
            1
        );
    }

    /// The metric counts SSTables, not table shards: a pass drains a bounded
    /// prefix, so one table_shard going 3 → 2 → 1 → 0 is three steps.
    #[test]
    fn progress_is_measured_in_sstables() {
        let target = 3;
        let counts: Vec<usize> = [&[1u64, 2, 3][..], &[2, 3][..], &[3][..], &[][..]]
            .iter()
            .map(|gens| table_shard("b0", gens, false).outstanding_sstables(target))
            .collect();
        assert_eq!(counts, vec![3, 2, 1, 0]);
    }

    #[test]
    fn an_empty_sstable_tier_has_no_target() {
        assert!(
            table_shard("b0", &[], false)
                .newest_sstable_generation()
                .is_none()
        );
    }
}
