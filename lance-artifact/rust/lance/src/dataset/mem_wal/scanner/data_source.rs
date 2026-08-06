// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data source types for LSM scanner.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use uuid::Uuid;

use crate::dataset::Dataset;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// A watermark marking how far into one shard's fresh tier a prior scan
/// observed, so membership can be evaluated as of that point (see
/// [`super::builder::LsmScanner::contains_pks_at`]).
///
/// Only the active memtable grows between two reads (appended batches, and a new
/// generation when it rolls); everything at a lower generation — frozen and
/// flushed — is immutable and was fully observed. The watermark includes lower
/// generations whole, the active generation up to `active_batch_count` batches,
/// and excludes higher generations (which appeared after it). It uses only the
/// batch count and generation — both always available, unlike per-batch WAL
/// positions, which the write path does not track. The bound only excludes rows
/// the scan did not observe, so a stale watermark under-counts (a tolerable
/// stale read) rather than dropping a row with no replacement.
#[derive(Debug, Clone, Copy)]
pub struct FreshTierWatermark {
    /// Active generation the scan observed. Higher generations are excluded;
    /// lower ones are immutable and included whole.
    pub active_generation: u64,
    /// Active-memtable batch count at snapshot time. Within the active
    /// generation, only batches at index `< active_batch_count` were observed.
    pub active_batch_count: u64,
}

/// Generation number in LSM tree.
///
/// The base table has generation 0. MemTables have positive integers
/// starting from 1, where higher numbers represent newer data.
///
/// Ordering: Higher generation = newer data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LsmGeneration(u64);

impl LsmGeneration {
    /// Generation for the base table (compacted data).
    pub const BASE_TABLE: Self = Self(0);

    /// Create a generation for a MemTable.
    ///
    /// # Panics
    ///
    /// Panics if `generation` is 0, as generation 0 is reserved for the base table.
    pub fn memtable(generation: u64) -> Self {
        assert!(
            generation > 0,
            "MemTable generation must be >= 1 (0 is reserved for base table)"
        );
        Self(generation)
    }

    /// Get the raw u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Check if this is the base table generation.
    pub fn is_base_table(&self) -> bool {
        self.0 == 0
    }
}

impl From<u64> for LsmGeneration {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for LsmGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_base_table() {
            write!(f, "base")
        } else {
            write!(f, "gen{}", self.0)
        }
    }
}

impl Default for LsmGeneration {
    fn default() -> Self {
        Self::BASE_TABLE
    }
}

/// An SSTable with its storage path.
#[derive(Debug, Clone)]
pub struct SsTable {
    /// Generation number.
    pub generation: u64,
    /// Path to the SSTable directory (relative to table root).
    pub path: String,
}

/// Snapshot of a shard's state at a point in time.
///
/// This is read from the MemWAL index for eventual consistency,
/// or from shard manifests directly for strong consistency.
#[derive(Debug, Clone)]
pub struct ShardSnapshot {
    /// Shard UUID.
    pub shard_id: Uuid,
    /// Shard spec ID (0 if manual shard).
    pub spec_id: u32,
    /// Current generation being written (next flush will be this generation).
    pub current_generation: u64,
    /// List of SSTables and their paths.
    pub sstables: Vec<SsTable>,
}

impl ShardSnapshot {
    /// Create a new shard snapshot.
    pub fn new(shard_id: Uuid) -> Self {
        Self {
            shard_id,
            spec_id: 0,
            current_generation: 1,
            sstables: Vec::new(),
        }
    }

    /// Set the spec ID.
    pub fn with_spec_id(mut self, spec_id: u32) -> Self {
        self.spec_id = spec_id;
        self
    }

    /// Set the current generation.
    pub fn with_current_generation(mut self, generation: u64) -> Self {
        self.current_generation = generation;
        self
    }

    /// Add an SSTable.
    pub fn with_sstable(mut self, generation: u64, path: String) -> Self {
        self.sstables.push(SsTable { generation, path });
        self
    }
}

/// A data source in the LSM tree that can be scanned.
pub enum LsmDataSource {
    /// Base Lance table (generation = 0).
    BaseTable {
        /// The base dataset.
        dataset: Arc<Dataset>,
    },
    /// SSTable stored as Lance table on disk.
    SsTable {
        /// Absolute path to the SSTable directory.
        path: String,
        /// Shard this MemTable belongs to.
        shard_id: Uuid,
        /// Generation number (1, 2, 3, ...).
        generation: LsmGeneration,
    },
    /// In-memory MemTable (active write buffer).
    ActiveMemTable {
        /// Batch store containing the data.
        batch_store: Arc<BatchStore>,
        /// Index store for the MemTable.
        index_store: Arc<IndexStore>,
        /// Schema of the data.
        schema: SchemaRef,
        /// Shard this MemTable belongs to.
        shard_id: Uuid,
        /// Generation number.
        generation: LsmGeneration,
    },
}

impl LsmDataSource {
    /// Get the generation of this data source.
    pub fn generation(&self) -> LsmGeneration {
        match self {
            Self::BaseTable { .. } => LsmGeneration::BASE_TABLE,
            Self::SsTable { generation, .. } => *generation,
            Self::ActiveMemTable { generation, .. } => *generation,
        }
    }

    /// Get the shard ID if this is a shard source.
    pub fn shard_id(&self) -> Option<Uuid> {
        match self {
            Self::BaseTable { .. } => None,
            Self::SsTable { shard_id, .. } => Some(*shard_id),
            Self::ActiveMemTable { shard_id, .. } => Some(*shard_id),
        }
    }

    /// Check if this is the base table.
    pub fn is_base_table(&self) -> bool {
        matches!(self, Self::BaseTable { .. })
    }

    /// Check if this is an active (in-memory) MemTable.
    pub fn is_active_memtable(&self) -> bool {
        matches!(self, Self::ActiveMemTable { .. })
    }

    /// Get a display name for logging.
    pub fn display_name(&self) -> String {
        match self {
            Self::BaseTable { .. } => "base_table".to_string(),
            Self::SsTable {
                shard_id,
                generation,
                ..
            } => format!("flushed[{}:{}]", &shard_id.to_string()[..8], generation),
            Self::ActiveMemTable {
                shard_id,
                generation,
                ..
            } => format!("memtable[{}:{}]", &shard_id.to_string()[..8], generation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsm_generation_ordering() {
        let base = LsmGeneration::BASE_TABLE;
        let gen1 = LsmGeneration::memtable(1);
        let gen2 = LsmGeneration::memtable(2);
        let gen10 = LsmGeneration::memtable(10);

        // Base table (gen=0) should be less than all MemTable generations
        assert!(base < gen1);
        assert!(base < gen2);
        assert!(base < gen10);

        // Higher generation = newer data
        assert!(gen1 < gen2);
        assert!(gen2 < gen10);

        // Test display
        assert_eq!(base.to_string(), "base");
        assert_eq!(gen1.to_string(), "gen1");
        assert_eq!(gen10.to_string(), "gen10");

        // Test as_u64
        assert_eq!(base.as_u64(), 0);
        assert_eq!(gen1.as_u64(), 1);
        assert_eq!(gen10.as_u64(), 10);
    }

    #[test]
    fn test_lsm_generation_conversions() {
        let from_u64: LsmGeneration = 5u64.into();
        assert_eq!(from_u64.as_u64(), 5);

        let base: LsmGeneration = 0u64.into();
        assert!(base.is_base_table());
    }

    #[test]
    #[should_panic(expected = "MemTable generation must be >= 1")]
    fn test_memtable_generation_zero_panics() {
        LsmGeneration::memtable(0);
    }

    #[test]
    fn test_shard_snapshot_builder() {
        let shard_id = Uuid::new_v4();
        let snapshot = ShardSnapshot::new(shard_id)
            .with_spec_id(1)
            .with_current_generation(5)
            .with_sstable(1, "abc123_gen_1".to_string())
            .with_sstable(2, "def456_gen_2".to_string());

        assert_eq!(snapshot.shard_id, shard_id);
        assert_eq!(snapshot.spec_id, 1);
        assert_eq!(snapshot.current_generation, 5);
        assert_eq!(snapshot.sstables.len(), 2);
        assert_eq!(snapshot.sstables[0].generation, 1);
        assert_eq!(snapshot.sstables[1].generation, 2);
    }
}
