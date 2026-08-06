// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use lance_core::Error;
use lance_core::deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::format::pb;

pub const MEM_WAL_INDEX_NAME: &str = "__lance_mem_wal";

/// Type alias for shard identifier (UUID v4).
pub type ShardId = Uuid;

/// An SSTable: the immutable result of flushing a MemTable, stored as a Lance dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct SsTable {
    pub generation: u64,
    pub path: String,
}

impl From<&SsTable> for pb::SsTable {
    fn from(sstable: &SsTable) -> Self {
        Self {
            generation: sstable.generation,
            path: sstable.path.clone(),
        }
    }
}

impl From<pb::SsTable> for SsTable {
    fn from(sstable: pb::SsTable) -> Self {
        Self {
            generation: sstable.generation,
            path: sstable.path,
        }
    }
}

/// A pointer to the latest SSTable compacted for a shard.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub struct CompactedSsTable {
    pub shard_id: Uuid,
    pub generation: u64,
}

impl DeepSizeOf for CompactedSsTable {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        0 // UUID is 16 bytes fixed size, no heap allocations
    }
}

impl CompactedSsTable {
    pub fn new(shard_id: Uuid, generation: u64) -> Self {
        Self {
            shard_id,
            generation,
        }
    }
}

impl From<&CompactedSsTable> for pb::CompactedSsTable {
    fn from(sstable: &CompactedSsTable) -> Self {
        Self {
            shard_id: Some((&sstable.shard_id).into()),
            generation: sstable.generation,
        }
    }
}

impl TryFrom<pb::CompactedSsTable> for CompactedSsTable {
    type Error = Error;

    fn try_from(sstable: pb::CompactedSsTable) -> lance_core::Result<Self> {
        let shard_id = sstable
            .shard_id
            .as_ref()
            .map(Uuid::try_from)
            .ok_or_else(|| Error::invalid_input("Missing shard_id in CompactedSsTable"))??;
        Ok(Self {
            shard_id,
            generation: sstable.generation,
        })
    }
}

/// Tracks which compacted SSTable generation a base table index covers.
/// Used to determine whether to read from SSTable indexes or base table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct IndexCatchupProgress {
    pub index_name: String,
    pub caught_up_generations: Vec<CompactedSsTable>,
}

impl IndexCatchupProgress {
    pub fn new(index_name: String, caught_up_generations: Vec<CompactedSsTable>) -> Self {
        Self {
            index_name,
            caught_up_generations,
        }
    }

    /// Get the caught up generation for a specific shard.
    /// Returns None if the shard is not present (assumed fully caught up).
    pub fn caught_up_generation_for_shard(&self, shard_id: &Uuid) -> Option<u64> {
        self.caught_up_generations
            .iter()
            .find(|sstable| &sstable.shard_id == shard_id)
            .map(|sstable| sstable.generation)
    }
}

impl From<&IndexCatchupProgress> for pb::IndexCatchupProgress {
    fn from(icp: &IndexCatchupProgress) -> Self {
        Self {
            index_name: icp.index_name.clone(),
            caught_up_generations: icp
                .caught_up_generations
                .iter()
                .map(|sstable| sstable.into())
                .collect(),
        }
    }
}

impl TryFrom<pb::IndexCatchupProgress> for IndexCatchupProgress {
    type Error = Error;

    fn try_from(icp: pb::IndexCatchupProgress) -> lance_core::Result<Self> {
        Ok(Self {
            index_name: icp.index_name,
            caught_up_generations: icp
                .caught_up_generations
                .into_iter()
                .map(CompactedSsTable::try_from)
                .collect::<lance_core::Result<_>>()?,
        })
    }
}

/// Lifecycle status of a WAL shard, persisted in [`ShardManifest`].
///
/// `Sealed` is the durable in-doubt record for drop-table two-phase
/// commit: a sealed shard refuses new writer claims (enforced in
/// `claim_epoch`) but is reversible back to `Active` on rollback.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardStatus {
    /// Normal: the shard accepts writer claims.
    #[default]
    Active,
    /// A drop is in flight: claims are refused. Reversible.
    Sealed,
}

impl ShardStatus {
    /// Map to the protobuf enum discriminant (`pb::ShardStatus`).
    fn to_i32(self) -> i32 {
        match self {
            Self::Active => 0,
            Self::Sealed => 1,
        }
    }

    /// Map from the protobuf enum discriminant; unknown values decode as
    /// `Active` (forward-compatible default).
    fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::Sealed,
            _ => Self::Active,
        }
    }
}

/// Shard manifest containing epoch-based fencing and WAL state.
/// Each shard has exactly one active writer at any time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardManifest {
    pub shard_id: Uuid,
    pub version: u64,
    pub shard_spec_id: u32,
    /// Computed shard field values as raw Arrow scalar bytes, keyed by field id.
    /// The byte encoding follows Arrow's little-endian convention: int32 is 4 LE
    /// bytes, utf8 is raw UTF-8 bytes, etc. The result_type in the corresponding
    /// ShardingField from the ShardingSpec determines how to interpret each value.
    pub shard_field_values: HashMap<String, Vec<u8>>,
    pub writer_epoch: u64,
    /// The most recent WAL entry position flushed to a MemTable.
    /// Recovery replays from `replay_after_wal_entry_position + 1`. The
    /// default value 0 means "no flush has ever stamped this shard" — WAL
    /// positions themselves are 1-based, so 0 is never a valid covered
    /// position.
    pub replay_after_wal_entry_position: u64,
    /// The most recent WAL entry position observed at manifest write time.
    /// Default 0 means "no entry has been written yet"; WAL positions are
    /// 1-based.
    pub wal_entry_position_last_seen: u64,
    pub current_generation: u64,
    pub sstables: Vec<SsTable>,
    /// Lifecycle status (drop-table 2PC). Defaults to `Active`; preserved
    /// across claims via `..base` so only fresh constructions set it.
    pub status: ShardStatus,
}

impl DeepSizeOf for ShardManifest {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.shard_field_values.deep_size_of_children(context)
            + self.sstables.deep_size_of_children(context)
    }
}

impl From<&ShardManifest> for pb::ShardManifest {
    fn from(rm: &ShardManifest) -> Self {
        Self {
            shard_id: Some((&rm.shard_id).into()),
            version: rm.version,
            shard_spec_id: rm.shard_spec_id,
            shard_field_entries: rm
                .shard_field_values
                .iter()
                .map(|(k, v)| pb::ShardFieldEntry {
                    field_id: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            writer_epoch: rm.writer_epoch,
            replay_after_wal_entry_position: rm.replay_after_wal_entry_position,
            wal_entry_position_last_seen: rm.wal_entry_position_last_seen,
            current_generation: rm.current_generation,
            sstables: rm.sstables.iter().map(|sstable| sstable.into()).collect(),
            status: rm.status.to_i32(),
        }
    }
}

impl TryFrom<pb::ShardManifest> for ShardManifest {
    type Error = Error;

    fn try_from(rm: pb::ShardManifest) -> lance_core::Result<Self> {
        let shard_id = rm
            .shard_id
            .as_ref()
            .map(Uuid::try_from)
            .ok_or_else(|| Error::invalid_input("Missing shard_id in ShardManifest"))??;
        let shard_field_values = rm
            .shard_field_entries
            .into_iter()
            .map(|e| (e.field_id, e.value))
            .collect();
        Ok(Self {
            shard_id,
            version: rm.version,
            shard_spec_id: rm.shard_spec_id,
            shard_field_values,
            writer_epoch: rm.writer_epoch,
            replay_after_wal_entry_position: rm.replay_after_wal_entry_position,
            wal_entry_position_last_seen: rm.wal_entry_position_last_seen,
            current_generation: rm.current_generation,
            sstables: rm.sstables.into_iter().map(SsTable::from).collect(),
            status: ShardStatus::from_i32(rm.status),
        })
    }
}

/// Sharding field definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct ShardingField {
    pub field_id: String,
    pub source_ids: Vec<i32>,
    pub transform: Option<String>,
    pub expression: Option<String>,
    pub result_type: String,
    pub parameters: HashMap<String, String>,
}

impl From<&ShardingField> for pb::ShardingField {
    fn from(rf: &ShardingField) -> Self {
        Self {
            field_id: rf.field_id.clone(),
            source_ids: rf.source_ids.clone(),
            transform: rf.transform.clone(),
            expression: rf.expression.clone(),
            result_type: rf.result_type.clone(),
            parameters: rf.parameters.clone(),
        }
    }
}

impl From<pb::ShardingField> for ShardingField {
    fn from(rf: pb::ShardingField) -> Self {
        Self {
            field_id: rf.field_id,
            source_ids: rf.source_ids,
            transform: rf.transform,
            expression: rf.expression,
            result_type: rf.result_type,
            parameters: rf.parameters,
        }
    }
}

/// Sharding spec definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct ShardingSpec {
    pub spec_id: u32,
    pub fields: Vec<ShardingField>,
}

impl From<&ShardingSpec> for pb::ShardingSpec {
    fn from(rs: &ShardingSpec) -> Self {
        Self {
            spec_id: rs.spec_id,
            fields: rs.fields.iter().map(|f| f.into()).collect(),
        }
    }
}

impl From<pb::ShardingSpec> for ShardingSpec {
    fn from(rs: pb::ShardingSpec) -> Self {
        Self {
            spec_id: rs.spec_id,
            fields: rs.fields.into_iter().map(ShardingField::from).collect(),
        }
    }
}

/// Index details for MemWAL Index, stored in IndexMetadata.index_details.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct MemWalIndexDetails {
    pub snapshot_ts_millis: i64,
    pub num_shards: u32,
    pub inline_snapshots: Option<Vec<u8>>,
    pub sharding_specs: Vec<ShardingSpec>,
    pub maintained_indexes: Vec<String>,
    pub compacted_sstables: Vec<CompactedSsTable>,
    pub index_catchup: Vec<IndexCatchupProgress>,
    /// Default `ShardWriter` configuration values for this MemWAL index.
    ///
    /// Persisted so every writer — across processes and restarts — starts
    /// from the same default writer configuration. These are defaults only;
    /// an individual writer may still override any value at runtime in its
    /// own (non-persisted) `ShardWriterConfig`.
    pub writer_config_defaults: HashMap<String, String>,
}

impl From<&MemWalIndexDetails> for pb::MemWalIndexDetails {
    fn from(details: &MemWalIndexDetails) -> Self {
        Self {
            snapshot_ts_millis: details.snapshot_ts_millis,
            num_shards: details.num_shards,
            inline_snapshots: details.inline_snapshots.clone(),
            sharding_specs: details.sharding_specs.iter().map(|rs| rs.into()).collect(),
            maintained_indexes: details.maintained_indexes.clone(),
            compacted_sstables: details
                .compacted_sstables
                .iter()
                .map(|sstable| sstable.into())
                .collect(),
            index_catchup: details.index_catchup.iter().map(|icp| icp.into()).collect(),
            writer_config_defaults: details.writer_config_defaults.clone(),
        }
    }
}

impl TryFrom<pb::MemWalIndexDetails> for MemWalIndexDetails {
    type Error = Error;

    fn try_from(details: pb::MemWalIndexDetails) -> lance_core::Result<Self> {
        Ok(Self {
            snapshot_ts_millis: details.snapshot_ts_millis,
            num_shards: details.num_shards,
            inline_snapshots: details.inline_snapshots,
            sharding_specs: details
                .sharding_specs
                .into_iter()
                .map(ShardingSpec::from)
                .collect(),
            maintained_indexes: details.maintained_indexes,
            compacted_sstables: details
                .compacted_sstables
                .into_iter()
                .map(CompactedSsTable::try_from)
                .collect::<lance_core::Result<_>>()?,
            index_catchup: details
                .index_catchup
                .into_iter()
                .map(IndexCatchupProgress::try_from)
                .collect::<lance_core::Result<_>>()?,
            writer_config_defaults: details.writer_config_defaults,
        })
    }
}

/// MemWAL Index provides access to MemWAL configuration and state.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub struct MemWalIndex {
    pub details: MemWalIndexDetails,
}

impl MemWalIndex {
    pub fn new(details: MemWalIndexDetails) -> Self {
        Self { details }
    }

    pub fn compacted_generation_for_shard(&self, shard_id: &Uuid) -> Option<u64> {
        self.details
            .compacted_sstables
            .iter()
            .find(|sstable| &sstable.shard_id == shard_id)
            .map(|sstable| sstable.generation)
    }

    /// Get the caught up generation for a specific index and shard.
    /// Returns None if the index is not tracked (assumed fully caught up).
    pub fn index_caught_up_generation(&self, index_name: &str, shard_id: &Uuid) -> Option<u64> {
        self.details
            .index_catchup
            .iter()
            .find(|icp| icp.index_name == index_name)
            .and_then(|icp| icp.caught_up_generation_for_shard(shard_id))
    }

    /// Check if an index is fully caught up for a shard.
    /// Returns true if the index covers all compacted data for the shard.
    pub fn is_index_caught_up(&self, index_name: &str, shard_id: &Uuid) -> bool {
        let compacted_gen = self.compacted_generation_for_shard(shard_id).unwrap_or(0);
        let caught_up_gen = self.index_caught_up_generation(index_name, shard_id);

        // If not tracked in index_catchup, assumed fully caught up
        caught_up_gen.is_none_or(|generation| generation >= compacted_gen)
    }
}
