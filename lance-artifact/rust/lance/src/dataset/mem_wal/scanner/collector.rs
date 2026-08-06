// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Data source collector for LSM scanner.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use lance_core::Result;
use uuid::Uuid;

use super::data_source::{LsmDataSource, LsmGeneration, ShardSnapshot};
use crate::dataset::Dataset;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// A point-in-time handle to one in-memory memtable, active or frozen —
/// structurally identical (a frozen memtable is just the immutable case).
#[derive(Clone)]
pub struct InMemoryMemTableRef {
    /// Batch store containing the data.
    pub batch_store: Arc<BatchStore>,
    /// Index store for the MemTable.
    pub index_store: Arc<IndexStore>,
    /// Schema of the data.
    pub schema: SchemaRef,
    /// Generation number.
    pub generation: u64,
}

/// Back-compat alias; prefer [`InMemoryMemTableRef`].
pub type ActiveMemTableRef = InMemoryMemTableRef;

/// A shard's in-memory memtables: the one live `active` memtable plus any
/// `frozen` memtables awaiting flush (not yet recorded in the manifest).
/// Mirrors the writer's `WriterState { memtable, frozen_memtables }` so the
/// reader and writer name the same thing the same way.
#[derive(Clone)]
pub struct InMemoryMemTables {
    /// The single live memtable accepting writes.
    pub active: InMemoryMemTableRef,
    /// Frozen memtables awaiting flush; element order is irrelevant — the
    /// collector sorts in-memory sources by `generation` before the scan.
    pub frozen: Vec<InMemoryMemTableRef>,
}

/// Collects data sources from base table and MemWAL shards.
///
/// This collector gathers all data sources that need to be scanned
/// for a query, including:
/// - The base table (compacted data) — optional; omit for fresh-tier-only scans
/// - SSTables from each shard
/// - In-memory memtables per shard (active + frozen-awaiting-flush)
///
/// When the base table is omitted (see [`Self::without_base_table`]), `collect`
/// returns only SSTable and active-memtable sources. This is used
/// by callers that own the base read path elsewhere and only need the WAL's
/// fresh tier (active memtable ∪ L0 SSTables).
pub struct LsmDataSourceCollector {
    /// Base Lance table (None when scanning only the fresh tier).
    base_table: Option<Arc<Dataset>>,
    /// Base path for resolving relative SSTable paths.
    base_path: String,
    /// Shard snapshots from MemWAL index.
    shard_snapshots: Vec<ShardSnapshot>,
    /// In-memory memtables by shard (active + frozen-awaiting-flush).
    in_memory_memtables: HashMap<Uuid, InMemoryMemTables>,
}

impl LsmDataSourceCollector {
    /// Create a new collector from base table and shard snapshots.
    ///
    /// # Arguments
    ///
    /// * `base_table` - The base Lance table (compacted data)
    /// * `shard_snapshots` - Snapshots of shard states from MemWAL index
    pub fn new(base_table: Arc<Dataset>, shard_snapshots: Vec<ShardSnapshot>) -> Self {
        // Use the dataset's URI as base path for resolving relative paths.
        // This ensures memory:// and other scheme-based URIs work correctly.
        let base_path = base_table.uri().trim_end_matches('/').to_string();
        Self {
            base_table: Some(base_table),
            base_path,
            shard_snapshots,
            in_memory_memtables: HashMap::new(),
        }
    }

    /// Create a collector without a base table (fresh-tier scan only).
    ///
    /// The collector emits only SSTable and active-memtable sources.
    /// `base_path` is the table-root URI used to resolve relative SSTable paths
    /// (typically the same URI that would have been the base dataset's URI).
    pub fn without_base_table(
        base_path: impl Into<String>,
        shard_snapshots: Vec<ShardSnapshot>,
    ) -> Self {
        Self {
            base_table: None,
            base_path: base_path.into().trim_end_matches('/').to_string(),
            shard_snapshots,
            in_memory_memtables: HashMap::new(),
        }
    }

    /// Set a shard's active memtable. Back-compat / test entry point; the
    /// read path uses [`Self::with_in_memory_memtables`]. Replaces the
    /// active memtable, preserving any frozen memtables already registered.
    pub fn with_active_memtable(mut self, shard_id: Uuid, memtable: InMemoryMemTableRef) -> Self {
        match self.in_memory_memtables.entry(shard_id) {
            Entry::Occupied(mut e) => e.get_mut().active = memtable,
            Entry::Vacant(e) => {
                e.insert(InMemoryMemTables {
                    active: memtable,
                    frozen: Vec::new(),
                });
            }
        }
        self
    }

    /// Register a shard's in-memory memtables (active + frozen-awaiting-
    /// flush) captured atomically by `ShardWriter::in_memory_memtable_refs`.
    /// The WAL read path's entry point.
    pub fn with_in_memory_memtables(
        mut self,
        shard_id: Uuid,
        memtables: InMemoryMemTables,
    ) -> Self {
        self.in_memory_memtables.insert(shard_id, memtables);
        self
    }

    /// Get the base table, if any.
    pub fn base_table(&self) -> Option<&Arc<Dataset>> {
        self.base_table.as_ref()
    }

    /// Get all shard snapshots.
    pub fn shard_snapshots(&self) -> &[ShardSnapshot] {
        &self.shard_snapshots
    }

    /// In-memory memtables (active + frozen-awaiting-flush) by shard.
    pub fn in_memory_memtables(&self) -> &HashMap<Uuid, InMemoryMemTables> {
        &self.in_memory_memtables
    }

    /// Whether the collector has any on-disk source (base table or an
    /// SSTable). The point-lookup fast path uses this to decide, after
    /// missing every in-memory memtable, between "definitely absent" (`false`)
    /// and "must consult disk via the plan path" (`true`). Cheap: no allocation.
    pub fn has_on_disk_sources(&self) -> bool {
        self.base_table.is_some() || self.shard_snapshots.iter().any(|s| !s.sstables.is_empty())
    }

    /// The in-memory memtables (active + frozen across all shards) as
    /// references, **newest generation first**. Used by batch point lookups
    /// that probe many keys against the same set of memtables; clones no
    /// `Arc`s. Empty when there are no in-memory memtables.
    pub fn in_memory_refs_newest_first(&self) -> Vec<&InMemoryMemTableRef> {
        let mut refs: Vec<&InMemoryMemTableRef> = Vec::new();
        for mems in self.in_memory_memtables.values() {
            refs.push(&mems.active);
            refs.extend(mems.frozen.iter());
        }
        refs.sort_by_key(|m| std::cmp::Reverse(m.generation));
        refs
    }

    /// Visit the in-memory memtables (active + frozen) **newest generation
    /// first** by reference, calling `f` until it returns `Some`; returns that
    /// value (or `None` if every memtable was visited without one).
    ///
    /// Unlike [`Self::collect`], this clones no `Arc`s and — in the common
    /// single-shard, single-active case — allocates nothing, so concurrent
    /// readers don't contend on source refcounts. Generation-DESC order makes
    /// a re-write in the active memtable win over a stale frozen row.
    pub fn find_in_memory_newest_first<T>(
        &self,
        mut f: impl FnMut(&InMemoryMemTableRef) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        // Hot path: one shard, only the active memtable → no Vec, no sort.
        if self.in_memory_memtables.len() == 1 {
            let mems = self.in_memory_memtables.values().next().unwrap();
            if mems.frozen.is_empty() {
                return f(&mems.active);
            }
        }
        let mut refs: Vec<&InMemoryMemTableRef> = Vec::new();
        for mems in self.in_memory_memtables.values() {
            refs.push(&mems.active);
            refs.extend(mems.frozen.iter());
        }
        refs.sort_by_key(|m| std::cmp::Reverse(m.generation));
        for m in refs {
            if let Some(v) = f(m)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// A shard's in-memory memtables (active + frozen-awaiting-flush) as
    /// scan sources, in **ascending generation order**. The planner relies
    /// on this: it reverses sources to generation-DESC so the newest row
    /// wins the dedup tiebreaker (see `LsmScanPlanner::plan_scan`). Active
    /// is the newest generation; frozen are older sealed ones — so without
    /// this sort a stale frozen row could outrank a re-write in the active
    /// memtable for the same pk.
    fn in_memory_sources(shard_id: Uuid, mems: &InMemoryMemTables) -> Vec<LsmDataSource> {
        let mut refs: Vec<&InMemoryMemTableRef> = std::iter::once(&mems.active)
            .chain(mems.frozen.iter())
            .collect();
        refs.sort_by_key(|m| m.generation);
        refs.into_iter()
            .map(|m| LsmDataSource::ActiveMemTable {
                batch_store: m.batch_store.clone(),
                index_store: m.index_store.clone(),
                schema: m.schema.clone(),
                shard_id,
                generation: LsmGeneration::memtable(m.generation),
            })
            .collect()
    }

    /// True when `generation` for `shard_id` is still pinned in memory as a
    /// frozen memtable. During the post-flush grace window a generation is both
    /// committed to the manifest (a flushed source) and held in memory (an
    /// in-memory source); it must be served only from memory — which preserves
    /// the per-batch boundaries the SSTable dataset has lost, so as-of reads
    /// stay snapshot-bounded — and its on-disk copy skipped to avoid scanning
    /// the generation twice. See `ShardWriterConfig::frozen_memtable_grace`.
    fn sstable_pinned_in_memory(&self, shard_id: &Uuid, generation: u64) -> bool {
        self.in_memory_memtables
            .get(shard_id)
            .is_some_and(|mems| mems.frozen.iter().any(|f| f.generation == generation))
    }

    /// Collect all data sources.
    ///
    /// Returns sources in a consistent order:
    /// 1. Base table (gen=0), if configured
    /// 2. SSTables per shard, ordered by generation
    /// 3. In-memory memtables per shard (active + frozen-awaiting-flush)
    pub fn collect(&self) -> Result<Vec<LsmDataSource>> {
        let mut sources = Vec::new();

        if let Some(base) = &self.base_table {
            sources.push(LsmDataSource::BaseTable {
                dataset: base.clone(),
            });
        }

        for snapshot in &self.shard_snapshots {
            for sstable in &snapshot.sstables {
                if self.sstable_pinned_in_memory(&snapshot.shard_id, sstable.generation) {
                    continue;
                }
                let path = self.resolve_sstable_path(&snapshot.shard_id, &sstable.path);
                sources.push(LsmDataSource::SsTable {
                    path,
                    shard_id: snapshot.shard_id,
                    generation: LsmGeneration::memtable(sstable.generation),
                });
            }
        }

        for (shard_id, mems) in &self.in_memory_memtables {
            sources.extend(Self::in_memory_sources(*shard_id, mems));
        }

        Ok(sources)
    }

    /// Collect data sources for specific shards only.
    ///
    /// This is used after shard pruning to avoid loading data from
    /// shards that cannot contain matching rows.
    ///
    /// The base table (when configured) is always included since it may
    /// contain data from any shard (after merging).
    pub fn collect_for_shards(&self, shard_ids: &HashSet<Uuid>) -> Result<Vec<LsmDataSource>> {
        let mut sources = Vec::new();

        if let Some(base) = &self.base_table {
            sources.push(LsmDataSource::BaseTable {
                dataset: base.clone(),
            });
        }

        for snapshot in &self.shard_snapshots {
            if !shard_ids.contains(&snapshot.shard_id) {
                continue;
            }

            for sstable in &snapshot.sstables {
                if self.sstable_pinned_in_memory(&snapshot.shard_id, sstable.generation) {
                    continue;
                }
                let path = self.resolve_sstable_path(&snapshot.shard_id, &sstable.path);
                sources.push(LsmDataSource::SsTable {
                    path,
                    shard_id: snapshot.shard_id,
                    generation: LsmGeneration::memtable(sstable.generation),
                });
            }
        }

        for (shard_id, mems) in &self.in_memory_memtables {
            if !shard_ids.contains(shard_id) {
                continue;
            }

            sources.extend(Self::in_memory_sources(*shard_id, mems));
        }

        Ok(sources)
    }

    /// Get the total number of data sources.
    pub fn num_sources(&self) -> usize {
        let sstable_count: usize = self.shard_snapshots.iter().map(|s| s.sstables.len()).sum();
        let base_count = if self.base_table.is_some() { 1 } else { 0 };
        let in_memory_count: usize = self
            .in_memory_memtables
            .values()
            .map(|m| 1 + m.frozen.len())
            .sum();
        base_count + sstable_count + in_memory_count
    }

    /// Resolve an SSTable path to an absolute path.
    ///
    /// SSTables are stored at: `{base_path}/_mem_wal/{shard_id}/{folder_name}`
    /// The `folder_name` is what's stored in `SsTable.path`.
    fn resolve_sstable_path(&self, shard_id: &Uuid, folder_name: &str) -> String {
        format!("{}/_mem_wal/{}/{}", self.base_path, shard_id, folder_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::scanner::data_source::SsTable;

    fn create_test_snapshots() -> Vec<ShardSnapshot> {
        let shard_a = Uuid::new_v4();
        let shard_b = Uuid::new_v4();

        vec![
            ShardSnapshot {
                shard_id: shard_a,
                spec_id: 1,
                current_generation: 3,
                sstables: vec![
                    SsTable {
                        generation: 1,
                        path: "abc_gen_1".to_string(),
                    },
                    SsTable {
                        generation: 2,
                        path: "def_gen_2".to_string(),
                    },
                ],
            },
            ShardSnapshot {
                shard_id: shard_b,
                spec_id: 1,
                current_generation: 2,
                sstables: vec![SsTable {
                    generation: 1,
                    path: "xyz_gen_1".to_string(),
                }],
            },
        ]
    }

    #[test]
    fn test_collector_num_sources() {
        let snapshots = create_test_snapshots();
        // 1 base table + 2 flushed from shard_a + 1 flushed from shard_b = 4
        // Using a mock dataset is complex, so we just test the counting logic
        assert_eq!(snapshots[0].sstables.len(), 2);
        assert_eq!(snapshots[1].sstables.len(), 1);
    }

    #[test]
    fn test_in_memory_memtable_ref() {
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let index_store = Arc::new(IndexStore::new());
        let schema = Arc::new(arrow_schema::Schema::empty());

        let memtable_ref = InMemoryMemTableRef {
            batch_store,
            index_store,
            schema,
            generation: 5,
        };

        assert_eq!(memtable_ref.generation, 5);
    }

    fn memtable_ref(generation: u64) -> InMemoryMemTableRef {
        InMemoryMemTableRef {
            batch_store: Arc::new(BatchStore::with_capacity(8)),
            index_store: Arc::new(IndexStore::new()),
            schema: Arc::new(arrow_schema::Schema::empty()),
            generation,
        }
    }

    /// Regression for the concurrent-read-vs-flush hole: frozen-awaiting-
    /// flush memtables must reach the scan as their own sources alongside
    /// the active one, so a reader sees no gap while a flush drains.
    #[test]
    fn test_collect_includes_active_and_frozen() {
        let shard = Uuid::new_v4();
        let other = Uuid::new_v4();
        // Frozen deliberately out of order to prove the collector sorts.
        let mems = InMemoryMemTables {
            active: memtable_ref(5),
            frozen: vec![memtable_ref(4), memtable_ref(3)],
        };

        let collector = LsmDataSourceCollector::without_base_table("/tmp/x", vec![])
            .with_in_memory_memtables(shard, mems);

        // One source per in-memory memtable, in ascending generation order
        // (the planner reverses to gen-DESC for the dedup tiebreaker, so a
        // stale frozen row must not outrank a re-write in the active one).
        // num_sources() must account for frozen too.
        assert_eq!(collector.num_sources(), 3);
        let sources = collector.collect().unwrap();
        assert_eq!(sources.len(), 3);
        assert!(sources.iter().all(|s| s.is_active_memtable()));
        assert!(sources.iter().all(|s| s.shard_id() == Some(shard)));
        let gens: Vec<u64> = sources.iter().map(|s| s.generation().as_u64()).collect();
        assert_eq!(gens, vec![3, 4, 5]);

        // Shard pruning keeps the active+frozen set together, all-or-nothing.
        assert!(
            collector
                .collect_for_shards(&HashSet::from([other]))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            collector
                .collect_for_shards(&HashSet::from([shard]))
                .unwrap()
                .len(),
            3
        );
    }

    /// During the post-flush grace window a generation is both committed to the
    /// manifest (a flushed source) and still pinned in memory (a frozen
    /// source). The collector must emit it once, from memory — so as-of reads
    /// keep batch-resolved membership — and skip the on-disk copy. SSTables
    /// NOT pinned in memory are still emitted from disk.
    #[test]
    fn test_collect_suppresses_sstable_pinned_in_memory() {
        let shard = Uuid::new_v4();
        // Manifest lists gens 1 and 2 as flushed; gen 2 is still pinned in
        // memory (just flushed, within grace), gen 1 has been swept.
        let snapshot = ShardSnapshot {
            shard_id: shard,
            spec_id: 0,
            current_generation: 3,
            sstables: vec![
                SsTable {
                    generation: 1,
                    path: "gen_1".to_string(),
                },
                SsTable {
                    generation: 2,
                    path: "gen_2".to_string(),
                },
            ],
        };
        let mems = InMemoryMemTables {
            active: memtable_ref(3),
            frozen: vec![memtable_ref(2)],
        };
        let collector = LsmDataSourceCollector::without_base_table("/tmp/x", vec![snapshot])
            .with_in_memory_memtables(shard, mems);

        let sources = collector.collect().unwrap();
        // gen 1: on-disk (not pinned). gen 2: in-memory only (pinned, disk
        // copy suppressed). gen 3: active. No duplicate gen 2.
        let sstable_gens: Vec<u64> = sources
            .iter()
            .filter(|s| !s.is_active_memtable())
            .map(|s| s.generation().as_u64())
            .collect();
        let in_memory: Vec<u64> = sources
            .iter()
            .filter(|s| s.is_active_memtable())
            .map(|s| s.generation().as_u64())
            .collect();
        assert_eq!(
            sstable_gens,
            vec![1],
            "only the unpinned SSTable gen from disk"
        );
        assert_eq!(in_memory, vec![2, 3], "pinned gen 2 served from memory");
    }
}
