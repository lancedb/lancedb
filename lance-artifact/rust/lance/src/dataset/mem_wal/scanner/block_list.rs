// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Per-source block-list construction for LSM vector search.
//!
//! A generation's membership is a [`GenMembership`]: in-memory generations
//! (active / frozen) are probed by value against their maintained primary-key
//! index (no per-query set), while SSTables are probed against their
//! standalone on-disk PK BTree (the sidecar written at flush, opened by path).
//! Probing is batched — [`GenMembership::contains_keys`] tests a whole batch of
//! keys per generation in one pass. Each source gets a `Vec<GenMembership>` of
//! the newer generations (`NEWER(G)`; base: all of them); the KNN drops a
//! candidate whose PK any of them contains (see
//! [`super::exec::PkBlockFilterExec`]).
//!
//! Cross-generation only: within-gen dups collapse via the global dedup's
//! `(generation, freshness)` tiebreaker.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::common::ScalarValue;
use lance_core::{Error, Result};

use lance_index::metrics::NoOpMetricsCollector;
use lance_index::registry::IndexPluginRegistry;
use lance_index::scalar::btree::BTreeIndex;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::{
    IndexStore as ScalarIndexStore, SargableQuery, ScalarIndex, SearchResult,
};
use uuid::Uuid;

use super::data_source::{FreshTierWatermark, LsmDataSource, LsmGeneration};
use super::sstable_cache::{DatasetCache, open_sstable};
use crate::dataset::mem_wal::index::encode_pk_tuple;
use crate::dataset::mem_wal::util::PK_INDEX_DIR;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// Default-plugin registry, used only to load the standalone PK BTree by its
/// `BTreeIndexDetails` type. Built once.
static PK_BTREE_REGISTRY: LazyLock<Arc<IndexPluginRegistry>> =
    LazyLock::new(IndexPluginRegistry::with_default_plugins);

/// One newer generation's PK membership, used to decide whether it shadows an
/// older source's row.
#[derive(Clone, Debug)]
pub enum GenMembership {
    /// Probe the in-memory memtable's primary-key index, bounded to its visible
    /// prefix (so a not-yet-visible write can't shadow an older visible copy).
    InMemory {
        index_store: Arc<IndexStore>,
        /// Inclusive visible row watermark; `None` when no rows are visible.
        max_visible_row: Option<u64>,
    },
    /// Probe the SSTable's standalone on-disk PK BTree.
    OnDisk(Arc<dyn ScalarIndex>),
}

impl GenMembership {
    /// Whether this generation visibly contains the primary `key` — the typed
    /// value for a single-column PK, the encoded `Binary` tuple for a composite
    /// one (built by [`on_disk_pk_key`]). The same key probes the in-memory
    /// BTree and the flushed on-disk BTree, which now share a key space.
    pub async fn contains(&self, key: &ScalarValue) -> Result<bool> {
        match self {
            Self::InMemory {
                index_store,
                max_visible_row,
            } => Ok(max_visible_row.is_some_and(|max| index_store.pk_contains_key(key, max))),
            Self::OnDisk(index) => {
                let result = index
                    .search(&SargableQuery::Equals(key.clone()), &NoOpMetricsCollector)
                    .await
                    .map_err(|e| Error::io(e.to_string()))?;
                Ok(!search_is_empty(&result))
            }
        }
    }

    /// Batched [`Self::contains`]: for each key in `keys`, whether this
    /// generation visibly contains it, returned as a mask aligned to `keys`.
    ///
    /// One probe replaces N. The on-disk arm issues a single
    /// [`BTreeIndex::contains_keys`] (no per-key `SearchResult` allocation); the
    /// in-memory arm maps the sync, allocation-free PK lookup over the slice.
    /// Keys are in the index's key space (see [`on_disk_pk_key`]).
    pub async fn contains_keys(&self, keys: &[ScalarValue]) -> Result<Vec<bool>> {
        match self {
            Self::InMemory {
                index_store,
                max_visible_row,
            } => Ok(keys
                .iter()
                .map(|key| max_visible_row.is_some_and(|max| index_store.pk_contains_key(key, max)))
                .collect()),
            Self::OnDisk(index) => {
                // The flushed PK sidecar is always a BTree (built via
                // `PK_BTREE_REGISTRY`); downcast to reach the batched probe.
                let btree = index.as_any().downcast_ref::<BTreeIndex>().ok_or_else(|| {
                    Error::io("flushed PK dedup index is not a BTree".to_string())
                })?;
                btree
                    .contains_keys(keys, &NoOpMetricsCollector)
                    .await
                    .map_err(|e| Error::io(e.to_string()))
            }
        }
    }

    /// Whether this generation has no (visible) membership — used to skip adding
    /// an empty blocked set. An SSTable always has rows (flush rejects
    /// an empty memtable), so it is never empty.
    fn is_empty(&self) -> bool {
        match self {
            Self::InMemory {
                index_store,
                max_visible_row,
            } => max_visible_row.is_none() || index_store.pk_is_empty(),
            Self::OnDisk(_) => false,
        }
    }
}

/// Whether a scalar search returned no rows (existence test for the block-list).
fn search_is_empty(result: &SearchResult) -> bool {
    match result {
        SearchResult::Exact(set) | SearchResult::AtMost(set) | SearchResult::AtLeast(set) => {
            set.is_empty()
        }
    }
}

/// The probe key for the on-disk PK BTree: a single-column PK indexes its typed
/// value directly; a composite PK indexes the order-preserving encoded tuple as
/// `Binary` (matching what flush wrote — see [`encode_pk_tuple`]).
pub fn on_disk_pk_key(values: &[ScalarValue]) -> Result<ScalarValue> {
    match values {
        [single] => Ok(single.clone()),
        _ => Ok(ScalarValue::Binary(Some(encode_pk_tuple(values)?))),
    }
}

/// Per-source blocked memberships, keyed by `(shard_id, generation)`. Each value
/// is the memberships of the generations newer than that source.
pub type SourceBlockLists = HashMap<(Option<Uuid>, LsmGeneration), Vec<GenMembership>>;

/// A shard's generations paired with their membership, before sorting.
type ShardGenSets = HashMap<Uuid, Vec<(LsmGeneration, GenMembership)>>;

/// Per-source `NEWER(G)`, keyed by `(shard_id, generation)`. Generations are
/// per-shard, so a source is superseded only by strictly-newer generations of
/// the **same** shard — it never appears in its own blocked list. The base table
/// is shardless (`None`, oldest) and superseded by every non-base generation.
/// Only superseded sources get an entry; the newest of each shard never does.
pub async fn compute_source_block_lists(
    sources: &[LsmDataSource],
    session: Option<&Arc<Session>>,
    store_params: Option<&ObjectStoreParams>,
    sstable_cache: Option<&Arc<dyn DatasetCache>>,
) -> Result<SourceBlockLists> {
    // Membership per non-base source, grouped by shard (generations are
    // per-shard, so supersession is within-shard only).
    let mut by_shard: ShardGenSets = HashMap::new();
    let mut has_base = false;
    // Flushed PK-BTree opens are cold S3 reads; overlap them with
    // `try_join_all`. Order is irrelevant — gens are sorted per-shard below.
    let mut sstable_loads = Vec::new();
    for source in sources {
        match source {
            LsmDataSource::BaseTable { .. } => has_base = true,
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                shard_id,
                generation,
                ..
            } => {
                let membership = in_memory_membership(batch_store, index_store);
                by_shard
                    .entry(*shard_id)
                    .or_default()
                    .push((*generation, membership));
            }
            LsmDataSource::SsTable {
                path,
                shard_id,
                generation,
                ..
            } => sstable_loads.push(async move {
                let index = open_pk_index(path, session, store_params, sstable_cache).await?;
                Ok::<_, Error>((*shard_id, *generation, GenMembership::OnDisk(index)))
            }),
        }
    }
    for (shard_id, generation, membership) in futures::future::try_join_all(sstable_loads).await? {
        by_shard
            .entry(shard_id)
            .or_default()
            .push((generation, membership));
    }

    let mut blocked: SourceBlockLists = HashMap::new();
    // Base (shardless, oldest) is superseded by every non-base generation.
    let mut base_blocked: Vec<GenMembership> = Vec::new();
    for (shard, mut gens) in by_shard {
        // Newest-first: a gen's blocked list is its own shard's newer gens.
        gens.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
        let mut newer: Vec<GenMembership> = Vec::new();
        for (generation, membership) in gens {
            if !newer.is_empty() {
                blocked.insert((Some(shard), generation), newer.clone());
            }
            if !membership.is_empty() {
                base_blocked.push(membership.clone());
                newer.push(membership);
            }
        }
    }
    if has_base && !base_blocked.is_empty() {
        blocked.insert((None, LsmGeneration::BASE_TABLE), base_blocked);
    }
    Ok(blocked)
}

/// The fresh-tier block-list: one [`GenMembership`] per generation that shadows
/// the base table — active + frozen memtables (probed against their index) and
/// SSTables (probed against their on-disk PK BTree). A base/external
/// reader can test any PK against these (via [`GenMembership::contains`]) to
/// decide whether the fresh tier shadows it. The base source, if present, is
/// skipped (it is what gets shadowed).
///
/// When `watermarks` carries a watermark for a source's shard, membership is
/// bounded to it (see [`FreshTierWatermark`]): higher generations are excluded,
/// the active generation is bounded to its first `active_batch_count` batches,
/// and lower generations (frozen and flushed) are immutable and included whole.
/// A shard absent from `watermarks` (or `watermarks == None`) uses the live tier.
pub async fn fresh_tier_block_list(
    sources: &[LsmDataSource],
    session: Option<&Arc<Session>>,
    store_params: Option<&ObjectStoreParams>,
    sstable_cache: Option<&Arc<dyn DatasetCache>>,
    watermarks: Option<&HashMap<Uuid, FreshTierWatermark>>,
) -> Result<Vec<GenMembership>> {
    // Membership per source, in source order (`None` = skipped). Flushed
    // PK-BTree opens are cold S3 reads, so collect them tagged with their slot
    // and overlap with `try_join_all` rather than opening one at a time.
    let mut slots: Vec<Option<GenMembership>> = Vec::with_capacity(sources.len());
    let mut sstable_loads = Vec::new();
    for source in sources {
        match source {
            LsmDataSource::BaseTable { .. } => slots.push(None),
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                shard_id,
                generation,
                ..
            } => {
                let membership = match watermarks.and_then(|m| m.get(shard_id)) {
                    None => Some(in_memory_membership(batch_store, index_store)),
                    Some(watermark) => {
                        let g = generation.as_u64();
                        if g > watermark.active_generation {
                            // Rolled in after the snapshot; the arm never saw it.
                            None
                        } else if g == watermark.active_generation {
                            // Bound the active generation to the batches the arm saw.
                            Some(bounded_in_memory_membership(
                                batch_store,
                                index_store,
                                watermark.active_batch_count,
                            ))
                        } else {
                            // Lower (frozen) generations are immutable — include all.
                            Some(in_memory_membership(batch_store, index_store))
                        }
                    }
                };
                slots.push(membership);
            }
            LsmDataSource::SsTable {
                path,
                shard_id,
                generation,
                ..
            } => {
                // A generation at or above the active one was flushed after the
                // snapshot; exclude it. Lower generations are immutable. The
                // `==` case is the active generation flushed between the two
                // reads: excluding the flushed copy loses nothing, since its
                // rows are already captured by the in-memory arm above (bounded
                // to `active_batch_count`).
                let flushed_after_snapshot = watermarks
                    .and_then(|m| m.get(shard_id))
                    .is_some_and(|watermark| generation.as_u64() >= watermark.active_generation);
                if flushed_after_snapshot {
                    slots.push(None);
                } else {
                    let slot = slots.len();
                    slots.push(None);
                    sstable_loads.push(async move {
                        let index =
                            open_pk_index(path, session, store_params, sstable_cache).await?;
                        Ok::<_, Error>((slot, GenMembership::OnDisk(index)))
                    });
                }
            }
        }
    }
    for (slot, membership) in futures::future::try_join_all(sstable_loads).await? {
        slots[slot] = Some(membership);
    }
    Ok(slots
        .into_iter()
        .flatten()
        .filter(|membership| !membership.is_empty())
        .collect())
}

/// Cross-source membership of an in-memory (active / frozen) memtable: a
/// snapshot-bounded probe of its maintained primary-key index. A memtable
/// without a primary-key index can't be probed, so it blocks nothing — the
/// production vector-search path always enables the index.
fn in_memory_membership(
    batch_store: &Arc<BatchStore>,
    index_store: &Arc<IndexStore>,
) -> GenMembership {
    let max_visible_row = batch_store.max_visible_row(index_store.visible_count());
    GenMembership::InMemory {
        index_store: index_store.clone(),
        max_visible_row,
    }
}

/// As-of variant of [`in_memory_membership`] for the active generation under a
/// watermark: bounds visibility to the first `batch_count` batches — those a
/// prior scan observed before the memtable grew. A later append lands at a
/// higher row position and is excluded by the probe, so it can't shadow a base
/// row whose replacement the scan never delivered. `batch_count == 0` leaves the
/// membership empty.
fn bounded_in_memory_membership(
    batch_store: &Arc<BatchStore>,
    index_store: &Arc<IndexStore>,
    batch_count: u64,
) -> GenMembership {
    // `batch_count` is already an exclusive count, and so is what
    // `max_visible_row` takes, so it passes straight through: no
    // count-to-inclusive-position conversion, and `0` yields `None` on its own.
    let max_visible_row = batch_store.max_visible_row(batch_count as usize);
    GenMembership::InMemory {
        index_store: index_store.clone(),
        max_visible_row,
    }
}

/// Open the standalone PK BTree at `{SSTable gen}/_pk_index` for one
/// SSTable. Reuses the SSTable dataset's (session-configured) object store
/// and **its index cache**, then loads the sidecar directly by path through the
/// BTree plugin — it is not a manifest index. The opened index and its pages
/// are cached in the session's index cache (keyed by the immutable SSTable
/// path), so repeated probes reuse them with no separate cache path and no
/// upfront scan; concurrent first-opens may each load before the cache fills.
/// A stable cache UUID for a non-manifest index identified only by its path.
///
/// `DSIndexCache::for_index` keys by `&Uuid`, but the flushed PK sidecar has no
/// manifest UUID — its identity is its immutable path. Derive a deterministic
/// UUID from the path so the cache namespace is per-path and stable across
/// probes (the `uuid` crate lacks the `v5` "name-based" feature here, so hash to
/// a `u128` instead).
fn path_cache_uuid(path: &str) -> Uuid {
    use std::hash::{Hash, Hasher};
    let mut lo = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut lo);
    let mut hi = std::collections::hash_map::DefaultHasher::new();
    // Seed the high half differently so it never equals the low half.
    "lance/flushed-pk-index".hash(&mut hi);
    path.hash(&mut hi);
    Uuid::from_u128(((hi.finish() as u128) << 64) | lo.finish() as u128)
}

async fn open_pk_index(
    path: &str,
    session: Option<&Arc<Session>>,
    store_params: Option<&ObjectStoreParams>,
    sstable_cache: Option<&Arc<dyn DatasetCache>>,
) -> Result<Arc<dyn ScalarIndex>> {
    let dataset = open_sstable(path, session, store_params, sstable_cache, None).await?;
    // Namespace the session index cache by the (immutable) SSTable path so this
    // sidecar's pages live alongside every other index instead of a bespoke
    // cache. `fri_uuid` is None — SSTables carry no fragment-reuse.
    let index_cache = dataset.index_cache.for_index(&path_cache_uuid(path), None);
    let index_dir = dataset.base.clone().join(PK_INDEX_DIR);
    let store: Arc<dyn ScalarIndexStore> = Arc::new(LanceIndexStore::new(
        dataset.object_store.clone(),
        index_dir,
        Arc::new(index_cache.clone()),
    ));

    let plugin = PK_BTREE_REGISTRY.get_plugin_by_name("BTree")?;
    // Cache the opened index in the session cache (mirrors `open_scalar_index`).
    if let Some(index) = plugin
        .get_from_cache(store.clone(), None, &index_cache)
        .await?
    {
        return Ok(index);
    }
    let details = prost_types::Any::from_msg(&lance_index::pbold::BTreeIndexDetails::default())
        .map_err(|e| Error::io(e.to_string()))?;
    let index = plugin
        .load_index(store, &details, None, &index_cache)
        .await?;
    plugin.put_in_cache(&index_cache, index.clone()).await?;
    Ok(index)
}

/// Write an SSTable's standalone PK sidecar at `{uri}/_pk_index` from
/// `batches`, mirroring what flush does in production. `pk_columns` are the
/// primary-key column names (field ids are synthesized by position — `insert`
/// resolves columns by name). A no-op when no batch carries the PK columns.
///
/// Used by Rust scanner tests and by the Python test-support binding to stage
/// faithful SSTables (an SSTable dataset alone, with no sidecar, is
/// not a state production ever produces).
pub async fn write_pk_sidecar(
    uri: &str,
    batches: &[arrow_array::RecordBatch],
    pk_columns: &[&str],
) -> Result<()> {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use lance_core::cache::LanceCache;
    use lance_index::scalar::btree::train_btree_index;
    use lance_io::object_store::ObjectStore;

    use crate::dataset::mem_wal::util::pk_index_path;

    let pk: Vec<(String, i32)> = pk_columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.to_string(), i as i32))
        .collect();
    let mut index = IndexStore::new();
    index.enable_pk_index(&pk);
    let mut offset = 0u64;
    for batch in batches {
        index.insert(batch, offset)?;
        offset += batch.num_rows() as u64;
    }

    let training = index.pk_training_batches(8192)?;
    if training.is_empty() {
        return Ok(());
    }
    let schema = training[0].schema();
    let (object_store, base_path) = ObjectStore::from_uri(uri).await?;
    let store = LanceIndexStore::new(
        object_store,
        pk_index_path(&base_path),
        Arc::new(LanceCache::no_cache()),
    );
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(training.into_iter().map(Ok)),
    ));
    // `train_btree_index` now returns the written index files; the sidecar
    // writer only needs success/failure.
    train_btree_index(stream, &store, 8192, None, None).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::scanner::data_source::{LsmDataSource, LsmGeneration};
    use crate::dataset::mem_wal::write::IndexStore;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use uuid::Uuid;

    fn id_batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    /// An active/frozen memtable source whose PK index holds one row per id in
    /// `ids` (positions 0..n), all committed and visible.
    fn active_source(shard: Uuid, generation: u64, ids: &[i32]) -> LsmDataSource {
        let store = BatchStore::with_capacity(16);
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        for &id in ids {
            let b = id_batch(&[id]);
            let (bp, off, _) = store.append(b.clone()).unwrap();
            index.insert_with_batch_position(&b, off, Some(bp)).unwrap();
        }
        LsmDataSource::ActiveMemTable {
            batch_store: Arc::new(store),
            index_store: Arc::new(index),
            schema: id_batch(&[1]).schema(),
            shard_id: shard,
            generation: LsmGeneration::memtable(generation),
        }
    }

    /// Whether `id`'s PK is blocked by any of a source's newer-gen memberships.
    async fn blocks(memberships: &[GenMembership], id: i32) -> bool {
        let key = on_disk_pk_key(&[ScalarValue::Int32(Some(id))]).unwrap();
        for m in memberships {
            if m.contains(&key).await.unwrap() {
                return true;
            }
        }
        false
    }

    #[test]
    fn on_disk_key_is_typed_for_single_and_binary_for_composite() {
        // Single-column → the typed value; composite → encoded Binary.
        let single = [ScalarValue::Int32(Some(7))];
        assert_eq!(
            on_disk_pk_key(&single).unwrap(),
            ScalarValue::Int32(Some(7))
        );
        let composite = [ScalarValue::Int32(Some(1)), ScalarValue::from("a")];
        assert!(matches!(
            on_disk_pk_key(&composite).unwrap(),
            ScalarValue::Binary(Some(_))
        ));
    }

    #[tokio::test]
    async fn fresh_tier_block_list_one_membership_per_in_memory_gen() {
        let shard = Uuid::new_v4();
        // Active gen 2: pk=1,2. Frozen gen 1: pk=3.
        let sources = vec![
            active_source(shard, 2, &[1, 2]),
            active_source(shard, 1, &[3]),
        ];

        let memberships = fresh_tier_block_list(&sources, None, None, None, None)
            .await
            .unwrap();

        // One membership per generation; together they cover pk=1,2,3 (not 4).
        assert_eq!(memberships.len(), 2);
        for id in [1, 2, 3] {
            assert!(blocks(&memberships, id).await);
        }
        assert!(!blocks(&memberships, 4).await);
    }

    #[tokio::test]
    async fn block_lists_suppress_stale_across_in_memory_gens() {
        let shard = Uuid::new_v4();
        // Frozen gen 1: stale pk=1. Active gen 2: pk=1 re-written, pk=2 new.
        let sources = vec![
            active_source(shard, 1, &[1]),
            active_source(shard, 2, &[1, 2]),
        ];

        let blocked = Box::pin(compute_source_block_lists(&sources, None, None, None))
            .await
            .unwrap();

        let g1 = LsmGeneration::memtable(1);
        let g2 = LsmGeneration::memtable(2);
        // The newer active write supersedes the frozen copy: gen 1 is blocked on
        // pk=1, so its KNN drops pk=1.
        assert!(blocks(&blocked[&(Some(shard), g1)], 1).await);
        // The active (newest) generation is superseded by nothing — no entry.
        assert!(!blocked.contains_key(&(Some(shard), g2)));
    }

    #[tokio::test]
    async fn block_lists_suppress_stale_base_row() {
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::RecordBatchIterator;

        // Base (gen 0): pk=1 (stale), pk=3 (live).
        let base_batch = id_batch(&[1, 3]);
        let schema = base_batch.schema();
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(base_batch)], schema.clone());
        let base = Arc::new(
            Dataset::write(reader, &uri, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        // Active gen 1: pk=1 re-written, pk=2 new.
        let sources = vec![
            LsmDataSource::BaseTable { dataset: base },
            active_source(Uuid::new_v4(), 1, &[1, 2]),
        ];

        let blocked = Box::pin(compute_source_block_lists(&sources, None, None, None))
            .await
            .unwrap();

        // Base is blocked by every newer gen: pk=1 (re-written in gen 1) is
        // blocked, pk=3 (base-only) is not.
        let base_blocked = blocked
            .get(&(None, LsmGeneration::BASE_TABLE))
            .expect("base has a blocked set");
        assert!(blocks(base_blocked, 1).await);
        assert!(!blocks(base_blocked, 3).await);
    }

    #[tokio::test]
    async fn block_lists_are_keyed_per_shard() {
        // Regression: generations are per-shard, so a source must only be blocked
        // by newer generations of its OWN shard.
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        // Two shards, each: frozen gen 1 (stale) + active gen 2 (re-write).
        // Shard A keys pk=1; shard B keys pk=2 (disjoint partitions).
        let sources = vec![
            active_source(a, 1, &[1]),
            active_source(a, 2, &[1]),
            active_source(b, 1, &[2]),
            active_source(b, 2, &[2]),
        ];

        let blocked = Box::pin(compute_source_block_lists(&sources, None, None, None))
            .await
            .unwrap();

        let g1 = LsmGeneration::memtable(1);
        let g2 = LsmGeneration::memtable(2);
        // Each shard's gen 1 is blocked by its OWN gen 2 only.
        assert!(blocks(&blocked[&(Some(a), g1)], 1).await);
        assert!(!blocks(&blocked[&(Some(a), g1)], 2).await);
        assert!(blocks(&blocked[&(Some(b), g1)], 2).await);
        assert!(!blocks(&blocked[&(Some(b), g1)], 1).await);
        // The newest generation of each shard is superseded by nothing.
        assert!(!blocked.contains_key(&(Some(a), g2)));
        assert!(!blocked.contains_key(&(Some(b), g2)));
    }

    #[tokio::test]
    async fn index_membership_is_snapshot_bounded() {
        // The index-sourced membership only counts a PK whose version is visible
        // at the source's watermark, so a newer generation's not-yet-visible
        // write can't shadow an older generation's visible copy.
        let shard = Uuid::new_v4();
        let schema = id_batch(&[1]).schema();

        // Older frozen gen 1: pk=1.
        let g1 = active_source(shard, 1, &[1]);

        // Newer active gen 2: pk=99 visible at position 0, then pk=1 written at
        // position 1 but with the watermark left at batch 0 (so pk=1 is in the
        // index yet not visible) — the concurrent-write race.
        let g2_store = BatchStore::with_capacity(8);
        let mut g2_index = IndexStore::new();
        g2_index.enable_pk_index(&[("id".to_string(), 0)]);
        let b0 = id_batch(&[99]);
        let (bp0, off0, _) = g2_store.append(b0.clone()).unwrap();
        g2_index
            .insert_with_batch_position(&b0, off0, Some(bp0)) // advances watermark to 0
            .unwrap();
        let b1 = id_batch(&[1]);
        let (_, off1, _) = g2_store.append(b1.clone()).unwrap();
        g2_index
            .insert_with_batch_position(&b1, off1, None) // index updated, watermark unchanged
            .unwrap();
        let g2 = LsmDataSource::ActiveMemTable {
            batch_store: Arc::new(g2_store),
            index_store: Arc::new(g2_index),
            schema,
            shard_id: shard,
            generation: LsmGeneration::memtable(2),
        };

        let blocked = Box::pin(compute_source_block_lists(&[g1, g2], None, None, None))
            .await
            .unwrap();

        let g1_block = &blocked[&(Some(shard), LsmGeneration::memtable(1))];
        // pk=99 is visible in gen 2 → it blocks gen 1's pk=99.
        assert!(blocks(g1_block, 99).await);
        // pk=1's only gen-2 copy is not yet visible → it must NOT shadow gen 1.
        assert!(
            !blocks(g1_block, 1).await,
            "a not-yet-visible newer write must not shadow an older visible copy"
        );
    }

    /// A fresh-tier watermark bounds the active generation to the first
    /// `active_batch_count` batches — those the arm observed before the memtable
    /// grew. A later append is invisible, so a base row is never dropped without
    /// the arm having delivered its replacement.
    #[tokio::test]
    async fn fresh_tier_watermark_bounds_active_memtable_by_batch_count() {
        use crate::dataset::mem_wal::scanner::data_source::FreshTierWatermark;
        use std::collections::HashMap;

        let shard = Uuid::new_v4();
        // Three single-row batches: pk=1 at batch 0, pk=2 at batch 1, pk=3 at
        // batch 2 (appended after the arm).
        let sources = vec![active_source(shard, 1, &[1, 2, 3])];

        // Watermark at 2 batches of gen 1: pk=1,2 are members; pk=3 (batch 2) is not.
        let watermarks: HashMap<Uuid, FreshTierWatermark> = [(
            shard,
            FreshTierWatermark {
                active_generation: 1,
                active_batch_count: 2,
            },
        )]
        .into_iter()
        .collect();
        let sets = fresh_tier_block_list(&sources, None, None, None, Some(&watermarks))
            .await
            .unwrap();
        assert!(blocks(&sets, 1).await);
        assert!(blocks(&sets, 2).await);
        assert!(!blocks(&sets, 3).await);

        // No watermark → live tier: all three are members.
        let sets = fresh_tier_block_list(&sources, None, None, None, None)
            .await
            .unwrap();
        for id in [1, 2, 3] {
            assert!(blocks(&sets, id).await);
        }
    }

    /// A generation above the active one rolled in after the snapshot and is
    /// excluded whole; a lower one is immutable (frozen) and included whole
    /// regardless of the active batch count.
    #[tokio::test]
    async fn fresh_tier_watermark_excludes_newer_gen_includes_lower_gen() {
        use crate::dataset::mem_wal::scanner::data_source::FreshTierWatermark;
        use std::collections::HashMap;

        let shard = Uuid::new_v4();
        // gen 3 newer (after snapshot), gen 2 == active (bounded to 1 batch),
        // gen 1 lower/immutable (whole). Each id is its own batch.
        let sources = vec![
            active_source(shard, 3, &[100]),
            active_source(shard, 2, &[20, 21]),
            active_source(shard, 1, &[1, 2]),
        ];

        let watermarks: HashMap<Uuid, FreshTierWatermark> = [(
            shard,
            FreshTierWatermark {
                active_generation: 2,
                active_batch_count: 1,
            },
        )]
        .into_iter()
        .collect();
        let sets = fresh_tier_block_list(&sources, None, None, None, Some(&watermarks))
            .await
            .unwrap();
        assert!(blocks(&sets, 1).await); // gen 1, whole
        assert!(blocks(&sets, 2).await); // gen 1, whole
        assert!(blocks(&sets, 20).await); // gen 2, batch 0
        assert!(!blocks(&sets, 21).await); // gen 2, batch 1 — past the watermark
        assert!(!blocks(&sets, 100).await); // gen 3 — after the snapshot
    }

    /// An SSTable at or above the active generation was produced by a
    /// flush after the snapshot and is excluded; one strictly below it is
    /// immutable and included.
    #[tokio::test]
    async fn fresh_tier_watermark_excludes_sstables_at_or_above_active() {
        use crate::dataset::mem_wal::scanner::data_source::FreshTierWatermark;
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::RecordBatchIterator;
        use std::collections::HashMap;

        // An SSTable 2 holding pk=5, staged as an SSTable dataset with
        // its standalone PK sidecar (what the on-disk membership probes).
        let sstable_batch = id_batch(&[5]);
        let schema = sstable_batch.schema();
        let tmp = tempfile::tempdir().unwrap();
        let path = format!("{}/gen2", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(sstable_batch.clone())], schema.clone());
        Dataset::write(reader, &path, Some(WriteParams::default()))
            .await
            .unwrap();
        write_pk_sidecar(&path, &[sstable_batch], &["id"])
            .await
            .unwrap();

        let shard = Uuid::new_v4();
        let sources = vec![LsmDataSource::SsTable {
            path,
            shard_id: shard,
            generation: LsmGeneration::memtable(2),
        }];

        // active_generation 2 (gen 2 flushed at/after the snapshot): excluded.
        let at: HashMap<Uuid, FreshTierWatermark> = [(
            shard,
            FreshTierWatermark {
                active_generation: 2,
                active_batch_count: u64::MAX,
            },
        )]
        .into_iter()
        .collect();
        let sets = fresh_tier_block_list(&sources, None, None, None, Some(&at))
            .await
            .unwrap();
        assert!(!blocks(&sets, 5).await);

        // active_generation 3 (gen 2 strictly below, immutable): included.
        let above: HashMap<Uuid, FreshTierWatermark> = [(
            shard,
            FreshTierWatermark {
                active_generation: 3,
                active_batch_count: u64::MAX,
            },
        )]
        .into_iter()
        .collect();
        let sets = fresh_tier_block_list(&sources, None, None, None, Some(&above))
            .await
            .unwrap();
        assert!(blocks(&sets, 5).await);
    }
}
