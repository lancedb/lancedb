// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! In-memory MemTable for buffering writes.

pub mod batch_store;
pub mod flush;
pub mod scanner;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, RecordBatch, RecordBatchIterator};
use arrow_schema::Schema as ArrowSchema;
use lance_core::datatypes::Schema;
use lance_core::utils::bloomfilter::sbbf::Sbbf;
use lance_core::{Error, Result};
use tokio::sync::RwLock;
use tracing::instrument;
use uuid::Uuid;

use super::index::IndexStore;
use super::util::{WatchableOnceCell, WatchableOnceCellReader};
use super::wal::WalFlushFailure;
use super::write::{DurabilityResult, WalFlushResult};
use crate::Dataset;
use batch_store::BatchStore;

/// Default batch store capacity when not specified.
const DEFAULT_BATCH_CAPACITY: usize = 1024;

/// Configuration for the reader cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Time-to-live for cached Dataset. Default: 60 seconds.
    pub ttl: Duration,
    /// Whether to always return fresh data (bypass cache). Default: false.
    pub always_fresh: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
            always_fresh: false,
        }
    }
}

/// In-memory table for buffering writes.
///
/// Stores Arrow RecordBatches in a lock-free append-only structure for O(1) operations.
/// Dataset is constructed on-demand for reading with configurable caching.
///
/// # Thread Safety
///
/// - **Writer**: Only one thread should call `insert_with_seq()` at a time.
///   This is enforced by the WriteBatchHandler architecture.
/// - **Readers**: Multiple threads can safely call read methods concurrently.
pub struct MemTable {
    /// Schema for this MemTable.
    schema: Arc<ArrowSchema>,
    /// Lance schema (for index operations).
    lance_schema: Schema,

    /// Lock-free batch storage.
    /// Wrapped in Arc for sharing with scanners.
    batch_store: Arc<BatchStore>,

    /// Unique URI for on-demand Dataset construction.
    dataset_uri: String,

    /// Cache configuration for reading.
    cache_config: CacheConfig,
    /// Cached Dataset for reading (with eventual consistency).
    cached_dataset: RwLock<Option<CachedDataset>>,

    /// Generation number (incremented on flush).
    generation: u64,

    /// Primary key bloom filter for staleness detection.
    pk_bloom_filter: Sbbf,
    /// Primary key field IDs (for bloom filter updates).
    pk_field_ids: Vec<i32>,

    /// Index registry (optional, for indexed writes).
    /// Wrapped in Arc for sharing with async index handler.
    indexes: Option<Arc<IndexStore>>,

    /// WAL entry position when this memtable was frozen.
    /// Used for WAL replay starting point during recovery.
    /// None means the memtable is still active (not frozen).
    frozen_at_wal_entry_position: Option<u64>,

    /// Reader for WAL flush completion notification.
    /// Set when the memtable is frozen and a WAL flush request is sent.
    /// The reader can be awaited to know when WAL flush is complete.
    /// Uses Mutex for interior mutability since the MemTable is wrapped in Arc when frozen.
    /// Uses `WalFlushFailure` (not `Error`) since `lance_core::Error` doesn't
    /// implement Clone; the carrier preserves the fence reason for the waiter.
    wal_flush_completion: std::sync::Mutex<
        Option<WatchableOnceCellReader<std::result::Result<WalFlushResult, WalFlushFailure>>>,
    >,

    /// Cell for memtable flush completion notification.
    /// Created when the memtable is frozen and set with a value when the flush completes.
    /// Used by backpressure to wait for oldest memtable flush completion.
    memtable_flush_completion: std::sync::Mutex<Option<WatchableOnceCell<DurabilityResult>>>,
}

/// Cached Dataset with timestamp for eventual consistency.
struct CachedDataset {
    dataset: Dataset,
    created_at: Instant,
    batch_count: usize,
}

/// Default expected items for primary key bloom filter.
/// Consistent with lance-index scalar bloomfilter defaults.
const PK_BLOOM_FILTER_EXPECTED_ITEMS: u64 = 8192;

/// Default false positive probability for primary key bloom filter.
/// Consistent with lance-index scalar bloomfilter defaults (≈ 1 in 1754).
const PK_BLOOM_FILTER_FPP: f64 = 0.00057;

impl MemTable {
    /// Create a new MemTable with default capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for the data
    /// * `generation` - Initial generation number (typically 1 for new, or from recovery)
    /// * `pk_field_ids` - Field IDs that form the primary key (for bloom filter)
    pub fn new(schema: Arc<ArrowSchema>, generation: u64, pk_field_ids: Vec<i32>) -> Result<Self> {
        Self::with_capacity(
            schema,
            generation,
            pk_field_ids,
            CacheConfig::default(),
            DEFAULT_BATCH_CAPACITY,
        )
    }

    /// Create a new MemTable with custom cache configuration.
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for the data
    /// * `generation` - Initial generation number (typically 1 for new, or from recovery)
    /// * `pk_field_ids` - Field IDs that form the primary key (for bloom filter)
    /// * `cache_config` - Configuration for reader cache (TTL, freshness)
    pub fn with_cache_config(
        schema: Arc<ArrowSchema>,
        generation: u64,
        pk_field_ids: Vec<i32>,
        cache_config: CacheConfig,
    ) -> Result<Self> {
        Self::with_capacity(
            schema,
            generation,
            pk_field_ids,
            cache_config,
            DEFAULT_BATCH_CAPACITY,
        )
    }

    /// Create a new MemTable with custom capacity.
    ///
    /// # Arguments
    ///
    /// * `schema` - Arrow schema for the data
    /// * `generation` - Initial generation number (typically 1 for new, or from recovery)
    /// * `pk_field_ids` - Field IDs that form the primary key (for bloom filter)
    /// * `cache_config` - Configuration for reader cache (TTL, freshness)
    /// * `batch_capacity` - Maximum number of batches before flush is required
    pub fn with_capacity(
        schema: Arc<ArrowSchema>,
        generation: u64,
        pk_field_ids: Vec<i32>,
        cache_config: CacheConfig,
        batch_capacity: usize,
    ) -> Result<Self> {
        Self::with_capacity_at(
            schema,
            generation,
            pk_field_ids,
            cache_config,
            batch_capacity,
            0,
        )
    }

    /// Create a memtable whose batch 0 sits at `global_offset` in the writer's
    /// batch sequence. Every memtable after the writer's first is rotated in by
    /// `freeze_memtable`, which stamps the outgoing memtable's `global_end()`
    /// here so writer-global cursors stay mappable onto local batch positions.
    #[allow(clippy::too_many_arguments)]
    pub fn with_capacity_at(
        schema: Arc<ArrowSchema>,
        generation: u64,
        pk_field_ids: Vec<i32>,
        cache_config: CacheConfig,
        batch_capacity: usize,
        global_offset: usize,
    ) -> Result<Self> {
        let lance_schema = Schema::try_from(schema.as_ref())?;

        // Initialize bloom filter for primary key staleness detection.
        let pk_bloom_filter =
            Sbbf::with_ndv_fpp(PK_BLOOM_FILTER_EXPECTED_ITEMS, PK_BLOOM_FILTER_FPP).map_err(
                |e| {
                    Error::io(format!(
                        "Failed to create bloom filter for primary key: {}",
                        e
                    ))
                },
            )?;

        // Generate unique URI for on-demand Dataset construction
        let dataset_uri = format!("memory://{}", Uuid::new_v4());

        // Create lock-free batch store
        let batch_store = Arc::new(BatchStore::with_capacity_at(batch_capacity, global_offset));

        // Create memtable_flush_completion cell immediately so backpressure can
        // wait on it even before the memtable is frozen. Every memtable will
        // eventually be frozen and flushed.
        let memtable_flush_cell = WatchableOnceCell::new();

        Ok(Self {
            schema,
            lance_schema,
            batch_store,
            dataset_uri,
            cache_config,
            cached_dataset: RwLock::new(None),
            generation,
            pk_bloom_filter,
            pk_field_ids,
            // Initialize with an empty IndexStore so the visibility cursor has
            // a stable Arc shared between the scanner (via `indexes_arc()`)
            // and the WAL flush handler. Replaced via `set_indexes_arc` if
            // index configs are supplied. Without this, the no-index path
            // would hand out fresh empty IndexStores to scanners and the WAL
            // flush handler, breaking cursor advance — see the regression
            // test `test_unindexed_memtable_visibility_after_flush`.
            indexes: Some(Arc::new(IndexStore::new())),
            frozen_at_wal_entry_position: None,
            wal_flush_completion: std::sync::Mutex::new(None),
            memtable_flush_completion: std::sync::Mutex::new(Some(memtable_flush_cell)),
        })
    }

    /// Set the index registry for indexed writes.
    pub fn set_indexes(&mut self, indexes: IndexStore) {
        self.indexes = Some(Arc::new(indexes));
    }

    /// Set the index registry with an Arc (for sharing with async handler).
    pub fn set_indexes_arc(&mut self, indexes: Arc<IndexStore>) {
        self.indexes = Some(indexes);
    }

    /// Mark this memtable as frozen with the given WAL entry position.
    ///
    /// Once frozen, no new writes should be added. The memtable will be
    /// added to the immutable queue for flushing to Lance storage.
    ///
    /// # Arguments
    ///
    /// * `wal_entry_position` - The last WAL entry position when this memtable was frozen
    pub fn freeze(&mut self, wal_entry_position: u64) {
        self.frozen_at_wal_entry_position = Some(wal_entry_position);
    }

    /// Set the WAL flush completion reader.
    ///
    /// Called when a WAL flush request is sent at freeze time.
    /// The reader can be awaited by flush_oldest_immutable to know when
    /// the WAL flush is complete.
    pub fn set_wal_flush_completion(
        &self,
        reader: WatchableOnceCellReader<std::result::Result<WalFlushResult, WalFlushFailure>>,
    ) {
        *self.wal_flush_completion.lock().unwrap() = Some(reader);
    }

    /// Take the WAL flush completion reader.
    ///
    /// Returns the reader if set, consuming it. Used by flush_oldest_immutable
    /// to await WAL flush completion before proceeding with memtable flush.
    /// Thread-safe via interior mutability.
    pub fn take_wal_flush_completion(
        &self,
    ) -> Option<WatchableOnceCellReader<std::result::Result<WalFlushResult, WalFlushFailure>>> {
        self.wal_flush_completion.lock().unwrap().take()
    }

    /// Check if this memtable has a pending WAL flush completion to await.
    pub fn has_pending_wal_flush(&self) -> bool {
        self.wal_flush_completion.lock().unwrap().is_some()
    }

    /// Get a reader for the memtable flush completion.
    ///
    /// The cell is created at memtable construction time, so this always
    /// returns a reader. This allows backpressure to wait on the active
    /// memtable's flush completion, not just frozen memtables.
    ///
    /// # Panics
    ///
    /// Panics if called after `signal_memtable_flush_complete()` has consumed the cell.
    pub fn create_memtable_flush_completion(&self) -> WatchableOnceCellReader<DurabilityResult> {
        self.memtable_flush_completion
            .lock()
            .unwrap()
            .as_ref()
            .expect("memtable_flush_completion cell should exist (created at construction)")
            .reader()
    }

    /// Get a reader for the memtable flush completion.
    ///
    /// Returns a reader if the completion cell exists, without consuming it.
    /// Multiple readers can be obtained from the same cell.
    pub fn get_memtable_flush_watcher(&self) -> Option<WatchableOnceCellReader<DurabilityResult>> {
        self.memtable_flush_completion
            .lock()
            .unwrap()
            .as_ref()
            .map(|cell| cell.reader())
    }

    /// Signal that the memtable flush has finished, with the outcome.
    ///
    /// Must be called whether the flush succeeded or failed — otherwise the
    /// watch channel is dropped without a value and any awaiting watcher
    /// (e.g. `ShardWriter::wait_for_flush_drain`) sees the closed channel
    /// and returns `Err` instead of receiving the actual outcome.
    pub fn signal_memtable_flush_complete(&self, result: DurabilityResult) {
        if let Some(cell) = self.memtable_flush_completion.lock().unwrap().take() {
            cell.write(result);
        }
    }

    /// Get the WAL entry position when this memtable was frozen.
    ///
    /// Returns `None` if the memtable is still active (not frozen).
    pub fn frozen_at_wal_entry_position(&self) -> Option<u64> {
        self.frozen_at_wal_entry_position
    }

    /// Check if this memtable has been frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen_at_wal_entry_position.is_some()
    }

    /// Insert a record batch into the MemTable.
    ///
    /// O(1) append.
    ///
    /// # Returns
    ///
    /// The batch position (0-indexed) for the inserted batch.
    ///
    /// # Single Writer Requirement
    ///
    /// This method MUST only be called from the single writer task.
    #[instrument(name = "mt_insert", level = "debug", skip_all, fields(num_rows = batch.num_rows(), generation = self.generation))]
    pub async fn insert(&mut self, batch: RecordBatch) -> Result<usize> {
        // Validate schema compatibility
        if batch.schema() != self.schema {
            return Err(Error::invalid_input(
                "Batch schema doesn't match MemTable schema",
            ));
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Err(Error::invalid_input("Cannot insert empty batch"));
        }

        // Row offset is the current row count (before adding this batch)
        let row_offset = self.batch_store.total_rows() as u64;

        // Update bloom filter with primary keys
        self.update_bloom_filter(&batch)?;

        // Get batch position before appending (for index coverage tracking)
        let batch_position = self.batch_store.len();

        // Update indexes with batch position for coverage tracking
        if let Some(ref indexes) = self.indexes {
            indexes.insert_with_batch_position(&batch, row_offset, Some(batch_position))?;
        }

        // Append to batch store (returns batch_position, row_offset, estimated_size)
        let (batch_position, _row_offset, _estimated_size) =
            self.batch_store.append(batch).map_err(|_| {
                Error::invalid_input("MemTable batch store is full - should have been flushed")
            })?;

        Ok(batch_position)
    }

    /// Insert a batch without updating indexes.
    ///
    /// Index updates are performed during WAL flush by `WalFlushHandler`.
    ///
    /// Returns `(batch_position, row_offset, estimated_size)` so the caller can queue the index update.
    ///
    /// # Single Writer Requirement
    ///
    /// This method MUST only be called from the single writer task.
    pub async fn insert_batch_only(&mut self, batch: RecordBatch) -> Result<(usize, u64, usize)> {
        // Validate schema compatibility
        if batch.schema() != self.schema {
            return Err(Error::invalid_input(
                "Batch schema doesn't match MemTable schema",
            ));
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Err(Error::invalid_input("Cannot insert empty batch"));
        }

        // Update bloom filter with primary keys
        self.update_bloom_filter(&batch)?;

        // NOTE: Index update is skipped - caller will queue async update

        // Append to batch store (returns batch_position, row_offset, estimated_size)
        let (batch_position, row_offset, estimated_size) =
            self.batch_store.append(batch).map_err(|_| {
                Error::invalid_input("MemTable batch store is full - should have been flushed")
            })?;

        Ok((batch_position, row_offset, estimated_size))
    }

    /// Insert multiple batches without updating indexes.
    ///
    /// All batches are inserted atomically - readers see either none or all.
    /// Index updates are performed during WAL flush by `WalFlushHandler`.
    ///
    /// Returns `Vec<(batch_position, row_offset, estimated_size)>` for each batch.
    ///
    /// # Single Writer Requirement
    ///
    /// This method MUST only be called from the single writer task.
    #[instrument(name = "mt_insert_batches", level = "debug", skip_all, fields(batch_count = batches.len(), generation = self.generation))]
    pub async fn insert_batches_only(
        &mut self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<(usize, u64, usize)>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        // Validate all batches upfront
        for (i, batch) in batches.iter().enumerate() {
            if batch.schema() != self.schema {
                return Err(Error::invalid_input(format!(
                    "Batch {} schema doesn't match MemTable schema",
                    i
                )));
            }
            if batch.num_rows() == 0 {
                return Err(Error::invalid_input(format!("Batch {} is empty", i)));
            }
        }

        // Update bloom filter for all batches
        for batch in &batches {
            self.update_bloom_filter(batch)?;
        }

        // NOTE: Index update is skipped - caller will queue async update

        // Append all batches atomically
        let results = self.batch_store.append_batches(batches).map_err(|_| {
            Error::invalid_input("MemTable batch store is full - should have been flushed")
        })?;

        Ok(results)
    }

    /// Check if the MemTable should be flushed.
    ///
    /// Returns true if the batch store is full or estimated size exceeds threshold.
    pub fn should_flush(&self, max_bytes: usize) -> bool {
        self.batch_store.is_full() || self.batch_store.estimated_bytes() >= max_bytes
    }

    /// Get the batches in the visible prefix.
    ///
    /// A batch at position `i` is visible if `i < visible_count`.
    ///
    /// # Arguments
    ///
    /// * `visible_count` - Exclusive count of batch positions to include
    ///
    /// # Returns
    ///
    /// Vector of visible batches.
    pub async fn get_visible_batches(&self, visible_count: usize) -> Vec<RecordBatch> {
        self.batch_store.visible_record_batches(visible_count)
    }

    /// Get the batch positions in the visible prefix.
    ///
    /// This is useful for filtering index results by visibility.
    pub async fn get_visible_batch_positions(&self, visible_count: usize) -> Vec<usize> {
        self.batch_store.visible_batch_positions(visible_count)
    }

    /// Check if a specific batch is visible at a given visibility position.
    ///
    /// Returns true if the batch is visible, false if not visible or doesn't exist.
    pub async fn is_batch_visible(&self, batch_position: usize, visible_count: usize) -> bool {
        self.batch_store
            .is_batch_visible(batch_position, visible_count)
    }

    /// Scan batches visible up to a specific batch position.
    ///
    /// This combines `get_visible_batches` with the scan interface.
    pub async fn scan_batches_at_position(&self, visible_count: usize) -> Result<Vec<RecordBatch>> {
        Ok(self.get_visible_batches(visible_count).await)
    }

    /// Update the bloom filter with primary keys from a batch.
    fn update_bloom_filter(&mut self, batch: &RecordBatch) -> Result<()> {
        let bloom = &mut self.pk_bloom_filter;

        // Get primary key columns
        let pk_columns: Vec<_> = self
            .pk_field_ids
            .iter()
            .filter_map(|&field_id| {
                // Find column by field ID
                self.lance_schema
                    .fields
                    .iter()
                    .position(|f| f.id == field_id)
                    .and_then(|idx| batch.column(idx).clone().into())
            })
            .collect();

        if pk_columns.len() != self.pk_field_ids.len() {
            return Err(Error::invalid_input("Batch is missing primary key columns"));
        }

        // Insert each row's primary key hash
        for row_idx in 0..batch.num_rows() {
            let hash = compute_row_hash(&pk_columns, row_idx);
            bloom.insert_hash(hash);
        }

        Ok(())
    }

    /// Get or create a Dataset for reading.
    ///
    /// Uses caching based on the configured eventual consistency strategy:
    /// - If `always_fresh` is true, always constructs a new Dataset
    /// - Otherwise, returns cached Dataset if within TTL and has same batch count
    ///
    /// Returns None if there's no data to read.
    pub async fn get_or_create_dataset(&self) -> Result<Option<Dataset>> {
        let current_batch_count = self.batch_count();
        if current_batch_count == 0 {
            return Ok(None);
        }

        // Check if we can use cached dataset
        if !self.cache_config.always_fresh {
            let cached = self.cached_dataset.read().await;
            if let Some(ref cached_ds) = *cached {
                // Check if cache is still valid (within TTL and same batch count)
                if cached_ds.batch_count == current_batch_count
                    && cached_ds.created_at.elapsed() < self.cache_config.ttl
                {
                    return Ok(Some(cached_ds.dataset.clone()));
                }
            }
        }

        // Need to construct a new Dataset
        let dataset = self.construct_dataset().await?;

        // Cache the new dataset (unless always_fresh)
        if !self.cache_config.always_fresh {
            let mut cached = self.cached_dataset.write().await;
            *cached = Some(CachedDataset {
                dataset: dataset.clone(),
                created_at: Instant::now(),
                batch_count: current_batch_count,
            });
        }

        Ok(Some(dataset))
    }

    /// Construct a fresh Dataset from stored batches.
    async fn construct_dataset(&self) -> Result<Dataset> {
        if self.batch_store.is_empty() {
            return Err(Error::invalid_input("Cannot construct Dataset: no batches"));
        }

        // Get batches
        let batches = self.batch_store.to_vec();

        // Create a new Dataset with all the batches
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), self.schema.clone());
        let dataset = Dataset::write(reader, &self.dataset_uri, None).await?;

        Ok(dataset)
    }

    /// Scan all data from the MemTable.
    ///
    /// Returns all batches for flushing to persistent storage.
    pub async fn scan_batches(&self) -> Result<Vec<RecordBatch>> {
        Ok(self.batch_store.to_vec())
    }

    /// Scan all data from the MemTable in reverse order (newest first).
    ///
    /// This is used when flushing MemTable to persistent storage to ensure
    /// the flushed data is ordered from newest to oldest. This enables more
    /// efficient K-way merge during LSM scan because SSTables
    /// will be pre-sorted in the order needed for deduplication.
    ///
    /// The total number of rows in the MemTable is also returned to allow
    /// callers to compute reversed row positions for indexes.
    pub async fn scan_batches_reversed(&self) -> Result<(Vec<RecordBatch>, usize)> {
        let total_rows = self.batch_store.total_rows();
        let batches = self.batch_store.to_vec_reversed()?;
        Ok((batches, total_rows))
    }

    /// Scan specific batches by their batch_positions.
    pub async fn scan_batches_by_ids(&self, batch_positions: &[usize]) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::with_capacity(batch_positions.len());
        for &batch_position in batch_positions {
            let batch = self.batch_store.get_batch(batch_position).ok_or_else(|| {
                Error::invalid_input(format!("Batch {} not found", batch_position))
            })?;
            results.push(batch.clone());
        }
        Ok(results)
    }

    /// Get batches for WAL flush.
    pub async fn get_batches_for_wal(&self, batch_positions: &[usize]) -> Result<Vec<RecordBatch>> {
        self.scan_batches_by_ids(batch_positions).await
    }

    /// Check if a primary key might exist in this MemTable.
    ///
    /// Uses bloom filter for fast negative lookups.
    /// Returns true if the key might exist, false if definitely not present.
    pub fn might_contain_pk(&self, pk_hash: u64) -> bool {
        self.pk_bloom_filter.check_hash(pk_hash)
    }

    /// Get the schema.
    pub fn schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }

    /// Get the Lance schema.
    pub fn lance_schema(&self) -> &Schema {
        &self.lance_schema
    }

    /// Get the generation number.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Get total row count.
    pub fn row_count(&self) -> usize {
        self.batch_store.total_rows()
    }

    /// Get batch count.
    pub fn batch_count(&self) -> usize {
        self.batch_store.len()
    }

    /// Get batch count (async version for API compatibility).
    #[allow(clippy::unused_async)]
    pub async fn batch_count_async(&self) -> usize {
        self.batch_count()
    }

    /// Get estimated size in bytes.
    pub fn estimated_size(&self) -> usize {
        self.batch_store.estimated_bytes() + self.pk_bloom_filter.estimated_memory_size()
    }

    /// Get the bloom filter for serialization.
    pub fn bloom_filter(&self) -> &Sbbf {
        &self.pk_bloom_filter
    }

    /// Get reference to indexes.
    pub fn indexes(&self) -> Option<&IndexStore> {
        self.indexes.as_ref().map(|arc| arc.as_ref())
    }

    /// Get the Arc-wrapped indexes (for sharing with async handler).
    pub fn indexes_arc(&self) -> Option<Arc<IndexStore>> {
        self.indexes.clone()
    }

    /// Take the index registry (for flushing).
    /// Returns the Arc, which may be shared with async handler.
    pub fn take_indexes(&mut self) -> Option<Arc<IndexStore>> {
        self.indexes.take()
    }

    /// Whether every committed batch in this memtable is WAL-durable, given the
    /// writer-global durability cursor. The L0 flush's precondition.
    pub fn all_flushed_to_wal(&self, durable: usize) -> bool {
        self.batch_store.pending_wal_flush_count(durable) == 0
    }

    /// Writer-global coordinate one past this memtable's last committed batch.
    pub fn global_end(&self) -> usize {
        self.batch_store.global_end()
    }

    /// Get cache configuration.
    pub fn cache_config(&self) -> &CacheConfig {
        &self.cache_config
    }

    /// Get the batch store capacity.
    pub fn batch_capacity(&self) -> usize {
        self.batch_store.capacity()
    }

    /// Get remaining batch capacity.
    pub fn remaining_batch_capacity(&self) -> usize {
        self.batch_store.remaining_capacity()
    }

    /// Create a scanner for querying this MemTable.
    ///
    /// # Arguments
    ///
    /// * `visible_count` - Maximum batch position visible (inclusive)
    ///
    /// The scanner captures the current `visible_count` from the
    /// `IndexStore` at construction time to ensure consistent visibility.
    ///
    /// # Panics
    ///
    /// Panics if the memtable has no indexes configured.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let scanner = memtable.scan();
    /// let results = scanner
    ///     .project(&["id", "name"])
    ///     .filter("id > 10")?
    ///     .try_into_batch()
    ///     .await?;
    /// ```
    pub fn scan(&self) -> scanner::MemTableScanner {
        let indexes = self
            .indexes
            .clone()
            .expect("MemTable must have indexes configured for scanning");
        scanner::MemTableScanner::new(self.batch_store.clone(), indexes, self.schema.clone())
    }

    /// Get a clone of the batch store Arc for external use.
    pub fn batch_store(&self) -> Arc<BatchStore> {
        self.batch_store.clone()
    }
}

/// Compute a hash for a row's primary key values.
fn compute_row_hash(columns: &[Arc<dyn Array>], row_idx: usize) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    for col in columns {
        // Hash the scalar value at this row
        let is_null = col.is_null(row_idx);
        is_null.hash(&mut hasher);

        if !is_null {
            // Hash based on data type
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int32Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                arr.value(row_idx).hash(&mut hasher);
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                arr.value(row_idx).hash(&mut hasher);
            }
            // Add more types as needed
        }
    }

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &ArrowSchema, num_rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                Arc::new(StringArray::from_iter_values(
                    (0..num_rows).map(|i| format!("name_{}", i)),
                )),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_memtable_insert() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        let batch = create_test_batch(&schema, 10);
        let batch_position = memtable.insert(batch).await.unwrap();

        assert_eq!(batch_position, 0);
        assert_eq!(memtable.row_count(), 10);
        assert_eq!(memtable.batch_count(), 1);
        // Dataset is constructed on-demand
        assert!(memtable.get_or_create_dataset().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_memtable_multiple_inserts() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        for i in 0..3 {
            let batch = create_test_batch(&schema, 10);
            let batch_position = memtable.insert(batch).await.unwrap();
            assert_eq!(batch_position, i);
        }

        assert_eq!(memtable.row_count(), 30);
        assert_eq!(memtable.batch_count(), 3);
    }

    #[tokio::test]
    async fn test_memtable_scan() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap();

        let batches = memtable.scan_batches().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 15);
    }

    /// `all_flushed_to_wal(durable)` is the L0 flush's precondition (`flush.rs:171`):
    /// false while any committed batch is still un-appended, true once the
    /// durability watermark covers every one of them.
    #[tokio::test]
    async fn test_all_flushed_to_wal_tracks_the_durability_watermark() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        let batch1 = memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        let batch2 = memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap();
        assert!(!memtable.all_flushed_to_wal(0), "nothing is durable yet");

        let durable = batch1 + 1;
        assert!(
            !memtable.all_flushed_to_wal(durable),
            "batch2 is still waiting on its WAL append"
        );

        let durable = batch2 + 1;
        assert!(memtable.all_flushed_to_wal(durable));
    }

    #[tokio::test]
    async fn test_memtable_visibility_tracking() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        // Insert batches at positions 0, 1, 2
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 3))
            .await
            .unwrap();

        // A count of N exposes the prefix [0, N).
        let visible = memtable.get_visible_batches(2).await;
        assert_eq!(visible.len(), 2);
        let total_rows: usize = visible.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 15); // 10 + 5

        let visible = memtable.get_visible_batches(3).await;
        assert_eq!(visible.len(), 3);

        // A count of 0 exposes nothing — not "batch 0".
        assert!(memtable.get_visible_batches(0).await.is_empty());
    }

    #[tokio::test]
    async fn test_memtable_get_visible_batch_positions() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        // Insert batches at positions 0, 1, 2
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 3))
            .await
            .unwrap();

        // A count of N exposes the prefix [0, N).
        let visible_ids = memtable.get_visible_batch_positions(2).await;
        assert_eq!(visible_ids, vec![0, 1]);

        let visible_ids = memtable.get_visible_batch_positions(3).await;
        assert_eq!(visible_ids, vec![0, 1, 2]);

        // A count of 0 exposes nothing.
        assert!(memtable.get_visible_batch_positions(0).await.is_empty());
    }

    #[tokio::test]
    async fn test_memtable_is_batch_visible() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap(); // position 0
        memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap(); // position 1
        memtable
            .insert(create_test_batch(&schema, 3))
            .await
            .unwrap(); // position 2

        // A count of 0 means nothing is visible, batch 0 included.
        assert!(!memtable.is_batch_visible(0, 0).await);

        // Batch i is visible once the count exceeds i.
        assert!(memtable.is_batch_visible(0, 1).await);
        assert!(memtable.is_batch_visible(0, 2).await);
        assert!(!memtable.is_batch_visible(2, 1).await);
        assert!(!memtable.is_batch_visible(2, 2).await);
        assert!(memtable.is_batch_visible(2, 3).await);

        // Non-existent batch
        assert!(!memtable.is_batch_visible(999, 100).await);
    }

    #[tokio::test]
    async fn test_memtable_scan_batches_at_position() {
        let schema = create_test_schema();
        let mut memtable = MemTable::new(schema.clone(), 1, vec![]).unwrap();

        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap(); // position 0
        memtable
            .insert(create_test_batch(&schema, 5))
            .await
            .unwrap(); // position 1

        let batches = memtable.scan_batches_at_position(1).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);

        let batches = memtable.scan_batches_at_position(2).await.unwrap();
        assert_eq!(batches.len(), 2);

        // Nothing indexed yet => nothing scannable.
        assert!(
            memtable
                .scan_batches_at_position(0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_memtable_capacity() {
        let schema = create_test_schema();
        let mut memtable =
            MemTable::with_capacity(schema.clone(), 1, vec![], CacheConfig::default(), 3).unwrap();

        assert_eq!(memtable.batch_capacity(), 3);
        assert_eq!(memtable.remaining_batch_capacity(), 3);
        assert!(!memtable.batch_store().is_full());

        // Fill up the store
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();

        assert!(memtable.batch_store().is_full());
        assert_eq!(memtable.remaining_batch_capacity(), 0);

        // Next insert should fail
        let result = memtable.insert(create_test_batch(&schema, 10)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memtable_should_flush() {
        let schema = create_test_schema();
        let mut memtable =
            MemTable::with_capacity(schema.clone(), 1, vec![], CacheConfig::default(), 2).unwrap();

        // Not full yet
        assert!(!memtable.should_flush(1024 * 1024));

        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();
        memtable
            .insert(create_test_batch(&schema, 10))
            .await
            .unwrap();

        // Now full
        assert!(memtable.should_flush(1024 * 1024));
    }
}
