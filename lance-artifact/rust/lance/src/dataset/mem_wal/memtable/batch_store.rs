// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lock-free append-only batch storage for MemTable.
//!
//! This module provides a high-performance, lock-free storage structure for
//! RecordBatches in the MemTable. It is designed for a single-writer,
//! multiple-reader scenario where:
//!
//! - A single writer task (WriteBatchHandler) appends batches
//! - Multiple reader tasks concurrently read batches
//! - No locks are needed for either reads or writes
//!
//! # Safety Model
//!
//! The lock-free design relies on these invariants:
//!
//! 1. **Single Writer**: Only one thread calls `append()` at a time.
//!    Enforced by the WriteBatchHandler architecture.
//!
//! 2. **Append-Only**: Once written, slots are never modified or removed
//!    until the entire store is dropped.
//!
//! 3. **Atomic Publishing**: Writer updates `committed_len` with Release
//!    ordering AFTER fully writing the slot. Readers load with Acquire
//!    ordering BEFORE reading slots.
//!
//! 4. **Fixed Capacity**: The store has a fixed capacity set at creation.
//!    When full, the MemTable should be flushed.
//!
//! # Memory Ordering
//!
//! ```text
//! Writer:                              Reader:
//! 1. Write data to slot[n]
//! 2. committed_len.store(n+1, Release)
//!    ─────────────────────────────────► synchronizes-with
//!                                      3. len = committed_len.load(Acquire)
//!                                      4. Read slot[i] where i < len
//! ```

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::ArrayData;
use arrow_array::RecordBatch;
use arrow_schema::DataType;

/// A batch stored in the lock-free store.
#[derive(Clone)]
pub struct StoredBatch {
    /// The Arrow RecordBatch data.
    pub data: RecordBatch,
    /// Number of rows in this batch (cached for quick access).
    pub num_rows: usize,
    /// Estimated memory size in bytes.
    pub estimated_size: usize,
    /// Row offset in the MemTable (cumulative rows before this batch).
    pub row_offset: u64,
    /// Position of this batch in the store (0-indexed).
    pub batch_position: usize,
}

impl StoredBatch {
    /// Create a new StoredBatch.
    pub fn new(data: RecordBatch, row_offset: u64, batch_position: usize) -> Self {
        let num_rows = data.num_rows();
        let estimated_size = Self::estimate_batch_size(&data);
        Self {
            data,
            num_rows,
            estimated_size,
            row_offset,
            batch_position,
        }
    }

    /// Estimate the memory size of a RecordBatch.
    ///
    /// Sums each column's slice-aware buffer size (see
    /// [`Self::estimate_array_size`]) plus the struct overhead, so a column that
    /// is a zero-copy slice of a larger parent contributes only its own window
    /// rather than the whole shared buffer.
    fn estimate_batch_size(batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|col| Self::estimate_array_size(&col.to_data()))
            .sum::<usize>()
            + std::mem::size_of::<RecordBatch>()
    }

    /// Slice-aware buffer size of a single array.
    ///
    /// [`ArrayData::get_slice_memory_size`] reports each buffer's own window
    /// (not the whole shared buffer), but omits the variadic data buffers of
    /// `Utf8View`/`BinaryView` (values > 12 bytes) while still returning `Ok`, so
    /// [`Self::view_data_buffers_size`] adds them. Those buffers are shared across
    /// zero-copy slices and are counted at full capacity for each slice — an
    /// over-count in the safe direction.
    fn estimate_array_size(data: &ArrayData) -> usize {
        match data.get_slice_memory_size() {
            Ok(size) => size + Self::view_data_buffers_size(data),
            // Fall back to the full-buffer sum for layouts the slice-aware call
            // cannot handle.
            Err(_) => data.get_array_memory_size(),
        }
    }

    /// Capacity of the variadic `Utf8View`/`BinaryView` data buffers that
    /// [`ArrayData::get_slice_memory_size`] omits, summed recursively over children.
    fn view_data_buffers_size(data: &ArrayData) -> usize {
        let mut size = 0;
        if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
            // buffers()[0] is the 16-byte view array that get_slice_memory_size
            // already counts; [1..] are the data buffers it skips.
            size += data
                .buffers()
                .iter()
                .skip(1)
                .map(|b| b.capacity())
                .sum::<usize>();
        }
        for child in data.child_data() {
            size += Self::view_data_buffers_size(child);
        }
        size
    }
}

/// Snapshot of the active batches that have not yet been flushed to WAL.
#[derive(Debug, Clone, Copy, Default)]
pub struct PendingWalFlushStats {
    /// First pending batch position, inclusive.
    pub start_batch_position: Option<usize>,
    /// Last pending batch position, exclusive.
    pub end_batch_position: Option<usize>,
    /// Number of pending batches.
    pub batch_count: usize,
    /// Number of rows in pending batches.
    pub row_count: usize,
    /// Estimated bytes in pending batches.
    pub estimated_bytes: usize,
}

/// Error returned when the store is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreFull;

impl std::fmt::Display for StoreFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchStore is full")
    }
}

impl std::error::Error for StoreFull {}

/// Lock-free append-only storage for memtable batches.
///
/// This structure provides O(1) lock-free appends and reads for a
/// single-writer, multiple-reader scenario.
///
/// # Example
///
/// ```ignore
/// let store = BatchStore::with_capacity(100);
///
/// // Writer (single thread)
/// store.append(batch1, 1)?;
/// store.append(batch2, 2)?;
///
/// // Readers (multiple threads, concurrent)
/// let len = store.len();
/// for i in 0..len {
///     let batch = store.get(i).unwrap();
///     // process batch...
/// }
/// ```
pub struct BatchStore {
    /// Pre-allocated storage slots.
    /// Each slot is either uninitialized or contains a valid StoredBatch.
    slots: Box<[UnsafeCell<MaybeUninit<StoredBatch>>]>,

    /// Number of committed (fully written) slots.
    /// Invariant: all slots [0, committed_len) contain valid data.
    committed_len: AtomicUsize,

    /// Total capacity (fixed at creation).
    capacity: usize,

    /// Total row count across all committed batches.
    total_rows: AtomicUsize,

    /// Estimated size in bytes (for flush threshold).
    estimated_bytes: AtomicUsize,

    /// Writer-global coordinate of this store's batch 0.
    ///
    /// A *coordinate*, not a cursor: stamped once at construction and never
    /// moved. `global_position = global_offset + local_position`. Batch
    /// positions restart at 0 in every memtable, so this is the only thing that
    /// lets a writer-global cursor (the WAL durability count) be mapped onto a
    /// particular store.
    global_offset: usize,
}

// SAFETY: Safe to share across threads because:
// - Single writer guarantee (architectural invariant)
// - Readers only access committed slots (index < committed_len)
// - Atomic operations provide proper synchronization
// - Slots are never modified after being written
unsafe impl Sync for BatchStore {}
unsafe impl Send for BatchStore {}

impl BatchStore {
    /// Create a new store with the given capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of batches. Should be sized based on
    ///   `max_memtable_size / expected_avg_batch_size`.
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_at(capacity, 0)
    }

    /// Create a store whose batch 0 sits at `global_offset` in the writer's
    /// batch sequence. Used by `freeze_memtable` for every memtable after the
    /// first; the first starts at 0.
    pub fn with_capacity_at(capacity: usize, global_offset: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        // Allocate uninitialized storage
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            slots: slots.into_boxed_slice(),
            committed_len: AtomicUsize::new(0),
            capacity,
            total_rows: AtomicUsize::new(0),
            estimated_bytes: AtomicUsize::new(0),
            global_offset,
        }
    }

    /// Calculate recommended capacity from memtable size configuration.
    ///
    /// Uses an assumed average batch size of 64KB with 20% buffer.
    pub fn recommended_capacity(max_memtable_bytes: usize) -> usize {
        const AVG_BATCH_SIZE: usize = 64 * 1024; // 64KB
        const BUFFER_FACTOR: f64 = 1.2;

        let estimated_batches = max_memtable_bytes / AVG_BATCH_SIZE;
        let capacity = ((estimated_batches as f64) * BUFFER_FACTOR) as usize;
        capacity.max(16) // Minimum 16 slots
    }

    /// Returns the capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns true if the store is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.committed_len.load(Ordering::Relaxed) >= self.capacity
    }

    /// Returns the number of remaining slots.
    #[inline]
    pub fn remaining_capacity(&self) -> usize {
        self.capacity
            .saturating_sub(self.committed_len.load(Ordering::Relaxed))
    }

    // =========================================================================
    // Writer API (Single Writer Only)
    // =========================================================================

    /// Append a batch to the store.
    ///
    /// # Safety Requirements
    ///
    /// This method MUST only be called from the single writer task.
    /// Concurrent calls from multiple threads cause undefined behavior.
    ///
    /// # Returns
    ///
    /// - `Ok((batch_position, row_offset, estimated_size))` - The index, row offset, and size of the appended batch
    /// - `Err(StoreFull)` - The store is at capacity, needs flush
    pub fn append(&self, batch: RecordBatch) -> Result<(usize, u64, usize), StoreFull> {
        // Load current length (Relaxed is fine - we're the only writer)
        let idx = self.committed_len.load(Ordering::Relaxed);

        if idx >= self.capacity {
            return Err(StoreFull);
        }

        // Row offset is the total rows BEFORE this batch
        let row_offset = self.total_rows.load(Ordering::Relaxed) as u64;

        let stored = StoredBatch::new(batch, row_offset, idx);
        let num_rows = stored.num_rows;
        let estimated_size = stored.estimated_size;

        // SAFETY:
        // 1. idx < capacity, so slot exists
        // 2. Single writer guarantee - no concurrent writes to this slot
        // 3. Slot at idx is uninitialized (never written before, append-only)
        unsafe {
            let slot_ptr = self.slots[idx].get();
            std::ptr::write(slot_ptr, MaybeUninit::new(stored));
        }

        // Update counters (Relaxed - just tracking, not synchronization)
        self.total_rows.fetch_add(num_rows, Ordering::Relaxed);
        self.estimated_bytes
            .fetch_add(estimated_size, Ordering::Relaxed);

        // CRITICAL: Publish with Release ordering.
        // This ensures all writes above are visible to readers
        // who load committed_len with Acquire ordering.
        self.committed_len.store(idx + 1, Ordering::Release);

        Ok((idx, row_offset, estimated_size))
    }

    /// Append multiple batches to the store atomically.
    ///
    /// All batches are written before publishing, so readers see either
    /// none of the batches or all of them (atomic visibility).
    ///
    /// # Safety Requirements
    ///
    /// This method MUST only be called from the single writer task.
    /// Concurrent calls from multiple threads cause undefined behavior.
    ///
    /// # Returns
    ///
    /// - `Ok(Vec<(batch_position, row_offset, estimated_size)>)` - Info for each appended batch
    /// - `Err(StoreFull)` - Not enough capacity for all batches
    pub fn append_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<(usize, u64, usize)>, StoreFull> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        // Load current length (Relaxed is fine - we're the only writer)
        let start_idx = self.committed_len.load(Ordering::Relaxed);
        let count = batches.len();

        // Check capacity for ALL batches upfront
        if start_idx + count > self.capacity {
            return Err(StoreFull);
        }

        let mut results = Vec::with_capacity(count);
        let mut total_rows_added = 0usize;
        let mut total_bytes_added = 0usize;
        let mut row_offset = self.total_rows.load(Ordering::Relaxed) as u64;

        // Write all batches to slots (not yet visible to readers)
        for (i, batch) in batches.into_iter().enumerate() {
            let idx = start_idx + i;
            let stored = StoredBatch::new(batch, row_offset, idx);
            let num_rows = stored.num_rows;
            let estimated_size = stored.estimated_size;

            // SAFETY:
            // 1. idx < capacity (checked above)
            // 2. Single writer guarantee - no concurrent writes to this slot
            // 3. Slot at idx is uninitialized (never written before, append-only)
            unsafe {
                let slot_ptr = self.slots[idx].get();
                std::ptr::write(slot_ptr, MaybeUninit::new(stored));
            }

            results.push((idx, row_offset, estimated_size));
            row_offset += num_rows as u64;
            total_rows_added += num_rows;
            total_bytes_added += estimated_size;
        }

        // Update counters (Relaxed - just tracking, not synchronization)
        self.total_rows
            .fetch_add(total_rows_added, Ordering::Relaxed);
        self.estimated_bytes
            .fetch_add(total_bytes_added, Ordering::Relaxed);

        // CRITICAL: Publish ALL batches at once with Release ordering.
        // This ensures all writes above are visible to readers
        // who load committed_len with Acquire ordering.
        self.committed_len
            .store(start_idx + count, Ordering::Release);

        Ok(results)
    }

    // =========================================================================
    // Reader API (Multiple Concurrent Readers)
    // =========================================================================

    /// Get the number of committed batches.
    #[inline]
    pub fn len(&self) -> usize {
        self.committed_len.load(Ordering::Acquire)
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the maximum buffered batch position (inclusive).
    ///
    /// Returns `None` if no batches have been buffered.
    /// Returns `Some(len - 1)` otherwise, which is the position of the last buffered batch.
    #[inline]
    pub fn max_buffered_batch_position(&self) -> Option<usize> {
        let len = self.len();
        if len == 0 { None } else { Some(len - 1) }
    }

    /// Get total row count.
    #[inline]
    pub fn total_rows(&self) -> usize {
        self.total_rows.load(Ordering::Relaxed)
    }

    /// Get estimated size in bytes.
    #[inline]
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    // =========================================================================
    // WAL Flush Tracking API
    // =========================================================================

    /// Writer-global coordinate one past this store's last committed batch.
    #[inline]
    pub fn global_end(&self) -> usize {
        self.global_offset + self.committed_len.load(Ordering::Acquire)
    }

    /// This store's writer-global coordinate for batch 0.
    #[inline]
    pub fn global_offset(&self) -> usize {
        self.global_offset
    }

    /// The local exclusive end of this store covered by a writer-global cursor.
    ///
    /// Saturating in both directions, and both directions are reachable in
    /// normal operation: a cursor *below* this store's offset means "nothing
    /// here yet" (the store was rotated in after the cursor last advanced —
    /// the ordinary state of a fresh memtable), and a cursor beyond its end
    /// clamps to what is committed.
    ///
    /// This is the **only** place the global-to-local subtraction is written.
    /// Open-coding it underflows on every memtable rotation, which in release
    /// wraps to a huge end and makes the whole new memtable instantly visible.
    #[inline]
    pub fn local_end(&self, global_cursor: usize) -> usize {
        global_cursor
            .saturating_sub(self.global_offset)
            .min(self.committed_len.load(Ordering::Acquire))
    }

    /// Batches in this store still waiting on their WAL append.
    #[inline]
    pub fn pending_wal_flush_count(&self, durable: usize) -> usize {
        self.committed_len.load(Ordering::Acquire) - self.local_end(durable)
    }

    /// Local range `[start, end)` of batches still waiting on their WAL append,
    /// or `None` when the store is fully durable.
    #[inline]
    pub fn pending_wal_flush_range(&self, durable: usize) -> Option<(usize, usize)> {
        let start = self.local_end(durable);
        let end = self.committed_len.load(Ordering::Acquire);
        (end > start).then_some((start, end))
    }

    /// Get a point-in-time summary of batches pending WAL flush.
    pub fn pending_wal_flush_stats(&self, durable: usize) -> PendingWalFlushStats {
        let Some((start, end)) = self.pending_wal_flush_range(durable) else {
            return PendingWalFlushStats::default();
        };

        let mut stats = PendingWalFlushStats {
            start_batch_position: Some(start),
            end_batch_position: Some(end),
            batch_count: 0,
            row_count: 0,
            estimated_bytes: 0,
        };
        for batch_position in start..end {
            if let Some(stored) = self.get(batch_position) {
                stats.batch_count += 1;
                stats.row_count += stored.num_rows;
                stats.estimated_bytes += stored.estimated_size;
            }
        }
        stats
    }

    /// Get a reference to a batch by index.
    ///
    /// Returns `None` if index >= committed length.
    ///
    /// # Safety
    ///
    /// The returned reference is valid as long as `self` is not dropped.
    /// This is safe because:
    /// - We only access slots where index < committed_len (Acquire load)
    /// - Slots are never modified after being written
    /// - The store is append-only
    #[inline]
    pub fn get(&self, index: usize) -> Option<&StoredBatch> {
        // Acquire ordering synchronizes with Release in append()
        let len = self.committed_len.load(Ordering::Acquire);

        if index >= len {
            return None;
        }

        // SAFETY:
        // 1. index < len, and len was loaded with Acquire ordering
        // 2. The Release-Acquire pair ensures the write is visible
        // 3. Slots are never modified after writing (append-only)
        unsafe {
            let slot_ptr = self.slots[index].get();
            Some((*slot_ptr).assume_init_ref())
        }
    }

    /// Get the RecordBatch data at an index.
    #[inline]
    pub fn get_batch(&self, index: usize) -> Option<&RecordBatch> {
        self.get(index).map(|s| &s.data)
    }

    /// Iterate over all committed batches.
    ///
    /// The iterator captures a snapshot of the committed length at creation
    /// time, so it will not see batches appended during iteration.
    pub fn iter(&self) -> BatchStoreIter<'_> {
        let len = self.committed_len.load(Ordering::Acquire);
        BatchStoreIter {
            store: self,
            current: 0,
            len,
        }
    }

    /// Get all batches as a Vec (clones the RecordBatch data).
    pub fn to_vec(&self) -> Vec<RecordBatch> {
        self.iter().map(|b| b.data.clone()).collect()
    }

    /// Get all StoredBatches as a Vec (clones).
    pub fn to_stored_vec(&self) -> Vec<StoredBatch> {
        self.iter().cloned().collect()
    }

    /// Iterate over all committed batches in reverse order (newest first).
    ///
    /// The iterator captures a snapshot of the committed length at creation
    /// time, so it will not see batches appended during iteration.
    pub fn iter_reversed(&self) -> BatchStoreIterReversed<'_> {
        let len = self.committed_len.load(Ordering::Acquire);
        BatchStoreIterReversed {
            store: self,
            current: len,
        }
    }

    /// Get all batches as a Vec with rows in reverse order (newest first).
    ///
    /// This is useful for flushing MemTable to disk where we want the
    /// flushed data to be ordered from newest to oldest for efficient
    /// K-way merge during LSM scan.
    ///
    /// The batches are iterated in reverse order, and the rows within each
    /// batch are also reversed, so the final result has all rows in reverse
    /// order from newest to oldest.
    pub fn to_vec_reversed(&self) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
        use arrow::compute::kernels::take::take;
        use arrow_array::UInt32Array;

        self.iter_reversed()
            .map(|b| {
                // Reverse the rows within each batch
                let num_rows = b.data.num_rows();
                if num_rows == 0 {
                    return Ok(b.data.clone());
                }

                // Create indices for reversed order: [n-1, n-2, ..., 1, 0]
                let indices: Vec<u32> = (0..num_rows as u32).rev().collect();
                let indices_array = UInt32Array::from(indices);

                // Take rows in reversed order
                let columns: Result<Vec<_>, _> = b
                    .data
                    .columns()
                    .iter()
                    .map(|col| take(col.as_ref(), &indices_array, None))
                    .collect();

                RecordBatch::try_new(b.data.schema(), columns?)
            })
            .collect()
    }

    /// Get all StoredBatches as a Vec in reverse order (newest first).
    pub fn to_stored_vec_reversed(&self) -> Vec<StoredBatch> {
        self.iter_reversed().cloned().collect()
    }

    // =========================================================================
    // Visibility API
    // =========================================================================

    /// Batches in the visible prefix `[0, visible_count)`.
    ///
    /// `visible_count` is an **exclusive count**, not an inclusive position: 0
    /// means nothing is visible. As an inclusive position, 0 meant *both*
    /// "nothing visible" and "batch 0 is visible", so a batch that was committed
    /// to the store but not yet indexed or WAL-durable was readable for a full
    /// PUT round-trip. The count makes that off-by-one inexpressible.
    pub fn visible_batches(&self, visible_count: usize) -> Vec<&StoredBatch> {
        let end = visible_count.min(self.committed_len.load(Ordering::Acquire));
        (0..end).filter_map(|i| self.get(i)).collect()
    }

    /// Positions of the batches in the visible prefix.
    pub fn visible_batch_positions(&self, visible_count: usize) -> Vec<usize> {
        let end = visible_count.min(self.committed_len.load(Ordering::Acquire));
        (0..end).collect()
    }

    /// The inclusive maximum visible *row* position, or `None` when no rows are
    /// visible. Each batch carries its cumulative `row_offset`, so this is the
    /// end of the last visible batch minus one. Bounds MVCC seeks against the
    /// maintained PK-position index.
    pub fn max_visible_row(&self, visible_count: usize) -> Option<u64> {
        let end = visible_count.min(self.committed_len.load(Ordering::Acquire));
        let last = self.get(end.checked_sub(1)?)?;
        let visible_end = last.row_offset + last.num_rows as u64; // exclusive
        visible_end.checked_sub(1)
    }

    /// Whether a batch falls inside the visible prefix.
    #[inline]
    pub fn is_batch_visible(&self, batch_position: usize, visible_count: usize) -> bool {
        let len = self.committed_len.load(Ordering::Acquire);
        batch_position < len && batch_position < visible_count
    }

    /// Visible RecordBatches (clones the data).
    pub fn visible_record_batches(&self, visible_count: usize) -> Vec<RecordBatch> {
        self.visible_batches(visible_count)
            .into_iter()
            .map(|b| b.data.clone())
            .collect()
    }

    /// Visible RecordBatches paired with the row position each one starts at.
    pub fn visible_batches_with_offsets(&self, visible_count: usize) -> Vec<(RecordBatch, u64)> {
        self.visible_batches(visible_count)
            .into_iter()
            .map(|b| (b.data.clone(), b.row_offset))
            .collect()
    }
}

impl Drop for BatchStore {
    fn drop(&mut self) {
        // Get the committed length directly (no atomic needed, we have &mut self)
        let len = *self.committed_len.get_mut();

        // Drop all initialized slots
        for i in 0..len {
            // SAFETY: slots [0, len) are initialized and we have exclusive access
            unsafe {
                let slot_ptr = self.slots[i].get();
                std::ptr::drop_in_place((*slot_ptr).as_mut_ptr());
            }
        }
    }
}

/// Iterator over committed batches in a BatchStore.
///
/// This iterator captures a snapshot of the committed length at creation,
/// providing a consistent view even if new batches are appended during
/// iteration.
pub struct BatchStoreIter<'a> {
    store: &'a BatchStore,
    current: usize,
    len: usize,
}

impl<'a> Iterator for BatchStoreIter<'a> {
    type Item = &'a StoredBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.len {
            return None;
        }

        // SAFETY: current < len, which was captured with Acquire ordering
        let batch = unsafe {
            let slot_ptr = self.store.slots[self.current].get();
            (*slot_ptr).assume_init_ref()
        };

        self.current += 1;
        Some(batch)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for BatchStoreIter<'_> {}

/// Reverse iterator over committed batches in a BatchStore.
///
/// Iterates from the newest batch (highest index) to the oldest batch (index 0).
/// This is used during MemTable flush to write batches in reverse order,
/// ensuring flushed data is ordered from newest to oldest for efficient
/// K-way merge during LSM scan.
pub struct BatchStoreIterReversed<'a> {
    store: &'a BatchStore,
    /// Points to the next batch to return (exclusive upper bound).
    /// Starts at len and decrements to 0.
    current: usize,
}

impl<'a> Iterator for BatchStoreIterReversed<'a> {
    type Item = &'a StoredBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == 0 {
            return None;
        }

        self.current -= 1;

        // SAFETY: current is now in range [0, len), and len was captured with Acquire ordering
        let batch = unsafe {
            let slot_ptr = self.store.slots[self.current].get();
            (*slot_ptr).assume_init_ref()
        };

        Some(batch)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.current, Some(self.current))
    }
}

impl ExactSizeIterator for BatchStoreIterReversed<'_> {}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = create_test_schema();
        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let values: Vec<i32> = ids.iter().map(|id| id * 10).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_create_store() {
        let store = BatchStore::with_capacity(10);
        assert_eq!(store.capacity(), 10);
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert!(!store.is_full());
        assert_eq!(store.remaining_capacity(), 10);
    }

    #[test]
    fn test_append_single() {
        let store = BatchStore::with_capacity(10);
        let batch = create_test_batch(100);

        let (id, row_offset, _size) = store.append(batch).unwrap();
        assert_eq!(id, 0);
        assert_eq!(row_offset, 0); // First batch starts at row 0
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        assert_eq!(store.total_rows(), 100);
    }

    #[test]
    fn test_append_multiple() {
        let store = BatchStore::with_capacity(10);

        let mut expected_row_offset = 0u64;
        for i in 0..5 {
            let num_rows = 10 * (i + 1);
            let batch = create_test_batch(num_rows);
            let (id, row_offset, _size) = store.append(batch).unwrap();
            assert_eq!(id, i);
            assert_eq!(row_offset, expected_row_offset);
            expected_row_offset += num_rows as u64;
        }

        assert_eq!(store.len(), 5);
        assert_eq!(store.total_rows(), 10 + 20 + 30 + 40 + 50);
    }

    #[test]
    fn test_capacity_limit() {
        let store = BatchStore::with_capacity(3);

        store.append(create_test_batch(10)).unwrap();
        store.append(create_test_batch(10)).unwrap();
        store.append(create_test_batch(10)).unwrap();

        assert!(store.is_full());
        assert_eq!(store.remaining_capacity(), 0);

        let result = store.append(create_test_batch(10));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), StoreFull);
    }

    #[test]
    fn test_get_batch() {
        let store = BatchStore::with_capacity(10);

        let batch1 = create_test_batch(10);
        let batch2 = create_test_batch(20);

        store.append(batch1).unwrap();
        store.append(batch2).unwrap();

        let retrieved1 = store.get(0).unwrap();
        assert_eq!(retrieved1.num_rows, 10);
        assert_eq!(retrieved1.row_offset, 0);

        let retrieved2 = store.get(1).unwrap();
        assert_eq!(retrieved2.num_rows, 20);
        assert_eq!(retrieved2.row_offset, 10); // After first batch

        // Out of bounds
        assert!(store.get(2).is_none());
        assert!(store.get(100).is_none());
    }

    #[test]
    fn test_iter() {
        let store = BatchStore::with_capacity(10);

        for _ in 0..5 {
            store.append(create_test_batch(10)).unwrap();
        }

        let batches: Vec<_> = store.iter().collect();
        assert_eq!(batches.len(), 5);
    }

    #[test]
    fn test_visibility_filtering() {
        let store = BatchStore::with_capacity(10);

        store.append(create_test_batch(10)).unwrap(); // position 0
        store.append(create_test_batch(10)).unwrap(); // position 1
        store.append(create_test_batch(10)).unwrap(); // position 2
        store.append(create_test_batch(10)).unwrap(); // position 3
        store.append(create_test_batch(10)).unwrap(); // position 4

        // A count of N exposes the prefix [0, N).
        assert_eq!(store.visible_batch_positions(3), vec![0, 1, 2]);
        assert_eq!(store.visible_batch_positions(5), vec![0, 1, 2, 3, 4]);

        // A count of 0 exposes nothing. Under the old inclusive cursor this
        // case was indistinguishable from "batch 0 is visible", so every
        // memtable leaked its first batch before it was indexed or durable.
        assert!(store.visible_batch_positions(0).is_empty());

        // Beyond the committed range, clamp.
        assert_eq!(store.visible_batch_positions(99), vec![0, 1, 2, 3, 4]);
    }

    /// The zero of the visibility cursor must be unambiguous.
    ///
    /// `BatchStore::append` publishes `committed_len` on the put path, under the
    /// state lock, *before* the WAL flush that indexes the batch is even
    /// triggered — and that flush is a ~100ms S3 PUT on another task. So batch 0
    /// sits committed and readable for a full round-trip before it is indexed or
    /// durable. As an inclusive position, a cursor of 0 meant both "nothing is
    /// visible" and "batch 0 is visible", so every read arm backed by the batch
    /// store served that batch while the index-backed arms did not — the tiers
    /// actively disagreed. An exclusive count makes the state inexpressible.
    #[test]
    fn test_zero_cursor_hides_the_committed_but_unindexed_prefix() {
        let store = BatchStore::with_capacity(4);
        store.append(create_test_batch(10)).unwrap();
        store.append(create_test_batch(10)).unwrap();

        // Committed, but nothing indexed yet: every visibility query must agree
        // that there is nothing to read.
        assert!(store.visible_batches(0).is_empty());
        assert!(store.visible_batch_positions(0).is_empty());
        assert!(store.visible_record_batches(0).is_empty());
        assert!(store.visible_batches_with_offsets(0).is_empty());
        assert!(!store.is_batch_visible(0, 0));
        assert_eq!(store.max_visible_row(0), None);

        // The batches are there — they are simply not yet published.
        assert_eq!(store.len(), 2);

        // Indexing batch 0 publishes exactly batch 0.
        assert_eq!(store.visible_batches(1).len(), 1);
        assert!(store.is_batch_visible(0, 1));
        assert!(!store.is_batch_visible(1, 1));
        assert_eq!(store.max_visible_row(1), Some(9));
    }

    #[test]
    fn test_is_batch_visible() {
        let store = BatchStore::with_capacity(10);

        store.append(create_test_batch(10)).unwrap(); // position 0
        store.append(create_test_batch(10)).unwrap(); // position 1
        store.append(create_test_batch(10)).unwrap(); // position 2

        // A count of 0 means *nothing* is visible — including batch 0. As an
        // inclusive position this case was indistinguishable from "batch 0 is
        // visible", so a batch that was committed to the store but not yet
        // indexed or WAL-durable was readable for a full PUT round-trip.
        assert!(!store.is_batch_visible(0, 0));

        // Batch i is visible once the count exceeds i.
        assert!(store.is_batch_visible(0, 1));
        assert!(store.is_batch_visible(0, 2));
        assert!(!store.is_batch_visible(2, 1));
        assert!(!store.is_batch_visible(2, 2));
        assert!(store.is_batch_visible(2, 3));

        // Batch 3 doesn't exist
        assert!(!store.is_batch_visible(3, 10));
    }

    #[test]
    fn test_max_visible_row() {
        // (1) Empty store: no rows are visible at any count.
        let store = BatchStore::with_capacity(10);
        assert_eq!(store.max_visible_row(0), None);
        assert_eq!(store.max_visible_row(100), None);

        // Three batches → rows [0,10) [10,30) [30,60); row_offsets 0, 10, 30.
        store.append(create_test_batch(10)).unwrap(); // position 0
        store.append(create_test_batch(20)).unwrap(); // position 1
        store.append(create_test_batch(30)).unwrap(); // position 2

        // (2) A count of 0 means nothing is visible — not "batch 0 is visible".
        assert_eq!(store.max_visible_row(0), None);

        // (3) A count of N yields the inclusive last row of the prefix [0, N).
        assert_eq!(store.max_visible_row(1), Some(9)); // batch 0: 0..10
        assert_eq!(store.max_visible_row(2), Some(29)); // + batch 1: 10..30
        assert_eq!(store.max_visible_row(3), Some(59)); // + batch 2: 30..60

        // (4) A count beyond the committed range clamps to the last batch.
        assert_eq!(store.max_visible_row(100), Some(59));

        // (5) An empty leading batch contributes no rows, so a prefix covering
        // only it still yields None, while a later non-empty batch is reported.
        let store = BatchStore::with_capacity(10);
        store.append(create_test_batch(0)).unwrap(); // position 0: rows [0,0)
        store.append(create_test_batch(5)).unwrap(); // position 1: rows [0,5)
        assert_eq!(store.max_visible_row(1), None); // empty prefix → no rows
        assert_eq!(store.max_visible_row(2), Some(4)); // through batch 1
    }

    #[test]
    fn test_recommended_capacity() {
        // 64MB memtable, 64KB avg batch = 1024 batches * 1.2 = ~1228
        let cap = BatchStore::recommended_capacity(64 * 1024 * 1024);
        assert!(
            (1200..=1300).contains(&cap),
            "capacity should be around 1200, got {}",
            cap
        );

        // Very small memtable should get minimum capacity
        let cap = BatchStore::recommended_capacity(1024);
        assert_eq!(cap, 16); // minimum
    }

    #[test]
    fn test_estimated_size_is_slice_aware() {
        // A batch that is a zero-copy slice of a larger parent must contribute
        // only its own window to the estimate, not the whole shared buffer.
        // `get_array_memory_size` counts every buffer's full capacity regardless
        // of offset/length, so N slices tiling one parent each report the
        // parent's size and inflate the memtable estimate ~N×, tripping the
        // flush threshold far below the configured size.
        let chunk = 1_000;
        let num_slices = 100;
        let parent = create_test_batch(chunk * num_slices);

        // One window vs an equivalently-sized owned batch should track each
        // other; the buggy per-slice estimate would be ~num_slices× larger.
        let slice_est = StoredBatch::estimate_batch_size(&parent.slice(0, chunk));
        let owned_est = StoredBatch::estimate_batch_size(&create_test_batch(chunk));
        assert!(
            slice_est <= owned_est * 2,
            "slice estimate {slice_est} should track its own window (~{owned_est}), not the parent"
        );

        // End-to-end: tiling the parent with zero-copy slices must not multiply
        // the store's running estimate. Track what the old full-buffer behavior
        // would have summed to for contrast.
        let store = BatchStore::with_capacity(num_slices);
        let mut over_counting_sum = 0usize;
        for k in 0..num_slices {
            let s = parent.slice(k * chunk, chunk);
            over_counting_sum += s
                .columns()
                .iter()
                .map(|col| col.get_array_memory_size())
                .sum::<usize>()
                + std::mem::size_of::<RecordBatch>();
            store.append(s).unwrap();
        }

        // Two non-nullable Int32 columns → exactly 4 bytes/row/col of payload.
        let payload_bytes = num_slices * chunk * 2 * std::mem::size_of::<i32>();
        let estimated = store.estimated_bytes();
        assert!(
            estimated >= payload_bytes,
            "estimate {estimated} should cover the actual payload {payload_bytes}"
        );
        // The old behavior over-counts by ~num_slices×; the fix must be far
        // below it (generous 10× margin against struct/alignment overhead).
        assert!(
            estimated * 10 < over_counting_sum,
            "estimate {estimated} should be far below the over-counting sum {over_counting_sum}"
        );
    }

    #[test]
    fn test_estimated_size_counts_view_data_buffers() {
        // Long Utf8View/BinaryView values live in variadic data buffers that
        // `get_slice_memory_size` ignores (returning ~16 * rows). The estimate
        // must include them, both for a top-level view column and for a view
        // array nested in a container, which is only reached via child_data
        // recursion.
        use arrow_array::{Array, ArrayRef, StringViewArray, StructArray};

        let num_rows = 1_000;
        // Each value exceeds the 12-byte inline limit, so it spills to a data buffer.
        let long_value = "x".repeat(64);
        let payload_bytes = num_rows * long_value.len();
        // What the slice-aware call alone reports: just the 16-byte view entries.
        let view_entries_only = num_rows * 16;

        let make_views = || {
            StringViewArray::from(
                (0..num_rows)
                    .map(|_| Some(long_value.as_str()))
                    .collect::<Vec<_>>(),
            )
        };
        let assert_covers = |batch: &RecordBatch| {
            let estimated = StoredBatch::estimate_batch_size(batch);
            assert!(
                estimated >= payload_bytes,
                "estimate {estimated} should cover the view data-buffer payload {payload_bytes}"
            );
            assert!(
                estimated > view_entries_only * 2,
                "estimate {estimated} must exceed the ~{view_entries_only}-byte view-entry-only undercount"
            );
        };

        // Top-level view column.
        let flat = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "s",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(make_views())],
        )
        .unwrap();
        assert_covers(&flat);

        // View nested inside a struct — reachable only through child_data recursion.
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8View, false)),
            Arc::new(make_views()) as ArrayRef,
        )]);
        let nested = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "st",
                nested.data_type().clone(),
                false,
            )])),
            vec![Arc::new(nested)],
        )
        .unwrap();
        assert_covers(&nested);
    }

    #[test]
    fn test_to_vec() {
        let store = BatchStore::with_capacity(10);

        let batch1 = create_test_batch(10);
        let batch2 = create_test_batch(20);

        store.append(batch1).unwrap();
        store.append(batch2).unwrap();

        let vec = store.to_vec();
        assert_eq!(vec.len(), 2);
        assert_eq!(vec[0].num_rows(), 10);
        assert_eq!(vec[1].num_rows(), 20);
    }

    #[test]
    fn test_to_vec_reversed() {
        let store = BatchStore::with_capacity(10);

        // Create batches with identifiable values
        // batch1: ids [0, 1, 2, ..., 9], values [0, 10, 20, ..., 90]
        let batch1 = create_test_batch(10);
        // batch2: ids [0, 1, 2, ..., 4], values [0, 10, 20, 30, 40]
        let batch2 = create_test_batch(5);

        store.append(batch1).unwrap();
        store.append(batch2).unwrap();

        // Forward order: batches in insertion order, rows in original order
        let forward = store.to_vec();
        assert_eq!(forward.len(), 2);
        assert_eq!(forward[0].num_rows(), 10);
        assert_eq!(forward[1].num_rows(), 5);

        // Verify first row of first batch is id=0
        let ids = forward[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(9), 9);

        // Reversed order: batches in reverse order, rows within each batch also reversed
        let reversed = store.to_vec_reversed().unwrap();
        assert_eq!(reversed.len(), 2);
        assert_eq!(reversed[0].num_rows(), 5); // batch2 comes first
        assert_eq!(reversed[1].num_rows(), 10); // batch1 comes second

        // Verify batch2 rows are reversed: [4, 3, 2, 1, 0] instead of [0, 1, 2, 3, 4]
        let ids = reversed[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 4); // Was last, now first
        assert_eq!(ids.value(4), 0); // Was first, now last

        // Verify batch1 rows are reversed: [9, 8, ..., 0] instead of [0, 1, ..., 9]
        let ids = reversed[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 9); // Was last, now first
        assert_eq!(ids.value(9), 0); // Was first, now last
    }

    #[test]
    fn test_iter_reversed() {
        let store = BatchStore::with_capacity(10);

        for i in 0..5 {
            store.append(create_test_batch(10 * (i + 1))).unwrap();
        }

        // Forward iteration: batch positions 0, 1, 2, 3, 4
        let forward: Vec<_> = store.iter().map(|b| b.batch_position).collect();
        assert_eq!(forward, vec![0, 1, 2, 3, 4]);

        // Reversed iteration: batch positions 4, 3, 2, 1, 0 (newest first)
        let reversed: Vec<_> = store.iter_reversed().map(|b| b.batch_position).collect();
        assert_eq!(reversed, vec![4, 3, 2, 1, 0]);

        // Verify row counts match
        let forward_rows: Vec<_> = store.iter().map(|b| b.num_rows).collect();
        let reversed_rows: Vec<_> = store.iter_reversed().map(|b| b.num_rows).collect();
        assert_eq!(forward_rows, vec![10, 20, 30, 40, 50]);
        assert_eq!(reversed_rows, vec![50, 40, 30, 20, 10]);
    }

    #[test]
    fn test_iter_reversed_empty() {
        let store = BatchStore::with_capacity(10);

        let reversed: Vec<_> = store.iter_reversed().collect();
        assert!(reversed.is_empty());
    }

    #[test]
    fn test_concurrent_readers() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(BatchStore::with_capacity(100));

        // Pre-populate with some batches
        for _ in 0..50 {
            store.append(create_test_batch(10)).unwrap();
        }

        // Spawn multiple reader threads
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let reader_store = store.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let len = reader_store.len();
                        assert_eq!(len, 50);

                        // Verify we can read all batches
                        for i in 0..len {
                            let batch = reader_store.get(i);
                            assert!(batch.is_some());
                            assert_eq!(batch.unwrap().num_rows, 10);
                        }

                        // Verify iterator
                        let count = reader_store.iter().count();
                        assert_eq!(count, 50);

                        thread::yield_now();
                    }
                })
            })
            .collect();

        for r in readers {
            r.join().unwrap();
        }
    }

    #[test]
    fn test_append_batches() {
        let store = BatchStore::with_capacity(10);

        let batches: Vec<_> = (0..5).map(|i| create_test_batch(10 * (i + 1))).collect();
        let results = store.append_batches(batches).unwrap();

        assert_eq!(results.len(), 5);
        assert_eq!(store.len(), 5);

        // Check batch positions are sequential
        for (i, (batch_pos, _, _)) in results.iter().enumerate() {
            assert_eq!(*batch_pos, i);
        }

        // Check row offsets are cumulative
        assert_eq!(results[0].1, 0); // First batch starts at 0
        assert_eq!(results[1].1, 10); // After 10 rows
        assert_eq!(results[2].1, 30); // After 10 + 20 rows
        assert_eq!(results[3].1, 60); // After 10 + 20 + 30 rows
        assert_eq!(results[4].1, 100); // After 10 + 20 + 30 + 40 rows

        // Check total rows
        assert_eq!(store.total_rows(), 10 + 20 + 30 + 40 + 50);
    }

    #[test]
    fn test_append_batches_capacity_check() {
        let store = BatchStore::with_capacity(3);

        // Append 2 batches, should succeed
        let batches: Vec<_> = (0..2).map(|_| create_test_batch(10)).collect();
        store.append_batches(batches).unwrap();
        assert_eq!(store.len(), 2);

        // Try to append 2 more, should fail (only 1 slot left)
        let batches: Vec<_> = (0..2).map(|_| create_test_batch(10)).collect();
        let result = store.append_batches(batches);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), StoreFull);

        // Store should be unchanged
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_append_batches_empty() {
        let store = BatchStore::with_capacity(10);

        let results = store.append_batches(vec![]).unwrap();
        assert!(results.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_concurrent_read_write() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use std::thread;

        let store = Arc::new(BatchStore::with_capacity(200));
        let done = Arc::new(AtomicBool::new(false));

        // Writer thread (single writer)
        let writer_store = store.clone();
        let writer_done = done.clone();
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                writer_store.append(create_test_batch(10)).unwrap();
                thread::yield_now();
            }
            writer_done.store(true, Ordering::Release);
        });

        // Reader threads (concurrent readers)
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let reader_store = store.clone();
                let reader_done = done.clone();
                thread::spawn(move || {
                    while !reader_done.load(Ordering::Acquire) {
                        let len = reader_store.len();

                        // Every batch we can see should be valid
                        for i in 0..len {
                            let batch = reader_store.get(i);
                            assert!(batch.is_some());
                        }

                        thread::yield_now();
                    }

                    // Final check - should see all 100 batches
                    assert_eq!(reader_store.len(), 100);
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }
}
