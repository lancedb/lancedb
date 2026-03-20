// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Progress monitoring for write operations.
//!
//! You can add a callback to process progress in [`crate::table::AddDataBuilder::progress`].
//! [`WriteProgress`] is the struct passed to the callback.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Progress snapshot for a write operation.
#[derive(Debug, Clone)]
pub struct WriteProgress {
    // These are private and only accessible via getters, to make it easy to add
    // new fields without breaking existing callbacks.
    elapsed: Duration,
    output_rows: usize,
    output_bytes: usize,
    total_rows: Option<usize>,
    active_tasks: usize,
    total_tasks: usize,
    done: bool,
}

impl WriteProgress {
    /// Wall-clock time since monitoring started.
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Number of rows written so far.
    pub fn output_rows(&self) -> usize {
        self.output_rows
    }

    /// Number of bytes written so far.
    pub fn output_bytes(&self) -> usize {
        self.output_bytes
    }

    /// Total rows expected.
    ///
    /// Populated when the input source reports a row count (e.g. a
    /// [`arrow_array::RecordBatch`]).  Always `Some` when [`WriteProgress::done`]
    /// is `true` — falling back to the actual number of rows written.
    pub fn total_rows(&self) -> Option<usize> {
        self.total_rows
    }

    /// Number of parallel write tasks currently in flight.
    pub fn active_tasks(&self) -> usize {
        self.active_tasks
    }

    /// Total number of parallel write tasks (i.e. the write parallelism).
    pub fn total_tasks(&self) -> usize {
        self.total_tasks
    }

    /// Whether the write operation has completed.
    ///
    /// The final callback always has `done = true`.  Callers can use this to
    /// finalize progress bars or perform cleanup.
    pub fn done(&self) -> bool {
        self.done
    }
}

/// Callback type for progress updates.
pub type ProgressCallback = Arc<dyn Fn(&WriteProgress) + Send + Sync>;

/// Tracks progress of a write operation and invokes a [`ProgressCallback`].
///
/// Call [`WriteProgressTracker::record_batch`] for each batch written.
/// Call [`WriteProgressTracker::finish`] once after all data is written.
impl std::fmt::Debug for WriteProgressTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteProgressTracker")
            .field("total_rows", &self.total_rows)
            .finish()
    }
}

pub(crate) struct WriteProgressTracker {
    rows_and_bytes: std::sync::Mutex<(usize, usize)>,
    /// Wire bytes tracked separately by the insert layer. When set (> 0),
    /// this takes precedence over the in-memory bytes from `rows_and_bytes`.
    wire_bytes: AtomicUsize,
    active_tasks: Arc<AtomicUsize>,
    total_tasks: AtomicUsize,
    start: Instant,
    /// Known total rows from the input source, if available.
    total_rows: Option<usize>,
    callback: ProgressCallback,
}

impl WriteProgressTracker {
    pub fn new(callback: ProgressCallback, total_rows: Option<usize>) -> Self {
        Self {
            rows_and_bytes: std::sync::Mutex::new((0, 0)),
            wire_bytes: AtomicUsize::new(0),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            total_tasks: AtomicUsize::new(1),
            start: Instant::now(),
            total_rows,
            callback,
        }
    }

    /// Set the total number of parallel write tasks (the write parallelism).
    pub fn set_total_tasks(&self, n: usize) {
        self.total_tasks.store(n, Ordering::Relaxed);
    }

    /// Increment the active task count. Returns a guard that decrements on drop.
    pub fn track_task(&self) -> ActiveTaskGuard {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
        ActiveTaskGuard(self.active_tasks.clone())
    }

    /// Record a batch of rows passing through the scan node.
    pub fn record_batch(&self, rows: usize, bytes: usize) {
        let progress = {
            // We hold a lock here to update the rows/bytes counts atomically,
            // since multiple batches may be processed concurrently.
            let mut guard = self.rows_and_bytes.lock().unwrap();
            guard.0 += rows;
            guard.1 += bytes;
            self.snapshot(guard.0, guard.1, false)
        };
        (self.callback)(&progress);
    }

    /// Record wire bytes from the insert layer (e.g. IPC-encoded bytes for
    /// remote writes). When wire bytes are recorded, they take precedence over
    /// the in-memory Arrow bytes tracked by [`record_batch`].
    pub fn record_bytes(&self, bytes: usize) {
        self.wire_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Emit the final progress callback indicating the write is complete.
    ///
    /// `total_rows` is always `Some` on the final callback: it uses the known
    /// total if available, or falls back to the number of rows actually written.
    pub fn finish(&self) {
        let progress = {
            let guard = self.rows_and_bytes.lock().unwrap();
            let mut snap = self.snapshot(guard.0, guard.1, true);
            snap.total_rows = Some(self.total_rows.unwrap_or(guard.0));
            snap
        };
        (self.callback)(&progress);
    }

    fn snapshot(&self, rows: usize, in_memory_bytes: usize, done: bool) -> WriteProgress {
        let wire = self.wire_bytes.load(Ordering::Relaxed);
        // Prefer wire bytes (actual I/O size) when the insert layer is
        // tracking them; fall back to in-memory Arrow size otherwise.
        // TODO: for local writes, track actual bytes written by Lance
        // instead of using in-memory Arrow size as a proxy.
        let output_bytes = if wire > 0 { wire } else { in_memory_bytes };
        WriteProgress {
            elapsed: self.start.elapsed(),
            output_rows: rows,
            output_bytes,
            total_rows: self.total_rows,
            active_tasks: self.active_tasks.load(Ordering::Relaxed),
            total_tasks: self.total_tasks.load(Ordering::Relaxed),
            done,
        }
    }
}

/// RAII guard that decrements the active task count when dropped.
pub(crate) struct ActiveTaskGuard(Arc<AtomicUsize>);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow_array::record_batch;

    use crate::connect;

    #[tokio::test]
    async fn test_progress_monitor_fires_callback() {
        let db = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let table = db
            .create_table("progress_test", batch)
            .execute()
            .await
            .unwrap();

        let callback_count = Arc::new(AtomicUsize::new(0));
        let last_rows = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let last_total_tasks = Arc::new(AtomicUsize::new(0));
        let cb_count = callback_count.clone();
        let cb_rows = last_rows.clone();
        let cb_active = max_active.clone();
        let cb_total_tasks = last_total_tasks.clone();

        let new_data = record_batch!(("id", Int32, [4, 5, 6])).unwrap();
        table
            .add(new_data)
            .progress(move |p| {
                cb_count.fetch_add(1, Ordering::SeqCst);
                cb_rows.store(p.output_rows(), Ordering::SeqCst);
                cb_active.fetch_max(p.active_tasks(), Ordering::SeqCst);
                cb_total_tasks.store(p.total_tasks(), Ordering::SeqCst);
            })
            .execute()
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 6);
        assert!(callback_count.load(Ordering::SeqCst) >= 1);
        // Progress tracks the newly inserted rows, not the total table size.
        assert_eq!(last_rows.load(Ordering::SeqCst), 3);
        // At least one callback should have seen an active task.
        assert!(max_active.load(Ordering::SeqCst) >= 1);
        // total_tasks should reflect the write parallelism.
        assert!(last_total_tasks.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_progress_done_fires_at_end() {
        let db = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let table = db
            .create_table("progress_done", batch)
            .execute()
            .await
            .unwrap();

        let seen_done = Arc::new(std::sync::Mutex::new(Vec::<bool>::new()));
        let seen = seen_done.clone();

        let new_data = record_batch!(("id", Int32, [4, 5, 6])).unwrap();
        table
            .add(new_data)
            .progress(move |p| {
                seen.lock().unwrap().push(p.done());
            })
            .execute()
            .await
            .unwrap();

        let done_flags = seen_done.lock().unwrap();
        assert!(!done_flags.is_empty(), "at least one callback must fire");
        // Only the last callback should have done=true.
        let last = *done_flags.last().unwrap();
        assert!(last, "last callback must have done=true");
        // All earlier callbacks should have done=false.
        for &d in done_flags.iter().rev().skip(1) {
            assert!(!d, "non-final callbacks must have done=false");
        }
    }

    #[tokio::test]
    async fn test_progress_total_rows_known() {
        let db = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let table = db
            .create_table("total_known", batch)
            .execute()
            .await
            .unwrap();

        let seen_total = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = seen_total.clone();

        // RecordBatch implements Scannable with num_rows() -> Some(3)
        let new_data = record_batch!(("id", Int32, [4, 5, 6])).unwrap();
        table
            .add(new_data)
            .progress(move |p| {
                seen.lock().unwrap().push(p.total_rows());
            })
            .execute()
            .await
            .unwrap();

        let totals = seen_total.lock().unwrap();
        // All callbacks (including done) should have total_rows = Some(3)
        assert!(
            totals.contains(&Some(3)),
            "expected total_rows=Some(3) in at least one callback, got: {:?}",
            *totals
        );
    }

    #[tokio::test]
    async fn test_progress_total_rows_unknown() {
        use arrow_array::RecordBatchIterator;

        let db = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let table = db
            .create_table("total_unknown", batch)
            .execute()
            .await
            .unwrap();

        let seen_total = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = seen_total.clone();

        // RecordBatchReader does not provide num_rows, so total_rows should be
        // None in intermediate callbacks but always Some on the done callback.
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]);
        let new_data: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(
                vec![Ok(record_batch!(("id", Int32, [4, 5, 6])).unwrap())],
                Arc::new(schema),
            ));
        table
            .add(new_data)
            .progress(move |p| {
                seen.lock().unwrap().push((p.total_rows(), p.done()));
            })
            .execute()
            .await
            .unwrap();

        let entries = seen_total.lock().unwrap();
        assert!(!entries.is_empty(), "at least one callback must fire");
        for (total, done) in entries.iter() {
            if *done {
                assert!(
                    total.is_some(),
                    "done callback must have total_rows set, got: {:?}",
                    total
                );
            } else {
                assert_eq!(
                    *total, None,
                    "intermediate callback must have total_rows=None, got: {:?}",
                    total
                );
            }
        }
    }
}
