// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Progress monitoring for write operations.
//!
//! [`WriteProgressTracker`] is injected into [`super::scannable_exec::ScannableExec`]
//! and called synchronously as each batch passes through the scan node, giving
//! callers per-batch progress updates during [`crate::table::Table::add`].

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
pub struct WriteProgressTracker {
    rows_any_bytes: std::sync::Mutex<(usize, usize)>,
    start: Instant,
    /// Known total rows from the input source, if available.
    total_rows: Option<usize>,
    callback: ProgressCallback,
}

impl WriteProgressTracker {
    pub fn new(callback: ProgressCallback, total_rows: Option<usize>) -> Self {
        Self {
            rows_any_bytes: std::sync::Mutex::new((0, 0)),
            start: Instant::now(),
            total_rows,
            callback,
        }
    }

    /// Record a batch of rows passing through the scan node.
    pub fn record_batch(&self, rows: usize, bytes: usize) {
        let progress = {
            // We hold a lock here to update the rows/bytes counts atomically,
            // since multiple batches may be processed concurrently.
            let mut guard = self.rows_any_bytes.lock().unwrap();
            guard.0 += rows;
            guard.1 += bytes;
            WriteProgress {
                elapsed: self.start.elapsed(),
                output_rows: guard.0,
                output_bytes: guard.1,
                total_rows: self.total_rows,
                done: false,
            }
        };
        (self.callback)(&progress);
    }

    /// Emit the final progress callback indicating the write is complete.
    ///
    /// `total_rows` is always `Some` on the final callback: it uses the known
    /// total if available, or falls back to the number of rows actually written.
    pub fn finish(&self) {
        let progress = {
            let guard = self.rows_any_bytes.lock().unwrap();
            WriteProgress {
                elapsed: self.start.elapsed(),
                output_rows: guard.0,
                output_bytes: guard.1,
                total_rows: Some(self.total_rows.unwrap_or(guard.0)),
                done: true,
            }
        };
        (self.callback)(&progress);
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
        let cb_count = callback_count.clone();
        let cb_rows = last_rows.clone();

        let new_data = record_batch!(("id", Int32, [4, 5, 6])).unwrap();
        table
            .add(new_data)
            .progress(move |p| {
                cb_count.fetch_add(1, Ordering::SeqCst);
                cb_rows.store(p.output_rows(), Ordering::SeqCst);
            })
            .execute()
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 6);
        assert!(callback_count.load(Ordering::SeqCst) >= 1);
        // Progress tracks the newly inserted rows, not the total table size.
        assert_eq!(last_rows.load(Ordering::SeqCst), 3);
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
