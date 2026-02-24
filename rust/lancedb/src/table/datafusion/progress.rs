// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Progress monitoring for write operations.
//!
//! [`PlanProgressMonitor`] polls DataFusion plan metrics at regular intervals,
//! allowing callers to observe progress (rows written, bytes processed, etc.)
//! during long-running operations like [`crate::table::Table::add`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion_physical_plan::metrics::MetricValue;
use datafusion_physical_plan::ExecutionPlan;

/// Progress snapshot for a write operation.
#[derive(Debug, Clone)]
pub struct PlanProgress {
    /// Wall-clock time since monitoring started.
    elapsed: Duration,

    /// Number of output rows reported so far.
    output_rows: usize,

    /// Number of output bytes reported so far.
    output_bytes: usize,

    /// Total rows expected, if known from partition statistics.
    total_rows: Option<usize>,
}

impl PlanProgress {
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn output_rows(&self) -> usize {
        self.output_rows
    }

    pub fn output_bytes(&self) -> usize {
        self.output_bytes
    }

    pub fn total_rows(&self) -> Option<usize> {
        self.total_rows
    }
}

fn collect_root_metrics(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize, Option<usize>) {
    let metrics = plan.metrics();
    let stats = plan.partition_statistics(None);

    let output_rows = metrics.as_ref().and_then(|m| m.output_rows()).unwrap_or(0);

    let output_bytes = metrics
        .as_ref()
        .and_then(|m| {
            m.sum(|metric| matches!(metric.value(), MetricValue::OutputBytes(_)))
                .map(|v| v.as_usize())
        })
        .unwrap_or(0);

    let total_rows = stats.ok().and_then(|s| s.num_rows.get_value().copied());

    (output_rows, output_bytes, total_rows)
}

/// Callback type for progress updates.
pub type ProgressCallback = Arc<dyn Fn(PlanProgress) + Send + Sync>;

/// Monitors a DataFusion execution plan by polling its metrics at regular
/// intervals. The monitor runs a background tokio task that is aborted when
/// the monitor is dropped.
pub struct PlanProgressMonitor {
    handle: tokio::task::JoinHandle<()>,
}

impl PlanProgressMonitor {
    /// Start monitoring `plan`, invoking `callback` every `interval`.
    ///
    /// The plan must be the same `Arc` that is passed to `execute_plan` so
    /// that the shared atomic metric counters are visible to the monitor.
    pub fn start(
        plan: Arc<dyn ExecutionPlan>,
        callback: ProgressCallback,
        interval: Duration,
    ) -> Self {
        let start = Instant::now();
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                let (output_rows, output_bytes, total_rows) = collect_root_metrics(&plan);
                let progress = PlanProgress {
                    elapsed: start.elapsed(),
                    output_rows,
                    output_bytes,
                    total_rows,
                };
                callback(progress);
            }
        });
        Self { handle }
    }
}

impl Drop for PlanProgressMonitor {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Create a progress callback that displays a terminal progress bar tracking
/// rows written.
///
/// The bar shows rows processed, throughput, and ETA when the total row count
/// is known. It is finished automatically when dropped.
///
/// Requires the `progress` feature.
#[cfg(feature = "progress")]
pub fn progress_bar_callback() -> impl Fn(PlanProgress) + Send + Sync + 'static {
    use indicatif::{HumanBytes, ProgressBar, ProgressFinish, ProgressStyle};
    use std::sync::OnceLock;

    let bar: Arc<OnceLock<ProgressBar>> = Arc::new(OnceLock::new());

    move |p: PlanProgress| {
        let bar = bar.get_or_init(|| {
            let pb = match p.total_rows {
                Some(total) => ProgressBar::new(total as u64),
                None => ProgressBar::new_spinner(),
            };
            pb.set_style(
                ProgressStyle::with_template(
                    "{msg} [{bar:40}] {pos}/{len} rows ({per_sec}, eta {eta})",
                )
                .unwrap()
                .progress_chars("=> "),
            );
            pb.set_message("Writing");
            pb.with_finish(ProgressFinish::AndLeave)
        });

        bar.set_position(p.output_rows as u64);
        bar.set_message(format!("Writing ({})", HumanBytes(p.output_bytes as u64)));

        if let Some(total) = p.total_rows {
            if bar.length() != Some(total as u64) {
                bar.set_length(total as u64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }

    #[cfg(feature = "progress")]
    #[tokio::test]
    async fn test_progress_bar() {
        let db = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let table = db
            .create_table("progress_bar_test", batch)
            .execute()
            .await
            .unwrap();

        let new_data = record_batch!(("id", Int32, [4, 5, 6])).unwrap();
        table.add(new_data).progress_bar().execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn test_collect_root_metrics() {
        let new_data = vec![record_batch!(("id", Int32, [4, 5])).unwrap()];
        let scannable =
            crate::table::datafusion::scannable_exec::ScannableExec::new(Box::new(new_data));
        let plan: Arc<dyn datafusion_physical_plan::ExecutionPlan> = Arc::new(scannable);

        // Execute to populate metrics
        let stream =
            lance_datafusion::exec::execute_plan(plan.clone(), Default::default()).unwrap();
        let _batches: Vec<_> = futures::TryStreamExt::try_collect(stream).await.unwrap();

        let (output_rows, output_bytes, _total_rows) = collect_root_metrics(&plan);
        assert_eq!(output_rows, 2);
        assert!(output_bytes > 0);
    }
}
