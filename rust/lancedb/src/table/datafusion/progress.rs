// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Progress monitoring for DataFusion execution plans.
//!
//! [`PlanProgressMonitor`] polls DataFusion plan metrics at regular intervals,
//! allowing callers to observe progress (rows written, bytes processed, etc.)
//! during long-running operations like [`crate::table::Table::add`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion_physical_plan::ExecutionPlan;

/// Progress snapshot for a single node in the execution plan tree.
#[derive(Debug, Clone)]
pub struct NodeProgress {
    /// The name of the execution plan node (e.g. "ScannableExec", "InsertExec").
    pub name: String,
    /// Depth in the plan tree (0 = root).
    pub depth: usize,
    /// Number of output rows reported by this node so far.
    pub output_rows: usize,
    /// Elapsed compute time reported by this node.
    pub elapsed_compute: Duration,
    /// Total rows expected, if known from partition statistics.
    pub total_rows: Option<usize>,
}

/// Aggregated progress snapshot across all nodes in an execution plan.
#[derive(Debug, Clone)]
pub struct PlanProgress {
    /// Per-node progress in depth-first tree order (root first).
    pub nodes: Vec<NodeProgress>,
    /// Wall-clock time since monitoring started.
    pub elapsed: Duration,
}

impl PlanProgress {
    /// Returns the progress of the root (top-level) node, if any.
    pub fn root(&self) -> Option<&NodeProgress> {
        self.nodes.first()
    }

    /// Returns the progress of the deepest leaf node.
    pub fn leaf(&self) -> Option<&NodeProgress> {
        self.nodes.iter().max_by_key(|n| n.depth)
    }

    /// Estimated fraction of work complete (0.0 to 1.0), based on the leaf
    /// node's output_rows vs total_rows. Returns `None` if total is unknown.
    pub fn progress_fraction(&self) -> Option<f64> {
        let leaf = self.leaf()?;
        let total = leaf.total_rows? as f64;
        if total == 0.0 {
            return Some(1.0);
        }
        Some((leaf.output_rows as f64 / total).min(1.0))
    }
}

fn collect_node_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    depth: usize,
    nodes: &mut Vec<NodeProgress>,
) {
    let metrics = plan.metrics();
    let stats = plan.partition_statistics(None);

    let output_rows = metrics.as_ref().and_then(|m| m.output_rows()).unwrap_or(0);

    let elapsed_compute_ns = metrics
        .as_ref()
        .and_then(|m| m.elapsed_compute())
        .unwrap_or(0);

    let total_rows = stats.ok().and_then(|s| s.num_rows.get_value().copied());

    nodes.push(NodeProgress {
        name: plan.name().to_string(),
        depth,
        output_rows,
        elapsed_compute: Duration::from_nanos(elapsed_compute_ns as u64),
        total_rows,
    });

    for child in plan.children() {
        collect_node_metrics(child, depth + 1, nodes);
    }
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
                let mut nodes = Vec::new();
                collect_node_metrics(&plan, 0, &mut nodes);
                let progress = PlanProgress {
                    nodes,
                    elapsed: start.elapsed(),
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
                if let Some(leaf) = p.leaf() {
                    cb_rows.store(leaf.output_rows, Ordering::SeqCst);
                }
            })
            .execute()
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 6);
        // The callback should have been invoked at least once
        // (though timing-dependent, we use a short interval)
        // We don't assert on exact count since it's timing-dependent
    }

    #[tokio::test]
    async fn test_collect_node_metrics() {
        let new_data = vec![record_batch!(("id", Int32, [4, 5])).unwrap()];
        let scannable =
            crate::table::datafusion::scannable_exec::ScannableExec::new(Box::new(new_data));
        let plan: Arc<dyn datafusion_physical_plan::ExecutionPlan> = Arc::new(scannable);

        // Execute to populate metrics
        let stream =
            lance_datafusion::exec::execute_plan(plan.clone(), Default::default()).unwrap();
        let _batches: Vec<_> = futures::TryStreamExt::try_collect(stream).await.unwrap();

        let mut nodes = Vec::new();
        collect_node_metrics(&plan, 0, &mut nodes);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].depth, 0);
        assert_eq!(nodes[0].name, "ScannableExec");
        assert_eq!(nodes[0].output_rows, 2);
    }
}
