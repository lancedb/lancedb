// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Physical Optimizer Rules

use std::sync::Arc;

use super::TakeExec;
use super::filtered_read::FilteredReadExec;
use arrow_schema::Schema as ArrowSchema;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result as DFResult,
    physical_optimizer::{PhysicalOptimizerRule, optimizer::PhysicalOptimizer},
    physical_plan::{ExecutionPlan, projection::ProjectionExec},
};
use datafusion_physical_expr::{PhysicalExpr, expressions::Column};

/// Rule that eliminates take nodes that are immediately followed by another
/// take node, fetching the union of the columns in a single node instead.
///
/// A "take" is either a [TakeExec] (legacy storage) or a [FilteredReadExec]
/// with a row-stream source (see `FilteredReadExec::row_stream_input`); the
/// scanner emits stacked takes in some plan shapes (e.g. filter columns then
/// projection columns).
#[derive(Debug)]
pub struct CoalesceTake;

impl CoalesceTake {
    /// Whether `plan` is a take node this rule knows how to collapse
    fn as_take(plan: &Arc<dyn ExecutionPlan>) -> Option<&dyn ExecutionPlan> {
        if plan.downcast_ref::<TakeExec>().is_some() {
            Some(plan.as_ref())
        } else if let Some(filtered_read) = plan.downcast_ref::<FilteredReadExec>() {
            filtered_read
                .row_stream_input()
                .is_some()
                .then_some(plan.as_ref())
        } else {
            None
        }
    }

    fn field_order_differs(old_schema: &ArrowSchema, new_schema: &ArrowSchema) -> bool {
        old_schema.fields.len() != new_schema.fields.len()
            || old_schema
                .fields
                .iter()
                .zip(&new_schema.fields)
                .any(|(old, new)| old.name() != new.name())
    }

    fn remap_collapsed_output(
        old_schema: &ArrowSchema,
        new_schema: &ArrowSchema,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let mut project_exprs = Vec::with_capacity(old_schema.fields.len());
        for field in &old_schema.fields {
            project_exprs.push((
                Arc::new(Column::new_with_schema(field.name(), new_schema).unwrap())
                    as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));
        }
        Arc::new(ProjectionExec::try_new(project_exprs, plan).unwrap())
    }

    /// Collapse two stacked takes into one, or return None when the rebuilt
    /// node would not produce every column of the original output (the
    /// rebuild re-derives what to fetch from the outer take's projection, so
    /// a column only the inner take fetched can go missing if the outer
    /// projection doesn't cover it)
    fn collapse_takes(
        inner_take: &dyn ExecutionPlan,
        outer_take: &dyn ExecutionPlan,
        outer_exec: Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let inner_take_input = inner_take.children()[0].clone();
        let old_output_schema = outer_take.schema();
        let collapsed = outer_exec.with_new_children(vec![inner_take_input]).ok()?;
        let new_output_schema = collapsed.schema();

        if old_output_schema
            .fields()
            .iter()
            .any(|field| new_output_schema.field_with_name(field.name()).is_err())
        {
            return None;
        }

        // It's possible that collapsing the take can change the field order.  This disturbs DF's planner and
        // so we must restore it.
        if Self::field_order_differs(&old_output_schema, &new_output_schema) {
            Some(Self::remap_collapsed_output(
                &old_output_schema,
                &new_output_schema,
                collapsed,
            ))
        } else {
            Some(collapsed)
        }
    }
}

impl PhysicalOptimizerRule for CoalesceTake {
    #[allow(deprecated)]
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|plan| {
                if let Some(outer_take) = Self::as_take(&plan) {
                    let child = outer_take.children()[0];
                    // Case 1: take -> take
                    if let Some(inner_take) = Self::as_take(child) {
                        if let Some(collapsed) =
                            Self::collapse_takes(inner_take, outer_take, plan.clone())
                        {
                            return Ok(Transformed::yes(collapsed));
                        }
                    // Case 2: take -> CoalesceBatchesExec -> take
                    } else if let Some(exec_child) = child.downcast_ref::<CoalesceBatchesExec>() {
                        let inner_child = exec_child.children()[0].clone();
                        if let Some(inner_take) = Self::as_take(&inner_child)
                            && let Some(collapsed) =
                                Self::collapse_takes(inner_take, outer_take, plan.clone())
                        {
                            return Ok(Transformed::yes(collapsed));
                        }
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "coalesce_take"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rule that eliminates [ProjectionExec] nodes that projects all columns
/// from its input with no additional expressions.
#[derive(Debug)]
pub struct SimplifyProjection;

impl PhysicalOptimizerRule for SimplifyProjection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|plan| {
                if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
                    let children = proj.children();
                    if children.len() != 1 {
                        return Ok(Transformed::no(plan));
                    }

                    let input = children[0];

                    // TODO: we could try to coalesce consecutive projections, something for later
                    // For now, we just keep things simple and only remove NoOp projections

                    // output has different schema, projection needed
                    if input.schema() != proj.schema() {
                        return Ok(Transformed::no(plan));
                    }

                    if proj.expr().iter().enumerate().all(|(index, proj_expr)| {
                        if let Some(expr) = proj_expr.expr.downcast_ref::<Column>() {
                            // no renaming, no reordering
                            expr.index() == index && expr.name() == proj_expr.alias
                        } else {
                            false
                        }
                    }) {
                        return Ok(Transformed::yes(input.clone()));
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "simplify_projection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub fn get_physical_optimizer() -> PhysicalOptimizer {
    PhysicalOptimizer::with_rules(vec![
        // Rewrite `COUNT(*)`-style aggregates into CountFromMaskExec so they
        // can be answered without scanning column data. Runs before the
        // generic rules so they don't see the rewritten subtree.
        Arc::new(crate::io::exec::count_pushdown::CountPushdown),
        Arc::new(crate::io::exec::optimizer::CoalesceTake),
        Arc::new(crate::io::exec::optimizer::SimplifyProjection),
        // Push down limit into FilteredReadExec and other Execs via with_fetch()
        Arc::new(datafusion::physical_optimizer::limit_pushdown::LimitPushdown::new()),
        // Insert exchange nodes (RepartitionExec, CoalescePartitionsExec) where needed
        // to satisfy distribution requirements as exec nodes migrate to multi-partition output.
        Arc::new(datafusion::physical_optimizer::enforce_distribution::EnforceDistribution::new()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::cast::AsArray;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::TryStreamExt;
    use lance_core::ROW_ID;
    use lance_core::datatypes::OnMissing;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datafusion::exec::OneShotExec;
    use lance_file::version::LanceFileVersion;

    use crate::dataset::{Dataset, WriteParams};
    use crate::io::exec::filtered_read::{FilteredReadExec, FilteredReadOptions};

    /// 20 rows, one fragment, columns i (Int32) and s (Utf8)
    async fn fixture(storage_version: LanceFileVersion) -> (Arc<Dataset>, TempStrDir) {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..20)),
                Arc::new(StringArray::from_iter_values(
                    (0..20).map(|v| format!("s-{v}")),
                )),
            ],
        )
        .unwrap();
        let tmp_dir = TempStrDir::default();
        let uri = tmp_dir.as_str();
        let params = WriteParams {
            data_storage_version: Some(storage_version),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        Dataset::write(reader, uri, Some(params)).await.unwrap();
        (Arc::new(Dataset::open(uri).await.unwrap()), tmp_dir)
    }

    /// An input plan producing one batch of `_rowid` keys
    fn keys_input(keys: Vec<u64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            ROW_ID,
            DataType::UInt64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(keys))]).unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]);
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        Arc::new(OneShotExec::new(stream))
    }

    fn row_stream_take(
        dataset: &Arc<Dataset>,
        input: Arc<dyn ExecutionPlan>,
        columns: &[&str],
    ) -> Arc<dyn ExecutionPlan> {
        // Mirror Scanner::take: full target projection, carried identity kept
        let mut projection = dataset
            .empty_projection()
            .union_columns(columns, OnMissing::Error)
            .unwrap();
        projection.with_row_id = true;
        Arc::new(
            FilteredReadExec::try_new(
                dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(input),
            )
            .unwrap(),
        )
    }

    async fn run(plan: &Arc<dyn ExecutionPlan>) -> RecordBatch {
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let schema = stream.schema();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        concat_batches(&schema, batches.iter()).unwrap()
    }

    fn count_takes(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let self_count = CoalesceTake::as_take(plan).map(|_| 1).unwrap_or(0);
        self_count
            + plan
                .children()
                .iter()
                .map(|child| count_takes(child))
                .sum::<usize>()
    }

    /// Stacked row-stream takes collapse into one node fetching both takes'
    /// columns, preserving the output schema and values
    #[tokio::test]
    async fn collapse_row_stream_takes() {
        let (dataset, _tmp) = fixture(LanceFileVersion::Stable).await;

        // OneShotExec inputs are single-use: build the plan fresh per run
        let build = |dataset: &Arc<Dataset>| {
            let inner = row_stream_take(dataset, keys_input(vec![3, 1, 4]), &["s"]);
            row_stream_take(dataset, inner, &["i", "s"])
        };
        let outer = build(&dataset);
        let expected_schema = outer.schema();
        assert_eq!(count_takes(&outer), 2);
        let expected = run(&outer).await;

        let optimized = CoalesceTake
            .optimize(build(&dataset), &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_takes(&optimized), 1);
        assert_eq!(optimized.schema(), expected_schema);

        let result = run(&optimized).await;
        assert_eq!(result, expected);
        let i_col = result
            .column_by_name("i")
            .unwrap()
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(i_col.values(), &[3, 1, 4]);
    }

    /// When the outer take's projection does not cover a column the inner
    /// take fetched, the collapse is skipped instead of dropping the column
    #[tokio::test]
    async fn collapse_skipped_when_column_would_drop() {
        let (dataset, _tmp) = fixture(LanceFileVersion::Stable).await;

        // Outer target deliberately omits the inner take's "s"
        let build = |dataset: &Arc<Dataset>| {
            let inner = row_stream_take(dataset, keys_input(vec![3, 1, 4]), &["s"]);
            row_stream_take(dataset, inner, &["i"])
        };
        let outer = build(&dataset);
        assert_eq!(count_takes(&outer), 2);
        let expected = run(&outer).await;

        let optimized = CoalesceTake
            .optimize(build(&dataset), &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_takes(&optimized), 2);
        assert_eq!(run(&optimized).await, expected);
        assert!(expected.column_by_name("s").is_some());
    }

    /// Legacy TakeExec pairs still collapse (through the CoalesceBatchesExec
    /// the scanner inserts on that path)
    #[tokio::test]
    async fn collapse_legacy_takes() {
        let (dataset, _tmp) = fixture(LanceFileVersion::Legacy).await;

        let build = |dataset: &Arc<Dataset>| -> Arc<dyn ExecutionPlan> {
            let inner_proj = dataset
                .empty_projection()
                .union_columns(["s"], OnMissing::Error)
                .unwrap();
            let inner: Arc<dyn ExecutionPlan> = Arc::new(
                TakeExec::try_new(dataset.clone(), keys_input(vec![3, 1, 4]), inner_proj)
                    .unwrap()
                    .unwrap(),
            );
            let outer_proj = dataset
                .empty_projection()
                .union_columns(["i", "s"], OnMissing::Error)
                .unwrap();
            Arc::new(
                TakeExec::try_new(dataset.clone(), inner, outer_proj)
                    .unwrap()
                    .unwrap(),
            )
        };
        let outer = build(&dataset);
        let expected_schema = outer.schema();
        assert_eq!(count_takes(&outer), 2);
        let expected = run(&outer).await;

        let optimized = CoalesceTake
            .optimize(build(&dataset), &ConfigOptions::default())
            .unwrap();
        assert_eq!(count_takes(&optimized), 1);
        assert_eq!(optimized.schema(), expected_schema);
        assert_eq!(run(&optimized).await, expected);
    }
}
