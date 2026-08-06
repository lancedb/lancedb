// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Physical optimizer rule that rewrites a `COUNT(*)`-style aggregate into
//! [`CountFromMaskExec`], answering the count from index metadata and the
//! deletion mask without scanning column data.
//!
//! This is the "count-from-mask" category of aggregate pushdown — one of
//! four planned. The other categories (mask-to-answer, zone-aware,
//! dimension-keyed) will each need their own rule and exec; the surrounding
//! infrastructure (the `fragment_bitmap` plumbed on each `ScalarIndexSearch`,
//! the `IndexInformationProvider::fragment_bitmap` lookup) is general
//! enough to be reused.
//!
//! Two rewritten shapes are emitted depending on whether the scalar index
//! backing the filter covers every dataset fragment.
//!
//! **Full coverage** (index ⊇ dataset, or no filter at all):
//!
//! ```text
//! AggregateExec(Final, aggs=[count(...)], group_by=[])
//!   └── CountFromMaskExec { prefilter_input = index_input }
//! ```
//!
//! **Partial coverage** (index ⊊ dataset — typically appended fragments):
//!
//! ```text
//! AggregateExec(Final, aggs=[count(...)], group_by=[])
//!   └── UnionExec
//!         ├── CountFromMaskExec(restrict_to_fragments = indexed)
//!         └── AggregateExec(Partial)
//!               └── FilteredReadExec(fragments = unindexed, full_filter = …)
//! ```
//!
//! [`CountFromMaskExec`] emits partial-state, so the outer
//! `AggregateExec(Final)` performs the final combine in either shape.
//!
//! If the prefilter's index coverage is unknown (any leaf is missing
//! `fragment_bitmap`, e.g. constructed outside scanner planning), the rule
//! refuses to fire and leaves the existing scan path in place.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties,
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_partitions::CoalescePartitionsExec,
    projection::ProjectionExec,
    repartition::RepartitionExec,
    union::UnionExec,
};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::expressions::{Column, Literal};
use lance_index::scalar::expression::ScalarIndexExpr;
use log::warn;
use roaring::RoaringBitmap;

use super::count_from_mask::CountFromMaskExec;
use super::filtered_read::{FilteredReadExec, FilteredReadOptions};
use super::scalar_index::ScalarIndexExec;

/// Physical optimizer rule that rewrites a `COUNT(*)`-style aggregate into
/// [`CountFromMaskExec`], optionally splitting into a parallel scan branch
/// when the index has partial coverage of the dataset.
///
/// Only fires when the shape is verifiably safe; everything outside that
/// envelope (GROUP BY, residual filters, scan ranges, etc.) is left alone
/// for the normal scan path.
#[derive(Debug)]
pub struct CountPushdown;

impl PhysicalOptimizerRule for CountPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|plan| {
                let Some(agg) = plan.downcast_ref::<AggregateExec>() else {
                    return Ok(Transformed::no(plan));
                };
                if let Some(rewritten) = try_rewrite(agg)? {
                    return Ok(Transformed::yes(rewritten));
                }
                Ok(Transformed::no(plan))
            })?
            .data)
    }

    fn name(&self) -> &str {
        "count_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn try_rewrite(agg: &AggregateExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    // We can accelerate Single (Lance scanner shape) and Partial (the shape
    // DataFusion's SQL planner emits at the leaf of an aggregate pipeline);
    // both produce results we know how to compute from the index. We will
    // never accelerate Final or FinalPartitioned — those combine an existing
    // partial stream, and the value of this rule is replacing the work that
    // produces the partial stream.
    let mode = match agg.mode() {
        AggregateMode::Single => AggregateMode::Single,
        AggregateMode::Partial => AggregateMode::Partial,
        _ => return Ok(None),
    };
    if !agg.group_expr().is_empty() {
        return Ok(None);
    }
    if agg.aggr_expr().is_empty() {
        return Ok(None);
    }

    // Every aggregate must be a `COUNT(<literal>)` shape (i.e. COUNT(*) /
    // COUNT(1) / etc.) with no per-aggregate `FILTER (WHERE ...)`. This rule
    // is scoped to the count-from-mask category only; other aggregate
    // categories (mask-to-answer, zone-aware, dimension-keyed) will need
    // their own rules with their own gates.
    for (af, filter) in agg.aggr_expr().iter().zip(agg.filter_expr().iter()) {
        if !is_count_star(af) {
            return Ok(None);
        }
        if filter.is_some() {
            return Ok(None);
        }
    }

    // The input must be a FilteredReadExec we can prove is safe to skip.
    // DataFusion's SQL planner inserts a few row-preserving wrappers above
    // the leaf — a `RepartitionExec` for parallelism, an empty
    // `ProjectionExec` once the count expression has been resolved to need
    // no columns, and `CoalesceBatchesExec` here and there. Walk through
    // those to reach the FilteredReadExec.
    let Some(filtered_read) = strip_row_preserving_wrappers(agg.children()[0]) else {
        return Ok(None);
    };

    let options = filtered_read.options();
    // We don't currently support count pushdown when the row selector
    // is a row stream.
    if filtered_read.row_stream_input().is_some() {
        return Ok(None);
    }
    // A refine filter is a residual the index couldn't fully evaluate — it
    // needs column data to apply, which we can't.
    if options.refine_filter.is_some() {
        return Ok(None);
    }
    // A full_filter without an index_input means the filter is evaluated by
    // scanning every row; not pushdownable.
    if options.full_filter.is_some() && filtered_read.index_input().is_none() {
        return Ok(None);
    }
    // LIMIT/OFFSET would change the count.
    if options.scan_range_before_filter.is_some() || options.scan_range_after_filter.is_some() {
        return Ok(None);
    }
    // We rely on the deletion mask being applied; with_deleted_rows changes
    // that contract. Surfacing as a warning because it shouldn't normally
    // pair with an aggregate plan — if we see it, the planner produced a
    // shape we could in principle accelerate but currently can't.
    if options.with_deleted_rows {
        warn!(
            "count_pushdown: skipped because the FilteredReadExec was built \
             with with_deleted_rows; the count will be computed via a full \
             scan."
        );
        return Ok(None);
    }
    // Same story for an explicit fragment subset: legitimate, but unexpected
    // alongside an aggregate, and we lose the pushdown opportunity.
    if options.fragments.is_some() {
        warn!(
            "count_pushdown: skipped because the FilteredReadExec was scoped \
             to an explicit fragment subset; the count will be computed via a \
             full scan. Intersecting that subset into the coverage logic would \
             let this query be answered from index metadata."
        );
        return Ok(None);
    }

    let dataset = filtered_read.dataset().clone();
    let dataset_fragments: RoaringBitmap =
        dataset.fragments().iter().map(|f| f.id as u32).collect();
    let prefilter_input = filtered_read.index_input().cloned();

    // If there is a prefilter, inspect its ScalarIndexExpr leaves:
    //   - Refuse to fire if any leaf is inexact (`needs_recheck`). The
    //     prefilter's serialized batch carries an Exact/AtMost/AtLeast
    //     discriminant but `load_prefilter` only reads the row-address mask
    //     and would treat AtMost as Exact, silently overcounting (and
    //     symmetrically AtLeast would undercount).
    //   - Compute the index's fragment coverage from leaf `fragment_bitmap`s.
    //     `None` means at least one leaf has no bitmap and we can't reason
    //     about coverage synchronously — refuse to fire.
    let index_coverage = match &prefilter_input {
        None => None,
        Some(input) => {
            let scalar_exec = input.downcast_ref::<ScalarIndexExec>().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "count_pushdown: FilteredReadExec.index_input is not a ScalarIndexExec"
                        .to_string(),
                )
            })?;
            if scalar_exec.expr().needs_recheck() {
                return Ok(None);
            }
            let Some(coverage) = collect_coverage(scalar_exec.expr()) else {
                return Ok(None);
            };
            Some(coverage)
        }
    };

    let aggr_exprs: Vec<Arc<AggregateFunctionExpr>> = agg.aggr_expr().to_vec();

    // Decide on the plan shape. Three cases:
    //
    // 1. No prefilter (no filter at all): single pushdown branch over every
    //    dataset fragment. Always safe.
    // 2. Prefilter + index covers every dataset fragment: single pushdown
    //    branch, prefilter feeds in directly.
    // 3. Prefilter + index covers a strict subset: split into pushdown over
    //    indexed fragments + parallel scan over unindexed fragments.
    let (partial_stream, partial_state_schema): (Arc<dyn ExecutionPlan>, _) = match index_coverage {
        None => {
            // No prefilter at all (verified above): nothing to restrict.
            let exec = CountFromMaskExec::try_new_restricted(
                dataset,
                aggr_exprs.clone(),
                prefilter_input,
                None,
            )?;
            let schema = exec.schema();
            (Arc::new(exec), schema)
        }
        Some(coverage) if (&dataset_fragments - &coverage).is_empty() => {
            // Prefilter exists and the index covers every dataset fragment —
            // safe to push the whole count down.
            let exec = CountFromMaskExec::try_new_restricted(
                dataset,
                aggr_exprs.clone(),
                prefilter_input,
                None,
            )?;
            let schema = exec.schema();
            (Arc::new(exec), schema)
        }
        Some(coverage) => {
            // Split plan: CountFromMaskExec for the indexed fragments, a
            // normal scan + AggregateExec(Partial) for the rest.
            let uncovered = &dataset_fragments - &coverage;
            let pushdown_exec = CountFromMaskExec::try_new_restricted(
                dataset,
                aggr_exprs.clone(),
                prefilter_input,
                Some(&dataset_fragments & &coverage),
            )?;
            let partial_state_schema = pushdown_exec.schema();
            let pushdown_branch: Arc<dyn ExecutionPlan> = Arc::new(pushdown_exec);
            let scan_branch =
                build_scan_branch(filtered_read, options, &uncovered, aggr_exprs.clone())?;
            let union: Arc<dyn ExecutionPlan> =
                UnionExec::try_new(vec![pushdown_branch, scan_branch])?;
            (union, partial_state_schema)
        }
    };

    match mode {
        AggregateMode::Partial => {
            // Caller's parent is already an AggregateExec(Final) that knows
            // how to consume multi-partition partial state — substitute our
            // partial stream and we're done.
            Ok(Some(partial_stream))
        }
        AggregateMode::Single => {
            // The original AggregateExec(Single) produced final output in one
            // step; our exec emits partial state, so add a Final on top to
            // recover the original output schema. Final expects a single
            // partition of partial-state rows, so coalesce when we have a
            // union producing multiple partitions.
            let final_input: Arc<dyn ExecutionPlan> =
                if partial_stream.output_partitioning().partition_count() > 1 {
                    Arc::new(CoalescePartitionsExec::new(partial_stream))
                } else {
                    partial_stream
                };
            // `AggregateExec::try_new` requires one
            // `Option<Arc<dyn PhysicalExpr>>` per aggregate expression for
            // the optional per-aggregate `FILTER (WHERE ...)` clause. We
            // rejected any aggregate carrying a filter back at the gate, so
            // every slot is `None` here.
            let filters: Vec<Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>> =
                (0..aggr_exprs.len()).map(|_| None).collect();
            let final_agg = AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                aggr_exprs,
                filters,
                final_input,
                partial_state_schema,
            )?;
            Ok(Some(Arc::new(final_agg)))
        }
        _ => unreachable!("mode was checked at the top of try_rewrite"),
    }
}

/// Build the scan branch of a partial-coverage split: a `FilteredReadExec`
/// restricted to the uncovered fragments (no `index_input`, the original
/// `full_filter` applied per row) wrapped in `AggregateExec(Partial)` so its
/// partial state can be unioned with the pushdown branch.
fn build_scan_branch(
    filtered_read: &FilteredReadExec,
    options: &FilteredReadOptions,
    uncovered: &RoaringBitmap,
    aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let dataset = filtered_read.dataset().clone();
    let uncovered_fragments: Vec<_> = dataset
        .manifest()
        .fragments
        .iter()
        .filter(|f| uncovered.contains(f.id as u32))
        .cloned()
        .collect();
    let mut scan_options = options.clone();
    scan_options.fragments = Some(Arc::new(uncovered_fragments));
    let scan = FilteredReadExec::try_new(dataset, scan_options, None)?;
    let scan: Arc<dyn ExecutionPlan> = Arc::new(scan);
    let scan_schema = scan.schema();
    // Per-aggregate `FILTER (WHERE ...)` placeholders — see the matching
    // comment in `try_rewrite`; we've already rejected any aggregate that
    // carried a filter, so every slot is `None`.
    let filters: Vec<Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>> =
        (0..aggr_exprs.len()).map(|_| None).collect();
    let partial = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        aggr_exprs,
        filters,
        scan,
        scan_schema,
    )?;
    Ok(Arc::new(partial))
}

/// Walk through row-preserving wrappers (`RepartitionExec`,
/// `CoalesceBatchesExec`, and identity-or-empty `ProjectionExec`) that
/// DataFusion's planner inserts between an `AggregateExec` and the leaf, and
/// return the underlying `FilteredReadExec` if one is reached.
///
/// "Row-preserving" here means the wrapper changes neither the number of rows
/// nor the predicate applied to them — it may reshape partitions, batches, or
/// drop unused columns, but the row population at the bottom is what reaches
/// the aggregate. That's all the rule needs from these layers, so it's safe to
/// look past them.
fn strip_row_preserving_wrappers(plan: &Arc<dyn ExecutionPlan>) -> Option<&FilteredReadExec> {
    let mut current: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(filtered_read) = current.downcast_ref::<FilteredReadExec>() {
            return Some(filtered_read);
        }
        let next: &Arc<dyn ExecutionPlan> =
            if let Some(inner) = current.downcast_ref::<RepartitionExec>() {
                inner.input()
            } else if let Some(inner) = {
                #[allow(deprecated)]
                current.downcast_ref::<CoalesceBatchesExec>()
            } {
                inner.input()
            } else if let Some(inner) = current.downcast_ref::<CoalescePartitionsExec>() {
                inner.input()
            } else {
                let proj = current.downcast_ref::<ProjectionExec>()?;
                // Only walk through projections that are row-preserving: every
                // output expression is a direct column reference back to the
                // input. (Empty projections trivially qualify — DataFusion uses
                // one when a `COUNT(*)`'s argument no longer needs any actual
                // columns.)
                let input_schema = proj.input().schema();
                let identity = proj.expr().iter().all(|projection_expr| {
                    projection_expr
                        .expr
                        .downcast_ref::<Column>()
                        .is_some_and(|c| c.name() == input_schema.field(c.index()).name())
                });
                if !identity {
                    return None;
                }
                proj.input()
            };
        current = next.as_ref();
    }
}

/// Walk a `ScalarIndexExpr` and intersect the per-leaf `fragment_bitmap`.
///
/// Returns `None` if any leaf is missing a bitmap (coverage unknown). All
/// three combinators (`And`, `Or`, `Not`) reduce to "every leaf must cover the
/// fragment for us to give a definitive answer about it" — i.e. intersection.
fn collect_coverage(expr: &ScalarIndexExpr) -> Option<RoaringBitmap> {
    match expr {
        ScalarIndexExpr::Not(inner) => collect_coverage(inner),
        ScalarIndexExpr::And(lhs, rhs) | ScalarIndexExpr::Or(lhs, rhs) => {
            let l = collect_coverage(lhs)?;
            let r = collect_coverage(rhs)?;
            Some(l & r)
        }
        ScalarIndexExpr::Query(search) => search.fragment_bitmap.clone(),
    }
}

/// Returns `true` if `af` is `COUNT(<literal>)` with no DISTINCT.
fn is_count_star(af: &Arc<AggregateFunctionExpr>) -> bool {
    if af.fun().name() != "count" {
        return false;
    }
    if af.is_distinct() {
        return false;
    }
    let args = af.expressions();
    if args.len() != 1 {
        return false;
    }
    let Some(lit) = args[0].downcast_ref::<Literal>() else {
        return false;
    };
    // `COUNT(NULL)` would always return 0; rule it out so we don't accidentally
    // produce a wrong answer if the planner ever lets it through.
    !lit.value().is_null()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Int64Type, UInt64Type};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use futures::TryStreamExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::gen_batch;
    use lance_index::IndexType;
    use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};

    use super::*;
    use crate::Dataset;
    use crate::dataset::scanner::AggregateExpr;
    use crate::index::DatasetIndexExt;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    struct Fixture {
        dataset: Arc<Dataset>,
        _tmp: TempStrDir,
    }

    /// 4 fragments × 10 rows, ascending `ordered` column with a BTree index.
    async fn make_fixture() -> Fixture {
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();
        Fixture {
            dataset: Arc::new(dataset),
            _tmp: tmp,
        }
    }

    fn plan_contains_pushdown(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let mut found = false;
        plan.apply(|node| {
            if node.is::<CountFromMaskExec>() {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    fn plan_contains_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let mut found = false;
        plan.apply(|node| {
            if node.is::<UnionExec>() {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        found
    }

    async fn run_count(
        scanner: &mut crate::dataset::scanner::Scanner,
    ) -> (Arc<dyn ExecutionPlan>, i64) {
        scanner
            .aggregate(AggregateExpr::builder().count_star().build())
            .unwrap();
        let plan = scanner.create_plan().await.unwrap();
        let stream = datafusion::physical_plan::execute_stream(
            plan.clone(),
            Arc::new(datafusion::execution::TaskContext::default()),
        )
        .unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(
            batches.len(),
            1,
            "count plan emitted {} batches",
            batches.len()
        );
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>()
            .expect("count column should be Int64")
            .value(0);
        (plan, count)
    }

    #[tokio::test]
    async fn rule_fires_on_unfiltered_count_star() {
        let fixture = make_fixture().await;
        let mut scanner = fixture.dataset.scan();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 40);
        assert!(
            plan_contains_pushdown(&plan),
            "expected CountFromMaskExec in plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
        assert!(
            !plan_contains_union(&plan),
            "no union expected for unfiltered count, got: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_fires_when_filter_fully_indexed() {
        let fixture = make_fixture().await;
        let mut scanner = fixture.dataset.scan();
        scanner.filter("ordered < 25").unwrap();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 25);
        assert!(
            plan_contains_pushdown(&plan),
            "expected CountFromMaskExec in plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
        assert!(
            !plan_contains_union(&plan),
            "no union expected when index covers every fragment, got: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_emits_split_plan_for_partial_index_coverage() {
        // Build index over 4 fragments, then append a 5th — the index now
        // covers a strict subset of the dataset. The rule must split into a
        // pushdown branch over the indexed fragments and a scan branch over
        // the rest, then sum the partials.
        use crate::dataset::WriteParams;
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();
        let extra = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_reader_rows(
                lance_datagen::RowCount::from(10),
                lance_datagen::BatchCount::from(1),
            );
        let dataset = Dataset::write(
            extra,
            tmp.as_str(),
            Some(WriteParams {
                mode: crate::dataset::WriteMode::Append,
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        scanner.filter("ordered < 100").unwrap();
        let (plan, count) = run_count(&mut scanner).await;
        // 5 fragments × 10 rows, all match `< 100`.
        assert_eq!(count, 50);
        assert!(
            plan_contains_pushdown(&plan),
            "expected pushdown branch in split plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
        assert!(
            plan_contains_union(&plan),
            "expected UnionExec for partial-coverage split, got: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_fires_with_stable_row_ids() {
        // Unfiltered count, stable row ids, with a deletion.
        use crate::dataset::WriteParams;
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset_with_params(
                tmp.as_str(),
                FragmentCount::from(2),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        dataset.delete("ordered = 0").await.unwrap();
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 19);
        assert!(
            plan_contains_pushdown(&plan),
            "rule should fire under stable row IDs, got plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_fires_with_stable_row_ids_and_filter() {
        // Indexed filter, stable row ids, deletions spread across fragments --
        // the case the pre-fix code got wrong (dropped rows in fragments > 0).
        use crate::dataset::WriteParams;
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset_with_params(
                tmp.as_str(),
                FragmentCount::from(3),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();
        // Delete one row from fragment 1 and one from fragment 2.
        dataset
            .delete("ordered = 15 OR ordered = 25")
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        // Matches every row across all three fragments; with the two deletions
        // the live count is 28.
        scanner.filter("ordered >= 0").unwrap();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 28);
        assert!(
            plan_contains_pushdown(&plan),
            "rule should fire under stable row IDs with a filter, got plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_skips_when_filter_needs_refine() {
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .col("unindexed", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        scanner.filter("unindexed > 5").unwrap();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 34);
        assert!(
            !plan_contains_pushdown(&plan),
            "rule should not fire with non-indexed filter, got plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_skips_when_index_is_inexact() {
        // Zonemap-style indices return AtMost (over-approximation) and set
        // ScalarIndexSearch.needs_recheck = true. CountFromMaskExec
        // ignores the discriminant on the prefilter batch, so firing the
        // rule against an inexact index would silently overcount. The rule
        // must refuse — and the scan path with its recheck still answers
        // correctly.
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::ZoneMap,
                None,
                &ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap),
                true,
            )
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        scanner.filter("ordered < 25").unwrap();
        let (plan, count) = run_count(&mut scanner).await;
        assert_eq!(count, 25);
        assert!(
            !plan_contains_pushdown(&plan),
            "rule must not fire when the index produces inexact (needs_recheck) results, \
             got plan: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    #[tokio::test]
    async fn rule_skips_count_with_group_by() {
        let fixture = make_fixture().await;
        let mut scanner = fixture.dataset.scan();
        scanner
            .aggregate(
                AggregateExpr::builder()
                    .group_by("ordered")
                    .count_star()
                    .build(),
            )
            .unwrap();
        let plan = scanner.create_plan().await.unwrap();
        assert!(
            !plan_contains_pushdown(&plan),
            "rule should not fire for GROUP BY: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }
}
