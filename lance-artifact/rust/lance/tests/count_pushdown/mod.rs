// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! End-to-end integration tests for [`CountPushdown`] when DataFusion
//! does the planning (i.e. an aggregate built from SQL through the public
//! [`LanceTableProvider`] surface), as opposed to going through
//! `Scanner::create_plan`.
//!
//! The plan shape DataFusion produces for a SQL aggregate differs from the
//! scanner's: it emits `AggregateExec(Final) → CoalescePartitionsExec →
//! AggregateExec(Partial) → LanceTableScan` rather than a single
//! `AggregateExec(Single)`. This file pins that down so future aggregate-
//! pushdown categories can be added here with the test scaffolding already
//! in place.

use std::sync::Arc;

use arrow::datatypes::UInt64Type;
use arrow_array::types::Int64Type;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance::Dataset;
use lance::datafusion::LanceTableProvider;
use lance::dataset::WriteParams;
use lance::index::DatasetIndexExt;
use lance::io::exec::count_from_mask::CountFromMaskExec;
use lance::io::exec::count_pushdown::CountPushdown;
use lance_core::utils::tempfile::TempStrDir;
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;

/// Build a 4-fragment, 10-row-per-fragment dataset with a BTREE index on `x`.
async fn make_indexed_dataset() -> (Arc<Dataset>, TempStrDir) {
    let tmp = TempStrDir::default();
    let reader = gen_batch()
        .col("x", array::step::<UInt64Type>())
        .into_reader_rows(RowCount::from(10), BatchCount::from(4));
    let mut dataset = Dataset::write(
        reader,
        tmp.as_str(),
        Some(WriteParams {
            max_rows_per_file: 10,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["x"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    (Arc::new(dataset), tmp)
}

/// Build a `SessionContext` configured with the Lance physical optimizer rule
/// for aggregate pushdown, then register `dataset` under the name `t`.
fn lance_aware_context(dataset: Arc<Dataset>) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(CountPushdown))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table(
        "t",
        Arc::new(LanceTableProvider::new(dataset, false, false)),
    )
    .unwrap();
    ctx
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

async fn execute_count(plan: Arc<dyn ExecutionPlan>) -> i64 {
    let stream = datafusion::physical_plan::execute_stream(
        plan,
        Arc::new(datafusion::execution::TaskContext::default()),
    )
    .unwrap();
    let batches: Vec<_> = stream.try_collect().await.unwrap();
    let total: i64 = batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>()
                .expect("count column should be Int64")
                .iter()
                .map(|v| v.unwrap_or(0))
                .sum::<i64>()
        })
        .sum();
    total
}

#[tokio::test]
async fn sql_count_star_with_indexed_filter() {
    // SELECT COUNT(*) FROM t WHERE x < 25
    //
    // The rule should fire on DataFusion's `AggregateExec(Partial)` node at
    // the leaf of the aggregate pipeline, replacing the column scan with
    // `CountFromMaskExec` while the outer `AggregateExec(Final)` keeps
    // doing the cross-partition combine.
    let (dataset, _tmp) = make_indexed_dataset().await;
    let ctx = lance_aware_context(dataset);

    let df = ctx
        .sql("SELECT COUNT(*) FROM t WHERE x < 25")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    assert!(
        plan_contains_pushdown(&plan),
        "expected CountFromMaskExec in SQL plan, got:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    assert_eq!(execute_count(plan).await, 25);
}

#[tokio::test]
async fn sql_unfiltered_count_star_uses_statistics() {
    // SELECT COUNT(*) FROM t with no filter is answered by DataFusion
    // statically from LanceTableProvider's row-count statistic — never
    // reaches an `AggregateExec` for our rule to look at. Pin that
    // behaviour: the rule should not fire, and the answer is correct.
    let (dataset, _tmp) = make_indexed_dataset().await;
    let ctx = lance_aware_context(dataset);

    let df = ctx.sql("SELECT COUNT(*) FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    assert!(
        !plan_contains_pushdown(&plan),
        "unfiltered COUNT(*) should be resolved from statistics, got:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    assert_eq!(execute_count(plan).await, 40);
}

#[tokio::test]
async fn sql_count_distinct_does_not_fire_yet() {
    // SELECT COUNT(DISTINCT x) FROM t WHERE x < 25
    //
    // `is_count_star` rejects distinct, so this rule never fires for
    // distinct counts — they belong to the mask-to-answer category and will
    // need their own rule (e.g. over a bitmap-index dictionary). This test
    // pins the not-firing behaviour and the scaffold for the future test.
    let (dataset, _tmp) = make_indexed_dataset().await;
    let ctx = lance_aware_context(dataset);

    let df = ctx
        .sql("SELECT COUNT(DISTINCT x) FROM t WHERE x < 25")
        .await
        .unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    assert!(
        !plan_contains_pushdown(&plan),
        "CountFromMaskExec must not fire for COUNT(DISTINCT) yet: \n{}",
        displayable(plan.as_ref()).indent(true)
    );
    // Correctness via the scan path: values 0..25 are all distinct.
    assert_eq!(execute_count(plan).await, 25);
}
