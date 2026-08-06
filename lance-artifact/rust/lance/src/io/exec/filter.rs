// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use datafusion::{execution::TaskContext, logical_expr::Expr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    Statistics, filter::FilterExec, metrics::MetricsSet,
};
use lance_core::{Result, error::DataFusionResult};
use lance_datafusion::planner::Planner;

#[derive(Debug)]
// LanceFilterExec is a wrapper around FilterExec that includes the original
// expression for the filter node. In comparison to a FilterExec, this makes it
// possible for an optimization rule to serialize the filter to substrait and
// send it to a remote worker.
pub struct LanceFilterExec {
    expr: Expr,
    pub filter: Arc<FilterExec>,
}

impl DisplayAs for LanceFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.filter.fmt_as(t, f)
    }
}

impl LanceFilterExec {
    pub fn try_new(expr: Expr, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let planner = Planner::new(input.schema());
        let predicate = planner.create_physical_expr(&expr)?;
        let filter_exec = FilterExec::try_new(predicate.clone(), input)?;
        Ok(Self {
            expr,
            filter: Arc::new(filter_exec),
        })
    }

    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

impl ExecutionPlan for LanceFilterExec {
    fn name(&self) -> &str {
        "LanceFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.filter.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.filter.children()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.filter.maintains_input_order()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Rewrap the result in a LanceFilterExec to preserve the logical expression
        let new_filter_plan = self.filter.clone().with_new_children(children)?;
        let new_filter = new_filter_plan
            .downcast_ref::<FilterExec>()
            .expect("FilterExec::with_new_children should return FilterExec")
            .clone();
        Ok(Arc::new(Self {
            expr: self.expr.clone(),
            filter: Arc::new(new_filter),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.filter.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.filter.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        self.filter.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> datafusion_physical_plan::execution_plan::CardinalityEffect {
        self.filter.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &datafusion_physical_plan::projection::ProjectionExec,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        self.filter.try_swapping_with_projection(projection)
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}
