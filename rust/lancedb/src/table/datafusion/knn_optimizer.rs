// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::{Result as DFResult, tree_node::Transformed};
use datafusion_expr::{
    Expr, LogicalPlan, Projection, UserDefinedLogicalNodeCore, logical_plan::Extension,
};

use super::knn::KnnNode;

/// Optimizer rule that sets [`KnnNode::project_vector`] to `false` when the
/// vector column is not referenced by a parent [`Projection`].
///
/// This avoids storing large embedding vectors in the `OwnedRow` heap during
/// [`PartialKnnExec`], saving significant memory on wide tables.
#[derive(Debug)]
pub struct KnnProjectVectorRule;

impl OptimizerRule for KnnProjectVectorRule {
    fn name(&self) -> &str {
        "knn_project_vector"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        // We only care about Projection(KnnNode) pairs.
        let LogicalPlan::Projection(proj) = plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(ext) = proj.input.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Projection(proj)));
        };
        let Some(knn) = ext.node.as_any().downcast_ref::<KnnNode>() else {
            return Ok(Transformed::no(LogicalPlan::Projection(proj)));
        };

        // Nothing to do if already opted out.
        if !knn.project_vector {
            return Ok(Transformed::no(LogicalPlan::Projection(proj)));
        }

        let vec_name = knn.column_field.name();
        let vector_projected = proj
            .expr
            .iter()
            .any(|e| expr_references_column(e, vec_name));
        if vector_projected {
            return Ok(Transformed::no(LogicalPlan::Projection(proj)));
        }

        // Rebuild KnnNode with project_vector = false by cloning via with_exprs_and_inputs
        // then applying the projection flag.
        let new_knn = knn
            .with_exprs_and_inputs(vec![], vec![knn.input.clone()])?
            .without_vector_projection();
        let new_ext = LogicalPlan::Extension(Extension {
            node: Arc::new(new_knn),
        });
        let new_proj = Projection::try_new(proj.expr.clone(), Arc::new(new_ext))?;
        Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
    }
}

/// Returns `true` if `expr` references a column with the given (unqualified) name.
fn expr_references_column(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(col) => col.name == name,
        Expr::Alias(alias) => expr_references_column(&alias.expr, name),
        // For any other expression type we conservatively return false; the
        // optimizer will simply leave project_vector=true (safe).
        _ => false,
    }
}
