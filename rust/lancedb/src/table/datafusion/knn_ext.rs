// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::FixedSizeListArray;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_common::Result as DFResult;
use datafusion_expr::{LogicalPlan, logical_plan::Extension};

use super::knn::{KnnNode, KnnPlanner};
use super::knn_optimizer::KnnProjectVectorRule;
use crate::DistanceType;

/// Extends DataFusion's [`DataFrame`] with a batch KNN method.
pub trait DataFrameKnnExt {
    /// Attach a brute-force KNN step to this DataFrame.
    ///
    /// For each row in `query_vectors`, the `k` nearest rows (by
    /// `distance_type`) are returned from the `column` column.
    ///
    /// The returned DataFrame has the same columns as the input, plus:
    /// - `query_index: Int32` — 0-based index into `query_vectors`
    /// - `_distance: Float32` — distance from that query to this row
    ///
    /// Results are sorted by `(query_index ASC, _distance ASC)` and contain
    /// at most `k` rows per distinct `query_index`.
    ///
    /// The returned DataFrame can be further chained with any DataFusion
    /// DataFrame operations (`.filter()`, `.sort()`, `.collect()`, etc.),
    /// provided the `SessionContext` was built with [`register_knn_planner`].
    fn knn(
        self,
        column: impl Into<String>,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
    ) -> DFResult<DataFrame>;
}

impl DataFrameKnnExt for DataFrame {
    fn knn(
        self,
        column: impl Into<String>,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
    ) -> DFResult<DataFrame> {
        let column = column.into();
        let (session_state, logical_plan) = self.into_parts();
        let node = Arc::new(KnnNode::try_new(
            logical_plan,
            &column,
            query_vectors,
            k,
            distance_type,
        )?);
        let ext_plan = LogicalPlan::Extension(Extension { node });
        Ok(Self::new(session_state, ext_plan))
    }
}

#[derive(Debug)]
struct KnnQueryPlanner;

#[async_trait]
impl QueryPlanner for KnnQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(KnnPlanner)])
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Register the KNN extension planner with a [`SessionState`] so that
/// [`KnnNode`] logical plan nodes can be converted to physical plans.
///
/// # Example
///
/// ```ignore
/// use datafusion::prelude::SessionContext;
/// use lancedb::table::datafusion::knn_ext::register_knn_planner;
///
/// let state = SessionContext::new().state();
/// let state = register_knn_planner(state);
/// let ctx = SessionContext::new_with_state(state);
/// ```
pub fn register_knn_planner(state: SessionState) -> SessionState {
    SessionStateBuilder::new_from_existing(state)
        .with_query_planner(Arc::new(KnnQueryPlanner))
        .with_optimizer_rule(Arc::new(KnnProjectVectorRule))
        .build()
}
