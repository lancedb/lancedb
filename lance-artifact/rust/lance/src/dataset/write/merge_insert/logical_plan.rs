// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use datafusion::{
    common::DFSchema,
    execution::SessionState,
    physical_plan::ExecutionPlan,
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use lance_core::{ROW_ADDR, ROW_ID};
use std::{
    cmp::Ordering,
    sync::{Arc, atomic::AtomicU64},
};

use crate::Dataset;
use crate::dataset::write::merge_insert::exec::{
    DeleteOnlyMergeInsertExec, FullSchemaMergeInsertExec,
};
use crate::dataset::{WhenMatched, WhenNotMatchedBySource};

use super::{MERGE_ACTION_COLUMN, MERGE_SOURCE_SENTINEL, MergeInsertParams};

/// Logical plan node for merge insert write.
///
/// Expects input schema:
/// * `source.{col1, col2, ...}` - columns from the source relation
/// * `target.{col1, col2, ...}` - columns from the target relation
/// * `target._rowaddr` - special column to locate existing rows in the target
/// * `__action` - unqualified column that describes the action to perform.
///   See [`super::assign_action::merge_insert_action`]
///
/// Output is empty.
#[derive(Debug)]
pub struct MergeInsertWriteNode {
    input: LogicalPlan,
    pub(crate) dataset: Arc<Dataset>,
    pub(crate) params: MergeInsertParams,
    pub(crate) source_skipped_duplicates: Arc<AtomicU64>,
    schema: Arc<DFSchema>,
}

impl PartialEq for MergeInsertWriteNode {
    fn eq(&self, other: &Self) -> bool {
        self.params == other.params
            && self.input == other.input
            && self.dataset.base == other.dataset.base
    }
}

impl Eq for MergeInsertWriteNode {}

impl std::hash::Hash for MergeInsertWriteNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.params.hash(state);
        self.input.hash(state);
        self.dataset.base.hash(state);
    }
}

impl PartialOrd for MergeInsertWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.params.partial_cmp(&other.params) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            cmp => cmp,
        }
    }
}

impl MergeInsertWriteNode {
    pub fn new(
        input: LogicalPlan,
        dataset: Arc<Dataset>,
        params: MergeInsertParams,
        source_skipped_duplicates: Arc<AtomicU64>,
    ) -> Self {
        let empty_schema = Arc::new(arrow_schema::Schema::empty());
        let schema = Arc::new(DFSchema::try_from(empty_schema).unwrap());
        Self {
            input,
            dataset,
            params,
            source_skipped_duplicates,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for MergeInsertWriteNode {
    fn name(&self) -> &str {
        "MergeInsertWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let on_keys = self.params.on.join(", ");
        let when_matched = match &self.params.when_matched {
            crate::dataset::WhenMatched::DoNothing => "DoNothing",
            crate::dataset::WhenMatched::UpdateAll => "UpdateAll",
            crate::dataset::WhenMatched::UpdateIf(_) => "UpdateIf",
            crate::dataset::WhenMatched::UpdateIfExpr(_) => "UpdateIfExpr",
            crate::dataset::WhenMatched::Fail => "Fail",
            crate::dataset::WhenMatched::Delete => "Delete",
        };
        let when_not_matched = if self.params.insert_not_matched {
            "InsertAll"
        } else {
            "DoNothing"
        };
        let when_not_matched_by_source = match &self.params.delete_not_matched_by_source {
            crate::dataset::WhenNotMatchedBySource::Keep => "Keep",
            crate::dataset::WhenNotMatchedBySource::Delete => "Delete",
            crate::dataset::WhenNotMatchedBySource::DeleteIf(_) => "DeleteIf",
        };

        write!(
            f,
            "MergeInsertWrite: on=[{}], when_matched={}, when_not_matched={}, when_not_matched_by_source={}",
            on_keys, when_matched, when_not_matched, when_not_matched_by_source
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        if !exprs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "MergeInsertWriteNode does not accept expressions".to_string(),
            ));
        }
        if inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "MergeInsertWriteNode requires exactly one input".to_string(),
            ));
        }
        Ok(Self::new(
            inputs[0].clone(),
            self.dataset.clone(),
            self.params.clone(),
            self.source_skipped_duplicates.clone(),
        ))
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Going to need:
        // * all columns from the `source` relation (or just key columns for delete-only)
        // * `__action` column (unqualified)
        // * `target._rowaddr` column specifically

        let input_schema = self.input.schema();
        let mut necessary_columns = Vec::new();

        // Check if this is a delete-only operation (no writes needed)
        // In delete-only mode, we only need the key columns from source for matching
        let no_upsert = matches!(
            self.params.when_matched,
            crate::dataset::WhenMatched::Delete
        ) && !self.params.insert_not_matched;

        for (i, (qualifier, field)) in input_schema.iter().enumerate() {
            let should_include = match qualifier {
                // For delete-only: only include source KEY columns (for matching) plus the
                // sentinel column needed for action determination.
                // For other ops: include all source columns - they contain the new data to write.
                Some(qualifier) if qualifier.table() == "source" => {
                    if no_upsert {
                        self.params.on.iter().any(|k| k == field.name())
                            || field.name() == MERGE_SOURCE_SENTINEL
                    } else {
                        true
                    }
                }

                // Include target._rowaddr specifically - needed to locate existing rows for updates/deletes
                Some(qualifier) if qualifier.table() == "target" && field.name() == ROW_ADDR => {
                    true
                }

                // Include target._rowid specifically - needed to locate existing rows for updates
                Some(qualifier) if qualifier.table() == "target" && field.name() == ROW_ID => true,

                // Include unqualified columns like "__action" - tells us what operation to perform
                None if field.name() == MERGE_ACTION_COLUMN => true,

                // Partial-schema upsert: the `create_plan` builder adds
                // unqualified columns (named after dataset fields) for every
                // column missing from the source, filled from the target
                // side of the join. Those columns carry the values that
                // should be written for non-source columns, so they must
                // flow through to the write exec alongside `source.*`.
                None if self.dataset.schema().field(field.name()).is_some() => true,

                // Skip other target columns (target.value, target.key, target._rowid) - not needed for write
                _ => false,
            };

            if should_include {
                necessary_columns.push(i);
            }
        }

        Some(vec![necessary_columns])
    }
}

/// Physical planner for MergeInsertWriteNode.
pub struct MergeInsertPlanner {}

impl MergeInsertPlanner {
    /// Check if this is a delete-only operation that can use the optimized path.
    ///
    /// Delete-only operations are when:
    /// - `when_matched` is `Delete`
    /// - `insert_not_matched` is `false` (no inserts)
    /// - `delete_not_matched_by_source` is `Keep` (no additional deletes of unmatched target rows)
    fn is_delete_only(params: &MergeInsertParams) -> bool {
        matches!(params.when_matched, WhenMatched::Delete)
            && !params.insert_not_matched
            && matches!(
                params.delete_not_matched_by_source,
                WhenNotMatchedBySource::Keep
            )
    }
}

#[async_trait]
impl ExtensionPlanner for MergeInsertPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(write_node) = node.as_any().downcast_ref::<MergeInsertWriteNode>() {
                assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");

                let exec: Arc<dyn ExecutionPlan> = if Self::is_delete_only(&write_node.params) {
                    Arc::new(DeleteOnlyMergeInsertExec::try_new(
                        physical_inputs[0].clone(),
                        write_node.dataset.clone(),
                        write_node.params.clone(),
                        write_node.source_skipped_duplicates.clone(),
                    )?)
                } else {
                    Arc::new(FullSchemaMergeInsertExec::try_new(
                        physical_inputs[0].clone(),
                        write_node.dataset.clone(),
                        write_node.params.clone(),
                        write_node.source_skipped_duplicates.clone(),
                    )?)
                };
                Some(exec)
            } else {
                None
            },
        )
    }
}
