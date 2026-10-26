// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Resolve scalar projections of structs containing blobs in write mode.
//!
//! TableProvider::scan receives top-level column indexes, so `extra.note` and
//! `extra.clip` initially request the same struct. Keep a non-executable planning
//! node until DataFusion supplies the physical projection. Only a projection
//! independent of blobs can replace it. Its child always has the real descriptor
//! schema; no operator ever produces a partial struct or synthetic blob values.

use std::{fmt, sync::Arc};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{
    DataFusionError, Result,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::{
    EquivalenceProperties, Partitioning, PhysicalExpr, ScalarFunctionExpr,
    expressions::{Column, Literal},
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execution_plan::InvariantLevel, projection::ProjectionExec,
};

use super::blob_nulls::{has_descriptor, is_descriptor};

pub(super) fn scan_error() -> DataFusionError {
    DataFusionError::Plan(
        "Blob v2 scans require BlobSchema::Read; use a separate read adapter for blob fields"
            .into(),
    )
}

fn referenced_field<'a>(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &'a Schema,
) -> Option<(&'a Field, bool)> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some((schema.fields().get(column.index())?, false));
    }
    let call = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())?;
    let (base, path) = call.args().split_first()?;
    let (mut field, mut within_descriptor) = referenced_field(base, schema)?;
    for name in path {
        let name = name
            .downcast_ref::<Literal>()?
            .value()
            .try_as_str()
            .flatten()?;
        within_descriptor |= is_descriptor(field);
        let DataType::Struct(fields) = field.data_type() else {
            return None;
        };
        field = fields.find(name)?.1;
    }
    Some((field, within_descriptor))
}

fn references_descriptor(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<bool> {
    let mut found = false;
    expr.apply(|expr| {
        if let Some((field, within_descriptor)) = referenced_field(expr, schema) {
            if within_descriptor || has_descriptor(field) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

pub(super) fn write_scan(
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !input
        .schema()
        .fields()
        .iter()
        .any(|field| has_descriptor(field))
    {
        return Ok(input);
    }
    Ok(Arc::new(BlobWriteScanExec::new(input, schema)))
}

#[derive(Debug)]
struct BlobWriteScanExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl BlobWriteScanExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for BlobWriteScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobWriteScanExec")
    }
}

impl ExecutionPlan for BlobWriteScanExec {
    fn name(&self) -> &str {
        "BlobWriteScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return Err(DataFusionError::Internal(
                "BlobWriteScanExec requires one input".into(),
            ));
        };
        Ok(Arc::new(Self::new(input.clone(), self.schema())))
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        match check {
            InvariantLevel::Always => Ok(()),
            // DataFusion checks this after physical projection pushdown, before
            // executing any input (including an INSERT's SELECT).
            InvariantLevel::Executable => Err(scan_error()),
        }
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        // Also protect callers executing a scan without physical optimization.
        Err(scan_error())
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let schema = self.input.schema();
        for expression in projection.expr() {
            if references_descriptor(&expression.expr, &schema)? {
                return Ok(None);
            }
        }
        let projected = Arc::new(ProjectionExec::try_new(
            projection.expr().iter().cloned(),
            self.input.clone(),
        )?);
        if projected.schema() != projection.schema() {
            return Ok(None);
        }
        Ok(Some(projected))
    }
}
