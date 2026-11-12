// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Preserve scan provenance until INSERT can materialize source-owned blobs.
//!
//! External descriptors remain references and must satisfy the destination's
//! existing external-base policy; source authorization grants no target trust.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result, tree_node::TreeNodeRecursion};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::{
    PhysicalExpr, ScalarFunctionExpr,
    expressions::{CastExpr, Column, Literal},
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    coalesce_partitions::CoalescePartitionsExec,
    execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions},
    filter::FilterExec,
    limit::{GlobalLimitExec, LocalLimitExec},
    projection::ProjectionExec,
    repartition::RepartitionExec,
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    stream::RecordBatchStreamAdapter,
};
use futures::{FutureExt, TryStreamExt, future::BoxFuture};
use lance::{Dataset, dataset::BlobV2BatchRewritePlan};
use lance_core::{
    ROW_ADDR,
    datatypes::{BlobV2Layout, Schema as LanceSchema},
};

use super::{BaseTable, MetadataEraserExec, blob_nulls};
use crate::{
    query::{QueryExecutionOptions, QueryRequest, Select},
    table::AnyQuery,
};

const HIDDEN_ROW_ADDR: &str = "__lancedb_blob_rowaddr";

// Expressions such as CASE can retain the descriptor layout while dropping
// extension metadata. They still require proven provenance before writing.
pub(super) fn has_descriptor(field: &Field) -> bool {
    contains_layout(field.data_type(), BlobV2Layout::Descriptor)
}

fn descriptor_for_target(input: &Field, target: &Field) -> bool {
    if crate::blob::is_blob(target) {
        return matches!(input.data_type(), DataType::Struct(fields)
            if BlobV2Layout::classify(fields) == Some(BlobV2Layout::Descriptor));
    }
    match (input.data_type(), target.data_type()) {
        (DataType::Struct(input), DataType::Struct(target)) => target.iter().any(|target| {
            input
                .find(target.name())
                .is_some_and(|(_, input)| descriptor_for_target(input, target))
        }),
        (
            DataType::List(input) | DataType::LargeList(input) | DataType::FixedSizeList(input, _),
            DataType::List(target)
            | DataType::LargeList(target)
            | DataType::FixedSizeList(target, _),
        ) => descriptor_for_target(input, target),
        _ => false,
    }
}

fn contains_layout(data_type: &DataType, layout: BlobV2Layout) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            BlobV2Layout::classify(fields) == Some(layout)
                || fields
                    .iter()
                    .any(|field| contains_layout(field.data_type(), layout))
        }
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            contains_layout(item.data_type(), layout)
        }
        _ => false,
    }
}

pub(super) fn has_source_scan(input: &Arc<dyn ExecutionPlan>) -> bool {
    input.downcast_ref::<BlobScanExec>().is_some()
        || input.children().iter().any(|child| has_source_scan(child))
}

#[expect(
    deprecated,
    reason = "DataFusion 55 still accepts CoalesceBatchesExec plans"
)]
fn preserves_row_identity(input: &Arc<dyn ExecutionPlan>) -> bool {
    input.downcast_ref::<ProjectionExec>().is_some()
        || input.downcast_ref::<FilterExec>().is_some()
        || input.downcast_ref::<SortExec>().is_some()
        || input.downcast_ref::<SortPreservingMergeExec>().is_some()
        || input.downcast_ref::<GlobalLimitExec>().is_some()
        || input.downcast_ref::<LocalLimitExec>().is_some()
        || input.downcast_ref::<CoalescePartitionsExec>().is_some()
        || input
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
            .is_some()
        || input.downcast_ref::<RepartitionExec>().is_some()
}

fn provenance_error(detail: impl fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!(
        "Blob INSERT SELECT requires descriptors from one source table snapshot with an \
         unambiguous row address; {detail}. JOIN, UNION, aggregation, and computed blob \
         descriptors are not supported"
    ))
}

/// A transparent scan boundary. It retains the snapshot and scan request, rather
/// than exposing a system column to SELECT * or fetching any payloads on reads.
#[derive(Debug)]
pub(super) struct BlobScanExec {
    input: Arc<dyn ExecutionPlan>,
    source: Arc<dyn BaseTable>,
    dataset: Arc<Dataset>,
    query: QueryRequest,
    options: QueryExecutionOptions,
}

impl BlobScanExec {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        source: Arc<dyn BaseTable>,
        dataset: Arc<Dataset>,
        query: QueryRequest,
        options: QueryExecutionOptions,
    ) -> Self {
        Self {
            input,
            source,
            dataset,
            query,
            options,
        }
    }

    async fn with_row_address(&self) -> Result<Provenance> {
        let mut query = self.query.clone();
        let Select::Columns(columns) = &mut query.select else {
            return Err(provenance_error(
                "source scan has no explicit column projection",
            ));
        };
        if !columns.iter().any(|name| name == ROW_ADDR) {
            columns.push(ROW_ADDR.to_owned());
        }
        let plan = self
            .source
            .create_plan(&AnyQuery::Query(query), self.options.clone())
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let plan = blob_nulls::mask_plan(plan)?;
        let plan: Arc<dyn ExecutionPlan> = Arc::new(MetadataEraserExec::new(plan));
        let schema = plan.schema();
        let mut projection: Vec<(Arc<dyn PhysicalExpr>, String)> = self
            .input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                Ok((
                    Arc::new(Column::new(field.name(), schema.index_of(field.name())?)) as _,
                    field.name().clone(),
                ))
            })
            .collect::<Result<_>>()?;
        projection.push((
            Arc::new(Column::new(ROW_ADDR, schema.index_of(ROW_ADDR)?)),
            HIDDEN_ROW_ADDR.into(),
        ));
        Ok(Provenance {
            plan: Arc::new(ProjectionExec::try_new(projection, plan)?),
            dataset: self.dataset.clone(),
            fields: self
                .input
                .schema()
                .fields()
                .iter()
                .map(|field| Some(field.name().clone()))
                .collect(),
        })
    }
}

impl DisplayAs for BlobScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlobScanExec: version={}",
            self.dataset.version().version
        )
    }
}

impl ExecutionPlan for BlobScanExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "BlobScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children
            .try_into()
            .map_err(|_| provenance_error("invalid scan children"))?;
        Ok(Arc::new(Self::new(
            input,
            self.source.clone(),
            self.dataset.clone(),
            self.query.clone(),
            self.options.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

struct Provenance {
    plan: Arc<dyn ExecutionPlan>,
    dataset: Arc<Dataset>,
    // Source field path for each output column, before the hidden address.
    fields: Vec<Option<String>>,
}

fn source_field(expr: &Arc<dyn PhysicalExpr>, fields: &[Option<String>]) -> Option<String> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return fields.get(column.index()).cloned().flatten();
    }
    let call = expr.downcast_ref::<ScalarFunctionExpr>()?;
    call.fun().inner().downcast_ref::<GetFieldFunc>()?;
    let (root, path) = call.args().split_first()?;
    let mut source = source_field(root, fields)?;
    for component in path {
        let name = component
            .downcast_ref::<Literal>()?
            .value()
            .try_as_str()
            .flatten()?;
        source.push('.');
        source.push_str(name);
    }
    Some(source)
}

fn with_provenance(input: Arc<dyn ExecutionPlan>) -> BoxFuture<'static, Result<Provenance>> {
    async move {
        if let Some(scan) = input.downcast_ref::<BlobScanExec>() {
            return scan.with_row_address().await;
        }
        let children = input.children();
        // An explicit allowlist keeps row identity from silently propagating
        // through a future operator that changes cardinality or column meaning.
        if children.len() != 1 || !preserves_row_identity(&input) {
            return Err(provenance_error(format!(
                "cannot establish provenance through {}",
                input.name()
            )));
        }
        let mut provenance = with_provenance(children[0].clone()).await?;
        if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
            let fields = projection
                .expr()
                .iter()
                .map(|expr| source_field(&expr.expr, &provenance.fields))
                .collect();
            let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
                .expr()
                .iter()
                .map(|expr| (expr.expr.clone(), expr.alias.clone()))
                .collect();
            expressions.push((
                Arc::new(Column::new(
                    HIDDEN_ROW_ADDR,
                    provenance.plan.schema().fields().len() - 1,
                )),
                HIDDEN_ROW_ADDR.into(),
            ));
            provenance.plan = Arc::new(ProjectionExec::try_new(expressions, provenance.plan)?);
            provenance.fields = fields;
        } else {
            if input.schema() != children[0].schema() {
                return Err(provenance_error(format!(
                    "{} changes the source schema",
                    input.name()
                )));
            }
            provenance.plan = input.replace_children(
                vec![provenance.plan],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
        }
        Ok(provenance)
    }
    .boxed()
}

/// Defer only a container coercion whose Blob leaves retain their named paths.
/// Descriptor leaves are atomic: casting their members is never provenance.
fn can_defer_blob_cast(source: &Field, cast_type: &DataType, target: &Field) -> bool {
    if crate::blob::is_blob(target) {
        let (DataType::Struct(source), DataType::Struct(cast)) = (source.data_type(), cast_type)
        else {
            return false;
        };
        return BlobV2Layout::classify(source) == Some(BlobV2Layout::Descriptor)
            && (BlobV2Layout::classify(cast) == Some(BlobV2Layout::Logical)
                || (BlobV2Layout::classify(cast) == Some(BlobV2Layout::Descriptor)
                    && source.len() == cast.len()
                    && source.iter().zip(cast).all(|(source, cast)| {
                        source.name() == cast.name() && source.data_type() == cast.data_type()
                    })));
    }
    match (source.data_type(), cast_type, target.data_type()) {
        (DataType::Struct(source), DataType::Struct(cast), DataType::Struct(target)) => {
            source.len() == cast.len()
                && source.len() == target.len()
                && target.iter().all(|target| {
                    source
                        .find(target.name())
                        .zip(cast.find(target.name()))
                        .is_some_and(|((_, source), (_, cast))| {
                            can_defer_blob_cast(source, cast.data_type(), target)
                        })
                })
        }
        (DataType::List(source), DataType::List(cast), DataType::List(target))
        | (DataType::LargeList(source), DataType::LargeList(cast), DataType::LargeList(target)) => {
            can_defer_blob_cast(source, cast.data_type(), target)
        }
        (
            DataType::FixedSizeList(source, source_len),
            DataType::FixedSizeList(cast, cast_len),
            DataType::FixedSizeList(target, target_len),
        ) => {
            source_len == cast_len
                && source_len == target_len
                && can_defer_blob_cast(source, cast.data_type(), target)
        }
        _ => !has_descriptor(source) && cast_type == target.data_type(),
    }
}

pub(super) async fn materialize(
    input: Arc<dyn ExecutionPlan>,
    target_schema: &Schema,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !target_schema
        .fields()
        .iter()
        .any(|field| crate::blob::field_tree_has_blob_v2(field))
    {
        return Ok(input);
    }
    // INSERT may cast containers for metadata, field order, scalar siblings,
    // or a Write adapter's logical layout. Bind the original Blob leaves and
    // materialize before applying the target coercion. Other expressions still
    // have to pass the Column/GetField provenance checks below.
    let input = if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        let mut changed = false;
        let expressions = projection
            .expr()
            .iter()
            .map(|projected| {
                let mut expr = projected.expr.clone();
                if let Some(cast) = expr.downcast_ref::<CastExpr>()
                    && let Ok(target) = target_schema.field_with_name(&projected.alias)
                    && crate::blob::field_tree_has_blob_v2(target)
                    && let source = cast
                        .expr()
                        .return_field(projection.input().schema().as_ref())?
                    && has_descriptor(&source)
                {
                    if !can_defer_blob_cast(
                        &source,
                        &cast.data_type(projection.input().schema().as_ref())?,
                        target,
                    ) {
                        return Err(provenance_error(format!(
                            "cast of column '{}' does not preserve Blob leaf identity",
                            projected.alias
                        )));
                    }
                    expr = cast.expr().clone();
                    changed = true;
                }
                Ok((expr, projected.alias.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        if changed {
            Arc::new(ProjectionExec::try_new(
                expressions,
                projection.input().clone(),
            )?) as Arc<dyn ExecutionPlan>
        } else {
            input
        }
    } else {
        input
    };
    let rewrite_columns: Vec<bool> = input
        .schema()
        .fields()
        .iter()
        .map(|field| {
            target_schema
                .field_with_name(field.name())
                .is_ok_and(|target| descriptor_for_target(field, target))
        })
        .collect();
    if !rewrite_columns.iter().any(|rewrite| *rewrite) {
        return Ok(input);
    }
    let provenance = with_provenance(input.clone()).await?;
    let mut source_schema = LanceSchema::default();
    let mut expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
    for (index, (field, source)) in input
        .schema()
        .fields()
        .iter()
        .zip(&provenance.fields)
        .enumerate()
    {
        let name = format!("__lancedb_blob_column_{index}");
        if rewrite_columns[index] {
            let source = source.as_ref().ok_or_else(|| {
                provenance_error(format!("column '{}' has no source", field.name()))
            })?;
            let mut source_field = provenance
                .dataset
                .schema()
                .field(source)
                .ok_or_else(|| provenance_error(format!("source field '{source}' is missing")))?
                .clone();
            if !crate::blob::field_tree_has_blob_v2(&Field::from(&source_field)) {
                return Err(provenance_error(format!(
                    "source field '{source}' is not a Blob v2 column"
                )));
            }
            source_field.name = name.clone();
            source_schema.fields.push(source_field);
        }
        expressions.push((Arc::new(Column::new(field.name(), index)), name));
    }
    expressions.push((
        Arc::new(Column::new(HIDDEN_ROW_ADDR, provenance.fields.len())),
        ROW_ADDR.into(),
    ));
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(expressions, provenance.plan)?);
    // preserve_managed=false forces even managed source payloads to become
    // target-owned bytes instead of retaining source storage references.
    let rewrite = Arc::new(BlobV2BatchRewritePlan::try_new(
        &source_schema,
        plan.schema().as_ref(),
        false,
        false,
    )?);
    let schema = Arc::new(Schema::new(
        rewrite
            .output_schema()
            .fields()
            .iter()
            .zip(input.schema().fields())
            .map(|(rewritten, original)| rewritten.as_ref().clone().with_name(original.name()))
            .collect::<Vec<_>>(),
    ));
    let plan = Arc::new(MaterializeBlobsExec::new(
        plan,
        provenance.dataset,
        rewrite,
        schema,
    ));
    super::cast::cast_to_table_schema(plan, target_schema)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

struct MaterializeBlobsExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: Arc<Dataset>,
    rewrite: Arc<BlobV2BatchRewritePlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for MaterializeBlobsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializeBlobsExec")
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

impl MaterializeBlobsExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        rewrite: Arc<BlobV2BatchRewritePlan>,
        schema: SchemaRef,
    ) -> Self {
        // Rewriting changes column types; descriptor equivalences/orderings do
        // not describe the resulting payload structs.
        let properties = Arc::new(PlanProperties::new(
            datafusion_physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion_physical_expr::Partitioning::UnknownPartitioning(
                input.output_partitioning().partition_count(),
            ),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self {
            input,
            dataset,
            rewrite,
            schema,
            properties,
        }
    }
}

impl DisplayAs for MaterializeBlobsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MaterializeBlobsExec")
    }
}

impl ExecutionPlan for MaterializeBlobsExec {
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn name(&self) -> &str {
        "MaterializeBlobsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [_; 1] = children
            .try_into()
            .map_err(|_| provenance_error("invalid rewrite children"))?;
        Ok(Arc::new(Self::new(
            input,
            self.dataset.clone(),
            self.rewrite.clone(),
            self.schema.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let dataset = self.dataset.clone();
        let rewrite = self.rewrite.clone();
        let schema = self.schema.clone();
        let stream = self
            .input
            .execute(partition, context)?
            .and_then(move |batch| {
                let dataset = dataset.clone();
                let rewrite = rewrite.clone();
                let schema = schema.clone();
                async move {
                    let batch = rewrite.transform_batch(&dataset, batch).await?;
                    Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
