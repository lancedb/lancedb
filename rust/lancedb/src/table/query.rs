// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

mod lsm;

use super::NativeTable;
use crate::connection::NamespaceClientPushdownOperation;
use crate::error::{Error, Result};
use crate::expr::expr_to_sql_string;
use crate::query::{
    DEFAULT_TOP_K, QueryExecutionOptions, QueryFilter, QueryRequest, Select, VectorQueryRequest,
};
use crate::utils::{MaxBatchLengthStream, TimeoutStream, default_vector_column};
use arrow::array::{AsArray, FixedSizeListBuilder, Float32Builder};
use arrow::datatypes::{Float32Type, UInt8Type};
use arrow_array::Array;
use arrow_schema::{DataType, Schema};
use datafusion_common::{Column, DataFusionError, ScalarValue, SchemaError};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn, Literal};
use datafusion_physical_plan::PhysicalExpr;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{ExecutionPlan, with_new_children_if_necessary};
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::scanner::Scanner;
use lance::index::DatasetIndexInternalExt;
use lance::io::exec::ANNIvfSubIndexExec;
use lance_datafusion::exec::{analyze_plan as lance_analyze_plan, execute_plan};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::{DIST_COL, quantizer::QuantizationType};
use lance_linalg::distance::DistanceType as LanceDistanceType;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    QueryTableRequest as NsQueryTableRequest, QueryTableRequestColumns,
    QueryTableRequestFullTextQuery, QueryTableRequestVector, StringFtsQuery,
};

#[derive(Debug, Clone)]
pub enum AnyQuery {
    Query(QueryRequest),
    VectorQuery(VectorQueryRequest),
}

impl AnyQuery {
    pub(crate) fn base(&self) -> &QueryRequest {
        match self {
            Self::Query(query) => query,
            Self::VectorQuery(query) => &query.base,
        }
    }

    fn base_mut(&mut self) -> &mut QueryRequest {
        match self {
            Self::Query(query) => query,
            Self::VectorQuery(query) => &mut query.base,
        }
    }

    /// Canonicalize any raw SQL filter immediately before backend dispatch.
    pub(crate) fn canonicalized(&self) -> Result<Self> {
        let mut query = self.clone();
        if let Some(QueryFilter::Sql(predicate)) = &mut query.base_mut().filter {
            *predicate = crate::expr::canonicalize_sql_predicate(predicate)?;
        }
        Ok(query)
    }
}

//Decide between namespace or local
pub async fn execute_query(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    let query = query.canonicalized()?;
    // QueryTable pushdown runs the query server-side, but only on the main
    // branch: the namespace request carries no branch yet, so a branch handle
    // must fall through to local execution.
    if can_execute_namespace_query(table, &query).await?
        && let Some(ref namespace_client) = table.namespace_client
    {
        return execute_namespace_query(table, namespace_client.clone(), &query, options).await;
    }
    execute_generic_query(table, &query, options).await
}

async fn can_execute_namespace_query(table: &NativeTable, query: &AnyQuery) -> Result<bool> {
    if !(table
        .pushdown_operations
        .contains(&NamespaceClientPushdownOperation::QueryTable)
        && table.namespace_client.is_some()
        && table.dataset.current_branch().is_none()
        // NsQueryTableRequest has no version field, so a pushed-down query would
        // read latest and ignore the pin.
        && table.dataset.time_travel_version().is_none()
        && !requires_local_namespace_execution(query))
    {
        return Ok(false);
    }
    // A MemWAL write spec means reads auto-route through the LSM scanner in
    // `create_plan` even when `use_lsm` is unset. The namespace request has no
    // use_lsm field, so pushing the default query down would silently omit
    // un-compacted rows — force local execution whenever a spec is installed.
    let dataset = table.dataset.get().await?;
    if dataset.mem_wal_index_details().await?.is_some() {
        return Ok(false);
    }
    Ok(true)
}

fn requires_local_namespace_execution(query: &AnyQuery) -> bool {
    // The namespace QueryTable request has no approx_mode or use_lsm field yet, so
    // pushing these down would silently ignore the user's setting. For use_lsm that
    // is worse than a tuning miss: MemWAL read routing lives only in `create_plan`,
    // so a pushed-down query would return stale base-only data with no error.
    if query.base().use_lsm.is_some() || query.base().take_offsets.is_some() {
        return true;
    }
    matches!(
        query,
        AnyQuery::VectorQuery(VectorQueryRequest {
            approx_mode: Some(_),
            ..
        })
    )
}

pub async fn analyze_query_plan(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<String> {
    let plan = create_plan(table, query, options).await?;
    Ok(lance_analyze_plan(plan, Default::default()).await?)
}

/// Local Execution Path (DataFusion)
async fn execute_generic_query(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    let plan = create_plan(table, query, options.clone()).await?;
    let inner = execute_plan(plan, Default::default())?;
    let inner = MaxBatchLengthStream::new_boxed(inner, options.max_batch_length as usize);
    let inner = if let Some(timeout) = options.timeout {
        TimeoutStream::new_boxed(inner, timeout)
    } else {
        inner
    };
    Ok(DatasetRecordBatchStream::new(inner))
}

pub async fn create_plan(
    table: &NativeTable,
    query: &AnyQuery,
    options: QueryExecutionOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let query = query.canonicalized()?;
    if let AnyQuery::Query(request) = &query
        && let Some(offsets) = &request.take_offsets
    {
        return crate::query::create_take_offsets_plan(table, request, offsets, options, false)
            .await;
    }

    let query = match query {
        AnyQuery::VectorQuery(query) => query,
        AnyQuery::Query(query) => VectorQueryRequest::from_plain_query(query),
    };
    query.base.check_filter()?;

    let ds_ref = table.dataset.get().await?;

    // MemWAL read routing driven by `use_lsm`:
    //   * unset  — route through the LSM scanner iff the table carries a write spec
    //   * Some(true)  — force LSM routing; error if the table has no write spec
    //   * Some(false) — read the base table only, bypassing the MemWAL
    // The LSM scanner surfaces in-flight `merge_insert` data (active/frozen
    // memtables + flushed generations); validation and dispatch live in `lsm`.
    let has_spec = ds_ref.mem_wal_index_details().await?.is_some();
    let use_lsm = match query.base.use_lsm {
        Some(true) if !has_spec => {
            return Err(Error::InvalidInput {
                message: "use_lsm(true) was set but the table has no MemWAL write spec; \
                    install one with set_lsm_write_spec or leave use_lsm unset"
                    .to_string(),
            });
        }
        Some(enable) => enable,
        None => has_spec,
    };
    if use_lsm {
        return lsm::create_lsm_plan(table, ds_ref, query).await;
    }

    let schema = ds_ref.schema();
    let mut column = query.column.clone();

    let mut query_vector = query.query_vector.first().cloned();
    let mut is_batch_query = false;
    if query.query_vector.len() > 1 {
        if column.is_none() {
            // Infer a vector column with the same dimension of the query vector.
            let arrow_schema = Schema::from(schema);
            column = Some(default_vector_column(
                &arrow_schema,
                Some(query.query_vector[0].len() as i32),
            )?);
        }
        let vector_field = schema.field(column.as_ref().unwrap()).unwrap();
        let (_, element_type) =
            lance::index::vector::utils::get_vector_type(schema, column.as_ref().unwrap())?;
        let is_binary = matches!(element_type, DataType::UInt8);
        if matches!(vector_field.data_type(), DataType::List(_))
            || (query.base.offset.unwrap_or(0) == 0 && !is_binary)
        {
            // Lance distinguishes these cases from the vector column type: a
            // list-like query against a List column is one multivector query,
            // while the same query against a FixedSizeList column is a batch of
            // independent queries. The batch path shares a single flat scan and
            // bounds retained candidate data instead of running one scan per
            // query vector.
            let vectors = query
                .query_vector
                .iter()
                .map(|arr| arr.as_ref())
                .collect::<Vec<_>>();
            let dim = vectors[0].len();
            if let Some((query_index, actual_dim)) = vectors
                .iter()
                .enumerate()
                .find_map(|(index, vector)| (vector.len() != dim).then_some((index, vector.len())))
            {
                return Err(Error::InvalidInput {
                    message: format!(
                        "query vector at index {query_index} has dimension {actual_dim}, expected {dim}"
                    ),
                });
            }
            let mut fsl_builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(dim * vectors.len()),
                dim as i32,
                vectors.len(),
            );
            for vec in vectors {
                fsl_builder
                    .values()
                    .append_slice(vec.as_primitive::<Float32Type>().values());
                fsl_builder.append(true);
            }
            query_vector = Some(Arc::new(fsl_builder.finish()));
            is_batch_query = !matches!(vector_field.data_type(), DataType::List(_));
        } else {
            // Lance's batch path has no per-query offset, and its binary path
            // requires primitive UInt8 queries rather than a fixed-size list.
            // Keep the prior plan shape for these cases so offsets are applied
            // per query and binary query vectors retain their primitive shape.
            let query_vecs = query.query_vector.clone();
            let plan_futures = query_vecs
                .into_iter()
                .map(|query_vector| {
                    let mut sub_query = query.clone();
                    sub_query.query_vector = vec![query_vector];
                    let options_ref = options.clone();
                    async move {
                        create_plan(table, &AnyQuery::VectorQuery(sub_query), options_ref).await
                    }
                })
                .collect::<Vec<_>>();
            let plans = futures::future::try_join_all(plan_futures).await?;
            return create_multi_vector_plan(plans);
        }
    }

    let mut scanner: Scanner = ds_ref.scan();

    if let Some(query_vector) = query_vector {
        let column = if let Some(col) = column {
            col
        } else {
            let arrow_schema = Schema::from(schema);
            default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
        };

        let (_, element_type) = lance::index::vector::utils::get_vector_type(schema, &column)?;
        let is_binary = matches!(element_type, DataType::UInt8);
        let top_k = query.base.limit.unwrap_or(DEFAULT_TOP_K) + query.base.offset.unwrap_or(0);

        if is_binary {
            let query_vector = arrow::compute::cast(&query_vector, &DataType::UInt8)?;
            let query_vector = query_vector.as_primitive::<UInt8Type>();
            scanner.nearest(&column, query_vector, top_k)?;
        } else {
            scanner.nearest(&column, query_vector.as_ref(), top_k)?;
        }

        if let Some(approx_mode) = query.approx_mode {
            scanner.approx_mode(approx_mode.into());
        }

        scanner.minimum_nprobes(query.minimum_nprobes);
        if let Some(maximum_nprobes) = query.maximum_nprobes {
            scanner.maximum_nprobes(maximum_nprobes);
        }
    }

    // For a batch query, `nearest` already applies k to each query vector.
    // Adding Scanner's global limit would truncate the combined result to k rows.
    if !is_batch_query {
        scanner.limit(
            query.base.limit.map(|limit| limit as i64),
            query.base.offset.map(|offset| offset as i64),
        )?;
    }

    if let Some(ef) = query.ef {
        scanner.ef(ef);
    }

    scanner.distance_range(query.lower_bound, query.upper_bound);
    scanner.use_index(query.use_index);
    scanner.prefilter(query.base.prefilter);

    match query.base.select {
        Select::Columns(ref columns) => {
            scanner.project(columns.as_slice())?;
        }
        Select::Dynamic(ref select_with_transform) => {
            scanner.project_with_transform(select_with_transform.as_slice())?;
        }
        Select::Expr(ref expr_pairs) => {
            let sql_pairs: crate::Result<Vec<(String, String)>> = expr_pairs
                .iter()
                .map(|(name, expr)| expr_to_sql_string(expr).map(|sql| (name.clone(), sql)))
                .collect();
            scanner.project_with_transform(sql_pairs?.as_slice())?;
        }
        Select::All => {}
    }

    if query.base.with_row_id {
        scanner.with_row_id();
    }

    if options.max_batch_length > 0 {
        scanner.batch_size(options.max_batch_length as usize);
    }

    if query.base.fast_search {
        scanner.fast_search();
    }

    if let Some(filter) = &query.base.filter {
        match filter {
            QueryFilter::Sql(sql) => {
                scanner.filter(sql)?;
            }
            QueryFilter::Substrait(substrait) => {
                scanner.filter_substrait(substrait)?;
            }
            QueryFilter::Datafusion(expr) => {
                scanner.filter_expr(expr.clone());
            }
        }
    }

    if let Some(fts) = &query.base.full_text_search {
        scanner.full_text_search(fts.clone())?;
    }

    if let Some(refine_factor) = query.refine_factor {
        scanner.refine(refine_factor);
    }

    if let Some(distance_type) = query.distance_type {
        scanner.distance_metric(distance_type.into());
    }

    if query.base.disable_scoring_autoprojection {
        scanner.disable_scoring_autoprojection();
    }

    if let Some(order_by) = &query.base.order_by {
        scanner.order_by(Some(order_by.clone()))?;
    }

    let mut plan = scanner
        .create_plan()
        .await
        .map_err(|error| enrich_lance_field_not_found(error, schema))?;
    let normalized_l2_indices = normalized_l2_ann_indices(plan.as_ref()).await?;
    if !normalized_l2_indices.is_empty() {
        // Rebuild only the affected ANN nodes with internal normalized squared-L2
        // bounds. Exact branches keep the public cosine bounds from `plan`.
        let internal_plan = if query.lower_bound.is_some() || query.upper_bound.is_some() {
            scanner.distance_range(
                query.lower_bound.map(|bound| bound / COSINE_ANN_SCALE),
                query.upper_bound.map(|bound| bound / COSINE_ANN_SCALE),
            );
            scanner
                .create_plan()
                .await
                .map_err(|error| enrich_lance_field_not_found(error, schema))?
        } else {
            plan.clone()
        };
        plan = normalize_ann_branches(plan, internal_plan, &normalized_l2_indices)?;
    }

    Ok(plan)
}

/// Replace DataFusion's top-level field candidates with qualified leaf paths.
///
/// DataFusion resolves nested fields but its `FieldNotFound` error only lists the
/// top-level Arrow fields. This makes a missing leaf look unavailable even when it
/// exists below a struct. Keep every other Lance/DataFusion error unchanged and
/// enrich only this one schema error at the LanceDB query boundary.
fn enrich_lance_field_not_found(
    error: lance::Error,
    schema: &lance_core::datatypes::Schema,
) -> Error {
    let Some(field) = find_missing_field(&error) else {
        return error.into();
    };
    field_not_found_error(field, &Schema::from(schema))
}

fn field_not_found_diagnostic(
    error: &(dyn std::error::Error + 'static),
    schema: &Schema,
) -> Option<Error> {
    let field = find_missing_field(error)?;
    Some(field_not_found_error(field, schema))
}

fn field_not_found_error(field: &Column, schema: &Schema) -> Error {
    let valid_fields = leaf_field_paths(schema);
    let mut message = format!("Schema error: No field named {}", field.quoted_flat_name());
    if !valid_fields.is_empty() {
        message.push_str(". Valid fields are ");
        message.push_str(&valid_fields.join(", "));
    }
    message.push('.');

    Error::InvalidInput { message }
}

fn find_missing_field<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Column> {
    if let Some(DataFusionError::SchemaError(schema_error, _)) =
        error.downcast_ref::<DataFusionError>()
        && let SchemaError::FieldNotFound { field, .. } = schema_error.as_ref()
    {
        return Some(field);
    }

    error.source().and_then(find_missing_field)
}

fn leaf_field_paths(schema: &Schema) -> Vec<String> {
    fn format_segment(segment: &str) -> String {
        // Quote every segment instead of maintaining a SQL keyword list. Bare
        // lowercase names such as `true` can be parsed as expressions rather
        // than identifiers, while backticks preserve all field names in both
        // local SQL parsers.
        format!("`{}`", segment.replace('`', "``"))
    }

    fn visit(fields: &arrow_schema::Fields, path: &mut Vec<String>, paths: &mut Vec<String>) {
        for field in fields {
            // Neither local planner can address an empty field-path segment,
            // even when it is backtick-quoted. Do not advertise leaves beneath
            // such a segment as valid filter fields.
            if field.name().is_empty() {
                continue;
            }
            path.push(field.name().clone());
            match field.data_type() {
                DataType::Struct(children) if !children.is_empty() => {
                    visit(children, path, paths);
                }
                _ => {
                    paths.push(
                        path.iter()
                            .map(|segment| format_segment(segment))
                            .collect::<Vec<_>>()
                            .join("."),
                    );
                }
            }
            path.pop();
        }
    }

    let mut paths = Vec::new();
    visit(schema.fields(), &mut Vec::new(), &mut paths);
    paths
}

//Helper functions below

const COSINE_ANN_SCALE: f32 = 0.5;

/// Find ANN index segments whose scores use normalized squared L2 for cosine search.
///
/// Cosine PQ/SQ/RQ indices normalize their vectors and use squared L2 internally.  This
/// preserves ranking, but squared L2 over unit vectors is twice the cosine distance.  Flat
/// cosine indices calculate cosine directly, so they are not included.
async fn normalized_l2_ann_indices(plan: &dyn ExecutionPlan) -> Result<HashSet<String>> {
    let mut ann_plans = Vec::new();
    find_ann_plans(plan, &mut ann_plans);

    let mut checked = HashSet::new();
    let mut normalized_l2 = HashSet::new();
    for ann in ann_plans {
        if ann.query().metric_type != Some(LanceDistanceType::Cosine) {
            continue;
        }
        for index in ann.indices() {
            let uuid = index.uuid.to_string();
            if !checked.insert(uuid.clone()) {
                continue;
            }
            let vector_index = ann
                .dataset()
                .open_vector_index(&ann.query().column, &index.uuid, &NoOpMetricsCollector)
                .await?;
            let (_, quantization_type) = vector_index.sub_index_type();
            if matches!(
                quantization_type,
                QuantizationType::Product | QuantizationType::Scalar | QuantizationType::Rabit
            ) {
                normalized_l2.insert(uuid);
            }
        }
    }
    Ok(normalized_l2)
}

/// Normalize affected ANN outputs before their parent plan nodes consume them.
///
/// This is used by planners that do not support distance ranges, such as the MemWAL
/// LSM planner. The standard scanner path rebuilds a second plan when it also needs
/// to translate range bounds, then calls [`normalize_ann_branches`] directly.
pub(super) async fn normalize_cosine_ann_branches(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let normalized_l2_indices = normalized_l2_ann_indices(plan.as_ref()).await?;
    if normalized_l2_indices.is_empty() {
        return Ok(plan);
    }
    normalize_ann_branches(plan.clone(), plan, &normalized_l2_indices)
}

fn find_ann_plans<'a>(plan: &'a dyn ExecutionPlan, ann_plans: &mut Vec<&'a ANNIvfSubIndexExec>) {
    if let Some(ann) = plan.downcast_ref::<ANNIvfSubIndexExec>() {
        ann_plans.push(ann);
    }
    for child in plan.children() {
        find_ann_plans(child.as_ref(), ann_plans);
    }
}

fn collect_ann_plans(
    plan: &Arc<dyn ExecutionPlan>,
    ann_plans: &mut VecDeque<Arc<dyn ExecutionPlan>>,
) {
    if plan.downcast_ref::<ANNIvfSubIndexExec>().is_some() {
        ann_plans.push_back(plan.clone());
        return;
    }
    for child in plan.children() {
        collect_ann_plans(child, ann_plans);
    }
}

/// Replace normalized-L2 ANN nodes with equivalent nodes that use internal bounds, then
/// convert their output to the public cosine scale before any generic plan node consumes it.
fn normalize_ann_branches(
    public_plan: Arc<dyn ExecutionPlan>,
    internal_plan: Arc<dyn ExecutionPlan>,
    normalized_l2_indices: &HashSet<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut internal_ann_plans = VecDeque::new();
    collect_ann_plans(&internal_plan, &mut internal_ann_plans);
    let normalized =
        replace_ann_branches(public_plan, &mut internal_ann_plans, normalized_l2_indices)?;
    if !internal_ann_plans.is_empty() {
        return Err(Error::Runtime {
            message: "internal and public vector plans contained different ANN branches"
                .to_string(),
        });
    }
    Ok(normalized)
}

fn replace_ann_branches(
    public_plan: Arc<dyn ExecutionPlan>,
    internal_ann_plans: &mut VecDeque<Arc<dyn ExecutionPlan>>,
    normalized_l2_indices: &HashSet<String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(public_ann) = public_plan.downcast_ref::<ANNIvfSubIndexExec>() {
        let internal_plan = internal_ann_plans
            .pop_front()
            .ok_or_else(|| Error::Runtime {
                message: "internal vector plan was missing an ANN branch".to_string(),
            })?;
        let internal_ann = internal_plan
            .downcast_ref::<ANNIvfSubIndexExec>()
            .expect("collected only ANN plans");
        let same_indices = public_ann
            .indices()
            .iter()
            .map(|index| &index.uuid)
            .eq(internal_ann.indices().iter().map(|index| &index.uuid));
        if public_ann.query().column != internal_ann.query().column
            || public_ann.query().metric_type != internal_ann.query().metric_type
            || !same_indices
        {
            return Err(Error::Runtime {
                message: "internal and public vector plans had mismatched ANN branches".to_string(),
            });
        }

        let normalized_count = public_ann
            .indices()
            .iter()
            .filter(|index| normalized_l2_indices.contains(&index.uuid.to_string()))
            .count();
        if normalized_count == 0 {
            return Ok(public_plan);
        }
        if normalized_count != public_ann.indices().len() {
            return Err(Error::Runtime {
                message: "one ANN branch mixed public and normalized-L2 distance scales"
                    .to_string(),
            });
        }
        return scale_distance_column(internal_plan, COSINE_ANN_SCALE);
    }

    let children = public_plan
        .children()
        .into_iter()
        .cloned()
        .map(|child| replace_ann_branches(child, internal_ann_plans, normalized_l2_indices))
        .collect::<Result<Vec<_>>>()?;
    Ok(with_new_children_if_necessary(public_plan, children)?)
}

fn scale_distance_column(
    plan: Arc<dyn ExecutionPlan>,
    scale: f32,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    if schema.column_with_name(DIST_COL).is_none() {
        return Ok(plan);
    }

    let expressions: Vec<(Arc<dyn PhysicalExpr>, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let column: Arc<dyn PhysicalExpr> = Arc::new(PhysicalColumn::new(field.name(), index));
            let expression = if field.name() == DIST_COL {
                let scale: Arc<dyn PhysicalExpr> =
                    Arc::new(Literal::new(ScalarValue::Float32(Some(scale))));
                Arc::new(BinaryExpr::new(column, Operator::Multiply, scale))
                    as Arc<dyn PhysicalExpr>
            } else {
                column
            };
            (expression, field.name().clone())
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(expressions, plan)?))
}

// Take many execution plans and map them into a single plan that adds
// a query_index column and unions them.
pub(crate) fn create_multi_vector_plan(
    plans: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plans.is_empty() {
        return Err(Error::InvalidInput {
            message: "No plans provided".to_string(),
        });
    }
    // Projection to keeping all existing columns
    let first_plan = plans[0].clone();
    let project_all_columns = first_plan
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let expr = datafusion_physical_plan::expressions::Column::new(field.name().as_str(), i);
            let expr = Arc::new(expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
            (expr, field.name().clone())
        })
        .collect::<Vec<_>>();

    let projected_plans = plans
        .into_iter()
        .enumerate()
        .map(|(plan_i, plan)| {
            let query_index = datafusion_common::ScalarValue::Int32(Some(plan_i as i32));
            let query_index_expr = datafusion_physical_plan::expressions::Literal::new(query_index);
            let query_index_expr =
                Arc::new(query_index_expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
            let mut projections = vec![(query_index_expr, "query_index".to_string())];
            projections.extend_from_slice(&project_all_columns);
            let projection = ProjectionExec::try_new(projections, plan).unwrap();
            Arc::new(projection) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        })
        .collect::<Vec<_>>();

    let unioned = UnionExec::try_new(projected_plans).map_err(|err| Error::Runtime {
        message: err.to_string(),
    })?;
    // We require 1 partition in the final output
    let repartitioned = RepartitionExec::try_new(
        unioned,
        datafusion_physical_plan::Partitioning::RoundRobinBatch(1),
    )
    .unwrap();
    Ok(Arc::new(repartitioned))
}

/// Execute a query on the namespace server instead of locally.
async fn execute_namespace_query(
    table: &NativeTable,
    namespace_client: Arc<dyn LanceNamespace>,
    query: &AnyQuery,
    _options: QueryExecutionOptions,
) -> Result<DatasetRecordBatchStream> {
    // Build table_id from namespace + table name
    let mut table_id = table.namespace.clone();
    table_id.push(table.name.clone());

    // Convert AnyQuery to namespace QueryTableRequest
    let mut ns_request = convert_to_namespace_query(query)?;
    // Set the table ID on the request
    ns_request.id = Some(table_id);

    // Call the namespace query_table API
    let response_bytes = namespace_client
        .query_table(ns_request)
        .await
        .map_err(|e| Error::Runtime {
            message: format!("Failed to execute server-side query: {}", e),
        })?;

    // Parse the Arrow IPC response into a RecordBatchStream
    parse_arrow_ipc_response(response_bytes).await
}

/// Convert an AnyQuery to the namespace QueryTableRequest format.
fn convert_to_namespace_query(query: &AnyQuery) -> Result<NsQueryTableRequest> {
    query.base().check_filter()?;
    match query {
        AnyQuery::VectorQuery(vq) => {
            // Extract the query vector(s)
            let vector = extract_query_vector(&vq.query_vector)?;

            // Convert filter to SQL string
            let filter = match &vq.base.filter {
                Some(f) => Some(filter_to_sql(f)?),
                None => None,
            };

            // Convert select to columns list
            let columns = match &vq.base.select {
                Select::All => None,
                Select::Columns(cols) => Some(Box::new(QueryTableRequestColumns {
                    column_names: Some(cols.clone()),
                    column_aliases: None,
                })),
                Select::Dynamic(_) => {
                    return Err(Error::NotSupported {
                        message:
                            "Dynamic column selection is not supported for server-side queries"
                                .to_string(),
                    });
                }
                Select::Expr(pairs) => {
                    let sql_pairs: crate::Result<Vec<(String, String)>> = pairs
                        .iter()
                        .map(|(name, expr)| expr_to_sql_string(expr).map(|sql| (name.clone(), sql)))
                        .collect();
                    let sql_pairs = sql_pairs?;
                    Some(Box::new(QueryTableRequestColumns {
                        column_names: None,
                        column_aliases: Some(sql_pairs.into_iter().collect()),
                    }))
                }
            };

            // Check for unsupported features
            if vq.base.reranker.is_some() {
                return Err(Error::NotSupported {
                    message: "Reranker is not supported for server-side queries".to_string(),
                });
            }

            // Convert FTS query if present
            let full_text_query = vq.base.full_text_search.as_ref().map(|fts| {
                let columns = fts.columns();
                let columns_vec = if columns.is_empty() {
                    None
                } else {
                    Some(columns.into_iter().collect())
                };
                Box::new(QueryTableRequestFullTextQuery {
                    string_query: Some(Box::new(StringFtsQuery {
                        query: fts.query.to_string(),
                        columns: columns_vec,
                    })),
                    structured_query: None,
                })
            });

            Ok(NsQueryTableRequest {
                id: None, // Will be set in namespace_query
                k: vq.base.limit.unwrap_or(10) as i32,
                vector: Box::new(vector),
                vector_column: vq.column.clone(),
                filter,
                columns,
                offset: vq.base.offset.map(|o| o as i32),
                distance_type: vq.distance_type.map(|dt| dt.to_string()),
                nprobes: Some(vq.minimum_nprobes as i32),
                ef: vq.ef.map(|e| e as i32),
                refine_factor: vq.refine_factor.map(|r| r as i32),
                lower_bound: vq.lower_bound,
                upper_bound: vq.upper_bound,
                prefilter: Some(vq.base.prefilter),
                fast_search: Some(vq.base.fast_search),
                with_row_id: Some(vq.base.with_row_id),
                bypass_vector_index: Some(!vq.use_index),
                full_text_query,
                ..Default::default()
            })
        }
        AnyQuery::Query(q) => {
            // For non-vector queries, pass an empty vector (similar to remote table implementation)
            if q.reranker.is_some() {
                return Err(Error::NotSupported {
                    message: "Reranker is not supported for server-side query execution"
                        .to_string(),
                });
            }

            let filter = q.filter.as_ref().map(filter_to_sql).transpose()?;

            let columns = match &q.select {
                Select::All => None,
                Select::Columns(cols) => Some(Box::new(QueryTableRequestColumns {
                    column_names: Some(cols.clone()),
                    column_aliases: None,
                })),
                Select::Dynamic(_) => {
                    return Err(Error::NotSupported {
                        message: "Dynamic columns are not supported for server-side query"
                            .to_string(),
                    });
                }
                Select::Expr(pairs) => {
                    let sql_pairs: crate::Result<Vec<(String, String)>> = pairs
                        .iter()
                        .map(|(name, expr)| expr_to_sql_string(expr).map(|sql| (name.clone(), sql)))
                        .collect();
                    let sql_pairs = sql_pairs?;
                    Some(Box::new(QueryTableRequestColumns {
                        column_names: None,
                        column_aliases: Some(sql_pairs.into_iter().collect()),
                    }))
                }
            };

            // Handle full text search if present
            let full_text_query = q.full_text_search.as_ref().map(|fts| {
                let columns_vec = if fts.columns().is_empty() {
                    None
                } else {
                    Some(fts.columns().iter().cloned().collect())
                };
                Box::new(QueryTableRequestFullTextQuery {
                    string_query: Some(Box::new(StringFtsQuery {
                        query: fts.query.to_string(),
                        columns: columns_vec,
                    })),
                    structured_query: None,
                })
            });

            // Empty vector for non-vector queries
            let vector = Box::new(QueryTableRequestVector {
                single_vector: Some(vec![]),
                multi_vector: None,
            });

            Ok(NsQueryTableRequest {
                id: None, // Will be set by caller
                vector,
                k: q.limit.unwrap_or(10) as i32,
                filter,
                columns,
                prefilter: Some(q.prefilter),
                offset: q.offset.map(|o| o as i32),
                vector_column: None, // No vector column for plain queries
                with_row_id: Some(q.with_row_id),
                bypass_vector_index: Some(true), // No vector index for plain queries
                full_text_query,
                ..Default::default()
            })
        }
    }
}

fn filter_to_sql(filter: &QueryFilter) -> Result<String> {
    match filter {
        QueryFilter::Sql(sql) => Ok(sql.clone()),
        QueryFilter::Substrait(_) => Err(Error::NotSupported {
            message: "Substrait filters are not supported for server-side queries".to_string(),
        }),
        QueryFilter::Datafusion(expr) => expr_to_sql_string(expr),
    }
}

/// Extract query vector(s) from Arrow arrays into the namespace format.
fn extract_query_vector(
    query_vectors: &[Arc<dyn arrow_array::Array>],
) -> Result<QueryTableRequestVector> {
    if query_vectors.is_empty() {
        return Err(Error::InvalidInput {
            message: "Query vector is required for vector search".to_string(),
        });
    }

    // Handle single vector case
    if query_vectors.len() == 1 {
        let arr = &query_vectors[0];
        let single_vector = array_to_f32_vec(arr)?;
        Ok(QueryTableRequestVector {
            single_vector: Some(single_vector),
            multi_vector: None,
        })
    } else {
        // Handle multi-vector case
        let multi_vector: Result<Vec<Vec<f32>>> =
            query_vectors.iter().map(array_to_f32_vec).collect();
        Ok(QueryTableRequestVector {
            single_vector: None,
            multi_vector: Some(multi_vector?),
        })
    }
}

/// Convert an Arrow array to a Vec<f32>.
fn array_to_f32_vec(arr: &Arc<dyn arrow_array::Array>) -> Result<Vec<f32>> {
    // Handle FixedSizeList (common for vectors)
    if let Some(fsl) = arr
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
    {
        let values = fsl.values();
        if let Some(f32_arr) = values.as_any().downcast_ref::<arrow_array::Float32Array>() {
            return Ok(f32_arr.values().to_vec());
        }
    }

    // Handle direct Float32Array
    if let Some(f32_arr) = arr.as_any().downcast_ref::<arrow_array::Float32Array>() {
        return Ok(f32_arr.values().to_vec());
    }

    Err(Error::InvalidInput {
        message: "Query vector must be Float32 type".to_string(),
    })
}

/// Magic bytes that prefix (and suffix) the Arrow IPC *file* format.
const ARROW_IPC_FILE_MAGIC: &[u8] = b"ARROW1";

/// Parse Arrow IPC response from the namespace server.
///
/// The server may return either the Arrow IPC *file* format or the *stream*
/// format. REST/phalanx returns the file format (it begins with the `ARROW1`
/// magic); reading that with a `StreamReader` fails with "failed to fill whole
/// buffer". Detect the magic and pick the matching reader so both are handled.
async fn parse_arrow_ipc_response(bytes: bytes::Bytes) -> Result<DatasetRecordBatchStream> {
    use arrow_ipc::reader::{FileReader, StreamReader};
    use std::io::Cursor;

    let (schema, batches) = if bytes.starts_with(ARROW_IPC_FILE_MAGIC) {
        let reader = FileReader::try_new(Cursor::new(bytes), None).map_err(|e| Error::Runtime {
            message: format!("Failed to parse Arrow IPC file response: {}", e),
        })?;
        let schema = reader.schema();
        let batches = reader
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to read Arrow IPC file batches: {}", e),
            })?;
        (schema, batches)
    } else {
        let reader =
            StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| Error::Runtime {
                message: format!("Failed to parse Arrow IPC response: {}", e),
            })?;
        let schema = reader.schema();
        let batches = reader
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Runtime {
                message: format!("Failed to read Arrow IPC batches: {}", e),
            })?;
        (schema, batches)
    };

    // Create a stream from the batches
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    let record_batch_stream =
        Box::pin(datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream));

    Ok(DatasetRecordBatchStream::new(record_batch_stream))
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray,
        StructArray,
    };
    use futures::TryStreamExt;
    use lance_arrow::FixedSizeListArrayExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::query::{ExecutableQuery, QueryBase, QueryExecutionOptions, QueryRequest};
    use crate::table::BaseTable;

    fn fixed_size_list_array(values: Vec<f32>, dimension: i32) -> FixedSizeListArray {
        FixedSizeListArray::try_new_from_values(Float32Array::from(values), dimension).unwrap()
    }

    #[tokio::test]
    async fn test_parse_arrow_ipc_response_handles_file_and_stream() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_ipc::writer::{FileWriter, StreamWriter};
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .unwrap();

        // Arrow IPC *file* format -- what REST/phalanx returns. Previously this
        // failed with "failed to fill whole buffer" because we used a StreamReader.
        let mut file_buf = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut file_buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        assert!(file_buf.starts_with(ARROW_IPC_FILE_MAGIC));
        let rows: usize = parse_arrow_ipc_response(bytes::Bytes::from(file_buf))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);

        // Arrow IPC *stream* format must still parse.
        let mut stream_buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut stream_buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        assert!(!stream_buf.starts_with(ARROW_IPC_FILE_MAGIC));
        let rows: usize = parse_arrow_ipc_response(bytes::Bytes::from(stream_buf))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_convert_to_namespace_query_vector() {
        let query_vector = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]));

        let vq = VectorQueryRequest {
            base: QueryRequest {
                limit: Some(10),
                offset: Some(5),
                filter: Some(QueryFilter::Sql("id > 0".to_string())),
                select: Select::Columns(vec!["id".to_string()]),
                ..Default::default()
            },
            column: Some("vector".to_string()),
            // We cast here to satisfy the struct definition
            query_vector: vec![query_vector as Arc<dyn Array>],
            minimum_nprobes: 20,
            distance_type: Some(crate::DistanceType::L2),
            ..Default::default()
        };

        let any_query = AnyQuery::VectorQuery(vq);

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 10);
        assert_eq!(ns_request.offset, Some(5));
        assert_eq!(ns_request.filter, Some("id > 0".to_string()));
        assert_eq!(
            ns_request
                .columns
                .as_ref()
                .and_then(|c| c.column_names.as_ref()),
            Some(&vec!["id".to_string()])
        );
        assert_eq!(ns_request.vector_column, Some("vector".to_string()));
        assert_eq!(ns_request.distance_type, Some("l2".to_string()));

        // Verify the vector data was extracted correctly
        assert!(ns_request.vector.single_vector.is_some());
        assert_eq!(
            ns_request.vector.single_vector.as_ref().unwrap(),
            &vec![1.0, 2.0, 3.0, 4.0]
        );
    }

    #[test]
    fn test_convert_to_namespace_query_plain_query() {
        let q = QueryRequest {
            limit: Some(20),
            offset: Some(5),
            filter: Some(QueryFilter::Sql("id > 5".to_string())),
            select: Select::Columns(vec!["id".to_string()]),
            with_row_id: true,
            ..Default::default()
        };

        let any_query = AnyQuery::Query(q);

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 20);
        assert_eq!(ns_request.offset, Some(5));
        assert_eq!(ns_request.filter, Some("id > 5".to_string()));
        assert_eq!(
            ns_request
                .columns
                .as_ref()
                .and_then(|c| c.column_names.as_ref()),
            Some(&vec!["id".to_string()])
        );
        assert_eq!(ns_request.with_row_id, Some(true));
        assert_eq!(ns_request.bypass_vector_index, Some(true));
        assert!(ns_request.vector_column.is_none());

        assert!(ns_request.vector.single_vector.as_ref().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_execute_query_local_routing() {
        use crate::connect;
        use crate::table::query::execute_query;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let table = conn
            .create_table("test_routing", vec![batch])
            .execute()
            .await
            .unwrap();

        let native_table = table.as_native().unwrap();

        // Setup a request
        let req = QueryRequest {
            filter: Some(QueryFilter::Sql("id > 3".to_string())),
            ..Default::default()
        };
        let query = AnyQuery::Query(req);

        // Action: Call execute_query directly
        // This validates that execute_query correctly routes to the local DataFusion engine
        // when table.namespace_client is None.
        let stream = execute_query(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();

        // Verify results
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(count, 2); // 4 and 5
    }

    #[tokio::test]
    async fn test_missing_filter_field_lists_nested_fields_in_local_planners() {
        use crate::connect;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let metadata = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("year", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![2024])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("genre", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["fiction"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("Title", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("true", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![8])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
            ),
        ]));
        let vector = Arc::new(fixed_size_list_array(vec![0.0, 1.0], 2));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vector.data_type().clone(), false),
            Field::new("content", DataType::Utf8, false),
            Field::new("metadata", metadata.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                vector,
                Arc::new(StringArray::from(vec!["example"])),
                metadata,
            ],
        )
        .unwrap();
        let table = conn
            .create_table("nested_error", batch)
            .execute()
            .await
            .unwrap();

        let error = table
            .query()
            .only_if("year = 2024")
            .execute()
            .await
            .err()
            .expect("query should reject the unqualified nested field");
        let case_sensitive_path = "`metadata`.`Title`";
        let keyword_path = "`metadata`.`true`";
        let expected = format!(
            "No field named year. Valid fields are `id`, `vector`, `content`, `metadata`.`year`, `metadata`.`genre`, {case_sensitive_path}, {keyword_path}."
        );

        assert!(
            error.to_string().contains(&expected),
            "unexpected error: {error}"
        );
        for (path, value) in [(case_sensitive_path, 7), (keyword_path, 8)] {
            table
                .query()
                .only_if(format!("{path} = {value}"))
                .execute()
                .await
                .expect("the path advertised by the diagnostic should be reusable");
        }

        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(crate::table::LsmWriteSpec::unsharded())
            .await
            .unwrap();
        let lsm_error = table
            .query()
            .only_if("year = 2024")
            .execute()
            .await
            .err()
            .expect("LSM query should reject the unqualified nested field");

        assert!(
            lsm_error.to_string().contains(&expected),
            "unexpected LSM error: {lsm_error}"
        );
        for (path, value) in [(case_sensitive_path, 7), (keyword_path, 8)] {
            table
                .query()
                .only_if(format!("{path} = {value}"))
                .execute()
                .await
                .expect("the path advertised by the diagnostic should be reusable in LSM queries");
        }
    }

    #[test]
    fn test_leaf_field_paths_preserve_arbitrary_depth() {
        use arrow_schema::{DataType, Field, Schema};

        fn nested_field(path: &[&str]) -> Field {
            let mut segments = path.iter().rev();
            let mut field = Field::new(
                *segments.next().expect("path must have a leaf"),
                DataType::Int32,
                false,
            );
            for segment in segments {
                field = Field::new(*segment, DataType::Struct(vec![field].into()), false);
            }
            field
        }

        let schema = Schema::new(vec![
            nested_field(&["a", "b", "c", "d", "e"]),
            nested_field(&["metadata", "child.with.dot"]),
            nested_field(&["metadata", "Title"]),
            nested_field(&["metadata", "123child"]),
            nested_field(&["metadata", "child`tick"]),
            nested_field(&["metadata", ""]),
            nested_field(&["", "child"]),
        ]);

        assert_eq!(
            leaf_field_paths(&schema),
            vec![
                "`a`.`b`.`c`.`d`.`e`",
                "`metadata`.`child.with.dot`",
                "`metadata`.`Title`",
                "`metadata`.`123child`",
                "`metadata`.`child``tick`",
            ]
        );

        let source = DataFusionError::SchemaError(
            Box::new(SchemaError::FieldNotFound {
                field: Box::new(Column::from_name("missing")),
                valid_fields: Vec::new(),
            }),
            Box::new(None),
        );
        let error = field_not_found_diagnostic(&source, &schema).unwrap();
        assert!(
            error.to_string().contains(
                "Valid fields are `a`.`b`.`c`.`d`.`e`, `metadata`.`child.with.dot`, `metadata`.`Title`, `metadata`.`123child`, `metadata`.`child``tick`"
            ),
            "unexpected error: {error}"
        );
    }

    #[derive(Debug, Default)]
    struct CountingNamespaceClient {
        query_table_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LanceNamespace for CountingNamespaceClient {
        fn namespace_id(&self) -> String {
            "counting".to_string()
        }

        async fn query_table(&self, _request: NsQueryTableRequest) -> lance::Result<bytes::Bytes> {
            self.query_table_calls.fetch_add(1, Ordering::SeqCst);
            panic!("query must not be pushed down to namespace query_table");
        }
    }

    #[tokio::test]
    async fn test_execute_query_pinned_snapshot_with_namespace_pushdown_runs_locally() {
        use crate::connect;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let table = conn
            .create_table("test_pinned_namespace_fallback", vec![batch])
            .execute()
            .await
            .unwrap();

        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        let snapshot = native_table.checkout_current().await.unwrap();
        let snapshot = snapshot.as_any().downcast_ref::<NativeTable>().unwrap();
        assert!(snapshot.dataset.time_travel_version().is_some());

        let query = AnyQuery::Query(QueryRequest {
            filter: Some(QueryFilter::Sql("id > 3".to_string())),
            ..Default::default()
        });
        let stream = execute_query(snapshot, &query, QueryExecutionOptions::default())
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );
        assert_eq!(namespace_client.query_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_execute_query_approx_mode_with_namespace_pushdown_runs_locally() {
        use crate::connect;
        use crate::table::query::execute_query;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();

        let vectors = Arc::new(fixed_size_list_array(
            vec![0.0, 0.0, 10.0, 10.0, 20.0, 20.0],
            2,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), vectors],
        )
        .unwrap();

        let table = conn
            .create_table("test_approx_mode_namespace_fallback", batch)
            .execute()
            .await
            .unwrap();
        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        let query_vector = Arc::new(Float32Array::from(vec![0.0, 0.0]));
        let query = AnyQuery::VectorQuery(VectorQueryRequest {
            base: QueryRequest {
                limit: Some(1),
                ..Default::default()
            },
            column: Some("vector".to_string()),
            query_vector: vec![query_vector as ArrayRef],
            approx_mode: Some(crate::ApproxMode::Accurate),
            ..Default::default()
        });

        let stream = execute_query(&native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(count, 1);
        assert_eq!(namespace_client.query_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_execute_query_use_lsm_with_namespace_pushdown_runs_locally() {
        use crate::connect;
        use crate::table::query::execute_query;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();

        let vectors = Arc::new(fixed_size_list_array(
            vec![0.0, 0.0, 10.0, 10.0, 20.0, 20.0],
            2,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), vectors],
        )
        .unwrap();

        let table = conn
            .create_table("test_use_lsm_namespace_fallback", batch)
            .execute()
            .await
            .unwrap();
        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        // `use_lsm` set (even to false) must force local execution — the namespace
        // request has no use_lsm field, so a pushdown would silently ignore it.
        let query_vector = Arc::new(Float32Array::from(vec![0.0, 0.0]));
        let query = AnyQuery::VectorQuery(VectorQueryRequest {
            base: QueryRequest {
                limit: Some(1),
                use_lsm: Some(false),
                ..Default::default()
            },
            column: Some("vector".to_string()),
            query_vector: vec![query_vector as ArrayRef],
            ..Default::default()
        });

        let stream = execute_query(&native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(count, 1);
        assert_eq!(namespace_client.query_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_query_snapshot_disables_namespace_pushdown() {
        use crate::connect;
        use crate::table::BaseTable;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let table = conn
            .create_table("test_snapshot_namespace_fallback", vec![batch])
            .execute()
            .await
            .unwrap();
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(Arc::new(CountingNamespaceClient::default()));
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        let snapshot = BaseTable::query_snapshot(&native_table).await.unwrap();
        let snapshot = snapshot.as_any().downcast_ref::<NativeTable>().unwrap();
        assert!(
            !can_execute_namespace_query(snapshot, &AnyQuery::Query(QueryRequest::default()),)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_create_plan_batch_vector_uses_shared_scan() {
        use arrow_array::{Float32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion_physical_plan::display::DisplayableExecutionPlan;

        use crate::table::query::create_plan;

        use crate::connect;

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));

        let batch = RecordBatch::new_empty(schema.clone());
        let table = conn
            .create_table("test_plan", vec![batch])
            .execute()
            .await
            .unwrap();
        let native_table = table.as_native().unwrap();

        // A batch of vectors against a fixed-size vector column should use
        // Lance's native batch KNN path instead of independent scan plans.
        let q1 = Arc::new(Float32Array::from(vec![1.0, 2.0]));
        let q2 = Arc::new(Float32Array::from(vec![3.0, 4.0]));

        let req = VectorQueryRequest {
            base: QueryRequest {
                filter: Some(QueryFilter::Sql("id >= 0".to_string())),
                limit: Some(1),
                select: Select::Columns(vec!["id".to_string()]),
                ..Default::default()
            },
            column: Some("vector".to_string()),
            query_vector: vec![q1, q2],
            ..Default::default()
        };
        let query = AnyQuery::VectorQuery(req);

        // Create the Plan
        let plan = create_plan(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();

        // formatting it allows us to see the hierarchy
        let display = DisplayableExecutionPlan::new(plan.as_ref())
            .indent(true)
            .to_string();

        assert!(
            display.contains("KNNVectorDistance: queries=2"),
            "plan should use native batch KNN, got:\n{display}"
        );
        assert!(
            !display.contains("UnionExec"),
            "flat batch KNN should share one scan, got:\n{display}"
        );
        assert!(
            display.contains("query_index"),
            "plan should add query_index column, got:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_cosine_pq_distance_uses_public_cosine_scale() {
        use arrow_array::{Int32Array, RecordBatch, types::Float32Type};
        use arrow_schema::{DataType, Field, Schema};

        use crate::connect;
        use crate::index::{Index, vector::IvfPqIndexBuilder};

        fn normalized_vector(state: &mut u64, dimension: usize) -> Vec<f32> {
            let mut vector = (0..dimension)
                .map(|_| {
                    *state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    ((*state >> 32) as u32 as f32 / u32::MAX as f32) * 2.0 - 1.0
                })
                .collect::<Vec<_>>();
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            vector.iter_mut().for_each(|value| *value /= norm);
            vector
        }

        fn distances(batches: &[RecordBatch]) -> Vec<f32> {
            batches
                .iter()
                .flat_map(|batch| {
                    batch[DIST_COL]
                        .as_primitive::<Float32Type>()
                        .values()
                        .to_vec()
                })
                .collect()
        }

        let conn = connect("memory://").execute().await.unwrap();
        let dimension = 8;
        let num_rows = 256;
        let mut state = 42;
        let values = (0..num_rows)
            .flat_map(|_| normalized_vector(&mut state, dimension))
            .collect::<Vec<_>>();
        let query_vector = normalized_vector(&mut state, dimension);
        let vectors = Arc::new(fixed_size_list_array(values, dimension as i32));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows)), vectors],
        )
        .unwrap();
        let table = conn
            .create_table("test_cosine_pq_distance", batch)
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(crate::DistanceType::Cosine)
                        .num_partitions(1)
                        .num_sub_vectors(1),
                ),
            )
            .execute()
            .await
            .unwrap();

        let approximate = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(5)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let refined = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(5)
            .refine_factor(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let approximate_distances = distances(&approximate);
        let refined_distances = distances(&refined);
        assert_eq!(approximate_distances.len(), refined_distances.len());
        for (approximate, refined) in approximate_distances.iter().zip(&refined_distances) {
            assert!(
                (approximate - refined).abs() < 1e-5,
                "approximate cosine distance {approximate} did not use the public scale; refined distance was {refined}"
            );
        }

        // Distance range bounds are public cosine distances too. Lance applies them to
        // internal ANN scores, so the planner must translate the bounds before execution.
        let nearest = approximate_distances[0];
        let ranged = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(1)
            .distance_range(Some(nearest - 1e-5), Some(nearest + 1e-5))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let ranged_distances = distances(&ranged);
        assert_eq!(ranged_distances.len(), 1);
        assert!((ranged_distances[0] - nearest).abs() < 1e-5);

        let refined_ranged = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(1)
            .refine_factor(1)
            .distance_range(None, Some(nearest + 1e-5))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            distances(&refined_ranged).len(),
            1,
            "refinement must not apply public cosine bounds to internal ANN scores"
        );

        let aliased = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(1)
            .select(Select::dynamic(&[("aliased_distance", "_distance")]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &aliased[0];
        let aliased_distance = batch["aliased_distance"]
            .as_primitive::<Float32Type>()
            .value(0);
        let public_distance = batch[DIST_COL].as_primitive::<Float32Type>().value(0);
        assert!(
            (aliased_distance - public_distance).abs() < 1e-5,
            "distance aliases and auto-projected distances must use the same public scale"
        );

        // Appended rows take an exact fallback branch. Its public range filter must stay
        // independent of the translated ANN bounds before both branches are merged.
        let mut orthogonal = normalized_vector(&mut state, dimension);
        let projection = orthogonal
            .iter()
            .zip(&query_vector)
            .map(|(left, right)| left * right)
            .sum::<f32>();
        for (value, query_value) in orthogonal.iter_mut().zip(&query_vector) {
            *value -= projection * query_value;
        }
        let norm = orthogonal
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        orthogonal.iter_mut().for_each(|value| *value /= norm);
        let appended_vectors = Arc::new(fixed_size_list_array(orthogonal, dimension as i32));
        let appended = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![num_rows])), appended_vectors],
        )
        .unwrap();
        table.add(appended).execute().await.unwrap();

        let mixed = table
            .vector_search(query_vector.as_slice())
            .unwrap()
            .limit(5)
            .distance_range(None, Some(nearest + 1e-5))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mixed_distances = distances(&mixed);
        assert_eq!(mixed_distances.len(), 1);
        assert!((mixed_distances[0] - nearest).abs() < 1e-5);
    }

    #[tokio::test]
    async fn test_create_plan_applies_approx_mode_to_ann_query() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        use datafusion_physical_plan::ExecutionPlan;
        use lance::io::exec::{ANNIvfPartitionExec, ANNIvfSubIndexExec};
        use lance_index::vector::ApproxMode;

        use crate::connect;
        use crate::index::{Index, vector::IvfRqIndexBuilder};
        use crate::table::query::create_plan;

        fn find_ann_approx_mode(plan: &dyn ExecutionPlan) -> Option<ApproxMode> {
            if let Some(ann) = (plan as &dyn std::any::Any).downcast_ref::<ANNIvfSubIndexExec>() {
                return Some(ann.query().approx_mode);
            }
            if let Some(ann) = (plan as &dyn std::any::Any).downcast_ref::<ANNIvfPartitionExec>() {
                return Some(ann.query.approx_mode);
            }
            plan.children()
                .into_iter()
                .find_map(|child| find_ann_approx_mode(child.as_ref()))
        }

        let conn = connect("memory://").execute().await.unwrap();
        let dimension = 8;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));

        let vectors = Arc::new(fixed_size_list_array(
            (0..512 * dimension)
                .map(|value| value as f32 / dimension as f32)
                .collect(),
            dimension,
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int32Array::from_iter_values(0..512)),
                vectors,
            ],
        )
        .unwrap();
        let table = conn
            .create_table("test_approx_mode_plan", vec![batch])
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfRq(
                    IvfRqIndexBuilder::default()
                        .num_partitions(1)
                        .sample_rate(1)
                        .max_iterations(1)
                        .num_bits(1),
                ),
            )
            .execute()
            .await
            .unwrap();
        let native_table = table.as_native().unwrap();
        let query_vector = Arc::new(Float32Array::from(vec![0.0; dimension as usize]));
        let query = AnyQuery::VectorQuery(VectorQueryRequest {
            column: Some("vector".to_string()),
            query_vector: vec![query_vector as ArrayRef],
            base: QueryRequest {
                limit: Some(1),
                ..Default::default()
            },
            approx_mode: Some(crate::ApproxMode::Accurate),
            ..Default::default()
        });

        let plan = create_plan(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();
        assert_eq!(
            find_ann_approx_mode(plan.as_ref()),
            Some(ApproxMode::Accurate)
        );
    }
}
