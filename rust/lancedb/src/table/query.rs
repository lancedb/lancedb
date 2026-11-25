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
use crate::index::scalar::{FtsQuery, Operator as FullTextOperator};
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
use datafusion_physical_plan::{ExecutionPlan, replace_children_if_necessary};
use lance::dataset::mem_wal::DatasetMemWalExt;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::scanner::Scanner;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance::io::exec::{ANNIvfBatchExec, ANNIvfSubIndexExec};
use lance_datafusion::exec::{analyze_plan as lance_analyze_plan, execute_plan};
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::vector::DIST_COL;
use lance_linalg::distance::DistanceType as LanceDistanceType;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    QueryTableRequest as NsQueryTableRequest, QueryTableRequestColumns,
    QueryTableRequestFullTextQuery, QueryTableRequestVector, StringFtsQuery, StructuredFtsQuery,
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
    // An RTE write spec means reads auto-route through the LSM scanner in
    // `create_plan` even when `use_rte` is unset. The namespace request has no
    // use_rte field, so pushing the default query down would silently omit
    // un-compacted rows — force local execution whenever a spec is installed.
    let dataset = table.dataset.get().await?;
    if dataset.mem_wal_index_details().await?.is_some() {
        return Ok(false);
    }
    Ok(true)
}

fn requires_local_namespace_execution(query: &AnyQuery) -> bool {
    // The namespace QueryTable request cannot represent approx_mode, search_effort,
    // use_rte, order_by or scan_in_order. For use_rte that is worse than a tuning miss:
    // RTE read routing lives only in `create_plan`, so a pushed-down query would return
    // stale base-only data. A dropped order_by would return the wrong top-K rows, and a
    // dropped ordered scan would silently stop guaranteeing manifest order.
    if query.base().use_rte.is_some()
        || query.base().has_take()
        || query.base().scan_in_order == Some(true)
        || query
            .base()
            .order_by
            .as_ref()
            .is_some_and(|order_by| !order_by.is_empty())
    {
        return true;
    }
    if matches!(query, AnyQuery::Query(query) if query.fast_search && query.full_text_search.is_some())
    {
        return true;
    }
    matches!(
        query,
        AnyQuery::VectorQuery(vector_query)
            if vector_query.approx_mode.is_some() || vector_query.search_effort.is_some()
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

/// Reject requests that ask for storage order in a query shape that cannot
/// preserve it, or that also define a different result order.
fn validate_scan_in_order(query: &AnyQuery) -> Result<()> {
    if query.base().scan_in_order != Some(true) {
        return Ok(());
    }

    let ranked = match query {
        AnyQuery::VectorQuery(_) => true,
        AnyQuery::Query(query) => {
            query.full_text_search.is_some() || query.reranker.is_some() || query.norm.is_some()
        }
    };
    if ranked {
        return Err(Error::InvalidInput {
            message: "scan_in_order(true) is only supported for ordinary scans; vector, \
                      full-text, hybrid, and reranked queries return ranked results"
                .to_string(),
        });
    }

    if query
        .base()
        .order_by
        .as_ref()
        .is_some_and(|order_by| !order_by.is_empty())
    {
        return Err(Error::InvalidInput {
            message: "scan_in_order(true) conflicts with a nonempty order_by; remove the \
                      ordering to keep manifest order, or remove scan_in_order(true) to sort"
                .to_string(),
        });
    }

    if query.base().has_take() {
        return crate::query::validate_take_scan_in_order(query.base());
    }

    Ok(())
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
    validate_scan_in_order(&query)?;
    if let AnyQuery::Query(request) = &query
        && request.has_take()
    {
        return crate::query::create_take_plan(table, request, options, request.preserve_order)
            .await;
    }

    let query = match query {
        AnyQuery::VectorQuery(query) => query,
        AnyQuery::Query(query) => VectorQueryRequest::from_plain_query(query),
    };
    query.base.check_filter()?;
    query.validate_search_effort()?;

    let ds_ref = table.dataset.get().await?;

    // RTE read routing driven by `use_rte`:
    //   * unset  — route through the LSM scanner iff the table carries a write spec
    //   * Some(true)  — force RTE routing; error if the table has no write spec
    //   * Some(false) — read the base table only, bypassing the RTE
    // The LSM scanner surfaces in-flight `merge_insert` data (active/frozen
    // memtables + SSTables); validation and dispatch live in `lsm`.
    let has_spec = ds_ref.mem_wal_index_details().await?.is_some();
    let use_rte = match query.base.use_rte {
        Some(true) if !has_spec => {
            return Err(Error::InvalidInput {
                message: "use_rte(true) was set but the table has no RTE write spec; \
                    install one with enable_rte or leave use_rte unset"
                    .to_string(),
            });
        }
        Some(enable) => enable,
        None => has_spec,
    };
    if use_rte {
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
        let (vector_type, element_type) =
            lance::index::vector::utils::get_vector_type(schema, column.as_ref().unwrap())?;
        let is_list_vector = matches!(&vector_type, DataType::List(_));
        let is_binary = matches!(element_type, DataType::UInt8);
        if is_list_vector || (query.base.offset.unwrap_or(0) == 0 && !is_binary) {
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
            is_batch_query = !is_list_vector;
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
    // Set only when this plan runs a vector search, so a distance type on a
    // non-vector request is left alone.
    let mut searched_column = None;

    if let Some(query_vector) = query_vector {
        let column = if let Some(col) = column {
            col
        } else {
            let arrow_schema = Schema::from(schema);
            default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
        };
        searched_column = Some(column.clone());

        let (_, element_type) = lance::index::vector::utils::get_vector_type(schema, &column)?;
        let is_binary = matches!(element_type, DataType::UInt8);
        let top_k = query
            .base
            .limit
            .unwrap_or(DEFAULT_TOP_K)
            .saturating_add(query.base.offset.unwrap_or(0))
            // Lance allocates candidates from k; no search can return more rows than the table.
            .min(ds_ref.count_rows(None).await?.max(1));

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

        if query.search_effort.is_none()
            && let Some(nprobes) = query.nprobes
        {
            if query.maximum_nprobes == Some(0) {
                scanner.minimum_nprobes(nprobes);
            } else {
                scanner.nprobes(nprobes);
            }
        }
        if let Some(minimum_nprobes) = query.minimum_nprobes {
            scanner.minimum_nprobes(minimum_nprobes);
        }
        if let Some(maximum_nprobes) = query.maximum_nprobes.filter(|nprobes| *nprobes != 0) {
            scanner.maximum_nprobes(maximum_nprobes);
        }
        if let Some(search_effort) = query.search_effort {
            scanner.search_effort(search_effort)?;
        }
    }

    // For a batch query, `nearest` already applies k to each query vector.
    // Adding Scanner's global limit would truncate the combined result to k rows.
    if !is_batch_query {
        let offset = query
            .base
            .offset
            .map(|offset| i64::try_from(offset).unwrap_or(i64::MAX));
        scanner.limit(
            // Lance's FTS planner adds limit + offset as i64.
            query.base.limit.map(|limit| {
                i64::try_from(limit)
                    .unwrap_or(i64::MAX)
                    .min(i64::MAX - offset.unwrap_or(0))
            }),
            offset,
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
        // Ordinary queries keep Lance's exact-scan fallback. fast_search does
        // not read unindexed rows, so a distance type with no matching covering
        // index would return nothing.
        if query.base.fast_search
            && query.use_index
            && let Some(column) = searched_column.as_deref()
        {
            reject_fast_search_metric_mismatch(&ds_ref, column, distance_type).await?;
        }
        scanner.distance_metric(distance_type.into());
    }

    if query.base.disable_scoring_autoprojection {
        scanner.disable_scoring_autoprojection();
    }

    if let Some(order_by) = &query.base.order_by {
        scanner.order_by(Some(order_by.clone()))?;
    }

    if let Some(scan_in_order) = query.base.scan_in_order {
        scanner.scan_in_order(scan_in_order);
        if scan_in_order {
            // Legacy scalar-index plans can union materialized index rows with
            // unindexed fragments and emit a different sequence. Explicit
            // storage order must come from the manifest scan, so filters fall
            // back to evaluating over the ordered fragments.
            scanner.use_scalar_index(false);
        }
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

/// Reject `fast_search` when the index Lance would select does not cover rows.
///
/// Selection follows Lance's scanner (`dataset/scanner/vector.rs`): candidates
/// are keyed by `fields[0]`, not `keyed_field()`. The metric comes from
/// manifest `VectorIndexDetails` when that message is populated. A segment
/// whose details are missing or empty is opened, the same fallback Lance uses.
/// The first candidate with the requested metric wins. If that index covers no
/// rows, Lance brute-forces and does not consider a later index, so
/// `fast_search` would return nothing. A missing fragment bitmap is full
/// coverage. With no covering index the check does nothing, so an unindexed
/// table is not rejected. If the index listing cannot be read, the check is
/// skipped.
async fn reject_fast_search_metric_mismatch(
    dataset: &lance::dataset::Dataset,
    column: &str,
    query_metric: crate::DistanceType,
) -> Result<()> {
    // `describe_indices` errors when a segment has no fragment bitmap. Those
    // indexes are still searchable, so use the scanner's own listing.
    let Ok(indices) = dataset.load_indices().await else {
        log::debug!("skipping vector metric check; index listing failed for column {column}");
        return Ok(());
    };
    // On this Lance, `load_indices` normally infers legacy details before the
    // scanner runs. The open below mirrors the scanner's residual branch for a
    // listing that still has missing or empty details.
    fast_search_index_decision(dataset, column, query_metric, indices.as_ref()).await?;
    Ok(())
}

/// Metric of the index `fast_search` would use, or `None` when nothing covers.
///
/// `indices` is the listing to select from. Callers that already loaded it can
/// pass that slice; tests can pass metadata `load_indices` would have inferred.
async fn fast_search_index_decision(
    dataset: &lance::dataset::Dataset,
    column: &str,
    query_metric: crate::DistanceType,
    indices: &[lance_table::format::IndexMetadata],
) -> Result<Option<crate::DistanceType>> {
    let Ok(column_id) = dataset.schema().field_id(column) else {
        return Ok(None);
    };

    let mut covering_metrics = Vec::new();
    for index in indices {
        if index.fields.first() != Some(&column_id) || !is_vector_index_segment(index) {
            continue;
        }
        // Metadata first. Open only when this segment has no metric. Stop at
        // the first match so a later segment that cannot be opened does not
        // fail a query Lance would already have answered.
        let metric = match metric_from_index_metadata(index) {
            Some(metric) => metric,
            None => dataset
                .open_vector_index(column, &index.uuid, &NoOpMetricsCollector)
                .await?
                .metric_type()
                .into(),
        };
        if metric == query_metric {
            if index_name_covers(dataset, indices, &index.name) {
                return Ok(Some(metric));
            }
            return Err(Error::InvalidInput {
                message: uncovered_match_message(column, query_metric),
            });
        }
        if segment_covers_rows(dataset, index) && !covering_metrics.contains(&metric) {
            covering_metrics.push(metric);
        }
    }

    // No candidate has this metric. A covering index of another metric makes
    // fast_search return nothing; with no covering index, leave it to Lance.
    if covering_metrics.is_empty() {
        return Ok(None);
    }
    covering_metrics.sort_by_key(|metric| metric.to_string());
    Err(Error::InvalidInput {
        message: mismatch_message(column, query_metric, &covering_metrics),
    })
}

/// Metric recorded in manifest `VectorIndexDetails`, without opening the index.
///
/// `None` means the segment is missing details or the value bytes are empty.
/// Callers open the index, matching Lance's scanner.
fn metric_from_index_metadata(
    index: &lance_table::format::IndexMetadata,
) -> Option<crate::DistanceType> {
    let details = index.index_details.as_ref()?;
    if details.value.is_empty() {
        return None;
    }
    let decoded = details
        .to_msg::<lance_index::pb::VectorIndexDetails>()
        .ok()?;
    let metric = lance_index::pb::VectorMetricType::try_from(decoded.metric_type).ok()?;
    Some(lance_linalg::distance::DistanceType::from(metric).into())
}

/// Whether this segment is a vector index Lance's scanner can select.
///
/// A missing `index_details` is a legacy vector segment (`index_type_is_known`
/// is true). A declared type matches Lance's `index_details_have_reader`: the
/// name after the last `/` equals the protobuf full name, case-insensitively,
/// including the pre-`pb` package name.
fn is_vector_index_segment(index: &lance_table::format::IndexMetadata) -> bool {
    match index.index_details.as_ref() {
        Some(details) => vector_details_type_is_known(&details.type_url),
        None => true,
    }
}

/// Lance compares the type URL's final component to these names without
/// regard to case (`index_details_have_reader`).
fn vector_details_type_is_known(type_url: &str) -> bool {
    let Some((_, name)) = type_url.rsplit_once('/') else {
        return false;
    };
    name.eq_ignore_ascii_case("lance.index.pb.VectorIndexDetails")
        || name.eq_ignore_ascii_case("lance.index.VectorIndexDetails")
}

fn uncovered_match_message(column: &str, query_metric: crate::DistanceType) -> String {
    format!(
        "cannot use fast_search on column '{column}' with distance type {query_metric} \
         because the first matching index covers no rows. Lance does not search a later \
         index. Remove fast_search for an exact scan with {query_metric}."
    )
}

/// Whether any segment of this logical index covers a current fragment.
///
/// This is Lance's `get_indexed_frags` for the segments of one name: a missing
/// bitmap is full coverage, and an empty bitmap covers nothing.
fn index_name_covers(
    dataset: &lance::dataset::Dataset,
    indices: &[lance_table::format::IndexMetadata],
    name: &str,
) -> bool {
    indices
        .iter()
        .filter(|index| index.name == name)
        .any(|index| segment_covers_rows(dataset, index))
}

fn mismatch_message(
    column: &str,
    query_metric: crate::DistanceType,
    covering_metrics: &[crate::DistanceType],
) -> String {
    let trained = if covering_metrics.len() == 1 {
        format!("it was trained with {}", covering_metrics[0])
    } else {
        format!(
            "those indexes were trained with {}",
            format_distance_types(covering_metrics)
        )
    };
    // Name the index metric only when there is one covering metric to use.
    let omit = if let [metric] = covering_metrics {
        format!(" Omit distance_type or distanceType to use the index metric ({metric}).")
    } else {
        String::new()
    };
    format!(
        "cannot use fast_search on column '{column}' with distance type {query_metric} \
         because {trained}.{omit} Remove fast_search for an exact scan with {query_metric}."
    )
}

fn format_distance_types(metrics: &[crate::DistanceType]) -> String {
    match metrics {
        [] => String::new(),
        [metric] => metric.to_string(),
        [first, rest @ ..] => {
            let mut message = first.to_string();
            for (index, metric) in rest.iter().enumerate() {
                if index + 1 == rest.len() {
                    message.push_str(" and ");
                } else {
                    message.push_str(", ");
                }
                message.push_str(&metric.to_string());
            }
            message
        }
    }
}

/// Whether this segment covers a fragment in the current dataset.
///
/// A missing fragment bitmap is full coverage, matching Lance's
/// `get_indexed_frags`. An empty bitmap covers nothing, so the scan is flat.
fn segment_covers_rows(
    dataset: &lance::dataset::Dataset,
    index: &lance_table::format::IndexMetadata,
) -> bool {
    let fragments = dataset.get_fragments();
    if fragments.is_empty() {
        return false;
    }
    match &index.fragment_bitmap {
        None => true,
        Some(bitmap) => fragments
            .iter()
            .any(|fragment| bitmap.contains(fragment.id() as u32)),
    }
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
    log::info!("Query field rejected: {error}");
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

    Error::FieldNotFound {
        name: field.name.clone(),
        message,
        source: Box::new(lance::Error::field_not_found(
            field.name.clone(),
            valid_fields,
        )),
    }
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

#[derive(Clone, Copy)]
enum AnnExecRef<'a> {
    Single(&'a ANNIvfSubIndexExec),
    Batch(&'a ANNIvfBatchExec),
}

impl<'a> AnnExecRef<'a> {
    fn from_plan(plan: &'a dyn ExecutionPlan) -> Option<Self> {
        if let Some(single) = plan.downcast_ref::<ANNIvfSubIndexExec>() {
            return Some(Self::Single(single));
        }
        plan.downcast_ref::<ANNIvfBatchExec>().map(Self::Batch)
    }

    fn query(self) -> &'a lance_index::vector::Query {
        match self {
            Self::Single(ann) => ann.query(),
            Self::Batch(ann) => ann.query(),
        }
    }

    fn dataset(self) -> &'a Arc<lance::Dataset> {
        match self {
            Self::Single(ann) => ann.dataset(),
            Self::Batch(ann) => ann.dataset(),
        }
    }

    fn indices(self) -> &'a [lance_table::format::IndexMetadata] {
        match self {
            Self::Single(ann) => ann.indices(),
            Self::Batch(ann) => ann.indices(),
        }
    }

    fn query_count(self) -> Option<usize> {
        match self {
            Self::Single(_) => None,
            Self::Batch(ann) => Some(ann.query_count()),
        }
    }

    fn same_kind(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Single(_), Self::Single(_)) | (Self::Batch(_), Self::Batch(_))
        )
    }
}

/// Find ANN index segments whose scores use normalized squared L2 for cosine search.
///
/// Cosine PQ/SQ/RQ indices normalize their vectors and use squared L2 internally.  This
/// preserves ranking, but squared L2 over unit vectors is twice the cosine distance.  Flat
/// cosine indices calculate cosine directly, so they are not included.
async fn normalized_l2_ann_indices(plan: &dyn ExecutionPlan) -> Result<HashSet<uuid::Uuid>> {
    let mut ann_plans = Vec::new();
    find_ann_plans(plan, &mut ann_plans);

    let mut checked = HashSet::new();
    let mut normalized_l2 = HashSet::new();
    for ann in ann_plans {
        if ann.query().metric_type != Some(LanceDistanceType::Cosine) {
            continue;
        }
        let uuids: Vec<_> = ann
            .indices()
            .iter()
            .map(|index| index.uuid)
            .filter(|uuid| checked.insert(*uuid))
            .collect();
        if uuids.is_empty() {
            continue;
        }
        let types = ann
            .dataset()
            .vector_index_types(&ann.query().column, &uuids)
            .await?;
        for (uuid, index_type) in uuids.into_iter().zip(types) {
            if matches!(
                index_type,
                IndexType::IvfPq
                    | IndexType::IvfSq
                    | IndexType::IvfRq
                    | IndexType::IvfHnswPq
                    | IndexType::IvfHnswSq
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

fn find_ann_plans<'a>(plan: &'a dyn ExecutionPlan, ann_plans: &mut Vec<AnnExecRef<'a>>) {
    if let Some(ann) = AnnExecRef::from_plan(plan) {
        ann_plans.push(ann);
        return;
    }
    for child in plan.children() {
        find_ann_plans(child.as_ref(), ann_plans);
    }
}

fn collect_ann_plans(
    plan: &Arc<dyn ExecutionPlan>,
    ann_plans: &mut VecDeque<Arc<dyn ExecutionPlan>>,
) {
    if AnnExecRef::from_plan(plan.as_ref()).is_some() {
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
    normalized_l2_indices: &HashSet<uuid::Uuid>,
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
    normalized_l2_indices: &HashSet<uuid::Uuid>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(public_ann) = AnnExecRef::from_plan(public_plan.as_ref()) {
        let internal_plan = internal_ann_plans
            .pop_front()
            .ok_or_else(|| Error::Runtime {
                message: "internal vector plan was missing an ANN branch".to_string(),
            })?;
        let internal_ann =
            AnnExecRef::from_plan(internal_plan.as_ref()).ok_or_else(|| Error::Runtime {
                message: "internal vector plan contained a non-ANN branch where an ANN branch was expected"
                    .to_string(),
            })?;
        let same_indices = public_ann
            .indices()
            .iter()
            .map(|index| &index.uuid)
            .eq(internal_ann.indices().iter().map(|index| &index.uuid));
        if !public_ann.same_kind(internal_ann)
            || public_ann.query_count() != internal_ann.query_count()
            || public_ann.query().column != internal_ann.query().column
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
            .filter(|index| normalized_l2_indices.contains(&index.uuid))
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
    Ok(replace_children_if_necessary(public_plan, children)?)
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

fn namespace_fts_query(fts: &FullTextSearchQuery) -> Result<Box<QueryTableRequestFullTextQuery>> {
    if !matches!(&fts.query, FtsQuery::Match(query) if query.operator == FullTextOperator::Or) {
        let query = serde_json::to_value(&fts.query)
            .and_then(serde_json::from_value)
            .map_err(|error| Error::InvalidInput {
                message: format!("Cannot serialize namespace full-text query: {error}"),
            })?;
        Ok(Box::new(QueryTableRequestFullTextQuery {
            string_query: None,
            structured_query: Some(Box::new(StructuredFtsQuery::new(query))),
        }))
    } else {
        let columns = fts.columns();
        Ok(Box::new(QueryTableRequestFullTextQuery {
            string_query: Some(Box::new(StringFtsQuery {
                query: fts.query.query().clone(),
                columns: if columns.is_empty() {
                    None
                } else {
                    Some(columns.into_iter().collect())
                },
            })),
            structured_query: None,
        }))
    }
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
            if vq.search_effort.is_some() {
                return Err(Error::NotSupported {
                    message: "namespace QueryTable does not support search_effort; execute the query locally".to_string(),
                });
            }
            if vq.base.reranker.is_some() {
                return Err(Error::NotSupported {
                    message: "Reranker is not supported for server-side queries".to_string(),
                });
            }

            // Convert FTS query if present
            let full_text_query = vq
                .base
                .full_text_search
                .as_ref()
                .map(namespace_fts_query)
                .transpose()?;

            Ok(NsQueryTableRequest {
                id: None, // Will be set in namespace_query
                k: vq.base.limit.unwrap_or(10) as i32,
                vector: Box::new(vector),
                vector_column: vq.column.clone(),
                filter,
                columns,
                offset: vq.base.offset.map(|o| o as i32),
                distance_type: vq.distance_type.map(|dt| dt.to_string()),
                nprobes: vq.nprobes.map(|nprobes| nprobes as i32),
                minimum_nprobes: vq.minimum_nprobes.map(|nprobes| nprobes as i32),
                maximum_nprobes: vq.maximum_nprobes.map(|nprobes| nprobes as i32),
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

            let k = match q.limit {
                Some(limit) => limit as i32,
                None => i32::MAX,
            };

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
            let full_text_query = q
                .full_text_search
                .as_ref()
                .map(namespace_fts_query)
                .transpose()?;

            // Empty vector for non-vector queries
            let vector = Box::new(QueryTableRequestVector {
                single_vector: Some(vec![]),
                multi_vector: None,
            });

            Ok(NsQueryTableRequest {
                id: None, // Will be set by caller
                vector,
                k,
                filter,
                columns,
                prefilter: Some(q.prefilter),
                fast_search: Some(q.fast_search),
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
    use crate::index::scalar::MatchQuery;
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

    fn collect_i32(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
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
        assert_eq!(ns_request.nprobes, None);

        // Verify the vector data was extracted correctly
        assert!(ns_request.vector.single_vector.is_some());
        assert_eq!(
            ns_request.vector.single_vector.as_ref().unwrap(),
            &vec![1.0, 2.0, 3.0, 4.0]
        );
    }

    #[test]
    fn test_namespace_query_probe_pushdown_compatibility() {
        let default_query = AnyQuery::VectorQuery(VectorQueryRequest::default());
        assert!(!requires_local_namespace_execution(&default_query));

        for effort in [0.0, 0.5, 1.0] {
            let effort_query = AnyQuery::VectorQuery(VectorQueryRequest {
                query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
                search_effort: Some(effort),
                ..Default::default()
            });
            assert!(requires_local_namespace_execution(&effort_query));
            assert!(matches!(
                convert_to_namespace_query(&effort_query),
                Err(Error::NotSupported { message }) if message.contains("search_effort")
            ));
        }

        let nprobes_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
            nprobes: Some(20),
            ..Default::default()
        });
        assert!(!requires_local_namespace_execution(&nprobes_query));
        assert_eq!(
            convert_to_namespace_query(&nprobes_query).unwrap().nprobes,
            Some(20)
        );

        let minimum_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
            minimum_nprobes: Some(5),
            ..Default::default()
        });
        assert!(!requires_local_namespace_execution(&minimum_query));
        let request = convert_to_namespace_query(&minimum_query).unwrap();
        assert_eq!(request.minimum_nprobes, Some(5));
        assert_eq!(request.maximum_nprobes, None);

        let maximum_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
            maximum_nprobes: Some(10),
            ..Default::default()
        });
        assert!(!requires_local_namespace_execution(&maximum_query));
        let request = convert_to_namespace_query(&maximum_query).unwrap();
        assert_eq!(request.minimum_nprobes, None);
        assert_eq!(request.maximum_nprobes, Some(10));

        let exact_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
            nprobes: Some(8),
            minimum_nprobes: Some(20),
            maximum_nprobes: Some(20),
            ..Default::default()
        });
        assert!(!requires_local_namespace_execution(&exact_query));
        let request = convert_to_namespace_query(&exact_query).unwrap();
        assert_eq!(request.nprobes, Some(8));
        assert_eq!(request.minimum_nprobes, Some(20));
        assert_eq!(request.maximum_nprobes, Some(20));

        let explicitly_unbounded_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![Arc::new(Float32Array::from(vec![1.0, 2.0]))],
            nprobes: Some(8),
            maximum_nprobes: Some(0),
            ..Default::default()
        });
        let request = convert_to_namespace_query(&explicitly_unbounded_query).unwrap();
        assert_eq!(request.nprobes, Some(8));
        assert_eq!(request.maximum_nprobes, Some(0));
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

    #[test]
    fn test_convert_to_namespace_query_plain_query_without_limit() {
        let any_query = AnyQuery::Query(QueryRequest::default());

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, i32::MAX);
    }

    #[test]
    fn test_convert_to_namespace_query_plain_query_with_explicit_limit() {
        let any_query = AnyQuery::Query(QueryRequest {
            limit: Some(10),
            ..Default::default()
        });

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 10);
    }

    #[test]
    fn test_convert_to_namespace_query_vector_without_limit() {
        let query_vector = Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let any_query = AnyQuery::VectorQuery(VectorQueryRequest {
            query_vector: vec![query_vector as ArrayRef],
            ..Default::default()
        });

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, 10);
    }

    #[test]
    fn test_convert_to_namespace_query_fts_without_limit() {
        let any_query = AnyQuery::Query(QueryRequest {
            full_text_search: Some(lance_index::scalar::FullTextSearchQuery::new(
                "hello world".to_string(),
            )),
            ..Default::default()
        });

        let ns_request = convert_to_namespace_query(&any_query).unwrap();

        assert_eq!(ns_request.k, i32::MAX);
    }

    #[test]
    fn test_namespace_fts_operators_and_unbounded_counts() {
        for columns in [
            vec!["text".to_string()],
            vec!["title".to_string(), "body".to_string()],
        ] {
            let fts = FullTextSearchQuery::new_query(
                MatchQuery::new("gripper closed".to_string())
                    .with_operator(FullTextOperator::And)
                    .into(),
            )
            .with_columns(&columns)
            .unwrap();
            let request = convert_to_namespace_query(&AnyQuery::Query(QueryRequest {
                full_text_search: Some(fts),
                ..Default::default()
            }))
            .unwrap();
            assert_eq!(request.k, i32::MAX);
            let full_text_query = request.full_text_query.unwrap();
            assert!(full_text_query.string_query.is_none());
            let query =
                serde_json::to_value(full_text_query.structured_query.unwrap().query).unwrap();
            if columns.len() == 1 {
                assert_eq!(query["match"]["operator"], "And");
                assert_eq!(query["match"]["column"], "text");
            } else {
                let leaves = query["multi_match"]["match_queries"].as_array().unwrap();
                assert_eq!(leaves.len(), 2);
                assert!(leaves.iter().all(|leaf| leaf["operator"] == "And"));
            }
        }
        let request =
            namespace_fts_query(&FullTextSearchQuery::new("gripper closed".to_string())).unwrap();
        assert_eq!(request.string_query.unwrap().query, "gripper closed");
        assert!(request.structured_query.is_none());
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
            .enable_rte(crate::table::RTEWriteSpec::unsharded())
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
        assert!(matches!(&error, Error::FieldNotFound { name, .. } if name == "missing"));
        assert!(matches!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<lance::Error>(),
            Some(lance::Error::FieldNotFound { .. })
        ));
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
    async fn test_execute_query_use_rte_with_namespace_pushdown_runs_locally() {
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
            .create_table("test_use_rte_namespace_fallback", batch)
            .execute()
            .await
            .unwrap();
        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        // `use_rte` set (even to false) must force local execution — the namespace
        // request has no use_rte field, so a pushdown would silently ignore it.
        let query_vector = Arc::new(Float32Array::from(vec![0.0, 0.0]));
        let query = AnyQuery::VectorQuery(VectorQueryRequest {
            base: QueryRequest {
                limit: Some(1),
                use_rte: Some(false),
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
    async fn test_execute_query_order_by_with_namespace_pushdown_runs_locally() {
        use crate::connect;
        use crate::query::ColumnOrdering;
        use crate::table::query::execute_query;
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        // Insertion order differs from ascending id, so a pushed-down query that
        // drops order_by would return 3 instead of the correct top-1 row 1.
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![3, 1, 2]))]).unwrap();

        let table = conn
            .create_table("test_order_by_namespace_fallback", vec![batch])
            .execute()
            .await
            .unwrap();
        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        let query = AnyQuery::Query(QueryRequest {
            limit: Some(1),
            order_by: Some(vec![ColumnOrdering::asc_nulls_first("id".to_string())]),
            ..Default::default()
        });

        let stream = execute_query(&native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();

        assert_eq!(ids, vec![1]);
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
    async fn test_create_plan_batch_vector_missing_column_returns_sdk_error() {
        use crate::connect;
        use crate::table::query::create_plan;
        use arrow_array::{Float32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let table = conn
            .create_table("test_batch_vector_missing_column", vec![batch])
            .execute()
            .await
            .unwrap();
        let native_table = table.as_native().unwrap();

        let query = AnyQuery::VectorQuery(VectorQueryRequest {
            base: QueryRequest {
                limit: Some(1),
                ..Default::default()
            },
            column: Some("missing_vector".to_string()),
            query_vector: vec![
                Arc::new(Float32Array::from(vec![1.0, 2.0])),
                Arc::new(Float32Array::from(vec![3.0, 4.0])),
            ],
            ..Default::default()
        });

        let error = create_plan(native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap_err();

        assert!(
            matches!(
                &error,
                Error::Lance {
                    source: lance::Error::Index { message, .. }
                } if message.contains("column missing_vector does not exist in schema")
            ),
            "expected ordinary vector column validation error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_cold_cosine_rq_plan_only_needs_index_header() {
        use crate::index::{Index, vector::IvfRqIndexBuilder};
        use lance::index::DatasetIndexExt;

        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let conn = crate::connect(uri).execute().await.unwrap();
        let vectors = Arc::new(fixed_size_list_array(
            (0..512 * 8).map(|n| (1 + n % 31) as f32).collect(),
            8,
        ));
        let schema = Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "vector",
            vectors.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![vectors]).unwrap();
        let table = conn.create_table("vectors", batch).execute().await.unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfRq(
                    IvfRqIndexBuilder::default()
                        .distance_type(crate::DistanceType::Cosine)
                        .num_partitions(1),
                ),
            )
            .execute()
            .await
            .unwrap();
        let dataset = table.dataset().unwrap().get().await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        // The quantizer is needed to execute the search. Planning should only read
        // the segment's type from index.idx, even with an empty session cache.
        let auxiliary = dir
            .path()
            .join("vectors.lance/_indices")
            .join(indices[0].uuid.to_string())
            .join(lance_index::INDEX_AUXILIARY_FILE_NAME);
        std::fs::remove_file(auxiliary).unwrap();
        let conn = crate::connect(uri).execute().await.unwrap();
        let table = conn.open_table("vectors").execute().await.unwrap();
        let plan = table
            .vector_search([1.0_f32; 8].as_slice())
            .unwrap()
            .create_plan(QueryExecutionOptions::default())
            .await
            .unwrap();
        let mut ann = Vec::new();
        find_ann_plans(plan.as_ref(), &mut ann);
        assert_eq!(ann.len(), 1);
        assert_eq!(ann[0].query().metric_type, Some(LanceDistanceType::Cosine));
    }

    #[tokio::test]
    async fn test_vector_search_metric_mismatch_falls_back_unless_fast_search() {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;

        let table = distinct_metric_table("memory://", "metric_mismatch").await;
        let cosine = Some(crate::DistanceType::Cosine);
        let l2 = Some(crate::DistanceType::L2);

        // No index: an explicit metric is a flat search.
        assert_eq!(top_id(&table, cosine, false).await, 1);

        table
            .create_index(
                &["vector"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1)),
            )
            .execute()
            .await
            .unwrap();

        // The L2 index would return id 0. An ordinary cosine query must keep
        // the exact neighbor instead.
        assert_eq!(top_id(&table, cosine, false).await, 1);
        let query = [1.0_f32, 0.0, 0.0, 0.0];
        let bypass = table
            .vector_search(query.as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .bypass_vector_index()
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_i32(&bypass), vec![1]);

        let Err(fast_search) = table
            .vector_search(query.as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(1)
            .execute()
            .await
        else {
            panic!("fast_search with a mismatched distance type should be rejected");
        };
        match &fast_search {
            Error::InvalidInput { message } => {
                assert!(message.contains("fast_search"), "{message}");
                assert!(message.contains("l2"), "{message}");
                assert!(message.contains("cosine"), "{message}");
                assert!(message.contains("exact scan"), "{message}");
                assert!(!message.contains("bypass"), "{message}");
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }

        assert_eq!(top_id(&table, l2, false).await, 0);
        assert_eq!(top_id(&table, l2, true).await, 0);
        assert_eq!(top_id(&table, None, false).await, 0);
    }

    async fn vector_table(name: &str) -> crate::Table {
        use crate::connect;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let num_rows = 8i32;
        let values = (0..num_rows)
            .flat_map(|id| [id as f32 + 1.0, 0.0, 0.0, 0.0])
            .collect::<Vec<_>>();
        let vectors = Arc::new(fixed_size_list_array(values, 4));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows)), vectors],
        )
        .unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    /// Rows whose nearest neighbor depends on the metric.
    ///
    /// The query is `[1, 0, 0, 0]`. Id 0 is nearest by L2, id 1 by cosine, and
    /// id 2 by dot. The other rows are far under every metric.
    async fn distinct_metric_table(uri: &str, name: &str) -> crate::Table {
        use crate::connect;
        use arrow_schema::{DataType, Field, Schema};

        let rows = [
            [1.0_f32, 0.1, 0.0, 0.0],
            [0.2, 0.0, 0.0, 0.0],
            [3.0, 1.5, 0.0, 0.0],
            [0.0, 0.0, 10.0, 0.0],
            [0.0, 0.0, 10.0, 0.0],
            [0.0, 0.0, 10.0, 0.0],
            [0.0, 0.0, 10.0, 0.0],
            [0.0, 0.0, 10.0, 0.0],
        ];
        let values = rows.into_iter().flatten().collect::<Vec<_>>();
        let vectors = Arc::new(fixed_size_list_array(values, 4));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)),
                vectors,
            ],
        )
        .unwrap();
        let conn = connect(uri).execute().await.unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    async fn top_id(
        table: &crate::Table,
        metric: Option<crate::DistanceType>,
        fast_search: bool,
    ) -> i32 {
        let mut search = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap();
        if let Some(metric) = metric {
            search = search.distance_type(metric);
        }
        if fast_search {
            search = search.fast_search();
        }
        let batches = search
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        collect_i32(&batches)[0]
    }

    #[tokio::test]
    async fn test_matching_metric_selects_that_index() {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;

        let table = distinct_metric_table("memory://", "two_metrics").await;
        table
            .create_index(
                &["vector"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1)),
            )
            .name("vector_l2".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfFlat(
                    IvfFlatIndexBuilder::default()
                        .distance_type(crate::DistanceType::Cosine)
                        .num_partitions(1),
                ),
            )
            .name("vector_cosine".to_string())
            .replace(false)
            .execute()
            .await
            .unwrap();

        // fast_search returns this row only when the cosine index is used.
        // The L2 index's neighbor is id 0, and a flat scan returns nothing.
        assert_eq!(
            top_id(&table, Some(crate::DistanceType::Cosine), true).await,
            1
        );
        // Dot matches neither index, so the ordinary query is an exact scan.
        assert_eq!(
            top_id(&table, Some(crate::DistanceType::Dot), false).await,
            2
        );

        let Err(mismatch) = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Dot)
            .fast_search()
            .limit(1)
            .execute()
            .await
        else {
            panic!("fast_search dot should match neither index");
        };
        match &mismatch {
            Error::InvalidInput { message } => {
                assert!(message.contains("dot"), "{message}");
                assert!(message.contains("l2"), "{message}");
                assert!(message.contains("cosine"), "{message}");
                assert!(message.contains("fast_search"), "{message}");
                assert!(!message.contains("bypass"), "{message}");
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_nested_vector_distance_mismatch_is_rejected() {
        use crate::connect;
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let num_rows = 8i32;
        let values = (0..num_rows)
            .flat_map(|id| [id as f32 + 1.0, 0.0, 0.0, 0.0])
            .collect::<Vec<_>>();
        let embeddings = fixed_size_list_array(values, 4);
        let image = StructArray::from(vec![(
            Arc::new(Field::new(
                "embedding",
                embeddings.data_type().clone(),
                false,
            )),
            Arc::new(embeddings) as ArrayRef,
        )]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("image", image.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows)),
                Arc::new(image),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("nested_metric", batch)
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["image.embedding"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1)),
            )
            .execute()
            .await
            .unwrap();

        let ordinary = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .column("image.embedding")
            .distance_type(crate::DistanceType::Cosine)
            .limit(2)
            .execute()
            .await
            .expect("an ordinary nested mismatch falls back to an exact scan")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            ordinary.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2
        );

        let Err(mismatch) = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .column("image.embedding")
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(2)
            .execute()
            .await
        else {
            panic!("nested L2 index should reject fast_search cosine");
        };
        match &mismatch {
            Error::InvalidInput { message } => {
                assert!(message.contains("image.embedding"), "{message}");
                assert!(message.contains("cosine"), "{message}");
                assert!(message.contains("fast_search"), "{message}");
                assert!(!message.contains("bypass"), "{message}");
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_rte_mismatched_distance_type_still_searches() {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;
        use crate::table::RTEWriteSpec;

        let table = vector_table("rte_metric").await;
        let query = [1.0_f32, 0.0, 0.0, 0.0];
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1)),
            )
            .name("vector_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .enable_rte(
                RTEWriteSpec::unsharded().with_maintained_indexes(vec!["vector_idx".to_string()]),
            )
            .await
            .unwrap();

        let batches = table
            .vector_search(query.as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .limit(2)
            .execute()
            .await
            .expect("RTE flat-scans a metric the base index was not trained with")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(rows, 2);
    }

    #[tokio::test]
    async fn test_pre_bitmap_index_accepts_matching_distance_type() {
        use crate::connect;
        use arrow_schema::{Field, Schema};
        use lance::index::DatasetIndexExt;
        use lance::index::vector::{IndexFileVersion, VectorIndexParams};
        use lance_index::IndexType;
        use lance_linalg::distance::MetricType;
        use lance_table::io::commit::write_manifest_file_to_path;
        use object_store::ObjectStoreExt;

        const INDEX_NAME: &str = "legacy_ivf_pq";
        const ROWS: i32 = 128;
        const DIMENSION: i32 = 8;

        let tmpdir = tempfile::tempdir().unwrap();
        let conn = connect(tmpdir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let vectors = FixedSizeListArray::try_new_from_values(
            Float32Array::from_iter_values((0..ROWS).flat_map(|row| {
                (0..DIMENSION).map(move |offset| (row * DIMENSION + offset) as f32)
            })),
            DIMENSION,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            vectors.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(vectors)]).unwrap();
        let table = conn
            .create_table("pre_bitmap_metric", batch.clone())
            .execute()
            .await
            .unwrap();
        table.add(batch.clone()).execute().await.unwrap();
        table.add(batch).execute().await.unwrap();
        let mut dataset = (*table.dataset().unwrap().get().await.unwrap()).clone();
        let mut params = VectorIndexParams::ivf_pq(1, 8, 1, MetricType::L2, 10);
        params.version(IndexFileVersion::Legacy);
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some(INDEX_NAME.to_string()),
                &params,
                true,
            )
            .await
            .unwrap();
        table.dataset().unwrap().update(dataset);

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut segments = dataset.load_indices_by_name(INDEX_NAME).await.unwrap();
        assert_eq!(segments.len(), 1);
        let mut legacy = segments.pop().unwrap();
        legacy.fragment_bitmap = None;
        let object_store = dataset.object_store(None).await.unwrap();
        let mut manifest = dataset.manifest().clone();
        manifest.index_section = None;
        manifest.transaction_section = None;
        let manifest_path = dataset.manifest_location().path.clone();
        object_store.inner.delete(&manifest_path).await.unwrap();
        write_manifest_file_to_path(
            object_store.as_ref(),
            &mut manifest,
            Some(vec![legacy]),
            &manifest_path,
            None,
        )
        .await
        .unwrap();
        let legacy_dataset = lance::Dataset::open(dataset.uri()).await.unwrap();
        table.dataset().unwrap().update(legacy_dataset);

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let Err(description_error) = dataset.describe_indices(None).await else {
            panic!("legacy coverage should be unknown to index descriptions");
        };
        assert!(
            description_error
                .to_string()
                .contains("Fragment bitmap is required"),
            "the regression needs an index description that cannot be built, got {description_error}"
        );

        let query = vec![0.0_f32; DIMENSION as usize];
        // fast_search uses the index without asking for unindexed fragments.
        // A missing bitmap is full coverage there; a normal scan asks
        // `unindexed_fragments`, which rejects this legacy shape.
        let matched = table
            .vector_search(query.as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::L2)
            .fast_search()
            .limit(2)
            .execute()
            .await
            .expect("a matching distance type must still search a pre-bitmap index")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let rows = matched.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(rows, 2);

        let Err(mismatch) = table
            .vector_search(query.as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(2)
            .execute()
            .await
        else {
            panic!(
                "a missing bitmap is full coverage, so fast_search cosine must not use the L2 index"
            );
        };
        match &mismatch {
            Error::InvalidInput { message } => {
                assert!(message.contains("l2"), "{message}");
                assert!(message.contains("cosine"), "{message}");
                assert!(message.contains("fast_search"), "{message}");
                assert!(!message.contains("bypass"), "{message}");
            }
            other => panic!("expected a metric mismatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_legacy_carried_fields_cosine_index_remains_usable() {
        use crate::connect;
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;
        use arrow_schema::Field;
        use lance::index::DatasetIndexExt;
        use lance_table::io::commit::write_manifest_file_to_path;
        use object_store::ObjectStoreExt;

        let dir = tempfile::tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let vectors = fixed_size_list_array(
            (0..8)
                .flat_map(|i| [i as f32 + 1.0, 0.0, 0.0, 0.0])
                .collect(),
            4,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("vector", vectors.data_type().clone(), false),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(0..8)) as ArrayRef,
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let table = conn.create_table("vectors", batch).execute().await.unwrap();
        for (name, metric) in [
            ("l2", crate::DistanceType::L2),
            ("cosine", crate::DistanceType::Cosine),
        ] {
            table
                .create_index(
                    &["vector"],
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default()
                            .distance_type(metric)
                            .num_partitions(1),
                    ),
                )
                .name(name.to_string())
                .replace(false)
                .execute()
                .await
                .unwrap();
        }

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut indices = dataset.load_indices().await.unwrap().as_ref().clone();
        let id = dataset.schema().field_id("id").unwrap();
        let cosine = indices
            .iter_mut()
            .find(|index| index.name == "cosine")
            .unwrap();
        // Older writers can keep carried columns in `fields` and drop
        // `covering_fields`. `keyed_field()` then returns None, while Lance
        // still selects the index from `fields[0]`.
        cosine.fields.push(id);
        cosine.covering_fields.clear();
        let store = dataset.object_store(None).await.unwrap();
        let mut manifest = dataset.manifest().clone();
        manifest.index_section = None;
        manifest.transaction_section = None;
        let path = dataset.manifest_location().path.clone();
        store.inner.delete(&path).await.unwrap();
        write_manifest_file_to_path(store.as_ref(), &mut manifest, Some(indices), &path, None)
            .await
            .unwrap();
        let reopened = lance::Dataset::open(dataset.uri()).await.unwrap();
        table.dataset().unwrap().update(reopened);

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let query = Float32Array::from(vec![1.0, 0.0, 0.0, 0.0]);
        let mut scanner = dataset.scan();
        scanner.nearest("vector", &query, 2).unwrap();
        scanner.distance_metric(lance_linalg::distance::DistanceType::Cosine);
        scanner.fast_search();
        let plan = scanner.create_plan().await.unwrap();
        let batches = super::execute_plan(plan, Default::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            2,
            "Lance must still search the cosine index"
        );

        let stream = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(2)
            .execute()
            .await
            .expect("the cosine index remains searchable when fields[0] is the vector column");
        let rows = stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 2);
    }

    async fn rewrite_manifest(
        table: &crate::Table,
        indices: Vec<lance_table::format::IndexMetadata>,
    ) {
        use lance_table::io::commit::write_manifest_file_to_path;
        use object_store::ObjectStoreExt;

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let store = dataset.object_store(None).await.unwrap();
        let mut manifest = dataset.manifest().clone();
        manifest.index_section = None;
        manifest.transaction_section = None;
        let path = dataset.manifest_location().path.clone();
        store.inner.delete(&path).await.unwrap();
        write_manifest_file_to_path(store.as_ref(), &mut manifest, Some(indices), &path, None)
            .await
            .unwrap();
        let reopened = lance::Dataset::open(dataset.uri()).await.unwrap();
        table.dataset().unwrap().update(reopened);
    }

    fn empty_coverage(index: &mut lance_table::format::IndexMetadata) {
        index.fragment_bitmap = Some(roaring::RoaringBitmap::new());
    }

    async fn l2_beside_cosine(path: &str, name: &str) -> crate::Table {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;

        let table = distinct_metric_table(path, name).await;
        for (index_name, metric) in [
            ("l2", crate::DistanceType::L2),
            ("cosine", crate::DistanceType::Cosine),
        ] {
            table
                .create_index(
                    &["vector"],
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default()
                            .distance_type(metric)
                            .num_partitions(1),
                    ),
                )
                .name(index_name.to_string())
                .replace(false)
                .execute()
                .await
                .unwrap();
        }
        table
    }

    #[tokio::test]
    async fn test_open_fallback_reads_legacy_cosine_metric() {
        use lance::index::DatasetIndexExt;

        let dir = tempfile::tempdir().unwrap();
        let table = l2_beside_cosine(dir.path().to_str().unwrap(), "open_fallback").await;
        let dataset = table.dataset().unwrap().get().await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let cosine = indices
            .iter()
            .find(|index| index.name == "cosine")
            .unwrap()
            .clone();
        let l2 = indices
            .iter()
            .find(|index| index.name == "l2")
            .unwrap()
            .clone();
        // These listings never pass through `load_indices`, so inference cannot
        // refill them. The helper has to open the cosine segment.
        let mut missing_details = cosine.clone();
        missing_details.index_details = None;
        let mut empty_details = cosine.clone();
        let mut details = (**cosine.index_details.as_ref().unwrap()).clone();
        details.type_url = "/lance.index.pb.VectorIndexDetails".to_string();
        details.value.clear();
        empty_details.index_details = Some(Arc::new(details));

        let dataset = table.dataset().unwrap().get().await.unwrap();
        for legacy in [missing_details, empty_details] {
            assert!(
                super::metric_from_index_metadata(&legacy).is_none(),
                "the fixture must not already record a metric"
            );
            assert_eq!(
                super::fast_search_index_decision(
                    &dataset,
                    "vector",
                    crate::DistanceType::Cosine,
                    &[l2.clone(), legacy],
                )
                .await
                .unwrap(),
                Some(crate::DistanceType::Cosine),
                "opening the legacy cosine segment must beat the modern L2 index"
            );
        }
    }

    #[tokio::test]
    async fn test_fast_search_legacy_cosine_beside_modern_l2() {
        use lance::index::DatasetIndexExt;

        let dir = tempfile::tempdir().unwrap();
        let table = l2_beside_cosine(dir.path().to_str().unwrap(), "mixed_legacy").await;
        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut stored = dataset.load_indices().await.unwrap().as_ref().clone();
        // The real legacy shape is `index_details: None`. `load_indices`
        // infers cosine from the index file; fast_search must use that index.
        stored
            .iter_mut()
            .find(|index| index.name == "cosine")
            .unwrap()
            .index_details = None;
        rewrite_manifest(&table, stored).await;

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let loaded = dataset.load_indices().await.unwrap();
        let cosine = loaded.iter().find(|index| index.name == "cosine").unwrap();
        assert_eq!(
            super::metric_from_index_metadata(cosine),
            Some(crate::DistanceType::Cosine),
            "inference should restore the cosine metric, got {:?}",
            cosine
                .index_details
                .as_ref()
                .map(|details| details.type_url.as_str())
        );
        // fast_search returns rows only through an index. A flat fallback is
        // empty, and the L2 neighbor is id 0.
        assert_eq!(
            top_id(&table, Some(crate::DistanceType::Cosine), true).await,
            1
        );
    }

    #[tokio::test]
    async fn test_fast_search_stops_at_first_uncovered_same_metric_index() {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;
        use lance::index::DatasetIndexExt;

        let dir = tempfile::tempdir().unwrap();
        let table = distinct_metric_table(dir.path().to_str().unwrap(), "first_uncovered").await;
        for name in ["cosine_empty", "cosine_full"] {
            table
                .create_index(
                    &["vector"],
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default()
                            .distance_type(crate::DistanceType::Cosine)
                            .num_partitions(1),
                    ),
                )
                .name(name.to_string())
                .replace(false)
                .execute()
                .await
                .unwrap();
        }

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut indices = dataset.load_indices().await.unwrap().as_ref().clone();
        indices.sort_by_key(|index| match index.name.as_str() {
            "cosine_empty" => 0,
            "cosine_full" => 1,
            _ => 2,
        });
        empty_coverage(
            indices
                .iter_mut()
                .find(|index| index.name == "cosine_empty")
                .unwrap(),
        );
        rewrite_manifest(&table, indices).await;

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert_eq!(indices[0].name, "cosine_empty");
        assert!(!super::segment_covers_rows(&dataset, &indices[0]));
        assert!(super::index_name_covers(
            &dataset,
            indices.as_ref(),
            "cosine_full"
        ));

        // Lance selects cosine_empty and then brute-forces. fast_search is empty.
        // A later covering cosine index must not make this succeed.
        let Err(error) = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(1)
            .execute()
            .await
        else {
            panic!("fast_search must not use the later covering cosine index");
        };
        match &error {
            Error::InvalidInput { message } => {
                assert!(message.contains("covers no rows"), "{message}");
                assert!(message.contains("fast_search"), "{message}");
                assert!(!message.contains("bypass"), "{message}");
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }
        assert_eq!(
            top_id(&table, Some(crate::DistanceType::Cosine), false).await,
            1,
            "an ordinary query still exact-scans"
        );

        // The covering index is first, so Lance serves it.
        let mut swapped = indices.as_ref().clone();
        swapped.sort_by_key(|index| match index.name.as_str() {
            "cosine_full" => 0,
            "cosine_empty" => 1,
            _ => 2,
        });
        rewrite_manifest(&table, swapped).await;
        assert_eq!(
            top_id(&table, Some(crate::DistanceType::Cosine), true).await,
            1
        );
    }

    #[tokio::test]
    async fn test_fast_search_noncovering_match_beside_other_metric_errors() {
        use crate::index::Index;
        use crate::index::vector::IvfFlatIndexBuilder;
        use lance::index::DatasetIndexExt;

        let dir = tempfile::tempdir().unwrap();
        let table = distinct_metric_table(dir.path().to_str().unwrap(), "noncovering_match").await;
        for (name, metric) in [
            ("cosine", crate::DistanceType::Cosine),
            ("l2", crate::DistanceType::L2),
        ] {
            table
                .create_index(
                    &["vector"],
                    Index::IvfFlat(
                        IvfFlatIndexBuilder::default()
                            .distance_type(metric)
                            .num_partitions(1),
                    ),
                )
                .name(name.to_string())
                .replace(false)
                .execute()
                .await
                .unwrap();
        }

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut indices = dataset.load_indices().await.unwrap().as_ref().clone();
        empty_coverage(
            indices
                .iter_mut()
                .find(|index| index.name == "cosine")
                .unwrap(),
        );
        indices.sort_by_key(|index| match index.name.as_str() {
            "cosine" => 0,
            "l2" => 1,
            _ => 2,
        });
        rewrite_manifest(&table, indices).await;

        let Err(error) = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(1)
            .execute()
            .await
        else {
            panic!("a non-covering cosine index must not fall through to the L2 index");
        };
        match &error {
            Error::InvalidInput { message } => {
                assert!(message.contains("covers no rows"), "{message}");
                assert!(message.contains("fast_search"), "{message}");
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }

        // The covering L2 index is first. Lance skips it and still stops on the
        // non-covering cosine index.
        let dataset = table.dataset().unwrap().get().await.unwrap();
        let mut swapped = dataset.load_indices().await.unwrap().as_ref().clone();
        swapped.sort_by_key(|index| match index.name.as_str() {
            "l2" => 0,
            "cosine" => 1,
            _ => 2,
        });
        rewrite_manifest(&table, swapped).await;
        let Err(error) = table
            .vector_search([1.0_f32, 0.0, 0.0, 0.0].as_slice())
            .unwrap()
            .distance_type(crate::DistanceType::Cosine)
            .fast_search()
            .limit(1)
            .execute()
            .await
        else {
            panic!("Lance still selects the non-covering cosine index after L2");
        };
        assert!(
            matches!(&error, Error::InvalidInput { message } if message.contains("covers no rows")),
            "expected uncovered match, got {error:?}"
        );
    }

    #[rstest::rstest]
    #[case::modern(false)]
    #[case::legacy(true)]
    #[tokio::test]
    async fn test_cosine_pq_distance_uses_public_cosine_scale(#[case] legacy: bool) {
        use arrow_array::{
            Float32Array, Int32Array, RecordBatch,
            types::{Float32Type, Int32Type},
        };
        use arrow_schema::{DataType, Field, Schema};

        use crate::connect;
        use crate::index::{Index, vector::IvfPqIndexBuilder};
        use lance::index::{
            DatasetIndexExt, DatasetIndexInternalExt,
            vector::{IndexFileVersion, VectorIndexParams},
        };
        use lance_index::metrics::NoOpMetricsCollector;

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
        if legacy {
            let mut dataset = (*table.dataset().unwrap().get().await.unwrap()).clone();
            dataset
                .create_index(
                    &["vector"],
                    IndexType::Vector,
                    None,
                    VectorIndexParams::ivf_pq(1, 8, 1, LanceDistanceType::Cosine, 10)
                        .version(IndexFileVersion::Legacy),
                    true,
                )
                .await
                .unwrap();
            table.dataset().unwrap().update(dataset);
        } else {
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
        }

        let expected_index_version = if legacy { "Legacy" } else { "V3" };
        let index_file_version = {
            let dataset = table.dataset().unwrap().get().await.unwrap();
            let metadata = dataset
                .load_indices()
                .await
                .unwrap()
                .first()
                .unwrap()
                .clone();
            let index = dataset
                .open_vector_index("vector", &metadata.uuid, &NoOpMetricsCollector)
                .await
                .unwrap();
            index.statistics().unwrap()["index_file_version"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(index_file_version, expected_index_version);

        let plan = table
            .dataset()
            .unwrap()
            .get()
            .await
            .unwrap()
            .scan()
            .nearest("vector", &Float32Array::from(query_vector.clone()), 5)
            .unwrap()
            .create_plan()
            .await
            .unwrap();
        let mut ann_plans = Vec::new();
        find_ann_plans(plan.as_ref(), &mut ann_plans);
        assert!(!ann_plans.is_empty());

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
        assert_eq!(approximate_distances.len(), 5);
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

        if !legacy {
            fn batch_distances_by_query(
                batches: &[RecordBatch],
                query_count: usize,
            ) -> Vec<Vec<f32>> {
                let mut distances = vec![Vec::new(); query_count];
                for batch in batches {
                    let query_index_column = batch["query_index"].as_primitive::<Int32Type>();
                    let distance_column = batch[DIST_COL].as_primitive::<Float32Type>();
                    for row in 0..batch.num_rows() {
                        let query_index = query_index_column.value(row) as usize;
                        distances[query_index].push(distance_column.value(row));
                    }
                }
                distances
            }

            fn sorted(mut values: Vec<f32>) -> Vec<f32> {
                values.sort_by(|left, right| left.partial_cmp(right).unwrap());
                values
            }

            let batch_query = table
                .vector_search(query_vector.as_slice())
                .unwrap()
                .add_query_vector(query_vector.as_slice())
                .unwrap()
                .nprobes(1)
                .unwrap()
                .limit(5);
            let batch_plan = batch_query.explain_plan(false).await.unwrap();
            assert!(batch_plan.contains("ANNIvfBatch"), "{batch_plan}");
            let batched = batch_query
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let batched_distances = batch_distances_by_query(&batched, 2);
            let expected_distances = sorted(approximate_distances.clone());
            for (query_index, actual) in batched_distances.into_iter().enumerate() {
                assert_eq!(
                    actual.len(),
                    expected_distances.len(),
                    "query {query_index} returned the wrong number of rows"
                );
                for (actual, expected) in sorted(actual).into_iter().zip(&expected_distances) {
                    assert!(
                        (actual - expected).abs() < 1e-5,
                        "batch distance {actual} for query {query_index} must use single-query public scale {expected}"
                    );
                }
            }

            let batched_ranged = table
                .vector_search(query_vector.as_slice())
                .unwrap()
                .add_query_vector(query_vector.as_slice())
                .unwrap()
                .nprobes(1)
                .unwrap()
                .limit(1)
                .distance_range(None, Some(nearest + 1e-5))
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let batched_ranged_distances = batch_distances_by_query(&batched_ranged, 2);
            for (query_index, query_distances) in batched_ranged_distances.into_iter().enumerate() {
                assert_eq!(
                    query_distances.len(),
                    1,
                    "public cosine range must return one hit for query {query_index}"
                );
                assert!(
                    (query_distances[0] - nearest).abs() < 1e-5,
                    "ranged batch distance {} for query {query_index} must match {nearest}",
                    query_distances[0]
                );
            }
        }

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

    #[tokio::test]
    async fn test_scan_in_order_local_multi_fragment_contract() {
        use crate::connect;
        use crate::query::ColumnOrdering;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = |ids: Vec<i32>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
        };

        let table = conn
            .create_table("scan_in_order_contract", batch(vec![0, 1]))
            .execute()
            .await
            .unwrap();
        table.add(batch(vec![2, 3])).execute().await.unwrap();
        table.add(batch(vec![4, 5])).execute().await.unwrap();

        let fragments = table
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .get_fragments()
            .len();
        assert_eq!(fragments, 3, "the test needs real multi-fragment data");

        // An ordered scan must not introduce a global sort.
        let plan = table
            .query()
            .select(Select::columns(&["id"]))
            .scan_in_order(true)
            .explain_plan(false)
            .await
            .unwrap();
        assert!(
            !plan.contains("SortExec"),
            "scan_in_order(true) must not add a sort, got:\n{plan}"
        );

        // Pin one version so repeated executions describe the same manifest.
        let version = table.version().await.unwrap();
        table.checkout(version).await.unwrap();
        for _ in 0..2 {
            let batches = table
                .query()
                .select(Select::columns(&["id"]))
                .scan_in_order(true)
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(collect_i32(&batches), vec![0, 1, 2, 3, 4, 5]);
        }

        // The default keeps the ordered local behavior.
        let batches = table
            .query()
            .select(Select::columns(&["id"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_i32(&batches), vec![0, 1, 2, 3, 4, 5]);

        // Allowing unordered scans changes no membership.
        let mut unordered = collect_i32(
            &table
                .query()
                .select(Select::columns(&["id"]))
                .scan_in_order(false)
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
        );
        unordered.sort();
        assert_eq!(unordered, vec![0, 1, 2, 3, 4, 5]);

        // An explicit sort still wins when storage order is not required.
        let batches = table
            .query()
            .select(Select::columns(&["id"]))
            .scan_in_order(false)
            .order_by(Some(vec![ColumnOrdering::desc_nulls_last(
                "id".to_string(),
            )]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_i32(&batches), vec![5, 4, 3, 2, 1, 0]);

        // Deletions keep manifest order, and small output batches plus
        // limit/offset must not disturb it.
        table.checkout_latest().await.unwrap();
        table.delete("id = 3").await.unwrap();
        let version = table.version().await.unwrap();
        table.checkout(version).await.unwrap();
        let batches = table
            .query()
            .select(Select::columns(&["id"]))
            .scan_in_order(true)
            .offset(1)
            .limit(2)
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 1,
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            batches.iter().all(|batch| batch.num_rows() <= 1),
            "max_batch_length must stay bounded"
        );
        assert_eq!(collect_i32(&batches), vec![1, 2]);
    }

    #[tokio::test]
    async fn test_scan_in_order_rejects_ranked_and_sorted_queries() {
        use crate::connect;
        use crate::query::ColumnOrdering;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let vector = fixed_size_list_array(vec![0.0, 0.0, 1.0, 0.0, 2.0, 0.0], 2);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vector.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![0, 1, 2])), Arc::new(vector)],
        )
        .unwrap();
        let table = conn
            .create_table("scan_in_order_conflicts", batch)
            .execute()
            .await
            .unwrap();

        let err = table
            .query()
            .scan_in_order(true)
            .order_by(Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]))
            .execute()
            .await
            .err()
            .expect("ranked and sorted scans must be rejected");
        assert!(
            matches!(&err, Error::InvalidInput { message }
                if message.contains("nonempty order_by")),
            "unexpected error: {err}"
        );

        let err = table
            .query()
            .scan_in_order(true)
            .nearest_to(vec![0.0f32, 0.0])
            .unwrap()
            .execute()
            .await
            .err()
            .expect("ranked scans must be rejected");
        assert!(
            matches!(&err, Error::InvalidInput { message }
                if message.contains("ranked results")),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_scan_in_order_with_namespace_pushdown_runs_locally() {
        use crate::connect;
        use crate::query::ColumnOrdering;
        use arrow_schema::{DataType, Field, Schema};

        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = |ids: Vec<i32>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
        };
        // Insertion order differs from sorted order so a dropped scan order or
        // an accidental sort changes the result.
        let table = conn
            .create_table("scan_in_order_namespace", batch(vec![3, 1]))
            .execute()
            .await
            .unwrap();
        table.add(batch(vec![2])).execute().await.unwrap();

        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);

        let query = AnyQuery::Query(QueryRequest {
            filter: Some(QueryFilter::Sql("id >= 1".to_string())),
            select: Select::columns(&["id"]),
            limit: Some(2),
            scan_in_order: Some(true),
            ..Default::default()
        });
        assert!(requires_local_namespace_execution(&query));

        let batches = execute_query(&native_table, &query, QueryExecutionOptions::default())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_i32(&batches), vec![3, 1]);
        assert_eq!(namespace_client.query_table_calls.load(Ordering::SeqCst), 0);

        // Allowing unordered scans does not require local execution.
        let allow_unordered = AnyQuery::Query(QueryRequest {
            scan_in_order: Some(false),
            ..Default::default()
        });
        assert!(!requires_local_namespace_execution(&allow_unordered));

        // An explicit sort is already local because the namespace request has
        // no order_by field.
        let sorted = AnyQuery::Query(QueryRequest {
            order_by: Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
            ..Default::default()
        });
        assert!(requires_local_namespace_execution(&sorted));
    }

    #[tokio::test]
    async fn test_scan_in_order_true_rejected_on_rte() {
        use crate::connect;
        use crate::table::RTEWriteSpec;
        use arrow_schema::{DataType, Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0, 1]))]).unwrap();
        let table = conn
            .create_table("scan_in_order_rte", batch)
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table.enable_rte(RTEWriteSpec::unsharded()).await.unwrap();

        let err = table
            .query()
            .scan_in_order(true)
            .execute()
            .await
            .err()
            .expect("RTE scans cannot guarantee manifest order");
        assert!(
            matches!(&err, Error::NotSupported { message }
                if message.contains("scan_in_order(true)") && message.contains("use_rte(false)")),
            "unexpected error: {err}"
        );

        // The base table can still be read in storage order explicitly.
        let batches = table
            .query()
            .scan_in_order(true)
            .use_rte(false)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_i32(&batches), vec![0, 1]);
    }
}
