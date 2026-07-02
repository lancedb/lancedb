// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! MemWAL LSM read path.
//!
//! When a table has an LSM write spec installed (see [`set_lsm_write_spec`]),
//! reads are routed through Lance's [`LsmScanner`] / LSM planners instead of the
//! plain base-table scan unless the query sets
//! [`use_lsm(false)`](crate::query::QueryBase::use_lsm). This makes data
//! written via the LSM `merge_insert` path — which lives in the active/frozen
//! in-memory memtables and the flushed (L0) generations until an external
//! compaction merges it into the base table — visible to queries, deduplicated by
//! primary key (newest generation wins).
//!
//! Three query shapes are supported, mirroring the standard scan: a plain scan
//! (filter / projection / limit), full-text search, and vector (ANN) search.
//! Shapes the LSM path cannot honor are rejected with [`Error::NotSupported`];
//! the caller must set `use_lsm(false)` to run those against the base table.
//!
//! [`set_lsm_write_spec`]: crate::Table::set_lsm_write_spec

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{AsArray, FixedSizeListBuilder, Float32Builder};
use arrow::datatypes::Float32Type;
use arrow_array::{Array, FixedSizeListArray};
use arrow_schema::{DataType, Schema as ArrowSchema};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::limit::GlobalLimitExec;
use lance::Dataset;
use lance::dataset::mem_wal::scanner::{
    InMemoryMemTables, LsmDataSourceCollector, LsmVectorSearchPlanner,
};
use lance::dataset::mem_wal::{
    DatasetMemWalExt, LsmScanner, ShardManifestStore, ShardSnapshot, ShardWriterConfig,
};
use lance_index::mem_wal::ShardManifest;
use uuid::Uuid;

use super::NativeTable;
use crate::DistanceType;
use crate::error::{Error, Result};
use crate::query::{DEFAULT_TOP_K, QueryFilter, Select, VectorQueryRequest};
use crate::utils::default_vector_column;

/// Build the LSM read plan for a MemWAL-routed query.
///
/// The caller guarantees `ds_ref` carries a MemWAL write spec (routing is decided
/// in [`create_plan`](super::create_plan)). Errors with [`Error::NotSupported`]
/// for query shapes the LSM scanner cannot honor — the caller must set
/// `use_lsm(false)` to run those against the base table.
pub(super) async fn create_lsm_plan(
    table: &NativeTable,
    ds_ref: Arc<Dataset>,
    query: VectorQueryRequest,
) -> Result<Arc<dyn ExecutionPlan>> {
    reject_unsupported(&query)?;

    let pk_columns = pk_columns(&ds_ref)?;
    let (snapshots, in_memory) = build_read_context(table, &ds_ref).await?;

    let limit = query.base.limit;
    let offset = query.base.offset;

    if !query.query_vector.is_empty() {
        return vector_plan(
            &ds_ref, &query, pk_columns, snapshots, in_memory, limit, offset,
        )
        .await;
    }

    if let Some(fts) = &query.base.full_text_search {
        return fts_plan(
            &ds_ref,
            fts.clone(),
            &query,
            pk_columns,
            snapshots,
            in_memory,
            limit,
            offset,
        )
        .await;
    }

    plain_plan(
        &ds_ref, &query, pk_columns, snapshots, in_memory, limit, offset,
    )
    .await
}

/// Reject query shapes the LSM read path does not implement. On a MemWAL table
/// reads route through the LSM scanner by default, so an unsupported shape is a
/// hard error rather than a silent fallback to the base-only scan — which would
/// exclude un-compacted MemWAL data. The caller must set `use_lsm(false)` to
/// run these against the base table, accepting that the results omit un-compacted
/// MemWAL data.
fn reject_unsupported(query: &VectorQueryRequest) -> Result<()> {
    let unsupported = |what: &str| {
        Err(Error::NotSupported {
            message: format!(
                "the MemWAL LSM scanner does not support {what}; set use_lsm(false) to read the base table only (results will exclude un-compacted MemWAL data)"
            ),
        })
    };
    if query.query_vector.len() > 1 {
        return unsupported("multiple query vectors");
    }
    if !query.query_vector.is_empty() && query.base.full_text_search.is_some() {
        return unsupported("hybrid (vector + full-text) search");
    }
    if query.base.with_row_id {
        return unsupported("with_row_id (the LSM scanner exposes _rowaddr, not a stable _rowid)");
    }
    if query.base.reranker.is_some() {
        return unsupported("reranking / hybrid search");
    }
    if query.base.order_by.is_some() {
        return unsupported("order_by");
    }
    match &query.base.select {
        Select::All | Select::Columns(_) => {}
        Select::Dynamic(_) | Select::Expr(_) => return unsupported("dynamic column projection"),
    }
    if let Some(QueryFilter::Substrait(_)) = &query.base.filter {
        return unsupported("Substrait filters");
    }
    Ok(())
}

/// Primary-key column names from the dataset's unenforced primary key.
fn pk_columns(dataset: &Dataset) -> Result<Vec<String>> {
    let pk: Vec<String> = dataset
        .schema()
        .unenforced_primary_key()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    if pk.is_empty() {
        return Err(Error::Runtime {
            message:
                "the MemWAL LSM scanner requires an unenforced primary key, but the table has none"
                    .to_string(),
        });
    }
    Ok(pk)
}

/// Assemble the per-shard snapshots (flushed generations) and the in-memory
/// memtables (active + frozen) for the table.
///
/// Snapshots for all shards come from their on-disk manifests; for the shard
/// with a live cached `ShardWriter` (this session's in-flight writes) the
/// writer's authoritative in-memory manifest and memtables override the
/// on-disk view so a read sees data not yet flushed.
async fn build_read_context(
    table: &NativeTable,
    dataset: &Dataset,
) -> Result<(Vec<ShardSnapshot>, HashMap<Uuid, InMemoryMemTables>)> {
    let shard_ids = dataset.list_mem_wal_latest_shard_ids().await?;
    // Use the dataset's own object store (not `ObjectStore::from_uri`, which
    // builds a fresh registry and would miss `memory://` and custom-registered
    // stores). The base path matches `list_mem_wal_latest_shard_ids`.
    let store = dataset.object_store(None).await?;
    let base_path = dataset.branch_location().path;
    let scan_batch_size = ShardWriterConfig::default().manifest_scan_batch_size;

    let mut snapshots: Vec<ShardSnapshot> = Vec::new();
    for shard_id in shard_ids {
        let manifest_store =
            ShardManifestStore::new(store.clone(), &base_path, shard_id, scan_batch_size);
        if let Some(manifest) = manifest_store.read_latest().await? {
            snapshots.push(snapshot_from_manifest(shard_id, &manifest));
        }
    }

    // Override the active shard with the cached writer's in-memory view.
    let mut in_memory: HashMap<Uuid, InMemoryMemTables> = HashMap::new();
    if let Some((shard_id, manifest, memtables)) =
        table.dataset.shard_writer().read_snapshot().await?
    {
        if let Some(manifest) = manifest {
            let snapshot = snapshot_from_manifest(shard_id, &manifest);
            match snapshots.iter_mut().find(|s| s.shard_id == shard_id) {
                Some(existing) => *existing = snapshot,
                None => snapshots.push(snapshot),
            }
        }
        if let Some(memtables) = memtables {
            in_memory.insert(shard_id, memtables);
        }
    }

    Ok((snapshots, in_memory))
}

/// Convert a shard manifest into a read snapshot (current + flushed generations).
fn snapshot_from_manifest(shard_id: Uuid, manifest: &ShardManifest) -> ShardSnapshot {
    let mut snapshot = ShardSnapshot::new(shard_id)
        .with_spec_id(manifest.shard_spec_id)
        .with_current_generation(manifest.current_generation);
    for flushed in &manifest.flushed_generations {
        snapshot = snapshot.with_flushed_generation(flushed.generation, flushed.path.clone());
    }
    snapshot
}

/// Apply optional skip/fetch to a plan when not already applied by the planner.
fn apply_limit_offset(
    plan: Arc<dyn ExecutionPlan>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    let skip = offset.unwrap_or(0);
    if skip == 0 && limit.is_none() {
        return plan;
    }
    Arc::new(GlobalLimitExec::new(plan, skip, limit))
}

/// Columns selected by the query, if an explicit projection was requested.
fn selected_columns(query: &VectorQueryRequest) -> Option<Vec<String>> {
    match &query.base.select {
        Select::Columns(columns) => Some(columns.clone()),
        _ => None,
    }
}

/// Build a base `LsmScanner` configured with sources, filter, and projection.
fn base_scanner(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
) -> Result<LsmScanner> {
    let mut scanner = LsmScanner::new(Arc::new(dataset.clone()), snapshots, pk_columns);
    for (shard_id, memtables) in in_memory {
        scanner = scanner.with_in_memory_memtables(shard_id, memtables);
    }
    if let Some(columns) = selected_columns(query) {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        scanner = scanner.project(&refs);
    }
    if let Some(filter) = &query.base.filter {
        scanner = match filter {
            QueryFilter::Sql(sql) => scanner.filter(sql)?,
            QueryFilter::Datafusion(expr) => scanner.filter_expr(expr.clone()),
            QueryFilter::Substrait(_) => {
                return Err(Error::NotSupported {
                    message: "the MemWAL LSM scanner does not support Substrait filters; set use_lsm(false) to read the base table only".to_string(),
                });
            }
        };
    }
    Ok(scanner)
}

/// Plain scan: filter / projection / limit over base ∪ flushed ∪ in-memory.
async fn plain_plan(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut scanner = base_scanner(dataset, query, pk_columns, snapshots, in_memory)?;
    // `LsmScanner::limit` applies both skip and fetch internally.
    if let Some(limit) = limit {
        scanner = scanner.limit(limit, offset);
    }
    let plan = scanner.create_plan().await?;
    // When only an offset (no limit) is set, fold it in here.
    if limit.is_none() && offset.unwrap_or(0) > 0 {
        return Ok(apply_limit_offset(plan, None, offset));
    }
    Ok(plan)
}

/// Full-text search over base ∪ flushed ∪ in-memory, merged by local BM25 score.
#[allow(clippy::too_many_arguments)]
async fn fts_plan(
    dataset: &Dataset,
    fts: lance_index::scalar::FullTextSearchQuery,
    query: &VectorQueryRequest,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let columns: Vec<String> = fts.columns().into_iter().collect();
    if columns.len() > 1 {
        return Err(Error::NotSupported {
            message: "the MemWAL LSM scanner full-text search supports a single column".to_string(),
        });
    }
    let column = columns
        .into_iter()
        .next()
        .ok_or_else(|| Error::NotSupported {
            message: "the MemWAL LSM scanner full-text search requires an explicit FTS column"
                .to_string(),
        })?;
    let scanner = base_scanner(dataset, query, pk_columns, snapshots, in_memory)?;
    let k = limit.unwrap_or(DEFAULT_TOP_K) + offset.unwrap_or(0);
    let plan = scanner.full_text_search(&column, fts, k).await?;
    Ok(apply_limit_offset(plan, limit, offset))
}

/// Vector (ANN) search over base ∪ flushed ∪ in-memory via the LSM vector planner.
#[allow(clippy::too_many_arguments)]
async fn vector_plan(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let query_vector = query
        .query_vector
        .first()
        .cloned()
        .ok_or_else(|| Error::InvalidInput {
            message: "vector search requires a query vector".to_string(),
        })?;

    let arrow_schema = ArrowSchema::from(dataset.schema());
    let column = match &query.column {
        Some(column) => column.clone(),
        None => default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?,
    };

    let distance_type = resolve_distance_type(dataset, query, &column).await?;
    let query_fsl = to_fixed_size_list(query_vector.as_ref(), &column, &arrow_schema)?;

    let dataset_arc = Arc::new(dataset.clone());
    let mut collector = LsmDataSourceCollector::new(dataset_arc.clone(), snapshots);
    for (shard_id, memtables) in in_memory {
        collector = collector.with_in_memory_memtables(shard_id, memtables);
    }

    let base_schema = Arc::new(arrow_schema);
    let planner = LsmVectorSearchPlanner::new(
        collector,
        pk_columns,
        base_schema,
        column,
        distance_type.into(),
    )
    .with_dataset(dataset_arc.clone())
    .with_session(dataset.session());

    let k = limit.unwrap_or(DEFAULT_TOP_K) + offset.unwrap_or(0);
    let nprobes = query.minimum_nprobes;
    let refine_base_table = query.refine_factor.is_some();
    let projection = selected_columns(query);
    let plan = planner
        .plan_search(
            &query_fsl,
            k,
            nprobes,
            projection.as_deref(),
            refine_base_table,
            1.0,
        )
        .await?;
    Ok(apply_limit_offset(plan, limit, offset))
}

/// Resolve the distance metric for the vector arm: the explicit query metric if
/// set, else the metric of the column's vector index, else L2.
async fn resolve_distance_type(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    column: &str,
) -> Result<DistanceType> {
    if let Some(dt) = query.distance_type {
        return Ok(dt);
    }
    // Inherit the column's vector-index metric so cross-source distances match
    // the metric the maintained memtable index was built with.
    use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
    use lance_index::metrics::NoOpMetricsCollector;
    let field = dataset.schema().field(column);
    if let Some(field) = field {
        let indices = dataset.load_indices().await?;
        for index in indices.iter() {
            if index.fields.contains(&field.id)
                && let Ok(vector_index) = dataset
                    .open_vector_index(column, &index.uuid.to_string(), &NoOpMetricsCollector)
                    .await
            {
                return Ok(vector_index.metric_type().into());
            }
        }
    }
    Ok(DistanceType::L2)
}

/// Wrap a query vector array into a single-row `FixedSizeListArray` of the
/// column's element type, as the LSM vector planner expects.
fn to_fixed_size_list(
    query_vector: &dyn Array,
    column: &str,
    schema: &ArrowSchema,
) -> Result<FixedSizeListArray> {
    // Determine whether the target column stores binary (uint8) vectors.
    let is_binary = matches!(
        schema.field_with_name(column).map(|f| f.data_type()),
        Ok(DataType::FixedSizeList(field, _)) if matches!(field.data_type(), DataType::UInt8)
    );

    if let Some(fsl) = query_vector.as_any().downcast_ref::<FixedSizeListArray>() {
        return Ok(fsl.clone());
    }

    let dim = query_vector.len() as i32;
    if is_binary {
        return Err(Error::NotSupported {
            message: "the MemWAL LSM scanner does not support binary (uint8) vector search; set use_lsm(false) to read the base table only".to_string(),
        });
    }
    let values = query_vector
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| {
            // Fall back to a cast attempt for non-f32 numeric inputs is overkill;
            // require f32 like the standard path's flat search.
            Error::InvalidInput {
                message: "query vector must be Float32".to_string(),
            }
        })?;
    let mut builder =
        FixedSizeListBuilder::with_capacity(Float32Builder::with_capacity(dim as usize), dim, 1);
    builder.values().append_slice(values.values());
    builder.append(true);
    Ok(builder.finish())
}
