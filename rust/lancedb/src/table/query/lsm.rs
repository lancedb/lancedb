// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! MemWAL LSM read path.
//!
//! When a table has an LSM write spec installed (see [`set_lsm_write_spec`]),
//! reads are routed through Lance's [`LsmScanner`] instead of the plain
//! base-table scan unless the query sets
//! [`use_lsm(false)`](crate::query::QueryBase::use_lsm). This makes data
//! written via the LSM `merge_insert` path — which lives in the active/frozen
//! in-memory memtables and the flushed SSTable generations until an external
//! compaction merges it into the base table — visible to queries, deduplicated by
//! primary key (newest generation wins).
//!
//! Three query shapes are supported, mirroring the standard scan: a plain scan
//! (filter / projection / limit), full-text search, and vector (ANN) search. All
//! three run through a single [`LsmScanner`], so a `where` filter is honored as a
//! prefilter uniformly — including for vector search, where `LsmScanner` threads
//! it into the vector planner's prefilter. Shapes the LSM path cannot honor are
//! rejected with [`Error::NotSupported`]; the caller must set `use_lsm(false)` to
//! run those against the base table.
//!
//! [`set_lsm_write_spec`]: crate::Table::set_lsm_write_spec

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::{DataType, Schema as ArrowSchema};
use datafusion_physical_plan::expressions::Column;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use lance::Dataset;
use lance::dataset::mem_wal::scanner::InMemoryMemTables;
use lance::dataset::mem_wal::{
    DatasetMemWalExt, LsmScanner, ShardManifestStore, ShardSnapshot, ShardWriterConfig,
};
use lance_index::mem_wal::{MemWalIndexDetails, ShardManifest};
use uuid::Uuid;

use super::NativeTable;
use crate::DistanceType;
use crate::error::{Error, Result};
use crate::query::{DEFAULT_TOP_K, QueryFilter, Select, VectorQueryRequest};
use crate::utils::default_vector_column;

/// Over-fetch factor for the LSM vector/FTS arms. With the default of `1.0` a
/// source blocked by cross-generation PK dedup fetches exactly `k` and can return
/// fewer than `k` live rows; upserts routinely create such blocked candidates, so
/// widen the per-source fetch to keep result pages filled.
const LSM_OVERFETCH_FACTOR: f64 = 2.0;

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

    // A time-traveled (checked-out) handle pins an older dataset version, but the
    // WAL manifests and cached writer expose current live state — mixing them would
    // surface WAL rows written after the requested version. `use_lsm(false)` reads
    // the base table at the pinned version.
    if table.dataset.time_travel_version().is_some() {
        return Err(Error::NotSupported {
            message: "the MemWAL LSM scanner cannot read from a time-traveled dataset version; set use_lsm(false) to read the base table at this version".to_string(),
        });
    }

    // Routing guarantees a write spec is installed (see `create_plan`).
    let details = ds_ref
        .mem_wal_index_details()
        .await?
        .ok_or_else(|| Error::Runtime {
            message: "the MemWAL LSM write spec disappeared during read planning".to_string(),
        })?;

    let pk_columns = pk_columns(&ds_ref)?;
    // The base index an indexed arm relies on may lag compaction; resolve it so the
    // snapshot retains SSTables the index has not yet caught up to.
    let arm_indexes = arm_maintained_index_names(&ds_ref, &query, &details).await?;
    let (snapshots, in_memory) = build_read_context(table, &ds_ref, &details, &arm_indexes).await?;

    let limit = query.base.limit;
    let offset = query.base.offset;

    let plan = if !query.query_vector.is_empty() {
        vector_plan(
            &ds_ref,
            &query,
            &details,
            pk_columns.clone(),
            snapshots,
            in_memory,
            limit,
            offset,
        )
        .await?
    } else if let Some(fts) = &query.base.full_text_search {
        fts_plan(
            &ds_ref,
            fts.clone(),
            &query,
            &details,
            pk_columns.clone(),
            snapshots,
            in_memory,
            limit,
            offset,
        )
        .await?
    } else {
        plain_plan(
            &ds_ref,
            &query,
            pk_columns.clone(),
            snapshots,
            in_memory,
            limit,
            offset,
        )
        .await?
    };

    // Lance appends the primary-key columns internally for dedup and keeps them in
    // the output; drop the ones the user did not request so the projection matches.
    restore_projection(plan, &query, &pk_columns)
}

/// Reject query shapes the LSM read path does not implement. On a MemWAL table
/// reads route through the LSM scanner by default, so an unsupported shape is a
/// hard error rather than a silent fallback to the base-only scan — which would
/// exclude un-compacted MemWAL data. The caller must set `use_lsm(false)` to
/// run these against the base table, accepting that the results omit un-compacted
/// MemWAL data.
///
/// A `where` filter is intentionally *not* rejected: every arm routes through
/// [`LsmScanner`], which applies it as a prefilter (see [`base_scanner`]).
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
    // Vector-only knobs the LSM scanner cannot honor. Both change results rather
    // than just recall, so error instead of silently ignoring them: distance_range
    // would return rows outside the bound, and use_index(false) asks for a
    // brute-force search the index-only base arm can't do. (ef / approx_mode /
    // maximum_nprobes are recall/speed knobs and are left to no-op — and
    // maximum_nprobes defaults to Some, so it cannot be rejected on presence.)
    if !query.query_vector.is_empty() {
        if query.lower_bound.is_some() || query.upper_bound.is_some() {
            return unsupported("distance_range on vector search");
        }
        if !query.use_index {
            return unsupported(
                "use_index(false) / brute-force vector search (the LSM base arm is index-only)",
            );
        }
    }
    // Postfilter changes result semantics for both vector and full-text search, and
    // the LSM scanner always prefilters — reject a requested postfilter for either.
    if (!query.query_vector.is_empty() || query.base.full_text_search.is_some())
        && !query.base.prefilter
    {
        return unsupported(
            "postfilter on vector or full-text search (the LSM scanner always prefilters)",
        );
    }
    match &query.base.select {
        Select::All | Select::Columns(_) => {}
        Select::Dynamic(_) | Select::Expr(_) => return unsupported("dynamic column projection"),
    }
    if let Some(QueryFilter::Substrait(_)) = &query.base.filter {
        return unsupported("Substrait filters");
    }
    // Take-by-row-id / row-offset queries carry a `_rowid` / `_rowoffset` filter,
    // columns the LSM scanner never exposes (only `_rowaddr`); reject with guidance
    // rather than failing deep in datafusion with a column-not-found error.
    if let Some(QueryFilter::Datafusion(expr)) = &query.base.filter
        && expr
            .column_refs()
            .iter()
            .any(|c| c.name == "_rowid" || c.name == "_rowoffset")
    {
        return unsupported(
            "take by row id or row offset (the LSM scanner has no stable _rowid / _rowoffset)",
        );
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
        return Err(Error::InvalidInput {
            message:
                "the MemWAL LSM scanner requires an unenforced primary key, but the table has none"
                    .to_string(),
        });
    }
    Ok(pk)
}

/// Per-shard SSTable exclusion watermark: the generation at or below which
/// SSTables are safe to drop for this query.
///
/// A generation is droppable only once it is compacted into the base table AND
/// covered by the catch-up of every index the query relies on, so the watermark
/// is the minimum across `index_names`. Gating on fewer than all of them would
/// drop SSTables holding rows an uncounted index has not yet indexed, and that
/// arm would silently return fewer rows.
///
/// See [`arm_maintained_index_names`] for which indexes are collected today: a
/// vector search with a scalar prefilter is not yet among them.
///
/// An empty `index_names` (a plain scan) uses the compaction watermark alone.
/// First occurrence per shard mirrors Lance's `compacted_generation_for_shard`.
fn exclusion_watermarks(
    details: &MemWalIndexDetails,
    index_names: &[String],
) -> HashMap<Uuid, u64> {
    let mut exclude: HashMap<Uuid, u64> = HashMap::new();
    for entry in &details.compacted_sstables {
        let mut watermark = entry.generation;
        for name in index_names {
            if let Some(caught_up) = details
                .index_catchup
                .iter()
                .find(|icp| icp.index_name == *name)
                .and_then(|icp| icp.caught_up_generation_for_shard(&entry.shard_id))
            {
                watermark = watermark.min(caught_up);
            }
        }
        exclude.entry(entry.shard_id).or_insert(watermark);
    }
    exclude
}

/// Assemble the per-shard snapshots (flushed SSTable generations) and the
/// in-memory memtables (active + frozen) for the table.
///
/// Snapshots for all shards come from their on-disk manifests; for the shard
/// with a live cached `ShardWriter` (this session's in-flight writes) the
/// writer's authoritative in-memory manifest and memtables override the
/// on-disk view so a read sees data not yet flushed.
async fn build_read_context(
    table: &NativeTable,
    dataset: &Dataset,
    details: &MemWalIndexDetails,
    index_names: &[String],
) -> Result<(Vec<ShardSnapshot>, HashMap<Uuid, InMemoryMemTables>)> {
    let exclude = exclusion_watermarks(details, index_names);

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
            snapshots.push(snapshot_from_manifest(shard_id, &manifest, &exclude));
        }
    }

    // WAL-only writers (enable_memtable=false) keep no in-memory memtable, and
    // `in_memory_memtable_refs` errors in that mode; the on-disk manifests above
    // already cover their flushed SSTables, so skip the live-writer snapshot. (Lance
    // forbids maintained indexes in WAL-only mode, so only plain scans reach here.)
    let wal_only = details
        .writer_config_defaults
        .get("enable_memtable")
        .map(|v| v == "false")
        .unwrap_or(false);

    // Override the active shard with the cached writer's in-memory view.
    let mut in_memory: HashMap<Uuid, InMemoryMemTables> = HashMap::new();
    if !wal_only
        && let Some((shard_id, manifest, memtables)) =
            table.dataset.shard_writer().read_snapshot().await?
    {
        if let Some(manifest) = manifest {
            let snapshot = snapshot_from_manifest(shard_id, &manifest, &exclude);
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

/// Convert a shard manifest into a read snapshot (current + not-yet-compacted
/// flushed SSTables). SSTable generations at or below the shard's compaction
/// watermark are already in the base table and are skipped.
fn snapshot_from_manifest(
    shard_id: Uuid,
    manifest: &ShardManifest,
    compacted: &HashMap<Uuid, u64>,
) -> ShardSnapshot {
    let mut snapshot = ShardSnapshot::new(shard_id)
        .with_spec_id(manifest.shard_spec_id)
        .with_current_generation(manifest.current_generation);
    let watermark = compacted.get(&shard_id).copied();
    for sstable in &manifest.sstables {
        if watermark.is_some_and(|w| sstable.generation <= w) {
            continue;
        }
        snapshot = snapshot.with_sstable(sstable.generation, sstable.path.clone());
    }
    snapshot
}

/// Columns selected by the query, if an explicit projection was requested.
fn selected_columns(query: &VectorQueryRequest) -> Option<Vec<String>> {
    match &query.base.select {
        Select::Columns(columns) => Some(columns.clone()),
        _ => None,
    }
}

/// Non-negative `Option<usize>` limit/offset as the `Option<i64>` the scanner
/// expects.
fn as_i64(value: Option<usize>) -> Option<i64> {
    value.map(|v| v as i64)
}

/// Build a base `LsmScanner` configured with sources, filter, and projection.
///
/// The filter set here is applied as a prefilter across every arm — plain scan,
/// full-text search, and vector search — since all three terminate on this
/// scanner's `create_plan`.
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
        scanner = scanner.project(&refs)?;
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

/// Plain scan: filter / projection / limit over base ∪ SSTables ∪ in-memory.
/// The plain scan applies limit and offset inside the planner.
async fn plain_plan(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let scanner = base_scanner(dataset, query, pk_columns, snapshots, in_memory)?
        .limit(as_i64(limit), as_i64(offset))?;
    Ok(scanner.create_plan().await?)
}

/// Full-text search over base ∪ SSTables ∪ in-memory, merged by local BM25 score.
/// The scanner threads the query filter in as a prefilter and pages via limit/offset.
#[allow(clippy::too_many_arguments)]
async fn fts_plan(
    dataset: &Dataset,
    fts: lance_index::scalar::FullTextSearchQuery,
    query: &VectorQueryRequest,
    details: &MemWalIndexDetails,
    pk_columns: Vec<String>,
    snapshots: Vec<ShardSnapshot>,
    in_memory: HashMap<Uuid, InMemoryMemTables>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Pre-check for a lancedb-flavored error; `LsmScanner` also validates the
    // single-column requirement, but without the `use_lsm(false)` guidance.
    let columns: Vec<String> = fts.columns().into_iter().collect();
    if columns.len() > 1 {
        return Err(Error::NotSupported {
            message: "the MemWAL LSM scanner full-text search supports a single column; set use_lsm(false) to read the base table only".to_string(),
        });
    }
    let column = columns.first().ok_or_else(|| Error::NotSupported {
        message: "the MemWAL LSM scanner full-text search requires an explicit FTS column"
            .to_string(),
    })?;

    // Without a maintained in-memory FTS index for this column, the active memtable
    // arm produces an empty plan (`active_source_can_execute_fts` returns false), so
    // the search silently omits un-compacted documents. Reject rather than mislead.
    if !index_maintained(
        dataset,
        column,
        &details.maintained_indexes,
        "InvertedIndexDetails",
    )
    .await?
    {
        return Err(Error::NotSupported {
            message: format!(
                "the MemWAL LSM scanner full-text search requires the FTS index on '{column}' to be maintained by the write spec (LsmWriteSpec::with_maintained_indexes); otherwise un-compacted documents are omitted. set use_lsm(false) to read the base table only"
            ),
        });
    }

    let scanner = base_scanner(dataset, query, pk_columns, snapshots, in_memory)?
        .with_overfetch_factor(LSM_OVERFETCH_FACTOR)
        .full_text_search(fts)?
        .limit(as_i64(limit), as_i64(offset))?;
    Ok(scanner.create_plan().await?)
}

/// Whether an index of `type_url_suffix` covering `column` is in the MemWAL spec's
/// maintained set. Only a maintained index has its catch-up tracked (so exclusion is
/// gated correctly) and its in-memory arm kept current; an unmaintained base index
/// falls back to the compaction watermark and can drop rows it has not re-indexed.
/// The type must match specifically — a maintained BTree on the same column is not
/// the FTS/vector index the arm relies on.
async fn index_maintained(
    dataset: &Dataset,
    column: &str,
    maintained: &[String],
    type_url_suffix: &str,
) -> Result<bool> {
    use lance::index::DatasetIndexExt;
    let Some(field) = dataset.schema().field(column) else {
        return Ok(false);
    };
    let indices = dataset.load_indices().await?;
    Ok(indices.iter().any(|idx| {
        idx.fields.contains(&field.id)
            && maintained.iter().any(|m| m == &idx.name)
            && idx
                .index_details
                .as_ref()
                .is_some_and(|d| d.type_url.ends_with(type_url_suffix))
    }))
}

/// Every maintained base index this query relies on, used to gate SSTable
/// exclusion by index catch-up.
///
/// Returns a list because the watermark must be the lowest across every index a
/// query relies on. Today it never holds more than one: `reject_unsupported`
/// refuses hybrid search, so the vector and full-text arms are mutually
/// exclusive.
///
/// The case that is genuinely multi-index -- a vector search with a scalar or
/// bitmap prefilter -- is **not collected yet**. Identifying those needs the
/// planner's chosen indexes, not the columns the filter names, and no Lance API
/// exposes them. Until it does, such a query is gated on its vector index alone.
///
/// Empty for a plain scan, or when no maintained index covers the searched
/// column.
async fn arm_maintained_index_names(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    details: &MemWalIndexDetails,
) -> Result<Vec<String>> {
    use lance::index::DatasetIndexExt;

    // Each arm's searched column, the index-detail type it relies on, and a
    // label for diagnostics — catch-up is taken from the vector/FTS index
    // specifically, not a BTree on the same column.
    let mut arms: Vec<(String, &str, &str)> = Vec::new();
    if !query.query_vector.is_empty() {
        let arrow_schema = ArrowSchema::from(dataset.schema());
        let column = match &query.column {
            Some(column) => column.clone(),
            None => {
                let dim = query.query_vector.first().map(|v| v.len() as i32);
                default_vector_column(&arrow_schema, dim)?
            }
        };
        arms.push((column, "VectorIndexDetails", "vector"));
    }
    if let Some(fts) = &query.base.full_text_search
        && let Some(column) = fts.columns().into_iter().next()
    {
        arms.push((column, "InvertedIndexDetails", "full-text"));
    }
    if arms.is_empty() {
        return Ok(Vec::new());
    }

    let indices = dataset.load_indices().await?;
    let mut names = Vec::with_capacity(arms.len());
    for (column, type_url_suffix, arm) in arms {
        let Some(field) = dataset.schema().field(&column) else {
            continue;
        };
        let segment_names: Vec<String> = indices
            .iter()
            .filter(|idx| {
                idx.fields.contains(&field.id)
                    && idx
                        .index_details
                        .as_ref()
                        .is_some_and(|d| d.type_url.ends_with(type_url_suffix))
            })
            .map(|idx| idx.name.clone())
            .collect();
        if let Some(name) =
            resolve_single_index(segment_names, &details.maintained_indexes, arm, &column)?
        {
            names.push(name);
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

/// Resolve the single logical index from the names of its matching physical
/// segments. `load_indices` returns one entry per segment, so one logical index can
/// appear multiple times (same name); dedupe by name before counting. Errors when
/// more than one *distinct* index covers the field — the base planner's choice is
/// ambiguous and their catch-up watermarks can diverge, so gating exclusion on the
/// wrong one could drop SSTables the used index has not caught up to. Otherwise
/// returns the name only when it is maintained (else the caller falls back to the
/// compaction watermark).
fn resolve_single_index(
    mut names: Vec<String>,
    maintained: &[String],
    arm: &str,
    column: &str,
) -> Result<Option<String>> {
    names.sort();
    names.dedup();
    if names.len() > 1 {
        return Err(Error::NotSupported {
            message: format!(
                "the MemWAL LSM scanner cannot resolve the {arm} index catch-up watermark for '{column}': it has multiple {arm} indexes; set use_lsm(false) to read the base table only"
            ),
        });
    }
    Ok(names
        .into_iter()
        .next()
        .filter(|name| maintained.contains(name)))
}

/// Drop the primary-key columns Lance appends internally for dedup when the user's
/// explicit projection did not request them, restoring the requested output schema.
/// `Select::All` legitimately includes the pk columns and is left untouched.
fn restore_projection(
    plan: Arc<dyn ExecutionPlan>,
    query: &VectorQueryRequest,
    pk_columns: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let Select::Columns(selected) = &query.base.select else {
        return Ok(plan);
    };
    let schema = plan.schema();
    // Keep a column unless it is a pk column the user did not select (this preserves
    // user columns and score columns like `_distance`, dropping only leaked pk).
    let keep: Vec<(Arc<dyn PhysicalExpr>, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            selected.iter().any(|c| c == f.name()) || !pk_columns.iter().any(|pk| pk == f.name())
        })
        .map(|(i, f)| {
            (
                Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
                f.name().clone(),
            )
        })
        .collect();
    if keep.len() == schema.fields().len() {
        return Ok(plan);
    }
    Ok(Arc::new(ProjectionExec::try_new(keep, plan)?))
}

/// Vector (ANN) search over base ∪ SSTables ∪ in-memory, routed through the same
/// [`LsmScanner`] as the other arms so the query filter is applied as a prefilter.
///
/// Note: the base and SSTable arms use `fast_search` (indexed data only), so a
/// base-table row not covered by a vector index is invisible here — it surfaces
/// only via the memtable or `use_lsm(false)`.
#[allow(clippy::too_many_arguments)]
async fn vector_plan(
    dataset: &Dataset,
    query: &VectorQueryRequest,
    details: &MemWalIndexDetails,
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

    // The base arm relies on the column's vector index (`fast_search`). Unless it is
    // maintained, its catch-up is untracked and exclusion falls back to the
    // compaction watermark — dropping compacted SSTables the (lagging) base index has
    // not re-indexed. Reject rather than silently omit rows, mirroring the FTS arm.
    if !index_maintained(
        dataset,
        &column,
        &details.maintained_indexes,
        "VectorIndexDetails",
    )
    .await?
    {
        return Err(Error::NotSupported {
            message: format!(
                "the MemWAL LSM scanner requires the vector index on '{column}' to be maintained by the write spec (LsmWriteSpec::with_maintained_indexes); otherwise compacted rows not yet re-indexed are omitted. set use_lsm(false) to read the base table only"
            ),
        });
    }

    // The LSM vector planner is Float32-only; reject binary (uint8) vectors with a
    // clear error rather than failing deep in the planner.
    if is_binary_vector_column(&arrow_schema, &column) {
        return Err(Error::NotSupported {
            message: "the MemWAL LSM scanner does not support binary (uint8) vector search; set use_lsm(false) to read the base table only".to_string(),
        });
    }

    let distance_type = resolve_distance_type(dataset, query, &column).await?;

    // `nearest` takes a flat query vector and builds the fixed-size list itself.
    // Guard `k` to at least 1 so a degenerate `limit(0)` is trimmed by `limit`
    // below rather than rejected by `nearest`.
    let k = limit.unwrap_or(DEFAULT_TOP_K).max(1);
    let mut scanner = base_scanner(dataset, query, pk_columns, snapshots, in_memory)?
        .with_overfetch_factor(LSM_OVERFETCH_FACTOR)
        .nearest(&column, query_vector.as_ref(), k)?
        .nprobes(query.minimum_nprobes)
        .distance_metric(distance_type.into());
    if let Some(refine_factor) = query.refine_factor {
        scanner = scanner.refine(refine_factor);
    }
    scanner = scanner.limit(as_i64(limit), as_i64(offset))?;
    Ok(scanner.create_plan().await?)
}

/// Whether `column` stores binary (uint8) vectors, which the LSM vector planner
/// does not support.
fn is_binary_vector_column(schema: &ArrowSchema, column: &str) -> bool {
    matches!(
        schema.field_with_name(column).map(|f| f.data_type()),
        Ok(DataType::FixedSizeList(field, _)) if matches!(field.data_type(), DataType::UInt8)
    )
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
                    .open_vector_index(column, &index.uuid, &NoOpMetricsCollector)
                    .await
            {
                return Ok(vector_index.metric_type().into());
            }
        }
    }
    Ok(DistanceType::L2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_index::mem_wal::{CompactedSsTable, IndexCatchupProgress};

    #[test]
    fn exclusion_watermark_gates_on_lagging_index_catchup() {
        let shard = Uuid::from_u128(1);
        let details = MemWalIndexDetails {
            // Compaction has drained generations through 5 into the base table...
            compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            // ...but the FTS index has only caught up through generation 2.
            index_catchup: vec![IndexCatchupProgress::new(
                "fts_idx".to_string(),
                vec![CompactedSsTable::new(shard, 2)],
            )],
            maintained_indexes: vec!["fts_idx".to_string()],
            ..Default::default()
        };

        // Plain scan: drop every compacted generation (through 5).
        assert_eq!(exclusion_watermarks(&details, &[]).get(&shard), Some(&5));

        // FTS arm with a lagging index: exclusion is capped at the index catch-up
        // (2), so SSTable generations 3..=5 are retained until the index covers
        // them — otherwise those documents would silently vanish from FTS results.
        assert_eq!(
            exclusion_watermarks(&details, &["fts_idx".to_string()]).get(&shard),
            Some(&2)
        );

        // A caught-up index — or one untracked in index_catchup — falls back to the
        // compaction watermark.
        assert_eq!(
            exclusion_watermarks(&details, &["caught_up_idx".to_string()]).get(&shard),
            Some(&5)
        );
    }

    /// A hybrid search reads a vector and a full-text index, and either may lag.
    /// Retaining to the lower of the two is what keeps both arms complete;
    /// gating on one alone would drop SSTables the other has not indexed.
    #[test]
    fn exclusion_watermark_takes_the_minimum_across_every_index_used() {
        let shard = Uuid::from_u128(1);
        let details = MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 9)],
            index_catchup: vec![
                IndexCatchupProgress::new(
                    "vec_idx".to_string(),
                    vec![CompactedSsTable::new(shard, 7)],
                ),
                IndexCatchupProgress::new(
                    "fts_idx".to_string(),
                    vec![CompactedSsTable::new(shard, 4)],
                ),
            ],
            maintained_indexes: vec!["vec_idx".to_string(), "fts_idx".to_string()],
            ..Default::default()
        };

        // Each index alone stops at its own catch-up.
        assert_eq!(
            exclusion_watermarks(&details, &["vec_idx".to_string()]).get(&shard),
            Some(&7)
        );
        assert_eq!(
            exclusion_watermarks(&details, &["fts_idx".to_string()]).get(&shard),
            Some(&4)
        );

        // Used together, the lower one governs regardless of order.
        let both = ["vec_idx".to_string(), "fts_idx".to_string()];
        assert_eq!(exclusion_watermarks(&details, &both).get(&shard), Some(&4));
        let reversed = ["fts_idx".to_string(), "vec_idx".to_string()];
        assert_eq!(
            exclusion_watermarks(&details, &reversed).get(&shard),
            Some(&4)
        );
    }

    /// An index with no catch-up entry contributes no cap today, so a lagging
    /// sibling must still govern rather than being widened by the untracked one.
    #[test]
    fn an_untracked_index_does_not_widen_a_lagging_sibling() {
        let shard = Uuid::from_u128(1);
        let details = MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 9)],
            index_catchup: vec![IndexCatchupProgress::new(
                "fts_idx".to_string(),
                vec![CompactedSsTable::new(shard, 4)],
            )],
            maintained_indexes: vec!["fts_idx".to_string(), "untracked_idx".to_string()],
            ..Default::default()
        };

        let both = ["fts_idx".to_string(), "untracked_idx".to_string()];
        assert_eq!(exclusion_watermarks(&details, &both).get(&shard), Some(&4));
    }

    #[test]
    fn resolve_single_index_dedupes_segments() {
        let maintained = vec!["fts_idx".to_string()];
        // Two physical segments of ONE logical index must not count as "multiple".
        assert_eq!(
            resolve_single_index(
                vec!["fts_idx".to_string(), "fts_idx".to_string()],
                &maintained,
                "full-text",
                "text"
            )
            .unwrap(),
            Some("fts_idx".to_string())
        );
        // Two distinct indexes on the field are ambiguous → error.
        assert!(
            resolve_single_index(
                vec!["fts_a".to_string(), "fts_b".to_string()],
                &maintained,
                "full-text",
                "text"
            )
            .is_err()
        );
        // A single unmaintained index resolves to None (compaction-watermark fallback).
        assert_eq!(
            resolve_single_index(vec!["other".to_string()], &maintained, "full-text", "text")
                .unwrap(),
            None
        );
    }
}
