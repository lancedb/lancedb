// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! LSM Scanner builder.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::{Array, FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{Expr, SessionContext};
use futures::TryStreamExt;
use lance_core::{Error, Result, is_system_column};
use lance_datafusion::expr::safe_coerce_scalar;
use lance_index::scalar::FullTextSearchQuery;
use lance_linalg::distance::DistanceType;
use uuid::Uuid;

use super::collector::{InMemoryMemTableRef, InMemoryMemTables, LsmDataSourceCollector};
use super::data_source::{FreshTierWatermark, ShardSnapshot};
use super::planner::LsmScanPlanner;
use super::point_lookup::LsmPointLookupPlanner;
use super::projection::validate_projection_names;
use super::sstable_cache::{DatasetCache, SsTableWarmer};
use crate::dataset::Dataset;
use crate::dataset::mem_wal::util::derived_store_params;
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// Vector (KNN) search state, set by [`LsmScanner::nearest`] and friends. Mirrors
/// the subset of `lance::dataset::scanner::Query` the LSM vector planner honors.
#[derive(Debug, Clone)]
struct LsmVectorQuery {
    /// Vector column to search.
    column: String,
    /// Flat query vector (one vector; validated against the column dim in `create_plan`).
    key: Arc<dyn Array>,
    /// Number of nearest neighbors to fetch per source before the global merge.
    k: usize,
    /// Number of IVF partitions to probe on the base arm.
    nprobes: usize,
    /// Re-rank base candidates with exact distances when set (refine factor is
    /// treated as a boolean; the LSM merge needs exact base distances).
    refine: bool,
    /// Distance metric; `None` defaults to L2 (matching the unindexed memtable arm).
    metric_type: Option<DistanceType>,
}

/// If `filter` is a point-lookup shape on `pk_col` — `pk = lit` (either
/// operand order) or `pk IN (lit, …)` — return the literal key values. Any
/// other shape returns `None`, so the scanner falls through to the general
/// scan plan. `IN` keys are normalized to `pk_type` only for deduplication;
/// the first original literal is retained so the lookup path still chooses
/// between its exact-type fast path and coercing fallback. A literal that
/// cannot be normalized routes the expression through the general scan.
fn extract_pk_point_keys(
    filter: &Expr,
    pk_col: &str,
    pk_type: &DataType,
) -> Option<Vec<ScalarValue>> {
    match filter {
        Expr::BinaryExpr(b) if matches!(b.op, Operator::Eq) => {
            match (b.left.as_ref(), b.right.as_ref()) {
                (Expr::Column(c), Expr::Literal(lit, _))
                | (Expr::Literal(lit, _), Expr::Column(c))
                    if c.name == pk_col =>
                {
                    Some(vec![lit.clone()])
                }
                _ => None,
            }
        }
        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(c) = in_list.expr.as_ref() else {
                return None;
            };
            if c.name != pk_col {
                return None;
            }
            let mut vals = Vec::with_capacity(in_list.list.len());
            let mut seen = HashSet::with_capacity(in_list.list.len());
            for e in &in_list.list {
                let Expr::Literal(lit, _) = e else {
                    return None; // a non-literal IN element → not a point lookup
                };
                let identity = safe_coerce_scalar(lit, pk_type)?;
                if seen.insert(identity) {
                    vals.push(lit.clone());
                }
            }
            (!vals.is_empty()).then_some(vals)
        }
        _ => None,
    }
}

/// Either a base Lance table, or an explicit base path used to resolve
/// SSTable directories when no base dataset is configured.
enum BaseSource {
    Table(Arc<Dataset>),
    PathOnly(String),
}

/// The fixed-size-list dimension of `column` in `schema`. Errors if the column
/// is missing or is not a fixed-size-list vector column.
fn vector_dim(schema: &arrow_schema::Schema, column: &str) -> Result<i32> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| Error::invalid_input(format!("vector column '{}' not found", column)))?;
    match field.data_type() {
        DataType::FixedSizeList(_, dim) => Ok(*dim),
        other => Err(Error::invalid_input(format!(
            "column '{}' is not a fixed-size-list vector (got {:?})",
            column, other
        ))),
    }
}

/// Wrap a flat `Float32` query vector into a single-row `FixedSizeListArray` of
/// `dim`, as the vector planner expects. A pre-built single-row FSL passes
/// through unchanged. The query length must match the column dimension.
fn key_to_fsl(key: &dyn Array, dim: i32) -> Result<FixedSizeListArray> {
    if let Some(fsl) = key.as_any().downcast_ref::<FixedSizeListArray>() {
        // The LSM cross-source merge assumes a single query vector; a multi-row
        // FSL would otherwise flow through and silently produce wrong results.
        if fsl.len() != 1 {
            return Err(Error::invalid_input(format!(
                "LSM vector search supports a single query vector, got {} rows",
                fsl.len()
            )));
        }
        if fsl.value_length() != dim {
            return Err(Error::invalid_input(format!(
                "query vector dimension {} does not match column dimension {}",
                fsl.value_length(),
                dim
            )));
        }
        return Ok(fsl.clone());
    }
    let values = key
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| Error::invalid_input("query vector must be Float32".to_string()))?;
    if values.len() != dim as usize {
        return Err(Error::invalid_input(format!(
            "query vector dimension {} does not match column dimension {}",
            values.len(),
            dim
        )));
    }
    let mut builder =
        FixedSizeListBuilder::with_capacity(Float32Builder::with_capacity(dim as usize), dim, 1);
    builder.values().append_slice(values.values());
    builder.append(true);
    Ok(builder.finish())
}

/// Scanner for LSM tree data spanning base table, SSTables, and active MemTable.
///
/// This scanner provides a unified interface for querying data across multiple
/// LSM tree levels:
/// - Base table (compacted data, generation = 0)
/// - SSTables (persisted but not yet compacted, generation = 1, 2, ...)
/// - Active MemTable (in-memory buffer, highest generation)
///
/// The scanner automatically handles deduplication by primary key, keeping
/// the newest version based on generation number and row address.
///
/// # Example
///
/// ```ignore
/// let scanner = LsmScanner::new(base_table, shard_snapshots, vec!["pk".to_string()])
///     .project(&["id", "name"])?
///     .filter("id > 10")?
///     .limit(Some(100), None)?;
///
/// let results = scanner.try_into_batch().await?;
/// ```
///
/// The query-building methods mirror [`crate::dataset::scanner::Scanner`]:
/// [`Self::nearest`] (+ [`Self::nprobes`] / [`Self::refine`] /
/// [`Self::distance_metric`]) for vector search and [`Self::full_text_search`]
/// for FTS are state setters, and [`Self::create_plan`] dispatches to the right
/// planner — so an LSM read reads like a normal scan.
pub struct LsmScanner {
    // Data sources
    base: BaseSource,
    /// Schema used for projection, empty plans, and filter parsing.
    /// Derived from the base dataset when one is present, otherwise supplied
    /// explicitly by [`Self::without_base_table`].
    schema: SchemaRef,
    shard_snapshots: Vec<ShardSnapshot>,
    /// In-memory memtables by shard (active + frozen-awaiting-flush), so
    /// the scanner path carries frozen-undrained generations too.
    in_memory_memtables: HashMap<Uuid, InMemoryMemTables>,

    // Query configuration
    projection: Option<Vec<String>>,
    filter: Option<Expr>,
    limit: Option<usize>,
    offset: Option<usize>,
    /// Vector (KNN) search; when set, `create_plan` routes to the vector planner.
    nearest: Option<LsmVectorQuery>,
    /// Full-text search; when set, `create_plan` routes to the FTS planner.
    full_text_query: Option<FullTextSearchQuery>,

    // Internal columns
    with_row_address: bool,
    with_memtable_gen: bool,

    // Primary key columns (required for deduplication)
    pk_columns: Vec<String>,

    /// Session for opening SSTables (shares the base's caches).
    /// Defaults to the base table's session.
    session: Option<Arc<Session>>,
    /// Store params for opening SSTables, reusing the base dataset's
    /// store. Defaults to the base table's params.
    store_params: Option<ObjectStoreParams>,
    /// Cache of opened SSTable datasets. When set, repeated
    /// queries against the same generation skip the manifest read entirely.
    sstable_cache: Option<Arc<dyn DatasetCache>>,
    /// Optional warmer fired on first open of an SSTable.
    warmer: Option<Arc<dyn SsTableWarmer>>,
    /// Over-fetch multiple for block-listed sources in search plans
    /// (see [`super::LsmFtsSearchPlanner::with_overfetch_factor`]).
    overfetch_factor: Option<f64>,
}

impl LsmScanner {
    /// Create a new LSM scanner.
    ///
    /// # Arguments
    ///
    /// * `base_table` - The base Lance table (compacted data)
    /// * `shard_snapshots` - Snapshots of shard states from MemWAL index
    /// * `pk_columns` - Primary key column names for deduplication
    pub fn new(
        base_table: Arc<Dataset>,
        shard_snapshots: Vec<ShardSnapshot>,
        pk_columns: Vec<String>,
    ) -> Self {
        let lance_schema = base_table.schema();
        let arrow_schema: arrow_schema::Schema = lance_schema.into();
        // Default the session to the base table's so the common path reuses
        // the shared index / metadata caches without extra wiring. An
        // explicit `with_session` still overrides this.
        let session = Some(base_table.session());
        // The scanner only ever opens SSTables with these — the base
        // table is already open and handed in — so they must not carry a
        // path-bound store binding.
        let store_params = base_table.store_params().map(derived_store_params);
        Self {
            base: BaseSource::Table(base_table),
            schema: Arc::new(arrow_schema),
            shard_snapshots,
            in_memory_memtables: HashMap::new(),
            projection: None,
            filter: None,
            limit: None,
            offset: None,
            nearest: None,
            full_text_query: None,
            with_row_address: false,
            with_memtable_gen: false,
            pk_columns,
            session,
            store_params,
            sstable_cache: None,
            warmer: None,
            overfetch_factor: None,
        }
    }

    /// Create a scanner that reads only the fresh tier (active memtable and
    /// SSTables) without including a base Lance table.
    ///
    /// This is useful when the caller owns the base read path separately and
    /// only needs the WAL's contribution: active memtable ∪ L0 SSTables.
    /// Deduplication semantics are unchanged — newer generations
    /// still win on PK conflicts.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema used for projection, filter parsing, and empty plans.
    ///   Should match the schema SSTables were written with.
    /// * `base_path` - Table-root URI used to resolve relative SSTable paths.
    /// * `shard_snapshots` - Snapshots of shard states from MemWAL index.
    /// * `pk_columns` - Primary key column names for deduplication.
    pub fn without_base_table(
        schema: SchemaRef,
        base_path: impl Into<String>,
        shard_snapshots: Vec<ShardSnapshot>,
        pk_columns: Vec<String>,
    ) -> Self {
        Self {
            base: BaseSource::PathOnly(base_path.into()),
            schema,
            shard_snapshots,
            in_memory_memtables: HashMap::new(),
            projection: None,
            filter: None,
            limit: None,
            offset: None,
            nearest: None,
            full_text_query: None,
            with_row_address: false,
            with_memtable_gen: false,
            pk_columns,
            session: None,
            store_params: None,
            sstable_cache: None,
            warmer: None,
            overfetch_factor: None,
        }
    }

    /// Set a shard's active memtable. Back-compat / test entry point; the
    /// read path uses [`Self::with_in_memory_memtables`]. Replaces the
    /// active memtable, preserving any frozen memtables already registered.
    pub fn with_active_memtable(mut self, shard_id: Uuid, memtable: InMemoryMemTableRef) -> Self {
        match self.in_memory_memtables.entry(shard_id) {
            Entry::Occupied(mut e) => e.get_mut().active = memtable,
            Entry::Vacant(e) => {
                e.insert(InMemoryMemTables {
                    active: memtable,
                    frozen: Vec::new(),
                });
            }
        }
        self
    }

    /// Register a shard's in-memory memtables (active + frozen-awaiting-
    /// flush) captured atomically by `ShardWriter::in_memory_memtable_refs`.
    /// The read path's entry point — closes the concurrent-read-vs-flush
    /// hole by carrying frozen-undrained generations into the scan.
    pub fn with_in_memory_memtables(
        mut self,
        shard_id: Uuid,
        memtables: InMemoryMemTables,
    ) -> Self {
        self.in_memory_memtables.insert(shard_id, memtables);
        self
    }

    /// Set the session used to open SSTables. Defaults to the base
    /// table's; set explicitly on a fresh-tier-only scanner (no base table).
    pub fn with_session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set the store params used to open SSTables. Defaults to the
    /// base table's; set explicitly on a fresh-tier-only scanner (no base table).
    ///
    /// Pass the params the *base* was opened with. As in [`Self::new`], they are
    /// adapted for generation URIs: a path-bound `object_store` binding would
    /// redirect every generation open at the base table itself, so it is dropped
    /// while storage options, wrapper, and credentials carry over.
    pub fn with_store_params(mut self, store_params: ObjectStoreParams) -> Self {
        self.store_params = Some(derived_store_params(&store_params));
        self
    }

    /// Inject a cache of opened SSTable datasets.
    ///
    /// With a cache, repeated queries against the same generation become a
    /// pure `Arc::clone` with no manifest read or object-store I/O. The cache
    /// is owned and sized by the caller (any [`DatasetCache`] impl, e.g.
    /// [`SsTableCache`](super::SsTableCache)); not set by
    /// default, so behavior is unchanged unless opted in.
    pub fn with_sstable_cache(mut self, cache: Arc<dyn DatasetCache>) -> Self {
        self.sstable_cache = Some(cache);
        self
    }

    /// Inject the warmer fired on first open of an SSTable. Not set by
    /// default, so behavior is unchanged unless opted in.
    pub fn with_warmer(mut self, warmer: Arc<dyn SsTableWarmer>) -> Self {
        self.warmer = Some(warmer);
        self
    }

    /// Set the over-fetch multiple block-listed sources use in search plans
    /// so they still yield `k` live rows after cross-generation dedup.
    /// Threaded into the LSM search planners; values below `1.0` are rejected
    /// when the plan is created.
    pub fn with_overfetch_factor(mut self, factor: f64) -> Self {
        self.overfetch_factor = Some(factor);
        self
    }

    /// Project specific columns.
    ///
    /// If not called, all columns from the base schema are included.
    /// Primary key columns are always included for deduplication.
    pub fn project<T: AsRef<str>>(mut self, columns: &[T]) -> Result<Self> {
        self.projection = Some(columns.iter().map(|s| s.as_ref().to_string()).collect());
        Ok(self)
    }

    /// Set filter expression using SQL-like syntax.
    ///
    /// The filter is pushed down to each data source when possible.
    pub fn filter(mut self, filter_expr: &str) -> Result<Self> {
        let expr = super::parse_filter_expr(self.schema.as_ref(), filter_expr)?;
        self.filter = Some(expr);
        Ok(self)
    }

    /// Set filter expression directly.
    pub fn filter_expr(mut self, expr: Expr) -> Self {
        self.filter = Some(expr);
        self
    }

    /// Limit the number of results, with an optional offset. Matches
    /// [`crate::dataset::scanner::Scanner::limit`]: both bounds are `Option<i64>`
    /// and must be non-negative.
    pub fn limit(mut self, limit: Option<i64>, offset: Option<i64>) -> Result<Self> {
        if let Some(value) = limit
            && value < 0
        {
            return Err(Error::invalid_input(
                "limit must be non-negative".to_string(),
            ));
        }
        if let Some(value) = offset
            && value < 0
        {
            return Err(Error::invalid_input(
                "offset must be non-negative".to_string(),
            ));
        }
        self.limit = limit.map(|value| value as usize);
        self.offset = offset.map(|value| value as usize);
        Ok(self)
    }

    /// Find the `k` nearest neighbors of `key` in `column`. Routes `create_plan`
    /// through the LSM vector planner (base ∪ SSTables ∪ in-memory). Mirrors
    /// [`crate::dataset::scanner::Scanner::nearest`]; the LSM path supports a
    /// single Float32 query vector. When combined with an offset, the LSM path
    /// fetches `k + offset` per source before applying the final page. Tune with
    /// [`Self::nprobes`], [`Self::refine`], and [`Self::distance_metric`].
    pub fn nearest(mut self, column: &str, key: &dyn Array, k: usize) -> Result<Self> {
        if k == 0 {
            return Err(Error::invalid_input("k must be positive".to_string()));
        }
        if key.is_empty() {
            return Err(Error::invalid_input(
                "query vector must have non-zero length".to_string(),
            ));
        }
        self.nearest = Some(LsmVectorQuery {
            column: column.to_string(),
            key: key.slice(0, key.len()),
            k,
            nprobes: 1,
            refine: false,
            metric_type: None,
        });
        Ok(self)
    }

    /// Number of IVF partitions to probe on the base arm (default 1). No-op
    /// unless [`Self::nearest`] was called.
    pub fn nprobes(mut self, nprobes: usize) -> Self {
        if let Some(q) = self.nearest.as_mut() {
            q.nprobes = nprobes;
        }
        self
    }

    /// Re-rank base-table candidates with exact distances so they are directly
    /// comparable to the exact memtable distances in the cross-source merge.
    /// The factor is treated as a boolean today. No-op unless `nearest` was set.
    pub fn refine(mut self, refine_factor: u32) -> Self {
        if let Some(q) = self.nearest.as_mut() {
            q.refine = refine_factor > 0;
        }
        self
    }

    /// Distance metric for the vector search. Defaults to L2. No-op unless
    /// [`Self::nearest`] was called.
    pub fn distance_metric(mut self, metric: DistanceType) -> Self {
        if let Some(q) = self.nearest.as_mut() {
            q.metric_type = Some(metric);
        }
        self
    }

    /// Include `_rowaddr` column in output.
    ///
    /// The row address is used for ordering within a generation.
    pub fn with_row_address(mut self) -> Self {
        self.with_row_address = true;
        self
    }

    /// Include `_memtable_gen` column in output.
    ///
    /// The generation column shows which data source each row came from:
    /// - 0: Base table
    /// - 1, 2, ...: MemTable generations (higher = newer)
    pub fn with_memtable_gen(mut self) -> Self {
        self.with_memtable_gen = true;
        self
    }

    /// Get the output schema.
    pub fn schema(&self) -> SchemaRef {
        // For now, return the configured schema. Full implementation would
        // compute the projected schema with optional _gen/_rowaddr columns.
        self.schema.clone()
    }

    /// Whether the projection requests any system column (e.g. `_rowaddr`,
    /// `_memtable_gen`), which only the union scan path can produce.
    fn projection_has_system_columns(&self) -> bool {
        self.projection
            .as_ref()
            .map(|p| p.iter().any(|c| is_system_column(c)))
            .unwrap_or(false)
    }

    /// Create the execution plan.
    pub async fn create_plan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        // Dispatch by builder state, mirroring `Scanner::create_plan` and
        // `MemTableScanner::create_plan`: vector search, then full-text search,
        // then the (point-lookup or union) plain scan.
        // The LSM path has no hybrid mode, so reject combined search rather than
        // silently dropping the full-text query under the vector dispatch.
        if self.nearest.is_some() && self.full_text_query.is_some() {
            return Err(Error::invalid_input(
                "LSM scanner does not support combined vector and full-text search".to_string(),
            ));
        }
        if self.nearest.is_some() {
            return self.plan_vector().await;
        }
        if self.full_text_query.is_some() {
            return self.plan_fts().await;
        }
        self.plan_scan().await
    }

    /// Apply the builder's `limit`/`offset` on top of a search plan. The plain
    /// scan applies them inside the planner; the vector/FTS arms wrap here.
    fn apply_limit_offset(&self, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let skip = self.offset.unwrap_or(0);
        if skip == 0 && self.limit.is_none() {
            return plan;
        }
        Arc::new(GlobalLimitExec::new(plan, skip, self.limit))
    }

    /// Vector (KNN) search across base ∪ SSTables ∪ in-memory, via the LSM vector
    /// planner. Honors the builder filter as a prefilter.
    async fn plan_vector(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let nearest = self
            .nearest
            .as_ref()
            .expect("plan_vector requires a nearest query");
        let base_schema = self.schema();
        let dim = vector_dim(base_schema.as_ref(), &nearest.column)?;
        let query_fsl = key_to_fsl(nearest.key.as_ref(), dim)?;
        let distance_type = nearest.metric_type.unwrap_or(DistanceType::L2);

        let collector = self.build_collector();
        let mut planner = super::LsmVectorSearchPlanner::new(
            collector,
            self.pk_columns.clone(),
            base_schema,
            nearest.column.clone(),
            distance_type,
        )
        .with_filter(self.filter.clone());
        if let BaseSource::Table(dataset) = &self.base {
            planner = planner.with_dataset(dataset.clone());
        }
        if let Some(session) = &self.session {
            planner = planner.with_session(session.clone());
        }
        if let Some(store_params) = &self.store_params {
            planner = planner.with_store_params(store_params.clone());
        }
        if let Some(cache) = &self.sstable_cache {
            planner = planner.with_sstable_cache(cache.clone());
        }
        if let Some(warmer) = &self.warmer {
            planner = planner.with_warmer(warmer.clone());
        }
        // Over-fetch by `offset` so the per-source top-k still yields `k` live
        // rows after `apply_limit_offset` skips the first `offset` (mirrors the
        // FTS arm, which folds `offset` into its `k`).
        let per_source_k = nearest.k.saturating_add(self.offset.unwrap_or(0));
        let overfetch_factor = self.overfetch_factor.unwrap_or(1.0);
        let plan = planner
            .plan_search(
                &query_fsl,
                per_source_k,
                nearest.nprobes,
                self.projection.as_deref(),
                nearest.refine,
                overfetch_factor,
            )
            .await?;
        Ok(self.apply_limit_offset(plan))
    }

    /// Full-text search across base ∪ SSTables ∪ in-memory, via the LSM FTS
    /// planner. Query/scanner limits bound per-source fetches when present;
    /// otherwise the search remains unbounded and any offset is applied above.
    async fn plan_fts(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let query = self
            .full_text_query
            .as_ref()
            .expect("plan_fts requires a full-text query");
        let columns: Vec<String> = query.columns().into_iter().collect();
        if columns.len() > 1 {
            return Err(Error::invalid_input(
                "LSM full-text search supports a single column".to_string(),
            ));
        }
        let column = columns.into_iter().next().ok_or_else(|| {
            Error::invalid_input(
                "full_text_search requires a column; set it with `FullTextSearchQuery::with_column`"
                    .to_string(),
            )
        })?;
        let base_schema = self.schema();
        let query_limit = query
            .limit
            .map(|limit| {
                if limit < 0 {
                    Err(Error::invalid_input(
                        "full-text search limit must be non-negative".to_string(),
                    ))
                } else {
                    Ok(limit as usize)
                }
            })
            .transpose()?;
        // Match the dataset Scanner: a query-level FTS limit is the hard
        // search cap, and scanner offset pages within that capped result set.
        let source_limit = match query_limit {
            Some(limit) => Some(limit),
            None => self
                .limit
                .map(|limit| limit.saturating_add(self.offset.unwrap_or(0))),
        };

        let collector = self.build_collector();
        let mut planner =
            super::LsmFtsSearchPlanner::new(collector, self.pk_columns.clone(), base_schema)
                .with_filter(self.filter.clone());
        if let Some(session) = &self.session {
            planner = planner.with_session(session.clone());
        }
        if let Some(store_params) = &self.store_params {
            planner = planner.with_store_params(store_params.clone());
        }
        if let Some(cache) = &self.sstable_cache {
            planner = planner.with_sstable_cache(cache.clone());
        }
        if let Some(warmer) = &self.warmer {
            planner = planner.with_warmer(warmer.clone());
        }
        if let Some(factor) = self.overfetch_factor {
            planner = planner.with_overfetch_factor(factor);
        }
        let plan = planner
            .plan_search(
                &column,
                query.clone(),
                source_limit,
                self.projection.as_deref(),
            )
            .await?;
        Ok(self.apply_limit_offset(plan))
    }

    /// Plain (filter / projection / limit) scan over base ∪ SSTables ∪ in-memory.
    async fn plan_scan(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let collector = self.build_collector();
        let base_schema = self.schema();
        validate_projection_names(self.projection.as_deref(), &base_schema, &[])?;

        // Fast point-lookup routing: a `pk = lit` / `pk IN (..)` filter on the
        // single pk column — with no offset and no scan-only system columns —
        // bypasses the union/dedup scan for the direct BTree point-lookup path
        // (`LsmPointLookupPlanner`), composed as a normal `ExecutionPlan` so a
        // `limit` still applies on top. Any other shape falls through to the
        // general scan, so this never changes results for unmatched queries.
        if self.pk_columns.len() == 1
            && self.offset.is_none()
            && !self.with_memtable_gen
            && !self.with_row_address
            && !self.projection_has_system_columns()
            && let Some(filter) = &self.filter
            && let Ok(pk_field) = base_schema.field_with_name(&self.pk_columns[0])
            && let Some(keys) =
                extract_pk_point_keys(filter, &self.pk_columns[0], pk_field.data_type())
        {
            let mut planner =
                LsmPointLookupPlanner::new(collector, self.pk_columns.clone(), base_schema);
            if let Some(session) = &self.session {
                planner = planner.with_session(session.clone());
            }
            if let Some(store_params) = &self.store_params {
                planner = planner.with_store_params(store_params.clone());
            }
            if let Some(cache) = &self.sstable_cache {
                planner = planner.with_sstable_cache(cache.clone());
            }
            if let Some(warmer) = &self.warmer {
                planner = planner.with_warmer(warmer.clone());
            }
            let plan = planner
                .plan_point_lookup(&keys, self.projection.as_deref())
                .await?;
            return Ok(match self.limit {
                Some(n) => Arc::new(GlobalLimitExec::new(plan, 0, Some(n))),
                None => plan,
            });
        }

        let mut planner = LsmScanPlanner::new(collector, self.pk_columns.clone(), base_schema);
        if let Some(session) = &self.session {
            planner = planner.with_session(session.clone());
        }
        if let Some(store_params) = &self.store_params {
            planner = planner.with_store_params(store_params.clone());
        }
        if let Some(cache) = &self.sstable_cache {
            planner = planner.with_sstable_cache(cache.clone());
        }
        if let Some(warmer) = &self.warmer {
            planner = planner.with_warmer(warmer.clone());
        }

        planner
            .plan_scan(
                self.projection.as_deref(),
                self.filter.as_ref(),
                self.limit,
                self.offset,
                self.with_memtable_gen,
                self.with_row_address,
            )
            .await
    }

    /// Find rows matching a full-text query. Routes `create_plan` through the
    /// LSM FTS planner (base ∪ SSTables ∪ in-memory), local-scored by BM25 and
    /// merged by `_score` DESC. Mirrors
    /// [`crate::dataset::scanner::Scanner::full_text_search`]: the searched
    /// column(s) come from the query (set via `FullTextSearchQuery::with_column`);
    /// the query limit, when present, controls the FTS source/merge top-k;
    /// scanner limit/offset still apply to the final merged result. The LSM
    /// path supports a single FTS-indexed column.
    pub fn full_text_search(mut self, query: FullTextSearchQuery) -> Result<Self> {
        self.full_text_query = Some(query);
        Ok(self)
    }

    /// Execute the scan and return a stream of record batches.
    pub async fn try_into_stream(&self) -> Result<SendableRecordBatchStream> {
        let plan = self.create_plan().await?;
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        plan.execute(0, task_ctx)
            .map_err(|e| Error::io(format!("Failed to execute plan: {}", e)))
    }

    /// Execute the scan and collect all results into a single RecordBatch.
    pub async fn try_into_batch(&self) -> Result<RecordBatch> {
        let stream = self.try_into_stream().await?;
        let output_schema = stream.schema();
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| Error::io(format!("Failed to collect batches: {}", e)))?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema));
        }

        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|e| Error::io(format!("Failed to concatenate batches: {}", e)))
    }

    /// Count the number of rows that match the query.
    pub async fn count_rows(&self) -> Result<u64> {
        let stream = self.try_into_stream().await?;
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| Error::io(format!("Failed to count rows: {}", e)))?;

        Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
    }

    /// Test which `pks` have been (re)written in the WAL fresh tier — the active
    /// and frozen memtables and SSTables this scanner spans — i.e.
    /// are shadowed above the base table. `pks` is a batch whose columns include
    /// the primary-key columns; the returned `Vec<bool>` is aligned with its
    /// rows. Hashing matches the scanner's internal dedup, so the caller never
    /// hashes PKs itself. Flushed membership comes from the injected
    /// [`DatasetCache`] when one is set.
    pub async fn contains_pks(&self, pks: &RecordBatch) -> Result<Vec<bool>> {
        self.contains_pks_at(pks, None).await
    }

    /// As-of variant of [`Self::contains_pks`]. Membership is evaluated against
    /// a per-shard watermark on the fresh tier, supplied via `watermarks` (see
    /// [`FreshTierWatermark`]), matching the tier a prior scan observed and
    /// avoiding the two-snapshot skew that would drop a base row with no
    /// delivered replacement. `None` evaluates against the live tier.
    pub async fn contains_pks_at(
        &self,
        pks: &RecordBatch,
        watermarks: Option<&HashMap<Uuid, FreshTierWatermark>>,
    ) -> Result<Vec<bool>> {
        let sources = self.build_collector().collect()?;
        let memberships = super::block_list::fresh_tier_block_list(
            &sources,
            self.session.as_ref(),
            self.store_params.as_ref(),
            self.sstable_cache.as_ref(),
            watermarks,
        )
        .await?;
        let pk_indices = super::exec::resolve_pk_indices(pks, &self.pk_columns)
            .map_err(|e| Error::invalid_input(e.to_string()))?;
        // One key per row, in the index key space (typed value, or encoded
        // `Binary` tuple for a composite PK).
        let keys: Vec<ScalarValue> = (0..pks.num_rows())
            .map(|row| {
                let values: Vec<ScalarValue> = pk_indices
                    .iter()
                    .map(|&col| ScalarValue::try_from_array(pks.column(col), row))
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|e| Error::invalid_input(e.to_string()))?;
                super::block_list::on_disk_pk_key(&values)
            })
            .collect::<Result<_>>()?;

        // A row is contained if any generation contains its key. Probe each
        // generation once (batched), narrowing to still-unfound rows.
        let mut contained = vec![false; keys.len()];
        let mut live: Vec<usize> = (0..keys.len()).collect();
        for membership in &memberships {
            if live.is_empty() {
                break;
            }
            let live_keys: Vec<ScalarValue> = live.iter().map(|&i| keys[i].clone()).collect();
            let mask = membership.contains_keys(&live_keys).await?;
            let mut next_live = Vec::with_capacity(live.len());
            for (pos, &row) in live.iter().enumerate() {
                if mask[pos] {
                    contained[row] = true;
                } else {
                    next_live.push(row);
                }
            }
            live = next_live;
        }
        Ok(contained)
    }

    /// Build the data source collector.
    fn build_collector(&self) -> LsmDataSourceCollector {
        let mut collector = match &self.base {
            BaseSource::Table(dataset) => {
                LsmDataSourceCollector::new(dataset.clone(), self.shard_snapshots.clone())
            }
            BaseSource::PathOnly(path) => LsmDataSourceCollector::without_base_table(
                path.clone(),
                self.shard_snapshots.clone(),
            ),
        };

        for (shard_id, mems) in &self.in_memory_memtables {
            collector = collector.with_in_memory_memtables(*shard_id, mems.clone());
        }

        collector
    }
}

impl std::fmt::Debug for LsmScanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (label, value) = match &self.base {
            BaseSource::Table(dataset) => ("base_table", dataset.uri().to_string()),
            BaseSource::PathOnly(path) => ("base_path", path.clone()),
        };
        f.debug_struct("LsmScanner")
            .field(label, &value)
            .field("num_shards", &self.shard_snapshots.len())
            .field(
                "num_in_memory_memtables",
                &self
                    .in_memory_memtables
                    .values()
                    .map(|m| 1 + m.frozen.len())
                    .sum::<usize>(),
            )
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .field("offset", &self.offset)
            .field("pk_columns", &self.pk_columns)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, ListArray, StringArray, StructArray, UInt32Array};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{Field, Fields};
    use lance_index::scalar::inverted::{DOC_INDEX_COL, DocumentGranularity, InvertedIndexParams};

    use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

    #[test]
    fn test_lsm_scanner_builder() {
        // Test that the builder pattern compiles and works
        // Full integration tests would require a real dataset

        let pk_columns = ["id".to_string()];
        let shard_snapshots: Vec<ShardSnapshot> = vec![];

        // We can't easily create an Arc<Dataset> without I/O,
        // so just test the type construction
        assert_eq!(pk_columns.len(), 1);
        assert!(shard_snapshots.is_empty());
    }

    #[test]
    fn point_lookup_extraction_requires_normalizable_literals() {
        use datafusion::prelude::{col, lit};

        let filters = [
            col("id").in_list(vec![lit(1i32), lit("not an integer")], false),
            col("id").in_list(vec![lit(1i32), col("other")], false),
        ];
        for filter in filters {
            assert!(
                extract_pk_point_keys(&filter, "id", &DataType::Int32).is_none(),
                "unsupported point-lookup filter must use the scan path: {filter}"
            );
        }
    }

    #[test]
    fn test_shard_snapshot_construction() {
        use super::super::data_source::ShardSnapshot;

        let shard_id = Uuid::new_v4();
        let snapshot = ShardSnapshot::new(shard_id)
            .with_spec_id(1)
            .with_current_generation(5)
            .with_sstable(1, "path/gen_1".to_string())
            .with_sstable(2, "path/gen_2".to_string());

        assert_eq!(snapshot.shard_id, shard_id);
        assert_eq!(snapshot.spec_id, 1);
        assert_eq!(snapshot.current_generation, 5);
        assert_eq!(snapshot.sstables.len(), 2);
    }

    #[test]
    fn test_in_memory_memtable_ref() {
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let index_store = Arc::new(IndexStore::new());
        let schema = Arc::new(arrow_schema::Schema::empty());

        let memtable_ref = InMemoryMemTableRef {
            batch_store,
            index_store,
            schema,
            generation: 10,
        };

        assert_eq!(memtable_ref.generation, 10);
    }

    /// Single-column `id: Int32` schema used by the PK-membership tests.
    fn pk_schema() -> SchemaRef {
        use arrow_schema::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    /// A `RecordBatch` of `id` values against [`pk_schema`].
    fn id_pk_batch(ids: &[i32]) -> RecordBatch {
        use arrow_array::Int32Array;
        RecordBatch::try_new(pk_schema(), vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
    }

    /// An active/frozen memtable holding `ids` at `generation`, with a single
    /// batch and a maintained primary-key index on `id`.
    fn mk_pk_memtable(ids: &[i32], generation: u64) -> InMemoryMemTableRef {
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        let store = BatchStore::with_capacity(8);
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        let b = id_pk_batch(ids);
        let (bp, off, _) = store.append(b.clone()).unwrap();
        index.insert_with_batch_position(&b, off, Some(bp)).unwrap();
        InMemoryMemTableRef {
            batch_store: Arc::new(store),
            index_store: Arc::new(index),
            schema: pk_schema(),
            generation,
        }
    }

    #[tokio::test]
    async fn overfetch_factor_only_applies_to_searches() {
        // Plain scans refill exactly via LocalLimitExec, so overfetch_factor is
        // search-only and must not affect scan planning.
        let scan = LsmScanner::without_base_table(
            pk_schema(),
            "memory://scan",
            vec![],
            vec!["id".to_string()],
        )
        .with_overfetch_factor(0.5)
        .try_into_batch()
        .await
        .unwrap();
        assert_eq!(scan.num_rows(), 0);

        let vector_schema = pk_schema_with(arrow_schema::Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
                4,
            ),
            false,
        ));
        let vector_search = LsmScanner::without_base_table(
            vector_schema,
            "memory://vector",
            vec![],
            vec!["id".to_string()],
        )
        .nearest(
            "vector",
            &arrow_array::Float32Array::from(vec![0.0f32, 0.0, 0.0, 0.0]),
            1,
        )
        .unwrap()
        .with_overfetch_factor(0.5);
        let Err(err) = vector_search.try_into_stream().await else {
            panic!("invalid overfetch factor should fail vector search planning");
        };
        assert!(
            err.to_string().contains("overfetch_factor"),
            "unexpected error for invalid overfetch factor: {err}"
        );

        let fts_search = LsmScanner::without_base_table(
            pk_schema_with(arrow_schema::Field::new("text", DataType::Utf8, true)),
            "memory://fts",
            vec![],
            vec!["id".to_string()],
        )
        .full_text_search(
            FullTextSearchQuery::new("lance".to_string())
                .with_column("text".to_string())
                .unwrap(),
        )
        .unwrap()
        .with_overfetch_factor(0.5);
        let Err(err) = fts_search.try_into_stream().await else {
            panic!("invalid overfetch factor should fail full-text search planning");
        };
        assert!(
            err.to_string().contains("overfetch_factor"),
            "unexpected error for invalid overfetch factor: {err}"
        );
    }

    #[tokio::test]
    async fn unknown_scan_projection_column_is_rejected() {
        let scanner = LsmScanner::without_base_table(
            pk_schema(),
            "memory://t",
            vec![],
            vec!["id".to_string()],
        )
        .project(&["missing"])
        .unwrap();

        let Err(err) = scanner.try_into_stream().await else {
            panic!("unknown projection column should fail planning");
        };
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing-column projection error: {err}"
        );
    }

    #[tokio::test]
    async fn point_lookup_fast_route_rejects_missing_projection_column() {
        let shard = Uuid::new_v4();
        let scanner = LsmScanner::without_base_table(
            pk_schema(),
            "memory://t",
            vec![],
            vec!["id".to_string()],
        )
        .with_in_memory_memtables(
            shard,
            InMemoryMemTables {
                active: mk_pk_memtable(&[1, 2], 2),
                frozen: vec![],
            },
        )
        .project(&["missing"])
        .unwrap()
        .filter("id = 1")
        .unwrap();

        let Err(err) = scanner.try_into_stream().await else {
            panic!("unknown projection column should fail on point-lookup fast route");
        };
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing-column point lookup error: {err}"
        );
    }

    #[tokio::test]
    async fn full_text_search_missing_column_is_rejected() {
        use arrow_schema::{DataType, Field};

        let scanner = LsmScanner::without_base_table(
            pk_schema_with(Field::new("text", DataType::Utf8, true)),
            "memory://t",
            vec![],
            vec!["id".to_string()],
        )
        .full_text_search(
            FullTextSearchQuery::new("lance".to_string())
                .with_column("missing".to_string())
                .unwrap(),
        )
        .unwrap();

        let Err(err) = scanner.try_into_stream().await else {
            panic!("unknown FTS column should fail planning");
        };
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing FTS column error: {err}"
        );
    }

    #[tokio::test]
    async fn contains_pks_reports_fresh_tier_membership() {
        // Fresh-tier only: active gen 2 (pk=1,2) + frozen gen 1 (pk=3).
        let shard = Uuid::new_v4();
        let scanner = LsmScanner::without_base_table(
            pk_schema(),
            "memory://t",
            vec![],
            vec!["id".to_string()],
        )
        .with_in_memory_memtables(
            shard,
            InMemoryMemTables {
                active: mk_pk_memtable(&[1, 2], 2),
                frozen: vec![mk_pk_memtable(&[3], 1)],
            },
        );

        // pk=1 (active), pk=4 (absent), pk=3 (frozen).
        let result = scanner
            .contains_pks(&id_pk_batch(&[1, 4, 3]))
            .await
            .unwrap();
        assert_eq!(result, vec![true, false, true]);
    }

    /// `contains_pks_at` probes each generation once over the still-unfound
    /// rows, so a multi-PK batch spanning several generations resolves to the
    /// right per-row mask — and a watermark bounds which generations count.
    #[tokio::test]
    async fn contains_pks_at_batched_probe_respects_watermark() {
        use crate::dataset::mem_wal::scanner::data_source::FreshTierWatermark;

        // active gen 2 (pk=1,2) + frozen gen 1 (pk=3,4).
        let shard = Uuid::new_v4();
        let scanner = LsmScanner::without_base_table(
            pk_schema(),
            "memory://t",
            vec![],
            vec!["id".to_string()],
        )
        .with_in_memory_memtables(
            shard,
            InMemoryMemTables {
                active: mk_pk_memtable(&[1, 2], 2),
                frozen: vec![mk_pk_memtable(&[3, 4], 1)],
            },
        );

        // Duplicate and out-of-order keys exercise the live-row narrowing: each
        // generation only re-probes the rows earlier generations didn't claim.
        let probe = id_pk_batch(&[4, 1, 9, 3, 2, 1]);

        // watermark=None → live tier: every PK present in either generation.
        let live = scanner.contains_pks_at(&probe, None).await.unwrap();
        assert_eq!(live, vec![true, true, false, true, true, true]);

        // watermark at gen 1 → active gen 2 rolled in after the snapshot and is
        // excluded; only the frozen gen 1 keys (3,4) remain members.
        let watermarks: HashMap<Uuid, FreshTierWatermark> = [(
            shard,
            FreshTierWatermark {
                active_generation: 1,
                active_batch_count: u64::MAX,
            },
        )]
        .into_iter()
        .collect();
        let bounded = scanner
            .contains_pks_at(&probe, Some(&watermarks))
            .await
            .unwrap();
        assert_eq!(bounded, vec![true, false, false, true, false, false]);
    }

    fn pk_schema_with(extra: arrow_schema::Field) -> SchemaRef {
        use arrow_schema::{DataType, Field, Schema};
        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            extra,
        ]))
    }

    /// `LsmScanner::nearest(..).create_plan()` must route through the vector
    /// planner (exercising the Scanner-aligned facade, `key_to_fsl`, and
    /// `vector_dim`) and surface the in-memory match.
    #[tokio::test]
    async fn nearest_dispatches_through_facade() {
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{Float32Array, Int32Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field};

        let schema = pk_schema_with(Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ));
        let make_batch = |ids: &[i32]| -> RecordBatch {
            let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
            for id in ids {
                let base = *id as f32 * 0.1;
                for d in 0..4 {
                    vb.values().append_value(base + d as f32 * 0.1);
                }
                vb.append(true);
            }
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids.to_vec())),
                    Arc::new(vb.finish()),
                ],
            )
            .unwrap()
        };

        // Far, unindexed base rows contribute nothing under fast_search.
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(make_batch(&[100, 200]))], schema.clone());
        let base = Arc::new(
            Dataset::write(reader, &uri, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        let store = Arc::new(BatchStore::with_capacity(16));
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        index.add_hnsw(
            "vec_hnsw".to_string(),
            1,
            "vector".to_string(),
            DistanceType::L2,
            64,
            8,
        );
        let batch = make_batch(&[1, 2, 3]);
        store.append(batch.clone()).unwrap();
        index
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store: store,
                        index_store: Arc::new(index),
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            )
            // A flat query vector (closest to id=1); the facade wraps it into an FSL.
            .nearest(
                "vector",
                &Float32Array::from(vec![0.1f32, 0.2, 0.3, 0.4]),
                3,
            )
            .unwrap();

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids.first().copied(),
            Some(1),
            "nearest neighbor via the facade should be id=1; got {ids:?}"
        );
    }

    /// Vector pagination: `limit(Some(2), Some(1))` over-fetches `k + offset`
    /// per source, then skips `offset` and caps at `limit`. With ids ordered by
    /// distance (id=1 nearest), the page must be the 2nd–3rd nearest (id=2, id=3),
    /// not the 1st–2nd. Covers `plan_vector`'s `per_source_k` + `apply_limit_offset`.
    #[tokio::test]
    async fn nearest_pagination_skips_offset_and_caps_limit() {
        use crate::dataset::{Dataset, WriteParams};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{Float32Array, Int32Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field};
        use lance_index::IndexType;

        let schema = pk_schema_with(Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ));
        let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        let ids: Vec<i32> = (1..=6).collect();
        for id in &ids {
            let base = *id as f32 * 0.1;
            for d in 0..4 {
                vb.values().append_value(base + d as f32 * 0.1);
            }
            vb.append(true);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.clone())),
                Arc::new(vb.finish()),
            ],
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        // `ivf_flat(1)` is exhaustive within its single partition, so ordering is exact.
        let mut base = Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();
        let ivf_flat = VectorIndexParams::ivf_flat(1, DistanceType::L2);
        base.create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base = Arc::new(base);

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .nearest(
                "vector",
                &Float32Array::from(vec![0.1f32, 0.2, 0.3, 0.4]),
                2,
            )
            .unwrap()
            .limit(Some(2), Some(1))
            .unwrap();

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut out: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        out.sort();
        assert_eq!(
            out,
            vec![2, 3],
            "offset=1, limit=2 over k=2 must return the 2nd-3rd nearest (id=2, id=3); got {out:?}"
        );
    }

    /// FTS pagination: `limit(Some(1), Some(1))` fetches `limit + offset` per
    /// source then skips `offset`. With BM25 ranking id=1 > id=2 > id=3 (by doc
    /// length), the page must be the 2nd-ranked hit (id=2). Covers `plan_fts`'s
    /// `k = limit + offset` + `apply_limit_offset`.
    #[tokio::test]
    async fn full_text_search_pagination_skips_offset_and_caps_limit() {
        use crate::dataset::{Dataset, WriteParams};
        use crate::index::DatasetIndexExt;
        use arrow_array::{Int32Array, RecordBatchIterator, StringArray};
        use arrow_schema::{DataType, Field};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = pk_schema_with(Field::new("text", DataType::Utf8, true));
        // Shorter docs score higher under BM25 length normalization, so the
        // ranking is deterministically id=1 > id=2 > id=3.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "lance",
                    "lance filler",
                    "lance filler filler",
                ])),
            ],
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut base = Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();
        base.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let base = Arc::new(Dataset::open(&uri).await.unwrap());

        let query_limited = LsmScanner::new(base.clone(), vec![], vec!["id".to_string()])
            .full_text_search(
                FullTextSearchQuery::new("lance".to_string())
                    .with_column("text".to_string())
                    .unwrap()
                    .limit(Some(1)),
            )
            .unwrap();
        let batches: Vec<RecordBatch> = query_limited
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let out: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            out,
            vec![1],
            "query-level FTS limit=1 must cap the unpaginated scanner result; got {out:?}"
        );

        let query_limited_with_offset =
            LsmScanner::new(base.clone(), vec![], vec!["id".to_string()])
                .full_text_search(
                    FullTextSearchQuery::new("lance".to_string())
                        .with_column("text".to_string())
                        .unwrap()
                        .limit(Some(2)),
                )
                .unwrap()
                .limit(None, Some(1))
                .unwrap();
        let batches: Vec<RecordBatch> = query_limited_with_offset
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let out: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            out,
            vec![2],
            "query-level FTS limit=2 plus offset=1 must page within the top 2; got {out:?}"
        );

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .full_text_search(
                FullTextSearchQuery::new("lance".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap()
            .limit(Some(1), Some(1))
            .unwrap();

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let out: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            out,
            vec![2],
            "offset=1, limit=1 must return the 2nd-ranked hit (id=2); got {out:?}"
        );
    }

    #[tokio::test]
    async fn full_text_search_without_limit_returns_all_matches() {
        use crate::dataset::{Dataset, WriteParams};
        use crate::index::DatasetIndexExt;
        use arrow_array::{Int32Array, RecordBatchIterator, StringArray};
        use arrow_schema::{DataType, Field};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = pk_schema_with(Field::new("text", DataType::Utf8, true));
        let ids: Vec<i32> = (0..12).collect();
        let texts: Vec<&str> = (0..12).map(|_| "lance").collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
        )
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let mut base = Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();
        base.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let base = Arc::new(Dataset::open(&uri).await.unwrap());

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .full_text_search(
                FullTextSearchQuery::new("lance".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 12,
            "unbounded LSM FTS must not apply the old default top-10 cap"
        );
    }

    /// Setting both `nearest` and `full_text_search` must error rather than
    /// silently running vector search and dropping the full-text query (the LSM
    /// path has no hybrid mode).
    #[tokio::test]
    async fn combined_vector_and_fts_is_rejected() {
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{Float32Array, Int32Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field};

        let schema = pk_schema_with(Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ));
        let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for d in 0..4 {
            vb.values().append_value(d as f32);
        }
        vb.append(true);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])), Arc::new(vb.finish())],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let base = Arc::new(
            Dataset::write(reader, &uri, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .nearest(
                "vector",
                &Float32Array::from(vec![0.0f32, 1.0, 2.0, 3.0]),
                1,
            )
            .unwrap()
            .full_text_search(
                FullTextSearchQuery::new("lance".to_string())
                    .with_column("vector".to_string())
                    .unwrap(),
            )
            .unwrap();
        let err = scanner.create_plan().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("combined vector and full-text search"),
            "expected combined-search rejection, got: {err}"
        );
    }

    /// A multi-row query vector must be rejected, not silently flow through the
    /// single-vector LSM merge as wrong results.
    #[tokio::test]
    async fn multi_row_query_vector_is_rejected() {
        use crate::dataset::{Dataset, WriteParams};
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_array::{Int32Array, RecordBatchIterator};
        use arrow_schema::{DataType, Field};

        let schema = pk_schema_with(Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ));
        let mut col = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for d in 0..4 {
            col.values().append_value(d as f32);
        }
        col.append(true);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])), Arc::new(col.finish())],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let base = Arc::new(
            Dataset::write(reader, &uri, Some(WriteParams::default()))
                .await
                .unwrap(),
        );

        // A two-row FSL query vector.
        let mut q = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for _ in 0..2 {
            for d in 0..4 {
                q.values().append_value(d as f32);
            }
            q.append(true);
        }
        let query = q.finish();

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .nearest("vector", &query, 1)
            .unwrap();
        let err = scanner.create_plan().await.unwrap_err();
        assert!(
            err.to_string().contains("single query vector"),
            "expected single-query-vector rejection, got: {err}"
        );
    }

    /// `LsmScanner::full_text_search(query).create_plan()` must route through the
    /// FTS planner and surface a memtable-only match.
    #[tokio::test]
    async fn full_text_search_dispatches_through_facade() {
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::dataset::{Dataset, WriteParams};
        use crate::index::DatasetIndexExt;
        use arrow_array::{Int32Array, RecordBatchIterator, StringArray};
        use arrow_schema::{DataType, Field};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = pk_schema_with(Field::new("text", DataType::Utf8, true));
        let make_batch = |ids: &[i32], texts: &[&str]| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids.to_vec())),
                    Arc::new(StringArray::from(texts.to_vec())),
                ],
            )
            .unwrap()
        };

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!("{}/base", tmp.path().to_str().unwrap());
        let reader =
            RecordBatchIterator::new(vec![Ok(make_batch(&[1], &["alpha"]))], schema.clone());
        let mut base = Dataset::write(reader, &uri, Some(WriteParams::default()))
            .await
            .unwrap();
        base.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let base = Arc::new(Dataset::open(&uri).await.unwrap());

        let store = Arc::new(BatchStore::with_capacity(16));
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        index.add_fts("text_fts".to_string(), 1, "text".to_string());
        let batch = make_batch(&[99], &["zebra"]);
        store.append(batch.clone()).unwrap();
        index
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store: store,
                        index_store: Arc::new(index),
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            )
            .full_text_search(
                FullTextSearchQuery::new("zebra".to_string())
                    .with_column("text".to_string())
                    .unwrap(),
            )
            .unwrap();

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "facade FTS should surface the memtable 'zebra' row"
        );
    }

    #[tokio::test]
    async fn full_text_search_supports_nested_list_element_path() {
        let doc_fields = Fields::from(vec![Field::new("content", DataType::Utf8, true)]);
        let doc_values = StructArray::new(
            doc_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["alpha", "beta", "alpha"]))],
            None,
        );
        let doc_item = Arc::new(Field::new("item", DataType::Struct(doc_fields), true));
        let docs = ListArray::new(
            doc_item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 3])),
            Arc::new(doc_values),
            None,
        );
        let group_fields = Fields::from(vec![Field::new("docs", DataType::List(doc_item), true)]);
        let group_values = StructArray::new(group_fields.clone(), vec![Arc::new(docs)], None);
        let group_item = Arc::new(Field::new("item", DataType::Struct(group_fields), true));
        let groups = ListArray::new(
            group_item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2])),
            Arc::new(group_values),
            None,
        );
        let schema = pk_schema_with(Field::new("groups", groups.data_type().clone(), true));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![7])), Arc::new(groups)],
        )
        .unwrap();

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes
            .add_fts_with_params(
                "content_list_element_fts".to_string(),
                1,
                "groups.docs.content".to_string(),
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            )
            .unwrap();
        let (batch_position, row_offset, _) = batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, row_offset, Some(batch_position))
            .unwrap();

        let scanner = LsmScanner::without_base_table(
            schema.clone(),
            "memory://nested_fts",
            vec![],
            vec!["id".to_string()],
        )
        .with_in_memory_memtables(
            Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: Arc::new(indexes),
                    schema,
                    generation: 1,
                },
                frozen: vec![],
            },
        )
        .project(&["id"])
        .unwrap()
        .full_text_search(
            FullTextSearchQuery::new("alpha".to_string())
                .with_column("groups.docs.content".to_string())
                .unwrap(),
        )
        .unwrap();

        let batch = scanner.try_into_batch().await.unwrap();
        let ids = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
        let coordinates = batch[DOC_INDEX_COL]
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let mut hits = (0..batch.num_rows())
            .map(|row| {
                let coordinate = coordinates
                    .value(row)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .values()
                    .to_vec();
                (ids.value(row), coordinate)
            })
            .collect::<Vec<_>>();
        hits.sort_unstable();
        assert_eq!(hits, vec![(7, vec![0, 0]), (7, vec![1, 1])]);
    }

    /// Empty search results must still preserve the execution plan schema.
    #[tokio::test]
    async fn try_into_batch_empty_fts_keeps_score_schema() {
        use arrow_schema::{DataType, Field};

        let schema = pk_schema_with(Field::new("text", DataType::Utf8, true));
        let scanner = LsmScanner::without_base_table(
            schema,
            "memory://empty",
            vec![],
            vec!["id".to_string()],
        )
        .full_text_search(
            FullTextSearchQuery::new("missing".to_string())
                .with_column("text".to_string())
                .unwrap(),
        )
        .unwrap();

        let batch = scanner.try_into_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(
            batch.schema().field_with_name("_score").is_ok(),
            "empty FTS batch must keep the planned _score column"
        );
    }

    /// One active memtable with a maintained BTree on `id`, all rows visible.
    fn mk_indexed_memtable(schema: &SchemaRef, ids: &[i32], names: &[&str]) -> InMemoryMemTableRef {
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use arrow_array::{Int32Array, StringArray};

        let store = BatchStore::with_capacity(8);
        let mut index = IndexStore::new();
        index.add_btree("id_idx".to_string(), 0, "id".to_string());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap();
        let (idx, row_offset, _) = store.append(batch.clone()).unwrap();
        index
            .insert_with_batch_position(&batch, row_offset, Some(idx))
            .unwrap();
        InMemoryMemTableRef {
            batch_store: Arc::new(store),
            index_store: Arc::new(index),
            schema: schema.clone(),
            generation: 1,
        }
    }

    #[tokio::test]
    async fn point_lookup_filter_routes_to_fast_path() {
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::physical_plan::displayable;
        use datafusion::prelude::{SessionContext, col, lit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let memtable = mk_indexed_memtable(&schema, &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
        let shard = Uuid::new_v4();
        let scanner = || {
            LsmScanner::without_base_table(
                schema.clone(),
                "memory://t",
                vec![],
                vec!["id".to_string()],
            )
            .with_in_memory_memtables(
                shard,
                InMemoryMemTables {
                    active: memtable.clone(),
                    frozen: vec![],
                },
            )
        };
        let ctx = SessionContext::new();
        let count = |plan: Arc<dyn ExecutionPlan>| {
            let ctx = ctx.clone();
            async move {
                let rows: Vec<RecordBatch> = plan
                    .execute(0, ctx.task_ctx())
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                rows.iter().map(|b| b.num_rows()).sum::<usize>()
            }
        };
        let collect_ids = |plan: Arc<dyn ExecutionPlan>| {
            let ctx = ctx.clone();
            async move {
                let rows: Vec<RecordBatch> = plan
                    .execute(0, ctx.task_ctx())
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let mut ids = Vec::new();
                for batch in rows {
                    let id_array = batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .unwrap();
                    ids.extend(id_array.values().iter().copied());
                }
                ids.sort_unstable();
                ids
            }
        };

        // `id = 2` routes to the direct point-lookup node (OneShotStream), not the
        // union/dedup scan, and returns the one matching row.
        let plan = scanner()
            .filter_expr(col("id").eq(lit(2i32)))
            .create_plan()
            .await
            .unwrap();
        let disp = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(disp.contains("OneShotStream"), "pk=lit must route: {disp}");
        assert!(
            !disp.contains("Union"),
            "must not use the union path: {disp}"
        );
        assert_eq!(count(plan).await, 1);

        // `id IN (1, 3)` routes and returns both rows.
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![lit(1i32), lit(3i32)], false))
            .create_plan()
            .await
            .unwrap();
        assert!(
            format!("{}", displayable(plan.as_ref()).indent(true)).contains("OneShotStream"),
            "pk IN (..) must route"
        );
        assert_eq!(count(plan).await, 2);

        // Duplicate literals retain predicate semantics: each matching row is
        // emitted once, including when a limit would otherwise hide a distinct
        // match behind the duplicate.
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![lit(1i32), lit(1i32), lit(2i32)], false))
            .limit(Some(2), None)
            .unwrap()
            .create_plan()
            .await
            .unwrap();
        assert_eq!(collect_ids(plan).await, vec![1, 2]);

        // Distinct literal types can coerce to the same primary-key value and
        // must share one deduplication identity before the limit is applied.
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![lit(1i32), lit(1i64), lit(2i32)], false))
            .limit(Some(2), None)
            .unwrap()
            .create_plan()
            .await
            .unwrap();
        assert_eq!(collect_ids(plan).await, vec![1, 2]);

        // Coercible literals use the per-key fallback and must have the same
        // set semantics as exact-type keys.
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![lit(1i64), lit(1i64), lit(3i64)], false))
            .create_plan()
            .await
            .unwrap();
        assert_eq!(collect_ids(plan).await, vec![1, 3]);

        // NULL literals cannot match, and duplicate NULLs must not affect the
        // non-NULL matches.
        let null = lit(ScalarValue::Int32(None));
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![null.clone(), lit(2i32), null], false))
            .create_plan()
            .await
            .unwrap();
        assert_eq!(collect_ids(plan).await, vec![2]);

        // A range filter is NOT a point lookup → falls through to the scan path.
        let plan = scanner()
            .filter_expr(col("id").gt(lit(2i32)))
            .create_plan()
            .await
            .unwrap();
        assert!(
            !format!("{}", displayable(plan.as_ref()).indent(true)).contains("OneShotStream"),
            "range filter must not route to the point-lookup node"
        );
        assert_eq!(count(plan).await, 3); // 3,4,5

        // Offset changes the semantics, so even a point-lookup-shaped filter
        // must bypass the direct lookup route and use the general scan path.
        let plan = scanner()
            .filter_expr(col("id").in_list(vec![lit(1i32), lit(3i32), lit(5i32)], false))
            .limit(None, Some(1))
            .unwrap()
            .create_plan()
            .await
            .unwrap();
        let disp = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            !disp.contains("OneShotStream"),
            "offset point-lookup filters must use the scan path: {disp}"
        );
        assert_eq!(collect_ids(plan).await, vec![3, 5]);
    }
}
