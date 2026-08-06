// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Point lookup planner for LSM scanner.
//!
//! Provides efficient primary key-based point lookups across LSM levels.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion::common::ScalarValue;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{Column, Literal, NotExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{Expr, SessionContext};
use futures::TryStreamExt;
use lance_core::utils::bloomfilter::sbbf::Sbbf;
use lance_core::{Result, is_system_column};
use lance_datafusion::exec::OneShotExec;
use tracing::instrument;

use crate::dataset::mem_wal::TOMBSTONE;
use crate::dataset::mem_wal::index::IndexStore;
use crate::dataset::mem_wal::memtable::batch_store::BatchStore;

use super::collector::LsmDataSourceCollector;
use super::data_source::LsmDataSource;
use super::exec::{BloomFilterGuardExec, CoalesceFirstExec, compute_pk_hash_from_scalars};
use super::projection::{
    DISTANCE_COLUMN, build_scanner_projection, canonical_output_schema, null_columns,
    project_to_canonical, validate_projection_names, wants_row_address, wants_row_id,
};
use super::sstable_cache::{DatasetCache, SsTableWarmer, open_sstable};
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// Plans point lookup queries over LSM data.
///
/// Point lookups are optimized for primary key-based queries where we expect
/// to find at most one row. The query plan uses:
///
/// 1. **Bloom filter guards**: Skip generations that definitely don't contain the key
/// 2. **Short-circuit evaluation**: Stop after finding the first match
/// 3. **Newest-first ordering**: Check newer generations before older ones
///
/// # Query Plan Structure
///
/// Since data is stored in reverse order (newest first), we use `GlobalLimitExec`
/// with limit=1 to take the first (most recent) matching row.
///
/// ```text
/// CoalesceFirstExec: return_first_non_null
///   BloomFilterGuardExec: gen=3
///     GlobalLimitExec: limit=1
///       FilterExec: pk = target
///         ScanExec: memtable_gen_3
///   BloomFilterGuardExec: gen=2
///     GlobalLimitExec: limit=1
///       FilterExec: pk = target
///         ScanExec: flushed_gen_2
///   BloomFilterGuardExec: gen=1
///     GlobalLimitExec: limit=1
///       FilterExec: pk = target
///         ScanExec: flushed_gen_1
///   GlobalLimitExec: limit=1
///     FilterExec: pk = target
///       ScanExec: base_table
/// ```
///
/// The base table doesn't use a bloom filter guard because:
/// - It's the fallback when no memtable has the key
/// - Bloom filters for the base table would be too large
pub struct LsmPointLookupPlanner {
    /// Data source collector.
    collector: LsmDataSourceCollector,
    /// Primary key column names.
    pk_columns: Vec<String>,
    /// Schema of the base table.
    base_schema: SchemaRef,
    /// Bloom filters for each memtable generation.
    /// Map: generation -> bloom filter
    bloom_filters: std::collections::HashMap<u64, Arc<Sbbf>>,
    /// Session threaded into SSTable opens (shared caches).
    session: Option<Arc<Session>>,
    /// Store params for opening SSTables, reusing the base dataset's store.
    store_params: Option<ObjectStoreParams>,
    /// Cache of opened SSTable datasets.
    sstable_cache: Option<Arc<dyn DatasetCache>>,
    /// Optional warmer fired on first open of an SSTable.
    warmer: Option<Arc<dyn SsTableWarmer>>,
    /// Precomputed canonical output schema for the no-projection case, so the
    /// hot `lookup(.., None)` path clones an `Arc` instead of rebuilding the
    /// schema on every call.
    none_target: SchemaRef,
    /// Shared DataFusion task context for plan execution. Built once and reused
    /// across lookups: `SessionContext::new()` per lookup is a real fixed cost
    /// on the plan fallback path (the part of point-lookup latency that doesn't
    /// scale with generation count).
    task_ctx: Arc<TaskContext>,
}

impl LsmPointLookupPlanner {
    /// Create a new planner.
    ///
    /// # Arguments
    ///
    /// * `collector` - Data source collector
    /// * `pk_columns` - Primary key column names
    /// * `base_schema` - Schema of the base table
    pub fn new(
        collector: LsmDataSourceCollector,
        pk_columns: Vec<String>,
        base_schema: SchemaRef,
    ) -> Self {
        let none_target = canonical_output_schema(None, &base_schema, &pk_columns, false);
        Self {
            collector,
            pk_columns,
            base_schema,
            bloom_filters: std::collections::HashMap::new(),
            session: None,
            store_params: None,
            sstable_cache: None,
            warmer: None,
            none_target,
            task_ctx: SessionContext::new().task_ctx(),
        }
    }

    /// Set the session used to open SSTables.
    pub fn with_session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set the store params used to open SSTables.
    pub fn with_store_params(mut self, store_params: ObjectStoreParams) -> Self {
        self.store_params = Some(store_params);
        self
    }

    /// Inject a cache of opened SSTable datasets, making repeated
    /// lookups against the same generation a pure `Arc::clone`. Populate it up
    /// front during scan setup via
    /// [`DatasetMemWalExt::prewarm_mem_wal`](crate::dataset::mem_wal::DatasetMemWalExt::prewarm_mem_wal)
    /// so the first gen-key lookup does not pay the dataset open.
    pub fn with_sstable_cache(mut self, cache: Arc<dyn DatasetCache>) -> Self {
        self.sstable_cache = Some(cache);
        self
    }

    /// Inject the warmer fired on first open of an SSTable.
    pub fn with_warmer(mut self, warmer: Arc<dyn SsTableWarmer>) -> Self {
        self.warmer = Some(warmer);
        self
    }

    /// Add a bloom filter for a generation.
    ///
    /// Bloom filters are optional but improve performance by skipping
    /// generations that definitely don't contain the target key.
    pub fn with_bloom_filter(mut self, generation: u64, bloom_filter: Arc<Sbbf>) -> Self {
        self.bloom_filters.insert(generation, bloom_filter);
        self
    }

    /// Add multiple bloom filters.
    pub fn with_bloom_filters(
        mut self,
        bloom_filters: impl IntoIterator<Item = (u64, Arc<Sbbf>)>,
    ) -> Self {
        self.bloom_filters.extend(bloom_filters);
        self
    }

    /// Create a point lookup plan for the given primary key values.
    ///
    /// # Arguments
    ///
    /// * `pk_values` - Primary key values to look up (one value per pk column)
    /// * `projection` - Columns to include in output (None = all columns)
    ///
    /// # Returns
    ///
    /// An execution plan that returns at most one row - the newest version
    /// of the row with the given primary key.
    #[instrument(name = "lsm_point_lookup", level = "debug", skip_all, fields(pk_column_count = self.pk_columns.len()))]
    pub async fn plan_lookup(
        &self,
        pk_values: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.plan_lookup_coalesced(pk_values, projection).await? {
            // Tombstones are dropped AFTER the coalesce, never per-source: the
            // tombstone wins the newest-first coalesce (its source is the newest
            // non-empty arm), so filtering it here yields "not found". Filtering
            // per-source would empty the newest arm and let `CoalesceFirstExec`
            // fall through to an older arm — resurrecting the deleted row. Then
            // the carried `_tombstone` column is projected away.
            Some(coalesced) => {
                let canonical =
                    canonical_output_schema(projection, &self.base_schema, &self.pk_columns, false);
                filter_tombstones_after_coalesce(coalesced, &canonical)
            }
            None => self.empty_plan(projection),
        }
    }

    /// Build the coalesced point-lookup plan: each source scanned newest-first,
    /// unioned under `CoalesceFirstExec`, output in the *carry* schema (canonical
    /// output + the `_tombstone` marker). `None` when there are no sources at
    /// all. [`Self::plan_lookup`] drops the tombstone on top of this;
    /// [`Self::lookup_keep_tombstone`] keeps it so partial-update merge can tell
    /// a fresh-deleted key from an absent one.
    async fn plan_lookup_coalesced(
        &self,
        pk_values: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        validate_projection_names(projection, &self.base_schema, &[])?;
        if pk_values.len() != self.pk_columns.len() {
            return Err(lance_core::Error::invalid_input(format!(
                "Expected {} primary key values, got {}",
                self.pk_columns.len(),
                pk_values.len()
            )));
        }

        let pk_hash = compute_pk_hash_from_scalars(pk_values);
        let filter_expr = self.build_pk_filter_expr(pk_values)?;
        let sources = self.collector.collect()?;

        if sources.is_empty() {
            return Ok(None);
        }

        // Sort by generation DESC (newest first)
        let mut sources: Vec<_> = sources.into_iter().collect();
        sources.sort_by_key(|b| std::cmp::Reverse(b.generation()));

        let mut source_plans = Vec::new();

        for source in sources {
            let generation = source.generation().as_u64();

            let scan = self
                .build_source_scan(&source, projection, &filter_expr)
                .await?;

            // Data is stored in reverse order, so first match is newest
            let limited: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(scan, 0, Some(1)));

            let guarded_plan: Arc<dyn ExecutionPlan> =
                if let Some(bf) = self.bloom_filters.get(&generation) {
                    Arc::new(BloomFilterGuardExec::new(
                        limited,
                        bf.clone(),
                        pk_hash,
                        generation,
                    ))
                } else {
                    limited
                };

            source_plans.push(guarded_plan);
        }

        // Always coalesce, even for a single source: besides picking the newest
        // non-empty arm, `CoalesceFirstExec` normalizes child statistics to a
        // schema-sized "unknown", which the downstream tombstone `FilterExec`
        // needs (the in-memory mem_wal execs report empty column statistics, and
        // datafusion's projection statistics would index out of bounds without
        // this normalization).
        Ok(Some(Arc::new(CoalesceFirstExec::new(source_plans))))
    }

    /// Like [`Self::lookup`] but does NOT drop a tombstone: the returned 1-row
    /// batch carries the `_tombstone` marker (`true` ⇒ the key's newest fresh
    /// version is a delete); `None` still means the key is absent from every
    /// source. Partial-update merge uses this to treat a fresh-deleted PK as
    /// absent, so it never resurrects stale, not-yet-compacted base columns.
    /// Always plans (no in-memory fast path) — used off the hot read path, on
    /// small partial-update batches.
    pub async fn lookup_keep_tombstone(
        &self,
        pk_values: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Option<RecordBatch>> {
        let Some(plan) = self.plan_lookup_coalesced(pk_values, projection).await? else {
            return Ok(None);
        };
        let batches: Vec<RecordBatch> = plan
            .execute(0, self.task_ctx.clone())?
            .try_collect()
            .await?;
        for batch in batches {
            if batch.num_rows() > 0 {
                return Ok(Some(batch.slice(0, 1)));
            }
        }
        Ok(None)
    }

    /// Tombstone-preserving [`Self::lookup_many`] for single- or multi-column
    /// keys: resolves each key with [`Self::lookup_keep_tombstone`] and
    /// concatenates the hits in the carry schema (canonical output +
    /// `_tombstone`); keys absent from every source are omitted. Per-key (no
    /// batched fast path) — the partial-update batches that use it are small.
    pub async fn lookup_many_keep_tombstone(
        &self,
        keys: &[Vec<ScalarValue>],
        projection: Option<&[String]>,
    ) -> Result<RecordBatch> {
        let canonical =
            canonical_output_schema(projection, &self.base_schema, &self.pk_columns, false);
        let target = carry_schema(&canonical);
        let mut out: Vec<RecordBatch> = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(b) = self.lookup_keep_tombstone(key, projection).await? {
                out.push(b);
            }
        }
        match out.len() {
            0 => Ok(RecordBatch::new_empty(target)),
            1 => Ok(out.pop().unwrap()),
            _ => Ok(arrow_select::concat::concat_batches(&target, &out)?),
        }
    }

    /// Resolve a single-row point lookup, returning the newest matching row (a
    /// 1-row batch with the canonical output schema) or `None`.
    ///
    /// For a single-column primary key this probes the in-memory memtables'
    /// BTree index directly — no DataFusion plan — newest generation first, and
    /// returns on the first hit. Only when the lookup must consult an on-disk
    /// source (an SSTable or the base table), a memtable lacks a
    /// BTree on the key, the key is multi-column, or the projection requests
    /// system columns does it fall back to [`Self::plan_lookup`]. The result is
    /// identical to executing `plan_lookup` and taking the first row; the fast
    /// path just skips the per-lookup plan/stream construction that dominates
    /// point-lookup latency.
    #[instrument(name = "lsm_lookup", level = "debug", skip_all)]
    pub async fn lookup(
        &self,
        pk_values: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Option<RecordBatch>> {
        // Fast path: exactly one key value (which must match the single PK
        // column), the key's scalar type exactly matches the PK column's Arrow
        // type, and no system columns in the output. The length check is first
        // so `pk_values[0]` is only indexed once it is known to exist (an empty
        // slice falls through to the plan path, which returns a clean
        // `invalid_input` error rather than panicking). The exact-type
        // requirement avoids the `OrderableScalarValue` panic on comparing
        // mismatched variants — the plan path coerces, so a coercible-but-
        // different literal (e.g. `Int64` for an `Int32` PK) falls back.
        let fast_eligible = pk_values.len() == 1
            && self.pk_columns.len() == 1
            && self
                .base_schema
                .field_with_name(&self.pk_columns[0])
                .ok()
                .map(|f| f.data_type() == &pk_values[0].data_type())
                .unwrap_or(false);
        if fast_eligible {
            // Borrow the cached schema for the common `None` case (no `Arc`
            // clone — the clone would contend on a shared refcount under
            // concurrency); only an explicit projection builds a fresh schema.
            let projected;
            let target: &SchemaRef = match projection {
                None => &self.none_target,
                Some(_) => {
                    projected = canonical_output_schema(
                        projection,
                        &self.base_schema,
                        &self.pk_columns,
                        false,
                    );
                    &projected
                }
            };
            if !target.fields().iter().any(|f| is_system_column(f.name())) {
                // Probe in-memory memtables newest-first *by reference* (no
                // source `Arc` clones / allocation in the single-memtable case),
                // so concurrent readers don't contend on source refcounts.
                let outcome = self.collector.find_in_memory_newest_first(
                    |m| -> Result<Option<FastOutcome>> {
                        match probe_memtable(
                            &m.batch_store,
                            &m.index_store,
                            &self.pk_columns[0],
                            &pk_values[0],
                            target,
                        )? {
                            Probe::Hit(batch) => Ok(Some(FastOutcome::Hit(batch))),
                            Probe::Deleted => Ok(Some(FastOutcome::Deleted)),
                            Probe::Miss => Ok(None),
                            Probe::NoIndex => Ok(Some(FastOutcome::NeedsFallback)),
                        }
                    },
                )?;
                match outcome {
                    Some(FastOutcome::Hit(batch)) => return Ok(Some(batch)),
                    // Newest version is a tombstone → deleted; do not consult
                    // older (on-disk) sources.
                    Some(FastOutcome::Deleted) => return Ok(None),
                    Some(FastOutcome::NeedsFallback) => { /* fall through to plan */ }
                    None => {
                        // Every in-memory memtable missed. If there is no
                        // on-disk source, the key does not exist; otherwise the
                        // plan path consults the base table / SSTables.
                        if !self.collector.has_on_disk_sources() {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        self.lookup_via_plan(pk_values, projection).await
    }

    /// Fallback: build and execute the DataFusion plan, returning its first row.
    async fn lookup_via_plan(
        &self,
        pk_values: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Option<RecordBatch>> {
        let plan = self.plan_lookup(pk_values, projection).await?;
        let batches: Vec<RecordBatch> = plan
            .execute(0, self.task_ctx.clone())?
            .try_collect()
            .await?;
        for batch in batches {
            if batch.num_rows() > 0 {
                return Ok(Some(batch.slice(0, 1)));
            }
        }
        Ok(None)
    }

    /// Resolve many single-column keys in one pass, returning the found rows
    /// (newest visible per key) as a single `RecordBatch` in the canonical
    /// output schema. Missing keys are omitted; row order is not guaranteed to
    /// match the input (a set result, like the scan path). Amortizes per-call
    /// overhead and gathers rows columnar (one vectorized `take` per source
    /// batch). Equivalent to N× [`Self::lookup`], minus the per-key plan/stream.
    #[instrument(name = "lsm_lookup_many", level = "debug", skip_all, fields(n = keys.len()))]
    pub async fn lookup_many(
        &self,
        keys: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<RecordBatch> {
        let target = match projection {
            None => self.none_target.clone(),
            Some(_) => {
                canonical_output_schema(projection, &self.base_schema, &self.pk_columns, false)
            }
        };
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(target));
        }
        // One key: the batch grouping (refs vec + hash map + pending) has
        // nothing to amortize, so it's pure overhead — delegate to the cheaper
        // single-lookup path. Keeps a one-element `lookup_many` (e.g. a routed
        // `pk IN (x)`) as fast as `lookup`.
        if keys.len() == 1 {
            return Ok(self
                .lookup(keys, projection)
                .await?
                .unwrap_or_else(|| RecordBatch::new_empty(target)));
        }

        // Fast path: single pk column, every key matches the pk Arrow type, no
        // system columns in the output. Otherwise the per-key path (correct for
        // multi-column keys, coercible types, system-column projections).
        let pk_type = self
            .pk_columns
            .first()
            .and_then(|c| self.base_schema.field_with_name(c).ok())
            .map(|f| f.data_type().clone());
        let fast_eligible = self.pk_columns.len() == 1
            && !target.fields().iter().any(|f| is_system_column(f.name()))
            && pk_type
                .as_ref()
                .map(|t| keys.iter().all(|k| &k.data_type() == t))
                .unwrap_or(false);
        if !fast_eligible {
            return self
                .lookup_many_via_per_key(keys, projection, &target)
                .await;
        }

        let pk_col = &self.pk_columns[0];
        let refs = self.collector.in_memory_refs_newest_first();
        // Hits grouped by (memtable index, batch index) so each source batch is
        // gathered with a single `take`.
        let mut hits: HashMap<(usize, usize), Vec<u32>> = HashMap::new();
        let mut pending: Vec<ScalarValue> = Vec::new();
        for key in keys {
            let mut resolved = false;
            for (ri, m) in refs.iter().enumerate() {
                match probe_position(&m.batch_store, &m.index_store, pk_col, key)? {
                    ProbePos::Found { batch_idx, row } => {
                        // Newest version is a tombstone → the key is deleted:
                        // resolve it as a miss (emit nothing) and do not fall
                        // through to an older source.
                        if !is_tombstone_at(&m.batch_store, batch_idx, row)? {
                            hits.entry((ri, batch_idx)).or_default().push(row as u32);
                        }
                        resolved = true;
                        break;
                    }
                    ProbePos::Miss => continue,
                    ProbePos::NoIndex => {
                        // A memtable without the pk BTree can't be batch-probed;
                        // fall back to the fully-correct per-key path.
                        return self
                            .lookup_many_via_per_key(keys, projection, &target)
                            .await;
                    }
                }
            }
            if !resolved {
                pending.push(key.clone());
            }
        }

        let mut out: Vec<RecordBatch> = Vec::with_capacity(hits.len() + 1);
        for ((ri, batch_idx), rows) in hits {
            out.push(gather_rows(
                &refs[ri].batch_store,
                batch_idx,
                &rows,
                &target,
            )?);
        }
        // Keys absent from every in-memory memtable may live on disk; resolve
        // those via the plan path. (All-in-memory hit case: `pending` is empty.)
        if !pending.is_empty() && self.collector.has_on_disk_sources() {
            out.push(
                self.lookup_many_via_per_key(&pending, projection, &target)
                    .await?,
            );
        }

        match out.len() {
            0 => Ok(RecordBatch::new_empty(target)),
            1 => Ok(out.pop().unwrap()),
            _ => Ok(arrow_select::concat::concat_batches(&target, &out)?),
        }
    }

    /// Correctness fallback for [`Self::lookup_many`]: resolve each key with
    /// [`Self::lookup`] and concatenate.
    async fn lookup_many_via_per_key(
        &self,
        keys: &[ScalarValue],
        projection: Option<&[String]>,
        target: &SchemaRef,
    ) -> Result<RecordBatch> {
        let mut out: Vec<RecordBatch> = Vec::new();
        for key in keys {
            if let Some(b) = self.lookup(std::slice::from_ref(key), projection).await? {
                out.push(b);
            }
        }
        match out.len() {
            0 => Ok(RecordBatch::new_empty(target.clone())),
            1 => Ok(out.pop().unwrap()),
            _ => Ok(arrow_select::concat::concat_batches(target, &out)?),
        }
    }

    /// Build a composable one-shot `ExecutionPlan` that yields the point-lookup
    /// result for `keys`, so the LSM scanner can place limit / projection / etc.
    /// on top and use the fast path inside general query execution. A single
    /// key uses [`Self::lookup`]; multiple keys use [`Self::lookup_many`].
    pub async fn plan_point_lookup(
        &self,
        keys: &[ScalarValue],
        projection: Option<&[String]>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = if keys.len() == 1 {
            match self.lookup(keys, projection).await? {
                Some(b) => b,
                None => RecordBatch::new_empty(canonical_output_schema(
                    projection,
                    &self.base_schema,
                    &self.pk_columns,
                    false,
                )),
            }
        } else {
            self.lookup_many(keys, projection).await?
        };
        let schema = batch.schema();
        let stream = futures::stream::once(async move { Ok(batch) });
        let adapter = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Arc::new(OneShotExec::new(Box::pin(adapter))))
    }

    /// Build the filter expression for primary key equality.
    fn build_pk_filter_expr(&self, pk_values: &[ScalarValue]) -> Result<Expr> {
        use datafusion::prelude::{col, lit};

        let mut expr: Option<Expr> = None;

        for (col_name, value) in self.pk_columns.iter().zip(pk_values.iter()) {
            let eq_expr = col(col_name.as_str()).eq(lit(value.clone()));

            expr = Some(match expr {
                Some(e) => e.and(eq_expr),
                None => eq_expr,
            });
        }

        expr.ok_or_else(|| lance_core::Error::invalid_input("No primary key columns specified"))
    }

    /// Build scan plan for a single data source.
    ///
    /// Output is projected to the canonical schema so user-requested system
    /// columns appear at the requested position — NULL where the source
    /// doesn't produce them or where per-source values aren't meaningful.
    async fn build_source_scan(
        &self,
        source: &LsmDataSource,
        projection: Option<&[String]>,
        filter: &Expr,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let cols = build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
        let target =
            canonical_output_schema(projection, &self.base_schema, &self.pk_columns, false);
        let want_row_id = wants_row_id(projection);
        let want_row_addr = wants_row_address(projection);
        let scan: Arc<dyn ExecutionPlan> = match source {
            LsmDataSource::BaseTable { dataset } => {
                let mut scanner = dataset.scan();
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                // Only the base produces row IDs callers can use against the
                // dataset (e.g. `take_rows`); non-base arms NULL via canonical.
                if want_row_id {
                    scanner.with_row_id();
                }
                if want_row_addr {
                    scanner.with_row_address();
                }
                scanner.filter_expr(filter.clone());
                // Box at the call site: `create_plan`'s inlined async layout exceeds
                // rustc's depth limit up this point-lookup chain, and boxing inside
                // `create_plan` instead triggers a `Box<Future>: Send` solver overflow
                // (E0275 downstream). Same for the other arms below.
                Box::pin(scanner.create_plan()).await?
            }
            LsmDataSource::SsTable { path, .. } => {
                let dataset = open_sstable(
                    path,
                    self.session.as_ref(),
                    self.store_params.as_ref(),
                    self.sstable_cache.as_ref(),
                    self.warmer.as_ref(),
                )
                .await?;
                let mut scanner = dataset.scan();
                // Carry `_tombstone` through so the post-coalesce filter can drop
                // a deleted key (gen written before deletes existed lack it →
                // `project_to_carry` synthesizes `false`).
                let cols = cols_with_tombstone(&cols, dataset.schema().field(TOMBSTONE).is_some());
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                scanner.filter_expr(filter.clone());
                Box::pin(scanner.create_plan()).await?
            }
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                schema,
                ..
            } => {
                use crate::dataset::mem_wal::memtable::scanner::MemTableScanner;

                let mut scanner =
                    MemTableScanner::new(batch_store.clone(), index_store.clone(), schema.clone());
                // Carry `_tombstone` through so the post-coalesce filter can drop
                // a deleted key; it survives the sort below.
                let cols = cols_with_tombstone(&cols, schema.column_with_name(TOMBSTONE).is_some());
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                scanner.filter_expr(filter.clone());
                // Expose `_rowid` (the BatchStore row offset, monotonic with
                // insert order) so we can pick the most recently inserted
                // duplicate below. Without this, a `FilterExec → LIMIT 1`
                // over insert-ordered scan would return the *oldest* of
                // multiple rows sharing the target primary key.
                scanner.with_row_id();
                let raw = Box::pin(scanner.create_plan()).await?;
                // The filter already restricts to the exact PK value, so the
                // scan yields that key's insert history. Within the active
                // memtable larger `_rowid` = newer insert, so sorting `_rowid`
                // DESC and keeping the first row picks the newest version — one
                // row per (value-exact) PK.
                let rowid_idx = raw.schema().index_of(lance_core::ROW_ID)?;
                let ordering = LexOrdering::new(vec![PhysicalSortExpr {
                    expr: Arc::new(Column::new(lance_core::ROW_ID, rowid_idx)),
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                }])
                .ok_or_else(|| {
                    lance_core::Error::internal("point-lookup: failed to build _rowid ordering")
                })?;
                let newest: Arc<dyn ExecutionPlan> =
                    Arc::new(SortExec::new(ordering, raw).with_fetch(Some(1)));
                // Per-source `_rowid` would collide with the base table's;
                // NULL it before canonicalization (the value is internal to
                // this arm). project_to_canonical drops it entirely when
                // the user didn't request `_rowid` in the projection.
                null_columns(newest, &[lance_core::ROW_ID])?
            }
        };
        // Output carries `_tombstone` (canonical + the marker) so it survives
        // the union/coalesce to the post-coalesce filter; base / legacy sources
        // that lack the column get a synthesized `false`.
        project_to_carry(scan, &target)
    }

    /// Create an empty execution plan with the canonical output schema.
    fn empty_plan(&self, projection: Option<&[String]>) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

        let schema =
            canonical_output_schema(projection, &self.base_schema, &self.pk_columns, false);
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

/// Append `_tombstone` to a scanner projection when the source carries it, so
/// the column survives to the post-coalesce tombstone filter. Sources without
/// it (base table, generations written before deletes existed) are left alone
/// and have `false` synthesized by [`project_to_carry`].
fn cols_with_tombstone(cols: &[String], present: bool) -> Vec<String> {
    if !present {
        return cols.to_vec();
    }
    let mut out = cols.to_vec();
    if !out.iter().any(|c| c == TOMBSTONE) {
        out.push(TOMBSTONE.to_string());
    }
    out
}

/// Carry schema = canonical output + a trailing non-nullable `_tombstone`
/// Boolean. Non-nullable so the base arm's synthesized `Literal(false)` matches
/// the WAL arms' real column under `CoalesceFirstExec`'s exact-schema check.
fn carry_schema(canonical: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = canonical.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(TOMBSTONE, DataType::Boolean, false)));
    Arc::new(Schema::new(fields))
}

/// Project a source scan to the carry schema: existing columns are forwarded, a
/// missing `_tombstone` becomes `false` (base table / legacy generations carry
/// no tombstones), and missing system / `_distance` columns are NULL-filled
/// (mirroring [`project_to_canonical`]).
fn project_to_carry(
    plan: Arc<dyn ExecutionPlan>,
    canonical: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input = plan.schema();
    let carry = carry_schema(canonical);
    let mut project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(carry.fields().len());
    for field in carry.fields() {
        let name = field.name();
        let expr: Arc<dyn PhysicalExpr> = match input.column_with_name(name) {
            Some((idx, _)) => Arc::new(Column::new(name, idx)),
            None if name == TOMBSTONE => Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
            None if is_system_column(name) => Arc::new(Literal::new(ScalarValue::UInt64(None))),
            None if name == DISTANCE_COLUMN => Arc::new(Literal::new(ScalarValue::Float32(None))),
            None => {
                return Err(lance_core::Error::internal(format!(
                    "Column '{}' missing from point-lookup carry source schema (have: {:?})",
                    name,
                    input
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect::<Vec<_>>()
                )));
            }
        };
        project_exprs.push((expr, name.clone()));
    }
    Ok(Arc::new(
        ProjectionExec::try_new(project_exprs, plan).map_err(|e| {
            lance_core::Error::internal(format!("Failed to build carry ProjectionExec: {}", e))
        })?,
    ))
}

/// Drop tombstone rows after `CoalesceFirstExec` has already picked the newest
/// source, then project the carried `_tombstone` column away (back to the
/// canonical schema).
fn filter_tombstones_after_coalesce(
    plan: Arc<dyn ExecutionPlan>,
    canonical: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let idx = plan.schema().index_of(TOMBSTONE).map_err(|e| {
        lance_core::Error::internal(format!("point-lookup carry plan missing _tombstone: {}", e))
    })?;
    let predicate: Arc<dyn PhysicalExpr> =
        Arc::new(NotExpr::new(Arc::new(Column::new(TOMBSTONE, idx))));
    let filtered: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, plan).map_err(|e| {
            lance_core::Error::internal(format!("Failed to build tombstone FilterExec: {}", e))
        })?);
    project_to_canonical(filtered, canonical)
}

/// Whether the row at `(batch_idx, row)` of `batch_store` is a tombstone.
/// Memtables without the `_tombstone` column (legacy / direct-construction
/// tests) are treated as carrying no tombstones.
fn is_tombstone_at(batch_store: &BatchStore, batch_idx: usize, row: usize) -> Result<bool> {
    let stored = batch_store.get(batch_idx).ok_or_else(|| {
        lance_core::Error::internal("point-lookup: tombstone-check batch missing")
    })?;
    let Some(col) = stored.data.column_by_name(TOMBSTONE) else {
        return Ok(false);
    };
    let arr = col.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
        lance_core::Error::internal("point-lookup: _tombstone column is not Boolean")
    })?;
    Ok(arr.is_valid(row) && arr.value(row))
}

/// Result of probing the in-memory memtables newest-first in `lookup()`.
enum FastOutcome {
    /// A visible row was found; here it is, projected.
    Hit(RecordBatch),
    /// The newest visible version of the key is a tombstone — the key is
    /// deleted. The search stops here (no fall-through to older sources) and
    /// resolves to "not found".
    Deleted,
    /// A memtable could not be probed directly (no BTree on the key) — the
    /// caller must fall back to the plan path.
    NeedsFallback,
}

/// Outcome of a direct BTree probe against one in-memory memtable.
enum Probe {
    /// The key was found; here is the newest visible row, projected.
    Hit(RecordBatch),
    /// The newest visible version of the key is a tombstone — deleted. Stop the
    /// search and return "not found".
    Deleted,
    /// The key is not present in this memtable (but may be in an older source).
    Miss,
    /// This memtable has no BTree on the key column, so it cannot be probed
    /// directly — the caller must fall back to the plan path.
    NoIndex,
}

/// Where a key's newest visible row lives within one in-memory memtable.
enum ProbePos {
    /// Found at `(batch_idx, row_in_batch)` in the memtable's `BatchStore`.
    Found {
        batch_idx: usize,
        row: usize,
    },
    Miss,
    NoIndex,
}

/// Resolve the `(batch_idx, row)` of a key's newest *visible* row in one
/// in-memory memtable via a seek-and-stop on the ordered skiplist
/// (`BTreeMemIndex::get_newest_visible`), honoring the MVCC watermark. No
/// materialization.
fn probe_position(
    batch_store: &BatchStore,
    index_store: &IndexStore,
    pk_column: &str,
    pk_value: &ScalarValue,
) -> Result<ProbePos> {
    // Visible batches are the committed prefix [0, last_visible_idx]; each
    // `StoredBatch` carries its cumulative `row_offset`, so visibility and the
    // position→batch mapping are O(1)/O(log) with no per-probe allocation.
    let len = batch_store.len();
    if len == 0 {
        return Ok(ProbePos::Miss);
    }
    // The cursor is an exclusive count, so the last visible batch sits at
    // `count - 1`. A count of 0 means nothing is visible yet — not "batch 0".
    let visible_count = index_store.visible_count().min(len);
    let Some(last_visible_idx) = visible_count.checked_sub(1) else {
        return Ok(ProbePos::Miss);
    };
    let last = batch_store.get(last_visible_idx).ok_or_else(|| {
        lance_core::Error::internal("point-lookup: visible batch index out of range")
    })?;
    let visible_end = last.row_offset + last.num_rows as u64; // exclusive
    if visible_end == 0 {
        return Ok(ProbePos::Miss);
    }
    let max_visible_row = visible_end - 1;

    // A single-column primary key always has a value-keyed BTree (reused or
    // auto-created — see `IndexStore::enable_pk_index`): collision-free, so one
    // seek yields the answer with no re-check. Absent only when the table has no
    // PK index, where the caller falls back to the plan path.
    let Some(btree) = index_store.get_btree_by_column(pk_column) else {
        return Ok(ProbePos::NoIndex);
    };
    let Some(pos) = btree.get_newest_visible(pk_value, max_visible_row) else {
        return Ok(ProbePos::Miss);
    };
    let (batch_idx, row) = resolve_position(batch_store, last_visible_idx, pos)?;
    Ok(ProbePos::Found { batch_idx, row })
}

/// Map a global row `position` to its `(batch_idx, row_in_batch)` by binary
/// searching the visible batch prefix on cumulative `row_offset` (batches are
/// appended in order).
fn resolve_position(
    batch_store: &BatchStore,
    last_visible_idx: usize,
    position: u64,
) -> Result<(usize, usize)> {
    let (mut lo, mut hi) = (0usize, last_visible_idx);
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let off = batch_store.get(mid).map(|b| b.row_offset).ok_or_else(|| {
            lance_core::Error::internal("point-lookup: batch index out of range during search")
        })?;
        if off <= position {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    let stored = batch_store
        .get(lo)
        .ok_or_else(|| lance_core::Error::internal("point-lookup: resolved batch missing"))?;
    Ok((lo, (position - stored.row_offset) as usize))
}

/// Gather `rows` from `batch_store`'s batch `batch_idx` into the `target`
/// schema. A single row is a zero-copy `slice` (the common point-lookup case);
/// multiple rows use one vectorized `take` per column.
fn gather_rows(
    batch_store: &BatchStore,
    batch_idx: usize,
    rows: &[u32],
    target: &SchemaRef,
) -> Result<RecordBatch> {
    let stored = batch_store
        .get(batch_idx)
        .ok_or_else(|| lance_core::Error::internal("point-lookup: gather batch missing"))?;
    let indices = (rows.len() > 1).then(|| arrow_array::UInt32Array::from(rows.to_vec()));
    // Borrow the stored schema once (no `Arc` clone): `schema()` clones the
    // shared schema `Arc`, and under concurrency that refcount cache line
    // ping-pongs across cores. `schema_ref()` borrows it.
    let stored_schema = stored.data.schema_ref();
    let cols: Vec<Arc<dyn Array>> = target
        .fields()
        .iter()
        .map(|f| {
            let idx = stored_schema.index_of(f.name()).map_err(|_| {
                lance_core::Error::invalid_input(format!(
                    "point-lookup projection column '{}' not found in memtable batch",
                    f.name()
                ))
            })?;
            let col = stored.data.column(idx);
            // Single row: zero-copy `slice` (the common point-lookup case, and
            // measurably faster than `take` — copying regressed single-thread
            // ~30% with no N-thread gain). Multiple rows: one vectorized `take`.
            match &indices {
                None => Ok(col.slice(rows[0] as usize, 1)),
                Some(idxs) => arrow_select::take::take(col.as_ref(), idxs, None)
                    .map_err(lance_core::Error::from),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(target.clone(), cols)?)
}

/// Probe one in-memory memtable for a single key and materialize the newest
/// visible row into `target`. Thin wrapper over [`probe_position`] +
/// [`gather_rows`] used by [`LsmPointLookupPlanner::lookup`].
fn probe_memtable(
    batch_store: &BatchStore,
    index_store: &IndexStore,
    pk_column: &str,
    pk_value: &ScalarValue,
    target: &SchemaRef,
) -> Result<Probe> {
    match probe_position(batch_store, index_store, pk_column, pk_value)? {
        ProbePos::NoIndex => Ok(Probe::NoIndex),
        ProbePos::Miss => Ok(Probe::Miss),
        ProbePos::Found { batch_idx, row } => {
            // The newest visible version is a tombstone → the key is deleted.
            // Stop here rather than materializing or falling through to an
            // older source.
            if is_tombstone_at(batch_store, batch_idx, row)? {
                return Ok(Probe::Deleted);
            }
            Ok(Probe::Hit(gather_rows(
                batch_store,
                batch_idx,
                &[row as u32],
                target,
            )?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::displayable;
    use std::collections::HashMap;
    use uuid::Uuid;

    use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
    use crate::dataset::{Dataset, WriteParams};

    fn create_pk_schema() -> Arc<ArrowSchema> {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_metadata);

        Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &ArrowSchema, ids: &[i32], name_prefix: &str) -> RecordBatch {
        let names: Vec<String> = ids
            .iter()
            .map(|id| format!("{}_{}", name_prefix, id))
            .collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    async fn create_dataset(uri: &str, batches: Vec<RecordBatch>) -> Dataset {
        let schema = batches[0].schema();
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_point_lookup_plan_structure() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Create base table
        let base_uri = format!("{}/base", base_path);
        let base_batch = create_test_batch(&schema, &[1, 2, 3], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        // Create collector without memtables
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema.clone());

        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let plan = planner.plan_lookup(&pk_values, None).await.unwrap();

        // Verify plan structure
        let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));

        // Should have GlobalLimitExec with limit=1 (data is stored in reverse order)
        assert!(
            plan_str.contains("GlobalLimitExec"),
            "Should have GlobalLimitExec in plan: {}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_point_lookup_with_memtables() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Create base table
        let base_uri = format!("{}/base", base_path);
        let base_batch = create_test_batch(&schema, &[1, 2, 3], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        // Create shard snapshot
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let gen1_batch = create_test_batch(&schema, &[2], "gen1"); // Update id=2
        create_dataset(&gen1_uri, vec![gen1_batch]).await;

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        // Create collector
        let collector = LsmDataSourceCollector::new(base_dataset, vec![shard_snapshot]);

        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema.clone());

        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let plan = planner.plan_lookup(&pk_values, None).await.unwrap();

        // Verify plan structure - should have CoalesceFirstExec with multiple children
        let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));

        assert!(
            plan_str.contains("CoalesceFirstExec") || plan_str.contains("GlobalLimitExec"),
            "Should have CoalesceFirstExec or GlobalLimitExec in plan: {}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_point_lookup_with_bloom_filter() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Create base table
        let base_uri = format!("{}/base", base_path);
        let base_batch = create_test_batch(&schema, &[1, 2, 3], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        // Create collector
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        // Create a bloom filter for generation 1 (simulating a memtable)
        let mut bf = Sbbf::with_ndv_fpp(100, 0.01).unwrap();
        let pk_hash = compute_pk_hash_from_scalars(&[ScalarValue::Int32(Some(2))]);
        bf.insert_hash(pk_hash);

        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema.clone())
            .with_bloom_filter(1, Arc::new(bf));

        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let plan = planner.plan_lookup(&pk_values, None).await.unwrap();

        // Plan should be valid
        assert!(plan.schema().field_with_name("id").is_ok());
    }

    #[tokio::test]
    async fn test_pk_filter_expr() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let pk_values = vec![ScalarValue::Int32(Some(42))];
        let expr = planner.build_pk_filter_expr(&pk_values).unwrap();

        // Verify expression is an equality
        let expr_str = format!("{}", expr);
        assert!(
            expr_str.contains("id"),
            "Expression should contain column name"
        );
    }

    #[tokio::test]
    async fn test_point_lookup_without_base_table() {
        use futures::TryStreamExt;

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // No base dataset is created. We still need a base URI so the collector
        // can resolve SSTable paths.
        let base_uri = format!("{}/base", base_path);

        // Create an SSTable under {base_uri}/_mem_wal/{shard}/gen_1
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let gen1_batch = create_test_batch(&schema, &[2, 3], "gen1");
        create_dataset(&gen1_uri, vec![gen1_batch]).await;

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![shard_snapshot]);
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        // id=3 lives in the SSTable
        let pk_values = vec![ScalarValue::Int32(Some(3))];
        let plan = planner.plan_lookup(&pk_values, None).await.unwrap();

        let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            !plan_str.contains("base/data"),
            "Plan must not scan base table, got: {}",
            plan_str
        );
        assert!(plan_str.contains("gen_1"));

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);

        // id=99 doesn't exist anywhere → empty
        let plan = planner
            .plan_lookup(&[ScalarValue::Int32(Some(99))], None)
            .await
            .unwrap();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_point_lookup_projection_with_system_columns() {
        // Regression: system columns in projection used to error in the
        // active-arm MemTableScanner or get silently dropped. Verify they're
        // surfaced at the requested position with the correct NULL/real mix.
        use futures::TryStreamExt;
        use lance_core::is_system_column;

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1, 2, 3], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        // User requests `_rowaddr` between `id` and `name`, plus `_rowoffset` at end.
        let projection = vec![
            "id".to_string(),
            "_rowaddr".to_string(),
            "name".to_string(),
            "_rowoffset".to_string(),
        ];
        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let plan = planner
            .plan_lookup(&pk_values, Some(&projection))
            .await
            .expect("planner must accept system columns in projection");

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "expected exactly one matching row");

        let out_schema = batches[0].schema();
        let out_cols: Vec<String> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            out_cols,
            vec![
                "id".to_string(),
                "_rowaddr".to_string(),
                "name".to_string(),
                "_rowoffset".to_string(),
            ],
            "system columns must appear at the user's requested position"
        );

        // Hit row is from base → `_rowaddr` is real. `_rowoffset` stays
        // NULL (no scanner produces it).
        // (Test 5 — empty-plan with system columns — lives in the next
        // test below.)
        let rowaddr = batches[0].column_by_name("_rowaddr").unwrap();
        assert!(
            !rowaddr.is_null(0),
            "_rowaddr from base should be populated, got: {:?}",
            rowaddr
        );
        let rowoffset = batches[0].column_by_name("_rowoffset").unwrap();
        assert!(is_system_column("_rowoffset"));
        assert!(
            rowoffset.is_null(0),
            "_rowoffset has no per-source flag, must be NULL across LSM, got: {:?}",
            rowoffset
        );
    }

    #[tokio::test]
    async fn test_point_lookup_empty_plan_with_system_columns() {
        // Test 5 (point_lookup slice): with no sources, the empty plan
        // must still expose user-requested system columns at the
        // requested position.
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let projection = vec![
            "id".to_string(),
            "_rowaddr".to_string(),
            "name".to_string(),
            "_rowid".to_string(),
        ];
        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let plan = planner
            .plan_lookup(&pk_values, Some(&projection))
            .await
            .expect("empty plan must accept system columns in projection");

        let names: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec![
                "id".to_string(),
                "_rowaddr".to_string(),
                "name".to_string(),
                "_rowid".to_string(),
            ],
            "empty point-lookup plan must honor user column order including system columns"
        );
    }

    #[tokio::test]
    async fn test_point_lookup_rejects_missing_projection_column() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let projection = vec!["missing".to_string()];
        let pk_values = vec![ScalarValue::Int32(Some(2))];
        let err = planner
            .plan_lookup(&pk_values, Some(&projection))
            .await
            .expect_err("unknown projection column should fail planning");
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing-column projection error: {err}"
        );
    }

    #[tokio::test]
    async fn test_point_lookup_active_memtable_returns_newest_duplicate() {
        // Regression: same primary key inserted twice into one active
        // memtable must return the *newest* row. The bug was that
        // `FilterExec → LIMIT 1` over an insert-ordered scan returned the
        // first (oldest) match. The plan-path active arm now sorts `_rowid`
        // DESC and keeps the first row (largest `_rowid` = newest insert).
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use futures::TryStreamExt;

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        // BTree on the PK: the point lookup resolves keys through the indexed PK
        // path, which this exercises. (`indexed_count`/`visible_count` advance
        // from the batch position regardless of whether any index is configured.)
        index_store.add_btree("id_idx".to_string(), 0, "id".to_string());

        // Two writes to pk=1, then an unrelated pk=2. The "new" row goes
        // *second* so its `_rowid` is larger.
        let b_old = create_test_batch(&schema, &[1], "old");
        let b_new = create_test_batch(&schema, &[1], "new");
        let b_other = create_test_batch(&schema, &[2], "two");
        let (bp_old, off_old, _) = batch_store.append(b_old.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_old, off_old, Some(bp_old))
            .unwrap();
        let (bp_new, off_new, _) = batch_store.append(b_new.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_new, off_new, Some(bp_new))
            .unwrap();
        let (bp_other, off_other, _) = batch_store.append(b_other.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_other, off_other, Some(bp_other))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = Uuid::new_v4();
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                shard_id,
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let plan = planner
            .plan_lookup(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "expected exactly one row for pk=1");
        let name_col = batches[0].column_by_name("name").unwrap();
        let name_arr = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            name_arr.value(0),
            "new_1",
            "active-arm lookup must return the newer insert, not the oldest"
        );
    }

    #[tokio::test]
    async fn test_point_lookup_probes_auto_created_pk_btree() {
        // No user `add_btree` on the PK column — only `enable_pk_index`, which
        // auto-creates a BTree on the primary key (the production default). The
        // fast probe must resolve the newest visible version through that
        // collision-free BTree rather than falling back to the plan path.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        // No `add_btree` — `enable_pk_index` auto-creates the PK BTree.
        index_store.enable_pk_index(&[("id".to_string(), 0)]);

        // pk=1 written twice (the newer second), plus an unrelated pk=2.
        let b_old = create_test_batch(&schema, &[1], "old");
        let b_new = create_test_batch(&schema, &[1], "new");
        let b_other = create_test_batch(&schema, &[2], "two");
        let (bp_old, off_old, _) = batch_store.append(b_old.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_old, off_old, Some(bp_old))
            .unwrap();
        let (bp_new, off_new, _) = batch_store.append(b_new.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_new, off_new, Some(bp_new))
            .unwrap();
        let (bp_other, off_other, _) = batch_store.append(b_other.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_other, off_other, Some(bp_other))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = Uuid::new_v4();
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                shard_id,
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        // `lookup` takes the fast probe path (single-column PK, no system cols).
        let hit = planner
            .lookup(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap()
            .expect("pk=1 must be found via the PK-position index probe");
        assert_eq!(hit.num_rows(), 1);
        let name = hit
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            name.value(0),
            "new_1",
            "probe must return the newest version"
        );

        // An absent key resolves to None (no on-disk sources to consult).
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(999))], None)
                .await
                .unwrap()
                .is_none(),
            "absent key must miss"
        );
    }

    #[tokio::test]
    async fn test_point_lookup_sstable_returns_newest_duplicate() {
        // Regression / invariant pin: when an SSTable contains two
        // rows for the same PK, the lookup must return the newer one. The
        // SSTable dataset is reverse-written (newest at the smallest
        // physical position), so we simulate that here by writing the
        // dataset with the new row first. The point-lookup plan today
        // returns the first match (smallest `_rowid`) under reverse-write,
        // and remains so after this change.
        use futures::TryStreamExt;

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let base_uri = format!("{}/base", base_path);

        // Simulated reverse-write: newest insert lives at row 0.
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let row_new = create_test_batch(&schema, &[1], "new");
        let row_old = create_test_batch(&schema, &[1], "old");
        create_dataset(&gen1_uri, vec![row_new, row_old]).await;

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![shard_snapshot]);
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let plan = planner
            .plan_lookup(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "expected exactly one row for pk=1");
        let name_col = batches[0].column_by_name("name").unwrap();
        let name_arr = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            name_arr.value(0),
            "new_1",
            "SSTable-arm lookup must return the row at the smallest _rowid (newest under reverse-write)"
        );
    }

    /// Build an in-memory active memtable ref from batches, with a BTree on
    /// `id` and the visibility watermark advanced so every row is visible.
    fn active_memtable_ref(
        schema: &Arc<ArrowSchema>,
        batches: &[RecordBatch],
        generation: u64,
    ) -> crate::dataset::mem_wal::scanner::collector::InMemoryMemTableRef {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTableRef;
        let batch_store = Arc::new(BatchStore::with_capacity(64));
        let mut index_store = IndexStore::new();
        index_store.add_btree("id_idx".to_string(), 0, "id".to_string());
        for b in batches {
            let (idx, row_offset, _) = batch_store.append(b.clone()).unwrap();
            index_store
                .insert_with_batch_position(b, row_offset, Some(idx))
                .unwrap();
        }
        InMemoryMemTableRef {
            batch_store,
            index_store: Arc::new(index_store),
            schema: schema.clone(),
            generation,
        }
    }

    fn id_at(batch: &RecordBatch) -> i32 {
        batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0)
    }

    fn name_at(batch: &RecordBatch) -> String {
        batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    #[tokio::test]
    async fn test_lookup_fast_path_active_hit_and_absent() {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(
            &schema,
            &[create_test_batch(&schema, &[10, 20, 30], "v")],
            1,
        );
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema.clone());

        let row = planner
            .lookup(&[ScalarValue::Int32(Some(20))], None)
            .await
            .unwrap()
            .expect("hit");
        assert_eq!(row.num_rows(), 1);
        assert_eq!(id_at(&row), 20);

        // Absent key, no on-disk source → fast path proves non-existence.
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(99))], None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_lookup_fast_path_newest_duplicate() {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        // Same pk inserted twice; the second (larger position) is newest.
        let active = active_memtable_ref(
            &schema,
            &[
                create_test_batch(&schema, &[5], "old"),
                create_test_batch(&schema, &[5], "new"),
            ],
            1,
        );
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let row = planner
            .lookup(&[ScalarValue::Int32(Some(5))], None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(name_at(&row), "new_5", "must return the newest insert");
    }

    #[tokio::test]
    async fn test_lookup_miss_falls_back_to_base() {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let base = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&schema, &[1, 2, 3], "base")],
            )
            .await,
        );
        let active = active_memtable_ref(&schema, &[create_test_batch(&schema, &[99], "act")], 1);
        let collector = LsmDataSourceCollector::new(base, vec![]).with_in_memory_memtables(
            Uuid::new_v4(),
            InMemoryMemTables {
                active,
                frozen: vec![],
            },
        );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema.clone());

        // In active only → fast-path hit.
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(99))], None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id_at(&row), 99);

        // Only in base → active misses, falls back to the plan path.
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(2))], None)
            .await
            .unwrap()
            .expect("base hit via fallback");
        assert_eq!(id_at(&row), 2);
        assert_eq!(name_at(&row), "base_2");

        // Nowhere → None (fallback plan over base finds nothing).
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(1000))], None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_lookup_projection_regular_columns() {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(
            &schema,
            &[create_test_batch(&schema, &[10, 20, 30], "v")],
            1,
        );
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let row = planner
            .lookup(&[ScalarValue::Int32(Some(20))], Some(&["name".to_string()]))
            .await
            .unwrap()
            .unwrap();
        // The canonical point-lookup schema always includes the pk column, so
        // a `name` projection yields `[name, id]` — matching the plan path.
        let row_schema = row.schema();
        let names: Vec<&str> = row_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["name", "id"]);
        assert_eq!(name_at(&row), "v_20");
        assert_eq!(id_at(&row), 20);
    }

    #[tokio::test]
    async fn test_lookup_type_mismatch_falls_back_no_panic() {
        // PK is Int32; an Int64 literal must NOT take the direct BTree probe
        // (which could panic comparing mismatched OrderableScalarValue
        // variants) — it falls back to the coercing plan path.
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(
            &schema,
            &[create_test_batch(&schema, &[10, 20, 30], "v")],
            1,
        );
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let row = planner
            .lookup(&[ScalarValue::Int64(Some(20))], None)
            .await
            .expect("must not panic on a coercible-but-different key type")
            .expect("plan path coerces Int64 → Int32 and finds id=20");
        assert_eq!(id_at(&row), 20);
    }

    #[tokio::test]
    async fn test_lookup_empty_pk_values_errors_not_panics() {
        // Regression: the fast-path eligibility check must not index
        // `pk_values[0]` before verifying the slice is non-empty. An empty
        // slice falls through to the plan path's length validation.
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(&schema, &[create_test_batch(&schema, &[1], "v")], 1);
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let err = planner.lookup(&[], None).await;
        assert!(err.is_err(), "empty pk_values must error, not panic");
    }

    fn sorted_ids(batch: &RecordBatch) -> Vec<i32> {
        let arr = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut v: Vec<i32> = (0..arr.len()).map(|i| arr.value(i)).collect();
        v.sort_unstable();
        v
    }

    fn active_planner(batches: &[RecordBatch]) -> LsmPointLookupPlanner {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(&schema, batches, 1);
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema)
    }

    #[tokio::test]
    async fn test_lookup_many_hits_and_misses() {
        let schema = create_pk_schema();
        let planner = active_planner(&[create_test_batch(&schema, &[10, 20, 30], "v")]);
        // Mix present + absent keys; absent omitted, order not guaranteed.
        let keys = [
            ScalarValue::Int32(Some(30)),
            ScalarValue::Int32(Some(10)),
            ScalarValue::Int32(Some(999)),
            ScalarValue::Int32(Some(20)),
        ];
        let batch = planner.lookup_many(&keys, None).await.unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(sorted_ids(&batch), vec![10, 20, 30]);

        // Empty input → empty batch with the canonical schema.
        let empty = planner.lookup_many(&[], None).await.unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert!(empty.schema().field_with_name("id").is_ok());
    }

    #[tokio::test]
    async fn test_lookup_many_newest_duplicate() {
        let schema = create_pk_schema();
        // id=5 written twice; the batch get must return the newest ("new_5").
        let planner = active_planner(&[
            create_test_batch(&schema, &[5], "old"),
            create_test_batch(&schema, &[5, 7], "new"),
        ]);
        let batch = planner
            .lookup_many(
                &[ScalarValue::Int32(Some(5)), ScalarValue::Int32(Some(7))],
                None,
            )
            .await
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        got.sort_unstable();
        assert_eq!(got, vec!["new_5", "new_7"]);
    }

    #[tokio::test]
    async fn test_lookup_many_projection_and_equivalence_to_lookup() {
        let schema = create_pk_schema();
        let planner = active_planner(&[create_test_batch(&schema, &[1, 2, 3, 4], "v")]);
        let keys = [
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int32(Some(4)),
            ScalarValue::Int32(Some(1)),
        ];
        // Projected batch get == set of single lookups, same schema.
        let proj = vec!["name".to_string()];
        let batch = planner.lookup_many(&keys, Some(&proj)).await.unwrap();
        let batch_schema = batch.schema();
        let names: Vec<&str> = batch_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["name", "id"]); // pk always appended
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(sorted_ids(&batch), vec![1, 2, 4]);
    }

    #[tokio::test]
    async fn test_plan_point_lookup_executes() {
        use futures::TryStreamExt;
        let schema = create_pk_schema();
        let planner = active_planner(&[create_test_batch(&schema, &[10, 20, 30], "v")]);
        let plan = planner
            .plan_point_lookup(
                &[ScalarValue::Int32(Some(10)), ScalarValue::Int32(Some(30))],
                None,
            )
            .await
            .unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        let batches: Vec<RecordBatch> = plan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_lookup_against_from_configs_built_index() {
        // A point lookup against an index built the production way
        // (`IndexStore::from_configs`) resolves correctly via the seek-and-stop
        // skiplist probe.
        use crate::dataset::mem_wal::index::{BTreeIndexConfig, IndexStore, MemIndexConfig};
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};

        let schema = create_pk_schema();
        let batch = create_test_batch(&schema, &[10, 20, 30], "v");
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let index_store = IndexStore::from_configs(
            &[MemIndexConfig::BTree(BTreeIndexConfig {
                name: "id_idx".to_string(),
                field_id: 0,
                column: "id".to_string(),
            })],
            1000,
            100,
        )
        .unwrap();
        let (idx, row_offset, _) = batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, row_offset, Some(idx))
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: Arc::new(index_store),
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], schema);

        let row = planner
            .lookup(&[ScalarValue::Int32(Some(20))], None)
            .await
            .unwrap()
            .expect("range fallback must find the row");
        assert_eq!(id_at(&row), 20);
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(99))], None)
                .await
                .unwrap()
                .is_none(),
            "absent key must miss"
        );
    }

    // ----- tombstone (delete) point-lookup tests -----

    /// Memtable schema = base (`id`, `name`) + the `_tombstone` marker.
    fn pk_ts_schema() -> Arc<ArrowSchema> {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id = Field::new("id", DataType::Int32, false).with_metadata(id_metadata);
        Arc::new(ArrowSchema::new(vec![
            id,
            Field::new("name", DataType::Utf8, true),
            Field::new(TOMBSTONE, DataType::Boolean, false),
        ]))
    }

    fn ts_real(schema: &Arc<ArrowSchema>, ids: &[i32], prefix: &str) -> RecordBatch {
        let names: Vec<String> = ids.iter().map(|i| format!("{}_{}", prefix, i)).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow_array::BooleanArray::from(vec![false; ids.len()])),
            ],
        )
        .unwrap()
    }

    fn ts_tomb(schema: &Arc<ArrowSchema>, ids: &[i32]) -> RecordBatch {
        let names: Vec<Option<String>> = ids.iter().map(|_| None).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow_array::BooleanArray::from(vec![true; ids.len()])),
            ],
        )
        .unwrap()
    }

    /// Active-only planner whose memtable carries `_tombstone`; the planner's
    /// base schema stays tombstone-free (as the base table is in production).
    fn active_ts_planner(batches: &[RecordBatch]) -> LsmPointLookupPlanner {
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let base_schema = create_pk_schema();
        let mem_schema = pk_ts_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let active = active_memtable_ref(&mem_schema, batches, 1);
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active,
                    frozen: vec![],
                },
            );
        LsmPointLookupPlanner::new(collector, vec!["id".to_string()], base_schema)
    }

    /// Read the `_tombstone` marker from row 0 of a keep-tombstone result.
    fn tombstone_at(b: &RecordBatch) -> bool {
        let idx = b.schema().index_of(TOMBSTONE).expect("_tombstone column");
        b.column(idx)
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .expect("_tombstone is Boolean")
            .value(0)
    }

    #[tokio::test]
    async fn test_lookup_keep_tombstone_returns_deleted_row_with_marker() {
        // The tombstone-preserving variant keeps a deleted key that the filtered
        // `lookup` would drop: id=1 deleted, id=2 live, id=99 absent.
        let schema = pk_ts_schema();
        let planner = active_ts_planner(&[ts_real(&schema, &[1, 2], "v"), ts_tomb(&schema, &[1])]);

        // Deleted key: present with `_tombstone = true` (vs `None` from `lookup`).
        let deleted = planner
            .lookup_keep_tombstone(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap()
            .expect("deleted key kept by lookup_keep_tombstone");
        assert_eq!(id_at(&deleted), 1);
        assert!(
            tombstone_at(&deleted),
            "deleted key carries _tombstone = true"
        );

        // Live key: present with `_tombstone = false`.
        let live = planner
            .lookup_keep_tombstone(&[ScalarValue::Int32(Some(2))], None)
            .await
            .unwrap()
            .expect("live key found");
        assert_eq!(id_at(&live), 2);
        assert!(!tombstone_at(&live), "live key carries _tombstone = false");

        // Absent key: still `None` — no fresh entry at all to distinguish.
        assert!(
            planner
                .lookup_keep_tombstone(&[ScalarValue::Int32(Some(99))], None)
                .await
                .unwrap()
                .is_none(),
            "absent key has no fresh entry"
        );
    }

    #[tokio::test]
    async fn test_lookup_many_keep_tombstone_includes_tombstoned_keys() {
        // Batched variant: the tombstoned key is INCLUDED (carrying its marker),
        // unlike `lookup_many` which omits it. id=2 deleted; 1 and 3 live.
        let schema = pk_ts_schema();
        let planner =
            active_ts_planner(&[ts_real(&schema, &[1, 2, 3], "v"), ts_tomb(&schema, &[2])]);
        let keys = vec![
            vec![ScalarValue::Int32(Some(1))],
            vec![ScalarValue::Int32(Some(2))],
            vec![ScalarValue::Int32(Some(3))],
        ];
        let batch = planner
            .lookup_many_keep_tombstone(&keys, None)
            .await
            .unwrap();
        assert_eq!(
            batch.num_rows(),
            3,
            "all three keys kept, including the tombstone"
        );
        // Exactly one row (id=2) is marked deleted.
        let deleted: Vec<i32> = (0..batch.num_rows())
            .map(|r| batch.slice(r, 1))
            .filter(tombstone_at)
            .map(|b| id_at(&b))
            .collect();
        assert_eq!(deleted, vec![2], "only id=2 is marked deleted");
    }

    #[tokio::test]
    async fn test_lookup_tombstone_within_active_not_found() {
        // Fast path: id=1 written then tombstoned (newer); id=2 is a control.
        let schema = pk_ts_schema();
        let planner = active_ts_planner(&[ts_real(&schema, &[1, 2], "v"), ts_tomb(&schema, &[1])]);

        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(1))], None)
                .await
                .unwrap()
                .is_none(),
            "deleted key must resolve to not-found, not the older real row"
        );
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(2))], None)
            .await
            .unwrap()
            .expect("untouched key still found");
        assert_eq!(id_at(&row), 2);
    }

    #[tokio::test]
    async fn test_lookup_tombstone_then_reinsert() {
        // delete then re-insert the same key → the re-insert is newest.
        let schema = pk_ts_schema();
        let planner = active_ts_planner(&[
            ts_real(&schema, &[1], "old"),
            ts_tomb(&schema, &[1]),
            ts_real(&schema, &[1], "new"),
        ]);
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap()
            .expect("re-inserted key must be found");
        assert_eq!(name_at(&row), "new_1");
    }

    #[tokio::test]
    async fn test_lookup_tombstone_of_absent_key_is_noop() {
        // Deleting a key that never existed is a no-op miss.
        let schema = pk_ts_schema();
        let planner = active_ts_planner(&[ts_real(&schema, &[1], "v"), ts_tomb(&schema, &[99])]);
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(99))], None)
                .await
                .unwrap()
                .is_none()
        );
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(1))], None)
            .await
            .unwrap()
            .expect("unrelated key unaffected");
        assert_eq!(id_at(&row), 1);
    }

    #[tokio::test]
    async fn test_lookup_tombstone_plan_path_resurrection_guard() {
        // Force the plan path (project a system column) so the after-coalesce
        // filter — not the fast-path short-circuit — must drop the tombstone.
        // Real row lives in base; the newer active arm holds only its tombstone.
        use crate::dataset::mem_wal::scanner::collector::InMemoryMemTables;
        let base_schema = create_pk_schema();
        let mem_schema = pk_ts_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let base = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&base_schema, &[1, 2], "base")],
            )
            .await,
        );
        let active = active_memtable_ref(&mem_schema, &[ts_tomb(&mem_schema, &[1])], 2);
        let collector = LsmDataSourceCollector::new(base, vec![]).with_in_memory_memtables(
            Uuid::new_v4(),
            InMemoryMemTables {
                active,
                frozen: vec![],
            },
        );
        let planner = LsmPointLookupPlanner::new(collector, vec!["id".to_string()], base_schema);

        let proj = vec!["id".to_string(), "_rowid".to_string()];
        assert!(
            planner
                .lookup(&[ScalarValue::Int32(Some(1))], Some(&proj))
                .await
                .unwrap()
                .is_none(),
            "plan path must not fall through to the base row for a deleted key"
        );
        let row = planner
            .lookup(&[ScalarValue::Int32(Some(2))], Some(&proj))
            .await
            .unwrap()
            .expect("untouched base row found");
        assert_eq!(id_at(&row), 2);
    }

    #[tokio::test]
    async fn test_lookup_many_skips_tombstoned_keys() {
        // Batched lookup: a tombstoned key is omitted, others resolved.
        let schema = pk_ts_schema();
        let planner =
            active_ts_planner(&[ts_real(&schema, &[1, 2, 3], "v"), ts_tomb(&schema, &[2])]);
        let keys = [
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int32(Some(3)),
        ];
        let batch = planner.lookup_many(&keys, None).await.unwrap();
        assert_eq!(batch.num_rows(), 2, "the tombstoned key is omitted");
        assert_eq!(sorted_ids(&batch), vec![1, 3]);
    }
}
