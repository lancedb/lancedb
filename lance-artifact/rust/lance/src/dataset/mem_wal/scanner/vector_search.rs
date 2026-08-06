// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Vector search planner for LSM scanner.
//!
//! Provides KNN (K-Nearest Neighbors) search across LSM levels with staleness detection.

use std::sync::Arc;

use arrow_array::FixedSizeListArray;
use arrow_schema::SchemaRef;
use arrow_schema::SortOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use lance_core::datatypes::OnMissing;
use lance_core::{Error, Result};
use tracing::instrument;

use crate::dataset::Dataset;
use crate::io::exec::TakeExec;

use super::collector::LsmDataSourceCollector;
use super::data_source::LsmDataSource;
use super::projection::{
    DISTANCE_COLUMN, build_scanner_projection, canonical_output_schema, null_columns,
    project_to_canonical, validate_projection_names, wants_row_id,
};
use super::sstable_cache::{DatasetCache, SsTableWarmer, open_sstable};
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// Plans vector search queries over LSM data.
///
/// Each source is independently newest-per-PK before the union — the active
/// memtable via exact brute-force KNN when PK rewrites or a filter require it
/// (append-only active data can still use HNSW),
/// SSTables via their within-generation deletion vector — and the
/// cross-generation block-list ([`super::exec::PkBlockFilterExec`]) drops any
/// PK superseded by a newer generation. So each PK reaches the union from
/// exactly one source and a distance-ordered merge yields the global top-k; no
/// cross-source dedup is needed.
///
/// # Query Plan Structure
///
/// ```text
/// TakeExec (optional: fetch user-projected cols from base dataset)
///   SortPreservingMergeExec: order_by=[_distance ASC], fetch=k
///     SortExec: order_by=[_distance ASC], fetch=k          (per partition, parallel)
///       UnionExec
///         ProjectionExec (canonical output schema)
///           SortExec(_distance, fetch=k)
///               MemTableBruteForceVectorExec or VectorIndexExec: active memtable KNN
///         ProjectionExec (canonical output schema)
///           ProjectionExec (null_columns _rowid)
///             PkBlockFilterExec: block-list                   (SSTable)
///               KNNExec: SSTable gen N, fetch=ceil(k*overfetch) (fast_search)
///         … one per SSTable gen …
///         ProjectionExec (canonical output schema)
///           PkBlockFilterExec: block-list                     (base)
///             KNNExec: base table, k (fast_search)[.refine()?]
/// ```
///
/// # Index-Only Search (fast_search)
///
/// For base table and SSTables we use `fast_search()` to only
/// search indexed data. This is correct because:
/// - Each SSTable has its own vector index built during flush.
/// - The active memtable covers any unindexed data.
/// - Searching unindexed data in base/SSTable would be redundant.
pub struct LsmVectorSearchPlanner {
    /// Data source collector.
    collector: LsmDataSourceCollector,
    /// Primary key column names (used by within-source dedup and block-list).
    pk_columns: Vec<String>,
    /// Schema of the base table.
    base_schema: SchemaRef,
    /// Vector column name.
    vector_column: String,
    /// Distance metric type (L2, Cosine, Dot, etc.).
    distance_type: lance_linalg::distance::DistanceType,
    /// Base dataset for the post-rerank take: after the cross-source distance
    /// merge, `TakeExec` materializes user-projected columns that weren't in
    /// the per-source KNN output. Memtable rows already carry all columns;
    /// the take only fetches additional data for base rows (real `_rowid`).
    dataset: Option<Arc<Dataset>>,
    /// Session threaded into SSTable opens (shared caches).
    session: Option<Arc<Session>>,
    /// Store params for opening SSTables, reusing the base dataset's store.
    store_params: Option<ObjectStoreParams>,
    /// Cache of opened SSTable datasets.
    sstable_cache: Option<Arc<dyn DatasetCache>>,
    /// Optional warmer fired on first open of an SSTable.
    warmer: Option<Arc<dyn SsTableWarmer>>,
    /// Optional prefilter predicate applied to every source arm before its KNN
    /// search, so rows failing the predicate never enter the top-k. Base and
    /// SSTable arms use the dataset scanner's native prefilter; memtable arms
    /// route to a filtered brute-force scan.
    filter: Option<Expr>,
}

impl LsmVectorSearchPlanner {
    /// Create a new planner.
    ///
    /// # Arguments
    ///
    /// * `collector` - Data source collector
    /// * `pk_columns` - Primary key column names
    /// * `base_schema` - Schema of the base table
    /// * `vector_column` - Name of the vector column to search
    /// * `distance_type` - Distance metric (L2, Cosine, etc.)
    pub fn new(
        collector: LsmDataSourceCollector,
        pk_columns: Vec<String>,
        base_schema: SchemaRef,
        vector_column: String,
        distance_type: lance_linalg::distance::DistanceType,
    ) -> Self {
        Self {
            collector,
            pk_columns,
            base_schema,
            vector_column,
            distance_type,
            dataset: None,
            session: None,
            store_params: None,
            sstable_cache: None,
            warmer: None,
            filter: None,
        }
    }

    /// Attach an optional prefilter predicate. Every source arm restricts its
    /// KNN to rows matching the predicate (true prefilter), so results match a
    /// normal filtered vector scan over base ∪ SSTables ∪ in-memory data.
    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
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
    /// searches against the same generation a pure `Arc::clone`.
    pub fn with_sstable_cache(mut self, cache: Arc<dyn DatasetCache>) -> Self {
        self.sstable_cache = Some(cache);
        self
    }

    /// Inject the warmer fired on first open of an SSTable.
    pub fn with_warmer(mut self, warmer: Arc<dyn SsTableWarmer>) -> Self {
        self.warmer = Some(warmer);
        self
    }

    /// Set the base dataset for post-rerank take.
    ///
    /// After global PK dedup and sort, a `TakeExec` against this dataset
    /// materializes any user-projected columns that were not part of the
    /// per-source KNN output. This is necessary because per-source KNN
    /// only returns the columns needed for dedup and ranking; the take
    /// step fetches the full user projection for the final top-k rows.
    pub fn with_dataset(mut self, dataset: Arc<Dataset>) -> Self {
        self.dataset = Some(dataset);
        self
    }

    /// Create a vector search plan.
    ///
    /// # Arguments
    ///
    /// * `query_vector` - Query vector for KNN search
    /// * `k` - Number of nearest neighbors to return
    /// * `nprobes` - Number of IVF partitions to search (for IVF-based indexes)
    /// * `projection` - Columns to include in output (None = all columns)
    /// * `refine_base_table` - When true, the base-table arm re-ranks its
    ///   candidates with exact distances (refine factor 1). Useful when the base
    ///   table uses an approximate index (IVF-PQ) so cross-source distance
    ///   comparison is exact. Memtable arms already use exact distances and
    ///   never need refine. Auto-enabled whenever stale filtering is on (see
    ///   below).
    /// * `overfetch_factor` - Controls how aggressively sources over-fetch to
    ///   backfill the rows dropped by stale-row filtering. Values below `1.0`
    ///   are rejected; stale filtering is always enabled.
    ///
    ///   - `factor == 1.0`: **stale filtering on, no over-fetch.** Each source
    ///     that has superseded rows fetches exactly `k` candidates, drops the
    ///     stale ones, and may therefore return fewer than `k` live rows.
    ///   - `factor > 1.0`: **stale filtering on, with over-fetch.** Such a source
    ///     fetches `ceil(k * factor)` candidates so that dropping the stale ones
    ///     still leaves `k` live rows for the merge.
    ///
    /// # Returns
    ///
    /// An execution plan that returns the top-K nearest neighbors across all
    /// LSM levels, with stale results filtered out.
    #[instrument(name = "lsm_vector_search", level = "info", skip_all, fields(k, nprobes, vector_column = %self.vector_column, distance_type = ?self.distance_type))]
    pub async fn plan_search(
        &self,
        query_vector: &FixedSizeListArray,
        k: usize,
        nprobes: usize,
        projection: Option<&[String]>,
        refine_base_table: bool,
        overfetch_factor: f64,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if k == 0 {
            return Err(Error::invalid_input("k must be positive".to_string()));
        }
        if nprobes == 0 {
            return Err(Error::invalid_input("nprobes must be positive".to_string()));
        }

        let sources = self.collector.collect()?;
        validate_projection_names(projection, &self.base_schema, &[DISTANCE_COLUMN])?;
        // The block-list is the sole cross-generation dedup mechanism, so it
        // runs unconditionally; `overfetch_factor` only tunes the over-fetch
        // multiple for blocked sources.
        let overfetch_factor = super::validate_overfetch_factor(overfetch_factor)?;

        if sources.is_empty() {
            return self.empty_plan(projection);
        }

        // Per-source PK block sets (`NEWER(G)`; base = union of all gens).
        // `Box::pin` keeps the future off `clippy::large_futures`.
        let block_lists = Box::pin(super::block_list::compute_source_block_lists(
            &sources,
            self.session.as_ref(),
            self.store_params.as_ref(),
            self.sstable_cache.as_ref(),
        ))
        .await?;

        let canonical_schema = canonical_output_schema(
            projection,
            &self.base_schema,
            &self.pk_columns,
            true, // include _distance — KNN always produces it
        );

        // Refine the base table when explicitly requested, or whenever the base
        // is blocked (it then over-fetches its approximate-index candidates, so
        // distances must be re-ranked to exact before the cross-source merge).
        // `block_lists` is non-empty exactly when a newer generation exists.
        let refine_base = refine_base_table || !block_lists.is_empty();

        // Stage per-source over-fetch decisions, then build every KNN plan
        // concurrently — the builds are independent and a sequential loop was
        // the dominant serial planning cost at multiple generations.
        let arm_inputs: Vec<_> = sources
            .iter()
            .map(|source| {
                let generation = source.generation();
                let is_base = matches!(source, LsmDataSource::BaseTable { .. });
                let is_active = matches!(source, LsmDataSource::ActiveMemTable { .. });
                // Over-fetch when the post-source block-list can drop
                // candidates. Active memtable PK recency is handled inside the
                // exact brute-force exec before top-k, so it does not need
                // source over-fetch.
                let blocked = block_lists.get(&(source.shard_id(), generation));
                let fetch_k = if blocked.is_some() && !self.pk_columns.is_empty() {
                    ((k as f64) * overfetch_factor).ceil() as usize
                } else {
                    k
                };
                (source, is_base, is_active, blocked, fetch_k)
            })
            .collect();
        let built = futures::future::try_join_all(arm_inputs.iter().map(
            |(source, is_base, _, _, fetch_k)| {
                Box::pin(self.build_knn_plan(
                    source,
                    query_vector,
                    *fetch_k,
                    nprobes,
                    projection,
                    *is_base && refine_base,
                ))
            },
        ))
        .await?;

        let mut knn_plans = Vec::new();
        for ((_, is_base, _, blocked, _), knn) in arm_inputs.iter().zip(built) {
            let is_base = *is_base;
            let blocked = *blocked;
            // Make each source independently newest-per-PK before the union:
            //  * active: append-only memtables can use HNSW directly; once a
            //    PK rewrite is observed, `MemTableBruteForceVectorExec` drops
            //    superseded versions before the top-k cut.
            //  * SSTable/base: drop cross-gen superseded rows via the
            //    block-list (within-gen is handled by the SSTable DV).
            let knn = match blocked {
                Some(_) if self.pk_columns.is_empty() => knn,
                Some(set) => Arc::new(super::exec::PkBlockFilterExec::new(
                    knn,
                    self.pk_columns.clone(),
                    set.clone(),
                    k,
                )) as Arc<dyn ExecutionPlan>,
                None => knn,
            };
            // Lance's `fast_search()` and the active scan both produce a
            // per-source `_rowid` that would collide with base row ids in the
            // canonical output, so NULL it on non-base arms. The base arm keeps
            // its real `_rowid` to drive the post-rerank take.
            let after_null = if is_base {
                knn
            } else {
                null_columns(knn, &[lance_core::ROW_ID])?
            };
            // Normalize each source to the canonical output schema.
            let normalized = project_to_canonical(after_null, &canonical_schema)?;
            knn_plans.push(normalized);
        }

        // No cross-source dedup needed (see struct doc): SortExec(per partition)
        // + SortPreservingMerge does the p-way distance-ordered top-k merge.
        #[allow(deprecated)]
        // The downstream `SortPreservingMergeExec` already spawns one driver
        // task per input partition (one per union arm) via `spawn_buffered`, so
        // each arm's per-arm CPU (HNSW search, distance refine) runs on its own
        // task without an extra repartition.
        let merged: Arc<dyn ExecutionPlan> = Arc::new(UnionExec::new(knn_plans));

        let distance_idx = merged.schema().index_of(DISTANCE_COLUMN).map_err(|_| {
            lance_core::Error::invalid_input(format!(
                "Column '{}' not found in schema",
                DISTANCE_COLUMN
            ))
        })?;

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(DISTANCE_COLUMN, distance_idx)),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }];

        let lex_ordering = LexOrdering::new(sort_expr).ok_or_else(|| {
            lance_core::Error::internal("Failed to create LexOrdering".to_string())
        })?;

        // Sort each partition's candidates in parallel, capped at k via `with_fetch`
        // so each partition early-terminates instead of materializing every candidate
        // into a global sort. SortPreservingMergeExec then does a p-way heap merge of
        // the pre-sorted streams (also capped at k), producing the final top-k.
        // This avoids serializing the merge on a single thread and pushes the k-limit
        // into per-partition work, where it actually helps.
        //
        // `with_preserve_partitioning(true)` is load-bearing: without it, SortExec
        // declares it needs a SinglePartition input but doesn't coalesce on its own —
        // execute(0) silently reads only partition 0 of the union and drops the rest.
        let per_partition_sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(lex_ordering.clone(), merged)
                .with_preserve_partitioning(true)
                .with_fetch(Some(k)),
        );
        let merged_sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(lex_ordering, per_partition_sorted).with_fetch(Some(k)),
        );

        // After global rerank, take any user-projected columns that the
        // per-source KNN didn't return. This fetches from the base dataset
        // using `_rowid`; memtable rows (NULL `_rowid`) already carry all
        // their data so the take is a no-op for them.
        #[allow(deprecated)]
        let result = if let Some(dataset) = &self.dataset {
            let cols = build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
            let output_projection = dataset
                .empty_projection()
                .union_columns(cols, OnMissing::Ignore)?;
            let coalesced: Arc<dyn ExecutionPlan> =
                Arc::new(CoalesceBatchesExec::new(merged_sorted.clone(), 8192));
            if let Some(take_plan) =
                TakeExec::try_new(dataset.clone(), coalesced, output_projection)?
            {
                Arc::new(take_plan) as Arc<dyn ExecutionPlan>
            } else {
                merged_sorted
            }
        } else {
            merged_sorted
        };

        // Under-fetch is warned per-source inside `PkBlockFilterExec`.
        Ok(result)
    }

    /// Build KNN plan for a single data source.
    ///
    async fn build_knn_plan(
        &self,
        source: &LsmDataSource,
        query_vector: &FixedSizeListArray,
        k: usize,
        nprobes: usize,
        projection: Option<&[String]>,
        refine: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match source {
            LsmDataSource::BaseTable { dataset } => {
                let mut scanner = dataset.scan();
                let cols =
                    build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // Native scanner prefilter: the ANN runs over rows matching
                    // the predicate, so the top-k holds only matching rows.
                    // `prefilter(true)` is required — without it the scanner
                    // post-filters the unfiltered top-k, dropping matching rows
                    // that ranked below non-matching ones.
                    scanner.filter_expr(filter.clone());
                    scanner.prefilter(true);
                }
                // Only the base produces a meaningful `_rowid`. `_rowaddr`
                // can't be combined with `fast_search()` — the IVF index
                // doesn't preserve it and `TakeExec` refuses to insert it
                // post-search — so it stays NULL across all arms.
                if wants_row_id(projection) {
                    scanner.with_row_id();
                }
                let query_arr = single_query_array(query_vector);
                scanner.nearest(&self.vector_column, query_arr.as_ref(), k)?;
                scanner.nprobes(nprobes);
                scanner.distance_metric(self.distance_type);
                // Memtables cover unindexed rows; only search indexed data here.
                scanner.fast_search();
                // Re-rank base candidates with exact distances so they're
                // directly comparable to memtable distances in the merge.
                if refine {
                    scanner.refine(1);
                }
                scanner.create_plan().await
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
                let cols =
                    build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // See the base arm: `prefilter(true)` makes this a true
                    // prefilter rather than a lossy post-filter on the top-k.
                    scanner.filter_expr(filter.clone());
                    scanner.prefilter(true);
                }
                // No `with_row_id/address`: per-source IDs would collide with base.
                let query_arr = single_query_array(query_vector);
                scanner.nearest(&self.vector_column, query_arr.as_ref(), k)?;
                scanner.nprobes(nprobes);
                scanner.distance_metric(self.distance_type);
                scanner.fast_search();
                scanner.create_plan().await
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
                // Supply PKs so the memtable scanner can choose HNSW for
                // append-only data and exact newest-before-top-k search when
                // PK rewrites or filters make stale suppression necessary.
                scanner.with_pk_columns(self.pk_columns.clone());
                // PK auto-included so the staleness filter retains its bloom hash key.
                let cols =
                    build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // Routed to filtered brute-force (see `plan_vector_search`):
                    // the predicate masks rows before the memtable top-k cut.
                    scanner.filter_expr(filter.clone());
                }
                scanner.nearest(&self.vector_column, query_vector, k)?;
                scanner.nprobes(nprobes);
                scanner.distance_metric(self.distance_type);
                scanner.create_plan().await
            }
        }
    }

    /// Create an empty execution plan with the canonical KNN output schema.
    fn empty_plan(&self, projection: Option<&[String]>) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = canonical_output_schema(projection, &self.base_schema, &self.pk_columns, true);
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

/// Convert a (typically single-row) FixedSizeList query into the array shape
/// `Scanner::nearest` expects:
///
/// - 1 row → the inner Float32Array (single-vector query). Passing the FSL
///   directly would make the scanner treat it as a multivector query and
///   reject it on a non-multivector column.
/// - >1 row → the FSL itself (multivector query path).
fn single_query_array(query_vector: &FixedSizeListArray) -> arrow_array::ArrayRef {
    use arrow_array::Array;
    if query_vector.len() == 1 {
        query_vector.value(0)
    } else {
        std::sync::Arc::new(query_vector.clone()) as arrow_array::ArrayRef
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{Dataset, WriteParams};
    use arrow_array::{
        BooleanArray, Int32Array, RecordBatch, RecordBatchIterator, builder::FixedSizeListBuilder,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::collections::HashMap;

    fn create_vector_schema() -> Arc<ArrowSchema> {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_metadata);

        Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new(
                "vector",
                // Use nullable=true to match what FixedSizeListBuilder produces
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]))
    }

    fn create_vector_tombstone_schema() -> Arc<ArrowSchema> {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_metadata);

        Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
            Field::new(crate::dataset::mem_wal::TOMBSTONE, DataType::Boolean, false),
        ]))
    }

    fn create_query_vector() -> FixedSizeListArray {
        use arrow_array::builder::Float32Builder;

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        builder.values().append_value(0.1);
        builder.values().append_value(0.2);
        builder.values().append_value(0.3);
        builder.values().append_value(0.4);
        builder.append(true);

        builder.finish()
    }

    fn create_test_batch(schema: &ArrowSchema, ids: &[i32]) -> RecordBatch {
        use arrow_array::builder::Float32Builder;

        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for id in ids {
            let base = *id as f32 * 0.1;
            vector_builder.values().append_value(base);
            vector_builder.values().append_value(base + 0.1);
            vector_builder.values().append_value(base + 0.2);
            vector_builder.values().append_value(base + 0.3);
            vector_builder.append(true);
        }

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(vector_builder.finish()),
            ],
        )
        .unwrap()
    }

    /// One row with an explicit `(id, vector)` — unlike [`create_test_batch`],
    /// the vector is not derived from `id`, so the same PK can carry different
    /// vectors across generations (an update / move-out-of-neighborhood).
    fn create_id_vector_batch(schema: &ArrowSchema, id: i32, vector: [f32; 4]) -> RecordBatch {
        use arrow_array::builder::Float32Builder;
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for v in vector {
            builder.values().append_value(v);
        }
        builder.append(true);
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(builder.finish()),
            ],
        )
        .unwrap()
    }

    fn tombstone_vector_batch(
        schema: &ArrowSchema,
        rows: &[(i32, Option<[f32; 4]>, bool)],
    ) -> RecordBatch {
        use arrow_array::builder::Float32Builder;

        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for (_, vector, _) in rows {
            if let Some(vector) = vector {
                for value in vector {
                    vector_builder.values().append_value(*value);
                }
                vector_builder.append(true);
            } else {
                for _ in 0..4 {
                    vector_builder.values().append_null();
                }
                vector_builder.append(false);
            }
        }
        let ids: Vec<i32> = rows.iter().map(|(id, _, _)| *id).collect();
        let tombstones: Vec<bool> = rows.iter().map(|(_, _, tombstone)| *tombstone).collect();

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(vector_builder.finish()),
                Arc::new(BooleanArray::from(tombstones)),
            ],
        )
        .unwrap()
    }

    async fn create_dataset(uri: &str, batches: Vec<RecordBatch>) -> Dataset {
        let schema = batches[0].schema();
        let has_id = schema.column_with_name("id").is_some();
        let reader = RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema);
        let dataset = Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap();
        // Also write the standalone PK sidecar (on `id`) so an SSTable
        // source can be probed by the block-list (harmless for a base table).
        if has_id {
            crate::dataset::mem_wal::scanner::block_list::write_pk_sidecar(uri, &batches, &["id"])
                .await
                .unwrap();
        }
        dataset
    }

    #[tokio::test]
    async fn test_vector_search_plan_structure() {
        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1, 2, 3]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema.clone(),
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner.plan_search(&query, 10, 8, None, false, 1.0).await;

        // Plan construction must succeed. Execution against empty data is a
        // separate concern handled by integration tests.
        plan.expect("planner should produce a plan even when memtables are empty");
    }

    #[tokio::test]
    async fn test_vector_search_validates_k_and_nprobes() {
        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1, 2, 3]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let err = planner
            .plan_search(&query, 0, 1, None, false, 1.0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("k must be positive"),
            "expected k validation error, got {err}"
        );

        let err = planner
            .plan_search(&query, 1, 0, None, false, 1.0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("nprobes must be positive"),
            "expected nprobes validation error, got {err}"
        );
    }

    #[tokio::test]
    async fn test_projection_includes_pk() {
        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]);

        let _planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema.clone(),
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        // Project only "vector" - should also include "id" for staleness detection
        let cols =
            build_scanner_projection(Some(&["vector".to_string()]), &schema, &["id".to_string()]);

        assert!(cols.contains(&"vector".to_string()));
        assert!(cols.contains(&"id".to_string()));
    }

    #[tokio::test]
    async fn test_vector_search_base_plus_active_returns_distance() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[10, 20, 30]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        // Active memtable with HNSW index over the "vector" column.
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = create_test_batch(&schema, &[1, 2, 3, 4]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 1.0)
            .await
            .expect("planner should produce a plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "expected at least one result row");

        let out_schema = batches[0].schema();
        let out_cols: Vec<String> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(
            out_schema.field_with_name(DISTANCE_COLUMN).is_ok(),
            "output schema is missing `_distance` column. Got: {:?}",
            out_cols
        );
        // Internal columns must not leak: `_rowid` (added by Lance's fast_search
        // in the base/SSTable arms) and `_memtable_gen` (added by the LSM merge
        // when bloom filters are present) are bookkeeping, not API.
        assert!(
            out_schema.field_with_name("_rowid").is_err(),
            "`_rowid` leaked into output: {:?}",
            out_cols
        );
        assert!(
            out_schema
                .field_with_name(super::super::exec::MEMTABLE_GEN_COLUMN)
                .is_err(),
            "`_memtable_gen` leaked into output: {:?}",
            out_cols
        );

        // The nearest neighbor to the query vector should be id=1 (exact match),
        // and its `_distance` must be ~0 — verifying both the column is present
        // and the values are correct.
        let id_col = batches[0]
            .column_by_name("id")
            .expect("id column missing")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column should be Int32");
        let dist_col = batches[0]
            .column_by_name(DISTANCE_COLUMN)
            .expect("_distance column missing")
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .expect("_distance column should be Float32");
        assert_eq!(id_col.value(0), 1, "expected id=1 as nearest neighbor");
        assert!(
            dist_col.value(0).abs() < 1e-3,
            "expected near-zero distance for self-match, got {}",
            dist_col.value(0)
        );
    }

    #[tokio::test]
    async fn test_vector_search_active_tombstone_masks_base_hit() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let base_schema = create_vector_schema();
        let mem_schema = create_vector_tombstone_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let q = [0.1, 0.2, 0.3, 0.4];
        let near = [0.12, 0.22, 0.32, 0.42];

        let mut base_dataset = create_dataset(
            &base_uri,
            vec![batch_rows(&base_schema, &[(1, q), (2, near)])],
        )
        .await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        let active_tombstone = tombstone_vector_batch(&mem_schema, &[(1, None, true)]);
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        let (_, row_offset, batch_position) = batch_store.append(active_tombstone.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_tombstone, row_offset, Some(batch_position))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store,
                    schema: mem_schema,
                    generation: 1,
                },
                frozen: vec![],
            },
        );
        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            base_schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 1, 1, None, false, 2.0)
            .await
            .unwrap();
        let stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        assert_eq!(rows.len(), 1, "expected one backfilled hit, got {rows:?}");
        assert_eq!(
            rows[0].0, 2,
            "active tombstone for id=1 must block the older base vector hit; got {rows:?}"
        );
    }

    /// A prefilter on a vector search must restrict the KNN to rows matching the
    /// predicate, even though the nearest (and second-nearest) rows fail it. The
    /// active memtable has an HNSW index, but a filtered search routes to the
    /// brute-force arm, which masks rows before the top-k cut.
    #[tokio::test]
    async fn test_vector_search_prefilter_restricts_to_matching_rows() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        // Base rows are far from the query and unindexed, so `fast_search`
        // contributes nothing; the test isolates the memtable prefilter.
        let base_dataset = Arc::new(
            create_dataset(&base_uri, vec![create_test_batch(&schema, &[100, 200])]).await,
        );

        // Active memtable with ids 0..=3 (id=1 is the exact match, id=0 ties id=2).
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = create_test_batch(&schema, &[0, 1, 2, 3]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        // `id >= 2` excludes the two nearest rows (id=1 exact, id=0 tie).
        .with_filter(Some(col("id").gt_eq(lit(2i32))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 10, 1, None, false, 1.0)
            .await
            .expect("planner should produce a filtered plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut ids: Vec<i32> = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(col.value(i));
            }
        }

        // Only id=2 and id=3 satisfy `id >= 2`; the nearer id=0/id=1 are excluded.
        assert_eq!(
            ids.first().copied(),
            Some(2),
            "nearest matching row is id=2"
        );
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![2, 3],
            "prefilter must drop id=0 and id=1 (nearer but failing `id >= 2`)"
        );
    }

    #[tokio::test]
    async fn test_vector_search_filtered_active_without_pk_keeps_all_matching_rows() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_dataset = Arc::new(
            create_dataset(&base_uri, vec![create_test_batch(&schema, &[100, 200])]).await,
        );

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let index_store = IndexStore::new();
        let batch = create_test_batch(&schema, &[1, 2, 3]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec![],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_filter(Some(col("id").gt_eq(lit(1i32))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 1.0)
            .await
            .expect("planner should produce a filtered active plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut ids: Vec<i32> = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(col.value(i));
            }
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "no-PK filtered active vector search must not collapse all rows into one key"
        );
    }

    /// The *base* arm must apply the filter as a true prefilter, not a
    /// post-filter on the unfiltered top-k. The base is vector-indexed (so
    /// `fast_search` uses the index); the two rows nearest the query (id=1, id=2)
    /// fail the predicate while matching rows (id>=3) are farther. Without
    /// `scanner.prefilter(true)` the base arm runs the ANN unfiltered, takes the
    /// top-`k`, then post-filters — dropping every row. A true prefilter restricts
    /// the ANN to matching rows. Regression for a missing base-arm `prefilter(true)`.
    #[tokio::test]
    async fn test_vector_search_base_prefilter_is_not_a_lossy_postfilter() {
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        // L2 distance to the query grows monotonically with id (id=1 is the exact
        // match), so the two nearest rows are id=1, id=2 — both excluded by `id>=3`.
        let base_batch = create_test_batch(&schema, &[1, 2, 3, 4, 5, 6]);
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        let collector = LsmDataSourceCollector::new(base_dataset.clone(), vec![]);
        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_dataset(base_dataset)
        // Keeps only id>=3, the *farther* matches. A post-filter on the
        // unfiltered top-2 (id=1, id=2) would drop everything.
        .with_filter(Some(col("id").gt_eq(lit(3i32))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 2, 1, None, false, 1.0)
            .await
            .expect("planner should produce a filtered base plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut ids: Vec<i32> = Vec::new();
        for b in &batches {
            let col = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(col.value(i));
            }
        }
        assert_eq!(
            ids.first().copied(),
            Some(3),
            "nearest matching base row is id=3; got {ids:?}"
        );
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![3, 4],
            "base prefilter must return the two nearest matches (id=3, id=4), not \
             post-filter the unfiltered top-2 (id=1, id=2) down to nothing; got {ids:?}"
        );
    }

    /// The SSTable arm must also apply the filter as a true prefilter, and that
    /// prefiltered candidate set must compose with cross-generation block-list
    /// filtering plus over-fetch. Gen 1's closest predicate-matching row (id=3)
    /// is superseded by gen 2; with over-fetch, gen 1 should still contribute
    /// the next live predicate match (id=4).
    #[tokio::test]
    async fn test_vector_search_sstable_prefilter_composes_with_block_list() {
        use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let shard_id = uuid::Uuid::new_v4();
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);

        let q = [0.1, 0.2, 0.3, 0.4];
        let near = [0.12, 0.22, 0.32, 0.42];
        let far = [9.0, 9.0, 9.0, 9.0];

        // Gen 1: id=1 is nearest but fails the predicate. id=3 passes but is
        // stale (blocked by gen 2). id=4 is the next live predicate match.
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let mut gen1 = create_dataset(
            &gen1_uri,
            vec![batch_rows(&schema, &[(1, q), (3, q), (4, near)])],
        )
        .await;
        gen1.create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();

        // Gen 2: newer id=3 shadows gen 1's close copy but is far from query.
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        let mut gen2 = create_dataset(&gen2_uri, vec![batch_rows(&schema, &[(3, far)])]).await;
        gen2.create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();

        let snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(3)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![snapshot]);

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_filter(Some(col("id").gt_eq(lit(3i32))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 1, 1, None, false, 2.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        assert_eq!(rows.len(), 1, "expected one result, got {:?}", rows);
        assert_eq!(
            rows[0].0, 4,
            "SSTable prefilter should return live id=4 after stale id=3 is blocked; got {:?}",
            rows
        );
    }

    /// An in-memtable update whose *newest* version fails the prefilter must
    /// exclude the PK entirely, not leak the stale older version that still
    /// passes. Regression for filter-before-dedup on the active arm: the filter
    /// is evaluated against the newest version of each PK.
    #[tokio::test]
    async fn test_vector_search_prefilter_excludes_pk_whose_newest_version_fails() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use arrow_array::StringArray;
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_schema::{DataType, Field};
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;

        let mut id_meta = std::collections::HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
        ]));
        // Same PK id=5 appended twice (an in-memtable update): row0 "active"
        // (passes), row1 "archived" (the current version, fails the filter). Both
        // vectors are near the query.
        let make_batch = |statuses: &[&str]| -> RecordBatch {
            let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
            for _ in statuses {
                for d in 0..4 {
                    vb.values().append_value(0.1 + d as f32 * 0.1);
                }
                vb.append(true);
            }
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![5; statuses.len()])),
                    Arc::new(vb.finish()),
                    Arc::new(StringArray::from(statuses.to_vec())),
                ],
            )
            .unwrap()
        };

        // Base is unindexed, so `fast_search` contributes nothing; the test
        // isolates the active-memtable dedup-before-filter behavior. Use a
        // non-conflicting id to avoid any PK confusion.
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let mut fb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for d in 0..4 {
            fb.values().append_value(100.0 + d as f32);
        }
        fb.append(true);
        let far = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![999])),
                Arc::new(fb.finish()),
                Arc::new(StringArray::from(vec!["active"])),
            ],
        )
        .unwrap();
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![far]).await);

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = make_batch(&["active", "archived"]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_filter(Some(col("status").eq(lit("active"))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 10, 1, None, false, 1.0)
            .await
            .expect("planner should produce a filtered plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 0,
            "pk=5's current version is 'archived' and must be excluded; a stale \
             'active' older version must not leak (filter evaluated on newest)"
        );
    }

    /// Cross-arm stale rows must be blocked even if the newer active row fails
    /// the prefilter. The base copy of pk=5 matches `status = 'active'`, but the
    /// newer active copy is `archived`; pk=5 must be absent from the result.
    #[tokio::test]
    async fn test_vector_search_prefilter_blocks_base_when_active_newest_fails() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use arrow_array::StringArray;
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        use arrow_schema::{DataType, Field};
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let mut id_meta = std::collections::HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
        ]));
        let make_batch = |rows: &[(i32, [f32; 4], &str)]| -> RecordBatch {
            let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 4);
            for (_, vector, _) in rows {
                for value in vector {
                    vectors.values().append_value(*value);
                }
                vectors.append(true);
            }
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(
                        rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                    )),
                    Arc::new(vectors.finish()),
                    Arc::new(StringArray::from(
                        rows.iter()
                            .map(|(_, _, status)| *status)
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let query = [0.1, 0.2, 0.3, 0.4];
        let fallback = [0.2, 0.3, 0.4, 0.5];

        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = make_batch(&[(5, query, "active"), (6, fallback, "active")]);
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            16,
            4,
        );
        let active_batch = make_batch(&[(5, query, "archived")]);
        batch_store.append(active_batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(base_dataset.clone(), vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_dataset(base_dataset)
        .with_filter(Some(col("status").eq(lit("active"))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 10, 1, None, false, 1.0)
            .await
            .expect("planner should produce a filtered base+active plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut ids: Vec<i32> = collect_id_dist(&batches)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![6],
            "base pk=5 passes the filter but is superseded by active archived pk=5; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn cross_gen_stale_version_blocked_by_newer_memtable() {
        // A PK updated ACROSS a freeze boundary: its old (near) version lives in
        // a frozen memtable, its new (far) version in the active one. Both are
        // in-memory `ActiveMemTable` sources, so each gets only a per-generation
        // recency filter (`NewestPkFilterExec`) — which can't see the newer gen.
        // The cross-generation block-list is the only thing that can drop the
        // stale near copy; if the active arm discards it, the frozen near version
        // leaks as a phantom near-neighbor. This is the residual wallop fuzz
        // delete/update phantom: the cluster constantly freezes memtables
        // (flush_interval ~250ms), so an insert and its later delete/update split
        // across in-memory generations.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        // Base seed far from the query; id=1 is not in the base.
        let base_dataset =
            Arc::new(create_dataset(&base_uri, vec![create_test_batch(&schema, &[99])]).await);

        // One in-memory memtable holding `id` at `vector`, with a PK + HNSW index.
        let make_gen = |id: i32, vector: [f32; 4], generation: u64| {
            let batch_store = Arc::new(BatchStore::with_capacity(16));
            let mut index_store = IndexStore::new();
            index_store.enable_pk_index(&[("id".to_string(), 0)]);
            index_store.add_hnsw(
                "vector_hnsw".to_string(),
                1,
                "vector".to_string(),
                lance_linalg::distance::DistanceType::L2,
                64,
                8,
            );
            let batch = create_id_vector_batch(&schema, id, vector);
            batch_store.append(batch.clone()).unwrap();
            index_store
                .insert_with_batch_position(&batch, 0, Some(0))
                .unwrap();
            InMemoryMemTableRef {
                batch_store,
                index_store: Arc::new(index_store),
                schema: schema.clone(),
                generation,
            }
        };

        // Frozen gen=1: id=1 @ the query vector (distance ~0).
        let frozen = make_gen(1, [0.1, 0.2, 0.3, 0.4], 1);
        // Active gen=2: id=1 moved FAR — the newest version of the PK.
        let active = make_gen(1, [9.0, 9.0, 9.0, 9.0], 2);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
            shard_id,
            InMemoryMemTables {
                active,
                frozen: vec![frozen],
            },
        );

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 1.0)
            .await
            .expect("planner should produce a plan");
        let ctx = SessionContext::new();
        let batches: Vec<RecordBatch> = plan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut id1_distances = Vec::new();
        for b in &batches {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dist = b
                .column_by_name(DISTANCE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                if ids.value(i) == 1 {
                    id1_distances.push(dist.value(i));
                }
            }
        }

        // id=1 must surface at most once, and only at its NEWEST (far) distance —
        // never the stale near ~0 copy the frozen generation still indexes.
        assert!(
            id1_distances.len() <= 1,
            "id=1 leaked its stale frozen copy (appears {} times): {id1_distances:?}",
            id1_distances.len()
        );
        if let Some(d) = id1_distances.first() {
            assert!(
                *d > 1.0,
                "id=1 surfaced at the stale near distance {d}; its newest version is far"
            );
        }
    }

    #[tokio::test]
    async fn test_vector_search_with_projection_returns_distance_and_pk() {
        // Regression for: active arm previously did NOT call `build_projection_for_knn`,
        // so when the caller passed `projection=Some([...])`, the active arm's output
        // dropped the PK column (and the staleness/dedup pipeline lost its hash input).
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[10]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = create_test_batch(&schema, &[1, 2, 3, 4]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        // Caller asks for ONLY "vector"; PK "id" must still be in output because
        // canonical_output_schema auto-includes it.
        let query = create_query_vector();
        let projection = vec!["vector".to_string()];
        let plan = planner
            .plan_search(&query, 3, 1, Some(&projection), false, 1.0)
            .await
            .expect("planner should produce a plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "expected at least one result row");

        let out_schema = batches[0].schema();
        assert!(
            out_schema.field_with_name("id").is_ok(),
            "PK column `id` should be auto-included even when user projects only `vector`"
        );
        assert!(out_schema.field_with_name("vector").is_ok());
        assert!(out_schema.field_with_name(DISTANCE_COLUMN).is_ok());
    }

    #[tokio::test]
    async fn test_vector_search_projection_with_explicit_distance_and_rowid() {
        // Regression: `_distance` / `_rowid` in projection must not break
        // the plan. `_distance` honored at requested position (no
        // duplication); `_rowid` honored as nullable.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[10]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = create_test_batch(&schema, &[1, 2, 3, 4]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        // Caller explicitly puts `_distance` and `_rowid` in projection.
        let projection = vec![
            "_distance".to_string(),
            "vector".to_string(),
            "_rowid".to_string(),
        ];
        let plan = planner
            .plan_search(&query, 3, 1, Some(&projection), false, 1.0)
            .await
            .expect(
                "planner must accept `_distance`/`_rowid` in projection without breaking the plan",
            );

        let ctx = SessionContext::new();
        let stream = plan
            .execute(0, ctx.task_ctx())
            .expect("plan must execute when `_distance`/`_rowid` are in projection");
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "expected at least one result row");

        let out_schema = batches[0].schema();
        // `_distance` must appear exactly once — auto-managed, never duplicated.
        let distance_count = out_schema
            .fields()
            .iter()
            .filter(|f| f.name() == DISTANCE_COLUMN)
            .count();
        assert_eq!(
            distance_count,
            1,
            "`_distance` must appear exactly once in output, got schema: {:?}",
            out_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        // User columns are still present.
        assert!(out_schema.field_with_name("vector").is_ok());
        // PK auto-included for staleness detection.
        assert!(out_schema.field_with_name("id").is_ok());
        // Top hit comes from the active memtable. Active-arm `_rowid` is
        // NULL by design (BatchStore position, not a Lance row id). Real
        // `_rowid` is only produced for base-table rows.
        assert!(out_schema.field_with_name("_rowid").is_ok());
        let rowid = batches[0].column_by_name("_rowid").unwrap();
        assert!(
            rowid.is_null(0),
            "active-memtable `_rowid` must be NULL (not a real Lance row id), got: {:?}",
            rowid
        );
    }

    #[tokio::test]
    async fn test_vector_search_strips_internal_columns_and_preserves_active_rows() {
        // Two regressions in one test:
        // (1) The plan must not leak internal columns (`_memtable_gen`,
        //     `_freshness`) into the user-visible output.
        // (2) Active-memtable rows must reach the output — the UnionExec puts
        //     them in non-zero partitions, and any downstream node that only
        //     reads partition 0 would silently drop them.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[10]);
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let batch = create_test_batch(&schema, &[1, 2, 3, 4]);
        batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 1.0)
            .await
            .expect("planner should produce a plan");

        // Each arm is independently newest-per-PK (active append-only data can
        // use HNSW directly; rewritten active data falls back to exact search)
        // and the block-list handles cross-gen, merged by a distance SPM. No
        // global PK dedup or source tag node is involved.
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(
            !plan_str.contains("LsmGlobalPkDedupExec") && !plan_str.contains("LsmSourceTagExec"),
            "vector plan must not contain a global PK dedup or source tag node, got:\n{}",
            plan_str
        );
        assert!(
            plan_str.contains("VectorIndexExec") && plan_str.contains("SortPreservingMergeExec"),
            "expected append-only active HNSW + distance merge, got:\n{}",
            plan_str
        );

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "expected at least one result row");

        let out_schema = batches[0].schema();
        assert!(out_schema.field_with_name(DISTANCE_COLUMN).is_ok());
        for internal in [super::super::exec::MEMTABLE_GEN_COLUMN, "_freshness"] {
            assert!(
                out_schema.field_with_name(internal).is_err(),
                "`{}` leaked into output: {:?}",
                internal,
                out_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            );
        }

        // (2) Active-memtable rows must survive: the union emits base as
        // partition 0 and the active memtable as partition 1+. The active
        // memtable holds ids 1..=4; the base holds id 10. At least one id in
        // 1..=4 must appear, otherwise the SortPreservingMerge dropped the
        // non-zero partitions.
        let mut all_ids: Vec<i32> = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column_by_name("id")
                .expect("id column missing")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column should be Int32");
            for i in 0..batch.num_rows() {
                all_ids.push(id_col.value(i));
            }
        }
        assert!(
            all_ids.iter().any(|&id| (1..=4).contains(&id)),
            "expected at least one active-memtable row (id in 1..=4) — none found, so \
             active partitions were silently dropped. Got ids: {:?}",
            all_ids
        );
    }

    #[tokio::test]
    async fn test_vector_search_dedup_across_generations() {
        // Regression: same primary key inserted into two sources (older
        // SSTable gen and newer active memtable) with different vectors.
        // Without the cross-source PK dedup the older SSTable row would
        // still appear in top-k. The newer-generation row must win.
        //
        // We simulate a "SSTable gen 1" by writing a tiny Lance dataset
        // under {base_uri}/_mem_wal/{shard}/gen_1 and pointing the
        // collector at it. Real flush would reverse-write, but for this
        // test we only have one row in the SSTable gen so order is moot.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let base_uri = format!("{}/base", base_path);

        // SSTable gen 1 holds an older version of pk=1 with a "wrong" vector.
        let shard_id = uuid::Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let old_pk1 = create_test_batch_with_vector(&schema, 1, [9.0, 9.0, 9.0, 9.0]);
        create_dataset(&gen1_uri, vec![old_pk1]).await;

        // Active memtable holds the newer version of pk=1 with the
        // "right" vector close to the query, plus an unrelated pk=2.
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let new_pk1 = create_test_batch_with_vector(&schema, 1, [0.1, 0.2, 0.3, 0.4]);
        let other = create_test_batch_with_vector(&schema, 2, [5.0, 5.0, 5.0, 5.0]);
        let (_, _, bp1) = batch_store.append(new_pk1.clone()).unwrap();
        index_store
            .insert_with_batch_position(&new_pk1, 0, Some(bp1))
            .unwrap();
        let (_, _, bp2) = batch_store.append(other.clone()).unwrap();
        index_store
            .insert_with_batch_position(&other, 1, Some(bp2))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![shard_snapshot])
            .with_in_memory_memtables(
                shard_id,
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema: schema.clone(),
                        generation: 2,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 5, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

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
        let pk1_count = ids.iter().filter(|i| **i == 1).count();
        assert_eq!(
            pk1_count, 1,
            "pk=1 must appear exactly once in the merged top-k; got ids={:?}",
            ids,
        );
    }

    #[tokio::test]
    async fn test_vector_search_system_columns_real_only_for_base() {
        // Covers three properties of the per-source system columns:
        //   1. base-hit `_rowid`/`_rowaddr` carry real values
        //   2. SSTable arm runs without erroring
        //   3. `_rowaddr` symmetry with `_rowid` (same code path, both are
        //      surfaced when requested and NULL'd outside the base arm)
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        // Base: id=1 (vector closest to query). Index it so fast_search
        // returns it.
        let base_batch = create_test_batch(&schema, &[1]);
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        // SSTable: id=2 (a separate Lance dataset under
        // {base_uri}/_mem_wal/{shard}/gen_1) with its own vector index.
        let shard_id = uuid::Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let gen1_batch = create_test_batch(&schema, &[2]);
        let mut gen1_dataset = create_dataset(&gen1_uri, vec![gen1_batch]).await;
        gen1_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();

        // Active memtable: id=3 with HNSW index.
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let active_batch = create_test_batch(&schema, &[3]);
        batch_store.append(active_batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        let collector = LsmDataSourceCollector::new(base_dataset, vec![shard_snapshot])
            .with_in_memory_memtables(
                shard_id,
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema: schema.clone(),
                        generation: 2,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        // Project both system columns alongside data columns.
        let query = create_query_vector();
        let projection = vec![
            "id".to_string(),
            "_rowid".to_string(),
            "_rowaddr".to_string(),
            "vector".to_string(),
        ];
        let plan = planner
            .plan_search(&query, 3, 1, Some(&projection), false, 1.0)
            .await
            .expect("planner should produce a plan");

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Top-3 over 3 ids, one per source. All should appear.
        assert_eq!(total, 3, "expected one row per source");

        // Group by id → (rowid_null, rowaddr_null).
        let mut seen: std::collections::HashMap<i32, (bool, bool)> =
            std::collections::HashMap::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rowid = batch.column_by_name("_rowid").unwrap();
            let rowaddr = batch.column_by_name("_rowaddr").unwrap();
            for i in 0..batch.num_rows() {
                seen.insert(ids.value(i), (rowid.is_null(i), rowaddr.is_null(i)));
            }
        }

        // id=1 (base): `_rowid` real (the index produces it). `_rowaddr` is
        // always NULL across vector_search — Lance's `fast_search()` can't
        // be combined with `with_row_address()`.
        let (rid_null, raddr_null) = seen.get(&1).expect("base row id=1 missing");
        assert!(
            !rid_null,
            "base row `_rowid` must be real (Lance row id), got NULL"
        );
        assert!(
            raddr_null,
            "`_rowaddr` is incompatible with vector_search's fast_search; must be NULL"
        );

        // id=2 (SSTable): both NULL — per-source values would collide with base.
        let (rid_null, raddr_null) = seen.get(&2).expect("SSTable row id=2 missing");
        assert!(rid_null, "SSTable row `_rowid` must be NULL");
        assert!(raddr_null, "SSTable row `_rowaddr` must be NULL");

        // id=3 (active): both NULL — BatchStore position is not a Lance row id.
        let (rid_null, raddr_null) = seen.get(&3).expect("active row id=3 missing");
        assert!(rid_null, "active row `_rowid` must be NULL");
        assert!(raddr_null, "active row `_rowaddr` must be NULL");
    }

    #[tokio::test]
    async fn test_vector_search_empty_plan_with_system_columns() {
        // Test 5 (vector_search slice): with no sources, the empty plan
        // must still expose user-requested system columns at the requested
        // position, plus `_distance` (always-on for KNN).
        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let projection = vec![
            "_rowid".to_string(),
            "vector".to_string(),
            "_rowaddr".to_string(),
        ];
        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 5, 1, Some(&projection), false, 1.0)
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
                "_rowid".to_string(),
                "vector".to_string(),
                "_rowaddr".to_string(),
                "id".to_string(),        // PK auto-appended
                "_distance".to_string(), // always-on for KNN
            ],
            "empty KNN plan must honor user position for system cols and append PK + _distance"
        );
    }

    #[tokio::test]
    async fn test_vector_search_rejects_missing_projection_column() {
        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let projection = vec!["missing".to_string()];
        let query = create_query_vector();
        let err = planner
            .plan_search(&query, 5, 1, Some(&projection), false, 1.0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing-column projection error: {err}"
        );
    }

    #[tokio::test]
    async fn test_vector_search_without_base_table() {
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        // No base dataset written. Plan construction must still succeed and
        // exclude any base-table scan node.
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 10, 8, None, false, 1.0)
            .await
            .expect("planner should produce a plan without a base table");

        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(
            !plan_str.contains("base/data"),
            "Plan must not scan base table, got: {}",
            plan_str
        );

        // Execute the plan so runtime issues (schema mismatches, missing
        // sources, etc.) surface here rather than at the call site.
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan
            .execute(0, ctx.task_ctx())
            .expect("plan should execute without a base table");
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .expect("collecting batches should succeed");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "fresh tier with no sources should yield no rows");
    }

    /// Build a single-row batch with an explicit (id, vector) so tests can
    /// pin the within-source dedup against same-PK / different-vector
    /// inputs.
    fn create_test_batch_with_vector(
        schema: &ArrowSchema,
        id: i32,
        vector: [f32; 4],
    ) -> RecordBatch {
        use arrow_array::builder::Float32Builder;

        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for v in &vector {
            vector_builder.values().append_value(*v);
        }
        vector_builder.append(true);
        let vector_array = vector_builder.finish();

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![id])), Arc::new(vector_array)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_vector_search_dedup_within_active_memtable() {
        // Regression: same PK inserted twice into one active memtable with
        // *different* vectors. The exact active search must consider only the
        // newest version of each PK before top-k. The newer insert must win.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );

        // Two rows with pk=1, different vectors. b_new (last insert) is the
        // newer version that the LSM contract says should win.
        let b_old = create_test_batch_with_vector(&schema, 1, [9.0, 9.0, 9.0, 9.0]);
        let b_new = create_test_batch_with_vector(&schema, 1, [0.1, 0.2, 0.3, 0.4]);
        // An unrelated row so top-k has more than one PK to choose from.
        let b_other = create_test_batch_with_vector(&schema, 2, [5.0, 5.0, 5.0, 5.0]);

        let (_, _, bp_old) = batch_store.append(b_old.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_old, 0, Some(bp_old))
            .unwrap();
        let (_, _, bp_new) = batch_store.append(b_new.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_new, 1, Some(bp_new))
            .unwrap();
        let (_, _, bp_other) = batch_store.append(b_other.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b_other, 2, Some(bp_other))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        // Query is exactly the *newer* vector for pk=1. The exact active
        // memtable arm should dedup before top-k so pk=1 appears exactly once.
        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 5, 1, None, false, 1.0)
            .await
            .unwrap();

        // The active arm uses exact brute force when PK recency is configured;
        // there is no cross-source dedup fallback.
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(
            plan_str.contains("MemTableBruteForceVectorExec"),
            "active vector arm must use exact brute-force PK recency, got:\n{}",
            plan_str
        );

        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

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
        let pk1_count = ids.iter().filter(|i| **i == 1).count();
        assert_eq!(
            pk1_count, 1,
            "pk=1 must appear exactly once after within-source dedup; got ids={:?}",
            ids,
        );
    }

    #[tokio::test]
    async fn test_vector_search_active_stale_update_out_of_neighborhood() {
        // BUG REPRODUCTION (vector case: a PK update that moves out of the neighborhood).
        //
        // Within a *single* active memtable, pk=1 is first inserted ON the query
        // (distance ~0), then updated to a FAR vector. Exact brute force must
        // compute newest-per-PK over the whole memtable before top-k, so the
        // stale near row is dropped even though the fresh row is far away.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );

        // First append: stale pk=1 ON the query, plus five filler rows strictly
        // farther than pk=1 but far nearer than the eventual fresh pk=1.
        let q = [0.1, 0.2, 0.3, 0.4];
        let stale_then_fillers = batch_rows(
            &schema,
            &[
                (1, q),
                (10, [0.11, 0.21, 0.31, 0.41]),
                (11, [0.13, 0.23, 0.33, 0.43]),
                (12, [0.15, 0.25, 0.35, 0.45]),
                (13, [0.17, 0.27, 0.37, 0.47]),
                (14, [0.19, 0.29, 0.39, 0.49]),
            ],
        );
        let (bp0, off0, _) = batch_store.append(stale_then_fillers.clone()).unwrap();
        index_store
            .insert_with_batch_position(&stale_then_fillers, off0, Some(bp0))
            .unwrap();

        // Second append: the UPDATE — pk=1 moved far from the query. This is the
        // newest version (largest row position) but it sits well outside top-k.
        let fresh_pk1 = batch_rows(&schema, &[(1, [9.0, 9.0, 9.0, 9.0])]);
        let (bp1, off1, _) = batch_store.append(fresh_pk1.clone()).unwrap();
        index_store
            .insert_with_batch_position(&fresh_pk1, off1, Some(bp1))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        // k=3, no over-fetch: exact active search still computes
        // newest-per-PK across the whole memtable before top-k, so stale
        // pk1@near cannot leak even though fresh pk1@far ranks 7th by distance.
        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        assert_eq!(
            rows.len(),
            3,
            "active PK recency filtering must not underfill k after dropping stale candidates; \
             results={:?}",
            rows
        );
        assert!(
            !rows.iter().any(|&(id, d)| id == 1 && d.abs() < 1e-3),
            "stale near pk=1 leaked: its live vector is far from the query, so it \
             must not appear at distance ~0. results={:?}",
            rows
        );
    }

    #[tokio::test]
    async fn test_vector_search_stale_read_when_fresh_falls_out_of_top_k() {
        // Regression for the cross-generation stale-read gap that the
        // PkBlockFilterExec block-list closes.
        //
        // Scenario:
        //   * Base (gen 0): stale pk=1 sitting on the query (distance ~0).
        //   * Active (gen 1): pk=1 updated to a far vector, plus pk=2 closer
        //     to the query than fresh pk=1. With k=1 the active arm surfaces
        //     pk=2 and drops fresh pk=1, so without the block-list the stale
        //     base copy of pk=1 wins top-1.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        // Base table (gen 0): the stale copy of pk=1 sits exactly on the query.
        let base_batch = create_test_batch_with_vector(&schema, 1, [0.1, 0.2, 0.3, 0.4]);
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        // Active memtable (gen 1): pk=1 updated to a far vector, plus a pk=2
        // that is closer to the query than fresh pk=1 — so with k=1 the
        // active arm surfaces pk=2 and drops fresh pk=1.
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let fresh_pk1 = create_test_batch_with_vector(&schema, 1, [9.0, 9.0, 9.0, 9.0]);
        let pk2 = create_test_batch_with_vector(&schema, 2, [1.0, 1.0, 1.0, 1.0]);
        let (_, _, bp1) = batch_store.append(fresh_pk1.clone()).unwrap();
        index_store
            .insert_with_batch_position(&fresh_pk1, 0, Some(bp1))
            .unwrap();
        let (_, _, bp2) = batch_store.append(pk2.clone()).unwrap();
        index_store
            .insert_with_batch_position(&pk2, 1, Some(bp2))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 1, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut rows: Vec<(i32, f32)> = Vec::new();
        for b in &batches {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dist = b
                .column_by_name(DISTANCE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((ids.value(i), dist.value(i)));
            }
        }

        // pk=1 was updated to a far vector; it must never be served at the
        // stale ~0 distance of the superseded base-table copy.
        assert!(
            rows.iter().all(|&(id, d)| !(id == 1 && d.abs() < 1e-3)),
            "stale read: pk=1 was updated to a far vector in gen 1, but the \
             stale base-table copy (distance ~0) was served because fresh \
             pk=1 fell out of the active arm's top-k and never deduped it; \
             got {:?}",
            rows
        );
        // Positive check: with the stale copy suppressed, the nearest *live*
        // neighbor is pk=2 — the top-1 result, not an empty or dropped-everything
        // result.
        assert_eq!(
            rows.len(),
            1,
            "k=1 must return exactly one row, got {:?}",
            rows
        );
        assert_eq!(
            rows[0].0, 2,
            "expected nearest live neighbor pk=2, got {:?}",
            rows
        );

        // The block-list is unconditional and cannot be disabled via
        // overfetch_factor; invalid sub-1.0 values are rejected instead.
        let err = planner
            .plan_search(&query, 1, 1, None, false, 0.0)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("overfetch_factor"),
            "unexpected error for invalid overfetch factor: {err}"
        );
    }

    /// Build a multi-row (id, vector) batch with explicit vectors, so a test can
    /// place some rows on the query and others nearby.
    fn batch_rows(schema: &ArrowSchema, rows: &[(i32, [f32; 4])]) -> RecordBatch {
        use arrow_array::builder::Float32Builder;
        let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for (_, v) in rows {
            for x in v {
                vb.values().append_value(*x);
            }
            vb.append(true);
        }
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(ids)), Arc::new(vb.finish())],
        )
        .unwrap()
    }

    /// Collect (id, distance) pairs from a KNN result stream.
    fn collect_id_dist(batches: &[RecordBatch]) -> Vec<(i32, f32)> {
        let mut rows = Vec::new();
        for b in batches {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dist = b
                .column_by_name(DISTANCE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((ids.value(i), dist.value(i)));
            }
        }
        rows
    }

    #[tokio::test]
    async fn test_vector_search_overfetch_backfills_when_top_k_all_stale() {
        // A source whose entire top-k is stale must still yield k live results
        // from its next-nearest rows: the over-fetch backfills the dropped rows.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        // Base (gen 0): pk 1,2,3 sit ON the query (stale); pk 4,5,6 are nearby
        // and live. The nearest 3 base rows are all about to be superseded.
        let q = [0.1, 0.2, 0.3, 0.4];
        let near = [0.12, 0.22, 0.32, 0.42];
        let base_batch = batch_rows(
            &schema,
            &[(1, q), (2, q), (3, q), (4, near), (5, near), (6, near)],
        );
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        // Active (gen 1): pk 1,2,3 re-inserted with a far vector (the fresh value).
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let far = [9.0, 9.0, 9.0, 9.0];
        let active_batch = batch_rows(&schema, &[(1, far), (2, far), (3, far)]);
        let (_, _, bp) = batch_store.append(active_batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_batch, 0, Some(bp))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        // Over-fetch (2.5x) so the post-filter can backfill the all-stale top-k.
        let plan = planner
            .plan_search(&query, 3, 1, None, false, 2.5)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        // No under-fill: still exactly k=3 rows, the next-nearest live ones.
        assert_eq!(rows.len(), 3, "expected k=3 live results, got {:?}", rows);
        let ids: std::collections::HashSet<i32> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            std::collections::HashSet::from([4, 5, 6]),
            "expected the next-nearest live rows {{4,5,6}}, got {:?}",
            rows
        );
        // No stale read: no pk in 1..=3 served at the superseded ~0 distance.
        assert!(
            rows.iter()
                .all(|&(id, d)| !((1..=3).contains(&id) && d.abs() < 1e-3)),
            "stale read: a superseded base row was served; got {:?}",
            rows
        );
    }

    #[tokio::test]
    async fn test_vector_search_sstable_superseded_by_newer_sstable() {
        // An older SSTable's stale row must be suppressed by a newer
        // SSTable (cross-SSTable blocking, no base/active involved).
        use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let shard_id = uuid::Uuid::new_v4();
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);

        let q = [0.1, 0.2, 0.3, 0.4];
        let far = [9.0, 9.0, 9.0, 9.0];
        let near = [0.12, 0.22, 0.32, 0.42];

        // gen 1 (older): stale pk=1 sitting on the query.
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let mut gen1 = create_dataset(&gen1_uri, vec![batch_rows(&schema, &[(1, q)])]).await;
        gen1.create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();

        // gen 2 (newer): fresh pk=1 (far) + an unrelated nearby pk=2.
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        let mut gen2 =
            create_dataset(&gen2_uri, vec![batch_rows(&schema, &[(1, far), (2, near)])]).await;
        gen2.create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();

        let snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(3)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![snapshot]);

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 1, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        // gen1's stale pk=1 (distance ~0) is blocked by gen2; the nearest live
        // neighbor is the unrelated pk=2.
        assert_eq!(rows.len(), 1, "expected one result, got {:?}", rows);
        assert_eq!(
            rows[0].0, 2,
            "expected nearest live row pk=2, got {:?}",
            rows
        );
    }

    fn create_multicol_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id1", DataType::Int32, false),
            Field::new("id2", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]))
    }

    fn multicol_batch(schema: &ArrowSchema, rows: &[((i32, i32), [f32; 4])]) -> RecordBatch {
        use arrow_array::builder::Float32Builder;
        let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
        for (_, v) in rows {
            for x in v {
                vb.values().append_value(*x);
            }
            vb.append(true);
        }
        let id1: Vec<i32> = rows.iter().map(|((a, _), _)| *a).collect();
        let id2: Vec<i32> = rows.iter().map(|((_, b), _)| *b).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(id1)),
                Arc::new(Int32Array::from(id2)),
                Arc::new(vb.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_vector_search_stale_read_with_composite_pk() {
        // The block-list must key on the full composite PK: a base row updated in
        // the active memtable (matched on (id1,id2)) must be suppressed.
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use lance_index::IndexType;

        let schema = create_multicol_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let q = [0.1, 0.2, 0.3, 0.4];
        let far = [9.0, 9.0, 9.0, 9.0];
        let near = [0.12, 0.22, 0.32, 0.42];

        // Base: composite pk (1,1) on the query (stale).
        let base_batch = multicol_batch(&schema, &[((1, 1), q)]);
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;
        let ivf_flat = VectorIndexParams::ivf_flat(1, lance_linalg::distance::DistanceType::L2);
        base_dataset
            .create_index(&["vector"], IndexType::Vector, None, &ivf_flat, true)
            .await
            .unwrap();
        let base_dataset = Arc::new(base_dataset);

        // Active: (1,1) re-inserted far (fresh) + an unrelated nearby (2,2).
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id1".to_string(), 0), ("id2".to_string(), 1)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        let active_batch = multicol_batch(&schema, &[((1, 1), far), ((2, 2), near)]);
        let (_, _, bp) = batch_store.append(active_batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_batch, 0, Some(bp))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::new(base_dataset, vec![]).with_in_memory_memtables(
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id1".to_string(), "id2".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 1, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // The stale composite-key (1,1) at distance ~0 must not be served; the
        // nearest live neighbor is (2,2).
        let mut rows: Vec<(i32, i32, f32)> = Vec::new();
        for b in &batches {
            let id1 = b
                .column_by_name("id1")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let id2 = b
                .column_by_name("id2")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let dist = b
                .column_by_name(DISTANCE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((id1.value(i), id2.value(i), dist.value(i)));
            }
        }
        assert_eq!(rows.len(), 1, "expected one result, got {:?}", rows);
        assert_eq!(
            (rows[0].0, rows[0].1),
            (2, 2),
            "expected nearest live composite key (2,2), got {:?}",
            rows
        );
    }

    #[tokio::test]
    async fn test_vector_search_prefilter_excludes_composite_pk_newest_version() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use arrow_array::StringArray;
        use arrow_array::builder::Float32Builder;
        use datafusion::prelude::{SessionContext, col, lit};
        use futures::TryStreamExt;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id1", DataType::Int32, false),
            Field::new("id2", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
        ]));

        fn batch(schema: &ArrowSchema, rows: &[((i32, i32), [f32; 4], &str)]) -> RecordBatch {
            let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), 4);
            for (_, vector, _) in rows {
                for value in vector {
                    vb.values().append_value(*value);
                }
                vb.append(true);
            }
            let id1: Vec<i32> = rows.iter().map(|((id1, _), _, _)| *id1).collect();
            let id2: Vec<i32> = rows.iter().map(|((_, id2), _, _)| *id2).collect();
            let status: Vec<&str> = rows.iter().map(|(_, _, status)| *status).collect();
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int32Array::from(id1)),
                    Arc::new(Int32Array::from(id2)),
                    Arc::new(vb.finish()),
                    Arc::new(StringArray::from(status)),
                ],
            )
            .unwrap()
        }

        let q = [0.1, 0.2, 0.3, 0.4];
        let near = [0.12, 0.22, 0.32, 0.42];
        let far = [9.0, 9.0, 9.0, 9.0];
        let batch = batch(
            &schema,
            &[
                ((1, 1), q, "active"),
                ((1, 1), far, "archived"),
                ((2, 2), near, "active"),
            ],
        );

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id1".to_string(), 0), ("id2".to_string(), 1)]);
        let (_, row_offset, batch_position) = batch_store.append(batch.clone()).unwrap();
        index_store
            .insert_with_batch_position(&batch, row_offset, Some(batch_position))
            .unwrap();
        let index_store = Arc::new(index_store);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let shard_id = uuid::Uuid::new_v4();
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id1".to_string(), "id2".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        )
        .with_filter(Some(col("status").eq(lit("active"))));

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 10, 1, None, false, 1.0)
            .await
            .unwrap();
        let stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut ids = Vec::new();
        for batch in &batches {
            let id1 = batch
                .column_by_name("id1")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let id2 = batch
                .column_by_name("id2")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                ids.push((id1.value(row), id2.value(row)));
            }
        }

        assert_eq!(
            ids,
            vec![(2, 2)],
            "composite PK (1,1)'s newest version is archived and must exclude \
             the older active version; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn test_vector_search_same_l0_override_newest_wins() {
        // Ported from the #6844 spec. The DANGEROUS within-memtable direction:
        // a PK is re-inserted in the SAME active memtable with a *farther* vector,
        // while the stale earlier copy sits ON the query. Newest-wins must keep
        // the newer far copy and exclude the stale near one — unlike
        // `test_vector_search_dedup_within_active_memtable`, which keeps the newer
        // copy only because it is also the closer one (a weaker check).
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;

        let schema = create_vector_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());

        let on_query = [0.1, 0.2, 0.3, 0.4]; // == query: the STALE copy of id=1
        let far = [9.0, 9.0, 9.0, 9.0]; // the FRESH (newer) copy of id=1
        let other = [1.0, 1.0, 1.0, 1.0]; // unrelated id=2, not on the query

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_hnsw(
            "vector_hnsw".to_string(),
            1,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
            64,
            8,
        );
        // Batch 0 @ positions 0,1: stale id=1 on the query, plus id=2.
        let b0 = batch_rows(&schema, &[(1, on_query), (2, other)]);
        let (_, _, bp0) = batch_store.append(b0.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b0, 0, Some(bp0))
            .unwrap();
        // Batch 1 @ position 2: id=1 re-inserted with the newer far vector.
        let b1 = batch_rows(&schema, &[(1, far)]);
        let (_, _, bp1) = batch_store.append(b1.clone()).unwrap();
        index_store
            .insert_with_batch_position(&b1, 2, Some(bp1))
            .unwrap();
        let index_store = Arc::new(index_store);

        let shard_id = uuid::Uuid::new_v4();
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

        let planner = LsmVectorSearchPlanner::new(
            collector,
            vec!["id".to_string()],
            schema,
            "vector".to_string(),
            lance_linalg::distance::DistanceType::L2,
        );

        let query = create_query_vector();
        let plan = planner
            .plan_search(&query, 5, 1, None, false, 1.0)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let rows = collect_id_dist(&batches);

        let id1: Vec<f32> = rows
            .iter()
            .filter(|&&(id, _)| id == 1)
            .map(|&(_, d)| d)
            .collect();
        assert_eq!(
            id1.len(),
            1,
            "newest-wins: id=1 must appear exactly once after a same-L0 override, got {:?}",
            rows
        );
        assert!(
            id1[0] > 1.0,
            "newest-wins: surviving id=1 must be the newer far vector, not the stale near one — got distance {}",
            id1[0]
        );
        assert!(
            rows.iter().all(|&(_, d)| d.abs() >= 1e-3),
            "newest-wins: the stale on-query copy (distance ~0) must be excluded, got {:?}",
            rows
        );
    }
}
