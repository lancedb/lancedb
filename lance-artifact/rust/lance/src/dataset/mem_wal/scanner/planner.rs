// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Query planner for LSM scanner.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, limit::GlobalLimitExec};
use datafusion::prelude::{Expr, col};
use lance_core::Result;
use tracing::instrument;

use crate::dataset::mem_wal::TOMBSTONE;

use super::collector::LsmDataSourceCollector;
use super::data_source::LsmDataSource;
use super::exec::{MEMTABLE_GEN_COLUMN, MemtableGenTagExec, PkBlockFilterExec, ROW_ADDRESS_COLUMN};
use super::projection::{
    build_scanner_projection, canonical_output_schema, null_columns, project_to_canonical,
    validate_projection_names,
};
use super::sstable_cache::{DatasetCache, SsTableWarmer, open_sstable};
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// Combine the user filter (if any) with `NOT _tombstone` so tombstone rows are
/// dropped from a WAL-arm scan. Used only for sources whose schema carries the
/// column (active / SSTables written since deletes existed).
fn fold_not_tombstone(filter: Option<&Expr>) -> Expr {
    let live = !col(TOMBSTONE);
    match filter {
        Some(expr) => expr.clone().and(live),
        None => live,
    }
}

/// Plans scan queries over LSM data.
pub struct LsmScanPlanner {
    /// Data source collector.
    collector: LsmDataSourceCollector,
    /// Primary key column names.
    pk_columns: Vec<String>,
    /// Schema of the base table.
    base_schema: SchemaRef,
    /// Session threaded into SSTable opens (shared caches).
    session: Option<Arc<Session>>,
    /// Store params for opening SSTables, reusing the base dataset's store.
    store_params: Option<ObjectStoreParams>,
    /// Cache of opened SSTable datasets.
    sstable_cache: Option<Arc<dyn DatasetCache>>,
    /// Optional warmer fired on first open of an SSTable.
    warmer: Option<Arc<dyn SsTableWarmer>>,
}

impl LsmScanPlanner {
    /// Create a new planner.
    pub fn new(
        collector: LsmDataSourceCollector,
        pk_columns: Vec<String>,
        base_schema: SchemaRef,
    ) -> Self {
        Self {
            collector,
            pk_columns,
            base_schema,
            session: None,
            store_params: None,
            sstable_cache: None,
            warmer: None,
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
    /// queries against the same generation a pure `Arc::clone`.
    pub fn with_sstable_cache(mut self, cache: Arc<dyn DatasetCache>) -> Self {
        self.sstable_cache = Some(cache);
        self
    }

    /// Inject the warmer fired on first open of an SSTable.
    pub fn with_warmer(mut self, warmer: Arc<dyn SsTableWarmer>) -> Self {
        self.warmer = Some(warmer);
        self
    }

    /// Create scan plan with deduplication.
    ///
    /// # Arguments
    ///
    /// * `projection` - Columns to include in output (None = all columns)
    /// * `filter` - Filter expression to apply
    /// * `limit` - Maximum rows to return
    /// * `offset` - Number of rows to skip
    /// * `with_memtable_gen` - Whether to include _memtable_gen in output
    /// * `keep_row_address` - Whether to include _rowaddr in output
    ///
    /// # Query plan
    ///
    /// Each source is independently newest-per-PK (active via the fused
    /// [`MemTableDedupScanExec`](super::super::memtable::scanner), flushed via
    /// its within-generation deletion vector) and a cross-generation block-list
    /// ([`PkBlockFilterExec`]) drops any PK superseded by a newer generation.
    /// Each PK therefore survives in exactly one source, so a plain
    /// `UnionExec` carries at most one row per PK — no cross-source dedup,
    /// sort, or merge needed. `_memtable_gen` / `_rowaddr` are output-only and
    /// only produced when the caller opts in.
    #[instrument(name = "lsm_plan_scan", level = "debug", skip_all, fields(has_filter = filter.is_some(), limit, offset))]
    pub async fn plan_scan(
        &self,
        projection: Option<&[String]>,
        filter: Option<&Expr>,
        limit: Option<usize>,
        offset: Option<usize>,
        with_memtable_gen: bool,
        keep_row_address: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If the caller explicitly listed `_rowaddr` in the projection, force
        // dedup to retain it so the canonical projection can surface real
        // values (instead of NULL-filling). Other system columns aren't
        // produced by dedup and remain NULL across LSM sources.
        let user_wants_rowaddr = projection
            .map(|p| p.iter().any(|c| c == ROW_ADDRESS_COLUMN))
            .unwrap_or(false);
        let keep_row_address = keep_row_address || user_wants_rowaddr;
        validate_projection_names(projection, &self.base_schema, &[])?;

        // 1. Collect all data sources
        let sources = self.collector.collect()?;

        if sources.is_empty() {
            // Return empty plan
            return self.empty_plan(projection, with_memtable_gen, keep_row_address);
        }

        // Cross-generation block-list keyed by source: a hit drops any row
        // whose PK lives in a newer generation, applied before the union.
        // `Box::pin` keeps the future off `clippy::large_futures`.
        let block_lists = Box::pin(super::block_list::compute_source_block_lists(
            &sources,
            self.session.as_ref(),
            self.store_params.as_ref(),
            self.sstable_cache.as_ref(),
        ))
        .await?;

        // Reverse so the union lists the newest generation first. This is
        // cosmetic — correctness comes from the per-source dedup and the
        // cross-gen block-list, not from output ordering.
        let sources: Vec<_> = sources.into_iter().rev().collect();

        // Per-source limit pushdown: an unordered LIMIT needs only
        // `offset + limit` live rows from each source to fill the global limit
        // (any-N semantics). However, a source with a cross-generation block
        // list can lose any number of rows after its scan, so a finite fetch
        // before that filter is not safe. Leave those scans unbounded and let
        // the LocalLimitExec below pull until it sees `n_needed` live rows or
        // reaches EOF. Sources without a block filter can still push the limit
        // down safely. The active memtable is in-memory and is never capped.
        let n_needed = limit.map(|l| l.saturating_add(offset.unwrap_or(0)));

        let mut source_plans = Vec::new();
        for source in sources {
            let is_base = matches!(source, LsmDataSource::BaseTable { .. });
            let is_active = matches!(source, LsmDataSource::ActiveMemTable { .. });
            let blocked = block_lists
                .get(&(source.shard_id(), source.generation()))
                .cloned();
            let has_block_filter = blocked.is_some() && !self.pk_columns.is_empty();
            let fetch = match (n_needed, is_active, has_block_filter) {
                (Some(n), false, false) => Some(n),
                _ => None,
            };
            let scan = self
                .build_source_scan(&source, projection, filter, fetch)
                .await?;

            // Drop cross-generation stale rows (PKs superseded by a newer gen).
            // Plain scans refill exactly, so keep the approximate-search
            // under-fetch warning disabled with k = 0.
            let scan = match blocked {
                Some(set) if !self.pk_columns.is_empty() => Arc::new(PkBlockFilterExec::new(
                    scan,
                    self.pk_columns.clone(),
                    set,
                    0,
                ))
                    as Arc<dyn ExecutionPlan>,
                _ => scan,
            };

            // Post-block-list cap: each source contributes at most `n_needed`
            // live rows toward the global limit.
            let scan: Arc<dyn ExecutionPlan> = match n_needed {
                Some(n) if !is_active => Arc::new(
                    datafusion::physical_plan::limit::LocalLimitExec::new(scan, n),
                ),
                _ => scan,
            };

            // When `_rowaddr` is surfaced, NULL it for non-base arms: only base
            // values are meaningful (e.g. for `take_rows`); per-source addresses
            // collide with base IDs.
            let scan: Arc<dyn ExecutionPlan> = if !is_base && keep_row_address {
                null_columns(scan, &[ROW_ADDRESS_COLUMN])?
            } else {
                scan
            };

            // Tag with generation only if the caller wants `_memtable_gen`.
            let plan: Arc<dyn ExecutionPlan> = if with_memtable_gen {
                Arc::new(MemtableGenTagExec::new(scan, source.generation()))
            } else {
                scan
            };

            source_plans.push(plan);
        }

        // Union, then coalesce into a single partition (UnionExec emits one
        // per arm; downstream consumers only read partition 0).
        let mut plan: Arc<dyn ExecutionPlan> = if source_plans.len() == 1 {
            source_plans.remove(0)
        } else {
            #[allow(deprecated)]
            let union = Arc::new(UnionExec::new(source_plans));
            Arc::new(CoalescePartitionsExec::new(union))
        };

        // Project to the canonical output schema, dropping `_rowaddr` /
        // `_memtable_gen` unless the caller opted in.
        plan = project_to_canonical(
            plan,
            &self.canonical_scan_schema(projection, with_memtable_gen, keep_row_address),
        )?;

        // 6. Add limit / offset if specified
        if limit.is_some() || offset.unwrap_or(0) > 0 {
            plan = Arc::new(GlobalLimitExec::new(plan, offset.unwrap_or(0), limit));
        }

        Ok(plan)
    }

    /// Canonical scan output: user projection (system cols at requested
    /// positions) + `_rowaddr` / `_memtable_gen` when their flags are set.
    fn canonical_scan_schema(
        &self,
        projection: Option<&[String]>,
        with_memtable_gen: bool,
        keep_row_address: bool,
    ) -> SchemaRef {
        let canonical = canonical_output_schema(
            projection,
            &self.base_schema,
            &self.pk_columns,
            false, // no _distance
        );
        let mut fields: Vec<Arc<Field>> = canonical.fields().iter().cloned().collect();
        if keep_row_address && !fields.iter().any(|f| f.name() == ROW_ADDRESS_COLUMN) {
            fields.push(Arc::new(Field::new(
                ROW_ADDRESS_COLUMN,
                DataType::UInt64,
                true,
            )));
        }
        if with_memtable_gen && !fields.iter().any(|f| f.name() == MEMTABLE_GEN_COLUMN) {
            fields.push(Arc::new(Field::new(
                MEMTABLE_GEN_COLUMN,
                DataType::UInt64,
                false,
            )));
        }
        Arc::new(Schema::new(fields))
    }

    /// Build scan plan for a single data source.
    async fn build_source_scan(
        &self,
        source: &LsmDataSource,
        projection: Option<&[String]>,
        filter: Option<&Expr>,
        fetch: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match source {
            LsmDataSource::BaseTable { dataset } => {
                // Use Lance Scanner
                let mut scanner = dataset.scan();

                // Project columns + _rowaddr (needed for dedup)
                let cols =
                    build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                scanner.with_row_address();
                // No `with_row_id()`: opting in only for base would mismatch
                // the union schema against flushed/active. `_rowid` stays NULL
                // for every row via `project_to_canonical`.

                if let Some(expr) = filter {
                    scanner.filter_expr(expr.clone());
                }
                // Per-source limit pushdown (post-filter rows): bounds the
                // physical scan instead of trimming above the union.
                if let Some(fetch) = fetch {
                    scanner.limit(Some(fetch as i64), None)?;
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
                scanner.with_row_address();

                // Drop tombstones: fold `NOT _tombstone` into the predicate so
                // it runs before the pushdown limit (counting only live rows).
                // The older real row a tombstone supersedes is dropped by the
                // cross-gen block-list, not by this filter. Gen written before
                // deletes existed lack the column → no fold, nothing to drop.
                let folded;
                let effective: Option<&Expr> = if dataset.schema().field(TOMBSTONE).is_some() {
                    folded = fold_not_tombstone(filter);
                    Some(&folded)
                } else {
                    filter
                };
                if let Some(expr) = effective {
                    scanner.filter_expr(expr.clone());
                }
                // Per-source limit pushdown: SSTables are
                // within-gen live (dedup-on-flush deletion vectors), so any
                // `fetch` post-filter rows are valid contributions.
                if let Some(fetch) = fetch {
                    scanner.limit(Some(fetch as i64), None)?;
                }

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

                let cols =
                    build_scanner_projection(projection, &self.base_schema, &self.pk_columns);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                scanner.with_row_address();

                // The dedup scan applies the filter post-dedup; pushing it
                // into the raw scan would resurrect older versions of PKs
                // whose newest version fails the predicate. Folding
                // `NOT _tombstone` here is correct: a tombstone wins the
                // position-based dedup (suppressing the older real row) and is
                // then dropped by this predicate. A memtable without the column
                // (legacy / test) gets no fold.
                let folded;
                let effective: Option<&Expr> = if schema.column_with_name(TOMBSTONE).is_some() {
                    folded = fold_not_tombstone(filter);
                    Some(&folded)
                } else {
                    filter
                };
                if let Some(expr) = effective {
                    scanner.filter_expr(expr.clone());
                }

                scanner.create_dedup_plan(&self.pk_columns).await
            }
        }
    }

    /// Create an empty execution plan with the canonical scan output schema.
    fn empty_plan(
        &self,
        projection: Option<&[String]>,
        with_memtable_gen: bool,
        keep_row_address: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;

        let schema = self.canonical_scan_schema(projection, with_memtable_gen, keep_row_address);
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_build_projection_with_rowaddr() {
        let schema = create_test_schema();

        // Create a mock collector (we can't easily create a real one without a dataset)
        // Instead, test the projection building logic directly

        // When projection is Some, should include specified cols + PK
        let pk_columns = vec!["id".to_string()];

        let mut cols: Vec<String> = vec!["name".to_string()];
        for pk in &pk_columns {
            if !cols.contains(pk) {
                cols.push(pk.clone());
            }
        }
        assert!(cols.contains(&"name".to_string()));
        assert!(cols.contains(&"id".to_string()));

        // When projection is None, should include all schema fields
        let cols_all: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(cols_all.len(), 3);
    }

    #[test]
    fn test_shard_snapshot() {
        let shard_id = uuid::Uuid::new_v4();
        let snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(5)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());

        assert_eq!(snapshot.sstables.len(), 2);
        assert_eq!(snapshot.current_generation, 5);
    }
}

/// Integration tests that verify LSM scanner behavior with real datasets.
///
/// These tests validate:
/// - Query plan structure for different configurations
/// - Deduplication correctness across multiple LSM levels
/// - Both with and without BTree index optimization
#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use uuid::Uuid;

    use crate::dataset::mem_wal::scanner::LsmScanner;
    use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
    use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
    use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
    use crate::dataset::{Dataset, WriteParams};
    use crate::utils::test::assert_plan_node_equals;

    /// Create test schema with id as primary key.
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

    /// Create a test batch with given ids and name prefix.
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

    /// Create a dataset at the given URI with the provided batches. Also writes
    /// the standalone PK sidecar (on `id`) so an SSTable source can be
    /// probed by the block-list; harmless for a base table (never probed).
    async fn create_dataset(uri: &str, batches: Vec<RecordBatch>) -> Dataset {
        let schema = batches[0].schema();
        let has_id = schema.column_with_name("id").is_some();
        let reader = RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema);
        let dataset = Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap();
        if has_id {
            super::super::block_list::write_pk_sidecar(uri, &batches, &["id"])
                .await
                .unwrap();
        }
        dataset
    }

    /// Build an in-memory memtable's `(batch_store, index_store)` with the PK
    /// index enabled and populated (mirrors production — the block-list needs
    /// the PK index to dedup in-memory generations).
    fn pk_indexed(batches: &[RecordBatch]) -> (Arc<BatchStore>, Arc<IndexStore>) {
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let mut index = IndexStore::new();
        index.enable_pk_index(&[("id".to_string(), 0)]);
        for b in batches {
            let (bp, off, _) = batch_store.append(b.clone()).unwrap();
            index.insert_with_batch_position(b, off, Some(bp)).unwrap();
        }
        (batch_store, Arc::new(index))
    }

    /// Setup a multi-level LSM structure with:
    /// - Base table: ids 1-5 with "base" prefix
    /// - SSTable gen1: ids 3,4 (updates) with "gen1" prefix
    /// - SSTable gen2: ids 4,5 (updates) + id 6 (new) with "gen2" prefix
    /// - Active memtable: ids 5,6 (updates) + id 7 (new) with "active" prefix
    ///
    /// Expected deduplication results:
    /// - id=1: "base_1" (only in base)
    /// - id=2: "base_2" (only in base)
    /// - id=3: "gen1_3" (updated in gen1)
    /// - id=4: "gen2_4" (updated in gen1 then gen2, keep gen2)
    /// - id=5: "active_5" (updated in gen2 then active, keep active)
    /// - id=6: "active_6" (added in gen2 then updated in active, keep active)
    /// - id=7: "active_7" (added in active)
    async fn setup_multi_level_lsm() -> (
        Arc<Dataset>,
        Vec<ShardSnapshot>,
        Option<(Uuid, InMemoryMemTables)>,
        Vec<String>,
        String, // temp_dir path for cleanup
    ) {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Create base table
        let base_uri = format!("{}/base", base_path);
        let base_batch = create_test_batch(&schema, &[1, 2, 3, 4, 5], "base");
        let base_dataset = Arc::new(create_dataset(&base_uri, vec![base_batch]).await);

        // Create SSTable gen1 as a separate dataset
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let gen1_batch = create_test_batch(&schema, &[3, 4], "gen1");
        create_dataset(&gen1_uri, vec![gen1_batch]).await;

        // Create SSTable gen2 as a separate dataset
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        let gen2_batch = create_test_batch(&schema, &[4, 5, 6], "gen2");
        create_dataset(&gen2_uri, vec![gen2_batch]).await;

        // Build shard snapshot
        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(3)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());

        // Create active memtable
        let (batch_store, index_store) =
            pk_indexed(&[create_test_batch(&schema, &[5, 6, 7], "active")]);

        let active_memtable = InMemoryMemTables {
            active: InMemoryMemTableRef {
                batch_store,
                index_store,
                schema: schema.clone(),
                generation: 3,
            },
            frozen: vec![],
        };

        let pk_columns = vec!["id".to_string()];

        // Keep temp_dir alive by storing path
        let temp_path = temp_dir.keep().to_string_lossy().to_string();

        (
            base_dataset,
            vec![shard_snapshot],
            Some((shard_id, active_memtable)),
            pk_columns,
            temp_path,
        )
    }

    #[tokio::test]
    async fn test_lsm_scan_query_plan_without_memtable_gen() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner without requesting _memtable_gen
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns);
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();

        // Verify the plan (gen DESC order: active -> gen2 -> gen1 -> base):
        // - plain UnionExec at top
        // - active arm: MemTableDedupScanExec (newest gen, not block-listed)
        // - older arms: PkBlockFilterExec (cross-gen block-list) -> LanceRead
        assert_plan_node_equals(
            plan,
            "ProjectionExec:...
  CoalescePartitionsExec
    UnionExec
    MemTableDedupScanExec: projection=[id, name, _rowaddr], with_row_id=false, with_row_address=true
    PkBlockFilterExec: pk_cols=[id]...
      LanceRead:...gen_2...
    PkBlockFilterExec: pk_cols=[id]...
      LanceRead:...gen_1...
    PkBlockFilterExec: pk_cols=[id]...
      LanceRead:...base/data...refine_filter=--",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_lsm_scan_query_plan_with_memtable_gen() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner requesting _memtable_gen
        let mut scanner =
            LsmScanner::new(base_dataset, shard_snapshots, pk_columns).with_memtable_gen();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();

        // Verify the plan with `_memtable_gen` tags (gen DESC order):
        // - plain UnionExec at top
        // - each arm: MemtableGenTagExec -> (PkBlockFilterExec ->) data source
        //   - gen3 (active): MemtableGenTagExec -> MemTableDedupScanExec
        //   - gen2/gen1/base: MemtableGenTagExec -> PkBlockFilterExec -> LanceRead
        assert_plan_node_equals(
            plan,
            "ProjectionExec:...
  CoalescePartitionsExec
    UnionExec
    MemtableGenTagExec: gen=gen3
      MemTableDedupScanExec: projection=[id, name, _rowaddr], with_row_id=false, with_row_address=true
    MemtableGenTagExec: gen=gen2
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...gen_2...
    MemtableGenTagExec: gen=gen1
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...gen_1...
    MemtableGenTagExec: gen=base
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...base/data...refine_filter=--",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_lsm_scan_deduplication_results() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns);
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // Execute and collect results
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Collect all results into a map for easy verification
        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // Verify deduplication kept the newest version of each row
        assert_eq!(results.len(), 7, "Should have 7 unique rows after dedup");

        // id=1: only in base
        assert_eq!(results.get(&1), Some(&"base_1".to_string()));
        // id=2: only in base
        assert_eq!(results.get(&2), Some(&"base_2".to_string()));
        // id=3: updated in gen1
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
        // id=4: updated in gen1, then gen2 -> keep gen2
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        // id=5: updated in gen2, then active -> keep active
        assert_eq!(results.get(&5), Some(&"active_5".to_string()));
        // id=6: added in gen2, updated in active -> keep active
        assert_eq!(results.get(&6), Some(&"active_6".to_string()));
        // id=7: only in active
        assert_eq!(results.get(&7), Some(&"active_7".to_string()));
    }

    /// The filtered-read plan applies the cross-generation block-list (older
    /// generations whose PKs are superseded by a newer one are filtered), while
    /// results stay newest-per-PK.
    #[tokio::test]
    async fn test_lsm_scan_filtered_read_applies_block_list() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns);
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // base/gen1/gen2 all hold PKs superseded by a newer generation, so each
        // is wrapped in a `PkBlockFilterExec`; the newest (active) arm is not.
        let plan = scanner.create_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(
            plan_str.contains("PkBlockFilterExec"),
            "filtered-read plan must apply the cross-gen block-list, got:\n{}",
            plan_str
        );

        // Results stay correct (newest-per-PK across generations).
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }
        assert_eq!(results.len(), 7);
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        assert_eq!(results.get(&5), Some(&"active_5".to_string()));
        assert_eq!(results.get(&6), Some(&"active_6".to_string()));
    }

    /// Regression for the concurrent-read-vs-flush hole: a sealed
    /// (frozen-awaiting-flush) memtable is not yet recorded as an
    /// SSTable, but its rows must still be in the scan's read union and
    /// dedup correctly by generation across the active/frozen seam.
    ///
    /// Layout: base(0) ids 1-5, SSTable gen1 ids 3,4, SSTable gen2 ids
    /// 4,5,6, frozen memtable gen3 ids 6,7, active memtable gen4 ids 7,8.
    #[tokio::test]
    async fn test_lsm_scan_frozen_memtable_in_read_union() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        let base_uri = format!("{}/base", base_path);
        let base_dataset = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&schema, &[1, 2, 3, 4, 5], "base")],
            )
            .await,
        );

        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        create_dataset(&gen1_uri, vec![create_test_batch(&schema, &[3, 4], "gen1")]).await;
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        create_dataset(
            &gen2_uri,
            vec![create_test_batch(&schema, &[4, 5, 6], "gen2")],
        )
        .await;

        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(4)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());

        // Frozen gen3 (sealed, NOT in the manifest) and active gen4.
        let (frozen_store, frozen_index) =
            pk_indexed(&[create_test_batch(&schema, &[6, 7], "frozen")]);
        let frozen = InMemoryMemTableRef {
            batch_store: frozen_store,
            index_store: frozen_index,
            schema: schema.clone(),
            generation: 3,
        };

        let (active_store, active_index) =
            pk_indexed(&[create_test_batch(&schema, &[7, 8], "active")]);
        let in_memory = InMemoryMemTables {
            active: InMemoryMemTableRef {
                batch_store: active_store,
                index_store: active_index,
                schema: schema.clone(),
                generation: 4,
            },
            frozen: vec![frozen],
        };

        let scanner = LsmScanner::new(base_dataset, vec![shard_snapshot], vec!["id".to_string()])
            .with_in_memory_memtables(shard_id, in_memory);

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        assert_eq!(results.len(), 8, "ids 1-8 should all be present");
        assert_eq!(results.get(&1), Some(&"base_1".to_string()));
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        assert_eq!(results.get(&5), Some(&"gen2_5".to_string()));
        // id=6: in SSTable gen2 AND frozen gen3 -> frozen wins. This is the
        // bug: pre-fix the frozen memtable fell out of the read union and
        // id=6 resolved to "gen2_6".
        assert_eq!(results.get(&6), Some(&"frozen_6".to_string()));
        // id=7: in frozen gen3 AND active gen4 -> active wins across the seam.
        assert_eq!(results.get(&7), Some(&"active_7".to_string()));
        assert_eq!(results.get(&8), Some(&"active_8".to_string()));
    }

    #[tokio::test]
    async fn test_lsm_scan_with_projection() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner with projection (only id column)
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .project(&["id"])
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // Execute and collect results
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Verify schema only has "id" column
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");

        // Count total rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 7, "Should have 7 unique rows after dedup");
    }

    #[tokio::test]
    async fn test_lsm_scan_with_limit() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner with limit
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .limit(Some(3), None)
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // Execute and collect results
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Count total rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should have 3 rows due to limit");
    }

    #[tokio::test]
    async fn test_lsm_scan_limit_offset_refills_after_update_shadow_across_fragments() {
        let schema = create_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let base_batch = create_test_batch(&schema, &[1, 2, 3, 4], "base");
        let reader = RecordBatchIterator::new([Ok(base_batch)], schema.clone());
        let base = Arc::new(
            Dataset::write(
                reader,
                &base_uri,
                Some(WriteParams {
                    max_rows_per_file: 2,
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        );
        assert_eq!(
            base.get_fragments().len(),
            2,
            "the stale prefix and live rows must occupy separate fragments"
        );

        // The newest values for ids 1 and 2 no longer match the predicate, so
        // their matching base values are shadowed.  The second base fragment
        // still contains the live matches that must fill offset + limit.
        let (batch_store, index_store) =
            pk_indexed(&[create_test_batch(&schema, &[1, 2], "active")]);
        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema,
                        generation: 1,
                    },
                    frozen: vec![],
                },
            )
            .filter("name LIKE 'base%'")
            .unwrap()
            .limit(Some(1), Some(1))
            .unwrap();

        let batch = scanner.try_into_batch().await.unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![Some(4)],
            "offset=1, limit=1 must skip id=3 after shadow filtering"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_with_offset_without_limit() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .limit(Some(3), None)
            .unwrap()
            .limit(None, Some(2))
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 5,
            "offset-only scan should skip 2 rows from the 7-row deduped result"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_base_only() {
        let (base_dataset, _, _, pk_columns, _temp_path) = setup_multi_level_lsm().await;

        // Create scanner with only base table (no shard snapshots or active memtable)
        let scanner = LsmScanner::new(base_dataset.clone(), vec![], pk_columns.clone());

        let plan = scanner.create_plan().await.unwrap();

        // A single source collapses to just its scan: no union, no block-list
        // (nothing supersedes the base), no dedup.
        assert_plan_node_equals(
            plan,
            "ProjectionExec:...
  LanceRead:...base/data...refine_filter=--",
        )
        .await
        .unwrap();

        // A base-only source has no cross-generation block filter, so its
        // finite limit remains safe to push into the physical Lance read.
        let scanner = LsmScanner::new(base_dataset, vec![], pk_columns)
            .limit(Some(3), None)
            .unwrap();
        let plan = scanner.create_plan().await.unwrap();
        assert_plan_node_equals(
            plan,
            "GlobalLimitExec: skip=0, fetch=3
  ProjectionExec:...
    LocalLimitExec: fetch=3
      LanceRead:...base/data...range_before=Some(0..3)...refine_filter=--",
        )
        .await
        .unwrap();

        // Execute and verify all 5 base rows are returned
        let scanner = LsmScanner::new(
            Arc::new(
                Dataset::open(&format!("{}/base", _temp_path))
                    .await
                    .unwrap(),
            ),
            vec![],
            vec!["id".to_string()],
        );
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "Should have 5 rows from base table");
    }

    #[tokio::test]
    async fn test_lsm_scan_sstable_only_no_active() {
        let (base_dataset, shard_snapshots, _, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner with base + flushed (no active memtable)
        let scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns);

        // Execute and collect results
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Collect all results into a map
        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // Verify results (without active memtable)
        assert_eq!(results.len(), 6, "Should have 6 unique rows (no id=7)");
        assert_eq!(results.get(&1), Some(&"base_1".to_string()));
        assert_eq!(results.get(&2), Some(&"base_2".to_string()));
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        // Without active, gen2 is newest
        assert_eq!(results.get(&5), Some(&"gen2_5".to_string()));
        assert_eq!(results.get(&6), Some(&"gen2_6".to_string()));
        // id=7 doesn't exist without active memtable
        assert_eq!(results.get(&7), None);
    }

    #[tokio::test]
    async fn test_lsm_scan_with_row_address() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner requesting _rowaddr
        let mut scanner =
            LsmScanner::new(base_dataset, shard_snapshots, pk_columns).with_row_address();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan with keep_addr=true (no _memtable_gen, so no MemtableGenTagExec).
        // Non-base arms wrap their scan in a ProjectionExec that NULLs `_rowaddr`:
        // per-source addresses are not meaningful to the caller. The base arm
        // leaves `_rowaddr` real. Older generations are block-list filtered.
        assert_plan_node_equals(
            plan,
            "ProjectionExec:...
  CoalescePartitionsExec
    UnionExec
    ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
      MemTableDedupScanExec: projection=[id, name, _rowaddr], with_row_id=false, with_row_address=true
    ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...gen_2...
    ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...gen_1...
    PkBlockFilterExec: pk_cols=[id]...
      LanceRead:...base/data...refine_filter=--",
        )
        .await
        .unwrap();

        // Execute and verify _rowaddr column is present
        let scanner = LsmScanner::new(
            Arc::new(
                Dataset::open(&format!("{}/base", _temp_path))
                    .await
                    .unwrap(),
            ),
            vec![],
            vec!["id".to_string()],
        )
        .with_row_address();

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Verify schema includes _rowaddr
        let schema = batches[0].schema();
        assert!(
            schema.column_with_name("_rowaddr").is_some(),
            "Schema should include _rowaddr"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_with_both_memtable_gen_and_row_address() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner requesting both _memtable_gen and _rowaddr
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .with_memtable_gen()
            .with_row_address();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan with both with_memtable_gen=true and keep_addr=true.
        // Non-base arms wrap their SortExec in a ProjectionExec that NULLs
        // `_rowaddr`; base leaves it real (so callers can still use it for
        // `take_rows`). MemtableGenTagExec sits above the NULL projection.
        assert_plan_node_equals(
            plan,
            "ProjectionExec:...
  CoalescePartitionsExec
    UnionExec
    MemtableGenTagExec: gen=gen3
      ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
        MemTableDedupScanExec: projection=[id, name, _rowaddr], with_row_id=false, with_row_address=true
    MemtableGenTagExec: gen=gen2
      ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
        PkBlockFilterExec: pk_cols=[id]...
          LanceRead:...gen_2...
    MemtableGenTagExec: gen=gen1
      ProjectionExec: expr=[id@0 as id, name@1 as name, NULL as _rowaddr]
        PkBlockFilterExec: pk_cols=[id]...
          LanceRead:...gen_1...
    MemtableGenTagExec: gen=base
      PkBlockFilterExec: pk_cols=[id]...
        LanceRead:...base/data...refine_filter=--",
        )
        .await
        .unwrap();
    }

    /// Setup LSM with BTree index on the primary key for filter optimization tests.
    ///
    /// Similar to setup_multi_level_lsm but:
    /// - Active memtable has a BTree index on the `id` column
    /// - SSTables have BTree index created (enabling ScalarIndexQuery)
    async fn setup_multi_level_lsm_with_btree_index() -> (
        Arc<Dataset>,
        Vec<ShardSnapshot>,
        Option<(Uuid, InMemoryMemTables)>,
        Vec<String>,
        String,
    ) {
        use crate::index::CreateIndexBuilder;
        use lance_index::IndexType;
        use lance_index::scalar::ScalarIndexParams;

        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Create base table with BTree index
        let base_uri = format!("{}/base", base_path);
        let base_batch = create_test_batch(&schema, &[1, 2, 3, 4, 5], "base");
        let mut base_dataset = create_dataset(&base_uri, vec![base_batch]).await;

        // Create BTree index on base table
        let params = ScalarIndexParams::default();
        CreateIndexBuilder::new(&mut base_dataset, &["id"], IndexType::BTree, &params)
            .await
            .unwrap();

        // Reload dataset to pick up the index
        let base_dataset = Arc::new(Dataset::open(&base_uri).await.unwrap());

        // Create SSTable gen1 with BTree index
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let gen1_batch = create_test_batch(&schema, &[3, 4], "gen1");
        let mut gen1_dataset = create_dataset(&gen1_uri, vec![gen1_batch]).await;
        CreateIndexBuilder::new(&mut gen1_dataset, &["id"], IndexType::BTree, &params)
            .await
            .unwrap();

        // Create SSTable gen2 with BTree index
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        let gen2_batch = create_test_batch(&schema, &[4, 5, 6], "gen2");
        let mut gen2_dataset = create_dataset(&gen2_uri, vec![gen2_batch]).await;
        CreateIndexBuilder::new(&mut gen2_dataset, &["id"], IndexType::BTree, &params)
            .await
            .unwrap();

        // Build shard snapshot
        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(3)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());

        // Create active memtable with BTree index
        let batch_store = Arc::new(BatchStore::with_capacity(100));
        let mut index_store = IndexStore::new();
        // Add BTree index on id column (field_id=0)
        index_store.add_btree("id_idx".to_string(), 0, "id".to_string());
        // Reuse it as the PK index so the block-list can dedup this generation.
        index_store.enable_pk_index(&[("id".to_string(), 0)]);

        let active_batch = create_test_batch(&schema, &[5, 6, 7], "active");
        let _ = batch_store.append(active_batch.clone());

        // Index the batch with row offset 0 and batch position 0
        index_store
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();

        let index_store = Arc::new(index_store);

        let active_memtable = InMemoryMemTables {
            active: InMemoryMemTableRef {
                batch_store,
                index_store,
                schema: schema.clone(),
                generation: 3,
            },
            frozen: vec![],
        };

        let pk_columns = vec!["id".to_string()];
        let temp_path = temp_dir.keep().to_string_lossy().to_string();

        (
            base_dataset,
            vec![shard_snapshot],
            Some((shard_id, active_memtable)),
            pk_columns,
            temp_path,
        )
    }

    #[tokio::test]
    async fn test_lsm_scan_with_btree_index_filter() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm_with_btree_index().await;

        // Use a range filter that is semantically `id = 5` but is NOT a
        // point-lookup shape, so it exercises the union/block-list/dedup/
        // pushdown structure asserted below. (The `id = 5` *equality* shape now
        // routes to the fast point-lookup node — verified separately at the end
        // of this test.)
        let mut scanner = LsmScanner::new(
            base_dataset.clone(),
            shard_snapshots.clone(),
            pk_columns.clone(),
        )
        .filter("id >= 5 AND id <= 5")
        .unwrap();
        if let Some((shard_id, memtable)) = active_memtable.clone() {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();

        // Verify plan structure with BTree index optimization.
        // Instead of complex pattern matching, verify key components directly:
        use datafusion::physical_plan::displayable;
        let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));

        // 1. Verify overall structure
        assert!(plan_str.contains("UnionExec"), "Should have UnionExec");
        assert!(
            plan_str.contains("PkBlockFilterExec"),
            "older generations should be block-list filtered"
        );
        assert!(
            !plan_str.contains("DeduplicateExec"),
            "filtered read must not use a cross-source DeduplicateExec"
        );

        // 2. The active arm uses the fused dedup scan: it deduplicates to
        //    newest-per-PK *before* applying the predicate, so it deliberately
        //    forgoes the in-memory BTree skip (the dedup must see every
        //    version). See MemTableDedupScanExec.
        assert!(
            plan_str.contains("MemTableDedupScanExec"),
            "Active memtable should use the fused dedup scan"
        );
        assert!(
            !plan_str.contains("BTreeIndexExec"),
            "Active filtered read no longer uses the BTree skip"
        );

        // 3. Verify filter pushdown to flushed and base datasets
        assert!(
            plan_str.contains("gen_2") && plan_str.contains("full_filter="),
            "gen_2 should have filter pushed down"
        );
        assert!(
            plan_str.contains("gen_1") && plan_str.contains("full_filter="),
            "gen_1 should have filter pushed down"
        );
        assert!(
            plan_str.contains("base/data") && plan_str.contains("full_filter="),
            "base table should have filter pushed down"
        );

        // Execute and verify result - should return only id=5 (from active, as it's newest)
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // Collect results
        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // Should only have id=5 with the active version (newest wins dedup)
        assert_eq!(results.len(), 1, "Filter should return only matching rows");
        assert_eq!(
            results.get(&5),
            Some(&"active_5".to_string()),
            "Should get newest version (active) for id=5"
        );

        // Equality shape `id = 5` routes to the fast point-lookup node and must
        // return the identical newest (active) row across the LSM levels.
        let mut routed = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .filter("id = 5")
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            routed = routed.with_in_memory_memtables(shard_id, memtable);
        }
        let routed_plan = routed.create_plan().await.unwrap();
        assert!(
            format!("{}", displayable(routed_plan.as_ref()).indent(true)).contains("OneShotStream"),
            "id = 5 must route to the fast point-lookup node"
        );
        let routed_batches: Vec<RecordBatch> = routed
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut routed_results: HashMap<i32, String> = HashMap::new();
        for batch in routed_batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                routed_results.insert(ids.value(i), names.value(i).to_string());
            }
        }
        assert_eq!(routed_results.len(), 1);
        assert_eq!(
            routed_results.get(&5),
            Some(&"active_5".to_string()),
            "routed point lookup must also return newest (active) id=5"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_with_filter_no_index() {
        // Test that filter still works correctly even without BTree index
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Create scanner with SQL filter
        // This tests that type coercion works correctly (Int64 literal -> Int32 column)
        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .filter("id = 3")
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // Execute and verify result
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // id=3 should return gen1 version (base had 3, gen1 updated it)
        assert_eq!(results.len(), 1);
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
    }

    /// End-to-end regression for the active within-generation phantom: a PK
    /// inserted then updated in one memtable so its newest version fails the
    /// predicate must NOT leak the older version that still passes.
    #[tokio::test]
    async fn test_lsm_scan_active_within_gen_phantom_suppressed() {
        let schema = create_pk_schema();
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();

        // Base has an unrelated matching row, to prove real matches survive.
        let base_uri = format!("{}/base", base_path);
        let base_dataset = Arc::new(
            create_dataset(&base_uri, vec![create_test_batch(&schema, &[1], "base")]).await,
        );

        let shard_id = Uuid::new_v4();
        let shard_snapshot = ShardSnapshot::new(shard_id).with_current_generation(1);

        // Active memtable: id=10 inserted ("keep") then updated to NULL within
        // the same generation; id=20 ("active_20") is a control that matches.
        let active_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 10])),
                Arc::new(StringArray::from(vec![
                    Some("keep"),
                    Some("active_20"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let (batch_store, index_store) = pk_indexed(&[active_batch]);

        let in_memory = InMemoryMemTables {
            active: InMemoryMemTableRef {
                batch_store,
                index_store,
                schema: schema.clone(),
                generation: 1,
            },
            frozen: vec![],
        };

        let scanner = LsmScanner::new(base_dataset, vec![shard_snapshot], vec!["id".to_string()])
            .filter("name IS NOT NULL")
            .unwrap()
            .with_in_memory_memtables(shard_id, in_memory);

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut results: HashMap<i32, Option<String>> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let name = (!names.is_null(i)).then(|| names.value(i).to_string());
                results.insert(ids.value(i), name);
            }
        }

        // id=10's newest version is NULL, so it must be absent. Pre-fix the
        // predicate dropped the NULL before dedup and the stale "keep" leaked.
        assert!(
            !results.contains_key(&10),
            "id=10 newest is NULL; stale 'keep' must not leak under name IS NOT NULL, got {:?}",
            results
        );
        assert_eq!(results.get(&1), Some(&Some("base_1".to_string())));
        assert_eq!(results.get(&20), Some(&Some("active_20".to_string())));
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_lsm_scan_without_base_table() {
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        // Use the same base URI the SSTables were created under, so
        // relative `gen_N` folders resolve to real datasets on disk.
        let base_uri = base_dataset.uri().to_string();
        let arrow_schema: arrow_schema::Schema = base_dataset.schema().into();
        let schema = Arc::new(arrow_schema);

        let mut scanner =
            LsmScanner::without_base_table(schema, base_uri, shard_snapshots, pk_columns);
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        // Verify the plan does not include a LanceRead for the base table.
        let plan = scanner.create_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(
            !plan_str.contains("base/data"),
            "Plan must not include base table scan, got: {}",
            plan_str
        );
        assert!(
            plan_str.contains("gen_1") && plan_str.contains("gen_2"),
            "Plan must scan SSTables, got: {}",
            plan_str
        );
        assert!(
            plan_str.contains("MemTableDedupScanExec"),
            "Plan must scan the active memtable, got: {}",
            plan_str
        );

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // Without the base table, ids that only exist in base (1, 2) are gone.
        // The fresh tier (gen1, gen2, active) supplies the rest with newest-wins.
        assert_eq!(results.len(), 5, "Fresh tier should yield 5 unique rows");
        assert_eq!(results.get(&1), None);
        assert_eq!(results.get(&2), None);
        assert_eq!(results.get(&3), Some(&"gen1_3".to_string()));
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        assert_eq!(results.get(&5), Some(&"active_5".to_string()));
        assert_eq!(results.get(&6), Some(&"active_6".to_string()));
        assert_eq!(results.get(&7), Some(&"active_7".to_string()));
    }

    #[tokio::test]
    async fn test_lsm_scan_without_base_table_with_filter() {
        // Exercises filter() on a scanner built via without_base_table(): the
        // filter must parse against the supplied schema (no base dataset to
        // borrow one from) and push down to the fresh-tier sources.
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        let base_uri = base_dataset.uri().to_string();
        let arrow_schema: arrow_schema::Schema = base_dataset.schema().into();
        let schema = Arc::new(arrow_schema);

        let mut scanner =
            LsmScanner::without_base_table(schema, base_uri, shard_snapshots, pk_columns)
                .filter("id > 3")
                .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut results: HashMap<i32, String> = HashMap::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                results.insert(ids.value(i), names.value(i).to_string());
            }
        }

        // id<=3 excluded by filter; id=1,2 also absent because base is excluded.
        // Fresh tier with newest-wins: gen2_4, active_5, active_6, active_7.
        assert_eq!(results.len(), 4);
        assert_eq!(results.get(&4), Some(&"gen2_4".to_string()));
        assert_eq!(results.get(&5), Some(&"active_5".to_string()));
        assert_eq!(results.get(&6), Some(&"active_6".to_string()));
        assert_eq!(results.get(&7), Some(&"active_7".to_string()));
    }

    #[tokio::test]
    async fn test_lsm_scan_without_base_table_no_sstable_no_active() {
        // No base, no SSTable, no active → empty result, valid plan.
        let schema = create_pk_schema();
        let scanner = LsmScanner::without_base_table(
            schema,
            "memory:///fresh-tier-empty",
            vec![],
            vec!["id".to_string()],
        );

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_lsm_scan_projection_with_system_columns() {
        // Regression: system columns in projection used to either error in
        // the active-arm MemTableScanner or get silently dropped. Verify
        // they're now surfaced at the requested position.
        use lance_core::is_system_column;

        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .project(&["id", "_rowoffset", "name", "_rowaddr", "_rowid"])
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .expect("plan must execute when system columns are in projection")
            .try_collect()
            .await
            .expect("collecting batches must not fail");

        assert!(!batches.is_empty(), "expected at least one batch");
        let out_schema = batches[0].schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec!["id", "_rowoffset", "name", "_rowaddr", "_rowid"],
            "system columns must appear at the user's requested position"
        );
        for sys in ["_rowoffset", "_rowaddr", "_rowid"] {
            assert!(is_system_column(sys));
        }

        // setup_multi_level_lsm: base=[1,2,3,4,5], gen1=[3,4], gen2=[4,5,6],
        // active=[5,6,7]. Dedup picks the newest generation per pk:
        //   id=1,2 → base (2 rows, real `_rowaddr`)
        //   id=3   → gen1   (1 row,  NULL  `_rowaddr`)
        //   id=4   → gen2   (1 row,  NULL  `_rowaddr`)
        //   id=5,6 → active (2 rows, NULL  `_rowaddr`)
        //   id=7   → active (1 row,  NULL  `_rowaddr`)
        // Total 7 rows, 2 real / 5 NULL `_rowaddr`. `_rowid` and
        // `_rowoffset` are NULL everywhere (no opt-in / no scanner support).
        let mut rowaddr_real = 0usize;
        let mut rowaddr_null = 0usize;
        let mut rowid_null = 0usize;
        let mut rowoffset_null = 0usize;
        let mut total = 0usize;
        for batch in &batches {
            let rowaddr = batch.column_by_name("_rowaddr").unwrap();
            let rowid = batch.column_by_name("_rowid").unwrap();
            let rowoffset = batch.column_by_name("_rowoffset").unwrap();
            for i in 0..batch.num_rows() {
                total += 1;
                if rowaddr.is_null(i) {
                    rowaddr_null += 1;
                } else {
                    rowaddr_real += 1;
                }
                if rowid.is_null(i) {
                    rowid_null += 1;
                }
                if rowoffset.is_null(i) {
                    rowoffset_null += 1;
                }
            }
        }
        assert_eq!(total, 7, "expected 7 unique pks after dedup");
        assert_eq!(
            rowaddr_real, 2,
            "expected 2 rows (id=1,2) with real `_rowaddr` from base"
        );
        assert_eq!(
            rowaddr_null, 5,
            "expected 5 rows (id=3-7) with NULL `_rowaddr` from non-base sources"
        );
        assert_eq!(rowid_null, total, "_rowid must be NULL for every row");
        assert_eq!(
            rowoffset_null, total,
            "_rowoffset must be NULL for every row"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_projection_with_rowid_only_no_rowaddr() {
        // Test 4: when only `_rowid` (not `_rowaddr`) is requested, the
        // canonical-projection wrap activates (system column triggers it),
        // but the per-arm `null_columns(_rowaddr)` wrap stays off
        // (`keep_row_address` remains false). `_rowid` ends up NULL for
        // every row because no arm opts into `with_row_id()` in this
        // planner (a base-only opt-in would mismatch the union schema).
        let (base_dataset, shard_snapshots, active_memtable, pk_columns, _temp_path) =
            setup_multi_level_lsm().await;

        let mut scanner = LsmScanner::new(base_dataset, shard_snapshots, pk_columns)
            .project(&["id", "_rowid", "name"])
            .unwrap();
        if let Some((shard_id, memtable)) = active_memtable {
            scanner = scanner.with_in_memory_memtables(shard_id, memtable);
        }

        let plan = scanner.create_plan().await.unwrap();
        let plan_str = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        // Canonical wrap activates for the user-requested `_rowid`.
        assert!(
            plan_str.contains("ProjectionExec"),
            "expected canonical projection wrap, got:\n{plan_str}",
        );
        // The per-arm `null_columns(_rowaddr)` wrap is gated on
        // `keep_row_address`, which stays false here. So no
        // `NULL as _rowaddr` projection should appear.
        assert!(
            !plan_str.contains("NULL as _rowaddr"),
            "no per-arm `_rowaddr` NULL'ing expected when caller didn't ask for `_rowaddr`, got:\n{plan_str}",
        );

        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut total = 0usize;
        let mut rowid_null = 0usize;
        for batch in &batches {
            let rowid = batch.column_by_name("_rowid").unwrap();
            for i in 0..batch.num_rows() {
                total += 1;
                if rowid.is_null(i) {
                    rowid_null += 1;
                }
            }
        }
        assert_eq!(total, 7, "expected 7 unique pks after dedup");
        assert_eq!(
            rowid_null, total,
            "_rowid must be NULL for every row (no opt-in)"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_empty_plan_with_system_columns() {
        // Test 5 (planner.rs slice): with no sources, the empty plan must
        // still expose user-requested system columns at the requested
        // position in the canonical schema.
        let temp_dir = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp_dir.path().to_str().unwrap());
        let schema: super::SchemaRef = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let scanner =
            LsmScanner::without_base_table(schema, base_uri, vec![], vec!["id".to_string()])
                .project(&["id", "_rowaddr", "name", "_rowid"])
                .unwrap();
        let plan = scanner.create_plan().await.unwrap();

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
            "empty plan must honor user column order including system columns"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_filter_referencing_rowaddr_is_rejected() {
        // Test 6 (planner.rs slice): documents that `LsmScanner::filter()`
        // rejects system-column references at parse time. The active-arm
        // `MemTableScanner` couldn't handle `_rowaddr` in a filter anyway,
        // and the higher-level builder validates the filter against the
        // user schema before any per-arm planning happens. Pin this
        // behavior so it doesn't silently regress to a panic deeper in the
        // pipeline.
        let (base_dataset, shard_snapshots, pk_columns, _temp_path) = {
            let (b, s, _a, p, t) = setup_multi_level_lsm().await;
            (b, s, p, t)
        };

        let result =
            LsmScanner::new(base_dataset, shard_snapshots, pk_columns).filter("_rowaddr > 0");
        let err = result.expect_err("filter referencing `_rowaddr` must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("_rowaddr"),
            "rejection message should mention the offending column, got: {msg}",
        );
    }

    // ----- tombstone (delete) filtered-read tests -----

    /// Memtable / generation schema: base (`id`, `name`) + `_tombstone`.
    fn ts_pk_schema() -> Arc<ArrowSchema> {
        let mut id_metadata = HashMap::new();
        id_metadata.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id = Field::new("id", DataType::Int32, false).with_metadata(id_metadata);
        Arc::new(ArrowSchema::new(vec![
            id,
            Field::new("name", DataType::Utf8, true),
            Field::new(crate::dataset::mem_wal::TOMBSTONE, DataType::Boolean, false),
        ]))
    }

    /// Build a `_tombstone`-bearing batch from `(id, name, tombstone)` rows.
    fn ts_batch(schema: &Arc<ArrowSchema>, rows: &[(i32, Option<&str>, bool)]) -> RecordBatch {
        let ids: Vec<i32> = rows.iter().map(|(i, _, _)| *i).collect();
        let names: Vec<Option<String>> = rows
            .iter()
            .map(|(_, n, _)| n.map(|s| s.to_string()))
            .collect();
        let ts: Vec<bool> = rows.iter().map(|(_, _, t)| *t).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(arrow_array::BooleanArray::from(ts)),
            ],
        )
        .unwrap()
    }

    fn collect_sorted_ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids: Vec<i32> = Vec::new();
        for b in batches {
            let arr = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ids.extend((0..arr.len()).map(|i| arr.value(i)));
        }
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn test_lsm_scan_active_tombstone_masks_base() {
        // Active gen tombstones id=2 (present in base) and adds real id=4. The
        // result drops id=2 (block-list masks the base row; the active arm's
        // folded `NOT _tombstone` drops the tombstone itself) and never
        // surfaces the tombstone row.
        let base_schema = create_pk_schema();
        let mem_schema = ts_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let base = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&base_schema, &[1, 2, 3], "base")],
            )
            .await,
        );

        let active_batch = ts_batch(
            &mem_schema,
            &[(4, Some("active_4"), false), (2, None, true)],
        );
        let (bs, ix) = pk_indexed(&[active_batch]);
        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store: bs,
                        index_store: ix,
                        schema: mem_schema,
                        generation: 2,
                    },
                    frozen: vec![],
                },
            );
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            collect_sorted_ids(&batches),
            vec![1, 3, 4],
            "id=2 deleted; tombstone row not surfaced"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_limit_refills_after_active_tombstone_shadows_base() {
        let base_schema = create_pk_schema();
        let mem_schema = ts_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());
        let base = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&base_schema, &[1, 2, 3], "base")],
            )
            .await,
        );

        let active_batch = ts_batch(&mem_schema, &[(1, None, true)]);
        let (batch_store, index_store) = pk_indexed(&[active_batch]);
        let scanner = LsmScanner::new(base, vec![], vec!["id".to_string()])
            .with_in_memory_memtables(
                Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store,
                        schema: mem_schema,
                        generation: 1,
                    },
                    frozen: vec![],
                },
            )
            .limit(Some(2), None)
            .unwrap();

        let batch = scanner.try_into_batch().await.unwrap();
        assert_eq!(
            collect_sorted_ids(&[batch]),
            vec![2, 3],
            "the base scan must continue past the shadowed row to fill the limit"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_sstable_tombstone_masks_base() {
        // A tombstone living in an SSTable masks the older base row by
        // PK presence (block-list) and is itself dropped by the folded predicate.
        let base_schema = create_pk_schema();
        let mem_schema = ts_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_path = temp.path().to_str().unwrap();
        let base_uri = format!("{}/base", base_path);
        let base = Arc::new(
            create_dataset(
                &base_uri,
                vec![create_test_batch(&base_schema, &[1, 2, 3], "base")],
            )
            .await,
        );

        // SSTable gen 1 holds only a tombstone for id=2 (written with the
        // `_tombstone` schema, so the SSTable arm folds `NOT _tombstone`).
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        create_dataset(&gen1_uri, vec![ts_batch(&mem_schema, &[(2, None, true)])]).await;
        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        let scanner = LsmScanner::new(base, vec![shard_snapshot], vec!["id".to_string()]);
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            collect_sorted_ids(&batches),
            vec![1, 3],
            "id=2 deleted via an SSTable tombstone"
        );
    }

    #[tokio::test]
    async fn test_lsm_scan_tombstone_does_not_consume_limit() {
        // A single (newest) SSTable holds both tombstones and live
        // rows. With LIMIT 2 the folded `NOT _tombstone` runs *before* the
        // per-source pushdown limit, so the limit counts only live rows — we get
        // 2 live rows, not 0 (which is what a post-limit tombstone filter, or a
        // limit that counted the tombstones, would yield).
        let base_schema = create_pk_schema();
        let mem_schema = ts_pk_schema();
        let temp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", temp.path().to_str().unwrap());

        // gen_1 file order: two tombstones first, then two live rows.
        let shard_id = Uuid::new_v4();
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        create_dataset(
            &gen1_uri,
            vec![ts_batch(
                &mem_schema,
                &[
                    (1, None, true),
                    (2, None, true),
                    (3, Some("live_3"), false),
                    (4, Some("live_4"), false),
                ],
            )],
        )
        .await;
        let shard_snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(2)
            .with_sstable(1, "gen_1".to_string());

        let scanner = LsmScanner::without_base_table(
            base_schema,
            base_uri,
            vec![shard_snapshot],
            vec!["id".to_string()],
        )
        .limit(Some(2), None)
        .unwrap();
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = collect_sorted_ids(&batches);
        assert_eq!(
            ids.len(),
            2,
            "limit honored with live rows only, got {:?}",
            ids
        );
        assert!(
            !ids.contains(&1) && !ids.contains(&2),
            "deleted ids must not appear, got {:?}",
            ids
        );
    }
}
