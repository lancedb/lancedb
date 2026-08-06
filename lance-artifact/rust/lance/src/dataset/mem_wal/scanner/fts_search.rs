// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Full-text search planner for LSM scanner (local scoring).
//!
//! Builds an execution plan that scores an FTS query across the base
//! table, SSTable generations, and the active/frozen-undrained
//! in-memory memtables, returning rows ordered by BM25 `_score` DESC.
//!
//! # Scoring
//!
//! Each source scores with its own corpus statistics (local BM25), and
//! the coordinator unions the per-source plans and merges by `_score`
//! (per-partition top-k sort + sort-preserving merge). The plan is
//! single-pass and never coordinates statistics across sources, so
//! cross-source `_score` values are only approximately comparable — but
//! within each source the ranking is exact. This mirrors the default
//! `query_then_fetch` trade-off of distributed search systems.
//!
//! A globally-consistent scoring mode (aggregate corpus statistics
//! across sources, then rescore) is a deliberate follow-up: the
//! benchmark in this PR shows it carries a real latency penalty, so the
//! local path lands first and the global option is optimized separately.
//!
//! Staleness: within an SSTable, the deletion vector written
//! at flush time (see #6929) already masks rows superseded by a newer
//! generation, so per-source results are clean within each tier. The
//! same primary key can still appear across tiers (active vs SSTable)
//! when an updated row sits in the active memtable while the older
//! copy lives in an SSTable; cross-tier deduplication is
//! left to the caller in local mode.
//!
//! Everything here is contained in the `mem_wal` module — it reuses the
//! existing per-source FTS read paths (`scanner.full_text_search` for
//! base/SSTable Lance datasets, `MemTableScanner` for the active
//! memtable) and requires no changes to `lance-index`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::Expr;
use lance_core::{Error, Result, is_system_column};
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{FtsQuery as IndexFtsQuery, Operator};
use lance_index::scalar::inverted::{DOC_INDEX_COL, DOC_INDEX_FIELD, DocumentGranularity};
use tracing::instrument;

use super::block_list::compute_source_block_lists;
use super::collector::LsmDataSourceCollector;
use super::data_source::LsmDataSource;
use super::exec::PkBlockFilterExec;
use super::projection::{project_to_canonical, validate_projection_names};
use super::sstable_cache::{DatasetCache, SsTableWarmer, open_sstable};
use crate::dataset::mem_wal::memtable::scanner::MemTableScanner;
use crate::index::scalar::inverted::{indexed_fts_document_granularities, resolve_fts_field};
use crate::session::Session;
use lance_io::object_store::ObjectStoreParams;

/// `_score` column name in FTS results — kept aligned with
/// `lance_index::scalar::inverted::SCORE_COL` so this module doesn't
/// require an import for one string constant.
pub const SCORE_COLUMN: &str = "_score";

/// Default over-fetch multiple for blocked sources. `1.0` keeps cross-generation
/// dedup on with no over-fetch; callers (e.g. the sophon WAL handler) raise it
/// so a blocked source still yields `k` live rows after the block-list filter.
const DEFAULT_OVERFETCH_FACTOR: f64 = 1.0;

fn requested_query_document_granularity(
    query: &IndexFtsQuery,
) -> Result<Option<DocumentGranularity>> {
    fn merge(
        current: &mut Option<DocumentGranularity>,
        requested: Option<DocumentGranularity>,
    ) -> Result<()> {
        let Some(requested) = requested else {
            return Ok(());
        };
        if let Some(current) = current
            && *current != requested
        {
            return Err(Error::invalid_input(
                "FTS queries cannot mix Row and ListElement document granularities".to_string(),
            ));
        }
        *current = Some(requested);
        Ok(())
    }

    fn visit(query: &IndexFtsQuery, current: &mut Option<DocumentGranularity>) -> Result<()> {
        match query {
            IndexFtsQuery::Match(query) => merge(current, query.document_granularity),
            IndexFtsQuery::Phrase(query) => merge(current, query.document_granularity),
            IndexFtsQuery::Boost(query) => {
                visit(&query.positive, current)?;
                visit(&query.negative, current)
            }
            IndexFtsQuery::Boolean(query) => {
                for child in query
                    .must
                    .iter()
                    .chain(&query.should)
                    .chain(&query.must_not)
                {
                    visit(child, current)?;
                }
                Ok(())
            }
            IndexFtsQuery::MultiMatch(query) => {
                for child in &query.match_queries {
                    merge(current, child.document_granularity)?;
                }
                Ok(())
            }
        }
    }

    let mut requested = None;
    visit(query, &mut requested)?;
    Ok(requested)
}

fn set_query_document_granularity(
    query: &mut IndexFtsQuery,
    document_granularity: DocumentGranularity,
) {
    match query {
        IndexFtsQuery::Match(query) => {
            query.document_granularity = Some(document_granularity);
        }
        IndexFtsQuery::Phrase(query) => {
            query.document_granularity = Some(document_granularity);
        }
        IndexFtsQuery::Boost(query) => {
            set_query_document_granularity(&mut query.positive, document_granularity);
            set_query_document_granularity(&mut query.negative, document_granularity);
        }
        IndexFtsQuery::Boolean(query) => {
            for child in query
                .must
                .iter_mut()
                .chain(&mut query.should)
                .chain(&mut query.must_not)
            {
                set_query_document_granularity(child, document_granularity);
            }
        }
        IndexFtsQuery::MultiMatch(query) => {
            for child in &mut query.match_queries {
                child.document_granularity = Some(document_granularity);
            }
        }
    }
}

fn query_document_granularity(query: &FullTextSearchQuery) -> Result<DocumentGranularity> {
    requested_query_document_granularity(&query.query)?.ok_or_else(|| {
        Error::internal("LSM FTS query document granularity was not resolved".to_string())
    })
}

fn resolve_document_granularity_from_candidates(
    column: &str,
    requested: Option<DocumentGranularity>,
    mut available: Vec<DocumentGranularity>,
) -> Result<DocumentGranularity> {
    available.sort_by_key(|document_granularity| match document_granularity {
        DocumentGranularity::Row => 0,
        DocumentGranularity::ListElement => 1,
    });
    available.dedup();
    match requested {
        Some(requested) if available.is_empty() || available.contains(&requested) => Ok(requested),
        Some(requested) => Err(Error::invalid_input(format!(
            "FTS query for field '{column}' requested {requested:?} document granularity, but \
             the MemWAL sources use a different indexed granularity: {available:?}"
        ))),
        None if available.is_empty() => Ok(DocumentGranularity::Row),
        None if available.len() == 1 => Ok(available[0]),
        None => Err(Error::invalid_input(format!(
            "FTS query for field '{column}' is ambiguous because Row and ListElement indexes \
             coexist across MemWAL sources; specify document_granularity"
        ))),
    }
}

fn validate_source_document_granularities(
    column: &str,
    resolved: DocumentGranularity,
    source_granularities: &[Vec<DocumentGranularity>],
) -> Result<()> {
    for granularities in source_granularities {
        if !granularities.is_empty() && !granularities.contains(&resolved) {
            return Err(Error::invalid_input(format!(
                "FTS query for field '{column}' resolved to {resolved:?}, but a MemWAL source has \
                 only incompatible indexed granularities: {granularities:?}"
            )));
        }
    }
    Ok(())
}

fn validate_lsm_fts_query(query: &FullTextSearchQuery) -> Result<()> {
    match &query.query {
        IndexFtsQuery::Match(m) => {
            if m.fuzziness != Some(0) && m.operator != Operator::Or {
                return Err(Error::not_supported(
                    "LSM fuzzy full-text search only supports OR match operators".to_string(),
                ));
            }
            Ok(())
        }
        IndexFtsQuery::Phrase(_) => Ok(()),
        _ => Err(Error::not_supported(
            "LSM full-text search only supports match and phrase leaf queries".to_string(),
        )),
    }
}

fn active_source_can_execute_fts(
    source: &LsmDataSource,
    column: &str,
    document_granularity: DocumentGranularity,
) -> bool {
    match source {
        LsmDataSource::ActiveMemTable {
            batch_store,
            index_store,
            ..
        } => {
            index_store
                .get_fts_by_column_and_granularity(column, document_granularity)
                .is_some_and(|index| !index.is_empty())
                && batch_store
                    .max_visible_row(index_store.visible_count())
                    .is_some()
        }
        _ => false,
    }
}

/// Plans local-scoring FTS queries over LSM data.
pub struct LsmFtsSearchPlanner {
    collector: LsmDataSourceCollector,
    pk_columns: Vec<String>,
    base_schema: SchemaRef,
    /// Session threaded into SSTable opens (shared caches).
    session: Option<Arc<Session>>,
    /// Store params for opening SSTables, reusing the base dataset's store.
    store_params: Option<ObjectStoreParams>,
    /// Cache of opened SSTable datasets.
    sstable_cache: Option<Arc<dyn DatasetCache>>,
    /// Optional warmer fired on first open of an SSTable.
    warmer: Option<Arc<dyn SsTableWarmer>>,
    /// Over-fetch multiple for blocked sources.
    overfetch_factor: f64,
    /// Optional prefilter predicate applied to every source arm so FTS hits
    /// failing the predicate are dropped. Base/SSTable arms use the dataset
    /// scanner's native filter; memtable arms filter the materialized hits.
    filter: Option<Expr>,
}

impl LsmFtsSearchPlanner {
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
            overfetch_factor: DEFAULT_OVERFETCH_FACTOR,
            filter: None,
        }
    }

    /// Attach an optional prefilter predicate. Every source arm restricts its
    /// FTS hits to rows matching the predicate, matching a normal filtered
    /// full-text scan over base ∪ SSTables ∪ in-memory data.
    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    /// Set the over-fetch multiple for blocked sources so they still yield `k`
    /// live rows after cross-generation block-list filtering. Values below
    /// `1.0` are rejected by [`Self::plan_search`].
    pub fn with_overfetch_factor(mut self, factor: f64) -> Self {
        self.overfetch_factor = factor;
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

    /// Build the FTS execution plan (local scoring).
    ///
    /// # Arguments
    ///
    /// * `column` — text column to search.
    /// * `query` — the FTS query (match / phrase / boolean / fuzzy for
    ///   base/SSTable Lance sources; the active memtable currently
    ///   supports `MatchQuery`).
    /// * `limit` — optional global top-k to return.
    /// * `projection` — user columns to project. PK columns are
    ///   auto-included; `_score` is always appended.
    ///
    /// Each source is scored independently (local BM25), normalized to a
    /// canonical schema, unioned, and merged by `_score` DESC. When a finite
    /// limit is supplied, top-k caps are pushed into each partition.
    #[instrument(
        name = "lsm_fts_search",
        level = "info",
        skip_all,
        fields(column = %column, limit)
    )]
    pub async fn plan_search(
        &self,
        column: &str,
        mut query: FullTextSearchQuery,
        limit: Option<usize>,
        projection: Option<&[String]>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sources = self.collector.collect()?;
        let requested = requested_query_document_granularity(&query.query)?;
        let mut available = Vec::new();
        let mut source_granularities = Vec::with_capacity(sources.len());
        for source in &sources {
            let granularities = match source {
                LsmDataSource::BaseTable { dataset } => {
                    indexed_fts_document_granularities(dataset, column)
                        .await?
                        .into_iter()
                        .map(|(_, document_granularity)| document_granularity)
                        .collect::<Vec<_>>()
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
                    indexed_fts_document_granularities(&dataset, column)
                        .await?
                        .into_iter()
                        .map(|(_, document_granularity)| document_granularity)
                        .collect::<Vec<_>>()
                }
                LsmDataSource::ActiveMemTable { index_store, .. } => {
                    index_store.fts_document_granularities_by_column(column)
                }
            };
            available.extend(granularities.iter().copied());
            source_granularities.push(granularities);
        }
        let document_granularity =
            resolve_document_granularity_from_candidates(column, requested, available)?;
        validate_source_document_granularities(
            column,
            document_granularity,
            &source_granularities,
        )?;
        let schema = lance_core::datatypes::Schema::try_from(self.base_schema.as_ref())?;
        resolve_fts_field(&schema, column, document_granularity)?;
        set_query_document_granularity(&mut query.query, document_granularity);
        if sources
            .iter()
            .any(|source| active_source_can_execute_fts(source, column, document_granularity))
        {
            validate_lsm_fts_query(&query)?;
        }
        let allowed_system_columns: &[&str] = if document_granularity.is_list_element() {
            &[SCORE_COLUMN, DOC_INDEX_COL]
        } else {
            &[SCORE_COLUMN]
        };
        validate_projection_names(projection, &self.base_schema, allowed_system_columns)?;
        let target_schema = self.canonical_fts_schema(projection, document_granularity);
        let overfetch = super::validate_overfetch_factor(self.overfetch_factor)?;

        if sources.is_empty() {
            return self.empty_plan(&target_schema);
        }

        // Per-source PK block sets for cross-generation dedup (NEWER(G) per
        // shard; base = union of all gens). Query-type-agnostic — same call the
        // vector planner makes. `Box::pin` keeps the future off
        // `clippy::large_futures`.
        let block_lists = Box::pin(compute_source_block_lists(
            &sources,
            self.session.as_ref(),
            self.store_params.as_ref(),
            self.sstable_cache.as_ref(),
        ))
        .await?;

        // Stage the per-source over-fetch decisions, then build every source
        // plan concurrently — the builds are independent and a sequential loop
        // was the dominant serial planning cost at multiple generations.
        let arm_inputs: Vec<_> = sources
            .iter()
            .map(|source| {
                let is_active = matches!(source, LsmDataSource::ActiveMemTable { .. });
                let blocked = block_lists.get(&(source.shard_id(), source.generation()));
                let active_needs_uncapped_recency = match source {
                    LsmDataSource::ActiveMemTable { index_store, .. }
                        if !self.pk_columns.is_empty() =>
                    {
                        !index_store.has_pk_index() || index_store.pk_has_overrides()
                    }
                    _ => false,
                };
                // Active PK arms only need to stay uncapped when recency
                // filtering may drop hits. Append-only PK memtables can safely
                // pass the limit through and let FtsIndexExec use WAND/top-k.
                // Blocked non-active sources use heuristic over-fetch because
                // their newer generation membership may also drop candidates.
                let fetch_limit = if active_needs_uncapped_recency {
                    None
                } else if blocked.is_some() && !self.pk_columns.is_empty() {
                    limit.map(|limit| ((limit as f64) * overfetch).ceil() as usize)
                } else {
                    limit
                };
                (source, is_active, blocked, fetch_limit)
            })
            .collect();
        let built =
            futures::future::try_join_all(arm_inputs.iter().map(|(source, _, _, fetch_limit)| {
                Box::pin(self.build_source_plan(source, column, &query, *fetch_limit, projection))
            }))
            .await?;

        let mut per_source_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(sources.len());
        for ((_, _, blocked, _), plan) in arm_inputs.iter().zip(built) {
            let blocked = *blocked;
            // Dedup, mirroring LsmVectorSearchPlanner:
            //  * each memtable: `FtsIndexExec` drops superseded PK versions
            //    within that memtable before the query limit whenever PK
            //    columns are present.
            //  * any source with a block-list: drop rows superseded by a newer
            //    generation, including frozen in-memory memtables.
            let deduped = if let Some(set) = blocked
                && !self.pk_columns.is_empty()
            {
                Arc::new(PkBlockFilterExec::new(
                    plan,
                    self.pk_columns.clone(),
                    set.clone(),
                    limit.unwrap_or(usize::MAX),
                )) as Arc<dyn ExecutionPlan>
            } else {
                plan
            };

            // Normalize to the canonical FTS schema before merging sources.
            let normalized = project_to_canonical(deduped, &target_schema)?;
            per_source_plans.push(normalized);
        }

        // Single source: skip Union and the merge.
        let merged: Arc<dyn ExecutionPlan> = if per_source_plans.len() == 1 {
            per_source_plans.into_iter().next().unwrap()
        } else {
            #[allow(deprecated)]
            // The downstream `SortPreservingMergeExec` already spawns one driver
            // task per input partition (one per union arm) via `spawn_buffered`,
            // so each arm's per-arm CPU (posting decode, BM25) runs on its own
            // task without an extra repartition.
            Arc::new(UnionExec::new(per_source_plans))
        };

        let score_idx = merged.schema().index_of(SCORE_COLUMN).map_err(|_| {
            Error::internal(format!(
                "{SCORE_COLUMN} missing from canonical FTS schema after merge"
            ))
        })?;

        let sort_expr = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(SCORE_COLUMN, score_idx)),
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];
        let lex_ordering = LexOrdering::new(sort_expr).ok_or_else(|| {
            Error::internal("Failed to build LexOrdering for FTS _score sort".to_string())
        })?;

        // Per-partition sort with `fetch=k` so each upstream partition
        // can early-terminate at k; the preserving merge then does a
        // K-way heap merge also capped at k. Same pattern as
        // LsmVectorSearchPlanner.
        let per_partition_sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(lex_ordering.clone(), merged)
                .with_preserve_partitioning(true)
                .with_fetch(limit),
        );
        let merged_sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(lex_ordering, per_partition_sorted).with_fetch(limit),
        );

        Ok(merged_sorted)
    }

    async fn build_source_plan(
        &self,
        source: &LsmDataSource,
        column: &str,
        query: &FullTextSearchQuery,
        limit: Option<usize>,
        projection: Option<&[String]>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match source {
            LsmDataSource::BaseTable { dataset } => {
                let mut scanner = dataset.scan();
                let cols = self.fts_scanner_projection(projection);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // `prefilter(true)` is required: without it the scanner
                    // post-filters the unfiltered BM25 top-k, dropping matching
                    // rows that scored below non-matching ones.
                    scanner.filter_expr(filter.clone());
                    scanner.prefilter(true);
                }
                let mut bound_query = query.clone().with_column(column.to_string())?;
                if let Some(limit) = limit {
                    bound_query = bound_query.limit(Some(limit as i64));
                } else {
                    bound_query = bound_query.limit(None);
                }
                scanner.full_text_search(bound_query)?;
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
                let cols = self.fts_scanner_projection(projection);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // See the base arm: `prefilter(true)` makes this a true
                    // prefilter rather than a lossy post-filter on the BM25 top-k.
                    scanner.filter_expr(filter.clone());
                    scanner.prefilter(true);
                }
                let mut bound_query = query.clone().with_column(column.to_string())?;
                if let Some(limit) = limit {
                    bound_query = bound_query.limit(Some(limit as i64));
                } else {
                    bound_query = bound_query.limit(None);
                }
                scanner.full_text_search(bound_query)?;
                scanner.create_plan().await
            }
            LsmDataSource::ActiveMemTable {
                batch_store,
                index_store,
                schema,
                ..
            } => {
                let document_granularity = query_document_granularity(query)?;
                if !active_source_can_execute_fts(source, column, document_granularity) {
                    return self
                        .empty_plan(&self.canonical_fts_schema(projection, document_granularity));
                }
                validate_lsm_fts_query(query)?;
                let mut scanner =
                    MemTableScanner::new(batch_store.clone(), index_store.clone(), schema.clone());
                let cols = self.fts_scanner_projection(projection);
                scanner.project(&cols.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
                if let Some(ref filter) = self.filter {
                    // Honored inside `plan_fts_search`: the materialized hits are
                    // masked by the predicate before projection.
                    scanner.filter_expr(filter.clone());
                }
                // The append-only inverted index keeps an updated row's old
                // postings live, so the memtable FTS exec needs PK columns to
                // drop stale hits before it applies the query limit.
                if !self.pk_columns.is_empty() {
                    scanner.with_pk_columns(self.pk_columns.clone());
                }
                // `MemTableScanner::full_text_search` now takes a structured
                // `FullTextSearchQuery` (match/phrase); it rejects compound
                // shapes the MemTable path can't model.
                let mut bound_query = query.clone().with_column(column.to_string())?;
                if let Some(limit) = limit {
                    bound_query = bound_query.limit(Some(limit as i64));
                } else {
                    bound_query = bound_query.limit(None);
                }
                scanner.full_text_search(bound_query)?;
                scanner.create_plan().await
            }
        }
    }

    /// Columns to pass to the underlying scanner: user projection minus
    /// system / `_score`, with PK columns appended.
    fn fts_scanner_projection(&self, user_projection: Option<&[String]>) -> Vec<String> {
        let mut cols: Vec<String> = if let Some(p) = user_projection {
            p.iter()
                .filter(|c| !is_system_column(c) && c.as_str() != SCORE_COLUMN)
                .cloned()
                .collect()
        } else {
            self.base_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };
        for pk in &self.pk_columns {
            if !cols.contains(pk) {
                cols.push(pk.clone());
            }
        }
        cols
    }

    /// Canonical FTS output: user-projected cols + PK + `_score`.
    fn canonical_fts_schema(
        &self,
        user_projection: Option<&[String]>,
        document_granularity: DocumentGranularity,
    ) -> SchemaRef {
        let mut ordered: Vec<String> = if let Some(p) = user_projection {
            p.to_vec()
        } else {
            self.base_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };
        for pk in &self.pk_columns {
            if !ordered.contains(pk) {
                ordered.push(pk.clone());
            }
        }
        if document_granularity.is_list_element() && !ordered.iter().any(|c| c == DOC_INDEX_COL) {
            ordered.push(DOC_INDEX_COL.to_string());
        }
        if !ordered.iter().any(|c| c == SCORE_COLUMN) {
            ordered.push(SCORE_COLUMN.to_string());
        }
        let fields: Vec<Arc<Field>> = ordered
            .iter()
            .filter_map(|name| {
                if name == SCORE_COLUMN {
                    Some(Arc::new(Field::new(SCORE_COLUMN, DataType::Float32, true)))
                } else if name == DOC_INDEX_COL {
                    Some(Arc::new(DOC_INDEX_FIELD.clone()))
                } else if is_system_column(name) {
                    Some(Arc::new(Field::new(name.clone(), DataType::UInt64, true)))
                } else {
                    self.base_schema
                        .field_with_name(name)
                        .ok()
                        .map(|f| Arc::new(f.clone()))
                }
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    fn empty_plan(&self, schema: &SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_plan::empty::EmptyExec;
        Ok(Arc::new(EmptyExec::new(schema.clone())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
    use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
    use crate::dataset::{Dataset, WriteParams};
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{
        Array, BooleanArray, Int32Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
        UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use lance_index::scalar::inverted::query::MatchQuery;
    use std::collections::HashMap;

    fn fts_schema() -> Arc<ArrowSchema> {
        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_meta);
        Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new("text", DataType::Utf8, true),
        ]))
    }

    fn fts_tombstone_schema() -> Arc<ArrowSchema> {
        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_meta);
        Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new("text", DataType::Utf8, true),
            Field::new(crate::dataset::mem_wal::TOMBSTONE, DataType::Boolean, false),
        ]))
    }

    fn make_batch(schema: &ArrowSchema, ids: &[i32], texts: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(texts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_tombstone_batch(
        schema: &ArrowSchema,
        rows: &[(i32, Option<&str>, bool)],
    ) -> RecordBatch {
        let ids: Vec<i32> = rows.iter().map(|(id, _, _)| *id).collect();
        let texts: Vec<Option<&str>> = rows.iter().map(|(_, text, _)| *text).collect();
        let tombstones: Vec<bool> = rows.iter().map(|(_, _, tombstone)| *tombstone).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(texts)),
                Arc::new(BooleanArray::from(tombstones)),
            ],
        )
        .unwrap()
    }

    async fn write_dataset(uri: &str, batches: Vec<RecordBatch>) -> Dataset {
        let schema = batches[0].schema();
        let has_id = schema.column_with_name("id").is_some();
        let reader = RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema);
        let dataset = Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap();
        if has_id {
            crate::dataset::mem_wal::scanner::block_list::write_pk_sidecar(uri, &batches, &["id"])
                .await
                .unwrap();
        }
        dataset
    }

    #[tokio::test]
    async fn rejects_missing_projection_column() {
        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);

        let projection = vec!["missing".to_string()];
        let err = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                Some(&projection),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("missing"),
            "unexpected missing-column projection error: {err}"
        );
    }

    #[test]
    fn resolves_document_granularity_from_memwal_indexes() {
        assert_eq!(
            resolve_document_granularity_from_candidates("tags", None, vec![]).unwrap(),
            DocumentGranularity::Row
        );
        assert_eq!(
            resolve_document_granularity_from_candidates(
                "tags",
                None,
                vec![DocumentGranularity::ListElement],
            )
            .unwrap(),
            DocumentGranularity::ListElement
        );
        assert!(
            resolve_document_granularity_from_candidates(
                "tags",
                Some(DocumentGranularity::Row),
                vec![DocumentGranularity::ListElement],
            )
            .unwrap_err()
            .to_string()
            .contains("different indexed granularity")
        );
        let both = vec![DocumentGranularity::Row, DocumentGranularity::ListElement];
        assert!(
            resolve_document_granularity_from_candidates("tags", None, both.clone())
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        assert_eq!(
            resolve_document_granularity_from_candidates(
                "tags",
                Some(DocumentGranularity::ListElement),
                both,
            )
            .unwrap(),
            DocumentGranularity::ListElement
        );
        assert!(
            validate_source_document_granularities(
                "tags",
                DocumentGranularity::ListElement,
                &[
                    vec![DocumentGranularity::ListElement],
                    vec![DocumentGranularity::Row],
                ],
            )
            .unwrap_err()
            .to_string()
            .contains("incompatible indexed granularities")
        );
    }

    #[tokio::test]
    async fn memwal_rejects_list_element_without_a_list_path() {
        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![]);
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let query = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("lance".to_string())
                .with_document_granularity(DocumentGranularity::ListElement),
        ));

        let error = planner
            .plan_search("text", query, Some(1), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("has no List layer"), "{error}");
    }

    #[tokio::test]
    async fn active_element_document_search_returns_physical_ordinals() {
        use lance_index::scalar::inverted::InvertedIndexParams;

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_meta);

        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("alpha");
        tags.values().append_value("beta gamma");
        tags.values().append_null();
        tags.values().append_value("beta");
        tags.append(true);
        tags.append(true);
        tags.values().append_value("beta");
        tags.append(true);
        let tags = tags.finish();
        let schema = Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        let active_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(tags)],
        )
        .unwrap();

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes
            .add_fts_with_params(
                "tags_list_element_fts".to_string(),
                1,
                "tags".to_string(),
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            )
            .unwrap();
        let (_, row_offset, batch_position) = batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, row_offset, Some(batch_position))
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: Arc::new(indexes),
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema.clone());
        let projection = vec!["id".to_string()];
        let plan = planner
            .plan_search(
                "tags",
                FullTextSearchQuery::new_query(IndexFtsQuery::Match(MatchQuery::new(
                    "beta".to_string(),
                ))),
                Some(10),
                Some(&projection),
            )
            .await
            .unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut hits = Vec::new();
        for batch in batches {
            let ids = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            let doc_indices = batch[DOC_INDEX_COL]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let coordinate = doc_indices
                    .value(row)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(0);
                hits.push((ids.value(row), coordinate));
            }
        }
        hits.sort_unstable();
        assert_eq!(hits, vec![(1, 1), (1, 3), (3, 0)]);
    }

    #[tokio::test]
    async fn element_document_search_unions_base_and_active_sources() {
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::inverted::InvertedIndexParams;

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(id_meta);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let schema = Arc::new(ArrowSchema::new(vec![
            id_field,
            Field::new("tags", list_type, true),
        ]));

        let mut base_tags = ListBuilder::new(StringBuilder::new());
        base_tags.values().append_value("beta");
        base_tags.values().append_value("other");
        base_tags.append(true);
        let base_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(base_tags.finish()),
            ],
        )
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(base_batch)], schema.clone()),
            &base_uri,
            None,
        )
        .await
        .unwrap();
        base_ds
            .create_index(
                &["tags"],
                IndexType::Inverted,
                Some("tags_list_element_fts".to_string()),
                &InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        let mut active_tags = ListBuilder::new(StringBuilder::new());
        active_tags.values().append_value("other");
        active_tags.values().append_value("beta");
        active_tags.append(true);
        let active_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(active_tags.finish()),
            ],
        )
        .unwrap();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes
            .add_fts_with_params(
                "tags_list_element_fts".to_string(),
                1,
                "tags".to_string(),
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            )
            .unwrap();
        let (_, row_offset, batch_position) = batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, row_offset, Some(batch_position))
            .unwrap();

        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: Arc::new(indexes),
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let projection = vec!["id".to_string()];
        let plan = planner
            .plan_search(
                "tags",
                FullTextSearchQuery::new_query(IndexFtsQuery::Match(MatchQuery::new(
                    "beta".to_string(),
                ))),
                Some(10),
                Some(&projection),
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
        let mut hits = Vec::new();
        for batch in batches {
            let ids = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            let coordinates = batch[DOC_INDEX_COL]
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let coordinate = coordinates
                    .value(row)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(0);
                hits.push((ids.value(row), coordinate));
            }
        }
        hits.sort_unstable();
        assert_eq!(hits, vec![(1, 0), (2, 1)]);
    }

    #[tokio::test]
    async fn local_mode_unions_base_and_active_with_consistent_score_schema() {
        // Regression for the `_score` nullability mismatch between
        // FtsIndexExec (active arm) and FTS_SCHEMA (base/SSTable). The
        // active-only test below would not catch this — UnionExec rejects
        // schema-inequality, so we need at least one base + one active
        // source to exercise that code path.
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();

        // Base Lance dataset with FTS index on the `text` column.
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_batch(
                &schema,
                &[1, 2],
                &["lance rocks", "unrelated text"],
            )],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        // Active memtable with its own FTS index, containing a matching row.
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_batch(
            &schema,
            &[3, 4],
            &["lance memwal goes fast", "completely unrelated"],
        );
        batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: indexes,
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a base+active union plan");

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Both base id=1 ("lance rocks") and active id=3 ("lance memwal ...")
        // should match. id=2 / id=4 do not contain "lance".
        assert!(
            total >= 2,
            "expected at least the 2 'lance' rows from base+active, got {total}"
        );

        // Both sources must agree on _score nullability — verifies the fix.
        let out = batches[0].schema();
        let score_field = out
            .field_with_name(SCORE_COLUMN)
            .expect("_score column missing from output");
        assert!(
            score_field.is_nullable(),
            "_score must be nullable to stay union-compatible across base+active"
        );

        // Sanity: ids contain at least one base hit (id=1) and one active hit (id=3).
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
        assert!(ids.contains(&1), "missing base hit id=1; got ids={ids:?}");
        assert!(ids.contains(&3), "missing active hit id=3; got ids={ids:?}");
    }

    /// A prefilter on a full-text search must drop hits failing the predicate
    /// from both the base arm (native scanner filter) and the active memtable
    /// arm (materialized-hit mask), even though they match the query text.
    #[tokio::test]
    async fn prefilter_drops_nonmatching_hits_across_base_and_active() {
        use crate::index::DatasetIndexExt;
        use datafusion::prelude::{col, lit};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();

        // Base rows 1 and 2 both contain "lance".
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_batch(&schema, &[1, 2], &["lance one", "lance two"])],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        // Active memtable rows 0 and 3 both contain "lance".
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_batch(&schema, &[0, 3], &["lance zero", "lance three"]);
        batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: indexes,
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            // `id >= 2` keeps base id=2 and active id=3; drops base id=1 and active id=0.
            .with_filter(Some(col("id").gt_eq(lit(2i32))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a filtered base+active plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
        ids.sort();
        assert_eq!(
            ids,
            vec![2, 3],
            "prefilter must keep only id>=2 across base (id=2) and active (id=3), \
            dropping base id=1 and active id=0; got {ids:?}"
        );
    }

    /// The SSTable arm must apply the filter as a true FTS prefilter, and that
    /// prefiltered candidate set must compose with cross-generation block-list
    /// filtering plus over-fetch. Gen 1's best predicate-matching hit (id=3) is
    /// superseded by gen 2; with over-fetch, gen 1 should still contribute id=4.
    #[tokio::test]
    async fn prefilter_on_sstable_composes_with_block_list() {
        use crate::dataset::mem_wal::scanner::data_source::ShardSnapshot;
        use crate::index::DatasetIndexExt;
        use datafusion::prelude::{col, lit};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let shard_id = uuid::Uuid::new_v4();

        // Gen 1: id=1 matches strongly but fails the predicate. id=3 matches
        // strongly but is stale (blocked by gen 2). id=4 is the next live
        // predicate match that only survives if the SSTable arm prefilters and
        // over-fetches before the block-list drops id=3.
        let gen1_uri = format!("{}/_mem_wal/{}/gen_1", base_uri, shard_id);
        let mut gen1 = write_dataset(
            &gen1_uri,
            vec![make_batch(
                &schema,
                &[1, 3, 4],
                &["lance lance lance lance", "lance lance lance", "lance"],
            )],
        )
        .await;
        gen1.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        // Gen 2: newer id=3 shadows gen 1's match but does not match the query.
        let gen2_uri = format!("{}/_mem_wal/{}/gen_2", base_uri, shard_id);
        let mut gen2 =
            write_dataset(&gen2_uri, vec![make_batch(&schema, &[3], &["other text"])]).await;
        gen2.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        let snapshot = ShardSnapshot::new(shard_id)
            .with_current_generation(3)
            .with_sstable(1, "gen_1".to_string())
            .with_sstable(2, "gen_2".to_string());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![snapshot]);

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            .with_filter(Some(col("id").gt_eq(lit(3i32))))
            .with_overfetch_factor(2.0);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                None,
            )
            .await
            .expect("planner should produce a filtered SSTable plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            ids,
            vec![4],
            "SSTable FTS prefilter should return live id=4 after stale id=3 is blocked; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn active_tombstone_masks_base_fts_hit() {
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let base_schema = fts_schema();
        let mem_schema = fts_tombstone_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());

        let mut base = write_dataset(
            &base_uri,
            vec![make_batch(&base_schema, &[1, 2], &["lance lance", "lance"])],
        )
        .await;
        base.create_index(
            &["text"],
            IndexType::Inverted,
            Some("text_fts".to_string()),
            &InvertedIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        let active_tombstone = make_tombstone_batch(&mem_schema, &[(1, None, true)]);
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut index_store = IndexStore::new();
        index_store.enable_pk_index(&[("id".to_string(), 0)]);
        index_store.add_fts("text_fts".to_string(), 1, "text".to_string());
        let (_, row_offset, batch_position) = batch_store.append(active_tombstone.clone()).unwrap();
        index_store
            .insert_with_batch_position(&active_tombstone, row_offset, Some(batch_position))
            .unwrap();
        let index_store = Arc::new(index_store);

        let collector = LsmDataSourceCollector::new(Arc::new(base), vec![])
            .with_in_memory_memtables(
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
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], base_schema)
            .with_overfetch_factor(2.0);

        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                None,
            )
            .await
            .unwrap();
        let stream = plan
            .execute(0, datafusion::prelude::SessionContext::new().task_ctx())
            .unwrap();
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
            ids,
            vec![2],
            "active tombstone for id=1 must block the older base FTS hit; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn active_update_and_tombstone_mask_frozen_fts_hits() {
        let base_schema = fts_schema();
        let mem_schema = fts_tombstone_schema();

        let frozen_batch = make_tombstone_batch(
            &mem_schema,
            &[
                (1, Some("lance stale update"), false),
                (2, Some("lance live"), false),
                (3, Some("lance deleted"), false),
            ],
        );
        let frozen_batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut frozen_index_store = IndexStore::new();
        frozen_index_store.enable_pk_index(&[("id".to_string(), 0)]);
        frozen_index_store.add_fts("text_fts".to_string(), 1, "text".to_string());
        let (_, frozen_row_offset, frozen_batch_position) =
            frozen_batch_store.append(frozen_batch.clone()).unwrap();
        frozen_index_store
            .insert_with_batch_position(
                &frozen_batch,
                frozen_row_offset,
                Some(frozen_batch_position),
            )
            .unwrap();

        let active_batch = make_tombstone_batch(
            &mem_schema,
            &[(1, Some("fresh other text"), false), (3, None, true)],
        );
        let active_batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut active_index_store = IndexStore::new();
        active_index_store.enable_pk_index(&[("id".to_string(), 0)]);
        active_index_store.add_fts("text_fts".to_string(), 1, "text".to_string());
        let (_, active_row_offset, active_batch_position) =
            active_batch_store.append(active_batch.clone()).unwrap();
        active_index_store
            .insert_with_batch_position(
                &active_batch,
                active_row_offset,
                Some(active_batch_position),
            )
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let shard_id = uuid::Uuid::new_v4();
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                shard_id,
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store: active_batch_store,
                        index_store: Arc::new(active_index_store),
                        schema: mem_schema.clone(),
                        generation: 2,
                    },
                    frozen: vec![InMemoryMemTableRef {
                        batch_store: frozen_batch_store,
                        index_store: Arc::new(frozen_index_store),
                        schema: mem_schema,
                        generation: 1,
                    }],
                },
            );
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], base_schema);

        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .unwrap();
        let stream = plan
            .execute(0, datafusion::prelude::SessionContext::new().task_ctx())
            .unwrap();
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
            vec![2],
            "active update id=1 and tombstone id=3 must block stale frozen FTS hits; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn active_filtered_search_without_pk_applies_small_limit_after_filter() {
        use datafusion::prelude::{col, lit};

        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_batch(
            &schema,
            &[1, 2, 3],
            &["lance", "lance filler", "lance filler filler"],
        );
        batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec![], schema)
            .with_filter(Some(col("id").gt_eq(lit(1i32))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(2),
                None,
            )
            .await
            .expect("planner should produce an active-only filtered plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            vec![1, 2],
            "no-PK filtered active search must apply the limit after filtering \
             and keep the top-scoring matching hits; got ids={ids:?}"
        );
    }

    #[tokio::test]
    async fn fuzzy_and_query_is_rejected_when_active_memtable_is_present() {
        use lance_index::scalar::inverted::query::{
            FtsQuery as IndexFtsQuery, MatchQuery, Operator,
        };

        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_batch(&schema, &[1], &["lance memwal"]);
        let (_, row_offset, batch_position) = batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, row_offset, Some(batch_position))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );
        let planner = LsmFtsSearchPlanner::new(collector, vec![], schema);
        let query = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("lance memwal".to_string())
                .with_operator(Operator::And)
                .with_fuzziness(Some(1)),
        ));

        let err = planner
            .plan_search("text", query, Some(10), None)
            .await
            .expect_err("fuzzy AND should be rejected consistently");
        assert!(
            err.to_string().contains("fuzzy full-text search"),
            "unexpected error for fuzzy AND query: {err}"
        );
    }

    #[tokio::test]
    async fn base_only_boolean_query_uses_dataset_scanner_support() {
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::inverted::query::{
            BooleanQuery, FtsQuery as IndexFtsQuery, MatchQuery, Occur,
        };
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_batch(
                &schema,
                &[1, 2],
                &["lance rocks", "unrelated text"],
            )],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());
        let collector = LsmDataSourceCollector::new(base_ds, vec![]);
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);

        let query = FullTextSearchQuery::new_query(IndexFtsQuery::Boolean(BooleanQuery::new(
            vec![(Occur::Must, MatchQuery::new("lance".to_string()).into())],
        )));
        let plan = planner
            .plan_search("text", query, Some(10), Some(&["id".to_string()]))
            .await
            .expect("base-only boolean query should be delegated to dataset scanner");
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1]);
    }

    #[tokio::test]
    async fn boolean_query_ignores_active_memtable_without_relevant_fts_index() {
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::inverted::query::{
            BooleanQuery, FtsQuery as IndexFtsQuery, MatchQuery, Occur,
        };
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_batch(
                &schema,
                &[1, 2],
                &["lance rocks", "unrelated text"],
            )],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        let active_batch = make_batch(&schema, &[99], &["active text has no fts index"]);
        let (_, row_offset, batch_position) = batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, row_offset, Some(batch_position))
            .unwrap();
        let indexes = Arc::new(indexes);
        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: indexes,
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);

        let query = FullTextSearchQuery::new_query(IndexFtsQuery::Boolean(BooleanQuery::new(
            vec![(Occur::Must, MatchQuery::new("lance".to_string()).into())],
        )));
        let plan = planner
            .plan_search("text", query, Some(10), Some(&["id".to_string()]))
            .await
            .expect("irrelevant active memtable must not reject base-supported boolean query");
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1]);
    }

    /// The base arm must apply the filter as a true *prefilter*, not a
    /// post-filter on the BM25 top-k. With `k = 1` and the higher-scoring base
    /// doc failing the predicate, a post-filter would return zero rows; a
    /// prefilter restricts BM25 to matching rows and returns the lower-scoring
    /// match. Regression for a missing `scanner.prefilter(true)` on the base arm.
    #[tokio::test]
    async fn prefilter_on_base_is_not_a_lossy_postfilter() {
        use crate::index::DatasetIndexExt;
        use datafusion::prelude::{col, lit};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let schema = fts_schema();
        let tmp = tempfile::tempdir().unwrap();

        // id=1 is a short doc ("lance") so it scores higher under BM25 length
        // normalization; id=2 buries "lance" among filler so it scores lower.
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_batch(
                &schema,
                &[1, 2],
                &["lance", "lance filler filler filler filler filler"],
            )],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        // Base-only collector (no in-memory memtables): isolates the base arm.
        let collector = LsmDataSourceCollector::new(base_ds, vec![]);
        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            // Keeps only id=2, which is the *lower*-scoring match. A post-filter
            // on the top-1 (id=1) would drop everything.
            .with_filter(Some(col("id").gt_eq(lit(2i32))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                None,
            )
            .await
            .expect("planner should produce a filtered base plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            ids,
            vec![2],
            "prefilter must return the lower-scoring match id=2, not post-filter \
             the top-1 (id=1) down to nothing; got {ids:?}"
        );
    }

    /// The active memtable FTS arm must also apply the predicate before its
    /// top-k cap. With `k = 1` and the higher-scoring active doc failing the
    /// predicate, pushing the limit into the index would return zero rows.
    #[tokio::test]
    async fn prefilter_on_active_is_not_a_lossy_postfilter() {
        use datafusion::prelude::{col, lit};

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new("text", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![
                    "lance",
                    "lance filler filler filler filler filler",
                ])),
                Arc::new(StringArray::from(vec!["archived", "active"])),
            ],
        )
        .unwrap();

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            .with_filter(Some(col("status").eq(lit("active"))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                None,
            )
            .await
            .expect("planner should produce a filtered active plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            ids,
            vec![2],
            "active FTS prefilter must return the lower-scoring matching row, \
             not post-filter the top-1 down to nothing; got {ids:?}"
        );
    }

    /// The active FTS arm must also avoid capping before newest-PK filtering.
    /// A stale high-scoring hit can be removed by `FtsIndexExec`; a lower
    /// scoring live hit must still be available for the final global top-k.
    #[tokio::test]
    async fn active_limit_applies_after_newest_pk_recency_filter() {
        use datafusion::prelude::{col, lit};

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new("text", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 1])),
                Arc::new(StringArray::from(vec![
                    "lance",
                    "lance filler filler filler filler filler",
                    "other text",
                ])),
                Arc::new(StringArray::from(vec!["active", "active", "active"])),
            ],
        )
        .unwrap();

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            .with_filter(Some(col("status").eq(lit("active"))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(1),
                None,
            )
            .await
            .expect("planner should produce a filtered active plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            ids,
            vec![2],
            "active FTS limit must apply after newest-PK filtering; got {ids:?}"
        );
    }

    /// An in-memtable update whose *newest* version fails the prefilter must
    /// exclude the PK, not leak the stale older hit that still passes. Both
    /// versions of pk=5 match the query text "lance", but only the older one is
    /// "active"; the current version is "archived" and must be dropped.
    /// Regression for filter-before-dedup on the active FTS arm.
    #[tokio::test]
    async fn prefilter_excludes_pk_whose_newest_version_fails() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use arrow_schema::{DataType, Field};
        use datafusion::prelude::{col, lit};

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new("text", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
        ]));
        let make_row = |statuses: &[&str]| -> RecordBatch {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![5; statuses.len()])),
                    Arc::new(StringArray::from(vec!["lance text"; statuses.len()])),
                    Arc::new(StringArray::from(statuses.to_vec())),
                ],
            )
            .unwrap()
        };

        // Base unindexed → contributes nothing; isolate the active arm.
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let base_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![999])),
                Arc::new(StringArray::from(vec!["unrelated"])),
                Arc::new(StringArray::from(vec!["active"])),
            ],
        )
        .unwrap();
        let base_ds = Arc::new(write_dataset(&base_uri, vec![base_batch]).await);

        // Active memtable: pk=5 appended twice (active then archived), both "lance".
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_row(&["active", "archived"]);
        batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: indexes,
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            .with_filter(Some(col("status").eq(lit("active"))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a filtered active plan");

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 0,
            "pk=5's current version is 'archived' and must be excluded; the stale \
             'active' older hit must not leak (filter evaluated on newest version)"
        );
    }

    /// Cross-arm stale hits must be blocked even if the newer active row fails
    /// the prefilter. The base copy of pk=5 matches both text and status, but
    /// the newer active copy is archived; pk=5 must not leak from the base arm.
    #[tokio::test]
    async fn prefilter_blocks_base_hit_when_active_newest_fails() {
        use crate::dataset::mem_wal::scanner::collector::{InMemoryMemTableRef, InMemoryMemTables};
        use crate::dataset::mem_wal::write::{BatchStore, IndexStore};
        use crate::index::DatasetIndexExt;
        use datafusion::prelude::{col, lit};
        use lance_index::IndexType;
        use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;

        let mut id_meta = HashMap::new();
        id_meta.insert(
            "lance-schema:unenforced-primary-key".to_string(),
            "true".to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(id_meta),
            Field::new("text", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, false),
        ]));
        let make_rows = |rows: &[(i32, &str, &str)]| -> RecordBatch {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(
                        rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|(_, text, _)| *text).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter()
                            .map(|(_, _, status)| *status)
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let mut base_ds = write_dataset(
            &base_uri,
            vec![make_rows(&[
                (5, "lance base stale", "active"),
                (6, "lance base live", "active"),
            ])],
        )
        .await;
        base_ds
            .create_index(
                &["text"],
                IndexType::Inverted,
                Some("text_fts".to_string()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let base_ds = Arc::new(Dataset::open(&base_uri).await.unwrap());

        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let active_batch = make_rows(&[(5, "lance active newest", "archived")]);
        batch_store.append(active_batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&active_batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let collector = LsmDataSourceCollector::new(base_ds, vec![]).with_in_memory_memtables(
            uuid::Uuid::new_v4(),
            InMemoryMemTables {
                active: InMemoryMemTableRef {
                    batch_store,
                    index_store: indexes,
                    schema: schema.clone(),
                    generation: 1,
                },
                frozen: vec![],
            },
        );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema)
            .with_filter(Some(col("status").eq(lit("active"))));
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a filtered base+active plan");

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let id_array = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                ids.push(id_array.value(row));
            }
        }
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![6],
            "base pk=5 passes the filter but is superseded by active archived pk=5; got {ids:?}"
        );
    }

    #[tokio::test]
    async fn local_mode_active_memtable_only_returns_score_sorted_hits() {
        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        // text column has field_id 1 in fts_schema()
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let batch = make_batch(
            &schema,
            &[1, 2, 3, 4],
            &[
                "lance is a columnar data format",
                "memwal handles streaming writes",
                "lance memwal lance lance",
                "completely unrelated",
            ],
        );
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("local mode planner should produce a plan");

        // Plan executes and emits _score-sorted rows.
        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(
            total >= 2,
            "expected at least the 2 'lance' rows, got {total}"
        );

        // Schema must include _score and the PK id.
        let out = batches[0].schema();
        assert!(out.field_with_name(SCORE_COLUMN).is_ok());
        assert!(out.field_with_name("id").is_ok());

        // _score must be non-ascending across the result.
        let mut prev_score: Option<f32> = None;
        for batch in &batches {
            let score = batch
                .column_by_name(SCORE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let s = score.value(i);
                if let Some(p) = prev_score {
                    assert!(p >= s, "scores not sorted DESC: {p} then {s}");
                }
                prev_score = Some(s);
            }
        }
    }

    #[tokio::test]
    async fn active_match_query_preserves_and_operator() {
        use lance_index::scalar::inverted::query::{
            FtsQuery as IndexFtsQuery, MatchQuery, Operator,
        };

        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());
        let batch = make_batch(
            &schema,
            &[1, 2, 3],
            &["lance only", "memwal only", "lance memwal"],
        );
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let query = FullTextSearchQuery::new_query(IndexFtsQuery::Match(
            MatchQuery::new("lance memwal".to_string())
                .with_operator(Operator::And)
                .with_column(Some("text".to_string())),
        ));
        let plan = planner
            .plan_search("text", query, Some(10), None)
            .await
            .expect("planner should produce an active-only plan");

        let ctx = datafusion::prelude::SessionContext::new();
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
            vec![3],
            "AND query must only return rows containing both terms; got ids={ids:?}"
        );
    }

    #[tokio::test]
    async fn local_mode_active_dedups_updated_pk_keeping_newest() {
        // The active memtable is an append log and the FTS index is
        // append-only, so a PK updated before flush is searchable as two
        // row-positions. WithinSourceDedupExec(KeepMaxRowAddr) must collapse
        // them to the newest insert. Without it the same PK would surface
        // twice (criterion 2 violation).
        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());

        // First append (positions 0,1): id=1 is the stale version of the PK.
        let batch_old = make_batch(&schema, &[1, 2], &["lance stale version", "other doc"]);
        batch_store.append(batch_old.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch_old, 0, Some(0))
            .unwrap();

        // Second append (position 2): id=1 updated — same PK, later row.
        let batch_new = make_batch(&schema, &[1], &["lance fresh version"]);
        batch_store.append(batch_new.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch_new, 2, Some(1))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("lance".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce an active-only plan");

        let ctx = datafusion::prelude::SessionContext::new();
        let stream = plan.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut rows: Vec<(i32, String)> = Vec::new();
        for b in &batches {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let texts = b
                .column_by_name("text")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((ids.value(i), texts.value(i).to_string()));
            }
        }

        // id=1 must appear exactly once, and it must be the *newest* version.
        let id1: Vec<&(i32, String)> = rows.iter().filter(|(id, _)| *id == 1).collect();
        assert_eq!(
            id1.len(),
            1,
            "updated PK id=1 must be deduped to one row; got {rows:?}"
        );
        assert_eq!(
            id1[0].1, "lance fresh version",
            "dedup must keep the newest (max row-position) version"
        );
    }

    #[tokio::test]
    async fn active_stale_update_predicate_crossing_leaks() {
        // A PK update that crosses out of the match set: pk=1 inserted as
        // "alpha lance", then updated to "beta lance". The append-only inverted
        // index keeps the old "alpha" posting live, so an "alpha" search still
        // matches the STALE pk=1 row — and the fresh "beta lance" row isn't even
        // a candidate, so a result-set dedup has nothing to suppress it against.
        // `FtsIndexExec` drops it predicate-independently: pk=1's newest visible
        // row is "beta lance", so the "alpha" hit is not the newest.
        let schema = fts_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(16));
        let mut indexes = IndexStore::new();
        indexes.enable_pk_index(&[("id".to_string(), 0)]);
        indexes.add_fts("text_fts".to_string(), 1, "text".to_string());

        // Insert pk=1 ("alpha lance") and an unrelated live pk=2 ("alpha foo").
        let b1 = make_batch(&schema, &[1, 2], &["alpha lance", "alpha foo"]);
        let (bp1, off1, _) = batch_store.append(b1.clone()).unwrap();
        indexes
            .insert_with_batch_position(&b1, off1, Some(bp1))
            .unwrap();

        // Update pk=1 → "beta lance" (no longer matches "alpha").
        let b2 = make_batch(&schema, &[1], &["beta lance"]);
        let (bp2, off2, _) = batch_store.append(b2.clone()).unwrap();
        indexes
            .insert_with_batch_position(&b2, off2, Some(bp2))
            .unwrap();
        let indexes = Arc::new(indexes);

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store,
                        index_store: indexes,
                        schema: schema.clone(),
                        generation: 1,
                    },
                    frozen: vec![],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("alpha".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a plan");

        let ctx = datafusion::prelude::SessionContext::new();
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

        assert!(
            !ids.contains(&1),
            "stale pk=1 (now 'beta lance') leaked on an 'alpha' search; got ids={ids:?}"
        );
        assert!(
            ids.contains(&2),
            "live pk=2 ('alpha foo') must still match 'alpha'; got ids={ids:?}"
        );
    }

    #[tokio::test]
    async fn cross_gen_stale_update_blocked_by_newer_memtable() {
        // The cross-generation analog of `active_stale_update_predicate_crossing_leaks`:
        // pk=1's stale "alpha" version lives in a FROZEN memtable and its fresh
        // "beta" version in the ACTIVE one. The frozen arm's recency filter is
        // per-generation and can't see the newer gen, so only the cross-gen
        // block-list can drop the stale "alpha" hit. The cluster constantly
        // freezes memtables, so an insert and its later update/delete split
        // across in-memory generations — this is the residual fuzz phantom.
        let schema = fts_schema();

        // Frozen gen=1: pk=1 "alpha lance" (matches), pk=2 "alpha foo" (live).
        let frozen_store = Arc::new(BatchStore::with_capacity(16));
        let mut frozen_idx = IndexStore::new();
        frozen_idx.enable_pk_index(&[("id".to_string(), 0)]);
        frozen_idx.add_fts("text_fts".to_string(), 1, "text".to_string());
        let fb = make_batch(&schema, &[1, 2], &["alpha lance", "alpha foo"]);
        let (bp, off, _) = frozen_store.append(fb.clone()).unwrap();
        frozen_idx
            .insert_with_batch_position(&fb, off, Some(bp))
            .unwrap();

        // Active gen=2: pk=1 updated to "beta lance" (no longer matches "alpha").
        let active_store = Arc::new(BatchStore::with_capacity(16));
        let mut active_idx = IndexStore::new();
        active_idx.enable_pk_index(&[("id".to_string(), 0)]);
        active_idx.add_fts("text_fts".to_string(), 1, "text".to_string());
        let ab = make_batch(&schema, &[1], &["beta lance"]);
        let (bp, off, _) = active_store.append(ab.clone()).unwrap();
        active_idx
            .insert_with_batch_position(&ab, off, Some(bp))
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let base_uri = format!("{}/base", tmp.path().to_str().unwrap());
        let collector = LsmDataSourceCollector::without_base_table(base_uri, vec![])
            .with_in_memory_memtables(
                uuid::Uuid::new_v4(),
                InMemoryMemTables {
                    active: InMemoryMemTableRef {
                        batch_store: active_store,
                        index_store: Arc::new(active_idx),
                        schema: schema.clone(),
                        generation: 2,
                    },
                    frozen: vec![InMemoryMemTableRef {
                        batch_store: frozen_store,
                        index_store: Arc::new(frozen_idx),
                        schema: schema.clone(),
                        generation: 1,
                    }],
                },
            );

        let planner = LsmFtsSearchPlanner::new(collector, vec!["id".to_string()], schema);
        let plan = planner
            .plan_search(
                "text",
                FullTextSearchQuery::new("alpha".to_string()),
                Some(10),
                None,
            )
            .await
            .expect("planner should produce a plan");

        let ctx = datafusion::prelude::SessionContext::new();
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

        assert!(
            !ids.contains(&1),
            "stale frozen pk=1 ('alpha lance', now 'beta lance' in the active gen) \
             leaked on an 'alpha' search; got ids={ids:?}"
        );
        assert!(
            ids.contains(&2),
            "live pk=2 ('alpha foo', only in the frozen gen) must still match; got ids={ids:?}"
        );
    }
}
