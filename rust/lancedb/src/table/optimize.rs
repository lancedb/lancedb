// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Table optimization operations for compaction, pruning, and index optimization.
//!
//! This module contains the implementation of optimization operations that help
//! maintain good performance for LanceDB tables.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use arrow_schema::DataType;
use chrono::{DateTime, Utc};
use lance::dataset::cleanup::{CleanupPolicyBuilder, RemovalStats};
use lance::dataset::optimize::{
    CompactionMetrics, CompactionPlan, CompactionPlanner, IndexRemapperOptions,
    compact_files_with_planner, plan_compaction,
};
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_core::utils::tracing::{DATASET_COMPACTING_EVENT, TRACE_DATASET_EVENTS};
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::optimize::OptimizeOptions as IndexOptimizeOptions;
use log::{debug, info};

use super::NativeTable;
use crate::error::{Error, Result};
pub use chrono::Duration;
use lance::dataset::optimize::CompactionOptions as LanceOptimizeOptions;
pub use lance::dataset::optimize::{CompactionMode as OptimizeDataMode, IndexRemapMode};

/// Deprecated: use [`OptimizeOptions`].
#[deprecated(note = "use table::optimize::OptimizeOptions")]
pub type CompactionOptions = LanceOptimizeOptions;
/// Deprecated: use [`OptimizeDataMode`].
#[deprecated(note = "use OptimizeDataMode")]
pub type CompactionMode = OptimizeDataMode;
/// Deprecated: use [`OptimizeOptions`].
#[deprecated(note = "use table::optimize::OptimizeOptions")]
pub type CompactionOverrides = OptimizeOverrides;
/// Deprecated: use [`OptimizeResult`].
#[deprecated(note = "use OptimizeResult")]
pub type CompactionResult = OptimizeResult;

/// Per-invocation Optimize settings. Omitted fields inherit Embedded table
/// `lance.compaction.*` settings or Enterprise table policy, then built-in defaults.
/// Explicit values, including zero I/O buffering, override table policy.
///
/// Passing these settings to [`crate::Table::optimize`] runs Embedded's full
/// maintenance cycle or submits Enterprise data rewriting. Select
/// [`OptimizeAction::RewriteData`] explicitly to rewrite only Embedded data.
/// Durable jobs and previews cover data rewriting only. Enterprise supports
/// `target_rows_per_fragment`, `max_source_rows`, `io_buffer_size`, and `batch_size`;
/// other non-default settings and custom remappers are Embedded-only.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OptimizeOptions {
    /// Override Lance data-rewrite `target_rows_per_fragment`; `None` inherits table policy.
    pub target_rows_per_fragment: Option<usize>,
    /// Override Lance data-rewrite `max_rows_per_group`; `None` inherits table policy.
    pub max_rows_per_group: Option<usize>,
    /// Override Lance data-rewrite `max_bytes_per_file`; `None` inherits table policy.
    pub max_bytes_per_file: Option<usize>,
    /// Override Lance data-rewrite `min_bytes_per_file`; `None` inherits table policy.
    pub min_bytes_per_file: Option<usize>,
    /// Override Lance data-rewrite `materialize_deletions`; `None` inherits table policy.
    pub materialize_deletions: Option<bool>,
    /// Override Lance data-rewrite `materialize_deletions_threshold`; `None` inherits table policy.
    pub materialize_deletions_threshold: Option<f32>,
    /// Override Lance data-rewrite `num_threads`; `None` inherits table policy.
    pub num_threads: Option<usize>,
    /// Override Lance data-rewrite `batch_size`; `None` inherits table policy.
    pub batch_size: Option<usize>,
    /// Override Lance data-rewrite `io_buffer_size`; `None` inherits table policy.
    pub io_buffer_size: Option<u64>,
    /// Override Lance data-rewrite `defer_index_remap`; `None` inherits table policy.
    pub defer_index_remap: Option<bool>,
    /// Override Lance data-rewrite `index_remap_mode`; `None` inherits table policy.
    pub index_remap_mode: Option<IndexRemapMode>,
    /// Override Lance data-rewrite `compaction_mode`; `None` inherits table policy.
    pub compaction_mode: Option<OptimizeDataMode>,
    /// Override Lance data-rewrite `binary_copy_read_batch_bytes`; `None` inherits table policy.
    pub binary_copy_read_batch_bytes: Option<usize>,
    /// Override Lance data-rewrite `max_source_fragments`; `None` inherits table policy.
    pub max_source_fragments: Option<usize>,
    /// Override Lance data-rewrite `max_source_rows`; `None` inherits table policy.
    pub max_source_rows: Option<usize>,
    /// Override Lance data-rewrite `max_source_bytes`; `None` inherits table policy.
    pub max_source_bytes: Option<u64>,
    /// Override Lance data-rewrite `source_budget_mode`; `None` inherits table policy.
    pub source_budget_mode: Option<lance::dataset::optimize::SourceBudgetMode>,
    /// Override Lance data-rewrite `excluded_fragment_ids`; `None` inherits table policy.
    pub excluded_fragment_ids: Option<Vec<u32>>,
    /// Override Lance data-rewrite `max_overlays_per_fragment`; `None` inherits table policy.
    pub max_overlays_per_fragment: Option<usize>,
    /// Override Lance data-rewrite `max_data_files_per_fragment`; `None` inherits table policy.
    pub max_data_files_per_fragment: Option<usize>,
    /// Override Lance data-rewrite `scope`; `None` inherits table policy.
    pub scope: Option<lance::dataset::optimize::CompactionScope>,
    /// Override Lance data-rewrite `data_storage_version`; `None` inherits table policy.
    pub data_storage_version: Option<crate::connection::LanceFileVersion>,
    /// Override Lance data-rewrite `transaction_properties`; `None` inherits table policy.
    pub transaction_properties: Option<Arc<std::collections::HashMap<String, String>>>,
    /// Override Lance data-rewrite `data_cache_bytes`; `None` inherits table policy.
    pub data_cache_bytes: Option<u64>,
    // Python's released nullable settings distinguish explicit None from omission.
    // Public only to allow Rust struct-update syntax; callers normally leave empty.
    #[doc(hidden)]
    pub cleared_fields: HashSet<String>,
}

impl OptimizeOptions {
    fn resolve(
        self,
        config: &std::collections::HashMap<String, String>,
    ) -> Result<LanceOptimizeOptions> {
        let mut resolved = LanceOptimizeOptions::from_dataset_config(config)?;
        for field in &self.cleared_fields {
            match field.as_str() {
                "max_bytes_per_file" => resolved.max_bytes_per_file = None,
                "min_bytes_per_file" => resolved.min_bytes_per_file = None,
                "num_threads" => resolved.num_threads = None,
                "batch_size" => resolved.batch_size = None,
                "io_buffer_size" => resolved.io_buffer_size = None,
                "compaction_mode" => resolved.compaction_mode = None,
                "binary_copy_read_batch_bytes" => resolved.binary_copy_read_batch_bytes = None,
                "max_source_fragments" => resolved.max_source_fragments = None,
                "max_source_rows" => resolved.max_source_rows = None,
                "max_source_bytes" => resolved.max_source_bytes = None,
                "max_overlays_per_fragment" => resolved.max_overlays_per_fragment = None,
                "max_data_files_per_fragment" => resolved.max_data_files_per_fragment = None,
                "data_storage_version" => resolved.data_storage_version = None,
                "transaction_properties" => resolved.transaction_properties = None,
                "data_cache_bytes" => resolved.data_cache_bytes = None,
                _ => {
                    return Err(Error::InvalidInput {
                        message: format!("Unknown nullable Optimize setting: {field}"),
                    });
                }
            }
        }

        if let Some(value) = self.target_rows_per_fragment {
            resolved.target_rows_per_fragment = value;
        }
        if let Some(value) = self.max_rows_per_group {
            resolved.max_rows_per_group = value;
        }
        if let Some(value) = self.max_bytes_per_file {
            resolved.max_bytes_per_file = Some(value);
        }
        if let Some(value) = self.min_bytes_per_file {
            resolved.min_bytes_per_file = Some(value);
        }
        if let Some(value) = self.materialize_deletions {
            resolved.materialize_deletions = value;
        }
        if let Some(value) = self.materialize_deletions_threshold {
            resolved.materialize_deletions_threshold = value;
        }
        if let Some(value) = self.num_threads {
            resolved.num_threads = Some(value);
        }
        if let Some(value) = self.batch_size {
            resolved.batch_size = Some(value);
        }
        if let Some(value) = self.io_buffer_size {
            resolved.io_buffer_size = Some(value);
        }
        if let Some(value) = self.defer_index_remap {
            resolved.defer_index_remap = value;
        }
        if let Some(value) = self.index_remap_mode {
            resolved.index_remap_mode = value;
        }
        if let Some(value) = self.compaction_mode {
            resolved.compaction_mode = Some(value);
        }
        if let Some(value) = self.binary_copy_read_batch_bytes {
            resolved.binary_copy_read_batch_bytes = Some(value);
        }
        if let Some(value) = self.max_source_fragments {
            resolved.max_source_fragments = Some(value);
        }
        if let Some(value) = self.max_source_rows {
            resolved.max_source_rows = Some(value);
        }
        if let Some(value) = self.max_source_bytes {
            resolved.max_source_bytes = Some(value);
        }
        if let Some(value) = self.source_budget_mode {
            resolved.source_budget_mode = value;
        }
        if let Some(value) = self.excluded_fragment_ids {
            resolved.excluded_fragment_ids = value;
        }
        if let Some(value) = self.max_overlays_per_fragment {
            resolved.max_overlays_per_fragment = Some(value);
        }
        if let Some(value) = self.max_data_files_per_fragment {
            resolved.max_data_files_per_fragment = Some(value);
        }
        if let Some(value) = self.scope {
            resolved.scope = value;
        }
        if let Some(value) = self.data_storage_version {
            resolved.data_storage_version = Some(value);
        }
        if let Some(value) = self.transaction_properties {
            resolved.transaction_properties = Some(value);
        }
        if let Some(value) = self.data_cache_bytes {
            resolved.data_cache_bytes = Some(value);
        }
        Ok(resolved)
    }
}

const MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX: u64 = u32::MAX as u64;

/// Actual outcome of one Enterprise Optimize data rewrite, returned by its job handle.
///
/// A successful bounded run can leave more work for another invocation. A
/// partial commit reports uncommitted rows rather than inventing file counts.
/// File/fragment counts are unknown (`None`) when an older worker omitted them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct OptimizeResult {
    /// A complete bounded plan left eligible work for a later invocation.
    #[serde(default)]
    pub more_work: bool,
    /// Some planned groups were not committed.
    pub partial_commit: bool,
    /// Live source rows in committed rewrite groups.
    pub rows_succeeded: u64,
    /// Planned live rows not committed by this job.
    pub rows_failed: u64,
    /// Percentage of planned live rows committed, from 0 to 100.
    pub completion_percentage: f64,
    /// Files added by committed rewrites, including deletion files, when reported.
    pub files_added: Option<u64>,
    /// Files removed by committed rewrites, including deletion files, when reported.
    pub files_removed: Option<u64>,
    /// Fragments added by committed rewrites, when reported by the server.
    pub fragments_added: Option<u64>,
    /// Fragments removed by committed rewrites, when reported by the server.
    pub fragments_removed: Option<u64>,
}

/// Operation covered by an Optimize preview. Cleanup and separate index
/// maintenance are not included in a data-rewrite plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OptimizeOperation {
    /// Rewrite data files while maintaining their index references.
    RewriteData,
}

/// Effective data-rewrite settings after Enterprise policy resolution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct OptimizeResolvedOptions {
    pub target_rows_per_fragment: u64,
    pub max_source_rows: Option<u64>,
    /// Zero disables byte-based I/O backpressure; None uses the worker default.
    pub io_buffer_size: Option<u64>,
    pub batch_size: Option<u64>,
}

/// Non-executing Enterprise Optimize preview for one source snapshot.
///
/// Counts describe planned work, not committed results, memory use, or runtime.
/// A later submission plans its own snapshot and may select different work.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub struct OptimizePlan {
    pub operation: OptimizeOperation,
    pub table: String,
    pub namespace: Vec<String>,
    pub branch: String,
    pub source_version: u64,
    pub effective_options: OptimizeResolvedOptions,
    pub planned_groups: u64,
    pub planned_fragments: u64,
    pub planned_source_rows: u64,
    pub more_work: bool,
}

struct PrecomputedCompactionPlanner {
    plan: Mutex<Option<CompactionPlan>>,
}

impl PrecomputedCompactionPlanner {
    fn new(plan: CompactionPlan) -> Self {
        Self {
            plan: Mutex::new(Some(plan)),
        }
    }
}

#[async_trait::async_trait]
impl CompactionPlanner for PrecomputedCompactionPlanner {
    async fn plan(&self, dataset: &lance::Dataset) -> lance::Result<CompactionPlan> {
        let plan = self
            .plan
            .lock()
            .map_err(|_| lance_core::Error::internal("precomputed compaction plan lock poisoned"))?
            .take()
            .ok_or_else(|| {
                lance_core::Error::internal("precomputed compaction plan was already consumed")
            })?;
        debug_assert_eq!(dataset.manifest().version, plan.read_version());
        Ok(plan)
    }
}

fn sq_vector_dimension(data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::FixedSizeList(_, dimension) => Some(*dimension),
        DataType::List(field) => match field.data_type() {
            DataType::FixedSizeList(_, dimension) => Some(*dimension),
            _ => None,
        },
        _ => None,
    }
}

fn segment_touches_fragments(
    fragment_ids: Option<impl IntoIterator<Item = u32>>,
    affected_fragments: &HashSet<u64>,
) -> bool {
    fragment_ids.is_none_or(|fragment_ids| {
        fragment_ids
            .into_iter()
            .any(|fragment| affected_fragments.contains(&(fragment as u64)))
    })
}

fn oversized_sq_partition(
    partition_sizes: impl IntoIterator<Item = u64>,
    dimension: u64,
) -> Option<u64> {
    partition_sizes.into_iter().find(|partition_size| {
        partition_size
            .checked_mul(dimension)
            .is_none_or(|child_len| child_len > MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX)
    })
}

async fn validate_sq_index_remapping(
    dataset: &lance::Dataset,
    options: &LanceOptimizeOptions,
    has_custom_remapper: bool,
) -> Result<CompactionPlan> {
    let plan = plan_compaction(dataset, options).await?;
    if options.defer_index_remap || has_custom_remapper || dataset.manifest().uses_stable_row_ids()
    {
        return Ok(plan);
    }

    if plan.tasks.is_empty() {
        return Ok(plan);
    }
    let affected_fragments: HashSet<u64> = plan
        .tasks
        .iter()
        .flat_map(|task| task.fragments.iter().map(|fragment| fragment.id))
        .collect();

    for segment in dataset.load_indices().await?.iter() {
        if !segment_touches_fragments(
            segment
                .fragment_bitmap
                .as_ref()
                .map(|fragment_bitmap| fragment_bitmap.iter()),
            &affected_fragments,
        ) {
            continue;
        }
        let Some(field_id) = segment.fields.first() else {
            continue;
        };
        let Some(field) = dataset.schema().field_by_id(*field_id) else {
            continue;
        };
        let data_type = field.data_type();
        let Some(dimension) = sq_vector_dimension(&data_type) else {
            continue;
        };
        let dimension = u64::try_from(dimension).map_err(|_| Error::InvalidInput {
            message: format!(
                "SQ index '{}' has an invalid vector dimension of {}",
                segment.name, dimension
            ),
        })?;

        let field_path = dataset.schema().field_path(*field_id)?;
        let vector_index = match dataset
            .open_vector_index(&field_path, &segment.uuid, &NoOpMetricsCollector)
            .await
        {
            Ok(vector_index) => vector_index,
            Err(error) => {
                // The default remapper also treats an index it cannot open as
                // unusable and drops it, so it cannot reach the SQ take kernel.
                debug!(
                    "Skipping remap preflight for index segment {} because it could not be opened: {}",
                    segment.uuid, error
                );
                continue;
            }
        };
        if !matches!(
            vector_index.index_type(),
            IndexType::IvfSq | IndexType::IvfHnswSq
        ) {
            continue;
        }

        let partition_sizes = (0..vector_index.total_partitions())
            .map(|partition_id| vector_index.partition_size(partition_id) as u64);
        if let Some(partition_size) = oversized_sq_partition(partition_sizes, dimension) {
            let max_partition_size = MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX / dimension;
            return Err(Error::InvalidInput {
                message: format!(
                    "Cannot compact table because SQ index '{}' segment {} has a partition with {} vectors of dimension {}. Arrow's fixed-size-list take kernel cannot remap partitions whose child array exceeds {} values. Recreate the index with default IVF partitioning, or enough partitions to keep each partition at or below {} vectors, before compacting.",
                    segment.name,
                    segment.uuid,
                    partition_size,
                    dimension,
                    MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX,
                    max_partition_size,
                ),
            });
        }
    }

    Ok(plan)
}

/// Four common rewrite settings, retained for compatibility. Prefer
/// [`OptimizeOptions`] for the complete Embedded tuning surface. Omitted fields
/// inherit table policy, then built-in defaults; explicit zero I/O buffering wins.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OptimizeOverrides {
    pub target_rows_per_fragment: Option<usize>,
    pub max_source_rows: Option<usize>,
    /// Read-ahead budget in bytes; zero disables byte-based I/O backpressure.
    pub io_buffer_size: Option<u64>,
    pub batch_size: Option<usize>,
}

impl TryFrom<LanceOptimizeOptions> for OptimizeOverrides {
    type Error = Error;

    fn try_from(options: LanceOptimizeOptions) -> Result<Self> {
        let mut remaining = options.clone();
        let defaults = LanceOptimizeOptions::default();
        remaining.target_rows_per_fragment = defaults.target_rows_per_fragment;
        remaining.max_source_rows = defaults.max_source_rows;
        remaining.io_buffer_size = defaults.io_buffer_size;
        remaining.batch_size = defaults.batch_size;
        if remaining != defaults {
            return Err(Error::NotSupported { message: "Enterprise OptimizeOptions support only target_rows_per_fragment, max_source_rows, io_buffer_size, and batch_size; other options must retain their defaults".into() });
        }
        Ok(Self {
            target_rows_per_fragment: Some(options.target_rows_per_fragment),
            max_source_rows: options.max_source_rows,
            io_buffer_size: options.io_buffer_size,
            batch_size: options.batch_size,
        })
    }
}

impl From<OptimizeOverrides> for OptimizeOptions {
    fn from(options: OptimizeOverrides) -> Self {
        Self {
            target_rows_per_fragment: options.target_rows_per_fragment,
            max_source_rows: options.max_source_rows,
            io_buffer_size: options.io_buffer_size,
            batch_size: options.batch_size,
            ..Default::default()
        }
    }
}

impl TryFrom<OptimizeOptions> for OptimizeOverrides {
    type Error = Error;
    fn try_from(options: OptimizeOptions) -> Result<Self> {
        let target = options.target_rows_per_fragment;
        let mut resolved = Self::try_from(options.resolve(&Default::default())?)?;
        resolved.target_rows_per_fragment = target;
        Ok(resolved)
    }
}

impl From<OptimizeOverrides> for OptimizeAction {
    fn from(options: OptimizeOverrides) -> Self {
        Self::AllWithOptions(options.into())
    }
}
impl From<OptimizeOptions> for OptimizeAction {
    fn from(options: OptimizeOptions) -> Self {
        Self::AllWithOptions(options)
    }
}
// Preserve full-snapshot Lance options through the deprecated compatibility path.
#[allow(deprecated)]
impl From<LanceOptimizeOptions> for OptimizeAction {
    fn from(options: LanceOptimizeOptions) -> Self {
        Self::Compact {
            options,
            remap_options: None,
        }
    }
}

/// Optimize the dataset.
///
/// Similar to `VACUUM` in PostgreSQL, it offers different options to
/// optimize different parts of the table on disk.
///
/// By default, it optimizes everything, as [`OptimizeAction::All`].
#[derive(Default)]
pub enum OptimizeAction {
    /// Run all Embedded optimizations using table configuration and defaults.
    #[default]
    All,
    /// Run the convenience maintenance cycle with per-invocation settings.
    /// Embedded rewrites data, prunes versions, and optimizes indexes. Enterprise
    /// submits only its supported data rewrite, returning after acceptance.
    AllWithOptions(OptimizeOptions),
    /// Rewrite only data, preserving versions and skipping separate index maintenance.
    /// Settings inherit table policy. Custom remappers are Embedded-only.
    RewriteData {
        options: OptimizeOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    },
    /// Deprecated full-snapshot Lance options. Use [`Self::RewriteData`] with
    /// [`OptimizeOptions`] to preserve field presence and inherit table policy.
    #[deprecated(note = "use RewriteData with OptimizeOptions")]
    Compact {
        options: LanceOptimizeOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    },
    /// Prune old version of datasets
    ///
    /// Every change in LanceDb is additive.  When data is removed from a dataset a new version is
    /// created that doesn't contain the removed data.  However, the old version, which does contain
    /// the removed data, is left in place.  This is necessary for consistency and concurrency and
    /// also enables time travel functionality like the ability to checkout an older version of the
    /// dataset to undo changes.
    ///
    /// Over time, these old versions can consume a lot of disk space.  The prune operation will
    /// remove versions of the dataset that are older than a certain age.  This will free up the
    /// space used by that old data.
    ///
    /// Once a version is pruned it can no longer be checked out.
    Prune {
        /// The duration of time to keep versions of the dataset.
        older_than: Option<Duration>,
        /// Because they may be part of an in-progress transaction, files newer than 7 days old are not deleted by default.
        /// If you are sure that there are no in-progress transactions, then you can set this to True to delete all files older than `older_than`.
        ///
        /// **WARNING**: This should only be set to true if you can guarantee that no other process is
        /// currently working on this dataset.  Otherwise the dataset could be put into a corrupted state.
        delete_unverified: Option<bool>,
        /// Tagged versions are never removed, regardless of their age.
        ///
        /// If true, an error will be returned if there are any old versions that are still tagged.
        /// If false or `None` (the default), tagged versions are silently retained and all other
        /// old versions are removed.
        error_if_tagged_old_versions: Option<bool>,
    },
    /// Optimize the indices
    ///
    /// This operation optimizes all indices in the table.  When new data is added to LanceDb
    /// it is not added to the indices.  However, it can still turn up in searches because the search
    /// function will scan both the indexed data and the unindexed data in parallel.  Over time, the
    /// unindexed data can become large enough that the search performance is slow.  This operation
    /// will add the unindexed data to the indices without rerunning the full index creation process.
    ///
    /// Optimizing an index is faster than re-training the index but it does not typically adjust the
    /// underlying model relied upon by the index.  This can eventually lead to poor search accuracy
    /// and so users may still want to occasionally retrain the index after adding a large amount of
    /// data.
    ///
    /// For example, when using IVF, an index will create clusters.  Optimizing an index assigns unindexed
    /// data to the existing clusters, but it does not move the clusters or create new clusters.
    Index(IndexOptimizeOptions),
}

impl OptimizeAction {
    /// Normalize an Enterprise data-rewrite request before any submission.
    #[allow(deprecated)]
    pub(crate) fn into_remote_overrides(self) -> Result<OptimizeOverrides> {
        match self {
            Self::AllWithOptions(options)
            | Self::RewriteData { options, remap_options: None } => options.try_into(),
            Self::Compact { options, remap_options: None } => {
                let target = options.target_rows_per_fragment;
                let mut overrides = OptimizeOverrides::try_from(options)?;
                // Only the deprecated full-snapshot conversion treats the old
                // default target as omitted; preferred options retain presence.
                if target == LanceOptimizeOptions::default().target_rows_per_fragment {
                    overrides.target_rows_per_fragment = None;
                }
                Ok(overrides)
            }
            Self::RewriteData { remap_options: Some(_), .. }
            | Self::Compact { remap_options: Some(_), .. } => Err(Error::NotSupported {
                message: "a custom index remap strategy is not supported in LanceDB Enterprise; omit remap_options to use the server's managed, index-safe compaction path.".into(),
            }),
            _ => Err(Error::NotSupported {
                message: "Optimize jobs support only data rewriting in LanceDB Enterprise; version pruning and separate index optimization are unsupported. Use OptimizeOptions or OptimizeAction::RewriteData.".into(),
            }),
        }
    }
}

/// Statistics about the optimization.
#[derive(Debug, Default)]
pub struct OptimizeStats {
    /// Stats of the file compaction.
    pub compaction: Option<CompactionMetrics>,

    /// Stats of the version pruning
    pub prune: Option<RemovalStats>,
}

/// Internal implementation of optimize_indices
///
/// This logic was moved from NativeTable to keep table.rs clean.
pub(crate) async fn optimize_indices(
    table: &NativeTable,
    options: &IndexOptimizeOptions,
) -> Result<()> {
    info!("LanceDB: optimizing indices: {:?}", options);
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.optimize_indices(options).await?;
    table.dataset.update(dataset);
    Ok(())
}

/// Remove old versions of the dataset from disk.
///
/// # Arguments
/// * `older_than` - The duration of time to keep versions of the dataset.
/// * `delete_unverified` - Because they may be part of an in-progress
///   transaction, files newer than 7 days old are not deleted by default.
///   If you are sure that there are no in-progress transactions, then you
///   can set this to True to delete all files older than `older_than`.
///
///   **WARNING**: This should only be set to true if you can guarantee that
///   no other process is currently working on this dataset.  Otherwise the
///   dataset could be put into a corrupted state.
///
/// * `error_if_tagged_old_versions` - Tagged versions are always retained.  If
///   this is true, an error is returned when a tagged version is older than the
///   cutoff.  Defaults to false (the tagged versions are retained silently),
///   unlike Lance's own default, so that `optimize` does not fail (and skip the
///   steps after pruning) once a tagged version ages past the cutoff.
///
/// This calls into [lance::dataset::Dataset::cleanup_old_versions] and
/// returns the result.
pub(crate) async fn cleanup_old_versions(
    table: &NativeTable,
    older_than: Duration,
    delete_unverified: Option<bool>,
    error_if_tagged_old_versions: Option<bool>,
) -> Result<RemovalStats> {
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;
    let stats = dataset
        .cleanup_old_versions(
            older_than,
            delete_unverified,
            Some(error_if_tagged_old_versions.unwrap_or(false)),
        )
        .await?;
    // Computed-column signature sidecars live outside lance's directories;
    // drop the ones the surviving versions no longer reference.
    let removed =
        super::freshness::prune_sidecars(&dataset, delete_unverified.unwrap_or(false)).await?;
    if removed > 0 {
        log::debug!("removed {removed} unreferenced computed-column signature sidecars");
    }
    Ok(stats)
}

/// Remove dataset versions committed before an absolute timestamp.
pub(crate) async fn cleanup_old_versions_before(
    table: &NativeTable,
    before_timestamp: DateTime<Utc>,
    delete_unverified: Option<bool>,
    error_if_tagged_old_versions: Option<bool>,
) -> Result<RemovalStats> {
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;
    let mut policy = CleanupPolicyBuilder::default().before_timestamp(before_timestamp);
    if let Some(delete_unverified) = delete_unverified {
        policy = policy.delete_unverified(delete_unverified);
    }
    // Tagged versions are retained rather than raising an error unless asked.
    policy = policy.error_if_tagged_old_versions(error_if_tagged_old_versions.unwrap_or(false));
    let stats = dataset.cleanup_with_policy(policy.build()).await?;
    // Computed-column signature sidecars live outside lance's directories;
    // drop the ones the surviving versions no longer reference.
    let removed =
        super::freshness::prune_sidecars(&dataset, delete_unverified.unwrap_or(false)).await?;
    if removed > 0 {
        log::debug!("removed {removed} unreferenced computed-column signature sidecars");
    }
    Ok(stats)
}

/// Compact files in the dataset.
///
/// This can be run after making several small appends to optimize the table
/// for faster reads.
///
/// This calls into [lance::dataset::optimize::compact_files_with_planner].
pub(crate) enum RewriteOptions {
    Policy(OptimizeOptions),
    Resolved(LanceOptimizeOptions),
}
impl From<OptimizeOptions> for RewriteOptions {
    fn from(options: OptimizeOptions) -> Self {
        Self::Policy(options)
    }
}
impl From<LanceOptimizeOptions> for RewriteOptions {
    fn from(options: LanceOptimizeOptions) -> Self {
        Self::Resolved(options)
    }
}

pub(crate) async fn compact_files_impl(
    table: &NativeTable,
    options: impl Into<RewriteOptions>,
    remap_options: Option<Arc<dyn IndexRemapperOptions>>,
) -> Result<CompactionMetrics> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    let options = match options.into() {
        RewriteOptions::Policy(options) => options.resolve(&dataset.manifest().config)?,
        RewriteOptions::Resolved(options) => options,
    };
    tracing::info!(target: TRACE_DATASET_EVENTS, event=DATASET_COMPACTING_EVENT, uri = dataset.uri());
    let plan = validate_sq_index_remapping(&dataset, &options, remap_options.is_some()).await?;
    let planner = PrecomputedCompactionPlanner::new(plan);
    let metrics = compact_files_with_planner(&mut dataset, remap_options, &planner).await?;
    table.dataset.update(dataset);
    Ok(metrics)
}

/// Execute the optimize operation on the table.
///
/// This is the main entry point for all optimization operations.
#[allow(deprecated)]
pub(crate) async fn execute_optimize(
    table: &NativeTable,
    action: OptimizeAction,
) -> Result<OptimizeStats> {
    let mut stats = OptimizeStats {
        compaction: None,
        prune: None,
    };
    let action = match action {
        OptimizeAction::All => OptimizeAction::AllWithOptions(OptimizeOptions::default()),
        action => action,
    };
    match action {
        OptimizeAction::All => unreachable!(),
        OptimizeAction::AllWithOptions(options) => {
            // Call helper functions directly to avoid async recursion issues
            stats.compaction = Some(compact_files_impl(table, options, None).await?);
            stats.prune = Some(
                cleanup_old_versions(
                    table,
                    Duration::try_days(7).expect("valid delta"),
                    None,
                    None,
                )
                .await?,
            );
            optimize_indices(table, &IndexOptimizeOptions::default()).await?;
        }
        OptimizeAction::RewriteData {
            options,
            remap_options,
        } => {
            stats.compaction = Some(compact_files_impl(table, options, remap_options).await?);
        }
        OptimizeAction::Compact {
            options,
            remap_options,
        } => {
            stats.compaction = Some(compact_files_impl(table, options, remap_options).await?);
        }
        OptimizeAction::Prune {
            older_than,
            delete_unverified,
            error_if_tagged_old_versions,
        } => {
            stats.prune = Some(
                cleanup_old_versions(
                    table,
                    older_than.unwrap_or(Duration::try_days(7).expect("valid delta")),
                    delete_unverified,
                    error_if_tagged_old_versions,
                )
                .await?,
            );
        }
        OptimizeAction::Index(options) => {
            optimize_indices(table, &options).await?;
        }
    }
    Ok(stats)
}

// Preserve coverage of released full-snapshot Compact requests.
#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::{oversized_sq_partition, segment_touches_fragments, sq_vector_dimension};
    use arrow::compute::concat_batches;
    use arrow_array::{
        Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray,
        cast::AsArray,
        types::{Float32Type, Int32Type},
    };
    use arrow_schema::{DataType, Field, Schema};
    use lance::index::DatasetIndexExt;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;
    use rstest::rstest;
    use std::{collections::HashSet, sync::Arc};

    use crate::connect;
    use crate::database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS;
    use crate::index::{
        Index,
        scalar::BTreeIndexBuilder,
        vector::{IvfHnswSqIndexBuilder, IvfRqIndexBuilder},
    };
    use crate::query::{ExecutableQuery, QueryBase};
    use crate::table::{OptimizeAction, OptimizeStats};
    use futures::TryStreamExt;
    use lance::dataset::optimize::CompactionOptions as LanceOptimizeOptions;

    /// Regression test for https://github.com/lancedb/lancedb/issues/2866.
    #[test]
    fn test_detect_oversized_sq_partition() {
        const DIMENSION: u64 = 4095;
        let safe_size = u32::MAX as u64 / DIMENSION;

        assert_eq!(
            oversized_sq_partition([safe_size, safe_size + 1], DIMENSION),
            Some(safe_size + 1)
        );
        assert_eq!(
            oversized_sq_partition([safe_size / 2, safe_size / 2 + 1], DIMENSION),
            None
        );
    }

    #[test]
    fn test_segment_touches_fragments_includes_unknown_coverage() {
        let affected_fragments = HashSet::from([7]);

        assert!(segment_touches_fragments(
            Some([7_u32]),
            &affected_fragments
        ));
        assert!(!segment_touches_fragments(
            Some([8_u32]),
            &affected_fragments
        ));
        assert!(segment_touches_fragments(
            None::<[u32; 0]>,
            &affected_fragments
        ));
    }

    #[test]
    fn test_sq_vector_dimension_includes_multivectors() {
        const DIMENSION: i32 = 4095;
        let vector = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            DIMENSION,
        );
        let multivector = DataType::List(Arc::new(Field::new("item", vector.clone(), false)));

        assert_eq!(sq_vector_dimension(&vector), Some(DIMENSION));
        assert_eq!(sq_vector_dimension(&multivector), Some(DIMENSION));
        assert_eq!(sq_vector_dimension(&DataType::Float32), None);
    }

    #[tokio::test]
    async fn test_compact_legacy_vector_index_with_unknown_fragment_coverage() {
        use lance::Dataset;
        use lance::index::vector::{IndexFileVersion, VectorIndexParams};
        use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
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
            .create_table("test_legacy_vector_compact", batch.clone())
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
        let original = segments.pop().unwrap();
        let field_path = dataset.schema().field_path(original.fields[0]).unwrap();
        let vector_index = dataset
            .open_vector_index(
                &field_path,
                &original.uuid,
                &lance_index::metrics::NoOpMetricsCollector,
            )
            .await
            .unwrap();
        assert_eq!(vector_index.index_type(), IndexType::IvfPq);
        assert_eq!(
            vector_index.statistics().unwrap()["index_file_version"],
            "Legacy"
        );
        let mut legacy = original.clone();
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
        let legacy_dataset = Dataset::open(dataset.uri()).await.unwrap();
        table.dataset().unwrap().update(legacy_dataset);

        let dataset = table.dataset().unwrap().get().await.unwrap();
        let description_error = dataset
            .describe_indices(None)
            .await
            .err()
            .expect("legacy coverage should be unknown to index descriptions");
        assert!(
            description_error
                .to_string()
                .contains("Fragment bitmap is required")
        );

        let stats = table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions {
                    target_rows_per_fragment: 1_000,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();
        assert!(stats.compaction.unwrap().fragments_removed > 0);
        assert_eq!(table.count_rows(None).await.unwrap(), ROWS as usize * 3);
    }

    #[rstest]
    #[case::lance_options(false)]
    #[case::optional_overrides(true)]
    #[tokio::test]
    async fn test_optimize_compact_simple(#[case] optional_overrides: bool) {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with initial data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_compact", batch)
            .execute()
            .await
            .unwrap();

        // Add more data to create multiple fragments
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 100 + 100)..((i + 1) * 100 + 100),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Verify we have multiple fragments before compaction
        let initial_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(initial_row_count, 600);

        // Both Optimize entry points rewrite only data and preserve history.
        let stats = if optional_overrides {
            table
                .optimize(OptimizeAction::RewriteData {
                    options: super::OptimizeOptions {
                        target_rows_per_fragment: Some(1000),
                        ..Default::default()
                    },
                    remap_options: None,
                })
                .await
        } else {
            table
                .optimize(LanceOptimizeOptions {
                    target_rows_per_fragment: 1000,
                    ..Default::default()
                })
                .await
        }
        .unwrap();
        assert!(stats.prune.is_none());

        // Verify compaction occurred
        assert!(stats.compaction.is_some());
        let compaction_metrics = stats.compaction.unwrap();
        assert!(compaction_metrics.fragments_removed > 0);

        // Verify data integrity after compaction
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 600);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 600);

        // Verify the values are as expected
        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..600).collect();
        assert_eq!(all_values, expected);
        let error = table
            .optimize_async(LanceOptimizeOptions::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("LanceDB Enterprise"));
        table.checkout(1).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_parallel_compaction_reserves_fragment_ids_once() {
        let conn = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..10))])
                .unwrap();

        let table = conn
            .create_table("test_parallel_compaction", batch.clone())
            .execute()
            .await
            .unwrap();

        // Create 64 fragments. With a 20-row target, compaction plans 32 tasks,
        // which is more than the commit retry limit that used to be exhausted
        // when each parallel task reserved fragment IDs independently.
        for _ in 1..64 {
            table.add(batch.clone()).execute().await.unwrap();
        }

        // Legacy row IDs require fragment IDs before an index can be remapped.
        assert!(
            !table
                .as_native()
                .unwrap()
                .manifest()
                .await
                .unwrap()
                .uses_stable_row_ids()
        );
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let version_before = table.version().await.unwrap();
        let stats = table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions {
                    target_rows_per_fragment: 20,
                    num_threads: Some(64),
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap()
            .compaction
            .unwrap();

        assert_eq!(stats.fragments_removed, 64);
        assert_eq!(stats.fragments_added, 32);
        assert_eq!(table.count_rows(None).await.unwrap(), 640);
        assert_eq!(
            table.version().await.unwrap(),
            version_before + 2,
            "parallel compaction should use one fragment reservation commit and one rewrite commit"
        );
    }

    #[tokio::test]
    async fn test_compact_with_concurrent_add() {
        const NUM_FRAGMENTS: usize = 5;
        const ROWS_PER_FRAGMENT: i32 = 300;

        let tmpdir = tempfile::tempdir().unwrap();
        let conn = connect(tmpdir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..ROWS_PER_FRAGMENT))],
        )
        .unwrap();

        let table = conn
            .create_table("test_concurrent_compact", batch.clone())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
        for _ in 0..NUM_FRAGMENTS {
            table.add(batch.clone()).execute().await.unwrap();
        }

        // Use separate handles so the two writes actually overlap, as they can
        // when different Node connections operate on the same S3 table.
        let compact_table = conn
            .open_table("test_concurrent_compact")
            .execute()
            .await
            .unwrap();
        let append_table = conn
            .open_table("test_concurrent_compact")
            .execute()
            .await
            .unwrap();
        let compact_task = tokio::spawn(async move {
            compact_table
                .optimize(OptimizeAction::Compact {
                    options: LanceOptimizeOptions {
                        target_rows_per_fragment: 1_000,
                        ..Default::default()
                    },
                    remap_options: None,
                })
                .await
        });
        tokio::task::yield_now().await;
        for _ in 0..NUM_FRAGMENTS {
            append_table.add(batch.clone()).execute().await.unwrap();
        }
        compact_task.await.unwrap().unwrap();

        let table = conn
            .open_table("test_concurrent_compact")
            .execute()
            .await
            .unwrap();
        let dataset = table.dataset().unwrap().get().await.unwrap();
        let fragment_ids = dataset
            .get_fragments()
            .iter()
            .map(|fragment| fragment.id())
            .collect::<Vec<_>>();
        assert!(fragment_ids.windows(2).all(|ids| ids[0] < ids[1]));

        // A second compaction exposed the original out-of-order row-id bug.
        table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions {
                    target_rows_per_fragment: 1_000,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();
        assert_eq!(
            table.count_rows(None).await.unwrap(),
            ROWS_PER_FRAGMENT as usize * (NUM_FRAGMENTS * 2 + 1)
        );
    }

    #[tokio::test]
    async fn test_optimize_prune_versions() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_prune", batch)
            .execute()
            .await
            .unwrap();

        // Make several modifications to create versions
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 10 + 10)..((i + 1) * 10 + 10),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Verify multiple versions exist
        let versions = table.list_versions().await.unwrap();
        assert!(versions.len() > 1);

        // Run prune with a very old cutoff (won't delete recent versions)
        let stats = table
            .optimize(OptimizeAction::Prune {
                older_than: Some(chrono::Duration::try_days(0).unwrap()),
                delete_unverified: Some(true),
                error_if_tagged_old_versions: None,
            })
            .await
            .unwrap();

        // Prune-only operation should not have compaction stats
        assert!(stats.compaction.is_none());

        // Verify prune stats
        let prune_stats = stats.prune.unwrap();
        assert!(prune_stats.bytes_removed > 0);
        assert_eq!(prune_stats.old_versions, 5);

        // Verify data is still intact
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 60);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..60).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_optimize_prune_retains_tagged_versions() {
        let tmpdir = tempfile::tempdir().unwrap();
        let conn = connect(tmpdir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let make_batch = |start: i32| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(start..start + 10))],
            )
            .unwrap()
        };

        let table = conn
            .create_table("test_prune_tagged", make_batch(0))
            .execute()
            .await
            .unwrap();
        // Tag the first version, then create more versions after it.
        table.tags().await.unwrap().create("keep", 1).await.unwrap();
        for i in 1..4 {
            table.add(make_batch(i * 10)).execute().await.unwrap();
        }
        assert_eq!(table.list_versions().await.unwrap().len(), 4);

        // Explicitly asking for an error still errors, and leaves everything in place.
        let err = table
            .optimize(OptimizeAction::Prune {
                older_than: Some(chrono::Duration::try_days(0).unwrap()),
                delete_unverified: Some(true),
                error_if_tagged_old_versions: Some(true),
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("tagged version"), "{err}");
        assert_eq!(table.list_versions().await.unwrap().len(), 4);

        // By default, tagged versions are retained and the other old versions are removed.
        let stats = table
            .optimize(OptimizeAction::Prune {
                older_than: Some(chrono::Duration::try_days(0).unwrap()),
                delete_unverified: Some(true),
                error_if_tagged_old_versions: None,
            })
            .await
            .unwrap();
        assert_eq!(stats.prune.unwrap().old_versions, 2);
        let mut versions = table
            .list_versions()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.version)
            .collect::<Vec<_>>();
        versions.sort();
        assert_eq!(versions, vec![1, 4]);

        // Same through the absolute-cutoff entry point used by the Node binding.
        table.add(make_batch(40)).execute().await.unwrap();
        let stats = table
            .optimize_prune_before(chrono::Utc::now(), Some(true), None)
            .await
            .unwrap();
        assert_eq!(stats.prune.unwrap().old_versions, 1);

        // The tagged version is still readable, including its data files.
        assert_eq!(table.count_rows(None).await.unwrap(), 50);
        table.checkout_tag("keep").await.unwrap();
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 10);
    }

    #[tokio::test]
    async fn test_optimize_index() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_index_optimize", batch)
            .execute()
            .await
            .unwrap();

        // Create an index
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        // Add more data (unindexed)
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(100..200))],
        )
        .unwrap();
        table.add(batch).execute().await.unwrap();

        // Verify index stats before optimization
        let indices = table.list_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        let index_name = indices[0].name.clone();
        let stats_before = table.index_stats(&index_name).await.unwrap().unwrap();
        assert_eq!(stats_before.num_indexed_rows, 100);
        assert_eq!(stats_before.num_unindexed_rows, 100);

        // Run index optimization
        let stats = table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
            .unwrap();

        // For index optimization, compaction and prune stats should be None
        assert!(stats.compaction.is_none());
        assert!(stats.prune.is_none());

        // Verify index stats after optimization
        let stats_after = table.index_stats(&index_name).await.unwrap().unwrap();
        assert_eq!(stats_after.num_indexed_rows, 200);
        assert_eq!(stats_after.num_unindexed_rows, 0);
        assert!(stats_after.num_indices.is_some());

        // Verify data integrity
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 200);
    }

    #[tokio::test]
    async fn test_optimize_vector_index_after_delete_with_stable_row_ids() {
        const NUM_ROWS: i32 = 400;
        const DIMENSION: i32 = 32;

        let conn = connect("memory://").execute().await.unwrap();
        let vectors = FixedSizeListArray::try_new_from_values(
            Float32Array::from_iter_values((0..NUM_ROWS).flat_map(|id| {
                (0..DIMENSION).map(move |offset| ((id as f32 * 0.1) + (offset as f32 * 0.3)).sin())
            })),
            DIMENSION,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("vector", vectors.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..NUM_ROWS)),
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("test_vector_index_optimize_after_delete", batch)
            .storage_option(OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS, "true")
            .execute()
            .await
            .unwrap();

        table
            .create_index(
                &["vector"],
                Index::IvfRq(IvfRqIndexBuilder::default().num_partitions(4)),
            )
            .execute()
            .await
            .unwrap();
        table.delete("id % 3 = 0").await.unwrap();

        // Regression test for #3330: deleted stable row IDs used to become
        // misaligned with row addresses while joining small IVF partitions.
        table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 266);
    }

    #[tokio::test]
    async fn test_optimize_all() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_optimize_all", batch)
            .execute()
            .await
            .unwrap();

        // Add more data
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 100 + 100)..((i + 1) * 100 + 100),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Run all optimizations
        let stats = table.optimize(OptimizeAction::All).await.unwrap();

        // Verify stats from both compaction and prune
        assert!(stats.compaction.is_some());
        assert!(stats.prune.is_some());

        // Verify data integrity
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 400);

        // Verify data content
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..400).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_optimize_all_with_hnsw_index() {
        let conn = connect("memory://").execute().await.unwrap();

        let dimension = 8;
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(item_field.clone(), dimension),
            false,
        )]));

        let make_batch = |offset: usize| {
            let values = Float32Array::from_iter_values(
                (offset * dimension as usize..(offset + 128) * dimension as usize)
                    .map(|value| value as f32),
            );
            let vectors =
                FixedSizeListArray::try_new(item_field.clone(), dimension, Arc::new(values), None)
                    .unwrap();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap()
        };

        let table = conn
            .create_table("test_hnsw_optimize", make_batch(0))
            .execute()
            .await
            .unwrap();
        for offset in [128, 256, 384] {
            table.add(make_batch(offset)).execute().await.unwrap();
        }

        table
            .create_index(
                &["vector"],
                Index::IvfHnswSq(IvfHnswSqIndexBuilder::default()),
            )
            .execute()
            .await
            .unwrap();

        let stats = table.optimize(OptimizeAction::All).await.unwrap();
        assert!(stats.compaction.unwrap().fragments_removed > 0);

        let indices = table.list_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0].index_type, crate::index::IndexType::IvfHnswSq);

        let index_stats = table.index_stats(&indices[0].name).await.unwrap().unwrap();
        assert_eq!(index_stats.num_indexed_rows, 512);
        assert_eq!(index_stats.num_unindexed_rows, 0);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
    }

    #[tokio::test]
    async fn test_optimize_default_action() {
        // Verify that default action is All
        let action: OptimizeAction = Default::default();
        assert!(matches!(action, OptimizeAction::All));
    }

    #[tokio::test]
    async fn test_optimize_stats_default() {
        // Verify OptimizeStats default values
        let stats: OptimizeStats = Default::default();
        assert!(stats.compaction.is_none());
        assert!(stats.prune.is_none());
    }

    #[tokio::test]
    async fn test_compact_with_deferred_index_remap() {
        // Smoke test: verifies compaction with deferred index remap doesn't error.
        // We don't currently assert that remap is actually deferred.
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_deferred_remap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Add more data
        table.add(batch).execute().await.unwrap();

        // Create an index
        table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        // Run compaction with deferred index remap
        let stats = table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions {
                    target_rows_per_fragment: 2000,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        assert!(stats.compaction.is_some());

        // Verify data integrity after compaction
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 200);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();

        // Since we added the same data twice (0..100 twice), we expect 200 values
        // with values 0-99 appearing twice
        let mut expected: Vec<i32> = (0..100).chain(0..100).collect();
        expected.sort();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_refined_rq_query_after_deferred_compaction() {
        const ROWS: i32 = 128;
        const DIMENSION: i32 = 32;
        let conn = connect("memory://").execute().await.unwrap();
        let vectors = FixedSizeListArray::try_new_from_values(
            Float32Array::from_iter_values((0..ROWS).flat_map(|id| {
                (0..DIMENSION).map(move |offset| (id as f32 * 0.1 + offset as f32 * 0.3).sin())
            })),
            DIMENSION,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("vector", vectors.data_type().clone(), false),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(0..ROWS)),
                Arc::new(vectors),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("refined_rq_compaction", batch.slice(0, 64))
            .storage_option(OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS, "false")
            .execute()
            .await
            .unwrap();
        table.add(batch.slice(64, 64)).execute().await.unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfRq(
                    IvfRqIndexBuilder::default()
                        .num_partitions(4)
                        .num_bits(7)
                        .max_iterations(5),
                ),
            )
            .name("rq".to_string())
            .execute()
            .await
            .unwrap();
        table.delete("id % 7 = 0").await.unwrap();
        let dataset = table.dataset().unwrap().get().await.unwrap();
        assert_eq!(dataset.fragments().len(), 2);
        let original_index = dataset.load_indices_by_name("rq").await.unwrap()[0].uuid;

        let key = (0..DIMENSION)
            .map(|offset| (0.5 + offset as f32 * 0.3).sin())
            .collect::<Vec<_>>();
        let query = || {
            table
                .query()
                .only_if("id % 3 != 0")
                .nearest_to(key.as_slice())
                .unwrap()
                .nprobes(4)
                .unwrap()
                .limit(8)
        };
        let before = query()
            .fast_search()
            .refine_factor(4)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let before = concat_batches(&before[0].schema(), &before).unwrap();

        let stats = table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions {
                    target_rows_per_fragment: 256,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();
        assert_eq!(stats.compaction.unwrap().fragments_removed, 2);
        let dataset = table.dataset().unwrap().get().await.unwrap();
        assert_eq!(dataset.fragments().len(), 1);
        assert_eq!(
            dataset.load_indices_by_name("rq").await.unwrap()[0].uuid,
            original_index
        );
        assert!(
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|index| index.name == FRAG_REUSE_INDEX_NAME)
        );

        let refined = query().fast_search().refine_factor(4);
        let plan = refined.explain_plan(false).await.unwrap();
        assert!(plan.contains("ANNIvf"), "{plan}");
        let after = refined
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let after = concat_batches(&after[0].schema(), &after).unwrap();
        assert_eq!(after.num_rows(), 8);
        assert_eq!(after["id"].as_ref(), before["id"].as_ref());
        assert_eq!(after["_distance"].as_ref(), before["_distance"].as_ref());
        let found = after["id"].as_primitive::<Int32Type>().values();
        assert!(found.iter().all(|id| id % 7 != 0 && id % 3 != 0));
        assert!(after["_distance"].as_primitive::<Float32Type>().values()[0] < 1e-5);

        let exact = query()
            .bypass_vector_index()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let exact = concat_batches(&exact[0].schema(), &exact).unwrap();
        let truth = exact["id"].as_primitive::<Int32Type>().values();
        let recall = found.iter().filter(|id| truth.contains(id)).count() as f64 / 8.0;
        assert!(recall >= 0.5, "recall={recall}");
    }

    #[tokio::test]
    async fn test_compaction_preserves_schema() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(StringArray::from(
                    (0..10).map(|i| format!("name_{}", i)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let original_schema = batch.schema();

        let table = conn
            .create_table("test_schema_preserved", batch.clone())
            .execute()
            .await
            .unwrap();

        // Add more data
        table.add(batch).execute().await.unwrap();

        // Run compaction
        table
            .optimize(OptimizeAction::Compact {
                options: LanceOptimizeOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();

        // Verify schema is preserved
        let current_schema = table.schema().await.unwrap();
        assert_eq!(current_schema, original_schema);

        // Verify data is intact and correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 20);
    }

    #[tokio::test]
    async fn test_optimize_empty_table() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table and delete all data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_empty_optimize", batch)
            .execute()
            .await
            .unwrap();

        // Delete all rows
        table.delete("true").await.unwrap();

        // Verify table is empty
        assert_eq!(table.count_rows(None).await.unwrap(), 0);

        // Optimize should work on empty table
        let stats = table.optimize(OptimizeAction::All).await.unwrap();
        assert!(stats.compaction.is_some());
        assert!(stats.prune.is_some());

        // Verify table is still empty but schema is preserved
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
        let current_schema = table.schema().await.unwrap();
        assert_eq!(current_schema, schema);
    }

    #[rstest]
    #[case::rewrite_data(OptimizeAction::RewriteData { options: crate::table::optimize::OptimizeOptions::default(), remap_options: None })]
    #[case::all(OptimizeAction::All)]
    #[case::compact(OptimizeAction::Compact {
        options: LanceOptimizeOptions::default(),
        remap_options: None,
    })]
    #[case::prune(OptimizeAction::Prune {
        older_than: Some(chrono::Duration::try_days(0).unwrap()),
        delete_unverified: Some(true),
        error_if_tagged_old_versions: None,
    })]
    #[case::index(OptimizeAction::Index(Default::default()))]
    #[tokio::test]
    async fn test_optimize_fails_on_checked_out_table(#[case] action: OptimizeAction) {
        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_checkout_optimize", batch.clone())
            .execute()
            .await
            .unwrap();

        table.add(batch).execute().await.unwrap();

        table.checkout(1).await.unwrap();

        let result = table.optimize(action).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot be modified when a specific version is checked out"),
            "Expected error message about checked out table, got: {}",
            err_msg
        );
    }
}

#[cfg(test)]
mod policy_tests {
    use super::{LanceOptimizeOptions, OptimizeOptions};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn optimize_policy_preserves_omissions_explicit_defaults_and_zero() {
        let config = HashMap::from([
            (
                "lance.compaction.target_rows_per_fragment".into(),
                "10".into(),
            ),
            (
                "lance.compaction.materialize_deletions".into(),
                "false".into(),
            ),
            ("lance.compaction.io_buffer_size".into(), "4096".into()),
        ]);
        let inherited = OptimizeOptions::default().resolve(&config).unwrap();
        assert_eq!(inherited.target_rows_per_fragment, 10);
        assert!(!inherited.materialize_deletions);
        assert_eq!(inherited.io_buffer_size, Some(4096));
        let explicit = OptimizeOptions {
            target_rows_per_fragment: Some(
                LanceOptimizeOptions::default().target_rows_per_fragment,
            ),
            materialize_deletions: Some(true),
            io_buffer_size: Some(0),
            ..Default::default()
        }
        .resolve(&config)
        .unwrap();
        assert_eq!(
            explicit.target_rows_per_fragment,
            LanceOptimizeOptions::default().target_rows_per_fragment
        );
        assert!(explicit.materialize_deletions);
        assert_eq!(explicit.io_buffer_size, Some(0));
        let cleared = OptimizeOptions {
            cleared_fields: HashSet::from(["io_buffer_size".into()]),
            ..Default::default()
        }
        .resolve(&config)
        .unwrap();
        assert_eq!(cleared.io_buffer_size, None);
    }

    #[test]
    fn optimize_rejects_invalid_table_policy() {
        let config = HashMap::from([(
            "lance.compaction.target_rows_per_fragment".into(),
            "not-an-integer".into(),
        )]);
        assert!(OptimizeOptions::default().resolve(&config).is_err());
    }
}
