// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Table maintenance for optimizing table layout.
//!
//! As a table is updated, its layout can become suboptimal. For example, if
//! a series of small streaming appends are performed, eventually there will be
//! a large number of small files. This imposes an overhead to track the large
//! number of files and for very small files can make it harder to read data
//! efficiently. In this case, files can be compacted into fewer larger files.
//!
//! To compact files in a table, use the [compact_files] method. This currently
//! can compact in two cases:
//!
//! 1. If a fragment has fewer rows than the target number of rows per fragment.
//!    The fragment must also have neighbors that are also candidates for
//!    compaction.
//! 2. If a fragment has a higher percentage of deleted rows than the provided
//!    threshold.
//!
//! In addition to the rules above there may be restrictions due to indexes.
//! When a fragment is compacted its row ids change and any index that contained
//! that fragment will be remapped.  However, we cannot combine indexed fragments
//! with unindexed fragments.
//!
//! ```rust
//! # use std::sync::Arc;
//! # use tokio::runtime::Runtime;
//! # use arrow_array::{RecordBatch, RecordBatchIterator, Int64Array};
//! # use arrow_schema::{Schema, Field, DataType};
//! use lance::{dataset::WriteParams, Dataset, dataset::optimize::compact_files};
//! // Remapping indices is ignored in this example.
//! use lance::dataset::optimize::IgnoreRemap;
//!
//! # let mut rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! #
//! # let test_dir = lance_core::utils::tempfile::TempStrDir::default();
//! # let uri = test_dir.to_string();
//! let schema = Arc::new(Schema::new(vec![Field::new("test", DataType::Int64, false)]));
//! let data = RecordBatch::try_new(
//!     schema.clone(),
//!     vec![Arc::new(Int64Array::from_iter_values(0..10_000))]
//! ).unwrap();
//! let reader = RecordBatchIterator::new(vec![Ok(data)], schema);
//!
//! // Write 100 small files
//! let write_params = WriteParams { max_rows_per_file: 100, ..Default::default()};
//! let mut dataset = Dataset::write(reader, &uri, Some(write_params)).await.unwrap();
//! assert_eq!(dataset.get_fragments().len(), 100);
//!
//! // Use compact_files() to consolidate the data to 1 fragment
//! let metrics = compact_files(&mut dataset, Default::default(), None).await.unwrap();
//! assert_eq!(metrics.fragments_removed, 100);
//! assert_eq!(metrics.fragments_added, 1);
//! assert_eq!(dataset.get_fragments().len(), 1);
//! # })
//! ```
//!
//! ## Distributed execution
//!
//! The [compact_files] method internally can use multiple threads, but
//! sometimes you might want to run it across multiple machines. To do this,
//! use the task API.
//!
//! ```text
//!                                      ┌──► CompactionTask.execute() ─► RewriteResult ─┐
//! plan_compaction() ─► CompactionPlan ─┼──► CompactionTask.execute() ─► RewriteResult ─┼─► commit_compaction()
//!                                      └──► CompactionTask.execute() ─► RewriteResult ─┘
//! ```
//!
//! [plan_compaction()] produces a [CompactionPlan]. This can be split into multiple
//! [CompactionTask], which can be serialized and sent to other machines. Calling
//! [CompactionTask::execute()] performs the compaction and returns a [RewriteResult].
//! The [RewriteResult] can be sent back to the coordinator, which can then call
//! [commit_compaction()] to commit the changes to the dataset.
//!
//! It's not required that all tasks are passed to [commit_compaction]. If some
//! didn't complete successfully or before a deadline, they can be omitted and
//! the successful tasks can be committed. You can also commit in batches if
//! you wish. As long as the tasks don't rewrite any of the same fragments,
//! they can be committed in any order.
use lance_core::utils::row_addr_remap::{GroupInput, RowAddrRemap};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::{AddAssign, Range};
use std::sync::Arc;

use super::fragment::FileFragment;
use super::index::{DatasetIndexRemapperOptions, load_indices_for_remapping};
use super::rowids::load_row_id_sequences;
use super::transaction::{
    Operation, RewriteGroup, RewrittenIndex, Transaction, TransactionBuilder,
};
use super::utils::make_rowid_capture_stream;
use super::{WriteMode, WriteParams, cleanup_data_fragments, write_fragments_internal};
use crate::Dataset;
use crate::Result;
use crate::dataset::utils::CapturedRowIds;
use crate::index::DatasetIndexExt;
use crate::io::commit::{DEFAULT_COMMIT_RETRY_TIMEOUT, commit_transaction, migrate_fragments};
use arrow::array::AsArray;
use arrow::datatypes::{UInt8Type, UInt32Type, UInt64Type};
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_array::builder::{LargeBinaryBuilder, PrimitiveBuilder, StringBuilder};
use arrow_buffer::NullBuffer;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use lance_core::Error;
use lance_core::datatypes::{
    BLOB_V2_LOGICAL_FIELDS, BLOB_V2_LOGICAL_TYPE, BlobHandling, BlobKind, BlobV2Layout,
};
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::utils::tracing::{DATASET_COMPACTING_EVENT, TRACE_DATASET_EVENTS};
use lance_index::frag_reuse::{FRAG_REUSE_INDEX_NAME, FragReuseGroup};
use lance_index::is_system_index;
use lance_table::format::{Fragment, RowIdMeta};
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

mod binary_copy;
pub mod remapping;

use crate::index::frag_reuse::build_new_frag_reuse_index;
use crate::io::deletion::read_dataset_deletion_file;
use binary_copy::rewrite_files_binary_copy;
pub use remapping::{IgnoreRemap, IndexRemapper, IndexRemapperOptions, RemappedIndex};

/// Controls how data is rewritten during compaction.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompactionMode {
    /// Decode and re-encode data (default).
    Reencode,
    /// Try binary copy if fragments are compatible, fall back to [`Reencode`](CompactionMode::Reencode) otherwise.
    TryBinaryCopy,
    /// Use binary copy or fail if fragments are not compatible.
    ForceBinaryCopy,
}

impl TryFrom<&str> for CompactionMode {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "reencode" => Ok(Self::Reencode),
            "try_binary_copy" => Ok(Self::TryBinaryCopy),
            "force_binary_copy" => Ok(Self::ForceBinaryCopy),
            _ => Err(Error::invalid_input(format!(
                "Invalid compaction mode \"{}\". Valid values: \"reencode\", \"try_binary_copy\", \"force_binary_copy\"",
                value
            ))),
        }
    }
}

/// Controls how the old-to-new row-address mapping is built when remapping
/// indices during compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IndexRemapMode {
    /// Store a compact remap and compute row-address mappings during lookup.
    ///
    /// Best for large compactions where peak memory is the constraint. Uses
    /// less memory, but each lookup does extra bitmap/range computation.
    Compact,
    /// Store the full row-address remap in memory for fast direct lookups.
    ///
    /// Best when the remap fits comfortably in memory and remap speed is the
    /// priority. Uses more peak memory because every rewritten/deleted row has
    /// a materialized mapping entry.
    #[default]
    Direct,
}

impl TryFrom<&str> for IndexRemapMode {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "compact" => Ok(Self::Compact),
            "direct" => Ok(Self::Direct),
            _ => Err(Error::invalid_input(format!(
                "Invalid index remap mode \"{}\". Valid values: \"compact\", \"direct\"",
                value
            ))),
        }
    }
}

/// Options to be passed to [compact_files].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionOptions {
    /// Target number of rows per file. Defaults to 1 million.
    ///
    /// This is used to determine which fragments need compaction, as any
    /// fragments that have fewer rows than this value will be candidates for
    /// compaction.
    pub target_rows_per_fragment: usize,
    /// Max number of rows per group
    ///
    /// This does not affect which fragments need compaction, but does affect
    /// how they are re-written if selected.
    pub max_rows_per_group: usize,
    /// Max number of bytes per file
    ///
    /// This does not affect which frgamnets need compaction, but does affect
    /// how they are re-written if selected.
    ///
    /// If not specified then the default (see [`WriteParams`]) will be used.
    pub max_bytes_per_file: Option<usize>,
    /// Whether to compact fragments with deletions so there are no deletions.
    /// Defaults to true.
    pub materialize_deletions: bool,
    /// The fraction of rows that need to be deleted in a fragment before
    /// materializing the deletions. Defaults to 10% (0.1). Setting to zero (or
    /// lower) will materialize deletions for all fragments with deletions.
    /// Setting above 1.0 will never materialize deletions.
    pub materialize_deletions_threshold: f32,
    /// The number of threads to use (how many compaction tasks to run in parallel).
    /// Defaults to the number of compute-intensive CPUs.  Not used when running
    /// tasks manually using [`plan_compaction`]
    pub num_threads: Option<usize>,
    /// The batch size to use when scanning the input fragments.  If not
    /// specified then the default (see
    /// [`crate::dataset::Scanner::batch_size`]) will be used.
    pub batch_size: Option<usize>,
    /// The number of bytes to allow to queue up in the I/O buffer when scanning
    /// the input fragments.  If not specified then the default (see
    /// [`crate::dataset::Scanner::io_buffer_size`]) will be used.
    ///
    /// Increasing this can avoid a deadlock that occurs when a single batch of
    /// data is larger than the I/O buffer size.
    pub io_buffer_size: Option<u64>,
    /// Whether to defer remapping indices during compaction. If true, indices will
    /// not be remapped during this compaction operation. Instead, the fragment reuse index
    /// is updated and will be used to perform remapping later.
    pub defer_index_remap: bool,
    /// How the old-to-new row-address mapping used to remap indices is built.
    /// Defaults to [`IndexRemapMode::Direct`].
    #[serde(default)]
    pub index_remap_mode: IndexRemapMode,
    /// The compaction mode to use. When set, this takes priority over the
    /// deprecated `enable_binary_copy` and `enable_binary_copy_force` fields.
    ///
    /// Defaults to `None` (falls back to legacy boolean fields).
    pub compaction_mode: Option<CompactionMode>,
    /// Deprecated: use `compaction_mode` instead.
    #[deprecated(note = "Use `compaction_mode` instead")]
    pub enable_binary_copy: bool,
    /// Deprecated: use `compaction_mode` instead.
    #[deprecated(note = "Use `compaction_mode` instead")]
    pub enable_binary_copy_force: bool,
    /// The batch size in bytes for reading during binary copy operations.
    /// Controls how much data is read at once when performing binary copy.
    /// Defaults to 16MB (16 * 1024 * 1024).
    pub binary_copy_read_batch_bytes: Option<usize>,
    /// Maximum number of source fragments to compact in a single run. When set,
    /// tasks are included in the plan until adding the next task would exceed
    /// this limit. This allows for incremental compaction (e.g., compact 20
    /// fragments at a time).
    /// Defaults to `None` (no limit, all eligible fragments are compacted).
    pub max_source_fragments: Option<usize>,
    /// Maximum number of data overlay files a fragment may carry before it is
    /// fully compacted. When set, any fragment with more than this many overlays
    /// is rewritten into a fresh fragment with its overlays (and deletions)
    /// materialized into the base data, dropping the fragment from any index
    /// left stale by those overlays.
    /// Defaults to `Some(10)`. Set to `Some(0)` to compact every fragment that
    /// carries any overlay, or `None` to disable the overlay-count trigger
    /// entirely.
    pub max_overlays_per_fragment: Option<usize>,
    /// Transaction properties to store with this commit.
    ///
    /// These key-value pairs are stored in the transaction file
    /// and can be read later to identify the source of the commit
    /// (e.g., job_id for tracking completed compaction jobs).
    #[serde(skip)]
    pub transaction_properties: Option<Arc<HashMap<String, String>>>,
}

#[allow(deprecated)]
impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            // Matching defaults for WriteParams
            target_rows_per_fragment: 1024 * 1024,
            max_rows_per_group: 1024,
            materialize_deletions: true,
            materialize_deletions_threshold: 0.1,
            num_threads: None,
            max_bytes_per_file: None,
            batch_size: None,
            io_buffer_size: None,
            defer_index_remap: false,
            index_remap_mode: IndexRemapMode::Direct,
            compaction_mode: None,
            enable_binary_copy: false,
            enable_binary_copy_force: false,
            binary_copy_read_batch_bytes: Some(16 * 1024 * 1024),
            max_source_fragments: None,
            max_overlays_per_fragment: Some(10),
            transaction_properties: None,
        }
    }
}

/// Config key prefix for compaction options stored in the dataset manifest.
pub const COMPACTION_CONFIG_PREFIX: &str = "lance.compaction.";

#[allow(deprecated)]
impl CompactionOptions {
    /// Create [`CompactionOptions`] by starting with defaults and applying any
    /// overrides found in the dataset manifest config.
    ///
    /// Config keys are prefixed with `lance.compaction.` and map to fields:
    /// - `lance.compaction.target_rows_per_fragment`
    /// - `lance.compaction.max_rows_per_group`
    /// - `lance.compaction.max_bytes_per_file`
    /// - `lance.compaction.materialize_deletions`
    /// - `lance.compaction.materialize_deletions_threshold`
    /// - `lance.compaction.defer_index_remap`
    /// - `lance.compaction.index_remap_mode`
    /// - `lance.compaction.batch_size`
    /// - `lance.compaction.io_buffer_size`
    /// - `lance.compaction.compaction_mode`
    /// - `lance.compaction.binary_copy_read_batch_bytes`
    /// - `lance.compaction.max_source_fragments`
    /// - `lance.compaction.max_overlays_per_fragment`
    pub fn from_dataset_config(config: &HashMap<String, String>) -> Result<Self> {
        let mut opts = Self::default();
        opts.apply_dataset_config(config)?;
        Ok(opts)
    }

    /// Apply overrides from the dataset manifest config to this options struct.
    ///
    /// Only fields with corresponding config keys are modified; other fields
    /// retain their current values.
    pub fn apply_dataset_config(&mut self, config: &HashMap<String, String>) -> Result<()> {
        for (key, value) in config {
            let Some(field) = key.strip_prefix(COMPACTION_CONFIG_PREFIX) else {
                continue;
            };
            match field {
                "target_rows_per_fragment" => {
                    self.target_rows_per_fragment = value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?;
                }
                "max_rows_per_group" => {
                    self.max_rows_per_group = value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?;
                }
                "max_bytes_per_file" => {
                    self.max_bytes_per_file = Some(value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?);
                }
                "materialize_deletions" => {
                    self.materialize_deletions = match value.to_lowercase().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => {
                            return Err(Error::invalid_input(format!(
                                "Invalid value for {}: '{}' (expected 'true' or 'false')",
                                key, value
                            )));
                        }
                    };
                }
                "materialize_deletions_threshold" => {
                    self.materialize_deletions_threshold = value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a float between 0.0 and 1.0)",
                            key, value
                        ))
                    })?;
                }
                "defer_index_remap" => {
                    self.defer_index_remap = match value.to_lowercase().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => {
                            return Err(Error::invalid_input(format!(
                                "Invalid value for {}: '{}' (expected 'true' or 'false')",
                                key, value
                            )));
                        }
                    };
                }
                "index_remap_mode" => {
                    self.index_remap_mode = IndexRemapMode::try_from(value.as_str())?;
                }
                "batch_size" => {
                    self.batch_size = Some(value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?);
                }
                "io_buffer_size" => {
                    self.io_buffer_size = Some(value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?);
                }
                "compaction_mode" => {
                    self.compaction_mode = Some(CompactionMode::try_from(value.as_str())?);
                }
                "binary_copy_read_batch_bytes" => {
                    self.binary_copy_read_batch_bytes = Some(value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?);
                }
                "max_source_fragments" => {
                    self.max_source_fragments = Some(value.parse().map_err(|_| {
                        Error::invalid_input(format!(
                            "Invalid value for {}: '{}' (expected a non-negative integer)",
                            key, value
                        ))
                    })?);
                }
                "max_overlays_per_fragment" => {
                    // The default is `Some(10)`, so an explicit "none" is the only
                    // way to disable the trigger through the manifest config.
                    self.max_overlays_per_fragment = match value.to_ascii_lowercase().as_str() {
                        "none" => None,
                        _ => Some(value.parse().map_err(|_| {
                            Error::invalid_input(format!(
                                "Invalid value for {}: '{}' (expected a non-negative integer or 'none')",
                                key, value
                            ))
                        })?),
                    };
                }
                _ => {
                    warn!("Ignoring unknown compaction config key: {}", key);
                }
            }
        }
        Ok(())
    }

    pub fn validate(&mut self) {
        // If threshold is 100%, same as turning off deletion materialization.
        if self.materialize_deletions && self.materialize_deletions_threshold >= 1.0 {
            self.materialize_deletions = false;
        }
    }

    /// Returns the effective [`CompactionMode`], preferring the new
    /// `compaction_mode` field and falling back to the deprecated boolean
    /// fields for backwards compatibility.
    pub fn compaction_mode(&self) -> CompactionMode {
        if let Some(mode) = self.compaction_mode {
            return mode;
        }
        // Fall back to deprecated booleans
        match (self.enable_binary_copy, self.enable_binary_copy_force) {
            (true, true) => CompactionMode::ForceBinaryCopy,
            (true, false) => CompactionMode::TryBinaryCopy,
            _ => CompactionMode::Reencode,
        }
    }

    /// Set transaction properties to store in the commit manifest.
    pub fn transaction_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.transaction_properties = Some(Arc::new(properties));
        self
    }
}

/// Determine if page-level binary copy can safely merge the provided fragments.
///
/// Preconditions checked in order:
/// - Compaction mode is not `Reencode`
/// - Dataset storage format is non-legacy
/// - Fragment list is non-empty
/// - All data files share identical Lance file versions
/// - No fragment has a deletion file
///   TODO: Need to support schema evolution case like add column and drop column
/// - All data files share identical schema mappings (`fields`, `column_indices`)
/// - Input data files must not contain extra global buffers (beyond schema / file descriptor)
async fn can_use_binary_copy(
    dataset: &Dataset,
    options: &CompactionOptions,
    fragments: &[Fragment],
) -> bool {
    can_use_binary_copy_impl(dataset, options, fragments)
        .await
        .unwrap_or_else(|err| {
            log::warn!("Binary copy disabled due to error: {}", err);
            false
        })
}

async fn can_use_binary_copy_impl(
    dataset: &Dataset,
    options: &CompactionOptions,
    fragments: &[Fragment],
) -> Result<bool> {
    use lance_file::reader::FileReader as LFReader;
    use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
    use lance_io::scheduler::{ScanScheduler, SchedulerConfig};

    if matches!(options.compaction_mode(), CompactionMode::Reencode) {
        log::debug!("Binary copy disabled: compaction mode is Reencode");
        return Ok(false);
    }

    let has_blob_columns = dataset
        .schema()
        .fields_pre_order()
        .any(|field| field.is_blob());
    if has_blob_columns {
        log::debug!("Binary copy disabled: dataset contains blob columns");
        return Ok(false);
    }

    let storage_ok = dataset
        .manifest
        .data_storage_format
        .lance_file_version()
        .map(|v| !matches!(v.resolve(), LanceFileVersion::Legacy))
        .unwrap_or(false);
    if !storage_ok {
        log::debug!("Binary copy disabled: dataset uses legacy storage format");
        return Ok(false);
    }

    if fragments.is_empty() {
        log::debug!("Binary copy disabled: no fragments to compact");
        return Ok(false);
    }

    let storage_file_version = dataset
        .manifest
        .data_storage_format
        .lance_file_version()?
        .resolve();

    if fragments[0].files.is_empty() {
        log::debug!(
            "Binary copy disabled: fragment {} has no data files",
            fragments[0].id
        );
        return Ok(false);
    }
    let ref_fields = &fragments[0].files[0].fields;
    let ref_cols = &fragments[0].files[0].column_indices;
    let mut is_same_version = true;

    for fragment in fragments {
        if fragment.deletion_file.is_some() {
            log::debug!(
                "Binary copy disabled: fragment {} has a deletion file",
                fragment.id
            );
            return Ok(false);
        }

        for data_file in &fragment.files {
            let version_ok = data_file
                .file_version()
                .is_ok_and(|v| v == ConcreteFileVersion::from(storage_file_version));

            if !version_ok {
                is_same_version = false;
            }
            if data_file.fields != *ref_fields || data_file.column_indices != *ref_cols {
                return Ok(false);
            }

            // check file global buffer
            let object_store = match data_file.base_id {
                Some(base_id) => dataset.object_store(Some(base_id)).await?,
                None => dataset.object_store.clone(),
            };
            let full_path = dataset
                .data_file_dir(data_file)?
                .clone()
                .join(data_file.path.as_str());
            let scan_scheduler = ScanScheduler::new(
                object_store.clone(),
                SchedulerConfig::max_bandwidth(&object_store),
            );
            let file_scheduler = scan_scheduler
                .open_file_with_priority(&full_path, 0, &data_file.file_size_bytes)
                .await?;
            let file_meta = LFReader::read_all_metadata(&file_scheduler).await?;
            // Binary copy only preserves page and column-buffer bytes. The output file's footer
            // (including global buffers) is re-generated, not copied from inputs.
            //
            // Therefore, we reject input files that contain any additional global buffers beyond
            // the required schema / file descriptor global buffer (global buffer index 0).
            if file_meta.file_buffers.len() > 1 {
                log::debug!(
                    "Binary copy disabled: data file has extra global buffers (len={})",
                    file_meta.file_buffers.len()
                );
                return Ok(false);
            }
        }
    }

    if !is_same_version {
        log::debug!("Binary copy disabled: data files use different file versions");
        return Ok(false);
    }

    Ok(true)
}

/// Metrics returned by [compact_files].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionMetrics {
    /// The number of fragments that have been overwritten.
    pub fragments_removed: usize,
    /// The number of new fragments that have been added.
    pub fragments_added: usize,
    /// The number of files that have been removed, including deletion files.
    pub files_removed: usize,
    /// The number of files that have been added, which is always equal to the
    /// number of fragments.
    pub files_added: usize,
}

impl AddAssign for CompactionMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.fragments_removed += rhs.fragments_removed;
        self.fragments_added += rhs.fragments_added;
        self.files_removed += rhs.files_removed;
        self.files_added += rhs.files_added;
    }
}

/// Trait for implementing custom compaction planning strategies.
///
/// This trait allows users to define their own compaction strategies by implementing
/// the `plan` method. The default implementation is provided by [`DefaultCompactionPlanner`].
#[async_trait::async_trait]
pub trait CompactionPlanner: Send + Sync {
    /// Build compaction plan.
    ///
    /// This method analyzes the dataset's fragments and generates a [`CompactionPlan`]
    /// containing a list of compaction tasks to execute.
    ///
    /// # Arguments
    ///
    /// * `dataset` - Reference to the dataset to be compacted
    async fn plan(&self, dataset: &Dataset) -> Result<CompactionPlan>;
}

/// Formulate a plan to compact the files in a dataset
///
/// The compaction plan will contain a list of tasks to execute. Each task
/// will contain approximately `target_rows_per_fragment` rows and will be
/// rewriting fragments that are adjacent in the dataset's fragment list. Some
/// tasks may contain a single fragment when that fragment has deletions that
/// are being materialized and doesn't have any neighbors that need to be
/// compacted.
#[derive(Debug, Clone, Default)]
pub struct DefaultCompactionPlanner {
    options: CompactionOptions,
}

impl DefaultCompactionPlanner {
    pub fn new(mut options: CompactionOptions) -> Self {
        options.validate();
        Self { options }
    }
}

#[async_trait::async_trait]
impl CompactionPlanner for DefaultCompactionPlanner {
    async fn plan(&self, dataset: &Dataset) -> Result<CompactionPlan> {
        if self.options.defer_index_remap && dataset.manifest.uses_stable_row_ids() {
            return Err(Error::invalid_input(
                "defer_index_remap=true is not supported on datasets with stable row IDs: \
                 stable row IDs do not require index remapping during compaction, so there \
                 is nothing to defer."
                    .to_string(),
            ));
        }

        // get_fragments should be returning fragments in sorted order (by id)
        // and fragment ids should be unique
        let fragments = dataset.get_fragments();

        debug_assert!(
            fragments.windows(2).all(|w| w[0].id() < w[1].id()),
            "fragments in manifest are not sorted"
        );
        let mut fragment_metrics = futures::stream::iter(fragments)
            .map(|fragment| async move {
                match collect_metrics(&fragment).await {
                    Ok(metrics) => Ok((fragment.metadata, metrics)),
                    Err(e) => Err(e),
                }
            })
            .buffered(dataset.object_store.as_ref().io_parallelism());

        let index_fragmaps = load_index_fragmaps(dataset).await?;
        let indices_containing_frag = |frag_id: u32| {
            index_fragmaps
                .iter()
                .enumerate()
                .filter(|(_, bitmap)| bitmap.contains(frag_id))
                .map(|(pos, _)| pos)
                .collect::<Vec<_>>()
        };

        let mut candidate_bins: Vec<CandidateBin> = Vec::new();
        let mut current_bin: Option<CandidateBin> = None;
        let mut i = 0;

        while let Some(res) = fragment_metrics.next().await {
            let (fragment, metrics) = res?;

            let over_overlay_limit = self
                .options
                .max_overlays_per_fragment
                .is_some_and(|max| fragment.overlays.len() > max);

            let candidacy = if over_overlay_limit {
                // Too many overlays: fully compact this fragment on its own,
                // regardless of its size or deletion count.
                Some(CompactionCandidacy::CompactItself)
            } else if self.options.materialize_deletions
                && metrics.deletion_percentage() > self.options.materialize_deletions_threshold
            {
                Some(CompactionCandidacy::CompactItself)
            } else if metrics.physical_rows < self.options.target_rows_per_fragment {
                // Only want to compact if their are neighbors to compact such that
                // we can get a larger fragment.
                Some(CompactionCandidacy::CompactWithNeighbors)
            } else {
                // Not a candidate
                None
            };

            let indices = indices_containing_frag(fragment.id as u32);

            match (candidacy, &mut current_bin) {
                (None, None) => {} // keep searching
                (Some(candidacy), None) => {
                    // Start a new bin
                    current_bin = Some(CandidateBin {
                        fragments: vec![fragment],
                        pos_range: i..(i + 1),
                        candidacy: vec![candidacy],
                        row_counts: vec![metrics.num_rows()],
                        indices,
                    });
                }
                (Some(candidacy), Some(bin)) => {
                    // We cannot mix "indexed" and "non-indexed" fragments and so we only consider
                    // the existing bin if it contains the same indices
                    if bin.indices == indices {
                        // Add to current bin
                        bin.fragments.push(fragment);
                        bin.pos_range.end += 1;
                        bin.candidacy.push(candidacy);
                        bin.row_counts.push(metrics.num_rows());
                    } else {
                        // Index set is different.  Complete previous bin and start new one
                        candidate_bins.push(current_bin.take().unwrap());
                        current_bin = Some(CandidateBin {
                            fragments: vec![fragment],
                            pos_range: i..(i + 1),
                            candidacy: vec![candidacy],
                            row_counts: vec![metrics.num_rows()],
                            indices,
                        });
                    }
                }
                (None, Some(_)) => {
                    // Bin is complete
                    candidate_bins.push(current_bin.take().unwrap());
                }
            }

            i += 1;
        }

        // Flush the last bin
        if let Some(bin) = current_bin {
            candidate_bins.push(bin);
        }

        let all_tasks: Vec<TaskData> = candidate_bins
            .into_iter()
            .filter(|bin| !bin.is_noop())
            .flat_map(|bin| bin.split_for_size(self.options.target_rows_per_fragment))
            .map(|bin| TaskData {
                fragments: bin.fragments,
            })
            .collect();

        let tasks = if let Some(max_frags) = self.options.max_source_fragments {
            let mut total_frags = 0;
            all_tasks
                .into_iter()
                .take_while(|task| {
                    total_frags += task.fragments.len();
                    total_frags <= max_frags
                })
                .collect()
        } else {
            all_tasks
        };

        let mut compaction_plan =
            CompactionPlan::new(dataset.manifest.version, self.options.clone());
        compaction_plan.extend_tasks(tasks);

        Ok(compaction_plan)
    }
}

/// Compacts the files in the dataset without reordering them.
///
/// By default, this does a few things:
///  * Removes deleted rows from fragments.
///  * Removes dropped columns from fragments.
///  * Merges fragments that are too small.
///
/// This method tries to preserve the insertion order of rows in the dataset.
///
/// If no compaction is needed, this method will not make a new version of the table.
pub async fn compact_files(
    dataset: &mut Dataset,
    options: CompactionOptions,
    remap_options: Option<Arc<dyn IndexRemapperOptions>>, // These will be deprecated later
) -> Result<CompactionMetrics> {
    info!(target: TRACE_DATASET_EVENTS, event=DATASET_COMPACTING_EVENT, uri = &dataset.uri);
    let planner = DefaultCompactionPlanner::new(options);
    compact_files_with_planner(dataset, remap_options, &planner).await
}

pub async fn compact_files_with_planner(
    dataset: &mut Dataset,
    remap_options: Option<Arc<dyn IndexRemapperOptions>>, // These will be deprecated later
    planner: &dyn CompactionPlanner,
) -> Result<CompactionMetrics> {
    let compaction_plan: CompactionPlan = planner.plan(dataset).await?;

    // If nothing to compact, don't make a commit.
    if compaction_plan.tasks().is_empty() {
        return Ok(CompactionMetrics::default());
    }

    let dataset_ref = &dataset.clone();

    let result_stream = futures::stream::iter(compaction_plan.tasks)
        .map(|task| rewrite_files(Cow::Borrowed(dataset_ref), task, &compaction_plan.options))
        .buffer_unordered(
            compaction_plan
                .options
                .num_threads
                .unwrap_or_else(get_num_compute_intensive_cpus),
        );

    let completed_tasks: Vec<RewriteResult> = result_stream.try_collect().await?;
    let remap_options = remap_options.unwrap_or(Arc::new(DatasetIndexRemapperOptions::default()));
    let metrics = commit_compaction(
        dataset,
        completed_tasks,
        remap_options,
        &compaction_plan.options,
    )
    .await?;

    Ok(metrics)
}

/// Information about a fragment used to decide its fate in compaction
#[derive(Debug)]
struct FragmentMetrics {
    /// The number of original rows in the fragment
    pub physical_rows: usize,
    /// The number of rows that have been deleted
    pub num_deletions: usize,
}

impl FragmentMetrics {
    /// The fraction of rows that have been deleted
    fn deletion_percentage(&self) -> f32 {
        if self.physical_rows > 0 {
            self.num_deletions as f32 / self.physical_rows as f32
        } else {
            0.0
        }
    }

    /// The number of rows that are still in the fragment
    fn num_rows(&self) -> usize {
        self.physical_rows - self.num_deletions
    }
}

async fn collect_metrics(fragment: &FileFragment) -> Result<FragmentMetrics> {
    let physical_rows = fragment.physical_rows();
    let num_deletions = fragment.count_deletions();
    let (physical_rows, num_deletions) =
        futures::future::try_join(physical_rows, num_deletions).await?;
    Ok(FragmentMetrics {
        physical_rows,
        num_deletions,
    })
}

/// A plan for what groups of fragments to compact.
///
/// See [plan_compaction()] for more details.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompactionPlan {
    pub tasks: Vec<TaskData>,
    pub read_version: u64,
    pub options: CompactionOptions,
}

impl CompactionPlan {
    /// Retrieve standalone tasks that be be executed in a distributed fashion.
    pub fn compaction_tasks(&self) -> impl Iterator<Item = CompactionTask> + '_ {
        let read_version = self.read_version;
        let options = self.options.clone();
        self.tasks.iter().map(move |task| CompactionTask {
            task: task.clone(),
            read_version,
            options: options.clone(),
        })
    }

    /// The number of tasks in the plan.
    pub fn num_tasks(&self) -> usize {
        self.tasks.len()
    }

    /// The version of the dataset that was read to produce this plan.
    pub fn read_version(&self) -> u64 {
        self.read_version
    }

    /// The options used to produce this plan.
    pub fn options(&self) -> &CompactionOptions {
        &self.options
    }
}

/// Classification for one blob v2 row during compaction.
///
/// - `Null`: NULL row.
/// - `External`: External blob referenced by URI.
/// - `DataBlob`: Inline/Packed/Dedicated blob stored in Lance files.
enum RowClass {
    Null,
    External,
    DataBlob,
}

/// Column views for the 5 fields in a blob v2 descriptor struct.
struct BlobV2Descriptor<'a> {
    kind_col: &'a arrow::array::UInt8Array,
    position_col: &'a arrow::array::UInt64Array,
    size_col: &'a arrow::array::UInt64Array,
    blob_uri_col: &'a arrow::array::StringArray,
    blob_id_col: &'a arrow::array::UInt32Array,
}

impl<'a> BlobV2Descriptor<'a> {
    /// Extract the 5 descriptor arrays from a blob v2 descriptor struct array.
    fn try_from_struct(struct_arr: &'a StructArray, column_name: &str) -> Result<Self> {
        if BlobV2Layout::classify(struct_arr.fields()) != Some(BlobV2Layout::Descriptor) {
            let actual = BlobV2Layout::classify(struct_arr.fields())
                .map(|layout| layout.to_string())
                .unwrap_or_else(|| format!("unrecognized ({:?})", struct_arr.fields()));
            return Err(Error::invalid_input(format!(
                "Blob v2 column '{column_name}' has {actual} layout; expected descriptor layout before conversion to logical"
            )));
        }
        let kind_col = struct_arr
            .column_by_name("kind")
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 descriptor for column '{}' missing `kind` field",
                    column_name
                ))
            })?
            .as_primitive::<UInt8Type>();
        let position_col = struct_arr
            .column_by_name("position")
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 descriptor for column '{}' missing `position` field",
                    column_name
                ))
            })?
            .as_primitive::<UInt64Type>();
        let size_col = struct_arr
            .column_by_name("size")
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 descriptor for column '{}' missing `size` field",
                    column_name
                ))
            })?
            .as_primitive::<UInt64Type>();
        let blob_uri_col = struct_arr
            .column_by_name("blob_uri")
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 descriptor for column '{}' missing `blob_uri` field",
                    column_name
                ))
            })?
            .as_string::<i32>();
        let blob_id_col = struct_arr
            .column_by_name("blob_id")
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 descriptor for column '{}' missing `blob_id` field",
                    column_name
                ))
            })?
            .as_primitive::<UInt32Type>();
        Ok(Self {
            kind_col,
            position_col,
            size_col,
            blob_uri_col,
            blob_id_col,
        })
    }
}

/// Result of row classification for blob v2 compaction.
struct RowClassification {
    row_classes: Vec<RowClass>,
    blob_read_addrs: Vec<u64>,
}

/// Classify each row of a blob v2 column as Null, External, or DataBlob.
fn classify_rows(
    struct_arr: &StructArray,
    descriptor: &BlobV2Descriptor<'_>,
    row_addrs: &arrow::array::UInt64Array,
    column_name: &str,
) -> Result<RowClassification> {
    let num_rows = struct_arr.len();
    let mut row_classes = Vec::with_capacity(num_rows);
    let mut blob_read_addrs = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        if struct_arr.is_null(i) || descriptor.kind_col.is_null(i) {
            row_classes.push(RowClass::Null);
        } else {
            let kind = BlobKind::try_from(descriptor.kind_col.value(i)).map_err(|e| {
                Error::internal(format!(
                    "Blob v2 column '{}' has invalid kind at row {}: {e}",
                    column_name, i
                ))
            })?;
            if kind == BlobKind::External {
                row_classes.push(RowClass::External);
            } else {
                row_classes.push(RowClass::DataBlob);
                blob_read_addrs.push(row_addrs.value(i));
            }
        }
    }

    Ok(RowClassification {
        row_classes,
        blob_read_addrs,
    })
}

/// Convert a blob v2 descriptor into the logical writer representation.
///
/// Reads blob data lazily using row addresses to avoid materializing all blob
/// payloads in memory at once.
async fn descriptor_to_logical_blob_array(
    dataset: &Arc<Dataset>,
    descriptor: &BlobV2Descriptor<'_>,
    classification: &RowClassification,
    column_name: &str,
    num_rows: usize,
    null_buffer: Option<NullBuffer>,
) -> Result<StructArray> {
    let blob_files = if classification.blob_read_addrs.is_empty() {
        Vec::new()
    } else {
        super::blob::take_blobs_by_addresses(dataset, &classification.blob_read_addrs, column_name)
            .await?
    };

    let mut data_builder = LargeBinaryBuilder::with_capacity(num_rows, 0);
    let mut uri_builder = StringBuilder::with_capacity(num_rows, 0);
    let mut out_position_builder = PrimitiveBuilder::<UInt64Type>::with_capacity(num_rows);
    let mut out_size_builder = PrimitiveBuilder::<UInt64Type>::with_capacity(num_rows);

    let mut blob_file_idx = 0;
    #[allow(clippy::needless_range_loop)]
    for i in 0..num_rows {
        match classification.row_classes[i] {
            RowClass::Null => {
                data_builder.append_null();
                uri_builder.append_null();
                out_position_builder.append_null();
                out_size_builder.append_null();
            }
            RowClass::External => {
                data_builder.append_null();
                let base_id = descriptor.blob_id_col.value(i);
                let uri_val = descriptor.blob_uri_col.value(i);
                if base_id == 0 {
                    uri_builder.append_value(uri_val);
                } else {
                    let base = dataset.manifest().base_paths.get(&base_id).ok_or_else(|| {
                        Error::internal(format!(
                            "External blob in column '{}' references unknown base_id {}",
                            column_name, base_id
                        ))
                    })?;
                    let absolute_uri = format!("{}/{}", base.path.trim_end_matches('/'), uri_val);
                    uri_builder.append_value(&absolute_uri);
                }
                if descriptor.position_col.is_null(i) {
                    out_position_builder.append_null();
                } else {
                    out_position_builder.append_value(descriptor.position_col.value(i));
                }
                if descriptor.size_col.is_null(i) {
                    out_size_builder.append_null();
                } else {
                    out_size_builder.append_value(descriptor.size_col.value(i));
                }
            }
            RowClass::DataBlob => {
                let blob_file = blob_files[blob_file_idx].as_ref().ok_or_else(|| {
                    Error::internal(format!(
                        "Non-null blob row {} in column '{}' resolved to null",
                        i, column_name
                    ))
                })?;
                let data = blob_file.read().await?;
                blob_file_idx += 1;
                data_builder.append_value(data.as_ref());
                uri_builder.append_null();
                out_position_builder.append_null();
                out_size_builder.append_null();
            }
        }
    }

    Ok(StructArray::try_new(
        BLOB_V2_LOGICAL_FIELDS.clone(),
        vec![
            Arc::new(data_builder.finish()),
            Arc::new(uri_builder.finish()),
            Arc::new(out_position_builder.finish()),
            Arc::new(out_size_builder.finish()),
        ],
        null_buffer,
    )?)
}

pub(crate) async fn transform_blob_v2_batch(
    dataset: &Arc<Dataset>,
    schema: &lance_core::datatypes::Schema,
    batch: RecordBatch,
    keep_row_addr: bool,
) -> Result<RecordBatch> {
    let row_addr_idx = batch
        .schema()
        .column_with_name(lance_core::ROW_ADDR)
        .ok_or_else(|| {
            Error::internal(format!(
                "_rowaddr column missing from batch for blob v2 compaction, columns: {:?}",
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ))
        })?
        .0;
    let row_addrs = batch.column(row_addr_idx).as_primitive::<UInt64Type>();

    let mut new_columns: Vec<Arc<dyn Array>> = Vec::new();
    let mut new_fields: Vec<Arc<arrow_schema::Field>> = Vec::new();

    let batch_schema = batch.schema();
    for (col_idx, field) in batch_schema.fields().iter().enumerate() {
        if field.name() == lance_core::ROW_ADDR && !keep_row_addr {
            continue;
        }

        let lance_field = schema.field(field.name());
        let is_blob_v2 = lance_field.is_some_and(|f| f.is_blob_v2());

        if !is_blob_v2 {
            new_columns.push(batch.column(col_idx).clone());
            new_fields.push(field.clone());
            continue;
        }

        let struct_arr = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                Error::internal(format!(
                    "Blob v2 column '{}' expected StructArray, got {:?}",
                    field.name(),
                    batch.column(col_idx).data_type()
                ))
            })?;

        match BlobV2Layout::classify(struct_arr.fields()) {
            // Merge-insert may supply a logical blob v2 value directly from the
            // source. It does not refer to a row in the target dataset.
            Some(BlobV2Layout::Logical) => {
                new_columns.push(batch.column(col_idx).clone());
                new_fields.push(field.clone());
                continue;
            }
            Some(BlobV2Layout::Descriptor) => {}
            Some(actual) => {
                return Err(Error::invalid_input(format!(
                    "Blob v2 column '{}' has {actual} layout; expected logical or descriptor layout during compaction",
                    field.name()
                )));
            }
            None => {
                return Err(Error::invalid_input(format!(
                    "Blob v2 column '{}' has unrecognized layout {:?}; expected logical or descriptor layout during compaction",
                    field.name(),
                    struct_arr.fields()
                )));
            }
        }

        let column_name = field.name();
        let descriptor = BlobV2Descriptor::try_from_struct(struct_arr, column_name)?;
        let classification = classify_rows(struct_arr, &descriptor, row_addrs, column_name)?;
        let num_rows = struct_arr.len();

        let new_struct = descriptor_to_logical_blob_array(
            dataset,
            &descriptor,
            &classification,
            column_name,
            num_rows,
            struct_arr.nulls().cloned(),
        )
        .await?;

        new_columns.push(Arc::new(new_struct));
        let logical_field = arrow_schema::Field::from(lance_field.ok_or_else(|| {
            Error::internal(format!(
                "Blob v2 column '{}' missing from dataset schema during compaction",
                field.name()
            ))
        })?);
        new_fields.push(Arc::new(
            arrow_schema::Field::new(
                field.name(),
                BLOB_V2_LOGICAL_TYPE.clone(),
                field.is_nullable(),
            )
            .with_metadata(logical_field.metadata().clone()),
        ));
    }

    let new_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        new_fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
        batch_schema.metadata().clone(),
    ));

    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// Build a scan reader for rewrite and optionally capture row IDs.
///
/// Parameters:
/// - `dataset`: Dataset handle used to create the scanner.
/// - `fragments`: When `with_frags` is true, restrict the scan to these old fragments
///   and preserve insertion order.
/// - `batch_size`: Optional batch size; if provided, set it on the scanner to control
///   read batching.
/// - `io_buffer_size`: Optional I/O buffer size in bytes; if provided, set it on the
///   scanner to control how much data is queued during reads.
/// - `with_frags`: Whether to scan only the specified old fragments and force
///   in-order reading.
/// - `capture_row_ids`: When index remapping is needed, include and capture the
///   `_rowid` column from the stream.
///
/// Returns:
/// - `SendableRecordBatchStream`: The batch stream (with `_rowid` removed if captured)
///   to feed the rewrite path.
/// - `Option<Receiver<CapturedRowIds>>`: A receiver to obtain captured row IDs after the
///   stream completes; `None` if not capturing.
/// - `bool`: Whether the dataset has blob v2 columns and the stream includes `_rowaddr`.
async fn prepare_reader(
    dataset: &Dataset,
    fragments: &[Fragment],
    batch_size: Option<usize>,
    io_buffer_size: Option<u64>,
    with_frags: bool,
    capture_row_ids: bool,
) -> Result<(
    SendableRecordBatchStream,
    Option<std::sync::mpsc::Receiver<CapturedRowIds>>,
    bool,
)> {
    let mut scanner = dataset.scan();
    let has_legacy_blob_columns = dataset
        .schema()
        .fields_pre_order()
        .any(|field| field.is_blob() && !field.is_blob_v2());
    if has_legacy_blob_columns {
        scanner.blob_handling(BlobHandling::AllBinary);
    }
    let has_blob_v2_columns = dataset
        .schema()
        .fields_pre_order()
        .any(|field| field.is_blob_v2());
    if has_blob_v2_columns {
        scanner.with_row_address();
    }
    if let Some(bs) = batch_size {
        scanner.batch_size(bs);
    }
    if let Some(io_buffer_size) = io_buffer_size {
        scanner.io_buffer_size(io_buffer_size);
    }
    if with_frags {
        scanner
            .with_fragments(fragments.to_vec())
            .scan_in_order(true);
    }
    if capture_row_ids {
        scanner.with_row_id();
        let data = SendableRecordBatchStream::from(scanner.try_into_stream().await?);
        let (data_no_row_ids, rx) =
            make_rowid_capture_stream(data, dataset.manifest.uses_stable_row_ids())?;
        Ok((data_no_row_ids, Some(rx), has_blob_v2_columns))
    } else {
        Ok((
            SendableRecordBatchStream::from(scanner.try_into_stream().await?),
            None,
            has_blob_v2_columns,
        ))
    }
}

/// A single group of fragments to compact, which is a view into the compaction
/// plan. We keep the `replace_range` indices so we can map the result of the
/// compact back to the fragments it replaces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskData {
    /// The fragments to compact.
    pub fragments: Vec<Fragment>,
}

/// A standalone task that can be serialized and sent to another machine for
/// execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionTask {
    pub task: TaskData,
    pub read_version: u64,
    pub options: CompactionOptions,
}

impl CompactionTask {
    /// Run the compaction task and return the result.
    ///
    /// This result should be later passed to [commit_compaction()] to commit
    /// the changes to the dataset.
    ///
    /// Note: you should pass the version of the dataset that is the same as
    /// the read version for this task (the same version from which the
    /// compaction was planned).
    pub async fn execute(&self, dataset: &Dataset) -> Result<RewriteResult> {
        let dataset = if dataset.manifest.version == self.read_version {
            Cow::Borrowed(dataset)
        } else {
            Cow::Owned(dataset.checkout_version(self.read_version).await?)
        };
        rewrite_files(dataset, self.task.clone(), &self.options).await
    }
}

impl CompactionPlan {
    fn new(read_version: u64, options: CompactionOptions) -> Self {
        Self {
            tasks: Vec::new(),
            read_version,
            options,
        }
    }

    fn extend_tasks(&mut self, tasks: impl IntoIterator<Item = TaskData>) {
        self.tasks.extend(tasks);
    }

    fn tasks(&self) -> &[TaskData] {
        &self.tasks
    }
}

#[derive(Debug, Clone)]
enum CompactionCandidacy {
    /// Compact the fragment if it has neighbors that are also candidates
    CompactWithNeighbors,
    /// Compact the fragment regardless.
    CompactItself,
}

/// Internal struct used for planning compaction.
struct CandidateBin {
    pub fragments: Vec<Fragment>,
    pub pos_range: Range<usize>,
    pub candidacy: Vec<CompactionCandidacy>,
    pub row_counts: Vec<usize>,
    pub indices: Vec<usize>,
}

impl CandidateBin {
    /// Return true if compacting these fragments wouldn't do anything.
    fn is_noop(&self) -> bool {
        if self.fragments.is_empty() {
            return true;
        }
        // If there's only one fragment, it's a noop if it's not CompactItself
        if self.fragments.len() == 1 {
            matches!(self.candidacy[0], CompactionCandidacy::CompactWithNeighbors)
        } else {
            false
        }
    }

    /// Split into one or more bins with at least `min_num_rows` in them.
    fn split_for_size(self, min_num_rows: usize) -> Vec<Self> {
        let total_rows = self.row_counts.iter().sum::<usize>();
        let mut remaining_rows = total_rows;
        let mut current_rows = 0;
        let mut current_len = 0;
        let mut split_lengths = Vec::new();

        for row_count in &self.row_counts {
            current_rows += *row_count;
            current_len += 1;
            remaining_rows -= *row_count;

            // Only split once the current bin is large enough and there is
            // enough left over to form another worthwhile non-empty bin.
            if current_rows >= min_num_rows && remaining_rows > 0 && remaining_rows >= min_num_rows
            {
                split_lengths.push(current_len);
                current_rows = 0;
                current_len = 0;
            }
        }

        if split_lengths.is_empty() {
            return vec![self];
        }

        let mut bins = Vec::with_capacity(split_lengths.len() + 1);
        let mut fragments = self.fragments.into_iter();
        let mut candidacy = self.candidacy.into_iter();
        let mut row_counts = self.row_counts.into_iter();
        let mut pos_start = self.pos_range.start;

        for bin_len in split_lengths {
            bins.push(Self {
                fragments: fragments.by_ref().take(bin_len).collect(),
                pos_range: pos_start..(pos_start + bin_len),
                candidacy: candidacy.by_ref().take(bin_len).collect(),
                row_counts: row_counts.by_ref().take(bin_len).collect(),
                // By the time we are splitting for size we are done considering indices
                indices: Vec::new(),
            });
            pos_start += bin_len;
        }

        bins.push(Self {
            fragments: fragments.collect(),
            pos_range: pos_start..self.pos_range.end,
            candidacy: candidacy.collect(),
            row_counts: row_counts.collect(),
            indices: self.indices,
        });

        bins
    }
}

async fn load_index_fragmaps(dataset: &Dataset) -> Result<Vec<RoaringBitmap>> {
    let indices = dataset.load_indices().await?;
    let mut index_fragmaps = Vec::with_capacity(indices.len());
    // System indices (fragment-reuse, mem-wal) don't define data coverage and
    // aren't remapped per rewrite group, so they must not constrain compaction
    // bins -- otherwise deferred compaction's fragment-reuse index repeatedly
    // splits the small-fragment run and they never coalesce.
    for index in indices.iter().filter(|idx| !is_system_index(idx)) {
        if let Some(fragment_bitmap) = index.fragment_bitmap.as_ref() {
            index_fragmaps.push(fragment_bitmap.clone());
        } else {
            let dataset_at_index = dataset.checkout_version(index.dataset_version).await?;
            // max_fragment_id is inclusive (the highest id); +1 for an exclusive
            // upper bound so the last fragment is covered (None => empty range).
            let frags = 0..dataset_at_index
                .manifest
                .max_fragment_id
                .map_or(0, |m| m + 1);
            index_fragmaps.push(RoaringBitmap::from_sorted_iter(frags).unwrap());
        }
    }
    Ok(index_fragmaps)
}

pub async fn plan_compaction(
    dataset: &Dataset,
    options: &CompactionOptions,
) -> Result<CompactionPlan> {
    let planner = DefaultCompactionPlanner::new(options.clone());
    planner.plan(dataset).await
}

/// The result of a single compaction task.
///
/// This should be passed to [commit_compaction()] to commit the operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RewriteResult {
    pub metrics: CompactionMetrics,
    pub new_fragments: Vec<Fragment>,
    /// The version of the dataset that was read to perform this compaction.
    pub read_version: u64,
    /// The original fragments being replaced
    pub original_fragments: Vec<Fragment>,
    /// Serialized `RoaringTreemap` of the row addresses from the original
    /// fragments that were read during compaction.
    ///
    /// - `None` when configured with stable row IDs because the row ID
    ///   sequences are rechunked directly.
    /// - `Some` then these addresses are either (1) written to storage for
    ///   deferred index remap post-processing, or (2) used with reserved
    ///   fragment IDs to build old-to-new mappings.
    pub row_addrs: Option<Vec<u8>>,
}

async fn reserve_fragment_ids(
    dataset: &Dataset,
    fragments: impl ExactSizeIterator<Item = &mut Fragment>,
) -> Result<()> {
    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::ReserveFragments {
            num_fragments: fragments.len() as u32,
        },
        None,
    );

    let (manifest, _) = commit_transaction(
        dataset,
        dataset.object_store.as_ref(),
        dataset.commit_handler.as_ref(),
        &transaction,
        &Default::default(),
        &Default::default(),
        DEFAULT_COMMIT_RETRY_TIMEOUT,
        dataset.manifest_location.naming_scheme,
        None,
    )
    .await?;

    // Need +1 since max_fragment_id is inclusive in this case and ranges are exclusive
    let new_max_exclusive = manifest.max_fragment_id.unwrap_or(0) + 1;
    let reserved_ids = (new_max_exclusive - fragments.len() as u32)..(new_max_exclusive);

    for (fragment, new_id) in fragments.zip(reserved_ids) {
        fragment.id = new_id as u64;
    }

    Ok(())
}

/// Rewrite the files in a single task.
///
/// This assumes that the dataset is the correct read version to be compacted.
async fn rewrite_files(
    dataset: Cow<'_, Dataset>,
    task: TaskData,
    options: &CompactionOptions,
) -> Result<RewriteResult> {
    let mut metrics = CompactionMetrics::default();

    if task.fragments.is_empty() {
        return Ok(RewriteResult {
            metrics,
            new_fragments: Vec::new(),
            read_version: dataset.manifest.version,
            original_fragments: task.fragments,
            row_addrs: None,
        });
    }

    let previous_writer_version = &dataset.manifest.writer_version;
    // The versions of Lance prior to when we started writing the writer version
    // sometimes wrote incorrect `Fragment.physical_rows` values, so we should
    // make sure to recompute them.
    // See: https://github.com/lance-format/lance/issues/1531
    let recompute_stats = previous_writer_version.is_none();

    // It's possible the fragments are old and don't have physical rows or
    // num deletions recorded. If that's the case, we need to grab and set that
    // information.
    let fragments = migrate_fragments(dataset.as_ref(), &task.fragments, recompute_stats).await?;
    let num_rows = fragments
        .iter()
        .map(|f| f.physical_rows.unwrap() as u64)
        .sum::<u64>();
    // Capturing row addresses is only useful if something will consume them:
    // an index to remap now, or a deferred remap through the FRI.
    let capture_row_addrs = !dataset.manifest.uses_stable_row_ids()
        && (options.defer_index_remap
            || load_indices_for_remapping(dataset.as_ref())
                .await?
                .is_some());
    let mut new_fragments: Vec<Fragment>;
    let task_id = uuid::Uuid::new_v4();
    log::info!(
        "Compaction task {}: Begin compacting {} rows across {} fragments",
        task_id,
        num_rows,
        fragments.len()
    );
    let mode = options.compaction_mode();
    let can_binary_copy = can_use_binary_copy(dataset.as_ref(), options, &fragments).await;
    if !can_binary_copy && matches!(mode, CompactionMode::ForceBinaryCopy) {
        return Err(Error::not_supported_source(
            format!("compaction task {}: binary copy is not supported", task_id).into(),
        ));
    }
    let mut row_ids_rx: Option<std::sync::mpsc::Receiver<CapturedRowIds>> = None;
    let mut reader: Option<SendableRecordBatchStream> = None;

    if !can_binary_copy {
        let (prepared_reader, rx_initial, has_blob_v2_columns) = prepare_reader(
            dataset.as_ref(),
            &fragments,
            options.batch_size,
            options.io_buffer_size,
            true,
            capture_row_addrs,
        )
        .await?;
        row_ids_rx = rx_initial;

        let mut rows_read = 0;
        let schema = prepared_reader.schema();
        let reader_with_progress = prepared_reader.inspect_ok(move |batch| {
            rows_read += batch.num_rows();
            log::info!(
                "Compaction task {}: Read progress {}/{}",
                task_id,
                rows_read,
                num_rows,
            );
        });

        if has_blob_v2_columns {
            let dataset_arc = Arc::new(dataset.as_ref().clone());
            let dataset_schema = dataset.schema().clone();
            let transformed = reader_with_progress.then(move |batch_result| {
                let dataset = dataset_arc.clone();
                let schema = dataset_schema.clone();
                async move {
                    let batch = batch_result?;
                    transform_blob_v2_batch(&dataset, &schema, batch, false)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
                }
            });
            let transformed_schema = {
                let mut fields: Vec<Arc<arrow_schema::Field>> = Vec::new();
                for field in schema.fields().iter() {
                    if field.name() == lance_core::ROW_ADDR {
                        continue;
                    }
                    let lance_field = dataset.schema().field(field.name());
                    if let Some(lance_field) = lance_field.filter(|f| f.is_blob_v2()) {
                        let logical_field = arrow_schema::Field::from(lance_field);
                        fields.push(Arc::new(
                            arrow_schema::Field::new(
                                field.name(),
                                BLOB_V2_LOGICAL_TYPE.clone(),
                                field.is_nullable(),
                            )
                            .with_metadata(logical_field.metadata().clone()),
                        ));
                    } else {
                        fields.push(field.clone());
                    }
                }
                Arc::new(arrow_schema::Schema::new_with_metadata(
                    fields
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect::<Vec<_>>(),
                    schema.metadata().clone(),
                ))
            };
            reader = Some(Box::pin(RecordBatchStreamAdapter::new(
                transformed_schema,
                transformed,
            )));
        } else {
            reader = Some(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                reader_with_progress,
            )));
        }
    }

    let mut params = WriteParams {
        max_rows_per_file: options.target_rows_per_fragment,
        max_rows_per_group: options.max_rows_per_group,
        mode: WriteMode::Append,
        // External blobs may reference URIs outside the dataset's base_paths
        // (e.g. absolute file:// URIs with base_id == 0). Without this flag
        // the writer would reject such blobs.
        allow_external_blob_outside_bases: true,
        ..Default::default()
    };
    if let Some(max_bytes_per_file) = options.max_bytes_per_file {
        params.max_bytes_per_file = max_bytes_per_file;
    }

    if dataset.manifest.uses_stable_row_ids() {
        params.enable_stable_row_ids = true;
    }

    if can_binary_copy {
        new_fragments = rewrite_files_binary_copy(
            dataset.as_ref(),
            &fragments,
            &params,
            options.binary_copy_read_batch_bytes,
        )
        .await?;

        if new_fragments.is_empty() && matches!(mode, CompactionMode::ForceBinaryCopy) {
            return Err(Error::not_supported_source(
                format!("compaction task {}: binary copy is not supported", task_id).into(),
            ));
        }

        if capture_row_addrs {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut addrs = RoaringTreemap::new();
            for frag in &fragments {
                let frag_id = frag.id as u32;
                let count = u64::try_from(frag.physical_rows.unwrap_or(0)).map_err(|_| {
                    Error::internal(format!(
                        "Fragment {} has too many physical rows to represent as row addresses",
                        frag.id
                    ))
                })?;
                let start = u64::from(lance_core::utils::address::RowAddress::first_row(frag_id));
                addrs.insert_range(start..start + count);
            }
            let captured = CapturedRowIds::AddressStyle(addrs);
            let _ = tx.send(captured);
            row_ids_rx = Some(rx);
        }
    } else {
        let (frags, _) = write_fragments_internal(
            dataset.manifest.data_storage_format.lance_file_format(),
            Some(dataset.as_ref()),
            dataset.object_store.clone(),
            &dataset.base,
            dataset.schema().clone(),
            reader.expect("reader must be prepared for non-binary-copy path"),
            params,
            None,
        )
        .await?;
        new_fragments = frags;
    }

    log::info!("Compaction task {}: file written", task_id);

    // Wrap in an async block so `?` returns into `row_addrs_result` and we can
    // run cleanup before propagating the error.
    let row_addrs_result: Result<Option<Vec<u8>>> = async {
        if let Some(row_ids_rx) = row_ids_rx {
            let captured_ids = row_ids_rx
                .try_recv()
                .map_err(|err| Error::internal(format!("Failed to receive row ids: {}", err)))?;
            let row_addrs = captured_ids.row_addrs(None).into_owned();
            let mut serialized = Vec::with_capacity(row_addrs.serialized_size());
            row_addrs.serialize_into(&mut serialized)?;
            Ok(Some(serialized))
        } else {
            if dataset.manifest.uses_stable_row_ids() {
                log::info!("Compaction task {}: rechunking stable row ids", task_id);
                rechunk_stable_row_ids(dataset.as_ref(), &mut new_fragments, &fragments).await?;
                recalc_versions_for_rewritten_fragments(
                    dataset.as_ref(),
                    &mut new_fragments,
                    &fragments,
                )
                .await?;
            }
            Ok(None)
        }
    }
    .await;

    let row_addrs = match row_addrs_result {
        Ok(v) => v,
        Err(e) => {
            cleanup_data_fragments(&dataset.object_store, &dataset.base, None, &new_fragments)
                .await;
            return Err(e);
        }
    };

    metrics.files_removed = task
        .fragments
        .iter()
        .map(|f| f.files.len() + f.deletion_file.is_some() as usize)
        .sum();
    metrics.fragments_removed = task.fragments.len();
    metrics.fragments_added = new_fragments.len();
    metrics.files_added = new_fragments
        .iter()
        .map(|f| f.files.len() + f.deletion_file.is_some() as usize)
        .sum();

    log::info!("Compaction task {}: completed", task_id);

    Ok(RewriteResult {
        metrics,
        new_fragments,
        read_version: dataset.manifest.version,
        original_fragments: fragments,
        row_addrs,
    })
}

async fn rechunk_stable_row_ids(
    dataset: &Dataset,
    new_fragments: &mut [Fragment],
    old_fragments: &[Fragment],
) -> Result<()> {
    let mut old_sequences = load_row_id_sequences(dataset, old_fragments)
        .try_collect::<Vec<_>>()
        .await?;
    // Should sort them back into original order.
    old_sequences.sort_by_key(|(frag_id, _)| {
        old_fragments
            .iter()
            .position(|frag| frag.id as u32 == *frag_id)
            .expect("Fragment not found")
    });

    // Need to remove deleted rows
    futures::stream::iter(old_sequences.iter_mut().zip(old_fragments.iter()))
        .map(Ok)
        .try_for_each(|((_, seq), frag)| async move {
            if let Some(deletion_file) = &frag.deletion_file {
                let deletions = read_dataset_deletion_file(dataset, frag.id, deletion_file).await?;

                let mut new_seq = seq.as_ref().clone();
                new_seq.mask(deletions.to_sorted_iter())?;
                *seq = Arc::new(new_seq);
            }
            Ok::<(), crate::Error>(())
        })
        .await?;

    debug_assert_eq!(
        { old_sequences.iter().map(|(_, seq)| seq.len()).sum::<u64>() },
        {
            new_fragments
                .iter()
                .map(|frag| frag.physical_rows.unwrap() as u64)
                .sum::<u64>()
        },
        "{:?}",
        old_sequences
    );

    let new_sequences = lance_table::rowids::rechunk_sequences(
        old_sequences
            .into_iter()
            .map(|(_, seq)| seq.as_ref().clone()),
        new_fragments
            .iter()
            .map(|frag| frag.physical_rows.unwrap() as u64),
        false,
    )?;

    for (fragment, sequence) in new_fragments.iter_mut().zip(new_sequences) {
        // TODO: if large enough, serialize to separate file
        let serialized = lance_table::rowids::write_row_ids(&sequence);
        fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
    }

    Ok(())
}

/// After row id rechunking, preserve per-row latest update versions by masking deletions and rechunking
async fn recalc_versions_for_rewritten_fragments(
    dataset: &Dataset,
    new_fragments: &mut [Fragment],
    old_fragments: &[Fragment],
) -> Result<()> {
    // Load old per-row last_updated_at version sequences
    let mut old_last_updated_sequences: Vec<lance_table::format::RowDatasetVersionSequence> =
        Vec::with_capacity(old_fragments.len());
    // Load old per-row created_at version sequences
    let mut old_created_at_sequences: Vec<lance_table::format::RowDatasetVersionSequence> =
        Vec::with_capacity(old_fragments.len());

    for frag in old_fragments.iter() {
        let row_count = if let Some(row_id_meta) = &frag.row_id_meta {
            match row_id_meta {
                RowIdMeta::Inline(data) => lance_table::rowids::read_row_ids(data)?.len(),
                RowIdMeta::External(_file) => frag.physical_rows.unwrap_or(0) as u64,
            }
        } else {
            frag.physical_rows.unwrap_or(0) as u64
        };

        // Load created_at sequence (default to version 1 if missing)
        let mut created_at_seq = if let Some(version_meta) = &frag.created_at_version_meta {
            version_meta.load_sequence().map_err(|e| {
                Error::internal(format!("Failed to load created_at version sequence: {}", e))
            })?
        } else {
            // Default: treat all rows as created at version 1
            lance_table::format::RowDatasetVersionSequence::from_uniform_row_count(row_count, 1)
        };

        // Load last_updated_at sequence (default to same as created_at sequence)
        let mut last_updated_seq = if let Some(version_meta) = &frag.last_updated_at_version_meta {
            version_meta.load_sequence().map_err(|e| {
                Error::internal(format!(
                    "Failed to load last_updated_at version sequence: {}",
                    e
                ))
            })?
        } else {
            created_at_seq.clone()
        };

        // Apply deletion mask if present (positions are local offsets)
        if let Some(deletion_file) = &frag.deletion_file {
            let deletions = read_dataset_deletion_file(dataset, frag.id, deletion_file).await?;
            last_updated_seq.mask(deletions.to_sorted_iter())?;
            created_at_seq.mask(deletions.to_sorted_iter())?;
        }

        old_last_updated_sequences.push(last_updated_seq);
        old_created_at_sequences.push(created_at_seq);
    }

    // Ensure row counts match new fragments total
    let old_total: u64 = old_last_updated_sequences.iter().map(|s| s.len()).sum();
    let new_total: u64 = new_fragments
        .iter()
        .map(|f| f.physical_rows.unwrap_or(0) as u64)
        .sum();
    debug_assert_eq!(old_total, new_total);

    // Rechunk version runs aligned to new fragment sizes
    let chunk_sizes: Vec<u64> = new_fragments
        .iter()
        .map(|f| f.physical_rows.unwrap_or(0) as u64)
        .collect();

    let new_last_updated_sequences = lance_table::rowids::version::rechunk_version_sequences(
        old_last_updated_sequences,
        chunk_sizes.clone(),
        false,
    )?;

    let new_created_at_sequences = lance_table::rowids::version::rechunk_version_sequences(
        old_created_at_sequences,
        chunk_sizes,
        false,
    )?;

    // Set both version metadata on new fragments
    for ((fragment, last_updated_seq), created_at_seq) in new_fragments
        .iter_mut()
        .zip(new_last_updated_sequences)
        .zip(new_created_at_sequences)
    {
        fragment.last_updated_at_version_meta = Some(
            lance_table::format::RowDatasetVersionMeta::from_sequence(&last_updated_seq).unwrap(),
        );
        fragment.created_at_version_meta = Some(
            lance_table::format::RowDatasetVersionMeta::from_sequence(&created_at_seq).unwrap(),
        );
    }

    Ok(())
}

/// Commit the results of file compaction.
///
/// It is not required that all tasks are passed to this method. If some failed,
/// they can be omitted and the successful tasks can be committed. However, once
/// some of the tasks have been committed, the remainder of the tasks will not
/// be able to be committed and should be considered cancelled.
pub async fn commit_compaction(
    dataset: &mut Dataset,
    completed_tasks: Vec<RewriteResult>,
    remap_options: Arc<dyn IndexRemapperOptions>,
    options: &CompactionOptions,
) -> Result<CompactionMetrics> {
    if completed_tasks.is_empty() {
        return Ok(CompactionMetrics::default());
    }

    let has_address_style = completed_tasks.iter().any(|t| t.row_addrs.is_some());
    // Address-style results require immediate index remapping unless it is deferred.
    let needs_remapping =
        !dataset.manifest.uses_stable_row_ids() && !options.defer_index_remap && has_address_style;

    // Confirm there is a remapper before materializing the potentially very large row address map.
    let index_remapper = if needs_remapping {
        remap_options.create_remapper(dataset).await?
    } else {
        None
    };

    // Determine the earliest version at which compaction tasks were planned/executed.
    //
    // In distributed mode (e.g. Spark) the caller opens *two separate* Dataset
    // handles: one for `plan_compaction` (at version V) and a fresh one for
    // `commit_compaction` (at the latest version V+N).  Using `dataset.manifest.version`
    // (= V+N) as the transaction's `read_version` would cause the conflict checker to
    // scan only versions after V+N — finding nothing — and therefore silently skip any
    // concurrent DELETE/UPDATE that landed between V and V+N, resurrecting deleted rows.
    //
    // By anchoring `read_version` to the minimum version carried in the RewriteResults
    // we ensure the conflict checker covers the full range [V, V+N] and will reject the
    // commit with a retryable conflict error if a concurrent write touched the same
    // fragments.
    let tasks_read_version = completed_tasks
        .iter()
        .map(|t| t.read_version)
        .min()
        .unwrap_or(dataset.manifest.version);

    let mut completed_tasks = completed_tasks;

    // Collect the rewritten fragments' file paths up front so every failure
    // path below can clean them up (or deliberately keep them). Fragment ids
    // may still be reassigned by reserve_fragment_ids; cleanup only needs the
    // file paths, which never change.
    let all_new_fragments: Vec<Fragment> = completed_tasks
        .iter()
        .flat_map(|t| t.new_fragments.iter().cloned())
        .collect();

    // Single reserve_fragment_ids for all address-style tasks
    if has_address_style {
        let frags: Vec<&mut Fragment> = completed_tasks
            .iter_mut()
            .filter(|t| t.row_addrs.is_some())
            .flat_map(|t| t.new_fragments.iter_mut())
            .collect();
        if let Err(e) = reserve_fragment_ids(dataset, frags.into_iter()).await {
            cleanup_compaction_files_after_reservation_failure(dataset, &all_new_fragments).await;
            return Err(e);
        }
    }

    let mut rewrite_groups = Vec::with_capacity(completed_tasks.len());
    let mut metrics = CompactionMetrics::default();

    let mut remap_group_inputs: Vec<GroupInput> = Vec::new();
    let mut direct_row_id_map: HashMap<u64, Option<u64>> = HashMap::default();
    let mut frag_reuse_groups: Vec<FragReuseGroup> = Vec::new();
    let mut new_fragment_bitmap: RoaringBitmap = RoaringBitmap::new();

    // Write an FRI only when the compaction touches data an index must later
    // remap: a rewrite group covered by a data index, or by the existing FRI's new
    // fragments (the composed remap chain). Compacting only not-yet-indexed data
    // needs no FRI (one written for it is un-drainable). Decide all-or-nothing per
    // compaction, never per group -- a partial FRI is unsound: a concurrent reindex
    // can make a skipped fragment indexed and the conflict resolver's FRI-present
    // path won't re-check it.
    let indexed_frags: RoaringBitmap = if options.defer_index_remap {
        let mut covered = RoaringBitmap::new();
        for bm in load_index_fragmaps(dataset).await? {
            covered |= bm;
        }
        if let Some(bm) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await?
            .and_then(|fri| fri.fragment_bitmap)
        {
            covered |= bm;
        }
        covered
    } else {
        RoaringBitmap::new()
    };
    let mut any_group_indexed = false;

    for task in completed_tasks {
        metrics += task.metrics;
        let rewrite_group = RewriteGroup {
            old_fragments: task.original_fragments.clone(),
            new_fragments: task.new_fragments.clone(),
        };

        if index_remapper.is_some() {
            if let Some(row_addrs_bytes) = task.row_addrs {
                let row_addrs =
                    RoaringTreemap::deserialize_from(&mut Cursor::new(&row_addrs_bytes))?;
                match options.index_remap_mode {
                    IndexRemapMode::Direct => {
                        let transposed = remapping::transpose_row_addrs(
                            row_addrs,
                            &task.original_fragments,
                            &task.new_fragments,
                        );
                        direct_row_id_map.extend(transposed);
                    }
                    IndexRemapMode::Compact => {
                        let new_frags = task
                            .new_fragments
                            .iter()
                            .map(|f| {
                                let physical_rows = f.physical_rows.ok_or_else(|| {
                                    Error::invalid_input(format!(
                                        "compacted fragment {} is missing physical_rows",
                                        f.id
                                    ))
                                })?;
                                Ok((f.id as u32, physical_rows as u32))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        remap_group_inputs.push(GroupInput {
                            rewritten_old_row_addrs: row_addrs,
                            old_frag_ids: task
                                .original_fragments
                                .iter()
                                .map(|f| f.id as u32)
                                .collect(),
                            new_frags,
                        });
                    }
                }
            }
        } else if options.defer_index_remap {
            // Record every group; track whether any touches indexed/chain data.
            if task
                .original_fragments
                .iter()
                .any(|f| indexed_frags.contains(f.id as u32))
            {
                any_group_indexed = true;
            }
            let changed_row_addrs = task.row_addrs.ok_or_else(|| {
                Error::internal(
                    "defer_index_remap requires row_addrs but none were provided".to_string(),
                )
            })?;
            frag_reuse_groups.push(FragReuseGroup {
                changed_row_addrs,
                old_frags: task.original_fragments.iter().map(|f| f.into()).collect(),
                new_frags: task.new_fragments.iter().map(|f| f.into()).collect(),
            });

            task.new_fragments.iter().for_each(|frag| {
                new_fragment_bitmap.insert(frag.id as u32);
            });
        }
        rewrite_groups.push(rewrite_group);
    }

    let rewritten_indices = if let Some(index_remapper) = index_remapper {
        let affected_ids = rewrite_groups
            .iter()
            .flat_map(|group| group.old_fragments.iter().map(|frag| frag.id))
            .collect::<Vec<_>>();

        let remap = match options.index_remap_mode {
            IndexRemapMode::Direct => RowAddrRemap::direct(direct_row_id_map),
            IndexRemapMode::Compact => RowAddrRemap::compact(remap_group_inputs)?,
        };
        let remapped_indices = index_remapper.remap_indices(remap, &affected_ids).await?;
        remapped_indices
            .into_iter()
            .map(|rewritten| RewrittenIndex {
                old_id: rewritten.old_id,
                new_id: rewritten.new_id,
                new_index_details: rewritten.index_details,
                new_index_version: rewritten.index_version,
                new_index_files: rewritten.files,
            })
            .collect()
    } else if !options.defer_index_remap && !has_address_style {
        // We need to reserve fragment ids here so that the fragment bitmap
        // can be updated for each index. Only needed for stable row IDs
        // since address-style IDs were already reserved above.
        let new_fragments = rewrite_groups
            .iter_mut()
            .flat_map(|group| group.new_fragments.iter_mut())
            .collect::<Vec<_>>();
        if let Err(e) = reserve_fragment_ids(dataset, new_fragments.into_iter()).await {
            cleanup_compaction_files_after_reservation_failure(dataset, &all_new_fragments).await;
            return Err(e);
        }
        Vec::new()
    } else {
        Vec::new()
    };

    // No indexed/chain data touched -> no FRI (all-or-nothing, see above).
    let frag_reuse_index = if options.defer_index_remap && any_group_indexed {
        Some(build_new_frag_reuse_index(dataset, frag_reuse_groups, new_fragment_bitmap).await?)
    } else {
        if options.defer_index_remap {
            log::debug!(
                "skipping fragment-reuse index: no rewritten fragments were covered by an index"
            );
        }
        None
    };

    let transaction = TransactionBuilder::new(
        // Use the version at which the compaction tasks were *planned*, not the
        // version of the dataset handle passed to this function.  In distributed
        // mode the caller may open a fresh dataset at a later version (V+N), but
        // the tasks were executed against an older snapshot (V).  Anchoring the
        // transaction to V ensures the OCC conflict checker scans all writes that
        // landed between V and the commit point, detecting concurrent DELETE
        // transactions that would otherwise cause deleted rows to reappear.
        tasks_read_version,
        Operation::Rewrite {
            groups: rewrite_groups,
            rewritten_indices,
            frag_reuse_index,
        },
    )
    .transaction_properties(options.transaction_properties.clone())
    .build();

    if let Err(e) = dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await
    {
        // RewriteResult is serializable and may be retried after an earlier
        // ambiguous success. A conflict on this call therefore does not prove
        // that the rewritten files are unreferenced. Leave them for dataset GC.
        log::warn!(
            "Compaction commit failed; leaving {} rewritten fragment(s) in place for GC: {}",
            all_new_fragments.len(),
            e
        );
        return Err(e);
    }

    Ok(metrics)
}

/// Remove rewritten files after fragment-id reservation fails. Reservation
/// commits do not reference the rewritten files, so they are still owned by
/// this uncommitted compaction attempt and are safe to delete.
async fn cleanup_compaction_files_after_reservation_failure(
    dataset: &Dataset,
    all_new_fragments: &[Fragment],
) {
    cleanup_data_fragments(
        &dataset.object_store,
        &dataset.base,
        None,
        all_new_fragments,
    )
    .await;
}

#[cfg(test)]
mod tests {

    mod binary_copy;
    use self::remapping::RemappedIndex;
    use super::*;
    use crate::dataset::WriteDestination;
    use crate::dataset::index::frag_reuse::cleanup_frag_reuse_index;
    use crate::dataset::optimize::remapping::{transpose_row_addrs, transpose_row_ids_from_digest};
    use crate::index::frag_reuse::{load_frag_reuse_index_details, open_frag_reuse_index};
    use crate::index::vector::{StageParams, VectorIndexParams};
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
    use arrow_array::{
        ArrayRef, Float32Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        PrimitiveArray, RecordBatch, RecordBatchIterator,
    };
    use arrow_schema::{DataType, Field, Schema};
    use arrow_select::concat::concat_batches;
    use async_trait::async_trait;
    use lance_arrow::BLOB_META_KEY;
    use lance_core::Error;
    use lance_core::ROW_ID;
    use lance_core::utils::address::RowAddress;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::Dimension;
    use lance_file::version::LanceFileVersion;
    use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;
    use lance_index::frag_reuse::FragReuseIndexHandle;
    use lance_index::scalar::{
        BuiltinIndexType, FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams,
    };
    use lance_index::vector::ivf::IvfBuildParams;
    use lance_index::vector::pq::PQBuildParams;
    use lance_index::{Index, IndexType};
    use lance_linalg::distance::{DistanceType, MetricType};
    use lance_table::io::manifest::read_manifest_indexes;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use rstest::rstest;
    use std::collections::HashSet;
    use std::io::Cursor;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn test_candidate_bin() {
        let empty_bin = CandidateBin {
            fragments: vec![],
            pos_range: 0..0,
            candidacy: vec![],
            row_counts: vec![],
            indices: vec![],
        };
        assert!(empty_bin.is_noop());

        let fragment = Fragment {
            id: 0,
            files: vec![],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(0),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };
        let single_bin = CandidateBin {
            fragments: vec![fragment.clone()],
            pos_range: 0..1,
            candidacy: vec![CompactionCandidacy::CompactWithNeighbors],
            row_counts: vec![100],
            indices: vec![],
        };
        assert!(single_bin.is_noop());

        let single_bin = CandidateBin {
            fragments: vec![fragment.clone()],
            pos_range: 0..1,
            candidacy: vec![CompactionCandidacy::CompactItself],
            row_counts: vec![100],
            indices: vec![],
        };
        // Not a no-op because it's CompactItself
        assert!(!single_bin.is_noop());

        let big_bin = CandidateBin {
            fragments: std::iter::repeat_n(fragment, 8).collect(),
            pos_range: 0..8,
            candidacy: std::iter::repeat_n(CompactionCandidacy::CompactItself, 8).collect(),
            row_counts: vec![100, 400, 200, 200, 400, 300, 300, 100],
            indices: vec![],
            // Will group into: [[100, 400], [200, 200, 400], [300, 300, 100]]
            // with size = 500
        };
        assert!(!big_bin.is_noop());
        let split = big_bin.split_for_size(500);
        assert_eq!(split.len(), 3);
        assert_eq!(split[0].pos_range, 0..2);
        assert_eq!(split[1].pos_range, 2..5);
        assert_eq!(split[2].pos_range, 5..8);

        let zero_min_split_bin = CandidateBin {
            fragments: std::iter::repeat_n(
                Fragment {
                    id: 0,
                    files: vec![],
                    overlays: vec![],
                    deletion_file: None,
                    row_id_meta: None,
                    physical_rows: Some(0),
                    last_updated_at_version_meta: None,
                    created_at_version_meta: None,
                },
                3,
            )
            .collect(),
            pos_range: 0..3,
            candidacy: std::iter::repeat_n(CompactionCandidacy::CompactItself, 3).collect(),
            row_counts: vec![100, 200, 300],
            indices: vec![],
        };
        let split = zero_min_split_bin.split_for_size(0);
        assert_eq!(split.len(), 3);
        assert!(split.iter().all(|bin| !bin.fragments.is_empty()));
        assert_eq!(split[0].pos_range, 0..1);
        assert_eq!(split[1].pos_range, 1..2);
        assert_eq!(split[2].pos_range, 2..3);
    }

    fn sample_data() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from_iter_values(0..10_000))],
        )
        .unwrap()
    }

    /// Build (or, with `replace`, rebuild) a scalar index named "scalar" on `col`.
    async fn create_scalar_index(dataset: &mut Dataset, col: &str, replace: bool) {
        dataset
            .create_index(
                &[col],
                IndexType::Scalar,
                Some("scalar".into()),
                &ScalarIndexParams::default(),
                replace,
            )
            .await
            .unwrap();
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    struct MockIndexRemapperExpectation {
        expected: HashMap<u64, Option<u64>>,
        answer: Vec<RemappedIndex>,
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    struct MockIndexRemapper {
        expectations: Vec<MockIndexRemapperExpectation>,
    }

    impl MockIndexRemapper {
        fn stringify_map(map: &HashMap<u64, Option<u64>>) -> String {
            let mut sorted_keys = map.keys().collect::<Vec<_>>();
            sorted_keys.sort();
            let mut first_keys = sorted_keys
                .into_iter()
                .take(10)
                .map(|key| {
                    format!(
                        "{}:{:?}",
                        RowAddress::from(*key),
                        map[key].map(RowAddress::from)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            if map.len() > 10 {
                first_keys.push_str(", ...");
            }
            let mut result_str = format!("(len={})", map.len());
            result_str.push_str(&first_keys);
            result_str
        }

        fn in_any_order(expectations: &[Self]) -> Self {
            let expectations = expectations
                .iter()
                .flat_map(|item| item.expectations.clone())
                .collect::<Vec<_>>();
            Self { expectations }
        }
    }

    #[async_trait]
    impl IndexRemapper for MockIndexRemapper {
        async fn remap_indices(
            &self,
            index_map: RowAddrRemap,
            _: &[u64],
        ) -> Result<Vec<RemappedIndex>> {
            for expectation in &self.expectations {
                let matches = match &index_map {
                    RowAddrRemap::Direct(map) => map == &expectation.expected,
                    RowAddrRemap::Compact(_) => {
                        let expected_frags: RoaringBitmap = expectation
                            .expected
                            .keys()
                            .map(|addr| (addr >> 32) as u32)
                            .collect();
                        index_map.affected_fragments() == expected_frags
                            && expectation
                                .expected
                                .iter()
                                .all(|(k, v)| index_map.get(*k) == Some(*v))
                    }
                };
                if matches {
                    return Ok(expectation.answer.clone());
                }
            }
            panic!(
                "Unexpected index map; expected one of:\n  {}",
                self.expectations
                    .iter()
                    .map(|expectation| Self::stringify_map(&expectation.expected))
                    .collect::<Vec<_>>()
                    .join("\n  ")
            );
        }
    }

    #[async_trait]
    impl IndexRemapperOptions for MockIndexRemapper {
        async fn create_remapper(&self, _: &Dataset) -> Result<Option<Box<dyn IndexRemapper>>> {
            Ok(Some(Box::new(self.clone())))
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_empty(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Compact an empty table
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        let reader = RecordBatchIterator::new(vec![].into_iter().map(Ok), Arc::new(schema));
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let plan = plan_compaction(&dataset, &CompactionOptions::default())
            .await
            .unwrap();
        assert_eq!(plan.tasks().len(), 0);

        let metrics = compact_files(&mut dataset, CompactionOptions::default(), None)
            .await
            .unwrap();

        assert_eq!(metrics, CompactionMetrics::default());
        assert_eq!(dataset.manifest.version, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_all_good(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        // Compact a table with nothing to do
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        // Just one file
        let write_params = WriteParams {
            max_rows_per_file: 10_000,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // There's only one file, so we can't compact any more if we wanted to.
        let plan = plan_compaction(&dataset, &CompactionOptions::default())
            .await
            .unwrap();
        assert_eq!(plan.tasks().len(), 0);

        // Now split across multiple files
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 3_000,
            max_rows_per_group: 1_000,
            data_storage_version: Some(data_storage_version),
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 3_000,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 0);
    }

    fn list_data_files(uri: &str) -> std::collections::BTreeSet<String> {
        std::fs::read_dir(std::path::Path::new(uri).join("data"))
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn execute_compaction_plan(
        dataset: &Dataset,
        options: &CompactionOptions,
    ) -> Vec<RewriteResult> {
        let plan = plan_compaction(dataset, options).await.unwrap();
        assert!(!plan.tasks.is_empty());
        let snapshot = dataset.clone();
        futures::stream::iter(plan.tasks)
            .map(|task| rewrite_files(Cow::Borrowed(&snapshot), task, options))
            .buffer_unordered(1)
            .try_collect()
            .await
            .unwrap()
    }

    /// When the compaction commit's status is unknown (the commit errored and
    /// verification was unavailable), the rewritten files must NOT be deleted:
    /// if the commit landed they are referenced by the new version, and
    /// deleting them would corrupt the table.
    #[tokio::test]
    async fn test_compaction_retry_after_ambiguous_success_preserves_live_files() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let data = sample_data();
        let num_rows = data.num_rows();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 3_000,
            enable_stable_row_ids: true,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();
        let files_before = list_data_files(test_uri);
        let options = CompactionOptions::default();
        let completed = execute_compaction_plan(&dataset, &options).await;
        let serialized = serde_json::to_vec(&completed).unwrap();
        let retry_results: Vec<RewriteResult> = serde_json::from_slice(&serialized).unwrap();
        let rewritten_files = list_data_files(test_uri)
            .difference(&files_before)
            .cloned()
            .collect::<Vec<_>>();
        assert!(!rewritten_files.is_empty());

        handler.fail_next_rewrite(AmbiguousFailure::LandAndError);
        handler
            .fail_resolve
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = commit_compaction(
            &mut dataset,
            completed,
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .expect_err("unknown commit status must surface as an error");
        assert!(
            err.is_commit_status_unknown(),
            "expected CommitStatusUnknown, got: {:?}",
            err
        );

        handler
            .fail_resolve
            .store(false, std::sync::atomic::Ordering::SeqCst);

        // Retrying the same serialized RewriteResult now conflicts with the
        // Rewrite that already landed. The retry must not delete those files.
        let retry_error = commit_compaction(
            &mut dataset,
            retry_results,
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .expect_err("the replayed rewrite must conflict with its landed predecessor");
        assert!(
            matches!(retry_error, Error::RetryableCommitConflict { .. }),
            "expected RetryableCommitConflict, got: {retry_error:?}"
        );
        let files_after_retry = list_data_files(test_uri);
        assert!(
            rewritten_files
                .iter()
                .all(|file| files_after_retry.contains(file)),
            "a replayed RewriteResult must not delete files referenced by the landed rewrite"
        );

        let ds = Dataset::open(test_uri).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), num_rows);
        let scanned = ds.scan().try_into_batch().await.unwrap();
        assert_eq!(scanned.num_rows(), num_rows);
    }

    /// A failed ReserveFragments commit cannot reference the rewritten files,
    /// so both stable-row-id reservation paths must still clean them up.
    #[tokio::test]
    async fn test_compaction_cleans_up_files_when_fragment_reservation_fails() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let data = sample_data();
        let num_rows = data.num_rows();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 3_000,
            enable_stable_row_ids: true,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();
        let files_before = list_data_files(test_uri);
        let options = CompactionOptions::default();
        let completed = execute_compaction_plan(&dataset, &options).await;
        assert!(list_data_files(test_uri).len() > files_before.len());

        handler.fail_next_reserve(AmbiguousFailure::FailOutright);
        let err = commit_compaction(
            &mut dataset,
            completed,
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .expect_err("fragment reservation that did not land must fail");
        assert!(
            !err.is_commit_status_unknown(),
            "a verified-absent reservation is a definite failure, got: {:?}",
            err
        );

        let files_after = list_data_files(test_uri);
        let leftover: Vec<_> = files_after.difference(&files_before).collect();
        assert!(
            leftover.is_empty(),
            "rewritten files must be cleaned up after reservation fails; leftover: {:?}",
            leftover
        );
        let ds = Dataset::open(test_uri).await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), num_rows);
    }

    #[tokio::test]
    async fn test_compact_blob_columns() {
        let test_dir = TempStrDir::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("blob", DataType::LargeBinary, false)
                .with_metadata([(BLOB_META_KEY.to_string(), "true".to_string())].into()),
        ]));
        let expected_payload: Vec<Vec<u8>> =
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9, 10], vec![11]];
        let id_column: ArrayRef = Arc::new(Int32Array::from_iter_values(
            0..expected_payload.len() as i32,
        ));
        let blob_array: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            expected_payload.iter().map(|value| Some(value.as_slice())),
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_column, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir,
            Some(WriteParams {
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.validate().await.unwrap();
        assert!(dataset.get_fragments().len() > 1);

        compact_files(&mut dataset, CompactionOptions::default(), None)
            .await
            .unwrap();
        dataset.validate().await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 1);

        let dataset = Arc::new(dataset);
        let row_indices: Vec<u64> = (0..expected_payload.len() as u64).collect();
        let blobs = dataset
            .take_blobs_by_indices(&row_indices, "blob")
            .await
            .unwrap();
        assert_eq!(blobs.len(), expected_payload.len());
        for (blob, expected) in blobs.iter().zip(expected_payload.iter()) {
            let bytes = blob.as_ref().unwrap().read().await.unwrap();
            assert_eq!(bytes.as_ref(), expected.as_slice());
        }
    }

    fn row_addrs(frag_idx: u32, offsets: Range<u32>) -> Range<u64> {
        let start = RowAddress::new_from_parts(frag_idx, offsets.start);
        let end = RowAddress::new_from_parts(frag_idx, offsets.end);
        start.into()..end.into()
    }

    // The outer list has one item per new fragment
    // The inner list has ranges of old row ids that map to the new fragment, in order
    fn expect_remap(
        ranges: &[Vec<(Range<u64>, bool)>],
        starting_new_frag_idx: u32,
    ) -> MockIndexRemapper {
        let mut expected_remap: HashMap<u64, Option<u64>> = HashMap::default();
        expected_remap.reserve(ranges.iter().map(|r| r.len()).sum());
        for (new_frag_offset, new_frag_ranges) in ranges.iter().enumerate() {
            let new_frag_idx = starting_new_frag_idx + new_frag_offset as u32;
            let mut row_offset = 0;
            for (old_id_range, is_found) in new_frag_ranges.iter() {
                for old_id in old_id_range.clone() {
                    if *is_found {
                        let new_id = RowAddress::new_from_parts(new_frag_idx, row_offset);
                        expected_remap.insert(old_id, Some(new_id.into()));
                        row_offset += 1;
                    } else {
                        expected_remap.insert(old_id, None);
                    }
                }
            }
        }
        MockIndexRemapper {
            expectations: vec![MockIndexRemapperExpectation {
                expected: expected_remap,
                answer: vec![],
            }],
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_many(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();

        // Create a table with 3 small fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 1200))], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 400,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Append 2 large fragments (1k rows)
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(1200, 2000))], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 1000,
            data_storage_version: Some(data_storage_version),
            mode: WriteMode::Append,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Delete 1 row from first large fragment
        dataset.delete("a = 1300").await.unwrap();

        // Delete 20% of rows from second large fragment
        dataset.delete("a >= 2400 AND a < 2600").await.unwrap();

        // Append 2 small fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(3200, 600))], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 300,
            data_storage_version: Some(data_storage_version),
            mode: WriteMode::Append,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        let first_new_frag_idx = 7;
        // Predicting the remap is difficult.  One task will remap to fragments 7/8 and the other
        // will remap to fragments 9/10 but we don't know which is which and so we just allow ourselves
        // to expect both possibilities.
        let remap_a = expect_remap(
            &[
                vec![
                    // 3 small fragments are rewritten to frags 7 & 8
                    (row_addrs(0, 0..400), true),
                    (row_addrs(1, 0..400), true),
                    (row_addrs(2, 0..200), true),
                ],
                vec![(row_addrs(2, 200..400), true)],
                // frag 3 is skipped since it does not have enough missing data
                // Frags 4, 5, and 6 are rewritten to frags 9 & 10
                vec![
                    // Only 800 of the 1000 rows taken from frag 4
                    (row_addrs(4, 0..200), true),
                    (row_addrs(4, 200..400), false),
                    (row_addrs(4, 400..1000), true),
                    // frags 5 compacted with frag 4
                    (row_addrs(5, 0..200), true),
                ],
                vec![(row_addrs(5, 200..300), true), (row_addrs(6, 0..300), true)],
            ],
            first_new_frag_idx,
        );
        let remap_b = expect_remap(
            &[
                // Frags 4, 5, and 6 are rewritten to frags 7 & 8
                vec![
                    (row_addrs(4, 0..200), true),
                    (row_addrs(4, 200..400), false),
                    (row_addrs(4, 400..1000), true),
                    (row_addrs(5, 0..200), true),
                ],
                vec![(row_addrs(5, 200..300), true), (row_addrs(6, 0..300), true)],
                // 3 small fragments rewritten to frags 9 & 10
                vec![
                    (row_addrs(0, 0..400), true),
                    (row_addrs(1, 0..400), true),
                    (row_addrs(2, 0..200), true),
                ],
                vec![(row_addrs(2, 200..400), true)],
            ],
            first_new_frag_idx,
        );

        // Create compaction plan
        let options = CompactionOptions {
            target_rows_per_fragment: 1000,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 2);
        assert_eq!(plan.tasks()[0].fragments.len(), 3);
        assert_eq!(plan.tasks()[1].fragments.len(), 3);

        assert_eq!(
            plan.tasks()[0]
                .fragments
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            plan.tasks()[1]
                .fragments
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );

        let mock_remapper = MockIndexRemapper::in_any_order(&[remap_a, remap_b]);

        // Run compaction
        let metrics = compact_files(&mut dataset, options, Some(Arc::new(mock_remapper)))
            .await
            .unwrap();

        // Assert on metrics
        assert_eq!(metrics.fragments_removed, 6);
        assert_eq!(metrics.fragments_added, 4);
        assert_eq!(metrics.files_removed, 7); // 6 data files + 1 deletion file
        assert_eq!(metrics.files_added, 4);

        let fragment_ids = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id())
            .collect::<Vec<_>>();
        assert_eq!(fragment_ids, vec![3, 7, 8, 9, 10]);
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_data_files(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();

        // Create a table with 2 small fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 5_000,
            max_rows_per_group: 1_000,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Add a column
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("x", DataType::Float32, false),
        ]);

        let data = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..10_000)),
                Arc::new(Float32Array::from_iter_values(
                    (0..10_000).map(|x| x as f32 * std::f32::consts::PI),
                )),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());

        dataset.merge(reader, "a", "a").await.unwrap();

        let expected_remap = expect_remap(
            &[vec![
                // 3 small fragments are rewritten entirely
                (row_addrs(0, 0..5000), true),
                (row_addrs(1, 0..5000), true),
            ]],
            2,
        );

        let plan = plan_compaction(
            &dataset,
            &CompactionOptions {
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(plan.tasks().len(), 1);
        assert_eq!(plan.tasks()[0].fragments.len(), 2);

        let metrics = compact_files(&mut dataset, plan.options, Some(Arc::new(expected_remap)))
            .await
            .unwrap();

        assert_eq!(metrics.files_removed, 4); // 2 fragments with 2 data files
        assert_eq!(metrics.files_added, 1); // 1 fragment with 1 data file
        assert_eq!(metrics.fragments_removed, 2);
        assert_eq!(metrics.fragments_added, 1);

        // Assert order unchanged and data is all there.
        let scanner = dataset.scan();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let scanned_data = concat_batches(&batches[0].schema(), &batches).unwrap();

        assert_eq!(scanned_data, data);
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_with_io_buffer_size(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        // Compaction should succeed and produce correct results when an
        // explicit io_buffer_size is provided via CompactionOptions.
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();

        // Create a table with 2 small fragments so there is something to compact.
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 5_000,
            max_rows_per_group: 1_000,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 2);

        let options = CompactionOptions {
            // A generous buffer so the read does not deadlock on large batches.
            io_buffer_size: Some(256 * 1024 * 1024),
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 1);

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert_eq!(metrics.fragments_removed, 2);
        assert_eq!(metrics.fragments_added, 1);

        // All rows are preserved after compaction.
        let scanner = dataset.scan();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let scanned_data = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(scanned_data.num_rows(), data.num_rows());
    }

    #[rstest]
    #[tokio::test]
    async fn test_compact_deletions(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        // For files that have few rows, we don't want to compact just 1 since
        // that won't do anything. But if there are deletions to materialize,
        // we want to do groups of 1. This test checks that.
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();

        // Create a table with 1 fragment
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 1000))], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 1000,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        dataset.delete("a <= 500").await.unwrap();

        // Threshold must be satisfied
        let mut options = CompactionOptions {
            materialize_deletions_threshold: 0.8,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 0);

        // Ignore deletions if materialize_deletions is false
        options.materialize_deletions_threshold = 0.1;
        options.materialize_deletions = false;
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 0);

        // Materialize deletions if threshold is met
        options.materialize_deletions = true;
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 1);

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert_eq!(metrics.fragments_removed, 1);
        assert_eq!(metrics.files_removed, 2);
        assert_eq!(metrics.fragments_added, 1);

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 1);
        assert!(fragments[0].metadata.deletion_file.is_none());
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct IgnoreRemap {}

    #[async_trait]
    impl IndexRemapper for IgnoreRemap {
        async fn remap_indices(&self, _: RowAddrRemap, _: &[u64]) -> Result<Vec<RemappedIndex>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl IndexRemapperOptions for IgnoreRemap {
        async fn create_remapper(&self, _: &Dataset) -> Result<Option<Box<dyn IndexRemapper>>> {
            Ok(None)
        }
    }

    #[rstest]
    #[case::without_index(false)]
    #[case::with_index(true)]
    #[tokio::test]
    async fn test_row_addrs_only_used_with_remappable_index(#[case] has_index: bool) {
        let data = sample_data();
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 9_000))], data.schema());
        let mut dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                max_rows_per_file: 3_000,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        if has_index {
            create_scalar_index(&mut dataset, "a", false).await;
        }

        let options = CompactionOptions {
            target_rows_per_fragment: 9_000,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 1);

        let mut result = rewrite_files(Cow::Borrowed(&dataset), plan.tasks()[0].clone(), &options)
            .await
            .unwrap();
        assert_eq!(result.row_addrs.is_some(), has_index);

        if has_index {
            let row_addrs_bytes = result
                .row_addrs
                .as_ref()
                .expect("indexed compaction should capture row addresses");
            let row_addrs =
                RoaringTreemap::deserialize_from(&mut Cursor::new(row_addrs_bytes)).unwrap();
            assert_eq!(row_addrs.len(), 9_000);
        } else {
            // Simulate a stale worker result that captured row addresses before the
            // dataset no longer needed a remapper. Invalid bytes ensure the commit
            // does not attempt to deserialize or materialize the unused map.
            result.row_addrs = Some(b"not a roaring treemap".to_vec());
            commit_compaction(
                &mut dataset,
                vec![result],
                Arc::new(DatasetIndexRemapperOptions::default()),
                &options,
            )
            .await
            .unwrap();
            assert_eq!(dataset.get_fragments().len(), 1);
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_compact_distributed(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
        #[values(false, true)] use_stable_row_id: bool,
    ) {
        // Can run the tasks independently
        // Can provide subset of tasks to commit_compaction
        // Once committed, can't commit remaining tasks
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();

        // Write dataset as 9 1k row fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 9000))], data.schema());
        let write_params = WriteParams {
            max_rows_per_file: 1000,
            data_storage_version: Some(data_storage_version),
            enable_stable_row_ids: use_stable_row_id,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Plan compaction with 3 tasks
        let options = CompactionOptions {
            target_rows_per_fragment: 3_000,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 3);

        let dataset_ref = &dataset;
        let mut results = futures::stream::iter(plan.compaction_tasks())
            .then(|task| async move { task.execute(dataset_ref).await.unwrap() })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(results.len(), 3);

        assert_eq!(
            results[0]
                .original_fragments
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(results[0].metrics.files_removed, 3);
        assert_eq!(results[0].metrics.files_added, 1);

        // Just commit the last task
        commit_compaction(
            &mut dataset,
            vec![results.pop().unwrap()],
            Arc::new(IgnoreRemap::default()),
            &options,
        )
        .await
        .unwrap();

        // 1 commit for reserve fragments and 1 for final commit, both
        // from the call to commit_compaction
        assert_eq!(dataset.manifest.version, 3);

        // Can commit the remaining tasks
        commit_compaction(
            &mut dataset,
            results,
            Arc::new(IgnoreRemap::default()),
            &options,
        )
        .await
        .unwrap();
        // 1 commit for reserve fragments and 1 for final commit, both
        // from the call to commit_compaction
        assert_eq!(dataset.manifest.version, 5);

        assert_eq!(dataset.manifest.uses_stable_row_ids(), use_stable_row_id,);
    }

    #[tokio::test]
    async fn test_stable_row_indices() {
        // Validate behavior of indices after compaction with stable row ids.
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(16).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));
        let mut dataset = Dataset::write(
            data_gen.batch(500),
            "memory://test/table",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 100, // 5 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Delete first 110 rows so rowids != final rowaddrs
        // First 100 rows deletes first file. Next 10 deletes part of second
        // file, so we will trigger the with deletions code path.
        dataset.delete("i < 110").await.unwrap();

        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                Some("scalar".into()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let params = VectorIndexParams::ivf_pq(1, 8, 1, MetricType::L2, 50);
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vector".into()),
                &params,
                false,
            )
            .await
            .unwrap();

        async fn index_set(dataset: &Dataset) -> HashSet<Uuid> {
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .map(|index| index.uuid)
                .collect()
        }
        let indices = index_set(&dataset).await;

        async fn vector_query(dataset: &Dataset) -> RecordBatch {
            let mut scanner = dataset.scan();

            let query = Float32Array::from(vec![0.0f32; 16]);
            scanner
                .nearest("vec", &query, 10)
                .unwrap()
                .project(&["i"])
                .unwrap();

            scanner.try_into_batch().await.unwrap()
        }

        async fn scalar_query(dataset: &Dataset) -> RecordBatch {
            let mut scanner = dataset.scan();

            scanner.filter("i = 100").unwrap().project(&["i"]).unwrap();

            scanner.try_into_batch().await.unwrap()
        }

        let before_vec_result = vector_query(&dataset).await;
        let before_scalar_result = scalar_query(&dataset).await;

        let options = CompactionOptions {
            target_rows_per_fragment: 180,
            ..Default::default()
        };
        let _metrics = compact_files(&mut dataset, options, None).await.unwrap();

        // The indices should be unchanged after compaction, since we are using
        // stable row ids.
        let current_indices = index_set(&dataset).await;
        assert_eq!(indices, current_indices);

        let after_vec_result = vector_query(&dataset).await;
        assert_eq!(before_vec_result, after_vec_result);

        let after_scalar_result = scalar_query(&dataset).await;
        assert_eq!(before_scalar_result, after_scalar_result);
    }

    /// Regression test for https://github.com/lance-format/lance/issues/8076
    ///
    /// A zone map or bloom filter index reports matches as physical row addresses, so
    /// compaction invalidates it even under stable row ids. Reusing it for the rewritten
    /// fragments made a filtered scan fail with an internal error (a fragment referenced
    /// by the index no longer existed) or, once translation tolerated that, silently drop
    /// every match.
    #[rstest]
    #[case::zone_map(BuiltinIndexType::ZoneMap, IndexType::ZoneMap)]
    #[case::bloom_filter(BuiltinIndexType::BloomFilter, IndexType::BloomFilter)]
    #[tokio::test]
    async fn test_addr_domain_index_after_compaction_with_stable_row_ids(
        #[case] builtin: BuiltinIndexType,
        #[case] index_type: IndexType,
    ) {
        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("i".to_owned())));
        let mut dataset = Dataset::write(
            data_gen.batch(200),
            "memory://test/table",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 100, // 2 fragments, so compaction has something to merge
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        dataset
            .create_index(
                &["i"],
                index_type,
                None,
                &ScalarIndexParams::for_builtin(builtin),
                false,
            )
            .await
            .unwrap();

        compact_files(&mut dataset, CompactionOptions::default(), None)
            .await
            .unwrap();

        // The index only knows the pre-compaction fragments, so it must not claim to
        // cover the fragment they were rewritten into.
        let live_fragments: RoaringBitmap =
            dataset.fragments().iter().map(|f| f.id as u32).collect();
        let index = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|index| index.fields == vec![0])
            .expect("index must survive compaction")
            .clone();
        assert!(
            index
                .effective_fragment_bitmap(&live_fragments)
                .is_none_or(|covered| covered.is_empty()),
            "compaction must not point an address-domain index at the fragments it wrote"
        );

        // Every fragment therefore falls back to a full scan, and the filter is answered
        // in full.
        let mut scanner = dataset.scan();
        scanner.filter("i > 0").unwrap();
        let matched = scanner.try_into_batch().await.unwrap();
        assert_eq!(matched.num_rows(), 199);
    }

    // Regression test for https://github.com/lancedb/lance/issues/6161
    // When FragReuseIndexDetails exceeds 204800 bytes it is written to an external
    // file. Previously the file was silently dropped (temp file deleted) because
    // tokio::io::AsyncWriteExt::shutdown was called instead of
    // lance_io::traits::Writer::shutdown, which persists the temp file.
    #[tokio::test]
    async fn test_defer_index_remap_large_external_file() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Create ~150 fragments × 1000 rows to produce a FragReuseIndexDetails
        // that exceeds the 204800-byte inline threshold (~302 KB serialized).
        let num_fragments = 150usize;
        let rows_per_fragment = 1000usize;
        let total_rows = num_fragments * rows_per_fragment;

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(
                vec![Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from_iter_values(0..total_rows as i32)) as ArrayRef],
                )
                .unwrap())],
                schema.clone(),
            ),
            test_uri,
            Some(WriteParams {
                max_rows_per_file: rows_per_fragment,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), num_fragments);

        // An FRI is only written for compactions that touch indexed data, so
        // index the column being compacted.
        create_scalar_index(&mut dataset, "i", false).await;

        // Delete a few rows from each fragment so compaction has something to do.
        dataset.delete("i % 1000 = 0").await.unwrap();

        compact_files(
            &mut dataset,
            CompactionOptions {
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        // Loading the FragReuseIndex details must succeed even when the details
        // were written to an external file.
        let frag_reuse_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("fragment reuse index must exist after compaction");

        load_frag_reuse_index_details(&dataset, &frag_reuse_meta)
            .await
            .expect("loading large frag reuse index details must not fail");
    }

    #[tokio::test]
    async fn test_defer_index_remap_rejected_with_stable_row_ids() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 9000))], data.schema());
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 1000, // 9 fragments
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert!(dataset.manifest.uses_stable_row_ids());

        let options = CompactionOptions {
            target_rows_per_fragment: 3_000,
            defer_index_remap: true,
            ..Default::default()
        };

        // Fails at planning time, before any fragment is rewritten.
        let plan_err = plan_compaction(&dataset, &options).await.unwrap_err();
        assert!(matches!(plan_err, Error::InvalidInput { .. }));
        let msg = plan_err.to_string();
        assert!(msg.contains("defer_index_remap"));
        assert!(msg.contains("stable row IDs"));

        // The full compact_files entry point fails the same way and leaves the
        // dataset untouched (no new manifest version, no orphaned data files).
        let version_before = dataset.manifest.version;
        let compact_err = compact_files(&mut dataset, options, None)
            .await
            .unwrap_err();
        assert!(matches!(compact_err, Error::InvalidInput { .. }));
        assert_eq!(dataset.manifest.version, version_before);
    }

    #[tokio::test]
    async fn test_defer_index_remap() {
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create another same dataset to mimic behavior without deferred index remap
        let mut data_gen2 = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset2 = Dataset::write(
            data_gen2.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Delete some rows to create deletions
        dataset.delete("i < 500").await.unwrap();
        dataset2.delete("i < 500").await.unwrap();

        // Create the same scalar index on both datasets so deferred and immediate
        // remapping are compared under the same conditions.
        create_scalar_index(&mut dataset, "i", false).await;
        create_scalar_index(&mut dataset2, "i", false).await;

        // Verify the initial state - no fragment reuse index should exist
        let initial_indices = dataset.load_indices().await.unwrap();
        assert_eq!(initial_indices.len(), 1);
        assert_eq!(initial_indices[0].name, "scalar");

        // Store the original scalar index UUID for comparison
        let original_scalar_uuid = initial_indices[0].uuid;

        // Plan and execute compaction manually
        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };
        let options2 = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: false,
            ..Default::default()
        };

        let plan = plan_compaction(&dataset, &options).await.unwrap();
        let plan2 = plan_compaction(&dataset2, &options2).await.unwrap();

        let mut expected_all_old_frag_ids = Vec::new();
        let mut expected_all_new_frag_ids = Vec::new();
        let mut expected_all_new_frag_bitmap = RoaringBitmap::new();
        let mut expected_all_row_id_map = HashMap::new();
        let mut deferred_results = Vec::new();
        let mut immediate_results = Vec::new();

        for (task, task2) in plan.tasks().iter().zip(plan2.tasks()) {
            let deferred_result = rewrite_files(Cow::Borrowed(&dataset), task.clone(), &options)
                .await
                .unwrap();
            let immediate_result =
                rewrite_files(Cow::Borrowed(&dataset2), task2.clone(), &options2)
                    .await
                    .unwrap();

            // Both should produce row_addrs (address-style row IDs)
            assert!(deferred_result.row_addrs.is_some());
            assert!(!deferred_result.row_addrs.as_ref().unwrap().is_empty());
            assert!(!deferred_result.row_addrs.as_ref().unwrap().is_empty());
            assert!(!deferred_result.original_fragments.is_empty());
            assert!(!deferred_result.new_fragments.is_empty());

            assert!(immediate_result.row_addrs.is_some());
            assert!(!immediate_result.original_fragments.is_empty());
            assert!(!immediate_result.new_fragments.is_empty());

            // Both should capture the same row addresses
            assert_eq!(deferred_result.row_addrs, immediate_result.row_addrs);

            deferred_results.push(deferred_result);
            immediate_results.push(immediate_result);
        }

        // Reserve fragment IDs for immediate results to build expected values
        {
            let frags: Vec<&mut Fragment> = immediate_results
                .iter_mut()
                .flat_map(|r| r.new_fragments.iter_mut())
                .collect();
            reserve_fragment_ids(&dataset2, frags.into_iter())
                .await
                .unwrap();
        }

        // Build expected values by transposing using the immediate results
        for immediate_result in &immediate_results {
            let row_addrs_bytes = immediate_result.row_addrs.as_ref().unwrap();
            let row_addrs =
                RoaringTreemap::deserialize_from(&mut Cursor::new(row_addrs_bytes)).unwrap();
            let transposed = transpose_row_addrs(
                row_addrs,
                &immediate_result.original_fragments,
                &immediate_result.new_fragments,
            );
            expected_all_row_id_map.extend(transposed);
            immediate_result.new_fragments.iter().for_each(|frag| {
                expected_all_new_frag_bitmap.insert(frag.id as u32);
            });
            expected_all_new_frag_ids.extend(
                immediate_result
                    .new_fragments
                    .iter()
                    .map(|s| s.id)
                    .collect::<Vec<_>>(),
            );
            expected_all_old_frag_ids.extend(
                immediate_result
                    .original_fragments
                    .iter()
                    .map(|s| s.id)
                    .collect::<Vec<_>>(),
            );
        }

        // Now commit the first compaction (using deferred results)
        let first_metrics = commit_compaction(
            &mut dataset,
            deferred_results.clone(),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        // Verify compaction happened
        assert!(first_metrics.fragments_removed > 0);
        assert!(first_metrics.fragments_added > 0);

        // Load and verify the fragment reuse index content
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };

        assert_eq!(
            frag_reuse_index_meta.fragment_bitmap.clone().unwrap(),
            expected_all_new_frag_bitmap
        );
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        let frag_reuse_index =
            open_frag_reuse_index(frag_reuse_index_meta.uuid, frag_reuse_details.as_ref())
                .await
                .unwrap();
        let stats = FragReuseIndexHandle(Arc::new(frag_reuse_index.clone()))
            .statistics()
            .unwrap();
        assert_eq!(
            serde_json::to_string(&stats).unwrap(),
            dataset
                .index_statistics(FRAG_REUSE_INDEX_NAME)
                .await
                .unwrap()
        );

        // Verify the index has one version with the correct dataset version
        let compaction_version = &frag_reuse_index.details.versions[0];
        assert_eq!(frag_reuse_index.details.versions.len(), 1);
        assert_eq!(
            compaction_version.dataset_version,
            frag_reuse_index_meta.dataset_version
        );

        // Verify the index compaction version information matches the RewriteResults
        let mut compacted_all_old_frag_digests = Vec::new();
        let mut compacted_all_new_frag_digests = Vec::new();
        let mut transposed_map = HashMap::new();
        for group in compaction_version.groups.iter() {
            let changed_row_addr_bytes = &group.changed_row_addrs;
            let mut cursor = Cursor::new(&changed_row_addr_bytes);
            let changed_row_addrs = RoaringTreemap::deserialize_from(&mut cursor).unwrap();
            compacted_all_old_frag_digests.extend(group.old_frags.clone());
            compacted_all_new_frag_digests.extend(group.new_frags.clone());

            let group_transposed_map = transpose_row_ids_from_digest(
                changed_row_addrs,
                &group.old_frags,
                &group.new_frags,
            );
            transposed_map.extend(group_transposed_map);
        }
        assert_eq!(transposed_map, expected_all_row_id_map);
        assert_eq!(
            compacted_all_old_frag_digests
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            expected_all_old_frag_ids
        );
        assert_eq!(
            compacted_all_new_frag_digests
                .iter()
                .map(|f| f.id)
                .collect::<Vec<_>>(),
            expected_all_new_frag_ids
        );

        // Verify the scalar index UUID is unchanged (it should not be remapped yet)
        let Some(current_scalar_index) = dataset.load_index_by_name("scalar").await.unwrap() else {
            panic!("scalar index must be available");
        };
        assert_eq!(current_scalar_index.uuid, original_scalar_uuid);
    }

    #[tokio::test]
    async fn test_defer_index_remap_skips_fri_when_no_indexed_data() {
        // A deferred compaction touching no indexed data must write no FRI --
        // such a version is un-drainable (remap no-ops, trim retains it forever).
        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(600),
            "memory://test/noindex",
            Some(WriteParams {
                max_rows_per_file: 100, // 6 small files -> compaction has work
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // No index at all: nothing covers any fragment.
        assert!(dataset.load_indices().await.unwrap().is_empty());
        let fragments_before = dataset.get_fragments().len();
        assert!(fragments_before > 1, "need multiple fragments to compact");

        let options = CompactionOptions {
            target_rows_per_fragment: 100_000,
            defer_index_remap: true,
            ..Default::default()
        };
        compact_files(&mut dataset, options, None).await.unwrap();

        // Compaction actually ran...
        assert!(
            dataset.get_fragments().len() < fragments_before,
            "compaction should have merged fragments"
        );
        // ...but no fragment-reuse index was created.
        assert!(
            dataset
                .load_index_by_name(FRAG_REUSE_INDEX_NAME)
                .await
                .unwrap()
                .is_none(),
            "deferred compaction with no indexed data must not create an FRI"
        );
    }

    #[tokio::test]
    async fn test_defer_index_remap_multiple_compactions() {
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // FRI is written only for compactions touching indexed data; index "i" so
        // the successive deferred compactions build a chained fragment-reuse index.
        create_scalar_index(&mut dataset, "i", false).await;

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let mut compact_read_versions = Vec::new();
        for i in 0..10 {
            dataset
                .delete(&format!("i < {}", 500 * (i + 1)))
                .await
                .unwrap();
            let read_version = dataset.manifest.version;
            compact_files(&mut dataset, options.clone(), None)
                .await
                .unwrap();

            // Record the read version for verification if compaction has happened
            if dataset.manifest.version > read_version {
                compact_read_versions.push(read_version);
            }

            // Load and verify the fragment reuse index content
            let Some(frag_reuse_index_meta) = dataset
                .load_index_by_name(FRAG_REUSE_INDEX_NAME)
                .await
                .unwrap()
            else {
                panic!("Fragment reuse index must be available");
            };
            let frag_reuse_details =
                load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
                    .await
                    .unwrap();
            let frag_reuse_index =
                open_frag_reuse_index(frag_reuse_index_meta.uuid, frag_reuse_details.as_ref())
                    .await
                    .unwrap();

            // Verify the index has one version with the correct dataset version
            assert_eq!(
                frag_reuse_index
                    .details
                    .versions
                    .iter()
                    .map(|v| v.dataset_version)
                    .collect::<Vec<_>>(),
                compact_read_versions
            );
        }
    }

    #[tokio::test]
    async fn test_defer_index_remap_mixed_records_all_groups() {
        // All-or-nothing: a compaction touching any indexed data records the full
        // FRI, including the unindexed group (a per-group filter would drop it).
        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("i".to_owned())));
        let mut dataset = Dataset::write(
            data_gen.batch(300),
            "memory://test/mixed",
            Some(WriteParams {
                max_rows_per_file: 100, // 3 fragments
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Index the initial fragments, then append more that stay unindexed.
        create_scalar_index(&mut dataset, "i", false).await;
        Dataset::write(
            data_gen.batch(300),
            WriteDestination::Dataset(Arc::new(dataset.clone())),
            Some(WriteParams {
                max_rows_per_file: 100, // 3 more, unindexed
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.checkout_latest().await.unwrap();

        // Fragments not covered by the scalar index are the "unindexed" ones.
        let indexed: HashSet<u32> = dataset
            .load_index_by_name("scalar")
            .await
            .unwrap()
            .unwrap()
            .fragment_bitmap
            .unwrap()
            .iter()
            .collect();
        let unindexed_frags: Vec<u64> = dataset
            .fragments()
            .iter()
            .map(|f| f.id)
            .filter(|id| !indexed.contains(&(*id as u32)))
            .collect();
        assert!(
            !unindexed_frags.is_empty(),
            "expected some unindexed fragments"
        );

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 100_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        // All-or-nothing: because indexed fragments were compacted, the FRI is
        // written AND records the unindexed group too (a per-group filter would
        // have dropped it).
        let fri_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("mixed compaction must write an FRI");
        let details = load_frag_reuse_index_details(&dataset, &fri_meta)
            .await
            .unwrap();
        let recorded_old: HashSet<u64> = details
            .versions
            .iter()
            .flat_map(|v| v.old_frag_ids())
            .collect();
        for f in &unindexed_frags {
            assert!(
                recorded_old.contains(f),
                "unindexed fragment {f} must be recorded in the FRI (all-or-nothing)"
            );
        }
    }

    #[tokio::test]
    async fn test_deferred_compaction_not_split_by_frag_reuse_index() {
        // The fragment-reuse index is a system index and must be excluded from
        // compaction bin planning; otherwise its covered fragment is isolated and
        // the small fragments never coalesce back to one.
        let data = sample_data();
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let options = CompactionOptions {
            defer_index_remap: true,
            ..Default::default()
        };

        // Two small fragments -> deferred compaction folds them into one,
        // creating the fragment-reuse index.
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 400))], data.schema());
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 200,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Index "a" so the deferred compaction records an FRI (only written for
        // compactions touching indexed data). The FRI is a system index and must
        // still not split later compaction bins -- the property this test guards.
        create_scalar_index(&mut dataset, "a", false).await;
        compact_files(&mut dataset, options.clone(), None)
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 1);
        assert!(
            dataset
                .load_index_by_name(FRAG_REUSE_INDEX_NAME)
                .await
                .unwrap()
                .is_some()
        );

        // Append two more small fragments, then compact again.
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(400, 400))], data.schema());
        let mut dataset = Dataset::write(
            reader,
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 200,
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);

        // Reindex so every fragment is data-indexed -- then the FRI (a system
        // index, correctly excluded from bin planning) is the only thing that
        // could split the bin.
        create_scalar_index(&mut dataset, "a", true).await;

        compact_files(&mut dataset, options, None).await.unwrap();
        assert_eq!(
            dataset.get_fragments().len(),
            1,
            "FRI (a system index) must not split the compaction bin; all fragments coalesce"
        );
    }

    #[tokio::test]
    async fn test_remap_index_after_compaction() {
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create a index to be remapped
        let index_name = Some("scalar".into());
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        // Remap without a frag reuse index should yield unsupported
        let Some(scalar_index) = dataset.load_index_by_name("scalar").await.unwrap() else {
            panic!("scalar index must be available");
        };

        let result = remapping::remap_column_index(&mut dataset, &["i"], index_name.clone()).await;
        assert!(matches!(result, Err(Error::NotSupported { .. })));

        let plan = plan_compaction(&dataset, &options).await.unwrap();

        // Commit each rewrite task separately to simulate 3 compaction runs
        // being accumulated in the fragment reuse index
        for task in plan.tasks().iter() {
            let rewrite_result = rewrite_files(Cow::Borrowed(&dataset), task.clone(), &options)
                .await
                .unwrap();

            commit_compaction(
                &mut dataset,
                Vec::from([rewrite_result]),
                Arc::new(DatasetIndexRemapperOptions::default()),
                &options,
            )
            .await
            .unwrap();
        }

        // Load and verify the fragment reuse index content
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        let frag_reuse_index =
            open_frag_reuse_index(frag_reuse_index_meta.uuid, frag_reuse_details.as_ref())
                .await
                .unwrap();

        assert_eq!(frag_reuse_index.details.versions.len(), plan.tasks().len());

        // Check auto-remap
        let mut all_fragment_bitmap = RoaringBitmap::new();
        dataset.fragments().iter().for_each(|f| {
            all_fragment_bitmap.insert(f.id as u32);
        });
        let Some(scalar_index_before_remap) = dataset.load_index_by_name("scalar").await.unwrap()
        else {
            panic!("scalar index must be available");
        };
        assert_eq!(
            scalar_index_before_remap.fragment_bitmap.unwrap(),
            all_fragment_bitmap
        );

        // Trigger index remap
        remapping::remap_column_index(&mut dataset, &["i"], index_name.clone())
            .await
            .unwrap();

        // Compare against original index
        let indices = read_manifest_indexes(
            &dataset.object_store,
            &dataset.manifest_location,
            &dataset.manifest,
        )
        .await
        .unwrap();
        let Some(remapped_scalar_index) = indices.into_iter().find(|idx| idx.name == "scalar")
        else {
            panic!("scalar index must be available");
        };
        assert_ne!(remapped_scalar_index.uuid, scalar_index.uuid);
        assert_eq!(
            remapped_scalar_index.fragment_bitmap.unwrap(),
            all_fragment_bitmap
        );
    }

    #[tokio::test]
    async fn test_concurrent_compaction_reindex_compaction_commit_first() {
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create an index
        let index_name = Some("scalar".into());
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Write some more data for reindexing
        Dataset::write(
            data_gen.batch(6_000),
            WriteDestination::Dataset(Arc::new(dataset.clone())),
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        dataset.checkout_latest().await.unwrap();
        let mut dataset_clone = dataset.clone();

        // First commit a compaction with deferred remap
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        // Concurrent reindex should succeed
        dataset_clone
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Check new index does not cover the compacted files
        dataset.checkout_latest().await.unwrap();

        let Some(scalar_index) = dataset.load_index_by_name("scalar").await.unwrap() else {
            panic!("scalar index must be available");
        };
        let index_frags = scalar_index
            .fragment_bitmap
            .unwrap()
            .iter()
            .collect::<HashSet<_>>();
        assert_eq!(
            index_frags,
            dataset
                .fragments()
                .iter()
                .map(|f| f.id as u32)
                .collect::<HashSet<_>>()
        )
    }

    #[tokio::test]
    async fn test_concurrent_compaction_reindex_reindex_commit_first() {
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                RandomVector::new().vec_width(128).named("vec".to_owned()),
            ))
            .col(Box::new(IncrementingInt32::new().named("i".to_owned())));

        let mut dataset = Dataset::write(
            data_gen.batch(6_000),
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create an index
        let index_name = Some("scalar".into());
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Write some more data for reindexing
        Dataset::write(
            data_gen.batch(6_000),
            WriteDestination::Dataset(Arc::new(dataset.clone())),
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        dataset.checkout_latest().await.unwrap();
        let mut dataset_clone = dataset.clone();

        // Concurrent reindex should succeed
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // First commit a compaction with deferred remap
        compact_files(
            &mut dataset_clone,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        // Check new index is auto-remapped
        dataset.checkout_latest().await.unwrap();
        let Some(scalar_index) = dataset.load_index_by_name("scalar").await.unwrap() else {
            panic!("scalar index must be available");
        };
        let index_frags = scalar_index
            .fragment_bitmap
            .unwrap()
            .iter()
            .collect::<HashSet<_>>();
        assert_eq!(
            index_frags,
            dataset
                .fragments()
                .iter()
                .map(|f| f.id as u32)
                .collect::<HashSet<_>>()
        )
    }

    #[tokio::test]
    async fn test_concurrent_cleanup_and_compaction_rebase_cleanup() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Index "i" so the deferred compaction touches indexed data and writes an FRI.
        create_scalar_index(&mut dataset, "i", false).await;

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let plan = plan_compaction(&dataset, &options).await.unwrap();
        let tasks = plan.tasks();

        // Only compact the first task, record the state of the dataset
        let rewrite_result = rewrite_files(Cow::Borrowed(&dataset), tasks[0].clone(), &options)
            .await
            .unwrap();

        commit_compaction(
            &mut dataset,
            Vec::from([rewrite_result]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        let mut dataset_clone = dataset.clone();

        // Load and verify the fragment reuse index content
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };

        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);

        // First commit the remaining 2 compaction tasks.
        let rewrite_result2 = rewrite_files(Cow::Borrowed(&dataset), tasks[1].clone(), &options)
            .await
            .unwrap();
        let rewritten_frags2 = rewrite_result2
            .original_fragments
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>();
        commit_compaction(
            &mut dataset,
            Vec::from([rewrite_result2]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        // Get the new fragment IDs from the frag_reuse_index after commit
        let frag_reuse_index_meta2 = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .unwrap();
        let frag_reuse_details2 = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta2)
            .await
            .unwrap();
        let new_frags2 = frag_reuse_details2.versions.last().unwrap().new_frag_ids();

        let rewrite_result3 = rewrite_files(Cow::Borrowed(&dataset), tasks[2].clone(), &options)
            .await
            .unwrap();
        let rewritten_frags3 = rewrite_result3
            .original_fragments
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>();
        commit_compaction(
            &mut dataset,
            Vec::from([rewrite_result3]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        // Get the new fragment IDs from the frag_reuse_index after commit
        let frag_reuse_index_meta3 = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .unwrap();
        let frag_reuse_details3 = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta3)
            .await
            .unwrap();
        let new_frags3 = frag_reuse_details3.versions.last().unwrap().new_frag_ids();

        // Concurrently commit a frag_reuse_index cleanup operation. dataset_clone
        // only knows the first reuse version; catch its index up so the cleanup
        // removes that version. After rebase onto the other compactions it should
        // contain the new compaction versions.
        remapping::remap_column_index(&mut dataset_clone, &["i"], Some("scalar".into()))
            .await
            .unwrap();
        cleanup_frag_reuse_index(&mut dataset_clone).await.unwrap();

        // Load and verify the fragment reuse index content
        dataset.checkout_latest().await.unwrap();
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 2);
        assert_eq!(
            frag_reuse_details.versions[0].old_frag_ids(),
            rewritten_frags2
        );
        assert_eq!(frag_reuse_details.versions[0].new_frag_ids(), new_frags2);
        assert_eq!(
            frag_reuse_details.versions[1].old_frag_ids(),
            rewritten_frags3
        );
        assert_eq!(frag_reuse_details.versions[1].new_frag_ids(), new_frags3);
    }

    #[tokio::test]
    async fn test_concurrent_cleanup_and_compaction_rebase_compaction() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Index "i" so the deferred compaction touches indexed data and writes an FRI.
        create_scalar_index(&mut dataset, "i", false).await;

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let plan = plan_compaction(&dataset, &options).await.unwrap();
        let tasks = plan.tasks();

        // Only compact the first task, record the state of the dataset
        let rewrite_result = rewrite_files(Cow::Borrowed(&dataset), tasks[0].clone(), &options)
            .await
            .unwrap();

        commit_compaction(
            &mut dataset,
            Vec::from([rewrite_result]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        let mut dataset_clone = dataset.clone();

        // Load and verify the fragment reuse index content
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);

        // Catch the index up to the compaction (on `dataset` only; `dataset_clone`
        // keeps the un-caught-up index for the concurrent rewrite below), then
        // clean up: with the index caught up the trim removes the first version.
        remapping::remap_column_index(&mut dataset, &["i"], Some("scalar".into()))
            .await
            .unwrap();
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();

        // Load and verify the fragment reuse index content
        dataset.checkout_latest().await.unwrap();
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 0);

        // Concurrently commit a rewrite
        // After rebase it should only contain the latest reuse version
        let rewrite_result2 =
            rewrite_files(Cow::Borrowed(&dataset_clone), tasks[1].clone(), &options)
                .await
                .unwrap();
        let rewritten_frags2 = rewrite_result2
            .original_fragments
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>();
        commit_compaction(
            &mut dataset_clone,
            Vec::from([rewrite_result2]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        // Load and verify the fragment reuse index content
        dataset.checkout_latest().await.unwrap();
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);
        assert_eq!(
            frag_reuse_details.versions[0].old_frag_ids(),
            rewritten_frags2
        );
        // Verify new fragment IDs are non-zero (allocated by commit_compaction)
        let new_frags2 = frag_reuse_details.versions[0].new_frag_ids();
        assert!(new_frags2.iter().all(|id| *id != 0));
    }

    #[tokio::test]
    async fn test_concurrent_compactions_with_defer_index_remap() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Index "i" so the deferred compaction touches indexed data and writes an FRI.
        create_scalar_index(&mut dataset, "i", false).await;

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let plan = plan_compaction(&dataset, &options).await.unwrap();
        let tasks = plan.tasks();

        let mut dataset_clone = dataset.clone();

        // Only compact the first task, record the state of the dataset
        let rewrite_result = rewrite_files(Cow::Borrowed(&dataset), tasks[0].clone(), &options)
            .await
            .unwrap();

        commit_compaction(
            &mut dataset,
            Vec::from([rewrite_result]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .unwrap();

        // Load and verify the fragment reuse index content
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);

        // Concurrently commit a rewrite should fail
        let rewrite_result2 =
            rewrite_files(Cow::Borrowed(&dataset_clone), tasks[1].clone(), &options)
                .await
                .unwrap();
        let result = commit_compaction(
            &mut dataset_clone,
            Vec::from([rewrite_result2]),
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await;
        assert!(matches!(result, Err(Error::RetryableCommitConflict { .. })));
    }

    #[tokio::test]
    async fn test_read_bitmap_index_with_defer_index_remap() {
        // Create a dataset with categorical values
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Get initial counts for each category
        let count1 = dataset
            .count_rows(Some("category = 1".to_owned()))
            .await
            .unwrap();
        let count2 = dataset
            .count_rows(Some("category = 2".to_owned()))
            .await
            .unwrap();
        let count3 = dataset
            .count_rows(Some("category = 3".to_owned()))
            .await
            .unwrap();

        // Create a bitmap index on the category column
        let index_name = Some("category_idx".into());
        dataset
            .create_index(
                &["category"],
                IndexType::Bitmap,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices
            .iter()
            .find(|idx| idx.name == "category_idx")
            .unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify the index UUID is unchanged (it should not be remapped yet)
        let Some(current_index) = dataset.load_index_by_name("category_idx").await.unwrap() else {
            panic!("category index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        // Verify that scans still work correctly and return the same counts
        assert_eq!(
            dataset
                .count_rows(Some("category = 1".to_owned()))
                .await
                .unwrap(),
            count1
        );
        assert_eq!(
            dataset
                .count_rows(Some("category = 2".to_owned()))
                .await
                .unwrap(),
            count2
        );
        assert_eq!(
            dataset
                .count_rows(Some("category = 3".to_owned()))
                .await
                .unwrap(),
            count3
        );

        // Verify that after index creation and compaction, scan uses bitmap index scan
        let mut scanner = dataset.scan();
        scanner.filter("category = 1").unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains("ScalarIndexQuery: query=[category = 1]@category_idx(Bitmap)"),
            "Expected index query in plan: {}",
            plan
        );
    }

    #[tokio::test]
    async fn test_read_btree_index_with_defer_index_remap() {
        // Create a dataset with an incremental ID column
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("id", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(110), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Get initial counts for some ID ranges
        let count_low = dataset
            .count_rows(Some("id < 1000".to_owned()))
            .await
            .unwrap();
        let count_mid = dataset
            .count_rows(Some("id >= 2000 and id < 3000".to_owned()))
            .await
            .unwrap();
        let count_high = dataset
            .count_rows(Some("id >= 5000".to_owned()))
            .await
            .unwrap();

        // Create a btree index on the id column
        let index_name = Some("id_idx".into());
        dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "id_idx").unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 50_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify the index UUID is unchanged (it should not be remapped yet)
        let Some(current_index) = dataset.load_index_by_name("id_idx").await.unwrap() else {
            panic!("id index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        // Verify that scans still work correctly and return the same counts
        assert_eq!(
            dataset
                .count_rows(Some("id < 1000".to_owned()))
                .await
                .unwrap(),
            count_low
        );
        assert_eq!(
            dataset
                .count_rows(Some("id >= 2000 and id < 3000".to_owned()))
                .await
                .unwrap(),
            count_mid
        );
        assert_eq!(
            dataset
                .count_rows(Some("id >= 5000".to_owned()))
                .await
                .unwrap(),
            count_high
        );

        // Verify that after index creation and compaction, scan uses btree index scan
        let mut scanner = dataset.scan();
        scanner.filter("id >= 2000 and id < 3000").unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains("ScalarIndexQuery: query=[id >= 2000 && id < 3000]@id_idx(BTree)"),
            "Expected scalar index query in plan: {}",
            plan
        );
    }

    #[rstest]
    #[case(IndexRemapMode::Compact)]
    #[case(IndexRemapMode::Direct)]
    #[tokio::test]
    async fn test_btree_index_remap_after_compaction(#[case] index_remap_mode: IndexRemapMode) {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(32)),
            )
            .col("id", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Delete rows scattered across fragments so the remap must drop some old
        // addresses and shift the survivors.
        dataset.delete("id % 10 == 0").await.unwrap();

        dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                Some("id_idx".into()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        let count_low = dataset
            .count_rows(Some("id < 1000".to_owned()))
            .await
            .unwrap();
        let count_mid = dataset
            .count_rows(Some("id >= 2000 and id < 3000".to_owned()))
            .await
            .unwrap();
        let count_high = dataset
            .count_rows(Some("id >= 5000".to_owned()))
            .await
            .unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 50_000,
            index_remap_mode,
            ..Default::default()
        };
        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // The index was remapped inline and must still drive scans.
        let mut scanner = dataset.scan();
        scanner.filter("id >= 2000 and id < 3000").unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains("ScalarIndexQuery: query=[id >= 2000 && id < 3000]@id_idx(BTree)"),
            "Expected scalar index query in plan: {}",
            plan
        );

        // Counts resolved through the remapped index match the pre-compaction
        // values in both remap modes.
        assert_eq!(
            dataset
                .count_rows(Some("id < 1000".to_owned()))
                .await
                .unwrap(),
            count_low
        );
        assert_eq!(
            dataset
                .count_rows(Some("id >= 2000 and id < 3000".to_owned()))
                .await
                .unwrap(),
            count_mid
        );
        assert_eq!(
            dataset
                .count_rows(Some("id >= 5000".to_owned()))
                .await
                .unwrap(),
            count_high
        );
    }

    #[rstest]
    #[case(IndexRemapMode::Compact)]
    #[case(IndexRemapMode::Direct)]
    #[tokio::test]
    async fn test_ivf_pq_index_remap_after_compaction(#[case] index_remap_mode: IndexRemapMode) {
        use arrow_array::cast::AsArray;
        use lance_index::vector::pq::PQBuildParams;

        const DIM: u32 = 32;
        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(DIM)),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            small_ivf(),
            PQBuildParams {
                max_iters: 2,
                num_sub_vectors: 2,
                ..Default::default()
            },
        );
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                false,
            )
            .await
            .unwrap();
        let original_uuid = dataset
            .load_index_by_name("vec_idx")
            .await
            .unwrap()
            .unwrap()
            .uuid;

        // Delete rows scattered across fragments so the remap must drop some old
        // addresses and shift the survivors.
        dataset.delete("id % 10 == 0").await.unwrap();

        // Sample queries from surviving vectors and capture the pre-compaction
        // KNN answer and the surviving id set.
        let mut survivors: Vec<(i32, Vec<f32>)> = Vec::new();
        {
            let mut scanner = dataset.scan();
            scanner.project(&["id", "vec"]).unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for batch in &batches {
                let ids = batch["id"].as_primitive::<Int32Type>();
                let vecs = batch["vec"].as_fixed_size_list();
                for i in 0..batch.num_rows() {
                    let v = vecs.value(i);
                    survivors.push((
                        ids.value(i),
                        v.as_primitive::<Float32Type>().values().to_vec(),
                    ));
                }
            }
        }
        let surviving_ids: std::collections::HashSet<i32> =
            survivors.iter().map(|(id, _)| *id).collect();
        let step = (survivors.len() / 16).max(1);
        let queries: Vec<Vec<f32>> = survivors
            .iter()
            .step_by(step)
            .map(|(_, v)| v.clone())
            .collect();
        let k = 10;
        let mut baseline: Vec<Vec<i32>> = Vec::new();
        for q in &queries {
            baseline.push(vector_knn_ids(&dataset, q, k).await);
        }

        // Inline remap (defer_index_remap = false): compaction physically
        // rebuilds the vector index through the configured remap mode.
        let metrics = compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 50_000,
                index_remap_mode,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // The index was physically remapped inline, so its uuid must change.
        assert_ne!(
            dataset
                .load_index_by_name("vec_idx")
                .await
                .unwrap()
                .unwrap()
                .uuid,
            original_uuid,
            "vector index must be physically remapped inline"
        );

        // The remap only relabels row addresses; it must not resurrect deleted
        // rows, and KNN must stay close to the pre-compaction answer in both
        // remap modes.
        for (i, q) in queries.iter().enumerate() {
            let after = vector_knn_ids(&dataset, q, k).await;
            for id in &after {
                assert!(
                    surviving_ids.contains(id),
                    "KNN returned id {id} that is not a surviving row (query #{i}, mode {index_remap_mode:?})"
                );
            }
            let overlap = after.iter().filter(|id| baseline[i].contains(id)).count();
            assert!(
                overlap >= 8,
                "KNN top-{k} diverged after compaction: overlap {overlap} < 8 (query #{i}, mode {index_remap_mode:?})"
            );
        }
    }

    #[rstest]
    #[case(IndexRemapMode::Compact)]
    #[case(IndexRemapMode::Direct)]
    #[tokio::test]
    async fn test_inverted_index_remap_after_compaction(#[case] index_remap_mode: IndexRemapMode) {
        use arrow_array::cast::AsArray;

        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col("doc", lance_datagen::array::random_sentence(1, 100, false))
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        dataset
            .create_index(
                &["doc"],
                IndexType::Inverted,
                Some("doc_idx".into()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let original_uuid = dataset
            .load_index_by_name("doc_idx")
            .await
            .unwrap()
            .unwrap()
            .uuid;

        // Sample a few words from a real document to drive full-text searches.
        let words: Vec<String> = {
            let mut scanner = dataset.scan();
            scanner
                .project(&["doc"])
                .unwrap()
                .limit(Some(1), None)
                .unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut words: Vec<String> = batches[0]["doc"]
                .as_string::<i32>()
                .value(0)
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            words.sort();
            words.dedup();
            words.truncate(3);
            words
        };
        assert!(!words.is_empty(), "sampled document must contain words");

        // Delete rows scattered across fragments so the remap must drop some old
        // addresses and shift the survivors.
        dataset.delete("id % 10 == 0").await.unwrap();

        // Capture the post-deletion full-text-search counts (resolved through the
        // index + deletion vectors) before compaction physically remaps.
        let mut before = Vec::new();
        for word in &words {
            let mut scanner = dataset.scan();
            scanner
                .full_text_search(FullTextSearchQuery::new(word.clone()))
                .unwrap();
            scanner.project::<String>(&[]).unwrap().with_row_id();
            before.push(scanner.count_rows().await.unwrap());
        }

        // Inline remap (defer_index_remap = false): compaction physically rebuilds
        // the inverted index through the configured remap mode.
        let options = CompactionOptions {
            target_rows_per_fragment: 50_000,
            index_remap_mode,
            ..Default::default()
        };
        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // The index was physically remapped inline, so its uuid must change.
        assert_ne!(
            dataset
                .load_index_by_name("doc_idx")
                .await
                .unwrap()
                .unwrap()
                .uuid,
            original_uuid,
            "inverted index must be physically remapped inline (mode {index_remap_mode:?})"
        );

        // The remapped index must still drive full-text search.
        let mut scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(words[0].clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(true).await.unwrap();
        assert!(
            plan.contains("MatchQuery"),
            "Expected inverted index scan in plan: {}",
            plan
        );

        // Counts resolved through the remapped index match the pre-compaction
        // values in both remap modes.
        for (word, expected) in words.iter().zip(before) {
            let mut scanner = dataset.scan();
            scanner
                .full_text_search(FullTextSearchQuery::new(word.clone()))
                .unwrap();
            scanner.project::<String>(&[]).unwrap().with_row_id();
            assert_eq!(
                scanner.count_rows().await.unwrap(),
                expected,
                "full-text count for {word:?} changed after compaction (mode {index_remap_mode:?})"
            );
        }
    }

    #[tokio::test]
    async fn test_read_inverted_index_with_defer_index_remap() {
        // Generate random words using lance-datagen
        let mut words_gen = lance_datagen::array::random_sentence(1, 100, true);
        let doc_col = words_gen
            .generate_default(lance_datagen::RowCount::from(6000))
            .unwrap();

        let batch = RecordBatch::try_new(
            Schema::new(vec![Field::new("doc", DataType::LargeUtf8, false)]).into(),
            vec![doc_col.clone()],
        )
        .unwrap();
        let schema_ref = batch.schema();
        let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema_ref);
        let mut dataset = Dataset::write(
            stream,
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Get initial counts for some word searches
        // Extract some test words from the generated documents
        let large_string_array = doc_col.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let sample_words: Vec<String> = large_string_array
            .value(0)
            .split_whitespace()
            .take(10)
            .map(|s| s.to_string())
            .collect();
        let test_word1 = &sample_words[0];
        let test_word2 = &sample_words[1];
        let test_word3 = &sample_words[2];

        // Create an inverted index on the doc column
        let index_name = Some("doc_idx".into());
        dataset
            .create_index(
                &["doc"],
                IndexType::Inverted,
                index_name.clone(),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "doc_idx").unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify the index UUID is unchanged (it should not be remapped yet)
        let Some(current_index) = dataset.load_index_by_name("doc_idx").await.unwrap() else {
            panic!("doc index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        // Initial scan
        let mut scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word1.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let count1 = scanner.count_rows().await.unwrap();
        scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word2.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let count2 = scanner.count_rows().await.unwrap();
        scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word3.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let count3 = scanner.count_rows().await.unwrap();

        // Verify that after index creation and compaction, scan uses inverted index scan
        let mut scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word1.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(true).await.unwrap();
        assert!(
            plan.contains("MatchQuery"),
            "Expected inverted index scan in plan: {}",
            plan
        );
        assert!(
            !plan.contains("LanceScan"),
            "Expected no fragment scan in plan: {}",
            plan
        );

        // Reindex to the latest
        dataset
            .create_index(
                &["doc"],
                IndexType::Inverted,
                index_name.clone(),
                &InvertedIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Verify that scans still work correctly and return the same counts
        let mut scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word1.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        assert_eq!(scanner.count_rows().await.unwrap(), count1);
        scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word2.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        assert_eq!(scanner.count_rows().await.unwrap(), count2);
        scanner = dataset.scan();
        scanner
            .full_text_search(FullTextSearchQuery::new(test_word3.clone()))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        assert_eq!(scanner.count_rows().await.unwrap(), count3);
    }

    /// Deferred compaction that materializes deletions must not corrupt an
    /// inverted (FTS) index read through the fragment-reuse index. The index's
    /// posting lists reference doc_ids positionally; if the load-time remap
    /// dropped the deleted rows it would renumber the doc_ids and desync the
    /// posting lists (out-of-bounds `num_tokens`, wrong/stale row ids). The
    /// tombstone-preserve-positions load path must keep results correct in the
    /// FRI window and after the physical remap + trim.
    #[tokio::test]
    async fn test_read_inverted_index_with_defer_index_remap_and_deletions() {
        // Enough surviving docs for several compressed posting-list blocks
        // (BLOCK_SIZE = 128), split across several fragments so compaction has
        // real work — but no larger.
        const ROWS: i32 = 1200;
        const DELETED: i32 = 400;

        // Every row contains "lance", so the term matches all live rows; `id`
        // tells us exactly which rows survive.
        let ids = Int32Array::from_iter_values(0..ROWS);
        let docs = LargeStringArray::from_iter_values((0..ROWS).map(|_| "lance apple orange"));
        let batch = RecordBatch::try_new(
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("doc", DataType::LargeUtf8, false),
            ])
            .into(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(docs) as ArrayRef],
        )
        .unwrap();
        let schema_ref = batch.schema();
        let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema_ref);
        let mut dataset = Dataset::write(
            stream,
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 200, // 6 fragments
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        dataset
            .create_index(
                &["doc"],
                IndexType::Inverted,
                Some("doc_idx".into()),
                &InvertedIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Delete a prefix, then deferred-compact so the deletions are
        // materialized into the fragment-reuse index the index is read through.
        dataset.delete(&format!("id < {DELETED}")).await.unwrap();
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(
            dataset
                .load_index_by_name(FRAG_REUSE_INDEX_NAME)
                .await
                .unwrap()
                .is_some(),
            "deferred compaction must leave a fragment-reuse index"
        );

        // FTS "lance" → sorted surviving ids. Projecting `id` forces a take, so
        // a stale row address would error or return a wrong/dead row.
        async fn search_ids(dataset: &Dataset) -> Vec<i32> {
            let mut scanner = dataset.scan();
            scanner
                .full_text_search(FullTextSearchQuery::new("lance".to_owned()))
                .unwrap();
            scanner.project::<&str>(&["id"]).unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut ids: Vec<i32> = batches
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
            ids.sort_unstable();
            ids
        }

        let expected = (DELETED..ROWS).collect::<Vec<_>>();

        // FRI window: index read through the reuse index.
        let during = search_ids(&dataset).await;
        assert_eq!(
            during, expected,
            "FRI-window FTS must return exactly the surviving rows (no resurrection, no loss, no stale rows)"
        );

        // Physical remap + trim: must still be correct.
        remapping::remap_column_index(&mut dataset, &["doc"], Some("doc_idx".into()))
            .await
            .unwrap();
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();
        let after = search_ids(&dataset).await;
        assert_eq!(
            after, expected,
            "FTS must stay correct after physical remap + fragment-reuse trim"
        );
    }

    #[tokio::test]
    async fn test_read_ngram_index_with_defer_index_remap() {
        // Generate random words using lance-datagen
        let mut words_gen = lance_datagen::array::random_sentence(1, 100, true);
        let doc_col = words_gen
            .generate_default(lance_datagen::RowCount::from(6000))
            .unwrap();

        let batch = RecordBatch::try_new(
            Schema::new(vec![Field::new("doc", DataType::LargeUtf8, false)]).into(),
            vec![doc_col.clone()],
        )
        .unwrap();
        let schema_ref = batch.schema();
        let stream = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema_ref);
        let mut dataset = Dataset::write(
            stream,
            "memory://test/table",
            Some(WriteParams {
                max_rows_per_file: 1_000, // 6 files
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Get initial counts for some word searches
        // Extract some test words from the generated documents
        let large_string_array = doc_col.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let sample_words: Vec<String> = large_string_array
            .value(0)
            .split_whitespace()
            .take(10)
            .map(|s| s.to_string())
            .collect();
        let test_word1 = &sample_words[0];
        let test_word2 = &sample_words[1];
        let test_word3 = &sample_words[2];

        // Create an inverted index on the doc column
        let index_name = Some("doc_idx".into());
        dataset
            .create_index(
                &["doc"],
                IndexType::NGram,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "doc_idx").unwrap();

        // Initial scan
        let count1 = dataset
            .count_rows(Some(format!("contains(doc, '{}')", test_word1)))
            .await
            .unwrap();
        let count2 = dataset
            .count_rows(Some(format!("contains(doc, '{}')", test_word2)))
            .await
            .unwrap();
        let count3 = dataset
            .count_rows(Some(format!("contains(doc, '{}')", test_word3)))
            .await
            .unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify the index UUID is unchanged (it should not be remapped yet)
        let Some(current_index) = dataset.load_index_by_name("doc_idx").await.unwrap() else {
            panic!("doc index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        // Verify that scans still work correctly and return the same counts
        assert_eq!(
            dataset
                .count_rows(Some(format!("contains(doc, '{}')", test_word1)))
                .await
                .unwrap(),
            count1
        );
        assert_eq!(
            dataset
                .count_rows(Some(format!("contains(doc, '{}')", test_word2)))
                .await
                .unwrap(),
            count2
        );
        assert_eq!(
            dataset
                .count_rows(Some(format!("contains(doc, '{}')", test_word3)))
                .await
                .unwrap(),
            count3
        );

        // Verify that after index creation and compaction, scan uses inverted index scan
        let mut scanner = dataset.scan();
        scanner
            .filter(&format!("contains(doc, '{}')", test_word1))
            .unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains("ScalarIndexQuery: query=[contains(doc, Utf8"),
            "Expected scalar index query in plan: {}",
            plan
        );
    }

    #[tokio::test]
    async fn test_read_label_list_index_with_defer_index_remap() {
        // Create a dataset with list data for labels
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col(
                "labels",
                lance_datagen::array::rand_list_any(
                    lance_datagen::array::cycle::<Int64Type>(vec![1, 2, 3, 4, 5]),
                    false,
                ),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Get initial counts for different label values
        let count1 = dataset
            .count_rows(Some("array_has_any(labels, [1])".to_owned()))
            .await
            .unwrap();
        let count2 = dataset
            .count_rows(Some("array_has_any(labels, [5])".to_owned()))
            .await
            .unwrap();
        let count3 = dataset
            .count_rows(Some("array_has_any(labels, [10])".to_owned()))
            .await
            .unwrap();

        // Create a label list index on the labels column
        let index_name = Some("labels_idx".into());
        dataset
            .create_index(
                &["labels"],
                IndexType::LabelList,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "labels_idx").unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 2000,
            defer_index_remap: true,
            ..Default::default()
        };
        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify that the index UUID remains unchanged
        let indices = dataset.load_indices().await.unwrap();
        let current_index = indices.iter().find(|idx| idx.name == "labels_idx").unwrap();
        assert_eq!(current_index.uuid, original_index.uuid);

        // Verify that scans still work correctly and return the same counts
        assert_eq!(
            dataset
                .count_rows(Some("array_has_any(labels, [1])".to_owned()))
                .await
                .unwrap(),
            count1
        );
        assert_eq!(
            dataset
                .count_rows(Some("array_has_any(labels, [5])".to_owned()))
                .await
                .unwrap(),
            count2
        );
        assert_eq!(
            dataset
                .count_rows(Some("array_has_any(labels, [10])".to_owned()))
                .await
                .unwrap(),
            count3
        );

        // Verify that after index creation and compaction, scan uses label list index scan
        let mut scanner = dataset.scan();
        scanner.filter("array_has_any(labels, [1])").unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains(
                "ScalarIndexQuery: query=[array_has_any(labels, List([1]))]@labels_idx(LabelList)",
            ),
            "Expected scalar index query in plan: {}",
            plan
        );
    }

    #[tokio::test]
    async fn test_read_ivf_pq_index_v3_with_defer_index_remap() {
        // Create a dataset with vector data
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Get some query vectors for KNN search
        let query_vec1: PrimitiveArray<Float32Type> =
            PrimitiveArray::from_iter_values(std::iter::repeat_n(0.0, 128));
        let query_vec2: PrimitiveArray<Float32Type> =
            PrimitiveArray::from_iter_values(std::iter::repeat_n(1.1, 128));
        let query_vec3: PrimitiveArray<Float32Type> =
            PrimitiveArray::from_iter_values(std::iter::repeat_n(2.2, 128));

        // Get initial KNN search results
        let mut scanner = dataset.scan();
        scanner.nearest("vec", &query_vec1, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let results1 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let count1 = results1.len();

        scanner = dataset.scan();
        scanner.nearest("vec", &query_vec2, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let results2 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let count2 = results2.len();

        scanner = dataset.scan();
        scanner.nearest("vec", &query_vec3, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let results3 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let count3 = results3.len();

        // Create an IVF-PQ index on the vec column
        let index_name = Some("vec_idx".into());
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                index_name.clone(),
                &VectorIndexParams {
                    metric_type: DistanceType::L2,
                    stages: vec![
                        StageParams::Ivf(IvfBuildParams {
                            max_iters: 2,
                            num_partitions: Some(2),
                            sample_rate: 2,
                            ..Default::default()
                        }),
                        StageParams::PQ(PQBuildParams {
                            max_iters: 2,
                            num_sub_vectors: 2,
                            ..Default::default()
                        }),
                    ],
                    version: crate::index::vector::IndexFileVersion::V3,
                    skip_transpose: false,
                    runtime_hints: Default::default(),
                },
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "vec_idx").unwrap();

        // Run compaction with deferred index remapping
        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };

        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        // Verify the index UUID is unchanged (it should not be remapped yet)
        let Some(current_index) = dataset.load_index_by_name("vec_idx").await.unwrap() else {
            panic!("vec index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        // Verify that KNN searches still work correctly and return the same counts
        let mut scanner = dataset.scan();
        scanner.nearest("vec", &query_vec1, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let new_results1 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(new_results1.len(), count1);

        scanner = dataset.scan();
        scanner.nearest("vec", &query_vec2, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let new_results2 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(new_results2.len(), count2);

        scanner = dataset.scan();
        scanner.nearest("vec", &query_vec3, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let new_results3 = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(new_results3.len(), count3);

        // Verify that after index creation and compaction, scan uses vector index scan
        let mut scanner = dataset.scan();
        scanner.nearest("vec", &query_vec1, 10).unwrap();
        scanner.project::<String>(&[]).unwrap().with_row_id();
        let plan = scanner.explain_plan(false).await.unwrap();
        assert!(
            plan.contains("ANNSubIndex"),
            "Expected vector index scan in plan: {}",
            plan
        );
        assert!(
            !plan.contains("LanceScan"),
            "Expected no fragment scan in plan: {}",
            plan
        );
    }

    #[tokio::test]
    async fn test_read_ivf_rq_index_v3_with_defer_index_remap() {
        use arrow_array::cast::AsArray;
        use lance_index::vector::bq::RQBuildParams;

        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        let stored: Vec<Vec<f32>> = {
            let mut scanner = dataset.scan();
            scanner.project(&["vec"]).unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut out = Vec::new();
            for batch in &batches {
                let vecs = batch["vec"].as_fixed_size_list();
                for i in 0..batch.num_rows() {
                    let values = vecs.value(i);
                    let values = values.as_primitive::<Float32Type>();
                    out.push(values.values().to_vec());
                }
            }
            out
        };

        let index_name = Some("vec_idx".into());
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                index_name.clone(),
                &VectorIndexParams {
                    metric_type: DistanceType::L2,
                    stages: vec![
                        StageParams::Ivf(IvfBuildParams {
                            max_iters: 2,
                            num_partitions: Some(2),
                            sample_rate: 2,
                            ..Default::default()
                        }),
                        StageParams::RQ(RQBuildParams::new(1)),
                    ],
                    version: crate::index::vector::IndexFileVersion::V3,
                    skip_transpose: false,
                    runtime_hints: Default::default(),
                },
                false,
            )
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let original_index = indices.iter().find(|idx| idx.name == "vec_idx").unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 2_000,
            defer_index_remap: true,
            ..Default::default()
        };
        let metrics = compact_files(&mut dataset, options, None).await.unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(metrics.fragments_added > 0);

        let Some(current_index) = dataset.load_index_by_name("vec_idx").await.unwrap() else {
            panic!("vec index must be available");
        };
        assert_eq!(current_index.uuid, original_index.uuid);

        let frag_reuse_present = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME);
        assert!(
            frag_reuse_present,
            "defer_index_remap must record a {} index",
            FRAG_REUSE_INDEX_NAME
        );

        let sample_step = (stored.len() / 8).max(1);
        let mut checked = 0;
        for query in stored.iter().step_by(sample_step) {
            let query_vec = PrimitiveArray::<Float32Type>::from_iter_values(query.iter().copied());
            let mut scanner = dataset.scan();
            scanner.nearest("vec", &query_vec, 5).unwrap();
            scanner.project(&["vec"]).unwrap().with_row_id();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(!batches.is_empty(), "query returned no batches");
            let top = &batches[0];
            assert!(top.num_rows() > 0, "query returned empty top batch");
            let top_vec = top["vec"].as_fixed_size_list().value(0);
            let top_vec = top_vec.as_primitive::<Float32Type>();
            assert_eq!(
                top_vec.values(),
                query.as_slice(),
                "top-1 self-recall returned a different vector than the query"
            );
            checked += 1;
        }
        assert!(checked > 0, "expected to check at least one stored vector");
    }

    /// Build an `id` + `vec` dataset, create the given IVF vector index,
    /// optionally delete rows, then run deferred compaction (which materializes
    /// the deletions into the fragment-reuse index) and assert that KNN over
    /// surviving vectors during the FRI window (a) never returns a deleted row
    /// and (b) stays consistent with the pre-compaction answer.
    ///
    /// The deletion path is the interesting one: materialized deletions drop
    /// rows from the quantization storage at load time, which shifts storage
    /// positions. Flat storage (FLAT/PQ/SQ/RQ) is scanned linearly so this is
    /// fine, but the HNSW graph addresses storage positionally and is not
    /// frag-reuse aware, so a desync would surface here as recall collapse or a
    /// resurrected/again-deleted row.
    /// Top-k `id`s for a KNN query against the `vec` column.
    async fn vector_knn_ids(dataset: &Dataset, query: &[f32], k: usize) -> Vec<i32> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float32Type, Int32Type};
        let qa = PrimitiveArray::<Float32Type>::from_iter_values(query.iter().copied());
        let mut scanner = dataset.scan();
        scanner.nearest("vec", &qa, k).unwrap();
        scanner.project(&["id"]).unwrap();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut ids = Vec::new();
        for b in &batches {
            ids.extend(b["id"].as_primitive::<Int32Type>().values().iter().copied());
        }
        ids
    }

    async fn check_vector_defer_compaction(
        params: VectorIndexParams,
        delete_predicate: Option<&str>,
        k: usize,
        min_overlap: usize,
    ) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float32Type, Int32Type};
        use lance_datagen::Dimension;

        const DIM: u32 = 32;
        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(DIM)),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                false,
            )
            .await
            .unwrap();
        let original_uuid = dataset
            .load_index_by_name("vec_idx")
            .await
            .unwrap()
            .unwrap()
            .uuid;

        if let Some(pred) = delete_predicate {
            dataset.delete(pred).await.unwrap();
        }

        // Collect surviving (id, vec) pairs and the set of surviving ids.
        let mut survivors: Vec<(i32, Vec<f32>)> = Vec::new();
        {
            let mut scanner = dataset.scan();
            scanner.project(&["id", "vec"]).unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for batch in &batches {
                let ids = batch["id"].as_primitive::<Int32Type>();
                let vecs = batch["vec"].as_fixed_size_list();
                for i in 0..batch.num_rows() {
                    let v = vecs.value(i);
                    let v = v.as_primitive::<Float32Type>().values().to_vec();
                    survivors.push((ids.value(i), v));
                }
            }
        }
        assert!(!survivors.is_empty());
        let surviving_ids: std::collections::HashSet<i32> =
            survivors.iter().map(|(id, _)| *id).collect();

        // Sample queries from survivors and capture the pre-compaction answer.
        let step = (survivors.len() / 16).max(1);
        let queries: Vec<(i32, Vec<f32>)> = survivors.iter().step_by(step).cloned().collect();
        let mut baseline: Vec<Vec<i32>> = Vec::new();
        for (_, q) in &queries {
            baseline.push(vector_knn_ids(&dataset, q, k).await);
        }

        // Deferred compaction materializes the deletions into the frag-reuse index.
        let metrics = compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME),
            "deferred compaction must record a frag-reuse index"
        );
        assert_eq!(
            dataset
                .load_index_by_name("vec_idx")
                .await
                .unwrap()
                .unwrap()
                .uuid,
            original_uuid,
            "index must not be physically remapped yet (FRI window)"
        );

        // During the FRI window: no deleted rows, and stable vs the baseline.
        for (i, (_, q)) in queries.iter().enumerate() {
            let after = vector_knn_ids(&dataset, q, k).await;
            for id in &after {
                assert!(
                    surviving_ids.contains(id),
                    "KNN returned id {id} that is not a surviving row (query #{i})"
                );
            }
            let overlap = after.iter().filter(|id| baseline[i].contains(id)).count();
            assert!(
                overlap >= min_overlap,
                "KNN top-{k} diverged after deferred compaction: overlap {overlap} < {min_overlap} (query #{i})"
            );
        }
    }

    fn small_ivf() -> lance_index::vector::ivf::IvfBuildParams {
        lance_index::vector::ivf::IvfBuildParams {
            max_iters: 2,
            num_partitions: Some(2),
            sample_rate: 2,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_ivf_flat_defer_compaction_with_deletions() {
        let params = VectorIndexParams::with_ivf_flat_params(DistanceType::L2, small_ivf());
        // Flat storage is scanned linearly; dropping deleted rows is exact.
        check_vector_defer_compaction(params, Some("id < 1500"), 10, 10).await;
    }

    #[tokio::test]
    async fn test_ivf_hnsw_sq_defer_compaction_merge_only() {
        use lance_index::vector::{hnsw::builder::HnswBuildParams, sq::builder::SQBuildParams};
        let params = VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::L2,
            small_ivf(),
            HnswBuildParams::default(),
            SQBuildParams::default(),
        );
        // No deletions: storage positions are stable, so the graph stays aligned.
        check_vector_defer_compaction(params, None, 10, 9).await;
    }

    // NOTE: IVF_HNSW_* under materialized deletions is a known gap (lance#3993,
    // HNSW auto-remap not implemented) — the HNSW graph isn't realigned after the
    // frag-reuse drop. Deferred remap is gated off for HNSW tables, so there is
    // no lance-level reproducer here; the gate is tested in the data plane.
    // Merge-only HNSW is covered (see the *_remap_and_trim tests).

    #[tokio::test]
    async fn test_ivf_pq_defer_compaction_with_deletions() {
        use lance_index::vector::pq::PQBuildParams;
        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            small_ivf(),
            PQBuildParams {
                max_iters: 2,
                num_sub_vectors: 2,
                ..Default::default()
            },
        );
        check_vector_defer_compaction(params, Some("id < 1500"), 10, 8).await;
    }

    #[tokio::test]
    async fn test_ivf_sq_defer_compaction_with_deletions() {
        use lance_index::vector::sq::builder::SQBuildParams;
        let params = VectorIndexParams::with_ivf_sq_params(
            DistanceType::L2,
            small_ivf(),
            SQBuildParams::default(),
        );
        check_vector_defer_compaction(params, Some("id < 1500"), 10, 8).await;
    }

    #[tokio::test]
    async fn test_ivf_rq_defer_compaction_with_deletions() {
        use lance_index::vector::bq::RQBuildParams;
        let params = VectorIndexParams::with_ivf_rq_params(
            DistanceType::L2,
            small_ivf(),
            RQBuildParams::new(1),
        );
        check_vector_defer_compaction(params, Some("id < 1500"), 10, 8).await;
    }

    /// Merge-only deferred compaction, then a PHYSICAL remap + FRI trim. Asserts
    /// the index is rebuilt, the fragment-reuse index trims to zero versions,
    /// and KNN stays consistent with the pre-compaction answer through both the
    /// FRI window and the physical remap. (HNSW rebuilds its graph on physical
    /// remap, so the overlap is recall-tolerant.)
    async fn check_vector_remap_and_trim(
        params: VectorIndexParams,
        k: usize,
        window_overlap: usize,
        post_remap_overlap: Option<usize>,
    ) {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float32Type, Int32Type};
        use lance_datagen::Dimension;

        const DIM: u32 = 32;
        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(DIM)),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".into()),
                &params,
                false,
            )
            .await
            .unwrap();
        let original_uuid = dataset
            .load_index_by_name("vec_idx")
            .await
            .unwrap()
            .unwrap()
            .uuid;

        // Sample queries from stored vectors + capture the pre-compaction answer.
        let mut rows: Vec<Vec<f32>> = Vec::new();
        {
            let mut scanner = dataset.scan();
            scanner.project(&["vec"]).unwrap();
            let batches = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for batch in &batches {
                let vecs = batch["vec"].as_fixed_size_list();
                for i in 0..batch.num_rows() {
                    let v = vecs.value(i);
                    rows.push(v.as_primitive::<Float32Type>().values().to_vec());
                }
            }
        }
        let step = (rows.len() / 16).max(1);
        let queries: Vec<Vec<f32>> = rows.iter().step_by(step).cloned().collect();
        let mut baseline: Vec<Vec<i32>> = Vec::new();
        for q in &queries {
            baseline.push(vector_knn_ids(&dataset, q, k).await);
        }

        // Merge-only deferred compaction.
        let metrics = compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0);
        assert_eq!(
            dataset
                .load_index_by_name("vec_idx")
                .await
                .unwrap()
                .unwrap()
                .uuid,
            original_uuid,
            "index must not be physically remapped yet (FRI window)"
        );
        for (i, q) in queries.iter().enumerate() {
            let window = vector_knn_ids(&dataset, q, k).await;
            let overlap = window.iter().filter(|id| baseline[i].contains(id)).count();
            assert!(
                overlap >= window_overlap,
                "FRI-window KNN diverged: overlap {overlap} < {window_overlap} (query #{i})"
            );
        }

        // Physical remap + trim the fragment-reuse index.
        remapping::remap_column_index(&mut dataset, &["vec"], Some("vec_idx".into()))
            .await
            .unwrap();
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();

        let remapped_uuid = dataset
            .load_index_by_name("vec_idx")
            .await
            .unwrap()
            .unwrap()
            .uuid;
        assert_ne!(
            remapped_uuid, original_uuid,
            "index should have been physically remapped"
        );
        if let Some(meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        {
            let versions = load_frag_reuse_index_details(&dataset, &meta)
                .await
                .unwrap()
                .versions
                .len();
            assert_eq!(versions, 0, "frag-reuse index must trim to zero versions");
        }

        for (i, q) in queries.iter().enumerate() {
            let after = vector_knn_ids(&dataset, q, k).await;
            // No stale/desynced addresses (a bad address fails the take above).
            assert!(
                !after.is_empty(),
                "post-remap KNN returned no rows (query #{i})"
            );
            // Physical remap rebuilds the HNSW graph, so recall is only compared
            // for the exact (non-HNSW) types.
            if let Some(min_overlap) = post_remap_overlap {
                let overlap = after.iter().filter(|id| baseline[i].contains(id)).count();
                assert!(
                    overlap >= min_overlap,
                    "post-remap KNN diverged: overlap {overlap} < {min_overlap} (query #{i})"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_ivf_flat_remap_and_trim() {
        let params = VectorIndexParams::with_ivf_flat_params(DistanceType::L2, small_ivf());
        check_vector_remap_and_trim(params, 10, 8, Some(8)).await;
    }

    // Regression: PQ storage used to remap its codes through the frag-reuse
    // index but keep the pre-remap `row_ids` field, so search returned stale
    // (compacted-away) addresses and the take failed with "fragment ... does
    // not exist" — even merge-only, and only observable when the query fetches
    // row content (the existing `test_read_ivf_pq_index_v3_with_defer_index_remap`
    // projects no columns, so it never takes and missed this).
    #[tokio::test]
    async fn test_ivf_pq_remap_and_trim() {
        use lance_index::vector::pq::PQBuildParams;
        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            small_ivf(),
            PQBuildParams {
                max_iters: 2,
                num_sub_vectors: 2,
                ..Default::default()
            },
        );
        check_vector_remap_and_trim(params, 10, 8, Some(8)).await;
    }

    #[tokio::test]
    async fn test_ivf_sq_remap_and_trim() {
        use lance_index::vector::sq::builder::SQBuildParams;
        let params = VectorIndexParams::with_ivf_sq_params(
            DistanceType::L2,
            small_ivf(),
            SQBuildParams::default(),
        );
        check_vector_remap_and_trim(params, 10, 8, Some(8)).await;
    }

    #[tokio::test]
    async fn test_ivf_rq_remap_and_trim() {
        use lance_index::vector::bq::RQBuildParams;
        let params = VectorIndexParams::with_ivf_rq_params(
            DistanceType::L2,
            small_ivf(),
            RQBuildParams::new(1),
        );
        check_vector_remap_and_trim(params, 10, 8, Some(8)).await;
    }

    #[tokio::test]
    async fn test_ivf_hnsw_sq_remap_and_trim() {
        use lance_index::vector::{hnsw::builder::HnswBuildParams, sq::builder::SQBuildParams};
        let params = VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::L2,
            small_ivf(),
            HnswBuildParams::default(),
            SQBuildParams::default(),
        );
        // Physical remap rebuilds the HNSW graph, so use a recall-tolerant overlap.
        check_vector_remap_and_trim(params, 10, 7, None).await;
    }

    #[tokio::test]
    async fn test_ivf_hnsw_pq_remap_and_trim() {
        use lance_index::vector::{hnsw::builder::HnswBuildParams, pq::PQBuildParams};
        let params = VectorIndexParams::with_ivf_hnsw_pq_params(
            DistanceType::L2,
            small_ivf(),
            HnswBuildParams::default(),
            PQBuildParams {
                max_iters: 2,
                num_sub_vectors: 2,
                ..Default::default()
            },
        );
        check_vector_remap_and_trim(params, 10, 7, None).await;
    }

    // Scalar index correctness across deferred compaction WITH materialized
    // deletions. The existing test_read_*_index_with_defer_index_remap tests are
    // merge-only and project no columns (count-only), so they never take and
    // never exercise the deletion drop path. These add an `id` column, delete a
    // prefix, defer-compact, then run the indexed query *projecting id* (a take)
    // and assert no deleted row is returned. Bitmap/BTree have no positional
    // internal structure so the drop path is exact; the Inverted (FTS) index
    // does (see its test below), and currently desyncs under deletions.

    #[tokio::test]
    async fn test_bitmap_index_defer_compaction_with_deletions() {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int32Type;
        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();
        dataset
            .create_index(
                &["category"],
                IndexType::Bitmap,
                Some("category_idx".into()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        dataset.delete("id < 1500").await.unwrap();
        let metrics = compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0);
        assert!(
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME),
            "deferred compaction must record a frag-reuse index"
        );

        let mut scanner = dataset.scan();
        scanner.filter("category = 3").unwrap();
        scanner.project(&["id"]).unwrap();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut returned = 0;
        for b in &batches {
            for id in b["id"].as_primitive::<Int32Type>().values() {
                assert!(
                    *id >= 1500,
                    "bitmap returned deleted id {id} in the FRI window"
                );
                returned += 1;
            }
        }
        assert!(returned > 0, "expected surviving category=3 rows");
    }

    // NOTE: Inverted/FTS under materialized deletions is broken (BM25 scores
    // via positional num_tokens[doc_id]; the frag-reuse drop shifts doc_id
    // positions -> out-of-bounds). It is gated off defer in the data plane
    // until fixed, so there is no lance-level reproducer here. Merge-only FTS
    // is covered by test_read_inverted_index_with_defer_index_remap.

    #[tokio::test]
    async fn test_default_compaction_planner() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();
        let schema = data.schema();

        // Create dataset with multiple small fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], schema.clone());
        let write_params = WriteParams {
            max_rows_per_file: 2000,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.get_fragments().len(), 5);

        // Test default planner
        let options = CompactionOptions {
            target_rows_per_fragment: 5000,
            materialize_deletions_threshold: 2.0,
            ..Default::default()
        };

        let planner = DefaultCompactionPlanner::new(options);
        let plan = planner.plan(&dataset).await.unwrap();

        // Should create tasks to compact small fragments
        assert!(!plan.tasks.is_empty());
        assert_eq!(plan.read_version, dataset.manifest.version);
        // make sure options.validate() worked
        assert!(!plan.options.materialize_deletions);
    }

    #[test]
    fn test_from_dataset_config() {
        let config = HashMap::from([
            (
                "lance.compaction.target_rows_per_fragment".to_string(),
                "500000".to_string(),
            ),
            (
                "lance.compaction.max_rows_per_group".to_string(),
                "2048".to_string(),
            ),
            (
                "lance.compaction.max_bytes_per_file".to_string(),
                "1000000".to_string(),
            ),
            (
                "lance.compaction.materialize_deletions".to_string(),
                "false".to_string(),
            ),
            (
                "lance.compaction.materialize_deletions_threshold".to_string(),
                "0.25".to_string(),
            ),
            (
                "lance.compaction.defer_index_remap".to_string(),
                "true".to_string(),
            ),
            (
                "lance.compaction.batch_size".to_string(),
                "4096".to_string(),
            ),
            (
                "lance.compaction.io_buffer_size".to_string(),
                "1073741824".to_string(),
            ),
            (
                "lance.compaction.compaction_mode".to_string(),
                "try_binary_copy".to_string(),
            ),
            (
                "lance.compaction.binary_copy_read_batch_bytes".to_string(),
                "8388608".to_string(),
            ),
            (
                "lance.compaction.index_remap_mode".to_string(),
                "compact".to_string(),
            ),
        ]);

        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        assert_eq!(opts.target_rows_per_fragment, 500_000);
        assert_eq!(opts.max_rows_per_group, 2048);
        assert_eq!(opts.max_bytes_per_file, Some(1_000_000));
        assert!(!opts.materialize_deletions);
        assert!((opts.materialize_deletions_threshold - 0.25).abs() < f32::EPSILON);
        assert!(opts.defer_index_remap);
        assert_eq!(opts.batch_size, Some(4096));
        assert_eq!(opts.io_buffer_size, Some(1_073_741_824));
        assert_eq!(opts.compaction_mode, Some(CompactionMode::TryBinaryCopy));
        assert_eq!(opts.binary_copy_read_batch_bytes, Some(8_388_608));
        // A non-default value proves the config string was actually parsed.
        assert_eq!(opts.index_remap_mode, IndexRemapMode::Compact);
    }

    #[test]
    fn test_from_dataset_config_empty() {
        let config = HashMap::new();
        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        let defaults = CompactionOptions::default();
        assert_eq!(
            opts.target_rows_per_fragment,
            defaults.target_rows_per_fragment
        );
        assert_eq!(opts.max_rows_per_group, defaults.max_rows_per_group);
        assert_eq!(opts.max_bytes_per_file, defaults.max_bytes_per_file);
        assert_eq!(opts.materialize_deletions, defaults.materialize_deletions);
        assert_eq!(
            opts.materialize_deletions_threshold,
            defaults.materialize_deletions_threshold
        );
        assert_eq!(opts.defer_index_remap, defaults.defer_index_remap);
        assert_eq!(opts.index_remap_mode, defaults.index_remap_mode);
        assert_eq!(opts.index_remap_mode, IndexRemapMode::Direct);
        assert_eq!(opts.batch_size, defaults.batch_size);
        assert_eq!(opts.compaction_mode, defaults.compaction_mode);
        assert_eq!(
            opts.binary_copy_read_batch_bytes,
            defaults.binary_copy_read_batch_bytes
        );
    }

    #[test]
    fn test_from_dataset_config_partial() {
        let config = HashMap::from([(
            "lance.compaction.target_rows_per_fragment".to_string(),
            "500000".to_string(),
        )]);

        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        assert_eq!(opts.target_rows_per_fragment, 500_000);
        // Other fields should remain at defaults
        let defaults = CompactionOptions::default();
        assert_eq!(opts.max_rows_per_group, defaults.max_rows_per_group);
        assert_eq!(opts.max_bytes_per_file, defaults.max_bytes_per_file);
        assert_eq!(opts.materialize_deletions, defaults.materialize_deletions);
        assert_eq!(opts.defer_index_remap, defaults.defer_index_remap);
        assert_eq!(opts.batch_size, defaults.batch_size);
        assert_eq!(opts.compaction_mode, defaults.compaction_mode);
        assert_eq!(
            opts.binary_copy_read_batch_bytes,
            defaults.binary_copy_read_batch_bytes
        );
    }

    #[test]
    fn test_from_dataset_config_ignores_other_keys() {
        let config = HashMap::from([
            (
                "lance.compaction.target_rows_per_fragment".to_string(),
                "500000".to_string(),
            ),
            (
                "lance.auto_cleanup.interval".to_string(),
                "3600".to_string(),
            ),
            ("some.other.key".to_string(), "value".to_string()),
        ]);

        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        assert_eq!(opts.target_rows_per_fragment, 500_000);
    }

    #[test]
    fn test_from_dataset_config_invalid_value() {
        let config = HashMap::from([(
            "lance.compaction.target_rows_per_fragment".to_string(),
            "not_a_number".to_string(),
        )]);

        let result = CompactionOptions::from_dataset_config(&config);
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("target_rows_per_fragment"));
        assert!(err_msg.contains("not_a_number"));
    }

    #[test]
    fn test_from_dataset_config_invalid_bool() {
        let config = HashMap::from([(
            "lance.compaction.materialize_deletions".to_string(),
            "yes".to_string(),
        )]);

        let result = CompactionOptions::from_dataset_config(&config);
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("materialize_deletions"));
        assert!(err_msg.contains("yes"));
    }

    #[test]
    fn test_from_dataset_config_unknown_compaction_key() {
        // Unknown keys should be ignored (with a warning) for forwards compatibility
        let config = HashMap::from([(
            "lance.compaction.unknown_key".to_string(),
            "value".to_string(),
        )]);

        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        // Should return defaults since the unknown key is skipped
        let defaults = CompactionOptions::default();
        assert_eq!(
            opts.target_rows_per_fragment,
            defaults.target_rows_per_fragment
        );
    }

    #[test]
    fn test_from_dataset_config_invalid_compaction_mode() {
        let config = HashMap::from([(
            "lance.compaction.compaction_mode".to_string(),
            "invalid_mode".to_string(),
        )]);

        let result = CompactionOptions::from_dataset_config(&config);
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid_mode"));
    }

    #[test]
    fn test_from_dataset_config_max_overlays_per_fragment() {
        let key = "lance.compaction.max_overlays_per_fragment".to_string();

        // An integer sets the threshold.
        let config = HashMap::from([(key.clone(), "3".to_string())]);
        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        assert_eq!(opts.max_overlays_per_fragment, Some(3));

        // "none" (case-insensitive) disables the trigger, overriding the Some(10) default.
        let config = HashMap::from([(key.clone(), "None".to_string())]);
        let opts = CompactionOptions::from_dataset_config(&config).unwrap();
        assert_eq!(opts.max_overlays_per_fragment, None);

        // Anything else is rejected.
        let config = HashMap::from([(key, "not_a_number".to_string())]);
        let err_msg = CompactionOptions::from_dataset_config(&config)
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("max_overlays_per_fragment"));
        assert!(err_msg.contains("not_a_number"));
    }

    #[test]
    fn test_apply_dataset_config_overrides() {
        let config = HashMap::from([(
            "lance.compaction.target_rows_per_fragment".to_string(),
            "500000".to_string(),
        )]);

        let mut opts = CompactionOptions {
            max_rows_per_group: 4096,
            ..Default::default()
        };
        opts.apply_dataset_config(&config).unwrap();

        // Config value should be applied
        assert_eq!(opts.target_rows_per_fragment, 500_000);
        // Explicitly set value should be preserved (config didn't have this key)
        assert_eq!(opts.max_rows_per_group, 4096);
    }

    #[test]
    fn test_apply_dataset_config_overwrites_matching_field() {
        let config = HashMap::from([(
            "lance.compaction.max_rows_per_group".to_string(),
            "2048".to_string(),
        )]);

        let mut opts = CompactionOptions {
            max_rows_per_group: 4096,
            ..Default::default()
        };
        opts.apply_dataset_config(&config).unwrap();

        // Config value should overwrite the pre-set value
        assert_eq!(opts.max_rows_per_group, 2048);
    }

    #[tokio::test]
    async fn test_max_source_fragments() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();
        let schema = data.schema();

        // Create 10 small fragments (100 rows each) via 10 appends
        let write_params = WriteParams {
            max_rows_per_file: 100,
            ..Default::default()
        };
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(data.slice(0, 100))], schema.clone()),
            test_uri,
            Some(write_params.clone()),
        )
        .await
        .unwrap();
        for i in 1..10 {
            let mut append_params = write_params.clone();
            append_params.mode = WriteMode::Append;
            Dataset::write(
                RecordBatchIterator::new(vec![Ok(data.slice(i * 100, 100))], schema.clone()),
                test_uri,
                Some(append_params),
            )
            .await
            .unwrap();
        }

        let dataset = Dataset::open(test_uri).await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 10);

        // Plan without limit - all 10 fragments should be candidates.
        // Use a target that splits the 10 fragments into multiple tasks.
        let opts_no_limit = CompactionOptions {
            target_rows_per_fragment: 250,
            ..Default::default()
        };
        let plan_all = plan_compaction(&dataset, &opts_no_limit).await.unwrap();
        let total_source_frags: usize = plan_all.tasks().iter().map(|t| t.fragments.len()).sum();
        assert_eq!(total_source_frags, 10);
        assert!(
            plan_all.num_tasks() > 2,
            "need multiple tasks to test bounding, got {}",
            plan_all.num_tasks()
        );

        // Plan with max_source_fragments=4 should include tasks covering <= 4
        // source fragments
        let opts_bounded = CompactionOptions {
            target_rows_per_fragment: 250,
            max_source_fragments: Some(4),
            ..Default::default()
        };
        let plan_bounded = plan_compaction(&dataset, &opts_bounded).await.unwrap();
        let bounded_source_frags: usize =
            plan_bounded.tasks().iter().map(|t| t.fragments.len()).sum();
        assert!(
            bounded_source_frags <= 4,
            "expected at most 4 source fragments, got {bounded_source_frags}"
        );
        assert!(
            bounded_source_frags > 0,
            "expected at least 1 source fragment in bounded plan"
        );
        assert!(
            plan_bounded.num_tasks() < plan_all.num_tasks(),
            "bounded plan ({}) should have fewer tasks than unbounded ({})",
            plan_bounded.num_tasks(),
            plan_all.num_tasks()
        );

        // Execute bounded compaction incrementally
        let mut dataset = dataset;
        compact_files(&mut dataset, opts_bounded, None)
            .await
            .unwrap();
        let after_first = dataset.get_fragments().len();
        assert!(
            after_first < 10,
            "expected fewer than 10 fragments after first compaction, got {after_first}"
        );
        assert!(
            after_first > 1,
            "expected partial compaction (not fully compacted), got {after_first}"
        );

        // Run again to make more progress
        let opts_bounded = CompactionOptions {
            target_rows_per_fragment: 250,
            max_source_fragments: Some(4),
            ..Default::default()
        };
        compact_files(&mut dataset, opts_bounded, None)
            .await
            .unwrap();
        let after_second = dataset.get_fragments().len();
        assert!(
            after_second <= after_first,
            "expected progress: {after_second} should be <= {after_first}"
        );
    }

    #[tokio::test]
    async fn test_compaction_uses_manifest_config() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = sample_data();
        let schema = data.schema();

        // Create dataset with small fragments
        let reader = RecordBatchIterator::new(vec![Ok(data.clone())], schema.clone());
        let write_params = WriteParams {
            max_rows_per_file: 2000,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.get_fragments().len(), 5);

        // Set compaction config in manifest
        dataset
            .update_config([
                ("lance.compaction.target_rows_per_fragment", "5000"),
                ("lance.compaction.materialize_deletions_threshold", "2.0"),
            ])
            .await
            .unwrap();

        // Build options from the dataset config (as the bindings do)
        let opts = CompactionOptions::from_dataset_config(&dataset.manifest.config).unwrap();
        assert_eq!(opts.target_rows_per_fragment, 5000);
        assert!((opts.materialize_deletions_threshold - 2.0).abs() < f32::EPSILON);

        // Verify the config flows through plan_compaction
        let plan = plan_compaction(&dataset, &opts).await.unwrap();
        assert!(!plan.tasks.is_empty());
        assert_eq!(plan.options.target_rows_per_fragment, 5000);
        // validate() should have turned off materialize_deletions since threshold >= 1.0
        assert!(!plan.options.materialize_deletions);
    }

    // check_rewrite_txn takes the (None, Some(_)) branch when a Rewrite with
    // defer_index_remap=true is committed against a previously committed
    // CreateIndex, declaring COMPATIBLE without verifying that the Rewrite's
    // FRI groups don't straddle the CreateIndex's fragment bitmap. When a
    // group mixes indexed and unindexed fragments, commit succeeds and later
    // queries fail at load_indices with "split of indexed and non-indexed
    // data".
    #[tokio::test]
    async fn test_rewrite_fri_vs_create_index_conflict() {
        use crate::index::DatasetIndexExt;
        use crate::index::vector::VectorIndexParams;
        use futures::TryStreamExt;
        use lance_datagen::{BatchCount, Dimension, RowCount, array, gen_batch};
        use lance_index::IndexType;
        use lance_linalg::distance::MetricType;

        async fn append_fragment(uri: &str, rows: u64) -> Dataset {
            let reader = gen_batch()
                .col("vec", array::rand_vec::<Float32Type>(Dimension::from(16)))
                .into_reader_rows(RowCount::from(rows), BatchCount::from(1));
            let params = WriteParams {
                max_rows_per_file: rows as usize,
                mode: WriteMode::Append,
                ..Default::default()
            };
            Dataset::write(reader, uri, Some(params)).await.unwrap()
        }

        let tmpdir = TempStrDir::default();
        let uri = format!("file://{}", tmpdir.as_str());

        // frag0 (256 rows) with a base IVF index.
        let reader = gen_batch()
            .col("vec", array::rand_vec::<Float32Type>(Dimension::from(16)))
            .into_reader_rows(RowCount::from(256), BatchCount::from(1));
        let mut dataset = Dataset::write(
            reader,
            &uri,
            Some(WriteParams {
                max_rows_per_file: 256,
                mode: WriteMode::Overwrite,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let index_params = VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 50);
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &index_params, true)
            .await
            .unwrap();

        // Append frag1 (unindexed), snapshot a stale handle pointing here,
        // then append frag2 (also unindexed).
        dataset = append_fragment(&uri, 64).await;
        let mut stale = dataset.clone();
        dataset = append_fragment(&uri, 64).await;

        // Plan + execute compaction of frag1+frag2 with deferred remap.
        let options = CompactionOptions {
            defer_index_remap: true,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert!(!plan.tasks.is_empty());
        let snapshot = dataset.clone();
        let completed: Vec<RewriteResult> = futures::stream::iter(plan.tasks.into_iter())
            .map(|task| rewrite_files(Cow::Borrowed(&snapshot), task, &options))
            .buffer_unordered(1)
            .try_collect()
            .await
            .unwrap();

        // optimize_indices on the stale handle indexes frag1 only (frag2
        // didn't exist at that version), commits as CreateIndex. `dataset`
        // stays at its pre-optimize version so the Rewrite commit has to
        // conflict-check against this CreateIndex.
        stale
            .optimize_indices(&lance_index::optimize::OptimizeOptions::append())
            .await
            .unwrap();

        // Commit the pre-executed Rewrite. The FRI group [frag1, frag2]
        // straddles the new CreateIndex bitmap (frag1 indexed, frag2 not), so
        // check_rewrite_txn must reject this as a retryable conflict rather
        // than letting it commit into a broken state that fails queries.
        let err = commit_compaction(
            &mut dataset,
            completed,
            Arc::new(DatasetIndexRemapperOptions::default()),
            &options,
        )
        .await
        .expect_err("commit should fail with retryable conflict");
        assert!(
            matches!(err, Error::RetryableCommitConflict { .. }),
            "unexpected error: {err}"
        );
    }

    /// Reproduce the distributed-compaction concurrent-delete data-resurrection bug.
    ///
    /// In the distributed (Spark) path the caller opens **two separate** `Dataset` handles:
    ///
    /// 1. dataset_plan  — used for `plan_compaction` (version = V)
    /// 2. dataset_commit — opened **fresh** for `commit_compaction` (version = V+N)
    ///
    /// Because `commit_compaction` builds the `Rewrite` transaction with
    /// `dataset.manifest.version` (= V+N), `load_and_sort_new_transactions` only
    /// scans versions after V+N and finds nothing.  Any concurrent DELETE that
    /// happened between V and V+N is silently ignored, causing the deleted rows to
    /// reappear in the compacted fragment.
    ///
    /// After the fix, `commit_compaction` uses `min(tasks.read_version)` (= V) as
    /// the transaction `read_version`, so the conflict checker correctly loads and
    /// rejects the DELETE, returning a retryable conflict error instead of silently
    /// resurrecting data.
    #[tokio::test]
    async fn test_distributed_compact_concurrent_delete_no_resurrection() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        // Write 4 fragments × 1 000 rows each (a=0..4000).
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..4_000))],
        )
        .unwrap();
        let mut dataset_plan = Dataset::write(
            RecordBatchIterator::new(vec![Ok(data)], schema.clone()),
            test_uri,
            Some(WriteParams {
                max_rows_per_file: 1_000,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset_plan.manifest.version, 1);
        assert_eq!(dataset_plan.get_fragments().len(), 4);

        // ── Step 1: plan compaction at version V=1 ───────────────────────────────
        let options = CompactionOptions {
            target_rows_per_fragment: 10_000,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset_plan, &options).await.unwrap();
        assert_eq!(plan.tasks().len(), 1, "expected one compaction task");

        // ── Step 2: execute tasks (simulating distributed executors at V=1) ──────
        // Clone dataset_plan so the closure can own its copy while the original
        // remains available for the concurrent DELETE in Step 3.
        let dataset_for_tasks = dataset_plan.clone();
        let results: Vec<RewriteResult> = futures::stream::iter(plan.compaction_tasks())
            .then(|task| {
                let ds = dataset_for_tasks.clone();
                async move {
                    // Executors open the dataset at the planned read_version
                    task.execute(&ds).await.unwrap()
                }
            })
            .collect()
            .await;
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].read_version, 1,
            "tasks must carry read_version=1"
        );

        // ── Step 3: concurrent DELETE commits at V=2 ─────────────────────────────
        // Delete rows where a < 1000 (the first 1 000 rows in fragment 0).
        dataset_plan.delete("a < 1000").await.unwrap();
        assert_eq!(dataset_plan.manifest.version, 2);

        // ── Step 4: the Spark driver opens a *fresh* dataset (latest = V=2) ──────
        // This is exactly what OptimizeExec.scala does for commitCompaction.
        let mut dataset_commit = Dataset::open(test_uri).await.unwrap();
        assert_eq!(
            dataset_commit.manifest.version, 2,
            "fresh dataset must be at the post-delete version"
        );

        // ── Step 5: commit_compaction with the stale results ─────────────────────
        let commit_result = commit_compaction(
            &mut dataset_commit,
            results,
            Arc::new(IgnoreRemap::default()),
            &options,
        )
        .await;

        // ── Step 6: assert correct behaviour ─────────────────────────────────────
        // BEFORE fix: commit_result is Ok(…) and the deleted rows are resurrected.
        // AFTER  fix: commit_result is Err(retryable conflict), protecting data integrity.
        assert!(
            commit_result.is_err(),
            "commit_compaction must fail with a conflict error when a concurrent \
             DELETE touched the same fragments; got Ok instead — deleted rows were \
             silently resurrected"
        );
        let err_msg = commit_result.unwrap_err().to_string();
        assert!(
            err_msg.contains("retryable")
                || err_msg.contains("conflict")
                || err_msg.contains("preempted"),
            "expected a retryable conflict error, got: {err_msg}"
        );

        // The on-disk table must still reflect the DELETE (a < 1000 remains absent).
        let latest = Dataset::open(test_uri).await.unwrap();
        let row_count = latest
            .count_rows(Some("a < 1000".to_string()))
            .await
            .unwrap();
        assert_eq!(
            row_count, 0,
            "rows deleted before compaction must not be resurrected; found {row_count}"
        );
    }

    fn count_all_files_in(dir: &std::path::Path) -> std::io::Result<usize> {
        if !dir.exists() {
            return Ok(0);
        }
        let mut count = 0;
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                count += count_all_files_in(&path)?;
            } else if path.is_file() {
                // Ignore macOS system files if any
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|file_name| !file_name.starts_with('.'))
                {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    fn count_data_files_in(base_dir: &str) -> usize {
        let data_dir = std::path::Path::new(base_dir).join("data");
        count_all_files_in(&data_dir).unwrap_or(0)
    }

    /// Once `commit_compaction` reaches the Rewrite commit, its input may be a
    /// replay of a result whose earlier commit landed ambiguously. A failure on
    /// this call must therefore leave data files for GC instead of deleting
    /// files that an existing version may reference.
    #[tokio::test]
    async fn test_commit_compaction_leaves_data_for_gc_on_commit_failure() {
        use crate::dataset::builder::DatasetBuilder;
        use crate::utils::test::FailingProxyStore;
        use lance_io::object_store::ObjectStoreParams;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        // Prefix `/` so Windows drive letters (e.g. `C:`) don't get parsed as
        // the URL authority.
        let path_prefix = if test_uri.starts_with('/') { "" } else { "/" };
        let routed_uri = format!("file-object-store://{path_prefix}{test_uri}");

        let data = sample_data();
        let reader = RecordBatchIterator::new(vec![Ok(data.slice(0, 200))], data.schema());
        Dataset::write(
            reader,
            &routed_uri,
            Some(WriteParams {
                max_rows_per_file: 100,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let baseline_files = count_data_files_in(test_uri);

        let failing = Arc::new(FailingProxyStore::new());
        // `commit_compaction` first calls `reserve_fragment_ids` (which writes a
        // ReserveFragments transaction) and then calls `apply_commit` for the
        // rewrite itself. Skip the first transaction write so the reserve
        // succeeds, and fail the second so `apply_commit` errors out — that's
        // the branch we want to exercise cleanup for.
        failing.fail_after_n("put", "_transactions", 1, "injected commit failure");
        failing.fail_after_n(
            "put_multipart",
            "_transactions",
            1,
            "injected commit failure",
        );

        let mut dataset = DatasetBuilder::from_uri(&routed_uri)
            .with_read_params(crate::dataset::ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(failing.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 1000,
            ..Default::default()
        };
        let result = compact_files(&mut dataset, options, None).await;
        assert!(
            result.is_err(),
            "Compaction should fail when transaction commit fails"
        );

        assert!(
            count_data_files_in(test_uri) > baseline_files,
            "Compaction data files should be retained for GC after the Rewrite commit fails"
        );
    }

    #[tokio::test]
    async fn test_commit_compaction_leaves_blob_v2_sidecars_for_gc_on_commit_failure() {
        use crate::BlobArrayBuilder;
        use crate::dataset::builder::DatasetBuilder;
        use crate::utils::test::FailingProxyStore;
        use lance_io::object_store::ObjectStoreParams;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let path_prefix = if test_uri.starts_with('/') { "" } else { "/" };
        let routed_uri = format!("file-object-store://{path_prefix}{test_uri}");

        let id_array = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        // Use one packed blob and one dedicated blob to verify both sidecar layouts.
        let packed_data = vec![0u8; 100 * 1024];
        let dedicated_data = vec![1u8; 5 * 1024 * 1024];
        let mut blob_builder = BlobArrayBuilder::new(2);
        blob_builder.push_bytes(&packed_data).unwrap();
        blob_builder.push_bytes(&dedicated_data).unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        Dataset::write(
            reader,
            &routed_uri,
            Some(WriteParams {
                max_rows_per_file: 1, // Create 2 fragments
                enable_stable_row_ids: true,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let baseline_files = count_data_files_in(test_uri);

        let failing = Arc::new(FailingProxyStore::new());
        failing.fail_after_n("put", "_transactions", 1, "injected commit failure");
        failing.fail_after_n(
            "put_multipart",
            "_transactions",
            1,
            "injected commit failure",
        );

        let mut dataset = DatasetBuilder::from_uri(&routed_uri)
            .with_read_params(crate::dataset::ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(failing.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();

        let options = CompactionOptions {
            target_rows_per_fragment: 1000,
            ..Default::default()
        };
        let result = compact_files(&mut dataset, options, None).await;
        assert!(
            result.is_err(),
            "Compaction should fail when transaction commit fails"
        );

        assert!(
            count_data_files_in(test_uri) > baseline_files,
            "Blob v2 sidecars should be retained for GC after the Rewrite commit fails"
        );
    }

    async fn read_blob_bytes_by_index(
        dataset: &Arc<Dataset>,
        column: &str,
    ) -> Vec<(i32, Option<Vec<u8>>)> {
        let mut scanner = dataset.scan();
        scanner.with_row_id();
        let batch = scanner
            .project(&["id", column])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let row_ids = batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_primitive::<UInt64Type>();

        let mut result = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let row_id = row_ids.value(i);
            let id = ids.value(i);
            let blobs = dataset.take_blobs(&[row_id], column).await.unwrap();
            match blobs.into_iter().next().flatten() {
                Some(blob) => {
                    let data = blob.read().await.unwrap();
                    result.push((id, Some(data.to_vec())));
                }
                None => result.push((id, None)),
            }
        }
        result
    }

    fn mixed_blob_values() -> Vec<(i32, Option<Vec<u8>>)> {
        vec![
            (0, Some(vec![b'0'; 80])),
            (1, None),
            (2, Some(Vec::new())),
            (3, Some(vec![b'3'; 80])),
            (4, Some(vec![b'4'; 80])),
            (5, Some(vec![b'5'; 80])),
        ]
    }

    async fn assert_compaction_preserves_blob_values(
        mut dataset: Dataset,
        expected: &[(i32, Option<Vec<u8>>)],
    ) {
        assert_eq!(dataset.get_fragments().len(), 3);

        let mut before = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        before.sort_by_key(|(id, _)| *id);
        assert_eq!(before, expected);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut after = read_blob_bytes_by_index(&Arc::new(dataset), "blob").await;
        after.sort_by_key(|(id, _)| *id);
        assert_eq!(after, expected);
    }

    #[tokio::test]
    async fn test_compact_blob_v1_preserves_null_empty_and_payload_order() {
        let test_dir = TempStrDir::default();
        let expected = mixed_blob_values();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("blob", DataType::LargeBinary, true)
                .with_metadata([(BLOB_META_KEY.to_string(), "true".to_string())].into()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..expected.len() as i32)),
                Arc::new(LargeBinaryArray::from_iter(
                    expected.iter().map(|(_, value)| value.as_deref()),
                )),
            ],
        )
        .unwrap();
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &test_dir,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_0),
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_compaction_preserves_blob_values(dataset, &expected).await;
    }

    #[tokio::test]
    async fn test_compact_blob_v2_preserves_null_empty_and_payload_order() {
        use crate::BlobArrayBuilder;

        let test_dir = TempStrDir::default();
        let expected = mixed_blob_values();
        let mut blob_builder = BlobArrayBuilder::new(expected.len());
        for (_, value) in &expected {
            match value {
                Some(value) => blob_builder.push_bytes(value).unwrap(),
                None => blob_builder.push_null().unwrap(),
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..expected.len() as i32)),
                blob_builder.finish().unwrap(),
            ],
        )
        .unwrap();
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &test_dir,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_compaction_preserves_blob_values(dataset, &expected).await;
    }

    #[tokio::test]
    async fn test_compact_blob_v2_preserves_external_references() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;
        use lance_table::format::BasePath;

        let test_dir = TempDir::default();
        let external_dir = TempDir::default();
        let external_path = external_dir.std_path().join("external.bin");
        std::fs::write(&external_path, b"external-data").unwrap();
        let external_uri = format!("file://{}", external_path.display());
        let base_uri = format!("file://{}", external_dir.std_path().display());

        let mut blob_builder = BlobArrayBuilder::new(2);
        blob_builder.push_uri(external_uri.clone()).unwrap();
        blob_builder.push_bytes(b"inline-data").unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 1,
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("external".to_string()),
                    path: base_uri,
                    is_dataset_root: false,
                }]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        for frag in dataset.get_fragments() {
            let rows = frag.physical_rows().await.unwrap();
            assert!(rows > 0, "fragment {} should have rows", frag.id());
        }

        let options = CompactionOptions {
            target_rows_per_fragment: 1024 * 1024,
            ..Default::default()
        };
        let plan = plan_compaction(&dataset, &options).await.unwrap();
        assert!(
            !plan.tasks().is_empty(),
            "compaction plan should have tasks, got {} tasks",
            plan.tasks().len()
        );

        compact_files(&mut dataset, options, None).await.unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let scan_result = dataset
            .scan()
            .project(&["id", "blob"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(scan_result.num_rows(), 2);

        let ids = scan_result
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let mut id_values: Vec<i32> = ids.iter().map(|v| v.unwrap()).collect();
        id_values.sort();
        assert_eq!(id_values, vec![0, 1]);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"external-data".to_vec())),
                (1, Some(b"inline-data".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_packed_and_dedicated() {
        use crate::BlobArrayBuilder;
        use lance_arrow::BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let inline_data = b"small-inline-blob".as_slice();
        let packed_data: Vec<u8> = (0..64 * 1024 + 1024).map(|i| (i % 256) as u8).collect();
        let dedicated_data: Vec<u8> = (0..4 * 1024 * 1024 + 512)
            .map(|i| ((i + 97) % 256) as u8)
            .collect();

        let mut blob_builder = BlobArrayBuilder::new(3);
        blob_builder.push_bytes(inline_data).unwrap();
        blob_builder.push_bytes(&packed_data).unwrap();
        blob_builder.push_bytes(&dedicated_data).unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let mut blob_field = crate::blob_field("blob", true);
        {
            let metadata = blob_field.metadata().clone();
            let mut new_metadata = metadata;
            new_metadata.insert(
                BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY.to_string(),
                (4 * 1024 * 1024).to_string(),
            );
            blob_field = blob_field.with_metadata(new_metadata);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            blob_field,
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let scan_result = dataset
            .scan()
            .project(&["id", "blob"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(scan_result.num_rows(), 3);

        let ids = scan_result
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let id_values: Vec<i32> = ids.iter().map(|v| v.unwrap()).collect();
        assert_eq!(id_values, vec![0, 1, 2]);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(inline_data.to_vec())),
                (1, Some(packed_data)),
                (2, Some(dedicated_data))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_with_null_rows() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let mut blob_builder = BlobArrayBuilder::new(4);
        blob_builder.push_bytes(b"inline-0").unwrap();
        blob_builder.push_null().unwrap();
        blob_builder.push_bytes(b"inline-2").unwrap();
        blob_builder.push_null().unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(0), Some(1), Some(2), Some(3)]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let scan_result = dataset
            .scan()
            .project(&["id", "blob"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(scan_result.num_rows(), 4);

        let ids = scan_result
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let id_values: Vec<i32> = ids.iter().map(|v| v.unwrap()).collect();
        assert_eq!(id_values, vec![0, 1, 2, 3]);

        let blob_col = scan_result.column_by_name("blob").unwrap();
        assert!(
            matches!(blob_col.data_type(), DataType::Struct(_)),
            "blob column should be a struct after compaction"
        );

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"inline-0".to_vec())),
                (1, None),
                (2, Some(b"inline-2".to_vec())),
                (3, None)
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_deleted_rows_not_resurrected() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let mut blob_builder = BlobArrayBuilder::new(4);
        blob_builder.push_bytes(b"blob-0").unwrap();
        blob_builder.push_bytes(b"blob-1").unwrap();
        blob_builder.push_bytes(b"blob-2").unwrap();
        blob_builder.push_bytes(b"blob-3").unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        dataset.delete("id = 1").await.unwrap();
        dataset.delete("id = 2").await.unwrap();

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                materialize_deletions_threshold: 0.0,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        let scan_result = dataset
            .scan()
            .project(&["id", "blob"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(scan_result.num_rows(), 2);

        let ids = scan_result
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let mut id_values: Vec<i32> = ids.iter().map(|v| v.unwrap()).collect();
        id_values.sort();
        assert_eq!(id_values, vec![0, 3]);

        let blob_col = scan_result.column_by_name("blob").unwrap();
        let struct_arr = blob_col.as_any().downcast_ref::<StructArray>().unwrap();
        let kind_col = struct_arr
            .column_by_name("kind")
            .unwrap()
            .as_primitive::<UInt8Type>();

        for i in 0..kind_col.len() {
            assert!(
                !kind_col.is_null(i),
                "row {} should have a non-null kind after compaction of deleted rows",
                i
            );
        }

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![(0, Some(b"blob-0".to_vec())), (3, Some(b"blob-3".to_vec()))]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_external_and_data_blob_mixed() {
        use crate::BlobArrayBuilder;
        use lance_arrow::BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY;
        use lance_core::utils::tempfile::TempDir;
        use lance_table::format::BasePath;

        let test_dir = TempDir::default();
        let external_dir = TempDir::default();
        let external_path = external_dir.std_path().join("external.bin");
        std::fs::write(&external_path, b"external-payload").unwrap();
        let external_uri = format!("file://{}", external_path.display());
        let base_uri = format!("file://{}", external_dir.std_path().display());

        let packed_data: Vec<u8> = (0..64 * 1024 + 512).map(|i| (i % 256) as u8).collect();

        let mut blob_builder = BlobArrayBuilder::new(4);
        blob_builder.push_uri(external_uri.clone()).unwrap();
        blob_builder.push_bytes(&packed_data).unwrap();
        blob_builder.push_bytes(b"inline-small").unwrap();
        blob_builder.push_uri(external_uri.clone()).unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let mut blob_field = crate::blob_field("blob", true);
        {
            let mut new_metadata = blob_field.metadata().clone();
            new_metadata.insert(
                BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY.to_string(),
                (4 * 1024 * 1024).to_string(),
            );
            blob_field = blob_field.with_metadata(new_metadata);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            blob_field,
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("external".to_string()),
                    path: base_uri,
                    is_dataset_root: false,
                }]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"external-payload".to_vec())),
                (1, Some(packed_data)),
                (2, Some(b"inline-small".to_vec())),
                (3, Some(b"external-payload".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_multiple_blob_columns() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let mut image_builder = BlobArrayBuilder::new(3);
        image_builder.push_bytes(b"image-0").unwrap();
        image_builder.push_bytes(b"image-1").unwrap();
        image_builder.push_bytes(b"image-2").unwrap();
        let image_array: ArrayRef = image_builder.finish().unwrap();

        let mut thumb_builder = BlobArrayBuilder::new(3);
        thumb_builder.push_bytes(b"thumb-0").unwrap();
        thumb_builder.push_null().unwrap();
        thumb_builder.push_bytes(b"thumb-2").unwrap();
        let thumb_array: ArrayRef = thumb_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("image", true),
            crate::blob_field("thumbnail", true),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_array, image_array, thumb_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut image_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "image").await;
        image_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            image_values,
            vec![
                (0, Some(b"image-0".to_vec())),
                (1, Some(b"image-1".to_vec())),
                (2, Some(b"image-2".to_vec()))
            ]
        );

        let mut thumb_values =
            read_blob_bytes_by_index(&Arc::new(dataset.clone()), "thumbnail").await;
        thumb_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            thumb_values,
            vec![
                (0, Some(b"thumb-0".to_vec())),
                (1, None),
                (2, Some(b"thumb-2".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_external_and_null_mixed() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;
        use lance_table::format::BasePath;

        let test_dir = TempDir::default();
        let external_dir = TempDir::default();
        let external_path = external_dir.std_path().join("mixed-external.bin");
        std::fs::write(&external_path, b"external-mixed-data").unwrap();
        let external_uri = format!("file://{}", external_path.display());
        let base_uri = format!("file://{}", external_dir.std_path().display());

        let mut blob_builder = BlobArrayBuilder::new(4);
        blob_builder.push_uri(external_uri.clone()).unwrap();
        blob_builder.push_null().unwrap();
        blob_builder.push_uri(external_uri.clone()).unwrap();
        blob_builder.push_null().unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("external".to_string()),
                    path: base_uri,
                    is_dataset_root: false,
                }]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"external-mixed-data".to_vec())),
                (1, None),
                (2, Some(b"external-mixed-data".to_vec())),
                (3, None)
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_all_null_and_all_external_fragments() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;
        use lance_table::format::BasePath;

        let test_dir = TempDir::default();
        let external_dir = TempDir::default();
        let external_path = external_dir.std_path().join("all-ext.bin");
        std::fs::write(&external_path, b"all-external-data").unwrap();
        let external_uri = format!("file://{}", external_path.display());
        let base_uri = format!("file://{}", external_dir.std_path().display());

        let mut null_builder = BlobArrayBuilder::new(2);
        null_builder.push_null().unwrap();
        null_builder.push_null().unwrap();
        let null_array: ArrayRef = null_builder.finish().unwrap();

        let mut ext_builder = BlobArrayBuilder::new(2);
        ext_builder.push_uri(external_uri.clone()).unwrap();
        ext_builder.push_uri(external_uri.clone()).unwrap();
        let ext_array: ArrayRef = ext_builder.finish().unwrap();

        let id_null_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));
        let null_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));
        let null_batch =
            RecordBatch::try_new(null_schema.clone(), vec![id_null_array, null_array]).unwrap();

        let id_ext_array: ArrayRef = Arc::new(Int32Array::from(vec![2, 3]));
        let ext_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));
        let ext_batch =
            RecordBatch::try_new(ext_schema.clone(), vec![id_ext_array, ext_array]).unwrap();

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(
                vec![null_batch, ext_batch].into_iter().map(Ok),
                null_schema.clone(),
            ),
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("external".to_string()),
                    path: base_uri,
                    is_dataset_root: false,
                }]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, None),
                (1, None),
                (2, Some(b"all-external-data".to_vec())),
                (3, Some(b"all-external-data".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_external_with_multiple_base_ids() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;
        use lance_table::format::BasePath;

        let test_dir = TempDir::default();
        let base_a_dir = TempDir::default();
        let base_b_dir = TempDir::default();

        let path_a = base_a_dir.std_path().join("data-a.bin");
        std::fs::write(&path_a, b"from-base-a").unwrap();
        let uri_a = format!("file://{}", path_a.display());
        let base_uri_a = format!("file://{}", base_a_dir.std_path().display());

        let path_b = base_b_dir.std_path().join("data-b.bin");
        std::fs::write(&path_b, b"from-base-b").unwrap();
        let uri_b = format!("file://{}", path_b.display());
        let base_uri_b = format!("file://{}", base_b_dir.std_path().display());

        let mut blob_builder = BlobArrayBuilder::new(4);
        blob_builder.push_uri(uri_a.clone()).unwrap();
        blob_builder.push_uri(uri_b).unwrap();
        blob_builder.push_bytes(b"inline-data").unwrap();
        blob_builder.push_uri(uri_a).unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base_a".to_string()),
                        path: base_uri_a,
                        is_dataset_root: false,
                    },
                    BasePath {
                        id: 2,
                        name: Some("base_b".to_string()),
                        path: base_uri_b,
                        is_dataset_root: false,
                    },
                ]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"from-base-a".to_vec())),
                (1, Some(b"from-base-b".to_vec())),
                (2, Some(b"inline-data".to_vec())),
                (3, Some(b"from-base-a".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_large_blobs() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let large_blob_a: Vec<u8> = (0..512 * 1024).map(|i| (i % 256) as u8).collect();
        let large_blob_b: Vec<u8> = (0..256 * 1024).map(|i| ((i + 42) % 256) as u8).collect();

        let mut blob_builder = BlobArrayBuilder::new(3);
        blob_builder.push_bytes(&large_blob_a).unwrap();
        blob_builder.push_bytes(&large_blob_b).unwrap();
        blob_builder.push_bytes(b"small-blob").unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(large_blob_a)),
                (1, Some(large_blob_b)),
                (2, Some(b"small-blob".to_vec()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_blob_kind_reclassification() {
        use crate::BlobArrayBuilder;
        use lance_arrow::BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let medium_data: Vec<u8> = (0..32 * 1024).map(|i| (i % 256) as u8).collect();

        let mut blob_builder = BlobArrayBuilder::new(2);
        blob_builder.push_bytes(&medium_data).unwrap();
        blob_builder.push_bytes(&medium_data).unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));
        let mut blob_field = crate::blob_field("blob", true);
        {
            let mut new_metadata = blob_field.metadata().clone();
            new_metadata.insert(
                BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY.to_string(),
                (16 * 1024).to_string(),
            );
            blob_field = blob_field.with_metadata(new_metadata);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            blob_field,
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 2);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(medium_data.clone())),
                (1, Some(medium_data.clone()))
            ]
        );
    }

    #[tokio::test]
    async fn test_compact_blob_v2_multi_batch() {
        use crate::BlobArrayBuilder;
        use lance_core::utils::tempfile::TempDir;

        let test_dir = TempDir::default();

        let mut blob_builder = BlobArrayBuilder::new(6);
        blob_builder.push_bytes(b"batch-0-row-0").unwrap();
        blob_builder.push_bytes(b"batch-0-row-1").unwrap();
        blob_builder.push_bytes(b"batch-1-row-0").unwrap();
        blob_builder.push_null().unwrap();
        blob_builder.push_bytes(b"batch-1-row-2").unwrap();
        blob_builder.push_bytes(b"batch-1-row-3").unwrap();
        let blob_array: ArrayRef = blob_builder.finish().unwrap();

        let id_array: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            crate::blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, blob_array]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());

        let mut dataset = Dataset::write(
            reader,
            &test_dir.path_str(),
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_2),
                max_rows_per_file: 2,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3);

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 1024 * 1024,
                batch_size: Some(2),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(dataset.get_fragments().len(), 1);

        let mut blob_values = read_blob_bytes_by_index(&Arc::new(dataset.clone()), "blob").await;
        blob_values.sort_by_key(|(id, _)| *id);
        assert_eq!(
            blob_values,
            vec![
                (0, Some(b"batch-0-row-0".to_vec())),
                (1, Some(b"batch-0-row-1".to_vec())),
                (2, Some(b"batch-1-row-0".to_vec())),
                (3, None),
                (4, Some(b"batch-1-row-2".to_vec())),
                (5, Some(b"batch-1-row-3".to_vec()))
            ]
        );
    }
    // ---- `max_overlays_per_fragment` compaction trigger ----
    //
    // Tests for the trigger that fully compacts a fragment carrying too many data
    // overlay files into a fresh fragment with the overlays (and deletions)
    // materialized into the base data.
    use arrow_array::record_batch;
    use lance_file::writer::FileWriterOptions;
    use lance_io::utils::CachedFileSize;
    use lance_table::format::DataFile;
    use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
    use std::collections::BTreeMap;

    use crate::dataset::DATA_DIR;
    use crate::dataset::transaction::DataOverlayGroup;

    /// Two-fragment Int32 dataset: `id` (field 0) = 0..12 and `val` (field 1) =
    /// id * 10, six rows per fragment (fragments 0 and 1).
    async fn create_base_dataset(uri: &str) -> Dataset {
        let batch = record_batch!(
            ("id", Int32, (0..12).collect::<Vec<_>>()),
            ("val", Int32, (0..12).map(|v| v * 10).collect::<Vec<_>>())
        )
        .unwrap();
        let schema = batch.schema();
        let write_params = WriteParams {
            max_rows_per_file: 6,
            max_rows_per_group: 6,
            data_storage_version: Some(LanceFileVersion::Stable),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        Dataset::write(reader, uri, Some(write_params))
            .await
            .unwrap()
    }

    fn i32_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from_iter(values))
    }

    fn bitmap(offsets: impl IntoIterator<Item = u32>) -> RoaringBitmap {
        RoaringBitmap::from_iter(offsets)
    }

    /// Write a dense overlay covering `fields` of `fragment_id` with `columns`
    /// as the per-field value columns, then commit it as a `DataOverlay`.
    async fn commit_overlay(
        dataset: Dataset,
        fragment_id: u64,
        fields: &[i32],
        coverage: OverlayCoverage,
        columns: Vec<ArrayRef>,
    ) -> Dataset {
        let read_version = dataset.version().version;
        let overlay_schema = dataset.schema().project_by_ids(fields, true);
        let filename = format!("{}.lance", Uuid::new_v4());
        let path = dataset.base.clone().join(DATA_DIR).join(filename.as_str());
        let obj_writer = dataset.object_store.create(&path).await.unwrap();
        let file_version = lance_file::version::ConcreteFileVersion::from(LanceFileVersion::Stable);
        let mut writer = lance_file::versions::create_writer(
            file_version,
            obj_writer,
            overlay_schema,
            FileWriterOptions::default(),
        )
        .unwrap();
        for (column_index, array) in columns.into_iter().enumerate() {
            writer.write_column(column_index, array).await.unwrap();
        }
        let summary = writer.finish().await.unwrap();

        let mut data_file = DataFile::new_unstarted(filename, file_version);
        data_file.fields = writer
            .field_id_to_column_indices()
            .iter()
            .map(|(f, _)| *f as i32)
            .collect::<Vec<_>>()
            .into();
        data_file.column_indices = writer
            .field_id_to_column_indices()
            .iter()
            .map(|(_, c)| *c as i32)
            .collect::<Vec<_>>()
            .into();
        data_file.file_size_bytes = CachedFileSize::new(summary.size_bytes);

        Dataset::commit(
            WriteDestination::Dataset(Arc::new(dataset)),
            Operation::DataOverlay {
                groups: vec![DataOverlayGroup {
                    fragment_id,
                    overlays: vec![DataOverlayFile {
                        data_file,
                        coverage,
                        committed_version: 0,
                    }],
                }],
            },
            Some(read_version),
            None,
            None,
            Arc::new(Default::default()),
            false,
        )
        .await
        .unwrap()
    }

    /// Commit `n` distinct single-cell overlays to fragment 0 (offset `i`, val
    /// column set to `1000 + i`), so the fragment ends up with `n` overlays. The
    /// `1000 +` offset keeps overlaid values clear of the base `id * 10` values.
    async fn commit_n_overlays(mut dataset: Dataset, n: u32) -> Dataset {
        for i in 0..n {
            dataset = commit_overlay(
                dataset,
                0,
                &[1],
                OverlayCoverage::dense(bitmap([i])),
                vec![i32_array([Some(1000 + i as i32)])],
            )
            .await;
        }
        dataset
    }

    /// Options whose only compaction trigger is the overlay limit: base
    /// fragments here are far below the default 1M-row target, which would
    /// otherwise make them size-based compaction candidates on their own.
    fn overlay_only_options(max_overlays_per_fragment: usize) -> CompactionOptions {
        CompactionOptions {
            max_overlays_per_fragment: Some(max_overlays_per_fragment),
            target_rows_per_fragment: 6,
            ..Default::default()
        }
    }

    /// Scan `id` and `val` and return an `id -> val` map (order-independent).
    async fn id_val_map(dataset: &Dataset) -> BTreeMap<i32, Option<i32>> {
        let mut scanner = dataset.scan();
        scanner.project(&["id", "val"]).unwrap();
        let batch = scanner.try_into_batch().await.unwrap();
        let mut out = BTreeMap::new();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                None
            } else {
                Some(vals.value(i))
            };
            out.insert(ids.value(i), v);
        }
        out
    }

    #[tokio::test]
    async fn test_max_overlays_triggers_full_compaction() {
        // Fragment 0 gets 3 overlays; fragment 1 stays clean.
        let dataset = create_base_dataset("memory://").await;
        let mut dataset = commit_n_overlays(dataset, 3).await;
        assert_eq!(
            dataset.get_fragment(0).unwrap().metadata().overlays.len(),
            3
        );

        // Threshold 2: only fragment 0 (3 > 2) is compacted.
        let metrics = compact_files(&mut dataset, overlay_only_options(2), None)
            .await
            .unwrap();
        assert_eq!(metrics.fragments_removed, 1);
        assert_eq!(metrics.fragments_added, 1);

        let fragments = dataset.get_fragments();
        assert_eq!(fragments.len(), 2);
        // The compacted fragment is a fresh single-data-file fragment with no
        // overlays; fragment 1 is untouched.
        let compacted = fragments
            .iter()
            .find(|f| f.id() != 1)
            .expect("a new fragment id was assigned");
        assert!(compacted.metadata().overlays.is_empty());
        assert_eq!(compacted.metadata().files.len(), 1);

        // The overlaid values were materialized: id i in 0..3 -> 1000 + i.
        let values = id_val_map(&dataset).await;
        let expected: BTreeMap<i32, Option<i32>> = (0..12)
            .map(|id| {
                let v = if id < 3 { 1000 + id } else { id * 10 };
                (id, Some(v))
            })
            .collect();
        assert_eq!(values, expected);
    }

    #[tokio::test]
    async fn test_below_threshold_is_a_noop() {
        let dataset = create_base_dataset("memory://").await;
        let mut dataset = commit_n_overlays(dataset, 2).await;

        // 2 overlays, threshold 2: `overlays > max` is false, so no compaction.
        let metrics = compact_files(&mut dataset, overlay_only_options(2), None)
            .await
            .unwrap();
        assert_eq!(metrics.fragments_removed, 0);
        assert_eq!(metrics.fragments_added, 0);
        assert_eq!(
            dataset.get_fragment(0).unwrap().metadata().overlays.len(),
            2
        );
    }

    #[tokio::test]
    async fn test_overlay_compaction_materializes_deletions() {
        let dataset = create_base_dataset("memory://").await;
        let mut dataset = commit_n_overlays(dataset, 3).await;
        // Delete a row from the overlaid fragment (id 2 is at offset 2).
        dataset.delete("id = 2").await.unwrap();
        assert!(
            dataset
                .get_fragment(0)
                .unwrap()
                .metadata()
                .deletion_file
                .is_some()
        );

        compact_files(&mut dataset, overlay_only_options(2), None)
            .await
            .unwrap();

        // The deletion was materialized: no deletion file remains and id 2 is gone.
        for fragment in dataset.get_fragments() {
            assert!(fragment.metadata().deletion_file.is_none());
            assert!(fragment.metadata().overlays.is_empty());
        }
        let values = id_val_map(&dataset).await;
        assert!(!values.contains_key(&2));
        // Surviving overlaid cells still carry their materialized values.
        assert_eq!(values.get(&0), Some(&Some(1000)));
        assert_eq!(values.get(&1), Some(&Some(1001)));
    }

    #[tokio::test]
    async fn test_overlay_compaction_reconciles_stale_index() {
        let mut dataset = create_base_dataset("memory://").await;
        // Index `val` before any overlay -> the index is stale once val is overlaid.
        dataset
            .create_index(
                &["val"],
                IndexType::Scalar,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Overlay val[0] 0 -> 100 (committed after the index) and push fragment 0
        // over the overlay limit.
        let mut dataset = commit_n_overlays(dataset, 3).await;

        let val_index_before = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|i| i.fields == vec![1])
            .expect("val index present")
            .clone();
        assert!(
            val_index_before
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .contains(0)
        );

        compact_files(&mut dataset, overlay_only_options(2), None)
            .await
            .unwrap();

        // The stale val index no longer covers the compacted fragment, so its
        // rows fall back to a flat scan instead of serving stale values.
        let indices = dataset.load_indices().await.unwrap();
        let val_index = indices
            .iter()
            .find(|i| i.fields == vec![1])
            .expect("val index present");
        let compacted_id = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .find(|id| *id != 1)
            .unwrap();
        assert!(
            !val_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .contains(compacted_id),
            "stale index must drop the compacted fragment from its coverage"
        );

        // The indexed query is correct: the materialized value is found and the
        // stale pre-overlay value is gone.
        let mut scanner = dataset.scan();
        scanner
            .filter("val = 1000")
            .unwrap()
            .project(&["id"])
            .unwrap();
        let batch = scanner.try_into_batch().await.unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.value(0), 0);

        let mut scanner = dataset.scan();
        scanner.filter("val = 0").unwrap().project(&["id"]).unwrap();
        let batch = scanner.try_into_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 0, "stale value 0 must no longer match");
    }
}
