// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::RecordBatch;
use bytes::Bytes;
use chrono::TimeDelta;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use lance_arrow::{
    ARROW_EXT_NAME_KEY, BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY,
    BLOB_INLINE_SIZE_THRESHOLD_META_KEY, BLOB_META_KEY, BLOB_PACK_FILE_SIZE_THRESHOLD_META_KEY,
    BLOB_V2_EXT_NAME,
};
use lance_core::datatypes::{NullabilityComparison, OnMissing, OnTypeMismatch};
use lance_core::utils::tracing::{
    AUDIT_MODE_CREATE, AUDIT_MODE_DELETE, AUDIT_TYPE_DATA, TRACE_FILE_AUDIT,
};
use lance_core::{Error, Result, datatypes::Schema};
use lance_datafusion::utils::StreamingWriteSource;
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_file::versions::v1::writer::{
    FileWriter as V1FileWriter, ManifestProvider as V1ManifestProvider,
};
use lance_file::writer::{self as current_writer};
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, parse_base_scoped_key,
};
use lance_io::traits::Writer;
use lance_table::format::{BasePath, DataFile, Fragment, IndexMetadata};
use lance_table::io::commit::{CommitHandler, commit_handler_from_url};
use lance_table::io::manifest::ManifestDescribing;
use object_store::path::Path;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tracing::{info, instrument};

use crate::Dataset;
use crate::blob::prepared_to_logical_blob_schema;
use crate::dataset::blob::{
    BlobPreprocessor, ExternalBaseCandidate, ExternalBaseResolver,
    blob_dedicated_threshold_from_metadata, blob_inline_threshold_from_metadata,
    blob_pack_file_threshold_from_metadata, preprocess_blob_batches,
};
use crate::index::DatasetIndexExt;
use crate::index::scalar::{IndexDetails, fetch_index_details};
use crate::session::Session;

use super::DATA_DIR;
use super::fragment::write::generate_random_filename;
use super::progress::{NoopFragmentWriteProgress, WriteFragmentProgress};
use super::transaction::Transaction;
use super::utils::SchemaAdapter;
use super::versions;

mod commit;
pub mod delete;
mod insert;
pub mod merge_insert;
mod retry;
pub mod update;

pub use super::progress::{WriteProgressFn, WriteStats};
pub use commit::{CommitBuilder, DEFAULT_COMMIT_TIMEOUT};
pub use delete::{DeleteBuilder, DeleteResult, UncommittedDelete};
pub use insert::InsertBuilder;

/// The destination to write data to.
#[derive(Debug, Clone)]
pub enum WriteDestination<'a> {
    /// An existing dataset to write to.
    Dataset(Arc<Dataset>),
    /// A URI to write to.
    Uri(&'a str),
}

impl WriteDestination<'_> {
    pub fn dataset(&self) -> Option<&Dataset> {
        match self {
            WriteDestination::Dataset(dataset) => Some(dataset.as_ref()),
            WriteDestination::Uri(_) => None,
        }
    }

    pub fn uri(&self) -> String {
        match self {
            WriteDestination::Dataset(dataset) => dataset.uri.clone(),
            WriteDestination::Uri(uri) => uri.to_string(),
        }
    }
}

impl From<Arc<Dataset>> for WriteDestination<'_> {
    fn from(dataset: Arc<Dataset>) -> Self {
        WriteDestination::Dataset(dataset)
    }
}

impl<'a> From<&'a str> for WriteDestination<'a> {
    fn from(uri: &'a str) -> Self {
        WriteDestination::Uri(uri)
    }
}

impl<'a> From<&'a String> for WriteDestination<'a> {
    fn from(uri: &'a String) -> Self {
        WriteDestination::Uri(uri.as_str())
    }
}

impl<'a> From<&'a Path> for WriteDestination<'a> {
    fn from(path: &'a Path) -> Self {
        WriteDestination::Uri(path.as_ref())
    }
}

/// The mode to write dataset.
#[derive(Debug, Clone, Copy)]
pub enum WriteMode {
    /// Create a new dataset. Expect the dataset does not exist.
    Create,
    /// Append to an existing dataset.
    Append,
    /// Overwrite a dataset as a new version, or create new dataset if not exist.
    Overwrite,
}

impl TryFrom<&str> for WriteMode {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "create" => Ok(Self::Create),
            "append" => Ok(Self::Append),
            "overwrite" => Ok(Self::Overwrite),
            _ => Err(Error::invalid_input(format!(
                "Invalid write mode: {}",
                value
            ))),
        }
    }
}

/// The strategy for handling external blob URIs on write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExternalBlobMode {
    /// Store the URI as an external blob reference.
    #[default]
    Reference,
    /// Read the external bytes during write and store them in Lance-managed storage.
    Ingest,
}

impl TryFrom<&str> for ExternalBlobMode {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "reference" => Ok(Self::Reference),
            "ingest" => Ok(Self::Ingest),
            _ => Err(Error::invalid_input(format!(
                "Invalid external blob mode: {}",
                value
            ))),
        }
    }
}

fn validate_external_blob_write_params(params: &WriteParams) -> Result<()> {
    if params.external_blob_mode == ExternalBlobMode::Ingest
        && params.allow_external_blob_outside_bases
    {
        return Err(Error::invalid_input(
            "allow_external_blob_outside_bases only applies when external_blob_mode=\"reference\"",
        ));
    }

    Ok(())
}

fn validate_blob_threshold_metadata_for_append(
    input_schema: &Schema,
    dataset_schema: &Schema,
) -> Result<()> {
    for input_field in &input_schema.fields {
        let Some(dataset_field) = dataset_schema.field(&input_field.name) else {
            continue;
        };
        validate_blob_threshold_metadata_for_field_recursive(input_field, dataset_field)?;
    }

    Ok(())
}

fn validate_blob_threshold_metadata_for_field_recursive(
    input_field: &lance_core::datatypes::Field,
    dataset_field: &lance_core::datatypes::Field,
) -> Result<()> {
    let input_is_blob_v2 = input_field
        .metadata
        .get(ARROW_EXT_NAME_KEY)
        .is_some_and(|extension_name| extension_name == BLOB_V2_EXT_NAME);
    let dataset_is_blob_v2 = dataset_field
        .metadata
        .get(ARROW_EXT_NAME_KEY)
        .is_some_and(|extension_name| extension_name == BLOB_V2_EXT_NAME);
    if input_is_blob_v2 || dataset_is_blob_v2 {
        for (key, read_threshold) in [
            (
                BLOB_INLINE_SIZE_THRESHOLD_META_KEY,
                blob_inline_threshold_from_metadata
                    as fn(&HashMap<String, String>, &str) -> Result<usize>,
            ),
            (
                BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY,
                blob_dedicated_threshold_from_metadata,
            ),
            (
                BLOB_PACK_FILE_SIZE_THRESHOLD_META_KEY,
                blob_pack_file_threshold_from_metadata,
            ),
        ] {
            if !input_field.metadata.contains_key(key) {
                continue;
            }
            let input_value = read_threshold(&input_field.metadata, &input_field.name)?;
            let dataset_value = read_threshold(&dataset_field.metadata, &dataset_field.name)?;
            if input_value != dataset_value {
                return Err(Error::invalid_input(format!(
                    "Cannot append data with blob threshold metadata {key}={input_value} for \
                     field '{}'; the dataset schema has effective value {dataset_value}. Blob \
                     thresholds for existing columns are stored in the dataset schema.",
                    input_field.name,
                )));
            }
        }
    }

    for input_child in &input_field.children {
        let Some(dataset_child) = dataset_field.child(&input_child.name) else {
            continue;
        };
        validate_blob_threshold_metadata_for_field_recursive(input_child, dataset_child)?;
    }

    Ok(())
}

/// Auto cleanup parameters
#[derive(Debug, Clone)]
pub struct AutoCleanupParams {
    pub interval: usize,
    pub older_than: TimeDelta,
}

impl Default for AutoCleanupParams {
    fn default() -> Self {
        Self {
            interval: 20,
            older_than: TimeDelta::days(14),
        }
    }
}

/// Dataset Write Parameters
#[derive(Debug, Clone)]
pub struct WriteParams {
    /// Max number of records per file.
    pub max_rows_per_file: usize,

    /// Max number of rows per row group.
    pub max_rows_per_group: usize,

    /// Max file size in bytes.
    ///
    /// This is a soft limit. The actual file size may be larger than this value
    /// by a few megabytes, since once we detect we hit this limit, we still
    /// need to flush the footer.
    ///
    /// This limit is checked after writing each group, so if max_rows_per_group
    /// is set to a large value, this limit may be exceeded by a large amount.
    ///
    /// The default is 90 GB. If you are using an object store such as S3, we
    /// currently have a hard 100 GB limit.
    pub max_bytes_per_file: usize,

    /// Write mode
    pub mode: WriteMode,

    /// Default object store params for the write.
    ///
    /// Storage options may carry base-scoped entries (`base_<id>.<key>`) that
    /// apply only to the registered base path with that id, overriding the
    /// unscoped options that every base inherits.
    pub store_params: Option<ObjectStoreParams>,

    /// Exact object store params per base path URI, taking precedence over
    /// `base_<id>.<key>` storage options in [`Self::store_params`]. See
    /// [`Self::with_base_store_params`].
    pub base_store_params: Option<HashMap<String, ObjectStoreParams>>,

    pub progress: Arc<dyn WriteFragmentProgress>,

    /// Optional callback invoked after each batch is written.
    ///
    /// Receives cumulative [`WriteStats`] so callers can render a progress bar
    /// or compute throughput. The callback must be cheap and non-blocking;
    /// spawn a task if you need async work.
    pub write_progress: Option<WriteProgressFn>,

    /// If present, dataset will use this to update the latest version
    ///
    /// If not set, the default will be based on the object store.  Generally this will
    /// be RenameCommitHandler unless the object store does not handle atomic renames (e.g. S3)
    ///
    /// If a custom object store is provided (via store_params.object_store) then this
    /// must also be provided.
    pub commit_handler: Option<Arc<dyn CommitHandler>>,

    /// The format version to use when writing data.
    ///
    /// Newer versions are more efficient but the data can only be read by more recent versions
    /// of lance.
    /// Lance file version 2.3 enables RLE v2 run length widths by default.
    ///
    /// If not specified then the latest stable version will be used.
    pub data_storage_version: Option<LanceFileVersion>,

    /// Experimental: if set to true, the writer will use stable row ids.
    /// These row ids are stable after compaction operations, but not after updates.
    /// This makes compaction more efficient, since with stable row ids no
    /// secondary indices need to be updated to point to new row ids.
    pub enable_stable_row_ids: bool,

    /// If set to true, and this is a new dataset, uses the new v2 manifest paths.
    /// These allow constant-time lookups for the latest manifest on object storage.
    /// This parameter has no effect on existing datasets. To migrate an existing
    /// dataset, use the [`super::Dataset::migrate_manifest_paths_v2`] method.
    /// Default is True.
    pub enable_v2_manifest_paths: bool,

    pub session: Option<Arc<Session>>,

    /// If Some and this is a new dataset, old dataset versions will be
    /// automatically cleaned up after commits according to the parameters set
    /// out in [`AutoCleanupParams`]. This parameter has no effect on existing
    /// datasets. To add auto-cleanup to an existing dataset, use
    /// [`Dataset::update_config`] to set `lance.auto_cleanup.interval` and
    /// `lance.auto_cleanup.older_than`. Both parameters must be set to invoke
    /// auto-cleanup.
    ///
    /// Defaults to `None` (auto-cleanup disabled). Enabling it makes every
    /// `interval`-th commit run a full cleanup pass, which lists and reads every
    /// manifest in the dataset even when nothing is old enough to delete; on
    /// object stores this adds noticeable per-commit latency that grows with the
    /// version count. Prefer calling [`Dataset::cleanup_old_versions`] explicitly
    /// when you actually need to reclaim space.
    pub auto_cleanup: Option<AutoCleanupParams>,

    /// If true, skip auto cleanup during commits. This should be set to true
    /// for high frequency writes to improve performance. This is also useful
    /// if the writer does not have delete permissions and the clean up would
    /// just try and log a failure anyway. Default is false.
    pub skip_auto_cleanup: bool,

    /// Configuration key-value pairs for this write operation.
    /// This can include commit messages, engine information, etc.
    /// this properties map will be persisted as part of Transaction object.
    pub transaction_properties: Option<Arc<HashMap<String, String>>>,

    /// New base paths to register in the manifest during dataset creation.
    /// Each BasePath must have a properly assigned ID (non-zero).
    /// Only used in CREATE/OVERWRITE modes for manifest registration.
    /// IDs should be assigned by the caller before passing to WriteParams.
    pub initial_bases: Option<Vec<BasePath>>,

    /// Target base IDs for writing data files.
    /// When provided, all new data files will be written to bases with these IDs.
    /// Used in all modes (CREATE, APPEND, OVERWRITE) to specify where data should be written.
    /// The IDs must correspond to either:
    /// - IDs in initial_bases (for CREATE/OVERWRITE modes)
    /// - IDs already registered in the existing dataset manifest (for APPEND mode)
    /// - [`PRIMARY_BASE_ID`] (0), which targets the dataset's primary storage
    ///   and participates in the round-robin like any other entry
    pub target_bases: Option<Vec<u32>>,

    /// Target base names or paths as strings (unresolved).
    /// These will be resolved to IDs when the write operation executes.
    /// Resolution happens at builder execution time when dataset context is available.
    /// An entry equal to the dataset's URI targets the dataset's primary storage.
    pub target_base_names_or_paths: Option<Vec<String>>,

    /// Target every base registered in the dataset manifest, resolved when the
    /// write executes. `Some(include_primary)`: when `include_primary` is true
    /// the dataset's primary storage participates in the rotation as the first
    /// slot. Cannot be combined with `target_bases` or
    /// `target_base_names_or_paths`.
    pub target_all_bases: Option<bool>,

    /// Allow writing external blob URIs that cannot be mapped to any registered
    /// non-dataset-root base path. When disabled, such rows are rejected.
    pub allow_external_blob_outside_bases: bool,

    /// The strategy used when writing external blob URIs.
    pub external_blob_mode: ExternalBlobMode,

    /// Maximum size in bytes for blob v2 pack (.blob) sidecar files.
    /// When a pack file reaches this size, a new one is started.
    /// If not set, defaults to 1 GiB.
    pub blob_pack_file_size_threshold: Option<usize>,
}

impl Default for WriteParams {
    fn default() -> Self {
        Self {
            max_rows_per_file: 1024 * 1024, // 1 million
            max_rows_per_group: 1024,
            // object-store has a 100GB limit, so we should at least make sure
            // we are under that.
            max_bytes_per_file: 90 * 1024 * 1024 * 1024, // 90 GB
            mode: WriteMode::Create,
            store_params: None,
            base_store_params: None,
            progress: Arc::new(NoopFragmentWriteProgress::new()),
            write_progress: None,
            commit_handler: None,
            data_storage_version: None,
            enable_stable_row_ids: false,
            enable_v2_manifest_paths: true,
            session: None,
            auto_cleanup: None,
            skip_auto_cleanup: false,
            transaction_properties: None,
            initial_bases: None,
            target_bases: None,
            target_base_names_or_paths: None,
            target_all_bases: None,
            allow_external_blob_outside_bases: false,
            external_blob_mode: ExternalBlobMode::Reference,
            blob_pack_file_size_threshold: None,
        }
    }
}

impl WriteParams {
    /// Create a new WriteParams with the given storage version.
    /// The other fields are set to their default values.
    pub fn with_storage_version(version: LanceFileVersion) -> Self {
        Self {
            data_storage_version: Some(version),
            ..Default::default()
        }
    }

    pub fn storage_version_or_default(&self) -> ConcreteFileVersion {
        self.data_storage_version.unwrap_or_default().into()
    }

    pub fn store_registry(&self) -> Arc<ObjectStoreRegistry> {
        self.session
            .as_ref()
            .map(|s| s.store_registry())
            .unwrap_or_default()
    }

    /// Set exact runtime object store params for a registered base path.
    ///
    /// These params are used as-is for that base, taking precedence over
    /// `base_<id>.<key>` storage options in `store_params`. The write-level
    /// default `store_params` remain the fallback for bases without an
    /// explicit binding.
    pub fn with_base_store_params(
        mut self,
        base_path: impl AsRef<str>,
        store_params: ObjectStoreParams,
    ) -> Self {
        self.base_store_params
            .get_or_insert_with(HashMap::new)
            .insert(base_path.as_ref().to_string(), store_params);
        self
    }

    /// Set the properties for this WriteParams.
    pub fn with_transaction_properties(self, properties: HashMap<String, String>) -> Self {
        Self {
            transaction_properties: Some(Arc::new(properties)),
            ..self
        }
    }

    /// Set the initial_bases for this WriteParams.
    ///
    /// This specifies new base paths to register in the manifest during dataset creation.
    /// Each BasePath must have a properly assigned ID (non-zero) before calling this method.
    /// Only used in CREATE/OVERWRITE modes for manifest registration.
    pub fn with_initial_bases(self, bases: Vec<BasePath>) -> Self {
        Self {
            initial_bases: Some(bases),
            ..self
        }
    }

    /// Set the target_bases for this WriteParams.
    ///
    /// This specifies the base IDs where data files should be written.
    /// The IDs must correspond to either:
    /// - IDs in initial_bases (for CREATE/OVERWRITE modes)
    /// - IDs already registered in the existing dataset manifest (for APPEND mode)
    pub fn with_target_bases(self, base_ids: Vec<u32>) -> Self {
        Self {
            target_bases: Some(base_ids),
            ..self
        }
    }

    /// Store target base names or paths for deferred resolution.
    ///
    /// This method stores the references in `target_base_names_or_paths` field
    /// to be resolved later at execution time when the dataset manifest is available.
    ///
    /// Resolution will happen at write execution time and will try to match:
    /// 1. initial_bases by name
    /// 2. initial_bases by path
    /// 3. existing manifest by name
    /// 4. existing manifest by path
    ///
    /// # Arguments
    ///
    /// * `references` - Vector of base names or paths to be resolved later
    pub fn with_target_base_names_or_paths(self, references: Vec<String>) -> Self {
        Self {
            target_base_names_or_paths: Some(references),
            ..self
        }
    }

    /// Target every base registered in the dataset manifest, resolved when the
    /// write executes. When `include_primary` is true the dataset's primary
    /// storage participates in the rotation as the first slot.
    ///
    /// Cannot be combined with [`Self::with_target_bases`] or
    /// [`Self::with_target_base_names_or_paths`].
    pub fn with_target_all_bases(self, include_primary: bool) -> Self {
        Self {
            target_all_bases: Some(include_primary),
            ..self
        }
    }

    /// Configure whether external blobs outside registered bases are allowed.
    pub fn with_allow_external_blob_outside_bases(self, allow: bool) -> Self {
        Self {
            allow_external_blob_outside_bases: allow,
            ..self
        }
    }

    /// Configure how external blob URIs are handled during writes.
    pub fn with_external_blob_mode(self, mode: ExternalBlobMode) -> Self {
        Self {
            external_blob_mode: mode,
            ..self
        }
    }

    /// Set the maximum size in bytes for blob v2 pack (.blob) sidecar files.
    pub fn with_blob_pack_file_size_threshold(self, max_bytes: usize) -> Self {
        Self {
            blob_pack_file_size_threshold: Some(max_bytes),
            ..self
        }
    }
}

/// Writes the given data to the dataset and returns fragments.
///
/// NOTE: the fragments have not yet been assigned an ID. That must be done
/// by the caller. This is so this function can be called in parallel, and the
/// IDs can be assigned after writing is complete.
#[deprecated(
    since = "0.20.0",
    note = "Use [`InsertBuilder::execute_uncommitted_stream`] instead"
)]
pub async fn write_fragments(
    dest: impl Into<WriteDestination<'_>>,
    data: impl StreamingWriteSource,
    params: WriteParams,
) -> Result<Transaction> {
    InsertBuilder::new(dest.into())
        .with_params(&params)
        .execute_uncommitted_stream(data)
        .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn do_write_fragments_impl<OpenWriter, OpenWriterFuture>(
    dataset: Option<&Dataset>,
    object_store: Arc<ObjectStore>,
    base_dir: &Path,
    schema: &Schema,
    mut buffered_reader: futures::stream::BoxStream<'static, Result<Vec<RecordBatch>>>,
    params: WriteParams,
    open_writer: OpenWriter,
    external_base_resolver: Option<Arc<ExternalBaseResolver>>,
    target_bases_info: Option<Vec<TargetBaseInfo>>,
    mut seed_writers: Vec<Box<dyn lance_index::scalar::seed::IndexSeedWriter>>,
) -> Result<Vec<Fragment>>
where
    OpenWriter: Fn(Arc<ObjectStore>, Schema, Path, WriterOptions) -> OpenWriterFuture + Send + Sync,
    OpenWriterFuture: Future<Output = Result<Box<dyn GenericWriter>>> + Send,
{
    let source_store_registry = dataset
        .map(|ds| ds.session.store_registry())
        .unwrap_or_else(|| params.store_registry());
    let source_store_params = params.store_params.clone().unwrap_or_default();

    // Keep a copy so failure paths can clean up files written to target bases.
    let cleanup_bases = target_bases_info.clone();
    let writer_generator = WriterGenerator::new(
        object_store.clone(),
        base_dir,
        schema,
        open_writer,
        target_bases_info,
        external_base_resolver,
        params.allow_external_blob_outside_bases,
        params.external_blob_mode,
        source_store_registry,
        source_store_params,
        params.blob_pack_file_size_threshold,
    );
    let mut writer: Option<Box<dyn GenericWriter>> = None;
    let mut num_rows_in_current_file = 0;
    let mut fragments: Vec<Fragment> = Vec::new();
    let mut bytes_completed: u64 = 0;
    let mut rows_completed: u64 = 0;
    let mut files_written: u32 = 0;

    // Wrap the loop in an async block so `?` returns into `loop_result` and we
    // can run cleanup before propagating the error.
    let loop_result: Result<()> = async {
        while let Some(batch_chunk) = buffered_reader.next().await {
            let batch_chunk = batch_chunk?;

            if writer.is_none() {
                let (new_writer, new_fragment) = writer_generator.new_writer().await?;
                params.progress.begin(&new_fragment).await?;
                writer = Some(new_writer);
                fragments.push(new_fragment);
            }

            writer.as_mut().unwrap().write(&batch_chunk).await?;
            for seed_writer in seed_writers.iter_mut() {
                let col_name = seed_writer.column_name().to_owned();
                for batch in &batch_chunk {
                    if let Some(col) = batch.column_by_name(&col_name) {
                        seed_writer.observe_batch(col)?;
                    }
                }
            }
            for batch in &batch_chunk {
                num_rows_in_current_file += batch.num_rows() as u32;
            }

            if let Some(cb) = &params.write_progress {
                let current_bytes = writer.as_mut().unwrap().tell().await?;
                cb.call(WriteStats {
                    bytes_written: bytes_completed + current_bytes,
                    rows_written: rows_completed + num_rows_in_current_file as u64,
                    files_written,
                });
            }

            if num_rows_in_current_file >= params.max_rows_per_file as u32
                || writer.as_mut().unwrap().tell().await? >= params.max_bytes_per_file as u64
            {
                let mut w = writer.take().unwrap();
                flush_seed_writers(w.as_mut(), &mut seed_writers).await?;
                let (num_rows, data_file) = w.finish().await?;
                info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_DATA, path = &data_file.path);
                debug_assert_eq!(num_rows, num_rows_in_current_file);
                bytes_completed += data_file.file_size_bytes.get().map_or(0, |s| s.get());
                rows_completed += num_rows as u64;
                files_written += 1;
                let last_fragment = fragments.last_mut().unwrap();
                last_fragment.physical_rows = Some(num_rows as usize);
                last_fragment.files.push(data_file);
                // Notify after pushing the data file so it's tracked for cleanup
                // if the callback fails.
                params.progress.complete(fragments.last().unwrap()).await?;
                if let Some(cb) = &params.write_progress {
                    cb.call(WriteStats {
                        bytes_written: bytes_completed,
                        rows_written: rows_completed,
                        files_written,
                    });
                }
                num_rows_in_current_file = 0;
            }
        }
        Ok(())
    }
    .await;

    if let Err(e) = loop_result {
        // Drop the writer so its in-progress file is cleaned up (LocalWriter
        // removes its temp file; ObjectWriter aborts the multipart upload).
        drop(writer.take());
        cleanup_data_fragments(
            &object_store,
            base_dir,
            cleanup_bases.as_deref(),
            &fragments,
        )
        .await;
        return Err(e);
    }

    // Complete the final writer
    if let Some(mut writer) = writer.take() {
        if let Err(e) = flush_seed_writers(writer.as_mut(), &mut seed_writers).await {
            drop(writer);
            cleanup_data_fragments(
                &object_store,
                base_dir,
                cleanup_bases.as_deref(),
                &fragments,
            )
            .await;
            return Err(e);
        }
        match writer.finish().await {
            Ok((num_rows, data_file)) => {
                info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_DATA, path = &data_file.path);
                bytes_completed += data_file.file_size_bytes.get().map_or(0, |s| s.get());
                rows_completed += num_rows as u64;
                files_written += 1;
                let last_fragment = fragments.last_mut().unwrap();
                last_fragment.physical_rows = Some(num_rows as usize);
                last_fragment.files.push(data_file);
                if let Some(cb) = &params.write_progress {
                    cb.call(WriteStats {
                        bytes_written: bytes_completed,
                        rows_written: rows_completed,
                        files_written,
                    });
                }
            }
            Err(e) => {
                drop(writer);
                cleanup_data_fragments(
                    &object_store,
                    base_dir,
                    cleanup_bases.as_deref(),
                    &fragments,
                )
                .await;
                return Err(e);
            }
        }
    }

    Ok(fragments)
}

/// Flush all seed writers into the given file writer, embedding seed buffers
/// and schema metadata before `finish()` is called.
async fn flush_seed_writers(
    writer: &mut dyn GenericWriter,
    seed_writers: &mut [Box<dyn lance_index::scalar::seed::IndexSeedWriter>],
) -> Result<()> {
    for seed_writer in seed_writers.iter_mut() {
        if let Some(bytes) = seed_writer.finish()? {
            let buf_index = writer.add_global_buffer(bytes).await?;
            let key = seed_writer.schema_metadata_key();
            let value = seed_writer.schema_metadata_value(buf_index);
            writer.add_schema_metadata(key, value);
        }
    }
    Ok(())
}

/// Best-effort cleanup of data files for fragments that were written but not committed.
///
/// Contract:
/// - Errors from individual `delete` calls are logged and swallowed, never returned —
///   callers should propagate the original write error.
/// - Files in the dataset's default storage (`base_id == None`) are deleted via
///   `object_store`; files whose `base_id` matches an entry in `target_bases` are
///   deleted via that base's object store. Files in bases not listed in
///   `target_bases` are skipped because we don't have their object stores here.
/// - Safe to call with an empty slice.
/// - Must be called before the fragments are committed, otherwise live data may be deleted.
pub(crate) async fn cleanup_data_fragments(
    object_store: &ObjectStore,
    base_dir: &Path,
    target_bases: Option<&[TargetBaseInfo]>,
    fragments: &[Fragment],
) {
    let data_dir = base_dir.clone().join(DATA_DIR);
    let mut skipped_external = 0usize;
    for fragment in fragments {
        for file in &fragment.files {
            let (store, file_dir) = if let Some(base_id) = file.base_id {
                match target_bases.and_then(|bases| bases.iter().find(|b| b.base_id == base_id)) {
                    Some(base_info) => {
                        let dir = if base_info.is_dataset_root {
                            base_info.base_dir.clone().join(DATA_DIR)
                        } else {
                            base_info.base_dir.clone()
                        };
                        (base_info.object_store.as_ref(), dir)
                    }
                    None => {
                        skipped_external += 1;
                        continue;
                    }
                }
            } else {
                (object_store, data_dir.clone())
            };

            let path = file_dir.clone().join(file.path.as_str());
            match store.delete(&path).await {
                Ok(()) => {
                    info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_DELETE, r#type=AUDIT_TYPE_DATA, path = file.path.as_str());
                }
                Err(e) => {
                    log::warn!("Failed to clean up orphaned data file '{}': {}", path, e);
                }
            }

            // Clean up any blob v2 sidecars that might exist for this data file.
            // Blob v2 sidecars are written to `data/{data_file_key}/{blob_id}.blob`.
            // The `data_file_key` is the file stem of the .lance file.
            if let Some(stem) = std::path::Path::new(file.path.as_str())
                .file_stem()
                .and_then(|s| s.to_str())
            {
                let blob_dir = file_dir.clone().join(stem);
                match store.remove_dir_all(blob_dir.clone()).await {
                    Err(e) if !matches!(e, Error::NotFound { .. }) => {
                        log::warn!("Failed to clean up orphaned blob dir '{}': {}", blob_dir, e);
                    }
                    _ => {}
                }
            }
        }
    }
    if skipped_external > 0 {
        log::warn!(
            "Skipped cleanup of {} orphaned data file(s) in external bases: \
             their object stores are not available here",
            skipped_external
        );
    }
}

pub async fn validate_and_resolve_target_bases(
    params: &mut WriteParams,
    existing_base_paths: Option<&HashMap<u32, BasePath>>,
) -> Result<Option<Vec<TargetBaseInfo>>> {
    // Step 1: Validations
    if !matches!(params.mode, WriteMode::Create) && params.initial_bases.is_some() {
        return Err(Error::invalid_input(format!(
            "Cannot register new bases in {:?} mode. Only CREATE mode can register new bases.",
            params.mode
        )));
    }

    if params.target_base_names_or_paths.is_some() && params.target_bases.is_some() {
        return Err(Error::invalid_input(
            "Cannot specify both target_base_names_or_paths and target_bases. Use one or the other.",
        ));
    }

    if params.target_all_bases.is_some() {
        return Err(Error::invalid_input(
            "target_all_bases requires dataset context to resolve; use the write or merge insert APIs to apply it.",
        ));
    }

    // Step 2: Assign IDs to initial_bases and add them to all_bases
    let mut all_bases: HashMap<u32, BasePath> = existing_base_paths.cloned().unwrap_or_default();
    if let Some(initial_bases) = &mut params.initial_bases {
        let mut next_id = all_bases.keys().max().map(|&id| id + 1).unwrap_or(1);

        for base_path in initial_bases.iter_mut() {
            if base_path.id == 0 {
                base_path.id = next_id;
                next_id += 1;
            }
            all_bases.insert(base_path.id, base_path.clone());
        }
    }
    log_unregistered_base_scoped_options(
        params.store_params.as_ref(),
        &all_bases,
        log::Level::Warn,
    );

    // Step 3: Resolve target_base_names_or_paths to IDs
    let target_base_ids = if let Some(ref names_or_paths) = params.target_base_names_or_paths {
        let mut resolved_ids = Vec::new();
        for reference in names_or_paths {
            let ref_str = reference.as_str();
            let id = all_bases
                .iter()
                .find(|(_, base)| {
                    base.name.as_ref().map(|n| n == ref_str).unwrap_or(false)
                        || base.path == ref_str
                })
                .map(|(&id, _)| id)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Base reference '{}' not found in available bases",
                        ref_str
                    ))
                })?;

            resolved_ids.push(id);
        }
        Some(resolved_ids)
    } else {
        params.target_bases.clone()
    };

    // Step 4: Prepare TargetBaseInfo structs
    let store_registry = params
        .session
        .as_ref()
        .map(|s| s.store_registry())
        .unwrap_or_default();

    if let Some(target_bases) = &target_base_ids {
        // An empty list would panic in round-robin selection; reject it
        // instead of silently writing to primary storage.
        if target_bases.is_empty() {
            return Err(Error::invalid_input(
                "target_bases cannot be empty. Omit the option to write to primary storage.",
            ));
        }
        let mut bases_info = Vec::new();

        for &target_base_id in target_bases {
            let base_path = all_bases.get(&target_base_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "Target base ID {} not found in available bases",
                    target_base_id
                ))
            })?;

            let store_params = write_store_params_for_base(params, base_path);
            let (target_object_store, extracted_path) = ObjectStore::from_uri_and_params(
                store_registry.clone(),
                &base_path.path,
                &store_params,
            )
            .await?;

            bases_info.push(TargetBaseInfo {
                base_id: target_base_id,
                object_store: target_object_store,
                base_dir: extracted_path,
                is_dataset_root: base_path.is_dataset_root,
            });
        }

        Ok(Some(bases_info))
    } else {
        Ok(None)
    }
}

/// Like [`validate_and_resolve_target_bases`], but also resolves references to
/// the dataset's primary storage: base id [`PRIMARY_BASE_ID`] in
/// `target_bases`, or an entry equal to `primary_uri` in
/// `target_base_names_or_paths`. Primary slots participate in the round-robin
/// like any other target base; files written through them carry no base id.
pub(crate) async fn validate_and_resolve_target_bases_with_primary(
    params: &mut WriteParams,
    existing_base_paths: Option<&HashMap<u32, BasePath>>,
    primary_object_store: &Arc<ObjectStore>,
    primary_base_dir: &Path,
    primary_uri: &str,
) -> Result<Option<Vec<TargetBaseInfo>>> {
    // Expand an all-bases request into an explicit id list (primary first,
    // then registered bases in ascending id order) and continue below.
    if let Some(include_primary) = params.target_all_bases {
        if params.target_bases.is_some() || params.target_base_names_or_paths.is_some() {
            return Err(Error::invalid_input(
                "Cannot specify target_all_bases together with target_bases or target_base_names_or_paths.",
            ));
        }
        let mut ids: Vec<u32> = existing_base_paths
            .map(|bases| bases.keys().copied().collect())
            .unwrap_or_default();
        // CREATE mode registers initial_bases in the same write; assign their
        // ids here (the delegate keeps non-zero ids as-is) so they join the
        // rotation.
        if let Some(initial_bases) = &mut params.initial_bases {
            let mut next_id = ids.iter().max().map(|id| id + 1).unwrap_or(1);
            for base_path in initial_bases.iter_mut() {
                if base_path.id == 0 {
                    base_path.id = next_id;
                    next_id += 1;
                }
                ids.push(base_path.id);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        if include_primary {
            ids.insert(0, PRIMARY_BASE_ID);
        }
        if ids.is_empty() {
            return Err(Error::invalid_input(
                "target_all_bases found no registered bases and include_primary is false. \
                 Register bases or include primary storage.",
            ));
        }
        params.target_bases = Some(ids);
        params.target_all_bases = None;
    }

    let has_primary_ids = params
        .target_bases
        .as_ref()
        .is_some_and(|ids| ids.contains(&PRIMARY_BASE_ID));
    let has_primary_refs = params
        .target_base_names_or_paths
        .as_ref()
        .is_some_and(|refs| refs.iter().any(|r| r == primary_uri));
    if !has_primary_ids && !has_primary_refs {
        return validate_and_resolve_target_bases(params, existing_base_paths).await;
    }

    // The delegate below may be skipped when only primary slots remain, so
    // validate mutual exclusion here as well.
    if params.target_base_names_or_paths.is_some() && params.target_bases.is_some() {
        return Err(Error::invalid_input(
            "Cannot specify both target_base_names_or_paths and target_bases. Use one or the other.",
        ));
    }

    // Strip the primary slots, resolve the remaining references through the
    // normal path, then splice the primary slots back into their original
    // positions so the round-robin order matches what the caller asked for.
    let is_primary_slot: Vec<bool> = if let Some(ids) = &params.target_bases {
        ids.iter().map(|id| *id == PRIMARY_BASE_ID).collect()
    } else {
        params
            .target_base_names_or_paths
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| r == primary_uri)
            .collect()
    };

    let mut shim = params.clone();
    if let Some(ids) = &params.target_bases {
        let rest: Vec<u32> = ids
            .iter()
            .copied()
            .filter(|id| *id != PRIMARY_BASE_ID)
            .collect();
        shim.target_bases = if rest.is_empty() { None } else { Some(rest) };
    } else {
        let rest: Vec<String> = params
            .target_base_names_or_paths
            .as_ref()
            .unwrap()
            .iter()
            .filter(|r| *r != primary_uri)
            .cloned()
            .collect();
        shim.target_base_names_or_paths = if rest.is_empty() { None } else { Some(rest) };
    }

    let resolved_rest = validate_and_resolve_target_bases(&mut shim, existing_base_paths).await?;
    // The delegate assigns ids to initial_bases in place; propagate that side
    // effect back so CREATE-mode transactions register properly assigned ids.
    params.initial_bases = shim.initial_bases;

    let mut rest_iter = resolved_rest.unwrap_or_default().into_iter();
    let mut bases_info = Vec::with_capacity(is_primary_slot.len());
    for is_primary in is_primary_slot {
        if is_primary {
            bases_info.push(TargetBaseInfo {
                base_id: PRIMARY_BASE_ID,
                object_store: primary_object_store.clone(),
                base_dir: primary_base_dir.clone(),
                is_dataset_root: true,
            });
        } else {
            bases_info.push(rest_iter.next().ok_or_else(|| {
                Error::internal("target base resolution returned fewer bases than requested")
            })?);
        }
    }
    Ok(Some(bases_info))
}

fn append_external_base_candidate(
    base_path: &BasePath,
    store_prefix: String,
    extracted_path: Path,
    store_params: ObjectStoreParams,
    candidates: &mut Vec<ExternalBaseCandidate>,
    seen_base_ids: &mut HashSet<u32>,
) {
    if base_path.is_dataset_root {
        return;
    }
    if seen_base_ids.insert(base_path.id) {
        candidates.push(ExternalBaseCandidate {
            base_id: base_path.id,
            store_prefix,
            base_path: extracted_path,
            store_params,
        });
    }
}

/// Log base-scoped storage options (`base_<id>.<key>`) whose id does not
/// match any registered base path. Unregistered entries are ignored during
/// resolution. The open path logs at debug, since options may legitimately be
/// vended for bases the loaded version does not register; the write path logs
/// at warn, since ids are already assigned there and an unmatched id is much
/// more likely a mistake.
pub(crate) fn log_unregistered_base_scoped_options(
    store_params: Option<&ObjectStoreParams>,
    base_paths: &HashMap<u32, BasePath>,
    level: log::Level,
) {
    if !log::log_enabled!(level) {
        return;
    }
    let Some(options) = store_params.and_then(|params| params.storage_options()) else {
        return;
    };
    let unregistered = options
        .keys()
        .filter_map(|key| parse_base_scoped_key(key).map(|(id, _)| id))
        .filter(|id| !base_paths.contains_key(id))
        .collect::<BTreeSet<_>>();
    if !unregistered.is_empty() {
        log::log!(
            level,
            "Ignoring base-scoped storage options for unregistered base path ids: {:?}",
            unregistered
        );
    }
}

fn write_store_params_for_base(params: &WriteParams, base_path: &BasePath) -> ObjectStoreParams {
    // Exact per-URI bindings are used as-is. Otherwise the write-level default
    // params are resolved for the base scope: `base_<id>.<key>` storage
    // options overlay the shared defaults for that base.
    if let Some(store_params) = params
        .base_store_params
        .as_ref()
        .and_then(|base_store_params| base_store_params.get(&base_path.path))
    {
        return store_params.clone();
    }
    let default_params = params.store_params.clone().unwrap_or_default();
    match default_params.scoped_to_base(Some(base_path.id)) {
        Cow::Owned(scoped_params) => scoped_params,
        Cow::Borrowed(_) => default_params,
    }
}

async fn append_external_initial_bases(
    initial_bases: Option<&Vec<BasePath>>,
    store_registry: Arc<ObjectStoreRegistry>,
    params: &WriteParams,
    candidates: &mut Vec<ExternalBaseCandidate>,
    seen_base_ids: &mut HashSet<u32>,
) -> Result<()> {
    if let Some(initial_bases) = initial_bases {
        for base_path in initial_bases {
            let store_params = write_store_params_for_base(params, base_path);
            let (store, extracted_path) = ObjectStore::from_uri_and_params(
                store_registry.clone(),
                &base_path.path,
                &store_params,
            )
            .await?;
            append_external_base_candidate(
                base_path,
                store.store_prefix.clone(),
                extracted_path,
                store_params,
                candidates,
                seen_base_ids,
            );
        }
    }
    Ok(())
}

async fn build_external_base_resolver(
    dataset: Option<&Dataset>,
    params: &WriteParams,
) -> Result<ExternalBaseResolver> {
    let store_registry = dataset
        .map(|ds| ds.session.store_registry())
        .unwrap_or_else(|| params.store_registry());

    let mut seen_base_ids = HashSet::new();
    let mut candidates = vec![];

    if let Some(dataset) = dataset {
        for base_path in dataset.manifest.base_paths.values() {
            let store_params = dataset.store_params_for_base(Some(base_path));
            let (store, extracted_path) = ObjectStore::from_uri_and_params(
                store_registry.clone(),
                &base_path.path,
                &store_params,
            )
            .await?;
            append_external_base_candidate(
                base_path,
                store.store_prefix.clone(),
                extracted_path,
                store_params,
                &mut candidates,
                &mut seen_base_ids,
            );
        }
    }

    append_external_initial_bases(
        params.initial_bases.as_ref(),
        store_registry.clone(),
        params,
        &mut candidates,
        &mut seen_base_ids,
    )
    .await?;

    Ok(ExternalBaseResolver::new(candidates, store_registry))
}

pub(super) async fn blob_v2_external_base_resolver(
    dataset: Option<&Dataset>,
    params: &WriteParams,
    schema: &Schema,
) -> Result<Option<Arc<ExternalBaseResolver>>> {
    if schema.fields_pre_order().any(|field| field.is_blob_v2()) {
        Ok(Some(Arc::new(
            build_external_base_resolver(dataset, params).await?,
        )))
    } else {
        Ok(None)
    }
}

/// Writes the given data to the dataset and returns fragments.
///
/// NOTE: the fragments have not yet been assigned an ID. That must be done
/// by the caller. This is so this function can be called in parallel, and the
/// IDs can be assigned after writing is complete.
///
/// This is a private variant that takes a `SendableRecordBatchStream` instead
/// of a reader. We don't expose the stream at our interface because it is a
/// DataFusion type.
///
/// The caller must resolve `storage_version` once for the operation. Operations
/// that also select a commit format must reuse the same value when committing.
#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip_all)]
pub async fn write_fragments_internal(
    storage_version: ConcreteFileVersion,
    dataset: Option<&Dataset>,
    object_store: Arc<ObjectStore>,
    base_dir: &Path,
    schema: Schema,
    data: SendableRecordBatchStream,
    params: WriteParams,
    target_bases_info: Option<Vec<TargetBaseInfo>>,
) -> Result<(Vec<Fragment>, Schema)> {
    let mut params = params;
    let adapter = SchemaAdapter::new(data.schema());

    let (data, converted_schema) = if adapter.requires_physical_conversion() {
        let data = adapter.to_physical_stream(data);
        // Update the schema to match the converted data
        let arrow_schema = data.schema();
        let converted_schema = Schema::try_from(arrow_schema.as_ref())?;
        (data, converted_schema)
    } else {
        // No conversion needed, use original schema to preserve dictionary info
        (data, schema)
    };

    // Make sure the max rows per group is not larger than the max rows per file
    params.max_rows_per_group = std::cmp::min(params.max_rows_per_group, params.max_rows_per_file);
    validate_external_blob_write_params(&params)?;
    let normalized_converted_schema = prepared_to_logical_blob_schema(&converted_schema)?;

    versions::write_fragments(
        storage_version,
        dataset,
        object_store,
        base_dir,
        normalized_converted_schema,
        data,
        params,
        target_bases_info,
    )
    .await
}

pub(super) fn prepare_write_schema(
    dataset: Option<&Dataset>,
    normalized_converted_schema: Schema,
    params: &WriteParams,
    mut schema_compare_options: lance_core::datatypes::SchemaCompareOptions,
) -> Result<Schema> {
    let schema = if let Some(dataset) = dataset
        && matches!(params.mode, WriteMode::Append | WriteMode::Create)
    {
        schema_compare_options.compare_nullability = NullabilityComparison::Ignore;
        schema_compare_options.allow_missing_if_nullable = true;
        schema_compare_options.ignore_field_order = true;
        normalized_converted_schema.check_compatible(dataset.schema(), &schema_compare_options)?;
        validate_blob_threshold_metadata_for_append(
            &normalized_converted_schema,
            dataset.schema(),
        )?;
        dataset.schema().project_by_schema(
            &normalized_converted_schema,
            OnMissing::Error,
            OnTypeMismatch::Error,
        )?
    } else {
        normalized_converted_schema
    };
    Ok(schema)
}

pub(super) fn validate_legacy_blob_write_schema(
    schema: &Schema,
    version_debug: &str,
) -> Result<()> {
    if schema.fields_pre_order().any(|field| field.is_blob_v2()) {
        return Err(Error::invalid_input(format!(
            "Blob v2 requires file version >= 2.2 (got {version_debug})"
        )));
    }
    Ok(())
}

pub(super) fn validate_blob_v2_write_schema(schema: &Schema) -> Result<()> {
    if let Some(blob_field_path) = legacy_blob_field_path(schema) {
        return Err(Error::invalid_input(format!(
            "Legacy blob columns (field metadata key {BLOB_META_KEY:?}) are not supported for file version >= 2.2. Found legacy blob field: {blob_field_path}. Use the blob v2 extension type (ARROW:extension:name = \"lance.blob.v2\") and the new blob APIs (e.g. lance::blob::blob_field / lance::blob::BlobArrayBuilder)."
        )));
    }
    Ok(())
}

pub(crate) async fn create_seed_writers_current(
    dataset: Option<&Dataset>,
    params: &WriteParams,
) -> Result<Vec<Box<dyn lance_index::scalar::seed::IndexSeedWriter>>> {
    // Seeds only make sense when appending to an existing dataset.
    if !matches!(params.mode, WriteMode::Append) {
        return Ok(Vec::new());
    }
    let Some(dataset) = dataset else {
        return Ok(Vec::new());
    };

    let indices: Arc<Vec<IndexMetadata>> = dataset.load_indices().await?;
    let mut writers: Vec<Box<dyn lance_index::scalar::seed::IndexSeedWriter>> = Vec::new();

    for index in indices.iter() {
        if index.fields.len() != 1 {
            continue;
        }
        let field_id = index.fields[0];
        let Ok(field_path) = dataset.schema().field_path(field_id) else {
            continue;
        };
        let Some(data_type) = dataset.schema().field(&field_path).map(|f| f.data_type()) else {
            continue;
        };

        let Ok(index_details) = fetch_index_details(dataset, &field_path, index).await else {
            continue;
        };
        let details = IndexDetails(index_details.clone());
        let Ok(plugin) = details.get_plugin() else {
            continue;
        };
        if let Some(writer) = plugin
            .create_seed_writer(&field_path, &data_type, &index_details)
            .await?
        {
            writers.push(writer);
        }
    }

    Ok(writers)
}

fn legacy_blob_field_path(schema: &Schema) -> Option<String> {
    schema
        .fields_pre_order()
        .find(|field| field.metadata.contains_key(BLOB_META_KEY))
        .map(|field| {
            schema
                .field_path(field.id)
                .unwrap_or_else(|_| field.name.clone())
        })
}

#[async_trait::async_trait]
pub trait GenericWriter: Send {
    /// Write the given batches to the file
    async fn write(&mut self, batches: &[RecordBatch]) -> Result<()>;
    /// Get the file path and base ID for the data file being written.
    fn data_file_path(&self) -> (&str, Option<u32>);
    /// Get the current position in the file
    ///
    /// We use this to know when the file is too large and we need to start
    /// a new file
    async fn tell(&mut self) -> Result<u64>;
    /// Finish writing the file (flush the remaining data and write footer)
    async fn finish(&mut self) -> Result<(u32, DataFile)>;

    /// Add a global buffer to the current file. Returns the 1-based buffer index.
    /// Must be called before `finish`. No-op on legacy (V1) files (returns `Ok(1)`).
    async fn add_global_buffer(&mut self, _buffer: Bytes) -> Result<u32> {
        Ok(1)
    }

    /// Add a key-value pair to the file's schema metadata.
    /// Must be called before `finish`. No-op on legacy (V1) files.
    fn add_schema_metadata(&mut self, _key: String, _value: String) {}
}

struct V1WriterAdapter<M>
where
    M: V1ManifestProvider + Send + Sync,
{
    writer: V1FileWriter<M>,
    path: String,
    base_id: Option<u32>,
}

#[async_trait::async_trait]
impl<M> GenericWriter for V1WriterAdapter<M>
where
    M: V1ManifestProvider + Send + Sync,
{
    async fn write(&mut self, batches: &[RecordBatch]) -> Result<()> {
        self.writer.write(batches).await
    }
    fn data_file_path(&self) -> (&str, Option<u32>) {
        (&self.path, self.base_id)
    }
    async fn tell(&mut self) -> Result<u64> {
        Ok(self.writer.tell().await? as u64)
    }
    async fn finish(&mut self) -> Result<(u32, DataFile)> {
        let summary = self.writer.finish().await?;
        Ok((
            summary.num_rows as u32,
            DataFile::new_legacy(
                self.path.clone(),
                self.writer.schema(),
                NonZero::new(summary.size_bytes),
                self.base_id,
            ),
        ))
    }
}

struct V2WriterAdapter {
    writer: current_writer::FileWriter,
    data_file: Option<DataFile>,
    preprocessor: Option<BlobPreprocessor>,
}

#[async_trait::async_trait]
impl GenericWriter for V2WriterAdapter {
    async fn write(&mut self, batches: &[RecordBatch]) -> Result<()> {
        if let Some(pre) = self.preprocessor.as_mut() {
            let processed = preprocess_blob_batches(batches, pre).await?;
            for batch in processed {
                self.writer.write_batch(&batch).await?;
            }
        } else {
            for batch in batches {
                self.writer.write_batch(batch).await?;
            }
        }
        Ok(())
    }
    fn data_file_path(&self) -> (&str, Option<u32>) {
        self.data_file
            .as_ref()
            .map(|data_file| (data_file.path.as_str(), data_file.base_id))
            .unwrap_or(("", None))
    }
    async fn tell(&mut self) -> Result<u64> {
        Ok(self.writer.tell().await?)
    }
    async fn finish(&mut self) -> Result<(u32, DataFile)> {
        if let Some(pre) = self.preprocessor.as_mut() {
            pre.finish().await?;
        }
        let field_ids = self
            .writer
            .field_id_to_column_indices()
            .iter()
            .map(|(field_id, _)| *field_id as i32)
            .collect::<Vec<_>>();
        let column_indices = self
            .writer
            .field_id_to_column_indices()
            .iter()
            .map(|(_, column_index)| *column_index as i32)
            .collect::<Vec<_>>();
        let write_summary = self.writer.finish().await?;
        let mut data_file = self
            .data_file
            .take()
            .ok_or_else(|| Error::internal("current writer was already finished"))?;
        data_file.fields = field_ids.into();
        data_file.column_indices = column_indices.into();
        data_file.file_size_bytes = NonZero::new(write_summary.size_bytes).into();
        Ok((write_summary.num_rows as u32, data_file))
    }

    async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        self.writer.add_global_buffer(buffer).await
    }

    fn add_schema_metadata(&mut self, key: String, value: String) {
        self.writer.add_schema_metadata(key, value);
    }
}

#[derive(Default)]
pub(crate) struct WriterOptions {
    add_data_dir: bool,
    base_id: Option<u32>,
    external_base_resolver: Option<Arc<ExternalBaseResolver>>,
    allow_external_blob_outside_bases: bool,
    external_blob_mode: ExternalBlobMode,
    source_store_registry: Arc<ObjectStoreRegistry>,
    source_store_params: ObjectStoreParams,
    blob_pack_file_size_threshold: Option<usize>,
}

impl WriterOptions {
    pub(super) fn update(
        source_store_registry: Arc<ObjectStoreRegistry>,
        external_base_resolver: Option<Arc<ExternalBaseResolver>>,
    ) -> Self {
        Self {
            add_data_dir: true,
            external_base_resolver,
            source_store_registry,
            ..Default::default()
        }
    }
}

pub(crate) async fn open_v1_writer(
    object_store: &ObjectStore,
    schema: &Schema,
    base_dir: &Path,
    options: WriterOptions,
) -> Result<Box<dyn GenericWriter>> {
    let WriterOptions {
        add_data_dir,
        base_id,
        ..
    } = options;
    let (_data_file_key, filename, _data_dir, full_path) =
        prepare_data_file_path(base_dir, add_data_dir);
    Ok(Box::new(V1WriterAdapter {
        writer: V1FileWriter::<ManifestDescribing>::try_new(
            object_store,
            &full_path,
            schema.clone(),
            &Default::default(),
        )
        .await?,
        path: filename,
        base_id,
    }))
}

pub(in crate::dataset) async fn open_current_writer<F>(
    create_file_writer: F,
    object_store: &ObjectStore,
    schema: &Schema,
    base_dir: &Path,
    options: WriterOptions,
) -> Result<Box<dyn GenericWriter>>
where
    F: FnOnce(
        Box<dyn Writer>,
        Schema,
        String,
        Option<u32>,
    ) -> Result<(current_writer::FileWriter, DataFile)>,
{
    let WriterOptions {
        add_data_dir,
        base_id,
        ..
    } = options;
    let (_data_file_key, filename, _data_dir, full_path) =
        prepare_data_file_path(base_dir, add_data_dir);
    let writer = object_store.create(&full_path).await?;
    let (file_writer, data_file) = create_file_writer(writer, schema.clone(), filename, base_id)?;
    Ok(Box::new(V2WriterAdapter {
        writer: file_writer,
        data_file: Some(data_file),
        preprocessor: None,
    }))
}

pub(in crate::dataset) async fn open_current_blob_v2_writer<F>(
    create_file_writer: F,
    object_store: &ObjectStore,
    schema: &Schema,
    base_dir: &Path,
    options: WriterOptions,
) -> Result<Box<dyn GenericWriter>>
where
    F: FnOnce(
        Box<dyn Writer>,
        Schema,
        String,
        Option<u32>,
    ) -> Result<(current_writer::FileWriter, DataFile)>,
{
    let WriterOptions {
        add_data_dir,
        base_id,
        external_base_resolver,
        allow_external_blob_outside_bases,
        external_blob_mode,
        source_store_registry,
        source_store_params,
        blob_pack_file_size_threshold,
    } = options;
    let (data_file_key, filename, data_dir, full_path) =
        prepare_data_file_path(base_dir, add_data_dir);
    let writer = object_store.create(&full_path).await?;
    let (file_writer, data_file) = create_file_writer(writer, schema.clone(), filename, base_id)?;
    let preprocessor = BlobPreprocessor::new(
        object_store.clone(),
        data_dir,
        data_file_key,
        schema,
        external_base_resolver,
        allow_external_blob_outside_bases,
        external_blob_mode,
        source_store_registry,
        source_store_params,
        blob_pack_file_size_threshold,
    )?;
    Ok(Box::new(V2WriterAdapter {
        writer: file_writer,
        data_file: Some(data_file),
        preprocessor: Some(preprocessor),
    }))
}

fn prepare_data_file_path(base_dir: &Path, add_data_dir: bool) -> (String, String, Path, Path) {
    let data_file_key = generate_random_filename();
    let filename = format!("{}.lance", data_file_key);
    let data_dir = if add_data_dir {
        base_dir.clone().join(DATA_DIR)
    } else {
        base_dir.clone()
    };
    let full_path = data_dir.clone().join(filename.as_str());
    (data_file_key, filename, data_dir, full_path)
}

/// Reserved base id that refers to the dataset's primary storage in
/// [`WriteParams::target_bases`]. Real base ids are assigned starting from 1,
/// so 0 is never a registered base. Files written through a primary slot
/// carry no base id, exactly like a write without target bases.
pub const PRIMARY_BASE_ID: u32 = 0;

/// Information about a target base for writing.
/// Contains the base ID, object store, directory path, and whether it's a dataset root.
#[derive(Clone)]
pub struct TargetBaseInfo {
    /// The registered base id, or [`PRIMARY_BASE_ID`] for the dataset's
    /// primary storage.
    pub base_id: u32,
    pub object_store: Arc<ObjectStore>,
    /// The base directory path (without /data subdirectory)
    pub base_dir: Path,
    /// Whether this base path is a dataset root.
    /// If true, /data will be added when creating file paths.
    /// If false, files will be written directly to base_dir.
    pub is_dataset_root: bool,
}

struct WriterGenerator<OpenWriter> {
    /// Default object store (used when no target bases specified)
    object_store: Arc<ObjectStore>,
    /// Default base directory (used when no target bases specified)
    base_dir: Path,
    schema: Schema,
    open_writer: OpenWriter,
    /// Target base information (if writing to specific bases)
    target_bases_info: Option<Vec<TargetBaseInfo>>,
    external_base_resolver: Option<Arc<ExternalBaseResolver>>,
    allow_external_blob_outside_bases: bool,
    external_blob_mode: ExternalBlobMode,
    source_store_registry: Arc<ObjectStoreRegistry>,
    source_store_params: ObjectStoreParams,
    blob_pack_file_size_threshold: Option<usize>,
    /// Counter for round-robin selection
    next_base_index: AtomicUsize,
}

impl<OpenWriter, OpenWriterFuture> WriterGenerator<OpenWriter>
where
    OpenWriter: Fn(Arc<ObjectStore>, Schema, Path, WriterOptions) -> OpenWriterFuture + Send + Sync,
    OpenWriterFuture: Future<Output = Result<Box<dyn GenericWriter>>> + Send,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        object_store: Arc<ObjectStore>,
        base_dir: &Path,
        schema: &Schema,
        open_writer: OpenWriter,
        target_bases_info: Option<Vec<TargetBaseInfo>>,
        external_base_resolver: Option<Arc<ExternalBaseResolver>>,
        allow_external_blob_outside_bases: bool,
        external_blob_mode: ExternalBlobMode,
        source_store_registry: Arc<ObjectStoreRegistry>,
        source_store_params: ObjectStoreParams,
        blob_pack_file_size_threshold: Option<usize>,
    ) -> Self {
        Self {
            object_store,
            base_dir: base_dir.clone(),
            schema: schema.clone(),
            open_writer,
            target_bases_info,
            external_base_resolver,
            allow_external_blob_outside_bases,
            external_blob_mode,
            source_store_registry,
            source_store_params,
            blob_pack_file_size_threshold,
            next_base_index: AtomicUsize::new(0),
        }
    }

    /// Select the next target base using round-robin strategy.
    /// TODO: In the future, we can develop different strategies for selecting target bases
    fn select_target_base(&self) -> Option<&TargetBaseInfo> {
        self.target_bases_info.as_ref().map(|bases| {
            let index = self
                .next_base_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            &bases[index % bases.len()]
        })
    }

    pub async fn new_writer(&self) -> Result<(Box<dyn GenericWriter>, Fragment)> {
        // Use temporary ID 0; will assign ID later.
        let fragment = Fragment::new(0);

        let writer = if let Some(base_info) = self.select_target_base() {
            (self.open_writer)(
                base_info.object_store.clone(),
                self.schema.clone(),
                base_info.base_dir.clone(),
                WriterOptions {
                    add_data_dir: base_info.is_dataset_root,
                    // Primary-storage slots stamp no base id, like a write
                    // without target bases.
                    base_id: (base_info.base_id != PRIMARY_BASE_ID).then_some(base_info.base_id),
                    external_base_resolver: self.external_base_resolver.clone(),
                    allow_external_blob_outside_bases: self.allow_external_blob_outside_bases,
                    external_blob_mode: self.external_blob_mode,
                    source_store_registry: self.source_store_registry.clone(),
                    source_store_params: self.source_store_params.clone(),
                    blob_pack_file_size_threshold: self.blob_pack_file_size_threshold,
                },
            )
            .await?
        } else {
            (self.open_writer)(
                self.object_store.clone(),
                self.schema.clone(),
                self.base_dir.clone(),
                WriterOptions {
                    add_data_dir: true,
                    base_id: None,
                    external_base_resolver: self.external_base_resolver.clone(),
                    allow_external_blob_outside_bases: self.allow_external_blob_outside_bases,
                    external_blob_mode: self.external_blob_mode,
                    source_store_registry: self.source_store_registry.clone(),
                    source_store_params: self.source_store_params.clone(),
                    blob_pack_file_size_threshold: self.blob_pack_file_size_threshold,
                },
            )
            .await?
        };

        Ok((writer, fragment))
    }
}

// Given input options resolve what the commit handler should be.
async fn resolve_commit_handler(
    uri: &str,
    commit_handler: Option<Arc<dyn CommitHandler>>,
    store_options: &Option<ObjectStoreParams>,
) -> Result<Arc<dyn CommitHandler>> {
    match commit_handler {
        None => {
            #[allow(deprecated)]
            if store_options
                .as_ref()
                .map(|opts| opts.object_store.is_some())
                .unwrap_or_default()
            {
                return Err(Error::invalid_input(
                    "when creating a dataset with a custom object store the commit_handler must also be specified",
                ));
            }
            commit_handler_from_url(uri, store_options).await
        }
        Some(commit_handler) => {
            if uri.starts_with("s3+ddb") {
                Err(Error::invalid_input(
                    "`s3+ddb://` scheme and custom commit handler are mutually exclusive",
                ))
            } else {
                Ok(commit_handler)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow_array::{Int32Array, RecordBatchIterator, RecordBatchReader, StructArray};
    use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
    use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter};
    use datafusion_physical_plan::RecordBatchStream;
    use futures::TryStreamExt;
    use lance_datafusion::chunker::chunk_stream;
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};
    use lance_file::version::ConcreteFileVersion;
    use lance_file::versions::v1::reader::FileReader as V1FileReader;
    use lance_io::object_store::StorageOptionsAccessor;
    use lance_io::traits::Reader;
    use lance_table::format::BasePath;

    async fn open_v2_1_test_writer(
        object_store: Arc<ObjectStore>,
        schema: Schema,
        base_dir: Path,
        options: WriterOptions,
    ) -> Result<Box<dyn GenericWriter>> {
        open_current_writer(
            |object_writer, schema, filename, base_id| {
                let writer = lance_file::versions::v2_1::create_writer(
                    object_writer,
                    schema,
                    lance_file::writer::FileWriterOptions::default(),
                )?
                .into();
                let mut data_file = DataFile::new_unstarted(filename, ConcreteFileVersion::V2_1);
                data_file.base_id = base_id;
                Ok((writer, data_file))
            },
            &object_store,
            &schema,
            &base_dir,
            options,
        )
        .await
    }

    #[test]
    fn test_auto_cleanup_disabled_by_default() {
        // Auto-cleanup must be off by default: the cleanup hook is expensive on
        // object stores and the 14-day default rarely deletes anything anyway.
        // See https://github.com/lance-format/lance/issues/6728
        let params = WriteParams::default();
        assert!(params.auto_cleanup.is_none());
        assert!(!params.skip_auto_cleanup);
    }

    #[tokio::test]
    async fn test_chunking_large_batches() {
        // Create a stream of 3 batches of 10 rows
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from_iter(0..28))])
                .unwrap();
        let batches: Vec<RecordBatch> =
            vec![batch.slice(0, 10), batch.slice(10, 10), batch.slice(20, 8)];
        let stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        );

        // Chunk into a stream of 3 row batches
        let chunks: Vec<Vec<RecordBatch>> = chunk_stream(Box::pin(stream), 3)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0].len(), 1);

        for (i, chunk) in chunks.iter().enumerate() {
            let num_rows = chunk.iter().map(|batch| batch.num_rows()).sum::<usize>();
            if i < chunks.len() - 1 {
                assert_eq!(num_rows, 3);
            } else {
                // Last chunk is shorter
                assert_eq!(num_rows, 1);
            }
        }

        // The fourth chunk is split along the boundary between the original first
        // two batches.
        assert_eq!(chunks[3].len(), 2);
        assert_eq!(chunks[3][0].num_rows(), 1);
        assert_eq!(chunks[3][1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_chunking_small_batches() {
        // Create a stream of 10 batches of 3 rows
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from_iter(0..30))])
                .unwrap();

        let batches: Vec<RecordBatch> = (0..10).map(|i| batch.slice(i * 3, 3)).collect();
        let stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        );

        // Chunk into a stream of 10 row batches
        let chunks: Vec<Vec<RecordBatch>> = chunk_stream(Box::pin(stream), 10)
            .try_collect()
            .await
            .unwrap();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 4);
        assert_eq!(chunks[0][0], batch.slice(0, 3));
        assert_eq!(chunks[0][1], batch.slice(3, 3));
        assert_eq!(chunks[0][2], batch.slice(6, 3));
        assert_eq!(chunks[0][3], batch.slice(9, 1));

        for chunk in &chunks {
            let num_rows = chunk.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(num_rows, 10);
        }
    }

    #[tokio::test]
    async fn test_file_size() {
        let reader_to_frags = |data_reader: Box<dyn RecordBatchReader + Send>| {
            let schema = data_reader.schema();
            let data_reader =
                data_reader.map(|rb| rb.map_err(datafusion::error::DataFusionError::from));

            let data_stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(data_reader),
            ));

            let write_params = WriteParams {
                max_rows_per_file: 1024 * 1024, // Won't be limited by this
                max_bytes_per_file: 2 * 1024,
                mode: WriteMode::Create,
                ..Default::default()
            };

            async move {
                let schema = Schema::try_from(schema.as_ref()).unwrap();

                let object_store = Arc::new(ObjectStore::memory());
                write_fragments_internal(
                    write_params.storage_version_or_default(),
                    None,
                    object_store,
                    &Path::from("test"),
                    schema,
                    data_stream,
                    write_params,
                    None,
                )
                .await
            }
        };

        // The writer will not generate a new file until at enough data is *written* (not
        // just accumulated) to justify a new file.  Since the default page size is 8MiB
        // we actually need to generate quite a bit of data to trigger this.
        //
        // To avoid generating and writing millions of rows (which is a bit slow for a unit
        // test) we can use a large data type (1KiB binary)
        let data_reader = Box::new(
            gen_batch()
                .anon_col(array::rand_fsb(1024))
                .into_reader_rows(RowCount::from(10 * 1024), BatchCount::from(2)),
        );

        let (fragments, _) = reader_to_frags(data_reader).await.unwrap();

        assert_eq!(fragments.len(), 2);
    }

    #[tokio::test]
    async fn test_max_rows_per_file() {
        let reader_to_frags = |data_reader: Box<dyn RecordBatchReader + Send>| {
            let schema = data_reader.schema();
            let data_reader =
                data_reader.map(|rb| rb.map_err(datafusion::error::DataFusionError::from));

            let data_stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(data_reader),
            ));

            let write_params = WriteParams {
                max_rows_per_file: 5000,                // Limit by rows
                max_bytes_per_file: 1024 * 1024 * 1024, // Won't be limited by this
                mode: WriteMode::Create,
                ..Default::default()
            };

            async move {
                let schema = Schema::try_from(schema.as_ref()).unwrap();

                let object_store = Arc::new(ObjectStore::memory());
                write_fragments_internal(
                    write_params.storage_version_or_default(),
                    None,
                    object_store,
                    &Path::from("test"),
                    schema,
                    data_stream,
                    write_params,
                    None,
                )
                .await
            }
        };

        // Generate 12000 rows total, which should create 3 files:
        // - File 1: 5000 rows
        // - File 2: 5000 rows
        // - File 3: 2000 rows
        let data_reader = Box::new(
            gen_batch()
                .anon_col(array::rand_type(&DataType::Int32))
                .into_reader_rows(RowCount::from(12000), BatchCount::from(1)),
        );

        let (fragments, _) = reader_to_frags(data_reader).await.unwrap();

        // Should have 3 fragments
        assert_eq!(fragments.len(), 3);

        // Verify the row count distribution
        let row_counts: Vec<usize> = fragments
            .iter()
            .map(|f| f.physical_rows.unwrap_or(0))
            .collect();
        assert_eq!(row_counts, vec![5000, 5000, 2000]);
    }

    #[tokio::test]
    async fn test_max_rows_per_group() {
        let reader_to_frags = |data_reader: Box<dyn RecordBatchReader + Send>,
                               version: LanceFileVersion| {
            let schema = data_reader.schema();
            let data_reader =
                data_reader.map(|rb| rb.map_err(datafusion::error::DataFusionError::from));

            let data_stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(data_reader),
            ));

            let write_params = WriteParams {
                max_rows_per_file: 5000,  // Smaller than total data to force multiple files
                max_rows_per_group: 3000, // Row group size affects V1 only
                mode: WriteMode::Create,
                data_storage_version: Some(version),
                ..Default::default()
            };

            async move {
                let schema = Schema::try_from(schema.as_ref()).unwrap();

                let object_store = Arc::new(ObjectStore::memory());
                write_fragments_internal(
                    write_params.storage_version_or_default(),
                    None,
                    object_store,
                    &Path::from("test"),
                    schema,
                    data_stream,
                    write_params,
                    None,
                )
                .await
            }
        };

        // Test V1 (Legacy) version: max_rows_per_group affects chunking
        // With max_rows_per_group=3000 and max_rows_per_file=5000:
        // - Stream is chunked into batches of max 3000 rows
        // - Batches are written to files, splitting when file exceeds 5000 rows
        // For 9000 rows:
        //   - Chunk 1 (3000 rows) -> File 1 (6000 rows) - exceeds limit, triggers new file
        //   - Chunk 2 (3000 rows) -> File 2 (3000 rows) - start of new file
        // Result: 2 fragments with [6000, 3000] rows
        // Note: The exact behavior depends on when file splitting occurs
        let data_reader_v1 = Box::new(
            gen_batch()
                .anon_col(array::rand_type(&DataType::Int32))
                .into_reader_rows(RowCount::from(9000), BatchCount::from(1)),
        );

        let (fragments_v1, _) = reader_to_frags(data_reader_v1, LanceFileVersion::Legacy)
            .await
            .unwrap();
        let row_counts_v1: Vec<usize> = fragments_v1
            .iter()
            .map(|f| f.physical_rows.unwrap_or(0))
            .collect();

        // V1 creates 2 fragments based on row group chunking and file size limit
        assert_eq!(fragments_v1.len(), 2);
        assert_eq!(row_counts_v1, vec![6000, 3000]);

        // Test V2+ version: max_rows_per_group is ignored, only max_rows_per_file matters
        // With max_rows_per_file=5000 and 9000 rows:
        // - Stream is not chunked by row group size
        // - Data is split only at file boundaries (5000 rows per file)
        // Result: 2 fragments with [5000, 4000] rows
        // V2 splits data more evenly at file boundaries regardless of row group size
        let data_reader_v2 = Box::new(
            gen_batch()
                .anon_col(array::rand_type(&DataType::Int32))
                .into_reader_rows(RowCount::from(9000), BatchCount::from(1)),
        );

        let (fragments_v2, _) = reader_to_frags(data_reader_v2, LanceFileVersion::Stable)
            .await
            .unwrap();
        let row_counts_v2: Vec<usize> = fragments_v2
            .iter()
            .map(|f| f.physical_rows.unwrap_or(0))
            .collect();

        // V2 should create 2 fragments based on file size only
        assert_eq!(fragments_v2.len(), 2);
        assert_eq!(row_counts_v2, vec![5000, 4000]);

        // Key difference: Both V1 and V2 create 2 fragments, but with different distributions
        // - V1: [6000, 3000] - chunking by row groups affects distribution
        // - V2: [5000, 4000] - split only at file boundaries, more even
        // V2 distribution should be more even (closer to 5000/5000 split)
        // V1 distribution is affected by row group chunking (3000)
        assert_eq!(fragments_v1.len(), fragments_v2.len());
        assert_ne!(row_counts_v1, row_counts_v2);
    }

    #[tokio::test]
    async fn test_file_write_version() {
        let schema = Arc::new(ArrowSchema::new(vec![arrow::datatypes::Field::new(
            "a",
            DataType::Int32,
            false,
        )]));

        // Write 1024 rows
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter(0..1024))],
        )
        .unwrap();

        let versions = vec![
            LanceFileVersion::Legacy,
            LanceFileVersion::V2_0,
            LanceFileVersion::V2_1,
            LanceFileVersion::V2_2,
            LanceFileVersion::Stable,
            LanceFileVersion::Next,
        ];
        for version in versions {
            let (major, minor) = ConcreteFileVersion::from(version).to_data_file_numbers();
            let write_params = WriteParams {
                data_storage_version: Some(version),
                // This parameter should be ignored
                max_rows_per_group: 1,
                ..Default::default()
            };

            let data_stream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(std::iter::once(Ok(batch.clone()))),
            ));

            let schema = Schema::try_from(schema.as_ref()).unwrap();

            let object_store = Arc::new(ObjectStore::memory());
            let (fragments, _) = write_fragments_internal(
                ConcreteFileVersion::from(version),
                None,
                object_store,
                &Path::from("test"),
                schema,
                data_stream,
                write_params,
                None,
            )
            .await
            .unwrap();

            assert_eq!(fragments.len(), 1);
            let fragment = &fragments[0];
            assert_eq!(fragment.files.len(), 1);
            assert_eq!(fragment.physical_rows, Some(1024));
            assert_eq!(
                fragment.files[0].file_major_version, major,
                "version: {}",
                version
            );
            assert_eq!(
                fragment.files[0].file_minor_version, minor,
                "version: {}",
                version
            );
        }
    }

    #[tokio::test]
    async fn test_file_v1_schema_order() {
        // Create a schema where fields ids are not in order and contain holes.
        // Also first field id is a struct.
        let struct_fields = Fields::from(vec![ArrowField::new("b", DataType::Int32, false)]);
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("d", DataType::Int32, false),
            ArrowField::new("a", DataType::Struct(struct_fields.clone()), false),
        ]);
        let mut schema = Schema::try_from(&arrow_schema).unwrap();
        // Make schema:
        // 0: a
        // 1: a.b
        // (hole at 2)
        // 3: d
        schema.mut_field_by_id(0).unwrap().id = 3;
        schema.mut_field_by_id(1).unwrap().id = 0;
        schema.mut_field_by_id(2).unwrap().id = 1;

        let field_ids = schema.fields_pre_order().map(|f| f.id).collect::<Vec<_>>();
        assert_eq!(field_ids, vec![3, 0, 1]);

        let data = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StructArray::new(
                    struct_fields,
                    vec![Arc::new(Int32Array::from(vec![3, 4]))],
                    None,
                )),
            ],
        )
        .unwrap();

        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let data_stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(arrow_schema),
            futures::stream::iter(std::iter::once(Ok(data.clone()))),
        ));

        let object_store = Arc::new(ObjectStore::memory());
        let base_path = Path::from("test");
        let (fragments, _) = write_fragments_internal(
            ConcreteFileVersion::V1,
            None,
            object_store.clone(),
            &base_path,
            schema.clone(),
            data_stream,
            write_params,
            None,
        )
        .await
        .unwrap();

        assert_eq!(fragments.len(), 1);
        let fragment = &fragments[0];
        assert_eq!(fragment.files.len(), 1);
        assert_eq!(fragment.files[0].fields.as_ref(), &[0, 1, 3]);

        let path = base_path
            .clone()
            .join(DATA_DIR)
            .join(fragment.files[0].path.as_str());
        let file_reader: Arc<dyn Reader> = object_store.open(&path).await.unwrap().into();
        let reader = V1FileReader::try_new_from_reader(
            &path,
            file_reader,
            None,
            schema.clone(),
            0,
            0,
            3,
            None,
        )
        .await
        .unwrap();
        assert_eq!(reader.num_batches(), 1);
        let batch = reader.read_batch(0, .., &schema).await.unwrap();
        assert_eq!(batch, data);
    }

    #[cfg(feature = "azure")]
    fn azure_store_params(account_name: &str) -> ObjectStoreParams {
        ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("account_name".to_string(), account_name.to_string()),
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                ]),
            ))),
            ..Default::default()
        }
    }

    #[cfg(feature = "azure")]
    #[tokio::test]
    async fn test_validate_and_resolve_target_bases_uses_base_store_params() {
        let mut params = WriteParams::default()
            .with_target_bases(vec![1, 2])
            .with_base_store_params("az://container/path-a", azure_store_params("account-a"))
            .with_base_store_params("az://container/path-b", azure_store_params("account-b"));

        let existing_base_paths = azure_base_paths_a_b();

        let target_bases =
            validate_and_resolve_target_bases(&mut params, Some(&existing_base_paths))
                .await
                .unwrap()
                .unwrap();

        assert_eq!(target_bases.len(), 2);
        assert_eq!(
            target_bases[0].object_store.store_prefix,
            "az$container@account-a"
        );
        assert_eq!(
            target_bases[1].object_store.store_prefix,
            "az$container@account-b"
        );
    }

    #[cfg(feature = "azure")]
    fn azure_base_paths_a_b() -> HashMap<u32, BasePath> {
        HashMap::from([
            (
                1,
                BasePath::new(
                    1,
                    "az://container/path-a".to_string(),
                    Some("base-a".to_string()),
                    false,
                ),
            ),
            (
                2,
                BasePath::new(
                    2,
                    "az://container/path-b".to_string(),
                    Some("base-b".to_string()),
                    false,
                ),
            ),
        ])
    }

    #[cfg(feature = "azure")]
    #[tokio::test]
    async fn test_validate_and_resolve_target_bases_uses_base_scoped_storage_options() {
        // A single flat storage options map carries per-base credentials via
        // the `base_<id>.<key>` convention; unscoped keys are shared defaults.
        let store_params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("account_name".to_string(), "account-shared".to_string()),
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                    ("base_1.account_name".to_string(), "account-a".to_string()),
                    ("base_2.account_name".to_string(), "account-b".to_string()),
                ]),
            ))),
            ..Default::default()
        };
        let mut params = WriteParams {
            store_params: Some(store_params),
            ..Default::default()
        }
        .with_target_bases(vec![1, 2]);

        let existing_base_paths = azure_base_paths_a_b();

        let target_bases =
            validate_and_resolve_target_bases(&mut params, Some(&existing_base_paths))
                .await
                .unwrap()
                .unwrap();

        assert_eq!(target_bases.len(), 2);
        assert_eq!(
            target_bases[0].object_store.store_prefix,
            "az$container@account-a"
        );
        assert_eq!(
            target_bases[1].object_store.store_prefix,
            "az$container@account-b"
        );
    }

    #[cfg(feature = "azure")]
    #[tokio::test]
    async fn test_base_store_params_take_precedence_over_base_scoped_options() {
        let store_params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                    (
                        "base_1.account_name".to_string(),
                        "account-scoped".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };
        let mut params = WriteParams {
            store_params: Some(store_params),
            ..Default::default()
        }
        .with_target_bases(vec![1])
        .with_base_store_params("az://container/path-a", azure_store_params("account-exact"));

        let existing_base_paths = azure_base_paths_a_b();

        let target_bases =
            validate_and_resolve_target_bases(&mut params, Some(&existing_base_paths))
                .await
                .unwrap()
                .unwrap();

        assert_eq!(target_bases.len(), 1);
        assert_eq!(
            target_bases[0].object_store.store_prefix,
            "az$container@account-exact"
        );
    }

    #[tokio::test]
    async fn test_explicit_data_file_bases_writer_generator() {
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use lance_io::object_store::ObjectStore;
        use std::sync::Arc;

        // Create test schema
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        // Create in-memory object store
        let object_store = Arc::new(ObjectStore::memory());
        let base_dir = Path::from("test/bucket2");

        // Test WriterGenerator with explicit data file bases configuration
        let target_bases = vec![TargetBaseInfo {
            base_id: 2,
            object_store: object_store.clone(),
            base_dir: base_dir.clone(),
            is_dataset_root: false, // Test uses direct data directory
        }];
        let writer_generator = WriterGenerator::new(
            object_store.clone(),
            &base_dir,
            &schema,
            open_v2_1_test_writer,
            Some(target_bases),
            None,
            false,
            ExternalBlobMode::Reference,
            Arc::new(ObjectStoreRegistry::default()),
            ObjectStoreParams::default(),
            None,
        );

        // Create a writer
        let (writer, fragment) = writer_generator.new_writer().await.unwrap();

        // Verify fragment is created
        assert_eq!(fragment.id, 0); // Temporary ID

        // Verify writer is created (we can't test much more without writing data)
        drop(writer); // Clean up
    }

    #[tokio::test]
    async fn test_writer_with_base_id() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance_io::object_store::ObjectStore;
        use std::sync::Arc;

        // Create test data
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Create in-memory object store and writer
        let object_store = Arc::new(ObjectStore::memory());
        let base_dir = Path::from("test/bucket2");

        let mut inner_writer = versions::open_writer(
            ConcreteFileVersion::from(LanceFileVersion::Stable),
            &object_store,
            &schema,
            &base_dir,
            WriterOptions {
                add_data_dir: false, // Don't add /data
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Write data
        inner_writer.write(&[batch]).await.unwrap();

        // Finish and manually set base_id
        let base_id = 2u32;
        let (_num_rows, mut data_file) = inner_writer.finish().await.unwrap();
        data_file.base_id = Some(base_id);

        assert_eq!(data_file.base_id, Some(base_id));
        assert!(!data_file.path.is_empty());
    }

    #[tokio::test]
    async fn test_round_robin_target_base_selection() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use arrow::record_batch::RecordBatch;
        use lance_io::object_store::ObjectStore;
        use std::sync::Arc;

        // Create test schema
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        // Create in-memory object stores for different bases
        let store1 = Arc::new(ObjectStore::memory());
        let store2 = Arc::new(ObjectStore::memory());
        let store3 = Arc::new(ObjectStore::memory());

        // Create WriterGenerator with multiple target bases
        let target_bases = vec![
            TargetBaseInfo {
                base_id: 1,
                object_store: store1.clone(),
                base_dir: Path::from("base1"),
                is_dataset_root: false,
            },
            TargetBaseInfo {
                base_id: 2,
                object_store: store2.clone(),
                base_dir: Path::from("base2"),
                is_dataset_root: false,
            },
            TargetBaseInfo {
                base_id: 3,
                object_store: store3.clone(),
                base_dir: Path::from("base3"),
                is_dataset_root: false,
            },
        ];

        let writer_generator = WriterGenerator::new(
            Arc::new(ObjectStore::memory()),
            &Path::from("default"),
            &schema,
            open_v2_1_test_writer,
            Some(target_bases),
            None,
            false,
            ExternalBlobMode::Reference,
            Arc::new(ObjectStoreRegistry::default()),
            ObjectStoreParams::default(),
            None,
        );

        // Create test batch
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Create multiple writers and verify round-robin selection
        let mut base_ids = Vec::new();
        for _ in 0..6 {
            let (mut writer, _fragment) = writer_generator.new_writer().await.unwrap();
            writer.write(std::slice::from_ref(&batch)).await.unwrap();
            let (_num_rows, data_file) = writer.finish().await.unwrap();
            base_ids.push(data_file.base_id.unwrap());
        }

        // Verify round-robin pattern: 1, 2, 3, 1, 2, 3
        assert_eq!(base_ids, vec![1, 2, 3, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_explicit_data_file_bases_path_parsing() {
        // Test URI parsing logic
        let test_cases = vec![
            ("s3://multi-path-test/test1/subBucket2", "test1/subBucket2"),
            ("gs://my-bucket/path/to/data", "path/to/data"),
            ("az://container/path/to/data", "path/to/data"),
            (
                "abfss://filesystem@account.dfs.core.windows.net/path/to/data",
                "path/to/data",
            ),
            ("file:///tmp/test/bucket", "tmp/test/bucket"),
        ];

        for (uri, expected_path) in test_cases {
            let url = url::Url::parse(uri).unwrap();
            let parsed_path = url.path().trim_start_matches('/');
            assert_eq!(parsed_path, expected_path, "Failed for URI: {}", uri);
        }
    }

    #[tokio::test]
    async fn test_write_params_validation() {
        // Test CREATE mode validation
        let mut params = WriteParams {
            mode: WriteMode::Create,
            initial_bases: Some(vec![
                BasePath {
                    id: 1,
                    name: Some("bucket1".to_string()),
                    path: "s3://bucket1/path1".to_string(),
                    is_dataset_root: true,
                },
                BasePath {
                    id: 2,
                    name: Some("bucket2".to_string()),
                    path: "s3://bucket2/path2".to_string(),
                    is_dataset_root: true,
                },
                BasePath {
                    id: 3,
                    name: Some("azure-az-base".to_string()),
                    path: "az://container/path1".to_string(),
                    is_dataset_root: true,
                },
                BasePath {
                    id: 4,
                    name: Some("azure-abfss-base".to_string()),
                    path: "abfss://filesystem@account.dfs.core.windows.net/path1".to_string(),
                    is_dataset_root: true,
                },
            ]),
            target_bases: Some(vec![1]), // Use ID 1 which corresponds to bucket1
            ..Default::default()
        };

        // This should be valid
        let result = validate_write_params(&params);
        assert!(result.is_ok());

        // Test target_bases with ID not in initial_bases (should fail)
        params.target_bases = Some(vec![99]); // ID 99 doesn't exist
        let result = validate_write_params(&params);
        assert!(result.is_err());

        // Test CREATE mode with target_bases but no initial_bases (should fail)
        params.initial_bases = None;
        params.target_bases = Some(vec![1]);
        let result = validate_write_params(&params);
        assert!(result.is_err());
    }

    fn validate_write_params(params: &WriteParams) -> Result<()> {
        validate_external_blob_write_params(params)?;

        // Replicate the validation logic from the main write function
        if matches!(params.mode, WriteMode::Create)
            && let Some(target_bases) = &params.target_bases
        {
            if target_bases.len() != 1 {
                return Err(Error::invalid_input(format!(
                    "target_bases with {} elements is not supported",
                    target_bases.len()
                )));
            }
            let target_base_id = target_bases[0];
            if let Some(initial_bases) = &params.initial_bases {
                if !initial_bases.iter().any(|bp| bp.id == target_base_id) {
                    return Err(Error::invalid_input(format!(
                        "target_base_id {} must be one of the initial_bases in CREATE mode",
                        target_base_id
                    )));
                }
            } else {
                return Err(Error::invalid_input(
                    "initial_bases must be provided when target_bases is specified in CREATE mode",
                ));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_external_blob_mode_validation() {
        let params = WriteParams {
            external_blob_mode: ExternalBlobMode::Ingest,
            allow_external_blob_outside_bases: true,
            ..Default::default()
        };

        let err = validate_write_params(&params).unwrap_err();
        assert!(
            err.to_string()
                .contains("allow_external_blob_outside_bases only applies")
        );
    }

    #[tokio::test]
    async fn test_multi_base_create() {
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        // Create dataset with multi-base configuration
        let test_uri = "memory://multi_base_test";
        let primary_uri = format!("{}/primary", test_uri);
        let base1_uri = format!("{}/base1", test_uri);
        let base2_uri = format!("{}/base2", test_uri);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = crate::dataset::Dataset::write(
            data_gen.batch(5),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        path: base1_uri.clone(),
                        is_dataset_root: true,
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        path: base2_uri.clone(),
                        is_dataset_root: true,
                    },
                ]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Verify dataset was created
        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);

        // Verify base_paths are registered in manifest
        assert_eq!(dataset.manifest.base_paths.len(), 2);
        assert!(
            dataset
                .manifest
                .base_paths
                .values()
                .any(|bp| bp.name == Some("base1".to_string()))
        );
        assert!(
            dataset
                .manifest
                .base_paths
                .values()
                .any(|bp| bp.name == Some("base2".to_string()))
        );

        // Verify data was written to base1
        let fragments = dataset.get_fragments();
        assert!(!fragments.is_empty());
        for fragment in fragments {
            assert!(
                fragment
                    .metadata
                    .files
                    .iter()
                    .any(|file| file.base_id == Some(1))
            );
        }

        // Test validation: cannot specify both target_bases and target_base_names_or_paths
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let result = Dataset::write(
            data_gen2.batch(5),
            &format!("{}/test_validation", test_uri),
            Some(WriteParams {
                mode: WriteMode::Create,
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("base1".to_string()),
                    path: base1_uri.clone(),
                    is_dataset_root: true,
                }]),
                target_bases: Some(vec![1]),
                target_base_names_or_paths: Some(vec!["base1".to_string()]),
                ..Default::default()
            }),
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot specify both target_base_names_or_paths and target_bases")
        );
    }

    #[tokio::test]
    async fn test_multi_base_write_read_with_base_scoped_storage_options() {
        use crate::dataset::builder::DatasetBuilder;
        use lance_core::utils::tempfile::TempStrDir;
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        let primary_dir = TempStrDir::default();
        let base1_dir = TempStrDir::default();

        // Local stores ignore these options; this verifies base-scoped entries
        // flow through the full write/read path without breaking anything.
        let scoped_options = HashMap::from([
            ("shared_option".to_string(), "shared".to_string()),
            (
                "base_1.scoped_option".to_string(),
                "base1-value".to_string(),
            ),
        ]);
        let store_params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                scoped_options.clone(),
            ))),
            ..Default::default()
        };

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen.batch(5),
            primary_dir.as_str(),
            Some(WriteParams {
                mode: WriteMode::Create,
                store_params: Some(store_params),
                initial_bases: Some(vec![BasePath {
                    id: 1,
                    name: Some("base1".to_string()),
                    path: base1_dir.as_str().to_string(),
                    is_dataset_root: true,
                }]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
        for fragment in dataset.get_fragments() {
            assert!(
                fragment
                    .metadata
                    .files
                    .iter()
                    .all(|file| file.base_id == Some(1))
            );
        }

        // Reopen with the same flat options and scan through the base store.
        let dataset = DatasetBuilder::from_uri(primary_dir.as_str())
            .with_storage_options(scoped_options)
            .load()
            .await
            .unwrap();
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let num_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(num_rows, 5);
    }

    #[tokio::test]
    async fn test_multi_base_overwrite() {
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        // Create initial dataset
        let test_uri = "memory://overwrite_test";
        let primary_uri = format!("{}/primary", test_uri);
        let base1_uri = format!("{}/base1", test_uri);
        let base2_uri = format!("{}/base2", test_uri);
        let _base3_uri = format!("{}/base3", test_uri);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen.batch(3),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        path: base1_uri.clone(),
                        is_dataset_root: true,
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        path: base2_uri.clone(),
                        is_dataset_root: true,
                    },
                ]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 3);

        // Overwrite - should inherit existing base configuration (base1, base2)
        // Write to base2
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen2.batch(2),
            std::sync::Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                // No initial_bases - inherits existing base_paths
                target_bases: Some(vec![2]), // Write to base2
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Verify data was overwritten
        assert_eq!(dataset.count_rows(None).await.unwrap(), 2);

        // Verify base_paths were inherited (still base1 and base2)
        assert_eq!(dataset.manifest.base_paths.len(), 2);
        assert!(
            dataset
                .manifest
                .base_paths
                .values()
                .any(|bp| bp.name == Some("base1".to_string()))
        );
        assert!(
            dataset
                .manifest
                .base_paths
                .values()
                .any(|bp| bp.name == Some("base2".to_string()))
        );

        // Verify data was written to base2 (ID 2)
        let fragments = dataset.get_fragments();
        assert!(
            fragments
                .iter()
                .all(|f| f.metadata.files.iter().all(|file| file.base_id == Some(2)))
        );

        // Test validation: cannot specify initial_bases in OVERWRITE mode
        let mut data_gen3 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let result = Dataset::write(
            data_gen3.batch(2),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                initial_bases: Some(vec![BasePath {
                    id: 3,
                    name: Some("base3".to_string()),
                    path: _base3_uri.clone(),
                    is_dataset_root: true,
                }]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot register new bases in Overwrite mode")
        );
    }

    #[tokio::test]
    async fn test_multi_base_append() {
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        // Create initial dataset with multi-base configuration
        let test_uri = "memory://append_test";
        let primary_uri = format!("{}/primary", test_uri);
        let base1_uri = format!("{}/base1", test_uri);
        let base2_uri = format!("{}/base2", test_uri);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen.batch(3),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        path: base1_uri.clone(),
                        is_dataset_root: true,
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        path: base2_uri.clone(),
                        is_dataset_root: true,
                    },
                ]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 3);

        // Append to base1 (same base as initial write)
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen2.batch(2),
            std::sync::Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);

        // Verify base_paths are still registered
        assert_eq!(dataset.manifest.base_paths.len(), 2);

        // Append to base2 (different base)
        let mut data_gen3 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen3.batch(4),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                target_bases: Some(vec![2]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 9);

        // Verify data is distributed across both bases
        let fragments = dataset.get_fragments();
        let has_base1_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(1)));
        let has_base2_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(2)));

        assert!(has_base1_data, "Should have data in base1");
        assert!(has_base2_data, "Should have data in base2");

        // Test validation: cannot specify initial_bases in APPEND mode
        let mut data_gen4 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let base3_uri = format!("{}/base3", test_uri);

        let result = Dataset::write(
            data_gen4.batch(2),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                initial_bases: Some(vec![BasePath {
                    id: 3,
                    name: Some("base3".to_string()),
                    path: base3_uri,
                    is_dataset_root: true,
                }]),
                target_bases: Some(vec![1]),
                ..Default::default()
            }),
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot register new bases in Append mode")
        );
    }

    #[tokio::test]
    async fn test_multi_base_is_dataset_root_flag() {
        use lance_core::utils::tempfile::TempDir;
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        // Create dataset with different is_dataset_root settings using tempdir
        let test_dir = TempDir::default();
        let primary_uri = test_dir.path_str();
        let base1_dir = test_dir.std_path().join("base1");
        let base2_dir = test_dir.std_path().join("base2");

        std::fs::create_dir_all(&base1_dir).unwrap();
        std::fs::create_dir_all(&base2_dir).unwrap();

        let base1_uri = format!("file://{}", base1_dir.display());
        let base2_uri = format!("file://{}", base2_dir.display());

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen.batch(10),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                max_rows_per_file: 5, // Create multiple fragments
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        path: base1_uri.clone(),
                        is_dataset_root: true, // Files will go to base1/data/
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        path: base2_uri.clone(),
                        is_dataset_root: false, // Files will go directly to base2/
                    },
                ]),
                target_bases: Some(vec![1, 2]), // Write to both bases
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 10);

        // Verify base_paths configuration
        assert_eq!(dataset.manifest.base_paths.len(), 2);

        let base1 = dataset
            .manifest
            .base_paths
            .values()
            .find(|bp| bp.name == Some("base1".to_string()))
            .expect("base1 not found");
        let base2 = dataset
            .manifest
            .base_paths
            .values()
            .find(|bp| bp.name == Some("base2".to_string()))
            .expect("base2 not found");

        // Verify is_dataset_root flags are persisted correctly in manifest
        assert!(
            base1.is_dataset_root,
            "base1 should have is_dataset_root=true"
        );
        assert!(
            !base2.is_dataset_root,
            "base2 should have is_dataset_root=false"
        );

        // Verify data was written to both bases
        let fragments = dataset.get_fragments();
        assert!(!fragments.is_empty());

        let has_base1_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(1)));
        let has_base2_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(2)));

        assert!(has_base1_data, "Should have data in base1");
        assert!(has_base2_data, "Should have data in base2");

        // Verify actual file paths on disk
        // For base1 (is_dataset_root=true), files should be in base1/data/
        let base1_data_dir = base1_dir.join("data");
        assert!(base1_data_dir.exists(), "base1/data directory should exist");
        let base1_files: Vec<_> = std::fs::read_dir(&base1_data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "lance")
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !base1_files.is_empty(),
            "base1/data should contain .lance files"
        );

        // For base2 (is_dataset_root=false), files should be directly in base2/
        let base2_files: Vec<_> = std::fs::read_dir(&base2_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "lance")
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !base2_files.is_empty(),
            "base2 should contain .lance files directly"
        );

        // Verify base2 does NOT have a data subdirectory with lance files
        let base2_data_dir = base2_dir.join("data");
        if base2_data_dir.exists() {
            let base2_data_files: Vec<_> = std::fs::read_dir(&base2_data_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .map(|ext| ext == "lance")
                        .unwrap_or(false)
                })
                .collect();
            assert!(
                base2_data_files.is_empty(),
                "base2/data should NOT contain .lance files"
            );
        }
    }

    #[tokio::test]
    async fn test_multi_base_target_by_path_uri() {
        use lance_core::utils::tempfile::TempDir;
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        // Create dataset with named bases
        let test_dir = TempDir::default();
        let primary_uri = test_dir.path_str();
        let base1_dir = test_dir.std_path().join("base1");
        let base2_dir = test_dir.std_path().join("base2");

        std::fs::create_dir_all(&base1_dir).unwrap();
        std::fs::create_dir_all(&base2_dir).unwrap();

        let base1_uri = format!("file://{}", base1_dir.display());
        let base2_uri = format!("file://{}", base2_dir.display());

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        // Create initial dataset writing to base1 using name
        let dataset = Dataset::write(
            data_gen.batch(10),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                max_rows_per_file: 5,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        path: base1_uri.clone(),
                        is_dataset_root: true,
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        path: base2_uri.clone(),
                        is_dataset_root: true,
                    },
                ]),
                target_base_names_or_paths: Some(vec!["base1".to_string()]), // Use name
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 10);

        // Verify data was written to base1
        let fragments = dataset.get_fragments();
        assert!(
            fragments
                .iter()
                .all(|f| f.metadata.files.iter().all(|file| file.base_id == Some(1)))
        );

        // Now append using the path URI instead of name
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        let dataset = Dataset::write(
            data_gen2.batch(5),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                // Use the actual path URI instead of the name
                target_base_names_or_paths: Some(vec![base2_uri.clone()]),
                max_rows_per_file: 5,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.count_rows(None).await.unwrap(), 15);

        // Verify data is now in both bases
        let fragments = dataset.get_fragments();
        let has_base1_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(1)));
        let has_base2_data = fragments
            .iter()
            .any(|f| f.metadata.files.iter().any(|file| file.base_id == Some(2)));

        assert!(has_base1_data, "Should have data in base1");
        assert!(has_base2_data, "Should have data in base2");

        // Verify base2 has exactly 1 fragment (from the append)
        let base2_fragments: Vec<_> = fragments
            .iter()
            .filter(|f| f.metadata.files.iter().all(|file| file.base_id == Some(2)))
            .collect();
        assert_eq!(base2_fragments.len(), 1, "Should have 1 fragment in base2");
    }

    #[tokio::test]
    async fn test_empty_stream_write() {
        use lance_io::object_store::ObjectStore;

        // Test writing an empty stream
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        // Create an empty stream
        let data_stream = Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema.clone(),
            futures::stream::iter(std::iter::empty::<
                std::result::Result<RecordBatch, DataFusionError>,
            >()),
        ));

        let object_store = Arc::new(ObjectStore::memory());
        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };

        let result = write_fragments_internal(
            write_params.storage_version_or_default(),
            None,
            object_store,
            &Path::from("test_empty"),
            schema,
            data_stream,
            write_params,
            None,
        )
        .await;

        // Empty stream should be handled gracefully
        // It should create an empty dataset or return an appropriate result
        match result {
            Ok((fragments, _)) => {
                // If successful, verify it creates an empty result
                assert!(
                    fragments.is_empty(),
                    "Empty stream should create no fragments"
                );
            }
            Err(e) => {
                panic!("Expected write empty stream success, got error: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_schema_mismatch_on_append() {
        use arrow_array::record_batch;

        // Create initial dataset with two Int32 columns
        let batch1 = record_batch!(
            ("id", Int32, [1, 2, 3, 4, 5]),
            ("value", Int32, [10, 20, 30, 40, 50])
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            })
            .execute(vec![batch1])
            .await
            .unwrap();

        // Verify initial dataset
        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
        assert_eq!(dataset.schema().fields.len(), 2);

        // Try to append with different schema (Float64 instead of Int32 for 'value' column)
        let batch2 = record_batch!(
            ("id", Int32, [6, 7, 8]),
            ("value", Float64, [60.0, 70.0, 80.0])
        )
        .unwrap();

        let result = InsertBuilder::new(Arc::new(dataset.clone()))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![batch2])
            .await;

        // Should fail due to schema mismatch
        assert!(result.is_err(), "Append with mismatched schema should fail");
        let error = result.unwrap_err();
        let error_msg = error.to_string().to_lowercase();
        assert!(
            error_msg.contains("schema")
                || error_msg.contains("type")
                || error_msg.contains("mismatch")
                || error_msg.contains("field")
                || error_msg.contains("not found"),
            "Error should mention schema or type mismatch: {}",
            error_msg
        );

        // Verify original dataset is still intact
        assert_eq!(dataset.count_rows(None).await.unwrap(), 5);
        assert_eq!(dataset.schema().fields.len(), 2);
    }

    #[tokio::test]
    async fn test_disk_full_error() {
        use std::io::{self, ErrorKind};
        use std::sync::Arc;

        use async_trait::async_trait;
        use futures::stream::BoxStream;
        use object_store::{
            CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
            PutMultipartOptions, PutOptions, PutPayload, PutResult,
        };

        // Create a custom ObjectStore that simulates disk full error
        #[derive(Debug)]
        struct DiskFullObjectStore;

        impl std::fmt::Display for DiskFullObjectStore {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "DiskFullObjectStore")
            }
        }

        #[async_trait]
        impl object_store::ObjectStore for DiskFullObjectStore {
            async fn put_opts(
                &self,
                _location: &object_store::path::Path,
                _bytes: PutPayload,
                _opts: PutOptions,
            ) -> object_store::Result<PutResult> {
                Err(object_store::Error::Generic {
                    store: "DiskFullStore",
                    source: Box::new(io::Error::new(
                        ErrorKind::StorageFull,
                        "No space left on device",
                    )),
                })
            }

            async fn put_multipart_opts(
                &self,
                _location: &object_store::path::Path,
                _opts: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                Err(object_store::Error::NotSupported {
                    source: "Multipart upload not supported".into(),
                })
            }

            async fn get_opts(
                &self,
                _location: &object_store::path::Path,
                _options: GetOptions,
            ) -> object_store::Result<GetResult> {
                Err(object_store::Error::NotFound {
                    path: "".into(),
                    source: "".into(),
                })
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, object_store::Result<object_store::path::Path>>,
            ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
                locations
            }

            fn list(
                &self,
                _prefix: Option<&object_store::path::Path>,
            ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
                Box::pin(futures::stream::empty())
            }

            async fn list_with_delimiter(
                &self,
                _prefix: Option<&object_store::path::Path>,
            ) -> object_store::Result<ListResult> {
                Ok(ListResult {
                    common_prefixes: vec![],
                    objects: vec![],
                })
            }

            async fn copy_opts(
                &self,
                _from: &object_store::path::Path,
                _to: &object_store::path::Path,
                _options: CopyOptions,
            ) -> object_store::Result<()> {
                Ok(())
            }
        }

        let object_store = Arc::new(lance_io::object_store::ObjectStore::new(
            Arc::new(DiskFullObjectStore) as Arc<dyn object_store::ObjectStore>,
            // Use a non-"file" scheme so writes go through ObjectWriter (which
            // uses the DiskFullObjectStore) instead of the optimized LocalWriter.
            url::Url::parse("mock:///test").unwrap(),
            None,
            None,
            false,
            true,
            lance_io::object_store::DEFAULT_LOCAL_IO_PARALLELISM,
            lance_io::object_store::DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));

        // Create test data
        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let data_reader = Box::new(RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            arrow_schema.clone(),
        ));

        let data_stream = Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema,
            futures::stream::iter(data_reader.map(|rb| rb.map_err(DataFusionError::from))),
        ));

        let schema = Schema::try_from(data_stream.schema().as_ref()).unwrap();

        let write_params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };

        // Attempt to write data - should fail with IO error due to disk full
        let result = write_fragments_internal(
            write_params.storage_version_or_default(),
            None,
            object_store,
            &Path::from("test_disk_full"),
            schema,
            data_stream,
            write_params,
            None,
        )
        .await;

        // Verify that the error is an IO error (which wraps the disk full error)
        assert!(result.is_err(), "Write should fail when disk is full");
        let error = result.unwrap_err();
        let error_msg = error.to_string().to_lowercase();

        // The error should mention IO, space, or storage
        assert!(
            error_msg.contains("io")
                || error_msg.contains("space")
                || error_msg.contains("storage")
                || error_msg.contains("full"),
            "Error should mention IO, space, or storage: {}",
            error_msg
        );

        // Verify it's an IO error type
        assert!(
            matches!(error, lance_core::Error::IO { .. }),
            "Expected IO error, got: {}",
            error
        );
    }

    /// Test that dataset remains consistent after write interruption and can recover.
    /// This verifies that:
    /// 1. The dataset is not corrupted when a write is interrupted (not committed)
    /// 2. Incomplete data files are not visible until committed
    /// 3. The transaction can be retried successfully
    #[tokio::test]
    async fn test_write_interruption_recovery() {
        use super::commit::CommitBuilder;
        use arrow_array::record_batch;
        use lance_core::utils::tempfile::TempDir;

        // Create a temporary directory for testing
        let temp_dir = TempDir::default();
        let dataset_uri = format!("file://{}", temp_dir.std_path().display());

        // First, create a normal dataset with some initial data
        let batch =
            record_batch!(("id", Int32, [1, 2, 3]), ("value", Utf8, ["a", "b", "c"])).unwrap();

        // Write initial dataset normally
        let dataset = InsertBuilder::new(&dataset_uri)
            .execute(vec![batch.clone()])
            .await
            .unwrap();

        // Verify initial dataset is valid
        assert_eq!(dataset.count_rows(None).await.unwrap(), 3);

        // Prepare additional data to write
        let new_batch =
            record_batch!(("id", Int32, [4, 5, 6]), ("value", Utf8, ["d", "e", "f"])).unwrap();

        // Step 1: Write uncommitted data (simulates interrupted write before commit)
        let uncommitted_result = InsertBuilder::new(WriteDestination::Dataset(Arc::new(
            Dataset::open(&dataset_uri).await.unwrap(),
        )))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute_uncommitted(vec![new_batch])
        .await;

        // The uncommitted write should succeed (data is written to files)
        assert!(
            uncommitted_result.is_ok(),
            "Uncommitted write should succeed"
        );
        let transaction = uncommitted_result.unwrap();

        // Step 2: Verify dataset is still consistent (uncommitted changes not visible)
        let dataset_before_commit = Dataset::open(&dataset_uri).await.unwrap();
        let row_count_before = dataset_before_commit.count_rows(None).await.unwrap();
        assert_eq!(
            row_count_before, 3,
            "Dataset should still have only original 3 rows (uncommitted data not visible)"
        );

        // Step 3: Commit to transaction (simulates retry after interruption)
        let commit_result = CommitBuilder::new(&dataset_uri).execute(transaction).await;
        commit_result.unwrap();

        // Step 4: Verify dataset now has all 6 rows after successful commit
        let dataset_after_commit = Dataset::open(&dataset_uri).await.unwrap();
        let row_count_after = dataset_after_commit.count_rows(None).await.unwrap();
        assert_eq!(
            row_count_after, 6,
            "Dataset should have all 6 rows after commit"
        );

        // Verify data integrity
        let mut scanner = dataset_after_commit.scan();
        scanner.project(&["id", "value"]).unwrap();
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let all_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect();

        assert_eq!(
            all_ids,
            vec![1, 2, 3, 4, 5, 6],
            "All data should be correctly written"
        );
    }

    /// Returns the number of files in `<base_dir>/data/`.
    fn count_data_files(base_dir: &str) -> usize {
        let data_dir = std::path::Path::new(base_dir).join("data");
        if !data_dir.exists() {
            return 0;
        }
        std::fs::read_dir(data_dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_file())
            .count()
    }

    #[tokio::test]
    async fn test_cleanup_data_files_on_failed_write() {
        use lance_core::utils::tempfile::TempStrDir;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let (object_store, base_dir) =
            ObjectStore::from_uri_and_params(Default::default(), test_uri, &Default::default())
                .await
                .unwrap();

        let good_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Build a stream: one good batch, then an error.
        let items: Vec<std::result::Result<RecordBatch, DataFusionError>> = vec![
            Ok(good_batch.clone()),
            Err(DataFusionError::External("injected failure".into())),
        ];
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema.clone(),
            futures::stream::iter(items),
        ));

        let result = versions::write_fragments_direct(
            ConcreteFileVersion::V2_1,
            None,
            object_store.clone(),
            &base_dir,
            &schema,
            stream,
            WriteParams::default(),
            None,
            Vec::new(),
        )
        .await;

        assert!(result.is_err(), "Expected write to fail");
        assert_eq!(
            count_data_files(test_uri),
            0,
            "All partial data files should be cleaned up on failure"
        );
    }

    #[tokio::test]
    async fn test_cleanup_data_files_on_failed_write_multi_file() {
        // Verify cleanup when a failure occurs after one file has already been completed
        // (i.e., max_rows_per_file causes a file boundary before the error).
        use lance_core::utils::tempfile::TempStrDir;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let (object_store, base_dir) =
            ObjectStore::from_uri_and_params(Default::default(), test_uri, &Default::default())
                .await
                .unwrap();

        // 3 rows per file; 2 good batches of 3 rows (fills one file), then error.
        let good_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let items: Vec<std::result::Result<RecordBatch, DataFusionError>> = vec![
            Ok(good_batch.clone()),
            Ok(good_batch.clone()),
            Err(DataFusionError::External("injected failure".into())),
        ];
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema.clone(),
            futures::stream::iter(items),
        ));

        let result = versions::write_fragments_direct(
            ConcreteFileVersion::V2_1,
            None,
            object_store.clone(),
            &base_dir,
            &schema,
            stream,
            WriteParams {
                max_rows_per_file: 3,
                ..Default::default()
            },
            None,
            Vec::new(),
        )
        .await;

        assert!(result.is_err(), "Expected write to fail");
        assert_eq!(
            count_data_files(test_uri),
            0,
            "All data files (including completed ones) should be cleaned up on failure"
        );
    }

    /// Verifies the external-base branch in `cleanup_data_fragments`: files with
    /// `base_id == Some(_)` are skipped (logged but not deleted via the dataset's
    /// object store), while same-fragment files with `base_id == None` are deleted.
    #[tokio::test]
    async fn test_cleanup_data_fragments_skips_external_base() {
        use lance_core::utils::tempfile::TempStrDir;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (object_store, base_dir) =
            ObjectStore::from_uri_and_params(Default::default(), test_uri, &Default::default())
                .await
                .unwrap();

        // Create a real local data file we expect to be cleaned up.
        let data_dir = base_dir.clone().join(DATA_DIR);
        let local_filename = "local.lance";
        let local_path = data_dir.clone().join(local_filename);
        object_store.put(&local_path, b"x").await.unwrap();
        // Sanity check: file is on disk.
        assert_eq!(count_data_files(test_uri), 1);

        let mut external_file =
            DataFile::new_unstarted("external.lance", ConcreteFileVersion::V2_1);
        external_file.base_id = Some(42);
        let local_file = DataFile::new_unstarted(local_filename, ConcreteFileVersion::V2_1);
        let fragments = vec![Fragment {
            id: 0,
            files: vec![external_file, local_file],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(0),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        }];

        cleanup_data_fragments(&object_store, &base_dir, None, &fragments).await;

        // The local file should be removed; the external file is skipped without
        // erroring (its base store isn't known here).
        assert_eq!(
            count_data_files(test_uri),
            0,
            "Local data file should be deleted by cleanup"
        );
    }

    /// Verifies the target-base branch in `cleanup_data_fragments`: files whose
    /// `base_id` matches a provided [`TargetBaseInfo`] are deleted via that base's
    /// object store (respecting `is_dataset_root` layout), while files in bases
    /// without a provided store are still skipped.
    #[tokio::test]
    async fn test_cleanup_data_fragments_deletes_target_base_files() {
        use lance_core::utils::tempfile::TempStrDir;

        let primary_dir = TempStrDir::default();
        let base1_dir = TempStrDir::default();
        let base2_dir = TempStrDir::default();

        let (object_store, base_dir) = ObjectStore::from_uri_and_params(
            Default::default(),
            primary_dir.as_str(),
            &Default::default(),
        )
        .await
        .unwrap();
        let (base1_store, base1_path) = ObjectStore::from_uri_and_params(
            Default::default(),
            base1_dir.as_str(),
            &Default::default(),
        )
        .await
        .unwrap();
        let (base2_store, base2_path) = ObjectStore::from_uri_and_params(
            Default::default(),
            base2_dir.as_str(),
            &Default::default(),
        )
        .await
        .unwrap();

        // base2 is a plain data directory: files sit at its root, not under data/.
        let count_plain_files = |dir: &str| {
            std::fs::read_dir(dir)
                .map(|entries| {
                    entries
                        .filter(|e| e.as_ref().unwrap().path().is_file())
                        .count()
                })
                .unwrap_or(0)
        };

        // base1 is a dataset root (files under data/), base2 is a plain data dir.
        let base1_file_path = base1_path.clone().join(DATA_DIR).join("one.lance");
        base1_store.put(&base1_file_path, b"x").await.unwrap();
        let base2_file_path = base2_path.clone().join("two.lance");
        base2_store.put(&base2_file_path, b"x").await.unwrap();
        assert_eq!(count_data_files(base1_dir.as_str()), 1);
        assert_eq!(count_plain_files(base2_dir.as_str()), 1);

        let mut base1_file = DataFile::new_unstarted("one.lance", ConcreteFileVersion::V2_1);
        base1_file.base_id = Some(1);
        let mut base2_file = DataFile::new_unstarted("two.lance", ConcreteFileVersion::V2_1);
        base2_file.base_id = Some(2);
        let mut unknown_file = DataFile::new_unstarted("unknown.lance", ConcreteFileVersion::V2_1);
        unknown_file.base_id = Some(42);
        let fragments = vec![Fragment {
            id: 0,
            files: vec![base1_file, base2_file, unknown_file],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(0),
            created_at_version_meta: None,
            last_updated_at_version_meta: None,
        }];

        let target_bases = vec![
            TargetBaseInfo {
                base_id: 1,
                object_store: base1_store,
                base_dir: base1_path,
                is_dataset_root: true,
            },
            TargetBaseInfo {
                base_id: 2,
                object_store: base2_store,
                base_dir: base2_path,
                is_dataset_root: false,
            },
        ];

        cleanup_data_fragments(&object_store, &base_dir, Some(&target_bases), &fragments).await;

        assert_eq!(
            count_data_files(base1_dir.as_str()),
            0,
            "File in dataset-root target base should be deleted"
        );
        assert_eq!(
            count_plain_files(base2_dir.as_str()),
            0,
            "File in plain-directory target base should be deleted"
        );
    }

    #[tokio::test]
    async fn test_cleanup_routed_data_files_on_failed_write() {
        // Files already completed in target bases must be removed when the
        // write later fails.
        use lance_core::utils::tempfile::TempStrDir;

        let primary_dir = TempStrDir::default();
        let base1_dir = TempStrDir::default();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let (object_store, base_dir) = ObjectStore::from_uri_and_params(
            Default::default(),
            primary_dir.as_str(),
            &Default::default(),
        )
        .await
        .unwrap();
        let (base1_store, base1_path) = ObjectStore::from_uri_and_params(
            Default::default(),
            base1_dir.as_str(),
            &Default::default(),
        )
        .await
        .unwrap();

        let good_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // 3 rows per file: the first batch fills and completes a file in the
        // target base, then the stream fails.
        let items: Vec<std::result::Result<RecordBatch, DataFusionError>> = vec![
            Ok(good_batch.clone()),
            Ok(good_batch.clone()),
            Err(DataFusionError::External("injected failure".into())),
        ];
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema.clone(),
            futures::stream::iter(items),
        ));

        let target_bases = vec![TargetBaseInfo {
            base_id: 1,
            object_store: base1_store,
            base_dir: base1_path,
            is_dataset_root: true,
        }];

        let result = versions::write_fragments_direct(
            ConcreteFileVersion::V2_1,
            None,
            object_store.clone(),
            &base_dir,
            &schema,
            stream,
            WriteParams {
                max_rows_per_file: 3,
                ..Default::default()
            },
            Some(target_bases),
            vec![],
        )
        .await;

        assert!(result.is_err(), "Expected write to fail");
        assert_eq!(
            count_data_files(base1_dir.as_str()),
            0,
            "Data files routed to the target base should be cleaned up on failure"
        );
        assert_eq!(count_data_files(primary_dir.as_str()), 0);
    }

    /// PRIMARY_BASE_ID (0) and the dataset URI include primary storage in the
    /// target rotation, alongside registered bases.
    #[tokio::test]
    async fn test_multi_base_target_primary_and_bases() {
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        let test_uri = "memory://primary_slot_test";
        let primary_uri = format!("{}/primary", test_uri);
        let base1_uri = format!("{}/base1", test_uri);
        let base2_uri = format!("{}/base2", test_uri);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));

        // CREATE mode targeting primary + a new base: also verifies the id
        // assignment on initial_bases reaches the committed manifest.
        let dataset = Dataset::write(
            data_gen.batch(6),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                max_rows_per_file: 3,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        is_dataset_root: true,
                        path: base1_uri.clone(),
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        is_dataset_root: false,
                        path: base2_uri.clone(),
                    },
                ]),
                target_bases: Some(vec![PRIMARY_BASE_ID, 1]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        assert_eq!(dataset.manifest.base_paths.len(), 2);
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![None, Some(1)]);

        // APPEND across primary + both bases, one file per slot in order.
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen2.batch(9),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                max_rows_per_file: 3,
                target_bases: Some(vec![PRIMARY_BASE_ID, 1, 2]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .skip(2)
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![None, Some(1), Some(2)]);

        // Names variant: the dataset's own URI selects primary storage.
        let mut data_gen3 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen3.batch(6),
            Arc::new(dataset),
            Some(WriteParams {
                mode: WriteMode::Append,
                max_rows_per_file: 3,
                target_base_names_or_paths: Some(vec![primary_uri.clone(), "base2".to_string()]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .skip(5)
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![None, Some(2)]);

        assert_eq!(dataset.count_rows(None).await.unwrap(), 21);
    }

    /// `target_all_bases` resolves to every registered base at execution
    /// time, with primary storage as the first slot when included.
    #[tokio::test]
    async fn test_multi_base_target_all_bases() {
        use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

        let test_uri = "memory://all_bases_test";
        let primary_uri = format!("{}/primary", test_uri);
        let base1_uri = format!("{}/base1", test_uri);
        let base2_uri = format!("{}/base2", test_uri);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen.batch(3),
            &primary_uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                initial_bases: Some(vec![
                    BasePath {
                        id: 1,
                        name: Some("base1".to_string()),
                        is_dataset_root: true,
                        path: base1_uri.clone(),
                    },
                    BasePath {
                        id: 2,
                        name: Some("base2".to_string()),
                        is_dataset_root: false,
                        path: base2_uri.clone(),
                    },
                ]),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // All bases including primary: slots are [primary, base1, base2].
        let mut data_gen2 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen2.batch(9),
            Arc::new(dataset),
            Some(
                WriteParams {
                    mode: WriteMode::Append,
                    max_rows_per_file: 3,
                    ..Default::default()
                }
                .with_target_all_bases(true),
            ),
        )
        .await
        .unwrap();
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .skip(1)
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![None, Some(1), Some(2)]);

        // Without primary: slots are [base1, base2].
        let mut data_gen3 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen3.batch(6),
            Arc::new(dataset),
            Some(
                WriteParams {
                    mode: WriteMode::Append,
                    max_rows_per_file: 3,
                    ..Default::default()
                }
                .with_target_all_bases(false),
            ),
        )
        .await
        .unwrap();
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .skip(4)
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![Some(1), Some(2)]);

        // Cannot be combined with explicit target bases.
        let mut data_gen4 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let result = Dataset::write(
            data_gen4.batch(3),
            Arc::new(dataset),
            Some(
                WriteParams {
                    mode: WriteMode::Append,
                    target_bases: Some(vec![1]),
                    ..Default::default()
                }
                .with_target_all_bases(true),
            ),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot specify target_all_bases together with")
        );

        // On a dataset with no registered bases: include_primary=true is a
        // no-op rotation over primary, false is rejected.
        let plain_uri = "memory://all_bases_plain";
        let mut data_gen5 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let plain = Dataset::write(data_gen5.batch(3), plain_uri, None)
            .await
            .unwrap();
        let mut data_gen6 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let plain = Dataset::write(
            data_gen6.batch(3),
            Arc::new(plain),
            Some(
                WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                }
                .with_target_all_bases(true),
            ),
        )
        .await
        .unwrap();
        assert!(
            plain.get_fragments().iter().all(|f| f
                .metadata
                .files
                .iter()
                .all(|file| file.base_id.is_none()))
        );
        let mut data_gen7 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let result = Dataset::write(
            data_gen7.batch(3),
            Arc::new(plain),
            Some(
                WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                }
                .with_target_all_bases(false),
            ),
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("target_all_bases found no registered bases")
        );

        // CREATE mode: initial_bases join the rotation before their ids are
        // committed to a manifest.
        let create_uri = "memory://all_bases_create";
        let mut data_gen8 =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("id".to_owned())));
        let dataset = Dataset::write(
            data_gen8.batch(9),
            create_uri,
            Some(
                WriteParams {
                    mode: WriteMode::Create,
                    max_rows_per_file: 3,
                    initial_bases: Some(vec![
                        BasePath {
                            id: 0,
                            name: Some("base1".to_string()),
                            is_dataset_root: true,
                            path: format!("{}/base1", create_uri),
                        },
                        BasePath {
                            id: 0,
                            name: Some("base2".to_string()),
                            is_dataset_root: false,
                            path: format!("{}/base2", create_uri),
                        },
                    ]),
                    ..Default::default()
                }
                .with_target_all_bases(true),
            ),
        )
        .await
        .unwrap();
        assert_eq!(dataset.manifest.base_paths.len(), 2);
        let file_bases: Vec<_> = dataset
            .get_fragments()
            .iter()
            .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
            .collect();
        assert_eq!(file_bases, vec![None, Some(1), Some(2)]);
    }

    #[tokio::test]
    async fn test_zone_map_seeds_used_during_update() {
        use crate::Dataset;
        use crate::index::DatasetIndexExt;
        use crate::index::scalar::open_scalar_index;
        use arrow::datatypes::Int32Type;
        use lance_datagen::{BatchCount, RowCount};
        use lance_datagen::{array, gen_batch};
        use lance_file::reader::FileReaderOptions;
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::scalar::seed::SEED_META_KEY_PREFIX;
        use lance_index::{IndexType, scalar::ScalarIndexParams};
        use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
        use lance_io::utils::CachedFileSize;

        let tmpdir = lance_core::utils::tempfile::TempStrDir::default();
        let uri = tmpdir.as_str();

        // Step 1: Create initial dataset
        let reader = gen_batch()
            .col("val", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, uri, None).await.unwrap();

        // Step 2: Create a zone map index with seeds explicitly enabled (Int32 defaults to off).
        let params = ScalarIndexParams::for_builtin(lance_index::scalar::BuiltinIndexType::ZoneMap)
            .with_params(&serde_json::json!({"use_seeds": true}));
        dataset
            .create_index(&["val"], IndexType::ZoneMap, None, &params, false)
            .await
            .unwrap();
        // Step 3: Append new data - seeds should be written automatically
        let reader = gen_batch()
            .col("val", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(50), BatchCount::from(1));
        let dataset = Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_1),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Step 4: Verify that the newly appended fragment has a seed embedded
        let fragments = dataset.fragments();
        let new_fragment = fragments.last().unwrap();
        let data_file = new_fragment.files.first().unwrap();

        let scheduler = ScanScheduler::new(
            dataset.object_store.clone(),
            SchedulerConfig::max_bandwidth(&dataset.object_store),
        );
        let path = dataset
            .base
            .clone()
            .join(super::DATA_DIR)
            .join(data_file.path.as_str());
        let file_scheduler = scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let reader = lance_file::reader::FileReader::try_open(
            file_scheduler,
            None,
            Default::default(),
            &dataset.metadata_cache.file_metadata_cache(&path),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let meta_key = format!("{}val", SEED_META_KEY_PREFIX);
        let has_seed = reader
            .metadata()
            .file_schema
            .metadata
            .contains_key(&meta_key);
        assert!(
            has_seed,
            "Newly appended fragment should have a zone map seed in metadata"
        );

        // Step 5: Optimize the index (should use seeds)
        let mut dataset = Dataset::open(uri).await.unwrap();
        dataset.optimize_indices(&Default::default()).await.unwrap();

        // Step 6: Query the updated index to verify it's correct
        let dataset = Dataset::open(uri).await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert!(
            !indices.is_empty(),
            "Dataset should still have an index after optimization"
        );

        // Verify the index is a ZoneMap and covers all fragments
        let index = indices.iter().find(|i| i.name.contains("val")).unwrap();
        let scalar_index = open_scalar_index(&dataset, "val", index, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            scalar_index.index_type(),
            IndexType::ZoneMap,
            "Index should still be a ZoneMap after optimization"
        );
        let frags = scalar_index.calculate_included_frags().await.unwrap();
        assert_eq!(frags.len(), 2, "Index should cover both fragments");
    }
}
