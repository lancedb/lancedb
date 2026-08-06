// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Trait for commit implementations.
//!
//! In Lance, a transaction is committed by writing the next manifest file.
//! However, care should be taken to ensure that the manifest file is written
//! only once, even if there are concurrent writers. Different stores have
//! different abilities to handle concurrent writes, so a trait is provided
//! to allow for different implementations.
//!
//! The trait [`CommitHandler`] can be implemented to provide different commit
//! strategies. The default implementation for most object stores is
//! `ConditionalPutCommitHandler`, which writes the manifest to a temporary path, then
//! renames the temporary path to the final path if no object already exists
//! at the final path.
//!
//! When providing your own commit handler, most often you are implementing in
//! terms of a lock. The trait `CommitLock` can be implemented as a simpler
//! alternative to [`CommitHandler`].

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::num::NonZero;
use std::sync::Arc;
use std::time::{Duration, Instant};

use conflict_resolver::TransactionRebase;
use lance_core::utils::backoff::{Backoff, SlotBackoff};
use lance_core::utils::tracing::{AUDIT_MODE_DELETE, AUDIT_TYPE_TRANSACTION, TRACE_FILE_AUDIT};
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_index::metrics::NoOpMetricsCollector;
use lance_io::utils::CachedFileSize;
use lance_select::RowAddrTreeMap;
use lance_table::format::{
    DETACHED_VERSION_MASK, DataStorageFormat, DeletionFile, Fragment, IndexMetadata, Manifest,
    WriterVersion, is_detached_version, list_index_files_with_sizes, pb,
};
use lance_table::io::commit::{
    CommitConfig, CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme,
};
use lance_table::io::manifest::read_manifest;
use rand::{Rng, rng};

use super::ObjectStore;
use crate::Dataset;
use crate::dataset::cleanup::auto_cleanup_hook;
use crate::dataset::fragment::FileFragment;
use crate::dataset::transaction::{Operation, Transaction};
use crate::dataset::{
    ManifestWriteConfig, NewTransactionResult, TRANSACTIONS_DIR, load_new_transactions,
    write_manifest_file,
};
use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;
use crate::index::vector::details::infer_missing_vector_details;
use crate::io::deletion::read_dataset_deletion_file;
use crate::session::Session;
use crate::session::caches::DSMetadataCache;
use crate::session::index_caches::IndexMetadataKey;
use futures::future::Either;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use lance_core::{Error, Result};
use lance_index::is_system_index;
use lance_io::object_store::ObjectStoreRegistry;
use log;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use prost::Message;

pub mod conflict_resolver;
#[cfg(all(feature = "dynamodb_tests", test))]
mod dynamodb;
#[cfg(test)]
mod external_manifest;
pub mod namespace_manifest;
#[cfg(all(feature = "dynamodb_tests", test))]
mod s3_test;

/// Wall-clock budget for conflict retry backoff when callers do not override it.
pub(crate) const DEFAULT_COMMIT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn timeout_error(retry_timeout: Duration, attempts: u32) -> Error {
    Error::too_much_write_contention(format!(
        "Attempted {} times, but failed on retry_timeout of {:.3} seconds.",
        attempts,
        retry_timeout.as_secs_f32()
    ))
}

pub(crate) fn maybe_timeout<T>(
    attempt: u32,
    start: Instant,
    retry_timeout: Duration,
    future: impl Future<Output = T>,
) -> impl Future<Output = Result<T>> {
    if attempt == 0 {
        // The first attempt establishes the observed latency used by SlotBackoff.
        Either::Left(future.map(Ok))
    } else {
        let remaining = retry_timeout.saturating_sub(start.elapsed());
        Either::Right(
            tokio::time::timeout(remaining, future)
                .map_err(move |_| timeout_error(retry_timeout, attempt + 1)),
        )
    }
}

/// Read the transaction data from a transaction file.
pub(crate) async fn read_transaction_file(
    object_store: &ObjectStore,
    base_path: &Path,
    transaction_file: &str,
) -> Result<Transaction> {
    let path = base_path
        .clone()
        .join(TRANSACTIONS_DIR)
        .join(transaction_file);
    let result = object_store.inner.get(&path).await?;
    let data = result.bytes().await?;
    let transaction = pb::Transaction::decode(data)?;
    transaction.try_into()
}

/// Best-effort delete of a transaction file that is no longer needed.
///
/// Logs a warning on failure rather than propagating the error, since the
/// primary operation has already failed and the orphaned file will eventually
/// be removed by GC.
///
/// Callers must only invoke this for attempts whose commit is confirmed to
/// have NOT landed (see [`verify_commit_outcome`]): a landed manifest
/// references its transaction file by path, so deleting it would corrupt the
/// version.
async fn cleanup_transaction_file(
    object_store: &ObjectStore,
    base_path: &Path,
    transaction_file: &str,
) {
    if transaction_file.is_empty() {
        return;
    }
    let path = base_path
        .clone()
        .join(TRANSACTIONS_DIR)
        .join(transaction_file);
    match object_store.delete(&path).await {
        Ok(()) => {
            tracing::info!(
                target: TRACE_FILE_AUDIT,
                mode = AUDIT_MODE_DELETE,
                r#type = AUDIT_TYPE_TRANSACTION,
                path = transaction_file,
            );
        }
        Err(e) => {
            log::warn!(
                "Failed to clean up orphaned transaction file '{}': {}",
                transaction_file,
                e
            );
        }
    }
}

/// Who owns the manifest at a version, checked after a failed commit attempt.
#[derive(Debug)]
enum CommitOutcome {
    /// The manifest at the version is the one this attempt wrote: the commit
    /// actually landed even though the store reported a failure (e.g. the
    /// response to a successful conditional PUT was lost and an internal
    /// retry surfaced "already exists").
    Ours {
        manifest: Box<Manifest>,
        location: ManifestLocation,
    },
    /// A manifest exists at the version and records a different transaction
    /// file: another writer definitely won the version.
    Foreign,
    /// No manifest exists at the version: this attempt definitely did not
    /// land.
    Absent,
    /// The verification reads themselves kept failing; whether the commit
    /// landed cannot be determined. Callers must not run destructive cleanup
    /// in this state.
    Unknown,
}

/// Maximum verification read attempts in [`verify_commit_outcome`].
const COMMIT_VERIFICATION_ATTEMPTS: u32 = 3;

/// Determine whether a failed commit attempt actually landed, by comparing
/// the complete transaction recorded in the manifest at `version` with this
/// attempt's transaction.
///
/// Never returns an error. Read failures and non-definitive not-found results
/// are retried briefly, then collapse to [`CommitOutcome::Unknown`].
async fn verify_commit_outcome(
    object_store: &ObjectStore,
    commit_handler: &dyn CommitHandler,
    base_path: &Path,
    version: u64,
    transaction: &Transaction,
) -> CommitOutcome {
    enum VerificationFailure {
        NotFound,
        Read(Error),
    }

    let mut backoff = Backoff::default();
    let failure = loop {
        let failure = match try_read_manifest_at(object_store, commit_handler, base_path, version)
            .await
        {
            Ok(Some((manifest, location))) => {
                match read_manifest_transaction(object_store, base_path, &manifest, &location).await
                {
                    Ok(Some(committed_transaction)) => {
                        return if committed_transaction == *transaction {
                            CommitOutcome::Ours {
                                manifest: Box::new(manifest),
                                location,
                            }
                        } else {
                            CommitOutcome::Foreign
                        };
                    }
                    Ok(None) => return CommitOutcome::Foreign,
                    Err(error) => VerificationFailure::Read(error),
                }
            }
            Ok(None) if commit_handler.is_version_not_found_definitive() => {
                return CommitOutcome::Absent;
            }
            Ok(None) => VerificationFailure::NotFound,
            Err(error) => VerificationFailure::Read(error),
        };

        if backoff.attempt() + 1 >= COMMIT_VERIFICATION_ATTEMPTS {
            break failure;
        }
        tokio::time::sleep(backoff.next_backoff()).await;
    };

    match failure {
        VerificationFailure::NotFound => {
            log::warn!(
                "The manifest for version {} was not visible after {} commit verification \
                 attempts, and the commit handler does not guarantee definitive not-found \
                 results; treating the commit status as unknown",
                version,
                COMMIT_VERIFICATION_ATTEMPTS
            );
            CommitOutcome::Unknown
        }
        VerificationFailure::Read(error) => {
            log::warn!(
                "Could not verify the outcome of the commit attempt for version {} after {} \
                 tries; treating the commit status as unknown: {}",
                version,
                COMMIT_VERIFICATION_ATTEMPTS,
                error
            );
            CommitOutcome::Unknown
        }
    }
}

async fn read_manifest_transaction(
    object_store: &ObjectStore,
    base_path: &Path,
    manifest: &Manifest,
    location: &ManifestLocation,
) -> Result<Option<Transaction>> {
    if let Some(position) = manifest.transaction_section {
        let reader = if let Some(size) = location.size {
            object_store
                .open_with_size(&location.path, size as usize)
                .await?
        } else {
            object_store.open(&location.path).await?
        };
        let transaction: pb::Transaction =
            lance_io::utils::read_message(reader.as_ref(), position).await?;
        Transaction::try_from(transaction).map(Some)
    } else if let Some(transaction_file) = manifest.transaction_file.as_deref() {
        read_transaction_file(object_store, base_path, transaction_file)
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}

/// Read the manifest at `version`, distinguishing "no such version"
/// (`Ok(None)`) from transient read failures (`Err`).
async fn try_read_manifest_at(
    object_store: &ObjectStore,
    commit_handler: &dyn CommitHandler,
    base_path: &Path,
    version: u64,
) -> Result<Option<(Manifest, ManifestLocation)>> {
    let location = match commit_handler
        .resolve_version_location(base_path, version, &object_store.inner)
        .await
    {
        Ok(location) => location,
        Err(Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };
    match read_manifest(object_store, &location.path, location.size).await {
        Ok(manifest) => Ok(Some((manifest, location))),
        Err(Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Write a transaction to a file and return the relative path.
pub(crate) async fn write_transaction_file(
    object_store: &ObjectStore,
    base_path: &Path,
    transaction: &pb::Transaction,
) -> Result<String> {
    let file_name = format!("{}-{}.txn", transaction.read_version, transaction.uuid);
    let path = base_path
        .clone()
        .join(TRANSACTIONS_DIR)
        .join(file_name.as_str());

    let buf = transaction.encode_to_vec();
    object_store.put(&path, &buf).await?;

    Ok(file_name)
}

/// Transactions serialized above this size are not inlined into the manifest.
#[cfg(not(test))]
pub(crate) const MAX_INLINE_TRANSACTION_BYTES: usize = 20 * 1024 * 1024;
/// Smaller threshold for unit tests so spill coverage does not need
/// multi-megabyte payloads.
#[cfg(test)]
pub(crate) const MAX_INLINE_TRANSACTION_BYTES: usize = 64 * 1024;

#[allow(clippy::too_many_arguments)]
async fn do_commit_new_dataset(
    object_store: &ObjectStore,
    source_store: Option<&ObjectStore>,
    commit_handler: &dyn CommitHandler,
    base_path: &Path,
    transaction: &Transaction,
    write_config: &ManifestWriteConfig,
    manifest_naming_scheme: ManifestNamingScheme,
    metadata_cache: &DSMetadataCache,
    store_registry: Arc<ObjectStoreRegistry>,
) -> Result<(Manifest, ManifestLocation)> {
    let pb_transaction = pb::Transaction::from(transaction);
    let inline_transaction = pb_transaction.encoded_len() <= MAX_INLINE_TRANSACTION_BYTES;

    let transaction_file = if !write_config.disable_transaction_file() {
        write_transaction_file(object_store, base_path, &pb_transaction).await?
    } else {
        String::new()
    };

    let (mut manifest, indices) = if let Operation::Clone {
        is_shallow,
        ref_name,
        ref_version,
        ref_path,
        branch_name,
        ..
    } = &transaction.operation
    {
        // The source manifest must be read through the source store, which may differ
        // from the destination store when cloning across object stores/accounts. Falls
        // back to the destination store for same-store clones.
        let source_store = source_store.unwrap_or(object_store);
        let source_base_path =
            ObjectStore::extract_path_from_uri(store_registry, ref_path.as_str())?;
        let source_manifest_location = commit_handler
            .resolve_version_location(&source_base_path, *ref_version, &source_store.inner)
            .await?;
        let source_manifest = Dataset::load_manifest(
            source_store,
            &source_manifest_location,
            ref_path.as_str(),
            &Session::default(),
        )
        .await?;

        if *is_shallow {
            let new_base_id = source_manifest
                .base_paths
                .keys()
                .max()
                .map(|id| *id + 1)
                .unwrap_or(0);
            let new_manifest = source_manifest.shallow_clone(
                ref_name.clone(),
                ref_path.clone(),
                new_base_id,
                branch_name.clone(),
                transaction_file.clone(),
            );

            let updated_indices = if let Some(index_section_pos) = source_manifest.index_section {
                let reader = source_store.open(&source_manifest_location.path).await?;
                let section: pb::IndexSection =
                    lance_io::utils::read_message(reader.as_ref(), index_section_pos).await?;
                section
                    .indices
                    .into_iter()
                    .map(|index_pb| {
                        let mut index = IndexMetadata::try_from(index_pb)?;
                        index.base_id = Some(new_base_id);
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                vec![]
            };
            (new_manifest, updated_indices)
        } else {
            // Deep clone: build a manifest that references local files (no external bases)
            let mut new_manifest = source_manifest.clone();
            new_manifest.base_paths.clear();
            new_manifest.branch = None;
            new_manifest.tag = None;
            new_manifest.index_section = None; // will be rewritten below
            new_manifest.transaction_file =
                (!transaction_file.is_empty()).then_some(transaction_file.clone());
            let mut new_frags = new_manifest.fragments.as_ref().clone();
            for f in &mut new_frags {
                for df in &mut f.files {
                    df.base_id = None;
                }
                if let Some(d) = f.deletion_file.as_mut() {
                    d.base_id = None;
                }
            }
            new_manifest.fragments = Arc::new(new_frags);

            // Indices: keep metadata but normalize base to local
            let mut updated_indices = Vec::new();
            if let Some(index_section_pos) = source_manifest.index_section {
                let reader = source_store.open(&source_manifest_location.path).await?;
                let section: pb::IndexSection =
                    lance_io::utils::read_message(reader.as_ref(), index_section_pos).await?;
                updated_indices = section
                    .indices
                    .into_iter()
                    .map(|index_pb| {
                        let mut index = IndexMetadata::try_from(index_pb)?;
                        index.base_id = None;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>>>()?;
            }
            (new_manifest, updated_indices)
        }
    } else {
        let (manifest, indices) =
            transaction.build_manifest(None, vec![], &transaction_file, write_config)?;
        (manifest, indices)
    };

    let result = write_manifest_file(
        object_store,
        commit_handler,
        base_path,
        &mut manifest,
        if indices.is_empty() {
            None
        } else {
            Some(indices.clone())
        },
        write_config,
        manifest_naming_scheme,
        inline_transaction.then(|| pb_transaction.into()),
    )
    .await;

    // TODO: Allow Append or Overwrite mode to retry using `commit_transaction`
    // if there is a conflict.
    match result {
        Ok(manifest_location) => {
            record_new_dataset_commit(metadata_cache, transaction, &manifest, &manifest_location)
                .await;
            Ok((manifest, manifest_location))
        }
        Err(CommitError::CommitConflict) => {
            // The dataset may "already exist" because this attempt's own
            // manifest write landed but returned an ambiguous error. Verify
            // before reporting a conflict (and before deleting the
            // transaction file a landed manifest would reference).
            match verify_commit_outcome(
                object_store,
                commit_handler,
                base_path,
                manifest.version,
                transaction,
            )
            .await
            {
                CommitOutcome::Ours {
                    manifest: committed_manifest,
                    location,
                } => {
                    let committed_manifest = *committed_manifest;
                    record_new_dataset_commit(
                        metadata_cache,
                        transaction,
                        &committed_manifest,
                        &location,
                    )
                    .await;
                    return Ok((committed_manifest, location));
                }
                CommitOutcome::Foreign | CommitOutcome::Absent => {}
                CommitOutcome::Unknown => {
                    return Err(Error::commit_status_unknown_source(
                        manifest.version,
                        "dataset creation reported a conflict but the manifest could not \
                         be read back for verification"
                            .to_string()
                            .into(),
                    ));
                }
            }
            cleanup_transaction_file(object_store, base_path, &transaction_file).await;
            Err(crate::Error::dataset_already_exists(base_path.to_string()))
        }
        Err(CommitError::OtherError(err)) => {
            match verify_commit_outcome(
                object_store,
                commit_handler,
                base_path,
                manifest.version,
                transaction,
            )
            .await
            {
                CommitOutcome::Ours {
                    manifest: committed_manifest,
                    location,
                } => {
                    let committed_manifest = *committed_manifest;
                    record_new_dataset_commit(
                        metadata_cache,
                        transaction,
                        &committed_manifest,
                        &location,
                    )
                    .await;
                    if commit_handler.propagate_commit_error_after_success() {
                        return Err(err);
                    }
                    return Ok((committed_manifest, location));
                }
                CommitOutcome::Foreign | CommitOutcome::Absent => {}
                CommitOutcome::Unknown => {
                    return Err(Error::commit_status_unknown_source(
                        manifest.version,
                        Box::new(err),
                    ));
                }
            }
            cleanup_transaction_file(object_store, base_path, &transaction_file).await;
            Err(err)
        }
    }
}

/// Cache bookkeeping for a successful new-dataset commit, shared by the
/// direct-success and verified-own-commit paths of `do_commit_new_dataset`.
async fn record_new_dataset_commit(
    metadata_cache: &DSMetadataCache,
    transaction: &Transaction,
    manifest: &Manifest,
    location: &ManifestLocation,
) {
    let tx_key = crate::session::caches::TransactionKey {
        version: manifest.version,
    };
    metadata_cache
        .insert_with_key(&tx_key, Arc::new(transaction.clone()))
        .await;

    let manifest_key = crate::session::caches::ManifestKey {
        version: location.version,
        e_tag: location.e_tag.as_deref(),
    };
    metadata_cache
        .insert_with_key(&manifest_key, Arc::new(manifest.clone()))
        .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_new_dataset(
    object_store: &ObjectStore,
    source_store: Option<&ObjectStore>,
    commit_handler: &dyn CommitHandler,
    base_path: &Path,
    transaction: &Transaction,
    write_config: &ManifestWriteConfig,
    manifest_naming_scheme: ManifestNamingScheme,
    metadata_cache: &crate::session::caches::DSMetadataCache,
    store_registry: Arc<ObjectStoreRegistry>,
) -> Result<(Manifest, ManifestLocation)> {
    do_commit_new_dataset(
        object_store,
        source_store,
        commit_handler,
        base_path,
        transaction,
        write_config,
        manifest_naming_scheme,
        metadata_cache,
        store_registry,
    )
    .await
}

/// Internal function to check if a manifest could use some migration.
///
/// Manifest migrations happen on each write, but sometimes we need to run them
/// before certain new operations. An easy way to force a migration is to run
/// `dataset.delete(false)`, which won't modify data but will cause a migration.
/// However, you don't want to always have to do this, so we provide this method
/// to check if a migration is needed.
pub fn manifest_needs_migration(manifest: &Manifest, indices: &[IndexMetadata]) -> bool {
    manifest.writer_version.is_none()
        || manifest.fragments.iter().any(|f| {
            f.physical_rows.is_none()
                || (f
                    .deletion_file
                    .as_ref()
                    .map(|d| d.num_deleted_rows.is_none())
                    .unwrap_or(false))
        })
        || indices
            .iter()
            .any(|i| must_recalculate_fragment_bitmap(i, manifest.writer_version.as_ref()))
}

/// Update manifest with new metadata fields.
///
/// Fields such as `physical_rows` and `num_deleted_rows` may not have been
/// in older datasets. To bring these old manifests up-to-date, we add them here.
async fn migrate_manifest(
    dataset: &Dataset,
    manifest: &mut Manifest,
    recompute_stats: bool,
) -> Result<()> {
    if !recompute_stats
        && manifest.fragments.iter().all(|f| {
            f.num_rows().map(|n| n > 0).unwrap_or(false)
                && f.files.iter().all(|f| f.file_size_bytes.get().is_some())
        })
    {
        return Ok(());
    }

    manifest.fragments =
        Arc::new(migrate_fragments(dataset, &manifest.fragments, recompute_stats).await?);

    Ok(())
}

fn check_storage_version(manifest: &mut Manifest) -> Result<()> {
    let data_storage_version = manifest.data_storage_format.lance_file_format();
    if data_storage_version == ConcreteFileVersion::V1 {
        // Due to bugs in 0.16 it is possible the dataset's data storage version does not
        // match the file version.  As a result, we need to check and see if they are out
        // of sync.
        if let Some(actual_file_version) =
            Fragment::try_infer_version(&manifest.fragments).map_err(|e| Error::internal(format!(
                "The dataset contains a mixture of file versions.  You will need to rollback to an earlier version: {}",
                e
            )))?
                && actual_file_version != ConcreteFileVersion::V1 {
                    log::warn!(
                        "Data storage version {} is less than the actual file version {}.  This has been automatically updated.",
                        data_storage_version,
                        actual_file_version
                    );
                    manifest.data_storage_format = DataStorageFormat::new(actual_file_version);
                }
    } else {
        // Otherwise, if we are on 2.0 or greater, we should ensure that the file versions
        // match the data storage version.  This is a sanity assertion to prevent data corruption.
        if let Some(actual_file_version) = Fragment::try_infer_version(&manifest.fragments)?
            && actual_file_version != data_storage_version
        {
            return Err(Error::internal(format!(
                "The operation added files with version {}.  However, the data storage version is {}.",
                actual_file_version, data_storage_version
            )));
        }
    }
    Ok(())
}

/// Reject a manifest in which two fragments share an id. Per-fragment state is
/// keyed by fragment id — deletion file paths, cached row id sequences, row
/// addresses — so a duplicate makes it ambiguous which rows that state describes.
///
/// Runs after the legacy fixups above, so a dataset that needs a rollback for some
/// other reason is diagnosed with that first. Relies on `build_manifest` leaving
/// the fragments sorted by id.
fn check_fragment_ids(manifest: &Manifest) -> Result<()> {
    if let Some(pair) = manifest.fragments.windows(2).find(|p| p[0].id == p[1].id) {
        return Err(Error::invalid_input(format!(
            "The commit would produce two fragments with id {}. Fragment ids must be \
             unique. Datasets written by Lance 0.16 and earlier may already contain \
             duplicate ids; those have to be rewritten, or rolled back to a version \
             without the duplicate.",
            pair[0].id
        )));
    }
    Ok(())
}

fn check_column_indices(manifest: &Manifest) -> Result<()> {
    let data_storage_version = manifest.data_storage_format.lance_file_version()?;
    if data_storage_version < LanceFileVersion::V2_1 {
        return Ok(());
    }

    for fragment in manifest.fragments.iter() {
        for data_file in &fragment.files {
            if data_file.is_legacy_file() || data_file.column_indices.is_empty() {
                continue;
            }
            if data_file.fields.len() != data_file.column_indices.len() {
                return Err(Error::invalid_input(format!(
                    "Data file '{}' (fragment {}) has {} field ids but {} column indices. \
                     These must be the same length.",
                    data_file.path,
                    fragment.id,
                    data_file.fields.len(),
                    data_file.column_indices.len()
                )));
            }
            let file_version: LanceFileVersion = data_file.file_version()?.into();
            if file_version < LanceFileVersion::V2_1 {
                continue;
            }
            for (field_id, column_index) in
                data_file.fields.iter().zip(data_file.column_indices.iter())
            {
                // Field ids may not exist in the current schema after schema
                // evolution (e.g. cast/drop column). Skip those.
                let Some(field) = manifest.schema.field_by_id(*field_id) else {
                    continue;
                };
                let needs_column = field.is_leaf() || field.is_packed_struct() || field.is_blob();
                if needs_column && *column_index == -1 {
                    return Err(Error::invalid_input(format!(
                        "Field '{}' (id={}) in data file '{}' (fragment {}) \
                         has column_index=-1, but leaf fields, packed structs, \
                         and blob fields must have a valid column index in \
                         file format 2.1+.",
                        field.name, field_id, data_file.path, fragment.id
                    )));
                }
                if !needs_column && *column_index != -1 {
                    return Err(Error::invalid_input(format!(
                        "Non-leaf field '{}' (id={}) in data file '{}' (fragment {}) \
                         has column_index={}, but non-leaf fields should have \
                         column_index=-1 in file format 2.1+. Only leaf fields, \
                         packed structs, and blob fields should have column indices.",
                        field.name, field_id, data_file.path, fragment.id, column_index
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Fix schema in case of duplicate field ids.
///
/// See test dataset v0.10.5/corrupt_schema
fn fix_schema(manifest: &mut Manifest) -> Result<()> {
    // We can short-circuit if there is only one file per fragment or no fragments.
    if manifest.fragments.iter().all(|f| f.files.len() <= 1) {
        return Ok(());
    }

    // First, see which, if any fields have duplicate ids, within any fragment.
    let mut fields_with_duplicate_ids = HashSet::new();
    let mut seen_fields = HashSet::new();
    for fragment in manifest.fragments.iter() {
        for file in fragment.files.iter() {
            for field_id in file.fields.iter() {
                if *field_id >= 0 && !seen_fields.insert(*field_id) {
                    fields_with_duplicate_ids.insert(*field_id);
                }
            }
        }
        seen_fields.clear();
    }
    if fields_with_duplicate_ids.is_empty() {
        return Ok(());
    }

    // Now, we need to remap the field ids to be unique.
    let mut old_field_id_mapping: HashMap<i32, i32> = HashMap::new();
    let mut fields_with_duplicate_ids = fields_with_duplicate_ids.into_iter().collect::<Vec<_>>();
    fields_with_duplicate_ids.sort_unstable();
    for (field_id_seed, field_id) in (manifest.max_field_id() + 1..).zip(fields_with_duplicate_ids)
    {
        old_field_id_mapping.insert(field_id, field_id_seed);
    }

    let mut fragments = manifest.fragments.as_ref().clone();

    // Apply mapping to fragment files list
    // We iterate over files in reverse order so that we only map the last field id
    seen_fields.clear();
    for fragment in fragments.iter_mut() {
        for file in fragment.files.iter_mut().rev() {
            let new_fields: Arc<[i32]> = file
                .fields
                .iter()
                .map(|field_id| {
                    if let Some(new_field_id) = old_field_id_mapping.get(field_id)
                        && seen_fields.insert(*field_id)
                    {
                        *new_field_id
                    } else {
                        *field_id
                    }
                })
                .collect::<Vec<_>>()
                .into();
            file.fields = new_fields;
        }
        seen_fields.clear();
    }

    // Apply mapping to the schema
    for (old_field_id, new_field_id) in &old_field_id_mapping {
        let field = manifest.schema.mut_field_by_id(*old_field_id).unwrap();
        field.id = *new_field_id;
    }

    // Drop data files that are no longer in use.
    let remaining_field_ids = manifest
        .schema
        .fields_pre_order()
        .map(|f| f.id)
        .collect::<HashSet<_>>();
    for fragment in fragments.iter_mut() {
        fragment.files.retain(|file| {
            file.fields
                .iter()
                .any(|field_id| remaining_field_ids.contains(field_id))
        });
    }

    manifest.fragments = Arc::new(fragments);

    Ok(())
}

/// Get updated vector of fragments that has `physical_rows` and `num_deleted_rows`
/// filled in. This is no-op for newer tables, but may do IO for tables written
/// with older versions of Lance.
pub(crate) async fn migrate_fragments(
    dataset: &Dataset,
    fragments: &[Fragment],
    recompute_stats: bool,
) -> Result<Vec<Fragment>> {
    let dataset = Arc::new(dataset.clone());
    let new_fragments = futures::stream::iter(fragments)
        .map(|fragment| async {
            let physical_rows = if recompute_stats {
                None
            } else {
                fragment.physical_rows
            };
            let physical_rows = if let Some(physical_rows) = physical_rows {
                Either::Right(futures::future::ready(Ok(physical_rows)))
            } else {
                let file_fragment = FileFragment::new(dataset.clone(), fragment.clone());
                Either::Left(async move { file_fragment.physical_rows().await })
            };
            let num_deleted_rows = match &fragment.deletion_file {
                None => Either::Left(futures::future::ready(Ok(None))),
                Some(DeletionFile {
                    num_deleted_rows: Some(deleted_rows),
                    ..
                }) if !recompute_stats => {
                    Either::Left(futures::future::ready(Ok(Some(*deleted_rows))))
                }
                Some(deletion_file) => Either::Right(async {
                    let deletion_vector =
                        read_dataset_deletion_file(dataset.as_ref(), fragment.id, deletion_file)
                            .await?;
                    Ok(Some(deletion_vector.len()))
                }),
            };

            let (physical_rows, num_deleted_rows) =
                futures::future::try_join(physical_rows, num_deleted_rows).await?;

            let mut data_files = fragment.files.clone();

            // For each of the data files in the fragment, we need to get the file size.
            // Resolve each file against its own storage base: multi-base datasets
            // keep data files outside the dataset root (DataFile.base_id).
            let get_sizes = data_files
                .iter()
                .map(|file| {
                    if let Some(size) = file.file_size_bytes.get() {
                        Either::Left(futures::future::ready(Ok(size)))
                    } else {
                        let dataset = dataset.clone();
                        Either::Right(async move {
                            let object_store = dataset.object_store_for_data_file(file).await?;
                            let data_dir = dataset.data_file_dir_for_base(file.base_id)?;
                            object_store
                                .size(&data_dir.join(file.path.clone()))
                                .map_ok(|size| {
                                    NonZero::new(size).ok_or_else(|| {
                                        Error::internal(format!("File {} has size 0", file.path))
                                    })
                                })
                                .await?
                        })
                    }
                })
                .collect::<Vec<_>>();
            let sizes = futures::future::try_join_all(get_sizes).await?;
            data_files.iter_mut().zip(sizes).for_each(|(file, size)| {
                file.file_size_bytes = CachedFileSize::new(size.into());
            });

            let deletion_file = fragment
                .deletion_file
                .as_ref()
                .map(|deletion_file| DeletionFile {
                    num_deleted_rows,
                    ..deletion_file.clone()
                });

            Ok::<_, Error>(Fragment {
                physical_rows: Some(physical_rows),
                deletion_file,
                files: data_files,
                ..fragment.clone()
            })
        })
        .buffered(dataset.object_store.io_parallelism())
        // Filter out empty fragments
        .try_filter(|frag| futures::future::ready(frag.num_rows().map(|n| n > 0).unwrap_or(false)))
        .boxed();

    new_fragments.try_collect().await
}

fn must_recalculate_fragment_bitmap(
    index: &IndexMetadata,
    version: Option<&WriterVersion>,
) -> bool {
    if index.fragment_bitmap.is_none() {
        return true;
    }
    // If the fragment bitmap was written by an old version of lance then we need to recalculate
    // it because it could be corrupt due to a bug in versions < 0.8.15
    if let Some(version) = version {
        if version.library != "lance" {
            // We assume a different library is not affected by the bug.
            return false;
        }

        let cutoff = semver::Version::new(0, 8, 15);
        version
            .lance_lib_version()
            .map(|lance_lib_version| lance_lib_version < cutoff)
            .unwrap_or(true)
    } else {
        // Older versions of Lance library didn't record writer version at all.
        true
    }
}

/// Update indices with new fields.
///
/// Indices might be missing `fragment_bitmap`, so this function will add it.
/// Indices might also be missing `files` (file sizes), so this function will collect them.
async fn migrate_indices(dataset: &Dataset, indices: &mut [IndexMetadata]) -> Result<()> {
    infer_missing_vector_details(dataset, indices).await;
    let needs_recalculating = match detect_overlapping_fragments(indices) {
        Ok(()) => vec![],
        Err(BadFragmentBitmapError { bad_indices }) => {
            bad_indices.into_iter().map(|(name, _)| name).collect()
        }
    };
    for index in indices.iter_mut() {
        if needs_recalculating.contains(&index.name)
            || must_recalculate_fragment_bitmap(index, dataset.manifest.writer_version.as_ref())
                && !is_system_index(index)
        {
            debug_assert_eq!(index.fields.len(), 1);
            let idx_field = dataset.schema().field_by_id(index.fields[0]).ok_or_else(|| Error::internal(format!("Index with uuid {} referred to field with id {} which did not exist in dataset", index.uuid, index.fields[0])))?;
            // We need to calculate the fragments covered by the index
            let idx = dataset
                .open_generic_index(&idx_field.name, &index.uuid, &NoOpMetricsCollector)
                .await?;
            index.fragment_bitmap = Some(idx.calculate_included_frags().await?);
        }
        // We can't reliably recalculate the index type for label_list and bitmap indices and so we can't migrate this field.
        // However, we still log for visibility and to help potentially diagnose issues in the future if we grow to rely on the field.
        if index.index_details.is_none() {
            log::debug!(
                "the index with uuid {} is missing index metadata.  This probably means it was written with Lance version <= 0.19.2.  This is not a problem.",
                index.uuid
            );
        }

        // Migrate file sizes for indices that don't have them.
        // Use indice_files_dir to handle shallow-cloned indices with base_id.
        if index.files.is_none() && !is_system_index(index) {
            let result = async {
                let index_dir = dataset
                    .indice_files_dir(index)?
                    .join(index.uuid.to_string());
                let object_store = dataset.object_store_for_index(index).await?;
                list_index_files_with_sizes(&object_store, &index_dir).await
            }
            .await;
            match result {
                Ok(files) => {
                    log::debug!(
                        "Migrated file sizes for index {} (uuid: {}): {} files",
                        index.name,
                        index.uuid,
                        files.len()
                    );
                    index.files = Some(files);
                }
                Err(e) => {
                    // Log but don't fail - file sizes are optional
                    log::debug!(
                        "Could not collect file sizes for index {} (uuid: {}): {}",
                        index.name,
                        index.uuid,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}

pub(crate) struct BadFragmentBitmapError {
    pub bad_indices: Vec<(String, Vec<u32>)>,
}

/// Detect whether a given index has overlapping fragment bitmaps in its index
/// segments.
pub(crate) fn detect_overlapping_fragments(
    indices: &[IndexMetadata],
) -> std::result::Result<(), BadFragmentBitmapError> {
    let index_names: HashSet<&str> = indices.iter().map(|i| i.name.as_str()).collect();
    let mut bad_indices = Vec::new(); // (index_name, overlapping_fragments)
    for name in index_names {
        let mut seen_fragment_ids = HashSet::new();
        let mut overlap = Vec::new();
        for index in indices.iter().filter(|i| i.name == name) {
            if let Some(fragment_bitmap) = index.fragment_bitmap.as_ref() {
                for fragment in fragment_bitmap {
                    if !seen_fragment_ids.insert(fragment) {
                        overlap.push(fragment);
                    }
                }
            }
        }
        if !overlap.is_empty() {
            bad_indices.push((name.to_string(), overlap));
        }
    }
    if bad_indices.is_empty() {
        Ok(())
    } else {
        Err(BadFragmentBitmapError { bad_indices })
    }
}

pub(crate) async fn do_commit_detached_transaction(
    dataset: &Dataset,
    object_store: &ObjectStore,
    commit_handler: &dyn CommitHandler,
    transaction: &Transaction,
    write_config: &ManifestWriteConfig,
    commit_config: &CommitConfig,
    retry_timeout: Duration,
) -> Result<(Manifest, ManifestLocation)> {
    let pb_transaction = pb::Transaction::from(transaction);
    let inline_transaction = pb_transaction.encoded_len() <= MAX_INLINE_TRANSACTION_BYTES;

    // We don't strictly need a transaction file but we go ahead and create one for
    // record-keeping if nothing else.
    let transaction_file = if !write_config.disable_transaction_file() {
        write_transaction_file(object_store, &dataset.base, &pb_transaction).await?
    } else {
        String::new()
    };

    // The inline copy is moved into the first commit attempt; a retry (only on
    // a random-version collision) rebuilds it instead of cloning up front.
    let mut inline_tx: Option<lance_table::format::Transaction> =
        inline_transaction.then(|| pb_transaction.into());

    // We still do a loop since we may have conflicts in the random version we pick
    let mut backoff = Backoff::default();
    let start = Instant::now();
    while backoff.attempt() < commit_config.num_retries {
        // Pick a random u64 with the highest bit set to indicate it is detached
        let random_version = rng().random::<u64>() | DETACHED_VERSION_MASK;

        let (mut manifest, mut indices) = match transaction.operation {
            Operation::Restore { version } => {
                Transaction::restore_old_manifest(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    version,
                    write_config,
                    &transaction_file,
                    &dataset.manifest,
                )
                .await?
            }
            _ => transaction.build_manifest(
                Some(dataset.manifest.as_ref()),
                dataset.load_indices().await?.as_ref().clone(),
                &transaction_file,
                write_config,
            )?,
        };

        manifest.version = random_version;

        // recompute_stats is always false so far because detached manifests are newer than
        // the old stats bug.
        migrate_manifest(dataset, &mut manifest, /*recompute_stats=*/ false).await?;
        // fix_schema and check_storage_version are just for sanity-checking and consistency
        fix_schema(&mut manifest)?;
        check_storage_version(&mut manifest)?;
        check_column_indices(&manifest)?;
        check_fragment_ids(&manifest)?;
        migrate_indices(dataset, &mut indices).await?;

        // Try to commit the manifest
        let result = write_manifest_file(
            object_store,
            commit_handler,
            &dataset.base,
            &mut manifest,
            if indices.is_empty() {
                None
            } else {
                Some(indices.clone())
            },
            write_config,
            ManifestNamingScheme::V2,
            inline_tx.take(),
        )
        .await;

        match result {
            Ok(location) => {
                return Ok((manifest, location));
            }
            Err(CommitError::CommitConflict) => {
                // Either an (extremely unlikely) random-version collision, or
                // our own write landed but returned an ambiguous error.
                // Verify before retrying with a new random version.
                match verify_commit_outcome(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    manifest.version,
                    transaction,
                )
                .await
                {
                    CommitOutcome::Ours {
                        manifest: committed_manifest,
                        location,
                    } => {
                        return Ok((*committed_manifest, location));
                    }
                    CommitOutcome::Foreign | CommitOutcome::Absent => {}
                    CommitOutcome::Unknown => {
                        return Err(Error::commit_status_unknown_source(
                            manifest.version,
                            "detached commit reported a conflict but the manifest could \
                             not be read back for verification"
                                .to_string()
                                .into(),
                        ));
                    }
                }
                if start.elapsed() > retry_timeout {
                    cleanup_transaction_file(object_store, &dataset.base, &transaction_file).await;
                    return Err(timeout_error(retry_timeout, backoff.attempt() + 1));
                }
                let sleep_fut = tokio::time::sleep(backoff.next_backoff());
                if let Err(error) =
                    maybe_timeout(backoff.attempt(), start, retry_timeout, sleep_fut).await
                {
                    cleanup_transaction_file(object_store, &dataset.base, &transaction_file).await;
                    return Err(error);
                }
                // The inline copy was moved into the failed attempt; rebuild
                // it for the retry with a new random version.
                inline_tx = inline_transaction.then(|| pb::Transaction::from(transaction).into());
            }
            Err(CommitError::OtherError(err)) => {
                match verify_commit_outcome(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    manifest.version,
                    transaction,
                )
                .await
                {
                    CommitOutcome::Ours {
                        manifest: committed_manifest,
                        location,
                    } => {
                        if commit_handler.propagate_commit_error_after_success() {
                            return Err(err);
                        }
                        return Ok((*committed_manifest, location));
                    }
                    CommitOutcome::Foreign | CommitOutcome::Absent => {}
                    CommitOutcome::Unknown => {
                        return Err(Error::commit_status_unknown_source(
                            manifest.version,
                            Box::new(err),
                        ));
                    }
                }
                cleanup_transaction_file(object_store, &dataset.base, &transaction_file).await;
                return Err(err);
            }
        }
    }

    // This should be extremely unlikely.  There should not be *that* many detached commits.  If
    // this happens then it seems more likely there is a bug in our random u64 generation.
    cleanup_transaction_file(object_store, &dataset.base, &transaction_file).await;
    Err(crate::Error::commit_conflict_source(
        0,
        format!(
            "Failed find unused random u64 after {} retries.",
            commit_config.num_retries
        )
        .into(),
    ))
}

pub(crate) async fn commit_detached_transaction(
    dataset: &Dataset,
    object_store: &ObjectStore,
    commit_handler: &dyn CommitHandler,
    transaction: &Transaction,
    write_config: &ManifestWriteConfig,
    commit_config: &CommitConfig,
    retry_timeout: Duration,
) -> Result<(Manifest, ManifestLocation)> {
    do_commit_detached_transaction(
        dataset,
        object_store,
        commit_handler,
        transaction,
        write_config,
        commit_config,
        retry_timeout,
    )
    .await
}

/// Load new transactions and sort them by version in ascending order (oldest to newest)
async fn load_and_sort_new_transactions(
    dataset: &Dataset,
) -> Result<(Dataset, Vec<(u64, Arc<Transaction>)>)> {
    let NewTransactionResult {
        dataset: new_ds,
        new_transactions,
    } = load_new_transactions(dataset);
    let new_transactions = new_transactions.try_collect::<Vec<_>>();
    let (new_ds, mut txns) = futures::future::try_join(new_ds, new_transactions).await?;
    txns.sort_by_key(|(version, _)| *version);
    Ok((new_ds, txns))
}

/// Success-path bookkeeping shared by the direct-success and
/// verified-own-commit paths of [`commit_transaction`]: populate the session
/// caches and run the auto-cleanup hook.
async fn record_successful_commit(
    dataset: &Dataset,
    transaction: &Transaction,
    manifest: &Manifest,
    location: &ManifestLocation,
    indices: Vec<IndexMetadata>,
    skip_auto_cleanup: bool,
) {
    let tx_key = crate::session::caches::TransactionKey {
        version: manifest.version,
    };
    dataset
        .metadata_cache
        .insert_with_key(&tx_key, Arc::new(transaction.clone()))
        .await;

    let manifest_key = crate::session::caches::ManifestKey {
        version: location.version,
        e_tag: location.e_tag.as_deref(),
    };
    dataset
        .metadata_cache
        .insert_with_key(&manifest_key, Arc::new(manifest.clone()))
        .await;
    if !indices.is_empty() {
        let key = IndexMetadataKey {
            version: manifest.version,
            store_identity: &dataset.object_store.store_prefix,
        };
        dataset
            .index_cache
            .insert_with_key(&key, Arc::new(indices))
            .await;
    }

    if !skip_auto_cleanup {
        // Note: We're using the old dataset here (before the new manifest is committed).
        // This means cleanup runs based on the previous version's state, which may affect
        // which versions are available for cleanup.
        match auto_cleanup_hook(dataset, manifest).await {
            Ok(Some(stats)) => log::info!("Auto cleanup triggered: {:?}", stats),
            Err(e) => log::error!("Error encountered during auto_cleanup_hook: {}", e),
            _ => {}
        };
    }
}

/// Attempt to commit a transaction, with retries and conflict resolution.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_transaction(
    dataset: &Dataset,
    object_store: &ObjectStore,
    commit_handler: &dyn CommitHandler,
    transaction: &Transaction,
    write_config: &ManifestWriteConfig,
    commit_config: &CommitConfig,
    retry_timeout: Duration,
    manifest_naming_scheme: ManifestNamingScheme,
    affected_rows: Option<&RowAddrTreeMap>,
) -> Result<(Manifest, ManifestLocation)> {
    // Note: object_store has been configured with WriteParams, but dataset.object_store.as_ref()
    // has not necessarily. So for anything involving writing, use `object_store`.
    let read_version = transaction.read_version;
    let mut target_version = read_version + 1;
    let original_dataset = dataset.clone();

    // read_version sometimes defaults to zero for overwrite.
    // If num_retries is zero, we are in "strict overwrite" mode.
    // Strict overwrites are not subject to any sort of automatic conflict resolution.
    let strict_overwrite = matches!(transaction.operation, Operation::Overwrite { .. })
        && commit_config.num_retries == 0;
    let mut dataset =
        if dataset.manifest.version != read_version && (read_version != 0 || strict_overwrite) {
            // If the dataset version is not the same as the read version, we need to
            // checkout the read version.
            dataset.checkout_version(read_version).await?
        } else {
            // If the dataset version is the same as the read version, we can use it directly.
            dataset.clone()
        };

    let mut transaction = transaction.clone();

    let num_attempts = std::cmp::max(commit_config.num_retries, 1);
    let mut backoff = SlotBackoff::default();
    let start = Instant::now();

    // Other transactions that may have been committed since the read_version.
    // We keep pair of (version, transaction). No other transactions to check initially
    let mut other_transactions: Vec<(u64, Arc<Transaction>)>;
    // Track the transaction file written in the current loop iteration so we can
    // delete it if the commit ultimately fails.
    let mut current_transaction_file = String::new();

    while backoff.attempt() < num_attempts {
        // We are pessimistic here and assume there may be other transactions
        // we need to check for. We could be optimistic here and blindly
        // attempt to commit, giving faster performance for sequence writes and
        // slower performance for concurrent writes. But that makes the fast path
        // faster and the slow path slower, which makes performance less predictable
        // for users. So we always check for other transactions.
        // We skip this for strict overwrites, because strict overwrites can't be rebased.
        if !strict_overwrite {
            (dataset, other_transactions) = load_and_sort_new_transactions(&dataset).await?;

            // See if we can retry the commit. Try to account for all
            // transactions that have been committed since the read_version.
            // Use small amount of backoff to handle transactions that all
            // started at exact same time better.

            let mut rebase =
                TransactionRebase::try_new(&original_dataset, transaction, affected_rows).await?;

            for (other_version, other_transaction) in other_transactions.iter() {
                rebase.check_txn(other_transaction, *other_version)?;
            }

            transaction = rebase.finish(&dataset).await?;
        }

        // Recomputed every attempt: the rebase above may have rewritten the
        // transaction.
        let pb_transaction = pb::Transaction::from(&transaction);
        let inline_transaction = pb_transaction.encoded_len() <= MAX_INLINE_TRANSACTION_BYTES;

        current_transaction_file = if !write_config.disable_transaction_file() {
            write_transaction_file(object_store, &dataset.base, &pb_transaction).await?
        } else {
            String::new()
        };
        let transaction_file = current_transaction_file.as_str();

        target_version = dataset.manifest.version + 1;
        if is_detached_version(target_version) {
            return Err(Error::internal(
                "more than 2^65 versions have been created and so regular version numbers are appearing as 'detached' versions.",
            ));
        }
        // Build an up-to-date manifest from the transaction and current manifest
        let (mut manifest, mut indices) = match transaction.operation {
            Operation::Restore { version } => {
                Transaction::restore_old_manifest(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    version,
                    write_config,
                    transaction_file,
                    &dataset.manifest,
                )
                .await?
            }
            _ => transaction.build_manifest(
                Some(dataset.manifest.as_ref()),
                dataset.load_indices().await?.as_ref().clone(),
                transaction_file,
                write_config,
            )?,
        };

        manifest.version = target_version;

        let previous_writer_version = &dataset.manifest.writer_version;
        // The versions of Lance prior to when we started writing the writer version
        // sometimes wrote incorrect `Fragment.physical_rows` values, so we should
        // make sure to recompute them.
        // See: https://github.com/lance-format/lance/issues/1531
        let recompute_stats = previous_writer_version.is_none();

        migrate_manifest(&dataset, &mut manifest, recompute_stats).await?;

        fix_schema(&mut manifest)?;

        check_storage_version(&mut manifest)?;
        check_column_indices(&manifest)?;
        check_fragment_ids(&manifest)?;

        migrate_indices(&dataset, &mut indices).await?;

        // Try to commit the manifest
        let result = write_manifest_file(
            object_store,
            commit_handler,
            &dataset.base,
            &mut manifest,
            if indices.is_empty() {
                None
            } else {
                Some(indices.clone())
            },
            write_config,
            manifest_naming_scheme,
            inline_transaction.then(|| pb_transaction.into()),
        )
        .await;

        match result {
            Ok(manifest_location) => {
                record_successful_commit(
                    &dataset,
                    &transaction,
                    &manifest,
                    &manifest_location,
                    indices,
                    commit_config.skip_auto_cleanup,
                )
                .await;
                return Ok((manifest, manifest_location));
            }
            Err(CommitError::CommitConflict) => {
                // The store may have applied this attempt's write and still
                // reported a conflict (e.g. the response to a successful
                // conditional PUT was lost and an internal retry saw
                // "already exists"). Verify who owns the version before
                // treating the attempt as lost: deleting the artifacts of a
                // commit that actually landed corrupts the version.
                match verify_commit_outcome(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    target_version,
                    &transaction,
                )
                .await
                {
                    CommitOutcome::Ours {
                        manifest: committed_manifest,
                        location,
                    } => {
                        let committed_manifest = *committed_manifest;
                        record_successful_commit(
                            &dataset,
                            &transaction,
                            &committed_manifest,
                            &location,
                            indices,
                            commit_config.skip_auto_cleanup,
                        )
                        .await;
                        return Ok((committed_manifest, location));
                    }
                    // Confirmed loss: another writer owns the version (or,
                    // for handlers that detect conflicts before writing,
                    // the attempt never landed). Proceed with the normal
                    // rebase-and-retry path.
                    CommitOutcome::Foreign | CommitOutcome::Absent => {}
                    CommitOutcome::Unknown => {
                        return Err(Error::commit_status_unknown_source(
                            target_version,
                            "commit reported a conflict but the manifest at the target \
                             version could not be read back for verification"
                                .to_string()
                                .into(),
                        ));
                    }
                }
                let next_attempt_i = backoff.attempt() + 1;

                if backoff.attempt() == 0 {
                    // We add 10% buffer here, to allow concurrent writes to complete.
                    // We pass the first attempt's time to the backoff so it's used
                    // as the unit for backoff time slots.
                    // See SlotBackoff implementation for more details on how this works.
                    backoff = backoff.with_unit((start.elapsed().as_millis() * 11 / 10) as u32);
                }

                if next_attempt_i < num_attempts {
                    // The transaction file from this attempt is now stale; clean it up
                    // before the next attempt writes a new one (possibly rebased).
                    cleanup_transaction_file(
                        object_store,
                        &dataset.base,
                        &current_transaction_file,
                    )
                    .await;
                    if start.elapsed() > retry_timeout {
                        return Err(timeout_error(retry_timeout, backoff.attempt() + 1));
                    }
                    let sleep_fut = tokio::time::sleep(backoff.next_backoff());
                    maybe_timeout(backoff.attempt(), start, retry_timeout, sleep_fut).await?;
                    continue;
                } else {
                    break;
                }
            }
            Err(CommitError::OtherError(err)) => {
                match verify_commit_outcome(
                    object_store,
                    commit_handler,
                    &dataset.base,
                    target_version,
                    &transaction,
                )
                .await
                {
                    CommitOutcome::Ours {
                        manifest: committed_manifest,
                        location,
                    } => {
                        let committed_manifest = *committed_manifest;
                        record_successful_commit(
                            &dataset,
                            &transaction,
                            &committed_manifest,
                            &location,
                            indices,
                            commit_config.skip_auto_cleanup,
                        )
                        .await;
                        if commit_handler.propagate_commit_error_after_success() {
                            return Err(err);
                        }
                        return Ok((committed_manifest, location));
                    }
                    CommitOutcome::Foreign | CommitOutcome::Absent => {
                        // The attempt certainly did not land; its
                        // transaction file is orphaned.
                        cleanup_transaction_file(
                            object_store,
                            &dataset.base,
                            &current_transaction_file,
                        )
                        .await;
                        return Err(err);
                    }
                    CommitOutcome::Unknown => {
                        return Err(Error::commit_status_unknown_source(
                            target_version,
                            Box::new(err),
                        ));
                    }
                }
            }
        }
    }

    cleanup_transaction_file(object_store, &dataset.base, &current_transaction_file).await;
    Err(crate::Error::commit_conflict_source(
        target_version,
        format!(
            "Failed to commit the transaction after {} retries.",
            commit_config.num_retries
        )
        .into(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::future::join_all;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_core::datatypes::{Field, Schema};
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};
    use lance_file::version::ConcreteFileVersion;
    use lance_index::IndexType;
    use lance_linalg::distance::MetricType;
    use lance_table::format::{DataFile, DataStorageFormat};
    use lance_table::io::commit::{
        CommitLease, CommitLock, ManifestWriter, RenameCommitHandler, UnsafeCommitHandler,
    };
    use lance_testing::datagen::generate_random_array;

    use super::*;

    use crate::Dataset;
    use crate::dataset::{WriteMode, WriteParams};
    use crate::index::vector::VectorIndexParams;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

    async fn test_commit_handler(handler: Arc<dyn CommitHandler>, should_succeed: bool) {
        // Create a dataset, passing handler as commit handler
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            DataType::Int64,
            false,
        )]));
        let data = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(data)], schema);

        let options = WriteParams {
            commit_handler: Some(handler),
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://test", Some(options))
            .await
            .unwrap();

        // Create 10 concurrent tasks to write into the table
        // Record how many succeed and how many fail
        let tasks = (0..10).map(|_| {
            let mut dataset = dataset.clone();
            tokio::task::spawn(async move {
                dataset
                    .delete("x = 2")
                    .await
                    .map(|_| dataset.manifest.version)
            })
        });

        let task_results: Vec<Option<u64>> = join_all(tasks)
            .await
            .iter()
            .map(|res| match res {
                Ok(Ok(version)) => Some(*version),
                _ => None,
            })
            .collect();

        let num_successes = task_results.iter().filter(|x| x.is_some()).count();
        let distinct_results: HashSet<_> = task_results.iter().filter_map(|x| x.as_ref()).collect();

        if should_succeed {
            assert_eq!(
                num_successes,
                distinct_results.len(),
                "Expected no two tasks to succeed for the same version. Got {:?}",
                task_results
            );
        } else {
            // All we can promise here is at least one tasks succeeds, but multiple
            // could in theory.
            assert!(num_successes >= distinct_results.len(),);
        }
    }

    #[tokio::test]
    async fn test_rename_commit_handler() {
        // Rename is default for memory
        let handler = Arc::new(RenameCommitHandler);
        test_commit_handler(handler, true).await;
    }

    #[tokio::test]
    async fn test_custom_commit() {
        #[derive(Debug)]
        struct CustomCommitHandler {
            locked_version: Arc<Mutex<Option<u64>>>,
        }

        struct CustomCommitLease {
            version: u64,
            locked_version: Arc<Mutex<Option<u64>>>,
        }

        #[async_trait::async_trait]
        impl CommitLock for CustomCommitHandler {
            type Lease = CustomCommitLease;

            async fn lock(&self, version: u64) -> std::result::Result<Self::Lease, CommitError> {
                let mut locked_version = self.locked_version.lock().unwrap();
                if locked_version.is_some() {
                    // Already locked
                    return Err(CommitError::CommitConflict);
                }

                // Lock the version
                *locked_version = Some(version);

                Ok(CustomCommitLease {
                    version,
                    locked_version: self.locked_version.clone(),
                })
            }
        }

        #[async_trait::async_trait]
        impl CommitLease for CustomCommitLease {
            async fn release(&self, _success: bool) -> std::result::Result<(), CommitError> {
                let mut locked_version = self.locked_version.lock().unwrap();
                if *locked_version != Some(self.version) {
                    // Already released
                    return Err(CommitError::CommitConflict);
                }

                // Release the version
                *locked_version = None;

                Ok(())
            }
        }

        let locked_version = Arc::new(Mutex::new(None));
        let handler = Arc::new(CustomCommitHandler { locked_version });
        test_commit_handler(handler, true).await;
    }

    #[tokio::test]
    async fn test_unsafe_commit_handler() {
        let handler = Arc::new(UnsafeCommitHandler);
        test_commit_handler(handler, false).await;
    }

    #[tokio::test]
    async fn test_roundtrip_transaction_file() {
        let object_store = ObjectStore::memory();
        let base_path = Path::from("test");
        let transaction = Transaction::new(
            42,
            Operation::Append { fragments: vec![] },
            Some("hello world".to_string()),
        );

        let file_name = write_transaction_file(
            &object_store,
            &base_path,
            &pb::Transaction::from(&transaction),
        )
        .await
        .unwrap();
        let read_transaction = read_transaction_file(&object_store, &base_path, &file_name)
            .await
            .unwrap();

        assert_eq!(transaction.read_version, read_transaction.read_version);
        assert_eq!(transaction.uuid, read_transaction.uuid);
        assert!(matches!(
            read_transaction.operation,
            Operation::Append { .. }
        ));
        assert_eq!(transaction.tag, read_transaction.tag);
    }

    #[tokio::test]
    async fn test_concurrent_create_index() {
        // Create a table with two vector columns
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let dimension = 16;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "vector1",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
            ArrowField::new(
                "vector2",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    dimension,
                ),
                false,
            ),
        ]));
        let float_arr = generate_random_array(512 * dimension as usize);
        let vectors = Arc::new(
            <arrow_array::FixedSizeListArray as FixedSizeListArrayExt>::try_new_from_values(
                float_arr, dimension,
            )
            .unwrap(),
        );
        let batches = vec![
            RecordBatch::try_new(schema.clone(), vec![vectors.clone(), vectors.clone()]).unwrap(),
        ];

        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        let dataset = Dataset::write(reader, test_uri, None).await.unwrap();
        dataset.validate().await.unwrap();

        // From initial version, concurrently call create index 3 times,
        // two of which will be for the same column.
        let params = VectorIndexParams::ivf_pq(10, 8, 2, MetricType::L2, 50);
        let futures: Vec<_> = ["vector1", "vector1", "vector2"]
            .iter()
            .map(|col_name| {
                let mut dataset = dataset.clone();
                let params = params.clone();
                tokio::spawn(async move {
                    dataset
                        .create_index(&[col_name], IndexType::Vector, None, &params, true)
                        .await
                })
            })
            .collect();

        let results = join_all(futures).await;
        let success_count = results
            .iter()
            .filter(|result| matches!(result, Ok(Ok(_))))
            .count();
        let retryable_count = results
            .iter()
            .filter(|result| matches!(result, Ok(Err(Error::RetryableCommitConflict { .. }))))
            .count();
        assert_eq!(success_count, 2, "{results:?}");
        assert_eq!(retryable_count, 1, "{results:?}");

        // Validate that each version has the anticipated number of indexes
        let dataset = dataset.checkout_version(1).await.unwrap();
        assert!(dataset.load_indices().await.unwrap().is_empty());

        let dataset = dataset.checkout_version(2).await.unwrap();
        assert_eq!(dataset.load_indices().await.unwrap().len(), 1);

        let dataset = dataset.checkout_version(3).await.unwrap();
        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.is_empty() && indices.len() <= 2);

        // At this point, we have created two indices. If they are both for the same column,
        // it must be vector1 and not vector2.
        if indices.len() == 2 {
            let mut fields: Vec<i32> = indices.iter().flat_map(|i| i.fields.clone()).collect();
            fields.sort();
            assert_eq!(fields, vec![0, 1]);
        } else {
            assert_eq!(indices[0].fields, vec![0]);
        }

        assert!(dataset.checkout_version(4).await.is_err());
    }

    #[tokio::test]
    async fn test_load_and_sort_new_transactions() {
        // Create a dataset
        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(1), FragmentRowCount::from(10))
            .await
            .unwrap();

        // Create 100 small UpdateConfig transactions
        for i in 0..100 {
            dataset
                .update_config(vec![(format!("key_{}", i), format!("value_{}", i))])
                .await
                .unwrap();
        }

        // Now load the dataset at version 1 and check that load_and_sort_new_transactions
        // returns transactions in order
        let dataset_v1 = dataset.checkout_version(1).await.unwrap();
        let (_, transactions) = load_and_sort_new_transactions(&dataset_v1).await.unwrap();

        // Verify transactions are sorted by version
        let versions: Vec<u64> = transactions.iter().map(|(v, _)| *v).collect();
        for i in 1..versions.len() {
            assert!(
                versions[i] > versions[i - 1],
                "Transactions not in order: version {} came after version {}",
                versions[i],
                versions[i - 1]
            );
        }

        // Also verify we have exactly 100 transactions (versions 2-101)
        assert_eq!(transactions.len(), 100);
        assert_eq!(versions.first(), Some(&2));
        assert_eq!(versions.last(), Some(&101));
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        // Test concurrent appends - all should succeed
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone()),
            test_uri,
            None,
        )
        .await
        .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let futures: Vec<_> = (0..5)
            .map(|_| {
                let batch = batch.clone();
                let schema = schema.clone();
                let uri = test_uri.to_string();
                tokio::spawn(async move {
                    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
                    Dataset::write(
                        reader,
                        &uri,
                        Some(WriteParams {
                            mode: WriteMode::Append,
                            ..Default::default()
                        }),
                    )
                    .await
                })
            })
            .collect();
        let results = join_all(futures).await;

        for result in results {
            assert!(matches!(result, Ok(Ok(_))), "{:?}", result);
        }

        let dataset = dataset.checkout_version(6).await.unwrap();
        assert_eq!(dataset.get_fragments().len(), 5);
        dataset.validate().await.unwrap()
    }

    #[tokio::test]
    async fn test_restore_does_not_decrease_max_fragment_id() {
        let reader = gen_batch()
            .col("i", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(3), BatchCount::from(1));
        let mut dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        // Append a few times to advance max_fragment_id and create newer versions.
        for _ in 0..2 {
            let reader = gen_batch()
                .col("i", array::step::<Int32Type>())
                .into_reader_rows(RowCount::from(3), BatchCount::from(1));
            dataset.append(reader, None).await.unwrap();
        }

        let latest_max = dataset.manifest.max_fragment_id().unwrap_or(0);

        // Restore an earlier version (version 1) as the latest.
        let mut dataset_v1 = dataset.checkout_version(1).await.unwrap();
        dataset_v1.restore().await.unwrap();

        // After restore, max_fragment_id should not decrease compared to the latest value before restore.
        let restored_max = dataset_v1.manifest.max_fragment_id().unwrap_or(0);
        assert!(
            restored_max >= latest_max,
            "max_fragment_id should not decrease on restore: before={}, after={}",
            latest_max,
            restored_max
        );
    }

    async fn get_empty_dataset() -> (TempStrDir, Dataset) {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));

        let ds = Dataset::write(
            RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone()),
            test_uri,
            None,
        )
        .await
        .unwrap();
        (test_dir, ds)
    }

    #[tokio::test]
    async fn test_good_concurrent_config_writes() {
        let (_tmpdir, dataset) = get_empty_dataset().await;
        let original_num_config_keys = dataset.manifest.config.len();

        // Test successful concurrent insert config operations
        let futures: Vec<_> = ["key1", "key2", "key3", "key4", "key5"]
            .iter()
            .map(|key| {
                let mut dataset = dataset.clone();
                tokio::spawn(async move {
                    dataset
                        .update_config(HashMap::from([(
                            key.to_string(),
                            Some("value".to_string()),
                        )]))
                        .await
                })
            })
            .collect();
        let results = join_all(futures).await;

        // Assert all succeeded
        for result in results {
            assert!(matches!(result, Ok(Ok(_))), "{:?}", result);
        }

        let dataset = dataset.checkout_version(6).await.unwrap();
        assert_eq!(dataset.manifest.config.len(), 5 + original_num_config_keys);

        dataset.validate().await.unwrap();

        // Test successful concurrent delete operations. If multiple delete
        // operations attempt to delete the same key, they are all successful.
        let futures: Vec<_> = ["key1", "key1", "key1", "key2", "key2"]
            .iter()
            .map(|key| {
                let mut dataset = dataset.clone();
                tokio::spawn(async move {
                    dataset
                        .update_config(HashMap::from([(key.to_string(), None)]))
                        .await
                })
            })
            .collect();
        let results = join_all(futures).await;

        // Assert all succeeded
        for result in results {
            assert!(matches!(result, Ok(Ok(_))), "{:?}", result);
        }

        let dataset = dataset.checkout_version(11).await.unwrap();

        // There are now two fewer keys
        assert_eq!(dataset.manifest.config.len(), 3 + original_num_config_keys);

        dataset.validate().await.unwrap()
    }

    #[tokio::test]
    async fn test_bad_concurrent_config_writes() {
        // If two concurrent insert config operations occur for the same key, a
        // `CommitConflict` should be returned
        let (_tmpdir, dataset) = get_empty_dataset().await;

        let futures: Vec<_> = ["key1", "key1", "key2", "key3", "key4"]
            .iter()
            .map(|key| {
                let mut dataset = dataset.clone();
                tokio::spawn(async move {
                    dataset
                        .update_config(HashMap::from([(
                            key.to_string(),
                            Some("value".to_string()),
                        )]))
                        .await
                })
            })
            .collect();

        let results = join_all(futures).await;

        // Assert that either the first or the second operation fails
        let mut first_operation_failed = false;
        for (i, result) in results.into_iter().enumerate() {
            let result = result.unwrap();
            match i {
                0 => {
                    if result.is_err() {
                        first_operation_failed = true;
                        assert!(
                            matches!(&result, &Err(Error::IncompatibleTransaction { .. })),
                            "{:?}",
                            result,
                        );
                    }
                }
                1 => match first_operation_failed {
                    true => assert!(result.is_ok(), "{:?}", result),
                    false => {
                        assert!(
                            matches!(&result, &Err(Error::IncompatibleTransaction { .. })),
                            "{:?}",
                            result,
                        );
                    }
                },
                _ => assert!(result.is_ok(), "{:?}", result),
            }
        }
    }

    #[test]
    fn test_fix_schema() {
        // Manifest has a fragment with no fields in use
        // Manifest has a duplicate field id in one fragment but not others.
        let mut field0 =
            Field::try_from(ArrowField::new("a", arrow_schema::DataType::Int64, false)).unwrap();
        field0.set_id(-1, &mut 0);
        let mut field2 =
            Field::try_from(ArrowField::new("b", arrow_schema::DataType::Int64, false)).unwrap();
        field2.set_id(-1, &mut 2);

        let schema = Schema {
            fields: vec![field0.clone(), field2.clone()],
            metadata: Default::default(),
        };
        let fragments = vec![
            Fragment {
                id: 0,
                files: vec![
                    DataFile::new_legacy_from_fields("path1", vec![0, 1, 2], None),
                    DataFile::new_legacy_from_fields("unused", vec![9], None),
                ],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
            Fragment {
                id: 1,
                files: vec![
                    DataFile::new_legacy_from_fields("path2", vec![0, 1, 2], None),
                    DataFile::new_legacy_from_fields("path3", vec![2], None),
                ],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
        ];

        let mut manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        fix_schema(&mut manifest).unwrap();

        // Because of the duplicate field id, the field id of field2 should have been changed to 10
        field2.id = 10;
        let expected_schema = Schema {
            fields: vec![field0, field2],
            metadata: Default::default(),
        };
        assert_eq!(manifest.schema, expected_schema);

        // The fragment with just field 9 should have been removed, since it's
        // not used in the current schema.
        // The field 2 should have been changed to 10, except in the first
        // file of the second fragment.
        let expected_fragments = vec![
            Fragment {
                id: 0,
                files: vec![DataFile::new_legacy_from_fields(
                    "path1",
                    vec![0, 1, 10],
                    None,
                )],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
            Fragment {
                id: 1,
                files: vec![
                    DataFile::new_legacy_from_fields("path2", vec![0, 1, 2], None),
                    DataFile::new_legacy_from_fields("path3", vec![10], None),
                ],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                last_updated_at_version_meta: None,
                created_at_version_meta: None,
            },
        ];
        assert_eq!(manifest.fragments.as_ref(), &expected_fragments);
    }

    /// A CommitHandler that always fails with OtherError, used to simulate
    /// a manifest write failure so we can verify orphaned transaction files
    /// are cleaned up.
    #[derive(Debug)]
    struct FailingCommitHandler;

    #[async_trait::async_trait]
    impl CommitHandler for FailingCommitHandler {
        fn is_version_not_found_definitive(&self) -> bool {
            true
        }

        async fn commit(
            &self,
            _manifest: &mut Manifest,
            _indices: Option<Vec<IndexMetadata>>,
            _base_path: &Path,
            _object_store: &ObjectStore,
            _manifest_writer: ManifestWriter,
            _naming_scheme: ManifestNamingScheme,
            _transaction: Option<lance_table::format::Transaction>,
        ) -> std::result::Result<ManifestLocation, CommitError> {
            Err(CommitError::OtherError(lance_core::Error::io(
                "simulated commit failure",
            )))
        }
    }

    fn count_txn_files(uri: &str) -> usize {
        let tx_dir = std::path::Path::new(uri).join("_transactions");
        std::fs::read_dir(&tx_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).count())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn test_transaction_file_cleanup_on_commit_failure() {
        let tmp = TempStrDir::default();
        let uri = tmp.as_str();

        // Create initial dataset with a normal commit handler.
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
        Dataset::write(reader, uri, None).await.unwrap();

        let txn_files_before = count_txn_files(uri);

        // Attempt to append with a commit handler that always fails.
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(Arc::new(FailingCommitHandler)),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let result = Dataset::write(reader, uri, Some(params)).await;
        assert!(result.is_err(), "expected commit to fail");

        // The failed commit must not leave any extra transaction files behind.
        let txn_files_after = count_txn_files(uri);
        assert_eq!(
            txn_files_after,
            txn_files_before,
            "failed commit left {extra} orphaned transaction file(s)",
            extra = txn_files_after.saturating_sub(txn_files_before),
        );
    }
    fn simple_batch(schema: &Arc<ArrowSchema>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn simple_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            DataType::Int32,
            false,
        )]))
    }

    /// A commit whose manifest lands but is reported as a conflict (the
    /// incident shape: successful conditional PUT, response lost, internal
    /// retry sees "already exists") must be recognized as our own commit and
    /// returned as success — with the rows appearing exactly once and the
    /// transaction file left in place.
    #[tokio::test]
    async fn test_commit_succeeds_when_conflict_is_own_commit() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        Dataset::write(reader, uri, Some(params)).await.unwrap();
        assert_eq!(
            handler.resolve_calls(),
            0,
            "an uncontended commit must not perform verification reads"
        );
        let txn_files_before = count_txn_files(uri);

        handler.fail_next(AmbiguousFailure::LandAndConflict);
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![4, 5, 6]))],
            schema.clone(),
        );
        let ds = Dataset::write(reader, uri, Some(params))
            .await
            .expect("a conflict with our own landed commit must be reported as success");

        assert_eq!(ds.version().version, 2);
        assert_eq!(
            ds.count_rows(None).await.unwrap(),
            6,
            "rows must appear exactly once (no duplicate re-commit)"
        );
        assert_eq!(
            count_txn_files(uri),
            txn_files_before + 1,
            "the landed commit's transaction file is referenced by the manifest and must survive"
        );

        // A fresh reader sees the committed version.
        let ds2 = Dataset::open(uri).await.unwrap();
        assert_eq!(ds2.version().version, 2);
        assert_eq!(ds2.count_rows(None).await.unwrap(), 6);
    }

    /// Same as above, but the landed commit is reported as a plain I/O error
    /// (e.g. the store's retries all returned 5xx while the first attempt had
    /// landed). Verification must still recognize the commit as ours.
    #[tokio::test]
    async fn test_commit_succeeds_when_landed_with_other_error() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        Dataset::write(reader, uri, Some(params)).await.unwrap();
        let txn_files_before = count_txn_files(uri);

        handler.fail_next(AmbiguousFailure::LandAndError);
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![4, 5, 6]))],
            schema.clone(),
        );
        let ds = Dataset::write(reader, uri, Some(params))
            .await
            .expect("an errored commit that actually landed must be reported as success");

        assert_eq!(ds.version().version, 2);
        assert_eq!(ds.count_rows(None).await.unwrap(), 6);
        assert_eq!(count_txn_files(uri), txn_files_before + 1);
    }

    #[tokio::test]
    async fn test_commit_retries_temporarily_invisible_manifest() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());
        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        Dataset::write(reader, uri, Some(params)).await.unwrap();

        handler.fail_next(AmbiguousFailure::LandAndError);
        handler.fail_next_resolves_with_not_found(2);
        let calls_before = handler.resolve_calls();
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader =
            RecordBatchIterator::new(vec![Ok(simple_batch(&schema, vec![4, 5, 6]))], schema);
        let dataset = Dataset::write(reader, uri, Some(params))
            .await
            .expect("verification must retry a non-definitive NotFound result");

        assert_eq!(handler.resolve_calls() - calls_before, 3);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn test_commit_verifies_inline_transaction_without_transaction_file() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());
        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader =
            RecordBatchIterator::new(vec![Ok(simple_batch(&schema, vec![1, 2, 3]))], schema);
        let dataset = Dataset::write(reader, uri, Some(params)).await.unwrap();
        let transaction = Transaction::new(
            dataset.version().version,
            Operation::Append { fragments: vec![] },
            None,
        );
        let write_config = ManifestWriteConfig::default().with_transaction_file_disabled();

        handler.fail_next(AmbiguousFailure::LandAndConflict);
        let (manifest, _) = commit_transaction(
            &dataset,
            dataset.object_store.as_ref(),
            handler.as_ref(),
            &transaction,
            &write_config,
            &CommitConfig::default(),
            DEFAULT_COMMIT_RETRY_TIMEOUT,
            dataset.manifest_location.naming_scheme,
            None,
        )
        .await
        .expect("the inline transaction must identify the landed commit");

        assert_eq!(manifest.version, 2);
        assert!(manifest.transaction_file.is_none());
        assert!(manifest.transaction_section.is_some());
    }

    /// A commit that errors without landing keeps today's behavior:
    /// verification finds no manifest at the target version, the original
    /// error propagates (not status-unknown), and the orphaned transaction
    /// file is cleaned up.
    #[tokio::test]
    async fn test_commit_definite_failure_cleans_up_and_keeps_error() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        Dataset::write(reader, uri, Some(params)).await.unwrap();
        let txn_files_before = count_txn_files(uri);

        handler.fail_next(AmbiguousFailure::FailOutright);
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![4, 5, 6]))],
            schema.clone(),
        );
        let result = Dataset::write(reader, uri, Some(params)).await;
        let err = result.expect_err("commit that did not land must fail");
        assert!(
            !err.is_commit_status_unknown(),
            "a verified-absent commit is a definite failure, got: {:?}",
            err
        );
        assert_eq!(
            count_txn_files(uri),
            txn_files_before,
            "orphaned transaction file of a definitely-failed commit must be cleaned up"
        );
    }

    /// When the commit errors AND verification itself is unavailable, the
    /// commit status is unknown: surface `CommitStatusUnknown` and delete
    /// nothing.
    #[tokio::test]
    async fn test_commit_status_unknown_when_verification_unavailable() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        Dataset::write(reader, uri, Some(params)).await.unwrap();
        let txn_files_before = count_txn_files(uri);

        // The commit lands but errors, and verification reads fail too.
        handler.fail_next(AmbiguousFailure::LandAndError);
        handler
            .fail_resolve
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let params = WriteParams {
            mode: WriteMode::Append,
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![4, 5, 6]))],
            schema.clone(),
        );
        let result = Dataset::write(reader, uri, Some(params)).await;
        let err = result.expect_err("unknown status must not be reported as success");
        assert!(
            err.is_commit_status_unknown(),
            "expected CommitStatusUnknown, got: {:?}",
            err
        );
        assert_eq!(
            count_txn_files(uri),
            txn_files_before + 1,
            "nothing may be deleted while the commit status is unknown"
        );

        // The commit did land: a fresh reader must see a consistent v2.
        handler
            .fail_resolve
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let ds = Dataset::open(uri).await.unwrap();
        assert_eq!(ds.version().version, 2);
        assert_eq!(ds.count_rows(None).await.unwrap(), 6);
    }

    /// Dataset creation whose manifest lands but is reported as a conflict
    /// must succeed instead of returning "dataset already exists".
    #[tokio::test]
    async fn test_create_dataset_succeeds_when_conflict_is_own_commit() {
        use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

        let tmp = TempStrDir::default();
        let uri = tmp.as_str();
        let schema = simple_schema();
        let handler = Arc::new(AmbiguousCommitHandler::default());
        handler.fail_next(AmbiguousFailure::LandAndConflict);

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(
            vec![Ok(simple_batch(&schema, vec![1, 2, 3]))],
            schema.clone(),
        );
        let ds = Dataset::write(reader, uri, Some(params))
            .await
            .expect("creation whose commit landed must succeed");
        assert_eq!(ds.version().version, 1);
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);
    }

    /// Helper to build a simple manifest for check_column_indices tests.
    fn make_manifest_with_file(
        schema: Schema,
        data_file: DataFile,
        data_storage_version: LanceFileVersion,
    ) -> Manifest {
        let fragment = Fragment {
            id: 0,
            files: vec![data_file],
            overlays: vec![],
            deletion_file: None,
            row_id_meta: None,
            physical_rows: Some(100),
            last_updated_at_version_meta: None,
            created_at_version_meta: None,
        };
        Manifest::new(
            schema,
            Arc::new(vec![fragment]),
            DataStorageFormat::new(ConcreteFileVersion::from(data_storage_version)),
            HashMap::new(),
        )
    }

    #[test]
    fn test_check_column_indices_rejects_struct_with_column() {
        // Struct (non-leaf) field with column_index=0 in v2.1 should be rejected.
        let mut struct_field = Field::try_from(ArrowField::new(
            "s",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        ))
        .unwrap();
        struct_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![struct_field],
            metadata: Default::default(),
        };

        // field ids: struct=0, leaf=1; give struct a real column_index (wrong)
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![0, 1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        let result = check_column_indices(&manifest);
        assert!(
            result.is_err(),
            "Expected error for struct with column_index=0"
        );
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Non-leaf field"), "{msg}");
    }

    #[test]
    fn test_check_column_indices_rejects_list_with_column() {
        // List (non-leaf) field with column_index=0 in v2.1 should be rejected.
        let mut list_field = Field::try_from(ArrowField::new(
            "l",
            DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true))),
            false,
        ))
        .unwrap();
        list_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![list_field],
            metadata: Default::default(),
        };

        // field ids: list=0, item=1; give list a real column_index (wrong)
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![0, 1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        let result = check_column_indices(&manifest);
        assert!(
            result.is_err(),
            "Expected error for list with column_index=0"
        );
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Non-leaf field"), "{msg}");
    }

    #[test]
    fn test_check_column_indices_allows_correct_v21() {
        // Non-leaf with column_index=-1 and leaf with column_index>=0 should pass.
        let mut struct_field = Field::try_from(ArrowField::new(
            "s",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        ))
        .unwrap();
        struct_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![struct_field],
            metadata: Default::default(),
        };

        // struct=-1 (correct), leaf=0 (correct)
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![-1, 0],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        assert!(check_column_indices(&manifest).is_ok());
    }

    #[test]
    fn test_check_column_indices_allows_packed_struct() {
        // Packed struct with a real column_index in v2.1 should be allowed.
        let mut struct_field = Field::try_from(ArrowField::new(
            "s",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        ))
        .unwrap();
        struct_field.set_id(-1, &mut 0);
        struct_field
            .metadata
            .insert("lance-encoding:packed".to_string(), "true".to_string());

        let schema = Schema {
            fields: vec![struct_field],
            metadata: Default::default(),
        };

        // packed struct=0 (allowed), leaf=1
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![0, 1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        assert!(check_column_indices(&manifest).is_ok());
    }

    #[test]
    fn test_check_column_indices_skips_v20() {
        // Non-leaf with column_index>=0 in v2.0 should be allowed (no validation).
        let mut struct_field = Field::try_from(ArrowField::new(
            "s",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        ))
        .unwrap();
        struct_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![struct_field],
            metadata: Default::default(),
        };

        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![0, 1],
            ConcreteFileVersion::V2_0,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_0);
        assert!(check_column_indices(&manifest).is_ok());
    }

    #[test]
    fn test_check_column_indices_rejects_mismatched_lengths() {
        // fields and column_indices must have the same length.
        let mut leaf_field = Field::try_from(ArrowField::new("x", DataType::Int32, false)).unwrap();
        leaf_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![leaf_field],
            metadata: Default::default(),
        };

        // 1 field id but 2 column indices
        let data_file = DataFile::new(
            "data.lance",
            vec![0],
            vec![0, 1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        let result = check_column_indices(&manifest);
        assert!(result.is_err(), "Expected error for mismatched lengths");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("1 field ids but 2 column indices"), "{msg}");
    }

    #[test]
    fn test_check_column_indices_skips_unknown_field_id() {
        // A field id not present in the schema is skipped (schema evolution).
        let mut leaf_field = Field::try_from(ArrowField::new("x", DataType::Int32, false)).unwrap();
        leaf_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![leaf_field],
            metadata: Default::default(),
        };

        // field id 99 does not exist in the schema — should be skipped
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 99],
            vec![0, 1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        assert!(check_column_indices(&manifest).is_ok());
    }

    #[test]
    fn test_check_column_indices_rejects_leaf_with_negative_one() {
        // A leaf field with column_index=-1 in v2.1 should be rejected.
        let mut struct_field = Field::try_from(ArrowField::new(
            "s",
            DataType::Struct(vec![ArrowField::new("x", DataType::Int32, false)].into()),
            false,
        ))
        .unwrap();
        struct_field.set_id(-1, &mut 0);

        let schema = Schema {
            fields: vec![struct_field],
            metadata: Default::default(),
        };

        // struct=-1 (correct), but leaf=-1 (wrong — leaf must have a real column)
        let data_file = DataFile::new(
            "data.lance",
            vec![0, 1],
            vec![-1, -1],
            ConcreteFileVersion::V2_1,
            None,
            None,
        );
        let manifest = make_manifest_with_file(schema, data_file, LanceFileVersion::V2_1);
        let result = check_column_indices(&manifest);
        assert!(
            result.is_err(),
            "Expected error for leaf with column_index=-1"
        );
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("must have a valid column index"), "{msg}");
    }
}
