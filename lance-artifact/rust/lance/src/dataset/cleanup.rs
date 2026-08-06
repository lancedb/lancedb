// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A task to clean up a lance dataset, removing files that are no longer
//! needed.
//!
//! Currently we try and be rather conservative about what we delete.
//!
//! The following types of files may be deleted by the cleanup function:
//!
//! * Old manifest files - If a manifest file is older than the threshold
//!   and is not the latest manifest then it will be deleted.
//! * Unreferenced data files - If a data file is not referenced by any
//!   fragment in a valid manifest file then it will be deleted.
//! * Unreferenced delete files - If a delete file is not referenced by
//!   any fragment in a valid manifest file then it will be deleted.
//! * Unreferenced index files - If an index file is not referenced by
//!   any valid manifest file then it will be deleted.
//!
//! It is also difficult to distinguish between a data/tx/idx file which was
//! leftover from an abandoned transaction and a data file which is part
//! of an ongoing operation (both will look like unreferenced data files).
//!
//! If the file is referenced by at least one manifest (even if that manifest
//! is old and being deleted) then we assume it is not part of an ongoing
//! operation and can be safely deleted.
//!
//! If the data is not referenced by any manifest then we look at the age of
//! the file.  If the file is at least 7 days old then we assume it is probably
//! not part of any ongoing operation and we will delete it.
//!
//! Otherwise we will leave the file unless delete_unverified is set to true.
//! (which should only be done if the caller can guarantee there are no updates
//! happening at the same time)

use super::refs::TagContents;
use crate::dataset::TRANSACTIONS_DIR;
use crate::{Dataset, utils::temporal::utc_now};
use chrono::{DateTime, TimeDelta, Utc};
use dashmap::DashSet;
use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use humantime::parse_duration;
use lance_core::{
    Error, Result,
    utils::tracing::{
        AUDIT_MODE_DELETE, AUDIT_MODE_DELETE_UNVERIFIED, AUDIT_TYPE_DATA, AUDIT_TYPE_DELETION,
        AUDIT_TYPE_INDEX, AUDIT_TYPE_MANIFEST, DATASET_CLEANING_EVENT, TRACE_DATASET_EVENTS,
        TRACE_FILE_AUDIT,
    },
};
use lance_table::{
    format::{IndexMetadata, Manifest},
    io::{
        commit::ManifestLocation,
        deletion::deletion_file_path,
        manifest::{read_manifest, read_manifest_indexes},
    },
};
use object_store::ObjectMeta;
use object_store::path::Path;
use std::fmt::Debug;
use std::{
    collections::{HashMap, HashSet},
    future,
    sync::{Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::wrappers::IntervalStream;
use tracing::{Span, debug, info, instrument};

#[derive(Clone, Debug, Default)]
struct ReferencedFiles {
    data_paths: HashSet<Path>,
    delete_paths: HashSet<Path>,
    tx_paths: HashSet<Path>,
    index_uuids: HashSet<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemovalStats {
    pub bytes_removed: u64,
    pub old_versions: u64,
    pub data_files_removed: u64,
    pub transaction_files_removed: u64,
    pub index_files_removed: u64,
    pub deletion_files_removed: u64,
}

/// A read-only explanation of what a cleanup operation would remove.
///
/// This is an explanation, not a deletion plan.  Calling
/// [`CleanupOperation::execute`] re-evaluates the current dataset and reference
/// state before deleting files.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupExplanation {
    /// Dataset version observed when the explanation was produced.
    pub read_version: u64,
    /// Aggregate statistics for files that would be removed.
    pub stats: RemovalStats,
    /// Candidate files that would be removed, capped by `candidate_file_limit`.
    pub candidate_files: Vec<CleanupCandidateFile>,
    /// True if more candidate files were found than are included.
    pub candidate_files_truncated: bool,
    /// Maximum number of candidate files included in this explanation.
    pub candidate_file_limit: usize,
    /// Referenced child branches and whether cleanup would cascade into them.
    pub referenced_branches: Vec<CleanupReferencedBranch>,
    /// Non-fatal warnings about the explanation.
    pub warnings: Vec<String>,
}

/// A file that cleanup identified as removable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupCandidateFile {
    /// Dataset-relative or storage path for the candidate file.
    pub path: String,
    /// Kind of file identified by cleanup.
    pub kind: CleanupFileKind,
    /// True if the file is removable only because it aged past the unverified
    /// retention threshold or `delete_unverified` is enabled.
    pub unverified: bool,
    /// Candidate file size in bytes.
    pub size_bytes: u64,
}

/// A branch that references the current branch lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupReferencedBranch {
    /// Branch name.
    pub name: String,
    /// Version of the current lineage referenced by this branch.
    pub referenced_version: u64,
    /// True if this branch would be cleaned when cascading cleanup is enabled.
    pub cleanup_candidate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupFileKind {
    Manifest,
    Data,
    Transaction,
    Index,
    Deletion,
    /// A leftover `_versions/.tmp` manifest from a failed transaction.  These
    /// are deleted but excluded from per-kind `RemovalStats` counts and audit
    /// logs to match the long-standing cleanup behavior.  Their bytes
    /// are still included in `bytes_removed`.
    TemporaryManifest,
}

impl CleanupCandidateFile {
    fn from_cleanup_file(file: &CleanupFile) -> Self {
        Self {
            path: file.path.to_string(),
            kind: file.kind,
            unverified: file.unverified,
            size_bytes: file.size_bytes,
        }
    }
}

fn cleanup_file(
    path: Path,
    kind: CleanupFileKind,
    unverified: bool,
    size_bytes: u64,
) -> Option<CleanupFile> {
    Some(CleanupFile {
        path,
        kind,
        unverified,
        size_bytes,
    })
}

#[derive(Clone, Debug)]
struct CleanupFile {
    path: Path,
    kind: CleanupFileKind,
    /// True when the file was kept on disk past its referenced lifetime
    /// because we could not verify it was safe to remove (e.g. produced by an
    /// unfinished commit) and is being deleted only because it has aged past
    /// the unverified-retention threshold or `delete_unverified` is set.
    unverified: bool,
    size_bytes: u64,
}

impl RemovalStats {
    fn record_file(&mut self, file: &CleanupFile) {
        self.bytes_removed += file.size_bytes;
        match file.kind {
            CleanupFileKind::Manifest => self.old_versions += 1,
            CleanupFileKind::Data => self.data_files_removed += 1,
            CleanupFileKind::Transaction => self.transaction_files_removed += 1,
            CleanupFileKind::Index => self.index_files_removed += 1,
            CleanupFileKind::Deletion => self.deletion_files_removed += 1,
            CleanupFileKind::TemporaryManifest => {}
        }
    }

    fn merge(&mut self, other: &Self) {
        self.bytes_removed += other.bytes_removed;
        self.old_versions += other.old_versions;
        self.data_files_removed += other.data_files_removed;
        self.transaction_files_removed += other.transaction_files_removed;
        self.index_files_removed += other.index_files_removed;
        self.deletion_files_removed += other.deletion_files_removed;
    }
}

#[derive(Debug, Default)]
struct CleanupRunResult {
    stats: RemovalStats,
    removed_manifests: HashSet<Path>,
    candidate_files: Vec<CleanupCandidateFile>,
    candidate_files_truncated: bool,
    referenced_branches: Vec<CleanupReferencedBranch>,
}

impl CleanupRunResult {
    fn record_file(
        &mut self,
        file: &CleanupFile,
        candidate_file_limit: Option<usize>,
        track_removed_manifests: bool,
    ) {
        self.stats.record_file(file);
        if track_removed_manifests && matches!(file.kind, CleanupFileKind::Manifest) {
            self.removed_manifests.insert(file.path.clone());
        }
        if let Some(limit) = candidate_file_limit {
            if self.candidate_files.len() < limit {
                self.candidate_files
                    .push(CleanupCandidateFile::from_cleanup_file(file));
            } else {
                self.candidate_files_truncated = true;
            }
        }
    }

    fn merge(&mut self, other: Self, candidate_file_limit: Option<usize>) {
        self.stats.merge(&other.stats);
        self.removed_manifests.extend(other.removed_manifests);
        self.referenced_branches.extend(other.referenced_branches);
        if let Some(limit) = candidate_file_limit {
            for file in other.candidate_files {
                if self.candidate_files.len() < limit {
                    self.candidate_files.push(file);
                } else {
                    self.candidate_files_truncated = true;
                }
            }
            self.candidate_files_truncated |= other.candidate_files_truncated;
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CleanupAction {
    Execute,
    Explain { max_candidate_files: usize },
}

impl CleanupAction {
    fn deletes_files(self) -> bool {
        matches!(self, Self::Execute)
    }

    fn candidate_file_limit(self) -> Option<usize> {
        match self {
            Self::Execute => None,
            Self::Explain {
                max_candidate_files,
            } => Some(max_candidate_files),
        }
    }
}

fn remove_prefix(path: &Path, prefix: &Path) -> Path {
    let relative_parts = path.prefix_match(prefix);
    if relative_parts.is_none() {
        return path.clone();
    }
    Path::from_iter(relative_parts.unwrap())
}

#[derive(Clone, Debug)]
struct CleanupTask<'a> {
    dataset: &'a Dataset,
    policy: CleanupPolicy,
    action: CleanupAction,
    read_version: u64,
    ignored_manifests: HashSet<Path>,
    track_removed_manifests: bool,
    include_referenced_branches: bool,
}

/// Information about the dataset that we learn by inspecting all of the manifests
#[derive(Clone, Debug, Default)]
struct CleanupInspection {
    old_manifests: HashMap<Path, u64>,
    /// Referenced files are part of our working set
    referenced_files: ReferencedFiles,
    /// Verified files may or may not be part of the working set but they are
    /// referenced by at least one manifest file (potentially an old one) and
    /// so we know that they are not part of an ongoing operation.
    verified_files: ReferencedFiles,
    /// Track tagged old versions in case we want to raise a `CleanupError`.
    tagged_old_versions: HashSet<u64>,
    /// The earliest timestamp of all retained manifests.
    earliest_retained_manifest_time: Option<DateTime<Utc>>,
}

/// If a file cannot be verified then it will only be deleted if it is at least
/// this many days old.
const UNVERIFIED_THRESHOLD_DAYS: i64 = 7;
const S3_DELETE_STREAM_BATCH_SIZE: u64 = 1_000;
const AZURE_DELETE_STREAM_BATCH_SIZE: u64 = 256;
const DEFAULT_EXPLANATION_MAX_CANDIDATE_FILES: usize = 1_000;

/// Builder-style cleanup operation.
///
/// Call [`Self::explain`] for a read-only explanation of what cleanup would
/// remove, or [`Self::execute`] to re-evaluate the current dataset state and
/// delete files.
pub struct CleanupOperation<'a> {
    dataset: &'a Dataset,
    policy: CleanupPolicy,
    max_candidate_files: usize,
}

impl<'a> CleanupOperation<'a> {
    pub(crate) fn new(dataset: &'a Dataset, policy: CleanupPolicy) -> Self {
        Self {
            dataset,
            policy,
            max_candidate_files: DEFAULT_EXPLANATION_MAX_CANDIDATE_FILES,
        }
    }

    /// Set the maximum number of candidate files included in explanations.
    ///
    /// The aggregate [`RemovalStats`] in [`CleanupExplanation`] still include
    /// all files that would be removed.
    pub fn with_max_candidate_files(mut self, max_candidate_files: usize) -> Self {
        self.max_candidate_files = max_candidate_files;
        self
    }

    /// Explain what cleanup would remove without deleting files.
    pub async fn explain(&self) -> Result<CleanupExplanation> {
        let cleanup = CleanupTask::new(
            self.dataset,
            self.policy.clone(),
            CleanupAction::Explain {
                max_candidate_files: self.max_candidate_files,
            },
        );
        let read_version = cleanup.read_version;
        let result = cleanup.run().await?;
        let warnings = if result.candidate_files_truncated {
            vec![format!(
                "candidate_files truncated to {} entries",
                self.max_candidate_files
            )]
        } else {
            Vec::new()
        };
        Ok(CleanupExplanation {
            read_version,
            stats: result.stats,
            candidate_files: result.candidate_files,
            candidate_files_truncated: result.candidate_files_truncated,
            candidate_file_limit: self.max_candidate_files,
            referenced_branches: result.referenced_branches,
            warnings,
        })
    }

    /// Execute cleanup by re-evaluating the current dataset state.
    pub async fn execute(&self) -> Result<RemovalStats> {
        info!(target: TRACE_DATASET_EVENTS, event=DATASET_CLEANING_EVENT, uri=&self.dataset.uri);
        let cleanup = CleanupTask::new(self.dataset, self.policy.clone(), CleanupAction::Execute);
        Ok(cleanup.run().await?.stats)
    }
}

impl<'a> CleanupTask<'a> {
    fn new(dataset: &'a Dataset, policy: CleanupPolicy, action: CleanupAction) -> Self {
        let track_removed_manifests = policy.clean_referenced_branches;
        let include_referenced_branches = action.candidate_file_limit().is_some();
        Self::new_with_ignored_manifests(
            dataset,
            policy,
            action,
            HashSet::new(),
            track_removed_manifests,
            include_referenced_branches,
        )
    }

    fn new_with_ignored_manifests(
        dataset: &'a Dataset,
        policy: CleanupPolicy,
        action: CleanupAction,
        ignored_manifests: HashSet<Path>,
        track_removed_manifests: bool,
        include_referenced_branches: bool,
    ) -> Self {
        Self {
            dataset,
            policy,
            action,
            read_version: dataset.version().version,
            ignored_manifests,
            track_removed_manifests,
            include_referenced_branches,
        }
    }

    async fn run(self) -> Result<CleanupRunResult> {
        let mut final_result = CleanupRunResult::default();
        let candidate_file_limit = self.action.candidate_file_limit();
        // First check if we need to clean referenced branches
        // For cases that referenced branches never clean and the current cleanup cannot clean anything
        // This must happen before cleaning the current branch if the setting is enabled.

        let referenced_branches: Vec<(String, u64)> = self.find_referenced_branches().await?;
        if self.include_referenced_branches {
            final_result.referenced_branches = referenced_branches
                .iter()
                .map(|(name, referenced_version)| CleanupReferencedBranch {
                    name: name.clone(),
                    referenced_version: *referenced_version,
                    cleanup_candidate: self.policy.clean_referenced_branches,
                })
                .collect();
        }
        if self.policy.clean_referenced_branches {
            final_result.merge(
                self.clean_referenced_branches(&referenced_branches).await?,
                candidate_file_limit,
            );
        }

        // we process all manifest files in parallel to figure
        // out which files are referenced by valid manifests

        // get protected manifests first, and include those in process_manifests
        // pass on option to process manifests around whether to return error
        // or clean around the manifest
        let tags = self.dataset.tags().list().await?;
        let current_branch = &self.dataset.manifest.branch;

        // Only retain tags on the current branch.
        // Tags on other branches would take effect in retain_branch_lineage_files
        let tagged_versions: HashSet<u64> = tags
            .values()
            .filter(|tag| match (tag.branch.as_ref(), current_branch.as_ref()) {
                (Some(branch_of_tag), Some(current_branch)) => branch_of_tag == current_branch,
                (None, None) => true,
                _ => false,
            })
            .map(|tag_content| tag_content.version)
            .collect();

        let mut inspection = self.process_manifests(&tagged_versions).await?;

        if self.policy.error_if_tagged_old_versions && !inspection.tagged_old_versions.is_empty() {
            return Err(tagged_old_versions_cleanup_error(
                &tags,
                &inspection.tagged_old_versions,
            ));
        }

        if !referenced_branches.is_empty() {
            let ignored_manifests: HashSet<_> = final_result
                .removed_manifests
                .union(&self.ignored_manifests)
                .cloned()
                .collect();
            inspection = self
                .retain_branch_lineage_files(inspection, &referenced_branches, &ignored_manifests)
                .await?
        };

        final_result.merge(
            self.delete_unreferenced_files(inspection).await?,
            candidate_file_limit,
        );
        Ok(final_result)
    }

    #[instrument(level = "debug", skip_all)]
    async fn process_manifests(
        &'a self,
        tagged_versions: &HashSet<u64>,
    ) -> Result<CleanupInspection> {
        let inspection = Mutex::new(CleanupInspection::default());
        self.dataset
            .commit_handler
            .list_manifest_locations(&self.dataset.base, &self.dataset.object_store, false)
            .try_filter(|location| future::ready(!self.ignored_manifests.contains(&location.path)))
            .try_for_each_concurrent(self.dataset.object_store.io_parallelism(), |location| {
                self.process_manifest_file(location, &inspection, tagged_versions)
            })
            .await?;
        Ok(inspection.into_inner().unwrap())
    }

    async fn process_manifest_file(
        &self,
        location: ManifestLocation,
        inspection: &Mutex<CleanupInspection>,
        tagged_versions: &HashSet<u64>,
    ) -> Result<()> {
        // TODO: We can't cleanup invalid manifests.  There is no way to distinguish
        // between an invalid manifest and a temporary I/O error.  It's also not safe
        // to ignore a manifest error because if it is a temporary I/O error and we
        // ignore it then we might delete valid data files thinking they are not
        // referenced.

        let manifest_and_indexes = async {
            let manifest =
                read_manifest(&self.dataset.object_store, &location.path, location.size).await?;
            let indexes =
                read_manifest_indexes(&self.dataset.object_store, &location, &manifest).await?;
            Ok::<_, Error>((manifest, indexes))
        }
        .await;
        let (manifest, indexes) = match manifest_and_indexes {
            Ok(manifest_and_indexes) => manifest_and_indexes,
            Err(error) if location.version < self.read_version && error.is_not_found() => {
                // Another cleanup may remove an old manifest after this cleanup lists it.
                // The current manifest is never safe to skip because it anchors our snapshot.
                debug!(
                    manifest_version = location.version,
                    read_version = self.read_version,
                    manifest_path = %location.path,
                    "Skipping old manifest removed by concurrent cleanup"
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        // Don't delete the latest version, even if it is old. Don't delete tagged versions,
        // regardless of age. Don't delete manifests if their version is newer than the dataset
        // version.  These are either in-progress or newly added since we started.
        let is_latest = self.read_version <= manifest.version;
        let is_tagged = tagged_versions.contains(&manifest.version);
        let in_working_set = is_latest || !self.policy.should_clean(&manifest) || is_tagged;
        let mut inspection = inspection.lock().unwrap();

        // Track tagged old versions in case we want to return a `CleanupError` later.
        // Only track tagged when it is old.
        if is_tagged && !is_latest && self.policy.should_clean(&manifest) {
            inspection.tagged_old_versions.insert(manifest.version);
        }

        self.process_manifest(&manifest, &indexes, in_working_set, &mut inspection)?;
        if !in_working_set {
            inspection
                .old_manifests
                .insert(location.path.clone(), manifest.version);
        } else {
            let commit_ts = manifest.timestamp();
            if let Some(ts) = inspection.earliest_retained_manifest_time {
                if commit_ts < ts {
                    inspection.earliest_retained_manifest_time = Some(commit_ts);
                }
            } else {
                inspection.earliest_retained_manifest_time = Some(commit_ts);
            }
        }
        Ok(())
    }

    fn process_manifest(
        &self,
        manifest: &Manifest,
        indexes: &Vec<IndexMetadata>,
        in_working_set: bool,
        inspection: &mut MutexGuard<CleanupInspection>,
    ) -> Result<()> {
        // If this part of our working set then update referenced_files.  Otherwise, just mark the
        // file as verified.
        let referenced_files = if in_working_set {
            &mut inspection.referenced_files
        } else {
            &mut inspection.verified_files
        };

        for fragment in manifest.fragments.iter() {
            for file in fragment.files.iter() {
                let full_data_path = self.dataset.data_dir().clone().join(file.path.as_str());
                let relative_data_path = remove_prefix(&full_data_path, &self.dataset.base);
                referenced_files.data_paths.insert(relative_data_path);
            }
            let delpath = fragment
                .deletion_file
                .as_ref()
                .map(|delfile| deletion_file_path(&self.dataset.base, fragment.id, delfile));
            if let Some(delpath) = delpath {
                let relative_path = remove_prefix(&delpath, &self.dataset.base);
                referenced_files.delete_paths.insert(relative_path);
            }
        }
        if let Some(relative_tx_path) = &manifest.transaction_file {
            referenced_files
                .tx_paths
                .insert(Path::parse(TRANSACTIONS_DIR)?.join(relative_tx_path.as_str()));
        }

        for index in indexes {
            let uuid_str = index.uuid.to_string();
            referenced_files.index_uuids.insert(uuid_str);
        }
        Ok(())
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            old_versions = inspection.old_manifests.len(),
            bytes_removed = tracing::field::Empty,
            data_files_removed = tracing::field::Empty,
            transaction_files_removed = tracing::field::Empty,
            index_files_removed = tracing::field::Empty,
            deletion_files_removed = tracing::field::Empty
        )
    )]
    async fn delete_unreferenced_files(
        &self,
        inspection: CleanupInspection,
    ) -> Result<CleanupRunResult> {
        let cleanup_result = Mutex::new(CleanupRunResult::default());
        let deletes_files = self.action.deletes_files();
        let candidate_file_limit = self.action.candidate_file_limit();
        let verification_threshold = utc_now()
            - TimeDelta::try_days(UNVERIFIED_THRESHOLD_DAYS).expect("TimeDelta::try_days");

        let is_not_found_err = |e: &Error| matches!(e, Error::NotFound { .. });
        // Build stream for a managed subtree
        let build_listing_stream = |dir: Path| {
            let inspection_ref = &inspection;
            self.dataset
                .object_store
                .read_dir_all(&dir, inspection.earliest_retained_manifest_time)
                .map_ok(|obj| stream::once(future::ready(Ok(obj))).boxed())
                .or_else(|e| {
                    // If the directory doesn't exist then we can just return an empty stream.
                    if is_not_found_err(&e) {
                        future::ready(Ok(stream::empty::<Result<ObjectMeta>>().boxed()))
                    } else {
                        future::ready(Err(e))
                    }
                })
                .try_flatten()
                .try_filter_map(move |obj_meta| {
                    // If a file is new-ish then it might be part of an ongoing operation and so we only
                    // delete it if we can verify it is part of an old version.
                    let maybe_in_progress = !self.policy.delete_unverified
                        && obj_meta.last_modified >= verification_threshold;
                    let file_to_remove = self.cleanup_file_if_not_referenced(
                        obj_meta,
                        maybe_in_progress,
                        inspection_ref,
                    );
                    future::ready(file_to_remove)
                })
                .boxed()
        };

        // Restrict scanning to Lance-managed subtrees for safety and performance.
        let streams = vec![
            build_listing_stream(self.dataset.versions_dir()),
            build_listing_stream(self.dataset.transactions_dir()),
            build_listing_stream(self.dataset.data_dir()),
            build_listing_stream(self.dataset.indices_dir()),
            build_listing_stream(self.dataset.deletions_dir()),
        ];
        let unreferenced_files = stream::iter(streams).flatten().boxed();

        let old_manifests = inspection.old_manifests.clone();
        let manifest_files = stream::iter(old_manifests)
            .map(|(path, _version)| async move {
                let size_bytes = self.dataset.object_store.size(&path).await?;
                Ok::<CleanupFile, Error>(CleanupFile {
                    path,
                    kind: CleanupFileKind::Manifest,
                    unverified: false,
                    size_bytes,
                })
            })
            .buffer_unordered(self.dataset.object_store.io_parallelism())
            .boxed();

        let all_files = stream::iter(vec![unreferenced_files, manifest_files]).flatten();
        let all_paths_to_remove = all_files.map(|file| {
            let file = file?;
            if deletes_files {
                let mode = if file.unverified {
                    AUDIT_MODE_DELETE_UNVERIFIED
                } else {
                    AUDIT_MODE_DELETE
                };
                let path_str = file.path.as_ref();
                match file.kind {
                    CleanupFileKind::Manifest => {
                        info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_DELETE, r#type=AUDIT_TYPE_MANIFEST, path = path_str);
                    }
                    CleanupFileKind::Data => {
                        info!(target: TRACE_FILE_AUDIT, mode=mode, r#type=AUDIT_TYPE_DATA, path = path_str);
                    }
                    CleanupFileKind::Deletion => {
                        info!(target: TRACE_FILE_AUDIT, mode=mode, r#type=AUDIT_TYPE_DELETION, path = path_str);
                    }
                    CleanupFileKind::Index => {
                        info!(target: TRACE_FILE_AUDIT, mode=mode, r#type=AUDIT_TYPE_INDEX, path = path_str);
                    }
                    CleanupFileKind::Transaction | CleanupFileKind::TemporaryManifest => {}
                }
            }
            cleanup_result
                .lock()
                .unwrap()
                .record_file(&file, candidate_file_limit, self.track_removed_manifests);
            Ok(file.path)
        });

        if deletes_files {
            let paths_to_delete: BoxStream<Result<Path>> =
                if let Some(rate) = self.policy.delete_rate_limit {
                    let duration =
                        calculate_duration(self.dataset.object_store.scheme().to_string(), rate);
                    let mut ticker = interval(duration);
                    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    IntervalStream::new(ticker)
                        .zip(all_paths_to_remove)
                        .map(|(_, path)| path)
                        .boxed()
                } else {
                    all_paths_to_remove.boxed()
                };

            self.dataset
                .object_store
                .remove_stream(paths_to_delete)
                .try_for_each(|_| future::ready(Ok(())))
                .await?;
        } else {
            // Drain the stream to populate stats, but do not call remove_stream.
            all_paths_to_remove
                .try_for_each(|_| future::ready(Ok(())))
                .await?;
        }

        let cleanup_result = cleanup_result.into_inner().unwrap();

        let span = Span::current();
        span.record("bytes_removed", cleanup_result.stats.bytes_removed);
        span.record(
            "data_files_removed",
            cleanup_result.stats.data_files_removed,
        );
        span.record(
            "transaction_files_removed",
            cleanup_result.stats.transaction_files_removed,
        );
        span.record(
            "index_files_removed",
            cleanup_result.stats.index_files_removed,
        );
        span.record(
            "deletion_files_removed",
            cleanup_result.stats.deletion_files_removed,
        );

        Ok(cleanup_result)
    }

    fn cleanup_file_if_not_referenced(
        &self,
        obj_meta: ObjectMeta,
        maybe_in_progress: bool,
        inspection: &CleanupInspection,
    ) -> Result<Option<CleanupFile>> {
        let path = obj_meta.location;
        let relative_path = remove_prefix(&path, &self.dataset.base);
        let size_bytes = obj_meta.size;
        if relative_path.as_ref().starts_with("_versions/.tmp") {
            // This is a temporary manifest file.
            //
            // If the file is old (or the user has verified there are no writes in progress) then
            // it must be leftover from a failed tx.
            if maybe_in_progress {
                return Ok(None);
            } else {
                return Ok(cleanup_file(
                    path,
                    CleanupFileKind::TemporaryManifest,
                    true,
                    size_bytes,
                ));
            }
        }
        if relative_path.as_ref().starts_with("_indices") {
            // Indices are referenced by UUID so we need to examine the UUID
            // portion of the path.
            if let Some(uuid) = relative_path.parts().nth(1) {
                if inspection
                    .referenced_files
                    .index_uuids
                    .contains(uuid.as_ref())
                {
                    return Ok(None);
                } else if !maybe_in_progress {
                    return Ok(cleanup_file(path, CleanupFileKind::Index, true, size_bytes));
                } else if inspection
                    .verified_files
                    .index_uuids
                    .contains(uuid.as_ref())
                {
                    return Ok(cleanup_file(
                        path,
                        CleanupFileKind::Index,
                        false,
                        size_bytes,
                    ));
                }
            } else {
                return Ok(None);
            }
        }
        match path.extension() {
            Some("lance") => {
                if relative_path.as_ref().starts_with("data") {
                    if inspection
                        .referenced_files
                        .data_paths
                        .contains(&relative_path)
                    {
                        Ok(None)
                    } else if !maybe_in_progress {
                        Ok(cleanup_file(path, CleanupFileKind::Data, true, size_bytes))
                    } else if inspection
                        .verified_files
                        .data_paths
                        .contains(&relative_path)
                    {
                        Ok(cleanup_file(path, CleanupFileKind::Data, false, size_bytes))
                    } else {
                        Ok(None)
                    }
                } else {
                    // If a .lance file isn't in the data directory we err on the side of leaving it alone
                    Ok(None)
                }
            }
            Some("blob") => {
                // Blob v2 sidecar files are keyed by the data file stem:
                //   data/{data_file_key}/{obfuscated_blob_id:032b}.blob
                //
                // These files are not referenced directly by the manifest.  Instead, treat them
                // as referenced if their parent data file is referenced.
                if !relative_path.as_ref().starts_with("data") {
                    debug!(
                        path = relative_path.as_ref(),
                        "Will not garbage collect blob file because it does not follow convention"
                    );
                    return Ok(None);
                }

                let mut parts = relative_path.parts();
                let data_dir = parts.next();
                let data_file_key = parts.next();
                let blob_file = parts.next();
                // Be conservative: only handle the expected 3-part layout.
                if !matches!(data_dir, Some(dir) if dir.as_ref() == "data")
                    || data_file_key.is_none()
                    || blob_file.is_none()
                {
                    debug!(
                        path = relative_path.as_ref(),
                        "Will not garbage collect blob file because it does not follow convention"
                    );
                    return Ok(None);
                }
                if parts.next().is_some() {
                    debug!(
                        path = relative_path.as_ref(),
                        "Will not garbage collect blob file because it does not follow convention"
                    );
                    return Ok(None);
                }

                let data_file_key = data_file_key.expect("checked is_some");
                let Ok(parent_data_path) =
                    Path::parse(format!("data/{}.lance", data_file_key.as_ref()))
                else {
                    debug!(
                        path = relative_path.as_ref(),
                        derived_parent = format!("data/{}.lance", data_file_key.as_ref()),
                        "Will not garbage collect blob file because derived parent data file path is invalid"
                    );
                    return Ok(None);
                };

                if inspection
                    .referenced_files
                    .data_paths
                    .contains(&parent_data_path)
                {
                    Ok(None)
                } else if !maybe_in_progress {
                    Ok(cleanup_file(path, CleanupFileKind::Data, true, size_bytes))
                } else if inspection
                    .verified_files
                    .data_paths
                    .contains(&parent_data_path)
                {
                    Ok(cleanup_file(path, CleanupFileKind::Data, false, size_bytes))
                } else {
                    Ok(None)
                }
            }
            Some("manifest") => {
                // We already scanned the manifest files
                Ok(None)
            }
            Some("arrow") | Some("bin") => {
                if relative_path.as_ref().starts_with("_deletions") {
                    if inspection
                        .referenced_files
                        .delete_paths
                        .contains(&relative_path)
                    {
                        Ok(None)
                    } else if !maybe_in_progress {
                        Ok(cleanup_file(
                            path,
                            CleanupFileKind::Deletion,
                            true,
                            size_bytes,
                        ))
                    } else if inspection
                        .verified_files
                        .delete_paths
                        .contains(&relative_path)
                    {
                        Ok(cleanup_file(
                            path,
                            CleanupFileKind::Deletion,
                            false,
                            size_bytes,
                        ))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Some("txn") => {
                if relative_path.as_ref().starts_with(TRANSACTIONS_DIR) {
                    if inspection
                        .referenced_files
                        .tx_paths
                        .contains(&relative_path)
                    {
                        Ok(None)
                    } else if !maybe_in_progress
                        || inspection.verified_files.tx_paths.contains(&relative_path)
                    {
                        let unverified =
                            !inspection.verified_files.tx_paths.contains(&relative_path);
                        Ok(cleanup_file(
                            path,
                            CleanupFileKind::Transaction,
                            unverified,
                            size_bytes,
                        ))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    async fn find_referenced_branches(&self) -> Result<Vec<(String, u64)>> {
        let current_branch_id = self.dataset.branch_identifier().await?;
        let all_branches = self.dataset.branches().list().await?;
        let children = current_branch_id.collect_referenced_versions(&all_branches);

        // Use a concurrent set to identify branches eligible for cleanup.
        // The filter below preserves the original (branch_name, version) tuples.
        let referenced_branches: DashSet<String> = DashSet::new();
        let tasks: Vec<_> = children
            .iter()
            .map(|(branch_name, referenced_version)| {
                let dataset = &self.dataset;
                let policy = &self.policy;
                let referenced_branches = &referenced_branches;

                async move {
                    let manifest_location = dataset
                        .commit_handler
                        .resolve_version_location(
                            &dataset.base,
                            *referenced_version,
                            &dataset.object_store.inner,
                        )
                        .await?;

                    let manifest = read_manifest(
                        &dataset.object_store,
                        &manifest_location.path,
                        manifest_location.size,
                    )
                    .await;

                    if let Ok(manifest) = manifest
                        && policy.should_clean(&manifest)
                    {
                        referenced_branches.insert(branch_name.clone());
                    }
                    Ok::<(), Error>(())
                }
            })
            .collect();

        try_join_all(tasks).await?;

        // Filter children to only include branches that should be cleaned.
        // The DashSet contains branch names found eligible during concurrent scan.
        let referenced_branches = children
            .iter()
            .filter(|(branch_name, _)| referenced_branches.contains(branch_name))
            .cloned()
            .collect();
        Ok(referenced_branches)
    }

    async fn clean_referenced_branches(
        &self,
        referenced_branches: &[(String, u64)],
    ) -> Result<CleanupRunResult> {
        let final_result = Mutex::new(CleanupRunResult::default());

        // Group branches by their lineage identifier (BranchIdentifier).
        // Branches with the same identifier share a lineage and must be cleaned sequentially
        // to preserve cleanup order. Different lineages can be cleaned concurrently.
        let mut branches_chains = HashMap::new();
        for (branch, id) in referenced_branches {
            branches_chains
                .entry(*id)
                .or_insert_with(Vec::new)
                .push(branch.clone());
        }
        let action = self.action;
        let candidate_file_limit = self.action.candidate_file_limit();
        let tasks: Vec<_> = branches_chains
            .values()
            .map(|branch_chain| {
                let final_result = &final_result;
                async move {
                    for branch in branch_chain {
                        let branch_dataset = self
                            .dataset
                            .checkout_version((branch.as_str(), None))
                            .await?;
                        let ignored_manifests =
                            final_result.lock().unwrap().removed_manifests.clone();
                        if let Some(result) = cleanup_cascade_branch_run(
                            &branch_dataset,
                            branch_dataset.manifest.as_ref(),
                            action,
                            ignored_manifests,
                        )
                        .await?
                        {
                            final_result
                                .lock()
                                .unwrap()
                                .merge(result, candidate_file_limit);
                        }
                    }
                    Ok::<(), Error>(())
                }
            })
            .collect();
        try_join_all(tasks).await?;
        Ok(final_result.into_inner().unwrap())
    }

    // Retain manifests containing files referenced by descendant branches.
    // This protects parent branch files that are still needed by child branches.
    async fn retain_branch_lineage_files(
        &self,
        inspection: CleanupInspection,
        referenced_branches: &[(String, u64)],
        removed_branch_manifests: &HashSet<Path>,
    ) -> Result<CleanupInspection> {
        let inspection = Mutex::new(inspection);
        for (branch, root_version_number) in referenced_branches {
            // Use find_branch to get the branch path directly without checkout.
            // This avoids creating a dataset instance and prevents manifest deletion
            // during the retain operation.
            let branch_location = self.dataset.branch_location().find_branch(Some(branch))?;
            self.dataset
                .commit_handler
                .list_manifest_locations(&branch_location.path, &self.dataset.object_store, false)
                .try_filter(|location| {
                    future::ready(!removed_branch_manifests.contains(&location.path))
                })
                .try_for_each_concurrent(self.dataset.object_store.io_parallelism(), |location| {
                    self.process_branch_referenced_manifests(
                        location,
                        *root_version_number,
                        &inspection,
                    )
                })
                .await?;
        }
        Ok(inspection.into_inner().unwrap())
    }

    async fn process_branch_referenced_manifests(
        &self,
        location: ManifestLocation,
        referenced_version: u64,
        inspection: &Mutex<CleanupInspection>,
    ) -> Result<()> {
        let manifest =
            read_manifest(&self.dataset.object_store, &location.path, location.size).await?;
        let indexes =
            read_manifest_indexes(&self.dataset.object_store, &location, &manifest).await?;
        let mut inspection = inspection.lock().unwrap();
        let mut is_referenced = false;

        for fragment in manifest.fragments.iter() {
            for file in fragment.files.iter() {
                if let Some(base_id) = file.base_id {
                    let base_path = manifest.base_paths.get(&base_id);
                    if let Some(base_path) = base_path
                        && base_path.path == self.dataset.uri
                    {
                        let full_data_path =
                            self.dataset.data_dir().clone().join(file.path.as_str());
                        let relative_data_path = remove_prefix(&full_data_path, &self.dataset.base);
                        inspection
                            .verified_files
                            .data_paths
                            .remove(&relative_data_path);
                        inspection
                            .referenced_files
                            .data_paths
                            .insert(relative_data_path);
                        is_referenced = true;
                    }
                }
            }
            if let Some(del_file) = fragment.deletion_file.as_ref()
                && let Some(base_id) = del_file.base_id
            {
                let base_path = manifest.base_paths.get(&base_id);
                if let Some(base_path) = base_path {
                    let deletion_path = fragment.deletion_file.as_ref().map(|deletion_file| {
                        deletion_file_path(&self.dataset.base, fragment.id, deletion_file)
                    });
                    if base_path.path == self.dataset.uri {
                        if let Some(deletion_path) = deletion_path {
                            let relative_del_path =
                                remove_prefix(&deletion_path, &self.dataset.base);
                            inspection
                                .verified_files
                                .delete_paths
                                .remove(&relative_del_path);
                            inspection
                                .referenced_files
                                .delete_paths
                                .insert(relative_del_path);
                        }
                        is_referenced = true;
                    }
                }
            }
        }
        for index in indexes {
            if let Some(base_id) = index.base_id {
                let base_path = manifest.base_paths.get(&base_id);
                if let Some(base_path) = base_path
                    && base_path.path == self.dataset.uri
                {
                    let uuid_str = index.uuid.to_string();
                    inspection.verified_files.index_uuids.remove(&uuid_str);
                    inspection.referenced_files.index_uuids.insert(uuid_str);
                    is_referenced = true;
                }
            }
        }
        if is_referenced {
            inspection
                .old_manifests
                .retain(|_path, version_number| *version_number != referenced_version);
        }

        Ok(())
    }
}

fn calculate_duration(scheme: String, rate: u64) -> Duration {
    let batch_size = if scheme.to_lowercase().contains("s3") {
        S3_DELETE_STREAM_BATCH_SIZE
    } else if scheme.to_lowercase().contains("az") {
        AZURE_DELETE_STREAM_BATCH_SIZE
    } else {
        1
    };
    let effective_rate = rate.max(1);
    let path_rate = effective_rate * batch_size;
    info!(
        "delete_rate_limit enabled: limit {} delete requests/sec",
        effective_rate
    );
    // convert user given op/s to the rate of issuing paths
    let duration_ns = 1_000_000_000u64.div_ceil(path_rate).max(1);
    Duration::from_nanos(duration_ns)
}

#[derive(Clone, Debug)]
pub struct CleanupPolicy {
    /// If not none, cleanup all versions before the specified timestamp.
    pub before_timestamp: Option<DateTime<Utc>>,
    /// If not none, cleanup all versions before the specified version.
    pub before_version: Option<u64>,
    /// If true, delete unverified data files even if they are recent
    pub delete_unverified: bool,
    /// If true, return an Error if a tagged version is old
    pub error_if_tagged_old_versions: bool,
    /// If clean the referenced branches
    pub clean_referenced_branches: bool,
    /// Maximum number of delete requests per second. If None, no rate limiting is applied.
    ///
    /// Use this to avoid hitting S3 (or other object store) request rate limits during cleanup.
    /// On stores with bulk delete, each request can include multiple paths.
    /// For example, `Some(100)` limits deletions to 100 delete requests per second.
    pub delete_rate_limit: Option<u64>,
}

impl CleanupPolicy {
    pub fn should_clean(&self, manifest: &Manifest) -> bool {
        let mut should_clean = true;
        if let Some(before_timestamp) = self.before_timestamp {
            should_clean &= manifest.timestamp() < before_timestamp;
        }
        if let Some(before_version) = self.before_version {
            should_clean &= manifest.version < before_version;
        }
        should_clean
    }
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        Self {
            before_timestamp: None,
            before_version: None,
            delete_unverified: false,
            error_if_tagged_old_versions: true,
            clean_referenced_branches: false,
            delete_rate_limit: None,
        }
    }
}

#[derive(Default)]
pub struct CleanupPolicyBuilder {
    policy: CleanupPolicy,
}

impl CleanupPolicyBuilder {
    /// If auto clean referenced branches.
    pub fn clean_referenced_branches(mut self, clean_referenced_branches: bool) -> Self {
        self.policy.clean_referenced_branches = clean_referenced_branches;
        self
    }

    /// Cleanup all versions before the specified timestamp.
    pub fn before_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.policy.before_timestamp = Some(timestamp);
        self
    }

    /// Cleanup all versions except the last `n` versions of the dataset.
    pub async fn retain_n_versions(mut self, dataset: &Dataset, n: usize) -> Result<Self> {
        let versions = dataset.versions().await?;
        self.policy.before_version = if versions.len() <= n {
            Some(versions[0].version)
        } else {
            Some(versions[versions.len() - n].version)
        };

        Ok(self)
    }

    /// Delete without verification.
    ///
    /// By default, files will only be deleted if they are not referenced and are not in
    /// progress(at least 7 days old). Setting delete_unverified to true will not verify whether the
    /// file is in progress.
    /// This config is dangerous, only set to true when you are sure there are no other in-progress
    /// dataset operations.
    pub fn delete_unverified(mut self, delete: bool) -> Self {
        self.policy.delete_unverified = delete;
        self
    }

    /// If this argument True, an exception will be raised if any tagged versions match the
    /// parameters.
    pub fn error_if_tagged_old_versions(mut self, error: bool) -> Self {
        self.policy.error_if_tagged_old_versions = error;
        self
    }

    /// Limit the number of delete requests per second during cleanup.
    ///
    /// By default (None), deletions run at full speed. Set this to a positive value to
    /// throttle deletions and avoid hitting object store request rate limits (e.g. S3 HTTP 503).
    /// On backends with bulk delete APIs, effective path throughput scales with batch size.
    ///
    /// # Errors
    ///
    /// Returns an error if `rate` is zero.
    pub fn delete_rate_limit(mut self, rate: u64) -> Result<Self> {
        if rate == 0 {
            return Err(Error::Cleanup {
                message: format!("delete_rate_limit must be greater than 0, got {}", rate),
            });
        }
        self.policy.delete_rate_limit = Some(rate);
        Ok(self)
    }

    pub fn build(self) -> CleanupPolicy {
        self.policy
    }
}

/// Deletes old versions of a dataset, removing files that are no longer
/// needed.
///
/// This function will remove old manifest files, data files, indexes,
/// delete files, and transaction files.
///
/// It will only remove files that are not referenced by any valid manifest.
///
/// The latest manifest is always considered valid and will not be removed
/// even if it satisfied the cleanup policy.
pub async fn cleanup_old_versions(
    dataset: &Dataset,
    policy: CleanupPolicy,
) -> Result<RemovalStats> {
    CleanupOperation::new(dataset, policy).execute().await
}

/// If the dataset config has `lance.auto_cleanup` parameters set,
/// this function automatically calls `dataset.cleanup_old_versions`
/// every `lance.auto_cleanup.interval` versions. This function calls
/// `dataset.cleanup_old_versions` with `lance.auto_cleanup.older_than`
/// for `older_than` and `Some(false)` for both `delete_unverified` and
/// `error_if_tagged_old_versions`.
pub async fn auto_cleanup_hook(
    dataset: &Dataset,
    manifest: &Manifest,
) -> Result<Option<RemovalStats>> {
    let policy = build_cleanup_policy(dataset, manifest).await?;
    if let Some(policy) = policy {
        Ok(Some(dataset.cleanup_with_policy(policy).await?))
    } else {
        Ok(None)
    }
}

/// This is trigger when a parent branch is cleaning and `clean_referenced_branches` is set as true
/// For cascade branches, some cleanup parameters need be overridden.
pub async fn cleanup_cascade_branch(
    dataset: &Dataset,
    manifest: &Manifest,
) -> Result<Option<RemovalStats>> {
    Ok(
        cleanup_cascade_branch_run(dataset, manifest, CleanupAction::Execute, HashSet::new())
            .await?
            .map(|result| result.stats),
    )
}

async fn cleanup_cascade_branch_run(
    dataset: &Dataset,
    manifest: &Manifest,
    action: CleanupAction,
    ignored_manifests: HashSet<Path>,
) -> Result<Option<CleanupRunResult>> {
    let policy = build_cleanup_policy(dataset, manifest).await?;
    if let Some(mut policy) = policy {
        policy.clean_referenced_branches = false;
        policy.error_if_tagged_old_versions = false;
        if action.deletes_files() {
            info!(target: TRACE_DATASET_EVENTS, event=DATASET_CLEANING_EVENT, uri=&dataset.uri);
        }
        let cleanup = CleanupTask::new_with_ignored_manifests(
            dataset,
            policy,
            action,
            ignored_manifests,
            true,
            false,
        );
        Ok(Some(cleanup.run().await?))
    } else {
        Ok(None)
    }
}

pub async fn build_cleanup_policy(
    dataset: &Dataset,
    manifest: &Manifest,
) -> Result<Option<CleanupPolicy>> {
    if let Some(interval) = manifest.config.get("lance.auto_cleanup.interval") {
        let interval: u64 = match interval.parse() {
            Ok(i) => i,
            Err(e) => {
                return Err(Error::Cleanup {
                    message: format!(
                        "Error encountered while parsing lance.auto_cleanup.interval as u64: {}",
                        e
                    ),
                });
            }
        };

        if interval != 0 && !manifest.version.is_multiple_of(interval) {
            return Ok(None);
        }
    } else {
        return Ok(None);
    }

    let mut builder = CleanupPolicyBuilder::default();
    if let Some(older_than) = manifest.config.get("lance.auto_cleanup.older_than") {
        let std_older_than = match parse_duration(older_than) {
            Ok(t) => t,
            Err(e) => {
                return Err(Error::Cleanup {
                    message: format!(
                        "Error encountered while parsing lance.auto_cleanup.older_than as std::time::Duration: {}",
                        e
                    ),
                });
            }
        };
        let timestamp = utc_now() - TimeDelta::from_std(std_older_than).unwrap_or(TimeDelta::MAX);
        builder = builder.before_timestamp(timestamp);
    }
    if let Some(retain_versions) = manifest.config.get("lance.auto_cleanup.retain_versions") {
        let retain_versions: usize = match retain_versions.parse() {
            Ok(n) => n,
            Err(e) => {
                return Err(Error::Cleanup {
                    message: format!(
                        "Error encountered while parsing lance.auto_cleanup.retain_versions as u64: {}",
                        e
                    ),
                });
            }
        };
        builder = builder.retain_n_versions(dataset, retain_versions).await?;
    }
    if let Some(referenced_branch) = manifest.config.get("lance.auto_cleanup.referenced_branch") {
        let clean_referenced: bool = match referenced_branch.parse() {
            Ok(b) => b,
            Err(e) => {
                return Err(Error::Cleanup {
                    message: format!(
                        "Error encountered while parsing lance.auto_cleanup.referenced_branch as bool: {}",
                        e
                    ),
                });
            }
        };
        // Map config to policy flag controlling whether referenced branches are cleaned
        builder = builder.clean_referenced_branches(clean_referenced);
    }
    if let Some(delete_rate_limit) = manifest.config.get("lance.auto_cleanup.delete_rate_limit") {
        let rate: u64 = match delete_rate_limit.parse() {
            Ok(r) => r,
            Err(e) => {
                return Err(Error::Cleanup {
                    message: format!(
                        "Error encountered while parsing lance.auto_cleanup.delete_rate_limit as u64: {}",
                        e
                    ),
                });
            }
        };
        builder = match builder.delete_rate_limit(rate) {
            Ok(b) => b,
            Err(e) => return Err(e),
        };
    }

    Ok(Some(builder.build()))
}

fn tagged_old_versions_cleanup_error(
    tags: &HashMap<String, TagContents>,
    tagged_old_versions: &HashSet<u64>,
) -> Error {
    let unreferenced_tags: HashMap<String, u64> = tags
        .iter()
        .filter_map(|(k, v)| {
            if tagged_old_versions.contains(&v.version) {
                Some((k.clone(), v.version))
            } else {
                None
            }
        })
        .collect();

    Error::Cleanup {
        message: format!(
            "{} tagged version(s) have been marked for cleanup. Either set `error_if_tagged_old_versions=false` or delete the following tag(s) to enable cleanup: {:?}",
            unreferenced_tags.len(),
            unreferenced_tags
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::blob::{BlobArrayBuilder, blob_field};
    use crate::index::DatasetIndexExt;
    use crate::{
        dataset::transaction::{Operation, Transaction},
        dataset::{AutoCleanupParams, ReadParams, WriteMode, WriteParams, builder::DatasetBuilder},
        index::vector::VectorIndexParams,
    };
    use all_asserts::{assert_gt, assert_lt};
    use arrow::compute;
    use arrow_array::{
        Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::common::assert_contains;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_core::utils::testing::{ProxyObjectStore, ProxyObjectStorePolicy};
    use lance_index::IndexType;
    use lance_io::object_store::{
        ObjectStore, ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore,
    };
    use lance_linalg::distance::MetricType;
    use lance_table::io::commit::RenameCommitHandler;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector, some_batch};
    use mock_instant::thread_local::MockClock;
    use uuid::Uuid;

    #[derive(Debug)]
    struct MockObjectStore {
        policy: Arc<Mutex<ProxyObjectStorePolicy>>,
        last_modified_times: Arc<Mutex<HashMap<Path, DateTime<Utc>>>>,
    }

    impl WrappingObjectStore for MockObjectStore {
        fn wrap(
            &self,
            _storage_prefix: &str,
            original: Arc<dyn object_store::ObjectStore>,
        ) -> Arc<dyn object_store::ObjectStore> {
            Arc::new(ProxyObjectStore::new(original, self.policy.clone()))
        }
    }

    impl MockObjectStore {
        pub(crate) fn new() -> Self {
            let instance = Self {
                policy: Arc::new(Mutex::new(ProxyObjectStorePolicy::new())),
                last_modified_times: Arc::new(Mutex::new(HashMap::new())),
            };
            instance.add_timestamp_policy();
            instance
        }

        fn add_timestamp_policy(&self) {
            let mut policy = self.policy.lock().unwrap();
            let times_map = self.last_modified_times.clone();
            policy.set_before_policy(
                "record_file_time",
                Arc::new(move |_, path| {
                    let mut times_map = times_map.lock().unwrap();
                    times_map.insert(path.clone(), utc_now());
                    Ok(())
                }),
            );
            let times_map = self.last_modified_times.clone();
            policy.set_obj_meta_policy(
                "add_recorded_file_time",
                Arc::new(move |_, meta| {
                    let mut meta = meta;
                    if let Some(recorded) = times_map.lock().unwrap().get(&meta.location) {
                        meta.last_modified = *recorded;
                    }
                    Ok(meta)
                }),
            );
        }
    }

    #[derive(Debug, PartialEq, Clone, Copy)]
    struct FileCounts {
        num_data_files: usize,
        num_manifest_files: usize,
        num_index_files: usize,
        num_delete_files: usize,
        num_tx_files: usize,
        num_bytes: u64,
    }

    struct MockDatasetFixture {
        // This is a temporary directory that will be deleted when the fixture
        // is dropped
        _tmpdir: TempStrDir,
        dataset_path: String,
        mock_store: Arc<MockObjectStore>,
    }

    impl MockDatasetFixture {
        fn try_new() -> Result<Self> {
            let tmpdir = TempStrDir::default();
            let tmpdir_path = tmpdir.as_str();
            // Use file-object-store:// scheme so that writes go through the ObjectStore
            // wrapper chain (MockObjectStore) instead of the optimized local writer path.
            // The path must always start with "/" (three slashes after the scheme) so that
            // on Windows, a drive letter like "C:" isn't parsed as the URL authority.
            let path_prefix = if tmpdir_path.starts_with('/') {
                ""
            } else {
                "/"
            };
            let dataset_path = format!("file-object-store://{path_prefix}{tmpdir_path}/my_db");
            Ok(Self {
                _tmpdir: tmpdir,
                dataset_path,
                mock_store: Arc::new(MockObjectStore::new()),
            })
        }

        fn os_params(&self) -> ObjectStoreParams {
            ObjectStoreParams {
                object_store_wrapper: Some(self.mock_store.clone()),
                ..Default::default()
            }
        }

        async fn write_data_impl(
            &self,
            data: impl RecordBatchReader + Send + 'static,
            mode: WriteMode,
        ) -> Result<()> {
            Dataset::write(
                data,
                &self.dataset_path,
                Some(WriteParams {
                    store_params: Some(self.os_params()),
                    commit_handler: Some(Arc::new(RenameCommitHandler)),
                    mode,
                    ..Default::default()
                }),
            )
            .await?;
            Ok(())
        }

        async fn write_some_data_impl(&self, mode: WriteMode) -> Result<()> {
            self.write_data_impl(some_batch(), mode).await?;
            Ok(())
        }

        async fn create_some_data(&self) -> Result<()> {
            self.write_some_data_impl(WriteMode::Create).await
        }

        // Auto-cleanup is disabled by default; this helper creates a dataset
        // with auto-cleanup enabled using the default interval/older_than.
        async fn create_some_data_with_auto_cleanup(&self) -> Result<()> {
            Dataset::write(
                some_batch(),
                &self.dataset_path,
                Some(WriteParams {
                    store_params: Some(self.os_params()),
                    commit_handler: Some(Arc::new(RenameCommitHandler)),
                    mode: WriteMode::Create,
                    auto_cleanup: Some(AutoCleanupParams::default()),
                    ..Default::default()
                }),
            )
            .await?;
            Ok(())
        }

        async fn overwrite_some_data(&self) -> Result<()> {
            self.write_some_data_impl(WriteMode::Overwrite).await
        }

        async fn append_some_data(&self) -> Result<()> {
            self.write_some_data_impl(WriteMode::Append).await
        }

        async fn create_with_data(
            &self,
            data: impl RecordBatchReader + Send + 'static,
        ) -> Result<()> {
            self.write_data_impl(data, WriteMode::Create).await
        }

        async fn append_data(&self, data: impl RecordBatchReader + Send + 'static) -> Result<()> {
            self.write_data_impl(data, WriteMode::Append).await
        }

        async fn overwrite_data(
            &self,
            data: impl RecordBatchReader + Send + 'static,
        ) -> Result<()> {
            self.write_data_impl(data, WriteMode::Overwrite).await
        }

        async fn delete_data(&self, predicate: &str) -> Result<()> {
            let mut db = self.open().await?;
            db.delete(predicate).await?;
            Ok(())
        }

        async fn create_some_index(&self) -> Result<()> {
            let mut db = self.open().await?;
            let index_params = Box::new(VectorIndexParams::ivf_pq(2, 8, 2, MetricType::L2, 5));
            db.create_index(
                &["indexable"],
                IndexType::Vector,
                Some("some_index".to_owned()),
                &*index_params,
                false,
            )
            .await?;
            Ok(())
        }

        fn block_commits(&mut self) {
            let mut policy = self.mock_store.policy.lock().unwrap();
            policy.set_before_policy(
                "block_commit",
                Arc::new(|op, _| -> Result<()> {
                    if op.contains("copy") || op.contains("rename") {
                        return Err(Error::internal("Commit blocked".to_string()));
                    }
                    Ok(())
                }),
            );
        }

        fn block_delete_manifest(&mut self) {
            let mut policy = self.mock_store.policy.lock().unwrap();
            policy.set_before_policy(
                "block_delete_manifest",
                Arc::new(|op, path| -> Result<()> {
                    if op.contains("delete") && path.extension() == Some("manifest") {
                        Err(Error::internal("Delete manifest blocked".to_string()))
                    } else {
                        Ok(())
                    }
                }),
            );
        }

        fn unblock_delete_manifest(&mut self) {
            let mut policy = self.mock_store.policy.lock().unwrap();
            policy.clear_before_policy("block_delete_manifest");
        }

        async fn run_cleanup(&self, before: DateTime<Utc>) -> Result<RemovalStats> {
            let db = self.open().await?;
            cleanup_old_versions(
                &db,
                CleanupPolicyBuilder::default()
                    .before_timestamp(before)
                    .build(),
            )
            .await
        }

        async fn run_cleanup_with_policy(&self, policy: CleanupPolicy) -> Result<RemovalStats> {
            let db = self.open().await?;
            cleanup_old_versions(&db, policy).await
        }

        async fn explain_cleanup_with_policy(
            &self,
            policy: CleanupPolicy,
        ) -> Result<CleanupExplanation> {
            let db = self.open().await?;
            db.cleanup(policy).explain().await
        }

        async fn run_cleanup_with_override(
            &self,
            before: DateTime<Utc>,
            delete_unverified: Option<bool>,
            error_if_tagged_old_versions: Option<bool>,
        ) -> Result<RemovalStats> {
            let db = self.open().await?;
            cleanup_old_versions(
                &db,
                CleanupPolicyBuilder::default()
                    .before_timestamp(before)
                    .delete_unverified(delete_unverified.unwrap_or(false))
                    .error_if_tagged_old_versions(error_if_tagged_old_versions.unwrap_or(true))
                    .build(),
            )
            .await
        }

        async fn open(&self) -> Result<Box<Dataset>> {
            let ds = DatasetBuilder::from_uri(&self.dataset_path)
                .with_read_params(ReadParams {
                    store_options: Some(self.os_params()),
                    ..Default::default()
                })
                .load()
                .await?;
            Ok(Box::new(ds))
        }

        // Load the fixture's dataset.
        async fn load(&self) -> Result<Dataset> {
            self.load_dataset(&self.dataset_path).await
        }

        // Helper to load a dataset with the mock store configured.
        async fn load_dataset(&self, uri: &str) -> Result<Dataset> {
            DatasetBuilder::from_uri(uri)
                .with_read_params(ReadParams {
                    store_options: Some(self.os_params()),
                    ..Default::default()
                })
                .load()
                .await
        }

        // Helper to create a branch and load it as a Dataset.
        async fn create_branch_and_load<V: Into<crate::dataset::refs::Ref>>(
            &self,
            from_dataset: &mut Dataset,
            branch_name: &str,
            source_ref: V,
        ) -> Result<Dataset> {
            let branch_ds = from_dataset
                .create_branch(branch_name, source_ref, Some(self.os_params()))
                .await?;
            self.load_dataset(&branch_ds.uri).await
        }

        async fn count_files(&self) -> Result<FileCounts> {
            let registry = Arc::new(ObjectStoreRegistry::default());
            let (os, path) =
                ObjectStore::from_uri_and_params(registry, &self.dataset_path, &self.os_params())
                    .await?;
            let mut file_stream = os.read_dir_all(&path, None);
            let mut file_count = FileCounts {
                num_data_files: 0,
                num_delete_files: 0,
                num_index_files: 0,
                num_manifest_files: 0,
                num_tx_files: 0,
                num_bytes: 0,
            };
            while let Some(path) = file_stream.try_next().await? {
                file_count.num_bytes += path.size;
                match path.location.extension() {
                    Some("lance") => file_count.num_data_files += 1,
                    Some("manifest") => file_count.num_manifest_files += 1,
                    Some("arrow") | Some("bin") => file_count.num_delete_files += 1,
                    Some("idx") => file_count.num_index_files += 1,
                    Some("txn") => file_count.num_tx_files += 1,
                    _ => (),
                }
            }
            Ok(file_count)
        }

        async fn count_blob_files(&self) -> Result<usize> {
            let registry = Arc::new(ObjectStoreRegistry::default());
            let (os, path) =
                ObjectStore::from_uri_and_params(registry, &self.dataset_path, &self.os_params())
                    .await?;
            let mut file_stream = os.read_dir_all(&path, None);
            let mut blob_count = 0usize;
            while let Some(path) = file_stream.try_next().await? {
                if path.location.extension() == Some("blob") {
                    blob_count += 1;
                }
            }
            Ok(blob_count)
        }

        async fn count_rows(&self) -> Result<usize> {
            let db = self.open().await?;
            let count = db.count_rows(None).await?;
            Ok(count)
        }
    }

    async fn write_dummy_index_artifact(dataset: &Dataset, uuid: Uuid) -> Result<()> {
        let index_dir = dataset.indices_dir().join(uuid.to_string());
        dataset
            .object_store
            .as_ref()
            .put(&index_dir.clone().join("index.idx"), b"idx")
            .await?;
        dataset
            .object_store
            .as_ref()
            .put(&index_dir.clone().join("auxiliary.idx"), b"aux")
            .await?;
        Ok(())
    }

    async fn write_dummy_staging_partial(
        dataset: &Dataset,
        staging_uuid: Uuid,
        shard_uuid: Uuid,
    ) -> Result<()> {
        let shard_dir = dataset
            .indices_dir()
            .join(staging_uuid.to_string())
            .join(format!("partial_{}", shard_uuid));
        dataset
            .object_store
            .as_ref()
            .put(&shard_dir.clone().join("index.idx"), b"idx")
            .await?;
        dataset
            .object_store
            .as_ref()
            .put(&shard_dir.clone().join("auxiliary.idx"), b"aux")
            .await?;
        Ok(())
    }

    fn dummy_index_metadata(
        dataset: &Dataset,
        field_id: i32,
        uuid: Uuid,
        fragment_bitmap: impl IntoIterator<Item = u32>,
    ) -> IndexMetadata {
        IndexMetadata {
            uuid,
            name: "some_index".to_string(),
            fields: vec![field_id],
            dataset_version: dataset.version().version,
            fragment_bitmap: Some(fragment_bitmap.into_iter().collect()),
            index_details: None,
            index_version: IndexType::Vector.version(),
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    fn blob_v2_batch(blob_len: usize) -> Box<dyn RecordBatchReader + Send> {
        let mut blobs = BlobArrayBuilder::new(1);
        blobs.push_bytes(vec![0u8; blob_len]).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            blob_field("blob", true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])), blobs.finish().unwrap()],
        )
        .unwrap();

        Box::new(RecordBatchIterator::new(
            vec![Ok(batch)].into_iter(),
            schema,
        ))
    }

    #[tokio::test]
    async fn cleanup_unreferenced_data_files() {
        // We should clean up data files that are only referenced
        // by old versions.  This can happen, for example, due to
        // an overwrite
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let before_count = fixture.count_files().await.unwrap();

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 1);
        assert_eq!(removed.data_files_removed, 1);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        // There should be one less data file
        assert_lt!(after_count.num_data_files, before_count.num_data_files);
        // And one less manifest file
        assert_lt!(
            after_count.num_manifest_files,
            before_count.num_manifest_files
        );
        assert_lt!(after_count.num_tx_files, before_count.num_tx_files);

        assert_gt!(after_count.num_manifest_files, 0);
        assert_gt!(after_count.num_data_files, 0);
        // We should keep referenced tx files
        assert_gt!(after_count.num_tx_files, 0);
    }

    #[tokio::test]
    async fn cleanup_ignores_old_manifest_removed_after_listing() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        let dataset = fixture.open().await.unwrap();

        let old_manifest = dataset
            .commit_handler
            .list_manifest_locations(&dataset.base, &dataset.object_store, false)
            .try_filter(|location| future::ready(location.version == 1))
            .try_next()
            .await
            .unwrap()
            .unwrap();
        dataset
            .object_store
            .delete(&old_manifest.path)
            .await
            .unwrap();

        let cleanup = CleanupTask::new(
            &dataset,
            CleanupPolicyBuilder::default().build(),
            CleanupAction::Execute,
        );
        cleanup
            .process_manifest_file(
                old_manifest,
                &Mutex::new(CleanupInspection::default()),
                &HashSet::new(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn explain_cleanup_does_not_delete_files() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        MockClock::set_system_time(TimeDelta::try_seconds(1).unwrap().to_std().unwrap());
        fixture.overwrite_some_data().await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        let policy = CleanupPolicyBuilder::default()
            .before_timestamp(utc_now())
            .build();

        let explanation = fixture
            .explain_cleanup_with_policy(policy.clone())
            .await
            .unwrap();
        let after_preview_count = fixture.count_files().await.unwrap();

        // Files are not actually removed when explaining cleanup.
        assert_eq!(before_count, after_preview_count);
        assert_eq!(explanation.read_version, 2);
        assert_eq!(explanation.stats.old_versions, 1);
        assert_eq!(explanation.stats.data_files_removed, 1);
        assert_eq!(explanation.stats.transaction_files_removed, 1);
        assert_gt!(explanation.stats.bytes_removed, 0);
        assert!(!explanation.candidate_files.is_empty());
        assert!(!explanation.candidate_files_truncated);

        // Running cleanup with the same policy should remove the same files the
        // explanation reported for this unchanged dataset.
        let removed = fixture.run_cleanup_with_policy(policy).await.unwrap();
        let after_cleanup_count = fixture.count_files().await.unwrap();

        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_cleanup_count.num_bytes
        );
        assert_eq!(removed.old_versions, explanation.stats.old_versions);
        assert_eq!(
            removed.data_files_removed,
            explanation.stats.data_files_removed
        );
        assert_eq!(removed.bytes_removed, explanation.stats.bytes_removed);
    }

    #[tokio::test]
    async fn cleanup_blob_v2_sidecar_files() {
        let fixture = MockDatasetFixture::try_new().unwrap();

        // First version: write a packed blob (sidecar .blob file).
        Dataset::write(
            blob_v2_batch(100 * 1024),
            &fixture.dataset_path,
            Some(WriteParams {
                store_params: Some(fixture.os_params()),
                commit_handler: Some(Arc::new(RenameCommitHandler)),
                mode: WriteMode::Create,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_gt!(fixture.count_blob_files().await.unwrap(), 0);

        // Second version: overwrite with an inline blob (no sidecar).
        Dataset::write(
            blob_v2_batch(1024),
            &fixture.dataset_path,
            Some(WriteParams {
                store_params: Some(fixture.os_params()),
                commit_handler: Some(Arc::new(RenameCommitHandler)),
                mode: WriteMode::Overwrite,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Advance time so the unverified threshold doesn't interfere.
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .unwrap();

        assert_eq!(fixture.count_blob_files().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cleanup_recent_blob_v2_sidecar_files_when_verified() {
        let fixture = MockDatasetFixture::try_new().unwrap();

        Dataset::write(
            blob_v2_batch(100 * 1024),
            &fixture.dataset_path,
            Some(WriteParams {
                store_params: Some(fixture.os_params()),
                commit_handler: Some(Arc::new(RenameCommitHandler)),
                mode: WriteMode::Create,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        Dataset::write(
            blob_v2_batch(1024),
            &fixture.dataset_path,
            Some(WriteParams {
                store_params: Some(fixture.os_params()),
                commit_handler: Some(Arc::new(RenameCommitHandler)),
                mode: WriteMode::Overwrite,
                data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Old version is verified (referenced by an old manifest) even though the files are
        // recent; cleanup should remove them without waiting 7 days.
        fixture
            .run_cleanup(utc_now() + TimeDelta::seconds(1))
            .await
            .unwrap();

        assert_eq!(fixture.count_blob_files().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn do_not_cleanup_newer_data() {
        // Even though an old manifest is removed the data files should
        // remain if they are still referenced by newer manifests
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.append_some_data().await.unwrap();
        fixture.append_some_data().await.unwrap();

        let before_count = fixture.count_files().await.unwrap();

        // 3 versions
        assert_eq!(before_count.num_data_files, 3);
        assert_eq!(before_count.num_manifest_files, 3);

        let before = utc_now() - TimeDelta::try_days(7).unwrap();
        let removed = fixture.run_cleanup(before).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();

        assert_eq!(removed.old_versions, 1);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        // The data files should all remain since they are referenced by
        // the latest version
        assert_eq!(after_count.num_data_files, 3);
        // Only the oldest manifest file should be removed
        assert_eq!(after_count.num_manifest_files, 2);
        assert_eq!(after_count.num_tx_files, 2);
    }

    #[tokio::test]
    async fn cleanup_error_when_tagged_old_versions() {
        // We should not clean up old versions that are tagged.
        // This tests when `error_if_tagged_old_version=true`.
        // When `true`, no files should be cleaned and a `Error::CleanupError`
        // should be returned.
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();

        let dataset = *(fixture.open().await.unwrap());

        dataset.tags().create("old-tag", 1).await.unwrap();
        dataset.tags().create("another-old-tag", 2).await.unwrap();

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(20).unwrap())
            .await
            .unwrap();
        assert_eq!(removed.old_versions, 0);

        let mut cleanup_error = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .err()
            .unwrap();
        assert_contains!(
            cleanup_error.to_string(),
            "Cleanup error: 2 tagged version(s) have been marked for cleanup. Either set `error_if_tagged_old_versions=false` or delete the following tag(s) to enable cleanup:"
        );

        dataset.tags().delete("old-tag").await.unwrap();

        cleanup_error = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .err()
            .unwrap();
        assert_contains!(
            cleanup_error.to_string(),
            "Cleanup error: 1 tagged version(s) have been marked for cleanup. Either set `error_if_tagged_old_versions=false` or delete the following tag(s) to enable cleanup:"
        );

        dataset.tags().delete("another-old-tag").await.unwrap();

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .unwrap();
        assert_eq!(removed.old_versions, 2);
    }

    #[tokio::test]
    async fn cleanup_around_tagged_old_versions() {
        // We should not clean up old versions that are tagged.
        // This tests when `error_if_tagged_old_version=false`.
        // When `false`, old versions should be cleaned up except
        // latest and those that are tagged.
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();

        let dataset = *(fixture.open().await.unwrap());

        dataset.tags().create("old-tag", 1).await.unwrap();
        dataset.tags().create("another-old-tag", 2).await.unwrap();
        dataset.tags().create("tag-latest", 3).await.unwrap();

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let mut removed = fixture
            .run_cleanup_with_override(
                utc_now() - TimeDelta::try_days(8).unwrap(),
                None,
                Some(false),
            )
            .await
            .unwrap();

        assert_eq!(removed.old_versions, 0);

        dataset.tags().delete("old-tag").await.unwrap();

        removed = fixture
            .run_cleanup_with_override(
                utc_now() - TimeDelta::try_days(8).unwrap(),
                None,
                Some(false),
            )
            .await
            .unwrap();
        assert_eq!(removed.old_versions, 1);

        dataset.tags().delete("another-old-tag").await.unwrap();

        removed = fixture
            .run_cleanup_with_override(
                utc_now() - TimeDelta::try_days(8).unwrap(),
                None,
                Some(false),
            )
            .await
            .unwrap();

        assert_eq!(removed.old_versions, 1);
    }

    // Helper function to check that the number of files is correct.
    async fn check_num_files(fixture: &MockDatasetFixture, num_expected_files: usize) {
        let file_count = fixture.count_files().await.unwrap();

        assert_eq!(file_count.num_data_files, num_expected_files);
        assert_eq!(file_count.num_manifest_files, num_expected_files);
        assert_eq!(file_count.num_tx_files, num_expected_files);
    }

    #[tokio::test]
    async fn auto_cleanup_old_versions() {
        // Every n commits, all versions older than T should be deleted.
        //
        // We first make many commits and check that all of the versions are
        // present. We then wait until the "older_than" period has elapsed and
        // make many more commits. We check that, without explicitly calling
        // `fixture.run_cleanup`, the old versions are automatically cleaned
        // up and only the new ones remain. File counts are made after every
        // commit.
        let fixture = MockDatasetFixture::try_new().unwrap();

        fixture.create_some_data_with_auto_cleanup().await.unwrap();

        let dataset_config = &fixture.open().await.unwrap().manifest.config;
        let cleanup_interval: usize = dataset_config
            .get("lance.auto_cleanup.interval")
            .unwrap()
            .parse()
            .unwrap();

        let cleanup_older_than = TimeDelta::from_std(
            parse_duration(dataset_config.get("lance.auto_cleanup.older_than").unwrap()).unwrap(),
        )
        .unwrap();

        // First, write many files within the "older_than" window. Check that
        // no files are automatically cleaned up.
        for num_expected_files in 2..2 * cleanup_interval {
            fixture.overwrite_some_data().await.unwrap();
            check_num_files(&fixture, num_expected_files).await;
        }

        // Fast forward so we are outside of the "older_than" window.
        MockClock::set_system_time(
            (cleanup_older_than + TimeDelta::minutes(1))
                .to_std()
                .unwrap(),
        );

        // Write more files and check that those outside of the "older_than" window
        // are cleaned up.
        for num_expected_files in 2..cleanup_interval {
            fixture.overwrite_some_data().await.unwrap();
            check_num_files(&fixture, num_expected_files).await;
        }

        // Overwrite auto cleanup params with custom values
        let mut dataset = *(fixture.open().await.unwrap());
        let mut new_autoclean_params = HashMap::new();

        let new_cleanup_older_than_str = "1month 2days 2h 42min 6sec";
        let new_cleanup_older_than =
            TimeDelta::from_std(parse_duration(new_cleanup_older_than_str).unwrap()).unwrap();
        new_autoclean_params.insert(
            "lance.auto_cleanup.older_than".to_string(),
            new_cleanup_older_than_str.to_string(),
        );

        let new_cleanup_interval = 5;
        new_autoclean_params.insert(
            "lance.auto_cleanup.interval".to_string(),
            new_cleanup_interval.to_string(),
        );

        // Convert to new API format
        let config_updates = new_autoclean_params
            .into_iter()
            .map(|(k, v)| (k, Some(v)))
            .collect::<HashMap<String, Option<String>>>();
        dataset.update_config(config_updates).await.unwrap();

        // Fast forward so we are outside of the new "older_than" window.
        MockClock::set_system_time(
            (cleanup_older_than + new_cleanup_older_than + TimeDelta::minutes(2))
                .to_std()
                .unwrap(),
        );

        fixture.overwrite_some_data().await.unwrap();

        for num_expected_files in 2..new_cleanup_interval {
            fixture.overwrite_some_data().await.unwrap();
            check_num_files(&fixture, num_expected_files).await;
        }
    }

    #[tokio::test]
    async fn test_auto_cleanup_interval_zero() {
        let fixture = MockDatasetFixture::try_new().unwrap();

        fixture.create_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        check_num_files(&fixture, 3).await;

        let mut dataset = fixture.open().await.unwrap();
        let mut config_updates = HashMap::new();
        config_updates.insert(
            "lance.auto_cleanup.interval".to_string(),
            Some("0".to_string()),
        );
        config_updates.insert(
            "lance.auto_cleanup.retain_versions".to_string(),
            Some("1".to_string()),
        );
        dataset
            .update_config(config_updates)
            .replace()
            .await
            .unwrap();

        fixture.overwrite_some_data().await.unwrap();
        fixture.overwrite_some_data().await.unwrap();
        // The last version before the new commit is retained, means we have 2 versions to assert
        check_num_files(&fixture, 2).await;

        fixture.overwrite_some_data().await.unwrap();
        check_num_files(&fixture, 2).await;
    }

    #[tokio::test]
    async fn cleanup_recent_verified_files() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        MockClock::set_system_time(TimeDelta::try_seconds(1).unwrap().to_std().unwrap());
        fixture.overwrite_some_data().await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count.num_data_files, 2);
        assert_eq!(before_count.num_manifest_files, 2);

        // Not much time has passed but we can still delete the old manifest
        // and the related data files
        let before = utc_now();
        let removed = fixture.run_cleanup(before).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 1);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_data_files, 1);
        assert_eq!(after_count.num_manifest_files, 1);
    }

    #[tokio::test]
    async fn dont_cleanup_recent_unverified_files() {
        for (override_opt, old_files) in [
            (Some(false), false), // User provides false, files are new - do not delete
            (Some(true), false),  // User provides true, files are new - delete
            (None, true),         // Default, files are old - delete
            (None, false),        // Default, files are new - do not delete
        ] {
            MockClock::set_system_time(std::time::Duration::from_secs(0));
            let mut fixture = MockDatasetFixture::try_new().unwrap();
            fixture.create_some_data().await.unwrap();
            fixture.block_commits();
            assert!(fixture.append_some_data().await.is_err());

            let age = if old_files {
                TimeDelta::try_days(UNVERIFIED_THRESHOLD_DAYS + 1).unwrap()
            } else {
                TimeDelta::try_days(UNVERIFIED_THRESHOLD_DAYS - 1).unwrap()
            };
            MockClock::set_system_time(age.to_std().unwrap());

            // The above created some unreferenced data files but, since they
            // are not referenced in any manifest, and 7 days has not passed, we
            // cannot safely delete them unless the user overrides the safety check

            let before_count = fixture.count_files().await.unwrap();
            assert_eq!(before_count.num_data_files, 2);
            assert_eq!(before_count.num_manifest_files, 1);

            let before = utc_now();
            let removed = fixture
                .run_cleanup_with_override(before, override_opt, None)
                .await
                .unwrap();

            let should_delete = override_opt.unwrap_or(false) || old_files;

            let after_count = fixture.count_files().await.unwrap();
            assert_eq!(removed.old_versions, 0);
            assert_eq!(
                removed.bytes_removed,
                before_count.num_bytes - after_count.num_bytes
            );

            if should_delete {
                assert_gt!(removed.bytes_removed, 0);
            } else {
                assert_eq!(removed.bytes_removed, 0);
            }
        }
    }

    #[tokio::test]
    async fn cleanup_old_index() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.create_some_index().await.unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.overwrite_some_data().await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        // we store 2 files (index and quantized storage) for each index
        assert_eq!(before_count.num_index_files, 2);
        // Two user data files
        assert_eq!(before_count.num_data_files, 2);
        // Creating an index creates a new manifest so there are 3 total
        assert_eq!(before_count.num_manifest_files, 3);

        let before = utc_now() - TimeDelta::try_days(8).unwrap();
        let removed = fixture.run_cleanup(before).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 2);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_index_files, 0);
        assert_eq!(after_count.num_data_files, 1);
        assert_eq!(after_count.num_manifest_files, 1);
        assert_eq!(after_count.num_tx_files, 1);
    }

    #[tokio::test]
    async fn clean_old_delete_files() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        let mut data_gen = BatchGenerator::new().col(Box::new(
            IncrementingInt32::new().named("filter_me".to_owned()),
        ));

        fixture.create_with_data(data_gen.batch(16)).await.unwrap();
        fixture.append_data(data_gen.batch(16)).await.unwrap();
        // This will keep some data from the appended file and should
        // completely remove the first file
        fixture.delete_data("filter_me < 20").await.unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.overwrite_data(data_gen.batch(16)).await.unwrap();
        // This will delete half of the last fragment
        fixture.delete_data("filter_me >= 40").await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count.num_data_files, 3);
        assert_eq!(before_count.num_delete_files, 2);
        assert_eq!(before_count.num_manifest_files, 5);

        let before = utc_now() - TimeDelta::try_days(8).unwrap();
        let removed = fixture.run_cleanup(before).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 3);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_data_files, 1);
        assert_eq!(after_count.num_delete_files, 1);
        assert_eq!(after_count.num_manifest_files, 2);
        assert_eq!(after_count.num_tx_files, 2);

        // Ensure we can still read the dataset
        let row_count_after = fixture.count_rows().await.unwrap();
        assert_eq!(row_count_after, 8);
    }

    #[tokio::test]
    async fn cleanup_collects_removed_file_metrics() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        let row_count = 512;
        let mut data_gen = BatchGenerator::new()
            .col(Box::new(
                IncrementingInt32::new().named("filter_me".to_owned()),
            ))
            .col(Box::new(RandomVector::new().named("indexable".to_owned())));

        fixture
            .create_with_data(data_gen.batch(row_count))
            .await
            .unwrap();
        fixture
            .append_data(data_gen.batch(row_count))
            .await
            .unwrap();
        fixture.create_some_index().await.unwrap();
        fixture.delete_data("filter_me < 20").await.unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture
            .overwrite_data(data_gen.batch(row_count))
            .await
            .unwrap();
        fixture.delete_data("filter_me >= 40").await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(8).unwrap())
            .await
            .unwrap();
        let after_count = fixture.count_files().await.unwrap();

        let data_files_removed = (before_count.num_data_files - after_count.num_data_files) as u64;
        let transaction_files_removed =
            (before_count.num_tx_files - after_count.num_tx_files) as u64;
        let index_files_removed =
            (before_count.num_index_files - after_count.num_index_files) as u64;
        let deletion_files_removed =
            (before_count.num_delete_files - after_count.num_delete_files) as u64;

        assert_eq!(removed.data_files_removed, data_files_removed);
        assert_eq!(removed.transaction_files_removed, transaction_files_removed);
        assert_eq!(removed.index_files_removed, index_files_removed);
        assert_eq!(removed.deletion_files_removed, deletion_files_removed);
        assert_gt!(removed.data_files_removed, 0);
        assert_gt!(removed.transaction_files_removed, 0);
        assert_gt!(removed.index_files_removed, 0);
        assert_gt!(removed.deletion_files_removed, 0);
    }

    #[tokio::test]
    async fn dont_clean_index_data_files() {
        // Indexes have .lance files in them that are not referenced
        // by any fragment.  We need to make sure the cleanup routine
        // doesn't over-zealously delete these
        let fixture = MockDatasetFixture::try_new().unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.create_some_data().await.unwrap();
        fixture.create_some_index().await.unwrap();

        let before_count = fixture.count_files().await.unwrap();
        let before = utc_now() - TimeDelta::try_days(8).unwrap();
        let removed = fixture.run_cleanup(before).await.unwrap();
        assert_eq!(removed.old_versions, 0);
        assert_eq!(removed.bytes_removed, 0);

        let after_count = fixture.count_files().await.unwrap();

        assert_eq!(before_count, after_count);
    }

    #[tokio::test]
    async fn cleanup_old_replaced_segment_keeps_still_referenced_segments() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();

        let mut dataset = fixture.open().await.unwrap();
        let field_id = dataset.schema().field("indexable").unwrap().id;

        let seg_a = Uuid::new_v4();
        let seg_b = Uuid::new_v4();
        write_dummy_index_artifact(&dataset, seg_a).await.unwrap();
        write_dummy_index_artifact(&dataset, seg_b).await.unwrap();

        let index_a = dummy_index_metadata(&dataset, field_id, seg_a, [0_u32]);
        let index_b = dummy_index_metadata(&dataset, field_id, seg_b, [1_u32]);
        let initial_tx = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![index_a.clone(), index_b.clone()],
                removed_indices: vec![],
            },
            None,
        );
        dataset
            .apply_commit(initial_tx, &Default::default(), &Default::default())
            .await
            .unwrap();

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let seg_c = Uuid::new_v4();
        write_dummy_index_artifact(&dataset, seg_c).await.unwrap();
        let index_c = dummy_index_metadata(&dataset, field_id, seg_c, [2_u32]);
        let replace_tx = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![index_c.clone()],
                removed_indices: vec![index_a.clone()],
            },
            None,
        );
        dataset
            .apply_commit(replace_tx, &Default::default(), &Default::default())
            .await
            .unwrap();

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
            .await
            .unwrap();

        assert_eq!(removed.index_files_removed, 2);
        assert!(
            !dataset
                .object_store
                .as_ref()
                .exists(
                    &dataset
                        .indices_dir()
                        .clone()
                        .join(seg_a.to_string())
                        .join("index.idx")
                )
                .await
                .unwrap()
        );
        assert!(
            dataset
                .object_store
                .as_ref()
                .exists(
                    &dataset
                        .indices_dir()
                        .clone()
                        .join(seg_b.to_string())
                        .join("index.idx")
                )
                .await
                .unwrap()
        );
        assert!(
            dataset
                .object_store
                .as_ref()
                .exists(
                    &dataset
                        .indices_dir()
                        .clone()
                        .join(seg_c.to_string())
                        .join("index.idx")
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cleanup_old_uncommitted_index_artifacts() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();

        let dataset = fixture.open().await.unwrap();
        let staging_uuid = Uuid::new_v4();
        let shard_uuid = Uuid::new_v4();
        let built_segment_uuid = Uuid::new_v4();

        write_dummy_staging_partial(&dataset, staging_uuid, shard_uuid)
            .await
            .unwrap();
        write_dummy_index_artifact(&dataset, built_segment_uuid)
            .await
            .unwrap();

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
            .await
            .unwrap();

        assert_eq!(removed.old_versions, 0);
        assert_eq!(removed.index_files_removed, 4);
        assert!(
            !dataset
                .object_store
                .as_ref()
                .exists(
                    &dataset
                        .indices_dir()
                        .clone()
                        .join(staging_uuid.to_string())
                        .join(format!("partial_{}", shard_uuid))
                        .join("index.idx"),
                )
                .await
                .unwrap()
        );
        assert!(
            !dataset
                .object_store
                .as_ref()
                .exists(
                    &dataset
                        .indices_dir()
                        .clone()
                        .join(built_segment_uuid.to_string())
                        .join("index.idx"),
                )
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cleanup_failed_commit_data_file() {
        // We should clean up data files that are written but the commit failed
        // for whatever reason

        let mut fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        fixture.block_commits();
        assert!(fixture.append_some_data().await.is_err());
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        let before_count = fixture.count_files().await.unwrap();
        // This append will fail since the commit is blocked but it should have
        // deposited a data file
        assert_eq!(before_count.num_data_files, 2);
        assert_eq!(before_count.num_manifest_files, 1);
        // Only 1 txn file: the failed commit's txn file was already cleaned up.
        assert_eq!(before_count.num_tx_files, 1);

        // All of our manifests are newer than the threshold but temp files
        // should still be deleted.
        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
            .await
            .unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 0);
        assert_eq!(removed.data_files_removed, 1);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_data_files, 1);
        assert_eq!(after_count.num_manifest_files, 1);
        assert_eq!(after_count.num_tx_files, 1);
    }

    #[tokio::test]
    async fn dont_cleanup_in_progress_write() {
        // We should not cleanup data files newer than our threshold as they might
        // belong to in-progress writes

        // For testing purposes we actually create these files with a failed write
        // but the cleanup routine has no way of detecting this.  They should look
        // just like an in-progress write.
        let mut fixture = MockDatasetFixture::try_new().unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.create_some_data().await.unwrap();
        fixture.block_commits();
        assert!(fixture.append_some_data().await.is_err());

        let before_count = fixture.count_files().await.unwrap();

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
            .await
            .unwrap();

        assert_eq!(removed.old_versions, 0);
        assert_eq!(removed.bytes_removed, 0);
        assert_eq!(removed.data_files_removed, 0);

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count, after_count);
    }

    #[tokio::test]
    async fn can_recover_delete_failure() {
        // We want to make sure that an I/O error during the cleanup process doesn't
        // prevent us from running cleanup again later.
        let mut fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());
        fixture.overwrite_some_data().await.unwrap();

        // The delete operation should delete the first version and its
        // data file.  However, we will block the manifest file from getting
        // cleaned up by simulating an I/O error.
        fixture.block_delete_manifest();

        let before_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count.num_data_files, 2);
        assert_eq!(before_count.num_manifest_files, 2);

        assert!(
            fixture
                .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
                .await
                .is_err()
        );

        // This test currently relies on us sending in manifest files after
        // data files.  Also, the delete process is run in parallel.  However,
        // it seems stable to stably delete the data file even though the manifest delete fails.
        // My guess is that it is not possible to interrupt a task in flight and so it still
        // has to finish the buffered tasks even if they are ignored.
        let mid_count = fixture.count_files().await.unwrap();
        assert_eq!(mid_count.num_data_files, 1);
        assert_eq!(mid_count.num_manifest_files, 2);

        fixture.unblock_delete_manifest();

        let removed = fixture
            .run_cleanup(utc_now() - TimeDelta::try_days(7).unwrap())
            .await
            .unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 1);
        assert_eq!(
            removed.bytes_removed,
            mid_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_data_files, 1);
        assert_eq!(after_count.num_manifest_files, 1);
    }

    #[tokio::test]
    async fn cleanup_and_retain_3_recent_versions() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        let mut time = 10i64;
        for _ in 0..4 {
            MockClock::set_system_time(TimeDelta::try_seconds(time).unwrap().to_std().unwrap());
            time += 10i64;
            fixture.overwrite_some_data().await.unwrap();
        }

        let before_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count.num_data_files, 5);
        assert_eq!(before_count.num_manifest_files, 5);

        // Retain 3 recent versions
        let policy = CleanupPolicyBuilder::default()
            .retain_n_versions(&fixture.open().await.unwrap(), 3)
            .await
            .unwrap()
            .build();
        let removed = fixture.run_cleanup_with_policy(policy).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 2);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );

        assert_eq!(after_count.num_data_files, 3);
        assert_eq!(after_count.num_manifest_files, 3);
    }

    #[tokio::test]
    async fn cleanup_before_ts_and_retain_n_recent_versions() {
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        for time in (1i64..).take(4) {
            MockClock::set_system_time(TimeDelta::try_days(time).unwrap().to_std().unwrap());
            fixture.overwrite_some_data().await.unwrap();
        }

        let before_count = fixture.count_files().await.unwrap();
        assert_eq!(before_count.num_data_files, 5);
        assert_eq!(before_count.num_manifest_files, 5);

        // Retain 3 recent versions before timestamp now - 6days
        let policy = CleanupPolicyBuilder::default()
            .before_timestamp(utc_now() - TimeDelta::try_days(6).unwrap())
            .retain_n_versions(&fixture.open().await.unwrap(), 3)
            .await
            .unwrap()
            .build();
        let removed = fixture.run_cleanup_with_policy(policy).await.unwrap();
        assert_eq!(removed.old_versions, 0);

        // Retain 10 recent versions before timestamp now
        let policy = CleanupPolicyBuilder::default()
            .before_timestamp(utc_now())
            .retain_n_versions(&fixture.open().await.unwrap(), 10)
            .await
            .unwrap()
            .build();
        let removed = fixture.run_cleanup_with_policy(policy).await.unwrap();
        assert_eq!(removed.old_versions, 0);

        // Retain 3 recent versions before timestamp now - 1days
        let policy = CleanupPolicyBuilder::default()
            .before_timestamp(utc_now() - TimeDelta::try_days(2).unwrap())
            .retain_n_versions(&fixture.open().await.unwrap(), 3)
            .await
            .unwrap()
            .build();
        let removed = fixture.run_cleanup_with_policy(policy).await.unwrap();

        let after_count = fixture.count_files().await.unwrap();
        assert_eq!(removed.old_versions, 2);
        assert_eq!(
            removed.bytes_removed,
            before_count.num_bytes - after_count.num_bytes
        );
        assert_eq!(after_count.num_data_files, 3);
        assert_eq!(after_count.num_manifest_files, 3);
    }

    #[tokio::test]
    async fn cleanup_preserves_unmanaged_dirs_and_files() {
        // Ensure cleanup does not delete unmanaged directories/files under the dataset root
        // Uses MockDatasetFixture and run_cleanup_with_override to match other tests' style
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();

        let registry = Arc::new(ObjectStoreRegistry::default());
        let (os, base) =
            ObjectStore::from_uri_and_params(registry, &fixture.dataset_path, &fixture.os_params())
                .await
                .unwrap();

        // Create unmanaged directories/files under dataset root
        let img = base.clone().join("images").join("clip.mp4");
        let misc = base.clone().join("misc").join("notes.txt");
        let branch_file = base.clone().join("tree").join("branchA").join("data.bin");
        os.put(&img, b"video").await.unwrap();
        os.put(&misc, b"notes").await.unwrap();
        os.put(&branch_file, b"branch").await.unwrap();

        // Create a temporary manifest file that should be cleaned
        let tmp_manifest = base.clone().join("_versions").join(".tmp").join("orphan");
        os.put(&tmp_manifest, b"tmp").await.unwrap();
        // Delete the _transactions directory so that we can test that if not_found err will be swallowed
        os.remove_dir_all(base.clone().join(TRANSACTIONS_DIR))
            .await
            .unwrap();

        fixture
            .run_cleanup_with_override(utc_now(), Some(true), Some(false))
            .await
            .unwrap();

        // Temp manifest file is managed by Lance and should be removed
        assert!(!os.exists(&tmp_manifest).await.unwrap());
        // Unrelated files must remain
        assert!(os.exists(&img).await.unwrap());
        assert!(os.exists(&misc).await.unwrap());
        assert!(os.exists(&branch_file).await.unwrap());
    }

    // Lineage overview with annotated base versions:
    // - branch1 is created from main@v1
    // - branch4 is created from main@v2 (after main receives a second write)
    // - dev/branch2 is created from branch1@latest
    // - feature/nathan/branch3 is created from dev/branch2@latest
    //
    // ASCII lineage with versions:
    //    main:v1 ──▶ branch1:v1 ──▶ dev/branch2:v2 ──▶ feature/nathan/branch3:v3
    //        │
    //    (main:v2) ──▶ branch4:v2
    //
    // Cleanup policy focus (unless explicitly overridden in a test):
    // - retain_n_versions = 1: keep the latest manifest per branch
    // - referenced branches: when enabled, protect parent files referenced by descendants
    // - file counts reported per branch:
    //   manifest: number of manifest files under _versions
    //   data: .lance files under data directory
    //   tx: .txn files count under _transactions
    //   delete: deletion files count under _deletions
    //   index: index files count under _indices
    //
    // Note: branch2 is stored as "dev/branch2"; comments may refer to it as branch2 for brevity.
    // Important: auto_cleanup_hook uses policy derived from manifest config; it does not flip
    // clean_referenced_branches unless tests call cleanup_old_versions with a custom policy.
    struct LineageSetup {
        main: BranchDatasetFixture,
        branch1: BranchDatasetFixture,
        branch2: BranchDatasetFixture,
        branch3: BranchDatasetFixture,
        branch4: BranchDatasetFixture,
    }

    impl LineageSetup {
        /// Assert all branches and main are unchanged since last refresh.
        pub async fn assert_all_unchanged(&mut self) {
            self.main.assert_not_changed().await.unwrap();
            self.branch1.assert_not_changed().await.unwrap();
            self.branch2.assert_not_changed().await.unwrap();
            self.branch3.assert_not_changed().await.unwrap();
            self.branch4.assert_not_changed().await.unwrap();
        }

        /// Assert specified branches are unchanged.
        pub async fn assert_unchanged(&mut self, branches: &[&str]) {
            for &b in branches {
                match b {
                    "main" => self.main.assert_not_changed().await.unwrap(),
                    "branch1" => self.branch1.assert_not_changed().await.unwrap(),
                    "branch2" => self.branch2.assert_not_changed().await.unwrap(),
                    "branch3" => self.branch3.assert_not_changed().await.unwrap(),
                    "branch4" => self.branch4.assert_not_changed().await.unwrap(),
                    _ => panic!("unknown branch: {}", b),
                }
            }
        }

        pub async fn enable_auto_cleanup(&mut self) -> Result<()> {
            let updates = [
                ("lance.auto_cleanup.interval", "1"),
                ("lance.auto_cleanup.retain_versions", "1"),
                ("lance.auto_cleanup.referenced_branch", "true"),
            ];
            self.main.dataset.update_config(updates).await?;
            self.branch1.dataset.update_config(updates).await?;
            self.branch2.dataset.update_config(updates).await?;
            self.branch3.dataset.update_config(updates).await?;
            self.branch4.dataset.update_config(updates).await?;
            self.main.refresh().await?;
            self.branch1.refresh().await?;
            self.branch2.refresh().await?;
            self.branch3.refresh().await?;
            self.branch4.refresh().await?;
            Ok(())
        }

        pub async fn disable_auto_cleanup(&mut self) -> Result<()> {
            let updates = [
                ("lance.auto_cleanup.interval", None),
                ("lance.auto_cleanup.retain_versions", None),
                ("lance.auto_cleanup.older_than", None),
            ];
            self.main.dataset.update_config(updates).await?;
            self.branch1.dataset.update_config(updates).await?;
            self.branch2.dataset.update_config(updates).await?;
            self.branch3.dataset.update_config(updates).await?;
            self.branch4.dataset.update_config(updates).await?;
            self.main.refresh().await?;
            self.branch1.refresh().await?;
            self.branch2.refresh().await?;
            self.branch3.refresh().await?;
            self.branch4.refresh().await?;
            Ok(())
        }
    }

    // Build the lineage and configure per-branch auto-cleanup to retain latest version.
    async fn build_lineage_datasets() -> Result<LineageSetup> {
        let fixture = Arc::new(MockDatasetFixture::try_new()?);

        MockClock::set_system_time(TimeDelta::try_seconds(1).unwrap().to_std().unwrap());

        // Create main (initial write) with id and text columns for inverted index
        use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
        use arrow_schema::{DataType, Field};
        let ids = Int32Array::from_iter_values(0..50i32);
        let texts = StringArray::from_iter_values((0..50i32).map(|i| format!("text_{}", i)));
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(texts)]).unwrap();
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);

        Dataset::write(
            reader,
            &fixture.dataset_path,
            Some(WriteParams {
                mode: WriteMode::Create,
                store_params: Some(fixture.os_params()),
                ..Default::default()
            }),
        )
        .await?;
        let mut main = BranchDatasetFixture::new(fixture.clone(), fixture.load().await?);
        // Initial index creation and refresh counts
        main.create_text_index().await?;
        main.write_data().await?;

        // Create branch1 from main@v1, then do an initial append + deterministic delete
        let mut branch1 = BranchDatasetFixture::new(
            fixture.clone(),
            fixture
                .create_branch_and_load(&mut main.dataset, "branch1", (None, None))
                .await?,
        );
        branch1.write_data().await?;

        // Create branch2 from branch1@latest
        let mut branch2 = BranchDatasetFixture::new(
            fixture.clone(),
            fixture
                .create_branch_and_load(&mut branch1.dataset, "dev/branch2", ("branch1", None))
                .await?,
        );
        branch2.write_data().await?;

        // Create branch3 from branch2@latest, initial append + delete
        let mut branch3 = BranchDatasetFixture::new(
            fixture.clone(),
            fixture
                .create_branch_and_load(
                    &mut branch2.dataset,
                    "feature/nathan/branch3",
                    ("dev/branch2", None),
                )
                .await?,
        );
        branch3.write_data().await?;

        // Create branch4 from a new version in main
        main.write_data().await?;
        let mut branch4 = BranchDatasetFixture::new(
            fixture.clone(),
            fixture
                .create_branch_and_load(&mut main.dataset, "branch4", (None, None))
                .await?,
        );
        branch4.write_data().await?;

        let mut lineage = LineageSetup {
            main,
            branch1,
            branch2,
            branch3,
            branch4,
        };

        lineage.disable_auto_cleanup().await?;
        Ok(lineage)
    }

    // BranchDatasetFixture combines dataset with branch-specific state and file counting.
    // It provides:
    // - Shared fixture for temporary directory and mock store
    // - Dataset holding for stateful operations (checkout, write, etc.)
    // - File counting for cleanup verification
    struct BranchDatasetFixture {
        fixture: Arc<MockDatasetFixture>,
        dataset: Dataset,
        counts: FileCounts,
    }

    impl BranchDatasetFixture {
        fn new(fixture: Arc<MockDatasetFixture>, dataset: Dataset) -> Self {
            Self {
                fixture,
                dataset,
                counts: FileCounts {
                    num_manifest_files: 0,
                    num_data_files: 0,
                    num_tx_files: 0,
                    num_delete_files: 0,
                    num_index_files: 0,
                    num_bytes: 0,
                },
            }
        }

        // Create a full-text index (Inverted) on the "text" column once.
        // We only create this on main during dataset creation. Branches inherit the index configuration.
        async fn create_text_index(&mut self) -> Result<()> {
            use crate::index::DatasetIndexExt;
            use lance_index::IndexType;
            use lance_index::scalar::InvertedIndexParams;
            let params = InvertedIndexParams::default();
            self.dataset
                .create_index(&["text"], IndexType::Inverted, None, &params, true)
                .await?;
            Ok(())
        }

        // Append a batch, then read exactly one row and delete that row; finally optimize indices.
        async fn append_delete_and_optimize_index(&mut self) -> Result<()> {
            // Append a small batch with id and text columns
            self.write_batch(5).await?;
            // Delete the last row to create a deletion file
            self.delete_last_row().await?;
            // Optimize indices after write and delete
            use lance_index::optimize::OptimizeOptions;
            self.dataset
                .optimize_indices(&OptimizeOptions::merge(1))
                .await?;
            Ok(())
        }

        // Append a batch with id and text columns.
        async fn write_batch(&mut self, rows: i32) -> Result<()> {
            use crate::dataset::WriteParams;
            use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
            use arrow_schema::{DataType, Field};

            let ids = Int32Array::from_iter_values(0..rows);
            let texts = StringArray::from_iter_values((0..rows).map(|i| format!("text_{}", i)));
            let schema = Arc::new(arrow_schema::Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(texts)]).unwrap();
            let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);

            self.dataset
                .append(
                    reader,
                    Some(WriteParams {
                        mode: WriteMode::Append,
                        store_params: Some(self.fixture.os_params()),
                        ..Default::default()
                    }),
                )
                .await?;
            self.dataset.checkout_latest().await?;
            Ok(())
        }

        // Delete the last row to generate a deletion file.
        async fn delete_last_row(&mut self) -> Result<()> {
            let batch = self.dataset.scan().with_row_id().try_into_batch().await?;
            if batch.num_rows() > 0 {
                let row_id_col = batch.column_by_name(lance_core::ROW_ID).unwrap();
                let uint64_array = row_id_col.as_any().downcast_ref::<UInt64Array>().unwrap();
                let max_row_id = compute::max(uint64_array).unwrap_or(0);
                self.dataset
                    .delete(&format!("_rowid = {}", max_row_id))
                    .await?;
            }
            Ok(())
        }

        // Update counters by listing authoritative branch directories instead of reading the latest manifest.
        async fn refresh(&mut self) -> Result<()> {
            use futures::TryStreamExt;
            let branch_path = self.dataset.base.clone();

            // Count files in a directory, filtering by optional extension(s).
            async fn count_dir(
                os: &ObjectStore,
                dir: &Path,
                exts: Option<&[&str]>,
            ) -> Result<usize> {
                let mut count = 0usize;
                let mut s = os.read_dir_all(dir, None);
                while let Some(meta) = s.try_next().await? {
                    match exts {
                        Some(exts) => {
                            if let Some(e) = meta.location.extension()
                                && exts.contains(&e)
                            {
                                count += 1;
                            }
                        }
                        None => count += 1,
                    }
                }
                Ok(count)
            }

            let manifest_dir = branch_path.clone().join("_versions");
            self.counts.num_manifest_files = count_dir(
                &self.dataset.object_store,
                &manifest_dir,
                Some(&["manifest"]),
            )
            .await
            .unwrap_or(0);

            // Transactions: count files under _transactions (extension .txn)
            let txn_dir = branch_path.clone().join("_transactions");
            self.counts.num_tx_files =
                count_dir(&self.dataset.object_store, &txn_dir, Some(&["txn"]))
                    .await
                    .unwrap_or(0);

            // Indices: count files under _indices
            let idx_dir = branch_path.clone().join(crate::dataset::INDICES_DIR);
            self.counts.num_index_files = count_dir(&self.dataset.object_store, &idx_dir, None)
                .await
                .unwrap_or(0);

            // Deletions: count files under _deletions (extensions .arrow / .bin)
            let del_dir = branch_path.clone().join("_deletions");
            self.counts.num_delete_files = count_dir(
                &self.dataset.object_store,
                &del_dir,
                Some(&["arrow", "bin"]),
            )
            .await
            .unwrap_or(0);

            // Data files: count .lance files under data/
            let data_dir = branch_path.clone().join(crate::dataset::DATA_DIR);
            self.counts.num_data_files =
                count_dir(&self.dataset.object_store, &data_dir, Some(&["lance"]))
                    .await
                    .unwrap_or(0);

            Ok(())
        }

        async fn count_data(&self) -> Result<usize> {
            use futures::TryStreamExt;
            let mut count = 0usize;
            let mut s = self.dataset.scan().try_into_stream().await?;
            while let Some(_batch) = s.try_next().await? {
                count += 1;
            }
            Ok(count)
        }

        // Strict equality assertion for all counters.
        async fn assert_not_changed(&mut self) -> Result<()> {
            let pre_counts = self.counts;
            let pre_data_count = self.count_data().await?;

            self.refresh().await?;
            assert_eq!(
                self.counts.num_manifest_files,
                pre_counts.num_manifest_files
            );
            assert_eq!(self.counts.num_data_files, pre_counts.num_data_files);
            assert_eq!(self.counts.num_tx_files, pre_counts.num_tx_files);
            assert_eq!(self.counts.num_delete_files, pre_counts.num_delete_files);
            assert_eq!(self.counts.num_index_files, pre_counts.num_index_files);
            assert_eq!(self.count_data().await?, pre_data_count);
            Ok(())
        }

        // Append, delete top row, and optimize indices.
        async fn write_data(&mut self) -> Result<()> {
            self.append_delete_and_optimize_index().await?;
            self.refresh().await
        }

        // Compact files for a given branch and optimize indices to stabilize index files.
        async fn compact(&mut self) -> Result<()> {
            use crate::dataset::optimize::{CompactionOptions, compact_files};
            compact_files(&mut self.dataset, CompactionOptions::default(), None).await?;
            self.refresh().await
        }

        async fn run_cleanup(&mut self) -> Result<RemovalStats> {
            let policy = CleanupPolicyBuilder::default()
                .error_if_tagged_old_versions(false)
                .retain_n_versions(&self.dataset, 1)
                .await?
                .build();
            self.run_cleanup_inner(policy).await
        }

        async fn run_cleanup_with_referenced_branches(&mut self) -> Result<RemovalStats> {
            let policy = CleanupPolicyBuilder::default()
                .error_if_tagged_old_versions(false)
                .clean_referenced_branches(true)
                .retain_n_versions(&self.dataset, 1)
                .await?
                .build();
            self.run_cleanup_inner(policy).await
        }

        async fn explain_cleanup_with_referenced_branches(&mut self) -> Result<CleanupExplanation> {
            let policy = CleanupPolicyBuilder::default()
                .error_if_tagged_old_versions(false)
                .clean_referenced_branches(true)
                .retain_n_versions(&self.dataset, 1)
                .await?
                .build();
            self.dataset.checkout_latest().await?;
            self.dataset.cleanup(policy).explain().await
        }

        async fn run_cleanup_inner(&mut self, policy: CleanupPolicy) -> Result<RemovalStats> {
            let pre_count = self.count_data().await?;
            self.dataset.checkout_latest().await?;
            let stats = cleanup_old_versions(&self.dataset, policy).await;
            self.refresh().await?;
            // Assert data could be read again and did't change
            assert_eq!(self.count_data().await?, pre_count);
            stats
        }
    }

    // ===================== Tests =====================
    #[tokio::test]
    async fn cleanup_lineage_branch1() {
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.branch1.write_data().await.unwrap();
        setup.branch1.run_cleanup().await.unwrap();
        // Branch2 and branch3 hold references from branch1:
        // - 1 manifest file
        // - 1 data file
        // - 1 deletion file
        // - 4 index files
        // The left is the counts for the latest version of appending
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 2);
        assert_eq!(setup.branch1.counts.num_index_files, 14);
        setup.assert_all_unchanged().await;

        setup.branch1.compact().await.unwrap();
        setup.branch1.run_cleanup().await.unwrap();
        // Branch2 and branch3 hold references from branch1:
        // - 1 manifest file
        // - 1 data file
        // - 1 deletion file
        // - 4 index files
        // The left (1, 1, 1, 0, 4) is the counts for the latest version of compaction
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 1);
        assert_eq!(setup.branch1.counts.num_index_files, 14);
        setup.assert_all_unchanged().await;

        // Now we clean the referenced files of branch1 by branch2 and branch3
        setup.branch2.compact().await.unwrap();
        setup.branch3.compact().await.unwrap();
        setup.branch3.run_cleanup().await.unwrap();
        setup.branch2.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version of compaction
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version of compaction
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup.branch1.run_cleanup().await.unwrap();

        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version of compaction
        assert_eq!(setup.branch1.counts.num_manifest_files, 1);
        assert_eq!(setup.branch1.counts.num_data_files, 1);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 0);
        assert_eq!(setup.branch1.counts.num_index_files, 7);
        setup.assert_all_unchanged().await;
    }

    #[tokio::test]
    async fn cleanup_lineage_branch3() {
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.branch3.write_data().await.unwrap();
        setup.branch3.run_cleanup().await.unwrap();
        // Two writes produced:
        // - 2 data files
        // - 2 deletion files
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 2);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 2);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup
            .assert_unchanged(&["branch1", "branch2", "branch4", "main"])
            .await;

        setup.branch2.compact().await.unwrap();
        setup.branch2.run_cleanup().await.unwrap();
        // Branch3 hold references from branch2:
        // - 1 manifest file
        // - 1 data file
        // - 1 deletion file
        // The left is the counts for the latest version of compaction
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 2);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 7);

        setup.branch3.compact().await.unwrap();
        setup.branch3.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup
            .assert_unchanged(&["branch1", "branch2", "branch4", "main"])
            .await;

        setup.branch2.compact().await.unwrap();
        setup.branch2.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
    }

    #[tokio::test]
    async fn cleanup_lineage_branch4() {
        // Setup shared lineage and per-branch auto-clean config
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.branch4.write_data().await.unwrap();
        setup.branch4.run_cleanup().await.unwrap();
        // Two writes produced:
        // - 2 data files
        // - 2 deletion files
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 2);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 2);
        assert_eq!(setup.branch4.counts.num_index_files, 7);
        setup.assert_all_unchanged().await;

        setup.main.compact().await.unwrap();
        setup.main.run_cleanup().await.unwrap();
        // Branch1-branch2 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        // - 4 index files
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 1 deletion file
        // - 4 index files
        // The left(1, 1, 1, 0, 0) is the counts for the latest version of compaction
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);

        setup.branch4.compact().await.unwrap();
        setup.branch4.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts of one version
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);
        setup.assert_all_unchanged().await;

        setup.main.run_cleanup().await.unwrap();
        // Branch1-branch2 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        // - 4 index files
        // The left(1, 1, 1, 0, 4) is the counts for the latest version of compaction
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 3);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 1);
        assert_eq!(setup.main.counts.num_index_files, 14);
    }

    #[tokio::test]
    async fn cleanup_lineage_main() {
        // Setup shared lineage and per-branch auto-clean config
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.main.write_data().await.unwrap();
        setup.main.run_cleanup().await.unwrap();
        // Branch1-branch2 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        // - 4 index files(only for branch1)
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 1 deletion file
        // - 4 index files
        // The left(1, 1, 1, 1, 4) is the counts for the latest version of compaction
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 3);
        assert_eq!(setup.main.counts.num_index_files, 21);
        setup.assert_all_unchanged().await;

        setup.main.compact().await.unwrap();
        setup.main.run_cleanup().await.unwrap();
        // Cleanup the deletion file
        // Produce 1 datafile and cleanup 1
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 21);
        setup.assert_all_unchanged().await;

        setup.branch1.write_data().await.unwrap();
        setup.branch1.compact().await.unwrap();
        setup.branch2.write_data().await.unwrap();
        setup.branch2.compact().await.unwrap();
        setup.branch2.run_cleanup().await.unwrap();
        // Branch3 holds references from branch2:
        // - 1 manifest file
        // - 1 data files
        // - 1 deletion file
        // Branch3 holds reference from branch1:
        // - 1 manifest file
        // - 1 data files
        // - 2 deletion files
        // - 4 index files
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 2);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 14);
        setup.branch1.run_cleanup().await.unwrap();
        // Cleanup 4 index files referenced from branch2
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 1);
        assert_eq!(setup.branch1.counts.num_index_files, 7);

        setup.main.run_cleanup().await.unwrap();
        // Branch3 holds references from main:
        // - 1 manifest file
        // - 1 data files
        // - 1 deletion file
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 2 deletion files
        // - 4 index files
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);

        setup.branch3.write_data().await.unwrap();
        setup.branch3.compact().await.unwrap();
        setup.branch3.run_cleanup().await.unwrap();
        // Only the counts for the latest version
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);

        setup.main.run_cleanup().await.unwrap();
        // Cleanup doesn't take effects if we don't clean branch2 and branch1 first
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);

        // Cleanup doesn't take effect if we don't clean branch2 first
        setup.branch1.run_cleanup().await.unwrap();
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 1);
        assert_eq!(setup.branch1.counts.num_index_files, 7);

        setup.branch2.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);

        setup.branch1.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.branch1.counts.num_manifest_files, 1);
        assert_eq!(setup.branch1.counts.num_data_files, 1);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 0);
        assert_eq!(setup.branch1.counts.num_index_files, 7);

        setup.main.run_cleanup().await.unwrap();
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 2 deletion files
        // - 4 index files
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);

        setup.branch4.write_data().await.unwrap();
        setup.branch4.compact().await.unwrap();
        setup.branch4.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);

        setup.main.run_cleanup().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts for the latest version
        assert_eq!(setup.main.counts.num_manifest_files, 1);
        assert_eq!(setup.main.counts.num_data_files, 1);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 0);
        assert_eq!(setup.main.counts.num_index_files, 7);
    }

    #[tokio::test]
    async fn auto_clean_referenced_branches_from_branch2() {
        // Setup shared lineage and per-branch auto-clean config
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.branch3.write_data().await.unwrap();
        setup.enable_auto_cleanup().await.unwrap();
        setup
            .branch2
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch3.refresh().await.unwrap();
        // Branch3 holds references from branch2:
        // - 1 manifest file
        // - 1 data file
        // - 1 deletion file
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        // After auto-clean: branch3
        // 2 appends produced 2 data files
        // 2 deletes produced 2 deletion files
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 2);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 2);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup
            .assert_unchanged(&["branch1", "branch4", "main"])
            .await;

        setup.disable_auto_cleanup().await.unwrap();
        setup.branch2.write_data().await.unwrap();
        setup.branch2.compact().await.unwrap();
        setup.branch3.compact().await.unwrap();
        setup.enable_auto_cleanup().await.unwrap();
        setup
            .branch2
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch3.refresh().await.unwrap();
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts of one version
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        // Only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts of one version
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup
            .assert_unchanged(&["branch1", "branch4", "main"])
            .await;
    }

    #[tokio::test]
    async fn auto_clean_referenced_branches_from_main() {
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.enable_auto_cleanup().await.unwrap();
        setup.main.write_data().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        // Branch3, branch2 and branch1 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 1 deletion file
        // - 4 index files
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 3);
        assert_eq!(setup.main.counts.num_index_files, 7);

        setup.main.compact().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        // Branch3, branch2 and branch1 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        // Branch4 holds references from main:
        // - 1 manifest file
        // - 3 data files
        // - 1 deletion file
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 7);

        setup.branch4.compact().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch4.refresh().await.unwrap();
        // Branch3, branch2 and branch1 hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 3);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 1);
        assert_eq!(setup.main.counts.num_index_files, 7);
        // (1, 1, 1, 0, 4) is the counts of one version
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);

        setup.branch1.write_data().await.unwrap();
        setup.branch1.compact().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch1.refresh().await.unwrap();
        // Branch3 and branch2 still hold references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 3);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 1);
        assert_eq!(setup.main.counts.num_index_files, 7);
        // Branch3 and branch2 still hold references from branch1:
        // - 1 manifest file
        // - 1 data files
        // - 1 deletion file
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 1);
        assert_eq!(setup.branch1.counts.num_index_files, 7);

        setup.branch2.write_data().await.unwrap();
        setup.branch2.compact().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch2.refresh().await.unwrap();
        // Branch3 still holds references from main:
        // - 1 manifest file
        // - 2 data files
        // - 1 deletion file
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 3);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 1);
        assert_eq!(setup.main.counts.num_index_files, 7);
        // Branch3 still holds references from branch1:
        // - 1 manifest file
        // - 1 data files
        // - 1 deletion file
        assert_eq!(setup.branch1.counts.num_manifest_files, 2);
        assert_eq!(setup.branch1.counts.num_data_files, 2);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 1);
        assert_eq!(setup.branch1.counts.num_index_files, 7);
        // Branch3 still holds references from branch2:
        // - 1 manifest file
        // - 1 data files
        // - 1 deletion file
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 2);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 7);

        setup.branch3.write_data().await.unwrap();
        setup.branch3.compact().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        // For all branches, only the latest manifest is retained.
        // (1, 1, 1, 0, 4) is the counts of one version
        assert_eq!(setup.main.counts.num_manifest_files, 1);
        assert_eq!(setup.main.counts.num_data_files, 1);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 0);
        assert_eq!(setup.main.counts.num_index_files, 7);
        assert_eq!(setup.branch1.counts.num_manifest_files, 1);
        assert_eq!(setup.branch1.counts.num_data_files, 1);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 0);
        assert_eq!(setup.branch1.counts.num_index_files, 7);
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        setup.assert_unchanged(&["branch4"]).await;
    }

    #[tokio::test]
    async fn explain_cleanup_with_referenced_branches_matches_cleanup() {
        let mut setup = build_lineage_datasets().await.unwrap();

        setup.enable_auto_cleanup().await.unwrap();
        setup.main.write_data().await.unwrap();
        setup.main.compact().await.unwrap();
        setup.branch4.compact().await.unwrap();
        setup.branch1.write_data().await.unwrap();
        setup.branch1.compact().await.unwrap();
        setup.branch2.write_data().await.unwrap();
        setup.branch2.compact().await.unwrap();
        setup.branch3.write_data().await.unwrap();
        setup.branch3.compact().await.unwrap();

        setup.main.refresh().await.unwrap();
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        let main_counts_before = setup.main.counts;
        let branch1_counts_before = setup.branch1.counts;
        let branch2_counts_before = setup.branch2.counts;
        let branch3_counts_before = setup.branch3.counts;
        let branch4_counts_before = setup.branch4.counts;

        let explanation = setup
            .main
            .explain_cleanup_with_referenced_branches()
            .await
            .unwrap();

        setup.main.refresh().await.unwrap();
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        assert_eq!(setup.main.counts, main_counts_before);
        assert_eq!(setup.branch1.counts, branch1_counts_before);
        assert_eq!(setup.branch2.counts, branch2_counts_before);
        assert_eq!(setup.branch3.counts, branch3_counts_before);
        assert_eq!(setup.branch4.counts, branch4_counts_before);

        let removed = setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();

        assert!(!explanation.referenced_branches.is_empty());
        assert!(
            explanation
                .referenced_branches
                .iter()
                .any(|branch| branch.cleanup_candidate)
        );
        assert_eq!(explanation.stats, removed);
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        assert_eq!(setup.main.counts.num_manifest_files, 1);
        assert_eq!(setup.branch1.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
    }

    #[tokio::test]
    async fn auto_clean_referenced_branches_with_tags() {
        let mut setup = build_lineage_datasets().await.unwrap();

        setup
            .branch3
            .dataset
            .tags()
            .create("branch3-tag", setup.branch3.dataset.version().version)
            .await
            .unwrap();
        setup
            .main
            .dataset
            .tags()
            .create("main-tag", setup.main.dataset.version().version)
            .await
            .unwrap();

        setup.branch1.compact().await.unwrap();
        setup.branch2.compact().await.unwrap();
        setup.branch3.compact().await.unwrap();
        setup.branch4.compact().await.unwrap();
        setup.main.compact().await.unwrap();
        setup.enable_auto_cleanup().await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        // Two tags hold two manifest references
        // Main tag holds 1 tx file, 3 data files, 2 deletion files and 4 index files
        assert_eq!(setup.main.counts.num_manifest_files, 3);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 2);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);
        // Branch3 tag holds branch1 with 1 tx file, 1 data files, 1 deletion files and 4 index files
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 2);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        // Branch3 tag holds branch2 with 1 tx file, 1 data files, 1 deletion files and 4 index files
        assert_eq!(setup.branch2.counts.num_manifest_files, 2);
        assert_eq!(setup.branch2.counts.num_data_files, 2);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 1);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);

        setup
            .branch3
            .dataset
            .tags()
            .delete("branch3-tag")
            .await
            .unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch1.refresh().await.unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        // 1 manifest file referenced by branch3-tag is cleaned
        assert_eq!(setup.main.counts.num_manifest_files, 2);
        assert_eq!(setup.main.counts.num_data_files, 4);
        assert_eq!(setup.main.counts.num_tx_files, 2);
        assert_eq!(setup.main.counts.num_delete_files, 2);
        assert_eq!(setup.main.counts.num_index_files, 14);
        assert_eq!(setup.branch1.counts.num_manifest_files, 1);
        assert_eq!(setup.branch1.counts.num_data_files, 1);
        assert_eq!(setup.branch1.counts.num_tx_files, 1);
        assert_eq!(setup.branch1.counts.num_delete_files, 0);
        assert_eq!(setup.branch1.counts.num_index_files, 7);
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);

        setup.main.dataset.tags().delete("main-tag").await.unwrap();
        setup
            .main
            .run_cleanup_with_referenced_branches()
            .await
            .unwrap();
        setup.branch2.refresh().await.unwrap();
        setup.branch3.refresh().await.unwrap();
        setup.branch4.refresh().await.unwrap();
        // All cleaned up
        assert_eq!(setup.main.counts.num_manifest_files, 1);
        assert_eq!(setup.main.counts.num_data_files, 1);
        assert_eq!(setup.main.counts.num_tx_files, 1);
        assert_eq!(setup.main.counts.num_delete_files, 0);
        assert_eq!(setup.main.counts.num_index_files, 7);
        assert_eq!(setup.branch2.counts.num_manifest_files, 1);
        assert_eq!(setup.branch2.counts.num_data_files, 1);
        assert_eq!(setup.branch2.counts.num_tx_files, 1);
        assert_eq!(setup.branch2.counts.num_delete_files, 0);
        assert_eq!(setup.branch2.counts.num_index_files, 7);
        assert_eq!(setup.branch3.counts.num_manifest_files, 1);
        assert_eq!(setup.branch3.counts.num_data_files, 1);
        assert_eq!(setup.branch3.counts.num_tx_files, 1);
        assert_eq!(setup.branch3.counts.num_delete_files, 0);
        assert_eq!(setup.branch3.counts.num_index_files, 7);
        assert_eq!(setup.branch4.counts.num_manifest_files, 1);
        assert_eq!(setup.branch4.counts.num_data_files, 1);
        assert_eq!(setup.branch4.counts.num_tx_files, 1);
        assert_eq!(setup.branch4.counts.num_delete_files, 0);
        assert_eq!(setup.branch4.counts.num_index_files, 7);
    }

    #[test]
    fn test_calculate_duration_s3() {
        // Normal case: duration is computed from S3 batch size and configured rate.
        let normal_rate = 100;
        let expected_duration_ns =
            1_000_000_000u64.div_ceil(normal_rate * S3_DELETE_STREAM_BATCH_SIZE);
        assert_eq!(
            calculate_duration("s3".to_string(), normal_rate),
            Duration::from_nanos(expected_duration_ns)
        );

        // Edge case: rate too small should be clamped to 1.
        let min_rate_duration = calculate_duration("s3".to_string(), 1);
        assert_eq!(calculate_duration("s3".to_string(), 0), min_rate_duration);

        // Edge case: computed duration_ns too small should be clamped to at least 1ns.
        let very_large_rate = 2_000_000;
        assert_eq!(
            calculate_duration("s3".to_string(), very_large_rate),
            Duration::from_nanos(1)
        );
    }

    #[tokio::test]
    async fn test_cleanup_with_rate_limit() {
        // Create multiple versions with data files that will be deleted.
        let fixture = MockDatasetFixture::try_new().unwrap();
        fixture.create_some_data().await.unwrap();
        // Create several old versions
        for _ in 0..4 {
            fixture.overwrite_some_data().await.unwrap();
        }

        MockClock::set_system_time(TimeDelta::try_days(10).unwrap().to_std().unwrap());

        // Set rate limit to 1 ops/second so cleanup of several files must take at least ~1s
        let policy = CleanupPolicyBuilder::default()
            .before_timestamp(utc_now() - TimeDelta::try_days(8).unwrap())
            .delete_rate_limit(1)
            .unwrap()
            .build();

        let start = std::time::Instant::now();
        let db = fixture.open().await.unwrap();
        let stats = cleanup_old_versions(&db, policy).await.unwrap();
        let elapsed = start.elapsed();

        // We deleted old versions, so there should be removed files
        assert!(
            stats.old_versions > 0,
            "expected some old versions to be removed"
        );
        // With rate=1 and multiple files, it must take at least 2s
        // (even just 2 deletions at 1/s means ≥2s)
        assert!(
            elapsed.as_millis() >= 2000,
            "expected cleanup to be rate-limited (elapsed: {:?})",
            elapsed
        );
    }
}
