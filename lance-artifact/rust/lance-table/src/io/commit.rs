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
//! The trait [CommitHandler] can be implemented to provide different commit
//! strategies. The default implementation for most object stores is
//! [RenameCommitHandler], which writes the manifest to a temporary path, then
//! renames the temporary path to the final path if no object already exists
//! at the final path. This is an atomic operation in most object stores, but
//! not in AWS S3. So for AWS S3, the default commit handler is
//! [UnsafeCommitHandler], which writes the manifest to the final path without
//! any checks.
//!
//! When providing your own commit handler, most often you are implementing in
//! terms of a lock. The trait [CommitLock] can be implemented as a simpler
//! alternative to [CommitHandler].

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::{fmt::Debug, fs::DirEntry};

use super::manifest::write_manifest;
use futures::Stream;
use futures::future::Either;
use futures::{
    StreamExt, TryStreamExt,
    future::{self, BoxFuture},
    stream::BoxStream,
};
use lance_file::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
use lance_io::object_writer::{ObjectWriter, WriteResult, get_etag};
use log::warn;
use object_store::ObjectStoreExt as OSObjectStoreExt;
use object_store::PutOptions;
use object_store::{Error as ObjectStoreError, ObjectStore as OSObjectStore, path::Path};
use tracing::info;
use url::Url;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod external_manifest;

use lance_core::{Error, Result};
use lance_io::object_store::{ObjectStore, ObjectStoreExt, ObjectStoreParams};
use lance_io::traits::{WriteExt, Writer};

use crate::format::{IndexMetadata, Manifest, Transaction, is_detached_version};
use lance_core::utils::tracing::{AUDIT_MODE_CREATE, AUDIT_TYPE_MANIFEST, TRACE_FILE_AUDIT};
#[cfg(feature = "dynamodb")]
use {
    self::external_manifest::{ExternalManifestCommitHandler, ExternalManifestStore},
    aws_credential_types::provider::ProvideCredentials,
    aws_credential_types::provider::error::CredentialsError,
    lance_io::object_store::{StorageOptions, providers::aws::build_aws_credential},
    object_store::aws::AmazonS3ConfigKey,
    object_store::aws::AwsCredentialProvider,
    std::borrow::Cow,
    std::time::{Duration, SystemTime},
};

pub const VERSIONS_DIR: &str = "_versions";
const MANIFEST_EXTENSION: &str = "manifest";
const DETACHED_VERSION_PREFIX: &str = "d";
/// File name for the JSON version hint file, stored under `_versions/`.
///
/// The file contains `{"version":N}` where `N` is the latest committed version
/// at the time of writing. It enables O(1)/O(k) latest-version lookup via HEAD
/// requests on object stores where listing is not lexicographically ordered
/// (e.g. S3 Express, local filesystem) instead of an O(n) listing.
const VERSION_HINT_FILE: &str = "latest_version_hint.json";

/// How manifest files should be named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestNamingScheme {
    /// `_versions/{version}.manifest`
    V1,
    /// `_manifests/{u64::MAX - version}.manifest`
    ///
    /// Zero-padded and reversed for O(1) lookup of latest version on object stores.
    V2,
}

impl ManifestNamingScheme {
    pub fn manifest_path(&self, base: &Path, version: u64) -> Path {
        if is_detached_version(version) {
            // Detached versions should never show up first in a list operation which
            // means it needs to come lexicographically after all attached manifest
            // files and so we add the prefix `d`.  There is no need to invert the
            // version number since detached versions are not part of the version
            base.clone().join(VERSIONS_DIR).join(format!(
                "{DETACHED_VERSION_PREFIX}{version}.{MANIFEST_EXTENSION}"
            ))
        } else {
            let directory = base.clone().join(VERSIONS_DIR);
            match self {
                Self::V1 => directory.join(format!("{version}.{MANIFEST_EXTENSION}")),
                Self::V2 => {
                    let inverted_version = u64::MAX - version;
                    directory.join(format!("{inverted_version:020}.{MANIFEST_EXTENSION}"))
                }
            }
        }
    }

    pub fn parse_version(&self, filename: &str) -> Option<u64> {
        let file_number = filename
            .split_once('.')
            // Detached versions will fail the `parse` step, which is ok.
            .and_then(|(version_str, _)| version_str.parse::<u64>().ok());
        match self {
            Self::V1 => file_number,
            Self::V2 => file_number.map(|v| u64::MAX - v),
        }
    }

    /// Parse a detached version from a filename like `d123456.manifest`.
    ///
    /// Returns the full version number with the detached mask bit set.
    pub fn parse_detached_version(filename: &str) -> Option<u64> {
        if !filename.starts_with(DETACHED_VERSION_PREFIX) {
            return None;
        }
        let without_prefix = &filename[DETACHED_VERSION_PREFIX.len()..];
        without_prefix
            .split_once('.')
            .and_then(|(version_str, _)| version_str.parse::<u64>().ok())
    }

    pub fn detect_scheme(filename: &str) -> Option<Self> {
        if filename.starts_with(DETACHED_VERSION_PREFIX) {
            // Currently, detached versions must imply V2
            return Some(Self::V2);
        }
        if filename.ends_with(MANIFEST_EXTENSION) {
            const V2_LEN: usize = 20 + 1 + MANIFEST_EXTENSION.len();
            if filename.len() == V2_LEN {
                Some(Self::V2)
            } else {
                Some(Self::V1)
            }
        } else {
            None
        }
    }

    pub fn detect_scheme_staging(filename: &str) -> Self {
        // We shouldn't have to worry about detached versions here since there is no
        // such thing as "detached" and "staged" at the same time.
        if filename.chars().nth(20) == Some('.') {
            Self::V2
        } else {
            Self::V1
        }
    }
}

/// Migrate all V1 manifests to V2 naming scheme.
///
/// This function will rename all V1 manifests to V2 naming scheme.
///
/// This function is idempotent, and can be run multiple times without
/// changing the state of the object store.
///
/// However, it should not be run while other concurrent operations are happening.
/// And it should also run until completion before resuming other operations.
pub async fn migrate_scheme_to_v2(object_store: &ObjectStore, dataset_base: &Path) -> Result<()> {
    object_store
        .inner
        .list(Some(&dataset_base.clone().join(VERSIONS_DIR)))
        .try_filter(|res| {
            let res = if let Some(filename) = res.location.filename() {
                ManifestNamingScheme::detect_scheme(filename) == Some(ManifestNamingScheme::V1)
            } else {
                false
            };
            future::ready(res)
        })
        .try_for_each_concurrent(object_store.io_parallelism(), |meta| async move {
            let filename = meta.location.filename().unwrap();
            let version = ManifestNamingScheme::V1.parse_version(filename).unwrap();
            let path = ManifestNamingScheme::V2.manifest_path(dataset_base, version);
            object_store.inner.rename(&meta.location, &path).await?;
            Ok(())
        })
        .await?;

    Ok(())
}

/// Function that writes the manifest to the object store.
///
/// Returns the size of the written manifest.
pub type ManifestWriter = for<'a> fn(
    object_store: &'a ObjectStore,
    manifest: &'a mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    path: &'a Path,
    transaction: Option<Transaction>,
) -> BoxFuture<'a, Result<WriteResult>>;

/// Canonical manifest writer; its function item type exactly matches `ManifestWriter`.
/// Rationale: keep a crate-local writer implementation so call sites can pass this function
/// directly without non-primitive casts or lifetime coercions.
pub fn write_manifest_file_to_path<'a>(
    object_store: &'a ObjectStore,
    manifest: &'a mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    path: &'a Path,
    transaction: Option<Transaction>,
) -> BoxFuture<'a, Result<WriteResult>> {
    Box::pin(async move {
        let mut object_writer = ObjectWriter::new(object_store, path).await?;
        let pos = write_manifest(&mut object_writer, manifest, indices, transaction).await?;
        object_writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await?;
        let res = Writer::shutdown(&mut object_writer).await?;
        info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_MANIFEST, path = path.to_string());
        Ok(res)
    })
}

#[derive(Debug, Clone)]
pub struct ManifestLocation {
    /// The version the manifest corresponds to.
    pub version: u64,
    /// Path of the manifest file, relative to the table root.
    pub path: Path,
    /// Size, in bytes, of the manifest file. If it is not known, this field should be `None`.
    pub size: Option<u64>,
    /// Naming scheme of the manifest file.
    pub naming_scheme: ManifestNamingScheme,
    /// Optional e-tag, used for integrity checks. Manifests should be immutable, so
    /// if we detect a change in the e-tag, it means the manifest was tampered with.
    /// This might happen if the dataset was deleted and then re-created.
    pub e_tag: Option<String>,
}

impl TryFrom<object_store::ObjectMeta> for ManifestLocation {
    type Error = Error;

    fn try_from(meta: object_store::ObjectMeta) -> Result<Self> {
        let filename = meta.location.filename().ok_or_else(|| {
            Error::internal("ObjectMeta location does not have a filename".to_string())
        })?;
        let scheme = ManifestNamingScheme::detect_scheme(filename)
            .ok_or_else(|| Error::internal(format!("Invalid manifest filename: '{}'", filename)))?;
        let version = scheme
            .parse_version(filename)
            .ok_or_else(|| Error::internal(format!("Invalid manifest filename: '{}'", filename)))?;
        Ok(Self {
            version,
            path: meta.location,
            size: Some(meta.size),
            naming_scheme: scheme,
            e_tag: meta.e_tag,
        })
    }
}

/// Get the latest manifest path.
///
/// - Local filesystem: a single directory read.
/// - Stores where listing is not lexicographically ordered (e.g. S3 Express):
///   the version hint (read the hint file, then probe higher versions with
///   HEADs), falling back to a listing if the hint is missing or stale. A full
///   listing on these stores is O(n) in the number of versions.
/// - Lexicographically ordered stores (e.g. S3 Standard, GCS): the listing
///   already resolves the latest version in roughly one request.
async fn current_manifest_path(
    object_store: &ObjectStore,
    base: &Path,
) -> Result<ManifestLocation> {
    if object_store.is_local() {
        if let Ok(Some(location)) = current_manifest_local(base) {
            return Ok(location);
        }
    } else if uses_version_hint(object_store)
        && let Some(location) = read_version_hint_and_probe(object_store, base).await
    {
        return Ok(location);
    }

    resolve_version_from_listing(object_store, base).await
}

/// JSON body of the version hint file: `{"version":N}`.
#[derive(serde::Serialize, serde::Deserialize)]
struct VersionHint {
    version: u64,
}

/// Set `LANCE_USE_VERSION_HINT=0` (or `false`) to globally disable the version
/// hint — writers stop emitting the hint file and readers stop consulting it,
/// falling back to plain listing. Intended as a benchmark/escape-hatch knob;
/// the hint is on by default.
const VERSION_HINT_ENV: &str = "LANCE_USE_VERSION_HINT";

fn version_hint_globally_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var(VERSION_HINT_ENV) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        ),
        Err(_) => true,
    })
}

/// Whether this object store benefits from a version hint.
///
/// On stores where listing is lexicographically ordered (S3 Standard, GCS,
/// Azure, ...) the latest version is already resolved in roughly one request,
/// so the hint would only add a write per commit for nothing. We write (and
/// read) it only on stores where listing is not lexicographically ordered —
/// S3 Express and the local filesystem. Can be force-disabled with the
/// `LANCE_USE_VERSION_HINT=0` environment variable.
pub fn uses_version_hint(object_store: &ObjectStore) -> bool {
    version_hint_globally_enabled() && !object_store.list_is_lexically_ordered
}

/// Path to the JSON version hint file for a dataset.
fn version_hint_path(base: &Path) -> Path {
    base.clone().join(VERSIONS_DIR).join(VERSION_HINT_FILE)
}

/// Write the version hint file after a successful commit.
///
/// The hint is stored as JSON: `{"version":N}`. This write is best-effort —
/// failures are logged and ignored, since the hint only accelerates reads and
/// never affects correctness (readers verify the hinted version and probe
/// upward from there). It is a no-op for detached versions and for stores that
/// do not benefit from a hint (see [`uses_version_hint`]).
pub async fn write_version_hint(object_store: &ObjectStore, base: &Path, version: u64) {
    if is_detached_version(version) || !uses_version_hint(object_store) {
        return;
    }
    let hint_path = version_hint_path(base);
    let content = serde_json::to_vec(&VersionHint { version }).expect("serialize version hint");
    if let Err(e) = object_store.put(&hint_path, content.as_slice()).await {
        warn!("Failed to write version hint file for version {version}: {e}");
    }
}

/// Read the latest version from the hint file, or `None` if it does not exist
/// or cannot be parsed.
async fn read_version_from_hint(object_store: &ObjectStore, base: &Path) -> Option<u64> {
    let bytes = object_store
        .inner
        .get(&version_hint_path(base))
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;
    Some(serde_json::from_slice::<VersionHint>(&bytes).ok()?.version)
}

/// Read the version hint and probe upward to find the true latest manifest.
///
/// Returns `None` if the hint file is missing, the hinted version no longer
/// exists, or any error occurred — callers should fall back to listing.
async fn read_version_hint_and_probe(
    object_store: &ObjectStore,
    base: &Path,
) -> Option<ManifestLocation> {
    let hint_version = read_version_from_hint(object_store, base).await?;
    let (version, scheme, mut probed) = probe_versions_upward(object_store, base, hint_version)
        .await
        .ok()
        .flatten()?;
    // `probed` is non-empty and its last entry is the highest version found.
    let (_, meta) = probed.pop()?;
    Some(ManifestLocation {
        version,
        path: scheme.manifest_path(base, version),
        size: Some(meta.size),
        naming_scheme: scheme,
        e_tag: meta.e_tag,
    })
}

/// Maximum version gap between the hint and the read version for which we use
/// the hint-based parallel-HEAD path; beyond this a single (paginated) listing
/// is cheaper, so callers fall back to it.
const MAX_HINT_PROBE_GAP: u64 = 1000;

/// Probe `from_version`, then `from_version + 1`, `+ 2`, ... with HEAD requests
/// until one is not found.
///
/// Assumes attached versions are contiguous above `from_version` (true in
/// practice: every commit increments by one, and cleanup only removes *old*
/// versions, never ones newer than the latest). A `NotFound` therefore marks
/// the end of the history.
///
/// - `Ok(Some((true_latest_version, naming_scheme, [(version, meta), ...])))`:
///   the vec covers every version from `from_version` through the true latest
///   in ascending order.
/// - `Ok(None)`: `from_version` itself does not exist (a `NotFound` for both
///   naming schemes) — i.e. the hint pointed past the end.
/// - `Err(_)`: a transient object-store error was hit, so the probed range may
///   be incomplete; callers should fall back to a full listing rather than
///   trust a possibly-stale result.
async fn probe_versions_upward(
    object_store: &ObjectStore,
    base: &Path,
    from_version: u64,
) -> Result<
    Option<(
        u64,
        ManifestNamingScheme,
        Vec<(u64, object_store::ObjectMeta)>,
    )>,
> {
    // Newer datasets use V2; fall back to V1 if the V2 path is not found.
    let mut scheme = ManifestNamingScheme::V2;
    let meta = match object_store
        .inner
        .head(&scheme.manifest_path(base, from_version))
        .await
    {
        Ok(meta) => meta,
        Err(ObjectStoreError::NotFound { .. }) => {
            scheme = ManifestNamingScheme::V1;
            match object_store
                .inner
                .head(&scheme.manifest_path(base, from_version))
                .await
            {
                Ok(meta) => meta,
                Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        Err(e) => return Err(e.into()),
    };

    let mut probed = vec![(from_version, meta)];
    let mut version = from_version;
    loop {
        let next = version + 1;
        match object_store
            .inner
            .head(&scheme.manifest_path(base, next))
            .await
        {
            Ok(meta) => {
                probed.push((next, meta));
                version = next;
            }
            // NotFound means we found the latest version.
            Err(ObjectStoreError::NotFound { .. }) => break,
            // A transient error means a newer version might exist that we
            // failed to observe — surface it so callers fall back to listing.
            Err(e) => return Err(e.into()),
        }
    }
    Ok(Some((version, scheme, probed)))
}

/// List manifest locations with version `> since_version` using the version
/// hint, in descending order of version.
///
/// Returns `None` if the hint is missing or stale enough that this is not
/// usable — callers should fall back to a full listing. `Some(vec![])` is the
/// fast path where the hint confirms there are no new versions.
async fn list_manifests_since_version_with_hint(
    object_store: &ObjectStore,
    base: &Path,
    since_version: u64,
) -> Option<Vec<ManifestLocation>> {
    let hint_version = read_version_from_hint(object_store, base).await?;

    // A reader that is very far behind is cheaper to serve with one paginated
    // listing than with thousands of HEADs.
    if hint_version.saturating_sub(since_version) > MAX_HINT_PROBE_GAP {
        return None;
    }

    // If the hint is not newer than the read version, the only versions that
    // could exist are right above it; otherwise start at the hint.
    let probe_from = if hint_version > since_version {
        hint_version
    } else {
        since_version + 1
    };

    let (scheme, probed) = match probe_versions_upward(object_store, base, probe_from).await {
        Ok(Some((_true_latest, scheme, probed))) => (scheme, probed),
        // Nothing at `probe_from`. If we were probing from the hint, the hint
        // is stale — bail to a full listing. If we were probing from
        // `since_version + 1`, there are simply no new versions.
        Ok(None) if hint_version > since_version => return None,
        Ok(None) => return Some(Vec::new()),
        // Transient error: don't trust the hint path, fall back to listing.
        Err(_) => return None,
    };

    let mut locations: Vec<ManifestLocation> = probed
        .into_iter()
        .filter(|(v, _)| *v > since_version)
        .map(|(version, meta)| ManifestLocation {
            version,
            path: scheme.manifest_path(base, version),
            size: Some(meta.size),
            naming_scheme: scheme,
            e_tag: meta.e_tag,
        })
        .collect();

    // Fill the gap between `since_version` and the hint with HEADs (the probe
    // above already covered `hint_version` and up). The range is contiguous, so
    // any error here (including a `NotFound`) means we can't trust the hint path
    // — fall back to a full listing.
    if hint_version > since_version + 1 {
        let gap_locations: Vec<ManifestLocation> =
            futures::stream::iter((since_version + 1)..hint_version)
                .map(|version| async move {
                    object_store
                        .inner
                        .head(&scheme.manifest_path(base, version))
                        .await
                        .map(|meta| ManifestLocation {
                            version,
                            path: scheme.manifest_path(base, version),
                            size: Some(meta.size),
                            naming_scheme: scheme,
                            e_tag: meta.e_tag,
                        })
                })
                .buffer_unordered(object_store.io_parallelism())
                .try_collect()
                .await
                .ok()?;
        locations.extend(gap_locations);
    }

    locations.sort_by_key(|loc| std::cmp::Reverse(loc.version));
    Some(locations)
}

/// Resolve the latest manifest by listing the versions directory.
async fn resolve_version_from_listing(
    object_store: &ObjectStore,
    base: &Path,
) -> Result<ManifestLocation> {
    let manifest_files = object_store.list(Some(base.clone().join(VERSIONS_DIR)));

    let mut valid_manifests = manifest_files.try_filter_map(|res| {
        let filename = res.location.filename().unwrap();
        if let Some(scheme) = ManifestNamingScheme::detect_scheme(filename) {
            // Only include if we can parse a version (skip detached versions)
            if scheme.parse_version(filename).is_some() {
                future::ready(Ok(Some((scheme, res))))
            } else {
                future::ready(Ok(None))
            }
        } else {
            future::ready(Ok(None))
        }
    });

    let first = valid_manifests.next().await.transpose()?;
    match (first, object_store.list_is_lexically_ordered) {
        // If the first valid manifest we see is V2, we can assume that we are using
        // V2 naming scheme for all manifests.
        (Some((scheme @ ManifestNamingScheme::V2, meta)), true) => {
            let version = scheme
                .parse_version(meta.location.filename().unwrap())
                .unwrap();

            // Sanity check: verify at least for the first 1k files that they are all V2
            // and that the version numbers are decreasing. We use the first 1k because
            // this is the typical size of an object store list endpoint response page.
            for (scheme, meta) in valid_manifests.take(999).try_collect::<Vec<_>>().await? {
                if scheme != ManifestNamingScheme::V2 {
                    warn!(
                        "Found V1 Manifest in a V2 directory. Use `migrate_manifest_paths_v2` \
                         to migrate the directory."
                    );
                    break;
                }
                let next_version = scheme
                    .parse_version(meta.location.filename().unwrap())
                    .unwrap();
                if next_version >= version {
                    warn!(
                        "List operation was expected to be lexically ordered, but was not. This \
                         could mean a corrupt read. Please make a bug report on the lance-format/lance \
                         GitHub repository."
                    );
                    break;
                }
            }

            Ok(ManifestLocation {
                version,
                path: meta.location,
                size: Some(meta.size),
                naming_scheme: scheme,
                e_tag: meta.e_tag,
            })
        }
        // If the list is not lexically ordered, we need to iterate all manifests
        // to find the latest version. This works for both V1 and V2 schemes.
        (Some((first_scheme, meta)), _) => {
            let mut current_version = first_scheme
                .parse_version(meta.location.filename().unwrap())
                .unwrap();
            let mut current_meta = meta;
            let scheme = first_scheme;

            while let Some((entry_scheme, meta)) = valid_manifests.next().await.transpose()? {
                if entry_scheme != scheme {
                    return Err(Error::internal(format!(
                        "Found multiple manifest naming schemes in the same directory: {:?} and {:?}. \
                         Use `migrate_manifest_paths_v2` to migrate the directory.",
                        scheme, entry_scheme
                    )));
                }
                let version = entry_scheme
                    .parse_version(meta.location.filename().unwrap())
                    .unwrap();
                if version > current_version {
                    current_version = version;
                    current_meta = meta;
                }
            }
            Ok(ManifestLocation {
                version: current_version,
                path: current_meta.location,
                size: Some(current_meta.size),
                naming_scheme: scheme,
                e_tag: current_meta.e_tag,
            })
        }
        (None, _) => Err(Error::not_found(
            base.clone().join(VERSIONS_DIR).to_string(),
        )),
    }
}

// This is an optimized function that searches for the latest manifest. In
// object_store, list operations lookup metadata for each file listed. This
// method only gets the metadata for the found latest manifest.
fn current_manifest_local(base: &Path) -> std::io::Result<Option<ManifestLocation>> {
    let path = lance_io::local::to_local_path(&base.clone().join(VERSIONS_DIR));
    let entries = std::fs::read_dir(path)?;

    let mut latest_entry: Option<(u64, DirEntry)> = None;

    let mut scheme: Option<ManifestNamingScheme> = None;

    for entry in entries {
        let entry = entry?;
        let filename_raw = entry.file_name();
        let filename = filename_raw.to_string_lossy();

        let Some(entry_scheme) = ManifestNamingScheme::detect_scheme(&filename) else {
            // Need to ignore temporary files, such as
            // .tmp_7.manifest_9c100374-3298-4537-afc6-f5ee7913666d
            continue;
        };

        if let Some(scheme) = scheme {
            if scheme != entry_scheme {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Found multiple manifest naming schemes in the same directory: {:?} and {:?}",
                        scheme, entry_scheme
                    ),
                ));
            }
        } else {
            scheme = Some(entry_scheme);
        }

        let Some(version) = entry_scheme.parse_version(&filename) else {
            continue;
        };

        if let Some((latest_version, _)) = &latest_entry {
            if version > *latest_version {
                latest_entry = Some((version, entry));
            }
        } else {
            latest_entry = Some((version, entry));
        }
    }

    if let Some((version, entry)) = latest_entry {
        let path = Path::from_filesystem_path(entry.path())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        let metadata = entry.metadata()?;
        Ok(Some(ManifestLocation {
            version,
            path,
            size: Some(metadata.len()),
            naming_scheme: scheme.unwrap(),
            e_tag: Some(get_etag(&metadata)),
        }))
    } else {
        Ok(None)
    }
}

fn list_manifests<'a>(
    base_path: &Path,
    object_store: &'a dyn OSObjectStore,
) -> impl Stream<Item = Result<ManifestLocation>> + 'a {
    object_store
        .read_dir_all(&base_path.clone().join(VERSIONS_DIR), None)
        .filter_map(|obj_meta| {
            futures::future::ready(
                obj_meta
                    .map(|m| ManifestLocation::try_from(m).ok())
                    .transpose(),
            )
        })
        .boxed()
}

/// Convert object metadata to ManifestLocation for detached manifests.
fn detached_manifest_location_from_meta(
    meta: object_store::ObjectMeta,
) -> Option<ManifestLocation> {
    let filename = meta.location.filename()?;
    let version = ManifestNamingScheme::parse_detached_version(filename)?;
    Some(ManifestLocation {
        version,
        path: meta.location,
        size: Some(meta.size),
        naming_scheme: ManifestNamingScheme::V2,
        e_tag: meta.e_tag,
    })
}

/// List all detached manifest files in the versions directory.
pub fn list_detached_manifests<'a>(
    base_path: &Path,
    object_store: &'a dyn OSObjectStore,
) -> impl Stream<Item = Result<ManifestLocation>> + 'a {
    object_store
        .read_dir_all(&base_path.clone().join(VERSIONS_DIR), None)
        .filter_map(|obj_meta| {
            futures::future::ready(
                obj_meta
                    .map(detached_manifest_location_from_meta)
                    .transpose(),
            )
        })
        .boxed()
}

fn make_staging_manifest_path(base: &Path) -> Result<Path> {
    let id = uuid::Uuid::new_v4().to_string();
    Path::parse(format!("{base}-{id}")).map_err(|e| Error::io_source(Box::new(e)))
}

#[cfg(feature = "dynamodb")]
const DDB_URL_QUERY_KEY: &str = "ddbTableName";

/// Handle commits that prevent conflicting writes.
///
/// Commit implementations ensure that if there are multiple concurrent writers
/// attempting to write the next version of a table, only one will win. In order
/// to work, all writers must use the same commit handler type.
/// This trait is also responsible for resolving where the manifests live.
///
// TODO: pub(crate)
#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait CommitHandler: Debug + Send + Sync {
    /// Whether a not-found result from [`Self::resolve_version_location`] is
    /// definitive immediately after a commit attempt.
    ///
    /// Handlers backed by an eventually consistent or external source of
    /// truth should keep the conservative default. This prevents callers from
    /// deleting files that a newly committed manifest may reference while the
    /// manifest is not yet visible through the resolver.
    fn is_version_not_found_definitive(&self) -> bool {
        false
    }

    /// Whether an error should still be returned after readback proves that
    /// the manifest from the current commit attempt landed.
    ///
    /// The conservative default preserves errors from custom handlers. Built-in
    /// object-store handlers override this because their commit errors may be
    /// ambiguous transport failures whose successful outcome is authoritative.
    fn propagate_commit_error_after_success(&self) -> bool {
        true
    }

    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<ManifestLocation> {
        Ok(current_manifest_path(object_store, base_path).await?)
    }

    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn OSObjectStore,
    ) -> Result<ManifestLocation> {
        default_resolve_version(base_path, version, object_store).await
    }

    /// Check whether an attached manifest version exists without loading it.
    ///
    /// The default implementation probes the deterministic manifest path for
    /// the given naming scheme. Commit handlers with an external source of
    /// truth should override this method.
    async fn version_exists(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn OSObjectStore,
        naming_scheme: ManifestNamingScheme,
    ) -> Result<bool> {
        let path = naming_scheme.manifest_path(base_path, version);
        match object_store.head(&path).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// List detached manifest locations.
    ///
    /// Returns a stream of detached manifest locations in arbitrary order.
    fn list_detached_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        list_detached_manifests(base_path, &object_store.inner).boxed()
    }

    /// If `sorted_descending` is `true`, the stream will yield manifests in descending
    /// order of version. When the object store has a lexicographically
    /// ordered list and the naming scheme is V2, this will use an optimized
    /// list operation. Otherwise, it will list all manifests and sort them
    /// in memory. When `sorted_descending` is `false`, the stream will yield manifests
    /// in arbitrary order.
    fn list_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        sorted_descending: bool,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        let underlying_stream = list_manifests(base_path, &object_store.inner);

        if !sorted_descending {
            return underlying_stream.boxed();
        }

        async fn sort_stream(
            input_stream: impl futures::Stream<Item = Result<ManifestLocation>> + Unpin,
        ) -> Result<impl Stream<Item = Result<ManifestLocation>> + Unpin> {
            let mut locations = input_stream.try_collect::<Vec<_>>().await?;
            locations.sort_by_key(|m| std::cmp::Reverse(m.version));
            Ok(futures::stream::iter(locations.into_iter().map(Ok)))
        }

        // If the object store supports lexicographically ordered lists and
        // the naming scheme is V2, we can use an optimized list operation.
        if object_store.list_is_lexically_ordered {
            // We don't know the naming scheme until we see the first manifest.
            let mut peekable = underlying_stream.peekable();

            futures::stream::once(async move {
                let naming_scheme = match Pin::new(&mut peekable).peek().await {
                    Some(Ok(m)) => m.naming_scheme,
                    // If we get an error or no manifests are found, we default
                    // to V2 naming scheme, since it doesn't matter.
                    Some(Err(_)) => ManifestNamingScheme::V2,
                    None => ManifestNamingScheme::V2,
                };

                if naming_scheme == ManifestNamingScheme::V2 {
                    // If the first manifest is V2, we can use the optimized list operation.
                    Ok(Either::Left(peekable))
                } else {
                    sort_stream(peekable).await.map(Either::Right)
                }
            })
            .try_flatten()
            .boxed()
        } else {
            // If the object store does not support lexicographically ordered lists,
            // we need to sort the manifests in memory. Systems where this isn't
            // supported (local fs, S3 express) are typically fast enough
            // that this is not a problem.
            futures::stream::once(sort_stream(underlying_stream))
                .try_flatten()
                .boxed()
        }
    }

    /// List manifest locations with version `> since_version`, in descending
    /// order of version.
    ///
    /// On lexically-ordered stores this is the standard listing with early
    /// termination. On non-lexically-ordered stores (e.g. S3 Express) it uses
    /// the version hint to avoid an O(n) listing, falling back to a full
    /// listing if the hint is missing or stale.
    fn list_manifest_locations_since<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        since_version: u64,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        if !uses_version_hint(object_store) {
            return self
                .list_manifest_locations(base_path, object_store, true)
                .try_take_while(move |loc| future::ready(Ok(loc.version > since_version)))
                .boxed();
        }

        let base_path = base_path.clone();
        futures::stream::once(async move {
            let locations = match list_manifests_since_version_with_hint(
                object_store,
                &base_path,
                since_version,
            )
            .await
            {
                Some(locations) => locations,
                None => {
                    let mut locations = list_manifests(&base_path, &object_store.inner)
                        .try_collect::<Vec<_>>()
                        .await?;
                    locations.retain(|loc| loc.version > since_version);
                    locations.sort_by_key(|loc| std::cmp::Reverse(loc.version));
                    locations
                }
            };
            Ok::<_, Error>(futures::stream::iter(locations.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    /// Commit a manifest.
    ///
    /// This function should return an [CommitError::CommitConflict] if another
    /// transaction has already been committed to the path.
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError>;

    /// Delete the recorded manifest information for a dataset at the base_path
    async fn delete(&self, _base_path: &Path) -> Result<()> {
        Ok(())
    }
}

async fn default_resolve_version(
    base_path: &Path,
    version: u64,
    object_store: &dyn OSObjectStore,
) -> Result<ManifestLocation> {
    if is_detached_version(version) {
        return Ok(ManifestLocation {
            version,
            // Detached versions are not supported with V1 naming scheme.  If we need
            // to support in the future we could use a different prefix (e.g. 'x' or something)
            naming_scheme: ManifestNamingScheme::V2,
            // Both V1 and V2 should give the same path for detached versions
            path: ManifestNamingScheme::V2.manifest_path(base_path, version),
            size: None,
            e_tag: None,
        });
    }

    // try V2, fallback to V1.
    let scheme = ManifestNamingScheme::V2;
    let path = scheme.manifest_path(base_path, version);
    match object_store.head(&path).await {
        Ok(meta) => Ok(ManifestLocation {
            version,
            path,
            size: Some(meta.size),
            naming_scheme: scheme,
            e_tag: meta.e_tag,
        }),
        Err(ObjectStoreError::NotFound { .. }) => {
            // fallback to V1
            let scheme = ManifestNamingScheme::V1;
            Ok(ManifestLocation {
                version,
                path: scheme.manifest_path(base_path, version),
                size: None,
                naming_scheme: scheme,
                e_tag: None,
            })
        }
        Err(e) => Err(e.into()),
    }
}
/// Adapt an object_store credentials into AWS SDK creds
#[cfg(feature = "dynamodb")]
#[derive(Debug)]
struct OSObjectStoreToAwsCredAdaptor(AwsCredentialProvider);

#[cfg(feature = "dynamodb")]
impl ProvideCredentials for OSObjectStoreToAwsCredAdaptor {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async {
            let creds = self
                .0
                .get_credential()
                .await
                .map_err(|e| CredentialsError::provider_error(Box::new(e)))?;
            Ok(aws_credential_types::Credentials::new(
                &creds.key_id,
                &creds.secret_key,
                creds.token.clone(),
                Some(
                    SystemTime::now()
                        .checked_add(Duration::from_secs(
                            60 * 10, //  10 min
                        ))
                        .expect("overflow"),
                ),
                "",
            ))
        })
    }
}

#[cfg(feature = "dynamodb")]
async fn build_dynamodb_external_store(
    table_name: &str,
    creds: AwsCredentialProvider,
    region: &str,
    endpoint: Option<String>,
    app_name: &str,
) -> Result<Arc<dyn ExternalManifestStore>> {
    use super::commit::dynamodb::DynamoDBExternalManifestStore;
    use aws_sdk_dynamodb::{
        Client,
        config::{IdentityCache, Region, retry::RetryConfig},
    };

    let mut dynamodb_config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version_latest()
        .region(Some(Region::new(region.to_string())))
        .credentials_provider(OSObjectStoreToAwsCredAdaptor(creds))
        // caching should be handled by passed AwsCredentialProvider
        .identity_cache(IdentityCache::no_cache())
        // Be more resilient to transient network issues.
        // 5 attempts = 1 initial + 4 retries with exponential backoff.
        .retry_config(RetryConfig::standard().with_max_attempts(5));

    if let Some(endpoint) = endpoint {
        dynamodb_config = dynamodb_config.endpoint_url(endpoint);
    }
    let client = Client::from_conf(dynamodb_config.build());

    DynamoDBExternalManifestStore::new_external_store(client.into(), table_name, app_name).await
}

pub async fn commit_handler_from_url(
    url_or_path: &str,
    // This looks unused if dynamodb feature disabled
    #[allow(unused_variables)] options: &Option<ObjectStoreParams>,
) -> Result<Arc<dyn CommitHandler>> {
    let local_handler: Arc<dyn CommitHandler> = if cfg!(windows) {
        Arc::new(RenameCommitHandler)
    } else {
        Arc::new(ConditionalPutCommitHandler)
    };

    let url = match Url::parse(url_or_path) {
        Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
            // On Windows, the drive is parsed as a scheme
            return Ok(local_handler);
        }
        Ok(url) => url,
        Err(_) => {
            return Ok(local_handler);
        }
    };

    match url.scheme() {
        "file" | "file-object-store" => Ok(local_handler),
        "s3" | "gs" | "az" | "abfss" | "memory" | "oss" | "cos" | "tos" | "shared-memory"
        | "goosefs" => Ok(Arc::new(ConditionalPutCommitHandler)),
        #[cfg(not(feature = "dynamodb"))]
        "s3+ddb" => Err(Error::invalid_input_source(
            "`s3+ddb://` scheme requires `dynamodb` feature to be enabled".into(),
        )),
        #[cfg(feature = "dynamodb")]
        "s3+ddb" => {
            if url.query_pairs().count() != 1 {
                return Err(Error::invalid_input_source(
                    "`s3+ddb://` scheme and expects exactly one query `ddbTableName`".into(),
                ));
            }
            let table_name = match url.query_pairs().next() {
                Some((Cow::Borrowed(key), Cow::Borrowed(table_name)))
                    if key == DDB_URL_QUERY_KEY =>
                {
                    if table_name.is_empty() {
                        return Err(Error::invalid_input_source(
                            "`s3+ddb://` scheme requires non empty dynamodb table name".into(),
                        ));
                    }
                    table_name
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        "`s3+ddb://` scheme and expects exactly one query `ddbTableName`".into(),
                    ));
                }
            };
            let options = options.clone().unwrap_or_default();
            let storage_options_raw =
                StorageOptions(options.storage_options().cloned().unwrap_or_default());
            let dynamo_endpoint = get_dynamodb_endpoint(&storage_options_raw);
            let storage_options = storage_options_raw.as_s3_options();

            let region = storage_options.get(&AmazonS3ConfigKey::Region).cloned();

            // Get accessor from the options
            let accessor = options.get_accessor();

            let provider_scheme = storage_options_raw.aws_provider_scheme()?;

            let (aws_creds, region) = build_aws_credential(
                options.s3_credentials_refresh_offset,
                options.aws_credentials.clone(),
                Some(&storage_options),
                region,
                accessor,
                provider_scheme,
            )
            .await?;

            Ok(Arc::new(ExternalManifestCommitHandler {
                external_manifest_store: build_dynamodb_external_store(
                    table_name,
                    aws_creds.clone(),
                    &region,
                    dynamo_endpoint,
                    "lancedb",
                )
                .await?,
            }))
        }
        _ => Ok(Arc::new(UnsafeCommitHandler)),
    }
}

#[cfg(feature = "dynamodb")]
fn get_dynamodb_endpoint(storage_options: &StorageOptions) -> Option<String> {
    if let Some(endpoint) = storage_options.0.get("dynamodb_endpoint") {
        Some(endpoint.clone())
    } else {
        std::env::var("DYNAMODB_ENDPOINT").ok()
    }
}

/// Errors that can occur when committing a manifest.
#[derive(Debug)]
pub enum CommitError {
    /// Another transaction has already been written to the path
    CommitConflict,
    /// Something else went wrong
    OtherError(Error),
}

impl From<Error> for CommitError {
    fn from(e: Error) -> Self {
        Self::OtherError(e)
    }
}

impl From<CommitError> for Error {
    fn from(e: CommitError) -> Self {
        match e {
            CommitError::CommitConflict => Self::internal("Commit conflict".to_string()),
            CommitError::OtherError(e) => e,
        }
    }
}

/// Whether we have issued a warning about using the unsafe commit handler.
static WARNED_ON_UNSAFE_COMMIT: AtomicBool = AtomicBool::new(false);

/// A naive commit implementation that does not prevent conflicting writes.
///
/// This will log a warning the first time it is used.
pub struct UnsafeCommitHandler;

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
impl CommitHandler for UnsafeCommitHandler {
    fn is_version_not_found_definitive(&self) -> bool {
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        false
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // Log a one-time warning
        if !WARNED_ON_UNSAFE_COMMIT.load(std::sync::atomic::Ordering::Relaxed) {
            WARNED_ON_UNSAFE_COMMIT.store(true, std::sync::atomic::Ordering::Relaxed);
            log::warn!(
                "Using unsafe commit handler. Concurrent writes may result in data loss. \
                 Consider providing a commit handler that prevents conflicting writes."
            );
        }

        let version_path = naming_scheme.manifest_path(base_path, manifest.version);
        let res =
            manifest_writer(object_store, manifest, indices, &version_path, transaction).await?;

        write_version_hint(object_store, base_path, manifest.version).await;

        Ok(ManifestLocation {
            version: manifest.version,
            size: Some(res.size as u64),
            naming_scheme,
            path: version_path,
            e_tag: res.e_tag,
        })
    }
}

impl Debug for UnsafeCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnsafeCommitHandler").finish()
    }
}

/// A commit implementation that uses a lock to prevent conflicting writes.
#[async_trait::async_trait]
pub trait CommitLock: Debug {
    type Lease: CommitLease;

    /// Attempt to lock the table for the given version.
    ///
    /// If it is already locked by another transaction, wait until it is unlocked.
    /// Once it is unlocked, return [CommitError::CommitConflict] if the version
    /// has already been committed. Otherwise, return the lock.
    ///
    /// To prevent poisoned locks, it's recommended to set a timeout on the lock
    /// of at least 30 seconds.
    ///
    /// It is not required that the lock tracks the version. It is provided in
    /// case the locking is handled by a catalog service that needs to know the
    /// current version of the table.
    async fn lock(&self, version: u64) -> std::result::Result<Self::Lease, CommitError>;
}

#[async_trait::async_trait]
pub trait CommitLease: Send + Sync {
    /// Return the lease, indicating whether the commit was successful.
    ///
    /// Implementations should tolerate being called more than once: if a commit
    /// is cancelled (e.g. by a timeout) while `release` is in flight, a
    /// best-effort `release(false)` may be issued afterwards from the drop path.
    async fn release(&self, success: bool) -> std::result::Result<(), CommitError>;
}

/// Guards a [CommitLease] so the lock is released even if the commit future is
/// dropped (e.g. cancelled by a commit timeout) before reaching an explicit
/// release.
///
/// [CommitLease::release] is async and cannot be awaited from `Drop`, so on the
/// drop path we spawn a best-effort background task that releases the lock with
/// `success = false`. Without this, a cancelled commit would leak the lock until
/// the lease's own TTL expired, blocking other writers in the meantime.
struct LeaseGuard<L: CommitLease + 'static> {
    lease: Option<L>,
}

impl<L: CommitLease + 'static> LeaseGuard<L> {
    fn new(lease: L) -> Self {
        Self { lease: Some(lease) }
    }

    /// Explicitly release the lease, consuming the guard so `Drop` is a no-op.
    async fn release(mut self, success: bool) -> std::result::Result<(), CommitError> {
        // Keep the lease inside the guard across the await so that, if this
        // future is cancelled mid-release (e.g. the release call itself hangs
        // and the commit timeout fires), `Drop` still issues a best-effort
        // release. Only clear it once the release has fully completed.
        let result = {
            let lease = self
                .lease
                .as_ref()
                .expect("LeaseGuard released more than once");
            lease.release(success).await
        };
        self.lease = None;
        result
    }
}

impl<L: CommitLease + 'static> Drop for LeaseGuard<L> {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            // The guard was dropped without an explicit release, meaning the
            // commit future was cancelled while holding the lock. We can't await
            // in `Drop`, so spawn a best-effort release. If there is no runtime,
            // leave the lease for its TTL to reclaim.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = lease.release(false).await;
                });
            }
        }
    }
}

#[async_trait::async_trait]
impl<T: CommitLock + Send + Sync> CommitHandler for T
where
    T::Lease: 'static,
{
    fn is_version_not_found_definitive(&self) -> bool {
        true
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let path = naming_scheme.manifest_path(base_path, manifest.version);
        // Hold the lease in a guard so the lock is released even if this future
        // is cancelled before we reach an explicit release below. The explicit
        // releases are still preferred since they report the correct success
        // flag and surface release errors; the guard only covers cancellation.
        let lease = LeaseGuard::new(self.lock(manifest.version).await?);

        // Head the location and make sure it's not already committed
        match object_store.inner.head(&path).await {
            Ok(_) => {
                // The path already exists, so it's already committed
                // Release the lock
                lease.release(false).await?;

                return Err(CommitError::CommitConflict);
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => {
                // Something else went wrong
                // Release the lock
                lease.release(false).await?;

                return Err(CommitError::OtherError(e.into()));
            }
        }
        let res = manifest_writer(object_store, manifest, indices, &path, transaction).await;

        // Release the lock
        lease.release(res.is_ok()).await?;

        let res = res?;

        write_version_hint(object_store, base_path, manifest.version).await;

        Ok(ManifestLocation {
            version: manifest.version,
            size: Some(res.size as u64),
            naming_scheme,
            path,
            e_tag: res.e_tag,
        })
    }
}

#[async_trait::async_trait]
impl<T: CommitLock + Send + Sync> CommitHandler for Arc<T>
where
    T::Lease: 'static,
{
    fn is_version_not_found_definitive(&self) -> bool {
        self.as_ref().is_version_not_found_definitive()
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        self.as_ref().propagate_commit_error_after_success()
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.as_ref()
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
            )
            .await
    }
}

/// A commit implementation that uses a temporary path and renames the object.
///
/// This only works for object stores that support atomic rename if not exist.
pub struct RenameCommitHandler;

#[async_trait::async_trait]
impl CommitHandler for RenameCommitHandler {
    fn is_version_not_found_definitive(&self) -> bool {
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        false
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // Create a temporary object, then use `rename_if_not_exists` to commit.
        // If failed, clean up the temporary object.

        let path = naming_scheme.manifest_path(base_path, manifest.version);
        let tmp_path = make_staging_manifest_path(&path)?;

        let res = manifest_writer(object_store, manifest, indices, &tmp_path, transaction).await?;

        match object_store
            .inner
            .rename_if_not_exists(&tmp_path, &path)
            .await
        {
            Ok(_) => {
                // Successfully committed
                write_version_hint(object_store, base_path, manifest.version).await;
                Ok(ManifestLocation {
                    version: manifest.version,
                    path,
                    size: Some(res.size as u64),
                    naming_scheme,
                    e_tag: None, // Re-name can change e-tag.
                })
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                // Another transaction has already been committed
                // Attempt to clean up temporary object, but ignore errors if we can't
                let _ = object_store.delete(&tmp_path).await;

                return Err(CommitError::CommitConflict);
            }
            Err(e) => {
                // Something else went wrong
                return Err(CommitError::OtherError(e.into()));
            }
        }
    }
}

impl Debug for RenameCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenameCommitHandler").finish()
    }
}

pub struct ConditionalPutCommitHandler;

#[async_trait::async_trait]
impl CommitHandler for ConditionalPutCommitHandler {
    fn is_version_not_found_definitive(&self) -> bool {
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        false
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let path = naming_scheme.manifest_path(base_path, manifest.version);

        let memory_store = ObjectStore::memory();
        let dummy_path = "dummy";
        manifest_writer(
            &memory_store,
            manifest,
            indices,
            &dummy_path.into(),
            transaction,
        )
        .await?;
        let dummy_data = memory_store.read_one_all(&dummy_path.into()).await?;
        let size = dummy_data.len() as u64;
        let res = object_store
            .inner
            .put_opts(
                &path,
                dummy_data.into(),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| match err {
                ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. } => {
                    CommitError::CommitConflict
                }
                _ => CommitError::OtherError(err.into()),
            })?;

        write_version_hint(object_store, base_path, manifest.version).await;

        Ok(ManifestLocation {
            version: manifest.version,
            path,
            size: Some(size),
            naming_scheme,
            e_tag: res.e_tag,
        })
    }
}

impl Debug for ConditionalPutCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionalPutCommitHandler").finish()
    }
}

#[derive(Debug, Clone)]
pub struct CommitConfig {
    pub num_retries: u32,
    pub skip_auto_cleanup: bool,
    // TODO: add isolation_level
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self {
            num_retries: 20,
            skip_auto_cleanup: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use lance_core::utils::tempfile::TempObjDir;

    use super::*;

    #[test]
    fn test_manifest_naming_scheme() {
        let v1 = ManifestNamingScheme::V1;
        let v2 = ManifestNamingScheme::V2;

        assert_eq!(
            v1.manifest_path(&Path::from("base"), 0),
            Path::from("base/_versions/0.manifest")
        );
        assert_eq!(
            v1.manifest_path(&Path::from("base"), 42),
            Path::from("base/_versions/42.manifest")
        );

        assert_eq!(
            v2.manifest_path(&Path::from("base"), 0),
            Path::from("base/_versions/18446744073709551615.manifest")
        );
        assert_eq!(
            v2.manifest_path(&Path::from("base"), 42),
            Path::from("base/_versions/18446744073709551573.manifest")
        );

        assert_eq!(v1.parse_version("0.manifest"), Some(0));
        assert_eq!(v1.parse_version("42.manifest"), Some(42));
        assert_eq!(
            v1.parse_version("42.manifest-cee4fbbb-eb19-4ea3-8ca7-54f5ec33dedc"),
            Some(42)
        );

        assert_eq!(v2.parse_version("18446744073709551615.manifest"), Some(0));
        assert_eq!(v2.parse_version("18446744073709551573.manifest"), Some(42));
        assert_eq!(
            v2.parse_version("18446744073709551573.manifest-cee4fbbb-eb19-4ea3-8ca7-54f5ec33dedc"),
            Some(42)
        );

        assert_eq!(ManifestNamingScheme::detect_scheme("0.manifest"), Some(v1));
        assert_eq!(
            ManifestNamingScheme::detect_scheme("18446744073709551615.manifest"),
            Some(v2)
        );
        assert_eq!(ManifestNamingScheme::detect_scheme("something else"), None);
    }

    #[tokio::test]
    async fn test_manifest_naming_migration() {
        let object_store = ObjectStore::memory();
        let base = Path::from("base");
        let versions_dir = base.clone().join(VERSIONS_DIR);

        // Write two v1 files and one v1
        let original_files = vec![
            versions_dir.clone().join("irrelevant"),
            ManifestNamingScheme::V1.manifest_path(&base, 0),
            ManifestNamingScheme::V2.manifest_path(&base, 1),
        ];
        for path in original_files {
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        migrate_scheme_to_v2(&object_store, &base).await.unwrap();

        let expected_files = vec![
            ManifestNamingScheme::V2.manifest_path(&base, 1),
            ManifestNamingScheme::V2.manifest_path(&base, 0),
            versions_dir.clone().join("irrelevant"),
        ];
        let actual_files = object_store
            .inner
            .list(Some(&versions_dir))
            .map_ok(|res| res.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(actual_files, expected_files);
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_list_manifests_sorted(
        #[values(true, false)] lexical_list_store: bool,
        #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)]
        naming_scheme: ManifestNamingScheme,
    ) {
        let tempdir;
        let (object_store, base) = if lexical_list_store {
            (Box::new(ObjectStore::memory()), Path::from("base"))
        } else {
            tempdir = TempObjDir::default();
            let path = tempdir.clone().join("base");
            let store = Box::new(ObjectStore::local());
            assert!(!store.list_is_lexically_ordered);
            (store, path)
        };

        // Write 12 manifest files, latest first
        let mut expected_paths = Vec::new();
        for i in (0..12).rev() {
            let path = naming_scheme.manifest_path(&base, i);
            object_store.put(&path, b"".as_slice()).await.unwrap();
            expected_paths.push(path);
        }

        let actual_versions = ConditionalPutCommitHandler
            .list_manifest_locations(&base, &object_store, true)
            .map_ok(|location| location.path)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(actual_versions, expected_paths);
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_current_manifest_path(
        #[values(true, false)] lexical_list_store: bool,
        #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)]
        naming_scheme: ManifestNamingScheme,
    ) {
        // Use memory store for both cases to avoid local FS special codepath.
        // Modify list_is_lexically_ordered to simulate different object stores.
        let mut object_store = ObjectStore::memory();
        object_store.list_is_lexically_ordered = lexical_list_store;
        let object_store = Box::new(object_store);
        let base = Path::from("base");

        // Write 12 manifest files in non-sequential order
        for version in [5, 2, 11, 0, 8, 3, 10, 1, 7, 4, 9, 6] {
            let path = naming_scheme.manifest_path(&base, version);
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        let location = current_manifest_path(&object_store, &base).await.unwrap();

        assert_eq!(location.version, 11);
        assert_eq!(location.naming_scheme, naming_scheme);
        assert_eq!(location.path, naming_scheme.manifest_path(&base, 11));
    }

    /// A memory store that reports `list_is_lexically_ordered == false`, like
    /// S3 Express, so the version-hint paths are exercised.
    fn non_lexical_memory_store() -> Box<ObjectStore> {
        let mut object_store = ObjectStore::memory();
        object_store.list_is_lexically_ordered = false;
        Box::new(object_store)
    }

    #[tokio::test]
    async fn test_write_version_hint() {
        let base = Path::from("base");

        // No hint is written on lexically-ordered stores (it would not be read).
        let lexical = ObjectStore::memory();
        write_version_hint(&lexical, &base, 42).await;
        assert_eq!(read_version_from_hint(&lexical, &base).await, None);

        let object_store = non_lexical_memory_store();
        write_version_hint(&object_store, &base, 42).await;
        assert_eq!(read_version_from_hint(&object_store, &base).await, Some(42));

        // A later commit overwrites the hint.
        write_version_hint(&object_store, &base, 100).await;
        assert_eq!(
            read_version_from_hint(&object_store, &base).await,
            Some(100)
        );

        // Detached versions are never written to the hint.
        write_version_hint(
            &object_store,
            &base,
            crate::format::DETACHED_VERSION_MASK | 7,
        )
        .await;
        assert_eq!(
            read_version_from_hint(&object_store, &base).await,
            Some(100)
        );

        // A corrupt / non-JSON hint file is treated as missing.
        let hint_path = version_hint_path(&base);
        object_store
            .put(&hint_path, b"not json".as_slice())
            .await
            .unwrap();
        assert_eq!(read_version_from_hint(&object_store, &base).await, None);
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_read_version_hint_and_probe(
        #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)]
        naming_scheme: ManifestNamingScheme,
    ) {
        let object_store = non_lexical_memory_store();
        let base = Path::from("base");

        // No hint file yet.
        assert!(
            read_version_hint_and_probe(&object_store, &base)
                .await
                .is_none()
        );

        for version in 1..=5 {
            object_store
                .put(&naming_scheme.manifest_path(&base, version), b"".as_slice())
                .await
                .unwrap();
        }

        // Stale hint: should probe forward and find version 5.
        write_version_hint(&object_store, &base, 3).await;
        let location = read_version_hint_and_probe(&object_store, &base)
            .await
            .unwrap();
        assert_eq!(location.version, 5);
        assert_eq!(location.naming_scheme, naming_scheme);

        // Up-to-date hint: returns version 5 directly.
        write_version_hint(&object_store, &base, 5).await;
        let location = read_version_hint_and_probe(&object_store, &base)
            .await
            .unwrap();
        assert_eq!(location.version, 5);

        // Hint points past the latest version: not usable.
        write_version_hint(&object_store, &base, 10).await;
        assert!(
            read_version_hint_and_probe(&object_store, &base)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_list_manifests_since_version_with_hint() {
        let object_store = non_lexical_memory_store();
        let base = Path::from("base");
        let scheme = ManifestNamingScheme::V2;

        for version in 1..=10 {
            object_store
                .put(&scheme.manifest_path(&base, version), b"".as_slice())
                .await
                .unwrap();
        }

        // No hint yet -> not usable, caller must fall back.
        assert!(
            list_manifests_since_version_with_hint(&object_store, &base, 7)
                .await
                .is_none()
        );

        // Hint exactly at the read version -> fast path, nothing new.
        write_version_hint(&object_store, &base, 10).await;
        assert!(matches!(
            list_manifests_since_version_with_hint(&object_store, &base, 10).await,
            Some(v) if v.is_empty()
        ));

        // Hint ahead of the read version, with a gap to fill (8, 9) plus probing
        // from the hint (10). Results are descending by version.
        let locations = list_manifests_since_version_with_hint(&object_store, &base, 7)
            .await
            .unwrap();
        assert_eq!(
            locations.iter().map(|l| l.version).collect::<Vec<_>>(),
            vec![10, 9, 8]
        );

        // Slightly stale hint (points at 8) still probes up to the true latest.
        write_version_hint(&object_store, &base, 8).await;
        let locations = list_manifests_since_version_with_hint(&object_store, &base, 7)
            .await
            .unwrap();
        assert_eq!(
            locations.iter().map(|l| l.version).collect::<Vec<_>>(),
            vec![10, 9, 8]
        );

        // Hint points past the latest -> not usable, caller falls back.
        write_version_hint(&object_store, &base, 20).await;
        assert!(
            list_manifests_since_version_with_hint(&object_store, &base, 7)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_current_manifest_path_with_hint_non_lexical() {
        // Simulate S3 Express (non-lexically ordered list) with many versions.
        let object_store = non_lexical_memory_store();
        let base = Path::from("base");
        let naming_scheme = ManifestNamingScheme::V2;

        for version in 1..=100 {
            object_store
                .put(&naming_scheme.manifest_path(&base, version), b"".as_slice())
                .await
                .unwrap();
        }

        // Slightly stale hint: probing from 98 still resolves the true latest.
        write_version_hint(&object_store, &base, 98).await;
        let location = current_manifest_path(&object_store, &base).await.unwrap();
        assert_eq!(location.version, 100);
    }

    #[tokio::test]
    async fn test_current_manifest_path_with_stale_hint_falls_back_to_listing() {
        let object_store = non_lexical_memory_store();
        let base = Path::from("base");
        let naming_scheme = ManifestNamingScheme::V2;

        // Only version 5 exists, but the hint claims version 10.
        object_store
            .put(&naming_scheme.manifest_path(&base, 5), b"".as_slice())
            .await
            .unwrap();
        write_version_hint(&object_store, &base, 10).await;

        // The stale hint is ignored; listing finds version 5.
        let location = current_manifest_path(&object_store, &base).await.unwrap();
        assert_eq!(location.version, 5);
    }

    #[test]
    fn test_parse_detached_version() {
        // Valid detached version filenames
        assert_eq!(
            ManifestNamingScheme::parse_detached_version("d12345.manifest"),
            Some(12345)
        );
        assert_eq!(
            ManifestNamingScheme::parse_detached_version("d9223372036854775808.manifest"),
            Some(9223372036854775808)
        );

        // Invalid: not starting with 'd' prefix
        assert_eq!(
            ManifestNamingScheme::parse_detached_version("12345.manifest"),
            None
        );

        // Invalid: regular V2 manifest
        assert_eq!(
            ManifestNamingScheme::parse_detached_version("18446744073709551615.manifest"),
            None
        );

        // Invalid: no extension
        assert_eq!(ManifestNamingScheme::parse_detached_version("d12345"), None);
    }

    #[tokio::test]
    async fn test_list_detached_manifests() {
        use crate::format::DETACHED_VERSION_MASK;
        use futures::TryStreamExt;

        let object_store = ObjectStore::memory();
        let base = Path::from("base");
        let versions_dir = base.clone().join(VERSIONS_DIR);

        // Create some regular manifests
        for version in [1, 2, 3] {
            let path = ManifestNamingScheme::V2.manifest_path(&base, version);
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        // Create some detached manifests
        let detached_versions: Vec<u64> = vec![
            100 | DETACHED_VERSION_MASK,
            200 | DETACHED_VERSION_MASK,
            300 | DETACHED_VERSION_MASK,
        ];
        for version in &detached_versions {
            let path = versions_dir.clone().join(format!("d{}.manifest", version));
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        // List detached manifests
        let detached_locations: Vec<ManifestLocation> =
            list_detached_manifests(&base, &object_store.inner)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(detached_locations.len(), 3);
        for loc in &detached_locations {
            assert_eq!(loc.naming_scheme, ManifestNamingScheme::V2);
        }

        let mut found_versions: Vec<u64> = detached_locations.iter().map(|l| l.version).collect();
        found_versions.sort();
        let mut expected_versions = detached_versions.clone();
        expected_versions.sort();
        assert_eq!(found_versions, expected_versions);
    }

    #[tokio::test]
    #[rstest::rstest]
    #[case::memory("memory://bucket-a/ds")]
    #[case::shared_memory("shared-memory://bucket-a/ds")]
    #[case::s3("s3://bucket-a/ds")]
    #[case::gs("gs://bucket-a/ds")]
    #[case::az("az://bucket-a/ds")]
    #[case::abfss("abfss://bucket-a/ds")]
    #[case::oss("oss://bucket-a/ds")]
    #[case::cos("cos://bucket-a/ds")]
    #[case::tos("tos://bucket-a/ds")]
    #[case::goosefs("goosefs://bucket-a/ds")]
    async fn test_commit_handler_from_url_conditional_put_schemes(#[case] url: &str) {
        // Every scheme whose store supports atomic put-if-not-exists must
        // route to ConditionalPutCommitHandler — otherwise concurrent writers
        // fall through to UnsafeCommitHandler and silently clobber each
        // other's manifests.
        let handler = commit_handler_from_url(url, &None).await.unwrap();
        assert_eq!(
            format!("{:?}", handler),
            "ConditionalPutCommitHandler",
            "{url} should route to ConditionalPutCommitHandler",
        );
    }

    /// A [CommitLock] whose lease records whether it was released, so we can
    /// assert the lock does not leak when the commit future is cancelled.
    #[derive(Debug)]
    struct TrackingLock {
        released: Arc<AtomicBool>,
    }

    struct TrackingLease {
        released: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl CommitLock for TrackingLock {
        type Lease = TrackingLease;
        async fn lock(&self, _version: u64) -> std::result::Result<Self::Lease, CommitError> {
            Ok(TrackingLease {
                released: self.released.clone(),
            })
        }
    }

    #[async_trait::async_trait]
    impl CommitLease for TrackingLease {
        async fn release(&self, _success: bool) -> std::result::Result<(), CommitError> {
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A [CommitLock] whose lease hangs on its first `release` call but completes
    /// on subsequent ones, so we can assert the drop-path best-effort release
    /// fires when a commit is cancelled *during* the explicit release.
    #[derive(Debug)]
    struct HangingReleaseLock {
        release_calls: Arc<AtomicUsize>,
        released: Arc<AtomicBool>,
    }

    struct HangingReleaseLease {
        release_calls: Arc<AtomicUsize>,
        released: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl CommitLock for HangingReleaseLock {
        type Lease = HangingReleaseLease;
        async fn lock(&self, _version: u64) -> std::result::Result<Self::Lease, CommitError> {
            Ok(HangingReleaseLease {
                release_calls: self.release_calls.clone(),
                released: self.released.clone(),
            })
        }
    }

    #[async_trait::async_trait]
    impl CommitLease for HangingReleaseLease {
        async fn release(&self, _success: bool) -> std::result::Result<(), CommitError> {
            // The first release (the explicit one) hangs, simulating a release
            // call that stalls long enough for the commit timeout to fire. The
            // best-effort release issued from `Drop` is the second call and
            // succeeds.
            if self
                .release_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
            {
                future::pending::<()>().await;
                unreachable!()
            }
            self.released
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A manifest writer that succeeds immediately, so the commit reaches the
    /// explicit lease release.
    fn succeeding_manifest_writer<'a>(
        _object_store: &'a ObjectStore,
        _manifest: &'a mut Manifest,
        _indices: Option<Vec<IndexMetadata>>,
        _path: &'a Path,
        _transaction: Option<Transaction>,
    ) -> BoxFuture<'a, Result<WriteResult>> {
        Box::pin(async move { Ok(WriteResult::default()) })
    }

    /// A manifest writer that never completes, simulating a hung object store.
    fn hanging_manifest_writer<'a>(
        _object_store: &'a ObjectStore,
        _manifest: &'a mut Manifest,
        _indices: Option<Vec<IndexMetadata>>,
        _path: &'a Path,
        _transaction: Option<Transaction>,
    ) -> BoxFuture<'a, Result<WriteResult>> {
        Box::pin(async move {
            future::pending::<()>().await;
            unreachable!()
        })
    }

    /// Cancelling a commit (as a commit timeout does) while the lock is held must
    /// still release the lock; otherwise it leaks until the lease's TTL expires.
    #[tokio::test]
    async fn test_commit_lock_released_on_cancellation() {
        use std::collections::HashMap;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use lance_core::datatypes::Schema;
        use lance_file::version::{ConcreteFileVersion, LanceFileVersion};

        use crate::format::DataStorageFormat;

        let released = Arc::new(AtomicBool::new(false));
        let lock = TrackingLock {
            released: released.clone(),
        };

        let object_store = ObjectStore::memory();
        let base_path = Path::from("test");
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("i", DataType::Int32, false)]);
        let mut manifest = Manifest::new(
            Schema::try_from(&arrow_schema).unwrap(),
            Arc::new(vec![]),
            DataStorageFormat::new(ConcreteFileVersion::from(LanceFileVersion::Stable)),
            HashMap::new(),
        );

        // The commit will hang on the manifest writer while holding the lock.
        // Cancel it the same way a commit timeout would: drop the future.
        let commit_fut = lock.commit(
            &mut manifest,
            None,
            &base_path,
            &object_store,
            hanging_manifest_writer,
            ManifestNamingScheme::V2,
            None,
        );
        let timed_out = tokio::time::timeout(Duration::from_millis(50), commit_fut).await;
        assert!(timed_out.is_err(), "commit should not have completed");

        // The drop guard releases the lock on a background task; wait for it.
        for _ in 0..100 {
            if released.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            released.load(Ordering::SeqCst),
            "lock must be released after the commit future is cancelled"
        );
    }

    /// Cancelling a commit *during* the explicit lease release (e.g. the release
    /// call itself hangs and the commit timeout fires) must still release the
    /// lock via the drop-path best-effort release.
    #[tokio::test]
    async fn test_commit_lock_released_on_cancellation_during_release() {
        use std::collections::HashMap;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        use lance_core::datatypes::Schema;
        use lance_file::version::{ConcreteFileVersion, LanceFileVersion};

        use crate::format::DataStorageFormat;

        let release_calls = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        let lock = HangingReleaseLock {
            release_calls: release_calls.clone(),
            released: released.clone(),
        };

        let object_store = ObjectStore::memory();
        let base_path = Path::from("test");
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("i", DataType::Int32, false)]);
        let mut manifest = Manifest::new(
            Schema::try_from(&arrow_schema).unwrap(),
            Arc::new(vec![]),
            DataStorageFormat::new(ConcreteFileVersion::from(LanceFileVersion::Stable)),
            HashMap::new(),
        );

        // The manifest writer succeeds, so the commit reaches the explicit
        // release, which hangs. Cancel it the same way a commit timeout would.
        let commit_fut = lock.commit(
            &mut manifest,
            None,
            &base_path,
            &object_store,
            succeeding_manifest_writer,
            ManifestNamingScheme::V2,
            None,
        );
        let timed_out = tokio::time::timeout(Duration::from_millis(50), commit_fut).await;
        assert!(timed_out.is_err(), "commit should not have completed");

        // The drop guard issues a best-effort release on a background task; wait
        // for it. This is the second release call (the first one hung).
        for _ in 0..100 {
            if released.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            released.load(Ordering::SeqCst),
            "lock must be released even when cancelled during the explicit release"
        );
        assert_eq!(
            release_calls.load(Ordering::SeqCst),
            2,
            "expected the hung explicit release plus one best-effort drop release"
        );
    }
}
