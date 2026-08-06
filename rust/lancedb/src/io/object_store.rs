// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object-store providers and adapters used by LanceDB.

use std::{fmt::Formatter, sync::Arc};

#[cfg(any(windows, test))]
use futures::TryStreamExt;
use futures::{StreamExt, TryFutureExt, stream::BoxStream};
use lance::io::WrappingObjectStore;
#[cfg(any(windows, test))]
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction},
    io::commit::{
        CommitError, CommitHandler, ManifestLocation, ManifestNamingScheme, ManifestWriter,
        RenameCommitHandler,
    },
};
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart, path::Path,
};

use async_trait::async_trait;

#[cfg(any(windows, test))]
use lance_core::{Error as LanceError, Result as LanceResult};
#[cfg(test)]
use lance_io::object_store::ObjectStoreRegistry;
#[cfg(any(windows, test))]
use lance_io::object_store::{
    DEFAULT_LOCAL_IO_PARALLELISM, ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
#[cfg(any(windows, test))]
use object_store::local::LocalFileSystem;
#[cfg(any(windows, test))]
use url::Url;

#[cfg(test)]
pub mod io_tracking;

/// A local commit handler that resolves the latest manifest through the object store.
///
/// Lance's native local shortcut reconstructs the selected manifest from a
/// filesystem path. On Windows that conversion drops the authority from a UNC
/// path. Listing through the already-rooted object store preserves the structural
/// server/share prefix while retaining the normal atomic-rename commit behavior.
#[derive(Debug)]
#[cfg(any(windows, test))]
struct RootedFileCommitHandler;

#[cfg(any(windows, test))]
#[async_trait]
impl CommitHandler for RootedFileCommitHandler {
    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &lance::io::ObjectStore,
    ) -> LanceResult<ManifestLocation> {
        self.list_manifest_locations(base_path, object_store, true)
            .try_next()
            .await?
            .ok_or_else(|| LanceError::not_found(base_path.to_string()))
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &lance::io::ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        RenameCommitHandler
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

#[cfg(any(windows, test))]
pub(crate) fn rooted_file_commit_handler() -> Arc<dyn CommitHandler> {
    Arc::new(RootedFileCommitHandler)
}

/// A file-store provider that anchors each request at its filesystem root.
///
/// On Windows, an unprefixed [`LocalFileSystem`] cannot service UNC paths. Its
/// conversion to an object-store [`Path`] drops the UNC host, so subsequent I/O
/// is directed at a different local path. Anchoring the store at the drive or
/// UNC-share root keeps the UNC authority in the filesystem prefix and exposes
/// only paths relative to that prefix to `object_store`.
///
/// Extracted paths retain the native drive or UNC-share root as a structural
/// first component. This keeps Lance's local classification and optimized I/O
/// safe without recovering absolute path provenance from ambiguous path text.
#[cfg(any(windows, test))]
#[derive(Debug)]
struct PrefixedFileStoreProvider;

#[cfg(any(windows, test))]
impl PrefixedFileStoreProvider {
    fn root_and_relative_path(url: &Url) -> LanceResult<(std::path::PathBuf, Path)> {
        let filesystem_path = url.to_file_path().map_err(|_| {
            LanceError::invalid_input(format!("Unable to convert URL '{url}' to a local path"))
        })?;

        let mut root = std::path::PathBuf::new();
        for component in filesystem_path.components() {
            match component {
                std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                    root.push(component.as_os_str());
                }
                _ => break,
            }
        }
        if root.as_os_str().is_empty() {
            return Err(LanceError::invalid_input(format!(
                "Local path '{}' has no filesystem root",
                filesystem_path.display()
            )));
        }

        let relative = filesystem_path.strip_prefix(&root).map_err(|_| {
            LanceError::invalid_input(format!(
                "Local path '{}' is not beneath store root '{}'",
                filesystem_path.display(),
                root.display()
            ))
        })?;
        let relative = relative
            .components()
            .filter_map(|component| match component {
                std::path::Component::Normal(part) => Some(part),
                _ => None,
            })
            .map(|part| {
                part.to_str().ok_or_else(|| {
                    LanceError::invalid_input(format!(
                        "Local path '{}' is not valid UTF-8",
                        filesystem_path.display()
                    ))
                })
            })
            .collect::<LanceResult<Vec<_>>>()?
            .join("/");

        Ok((root, Path::parse(relative)?))
    }

    /// Preserve the native filesystem root as a structural path component.
    ///
    /// `object_store::Path::from_absolute_path` converts a UNC path to its URL
    /// path and loses the server. Keeping `C:` or `\\server\share` as the first
    /// component distinguishes an absolute path from every relative path while
    /// remaining directly usable by Windows filesystem APIs.
    fn rooted_path(root: &std::path::Path, relative: &Path) -> LanceResult<Path> {
        let root = root.to_string_lossy();
        let root = root.trim_end_matches(['/', '\\']);
        if root.is_empty() {
            return Ok(relative.clone());
        }
        Ok(relative
            .parts()
            .fold(Path::parse(root)?, |path, part| path.join(part)))
    }
}

/// A local store rooted at a Windows drive or UNC share.
///
/// Most calls use paths returned by [`PrefixedFileStoreProvider`], which are
/// relative to `root`. Some Lance operations retain an existing object store
/// while independently re-extracting a Windows file URI with the default file
/// provider. Those paths include the drive (`C:/...`) or UNC share
/// (`share/...`) again. Normalize that absolute alias before delegating so the
/// filesystem prefix is never applied twice.
#[cfg(any(windows, test))]
#[derive(Debug, Clone)]
struct RootedLocalFileSystem {
    inner: Arc<LocalFileSystem>,
    root: std::path::PathBuf,
    absolute_alias: Path,
}

#[cfg(any(windows, test))]
impl RootedLocalFileSystem {
    fn new(root: std::path::PathBuf) -> LanceResult<Self> {
        let absolute_alias = PrefixedFileStoreProvider::rooted_path(&root, &Path::default())?;
        Ok(Self {
            inner: Arc::new(LocalFileSystem::new_with_prefix(&root)?),
            root,
            absolute_alias,
        })
    }

    fn path_from_parts<'a>(parts: impl Iterator<Item = object_store::path::PathPart<'a>>) -> Path {
        parts.fold(Path::default(), |path, part| path.join(part))
    }

    fn normalize(&self, path: &Path) -> Path {
        if self.absolute_alias.as_ref().is_empty() {
            return path.clone();
        }
        let Some(suffix) = path.prefix_match(&self.absolute_alias) else {
            return path.clone();
        };
        Self::path_from_parts(suffix)
    }

    fn restore_prefix(&self, path: Path, requested: &Path, normalized: &Path) -> Path {
        if requested == normalized {
            return path;
        }
        path.prefix_match(normalized)
            .map(|suffix| suffix.fold(requested.clone(), |path, part| path.join(part)))
            .unwrap_or(path)
    }
}

#[cfg(any(windows, test))]
impl std::fmt::Display for RootedLocalFileSystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RootedLocalFileSystem({})", self.root.display())
    }
}

#[cfg(any(windows, test))]
#[async_trait]
impl ObjectStore for RootedLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner
            .put_opts(&self.normalize(location), payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner
            .put_multipart_opts(&self.normalize(location), options)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let normalized = self.normalize(location);
        let mut result = self.inner.get_opts(&normalized, options).await?;
        result.meta.location = location.clone();
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .map(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let normalized = store.normalize(&location);
                    store.inner.delete(&normalized).await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let requested = prefix.cloned().unwrap_or_default();
        let normalized = self.normalize(&requested);
        let store = self.clone();
        self.inner
            .list(prefix.map(|_| &normalized))
            .map(move |result| {
                result.map(|mut meta| {
                    meta.location = store.restore_prefix(meta.location, &requested, &normalized);
                    meta
                })
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let requested = prefix.cloned().unwrap_or_default();
        let normalized = self.normalize(&requested);
        let mut result = self
            .inner
            .list_with_delimiter(prefix.map(|_| &normalized))
            .await?;
        for meta in &mut result.objects {
            meta.location = self.restore_prefix(meta.location.clone(), &requested, &normalized);
        }
        for path in &mut result.common_prefixes {
            *path = self.restore_prefix(path.clone(), &requested, &normalized);
        }
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner
            .copy_opts(&self.normalize(from), &self.normalize(to), options)
            .await
    }
}

#[cfg(any(windows, test))]
#[async_trait]
impl ObjectStoreProvider for PrefixedFileStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> LanceResult<lance::io::ObjectStore> {
        let (root, _) = Self::root_and_relative_path(&base_path)?;
        let raw_store: Arc<dyn ObjectStore> = Arc::new(RootedLocalFileSystem::new(root)?);
        // Native local shortcuts are safe only when the rooted filesystem is
        // the final store. An arbitrary wrapper can redirect I/O, so use
        // Lance's non-cloud object-store route in that case. This keeps local
        // scan planning without enabling native copy/delete or the io_uring
        // scheduler, all of which would bypass the wrapper.
        let location = if params.object_store_wrapper.is_some() {
            Url::parse("memory:///").expect("static URL must be valid")
        } else {
            Url::parse("file:///").expect("static URL must be valid")
        };
        let storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());

        // ObjectStore::new initializes the private local-store fields. The
        // registry owns tracing, custom wrapper, and I/O tracker installation,
        // so restore the raw store before returning to avoid applying them twice.
        let mut store = lance::io::ObjectStore::new(
            raw_store.clone(),
            location,
            Some(params.block_size.unwrap_or(4 * 1024)),
            None,
            false,
            false,
            DEFAULT_LOCAL_IO_PARALLELISM,
            storage_options.download_retry_count(),
            params.storage_options(),
        );
        store.inner = raw_store;
        store.store_prefix =
            self.calculate_object_store_prefix(&base_path, params.storage_options())?;
        Ok(store)
    }

    fn extract_path(&self, url: &Url) -> LanceResult<Path> {
        let (root, relative) = Self::root_and_relative_path(url)?;
        Self::rooted_path(&root, &relative)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&std::collections::HashMap<String, String>>,
    ) -> LanceResult<String> {
        let (root, _) = Self::root_and_relative_path(url)?;
        let root = root.canonicalize()?;
        // One store per drive or UNC share keeps registry and metrics
        // cardinality bounded. Absolute-vs-relative provenance is carried by
        // the extracted path instead of the cache key.
        Ok(format!("file${}", root.display()))
    }
}

/// Build LanceDB's default session with the Windows file fallback installed.
///
/// Callers that provide a Session retain its registry unchanged.
pub(crate) fn new_default_session() -> Arc<lance::session::Session> {
    let session = Arc::new(lance::session::Session::default());
    #[cfg(windows)]
    session
        .store_registry()
        .insert("file", Arc::new(PrefixedFileStoreProvider));
    session
}

#[cfg(test)]
pub(crate) fn new_prefixed_file_session() -> Arc<lance::session::Session> {
    let session = Arc::new(lance::session::Session::default());
    session
        .store_registry()
        .insert("file", Arc::new(PrefixedFileStoreProvider));
    session
}

#[cfg(test)]
mod prefixed_file_store_test {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct CountingWrapper {
        calls: AtomicUsize,
    }

    impl WrappingObjectStore for CountingWrapper {
        fn wrap(
            &self,
            _store_prefix: &str,
            original: Arc<dyn ObjectStore>,
        ) -> Arc<dyn ObjectStore> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            original
        }
    }

    #[derive(Debug, Default)]
    struct MemoryRedirectWrapper {
        store: Arc<InMemory>,
    }

    impl WrappingObjectStore for MemoryRedirectWrapper {
        fn wrap(
            &self,
            _store_prefix: &str,
            _original: Arc<dyn ObjectStore>,
        ) -> Arc<dyn ObjectStore> {
            self.store.clone()
        }
    }

    #[tokio::test]
    async fn anchors_new_and_existing_directories_at_a_filesystem_prefix() {
        let tempdir = tempfile::tempdir().unwrap();
        let database_path = tempdir.path().join("database");
        std::fs::create_dir(&database_path).unwrap();
        let table_path = database_path.join("test.lance");
        let table_url = Url::from_directory_path(&table_path).unwrap();

        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));

        // The extracted path remains absolute for Lance's local fast paths,
        // while the inner store strips the structural root before delegation.
        let (store, base_path) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            table_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert_eq!(store.scheme(), "file");
        assert!(store.is_local());
        assert!(!store.is_cloud());
        assert_eq!(store.block_size(), 4 * 1024);
        assert_eq!(store.io_parallelism(), DEFAULT_LOCAL_IO_PARALLELISM);
        assert_eq!(base_path.filename(), Some("test.lance"));
        let initial_base_path = base_path.clone();

        let marker = base_path.join("marker");
        store
            .inner
            .put(&marker, bytes::Bytes::from_static(b"new").into())
            .await
            .unwrap();
        assert_eq!(std::fs::read(table_path.join("marker")).unwrap(), b"new");

        // Once the table directory exists, a fresh store uses the same stable
        // root and object-store path.
        drop(store);
        let (store, base_path) = lance::io::ObjectStore::from_uri_and_params(
            registry,
            table_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert_eq!(base_path, initial_base_path);
        let contents = store
            .inner
            .get(&base_path.join("marker"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(contents.as_ref(), b"new");
    }

    #[tokio::test]
    async fn applies_wrapper_and_io_tracking_once() {
        let tempdir = tempfile::tempdir().unwrap();
        let table_url = Url::from_directory_path(tempdir.path().join("test.lance")).unwrap();
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));
        let wrapper = Arc::new(CountingWrapper::default());
        let params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..Default::default()
        };

        let (store, base_path) =
            lance::io::ObjectStore::from_uri_and_params(registry, table_url.as_str(), &params)
                .await
                .unwrap();
        assert_eq!(wrapper.calls.load(Ordering::Relaxed), 1);
        assert_eq!(store.scheme(), "memory");
        assert!(!store.is_local());
        assert!(!store.is_cloud());
        assert!(!store.prefers_lite_scheduler());

        store
            .inner
            .put(
                &base_path.join("marker"),
                bytes::Bytes::from_static(b"tracked").into(),
            )
            .await
            .unwrap();
        let stats = store.io_tracker().stats();
        assert_eq!(stats.write_iops, 1);
        assert_eq!(stats.written_bytes, 7);
    }

    #[tokio::test]
    async fn wrapped_store_cleanup_does_not_bypass_the_wrapper() {
        let tempdir = tempfile::tempdir().unwrap();
        let table_path = tempdir.path().join("test.lance");
        std::fs::create_dir(&table_path).unwrap();
        std::fs::write(table_path.join("native-marker"), b"native").unwrap();

        let table_url = Url::from_directory_path(&table_path).unwrap();
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));
        let wrapper = Arc::new(MemoryRedirectWrapper::default());
        let params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..Default::default()
        };
        let (store, base_path) =
            lance::io::ObjectStore::from_uri_and_params(registry, table_url.as_str(), &params)
                .await
                .unwrap();
        let wrapped_marker = base_path.clone().join("wrapped-marker");
        store
            .inner
            .put(
                &wrapped_marker,
                bytes::Bytes::from_static(b"wrapped").into(),
            )
            .await
            .unwrap();

        store.remove_dir_all(base_path).await.unwrap();

        assert!(table_path.join("native-marker").exists());
        assert!(matches!(
            wrapper.store.head(&wrapped_marker).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn rooted_commit_handler_uses_store_paths_for_latest_manifest() {
        let tempdir = tempfile::tempdir().unwrap();
        let versions = tempdir.path().join("_versions");
        std::fs::create_dir(&versions).unwrap();
        std::fs::write(versions.join("2.manifest"), b"native").unwrap();

        let base_path = Path::from_absolute_path(tempdir.path()).unwrap();
        let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        raw_store
            .put(
                &base_path.clone().join("_versions").join("1.manifest"),
                bytes::Bytes::from_static(b"wrapped").into(),
            )
            .await
            .unwrap();
        let mut store = lance::io::ObjectStore::new(
            raw_store.clone(),
            Url::parse("file:///").unwrap(),
            Some(4 * 1024),
            None,
            false,
            false,
            DEFAULT_LOCAL_IO_PARALLELISM,
            0,
            None,
        );
        store.inner = raw_store;

        let native = RenameCommitHandler
            .resolve_latest_location(&base_path, &store)
            .await
            .unwrap();
        assert_eq!(native.version, 2);

        let rooted = rooted_file_commit_handler()
            .resolve_latest_location(&base_path, &store)
            .await
            .unwrap();
        assert_eq!(rooted.version, 1);
        assert_eq!(
            rooted.path,
            base_path.clone().join("_versions").join("1.manifest")
        );
    }

    #[tokio::test]
    async fn reuses_store_cache_across_database_paths() {
        let tempdir = tempfile::tempdir().unwrap();
        let first_url = Url::from_directory_path(tempdir.path().join("database")).unwrap();
        let second_url = Url::from_directory_path(tempdir.path().join("share/database")).unwrap();
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));

        let (first, _) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            first_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        let (second, _) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            second_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.stats().misses, 1);
        assert_eq!(registry.stats().hits, 1);
    }

    #[tokio::test]
    async fn reuses_database_store_for_table_and_clone_targets() {
        let tempdir = tempfile::tempdir().unwrap();
        let database_url = Url::from_directory_path(tempdir.path().join("database")).unwrap();
        let source_url =
            Url::from_directory_path(tempdir.path().join("database/source.lance")).unwrap();
        let target_url =
            Url::from_directory_path(tempdir.path().join("database/target.lance")).unwrap();
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));

        let (database, _) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            database_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        let (source, _) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            source_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        let (target, _) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            target_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();

        assert!(Arc::ptr_eq(&database, &source));
        assert!(Arc::ptr_eq(&database, &target));
        assert_eq!(registry.stats().misses, 1);
        assert_eq!(registry.stats().hits, 2);
    }

    #[test]
    fn normalizes_absolute_drive_and_unc_aliases() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut drive_store = RootedLocalFileSystem::new(tempdir.path().to_path_buf()).unwrap();
        drive_store.absolute_alias = Path::from("C:");
        assert_eq!(
            drive_store.normalize(&Path::from("C:/Users/db/table.lance")),
            Path::from("Users/db/table.lance")
        );

        let mut unc_store = RootedLocalFileSystem::new(tempdir.path().to_path_buf()).unwrap();
        unc_store.absolute_alias = Path::parse(r"\\server\share").unwrap();
        assert_eq!(
            unc_store.normalize(&Path::parse(r"\\server\share/share/db/table.lance").unwrap()),
            Path::from("share/db/table.lance")
        );
        assert_eq!(
            unc_store.normalize(&Path::from("share/db/table.lance")),
            Path::from("share/db/table.lance")
        );
    }

    #[test]
    fn relative_unc_alias_does_not_cross_database_roots() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut store = RootedLocalFileSystem::new(tempdir.path().to_path_buf()).unwrap();
        store.absolute_alias = Path::parse(r"\\server\share").unwrap();

        let relative = Path::from("share/database/table.lance/marker");
        assert_eq!(store.normalize(&relative), relative);
        assert_eq!(
            store.normalize(
                &Path::parse(r"\\server\share/share/database/table.lance/marker").unwrap()
            ),
            relative
        );
    }

    #[tokio::test]
    async fn routes_unc_alias_lifecycle_through_the_prefix() {
        let tempdir = tempfile::tempdir().unwrap();
        let mut store = RootedLocalFileSystem::new(tempdir.path().to_path_buf()).unwrap();
        store.absolute_alias = Path::parse(r"\\server\share").unwrap();
        let table = Path::parse(r"\\server\share/share/db/test.lance").unwrap();
        let marker = table.clone().join("marker");

        store
            .put(&marker, bytes::Bytes::from_static(b"unc").into())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(tempdir.path().join("share/db/test.lance/marker")).unwrap(),
            b"unc"
        );

        let listed = store.list(Some(&table)).collect::<Vec<_>>().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].as_ref().unwrap().location, marker);
        assert_eq!(
            store
                .get(&marker)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"unc"
        );

        store.delete(&marker).await.unwrap();
        assert!(!tempdir.path().join("share/db/test.lance/marker").exists());
    }

    #[cfg(windows)]
    #[test]
    fn extracts_drive_and_unc_share_roots() {
        let (root, path) = PrefixedFileStoreProvider::root_and_relative_path(
            &Url::parse("file:///C:/database").unwrap(),
        )
        .unwrap();
        assert_eq!(root, std::path::PathBuf::from(r"C:\"));
        assert_eq!(path, Path::from("database"));

        let (root, path) = PrefixedFileStoreProvider::root_and_relative_path(
            &Url::parse("file://server/share/database").unwrap(),
        )
        .unwrap();
        assert_eq!(root, std::path::PathBuf::from(r"\\server\share\"));
        assert_eq!(path, Path::from("database"));
    }

    #[test]
    fn preserves_drive_and_unc_roots_in_extracted_paths() {
        assert_eq!(
            PrefixedFileStoreProvider::rooted_path(
                std::path::Path::new(r"C:\"),
                &Path::from("database/table.lance")
            )
            .unwrap(),
            Path::from("C:/database/table.lance")
        );
        assert_eq!(
            PrefixedFileStoreProvider::rooted_path(
                std::path::Path::new(r"\\server\share\"),
                &Path::from("database/table.lance")
            )
            .unwrap(),
            Path::parse(r"\\server\share/database/table.lance").unwrap()
        );
    }
}

#[derive(Debug)]
struct MirroringObjectStore {
    primary: Arc<dyn ObjectStore>,
    secondary: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for MirroringObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MirrowingObjectStore")?;
        writeln!(f, "primary:")?;
        self.primary.fmt(f)?;
        writeln!(f, "secondary:")?;
        self.secondary.fmt(f)?;
        Ok(())
    }
}

trait PrimaryOnly {
    fn primary_only(&self) -> bool;
}

impl PrimaryOnly for Path {
    fn primary_only(&self) -> bool {
        self.filename().unwrap_or("") == "_latest.manifest"
    }
}

/// An object store that mirrors write to secondsry object store first
/// and than commit to primary object store.
///
/// This is meant to mirrow writes to a less-durable but lower-latency
/// store. We have primary store that is durable but slow, and a secondary
/// store that is fast but not asdurable
///
/// Note: this object store does not mirror writes to *.manifest files
#[async_trait]
impl ObjectStore for MirroringObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if location.primary_only() {
            self.primary.put_opts(location, bytes, options).await
        } else {
            self.secondary
                .put_opts(location, bytes.clone(), options.clone())
                .await?;
            self.primary.put_opts(location, bytes, options).await
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if location.primary_only() {
            return self.primary.put_multipart_opts(location, opts).await;
        }

        let secondary = self
            .secondary
            .put_multipart_opts(location, opts.clone())
            .await?;
        let primary = self.primary.put_multipart_opts(location, opts).await?;

        Ok(Box::new(MirroringUpload { primary, secondary }))
    }

    // Reads are routed to primary only
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.primary.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.primary.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.primary.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let primary = self.primary.clone();
        let secondary = self.secondary.clone();
        locations
            .map(move |location| {
                let primary = primary.clone();
                let secondary = secondary.clone();
                async move {
                    let location = location?;
                    if !location.primary_only() {
                        match secondary.delete(&location).await {
                            Err(Error::NotFound { .. }) | Ok(_) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    primary.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        if to.primary_only() {
            self.primary.copy_opts(from, to, options).await
        } else {
            // The secondary store can be process-local and less durable than the
            // primary, so a source written by another process may not exist here
            // or may be evicted before the copy begins.
            match self.secondary.copy_opts(from, to, options.clone()).await {
                Ok(()) | Err(Error::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
            self.primary.copy_opts(from, to, options).await
        }
    }
}

#[derive(Debug)]
struct MirroringUpload {
    primary: Box<dyn MultipartUpload>,
    secondary: Box<dyn MultipartUpload>,
}

#[async_trait]
impl MultipartUpload for MirroringUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let put_primary = self.primary.put_part(data.clone());
        let put_secondary = self.secondary.put_part(data);
        Box::pin(put_secondary.and_then(|_| put_primary))
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.secondary.complete().await?;
        self.primary.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.secondary.abort().await?;
        self.primary.abort().await
    }
}

#[derive(Debug)]
pub struct MirroringObjectStoreWrapper {
    secondary: Arc<dyn ObjectStore>,
}

impl MirroringObjectStoreWrapper {
    pub fn new(secondary: Arc<dyn ObjectStore>) -> Self {
        Self { secondary }
    }
}

impl WrappingObjectStore for MirroringObjectStoreWrapper {
    fn wrap(&self, _store_prefix: &str, primary: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(MirroringObjectStore {
            primary,
            secondary: self.secondary.clone(),
        })
    }
}

// windows pathing can't be simply concatenated
#[cfg(all(test, not(windows)))]
mod test {
    use super::*;

    use futures::TryStreamExt;
    use lance::{dataset::WriteParams, io::ObjectStoreParams};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use object_store::{local::LocalFileSystem, memory::InMemory};
    use std::time::Duration;
    use tempfile;

    use crate::{
        connect,
        query::{ExecutableQuery, QueryBase},
        table::WriteOptions,
    };

    #[derive(Debug)]
    struct EvictBeforeCopyStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Display for EvictBeforeCopyStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "EvictBeforeCopyStore")
        }
    }

    #[async_trait]
    impl ObjectStore for EvictBeforeCopyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.delete(from).await?;
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn test_copy_when_source_is_missing_from_secondary() {
        let primary_dir = tempfile::tempdir().unwrap();
        let secondary_dir = tempfile::tempdir().unwrap();
        let primary: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(primary_dir.path()).unwrap());
        let secondary: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(secondary_dir.path()).unwrap());
        let store = MirroringObjectStore {
            primary: primary.clone(),
            secondary: secondary.clone(),
        };
        let staging = Path::from("_versions/1.manifest-staging");
        let finalized = Path::from("_versions/1.manifest");

        primary
            .put(&staging, "manifest contents".into())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), store.copy(&staging, &finalized))
            .await
            .expect("copy should not hang when the secondary source is missing")
            .unwrap();

        let copied = primary
            .get(&finalized)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(copied, "manifest contents");
        assert!(matches!(
            secondary.head(&finalized).await,
            Err(Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn test_copy_when_secondary_source_disappears_after_head() {
        let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let secondary_inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let secondary: Arc<dyn ObjectStore> = Arc::new(EvictBeforeCopyStore {
            inner: secondary_inner.clone(),
        });
        let store = MirroringObjectStore {
            primary: primary.clone(),
            secondary,
        };
        let staging = Path::from("_versions/1.manifest-staging");
        let finalized = Path::from("_versions/1.manifest");

        primary
            .put(&staging, "manifest contents".into())
            .await
            .unwrap();
        secondary_inner
            .put(&staging, "manifest contents".into())
            .await
            .unwrap();

        store.copy(&staging, &finalized).await.unwrap();

        let copied = primary
            .get(&finalized)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(copied, "manifest contents");
        assert!(matches!(
            secondary_inner.head(&finalized).await,
            Err(Error::NotFound { .. })
        ));
    }

    // This test is ignored because lance 3.0 introduced LocalWriter optimization
    // that bypasses the object store wrapper for local writes. The mirroring feature
    // still works for remote/cloud storage, but can't be tested with local storage.
    // See lance commit c878af433 "perf: create local writer for efficient local writes"
    #[ignore]
    #[tokio::test]
    async fn test_e2e() {
        let dir1 = tempfile::tempdir().unwrap().keep().canonicalize().unwrap();
        let dir2 = tempfile::tempdir().unwrap().keep().canonicalize().unwrap();

        let secondary_store = LocalFileSystem::new_with_prefix(dir2.to_str().unwrap()).unwrap();
        let object_store_wrapper = Arc::new(MirroringObjectStoreWrapper {
            secondary: Arc::new(secondary_store),
        });

        let db = connect(dir1.to_str().unwrap()).execute().await.unwrap();

        let mut param = WriteParams::default();
        let store_params = ObjectStoreParams {
            object_store_wrapper: Some(object_store_wrapper),
            ..Default::default()
        };
        param.store_params = Some(store_params);

        let mut datagen = BatchGenerator::new();
        datagen = datagen.col(Box::<IncrementingInt32>::default());
        datagen = datagen.col(Box::new(RandomVector::default().named("vector".into())));

        let data: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(datagen.batch(100));
        let res = db
            .create_table("test", data)
            .write_options(WriteOptions {
                lance_write_params: Some(param),
            })
            .execute()
            .await;

        // leave this here for easy debugging
        let t = res.unwrap();

        assert_eq!(t.count_rows(None).await.unwrap(), 100);

        let q = t
            .query()
            .limit(10)
            .nearest_to(&[0.1, 0.1, 0.1, 0.1])
            .unwrap()
            .execute()
            .await
            .unwrap();

        let bateches = q.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(bateches.len(), 1);
        assert_eq!(bateches[0].num_rows(), 10);

        use walkdir::WalkDir;

        let primary_location = dir1.join("test.lance").canonicalize().unwrap();
        let secondary_location = dir2.join(primary_location.strip_prefix("/").unwrap());

        // Skip lance internal directories (_versions, _transactions) and manifest files
        let should_skip = |path: &std::path::Path| -> bool {
            let path_str = path.to_str().unwrap();
            path_str.contains("_latest.manifest")
                || path_str.contains("_versions")
                || path_str.contains("_transactions")
        };

        let primary_files: Vec<_> = WalkDir::new(&primary_location)
            .into_iter()
            .filter_entry(|e| !should_skip(e.path()))
            .filter_map(|e| e.ok())
            .map(|e| {
                e.path()
                    .strip_prefix(&primary_location)
                    .unwrap()
                    .to_path_buf()
            })
            .collect();

        let secondary_files: Vec<_> = WalkDir::new(&secondary_location)
            .into_iter()
            .filter_entry(|e| !should_skip(e.path()))
            .filter_map(|e| e.ok())
            .map(|e| {
                e.path()
                    .strip_prefix(&secondary_location)
                    .unwrap()
                    .to_path_buf()
            })
            .collect();

        assert_eq!(primary_files, secondary_files, "File lists should match");
    }
}
