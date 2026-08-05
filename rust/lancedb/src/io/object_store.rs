// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object-store providers and adapters used by LanceDB.

use std::{fmt::Formatter, sync::Arc};

use futures::{StreamExt, TryFutureExt, stream::BoxStream};
use lance::io::WrappingObjectStore;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart, path::Path,
};

use async_trait::async_trait;

#[cfg(any(windows, test))]
use lance_core::{Error as LanceError, Result as LanceResult};
#[cfg(any(windows, test))]
use lance_io::object_store::{
    DEFAULT_LOCAL_IO_PARALLELISM, ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry,
    StorageOptions,
};
#[cfg(any(windows, test))]
use object_store::local::LocalFileSystem;
#[cfg(any(windows, test))]
use url::Url;

#[cfg(test)]
pub mod io_tracking;

/// A file-store provider that anchors each request at its filesystem root.
///
/// On Windows, an unprefixed [`LocalFileSystem`] cannot service UNC paths. Its
/// conversion to an object-store [`Path`] drops the UNC host, so subsequent I/O
/// is directed at a different local path. Anchoring the store at the drive or
/// UNC-share root keeps the UNC authority in the filesystem prefix and exposes
/// only paths relative to that prefix to `object_store`.
///
/// The returned Lance store deliberately uses the `file-object-store` scheme.
/// The regular `file` scheme enables optimized readers and writers that bypass
/// the configured object store and would reintroduce the broken UNC conversion.
#[cfg(any(windows, test))]
#[derive(Debug, Default)]
struct PrefixedFileStoreProvider;

#[cfg(any(windows, test))]
impl PrefixedFileStoreProvider {
    fn root_and_relative_path(url: &Url) -> LanceResult<(std::path::PathBuf, Path)> {
        let filesystem_path = url.to_file_path().map_err(|_| {
            LanceError::invalid_input(format!("Unable to convert URL '{url}' to a local path"))
        })?;

        let root = filesystem_path.ancestors().last().ok_or_else(|| {
            LanceError::invalid_input(format!(
                "Local path '{}' has no filesystem root",
                filesystem_path.display()
            ))
        })?;

        let relative = filesystem_path.strip_prefix(root).map_err(|_| {
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

        Ok((root.to_path_buf(), Path::parse(relative)?))
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
        let store = Arc::new(LocalFileSystem::new_with_prefix(root)?);
        let location = Url::parse("file-object-store:///").expect("static URL must be valid");
        let storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());

        Ok(lance::io::ObjectStore::new(
            store,
            location,
            params.block_size,
            params.object_store_wrapper.clone(),
            false,
            false,
            DEFAULT_LOCAL_IO_PARALLELISM,
            storage_options.download_retry_count(),
            params.storage_options(),
        ))
    }

    fn extract_path(&self, url: &Url) -> LanceResult<Path> {
        Self::root_and_relative_path(url).map(|(_, path)| path)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&std::collections::HashMap<String, String>>,
    ) -> LanceResult<String> {
        let (root, _) = Self::root_and_relative_path(url)?;
        let root = root.canonicalize()?;
        Ok(format!("file${}", root.display()))
    }
}

/// Replace Lance's default Windows file provider with one that preserves UNC
/// roots by using `LocalFileSystem::new_with_prefix`.
#[cfg(any(windows, test))]
pub(crate) fn register_windows_file_store(registry: &Arc<ObjectStoreRegistry>) {
    registry.insert("file", Arc::new(PrefixedFileStoreProvider));
}

#[cfg(test)]
mod prefixed_file_store_test {
    use super::*;

    #[tokio::test]
    async fn anchors_new_and_existing_directories_at_a_filesystem_prefix() {
        let tempdir = tempfile::tempdir().unwrap();
        let database_path = tempdir.path().join("database");
        std::fs::create_dir(&database_path).unwrap();
        let table_path = database_path.join("test.lance");
        let table_url = Url::from_directory_path(&table_path).unwrap();

        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert("file", Arc::new(PrefixedFileStoreProvider));

        // A new table is relative to its filesystem root. The non-`file`
        // scheme proves Lance will not bypass this prefixed store.
        let (store, base_path) = lance::io::ObjectStore::from_uri_and_params(
            registry.clone(),
            table_url.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .unwrap();
        assert_eq!(store.scheme(), "file-object-store");
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
            self.secondary.copy_opts(from, to, options.clone()).await?;
            self.primary.copy_opts(from, to, options).await?;
            Ok(())
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
    use object_store::local::LocalFileSystem;
    use tempfile;

    use crate::{
        connect,
        query::{ExecutableQuery, QueryBase},
        table::WriteOptions,
    };

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
