// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Keep the tests in `lance` crate because it has dependency on [Dataset].
#[cfg(test)]
mod test {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{collections::HashMap, time::Duration};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::{StreamExt, TryStreamExt, future::join_all};
    use lance_core::{Error, Result};
    use lance_io::object_store::ObjectStore;
    use lance_table::io::commit::external_manifest::{
        ExternalManifestCommitHandler, ExternalManifestStore,
    };
    use lance_table::io::commit::{CommitHandler, ManifestLocation, ManifestNamingScheme};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore as OSObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
        PutResult, RenameOptions, Result as OSResult, local::LocalFileSystem, path::Path,
    };
    use tokio::sync::Mutex;

    use crate::dataset::builder::DatasetBuilder;
    use crate::{
        Dataset,
        dataset::{ReadParams, WriteMode, WriteParams},
    };
    use lance_core::utils::tempfile::TempStrDir;

    // sleep for 1 second to simulate a slow external store on write
    #[derive(Debug)]
    struct SleepyExternalManifestStore {
        store: Arc<Mutex<HashMap<(String, u64), String>>>,
    }

    impl SleepyExternalManifestStore {
        fn new() -> Self {
            Self {
                store: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl ExternalManifestStore for SleepyExternalManifestStore {
        /// Get the manifest path for a given uri and version
        async fn get(&self, uri: &str, version: u64) -> Result<String> {
            let store = self.store.lock().await;
            match store.get(&(uri.to_string(), version)) {
                Some(path) => Ok(path.clone()),
                None => Err(Error::not_found(uri.to_string())),
            }
        }

        /// Get the latest version of a dataset at the path
        async fn get_latest_version(&self, uri: &str) -> Result<Option<(u64, String)>> {
            let store = self.store.lock().await;
            let max_version = store
                .iter()
                .filter_map(|((stored_uri, version), manifest_uri)| {
                    if stored_uri == uri {
                        Some((version, manifest_uri))
                    } else {
                        None
                    }
                })
                .max_by_key(|v| v.0);

            Ok(max_version.map(|(version, uri)| (*version, uri.clone())))
        }

        /// Put the manifest path for a given uri and version, should fail if the version already exists
        async fn put_if_not_exists(
            &self,
            uri: &str,
            version: u64,
            path: &str,
            _size: u64,
            _e_tag: Option<String>,
        ) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut store = self.store.lock().await;
            match store.get(&(uri.to_string(), version)) {
                Some(_) => Err(Error::io(format!(
                    "manifest already exists for uri: {}, version: {}",
                    uri, version
                ))),
                None => {
                    store.insert((uri.to_string(), version), path.to_string());
                    Ok(())
                }
            }
        }

        /// Put the manifest path for a given uri and version, should fail if the version already exists
        async fn put_if_exists(
            &self,
            uri: &str,
            version: u64,
            path: &str,
            _size: u64,
            _e_tag: Option<String>,
        ) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let mut store = self.store.lock().await;
            match store.get(&(uri.to_string(), version)) {
                Some(_) => {
                    store.insert((uri.to_string(), version), path.to_string());
                    Ok(())
                }
                None => Err(Error::io(format!(
                    "manifest already exists for uri: {}, version: {}",
                    uri, version
                ))),
            }
        }
    }

    #[derive(Debug)]
    struct StaticExternalManifestStore {
        location: ManifestLocation,
        verify_store: Option<Arc<dyn OSObjectStore>>,
    }

    #[async_trait]
    impl ExternalManifestStore for StaticExternalManifestStore {
        async fn get(&self, _uri: &str, version: u64) -> Result<String> {
            if version == self.location.version {
                Ok(self.location.path.to_string())
            } else {
                Err(Error::not_found(format!("version {}", version)))
            }
        }

        async fn get_manifest_location(
            &self,
            _base_uri: &str,
            version: u64,
        ) -> Result<ManifestLocation> {
            if version == self.location.version {
                Ok(self.location.clone())
            } else {
                Err(Error::not_found(format!("version {}", version)))
            }
        }

        async fn get_latest_version(&self, _uri: &str) -> Result<Option<(u64, String)>> {
            Ok(Some((
                self.location.version,
                self.location.path.to_string(),
            )))
        }

        async fn get_latest_manifest_location(
            &self,
            _base_uri: &str,
        ) -> Result<Option<ManifestLocation>> {
            Ok(Some(self.location.clone()))
        }

        async fn put_if_not_exists(
            &self,
            _uri: &str,
            _version: u64,
            _path: &str,
            _size: u64,
            _e_tag: Option<String>,
        ) -> Result<()> {
            Ok(())
        }

        async fn put_if_exists(
            &self,
            _uri: &str,
            _version: u64,
            path: &str,
            size: u64,
            e_tag: Option<String>,
        ) -> Result<()> {
            if let Some(store) = &self.verify_store {
                let final_meta = store.head(&Path::from(path)).await?;
                assert_eq!(size, final_meta.size);
                assert_eq!(e_tag, final_meta.e_tag);
            }
            Ok(())
        }
    }

    fn read_params(handler: Arc<dyn CommitHandler>) -> ReadParams {
        ReadParams {
            commit_handler: Some(handler),
            ..Default::default()
        }
    }

    fn write_params(handler: Arc<dyn CommitHandler>) -> WriteParams {
        WriteParams {
            commit_handler: Some(handler),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn finalized_external_manifest_location_is_head_checked() {
        let sleepy_store = SleepyExternalManifestStore::new();
        let inner_store = sleepy_store.store.clone();
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(sleepy_store),
        };
        let object_store = ObjectStore::memory();
        let base_path = Path::from("repro");
        let version = 7;
        let final_path = ManifestNamingScheme::V2.manifest_path(&base_path, version);
        let body = b"manifest body";

        object_store
            .inner
            .put(&final_path, PutPayload::from_static(body))
            .await
            .expect("seed finalized manifest");
        inner_store
            .lock()
            .await
            .insert((base_path.to_string(), version), final_path.to_string());

        let location = handler
            .resolve_latest_location(&base_path, &object_store)
            .await
            .expect("resolve latest finalized manifest");

        assert_eq!(location.path, final_path);
        assert_eq!(location.size, Some(body.len() as u64));
        assert_eq!(location.naming_scheme, ManifestNamingScheme::V2);
    }

    #[tokio::test]
    async fn finalized_external_manifest_location_falls_back_to_v1() {
        let sleepy_store = SleepyExternalManifestStore::new();
        let inner_store = sleepy_store.store.clone();
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(sleepy_store),
        };
        let object_store = ObjectStore::memory();
        let base_path = Path::from("repro");
        let version = 7;
        let missing_v2_path = ManifestNamingScheme::V2.manifest_path(&base_path, version);
        let v1_path = ManifestNamingScheme::V1.manifest_path(&base_path, version);

        object_store
            .inner
            .put(&v1_path, PutPayload::from_static(b"v1 manifest body"))
            .await
            .expect("seed V1 manifest");
        inner_store.lock().await.insert(
            (base_path.to_string(), version),
            missing_v2_path.to_string(),
        );

        let latest_location = handler
            .resolve_latest_location(&base_path, &object_store)
            .await
            .expect("resolve latest should fall back to V1");
        assert_eq!(latest_location.path, v1_path);
        assert_eq!(latest_location.naming_scheme, ManifestNamingScheme::V1);

        let version_location = handler
            .resolve_version_location(&base_path, version, object_store.inner.as_ref())
            .await
            .expect("resolve version should fall back to V1");
        assert_eq!(version_location.path, v1_path);
        assert_eq!(version_location.naming_scheme, ManifestNamingScheme::V1);
    }

    #[tokio::test]
    async fn finalized_external_manifest_location_rejects_size_mismatch() {
        let object_store = ObjectStore::memory();
        let base_path = Path::from("repro");
        let version = 7;
        let final_path = ManifestNamingScheme::V2.manifest_path(&base_path, version);
        let body = b"manifest body";

        object_store
            .inner
            .put(&final_path, PutPayload::from_static(body))
            .await
            .expect("seed finalized manifest");
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(StaticExternalManifestStore {
                location: ManifestLocation {
                    version,
                    path: final_path,
                    size: Some(body.len() as u64 + 1),
                    naming_scheme: ManifestNamingScheme::V2,
                    e_tag: None,
                },
                verify_store: None,
            }),
        };

        let err = handler
            .resolve_latest_location(&base_path, &object_store)
            .await
            .expect_err("stale external manifest size should be rejected");
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("Manifest size mismatch"), "{err}");
    }

    #[tokio::test]
    async fn finalized_external_manifest_location_rejects_etag_mismatch() {
        let object_store = ObjectStore::memory();
        let base_path = Path::from("repro");
        let version = 7;
        let final_path = ManifestNamingScheme::V2.manifest_path(&base_path, version);
        let body = b"manifest body";

        object_store
            .inner
            .put(&final_path, PutPayload::from_static(body))
            .await
            .expect("seed finalized manifest");
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(StaticExternalManifestStore {
                location: ManifestLocation {
                    version,
                    path: final_path,
                    size: Some(body.len() as u64),
                    naming_scheme: ManifestNamingScheme::V2,
                    e_tag: Some("stale-etag".to_string()),
                },
                verify_store: None,
            }),
        };

        let err = handler
            .resolve_latest_location(&base_path, &object_store)
            .await
            .expect_err("stale external manifest e_tag should be rejected");
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("Manifest e_tag mismatch"), "{err}");
    }

    #[tokio::test]
    async fn external_manifest_store_put_records_destination_metadata() {
        let object_store: Arc<dyn OSObjectStore> = Arc::new(InMemory::new());
        let base_path = Path::from("repro");
        let staging_path = Path::from("repro/_versions/1.manifest.staging-abcd");
        object_store
            .put(&staging_path, PutPayload::from_static(b"manifest body"))
            .await
            .expect("seed staging manifest");
        let staging_meta = object_store
            .head(&staging_path)
            .await
            .expect("read staging metadata");

        let external_store = StaticExternalManifestStore {
            location: ManifestLocation {
                version: 1,
                path: staging_path.clone(),
                size: Some(staging_meta.size),
                naming_scheme: ManifestNamingScheme::V2,
                e_tag: staging_meta.e_tag.clone(),
            },
            verify_store: Some(object_store.clone()),
        };
        let location = external_store
            .put(
                &base_path,
                1,
                &staging_path,
                staging_meta.size,
                staging_meta.e_tag.clone(),
                object_store.as_ref(),
                ManifestNamingScheme::V2,
            )
            .await
            .expect("finalize manifest");
        let final_meta = object_store
            .head(&location.path)
            .await
            .expect("read finalized metadata");

        assert_ne!(
            staging_meta.e_tag, final_meta.e_tag,
            "test store must assign a new ETag to the copied object"
        );
        assert_eq!(location.size, Some(final_meta.size));
        assert_eq!(location.e_tag, final_meta.e_tag);
    }

    #[tokio::test]
    async fn external_manifest_handler_finalize_records_destination_metadata() {
        let object_store = ObjectStore::memory();
        let base_path = Path::from("repro");
        let version = 1;
        let staging_path = Path::from("repro/_versions/1.manifest.staging-abcd");
        object_store
            .inner
            .put(&staging_path, PutPayload::from_static(b"manifest body"))
            .await
            .expect("seed staging manifest");
        let staging_meta = object_store
            .inner
            .head(&staging_path)
            .await
            .expect("read staging metadata");

        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(StaticExternalManifestStore {
                location: ManifestLocation {
                    version,
                    path: staging_path,
                    size: Some(staging_meta.size),
                    naming_scheme: ManifestNamingScheme::V2,
                    e_tag: staging_meta.e_tag.clone(),
                },
                verify_store: Some(object_store.inner.clone()),
            }),
        };

        let location = handler
            .resolve_version_location(&base_path, version, object_store.inner.as_ref())
            .await
            .expect("finalize manifest");
        let final_meta = object_store
            .inner
            .head(&location.path)
            .await
            .expect("read finalized metadata");

        assert_ne!(
            staging_meta.e_tag, final_meta.e_tag,
            "test store must assign a new ETag to the copied object"
        );
        assert_eq!(location.size, Some(final_meta.size));
        assert_eq!(location.e_tag, final_meta.e_tag);
    }

    #[tokio::test]
    async fn test_dataset_can_onboard_external_store() {
        // First write a dataset WITHOUT external store
        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("x".to_owned())));
        let reader = data_gen.batch(100);
        let dir = TempStrDir::default();
        let ds_uri = &dir;
        Dataset::write(reader, ds_uri, None).await.unwrap();

        // Then try to load the dataset with external store handler set
        let sleepy_store = SleepyExternalManifestStore::new();
        let handler = Arc::new(ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(sleepy_store),
        });
        let options = read_params(handler.clone());
        DatasetBuilder::from_uri(ds_uri)
            .with_read_params(options)
            .load()
            .await
            .unwrap();

        Dataset::write(
            data_gen.batch(100),
            ds_uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                commit_handler: Some(handler),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn test_can_create_dataset_with_external_store() {
        let sleepy_store = SleepyExternalManifestStore::new();
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(sleepy_store),
        };
        let handler = Arc::new(handler);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("x".to_owned())));
        let reader = data_gen.batch(100);
        let dir = TempStrDir::default();
        let ds_uri = &dir;
        Dataset::write(reader, ds_uri, Some(write_params(handler.clone())))
            .await
            .unwrap();

        // load the data and check the content
        let ds = DatasetBuilder::from_uri(ds_uri)
            .with_read_params(read_params(handler))
            .load()
            .await
            .unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 100);
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_concurrent_commits_are_okay() {
        // Run test 20 times to have a higher chance of catching race conditions
        for _ in 0..20 {
            let sleepy_store = SleepyExternalManifestStore::new();
            let handler = ExternalManifestCommitHandler {
                external_manifest_store: Arc::new(sleepy_store),
            };
            let handler = Arc::new(handler);

            let mut data_gen =
                BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("x".to_owned())));
            let dir = TempStrDir::default();
            let ds_uri = &dir;

            Dataset::write(
                data_gen.batch(10),
                ds_uri,
                Some(write_params(handler.clone())),
            )
            .await
            .unwrap();

            // we have 5 retries by default, more than this will just fail
            let write_futs = (0..5)
                .map(|_| data_gen.batch(10))
                .map(|data| {
                    let mut params = write_params(handler.clone());
                    params.mode = WriteMode::Append;
                    Dataset::write(data, ds_uri, Some(params))
                })
                .collect::<Vec<_>>();

            let res = join_all(write_futs).await;

            let errors = res
                .into_iter()
                .filter(|r| r.is_err())
                .map(|r| r.unwrap_err())
                .collect::<Vec<_>>();

            assert!(errors.is_empty(), "{:?}", errors);

            // load the data and check the content
            let ds = DatasetBuilder::from_uri(ds_uri)
                .with_read_params(read_params(handler))
                .load()
                .await
                .unwrap();
            assert_eq!(ds.count_rows(None).await.unwrap(), 60);

            // No temporary manifests left over
            let manifest_path = format!("{}/{}", dir, "_versions/");
            let unexpected_entries = std::fs::read_dir(manifest_path)
                .unwrap()
                .filter(|entry| {
                    let entry = entry.as_ref().unwrap();
                    !entry
                        .file_name()
                        .as_os_str()
                        .to_string_lossy()
                        .ends_with(".manifest")
                })
                // There is a bug in local fs where concurrent commits can leave behind
                // temporary `x.manifest#n` files. This might be a bug in object-store.
                // TODO: fix this.
                .filter(|entry| {
                    let entry = entry.as_ref().unwrap();
                    !entry
                        .file_name()
                        .as_os_str()
                        .to_string_lossy()
                        .contains(".manifest#")
                })
                // The version hint file is expected to be present.
                .filter(|entry| {
                    let entry = entry.as_ref().unwrap();
                    !entry
                        .file_name()
                        .as_os_str()
                        .to_string_lossy()
                        .starts_with("latest_version_hint")
                })
                .collect::<Vec<_>>();
            assert!(unexpected_entries.is_empty(), "{:?}", unexpected_entries);
        }
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn test_out_of_sync_dataset_can_recover() {
        let sleepy_store = SleepyExternalManifestStore::new();
        let inner_store = sleepy_store.store.clone();
        let handler = ExternalManifestCommitHandler {
            external_manifest_store: Arc::new(sleepy_store),
        };
        let handler = Arc::new(handler);

        let mut data_gen =
            BatchGenerator::new().col(Box::new(IncrementingInt32::new().named("x".to_owned())));
        let dir = TempStrDir::default();
        let ds_uri = &dir;

        let params = WriteParams {
            commit_handler: Some(handler.clone()),
            enable_v2_manifest_paths: false,
            ..Default::default()
        };
        let mut ds = Dataset::write(data_gen.batch(10), ds_uri, Some(params))
            .await
            .unwrap();

        for _ in 0..5 {
            let data = data_gen.batch(10);
            let mut params = write_params(handler.clone());
            params.mode = WriteMode::Append;
            ds = Dataset::write(data, ds_uri, Some(params)).await.unwrap();
        }

        // manually simulate last version is out of sync
        let localfs: Box<dyn object_store::ObjectStore> = Box::new(LocalFileSystem::new());
        // Move version 6 to a temporary location, put that in the store.
        let base_path = Path::parse(ds_uri).unwrap();
        let version_six_staging_location = base_path
            .clone()
            .join(format!("6.manifest-{}", uuid::Uuid::new_v4()));
        localfs
            .rename(
                &ManifestNamingScheme::V1.manifest_path(&ds.base, 6),
                &version_six_staging_location,
            )
            .await
            .unwrap();
        {
            inner_store.lock().await.insert(
                (ds.base.to_string(), 6),
                version_six_staging_location.to_string(),
            );
        }
        // set the store back to dataset path with -{uuid} suffix
        let mut version_six = localfs
            .list(Some(&ds.base))
            .try_filter(|p| {
                let p = p.clone();
                async move { p.location.filename().unwrap().starts_with("6.manifest-") }
            })
            .take(1)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(version_six.len(), 1);
        let version_six_staging_location = version_six.pop().unwrap().unwrap().location;
        {
            inner_store.lock().await.insert(
                (ds.base.to_string(), 6),
                version_six_staging_location.to_string(),
            );
        }

        // Open without external store handler, should not see the out-of-sync commit
        let ds = DatasetBuilder::from_uri(ds_uri).load().await.unwrap();
        assert_eq!(ds.version().version, 5);
        assert_eq!(ds.count_rows(None).await.unwrap(), 50);

        // Open with external store handler, should sync the out-of-sync commit on open
        let ds = DatasetBuilder::from_uri(ds_uri)
            .with_commit_handler(handler.clone())
            .load()
            .await
            .unwrap();
        assert_eq!(ds.version().version, 6);
        assert_eq!(ds.count_rows(None).await.unwrap(), 60);

        {
            inner_store.lock().await.remove(&(ds.base.to_string(), 6));
        }
        assert!(
            handler
                .version_exists(
                    &ds.base,
                    6,
                    ds.object_store.inner.as_ref(),
                    ds.manifest_location().naming_scheme,
                )
                .await
                .unwrap()
        );
        assert!(
            !handler
                .version_exists(
                    &ds.base,
                    7,
                    ds.object_store.inner.as_ref(),
                    ds.manifest_location().naming_scheme,
                )
                .await
                .unwrap()
        );

        // Open without external store handler again, should see the newly sync'd commit
        let ds = DatasetBuilder::from_uri(ds_uri).load().await.unwrap();
        assert_eq!(ds.version().version, 6);
        assert_eq!(ds.count_rows(None).await.unwrap(), 60);

        // No temporary manifests left over
        let manifest_path = format!("{}/{}", dir, "_versions/");
        let unexpected_entries = std::fs::read_dir(manifest_path)
            .unwrap()
            .filter(|entry| {
                let entry = entry.as_ref().unwrap();
                !entry
                    .file_name()
                    .as_os_str()
                    .to_string_lossy()
                    .ends_with(".manifest")
            })
            // The version hint file is expected to be present.
            .filter(|entry| {
                let entry = entry.as_ref().unwrap();
                !entry
                    .file_name()
                    .as_os_str()
                    .to_string_lossy()
                    .starts_with("latest_version_hint")
            })
            .collect::<Vec<_>>();
        assert!(unexpected_entries.is_empty(), "{:?}", unexpected_entries);
    }

    /// S3's `CopyObject` API has a hard 5 GB cap on the source object size.
    /// Above that, callers must use multipart copy (`UploadPartCopy`) instead.
    /// `lance-table::io::commit::external_manifest` calls
    /// `object_store.copy(staging, final)` unconditionally on the manifest
    /// commit path — which fails for manifests >5 GB.
    ///
    /// This wrapper enforces that S3-side cap on top of any inner store, so
    /// the regression can be reproduced in-process without S3.
    ///
    /// It also lets the test override `head().size` for a chosen path, so the
    /// staging file can *appear* to be 14 GB without actually putting that
    /// many bytes into the inner store.
    const S3_COPY_OBJECT_CAP_BYTES: u64 = 5 * 1024 * 1024 * 1024;

    #[derive(Debug)]
    struct CopyCapStore {
        inner: Arc<dyn OSObjectStore>,
        /// path → fake size returned by head(); overrides the inner store.
        head_size_overrides: Arc<Mutex<HashMap<String, u64>>>,
        /// Counts calls to `copy_opts` (the fast path). Tests use this to
        /// assert which branch of `copy_size_aware` was taken — succeeding
        /// alone is not enough, since the slow path can also succeed for
        /// small files.
        copy_calls: AtomicUsize,
        /// Counts calls to `put_multipart_opts` (the slow read+rewrite path).
        put_multipart_calls: AtomicUsize,
    }

    impl CopyCapStore {
        fn new(inner: Arc<dyn OSObjectStore>) -> Self {
            Self {
                inner,
                head_size_overrides: Arc::new(Mutex::new(HashMap::new())),
                copy_calls: AtomicUsize::new(0),
                put_multipart_calls: AtomicUsize::new(0),
            }
        }

        async fn override_size(&self, path: &Path, size: u64) {
            self.head_size_overrides
                .lock()
                .await
                .insert(path.to_string(), size);
        }

        async fn effective_size(&self, location: &Path, real: u64) -> u64 {
            self.head_size_overrides
                .lock()
                .await
                .get(&location.to_string())
                .copied()
                .unwrap_or(real)
        }

        fn copy_calls(&self) -> usize {
            self.copy_calls.load(Ordering::SeqCst)
        }

        fn put_multipart_calls(&self) -> usize {
            self.put_multipart_calls.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for CopyCapStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CopyCapStore({})", self.inner)
        }
    }

    #[async_trait]
    impl OSObjectStore for CopyCapStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.put_multipart_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            // `head()` is a default method on `ObjectStore` that delegates to
            // `get_opts(location, GetOptions { head: true, .. })`. To make a
            // staging file *appear* to be 14 GB without holding 14 GB in
            // memory, we override the size in the returned ObjectMeta here.
            let mut res = self.inner.get_opts(location, options).await?;
            let overridden = self.effective_size(location, res.meta.size).await;
            res.meta.size = overridden;
            Ok(res)
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        // `head` and `delete` are default methods on `ObjectStore`, derived
        // from `get_opts`/`delete_stream`. We override `head` indirectly by
        // overriding `get_opts` below — it returns size based on the
        // overrides table for the chosen path.
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            // Mimic S3's CopyObject 5 GB hard cap: read the (possibly-overridden)
            // size of the source via head() and reject if it crosses the cap.
            let meta = self.head(from).await?;
            if meta.size >= S3_COPY_OBJECT_CAP_BYTES {
                return Err(object_store::Error::Generic {
                    store: "S3",
                    source: format!(
                        "EntityTooLarge: ProposedSize {} exceeds CopyObject 5GB cap",
                        meta.size
                    )
                    .into(),
                });
            }
            self.copy_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.copy_opts(from, to, opts).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OSResult<()> {
            self.inner.rename_opts(from, to, opts).await
        }
    }

    /// Repro for the manifest >5 GB bug.
    ///
    /// Drives `ExternalManifestStore::put` (the default impl) against a
    /// staging file whose `head().size` is reported as 14 GB. That `put`
    /// calls `object_store.copy(staging, final)` unconditionally — which
    /// our `CopyCapStore` wrapper rejects with the same `EntityTooLarge`
    /// error S3 returns in production.
    ///
    /// Today this test is RED: the copy step fails on >5 GB.
    /// After `copy_size_aware` lands, it should turn GREEN by falling back
    /// to a multipart-equivalent path (option 1: read+rewrite via
    /// `ObjectWriter`).
    #[tokio::test]
    async fn manifest_commit_succeeds_when_staging_exceeds_5gb_copy_cap() {
        let inner: Arc<dyn OSObjectStore> = Arc::new(InMemory::new());
        let capped = Arc::new(CopyCapStore::new(inner));

        // Write a small staging file, then lie about its size so the
        // CopyObject cap fires without holding 14 GB in memory.
        let base_path = Path::from("repro");
        let staging_path = Path::from("repro/_versions/1.manifest.staging-abcd");
        let body = b"fake manifest body";
        capped
            .put(&staging_path, PutPayload::from_static(body))
            .await
            .expect("seed staging file");
        capped
            .override_size(&staging_path, 14_961_429_442) // matches the production failure
            .await;

        // Spin up an ExternalManifestStore and drive `put` (the same code
        // path the failing CTAS hits via ExternalManifestCommitHandler).
        let external = SleepyExternalManifestStore::new();
        let head_meta = capped.head(&staging_path).await.unwrap();

        let location = external
            .put(
                &base_path,
                1,
                &staging_path,
                head_meta.size,
                head_meta.e_tag,
                capped.as_ref(),
                ManifestNamingScheme::V2,
            )
            .await
            .expect(
                "manifest commit should succeed for a >5 GB staging file via multipart-aware copy",
            );

        // Branch-taken assertions: the slow read+rewrite path was used.
        assert_eq!(
            capped.copy_calls(),
            0,
            "CopyObject must not be attempted for >5 GiB sources"
        );
        assert!(
            capped.put_multipart_calls() >= 1,
            "read+rewrite path must initiate a multipart upload"
        );

        // End-state assertions: final manifest exists with the original
        // bytes, and the staging file was deleted.
        let final_get = capped
            .inner
            .get(&location.path)
            .await
            .expect("final manifest must exist on the inner store")
            .bytes()
            .await
            .unwrap();
        assert_eq!(final_get.as_ref(), body);
        let staging_after = capped.inner.head(&staging_path).await;
        assert!(
            matches!(staging_after, Err(object_store::Error::NotFound { .. })),
            "staging file must be cleaned up after commit, got: {:?}",
            staging_after
        );
    }

    /// Counterpart to manifest_commit_succeeds_when_staging_exceeds_5gb_copy_cap.
    /// Confirms that for staging files BELOW the 5 GB cap, the fast-path
    /// server-side `copy()` is still used — i.e. we haven't accidentally
    /// regressed every commit to read+rewrite.
    #[tokio::test]
    async fn manifest_commit_uses_fast_copy_for_small_staging() {
        let inner: Arc<dyn OSObjectStore> = Arc::new(InMemory::new());
        let capped = Arc::new(CopyCapStore::new(inner));

        let base_path = Path::from("repro");
        let staging_path = Path::from("repro/_versions/1.manifest.staging-abcd");
        capped
            .put(
                &staging_path,
                PutPayload::from_static(b"small manifest body"),
            )
            .await
            .expect("seed staging file");
        // No size override — the staging file's real size is ~20 bytes,
        // well below the 5 GB cap, so copy_size_aware must take the fast
        // path.

        let external = SleepyExternalManifestStore::new();
        let head_meta = capped.head(&staging_path).await.unwrap();

        external
            .put(
                &base_path,
                1,
                &staging_path,
                head_meta.size,
                head_meta.e_tag,
                capped.as_ref(),
                ManifestNamingScheme::V2,
            )
            .await
            .expect("small manifest commit must succeed via fast-path copy");

        // The branch-taken assertion: fast path was used, slow path was not.
        assert!(
            capped.copy_calls() >= 1,
            "small-file commit must use server-side CopyObject"
        );
        assert_eq!(
            capped.put_multipart_calls(),
            0,
            "small-file commit must NOT initiate a multipart upload"
        );
    }
}
