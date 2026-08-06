// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object store helpers and a store that mirrors writes to a secondary store

use std::{collections::HashMap, fmt::Formatter, sync::Arc};

use futures::{StreamExt, TryFutureExt, stream::BoxStream};
use lance::io::{ObjectStoreParams, WrappingObjectStore};
use lance_io::object_store::{StorageOptionsAccessor, StorageOptionsProvider};
#[cfg(feature = "aws")]
use object_store::aws::AmazonS3ConfigKey;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart, path::Path,
};
#[cfg(feature = "aws")]
use std::str::FromStr;

use async_trait::async_trait;

#[cfg(test)]
pub mod io_tracking;

#[cfg(feature = "aws")]
fn is_aws_credential_key(key: &AmazonS3ConfigKey) -> bool {
    matches!(
        key,
        AmazonS3ConfigKey::AccessKeyId
            | AmazonS3ConfigKey::SecretAccessKey
            | AmazonS3ConfigKey::Token
    )
}

#[cfg(feature = "aws")]
pub(crate) fn is_aws_credential_option(key: &str) -> bool {
    AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
        .is_ok_and(|key| is_aws_credential_key(&key))
}

#[cfg(not(feature = "aws"))]
pub(crate) fn is_aws_credential_option(_key: &str) -> bool {
    false
}

/// Select a supplied session or create the default Lance session.
pub(crate) fn atomic_aws_session(
    session: Option<Arc<lance::session::Session>>,
) -> Arc<lance::session::Session> {
    session.unwrap_or_else(|| Arc::new(lance::session::Session::default()))
}

/// Apply storage options to object store parameters.
pub(crate) fn set_storage_options(
    params: &mut ObjectStoreParams,
    storage_options: HashMap<String, String>,
    provider: Option<Arc<dyn StorageOptionsProvider>>,
) {
    params.storage_options_accessor = match (storage_options.is_empty(), provider) {
        (true, None) => None,
        (true, Some(provider)) => Some(Arc::new(StorageOptionsAccessor::with_provider(provider))),
        (false, None) => Some(Arc::new(StorageOptionsAccessor::with_static_options(
            storage_options,
        ))),
        (false, Some(provider)) => Some(Arc::new(
            StorageOptionsAccessor::with_initial_and_provider(storage_options, provider),
        )),
    };
}

pub(crate) fn object_store_params_from_storage_options(
    storage_options: HashMap<String, String>,
) -> ObjectStoreParams {
    let mut params = ObjectStoreParams::default();
    set_storage_options(&mut params, storage_options, None);
    params
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

#[cfg(all(test, feature = "aws"))]
mod credential_tests {
    use super::*;
    use lance_io::object_store::{
        ObjectStore as LanceObjectStore, ObjectStoreProvider, ObjectStoreRegistry, StorageOptions,
        providers::aws::{AwsStoreProvider, merge_atomic_aws_environment},
    };
    use object_store::{
        StaticCredentialProvider,
        aws::{AwsCredential, AwsCredentialProvider},
        memory::InMemory,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct RotatingOptionsProvider {
        fetches: Arc<AtomicUsize>,
        custom_ordered: bool,
    }

    #[async_trait]
    impl StorageOptionsProvider for RotatingOptionsProvider {
        async fn fetch_storage_options(
            &self,
        ) -> lance_core::Result<Option<HashMap<String, String>>> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            Ok(Some(HashMap::from([
                ("aws_access_key_id".to_string(), "refreshed-key".to_string()),
                (
                    "aws_secret_access_key".to_string(),
                    "refreshed-secret".to_string(),
                ),
                (
                    "custom_ordered".to_string(),
                    self.custom_ordered.to_string(),
                ),
            ])))
        }

        fn provider_id(&self) -> String {
            format!("rotating-test-provider-{}", self.custom_ordered)
        }
    }

    #[derive(Debug)]
    struct NonAwsOptionsProvider;

    #[async_trait]
    impl StorageOptionsProvider for NonAwsOptionsProvider {
        async fn fetch_storage_options(
            &self,
        ) -> lance_core::Result<Option<HashMap<String, String>>> {
            Ok(Some(HashMap::from([(
                "aws_region".to_string(),
                "us-east-1".to_string(),
            )])))
        }

        fn provider_id(&self) -> String {
            "non-aws-credential-test-provider".to_string()
        }
    }

    struct CustomStoreProvider {
        expected_accessor: Arc<StorageOptionsAccessor>,
        expected_credentials: AwsCredentialProvider,
        marker: Arc<dyn ObjectStore>,
        constructions: Arc<AtomicUsize>,
        saw_original_inputs: Arc<AtomicBool>,
    }

    impl std::fmt::Debug for CustomStoreProvider {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            // Diagnostic output must never grant built-in AWS provider capabilities.
            formatter.write_str("AwsStoreProvider")
        }
    }

    #[async_trait]
    impl ObjectStoreProvider for CustomStoreProvider {
        async fn new_store(
            &self,
            base_path: url::Url,
            params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            let accessor = params.storage_options_accessor.as_ref().ok_or_else(|| {
                lance_core::Error::invalid_input("custom provider lost dynamic accessor")
            })?;
            if !accessor.has_provider() || !Arc::ptr_eq(accessor, &self.expected_accessor) {
                return Err(lance_core::Error::invalid_input(
                    "custom provider lost dynamic accessor",
                ));
            }
            let credentials = params.aws_credentials.as_ref().ok_or_else(|| {
                lance_core::Error::invalid_input("custom provider lost AWS credential provider")
            })?;
            if !Arc::ptr_eq(credentials, &self.expected_credentials) {
                return Err(lance_core::Error::invalid_input(
                    "custom provider lost AWS credential provider",
                ));
            }

            self.saw_original_inputs.store(true, Ordering::SeqCst);
            self.constructions.fetch_add(1, Ordering::SeqCst);
            let current_options = accessor.get_storage_options().await?.0;
            let mut store = AwsStoreProvider.new_store(base_path, params).await?;
            store.inner = self.marker.clone();
            store.list_is_lexically_ordered = current_options
                .get("custom_ordered")
                .is_none_or(|value| value == "true");
            Ok(store)
        }
    }

    fn dynamic_opendal_params(
        fetches: Arc<AtomicUsize>,
        custom_ordered: bool,
    ) -> (
        ObjectStoreParams,
        Arc<StorageOptionsAccessor>,
        AwsCredentialProvider,
    ) {
        let accessor = Arc::new(StorageOptionsAccessor::with_initial_and_provider(
            HashMap::from([
                ("aws_access_key_id".to_string(), "expired-key".to_string()),
                (
                    "aws_secret_access_key".to_string(),
                    "expired-secret".to_string(),
                ),
                ("expires_at_millis".to_string(), "0".to_string()),
                ("use_opendal".to_string(), "true".to_string()),
                ("aws_region".to_string(), "us-east-1".to_string()),
                ("custom_ordered".to_string(), "true".to_string()),
            ]),
            Arc::new(RotatingOptionsProvider {
                fetches,
                custom_ordered,
            }),
        ));
        let credentials: AwsCredentialProvider =
            Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: "provider-key".to_string(),
                secret_key: "provider-secret".to_string(),
                token: None,
            }));
        (
            ObjectStoreParams {
                aws_credentials: Some(credentials.clone()),
                storage_options_accessor: Some(accessor.clone()),
                ..Default::default()
            },
            accessor,
            credentials,
        )
    }

    #[tokio::test]
    async fn opendal_refreshes_the_actual_dynamic_credential_family() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                StorageOptionsAccessor::with_initial_and_provider(
                    HashMap::from([
                        ("aws_access_key_id".to_string(), "expired-key".to_string()),
                        (
                            "aws_secret_access_key".to_string(),
                            "expired-secret".to_string(),
                        ),
                        ("expires_at_millis".to_string(), "0".to_string()),
                        ("use_opendal".to_string(), "true".to_string()),
                        ("aws_region".to_string(), "us-east-1".to_string()),
                    ]),
                    Arc::new(RotatingOptionsProvider {
                        fetches: fetches.clone(),
                        custom_ordered: true,
                    }),
                ),
            )),
            ..Default::default()
        };

        let session = atomic_aws_session(Some(Arc::new(lance::session::Session::default())));
        session
            .store_registry()
            .get_provider("s3")
            .unwrap()
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap();

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn opendal_preserves_custom_provider_dynamic_accessor() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (params, accessor, credentials) = dynamic_opendal_params(fetches.clone(), false);
        let marker: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let saw_original_inputs = Arc::new(AtomicBool::new(false));
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "s3",
            Arc::new(CustomStoreProvider {
                expected_accessor: accessor,
                expected_credentials: credentials,
                marker: marker.clone(),
                constructions: Arc::new(AtomicUsize::new(0)),
                saw_original_inputs: saw_original_inputs.clone(),
            }),
        );
        let session = atomic_aws_session(Some(Arc::new(lance::session::Session::new(
            16,
            16,
            registry.clone(),
        ))));

        let store = session
            .store_registry()
            .get_provider("s3")
            .unwrap()
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap();

        assert!(saw_original_inputs.load(Ordering::SeqCst));
        assert!(Arc::ptr_eq(&store.inner, &marker));
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn opendal_refresh_preserves_provider_metadata() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (params, accessor, credentials) = dynamic_opendal_params(fetches.clone(), false);
        let marker: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let constructions = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "s3",
            Arc::new(CustomStoreProvider {
                expected_accessor: accessor,
                expected_credentials: credentials,
                marker,
                constructions: constructions.clone(),
                saw_original_inputs: Arc::new(AtomicBool::new(false)),
            }),
        );

        let store = registry
            .get_provider("s3")
            .unwrap()
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap();
        let _ = store.inner.list(None).next().await;

        assert!(!store.list_is_lexically_ordered);
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn non_aws_dynamic_options_cannot_complete_a_partial_static_family() {
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                StorageOptionsAccessor::with_initial_and_provider(
                    HashMap::from([
                        ("aws_access_key_id".to_string(), "explicit-key".to_string()),
                        ("expires_at_millis".to_string(), "0".to_string()),
                        ("use_opendal".to_string(), "true".to_string()),
                    ]),
                    Arc::new(NonAwsOptionsProvider),
                ),
            )),
            ..Default::default()
        };

        let error = AwsStoreProvider
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("require both"));
    }

    fn local_s3_options() -> HashMap<String, String> {
        HashMap::from([
            ("aws_access_key_id".to_string(), "explicit-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "explicit-secret".to_string(),
            ),
            ("aws_region".to_string(), "us-east-1".to_string()),
            ("aws_endpoint".to_string(), "http://127.0.0.1:9".to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ])
    }

    #[test]
    fn explicit_aws_credentials_do_not_inherit_an_ambient_session_token() {
        let mut options = StorageOptions::new(HashMap::from([
            ("aws_access_key_id".to_string(), "explicit-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "explicit-secret".to_string(),
            ),
        ]));

        merge_atomic_aws_environment(
            &mut options,
            [
                ("AWS_SESSION_TOKEN".to_string(), "ambient-token".to_string()),
                ("AWS_REGION".to_string(), "us-east-1".to_string()),
            ],
        );

        assert_eq!(options.0.get("aws_access_key_id").unwrap(), "explicit-key");
        assert_eq!(
            options.0.get("aws_secret_access_key").unwrap(),
            "explicit-secret"
        );
        assert!(!options.0.contains_key("aws_session_token"));
        assert_eq!(options.0.get("aws_region").unwrap(), "us-east-1");
    }

    #[tokio::test]
    async fn identical_explicit_options_reuse_the_session_store() {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let url = url::Url::parse("s3://bucket/table").unwrap();
        let first_params = object_store_params_from_storage_options(local_s3_options());
        let second_params = object_store_params_from_storage_options(local_s3_options());

        let first = registry
            .get_store(url.clone(), &first_params)
            .await
            .unwrap();
        let second = registry.get_store(url, &second_params).await.unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "logically identical explicit credentials should hit the session cache"
        );
    }

    #[test]
    fn aws_credential_options_are_one_merge_family() {
        assert!(is_aws_credential_option("aws_access_key_id"));
        assert!(is_aws_credential_option("AWS_SECRET_ACCESS_KEY"));
        assert!(is_aws_credential_option("aws_session_token"));
        assert!(!is_aws_credential_option("aws_region"));
    }

    #[test]
    fn dynamic_params_keep_the_original_opaque_authorities() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let (params, accessor, credentials) = dynamic_opendal_params(fetches, true);

        assert!(Arc::ptr_eq(
            params.storage_options_accessor.as_ref().unwrap(),
            &accessor
        ));
        assert!(Arc::ptr_eq(
            params.aws_credentials.as_ref().unwrap(),
            &credentials
        ));
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
