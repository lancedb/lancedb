// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object store helpers and a store that mirrors writes to a secondary store

use std::{collections::HashMap, fmt::Formatter, sync::Arc};

#[cfg(feature = "aws")]
use std::sync::{LazyLock, Mutex, Weak};

use futures::{StreamExt, TryFutureExt, stream::BoxStream};
use lance::io::{ObjectStoreParams, WrappingObjectStore};
#[cfg(feature = "aws")]
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreProvider, ObjectStoreRegistry,
    throttle::{AimdThrottleConfig, AimdThrottledStore},
};
use lance_io::object_store::{StorageOptionsAccessor, StorageOptionsProvider};
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart, path::Path,
};
#[cfg(feature = "aws")]
use object_store::{
    StaticCredentialProvider,
    aws::{AmazonS3ConfigKey, AwsCredential},
};
#[cfg(feature = "aws")]
use object_store_opendal::OpendalStore;
#[cfg(feature = "aws")]
use opendal::{Operator, services::S3};
#[cfg(feature = "aws")]
use std::str::FromStr;

use async_trait::async_trait;

#[cfg(test)]
pub mod io_tracking;

#[cfg(feature = "aws")]
fn explicit_aws_credentials(
    storage_options: &HashMap<String, String>,
) -> Option<object_store::aws::AwsCredentialProvider> {
    explicit_aws_credential(storage_options)
        .ok()
        .flatten()
        .map(|credential| Arc::new(StaticCredentialProvider::new(credential)) as _)
}

#[cfg(feature = "aws")]
fn explicit_aws_credential(
    storage_options: &HashMap<String, String>,
) -> lance_core::Result<Option<AwsCredential>> {
    let aws_options = storage_options
        .iter()
        .filter_map(|(key, value)| {
            AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                .ok()
                .map(|key| (key, value))
        })
        .collect::<HashMap<_, _>>();

    let key_id = aws_options.get(&AmazonS3ConfigKey::AccessKeyId);
    let secret_key = aws_options.get(&AmazonS3ConfigKey::SecretAccessKey);
    let token = aws_options.get(&AmazonS3ConfigKey::Token);
    if key_id.is_none() && secret_key.is_none() && token.is_none() {
        return Ok(None);
    }
    let (Some(key_id), Some(secret_key)) = (key_id, secret_key) else {
        return Err(lance_core::Error::invalid_input(
            "Explicit AWS credentials require both aws_access_key_id and aws_secret_access_key",
        ));
    };

    Ok(Some(AwsCredential {
        key_id: (*key_id).clone(),
        secret_key: (*secret_key).clone(),
        token: token.map(|token| (*token).clone()),
    }))
}

#[cfg(feature = "aws")]
fn is_aws_credential_key(key: &AmazonS3ConfigKey) -> bool {
    matches!(
        key,
        AmazonS3ConfigKey::AccessKeyId
            | AmazonS3ConfigKey::SecretAccessKey
            | AmazonS3ConfigKey::Token
    )
}

/// Resolve OpenDAL options while keeping one explicit AWS credential family atomic.
///
/// Lance's native S3 provider accepts an explicit credential provider, but OpenDAL does not.
/// For that backend we therefore merge non-credential environment options here and disable
/// OpenDAL's second credential lookup. Wholly ambient credentials never enter this path.
#[cfg(feature = "aws")]
fn atomic_opendal_options(
    storage_options: &HashMap<String, String>,
    environment: impl IntoIterator<Item = (String, String)>,
) -> lance_core::Result<Option<HashMap<String, String>>> {
    let Some(credential) = explicit_aws_credential(storage_options)? else {
        return Ok(None);
    };

    let mut options = HashMap::new();
    for (key, value) in storage_options {
        match AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
            Ok(config_key) if is_aws_credential_key(&config_key) => {}
            Ok(config_key) => {
                options.insert(config_key.as_ref().to_string(), value.clone());
            }
            Err(_) => {
                options.insert(key.clone(), value.clone());
            }
        }
    }
    for (key, value) in environment {
        if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
            && !is_aws_credential_key(&config_key)
        {
            options
                .entry(config_key.as_ref().to_string())
                .or_insert(value);
        }
    }

    options.insert(
        AmazonS3ConfigKey::AccessKeyId.as_ref().to_string(),
        credential.key_id,
    );
    options.insert(
        AmazonS3ConfigKey::SecretAccessKey.as_ref().to_string(),
        credential.secret_key,
    );
    if let Some(token) = credential.token {
        options.insert(AmazonS3ConfigKey::Token.as_ref().to_string(), token);
    }
    options.insert("disable_config_load".to_string(), "true".to_string());
    Ok(Some(options))
}

#[cfg(feature = "aws")]
#[derive(Debug)]
struct AtomicAwsStoreProvider {
    inner: Arc<dyn ObjectStoreProvider>,
}

#[cfg(feature = "aws")]
#[async_trait]
impl ObjectStoreProvider for AtomicAwsStoreProvider {
    async fn new_store(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<LanceObjectStore> {
        // Caller-supplied credential providers and refreshable storage options are already
        // atomic credential authorities. Preserve Lance's precedence and refresh behavior.
        if params.aws_credentials.is_some()
            || params
                .storage_options_accessor
                .as_ref()
                .is_some_and(|accessor| accessor.has_provider())
        {
            return self.inner.new_store(base_path, params).await;
        }

        let storage_options = params.storage_options().cloned().unwrap_or_default();
        let Some(credential) = explicit_aws_credential(&storage_options)? else {
            return self.inner.new_store(base_path, params).await;
        };

        let use_opendal = storage_options
            .get("use_opendal")
            .is_some_and(|value| value == "true");

        // The native provider gives an explicit credential provider highest precedence, so its
        // later environment merge cannot splice in an unrelated session token. It also supplies
        // the correctly initialized Lance ObjectStore shell used below for OpenDAL.
        let mut native_options = storage_options.clone();
        native_options.insert("use_opendal".to_string(), "false".to_string());
        let mut native_params = params.clone();
        native_params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(native_options),
        ));
        native_params.aws_credentials = Some(Arc::new(StaticCredentialProvider::new(credential)));
        let mut store = self
            .inner
            .new_store(base_path.clone(), &native_params)
            .await?;

        if use_opendal {
            if storage_options
                .get("aws_provider_scheme")
                .is_some_and(|scheme| !scheme.is_empty())
            {
                return Err(lance_core::Error::not_supported(
                    "OpendalStore does not support an explicit aws_provider_scheme".to_string(),
                ));
            }
            let mut config = atomic_opendal_options(
                &storage_options,
                std::env::vars_os().filter_map(|(key, value)| {
                    Some((key.into_string().ok()?, value.into_string().ok()?))
                }),
            )?
            .expect("explicit credentials were already validated");
            let bucket = base_path.host_str().ok_or_else(|| {
                lance_core::Error::invalid_input("S3 URL must contain bucket name")
            })?;
            config.insert("bucket".to_string(), bucket.to_string());
            if !base_path.path().trim_start_matches('/').is_empty() {
                config.insert("root".to_string(), "/".to_string());
            }
            let operator = Operator::from_iter::<S3>(config).map_err(|error| {
                lance_core::Error::invalid_input(format!("Failed to create S3 operator: {error:?}"))
            })?;
            let opendal_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(operator));
            let throttle_config = AimdThrottleConfig::from_storage_options(Some(&storage_options))?;
            store.inner = if throttle_config.is_disabled() {
                opendal_store
            } else {
                Arc::new(AimdThrottledStore::new(opendal_store, throttle_config)?)
            };
        }

        Ok(store)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &url::Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.inner
            .calculate_object_store_prefix(url, storage_options)
    }
}

#[cfg(feature = "aws")]
static ATOMIC_AWS_REGISTRIES: LazyLock<Mutex<Vec<Weak<ObjectStoreRegistry>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Install the credential-safe S3 provider once on a session's shared object-store registry.
#[cfg(feature = "aws")]
pub(crate) fn install_atomic_aws_provider(session: &lance::session::Session) {
    let registry = session.store_registry();
    let mut installed = ATOMIC_AWS_REGISTRIES
        .lock()
        .expect("atomic AWS registry lock poisoned");
    installed.retain(|entry| entry.strong_count() > 0);
    if installed
        .iter()
        .filter_map(Weak::upgrade)
        .any(|entry| Arc::ptr_eq(&entry, &registry))
    {
        return;
    }

    for scheme in ["s3", "s3+ddb"] {
        if let Some(inner) = registry.get_provider(scheme) {
            registry.insert(scheme, Arc::new(AtomicAwsStoreProvider { inner }));
        }
    }
    installed.push(Arc::downgrade(&registry));
}

#[cfg(not(feature = "aws"))]
pub(crate) fn install_atomic_aws_provider(_session: &lance::session::Session) {}

/// Apply storage options to object store parameters.
///
/// Static credentials for Lance's native S3 backend are installed directly. OpenDAL options are
/// left for the session's credential-safe provider because that backend ignores this field.
/// Caller-supplied and refreshable credential providers retain their existing precedence.
pub(crate) fn set_storage_options(
    params: &mut ObjectStoreParams,
    storage_options: HashMap<String, String>,
    provider: Option<Arc<dyn StorageOptionsProvider>>,
) {
    #[cfg(feature = "aws")]
    if provider.is_none()
        && params.aws_credentials.is_none()
        && !storage_options
            .get("use_opendal")
            .is_some_and(|value| value == "true")
    {
        params.aws_credentials = explicit_aws_credentials(&storage_options);
    }

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
    use lance_io::object_store::providers::aws::build_aws_credential;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[derive(Debug)]
    struct RecordingProvider {
        saw_atomic_credentials: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ObjectStoreProvider for RecordingProvider {
        async fn new_store(
            &self,
            _base_path: url::Url,
            params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            self.saw_atomic_credentials
                .store(params.aws_credentials.is_some(), Ordering::SeqCst);
            Err(lance_core::Error::invalid_input("recorded test request"))
        }
    }

    #[derive(Debug)]
    struct RotatingOptionsProvider {
        fetches: Arc<AtomicUsize>,
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
            ])))
        }

        fn provider_id(&self) -> String {
            "rotating-test-provider".to_string()
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ObservedCredential {
        key_id: String,
        token: Option<String>,
    }

    #[derive(Debug)]
    struct ResolvingProvider {
        resolved_credential: Arc<Mutex<Option<ObservedCredential>>>,
    }

    #[async_trait]
    impl ObjectStoreProvider for ResolvingProvider {
        async fn new_store(
            &self,
            _base_path: url::Url,
            params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            let storage_options = params
                .storage_options()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(key, value)| {
                    AmazonS3ConfigKey::from_str(&key)
                        .ok()
                        .map(|key| (key, value))
                })
                .collect::<HashMap<_, _>>();
            let (provider, _) = build_aws_credential(
                Duration::from_secs(60),
                params.aws_credentials.clone(),
                Some(&storage_options),
                Some("us-east-1".to_string()),
                params.storage_options_accessor.clone(),
                None,
            )
            .await?;
            let credential = provider.get_credential().await?;
            *self.resolved_credential.lock().unwrap() = Some(ObservedCredential {
                key_id: credential.key_id.clone(),
                token: credential.token.clone(),
            });
            Err(lance_core::Error::invalid_input("recorded test request"))
        }
    }

    #[tokio::test]
    async fn explicit_aws_credentials_do_not_inherit_an_ambient_session_token() {
        let storage_options = HashMap::from([
            ("aws_access_key_id".to_string(), "explicit-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "explicit-secret".to_string(),
            ),
        ]);
        let params = object_store_params_from_storage_options(storage_options.clone());

        // Simulate Lance's environment merge, which adds AWS_SESSION_TOKEN when running in
        // Lambda. The explicit provider must remain an atomic two-part credential and take
        // precedence over the mixed storage options.
        let mut merged_options = storage_options
            .into_iter()
            .map(|(key, value)| (AmazonS3ConfigKey::from_str(&key).unwrap(), value))
            .collect::<HashMap<_, _>>();
        merged_options.insert(
            AmazonS3ConfigKey::Token,
            "lambda-execution-role-token".to_string(),
        );

        let (provider, _) = build_aws_credential(
            Duration::from_secs(60),
            params.aws_credentials,
            Some(&merged_options),
            Some("us-east-1".to_string()),
            None,
            None,
        )
        .await
        .unwrap();
        let credential = provider.get_credential().await.unwrap();

        assert_eq!(credential.key_id, "explicit-key");
        assert_eq!(credential.secret_key, "explicit-secret");
        assert_eq!(credential.token, None);
    }

    #[test]
    fn opendal_explicit_credentials_exclude_ambient_token() {
        let storage_options = HashMap::from([
            ("aws_access_key_id".to_string(), "explicit-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "explicit-secret".to_string(),
            ),
            ("use_opendal".to_string(), "true".to_string()),
        ]);
        let environment = [
            (
                "AWS_SESSION_TOKEN".to_string(),
                "lambda-execution-role-token".to_string(),
            ),
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
        ];
        let params = object_store_params_from_storage_options(storage_options.clone());

        let options = atomic_opendal_options(&storage_options, environment)
            .unwrap()
            .unwrap();

        assert!(params.aws_credentials.is_none());
        assert_eq!(options.get("aws_access_key_id").unwrap(), "explicit-key");
        assert_eq!(
            options.get("aws_secret_access_key").unwrap(),
            "explicit-secret"
        );
        assert!(!options.contains_key("aws_session_token"));
        assert_eq!(options.get("aws_region").unwrap(), "us-east-1");
        assert_eq!(options.get("disable_config_load").unwrap(), "true");
    }

    #[test]
    fn opendal_preserves_an_explicit_session_token() {
        let storage_options = HashMap::from([
            ("aws_access_key_id".to_string(), "explicit-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "explicit-secret".to_string(),
            ),
            (
                "aws_session_token".to_string(),
                "explicit-token".to_string(),
            ),
        ]);

        let options = atomic_opendal_options(
            &storage_options,
            [("AWS_SESSION_TOKEN".to_string(), "ambient-token".to_string())],
        )
        .unwrap()
        .unwrap();

        assert_eq!(options.get("aws_session_token").unwrap(), "explicit-token");
    }

    #[test]
    fn wholly_ambient_credentials_still_use_the_default_chain() {
        let options = atomic_opendal_options(
            &HashMap::new(),
            [
                ("AWS_ACCESS_KEY_ID".to_string(), "ambient-key".to_string()),
                (
                    "AWS_SECRET_ACCESS_KEY".to_string(),
                    "ambient-secret".to_string(),
                ),
                ("AWS_SESSION_TOKEN".to_string(), "ambient-token".to_string()),
            ],
        )
        .unwrap();

        assert!(options.is_none());
    }

    #[test]
    fn partial_explicit_credentials_are_rejected() {
        let error = explicit_aws_credential(&HashMap::from([(
            "aws_access_key_id".to_string(),
            "explicit-key".to_string(),
        )]))
        .unwrap_err();

        assert!(error.to_string().contains("require both"));
    }

    #[tokio::test]
    async fn public_namespace_connection_installs_the_atomic_provider() {
        let saw_atomic_credentials = Arc::new(AtomicBool::new(false));
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "s3",
            Arc::new(RecordingProvider {
                saw_atomic_credentials: saw_atomic_credentials.clone(),
            }),
        );
        let session = Arc::new(lance::session::Session::new(16, 16, registry.clone()));
        let root = tempfile::tempdir().unwrap();

        crate::connect_namespace(
            "dir",
            HashMap::from([(
                "root".to_string(),
                root.path().to_string_lossy().into_owned(),
            )]),
        )
        .storage_options([
            ("aws_access_key_id", "explicit-key"),
            ("aws_secret_access_key", "explicit-secret"),
        ])
        .session(session)
        .execute()
        .await
        .unwrap();

        // DirectoryNamespaceBuilder constructs fresh params with only a static accessor. The
        // OpenDAL selector is included to verify that both paths cross the installed boundary.
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("aws_access_key_id".to_string(), "explicit-key".to_string()),
                    (
                        "aws_secret_access_key".to_string(),
                        "explicit-secret".to_string(),
                    ),
                    ("use_opendal".to_string(), "true".to_string()),
                ]),
            ))),
            ..Default::default()
        };

        let error = registry
            .get_provider("s3")
            .unwrap()
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("recorded test request"));
        assert!(saw_atomic_credentials.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dynamic_storage_options_provider_remains_the_credential_authority() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let resolved_credential = Arc::new(Mutex::new(None));
        let provider = AtomicAwsStoreProvider {
            inner: Arc::new(ResolvingProvider {
                resolved_credential: resolved_credential.clone(),
            }),
        };
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
                    ]),
                    Arc::new(RotatingOptionsProvider {
                        fetches: fetches.clone(),
                    }),
                ),
            )),
            ..Default::default()
        };

        provider
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap_err();

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(
            *resolved_credential.lock().unwrap(),
            Some(ObservedCredential {
                key_id: "refreshed-key".to_string(),
                token: None,
            })
        );
    }

    #[tokio::test]
    async fn caller_supplied_aws_provider_remains_the_credential_authority() {
        let resolved_credential = Arc::new(Mutex::new(None));
        let provider = AtomicAwsStoreProvider {
            inner: Arc::new(ResolvingProvider {
                resolved_credential: resolved_credential.clone(),
            }),
        };
        let params = ObjectStoreParams {
            aws_credentials: Some(Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: "provider-key".to_string(),
                secret_key: "provider-secret".to_string(),
                token: None,
            }))),
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("aws_access_key_id".to_string(), "option-key".to_string()),
                    (
                        "aws_secret_access_key".to_string(),
                        "option-secret".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };

        provider
            .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
            .await
            .unwrap_err();

        assert_eq!(
            *resolved_credential.lock().unwrap(),
            Some(ObservedCredential {
                key_id: "provider-key".to_string(),
                token: None,
            })
        );
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
