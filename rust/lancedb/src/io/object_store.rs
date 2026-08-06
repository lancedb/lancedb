// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Object store helpers and a store that mirrors writes to a secondary store

use std::{collections::HashMap, fmt::Formatter, sync::Arc};

#[cfg(feature = "aws")]
use std::{
    ops::Range,
    sync::{LazyLock, Mutex, Weak},
};

#[cfg(feature = "aws")]
use bytes::Bytes;
use futures::{StreamExt, TryFutureExt, stream::BoxStream};
#[cfg(feature = "aws")]
use futures::{TryStreamExt, stream};
use lance::io::{ObjectStoreParams, WrappingObjectStore};
#[cfg(feature = "aws")]
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreProvider, ObjectStoreRegistry, StorageOptions,
    providers::aws::build_aws_credential,
};
use lance_io::object_store::{StorageOptionsAccessor, StorageOptionsProvider};
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart, path::Path,
};
#[cfg(feature = "aws")]
use object_store::{
    CredentialProvider, RenameOptions, StaticCredentialProvider,
    aws::{AmazonS3ConfigKey, AwsCredential},
};
#[cfg(feature = "aws")]
use std::str::FromStr;
#[cfg(feature = "aws")]
use tokio::sync::RwLock as TokioRwLock;

use async_trait::async_trait;

#[cfg(test)]
pub mod io_tracking;

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
fn has_aws_credential_member(storage_options: &HashMap<String, String>) -> bool {
    storage_options.keys().any(|key| {
        AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
            .is_ok_and(|key| is_aws_credential_key(&key))
    })
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

#[cfg(feature = "aws")]
pub(crate) fn is_aws_credential_option(key: &str) -> bool {
    AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
        .is_ok_and(|key| is_aws_credential_key(&key))
}

#[cfg(not(feature = "aws"))]
pub(crate) fn is_aws_credential_option(_key: &str) -> bool {
    false
}

#[cfg(feature = "aws")]
fn canonical_noncredential_options(
    storage_options: &HashMap<String, String>,
) -> HashMap<String, String> {
    storage_options
        .iter()
        .filter_map(
            |(key, value)| match AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                Ok(config_key) if is_aws_credential_key(&config_key) => None,
                Ok(config_key) => Some((config_key.as_ref().to_string(), value.clone())),
                Err(_) => Some((key.clone(), value.clone())),
            },
        )
        .collect()
}

#[cfg(feature = "aws")]
fn insert_aws_credential(options: &mut HashMap<String, String>, credential: AwsCredential) {
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
    } else {
        // Lance's environment merge treats an empty value as an explicit sentinel, while
        // OpenDAL ignores an empty session token. This blocks a foreign ambient token without
        // changing the semantics of long-lived key/secret credentials.
        options.insert(AmazonS3ConfigKey::Token.as_ref().to_string(), String::new());
    }
}

/// Merge an OpenDAL configuration without ever combining two AWS credential families.
#[cfg(feature = "aws")]
fn atomic_opendal_options(
    base_options: &HashMap<String, String>,
    dynamic_options: &HashMap<String, String>,
    credential: Option<AwsCredential>,
    environment: impl IntoIterator<Item = (String, String)>,
) -> lance_core::Result<HashMap<String, String>> {
    let mut options = canonical_noncredential_options(base_options);
    for (key, value) in environment {
        match AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
            Ok(config_key) if is_aws_credential_key(&config_key) => {}
            Ok(config_key) => {
                options
                    .entry(config_key.as_ref().to_string())
                    .or_insert(value);
            }
            Err(_) => {}
        }
    }
    options.extend(canonical_noncredential_options(dynamic_options));

    let credential = match credential {
        Some(credential) => Some(credential),
        None if has_aws_credential_member(dynamic_options) => {
            explicit_aws_credential(dynamic_options)?
        }
        None => explicit_aws_credential(base_options)?,
    };
    if let Some(credential) = credential {
        insert_aws_credential(&mut options, credential);
        // OpenDAL must not run another credential lookup after an explicit family wins.
        options.insert("disable_config_load".to_string(), "true".to_string());
    }
    Ok(options)
}

#[cfg(feature = "aws")]
#[derive(Debug)]
struct AtomicAccessorAwsCredentialProvider {
    accessor: Arc<StorageOptionsAccessor>,
    fallback: Option<object_store::aws::AwsCredentialProvider>,
}

#[cfg(feature = "aws")]
#[async_trait]
impl CredentialProvider for AtomicAccessorAwsCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let options = self
            .accessor
            .get_storage_options()
            .await
            .map_err(|error| Error::Generic {
                store: "AtomicAwsCredentialProvider",
                source: Box::new(error),
            })?
            .0;
        match explicit_aws_credential(&options).map_err(|error| Error::Generic {
            store: "AtomicAwsCredentialProvider",
            source: Box::new(error),
        })? {
            Some(credential) => Ok(Arc::new(credential)),
            None => match &self.fallback {
                Some(fallback) => fallback.get_credential().await,
                None => Err(Error::Generic {
                    store: "AtomicAwsCredentialProvider",
                    source: "Explicit AWS credentials require both aws_access_key_id and aws_secret_access_key".into(),
                }),
            },
        }
    }
}

#[cfg(feature = "aws")]
#[derive(Debug, Clone)]
struct CachedProviderStore {
    config: HashMap<String, String>,
    store: Arc<dyn ObjectStore>,
}

/// Store that refreshes credentials by rebuilding through the registered provider.
///
/// Re-entering the original provider preserves custom encryption, authorization, wrapping, and
/// backend behavior while still letting built-in OpenDAL stores consume refreshed credentials.
#[cfg(feature = "aws")]
#[derive(Clone)]
struct AtomicProviderStore {
    provider: Arc<dyn ObjectStoreProvider>,
    base_path: url::Url,
    base_params: ObjectStoreParams,
    base_options: Arc<HashMap<String, String>>,
    accessor: Option<Arc<StorageOptionsAccessor>>,
    aws_credentials: Option<object_store::aws::AwsCredentialProvider>,
    cache: Arc<TokioRwLock<Option<CachedProviderStore>>>,
}

#[cfg(feature = "aws")]
impl std::fmt::Debug for AtomicProviderStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AtomicProviderStore")
            .field("base_path", &self.base_path)
            .field("accessor", &self.accessor)
            .finish()
    }
}

#[cfg(feature = "aws")]
impl std::fmt::Display for AtomicProviderStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "AtomicProviderStore({})", self.base_path)
    }
}

#[cfg(feature = "aws")]
impl AtomicProviderStore {
    async fn current_config(&self) -> lance_core::Result<HashMap<String, String>> {
        let dynamic_options = match &self.accessor {
            Some(accessor) if accessor.has_provider() => accessor.get_storage_options().await?.0,
            _ => HashMap::new(),
        };
        let credential = match &self.aws_credentials {
            Some(provider) => Some({
                let credential = provider
                    .get_credential()
                    .await
                    .map_err(|error| lance_core::Error::io_source(Box::new(error)))?;
                AwsCredential {
                    key_id: credential.key_id.clone(),
                    secret_key: credential.secret_key.clone(),
                    token: credential.token.clone(),
                }
            }),
            None => None,
        };
        atomic_opendal_options(
            &self.base_options,
            &dynamic_options,
            credential,
            std::env::vars_os().filter_map(|(key, value)| {
                Some((key.into_string().ok()?, value.into_string().ok()?))
            }),
        )
    }

    async fn build_store(
        &self,
        config: &HashMap<String, String>,
    ) -> lance_core::Result<LanceObjectStore> {
        let mut params = self.base_params.clone();
        params.aws_credentials = None;
        set_storage_options(&mut params, config.clone(), None);
        self.provider
            .new_store(self.base_path.clone(), &params)
            .await
    }

    async fn initialize_store(&self) -> lance_core::Result<LanceObjectStore> {
        let config = self.current_config().await?;
        let store = self.build_store(&config).await?;
        *self.cache.write().await = Some(CachedProviderStore {
            config,
            store: store.inner.clone(),
        });
        Ok(store)
    }

    async fn current_store(&self) -> lance_core::Result<Arc<dyn ObjectStore>> {
        let config = self.current_config().await?;

        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref()
                && cached.config == config
            {
                return Ok(cached.store.clone());
            }
        }

        let store = self.build_store(&config).await?.inner;
        let mut cache = self.cache.write().await;
        if let Some(cached) = cache.as_ref()
            && cached.config == config
        {
            return Ok(cached.store.clone());
        }
        *cache = Some(CachedProviderStore {
            config,
            store: store.clone(),
        });
        Ok(store)
    }

    fn map_store_error(error: lance_core::Error) -> Error {
        Error::Generic {
            store: "AtomicProviderStore",
            source: Box::new(error),
        }
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl ObjectStore for AtomicProviderStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .put_opts(location, payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .put_multipart_opts(location, options)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .get_opts(location, options)
            .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .get_ranges(location, ranges)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let this = self.clone();
        stream::once(async move {
            let store = this.current_store().await.map_err(Self::map_store_error)?;
            Ok::<_, Error>((store, locations))
        })
        .map_ok(|(store, locations)| store.delete_stream(locations))
        .try_flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let this = self.clone();
        stream::once(async move { this.current_store().await.map_err(Self::map_store_error) })
            .map_ok(move |store| store.list(prefix.as_ref()))
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .copy_opts(from, to, options)
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.current_store()
            .await
            .map_err(Self::map_store_error)?
            .rename_opts(from, to, options)
            .await
    }
}

#[cfg(feature = "aws")]
#[derive(Debug)]
struct AtomicAwsStoreProvider {
    inner: Arc<dyn ObjectStoreProvider>,
}

#[cfg(feature = "aws")]
impl AtomicAwsStoreProvider {
    const CACHE_GENERATION: &'static str = "lancedb-atomic-aws-v1";

    fn generated_prefix(
        &self,
        url: &url::Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.inner
            .calculate_object_store_prefix(url, storage_options)
            .map(|prefix| format!("{prefix}${}", Self::CACHE_GENERATION))
    }

    async fn new_store_inner(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<LanceObjectStore> {
        let storage_options = params.storage_options().cloned().unwrap_or_default();
        let use_opendal = storage_options
            .get("use_opendal")
            .is_some_and(|value| value == "true");

        if use_opendal {
            let has_dynamic_options = params
                .storage_options_accessor
                .as_ref()
                .is_some_and(|accessor| accessor.has_provider());
            if params.aws_credentials.is_none()
                && !has_dynamic_options
                && explicit_aws_credential(&storage_options)?.is_none()
            {
                return self.inner.new_store(base_path, params).await;
            }

            let dynamic_store = AtomicProviderStore {
                provider: self.inner.clone(),
                base_path,
                base_params: params.clone(),
                base_options: Arc::new(storage_options.clone()),
                accessor: params.storage_options_accessor.clone(),
                aws_credentials: params.aws_credentials.clone(),
                cache: Arc::new(TokioRwLock::new(None)),
            };
            let mut store = dynamic_store.initialize_store().await?;

            // Static explicit credentials need no runtime wrapper, so an unknown provider's
            // returned store remains pointer-identical. Dynamic authorities rebuild through that
            // same provider whenever their normalized credential configuration changes.
            if has_dynamic_options || params.aws_credentials.is_some() {
                store.inner = Arc::new(dynamic_store);
            }
            return Ok(store);
        }

        if params.aws_credentials.is_some() {
            return self.inner.new_store(base_path, params).await;
        }

        let Some(accessor) = params.storage_options_accessor.as_ref() else {
            return self.inner.new_store(base_path, params).await;
        };
        let credential_provider: object_store::aws::AwsCredentialProvider =
            if accessor.has_provider() {
                // Validate the currently vended family first. A complete dynamic family replaces
                // the whole static family, while a provider returning no AWS options must never
                // make a partial static family fall through to Lance's environment-merged map.
                let current_options = accessor.get_storage_options().await?.0;
                let current_credential = explicit_aws_credential(&current_options)?;
                let static_credential = explicit_aws_credential(&storage_options);
                if current_credential.is_none() {
                    static_credential
                        .as_ref()
                        .map_err(|error| lance_core::Error::invalid_input(error.to_string()))?;
                }

                let s3_options = storage_options
                    .iter()
                    .filter_map(|(key, value)| {
                        AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                            .ok()
                            .map(|key| (key, value.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                let provider_scheme =
                    StorageOptions::new(storage_options.clone()).aws_provider_scheme()?;
                let region = s3_options.get(&AmazonS3ConfigKey::Region).cloned();
                let fallback = if static_credential.is_ok() {
                    Some(
                        build_aws_credential(
                            params.s3_credentials_refresh_offset,
                            None,
                            Some(&s3_options),
                            region,
                            None,
                            provider_scheme,
                        )
                        .await?
                        .0,
                    )
                } else {
                    None
                };
                Arc::new(AtomicAccessorAwsCredentialProvider {
                    accessor: accessor.clone(),
                    fallback,
                })
            } else if let Some(credential) = explicit_aws_credential(&storage_options)? {
                Arc::new(StaticCredentialProvider::new(credential))
            } else {
                return self.inner.new_store(base_path, params).await;
            };

        // This allocation occurs only after the registry cache miss. Cache identity therefore
        // remains the semantic identity of the original storage-options accessor.
        let mut atomic_params = params.clone();
        atomic_params.aws_credentials = Some(credential_provider);
        self.inner.new_store(base_path, &atomic_params).await
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl ObjectStoreProvider for AtomicAwsStoreProvider {
    async fn new_store(
        &self,
        base_path: url::Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<LanceObjectStore> {
        let store_prefix = self.generated_prefix(&base_path, params.storage_options())?;
        let mut store = self.new_store_inner(base_path, params).await?;
        // The registry cache and the returned store must use the same identity. Lance compares
        // these values when resolving external blob bases.
        store.store_prefix = store_prefix;
        Ok(store)
    }

    fn extract_path(&self, url: &url::Url) -> lance_core::Result<Path> {
        self.inner.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &url::Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.generated_prefix(url, storage_options)
    }
}

#[cfg(feature = "aws")]
static ATOMIC_AWS_REGISTRIES: LazyLock<Mutex<Vec<Weak<ObjectStoreRegistry>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Install the credential-safe S3 provider once on a session's shared object-store registry.
#[cfg(feature = "aws")]
fn install_atomic_aws_provider_inner(session: &lance::session::Session) {
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

#[cfg(feature = "aws")]
pub(crate) fn install_atomic_aws_provider(session: &lance::session::Session) {
    install_atomic_aws_provider_inner(session);
}

#[cfg(not(feature = "aws"))]
pub(crate) fn install_atomic_aws_provider(_session: &lance::session::Session) {}

/// Select or create a session and protect its registered AWS providers.
pub(crate) fn atomic_aws_session(
    session: Option<Arc<lance::session::Session>>,
) -> Arc<lance::session::Session> {
    match session {
        Some(session) => {
            install_atomic_aws_provider(&session);
            session
        }
        None => {
            let session = Arc::new(lance::session::Session::default());
            #[cfg(feature = "aws")]
            install_atomic_aws_provider_inner(&session);
            session
        }
    }
}

/// Apply storage options to object store parameters.
///
/// Credential providers are deliberately installed by [`AtomicAwsStoreProvider`] only after a
/// registry cache miss, preserving semantic cache reuse for identical option maps.
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
    use lance_io::object_store::providers::aws::{AwsStoreProvider, build_aws_credential};
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

    #[derive(Debug)]
    struct CustomPathProvider;

    #[async_trait]
    impl ObjectStoreProvider for CustomPathProvider {
        async fn new_store(
            &self,
            _base_path: url::Url,
            _params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            Err(lance_core::Error::invalid_input("unused test provider"))
        }

        fn extract_path(&self, _url: &url::Url) -> lance_core::Result<Path> {
            Ok(Path::from("custom/tenant/path"))
        }
    }

    struct CustomStoreProvider {
        marker: Arc<dyn ObjectStore>,
    }

    impl std::fmt::Debug for CustomStoreProvider {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            // Diagnostic output must never grant authority to replace a custom provider's store.
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
            let mut store = AwsStoreProvider.new_store(base_path, params).await?;
            store.inner = self.marker.clone();
            Ok(store)
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
        let resolved_credential = Arc::new(Mutex::new(None));
        let provider = AtomicAwsStoreProvider {
            inner: Arc::new(ResolvingProvider {
                resolved_credential: resolved_credential.clone(),
            }),
        };

        provider
            .new_store(
                url::Url::parse("s3://bucket/table").unwrap(),
                &object_store_params_from_storage_options(storage_options),
            )
            .await
            .unwrap_err();

        assert_eq!(
            *resolved_credential.lock().unwrap(),
            Some(ObservedCredential {
                key_id: "explicit-key".to_string(),
                token: None,
            })
        );
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

        let options =
            atomic_opendal_options(&storage_options, &HashMap::new(), None, environment).unwrap();

        assert!(params.aws_credentials.is_none());
        assert_eq!(options.get("aws_access_key_id").unwrap(), "explicit-key");
        assert_eq!(
            options.get("aws_secret_access_key").unwrap(),
            "explicit-secret"
        );
        assert_eq!(options.get("aws_session_token").unwrap(), "");
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
            &HashMap::new(),
            None,
            [("AWS_SESSION_TOKEN".to_string(), "ambient-token".to_string())],
        )
        .unwrap();

        assert_eq!(options.get("aws_session_token").unwrap(), "explicit-token");
    }

    #[test]
    fn opendal_dynamic_credential_family_replaces_the_entire_static_family() {
        let base_options = HashMap::from([
            ("aws_access_key_id".to_string(), "base-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "base-secret".to_string(),
            ),
            ("aws_session_token".to_string(), "base-token".to_string()),
        ]);
        let dynamic_options = HashMap::from([
            ("aws_access_key_id".to_string(), "dynamic-key".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "dynamic-secret".to_string(),
            ),
        ]);

        let options =
            atomic_opendal_options(&base_options, &dynamic_options, None, std::iter::empty())
                .unwrap();

        assert_eq!(options.get("aws_access_key_id").unwrap(), "dynamic-key");
        assert_eq!(
            options.get("aws_secret_access_key").unwrap(),
            "dynamic-secret"
        );
        assert_eq!(options.get("aws_session_token").unwrap(), "");
    }

    #[test]
    fn wholly_ambient_credentials_still_use_the_default_chain() {
        let options = atomic_opendal_options(
            &HashMap::new(),
            &HashMap::new(),
            None,
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

        assert!(!options.contains_key("aws_access_key_id"));
        assert!(!options.contains_key("aws_secret_access_key"));
        assert!(!options.contains_key("aws_session_token"));
        assert!(!options.contains_key("disable_config_load"));
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

        // DirectoryNamespaceBuilder constructs fresh params with only a static accessor.
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("aws_access_key_id".to_string(), "explicit-key".to_string()),
                    (
                        "aws_secret_access_key".to_string(),
                        "explicit-secret".to_string(),
                    ),
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
    async fn non_aws_dynamic_options_cannot_complete_a_partial_static_family() {
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                StorageOptionsAccessor::with_initial_and_provider(
                    HashMap::from([
                        ("aws_access_key_id".to_string(), "explicit-key".to_string()),
                        ("expires_at_millis".to_string(), "0".to_string()),
                    ]),
                    Arc::new(NonAwsOptionsProvider),
                ),
            )),
            ..Default::default()
        };

        let error = AtomicAwsStoreProvider {
            inner: Arc::new(AwsStoreProvider),
        }
        .new_store(url::Url::parse("s3://bucket/table").unwrap(), &params)
        .await
        .unwrap_err();

        assert!(error.to_string().contains("require both"));
    }

    #[tokio::test]
    async fn complete_dynamic_credentials_replace_a_partial_static_family() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let resolved_credential = Arc::new(Mutex::new(None));
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                StorageOptionsAccessor::with_initial_and_provider(
                    HashMap::from([
                        ("aws_access_key_id".to_string(), "stale-key".to_string()),
                        ("expires_at_millis".to_string(), "0".to_string()),
                    ]),
                    Arc::new(RotatingOptionsProvider {
                        fetches: fetches.clone(),
                    }),
                ),
            )),
            ..Default::default()
        };

        AtomicAwsStoreProvider {
            inner: Arc::new(ResolvingProvider {
                resolved_credential: resolved_credential.clone(),
            }),
        }
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

    #[test]
    fn wrapper_delegates_custom_path_extraction() {
        let provider = AtomicAwsStoreProvider {
            inner: Arc::new(CustomPathProvider),
        };

        assert_eq!(
            provider
                .extract_path(&url::Url::parse("s3://bucket/original/path").unwrap())
                .unwrap(),
            Path::from("custom/tenant/path")
        );
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

    #[tokio::test]
    async fn installing_the_wrapper_does_not_reuse_a_preexisting_store() {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let params = object_store_params_from_storage_options(local_s3_options());
        let url = url::Url::parse("s3://bucket/table").unwrap();
        let before = registry.get_store(url.clone(), &params).await.unwrap();

        let session = lance::session::Session::new(16, 16, registry.clone());
        install_atomic_aws_provider(&session);
        let after = registry.get_store(url, &params).await.unwrap();

        assert!(
            !Arc::ptr_eq(&before, &after),
            "the wrapper cache generation must isolate pre-install stores"
        );
    }

    #[tokio::test]
    async fn opendal_preserves_custom_provider_store_behavior() {
        let marker: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "s3",
            Arc::new(CustomStoreProvider {
                marker: marker.clone(),
            }),
        );
        let session = lance::session::Session::new(16, 16, registry.clone());
        install_atomic_aws_provider(&session);
        let mut options = local_s3_options();
        options.insert("use_opendal".to_string(), "true".to_string());

        let store = registry
            .get_provider("s3")
            .unwrap()
            .new_store(
                url::Url::parse("s3://bucket/table").unwrap(),
                &object_store_params_from_storage_options(options),
            )
            .await
            .unwrap();

        assert!(
            Arc::ptr_eq(&store.inner, &marker),
            "the wrapper must not discard custom provider store behavior"
        );
    }

    #[tokio::test]
    async fn wrapper_store_prefix_matches_registry_identity() {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let session = lance::session::Session::new(16, 16, registry.clone());
        install_atomic_aws_provider(&session);
        let uri = "s3://bucket/table";
        let url = url::Url::parse(uri).unwrap();
        let params = object_store_params_from_storage_options(local_s3_options());

        let store = registry.get_store(url, &params).await.unwrap();
        let registry_prefix = registry
            .calculate_object_store_prefix(uri, params.storage_options())
            .unwrap();

        assert_eq!(store.store_prefix, registry_prefix);
    }

    #[tokio::test]
    async fn identical_explicit_options_reuse_the_session_store() {
        let registry = Arc::new(ObjectStoreRegistry::default());
        let session = lance::session::Session::new(16, 16, registry.clone());
        install_atomic_aws_provider(&session);
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
