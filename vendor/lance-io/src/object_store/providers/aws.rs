// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

#[cfg(test)]
use mock_instant::thread_local::{SystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::S3};

use aws_config::Region;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::ecs::EcsCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_credential_types::provider::ProvideCredentials;
use object_store::{
    ClientOptions, CredentialProvider, Result as ObjectStoreResult, RetryConfig,
    StaticCredentialProvider,
    aws::{
        AmazonS3Builder, AmazonS3ConfigKey, AwsCredential as ObjectStoreAwsCredential,
        AwsCredentialProvider,
    },
};
use tokio::sync::RwLock;
use url::Url;

use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions, StorageOptionsAccessor,
    StorageOptionsProvider,
    dynamic_credentials::{NamespaceCredentialsProvider, build_dynamic_credential_provider},
    dynamic_opendal::DynamicOpenDalStore,
    throttle::{AimdThrottleConfig, AimdThrottleState, AimdThrottledStore, cloud_http_connector},
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct AwsStoreProvider;

const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const AWS_SESSION_TOKEN: &str = "aws_session_token";

fn is_aws_credential_key(key: &AmazonS3ConfigKey) -> bool {
    matches!(
        key,
        AmazonS3ConfigKey::AccessKeyId
            | AmazonS3ConfigKey::SecretAccessKey
            | AmazonS3ConfigKey::Token
    )
}

fn has_aws_credential_member(options: &HashMap<String, String>) -> bool {
    options.keys().any(|key| {
        AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
            .is_ok_and(|key| is_aws_credential_key(&key))
    })
}

/// Merge AWS environment options without combining two credential authorities.
fn with_atomic_env_s3(options: &mut StorageOptions) {
    merge_atomic_aws_environment(
        options,
        std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?))),
    );
}

/// Merge a supplied environment into S3 options without mixing credential families.
///
/// This is public only so downstream regression tests can provide a deterministic environment.
#[doc(hidden)]
pub fn merge_atomic_aws_environment(
    options: &mut StorageOptions,
    environment: impl IntoIterator<Item = (String, String)>,
) {
    let has_explicit_credential = has_aws_credential_member(&options.0);
    for (key, value) in environment {
        if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
            && !(has_explicit_credential && is_aws_credential_key(&config_key))
        {
            options
                .0
                .entry(config_key.as_ref().to_string())
                .or_insert(value);
        }
    }
}

fn canonical_opendal_s3_config(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .map(|(key, value)| {
            let key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                .map(|key| key.as_ref().to_string())
                .unwrap_or_else(|_| key.clone());
            (key, value.clone())
        })
        .collect()
}

fn dynamic_aws_credential_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .filter_map(|(key, value)| {
            let key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
            is_aws_credential_key(&key).then(|| (key.as_ref().to_string(), value.clone()))
        })
        .collect()
}

fn normalize_opendal_s3_config(
    options: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut options = canonical_opendal_s3_config(options);
    let key_id = options.get(AWS_ACCESS_KEY_ID);
    let secret_key = options.get(AWS_SECRET_ACCESS_KEY);
    let token = options.get(AWS_SESSION_TOKEN);
    if key_id.is_some() || secret_key.is_some() || token.is_some() {
        if key_id.is_none() || secret_key.is_none() {
            return Err(Error::invalid_input(
                "Explicit AWS credentials require both aws_access_key_id and aws_secret_access_key",
            ));
        }
        // Once one explicit family wins, OpenDAL must not consult a second ambient authority.
        options.insert("disable_config_load".to_string(), "true".to_string());
    }
    Ok(options)
}

fn build_opendal_s3_store(config: HashMap<String, String>) -> Result<OpendalStore> {
    let operator = Operator::from_iter::<S3>(config).map_err(|error| {
        Error::invalid_input(format!("Failed to create S3 operator: {error:?}"))
    })?;
    Ok(OpendalStore::new(operator))
}

#[derive(Debug)]
struct AwsCredentialStorageOptionsProvider {
    provider: AwsCredentialProvider,
}

#[async_trait::async_trait]
impl StorageOptionsProvider for AwsCredentialStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let credential = self
            .provider
            .get_credential()
            .await
            .map_err(|error| Error::io_source(Box::new(error)))?;
        let mut options = HashMap::from([
            (AWS_ACCESS_KEY_ID.to_string(), credential.key_id.clone()),
            (
                AWS_SECRET_ACCESS_KEY.to_string(),
                credential.secret_key.clone(),
            ),
            // The object_store provider owns its own cache, so ask it on every store access.
            ("expires_at_millis".to_string(), "0".to_string()),
        ]);
        if let Some(token) = &credential.token {
            options.insert(AWS_SESSION_TOKEN.to_string(), token.clone());
        }
        Ok(Some(options))
    }

    fn provider_id(&self) -> String {
        format!(
            "aws-credential-provider[{:p}]",
            Arc::as_ptr(&self.provider) as *const ()
        )
    }
}

impl AwsStoreProvider {
    async fn build_amazon_s3_store(
        &self,
        base_path: &mut Url,
        params: &ObjectStoreParams,
        storage_options: &StorageOptions,
        is_s3_express: bool,
        throttle_state: Option<&AimdThrottleState>,
    ) -> Result<Arc<dyn OSObjectStore>> {
        // Use a low retry count since the AIMD throttle layer handles
        // throttle recovery with its own retry loop.
        let retry_config = RetryConfig {
            backoff: Default::default(),
            max_retries: storage_options.client_max_retries(),
            retry_timeout: Duration::from_secs(storage_options.client_retry_timeout()),
        };

        let mut s3_storage_options = storage_options.as_s3_options();
        let region = resolve_s3_region(base_path, &s3_storage_options).await?;

        // Get accessor from params
        let accessor = params.get_accessor();

        let provider_scheme = storage_options.aws_provider_scheme()?;

        let (aws_creds, region) = build_aws_credential(
            params.s3_credentials_refresh_offset,
            params.aws_credentials.clone(),
            Some(&s3_storage_options),
            region,
            accessor,
            provider_scheme,
        )
        .await?;

        // Set S3Express flag if detected
        if is_s3_express {
            s3_storage_options.insert(AmazonS3ConfigKey::S3Express, true.to_string());
        }

        // Compute the metrics label before rewriting the URL below so it
        // matches the prefix the registry uses to key this store.
        let store_prefix =
            self.calculate_object_store_prefix(base_path, Some(&storage_options.0))?;

        // before creating the OSObjectStore we need to rewrite the url to drop ddb related parts
        base_path.set_scheme("s3").unwrap();
        base_path.set_query(None);

        // we can't use parse_url_opts here because we need to manually set the credentials provider
        let mut builder =
            AmazonS3Builder::new().with_client_options(storage_options.client_options()?);
        for (key, value) in s3_storage_options {
            builder = builder.with_config(key, value);
        }
        builder = builder
            .with_url(base_path.as_ref())
            .with_credentials(aws_creds)
            .with_retry(retry_config)
            .with_region(region);

        builder = builder.with_http_connector(cloud_http_connector(throttle_state, store_prefix));

        Ok(Arc::new(builder.build()?) as Arc<dyn OSObjectStore>)
    }

    async fn build_opendal_s3_store(
        &self,
        base_path: &Url,
        params: &ObjectStoreParams,
        storage_options: &StorageOptions,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("S3 URL must contain bucket name"))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        if let Some(provider_scheme) = storage_options.aws_provider_scheme()? {
            return Result::Err(Error::not_supported(format!(
                "OpendalStore does not currently support an explicit provider_scheme (currently set to {:?})",
                provider_scheme
            )));
        }

        let mut config_map = canonical_opendal_s3_config(&storage_options.0);
        config_map.insert("bucket".to_string(), bucket);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), "/".to_string());
        }

        let dynamic_accessor = if let Some(provider) = params.aws_credentials.clone() {
            Some(Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
                AwsCredentialStorageOptionsProvider { provider },
            ))))
        } else {
            params
                .get_accessor()
                .filter(|accessor| accessor.has_provider())
        };

        if let Some(accessor) = dynamic_accessor {
            // Dynamic OpenDAL refresh is deliberately credential-only. Noncredential changes can
            // alter the outer ObjectStore contract and require a new registry entry instead.
            let store = DynamicOpenDalStore::new(
                "s3",
                config_map,
                accessor,
                normalize_opendal_s3_config,
                build_opendal_s3_store,
            )
            .with_dynamic_options_filter(dynamic_aws_credential_options)
            .with_atomic_key_group([
                AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY,
                AWS_SESSION_TOKEN,
            ]);
            // Validate the currently active family and prime the normalized-config cache.
            store.current_store().await?;
            Ok(Arc::new(store) as Arc<dyn OSObjectStore>)
        } else {
            let config_map = normalize_opendal_s3_config(&config_map)?;
            Ok(Arc::new(build_opendal_s3_store(config_map)?) as Arc<dyn OSObjectStore>)
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for AwsStoreProvider {
    async fn new_store(
        &self,
        mut base_path: Url,
        params: &ObjectStoreParams,
    ) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let mut storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());
        with_atomic_env_s3(&mut storage_options);
        let download_retry_count = storage_options.download_retry_count();

        let use_opendal = storage_options
            .0
            .get("use_opendal")
            .map(|v| v == "true")
            .unwrap_or(false);

        // Determine S3 Express and constant size upload parts before building the store
        let is_s3_express = check_s3_express(&base_path, &storage_options);

        let use_constant_size_upload_parts = storage_options
            .0
            .get("aws_endpoint")
            .map(|endpoint| endpoint.contains("r2.cloudflarestorage.com"))
            .unwrap_or(false);

        let throttle_config = AimdThrottleConfig::from_storage_options(params.storage_options())?;
        let throttle_state = if throttle_config.is_disabled() {
            None
        } else {
            Some(AimdThrottleState::new(throttle_config)?)
        };

        let inner = if use_opendal {
            // Use OpenDAL implementation
            self.build_opendal_s3_store(&base_path, params, &storage_options)
                .await?
        } else {
            // Use default Amazon S3 implementation
            self.build_amazon_s3_store(
                &mut base_path,
                params,
                &storage_options,
                is_s3_express,
                throttle_state.as_ref(),
            )
            .await?
        };
        let inner = if let Some(throttle_state) = throttle_state {
            Arc::new(AimdThrottledStore::new_with_state(
                inner,
                throttle_state,
                !use_opendal,
            )) as Arc<dyn OSObjectStore>
        } else {
            inner
        };

        Ok(ObjectStore {
            inner,
            scheme: String::from(base_path.scheme()),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts,
            list_is_lexically_ordered: !is_s3_express,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }
}

/// Check if the storage is S3 Express
fn check_s3_express(url: &Url, storage_options: &StorageOptions) -> bool {
    storage_options
        .0
        .get("s3_express")
        .map(|v| v == "true")
        .unwrap_or(false)
        || url.authority().ends_with("--x-s3")
}

/// Figure out the S3 region of the bucket.
///
/// This resolves in order of precedence:
/// 1. The region provided in the storage options
/// 2. (If endpoint is not set), the region returned by the S3 API for the bucket
///
/// It can return None if no region is provided and the endpoint is set.
async fn resolve_s3_region(
    url: &Url,
    storage_options: &HashMap<AmazonS3ConfigKey, String>,
) -> Result<Option<String>> {
    if let Some(region) = storage_options.get(&AmazonS3ConfigKey::Region) {
        Ok(Some(region.clone()))
    } else if storage_options.get(&AmazonS3ConfigKey::Endpoint).is_none() {
        // If no endpoint is set, we can assume this is AWS S3 and the region
        // can be resolved from the bucket.
        let bucket = url.host_str().ok_or_else(|| {
            Error::invalid_input(format!("Could not parse bucket from url: {}", url))
        })?;

        let mut client_options = ClientOptions::default();
        for (key, value) in storage_options {
            if let AmazonS3ConfigKey::Client(client_key) = key {
                client_options = client_options.with_config(*client_key, value.clone());
            }
        }

        let bucket_region =
            object_store::aws::resolve_bucket_region(bucket, &client_options).await?;
        Ok(Some(bucket_region))
    } else {
        Ok(None)
    }
}

/// Selects which AWS credential provider to use for a dataset.
///
/// When set, overrides automatic credential resolution for everything except an
/// explicitly-supplied `credentials` provider or `storage_options_accessor`.
#[derive(Debug, Clone, PartialEq)]
pub enum AwsProviderScheme {
    /// Require static access-key credentials (`aws_access_key_id` +
    /// `aws_secret_access_key`). Returns an error if they are absent.
    Token,
    /// Use the ECS/Pod Identity container credential endpoint.
    /// The endpoint URI is read from the `AWS_CONTAINER_CREDENTIALS_FULL_URI`
    /// or `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` environment variables.
    Ecs,
    /// Use IRSA (IAM Roles for Service Accounts) web identity token credentials.
    /// The token file and role ARN are read from the `AWS_WEB_IDENTITY_TOKEN_FILE`
    /// and `AWS_ROLE_ARN` environment variables.
    Irsa,
}

/// Build AWS credentials
///
/// This resolves credentials from the following sources in order:
/// 1. An explicit `credentials` provider
/// 2. An explicit `storage_options_accessor` with a provider
/// 3. If `provider_scheme` is set:
///    - [`AwsProviderScheme::Token`]: static access-key credentials (error if absent)
///    - [`AwsProviderScheme::Ecs`]: ECS container credential provider
///    - [`AwsProviderScheme::Irsa`]: web identity token (IRSA) provider
/// 4. Static access-key credentials from `storage_options`, if present
/// 5. The default AWS credential provider chain
///
/// # Storage Options Accessor
///
/// When `storage_options_accessor` is provided and has a dynamic provider,
/// credentials are fetched and cached by the accessor with automatic refresh
/// before expiration.
///
/// `credentials_refresh_offset` is the amount of time before expiry to refresh credentials.
pub async fn build_aws_credential(
    credentials_refresh_offset: Duration,
    credentials: Option<AwsCredentialProvider>,
    storage_options: Option<&HashMap<AmazonS3ConfigKey, String>>,
    region: Option<String>,
    storage_options_accessor: Option<Arc<StorageOptionsAccessor>>,
    provider_scheme: Option<AwsProviderScheme>,
) -> Result<(AwsCredentialProvider, String)> {
    use aws_config::meta::region::RegionProviderChain;
    const DEFAULT_REGION: &str = "us-west-2";

    let region = if let Some(region) = region {
        region
    } else {
        RegionProviderChain::default_provider()
            .or_else(DEFAULT_REGION)
            .region()
            .await
            .map(|r| r.as_ref().to_string())
            .unwrap_or(DEFAULT_REGION.to_string())
    };

    // If the user supplied their own credential provider that takes top priority
    if let Some(creds) = credentials {
        return Ok((creds, region));
    }

    // Otherwise, if the user provided a storage_options_accessor, try and use that
    if let Some(dynamic_creds) = build_dynamic_credential_provider::<ObjectStoreAwsCredential>(
        storage_options_accessor.clone(),
    )
    .await?
    {
        return Ok((dynamic_creds, region));
    }

    // If the user provided a storage_options_accessor, then it must not have matched AWS.
    // Log a message and ignore it.
    if storage_options_accessor
        .as_ref()
        .is_some_and(|a| a.has_provider())
    {
        log::debug!(
            "Storage options from provider do not contain explicit AWS credentials, \
             falling back to default AWS credentials chain."
        );
    }

    // If the caller specified an explicit provider scheme, use only that provider.
    if let Some(scheme) = provider_scheme {
        return match scheme {
            AwsProviderScheme::Token => {
                let creds = storage_options
                    .and_then(extract_static_s3_credentials)
                    .ok_or_else(|| {
                        Error::invalid_input(
                            "aws_provider_scheme=token requires aws_access_key_id \
                             and aws_secret_access_key to be set",
                        )
                    })?;
                Ok((Arc::new(creds), region))
            }
            AwsProviderScheme::Ecs => {
                let provider = EcsCredentialsProvider::builder().build();
                Ok((
                    Arc::new(AwsCredentialAdapter::new(
                        Arc::new(provider),
                        credentials_refresh_offset,
                    )),
                    region,
                ))
            }
            AwsProviderScheme::Irsa => {
                let conf = ProviderConfig::default().with_region(Some(Region::new(region.clone())));
                let provider = WebIdentityTokenCredentialsProvider::builder()
                    .configure(&conf)
                    .build();
                Ok((
                    Arc::new(AwsCredentialAdapter::new(
                        Arc::new(provider),
                        credentials_refresh_offset,
                    )),
                    region,
                ))
            }
        };
    }

    if let Some(opts) = storage_options {
        // Check for static credentials (access key & secret)
        if let Some(creds) = extract_static_s3_credentials(opts) {
            return Ok((Arc::new(creds), region));
        }
        if opts.keys().any(is_aws_credential_key) {
            return Err(Error::invalid_input(
                "Explicit AWS credentials require both aws_access_key_id and aws_secret_access_key",
            ));
        }
    }

    let credentials_provider = DefaultCredentialsChain::builder().build().await;
    Ok((
        Arc::new(AwsCredentialAdapter::new(
            Arc::new(credentials_provider),
            credentials_refresh_offset,
        )),
        region,
    ))
}

fn extract_static_s3_credentials(
    options: &HashMap<AmazonS3ConfigKey, String>,
) -> Option<StaticCredentialProvider<ObjectStoreAwsCredential>> {
    let key_id = options.get(&AmazonS3ConfigKey::AccessKeyId).cloned();
    let secret_key = options.get(&AmazonS3ConfigKey::SecretAccessKey).cloned();
    let token = options.get(&AmazonS3ConfigKey::Token).cloned();
    match (key_id, secret_key, token) {
        (Some(key_id), Some(secret_key), token) => {
            Some(StaticCredentialProvider::new(ObjectStoreAwsCredential {
                key_id,
                secret_key,
                token,
            }))
        }
        _ => None,
    }
}

/// Adapt an AWS SDK cred into object_store credentials
#[derive(Debug)]
pub struct AwsCredentialAdapter {
    pub inner: Arc<dyn ProvideCredentials>,

    // RefCell can't be shared across threads, so we use HashMap
    cache: Arc<RwLock<HashMap<String, Arc<aws_credential_types::Credentials>>>>,

    // The amount of time before expiry to refresh credentials
    credentials_refresh_offset: Duration,
}

impl AwsCredentialAdapter {
    pub fn new(
        provider: Arc<dyn ProvideCredentials>,
        credentials_refresh_offset: Duration,
    ) -> Self {
        Self {
            inner: provider,
            cache: Arc::new(RwLock::new(HashMap::new())),
            credentials_refresh_offset,
        }
    }
}

const AWS_CREDS_CACHE_KEY: &str = "aws_credentials";

/// Convert std::time::SystemTime from AWS SDK to our mockable SystemTime
fn to_system_time(time: std::time::SystemTime) -> SystemTime {
    let duration_since_epoch = time
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time should be after UNIX_EPOCH");
    UNIX_EPOCH + duration_since_epoch
}

#[async_trait::async_trait]
impl CredentialProvider for AwsCredentialAdapter {
    type Credential = ObjectStoreAwsCredential;

    async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
        let cached_creds = {
            let cache_value = self.cache.read().await.get(AWS_CREDS_CACHE_KEY).cloned();
            let expired = cache_value
                .clone()
                .map(|cred| {
                    cred.expiry()
                        .map(|exp| {
                            to_system_time(exp)
                                .checked_sub(self.credentials_refresh_offset)
                                .expect("this time should always be valid")
                                < SystemTime::now()
                        })
                        // no expiry is never expire
                        .unwrap_or(false)
                })
                .unwrap_or(true); // no cred is the same as expired;
            if expired { None } else { cache_value.clone() }
        };

        if let Some(creds) = cached_creds {
            Ok(Arc::new(Self::Credential {
                key_id: creds.access_key_id().to_string(),
                secret_key: creds.secret_access_key().to_string(),
                token: creds.session_token().map(|s| s.to_string()),
            }))
        } else {
            let refreshed_creds =
                Arc::new(self.inner.provide_credentials().await.map_err(|e| {
                    Error::internal(format!("Failed to get AWS credentials: {:?}", e))
                })?);

            self.cache
                .write()
                .await
                .insert(AWS_CREDS_CACHE_KEY.to_string(), refreshed_creds.clone());

            Ok(Arc::new(Self::Credential {
                key_id: refreshed_creds.access_key_id().to_string(),
                secret_key: refreshed_creds.secret_access_key().to_string(),
                token: refreshed_creds.session_token().map(|s| s.to_string()),
            }))
        }
    }
}

impl StorageOptions {
    /// Add values from the environment to storage options.
    ///
    /// Only adds keys that are not already present, so explicitly-set options
    /// (including empty-string sentinels) always take precedence over env vars.
    pub fn with_env_s3(&mut self) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str())
                && let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())
                && !self.0.contains_key(config_key.as_ref())
            {
                self.0
                    .insert(config_key.as_ref().to_string(), value.to_string());
            }
        }
    }

    /// Subset of options relevant for s3 storage
    pub fn as_s3_options(&self) -> HashMap<AmazonS3ConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let s3_key = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((s3_key, value.clone()))
            })
            .collect()
    }

    /// Parse the `aws_provider_scheme` storage option, if set.
    pub fn aws_provider_scheme(&self) -> Result<Option<AwsProviderScheme>> {
        match self.0.get("aws_provider_scheme").map(|s| s.as_str()) {
            None | Some("") => Ok(None),
            Some("token") => Ok(Some(AwsProviderScheme::Token)),
            Some("ecs") => Ok(Some(AwsProviderScheme::Ecs)),
            Some("irsa") => Ok(Some(AwsProviderScheme::Irsa)),
            Some(other) => Err(Error::invalid_input(format!(
                "Invalid aws_provider_scheme '{}'. Valid values are: token, ecs, irsa",
                other
            ))),
        }
    }
}

impl ObjectStoreParams {
    /// Create a new instance of [`ObjectStoreParams`] based on the AWS credentials.
    pub fn with_aws_credentials(
        aws_credentials: Option<AwsCredentialProvider>,
        region: Option<String>,
    ) -> Self {
        let storage_options_accessor = region.map(|region| {
            let opts: HashMap<String, String> =
                [("region".into(), region)].iter().cloned().collect();
            Arc::new(StorageOptionsAccessor::with_static_options(opts))
        });
        Self {
            aws_credentials,
            storage_options_accessor,
            ..Default::default()
        }
    }
}

pub type DynamicStorageOptionsCredentialProvider =
    NamespaceCredentialsProvider<ObjectStoreAwsCredential>;

#[cfg(test)]
mod tests {
    use crate::object_store::ObjectStoreRegistry;
    use crate::object_store::StorageOptionsProvider;
    use mock_instant::thread_local::MockClock;
    use object_store::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[derive(Debug, Default)]
    struct MockAwsCredentialsProvider {
        called: AtomicBool,
    }

    #[async_trait::async_trait]
    impl CredentialProvider for MockAwsCredentialsProvider {
        type Credential = ObjectStoreAwsCredential;

        async fn get_credential(&self) -> ObjectStoreResult<Arc<Self::Credential>> {
            self.called.store(true, Ordering::Relaxed);
            Ok(Arc::new(Self::Credential {
                key_id: "".to_string(),
                secret_key: "".to_string(),
                token: None,
            }))
        }
    }

    #[tokio::test]
    async fn test_injected_aws_creds_option_is_used() {
        let mock_provider = Arc::new(MockAwsCredentialsProvider::default());
        let registry = Arc::new(ObjectStoreRegistry::default());

        let params = ObjectStoreParams {
            aws_credentials: Some(mock_provider.clone() as AwsCredentialProvider),
            ..ObjectStoreParams::default()
        };

        // Not called yet
        assert!(!mock_provider.called.load(Ordering::Relaxed));

        let (store, _) = ObjectStore::from_uri_and_params(registry, "s3://not-a-bucket", &params)
            .await
            .unwrap();

        // fails, but we don't care
        let _ = store
            .open(&Path::parse("/").unwrap())
            .await
            .unwrap()
            .get_range(0..1)
            .await;

        // Not called yet
        assert!(mock_provider.called.load(Ordering::Relaxed));
    }

    #[test]
    fn test_s3_path_parsing() {
        let provider = AwsStoreProvider;

        let cases = [
            ("s3://bucket/path/to/file", "path/to/file"),
            // for non ASCII string tests: the URL encodes them, extract_path must decode back
            ("s3://bucket/测试path/to/file", "测试path/to/file"),
            ("s3://bucket/path/&to/file", "path/&to/file"),
            ("s3://bucket/path/=to/file", "path/=to/file"),
            (
                "s3+ddb://bucket/path/to/file?ddbTableName=test",
                "path/to/file",
            ),
        ];

        for (uri, expected_path) in cases {
            let url = Url::parse(uri).unwrap();
            let path = provider.extract_path(&url).unwrap();
            // extract_path decodes url.path(), so the Path stores the raw (decoded)
            // string. Path::parse keeps its input verbatim, matching that, whereas
            // Path::from would percent-encode non-ASCII bytes and not match.
            let expected_path = Path::parse(expected_path).unwrap();
            assert_eq!(path, expected_path)
        }
    }

    // Regression test for https://github.com/lance-format/lance/issues/6643
    // extract_path must NOT double-encode paths that contain non-ASCII characters.
    // url.path() returns a percent-encoded string; we must decode it back to raw
    // UTF-8 before storing it in a Path, so the object store HTTP client can apply
    // a single, correct percent-encoding when building the request URL.
    #[test]
    fn test_s3_non_ascii_path_no_double_encoding() {
        let provider = AwsStoreProvider;

        // "s3://bucket/中文路径" → url.path() == "/%E4%B8%AD%E6%96%87%E8%B7%AF%E5%BE%84".
        // The buggy Path::parse(url.path()) stored "%E4%B8%AD..." verbatim; the S3
        // client then percent-encodes the '%' again, yielding "%25E4%25B8%25AD...".
        // With Path::from_url_path the Path stores the decoded UTF-8 instead.
        let url = Url::parse("s3://bucket/中文路径").unwrap();
        let path = provider.extract_path(&url).unwrap();

        // The Path must hold the decoded UTF-8, not the percent-encoded form.
        assert_eq!(path.as_ref(), "中文路径");
    }

    #[test]
    fn test_is_s3_express() {
        let cases = [
            (
                "s3://bucket/path/to/file",
                HashMap::from([("s3_express".to_string(), "true".to_string())]),
                true,
            ),
            (
                "s3://bucket/path/to/file",
                HashMap::from([("s3_express".to_string(), "false".to_string())]),
                false,
            ),
            ("s3://bucket/path/to/file", HashMap::from([]), false),
            (
                "s3://bucket--x-s3/path/to/file",
                HashMap::from([("s3_express".to_string(), "true".to_string())]),
                true,
            ),
            (
                "s3://bucket--x-s3/path/to/file",
                HashMap::from([("s3_express".to_string(), "false".to_string())]),
                true, // URL takes precedence
            ),
            ("s3://bucket--x-s3/path/to/file", HashMap::from([]), true),
        ];

        for (uri, storage_map, expected) in cases {
            let url = Url::parse(uri).unwrap();
            let storage_options = StorageOptions(storage_map);
            let is_s3_express = check_s3_express(&url, &storage_options);
            assert_eq!(is_s3_express, expected);
        }
    }

    #[tokio::test]
    async fn test_use_opendal_flag() {
        use crate::object_store::StorageOptionsAccessor;
        let provider = AwsStoreProvider;
        let url = Url::parse("s3://test-bucket/path").unwrap();
        let params_with_flag = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    ("region".to_string(), "us-west-2".to_string()),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider
            .new_store(url.clone(), &params_with_flag)
            .await
            .unwrap();
        assert_eq!(store.scheme, "s3");
    }

    #[derive(Debug)]
    struct MockStorageOptionsProvider {
        call_count: Arc<RwLock<usize>>,
        expires_in_millis: Option<u64>,
    }

    impl MockStorageOptionsProvider {
        fn new(expires_in_millis: Option<u64>) -> Self {
            Self {
                call_count: Arc::new(RwLock::new(0)),
                expires_in_millis,
            }
        }

        async fn get_call_count(&self) -> usize {
            *self.call_count.read().await
        }
    }

    #[async_trait::async_trait]
    impl StorageOptionsProvider for MockStorageOptionsProvider {
        async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
            let count = {
                let mut c = self.call_count.write().await;
                *c += 1;
                *c
            };

            let mut options = HashMap::from([
                ("aws_access_key_id".to_string(), format!("AKID_{}", count)),
                (
                    "aws_secret_access_key".to_string(),
                    format!("SECRET_{}", count),
                ),
                ("aws_session_token".to_string(), format!("TOKEN_{}", count)),
            ]);

            if let Some(expires_in) = self.expires_in_millis {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expires_at = now_ms + expires_in;
                options.insert("expires_at_millis".to_string(), expires_at.to_string());
            }

            Ok(Some(options))
        }

        fn provider_id(&self) -> String {
            let ptr = Arc::as_ptr(&self.call_count) as usize;
            format!("MockStorageOptionsProvider {{ id: {} }}", ptr)
        }
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_with_initial_cache() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create a mock provider that returns credentials expiring in 10 minutes
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            600_000, // Expires in 10 minutes
        )));

        // Create initial options with cached credentials that expire in 10 minutes
        let expires_at = now_ms + 600_000; // 10 minutes from now
        let initial_options = HashMap::from([
            ("aws_access_key_id".to_string(), "AKID_CACHED".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "SECRET_CACHED".to_string(),
            ),
            ("aws_session_token".to_string(), "TOKEN_CACHED".to_string()),
            ("expires_at_millis".to_string(), expires_at.to_string()),
            ("refresh_offset_millis".to_string(), "300000".to_string()), // 5 minute refresh offset
        ]);

        let provider = DynamicStorageOptionsCredentialProvider::from_provider_with_initial(
            mock.clone(),
            initial_options,
        );

        // First call should use cached credentials (not expired yet)
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_CACHED");
        assert_eq!(cred.secret_key, "SECRET_CACHED");
        assert_eq!(cred.token, Some("TOKEN_CACHED".to_string()));

        // Should not have called the provider yet
        assert_eq!(mock.get_call_count().await, 0);
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_with_expired_cache() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create a mock provider that returns credentials expiring in 10 minutes
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            600_000, // Expires in 10 minutes
        )));

        // Create initial options with credentials that expired 1 second ago
        let expired_time = now_ms - 1_000; // 1 second ago
        let initial_options = HashMap::from([
            ("aws_access_key_id".to_string(), "AKID_EXPIRED".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "SECRET_EXPIRED".to_string(),
            ),
            ("expires_at_millis".to_string(), expired_time.to_string()),
            ("refresh_offset_millis".to_string(), "300000".to_string()), // 5 minute refresh offset
        ]);

        let provider = DynamicStorageOptionsCredentialProvider::from_provider_with_initial(
            mock.clone(),
            initial_options,
        );

        // First call should fetch new credentials because cached ones are expired
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(cred.secret_key, "SECRET_1");
        assert_eq!(cred.token, Some("TOKEN_1".to_string()));

        // Should have called the provider once
        assert_eq!(mock.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_refresh_lead_time() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns credentials expiring in 30 seconds
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            30_000, // Expires in 30 seconds
        )));

        // Create credential provider with default 60 second refresh offset
        // This means credentials should be refreshed when they have less than 60 seconds left
        let provider = DynamicStorageOptionsCredentialProvider::from_provider(mock.clone());

        // First call should fetch credentials from provider (no initial cache)
        // Credentials expire in 30 seconds, which is less than our 60 second refresh offset,
        // so they should be considered "needs refresh" immediately
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(mock.get_call_count().await, 1);

        // Second call should trigger refresh because credentials expire in 30 seconds
        // but our refresh lead time is 60 seconds (now + 60sec > expires_at)
        // The mock will return new credentials (AKID_2) with the same expiration
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_2");
        assert_eq!(mock.get_call_count().await, 2);
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_no_initial_cache() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // Create a mock provider that returns credentials expiring in 2 minutes
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            120_000, // Expires in 2 minutes
        )));

        // Create credential provider without initial cache, using default 60 second refresh offset
        let provider = DynamicStorageOptionsCredentialProvider::from_provider(mock.clone());

        // First call should fetch from provider (call count = 1)
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(cred.secret_key, "SECRET_1");
        assert_eq!(cred.token, Some("TOKEN_1".to_string()));
        assert_eq!(mock.get_call_count().await, 1);

        // Second call should use cached credentials (not expired yet, still > 60 seconds remaining)
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(mock.get_call_count().await, 1); // Still 1, didn't fetch again

        // Advance time to 90 seconds - should trigger refresh (within 60 sec refresh offset)
        // At this point, credentials expire in 30 seconds (< 60 sec offset)
        MockClock::set_system_time(Duration::from_secs(100_000 + 90));
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_2");
        assert_eq!(cred.secret_key, "SECRET_2");
        assert_eq!(cred.token, Some("TOKEN_2".to_string()));
        assert_eq!(mock.get_call_count().await, 2);

        // Advance time to 210 seconds total (90 + 120) - should trigger another refresh
        MockClock::set_system_time(Duration::from_secs(100_000 + 210));
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_3");
        assert_eq!(cred.secret_key, "SECRET_3");
        assert_eq!(mock.get_call_count().await, 3);
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_with_initial_options() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create a mock provider that returns credentials expiring in 10 minutes
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            600_000, // Expires in 10 minutes
        )));

        // Create initial options with expiration in 10 minutes
        let expires_at = now_ms + 600_000; // 10 minutes from now
        let initial_options = HashMap::from([
            ("aws_access_key_id".to_string(), "AKID_INITIAL".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "SECRET_INITIAL".to_string(),
            ),
            ("aws_session_token".to_string(), "TOKEN_INITIAL".to_string()),
            ("expires_at_millis".to_string(), expires_at.to_string()),
            ("refresh_offset_millis".to_string(), "300000".to_string()), // 5 minute refresh offset
        ]);

        // Create credential provider with initial options
        let provider = DynamicStorageOptionsCredentialProvider::from_provider_with_initial(
            mock.clone(),
            initial_options,
        );

        // First call should use the initial credential (not expired yet)
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_INITIAL");
        assert_eq!(cred.secret_key, "SECRET_INITIAL");
        assert_eq!(cred.token, Some("TOKEN_INITIAL".to_string()));

        // Should not have called the provider yet
        assert_eq!(mock.get_call_count().await, 0);

        // Advance time to 6 minutes - this should trigger a refresh
        // (5 minute refresh offset means we refresh 5 minutes before expiration)
        MockClock::set_system_time(Duration::from_secs(100_000 + 360));
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(cred.secret_key, "SECRET_1");
        assert_eq!(cred.token, Some("TOKEN_1".to_string()));

        // Should have called the provider once
        assert_eq!(mock.get_call_count().await, 1);

        // Advance time to 11 minutes total - this should trigger another refresh
        MockClock::set_system_time(Duration::from_secs(100_000 + 660));
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_2");
        assert_eq!(cred.secret_key, "SECRET_2");
        assert_eq!(cred.token, Some("TOKEN_2".to_string()));

        // Should have called the provider twice
        assert_eq!(mock.get_call_count().await, 2);

        // Advance time to 16 minutes total - this should trigger yet another refresh
        MockClock::set_system_time(Duration::from_secs(100_000 + 960));
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_3");
        assert_eq!(cred.secret_key, "SECRET_3");
        assert_eq!(cred.token, Some("TOKEN_3".to_string()));

        // Should have called the provider three times
        assert_eq!(mock.get_call_count().await, 3);
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_concurrent_access() {
        // Create a mock provider with far future expiration
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(9999999999999)));

        let provider = Arc::new(DynamicStorageOptionsCredentialProvider::from_provider(
            mock.clone(),
        ));

        // Spawn 10 concurrent tasks that all try to get credentials at the same time
        let mut handles = vec![];
        for i in 0..10 {
            let provider = provider.clone();
            let handle = tokio::spawn(async move {
                let cred = provider.get_credential().await.unwrap();
                // Verify we got the correct credentials (should all be AKID_1 from first fetch)
                assert_eq!(cred.key_id, "AKID_1");
                assert_eq!(cred.secret_key, "SECRET_1");
                assert_eq!(cred.token, Some("TOKEN_1".to_string()));
                i // Return task number for verification
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 10 tasks completed successfully
        assert_eq!(results.len(), 10);
        for i in 0..10 {
            assert!(results.contains(&i));
        }

        // The provider should have been called exactly once (first request triggers fetch,
        // subsequent requests use cache)
        let call_count = mock.get_call_count().await;
        assert_eq!(
            call_count, 1,
            "Provider should be called exactly once despite concurrent access"
        );
    }

    #[tokio::test]
    async fn test_dynamic_credential_provider_concurrent_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create initial options with credentials that expired in the past (1000 seconds ago)
        let expires_at = now_ms - 1_000_000;
        let initial_options = HashMap::from([
            ("aws_access_key_id".to_string(), "AKID_OLD".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "SECRET_OLD".to_string(),
            ),
            ("aws_session_token".to_string(), "TOKEN_OLD".to_string()),
            ("expires_at_millis".to_string(), expires_at.to_string()),
            ("refresh_offset_millis".to_string(), "300000".to_string()), // 5 minute refresh offset
        ]);

        // Mock will return credentials expiring in 1 hour
        let mock = Arc::new(MockStorageOptionsProvider::new(Some(
            3_600_000, // Expires in 1 hour
        )));

        let provider = Arc::new(
            DynamicStorageOptionsCredentialProvider::from_provider_with_initial(
                mock.clone(),
                initial_options,
            ),
        );

        // Spawn 20 concurrent tasks that all try to get credentials at the same time
        // Since the initial credential is expired, they'll all try to refresh
        let mut handles = vec![];
        for i in 0..20 {
            let provider = provider.clone();
            let handle = tokio::spawn(async move {
                let cred = provider.get_credential().await.unwrap();
                // All should get the new credentials (AKID_1 from first fetch)
                assert_eq!(cred.key_id, "AKID_1");
                assert_eq!(cred.secret_key, "SECRET_1");
                assert_eq!(cred.token, Some("TOKEN_1".to_string()));
                i
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 20 tasks completed successfully
        assert_eq!(results.len(), 20);

        // The provider should have been called at least once, but possibly more times
        // due to the try_write mechanism and race conditions
        let call_count = mock.get_call_count().await;
        assert!(
            call_count >= 1,
            "Provider should be called at least once, was called {} times",
            call_count
        );

        // It shouldn't be called 20 times though - the lock should prevent most concurrent fetches
        assert!(
            call_count < 10,
            "Provider should not be called too many times due to lock contention, was called {} times",
            call_count
        );
    }

    #[tokio::test]
    async fn test_explicit_aws_credentials_takes_precedence_over_accessor() {
        // Create a mock storage options provider that should NOT be called
        let mock_storage_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create an accessor with the mock provider
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(
            mock_storage_provider.clone(),
        ));

        // Create an explicit AWS credentials provider
        let explicit_cred_provider = Arc::new(MockAwsCredentialsProvider::default());

        // Build credentials with both aws_credentials AND accessor
        // The explicit aws_credentials should take precedence
        let (result, _region) = build_aws_credential(
            Duration::from_secs(300),
            Some(explicit_cred_provider.clone() as AwsCredentialProvider),
            None, // no storage_options
            Some("us-west-2".to_string()),
            Some(accessor),
            None,
        )
        .await
        .unwrap();

        // Get credential from the result
        let cred = result.get_credential().await.unwrap();

        // The explicit provider should have been called (it returns empty strings)
        assert!(explicit_cred_provider.called.load(Ordering::Relaxed));

        // The storage options provider should NOT have been called
        assert_eq!(
            mock_storage_provider.get_call_count().await,
            0,
            "Storage options provider should not be called when explicit aws_credentials is provided"
        );

        // Verify we got credentials from the explicit provider (empty strings)
        assert_eq!(cred.key_id, "");
        assert_eq!(cred.secret_key, "");
    }

    #[tokio::test]
    async fn test_accessor_used_when_no_explicit_aws_credentials() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;

        // Create a mock storage options provider
        let mock_storage_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        // Create initial options
        let expires_at = now_ms + 600_000; // 10 minutes from now
        let initial_options = HashMap::from([
            (
                "aws_access_key_id".to_string(),
                "AKID_FROM_ACCESSOR".to_string(),
            ),
            (
                "aws_secret_access_key".to_string(),
                "SECRET_FROM_ACCESSOR".to_string(),
            ),
            (
                "aws_session_token".to_string(),
                "TOKEN_FROM_ACCESSOR".to_string(),
            ),
            ("expires_at_millis".to_string(), expires_at.to_string()),
            ("refresh_offset_millis".to_string(), "300000".to_string()), // 5 minute refresh offset
        ]);

        // Create an accessor with initial options and provider
        let accessor = Arc::new(StorageOptionsAccessor::with_initial_and_provider(
            initial_options,
            mock_storage_provider.clone(),
        ));

        // Build credentials with accessor but NO explicit aws_credentials
        let (result, _region) = build_aws_credential(
            Duration::from_secs(300),
            None, // no explicit aws_credentials
            None, // no storage_options
            Some("us-west-2".to_string()),
            Some(accessor),
            None,
        )
        .await
        .unwrap();

        // Get credential - should use the initial accessor credentials
        let cred = result.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_FROM_ACCESSOR");
        assert_eq!(cred.secret_key, "SECRET_FROM_ACCESSOR");

        // Storage options provider should NOT have been called yet (using cached initial creds)
        assert_eq!(mock_storage_provider.get_call_count().await, 0);

        // Advance time to trigger refresh (past the 5 minute refresh offset)
        MockClock::set_system_time(Duration::from_secs(100_000 + 360));

        // Get credential again - should now fetch from provider
        let cred = result.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID_1");
        assert_eq!(cred.secret_key, "SECRET_1");

        // Storage options provider should have been called once
        assert_eq!(mock_storage_provider.get_call_count().await, 1);
    }

    // Test that aws_provider_scheme=token selects static credentials.
    #[tokio::test]
    async fn test_provider_scheme_token() {
        let opts = HashMap::from([
            (AmazonS3ConfigKey::AccessKeyId, "AKID".to_string()),
            (AmazonS3ConfigKey::SecretAccessKey, "SECRET".to_string()),
        ]);

        let (provider, _) = build_aws_credential(
            Duration::from_secs(300),
            None,
            Some(&opts),
            Some("us-east-1".to_string()),
            None,
            Some(AwsProviderScheme::Token),
        )
        .await
        .unwrap();

        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.key_id, "AKID");
        assert_eq!(cred.secret_key, "SECRET");
    }

    // Test that aws_provider_scheme=token errors when no static credentials are present.
    #[tokio::test]
    async fn test_provider_scheme_token_errors_without_credentials() {
        let opts: HashMap<AmazonS3ConfigKey, String> = HashMap::new();

        let result = build_aws_credential(
            Duration::from_secs(300),
            None,
            Some(&opts),
            Some("us-east-1".to_string()),
            None,
            Some(AwsProviderScheme::Token),
        )
        .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("aws_provider_scheme=token"),
            "error should mention aws_provider_scheme=token"
        );
    }

    // Test that aws_provider_scheme=ecs builds a provider without error.
    // The ECS provider itself reads from env vars lazily; construction always succeeds.
    #[tokio::test]
    async fn test_provider_scheme_ecs() {
        let opts: HashMap<AmazonS3ConfigKey, String> = HashMap::new();

        let result = build_aws_credential(
            Duration::from_secs(300),
            None,
            Some(&opts),
            Some("us-east-1".to_string()),
            None,
            Some(AwsProviderScheme::Ecs),
        )
        .await;
        assert!(result.is_ok(), "ECS provider should build without error");
    }

    // Test that aws_provider_scheme=irsa builds a provider and attempts credential
    // retrieval (which fails with a provider error, not a config error like
    // "Missing Region" — confirming the region is wired through to the STS client).
    #[tokio::test]
    async fn test_provider_scheme_irsa() {
        let opts: HashMap<AmazonS3ConfigKey, String> = HashMap::new();

        let (provider, _) = build_aws_credential(
            Duration::from_secs(300),
            None,
            Some(&opts),
            Some("us-east-1".to_string()),
            None,
            Some(AwsProviderScheme::Irsa),
        )
        .await
        .unwrap();

        // Credential retrieval must fail with a provider error (missing env vars or
        // network), NOT a configuration error like "Invalid Configuration: Missing Region".
        let err = provider.get_credential().await.unwrap_err();
        assert!(
            !err.to_string().contains("Missing Region"),
            "should not fail with Missing Region; region was provided. got: {err}"
        );
    }

    // Test that an invalid aws_provider_scheme value produces a clear error.
    #[test]
    fn test_provider_scheme_invalid_value() {
        let opts = StorageOptions::new(HashMap::from([(
            "aws_provider_scheme".to_string(),
            "magic".to_string(),
        )]));
        let result = opts.aws_provider_scheme();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("magic"));
    }

    // Test that no aws_provider_scheme falls through to DefaultCredentialsChain without error.
    #[tokio::test]
    async fn test_no_provider_scheme_uses_default_chain() {
        let opts: HashMap<AmazonS3ConfigKey, String> = HashMap::new();

        let result = build_aws_credential(
            Duration::from_secs(300),
            None,
            Some(&opts),
            Some("us-east-1".to_string()),
            None,
            None,
        )
        .await;
        assert!(result.is_ok());
    }
}
