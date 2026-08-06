// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Gcs};

use object_store::{
    RetryConfig, StaticCredentialProvider,
    gcp::{GcpCredential, GoogleCloudStorageBuilder, GoogleConfigKey},
};
use url::Url;

use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions, StorageOptionsAccessor,
    dynamic_credentials::build_dynamic_credential_provider,
    throttle::{AimdThrottleConfig, AimdThrottleState, AimdThrottledStore, cloud_http_connector},
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct GcsStoreProvider;

impl GcsStoreProvider {
    async fn build_opendal_gcs_store(
        &self,
        base_path: &Url,
        storage_options: &StorageOptions,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("GCS URL must contain bucket name"))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Start with all storage options as the config map
        // OpenDAL will handle environment variables through its default credentials chain
        let mut config_map: HashMap<String, String> = storage_options.0.clone();

        // Set required OpenDAL configuration
        config_map.insert("bucket".to_string(), bucket);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), format!("/{}", prefix));
        }

        let operator = Operator::from_iter::<Gcs>(config_map)
            .map_err(|e| Error::invalid_input(format!("Failed to create GCS operator: {:?}", e)))?;

        Ok(Arc::new(OpendalStore::new(operator)) as Arc<dyn OSObjectStore>)
    }

    async fn build_google_cloud_store(
        &self,
        base_path: &Url,
        storage_options: &StorageOptions,
        accessor: Option<Arc<StorageOptionsAccessor>>,
        throttle_state: Option<&AimdThrottleState>,
    ) -> Result<Arc<dyn OSObjectStore>> {
        // Use a low retry count since the AIMD throttle layer handles
        // throttle recovery with its own retry loop.
        let retry_config = RetryConfig {
            backoff: Default::default(),
            max_retries: storage_options.client_max_retries(),
            retry_timeout: Duration::from_secs(storage_options.client_retry_timeout()),
        };

        let mut builder = GoogleCloudStorageBuilder::new()
            .with_url(base_path.as_ref())
            .with_retry(retry_config)
            .with_client_options(storage_options.client_options()?);
        for (key, value) in storage_options.as_gcs_options() {
            builder = builder.with_config(key, value);
        }

        if let Some(credentials) =
            build_dynamic_credential_provider::<GcpCredential>(accessor).await?
        {
            builder = builder.with_credentials(credentials);
        } else if let Some(storage_token) = storage_options.get("google_storage_token") {
            let credential = GcpCredential {
                bearer: storage_token.clone(),
            };
            let credential_provider = Arc::new(StaticCredentialProvider::new(credential)) as _;
            builder = builder.with_credentials(credential_provider);
        }

        let store_prefix =
            self.calculate_object_store_prefix(base_path, Some(&storage_options.0))?;
        builder = builder.with_http_connector(cloud_http_connector(throttle_state, store_prefix));

        Ok(Arc::new(builder.build()?) as Arc<dyn OSObjectStore>)
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for GcsStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let mut storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());
        storage_options.with_env_gcs();
        let download_retry_count = storage_options.download_retry_count();

        let use_opendal = storage_options
            .0
            .get("use_opendal")
            .map(|v| v.as_str() == "true")
            .unwrap_or(false);

        let accessor = params.get_accessor();

        let throttle_config = AimdThrottleConfig::from_storage_options(params.storage_options())?;
        let throttle_state = if throttle_config.is_disabled() {
            None
        } else {
            Some(AimdThrottleState::new(throttle_config)?)
        };

        let inner = if use_opendal {
            // OpenDAL GCS intentionally uses static/environment-backed configuration only.
            // Namespace-vended dynamic credentials are supported on the native object_store path.
            self.build_opendal_gcs_store(&base_path, &storage_options)
                .await?
        } else {
            self.build_google_cloud_store(
                &base_path,
                &storage_options,
                accessor,
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
            scheme: String::from("gs"),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: true,
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }
}

impl StorageOptions {
    /// Add values from the environment to storage options
    pub fn with_env_gcs(&mut self) {
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                let lowercase_key = key.to_ascii_lowercase();
                let token_key = "google_storage_token";

                if let Ok(config_key) = GoogleConfigKey::from_str(&lowercase_key) {
                    if !self.0.contains_key(config_key.as_ref()) {
                        self.0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
                // Check for GOOGLE_STORAGE_TOKEN until GoogleConfigKey supports storage token
                else if lowercase_key == token_key && !self.0.contains_key(token_key) {
                    self.0.insert(token_key.to_string(), value.to_string());
                }
            }
        }
    }

    /// Subset of options relevant for gcs storage
    pub fn as_gcs_options(&self) -> HashMap<GoogleConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let gcs_key = GoogleConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((gcs_key, value.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::object_store::test_utils::StaticMockStorageOptionsProvider;
    use crate::object_store::{ObjectStoreParams, StorageOptionsAccessor};
    use std::collections::HashMap;

    #[test]
    fn test_gcs_store_path() {
        let provider = GcsStoreProvider;

        let url = Url::parse("gs://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_use_opendal_flag() {
        let provider = GcsStoreProvider;
        let url = Url::parse("gs://test-bucket/path").unwrap();
        let params_with_flag = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    (
                        "service_account".to_string(),
                        "test@example.iam.gserviceaccount.com".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider
            .new_store(url.clone(), &params_with_flag)
            .await
            .unwrap();
        assert_eq!(store.scheme, "gs");
    }

    #[tokio::test]
    async fn test_dynamic_gcp_credentials_provider() {
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::from([(
                    "google_storage_token".to_string(),
                    "gcp-token".to_string(),
                )]),
            },
        )));

        let credentials = build_dynamic_credential_provider::<GcpCredential>(Some(accessor))
            .await
            .expect("dynamic gcp credentials should build")
            .expect("expected credential provider")
            .get_credential()
            .await
            .expect("expected gcp credential");

        assert_eq!(credentials.bearer, "gcp-token");
    }
}
