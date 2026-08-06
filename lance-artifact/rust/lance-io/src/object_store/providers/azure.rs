// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Azblob, services::Azdls};

use object_store::{
    RetryConfig,
    azure::{AzureConfigKey, AzureCredential, MicrosoftAzureBuilder},
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
pub struct AzureBlobStoreProvider;

impl AzureBlobStoreProvider {
    /// Normalize Azure storage options for OpenDAL, resolving aliases for
    /// well-known keys while passing through all other options (e.g.
    /// `client_id`, `tenant_id`, `encryption_key`, etc.) so that OpenDAL
    /// can use them directly.
    fn normalize_opendal_azure_options(
        options: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        // Start with all options so unknown keys are forwarded to OpenDAL.
        let mut config_map = options.clone();

        // Normalize well-known aliases into canonical OpenDAL key names.
        // Remove the alias after resolving to avoid duplicate/conflicting entries.
        let alias_groups: &[(&str, &[&str])] = &[
            ("account_name", &["azure_storage_account_name"]),
            ("endpoint", &["azure_storage_endpoint", "azure_endpoint"]),
            (
                "account_key",
                &[
                    "azure_storage_account_key",
                    "azure_storage_access_key",
                    "azure_storage_master_key",
                    "access_key",
                    "master_key",
                ],
            ),
            (
                "sas_token",
                &[
                    "azure_storage_sas_token",
                    "azure_storage_sas_key",
                    "sas_key",
                ],
            ),
        ];

        for (canonical, aliases) in alias_groups {
            if !config_map.contains_key(*canonical) {
                for alias in *aliases {
                    if let Some(value) = config_map.remove(*alias) {
                        config_map.insert(canonical.to_string(), value);
                        break;
                    }
                }
            } else {
                // Canonical key exists; remove aliases to avoid conflicts.
                for alias in *aliases {
                    config_map.remove(*alias);
                }
            }
        }

        config_map
    }

    fn build_opendal_operator(
        base_path: &Url,
        storage_options: &StorageOptions,
    ) -> Result<Operator> {
        // Start with all storage options as the config map
        // OpenDAL will handle environment variables through its default credentials chain
        let mut config_map = Self::normalize_opendal_azure_options(&storage_options.0);

        match base_path.scheme() {
            "az" => {
                let container = base_path
                    .host_str()
                    .ok_or_else(|| Error::invalid_input("Azure URL must contain container name"))?
                    .to_string();

                config_map.insert("container".to_string(), container);

                let prefix = base_path.path().trim_start_matches('/');
                if !prefix.is_empty() {
                    config_map.insert("root".to_string(), format!("/{}", prefix));
                }

                Operator::from_iter::<Azblob>(config_map).map_err(|e| {
                    Error::invalid_input(format!("Failed to create Azure Blob operator: {:?}", e))
                })
            }
            "abfss" => {
                let filesystem = base_path.username();
                if filesystem.is_empty() {
                    return Err(Error::invalid_input(
                        "abfss:// URL must include account: abfss://<filesystem>@<account>.dfs.core.windows.net/path",
                    ));
                }
                let host = base_path.host_str().ok_or_else(|| {
                    Error::invalid_input(
                        "abfss:// URL must include account: abfss://<filesystem>@<account>.dfs.core.windows.net/path"
                    )
                })?;

                config_map.insert("filesystem".to_string(), filesystem.to_string());
                config_map.insert("endpoint".to_string(), format!("https://{}", host));
                config_map
                    .entry("account_name".to_string())
                    .or_insert_with(|| host.split('.').next().unwrap_or(host).to_string());

                let root_path = base_path.path().trim_start_matches('/');
                if !root_path.is_empty() {
                    config_map.insert("root".to_string(), format!("/{}", root_path));
                }

                Operator::from_iter::<Azdls>(config_map).map_err(|e| {
                    Error::invalid_input(format!(
                        "Failed to create Azure DFS (ADLS Gen2) operator: {:?}",
                        e
                    ))
                })
            }
            _ => Err(Error::invalid_input(format!(
                "Unsupported Azure scheme: {}",
                base_path.scheme()
            ))),
        }
    }

    async fn build_opendal_azure_store(
        &self,
        base_path: &Url,
        storage_options: &StorageOptions,
    ) -> Result<Arc<dyn OSObjectStore>> {
        let operator = Self::build_opendal_operator(base_path, storage_options)?;
        Ok(Arc::new(OpendalStore::new(operator)))
    }

    async fn build_microsoft_azure_store(
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

        let mut builder = MicrosoftAzureBuilder::new()
            .with_url(base_path.as_ref())
            .with_retry(retry_config)
            .with_client_options(storage_options.client_options()?);
        for (key, value) in storage_options.as_azure_options() {
            builder = builder.with_config(key, value);
        }

        if let Some(credentials) =
            build_dynamic_credential_provider::<AzureCredential>(accessor).await?
        {
            builder = builder.with_credentials(credentials);
        }

        let store_prefix =
            self.calculate_object_store_prefix(base_path, Some(&storage_options.0))?;
        builder = builder.with_http_connector(cloud_http_connector(throttle_state, store_prefix));

        Ok(Arc::new(builder.build()?) as Arc<dyn OSObjectStore>)
    }

    fn calculate_object_store_prefix_with_env(
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
        env_options: &HashMap<String, String>,
    ) -> Result<String> {
        let authority = url.authority();
        let (container, account) = match authority.find("@") {
            Some(at_index) => {
                // The URI has an:
                // - az:// schema type and is similar to 'az://container@account.dfs.core.windows.net/path-part/file
                //         or possibly 'az://container@account/path-part/file' (the short version).
                // - abfss:// schema type and is similar to 'abfss://filesystem@account.dfs.core.windows.net/path-part/file'.
                let container = &authority[..at_index];
                let account = &authority[at_index + 1..];
                (
                    container,
                    account.split(".").next().unwrap_or_default().to_string(),
                )
            }
            None => {
                // The URI looks like 'az://container/path-part/file'.
                // We must look at the storage options to find the account.
                let mut account = match storage_options {
                    Some(opts) => StorageOptions::find_configured_storage_account(opts),
                    None => None,
                };
                if account.is_none() {
                    account = StorageOptions::find_configured_storage_account(env_options);
                }
                let account = account.ok_or(Error::invalid_input("Unable to find object store prefix: no Azure account name in URI, and no storage account configured."))?;
                (authority, account)
            }
        };
        Ok(format!("{}${}@{}", url.scheme(), container, account))
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for AzureBlobStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let scheme = base_path.scheme().to_string();
        if scheme != "az" && scheme != "abfss" {
            return Err(Error::invalid_input(format!(
                "Unsupported Azure scheme '{}', expected 'az' or 'abfss'",
                scheme
            )));
        }

        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let mut storage_options =
            StorageOptions::new(params.storage_options().cloned().unwrap_or_default());
        storage_options.with_env_azure();
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

        let inner: Arc<dyn OSObjectStore> = if use_opendal {
            // OpenDAL Azure intentionally uses static/environment-backed configuration only.
            // Namespace-vended dynamic credentials are supported on the native object_store path.
            self.build_opendal_azure_store(&base_path, &storage_options)
                .await?
        } else {
            self.build_microsoft_azure_store(
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
            scheme,
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

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Self::calculate_object_store_prefix_with_env(url, storage_options, &ENV_OPTIONS.0)
    }
}

static ENV_OPTIONS: LazyLock<StorageOptions> = LazyLock::new(StorageOptions::from_env);

impl StorageOptions {
    /// Iterate over all environment variables, looking for anything related to Azure.
    fn from_env() -> Self {
        let mut opts = HashMap::<String, String>::new();
        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str())
                && let Ok(config_key) = AzureConfigKey::from_str(&key.to_ascii_lowercase())
            {
                opts.insert(config_key.as_ref().to_string(), value.to_string());
            }
        }
        Self(opts)
    }

    /// Add values from the environment to storage options
    pub fn with_env_azure(&mut self) {
        for (os_key, os_value) in &ENV_OPTIONS.0 {
            if !self.0.contains_key(os_key) {
                self.0.insert(os_key.clone(), os_value.clone());
            }
        }
    }

    /// Subset of options relevant for azure storage
    pub fn as_azure_options(&self) -> HashMap<AzureConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                let az_key = AzureConfigKey::from_str(&key.to_ascii_lowercase()).ok()?;
                Some((az_key, value.clone()))
            })
            .collect()
    }

    #[allow(clippy::manual_map)]
    fn find_configured_storage_account(map: &HashMap<String, String>) -> Option<String> {
        if let Some(account) = map.get("azure_storage_account_name") {
            Some(account.clone())
        } else if let Some(account) = map.get("account_name") {
            Some(account.clone())
        } else {
            None
        }
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
    fn test_azure_store_path() {
        let provider = AzureBlobStoreProvider;

        let url = Url::parse("az://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[tokio::test]
    async fn test_use_opendal_flag() {
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("az://test-container/path").unwrap();
        let params_with_flag = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    ("account_name".to_string(), "test_account".to_string()),
                    (
                        "endpoint".to_string(),
                        "https://test_account.blob.core.windows.net".to_string(),
                    ),
                    (
                        "account_key".to_string(),
                        "dGVzdF9hY2NvdW50X2tleQ==".to_string(),
                    ),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider
            .new_store(url.clone(), &params_with_flag)
            .await
            .unwrap();
        assert_eq!(store.scheme, "az");
        let inner_desc = store.inner.to_string();
        assert!(
            inner_desc.contains("Opendal") && inner_desc.contains("azblob"),
            "az:// with use_opendal=true should use OpenDAL Azblob, got: {}",
            inner_desc
        );
    }

    #[tokio::test]
    async fn test_dynamic_azure_credentials_provider() {
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::from([(
                    "azure_storage_sas_token".to_string(),
                    "?sv=2022-11-02&sp=rl&sig=test".to_string(),
                )]),
            },
        )));

        let credentials = build_dynamic_credential_provider::<AzureCredential>(Some(accessor))
            .await
            .expect("dynamic azure credentials should build")
            .expect("expected credential provider")
            .get_credential()
            .await
            .expect("expected azure credential");

        match credentials.as_ref() {
            AzureCredential::SASToken(pairs) => {
                assert!(
                    pairs
                        .iter()
                        .any(|(key, value)| key == "sig" && value == "test")
                );
            }
            other => panic!("expected SAS token, got {other:?}"),
        }
    }

    #[test]
    fn test_find_configured_storage_account() {
        assert_eq!(
            Some("myaccount".to_string()),
            StorageOptions::find_configured_storage_account(&HashMap::from_iter(
                [
                    ("access_key".to_string(), "myaccesskey".to_string()),
                    (
                        "azure_storage_account_name".to_string(),
                        "myaccount".to_string()
                    )
                ]
                .into_iter()
            ))
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_from_url_and_options() {
        let provider = AzureBlobStoreProvider;
        let options = HashMap::from_iter([("account_name".to_string(), "bob".to_string())]);
        assert_eq!(
            "az$container@bob",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("az://container/path").unwrap(),
                    Some(&options)
                )
                .unwrap()
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_from_url_and_ignored_options() {
        let provider = AzureBlobStoreProvider;
        let options = HashMap::from_iter([("account_name".to_string(), "bob".to_string())]);
        assert_eq!(
            "az$container@account",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("az://container@account.dfs.core.windows.net/path").unwrap(),
                    Some(&options)
                )
                .unwrap()
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_from_url_short_account() {
        let provider = AzureBlobStoreProvider;
        let options = HashMap::from_iter([("account_name".to_string(), "bob".to_string())]);
        assert_eq!(
            "az$container@account",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("az://container@account/path").unwrap(),
                    Some(&options)
                )
                .unwrap()
        );
    }

    #[test]
    fn test_fail_to_calculate_object_store_prefix_from_url() {
        let options = HashMap::from_iter([("access_key".to_string(), "myaccesskey".to_string())]);
        let expected = "Invalid user input: Unable to find object store prefix: no Azure account name in URI, and no storage account configured.";
        let result = AzureBlobStoreProvider::calculate_object_store_prefix_with_env(
            &Url::parse("az://container/path").unwrap(),
            Some(&options),
            &HashMap::new(),
        )
        .expect_err("expected error")
        .to_string();
        assert_eq!(expected, &result[..expected.len()]);
    }

    // --- abfss:// tests ---

    #[test]
    fn test_abfss_extract_path() {
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("abfss://myfs@myaccount.dfs.core.windows.net/path/to/dataset.lance")
            .unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(
            path,
            object_store::path::Path::from("path/to/dataset.lance")
        );
    }

    #[test]
    fn test_calculate_abfss_prefix() {
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("abfss://myfs@myaccount.dfs.core.windows.net/path/to/data").unwrap();
        let prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        assert_eq!(prefix, "abfss$myfs@myaccount");
    }

    #[test]
    fn test_calculate_abfss_prefix_ignores_storage_options() {
        let provider = AzureBlobStoreProvider;
        let options =
            HashMap::from_iter([("account_name".to_string(), "other_account".to_string())]);
        let url = Url::parse("abfss://myfs@myaccount.dfs.core.windows.net/path").unwrap();
        let prefix = provider
            .calculate_object_store_prefix(&url, Some(&options))
            .unwrap();
        assert_eq!(prefix, "abfss$myfs@myaccount");
    }

    #[tokio::test]
    async fn test_abfss_default_uses_microsoft_builder() {
        use crate::object_store::StorageOptionsAccessor;
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("abfss://testfs@testaccount.dfs.core.windows.net/data").unwrap();
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("account_name".to_string(), "testaccount".to_string()),
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider.new_store(url, &params).await.unwrap();
        assert_eq!(store.scheme, "abfss");
        assert!(!store.is_local());
        assert!(store.is_cloud());
        let inner_desc = store.inner.to_string();
        assert!(
            inner_desc.contains("MicrosoftAzure"),
            "abfss:// without use_opendal should use MicrosoftAzureBuilder, got: {}",
            inner_desc
        );
    }

    #[tokio::test]
    async fn test_unsupported_scheme_rejected() {
        use crate::object_store::StorageOptionsAccessor;
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("wasbs://container@myaccount.blob.core.windows.net/path").unwrap();
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("account_name".to_string(), "myaccount".to_string()),
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                ]),
            ))),
            ..Default::default()
        };

        let err = provider
            .new_store(url, &params)
            .await
            .expect_err("expected error for unsupported scheme");
        assert!(
            err.to_string().contains("Unsupported Azure scheme"),
            "unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_abfss_with_opendal_uses_azdls() {
        use crate::object_store::StorageOptionsAccessor;
        let provider = AzureBlobStoreProvider;
        let url = Url::parse("abfss://testfs@testaccount.dfs.core.windows.net/data").unwrap();
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("use_opendal".to_string(), "true".to_string()),
                    ("account_name".to_string(), "testaccount".to_string()),
                    ("account_key".to_string(), "dGVzdA==".to_string()),
                ]),
            ))),
            ..Default::default()
        };

        let store = provider.new_store(url, &params).await.unwrap();
        assert_eq!(store.scheme, "abfss");
        assert!(!store.is_local());
        assert!(store.is_cloud());
        let inner_desc = store.inner.to_string();
        assert!(
            inner_desc.contains("Opendal") && inner_desc.contains("azdls"),
            "abfss:// with use_opendal=true should use OpenDAL Azdls, got: {}",
            inner_desc
        );
    }

    #[test]
    fn test_azdls_capabilities_differ_from_azblob() {
        let common_opts = StorageOptions(HashMap::from([
            ("account_name".to_string(), "testaccount".to_string()),
            ("account_key".to_string(), "dGVzdA==".to_string()),
            (
                "endpoint".to_string(),
                "https://testaccount.blob.core.windows.net".to_string(),
            ),
        ]));

        // Build az:// operator (uses Azblob backend)
        let az_url = Url::parse("az://test-container/path").unwrap();
        let az_operator =
            AzureBlobStoreProvider::build_opendal_operator(&az_url, &common_opts).unwrap();

        // Build abfss:// operator (uses Azdls backend)
        let abfss_url = Url::parse("abfss://testfs@testaccount.dfs.core.windows.net/data").unwrap();
        let abfss_operator =
            AzureBlobStoreProvider::build_opendal_operator(&abfss_url, &common_opts).unwrap();

        let azblob_cap = az_operator.info().capability();
        let azdls_cap = abfss_operator.info().capability();

        // Both support basic operations
        assert!(azblob_cap.read);
        assert!(azdls_cap.read);
        assert!(azblob_cap.write);
        assert!(azdls_cap.write);
        assert!(azblob_cap.list);
        assert!(azdls_cap.list);

        // Azdls supports rename and create_dir (HNS features); Azblob does not
        assert!(azdls_cap.rename, "Azdls should support rename");
        assert!(azdls_cap.create_dir, "Azdls should support create_dir");
        assert!(!azblob_cap.rename, "Azblob should not support rename");
    }
}
