// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore as OSObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Oss};
use url::Url;

use crate::object_store::dynamic_opendal::DynamicOpenDalStore;
use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct OssStoreProvider;

impl OssStoreProvider {
    fn base_oss_options(
        base_path: &Url,
        storage_options: &StorageOptions,
    ) -> Result<HashMap<String, String>> {
        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("OSS URL must contain bucket name"))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Snapshot env-backed OSS defaults at store construction time. Dynamic provider
        // options can still override these values during per-request config merging.
        let mut config_map: HashMap<String, String> = std::env::vars()
            .filter(|(key, _)| {
                key.starts_with("OSS_")
                    || key.starts_with("AWS_")
                    || key.starts_with("ALIBABA_CLOUD_")
            })
            .map(|(key, value)| {
                let normalized_key = key
                    .to_lowercase()
                    .replace("oss_", "")
                    .replace("aws_", "")
                    .replace("alibaba_cloud_", "");
                (normalized_key, value)
            })
            .collect();

        config_map.extend(storage_options.0.clone());

        config_map.insert("bucket".to_string(), bucket);
        if prefix.is_empty() {
            config_map.remove("root");
        } else {
            config_map.insert("root".to_string(), "/".to_string());
        }

        Ok(config_map)
    }

    /// Normalize OSS storage options, resolving aliases for well-known keys
    /// while passing through all other options (e.g. `role_arn`,
    /// `sts_endpoint`, `allow_anonymous`, etc.) so that OpenDAL can use them.
    fn normalize_oss_config(options: &HashMap<String, String>) -> Result<HashMap<String, String>> {
        let mut config_map = options.clone();

        let alias_groups: &[(&str, &[&str])] = &[
            ("endpoint", &["oss_endpoint"]),
            ("access_key_id", &["oss_access_key_id"]),
            ("access_key_secret", &["oss_secret_access_key"]),
            ("region", &["oss_region"]),
            ("security_token", &["oss_security_token"]),
        ];

        for (canonical, aliases) in alias_groups {
            for alias in *aliases {
                if let Some(value) = config_map.remove(*alias) {
                    config_map.insert(canonical.to_string(), value);
                    break;
                }
            }
        }

        if !config_map.contains_key("endpoint") {
            return Err(Error::invalid_input(
                "OSS endpoint is required. Please provide 'oss_endpoint' in storage options or set OSS_ENDPOINT environment variable",
            ));
        }

        Ok(config_map)
    }

    fn build_oss_store(config_map: HashMap<String, String>) -> Result<OpendalStore> {
        let operator = Operator::from_iter::<Oss>(config_map)
            .map_err(|e| Error::invalid_input(format!("Failed to create OSS operator: {:?}", e)))?;

        Ok(OpendalStore::new(operator))
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for OssStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());

        let base_options = Self::base_oss_options(&base_path, &storage_options)?;
        let accessor = params.get_accessor();

        let inner: Arc<dyn OSObjectStore> =
            if let Some(accessor) = accessor.filter(|a| a.has_provider()) {
                Arc::new(
                    DynamicOpenDalStore::new(
                        format!("oss:{}", base_path),
                        base_options,
                        accessor,
                        Self::normalize_oss_config,
                        Self::build_oss_store,
                    )
                    .with_protected_keys(["bucket", "root"]),
                )
            } else {
                Arc::new(Self::build_oss_store(Self::normalize_oss_config(
                    &base_options,
                )?)?)
            };

        let mut url = base_path;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        Ok(ObjectStore {
            scheme: "oss".to_string(),
            inner,
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: params.use_constant_size_upload_parts,
            list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or(true),
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count: storage_options.download_retry_count(),
            io_tracker: Default::default(),
            store_prefix: self.calculate_object_store_prefix(&url, params.storage_options())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::OssStoreProvider;
    use crate::object_store::dynamic_opendal::DynamicOpenDalStore;
    use crate::object_store::test_utils::StaticMockStorageOptionsProvider;
    use crate::object_store::{ObjectStoreProvider, StorageOptionsAccessor};
    use url::Url;

    #[test]
    fn test_oss_store_path() {
        let provider = OssStoreProvider;

        let url = Url::parse("oss://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_oss_alias_options_override_canonical_env_options() {
        let config = OssStoreProvider::normalize_oss_config(&HashMap::from([
            (
                "endpoint".to_string(),
                "https://env.example.com".to_string(),
            ),
            (
                "oss_endpoint".to_string(),
                "https://user.example.com".to_string(),
            ),
            ("access_key_id".to_string(), "env-akid".to_string()),
            ("oss_access_key_id".to_string(), "user-akid".to_string()),
            ("access_key_secret".to_string(), "env-secret".to_string()),
            (
                "oss_secret_access_key".to_string(),
                "user-secret".to_string(),
            ),
            ("region".to_string(), "env-region".to_string()),
            ("oss_region".to_string(), "user-region".to_string()),
            ("security_token".to_string(), "env-token".to_string()),
            ("oss_security_token".to_string(), "user-token".to_string()),
            ("bucket".to_string(), "bucket".to_string()),
        ]))
        .unwrap();

        assert_eq!(config.get("endpoint").unwrap(), "https://user.example.com");
        assert_eq!(config.get("access_key_id").unwrap(), "user-akid");
        assert_eq!(config.get("access_key_secret").unwrap(), "user-secret");
        assert_eq!(config.get("region").unwrap(), "user-region");
        assert_eq!(config.get("security_token").unwrap(), "user-token");
        assert!(!config.contains_key("oss_endpoint"));
        assert!(!config.contains_key("oss_security_token"));
    }

    #[test]
    fn test_oss_url_bucket_and_root_are_authoritative() {
        let storage_options = crate::object_store::StorageOptions(HashMap::from([
            (
                "oss_endpoint".to_string(),
                "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
            ),
            ("bucket".to_string(), "storage-options-bucket".to_string()),
            ("root".to_string(), "/storage-options-root".to_string()),
        ]));
        let base_options = OssStoreProvider::base_oss_options(
            &Url::parse("oss://url-bucket/path").unwrap(),
            &storage_options,
        )
        .unwrap();
        let config = OssStoreProvider::normalize_oss_config(&base_options).unwrap();

        assert_eq!(config.get("bucket").unwrap(), "url-bucket");
        assert_eq!(config.get("root").unwrap(), "/");
    }

    #[test]
    fn test_oss_empty_url_path_removes_storage_option_root() {
        let storage_options = crate::object_store::StorageOptions(HashMap::from([
            (
                "oss_endpoint".to_string(),
                "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
            ),
            ("root".to_string(), "/storage-options-root".to_string()),
        ]));
        let base_options = OssStoreProvider::base_oss_options(
            &Url::parse("oss://url-bucket").unwrap(),
            &storage_options,
        )
        .unwrap();
        let config = OssStoreProvider::normalize_oss_config(&base_options).unwrap();

        assert_eq!(config.get("bucket").unwrap(), "url-bucket");
        assert!(!config.contains_key("root"));
    }

    #[tokio::test]
    async fn test_dynamic_opendal_oss_store_uses_provider_credentials() {
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::from([
                    (
                        "oss_endpoint".to_string(),
                        "https://oss-cn-hangzhou.aliyuncs.com".to_string(),
                    ),
                    ("oss_access_key_id".to_string(), "akid".to_string()),
                    ("oss_secret_access_key".to_string(), "secret".to_string()),
                    ("oss_security_token".to_string(), "token".to_string()),
                ]),
            },
        )));

        let base_options = OssStoreProvider::base_oss_options(
            &Url::parse("oss://bucket/path").unwrap(),
            &crate::object_store::StorageOptions(HashMap::new()),
        )
        .unwrap();

        let store = DynamicOpenDalStore::new(
            "oss",
            base_options,
            accessor,
            OssStoreProvider::normalize_oss_config,
            OssStoreProvider::build_oss_store,
        );

        let current_store = store
            .current_store()
            .await
            .expect("dynamic OpenDAL OSS store should build");

        assert!(current_store.to_string().contains("Opendal"));
    }
}
