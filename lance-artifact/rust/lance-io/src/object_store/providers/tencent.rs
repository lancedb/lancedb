// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::{Operator, services::Cos};
use url::Url;

use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct TencentStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for TencentStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());

        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("Tencent Cos URL must contain bucket name"))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Start with environment variables as base configuration
        let mut config_map: HashMap<String, String> = std::env::vars()
            .filter(|(k, _)| k.starts_with("COS_") || k.starts_with("TENCENTCLOUD_"))
            .map(|(k, v)| {
                // Convert env var names to opendal config keys
                let key = k
                    .to_lowercase()
                    .replace("cos_", "")
                    .replace("tencentcloud_", "");
                (key, v)
            })
            .collect();

        config_map.insert("bucket".to_string(), bucket);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), "/".to_string());
        }

        // Override with storage options if provided
        if let Some(endpoint) = storage_options.0.get("cos_endpoint") {
            config_map.insert("endpoint".to_string(), endpoint.clone());
        }

        if let Some(secret_id) = storage_options.0.get("cos_secret_id") {
            config_map.insert("secret_id".to_string(), secret_id.clone());
        }

        if let Some(secret_key) = storage_options.0.get("cos_secret_key") {
            config_map.insert("secret_key".to_string(), secret_key.clone());
        }

        if let Some(enable_versioning) = storage_options.0.get("cos_enable_versioning") {
            config_map.insert("enable_versioning".to_string(), enable_versioning.clone());
        }

        // Currently, the configuration options for CosConfig in OpenDAL are very limited.
        // Most configurations need to be entered via environment variables, such as TENCENTCLOUD_SECURITY_TOKEN, TENCENTCLOUD_REGION, etc.
        // (more env config details: https://github.com/apache/opendal-reqsign/blob/v0.16.5/src/tencent/config.rs)
        // Therefore, we need to keep `disable_config_load` always false to allow configurations to be loaded from environment variables.
        // TODO: improve CosConfig in opendal and add more storage_option here
        config_map.insert("disable_config_load".to_string(), "false".to_string());

        if !config_map.contains_key("endpoint") {
            return Err(Error::invalid_input(
                "COS endpoint is required. Please provide 'cos_endpoint' in storage options or set COS_ENDPOINT environment variable",
            ));
        }

        let operator = Operator::from_iter::<Cos>(config_map)
            .map_err(|e| Error::invalid_input(format!("Failed to create COS operator: {:?}", e)))?;

        let opendal_store = Arc::new(OpendalStore::new(operator));

        let mut url = base_path;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        Ok(ObjectStore {
            scheme: "cos".to_string(),
            inner: opendal_store,
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
    use super::TencentStoreProvider;
    use crate::object_store::ObjectStoreProvider;
    use url::Url;

    #[test]
    fn test_cos_store_path() {
        let provider = TencentStoreProvider;

        let url = Url::parse("cos://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }
}
