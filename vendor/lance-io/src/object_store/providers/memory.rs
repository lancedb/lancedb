// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, sync::Arc};

use crate::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_LOCAL_BLOCK_SIZE, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::Result;
use object_store::{memory::InMemory, path::Path};
use url::Url;

/// Provides a fresh in-memory object store for each call to `new_store`.
#[derive(Default, Debug)]
pub struct MemoryStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for MemoryStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_LOCAL_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();
        Ok(ObjectStore {
            inner: Arc::new(InMemory::new()),
            scheme: String::from("memory"),
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

    fn extract_path(&self, url: &Url) -> Result<Path> {
        let mut output = String::new();
        if let Some(domain) = url.domain() {
            output.push_str(domain);
        }
        output.push_str(url.path());
        // The in-memory store uses the Path directly as a key with no HTTP layer,
        // so there is no re-encoding step and thus no double-encoding to avoid.
        // Path::from also tolerates the empty segments that local temp paths embed.
        Ok(Path::from(output))
    }

    fn calculate_object_store_prefix(
        &self,
        _url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Ok("memory".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_store_path() {
        let provider = MemoryStoreProvider;

        let url = Url::parse("memory://path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_calculate_object_store_prefix() {
        let provider = MemoryStoreProvider;
        assert_eq!(
            "memory",
            provider
                .calculate_object_store_prefix(&Url::parse("memory://etc").unwrap(), None)
                .unwrap()
        );
    }
}
