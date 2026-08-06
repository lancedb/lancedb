// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;

use async_trait::async_trait;

use super::StorageOptionsProvider;
use lance_core::Result;

#[derive(Debug)]
pub struct StaticMockStorageOptionsProvider {
    pub options: HashMap<String, String>,
}

#[async_trait]
impl StorageOptionsProvider for StaticMockStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        Ok(Some(self.options.clone()))
    }

    fn provider_id(&self) -> String {
        "StaticMockStorageOptionsProvider".to_string()
    }
}
