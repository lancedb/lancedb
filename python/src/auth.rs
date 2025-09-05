// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A wrapper around a Python HeaderProvider that can be called from Rust
#[derive(Clone)]
pub struct PyHeaderProvider {
    provider: Arc<RwLock<Option<Py<PyAny>>>>,
}

impl PyHeaderProvider {
    pub fn new(provider: Py<PyAny>) -> Self {
        Self {
            provider: Arc::new(RwLock::new(Some(provider))),
        }
    }

    /// Get headers from the Python provider (internal implementation)
    async fn get_headers_internal(&self) -> Result<HashMap<String, String>, String> {
        let provider_guard = self.provider.read().await;
        let provider = provider_guard
            .as_ref()
            .ok_or_else(|| "HeaderProvider has been dropped".to_string())?;

        Python::with_gil(|py| {
            // Try to call the sync version first for simplicity in Rust context
            let result = provider.call_method0(py, "get_headers_sync");

            match result {
                Ok(headers_py) => {
                    // Convert Python dict to Rust HashMap
                    let bound_headers = headers_py.bind(py);
                    let dict: &Bound<PyDict> = bound_headers.downcast().map_err(|e| {
                        format!("HeaderProvider.get_headers_sync must return a dict: {}", e)
                    })?;

                    let mut headers = HashMap::new();
                    for (key, value) in dict {
                        let key_str: String = key
                            .extract()
                            .map_err(|e| format!("Header key must be string: {}", e))?;
                        let value_str: String = value
                            .extract()
                            .map_err(|e| format!("Header value must be string: {}", e))?;
                        headers.insert(key_str, value_str);
                    }
                    Ok(headers)
                }
                Err(e) => Err(format!("Failed to get headers from provider: {}", e)),
            }
        })
    }
}

#[cfg(feature = "remote")]
#[async_trait]
impl lancedb::remote::HeaderProvider for PyHeaderProvider {
    async fn get_headers(&self) -> lancedb::error::Result<HashMap<String, String>> {
        self.get_headers_internal()
            .await
            .map_err(|e| lancedb::Error::Runtime { message: e })
    }
}

impl std::fmt::Debug for PyHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PyHeaderProvider")
    }
}
