// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[derive(Clone)]
pub(super) struct NamespaceHeaderProviderContext {
    pub(super) header_provider: Arc<dyn HeaderProvider>,
}

impl std::fmt::Debug for NamespaceHeaderProviderContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceHeaderProviderContext")
            .field("header_provider", &"Some(...)")
            .finish()
    }
}

impl DynamicContextProvider for NamespaceHeaderProviderContext {
    fn provide_context(&self, _info: &OperationInfo) -> HashMap<String, String> {
        let header_provider = Arc::clone(&self.header_provider);
        let handle = match std::thread::Builder::new()
            .name("lancedb-namespace-headers".to_string())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::Runtime {
                        message: format!(
                            "Failed to create runtime for namespace header provider: {e}"
                        ),
                    })?
                    .block_on(header_provider.get_headers())
            }) {
            Ok(handle) => handle,
            Err(err) => {
                log::warn!("Failed to spawn dynamic namespace header provider thread: {err}");
                return HashMap::new();
            }
        };

        let headers = handle.join();

        match headers {
            Ok(Ok(headers)) => headers
                .into_iter()
                .map(|(key, value)| (format!("headers.{key}"), value))
                .collect(),
            Ok(Err(err)) => {
                log::warn!("Failed to get dynamic namespace headers: {err}");
                HashMap::new()
            }
            Err(_) => {
                log::warn!("Dynamic namespace header provider panicked");
                HashMap::new()
            }
        }
    }
}
