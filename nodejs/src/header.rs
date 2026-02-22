// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction};
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;

type GetHeadersFn = ThreadsafeFunction<(), Promise<HashMap<String, String>>, (), Status, false>;

/// JavaScript HeaderProvider implementation that wraps a JavaScript callback.
/// This is the only native header provider - all header provider implementations
/// should provide a JavaScript function that returns headers.
#[napi]
pub struct JsHeaderProvider {
    get_headers_fn: Arc<GetHeadersFn>,
}

impl Clone for JsHeaderProvider {
    fn clone(&self) -> Self {
        Self {
            get_headers_fn: self.get_headers_fn.clone(),
        }
    }
}

#[napi]
impl JsHeaderProvider {
    /// Create a new JsHeaderProvider from a JavaScript callback
    #[napi(constructor)]
    pub fn new(
        get_headers_callback: Function<(), Promise<HashMap<String, String>>>,
    ) -> Result<Self> {
        let get_headers_fn = get_headers_callback
            .build_threadsafe_function()
            .build()
            .map_err(|e| {
                Error::new(
                    Status::GenericFailure,
                    format!("Failed to create threadsafe function: {}", e),
                )
            })?;

        Ok(Self {
            get_headers_fn: Arc::new(get_headers_fn),
        })
    }
}

#[cfg(feature = "remote")]
#[async_trait::async_trait]
impl lancedb::remote::HeaderProvider for JsHeaderProvider {
    async fn get_headers(&self) -> lancedb::error::Result<HashMap<String, String>> {
        // Call the JavaScript function asynchronously
        let promise: Promise<HashMap<String, String>> =
            self.get_headers_fn.call_async(()).await.map_err(|e| {
                lancedb::error::Error::Runtime {
                    message: format!("Failed to call JavaScript get_headers: {}", e),
                }
            })?;

        // Await the promise result
        promise.await.map_err(|e| lancedb::error::Error::Runtime {
            message: format!("JavaScript get_headers failed: {}", e),
        })
    }
}

impl std::fmt::Debug for JsHeaderProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JsHeaderProvider")
    }
}
