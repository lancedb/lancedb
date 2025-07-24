// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lancedb::{ObjectStoreRegistry, Session as LanceSession};
use napi::bindgen_prelude::*;
use napi_derive::*;

/// A session for managing caches and object stores across LanceDB operations.
///
/// Sessions allow you to configure cache sizes for index and metadata caches,
/// which can significantly impact memory use and performance. They can
/// also be re-used across multiple connections to share the same cache state.
#[napi]
#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Arc<LanceSession>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("size_bytes", &self.inner.size_bytes())
            .field("approx_num_items", &self.inner.approx_num_items())
            .finish()
    }
}

#[napi]
impl Session {
    /// Create a new session with custom cache sizes.
    ///
    /// # Parameters
    ///
    /// - `index_cache_size_bytes`: The size of the index cache in bytes.
    ///   Index data is stored in memory in this cache to speed up queries.
    ///   Defaults to 6GB if not specified.
    /// - `metadata_cache_size_bytes`: The size of the metadata cache in bytes.
    ///   The metadata cache stores file metadata and schema information in memory.
    ///   This cache improves scan and write performance.
    ///   Defaults to 1GB if not specified.
    #[napi(constructor)]
    pub fn new(
        index_cache_size_bytes: Option<BigInt>,
        metadata_cache_size_bytes: Option<BigInt>,
    ) -> napi::Result<Self> {
        let index_cache_size = index_cache_size_bytes
            .map(|size| size.get_u64().1 as usize)
            .unwrap_or(6 * 1024 * 1024 * 1024); // 6GB default

        let metadata_cache_size = metadata_cache_size_bytes
            .map(|size| size.get_u64().1 as usize)
            .unwrap_or(1024 * 1024 * 1024); // 1GB default

        let session = LanceSession::new(
            index_cache_size,
            metadata_cache_size,
            Arc::new(ObjectStoreRegistry::default()),
        );

        Ok(Self {
            inner: Arc::new(session),
        })
    }

    /// Create a session with default cache sizes.
    ///
    /// This is equivalent to creating a session with 6GB index cache
    /// and 1GB metadata cache.
    #[napi(factory)]
    pub fn default() -> Self {
        Self {
            inner: Arc::new(LanceSession::default()),
        }
    }

    /// Get the current size of the session caches in bytes.
    #[napi]
    pub fn size_bytes(&self) -> BigInt {
        BigInt::from(self.inner.size_bytes())
    }

    /// Get the approximate number of items cached in the session.
    #[napi]
    pub fn approx_num_items(&self) -> u32 {
        self.inner.approx_num_items() as u32
    }
}

// Implement FromNapiValue for Session to work with napi(object)
impl napi::bindgen_prelude::FromNapiValue for Session {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> napi::Result<Self> {
        let object: napi::bindgen_prelude::ClassInstance<Session> =
            napi::bindgen_prelude::ClassInstance::from_napi_value(env, napi_val)?;
        let copy = object.clone();
        Ok(copy)
    }
}
