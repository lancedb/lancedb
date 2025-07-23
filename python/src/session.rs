// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lancedb::{ObjectStoreRegistry, Session as LanceSession};
use pyo3::{pyclass, pymethods, PyResult};

/// A session for managing caches and object stores across LanceDB operations.
///
/// Sessions allow you to configure cache sizes for index and metadata caches,
/// which can significantly impact performance for large datasets.
#[pyclass]
#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Arc<LanceSession>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            inner: Arc::new(LanceSession::default()),
        }
    }
}

#[pymethods]
impl Session {
    /// Create a new session with custom cache sizes.
    ///
    /// Parameters
    /// ----------
    /// index_cache_size_bytes : int, optional
    ///     The size of the index cache in bytes.
    ///     Index data is stored in memory in this cache to speed up queries.
    ///     Default: 6GB (6 * 1024 * 1024 * 1024 bytes)
    /// metadata_cache_size_bytes : int, optional
    ///     The size of the metadata cache in bytes.
    ///     The metadata cache stores file metadata and schema information in memory.
    ///     This cache improves scan and write performance.
    ///     Default: 1GB (1024 * 1024 * 1024 bytes)
    #[new]
    #[pyo3(signature = (index_cache_size_bytes=None, metadata_cache_size_bytes=None))]
    pub fn new(
        index_cache_size_bytes: Option<usize>,
        metadata_cache_size_bytes: Option<usize>,
    ) -> PyResult<Self> {
        let index_cache_size = index_cache_size_bytes.unwrap_or(6 * 1024 * 1024 * 1024); // 6GB default
        let metadata_cache_size = metadata_cache_size_bytes.unwrap_or(1024 * 1024 * 1024); // 1GB default

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
    ///
    /// Returns
    /// -------
    /// Session
    ///     A new Session with default cache sizes
    #[staticmethod]
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Default::default()
    }

    /// Get the current size of the session caches in bytes.
    ///
    /// Returns
    /// -------
    /// int
    ///     The total size of all caches in the session
    #[getter]
    pub fn size_bytes(&self) -> u64 {
        self.inner.size_bytes()
    }

    /// Get the approximate number of items cached in the session.
    ///
    /// Returns
    /// -------
    /// int
    ///     The number of cached items across all caches
    #[getter]
    pub fn approx_num_items(&self) -> usize {
        self.inner.approx_num_items()
    }

    fn __repr__(&self) -> String {
        format!(
            "Session(size_bytes={}, approx_num_items={})",
            self.size_bytes(),
            self.approx_num_items()
        )
    }
}
