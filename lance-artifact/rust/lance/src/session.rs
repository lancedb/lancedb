// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use lance_core::cache::{CacheBackend, LanceCache, QuickCacheBackend};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use lance_index::IndexType;
use lance_io::object_store::ObjectStoreRegistry;
use lance_io::spill::{LocalSpillStore, SpillStore};

use crate::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use crate::session::caches::GlobalMetadataCache;
use crate::session::index_caches::GlobalIndexCache;

use self::index_extension::IndexExtension;

pub(crate) mod caches;
pub mod index_caches;
pub(crate) mod index_extension;

/// Cache selection for one session cache tier.
#[derive(Clone, Debug)]
pub enum CacheSpec {
    /// Use the Lance-level default capacity for the tier.
    Default,
    /// Use the default in-memory backend with this capacity.
    Size(usize),
    /// Use an already constructed backend.
    Backend(Arc<dyn CacheBackend>),
}

/// A user session holds the runtime state for a [`crate::Dataset`]
///
/// A session will be created automatically when a Dataset is opened.  However, you
/// can manually create the session and provide it to the Dataset builder in order
/// to share runtime state between multiple datasets.
///
/// This can be used to share caches between multiple datasets, increasing the hit
/// rate and reducing the amount of memory used.
///
/// A session contains two different caches:
///  - The index cache is used to cache opened indices and will cache index data
///  - The metadata cache is used to cache a variety of dataset metadata (more
///    details can be found in the [performance guide](https://lance.org/guide/performance/)
#[derive(Clone)]
pub struct Session {
    /// Global cache for opened indices.
    ///
    /// Sub-caches are created from this cache for each dataset by adding the
    /// URI and index UUID as a key prefix. If there is a fragment re-use index,
    /// that is also in the key prefix. This prevents collisions between different
    /// datasets and indices.
    pub(crate) index_cache: GlobalIndexCache,

    /// Global cache for file metadata.
    ///
    /// Sub-caches are created from this cache for each dataset by adding the
    /// URI as a key prefix. See the [`LanceDataset::metadata_cache`] field.
    /// This prevents collisions between different datasets.
    pub(crate) metadata_cache: caches::GlobalMetadataCache,

    pub(crate) index_extensions: HashMap<(IndexType, String), Arc<dyn IndexExtension>>,

    store_registry: Arc<ObjectStoreRegistry>,

    spill_store: Arc<dyn SpillStore>,
}

impl DeepSizeOf for Session {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let mut size = 0;
        // Measure the actual cache contents through the wrapper types
        size += self.index_cache.deep_size_of_children(context);
        size += self.metadata_cache.deep_size_of_children(context);
        for ext in self.index_extensions.values() {
            size += ext.deep_size_of_children(context);
        }
        size
    }
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field(
                "index_cache",
                &format!("IndexCache(items={})", self.index_cache.0.approx_size(),),
            )
            .field(
                "file_metadata_cache",
                &format!("LanceCache(items={})", self.metadata_cache.0.approx_size(),),
            )
            .field(
                "index_extensions",
                &self.index_extensions.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Session {
    /// Create a new session.
    ///
    /// Parameters:
    ///
    /// - ***index_cache_size***: the size of the index cache, backed by
    ///   [`QuickCacheBackend`].
    /// - ***metadata_cache_size***: the size of the metadata cache, backed by
    ///   [`QuickCacheBackend`].
    /// - ***store_registry***: the object store registry to use when opening
    ///   datasets. This determines which schemes are available, and also allows
    ///   re-using object stores.
    pub fn new(
        index_cache_size: usize,
        metadata_cache_size: usize,
        store_registry: Arc<ObjectStoreRegistry>,
    ) -> Self {
        Self {
            index_cache: GlobalIndexCache(LanceCache::with_backend(Arc::new(
                QuickCacheBackend::with_capacity(index_cache_size),
            ))),
            metadata_cache: GlobalMetadataCache(LanceCache::with_backend(Arc::new(
                QuickCacheBackend::with_capacity(metadata_cache_size),
            ))),
            index_extensions: HashMap::new(),
            store_registry,
            spill_store: Arc::new(LocalSpillStore::default()),
        }
    }

    /// Create a session with a custom index cache backend.
    ///
    /// The provided backend will be used for caching index data. The metadata
    /// cache uses a [`QuickCacheBackend`] with the given capacity.
    pub fn with_index_cache_backend(
        index_cache_backend: Arc<dyn CacheBackend>,
        metadata_cache_size: usize,
        store_registry: Arc<ObjectStoreRegistry>,
    ) -> Self {
        Self {
            index_cache: GlobalIndexCache(LanceCache::with_backend(index_cache_backend)),
            metadata_cache: GlobalMetadataCache(LanceCache::with_backend(Arc::new(
                QuickCacheBackend::with_capacity(metadata_cache_size),
            ))),
            index_extensions: HashMap::new(),
            store_registry,
            spill_store: Arc::new(LocalSpillStore::default()),
        }
    }

    /// Replace the spill store used by this session.
    ///
    /// This is a builder-style method that consumes and returns `self`, making
    /// it easy to chain during session construction:
    ///
    /// ```rust,no_run
    /// # use lance::session::Session;
    /// # use lance_io::spill::LocalSpillStore;
    /// # use std::sync::Arc;
    /// let session = Session::default()
    ///     .with_spill_store(Arc::new(LocalSpillStore::with_cap(1 << 30).unwrap()));
    /// ```
    pub fn with_spill_store(mut self, store: Arc<dyn SpillStore>) -> Self {
        self.spill_store = store;
        self
    }

    /// Return a reference to the session's spill store.
    ///
    /// Callers use this to obtain reclaimable scratch space for intermediate
    /// state that overflows memory (e.g. index builders).
    pub fn spill_store(&self) -> &dyn SpillStore {
        &*self.spill_store
    }

    /// Create a session with custom backends for both caches.
    ///
    /// Each [`CacheSpec`] controls one tier. [`CacheSpec::Default`] uses that
    /// tier's Lance-level default capacity, [`CacheSpec::Size`] uses the
    /// default in-memory backend with an explicit capacity, and
    /// [`CacheSpec::Backend`] uses a caller-provided backend. This keeps size
    /// and backend selection mutually exclusive.
    ///
    /// This is the recommended constructor when a caller has already resolved
    /// backend selection through
    /// [`build_from_config`](lance_core::cache::build_from_config) or
    /// [`build_from_uri`](lance_core::cache::build_from_uri) — the resulting
    /// `Arc<dyn CacheBackend>` can be plugged in for either or both caches.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lance::session::{CacheSpec, Session};
    /// # use lance_core::cache::build_from_uri;
    /// # fn example() -> lance_core::Result<()> {
    /// let index_backend = build_from_uri("moka://?capacity=1048576")?;
    /// let session = Session::with_cache_backends(
    ///     CacheSpec::Backend(index_backend),
    ///     CacheSpec::Default,
    ///     Default::default(),
    /// );
    /// # let _ = session;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_cache_backends(
        index_cache: CacheSpec,
        metadata_cache: CacheSpec,
        store_registry: Arc<ObjectStoreRegistry>,
    ) -> Self {
        let index_cache = Self::build_cache(index_cache, DEFAULT_INDEX_CACHE_SIZE);
        let metadata_cache = Self::build_cache(metadata_cache, DEFAULT_METADATA_CACHE_SIZE);
        Self {
            index_cache: GlobalIndexCache(index_cache),
            metadata_cache: GlobalMetadataCache(metadata_cache),
            index_extensions: HashMap::new(),
            store_registry,
            spill_store: Arc::new(LocalSpillStore::default()),
        }
    }

    fn build_cache(spec: CacheSpec, default_size: usize) -> LanceCache {
        match spec {
            CacheSpec::Default => {
                LanceCache::with_backend(Arc::new(QuickCacheBackend::with_capacity(default_size)))
            }
            CacheSpec::Size(size) => {
                LanceCache::with_backend(Arc::new(QuickCacheBackend::with_capacity(size)))
            }
            CacheSpec::Backend(backend) => LanceCache::with_backend(backend),
        }
    }

    /// Register a new index extension.
    ///
    /// A name can only be registered once per type of index extension.
    ///
    /// Parameters:
    ///
    /// - ***name***: the name of the extension.
    /// - ***extension***: the extension to register.
    pub fn register_index_extension(
        &mut self,
        name: String,
        extension: Arc<dyn IndexExtension>,
    ) -> Result<()> {
        match extension.index_type() {
            IndexType::Vector => {
                if self
                    .index_extensions
                    .contains_key(&(IndexType::Vector, name.clone()))
                {
                    return Err(Error::invalid_input(format!(
                        "{name} is already registered"
                    )));
                }

                if let Some(ext) = extension.to_vector() {
                    self.index_extensions
                        .insert((IndexType::Vector, name), ext.to_generic());
                } else {
                    return Err(Error::invalid_input(format!(
                        "{name} is not a vector index extension"
                    )));
                }
            }
            _ => {
                return Err(Error::invalid_input(format!(
                    "scalar index extension is not support yet: {}",
                    extension.index_type()
                )));
            }
        }

        Ok(())
    }

    /// Return the current size of the session in bytes
    ///
    /// Keep in mind that this is not trivial to compute, as we will need to walk the caches
    pub fn size_bytes(&self) -> u64 {
        // We re-expose deep_size_of here so that users don't
        // need the deepsize crate themselves (e.g. to use deep_size_of)
        self.deep_size_of() as u64
    }

    /// Get the approximate number of items in the session.
    ///
    /// This is a rough estimate of the number of items in the session.  It is not
    /// exact and is not guaranteed to be accurate.
    pub fn approx_num_items(&self) -> usize {
        self.index_cache.0.approx_size()
            + self.metadata_cache.0.approx_size()
            + self.index_extensions.len()
    }

    /// Get the object store registry.
    pub fn store_registry(&self) -> Arc<ObjectStoreRegistry> {
        self.store_registry.clone()
    }

    /// Get a reference to the raw metadata cache (for use in index reconstruction).
    pub fn file_metadata_cache(&self) -> &LanceCache {
        &self.metadata_cache.0
    }

    /// Fetch statistics for the metadata cache
    pub async fn metadata_cache_stats(&self) -> lance_core::cache::CacheStats {
        self.metadata_cache.0.stats().await
    }

    /// Fetch statistics for the index cache
    pub async fn index_cache_stats(&self) -> lance_core::cache::CacheStats {
        self.index_cache.0.stats().await
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new(
            DEFAULT_INDEX_CACHE_SIZE,
            DEFAULT_METADATA_CACHE_SIZE,
            Arc::new(ObjectStoreRegistry::default()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_core::cache::{CacheKey, UnsizedCacheKey};
    use lance_index::vector::VectorIndex;
    use std::borrow::Cow;
    use tokio::io::AsyncWriteExt;

    struct TestKey(&'static str);
    impl CacheKey for TestKey {
        type ValueType = Vec<i32>;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "Test"
        }
    }

    struct TestUnsizedKey(&'static str);
    impl UnsizedCacheKey for TestUnsizedKey {
        type ValueType = dyn VectorIndex;
        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "TestUnsized"
        }
    }

    #[tokio::test]
    async fn test_disable_index_cache() {
        let no_cache = Session::new(0, 0, Default::default());
        assert!(
            no_cache
                .index_cache
                .get_unsized_with_key(&TestUnsizedKey("abc"))
                .await
                .is_none()
        );
    }

    /// `with_cache_backends` should honor whichever tier the caller
    /// provided a backend for and fall back to that tier's default on the
    /// other tier.
    #[tokio::test]
    async fn test_with_cache_backends_uses_provided_and_default() {
        use lance_core::cache::build_from_uri;

        let index_backend = build_from_uri("moka://?capacity=1048576").unwrap();
        let session = Session::with_cache_backends(
            CacheSpec::Backend(index_backend),
            CacheSpec::Default,
            Default::default(),
        );

        let value = Arc::new(vec![1, 2, 3]);
        session
            .index_cache
            .insert_with_key(&TestKey("injected-index-backend"), value.clone())
            .await;
        assert_eq!(
            session
                .index_cache
                .get_with_key(&TestKey("injected-index-backend"))
                .await
                .as_deref(),
            Some(value.as_ref())
        );
        // Metadata cache fell back to a size-based default. We can only
        // sanity-check that the session was constructed without panicking.
        let stats = session.metadata_cache.0.stats().await;
        assert_eq!(stats.num_entries, 0);
    }

    #[tokio::test]
    async fn test_with_cache_backends_uses_explicit_size() {
        let session = Session::with_cache_backends(
            CacheSpec::Size(0),
            CacheSpec::Size(2048),
            Default::default(),
        );

        session
            .index_cache
            .insert_with_key(&TestKey("disabled-index-cache"), Arc::new(vec![1, 2, 3]))
            .await;
        assert!(
            session
                .index_cache
                .get_with_key(&TestKey("disabled-index-cache"))
                .await
                .is_none()
        );

        session
            .metadata_cache
            .0
            .insert_with_key(&TestKey("metadata-cache"), Arc::new(vec![4, 5, 6]))
            .await;
        assert!(
            session
                .metadata_cache
                .0
                .get_with_key(&TestKey("metadata-cache"))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_default_session_has_spill_store() {
        let session = Session::default();
        // Should be able to allocate a spill and write to it without error.
        let (mut writer, _spill) = session.spill_store().new_spill().await.unwrap();
        writer.write_all(b"scratch").await.unwrap();
        lance_io::traits::Writer::shutdown(writer.as_mut())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_custom_spill_store_injected() {
        let capped = Arc::new(LocalSpillStore::with_cap(50).unwrap());
        let session = Session::default().with_spill_store(capped);

        let (mut writer, _spill) = session.spill_store().new_spill().await.unwrap();
        // Writing 51 bytes exceeds the 50-byte cap; the typed error is wrapped
        // in an io::Error by the writer and recovered on conversion.
        let io_err = writer.write_all(&[0u8; 51]).await.unwrap_err();
        let err: lance_core::Error = io_err.into();
        assert!(
            matches!(
                err,
                lance_core::Error::DiskCapExceeded { cap_bytes: 50, .. }
            ),
            "expected DiskCapExceeded, got {err}"
        );
    }
}
