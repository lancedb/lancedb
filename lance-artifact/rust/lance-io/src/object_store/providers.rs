// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use object_store::path::Path;
use url::Url;

use crate::object_store::WrappingObjectStore;
use crate::object_store::uri_to_url;

use super::{ObjectStore, ObjectStoreParams, tracing::ObjectStoreTracingExt};
use lance_core::error::{Error, LanceOptionExt, Result};

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "gcp")]
pub mod gcp;
#[cfg(feature = "goosefs")]
pub mod goosefs;
#[cfg(feature = "huggingface")]
pub mod huggingface;
pub mod local;
pub mod memory;
#[cfg(feature = "oss")]
pub mod oss;
pub mod shared_memory;
#[cfg(feature = "tencent")]
pub mod tencent;
#[cfg(feature = "tos")]
pub mod tos;

#[async_trait::async_trait]
pub trait ObjectStoreProvider: std::fmt::Debug + Sync + Send {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore>;

    /// Extract the path relative to the base of the store.
    ///
    /// For example, in S3 the path is relative to the bucket. So a URL of
    /// `s3://bucket/path/to/file` would return `path/to/file`.
    ///
    /// Meanwhile, for a file store, the path is relative to the filesystem root.
    /// So a URL of `file:///path/to/file` would return `/path/to/file`.
    fn extract_path(&self, url: &Url) -> Result<Path> {
        // url.path() returns a percent-encoded string (per the WHATWG URL spec).
        // Path::from_url_path decodes it first so the Path internal representation
        // holds the raw UTF-8 string. This prevents double-encoding when the
        // object store client later percent-encodes the path for HTTP requests.
        Path::from_url_path(url.path()).map_err(|e| {
            Error::invalid_input(format!("Invalid path in URL '{}': {}", url.path(), e))
        })
    }

    /// Calculate the unique prefix that should be used for this object store.
    ///
    /// For object stores that don't have the concept of buckets, this will just be something like
    /// 'file' or 'memory'.
    ///
    /// In object stores where all bucket names are unique, like s3, this will be
    /// simply 's3$my_bucket_name' or similar.
    ///
    /// In Azure, only the combination of (account name, container name) is unique, so
    /// this will be something like 'az$account_name@container'
    ///
    /// Providers should override this if they have special requirements like Azure's.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Ok(format!("{}${}", url.scheme(), url.authority()))
    }
}

/// Statistics for the object store registry cache.
#[derive(Debug, Clone, Default)]
pub struct ObjectStoreRegistryStats {
    /// Number of cache hits (store was already cached and reused).
    pub hits: u64,
    /// Number of cache misses (new store had to be created).
    pub misses: u64,
    /// Number of currently active object stores in the cache.
    pub active_stores: usize,
}

/// A registry of object store providers.
///
/// Use [`Self::default()`] to create one with the available default providers.
/// This includes (depending on features enabled):
/// - `memory`: An in-memory object store.
/// - `file`: A local file object store, with optimized code paths.
/// - `file-object-store`: A local file object store that uses the ObjectStore API,
///   for all operations. Used for testing with ObjectStore wrappers.
/// - `file+uring`: A local file object store using io_uring (Linux only).
/// - `s3`: An S3 object store.
/// - `s3+ddb`: An S3 object store with DynamoDB for metadata.
/// - `az`: An Azure Blob Storage object store.
/// - `gs`: A Google Cloud Storage object store.
/// - `tos`: A Volcengine TOS object store.
///
/// Use [`Self::empty()`] to create an empty registry, with no providers registered.
///
/// The registry also caches object stores that are currently in use. It holds
/// weak references to the object stores, so they are not held onto. If an object
/// store is no longer in use, it will be removed from the cache on the next
/// call to either [`Self::active_stores()`] or [`Self::get_store()`].
#[derive(Debug)]
pub struct ObjectStoreRegistry {
    providers: RwLock<HashMap<String, Arc<dyn ObjectStoreProvider>>>,
    // Cache of object stores currently in use. We use a weak reference so the
    // cache itself doesn't keep them alive if no object store is actually using
    // it.
    active_stores: RwLock<HashMap<(String, ObjectStoreParams), Weak<ObjectStore>>>,
    // Cache statistics
    hits: AtomicU64,
    misses: AtomicU64,
}

impl ObjectStoreRegistry {
    /// Create a new registry with no providers registered.
    ///
    /// Typically, you want to use [`Self::default()`] instead, so you get the
    /// default providers.
    pub fn empty() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            active_stores: RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get the object store provider for a given scheme.
    pub fn get_provider(&self, scheme: &str) -> Option<Arc<dyn ObjectStoreProvider>> {
        self.providers
            .read()
            .expect("ObjectStoreRegistry lock poisoned")
            .get(scheme)
            .cloned()
    }

    /// Get a list of all active object stores.
    ///
    /// Calling this will also clean up any weak references to object stores that
    /// are no longer valid.
    pub fn active_stores(&self) -> Vec<Arc<ObjectStore>> {
        let mut found_inactive = false;
        let output = self
            .active_stores
            .read()
            .expect("ObjectStoreRegistry lock poisoned")
            .values()
            .filter_map(|weak| match weak.upgrade() {
                Some(store) => Some(store),
                None => {
                    found_inactive = true;
                    None
                }
            })
            .collect();

        if found_inactive {
            // Clean up the cache by removing any weak references that are no longer valid
            let mut cache_lock = self
                .active_stores
                .write()
                .expect("ObjectStoreRegistry lock poisoned");
            cache_lock.retain(|_, weak| weak.upgrade().is_some());
        }
        output
    }

    /// Get cache statistics for monitoring and debugging.
    ///
    /// Returns the number of cache hits, misses, and currently active stores.
    /// This is useful for detecting configuration issues that cause excessive
    /// cache misses (e.g., storage options that vary per-request).
    pub fn stats(&self) -> ObjectStoreRegistryStats {
        let active_stores = self
            .active_stores
            .read()
            .map(|s| s.values().filter(|w| w.strong_count() > 0).count())
            .unwrap_or(0);
        ObjectStoreRegistryStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            active_stores,
        }
    }

    fn scheme_not_found_error(&self, scheme: &str) -> Error {
        let mut message = format!("No object store provider found for scheme: '{}'", scheme);
        if let Ok(providers) = self.providers.read() {
            let valid_schemes = providers.keys().cloned().collect::<Vec<_>>().join(", ");
            message.push_str(&format!("\nValid schemes: {}", valid_schemes));
        }
        Error::invalid_input(message)
    }

    /// Get an object store for a given base path and parameters.
    ///
    /// If the object store is already in use, it will return a strong reference
    /// to the object store. If the object store is not in use, it will create a
    /// new object store and return a strong reference to it.
    pub async fn get_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> Result<Arc<ObjectStore>> {
        // Base-scoped storage options (`base_<id>.<key>`) are directives for
        // other registered base paths; resolve them away before building or
        // caching a store for this location. Params already resolved for a
        // base contain no scoped entries, so this is a no-op for them.
        let params = params.scoped_to_base(None);
        let params = params.as_ref();
        let scheme = base_path.scheme();
        let Some(provider) = self.get_provider(scheme) else {
            return Err(self.scheme_not_found_error(scheme));
        };

        let cache_path =
            provider.calculate_object_store_prefix(&base_path, params.storage_options())?;
        let cache_key = (cache_path.clone(), params.clone());

        // Check if we have a cached store for this base path and params
        {
            let maybe_store = self
                .active_stores
                .read()
                .ok()
                .expect_ok()?
                .get(&cache_key)
                .cloned();
            if let Some(store) = maybe_store {
                if let Some(store) = store.upgrade() {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(store);
                } else {
                    // Remove the weak reference if it is no longer valid
                    let mut cache_lock = self
                        .active_stores
                        .write()
                        .expect("ObjectStoreRegistry lock poisoned");
                    if let Some(store) = cache_lock.get(&cache_key)
                        && store.upgrade().is_none()
                    {
                        // Remove the weak reference if it is no longer valid
                        cache_lock.remove(&cache_key);
                    }
                }
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);

        let mut store = provider.new_store(base_path, params).await?;

        store.inner = store.inner.traced();

        // Label metrics by the store's unique prefix (e.g. `s3$bucket`,
        // `az$container@account`) so multiple stores on one cloud differ.
        crate::object_store::meter_store(&mut store.inner, &mut store.io_tracker, &cache_path);

        if let Some(wrapper) = &params.object_store_wrapper {
            store.inner = wrapper.wrap(&cache_path, store.inner);
        }

        // Always wrap with IO tracking
        store.inner = store.io_tracker.wrap("", store.inner);

        let store = Arc::new(store);

        {
            // Insert the store into the cache
            let mut cache_lock = self.active_stores.write().ok().expect_ok()?;
            cache_lock.insert(cache_key, Arc::downgrade(&store));
        }

        Ok(store)
    }

    /// Calculate the datastore prefix based on the URI and the storage options.
    /// The data store prefix should uniquely identify the datastore.
    pub fn calculate_object_store_prefix(
        &self,
        uri: &str,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let url = uri_to_url(uri)?;
        match self.get_provider(url.scheme()) {
            None => {
                if url.scheme() == "file" || url.scheme().len() == 1 {
                    Ok("file".to_string())
                } else {
                    Err(self.scheme_not_found_error(url.scheme()))
                }
            }
            Some(provider) => provider.calculate_object_store_prefix(&url, storage_options),
        }
    }
}

impl Default for ObjectStoreRegistry {
    fn default() -> Self {
        let mut providers: HashMap<String, Arc<dyn ObjectStoreProvider>> = HashMap::new();

        providers.insert("memory".into(), Arc::new(memory::MemoryStoreProvider));
        providers.insert(
            "shared-memory".into(),
            Arc::new(shared_memory::SharedMemoryStoreProvider::default()),
        );
        providers.insert("file".into(), Arc::new(local::FileStoreProvider));
        // The "file" scheme has special optimized code paths that bypass
        // the ObjectStore API for better performance. However, this can make it
        // hard to test when using ObjectStore wrappers, such as IOTrackingStore.
        // So we provide a "file-object-store" scheme that uses the ObjectStore API.
        // The specialized code paths are differentiated by the scheme name.
        providers.insert(
            "file-object-store".into(),
            Arc::new(local::FileStoreProvider),
        );
        #[cfg(target_os = "linux")]
        providers.insert("file+uring".into(), Arc::new(local::FileStoreProvider));

        #[cfg(feature = "aws")]
        {
            let aws = Arc::new(aws::AwsStoreProvider);
            providers.insert("s3".into(), aws.clone());
            providers.insert("s3+ddb".into(), aws);
        }
        #[cfg(feature = "azure")]
        {
            let azure = Arc::new(azure::AzureBlobStoreProvider);
            providers.insert("az".into(), azure.clone());
            providers.insert("abfss".into(), azure);
        }
        #[cfg(feature = "gcp")]
        providers.insert("gs".into(), Arc::new(gcp::GcsStoreProvider));
        #[cfg(feature = "goosefs")]
        providers.insert("goosefs".into(), Arc::new(goosefs::GooseFsStoreProvider));
        #[cfg(feature = "oss")]
        providers.insert("oss".into(), Arc::new(oss::OssStoreProvider));
        #[cfg(feature = "tencent")]
        providers.insert("cos".into(), Arc::new(tencent::TencentStoreProvider));
        #[cfg(feature = "huggingface")]
        providers.insert("hf".into(), Arc::new(huggingface::HuggingfaceStoreProvider));
        #[cfg(feature = "tos")]
        providers.insert("tos".into(), Arc::new(tos::TosStoreProvider));
        Self {
            providers: RwLock::new(providers),
            active_stores: RwLock::new(HashMap::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
}

impl ObjectStoreRegistry {
    /// Add a new object store provider to the registry. The provider will be used
    /// in [`Self::get_store()`] when a URL is passed with a matching scheme.
    pub fn insert(&self, scheme: &str, provider: Arc<dyn ObjectStoreProvider>) {
        self.providers
            .write()
            .expect("ObjectStoreRegistry lock poisoned")
            .insert(scheme.into(), provider);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[derive(Debug)]
    struct DummyProvider;

    #[async_trait::async_trait]
    impl ObjectStoreProvider for DummyProvider {
        async fn new_store(
            &self,
            _base_path: Url,
            _params: &ObjectStoreParams,
        ) -> Result<ObjectStore> {
            unreachable!("This test doesn't create stores")
        }
    }

    #[test]
    fn test_calculate_object_store_prefix() {
        let provider = DummyProvider;
        let url = Url::parse("dummy://blah/path").unwrap();
        assert_eq!(
            "dummy$blah",
            provider.calculate_object_store_prefix(&url, None).unwrap()
        );
    }

    #[tokio::test]
    async fn test_get_store_resolves_base_scoped_options() {
        use crate::object_store::StorageOptionsAccessor;

        let registry = ObjectStoreRegistry::default();
        let url = Url::parse("memory://test").unwrap();

        let with_scoped = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([
                    ("shared".to_string(), "value".to_string()),
                    ("base_1.account_key".to_string(), "base1-key".to_string()),
                ]),
            ))),
            ..Default::default()
        };
        let without_scoped = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([("shared".to_string(), "value".to_string())]),
            ))),
            ..Default::default()
        };

        // Base-scoped entries are resolved away before the store is built and
        // cached, so params with and without them yield the same cached store.
        let store_scoped = registry.get_store(url.clone(), &with_scoped).await.unwrap();
        let store_plain = registry.get_store(url, &without_scoped).await.unwrap();
        assert!(Arc::ptr_eq(&store_scoped, &store_plain));
    }

    #[test]
    fn test_calculate_object_store_scheme_not_found() {
        let registry = ObjectStoreRegistry::empty();
        registry.insert("dummy", Arc::new(DummyProvider));
        let s = "Invalid user input: No object store provider found for scheme: 'dummy2'\nValid schemes: dummy";
        let result = registry
            .calculate_object_store_prefix("dummy2://mybucket/my/long/path", None)
            .expect_err("expected error")
            .to_string();
        assert_eq!(s, &result[..s.len()]);
    }

    // Test that paths without a scheme get treated as local paths.
    #[test]
    fn test_calculate_object_store_prefix_for_local() {
        let registry = ObjectStoreRegistry::empty();
        assert_eq!(
            "file",
            registry
                .calculate_object_store_prefix("/tmp/foobar", None)
                .unwrap()
        );
    }

    // Test that paths with a single-letter scheme that is not registered for anything get treated as local paths.
    #[test]
    fn test_calculate_object_store_prefix_for_local_windows_path() {
        let registry = ObjectStoreRegistry::empty();
        assert_eq!(
            "file",
            registry
                .calculate_object_store_prefix("c://dos/path", None)
                .unwrap()
        );
    }

    // Test that paths with a given scheme get mapped to that storage provider.
    #[test]
    fn test_calculate_object_store_prefix_for_dummy_path() {
        let registry = ObjectStoreRegistry::empty();
        registry.insert("dummy", Arc::new(DummyProvider));
        assert_eq!(
            "dummy$mybucket",
            registry
                .calculate_object_store_prefix("dummy://mybucket/my/long/path", None)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_stats_hit_miss_tracking() {
        use crate::object_store::StorageOptionsAccessor;
        let registry = ObjectStoreRegistry::default();
        let url = Url::parse("memory://test").unwrap();

        let params1 = ObjectStoreParams::default();
        let params2 = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([("k".into(), "v".into())]),
            ))),
            ..Default::default()
        };

        // (hits, misses, active)
        let cases: &[(&ObjectStoreParams, (u64, u64, usize))] = &[
            (&params1, (0, 1, 1)), // miss: new params
            (&params1, (1, 1, 1)), // hit: same params
            (&params2, (1, 2, 2)), // miss: different storage_options
        ];

        let mut stores = vec![]; // retain the stores
        for (params, (hits, misses, active)) in cases {
            stores.push(registry.get_store(url.clone(), params).await.unwrap());
            let s = registry.stats();
            assert_eq!(
                (s.hits, s.misses, s.active_stores),
                (*hits, *misses, *active)
            );
        }

        // Same params returns same instance
        assert!(Arc::ptr_eq(&stores[0], &stores[1]));
    }
}
