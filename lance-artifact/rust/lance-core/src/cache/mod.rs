// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance cache system.
//!
//! ## For cache users
//!
//! Use [`LanceCache`] (or [`WeakLanceCache`]) to store and retrieve typed
//! values. Define a [`CacheKey`] (or [`UnsizedCacheKey`] for trait objects) to
//! describe what you're caching and its type.
//!
//! To make a value type serializable (so persistent backends can store it),
//! implement [`CacheCodecImpl`] on the type, then override [`CacheKey::codec`]:
//!
//! ```ignore
//! impl CacheCodecImpl for MyData {
//!     fn serialize(&self, w: &mut dyn Write) -> Result<()> { /* ... */ }
//!     fn deserialize(data: &Bytes) -> Result<Self> { /* ... */ }
//! }
//!
//! impl CacheKey for MyDataKey {
//!     type ValueType = MyData;
//!     fn key(&self) -> Cow<'_, str> { /* ... */ }
//!     fn type_name() -> &'static str { "MyData" }
//!     fn codec() -> Option<CacheCodec> {
//!         Some(CacheCodec::from_impl::<MyData>())
//!     }
//! }
//! ```
//!
//! ## For backend implementors
//!
//! Implement [`CacheBackend`] to provide a custom storage layer (disk, Redis,
//! etc.). Backends receive opaque, fixed-size [`InternalCacheKey`] values and
//! type-erased [`CacheEntry`] values. The typed wrapping is handled by
//! [`LanceCache`]. See the [`backend`] module for migration details.
//!
//! ## Serialization flow
//!
//! When a [`CacheKey`] provides a codec via [`CacheKey::codec`]:
//!
//! 1. [`LanceCache`] wraps the [`CacheCodec`] and passes it to the backend
//!    alongside the entry on `insert` and `get` calls.
//! 2. In-memory backends (like [`MokaCacheBackend`]) ignore the codec.
//! 3. Persistent backends use `codec.serialize(entry, writer)` on insert and
//!    `codec.deserialize(reader)` on get to persist entries across restarts.

pub mod backend;
mod backend_uri;
pub mod codec;
mod entry_io;
mod key;
mod moka;
mod quick;
mod registry;

pub use backend::{CacheBackend, CacheEntry};
pub use backend_uri::{build_from_uri, parse_backend_uri};
pub use codec::{
    CacheCodec, CacheCodecImpl, CacheDecode, CacheMissReason, MAGIC, has_cache_envelope,
};
pub use entry_io::{CacheEntryReader, CacheEntryWriter};
pub use key::{CACHE_KEY_FORMAT, CacheKeySchema, CacheNamespace, InternalCacheKey, KeyBuilder};
pub use moka::MokaCacheBackend;
pub use quick::{QuickCacheBackend, recommended_cache_shards};
pub use registry::{BackendBuildFn, BackendConfig, build_from_config, register_backend};

use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{
    Arc, RwLock, Weak,
    atomic::{AtomicU64, Ordering},
};

use futures::Future;

use crate::{Error, Result};

pub use crate::deepsize::{Context, DeepSizeOf};

// ---------------------------------------------------------------------------
// CacheKey / UnsizedCacheKey — typed key traits for cache users
// ---------------------------------------------------------------------------

/// Typed cache key for sized value types.
///
/// Existing implementations can continue returning a logical string from
/// [`key`](Self::key). Performance-sensitive implementations should also
/// provide a stable schema and stream typed fields through
/// [`write_key`](Self::write_key), avoiding construction of that string.
///
/// # Example
///
/// ```ignore
/// struct MyKey { id: u64 }
///
/// impl CacheKey for MyKey {
///     type ValueType = MyData;
///     fn key(&self) -> Cow<'_, str> { self.id.to_string().into() }
///     fn type_name() -> &'static str { "MyData" }
/// }
/// ```
pub trait CacheKey {
    type ValueType: 'static;

    fn key(&self) -> Cow<'_, str>;

    /// Short, stable string identifying this value type.
    ///
    /// Two `CacheKey` impls that store different `ValueType`s **must** return
    /// different type names.
    ///
    /// Use a short literal (e.g. `"Vec<IndexMetadata>"`), not
    /// `std::any::type_name` — the latter is not guaranteed stable across
    /// compiler versions or build configurations.
    fn type_name() -> &'static str;

    /// Stable identity included in the physical key.
    ///
    /// The compatibility default preserves existing implementations by using
    /// their author-assigned [`type_name`](Self::type_name).
    fn stable_type_id() -> &'static str {
        Self::type_name()
    }

    /// Versioned schema for the logical key fields.
    fn schema() -> CacheKeySchema {
        CacheKeySchema::LEGACY_TEXT
    }

    /// Stream the logical key fields into the canonical key builder.
    ///
    /// The compatibility default hashes the existing string key. In-tree hot
    /// paths override this with typed, allocation-free field encoding.
    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(self.key().as_ref());
    }

    /// Optional codec for serializing/deserializing this key's value type.
    ///
    /// Returns `None` by default. Cache backends that support persistence
    /// (e.g. disk-backed caches) use this to serialize entries on insert and
    /// deserialize on get. Types without a codec will only be stored in-memory.
    ///
    /// [`CacheCodec`] is `Copy` (two plain function pointers), so returning it
    /// by value is cheap — no allocation needed.
    fn codec() -> Option<CacheCodec> {
        None
    }
}

/// Like [`CacheKey`] but for unsized value types (e.g. `dyn Trait`).
///
/// The cache wraps values in an extra `Arc` layer internally; callers pass
/// and receive `Arc<T>` where `T: ?Sized`.
///
/// Unsized cache entries are always in-memory only (no serialization codec).
/// For serializable entries, use a sized [`CacheKey`] instead.
pub trait UnsizedCacheKey {
    type ValueType: 'static + ?Sized;

    fn key(&self) -> Cow<'_, str>;

    /// Short, stable string identifying this value type.
    /// See [`CacheKey::type_name`] for requirements.
    fn type_name() -> &'static str;

    /// Stable identity included in the physical key.
    fn stable_type_id() -> &'static str {
        Self::type_name()
    }

    /// Versioned schema for the logical key fields.
    fn schema() -> CacheKeySchema {
        CacheKeySchema::LEGACY_TEXT
    }

    /// Stream the logical key fields into the canonical key builder.
    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(self.key().as_ref());
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Size of a cached `Arc<T>`, accounting for the Arc overhead (two atomic counters).
fn cache_entry_size<T: DeepSizeOf + ?Sized>(value: &T) -> usize {
    value.deep_size_of() + std::mem::size_of::<std::sync::atomic::AtomicUsize>() * 2
}

type CacheEntrySizeAccessor = fn(&CacheEntry, &mut Context) -> Option<usize>;

fn cache_entry_size_with_context<T>(entry: &CacheEntry, context: &mut Context) -> Option<usize>
where
    T: DeepSizeOf + Send + Sync + 'static,
{
    let value = entry.downcast_ref::<T>()?;
    let entry_ptr = Arc::as_ptr(entry) as *const () as usize;
    if !context.mark_seen(entry_ptr) {
        return Some(0);
    }
    Some(
        std::mem::size_of_val(value)
            + value.deep_size_of_children(context)
            + std::mem::size_of::<std::sync::atomic::AtomicUsize>() * 2,
    )
}

#[derive(Debug)]
struct CacheState {
    backend: Arc<dyn CacheBackend>,
    hits: AtomicU64,
    misses: AtomicU64,
    entry_size_accessors: RwLock<HashMap<TypeId, CacheEntrySizeAccessor>>,
}

impl CacheState {
    fn new(backend: Arc<dyn CacheBackend>) -> Self {
        Self {
            backend,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            entry_size_accessors: RwLock::new(HashMap::new()),
        }
    }

    fn entry_size<T>(&self, value: &T) -> usize
    where
        T: DeepSizeOf + Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        let is_registered = self
            .entry_size_accessors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(&type_id);
        if !is_registered {
            self.entry_size_accessors
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(type_id)
                .or_insert(cache_entry_size_with_context::<T>);
        }
        cache_entry_size(value)
    }
}

// ---------------------------------------------------------------------------
// LanceCache — typed wrapper around dyn CacheBackend
// ---------------------------------------------------------------------------

/// Typed cache wrapper that handles key construction and type safety.
///
/// Internally delegates to a [`CacheBackend`]. The default backend is
/// [`MokaCacheBackend`]; pass a custom backend via [`LanceCache::with_backend`].
#[derive(Clone)]
pub struct LanceCache {
    state: Arc<CacheState>,
    namespace: key::CacheNamespace,
}

impl std::fmt::Debug for LanceCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceCache")
            .field("backend", &self.state.backend)
            .finish_non_exhaustive()
    }
}

impl DeepSizeOf for LanceCache {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        let state_ptr = Arc::as_ptr(&self.state) as usize;
        if !context.mark_seen(state_ptr) {
            return 0;
        }

        let accessors = self
            .state
            .entry_size_accessors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.state
            .backend
            .deep_size_of_entries(context, &|entry, context| {
                accessors
                    .get(&entry.as_ref().type_id())
                    .and_then(|size_of_entry| size_of_entry(entry, context))
            })
            .unwrap_or_else(|| self.state.backend.approx_size_bytes())
    }
}

impl LanceCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_backend(Arc::new(MokaCacheBackend::with_capacity(capacity)))
    }

    /// Create a cache backed by a custom [`CacheBackend`].
    pub fn with_backend(backend: Arc<dyn CacheBackend>) -> Self {
        Self {
            state: Arc::new(CacheState::new(backend)),
            namespace: key::CacheNamespace::root(),
        }
    }

    pub fn no_cache() -> Self {
        Self::with_backend(Arc::new(MokaCacheBackend::no_cache()))
    }

    /// Derive a child namespace for all keys in the returned cache handle.
    ///
    /// Each call adds one framed hierarchy segment. Consequently,
    /// `cache.with_key_prefix("a").with_key_prefix("b")` is deliberately
    /// distinct from `cache.with_key_prefix("a/b")`.
    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            state: self.state.clone(),
            namespace: self.namespace.child(prefix),
        }
    }

    pub async fn size(&self) -> usize {
        self.state.backend.num_entries().await
    }

    pub fn approx_size(&self) -> usize {
        self.state.backend.approx_num_entries()
    }

    pub async fn size_bytes(&self) -> usize {
        self.state.backend.size_bytes().await
    }

    // -- Stats / clear --------------------------------------------------------

    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.state.hits.load(Ordering::Relaxed),
            misses: self.state.misses.load(Ordering::Relaxed),
            num_entries: self.state.backend.num_entries().await,
            size_bytes: self.state.backend.size_bytes().await,
        }
    }

    pub async fn clear(&self) {
        self.state.backend.clear().await;
        self.state.hits.store(0, Ordering::Relaxed);
        self.state.misses.store(0, Ordering::Relaxed);
    }

    // -- CacheKey-based methods -----------------------------------------------

    pub async fn insert_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let size = self.state.entry_size(metadata.as_ref());
        let key = self.sized_key(cache_key);
        self.state
            .backend
            .insert(&key, metadata, size, K::codec())
            .await;
    }

    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key = self.sized_key(cache_key);
        let Some(entry) = self.state.backend.get(&key, K::codec()).await else {
            self.state.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        match entry.downcast::<K::ValueType>() {
            Ok(value) => {
                self.state.hits.fetch_add(1, Ordering::Relaxed);
                Some(value)
            }
            Err(_) => {
                // Type mismatch: the backend returned a different concrete
                // type than expected (e.g. a disk cache may store
                // intermediate state). Treat as a miss.
                log::warn!(
                    "cache backend returned a value with the wrong concrete type for key type {:?}",
                    K::stable_type_id()
                );
                self.state.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        self.get_or_insert_with_key_hit(cache_key, loader)
            .await
            .map(|(value, _)| value)
    }

    /// Same as [`get_or_insert_with_key`](Self::get_or_insert_with_key), but
    /// also returns a boolean indicating whether the loader was skipped for
    /// this call.
    ///
    /// - `true` means this call did **not** execute the loader. That covers
    ///   both a true cache hit on an already-populated entry and a coalesced
    ///   concurrent load where an in-flight loader started by a different
    ///   caller produced the value.
    /// - `false` means the loader ran on this call (a real cache miss).
    ///
    /// Callers that want strict "served from cache" semantics should treat
    /// coalesced loads as misses; the current backend does not distinguish the
    /// two cases. Prefer this over rolling a caller-side `Arc<AtomicBool>`
    /// when the caller needs per-query hit/miss counters — the backend already
    /// tracks this bit internally and this method just exposes it.
    pub async fn get_or_insert_with_key_hit<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<(Arc<K::ValueType>, bool)>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        let key = self.sized_key(&cache_key);
        let state = self.state.clone();
        let typed_loader = Box::pin(async move {
            let value = Arc::new(loader().await?);
            let size = state.entry_size(value.as_ref());
            Ok((value as CacheEntry, size))
        });

        let (entry, was_cached) = self
            .state
            .backend
            .get_or_insert(&key, typed_loader, K::codec())
            .await?;
        let entry = entry.downcast::<K::ValueType>().map_err(|_| {
            self.state.misses.fetch_add(1, Ordering::Relaxed);
            Error::io(format!(
                "cache backend returned a value with the wrong concrete type for key type {:?}",
                K::stable_type_id()
            ))
        })?;
        if was_cached {
            self.state.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.state.misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok((entry, was_cached))
    }

    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, metadata: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let metadata = Arc::new(metadata);
        let size = self.state.entry_size(metadata.as_ref());
        let key = self.unsized_key(cache_key);
        self.state.backend.insert(&key, metadata, size, None).await;
    }

    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key = self.unsized_key(cache_key);
        let Some(entry) = self.state.backend.get(&key, None).await else {
            self.state.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        match entry.downcast::<Arc<K::ValueType>>() {
            Ok(value) => {
                self.state.hits.fetch_add(1, Ordering::Relaxed);
                Some(value.as_ref().clone())
            }
            Err(_) => {
                // Type mismatch: the backend returned a different concrete
                // type than expected (e.g. a disk cache may store
                // intermediate state). Treat as a miss.
                log::warn!(
                    "cache backend returned a value with the wrong concrete type for unsized key type {:?}",
                    K::stable_type_id()
                );
                self.state.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn sized_key<K: CacheKey>(&self, cache_key: &K) -> InternalCacheKey {
        let mut builder = KeyBuilder::new(self.namespace, K::stable_type_id(), K::schema());
        cache_key.write_key(&mut builder);
        builder.finish()
    }

    fn unsized_key<K: UnsizedCacheKey>(&self, cache_key: &K) -> InternalCacheKey {
        let mut builder = KeyBuilder::new(self.namespace, K::stable_type_id(), K::schema());
        cache_key.write_key(&mut builder);
        builder.finish()
    }
}

// ---------------------------------------------------------------------------
// WeakLanceCache
// ---------------------------------------------------------------------------

/// A weak reference to a LanceCache, used by indices to avoid circular references.
/// When the original cache is dropped, operations on this will gracefully no-op.
#[derive(Clone, Debug)]
pub struct WeakLanceCache {
    state: Weak<CacheState>,
    namespace: key::CacheNamespace,
}

impl WeakLanceCache {
    pub fn from(cache: &LanceCache) -> Self {
        Self {
            state: Arc::downgrade(&cache.state),
            namespace: cache.namespace,
        }
    }

    pub fn with_key_prefix(&self, prefix: &str) -> Self {
        Self {
            state: self.state.clone(),
            namespace: self.namespace.child(prefix),
        }
    }

    pub async fn get_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.upgrade()?.get_with_key(cache_key).await
    }

    pub async fn insert_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>) -> bool
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let Some(cache) = self.upgrade() else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert item");
            return false;
        };
        cache.insert_with_key(cache_key, value).await;
        true
    }

    /// Get or insert an item, computing it if necessary.
    ///
    /// Deduplication of concurrent loads is handled by the backend.
    pub async fn get_or_insert_with_key<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        self.get_or_insert_with_key_hit(cache_key, loader)
            .await
            .map(|(value, _)| value)
    }

    /// Same as [`get_or_insert_with_key`](Self::get_or_insert_with_key), but
    /// also returns a boolean indicating whether the loader was skipped for
    /// this call. See [`LanceCache::get_or_insert_with_key_hit`] for the
    /// coalesced-load caveat.
    pub async fn get_or_insert_with_key_hit<K, F, Fut>(
        &self,
        cache_key: K,
        loader: F,
    ) -> Result<(Arc<K::ValueType>, bool)>
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<K::ValueType>> + Send,
    {
        let Some(cache) = self.upgrade() else {
            log::warn!("WeakLanceCache: cache no longer available, computing without caching");
            return loader().await.map(|value| (Arc::new(value), false));
        };
        cache.get_or_insert_with_key_hit(cache_key, loader).await
    }

    pub async fn get_unsized_with_key<K>(&self, cache_key: &K) -> Option<Arc<K::ValueType>>
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        self.upgrade()?.get_unsized_with_key(cache_key).await
    }

    pub async fn insert_unsized_with_key<K>(&self, cache_key: &K, value: Arc<K::ValueType>)
    where
        K: UnsizedCacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let Some(cache) = self.upgrade() else {
            log::warn!("WeakLanceCache: cache no longer available, unable to insert unsized item");
            return;
        };
        cache.insert_unsized_with_key(cache_key, value).await;
    }

    fn upgrade(&self) -> Option<LanceCache> {
        Some(LanceCache {
            state: self.state.upgrade()?,
            namespace: self.namespace,
        })
    }
}

// ---------------------------------------------------------------------------
// CacheStats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of times `get`, `get_unsized`, or `get_or_insert` found an item in the cache.
    pub hits: u64,
    /// Number of times `get`, `get_unsized`, or `get_or_insert` did not find an item in the cache.
    pub misses: u64,
    /// Number of entries currently in the cache.
    pub num_entries: usize,
    /// Total size in bytes of all entries in the cache.
    pub size_bytes: usize,
}

impl CacheStats {
    pub fn hit_ratio(&self) -> f32 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.hits as f32 / (self.hits + self.misses) as f32
        }
    }

    pub fn miss_ratio(&self) -> f32 {
        if self.hits + self.misses == 0 {
            0.0
        } else {
            self.misses as f32 / (self.hits + self.misses) as f32
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::task::Poll;
    use std::thread;
    use std::time::Duration;

    use super::*;

    async fn report_first_pending<F>(
        future: F,
        parked: tokio::sync::oneshot::Sender<()>,
    ) -> F::Output
    where
        F: Future,
    {
        tokio::pin!(future);
        let mut parked = Some(parked);
        futures::future::poll_fn(|cx| match future.as_mut().poll(cx) {
            Poll::Pending => {
                if let Some(parked) = parked.take() {
                    let _ = parked.send(());
                }
                Poll::Pending
            }
            Poll::Ready(output) => Poll::Ready(output),
        })
        .await
    }

    #[derive(Clone)]
    struct VersionedTestKey<const SCHEMA_VERSION: u32> {
        id: u64,
    }

    type TestKey = VersionedTestKey<1>;
    type TestKeyV2 = VersionedTestKey<2>;

    impl<const SCHEMA_VERSION: u32> VersionedTestKey<SCHEMA_VERSION> {
        fn new(id: u64) -> Self {
            Self { id }
        }
    }

    impl<const SCHEMA_VERSION: u32> CacheKey for VersionedTestKey<SCHEMA_VERSION> {
        type ValueType = Vec<u32>;

        fn key(&self) -> Cow<'_, str> {
            self.id.to_string().into()
        }

        fn type_name() -> &'static str {
            "test.VecU32"
        }

        fn schema() -> CacheKeySchema {
            CacheKeySchema::new("test.vec-u32-key", SCHEMA_VERSION)
        }

        fn write_key(&self, builder: &mut KeyBuilder) {
            builder.write_u64(self.id);
        }
    }

    struct SharedTestValue {
        data: Arc<Vec<u8>>,
    }

    impl DeepSizeOf for SharedTestValue {
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.data.deep_size_of_children(context)
        }
    }

    struct SharedTestKey(u64);

    impl CacheKey for SharedTestKey {
        type ValueType = SharedTestValue;

        fn key(&self) -> Cow<'_, str> {
            self.0.to_string().into()
        }

        fn type_name() -> &'static str {
            "test.SharedValue"
        }

        fn schema() -> CacheKeySchema {
            CacheKeySchema::new("test.shared-value-key", 1)
        }

        fn write_key(&self, builder: &mut KeyBuilder) {
            builder.write_u64(self.0);
        }
    }

    struct ReentrantValue(LanceCache);

    impl DeepSizeOf for ReentrantValue {
        fn deep_size_of_children(&self, context: &mut Context) -> usize {
            self.0.deep_size_of_children(context)
        }
    }

    struct ReentrantKey;

    impl CacheKey for ReentrantKey {
        type ValueType = ReentrantValue;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed("reentrant")
        }

        fn type_name() -> &'static str {
            "test.ReentrantValue"
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum TestBackendKind {
        Moka,
        Quick,
    }

    impl TestBackendKind {
        fn cache(self, capacity: usize) -> LanceCache {
            match self {
                Self::Moka => LanceCache::with_capacity(capacity),
                Self::Quick => {
                    LanceCache::with_backend(Arc::new(QuickCacheBackend::with_capacity(capacity)))
                }
            }
        }
    }

    struct LegacyBridgeKey(&'static str);

    impl CacheKey for LegacyBridgeKey {
        type ValueType = Vec<u32>;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "test.LegacyBridge"
        }
    }

    struct ExplicitBridgeKey(&'static str);

    impl CacheKey for ExplicitBridgeKey {
        type ValueType = Vec<u32>;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "test.LegacyBridge"
        }

        fn write_key(&self, builder: &mut KeyBuilder) {
            builder.write_str(self.0);
        }
    }

    trait TestDynValue: DeepSizeOf + Send + Sync {
        fn values(&self) -> &[u32];
    }

    impl TestDynValue for Vec<u32> {
        fn values(&self) -> &[u32] {
            self
        }
    }

    struct LegacyUnsizedBridgeKey(&'static str);

    impl UnsizedCacheKey for LegacyUnsizedBridgeKey {
        type ValueType = dyn TestDynValue;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "test.LegacyUnsizedBridge"
        }
    }

    struct ExplicitUnsizedBridgeKey(&'static str);

    impl UnsizedCacheKey for ExplicitUnsizedBridgeKey {
        type ValueType = dyn TestDynValue;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed(self.0)
        }

        fn type_name() -> &'static str {
            "test.LegacyUnsizedBridge"
        }

        fn write_key(&self, builder: &mut KeyBuilder) {
            builder.write_str(self.0);
        }
    }

    #[derive(Debug, Default)]
    struct HashMapBackend {
        entries: tokio::sync::Mutex<HashMap<InternalCacheKey, (CacheEntry, usize)>>,
    }

    #[async_trait::async_trait]
    impl CacheBackend for HashMapBackend {
        async fn get(
            &self,
            key: &InternalCacheKey,
            _codec: Option<CacheCodec>,
        ) -> Option<CacheEntry> {
            self.entries
                .lock()
                .await
                .get(key)
                .map(|(entry, _)| entry.clone())
        }

        async fn insert(
            &self,
            key: &InternalCacheKey,
            entry: CacheEntry,
            size_bytes: usize,
            _codec: Option<CacheCodec>,
        ) {
            self.entries.lock().await.insert(*key, (entry, size_bytes));
        }

        async fn get_or_insert<'a>(
            &self,
            key: &InternalCacheKey,
            loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
            codec: Option<CacheCodec>,
        ) -> Result<(CacheEntry, bool)> {
            if let Some(entry) = self.get(key, codec).await {
                return Ok((entry, true));
            }
            let (entry, size_bytes) = loader.await?;
            self.insert(key, entry.clone(), size_bytes, codec).await;
            Ok((entry, false))
        }

        async fn clear(&self) {
            self.entries.lock().await.clear();
        }

        async fn num_entries(&self) -> usize {
            self.entries.lock().await.len()
        }

        async fn size_bytes(&self) -> usize {
            self.entries
                .lock()
                .await
                .values()
                .map(|(_, size_bytes)| size_bytes)
                .sum()
        }
    }

    #[derive(Debug)]
    struct WrongTypeBackend;

    #[async_trait::async_trait]
    impl CacheBackend for WrongTypeBackend {
        async fn get(
            &self,
            _key: &InternalCacheKey,
            _codec: Option<CacheCodec>,
        ) -> Option<CacheEntry> {
            Some(Arc::new(String::from("wrong type")))
        }

        async fn insert(
            &self,
            _key: &InternalCacheKey,
            _entry: CacheEntry,
            _size_bytes: usize,
            _codec: Option<CacheCodec>,
        ) {
        }

        async fn get_or_insert<'a>(
            &self,
            _key: &InternalCacheKey,
            _loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
            _codec: Option<CacheCodec>,
        ) -> Result<(CacheEntry, bool)> {
            Ok((Arc::new(String::from("wrong type")), true))
        }

        async fn clear(&self) {}

        async fn num_entries(&self) -> usize {
            0
        }

        async fn size_bytes(&self) -> usize {
            0
        }
    }

    #[tokio::test]
    async fn typed_roundtrip_stats_clear_and_namespace_isolation() {
        let cache = LanceCache::with_capacity(4096);
        let left = cache.with_key_prefix("left");
        let right = cache.with_key_prefix("right");
        left.insert_with_key(&TestKey::new(7), Arc::new(vec![1, 2, 3]))
            .await;

        assert_eq!(
            left.get_with_key(&TestKey::new(7)).await.as_deref(),
            Some(&vec![1, 2, 3])
        );
        assert!(right.get_with_key(&TestKey::new(7)).await.is_none());
        let stats = cache.stats().await;
        assert_eq!((stats.hits, stats.misses, stats.num_entries), (1, 1, 1));

        cache.clear().await;
        let stats = left.stats().await;
        assert_eq!((stats.hits, stats.misses, stats.num_entries), (0, 0, 0));
    }

    #[tokio::test]
    async fn strong_and_weak_handles_share_state_and_namespace() {
        let cache = LanceCache::with_capacity(4096);
        let child = cache.with_key_prefix("child");
        let weak = WeakLanceCache::from(&child);

        assert!(
            weak.insert_with_key(&TestKey::new(1), Arc::new(vec![1]))
                .await
        );
        assert_eq!(
            child.get_with_key(&TestKey::new(1)).await.as_deref(),
            Some(&vec![1])
        );
        child
            .insert_with_key(&TestKey::new(2), Arc::new(vec![2]))
            .await;
        assert_eq!(
            weak.get_with_key(&TestKey::new(2)).await.as_deref(),
            Some(&vec![2])
        );
        assert_eq!((cache.stats().await.hits, cache.size().await), (2, 2));
    }

    #[tokio::test]
    async fn nested_namespace_segments_do_not_alias_combined_segments() {
        let cache = LanceCache::with_capacity(4096);
        let nested = cache.with_key_prefix("a").with_key_prefix("b");
        let combined = cache.with_key_prefix("a/b");
        nested
            .insert_with_key(&TestKey::new(1), Arc::new(vec![10]))
            .await;
        assert!(combined.get_with_key(&TestKey::new(1)).await.is_none());
    }

    #[tokio::test]
    async fn schema_change_produces_a_cold_miss() {
        let cache = LanceCache::with_capacity(4096);
        cache
            .insert_with_key(&TestKey::new(1), Arc::new(vec![10]))
            .await;
        assert!(cache.get_with_key(&TestKeyV2::new(1)).await.is_none());
    }

    #[tokio::test]
    async fn get_or_insert_with_key_hit_reports_loader_execution() {
        let cache = LanceCache::with_capacity(4096);

        // Cold: loader runs, was_cached = false.
        let (value, was_cached) = cache
            .get_or_insert_with_key_hit(TestKey::new(1), || async { Ok(vec![1, 2, 3]) })
            .await
            .unwrap();
        assert_eq!(*value, vec![1, 2, 3]);
        assert!(!was_cached);

        // Warm: loader must not run and was_cached = true.
        let (value, was_cached) = cache
            .get_or_insert_with_key_hit(TestKey::new(1), || async {
                panic!("should not be called")
            })
            .await
            .unwrap();
        assert_eq!(*value, vec![1, 2, 3]);
        assert!(was_cached);
    }

    #[tokio::test]
    async fn default_string_bridge_matches_explicit_legacy_encoding() {
        let cache = LanceCache::with_capacity(4096);
        cache
            .insert_with_key(&LegacyBridgeKey("same"), Arc::new(vec![10]))
            .await;
        assert_eq!(
            cache
                .get_with_key(&ExplicitBridgeKey("same"))
                .await
                .as_deref(),
            Some(&vec![10])
        );
    }

    #[tokio::test]
    async fn unsized_default_string_bridge_matches_explicit_legacy_encoding() {
        let cache = LanceCache::with_capacity(4096);
        let value: Arc<dyn TestDynValue> = Arc::new(vec![10, 20]);
        cache
            .insert_unsized_with_key(&LegacyUnsizedBridgeKey("same"), value)
            .await;

        let cached = cache
            .get_unsized_with_key(&ExplicitUnsizedBridgeKey("same"))
            .await
            .unwrap();
        assert_eq!(cached.values(), &[10, 20]);
    }

    #[tokio::test]
    async fn custom_backend_receives_opaque_keys_and_shared_clear() {
        let backend = Arc::new(HashMapBackend::default());
        let cache = LanceCache::with_backend(backend.clone());
        let child = cache.with_key_prefix("child");
        let value = Arc::new(vec![1, 2, 3]);
        let value_size = cache_entry_size(value.as_ref());

        child.insert_with_key(&TestKey::new(7), value).await;
        assert_eq!(
            child.get_with_key(&TestKey::new(7)).await.as_deref(),
            Some(&vec![1, 2, 3])
        );
        assert_eq!(backend.entries.lock().await.len(), 1);
        assert_eq!(cache.size_bytes().await, value_size);

        cache.clear().await;
        assert!(backend.entries.lock().await.is_empty());
        assert_eq!(child.stats().await.hits, 0);
    }

    #[tokio::test]
    async fn backend_type_collisions_are_contextual_misses_or_errors() {
        let cache = LanceCache::with_backend(Arc::new(WrongTypeBackend));

        assert!(cache.get_with_key(&TestKey::new(1)).await.is_none());
        let error = cache
            .get_or_insert_with_key(TestKey::new(2), || async { Ok(vec![2]) })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("test.VecU32"));
        let stats = cache.stats().await;
        assert_eq!((stats.hits, stats.misses), (0, 2));
    }

    #[tokio::test]
    async fn moka_weight_includes_the_fixed_physical_key() {
        let value = Arc::new(vec![0_u32; 3]);
        let expected = cache_entry_size(value.as_ref())
            .checked_add(std::mem::size_of::<InternalCacheKey>())
            .unwrap();
        let cache = LanceCache::with_capacity(expected * 2);
        cache.insert_with_key(&TestKey::new(1), value).await;
        assert_eq!(cache.size_bytes().await, expected);
    }

    #[rstest::rstest]
    #[case::moka(TestBackendKind::Moka)]
    #[case::quick(TestBackendKind::Quick)]
    #[tokio::test]
    async fn deep_size_deduplicates_shared_entry_allocations(
        #[case] backend_kind: TestBackendKind,
    ) {
        let cache = backend_kind.cache(1 << 20);
        let shared_data = Arc::new(vec![0_u8; 1024]);

        for id in 0..2 {
            let data = shared_data.clone();
            cache
                .get_or_insert_with_key(SharedTestKey(id), || async move {
                    Ok(SharedTestValue { data })
                })
                .await
                .unwrap();
        }

        let arc_overhead = std::mem::size_of::<AtomicUsize>() * 2;
        let shared_allocation = std::mem::size_of::<Vec<u8>>() + shared_data.capacity();
        let expected_entries = 2 * std::mem::size_of::<InternalCacheKey>()
            + 2 * (std::mem::size_of::<SharedTestValue>() + arc_overhead)
            + shared_allocation;

        let weighted_size = cache.size_bytes().await;
        assert_eq!(weighted_size, expected_entries + shared_allocation);
        assert_eq!(
            cache.deep_size_of(),
            std::mem::size_of::<LanceCache>() + expected_entries
        );

        let mut context = Context::new();
        assert_eq!(cache.deep_size_of_children(&mut context), expected_entries);
        assert_eq!(
            cache
                .with_key_prefix("another-handle")
                .deep_size_of_children(&mut context),
            0
        );
    }

    #[test]
    fn sizing_can_reenter_the_same_cache() {
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let cache = LanceCache::with_capacity(4096);
                cache
                    .insert_with_key(&ReentrantKey, Arc::new(ReentrantValue(cache.clone())))
                    .await;
                done_tx.send(()).unwrap();
            });
        });

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cache insertion deadlocked during sizing");
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn no_cache_computes_each_time() {
        let cache = LanceCache::no_cache();
        let loads = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let loads = loads.clone();
            let value = cache
                .get_or_insert_with_key(TestKey::new(1), move || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![42])
                })
                .await
                .unwrap();
            assert_eq!(value.as_slice(), &[42]);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 2);
        assert_eq!(cache.size().await, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_coalesces_success_after_contenders_are_parked() {
        const CONTENDERS: usize = 4;

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let owner = {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            let release = release.clone();
            tokio::spawn(async move {
                cache
                    .get_or_insert_with_key(TestKey::new(10), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        let _ = started_tx.send(());
                        release.notified().await;
                        Ok(vec![10])
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let mut contenders = Vec::new();
        let mut parked = Vec::new();
        for _ in 0..CONTENDERS {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
            parked.push(parked_rx);
            contenders.push(tokio::spawn(async move {
                report_first_pending(
                    cache.get_or_insert_with_key(TestKey::new(10), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![99])
                    }),
                    parked_tx,
                )
                .await
            }));
        }
        for parked in parked {
            parked
                .await
                .expect("contender completed instead of parking behind owner");
        }
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert!(contenders.iter().all(|handle| !handle.is_finished()));

        release.notify_one();
        assert_eq!(owner.await.unwrap().unwrap().as_slice(), &[10]);
        for contender in contenders {
            assert_eq!(contender.await.unwrap().unwrap().as_slice(), &[10]);
        }
        let stats = cache.stats().await;
        assert_eq!((stats.hits, stats.misses), (CONTENDERS as u64, 1),);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_coalesces_errors_after_contenders_are_parked() {
        const CONTENDERS: usize = 4;

        let cache = Arc::new(LanceCache::with_capacity(4096));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let owner = {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            let release = release.clone();
            tokio::spawn(async move {
                cache
                    .get_or_insert_with_key(TestKey::new(20), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        let _ = started_tx.send(());
                        release.notified().await;
                        Err(Error::timeout("owner loader timed out"))
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let mut contenders = Vec::new();
        let mut parked = Vec::new();
        for _ in 0..CONTENDERS {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
            parked.push(parked_rx);
            contenders.push(tokio::spawn(async move {
                report_first_pending(
                    cache.get_or_insert_with_key(TestKey::new(20), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        Err(Error::timeout("contender loader timed out"))
                    }),
                    parked_tx,
                )
                .await
            }));
        }
        for parked in parked {
            parked
                .await
                .expect("contender completed instead of parking behind owner");
        }
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert!(contenders.iter().all(|handle| !handle.is_finished()));

        release.notify_one();
        assert!(matches!(owner.await.unwrap(), Err(Error::Timeout { .. })));
        for contender in contenders {
            assert!(matches!(
                contender.await.unwrap(),
                Err(Error::Timeout { .. })
            ));
        }
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_retries_after_the_owner_is_cancelled() {
        let cache = Arc::new(LanceCache::with_capacity(4096));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();

        let owner = {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            tokio::spawn(async move {
                cache
                    .get_or_insert_with_key(TestKey::new(30), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        let _ = started_tx.send(());
                        std::future::pending::<()>().await;
                        Ok(vec![30])
                    })
                    .await
            })
        };
        started_rx.await.unwrap();

        let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
        let contender = {
            let cache = cache.clone();
            let loader_calls = loader_calls.clone();
            tokio::spawn(async move {
                report_first_pending(
                    cache.get_or_insert_with_key(TestKey::new(30), move || async move {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![31])
                    }),
                    parked_tx,
                )
                .await
            })
        };
        parked_rx
            .await
            .expect("contender completed instead of parking behind owner");
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert!(!contender.is_finished());

        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        let value = tokio::time::timeout(std::time::Duration::from_secs(5), contender)
            .await
            .expect("contender remained parked after owner cancellation")
            .unwrap()
            .unwrap();
        assert_eq!(value.as_slice(), &[31]);
        assert_eq!(loader_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_weak_cache_degrades_without_retaining_state() {
        let cache = LanceCache::with_capacity(4096);
        let weak = WeakLanceCache::from(&cache);
        drop(cache);

        assert!(weak.get_with_key(&TestKey::new(1)).await.is_none());
        assert!(
            !weak
                .insert_with_key(&TestKey::new(1), Arc::new(vec![1]))
                .await
        );
        let value = weak
            .get_or_insert_with_key(TestKey::new(1), || async { Ok(vec![7]) })
            .await
            .unwrap();
        assert_eq!(value.as_slice(), &[7]);
    }
}
