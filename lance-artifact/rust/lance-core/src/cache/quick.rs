// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! [`CacheBackend`] backed by [quick_cache](https://crates.io/crates/quick_cache),
//! whose hit path is one atomic bit — no read-op channel or inline
//! housekeeping. Used for the session index and metadata caches; the index
//! cache sees thousands of cache reads per query.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Future;

use super::backend::{CacheBackend, CacheEntry};
use super::moka::key_footprint;
use super::{CacheCodec, InternalCacheKey};
use crate::Result;
use crate::deepsize::Context;

#[derive(Clone)]
struct QuickEntry {
    entry: CacheEntry,
    size_bytes: usize,
}

#[derive(Clone)]
struct EntryWeighter;

impl quick_cache::Weighter<InternalCacheKey, QuickEntry> for EntryWeighter {
    fn weight(&self, key: &InternalCacheKey, value: &QuickEntry) -> u64 {
        // Same accounting as the moka backend.
        key_footprint(key).saturating_add(value.size_bytes).max(1) as u64
    }
}

pub struct QuickCacheBackend {
    cache: quick_cache::sync::Cache<InternalCacheKey, QuickEntry, EntryWeighter>,
}

impl std::fmt::Debug for QuickCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickCacheBackend")
            .field("entry_count", &self.cache.len())
            .finish()
    }
}

/// Minimum weight budget (4 GiB) per shard: shards don't borrow capacity, and
/// an entry heavier than ~its shard's budget is silently refused admission.
const MIN_SHARD_SHARE: usize = 4 << 30;

/// Recommended shard count: `min(cpus / 2, capacity / 4 GiB)`, power of two
/// in `[1, 1024]`. The cpu term bounds lock contention; the capacity term
/// keeps each shard's budget >= 4 GiB so large entries stay admissible.
/// Rounded down because quick_cache rounds requests up.
pub fn recommended_cache_shards(capacity: usize) -> usize {
    let by_cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        / 2;
    let shards = (capacity / MIN_SHARD_SHARE).min(by_cpu).max(1);
    let shards = if shards.is_power_of_two() {
        shards
    } else {
        shards.next_power_of_two() / 2
    };
    shards.clamp(1, 1024)
}

/// Assumed average entry size for pre-allocation sizing.
const ESTIMATED_AVG_ENTRY_BYTES: usize = 64 << 10;

impl QuickCacheBackend {
    /// Create a backend holding up to `capacity` bytes of weighted entries
    /// (weight = key footprint + declared size), sharded per
    /// [`recommended_cache_shards`].
    pub fn with_capacity(capacity: usize) -> Self {
        let shards = recommended_cache_shards(capacity);
        // Floor protects the shard count from quick_cache's items-per-shard
        // heuristic; ceiling bounds pre-allocation.
        let estimated_items = (capacity / ESTIMATED_AVG_ENTRY_BYTES).clamp(shards * 32, 1_000_000);
        let options = quick_cache::OptionsBuilder::new()
            .estimated_items_capacity(estimated_items)
            .weight_capacity(capacity as u64)
            .shards(shards)
            .build()
            // Only errors when weight/item capacity is missing; both are set.
            .expect("quick_cache options");
        let cache = quick_cache::sync::Cache::with_options(
            options,
            EntryWeighter,
            Default::default(),
            Default::default(),
        );
        Self { cache }
    }
}

#[async_trait]
impl CacheBackend for QuickCacheBackend {
    async fn get(&self, key: &InternalCacheKey, _codec: Option<CacheCodec>) -> Option<CacheEntry> {
        self.cache.get(key).map(|v| v.entry)
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        _codec: Option<CacheCodec>,
    ) {
        self.cache.insert(*key, QuickEntry { entry, size_bytes });
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        _codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        match self.cache.get_value_or_guard_async(key).await {
            Ok(value) => Ok((value.entry, true)),
            Err(guard) => {
                let (entry, size_bytes) = loader.await?;
                let _ = guard.insert(QuickEntry {
                    entry: entry.clone(),
                    size_bytes,
                });
                Ok((entry, false))
            }
        }
    }

    async fn clear(&self) {
        self.cache.clear();
    }

    async fn num_entries(&self) -> usize {
        self.cache.len()
    }

    async fn size_bytes(&self) -> usize {
        self.cache.weight() as usize
    }

    fn approx_num_entries(&self) -> usize {
        self.cache.len()
    }

    fn approx_size_bytes(&self) -> usize {
        self.cache.weight() as usize
    }

    fn deep_size_of_entries(
        &self,
        context: &mut Context,
        size_of_entry: &dyn Fn(&CacheEntry, &mut Context) -> Option<usize>,
    ) -> Option<usize> {
        Some(
            self.cache
                .iter()
                .map(|(key, record)| {
                    key_footprint(&key)
                        + size_of_entry(&record.entry, context).unwrap_or(record.size_bytes)
                })
                .sum(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::cache::{CacheKey, LanceCache};

    struct TestKey<T: 'static> {
        key: String,
        _phantom: PhantomData<T>,
    }

    impl<T: 'static> TestKey<T> {
        fn new(key: &str) -> Self {
            Self {
                key: key.to_string(),
                _phantom: PhantomData,
            }
        }
    }

    impl<T: 'static> CacheKey for TestKey<T> {
        type ValueType = T;
        fn key(&self) -> std::borrow::Cow<'_, str> {
            std::borrow::Cow::Borrowed(&self.key)
        }
        fn type_name() -> &'static str {
            std::any::type_name::<T>()
        }
    }

    #[test]
    fn entry_weight_includes_fixed_key() {
        let key = InternalCacheKey::from_bytes([0; 16]);
        let entry = QuickEntry {
            entry: Arc::new(()),
            size_bytes: 7,
        };
        assert_eq!(
            quick_cache::Weighter::weight(&EntryWeighter, &key, &entry),
            23
        );
    }

    #[tokio::test]
    async fn test_quick_backend_roundtrip_singleflight_and_eviction() {
        // Capacity must be large relative to one entry: quick_cache shards
        // its weight budget, and an entry heavier than its shard's share is
        // not admitted at all.
        const CAPACITY: usize = 1 << 20;
        let item = Arc::new(vec![1u8, 2, 3]);
        let cache = LanceCache::with_backend(Arc::new(QuickCacheBackend::with_capacity(CAPACITY)));

        // insert + get roundtrip and weighted accounting
        cache
            .insert_with_key(&TestKey::<Vec<u8>>::new("a"), item.clone())
            .await;
        assert_eq!(
            cache
                .get_with_key(&TestKey::<Vec<u8>>::new("a"))
                .await
                .as_deref(),
            Some(&vec![1u8, 2, 3])
        );
        assert_eq!(cache.approx_size(), 1);
        assert!(cache.size_bytes().await > 0);

        // get_or_insert runs the loader only on a miss
        let loads = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let loads = loads.clone();
            let value = cache
                .get_or_insert_with_key(TestKey::<Vec<u8>>::new("b"), || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![7u8])
                })
                .await
                .unwrap();
            assert_eq!(value.as_ref(), &vec![7u8]);
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        // capacity is enforced: overfill with 4x capacity of 16KiB entries
        // and confirm eviction kept the weighted size within budget
        for i in 0..256 {
            cache
                .insert_with_key(
                    &TestKey::<Vec<u8>>::new(&format!("fill-{i}")),
                    Arc::new(vec![0u8; 16 << 10]),
                )
                .await;
        }
        assert!(cache.size_bytes().await <= CAPACITY);
        assert!(cache.size().await < 258);

        cache.clear().await;
        assert_eq!(cache.size().await, 0);
    }

    #[tokio::test]
    async fn test_quick_backend_tiny_capacity() {
        // A tiny cache must not over-provision item metadata and must still
        // admit and evict correctly within its weight budget.
        const CAPACITY: usize = 64 << 10;
        let cache = LanceCache::with_backend(Arc::new(QuickCacheBackend::with_capacity(CAPACITY)));
        for i in 0..64 {
            cache
                .insert_with_key(
                    &TestKey::<Vec<u8>>::new(&format!("k-{i}")),
                    Arc::new(vec![0u8; 4 << 10]),
                )
                .await;
        }
        assert!(cache.size_bytes().await <= CAPACITY);
        assert!(cache.size().await >= 1);
        let hit = cache
            .get_with_key(&TestKey::<Vec<u8>>::new("k-63"))
            .await
            .is_some()
            || cache
                .get_with_key(&TestKey::<Vec<u8>>::new("k-62"))
                .await
                .is_some();
        assert!(hit, "recently inserted entries should be resident");
    }
}
