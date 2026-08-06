// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::any::Any;
use std::borrow::Cow;
use std::hash::{BuildHasher, RandomState};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::FutureExt;
use lance_core::cache::{
    CacheKey, CacheKeySchema, CacheNamespace, KeyBuilder, LanceCache, WeakLanceCache,
};

struct PageKey {
    column_index: u32,
    page_index: u64,
}

impl CacheKey for PageKey {
    type ValueType = Vec<u8>;

    fn key(&self) -> Cow<'_, str> {
        format!("{}-{}", self.column_index, self.page_index).into()
    }

    fn type_name() -> &'static str {
        "bench.Page"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("bench.page-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.column_index);
        builder.write_u64(self.page_index);
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct LegacyPhysicalKey {
    namespace: Arc<str>,
    logical_key: Arc<str>,
    type_name: &'static str,
}

type LegacyEntryValue = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
struct LegacyEntry {
    value: LegacyEntryValue,
    size_bytes: usize,
}

#[async_trait]
trait LegacyBackend: Send + Sync {
    async fn get(&self, key: &LegacyPhysicalKey) -> Option<LegacyEntryValue>;
    async fn insert(&self, key: &LegacyPhysicalKey, value: LegacyEntryValue, size_bytes: usize);
}

struct LegacyMokaBackend {
    cache: moka::future::Cache<LegacyPhysicalKey, LegacyEntry>,
}

impl LegacyMokaBackend {
    fn with_capacity(capacity: usize) -> Self {
        let cache = moka::future::Cache::builder()
            .max_capacity(capacity as u64)
            .weigher(|key: &LegacyPhysicalKey, entry: &LegacyEntry| {
                std::mem::size_of::<LegacyPhysicalKey>()
                    .saturating_add(key.logical_key.len())
                    .saturating_add(entry.size_bytes)
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .support_invalidation_closures()
            .build();
        Self { cache }
    }
}

#[async_trait]
impl LegacyBackend for LegacyMokaBackend {
    async fn get(&self, key: &LegacyPhysicalKey) -> Option<LegacyEntryValue> {
        self.cache.get(key).await.map(|entry| entry.value)
    }

    async fn insert(&self, key: &LegacyPhysicalKey, value: LegacyEntryValue, size_bytes: usize) {
        self.cache
            .insert(key.clone(), LegacyEntry { value, size_bytes })
            .await;
    }
}

#[derive(Clone)]
struct LegacyCache {
    backend: Arc<dyn LegacyBackend>,
    namespace: Arc<str>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl LegacyCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            backend: Arc::new(LegacyMokaBackend::with_capacity(capacity)),
            namespace: Arc::from(""),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    fn with_key_prefix(&self, segment: &str) -> Self {
        Self {
            backend: self.backend.clone(),
            namespace: Arc::from(format!("{}{segment}/", self.namespace)),
            hits: self.hits.clone(),
            misses: self.misses.clone(),
        }
    }

    fn physical_key(&self, key: &PageKey) -> LegacyPhysicalKey {
        let logical_key = key.key();
        LegacyPhysicalKey {
            namespace: self.namespace.clone(),
            logical_key: Arc::from(logical_key.as_ref()),
            type_name: PageKey::type_name(),
        }
    }

    async fn insert(&self, key: &PageKey, value: Arc<Vec<u8>>) {
        let size_bytes = std::mem::size_of::<Vec<u8>>()
            + value.capacity()
            + std::mem::size_of::<AtomicU64>() * 2;
        self.backend
            .insert(&self.physical_key(key), value, size_bytes)
            .boxed()
            .await;
    }

    async fn get(&self, key: &PageKey) -> Option<Arc<Vec<u8>>> {
        async {
            let Some(value) = self.backend.get(&self.physical_key(key)).await else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            match value.downcast::<Vec<u8>>() {
                Ok(value) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    Some(value)
                }
                Err(_) => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        }
        .boxed()
        .await
    }
}

struct LegacyWeakCache {
    backend: Weak<dyn LegacyBackend>,
    namespace: Arc<str>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl LegacyWeakCache {
    fn from(cache: &LegacyCache) -> Self {
        Self {
            backend: Arc::downgrade(&cache.backend),
            namespace: cache.namespace.clone(),
            hits: cache.hits.clone(),
            misses: cache.misses.clone(),
        }
    }

    async fn get(&self, key: &PageKey) -> Option<Arc<Vec<u8>>> {
        let backend = self.backend.upgrade()?;
        let logical_key = key.key();
        let physical_key = LegacyPhysicalKey {
            namespace: self.namespace.clone(),
            logical_key: Arc::from(logical_key.as_ref()),
            type_name: PageKey::type_name(),
        };
        let Some(value) = backend.get(&physical_key).await else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        match value.downcast::<Vec<u8>>() {
            Ok(value) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(value)
            }
            Err(_) => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

fn benchmark_key_preparation(c: &mut Criterion) {
    let key = PageKey {
        column_index: 17,
        page_index: 42,
    };
    let outer_hashes = RandomState::new();
    let prefixes: [(&str, Arc<str>); 2] = [
        ("short", Arc::from("dataset")),
        ("long", Arc::from("p".repeat(1024))),
    ];
    let mut group = c.benchmark_group("cache_key_preparation");

    for (case, prefix) in prefixes {
        let namespace = CacheNamespace::root().child(&prefix);

        group.bench_with_input(BenchmarkId::new("legacy", case), &prefix, |b, prefix| {
            b.iter(|| {
                let logical_key = key.key();
                let physical_key = LegacyPhysicalKey {
                    namespace: Arc::clone(prefix),
                    logical_key: Arc::from(logical_key.as_ref()),
                    type_name: PageKey::type_name(),
                };
                black_box(outer_hashes.hash_one(physical_key))
            })
        });

        group.bench_function(BenchmarkId::new("blake3_typed", case), |b| {
            b.iter(|| {
                let mut builder =
                    KeyBuilder::new(namespace, PageKey::stable_type_id(), PageKey::schema());
                key.write_key(&mut builder);
                black_box(outer_hashes.hash_one(builder.finish()))
            })
        });
    }

    group.finish();
}

fn benchmark_namespace_derivation(c: &mut Criterion) {
    let root = CacheNamespace::root();
    let long_segment = "p".repeat(160);
    let mut group = c.benchmark_group("cache_namespace_derivation");

    group.bench_function("root", |b| {
        b.iter(|| black_box(CacheNamespace::root()));
    });
    group.bench_with_input(
        BenchmarkId::new("child", "short"),
        &"dataset",
        |b, segment| {
            b.iter(|| black_box(root.child(black_box(segment))));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("child", "long"),
        &long_segment,
        |b, segment| {
            b.iter(|| black_box(root.child(black_box(segment))));
        },
    );

    group.finish();
}

fn benchmark_cache_operations(c: &mut Criterion) {
    const CAPACITY: usize = 64 * 1024;
    const ROTATING_KEYS: u64 = 512;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let prefix = "p".repeat(1024);
    let legacy = LegacyCache::with_capacity(CAPACITY).with_key_prefix(&prefix);
    let legacy_weak = LegacyWeakCache::from(&legacy);
    let fixed = LanceCache::with_capacity(CAPACITY).with_key_prefix(&prefix);
    let fixed_weak = WeakLanceCache::from(&fixed);
    let hit_key = PageKey {
        column_index: 17,
        page_index: 42,
    };
    runtime.block_on(async {
        legacy.insert(&hit_key, Arc::new(vec![1_u8; 32])).await;
        fixed
            .insert_with_key(&hit_key, Arc::new(vec![1_u8; 32]))
            .await;
    });

    let mut group = c.benchmark_group("cache_operations");
    group.bench_function(BenchmarkId::new("strong_warmed_hit", "legacy"), |b| {
        b.to_async(&runtime)
            .iter(|| legacy.get(black_box(&hit_key)));
    });
    group.bench_function(BenchmarkId::new("strong_warmed_hit", "fixed"), |b| {
        b.to_async(&runtime)
            .iter(|| fixed.get_with_key(black_box(&hit_key)));
    });
    group.bench_function(BenchmarkId::new("weak_warmed_hit", "legacy"), |b| {
        b.to_async(&runtime)
            .iter(|| legacy_weak.get(black_box(&hit_key)));
    });
    group.bench_function(BenchmarkId::new("weak_warmed_hit", "fixed"), |b| {
        b.to_async(&runtime)
            .iter(|| fixed_weak.get_with_key(black_box(&hit_key)));
    });

    let values: Vec<_> = (0..16).map(|value| Arc::new(vec![value; 32])).collect();
    let next_legacy_insert = AtomicU64::new(0);
    group.bench_function(BenchmarkId::new("bounded_rotating_insert", "legacy"), |b| {
        b.to_async(&runtime).iter(|| {
            let sequence = next_legacy_insert.fetch_add(1, Ordering::Relaxed);
            let page_index = sequence % ROTATING_KEYS;
            let value = Arc::clone(&values[sequence as usize % values.len()]);
            let legacy = &legacy;
            async move {
                legacy
                    .insert(
                        &PageKey {
                            column_index: 17,
                            page_index,
                        },
                        value,
                    )
                    .await;
            }
        });
    });
    let next_fixed_insert = AtomicU64::new(0);
    group.bench_function(BenchmarkId::new("bounded_rotating_insert", "fixed"), |b| {
        b.to_async(&runtime).iter(|| {
            let sequence = next_fixed_insert.fetch_add(1, Ordering::Relaxed);
            let page_index = sequence % ROTATING_KEYS;
            let value = Arc::clone(&values[sequence as usize % values.len()]);
            let fixed = &fixed;
            async move {
                fixed
                    .insert_with_key(
                        &PageKey {
                            column_index: 17,
                            page_index,
                        },
                        value,
                    )
                    .await;
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_key_preparation,
    benchmark_namespace_derivation,
    benchmark_cache_operations
);
criterion_main!(benches);
