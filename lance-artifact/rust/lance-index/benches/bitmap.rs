// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of Bitmap scalar index.
//!
//! This benchmark measures the performance of Bitmap index with:
//! - 50 million data points
//! - Int64 and String data types
//! - High cardinality (unique values) and low cardinality (100 unique values)
//! - Equality filters
//! - IN filters with varying size (1, 3, 5 values)

mod common;

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use common::{LOW_CARDINALITY_COUNT, TOTAL_ROWS};
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use lance_core::cache::LanceCache;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::pbold;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::ScalarIndexPlugin;
use lance_index::scalar::{SargableQuery, ScalarIndex, bitmap::BitmapIndexPlugin};
use lance_io::object_store::ObjectStore;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use object_store::path::Path;

// Lazy static runtime - only created once
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

// Lazy static cache - only created when cached benchmarks are run
static CACHE: OnceLock<Arc<LanceCache>> = OnceLock::new();

// Lazy static indices - only created when first accessed
// Separate indices for cached and uncached variants
static INT_UNIQUE_INDEX_NO_CACHE: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static INT_UNIQUE_INDEX_CACHED: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static INT_LOW_CARD_INDEX_NO_CACHE: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static INT_LOW_CARD_INDEX_CACHED: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static STRING_UNIQUE_INDEX_NO_CACHE: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static STRING_UNIQUE_INDEX_CACHED: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static STRING_LOW_CARD_INDEX_NO_CACHE: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();
static STRING_LOW_CARD_INDEX_CACHED: OnceLock<Arc<dyn ScalarIndex>> = OnceLock::new();

/// Get or create the tokio runtime
fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Builder::new_multi_thread().build().unwrap())
}

/// Get the cache - either a singleton cache or no_cache based on use_cache parameter
fn get_cache(use_cache: bool, key_prefix: &str) -> Arc<LanceCache> {
    if use_cache {
        Arc::new(
            CACHE
                .get_or_init(|| Arc::new(LanceCache::with_capacity(1024 * 1024 * 1024)))
                .with_key_prefix(key_prefix),
        )
    } else {
        Arc::new(LanceCache::no_cache())
    }
}

/// Create and train a Bitmap index for int64 data with unique values
async fn create_int_unique_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_int_unique_stream();

    BitmapIndexPlugin::train_bitmap_index(stream, store.as_ref())
        .await
        .unwrap();

    let details = prost_types::Any::from_msg(&pbold::BitmapIndexDetails::default()).unwrap();

    (BitmapIndexPlugin
        .load_index(store, &details, None, &get_cache(use_cache, "int_unique"))
        .await
        .unwrap()) as _
}

/// Create and train a Bitmap index for int64 data with low cardinality
async fn create_int_low_card_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_int_low_cardinality_stream();

    BitmapIndexPlugin::train_bitmap_index(stream, store.as_ref())
        .await
        .unwrap();

    let details = prost_types::Any::from_msg(&pbold::BitmapIndexDetails::default()).unwrap();

    (BitmapIndexPlugin
        .load_index(store, &details, None, &get_cache(use_cache, "int_low_card"))
        .await
        .unwrap()) as _
}

/// Create and train a Bitmap index for string data with unique values
async fn create_string_unique_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_string_unique_stream();

    BitmapIndexPlugin::train_bitmap_index(stream, store.as_ref())
        .await
        .unwrap();

    let details = prost_types::Any::from_msg(&pbold::BitmapIndexDetails::default()).unwrap();

    (BitmapIndexPlugin
        .load_index(
            store,
            &details,
            None,
            &get_cache(use_cache, "string_unique"),
        )
        .await
        .unwrap()) as _
}

/// Create and train a Bitmap index for string data with low cardinality
async fn create_string_low_card_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_string_low_cardinality_stream();

    BitmapIndexPlugin::train_bitmap_index(stream, store.as_ref())
        .await
        .unwrap();

    let details = prost_types::Any::from_msg(&pbold::BitmapIndexDetails::default()).unwrap();

    (BitmapIndexPlugin
        .load_index(
            store,
            &details,
            None,
            &get_cache(use_cache, "string_low_card"),
        )
        .await
        .unwrap()) as _
}

/// Set up all benchmark indices
/// Setup function for int unique index - creates it only once per cache variant
fn setup_int_unique_index(rt: &tokio::runtime::Runtime, use_cache: bool) -> Arc<dyn ScalarIndex> {
    let static_ref = if use_cache {
        &INT_UNIQUE_INDEX_CACHED
    } else {
        &INT_UNIQUE_INDEX_NO_CACHE
    };

    static_ref
        .get_or_init(|| {
            rt.block_on(async {
                let tempdir = tempfile::tempdir().unwrap();
                let store = Arc::new(LanceIndexStore::new(
                    Arc::new(ObjectStore::local()),
                    Path::from_filesystem_path(tempdir.path()).unwrap(),
                    get_cache(use_cache, "int_unique"),
                ));
                let index = create_int_unique_index(store, use_cache).await;
                let _ = tempdir.keep();
                index
            })
        })
        .clone()
}

/// Setup function for int low cardinality index - creates it only once per cache variant
fn setup_int_low_card_index(rt: &tokio::runtime::Runtime, use_cache: bool) -> Arc<dyn ScalarIndex> {
    let static_ref = if use_cache {
        &INT_LOW_CARD_INDEX_CACHED
    } else {
        &INT_LOW_CARD_INDEX_NO_CACHE
    };

    static_ref
        .get_or_init(|| {
            rt.block_on(async {
                let tempdir = tempfile::tempdir().unwrap();
                let store = Arc::new(LanceIndexStore::new(
                    Arc::new(ObjectStore::local()),
                    Path::from_filesystem_path(tempdir.path()).unwrap(),
                    get_cache(use_cache, "int_low_card"),
                ));
                let index = create_int_low_card_index(store, use_cache).await;
                let _ = tempdir.keep();
                index
            })
        })
        .clone()
}

/// Setup function for string unique index - creates it only once per cache variant
fn setup_string_unique_index(
    rt: &tokio::runtime::Runtime,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let static_ref = if use_cache {
        &STRING_UNIQUE_INDEX_CACHED
    } else {
        &STRING_UNIQUE_INDEX_NO_CACHE
    };

    static_ref
        .get_or_init(|| {
            rt.block_on(async {
                let tempdir = tempfile::tempdir().unwrap();
                let store = Arc::new(LanceIndexStore::new(
                    Arc::new(ObjectStore::local()),
                    Path::from_filesystem_path(tempdir.path()).unwrap(),
                    get_cache(use_cache, "string_unique"),
                ));
                let index = create_string_unique_index(store, use_cache).await;
                let _ = tempdir.keep();
                index
            })
        })
        .clone()
}

/// Setup function for string low cardinality index - creates it only once per cache variant
fn setup_string_low_card_index(
    rt: &tokio::runtime::Runtime,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let static_ref = if use_cache {
        &STRING_LOW_CARD_INDEX_CACHED
    } else {
        &STRING_LOW_CARD_INDEX_NO_CACHE
    };

    static_ref
        .get_or_init(|| {
            rt.block_on(async {
                let tempdir = tempfile::tempdir().unwrap();
                let store = Arc::new(LanceIndexStore::new(
                    Arc::new(ObjectStore::local()),
                    Path::from_filesystem_path(tempdir.path()).unwrap(),
                    get_cache(use_cache, "string_low_card"),
                ));
                let index = create_string_low_card_index(store, use_cache).await;
                let _ = tempdir.keep();
                index
            })
        })
        .clone()
}

fn bench_equality(c: &mut Criterion) {
    let rt = get_runtime();

    // Calculate test values from constants (middle of range)
    let int_unique_value = (TOTAL_ROWS / 2) as i64;
    let string_unique_value = format!("string_{:010}", TOTAL_ROWS / 2);
    let int_low_card_value = (LOW_CARDINALITY_COUNT / 2) as i64;
    let string_low_card_value = format!("value_{:03}", LOW_CARDINALITY_COUNT / 2);

    let mut group = c.benchmark_group("bitmap_equality");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));

    // Benchmark both cached and uncached variants
    for use_cache in [false, true] {
        let cache_label = if use_cache { "cached" } else { "no_cache" };

        // int unique
        group.bench_function(BenchmarkId::new("int_unique", cache_label), |b| {
            let index = setup_int_unique_index(rt, use_cache);
            b.to_async(rt).iter(|| {
                let index = index.clone();
                let value = int_unique_value;
                async move {
                    let query = SargableQuery::Equals(ScalarValue::Int64(Some(value)));
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // int low cardinality
        group.bench_function(BenchmarkId::new("int_low_card", cache_label), |b| {
            let index = setup_int_low_card_index(rt, use_cache);
            b.to_async(rt).iter(|| {
                let index = index.clone();
                let value = int_low_card_value;
                async move {
                    let query = SargableQuery::Equals(ScalarValue::Int64(Some(value)));
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // String unique
        group.bench_function(BenchmarkId::new("string_unique", cache_label), |b| {
            let index = setup_string_unique_index(rt, use_cache);
            let value = string_unique_value.clone();
            b.to_async(rt).iter(|| {
                let index = index.clone();
                let value = value.clone();
                async move {
                    let query = SargableQuery::Equals(ScalarValue::Utf8(Some(value)));
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // String low cardinality
        group.bench_function(BenchmarkId::new("string_low_card", cache_label), |b| {
            let index = setup_string_low_card_index(rt, use_cache);
            let value = string_low_card_value.clone();
            b.to_async(rt).iter(|| {
                let index = index.clone();
                let value = value.clone();
                async move {
                    let query = SargableQuery::Equals(ScalarValue::Utf8(Some(value)));
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });
    }

    group.finish();
}

fn bench_in(c: &mut Criterion) {
    let rt = get_runtime();

    // Test with different numbers of values in the IN clause
    let value_counts = [1, 3, 5];

    for &num_values in &value_counts {
        let mut group = c.benchmark_group(format!("bitmap_in_{}", num_values));
        group
            .sample_size(10)
            .measurement_time(Duration::from_secs(10));

        // Calculate values around the middle of the range
        let mid_int = (TOTAL_ROWS / 2) as i64;
        let mid_string = TOTAL_ROWS / 2;
        let mid_low_card = LOW_CARDINALITY_COUNT / 2;

        // Int unique - IN query
        let int_values: Vec<ScalarValue> = (0..num_values)
            .map(|i| ScalarValue::Int64(Some(mid_int + i as i64 - num_values as i64 / 2)))
            .collect();

        // Int low cardinality - IN query
        let int_low_card_values: Vec<ScalarValue> = (0..num_values)
            .map(|i| ScalarValue::Int64(Some((mid_low_card + i - num_values / 2) as i64)))
            .collect();

        // String unique - IN query
        let string_values: Vec<ScalarValue> = (0..num_values)
            .map(|i| {
                ScalarValue::Utf8(Some(format!(
                    "string_{:010}",
                    (mid_string as i64 + i as i64 - num_values as i64 / 2) as u64
                )))
            })
            .collect();

        // String low cardinality - IN query
        let string_low_card_values: Vec<ScalarValue> = (0..num_values)
            .map(|i| {
                ScalarValue::Utf8(Some(format!(
                    "value_{:03}",
                    (mid_low_card as i32 + i as i32 - num_values as i32 / 2) as usize
                )))
            })
            .collect();

        // Benchmark both cached and uncached variants
        for use_cache in [false, true] {
            let cache_label = if use_cache { "cached" } else { "no_cache" };

            group.bench_function(BenchmarkId::new("int_unique", cache_label), |b| {
                let index = setup_int_unique_index(rt, use_cache);
                let values = int_values.clone();
                b.to_async(rt).iter(|| {
                    let index = index.clone();
                    let values = values.clone();
                    async move {
                        let query = SargableQuery::IsIn(values);
                        black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                    }
                })
            });

            group.bench_function(BenchmarkId::new("int_low_card", cache_label), |b| {
                let index = setup_int_low_card_index(rt, use_cache);
                let values = int_low_card_values.clone();
                b.to_async(rt).iter(|| {
                    let index = index.clone();
                    let values = values.clone();
                    async move {
                        let query = SargableQuery::IsIn(values);
                        black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                    }
                })
            });

            group.bench_function(BenchmarkId::new("string_unique", cache_label), |b| {
                let index = setup_string_unique_index(rt, use_cache);
                let values = string_values.clone();
                b.to_async(rt).iter(|| {
                    let index = index.clone();
                    let values = values.clone();
                    async move {
                        let query = SargableQuery::IsIn(values);
                        black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                    }
                })
            });

            group.bench_function(BenchmarkId::new("string_low_card", cache_label), |b| {
                let index = setup_string_low_card_index(rt, use_cache);
                let values = string_low_card_values.clone();
                b.to_async(rt).iter(|| {
                    let index = index.clone();
                    let values = values.clone();
                    async move {
                        let query = SargableQuery::IsIn(values);
                        black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                    }
                })
            });
        }

        group.finish();
    }
}

fn bench_bitmap(c: &mut Criterion) {
    // Run equality benchmarks
    bench_equality(c);

    // Run IN query benchmarks
    bench_in(c);
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_bitmap);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_bitmap);

criterion_main!(benches);
