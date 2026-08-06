// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of BTree scalar index.
//!
//! This benchmark measures the performance of BTree index with:
//! - 50 million data points
//! - int and String data types
//! - High cardinality (unique values) and low cardinality (100 unique values)
//! - Equality filters
//! - Range filters with varying selectivity (few/many/most rows match)
//! - IN filters with varying size (10, 20, 30 values)

mod common;

use std::{
    ops::Bound,
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
use lance_index::scalar::btree::{BTreeIndexPlugin, DEFAULT_BTREE_BATCH_SIZE, train_btree_index};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::ScalarIndexPlugin;
use lance_index::scalar::{SargableQuery, ScalarIndex};
use lance_io::object_store::ObjectStore;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use object_store::path::Path;

/// Selectivity level for range queries
#[derive(Clone, Copy, Debug)]
enum Selectivity {
    Few,  // ~0.1% of rows
    Many, // ~10% of rows
    Most, // ~90% of rows
}

impl Selectivity {
    fn name(&self) -> &'static str {
        match self {
            Self::Few => "few",
            Self::Many => "many",
            Self::Most => "most",
        }
    }

    /// Get the approximate percentage of rows that should match
    fn percentage(&self) -> f64 {
        match self {
            Self::Few => 0.001,
            Self::Many => 0.10,
            Self::Most => 0.90,
        }
    }
}

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

// Keep temp directories alive for the lifetime of the program
static TEMP_DIRS: OnceLock<Vec<tempfile::TempDir>> = OnceLock::new();

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

/// Create and train a BTree index for int64 data with unique values
async fn create_int_unique_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_int_unique_stream();

    train_btree_index(stream, store.as_ref(), DEFAULT_BTREE_BATCH_SIZE, None, None)
        .await
        .unwrap();

    let cache = get_cache(use_cache, "int_unique");
    let details = prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default()).unwrap();

    (BTreeIndexPlugin
        .load_index(store, &details, None, &cache)
        .await
        .unwrap()) as _
}

/// Create and train a BTree index for int64 data with low cardinality
async fn create_int_low_card_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_int_low_cardinality_stream();

    train_btree_index(stream, store.as_ref(), DEFAULT_BTREE_BATCH_SIZE, None, None)
        .await
        .unwrap();

    let cache = get_cache(use_cache, "int_low_card");
    let details = prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default()).unwrap();

    (BTreeIndexPlugin
        .load_index(store, &details, None, &cache)
        .await
        .unwrap()) as _
}

/// Create and train a BTree index for string data with unique values
async fn create_string_unique_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_string_unique_stream();

    train_btree_index(stream, store.as_ref(), DEFAULT_BTREE_BATCH_SIZE, None, None)
        .await
        .unwrap();

    let cache = get_cache(use_cache, "string_unique");
    let details = prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default()).unwrap();

    (BTreeIndexPlugin
        .load_index(store, &details, None, &cache)
        .await
        .unwrap()) as _
}

/// Create and train a BTree index for string data with low cardinality
async fn create_string_low_card_index(
    store: Arc<LanceIndexStore>,
    use_cache: bool,
) -> Arc<dyn ScalarIndex> {
    let stream = common::generate_string_low_cardinality_stream();

    train_btree_index(stream, store.as_ref(), DEFAULT_BTREE_BATCH_SIZE, None, None)
        .await
        .unwrap();

    let cache = get_cache(use_cache, "string_low_card");
    let details = prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default()).unwrap();

    (BTreeIndexPlugin
        .load_index(store, &details, None, &cache)
        .await
        .unwrap()) as _
}

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

                // Store the temp directory to keep it alive
                TEMP_DIRS.get_or_init(Vec::new);
                // Note: We can't modify TEMP_DIRS after init, but the tempdir staying in scope here
                // should keep it alive for the program duration due to the static lifetime
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

    let mut group = c.benchmark_group("btree_equality");
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

/// Helper function to count results from a range query
fn count_range_results(
    rt: &tokio::runtime::Runtime,
    index: &Arc<dyn ScalarIndex>,
    query: SargableQuery,
) -> usize {
    rt.block_on(async {
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        match result {
            lance_index::scalar::SearchResult::Exact(row_ids) => {
                row_ids.len().expect("Expected exact row count") as usize
            }
            _ => panic!("Expected exact search result"),
        }
    })
}

fn bench_range(c: &mut Criterion, selectivity: Selectivity) {
    let rt = get_runtime();

    let group_name = format!("btree_range_{}", selectivity.name());
    let mut group = c.benchmark_group(&group_name);
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));

    let pct = selectivity.percentage();

    // Int unique - range queries
    let int_range_size = (TOTAL_ROWS as f64 * pct) as u64;
    let int_start = (TOTAL_ROWS / 2) - (int_range_size / 2);
    let int_end = int_start + int_range_size;

    // Benchmark both cached and uncached variants
    for use_cache in [false, true] {
        let cache_label = if use_cache { "cached" } else { "no_cache" };

        group.bench_function(BenchmarkId::new("int_unique", cache_label), |b| {
            // Setup index and run sanity check
            let index = setup_int_unique_index(rt, use_cache);

            // Sanity check: verify int unique range returns expected count
            let int_unique_query = SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(int_start as i64))),
                Bound::Included(ScalarValue::Int64(Some(int_end as i64))),
            );
            let int_unique_count = count_range_results(rt, &index, int_unique_query);
            let expected_count = (int_end - int_start + 1) as usize; // +1 because range is inclusive
            assert!(
                (int_unique_count as f64 - expected_count as f64).abs() / (expected_count as f64)
                    < 0.01,
                "int unique count mismatch: expected {}, got {}",
                expected_count,
                int_unique_count
            );
            b.to_async(rt).iter(|| {
                let index = index.clone();
                async move {
                    let query = SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(int_start as i64))),
                        Bound::Included(ScalarValue::Int64(Some(int_end as i64))),
                    );
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // int low cardinality - range queries
        // With 100 unique values, select appropriate range
        let low_card_range_size = (LOW_CARDINALITY_COUNT as f64 * pct) as usize;
        let low_card_start = (LOW_CARDINALITY_COUNT / 2) - (low_card_range_size / 2);
        let low_card_end = low_card_start + low_card_range_size;

        group.bench_function(BenchmarkId::new("int_low_card", cache_label), |b| {
            // Setup index and run sanity check
            let index = setup_int_low_card_index(rt, use_cache);

            // Sanity check: verify int low cardinality range returns expected count
            let int_low_card_query = SargableQuery::Range(
                Bound::Included(ScalarValue::Int64(Some(low_card_start as i64))),
                Bound::Included(ScalarValue::Int64(Some(low_card_end as i64))),
            );
            let int_low_card_count = count_range_results(rt, &index, int_low_card_query);
            let rows_per_value = TOTAL_ROWS / LOW_CARDINALITY_COUNT as u64;
            let expected_low_card_count =
                ((low_card_end - low_card_start + 1) as u64 * rows_per_value) as usize;
            assert!(
                (int_low_card_count as f64 - expected_low_card_count as f64).abs()
                    / (expected_low_card_count as f64)
                    < 0.01,
                "int low cardinality count mismatch: expected {}, got {}",
                expected_low_card_count,
                int_low_card_count
            );
            b.to_async(rt).iter(|| {
                let index = index.clone();
                async move {
                    let query = SargableQuery::Range(
                        Bound::Included(ScalarValue::Int64(Some(low_card_start as i64))),
                        Bound::Included(ScalarValue::Int64(Some(low_card_end as i64))),
                    );
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // String unique - range queries
        let string_start_row = int_start;
        let string_end_row = int_end;

        group.bench_function(BenchmarkId::new("string_unique", cache_label), |b| {
            // Setup index and run sanity check
            let index = setup_string_unique_index(rt, use_cache);

            // Sanity check: verify string unique range returns expected count
            let string_unique_query = SargableQuery::Range(
                Bound::Included(ScalarValue::Utf8(Some(format!(
                    "string_{:010}",
                    string_start_row
                )))),
                Bound::Included(ScalarValue::Utf8(Some(format!(
                    "string_{:010}",
                    string_end_row
                )))),
            );
            let string_unique_count = count_range_results(rt, &index, string_unique_query);
            let expected_string_count = (string_end_row - string_start_row + 1) as usize;
            assert!(
                (string_unique_count as f64 - expected_string_count as f64).abs()
                    / (expected_string_count as f64)
                    < 0.01,
                "String unique count mismatch: expected {}, got {}",
                expected_string_count,
                string_unique_count
            );
            b.to_async(rt).iter(|| {
                let index = index.clone();
                async move {
                    let query = SargableQuery::Range(
                        Bound::Included(ScalarValue::Utf8(Some(format!(
                            "string_{:010}",
                            string_start_row
                        )))),
                        Bound::Included(ScalarValue::Utf8(Some(format!(
                            "string_{:010}",
                            string_end_row
                        )))),
                    );
                    black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
                }
            })
        });

        // String low cardinality - range queries
        group.bench_function(BenchmarkId::new("string_low_card", cache_label), |b| {
            // Setup index and run sanity check
            let index = setup_string_low_card_index(rt, use_cache);

            // Sanity check: verify string low cardinality range returns expected count
            let string_low_card_query = SargableQuery::Range(
                Bound::Included(ScalarValue::Utf8(Some(format!(
                    "value_{:03}",
                    low_card_start
                )))),
                Bound::Included(ScalarValue::Utf8(Some(format!(
                    "value_{:03}",
                    low_card_end
                )))),
            );
            let string_low_card_count = count_range_results(rt, &index, string_low_card_query);
            let rows_per_value = TOTAL_ROWS / LOW_CARDINALITY_COUNT as u64;
            let expected_string_low_card_count =
                ((low_card_end - low_card_start + 1) as u64 * rows_per_value) as usize;
            assert!(
                (string_low_card_count as f64 - expected_string_low_card_count as f64).abs()
                    / (expected_string_low_card_count as f64)
                    < 0.01,
                "String low cardinality count mismatch: expected {}, got {}",
                expected_string_low_card_count,
                string_low_card_count
            );
            b.to_async(rt).iter(|| {
                let index = index.clone();
                async move {
                    let query = SargableQuery::Range(
                        Bound::Included(ScalarValue::Utf8(Some(format!(
                            "value_{:03}",
                            low_card_start
                        )))),
                        Bound::Included(ScalarValue::Utf8(Some(format!(
                            "value_{:03}",
                            low_card_end
                        )))),
                    );
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
    let value_counts = [10, 20, 30];

    for &num_values in &value_counts {
        let mut group = c.benchmark_group(format!("btree_in_{}", num_values));
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

fn bench_btree(c: &mut Criterion) {
    // Run equality benchmarks
    bench_equality(c);

    // Run IN query benchmarks
    bench_in(c);

    // Run range benchmarks with different selectivities
    bench_range(c, Selectivity::Few);
    bench_range(c, Selectivity::Many);
    bench_range(c, Selectivity::Most);
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_btree);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_btree);

criterion_main!(benches);
