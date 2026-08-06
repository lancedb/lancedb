// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{
    RecordBatchReader,
    types::{UInt32Type, UInt64Type},
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::{physical_plan::SendableRecordBatchStream, scalar::ScalarValue};
use futures::{FutureExt, TryStreamExt};
use lance::{Dataset, io::ObjectStore};
use lance_core::cache::LanceCache;
use lance_core::utils::tempfile::TempStrDir;
use lance_datafusion::utils::reader_to_stream;
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_index::scalar::{
    IndexStore, SargableQuery, ScalarIndex, SearchResult,
    btree::{DEFAULT_BTREE_BATCH_SIZE, train_btree_index},
    lance_format::LanceIndexStore,
    registry::ScalarIndexPlugin,
};
use lance_index::{metrics::NoOpMetricsCollector, scalar::btree::BTreeIndexPlugin};
use lance_select::RowSetOps;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

struct BenchmarkFixture {
    _datadir: TempStrDir,
    index_store: Arc<dyn IndexStore>,
    baseline_dataset: Arc<Dataset>,
}

fn test_data() -> impl RecordBatchReader {
    gen_batch()
        .col("values", array::step::<UInt32Type>())
        .col("row_ids", array::step::<UInt64Type>())
        .into_reader_rows(RowCount::from(1024), BatchCount::from(100 * 1024))
}

fn test_data_stream() -> SendableRecordBatchStream {
    reader_to_stream(Box::new(test_data()))
}

impl BenchmarkFixture {
    fn test_store(tempdir: &TempStrDir) -> Arc<dyn IndexStore> {
        let test_path = tempdir.as_str();
        let (object_store, test_path) = ObjectStore::from_uri(test_path)
            .now_or_never()
            .unwrap()
            .unwrap();
        Arc::new(LanceIndexStore::new(
            object_store,
            test_path,
            Arc::new(LanceCache::no_cache()),
        ))
    }

    async fn write_baseline_data(tempdir: &TempStrDir) -> Arc<Dataset> {
        let test_path = tempdir.as_str();
        Arc::new(Dataset::write(test_data(), test_path, None).await.unwrap())
    }

    async fn train_scalar_index(index_store: &Arc<dyn IndexStore>) {
        train_btree_index(
            test_data_stream(),
            index_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();
    }

    async fn open() -> Self {
        let tempdir = TempStrDir::default();
        let index_store = Self::test_store(&tempdir);
        let baseline_dataset = Self::write_baseline_data(&tempdir).await;
        Self::train_scalar_index(&index_store).await;

        Self {
            _datadir: tempdir,
            index_store,
            baseline_dataset,
        }
    }
}

async fn baseline_equality_search(fixture: &BenchmarkFixture) {
    let mut stream = fixture
        .baseline_dataset
        .scan()
        .filter("values == 10000")
        .unwrap()
        .with_row_id()
        .try_into_stream()
        .await
        .unwrap();
    let mut num_rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        num_rows += batch.num_rows();
    }
    assert_eq!(num_rows, 1);
}

async fn warm_indexed_equality_search(index: &dyn ScalarIndex) {
    let result = index
        .search(
            &SargableQuery::Equals(ScalarValue::UInt32(Some(10000))),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
    let SearchResult::Exact(row_ids) = result else {
        panic!("Expected exact results")
    };
    assert_eq!(row_ids.true_rows().len(), Some(1));
}

async fn baseline_inequality_search(fixture: &BenchmarkFixture) {
    let mut stream = fixture
        .baseline_dataset
        .scan()
        .filter("values >= 50000000")
        .unwrap()
        .with_row_id()
        .try_into_stream()
        .await
        .unwrap();
    let mut num_rows = 0;
    while let Some(batch) = stream.try_next().await.unwrap() {
        num_rows += batch.num_rows();
    }
    // 100Mi - 50M = 54,857,600
    assert_eq!(num_rows, 54857600);
}

async fn warm_indexed_inequality_search(index: &dyn ScalarIndex) {
    let result = index
        .search(
            &SargableQuery::Range(
                std::ops::Bound::Included(ScalarValue::UInt32(Some(50_000_000))),
                std::ops::Bound::Unbounded,
            ),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
    let SearchResult::Exact(row_ids) = result else {
        panic!("Expected exact results")
    };

    // 100Mi - 50M = 54,857,600
    assert_eq!(row_ids.true_rows().len(), Some(54857600));
}

async fn warm_indexed_isin_search(index: &dyn ScalarIndex) {
    let result = index
        .search(
            &SargableQuery::IsIn(vec![
                ScalarValue::UInt32(Some(10000)),
                ScalarValue::UInt32(Some(50000000)),
                ScalarValue::UInt32(Some(150000000)), // Not found
                ScalarValue::UInt32(Some(287123)),
            ]),
            &NoOpMetricsCollector,
        )
        .await
        .unwrap();
    let SearchResult::Exact(row_ids) = result else {
        panic!("Expected exact results")
    };

    // Only 3 because 150M is not in dataset
    assert_eq!(row_ids.true_rows().len(), Some(3));
}

fn bench_baseline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let fixture = rt.block_on(BenchmarkFixture::open());

    c.bench_function("baseline_equality", |b| {
        b.iter(|| rt.block_on(baseline_equality_search(&fixture)));
    });

    c.bench_function("baseline_inequality", |b| {
        b.iter(|| rt.block_on(baseline_inequality_search(&fixture)));
    });
}

fn bench_warm_indexed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let fixture = rt.block_on(BenchmarkFixture::open());
    let details =
        prost_types::Any::from_msg(&lance_index::pbold::BTreeIndexDetails::default()).unwrap();

    let index = rt
        .block_on(BTreeIndexPlugin.load_index(
            fixture.index_store.clone(),
            &details,
            None,
            &LanceCache::no_cache(),
        ))
        .unwrap();

    c.bench_function("windexed_equality", |b| {
        b.iter(|| rt.block_on(warm_indexed_equality_search(index.as_ref())))
    });

    c.bench_function("windexed_inequality", |b| {
        b.iter(|| rt.block_on(warm_indexed_inequality_search(index.as_ref())))
    });

    c.bench_function("windexed_is_in", |b| {
        b.iter(|| rt.block_on(warm_indexed_isin_search(index.as_ref())))
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_baseline, bench_warm_indexed);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_baseline, bench_warm_indexed);

criterion_main!(benches);
