// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::{sync::Arc, time::Duration};

use arrow_array::{Int32Array, RecordBatch, UInt64Array};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::stream;
use itertools::Itertools;
use lance_core::ROW_ADDR;
use lance_core::cache::LanceCache;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::pbold;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::zonemap::{
    ZoneMapIndexBuilder, ZoneMapIndexBuilderParams, ZoneMapIndexPlugin,
};
use lance_index::scalar::{SargableQuery, registry::ScalarIndexPlugin};
use lance_io::object_store::ObjectStore;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use object_store::path::Path;

fn bench_zonemap(c: &mut Criterion) {
    const TOTAL: usize = 1_000_000;

    env_logger::init();

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
    let store = rt.block_on(async {
        Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ))
    });

    // Generate sequential integers for the zonemap index
    let data_col = arrow_array::Int32Array::from_iter_values(0..TOTAL as i32);

    let row_addr_col = Arc::new(UInt64Array::from(
        (0..TOTAL).map(|i| i as u64).collect_vec(),
    ));

    let batch = RecordBatch::try_new(
        arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("values", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new(ROW_ADDR, arrow_schema::DataType::UInt64, false),
        ])
        .into(),
        vec![Arc::new(data_col), row_addr_col],
    )
    .unwrap();

    let batches = (0..1000).map(|i| batch.slice(i * 1000, 1000)).collect_vec();

    let mut group = c.benchmark_group("train");

    group.sample_size(10);
    group.bench_function(format!("zonemap_train({TOTAL})").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let stream = RecordBatchStreamAdapter::new(
                batch.schema(),
                stream::iter(batches.clone().into_iter().map(Ok)),
            );

            let mut builder = ZoneMapIndexBuilder::try_new(
                ZoneMapIndexBuilderParams::default(),
                batch.schema().field(0).data_type().clone(),
            )
            .unwrap();

            builder.train(Box::pin(stream)).await.unwrap();
            builder.write_index(store.as_ref()).await.unwrap();
        })
    });

    drop(group);

    let mut group = c.benchmark_group("search");

    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(10));
    let details = prost_types::Any::from_msg(&pbold::ZoneMapIndexDetails::default()).unwrap();
    let index = rt
        .block_on(ZoneMapIndexPlugin.load_index(store, &details, None, &LanceCache::no_cache()))
        .unwrap();
    group.bench_function(format!("zonemap_search({TOTAL})").as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let sample_idx = rand::random_range(0..batch.num_rows());
            let sample_value = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(sample_idx);
            let query = SargableQuery::Equals(ScalarValue::Int32(Some(sample_value)));
            black_box(index.search(&query, &NoOpMetricsCollector).await.unwrap());
        })
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_zonemap);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_zonemap);

criterion_main!(benches);
