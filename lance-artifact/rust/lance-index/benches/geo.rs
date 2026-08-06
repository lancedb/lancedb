// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::ScalarValue;
use geo_types::coord;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::Dimension;
use lance_core::cache::LanceCache;
use lance_core::{Error, ROW_ID};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::BasicTrainer;
use lance_index::scalar::rtree::{BoundingBox, RTreeIndex, RTreeIndexPlugin, RTreeTrainingRequest};
use lance_index::scalar::{GeoQuery, RelationQuery, ScalarIndex};
use lance_io::object_store::ObjectStore;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use object_store::path::Path;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::sync::Arc;
use std::time::Duration;

fn generate_geo_data(num_rects: usize, seed: u64) -> Vec<BoundingBox> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(num_rects);

    for _ in 0..num_rects {
        let x1 = rng.random_range(0.0..=1000.0);
        let y1 = rng.random_range(0.0..=1000.0);
        let x2 = x1 + rng.random_range(0.1..=10.0);
        let y2 = y1 + rng.random_range(0.1..=10.0);

        data.push(BoundingBox::new_with_coords(&[
            coord! { x: x1, y: y1 },
            coord! { x: x2, y: y2 },
        ]));
    }

    data
}

async fn create_record_batch(geo_data: &[BoundingBox]) -> RecordBatch {
    let rect_type = geoarrow_schema::RectType::new(Dimension::XY, Default::default());
    let bbox_field = rect_type.to_field("bbox", false);
    let rowid_field = Field::new(ROW_ID, DataType::UInt64, false);

    let mut rect_builder = RectBuilder::new(rect_type);
    for rect in geo_data {
        rect_builder.push_rect(Some(rect));
    }

    let rect_arr = rect_builder.finish();
    let rowid_arr = Arc::new(UInt64Array::from_iter(0..rect_arr.len() as u64));

    let schema = arrow_schema::Schema::new(vec![bbox_field, rowid_field]);
    RecordBatch::try_new(Arc::new(schema), vec![rect_arr.to_array_ref(), rowid_arr]).unwrap()
}

async fn build_rtree(
    store: Arc<LanceIndexStore>,
    geo_data: &[BoundingBox],
) -> Result<Arc<RTreeIndex>, Error> {
    let batch = create_record_batch(geo_data).await;
    let schema = batch.schema().clone();
    let stream = Box::pin(futures::stream::once(async move { Ok(batch) }));
    let stream = Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream));

    let plugin = RTreeIndexPlugin;
    plugin
        .train_index(
            stream,
            store.as_ref(),
            Box::new(RTreeTrainingRequest::default()),
            None,
            lance_index::progress::noop_progress(),
        )
        .await?;

    let index = RTreeIndex::load(store, None, &LanceCache::no_cache()).await?;

    Ok(index)
}

async fn rect_search_rtree(
    index: Arc<RTreeIndex>,
    bbox: &BoundingBox,
) -> Result<lance_index::scalar::SearchResult, Error> {
    let field =
        geoarrow_schema::RectType::new(Dimension::XY, Default::default()).to_field("bbox", false);

    let rect_type = geoarrow_schema::RectType::new(Dimension::XY, Default::default());
    let mut builder = RectBuilder::new(rect_type);
    builder.push_rect(Some(bbox));
    let scalar_value =
        ScalarValue::try_from_array(builder.finish().to_array_ref().as_ref(), 0).unwrap();

    let geo_query = GeoQuery::IntersectQuery(RelationQuery {
        value: scalar_value,
        field,
    });

    index
        .search(&geo_query, &lance_index::metrics::NoOpMetricsCollector)
        .await
}

fn bench_rtree(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let num_rows = 1_000_000;

    let tempdir = tempfile::tempdir().unwrap();
    let index_dir = Path::from_filesystem_path(tempdir.path()).unwrap();
    let store = rt.block_on(async {
        Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            index_dir,
            Arc::new(LanceCache::no_cache()),
        ))
    });

    let geo_data = rt.block_on(async { black_box(generate_geo_data(num_rows, 42)) });

    let mut group = c.benchmark_group("RTree");
    group.sample_size(10);

    group.bench_function("indexing", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(build_rtree(store.clone(), &geo_data).await.unwrap());
        });
    });

    let index = rt
        .block_on(RTreeIndex::load(
            store.clone(),
            None,
            &LanceCache::no_cache(),
        ))
        .unwrap();

    group.bench_function("search", |b| {
        b.to_async(&rt).iter(|| async {
            let query_bbox = BoundingBox::new_with_coords(&[
                coord! { x: 400.0, y: 400.0 },
                coord! { x: 600.0, y: 600.0 },
            ]);
            let result = rect_search_rtree(black_box(index.clone()), black_box(&query_bbox)).await;
            assert!(result.is_ok());
        });
    });

    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_rtree);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_rtree);

criterion_main!(benches);
