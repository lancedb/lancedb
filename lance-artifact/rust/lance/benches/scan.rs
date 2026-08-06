// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Before running the dataset, prepare a "test.lance" dataset, in the
//! `lance/rust` directory. There is no limitation in the dataset size,
//! schema, or content.
//!
//! Run benchmark.
//! ```
//! cargo bench --bench scan
//! ```.
//!
//! TODO: Take parameterized input to specify dataset URI from command line.

#![allow(clippy::print_stdout)]

use arrow_array::{
    BinaryArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::stream::TryStreamExt;
use lance_arrow::FixedSizeListArrayExt;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use std::sync::Arc;

use lance::dataset::{Dataset, WriteMode, WriteParams};

fn bench_scan(c: &mut Criterion) {
    // default tokio runtime
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        create_file(std::path::Path::new("./test.lance"), WriteMode::Create).await
    });
    let dataset = rt.block_on(async { Dataset::open("./test.lance").await.unwrap() });

    c.bench_function("Scan full dataset", |b| {
        b.to_async(&rt).iter(|| async {
            let count = dataset
                .scan()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert!(!count.is_empty());
        })
    });
}

async fn create_file(path: &std::path::Path, mode: WriteMode) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("i", DataType::Int32, false),
        Field::new("f", DataType::Float32, false),
        Field::new("s", DataType::Utf8, false),
        Field::new(
            "fsl",
            DataType::FixedSizeList(
                FieldRef::new(Field::new("item", DataType::Float32, true)),
                2,
            ),
            false,
        ),
        Field::new("blob", DataType::Binary, false),
    ]));
    let num_rows = 100_000;
    let batch_size = 10000;
    let batches: Vec<RecordBatch> = (0..(num_rows / batch_size))
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        i * batch_size..(i + 1) * batch_size,
                    )),
                    Arc::new(Float32Array::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| x as f32)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| format!("s-{}", x))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(
                        FixedSizeListArray::try_new_from_values(
                            Float32Array::from_iter_values(
                                (i * batch_size..(i + 2) * batch_size)
                                    .map(|x| (batch_size + (x - batch_size) / 2) as f32),
                            ),
                            2,
                        )
                        .unwrap(),
                    ),
                    Arc::new(BinaryArray::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| format!("blob-{}", x).into_bytes()),
                    )),
                ],
            )
            .unwrap()
        })
        .collect();

    let test_uri = path.to_str().unwrap();
    std::fs::remove_dir_all(test_uri).map_or_else(|_| println!("{} not exists", test_uri), |_| {});
    let write_params = WriteParams {
        max_rows_per_file: num_rows as usize,
        max_rows_per_group: batch_size as usize,
        mode,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(reader, test_uri, Some(write_params))
        .await
        .unwrap();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_scan);
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_scan);
criterion_main!(benches);
