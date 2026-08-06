// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![recursion_limit = "256"]

use std::sync::Arc;

use arrow_array::{FixedSizeListArray, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};

use lance::Dataset;
use lance::dataset::WriteParams;
use lance::index::vector::ivf::build_ivf_model;
use lance_arrow::FixedSizeListArrayExt;
use lance_index::progress::noop_progress;
use lance_index::vector::ivf::IvfBuildParams;
use lance_linalg::distance::MetricType;
use lance_testing::datagen::generate_random_array;

const DIM: usize = 8;
const NUM_PARTITIONS: usize = 320;
const SAMPLE_RATE: usize = 2;
const STREAMING_SAMPLE_RATE: usize = 1;
const MAX_ITERS: usize = 1;

async fn create_training_dataset(uri: &str) -> Dataset {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            FieldRef::new(Field::new("item", DataType::Float32, true)),
            DIM as i32,
        ),
        false,
    )]));
    let num_rows = NUM_PARTITIONS * SAMPLE_RATE;
    let batch_size = num_rows / 2;
    let batches = (0..2)
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        generate_random_array(batch_size * DIM),
                        DIM as i32,
                    )
                    .unwrap(),
                )],
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            max_rows_per_file: num_rows,
            max_rows_per_group: batch_size,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

fn bench_streaming_ivf_training(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let uri = tempdir
        .path()
        .join("streaming_ivf_training.lance")
        .to_string_lossy()
        .to_string();
    let dataset = rt.block_on(create_training_dataset(&uri));

    let mut params = IvfBuildParams::new(NUM_PARTITIONS);
    params.sample_rate = SAMPLE_RATE;
    params.streaming_sample_rate = Some(STREAMING_SAMPLE_RATE);
    params.max_iters = MAX_ITERS;

    c.bench_function(
        &format!(
            "streaming_ivf_training_{}d_{}p_sample_{}_stream_{}",
            DIM, NUM_PARTITIONS, SAMPLE_RATE, STREAMING_SAMPLE_RATE
        ),
        |b| {
            b.to_async(&rt).iter(|| async {
                let ivf_model = build_ivf_model(
                    &dataset,
                    "vector",
                    DIM,
                    MetricType::L2,
                    &params,
                    None,
                    noop_progress(),
                )
                .await
                .unwrap();
                black_box(ivf_model.num_partitions());
            });
        },
    );
}

criterion_group!(
    name=benches;
    config = Criterion::default().sample_size(10);
    targets = bench_streaming_ivf_training
);
criterion_main!(benches);
