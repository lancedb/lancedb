// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{FixedSizeListArray, RecordBatch, UInt32Array, types::Float32Type};
use arrow_schema::{DataType, Field, Schema};
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::residual::ResidualTransform;
use lance_index::vector::transform::Transformer;
use lance_testing::datagen::generate_random_array_with_seed;

const NUM_CENTROIDS: usize = 1024;
const DIMENSION: usize = 256;
const NUM_VECTORS: usize = 64 * 1024;
const PARTITION_COL: &str = "__part_id";
const VECTOR_COL: &str = "vector";

fn bench_residual_transform(c: &mut Criterion) {
    let centroids =
        generate_random_array_with_seed::<Float32Type>(NUM_CENTROIDS * DIMENSION, [7; 32]);
    let centroids = FixedSizeListArray::try_new_from_values(centroids, DIMENSION as i32).unwrap();

    let vectors = generate_random_array_with_seed::<Float32Type>(NUM_VECTORS * DIMENSION, [42; 32]);
    let vectors = FixedSizeListArray::try_new_from_values(vectors, DIMENSION as i32).unwrap();

    let part_ids =
        UInt32Array::from_iter_values((0..NUM_VECTORS).map(|idx| (idx % NUM_CENTROIDS) as u32));

    let schema = Arc::new(Schema::new(vec![
        Field::new(PARTITION_COL, DataType::UInt32, false),
        Field::new(
            VECTOR_COL,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIMENSION as i32,
            ),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(part_ids), Arc::new(vectors)]).unwrap();

    let transform = ResidualTransform::new(centroids, PARTITION_COL, VECTOR_COL);
    c.bench_function("residual_transform_float32", |b| {
        b.iter(|| {
            black_box(transform.transform(black_box(&batch)).unwrap());
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(8))
        .sample_size(10);
    targets = bench_residual_transform
);
criterion_main!(benches);
