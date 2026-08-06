// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of Building PQ code from Dense Vectors.

use arrow_array::{FixedSizeListArray, types::Float32Type};
use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::quantizer::Quantization;
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array_with_seed;

const PQ: usize = 96;
const DIM: usize = 1536;
const TOTAL: usize = 32 * 1024;

fn pq_transform(c: &mut Criterion) {
    let codebook = generate_random_array_with_seed::<Float32Type>(256 * DIM, [88; 32]);

    let vectors = generate_random_array_with_seed::<Float32Type>(DIM * TOTAL, [3; 32]);
    let fsl = FixedSizeListArray::try_new_from_values(vectors, DIM as i32).unwrap();

    for dt in [DistanceType::L2, DistanceType::Dot].iter() {
        let pq = ProductQuantizer::new(
            PQ,
            8,
            DIM,
            FixedSizeListArray::try_new_from_values(codebook.clone(), DIM as i32).unwrap(),
            *dt,
        );

        c.bench_function(format!("{},{}", dt, TOTAL).as_str(), |b| {
            b.iter(|| {
                let _ = pq.quantize(&fsl).unwrap();
            })
        });
    }
}

criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = pq_transform);

criterion_main!(benches);
