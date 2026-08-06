// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lance_encoding::buffer::LanceBuffer;

const NUM_VALUES: &[usize] = &[1024 * 1024, 32 * 1024, 8 * 1024];

fn bench_zip(c: &mut Criterion) {
    for num_values in NUM_VALUES {
        let num_values = *num_values;
        let mut group = c.benchmark_group(format!("zip_{}Ki", num_values / 1024));

        group.throughput(Throughput::Bytes((num_values * 6) as u64));

        group.bench_function("2_4_zip_into_6", move |b| {
            // Zip together a 2-byte-per-buffer and an 8-byte-per-buffer array, each with 1Mi items
            let random_shorts: Vec<u8> =
                (0..num_values * 2).map(|_| rand::random::<u8>()).collect();
            let random_ints: Vec<u8> = (0..num_values * 4).map(|_| rand::random::<u8>()).collect();
            let mut buffers = vec![
                (LanceBuffer::from(random_shorts), 16),
                (LanceBuffer::from(random_ints), 32),
            ];
            let buffers = &mut buffers;

            b.iter(move || {
                let buffers = buffers
                    .iter_mut()
                    .map(|(buf, bits_per_value)| (buf.clone(), *bits_per_value))
                    .collect::<Vec<_>>();
                black_box(LanceBuffer::zip_into_one(buffers, num_values as u64).unwrap());
            })
        });

        group.bench_function("2_2_2_zip_into_6", move |b| {
            // Zip together a 2-byte-per-buffer and an 8-byte-per-buffer array, each with 1Mi items
            let random_shorts1: Vec<u8> =
                (0..num_values * 2).map(|_| rand::random::<u8>()).collect();
            let random_shorts2: Vec<u8> =
                (0..num_values * 2).map(|_| rand::random::<u8>()).collect();
            let random_shorts3: Vec<u8> =
                (0..num_values * 2).map(|_| rand::random::<u8>()).collect();
            let mut buffers = vec![
                (LanceBuffer::from(random_shorts1), 16),
                (LanceBuffer::from(random_shorts2), 16),
                (LanceBuffer::from(random_shorts3), 16),
            ];
            let buffers = &mut buffers;

            b.iter(move || {
                let buffers = buffers
                    .iter_mut()
                    .map(|(buf, bits_per_value)| (buf.clone(), *bits_per_value))
                    .collect::<Vec<_>>();
                black_box(LanceBuffer::zip_into_one(buffers, num_values as u64).unwrap());
            })
        });
    }
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(lance_testing::pprof::PProfProfiler::new(100, lance_testing::pprof::Output::Flamegraph(None)));
    targets = bench_zip);

// Non-linux version does not support pprof.
#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_zip);

criterion_main!(benches);
