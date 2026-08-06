// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmarks for `COUNT(*)` via the scanner aggregate plan (the path the
//! `count_pushdown` rule rewrites into `CountFromMaskExec`).
//!
//! The dataset uses stable row ids, multiple fragments, and scattered
//! cross-fragment deletions, with a BTree scalar index on the filter column.
//! Run on two revisions to compare (e.g. before/after a change to the rule):
//!
//! ```text
//! cargo bench -p lance --bench count_pushdown
//! ```

use std::sync::Arc;

use arrow_array::types::UInt32Type;
use criterion::{Criterion, criterion_group, criterion_main};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::index::DatasetIndexExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

const ROWS_PER_FRAGMENT: usize = 100_000;
const NUM_FRAGMENTS: usize = 50;
const TOTAL_ROWS: u32 = (ROWS_PER_FRAGMENT * NUM_FRAGMENTS) as u32; // 5,000,000

struct Fixture {
    _datadir: TempStrDir,
    dataset: Arc<Dataset>,
}

impl Fixture {
    async fn open() -> Self {
        let datadir = TempStrDir::default();
        // `value` steps 0..TOTAL_ROWS, so `value < k` selects exactly k rows
        // (before deletions) and gives precise control over selectivity.
        let reader = gen_batch()
            .col("value", array::step::<UInt32Type>())
            .into_reader_rows(
                RowCount::from(ROWS_PER_FRAGMENT as u64),
                BatchCount::from(NUM_FRAGMENTS as u32),
            );
        let mut dataset = Dataset::write(
            reader,
            datadir.as_str(),
            Some(WriteParams {
                max_rows_per_file: ROWS_PER_FRAGMENT,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Scatter deletions across every fragment (~1%) to exercise the
        // deletion mask in stable-id space.
        dataset.delete("value % 100 = 0").await.unwrap();

        dataset
            .create_index(
                &["value"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        Self {
            _datadir: datadir,
            dataset: Arc::new(dataset),
        }
    }
}

async fn count_unfiltered(dataset: &Dataset) -> u64 {
    dataset.scan().count_rows().await.unwrap()
}

async fn count_filtered(dataset: &Dataset, filter: &str) -> u64 {
    let mut scanner = dataset.scan();
    scanner.filter(filter).unwrap();
    scanner.count_rows().await.unwrap()
}

fn bench_count(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let fixture = rt.block_on(Fixture::open());
    let ds = &fixture.dataset;

    c.bench_function("count_unfiltered", |b| {
        b.iter(|| rt.block_on(count_unfiltered(ds)))
    });

    // ~1% of rows match.
    let filter_1pct = format!("value < {}", TOTAL_ROWS / 100);
    c.bench_function("count_filtered_1pct", |b| {
        b.iter(|| rt.block_on(count_filtered(ds, &filter_1pct)))
    });

    // ~50% of rows match.
    let filter_50pct = format!("value < {}", TOTAL_ROWS / 2);
    c.bench_function("count_filtered_50pct", |b| {
        b.iter(|| rt.block_on(count_filtered(ds, &filter_50pct)))
    });
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_count);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_count);

criterion_main!(benches);
