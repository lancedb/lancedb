// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark: regex predicate scans over an ngram-indexed string column.
//!
//! Each query is a `regexp_match(doc, '...')` filter against a dataset that has
//! an NGram index on `doc`. The query set spans a selective AND pattern, an
//! alternation, a plain literal (rewritten to an infix LIKE before it reaches
//! the index), and a deliberately non-accelerable pattern (`a.b`, which yields
//! no trigram) that serves as a regression guard.
//!
//! On `main` none of these use the index (regex falls through to a full scan +
//! recheck); with the ngram-regex acceleration the index prunes candidates for
//! the first three while `a.b` stays a full scan. Capture a baseline on `main`
//! with `--save-baseline before_7130`, then compare after the change with
//! `--baseline before_7130`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_datagen::{RowCount, array};
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

const TOTAL: usize = 200_000;

/// Build the `doc` column: random sentences with rare markers injected into a
/// small fraction of rows so the regex queries have controlled selectivity.
/// The markers (`zqxwvu`, `needlexyz`, `qwerasdf`) are unlikely to appear in
/// the generated English-word sentences.
fn build_docs() -> StringArray {
    let mut sentence_gen = array::random_sentence(1, 30, false);
    let base = sentence_gen
        .generate_default(RowCount::from(TOTAL as u64))
        .unwrap();
    let base = base.as_string::<i32>();
    let docs = (0..TOTAL).map(|i| {
        let sentence = base.value(i);
        if i % 200 == 0 {
            // ~0.5% of rows match `zqxwvu.*needlexyz` and `zqxwvu`.
            format!("{sentence} zqxwvu needlexyz")
        } else if i % 211 == 0 {
            // A second marker for the alternation query.
            format!("{sentence} qwerasdf")
        } else {
            sentence.to_string()
        }
    });
    StringArray::from_iter_values(docs)
}

async fn build_dataset(tempdir: &TempStrDir) -> Arc<Dataset> {
    let schema = Arc::new(Schema::new(vec![Field::new("doc", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(build_docs())]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

    let mut dataset = Dataset::write(reader, tempdir.as_str(), None)
        .await
        .unwrap();
    dataset
        .create_index(
            &["doc"],
            IndexType::NGram,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    Arc::new(dataset)
}

async fn scan_filter(dataset: &Dataset, filter: &str) -> usize {
    let mut scanner = dataset.scan();
    scanner.filter(filter).unwrap();
    let stream = scanner.try_into_stream().await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn bench_regex_ngram(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tempdir = TempStrDir::default();
    let dataset = rt.block_on(build_dataset(&tempdir));

    let queries = [
        ("selective_and", "regexp_match(doc, 'zqxwvu.*needlexyz')"),
        (
            "alternation",
            "regexp_match(doc, '(zqxwvu|qwerasdf|needlexyz)')",
        ),
        ("plain_literal", "regexp_match(doc, 'zqxwvu')"),
        ("non_accelerable_a_dot_b", "regexp_match(doc, 'a.b')"),
    ];

    let mut group = c.benchmark_group("regex_ngram");
    group
        .sample_size(10)
        .measurement_time(Duration::from_secs(15));
    for (name, filter) in queries {
        group.bench_function(name, |b| {
            b.iter(|| black_box(rt.block_on(scan_filter(&dataset, filter))));
        });
    }
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default()
        .significance_level(0.1)
        .sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_regex_ngram);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_regex_ngram);

criterion_main!(benches);
