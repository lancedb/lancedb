// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![recursion_limit = "256"]

/// This is a rust end-to-end benchmark for full text search.  It is meant to be supplementary to the
/// python benchmark located at python/python/ci_benchmarks/benchmarks/test_fts_search.py.  You can use
/// the python/python/ci_benchmarks/datagen/wikipedia.py script to generate the dataset.  You will need
/// to set the LANCE_WIKIPEDIA_DATASET_PATH environment variable to the path of the dataset generated
/// by that script.
///
/// This benchmark is primarily intended for developers to use for profiling and debugging.  The python
/// benchmark is more comprehensive and will cover regression testing.
use std::{collections::BTreeSet, env, sync::Arc};

use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use lance::{Dataset, dataset::WriteParams, index::DatasetIndexExt};
use lance_index::{
    IndexType,
    scalar::{FullTextSearchQuery, inverted::tokenizer::InvertedIndexParams},
};
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use tempfile::TempDir;

const WIKIPEDIA_DATASET_ENV_VAR: &str = "LANCE_WIKIPEDIA_DATASET_PATH";
const INDEX_NAME: &str = "segmented_fts";
const PARTITION_COMPARE_INDEX_NAME: &str = "partition_shape_fts";
const INDEXED_FRAGMENT_COUNT: usize = 12;
const UNINDEXED_FRAGMENT_COUNT: usize = 1;
const ROWS_PER_FRAGMENT: usize = 64;
const PARTITION_COMPARE_INDEXED_FRAGMENT_COUNT: usize = 64;
const PARTITION_COMPARE_UNINDEXED_FRAGMENT_COUNT: usize = 1;
const PARTITION_COMPARE_ROWS_PER_FRAGMENT: usize = 512;
const PARTITION_COMPARE_TOKEN_REPEAT: usize = 1024;

/// Get the Wikipedia dataset path from environment variable.
/// Panics if the environment variable is not set.
fn get_wikipedia_dataset_path() -> String {
    env::var(WIKIPEDIA_DATASET_ENV_VAR).unwrap_or_else(|_| {
        panic!(
            "Environment variable {} must be set to the path of the indexed Wikipedia dataset",
            WIKIPEDIA_DATASET_ENV_VAR
        )
    })
}

struct BenchDataset {
    _tmpdir: TempDir,
    dataset: Dataset,
}

fn create_fragment_batch(fragment_id: usize) -> RecordBatch {
    let start = (fragment_id * ROWS_PER_FRAGMENT) as i32;
    let ids = Arc::new(Int32Array::from_iter_values(
        start..start + ROWS_PER_FRAGMENT as i32,
    ));
    let texts = Arc::new(StringArray::from_iter_values((0..ROWS_PER_FRAGMENT).map(
        |row| {
            let term = match (fragment_id + row) % 4 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                _ => "delta",
            };
            format!("shared {term} fragment-{fragment_id} row-{row}")
        },
    )));
    RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", texts as ArrayRef)]).unwrap()
}

fn create_partition_compare_fragment_batch(fragment_id: usize) -> RecordBatch {
    let start = (fragment_id * PARTITION_COMPARE_ROWS_PER_FRAGMENT) as i32;
    let ids = Arc::new(Int32Array::from_iter_values(
        start..start + PARTITION_COMPARE_ROWS_PER_FRAGMENT as i32,
    ));
    let texts = Arc::new(StringArray::from_iter_values(
        (0..PARTITION_COMPARE_ROWS_PER_FRAGMENT).map(|row| {
            let term = match (fragment_id + row) % 4 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                _ => "delta",
            };
            let unique = format!("fragment-{fragment_id} row-{row}");
            let repeated = std::iter::repeat_n(
                format!("shared {term} {unique}"),
                PARTITION_COMPARE_TOKEN_REPEAT,
            )
            .collect::<Vec<_>>()
            .join(" ");
            format!("{repeated} tail-{term}")
        }),
    ));
    RecordBatch::try_from_iter(vec![("id", ids as ArrayRef), ("text", texts as ArrayRef)]).unwrap()
}

fn grouped_fragment_ids(total_fragments: usize, segment_count: usize) -> Vec<Vec<u32>> {
    let fragments_per_segment = total_fragments / segment_count;
    (0..segment_count)
        .map(|segment_idx| {
            let start = segment_idx * fragments_per_segment;
            let end = start + fragments_per_segment;
            (start..end).map(|fragment_id| fragment_id as u32).collect()
        })
        .collect()
}

async fn build_segmented_fts_dataset(segment_count: usize) -> BenchDataset {
    let tmpdir = TempDir::new().unwrap();
    let uri = format!("file://{}", tmpdir.path().display());
    let batches = RecordBatchIterator::new(
        (0..(INDEXED_FRAGMENT_COUNT + UNINDEXED_FRAGMENT_COUNT))
            .map(|fragment_id| Ok(create_fragment_batch(fragment_id)))
            .collect::<Vec<_>>(),
        create_fragment_batch(0).schema(),
    );
    let mut dataset = Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            max_rows_per_file: ROWS_PER_FRAGMENT,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        dataset.get_fragments().len(),
        INDEXED_FRAGMENT_COUNT + UNINDEXED_FRAGMENT_COUNT
    );

    let params = InvertedIndexParams::default();
    let mut staged_segments = Vec::with_capacity(segment_count);
    for fragment_ids in grouped_fragment_ids(INDEXED_FRAGMENT_COUNT, segment_count) {
        let segment = dataset
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name(INDEX_NAME.to_string())
            .fragments(fragment_ids)
            .execute_uncommitted()
            .await
            .unwrap();
        staged_segments.push(segment);
    }
    dataset
        .commit_existing_index_segments(INDEX_NAME, "text", staged_segments)
        .await
        .unwrap();

    BenchDataset {
        _tmpdir: tmpdir,
        dataset,
    }
}

fn count_partitions(segment: &lance_table::format::IndexMetadata) -> usize {
    segment
        .files
        .as_ref()
        .map(|files| {
            files
                .iter()
                .filter_map(|file| {
                    file.path
                        .strip_prefix("part_")
                        .and_then(|path| path.split_once('_'))
                        .map(|(partition_id, _)| partition_id.to_string())
                })
                .collect::<BTreeSet<_>>()
                .len()
        })
        .unwrap_or(0)
}

async fn build_partition_compare_dataset_with_memory_limit(
    partition_count: usize,
    segmented: bool,
    memory_limit_mb: u64,
) -> BenchDataset {
    let tmpdir = TempDir::new().unwrap();
    let uri = format!("file://{}", tmpdir.path().display());
    let batches = RecordBatchIterator::new(
        (0..(PARTITION_COMPARE_INDEXED_FRAGMENT_COUNT
            + PARTITION_COMPARE_UNINDEXED_FRAGMENT_COUNT))
            .map(|fragment_id| Ok(create_partition_compare_fragment_batch(fragment_id)))
            .collect::<Vec<_>>(),
        create_partition_compare_fragment_batch(0).schema(),
    );
    let mut dataset = Dataset::write(
        batches,
        &uri,
        Some(WriteParams {
            max_rows_per_file: PARTITION_COMPARE_ROWS_PER_FRAGMENT,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let fragment_groups = if segmented {
        grouped_fragment_ids(PARTITION_COMPARE_INDEXED_FRAGMENT_COUNT, partition_count)
    } else {
        vec![(0..PARTITION_COMPARE_INDEXED_FRAGMENT_COUNT as u32).collect()]
    };
    let params = InvertedIndexParams::default()
        .with_position(true)
        .num_workers(1)
        .memory_limit_mb(memory_limit_mb);

    let mut staged_segments = Vec::with_capacity(fragment_groups.len());
    for fragment_ids in fragment_groups {
        let segment = dataset
            .create_index_builder(&["text"], IndexType::Inverted, &params)
            .name(PARTITION_COMPARE_INDEX_NAME.to_string())
            .fragments(fragment_ids)
            .execute_uncommitted()
            .await
            .unwrap();
        staged_segments.push(segment);
    }
    dataset
        .commit_existing_index_segments(PARTITION_COMPARE_INDEX_NAME, "text", staged_segments)
        .await
        .unwrap();

    let committed_segments = dataset
        .load_indices_by_name(PARTITION_COMPARE_INDEX_NAME)
        .await
        .unwrap();
    if segmented {
        assert_eq!(committed_segments.len(), partition_count);
        for segment in &committed_segments {
            assert_eq!(
                count_partitions(segment),
                1,
                "expected each segmented FTS segment to have exactly one partition"
            );
        }
    }

    BenchDataset {
        _tmpdir: tmpdir,
        dataset,
    }
}

async fn build_partition_compare_dataset(partition_count: usize, segmented: bool) -> BenchDataset {
    if segmented {
        return build_partition_compare_dataset_with_memory_limit(partition_count, true, 512).await;
    }

    let mut observed = Vec::new();
    for memory_limit_mb in [
        512, 256, 192, 160, 128, 96, 80, 64, 56, 48, 40, 36, 32, 28, 24, 20, 18, 16, 14, 12, 10, 9,
        8, 7, 6, 5, 4, 3, 2, 1,
    ] {
        let bench_dataset = build_partition_compare_dataset_with_memory_limit(
            partition_count,
            false,
            memory_limit_mb,
        )
        .await;
        let committed_segments = bench_dataset
            .dataset
            .load_indices_by_name(PARTITION_COMPARE_INDEX_NAME)
            .await
            .unwrap();
        let actual_partition_count = if committed_segments.len() == 1 {
            count_partitions(&committed_segments[0])
        } else {
            0
        };
        observed.push((
            memory_limit_mb,
            committed_segments.len(),
            actual_partition_count,
        ));
        if committed_segments.len() == 1 && actual_partition_count == partition_count {
            return bench_dataset;
        }
    }

    panic!(
        "failed to build 1 segment x {partition_count} partitions for partition-shape benchmark: {observed:?}"
    );
}

/// Benchmark full text search on Wikipedia dataset with different K values
fn bench_fts_search(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dataset_path = get_wikipedia_dataset_path();

    // Open the dataset once
    let dataset = rt
        .block_on(Dataset::open(&dataset_path))
        .unwrap_or_else(|e| {
            panic!(
                "Failed to open Wikipedia dataset at '{}': {}",
                dataset_path, e
            )
        });

    // Test with different K values
    let k_values = [10, 100, 1000];

    let mut group = c.benchmark_group("fts_search_lost_episode");

    for k in k_values.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(k), k, |b, &k| {
            b.iter(|| {
                rt.block_on(async {
                    let mut scanner = dataset.scan();
                    let mut stream = scanner
                        .full_text_search(FullTextSearchQuery::new("lost episode".to_string()))
                        .unwrap()
                        .limit(Some(k as i64), None)
                        .unwrap()
                        .project(&["_rowid"])
                        .unwrap()
                        .try_into_stream()
                        .await
                        .unwrap();

                    let mut num_rows = 0;
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        num_rows += batch.num_rows();
                    }

                    // Verify we got results (should be at most k rows)
                    assert!(
                        num_rows <= k,
                        "Expected at most {} rows, got {}",
                        k,
                        num_rows
                    );
                })
            });
        });
    }

    group.finish();
}

fn bench_segmented_fts_search(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bench_datasets = [1_usize, 2, 4, 6]
        .into_iter()
        .map(|segment_count| {
            (
                segment_count,
                rt.block_on(build_segmented_fts_dataset(segment_count)),
            )
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("fts_search_segment_count");
    for (segment_count, bench_dataset) in &bench_datasets {
        group.bench_with_input(
            BenchmarkId::from_parameter(segment_count),
            segment_count,
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut scanner = bench_dataset.dataset.scan();
                        let query = FullTextSearchQuery::new("shared alpha".to_string())
                            .with_column("text".to_string())
                            .unwrap();
                        let mut stream = scanner
                            .full_text_search(query)
                            .unwrap()
                            .limit(Some(20), None)
                            .unwrap()
                            .project(&["_rowid"])
                            .unwrap()
                            .try_into_stream()
                            .await
                            .unwrap();

                        let mut num_rows = 0;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            num_rows += batch.num_rows();
                        }
                        assert!(num_rows <= 20);
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_fts_segment_vs_partition_shape(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bench_datasets = [16_usize]
        .into_iter()
        .flat_map(|count| {
            [
                (
                    format!("{count}_segments_x_1_partition"),
                    rt.block_on(build_partition_compare_dataset(count, true)),
                ),
                (
                    format!("1_segment_x_{count}_partitions"),
                    rt.block_on(build_partition_compare_dataset(count, false)),
                ),
            ]
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("fts_search_segment_vs_partition_shape");
    for (shape, bench_dataset) in &bench_datasets {
        group.bench_with_input(BenchmarkId::from_parameter(shape), shape, |b, _| {
            b.iter(|| {
                rt.block_on(async {
                    let mut scanner = bench_dataset.dataset.scan();
                    let query = FullTextSearchQuery::new("shared alpha".to_string())
                        .with_column("text".to_string())
                        .unwrap();
                    let mut stream = scanner
                        .full_text_search(query)
                        .unwrap()
                        .limit(Some(20), None)
                        .unwrap()
                        .project(&["_rowid"])
                        .unwrap()
                        .try_into_stream()
                        .await
                        .unwrap();

                    let mut num_rows = 0;
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        num_rows += batch.num_rows();
                    }
                    assert!(num_rows <= 20);
                })
            });
        });
    }
    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_fts_search, bench_segmented_fts_search, bench_fts_segment_vs_partition_shape
);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_fts_search, bench_segmented_fts_search, bench_fts_segment_vs_partition_shape
);

criterion_main!(benches);
