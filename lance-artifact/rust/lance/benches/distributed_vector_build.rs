// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, FixedSizeListArray, RecordBatch, RecordBatchIterator};
use arrow_array::{cast::AsArray, types::Float32Type};
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};
use serde::Serialize;
use uuid::Uuid;

use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::{DatasetIndexExt, vector::VectorIndexParams};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::progress::noop_progress;
use lance_index::vector::kmeans::{KMeansParams, train_kmeans};
use lance_index::{
    IndexType,
    vector::{ivf::IvfBuildParams, pq::PQBuildParams},
};
use lance_linalg::distance::DistanceType;
use lance_testing::datagen::generate_random_array;
use tokio::runtime::Runtime;

const NUM_FRAGMENTS: usize = 128;
const ROWS_PER_FRAGMENT: usize = 1024;
const DIM: i32 = 128;
const NUM_SUB_VECTORS: usize = 16;
const NUM_BITS: usize = 8;
const MAX_ITERS: usize = 20;
const SAMPLE_RATE: usize = 8;

#[derive(Clone, Copy, Debug)]
struct BenchCase {
    num_shards: usize,
    num_partitions: usize,
}

impl BenchCase {
    fn label(&self) -> String {
        format!(
            "pq_shards_{}_partitions_{}",
            self.num_shards, self.num_partitions
        )
    }
}

#[derive(Clone, Debug)]
struct MergeFixture {
    index_dir: PathBuf,
    partial_aux_bytes: u64,
    partial_dir_count: usize,
}

#[derive(Debug, Serialize)]
struct CaseMetadata {
    label: String,
    num_shards: usize,
    num_partitions: usize,
    partial_dir_count: usize,
    partial_aux_bytes: u64,
    partial_aux_bytes_per_shard: u64,
    total_rows: usize,
    rows_per_shard: usize,
}

fn dataset_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "lance_bench_distributed_build_{}_{}_{}",
        NUM_FRAGMENTS, ROWS_PER_FRAGMENT, DIM
    ))
}

fn dataset_uri() -> String {
    format!("file://{}", dataset_root().display())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf()
}

fn criterion_group_root() -> PathBuf {
    workspace_root()
        .join("target")
        .join("criterion")
        .join("distributed_merge_only_ivf_pq")
}

fn bench_cases() -> [BenchCase; 6] {
    [
        BenchCase {
            num_shards: 8,
            num_partitions: 256,
        },
        BenchCase {
            num_shards: 32,
            num_partitions: 256,
        },
        BenchCase {
            num_shards: 128,
            num_partitions: 256,
        },
        BenchCase {
            num_shards: 8,
            num_partitions: 1024,
        },
        BenchCase {
            num_shards: 32,
            num_partitions: 1024,
        },
        BenchCase {
            num_shards: 128,
            num_partitions: 1024,
        },
    ]
}

fn fixture_uuid(bench_case: BenchCase) -> Uuid {
    Uuid::from_u128(
        0x733a_0000_0000_0000_0000_0000_0000_0000
            | ((bench_case.num_shards as u128) << 64)
            | bench_case.num_partitions as u128,
    )
}

fn working_uuid(bench_case: BenchCase) -> Uuid {
    Uuid::from_u128(
        0x733b_0000_0000_0000_0000_0000_0000_0000
            | ((bench_case.num_shards as u128) << 64)
            | bench_case.num_partitions as u128,
    )
}

fn create_batches() -> (Arc<ArrowSchema>, Vec<RecordBatch>) {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            FieldRef::new(Field::new("item", DataType::Float32, true)),
            DIM,
        ),
        false,
    )]));

    let batches = (0..NUM_FRAGMENTS)
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        generate_random_array(ROWS_PER_FRAGMENT * DIM as usize),
                        DIM,
                    )
                    .unwrap(),
                )],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    (schema, batches)
}

async fn create_or_open_dataset() -> Dataset {
    let uri = dataset_uri();
    if let Ok(dataset) = Dataset::open(&uri).await
        && dataset.get_fragments().len() == NUM_FRAGMENTS
    {
        return dataset;
    }

    let dataset_path = dataset_root();
    if dataset_path.exists() {
        fs::remove_dir_all(&dataset_path).unwrap();
    }

    let (schema, batches) = create_batches();
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let write_params = WriteParams {
        max_rows_per_file: ROWS_PER_FRAGMENT,
        max_rows_per_group: ROWS_PER_FRAGMENT,
        mode: WriteMode::Overwrite,
        ..Default::default()
    };

    let dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .unwrap();
    assert_eq!(dataset.get_fragments().len(), NUM_FRAGMENTS);
    dataset
}

async fn train_shared_ivf_pq(
    dataset: &Dataset,
    num_partitions: usize,
) -> (IvfBuildParams, PQBuildParams) {
    let batch = dataset
        .scan()
        .project(&["vector".to_string()])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let vectors = batch.column_by_name("vector").unwrap().as_fixed_size_list();
    let dim = vectors.value_length() as usize;
    let values = vectors.values().as_primitive::<Float32Type>();

    let kmeans = train_kmeans::<Float32Type>(
        values,
        KMeansParams::new(None, MAX_ITERS as u32, 1, DistanceType::L2),
        dim,
        num_partitions,
        SAMPLE_RATE,
    )
    .unwrap();

    let centroids = Arc::new(
        FixedSizeListArray::try_new_from_values(
            kmeans.centroids.as_primitive::<Float32Type>().clone(),
            dim as i32,
        )
        .unwrap(),
    );
    let mut ivf_params = IvfBuildParams::try_with_centroids(num_partitions, centroids).unwrap();
    ivf_params.max_iters = MAX_ITERS;
    ivf_params.sample_rate = SAMPLE_RATE;

    let mut pq_train_params = PQBuildParams::new(NUM_SUB_VECTORS, NUM_BITS);
    pq_train_params.max_iters = MAX_ITERS;
    pq_train_params.sample_rate = SAMPLE_RATE;

    let pq = pq_train_params.build(vectors, DistanceType::L2).unwrap();
    let codebook: ArrayRef = Arc::new(pq.codebook.values().as_primitive::<Float32Type>().clone());

    let mut pq_params = PQBuildParams::with_codebook(NUM_SUB_VECTORS, NUM_BITS, codebook);
    pq_params.max_iters = MAX_ITERS;
    pq_params.sample_rate = SAMPLE_RATE;

    (ivf_params, pq_params)
}

fn contiguous_fragment_groups(dataset: &Dataset, num_shards: usize) -> Vec<Vec<u32>> {
    assert_eq!(NUM_FRAGMENTS % num_shards, 0);
    let fragments = dataset.get_fragments();
    let group_size = fragments.len() / num_shards;
    fragments
        .chunks(group_size)
        .map(|group| {
            group
                .iter()
                .map(|frag| frag.id() as u32)
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn build_partial_fixture(dataset: &mut Dataset, bench_case: BenchCase) -> MergeFixture {
    let fixture_uuid = fixture_uuid(bench_case);
    let index_dir = dataset_root()
        .join("_indices")
        .join(fixture_uuid.to_string());

    if has_partial_dirs(&index_dir) {
        return MergeFixture {
            partial_aux_bytes: sum_partial_auxiliary_bytes(&index_dir),
            partial_dir_count: count_partial_dirs(&index_dir),
            index_dir,
        };
    }

    if index_dir.exists() {
        fs::remove_dir_all(&index_dir).unwrap();
    }

    let fragment_groups = contiguous_fragment_groups(dataset, bench_case.num_shards);
    let (ivf_params, pq_params) =
        Box::pin(train_shared_ivf_pq(dataset, bench_case.num_partitions)).await;
    let params = VectorIndexParams::with_ivf_pq_params(DistanceType::L2, ivf_params, pq_params);

    for fragments in fragment_groups {
        let mut builder = dataset.create_index_builder(&["vector"], IndexType::Vector, &params);
        builder = builder
            .name("distributed_merge_only".to_string())
            .fragments(fragments)
            .index_uuid(fixture_uuid);
        Box::pin(builder.execute_uncommitted()).await.unwrap();
    }

    MergeFixture {
        partial_aux_bytes: sum_partial_auxiliary_bytes(&index_dir),
        partial_dir_count: count_partial_dirs(&index_dir),
        index_dir,
    }
}

fn has_partial_dirs(index_dir: &Path) -> bool {
    fs::read_dir(index_dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| {
            entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && entry.file_name().to_string_lossy().starts_with("partial_")
        })
}

fn count_partial_dirs(index_dir: &Path) -> usize {
    fs::read_dir(index_dir)
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && entry.file_name().to_string_lossy().starts_with("partial_")
        })
        .count()
}

fn sum_partial_auxiliary_bytes(index_dir: &Path) -> u64 {
    fs::read_dir(index_dir)
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && entry.file_name().to_string_lossy().starts_with("partial_")
        })
        .map(|entry| entry.path().join("auxiliary.idx"))
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn copy_dir_recursive(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap().flatten() {
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &target_path);
        } else {
            fs::copy(&source_path, &target_path).unwrap();
        }
    }
}

fn prepare_iteration_target(source: &Path, target: &Path) {
    if target.exists() {
        fs::remove_dir_all(target).unwrap();
    }
    copy_dir_recursive(source, target);
}

fn write_case_metadata(fixtures: &[(BenchCase, MergeFixture)]) {
    let output_dir = criterion_group_root();
    fs::create_dir_all(&output_dir).unwrap();
    let metadata = fixtures
        .iter()
        .map(|(bench_case, fixture)| CaseMetadata {
            label: bench_case.label(),
            num_shards: bench_case.num_shards,
            num_partitions: bench_case.num_partitions,
            partial_dir_count: fixture.partial_dir_count,
            partial_aux_bytes: fixture.partial_aux_bytes,
            partial_aux_bytes_per_shard: fixture.partial_aux_bytes
                / fixture.partial_dir_count as u64,
            total_rows: NUM_FRAGMENTS * ROWS_PER_FRAGMENT,
            rows_per_shard: (NUM_FRAGMENTS * ROWS_PER_FRAGMENT) / bench_case.num_shards,
        })
        .collect::<Vec<_>>();
    let payload = serde_json::to_vec_pretty(&metadata).unwrap();
    fs::write(output_dir.join("case_metadata.json"), payload).unwrap();
}

fn bench_distributed_merge_only(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut dataset = rt.block_on(create_or_open_dataset());
    let mut fixtures = Vec::new();

    for bench_case in bench_cases() {
        fixtures.push((
            bench_case,
            rt.block_on(build_partial_fixture(&mut dataset, bench_case)),
        ));
    }
    write_case_metadata(&fixtures);

    let dataset = Arc::new(dataset);
    let mut group = c.benchmark_group("distributed_merge_only_ivf_pq");
    group.sample_size(10);

    for (bench_case, fixture) in fixtures {
        let target_uuid = working_uuid(bench_case);
        let target_index_dir_fs = dataset_root()
            .join("_indices")
            .join(target_uuid.to_string());
        let source_index_dir_fs = fixture.index_dir.clone();

        group.throughput(Throughput::Bytes(fixture.partial_aux_bytes));
        group.bench_with_input(
            BenchmarkId::new("finalize_only", bench_case.label()),
            &bench_case,
            |b, _| {
                let dataset = dataset.clone();
                let target_index_dir_fs = target_index_dir_fs.clone();
                let source_index_dir_fs = source_index_dir_fs.clone();
                b.iter_batched(
                    || prepare_iteration_target(&source_index_dir_fs, &target_index_dir_fs),
                    |_| {
                        rt.block_on(dataset.merge_index_metadata(
                            &target_uuid,
                            IndexType::IvfPq,
                            None,
                            noop_progress(),
                        ))
                        .unwrap();
                    },
                    BatchSize::PerIteration,
                );
            },
        );
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
    targets = bench_distributed_merge_only
);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name = benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_distributed_merge_only
);

criterion_main!(benches);
