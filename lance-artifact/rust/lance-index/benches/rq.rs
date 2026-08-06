// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmark of building PQ distance table.

use std::time::Duration;

use arrow::datatypes::UInt64Type;
use arrow_array::types::Float32Type;
use arrow_schema::DataType;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lance_arrow::fixed_size_list_type;
use lance_core::ROW_ID;
use lance_datagen::array::rand_type;
use lance_datagen::{BatchGeneratorBuilder, RowCount};
use lance_index::vector::bq::RQRotationType;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::ex_dot::{
    blocked_ex_code_bytes, ex_dot_kernel, pack_blocked_row, packed_ex_code_value,
};
use lance_index::vector::bq::storage::*;
use lance_index::vector::bq::transform::{ADD_FACTORS_COLUMN, SCALE_FACTORS_COLUMN};
use lance_index::vector::quantizer::{Quantization, QuantizerStorage};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_linalg::distance::DistanceType;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const DIM: usize = 128;
const TOTAL: usize = 16 * 1000;

fn mock_rq_storage(num_bits: u8, rotation_type: RQRotationType) -> RabitQuantizationStorage {
    // generate random rq codes
    let rq = RabitQuantizer::new_with_rotation::<Float32Type>(num_bits, DIM as i32, rotation_type);
    let builder = BatchGeneratorBuilder::new()
        .col(ROW_ID, lance_datagen::array::step::<UInt64Type>())
        .col(
            RABIT_CODE_COLUMN,
            rand_type(&fixed_size_list_type(
                (DIM * num_bits as usize / u8::BITS as usize) as i32,
                DataType::UInt8,
            )),
        )
        .col(ADD_FACTORS_COLUMN, rand_type(&DataType::Float32))
        .col(SCALE_FACTORS_COLUMN, rand_type(&DataType::Float32));
    RabitQuantizationStorage::try_from_batch(
        builder
            .into_batch_rows(RowCount::from(TOTAL as u64))
            .unwrap(),
        &rq.metadata(None),
        DistanceType::L2,
        None,
    )
    .unwrap()
}

fn construct_dist_table(c: &mut Criterion) {
    let rotation_types = [RQRotationType::Fast, RQRotationType::Matrix];
    for num_bits in 1..=1 {
        for rotation_type in rotation_types {
            let rq = mock_rq_storage(num_bits, rotation_type);
            let query = rand_type(&DataType::Float32)
                .generate_default(RowCount::from(DIM as u64))
                .unwrap();
            c.bench_function(
                format!(
                    "RQ{}({:?}): construct_dist_table: {},DIM={}",
                    num_bits,
                    rotation_type,
                    DistanceType::L2,
                    DIM
                )
                .as_str(),
                |b| {
                    b.iter(|| {
                        black_box(rq.dist_calculator(query.clone(), 0.0));
                    })
                },
            );
        }
    }
}

fn compute_distances(c: &mut Criterion) {
    let rotation_types = [RQRotationType::Fast, RQRotationType::Matrix];
    for num_bits in 1..=1 {
        for rotation_type in rotation_types {
            let rq = mock_rq_storage(num_bits, rotation_type);
            let query = rand_type(&DataType::Float32)
                .generate_default(RowCount::from(DIM as u64))
                .unwrap();
            let dist_calc = rq.dist_calculator(query.clone(), 0.0);

            c.bench_function(
                format!(
                    "RQ{}({:?}): compute_distances: {},DIM={}",
                    num_bits, rotation_type, TOTAL, DIM
                )
                .as_str(),
                |b| {
                    b.iter(|| {
                        black_box(dist_calc.distance_all(0));
                    })
                },
            );

            c.bench_function(
                format!(
                    "RQ{}({:?}): compute_distances_single: {},DIM={}",
                    num_bits, rotation_type, TOTAL, DIM
                )
                .as_str(),
                |b| {
                    b.iter(|| {
                        for i in 0..TOTAL {
                            black_box(dist_calc.distance(i as u32));
                        }
                    })
                },
            );
        }
    }
}

/// The table-gather ex distance used before the dedicated ex-dot kernels,
/// kept here as the baseline: per dim, extract the packed code and gather
/// `query[d] * code` from a `dim * 2^ex_bits` table.
fn gather_ex_distance(row_codes: &[u8], dim: usize, ex_bits: u8, ex_dist_table: &[f32]) -> f32 {
    let entries_per_dim = 1usize << ex_bits;
    (0..dim)
        .map(|dim_idx| {
            let code = packed_ex_code_value(row_codes, dim_idx, ex_bits) as usize;
            ex_dist_table[dim_idx * entries_per_dim + code]
        })
        .sum()
}

fn ex_dot_kernels(c: &mut Criterion) {
    for ex_dim in [1536usize, 2048] {
        ex_dot_kernels_for_dim(c, ex_dim);
    }
}

fn ex_dot_kernels_for_dim(c: &mut Criterion, ex_dim: usize) {
    const NUM_ROWS: usize = 1024;

    let mut rng = SmallRng::seed_from_u64(42);
    let query = (0..ex_dim)
        .map(|_| rng.random_range(-1.0f32..1.0))
        .collect::<Vec<_>>();

    for ex_bits in 1..=8u8 {
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        let values = (0..NUM_ROWS * ex_dim)
            .map(|_| rng.random_range(0..=max_code))
            .collect::<Vec<_>>();

        // The gather baseline reads the legacy sequential layout it shipped
        // with; the kernel reads the blocked layout.
        let seq_code_len = (ex_dim * ex_bits as usize).div_ceil(8);
        let mut seq_codes = vec![0u8; NUM_ROWS * seq_code_len];
        for (row, row_values) in seq_codes
            .chunks_exact_mut(seq_code_len)
            .zip(values.chunks_exact(ex_dim))
        {
            for (dim, &value) in row_values.iter().enumerate() {
                let bit_offset = dim * ex_bits as usize;
                let bits = (value as u16) << (bit_offset % 8);
                row[bit_offset / 8] |= bits as u8;
                if bits >> 8 != 0 {
                    row[bit_offset / 8 + 1] |= (bits >> 8) as u8;
                }
            }
        }

        let kernel_code_len = blocked_ex_code_bytes(ex_dim, ex_bits);
        let mut kernel_codes = vec![0u8; NUM_ROWS * kernel_code_len];
        for (row, row_values) in kernel_codes
            .chunks_exact_mut(kernel_code_len)
            .zip(values.chunks_exact(ex_dim))
        {
            pack_blocked_row(row_values, ex_bits, row);
        }

        // ex_dim is block-aligned here, so the kernels read the query as-is.
        let ex_query = &query;
        let kernel = ex_dot_kernel(ex_bits);
        c.bench_function(
            format!("RQ ex_dot kernel: ex_bits={ex_bits}, DIM={ex_dim}, rows={NUM_ROWS}").as_str(),
            |b| {
                b.iter(|| {
                    let mut sum = 0.0f32;
                    for row in kernel_codes.chunks_exact(kernel_code_len) {
                        sum += kernel(ex_query, row);
                    }
                    black_box(sum)
                })
            },
        );

        let entries_per_dim = 1usize << ex_bits;
        let mut ex_dist_table = vec![0.0f32; ex_dim * entries_per_dim];
        for (dim, table) in ex_dist_table.chunks_exact_mut(entries_per_dim).enumerate() {
            for (code, value) in table.iter_mut().enumerate() {
                *value = query[dim] * code as f32;
            }
        }
        c.bench_function(
            format!("RQ ex_dot table-gather: ex_bits={ex_bits}, DIM={ex_dim}, rows={NUM_ROWS}")
                .as_str(),
            |b| {
                b.iter(|| {
                    let mut sum = 0.0f32;
                    for row in seq_codes.chunks_exact(seq_code_len) {
                        sum += gather_ex_distance(row, ex_dim, ex_bits, &ex_dist_table);
                    }
                    black_box(sum)
                })
            },
        );
    }
}

/// Storage load cost per format: blocked-format ex codes are aliased as-is,
/// legacy sequential ex codes are repacked row by row.
fn ex_code_storage_load(c: &mut Criterion) {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, UInt8Array, UInt64Array};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::bq::ex_dot::repack_sequential_row;
    use lance_index::vector::bq::rabit_ex_code_bytes;
    use lance_index::vector::bq::transform::{EX_ADD_FACTORS_COLUMN, EX_SCALE_FACTORS_COLUMN};
    use std::sync::Arc;

    const LOAD_DIM: usize = 1536;
    const LOAD_ROWS: usize = 8192;
    const NUM_BITS: u8 = 4; // ex_bits=3, a bit-plane width

    let ex_bits = NUM_BITS - 1;
    let mut rng = SmallRng::seed_from_u64(7);
    let metadata = RabitQuantizationMetadata {
        rotate_mat: None,
        rotate_mat_position: None,
        fast_rotation_signs: None,
        rotation_type: RQRotationType::Fast,
        code_dim: LOAD_DIM as u32,
        num_bits: NUM_BITS,
        packed: true,
        query_estimator: RabitQueryEstimator::RawQuery,
    };
    let code_len = LOAD_DIM / 8;
    let binary_codes = (0..LOAD_ROWS * code_len)
        .map(|_| rng.random_range(0..=u8::MAX))
        .collect::<Vec<_>>();
    let seq_code_len = rabit_ex_code_bytes(LOAD_DIM, ex_bits).unwrap();
    let seq_codes = (0..LOAD_ROWS * seq_code_len)
        .map(|_| rng.random_range(0..=u8::MAX))
        .collect::<Vec<_>>();
    let blocked_code_len = blocked_ex_code_bytes(LOAD_DIM, ex_bits);
    let mut blocked_codes = vec![0u8; LOAD_ROWS * blocked_code_len];
    for (seq_row, blocked_row) in seq_codes
        .chunks_exact(seq_code_len)
        .zip(blocked_codes.chunks_exact_mut(blocked_code_len))
    {
        repack_sequential_row(seq_row, LOAD_DIM, ex_bits, blocked_row);
    }

    let make_batch = |ex_column: &str, ex_values: Vec<u8>, ex_code_len: usize| {
        arrow_array::RecordBatch::try_from_iter(vec![
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..LOAD_ROWS as u64)) as ArrayRef,
            ),
            (
                RABIT_CODE_COLUMN,
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(binary_codes.clone()),
                        code_len as i32,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; LOAD_ROWS])) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; LOAD_ROWS])) as ArrayRef,
            ),
            (
                ex_column,
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(ex_values),
                        ex_code_len as i32,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                EX_ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; LOAD_ROWS])) as ArrayRef,
            ),
            (
                EX_SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; LOAD_ROWS])) as ArrayRef,
            ),
        ])
        .unwrap()
    };

    let blocked_batch = make_batch(
        RABIT_BLOCKED_EX_CODE_COLUMN,
        blocked_codes,
        blocked_code_len,
    );
    c.bench_function(
        format!("RQ storage load (blocked ex codes): num_bits={NUM_BITS}, DIM={LOAD_DIM}, rows={LOAD_ROWS}")
            .as_str(),
        |b| {
            b.iter(|| {
                black_box(
                    RabitQuantizationStorage::try_from_batch(
                        blocked_batch.clone(),
                        &metadata,
                        DistanceType::L2,
                        None,
                    )
                    .unwrap(),
                )
            })
        },
    );

    let legacy_batch = make_batch(RABIT_EX_CODE_COLUMN, seq_codes, seq_code_len);
    c.bench_function(
        format!("RQ storage load (legacy ex codes): num_bits={NUM_BITS}, DIM={LOAD_DIM}, rows={LOAD_ROWS}")
            .as_str(),
        |b| {
            b.iter(|| {
                black_box(
                    RabitQuantizationStorage::try_from_batch(
                        legacy_batch.clone(),
                        &metadata,
                        DistanceType::L2,
                        None,
                    )
                    .unwrap(),
                )
            })
        },
    );
}

/// Bulk-scoring cost of the ex stage: the quantized ex-FastScan LUT path
/// (inside `distance_all`) vs the exact per-row ex-dot kernel. The
/// binary-only run isolates the shared binary stage so the ex cost is the
/// difference from the full run.
fn ex_bulk_paths(c: &mut Criterion) {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, UInt8Array, UInt64Array};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::ApproxMode;
    use lance_index::vector::bq::ex_dot::pad_query_into;
    use lance_index::vector::bq::transform::{EX_ADD_FACTORS_COLUMN, EX_SCALE_FACTORS_COLUMN};
    use lance_index::vector::storage::DistanceCalculatorOptions;
    use std::sync::Arc;

    const BULK_DIM: usize = 1536;
    const BULK_ROWS: usize = 16384;

    let mut rng = SmallRng::seed_from_u64(13);
    for num_bits in [3u8, 5, 9] {
        let ex_bits = num_bits - 1;
        let max_code = ((1u16 << ex_bits) - 1) as u8;

        let rq = RabitQuantizer::new_with_rotation::<Float32Type>(
            num_bits,
            BULK_DIM as i32,
            RQRotationType::Fast,
        );
        let metadata = rq.metadata(None);

        let code_len = BULK_DIM / 8;
        let binary_codes = (0..BULK_ROWS * code_len)
            .map(|_| rng.random_range(0..=u8::MAX))
            .collect::<Vec<_>>();
        let ex_code_len = blocked_ex_code_bytes(BULK_DIM, ex_bits);
        let mut ex_codes = vec![0u8; BULK_ROWS * ex_code_len];
        let values = (0..BULK_DIM)
            .map(|_| rng.random_range(0..=max_code))
            .collect::<Vec<_>>();
        for row in ex_codes.chunks_exact_mut(ex_code_len) {
            pack_blocked_row(&values, ex_bits, row);
        }

        // No error factors: `distance_all` takes the FastScan ex bulk branch.
        let batch = arrow_array::RecordBatch::try_from_iter(vec![
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..BULK_ROWS as u64)) as ArrayRef,
            ),
            (
                RABIT_CODE_COLUMN,
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(binary_codes),
                        code_len as i32,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; BULK_ROWS])) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; BULK_ROWS])) as ArrayRef,
            ),
            (
                RABIT_BLOCKED_EX_CODE_COLUMN,
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(ex_codes.clone()),
                        ex_code_len as i32,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                EX_ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0f32; BULK_ROWS])) as ArrayRef,
            ),
            (
                EX_SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![1.0f32; BULK_ROWS])) as ArrayRef,
            ),
        ])
        .unwrap();
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();

        let query: ArrayRef = Arc::new(Float32Array::from(
            (0..BULK_DIM)
                .map(|_| rng.random_range(-1.0f32..1.0))
                .collect::<Vec<_>>(),
        ));

        for (label, approx_mode) in [
            ("full distance_all (binary + ex LUT)", ApproxMode::Normal),
            ("binary-only distance_all (fast mode)", ApproxMode::Fast),
        ] {
            let mut f32_scratch = Vec::new();
            let calc = storage.dist_calculator_with_scratch(
                query.clone(),
                0.0,
                None,
                &mut f32_scratch,
                DistanceCalculatorOptions { approx_mode },
            );
            let mut dists = Vec::new();
            let mut u16_scratch = Vec::new();
            let mut u8_scratch = Vec::new();
            let mut u32_scratch = Vec::new();
            c.bench_function(
                format!("RQ bulk {label}: num_bits={num_bits}, DIM={BULK_DIM}, rows={BULK_ROWS}")
                    .as_str(),
                |b| {
                    b.iter(|| {
                        calc.distance_all_with_scratch(
                            0,
                            &mut dists,
                            &mut u16_scratch,
                            &mut u8_scratch,
                            &mut u32_scratch,
                        );
                        black_box(dists.len())
                    })
                },
            );
        }

        let kernel = ex_dot_kernel(ex_bits);
        let mut ex_query = vec![0.0f32; BULK_DIM];
        pad_query_into(
            query
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values(),
            &mut ex_query,
        );
        c.bench_function(
            format!(
                "RQ bulk ex kernel loop: num_bits={num_bits}, DIM={BULK_DIM}, rows={BULK_ROWS}"
            )
            .as_str(),
            |b| {
                b.iter(|| {
                    let mut sum = 0.0f32;
                    for row in ex_codes.chunks_exact(ex_code_len) {
                        sum += kernel(&ex_query, row);
                    }
                    black_box(sum)
                })
            },
        );
    }
}

/// Top-k accumulation through the gated raw-query multi-bit path: binary
/// FastScan, the per-row lower-bound pruning scan, and the exact rerank of
/// the surviving rows. Error factors are present so the gating is enabled.
fn heap_topk(c: &mut Criterion) {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, UInt8Array, UInt64Array};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::ApproxMode;
    use lance_index::vector::bq::transform::{
        ERROR_FACTORS_COLUMN, EX_ADD_FACTORS_COLUMN, EX_SCALE_FACTORS_COLUMN,
    };
    use lance_index::vector::storage::DistanceCalculatorOptions;
    use std::collections::BinaryHeap;
    use std::sync::Arc;

    const TOPK_DIM: usize = 1536;
    const TOPK_ROWS: usize = 4096;
    const TOPK_K: usize = 10;
    const NUM_BITS: u8 = 5;
    let ex_bits = NUM_BITS - 1;

    let mut rng = SmallRng::seed_from_u64(99);
    let rq = RabitQuantizer::new_with_rotation::<Float32Type>(
        NUM_BITS,
        TOPK_DIM as i32,
        RQRotationType::Fast,
    );
    let metadata = rq.metadata(None);

    let code_len = TOPK_DIM / 8;
    let binary_codes = (0..TOPK_ROWS * code_len)
        .map(|_| rng.random())
        .collect::<Vec<u8>>();
    let ex_code_len = blocked_ex_code_bytes(TOPK_DIM, ex_bits);
    let ex_codes = (0..TOPK_ROWS * ex_code_len)
        .map(|_| rng.random())
        .collect::<Vec<u8>>();
    // Factor magnitudes chosen so the lower bounds spread mostly with the add
    // factors; once the heap is full the threshold prunes the vast majority
    // of rows, like a production multi-partition scan.
    let mut rand_factors = |low: f32, high: f32| {
        Arc::new(Float32Array::from(
            (0..TOPK_ROWS)
                .map(|_| rng.random_range(low..high))
                .collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let batch = arrow_array::RecordBatch::try_from_iter(vec![
        (
            ROW_ID,
            Arc::new(UInt64Array::from_iter_values(0..TOPK_ROWS as u64)) as ArrayRef,
        ),
        (
            RABIT_CODE_COLUMN,
            Arc::new(
                FixedSizeListArray::try_new_from_values(
                    UInt8Array::from(binary_codes),
                    code_len as i32,
                )
                .unwrap(),
            ) as ArrayRef,
        ),
        (ADD_FACTORS_COLUMN, rand_factors(0.0, 1.0)),
        (SCALE_FACTORS_COLUMN, rand_factors(0.0005, 0.0015)),
        (ERROR_FACTORS_COLUMN, rand_factors(0.0, 0.01)),
        (
            RABIT_BLOCKED_EX_CODE_COLUMN,
            Arc::new(
                FixedSizeListArray::try_new_from_values(
                    UInt8Array::from(ex_codes),
                    ex_code_len as i32,
                )
                .unwrap(),
            ) as ArrayRef,
        ),
        (EX_ADD_FACTORS_COLUMN, rand_factors(0.0, 1.0)),
        (EX_SCALE_FACTORS_COLUMN, rand_factors(0.00003, 0.0001)),
    ])
    .unwrap();
    let storage =
        RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None).unwrap();
    let query: ArrayRef = Arc::new(Float32Array::from(
        (0..TOPK_DIM)
            .map(|_| rng.random_range(-1.0f32..1.0))
            .collect::<Vec<_>>(),
    ));

    for (label, approx_mode) in [
        ("normal", ApproxMode::Normal),
        ("accurate", ApproxMode::Accurate),
    ] {
        let mut f32_scratch = Vec::new();
        let calc = storage.dist_calculator_with_scratch(
            query.clone(),
            1.0,
            None,
            &mut f32_scratch,
            DistanceCalculatorOptions { approx_mode },
        );
        let mut heap = BinaryHeap::with_capacity(TOPK_K + 1);
        let mut dists = Vec::new();
        let mut u16_scratch = Vec::new();
        let mut u8_scratch = Vec::new();
        let mut u32_scratch = Vec::new();
        c.bench_function(
            format!(
                "RQ heap topk ({label}): num_bits={NUM_BITS}, DIM={TOPK_DIM}, rows={TOPK_ROWS}, k={TOPK_K}"
            )
            .as_str(),
            |b| {
                b.iter(|| {
                    heap.clear();
                    calc.accumulate_topk_with_scratch(
                        TOPK_K,
                        None,
                        None,
                        |id| id as u64,
                        &mut heap,
                        &mut dists,
                        &mut u16_scratch,
                        &mut u8_scratch,
                        &mut u32_scratch,
                    );
                    black_box(heap.len())
                })
            },
        );
    }
}

criterion_group!(
    name=benches;
    config = Criterion::default().measurement_time(Duration::from_secs(10));
    targets = construct_dist_table, compute_distances, ex_dot_kernels, ex_code_storage_load, ex_bulk_paths, heap_topk);

criterion_main!(benches);
