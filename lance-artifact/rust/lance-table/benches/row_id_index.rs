// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// TODO:
// - [x] Create base cases with HashMap
// - [x] Create on-disk size measurement
// - [x] Create different cases for the index. Ideal, 25% deletions, 80% deletions + compaction.
// - [ ] Create a benchmark for the get method
//   - [x] Average over all valid values
//   - [ ] Time to get a value that is not in the index
// - [ ] Create a benchmark for the new method (building the in-memory index)
// Optional:
// - [ ] Create in-memory size measurement (if possible)

// Questions:
// How can I write out the file? Where should I put it?
// How can I take a argument to set the size of the index?

use std::{collections::HashMap, io::Write, ops::Range, sync::Arc};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use lance_core::utils::address::RowAddress;
use lance_core::utils::deletion::DeletionVector;
use lance_io::ReadBatchParams;
use lance_table::rowids::FragmentRowIdIndex;
use lance_table::{
    rowids::{RowIdIndex, RowIdSequence, write_row_ids},
    utils::stream::{RowIdAndDeletesConfig, apply_row_id_and_deletes},
};

fn make_sequence(row_id_range: Range<u64>, deletions: usize) -> RowIdSequence {
    let mut sequence = RowIdSequence::from(row_id_range);

    // Delete every other row
    let delete_ids = sequence
        .iter()
        .step_by(2)
        .take(deletions)
        .collect::<Vec<_>>();
    sequence.delete(delete_ids);

    sequence
}

fn make_frag_sequences(
    num_rows: u64,
    num_frags: u64,
    percent_deletion: f32,
) -> Vec<(u32, Arc<RowIdSequence>)> {
    let rows_per_frag = num_rows / num_frags;
    let mut start = 0;
    (0..num_frags)
        .map(|i| {
            let sequence = make_sequence(
                start..(start + rows_per_frag),
                (rows_per_frag as f32 * percent_deletion) as usize,
            );
            start += rows_per_frag;
            (i as u32, Arc::new(sequence))
        })
        .collect()
}

// For range of values
// https://bheisler.github.io/criterion.rs/book/user_guide/benchmarking_with_inputs.html

fn num_rows() -> u64 {
    std::env::var("BENCH_NUM_ROWS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(1_000_000)
}

struct SizeStats {
    structure: String,
    percent_deletions: f32,
    size: u64,
}

struct SizeStatsFile {
    file: Option<std::fs::File>,
}

impl SizeStatsFile {
    fn new() -> Self {
        if let Ok(path) = std::env::var("BENCH_SIZE_STATS_FILE") {
            let mut file = std::fs::File::create(path).unwrap();
            // Header row
            writeln!(file, "structure,percent_deletions,size").unwrap();
            Self { file: Some(file) }
        } else {
            Self { file: None }
        }
    }

    fn write_row(&mut self, stats: SizeStats) {
        if let Some(file) = &mut self.file {
            writeln!(
                file,
                "\"{}\",{},{}",
                stats.structure, stats.percent_deletions, stats.size
            )
            .unwrap();
        }
    }
}

fn bench_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_id_index_creation");
    let mut stats_file = SizeStatsFile::new();

    for percent_deletions in [0.0, 0.25, 0.5] {
        let sequences = make_frag_sequences(num_rows(), 100, percent_deletions);

        let fragment_indices: Vec<FragmentRowIdIndex> = sequences
            .iter()
            .map(|(frag_id, sequence)| FragmentRowIdIndex {
                fragment_id: *frag_id,
                row_id_sequence: sequence.clone(),
                deletion_vector: Arc::new(DeletionVector::default()),
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("BuildIndex", percent_deletions),
            &percent_deletions,
            |b, _| {
                b.iter(|| {
                    let _index = RowIdIndex::new(&fragment_indices).unwrap();
                });
            },
        );

        // Measure size of index
        {
            let mut size = 0;
            for (_frag_id, sequence) in &sequences {
                size += write_row_ids(sequence).len() as u64;
            }
            let stats = SizeStats {
                structure: "RowIdIndex".to_string(),
                percent_deletions,
                size,
            };
            stats_file.write_row(stats);
        }

        // TODO: we should compare tombstoned vs compacted. We don't mind the
        // regression in the tombstoned case, but we want to see the improvement
        // in the compacted case.

        // TODO: collect size of sequences when serialized

        // TODO: also show building a BTreeMap and HashMap

        let flat_data = sequences
            .iter()
            .map(|(frag_id, sequence)| {
                let row_ids = sequence.iter().collect::<Vec<_>>();
                let row_addresses = (0..sequence.len())
                    .map(|i| RowAddress::new_from_parts(*frag_id, i as u32))
                    .map(u64::from)
                    .collect::<Vec<_>>();
                (row_ids, row_addresses)
            })
            .collect::<Vec<_>>();

        // Size of flat data is just 16 bytes per row
        let size = flat_data
            .iter()
            .map(|(ids, _addresses)| ids.len() * 16)
            .sum::<usize>() as u64;
        let stats = SizeStats {
            structure: "FlatData".to_string(),
            percent_deletions,
            size,
        };
        stats_file.write_row(stats);

        group.bench_with_input(
            BenchmarkId::new("BuildHashMap", percent_deletions),
            &percent_deletions,
            |b, _| {
                b.iter(|| {
                    let mut index = HashMap::new();
                    index.extend(flat_data.iter().flat_map(|(ids, addresses)| {
                        ids.iter().copied().zip(addresses.iter().copied())
                    }));
                });
            },
        );
    }

    group.finish();
}

fn bench_get_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_id_index_get_single");

    for percent_deletions in [0.0, 0.02, 0.25, 0.5, 0.8] {
        let sequences = make_frag_sequences(num_rows(), 100, percent_deletions);

        let fragment_indices: Vec<FragmentRowIdIndex> = sequences
            .iter()
            .map(|(frag_id, sequence)| FragmentRowIdIndex {
                fragment_id: *frag_id,
                row_id_sequence: sequence.clone(),
                deletion_vector: Arc::new(DeletionVector::default()),
            })
            .collect();

        let index = RowIdIndex::new(&fragment_indices).unwrap();

        let mut i = 0;
        let total_rows: u64 = num_rows();
        let mut next_id = || {
            let id = i;
            i += 241861;
            i %= total_rows;
            id
        };

        group.bench_with_input(
            BenchmarkId::new("GetIndex", percent_deletions),
            &percent_deletions,
            |b, _| {
                b.iter(|| {
                    let _ = index.get(next_id());
                });
            },
        );

        let flat_data = sequences
            .iter()
            .map(|(frag_id, sequence)| {
                let row_ids = sequence.iter().collect::<Vec<_>>();
                let row_addresses = (0..sequence.len())
                    .map(|i| RowAddress::new_from_parts(*frag_id, i as u32))
                    .map(u64::from)
                    .collect::<Vec<_>>();
                (row_ids, row_addresses)
            })
            .collect::<Vec<_>>();

        let index =
            {
                let mut index = HashMap::new();
                index.extend(flat_data.iter().flat_map(|(ids, addresses)| {
                    ids.iter().copied().zip(addresses.iter().copied())
                }));
                index
            };

        group.bench_with_input(
            BenchmarkId::new("GetHashMap", percent_deletions),
            &percent_deletions,
            |b, _| {
                b.iter(|| {
                    for i in 0..num_rows() {
                        let _ = index.get(&i);
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_apply_row_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_row_id");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(
            (0..num_rows()).collect::<Vec<_>>(),
        ))],
    )
    .unwrap();

    let config = RowIdAndDeletesConfig {
        params: ReadBatchParams::default(),
        with_row_id: true,
        with_row_addr: false,
        with_row_last_updated_at_version: false,
        with_row_created_at_version: false,
        deletion_vector: None,
        row_id_sequence: None,
        last_updated_at_sequence: None,
        created_at_sequence: None,
        make_deletions_null: false,
        total_num_rows: num_rows() as u32,
    };

    group.bench_function("ApplyRowId", |b| {
        let batch = batch.clone();
        b.iter(|| {
            let _ = apply_row_id_and_deletes(batch.clone(), 0, 0, &config);
        });
    });

    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config=Criterion::default().with_profiler(lance_testing::pprof::PProfProfiler::new(100, lance_testing::pprof::Output::Flamegraph(None)));
    targets=bench_creation, bench_get_single, bench_apply_row_id);
#[cfg(not(target_os = "linux"))]
criterion_group!(
    benches,
    bench_creation,
    bench_get_single,
    bench_apply_row_id
);
criterion_main!(benches);
