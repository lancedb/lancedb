// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Benchmarks use eprintln! to report memory stats alongside criterion output.
#![allow(clippy::print_stderr)]

//! Benchmark for manifest fragment interning.
//!
//! Measures memory savings and deserialization throughput when interning
//! `DataFile.fields`, `DataFile.column_indices`, and
//! `RowDatasetVersionMeta::Inline` bytes across many fragments.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lance_core::deepsize::DeepSizeOf;
use prost::Message;

use lance_table::format::pb;
use lance_table::format::{DataFileFieldInterner, Fragment};

fn num_fragments() -> u64 {
    std::env::var("BENCH_NUM_FRAGMENTS")
        .map(|s| s.parse().unwrap())
        .unwrap_or(100_000)
}

/// Build a vector of protobuf DataFragment messages that simulate a
/// homogeneous, post-compaction table: every fragment has the same field
/// list, column indices, and version metadata bytes.
fn make_uniform_pb_fragments(n: u64, num_fields: usize) -> Vec<pb::DataFragment> {
    let fields: Vec<i32> = (0..num_fields as i32).collect();
    let column_indices: Vec<i32> = (0..num_fields as i32).collect();

    // Simulate version metadata: a small protobuf-encoded payload
    // (identical across all fragments post-compaction)
    let version_bytes: Vec<u8> = {
        let seq = pb::RowDatasetVersionSequence {
            runs: vec![pb::RowDatasetVersionRun {
                span: Some(pb::U64Segment {
                    segment: Some(pb::u64_segment::Segment::Range(pb::u64_segment::Range {
                        start: 0,
                        end: 1000,
                    })),
                }),
                version: 42,
            }],
        };
        seq.encode_to_vec()
    };

    (0..n)
        .map(|i| pb::DataFragment {
            id: i,
            files: vec![pb::DataFile {
                path: format!("data/{i}.lance"),
                fields: fields.clone(),
                column_indices: column_indices.clone(),
                file_major_version: 2,
                file_minor_version: 0,
                file_size_bytes: 0,
                base_id: None,
            }],
            overlays: vec![],
            deletion_file: None,
            row_id_sequence: None,
            physical_rows: 1000,
            last_updated_at_version_sequence: Some(
                pb::data_fragment::LastUpdatedAtVersionSequence::InlineLastUpdatedAtVersions(
                    version_bytes.clone(),
                ),
            ),
            created_at_version_sequence: Some(
                pb::data_fragment::CreatedAtVersionSequence::InlineCreatedAtVersions(
                    version_bytes.clone(),
                ),
            ),
        })
        .collect()
}

/// Deserialize protobuf fragments WITHOUT interning (baseline).
fn deserialize_without_interning(protos: &[pb::DataFragment]) -> Vec<Fragment> {
    protos
        .iter()
        .map(|p| Fragment::try_from(p.clone()).unwrap())
        .collect()
}

/// Deserialize protobuf fragments WITH interning.
fn deserialize_with_interning(protos: &[pb::DataFragment]) -> Vec<Fragment> {
    let mut interner = DataFileFieldInterner::default();
    protos
        .iter()
        .map(|p| interner.intern_fragment(p.clone()).unwrap())
        .collect()
}

/// Build fragments where each group shares the same version metadata,
/// simulating many small appends without compaction.
fn make_diverse_pb_fragments(
    n: u64,
    num_fields: usize,
    unique_versions: u64,
) -> Vec<pb::DataFragment> {
    let fields: Vec<i32> = (0..num_fields as i32).collect();
    let column_indices: Vec<i32> = (0..num_fields as i32).collect();
    let group_size = n / unique_versions;

    let version_payloads: Vec<Vec<u8>> = (0..unique_versions)
        .map(|v| {
            let seq = pb::RowDatasetVersionSequence {
                runs: vec![pb::RowDatasetVersionRun {
                    span: Some(pb::U64Segment {
                        segment: Some(pb::u64_segment::Segment::Range(pb::u64_segment::Range {
                            start: 0,
                            end: 1000,
                        })),
                    }),
                    version: v,
                }],
            };
            seq.encode_to_vec()
        })
        .collect();

    (0..n)
        .map(|i| {
            let version_idx = (i / group_size).min(unique_versions - 1) as usize;
            pb::DataFragment {
                id: i,
                files: vec![pb::DataFile {
                    path: format!("data/{i}.lance"),
                    fields: fields.clone(),
                    column_indices: column_indices.clone(),
                    file_major_version: 2,
                    file_minor_version: 0,
                    file_size_bytes: 0,
                    base_id: None,
                }],
                overlays: vec![],
                deletion_file: None,
                row_id_sequence: None,
                physical_rows: 1000,
                last_updated_at_version_sequence: Some(
                    pb::data_fragment::LastUpdatedAtVersionSequence::InlineLastUpdatedAtVersions(
                        version_payloads[version_idx].clone(),
                    ),
                ),
                created_at_version_sequence: Some(
                    pb::data_fragment::CreatedAtVersionSequence::InlineCreatedAtVersions(
                        version_payloads[version_idx].clone(),
                    ),
                ),
            }
        })
        .collect()
}

fn bench_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_intern");
    let n = num_fragments();

    for num_fields in [10, 50] {
        let protos = make_uniform_pb_fragments(n, num_fields);

        group.bench_with_input(
            BenchmarkId::new("deserialize_no_intern", num_fields),
            &num_fields,
            |b, _| {
                b.iter(|| deserialize_without_interning(&protos));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("deserialize_with_intern", num_fields),
            &num_fields,
            |b, _| {
                b.iter(|| deserialize_with_interning(&protos));
            },
        );
    }

    // Benchmark with many unique version payloads
    for unique_versions in [10, 100, 500] {
        let protos = make_diverse_pb_fragments(n, 10, unique_versions);

        group.bench_with_input(
            BenchmarkId::new("deserialize_no_intern_diverse", unique_versions),
            &unique_versions,
            |b, _| {
                b.iter(|| deserialize_without_interning(&protos));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("deserialize_with_intern_diverse", unique_versions),
            &unique_versions,
            |b, _| {
                b.iter(|| deserialize_with_interning(&protos));
            },
        );
    }

    group.finish();
}

fn bench_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_memory");
    let n = num_fragments();

    for num_fields in [10, 50] {
        let protos = make_uniform_pb_fragments(n, num_fields);

        let no_intern = deserialize_without_interning(&protos);
        let with_intern = deserialize_with_interning(&protos);

        let size_no_intern = no_intern.deep_size_of();
        let size_with_intern = with_intern.deep_size_of();

        eprintln!(
            "\n[{} fragments, {} fields] Memory without interning: {:.2} MB",
            n,
            num_fields,
            size_no_intern as f64 / 1_048_576.0
        );
        eprintln!(
            "[{} fragments, {} fields] Memory with interning:    {:.2} MB",
            n,
            num_fields,
            size_with_intern as f64 / 1_048_576.0
        );
        eprintln!(
            "[{} fragments, {} fields] Savings:                  {:.2} MB ({:.1}%)",
            n,
            num_fields,
            (size_no_intern - size_with_intern) as f64 / 1_048_576.0,
            (1.0 - size_with_intern as f64 / size_no_intern as f64) * 100.0
        );

        // Benchmark deep_size_of measurement itself (sanity check)
        group.bench_with_input(
            BenchmarkId::new("deep_size_of_interned", num_fields),
            &num_fields,
            |b, _| {
                b.iter(|| with_intern.deep_size_of());
            },
        );

        drop(no_intern);
        drop(with_intern);
    }

    group.finish();
}

#[cfg(target_os = "linux")]
criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(lance_testing::pprof::PProfProfiler::new(100, lance_testing::pprof::Output::Flamegraph(None)));
    targets = bench_deserialization, bench_memory
);
#[cfg(not(target_os = "linux"))]
criterion_group!(benches, bench_deserialization, bench_memory);
criterion_main!(benches);
