// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance_core::datatypes::Schema;
use lance_file::{datatypes::Fields, format::pb};

fn proto_field(id: i32, parent_id: i32, name: String, logical_type: &str) -> pb::Field {
    pb::Field {
        id,
        parent_id,
        name,
        logical_type: logical_type.to_owned(),
        ..Default::default()
    }
}

/// Builds a pre-order flat schema with `num_physical_columns` physical leaves.
///
/// Each struct contributes one parent and two `int32` leaves. Root fields use
/// `-1` as `parent_id`, and each struct consumes a three-ID block.
fn wide_two_leaf_structs(num_physical_columns: usize) -> Fields {
    assert_eq!(num_physical_columns % 2, 0);
    let num_structs = num_physical_columns / 2;
    let mut fields = Vec::with_capacity(num_structs + num_physical_columns);

    for struct_index in 0..num_structs {
        let parent_id = (struct_index * 3) as i32;
        fields.push(proto_field(
            parent_id,
            -1,
            format!("struct_{struct_index}"),
            "struct",
        ));
        fields.push(proto_field(
            parent_id + 1,
            parent_id,
            format!("left_{struct_index}"),
            "int32",
        ));
        fields.push(proto_field(
            parent_id + 2,
            parent_id,
            format!("right_{struct_index}"),
            "int32",
        ));
    }

    Fields(fields)
}

fn bench_schema_reconstruction(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_from_flat_fields");

    for num_physical_columns in [1024, 4096, 16_384, 65_536] {
        let fields = wide_two_leaf_structs(num_physical_columns);
        group.throughput(Throughput::Elements(fields.0.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("physical_columns", num_physical_columns),
            &fields,
            |bencher, fields| {
                bencher.iter(|| Schema::try_from(black_box(fields)).unwrap());
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_schema_reconstruction);
criterion_main!(benches);
