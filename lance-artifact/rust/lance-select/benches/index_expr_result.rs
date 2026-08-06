// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmarks for the `NullableIndexExprResult` boolean algebra
//! (`Not` / `BitAnd` / `BitOr`).
//!
//! Captures a baseline of the current 3-variant `Exact`/`AtMost`/`AtLeast`
//! representation before we convert it to a 2-mask `{lower, upper}` form.
//! After the conversion every binary op will do two `NullableRowAddrMask`
//! operations instead of zero or one, so we expect a 2x slowdown on
//! same-variant micro-ops in exchange for richer results (Refined-style
//! interval bounds).
//!
//! Each variant is constructed from a fragment with a single contiguous
//! run of selected rows (the shape produced by zone-map / bloom-filter
//! IsNull searches and by `mask_to_offset_ranges` on contiguous segments).
//! We sweep N across 10K..10M to expose mask-size scaling.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance_select::{
    NullableIndexExprResult, NullableRowAddrMask, NullableRowAddrSet, RowAddrTreeMap,
};

const ROW_COUNTS: &[u64] = &[10_000, 100_000, 1_000_000, 10_000_000];

/// Build a `NullableRowAddrMask::AllowList` covering `0..n` in fragment 0
/// with no NULL bits set. This is the shape an index search produces
/// when it identifies a contiguous run as a match.
fn allow_run(n: u64) -> NullableRowAddrMask {
    let mut tree = RowAddrTreeMap::new();
    tree.insert_range(0..n);
    NullableRowAddrMask::AllowList(NullableRowAddrSet::new(tree, RowAddrTreeMap::new()))
}

/// Build an overlapping mask: covers `n/4..3n/4` (the middle half of an
/// equivalent `allow_run(n)`), so `&` / `|` against `allow_run(n)`
/// produces a non-trivial output that still has roaring-internal runs.
fn allow_middle(n: u64) -> NullableRowAddrMask {
    let mut tree = RowAddrTreeMap::new();
    tree.insert_range((n / 4)..(3 * n / 4));
    NullableRowAddrMask::AllowList(NullableRowAddrSet::new(tree, RowAddrTreeMap::new()))
}

// --- NOT ---------------------------------------------------------------

fn bench_not(c: &mut Criterion) {
    let mut group = c.benchmark_group("not");
    for &n in ROW_COUNTS {
        group.throughput(Throughput::Elements(n));
        for label in ["Exact", "AtMost", "AtLeast"] {
            let id = BenchmarkId::new(label, n);
            let make = move || match label {
                "Exact" => NullableIndexExprResult::exact(allow_run(n)),
                "AtMost" => NullableIndexExprResult::at_most(allow_run(n)),
                "AtLeast" => NullableIndexExprResult::at_least(allow_run(n)),
                _ => unreachable!(),
            };
            group.bench_function(id, |b| {
                b.iter_batched(make, |r| black_box(!r), criterion::BatchSize::SmallInput);
            });
        }
    }
    group.finish();
}

// --- AND / OR ----------------------------------------------------------

type PairFn = Box<dyn Fn() -> (NullableIndexExprResult, NullableIndexExprResult)>;

/// Helper that builds `(lhs, rhs)` for every combination we want to bench.
/// Returns `(label, build_fn)` pairs. We test the same-variant cases
/// (Exact/Exact, AtMost/AtMost, AtLeast/AtLeast) plus the cross-variant
/// cases that today drop information (Exact/AtMost, Exact/AtLeast,
/// AtMost/AtLeast).
fn pair_cases(n: u64) -> Vec<(&'static str, PairFn)> {
    let lhs = move || allow_run(n);
    let rhs = move || allow_middle(n);
    let exact = |m| NullableIndexExprResult::exact(m);
    let at_most = |m| NullableIndexExprResult::at_most(m);
    let at_least = |m| NullableIndexExprResult::at_least(m);
    vec![
        (
            "Exact_Exact",
            Box::new(move || (exact(lhs()), exact(rhs()))),
        ),
        (
            "AtMost_AtMost",
            Box::new(move || (at_most(lhs()), at_most(rhs()))),
        ),
        (
            "AtLeast_AtLeast",
            Box::new(move || (at_least(lhs()), at_least(rhs()))),
        ),
        (
            "Exact_AtMost",
            Box::new(move || (exact(lhs()), at_most(rhs()))),
        ),
        (
            "Exact_AtLeast",
            Box::new(move || (exact(lhs()), at_least(rhs()))),
        ),
        (
            "AtMost_AtLeast",
            Box::new(move || (at_most(lhs()), at_least(rhs()))),
        ),
    ]
}

fn bench_and(c: &mut Criterion) {
    let mut group = c.benchmark_group("and");
    for &n in ROW_COUNTS {
        group.throughput(Throughput::Elements(n));
        for (label, make) in pair_cases(n) {
            let id = BenchmarkId::new(label, n);
            group.bench_function(id, |b| {
                b.iter_batched(
                    &make,
                    |(l, r)| black_box(l & r),
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_or(c: &mut Criterion) {
    let mut group = c.benchmark_group("or");
    for &n in ROW_COUNTS {
        group.throughput(Throughput::Elements(n));
        for (label, make) in pair_cases(n) {
            let id = BenchmarkId::new(label, n);
            group.bench_function(id, |b| {
                b.iter_batched(
                    &make,
                    |(l, r)| black_box(l | r),
                    criterion::BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_not, bench_and, bench_or);
criterion_main!(benches);
