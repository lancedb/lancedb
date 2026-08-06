// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Benchmarks for `RowAddrMask` / `RowAddrTreeMap`.
//!
//! These benchmarks are deliberately structured to expose the row-cardinality
//! scaling weakness of the current per-row bitmap representation. Producers
//! (e.g. scalar-index `search` implementations) and consumers (e.g.
//! `mask_to_offset_ranges`) are frequently range-shaped, but every operation
//! must round-trip through `Partial(RoaringBitmap)` and therefore costs O(N)
//! in the number of rows, not O(R) in the number of distinct ranges.
//!
//! Each benchmark varies the number of rows while keeping the number of
//! ranges fixed at 1. A range-aware representation should make these
//! near-constant time; today they are linear in N.
//!
//! Run with `cargo bench -p lance-select --bench row_addr_mask`.

use std::ops::Range;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lance_select::{RowAddrMask, RowAddrTreeMap};

/// Row counts we sweep across. Chosen to cover the realistic range of
/// matches a zonemap produces for an `IS NULL`-like predicate on a single
/// fragment: a few thousand rows up through tens of millions.
const ROW_COUNTS: &[u64] = &[10_000, 100_000, 1_000_000, 10_000_000];

fn make_range_mask(num_rows: u64) -> RowAddrTreeMap {
    // Build a mask covering a single contiguous run in fragment 0.
    // This is the exact shape a scalar-index search produces when it
    // determines a contiguous chunk of zones matches.
    let mut map = RowAddrTreeMap::new();
    map.insert_range(0..num_rows);
    map
}

/// Producer cost: building a mask from one contiguous Range.
///
/// Today this is O(N) — every bit gets inserted into a roaring bitmap.
/// With a range-aware representation it would be O(1) (push a single run).
fn bench_insert_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_range_single_run");
    for &n in ROW_COUNTS {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut map = RowAddrTreeMap::new();
                map.insert_range(0..n);
                std::hint::black_box(map);
            });
        });
    }
    group.finish();
}

/// Consumer cost: iterating every row address in a dense mask.
///
/// `into_addr_iter` walks set bits one at a time. For a contiguous run
/// of N rows this is O(N) — even though the rows are trivially
/// representable as a single Range. This is what `mask_to_offset_ranges`
/// does after intersecting with a source segment: it pays per-row
/// iteration cost only to immediately collapse the addresses back into
/// ranges via `GroupingIterator`.
fn bench_iter_addrs(c: &mut Criterion) {
    let mut group = c.benchmark_group("into_addr_iter_single_run");
    for &n in ROW_COUNTS {
        let map = make_range_mask(n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // SAFETY: the map only contains Partial selections; no Full entries.
                let count: u64 = unsafe { map.clone().into_addr_iter() }.count() as u64;
                std::hint::black_box(count);
            });
        });
    }
    group.finish();
}

/// Best-achievable iteration over the same data.
///
/// `Iter::next_range` walks the bitmap's run containers in O(num_runs).
/// For a single contiguous run this should be ~constant time — the
/// public `RowAddrMask` API gives no way to surface that today, so the
/// performance is currently inaccessible to callers. Comparing this to
/// `into_addr_iter_single_run` quantifies the speedup a range-aware
/// representation could deliver to consumers.
fn bench_iter_runs(c: &mut Criterion) {
    let mut group = c.benchmark_group("next_range_iter_single_run");
    for &n in ROW_COUNTS {
        // Use the same underlying roaring bitmap shape that `make_range_mask`
        // produces internally (one fragment, one contiguous run).
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert_range(0..(n as u32));
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let mut iter = bitmap.iter();
                let mut runs: u64 = 0;
                while iter.next_range().is_some() {
                    runs += 1;
                }
                std::hint::black_box(runs);
            });
        });
    }
    group.finish();
}

/// Range-aware consumer cost via the public `RowAddrTreeMap::iter_runs`
/// API. The map is built the ordinary way (`insert_range` → Partial
/// bitmap); `iter_runs` walks the bitmap's run containers via
/// `Iter::next_range`. Compare against `into_addr_iter_single_run` to see
/// the consumer-side speedup callers get without changing the underlying
/// representation.
fn bench_iter_runs_partial(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_runs_partial_single_run");
    for &n in ROW_COUNTS {
        let map = make_range_mask(n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // SAFETY: map only contains Partial selections.
                let mut runs: u64 = 0;
                for _ in unsafe { map.iter_runs() } {
                    runs += 1;
                }
                std::hint::black_box(runs);
            });
        });
    }
    group.finish();
}

/// Set intersection of two range-shaped masks.
///
/// Both inputs are single contiguous runs that overlap in their middle
/// half (so the output is itself a single contiguous run). With per-row
/// bitmaps this is O(N) — the entire bitmap participates in the AND.
/// With ranges it would be O(1).
fn bench_intersect_ranges(c: &mut Criterion) {
    let mut group = c.benchmark_group("intersect_two_runs");
    for &n in ROW_COUNTS {
        let lhs = make_range_mask(n);
        let rhs_range = (n / 4)..(3 * n / 4);
        let mut rhs = RowAddrTreeMap::new();
        rhs.insert_range(rhs_range);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let mut tmp = lhs.clone();
                tmp &= &rhs;
                std::hint::black_box(tmp);
            });
        });
    }
    group.finish();
}

/// Full round trip: build a source range bitmap, AND with a mask, iterate
/// each surviving bit. This is the exact slow path of
/// `mask_to_offset_ranges` in `lance-table/src/rowids.rs:387`. Profiling
/// a 10M-row zonemap `IS NULL` query showed this consuming ~55% of the
/// hot-loop time (~495 ms of 889 ms). The benchmark separates the
/// per-row producer/consumer cost from the rest of the scan pipeline so
/// it can be tracked in isolation.
fn bench_range_to_ranges_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("mask_to_offset_ranges_inner_loop");
    for &n in ROW_COUNTS {
        // The mask selects the back half of a 2N-row fragment.
        let mask_range = n..(2 * n);
        let mask = RowAddrMask::AllowList(RowAddrTreeMap::from(mask_range));
        // The source segment covers the whole fragment.
        let src: Range<u64> = 0..(2 * n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Mimic the slow path: materialize source range, AND with mask,
                // iterate to count survivors (a stand-in for whatever the
                // consumer actually does — e.g. GroupingIterator).
                let mut ids = RowAddrTreeMap::from(src.clone());
                ids.mask(&mask);
                let count = unsafe { ids.into_addr_iter() }.count();
                std::hint::black_box(count);
            });
        });
    }
    group.finish();
}

/// Same end-to-end shape as `mask_to_offset_ranges_inner_loop`, but the
/// final per-bit walk is replaced by `iter_runs`. Quantifies the speedup
/// the consumer side gets purely from switching iteration APIs — no
/// representation change.
fn bench_range_to_ranges_round_trip_runs(c: &mut Criterion) {
    let mut group = c.benchmark_group("mask_to_offset_ranges_inner_loop_runs");
    for &n in ROW_COUNTS {
        let mask_range = n..(2 * n);
        let mask = RowAddrMask::AllowList(RowAddrTreeMap::from(mask_range));
        let src: Range<u64> = 0..(2 * n);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let mut ids = RowAddrTreeMap::from(src.clone());
                ids.mask(&mask);
                // SAFETY: only Partial selections in play.
                let count: u64 = unsafe { ids.iter_runs() }
                    .map(|(_, r)| (*r.end() as u64) - (*r.start() as u64) + 1)
                    .sum();
                std::hint::black_box(count);
            });
        });
    }
    group.finish();
}

/// Many small runs vs one big run with the same total cardinality.
///
/// A range-aware representation should be O(num_runs), so the
/// `single_run` case should be ~K times faster than the `K_runs` case.
/// Today they are essentially equal: the cost is dictated by the number
/// of rows, not the number of runs.
fn bench_runs_vs_rows(c: &mut Criterion) {
    let total_rows: u64 = 1_000_000;
    let mut group = c.benchmark_group("insert_runs_constant_cardinality");

    group.throughput(Throughput::Elements(total_rows));
    group.bench_function("single_run_1M", |b| {
        b.iter(|| {
            let mut map = RowAddrTreeMap::new();
            map.insert_range(0..total_rows);
            std::hint::black_box(map);
        });
    });

    for k in [10u64, 100, 1_000, 10_000] {
        let run_size = total_rows / k;
        // Stride between runs is 2 * run_size so the bitmap is half full.
        let stride = run_size * 2;
        group.bench_function(format!("{k}_runs_1M_total"), |b| {
            b.iter(|| {
                let mut map = RowAddrTreeMap::new();
                for i in 0..k {
                    let start = i * stride;
                    map.insert_range(start..(start + run_size));
                }
                std::hint::black_box(map);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_range,
    bench_iter_addrs,
    bench_iter_runs,
    bench_iter_runs_partial,
    bench_intersect_ranges,
    bench_range_to_ranges_round_trip,
    bench_range_to_ranges_round_trip_runs,
    bench_runs_vs_rows,
);
criterion_main!(benches);
