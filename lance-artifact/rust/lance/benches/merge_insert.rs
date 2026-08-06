// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//
//! Benchmarks for `merge_insert` with a scalar index on the merge key.
//!
//! The interesting case is the dataset shape that exercises the fragment-
//! bitmap allow-list path in `DatasetPreFilter::create_restricted_deletion_mask`:
//!
//! - some fragments live OUTSIDE the index's `fragment_bitmap`
//!   (i.e. data was appended after the index was built, or partially
//!   rewritten without re-indexing), AND
//! - some fragments INSIDE the bitmap have a deletion file.
//!
//! Both conditions together force the slow `AllowList(Full) - BlockList(Partial)`
//! computation. The other shapes (`clean`, `with_new_rows_only`,
//! `with_deletions_only`) skip that branch and serve as controls.
//!
//! The `composite_key_indexed` shape covers merge_insert joined on two
//! indexed key columns, which probes every index and AND-folds the results.
//!
//! Run with `cargo bench --bench merge_insert`.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use criterion::{Criterion, criterion_group, criterion_main};
use lance::dataset::write::merge_insert::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
#[cfg(target_os = "linux")]
use lance_testing::pprof::{Output, PProfProfiler};

// Many small fragments to amplify the slow path: each indexed fragment with
// a deletion file produces one RoaringBitmap::full() allocation per
// merge_insert call. Cost scales linearly with NUM_FRAGS.
const ROWS_PER_FRAG: u64 = 100;
const NUM_FRAGS: u64 = 200;
// Minimal schema so the merge_insert baseline (sort, hash-join, write) is small
// and the prefilter overhead dominates the measurement.
fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]))
}

fn make_batch(start_id: i64, n: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values(start_id..start_id + n as i64);
    RecordBatch::try_new(schema(), vec![Arc::new(ids)]).unwrap()
}

fn make_batches(start_id: i64, total_rows: u64) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut remaining = total_rows;
    let mut next_start = start_id;
    while remaining > 0 {
        let n = remaining.min(ROWS_PER_FRAG) as usize;
        out.push(make_batch(next_start, n));
        next_start += n as i64;
        remaining -= n as u64;
    }
    out
}

async fn build_indexed_base(path: &str) -> Dataset {
    let total = ROWS_PER_FRAG * NUM_FRAGS;
    let params = WriteParams {
        max_rows_per_file: ROWS_PER_FRAG as usize,
        max_rows_per_group: ROWS_PER_FRAG as usize,
        mode: WriteMode::Create,
        ..Default::default()
    };
    let batches = make_batches(0, total);
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema());
    Dataset::write(reader, path, Some(params)).await.unwrap();

    let mut ds = Dataset::open(path).await.unwrap();
    ds.create_index(
        &["id"],
        IndexType::BTree,
        None,
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();
    ds
}

async fn append_rows(path: &str, base_id: i64, n: usize) -> Dataset {
    let batch = make_batch(base_id, n);
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema());
    let params = WriteParams {
        max_rows_per_file: n,
        max_rows_per_group: n,
        mode: WriteMode::Append,
        ..Default::default()
    };
    Dataset::write(reader, path, Some(params)).await.unwrap();
    Dataset::open(path).await.unwrap()
}

async fn delete_some_indexed_rows(ds: &mut Dataset) {
    // Delete a sparse pattern that lands in EVERY indexed fragment (one row
    // per fragment, since ROWS_PER_FRAG = 100 and we delete `id % 100 == 0`).
    // Each affected fragment gets its own deletion file inside the bitmap,
    // which is what scales the slow `RoaringBitmap::full()` materialization
    // path: one allocation per fragment per merge_insert call.
    ds.delete("id % 100 == 0").await.unwrap();
}

/// One merge_insert op: 30 updates of existing IDs + 70 inserts of new IDs.
async fn one_merge_insert(ds: Arc<Dataset>, base_existing: i64, base_new: i64) {
    let mut ids: Vec<i64> = (0..30).map(|i| base_existing + i).collect();
    ids.extend(base_new..base_new + 70);
    let id_arr = Int64Array::from(ids);
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(id_arr)]).unwrap();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema());

    let mut builder = MergeInsertBuilder::try_new(ds, vec!["id".to_string()]).unwrap();
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll);
    let job = builder.try_build().unwrap();
    let _ = job.execute_reader(reader).await.unwrap();
}

// --- Composite-key shape: join on (a, b) with a BTree index on each key ---
//
// Exercises the multi-index probe path: one `IsIn` query per indexed key
// column, AND-folded inside a single `MapIndexExec`, followed by the
// composite hash join.  `b` is a deterministic function of `a` so source
// rows can target existing composite keys without tracking extra state.

fn composite_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

fn composite_b(a: i64) -> i64 {
    a * 7 + 3
}

fn make_composite_batch(start: i64, n: usize) -> RecordBatch {
    let a = Int64Array::from_iter_values(start..start + n as i64);
    let b = Int64Array::from_iter_values((start..start + n as i64).map(composite_b));
    RecordBatch::try_new(composite_schema(), vec![Arc::new(a), Arc::new(b)]).unwrap()
}

async fn build_composite_key_indexed() -> (TempStrDir, Arc<Dataset>) {
    let dir = TempStrDir::default();
    let path = dir.as_str().to_string();
    let total = ROWS_PER_FRAG * NUM_FRAGS;
    let mut batches = Vec::new();
    let mut start = 0i64;
    while (start as u64) < total {
        let n = (total - start as u64).min(ROWS_PER_FRAG) as usize;
        batches.push(make_composite_batch(start, n));
        start += n as i64;
    }
    let params = WriteParams {
        max_rows_per_file: ROWS_PER_FRAG as usize,
        max_rows_per_group: ROWS_PER_FRAG as usize,
        mode: WriteMode::Create,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), composite_schema());
    Dataset::write(reader, &path, Some(params)).await.unwrap();

    let mut ds = Dataset::open(&path).await.unwrap();
    for col in ["a", "b"] {
        ds.create_index(
            &[col],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    }
    (dir, Arc::new(ds))
}

/// Composite-key merge_insert op: 30 updates of existing (a, b) pairs +
/// 70 inserts of new pairs, joined on both columns.
async fn one_composite_merge_insert(ds: Arc<Dataset>, base_existing: i64, base_new: i64) {
    let mut a_vals: Vec<i64> = (0..30).map(|i| base_existing + i).collect();
    a_vals.extend(base_new..base_new + 70);
    let b_vals: Vec<i64> = a_vals.iter().copied().map(composite_b).collect();
    let batch = RecordBatch::try_new(
        composite_schema(),
        vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), composite_schema());

    let mut builder =
        MergeInsertBuilder::try_new(ds, vec!["a".to_string(), "b".to_string()]).unwrap();
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll);
    let job = builder.try_build().unwrap();
    let _ = job.execute_reader(reader).await.unwrap();
}

async fn build_clean() -> (TempStrDir, Arc<Dataset>) {
    let dir = TempStrDir::default();
    let path = dir.as_str().to_string();
    let ds = build_indexed_base(&path).await;
    (dir, Arc::new(ds))
}

async fn build_with_new_rows_only() -> (TempStrDir, Arc<Dataset>) {
    let dir = TempStrDir::default();
    let path = dir.as_str().to_string();
    build_indexed_base(&path).await;
    let base = (ROWS_PER_FRAG * NUM_FRAGS) as i64;
    let ds = append_rows(&path, base, 500).await;
    (dir, Arc::new(ds))
}

async fn build_with_deletions_only() -> (TempStrDir, Arc<Dataset>) {
    let dir = TempStrDir::default();
    let path = dir.as_str().to_string();
    let mut ds = build_indexed_base(&path).await;
    delete_some_indexed_rows(&mut ds).await;
    (dir, Arc::new(ds))
}

async fn build_with_new_rows_and_deletions() -> (TempStrDir, Arc<Dataset>) {
    let dir = TempStrDir::default();
    let path = dir.as_str().to_string();
    build_indexed_base(&path).await;
    let base = (ROWS_PER_FRAG * NUM_FRAGS) as i64;
    let mut ds = append_rows(&path, base, 500).await;
    delete_some_indexed_rows(&mut ds).await;
    (dir, Arc::new(ds))
}

fn bench_one_shape<F, Fut, M, MFut>(c: &mut Criterion, name: &str, builder: F, merge: M)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = (TempStrDir, Arc<Dataset>)>,
    M: Fn(Arc<Dataset>, i64, i64) -> MFut + Copy,
    MFut: std::future::Future<Output = ()>,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_dir, ds) = rt.block_on(builder());
    // Cache a snapshot version so each iteration restores to the same baseline.
    let base_version = ds.version().version;
    let path = ds.uri().to_string();
    let total = rt.block_on(ds.count_rows(None)).unwrap() as i64;

    c.bench_function(name, |b| {
        b.iter(|| {
            rt.block_on(async {
                // Restore to the base version so the bench measures a single
                // merge_insert against the same dataset shape every time.
                let bench_ds = Dataset::open(&path).await.unwrap();
                let mut bench_ds = bench_ds.checkout_version(base_version).await.unwrap();
                bench_ds.restore().await.unwrap();
                let arc = Arc::new(bench_ds);
                // base_existing in the indexed range, base_new beyond all data so it's an insert.
                merge(arc, 100, total + 1_000_000).await;
            })
        })
    });
}

fn bench_merge_insert(c: &mut Criterion) {
    bench_one_shape(c, "merge_insert/clean", build_clean, one_merge_insert);
    bench_one_shape(
        c,
        "merge_insert/with_new_rows_only",
        build_with_new_rows_only,
        one_merge_insert,
    );
    bench_one_shape(
        c,
        "merge_insert/with_deletions_only",
        build_with_deletions_only,
        one_merge_insert,
    );
    // The shape that exercises the AllowList(Full) - BlockList(Partial) path.
    bench_one_shape(
        c,
        "merge_insert/with_new_rows_and_deletions",
        build_with_new_rows_and_deletions,
        one_merge_insert,
    );
    // Composite key joined on (a, b), both BTree-indexed: exercises the
    // AND-folded multi-index probe inside MapIndexExec.
    bench_one_shape(
        c,
        "merge_insert/composite_key_indexed",
        build_composite_key_indexed,
        one_composite_merge_insert,
    );
}

#[cfg(target_os = "linux")]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_merge_insert);

#[cfg(not(target_os = "linux"))]
criterion_group!(
    name=benches;
    config = Criterion::default().significance_level(0.1).sample_size(10);
    targets = bench_merge_insert);

criterion_main!(benches);
