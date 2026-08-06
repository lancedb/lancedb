// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! End-to-end tests for data-overlay index masking: a scalar index masks data overlay files so that
//! queries stay correct while overlays remain (stale index hits are dropped and new
//! matches are added by re-evaluating overlay-covered rows on the flat path).

use std::sync::Arc;

use futures::TryStreamExt;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::BuiltinIndexType;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::ScalarIndexParams;
use lance_index::scalar::inverted::query::{FtsQuery, MatchQuery, PhraseQuery};
use lance_index::scalar::inverted::{DocumentGranularity, InvertedIndexParams};
use lance_io::utils::CachedFileSize;
use lance_linalg::distance::MetricType;
use lance_table::format::DataFile;
use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
use roaring::RoaringBitmap;
use rstest::rstest;

use lance_file::writer::FileWriterOptions;

use crate::Dataset;
use crate::dataset::optimize::{CompactionOptions, compact_files, remapping};
use crate::dataset::transaction::{DataOverlayGroup, Operation};
use crate::dataset::{WriteDestination, WriteParams};
use crate::index::vector::VectorIndexParams;
use crate::index::{CreateIndexBuilder, DatasetIndexExt};
use crate::io::exec::filtered_read::FilteredReadExec;
use crate::io::exec::fts::FlatMatchQueryExec;

/// Two-fragment Int32 dataset: `id` (field 0) = 0..12 and `age` (field 1) = id * 10,
/// six rows per file (fragments 0 and 1). In-memory store so overlay files can be written
/// with a store-relative `data/<name>.lance` path and committed against the dataset.
async fn create_base_dataset() -> Dataset {
    create_base_dataset_with(false).await
}

async fn create_base_dataset_with(stable_row_ids: bool) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new("age", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..12)),
            Arc::new(Int32Array::from_iter_values((0..12).map(|v| v * 10))),
        ],
    )
    .unwrap();
    let write_params = WriteParams {
        max_rows_per_file: 6,
        enable_stable_row_ids: stable_row_ids,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    Dataset::write(reader, "memory://", Some(write_params))
        .await
        .unwrap()
}

async fn build_age_index(dataset: &mut Dataset) {
    dataset
        .create_index(
            &["age"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
}

/// Write an overlay file covering `fields` of `fragment_id` with `coverage` and the given
/// per-field value columns, then commit it as a `DataOverlay` transaction. `name` makes
/// the overlay file unique.
async fn commit_overlay(
    dataset: Dataset,
    name: &str,
    fragment_id: u64,
    fields: &[i32],
    coverage: OverlayCoverage,
    columns: Vec<ArrayRef>,
) -> Dataset {
    let read_version = dataset.version().version;
    let overlay_schema = dataset.schema().project_by_ids(fields, true);

    let filename = format!("{name}.lance");
    // Use dataset.base so the path is absolute for file:// stores.
    // to_local_path() prepends '/' to the object_store path, so a bare
    // "data/foo.lance" would resolve to /data/foo.lance (root fs). With
    // base we get e.g. tmp/lance-bench/data/foo.lance → /tmp/lance-bench/data/foo.lance.
    // For memory:// stores base is empty so the result is the same as before.
    let path = dataset.base.clone().join("data").join(filename.as_str());
    let obj_writer = dataset.object_store.create(&path).await.unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        obj_writer,
        overlay_schema,
        FileWriterOptions::default(),
    )
    .unwrap();
    let file_version = lance_file::version::ConcreteFileVersion::V2_1;
    for (i, array) in columns.into_iter().enumerate() {
        writer.write_column(i, array).await.unwrap();
    }
    let summary = writer.finish().await.unwrap();

    let mut data_file = DataFile::new_unstarted(filename, file_version);
    data_file.fields = writer
        .field_id_to_column_indices()
        .iter()
        .map(|(field_id, _)| *field_id as i32)
        .collect::<Vec<_>>()
        .into();
    data_file.column_indices = writer
        .field_id_to_column_indices()
        .iter()
        .map(|(_, column_index)| *column_index as i32)
        .collect::<Vec<_>>()
        .into();
    data_file.file_size_bytes = CachedFileSize::new(summary.size_bytes);

    let overlay = DataOverlayFile {
        data_file,
        coverage,
        committed_version: 0,
    };
    Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id,
                overlays: vec![overlay],
            }],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap()
}

/// Sorted `id` values returned by a filtered scan.
async fn ids_matching(dataset: &Dataset, filter: &str) -> Vec<i32> {
    ids_matching_opts(dataset, filter, false).await
}

/// Like [`ids_matching`] but lets a test enable `fast_search()`, which skips unindexed
/// fragments. Overlay masking on indexed fragments must still apply regardless.
async fn ids_matching_opts(dataset: &Dataset, filter: &str, fast_search: bool) -> Vec<i32> {
    let mut scanner = dataset.scan();
    scanner.filter(filter).unwrap().project(&["id"]).unwrap();
    if fast_search {
        scanner.fast_search();
    }
    let batch = scanner.try_into_batch().await.unwrap();
    let mut ids = ids_from_batches(std::slice::from_ref(&batch));
    ids.sort_unstable();
    ids
}

/// Concatenate the `id` (Int32) column from each batch, in batch order.
fn ids_from_batches(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn i32_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from_iter(values))
}

fn string_lists(rows: &[&[&str]]) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for value in *row {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn fsl(rows: Vec<Vec<f32>>, dim: i32) -> ArrayRef {
    let flat: Vec<f32> = rows.into_iter().flatten().collect();
    let item = Arc::new(ArrowField::new("item", DataType::Float32, true));
    Arc::new(
        arrow_array::FixedSizeListArray::try_new(
            item,
            dim,
            Arc::new(arrow_array::Float32Array::from(flat)),
            None,
        )
        .unwrap(),
    )
}

/// A newer overlay on the indexed field drops stale index hits (the old value no longer
/// matches) and surfaces new matches (the new value is found even though the index never
/// saw it). Mirrors the spec's Bob 25 -> 26 worked example.
///
/// Parametrized over `stable_row_ids` to cover the address-based stale-Take path under both
/// row-id schemes.
#[rstest]
#[tokio::test]
async fn test_overlay_stale_drop_and_new_match(#[values(false, true)] stable_row_ids: bool) {
    let mut dataset = create_base_dataset_with(stable_row_ids).await;
    build_age_index(&mut dataset).await;

    // Fragment 0, offset 1 is id=1, age=10. The overlay (committed after the index)
    // changes its age to 999.
    let dataset = commit_overlay(
        dataset,
        "age_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Stale-drop: the index still holds age=10 for id=1, but its current value is 999,
    // so it must not be returned.
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    // New-match: the index never saw age=999, but re-evaluation finds it.
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);
    // An untouched indexed value is unaffected.
    assert_eq!(ids_matching(&dataset, "age = 20").await, vec![2]);
}

/// Row-level BTree precision: when one row in a covered fragment is stale, only that row is
/// blocked from the index result and re-evaluated on the stale-Take path. Non-stale rows in
/// the same fragment (including one that matches the predicate) remain on the indexed path.
///
/// Setup: fragment 0 has id=5 → age=50 (not stale). Overlay id=1 → age=50 (stale).
/// After the overlay two rows in fragment 0 have age=50. The row-level optimization must
/// return both: id=5 from the index and id=1 from the stale-Take path.
///
/// Parametrized over `stable_row_ids`: with stable row ids enabled the stale-Take path must
/// identify rows by physical address, not `_rowid`, or it would take the wrong rows.
#[rstest]
#[tokio::test]
async fn test_btree_overlay_row_level_precision(#[values(false, true)] stable_row_ids: bool) {
    let mut dataset = create_base_dataset_with(stable_row_ids).await;
    build_age_index(&mut dataset).await;

    // Fragment 0: ids 0-5, ages 0,10,20,30,40,50. Overlay offset 1 (id=1): age 10→50.
    // After this both id=1 and id=5 have age=50, in the same fragment.
    let dataset = commit_overlay(
        dataset,
        "age_row_level",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(50)])],
    )
    .await;

    // Stale drop: id=1's old age=10 entry must not appear.
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());

    // id=5 via index + id=1 via stale-Take path — both in fragment 0.
    assert_eq!(ids_matching(&dataset, "age = 50").await, vec![1, 5]);

    // Non-stale rows in the same fragment still return correctly.
    assert_eq!(ids_matching(&dataset, "age = 20").await, vec![2]);
    assert_eq!(ids_matching(&dataset, "age = 30").await, vec![3]);
}

/// `fast_search` skips *unindexed fragments*, but overlay masking on indexed fragments must
/// still apply: the drop-stale block and the stale-Take re-eval both run regardless of
/// `fast_search` on the scalar path. A regression that gated overlay masking behind
/// `!fast_search` would leak id=1's stale age=10 hit here.
#[tokio::test]
async fn test_btree_overlay_masked_under_fast_search() {
    let mut dataset = create_base_dataset().await;
    build_age_index(&mut dataset).await;

    // Fragment 0, offset 1 is id=1, age=10. Overlay (committed after the index) → age=999.
    let dataset = commit_overlay(
        dataset,
        "age_fast_search",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Stale hit dropped even under fast_search — the block is not gated by fast_search.
    assert_eq!(
        ids_matching_opts(&dataset, "age = 10", true).await,
        Vec::<i32>::new()
    );
    // The scalar re-eval path is likewise not gated, so the new value is still surfaced.
    assert_eq!(
        ids_matching_opts(&dataset, "age = 999", true).await,
        vec![1]
    );
    // An untouched indexed value on the same fragment is unaffected.
    assert_eq!(ids_matching_opts(&dataset, "age = 20", true).await, vec![2]);
}

/// An overlay touching only a non-indexed field excludes nothing from the index on `age`.
#[tokio::test]
async fn test_overlay_on_unrelated_field_excludes_nothing() {
    let mut dataset = create_base_dataset().await;
    build_age_index(&mut dataset).await;

    // Overlay field 0 (`id`), not the indexed `age`. The age index stays fully trusted.
    let dataset = commit_overlay(
        dataset,
        "id_overlay",
        0,
        &[0],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(777)])],
    )
    .await;

    // The age index is still trusted: age=10 finds the offset-1 row, whose id now reads
    // through the overlay as 777. The fragment was not routed to the flat path on account
    // of an overlay that touches no indexed field.
    assert_eq!(ids_matching(&dataset, "age = 10").await, vec![777]);
    // An untouched row is unaffected.
    assert_eq!(ids_matching(&dataset, "age = 20").await, vec![2]);
    // The overlaid id is the new value on read, and the old one is gone.
    assert_eq!(ids_matching(&dataset, "id = 777").await, vec![777]);
    assert_eq!(ids_matching(&dataset, "id = 1").await, Vec::<i32>::new());
}

/// An overlay whose `committed_version <= index.dataset_version` is already incorporated by
/// the index (the index was built reading merged values) and is not excluded.
#[tokio::test]
async fn test_overlay_older_than_index_not_excluded() {
    let dataset = create_base_dataset().await;

    // Commit the overlay first (age of id=1 becomes 999), then build the index on top.
    let mut dataset = commit_overlay(
        dataset,
        "age_overlay_old",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;
    build_age_index(&mut dataset).await;

    // The index incorporates the overlay, so it returns the merged value directly.
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
}

/// A covered offset whose overlay value is NULL overrides the cell to NULL, so the stale
/// index hit for its old value is dropped.
#[tokio::test]
async fn test_overlay_null_override() {
    let mut dataset = create_base_dataset().await;
    build_age_index(&mut dataset).await;

    // id=1 (age=10) is overridden to NULL.
    let dataset = commit_overlay(
        dataset,
        "age_overlay_null",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([None])],
    )
    .await;

    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    assert_eq!(ids_matching(&dataset, "age IS NULL").await, vec![1]);
}

/// Overlays on a non-first fragment are masked correctly, and a query spanning both
/// fragments returns the right rows.
///
/// Parametrized over `stable_row_ids`, and crucially overlays fragment 1 (ids 6..12), where a
/// physical address diverges from the stable row id — so this exercises the address-vs-row-id
/// distinction that a fragment-0 overlay cannot.
#[rstest]
#[tokio::test]
async fn test_overlay_multi_fragment(#[values(false, true)] stable_row_ids: bool) {
    let mut dataset = create_base_dataset_with(stable_row_ids).await;
    build_age_index(&mut dataset).await;

    // Fragment 1 holds ids 6..12 (ages 60..110). Offset 2 within fragment 1 is id=8,
    // age=80; change it to 60 (a value that also legitimately exists at id=6).
    let dataset = commit_overlay(
        dataset,
        "age_overlay_frag1",
        1,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([2])),
        vec![i32_array([Some(60)])],
    )
    .await;

    // id=8 no longer has age=80 (stale-drop on fragment 1).
    assert_eq!(ids_matching(&dataset, "age = 80").await, Vec::<i32>::new());
    // Both id=6 (base) and id=8 (overlay) now have age=60 (new-match added to base hit).
    assert_eq!(ids_matching(&dataset, "age = 60").await, vec![6, 8]);
    // A value in the untouched fragment 0 is still served correctly.
    assert_eq!(ids_matching(&dataset, "age = 30").await, vec![3]);
}

/// A deletion below an overlaid row must not corrupt the physical-offset → stable-row-id
/// translation used to build the overlay block mask.
///
/// Under stable row ids the stale-row block/take set is computed by mapping each stale
/// *physical offset* to its stable row id via the fragment's `RowIdSequence`. The sequence
/// keeps one entry per physical row (deleted rows are tracked separately by the deletion
/// vector, not compacted out), so the correct mapping is `sequence.get(offset)`. A regression
/// that instead advanced a `sequence.iter()` cursor only for non-deleted offsets desynced the
/// cursor after any deletion at an offset *below* the stale one, blocking/taking the wrong row
/// id: the stale index hit then leaked and the new value was never surfaced.
///
/// Setup (stable row ids): fragment 1 holds ids 6..12 at offsets 0..6. Delete id=6 (offset 0),
/// then overlay offset 2 (id=8, age 80 → 999). The deletion at offset 0 sits below the stale
/// offset 2, so a cursor-based translation would map offset 2 to id=7 instead of id=8.
///
/// Parametrized over `stable_row_ids`: only the stable-row-id path translates offsets to row
/// ids, so the bug is specific to it; the non-stable case (addresses are row ids) is a control.
#[rstest]
#[tokio::test]
async fn test_btree_overlay_stale_row_with_prior_deletion(
    #[values(false, true)] stable_row_ids: bool,
) {
    let mut dataset = create_base_dataset_with(stable_row_ids).await;
    build_age_index(&mut dataset).await;

    // Delete id=6 (fragment 1, offset 0) — a deletion hole below the row the overlay marks stale.
    dataset.delete("id = 6").await.unwrap();

    // Fragment 1, offset 2 is id=8 (age 80). The overlay (committed after the index) → age 999.
    let dataset = commit_overlay(
        dataset,
        "age_overlay_del",
        1,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([2])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Stale-drop: id=8's old age=80 index entry must not be returned.
    assert_eq!(ids_matching(&dataset, "age = 80").await, Vec::<i32>::new());
    // New-match: id=8's current age=999 is found by re-evaluating the stale row.
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![8]);
    // A non-stale row in the same deletion-bearing fragment is still served by the index.
    assert_eq!(ids_matching(&dataset, "age = 70").await, vec![7]);
    // The deleted row is gone.
    assert_eq!(ids_matching(&dataset, "age = 60").await, Vec::<i32>::new());
}

const VEC_DIM: i32 = 8;

fn vec_query() -> Vec<f32> {
    vec![1.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
}

/// 64-row two-fragment vector dataset with a single-partition IVF_FLAT index, then an overlay
/// on fragment 1 that moves id=35 (offset 3) onto `far` (away from the query) and id=40
/// (offset 8) onto the query. Built before the overlay, the index still believes id=35 is the
/// query and has never seen id=40 near it. Every other base vector is orthogonal to the query.
///
/// Overlaying fragment 1 (ids 32..64) is deliberate: a physical address diverges from the
/// stable row id there, so both the ANN prefilter block and the flat re-score take must operate
/// in the row-id domain when `stable_row_ids` is enabled.
async fn create_vector_overlay_dataset(stable_row_ids: bool) -> Dataset {
    let query = vec_query();
    let far = vec![0.0_f32, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

    let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(64);
    for i in 0..64 {
        if i == 35 {
            vectors.push(query.clone());
        } else {
            let mut v = vec![0.0_f32; VEC_DIM as usize];
            v[1] = (i + 2) as f32; // orthogonal to the query, distinct, far
            vectors.push(v);
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                VEC_DIM,
            ),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..64)),
            fsl(vectors, VEC_DIM),
        ],
    )
    .unwrap();
    let write_params = WriteParams {
        max_rows_per_file: 32,
        enable_stable_row_ids: stable_row_ids,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(reader, "memory://", Some(write_params))
        .await
        .unwrap();

    // Single-partition IVF_FLAT: the ANN searches every indexed row with exact distances.
    let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
    dataset
        .create_index(&["vec"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    commit_overlay(
        dataset,
        "vec_overlay",
        1,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([3, 8])),
        vec![fsl(vec![far, query], VEC_DIM)],
    )
    .await
}

/// Run a top-`k` ANN search for the standard query vector and return the returned `id`s,
/// optionally with `fast_search()` enabled.
async fn vector_query_ids(dataset: &Dataset, k: usize, fast_search: bool) -> Vec<i32> {
    let mut scanner = dataset.scan();
    scanner
        .nearest("vec", &arrow_array::Float32Array::from(vec_query()), k)
        .unwrap()
        .minimum_nprobes(1)
        .project(&["id"])
        .unwrap();
    if fast_search {
        scanner.fast_search();
    }
    let results = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    ids_from_batches(&results)
}

/// A vector index masks overlays: a row whose vector was moved (by a newer overlay) away
/// from the query is dropped from results, and a row moved *onto* the query is found by
/// re-scoring its current vector on the flat path — even though the index never saw it.
///
/// Parametrized over `stable_row_ids` to cover the row-id domain for both block and re-score.
#[rstest]
#[tokio::test]
async fn test_vector_index_rescore_on_overlay(#[values(false, true)] stable_row_ids: bool) {
    let dataset = create_vector_overlay_dataset(stable_row_ids).await;
    let ids = vector_query_ids(&dataset, 3, false).await;

    // id=40 was moved onto the query and is found by re-scoring (new-match recall).
    assert!(
        ids.contains(&40),
        "expected id=40 (re-scored to query) in {ids:?}"
    );
    // id=35's stale index entry (the query) must not resurface: its current vector is far.
    assert!(
        !ids.contains(&35),
        "stale vector for id=35 should be dropped, got {ids:?}"
    );
}

/// The ANN prefilter block that drops stale overlay rows runs regardless of `fast_search`;
/// only the flat re-score is gated by it. So under `fast_search` id=35's stale hit must still
/// be dropped, while id=40 (moved onto the query) is intentionally not re-scored — the same
/// recall tradeoff `fast_search` already makes for unindexed data. A regression that moved the
/// `overlay_block` computation inside the `!fast_search` guard would leak id=35's stale vector.
#[tokio::test]
async fn test_vector_overlay_stale_dropped_under_fast_search() {
    let dataset = create_vector_overlay_dataset(false).await;
    let ids = vector_query_ids(&dataset, 3, true).await;

    // Correctness: the stale index hit is dropped even though the re-score is skipped.
    assert!(
        !ids.contains(&35),
        "stale vector for id=35 must be dropped under fast_search, got {ids:?}"
    );
    // Recall tradeoff: fast_search skips the flat re-score, so the moved-on match is not surfaced.
    assert!(
        !ids.contains(&40),
        "fast_search skips re-score, so id=40 should be absent, got {ids:?}"
    );
}

/// A compound boolean predicate (age AND id) exercises the ScalarIndexExpr tree-walk in
/// `overlay_stale_index_rows`. An overlay on `age` marks fragment 0 stale from the `age`
/// index's perspective, so the compound query must re-evaluate fragment 0 on the flat path.
#[tokio::test]
async fn test_overlay_stale_with_compound_index_expression() {
    let mut dataset = create_base_dataset().await;
    // Build BTree indexes on both columns so a compound filter can use both.
    build_age_index(&mut dataset).await;
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    // Fragment 0 covers id=0..5, age=0..50. Overlay changes id=1's age from 10 to 999.
    let dataset = commit_overlay(
        dataset,
        "age_compound",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Compound query: both the `age` and `id` index are involved. The overlay on `age`
    // makes fragment 0 stale for the `age` index; it falls to the flat path, which uses
    // the merged (overlay) value. Result: the stale age=10 hit is gone, age=999 appears.
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);
    // A pure `id` query on an unaffected fragment still works correctly.
    assert_eq!(ids_matching(&dataset, "id = 2").await, vec![2]);
}

/// A `RewriteRows` update (under stable row ids) that touches only a *non-indexed* column moves
/// the matched rows to a new fragment and, because the scalar index's field was not modified,
/// extends that index's fragment coverage onto the new fragment
/// (`register_pure_rewrite_rows_update_frags_in_indices`) so its existing entries are reused.
///
/// That reuse is unsound when a moved row carried a data overlay on the *indexed* field: the
/// update materializes the overlay's current value into the new fragment, but the reused index
/// entry still holds the stale pre-overlay value, and the new fragment (now marked covered) no
/// longer falls to the flat path that previously served the correct value via overlay masking.
///
/// Here `age` is indexed and overlaid (id=1: age 10 -> 999); the update sets the non-indexed
/// `id` column on that row. After it, `age = 10` must stay dropped and `age = 999` must still
/// find the row — otherwise the stale index entry has resurfaced.
#[tokio::test]
async fn test_update_nonindexed_column_preserves_overlay_masking() {
    use crate::dataset::UpdateBuilder;

    let mut dataset = create_base_dataset_with(true).await;
    build_age_index(&mut dataset).await;

    // Overlay fragment 0, offset 1 (id=1): age 10 -> 999, committed after the index.
    let dataset = commit_overlay(
        dataset,
        "age_update",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Masking works before the update.
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);

    // Update only the non-indexed `id` column of the overlaid row. This is a rewrite-rows move:
    // the row (with age materialized to 999) is written to a new fragment and deleted from
    // fragment 0, keeping its stable row id.
    let dataset = UpdateBuilder::new(Arc::new(dataset))
        .update_where("id = 1")
        .unwrap()
        .set("id", "100")
        .unwrap()
        .build()
        .unwrap()
        .execute()
        .await
        .unwrap()
        .new_dataset;

    // Still masked: the stale age=10 entry must stay dropped and the overlaid age=999 value must
    // still be found (now on the moved row, whose id is 100).
    assert_eq!(
        ids_matching(&dataset, "age = 10").await,
        Vec::<i32>::new(),
        "stale index entry age=10 resurfaced after updating a non-indexed column"
    );
    assert_eq!(
        ids_matching(&dataset, "age = 999").await,
        vec![100],
        "overlaid value age=999 lost after updating a non-indexed column"
    );
    // A row untouched by the overlay is unaffected.
    assert_eq!(ids_matching(&dataset, "age = 20").await, vec![2]);
}

/// Text dataset: two fragments, 6 rows each. Schema: id (Int32), text (Utf8).
/// Texts are unique tokens so each row can be identified by its term.
async fn create_text_dataset(stable_row_ids: bool) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new("text", DataType::Utf8, true),
    ]));
    let texts: Vec<&str> = vec![
        "apple pie",
        "apple banana", // row 1, fragment 0 — will be overlaid in tests
        "cherry cake",
        "banana split",
        "orange juice",
        "grape vine",
        "mango sorbet", // fragment 1 starts here
        "pear tart",
        "lemon curd",
        "peach cobbler",
        "plum pudding",
        "fig newton",
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..12)),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .unwrap();
    let write_params = WriteParams {
        max_rows_per_file: 6,
        enable_stable_row_ids: stable_row_ids,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    Dataset::write(reader, "memory://", Some(write_params))
        .await
        .unwrap()
}

async fn build_text_fts_index(dataset: &mut Dataset) {
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();
}

/// FTS index with token positions stored, required for phrase queries.
async fn build_text_fts_index_with_positions(dataset: &mut Dataset) {
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();
}

/// Collect sorted IDs of rows returned by an FTS query on `text`.
async fn fts_ids(dataset: &Dataset, query: FullTextSearchQuery) -> Vec<i32> {
    let results = dataset
        .scan()
        .full_text_search(query)
        .unwrap()
        .project(&["id"])
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = ids_from_batches(&results);
    ids.sort_unstable();
    ids
}

async fn fts_ids_matching(dataset: &Dataset, term: &str) -> Vec<i32> {
    fts_ids(dataset, FullTextSearchQuery::new(term.to_owned())).await
}

#[tokio::test]
async fn test_ngram_optimize_preserves_overlay_staleness() {
    let mut dataset = create_text_dataset(false).await;
    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::NGram);
    let fragment_ids = dataset
        .get_fragments()
        .into_iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let mut segments = Vec::with_capacity(fragment_ids.len());
    for fragment_id in fragment_ids {
        segments.push(
            CreateIndexBuilder::new(&mut dataset, &["text"], IndexType::NGram, &params)
                .name("text_ngram".to_string())
                .fragments(vec![fragment_id])
                .execute_uncommitted()
                .await
                .unwrap(),
        );
    }
    let source_version = segments[0].dataset_version;
    dataset
        .commit_existing_index_segments("text_ngram", "text", segments)
        .await
        .unwrap();

    let mut dataset = commit_overlay(
        dataset,
        "ngram_text_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;
    dataset
        .optimize_indices(&OptimizeOptions::merge(2))
        .await
        .unwrap();

    let committed = dataset.load_indices_by_name("text_ngram").await.unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].dataset_version, source_version);
    assert_eq!(
        ids_matching(&dataset, "contains(text, 'apple')").await,
        vec![0]
    );
    assert_eq!(
        ids_matching(&dataset, "contains(text, 'mango')").await,
        vec![1, 6]
    );
}

#[tokio::test]
async fn test_btree_physical_merge_preserves_overlay_staleness() {
    let mut dataset = create_base_dataset().await;
    let params = ScalarIndexParams::default();
    let mut segments = Vec::new();
    for fragment in dataset.get_fragments() {
        segments.push(
            CreateIndexBuilder::new(&mut dataset, &["age"], IndexType::BTree, &params)
                .name("age_btree".to_string())
                .fragments(vec![fragment.id() as u32])
                .execute_uncommitted()
                .await
                .unwrap(),
        );
    }
    let source_version = segments[0].dataset_version;
    let mut dataset = commit_overlay(
        dataset,
        "btree_before_merge",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    let merged = dataset
        .merge_existing_index_segments(segments)
        .await
        .unwrap();
    assert_eq!(merged.dataset_version, source_version);
    dataset
        .commit_existing_index_segments("age_btree", "age", vec![merged])
        .await
        .unwrap();

    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);
}

#[tokio::test]
async fn test_ngram_remap_excludes_newer_overlay_fragments() {
    let mut dataset = create_text_dataset(false).await;
    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::NGram);
    dataset
        .create_index(
            &["text"],
            IndexType::NGram,
            Some("text_ngram".to_string()),
            &params,
            false,
        )
        .await
        .unwrap();
    let source_version =
        dataset.load_indices_by_name("text_ngram").await.unwrap()[0].dataset_version;

    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 12,
            defer_index_remap: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let compacted_fragment_id = dataset.get_fragments()[0].id();
    let mut dataset = commit_overlay(
        dataset,
        "ngram_after_compaction",
        compacted_fragment_id as u64,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    remapping::remap_column_index(&mut dataset, &["text"], Some("text_ngram".to_string()))
        .await
        .unwrap();

    let committed = dataset.load_indices_by_name("text_ngram").await.unwrap();
    assert_eq!(committed.len(), 1);
    assert!(committed[0].dataset_version > source_version);
    assert!(
        !committed[0]
            .fragment_bitmap
            .as_ref()
            .unwrap()
            .contains(compacted_fragment_id as u32)
    );
    assert_eq!(
        ids_matching(&dataset, "contains(text, 'apple')").await,
        vec![0]
    );
    assert_eq!(
        ids_matching(&dataset, "contains(text, 'mango')").await,
        vec![1, 6]
    );
}

async fn fts_phrase_ids_matching(dataset: &Dataset, phrase: &str) -> Vec<i32> {
    use lance_index::scalar::inverted::query::{FtsQuery, PhraseQuery};

    let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(
        PhraseQuery::new(phrase.to_owned()).with_column(Some("text".to_owned())),
    ));
    fts_ids(dataset, query).await
}

/// An overlay committed after the FTS index is built replaces a row's text. Searching for
/// the old term must not return the stale row; searching for the new term must find it.
#[rstest]
#[tokio::test]
async fn test_fts_overlay_stale_drop_and_new_match(#[values(false, true)] stable_row_ids: bool) {
    let mut dataset = create_text_dataset(stable_row_ids).await;
    build_text_fts_index(&mut dataset).await;

    // fragment 0, row offset 1 (id=1): "apple banana" → "cherry mango"
    // field ID 1 is the `text` column.
    let dataset = commit_overlay(
        dataset,
        "text_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    // "apple" now matches only id=0 ("apple pie"); id=1's stale index entry must be dropped.
    assert_eq!(fts_ids_matching(&dataset, "apple").await, vec![0]);

    // "banana" matched id=1 and id=3 before; after overlay id=1's stale entry must be gone.
    assert_eq!(fts_ids_matching(&dataset, "banana").await, vec![3]);

    // "cherry" now matches id=1 (via flat path on stale fragment) and id=2 ("cherry cake").
    let cherry_ids = fts_ids_matching(&dataset, "cherry").await;
    assert!(
        cherry_ids.contains(&1),
        "id=1 overlay→cherry mango should be found: {cherry_ids:?}"
    );
    assert!(
        cherry_ids.contains(&2),
        "id=2 cherry cake should still be found: {cherry_ids:?}"
    );

    // "mango" now matches id=1 (overlay) and id=6 ("mango sorbet" in fragment 1).
    let mango_ids = fts_ids_matching(&dataset, "mango").await;
    assert!(
        mango_ids.contains(&1),
        "id=1 overlay→cherry mango should be found: {mango_ids:?}"
    );
    assert!(
        mango_ids.contains(&6),
        "id=6 mango sorbet should still be found: {mango_ids:?}"
    );
}

/// A phrase query must drop stale indexed positions and re-evaluate the current
/// overlay value on the flat phrase path.
#[rstest]
#[tokio::test]
async fn test_fts_phrase_overlay_stale_drop(#[values(false, true)] stable_row_ids: bool) {
    let mut dataset = create_text_dataset(stable_row_ids).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    // Before any overlay the phrase "apple banana" matches only id=1.
    assert_eq!(
        fts_phrase_ids_matching(&dataset, "apple banana").await,
        vec![1]
    );

    // Overlay id=1's text (field 1) so the phrase no longer applies to its current value.
    let dataset = commit_overlay(
        dataset,
        "phrase_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    // The stale inverted-index positions for "apple banana" on id=1 must not be returned.
    assert_eq!(
        fts_phrase_ids_matching(&dataset, "apple banana").await,
        Vec::<i32>::new()
    );
    assert_eq!(
        fts_phrase_ids_matching(&dataset, "cherry mango").await,
        vec![1]
    );
}

#[tokio::test]
async fn test_fts_empty_fragment_selection_is_empty() {
    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    let mut match_scan = dataset.scan();
    match_scan.with_fragments(Vec::new());
    match_scan
        .full_text_search(FullTextSearchQuery::new("apple".to_owned()))
        .unwrap();
    match_scan.project(&["id"]).unwrap();
    let match_plan = match_scan.explain_plan(false).await.unwrap();
    assert!(
        match_plan.contains("EmptyExec"),
        "explicit empty fragment selection should produce EmptyExec: {match_plan}"
    );
    assert_eq!(match_scan.try_into_batch().await.unwrap().num_rows(), 0);

    let mut phrase_scan = dataset.scan();
    phrase_scan.with_fragments(Vec::new());
    phrase_scan
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Phrase(
            PhraseQuery::new("apple pie".to_owned()).with_column(Some("text".to_owned())),
        )))
        .unwrap();
    phrase_scan.project(&["id"]).unwrap();
    let phrase_plan = phrase_scan.explain_plan(false).await.unwrap();
    assert!(
        phrase_plan.contains("EmptyExec"),
        "explicit empty fragment selection should produce EmptyExec: {phrase_plan}"
    );
    assert_eq!(phrase_scan.try_into_batch().await.unwrap().num_rows(), 0);
}

#[rstest]
#[tokio::test]
async fn test_fts_combines_indexed_overlay_stale_and_unindexed_rows(
    #[values(false, true)] stable_row_ids: bool,
) {
    let mut dataset = create_text_dataset(stable_row_ids).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    let batch =
        arrow_array::record_batch!(("id", Int32, [12]), ("text", Utf8, ["cherry mango"])).unwrap();
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let dataset = Dataset::write(
        reader,
        Arc::new(dataset),
        Some(WriteParams {
            mode: crate::dataset::write::WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let dataset = commit_overlay(
        dataset,
        "fts_combined_flat_paths",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    assert_eq!(fts_ids_matching(&dataset, "mango").await, vec![1, 6, 12]);
    assert_eq!(
        fts_phrase_ids_matching(&dataset, "cherry mango").await,
        vec![1, 12]
    );
}

#[rstest]
#[tokio::test]
async fn test_fts_overlay_row_level_masking_under_fast_search(
    #[values(false, true)] stable_row_ids: bool,
) {
    let mut dataset = create_text_dataset(stable_row_ids).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    let dataset = commit_overlay(
        dataset,
        "fts_row_level_fast_search",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    let mut match_scan = dataset.scan();
    match_scan
        .full_text_search(FullTextSearchQuery::new("apple".to_owned()))
        .unwrap();
    match_scan.project(&["id"]).unwrap();
    match_scan.fast_search();
    let match_result = match_scan.try_into_batch().await.unwrap();
    assert_eq!(
        ids_from_batches(std::slice::from_ref(&match_result)),
        vec![0]
    );

    let mut indexed_phrase_scan = dataset.scan();
    indexed_phrase_scan
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Phrase(
            PhraseQuery::new("apple pie".to_owned()).with_column(Some("text".to_owned())),
        )))
        .unwrap();
    indexed_phrase_scan.project(&["id"]).unwrap();
    indexed_phrase_scan.fast_search();
    let indexed_phrase_result = indexed_phrase_scan.try_into_batch().await.unwrap();
    assert_eq!(
        ids_from_batches(std::slice::from_ref(&indexed_phrase_result)),
        vec![0]
    );

    let mut new_phrase_scan = dataset.scan();
    new_phrase_scan
        .full_text_search(FullTextSearchQuery::new_query(FtsQuery::Phrase(
            PhraseQuery::new("cherry mango".to_owned()).with_column(Some("text".to_owned())),
        )))
        .unwrap();
    new_phrase_scan.project(&["id"]).unwrap();
    new_phrase_scan.fast_search();
    assert_eq!(
        new_phrase_scan.try_into_batch().await.unwrap().num_rows(),
        0
    );
}

#[rstest]
#[tokio::test]
async fn test_fts_overlay_flat_path_takes_only_stale_rows(
    #[values(false, true)] stable_row_ids: bool,
) {
    let mut dataset = create_text_dataset(stable_row_ids).await;
    build_text_fts_index(&mut dataset).await;

    let dataset = commit_overlay(
        dataset,
        "fts_targeted_take",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    let mut scan = dataset.scan();
    scan.full_text_search(FullTextSearchQuery::new("cherry".to_owned()))
        .unwrap();
    scan.project(&["id"]).unwrap();
    let plan = scan.create_plan().await.unwrap();

    let mut nodes = vec![plan];
    let mut flat_path_uses_targeted_take = false;
    while let Some(node) = nodes.pop() {
        if node.downcast_ref::<FlatMatchQueryExec>().is_some() {
            let mut flat_nodes = node.children().into_iter().cloned().collect::<Vec<_>>();
            while let Some(flat_node) = flat_nodes.pop() {
                if flat_node
                    .downcast_ref::<FilteredReadExec>()
                    .is_some_and(|read| read.index_input().is_some())
                {
                    flat_path_uses_targeted_take = true;
                    break;
                }
                flat_nodes.extend(flat_node.children().into_iter().cloned());
            }
        }
        nodes.extend(node.children().into_iter().cloned());
    }

    assert!(
        flat_path_uses_targeted_take,
        "overlay-stale FTS rows must be re-evaluated through a targeted take"
    );
}

#[tokio::test]
async fn test_fts_phrase_searches_unindexed_fragments_unless_fast_search() {
    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    let batch = arrow_array::record_batch!(
        ("id", Int32, [12, 13, 14]),
        (
            "text",
            Utf8,
            [
                "kiwi berry",
                "kiwi berry filling",
                "kiwi berry filling filling"
            ]
        )
    )
    .unwrap();
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let dataset = Dataset::write(
        reader,
        Arc::new(dataset),
        Some(WriteParams {
            mode: crate::dataset::write::WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(
        PhraseQuery::new("kiwi berry".to_owned()).with_column(Some("text".to_owned())),
    ));
    assert_eq!(fts_ids(&dataset, query.clone()).await, vec![12, 13, 14]);

    let mut limited_scan = dataset.scan();
    limited_scan.with_fragments(vec![dataset.fragments().last().unwrap().clone()]);
    limited_scan
        .full_text_search(query.clone().limit(Some(1)))
        .unwrap();
    limited_scan.project(&["id"]).unwrap();
    let limited_result = limited_scan.try_into_batch().await.unwrap();
    assert_eq!(
        ids_from_batches(std::slice::from_ref(&limited_result)),
        vec![12]
    );

    let mut filtered_scan = dataset.scan();
    filtered_scan.full_text_search(query.clone()).unwrap();
    filtered_scan.project(&["id"]).unwrap();
    filtered_scan.filter("id < 12").unwrap();
    assert_eq!(filtered_scan.try_into_batch().await.unwrap().num_rows(), 0);

    let mut fast_scan = dataset.scan();
    fast_scan.full_text_search(query).unwrap();
    fast_scan.project(&["id"]).unwrap();
    fast_scan.fast_search();
    assert_eq!(fast_scan.try_into_batch().await.unwrap().num_rows(), 0);
}

#[tokio::test]
async fn test_fts_phrase_stale_rows_honor_query_limit() {
    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    let dataset = commit_overlay(
        dataset,
        "phrase_limit_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some(
            "apple banana filling filling",
        )]))],
    )
    .await;

    let query = FullTextSearchQuery::new_query(FtsQuery::Phrase(
        PhraseQuery::new("apple".to_owned()).with_column(Some("text".to_owned())),
    ))
    .limit(Some(1));
    let mut scan = dataset.scan();
    scan.with_fragments(vec![dataset.fragments()[0].clone()]);
    scan.full_text_search(query).unwrap();
    scan.project(&["id"]).unwrap();

    let result = scan.try_into_batch().await.unwrap();
    assert_eq!(ids_from_batches(std::slice::from_ref(&result)), vec![0]);
}

/// Overlay routing must select FTS segments by both field and document
/// granularity when Row and ListElement indexes coexist.
#[tokio::test]
async fn test_list_element_fts_overlay_uses_exact_index_and_flat_fallback() {
    let ids = Arc::new(Int32Array::from_iter_values(0..4)) as ArrayRef;
    let tags = string_lists(&[
        &["old phrase", "keep"],
        &["other"],
        &["new phrase"],
        &["unrelated"],
    ]);
    let batch = RecordBatch::try_from_iter(vec![("id", ids), ("tags", tags)]).unwrap();
    let schema = batch.schema();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    dataset
        .create_index(
            &["tags"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default()
                .with_position(true)
                .document_granularity(DocumentGranularity::ListElement),
            true,
        )
        .await
        .unwrap();
    dataset
        .create_index(
            &["tags"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default().with_position(true),
            true,
        )
        .await
        .unwrap();

    let dataset = commit_overlay(
        dataset,
        "list_element_text_overlay",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([0])),
        vec![string_lists(&[&["new phrase", "keep"]])],
    )
    .await;

    let list_element_match = |terms: &str| {
        FullTextSearchQuery::new_query(FtsQuery::Match(
            MatchQuery::new(terms.to_owned())
                .with_column(Some("tags".to_owned()))
                .with_document_granularity(DocumentGranularity::ListElement),
        ))
    };
    let list_element_phrase = |terms: &str| {
        FullTextSearchQuery::new_query(FtsQuery::Phrase(
            PhraseQuery::new(terms.to_owned())
                .with_column(Some("tags".to_owned()))
                .with_document_granularity(DocumentGranularity::ListElement),
        ))
    };

    assert_eq!(
        fts_ids(&dataset, list_element_match("old")).await,
        Vec::<i32>::new()
    );
    assert_eq!(
        fts_ids(&dataset, list_element_match("new")).await,
        vec![0, 2]
    );
    assert_eq!(
        fts_ids(&dataset, list_element_phrase("new phrase")).await,
        vec![0, 2]
    );
}

/// An overlay on a non-FTS field must not exclude the fragment from phrase search.
#[tokio::test]
async fn test_fts_phrase_overlay_unrelated_field_not_excluded() {
    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index_with_positions(&mut dataset).await;

    // Overlay field 0 (`id`), not the FTS-indexed `text` column: phrase coverage is untouched.
    let dataset = commit_overlay(
        dataset,
        "id_overlay",
        0,
        &[0],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(777)])],
    )
    .await;

    assert_eq!(
        fts_phrase_ids_matching(&dataset, "apple banana").await,
        vec![777]
    );
}

/// An overlay on a field the FTS index does NOT cover must not exclude anything.
#[tokio::test]
async fn test_fts_overlay_unrelated_field_not_excluded() {
    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index(&mut dataset).await;

    // Overlay field 0 (id) — not covered by the FTS index on `text`.
    let dataset = commit_overlay(
        dataset,
        "id_overlay_for_fts",
        0,
        &[0],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // FTS coverage must be unchanged — both rows containing "apple" are still returned.
    // The `id` overlay changes row offset 1's id from 1 to 999, so the projected id column
    // reflects the overlay even though the FTS index correctly returned that row.
    assert_eq!(fts_ids_matching(&dataset, "apple").await, vec![0, 999]);
    assert_eq!(fts_ids_matching(&dataset, "banana").await, vec![3, 999]);
}

/// Benchmark: measure query latency for BTree, FTS, and vector ANN with 0/4/16 overlay layers.
///
/// Run with:
/// cargo test -p lance --lib --profile release-with-debug -- overlay_index_masking::bench --ignored --nocapture
#[tokio::test]
#[ignore = "benchmark"]
#[allow(clippy::print_stdout)]
async fn bench_index_query_overlay_overhead() {
    use std::time::Instant;

    use arrow_array::Float32Array;

    const DIM: i32 = 32;
    const ROWS: i32 = 1_000_000;
    const ROWS_PER_FRAG: i32 = 100_000; // 10 fragments
    const ITERS: u32 = 10; // large scans — 10 is enough for stable averages

    // Fixed disk path so timings are comparable across runs. Deleted and recreated fresh.
    let uri = "/tmp/lance-bench-overlay-oss1325";
    if std::path::Path::new(uri).exists() {
        std::fs::remove_dir_all(uri).unwrap();
    }

    // --- Build 1M-row dataset on local disk --------------------------------
    // Schema: id, age, vec, text. Resolve field IDs from the Lance schema instead of
    // assuming how nested Arrow child fields are numbered.

    println!("Building {ROWS}-row dataset at {uri} (this takes ~30 s)...");

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("age", DataType::Int32, false),
        ArrowField::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                DIM,
            ),
            false,
        ),
        ArrowField::new("text", DataType::Utf8, false),
    ]));

    let row_ids: Vec<i32> = (0..ROWS).collect();
    let ages: Vec<i32> = row_ids.iter().map(|&i| i * 10).collect();
    // Build the 128 MB flat float array directly (avoids 1M per-row Vec allocations).
    let flat_vecs: Vec<f32> = (0..(ROWS as usize * DIM as usize))
        .map(|j| (j / DIM as usize) as f32 % 1000.0)
        .collect();
    let vec_col = Arc::new(
        arrow_array::FixedSizeListArray::try_new(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            DIM,
            Arc::new(Float32Array::from(flat_vecs)),
            None,
        )
        .unwrap(),
    );
    let text_col = Arc::new(StringArray::from_iter_values(
        (0..ROWS).map(|row| if row == 42 { "needle" } else { "common" }),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(row_ids)),
            Arc::new(Int32Array::from(ages)),
            vec_col,
            text_col,
        ],
    )
    .unwrap();

    let write_params = WriteParams {
        max_rows_per_file: ROWS_PER_FRAG as usize,
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(reader, uri, Some(write_params))
        .await
        .unwrap();
    let text_field_id = dataset.schema().field_id("text").unwrap();

    println!("Building BTree index on age...");
    dataset
        .create_index(
            &["age"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    println!("Building IVF_FLAT(1 partition) index on vec...");
    dataset
        .create_index(
            &["vec"],
            IndexType::Vector,
            None,
            &VectorIndexParams::ivf_flat(1, MetricType::L2),
            true,
        )
        .await
        .unwrap();

    println!("Building FTS index on text...");
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();

    println!("Indexes built.\n");

    // --- Timing helper ---------------------------------------------------

    async fn timeit<F, Fut>(iters: u32, mut f: F) -> f64
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        f().await; // warmup
        let t0 = Instant::now();
        for _ in 0..iters {
            f().await;
        }
        t0.elapsed().as_secs_f64() * 1000.0 / iters as f64
    }

    // === Scenario A: BTree query overhead ================================
    //
    // Overlay on `age` (field 1), covering only offset 0 of fragment 0. The stale row is
    // blocked from the BTree and re-evaluated by targeted take.
    //
    // btree_same_fragment: `age = 420` → id=42 → in fragment 0 (rows 0..99999).
    //   The matching row stays indexed even though another row in the fragment is stale.
    //
    // btree_other_fragment: `age = 1000420` → id=100042 → in fragment 1.
    //   This isolates the index-lookup baseline outside the overlaid fragment.
    println!("=== Scenario A: BTree (one stale row in fragment 0) ===");
    println!(
        "{:>10}  {:>14}  {:>14}",
        "overlays", "same_frag_ms", "other_frag_ms"
    );

    let mut committed_a = 0u32;
    for num_overlays in [0u32, 1, 4, 16] {
        // Commit only the delta since the last iteration.
        for layer in committed_a..num_overlays {
            dataset = commit_overlay(
                dataset,
                &format!("age_ol{layer}"),
                0,    // fragment 0
                &[1], // field 1 = age
                OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                vec![i32_array([Some(999)])],
            )
            .await;
        }
        committed_a = num_overlays;

        let ds = Arc::new(dataset.clone());

        // Same-fragment indexed match plus targeted re-evaluation of the stale row.
        let ds2 = ds.clone();
        let same_fragment_ms = timeit(ITERS, || {
            let ds = ds2.clone();
            async move {
                ds.scan()
                    .filter("age = 420")
                    .unwrap()
                    .project(&["age"])
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
            }
        })
        .await;

        // Fragment 1 never has a stale row and stays entirely index-served.
        let ds2 = ds.clone();
        let other_fragment_ms = timeit(ITERS, || {
            let ds = ds2.clone();
            async move {
                ds.scan()
                    .filter("age = 1000420")
                    .unwrap()
                    .project(&["age"])
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
            }
        })
        .await;

        println!("{num_overlays:>10}  {same_fragment_ms:>14.1}  {other_fragment_ms:>14.1}");
    }

    // === Scenario B: Vector ANN overhead =================================
    //
    // Overlay on `vec` (field 2), covering only offset 0 of fragment 0.
    // The field-aware check means the 16 age overlays from Scenario A do NOT affect
    // the vector index (they touch field 1, not field 2). Only a vec overlay (field 2)
    // marks fragment 0 stale for the vector index.
    //
    // With a vec overlay, only the stale row is excluded from ANN and re-scored exactly.
    println!("\n=== Scenario B: Vector ANN (one stale row re-scored) ===");
    println!("{:>12}  {:>10}", "vec_overlays", "ann_ms");

    let query_vec = Float32Array::from(vec![0.5f32; DIM as usize]);

    for num_vec_overlays in [0u32, 1] {
        if num_vec_overlays == 1 {
            dataset = commit_overlay(
                dataset,
                "vec_ol0",
                0,    // fragment 0
                &[2], // field 2 = vec (FixedSizeList top-level field)
                OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                vec![fsl(vec![vec![0.0f32; DIM as usize]], DIM)],
            )
            .await;
        }

        let ds = Arc::new(dataset.clone());
        let ds2 = ds.clone();
        let qv = query_vec.clone();
        let ann_ms = timeit(ITERS, || {
            let ds = ds2.clone();
            let q = qv.clone();
            async move {
                ds.scan()
                    .nearest("vec", &q, 10)
                    .unwrap()
                    .minimum_nprobes(1)
                    .project(&["id"])
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
            }
        })
        .await;

        println!("{num_vec_overlays:>12}  {ann_ms:>10.1}");
    }

    // === Scenario C: FTS overhead ========================================
    //
    // The FTS index has one segment spanning all 10 fragments. An overlay on one text row must
    // keep that segment indexed, block just the stale row, and re-evaluate that row by targeted
    // take. `needle` belongs to an unaffected row in the same fragment as the stale row.
    println!("\n=== Scenario C: FTS (one stale row in a 1M-row segment) ===");
    println!("{:>13}  {:>10}", "text_overlays", "fts_ms");

    for num_text_overlays in [0u32, 1] {
        if num_text_overlays == 1 {
            dataset = commit_overlay(
                dataset,
                "text_ol0",
                0,
                &[text_field_id],
                OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                vec![Arc::new(StringArray::from(vec![Some("updated")]))],
            )
            .await;
        }

        let ds = Arc::new(dataset.clone());
        let ds2 = ds.clone();
        let fts_ms = timeit(ITERS, || {
            let ds = ds2.clone();
            async move {
                let result = ds
                    .scan()
                    .full_text_search(FullTextSearchQuery::new("needle".to_owned()))
                    .unwrap()
                    .project(&["id"])
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
                assert_eq!(result.num_rows(), 1);
            }
        })
        .await;

        println!("{num_text_overlays:>13}  {fts_ms:>10.1}");
    }
}

async fn append_age_fragment(dataset: &mut Dataset, ids: std::ops::Range<i32>) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new("age", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(ids.clone())),
            Arc::new(Int32Array::from_iter_values(ids.map(|v| v * 10))),
        ],
    )
    .unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();
}

// `OptimizeIndices` merges an index's delta segments without re-reading data overlays, so the
// merged segment carries the old segments' pre-overlay entries. What keeps those entries masked
// is that the merge stamps the new segment with the *oldest* merged segment's `dataset_version`
// rather than the current one, leaving the mask's version gate
// (`overlay.committed_version > segment.dataset_version`) on.
//
// That invariant is easy to break by accident -- stamping the current version un-masks every
// carried-over entry -- and nothing else asserts it end to end. The following tests pin it down
// for each index type.

/// Scalar (BTree, Bitmap, ZoneMap): a range query over the indexed column after an overlay +
/// optimize must still drop the stale value and surface the overlaid one.
///
/// ZoneMap is the case worth having: it ignores `OldIndexDataFilter`, so nothing scrubs the
/// pre-overlay zone summaries out of the merged segment. Breaking the version gate shows up here
/// as a false *negative* -- the stale zone prunes the overlaid value -- rather than the resurfaced
/// stale entry the other two produce.
#[rstest]
#[case::btree(IndexType::BTree)]
#[case::bitmap(IndexType::Bitmap)]
#[case::zonemap(IndexType::ZoneMap)]
#[tokio::test]
async fn test_optimize_preserves_scalar_overlay_masking(#[case] index_type: IndexType) {
    use crate::index::DatasetIndexExt;
    use lance_index::optimize::OptimizeOptions;
    use lance_index::scalar::BuiltinIndexType;

    let params = match index_type {
        IndexType::Bitmap => ScalarIndexParams::for_builtin(BuiltinIndexType::Bitmap),
        IndexType::ZoneMap => ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap),
        _ => ScalarIndexParams::default(),
    };
    let mut dataset = create_base_dataset().await;
    dataset
        .create_index(&["age"], index_type, None, &params, true)
        .await
        .unwrap();

    // Overlay fragment 0, offset 1 (id=1): age 10 -> 999, committed after the index.
    let mut dataset = commit_overlay(
        dataset,
        "age_opt",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    // Masking works before optimize.
    assert_eq!(ids_matching(&dataset, "age = 10").await, Vec::<i32>::new());
    assert_eq!(ids_matching(&dataset, "age = 999").await, vec![1]);

    // Append an unindexed fragment so the merge does real work, then merge all deltas.
    append_age_fragment(&mut dataset, 12..18).await;
    dataset
        .optimize_indices(&OptimizeOptions::merge(10))
        .await
        .unwrap();

    // Still masked: the stale age=10 entry stays dropped and the overlaid age=999 value stays
    // visible, because the merged segment kept the old segment's `dataset_version`.
    assert_eq!(
        ids_matching(&dataset, "age = 10").await,
        Vec::<i32>::new(),
        "stale index entry age=10 for id=1 resurfaced after optimize"
    );
    assert_eq!(
        ids_matching(&dataset, "age = 999").await,
        vec![1],
        "overlaid value age=999 dropped after optimize"
    );
    // A row untouched by the overlay is unaffected.
    assert_eq!(ids_matching(&dataset, "age = 20").await, vec![2]);
}

/// ZoneMap seed path: an *unindexed* fragment carrying an overlay gets folded into the merged
/// segment from its data file's seed buffer. A seed is a zone summary captured while the base
/// data file was written, so it describes pre-overlay values -- the merged segment ends up
/// holding zones that never saw the overlay.
///
/// That is only safe because the merge keeps the old `dataset_version`, so the mask still covers
/// those rows. Stamp the current version (or teach the seed path to claim freshness) and the
/// overlaid value becomes unfindable, since its stale zone prunes it.
///
/// `name` is Utf8, for which seeds are on by default (`default_use_seeds`).
#[tokio::test]
async fn test_optimize_seed_path_respects_overlay() {
    use crate::index::DatasetIndexExt;
    use lance_index::optimize::OptimizeOptions;
    use lance_index::scalar::BuiltinIndexType;

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new("name", DataType::Utf8, true),
    ]));
    let names: Vec<String> = (0..12).map(|i| format!("n{i:02}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..12)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
        "memory://",
        Some(WriteParams {
            max_rows_per_file: 6,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["name"],
            IndexType::ZoneMap,
            None,
            &ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap),
            true,
        )
        .await
        .unwrap();

    // Append fragment 2. The index already exists, so the write emits a zone-map seed buffer
    // into the new data file.
    let appended: Vec<String> = (12..18).map(|i| format!("n{i:02}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(12..18)),
            Arc::new(StringArray::from(appended)),
        ],
    )
    .unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();

    // Overlay fragment 2, offset 1 (id=13): "n13" -> "zzz", well outside the seed's zone range.
    let mut dataset = commit_overlay(
        dataset,
        "name_seed",
        2,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("zzz")]))],
    )
    .await;

    // Fragment 2 is unindexed, so the overlaid value is visible via the flat path.
    assert_eq!(ids_matching(&dataset, "name = 'zzz'").await, vec![13]);
    assert_eq!(
        ids_matching(&dataset, "name = 'n13'").await,
        Vec::<i32>::new()
    );

    dataset
        .optimize_indices(&OptimizeOptions::merge(10))
        .await
        .unwrap();

    assert_eq!(
        ids_matching(&dataset, "name = 'zzz'").await,
        vec![13],
        "overlaid value dropped after optimize: the merged segment took pre-overlay zones \
         from fragment 2's seed buffer"
    );
    assert_eq!(
        ids_matching(&dataset, "name = 'n13'").await,
        Vec::<i32>::new(),
        "stale pre-overlay value resurfaced after optimize"
    );
    // Rows the overlay never touched keep working through the index.
    assert_eq!(ids_matching(&dataset, "name = 'n14'").await, vec![14]);
    assert_eq!(ids_matching(&dataset, "name = 'n03'").await, vec![3]);
}

/// `DatasetStatistics::column_value_range` folds ZoneMap summaries into a global `[min, max]`
/// that callers may prune with, so it must be a superset of the live values. An overlay can
/// move a value outside the summarised range, and the ZoneMap never saw it.
#[tokio::test]
async fn test_column_value_range_none_under_overlay() {
    use crate::index::DatasetIndexExt;
    use datafusion::scalar::ScalarValue;
    use lance_index::scalar::BuiltinIndexType;

    let mut dataset = create_base_dataset().await;
    dataset
        .create_index(
            &["age"],
            IndexType::ZoneMap,
            None,
            &ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap),
            true,
        )
        .await
        .unwrap();

    assert_eq!(
        dataset
            .statistics()
            .column_value_range("age")
            .await
            .unwrap(),
        Some((ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(110))))
    );

    // age 10 -> 999 on fragment 0, committed after the index: 999 is outside [0, 110].
    let dataset = commit_overlay(
        dataset,
        "age_range",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![i32_array([Some(999)])],
    )
    .await;

    assert_eq!(
        dataset
            .statistics()
            .column_value_range("age")
            .await
            .unwrap(),
        None,
        "ZoneMap range must not be reported once an overlay may have moved a value outside it"
    );
}

/// FTS: after an overlay replaces a row's text and the index is optimized, searching for the old
/// terms must not return the stale row, and the new terms must find it.
#[tokio::test]
async fn test_optimize_preserves_fts_overlay_masking() {
    use crate::index::DatasetIndexExt;
    use lance_index::optimize::OptimizeOptions;

    let mut dataset = create_text_dataset(false).await;
    build_text_fts_index(&mut dataset).await;

    // fragment 0, offset 1 (id=1): "apple banana" -> "cherry mango".
    let mut dataset = commit_overlay(
        dataset,
        "text_opt",
        0,
        &[1],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1])),
        vec![Arc::new(StringArray::from(vec![Some("cherry mango")]))],
    )
    .await;

    // Masking works before optimize: id=1 no longer matches "banana"/"apple".
    assert_eq!(fts_ids_matching(&dataset, "banana").await, vec![3]);
    assert_eq!(fts_ids_matching(&dataset, "apple").await, vec![0]);

    // Append an unindexed fragment of new text, then merge all deltas.
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(12..18)),
            Arc::new(StringArray::from(vec![
                "kiwi", "melon", "date", "guava", "papaya", "lychee",
            ])),
        ],
    )
    .unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();
    dataset
        .optimize_indices(&OptimizeOptions::merge(10))
        .await
        .unwrap();

    // Still masked: id=1's stale "apple"/"banana" postings stay dropped.
    assert_eq!(
        fts_ids_matching(&dataset, "banana").await,
        vec![3],
        "stale FTS posting for id=1 (banana) resurfaced after optimize"
    );
    assert_eq!(
        fts_ids_matching(&dataset, "apple").await,
        vec![0],
        "stale FTS posting for id=1 (apple) resurfaced after optimize"
    );
    // The overlaid terms are found via the flat path.
    assert!(fts_ids_matching(&dataset, "cherry").await.contains(&1));
    assert!(fts_ids_matching(&dataset, "mango").await.contains(&1));
}

/// Vector (IVF): after an overlay moves a row's vector and the index is optimized, the ANN must
/// not resurface the stale vector, and the moved-onto-query row is found by flat re-scoring.
#[tokio::test]
async fn test_optimize_preserves_vector_overlay_masking() {
    use crate::index::DatasetIndexExt;
    use lance_index::optimize::OptimizeOptions;

    // Overlay on fragment 1 moves id=35 away from the query and id=40 onto it.
    let mut dataset = create_vector_overlay_dataset(false).await;

    // Masking works before optimize.
    let before = vector_query_ids(&dataset, 3, false).await;
    assert!(
        !before.contains(&35),
        "pre-optimize id=35 should be dropped: {before:?}"
    );
    assert!(
        before.contains(&40),
        "pre-optimize id=40 should be found: {before:?}"
    );

    // Append an unindexed fragment of far vectors, then merge all deltas.
    let far_vecs: Vec<Vec<f32>> = (0..32)
        .map(|i| {
            let mut v = vec![0.0_f32; VEC_DIM as usize];
            v[1] = (i + 200) as f32;
            v
        })
        .collect();
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, true),
        ArrowField::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(ArrowField::new("item", DataType::Float32, true)),
                VEC_DIM,
            ),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(64..96)),
            fsl(far_vecs, VEC_DIM),
        ],
    )
    .unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();
    dataset
        .optimize_indices(&OptimizeOptions::merge(10))
        .await
        .unwrap();

    // Still masked: id=35's stale vector stays dropped and id=40 is still found via re-scoring.
    let after = vector_query_ids(&dataset, 3, false).await;
    assert!(
        !after.contains(&35),
        "stale index vector for id=35 resurfaced after optimize: {after:?}"
    );
    assert!(
        after.contains(&40),
        "overlaid vector for id=40 dropped after optimize: {after:?}"
    );
}
