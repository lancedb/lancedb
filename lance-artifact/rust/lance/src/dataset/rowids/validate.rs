// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Integrity checks for the stable row id invariants that the row id index and the write
//! paths rely on. Reached through [`Dataset::validate`].

use super::load_row_id_sequence;
use crate::dataset::Dataset;
use crate::dataset::fragment::FileFragment;
use crate::{Error, Result};
use futures::{StreamExt, TryStreamExt};
use lance_core::utils::deletion::DeletionVector;
use lance_table::format::RowDatasetVersionMeta;
use lance_table::rowids::RowIdSequence;
use roaring::RoaringTreemap;
use std::sync::Arc;

/// The per-fragment state a stable row id invariant is checked against.
type FragmentRowIds = (
    FileFragment,
    Arc<RowIdSequence>,
    Option<Arc<DeletionVector>>,
);

/// Check the invariants that the stable row id machinery relies on.
///
/// A no-op on datasets that don't use stable row ids. See [`Dataset::validate`].
pub async fn validate_stable_row_ids(dataset: &Dataset) -> Result<()> {
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(());
    }

    let corrupt = |message: String| Error::corrupt_file(dataset.base.clone(), message);

    for fragment in dataset.manifest.fragments.iter() {
        if fragment.row_id_meta.is_none() {
            return Err(corrupt(format!(
                "Fragment {} has no row id metadata, but dataset {:?} uses stable row ids",
                fragment.id, dataset.base
            )));
        }
    }

    // `buffered` preserves manifest order, which the uniqueness check below relies on to
    // report the earlier of the two fragments holding a duplicate id.
    let fragments: Vec<FragmentRowIds> = futures::stream::iter(dataset.get_fragments())
        .map(|fragment| async move {
            let sequence = load_row_id_sequence(dataset, fragment.metadata()).await?;
            let deletion_vector = fragment.get_deletion_vector().await?;
            Result::Ok((fragment, sequence, deletion_vector))
        })
        .buffered(dataset.object_store.io_parallelism())
        .try_collect()
        .await?;

    let mut live_row_ids = RoaringTreemap::new();
    let mut max_row_id: Option<u64> = None;

    for (index, (fragment, sequence, deletion_vector)) in fragments.iter().enumerate() {
        let metadata = fragment.metadata();
        let physical_rows = metadata.physical_rows.ok_or_else(|| {
            corrupt(format!(
                "Fragment {} has an unknown physical row count, but dataset {:?} uses stable row ids",
                metadata.id, dataset.base
            ))
        })? as u64;

        if sequence.len() != physical_rows {
            return Err(corrupt(format!(
                "Fragment {} has {} row ids, but {} physical rows, in dataset {:?}",
                metadata.id,
                sequence.len(),
                physical_rows,
                dataset.base
            )));
        }

        for (name, meta) in [
            ("created_at", &metadata.created_at_version_meta),
            ("last_updated_at", &metadata.last_updated_at_version_meta),
        ] {
            // Only inline version metadata can be read back; nothing writes the
            // external form yet.
            if let Some(meta @ RowDatasetVersionMeta::Inline(_)) = meta {
                let versions = meta.load_sequence()?.len();
                if versions != physical_rows {
                    return Err(corrupt(format!(
                        "Fragment {} has {} {} versions, but {} physical rows, in dataset {:?}",
                        metadata.id, versions, name, physical_rows, dataset.base
                    )));
                }
            }
        }

        // The bounding range covers tombstoned slots too: their ids are retired and must
        // not be handed out again either.
        if let Some(range) = sequence.row_id_range() {
            max_row_id = max_row_id.max(Some(*range.end()));
        }

        for (offset, row_id) in sequence.iter().enumerate() {
            // An update rewrites a row into a new fragment under the same id and tombstones
            // the original slot, so only live ids are required to be unique.
            if deletion_vector
                .as_ref()
                .is_some_and(|deletions| deletions.contains(offset as u32))
            {
                continue;
            }
            if !live_row_ids.insert(row_id) {
                return Err(corrupt(format!(
                    "Row id {} is live in both {} and fragment {} at offset {} in dataset {:?}",
                    row_id,
                    describe_first_live_slot(&fragments[..=index], row_id),
                    metadata.id,
                    offset,
                    dataset.base
                )));
            }
        }
    }

    if let Some(max_row_id) = max_row_id
        && dataset.manifest.next_row_id <= max_row_id
    {
        return Err(corrupt(format!(
            "Dataset {:?} will hand out row id {} next, but row id {} is already in use",
            dataset.base, dataset.manifest.next_row_id, max_row_id
        )));
    }

    Ok(())
}

/// Describe where `row_id` is first live, so a uniqueness failure can name both of the
/// slots involved. Only runs on the failure path, hence the linear rescan.
fn describe_first_live_slot(fragments: &[FragmentRowIds], row_id: u64) -> String {
    for (fragment, sequence, deletion_vector) in fragments {
        // A bounding range that excludes the id rules the whole fragment out.
        if sequence
            .row_id_range()
            .is_none_or(|range| !range.contains(&row_id))
        {
            continue;
        }
        for (offset, candidate) in sequence.iter().enumerate() {
            if candidate == row_id
                && !deletion_vector
                    .as_ref()
                    .is_some_and(|deletions| deletions.contains(offset as u32))
            {
                return format!("fragment {} at offset {}", fragment.id(), offset);
            }
        }
    }
    "an earlier fragment".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shared with the row id tests next door, which cover the same operations.
    use super::super::test::{compact, delete};

    use crate::dataset::builder::DatasetBuilder;
    use crate::dataset::{
        MergeInsertBuilder, UpdateBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams,
    };
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use arrow_array::RecordBatchIterator;
    use arrow_array::types::Int32Type;
    use arrow_schema::Schema as ArrowSchema;
    use lance_table::format::{Fragment, RowIdMeta, pb};
    use lance_table::rowids::version::RowDatasetVersionSequence;
    use lance_table::rowids::write_row_ids;
    use prost::Message;
    use rstest::rstest;

    fn encode_segments(segments: Vec<pb::u64_segment::Segment>) -> Vec<u8> {
        pb::RowIdSequence {
            segments: segments
                .into_iter()
                .map(|segment| pb::U64Segment {
                    segment: Some(segment),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    fn range_segment(start: u64, end: u64) -> pb::u64_segment::Segment {
        pb::u64_segment::Segment::Range(pb::u64_segment::Range { start, end })
    }

    fn empty_encoded_array() -> pb::EncodedU64Array {
        pb::EncodedU64Array {
            array: Some(pb::encoded_u64_array::Array::U64Array(
                pb::encoded_u64_array::U64Array { values: Vec::new() },
            )),
        }
    }

    fn empty_array_segment() -> pb::u64_segment::Segment {
        pb::u64_segment::Segment::Array(empty_encoded_array())
    }

    fn empty_sorted_array_segment() -> pb::u64_segment::Segment {
        pb::u64_segment::Segment::SortedArray(empty_encoded_array())
    }

    fn empty_range_with_holes_segment() -> pb::u64_segment::Segment {
        pb::u64_segment::Segment::RangeWithHoles(pb::u64_segment::RangeWithHoles {
            start: 0,
            end: 0,
            holes: Some(empty_encoded_array()),
        })
    }

    fn empty_range_with_bitmap_segment() -> pb::u64_segment::Segment {
        pb::u64_segment::Segment::RangeWithBitmap(pb::u64_segment::RangeWithBitmap {
            start: 0,
            end: 0,
            bitmap: Vec::new(),
        })
    }

    /// Two `Range` segments that each fit a `usize` but whose lengths sum past `u64`.
    /// Decoding accepts them, so `validate()` must report corruption rather than
    /// overflowing while measuring the sequence.
    #[tokio::test]
    async fn test_validate_rejects_row_id_count_overflow() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        let encoded = encode_segments(vec![range_segment(0, u64::MAX), range_segment(0, u64::MAX)]);
        edit_fragments(&mut dataset, |fragments| {
            fragments[1].row_id_meta = Some(RowIdMeta::Inline(encoded.into()));
        });

        assert_invalid(&dataset, "total length exceeding u64::MAX").await;
    }

    /// An empty segment carries no minimum or maximum, and decoding accepts an empty
    /// encoding of every variant. Neither the cardinality check nor the `next_row_id`
    /// bound may unwind on one.
    #[rstest]
    #[case::array(empty_array_segment())]
    #[case::sorted_array(empty_sorted_array_segment())]
    #[case::range_with_holes(empty_range_with_holes_segment())]
    #[case::range_with_bitmap(empty_range_with_bitmap_segment())]
    #[tokio::test]
    async fn test_validate_tolerates_empty_segment(#[case] empty: pb::u64_segment::Segment) {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        // Ten ids across two segments matches the fragment's ten physical rows, and the
        // ids stay clear of fragment 0's, so this dataset is well-formed.
        let encoded = encode_segments(vec![range_segment(10, 20), empty]);
        edit_fragments(&mut dataset, |fragments| {
            fragments[1].row_id_meta = Some(RowIdMeta::Inline(encoded.into()));
        });

        dataset.validate().await.unwrap();
    }

    /// Write a two-fragment dataset with stable row ids to `uri`, then reopen it with a
    /// fresh session so the row id sequence cache is empty. Tests that doctor a
    /// fragment's `row_id_meta` need that: the cache is keyed by fragment id alone, so a
    /// sequence loaded before the mutation would shadow the doctored one.
    async fn validation_fixture(uri: &str) -> Dataset {
        lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_dataset_with_params(
                uri,
                FragmentCount::from(2),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        DatasetBuilder::from_uri(uri).load().await.unwrap()
    }

    fn edit_fragments(dataset: &mut Dataset, edit: impl FnOnce(&mut Vec<Fragment>)) {
        let mut manifest = dataset.manifest.as_ref().clone();
        let mut fragments = manifest.fragments.as_ref().clone();
        edit(&mut fragments);
        manifest.fragments = Arc::new(fragments);
        dataset.manifest = Arc::new(manifest);
    }

    async fn assert_invalid(dataset: &Dataset, expected_message: &str) {
        let err = dataset.validate().await.unwrap_err();
        assert!(
            matches!(err, Error::CorruptFile { .. }),
            "expected a corrupt file error, got {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(expected_message),
            "expected {expected_message:?} in {message:?}"
        );
    }

    #[tokio::test]
    async fn test_validate_rejects_missing_row_id_meta() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        edit_fragments(&mut dataset, |fragments| fragments[1].row_id_meta = None);

        assert_invalid(&dataset, "Fragment 1 has no row id metadata").await;
    }

    #[tokio::test]
    async fn test_validate_rejects_row_id_sequence_length_mismatch() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        edit_fragments(&mut dataset, |fragments| {
            let short = RowIdSequence::from(&[100u64, 101, 102][..]);
            fragments[1].row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&short).into()));
        });

        assert_invalid(&dataset, "Fragment 1 has 3 row ids, but 10 physical rows").await;
    }

    #[tokio::test]
    async fn test_validate_rejects_duplicate_live_row_ids() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        edit_fragments(&mut dataset, |fragments| {
            fragments[1].row_id_meta = fragments[0].row_id_meta.clone();
        });

        assert_invalid(
            &dataset,
            "Row id 0 is live in both fragment 0 at offset 0 and fragment 1 at offset 0",
        )
        .await;
    }

    #[tokio::test]
    async fn test_validate_rejects_reused_next_row_id() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        let mut manifest = dataset.manifest.as_ref().clone();
        manifest.next_row_id = 19;
        dataset.manifest = Arc::new(manifest);

        assert_invalid(
            &dataset,
            "will hand out row id 19 next, but row id 19 is already in use",
        )
        .await;
    }

    #[tokio::test]
    async fn test_validate_rejects_misaligned_version_sequence() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let mut dataset = validation_fixture(&temp_dir).await;
        edit_fragments(&mut dataset, |fragments| {
            let too_long = RowDatasetVersionSequence::from_uniform_row_count(11, 1);
            fragments[1].created_at_version_meta =
                Some(RowDatasetVersionMeta::from_sequence(&too_long).unwrap());
        });

        assert_invalid(
            &dataset,
            "Fragment 1 has 11 created_at versions, but 10 physical rows",
        )
        .await;
    }

    /// Number of sequence entries whose row id already appeared in an earlier fragment,
    /// tombstoned slots included.
    async fn count_repeated_row_ids(dataset: &Dataset) -> usize {
        let mut seen = RoaringTreemap::new();
        let mut repeated = 0;
        for fragment in dataset.get_fragments() {
            let sequence = load_row_id_sequence(dataset, fragment.metadata())
                .await
                .unwrap();
            repeated += sequence.iter().filter(|id| !seen.insert(*id)).count();
        }
        repeated
    }

    /// A tombstoned slot keeps its row id, so after an update the same id lives in the
    /// rewritten fragment and lingers, deleted, in the original. Only live ids must be
    /// unique — this asserts the checks agree with that.
    #[tokio::test]
    async fn test_validate_across_write_operations() {
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let dataset = validation_fixture(&temp_dir).await;
        dataset.validate().await.unwrap();

        let batch = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step_custom::<Int32Type>(20, 1))
            .into_batch_rows(lance_datagen::RowCount::from(10))
            .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::from(dataset.schema()));
        let reader = RecordBatchIterator::new(vec![Ok(batch)], arrow_schema);
        let mut dataset = Dataset::write(
            reader,
            &temp_dir,
            Some(WriteParams {
                mode: WriteMode::Append,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.validate().await.unwrap();

        delete(&mut dataset, "i = 4 or i = 12").await;
        dataset.validate().await.unwrap();

        let dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("i >= 15")
            .unwrap()
            .set("i", "i + 1000")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;
        dataset.validate().await.unwrap();
        assert!(
            count_repeated_row_ids(&dataset).await > 0,
            "update should leave rewritten row ids behind in tombstoned slots"
        );

        let merge_source = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step_custom::<Int32Type>(8, 1))
            .into_batch_rows(lance_datagen::RowCount::from(6))
            .unwrap();
        let schema = merge_source.schema();
        let merge_job = MergeInsertBuilder::try_new(dataset.clone(), vec!["i".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let source = lance_datafusion::utils::reader_to_stream(Box::new(RecordBatchIterator::new(
            [Ok(merge_source)],
            schema,
        )));
        let (dataset, _stats) = merge_job.execute(source).await.unwrap();
        dataset.validate().await.unwrap();

        let mut dataset = dataset.as_ref().clone();
        compact(&mut dataset, 20).await;
        dataset.validate().await.unwrap();
    }
}
