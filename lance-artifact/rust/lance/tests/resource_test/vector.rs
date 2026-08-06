// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use super::utils::AllocTracker;
use all_asserts::assert_le;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, types::Float32Type};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::index::vector::utils::maybe_sample_training_data;
use lance_arrow::FixedSizeListArrayExt;

#[tokio::test]
async fn test_nullable_fragment_sampling_memory_stays_bounded() {
    let dim = 1024;
    let num_fragments = 4;
    let rows_per_fragment = 8_192;
    let sample_size = 32;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vec",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
        true,
    )]));

    let batches = (0..num_fragments)
        .map(|seed| {
            let values = lance_testing::datagen::generate_random_array_with_seed::<Float32Type>(
                rows_per_fragment * dim as usize,
                [seed as u8; 32],
            );
            let vectors = Arc::new(
                arrow_array::FixedSizeListArray::try_new_from_values(values, dim).unwrap(),
            ) as ArrayRef;
            RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap()
        })
        .collect::<Vec<_>>();

    let tmp_dir = tempfile::tempdir().unwrap();
    let uri = tmp_dir.path().to_str().unwrap();
    let dataset = Dataset::write(
        RecordBatchIterator::new(batches.into_iter().map(Ok), schema),
        uri,
        Some(WriteParams {
            max_rows_per_file: rows_per_fragment,
            max_rows_per_group: rows_per_fragment,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let fragment_ids = dataset
        .get_fragments()
        .into_iter()
        .take(2)
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();

    let alloc_tracker = AllocTracker::new();
    let training_data = {
        let _guard = alloc_tracker.enter();
        maybe_sample_training_data(&dataset, "vec", sample_size, Some(&fragment_ids))
            .await
            .unwrap()
    };
    let stats = alloc_tracker.stats();

    assert_eq!(training_data.len(), sample_size);

    // A full scan of the selected fragments would need at least:
    // 2 fragments * 8192 rows * 1024 dims * 4 bytes = 64 MiB
    // Keep a generous ceiling well below that lower bound so the test remains
    // stable while still catching regressions back to eager materialization.
    assert_le!(
        stats.max_bytes_allocated,
        24 * 1024 * 1024,
        "nullable fragment sampling allocated too much memory: {:?}",
        stats
    );
}
