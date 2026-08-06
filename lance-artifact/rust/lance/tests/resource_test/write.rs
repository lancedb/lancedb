// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use super::utils::AllocTracker;
use all_asserts::assert_le;
use arrow_schema::DataType;
use lance::dataset::InsertBuilder;
use lance_datafusion::datagen::DatafusionDatagenExt;
use lance_datagen::{BatchCount, ByteCount, RoundingBehavior, array, gen_batch};

#[tokio::test]
async fn test_insert_memory() {
    // Create a stream of 100MB of data, in batches
    let batch_size = 10 * 1024 * 1024; // 10MB
    let num_batches = BatchCount::from(10);
    let data = gen_batch()
        .col("a", array::rand_type(&DataType::Int32))
        .into_df_stream_bytes(
            ByteCount::from(batch_size),
            num_batches,
            RoundingBehavior::RoundDown,
        )
        .unwrap();

    let alloc_tracker = AllocTracker::new();
    {
        let _guard = alloc_tracker.enter();

        // write out to temporary directory
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let _dataset = InsertBuilder::new(tmp_path)
            .execute_stream(data)
            .await
            .unwrap();
    }

    let stats = alloc_tracker.stats();
    // Allow for 2x the batch size to account for overheads.
    // The key test is that we don't load all 100MB into memory at once
    assert_le!(
        stats.max_bytes_allocated,
        (batch_size * 2) as isize,
        "Max memory usage exceeded"
    );
}
