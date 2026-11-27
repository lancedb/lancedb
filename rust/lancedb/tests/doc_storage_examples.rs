// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
};

fn wide_rows(ids: Range<i64>) -> anyhow::Result<Box<dyn RecordBatchReader + Send>> {
    let payload = [b'x'; 1016];
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(ids.clone())) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(BinaryArray::from_iter_values(
                ids.map(|_| payload.as_slice()),
            )) as ArrayRef,
        ),
    ])?;
    let schema = batch.schema();
    Ok(Box::new(RecordBatchIterator::new(
        std::iter::once(Ok(batch)),
        schema,
    )))
}

async fn tune_compaction(table: &lancedb::Table) -> lancedb::Result<()> {
    // --8<-- [start:storage_compaction_tuning]
    use lancedb::table::{CompactionOptions, OptimizeAction};

    let desired_fragment_bytes: usize = 64 * 1024;
    let average_row_bytes: usize = 1024; // Estimate from representative rows.
    let options = CompactionOptions {
        target_rows_per_fragment: (desired_fragment_bytes / average_row_bytes).max(1),
        batch_size: Some(16),
        num_threads: Some(1),
        ..Default::default()
    };
    table
        .optimize(OptimizeAction::Compact {
            options,
            remap_options: None,
        })
        .await?;
    // --8<-- [end:storage_compaction_tuning]
    Ok(())
}

#[tokio::test]
async fn compaction_tuning() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let db = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await?;
    let table = db
        .create_table("compaction_tuning", wide_rows(0..32)?)
        .execute()
        .await?;
    table.add(wide_rows(32..64)?).execute().await?;
    assert_eq!(table.stats().await?.fragment_stats.num_fragments, 2);

    tune_compaction(&table).await?;
    assert_eq!(table.stats().await?.fragment_stats.num_fragments, 1);
    assert_eq!(table.count_rows(None).await?, 64);

    table.add(wide_rows(64..65)?).execute().await?;
    tune_compaction(&table).await?;
    assert_eq!(table.stats().await?.fragment_stats.num_fragments, 2);
    assert_eq!(table.count_rows(None).await?, 65);
    Ok(())
}
