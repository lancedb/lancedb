// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Table optimization operations for compaction, pruning, and index optimization.
//!
//! This module contains the implementation of optimization operations that help
//! maintain good performance for LanceDB tables.

use std::{collections::HashSet, sync::Arc};

use arrow_schema::DataType;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{
    CompactionMetrics, IndexRemapperOptions, compact_files, plan_compaction,
};
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;
use log::info;
use serde::Deserialize;

use super::NativeTable;
use crate::error::{Error, Result};
pub use chrono::Duration;
pub use lance::dataset::optimize::CompactionOptions;

const MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX: u64 = u32::MAX as u64;

#[derive(Debug, Deserialize)]
struct VectorIndexStatistics {
    indices: Vec<VectorIndexSegmentStatistics>,
}

#[derive(Debug, Deserialize)]
struct VectorIndexSegmentStatistics {
    partitions: Vec<VectorIndexPartitionStatistics>,
}

#[derive(Debug, Deserialize)]
struct VectorIndexPartitionStatistics {
    size: u64,
}

fn oversized_sq_partition(statistics: &str, dimension: u64) -> Result<Option<u64>> {
    let statistics: VectorIndexStatistics =
        serde_json::from_str(statistics).map_err(|source| Error::InvalidInput {
            message: format!("Could not parse vector index statistics: {source}"),
        })?;

    Ok(statistics
        .indices
        .iter()
        .flat_map(|index| &index.partitions)
        .map(|partition| partition.size)
        .find(|partition_size| {
            partition_size
                .checked_mul(dimension)
                .is_none_or(|child_len| child_len > MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX)
        }))
}

async fn validate_sq_index_remapping(
    dataset: &lance::Dataset,
    options: &CompactionOptions,
    has_custom_remapper: bool,
) -> Result<()> {
    if options.defer_index_remap || has_custom_remapper || dataset.manifest().uses_stable_row_ids()
    {
        return Ok(());
    }

    let plan = plan_compaction(dataset, options).await?;
    if plan.tasks.is_empty() {
        return Ok(());
    }
    let affected_fragments: HashSet<u64> = plan
        .tasks
        .iter()
        .flat_map(|task| task.fragments.iter().map(|fragment| fragment.id))
        .collect();

    for index in dataset.describe_indices(None).await? {
        if !matches!(index.index_type(), "IVF_SQ" | "IVF_HNSW_SQ")
            || !index.segments().iter().any(|segment| {
                segment.fragment_bitmap.as_ref().is_none_or(|bitmap| {
                    bitmap
                        .iter()
                        .any(|fragment| affected_fragments.contains(&(fragment as u64)))
                })
            })
        {
            continue;
        }

        let Some(field_id) = index.field_ids().first() else {
            continue;
        };
        let Some(field) = dataset.schema().field_by_id(*field_id as i32) else {
            continue;
        };
        let DataType::FixedSizeList(_, dimension) = field.data_type() else {
            continue;
        };
        let dimension = u64::try_from(dimension).map_err(|_| Error::InvalidInput {
            message: format!(
                "SQ index '{}' has an invalid vector dimension of {}",
                index.name(),
                dimension
            ),
        })?;

        // If the entire index fits then every individual partition does too,
        // avoiding the index-file read needed for detailed statistics.
        if index
            .rows_indexed()
            .checked_mul(dimension)
            .is_some_and(|child_len| child_len <= MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX)
        {
            continue;
        }

        let statistics = dataset.index_statistics(index.name()).await?;
        if let Some(partition_size) = oversized_sq_partition(&statistics, dimension)? {
            let max_partition_size = MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX / dimension;
            return Err(Error::InvalidInput {
                message: format!(
                    "Cannot compact table because SQ index '{}' has a partition with {} vectors of dimension {}. Arrow's fixed-size-list take kernel cannot remap partitions whose child array exceeds {} values. Recreate the index with default IVF partitioning, or enough partitions to keep each partition at or below {} vectors, before compacting.",
                    index.name(),
                    partition_size,
                    dimension,
                    MAX_ARROW_FIXED_SIZE_LIST_CHILD_INDEX,
                    max_partition_size,
                ),
            });
        }
    }

    Ok(())
}

/// Optimize the dataset.
///
/// Similar to `VACUUM` in PostgreSQL, it offers different options to
/// optimize different parts of the table on disk.
///
/// By default, it optimizes everything, as [`OptimizeAction::All`].
#[derive(Default)]
pub enum OptimizeAction {
    /// Run all optimizations with default values
    #[default]
    All,
    /// Compacts files in the dataset
    ///
    /// LanceDb uses a readonly filesystem for performance and safe concurrency.  Every time
    /// new data is added it will be added into new files.  Small files
    /// can hurt both read and write performance.  Compaction will merge small files
    /// into larger ones.
    ///
    /// All operations that modify data (add, delete, update, merge insert, etc.) will create
    /// new files.  If these operations are run frequently then compaction should run frequently.
    ///
    /// If these operations are never run (search only) then compaction is not necessary.
    Compact {
        options: CompactionOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    },
    /// Prune old version of datasets
    ///
    /// Every change in LanceDb is additive.  When data is removed from a dataset a new version is
    /// created that doesn't contain the removed data.  However, the old version, which does contain
    /// the removed data, is left in place.  This is necessary for consistency and concurrency and
    /// also enables time travel functionality like the ability to checkout an older version of the
    /// dataset to undo changes.
    ///
    /// Over time, these old versions can consume a lot of disk space.  The prune operation will
    /// remove versions of the dataset that are older than a certain age.  This will free up the
    /// space used by that old data.
    ///
    /// Once a version is pruned it can no longer be checked out.
    Prune {
        /// The duration of time to keep versions of the dataset.
        older_than: Option<Duration>,
        /// Because they may be part of an in-progress transaction, files newer than 7 days old are not deleted by default.
        /// If you are sure that there are no in-progress transactions, then you can set this to True to delete all files older than `older_than`.
        ///
        /// **WARNING**: This should only be set to true if you can guarantee that no other process is
        /// currently working on this dataset.  Otherwise the dataset could be put into a corrupted state.
        delete_unverified: Option<bool>,
        /// If true, an error will be returned if there are any old versions that are still tagged.
        error_if_tagged_old_versions: Option<bool>,
    },
    /// Optimize the indices
    ///
    /// This operation optimizes all indices in the table.  When new data is added to LanceDb
    /// it is not added to the indices.  However, it can still turn up in searches because the search
    /// function will scan both the indexed data and the unindexed data in parallel.  Over time, the
    /// unindexed data can become large enough that the search performance is slow.  This operation
    /// will add the unindexed data to the indices without rerunning the full index creation process.
    ///
    /// Optimizing an index is faster than re-training the index but it does not typically adjust the
    /// underlying model relied upon by the index.  This can eventually lead to poor search accuracy
    /// and so users may still want to occasionally retrain the index after adding a large amount of
    /// data.
    ///
    /// For example, when using IVF, an index will create clusters.  Optimizing an index assigns unindexed
    /// data to the existing clusters, but it does not move the clusters or create new clusters.
    Index(OptimizeOptions),
}

/// Statistics about the optimization.
#[derive(Debug, Default)]
pub struct OptimizeStats {
    /// Stats of the file compaction.
    pub compaction: Option<CompactionMetrics>,

    /// Stats of the version pruning
    pub prune: Option<RemovalStats>,
}

/// Internal implementation of optimize_indices
///
/// This logic was moved from NativeTable to keep table.rs clean.
pub(crate) async fn optimize_indices(table: &NativeTable, options: &OptimizeOptions) -> Result<()> {
    info!("LanceDB: optimizing indices: {:?}", options);
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.optimize_indices(options).await?;
    table.dataset.update(dataset);
    Ok(())
}

/// Remove old versions of the dataset from disk.
///
/// # Arguments
/// * `older_than` - The duration of time to keep versions of the dataset.
/// * `delete_unverified` - Because they may be part of an in-progress
///   transaction, files newer than 7 days old are not deleted by default.
///   If you are sure that there are no in-progress transactions, then you
///   can set this to True to delete all files older than `older_than`.
///
///   **WARNING**: This should only be set to true if you can guarantee that
///   no other process is currently working on this dataset.  Otherwise the
///   dataset could be put into a corrupted state.
///
/// This calls into [lance::dataset::Dataset::cleanup_old_versions] and
/// returns the result.
pub(crate) async fn cleanup_old_versions(
    table: &NativeTable,
    older_than: Duration,
    delete_unverified: Option<bool>,
    error_if_tagged_old_versions: Option<bool>,
) -> Result<RemovalStats> {
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;
    Ok(dataset
        .cleanup_old_versions(older_than, delete_unverified, error_if_tagged_old_versions)
        .await?)
}

/// Compact files in the dataset.
///
/// This can be run after making several small appends to optimize the table
/// for faster reads.
///
/// This calls into [lance::dataset::optimize::compact_files].
pub(crate) async fn compact_files_impl(
    table: &NativeTable,
    options: CompactionOptions,
    remap_options: Option<Arc<dyn IndexRemapperOptions>>,
) -> Result<CompactionMetrics> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    validate_sq_index_remapping(&dataset, &options, remap_options.is_some()).await?;
    let metrics = compact_files(&mut dataset, options, remap_options).await?;
    table.dataset.update(dataset);
    Ok(metrics)
}

/// Execute the optimize operation on the table.
///
/// This is the main entry point for all optimization operations.
pub(crate) async fn execute_optimize(
    table: &NativeTable,
    action: OptimizeAction,
) -> Result<OptimizeStats> {
    let mut stats = OptimizeStats {
        compaction: None,
        prune: None,
    };
    match action {
        OptimizeAction::All => {
            // Call helper functions directly to avoid async recursion issues
            stats.compaction =
                Some(compact_files_impl(table, CompactionOptions::default(), None).await?);
            stats.prune = Some(
                cleanup_old_versions(
                    table,
                    Duration::try_days(7).expect("valid delta"),
                    None,
                    None,
                )
                .await?,
            );
            optimize_indices(table, &OptimizeOptions::default()).await?;
        }
        OptimizeAction::Compact {
            options,
            remap_options,
        } => {
            stats.compaction = Some(compact_files_impl(table, options, remap_options).await?);
        }
        OptimizeAction::Prune {
            older_than,
            delete_unverified,
            error_if_tagged_old_versions,
        } => {
            stats.prune = Some(
                cleanup_old_versions(
                    table,
                    older_than.unwrap_or(Duration::try_days(7).expect("valid delta")),
                    delete_unverified,
                    error_if_tagged_old_versions,
                )
                .await?,
            );
        }
        OptimizeAction::Index(options) => {
            optimize_indices(table, &options).await?;
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::oversized_sq_partition;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rstest::rstest;
    use std::sync::Arc;

    use crate::connect;
    use crate::index::{Index, scalar::BTreeIndexBuilder};
    use crate::query::ExecutableQuery;
    use crate::table::{CompactionOptions, OptimizeAction, OptimizeStats};
    use futures::TryStreamExt;

    /// Regression test for https://github.com/lancedb/lancedb/issues/2866.
    #[test]
    fn test_detect_oversized_sq_partition() {
        const DIMENSION: u64 = 4095;
        let safe_size = u32::MAX as u64 / DIMENSION;
        let statistics = serde_json::json!({
            "indices": [{
                "partitions": [
                    { "size": safe_size },
                    { "size": safe_size + 1 }
                ]
            }]
        });

        assert_eq!(
            oversized_sq_partition(&statistics.to_string(), DIMENSION).unwrap(),
            Some(safe_size + 1)
        );

        let split_statistics = serde_json::json!({
            "indices": [{
                "partitions": [
                    { "size": safe_size / 2 },
                    { "size": safe_size / 2 + 1 }
                ]
            }]
        });
        assert_eq!(
            oversized_sq_partition(&split_statistics.to_string(), DIMENSION).unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_optimize_compact_simple() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with initial data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_compact", batch)
            .execute()
            .await
            .unwrap();

        // Add more data to create multiple fragments
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 100 + 100)..((i + 1) * 100 + 100),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Verify we have multiple fragments before compaction
        let initial_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(initial_row_count, 600);

        // Run compaction
        let stats = table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions {
                    target_rows_per_fragment: 1000,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        // Verify compaction occurred
        assert!(stats.compaction.is_some());
        let compaction_metrics = stats.compaction.unwrap();
        assert!(compaction_metrics.fragments_removed > 0);

        // Verify data integrity after compaction
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 600);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 600);

        // Verify the values are as expected
        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..600).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_optimize_prune_versions() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_prune", batch)
            .execute()
            .await
            .unwrap();

        // Make several modifications to create versions
        for i in 0..5 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 10 + 10)..((i + 1) * 10 + 10),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Verify multiple versions exist
        let versions = table.list_versions().await.unwrap();
        assert!(versions.len() > 1);

        // Run prune with a very old cutoff (won't delete recent versions)
        let stats = table
            .optimize(OptimizeAction::Prune {
                older_than: Some(chrono::Duration::try_days(0).unwrap()),
                delete_unverified: Some(true),
                error_if_tagged_old_versions: None,
            })
            .await
            .unwrap();

        // Prune-only operation should not have compaction stats
        assert!(stats.compaction.is_none());

        // Verify prune stats
        let prune_stats = stats.prune.unwrap();
        assert!(prune_stats.bytes_removed > 0);
        assert_eq!(prune_stats.old_versions, 5);

        // Verify data is still intact
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 60);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..60).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_optimize_index() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_index_optimize", batch)
            .execute()
            .await
            .unwrap();

        // Create an index
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        // Add more data (unindexed)
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(100..200))],
        )
        .unwrap();
        table.add(batch).execute().await.unwrap();

        // Verify index stats before optimization
        let indices = table.list_indices().await.unwrap();
        assert_eq!(indices.len(), 1);
        let index_name = indices[0].name.clone();
        let stats_before = table.index_stats(&index_name).await.unwrap().unwrap();
        assert_eq!(stats_before.num_indexed_rows, 100);
        assert_eq!(stats_before.num_unindexed_rows, 100);

        // Run index optimization
        let stats = table
            .optimize(OptimizeAction::Index(Default::default()))
            .await
            .unwrap();

        // For index optimization, compaction and prune stats should be None
        assert!(stats.compaction.is_none());
        assert!(stats.prune.is_none());

        // Verify index stats after optimization
        let stats_after = table.index_stats(&index_name).await.unwrap().unwrap();
        assert_eq!(stats_after.num_indexed_rows, 200);
        assert_eq!(stats_after.num_unindexed_rows, 0);
        assert!(stats_after.num_indices.is_some());

        // Verify data integrity
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 200);
    }

    #[tokio::test]
    async fn test_optimize_all() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_optimize_all", batch)
            .execute()
            .await
            .unwrap();

        // Add more data
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    (i * 100 + 100)..((i + 1) * 100 + 100),
                ))],
            )
            .unwrap();
            table.add(batch).execute().await.unwrap();
        }

        // Run all optimizations
        let stats = table.optimize(OptimizeAction::All).await.unwrap();

        // Verify stats from both compaction and prune
        assert!(stats.compaction.is_some());
        assert!(stats.prune.is_some());

        // Verify data integrity
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 400);

        // Verify data content
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["i"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();
        let expected: Vec<i32> = (0..400).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_optimize_default_action() {
        // Verify that default action is All
        let action: OptimizeAction = Default::default();
        assert!(matches!(action, OptimizeAction::All));
    }

    #[tokio::test]
    async fn test_optimize_stats_default() {
        // Verify OptimizeStats default values
        let stats: OptimizeStats = Default::default();
        assert!(stats.compaction.is_none());
        assert!(stats.prune.is_none());
    }

    #[tokio::test]
    async fn test_compact_with_deferred_index_remap() {
        // Smoke test: verifies compaction with deferred index remap doesn't error.
        // We don't currently assert that remap is actually deferred.
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with data
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..100))],
        )
        .unwrap();

        let table = conn
            .create_table("test_deferred_remap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Add more data
        table.add(batch).execute().await.unwrap();

        // Create an index
        table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        // Run compaction with deferred index remap
        let stats = table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions {
                    target_rows_per_fragment: 2000,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        assert!(stats.compaction.is_some());

        // Verify data integrity after compaction
        let final_row_count = table.count_rows(None).await.unwrap();
        assert_eq!(final_row_count, 200);

        // Verify data content is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut all_values: Vec<i32> = Vec::new();
        for batch in &batches {
            let array = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            all_values.extend(array.values().iter().copied());
        }
        all_values.sort();

        // Since we added the same data twice (0..100 twice), we expect 200 values
        // with values 0-99 appearing twice
        let mut expected: Vec<i32> = (0..100).chain(0..100).collect();
        expected.sort();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_compaction_preserves_schema() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table with multiple columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(StringArray::from(
                    (0..10).map(|i| format!("name_{}", i)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let original_schema = batch.schema();

        let table = conn
            .create_table("test_schema_preserved", batch.clone())
            .execute()
            .await
            .unwrap();

        // Add more data
        table.add(batch).execute().await.unwrap();

        // Run compaction
        table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();

        // Verify schema is preserved
        let current_schema = table.schema().await.unwrap();
        assert_eq!(current_schema, original_schema);

        // Verify data is intact and correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 20);
    }

    #[tokio::test]
    async fn test_optimize_empty_table() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a table and delete all data
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_empty_optimize", batch)
            .execute()
            .await
            .unwrap();

        // Delete all rows
        table.delete("true").await.unwrap();

        // Verify table is empty
        assert_eq!(table.count_rows(None).await.unwrap(), 0);

        // Optimize should work on empty table
        let stats = table.optimize(OptimizeAction::All).await.unwrap();
        assert!(stats.compaction.is_some());
        assert!(stats.prune.is_some());

        // Verify table is still empty but schema is preserved
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
        let current_schema = table.schema().await.unwrap();
        assert_eq!(current_schema, schema);
    }

    #[rstest]
    #[case::all(OptimizeAction::All)]
    #[case::compact(OptimizeAction::Compact {
        options: CompactionOptions::default(),
        remap_options: None,
    })]
    #[case::prune(OptimizeAction::Prune {
        older_than: Some(chrono::Duration::try_days(0).unwrap()),
        delete_unverified: Some(true),
        error_if_tagged_old_versions: None,
    })]
    #[case::index(OptimizeAction::Index(Default::default()))]
    #[tokio::test]
    async fn test_optimize_fails_on_checked_out_table(#[case] action: OptimizeAction) {
        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();

        let table = conn
            .create_table("test_checkout_optimize", batch.clone())
            .execute()
            .await
            .unwrap();

        table.add(batch).execute().await.unwrap();

        table.checkout(1).await.unwrap();

        let result = table.optimize(action).await;
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot be modified when a specific version is checked out"),
            "Expected error message about checked out table, got: {}",
            err_msg
        );
    }
}
