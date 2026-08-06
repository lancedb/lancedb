// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemWAL Index operations.
//!
//! The MemWAL Index stores:
//! - Configuration (sharding_specs, maintained_indexes)
//! - SSTable compaction progress
//! - Shard state snapshots (eventually consistent)
//!
//! Writers no longer update the index on every write. Instead, they update
//! shard manifests directly. This module provides functions to:
//! - Load the MemWAL index
//! - Update compacted SSTables (called during merge-insert commits)

use std::sync::Arc;

use lance_core::{Error, Result};
use lance_index::mem_wal::{CompactedSsTable, MEM_WAL_INDEX_NAME, MemWalIndex, MemWalIndexDetails};
use lance_table::format::{IndexMetadata, pb};
use uuid::Uuid;

/// Load MemWalIndexDetails from an IndexMetadata.
pub(crate) fn load_mem_wal_index_details(index: IndexMetadata) -> Result<MemWalIndexDetails> {
    if let Some(details_any) = index.index_details.as_ref() {
        if !details_any.type_url.ends_with("MemWalIndexDetails") {
            return Err(Error::index(format!(
                "Index details is not for the MemWAL index, but {}",
                details_any.type_url
            )));
        }

        Ok(MemWalIndexDetails::try_from(
            details_any.to_msg::<pb::MemWalIndexDetails>()?,
        )?)
    } else {
        Err(Error::index("Index details not found for the MemWAL index"))
    }
}

/// Open the MemWAL index from its metadata.
pub(crate) fn open_mem_wal_index(index: IndexMetadata) -> Result<Arc<MemWalIndex>> {
    Ok(Arc::new(MemWalIndex::new(load_mem_wal_index_details(
        index,
    )?)))
}

/// Update `compacted_sstables` in the MemWAL index.
///
/// This is called during merge-insert commits to atomically record which
/// SSTables have been compacted into the base table.
pub(crate) fn update_mem_wal_index_compacted_sstables(
    indices: &mut Vec<IndexMetadata>,
    dataset_version: u64,
    new_compacted_sstables: Vec<CompactedSsTable>,
) -> Result<()> {
    if new_compacted_sstables.is_empty() {
        return Ok(());
    }

    let pos = indices
        .iter()
        .position(|idx| idx.name == MEM_WAL_INDEX_NAME);

    let new_meta = if let Some(pos) = pos {
        let current_meta = indices.remove(pos);
        let mut details = load_mem_wal_index_details(current_meta)?;

        // Update compacted_sstables - for each shard, keep the higher generation
        for new_sstable in new_compacted_sstables {
            if let Some(existing) = details
                .compacted_sstables
                .iter_mut()
                .find(|sstable| sstable.shard_id == new_sstable.shard_id)
            {
                if new_sstable.generation > existing.generation {
                    existing.generation = new_sstable.generation;
                }
            } else {
                details.compacted_sstables.push(new_sstable);
            }
        }

        new_mem_wal_index_meta(dataset_version, details)?
    } else {
        // Create a MemWAL index containing only compaction progress.
        let details = MemWalIndexDetails {
            compacted_sstables: new_compacted_sstables,
            ..Default::default()
        };
        new_mem_wal_index_meta(dataset_version, details)?
    };

    indices.push(new_meta);
    Ok(())
}

/// Create a new MemWAL index metadata entry.
pub(crate) fn new_mem_wal_index_meta(
    dataset_version: u64,
    details: MemWalIndexDetails,
) -> Result<IndexMetadata> {
    Ok(IndexMetadata {
        uuid: Uuid::new_v4(),
        name: MEM_WAL_INDEX_NAME.to_string(),
        fields: vec![],
        dataset_version,
        fragment_bitmap: None,
        index_details: Some(Arc::new(prost_types::Any::from_msg(
            &pb::MemWalIndexDetails::from(&details),
        )?)),
        index_version: 0,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        // Memory WAL index is inline (no files)
        files: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::index::DatasetIndexExt;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use crate::dataset::transaction::{Operation, Transaction};
    use crate::dataset::{CommitBuilder, InsertBuilder, WriteParams};

    async fn test_dataset() -> crate::Dataset {
        let write_params = WriteParams {
            max_rows_per_file: 10,
            ..Default::default()
        };
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10_i32)),
                Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(0, 10))),
            ],
        )
        .unwrap();
        InsertBuilder::new("memory://test_mem_wal")
            .with_params(&write_params)
            .execute(vec![data])
            .await
            .unwrap()
    }

    /// Test that UpdateMemWalState with lower generation than committed fails without retry.
    /// Per spec: If committed_generation >= to_commit_generation, abort without retry.
    #[tokio::test]
    async fn test_update_mem_wal_state_conflict_lower_generation_no_retry() {
        let dataset = test_dataset().await;
        let shard = Uuid::new_v4();

        // First commit UpdateMemWalState with generation 10
        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // Try to commit UpdateMemWalState with generation 5 (lower than 10)
        // This should fail with non-retryable conflict
        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            matches!(result, Err(crate::Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction for lower generation, got {:?}",
            result
        );
    }

    /// Test that UpdateMemWalState with equal generation as committed fails without retry.
    #[tokio::test]
    async fn test_update_mem_wal_state_conflict_equal_generation_no_retry() {
        let dataset = test_dataset().await;
        let shard = Uuid::new_v4();

        // First commit UpdateMemWalState with generation 10
        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // Try to commit UpdateMemWalState with generation 10 (equal)
        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            matches!(result, Err(crate::Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction for equal generation, got {:?}",
            result
        );
    }

    /// Test that UpdateMemWalState with higher generation than committed is retryable.
    /// Per spec: If committed_generation < to_commit_generation, retry is allowed.
    #[tokio::test]
    async fn test_update_mem_wal_state_conflict_higher_generation_retryable() {
        let dataset = test_dataset().await;
        let shard = Uuid::new_v4();

        // First commit UpdateMemWalState with generation 5
        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // Try to commit UpdateMemWalState with generation 10 (higher than 5)
        // This should fail with retryable conflict
        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            matches!(result, Err(crate::Error::RetryableCommitConflict { .. })),
            "Expected retryable conflict for higher generation, got {:?}",
            result
        );
    }

    /// Test that UpdateMemWalState on different shards don't conflict.
    #[tokio::test]
    async fn test_update_mem_wal_state_different_shards_no_conflict() {
        let dataset = test_dataset().await;
        let shard1 = Uuid::new_v4();
        let shard2 = Uuid::new_v4();

        // First commit UpdateMemWalState for shard1
        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard1, 10)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // Commit UpdateMemWalState for shard2 based on old version
        // This should succeed because different shards don't conflict
        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard2, 5)],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            result.is_ok(),
            "Expected success for different shards, got {:?}",
            result
        );

        // Verify both shards are in the index
        let dataset = result.unwrap();
        let mem_wal_idx = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .unwrap()
            .clone();
        let details = load_mem_wal_index_details(mem_wal_idx).unwrap();
        assert_eq!(details.compacted_sstables.len(), 2);
    }

    /// Test that CreateIndex of MemWalIndex can be rebased against UpdateMemWalState.
    /// The compacted_sstables from UpdateMemWalState should be included in CreateIndex.
    #[tokio::test]
    async fn test_create_index_rebase_against_update_mem_wal_state() {
        let dataset = test_dataset().await;
        let shard = Uuid::new_v4();

        // First commit UpdateMemWalState with generation 10
        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // CreateIndex of MemWalIndex based on old version (before UpdateMemWalState)
        // This should succeed and combine the compaction progress.
        let details = MemWalIndexDetails {
            num_shards: 1,
            ..Default::default()
        };
        let mem_wal_index = new_mem_wal_index_meta(dataset.manifest.version - 1, details).unwrap();

        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::CreateIndex {
                new_indices: vec![mem_wal_index],
                removed_indices: vec![],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            result.is_ok(),
            "Expected CreateIndex to succeed with rebase, got {:?}",
            result
        );

        // Verify the compacted_sstables from UpdateMemWalState were included in CreateIndex
        let dataset = result.unwrap();
        let mem_wal_idx = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
            .unwrap()
            .clone();
        let details = load_mem_wal_index_details(mem_wal_idx).unwrap();
        assert_eq!(details.compacted_sstables.len(), 1);
        assert_eq!(details.compacted_sstables[0].shard_id, shard);
        assert_eq!(details.compacted_sstables[0].generation, 10);
        assert_eq!(details.num_shards, 1); // Config from CreateIndex preserved
    }

    /// Test that UpdateMemWalState against CreateIndex of MemWalIndex checks generations.
    #[tokio::test]
    async fn test_update_mem_wal_state_against_create_index_lower_generation() {
        let dataset = test_dataset().await;
        let shard = Uuid::new_v4();

        // First commit CreateIndex of MemWalIndex with compacted_sstables
        let details = MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            ..Default::default()
        };
        let mem_wal_index = new_mem_wal_index_meta(dataset.manifest.version, details).unwrap();

        let txn1 = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![mem_wal_index],
                removed_indices: vec![],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn1)
            .await
            .unwrap();

        // Try UpdateMemWalState with lower generation
        let txn2 = Transaction::new(
            dataset.manifest.version - 1, // Based on old version
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );
        let result = CommitBuilder::new(Arc::new(dataset)).execute(txn2).await;

        assert!(
            matches!(result, Err(crate::Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction when UpdateMemWalState generation is lower than CreateIndex, got {:?}",
            result
        );
    }

    #[test]
    fn test_update_compacted_sstables() {
        let mut indices = Vec::new();
        let shard1 = Uuid::new_v4();
        let shard2 = Uuid::new_v4();

        // First update - creates new index
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            1,
            vec![CompactedSsTable::new(shard1, 5)],
        )
        .unwrap();

        assert_eq!(indices.len(), 1);
        let details = load_mem_wal_index_details(indices[0].clone()).unwrap();
        assert_eq!(details.compacted_sstables.len(), 1);
        assert_eq!(details.compacted_sstables[0].shard_id, shard1);
        assert_eq!(details.compacted_sstables[0].generation, 5);

        // Second update - updates existing shard
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard1, 10)],
        )
        .unwrap();

        assert_eq!(indices.len(), 1);
        let details = load_mem_wal_index_details(indices[0].clone()).unwrap();
        assert_eq!(details.compacted_sstables.len(), 1);
        assert_eq!(details.compacted_sstables[0].generation, 10);

        // Third update - adds new shard
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            3,
            vec![CompactedSsTable::new(shard2, 3)],
        )
        .unwrap();

        assert_eq!(indices.len(), 1);
        let details = load_mem_wal_index_details(indices[0].clone()).unwrap();
        assert_eq!(details.compacted_sstables.len(), 2);

        // Fourth update - lower generation should not update
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            4,
            vec![CompactedSsTable::new(shard1, 8)], // lower than 10
        )
        .unwrap();

        let details = load_mem_wal_index_details(indices[0].clone()).unwrap();
        let shard1_sstable = details
            .compacted_sstables
            .iter()
            .find(|sstable| sstable.shard_id == shard1)
            .unwrap();
        assert_eq!(shard1_sstable.generation, 10); // Should still be 10
    }

    #[test]
    fn test_empty_compacted_sstables_noop() {
        let mut indices = Vec::new();

        // Empty update should be a no-op
        update_mem_wal_index_compacted_sstables(&mut indices, 1, vec![]).unwrap();

        assert!(indices.is_empty());
    }

    /// Regression: a committed `__mem_wal` (legitimately `fragment_bitmap:
    /// None`) must not break `describe_indices` — the path behind lancedb's
    /// `list_indices`/`wait_for_index`. It's described as zero indexed rows,
    /// like `__frag_reuse`.
    #[tokio::test]
    async fn test_describe_indices_includes_mem_wal_system_index() {
        use crate::index::DatasetIndexExt;
        use lance_index::IndexType;
        use lance_index::scalar::ScalarIndexParams;

        let mut dataset = test_dataset().await;

        // A real user index that describe_indices must keep returning.
        dataset
            .create_index(
                &["a"],
                IndexType::Scalar,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        // Commit a __mem_wal index, as WAL provisioning does in production.
        let shard = Uuid::new_v4();
        let txn = Transaction::new(
            dataset.manifest.version,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 1)],
            },
            None,
        );
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(txn)
            .await
            .unwrap();

        // The system index is present with no fragment_bitmap (by design).
        let mem_wal = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|i| i.name == MEM_WAL_INDEX_NAME)
            .unwrap()
            .clone();
        assert!(mem_wal.fragment_bitmap.is_none());

        // describe_indices describes the bitmap-less __mem_wal alongside the
        // real index instead of erroring.
        let descriptions = dataset.describe_indices(None).await.unwrap();
        let mem_wal_desc = descriptions
            .iter()
            .find(|d| d.name() == MEM_WAL_INDEX_NAME)
            .expect("__mem_wal must be described, not skipped");
        assert_eq!(
            mem_wal_desc.index_type(),
            "MemWal",
            "system index type must resolve via infer_system_index_type"
        );
        assert_eq!(
            mem_wal_desc.rows_indexed(),
            0,
            "a bitmap-less system index indexes zero rows"
        );
        assert_eq!(
            descriptions.len(),
            2,
            "both the real scalar index and __mem_wal must be listed"
        );
    }
}
