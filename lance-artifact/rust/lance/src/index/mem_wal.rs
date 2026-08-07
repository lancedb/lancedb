// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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

use std::collections::HashSet;
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
/// Called from the final data-changing merge-insert commit for a compaction
/// target, so the rows and the generation that describes them publish
/// together.
///
/// A proposed generation must be **strictly greater** than the one the latest
/// state records for that shard, and a stale one fails the whole transaction.
/// Accepting it while keeping the larger marker would publish that worker's row
/// mutations under a generation it did not produce, and anything reading only
/// the marker could then stop serving SSTables whose rows were never inserted.
///
/// Every other `MemWalIndexDetails` field is carried through untouched.
pub(crate) fn update_mem_wal_index_compacted_sstables(
    indices: &mut [IndexMetadata],
    dataset_version: u64,
    new_compacted_sstables: Vec<CompactedSsTable>,
) -> Result<()> {
    if new_compacted_sstables.is_empty() {
        return Ok(());
    }

    let mut seen_shards = HashSet::with_capacity(new_compacted_sstables.len());
    for sstable in &new_compacted_sstables {
        if !seen_shards.insert(sstable.shard_id) {
            return Err(Error::invalid_input(format!(
                "Duplicate shard {} in one SSTable compaction update; each shard \
                 may advance at most once per transaction",
                sstable.shard_id
            )));
        }
    }

    // Default details would describe a table with no MemWAL shards at all, so
    // the recorded generation would name a shard nothing can corroborate.
    // Refuse instead of inventing metadata.
    let pos = indices
        .iter()
        .position(|idx| idx.name == MEM_WAL_INDEX_NAME)
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "Cannot record SSTable compaction progress: the {} system index \
                 does not exist on this table",
                MEM_WAL_INDEX_NAME
            ))
        })?;

    // Validated against a copy so a rejected update leaves `indices` exactly as
    // the caller passed it.
    let mut details = load_mem_wal_index_details(indices[pos].clone())?;

    for new_sstable in new_compacted_sstables {
        match details
            .compacted_sstables
            .iter_mut()
            .find(|sstable| sstable.shard_id == new_sstable.shard_id)
        {
            Some(existing) if new_sstable.generation <= existing.generation => {
                return Err(Error::invalid_input(format!(
                    "Stale SSTable compaction for shard {}: proposed generation {} \
                     is not greater than the recorded generation {}",
                    new_sstable.shard_id, new_sstable.generation, existing.generation
                )));
            }
            Some(existing) => existing.generation = new_sstable.generation,
            None => details.compacted_sstables.push(new_sstable),
        }
    }

    // Replaced in place so the index list keeps its order.
    indices[pos] = new_mem_wal_index_meta(dataset_version, details)?;
    Ok(())
}

/// Create a new MemWAL index metadata entry.
///
/// A fresh UUID is minted on every rewrite, including metadata-only updates.
/// The decoded-details cache is keyed on that UUID, so the change of identity
/// is what invalidates it; holding the UUID steady would leave a warmed reader
/// answering with the state from before the update.
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

    use std::collections::HashMap;
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

    /// A dataset with `__lance_mem_wal` already installed, as MemWAL
    /// initialization leaves it. Recording compaction progress requires the
    /// system index to exist, so any test that commits progress needs this
    /// rather than a bare [`test_dataset`].
    async fn test_dataset_with_mem_wal() -> crate::Dataset {
        let dataset = test_dataset().await;
        let mem_wal_index =
            new_mem_wal_index_meta(dataset.manifest.version, MemWalIndexDetails::default())
                .unwrap();
        let txn = Transaction::new(
            dataset.manifest.version,
            Operation::CreateIndex {
                new_indices: vec![mem_wal_index],
                removed_indices: vec![],
            },
            None,
        );
        CommitBuilder::new(Arc::new(dataset))
            .execute(txn)
            .await
            .unwrap()
    }

    /// Test that UpdateMemWalState with lower generation than committed fails without retry.
    /// Per spec: If committed_generation >= to_commit_generation, abort without retry.
    #[tokio::test]
    async fn test_update_mem_wal_state_conflict_lower_generation_no_retry() {
        let dataset = test_dataset_with_mem_wal().await;
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
        let dataset = test_dataset_with_mem_wal().await;
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
        let dataset = test_dataset_with_mem_wal().await;
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
        let dataset = test_dataset_with_mem_wal().await;
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
        let dataset = test_dataset_with_mem_wal().await;
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

    /// One `__lance_mem_wal` entry carrying `details`, as a real table has.
    fn indices_with(details: MemWalIndexDetails) -> Vec<IndexMetadata> {
        vec![new_mem_wal_index_meta(1, details).unwrap()]
    }

    fn compacted_generation(indices: &[IndexMetadata], shard: Uuid) -> Option<u64> {
        load_mem_wal_index_details(indices[0].clone())
            .unwrap()
            .compacted_sstables
            .iter()
            .find(|sstable| sstable.shard_id == shard)
            .map(|sstable| sstable.generation)
    }

    #[test]
    fn test_update_compacted_sstables() {
        let shard1 = Uuid::new_v4();
        let shard2 = Uuid::new_v4();
        let mut indices = indices_with(MemWalIndexDetails::default());

        update_mem_wal_index_compacted_sstables(
            &mut indices,
            1,
            vec![CompactedSsTable::new(shard1, 5)],
        )
        .unwrap();
        assert_eq!(indices.len(), 1);
        assert_eq!(compacted_generation(&indices, shard1), Some(5));

        // Advancing an existing shard.
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard1, 10)],
        )
        .unwrap();
        assert_eq!(compacted_generation(&indices, shard1), Some(10));

        // A second shard is independent.
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            3,
            vec![CompactedSsTable::new(shard2, 3)],
        )
        .unwrap();
        assert_eq!(compacted_generation(&indices, shard1), Some(10));
        assert_eq!(compacted_generation(&indices, shard2), Some(3));
    }

    /// A stale proposal must fail the whole transaction: accepting it while
    /// keeping generation 10 would publish the stale worker's rows under a
    /// marker they did not produce.
    #[test]
    fn a_lower_generation_rejects_the_whole_update() {
        let shard = Uuid::new_v4();
        let mut indices = indices_with(MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            ..Default::default()
        });
        let before = indices.clone();

        let err = update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard, 8)],
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("Stale SSTable compaction"),
            "{err}"
        );
        assert_eq!(
            indices[0].uuid, before[0].uuid,
            "a rejected update must leave the index list untouched"
        );
        assert_eq!(compacted_generation(&indices, shard), Some(10));
    }

    /// Equal is also refused: it proves nothing new, and accepting it would let
    /// a retry publish a second set of row mutations under the same marker.
    #[test]
    fn an_equal_generation_rejects() {
        let shard = Uuid::new_v4();
        let mut indices = indices_with(MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            ..Default::default()
        });

        let err = update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard, 10)],
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("Stale SSTable compaction"),
            "{err}"
        );
    }

    #[test]
    fn duplicate_shards_in_one_update_reject() {
        let shard = Uuid::new_v4();
        let mut indices = indices_with(MemWalIndexDetails::default());

        let err = update_mem_wal_index_compacted_sstables(
            &mut indices,
            1,
            vec![
                CompactedSsTable::new(shard, 5),
                CompactedSsTable::new(shard, 6),
            ],
        )
        .unwrap_err();

        assert!(err.to_string().contains("Duplicate shard"), "{err}");
    }

    /// Absent metadata must not be materialized: default details describe a
    /// table with no MemWAL shards, so the recorded generation would name a
    /// shard nothing can corroborate.
    #[test]
    fn a_missing_mem_wal_index_rejects() {
        let mut indices: Vec<IndexMetadata> = Vec::new();

        let err = update_mem_wal_index_compacted_sstables(
            &mut indices,
            1,
            vec![CompactedSsTable::new(Uuid::new_v4(), 5)],
        )
        .unwrap_err();

        assert!(err.to_string().contains("does not exist"), "{err}");
        assert!(indices.is_empty(), "nothing should have been created");
    }

    /// Recording progress replaces the entry where it sits, so the index list
    /// keeps its order.
    #[test]
    fn recording_progress_keeps_the_system_index_position() {
        let shard = Uuid::new_v4();
        let mut indices = indices_with(MemWalIndexDetails::default());
        // A neighbour to prove the entry is replaced in place, not moved.
        indices.push(IndexMetadata {
            name: "other_index".to_string(),
            ..indices[0].clone()
        });
        update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard, 5)],
        )
        .unwrap();

        assert_eq!(indices[0].name, MEM_WAL_INDEX_NAME);
        assert_eq!(indices[1].name, "other_index");
    }

    /// Recording progress must not disturb any other saved MemWAL field.
    #[test]
    fn recording_progress_preserves_unrelated_mem_wal_state() {
        let shard = Uuid::new_v4();
        let other_shard = Uuid::new_v4();
        let mut writer_config_defaults = HashMap::new();
        writer_config_defaults.insert("target_size".to_string(), "64MB".to_string());
        let details = MemWalIndexDetails {
            snapshot_ts_millis: 12_345,
            num_shards: 4,
            maintained_indexes: vec!["vector_idx".to_string()],
            compacted_sstables: vec![CompactedSsTable::new(other_shard, 7)],
            writer_config_defaults: writer_config_defaults.clone(),
            ..Default::default()
        };
        let mut indices = indices_with(details);

        update_mem_wal_index_compacted_sstables(
            &mut indices,
            2,
            vec![CompactedSsTable::new(shard, 5)],
        )
        .unwrap();

        let after = load_mem_wal_index_details(indices[0].clone()).unwrap();
        assert_eq!(after.snapshot_ts_millis, 12_345);
        assert_eq!(after.num_shards, 4);
        assert_eq!(after.maintained_indexes, vec!["vector_idx".to_string()]);
        assert_eq!(after.writer_config_defaults, writer_config_defaults);
        // The untouched shard keeps its generation.
        assert_eq!(compacted_generation(&indices, other_shard), Some(7));
    }

    /// The hole this guards is a worker that refreshed to HEAD before
    /// submitting: there is no intervening transaction, so the conflict
    /// resolver sees nothing to reject and only apply-time validation stands
    /// between a stale generation and a published commit.
    ///
    /// The row mutation is the point: were the stale generation accepted, this
    /// commit would add a fragment while the recorded generation stayed at 10,
    /// so the rows and the number describing them would disagree.
    #[tokio::test]
    async fn a_stale_generation_against_latest_head_publishes_nothing() {
        use lance_table::format::Fragment;

        let shard = Uuid::new_v4();
        let dataset = test_dataset_with_mem_wal().await;
        let version = dataset.manifest.version;

        // Record generation 10.
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new(
                version,
                Operation::UpdateMemWalState {
                    compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
                },
                None,
            ))
            .await
            .unwrap();

        let version_before = dataset.manifest.version;
        let fragments_before = dataset.get_fragments().len();

        // Built against the LATEST version, so there is nothing stale for the
        // conflict resolver to catch, and carrying a real row mutation.
        let stale = Transaction::new(
            version_before,
            Operation::Update {
                removed_fragment_ids: vec![],
                updated_fragments: vec![],
                new_fragments: vec![Fragment::new(999)],
                fields_modified: vec![],
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            None,
        );
        let mut dataset = dataset;
        let result = CommitBuilder::new(Arc::new(dataset.clone()))
            .execute(stale)
            .await;

        // Asserting the reason, not just the failure: the commit must be
        // refused by apply-time generation validation, not by something
        // incidental about the fragment.
        let err = result.expect_err("a stale generation must fail the whole commit");
        assert!(
            err.to_string().contains("Stale SSTable compaction"),
            "expected stale-generation rejection, got {err}"
        );

        // Neither the marker nor the fragment may have been published.
        dataset.checkout_latest().await.unwrap();
        let latest = dataset;
        assert_eq!(
            latest.manifest.version, version_before,
            "no new table version may be published"
        );
        assert_eq!(
            latest.get_fragments().len(),
            fragments_before,
            "the row mutation must not be published"
        );
        let details = load_mem_wal_index_details(
            latest
                .load_indices()
                .await
                .unwrap()
                .iter()
                .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(
            details.compacted_sstables[0].generation, 10,
            "the recorded generation must be unchanged"
        );
    }

    #[test]
    fn test_empty_compacted_sstables_noop() {
        let mut indices = Vec::new();

        // Empty update should be a no-op, even with no MemWAL index present.
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

        let mut dataset = test_dataset_with_mem_wal().await;

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
