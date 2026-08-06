// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::index::DatasetIndexExt;
use crate::index::frag_reuse::{build_frag_reuse_index_metadata, load_frag_reuse_index_details};
use crate::index::mem_wal::{load_mem_wal_index_details, new_mem_wal_index_meta};
use crate::io::deletion::read_dataset_deletion_file;
use crate::{
    Dataset,
    dataset::transaction::{DataOverlayGroup, Operation, Transaction, UpdateMode},
};
use futures::{StreamExt, TryStreamExt};
use lance_core::{Error, Result, utils::deletion::DeletionVector};
use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;
use lance_index::mem_wal::{CompactedSsTable, MEM_WAL_INDEX_NAME};
use lance_select::{RowAddrTreeMap, RowSetOps};
use lance_table::format::IndexMetadata;
use lance_table::format::overlay::OverlayCoverage;
use lance_table::{format::Fragment, io::deletion::write_deletion_file};
use roaring::RoaringBitmap;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Debug)]
pub struct TransactionRebase<'a> {
    transaction: Transaction,
    /// Relevant fragments as they were at the read version of the transaction.
    /// Has original fragment, plus a bool indicating whether a rewrite is needed.
    initial_fragments: HashMap<u64, (Fragment, bool)>,
    /// Fragments that have been deleted or modified
    modified_fragment_ids: HashSet<u64>,
    affected_rows: Option<&'a RowAddrTreeMap>,
    conflicting_frag_reuse_indices: Vec<IndexMetadata>,
    /// Compacted SSTables from conflicting UpdateMemWalState transactions.
    /// Used when rebasing CreateIndex of MemWalIndex.
    conflicting_mem_wal_compacted_sstables: Vec<CompactedSsTable>,
}

impl<'a> TransactionRebase<'a> {
    pub async fn try_new(
        dataset: &Dataset,
        transaction: Transaction,
        affected_rows: Option<&'a RowAddrTreeMap>,
    ) -> Result<Self> {
        match &transaction.operation {
            // These operations add new fragments or don't modify any.
            Operation::Append { .. }
            | Operation::Overwrite { .. }
            | Operation::CreateIndex { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Project { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateMemWalState { .. }
            | Operation::Clone { .. }
            | Operation::Restore { .. }
            | Operation::UpdateBases { .. } => Ok(Self {
                transaction,
                affected_rows,
                initial_fragments: HashMap::new(),
                modified_fragment_ids: HashSet::new(),
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            }),
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            }
            | Operation::Update {
                updated_fragments,
                removed_fragment_ids: deleted_fragment_ids,
                ..
            } => {
                let modified_fragment_ids = updated_fragments
                    .iter()
                    .map(|f| f.id)
                    .chain(deleted_fragment_ids.iter().copied())
                    .collect::<HashSet<_>>();

                // short circuit for full fragment update or delete case
                // set affected_rows as None with non-empty modified_fragment_ids
                // to indicate this condition to be used in [check_delete_update_txn]
                if updated_fragments.is_empty() && affected_rows.is_some() {
                    return Ok(Self {
                        transaction,
                        initial_fragments: HashMap::new(),
                        modified_fragment_ids,
                        affected_rows: None,
                        conflicting_frag_reuse_indices: Vec::new(),
                        conflicting_mem_wal_compacted_sstables: Vec::new(),
                    });
                }

                let initial_fragments =
                    initial_fragments_for_rebase(dataset, &transaction, &modified_fragment_ids)
                        .await;
                Ok(Self {
                    transaction,
                    affected_rows,
                    initial_fragments,
                    modified_fragment_ids,
                    conflicting_frag_reuse_indices: Vec::new(),
                    conflicting_mem_wal_compacted_sstables: Vec::new(),
                })
            }
            Operation::Rewrite { groups, .. } => {
                let modified_fragment_ids = groups
                    .iter()
                    .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                    .collect::<HashSet<_>>();

                let initial_fragments =
                    initial_fragments_for_rebase(dataset, &transaction, &modified_fragment_ids)
                        .await;
                Ok(Self {
                    transaction,
                    affected_rows,
                    initial_fragments,
                    modified_fragment_ids,
                    conflicting_frag_reuse_indices: Vec::new(),
                    conflicting_mem_wal_compacted_sstables: Vec::new(),
                })
            }
            Operation::DataReplacement { replacements } => {
                let modified_fragment_ids =
                    replacements.iter().map(|r| r.0).collect::<HashSet<_>>();
                let initial_fragments =
                    initial_fragments_for_rebase(dataset, &transaction, &modified_fragment_ids)
                        .await;
                Ok(Self {
                    transaction,
                    affected_rows,
                    initial_fragments,
                    modified_fragment_ids,
                    conflicting_frag_reuse_indices: Vec::new(),
                    conflicting_mem_wal_compacted_sstables: Vec::new(),
                })
            }
            Operation::DataOverlay { groups } => {
                let modified_fragment_ids =
                    groups.iter().map(|g| g.fragment_id).collect::<HashSet<_>>();
                let initial_fragments =
                    initial_fragments_for_rebase(dataset, &transaction, &modified_fragment_ids)
                        .await;
                Ok(Self {
                    transaction,
                    affected_rows,
                    initial_fragments,
                    modified_fragment_ids,
                    conflicting_frag_reuse_indices: Vec::new(),
                    conflicting_mem_wal_compacted_sstables: Vec::new(),
                })
            }
            Operation::Merge { fragments, .. } => {
                let modified_fragment_ids = fragments.iter().map(|f| f.id).collect::<HashSet<_>>();
                let initial_fragments =
                    initial_fragments_for_rebase(dataset, &transaction, &modified_fragment_ids)
                        .await;
                Ok(Self {
                    transaction,
                    affected_rows,
                    initial_fragments,
                    modified_fragment_ids,
                    conflicting_frag_reuse_indices: Vec::new(),
                    conflicting_mem_wal_compacted_sstables: Vec::new(),
                })
            }
        }
    }

    #[track_caller]
    fn retryable_conflict_err(&self, other_transaction: &Transaction, other_version: u64) -> Error {
        Error::retryable_commit_conflict_source(
            other_version,
            format!(
                "This {} transaction was preempted by concurrent transaction {} at version {}. Please retry.",
                self.transaction.operation, other_transaction.operation, other_version
            )
            .into(),
        )
    }

    #[track_caller]
    fn incompatible_conflict_err(
        &self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Error {
        Error::incompatible_transaction_source(
            format!(
                "This {} transaction is incompatible with concurrent transaction {} at version {}.",
                self.transaction.operation, other_transaction.operation, other_version
            )
            .into(),
        )
    }

    #[track_caller]
    fn data_replacement_target_removed_err(
        &self,
        fragment_id: u64,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Error {
        Error::incompatible_transaction_source(
            format!(
                "DataReplacement target fragment {} was removed by concurrent {} at version {}.",
                fragment_id, other_transaction.operation, other_version
            )
            .into(),
        )
    }

    /// Check whether the transaction conflicts with another transaction.
    /// Mutate the current [TransactionRebase] based on `other_transaction` to be used for
    /// eventually finishing the rebase process.
    ///
    /// Will return an error if the transaction is not valid. Otherwise, it will
    /// return Ok(()).
    pub fn check_txn(&mut self, other_transaction: &Transaction, other_version: u64) -> Result<()> {
        let op = &self.transaction.operation;
        match op {
            Operation::Delete { .. } => self.check_delete_txn(other_transaction, other_version),
            Operation::Update { .. } => self.check_update_txn(other_transaction, other_version),
            Operation::CreateIndex { .. } => {
                self.check_create_index_txn(other_transaction, other_version)
            }
            Operation::Rewrite { .. } => self.check_rewrite_txn(other_transaction, other_version),
            Operation::Overwrite { .. } => {
                self.check_overwrite_txn(other_transaction, other_version)
            }
            Operation::Append { .. } => self.check_append_txn(other_transaction, other_version),
            Operation::DataReplacement { .. } => {
                self.check_data_replacement_txn(other_transaction, other_version)
            }
            Operation::DataOverlay { .. } => {
                self.check_data_overlay_txn(other_transaction, other_version)
            }
            Operation::Merge { .. } => self.check_merge_txn(other_transaction, other_version),
            Operation::Restore { .. } => self.check_restore_txn(other_transaction, other_version),
            Operation::ReserveFragments { .. } => {
                self.check_reserve_fragments_txn(other_transaction, other_version)
            }
            Operation::Project { .. } => self.check_project_txn(other_transaction, other_version),
            Operation::UpdateConfig { .. } => {
                self.check_update_config_txn(other_transaction, other_version)
            }
            Operation::UpdateMemWalState { .. } => {
                self.check_update_mem_wal_state_txn(other_transaction, other_version)
            }
            Operation::Clone { .. } => Ok(()),
            Operation::UpdateBases { .. } => {
                self.check_add_bases_txn(other_transaction, other_version)
            }
        }
    }

    fn check_delete_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::Delete { .. } = &self.transaction.operation {
            match &other_transaction.operation {
                Operation::CreateIndex { .. }
                | Operation::ReserveFragments { .. }
                | Operation::Clone { .. }
                | Operation::Project { .. }
                | Operation::Append { .. }
                | Operation::UpdateConfig { .. }
                // A concurrent overlay is inert against the rows we delete
                // (deletions take precedence over overlays) and otherwise
                // preserves physical offsets, so it never conflicts.
                | Operation::DataOverlay { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::Rewrite { groups, .. } => {
                    if groups
                        .iter()
                        .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::DataReplacement { replacements, .. } => {
                    if replacements
                        .iter()
                        .map(|r| r.0)
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::Update {
                    updated_fragments,
                    removed_fragment_ids,
                    ..
                }
                | Operation::Delete {
                    updated_fragments,
                    deleted_fragment_ids: removed_fragment_ids,
                    ..
                } => {
                    if !updated_fragments
                        .iter()
                        .map(|f| f.id)
                        .chain(removed_fragment_ids.iter().copied())
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        return Ok(());
                    }

                    if self.affected_rows.is_none() {
                        // We don't have any affected rows, so we can't
                        // do the rebase anyways.
                        return Err(self.retryable_conflict_err(other_transaction, other_version));
                    }
                    for updated in updated_fragments {
                        if let Some((fragment, needs_rewrite)) =
                            self.initial_fragments.get_mut(&updated.id)
                        {
                            // If data files, not just deletion files, are modified,
                            // then we can't rebase.
                            if fragment.files != updated.files {
                                return Err(
                                    self.retryable_conflict_err(other_transaction, other_version)
                                );
                            }

                            // Mark any modified fragments as needing a rewrite.
                            *needs_rewrite |= updated.deletion_file != fragment.deletion_file;
                        }
                    }

                    for removed_fragment_id in removed_fragment_ids {
                        if self.initial_fragments.contains_key(removed_fragment_id) {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                    }
                    Ok(())
                }
                Operation::Merge { .. } => {
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                }
                Operation::Overwrite { .. }
                | Operation::Restore { .. }
                | Operation::UpdateMemWalState { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_update_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::Update {
            inserted_rows_filter: self_inserted_rows_filter,
            compacted_sstables: self_compacted_sstables,
            new_fragments: self_new_fragments,
            update_mode: self_update_mode,
            ..
        } = &self.transaction.operation
        {
            if let Operation::Update {
                inserted_rows_filter: other_inserted_rows_filter,
                ..
            } = &other_transaction.operation
            {
                // The presence of inserted_rows_filter means this is a primary key operation
                // and strict conflict detection should be applied.
                match (self_inserted_rows_filter, other_inserted_rows_filter) {
                    (Some(self_keys), Some(other_keys)) => {
                        if self_keys.field_ids != other_keys.field_ids {
                            // Different key columns - can't verify conflicts
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                        // Check for intersection. If the bloom filter configs don't match
                        // (e.g., different number_of_items or probability), intersects() returns
                        // an error and we treat it as a conflict to be safe.
                        let Ok((has_intersection, _maybe_false_positive)) =
                            self_keys.intersects(other_keys)
                        else {
                            // Bloom filter configs don't match - treat as conflict
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        };
                        if has_intersection {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                    }
                    (Some(_), None) => {
                        // Current transaction has primary key conflict detection but
                        // the already committed transaction doesn't have a filter.
                        // We can't determine what rows were inserted by the other
                        // transaction, so we must fail to be safe.
                        return Err(self.retryable_conflict_err(other_transaction, other_version));
                    }
                    _ => {}
                }
            }

            match &other_transaction.operation {
                Operation::CreateIndex { .. }
                | Operation::ReserveFragments { .. }
                | Operation::Project { .. }
                | Operation::Clone { .. }
                | Operation::UpdateConfig { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::DataOverlay { groups } => {
                    // Our update recomputed rows from the pre-overlay base, so if
                    // it commits over an overlay it would silently undo the
                    // overlay's values for any cell it recomputed. A row-moving
                    // update (RewriteRows) relocates the rows it touches out to
                    // new fragments; only the rows it actually moved lose their
                    // overlay, so we conflict only when the moved rows intersect
                    // the overlay's coverage. An in-place column rewrite
                    // (RewriteColumns) preserves offsets and just tombstones the
                    // overlaid fields at build time, so it never conflicts.
                    let moves_rows = !self_new_fragments.is_empty()
                        && matches!(self_update_mode, Some(UpdateMode::RewriteRows) | None);
                    if !moves_rows {
                        return Ok(());
                    }
                    // `affected_rows` holds the physical offsets (per fragment)
                    // this update moved. The overlay's coverage is in the same
                    // physical-offset space, so we can intersect the two in
                    // memory. Without affected rows we cannot be precise, so we
                    // fall back to a fragment-granular conflict.
                    for group in groups {
                        if !self.modified_fragment_ids.contains(&group.fragment_id) {
                            continue;
                        }
                        let Some(affected_rows) = self.affected_rows else {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        };
                        let Some(moved) =
                            affected_rows.get_fragment_bitmap(group.fragment_id as u32)
                        else {
                            continue;
                        };
                        let coverage = overlay_group_coverage(group);
                        if !(moved & &coverage).is_empty() {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                    }
                    Ok(())
                }
                Operation::Append { .. } => {
                    // If current transaction has primary key conflict detection,
                    // we can't safely commit against an Append because we don't
                    // know if the appended rows conflict with inserted rows.
                    if self_inserted_rows_filter.is_some() {
                        return Err(self.retryable_conflict_err(other_transaction, other_version));
                    }
                    Ok(())
                }
                Operation::Rewrite { groups, .. } => {
                    if groups
                        .iter()
                        .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::DataReplacement { replacements, .. } => {
                    if replacements
                        .iter()
                        .map(|r| r.0)
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::Update {
                    updated_fragments,
                    removed_fragment_ids,
                    ..
                }
                | Operation::Delete {
                    updated_fragments,
                    deleted_fragment_ids: removed_fragment_ids,
                    ..
                } => {
                    if !updated_fragments
                        .iter()
                        .map(|f| f.id)
                        .chain(removed_fragment_ids.iter().copied())
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        return Ok(());
                    }

                    if self.affected_rows.is_none() {
                        // We don't have any affected rows, so we can't
                        // do the rebase anyways.
                        return Err(self.retryable_conflict_err(other_transaction, other_version));
                    }
                    for updated in updated_fragments {
                        if let Some((fragment, needs_rewrite)) =
                            self.initial_fragments.get_mut(&updated.id)
                        {
                            // If data files, not just deletion files, are modified,
                            // then we can't rebase.
                            if fragment.files != updated.files {
                                return Err(
                                    self.retryable_conflict_err(other_transaction, other_version)
                                );
                            }

                            // Mark any modified fragments as needing a rewrite.
                            *needs_rewrite |= updated.deletion_file != fragment.deletion_file;
                        }
                    }

                    for removed_fragment_id in removed_fragment_ids {
                        if self.initial_fragments.contains_key(removed_fragment_id) {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                    }
                    Ok(())
                }
                Operation::Merge { .. } => {
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                }
                Operation::Overwrite { .. } | Operation::Restore { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
                Operation::UpdateMemWalState {
                    compacted_sstables: other_compacted_sstables,
                } => self.check_compacted_sstables_conflict(
                    other_compacted_sstables,
                    self_compacted_sstables,
                    other_transaction,
                    other_version,
                ),
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_create_index_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::CreateIndex {
            new_indices,
            removed_indices,
            ..
        } = &mut self.transaction.operation
        {
            match &other_transaction.operation {
                Operation::Append { .. }
                | Operation::Clone { .. }
                // An overlay committed after this index's version is newer than
                // the index; the query path excludes its covered cells via the
                // version gate, so the build does not conflict.
                | Operation::DataOverlay { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::CreateIndex {
                    new_indices: created_indices,
                    ..
                } => {
                    let self_has_frag_reuse = new_indices
                        .iter()
                        .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME);
                    let other_has_frag_reuse = created_indices
                        .iter()
                        .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME);
                    let self_has_mem_wal =
                        new_indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME);
                    let other_has_mem_wal = created_indices
                        .iter()
                        .any(|idx| idx.name == MEM_WAL_INDEX_NAME);
                    let has_regular_name_conflict = new_indices
                        .iter()
                        .filter(|idx| {
                            idx.name != FRAG_REUSE_INDEX_NAME && idx.name != MEM_WAL_INDEX_NAME
                        })
                        .any(|new_index| {
                            created_indices
                                .iter()
                                .any(|created_index| created_index.name == new_index.name)
                        });

                    if (self_has_frag_reuse && other_has_frag_reuse)
                        || (self_has_mem_wal && other_has_mem_wal)
                        || has_regular_name_conflict
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                // Although some of the rows we indexed may have been deleted / moved,
                // row ids are still valid, so we allow this optimistically.
                Operation::Delete { .. } => Ok(()),
                Operation::Update {
                    updated_fragments,
                    fields_modified,
                    ..
                } => {
                    Transaction::prune_updated_fields_from_indices(
                        new_indices,
                        updated_fragments,
                        fields_modified,
                    );
                    Ok(())
                }
                // Merge, reserve, and project don't change row ids, so this should be fine.
                Operation::Merge { .. } => Ok(()),
                Operation::ReserveFragments { .. } => Ok(()),
                Operation::Project { .. } => Ok(()),
                // Should be compatible with rewrite if it didn't move the rows
                // we indexed. If it did, we could retry.
                // TODO: this will change with stable row ids.
                Operation::Rewrite {
                    groups,
                    frag_reuse_index,
                    ..
                } => {
                    // if frag_reuse_index is available, index remapping is deferred and
                    // there is no conflict with concurrent CreateIndex of column indices.
                    // The only case that needs rebasing is when the frag_reuse_index cleanup
                    // triggers a CreateIndex, and it needs to add the new reuse
                    // version created by the rewrite
                    if let Some(committed_fri) = frag_reuse_index {
                        let ngram_coverage = new_indices
                            .iter()
                            .filter(|idx| {
                                idx.index_details.as_ref().is_some_and(|details| {
                                    details.type_url.ends_with("NGramIndexDetails")
                                })
                            })
                            .filter_map(|idx| idx.fragment_bitmap.as_ref())
                            .fold(RoaringBitmap::new(), |coverage, fragments| {
                                coverage | fragments
                            });
                        if groups
                            .iter()
                            .flat_map(|group| group.old_fragments.iter())
                            .any(|fragment| ngram_coverage.contains(fragment.id as u32))
                        {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }

                        if new_indices
                            .iter()
                            .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME)
                        {
                            // this should not happen today since we don't support committing
                            // a mixture of frag_reuse_index and other indices.
                            if new_indices.len() != 1 || removed_indices.len() != 1 {
                                return Err(self
                                    .incompatible_conflict_err(other_transaction, other_version));
                            }

                            self.conflicting_frag_reuse_indices
                                .push(committed_fri.clone());
                            Ok(())
                        } else {
                            Ok(())
                        }
                    } else {
                        let mut affected_ids = HashSet::new();
                        for index in new_indices.iter() {
                            if let Some(frag_bitmap) = &index.fragment_bitmap {
                                affected_ids.extend(frag_bitmap.iter());
                            } else {
                                return Err(
                                    self.retryable_conflict_err(other_transaction, other_version)
                                );
                            }
                        }

                        if groups
                            .iter()
                            .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                            .any(|id| affected_ids.contains(&(id as u32)))
                        {
                            Err(self.retryable_conflict_err(other_transaction, other_version))
                        } else {
                            Ok(())
                        }
                    }
                }
                Operation::UpdateConfig { .. } => Ok(()),
                Operation::DataReplacement { replacements } => {
                    // A data replacement only conflicts if it is updating the field that
                    // is being indexed.
                    let newly_indexed_fields = new_indices
                        .iter()
                        .flat_map(|idx| idx.fields.iter())
                        .collect::<HashSet<_>>();
                    for replacement in replacements {
                        for field in replacement.1.fields.iter() {
                            if newly_indexed_fields.contains(&field) {
                                return Err(
                                    self.retryable_conflict_err(other_transaction, other_version)
                                );
                            }
                        }
                    }
                    Ok(())
                }
                Operation::UpdateMemWalState {
                    compacted_sstables: other_compacted_sstables,
                } => {
                    // CreateIndex of MemWalIndex is compatible with UpdateMemWalState
                    // as they can be rebased on each other
                    if new_indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME) {
                        // Collect compacted_sstables from UpdateMemWalState for rebasing
                        self.conflicting_mem_wal_compacted_sstables
                            .extend(other_compacted_sstables.iter().cloned());
                        Ok(())
                    } else {
                        Err(self.incompatible_conflict_err(other_transaction, other_version))
                    }
                }
                Operation::Overwrite { .. } | Operation::Restore { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_rewrite_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::Rewrite {
            groups,
            frag_reuse_index,
            ..
        } = &self.transaction.operation
        {
            match &other_transaction.operation {
                // Rewrite is only compatible with operations that don't touch
                // existing fragments or update fragments we don't touch.
                Operation::Append { .. }
                | Operation::ReserveFragments { .. }
                | Operation::Project { .. }
                | Operation::Clone { .. }
                | Operation::UpdateConfig { .. }
                | Operation::UpdateMemWalState { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::Delete {
                    updated_fragments,
                    deleted_fragment_ids,
                    ..
                }
                | Operation::Update {
                    updated_fragments,
                    removed_fragment_ids: deleted_fragment_ids,
                    ..
                } => {
                    if updated_fragments
                        .iter()
                        .map(|f| f.id)
                        .chain(deleted_fragment_ids.iter().copied())
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::DataOverlay { groups } => {
                    // Rewriting a fragment changes its physical row addresses, so
                    // an overlay addressed by physical offset on that fragment is
                    // invalidated and must be re-applied against the new base.
                    if groups
                        .iter()
                        .map(|g| g.fragment_id)
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::Rewrite {
                    groups,
                    frag_reuse_index: committed_fri,
                    ..
                } => {
                    if groups
                        .iter()
                        .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                        .any(|id| self.modified_fragment_ids.contains(&id))
                    {
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else if committed_fri.is_some() && frag_reuse_index.is_some() {
                        // Do not commit concurrent rewrites that could produce conflicting frag_reuse_indexes.
                        // The other rewrite must retry.
                        // TODO: could potentially rebase to combine both frag_reuse_indexes,
                        //   but today it is already rare to run concurrent rewrites.
                        Err(self.retryable_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::DataReplacement { replacements } => {
                    // These conflict if the rewrite touches any of the fragments being replaced.
                    for replacement in replacements {
                        for group in groups {
                            for old_fragment in &group.old_fragments {
                                if replacement.0 == old_fragment.id {
                                    return Err(self
                                        .retryable_conflict_err(other_transaction, other_version));
                                }
                            }
                        }
                    }
                    Ok(())
                }
                Operation::Merge { .. } => {
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                }
                Operation::CreateIndex {
                    new_indices,
                    removed_indices,
                    ..
                } => {
                    match (
                        new_indices
                            .iter()
                            .find(|idx| idx.name == FRAG_REUSE_INDEX_NAME),
                        &frag_reuse_index,
                    ) {
                        // If the rewrite produces a frag_reuse_index, but frag_reuse_index was cleaned up
                        // in the other transaction, the frag_reuse_index produced by the rewrite should
                        // be cleaned up in the same way as a part of the rebase.
                        (Some(committed_fri), Some(_)) => {
                            // this should not happen today since we don't support committing
                            // a mixture of frag_reuse_index and other indices.
                            if new_indices.len() != 1 || removed_indices.len() != 1 {
                                return Err(self
                                    .incompatible_conflict_err(other_transaction, other_version));
                            }

                            self.conflicting_frag_reuse_indices
                                .push(committed_fri.clone());
                            Ok(())
                        }
                        // If rewrite defers index remap, the FRI handles the
                        // post-commit bitmap update — but only if each rewrite
                        // group is fully inside or fully outside each new
                        // index's fragment bitmap. A group that straddles
                        // would produce a bitmap with a mix of indexed and
                        // non-indexed fragments, which load_indices rejects.
                        (None, Some(_)) => {
                            for index in new_indices {
                                let Some(frag_bitmap) = &index.fragment_bitmap else {
                                    return Err(self
                                        .retryable_conflict_err(other_transaction, other_version));
                                };
                                for group in groups {
                                    let mut indexed = 0usize;
                                    let mut unindexed = 0usize;
                                    for frag in &group.old_fragments {
                                        if frag_bitmap.contains(frag.id as u32) {
                                            indexed += 1;
                                        } else {
                                            unindexed += 1;
                                        }
                                    }
                                    if indexed > 0 && unindexed > 0 {
                                        return Err(self.retryable_conflict_err(
                                            other_transaction,
                                            other_version,
                                        ));
                                    }
                                }
                            }
                            Ok(())
                        }
                        // Rewrite with remapping and frag_reuse_index creation can commit without conflict
                        (Some(_), None) => {
                            // this should not happen today since we don't support committing
                            // a mixture of frag_reuse_index and other indices.
                            if new_indices.len() != 1 || removed_indices.len() != 1 {
                                Err(self
                                    .incompatible_conflict_err(other_transaction, other_version))
                            } else {
                                Ok(())
                            }
                        }
                        // Rewrite with remapping will conflict with
                        // index creation that touches overlapping fragments.
                        (_, None) => {
                            let mut affected_ids = HashSet::new();
                            for index in new_indices {
                                if let Some(frag_bitmap) = &index.fragment_bitmap {
                                    affected_ids.extend(frag_bitmap.iter());
                                } else {
                                    return Err(self
                                        .retryable_conflict_err(other_transaction, other_version));
                                }
                            }
                            if groups
                                .iter()
                                .flat_map(|f| f.old_fragments.iter().map(|f| f.id))
                                .any(|id| affected_ids.contains(&(id as u32)))
                            {
                                Err(self.retryable_conflict_err(other_transaction, other_version))
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
                Operation::Overwrite { .. } | Operation::Restore { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_overwrite_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            Operation::Overwrite { .. } => {
                if self
                    .transaction
                    .operation
                    .upsert_key_conflict(&other_transaction.operation)
                {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                } else {
                    // Concurrent overwrites are retryable so user can decide
                    // if their overwrite should still proceed
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                }
            }
            Operation::UpdateConfig { .. } => {
                if self
                    .transaction
                    .operation
                    .upsert_key_conflict(&other_transaction.operation)
                {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                } else {
                    Ok(())
                }
            }
            Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
            Operation::Append { .. }
            | Operation::Clone { .. }
            | Operation::Delete { .. }
            | Operation::CreateIndex { .. }
            | Operation::Rewrite { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. }
            | Operation::Merge { .. }
            | Operation::Restore { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Update { .. }
            | Operation::Project { .. }
            | Operation::UpdateBases { .. } => Ok(()),
        }
    }

    fn check_append_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            // Append is not compatible with any operation that completely
            // overwrites the schema.
            Operation::Overwrite { .. }
            | Operation::Restore { .. }
            | Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
            Operation::Append { .. }
            | Operation::Rewrite { .. }
            | Operation::CreateIndex { .. }
            | Operation::Delete { .. }
            | Operation::Update { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Project { .. }
            | Operation::UpdateBases { .. }
            | Operation::Merge { .. }
            | Operation::UpdateConfig { .. }
            | Operation::Clone { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. } => Ok(()),
        }
    }

    fn check_data_replacement_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::DataReplacement { replacements } = &self.transaction.operation {
            match &other_transaction.operation {
                Operation::Append { .. }
                | Operation::Clone { .. }
                | Operation::UpdateConfig { .. }
                | Operation::ReserveFragments { .. }
                | Operation::Project { .. }
                // Both a column replacement and an overlay preserve physical row
                // addresses; the overlay is newer and wins its covered cells.
                | Operation::DataOverlay { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::Merge { .. } => {
                    // Merge rewrites the whole fragment list; always conflict
                    // (symmetric with check_merge_txn).
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                }
                Operation::Delete {
                    deleted_fragment_ids,
                    ..
                } => {
                    // A delete only tombstones rows (deletion vector); our positional
                    // file stays aligned and the rebase preserves the deletion vector.
                    // Conflict only if our target fragment was removed outright.
                    for replacement in replacements {
                        if deleted_fragment_ids.contains(&replacement.0) {
                            return Err(self.data_replacement_target_removed_err(
                                replacement.0,
                                other_transaction,
                                other_version,
                            ));
                        }
                    }
                    Ok(())
                }
                Operation::Update {
                    removed_fragment_ids,
                    updated_fragments,
                    new_fragments,
                    fields_modified,
                    update_mode,
                    ..
                } => {
                    for replacement in replacements {
                        if removed_fragment_ids.contains(&replacement.0) {
                            return Err(self.data_replacement_target_removed_err(
                                replacement.0,
                                other_transaction,
                                other_version,
                            ));
                        }
                        if !updated_fragments.iter().any(|f| f.id == replacement.0) {
                            continue;
                        }
                        // A row-rewriting update moves the matched rows out to
                        // new_fragments our positional file does not cover; a horizontal
                        // update may rewrite one of our fields in place. Either makes the
                        // file stale. (RewriteColumns new_fragments are unrelated inserts,
                        // not moved rows, so they stay aligned.)
                        let moved_rows = !new_fragments.is_empty()
                            && matches!(update_mode, Some(UpdateMode::RewriteRows) | None);
                        let field_rewritten = replacement
                            .1
                            .fields
                            .iter()
                            .any(|f| *f >= 0 && fields_modified.contains(&(*f as u32)));
                        if moved_rows || field_rewritten {
                            return Err(
                                self.retryable_conflict_err(other_transaction, other_version)
                            );
                        }
                    }
                    Ok(())
                }
                Operation::CreateIndex { new_indices, .. } => {
                    // A data replacement only conflicts if it is updating the field that
                    // is being indexed.
                    //
                    // TODO: We could potentially just drop the fragments being replaced from
                    // the index's fragment bitmap, which would lead to fewer conflicts.  However
                    // this would introduce fragment bitmaps with holes which may not be well tested
                    // yet.  For now, we don't allow this case.
                    let newly_indexed_fields = new_indices
                        .iter()
                        .flat_map(|idx| idx.fields.iter())
                        .collect::<HashSet<_>>();
                    for replacement in replacements {
                        for field in replacement.1.fields.iter() {
                            if newly_indexed_fields.contains(&field) {
                                return Err(
                                    self.retryable_conflict_err(other_transaction, other_version)
                                );
                            }
                        }
                    }
                    Ok(())
                }
                Operation::Rewrite { groups, .. } => {
                    // These conflict if the rewrite touches any of the fragments being replaced.
                    for replacement in replacements {
                        for group in groups {
                            for old_fragment in &group.old_fragments {
                                if replacement.0 == old_fragment.id {
                                    return Err(self
                                        .retryable_conflict_err(other_transaction, other_version));
                                }
                            }
                        }
                    }

                    Ok(())
                }
                Operation::DataReplacement {
                    replacements: other_replacements,
                } => {
                    // These conflict if there is overlap in fragment id && fields.
                    for replacement in replacements {
                        for other_replacement in other_replacements {
                            if replacement.0 != other_replacement.0 {
                                continue;
                            }

                            for field in replacement.1.fields.iter() {
                                if other_replacement.1.fields.contains(field) {
                                    return Err(self
                                        .retryable_conflict_err(other_transaction, other_version));
                                }
                            }
                        }
                    }
                    Ok(())
                }
                Operation::Overwrite { .. }
                | Operation::Restore { .. }
                | Operation::UpdateMemWalState { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    /// Conflict checks for our DataOverlay transaction against a concurrent one.
    ///
    /// Overlays are intentionally permissive (see the Data Overlay Files spec):
    /// they stack with other overlays and tolerate appends, index builds, data
    /// replacement, deletes, and in-place column rewrites (Update with
    /// `RewriteColumns`), because overlay coverage is addressed by physical offset
    /// and the version gate keeps indexes correct. A concurrent operation
    /// conflicts when it takes precedence over the overlay for cells the overlay
    /// covers, dropping the overlay's values: retryably when it rewrites the
    /// physical layout of one of our fragments (Rewrite, Merge) or re-creates the
    /// covered rows from the pre-overlay base (a row-moving Update — checked
    /// row-by-row in `finish_data_overlay`), or removes an overlaid fragment
    /// outright (a Delete / Update that drops the fragment); and incompatibly for
    /// whole-dataset replacements (Overwrite / Restore) and MemWAL state updates
    /// (UpdateMemWalState), which do not rebase against data operations.
    fn check_data_overlay_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            Operation::Append { .. }
            | Operation::CreateIndex { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Project { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateBases { .. }
            | Operation::Clone { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. } => Ok(()),
            // A concurrent Delete only tombstones rows via a deletion vector,
            // which preserves physical offsets; the overlay value for a deleted
            // offset is simply inert. Conflict only if the whole overlaid
            // fragment was removed, orphaning the overlay.
            Operation::Delete {
                deleted_fragment_ids,
                ..
            } => {
                if deleted_fragment_ids
                    .iter()
                    .any(|id| self.modified_fragment_ids.contains(id))
                {
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                } else {
                    Ok(())
                }
            }
            // A concurrent Update that removed an overlaid fragment orphans the
            // overlay outright — conflict. A row-moving update (RewriteRows)
            // deletes the rows it touches and re-creates them in new fragments;
            // the update took precedence and the re-created rows were computed
            // from the pre-overlay base, so the overlay's values for those cells
            // are lost. That is a per-row problem, not an offset one: only the
            // moved rows are affected. Comparing the moved rows against the
            // overlay's coverage needs the update's deletion vectors, so we mark
            // the fragment here and verify row-by-row in `finish_data_overlay`.
            // An in-place column rewrite (RewriteColumns) preserves rows and just
            // tombstones the overlaid fields at build time, so it never conflicts.
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                update_mode,
                ..
            } => {
                let removed_ours = removed_fragment_ids
                    .iter()
                    .any(|id| self.modified_fragment_ids.contains(id));
                if removed_ours {
                    return Err(self.retryable_conflict_err(other_transaction, other_version));
                }
                let moves_rows = !new_fragments.is_empty()
                    && matches!(update_mode, Some(UpdateMode::RewriteRows) | None);
                if moves_rows {
                    for updated in updated_fragments {
                        if let Some((_, needs_row_check)) =
                            self.initial_fragments.get_mut(&updated.id)
                        {
                            *needs_row_check = true;
                        }
                    }
                }
                Ok(())
            }
            Operation::Rewrite { groups, .. } => {
                // A rewrite (compaction / fold) of a fragment we are overlaying
                // changes its physical row addresses, so our offsets would be
                // invalid. Conflict only if it touches one of our fragments.
                let touches_our_fragment = groups
                    .iter()
                    .flat_map(|g| g.old_fragments.iter())
                    .any(|f| self.modified_fragment_ids.contains(&f.id));
                if touches_our_fragment {
                    Err(self.retryable_conflict_err(other_transaction, other_version))
                } else {
                    Ok(())
                }
            }
            Operation::Merge { .. } => {
                // Merge rewrites the whole fragment list; always conflict.
                Err(self.retryable_conflict_err(other_transaction, other_version))
            }
            // Overwrite/Restore replace the dataset; UpdateMemWalState does not
            // rebase against data operations (mirroring check_update_mem_wal_state_txn,
            // which likewise treats a concurrent DataOverlay as incompatible).
            Operation::Overwrite { .. }
            | Operation::Restore { .. }
            | Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
        }
    }

    fn check_merge_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            Operation::CreateIndex { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Clone { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateBases { .. } => Ok(()),

            Operation::Update { .. }
            | Operation::Append { .. }
            | Operation::Delete { .. }
            | Operation::Rewrite { .. }
            | Operation::Merge { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. } => {
                Err(self.retryable_conflict_err(other_transaction, other_version))
            }
            Operation::Overwrite { .. }
            | Operation::Restore { .. }
            | Operation::Project { .. }
            | Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
        }
    }

    fn check_restore_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            Operation::Append { .. }
            | Operation::Delete { .. }
            | Operation::Overwrite { .. }
            | Operation::CreateIndex { .. }
            | Operation::Rewrite { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. }
            | Operation::Merge { .. }
            | Operation::Restore { .. }
            | Operation::ReserveFragments { .. }
            | Operation::UpdateBases { .. }
            | Operation::Update { .. }
            | Operation::Project { .. }
            | Operation::Clone { .. }
            | Operation::UpdateConfig { .. } => Ok(()),
            Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
        }
    }

    fn check_reserve_fragments_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            Operation::Overwrite { .. } | Operation::Restore { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
            Operation::Append { .. }
            | Operation::Delete { .. }
            | Operation::CreateIndex { .. }
            | Operation::Rewrite { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. }
            | Operation::Merge { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Update { .. }
            | Operation::Project { .. }
            | Operation::Clone { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateMemWalState { .. }
            | Operation::UpdateBases { .. } => Ok(()),
        }
    }

    fn check_project_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        match &other_transaction.operation {
            // Project is compatible with anything that doesn't change the schema
            Operation::Append { .. }
            | Operation::Update { .. }
            | Operation::Delete { .. }
            | Operation::UpdateConfig { .. }
            | Operation::CreateIndex { .. }
            | Operation::DataReplacement { .. }
            | Operation::DataOverlay { .. }
            | Operation::Rewrite { .. }
            | Operation::Clone { .. }
            | Operation::ReserveFragments { .. }
            | Operation::UpdateBases { .. } => Ok(()),
            Operation::Merge { .. } | Operation::Project { .. } => {
                // Need to recompute the schema
                Err(self.retryable_conflict_err(other_transaction, other_version))
            }
            Operation::Overwrite { .. }
            | Operation::Restore { .. }
            | Operation::UpdateMemWalState { .. } => {
                Err(self.incompatible_conflict_err(other_transaction, other_version))
            }
        }
    }

    fn check_update_config_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::UpdateConfig {
            schema_metadata_updates,
            field_metadata_updates,
            ..
        } = &self.transaction.operation
        {
            match &other_transaction.operation {
                Operation::Overwrite { .. } => {
                    // Updates to schema metadata or field metadata conflict with any kind
                    // of overwrite.
                    if schema_metadata_updates.is_some()
                        || !field_metadata_updates.is_empty()
                        || self
                            .transaction
                            .operation
                            .upsert_key_conflict(&other_transaction.operation)
                    {
                        Err(self.incompatible_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::UpdateConfig { .. } => {
                    if self
                        .transaction
                        .operation
                        .upsert_key_conflict(&other_transaction.operation)
                        || self
                            .transaction
                            .operation
                            .modifies_same_metadata(&other_transaction.operation)
                    {
                        Err(self.incompatible_conflict_err(other_transaction, other_version))
                    } else {
                        Ok(())
                    }
                }
                Operation::Append { .. }
                | Operation::Clone { .. }
                | Operation::Delete { .. }
                | Operation::CreateIndex { .. }
                | Operation::Rewrite { .. }
                | Operation::DataReplacement { .. }
                | Operation::DataOverlay { .. }
                | Operation::Merge { .. }
                | Operation::Restore { .. }
                | Operation::ReserveFragments { .. }
                | Operation::Update { .. }
                | Operation::Project { .. }
                | Operation::UpdateMemWalState { .. }
                | Operation::UpdateBases { .. } => Ok(()),
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_update_mem_wal_state_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::UpdateMemWalState {
            compacted_sstables: self_compacted_sstables,
        } = &self.transaction.operation
        {
            match &other_transaction.operation {
                Operation::UpdateMemWalState {
                    compacted_sstables: other_compacted_sstables,
                } => {
                    // Two UpdateMemWalState transactions conflict if they're updating
                    // the same shard's compacted SSTable
                    self.check_compacted_sstables_conflict(
                        other_compacted_sstables,
                        self_compacted_sstables,
                        other_transaction,
                        other_version,
                    )
                }
                Operation::Update {
                    compacted_sstables: other_compacted_sstables,
                    ..
                } => {
                    // Update transactions with compacted_sstables can conflict
                    self.check_compacted_sstables_conflict(
                        other_compacted_sstables,
                        self_compacted_sstables,
                        other_transaction,
                        other_version,
                    )
                }
                Operation::CreateIndex { new_indices, .. } => {
                    // Check if CreateIndex has a MemWalIndex with compacted_sstables
                    if let Some(mem_wal_idx) = new_indices
                        .iter()
                        .find(|idx| idx.name == MEM_WAL_INDEX_NAME)
                    {
                        let details = load_mem_wal_index_details(mem_wal_idx.clone())?;
                        self.check_compacted_sstables_conflict(
                            &details.compacted_sstables,
                            self_compacted_sstables,
                            other_transaction,
                            other_version,
                        )
                    } else {
                        Ok(())
                    }
                }
                Operation::UpdateConfig { .. }
                | Operation::Rewrite { .. }
                | Operation::ReserveFragments { .. }
                | Operation::UpdateBases { .. } => Ok(()),
                Operation::Append { .. }
                | Operation::Overwrite { .. }
                | Operation::Delete { .. }
                | Operation::DataReplacement { .. }
                | Operation::DataOverlay { .. }
                | Operation::Merge { .. }
                | Operation::Restore { .. }
                | Operation::Clone { .. }
                | Operation::Project { .. } => {
                    Err(self.incompatible_conflict_err(other_transaction, other_version))
                }
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_add_bases_txn(
        &mut self,
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        if let Operation::UpdateBases { new_bases } = &self.transaction.operation {
            match &other_transaction.operation {
                Operation::UpdateBases {
                    new_bases: committed_bases,
                } => {
                    // Check if any of the bases being added conflict with committed bases
                    for new_base in new_bases {
                        for committed_base in committed_bases {
                            // Check for ID conflicts (if both have non-zero IDs)
                            if new_base.id != 0
                                && committed_base.id != 0
                                && new_base.id == committed_base.id
                            {
                                return Err(self
                                    .incompatible_conflict_err(other_transaction, other_version));
                            }
                            // Check for name conflicts
                            if new_base.name == committed_base.name && new_base.name.is_some() {
                                return Err(self
                                    .incompatible_conflict_err(other_transaction, other_version));
                            }
                            // Check for path conflicts
                            if new_base.path == committed_base.path {
                                return Err(self
                                    .incompatible_conflict_err(other_transaction, other_version));
                            }
                        }
                    }
                    Ok(())
                }
                // UpdateBases doesn't conflict with data operations
                _ => Ok(()),
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    fn check_compacted_sstables_conflict(
        &self,
        committed: &[CompactedSsTable],
        to_commit: &[CompactedSsTable],
        other_transaction: &Transaction,
        other_version: u64,
    ) -> Result<()> {
        // Check if any shard has conflicting updates
        for committed_mg in committed {
            for to_commit_mg in to_commit {
                if committed_mg.shard_id == to_commit_mg.shard_id {
                    // Same shard being updated
                    // If committed >= to_commit, the SSTable is already compacted
                    // or superseded, so abort without retry.
                    // If committed < to_commit, can retry with new state
                    if committed_mg.generation >= to_commit_mg.generation {
                        return Err(
                            self.incompatible_conflict_err(other_transaction, other_version)
                        );
                    } else {
                        return Err(self.retryable_conflict_err(other_transaction, other_version));
                    }
                }
            }
        }
        Ok(())
    }

    /// Writes
    pub async fn finish(self, dataset: &Dataset) -> Result<Transaction> {
        match &self.transaction.operation {
            Operation::Delete { .. } | Operation::Update { .. } => {
                self.finish_delete_update(dataset).await
            }
            Operation::CreateIndex { .. } => self.finish_create_index(dataset).await,
            Operation::Rewrite { .. } => self.finish_rewrite(dataset).await,
            Operation::DataOverlay { .. } => self.finish_data_overlay(dataset).await,
            Operation::Append { .. }
            | Operation::Overwrite { .. }
            | Operation::DataReplacement { .. }
            | Operation::Merge { .. }
            | Operation::Restore { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Project { .. }
            | Operation::Clone { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateMemWalState { .. }
            | Operation::UpdateBases { .. } => Ok(self.transaction),
        }
    }

    async fn finish_delete_update(mut self, dataset: &Dataset) -> Result<Transaction> {
        if self
            .initial_fragments
            .iter()
            .any(|(_, (_, needs_rewrite))| *needs_rewrite)
        {
            if let Some(affected_rows) = self.affected_rows {
                // Then we do the rebase

                // 1. Load the deletion files that need a rewrite.
                // 2. Validate there is no overlap with the affected rows. (if there is, return retryable conflict error)
                // 3. Write out new deletion files with existing deletes | affected rows.
                // 4. Update the transaction with the new deletion files.

                let fragments_ids_to_rewrite = self
                    .initial_fragments
                    .iter()
                    .filter_map(|(_, (fragment, needs_rewrite))| {
                        if *needs_rewrite {
                            Some(fragment.id)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                // We are rewriting the deletion files on the *current* dataset.
                let files_to_rewrite = dataset
                    .fragments()
                    .as_slice()
                    .iter()
                    .filter_map(|fragment| {
                        if fragments_ids_to_rewrite.contains(&fragment.id) {
                            Some((fragment.id, fragment.deletion_file.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let existing_deletion_vecs = futures::stream::iter(files_to_rewrite)
                    .map(|(fragment_id, deletion_file)| async move {
                        read_dataset_deletion_file(
                            dataset,
                            fragment_id,
                            &deletion_file.expect("there should be a deletion file"),
                        )
                        .await
                        .map(|dv| (fragment_id, dv))
                    })
                    .buffered(dataset.object_store.as_ref().io_parallelism())
                    .try_collect::<Vec<_>>()
                    .await?;

                // Check for row-level conflicts
                let mut existing_deletions = RowAddrTreeMap::new();
                for (fragment_id, deletion_vec) in existing_deletion_vecs {
                    existing_deletions
                        .insert_bitmap(fragment_id as u32, deletion_vec.as_ref().into());
                }
                let conflicting_rows = existing_deletions.clone() & affected_rows.clone();
                if conflicting_rows.len().map(|v| v > 0).unwrap_or(true) {
                    let sample_addressed = conflicting_rows
                        .row_addrs()
                        .unwrap()
                        .take(5)
                        .collect::<Vec<_>>();
                    return Err(crate::Error::retryable_commit_conflict_source(dataset.manifest.version, format!(
                        "This {} transaction was preempted by concurrent transaction {} (both modified rows at addresses {:?}). Please retry",
                        self.transaction.uuid,
                        dataset.manifest.version,
                        sample_addressed.as_slice()
                    )
                        .into()));
                }

                let merged = existing_deletions.clone() | affected_rows.clone();

                let mut new_deleted_frag_ids = Vec::new();
                let mut new_deletion_files = HashMap::with_capacity(fragments_ids_to_rewrite.len());
                for fragment_id in fragments_ids_to_rewrite.iter() {
                    let dv = DeletionVector::from(
                        merged
                            .get_fragment_bitmap(*fragment_id as u32)
                            .unwrap()
                            .clone(),
                    );
                    // If we've deleted all rows in the fragment, we can delete it.
                    // It's acceptable if we don't handle it here, as the commit step
                    // can handle it later. Though it should be rare that physical_rows
                    // is missing.
                    if let Some(physical_rows) = self
                        .initial_fragments
                        .get(fragment_id)
                        .and_then(|(fragment, _)| fragment.physical_rows)
                        && dv.len() == physical_rows
                    {
                        new_deleted_frag_ids.push(*fragment_id);
                        continue;
                    }

                    let new_deletion_file = write_deletion_file(
                        &dataset.base,
                        *fragment_id,
                        dataset.manifest.version,
                        &dv,
                        dataset.object_store.as_ref(),
                    )
                    .await?;

                    // Make sure this is available in the cache for future conflict resolution.
                    let deletion_file = new_deletion_file.as_ref().unwrap();
                    let key = crate::session::caches::DeletionFileKey {
                        fragment_id: *fragment_id,
                        deletion_file,
                    };
                    dataset
                        .metadata_cache
                        .insert_with_key(&key, Arc::new(dv))
                        .await;

                    // TODO: also cleanup the old deletion file.
                    new_deletion_files.insert(*fragment_id, new_deletion_file);
                }

                match &mut self.transaction.operation {
                    Operation::Update {
                        updated_fragments,
                        removed_fragment_ids,
                        ..
                    }
                    | Operation::Delete {
                        updated_fragments,
                        deleted_fragment_ids: removed_fragment_ids,
                        ..
                    } => {
                        for updated in updated_fragments {
                            if let Some(new_deletion_file) = new_deletion_files.get(&updated.id) {
                                updated.deletion_file = new_deletion_file.clone();
                            }
                        }
                        removed_fragment_ids.extend(new_deleted_frag_ids);
                    }
                    _ => {}
                }

                Ok(Transaction {
                    read_version: dataset.manifest.version,
                    ..self.transaction
                })
            } else {
                // We shouldn't hit this.
                Err(crate::Error::internal(
                    "We have a transaction that needs to be rebased, but we don't have any affected rows.",
                ))
            }
        } else {
            Ok(Transaction {
                read_version: dataset.manifest.version,
                ..self.transaction
            })
        }
    }

    /// Verify no concurrent row-moving Update dropped the values of any cell
    /// this overlay covers. `check_data_overlay_txn` flags (via the
    /// `initial_fragments` needs-check bool) each overlaid fragment on which a
    /// concurrent RewriteRows update relocated rows; here we read the deletion
    /// vectors and conflict only when the moved rows intersect the overlay's
    /// coverage.
    ///
    /// The moved rows are computed as the current deletion vector minus the
    /// read-time one. In the rare case where both a concurrent Delete and a
    /// concurrent Update touched the same flagged fragment, the Delete's rows are
    /// also counted and may trigger an unnecessary retry — never data loss. Pure
    /// concurrent deletes leave the fragment unflagged and are not examined here.
    async fn finish_data_overlay(self, dataset: &Dataset) -> Result<Transaction> {
        let fragments_to_check: HashSet<u64> = self
            .initial_fragments
            .iter()
            .filter_map(|(id, (_, needs_check))| needs_check.then_some(*id))
            .collect();
        if fragments_to_check.is_empty() {
            return Ok(Transaction {
                read_version: dataset.manifest.version,
                ..self.transaction
            });
        }

        // Coverage (physical offsets, unioned across fields) per flagged fragment.
        let Operation::DataOverlay { groups } = &self.transaction.operation else {
            return Err(wrong_operation_err(&self.transaction.operation));
        };
        let mut coverage_by_fragment: HashMap<u64, RoaringBitmap> = HashMap::new();
        for group in groups {
            if !fragments_to_check.contains(&group.fragment_id) {
                continue;
            }
            *coverage_by_fragment.entry(group.fragment_id).or_default() |=
                overlay_group_coverage(group);
        }

        for (fragment_id, coverage) in coverage_by_fragment {
            let Some(current_fragment) = dataset
                .fragments()
                .as_slice()
                .iter()
                .find(|f| f.id == fragment_id)
            else {
                // The fragment is gone entirely; the overlay is orphaned.
                return Err(crate::Error::retryable_commit_conflict_source(
                    dataset.manifest.version,
                    format!(
                        "This {} transaction was preempted: overlaid fragment {} was removed by a concurrent transaction. Please retry.",
                        self.transaction.uuid, fragment_id
                    )
                    .into(),
                ));
            };
            let current_deletions =
                read_fragment_deletion_bitmap(dataset, current_fragment).await?;
            let initial_deletions = match self.initial_fragments.get(&fragment_id) {
                Some((initial_fragment, _)) => {
                    read_fragment_deletion_bitmap(dataset, initial_fragment).await?
                }
                None => RoaringBitmap::new(),
            };
            let moved_rows = &current_deletions - &initial_deletions;
            let conflicting = &moved_rows & &coverage;
            if !conflicting.is_empty() {
                let sample: Vec<u32> = conflicting.iter().take(5).collect();
                return Err(crate::Error::retryable_commit_conflict_source(
                    dataset.manifest.version,
                    format!(
                        "This {} transaction was preempted by a concurrent update that moved overlaid rows on fragment {} (offsets {:?}). Please retry.",
                        self.transaction.uuid, fragment_id, sample.as_slice()
                    )
                    .into(),
                ));
            }
        }

        Ok(Transaction {
            read_version: dataset.manifest.version,
            ..self.transaction
        })
    }

    async fn finish_create_index(mut self, dataset: &Dataset) -> Result<Transaction> {
        if let Operation::CreateIndex {
            new_indices,
            removed_indices,
        } = &mut self.transaction.operation
        {
            // Handle FRAG_REUSE_INDEX rebasing
            let has_frag_reuse = new_indices
                .iter()
                .any(|idx| idx.name == FRAG_REUSE_INDEX_NAME);

            if has_frag_reuse && !self.conflicting_frag_reuse_indices.is_empty() {
                // had at least 1 previous rewrite conflict
                // get the max reuse version from each run to be added to the cleaned up index
                let mut max_versions =
                    Vec::with_capacity(self.conflicting_frag_reuse_indices.len());
                for committed_fri in &self.conflicting_frag_reuse_indices {
                    let committed_fri_details = Arc::try_unwrap(
                        load_frag_reuse_index_details(dataset, committed_fri)
                            .await
                            .unwrap(),
                    )
                    .unwrap();
                    let max_version = committed_fri_details
                        .versions
                        .into_iter()
                        .max_by_key(|v| v.dataset_version)
                        .unwrap();
                    max_versions.push(max_version);
                }

                // there should be only 1 frag_reuse_index in new indices
                let new_fri = &new_indices[0];
                let mut new_fri_details = Arc::try_unwrap(
                    load_frag_reuse_index_details(dataset, new_fri)
                        .await
                        .unwrap(),
                )
                .unwrap();
                new_fri_details.versions.extend(max_versions);

                let new_frag_bitmap = new_fri_details.new_frag_bitmap();

                let new_frag_reuse_index_meta = build_frag_reuse_index_metadata(
                    dataset,
                    Some(new_fri),
                    new_fri_details,
                    new_frag_bitmap,
                )
                .await?;

                new_indices.retain(|idx| idx.name != FRAG_REUSE_INDEX_NAME);
                new_indices.push(new_frag_reuse_index_meta);
            }

            // Handle MEM_WAL_INDEX rebasing
            let has_mem_wal = new_indices.iter().any(|idx| idx.name == MEM_WAL_INDEX_NAME);

            if has_mem_wal && !self.conflicting_mem_wal_compacted_sstables.is_empty() {
                let pos = new_indices
                    .iter()
                    .position(|idx| idx.name == MEM_WAL_INDEX_NAME)
                    .unwrap();

                let current_meta = new_indices.remove(pos);
                let mut details = load_mem_wal_index_details(current_meta)?;

                // Reconcile conflicting compacted_sstables by keeping each shard's higher generation.
                // We own self so we can consume conflicting_mem_wal_compacted_sstables directly
                for new_sstable in self.conflicting_mem_wal_compacted_sstables {
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

                let new_meta = new_mem_wal_index_meta(dataset.manifest.version, details)?;
                new_indices.push(new_meta);
            }

            for singleton_name in [FRAG_REUSE_INDEX_NAME, MEM_WAL_INDEX_NAME] {
                if new_indices.iter().any(|idx| idx.name == singleton_name) {
                    for existing_idx in dataset
                        .load_indices()
                        .await?
                        .iter()
                        .filter(|idx| idx.name == singleton_name)
                        .cloned()
                    {
                        if !removed_indices
                            .iter()
                            .any(|removed_idx| removed_idx.uuid == existing_idx.uuid)
                        {
                            removed_indices.push(existing_idx);
                        }
                    }
                }
            }

            Ok(self.transaction)
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }

    async fn finish_rewrite(mut self, dataset: &Dataset) -> Result<Transaction> {
        if let Operation::Rewrite {
            frag_reuse_index, ..
        } = &mut self.transaction.operation
        {
            if let Some(new_fri) = frag_reuse_index {
                if self.conflicting_frag_reuse_indices.is_empty() {
                    return Ok(self.transaction);
                }

                let mut new_fri_details = Arc::try_unwrap(
                    load_frag_reuse_index_details(dataset, new_fri)
                        .await
                        .unwrap(),
                )
                .unwrap();
                let mut min_dataset_version = new_fri_details
                    .versions
                    .iter()
                    .map(|v| v.dataset_version)
                    .min()
                    .unwrap();
                for committed_fri in self.conflicting_frag_reuse_indices.into_iter() {
                    let committed_fri_details =
                        load_frag_reuse_index_details(dataset, &committed_fri)
                            .await
                            .unwrap();
                    let committed_min_dataset_version = committed_fri_details
                        .versions
                        .iter()
                        .map(|v| v.dataset_version)
                        .min();

                    // For example, if we have new_fri has reuse versions [1, 2, 3]
                    // If committed_fri has versions [2], that means 1 is cleaned up,
                    // then [2, 3] should be retained in the new_fri.
                    // If committed_fri is empty, that means everything is cleaned up.
                    // then only the last item in committed_fri should be retained, which is [3].
                    // Note that this is under the assumption that the sequence of
                    // conflicting_frag_reuse_indices all come from frag_reuse_index cleanup rebase.
                    match committed_min_dataset_version {
                        Some(committed_min_dataset_version) => {
                            if committed_min_dataset_version > min_dataset_version {
                                min_dataset_version = committed_min_dataset_version;
                            }
                        }
                        None => {
                            min_dataset_version = new_fri_details
                                .versions
                                .iter()
                                .map(|v| v.dataset_version)
                                .max()
                                .unwrap();
                        }
                    }
                }

                new_fri_details
                    .versions
                    .retain(|v| v.dataset_version >= min_dataset_version);
                let new_frag_bitmap = new_fri_details.new_frag_bitmap();

                let new_frag_reuse_index_meta = build_frag_reuse_index_metadata(
                    dataset,
                    Some(new_fri),
                    new_fri_details,
                    new_frag_bitmap,
                )
                .await?;

                *frag_reuse_index = Some(new_frag_reuse_index_meta);
                Ok(self.transaction)
            } else {
                Ok(self.transaction)
            }
        } else {
            Err(wrong_operation_err(&self.transaction.operation))
        }
    }
}

async fn initial_fragments_for_rebase(
    dataset: &Dataset,
    transaction: &Transaction,
    modified_fragment_ids: &HashSet<u64>,
) -> HashMap<u64, (Fragment, bool)> {
    if modified_fragment_ids.is_empty() {
        return HashMap::new();
    }

    let dataset = if dataset.manifest.version != transaction.read_version {
        Cow::Owned(
            dataset
                .checkout_version(transaction.read_version)
                .await
                .unwrap(),
        )
    } else {
        Cow::Borrowed(dataset)
    };

    dataset
        .fragments()
        .iter()
        .filter(|fragment| {
            // Check if the fragment is modified by the transaction.
            modified_fragment_ids.contains(&fragment.id)
        })
        .map(|fragment| (fragment.id, (fragment.clone(), false)))
        .collect::<HashMap<_, _>>()
}

/// Read a fragment's deletion vector as a bitmap of physical offsets, or an
/// empty bitmap when the fragment has no deletion file.
async fn read_fragment_deletion_bitmap(
    dataset: &Dataset,
    fragment: &Fragment,
) -> Result<RoaringBitmap> {
    match &fragment.deletion_file {
        Some(deletion_file) => {
            let dv = read_dataset_deletion_file(dataset, fragment.id, deletion_file).await?;
            Ok(RoaringBitmap::from(dv.as_ref()))
        }
        None => Ok(RoaringBitmap::new()),
    }
}

/// The physical offsets a group's overlays cover, unioned across every overlay
/// and every field. This is the set of cells whose values the overlay supplies,
/// used to test whether a concurrent row-moving Update actually invalidates the
/// overlay.
fn overlay_group_coverage(group: &DataOverlayGroup) -> RoaringBitmap {
    let mut union = RoaringBitmap::new();
    for overlay in &group.overlays {
        match &overlay.coverage {
            OverlayCoverage::Shared(bitmap) => union |= bitmap.as_ref(),
            OverlayCoverage::PerField(bitmaps) => {
                for bitmap in bitmaps {
                    union |= bitmap.as_ref();
                }
            }
        }
    }
    union
}

fn wrong_operation_err(op: &Operation) -> Error {
    Error::internal(format!("function called against a wrong operation: {}", op))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZero, sync::Arc};

    use crate::dataset::transaction::UpdateMode::{RewriteColumns, RewriteRows};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lance_core::Error;
    use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
    use lance_io::assert_io_eq;
    use uuid::Uuid;

    use lance_table::format::IndexMetadata;
    use lance_table::io::deletion::{deletion_file_path, read_deletion_file};

    use super::*;
    use crate::dataset::transaction::{DataReplacementGroup, RewriteGroup};
    use crate::dataset::write::WriteMode;
    use crate::session::caches::DeletionFileKey;
    use crate::{
        dataset::{CommitBuilder, InsertBuilder, WriteParams},
        io,
    };
    use lance_table::format::DataFile;

    async fn test_dataset(num_rows: usize, num_fragments: usize) -> Dataset {
        let write_params = WriteParams {
            max_rows_per_file: num_rows / num_fragments,
            ..Default::default()
        };
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
                    0, num_rows,
                ))),
            ],
        )
        .unwrap();

        InsertBuilder::new("memory://")
            .with_params(&write_params)
            .execute(vec![data])
            .await
            .unwrap()
    }

    /// Helper function for tests to create UpdateConfig operations using old-style parameters
    #[cfg(test)]
    fn create_update_config_for_test(
        upsert_values: Option<HashMap<String, String>>,
        delete_keys: Option<Vec<String>>,
        schema_metadata: Option<HashMap<String, String>>,
        field_metadata: Option<HashMap<u32, HashMap<String, String>>>,
    ) -> Operation {
        use crate::dataset::transaction::{
            translate_config_updates, translate_schema_metadata_updates,
        };

        let config_updates = if let Some(upsert) = &upsert_values {
            if let Some(delete) = &delete_keys {
                Some(translate_config_updates(upsert, delete))
            } else {
                Some(translate_config_updates(upsert, &[]))
            }
        } else {
            delete_keys
                .as_ref()
                .map(|delete| translate_config_updates(&HashMap::new(), delete))
        };

        let schema_metadata_updates = schema_metadata
            .as_ref()
            .map(translate_schema_metadata_updates);

        let field_metadata_updates = field_metadata
            .unwrap_or_default()
            .into_iter()
            .map(|(field_id, metadata)| {
                (
                    field_id as i32,
                    translate_schema_metadata_updates(&metadata),
                )
            })
            .collect();

        Operation::UpdateConfig {
            config_updates,
            table_metadata_updates: None,
            schema_metadata_updates,
            field_metadata_updates,
        }
    }

    #[tokio::test]
    async fn test_non_overlapping_rebase_delete_update() {
        let dataset = test_dataset(5, 5).await;
        let operation = Operation::Update {
            updated_fragments: vec![Fragment::new(0)],
            removed_fragment_ids: vec![],
            new_fragments: vec![],
            fields_modified: vec![],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: None,
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let transaction = Transaction::new_from_version(1, operation);
        let other_operations = [
            Operation::Update {
                updated_fragments: vec![Fragment::new(1)],
                removed_fragment_ids: vec![2],
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            Operation::Delete {
                deleted_fragment_ids: vec![3],
                updated_fragments: vec![],
                predicate: "a > 0".to_string(),
            },
            Operation::Update {
                removed_fragment_ids: vec![],
                updated_fragments: vec![Fragment::new(4)],
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
        ];
        let other_transactions = other_operations.map(|op| Transaction::new_from_version(2, op));
        let mut rebase = TransactionRebase::try_new(&dataset, transaction.clone(), None)
            .await
            .unwrap();

        dataset.object_store.as_ref().io_stats_incremental(); // reset
        for (other_version, other_transaction) in other_transactions.iter().enumerate() {
            rebase
                .check_txn(other_transaction, other_version as u64)
                .unwrap();
            let io_stats = dataset.object_store.as_ref().io_stats_incremental();
            assert_io_eq!(io_stats, read_iops, 0);
            assert_io_eq!(io_stats, write_iops, 0);
        }

        let expected_transaction = Transaction {
            // This doesn't really exercise it, since the other transactions
            // haven't been applied yet, but just doing this for completeness.
            read_version: dataset.manifest.version,
            ..transaction
        };
        let rebased_transaction = rebase.finish(&dataset).await.unwrap();
        assert_eq!(rebased_transaction, expected_transaction);
        // We didn't need to do any IO, so the stats should be 0.
        let io_stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(io_stats, read_iops, 0);
        assert_io_eq!(io_stats, write_iops, 0);
    }

    async fn apply_deletion(
        delete_rows: &[u32],
        fragment: &mut Fragment,
        dataset: &Dataset,
    ) -> Fragment {
        let mut current_deletions = if let Some(deletion_file) = &fragment.deletion_file {
            read_deletion_file(
                fragment.id,
                deletion_file,
                // Reference deletion file should never enter this apply_deletion. So base path is fine.
                &dataset.base,
                dataset.object_store.as_ref(),
            )
            .await
            .unwrap()
        } else {
            DeletionVector::default()
        };

        current_deletions.extend(delete_rows.iter().copied());

        fragment.deletion_file = write_deletion_file(
            &dataset.base,
            fragment.id,
            dataset.manifest.version,
            &current_deletions,
            dataset.object_store.as_ref(),
        )
        .await
        .unwrap();

        let deletion_file = fragment.deletion_file.as_ref().unwrap();
        let key = DeletionFileKey {
            fragment_id: fragment.id,
            deletion_file,
        };
        dataset
            .metadata_cache
            .insert_with_key(&key, Arc::new(current_deletions))
            .await;

        fragment.clone()
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_non_conflicting_rebase_delete_update() {
        // 5 rows, all in one fragment. Each transaction modifies a different row.
        let mut dataset = test_dataset(5, 1).await;
        let mut fragment = dataset.fragments().as_slice()[0].clone();

        // Other operations modify the 1st, 2nd, and 3rd rows sequentially.
        let sample_file = Fragment::new(0)
            .with_file(
                "path1",
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                NonZero::new(10),
            )
            .with_physical_rows(3);
        let operations = [
            Operation::Update {
                updated_fragments: vec![apply_deletion(&[0], &mut fragment, &dataset).await],
                removed_fragment_ids: vec![],
                new_fragments: vec![sample_file.clone()],
                fields_modified: vec![],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            Operation::Delete {
                updated_fragments: vec![apply_deletion(&[1], &mut fragment, &dataset).await],
                deleted_fragment_ids: vec![],
                predicate: "a > 0".to_string(),
            },
            Operation::Update {
                updated_fragments: vec![apply_deletion(&[2], &mut fragment, &dataset).await],
                removed_fragment_ids: vec![],
                new_fragments: vec![sample_file],
                fields_modified: vec![],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
        ];
        let transactions =
            operations.map(|op| Transaction::new_from_version(dataset.manifest.version, op));

        for (i, transaction) in transactions.iter().enumerate() {
            let previous_transactions = transactions.iter().take(i).cloned().collect::<Vec<_>>();

            let affected_rows = RowAddrTreeMap::from_iter([i as u64]);
            let mut rebase =
                TransactionRebase::try_new(&dataset, transaction.clone(), Some(&affected_rows))
                    .await
                    .unwrap();

            dataset.object_store.as_ref().io_stats_incremental(); // reset
            for (other_version, other_transaction) in previous_transactions.iter().enumerate() {
                rebase
                    .check_txn(other_transaction, other_version as u64)
                    .unwrap();
                let io_stats = dataset.object_store.as_ref().io_stats_incremental();
                assert_io_eq!(io_stats, read_iops, 0);
                assert_io_eq!(io_stats, write_iops, 0);
            }

            // First iteration, we don't need to rewrite the deletion file.
            let expected_rewrite = i > 0;

            let rebased_transaction = rebase.finish(&dataset).await.unwrap();
            assert_eq!(rebased_transaction.read_version, dataset.manifest.version);

            let io_stats = dataset.object_store.as_ref().io_stats_incremental();
            if expected_rewrite {
                // Read the current deletion file, and write the new one.
                assert_io_eq!(io_stats, read_iops, 0, "deletion file should be cached");
                assert_io_eq!(io_stats, write_iops, 1, "write one deletion file");

                // TODO: The old deletion file should be gone.
                // This can be done later, as it will be cleaned up by the
                // background cleanup process for now.
                // let original_fragment = match &original_transaction.operation {
                //     Operation::Update {
                //         updated_fragments, ..
                //     }
                //     | Operation::Delete {
                //         updated_fragments, ..
                //     } => updated_fragments[0].clone(),
                //     _ => {
                //         panic!("Expected an update or delete operation");
                //     }
                // };
                // let old_path = deletion_file_path(
                //     &dataset.base,
                //     original_fragment.id,
                //     original_fragment.deletion_file.as_ref().unwrap(),
                // );
                // assert!(!dataset.object_store.as_ref().exists(&old_path).await.unwrap());
                // The new deletion file should exist.
                let final_fragment = match &rebased_transaction.operation {
                    Operation::Update {
                        updated_fragments, ..
                    }
                    | Operation::Delete {
                        updated_fragments, ..
                    } => updated_fragments[0].clone(),
                    _ => {
                        panic!("Expected an update or delete operation");
                    }
                };
                let new_path = deletion_file_path(
                    &dataset.base,
                    final_fragment.id,
                    final_fragment.deletion_file.as_ref().unwrap(),
                );
                assert!(
                    dataset
                        .object_store
                        .as_ref()
                        .exists(&new_path)
                        .await
                        .unwrap()
                );

                assert_io_eq!(io_stats, num_stages, 1);
            } else {
                // No IO should have happened.
                assert_io_eq!(io_stats, read_iops, 0);
                assert_io_eq!(io_stats, write_iops, 0);
            }

            dataset = CommitBuilder::new(Arc::new(dataset))
                .execute(rebased_transaction)
                .await
                .unwrap();
        }
    }

    /// Validate we get a conflict error when rebasing `operation` on top of `other`.
    #[tokio::test]
    #[rstest::rstest]
    async fn test_conflicting_rebase(
        #[values("update_full", "update_partial", "delete_full", "delete_partial")] ours: &str,
        #[values("update_full", "update_partial", "delete_full", "delete_partial")] other: &str,
    ) {
        // 5 rows, all in one fragment. Each transaction modifies the same row.
        let dataset = test_dataset(5, 1).await;
        let mut fragment = dataset.fragments().as_slice()[0].clone();

        let sample_file = Fragment::new(0)
            .with_file(
                "path1",
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                NonZero::new(10),
            )
            .with_physical_rows(3);

        let operations = [
            (
                "update_full",
                Operation::Update {
                    updated_fragments: vec![],
                    removed_fragment_ids: vec![0],
                    new_fragments: vec![sample_file.clone()],
                    fields_modified: vec![],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: None,
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
            ),
            (
                "update_partial",
                Operation::Update {
                    updated_fragments: vec![apply_deletion(&[0], &mut fragment, &dataset).await],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![sample_file.clone()],
                    fields_modified: vec![],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: None,
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
            ),
            (
                "delete_full",
                Operation::Delete {
                    updated_fragments: vec![],
                    deleted_fragment_ids: vec![0],
                    predicate: "a > 0".to_string(),
                },
            ),
            (
                "delete_partial",
                Operation::Delete {
                    updated_fragments: vec![apply_deletion(&[0], &mut fragment, &dataset).await],
                    deleted_fragment_ids: vec![],
                    predicate: "a > 0".to_string(),
                },
            ),
        ];

        let operation = operations
            .iter()
            .find(|(name, _)| *name == ours)
            .unwrap()
            .1
            .clone();
        let other_op = operations
            .iter()
            .find(|(name, _)| *name == other)
            .unwrap()
            .1
            .clone();

        let other_txn = Transaction::new_from_version(dataset.manifest.version, other_op);
        let txn = Transaction::new_from_version(dataset.manifest.version, operation);

        // Can apply first transaction to create the conflict
        let latest_dataset = CommitBuilder::new(Arc::new(dataset.clone()))
            .execute(other_txn.clone())
            .await
            .unwrap();

        let affected_rows = RowAddrTreeMap::from_iter([0]);

        dataset.object_store.as_ref().io_stats_incremental(); // reset
        let mut rebase = TransactionRebase::try_new(&dataset, txn.clone(), Some(&affected_rows))
            .await
            .unwrap();

        let io_stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(io_stats, read_iops, 0);
        assert_io_eq!(io_stats, write_iops, 0);

        let res = rebase.check_txn(&other_txn, 1);
        if other.ends_with("full") || ours.ends_with("full") {
            // If the other transaction fully deleted a fragment, we can error early.
            assert!(matches!(
                res,
                Err(crate::Error::RetryableCommitConflict { .. })
            ));
            return;
        } else {
            assert!(res.is_ok());
        }

        assert_eq!(
            rebase
                .initial_fragments
                .iter()
                .map(|(id, (_, needs_rewrite))| (*id, *needs_rewrite))
                .collect::<Vec<_>>(),
            vec![(0, true)],
        );

        let io_stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(io_stats, read_iops, 0);
        assert_io_eq!(io_stats, write_iops, 0);

        let res = rebase.finish(&latest_dataset).await;
        assert!(matches!(
            res,
            Err(crate::Error::RetryableCommitConflict { .. })
        ));

        let io_stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(io_stats, read_iops, 0, "deletion file should be cached");
        assert_io_eq!(io_stats, write_iops, 0, "failed before writing");
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ConflictResult {
        Compatible,
        NotCompatible,
        Retryable,
    }

    #[test]
    fn test_conflicts() {
        use io::commit::conflict_resolver::tests::{ConflictResult::*, modified_fragment_ids};

        let index0 = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            name: "test".to_string(),
            fields: vec![0],
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 0,
            created_at: None, // Test index, not setting timestamp
            base_id: None,
            files: None,
        };
        let fragment0 = Fragment::new(0);
        let fragment1 = Fragment::new(1);
        let fragment2 = Fragment::new(2);
        // The transactions that will be checked against
        let other_operations = [
            Operation::Append {
                fragments: vec![fragment0.clone()],
            },
            Operation::CreateIndex {
                new_indices: vec![index0.clone()],
                removed_indices: vec![index0.clone()],
            },
            Operation::Delete {
                updated_fragments: vec![fragment0.clone()],
                deleted_fragment_ids: vec![2],
                predicate: "x > 2".to_string(),
            },
            Operation::Merge {
                fragments: vec![fragment0.clone(), fragment2.clone()],
                schema: lance_core::datatypes::Schema::default(),
            },
            Operation::Overwrite {
                fragments: vec![fragment0.clone(), fragment2.clone()],
                schema: lance_core::datatypes::Schema::default(),
                config_upsert_values: Some(HashMap::from_iter(vec![(
                    "overwrite-key".to_string(),
                    "value".to_string(),
                )])),
                initial_bases: None,
            },
            Operation::Rewrite {
                groups: vec![RewriteGroup {
                    old_fragments: vec![fragment0.clone()],
                    new_fragments: vec![fragment1.clone()],
                }],
                rewritten_indices: vec![],
                frag_reuse_index: None,
            },
            Operation::ReserveFragments { num_fragments: 3 },
            Operation::Update {
                removed_fragment_ids: vec![1],
                updated_fragments: vec![fragment0.clone()],
                new_fragments: vec![fragment2.clone()],
                fields_modified: vec![0],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            create_update_config_for_test(
                Some(HashMap::from_iter(vec![(
                    "lance.test".to_string(),
                    "value".to_string(),
                )])),
                Some(vec!["remove-key".to_string()]),
                Some(HashMap::from_iter(vec![(
                    "schema-key".to_string(),
                    "schema-value".to_string(),
                )])),
                Some(HashMap::from_iter(vec![(
                    0,
                    HashMap::from_iter(vec![("field-key".to_string(), "field-value".to_string())]),
                )])),
            ),
        ];
        let other_transactions = other_operations
            .iter()
            .map(|op| Transaction::new(0, op.clone(), None))
            .collect::<Vec<_>>();

        // Transactions and whether they are expected to conflict with each
        // of other_transactions
        let cases = [
            (
                Operation::Append {
                    fragments: vec![fragment0.clone()],
                },
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::Delete {
                    // Delete that affects fragments different from other transactions
                    updated_fragments: vec![fragment1.clone()],
                    deleted_fragment_ids: vec![],
                    predicate: "x > 2".to_string(),
                },
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::Delete {
                    // Delete that affects same fragments as other transactions
                    updated_fragments: vec![fragment0.clone(), fragment2.clone()],
                    deleted_fragment_ids: vec![],
                    predicate: "x > 2".to_string(),
                },
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Retryable,     // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Retryable,     // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::Overwrite {
                    fragments: vec![fragment0.clone(), fragment2.clone()],
                    schema: lance_core::datatypes::Schema::default(),
                    config_upsert_values: None,
                    initial_bases: None,
                },
                // Concurrent overwrites are retryable so user can decide
                // if their overwrite should still proceed.
                [
                    Compatible, // append
                    Compatible, // create index
                    Compatible, // delete
                    Compatible, // merge
                    Retryable,  // overwrite
                    Compatible, // rewrite
                    Compatible, // reserve
                    Compatible, // update
                    Compatible, // update config
                ],
            ),
            (
                Operation::CreateIndex {
                    new_indices: vec![index0.clone()],
                    removed_indices: vec![index0],
                },
                // Conflicts with row-id-changing operations and same-name CreateIndex.
                [
                    Compatible,    // append
                    Retryable,     // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Retryable,     // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    Compatible,    // update config
                ],
            ),
            (
                // Rewrite that affects different fragments
                Operation::Rewrite {
                    groups: vec![RewriteGroup {
                        old_fragments: vec![fragment1],
                        new_fragments: vec![fragment0.clone()],
                    }],
                    rewritten_indices: Vec::new(),
                    frag_reuse_index: None,
                },
                [
                    Compatible,    // append
                    Retryable,     // create index
                    Compatible,    // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                // Rewrite that affects the same fragments
                Operation::Rewrite {
                    groups: vec![RewriteGroup {
                        old_fragments: vec![fragment0.clone(), fragment2.clone()],
                        new_fragments: vec![fragment0.clone()],
                    }],
                    rewritten_indices: Vec::new(),
                    frag_reuse_index: None,
                },
                [
                    Compatible,    // append
                    Retryable,     // create index
                    Retryable,     // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Retryable,     // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::Merge {
                    fragments: vec![fragment0.clone(), fragment2.clone()],
                    schema: lance_core::datatypes::Schema::default(),
                },
                // Merge conflicts with everything except CreateIndex and ReserveFragments.
                [
                    Retryable,     // append
                    Compatible,    // create index
                    Retryable,     // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Retryable,     // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::ReserveFragments { num_fragments: 2 },
                // ReserveFragments only conflicts with Overwrite and Restore.
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    Compatible,    // update config
                ],
            ),
            (
                Operation::Update {
                    // Update that affects same fragments as other transactions
                    updated_fragments: vec![fragment0],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![fragment2],
                    fields_modified: vec![0],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: None,
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Retryable,     // delete
                    Retryable,     // merge
                    NotCompatible, // overwrite
                    Retryable,     // rewrite
                    Compatible,    // reserve
                    Retryable,     // update
                    Compatible,    // update config
                ],
            ),
            (
                // Update config that should not conflict with anything
                create_update_config_for_test(
                    Some(HashMap::from_iter(vec![(
                        "other-key".to_string(),
                        "new-value".to_string(),
                    )])),
                    None,
                    None,
                    None,
                ),
                [Compatible; 9],
            ),
            (
                // Update config that conflicts with key being upserted by other UpdateConfig operation
                create_update_config_for_test(
                    Some(HashMap::from_iter(vec![(
                        "lance.test".to_string(),
                        "new-value".to_string(),
                    )])),
                    None,
                    None,
                    None,
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    Compatible,    // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    NotCompatible, // update config
                ],
            ),
            (
                // Update config that conflicts with key being deleted by other UpdateConfig operation
                create_update_config_for_test(
                    Some(HashMap::from_iter(vec![(
                        "remove-key".to_string(),
                        "new-value".to_string(),
                    )])),
                    None,
                    None,
                    None,
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    Compatible,    // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    NotCompatible, // update config
                ],
            ),
            (
                // Delete config keys currently being deleted by other UpdateConfig operation
                create_update_config_for_test(
                    None,
                    Some(vec!["remove-key".to_string()]),
                    None,
                    None,
                ),
                [Compatible; 9],
            ),
            (
                // Delete config keys currently being upserted by other UpdateConfig operation
                create_update_config_for_test(
                    None,
                    Some(vec!["lance.test".to_string()]),
                    None,
                    None,
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    Compatible,    // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    NotCompatible, // update config
                ],
            ),
            (
                // Changing schema metadata conflicts with another update changing schema
                // metadata or with an overwrite
                create_update_config_for_test(
                    None,
                    None,
                    Some(HashMap::from_iter(vec![(
                        "schema-key".to_string(),
                        "new-value".to_string(),
                    )])),
                    None,
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    NotCompatible, // update config
                ],
            ),
            (
                // Changing field metadata conflicts with another update changing same field
                // metadata or overwrite
                create_update_config_for_test(
                    None,
                    None,
                    None,
                    Some(HashMap::from_iter(vec![(
                        0,
                        HashMap::from_iter(vec![(
                            "field_key".to_string(),
                            "field_value".to_string(),
                        )]),
                    )])),
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    NotCompatible, // update config
                ],
            ),
            (
                // Updates to different field metadata are allowed
                create_update_config_for_test(
                    None,
                    None,
                    None,
                    Some(HashMap::from_iter(vec![(
                        1,
                        HashMap::from_iter(vec![(
                            "field_key".to_string(),
                            "field_value".to_string(),
                        )]),
                    )])),
                ),
                [
                    Compatible,    // append
                    Compatible,    // create index
                    Compatible,    // delete
                    Compatible,    // merge
                    NotCompatible, // overwrite
                    Compatible,    // rewrite
                    Compatible,    // reserve
                    Compatible,    // update
                    Compatible,    // update config
                ],
            ),
        ];

        for (operation, expected_conflicts) in &cases {
            let transaction = Transaction::new(0, operation.clone(), None);
            let mut rebase = TransactionRebase {
                transaction,
                initial_fragments: HashMap::new(),
                modified_fragment_ids: modified_fragment_ids(operation).collect::<HashSet<_>>(),
                affected_rows: None,
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };

            for (other, expected_conflict) in other_transactions.iter().zip(expected_conflicts) {
                match expected_conflict {
                    Compatible => {
                        let result = rebase.check_txn(other, 1);
                        assert!(
                            result.is_ok(),
                            "Transaction {:?} should {:?} with {:?}, but was {:?}",
                            operation,
                            expected_conflict,
                            other,
                            result
                        )
                    }
                    NotCompatible => {
                        let result = rebase.check_txn(other, 1);
                        assert!(
                            matches!(result, Err(Error::IncompatibleTransaction { .. })),
                            "Transaction {:?} should be {:?} with {:?}, but was: {:?}",
                            operation,
                            expected_conflict,
                            other,
                            result
                        )
                    }
                    Retryable => {
                        let result = rebase.check_txn(other, 1);
                        assert!(
                            matches!(result, Err(Error::RetryableCommitConflict { .. })),
                            "Transaction {:?} should be {:?} with {:?}, but was {:?}",
                            operation,
                            expected_conflict,
                            other,
                            result
                        )
                    }
                }
            }
        }
    }

    #[test]
    fn test_data_overlay_conflicts() {
        use crate::dataset::transaction::{DataOverlayGroup, UpdateMode};
        use ConflictResult::*;
        use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
        use roaring::RoaringBitmap;

        // Our transaction overlays fragment 1.
        let overlay_op = |fragment_id: u64| Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id,
                overlays: vec![DataOverlayFile {
                    data_file: DataFile::new_legacy_from_fields("overlay.lance", vec![0], None),
                    coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                    committed_version: 0,
                }],
            }],
        };
        let update_removing = |removed_fragment_ids: Vec<u64>| Operation::Update {
            removed_fragment_ids,
            updated_fragments: vec![],
            new_fragments: vec![],
            fields_modified: vec![],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: None,
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let delete = |updated: Vec<Fragment>, deleted: Vec<u64>| Operation::Delete {
            updated_fragments: updated,
            deleted_fragment_ids: deleted,
            predicate: "x > 2".to_string(),
        };
        // A row-moving update (RewriteRows) relocates the updated rows into
        // new_fragments; an in-place column rewrite (RewriteColumns) leaves rows
        // where they are.
        let update_moving = |updated: Vec<Fragment>, new: Vec<Fragment>| Operation::Update {
            removed_fragment_ids: vec![],
            updated_fragments: updated,
            new_fragments: new,
            fields_modified: vec![],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteRows),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let update_rewrite_columns = |updated: Vec<Fragment>| Operation::Update {
            removed_fragment_ids: vec![],
            updated_fragments: updated,
            new_fragments: vec![],
            fields_modified: vec![0],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteColumns),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let rewrite_of = |old: &Fragment| Operation::Rewrite {
            groups: vec![RewriteGroup {
                old_fragments: vec![old.clone()],
                new_fragments: vec![],
            }],
            rewritten_indices: vec![],
            frag_reuse_index: None,
        };

        let fragment0 = Fragment::new(0);
        let fragment1 = Fragment::new(1);

        // Each case is checked against our overlay on fragment 1.
        let cases: Vec<(Operation, ConflictResult)> = vec![
            // Permissive: preserves physical offsets / leaves fragment 1 in place.
            (
                Operation::Append {
                    fragments: vec![fragment0.clone()],
                },
                Compatible,
            ),
            (
                Operation::CreateIndex {
                    new_indices: vec![],
                    removed_indices: vec![],
                },
                Compatible,
            ),
            (
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(
                        1,
                        DataFile::new_legacy_from_fields("r.lance", vec![0], None),
                    )],
                },
                Compatible,
            ),
            // Another overlay on the same fragment stacks rather than conflicts.
            (overlay_op(1), Compatible),
            // A Delete only tombstones rows (deletion vector) on fragment 1, and
            // an in-place column rewrite preserves offsets, so both are compatible.
            (delete(vec![fragment1.clone()], vec![]), Compatible),
            (update_rewrite_columns(vec![fragment1.clone()]), Compatible),
            (update_removing(vec![2]), Compatible),
            // ...but removing our overlaid fragment 1 orphans the overlay -> conflict.
            (delete(vec![], vec![1]), Retryable),
            (update_removing(vec![1]), Retryable),
            // A row-moving update re-creates the rows it touches from the
            // pre-overlay base. Whether that actually drops any overlaid cell is
            // a per-row question answered in `finish_data_overlay` (see
            // test_data_overlay_finish_conflicts_with_row_moving_update), so the
            // check itself defers rather than conflicting; a moving update on any
            // fragment is compatible at this stage.
            (
                update_moving(vec![fragment1.clone()], vec![fragment0.clone()]),
                Compatible,
            ),
            (
                update_moving(vec![fragment0.clone()], vec![fragment0.clone()]),
                Compatible,
            ),
            // Rewriting fragment 1 invalidates its physical offsets -> conflict;
            // a rewrite of a different fragment does not.
            (rewrite_of(&fragment1), Retryable),
            (rewrite_of(&fragment0), Compatible),
            // Merge rewrites the whole fragment list; Restore replaces the dataset.
            (
                Operation::Merge {
                    fragments: vec![fragment1.clone()],
                    schema: lance_core::datatypes::Schema::default(),
                },
                Retryable,
            ),
            (Operation::Restore { version: 1 }, NotCompatible),
            // Overwrite/Restore replace the dataset, and UpdateMemWalState does
            // not rebase against data operations — all hard conflicts.
            (
                Operation::Overwrite {
                    fragments: vec![fragment0.clone()],
                    schema: lance_core::datatypes::Schema::default(),
                    config_upsert_values: None,
                    initial_bases: None,
                },
                NotCompatible,
            ),
            (
                Operation::UpdateMemWalState {
                    compacted_sstables: vec![],
                },
                NotCompatible,
            ),
        ];

        for (other, expected) in cases {
            let mut rebase = TransactionRebase {
                transaction: Transaction::new(0, overlay_op(1), None),
                initial_fragments: HashMap::new(),
                modified_fragment_ids: modified_fragment_ids(&overlay_op(1))
                    .collect::<HashSet<_>>(),
                affected_rows: None,
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };
            let other_txn = Transaction::new(0, other.clone(), None);
            let result = rebase.check_txn(&other_txn, 1);
            match expected {
                Compatible => assert!(
                    result.is_ok(),
                    "overlay should be compatible with {other:?}, got {result:?}"
                ),
                Retryable => assert!(
                    matches!(result, Err(Error::RetryableCommitConflict { .. })),
                    "overlay should retryably conflict with {other:?}, got {result:?}"
                ),
                NotCompatible => assert!(
                    matches!(result, Err(Error::IncompatibleTransaction { .. })),
                    "overlay should be incompatible with {other:?}, got {result:?}"
                ),
            }
        }
    }

    #[test]
    fn test_rewrite_conflicts_with_data_overlay() {
        // Reverse direction of test_data_overlay_conflicts: our transaction is a
        // Rewrite and a concurrent DataOverlay has already committed. A rewrite
        // changes the physical row addresses of the fragments it touches, so an
        // overlay on one of those fragments is invalidated (retryable); an
        // overlay on any other fragment is unaffected.
        use crate::dataset::transaction::DataOverlayGroup;
        use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
        use roaring::RoaringBitmap;

        let overlay_on = |fragment_id: u64| Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id,
                overlays: vec![DataOverlayFile {
                    data_file: DataFile::new_legacy_from_fields("overlay.lance", vec![0], None),
                    coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                    committed_version: 0,
                }],
            }],
        };
        // Our transaction rewrites fragment 1.
        let rewrite_op = Operation::Rewrite {
            groups: vec![RewriteGroup {
                old_fragments: vec![Fragment::new(1)],
                new_fragments: vec![],
            }],
            rewritten_indices: vec![],
            frag_reuse_index: None,
        };

        for (other, expect_conflict) in [(overlay_on(1), true), (overlay_on(0), false)] {
            let mut rebase = TransactionRebase {
                transaction: Transaction::new(0, rewrite_op.clone(), None),
                initial_fragments: HashMap::new(),
                modified_fragment_ids: modified_fragment_ids(&rewrite_op).collect::<HashSet<_>>(),
                affected_rows: None,
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };
            let other_txn = Transaction::new(0, other.clone(), None);
            let result = rebase.check_txn(&other_txn, 1);
            if expect_conflict {
                assert!(
                    matches!(result, Err(Error::RetryableCommitConflict { .. })),
                    "rewrite of fragment 1 should retryably conflict with {other:?}, got {result:?}"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "rewrite of fragment 1 should not conflict with {other:?}, got {result:?}"
                );
            }
        }
    }

    #[test]
    fn test_update_conflicts_with_data_overlay() {
        // Reverse direction of test_data_overlay_conflicts: our transaction is an
        // Update and a concurrent DataOverlay has already committed. A row-moving
        // update relocates the rows it touches, so an overlay on one of those
        // fragments can no longer be applied (retryable); an overlay on any other
        // fragment, or an in-place column rewrite, is compatible.
        use crate::dataset::transaction::{DataOverlayGroup, UpdateMode};
        use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
        use roaring::RoaringBitmap;

        let overlay_on = |fragment_id: u64| Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id,
                overlays: vec![DataOverlayFile {
                    data_file: DataFile::new_legacy_from_fields("overlay.lance", vec![0], None),
                    coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                    committed_version: 0,
                }],
            }],
        };
        // Our update always touches fragment 1.
        let update =
            |update_mode: Option<UpdateMode>, new_fragments: Vec<Fragment>| Operation::Update {
                removed_fragment_ids: vec![],
                updated_fragments: vec![Fragment::new(1)],
                new_fragments,
                fields_modified: vec![0],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            };

        // The overlay covers physical offset 0 of its fragment. Row addresses
        // pack the fragment id in the high 32 bits and the offset in the low 32.
        let rows_on = |fragment_id: u64, offsets: &[u32]| {
            let mut map = RowAddrTreeMap::new();
            map.insert_bitmap(
                fragment_id as u32,
                RoaringBitmap::from_iter(offsets.iter().copied()),
            );
            map
        };

        // (update, committed overlay, moved rows the update carries, expect conflict)
        let cases = [
            // Row-moving update whose moved rows include the overlaid cell -> the
            // update would undo the overlay, so conflict.
            (
                update(Some(UpdateMode::RewriteRows), vec![Fragment::new(2)]),
                overlay_on(1),
                Some(rows_on(1, &[0])),
                true,
            ),
            // ...but if the moved rows miss the overlaid cell, the overlay survives.
            (
                update(Some(UpdateMode::RewriteRows), vec![Fragment::new(2)]),
                overlay_on(1),
                Some(rows_on(1, &[5])),
                false,
            ),
            // An overlay on a fragment the update did not touch is fine.
            (
                update(Some(UpdateMode::RewriteRows), vec![Fragment::new(2)]),
                overlay_on(0),
                Some(rows_on(1, &[0])),
                false,
            ),
            // An in-place column rewrite preserves rows -> compatible.
            (
                update(Some(UpdateMode::RewriteColumns), vec![]),
                overlay_on(1),
                Some(rows_on(1, &[0])),
                false,
            ),
            // Without affected rows we cannot be precise, so a row-moving update
            // on the overlaid fragment falls back to a conservative conflict.
            (
                update(Some(UpdateMode::RewriteRows), vec![Fragment::new(2)]),
                overlay_on(1),
                None,
                true,
            ),
        ];

        for (update_op, other, affected_rows, expect_conflict) in cases {
            let mut rebase = TransactionRebase {
                transaction: Transaction::new(0, update_op.clone(), None),
                initial_fragments: HashMap::new(),
                modified_fragment_ids: modified_fragment_ids(&update_op).collect::<HashSet<_>>(),
                affected_rows: affected_rows.as_ref(),
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };
            let other_txn = Transaction::new(0, other.clone(), None);
            let result = rebase.check_txn(&other_txn, 1);
            if expect_conflict {
                assert!(
                    matches!(result, Err(Error::RetryableCommitConflict { .. })),
                    "update should retryably conflict with {other:?}, got {result:?}"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "update should be compatible with {other:?}, got {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    #[rstest::rstest]
    #[case::coverage_overlaps_moved_row(vec![0u32], true)]
    #[case::coverage_disjoint_from_moved_row(vec![3u32], false)]
    async fn test_data_overlay_finish_conflicts_with_row_moving_update(
        #[case] coverage_offsets: Vec<u32>,
        #[case] expect_conflict: bool,
    ) {
        // 5 rows in one fragment. A concurrent RewriteRows update moves row 0 out
        // to a new fragment (deleting it from fragment 0). Our overlay on fragment
        // 0 conflicts only when its coverage includes the moved row; the decision
        // is made in finish, which reads the deletion vectors.
        use crate::dataset::transaction::{DataOverlayGroup, UpdateMode};
        use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
        use roaring::RoaringBitmap;

        let dataset = test_dataset(5, 1).await;
        let mut fragment = dataset.fragments().as_slice()[0].clone();

        let moved_fragment = Fragment::new(0)
            .with_file(
                "moved.lance",
                vec![0],
                vec![0],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                NonZero::new(10),
            )
            .with_physical_rows(1);
        let update_op = Operation::Update {
            updated_fragments: vec![apply_deletion(&[0], &mut fragment, &dataset).await],
            removed_fragment_ids: vec![],
            new_fragments: vec![moved_fragment],
            fields_modified: vec![],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteRows),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let update_txn = Transaction::new_from_version(dataset.manifest.version, update_op);

        let overlay_op = Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id: 0,
                overlays: vec![DataOverlayFile {
                    data_file: DataFile::new_legacy_from_fields("overlay.lance", vec![0], None),
                    coverage: OverlayCoverage::dense(RoaringBitmap::from_iter(coverage_offsets)),
                    committed_version: 0,
                }],
            }],
        };
        let overlay_txn = Transaction::new_from_version(dataset.manifest.version, overlay_op);

        // Commit the update so the latest dataset reflects the moved (deleted) row.
        let latest_dataset = CommitBuilder::new(Arc::new(dataset.clone()))
            .execute(update_txn.clone())
            .await
            .unwrap();

        let mut rebase = TransactionRebase::try_new(&dataset, overlay_txn.clone(), None)
            .await
            .unwrap();
        // The check defers the row-level decision to finish, flagging fragment 0.
        rebase.check_txn(&update_txn, 1).unwrap();
        assert_eq!(
            rebase
                .initial_fragments
                .iter()
                .map(|(id, (_, needs_check))| (*id, *needs_check))
                .collect::<Vec<_>>(),
            vec![(0, true)],
        );

        let res = rebase.finish(&latest_dataset).await;
        if expect_conflict {
            assert!(
                matches!(res, Err(crate::Error::RetryableCommitConflict { .. })),
                "overlay covering the moved row should conflict, got {res:?}"
            );
        } else {
            assert!(
                res.is_ok(),
                "overlay disjoint from the moved row should succeed, got {res:?}"
            );
        }
    }

    #[rstest::rstest]
    #[test]
    #[case::indexed_field_updated(0, vec![0])]
    #[case::other_field_updated(1, vec![0, 1])]
    fn test_create_index_rebase_prunes_updated_field_coverage(
        #[case] field_modified: u32,
        #[case] expected_fragment_ids: Vec<u32>,
    ) {
        let index = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "test".to_string(),
            fields: vec![0],
            dataset_version: 1,
            fragment_bitmap: Some(RoaringBitmap::from_iter([0, 1])),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };
        let mut rebase = TransactionRebase {
            transaction: Transaction::new(
                1,
                Operation::CreateIndex {
                    new_indices: vec![index],
                    removed_indices: vec![],
                },
                None,
            ),
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };
        let update = Transaction::new(
            1,
            Operation::Update {
                updated_fragments: vec![Fragment::new(1)],
                removed_fragment_ids: vec![],
                new_fragments: vec![],
                fields_modified: vec![field_modified],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: Some(UpdateMode::RewriteColumns),
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
            None,
        );

        rebase.check_txn(&update, 2).unwrap();

        let Operation::CreateIndex { new_indices, .. } = &rebase.transaction.operation else {
            panic!("expected CreateIndex operation");
        };
        assert_eq!(
            new_indices[0].fragment_bitmap.as_ref().unwrap(),
            &RoaringBitmap::from_iter(expected_fragment_ids)
        );
    }

    #[test]
    fn test_create_index_conflicts_only_on_same_name() {
        let index0 = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            name: "test".to_string(),
            fields: vec![0],
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };
        let index1 = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            name: "other".to_string(),
            ..index0.clone()
        };

        let txn = Transaction::new(
            0,
            Operation::CreateIndex {
                new_indices: vec![index0.clone()],
                removed_indices: vec![],
            },
            None,
        );
        let mut rebase = TransactionRebase {
            transaction: txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let same_name = Transaction::new(
            0,
            Operation::CreateIndex {
                new_indices: vec![IndexMetadata {
                    uuid: uuid::Uuid::new_v4(),
                    ..index0
                }],
                removed_indices: vec![],
            },
            None,
        );
        let different_name = Transaction::new(
            0,
            Operation::CreateIndex {
                new_indices: vec![index1],
                removed_indices: vec![],
            },
            None,
        );

        let same_name_result = rebase.check_txn(&same_name, 1);
        assert!(
            matches!(same_name_result, Err(Error::RetryableCommitConflict { .. })),
            "Expected retryable conflict for same-name CreateIndex, got {:?}",
            same_name_result
        );

        let mut rebase = TransactionRebase {
            transaction: Transaction::new(
                0,
                Operation::CreateIndex {
                    new_indices: vec![IndexMetadata {
                        uuid: uuid::Uuid::new_v4(),
                        name: "test".to_string(),
                        fields: vec![0],
                        dataset_version: 1,
                        fragment_bitmap: None,
                        index_details: None,
                        index_version: 0,
                        created_at: None,
                        base_id: None,
                        files: None,
                    }],
                    removed_indices: vec![],
                },
                None,
            ),
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };
        let different_name_result = rebase.check_txn(&different_name, 1);
        assert!(
            different_name_result.is_ok(),
            "Expected compatibility for different-name CreateIndex, got {:?}",
            different_name_result
        );
    }

    #[test]
    fn test_create_ngram_index_conflicts_with_overlapping_deferred_rewrite() {
        let ngram_index = |fragment_id| IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "text_ngram".to_string(),
            fields: vec![0],
            dataset_version: 1,
            fragment_bitmap: Some(RoaringBitmap::from_iter([fragment_id])),
            index_details: Some(Arc::new(prost_types::Any {
                type_url: "lance.index.NGramIndexDetails".to_string(),
                value: Vec::new(),
            })),
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };
        let frag_reuse_index = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: FRAG_REUSE_INDEX_NAME.to_string(),
            fields: vec![],
            dataset_version: 2,
            fragment_bitmap: Some(RoaringBitmap::from_iter([2u32])),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        };
        let rewrite = Transaction::new(
            1,
            Operation::Rewrite {
                groups: vec![RewriteGroup {
                    old_fragments: vec![Fragment::new(1)],
                    new_fragments: vec![Fragment::new(2)],
                }],
                rewritten_indices: vec![],
                frag_reuse_index: Some(frag_reuse_index),
            },
            None,
        );

        for (covered_fragment, expect_conflict) in [(1u32, true), (3u32, false)] {
            let mut rebase = TransactionRebase {
                transaction: Transaction::new(
                    1,
                    Operation::CreateIndex {
                        new_indices: vec![ngram_index(covered_fragment)],
                        removed_indices: vec![],
                    },
                    None,
                ),
                initial_fragments: HashMap::new(),
                modified_fragment_ids: HashSet::new(),
                affected_rows: None,
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };
            let result = rebase.check_txn(&rewrite, 2);
            if expect_conflict {
                assert!(
                    matches!(result, Err(Error::RetryableCommitConflict { .. })),
                    "overlapping staged NGram index should conflict, got {result:?}"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "disjoint staged NGram index should remain compatible, got {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_add_bases_non_conflicting() {
        let dataset = test_dataset(10, 2).await;

        // Create two transactions adding different bases
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 1,
                    path: "s3://bucket1/path1".to_string(),
                    name: Some("base1".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 2,
                    path: "s3://bucket2/path2".to_string(),
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // txn1 should not conflict with txn2
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        assert!(rebase.check_txn(&txn2, 2).is_ok());
    }

    #[tokio::test]
    async fn test_add_bases_name_conflict() {
        let dataset = test_dataset(10, 2).await;

        // Create two transactions adding bases with the same name
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 1,
                    path: "s3://bucket1/path1".to_string(),
                    name: Some("duplicate_name".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 2,
                    path: "s3://bucket2/path2".to_string(),
                    name: Some("duplicate_name".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // txn1 should conflict with txn2 due to duplicate name
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        let result = rebase.check_txn(&txn2, 2);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected IncompatibleTransaction error for duplicate name, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_add_bases_path_conflict() {
        let dataset = test_dataset(10, 2).await;

        // Create two transactions adding bases with the same path
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 1,
                    path: "s3://bucket/duplicate_path".to_string(),
                    name: Some("base1".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 2,
                    path: "s3://bucket/duplicate_path".to_string(),
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // txn1 should conflict with txn2 due to duplicate path
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        let result = rebase.check_txn(&txn2, 2);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected IncompatibleTransaction error for duplicate path, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_add_bases_id_conflict() {
        let dataset = test_dataset(10, 2).await;

        // Create two transactions adding bases with the same non-zero ID
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 42,
                    path: "s3://bucket1/path1".to_string(),
                    name: Some("base1".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 42,
                    path: "s3://bucket2/path2".to_string(),
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // txn1 should conflict with txn2 due to duplicate non-zero ID
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        let result = rebase.check_txn(&txn2, 2);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected IncompatibleTransaction error for duplicate ID, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_add_bases_no_conflict_with_data_operations() {
        let dataset = test_dataset(10, 2).await;

        let add_bases_txn = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 1,
                    path: "s3://bucket/path".to_string(),
                    name: Some("base1".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // Test against various data operations
        let data_operations = vec![
            Operation::Append { fragments: vec![] },
            Operation::Delete {
                deleted_fragment_ids: vec![0],
                updated_fragments: vec![],
                predicate: "a > 5".to_string(),
            },
            Operation::Update {
                updated_fragments: vec![Fragment::new(0)],
                removed_fragment_ids: vec![],
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: vec![],
                update_mode: None,
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            },
        ];

        for operation in data_operations {
            let data_txn = Transaction::new_from_version(1, operation.clone());
            let mut rebase = TransactionRebase::try_new(&dataset, add_bases_txn.clone(), None)
                .await
                .unwrap();
            assert!(
                rebase.check_txn(&data_txn, 2).is_ok(),
                "UpdateBases should not conflict with {:?}",
                operation
            );
        }
    }

    #[tokio::test]
    async fn test_add_bases_multiple_bases() {
        let dataset = test_dataset(10, 2).await;

        // txn1 adds two bases
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![
                    lance_table::format::BasePath {
                        id: 1,
                        path: "s3://bucket1/path1".to_string(),
                        name: Some("base1".to_string()),
                        is_dataset_root: false,
                    },
                    lance_table::format::BasePath {
                        id: 2,
                        path: "s3://bucket2/path2".to_string(),
                        name: Some("base2".to_string()),
                        is_dataset_root: false,
                    },
                ],
            },
        );

        // txn2 adds a base that conflicts with one of txn1's bases
        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 3,
                    path: "s3://bucket1/path1".to_string(), // Same path as txn1's first base
                    name: Some("base3".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // Should conflict due to path conflict
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        let result = rebase.check_txn(&txn2, 2);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected IncompatibleTransaction error, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_add_bases_with_none_name() {
        let dataset = test_dataset(10, 2).await;

        // Bases with None names should not conflict on name
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 1,
                    path: "s3://bucket1/path1".to_string(),
                    name: None,
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 2,
                    path: "s3://bucket2/path2".to_string(),
                    name: None,
                    is_dataset_root: false,
                }],
            },
        );

        // Should not conflict despite both having None names
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        assert!(rebase.check_txn(&txn2, 2).is_ok());
    }

    #[tokio::test]
    async fn test_add_bases_with_zero_id() {
        let dataset = test_dataset(10, 2).await;

        // Bases with zero IDs should not conflict on ID
        let txn1 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 0,
                    path: "s3://bucket1/path1".to_string(),
                    name: Some("base1".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        let txn2 = Transaction::new_from_version(
            1,
            Operation::UpdateBases {
                new_bases: vec![lance_table::format::BasePath {
                    id: 0,
                    path: "s3://bucket2/path2".to_string(),
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                }],
            },
        );

        // Should not conflict despite both having zero IDs
        let mut rebase = TransactionRebase::try_new(&dataset, txn1, None)
            .await
            .unwrap();
        assert!(rebase.check_txn(&txn2, 2).is_ok());
    }

    /// Returns the IDs of fragments that have been modified by this operation.
    ///
    /// This does not include new fragments.
    fn modified_fragment_ids(operation: &Operation) -> Box<dyn Iterator<Item = u64> + '_> {
        match operation {
            // These operations add new fragments or don't modify any.
            Operation::Append { .. }
            | Operation::Clone { .. }
            | Operation::Overwrite { .. }
            | Operation::CreateIndex { .. }
            | Operation::ReserveFragments { .. }
            | Operation::Project { .. }
            | Operation::UpdateConfig { .. }
            | Operation::UpdateBases { .. }
            | Operation::Restore { .. }
            | Operation::UpdateMemWalState { .. } => Box::new(std::iter::empty()),
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            } => Box::new(
                updated_fragments
                    .iter()
                    .map(|f| f.id)
                    .chain(deleted_fragment_ids.iter().copied()),
            ),
            Operation::Rewrite { groups, .. } => Box::new(
                groups
                    .iter()
                    .flat_map(|f| f.old_fragments.iter().map(|f| f.id)),
            ),
            Operation::Merge { fragments, .. } => Box::new(fragments.iter().map(|f| f.id)),
            Operation::Update {
                updated_fragments,
                removed_fragment_ids,
                ..
            } => Box::new(
                updated_fragments
                    .iter()
                    .map(|f| f.id)
                    .chain(removed_fragment_ids.iter().copied()),
            ),
            Operation::DataReplacement { replacements } => {
                Box::new(replacements.iter().map(|r| r.0))
            }
            Operation::DataOverlay { groups } => Box::new(groups.iter().map(|g| g.fragment_id)),
        }
    }

    #[tokio::test]
    async fn test_conflicts_data_replacement() {
        use io::commit::conflict_resolver::tests::{ConflictResult::*, modified_fragment_ids};

        let fragment0 = Fragment::new(0);
        let fragment1 = Fragment::new(1);

        let data_file_frag0_fields01 =
            DataFile::new_legacy_from_fields("path0_01", vec![0, 1], None);
        let data_file_frag0_fields23 =
            DataFile::new_legacy_from_fields("path0_23", vec![2, 3], None);
        let data_file_frag1_fields01 =
            DataFile::new_legacy_from_fields("path1_01", vec![0, 1], None);

        let cases = vec![
            (
                "Different fragments",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(1, data_file_frag1_fields01)],
                },
                Compatible,
            ),
            (
                "Same fragment, different fields",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields23)],
                },
                Compatible,
            ),
            (
                "Same fragment, same fields",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Retryable,
            ),
            (
                "Same fragment, overlapping fields",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(
                        0,
                        DataFile::new_legacy_from_fields("path0_12", vec![1, 2], None),
                    )],
                },
                Retryable,
            ),
            (
                "DataReplacement vs Rewrite on same fragment",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Rewrite {
                    groups: vec![RewriteGroup {
                        old_fragments: vec![fragment0.clone()],
                        new_fragments: vec![fragment1.clone()],
                    }],
                    rewritten_indices: vec![],
                    frag_reuse_index: None,
                },
                Retryable,
            ),
            (
                "DataReplacement vs Rewrite on different fragment",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Rewrite {
                    groups: vec![RewriteGroup {
                        old_fragments: vec![fragment1],
                        new_fragments: vec![fragment0],
                    }],
                    rewritten_indices: vec![],
                    frag_reuse_index: None,
                },
                Compatible,
            ),
            // A concurrent Update/Delete only invalidates our positional file when it
            // removes our target fragment outright, or (a horizontal update) rewrites
            // one of our fields. A deletion-vector-only change stays aligned.
            (
                "DataReplacement vs Update (RewriteColumns) on a different field",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![Fragment::new(0)],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![],
                    fields_modified: vec![2],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(RewriteColumns),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                Compatible,
            ),
            (
                // RewriteColumns new_fragments are unrelated inserts, not moved rows.
                "DataReplacement vs Update (RewriteColumns) with inserts on a different field",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![Fragment::new(0)],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![Fragment::new(5)],
                    fields_modified: vec![2],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(RewriteColumns),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                Compatible,
            ),
            (
                "DataReplacement vs Update (RewriteColumns) that rewrote one of our fields",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![Fragment::new(0)],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![],
                    fields_modified: vec![1],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(RewriteColumns),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                Retryable,
            ),
            (
                "DataReplacement vs Update (RewriteRows) that moved our rows",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![Fragment::new(0)],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![Fragment::new(5)],
                    fields_modified: vec![],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(RewriteRows),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                Retryable,
            ),
            (
                "DataReplacement vs Update that removed our fragment",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![],
                    removed_fragment_ids: vec![0],
                    new_fragments: vec![],
                    fields_modified: vec![],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: None,
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                NotCompatible,
            ),
            (
                "DataReplacement vs Update (RewriteRows) that moved a different fragment's rows",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Update {
                    updated_fragments: vec![Fragment::new(1)],
                    removed_fragment_ids: vec![],
                    new_fragments: vec![Fragment::new(5)],
                    fields_modified: vec![],
                    compacted_sstables: Vec::new(),
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(RewriteRows),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                },
                Compatible,
            ),
            (
                "DataReplacement vs Delete (deletion-vector only) on same fragment",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Delete {
                    deleted_fragment_ids: vec![],
                    updated_fragments: vec![Fragment::new(0)],
                    predicate: "a > 0".to_string(),
                },
                Compatible,
            ),
            (
                "DataReplacement vs Delete that removes the fragment",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01.clone())],
                },
                Operation::Delete {
                    deleted_fragment_ids: vec![0],
                    updated_fragments: vec![],
                    predicate: "a > 0".to_string(),
                },
                NotCompatible,
            ),
            // Merge rewrites the whole fragment list -> always conflicts.
            (
                "DataReplacement vs Merge",
                Operation::DataReplacement {
                    replacements: vec![DataReplacementGroup(0, data_file_frag0_fields01)],
                },
                Operation::Merge {
                    fragments: vec![Fragment::new(0)],
                    schema: lance_core::datatypes::Schema::default(),
                },
                Retryable,
            ),
        ];

        for (description, op1, op2, expected) in cases {
            let txn1 = Transaction::new(0, op1.clone(), None);
            let txn2 = Transaction::new(0, op2.clone(), None);

            let mut rebase = TransactionRebase {
                transaction: txn1,
                initial_fragments: HashMap::new(),
                modified_fragment_ids: modified_fragment_ids(&op1).collect::<HashSet<_>>(),
                affected_rows: None,
                conflicting_frag_reuse_indices: Vec::new(),
                conflicting_mem_wal_compacted_sstables: Vec::new(),
            };

            let result = rebase.check_txn(&txn2, 1);
            match expected {
                Compatible => {
                    assert!(
                        result.is_ok(),
                        "{}: expected Compatible but got {:?}",
                        description,
                        result
                    );
                }
                NotCompatible => {
                    // Removal returns a non-retryable IncompatibleTransaction so the
                    // caller can drop the fragment instead of retrying.
                    assert!(
                        matches!(result, Err(Error::IncompatibleTransaction { .. })),
                        "{}: expected NotCompatible but got {:?}",
                        description,
                        result
                    )
                }
                Retryable => {
                    assert!(
                        matches!(result, Err(Error::RetryableCommitConflict { .. })),
                        "{}: expected Retryable but got {:?}",
                        description,
                        result
                    );
                }
            }
        }
    }

    #[test]
    fn test_compacted_sstables_conflict_lower_generation_fails() {
        // Test: committed generation >= to_commit generation should be incompatible (no retry)
        let shard = Uuid::new_v4();

        // Committed has generation 10, we're trying to commit generation 5
        let committed_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );

        let to_commit_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction for lower generation, got {:?}",
            result
        );
    }

    #[test]
    fn test_compacted_sstables_conflict_equal_generation_fails() {
        // Test: committed generation == to_commit generation should be incompatible (no retry)
        let shard = Uuid::new_v4();

        let committed_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );

        let to_commit_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction for equal generation, got {:?}",
            result
        );
    }

    #[test]
    fn test_compacted_sstables_conflict_higher_generation_retryable() {
        // Test: committed generation < to_commit generation should be retryable
        let shard = Uuid::new_v4();

        // Committed has generation 5, we're trying to commit generation 10
        let committed_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );

        let to_commit_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            matches!(result, Err(Error::RetryableCommitConflict { .. })),
            "Expected retryable conflict for higher generation, got {:?}",
            result
        );
    }

    #[test]
    fn test_compacted_sstables_different_shards_ok() {
        // Test: different shards should not conflict
        let shard1 = Uuid::new_v4();
        let shard2 = Uuid::new_v4();

        let committed_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard1, 10)],
            },
            None,
        );

        let to_commit_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard2, 5)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            result.is_ok(),
            "Expected OK for different shards, got {:?}",
            result
        );
    }

    #[test]
    fn test_update_mem_wal_state_vs_create_index_with_compacted_sstables() {
        use crate::index::mem_wal::new_mem_wal_index_meta;
        use lance_index::mem_wal::MemWalIndexDetails;

        let shard = Uuid::new_v4();

        // Create a MemWalIndex with compacted_sstables
        let details = MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            ..Default::default()
        };
        let mem_wal_index = new_mem_wal_index_meta(1, details).unwrap();

        // CreateIndex with MemWalIndex that has generation 10
        let committed_txn = Transaction::new(
            0,
            Operation::CreateIndex {
                new_indices: vec![mem_wal_index],
                removed_indices: vec![],
            },
            None,
        );

        // UpdateMemWalState trying to set generation 5 (lower than committed)
        let to_commit_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            matches!(result, Err(Error::IncompatibleTransaction { .. })),
            "Expected non-retryable IncompatibleTransaction when UpdateMemWalState generation is lower than CreateIndex, got {:?}",
            result
        );

        // Now test with higher generation (should be retryable)
        let to_commit_txn_higher = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 15)],
            },
            None,
        );

        let mut rebase_higher = TransactionRebase {
            transaction: to_commit_txn_higher,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        let result_higher = rebase_higher.check_txn(&committed_txn, 1);
        assert!(
            matches!(result_higher, Err(Error::RetryableCommitConflict { .. })),
            "Expected retryable conflict when UpdateMemWalState generation is higher than CreateIndex, got {:?}",
            result_higher
        );
    }

    #[test]
    fn test_create_index_vs_update_mem_wal_state_rebase() {
        use crate::index::mem_wal::new_mem_wal_index_meta;
        use lance_index::mem_wal::MemWalIndexDetails;

        let shard = Uuid::new_v4();

        // CreateIndex with MemWalIndex (no compacted_sstables initially)
        let details = MemWalIndexDetails::default();
        let mem_wal_index = new_mem_wal_index_meta(1, details).unwrap();

        let to_commit_txn = Transaction::new(
            0,
            Operation::CreateIndex {
                new_indices: vec![mem_wal_index],
                removed_indices: vec![],
            },
            None,
        );

        // UpdateMemWalState with generation 10
        let committed_txn = Transaction::new(
            0,
            Operation::UpdateMemWalState {
                compacted_sstables: vec![CompactedSsTable::new(shard, 10)],
            },
            None,
        );

        let mut rebase = TransactionRebase {
            transaction: to_commit_txn,
            initial_fragments: HashMap::new(),
            modified_fragment_ids: HashSet::new(),
            affected_rows: None,
            conflicting_frag_reuse_indices: Vec::new(),
            conflicting_mem_wal_compacted_sstables: Vec::new(),
        };

        // CreateIndex of MemWalIndex should be compatible with UpdateMemWalState
        // and should collect the compacted_sstables for rebasing
        let result = rebase.check_txn(&committed_txn, 1);
        assert!(
            result.is_ok(),
            "Expected OK for CreateIndex vs UpdateMemWalState, got {:?}",
            result
        );

        // Verify that compacted_sstables were collected
        assert_eq!(rebase.conflicting_mem_wal_compacted_sstables.len(), 1);
        assert_eq!(
            rebase.conflicting_mem_wal_compacted_sstables[0].shard_id,
            shard
        );
        assert_eq!(
            rebase.conflicting_mem_wal_compacted_sstables[0].generation,
            10
        );
    }

    #[tokio::test]
    async fn test_concurrent_overwrites_retryable() {
        let dataset = test_dataset(5, 1).await;
        let dataset_v1_reader1 = Arc::new(dataset.checkout_version(1).await.unwrap());
        let dataset_v1_reader2 = Arc::new(dataset.checkout_version(1).await.unwrap());

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values(10..15)),
                Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(1, 5))),
            ],
        )
        .unwrap();

        // First overwrite succeeds
        let txn1 = InsertBuilder::new(dataset_v1_reader1.clone())
            .with_params(&WriteParams {
                mode: WriteMode::Overwrite,
                ..Default::default()
            })
            .execute_uncommitted(vec![data.clone()])
            .await
            .unwrap();
        let dataset_v2 = CommitBuilder::new(dataset_v1_reader1)
            .execute(txn1)
            .await
            .unwrap();
        assert_eq!(dataset_v2.manifest.version, 2);

        // Second overwrite should fail with retryable conflict
        let txn2 = InsertBuilder::new(dataset_v1_reader2.clone())
            .with_params(&WriteParams {
                mode: WriteMode::Overwrite,
                ..Default::default()
            })
            .execute_uncommitted(vec![data])
            .await
            .unwrap();
        let result = CommitBuilder::new(dataset_v1_reader2).execute(txn2).await;
        assert!(
            matches!(result, Err(Error::RetryableCommitConflict { .. })),
            "Expected RetryableCommitConflict but got: {:?}",
            result
        );

        assert_eq!(dataset_v2.count_rows(None).await.unwrap(), 5);
    }
}
