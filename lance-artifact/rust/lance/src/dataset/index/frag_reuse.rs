// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Dataset;
use crate::dataset::transaction::{Operation, Transaction};
use crate::index::frag_reuse::{build_frag_reuse_index_metadata, load_frag_reuse_index_details};
use lance_core::Error;
use lance_index::frag_reuse::{FRAG_REUSE_INDEX_NAME, FragReuseIndexDetails, FragReuseVersion};
use lance_index::is_system_index;
use lance_table::format::IndexMetadata;
use lance_table::io::manifest::read_manifest_indexes;
use log::warn;
use roaring::RoaringBitmap;

/// Cleanup a fragment reuse index based on the current condition of the indices.
/// If all the indices currently available are already caught up to as a specific reuse version,
/// all older reuse versions (inclusive) can be cleaned up.
///
/// An index is considered caught up against a specific reuse version if either:
/// 1. its coverage is disjoint from the fragments the reuse chain touches, so it
///    holds nothing the FRI would remap (the common multi-index case: a
///    compaction rewrote a sibling index's fragments, not this one); or
/// 2. it is at or past the reuse version's dataset version and no old fragment
///    in the version is still in its bitmap. A missing bitmap counts as caught
///    up, else the version could never be cleaned up.
///
/// Note that there could be a race condition that an index is being added during the cleanup,
/// This will make that specific index not efficient until the next reindex,
/// but it will not cause any correctness problem.
///
/// Typically run after [`compact_files`] with deferred remap and per-index
/// [`remap_column_index`] have caught the indexes up.
///
/// # Example
///
/// ```no_run
/// # use lance::dataset::index::frag_reuse::cleanup_frag_reuse_index;
/// # async fn example(dataset: &mut lance::Dataset) -> lance::Result<()> {
/// // Trim the fragment-reuse index to the versions still needed by some index.
/// cleanup_frag_reuse_index(dataset).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`compact_files`]: crate::dataset::optimize::compact_files
/// [`remap_column_index`]: crate::dataset::optimize::remapping::remap_column_index
pub async fn cleanup_frag_reuse_index(dataset: &mut Dataset) -> lance_core::Result<()> {
    // check against index metadata before auto-remap
    let indices = read_manifest_indexes(
        &dataset.object_store,
        &dataset.manifest_location,
        &dataset.manifest,
    )
    .await?;
    let Some(frag_reuse_index_meta) = indices.iter().find(|idx| idx.name == FRAG_REUSE_INDEX_NAME)
    else {
        return Ok(());
    };

    let frag_reuse_details = load_frag_reuse_index_details(dataset, frag_reuse_index_meta)
        .await
        .unwrap();

    let chain_frag_bitmap = reuse_chain_frag_bitmap(&frag_reuse_details.versions);

    let mut retained_versions = Vec::new();
    let mut fragment_bitmaps = RoaringBitmap::new();
    for version in frag_reuse_details.versions.iter() {
        let check_results = indices
            .iter()
            .map(|idx| is_index_remap_caught_up(version, idx, &chain_frag_bitmap))
            .collect::<Vec<_>>();

        if check_results
            .iter()
            .any(|r| matches!(r, Err(Error::InvalidInput { .. })))
        {
            // If the check fails, the reuse version is likely corrupted, do not retain it.
            continue;
        }

        if !check_results.into_iter().all(|r| r.unwrap()) {
            fragment_bitmaps.extend(version.new_frag_bitmap());
            retained_versions.push(version.clone());
        }
    }

    // Return early if there is nothing to cleanup
    if retained_versions.len() == frag_reuse_details.versions.len() {
        return Ok(());
    }

    let frag_reuse_index_details = FragReuseIndexDetails {
        versions: retained_versions,
    };

    let new_index_meta = build_frag_reuse_index_metadata(
        dataset,
        Some(frag_reuse_index_meta),
        frag_reuse_index_details,
        fragment_bitmaps,
    )
    .await?;

    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::CreateIndex {
            new_indices: vec![new_index_meta],
            removed_indices: vec![frag_reuse_index_meta.clone()],
        },
        None,
    );

    dataset
        .apply_commit(transaction, &Default::default(), &Default::default())
        .await?;

    Ok(())
}

/// Every fragment the reuse chain touches (old + new) across all versions. An
/// index disjoint from this set holds no row address the FRI remaps, so trimming
/// can never strand it (fragment ids are never reused).
fn reuse_chain_frag_bitmap(versions: &[FragReuseVersion]) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    for version in versions {
        bitmap.extend(version.old_frag_ids().iter().map(|&id| id as u32));
        bitmap.extend(version.new_frag_ids().iter().map(|&id| id as u32));
    }
    bitmap
}

fn is_index_remap_caught_up(
    frag_reuse_version: &FragReuseVersion,
    index_meta: &IndexMetadata,
    chain_frag_bitmap: &RoaringBitmap,
) -> lance_core::Result<bool> {
    if is_system_index(index_meta) {
        return Ok(true);
    }

    // Disjoint coverage => caught up regardless of dataset_version, bypassing the
    // stale-version gate below (see fn docs). The chain includes NEW fragments
    // deliberately: a deferred-remap commit advances a covering index's bitmap
    // onto them before its data is remapped, so an old-frag-only check would
    // clear a still-stale index and trim a version it needs.
    if let Some(index_frag_bitmap) = &index_meta.fragment_bitmap
        && index_frag_bitmap.is_disjoint(chain_frag_bitmap)
    {
        return Ok(true);
    }

    if index_meta.dataset_version < frag_reuse_version.dataset_version {
        return Ok(false);
    }

    match index_meta.fragment_bitmap.clone() {
        Some(index_frag_bitmap) => {
            for group in frag_reuse_version.groups.iter() {
                let mut old_frag_in_index = 0;
                for old_frag in group.old_frags.iter() {
                    if index_frag_bitmap.contains(old_frag.id as u32) {
                        old_frag_in_index += 1;
                    }
                }

                if old_frag_in_index > 0 {
                    if old_frag_in_index != group.old_frags.len() {
                        // This should never happen because we always commit a full rewrite group
                        // and we always reindex either the entire group or nothing.
                        // We use invalid input to be consistent with
                        // dataset::transaction::recalculate_fragment_bitmap
                        return Err(Error::invalid_input(format!(
                            "The compaction plan included a rewrite group that was a split of indexed and non-indexed data: {:?}",
                            group.old_frags
                        )));
                    }
                    return Ok(false);
                }
            }
            Ok(true)
        }
        None => {
            warn!(
                "Index {} ({}) missing fragment bitmap, cannot determine if it is caught up with the fragment reuse version, consider retraining the index",
                index_meta.name, index_meta.uuid
            );
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::optimize::{CompactionOptions, compact_files, remapping};
    use crate::index::DatasetIndexExt;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    use all_asserts::{assert_false, assert_true};
    use arrow_array::types::{Float32Type, Int32Type};
    use lance_datagen::Dimension;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;

    fn frag_digest(id: u64) -> lance_index::frag_reuse::FragDigest {
        lance_index::frag_reuse::FragDigest {
            id,
            physical_rows: 100,
            num_deleted_rows: 0,
        }
    }

    fn reuse_version(dataset_version: u64, old: &[u64], new: &[u64]) -> FragReuseVersion {
        FragReuseVersion {
            dataset_version,
            groups: vec![lance_index::frag_reuse::FragReuseGroup {
                changed_row_addrs: Vec::new(),
                old_frags: old.iter().copied().map(frag_digest).collect(),
                new_frags: new.iter().copied().map(frag_digest).collect(),
            }],
        }
    }

    fn index_covering(dataset_version: u64, covered: &[u32]) -> IndexMetadata {
        IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: vec![0],
            name: "test_idx".into(),
            dataset_version,
            fragment_bitmap: Some(RoaringBitmap::from_iter(covered.iter().copied())),
            index_details: None,
            index_version: 0,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    /// The catch-up determination must not pin the FRI on an index that is
    /// simply unrelated to the compaction, while still retaining versions that a
    /// covering-but-not-yet-remapped index needs.
    #[test]
    fn test_caught_up_uses_fragment_coverage_not_only_version() {
        // A reuse version at dataset_version 10 rewrote fragments [4, 5] -> [6].
        let version = reuse_version(10, &[4, 5], &[6]);
        let chain = reuse_chain_frag_bitmap(std::slice::from_ref(&version));

        // Non-covering, stale version: touches none of the rewritten frags, so
        // caught up despite version 5 < 10 (the case the old gate got wrong).
        assert_true!(
            is_index_remap_caught_up(&version, &index_covering(5, &[1, 2, 3]), &chain).unwrap()
        );

        // Still holds an old fragment: not caught up.
        assert_false!(
            is_index_remap_caught_up(&version, &index_covering(5, &[1, 4, 5]), &chain).unwrap()
        );

        // Bitmap advanced onto the new fragment but data not yet remapped: not
        // caught up (why the chain must include new frags).
        assert_false!(
            is_index_remap_caught_up(&version, &index_covering(5, &[1, 6]), &chain).unwrap()
        );

        // Once remapped (version advanced): caught up.
        assert_true!(
            is_index_remap_caught_up(&version, &index_covering(11, &[1, 6]), &chain).unwrap()
        );
    }

    /// The chain spans every reuse version, not just the one being checked: a
    /// stale index touching only a *later* version's fragment must still fall to
    /// the version gate (a per-version chain would wrongly clear it).
    #[test]
    fn test_caught_up_uses_whole_reuse_chain() {
        let v1 = reuse_version(10, &[4, 5], &[6]); // 4,5 -> 6
        let v2 = reuse_version(11, &[6], &[7]); // 6 -> 7
        let chain = reuse_chain_frag_bitmap(&[v1.clone(), v2]);

        // Stale index (version 5) covering only v2's new fragment [7]: not
        // disjoint from the chain, so not caught up on v1.
        assert_false!(is_index_remap_caught_up(&v1, &index_covering(5, &[1, 7]), &chain).unwrap());
    }

    /// Whole-fragment removal (every row deleted, no replacement): an index
    /// emptied by the deletion has an empty bitmap and must count as caught up --
    /// it holds only dead rows -- else its stale version pins the removed-fragment
    /// version forever (remap hits the drop-everything path, never advancing it).
    #[test]
    fn test_caught_up_handles_fragment_removal() {
        // Reuse version 20 removed fragment [7] outright (no replacement).
        let version = reuse_version(20, &[7], &[]);
        let chain = reuse_chain_frag_bitmap(std::slice::from_ref(&version));

        // Index emptied by the deletion (empty bitmap): caught up.
        assert_true!(is_index_remap_caught_up(&version, &index_covering(5, &[]), &chain).unwrap());

        // Bitmap still lists the removed fragment (not yet updated): retained.
        assert_false!(
            is_index_remap_caught_up(&version, &index_covering(5, &[7]), &chain).unwrap()
        );
    }

    #[tokio::test]
    async fn test_cleanup_frag_reuse_index() {
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        // Create an index to be remapped
        let index_name = Some("scalar".into());
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                index_name.clone(),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Compact and check index not caught up
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);
        let indices = dataset.load_indices().await.unwrap();
        let scalar_index = indices.iter().find(|idx| idx.name == "scalar").unwrap();
        // Should not be considered caught up because index was created at an old dataset version
        assert_false!(
            is_index_remap_caught_up(
                &frag_reuse_details.versions[0],
                scalar_index,
                &reuse_chain_frag_bitmap(&frag_reuse_details.versions),
            )
            .unwrap()
        );

        // Remap and check index is caught up
        remapping::remap_column_index(&mut dataset, &["i"], index_name.clone())
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let scalar_index = indices.iter().find(|idx| idx.name == "scalar").unwrap();
        assert_true!(
            is_index_remap_caught_up(
                &frag_reuse_details.versions[0],
                scalar_index,
                &reuse_chain_frag_bitmap(&frag_reuse_details.versions),
            )
            .unwrap()
        );

        // Cleanup frag reuse index and check there is no reuse version
        let mut dataset_clone = dataset.clone();
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();
        let Some(frag_reuse_index_meta) = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
        else {
            panic!("Fragment reuse index must be available");
        };
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 0);

        // Try doing a concurrent cleanup should fail with conflict
        assert!(matches!(
            cleanup_frag_reuse_index(&mut dataset_clone).await,
            Err(Error::RetryableCommitConflict { .. })
        ));
    }

    /// With more than one index on the table, remapping every index must catch
    /// all of them up so the reuse index can be trimmed.
    ///
    /// Regression: `remap_column_index` used to decide whether to remap an
    /// index's data from the presence of the old fragments in its fragment
    /// bitmap. But `load_indices` coverage-remaps the bitmap onto the new
    /// fragments in memory, and remapping the *first* index commits a manifest
    /// that persists that cleaned bitmap for the others — so remapping the
    /// remaining indexes became a silent no-op (their data was never remapped
    /// and their `dataset_version` never advanced), and the reuse index could
    /// never be trimmed.
    #[tokio::test]
    async fn test_cleanup_frag_reuse_index_multiple_indices() {
        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .col("j", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(6), FragmentRowCount::from(1000))
            .await
            .unwrap();

        for col in ["i", "j"] {
            dataset
                .create_index(
                    &[col],
                    IndexType::Scalar,
                    Some(format!("{col}_idx")),
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
        }

        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 2_000,
                defer_index_remap: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

        let frag_reuse_index_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("Fragment reuse index must be available");
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 1);

        for col in ["i", "j"] {
            remapping::remap_column_index(&mut dataset, &[col], Some(format!("{col}_idx")))
                .await
                .unwrap();
        }

        // Every index must now be caught up (data remapped, version advanced).
        let indices = dataset.load_indices().await.unwrap();
        for col in ["i", "j"] {
            let index = indices
                .iter()
                .find(|idx| idx.name == format!("{col}_idx"))
                .unwrap();
            assert!(
                is_index_remap_caught_up(
                    &frag_reuse_details.versions[0],
                    index,
                    &reuse_chain_frag_bitmap(&frag_reuse_details.versions),
                )
                .unwrap(),
                "index {col}_idx was not caught up after remap"
            );
        }

        // ... so the reuse index trims down to zero versions.
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();
        let frag_reuse_index_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("Fragment reuse index must be available");
        let frag_reuse_details = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap();
        assert_eq!(frag_reuse_details.versions.len(), 0);

        // Data correctness, not just version bookkeeping: with the reuse index
        // trimmed there is no auto-remap safety net, so each index must resolve
        // to LIVE rows. An index whose data was not actually remapped (e.g. one
        // whose bitmap was coverage-remapped by a sibling's commit before its
        // own data remap) points at compacted-away fragments and errors on take.
        use futures::TryStreamExt;
        for col in ["i", "j"] {
            let rows: usize = dataset
                .scan()
                .filter(&format!("{col} >= 2000 AND {col} < 3000"))
                .unwrap()
                .try_into_stream()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            assert_eq!(
                rows, 1000,
                "index {col}_idx must resolve to live rows after remap+trim"
            );
        }
    }

    /// When the reuse index has accumulated several versions, a single remap
    /// must compose them and rebuild + commit the index exactly ONCE, not once
    /// per version.
    #[tokio::test]
    async fn test_remap_index_batches_multiple_reuse_versions() {
        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .into_ram_dataset(FragmentCount::from(8), FragmentRowCount::from(1000))
            .await
            .unwrap();
        dataset
            .create_index(
                &["i"],
                IndexType::Scalar,
                Some("i_idx".into()),
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        // Accumulate multiple reuse versions: each round deletes a prefix, which
        // shrinks fragments below target and forces another deferred compaction.
        let options = CompactionOptions {
            target_rows_per_fragment: 4_000,
            defer_index_remap: true,
            ..Default::default()
        };
        for round in 0..4 {
            dataset
                .delete(&format!("i < {}", 1_000 * (round + 1)))
                .await
                .unwrap();
            compact_files(&mut dataset, options.clone(), None)
                .await
                .unwrap();
        }

        let frag_reuse_index_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("Fragment reuse index must be available");
        let num_versions = load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
            .await
            .unwrap()
            .versions
            .len();
        assert!(
            num_versions >= 2,
            "test needs multiple reuse versions to exercise batching, got {num_versions}"
        );

        // A single remap must commit exactly once, regardless of version count.
        let version_before = dataset.manifest.version;
        remapping::remap_column_index(&mut dataset, &["i"], Some("i_idx".into()))
            .await
            .unwrap();
        let commits = dataset.manifest.version - version_before;
        assert_eq!(
            commits, 1,
            "batched remap must commit once, not once per reuse version ({num_versions})"
        );

        // ... and the reuse index then trims to zero.
        cleanup_frag_reuse_index(&mut dataset).await.unwrap();
        let frag_reuse_index_meta = dataset
            .load_index_by_name(FRAG_REUSE_INDEX_NAME)
            .await
            .unwrap()
            .expect("Fragment reuse index must be available");
        assert_eq!(
            load_frag_reuse_index_details(&dataset, &frag_reuse_index_meta)
                .await
                .unwrap()
                .versions
                .len(),
            0
        );
    }
}
