// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::Dataset;
use crate::session::caches::{RowIdIndexKey, RowIdSequenceKey};
use crate::{Error, Result};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use lance_core::utils::{address::RowAddress, deletion::DeletionVector};
use lance_select::{RowAddrSelection, RowAddrTreeMap};
use lance_table::{
    format::{Fragment, RowIdMeta},
    rowids::{FragmentRowIdIndex, RowIdIndex, RowIdSequence, read_row_ids},
};
use std::sync::Arc;

/// Load a row id sequence from the given dataset and fragment.
pub async fn load_row_id_sequence(
    dataset: &Dataset,
    fragment: &Fragment,
) -> Result<Arc<RowIdSequence>> {
    // Virtual path to prevent collisions in the cache.
    match &fragment.row_id_meta {
        None => Err(Error::internal("Missing row id meta")),
        Some(RowIdMeta::Inline(data)) => {
            let data = data.clone();
            let key = RowIdSequenceKey {
                fragment_id: fragment.id,
            };
            dataset
                .metadata_cache
                .get_or_insert_with_key(key, || async move { read_row_ids(&data) })
                .await
        }
        Some(RowIdMeta::External(file_slice)) => {
            let file_slice = file_slice.clone();
            let dataset_clone = dataset.clone();
            let key = RowIdSequenceKey {
                fragment_id: fragment.id,
            };
            dataset
                .metadata_cache
                .get_or_insert_with_key(key, || async move {
                    let path = dataset_clone.base.clone().join(file_slice.path.as_str());
                    let range = file_slice.offset as usize
                        ..(file_slice.offset as usize + file_slice.size as usize);
                    let data = dataset_clone
                        .object_store
                        .open(&path)
                        .await?
                        .get_range(range)
                        .await?;
                    read_row_ids(&data)
                })
                .await
        }
    }
}

/// Load row id sequences from the given dataset and fragments.
///
/// Returned as a vector of (fragment_id, sequence) pairs. These are not
/// guaranteed to be in the same order as the input fragments.
pub fn load_row_id_sequences<'a>(
    dataset: &'a Dataset,
    fragments: &'a [Fragment],
) -> impl Stream<Item = Result<(u32, Arc<RowIdSequence>)>> + 'a {
    futures::stream::iter(fragments)
        .map(|fragment| {
            load_row_id_sequence(dataset, fragment).map_ok(move |seq| (fragment.id as u32, seq))
        })
        .buffer_unordered(dataset.object_store.io_parallelism())
}

pub async fn get_row_id_index(
    dataset: &Dataset,
) -> Result<Option<Arc<lance_table::rowids::RowIdIndex>>> {
    if dataset.manifest.uses_stable_row_ids() {
        let key = RowIdIndexKey {
            version: dataset.manifest.version,
        };
        let index = dataset
            .metadata_cache
            .get_or_insert_with_key(key, || load_row_id_index(dataset))
            .await?;
        Ok(Some(index))
    } else {
        Ok(None)
    }
}

/// Map a set of physical row addresses to their stable row ids
///
/// A fragment's [`RowIdSequence`] holds one id per physical row in offset order;
/// deletions are tracked by the deletion vector and do not compact the sequence,
/// so a physical offset indexes the sequence directly (see [`RowIdIndex`], which
/// maps ids to `start_address + position` and filters deletions separately).
/// Addresses that no longer have a live counterpart are dropped, which is correct:
/// those rows are not part of the answer. That covers both a deleted row and a
/// whole fragment that a maintenance operation replaced — an address-domain index
/// is not remapped by compaction or `update` (neither the zone map nor the bloom
/// filter index supports remap), so its results outlive the fragments they address.
/// The replacement fragments are absent from the index's fragment bitmap, so the
/// scanner routes them to a full scan and no match is lost.
pub(crate) async fn translate_addr_treemap_to_row_ids(
    dataset: &Dataset,
    addrs: &RowAddrTreeMap,
) -> Result<RowAddrTreeMap> {
    let mut row_ids = RowAddrTreeMap::new();
    for (fragment_id, selection) in addrs.iter() {
        let Some(file_fragment) = dataset.get_fragment(*fragment_id as usize) else {
            continue;
        };
        let sequence = load_row_id_sequence(dataset, file_fragment.metadata()).await?;

        match selection {
            RowAddrSelection::Full => {
                // The whole fragment is selected: every live row's id qualifies.
                row_ids |= RowAddrTreeMap::from(sequence.as_ref());
            }
            RowAddrSelection::Partial(offsets) => {
                let deletion_vector = file_fragment.get_deletion_vector().await?;
                for physical_offset in offsets.iter() {
                    // A deletion does not compact the row id sequence; the deleted row keeps
                    // its slot (the deletion vector tracks it separately). So a physical offset
                    // is a direct index into the sequence, regardless of any deletions below it.
                    // A stale offset that points at a deleted row has no live counterpart and is
                    // not part of any answer, so it contributes no id to the block/take set.
                    let deleted = deletion_vector
                        .as_ref()
                        .is_some_and(|dv| dv.contains(physical_offset));
                    if deleted {
                        continue;
                    }
                    // A selected offset with no sequence entry points past the fragment's rows.
                    // Silently dropping it would let a stale index result escape masking, so
                    // treat the mismatch as corruption.
                    let id = sequence.get(physical_offset as usize).ok_or_else(|| {
                        Error::internal(format!(
                            "fragment_id={fragment_id} row-id sequence has no entry at \
                             physical_offset={physical_offset} (sequence len={})",
                            sequence.len()
                        ))
                    })?;
                    row_ids.insert(id);
                }
            }
        }
    }
    Ok(row_ids)
}

/// Resolve row addresses to row ids, positionally: `addr = (fragment << 32) | offset`
/// looks up `offset` in the fragment's [`RowIdSequence`]. The inverse companion of
/// [`get_row_id_index`].
///
/// Returns one entry per input address, in order. Addresses that no longer
/// resolve — a missing fragment, an out-of-range offset, or a `None` input —
/// yield `None`. On datasets without stable row ids, addresses are the row
/// ids, so the input is returned unchanged.
pub async fn row_addrs_to_row_ids(
    dataset: &Dataset,
    addrs: impl IntoIterator<Item = Option<u64>>,
) -> Result<Vec<Option<u64>>> {
    row_addrs_to_row_ids_impl(dataset, addrs, DeletedRowBehavior::Include).await
}

/// Resolve physical row addresses to stable row ids only for live physical slots.
///
/// When stable row ids are enabled, missing fragments, out-of-range offsets,
/// deleted physical slots, and `None` inputs yield `None`. This prevents a
/// deleted slot's stable id from selecting a replacement row written by an
/// update. Without stable row ids, the input is returned unchanged.
pub(crate) async fn live_row_addrs_to_row_ids(
    dataset: &Dataset,
    addrs: impl IntoIterator<Item = Option<u64>>,
) -> Result<Vec<Option<u64>>> {
    row_addrs_to_row_ids_impl(dataset, addrs, DeletedRowBehavior::Exclude).await
}

#[derive(Clone, Copy)]
enum DeletedRowBehavior {
    Include,
    Exclude,
}

async fn row_addrs_to_row_ids_impl(
    dataset: &Dataset,
    addrs: impl IntoIterator<Item = Option<u64>>,
    deleted_row_behavior: DeletedRowBehavior,
) -> Result<Vec<Option<u64>>> {
    let addrs: Vec<Option<u64>> = addrs.into_iter().collect();
    if !dataset.manifest.uses_stable_row_ids() {
        return Ok(addrs);
    }

    let mut positions_by_fragment: std::collections::HashMap<u32, Vec<usize>> =
        std::collections::HashMap::new();
    for (position, addr) in addrs.iter().enumerate() {
        if let Some(addr) = addr {
            positions_by_fragment
                .entry(RowAddress::from(*addr).fragment_id())
                .or_default()
                .push(position);
        }
    }

    let mut ids: Vec<Option<u64>> = vec![None; addrs.len()];
    for (fragment_id, positions) in positions_by_fragment {
        let Some(fragment) = dataset.get_fragment(fragment_id as usize) else {
            continue;
        };
        let sequence = load_row_id_sequence(dataset, fragment.metadata()).await?;
        let deletion_vector = match deleted_row_behavior {
            DeletedRowBehavior::Include => None,
            DeletedRowBehavior::Exclude => fragment.get_deletion_vector().await?,
        };
        for position in positions {
            let offset = RowAddress::from(addrs[position].expect("grouped from Some")).row_offset();
            if deletion_vector
                .as_ref()
                .is_some_and(|deletions| deletions.contains(offset))
            {
                continue;
            }
            ids[position] = sequence.get(offset as usize);
        }
    }
    Ok(ids)
}

async fn load_row_id_index(dataset: &Dataset) -> Result<lance_table::rowids::RowIdIndex> {
    let sequences = load_row_id_sequences(dataset, &dataset.manifest.fragments)
        .try_collect::<Vec<_>>()
        .await?;

    let fragments = dataset.get_fragments();
    let fragment_map: std::collections::HashMap<u32, &crate::dataset::fragment::FileFragment> =
        fragments.iter().map(|f| (f.id() as u32, f)).collect();

    let fragment_indices: Vec<_> =
        futures::stream::iter(sequences.into_iter().map(|(fragment_id, sequence)| {
            let fragment = fragment_map
                .get(&fragment_id)
                .expect("Fragment should exist");
            let has_deletion_file = fragment.metadata().deletion_file.is_some();
            let fragment_clone = (*fragment).clone();
            async move {
                let deletion_vector = if has_deletion_file {
                    fragment_clone
                        .get_deletion_vector()
                        .await?
                        .ok_or_else(|| {
                            Error::internal(format!(
                                "fragment_id={fragment_id} has deletion-file metadata but no deletion vector"
                            ))
                        })?
                } else {
                    Arc::new(DeletionVector::default())
                };

                Ok::<FragmentRowIdIndex, Error>(FragmentRowIdIndex {
                    fragment_id,
                    row_id_sequence: sequence,
                    deletion_vector,
                })
            }
        }))
        .buffer_unordered(dataset.object_store.io_parallelism())
        .try_collect()
        .await?;

    let index = RowIdIndex::new(&fragment_indices)?;

    Ok(index)
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use crate::dataset::{
        ReadParams, UpdateBuilder, WriteMode, WriteParams, builder::DatasetBuilder,
    };

    use super::*;

    use crate::dataset::optimize::{CompactionOptions, compact_files};
    use crate::index::DatasetIndexExt;
    use crate::utils::test::{DatagenExt, FailingProxyStore, FragmentCount, FragmentRowCount};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Int32Type, UInt64Type};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, UInt64Array};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::Future;
    use lance_core::datatypes::Schema;
    use lance_core::{ROW_ADDR, ROW_ID, utils::address::RowAddress};
    use lance_datagen::Dimension;
    use lance_index::{IndexType, scalar::ScalarIndexParams};
    use lance_io::object_store::ObjectStoreParams;
    use std::collections::HashMap;
    use std::collections::HashSet;

    fn sequence_batch(values: Range<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(values))]).unwrap()
    }

    #[tokio::test]
    async fn test_empty_dataset_rowids() {
        let schema = sequence_batch(0..0).schema();
        let reader = RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        assert!(dataset.manifest.uses_stable_row_ids());

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_none());

        assert_eq!(dataset.manifest().next_row_id, 0);
    }

    #[tokio::test]
    async fn test_must_set_on_creation() {
        let tmp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &tmp_dir;

        let batch = sequence_batch(0..10);
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: false,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();
        assert!(!dataset.manifest().uses_stable_row_ids());

        // Trying to append without stable row ids should pass (a warning is emitted) but should not
        // affect the stable_row_ids setting.
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            mode: WriteMode::Append,
            ..Default::default()
        };
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), batch.schema());
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();
        assert!(!dataset.manifest().uses_stable_row_ids());
    }

    #[tokio::test]
    async fn test_new_row_ids() {
        let num_rows = 25u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 10,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();

        let found_addresses = (0..num_rows)
            .map(|i| index.get(i).unwrap())
            .collect::<Vec<_>>();
        let expected_addresses = (0..num_rows)
            .map(|i| {
                let fragment_id = i / 10;
                RowAddress::new_from_parts(fragment_id as u32, (i % 10) as u32)
            })
            .collect::<Vec<_>>();
        assert_eq!(found_addresses, expected_addresses);

        assert_eq!(dataset.manifest().next_row_id, num_rows);
    }

    #[tokio::test]
    async fn test_row_id_index_propagates_deletion_vector_read_error() {
        let batch = sequence_batch(0..10);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let test_uri = temp_dir.as_str();
        let path_prefix = if test_uri.starts_with('/') { "" } else { "/" };
        let routed_uri = format!("file-object-store://{path_prefix}{test_uri}");
        let mut dataset = Dataset::write(
            reader,
            &routed_uri,
            Some(WriteParams {
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.delete("id = 3").await.unwrap();
        drop(dataset);

        let failing_store = Arc::new(FailingProxyStore::new());
        failing_store.fail_when(
            "get_opts",
            "_deletions",
            "injected deletion-vector read failure",
        );
        let dataset = DatasetBuilder::from_uri(&routed_uri)
            .with_read_params(ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(failing_store.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();

        let error = get_row_id_index(&dataset).await.unwrap_err();
        assert!(matches!(&error, Error::IO { .. }));
        assert!(
            error
                .to_string()
                .contains("injected deletion-vector read failure")
        );

        failing_store.clear_fail_when("get_opts", "_deletions");
        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(2).is_some());
        assert!(index.get(3).is_none());
    }

    #[tokio::test]
    async fn test_row_addrs_to_row_ids() {
        let num_rows = 25u64;
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 10,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        // Sequential assignment: row n lives at (n / 10, n % 10) with id n
        let addr = |frag: u64, offset: u64| Some((frag << 32) | offset);
        let addrs = vec![
            addr(2, 1),   // id 21
            addr(0, 3),   // id 3
            None,         // null stays null
            addr(0, 3),   // duplicates allowed
            addr(9, 0),   // missing fragment
            addr(1, 100), // out-of-range offset
        ];
        let ids = row_addrs_to_row_ids(&dataset, addrs).await.unwrap();
        assert_eq!(ids, vec![Some(21), Some(3), None, Some(3), None, None]);

        // Without stable row ids, addresses are the ids: input passes through
        let batch = sequence_batch(0..num_rows as i32);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let plain = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let addrs = vec![addr(1, 2), None];
        let ids = row_addrs_to_row_ids(&plain, addrs.clone()).await.unwrap();
        assert_eq!(ids, addrs);
    }

    #[tokio::test]
    async fn test_row_ids_overwrite() {
        // Validate we don't re-use after overwriting
        let num_rows = 10u64;
        let batch = sequence_batch(0..num_rows as i32);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &temp_dir;
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.manifest().next_row_id, num_rows);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, tmp_path, Some(write_params))
            .await
            .unwrap();

        // Overwriting should NOT reset the row id counter.
        assert_eq!(dataset.manifest().next_row_id, 2 * num_rows);
        // Nor the fragment id counter: ids are a high water mark, so the
        // overwritten fragment cannot alias the one it replaced.
        assert_eq!(dataset.manifest.fragments[0].id, 1);

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_none());
        assert!(index.get(num_rows).is_some());
    }

    #[tokio::test]
    async fn test_row_ids_append() {
        // Validate we handle row ids well when appending concurrently.
        fn write_batch(uri: &str, start: i32) -> impl Future<Output = Result<()>> + '_ {
            let batch = sequence_batch(start..(start + 10));
            let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
            let write_params = WriteParams {
                enable_stable_row_ids: true,
                mode: WriteMode::Append,
                ..Default::default()
            };
            async move {
                let _ = Dataset::write(reader, uri, Some(write_params)).await?;
                Ok(())
            }
        }

        let temp_dir = lance_core::utils::tempfile::TempStrDir::default();
        let tmp_path = &temp_dir;
        let mut start = 0;
        // Just do one first to create the dataset.
        write_batch(tmp_path, start).await.unwrap();
        start += 10;
        // Now do the rest concurrently.
        let futures = (0..5)
            .map(|offset| write_batch(tmp_path, start + offset * 10))
            .collect::<Vec<_>>();
        futures::future::try_join_all(futures).await.unwrap();

        let dataset = DatasetBuilder::from_uri(tmp_path).load().await.unwrap();

        assert_eq!(dataset.manifest().next_row_id, 60);

        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_some());
        assert!(index.get(60).is_none());
    }

    #[tokio::test]
    async fn test_scan_row_ids() {
        // Write dataset with multiple files -> _rowid != _rowaddr
        // Scan with and without each.;
        let batch = sequence_batch(0..6);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 2,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);

        for with_row_id in [true, false] {
            for with_row_address in &[true, false] {
                for projection in &[vec![], vec!["id"]] {
                    if !with_row_id && !with_row_address && projection.is_empty() {
                        continue;
                    }

                    let mut scan = dataset.scan();
                    if with_row_id {
                        scan.with_row_id();
                    }
                    if *with_row_address {
                        scan.with_row_address();
                    }
                    let scan = scan.project(projection).unwrap();
                    let result = scan.try_into_batch().await.unwrap();

                    if with_row_id {
                        let row_ids = result[ROW_ID]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let expected = vec![0, 1, 2, 3, 4, 5].into();
                        assert_eq!(row_ids, &expected);
                    }

                    if *with_row_address {
                        let row_addrs = result[ROW_ADDR]
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let expected =
                            vec![0, 1, 1 << 32, (1 << 32) + 1, 2 << 32, (2 << 32) + 1].into();
                        assert_eq!(row_addrs, &expected);
                    }

                    if !projection.is_empty() {
                        let ids = result["id"].as_any().downcast_ref::<Int32Array>().unwrap();
                        let expected = vec![0, 1, 2, 3, 4, 5].into();
                        assert_eq!(ids, &expected);
                    }
                }
            }
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_delete_with_row_ids(#[values(true, false)] with_scalar_index: bool) {
        let batch = sequence_batch(0..6);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 2,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);

        if with_scalar_index {
            dataset
                .create_index(
                    &["id"],
                    IndexType::Scalar,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
        }

        dataset.delete("id = 3 or id = 4").await.unwrap();

        let mut scan = dataset.scan();
        scan.with_row_id().with_row_address();
        let result = scan.try_into_batch().await.unwrap();

        let row_ids = result[ROW_ID]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let expected = vec![0, 1, 2, 5].into();
        assert_eq!(row_ids, &expected);

        let row_addrs = result[ROW_ADDR]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let expected = vec![0, 1, 1 << 32, (2 << 32) + 1].into();
        assert_eq!(row_addrs, &expected);
    }

    #[tokio::test]
    async fn test_row_ids_update() {
        // Updated fragments get fresh row ids.
        let num_rows = 5u64;
        let batch = sequence_batch(0..num_rows as i32);

        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();

        assert_eq!(dataset.manifest().next_row_id, num_rows);

        let update_result = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id = 3")
            .unwrap()
            .set("id", "100")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        let dataset = update_result.new_dataset;
        let index = get_row_id_index(&dataset).await.unwrap().unwrap();
        assert!(index.get(0).is_some());
        // the updated row ids mapping to new address
        assert_eq!(index.get(3), Some(RowAddress::new_from_parts(1, 0)));
        // there is no new row id
        assert_eq!(index.get(5), None);
    }

    /// 100 sequential rows across 4 fragments with every third row deleted.
    async fn deleted_thirds_dataset(enable_stable_row_ids: bool) -> Dataset {
        let batch = sequence_batch(0..100);
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids,
            max_rows_per_file: 25,
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, "memory://", Some(write_params))
            .await
            .unwrap();
        dataset.delete("id % 3 = 0").await.unwrap();
        dataset
    }

    #[rstest::rstest]
    #[case::interleaved((0..100).collect(), (0..100).filter(|id| id % 3 != 0).collect())]
    #[case::all_deleted(vec![0, 3, 9], vec![])]
    #[case::all_live(vec![1, 2, 50, 98], vec![1, 2, 50, 98])]
    #[tokio::test]
    async fn test_filter_deleted_ids_with_stable_row_ids(
        #[case] ids: Vec<u64>,
        #[case] expected: Vec<u64>,
    ) {
        // Regression test for https://github.com/lance-format/lance/issues/7701:
        // filter_deleted_ids must return exactly the live ids, in order.
        // Sequential inserts, so stable row id == id column value.
        let dataset = deleted_thirds_dataset(true).await;
        assert_eq!(dataset.filter_deleted_ids(&ids).await.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_filter_deleted_ids_without_stable_row_ids() {
        // Non-stable: ids are row addresses; only deleted rows drop out.
        let dataset = deleted_thirds_dataset(false).await;

        // Row addresses: fragment (id / 25) << 32 | offset (id % 25).
        let addr = |id: u64| (id / 25) << 32 | (id % 25);
        let ids = (0..100).map(addr).collect::<Vec<u64>>();
        let expected = (0..100)
            .filter(|id| id % 3 != 0)
            .map(addr)
            .collect::<Vec<u64>>();
        assert_eq!(dataset.filter_deleted_ids(&ids).await.unwrap(), expected);
    }

    fn build_rowid_to_i_map(row_ids: &UInt64Array, i_array: &Int32Array) -> HashMap<u64, i32> {
        row_ids
            .values()
            .iter()
            .zip(i_array.values().iter())
            .map(|(&row_id, &i)| (row_id, i))
            .collect()
    }

    async fn scan_rowid_map(dataset: &Dataset) -> HashMap<u64, i32> {
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan.try_into_batch().await.unwrap();
        let i = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let row_ids = result[ROW_ID]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        build_rowid_to_i_map(row_ids, i)
    }

    async fn compact(dataset: &mut Dataset, target_rows: usize) {
        let options = CompactionOptions {
            target_rows_per_fragment: target_rows,
            ..Default::default()
        };
        let _ = compact_files(dataset, options, None).await.unwrap();
    }

    async fn delete(dataset: &mut Dataset, expr: &str) {
        dataset.delete(expr).await.unwrap();
    }

    #[tokio::test]
    async fn test_stable_row_id_after_multiple_deletion_and_compaction() {
        async fn delete(dataset: &mut Dataset, expr: &str) {
            dataset.delete(expr).await.unwrap();
        }

        let mut dataset = lance_datagen::gen_batch()
            .col("i", lance_datagen::array::step::<Int32Type>())
            .col(
                "vec",
                lance_datagen::array::rand_vec::<Float32Type>(Dimension::from(128)),
            )
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(6),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    enable_v2_manifest_paths: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // first delete and compact
        delete(&mut dataset, "i = 2 or i = 3 or i = 5").await;
        let map_before = scan_rowid_map(&dataset).await;
        compact(&mut dataset, 20).await;
        let map_after = scan_rowid_map(&dataset).await;

        // verify row id
        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // second delete
        delete(&mut dataset, "i = 9").await;
        let mut scan = dataset.scan();
        let result = scan
            .filter("i >= 0")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let ids = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let id_set = ids.values().iter().cloned().collect::<HashSet<_>>();
        let expected: Vec<i32> = (0..60)
            .filter(|&i| i != 2 && i != 3 && i != 5 && i != 9)
            .collect();
        assert_eq!(id_set, expected.iter().cloned().collect::<HashSet<_>>());

        // get the row_id where i == 15
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan
            .filter("i == 15")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let row_id_vec = result[ROW_ID]
            .as_primitive::<UInt64Type>()
            .values()
            .to_vec();

        // third delete and compact
        delete(&mut dataset, "i = 15 or i = 25").await;
        let map_before = scan_rowid_map(&dataset).await;
        compact(&mut dataset, 30).await;
        let map_after = scan_rowid_map(&dataset).await;

        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // verify the rowid represent i == 15 has been deleted
        let result = dataset
            .take_rows(&row_id_vec, Schema::try_from(dataset.schema()).unwrap())
            .await
            .unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_stable_row_id_after_deletion_update_and_compaction() {
        // gen dataset
        let mut dataset = lance_datagen::gen_batch()
            .col(
                "i",
                lance_datagen::array::step::<arrow_array::types::Int32Type>(),
            )
            .col(
                "category",
                lance_datagen::array::cycle::<Int32Type>(vec![1, 2, 3]),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(6),
                FragmentRowCount::from(10),
                Some(WriteParams {
                    max_rows_per_file: 10,
                    enable_stable_row_ids: true,
                    enable_v2_manifest_paths: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // delete some rows
        delete(&mut dataset, "i = 2 or i = 3 or i = 5").await;
        let map_before = scan_rowid_map(&dataset).await;

        // update some rows
        let updated_dataset = UpdateBuilder::new(Arc::new(dataset))
            .update_where("i >= 15")
            .unwrap()
            .set("category", "999")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap()
            .new_dataset;

        // compact the dataset
        let mut dataset = Arc::try_unwrap(updated_dataset).expect("no other Arc references");
        compact(&mut dataset, 20).await;
        let map_after = scan_rowid_map(&dataset).await;

        // verify row id
        assert_eq!(
            map_before.keys().collect::<HashSet<_>>(),
            map_after.keys().collect::<HashSet<_>>()
        );
        for row_id in map_before.keys() {
            assert_eq!(map_before[row_id], map_after[row_id]);
        }

        // verify category filed
        let mut scan = dataset.scan();
        scan.with_row_id();
        scan.scan_in_order(true);
        let result = scan.try_into_batch().await.unwrap();
        let i = result["i"].as_any().downcast_ref::<Int32Array>().unwrap();
        let category = result["category"]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for idx in 0..i.len() {
            if i.value(idx) >= 15 {
                assert_eq!(category.value(idx), 999);
            }
        }
    }
}
