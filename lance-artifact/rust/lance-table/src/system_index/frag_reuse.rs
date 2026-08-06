// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, UInt64Array};
use lance_core::deepsize::{Context, DeepSizeOf};
use lance_core::{Error, Result};
use lance_select::RowAddrTreeMap;
use roaring::{RoaringBitmap, RoaringTreemap};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::format::pb::fragment_reuse_index_details::InlineContent;
use crate::format::{ExternalFile, Fragment, pb};

pub const FRAG_REUSE_INDEX_NAME: &str = "__lance_frag_reuse";
pub const FRAG_REUSE_DETAILS_FILE_NAME: &str = "details.binpb";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct FragDigest {
    pub id: u64,
    pub physical_rows: usize,
    pub num_deleted_rows: usize,
}

impl From<&FragDigest> for pb::fragment_reuse_index_details::FragmentDigest {
    fn from(digest: &FragDigest) -> Self {
        Self {
            id: digest.id,
            physical_rows: digest.physical_rows as u64,
            num_deleted_rows: digest.num_deleted_rows as u64,
        }
    }
}

impl From<&Fragment> for FragDigest {
    fn from(fragment: &Fragment) -> Self {
        Self {
            id: fragment.id,
            physical_rows: fragment
                .physical_rows
                .expect("Fragment doesn't have physical rows recorded"),
            num_deleted_rows: fragment
                .deletion_file
                .as_ref()
                .and_then(|d| d.num_deleted_rows)
                .unwrap_or(0),
        }
    }
}

impl TryFrom<pb::fragment_reuse_index_details::FragmentDigest> for FragDigest {
    type Error = Error;

    fn try_from(digest: pb::fragment_reuse_index_details::FragmentDigest) -> Result<Self> {
        Ok(Self {
            id: digest.id,
            physical_rows: digest.physical_rows as usize,
            num_deleted_rows: digest.num_deleted_rows as usize,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct FragReuseGroup {
    pub changed_row_addrs: Vec<u8>,
    pub old_frags: Vec<FragDigest>,
    pub new_frags: Vec<FragDigest>,
}

impl From<&FragReuseGroup> for pb::fragment_reuse_index_details::Group {
    fn from(group: &FragReuseGroup) -> Self {
        Self {
            changed_row_addrs: group.changed_row_addrs.clone(),
            old_fragments: group.old_frags.iter().map(|f| f.into()).collect(),
            new_fragments: group.new_frags.iter().map(|f| f.into()).collect(),
        }
    }
}

impl TryFrom<pb::fragment_reuse_index_details::Group> for FragReuseGroup {
    type Error = Error;

    fn try_from(group: pb::fragment_reuse_index_details::Group) -> Result<Self> {
        Ok(Self {
            changed_row_addrs: group.changed_row_addrs,
            old_frags: group
                .old_fragments
                .into_iter()
                .map(FragDigest::try_from)
                .collect::<Result<_>>()?,
            new_frags: group
                .new_fragments
                .into_iter()
                .map(FragDigest::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct FragReuseVersion {
    pub dataset_version: u64,
    pub groups: Vec<FragReuseGroup>,
}

impl From<&FragReuseVersion> for pb::fragment_reuse_index_details::Version {
    fn from(version: &FragReuseVersion) -> Self {
        Self {
            dataset_version: version.dataset_version,
            groups: version.groups.iter().map(|g| g.into()).collect(),
        }
    }
}

impl TryFrom<pb::fragment_reuse_index_details::Version> for FragReuseVersion {
    type Error = Error;

    fn try_from(version: pb::fragment_reuse_index_details::Version) -> Result<Self> {
        Ok(Self {
            dataset_version: version.dataset_version,
            groups: version
                .groups
                .into_iter()
                .map(FragReuseGroup::try_from)
                .collect::<Result<_>>()?,
        })
    }
}

impl FragReuseVersion {
    pub fn old_frag_ids(&self) -> Vec<u64> {
        self.groups
            .iter()
            .flat_map(|g| g.old_frags.iter().map(|f| f.id))
            .collect::<Vec<_>>()
    }

    pub fn new_frag_ids(&self) -> Vec<u64> {
        self.groups
            .iter()
            .flat_map(|g| g.new_frags.iter().map(|f| f.id))
            .collect::<Vec<_>>()
    }

    pub fn new_frag_bitmap(&self) -> RoaringBitmap {
        RoaringBitmap::from_iter(self.new_frag_ids().iter().map(|&id| id as u32))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub enum FragReuseIndexDetailsContentType {
    Inline(FragReuseIndexDetails),
    External(ExternalFile),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeepSizeOf)]
pub struct FragReuseIndexDetails {
    pub versions: Vec<FragReuseVersion>,
}

impl From<&FragReuseIndexDetails> for InlineContent {
    fn from(details: &FragReuseIndexDetails) -> Self {
        let mut versions: Vec<pb::fragment_reuse_index_details::Version> =
            details.versions.iter().map(|m| m.into()).collect();
        // sort from oldest to latest version
        versions.sort_by_key(|v| v.dataset_version);
        Self { versions }
    }
}

impl TryFrom<InlineContent> for FragReuseIndexDetails {
    type Error = Error;

    fn try_from(content: InlineContent) -> Result<Self> {
        Ok(Self {
            versions: content
                .versions
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl FragReuseIndexDetails {
    pub fn new_frag_bitmap(&self) -> RoaringBitmap {
        RoaringBitmap::from_iter(
            self.versions
                .iter()
                .flat_map(|v| v.new_frag_ids().into_iter().map(|id| id as u32)),
        )
    }
}

/// An index that stores row ID maps.
/// A row ID map describes the mapping from old row address to new address after compactions.
/// Each version contains the mapping for one round of compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragReuseIndex {
    pub uuid: Uuid,
    pub row_id_maps: Vec<HashMap<u64, Option<u64>>>,
    pub details: FragReuseIndexDetails,
}

impl DeepSizeOf for FragReuseIndex {
    fn deep_size_of_children(&self, cx: &mut Context) -> usize {
        self.row_id_maps.deep_size_of_children(cx) + self.details.deep_size_of_children(cx)
    }
}

impl FragReuseIndex {
    pub fn new(
        uuid: Uuid,
        row_id_maps: Vec<HashMap<u64, Option<u64>>>,
        details: FragReuseIndexDetails,
    ) -> Self {
        Self {
            uuid,
            row_id_maps,
            details,
        }
    }

    pub fn remap_row_id(&self, row_id: u64) -> Option<u64> {
        let mut mapped_value = Some(row_id);
        for row_id_map in self.row_id_maps.iter() {
            if mapped_value.is_some() {
                mapped_value = row_id_map
                    .get(&mapped_value.unwrap())
                    .copied()
                    .unwrap_or(mapped_value);
            }
        }

        mapped_value
    }

    pub fn remap_row_addrs_tree_map(&self, row_addrs: &RowAddrTreeMap) -> RowAddrTreeMap {
        RowAddrTreeMap::from_iter(row_addrs.row_addrs().unwrap().filter_map(|addr| {
            let addr_as_u64 = u64::from(addr);
            self.remap_row_id(addr_as_u64)
        }))
    }

    pub fn remap_row_ids_roaring_tree_map(&self, row_ids: &RoaringTreemap) -> RoaringTreemap {
        RoaringTreemap::from_iter(row_ids.iter().filter_map(|addr| self.remap_row_id(addr)))
    }

    /// Remap a record batch that contains a row_id column at index `row_id_idx`
    /// Currently this assumes there are only 2 columns in the schema,
    /// which is the case for all indexes.
    /// For example, for btree, the schema is (value, row_id).
    /// For vector index storage, the schema is (row_id, vector).
    pub fn remap_row_ids_record_batch(
        &self,
        batch: RecordBatch,
        row_id_idx: usize,
    ) -> Result<RecordBatch> {
        assert_eq!(batch.schema().fields().len(), 2);
        let other_column_idx = 1 - row_id_idx;
        let row_ids = batch.column(row_id_idx).as_primitive::<UInt64Type>();
        let (val_indices, new_row_ids): (Vec<u64>, Vec<u64>) = row_ids
            .values()
            .iter()
            .enumerate()
            .filter_map(|(idx, old_id)| {
                self.remap_row_id(*old_id)
                    .map(|new_id| (idx as u64, new_id))
            })
            .unzip();
        let new_val_indices = UInt64Array::from_iter_values(val_indices);
        let new_vals =
            arrow::compute::take(batch.column(other_column_idx), &new_val_indices, None)?;

        let mut batch_data: Vec<(usize, ArrayRef)> = vec![
            (
                row_id_idx,
                Arc::new(UInt64Array::from_iter_values(new_row_ids)) as ArrayRef,
            ),
            (other_column_idx, Arc::new(new_vals)),
        ];
        batch_data.sort_by_key(|(i, _)| *i);
        Ok(RecordBatch::try_new(
            batch.schema(),
            batch_data.into_iter().map(|(_, item)| item).collect(),
        )?)
    }

    pub fn remap_row_ids_array(&self, array: ArrayRef) -> PrimitiveArray<UInt64Type> {
        let primitive_array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<UInt64Type>>()
            .expect("expected row IDs to be uint64 array");
        (0..primitive_array.len())
            .map(|i| {
                if primitive_array.is_null(i) {
                    None
                } else {
                    self.remap_row_id(primitive_array.value(i))
                }
            })
            .collect()
    }

    pub fn remap_fragment_bitmap(&self, fragment_bitmap: &mut RoaringBitmap) -> Result<()> {
        for version in self.details.versions.iter() {
            for group in version.groups.iter() {
                let mut removed = 0;
                for old_frag in group.old_frags.iter() {
                    if fragment_bitmap.remove(old_frag.id as u32) {
                        removed += 1;
                    }
                }

                if removed > 0 {
                    if removed != group.old_frags.len() {
                        // Straddle: the index covered only part of this rewrite
                        // group. Caused by the bug fixed in
                        // <https://github.com/lance-format/lance/pull/6610>.
                        // We've already removed the indexed old_frags from the
                        // bitmap above; deliberately do NOT insert new_frags,
                        // since the merged fragment also contains rows that
                        // were never indexed. Affected rows fall through to
                        // flat scan until the next optimize_indices. The fix
                        // is persisted on the next write via build_manifest.
                        tracing::warn!(
                            "Healing straddling fragment-reuse rewrite group in index bitmap: \
                             group {:?} was only partially indexed ({} of {} old fragments). \
                             Affected rows will use flat scan until the next optimize_indices.",
                            group.old_frags,
                            removed,
                            group.old_frags.len(),
                        );
                        continue;
                    }

                    for new_frag in group.new_frags.iter() {
                        fragment_bitmap.insert(new_frag.id as u32);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_serialize_deserialize_index_details() {
        // Create sample FragReuseVersions with different dataset versions
        let version1 = FragReuseVersion {
            dataset_version: 2,
            groups: vec![FragReuseGroup {
                changed_row_addrs: vec![1, 2, 3],
                old_frags: vec![FragDigest {
                    id: 1,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                }],
                new_frags: vec![
                    FragDigest {
                        id: 2,
                        physical_rows: 1,
                        num_deleted_rows: 0,
                    },
                    FragDigest {
                        id: 3,
                        physical_rows: 1,
                        num_deleted_rows: 0,
                    },
                ],
            }],
        };

        let version2 = FragReuseVersion {
            dataset_version: 1,
            groups: vec![FragReuseGroup {
                changed_row_addrs: vec![4, 5, 6],
                old_frags: vec![FragDigest {
                    id: 2,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                }],
                new_frags: vec![
                    FragDigest {
                        id: 4,
                        physical_rows: 1,
                        num_deleted_rows: 0,
                    },
                    FragDigest {
                        id: 5,
                        physical_rows: 1,
                        num_deleted_rows: 0,
                    },
                ],
            }],
        };

        // Create FragReuseIndexDetails with versions in reverse order
        let details = FragReuseIndexDetails {
            versions: vec![version1, version2],
        };

        // Convert to protobuf format
        let inline_content: InlineContent = (&details).into();

        // Convert back to FragReuseIndexDetails
        let roundtrip_details = FragReuseIndexDetails::try_from(inline_content).unwrap();

        // Verify the roundtrip
        assert_eq!(roundtrip_details.versions.len(), 2);

        // Verify versions are sorted by dataset_version (oldest to latest)
        assert_eq!(roundtrip_details.versions[0].dataset_version, 1);
        assert_eq!(
            roundtrip_details.versions[0].groups[0].changed_row_addrs,
            vec![4, 5, 6]
        );
        assert_eq!(
            roundtrip_details.versions[0].groups[0].new_frags,
            vec![
                FragDigest {
                    id: 4,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                },
                FragDigest {
                    id: 5,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                }
            ]
        );
        assert_eq!(
            roundtrip_details.versions[0].groups[0].old_frags,
            vec![FragDigest {
                id: 2,
                physical_rows: 1,
                num_deleted_rows: 0,
            }]
        );

        assert_eq!(roundtrip_details.versions[1].dataset_version, 2);
        assert_eq!(
            roundtrip_details.versions[1].groups[0].changed_row_addrs,
            vec![1, 2, 3]
        );
        assert_eq!(
            roundtrip_details.versions[1].groups[0].new_frags,
            vec![
                FragDigest {
                    id: 2,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                },
                FragDigest {
                    id: 3,
                    physical_rows: 1,
                    num_deleted_rows: 0,
                }
            ]
        );
        assert_eq!(
            roundtrip_details.versions[1].groups[0].old_frags,
            vec![FragDigest {
                id: 1,
                physical_rows: 1,
                num_deleted_rows: 0,
            }]
        );
    }
}
