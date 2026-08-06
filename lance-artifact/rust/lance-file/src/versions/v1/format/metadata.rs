// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::BTreeMap;
use std::ops::Range;

use crate::datatypes::{Fields, FieldsWithMeta};
use crate::format::pb;
use lance_core::datatypes::Schema;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use lance_io::traits::ProtoStruct;

/// Data File Metadata
#[derive(Debug, Default, DeepSizeOf, PartialEq)]
pub struct Metadata {
    /// Offset of each record batch.
    pub batch_offsets: Vec<i32>,

    /// The file position of the page table in the file.
    pub page_table_position: usize,

    /// The file position of the manifest block in the file.
    pub manifest_position: Option<usize>,

    /// Metadata about statistics.
    pub stats_metadata: Option<StatisticsMetadata>,
}

impl ProtoStruct for Metadata {
    type Proto = pb::Metadata;
}

impl From<&Metadata> for pb::Metadata {
    fn from(m: &Metadata) -> Self {
        let statistics = if let Some(stats_meta) = &m.stats_metadata {
            let fields_with_meta: FieldsWithMeta = (&stats_meta.schema).into();
            Some(pb::metadata::StatisticsMetadata {
                schema: fields_with_meta.fields.0,
                fields: stats_meta.leaf_field_ids.clone(),
                page_table_position: stats_meta.page_table_position as u64,
            })
        } else {
            None
        };

        Self {
            batch_offsets: m.batch_offsets.clone(),
            page_table_position: m.page_table_position as u64,
            manifest_position: m.manifest_position.unwrap_or(0) as u64,
            statistics,
        }
    }
}

impl TryFrom<pb::Metadata> for Metadata {
    type Error = Error;
    fn try_from(m: pb::Metadata) -> Result<Self> {
        Ok(Self {
            batch_offsets: m.batch_offsets.clone(),
            page_table_position: m.page_table_position as usize,
            manifest_position: Some(m.manifest_position as usize),
            stats_metadata: if let Some(stats_meta) = m.statistics {
                Some(StatisticsMetadata {
                    schema: Schema::try_from(FieldsWithMeta {
                        fields: Fields(stats_meta.schema),
                        metadata: Default::default(),
                    })?,
                    leaf_field_ids: stats_meta.fields,
                    page_table_position: stats_meta.page_table_position as usize,
                })
            } else {
                None
            },
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct BatchOffsets {
    pub batch_id: i32,
    pub offsets: Vec<u32>,
}

impl Metadata {
    /// Get the number of batches in this file.
    pub fn num_batches(&self) -> usize {
        if self.batch_offsets.is_empty() {
            0
        } else {
            self.batch_offsets.len() - 1
        }
    }

    /// Get the number of records in this file
    pub fn len(&self) -> usize {
        *self.batch_offsets.last().unwrap_or(&0) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push the length of the batch.
    pub fn push_batch_length(&mut self, batch_len: i32) {
        if self.batch_offsets.is_empty() {
            self.batch_offsets.push(0)
        }
        self.batch_offsets
            .push(batch_len + self.batch_offsets.last().unwrap())
    }

    /// Get the starting offset of the batch.
    pub fn get_offset(&self, batch_id: i32) -> Option<i32> {
        self.batch_offsets.get(batch_id as usize).copied()
    }

    /// Get the length of the batch.
    pub fn get_batch_length(&self, batch_id: i32) -> Option<i32> {
        self.get_offset(batch_id + 1)
            .map(|o| o - self.get_offset(batch_id).unwrap_or_default())
    }

    /// Group row indices into each batch.
    ///
    /// The indices must be sorted.
    // TODO: pub(crate)
    pub fn group_indices_to_batches(&self, indices: &[u32]) -> Vec<BatchOffsets> {
        let mut batch_id: i32 = 0;
        let num_batches = self.num_batches() as i32;
        let mut indices_per_batch: BTreeMap<i32, Vec<u32>> = BTreeMap::new();

        let mut indices = Vec::from(indices);
        // sort unstable is quick sort and is almost always faster than sort
        indices.sort_unstable();

        for idx in indices.iter() {
            while batch_id < num_batches && *idx >= self.batch_offsets[batch_id as usize + 1] as u32
            {
                batch_id += 1;
            }
            indices_per_batch
                .entry(batch_id)
                .and_modify(|v| v.push(*idx))
                .or_insert(vec![*idx]);
        }

        indices_per_batch
            .iter()
            .map(|(batch_id, indices)| {
                let batch_offset = self.batch_offsets[*batch_id as usize];
                // Adjust indices to be the in-batch offsets.
                let in_batch_offsets = indices
                    .iter()
                    .map(|i| i - batch_offset as u32)
                    .collect::<Vec<_>>();
                BatchOffsets {
                    batch_id: *batch_id,
                    offsets: in_batch_offsets,
                }
            })
            .collect()
    }

    /// Map the range of row indices to the corresponding batches.
    ///
    /// It returns a list of (batch_id, in_batch_range) tuples.
    // TODO: pub(crate)
    pub fn range_to_batches(&self, range: Range<usize>) -> Result<Vec<(i32, Range<usize>)>> {
        if range.end > *(self.batch_offsets.last().unwrap()) as usize {
            return Err(Error::invalid_input(format!(
                "Range {:?} is out of bounds {}",
                range,
                self.batch_offsets.last().unwrap()
            )));
        }
        let offsets = self.batch_offsets.as_slice();
        let mut batch_id = offsets
            .binary_search(&(range.start as i32))
            .unwrap_or_else(|x| x - 1);
        let mut batches = vec![];

        while batch_id < self.num_batches() {
            let batch_start = offsets[batch_id] as usize;
            if batch_start >= range.end {
                break;
            }
            let start = std::cmp::max(range.start, batch_start) - batch_start;
            let end = std::cmp::min(range.end, offsets[batch_id + 1] as usize) - batch_start;
            batches.push((batch_id as i32, start..end));
            batch_id += 1;
        }
        Ok(batches)
    }
}

/// Metadata about the statistics
#[derive(Debug, PartialEq, DeepSizeOf)]
pub struct StatisticsMetadata {
    /// Schema of the page-level statistics.
    ///
    /// For a given field with id `i`, the statistics are stored in the field
    /// `i.null_count`, `i.min_value`, and `i.max_value`.
    pub schema: Schema,
    pub leaf_field_ids: Vec<i32>,
    pub page_table_position: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_indices_to_batch() {
        let mut metadata = Metadata::default();
        metadata.push_batch_length(20);
        metadata.push_batch_length(20);

        let batches = metadata.group_indices_to_batches(&[6, 24]);
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches,
            vec![
                BatchOffsets {
                    batch_id: 0,
                    offsets: vec![6]
                },
                BatchOffsets {
                    batch_id: 1,
                    offsets: vec![4]
                }
            ]
        );
    }

    #[test]
    fn test_range_to_batches() {
        let mut metadata = Metadata::default();
        for l in [5, 10, 15, 20] {
            metadata.push_batch_length(l);
        }

        let batches = metadata.range_to_batches(0..10).unwrap();
        assert_eq!(batches, vec![(0, 0..5), (1, 0..5)]);

        let batches = metadata.range_to_batches(2..10).unwrap();
        assert_eq!(batches, vec![(0, 2..5), (1, 0..5)]);

        let batches = metadata.range_to_batches(15..33).unwrap();
        assert_eq!(batches, vec![(2, 0..15), (3, 0..3)]);

        let batches = metadata.range_to_batches(14..33).unwrap();
        assert_eq!(batches, vec![(1, 9..10), (2, 0..15), (3, 0..3)]);
    }
}
