// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Transform of a Vector Input with partition IDs.

use std::ops::Range;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_array::{
    Array, FixedSizeListArray, RecordBatch, UInt32Array, cast::AsArray, types::UInt32Type,
};
use lance_table::utils::LanceIteratorExtension;
use tracing::instrument;

use lance_arrow::RecordBatchExt;
use lance_core::Result;
use lance_linalg::distance::DistanceType;

use crate::vector::kmeans::compute_partitions_arrow_array;
use crate::vector::transform::Transformer;
use crate::vector::utils::SimpleIndex;
use crate::vector::{CENTROID_DIST_COLUMN, CENTROID_DIST_FIELD, LOSS_METADATA_KEY, PART_ID_FIELD};

use super::PART_ID_COLUMN;

/// PartitionTransformer
///
/// It computes the partition ID for each row from the input batch,
/// and adds the partition ID as a new column to the batch,
/// and adds the loss as a metadata to the batch.
///
/// If the partition ID ("__ivf_part_id") column is already present in the Batch,
/// this transform is a Noop.
///
#[derive(Debug)]
pub struct PartitionTransformer {
    centroids: FixedSizeListArray,
    distance_type: DistanceType,
    input_column: String,
    output_column: String,
    with_distance: bool,
    index: Option<SimpleIndex>,
}

impl PartitionTransformer {
    pub fn new(
        centroids: FixedSizeListArray,
        distance_type: DistanceType,
        input_column: impl AsRef<str>,
    ) -> Self {
        let index = SimpleIndex::may_train_index(
            centroids.values().clone(),
            centroids.value_length() as usize,
            distance_type,
        )
        .unwrap();

        Self {
            centroids,
            distance_type,
            input_column: input_column.as_ref().to_owned(),
            output_column: PART_ID_COLUMN.to_owned(),
            with_distance: false,
            index,
        }
    }

    pub fn with_distance(mut self, with_distance: bool) -> Self {
        self.with_distance = with_distance;
        self
    }
}
impl Transformer for PartitionTransformer {
    #[instrument(name = "PartitionTransformer::transform", level = "debug", skip_all)]
    fn transform(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if !(batch.column_by_name(&self.output_column).is_none()
            || self.with_distance && batch.column_by_name(CENTROID_DIST_COLUMN).is_none())
        {
            // If the output columns are already present, we don't need to compute it again.
            return Ok(batch.clone());
        }

        // clear the columns if any of them is present
        let batch = batch
            .drop_column(PART_ID_COLUMN)?
            .drop_column(CENTROID_DIST_COLUMN)?;

        let arr = batch.column_by_name(&self.input_column).ok_or_else(|| {
            lance_core::Error::index(format!(
                "PartitionTransformer: column {} not found in the RecordBatch",
                self.input_column
            ))
        })?;

        let fsl = arr.as_fixed_size_list_opt().ok_or_else(|| {
            lance_core::Error::index(format!(
                "PartitionTransformer: column {} is not a FixedSizeListArray: {}",
                self.input_column,
                arr.data_type(),
            ))
        })?;

        let (part_ids, dists) = match &self.index {
            Some(index) => fsl
                .iter()
                .map(|vec| match vec {
                    Some(v) => {
                        let (id, dist) = index.search(v).unwrap();
                        (Some(id), Some(dist))
                    }
                    None => (None, None),
                })
                .unzip(),
            None => compute_partitions_arrow_array(&self.centroids, fsl, self.distance_type)?,
        };
        let loss = dists
            .iter()
            .map(|d| d.unwrap_or_default() as f64)
            .sum::<f64>();
        let part_ids = UInt32Array::from(part_ids);
        let mut batch = batch.try_with_column(PART_ID_FIELD.clone(), Arc::new(part_ids))?;
        if self.with_distance {
            let dists = Float32Array::from(dists);
            batch = batch.try_with_column(CENTROID_DIST_FIELD.clone(), Arc::new(dists))?;
        }
        Ok(batch.add_metadata(LOSS_METADATA_KEY.to_owned(), loss.to_string())?)
    }
}

#[derive(Debug)]
pub(super) struct PartitionFilter {
    /// The partition column name.
    column: String,
    /// The partition range to filter.
    partition_range: Range<u32>,
}

impl PartitionFilter {
    pub fn new(column: impl AsRef<str>, partition_range: Range<u32>) -> Self {
        Self {
            column: column.as_ref().to_owned(),
            partition_range,
        }
    }

    fn filter_row_ids(&self, partition_ids: &[u32]) -> Vec<u32> {
        partition_ids
            .iter()
            .enumerate()
            .filter_map(|(idx, &part_id)| {
                if self.partition_range.contains(&part_id) {
                    Some(idx as u32)
                } else {
                    None
                }
            })
            // in most cases, no partition will be filtered out.
            .exact_size(partition_ids.len())
            .collect()
    }
}

impl Transformer for PartitionFilter {
    #[instrument(name = "PartitionFilter::transform", level = "debug", skip_all)]
    fn transform(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // TODO: use datafusion execute?
        let arr = batch.column_by_name(&self.column).ok_or_else(|| {
            lance_core::Error::index(format!(
                "PartitionFilter: column {} not found in the RecordBatch",
                self.column
            ))
        })?;
        let part_ids = arr.as_primitive::<UInt32Type>();
        let indices = UInt32Array::from(self.filter_row_ids(part_ids.values()));
        Ok(batch.take(&indices)?)
    }
}
