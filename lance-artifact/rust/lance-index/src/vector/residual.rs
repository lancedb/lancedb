// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::{AddAssign, DivAssign};
use std::sync::Arc;
use std::{iter, ops::MulAssign};

use crate::vector::kmeans::{KMeansAlgoFloat, compute_partitions};
use arrow_array::ArrowNumericType;
use arrow_array::{
    Array, FixedSizeListArray, PrimitiveArray, RecordBatch, UInt32Array,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type, UInt32Type},
};
use arrow_schema::DataType;
use lance_arrow::{FixedSizeListArrayExt, RecordBatchExt};
use lance_core::{Error, Result};
use lance_linalg::distance::{DistanceType, Dot, L2};
use num_traits::{Float, FromPrimitive, Num};
use tracing::instrument;

use super::{PQ_CODE_COLUMN, transform::Transformer};

/// Compute the residual vector of a Vector Matrix to their centroids.
///
/// The residual vector is the difference between the original vector and the centroid.
///
#[derive(Clone)]
pub struct ResidualTransform {
    /// Flattened centroids.
    centroids: FixedSizeListArray,

    /// Partition Column
    part_col: String,

    /// Vector Column
    vec_col: String,
}

impl std::fmt::Debug for ResidualTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResidualTransform")
    }
}

impl ResidualTransform {
    pub fn new(centroids: FixedSizeListArray, part_col: &str, column: &str) -> Self {
        Self {
            centroids,
            part_col: part_col.to_owned(),
            vec_col: column.to_owned(),
        }
    }
}

fn do_compute_residual<T: ArrowNumericType>(
    centroids: &FixedSizeListArray,
    vectors: &FixedSizeListArray,
    distance_type: Option<DistanceType>,
    partitions: Option<&UInt32Array>,
) -> Result<FixedSizeListArray>
where
    T::Native: Num + Float + L2 + Dot + MulAssign + DivAssign + AddAssign + FromPrimitive,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let dimension = centroids.value_length() as usize;
    let centroids = centroids.values().as_primitive::<T>();
    let vectors = vectors.values().as_primitive::<T>();

    let part_ids = partitions.cloned().unwrap_or_else(|| {
        compute_partitions::<T, KMeansAlgoFloat<T>>(
            centroids,
            vectors,
            dimension,
            distance_type.expect("provide either partitions or distance type"),
        )
        .0
        .into()
    });
    let part_ids = part_ids.values();

    let vectors_slice = vectors.values();
    let centroids_slice = centroids.values();
    let mut residuals = Vec::with_capacity(vectors.len());
    for (idx, vector) in vectors_slice.chunks_exact(dimension).enumerate() {
        let part_id = part_ids[idx] as usize;
        let c = &centroids_slice[part_id * dimension..(part_id + 1) * dimension];
        residuals.extend(iter::zip(vector, c).map(|(v, cent)| *v - *cent));
    }
    debug_assert_eq!(residuals.len(), vectors.len());
    let residual_arr = PrimitiveArray::<T>::from_iter_values(residuals);
    debug_assert_eq!(residual_arr.len(), vectors.len());
    Ok(FixedSizeListArray::try_new_from_values(
        residual_arr,
        dimension as i32,
    )?)
}

/// Compute residual vectors from the original vectors and centroids.
///
/// ## Parameter
/// - `centroids`: The KMeans centroids.
/// - `vectors`: The original vectors to compute residual vectors.
/// - `distance_type`: The distance type to compute the residual vector.
/// - `partitions`: The partition ID for each vector, if present.
pub(crate) fn compute_residual(
    centroids: &FixedSizeListArray,
    vectors: &FixedSizeListArray,
    distance_type: Option<DistanceType>,
    partitions: Option<&UInt32Array>,
) -> Result<FixedSizeListArray> {
    if centroids.value_length() != vectors.value_length() {
        return Err(Error::index(format!(
            "Compute residual vector: centroid and vector length mismatch: centroid: {}, vector: {}",
            centroids.value_length(),
            vectors.value_length(),
        )));
    }
    // TODO: Bf16 is not supported yet.
    match (centroids.value_type(), vectors.value_type()) {
        (DataType::Float16, DataType::Float16) => {
            do_compute_residual::<Float16Type>(centroids, vectors, distance_type, partitions)
        }
        (DataType::Float32, DataType::Float32) => {
            do_compute_residual::<Float32Type>(centroids, vectors, distance_type, partitions)
        }
        (DataType::Float64, DataType::Float64) => {
            do_compute_residual::<Float64Type>(centroids, vectors, distance_type, partitions)
        }
        (DataType::Float32, DataType::Int8) => do_compute_residual::<Float32Type>(
            centroids,
            &vectors.convert_to_floating_point()?,
            distance_type,
            partitions,
        ),
        _ => Err(Error::index(format!(
            "Compute residual vector: centroids and vector type mismatch: centroid: {}, vector: {}",
            centroids.value_type(),
            vectors.value_type(),
        ))),
    }
}

impl Transformer for ResidualTransform {
    /// Replace the original vector in the [`RecordBatch`] to residual vectors.
    ///
    /// The new [`RecordBatch`] will have a new column named `RESIDUAL_COLUMN`.
    #[instrument(name = "ResidualTransform::transform", level = "debug", skip_all)]
    fn transform(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.column_by_name(PQ_CODE_COLUMN).is_some() {
            // If the PQ code column is present, we don't need to compute residual vectors.
            return Ok(batch.clone());
        }

        let part_ids = batch
            .column_by_name(&self.part_col)
            .ok_or(Error::index(format!(
                "Compute residual vector: partition id column not found: {}",
                self.part_col
            )))?;
        let original = batch
            .column_by_name(&self.vec_col)
            .ok_or(Error::index(format!(
                "Compute residual vector: original vector column {} not found in batch {}",
                self.vec_col,
                batch.schema(),
            )))?;
        let original_vectors = original
            .as_fixed_size_list_opt()
            .ok_or(Error::index(format!(
                "Compute residual vector: original vector column {} is not fixed size list: {}",
                self.vec_col,
                original.data_type(),
            )))?;

        let part_ids_ref = part_ids.as_primitive::<UInt32Type>();
        let residual_arr =
            compute_residual(&self.centroids, original_vectors, None, Some(part_ids_ref))?;

        let batch = if residual_arr.data_type() != original.data_type() {
            batch.replace_column_schema_by_name(
                &self.vec_col,
                residual_arr.data_type().clone(),
                Arc::new(residual_arr),
            )?
        } else {
            batch.replace_column_by_name(&self.vec_col, Arc::new(residual_arr))?
        };

        Ok(batch)
    }
}
