// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Index creation helpers for [`NativeTable`].
//!
//! This module contains methods for building index parameters, validating
//! index types, and resolving index fields. These are used by the
//! [`BaseTable::create_index`](super::BaseTable::create_index) implementation
//! on `NativeTable`.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::{AddAssign, DivAssign, MulAssign};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, PrimitiveArray, UInt64Array};
use arrow_schema::{DataType, Field};
use arrow_select::concat::concat;
use arrow_select::filter::filter;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance::index::vector::utils::filter_finite_training_data;
use lance::index::vector::utils::infer_vector_dim;
use lance_arrow::{FixedSizeListArrayExt, RecordBatchExt};
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::vector::bq::RQBuildParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::builder::recommended_num_partitions;
use lance_index::vector::ivf::{IvfBuildParams, new_ivf_transformer};
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_linalg::distance::{
    DistanceType as LanceDistanceType, Dot, L2, dot_distance_batch, l2_distance_batch,
};
use lance_linalg::kernels::{argmin_value_float_with_bias, normalize_fsl_owned};
use num_traits::{Float, FromPrimitive, Zero};
use rand::rngs::SmallRng;
use rand::seq::index::sample;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;

use crate::error::{Error, Result};

/// Index parameters that are either ready or require data-dependent seeded training.
pub(super) enum PreparedIndexParams {
    Ready(Box<dyn lance::index::IndexParams>),
    SeededIvfPq {
        dimension: u32,
        options: crate::index::vector::IvfPqIndexBuilder,
    },
}

/// Resolved column, parameter preparation and index type for one build.
pub(super) type PreparedIndex = (String, PreparedIndexParams, IndexType);
use crate::index::Index;
use crate::index::vector::{VectorIndex, suggested_num_sub_vectors};
use crate::utils::{
    supported_bitmap_data_type, supported_btree_data_type, supported_fm_data_type,
    supported_fts_data_type, supported_label_list_data_type, supported_vector_data_type,
};

use super::NativeTable;

/// A keyed permutation over row offsets with O(1) state.
///
/// The Feistel domain is the smallest power of four containing `num_rows`, so
/// filtering values outside the row range visits at most four candidates per
/// row on average. This lets sampling continue past invalid vectors without
/// favoring low physical row offsets or allocating a table-sized permutation.
struct SeededRowPermutation {
    cursor: u64,
    domain_size: u64,
    half_bits: u32,
    half_mask: u64,
    num_rows: u64,
    seed: u64,
}

impl SeededRowPermutation {
    fn new(num_rows: usize, seed: u64) -> Result<Self> {
        let num_rows = num_rows as u64;
        if num_rows == 0 {
            return Err(Error::InvalidInput {
                message: "Cannot sample rows from an empty table".to_string(),
            });
        }
        if num_rows == 1 {
            return Ok(Self {
                cursor: 0,
                domain_size: 1,
                half_bits: 0,
                half_mask: 0,
                num_rows,
                seed,
            });
        }

        let required_bits = u64::BITS - (num_rows - 1).leading_zeros();
        let half_bits = required_bits.div_ceil(2);
        let domain_bits = half_bits * 2;
        let domain_size = 1u64
            .checked_shl(domain_bits)
            .ok_or_else(|| Error::InvalidInput {
                message: "Table is too large for deterministic row sampling".to_string(),
            })?;
        Ok(Self {
            cursor: 0,
            domain_size,
            half_bits,
            half_mask: (1u64 << half_bits) - 1,
            num_rows,
            seed,
        })
    }

    fn round(value: u64, seed: u64, round: u64) -> u64 {
        let mut mixed = value ^ seed.wrapping_add(round.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn permute(&self, value: u64) -> u64 {
        if self.domain_size == 1 {
            return 0;
        }
        let mut left = value >> self.half_bits;
        let mut right = value & self.half_mask;
        for round in 0..4 {
            let next_left = right;
            let next_right = left ^ (Self::round(right, self.seed, round) & self.half_mask);
            left = next_left;
            right = next_right;
        }
        (left << self.half_bits) | right
    }
}

impl Iterator for SeededRowPermutation {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor < self.domain_size {
            let candidate = self.permute(self.cursor);
            self.cursor += 1;
            if candidate < self.num_rows {
                return Some(candidate);
            }
        }
        None
    }
}

impl NativeTable {
    const IVF_SAMPLE_SEED_SALT: u64 = 0x4956_465f_5341_4d50;
    const IVF_INIT_SEED_SALT: u64 = 0x4956_465f_494e_4954;
    const PQ_SAMPLE_SEED_SALT: u64 = 0x5051_5f53_414d_504c;
    const PQ_INIT_SEED_SALT: u64 = 0x5051_5f49_4e49_5400;

    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let dataset = self.dataset.get().await?;
        let mf = dataset.manifest();
        let indices = dataset.load_indices().await?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(mf, i))
            .collect())
    }

    // Helper to validate index type compatibility with field data type
    pub(super) fn validate_index_type(
        field: &Field,
        index_name: &str,
        supported_fn: impl Fn(&DataType) -> bool,
    ) -> Result<()> {
        if !supported_fn(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A {} index cannot be created on the field `{}` which has data type {}",
                    index_name,
                    field.name(),
                    field.data_type()
                ),
            });
        }
        Ok(())
    }

    // Helper to build IVF params honoring table options.
    pub(super) fn build_ivf_params(
        num_partitions: Option<u32>,
        target_partition_size: Option<u32>,
        sample_rate: u32,
        max_iterations: u32,
    ) -> IvfBuildParams {
        let mut ivf_params = match (num_partitions, target_partition_size) {
            (Some(num_partitions), _) => IvfBuildParams::new(num_partitions as usize),
            (None, Some(target_partition_size)) => {
                IvfBuildParams::with_target_partition_size(target_partition_size as usize)
            }
            (None, None) => IvfBuildParams::default(),
        };
        ivf_params.sample_rate = sample_rate as usize;
        ivf_params.max_iters = max_iterations as usize;
        ivf_params
    }

    fn flatten_vector_array(array: &ArrayRef) -> Result<FixedSizeListArray> {
        let array = if array.null_count() > 0 {
            let valid = arrow::compute::is_not_null(array.as_ref())?;
            filter(array.as_ref(), &valid)?
        } else {
            array.clone()
        };
        let vectors = match array.data_type() {
            DataType::FixedSizeList(_, _) => array,
            DataType::List(_) => array.as_list::<i32>().values().clone(),
            data_type => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "Seeded IVF PQ training requires a vector or multivector column, got {data_type}"
                    ),
                });
            }
        };
        let vectors = if vectors.null_count() > 0 {
            let valid = arrow::compute::is_not_null(vectors.as_ref())?;
            filter(vectors.as_ref(), &valid)?
        } else {
            vectors
        };
        vectors
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .cloned()
            .ok_or_else(|| Error::InvalidInput {
                message: "Seeded IVF PQ training could not flatten the vector column".to_string(),
            })
    }

    fn seeded_sampling_batch_rows(
        remaining_vectors: usize,
        is_multivector: bool,
        vectors_per_row: Option<usize>,
    ) -> usize {
        const MAX_ROWS_PER_TAKE: usize = 8192;
        const MIN_MULTIVECTOR_ROWS_PER_TAKE: usize = 128;

        if !is_multivector {
            return remaining_vectors.min(MAX_ROWS_PER_TAKE);
        }
        vectors_per_row
            .map(|estimate| remaining_vectors.div_ceil(estimate.max(1)))
            .unwrap_or(MIN_MULTIVECTOR_ROWS_PER_TAKE)
            .clamp(MIN_MULTIVECTOR_ROWS_PER_TAKE, MAX_ROWS_PER_TAKE)
    }

    /// Select training vectors in a stable pseudo-random order.
    ///
    /// The row permutation is continued when null or non-finite vectors are
    /// encountered, so retries remain uniform over row position. Multivector
    /// rows are flattened in their stable subvector order.
    async fn seeded_training_data(
        dataset: &lance::Dataset,
        column: &str,
        sample_size: usize,
        seed: u64,
    ) -> Result<FixedSizeListArray> {
        let num_rows = dataset.count_rows(None).await?;
        if num_rows == 0 {
            return Err(Error::InvalidInput {
                message: "Cannot train a seeded IVF PQ index on an empty table".to_string(),
            });
        }

        let projection = Arc::new(dataset.schema().project(&[column])?);
        let is_multivector = matches!(
            projection.field(column).map(|field| field.data_type()),
            Some(DataType::List(_))
        );
        let mut permutation = SeededRowPermutation::new(num_rows, seed)?;
        let mut arrays = Vec::new();
        let mut sampled_vectors = 0;
        let mut vectors_per_row = None;

        while sampled_vectors < sample_size {
            let remaining_vectors = sample_size - sampled_vectors;
            let rows_to_read = Self::seeded_sampling_batch_rows(
                remaining_vectors,
                is_multivector,
                vectors_per_row,
            );
            let indices = permutation.by_ref().take(rows_to_read).collect::<Vec<_>>();
            if indices.is_empty() {
                break;
            }
            let batch = dataset.take(&indices, projection.clone()).await?;
            let array = batch
                .column_by_qualified_name(column)
                .ok_or_else(|| Error::Schema {
                    message: format!("Vector column `{column}` missing from sampled batch"),
                })?;
            let sampled = Self::flatten_vector_array(array)?;
            if is_multivector && !sampled.is_empty() {
                vectors_per_row = Some(sampled.len().div_ceil(indices.len()));
            }
            let sampled = filter_finite_training_data(sampled)?;
            if !sampled.is_empty() {
                let retained = sampled.len().min(remaining_vectors);
                let retained = if retained == sampled.len() {
                    sampled
                } else {
                    let indices = (0..retained as u64).collect::<UInt64Array>();
                    arrow_select::take::take(&sampled, &indices, None)?
                        .as_fixed_size_list()
                        .clone()
                };
                sampled_vectors += retained.len();
                arrays.push(retained);
            }
        }

        if arrays.is_empty() {
            return Err(Error::InvalidInput {
                message: "No valid vectors are available for seeded IVF PQ training".to_string(),
            });
        }
        let array_refs = arrays
            .iter()
            .map(|array| array as &dyn Array)
            .collect::<Vec<_>>();
        let sampled = concat(&array_refs)?;
        Ok(sampled.as_fixed_size_list().clone())
    }

    fn seeded_initial_centroids(
        data: &FixedSizeListArray,
        num_centroids: usize,
        seed: u64,
    ) -> Result<FixedSizeListArray> {
        if data.len() < num_centroids {
            return Err(Error::InvalidInput {
                message: format!(
                    "Not enough valid vectors to train {num_centroids} centroids; only {} are available",
                    data.len()
                ),
            });
        }
        let mut rng = SmallRng::seed_from_u64(seed);
        let indices = sample(&mut rng, data.len(), num_centroids)
            .into_iter()
            .map(|index| index as u64)
            .collect::<UInt64Array>();
        Ok(arrow_select::take::take(data, &indices, None)?
            .as_fixed_size_list()
            .clone())
    }

    fn seeded_assignments<T>(
        data: &[T::Native],
        centroids: &[T::Native],
        dimension: usize,
        distance_type: LanceDistanceType,
        balance_factor: f32,
        cluster_sizes: Option<&[usize]>,
    ) -> Result<Vec<(usize, f32)>>
    where
        T: ArrowPrimitiveType,
        T::Native: Float + Dot + L2 + Send + Sync,
    {
        data.par_chunks(dimension)
            .map(|vector| {
                let bias = || {
                    cluster_sizes
                        .map(|sizes| sizes.iter().map(|size| balance_factor * *size as f32))
                };
                let nearest = match distance_type {
                    LanceDistanceType::L2 => argmin_value_float_with_bias(
                        l2_distance_batch(vector, centroids, dimension),
                        bias(),
                    ),
                    LanceDistanceType::Dot => argmin_value_float_with_bias(
                        dot_distance_batch(vector, centroids, dimension),
                        bias(),
                    ),
                    distance_type => {
                        return Err(Error::InvalidInput {
                            message: format!(
                                "Distance type {distance_type} is not supported for seeded kmeans"
                            ),
                        });
                    }
                };
                nearest
                    .map(|(cluster, distance)| (cluster as usize, distance))
                    .ok_or_else(|| Error::InvalidInput {
                        message: "Could not assign a vector during seeded kmeans".to_string(),
                    })
            })
            .collect()
    }

    fn seeded_cluster_statistics(assignments: &[(usize, f32)], cluster_sizes: &mut [usize]) -> f32 {
        cluster_sizes.fill(0);
        let mut radii = vec![0.0_f32; cluster_sizes.len()];
        let mut losses = vec![0.0_f64; cluster_sizes.len()];
        let mut max_cluster = 0;
        let mut max_cluster_size = 0;
        for (cluster, distance) in assignments {
            cluster_sizes[*cluster] += 1;
            radii[*cluster] = radii[*cluster].max(*distance);
            losses[*cluster] += *distance as f64;
            if cluster_sizes[*cluster] > max_cluster_size {
                max_cluster = *cluster;
                max_cluster_size = cluster_sizes[*cluster];
            }
        }
        if max_cluster_size == 0 {
            return 0.0;
        }
        (radii[max_cluster] - losses[max_cluster] as f32 / max_cluster_size as f32)
            / assignments.len() as f32
    }

    fn seeded_balance_loss(
        cluster_sizes: &[usize],
        num_vectors: usize,
        balance_factor: f32,
    ) -> f32 {
        let size_loss = cluster_sizes.iter().map(|size| size.pow(2)).sum::<usize>() as f32;
        balance_factor * (size_loss - num_vectors.pow(2) as f32 / cluster_sizes.len() as f32)
    }

    /// Lance-compatible empty-cluster splitting with an injected RNG.
    fn split_seeded_empty_clusters<N>(
        num_vectors: usize,
        cluster_sizes: &mut [usize],
        centroids: &mut [N],
        dimension: usize,
        rng: &mut SmallRng,
    ) where
        N: Float + MulAssign,
    {
        let epsilon = N::from(1.0 / 1024.0).unwrap();
        for cluster in 0..cluster_sizes.len() {
            if cluster_sizes[cluster] != 0 {
                continue;
            }
            let mut donor = 0;
            loop {
                let probability = (cluster_sizes[donor] as f32 - 1.0)
                    / (num_vectors - cluster_sizes.len()) as f32;
                if rng.random::<f32>() < probability {
                    break;
                }
                donor = (donor + 1) % cluster_sizes.len();
            }

            cluster_sizes[cluster] = cluster_sizes[donor] / 2;
            cluster_sizes[donor] -= cluster_sizes[cluster];
            for value in 0..dimension {
                if value % 2 == 0 {
                    centroids[cluster * dimension + value] =
                        centroids[donor * dimension + value] * (N::one() + epsilon);
                    centroids[donor * dimension + value] *= N::one() - epsilon;
                } else {
                    centroids[cluster * dimension + value] =
                        centroids[donor * dimension + value] * (N::one() - epsilon);
                    centroids[donor * dimension + value] *= N::one() + epsilon;
                }
            }
        }
    }

    fn seeded_domain(seed: u64, domain: u64) -> u64 {
        SeededRowPermutation::round(domain, seed, 0)
    }

    fn train_seeded_flat_kmeans_typed<T>(
        data: &FixedSizeListArray,
        num_centroids: usize,
        max_iterations: u32,
        distance_type: LanceDistanceType,
        balance_factor: f32,
        seed: u64,
    ) -> Result<FixedSizeListArray>
    where
        T: ArrowPrimitiveType,
        T::Native:
            Float + FromPrimitive + AddAssign + DivAssign + MulAssign + Dot + L2 + Send + Sync,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        // Match Lance's per-kmeans cap. Hierarchical IVF recursively trains
        // small clusters, while PQ remains on the flat 256-centroid path.
        let data = if data.len() >= num_centroids * 512 {
            data.slice(0, num_centroids * 512)
        } else {
            data.clone()
        };
        let dimension = data.value_length() as usize;
        let data_values = data.values().as_primitive::<T>().values();
        let initial = Self::seeded_initial_centroids(
            &data,
            num_centroids,
            Self::seeded_domain(seed, 0x494e_4954),
        )?;
        let mut centroids = initial.values().as_primitive::<T>().values().to_vec();
        let mut previous_loss = f64::MAX;
        let mut cluster_sizes = vec![0usize; num_centroids];
        let mut adjusted_balance_factor = f32::MAX;
        let mut split_rng =
            SmallRng::seed_from_u64(Self::seeded_domain(seed, 0x454d_5054_595f_5350));

        for _ in 0..max_iterations {
            let iteration_balance_factor = adjusted_balance_factor.min(balance_factor);
            let assignments = Self::seeded_assignments::<T>(
                data_values,
                &centroids,
                dimension,
                distance_type,
                iteration_balance_factor,
                Some(&cluster_sizes),
            )?;
            adjusted_balance_factor =
                Self::seeded_cluster_statistics(&assignments, &mut cluster_sizes);
            let loss = assignments
                .iter()
                .map(|(_, distance)| *distance as f64)
                .sum::<f64>()
                + Self::seeded_balance_loss(&cluster_sizes, data.len(), iteration_balance_factor)
                    as f64;
            let mut next_centroids = vec![T::Native::zero(); num_centroids * dimension];
            for (row, (cluster, _)) in assignments.iter().enumerate() {
                let vector = &data_values[row * dimension..(row + 1) * dimension];
                let centroid =
                    &mut next_centroids[*cluster * dimension..(*cluster + 1) * dimension];
                for (centroid_value, vector_value) in centroid.iter_mut().zip(vector) {
                    *centroid_value += *vector_value;
                }
            }
            for (centroid, cluster_size) in next_centroids
                .chunks_mut(dimension)
                .zip(cluster_sizes.iter())
            {
                if *cluster_size > 0 {
                    let divisor = T::Native::from_usize(*cluster_size).unwrap();
                    for value in centroid {
                        *value /= divisor;
                    }
                }
            }

            Self::split_seeded_empty_clusters(
                data.len(),
                &mut cluster_sizes,
                &mut next_centroids,
                dimension,
                &mut split_rng,
            );

            let converged = (previous_loss - loss).abs() < 1e-4 * loss;
            centroids = next_centroids;
            previous_loss = loss;
            if converged {
                break;
            }
        }

        Ok(FixedSizeListArray::try_new_from_values(
            PrimitiveArray::<T>::from(centroids),
            dimension as i32,
        )?)
    }

    /// Seeded counterpart to Lance's balanced, 16-way hierarchical trainer.
    fn train_seeded_hierarchical_kmeans_typed<T>(
        data: &FixedSizeListArray,
        target_centroids: usize,
        max_iterations: u32,
        distance_type: LanceDistanceType,
        balance_factor: f32,
        seed: u64,
    ) -> Result<FixedSizeListArray>
    where
        T: ArrowPrimitiveType,
        T::Native:
            Float + FromPrimitive + AddAssign + DivAssign + MulAssign + Dot + L2 + Send + Sync,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        #[derive(Clone)]
        struct Cluster<N> {
            id: usize,
            indices: Vec<usize>,
            centroid: Vec<N>,
            finalized: bool,
        }

        impl<N> Eq for Cluster<N> {}

        impl<N> PartialEq for Cluster<N> {
            fn eq(&self, other: &Self) -> bool {
                self.indices.len() == other.indices.len() && self.id == other.id
            }
        }

        impl<N> Ord for Cluster<N> {
            fn cmp(&self, other: &Self) -> Ordering {
                match (self.finalized, other.finalized) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    _ => self
                        .indices
                        .len()
                        .cmp(&other.indices.len())
                        .then_with(|| other.id.cmp(&self.id)),
                }
            }
        }

        impl<N> PartialOrd for Cluster<N> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        const HIERARCHICAL_K: usize = 16;
        let dimension = data.value_length() as usize;
        let data_values = data.values().as_primitive::<T>().values();
        let initial_k = HIERARCHICAL_K.min(target_centroids).min(data.len());
        let initial = Self::train_seeded_flat_kmeans_typed::<T>(
            data,
            initial_k,
            max_iterations,
            distance_type,
            balance_factor,
            Self::seeded_domain(seed, 0x4849_4552_5f49_4e49),
        )?;
        let initial_values = initial.values().as_primitive::<T>().values();
        let assignments = Self::seeded_assignments::<T>(
            data_values,
            initial_values,
            dimension,
            distance_type,
            0.0,
            None,
        )?;

        let mut heap = BinaryHeap::new();
        let mut next_cluster_id = 0;
        for cluster in 0..initial_k {
            let indices = assignments
                .iter()
                .enumerate()
                .filter_map(|(row, (assigned, _))| (*assigned == cluster).then_some(row))
                .collect::<Vec<_>>();
            if !indices.is_empty() {
                heap.push(Cluster {
                    id: next_cluster_id,
                    indices,
                    centroid: initial_values[cluster * dimension..(cluster + 1) * dimension]
                        .to_vec(),
                    finalized: false,
                });
                next_cluster_id += 1;
            }
        }

        while heap.len() < target_centroids {
            let mut largest = heap.pop().ok_or_else(|| Error::InvalidInput {
                message: "No seeded kmeans cluster can be further split".to_string(),
            })?;
            if largest.finalized || largest.indices.len() <= 1 {
                largest.finalized = true;
                heap.push(largest);
                if heap.iter().all(|cluster| cluster.finalized) {
                    break;
                }
                continue;
            }

            let remaining_centroids = target_centroids - heap.len();
            let cluster_k = if largest.indices.len() <= HIERARCHICAL_K {
                2.min(remaining_centroids).min(largest.indices.len())
            } else {
                (largest.indices.len() / HIERARCHICAL_K)
                    .min(remaining_centroids)
                    .clamp(2, HIERARCHICAL_K)
            };
            let mut sub_values = Vec::with_capacity(largest.indices.len() * dimension);
            for row in &largest.indices {
                sub_values
                    .extend_from_slice(&data_values[*row * dimension..(*row + 1) * dimension]);
            }
            let sub_data = FixedSizeListArray::try_new_from_values(
                PrimitiveArray::<T>::from(sub_values),
                dimension as i32,
            )?;
            let sub_centroids = Self::train_seeded_flat_kmeans_typed::<T>(
                &sub_data,
                cluster_k,
                max_iterations,
                distance_type,
                balance_factor,
                Self::seeded_domain(seed, largest.id as u64 + 1),
            )?;
            let sub_centroid_values = sub_centroids.values().as_primitive::<T>().values();
            let sub_values = sub_data.values().as_primitive::<T>().values();
            let sub_assignments = Self::seeded_assignments::<T>(
                sub_values,
                sub_centroid_values,
                dimension,
                distance_type,
                0.0,
                None,
            )?;
            let mut cluster_assignments = vec![Vec::new(); cluster_k];
            for (local_row, (cluster, _)) in sub_assignments.iter().enumerate() {
                cluster_assignments[*cluster].push(largest.indices[local_row]);
            }
            let non_empty_clusters = cluster_assignments
                .iter()
                .filter(|indices| !indices.is_empty())
                .count();
            if non_empty_clusters <= 1 {
                largest.finalized = true;
                heap.push(largest);
                continue;
            }

            for (cluster, indices) in cluster_assignments.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                heap.push(Cluster {
                    id: next_cluster_id,
                    indices,
                    centroid: sub_centroid_values[cluster * dimension..(cluster + 1) * dimension]
                        .to_vec(),
                    finalized: false,
                });
                next_cluster_id += 1;
            }
        }

        if heap.len() < target_centroids {
            return Err(Error::InvalidInput {
                message: format!(
                    "Cannot create {target_centroids} IVF partitions: seeded kmeans could only form {} non-empty clusters from {} training vectors",
                    heap.len(),
                    data.len()
                ),
            });
        }
        let mut clusters = heap.into_vec();
        clusters.sort_by_key(|cluster| cluster.id);
        let centroids = clusters
            .into_iter()
            .flat_map(|cluster| cluster.centroid)
            .collect::<Vec<_>>();
        Ok(FixedSizeListArray::try_new_from_values(
            PrimitiveArray::<T>::from(centroids),
            dimension as i32,
        )?)
    }

    fn train_seeded_kmeans_typed<T>(
        data: &FixedSizeListArray,
        num_centroids: usize,
        max_iterations: u32,
        distance_type: LanceDistanceType,
        balance_factor: f32,
        seed: u64,
    ) -> Result<FixedSizeListArray>
    where
        T: ArrowPrimitiveType,
        T::Native:
            Float + FromPrimitive + AddAssign + DivAssign + MulAssign + Dot + L2 + Send + Sync,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        if max_iterations == 0 {
            return Err(Error::InvalidInput {
                message: "max_iterations must be greater than zero for seeded IVF PQ training"
                    .to_string(),
            });
        }
        if data.len() < num_centroids {
            return Err(Error::InvalidInput {
                message: format!(
                    "Not enough valid vectors to train {num_centroids} centroids; only {} are available",
                    data.len()
                ),
            });
        }
        // Lance scales the IVF balance factor by the full training set and
        // switches to hierarchical training above 256 centroids.
        let balance_factor = balance_factor / data.len() as f32;
        if num_centroids > 256 {
            Self::train_seeded_hierarchical_kmeans_typed::<T>(
                data,
                num_centroids,
                max_iterations,
                distance_type,
                balance_factor,
                seed,
            )
        } else {
            Self::train_seeded_flat_kmeans_typed::<T>(
                data,
                num_centroids,
                max_iterations,
                distance_type,
                balance_factor,
                seed,
            )
        }
    }

    fn train_seeded_kmeans(
        data: &FixedSizeListArray,
        num_centroids: usize,
        max_iterations: u32,
        distance_type: LanceDistanceType,
        balance_factor: f32,
        seed: u64,
    ) -> Result<FixedSizeListArray> {
        match data.value_type() {
            DataType::Float16 => Self::train_seeded_kmeans_typed::<Float16Type>(
                data,
                num_centroids,
                max_iterations,
                distance_type,
                balance_factor,
                seed,
            ),
            DataType::Float32 => Self::train_seeded_kmeans_typed::<Float32Type>(
                data,
                num_centroids,
                max_iterations,
                distance_type,
                balance_factor,
                seed,
            ),
            DataType::Float64 => Self::train_seeded_kmeans_typed::<Float64Type>(
                data,
                num_centroids,
                max_iterations,
                distance_type,
                balance_factor,
                seed,
            ),
            data_type => Err(Error::InvalidInput {
                message: format!(
                    "Seeded IVF PQ training requires floating-point vectors, got {data_type}"
                ),
            }),
        }
    }

    fn train_seeded_pq_codebook_typed<T>(
        data: &FixedSizeListArray,
        num_sub_vectors: usize,
        num_centroids: usize,
        max_iterations: u32,
        seed: u64,
    ) -> Result<ArrayRef>
    where
        T: ArrowPrimitiveType,
        T::Native:
            Float + FromPrimitive + AddAssign + DivAssign + MulAssign + Dot + L2 + Send + Sync,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        let dimension = data.value_length() as usize;
        if !dimension.is_multiple_of(num_sub_vectors) {
            return Err(Error::InvalidInput {
                message: format!(
                    "Vector dimension {dimension} must be divisible by num_sub_vectors {num_sub_vectors}"
                ),
            });
        }
        let values = data.values().as_primitive::<T>().values();
        let sub_dimension = dimension / num_sub_vectors;
        let mut codebook = Vec::with_capacity(num_centroids * dimension);
        // PQ stores all centroids for sub-vector 0, then sub-vector 1, and so on.
        for sub_vector in 0..num_sub_vectors {
            let sub_start = sub_vector * sub_dimension;
            let mut sub_vectors = Vec::with_capacity(data.len() * sub_dimension);
            for row in 0..data.len() {
                let start = row * dimension + sub_start;
                sub_vectors.extend_from_slice(&values[start..start + sub_dimension]);
            }
            let sub_vectors = FixedSizeListArray::try_new_from_values(
                PrimitiveArray::<T>::from(sub_vectors),
                sub_dimension as i32,
            )?;
            let centroids = Self::train_seeded_kmeans_typed::<T>(
                &sub_vectors,
                num_centroids,
                max_iterations,
                LanceDistanceType::L2,
                0.0,
                seed.wrapping_add((sub_vector as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)),
            )?;
            codebook.extend_from_slice(centroids.values().as_primitive::<T>().values());
        }
        Ok(Arc::new(PrimitiveArray::<T>::from(codebook)))
    }

    fn train_seeded_pq_codebook(
        data: &FixedSizeListArray,
        num_sub_vectors: usize,
        num_centroids: usize,
        max_iterations: u32,
        seed: u64,
    ) -> Result<ArrayRef> {
        match data.value_type() {
            DataType::Float16 => Self::train_seeded_pq_codebook_typed::<Float16Type>(
                data,
                num_sub_vectors,
                num_centroids,
                max_iterations,
                seed,
            ),
            DataType::Float32 => Self::train_seeded_pq_codebook_typed::<Float32Type>(
                data,
                num_sub_vectors,
                num_centroids,
                max_iterations,
                seed,
            ),
            DataType::Float64 => Self::train_seeded_pq_codebook_typed::<Float64Type>(
                data,
                num_sub_vectors,
                num_centroids,
                max_iterations,
                seed,
            ),
            data_type => Err(Error::InvalidInput {
                message: format!(
                    "Seeded IVF PQ training requires floating-point vectors, got {data_type}"
                ),
            }),
        }
    }

    fn validate_seeded_ivf_pq_params(
        dimension: u32,
        index: &crate::index::vector::IvfPqIndexBuilder,
    ) -> Result<()> {
        if index.target_partition_size == Some(0) {
            return Err(Error::InvalidInput {
                message: "target_partition_size must be greater than zero".to_string(),
            });
        }
        if index.num_partitions == Some(0) {
            return Err(Error::InvalidInput {
                message: "num_partitions must be greater than zero".to_string(),
            });
        }
        if index.sample_rate == 0 {
            return Err(Error::InvalidInput {
                message: "sample_rate must be greater than zero".to_string(),
            });
        }
        if index.max_iterations == 0 {
            return Err(Error::InvalidInput {
                message: "max_iterations must be greater than zero for seeded IVF PQ training"
                    .to_string(),
            });
        }
        let num_bits = index.num_bits.unwrap_or(8);
        if !matches!(num_bits, 4 | 8) {
            return Err(Error::InvalidInput {
                message: format!("IVF PQ only supports 4 or 8 bits, got {num_bits}"),
            });
        }
        let num_sub_vectors =
            Self::get_num_sub_vectors(index.num_sub_vectors, dimension, index.num_bits);
        if num_sub_vectors == 0 {
            return Err(Error::InvalidInput {
                message: "num_sub_vectors must be greater than zero".to_string(),
            });
        }
        if !dimension.is_multiple_of(num_sub_vectors) {
            return Err(Error::InvalidInput {
                message: format!(
                    "Vector dimension {dimension} must be divisible by num_sub_vectors {num_sub_vectors}"
                ),
            });
        }
        Ok(())
    }

    async fn build_seeded_ivf_pq_params(
        dataset: &lance::Dataset,
        column: &str,
        dimension: u32,
        index: &crate::index::vector::IvfPqIndexBuilder,
        seed: u64,
    ) -> Result<(IvfBuildParams, PQBuildParams)> {
        let num_rows = dataset.count_rows(None).await?;
        let target_partition_size = index
            .target_partition_size
            .map(|size| size as usize)
            .unwrap_or_else(|| IndexType::IvfPq.target_partition_size());
        if target_partition_size == 0 {
            return Err(Error::InvalidInput {
                message: "target_partition_size must be greater than zero".to_string(),
            });
        }
        let num_partitions = index
            .num_partitions
            .map(|value| value as usize)
            .unwrap_or_else(|| recommended_num_partitions(num_rows, target_partition_size));
        if num_partitions == 0 {
            return Err(Error::InvalidInput {
                message: "num_partitions must be greater than zero".to_string(),
            });
        }
        if index.sample_rate == 0 {
            return Err(Error::InvalidInput {
                message: "sample_rate must be greater than zero".to_string(),
            });
        }
        let mut ivf_params = Self::build_ivf_params(
            Some(num_partitions as u32),
            index.target_partition_size,
            index.sample_rate,
            index.max_iterations,
        );

        let ivf_sample_size = num_partitions
            .checked_mul(index.sample_rate as usize)
            .ok_or_else(|| Error::InvalidInput {
                message: "IVF training sample size overflowed usize".to_string(),
            })?;
        let mut ivf_training = Self::seeded_training_data(
            dataset,
            column,
            ivf_sample_size,
            seed ^ Self::IVF_SAMPLE_SEED_SALT,
        )
        .await?;
        let mut metric_type: LanceDistanceType = index.distance_type.into();
        if metric_type == LanceDistanceType::Cosine {
            ivf_training = normalize_fsl_owned(ivf_training)?;
            metric_type = LanceDistanceType::L2;
        }
        ivf_training = filter_finite_training_data(ivf_training)?;
        let ivf_centroids = Self::train_seeded_kmeans(
            &ivf_training,
            num_partitions,
            index.max_iterations,
            metric_type,
            1.0,
            seed ^ Self::IVF_INIT_SEED_SALT,
        )?;
        ivf_params.centroids = Some(Arc::new(ivf_centroids.clone()));

        let num_sub_vectors =
            Self::get_num_sub_vectors(index.num_sub_vectors, dimension, index.num_bits) as usize;
        if num_sub_vectors == 0 {
            return Err(Error::InvalidInput {
                message: "num_sub_vectors must be greater than zero".to_string(),
            });
        }
        let num_bits = index.num_bits.unwrap_or(8) as usize;
        if !matches!(num_bits, 4 | 8) {
            return Err(Error::InvalidInput {
                message: format!("IVF PQ only supports 4 or 8 bits, got {num_bits}"),
            });
        }
        let num_pq_centroids = 1usize << num_bits;
        let pq_sample_rate = PQBuildParams::default().sample_rate;
        let pq_sample_size = num_pq_centroids
            .checked_mul(pq_sample_rate)
            .ok_or_else(|| Error::InvalidInput {
                message: "PQ training sample size overflowed usize".to_string(),
            })?;
        let mut pq_training = Self::seeded_training_data(
            dataset,
            column,
            pq_sample_size,
            seed ^ Self::PQ_SAMPLE_SEED_SALT,
        )
        .await?;
        if index.distance_type == crate::DistanceType::Cosine {
            pq_training = normalize_fsl_owned(pq_training)?;
        }
        pq_training = filter_finite_training_data(pq_training)?;
        if matches!(
            index.distance_type,
            crate::DistanceType::L2 | crate::DistanceType::Cosine
        ) {
            let transformer = new_ivf_transformer(ivf_centroids, LanceDistanceType::L2, Vec::new());
            pq_training = transformer.compute_residual(&pq_training)?;
        }
        let codebook = Self::train_seeded_pq_codebook(
            &pq_training,
            num_sub_vectors,
            num_pq_centroids,
            index.max_iterations,
            seed ^ Self::PQ_INIT_SEED_SALT,
        )?;
        let mut pq_params = PQBuildParams::with_codebook(num_sub_vectors, num_bits, codebook);
        pq_params.max_iters = index.max_iterations as usize;
        pq_params.sample_rate = pq_sample_rate;
        Ok((ivf_params, pq_params))
    }

    // Helper to get num_sub_vectors with default calculation
    pub(super) fn get_num_sub_vectors(
        provided: Option<u32>,
        dim: u32,
        num_bits: Option<u32>,
    ) -> u32 {
        if let Some(provided) = provided {
            return provided;
        }
        let suggested = suggested_num_sub_vectors(dim);
        if num_bits.is_some_and(|num_bits| num_bits == 4) && !suggested.is_multiple_of(2) {
            // num_sub_vectors must be even when 4 bits are used
            suggested + 1
        } else {
            suggested
        }
    }

    // Helper to extract vector dimension from field
    pub(super) fn get_vector_dimension(field: &Field) -> Result<u32> {
        match field.data_type() {
            arrow_schema::DataType::FixedSizeList(_, n) => Ok(*n as u32),
            _ => Ok(infer_vector_dim(field.data_type())? as u32),
        }
    }

    /// Resolves the target column and index parameters, erroring on input the
    /// build would reject.
    pub(super) async fn prepare_index(
        &self,
        opts: &crate::index::IndexBuilder,
    ) -> Result<PreparedIndex> {
        if opts.columns.len() != 1 {
            return Err(Error::Schema {
                message: "Multi-column (composite) indices are not yet supported".to_string(),
            });
        }
        self.dataset.ensure_mutable()?;
        let dataset = self.dataset.get().await?;
        let (column, field) = Self::resolve_index_field(dataset.schema(), &opts.columns[0])?;
        let params = match &opts.index {
            Index::IvfPq(index) if index.seed.is_some() => {
                Self::validate_index_type(&field, "IVF PQ", supported_vector_data_type)?;
                let dimension = Self::get_vector_dimension(&field)?;
                Self::validate_seeded_ivf_pq_params(dimension, index)?;
                PreparedIndexParams::SeededIvfPq {
                    dimension,
                    options: index.clone(),
                }
            }
            _ => PreparedIndexParams::Ready(Self::make_index_params(&field, opts.index.clone())?),
        };
        let index_type = self.get_index_type_for_field(&field, &opts.index);
        Ok((column, params, index_type))
    }

    /// Builds a prepared index and publishes the new dataset version.
    pub(super) async fn build_index(
        &self,
        opts: crate::index::IndexBuilder,
        prepared: PreparedIndex,
    ) -> Result<()> {
        let (column, prepared_params, index_type) = prepared;
        let mut dataset = (*self.dataset.get().await?).clone();
        let lance_idx_params = match prepared_params {
            PreparedIndexParams::Ready(params) => params,
            PreparedIndexParams::SeededIvfPq { dimension, options } => {
                let seed = options
                    .seed
                    .expect("seeded parameter preparation requires a seed");
                let (ivf_params, pq_params) =
                    Self::build_seeded_ivf_pq_params(&dataset, &column, dimension, &options, seed)
                        .await?;
                Box::new(VectorIndexParams::with_ivf_pq_params(
                    options.distance_type.into(),
                    ivf_params,
                    pq_params,
                ))
            }
        };
        let columns = [column.as_str()];
        let mut builder = dataset
            .create_index_builder(&columns, index_type, lance_idx_params.as_ref())
            .train(opts.train)
            .replace(opts.replace);

        if let Some(name) = opts.name {
            builder = builder.name(name);
        }
        builder.await?;
        self.dataset.update(dataset);
        Ok(())
    }

    pub(super) fn resolve_index_field(
        schema: &lance_core::datatypes::Schema,
        column: &str,
    ) -> Result<(String, Field)> {
        lance_core::datatypes::parse_field_path(column).map_err(|e| Error::InvalidInput {
            message: format!("Invalid field path `{}`: {}", column, e),
        })?;

        let field_path = schema
            .resolve_case_insensitive(column)
            .ok_or_else(|| Error::Schema {
                message: format!(
                    "Field path `{}` not found in schema. Available field paths: {}",
                    column,
                    schema.field_paths().join(", ")
                ),
            })?;
        let field = field_path.last().expect("field path should be non-empty");
        let path_segments = field_path
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let canonical_path = lance_core::datatypes::format_field_path(&path_segments);

        Ok((canonical_path, Field::from(*field)))
    }

    // Convert LanceDB Index to Lance IndexParams
    pub(super) fn make_index_params(
        field: &Field,
        index_opts: Index,
    ) -> Result<Box<dyn lance::index::IndexParams>> {
        match index_opts {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    // Use IvfPq as the default for auto vector indices
                    let dim = Self::get_vector_dimension(field)?;
                    let ivf_params = lance_index::vector::ivf::IvfBuildParams::default();
                    let num_sub_vectors = Self::get_num_sub_vectors(None, dim, None);
                    let pq_params =
                        lance_index::vector::pq::PQBuildParams::new(num_sub_vectors as usize, 8);
                    let lance_idx_params =
                        lance::index::vector::VectorIndexParams::with_ivf_pq_params(
                            lance_linalg::distance::MetricType::L2,
                            ivf_params,
                            pq_params,
                        );
                    Ok(Box::new(lance_idx_params))
                } else if supported_btree_data_type(field.data_type()) {
                    Ok(Box::new(ScalarIndexParams::for_builtin(
                        BuiltinIndexType::BTree,
                    )))
                } else {
                    Err(Error::InvalidInput {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    })?
                }
            }
            Index::BTree(_) => {
                Self::validate_index_type(field, "BTree", supported_btree_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::BTree,
                )))
            }
            Index::Bitmap(_) => {
                Self::validate_index_type(field, "Bitmap", supported_bitmap_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Bitmap,
                )))
            }
            Index::LabelList(_) => {
                Self::validate_index_type(field, "LabelList", supported_label_list_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::LabelList,
                )))
            }
            Index::Fm(_) => {
                Self::validate_index_type(field, "FM", supported_fm_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Fm,
                )))
            }
            Index::FTS(fts_opts) => {
                Self::validate_index_type(field, "FTS", supported_fts_data_type)?;
                Ok(Box::new(fts_opts))
            }
            Index::IvfFlat(index) => {
                Self::validate_index_type(field, "IVF Flat", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let lance_idx_params =
                    VectorIndexParams::with_ivf_flat_params(index.distance_type.into(), ivf_params);
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfSq(index) => {
                Self::validate_index_type(field, "IVF SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfPq(index) => {
                Self::validate_index_type(field, "IVF PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                debug_assert!(index.seed.is_none());
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let num_bits = index.num_bits.unwrap_or(8) as usize;
                let mut pq_params = PQBuildParams::new(num_sub_vectors as usize, num_bits);
                pq_params.max_iters = index.max_iterations as usize;
                let lance_idx_params = VectorIndexParams::with_ivf_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfRq(index) => {
                Self::validate_index_type(field, "IVF RQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let rq_params = RQBuildParams::new(index.num_bits.unwrap_or(1) as u8);
                let lance_idx_params = VectorIndexParams::with_ivf_rq_params(
                    index.distance_type.into(),
                    ivf_params,
                    rq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswPq(index) => {
                Self::validate_index_type(field, "IVF HNSW PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let pq_params = PQBuildParams::new(
                    num_sub_vectors as usize,
                    index.num_bits.unwrap_or(8) as usize,
                );
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswSq(index) => {
                Self::validate_index_type(field, "IVF HNSW SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswFlat(index) => {
                Self::validate_index_type(field, "IVF HNSW FLAT", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let lance_idx_params = VectorIndexParams::ivf_hnsw(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                );
                Ok(Box::new(lance_idx_params))
            }
        }
    }

    // Helper method to get the correct IndexType based on the Index variant and field data type
    pub(super) fn get_index_type_for_field(&self, field: &Field, index: &Index) -> IndexType {
        match index {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    IndexType::Vector
                } else if supported_btree_data_type(field.data_type()) {
                    IndexType::BTree
                } else {
                    // This should not happen since make_index_params would have failed
                    IndexType::BTree
                }
            }
            Index::BTree(_) => IndexType::BTree,
            Index::Bitmap(_) => IndexType::Bitmap,
            Index::LabelList(_) => IndexType::LabelList,
            Index::Fm(_) => IndexType::Fm,
            Index::FTS(_) => IndexType::Inverted,
            Index::IvfFlat(_)
            | Index::IvfSq(_)
            | Index::IvfPq(_)
            | Index::IvfRq(_)
            | Index::IvfHnswPq(_)
            | Index::IvfHnswSq(_)
            | Index::IvfHnswFlat(_) => IndexType::Vector,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::builder::{
        FixedSizeListBuilder, Float32Builder, LargeListBuilder, ListBuilder, StringBuilder,
    };
    use arrow_array::cast::AsArray;
    use arrow_array::record_batch;
    use arrow_array::types::Float32Type;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Int32Array,
        LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, StructArray,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    use crate::connect;
    use crate::connection::ConnectBuilder;
    use crate::index::Index;
    use crate::index::scalar::{
        BTreeIndexBuilder, BitmapIndexBuilder, FmIndexBuilder, FtsIndexBuilder,
    };
    use crate::index::vector::{
        IvfHnswFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder,
    };
    use crate::query::{ExecutableQuery, QueryBase};
    use crate::table::optimize::{CompactionOptions, OptimizeAction};
    use lance_index::scalar::FullTextSearchQuery;

    fn create_fixed_size_list<T: Array>(
        values: T,
        list_size: i32,
    ) -> crate::error::Result<FixedSizeListArray> {
        let list_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list_size,
        );
        let data = ArrayDataBuilder::new(list_type)
            .len(values.len() / list_size as usize)
            .add_child_data(values.into_data())
            .build()
            .unwrap();

        Ok(FixedSizeListArray::from(data))
    }

    async fn trained_ivf_pq_models(
        table: &crate::Table,
    ) -> (FixedSizeListArray, FixedSizeListArray) {
        use lance::index::DatasetIndexInternalExt;
        use lance::index::vector::ivf::v2::IvfPq as LanceIvfPq;
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::vector::VectorIndex as LanceVectorIndex;
        use lance_index::vector::quantizer::Quantizer;

        let native_table = table.as_native().unwrap();
        let indices = native_table.load_indices().await.unwrap();
        let index_uuid = uuid::Uuid::parse_str(&indices[0].index_uuid).unwrap();
        let dataset = native_table.dataset.get().await.unwrap();
        let lance_index = dataset
            .open_vector_index("embeddings", &index_uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_index = lance_index
            .as_any()
            .downcast_ref::<LanceIvfPq>()
            .expect("expected IvfPq index");
        let centroids = ivf_index.ivf_model().centroids_array().unwrap().clone();
        let Quantizer::Product(product_quantizer) = ivf_index.quantizer() else {
            panic!("expected a product quantizer");
        };
        (centroids, product_quantizer.codebook)
    }

    #[tokio::test]
    async fn test_create_index() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        assert_eq!(table.index_stats("my_index").await.unwrap(), None);

        table
            .create_index(&["embeddings"], Index::Auto)
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert!(index.index_uuid.is_some());
        assert!(index.type_url.is_some());
        assert_eq!(index.num_segments, Some(1));
        assert_eq!(index.num_indexed_rows, Some(512));
        assert_eq!(index.num_unindexed_rows, Some(0));
        assert!(index.created_at.is_some());
        assert!(index.size_bytes.is_some());
        assert!(index.index_version.is_some());
        assert!(index.index_details.is_some());
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));

        table.drop_index(index_name).await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_execute_async_job_waits_for_local_build() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();

        let job = table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute_async()
            .await
            .unwrap();
        // Local jobs run in this process and have no server id.
        assert_eq!(job.id(), None);
        // The build runs as a task, so the index need not exist yet; it must
        // once the job resolves.
        job.wait().await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 1);
        // Cancelling a finished job is a no-op.
        job.cancel().await.unwrap();
    }

    /// Concurrent waiters, and a wait issued after the job settled, all
    /// succeed once the build does.
    #[tokio::test]
    async fn test_execute_async_job_reports_success_to_every_waiter() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();

        let job = Arc::new(
            table
                .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
                .execute_async()
                .await
                .unwrap(),
        );
        let waiters = (0..4)
            .map(|_| {
                let job = job.clone();
                tokio::spawn(async move { job.wait().await })
            })
            .collect::<Vec<_>>();
        for waiter in waiters {
            waiter.await.unwrap().unwrap();
        }
        // A wait after the job settled still reports the same outcome.
        job.wait().await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 1);
    }

    /// Every waiter sees a failure, not just the first: a waiter that missed
    /// the outcome would be told the job succeeded.
    #[tokio::test]
    async fn test_execute_async_job_reports_failure_to_every_waiter() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();
        table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        // Rebuilding the same index without replace fails once the build
        // starts, so the failure reaches the job rather than execute_async.
        let job = Arc::new(
            table
                .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
                .replace(false)
                .execute_async()
                .await
                .unwrap(),
        );
        let waiters = (0..3)
            .map(|_| {
                let job = job.clone();
                tokio::spawn(async move { job.wait().await })
            })
            .collect::<Vec<_>>();
        for waiter in waiters {
            waiter
                .await
                .unwrap()
                .expect_err("every waiter must see the failure");
        }
        job.wait().await.expect_err("a later wait still fails");
    }

    /// A local failure keeps the error it failed with, so a caller can match on
    /// the original variant rather than parse a message.
    #[tokio::test]
    async fn test_execute_async_failure_keeps_the_source_error() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();
        table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let job = table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .replace(false)
            .execute_async()
            .await
            .unwrap();

        let crate::Error::JobFailed { failure, .. } = job.wait().await.unwrap_err() else {
            panic!("a failed job reports JobFailed");
        };
        let source = failure.source.expect("a local failure carries its error");
        assert_eq!(
            failure.message.as_deref(),
            Some(source.to_string()).as_deref()
        );
        // Nothing local can report these, so they must be absent rather than invented.
        assert!(failure.phase.is_none());
        assert!(failure.retryable.is_none());
    }

    /// Every waiter sees the cancellation, including ones that were already
    /// waiting when the cancel landed.
    #[tokio::test]
    async fn test_execute_async_job_reports_cancellation_to_every_waiter() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();

        let job = Arc::new(
            table
                .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
                .execute_async()
                .await
                .unwrap(),
        );
        // Cancel before yielding, so the build cannot have started and the
        // outcome is always the cancellation.
        job.cancel().await.unwrap();

        let waiters = (0..2)
            .map(|_| {
                let job = job.clone();
                tokio::spawn(async move { job.wait().await })
            })
            .collect::<Vec<_>>();
        for waiter in waiters {
            match waiter.await.unwrap() {
                Err(crate::Error::JobCancelled { .. }) => {}
                other => panic!("expected the cancellation, got {other:?}"),
            }
        }
        match job.wait().await {
            Err(crate::Error::JobCancelled { .. }) => {}
            other => panic!("expected the cancellation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_execute_async_job_cancel_stops_local_build() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("id", Int32, (0..512).collect::<Vec<_>>())).unwrap();
        let table = conn.create_table("t", batch).execute().await.unwrap();

        let job = table
            .create_index(&["id"], Index::BTree(BTreeIndexBuilder::default()))
            .execute_async()
            .await
            .unwrap();
        job.cancel().await.unwrap();
        match job.wait().await {
            Err(crate::Error::JobCancelled { .. }) => {}
            // The build may finish before the abort lands.
            Ok(()) => {}
            other => panic!("unexpected job outcome: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_ivf_pq_uses_default_partition_size_for_num_partitions() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        const PARTITION_SIZE: usize = 8192;
        let num_rows = PARTITION_SIZE * 2;
        let dimension = 8usize;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension as i32,
            ),
            false,
        )]));

        let float_arr =
            Float32Array::from_iter_values((0..(num_rows * dimension)).map(|v| v as f32));
        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension as i32).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let native_table = table.as_native().unwrap();
        let builder = IvfPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfPq(builder))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], std::time::Duration::from_secs(30))
            .await
            .unwrap();

        use lance::index::DatasetIndexInternalExt;
        use lance::index::vector::ivf::v2::IvfPq as LanceIvfPq;
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::vector::VectorIndex as LanceVectorIndex;

        let indices = native_table.load_indices().await.unwrap();
        let index_uuid = uuid::Uuid::parse_str(&indices[0].index_uuid).unwrap();

        let dataset_guard = native_table.dataset.get().await.unwrap();
        let dataset = (*dataset_guard).clone();
        drop(dataset_guard);

        let lance_index = dataset
            .open_vector_index("embeddings", &index_uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_index = lance_index
            .as_any()
            .downcast_ref::<LanceIvfPq>()
            .expect("expected IvfPq index");
        let partition_count = ivf_index.ivf_model().num_partitions();

        let expected_partitions = num_rows / PARTITION_SIZE;
        assert_eq!(partition_count, expected_partitions);
    }

    #[tokio::test]
    async fn test_seeded_training_sampling_stays_uniform_after_invalid_vectors() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        const NUM_ROWS: usize = 1_000;
        const DIMENSION: usize = 2;
        let values = Float32Array::from_iter_values((0..NUM_ROWS).flat_map(|row| {
            let value = if row % 10 == 0 { f32::NAN } else { row as f32 };
            [value, 1.0]
        }));
        let vectors = Arc::new(create_fixed_size_list(values, DIMENSION as i32).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            vectors.data_type().clone(),
            false,
        )]));
        let table = conn
            .create_table(
                "sampling",
                RecordBatch::try_new(schema, vec![vectors]).unwrap(),
            )
            .execute()
            .await
            .unwrap();
        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();

        let first =
            super::NativeTable::seeded_training_data(dataset.as_ref(), "embeddings", 100, 42)
                .await
                .unwrap();
        let second =
            super::NativeTable::seeded_training_data(dataset.as_ref(), "embeddings", 100, 42)
                .await
                .unwrap();
        assert_eq!(first, second);

        let offsets = first
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .chunks_exact(DIMENSION)
            .map(|vector| vector[0])
            .collect::<Vec<_>>();
        let mean = offsets.iter().sum::<f32>() / offsets.len() as f32;
        assert!((400.0..600.0).contains(&mean), "sample mean was {mean}");
        assert!(offsets.iter().any(|offset| *offset > 800.0));
    }

    #[test]
    fn test_seeded_multivector_sampling_batch_is_bounded() {
        assert_eq!(
            super::NativeTable::seeded_sampling_batch_rows(65_536, true, None),
            128
        );
        assert_eq!(
            super::NativeTable::seeded_sampling_batch_rows(65_536, true, Some(512)),
            128
        );
        assert_eq!(
            super::NativeTable::seeded_sampling_batch_rows(65_536, false, None),
            8192
        );
    }

    #[test]
    fn test_seeded_kmeans_hierarchical_path_is_reproducible() {
        const NUM_VECTORS: usize = 4096;
        const DIMENSION: usize = 2;
        const NUM_CENTROIDS: usize = 257;
        let values = Float32Array::from_iter_values((0..NUM_VECTORS).flat_map(|row| {
            let row = row as f32;
            [row, (row * 0.618_034).fract() * 1000.0]
        }));
        let vectors = create_fixed_size_list(values, DIMENSION as i32).unwrap();
        let first = super::NativeTable::train_seeded_kmeans(
            &vectors,
            NUM_CENTROIDS,
            3,
            super::LanceDistanceType::L2,
            1.0,
            42,
        )
        .unwrap();
        let second = super::NativeTable::train_seeded_kmeans(
            &vectors,
            NUM_CENTROIDS,
            3,
            super::LanceDistanceType::L2,
            1.0,
            42,
        )
        .unwrap();
        assert_eq!(first.len(), NUM_CENTROIDS);
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn test_seeded_ivf_pq_training_supports_multivectors() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        const NUM_ROWS: usize = 64;
        const VECTORS_PER_ROW: usize = 4;
        const DIMENSION: i32 = 2;
        let mut builder =
            ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), DIMENSION));
        for row in 0..NUM_ROWS {
            for subvector in 0..VECTORS_PER_ROW {
                let ordinal = row * VECTORS_PER_ROW + subvector + 1;
                builder.values().values().append_value(ordinal as f32);
                builder.values().values().append_value((ordinal * 7) as f32);
                builder.values().append(true);
            }
            builder.append(true);
        }
        let vectors = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            vectors.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![vectors]).unwrap();
        let first = conn
            .create_table("multivector_first", batch.clone())
            .execute()
            .await
            .unwrap();
        let second = conn
            .create_table("multivector_second", batch)
            .execute()
            .await
            .unwrap();
        let index = IvfPqIndexBuilder::default()
            .distance_type(crate::DistanceType::Cosine)
            .num_partitions(4)
            .num_sub_vectors(2)
            .num_bits(4)
            .sample_rate(8)
            .max_iterations(5)
            .seed(42);
        first
            .create_index(&["embeddings"], Index::IvfPq(index.clone()))
            .execute()
            .await
            .unwrap();
        second
            .create_index(&["embeddings"], Index::IvfPq(index))
            .execute()
            .await
            .unwrap();

        let first_models = trained_ivf_pq_models(&first).await;
        let second_models = trained_ivf_pq_models(&second).await;
        assert_eq!(first_models, second_models);
    }

    #[tokio::test]
    async fn test_seeded_ivf_pq_async_training_starts_inside_job() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        const NUM_ROWS: usize = 32_768;
        const DIMENSION: usize = 64;
        let values = Float32Array::from_iter_values(
            (0..NUM_ROWS * DIMENSION).map(|value| (value % 1009) as f32 / 1009.0),
        );
        let vectors = Arc::new(create_fixed_size_list(values, DIMENSION as i32).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            vectors.data_type().clone(),
            false,
        )]));
        let table = conn
            .create_table(
                "async_seeded",
                RecordBatch::try_new(schema, vec![vectors]).unwrap(),
            )
            .execute()
            .await
            .unwrap();
        let index = IvfPqIndexBuilder::default()
            .num_partitions(64)
            .num_sub_vectors(4)
            .num_bits(4)
            .sample_rate(64)
            .max_iterations(10)
            .seed(42);

        let job = tokio::time::timeout(
            Duration::from_secs(1),
            table
                .create_index(&["embeddings"], Index::IvfPq(index))
                .execute_async(),
        )
        .await
        .expect("execute_async should return before seeded training")
        .unwrap();
        job.cancel().await.unwrap();
        assert!(matches!(
            job.wait().await,
            Err(crate::Error::JobCancelled { .. })
        ));
        assert!(table.list_indices().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_seeded_ivf_pq_training_is_reproducible_across_tables() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        const NUM_ROWS: usize = 512;
        const DIMENSION: usize = 8;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIMENSION as i32,
            ),
            false,
        )]));
        // Eight unique vectors force empty-cluster repair while PQ trains 16
        // centroids, covering the deterministic fallback as well as sampling.
        let values = Float32Array::from_iter_values((0..NUM_ROWS).flat_map(|row| {
            (0..DIMENSION)
                .map(move |column| (((row % 8) * 97 + column * 31) % 1009) as f32 / 1009.0)
        }));
        let vectors =
            Arc::new(create_fixed_size_list(values, DIMENSION as i32).expect("valid vector array"));
        let batch = RecordBatch::try_new(schema, vec![vectors]).unwrap();

        let first = conn
            .create_table("first", batch.clone())
            .execute()
            .await
            .unwrap();
        let second = conn.create_table("second", batch).execute().await.unwrap();
        let index = IvfPqIndexBuilder::default()
            .num_partitions(4)
            .num_sub_vectors(2)
            .num_bits(4)
            .sample_rate(8)
            .max_iterations(5)
            .seed(42);
        first
            .create_index(&["embeddings"], Index::IvfPq(index.clone()))
            .execute()
            .await
            .unwrap();
        second
            .create_index(&["embeddings"], Index::IvfPq(index))
            .execute()
            .await
            .unwrap();

        let (first_centroids, first_codebook) = trained_ivf_pq_models(&first).await;
        let (second_centroids, second_codebook) = trained_ivf_pq_models(&second).await;
        assert_eq!(first_centroids, second_centroids);
        assert_eq!(first_codebook, second_codebook);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_sq() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswSqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswSq(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswSq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_pq() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswPq(index))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices: Vec<crate::index::vector::VectorIndex> =
            table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_flat() {
        use std::iter::repeat_with;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let index = IvfHnswFlatIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswFlat(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswFlat);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        let stats = table.index_stats(&index.name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));
    }

    #[tokio::test]
    async fn test_create_scalar_index() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("i", Int32, [1])).unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can create an index on a scalar column (will default to btree)
        table
            .create_index(&["i"], Index::Auto)
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["i_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        // Can also specify btree
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        // The richer metadata surfaced from describe_indices should be populated.
        assert!(index.index_uuid.is_some());
        assert!(index.type_url.is_some());
        assert_eq!(index.num_segments, Some(1));
        assert_eq!(index.num_indexed_rows, Some(1));
        assert_eq!(index.num_unindexed_rows, Some(0));
        assert!(index.created_at.is_some());
        assert!(index.size_bytes.is_some());
        assert!(index.index_version.is_some());
        assert!(index.index_details.is_some());

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 1);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::BTree);
        assert_eq!(stats.distance_type, None);

        // Rows added after the index was built appear as unindexed.
        let new_batch = record_batch!(("i", Int32, [2])).unwrap();
        table.add(new_batch).execute().await.unwrap();
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 1);
        assert_eq!(stats.num_unindexed_rows, 1);
    }

    #[tokio::test]
    async fn test_create_fm_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["hello world"]))],
        )
        .unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], Index::Fm(FmIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["text_idx"], Duration::from_millis(10))
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Fm);
        assert_eq!(index.columns, vec!["text".to_string()]);

        let count = table
            .query()
            .only_if("contains(text, 'world')")
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(count, 1);

        let stats = table.index_stats("text_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 1);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::Fm);
        assert_eq!(stats.distance_type, None);
    }

    #[tokio::test]
    async fn test_create_index_nested_field_paths() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let num_rows = 512;
        let dimension = 8;

        let row_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let row_dash_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let top_user_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;

        let metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user_id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let mixed_case_metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("userId", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let vector_values = arrow_array::Float32Array::from_iter_values(
            (0..num_rows * dimension).map(|v| v as f32),
        );
        let embeddings =
            Arc::new(create_fixed_size_list(vector_values, dimension).unwrap()) as ArrayRef;
        let image = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(
                "embedding",
                embeddings.data_type().clone(),
                false,
            )),
            embeddings,
        )]));

        let payload = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("text", DataType::Utf8, false)),
            Arc::new(StringArray::from_iter_values(
                (0..num_rows).map(|i| format!("document {}", i)),
            )) as ArrayRef,
        )]));

        let meta_data = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user-id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let literal = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a.b", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("rowId", DataType::Int32, false),
            Field::new("row-id", DataType::Int32, false),
            Field::new("userId", DataType::Int32, false),
            Field::new("metadata", metadata.data_type().clone(), false),
            Field::new("MetaData", mixed_case_metadata.data_type().clone(), false),
            Field::new("image", image.data_type().clone(), false),
            Field::new("payload", payload.data_type().clone(), false),
            Field::new("meta-data", meta_data.data_type().clone(), false),
            Field::new("literal", literal.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                row_id,
                row_dash_id,
                top_user_id,
                metadata,
                mixed_case_metadata,
                image,
                payload,
                meta_data,
                literal,
            ],
        )
        .unwrap();

        let table = conn
            .create_table("nested_index_paths", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(
                &["metadata.user_id"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["rowId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["`row-id`"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_dash_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["userId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("top_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["MetaData.userId"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("mixed_case_metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["image.embedding"], Index::Auto)
            .name("image_embedding_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["payload.text"], Index::FTS(Default::default()))
            .name("payload_text_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["`meta-data`.`user-id`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("escaped_names_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["literal.`a.b`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("literal_dot_idx".to_string())
            .execute()
            .await
            .unwrap();

        let mut index_configs = table.list_indices().await.unwrap();
        index_configs.sort_by(|left, right| left.name.cmp(&right.name));

        let indexed_columns = index_configs
            .iter()
            .map(|index| {
                (
                    index.name.as_str(),
                    index.columns.as_slice(),
                    index.index_type.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            indexed_columns,
            vec![
                (
                    "escaped_names_idx",
                    &["`meta-data`.`user-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "image_embedding_idx",
                    &["image.embedding".to_string()][..],
                    crate::index::IndexType::IvfPq,
                ),
                (
                    "literal_dot_idx",
                    &["literal.`a.b`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "metadata_user_id_idx",
                    &["metadata.user_id".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "mixed_case_metadata_user_id_idx",
                    &["MetaData.userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "payload_text_idx",
                    &["payload.text".to_string()][..],
                    crate::index::IndexType::FTS,
                ),
                (
                    "row_dash_id_idx",
                    &["`row-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "row_id_idx",
                    &["rowId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "top_user_id_idx",
                    &["userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
            ]
        );

        let vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .column("image.embedding")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let default_vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            default_vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let fts_results = table
            .query()
            .full_text_search(FullTextSearchQuery::new("document".to_string()))
            .limit(5)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!fts_results.is_empty());

        let filtered_results = table
            .query()
            .only_if("metadata.user_id = 42")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            filtered_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn test_create_bitmap_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("large_category", DataType::LargeUtf8, true),
            Field::new("is_active", DataType::Boolean, true),
            Field::new("data", DataType::Binary, true),
            Field::new("large_data", DataType::LargeBinary, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(StringArray::from_iter_values(
                    (0..100).map(|i| format!("category_{}", i % 5)),
                )),
                Arc::new(LargeStringArray::from_iter_values(
                    (0..100).map(|i| format!("large_category_{}", i % 5)),
                )),
                Arc::new(BooleanArray::from_iter((0..100).map(|i| Some(i % 2 == 0)))),
                Arc::new(BinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "category" column
        table
            .create_index(&["category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "is_active" column
        table
            .create_index(&["is_active"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "data" column
        table
            .create_index(&["data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_data" column
        table
            .create_index(&["large_data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_category" column
        table
            .create_index(&["large_category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 5);

        // list_indices returns indices in alphabetical order by name
        let mut configs_iter = index_configs.into_iter();
        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["data".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["is_active".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_data".to_string()]);

        let stats = table.index_stats("category_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 100);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(stats.distance_type, None);
    }

    #[tokio::test]
    async fn test_create_label_list_index() {
        let conn = connect("memory://").execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Field::new("item", DataType::Utf8, true).into()),
                true,
            ),
        ]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..40)), tags],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can not create btree or bitmap index on list column
        assert!(
            table
                .create_index(&["tags"], Index::BTree(Default::default()))
                .execute()
                .await
                .is_err()
        );
        assert!(
            table
                .create_index(&["tags"], Index::Bitmap(Default::default()))
                .execute()
                .await
                .is_err()
        );

        // Create bitmap index on the "category" column
        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);

        let stats = table.index_stats("tags_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 40);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::LabelList);
        assert_eq!(stats.distance_type, None);
    }

    #[tokio::test]
    async fn test_create_label_list_index_on_large_list() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::LargeList(Field::new("item", DataType::Utf8, true).into()),
            true,
        )]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = LargeListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![tags]).unwrap();

        let table = conn
            .create_table("test_large_list_label_list", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);
    }

    #[tokio::test]
    async fn test_create_inverted_index() {
        let conn = connect("memory://").execute().await.unwrap();

        let id = Int32Array::from_iter_values(0..120_i32);
        let text = StringArray::from_iter_values((0..120).map(|i| {
            const WORDS: [&str; 3] = ["cat", "dog", "fish"];
            WORDS[i % 3].to_string()
        }));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![Arc::new(id) as ArrayRef, Arc::new(text) as ArrayRef],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(
                &["text"],
                Index::FTS(
                    FtsIndexBuilder::default()
                        .stem(false)
                        .custom_stop_words(Some(vec!["cat".to_string()]))
                        .block_size(256)
                        .unwrap(),
                ),
            )
            .execute()
            .await
            .unwrap();
        drop(table);
        let table = conn.open_table("test_bitmap").execute().await.unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::FTS);
        assert_eq!(index.columns, vec!["text".to_string()]);
        assert_eq!(index.name, "text_idx");
        assert_eq!(index.index_version, Some(3));
        let index_params: FtsIndexBuilder =
            serde_json::from_str(index.index_details.as_deref().unwrap()).unwrap();
        assert_eq!(index_params.posting_block_size(), 256);
        assert_eq!(
            serde_json::to_value(&index_params).unwrap()["custom_stop_words"],
            serde_json::json!(["cat"])
        );
        assert_eq!(
            table
                .tokenize("cat dog", "text_idx")
                .await
                .unwrap()
                .into_iter()
                .map(|token| token.text)
                .collect::<Vec<_>>(),
            vec!["dog"]
        );

        let batches = table
            .query()
            .full_text_search(FullTextSearchQuery::new("cat dog".to_string()))
            .limit(120)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 40);

        let num_rows = 120;
        let stats = table.index_stats("text_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, num_rows);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::FTS);
        assert_eq!(stats.distance_type, None);

        // Make sure we can call prewarm without error
        table.prewarm_index("text_idx").await.unwrap();
    }

    #[tokio::test]
    pub async fn test_list_indices_skip_frag_reuse() {
        let conn = connect("memory://").execute().await.unwrap();

        let id = Int32Array::from_iter_values(0..100);
        let foo = Int32Array::from_iter_values(0..100);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("foo", DataType::Int32, true),
            ])),
            vec![Arc::new(id) as ArrayRef, Arc::new(foo) as ArrayRef],
        )
        .unwrap();

        let table = conn
            .create_table("test_list_indices_skip_frag_reuse", batch.clone())
            .execute()
            .await
            .unwrap();

        table.add(batch.clone()).execute().await.unwrap();

        table
            .create_index(&["id"], Index::Bitmap(BitmapIndexBuilder {}))
            .execute()
            .await
            .unwrap();

        table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions {
                    target_rows_per_fragment: 2_000,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.list_indices().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_type, crate::index::IndexType::Bitmap);
    }
}
