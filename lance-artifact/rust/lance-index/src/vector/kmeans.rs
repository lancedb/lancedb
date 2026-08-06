// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! KMeans implementation for Apache Arrow Arrays.
//!
//! Support ``l2``, ``cosine`` and ``dot`` distances, see [DistanceType].
//!
//! ``Cosine`` distance are calculated by normalizing the vectors to unit length,
//! and run ``l2`` distance on the unit vectors.
//!

use core::f32;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ops::{AddAssign, DivAssign};
use std::sync::Arc;
use std::vec;
use std::{collections::HashMap, ops::MulAssign};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, PrimitiveArray, UInt32Array,
    cast::AsArray,
    types::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type, UInt8Type},
};
use arrow_array::{ArrowNumericType, UInt8Array};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{ArrowError, DataType};
use bitvec::prelude::*;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_linalg::distance::hamming::{hamming, hamming_distance_batch};
use lance_linalg::distance::{DistanceType, Normalize, dot_distance_batch};
use lance_linalg::kernels::{argmin_value_float, argmin_value_float_with_bias};
use log::{info, warn};
use num_traits::One;
use num_traits::{AsPrimitive, Float, FromPrimitive, Num, Zero};
use rand::prelude::*;
use rayon::prelude::*;
use {
    lance_linalg::distance::{
        Dot,
        l2::{L2, l2_distance_batch},
    },
    lance_linalg::kernels::argmin_value,
};

use crate::vector::utils::SimpleIndex;
use lance_core::{Error, Result};

/// KMean initialization method.
#[derive(Debug, PartialEq)]
pub enum KMeanInit {
    Random,
    Incremental(Arc<FixedSizeListArray>),
}

/// KMean Training Parameters
pub struct KMeansParams {
    /// Max number of iterations.
    pub max_iters: u32,

    /// When the difference of mean distance to the centroids is less than this `tolerance`
    /// threshold, stop the training.
    pub tolerance: f64,

    /// Run kmeans multiple times and pick the best (balanced) one.
    pub redos: usize,

    /// Init methods.
    pub init: KMeanInit,

    /// The metric to calculate distance.
    pub distance_type: DistanceType,

    /// Balance factor for the kmeans clustering.
    /// Higher value means more balanced clustering.
    ///
    /// Setting this value to 0 means no balance factor,
    /// which is the same as normal kmeans clustering.
    pub balance_factor: f32,

    /// The number of clusters to train in each hierarchical level.
    ///
    /// Default is 16, which performs the best performance in our experiments.
    /// Higher would split the clusters more aggressively, which would be more accurate but slower.
    /// hierarchical kmeans is enabled only if hierarchical_k > 1 and k > 256.
    pub hierarchical_k: usize,

    /// Optional sync callback for iteration progress: (current_iteration, max_iterations).
    pub on_progress: Option<Arc<dyn Fn(u32, u32) + Send + Sync>>,
}

impl std::fmt::Debug for KMeansParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KMeansParams")
            .field("max_iters", &self.max_iters)
            .field("tolerance", &self.tolerance)
            .field("redos", &self.redos)
            .field("init", &self.init)
            .field("distance_type", &self.distance_type)
            .field("balance_factor", &self.balance_factor)
            .field("hierarchical_k", &self.hierarchical_k)
            .field("on_progress", &self.on_progress.as_ref().map(|_| "..."))
            .finish()
    }
}

impl Default for KMeansParams {
    fn default() -> Self {
        Self {
            max_iters: 50,
            tolerance: 1e-4,
            redos: 1,
            init: KMeanInit::Random,
            distance_type: DistanceType::L2,
            balance_factor: 0.0,
            hierarchical_k: 16,
            on_progress: None,
        }
    }
}

impl KMeansParams {
    pub fn new(
        centroids: Option<Arc<FixedSizeListArray>>,
        max_iters: u32,
        redos: usize,
        distance_type: DistanceType,
    ) -> Self {
        let init = match centroids {
            Some(centroids) => KMeanInit::Incremental(centroids),
            None => KMeanInit::Random,
        };
        Self {
            max_iters,
            redos,
            distance_type,
            init,
            ..Default::default()
        }
    }

    /// Set the balance factor for the kmeans clustering.
    ///
    /// Higher value means more balanced clustering.
    /// Setting this value to 0 means no balance factor,
    /// which is the same as normal kmeans clustering.
    pub fn with_balance_factor(mut self, balance_factor: f32) -> Self {
        self.balance_factor = balance_factor;
        self
    }

    pub fn with_on_progress(mut self, cb: Arc<dyn Fn(u32, u32) + Send + Sync>) -> Self {
        self.on_progress = Some(cb);
        self
    }

    /// Set the number of clusters to train in each hierarchical level.
    ///
    /// Higher would split the clusters more aggressively, which would be more accurate but slower.
    /// hierarchical kmeans is enabled only if hierarchical_k > 1 and k > 256.
    pub fn with_hierarchical_k(mut self, hierarchical_k: usize) -> Self {
        self.hierarchical_k = hierarchical_k;
        self
    }
}

/// Randomly initialize kmeans centroids.
///
///
fn kmeans_random_init<T: ArrowPrimitiveType>(
    data: &[T::Native],
    dimension: usize,
    k: usize,
    mut rng: impl Rng,
    distance_type: DistanceType,
) -> KMeans {
    assert!(data.len() >= k * dimension);
    let chosen = (0..data.len() / dimension).choose_multiple(&mut rng, k);
    let centroids = PrimitiveArray::<T>::from_iter_values(
        chosen
            .iter()
            .flat_map(|&i| data[i * dimension..(i + 1) * dimension].iter())
            .copied(),
    );
    KMeans {
        centroids: Arc::new(centroids),
        dimension,
        distance_type,
        loss: f64::MAX,
    }
}

/// Split one big cluster into two smaller clusters. After split, each
/// cluster has approximately half of the vectors.
fn split_clusters<T: Float + MulAssign>(
    n: usize,
    cnts: &mut [usize],
    centroids: &mut [T],
    dim: usize,
) {
    let eps = T::from(1.0 / 1024.0).unwrap();
    let mut rng = SmallRng::from_os_rng();
    for i in 0..cnts.len() {
        if cnts[i] == 0 {
            let mut j = 0;
            loop {
                let p = (cnts[j] as f32 - 1.0) / (n - cnts.len()) as f32;
                if rng.random::<f32>() < p {
                    break;
                }
                j += 1;
                j %= cnts.len();
            }

            cnts[i] = cnts[j] / 2;
            cnts[j] -= cnts[i];
            for k in 0..dim {
                if k % 2 == 0 {
                    centroids[i * dim + k] = centroids[j * dim + k] * (T::one() + eps);
                    centroids[j * dim + k] *= T::one() - eps;
                } else {
                    centroids[i * dim + k] = centroids[j * dim + k] * (T::one() - eps);
                    centroids[j * dim + k] *= T::one() + eps;
                }
            }
        }
    }
}

// compute the cluster sizes and return adjusted balance factor
fn compute_cluster_sizes(
    membership: &[Option<u32>],
    radius: &[f32],
    losses: &[f64],
    cluster_sizes: &mut [usize],
) -> f32 {
    cluster_sizes.fill(0);
    let mut max_cluster_id = 0;
    let mut max_cluster_size = 0;
    membership.iter().for_each(|cluster_id| {
        if let Some(cluster_id) = cluster_id {
            let cluster_id = *cluster_id as usize;
            cluster_sizes[cluster_id] += 1;
            if cluster_sizes[cluster_id] > max_cluster_size {
                max_cluster_size = cluster_sizes[cluster_id];
                max_cluster_id = cluster_id;
            }
        }
    });

    (radius[max_cluster_id] - losses[max_cluster_id] as f32 / cluster_sizes[max_cluster_id] as f32)
        / membership.len() as f32
}

fn compute_balance_loss(cluster_sizes: &[usize], n: usize, balance_factor: f32) -> f32 {
    let size_loss = cluster_sizes.iter().map(|size| size.pow(2)).sum::<usize>() as f32;
    balance_factor * (size_loss - n.pow(2) as f32 / cluster_sizes.len() as f32)
}

pub trait KMeansAlgo<T: Num> {
    /// Recompute the membership of each vector.
    ///
    /// Parameters:
    ///
    /// - *data*: a `N * dimension` floating array. Not necessarily normalized.
    ///
    /// Returns:
    /// - *membership*: the membership of each vector.
    /// - *cluster_radius*: the radius of each cluster.
    /// - *losses*: the losses of each cluster.
    fn compute_membership_and_loss(
        centroids: &[T],
        data: &[T],
        dimension: usize,
        distance_type: DistanceType,
        balance_factor: f32,
        cluster_sizes: Option<&[usize]>,
        index: Option<&SimpleIndex>,
    ) -> (Vec<Option<u32>>, Vec<f32>, Vec<f64>) {
        let (membership, dists) = Self::compute_membership_and_dist(
            centroids,
            data,
            dimension,
            distance_type,
            balance_factor,
            cluster_sizes,
            index,
        );

        let k = centroids.len() / dimension;
        let mut cluster_radius = vec![0.0; k];
        let mut losses = vec![0.0; k];
        for (cluster_id, dist) in membership.iter().zip(dists.iter()) {
            if let (Some(cluster_id), Some(dist)) = (cluster_id, dist) {
                let cluster_id = *cluster_id as usize;
                cluster_radius[cluster_id] = cluster_radius[cluster_id].max(*dist);
                losses[cluster_id] += *dist as f64;
            }
        }

        (membership, cluster_radius, losses)
    }

    fn compute_membership_and_dist(
        centroids: &[T],
        data: &[T],
        dimension: usize,
        distance_type: DistanceType,
        balance_factor: f32,
        cluster_sizes: Option<&[usize]>,
        index: Option<&SimpleIndex>,
    ) -> (Vec<Option<u32>>, Vec<Option<f32>>);

    /// Construct a new KMeans model.
    fn to_kmeans(
        data: &[T],
        dimension: usize,
        k: usize,
        membership: &[Option<u32>],
        cluster_sizes: &mut [usize],
        distance_type: DistanceType,
        loss: f64,
    ) -> KMeans;
}

pub struct KMeansAlgoFloat<T: ArrowNumericType>
where
    T::Native: Float + Num,
{
    phantom_data: std::marker::PhantomData<T>,
}

impl<T: ArrowNumericType> KMeansAlgo<T::Native> for KMeansAlgoFloat<T>
where
    T::Native: Float + Dot + L2 + MulAssign + DivAssign + AddAssign + FromPrimitive + Sync,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    fn compute_membership_and_dist(
        centroids: &[T::Native],
        data: &[T::Native],
        dimension: usize,
        distance_type: DistanceType,
        balance_factor: f32,
        cluster_sizes: Option<&[usize]>,
        index: Option<&SimpleIndex>,
    ) -> (Vec<Option<u32>>, Vec<Option<f32>>) {
        let cluster_and_dists = match index {
            Some(index) => data
                .par_chunks(dimension)
                .map(|vec| {
                    let query = PrimitiveArray::<T>::from_iter_values(vec.iter().copied());
                    // unable to use balance_factor here because index.search returns the closest centroid
                    index
                        .search(Arc::new(query))
                        .map(|(id, dist)| Some((id, dist)))
                        .unwrap()
                })
                .collect::<Vec<_>>(),
            None => match distance_type {
                DistanceType::L2 => data
                    .par_chunks(dimension)
                    .map(|vec| {
                        argmin_value_float_with_bias(
                            l2_distance_batch(vec, centroids, dimension),
                            cluster_sizes
                                .map(|size| size.iter().map(|size| balance_factor * *size as f32)),
                        )
                    })
                    .collect::<Vec<_>>(),
                DistanceType::Dot => data
                    .par_chunks(dimension)
                    .map(|vec| {
                        argmin_value_float_with_bias(
                            dot_distance_batch(vec, centroids, dimension),
                            cluster_sizes
                                .map(|size| size.iter().map(|size| balance_factor * *size as f32)),
                        )
                    })
                    .collect::<Vec<_>>(),
                _ => {
                    panic!(
                        "KMeans::find_partitions: {} is not supported",
                        distance_type
                    );
                }
            },
        };

        cluster_and_dists.into_iter().map(Option::unzip).unzip()
    }

    fn to_kmeans(
        data: &[T::Native],
        dimension: usize,
        k: usize,
        membership: &[Option<u32>],
        cluster_sizes: &mut [usize],
        distance_type: DistanceType,
        loss: f64,
    ) -> KMeans {
        let mut centroids = vec![T::Native::zero(); k * dimension];

        let mut num_cpus = get_num_compute_intensive_cpus();
        if k < num_cpus || k < 16 {
            num_cpus = 1;
        }
        let chunk_size = k / num_cpus;

        centroids
            .par_chunks_mut(dimension * chunk_size)
            .enumerate()
            .with_max_len(1)
            .for_each(|(i, centroids)| {
                let start = i * chunk_size;
                let end = ((i + 1) * chunk_size).min(k);
                data.chunks(dimension)
                    .zip(membership.iter())
                    .filter_map(|(vector, cluster_id)| {
                        cluster_id.map(|cluster_id| (vector, cluster_id as usize))
                    })
                    .for_each(|(vector, cluster_id)| {
                        if start <= cluster_id && cluster_id < end {
                            let local_id = cluster_id - start;
                            let centroid =
                                &mut centroids[local_id * dimension..(local_id + 1) * dimension];
                            centroid.iter_mut().zip(vector).for_each(|(c, v)| *c += *v);
                        }
                    });
            });

        centroids
            .par_chunks_mut(dimension)
            .zip(cluster_sizes.par_iter())
            .for_each(|(centroid, &cnt)| {
                if cnt > 0 {
                    let norm = T::Native::one() / T::Native::from_usize(cnt).unwrap();
                    centroid.iter_mut().for_each(|v| *v *= norm);
                }
            });

        let empty_clusters = cluster_sizes.iter().filter(|&cnt| *cnt == 0).count();
        if empty_clusters as f32 / k as f32 > 0.1 {
            if data.len() / dimension < k * 256 {
                warn!(
                    "KMeans: more than 10% of clusters are empty: {} of {}.\nHelp: this could mean your dataset \
                is too small to have a meaningful index ({} < {}) or has many duplicate vectors.",
                    empty_clusters,
                    k,
                    data.len() / dimension,
                    k * 256
                );
            } else {
                warn!(
                    "KMeans: more than 10% of clusters are empty: {} of {}.\nHelp: this could mean your dataset \
                has many duplicate vectors.",
                    empty_clusters, k
                );
            }
        }

        split_clusters(
            data.len() / dimension,
            cluster_sizes,
            &mut centroids,
            dimension,
        );

        KMeans {
            centroids: Arc::new(PrimitiveArray::<T>::from(centroids)),
            dimension,
            distance_type,
            loss,
        }
    }
}

struct KModeAlgo {}

impl KMeansAlgo<u8> for KModeAlgo {
    fn compute_membership_and_dist(
        centroids: &[u8],
        data: &[u8],
        dimension: usize,
        distance_type: DistanceType,
        balance_factor: f32,
        cluster_sizes: Option<&[usize]>,
        _: Option<&SimpleIndex>,
    ) -> (Vec<Option<u32>>, Vec<Option<f32>>) {
        assert_eq!(distance_type, DistanceType::Hamming);
        let cluster_and_dists = data
            .par_chunks(dimension)
            .map(|vec| {
                argmin_value(
                    centroids
                        .chunks_exact(dimension)
                        .enumerate()
                        .map(|(id, c)| {
                            hamming(vec, c)
                                + balance_factor
                                    * cluster_sizes.map(|sizes| sizes[id] as f32).unwrap_or(0.0)
                        }),
                )
            })
            .collect::<Vec<_>>();
        cluster_and_dists.into_iter().map(Option::unzip).unzip()
    }

    fn to_kmeans(
        data: &[u8],
        dimension: usize,
        k: usize,
        membership: &[Option<u32>],
        _cluster_sizes: &mut [usize],
        distance_type: DistanceType,
        loss: f64,
    ) -> KMeans {
        assert_eq!(distance_type, DistanceType::Hamming);

        let mut clusters = HashMap::<u32, Vec<usize>>::new();
        membership.iter().enumerate().for_each(|(i, part_id)| {
            if let Some(part_id) = part_id {
                clusters.entry(*part_id).or_default().push(i);
            }
        });
        let centroids = (0..k as u32)
            .into_par_iter()
            .flat_map(|part_id| {
                if let Some(vecs) = clusters.get(&part_id) {
                    let mut ones = vec![0_u32; dimension * 8];
                    let cnt = vecs.len() as u32;
                    vecs.iter().for_each(|&i| {
                        let vec = &data[i * dimension..(i + 1) * dimension];
                        ones.iter_mut()
                            .zip(vec.view_bits::<Lsb0>())
                            .for_each(|(c, v)| {
                                if *v.as_ref() {
                                    *c += 1;
                                }
                            });
                    });

                    let bits = ones.iter().map(|&c| c * 2 > cnt).collect::<BitVec<u8>>();
                    bits.as_raw_slice()
                        .iter()
                        .copied()
                        .map(Some)
                        .collect::<Vec<_>>()
                } else {
                    vec![None; dimension]
                }
            })
            .collect::<Vec<_>>();

        KMeans {
            centroids: Arc::new(UInt8Array::from(centroids)),
            dimension,
            distance_type,
            loss,
        }
    }
}

/// Cluster id assignment for each vector in a batch.
pub type KMeansMembership = Vec<Option<u32>>;

/// Distance from each vector to its assigned centroid.
pub type KMeansDistances = Vec<Option<f32>>;

/// Maximum assignment distance per centroid.
pub type KMeansClusterRadii = Vec<f32>;

/// Sum of assignment distances per centroid.
pub type KMeansClusterLosses = Vec<f64>;

/// Batch assignment results with per-centroid radii and losses.
pub type KMeansMembershipAndLoss = (KMeansMembership, KMeansClusterRadii, KMeansClusterLosses);

/// Batch assignment results with per-vector distances.
pub type KMeansMembershipAndDistances = (KMeansMembership, KMeansDistances);

/// KMeans implementation for Apache Arrow Arrays.
#[derive(Debug, Clone)]
pub struct KMeans {
    /// Flattened array of centroids.
    ///
    /// dimension * k of floating number.
    pub centroids: ArrayRef,

    /// The dimension of each vector.
    pub dimension: usize,

    /// How to calculate distance between two vectors.
    pub distance_type: DistanceType,

    /// The loss of the last training.
    pub loss: f64,
}

impl KMeans {
    fn empty(dimension: usize, distance_type: DistanceType) -> Self {
        Self {
            centroids: arrow_array::array::new_empty_array(&DataType::Float32),
            dimension,
            distance_type,
            loss: f64::MAX,
        }
    }

    /// Create a [`KMeans`] with existing centroids.
    /// It is useful for continuing training.
    pub fn with_centroids(
        centroids: ArrayRef,
        dimension: usize,
        distance_type: DistanceType,
        loss: f64,
    ) -> Self {
        assert!(matches!(
            centroids.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::UInt8
        ));
        Self {
            centroids,
            dimension,
            distance_type,
            loss,
        }
    }

    /// Initialize a [`KMeans`] with random centroids.
    ///
    /// Parameters
    /// - *data*: training data. provided to do samplings.
    /// - *k*: the number of clusters.
    /// - *distance_type*: the distance type to calculate distance.
    /// - *rng*: random generator.
    fn init_random<T: ArrowPrimitiveType>(
        data: &[T::Native],
        dimension: usize,
        k: usize,
        rng: impl Rng,
        distance_type: DistanceType,
    ) -> Self {
        kmeans_random_init::<T>(data, dimension, k, rng, distance_type)
    }

    /// Train a KMeans model on data with `k` clusters.
    pub fn new(data: &FixedSizeListArray, k: usize, max_iters: u32) -> arrow::error::Result<Self> {
        let params = KMeansParams {
            max_iters,
            distance_type: DistanceType::L2,
            ..Default::default()
        };
        Self::new_with_params(data, k, &params)
    }

    /// Assign a batch of vectors to these centroids and return membership, radius, and loss.
    pub fn compute_membership_and_loss(
        &self,
        data: &FixedSizeListArray,
    ) -> arrow::error::Result<KMeansMembershipAndLoss> {
        let (membership, distances) = self.compute_membership_and_distances(data)?;
        let k = self.centroids.len() / self.dimension;
        let mut cluster_radius: Vec<f32> = vec![0.0_f32; k];
        let mut losses = vec![0.0; k];
        for (cluster_id, dist) in membership.iter().zip(distances.iter()) {
            if let (Some(cluster_id), Some(dist)) = (cluster_id, dist) {
                let cluster_id = *cluster_id as usize;
                cluster_radius[cluster_id] = cluster_radius[cluster_id].max(*dist);
                losses[cluster_id] += *dist as f64;
            }
        }
        Ok((membership, cluster_radius, losses))
    }

    /// Assign a batch of vectors to these centroids and return per-vector distances.
    pub fn compute_membership_and_distances(
        &self,
        data: &FixedSizeListArray,
    ) -> arrow::error::Result<KMeansMembershipAndDistances> {
        if data.value_length() as usize != self.dimension {
            return Err(ArrowError::InvalidArgumentError(format!(
                "KMeans: data dimension {} does not match centroid dimension {}",
                data.value_length(),
                self.dimension
            )));
        }

        let index = SimpleIndex::may_train_index(
            self.centroids.clone(),
            self.dimension,
            self.distance_type,
        )
        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        match (
            data.value_type(),
            self.centroids.data_type(),
            self.distance_type,
        ) {
            (DataType::Float16, DataType::Float16, _) => {
                let data_values = data.values().as_primitive::<Float16Type>().values();
                let centroids = self.centroids.as_primitive::<Float16Type>().values();
                Ok(KMeansAlgoFloat::<Float16Type>::compute_membership_and_dist(
                    centroids,
                    data_values,
                    self.dimension,
                    self.distance_type,
                    0.0,
                    None,
                    index.as_ref(),
                ))
            }
            (DataType::Float32, DataType::Float32, _) => {
                let data_values = data.values().as_primitive::<Float32Type>().values();
                let centroids = self.centroids.as_primitive::<Float32Type>().values();
                Ok(KMeansAlgoFloat::<Float32Type>::compute_membership_and_dist(
                    centroids,
                    data_values,
                    self.dimension,
                    self.distance_type,
                    0.0,
                    None,
                    index.as_ref(),
                ))
            }
            (DataType::Float64, DataType::Float64, _) => {
                let data_values = data.values().as_primitive::<Float64Type>().values();
                let centroids = self.centroids.as_primitive::<Float64Type>().values();
                Ok(KMeansAlgoFloat::<Float64Type>::compute_membership_and_dist(
                    centroids,
                    data_values,
                    self.dimension,
                    self.distance_type,
                    0.0,
                    None,
                    index.as_ref(),
                ))
            }
            (DataType::UInt8, DataType::UInt8, DistanceType::Hamming) => {
                let data_values = data.values().as_primitive::<UInt8Type>().values();
                let centroids = self.centroids.as_primitive::<UInt8Type>().values();
                Ok(KModeAlgo::compute_membership_and_dist(
                    centroids,
                    data_values,
                    self.dimension,
                    self.distance_type,
                    0.0,
                    None,
                    index.as_ref(),
                ))
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "KMeans: can not compute membership for data type {} with centroid type {} and distance type {}",
                data.value_type(),
                self.centroids.data_type(),
                self.distance_type
            ))),
        }
    }

    /// Compute the kmeans loss for a batch of vectors against these centroids.
    pub fn compute_loss(&self, data: &FixedSizeListArray) -> arrow::error::Result<f64> {
        let (_, _, losses) = self.compute_membership_and_loss(data)?;
        Ok(losses.iter().sum())
    }

    fn train_kmeans<T: ArrowNumericType, Algo: KMeansAlgo<T::Native>>(
        data: &FixedSizeListArray,
        k: usize,
        params: &KMeansParams,
    ) -> arrow::error::Result<Self>
    where
        T::Native: Num,
    {
        // the data is `num_partitions * sample_rate` vectors,
        // but here `k` may be not `num_partitions` in the case of hierarchical kmeans,
        // so we need to sample the sampled data again here.
        // we have to limit the number of data to avoid division underflow,
        // the threshold 512 is chosen because the minimal normal f16 value will be 0 if divided by 1024.
        let data = if data.len() >= k * 512 {
            data.slice(0, k * 512)
        } else {
            data.clone()
        };

        let n = data.len();
        let dimension = data.value_length() as usize;

        let data =
            data.values()
                .as_primitive_opt::<T>()
                .ok_or(ArrowError::InvalidArgumentError(format!(
                    "KMeans: data must be {}, got: {}",
                    T::DATA_TYPE,
                    data.value_type()
                )))?;

        let mut best_kmeans = Self::empty(dimension, params.distance_type);
        let mut cluster_sizes = vec![0; k];
        let mut adjusted_balance_factor = f32::MAX;

        // TODO: use seed for Rng.
        let mut rng = SmallRng::from_os_rng();
        for redo in 1..=params.redos {
            let mut kmeans: Self = match &params.init {
                KMeanInit::Random => Self::init_random::<T>(
                    data.values(),
                    dimension,
                    k,
                    &mut rng,
                    params.distance_type,
                ),
                KMeanInit::Incremental(centroids) => Self::with_centroids(
                    centroids.values().clone(),
                    dimension,
                    params.distance_type,
                    f64::MAX,
                ),
            };

            let mut loss = f64::MAX;
            for i in 1..=params.max_iters {
                if let Some(cb) = &params.on_progress {
                    cb(i, params.max_iters);
                }
                if i % 10 == 0 {
                    info!(
                        "KMeans training: iteration {} / {}, redo={}",
                        i, params.max_iters, redo
                    );
                };

                let index = SimpleIndex::may_train_index(
                    kmeans.centroids.clone(),
                    kmeans.dimension,
                    kmeans.distance_type,
                )?;

                let balance_factor = adjusted_balance_factor.min(params.balance_factor);
                let (membership, radius, losses) = Algo::compute_membership_and_loss(
                    kmeans.centroids.as_primitive::<T>().values(),
                    data.values(),
                    dimension,
                    params.distance_type,
                    balance_factor,
                    Some(&cluster_sizes),
                    index.as_ref(),
                );

                adjusted_balance_factor =
                    compute_cluster_sizes(&membership, &radius, &losses, &mut cluster_sizes);
                let balance_loss = compute_balance_loss(&cluster_sizes, n, balance_factor);
                let last_loss = losses.iter().sum::<f64>() + balance_loss as f64;

                kmeans = Algo::to_kmeans(
                    data.values(),
                    dimension,
                    k,
                    &membership,
                    &mut cluster_sizes,
                    params.distance_type,
                    last_loss,
                );
                if (loss - last_loss).abs() < params.tolerance * last_loss {
                    info!(
                        "KMeans training: converged at iteration {} / {}, redo={}, loss={}, last_loss={}, loss_diff={}",
                        i,
                        params.max_iters,
                        redo,
                        loss,
                        last_loss,
                        (loss - last_loss).abs() / last_loss
                    );
                    break;
                }
                loss = last_loss;
            }
            if kmeans.loss < best_kmeans.loss {
                best_kmeans = kmeans;
            }
        }

        Ok(best_kmeans)
    }

    /// Helper function to create a FixedSizeListArray from indices
    fn create_array_from_indices<T: ArrowNumericType>(
        indices: &[usize],
        data_values: &[T::Native],
        dimension: usize,
    ) -> arrow::error::Result<FixedSizeListArray>
    where
        T::Native: Clone,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        let mut subset_data = Vec::with_capacity(indices.len() * dimension);
        for &idx in indices {
            let start = idx * dimension;
            let end = start + dimension;
            subset_data.extend_from_slice(&data_values[start..end]);
        }
        let array = PrimitiveArray::<T>::from(subset_data);
        FixedSizeListArray::try_new_from_values(array, dimension as i32)
    }

    /// Train a hierarchical KMeans model when k > 256
    ///
    /// This function implements a hierarchical clustering approach:
    /// 1. Start with k'=256 initial clusters
    /// 2. Iteratively split the largest cluster until we have k clusters
    fn train_hierarchical_kmeans<T: ArrowNumericType, Algo: KMeansAlgo<T::Native>>(
        data: &FixedSizeListArray,
        target_k: usize,
        params: &KMeansParams,
    ) -> arrow::error::Result<Self>
    where
        T::Native: Num,
        PrimitiveArray<T>: From<Vec<T::Native>>,
    {
        // Cluster structure for the heap
        #[derive(Clone, Debug)]
        struct Cluster<N> {
            id: usize,
            indices: Vec<usize>,
            centroid: Vec<N>,
            finalized: bool,
        }

        impl<N> Eq for Cluster<N> {}

        impl<N> PartialEq for Cluster<N> {
            fn eq(&self, other: &Self) -> bool {
                self.indices.len() == other.indices.len()
            }
        }

        impl<N> Ord for Cluster<N> {
            fn cmp(&self, other: &Self) -> Ordering {
                // Non-finalized clusters should always have higher priority than finalized ones
                match (self.finalized, other.finalized) {
                    (false, true) => Ordering::Greater,
                    (true, false) => Ordering::Less,
                    _ => {
                        // Max heap: larger clusters first
                        self.indices.len().cmp(&other.indices.len())
                    }
                }
            }
        }

        impl<N> PartialOrd for Cluster<N> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let n = data.len();
        let dimension = data.value_length() as usize;

        let data_values = data
            .values()
            .as_primitive_opt::<T>()
            .ok_or(ArrowError::InvalidArgumentError(format!(
                "KMeans: data must be {}, got: {}",
                T::DATA_TYPE,
                data.value_type()
            )))?
            .values();

        // Initial clustering with k'=16
        let initial_k = params.hierarchical_k.min(target_k).min(n);
        info!(
            "Hierarchical clustering: initial k={}, target k={}",
            initial_k, target_k
        );

        let initial_kmeans = Self::train_kmeans::<T, Algo>(data, initial_k, params)?;

        // Get membership for all data points
        let (membership, _, _) = Algo::compute_membership_and_loss(
            initial_kmeans.centroids.as_primitive::<T>().values(),
            data_values,
            dimension,
            params.distance_type,
            0.0, // No balance factor for membership computation
            None,
            None,
        );

        // Build initial clusters and add to heap
        let mut heap: BinaryHeap<Cluster<T::Native>> = BinaryHeap::new();
        let mut next_cluster_id = 0;
        let initial_centroids = initial_kmeans.centroids.as_primitive::<T>().values();

        for i in 0..initial_k {
            let mut cluster_indices = Vec::new();
            for (idx, &cluster_id) in membership.iter().enumerate() {
                if let Some(cid) = cluster_id
                    && cid as usize == i
                {
                    cluster_indices.push(idx);
                }
            }

            if !cluster_indices.is_empty() {
                let centroid_start = i * dimension;
                let centroid_end = centroid_start + dimension;
                let centroid = initial_centroids[centroid_start..centroid_end].to_vec();

                heap.push(Cluster {
                    id: next_cluster_id,
                    indices: cluster_indices,
                    centroid,
                    finalized: false,
                });
                next_cluster_id += 1;
            }
        }

        // Iteratively split largest clusters until we have target_k clusters
        while heap.len() < target_k {
            // Get the largest cluster
            let mut largest_cluster = heap.pop().ok_or(ArrowError::InvalidArgumentError(
                "No cluster can be further split".to_string(),
            ))?;

            // If this cluster is already finalized, no further split is possible; stop splitting
            if largest_cluster.finalized {
                log::warn!(
                    "Cluster {} is already finalized, no further split is possible, finish with {} clusters",
                    largest_cluster.id,
                    heap.len() + 1
                );
                heap.push(largest_cluster);
                break;
            }

            // Because the clusters are sorted by size, if the cluster has only 1 point, no further split is possible; stop splitting
            if largest_cluster.indices.len() <= 1 {
                log::warn!(
                    "Cluster {} has only 1 point, no further split is possible, finish with {} clusters",
                    largest_cluster.id,
                    heap.len() + 1
                );
                heap.push(largest_cluster);
                break;
            }

            let cluster_size = largest_cluster.indices.len();
            log::debug!(
                "Splitting cluster {} with {} points (current total clusters: {})",
                largest_cluster.id,
                cluster_size,
                heap.len() + 1 // +1 for the cluster we just popped
            );

            // Determine k' for this cluster based on its size
            let remaining_k = target_k - heap.len(); // Spaces left to fill
            let cluster_k = if cluster_size <= params.hierarchical_k {
                2.min(remaining_k).min(cluster_size)
            } else {
                // For larger clusters, split more aggressively
                let suggested_k = cluster_size / params.hierarchical_k;
                suggested_k
                    .min(remaining_k)
                    .min(params.hierarchical_k)
                    .max(2)
            };

            // Create sub-dataset for this cluster using indices
            let sub_data = Self::create_array_from_indices::<T>(
                &largest_cluster.indices,
                data_values,
                dimension,
            )?;

            // Run kmeans on this cluster
            let sub_kmeans = Self::train_kmeans::<T, Algo>(&sub_data, cluster_k, params)?;

            // Get membership for points in the sub-cluster
            let sub_data = sub_data.values().as_primitive::<T>().values();
            let (sub_membership, _, _) = Algo::compute_membership_and_loss(
                sub_kmeans.centroids.as_primitive::<T>().values(),
                sub_data,
                dimension,
                params.distance_type,
                0.0,
                None,
                None,
            );

            // Build per-cluster membership while checking whether the split is effective
            let approx_cluster_capacity = if cluster_k > 0 {
                largest_cluster.indices.len().div_ceil(cluster_k)
            } else {
                0
            };
            let mut cluster_assignments: Vec<Vec<usize>> = (0..cluster_k)
                .map(|_| Vec::with_capacity(approx_cluster_capacity))
                .collect();

            let mut first_sid: Option<u32> = None;
            let mut all_same = true;
            for (local_idx, &membership) in sub_membership.iter().enumerate() {
                let Some(sub_cluster_id) = membership else {
                    continue;
                };

                if let Some(first) = first_sid {
                    if sub_cluster_id != first {
                        all_same = false;
                    }
                } else {
                    first_sid = Some(sub_cluster_id);
                }

                let sub_cluster_id = sub_cluster_id as usize;
                if let Some(indices) = cluster_assignments.get_mut(sub_cluster_id) {
                    indices.push(largest_cluster.indices[local_idx]);
                } else {
                    // Unexpected assignment outside [0, cluster_k); treat as ineffective split.
                    all_same = false;
                }
            }

            // If all memberships are identical, the split is ineffective; finalize the original cluster
            if all_same {
                largest_cluster.finalized = true;
                heap.push(largest_cluster);
                continue;
            }

            // Create new sub-clusters and add to heap
            let sub_centroids = sub_kmeans.centroids.as_primitive::<T>().values();
            for (i, new_cluster_indices) in cluster_assignments.into_iter().enumerate() {
                if new_cluster_indices.is_empty() {
                    continue;
                }

                let centroid_start = i * dimension;
                let centroid_end = centroid_start + dimension;
                let centroid = sub_centroids[centroid_start..centroid_end].to_vec();

                heap.push(Cluster {
                    id: next_cluster_id,
                    indices: new_cluster_indices,
                    centroid,
                    finalized: false,
                });
                next_cluster_id += 1;
            }

            log::debug!(
                "Split complete: now have {} clusters (target: {})",
                heap.len(),
                target_k
            );
        }
        if heap.len() < target_k {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Cannot create {target_k} IVF partitions: k-means could only form {} non-empty \
                 clusters from {n} training vectors. The dataset is likely too small or has too \
                 many (near-)duplicate vectors for this many partitions. Reduce num_partitions to \
                 <= {} or provide more diverse data.",
                heap.len(),
                heap.len()
            )));
        }

        // Construct final KMeans model with all centroids
        let mut all_clusters: Vec<Cluster<T::Native>> = heap.into_vec();
        // Sort by ID to ensure consistent ordering
        all_clusters.sort_by_key(|c| c.id);

        let flat_centroids: Vec<T::Native> =
            all_clusters.into_iter().flat_map(|c| c.centroid).collect();
        let centroids_array = PrimitiveArray::<T>::from(flat_centroids);

        Ok(Self {
            centroids: Arc::new(centroids_array),
            dimension,
            distance_type: params.distance_type,
            loss: 0.0, // Loss is not meaningful for hierarchical clustering
        })
    }

    /// Train a [`KMeans`] model with full parameters.
    ///
    /// If the DistanceType is `Cosine`, the input vectors will be normalized with each iteration.
    pub fn new_with_params(
        data: &FixedSizeListArray,
        k: usize,
        params: &KMeansParams,
    ) -> arrow::error::Result<Self> {
        let n = data.len();
        if n < k {
            return Err(ArrowError::InvalidArgumentError(format!(
                "KMeans: training does not have sufficient data points: n({}) is smaller than k({})",
                n, k
            )));
        }

        // use hierarchical clustering if k > 256 and hierarchical_k > 1
        // we set 256 as the threshold because:
        // 1. PQ would run kmeans with k=256, in that case we don't want to use hierarchical clustering for accuracy
        // 2. kmeans with k=256 is small enough that we don't need to use hierarchical clustering for efficiency
        if k > 256 && params.hierarchical_k > 1 {
            log::debug!("Using hierarchical clustering for k={}", k);
            return match (data.value_type(), params.distance_type) {
                (DataType::Float16, _) => Self::train_hierarchical_kmeans::<
                    Float16Type,
                    KMeansAlgoFloat<Float16Type>,
                >(data, k, params),
                (DataType::Float32, _) => Self::train_hierarchical_kmeans::<
                    Float32Type,
                    KMeansAlgoFloat<Float32Type>,
                >(data, k, params),
                (DataType::Float64, _) => Self::train_hierarchical_kmeans::<
                    Float64Type,
                    KMeansAlgoFloat<Float64Type>,
                >(data, k, params),
                (DataType::UInt8, DistanceType::Hamming) => {
                    Self::train_hierarchical_kmeans::<UInt8Type, KModeAlgo>(data, k, params)
                }
                _ => Err(ArrowError::InvalidArgumentError(format!(
                    "KMeans: can not train data type {} with distance type: {}",
                    data.value_type(),
                    params.distance_type
                ))),
            };
        }

        match (data.value_type(), params.distance_type) {
            (DataType::Float16, _) => {
                Self::train_kmeans::<Float16Type, KMeansAlgoFloat<Float16Type>>(data, k, params)
            }

            (DataType::Float32, _) => {
                Self::train_kmeans::<Float32Type, KMeansAlgoFloat<Float32Type>>(data, k, params)
            }
            (DataType::Float64, _) => {
                Self::train_kmeans::<Float64Type, KMeansAlgoFloat<Float64Type>>(data, k, params)
            }
            (DataType::UInt8, DistanceType::Hamming) => {
                Self::train_kmeans::<UInt8Type, KModeAlgo>(data, k, params)
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "KMeans: can not train data type {} with distance type: {}",
                data.value_type(),
                params.distance_type
            ))),
        }
    }
}

pub fn kmeans_find_partitions_arrow_array(
    centroids: &FixedSizeListArray,
    query: &dyn Array,
    nprobes: usize,
    distance_type: DistanceType,
) -> arrow::error::Result<(UInt32Array, Float32Array)> {
    if centroids.value_length() as usize != query.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Centroids and vectors have different dimensions: {} != {}",
            centroids.value_length(),
            query.len()
        )));
    }

    match (centroids.value_type(), query.data_type()) {
        (DataType::Float16, DataType::Float16) => Ok(kmeans_find_partitions(
            centroids.values().as_primitive::<Float16Type>().values(),
            query.as_primitive::<Float16Type>().values(),
            nprobes,
            distance_type,
        )?),
        (DataType::Float32, DataType::Float32) => Ok(kmeans_find_partitions(
            centroids.values().as_primitive::<Float32Type>().values(),
            query.as_primitive::<Float32Type>().values(),
            nprobes,
            distance_type,
        )?),
        (DataType::Float64, DataType::Float64) => Ok(kmeans_find_partitions(
            centroids.values().as_primitive::<Float64Type>().values(),
            query.as_primitive::<Float64Type>().values(),
            nprobes,
            distance_type,
        )?),
        (DataType::UInt8, DataType::UInt8) => Ok(kmeans_find_partitions_binary(
            centroids.values().as_primitive::<UInt8Type>().values(),
            query.as_primitive::<UInt8Type>().values(),
            nprobes,
            distance_type,
        )?),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Centroids and vectors have different types: {} != {}",
            centroids.value_type(),
            query.data_type()
        ))),
    }
}

/// KMeans finds N nearest partitions.
///
/// Parameters:
/// - *centroids*: a `k * dimension` floating array.
/// - *query*: a `dimension` floating array.
/// - *nprobes*: the number of partitions to find.
/// - *distance_type*: the distance type to calculate distance.
///
/// This function allows to conduct kmeans search without constructing
/// `Arrow Array` or `Vec<Float>` types.
///
pub fn kmeans_find_partitions<T: Float + L2 + Dot>(
    centroids: &[T],
    query: &[T],
    nprobes: usize,
    distance_type: DistanceType,
) -> arrow::error::Result<(UInt32Array, Float32Array)> {
    let dists: Vec<f32> = match distance_type {
        DistanceType::L2 => l2_distance_batch(query, centroids, query.len()).collect(),
        DistanceType::Dot => dot_distance_batch(query, centroids, query.len()).collect(),
        _ => {
            panic!(
                "KMeans::find_partitions: {} is not supported",
                distance_type
            );
        }
    };

    // TODO: use heap to just keep nprobes smallest values.
    let dists_arr = Float32Array::from(dists);
    let indices = sort_to_indices(&dists_arr, None, Some(nprobes))?;
    let dists = arrow::compute::take(&dists_arr, &indices, None)?
        .as_primitive::<Float32Type>()
        .clone();
    Ok((indices, dists))
}

pub fn kmeans_find_partitions_binary(
    centroids: &[u8],
    query: &[u8],
    nprobes: usize,
    distance_type: DistanceType,
) -> arrow::error::Result<(UInt32Array, Float32Array)> {
    let dists: Vec<f32> = match distance_type {
        DistanceType::Hamming => hamming_distance_batch(query, centroids, query.len()).collect(),
        _ => {
            panic!(
                "KMeans::find_partitions: {} is not supported",
                distance_type
            );
        }
    };

    // TODO: use heap to just keep nprobes smallest values.
    let dists_arr = Float32Array::from(dists);
    let indices = sort_to_indices(&dists_arr, None, Some(nprobes))?;
    let dists = arrow::compute::take(&dists_arr, &indices, None)?
        .as_primitive::<Float32Type>()
        .clone();
    Ok((indices, dists))
}

/// Compute partitions from Arrow FixedSizeListArray.
#[allow(clippy::type_complexity)]
pub fn compute_partitions_arrow_array(
    centroids: &FixedSizeListArray,
    vectors: &FixedSizeListArray,
    distance_type: DistanceType,
) -> arrow::error::Result<(Vec<Option<u32>>, Vec<Option<f32>>)> {
    if centroids.value_length() != vectors.value_length() {
        return Err(ArrowError::InvalidArgumentError(
            "Centroids and vectors have different dimensions".to_string(),
        ));
    }
    match (centroids.value_type(), vectors.value_type()) {
        (DataType::Float16, DataType::Float16) => Ok(compute_partitions_with_dists::<
            Float16Type,
            KMeansAlgoFloat<Float16Type>,
        >(
            centroids.values().as_primitive(),
            vectors.values().as_primitive(),
            centroids.value_length(),
            distance_type,
        )),
        (DataType::Float32, DataType::Float32) => Ok(compute_partitions_with_dists::<
            Float32Type,
            KMeansAlgoFloat<Float32Type>,
        >(
            centroids.values().as_primitive(),
            vectors.values().as_primitive(),
            centroids.value_length(),
            distance_type,
        )),
        (DataType::Float32, DataType::Int8) => Ok(compute_partitions_with_dists::<
            Float32Type,
            KMeansAlgoFloat<Float32Type>,
        >(
            centroids.values().as_primitive(),
            vectors.convert_to_floating_point()?.values().as_primitive(),
            centroids.value_length(),
            distance_type,
        )),
        (DataType::Float64, DataType::Float64) => Ok(compute_partitions_with_dists::<
            Float64Type,
            KMeansAlgoFloat<Float64Type>,
        >(
            centroids.values().as_primitive(),
            vectors.values().as_primitive(),
            centroids.value_length(),
            distance_type,
        )),
        (DataType::UInt8, DataType::UInt8) => {
            Ok(compute_partitions_with_dists::<UInt8Type, KModeAlgo>(
                centroids.values().as_primitive(),
                vectors.values().as_primitive(),
                centroids.value_length(),
                distance_type,
            ))
        }
        _ => Err(ArrowError::InvalidArgumentError(
            "Centroids and vectors have incompatible types".to_string(),
        )),
    }
}

/// Compute partition ID of each vector in the KMeans.
///
/// If returns `None`, means the vector is not valid, i.e., all `NaN`.
pub fn compute_partitions<T: ArrowNumericType, K: KMeansAlgo<T::Native>>(
    centroids: &PrimitiveArray<T>,
    vectors: &PrimitiveArray<T>,
    dimension: impl AsPrimitive<usize>,
    distance_type: DistanceType,
) -> (Vec<Option<u32>>, f64)
where
    T::Native: Num,
{
    let dimension = dimension.as_();
    let (membership, _, losses) = K::compute_membership_and_loss(
        centroids.values(),
        vectors.values(),
        dimension,
        distance_type,
        0.0,
        None,
        None,
    );
    (membership, losses.iter().sum::<f64>())
}

/// compute the partition id and the distance to the centroid for each vector,
/// NOTE the distance is squared distance for L2
pub fn compute_partitions_with_dists<T: ArrowNumericType, K: KMeansAlgo<T::Native>>(
    centroids: &PrimitiveArray<T>,
    vectors: &PrimitiveArray<T>,
    dimension: impl AsPrimitive<usize>,
    distance_type: DistanceType,
) -> (Vec<Option<u32>>, Vec<Option<f32>>)
where
    T::Native: Num,
{
    let dimension = dimension.as_();
    K::compute_membership_and_dist(
        centroids.values(),
        vectors.values(),
        dimension,
        distance_type,
        0.0,
        None,
        None,
    )
}

/// Train KMeans model and returns the centroids of each cluster.
///
/// Parameters
/// ----------
/// - *centroids*: initial centroids, use the random initialization if None
/// - *array*: a flatten floating number array of vectors
/// - *dimension*: dimension of the vector
/// - *k*: number of clusters
/// - *max_iterations*: maximum number of iterations
/// - *redos*: number of times to redo the k-means clustering
/// - *distance_type*: distance type to compute pair-wise vector distance
/// - *sample_rate*: sample rate to select the data for training
#[allow(clippy::too_many_arguments)]
pub fn train_kmeans<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    mut params: KMeansParams,
    dimension: usize,
    k: usize,
    sample_rate: usize,
) -> Result<KMeans>
where
    T::Native: Dot + L2 + Normalize,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let num_rows = array.len() / dimension;
    if num_rows < k {
        return Err(Error::unprocessable(format!(
            "KMeans cannot train {k} centroids with {num_rows} vectors; choose a smaller K (< {num_rows})"
        )));
    }

    // Only sample sample_rate * num_clusters. See Faiss
    let data = if num_rows > sample_rate * k {
        log::info!(
            "Sample {} out of {} to train kmeans of {} dim, {} clusters",
            sample_rate * k,
            array.len() / dimension,
            dimension,
            k,
        );
        let sample_size = sample_rate * k;
        array.slice(0, sample_size * dimension)
    } else {
        array.clone()
    };

    let data = FixedSizeListArray::try_new_from_values(data, dimension as i32)?;

    params.balance_factor /= data.len() as f32;
    let model = KMeans::new_with_params(&data, k, &params)?;
    Ok(model)
}

#[inline]
pub fn compute_partition<T: Float + L2 + Dot>(
    centroids: &[T],
    vector: &[T],
    distance_type: DistanceType,
) -> Option<u32> {
    match distance_type {
        DistanceType::L2 => {
            argmin_value_float(l2_distance_batch(vector, centroids, vector.len())).map(|(c, _)| c)
        }
        DistanceType::Dot => {
            argmin_value_float(dot_distance_batch(vector, centroids, vector.len())).map(|(c, _)| c)
        }
        _ => {
            panic!(
                "KMeans::compute_partition: distance type {} is not supported",
                distance_type
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_n;

    use arrow_array::Float16Array;
    use arrow_array::types::Float16Type;
    use half::f16;
    use lance_arrow::*;
    use lance_testing::datagen::generate_random_array;

    use super::*;
    use lance_linalg::distance::l2;
    use lance_linalg::kernels::argmin;

    #[test]
    fn test_train_with_small_dataset() {
        let data = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let data = FixedSizeListArray::try_new_from_values(data, 2).unwrap();
        match KMeans::new(&data, 128, 5) {
            Ok(_) => panic!("Should fail to train KMeans"),
            Err(e) => {
                assert!(e.to_string().contains("smaller than"));
            }
        }
    }

    #[test]
    fn test_compute_partitions() {
        const DIM: usize = 256;
        let centroids = generate_random_array(DIM * 18);
        let data = generate_random_array(DIM * 20);

        let expected = data
            .values()
            .chunks(DIM)
            .map(|row| {
                argmin(
                    centroids
                        .values()
                        .chunks(DIM)
                        .map(|centroid| l2(row, centroid)),
                )
            })
            .collect::<Vec<_>>();
        let (actual, _) = compute_partitions::<Float32Type, KMeansAlgoFloat<Float32Type>>(
            &centroids,
            &data,
            DIM,
            DistanceType::L2,
        );
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_random_init_advances_rng() {
        let values = Float32Array::from_iter_values((0..64).map(|value| value as f32));
        let mut rng = SmallRng::seed_from_u64(42);
        let first =
            KMeans::init_random::<Float32Type>(values.values(), 1, 8, &mut rng, DistanceType::L2);
        let second =
            KMeans::init_random::<Float32Type>(values.values(), 1, 8, &mut rng, DistanceType::L2);

        assert_ne!(
            first.centroids.as_primitive::<Float32Type>().values(),
            second.centroids.as_primitive::<Float32Type>().values(),
        );
    }

    #[tokio::test]
    async fn test_compute_membership_and_loss() {
        const DIM: usize = 256;
        let centroids = generate_random_array(DIM * 18);
        let data = generate_random_array(DIM * 20);

        let (membership, _, losses) = KMeansAlgoFloat::<Float32Type>::compute_membership_and_loss(
            centroids.as_slice(),
            data.values(),
            DIM,
            DistanceType::L2,
            0.0,
            None,
            None,
        );
        let loss = losses.iter().sum::<f64>();
        assert!(loss > 0.0, "loss is not zero: {}", loss);
        membership.iter().for_each(|cd| {
            assert!(cd.is_some());
        });
    }

    #[tokio::test]
    async fn test_l2_with_nans() {
        const DIM: usize = 8;
        const K: usize = 32;
        const NUM_CENTROIDS: usize = 16 * 2048;
        let centroids = generate_random_array(DIM * NUM_CENTROIDS);
        let values = Float32Array::from_iter_values(repeat_n(f32::NAN, DIM * K));

        compute_partitions::<Float32Type, KMeansAlgoFloat<Float32Type>>(
            &centroids,
            &values,
            DIM,
            DistanceType::L2,
        )
        .0
        .iter()
        .for_each(|cd| {
            assert!(cd.is_none());
        });
    }

    #[tokio::test]
    async fn test_train_l2_kmeans_with_nans() {
        const DIM: usize = 8;
        const K: usize = 32;
        const NUM_CENTROIDS: usize = 16 * 2048;
        let centroids = generate_random_array(DIM * NUM_CENTROIDS);
        let values = repeat_n(f32::NAN, DIM * K).collect::<Vec<_>>();

        let (membership, _, _) = KMeansAlgoFloat::<Float32Type>::compute_membership_and_loss(
            centroids.as_slice(),
            &values,
            DIM,
            DistanceType::L2,
            0.0,
            None,
            None,
        );

        membership.iter().for_each(|cd| assert!(cd.is_none()));
    }

    #[tokio::test]
    async fn test_train_kmode() {
        const DIM: usize = 16;
        const K: usize = 32;
        const NUM_VALUES: usize = 256 * K;

        let mut rng = SmallRng::from_os_rng();
        let values =
            UInt8Array::from_iter_values((0..NUM_VALUES * DIM).map(|_| rng.random_range(0..255)));

        let fsl = FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap();

        let params = KMeansParams {
            distance_type: DistanceType::Hamming,
            ..Default::default()
        };
        let kmeans = KMeans::new_with_params(&fsl, K, &params).unwrap();
        assert_eq!(kmeans.centroids.len(), K * DIM);
        assert_eq!(kmeans.dimension, DIM);
        assert_eq!(kmeans.centroids.data_type(), &DataType::UInt8);
    }

    #[tokio::test]
    async fn test_hierarchical_kmeans() {
        const DIM: usize = 64;
        const K: usize = 257; // Greater than 256 to trigger hierarchical clustering
        const NUM_VALUES: usize = 1024 * K;

        let values = generate_random_array(NUM_VALUES * DIM);
        let fsl = FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap();

        let params = KMeansParams {
            max_iters: 10,
            hierarchical_k: 16,
            ..Default::default()
        };

        let kmeans = KMeans::new_with_params(&fsl, K, &params).unwrap();

        // Verify that we have the correct number of clusters
        assert_eq!(kmeans.centroids.len(), K * DIM);
        assert_eq!(kmeans.dimension, DIM);
        assert_eq!(kmeans.centroids.data_type(), &DataType::Float32);

        // Verify that all centroids are valid (not NaN)
        let centroids = kmeans.centroids.as_primitive::<Float32Type>().values();
        for val in centroids {
            assert!(!val.is_nan(), "Centroid should not contain NaN values");
        }
    }

    #[tokio::test]
    async fn test_hierarchical_kmeans_too_few_distinct_vectors_errors() {
        // Regression test for https://github.com/lance-format/lance/issues/7867
        //
        // With a small number of distinct vectors repeated many times (heavy
        // near-duplication) and dot distance, hierarchical k-means cannot form
        // `target_k` non-empty clusters no matter how it splits: every split of a
        // cluster of identical vectors is either ineffective (`all_same`) or
        // immediately hits the "<= 1 point" floor. This used to trip
        // `debug_assert_eq!(heap.len(), target_k)` (panic in debug builds) or
        // silently return a half-empty centroid set (release builds). It should
        // now return a clear error instead.
        const DIM: usize = 8;
        const NUM_DISTINCT: usize = 5;
        const REPEATS: usize = 200;
        const TARGET_K: usize = 300; // > 256 to trigger hierarchical clustering

        let base_vectors = generate_random_array(NUM_DISTINCT * DIM);
        let mut values = Vec::with_capacity(NUM_DISTINCT * REPEATS * DIM);
        for _ in 0..REPEATS {
            values.extend_from_slice(base_vectors.values());
        }
        let values = Float32Array::from(values);
        let fsl = FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap();

        let params = KMeansParams {
            max_iters: 10,
            hierarchical_k: 16,
            distance_type: DistanceType::Dot,
            ..Default::default()
        };

        let err = KMeans::new_with_params(&fsl, TARGET_K, &params)
            .expect_err("training should fail rather than panic or silently under-produce");
        let msg = err.to_string();
        assert!(
            msg.contains("Cannot create") && msg.contains(&TARGET_K.to_string()),
            "unexpected error message: {msg}"
        );
    }

    #[tokio::test]
    async fn test_float16_underflow_fix() {
        // This test verifies the fix for float16 division underflow
        // When training k-means on many float16 vectors with small k,
        // without limiting the data size, dividing centroids by count
        // can underflow to 0,
        // The fix limits data to k * 512 to prevent this
        const DIM: usize = 2;
        const K: usize = 2;
        const NUM_VALUES: usize = K * 65536; // Many vectors to trigger the issue

        let f32_values = generate_random_array(NUM_VALUES * DIM);
        let f16_values = Float16Array::from_iter_values(
            f32_values.values().iter().map(|&v| half::f16::from_f32(v)),
        );
        let fsl = FixedSizeListArray::try_new_from_values(f16_values, DIM as i32).unwrap();

        let params = KMeansParams {
            max_iters: 10,
            ..Default::default()
        };

        let kmeans = KMeans::new_with_params(&fsl, K, &params).unwrap();

        // Verify that we have the correct number of clusters
        assert_eq!(kmeans.centroids.len(), K * DIM);
        assert_eq!(kmeans.dimension, DIM);
        assert_eq!(kmeans.centroids.data_type(), &DataType::Float16);

        // Verify that all centroids are valid (not zero or NaN)
        // Without the fix, they would all be zero due to underflow
        let centroids = kmeans.centroids.as_primitive::<Float16Type>().values();
        for &val in centroids {
            assert!(!val.is_nan(), "Centroid should not contain NaN values");
            assert!(val != f16::ZERO);
        }
    }
}
