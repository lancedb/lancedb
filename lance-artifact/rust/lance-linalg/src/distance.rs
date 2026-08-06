// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Distance metrics
//!
//! This module provides distance metrics for vectors.
//!
//! - `bf16, f16, f32, f64` types are supported.
//! - SIMD is used when available, on `x86_64`, `aarch64` and `loongarch64`
//!   architectures.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type, UInt8Type};
use arrow_array::{Array, ArrowPrimitiveType, FixedSizeListArray, Float32Array, ListArray};
use arrow_schema::{ArrowError, DataType};

pub mod cosine;
pub mod cosine_u8;
pub mod dot;
pub mod dot_u8;
pub mod hamming;
pub mod l2;
pub mod l2_u8;
pub mod norm_l2;

/// What a per-batch distance kernel yields.
///
/// The two batch paths produce different shapes: the build-baseline path maps
/// lazily over the batch and allocates nothing, while a `#[target_feature]`
/// kernel must collect eagerly because it cannot be inlined into a lazy
/// closure. A concrete enum keeps both statically dispatched.
///
/// Only sub-AVX2 builds need this. On an AVX2-baseline build the batch methods
/// return the bare `Map` instead, because any wrapper — trait object or enum —
/// loses `TrustedLen` (so `.collect()` stops preallocating) and loses
/// `Map::fold`'s inlined loop. Benchmarks showed that costing 2.5x on the dim-8
/// batch, far more than the per-vector dispatch it was meant to remove.
#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
pub(crate) enum BatchIter<L> {
    /// Lazy per-vector map. No allocation.
    Lazy(L),
    /// Eagerly collected by a `#[target_feature]` kernel.
    Eager(std::vec::IntoIter<f32>),
}

#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
impl<L: Iterator<Item = f32>> Iterator for BatchIter<L> {
    type Item = f32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Lazy(iter) => iter.next(),
            Self::Eager(iter) => iter.next(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Lazy(iter) => iter.size_hint(),
            Self::Eager(iter) => iter.size_hint(),
        }
    }

    /// Delegated, not defaulted. `Map` overrides `fold` to drive the underlying
    /// `ChunksExact` in one inlined, auto-vectorized loop; the default `fold`
    /// would instead call `next()` per element, paying an enum branch and
    /// losing that loop. On the dim-8 batch that costs ~2.5x.
    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        match self {
            Self::Lazy(iter) => iter.fold(init, f),
            Self::Eager(iter) => iter.fold(init, f),
        }
    }

    /// `for_each`, `sum` and `collect` all route through `fold`, so delegating
    /// it covers them too. (`try_fold` cannot be overridden on stable: its
    /// `Try` bound is unstable.)
    #[inline]
    fn for_each<F>(self, f: F)
    where
        F: FnMut(Self::Item),
    {
        match self {
            Self::Lazy(iter) => iter.for_each(f),
            Self::Eager(iter) => iter.for_each(f),
        }
    }
}

#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
impl<L: ExactSizeIterator<Item = f32>> ExactSizeIterator for BatchIter<L> {
    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Lazy(iter) => iter.len(),
            Self::Eager(iter) => iter.len(),
        }
    }
}

pub use cosine::*;
pub use dot::*;
pub use hamming::{
    BinaryHashValues, Cluster, ClusteringResult, PairwiseResult, UnionFind, cluster_edges,
    cluster_pairwise_result, extract_binary_hashes_from_fixed_list, extract_hashes_from_fixed_list,
    hamming_distance_arrow_batch, hamming_u64, pairwise_hamming_distance,
    pairwise_hamming_distance_binary, pairwise_hamming_distance_binary_parallel,
    pairwise_hamming_distance_parallel,
};
pub use l2::*;
use lance_core::deepsize::DeepSizeOf;
pub use norm_l2::*;

use crate::Result;

/// Distance metrics type.
#[derive(Debug, Copy, Clone, PartialEq, DeepSizeOf)]
pub enum DistanceType {
    L2,
    Cosine,
    /// Dot Product
    Dot,
    /// Hamming Distance
    Hamming,
}

/// For backwards compatibility.
pub type MetricType = DistanceType;

pub type DistanceFunc<T> = fn(&[T], &[T]) -> f32;
pub type BatchDistanceFunc = fn(&[f32], &[f32], usize) -> Arc<Float32Array>;
pub type ArrowBatchDistanceFunc = fn(&dyn Array, &FixedSizeListArray) -> Result<Arc<Float32Array>>;

impl DistanceType {
    /// Compute the distance from one vector to a batch of vectors.
    ///
    /// This propagates nulls to the output.
    pub fn arrow_batch_func(&self) -> ArrowBatchDistanceFunc {
        match self {
            Self::L2 => l2_distance_arrow_batch,
            Self::Cosine => cosine_distance_arrow_batch,
            Self::Dot => dot_distance_arrow_batch,
            Self::Hamming => hamming_distance_arrow_batch,
        }
    }

    /// Returns the distance function between two vectors.
    pub fn func<T: L2 + Cosine + Dot>(&self) -> DistanceFunc<T> {
        match self {
            Self::L2 => l2,
            Self::Cosine => cosine_distance,
            Self::Dot => dot_distance,
            Self::Hamming => todo!(),
        }
    }
}

impl std::fmt::Display for DistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::L2 => "l2",
                Self::Cosine => "cosine",
                Self::Dot => "dot",
                Self::Hamming => "hamming",
            }
        )
    }
}

impl TryFrom<&str> for DistanceType {
    type Error = ArrowError;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "l2" | "euclidean" => Ok(Self::L2),
            "cosine" => Ok(Self::Cosine),
            "dot" => Ok(Self::Dot),
            "hamming" => Ok(Self::Hamming),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Metric type '{s}' is not supported"
            ))),
        }
    }
}

pub fn multivec_distance(
    query: &dyn Array,
    vectors: &ListArray,
    distance_type: DistanceType,
) -> Result<Vec<f32>> {
    let dim = if let DataType::FixedSizeList(_, dim) = vectors.value_type() {
        dim as usize
    } else {
        return Err(ArrowError::InvalidArgumentError(
            "vectors must be a list of fixed size list".to_string(),
        ));
    };

    // check the query vectors type first
    // because we don't want to check the vectors type for each vector
    match query.data_type() {
        DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::UInt8 => {}
        _ => {
            return Err(ArrowError::InvalidArgumentError(
                "query must be a float array or binary array".to_string(),
            ));
        }
    }

    let mut dists = Vec::with_capacity(vectors.len());
    for v in vectors.iter() {
        match v {
            None => dists.push(f32::NAN),
            Some(v) => {
                let multivector = v.as_fixed_size_list();
                if multivector.len() == 0 {
                    dists.push(f32::NAN);
                    continue;
                }

                let sim = match distance_type {
                    DistanceType::Hamming => {
                        let query = query.as_primitive::<UInt8Type>().values();
                        query
                            .chunks_exact(dim)
                            .map(|q| {
                                multivector
                                    .values()
                                    .as_primitive::<UInt8Type>()
                                    .values()
                                    .chunks_exact(dim)
                                    .map(|v| hamming::hamming(q, v))
                                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                                    .unwrap()
                            })
                            .sum()
                    }
                    _ => match query.data_type() {
                        DataType::Float16 => multivec_distance_impl::<Float16Type>(
                            query,
                            multivector,
                            dim,
                            distance_type,
                        ),
                        DataType::Float32 => multivec_distance_impl::<Float32Type>(
                            query,
                            multivector,
                            dim,
                            distance_type,
                        ),
                        DataType::Float64 => multivec_distance_impl::<Float64Type>(
                            query,
                            multivector,
                            dim,
                            distance_type,
                        ),
                        _ => unreachable!("missed to check query type"),
                    },
                };

                dists.push(1.0 - sim);
            }
        }
    }
    Ok(dists)
}

fn multivec_distance_impl<T: ArrowPrimitiveType>(
    query: &dyn Array,
    multivector: &FixedSizeListArray,
    dim: usize,
    distance_type: DistanceType,
) -> f32
where
    T::Native: L2 + Cosine + Dot,
{
    let query = query.as_primitive::<T>().values();
    query
        .chunks_exact(dim)
        .map(|q| {
            multivector
                .values()
                .as_primitive::<T>()
                .values()
                .chunks_exact(dim)
                .map(|v| 1.0 - distance_type.func()(q, v))
                .max_by(|a, b| a.total_cmp(b))
                .unwrap()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::types::Float32Type;
    use arrow_array::{Float32Array, ListArray};
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;

    #[test]
    fn test_multivec_distance_empty_row_is_nan() {
        let query: Arc<dyn Array> = Arc::new(Float32Array::from_iter_values([1.0_f32, 2.0]));

        let dim = 2;
        let values = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vec![Some(vec![Some(1.0_f32), Some(2.0)])],
            dim,
        );

        // Two rows: first is empty list, second has one sub-vector.
        let offsets = OffsetBuffer::from_lengths([0_usize, 1]);
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let vectors = ListArray::try_new(field, offsets, Arc::new(values), None).unwrap();

        let dists = multivec_distance(query.as_ref(), &vectors, DistanceType::Dot).unwrap();
        assert_eq!(dists.len(), 2);
        assert!(dists[0].is_nan());
        assert_eq!(dists[1], -4.0);
    }
}
