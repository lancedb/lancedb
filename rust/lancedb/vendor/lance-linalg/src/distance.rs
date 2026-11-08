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
use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray};
use arrow_schema::ArrowError;
use lance_core::utils::cpu::SimdSupport;

mod arrow;

pub mod cosine;
pub mod cosine_u8;
pub mod dot;
pub mod dot_f16;
pub mod dot_u8;
pub mod hamming;
pub mod l2;
pub mod l2_u8;
pub mod norm_l2;

// `#[track_caller]` on these helpers is load-bearing: without it a length-contract
// panic reports this file rather than the distance function the caller reached.
// See #8863.
#[inline]
#[track_caller]
fn assert_equal_lengths(left_len: usize, right_len: usize) {
    assert_eq!(
        left_len, right_len,
        "distance inputs must have equal lengths: left={left_len}, right={right_len}"
    );
}

#[inline]
#[track_caller]
fn assert_batch_layout(vector_len: usize, batch_len: usize, dimension: usize) {
    assert!(
        dimension > 0,
        "distance dimension must be greater than zero"
    );
    assert_eq!(
        vector_len, dimension,
        "distance vector length must match dimension: vector={vector_len}, dimension={dimension}"
    );
    assert_eq!(
        batch_len % dimension,
        0,
        "distance batch length must be divisible by dimension: batch={batch_len}, dimension={dimension}"
    );
}

/// [`assert_batch_layout`], plus one output slot per vector in the batch.
#[inline]
#[track_caller]
fn assert_batch_output_layout(
    vector_len: usize,
    batch_len: usize,
    dimension: usize,
    output_len: usize,
) {
    assert_batch_layout(vector_len, batch_len, dimension);
    assert_eq!(
        output_len,
        batch_len / dimension,
        "distance output length must match the number of vectors in the batch: output={output_len}, batch={batch_len}, dimension={dimension}"
    );
}

/// Runtime backend shared by the f16 and bf16 C kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HalfBackend {
    Avx512,
    Avx2,
    Neon,
    Lsx,
    Lasx,
    Scalar,
}

/// Half-precision element type whose fallback kernel is being selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HalfType {
    F16,
    Bf16,
}

/// CPU features required by the x86 fallback objects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct X86HalfFeatures {
    has_avx2: bool,
    has_f16c: bool,
    has_fma: bool,
}

/// Whether this target links the optional half-precision C objects.
const HALF_KERNELS_COMPILED: bool = cfg!(all(
    feature = "fp16kernels",
    not(target_os = "windows"),
    any(
        target_arch = "aarch64",
        target_arch = "x86_64",
        target_arch = "loongarch64"
    )
));

/// Selects a half-precision backend without reading host or build state so the
/// exclusive SIMD tier ladder can be tested on any machine.
fn half_backend(
    support: SimdSupport,
    half_type: HalfType,
    has_kernels: bool,
    has_avx512_kernel: bool,
    x86_features: X86HalfFeatures,
) -> HalfBackend {
    if !has_kernels {
        return HalfBackend::Scalar;
    }

    match support {
        SimdSupport::Avx512FP16 if has_avx512_kernel => HalfBackend::Avx512,
        // SIMD_SUPPORT reports one exclusive tier. An AVX-512 host that cannot
        // use the optional AVX-512 C object must therefore be named here to
        // reach the always-built x86 fallback object.
        SimdSupport::Avx512 | SimdSupport::Avx512FP16 | SimdSupport::Avx2
            if x86_features.has_fma
                && match half_type {
                    HalfType::F16 => x86_features.has_f16c,
                    HalfType::Bf16 => x86_features.has_avx2,
                } =>
        {
            HalfBackend::Avx2
        }
        SimdSupport::Neon => HalfBackend::Neon,
        SimdSupport::Lsx => HalfBackend::Lsx,
        SimdSupport::Lasx => HalfBackend::Lasx,
        _ => HalfBackend::Scalar,
    }
}

/// Detects every feature emitted by the x86 fallback objects.
#[inline]
fn x86_half_features() -> X86HalfFeatures {
    #[cfg(target_arch = "x86_64")]
    {
        X86HalfFeatures {
            has_avx2: std::is_x86_feature_detected!("avx2"),
            has_f16c: std::is_x86_feature_detected!("f16c"),
            has_fma: std::is_x86_feature_detected!("fma"),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        X86HalfFeatures::default()
    }
}

/// Largest number of maximal u8 product terms whose sum fits in a u32.
const U8_U32_ACCUMULATOR_MAX_LEN: usize = u32::MAX as usize / (u8::MAX as usize * u8::MAX as usize);

/// Distance between two vectors of equal length.
pub(crate) type PairKernel = unsafe fn(&[f32], &[f32]) -> f32;

/// Writes the distance from `key` to each `dimension`-sized vector of `batch`
/// into the matching element of `output`.
pub(crate) type BatchKernel = unsafe fn(&[f32], &[f32], usize, &mut [f32]);

pub use cosine::*;
pub use dot::*;
#[allow(deprecated)]
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
    /// Compute distances from `query` to each row of `vectors`, propagating row nulls.
    ///
    /// L2, cosine and dot accept Float16/Float32/Float64 queries and Int8 queries.
    /// An Int8 query also accepts Float32, Int16, or Int32 stored vectors; both
    /// sides are widened to Float32, preserving the legacy mixed-type behavior.
    /// Other query and stored element types must match. Hamming accepts UInt8.
    /// The query must be non-null and have the same positive dimension as the rows.
    /// Unsupported types and invalid queries return an [`ArrowError::InvalidArgumentError`].
    ///
    /// ```
    /// use arrow_array::{FixedSizeListArray, Float32Array, types::Float32Type};
    /// use lance_linalg::distance::DistanceType;
    /// let query = Float32Array::from(vec![1.0, 2.0]);
    /// let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
    ///     [Some(vec![Some(3.0), Some(4.0)])], 2);
    /// assert_eq!(DistanceType::L2.batch(&query, &vectors)?.value(0), 8.0);
    /// # Ok::<(), arrow_schema::ArrowError>(())
    /// ```
    pub fn batch(&self, query: &dyn Array, vectors: &FixedSizeListArray) -> Result<Float32Array> {
        let values = arrow::dispatch(arrow::Operation::Batch(*self), Some(query), vectors)?;
        Ok(Float32Array::new(values.into(), vectors.nulls().cloned()))
    }

    /// Compute additive late-interaction distances for a flattened multivector query.
    ///
    /// Each query sub-vector contributes its minimum distance to any stored
    /// sub-vector in the row. Null or empty stored rows produce `NaN`.
    /// Float16/Float32/Float64 support L2, cosine and dot; UInt8 supports Hamming.
    /// Int8 multivectors remain unsupported. Query and stored element types must
    /// match, and the non-null query length must be a positive multiple of the
    /// stored vector dimension. Invalid inputs return [`ArrowError::InvalidArgumentError`].
    ///
    /// ```
    /// use arrow_array::ListArray;
    /// use lance_linalg::distance::DistanceType;
    /// # fn example(query: &dyn arrow_array::Array, vectors: &ListArray)
    /// # -> Result<Vec<f32>, arrow_schema::ArrowError> {
    /// DistanceType::Cosine.multivec(query, vectors)
    /// # }
    /// ```
    pub fn multivec(&self, query: &dyn Array, vectors: &ListArray) -> Result<Vec<f32>> {
        let values = vectors.values().as_fixed_size_list_opt().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "{self} multivec: vectors must be a list of fixed size list, got {}",
                vectors.data_type()
            ))
        })?;
        arrow::dispatch(
            arrow::Operation::Multivec(*self, vectors),
            Some(query),
            values,
        )
    }

    /// Return a typed numeric kernel, or an error for Hamming.
    ///
    /// Use [`Self::batch`] for UInt8 Hamming distances.
    ///
    /// ```
    /// use lance_linalg::distance::DistanceType;
    /// let distance = DistanceType::L2.try_func::<f32>()?;
    /// assert_eq!(distance(&[1.0], &[3.0]), 4.0);
    /// assert!(DistanceType::Hamming.try_func::<f32>().is_err());
    /// # Ok::<(), arrow_schema::ArrowError>(())
    /// ```
    pub fn try_func<T: L2 + Cosine + Dot>(&self) -> Result<DistanceFunc<T>> {
        match self {
            Self::L2 => Ok(l2),
            Self::Cosine => Ok(cosine_distance),
            Self::Dot => Ok(dot_distance),
            Self::Hamming => Err(ArrowError::InvalidArgumentError(
                "hamming: typed numeric kernels do not support Hamming; use DistanceType::batch with UInt8 vectors".to_string(),
            )),
        }
    }

    /// Compute distances through the legacy function-pointer API.
    #[deprecated(note = "Use DistanceType::batch")]
    pub fn arrow_batch_func(&self) -> ArrowBatchDistanceFunc {
        match self {
            Self::L2 => |query, vectors| Self::L2.batch(query, vectors).map(Arc::new),
            Self::Cosine => |query, vectors| Self::Cosine.batch(query, vectors).map(Arc::new),
            Self::Dot => |query, vectors| Self::Dot.batch(query, vectors).map(Arc::new),
            Self::Hamming => |query, vectors| Self::Hamming.batch(query, vectors).map(Arc::new),
        }
    }

    /// Return a typed numeric kernel through the legacy infallible API.
    ///
    /// # Panics
    /// Panics for Hamming. Use [`Self::try_func`] to receive an error instead.
    #[deprecated(note = "Use DistanceType::try_func or DistanceType::batch")]
    pub fn func<T: L2 + Cosine + Dot>(&self) -> DistanceFunc<T> {
        // This deprecated signature cannot return an error without breaking callers.
        self.try_func()
            .expect("Hamming requires UInt8 Arrow vectors")
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

/// Computes the additive late-interaction distance from a multivector query.
///
/// For each query sub-vector, this finds the minimum distance to any stored
/// sub-vector in the row, then sums those minimum distances. Null or empty
/// stored rows produce `NaN`.
#[deprecated(note = "Use DistanceType::multivec")]
pub fn multivec_distance(
    query: &dyn Array,
    vectors: &ListArray,
    distance_type: DistanceType,
) -> Result<Vec<f32>> {
    distance_type.multivec(query, vectors)
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    #[cfg(target_arch = "x86_64")]
    use std::io::Write;
    use std::sync::Arc;

    use arrow_array::ArrowPrimitiveType;
    use arrow_array::types::{Float16Type, Float32Type, Int8Type, UInt8Type};
    use arrow_array::{
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, ListArray, PrimitiveArray,
        UInt8Array,
    };
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field};
    use half::f16;
    use lance_arrow::FixedSizeListArrayExt;

    const NO_X86_HALF_FEATURES: X86HalfFeatures = X86HalfFeatures {
        has_avx2: false,
        has_f16c: false,
        has_fma: false,
    };
    const F16_X86_HALF_FEATURES: X86HalfFeatures = X86HalfFeatures {
        has_avx2: false,
        has_f16c: true,
        has_fma: true,
    };
    const BF16_X86_HALF_FEATURES: X86HalfFeatures = X86HalfFeatures {
        has_avx2: true,
        has_f16c: false,
        has_fma: true,
    };
    const ALL_X86_HALF_FEATURES: X86HalfFeatures = X86HalfFeatures {
        has_avx2: true,
        has_f16c: true,
        has_fma: true,
    };
    const X86_HALF_FEATURES_WITHOUT_FMA: X86HalfFeatures = X86HalfFeatures {
        has_avx2: true,
        has_f16c: true,
        has_fma: false,
    };

    #[rstest::rstest]
    #[case::kernels_disabled(
        SimdSupport::Avx512FP16,
        HalfType::F16,
        false,
        false,
        ALL_X86_HALF_FEATURES,
        HalfBackend::Scalar
    )]
    #[case::avx512_kernel_ready(
        SimdSupport::Avx512FP16,
        HalfType::F16,
        true,
        true,
        NO_X86_HALF_FEATURES,
        HalfBackend::Avx512
    )]
    #[case::f16_fallback_from_avx512fp16(
        SimdSupport::Avx512FP16,
        HalfType::F16,
        true,
        false,
        F16_X86_HALF_FEATURES,
        HalfBackend::Avx2
    )]
    #[case::bf16_fallback_from_avx512fp16(
        SimdSupport::Avx512FP16,
        HalfType::Bf16,
        true,
        false,
        BF16_X86_HALF_FEATURES,
        HalfBackend::Avx2
    )]
    #[case::fallback_from_avx512(
        SimdSupport::Avx512,
        HalfType::Bf16,
        true,
        false,
        BF16_X86_HALF_FEATURES,
        HalfBackend::Avx2
    )]
    #[case::f16_missing_f16c(
        SimdSupport::Avx512FP16,
        HalfType::F16,
        true,
        false,
        BF16_X86_HALF_FEATURES,
        HalfBackend::Scalar
    )]
    #[case::bf16_missing_avx2(
        SimdSupport::Avx512FP16,
        HalfType::Bf16,
        true,
        false,
        F16_X86_HALF_FEATURES,
        HalfBackend::Scalar
    )]
    #[case::fallback_missing_fma(
        SimdSupport::Avx512FP16,
        HalfType::F16,
        true,
        false,
        X86_HALF_FEATURES_WITHOUT_FMA,
        HalfBackend::Scalar
    )]
    #[case::avx2_tier(
        SimdSupport::Avx2,
        HalfType::Bf16,
        true,
        false,
        BF16_X86_HALF_FEATURES,
        HalfBackend::Avx2
    )]
    #[case::neon(
        SimdSupport::Neon,
        HalfType::F16,
        true,
        false,
        NO_X86_HALF_FEATURES,
        HalfBackend::Neon
    )]
    #[case::lsx(
        SimdSupport::Lsx,
        HalfType::Bf16,
        true,
        false,
        NO_X86_HALF_FEATURES,
        HalfBackend::Lsx
    )]
    #[case::lasx(
        SimdSupport::Lasx,
        HalfType::Bf16,
        true,
        false,
        NO_X86_HALF_FEATURES,
        HalfBackend::Lasx
    )]
    fn half_backend_follows_exclusive_tier_and_feature_requirements(
        #[case] support: SimdSupport,
        #[case] half_type: HalfType,
        #[case] has_kernels: bool,
        #[case] has_avx512_kernel: bool,
        #[case] features: X86HalfFeatures,
        #[case] expected: HalfBackend,
    ) {
        assert_eq!(
            half_backend(support, half_type, has_kernels, has_avx512_kernel, features),
            expected
        );
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_x86_runtime_feature_report() {
        // Write directly to stderr so this remains visible when libtest captures
        // ordinary output from passing tests.
        writeln!(
            std::io::stderr().lock(),
            "lance-linalg x86 runtime features: avx={}, fma={}, avx2={}, avx512f={}, avx512bw={}, avx512vnni={}, avx512vpopcntdq={}",
            std::is_x86_feature_detected!("avx"),
            std::is_x86_feature_detected!("fma"),
            std::is_x86_feature_detected!("avx2"),
            std::is_x86_feature_detected!("avx512f"),
            std::is_x86_feature_detected!("avx512bw"),
            std::is_x86_feature_detected!("avx512vnni"),
            std::is_x86_feature_detected!("avx512vpopcntdq"),
        )
        .expect("write x86 runtime feature report");
    }

    #[test]
    fn test_arrow_batch_type_errors_identify_the_argument() {
        let float32_targets =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![1.0, 2.0]), 2).unwrap();
        let unsupported_query = Int32Array::from(vec![1, 2]);

        for distance_type in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let error = distance_type
                .batch(&unsupported_query, &float32_targets)
                .unwrap_err();
            assert!(
                matches!(error, ArrowError::InvalidArgumentError(_)),
                "{distance_type} returned a different error variant: {error}"
            );
        }

        let unsupported_from_error = DistanceType::Cosine
            .batch(&unsupported_query, &float32_targets)
            .unwrap_err();
        assert!(
            matches!(&unsupported_from_error, ArrowError::InvalidArgumentError(message)
                if message == "cosine batch: query type Int32 does not match the stored vector type Float32"),
            "unexpected unsupported `from` error: {unsupported_from_error}"
        );

        let float32_query = Float32Array::from(vec![1.0, 2.0]);
        let float64_targets =
            FixedSizeListArray::try_new_from_values(Float64Array::from(vec![1.0, 2.0]), 2).unwrap();
        let mismatched_to_error = DistanceType::Cosine
            .batch(&float32_query, &float64_targets)
            .unwrap_err();
        assert!(
            matches!(&mismatched_to_error, ArrowError::InvalidArgumentError(message)
                if message == "cosine batch: query type Float32 does not match the stored vector type Float64"),
            "unexpected mismatched `to` error: {mismatched_to_error}"
        );
    }

    /// Build `List<FixedSizeList<T, dim>>` rows from flattened sub-vector values.
    fn multivecs_of<T: ArrowPrimitiveType>(rows: Vec<Vec<T::Native>>, dim: i32) -> ListArray {
        let lengths = rows
            .iter()
            .map(|row| {
                assert_eq!(row.len() % dim as usize, 0);
                row.len() / dim as usize
            })
            .collect::<Vec<_>>();
        let values = ScalarBuffer::from(rows.into_iter().flatten().collect::<Vec<_>>());
        let inner = PrimitiveArray::<T>::new(values, None);
        let fsl = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", T::DATA_TYPE, true)),
            dim,
            Arc::new(inner),
            None,
        )
        .unwrap();
        let offsets = OffsetBuffer::from_lengths(lengths);
        let field = Arc::new(Field::new("item", fsl.data_type().clone(), true));
        ListArray::try_new(field, offsets, Arc::new(fsl), None).unwrap()
    }

    /// Build one `List<FixedSizeList<T, dim>>` row.
    fn multivec_of<T: ArrowPrimitiveType>(values: Vec<T::Native>, dim: i32) -> ListArray {
        multivecs_of::<T>(vec![values], dim)
    }

    /// The `(query dtype, distance type)` pre-check and the dispatch must agree.
    /// `UInt8` is only valid with Hamming, and the float types only with the
    /// float metrics; a mismatch must be an error rather than a panic in the
    /// dispatch arm or inside an arrow downcast.
    #[test]
    fn test_multivec_distance_rejects_dtype_metric_mismatch() {
        let f32_vectors = multivec_of::<Float32Type>(vec![1.0, 2.0], 2);
        let u8_vectors = multivec_of::<UInt8Type>(vec![1, 2], 2);

        let u8_query: Arc<dyn Array> = Arc::new(UInt8Array::from(vec![1_u8, 2]));
        let f32_query: Arc<dyn Array> = Arc::new(Float32Array::from(vec![1.0_f32, 2.0]));

        // Query and stored types MATCH in each case, so only the metric is wrong
        // — otherwise the element-type check would reject these first and this
        // test would pass with the metric guard deleted.
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let err = multivec_distance(u8_query.as_ref(), &u8_vectors, dt).unwrap_err();
            assert!(
                matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("unsupported vector element type")),
                "UInt8 query with {dt} must be rejected for the metric, got: {err}"
            );
        }

        let err =
            multivec_distance(f32_query.as_ref(), &f32_vectors, DistanceType::Hamming).unwrap_err();
        assert!(
            matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("unsupported vector element type")),
            "Float32 query with hamming must be rejected for the metric, got: {err}"
        );
    }

    /// The `_arrow_batch` entry points that accept `Int8` widen the query element
    /// by element.
    /// A null there used to reach an `unwrap`, so a query column with a null in
    /// its values panicked instead of returning an error, on the metrics that
    /// accept `Int8`.
    #[test]
    fn test_arrow_batch_rejects_null_int8_query() {
        let targets =
            FixedSizeListArray::try_new_from_values(Int8Array::from(vec![1_i8, 2, 3, 4]), 2)
                .unwrap();
        let query: Arc<dyn Array> = Arc::new(Int8Array::from(vec![Some(1_i8), None]));

        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let err = dt.batch(query.as_ref(), &targets).unwrap_err();
            assert!(
                matches!(&err, ArrowError::InvalidArgumentError(m)
                    if m.contains("query must not contain nulls") && m.contains("found 1 in 2 values")),
                "{dt} accepted a null Int8 query element, got: {err}"
            );
        }

        // The same query without nulls goes through, so the guard is not
        // rejecting every `Int8` query.
        let query: Arc<dyn Array> = Arc::new(Int8Array::from(vec![1_i8, 2]));
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            assert_eq!(
                dt.batch(query.as_ref(), &targets).unwrap().len(),
                2,
                "{dt} rejected a well-formed Int8 query"
            );
        }

        // A sliced query reads through `values()`, which has to follow the slice:
        // the window here holds no nulls while the full buffer does. The L2
        // distances are asserted literally rather than against a second call,
        // since computing the expected values by the same route would hide a bug
        // that transformed both alike. Query [3, 4] against [[1, 2], [3, 4]]
        // gives (3-1)^2 + (4-2)^2 = 8 and 0.
        let sliced = Int8Array::from(vec![None, Some(3_i8), Some(4), None]).slice(1, 2);
        let query: Arc<dyn Array> = Arc::new(sliced);
        let got = DistanceType::L2.batch(query.as_ref(), &targets).unwrap();
        assert_eq!(
            got.values(),
            &[8.0_f32, 0.0],
            "L2 did not follow the query slice"
        );
        for dt in [DistanceType::Cosine, DistanceType::Dot] {
            let got = dt.batch(query.as_ref(), &targets).unwrap();
            let want = dt
                .batch(Arc::new(Int8Array::from(vec![3_i8, 4])).as_ref(), &targets)
                .unwrap();
            assert_eq!(got, want, "{dt} did not follow the query slice");
        }
    }

    /// An input that is both a length mismatch and a null query must reach the
    /// null error on all three metrics. `dot` used to carry a second
    /// `debug_assert_eq!` on the dimension in its public entry point, ahead of
    /// the `Int8` arm's null guard.
    #[test]
    fn test_arrow_batch_null_and_length_mismatch_agree() {
        let targets =
            FixedSizeListArray::try_new_from_values(Int8Array::from(vec![1_i8, 2, 3, 4]), 2)
                .unwrap();
        let query: Arc<dyn Array> = Arc::new(Int8Array::from(vec![Some(1_i8), None, Some(2)]));

        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let err = dt.batch(query.as_ref(), &targets).unwrap_err();
            assert!(
                matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("must not contain nulls")),
                "{dt} did not report the null query, got: {err}"
            );
        }
    }

    /// `Int8` is a valid vector element type elsewhere in the crate but has no
    /// multivector kernel, so it must be rejected for the type, not the metric.
    #[test]
    fn test_multivec_distance_rejects_unsupported_element_type() {
        let i8_vectors = multivec_of::<Int8Type>(vec![1, 2], 2);
        let i8_query: Arc<dyn Array> = Arc::new(Int8Array::from(vec![1_i8, 2]));

        let err = multivec_distance(i8_query.as_ref(), &i8_vectors, DistanceType::L2).unwrap_err();
        assert!(
            matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("unsupported vector element type")),
            "Int8 must be rejected for the element type, got: {err}"
        );
    }

    /// Query and stored vector element types must match before dispatch.
    #[test]
    fn test_multivec_distance_rejects_element_type_mismatch() {
        let f16_vectors =
            multivec_of::<Float16Type>(vec![f16::from_f32(1.0), f16::from_f32(2.0)], 2);
        let f32_query: Arc<dyn Array> = Arc::new(Float32Array::from(vec![1.0_f32, 2.0]));

        let err =
            multivec_distance(f32_query.as_ref(), &f16_vectors, DistanceType::L2).unwrap_err();
        assert!(
            matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("does not match the stored vector type")),
            "Float32 query against a Float16 column must be rejected, got: {err}"
        );
    }

    /// A query length that is not a positive multiple of `dim` is structurally
    /// invalid: `chunks_exact` would silently drop the tail, and a query shorter
    /// than `dim` would yield no sub-vectors at all and score every row `0.0`.
    #[test]
    fn test_multivec_distance_rejects_bad_query_length() {
        let vectors = multivec_of::<Float32Type>(vec![1.0, 2.0], 2);

        for bad in [vec![7.0_f32], vec![7.0, 7.0, 999.0], vec![]] {
            let len = bad.len();
            let query: Arc<dyn Array> = Arc::new(Float32Array::from(bad));
            let err = multivec_distance(query.as_ref(), &vectors, DistanceType::L2).unwrap_err();
            assert!(
                matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("must be a positive multiple")),
                "query of length {len} against dim 2 must be rejected, got: {err}"
            );
        }
    }

    /// A zero-dimension column would panic in `chunks_exact(0)`; it gets its own
    /// message rather than blaming the query's length.
    #[test]
    fn test_multivec_distance_rejects_zero_dim() {
        let values = Float32Array::from(Vec::<f32>::new());
        let fsl = FixedSizeListArray::try_new_with_length(
            Arc::new(Field::new("item", DataType::Float32, true)),
            0,
            Arc::new(values),
            None,
            1,
        )
        .unwrap();
        let field = Arc::new(Field::new("item", fsl.data_type().clone(), true));
        let vectors = ListArray::try_new(
            field,
            OffsetBuffer::from_lengths([1_usize]),
            Arc::new(fsl),
            None,
        )
        .unwrap();
        let query: Arc<dyn Array> = Arc::new(Float32Array::from(vec![1.0_f32, 2.0]));

        let err = multivec_distance(query.as_ref(), &vectors, DistanceType::L2).unwrap_err();
        assert!(
            matches!(&err, ArrowError::InvalidArgumentError(m)
                if m.contains("stored vectors have dimension 0")
                    && !m.contains("positive multiple")),
            "a zero-dim column must be rejected on its own terms, got: {err}"
        );
    }

    /// A null query slot is read from the raw values buffer, so it would be
    /// silently scored as whatever the buffer holds.
    #[test]
    fn test_multivec_distance_rejects_null_query() {
        let vectors = multivec_of::<Float32Type>(vec![1.0, 2.0], 2);
        let query: Arc<dyn Array> = Arc::new(Float32Array::from(vec![Some(1.0_f32), None]));

        let err = multivec_distance(query.as_ref(), &vectors, DistanceType::L2).unwrap_err();
        assert!(
            matches!(&err, ArrowError::InvalidArgumentError(m) if m.contains("must not contain nulls")),
            "a query with nulls must be rejected, got: {err}"
        );
    }

    /// Each query sub-vector contributes its minimum Hamming distance to the
    /// row total.
    #[test]
    fn test_multivec_distance_hamming() {
        let vectors =
            multivecs_of::<UInt8Type>(vec![vec![0b0000_0000, 0b0000_1111], vec![0b0000_0011]], 1);
        let query: Arc<dyn Array> = Arc::new(UInt8Array::from(vec![0b0000_0000_u8, 0b0000_1111]));

        let dists = multivec_distance(query.as_ref(), &vectors, DistanceType::Hamming).unwrap();

        assert_eq!(dists, vec![0.0, 4.0]);
    }

    #[rstest::rstest]
    #[case::l2_perfect(
        DistanceType::L2,
        vec![1.0, 0.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 1.0],
        0.0
    )]
    #[case::cosine_perfect(
        DistanceType::Cosine,
        vec![1.0, 0.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 1.0],
        0.0
    )]
    #[case::dot_perfect(
        DistanceType::Dot,
        vec![1.0, 0.0, 0.0, 1.0],
        vec![1.0, 0.0, 0.0, 1.0],
        0.0
    )]
    #[case::cosine_repeated_query(
        DistanceType::Cosine,
        vec![0.6, 0.8],
        vec![1.0, 0.0, 1.0, 0.0],
        0.8
    )]
    #[case::cosine_single_query(
        DistanceType::Cosine,
        vec![0.0, 1.0],
        vec![1.0, 0.0],
        1.0
    )]
    fn test_multivec_distance_float(
        #[case] distance_type: DistanceType,
        #[case] vectors: Vec<f32>,
        #[case] query: Vec<f32>,
        #[case] expected: f32,
    ) {
        let vectors = multivec_of::<Float32Type>(vectors, 2);
        let query: Arc<dyn Array> = Arc::new(Float32Array::from(query));

        let dists = multivec_distance(query.as_ref(), &vectors, distance_type).unwrap();

        assert!((dists[0] - expected).abs() < 1e-6);
    }

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

    #[cfg_attr(coverage, coverage(off))]
    pub(super) fn arrow_values(dtype: &DataType, values: Vec<i32>) -> Arc<dyn Array> {
        match dtype {
            DataType::Float16 => Arc::new(PrimitiveArray::<Float16Type>::from(
                values
                    .into_iter()
                    .map(|v| f16::from_f32(v as f32))
                    .collect::<Vec<_>>(),
            )),
            DataType::Float32 => Arc::new(Float32Array::from(
                values.into_iter().map(|v| v as f32).collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                values.into_iter().map(|v| v as f64).collect::<Vec<_>>(),
            )),
            DataType::Int8 => Arc::new(Int8Array::from(
                values.into_iter().map(|v| v as i8).collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values.into_iter().map(|v| v as i16).collect::<Vec<_>>(),
            )),
            DataType::UInt8 => Arc::new(UInt8Array::from(
                values.into_iter().map(|v| v as u8).collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(values)),
            _ => panic!("unsupported test dtype {dtype}"),
        }
    }

    #[rstest::rstest]
    #[case::l2(DistanceType::L2, [0.0, 8.0])]
    #[case::dot(DistanceType::Dot, [-4.0, -10.0])]
    #[case::cosine(DistanceType::Cosine, [0.0, 1.0 - 11.0 / (5.0_f32.sqrt() * 5.0)])]
    #[case::hamming(DistanceType::Hamming, [0.0, 3.0])]
    fn test_arrow_support_table(
        #[case] metric: DistanceType,
        #[case] expected: [f32; 2],
        #[values(
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Int8,
            DataType::UInt8,
            DataType::Int32
        )]
        dtype: DataType,
    ) {
        let query = arrow_values(&dtype, vec![1, 2]);
        let vectors =
            FixedSizeListArray::try_new_from_values(arrow_values(&dtype, vec![1, 2, 3, 4]), 2)
                .unwrap();
        let rows = ListArray::new(
            Arc::new(Field::new("item", vectors.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(vectors.clone()),
            None,
        );
        let supports_batch = if metric == DistanceType::Hamming {
            dtype == DataType::UInt8
        } else {
            matches!(
                dtype,
                DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Int8
            )
        };
        let supports_multivec = supports_batch && dtype != DataType::Int8;
        let batch = metric.batch(query.as_ref(), &vectors);
        let legacy: ArrowBatchDistanceFunc = match metric {
            DistanceType::L2 => l2_distance_arrow_batch,
            DistanceType::Cosine => cosine_distance_arrow_batch,
            DistanceType::Dot => dot_distance_arrow_batch,
            DistanceType::Hamming => hamming_distance_arrow_batch,
        };
        for legacy in [legacy, metric.arrow_batch_func()] {
            match (&batch, legacy(query.as_ref(), &vectors)) {
                (Ok(batch), Ok(legacy)) => assert_eq!(batch, legacy.as_ref()),
                (Err(batch), Err(legacy)) => assert_eq!(batch.to_string(), legacy.to_string()),
                (batch, legacy) => panic!("legacy dispatch differs: {batch:?}, {legacy:?}"),
            }
        }
        let multivec = metric.multivec(query.as_ref(), &rows);
        for (result, supported) in [
            (batch.map(|a| a.values().to_vec()), supports_batch),
            (multivec, supports_multivec),
        ] {
            if supported {
                let got = result.unwrap();
                assert_eq!(got.len(), expected.len());
                for (got, want) in got.iter().zip(expected) {
                    approx::assert_abs_diff_eq!(*got, want, epsilon = 1e-6);
                }
            } else {
                let error = result.unwrap_err();
                assert!(
                    matches!(&error, ArrowError::InvalidArgumentError(message)
                    if message.contains("unsupported vector element type") && message.contains(&dtype.to_string())),
                    "{metric} {dtype}: {error}"
                );
            }
        }
    }

    #[rstest::rstest]
    #[case::l2(DistanceType::L2, [0.0, 8.0])]
    #[case::dot(DistanceType::Dot, [-4.0, -10.0])]
    #[case::cosine(DistanceType::Cosine, [0.0, 1.0 - 11.0 / (5.0_f32.sqrt() * 5.0)])]
    fn test_int8_query_with_legacy_stored_types(
        #[case] metric: DistanceType,
        #[case] expected: [f32; 2],
        #[values(DataType::Float32, DataType::Int16, DataType::Int32)] dtype: DataType,
    ) {
        let query = arrow_values(&DataType::Int8, vec![1, 2]);
        let vectors =
            FixedSizeListArray::try_new_from_values(arrow_values(&dtype, vec![1, 2, 3, 4]), 2)
                .unwrap();

        let got = metric.batch(query.as_ref(), &vectors).unwrap();
        for (got, want) in got.values().iter().zip(expected) {
            approx::assert_abs_diff_eq!(*got, want, epsilon = 1e-6);
        }

        let legacy: ArrowBatchDistanceFunc = match metric {
            DistanceType::L2 => l2_distance_arrow_batch,
            DistanceType::Cosine => cosine_distance_arrow_batch,
            DistanceType::Dot => dot_distance_arrow_batch,
            DistanceType::Hamming => hamming_distance_arrow_batch,
        };
        assert_eq!(&got, legacy(query.as_ref(), &vectors).unwrap().as_ref());
    }

    #[test]
    fn test_int8_query_does_not_broaden_past_legacy_stored_types() {
        let query = arrow_values(&DataType::Int8, vec![1, 2]);
        for dtype in [DataType::Float16, DataType::Float64, DataType::UInt8] {
            let vectors =
                FixedSizeListArray::try_new_from_values(arrow_values(&dtype, vec![1, 2, 3, 4]), 2)
                    .unwrap();
            assert!(
                DistanceType::L2.batch(query.as_ref(), &vectors).is_err(),
                "Int8 query unexpectedly accepted stored {dtype}"
            );
        }
    }

    #[rstest::rstest]
    #[case::l2(DistanceType::L2, DataType::Float32)]
    #[case::dot(DistanceType::Dot, DataType::Float32)]
    #[case::cosine(DistanceType::Cosine, DataType::Float32)]
    #[case::hamming(DistanceType::Hamming, DataType::UInt8)]
    fn test_arrow_batch_dimension_errors(
        #[case] metric: DistanceType,
        #[case] dtype: DataType,
        #[values(0, 1, 3)] query_len: usize,
    ) {
        let vectors =
            FixedSizeListArray::try_new_from_values(arrow_values(&dtype, vec![1, 2, 3, 4]), 2)
                .unwrap();
        let query = arrow_values(&dtype, vec![1; query_len]);
        let error = metric.batch(query.as_ref(), &vectors).unwrap_err();
        assert!(
            matches!(&error, ArrowError::InvalidArgumentError(message)
            if message.contains(&format!("query length {query_len}")) && message.contains("dimension 2")),
            "{error}"
        );
    }

    #[test]
    fn test_hamming_validation_and_legacy_wrapper() {
        let targets =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![1.0, 2.0]), 2).unwrap();
        let query = UInt8Array::from(vec![1, 2]);
        let error = hamming_distance_arrow_batch(&query, &targets).unwrap_err();
        assert!(
            matches!(&error, ArrowError::InvalidArgumentError(message)
            if message.contains("hamming batch") && message.contains("Float32")),
            "{error}"
        );

        let targets =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![1, 2]), 2).unwrap();
        let query = UInt8Array::from(vec![Some(1), None]);
        let error = DistanceType::Hamming.batch(&query, &targets).unwrap_err();
        assert!(
            matches!(&error, ArrowError::InvalidArgumentError(message)
            if message.contains("query must not contain nulls")),
            "{error}"
        );
        let query = UInt8Array::from(vec![1, 2]);
        assert_eq!(
            hamming_distance_arrow_batch(&query, &targets)
                .unwrap()
                .as_ref(),
            &DistanceType::Hamming.batch(&query, &targets).unwrap()
        );
        let error = DistanceType::Hamming.try_func::<f32>().unwrap_err();
        assert!(
            matches!(&error, ArrowError::InvalidArgumentError(message)
            if message.contains("Hamming") && message.contains("UInt8")),
            "{error}"
        );
    }

    #[rstest::rstest]
    #[case::l2(DistanceType::L2, DataType::Float32, 0.0)]
    #[case::dot(DistanceType::Dot, DataType::Float32, -24.0)]
    #[case::cosine(DistanceType::Cosine, DataType::Float32, 0.0)]
    #[case::hamming(DistanceType::Hamming, DataType::UInt8, 0.0)]
    fn test_arrow_batch_slices_and_null_rows(
        #[case] metric: DistanceType,
        #[case] dtype: DataType,
        #[case] expected: f32,
    ) {
        let values = arrow_values(&dtype, vec![99, 99, 3, 4, 0, 0, 99, 99]);
        let vectors = FixedSizeListArray::new(
            Arc::new(Field::new("item", dtype.clone(), true)),
            2,
            values,
            Some(arrow_buffer::NullBuffer::from(vec![
                true, true, false, true,
            ])),
        )
        .slice(1, 2);
        let query = arrow_values(&dtype, vec![99, 3, 4, 99]).slice(1, 2);
        let got = metric.batch(query.as_ref(), &vectors).unwrap();
        assert_eq!(got.len(), 2);
        approx::assert_abs_diff_eq!(got.value(0), expected, epsilon = 1e-6);
        assert!(got.is_null(1));

        let empty = vectors.slice(0, 0);
        assert_eq!(metric.batch(query.as_ref(), &empty).unwrap().len(), 0);
        let zero_dim = FixedSizeListArray::try_new_with_length(
            Arc::new(Field::new("item", dtype.clone(), true)),
            0,
            arrow_values(&dtype, vec![]),
            None,
            1,
        )
        .unwrap();
        let error = metric.batch(query.as_ref(), &zero_dim).unwrap_err();
        assert!(
            matches!(&error, ArrowError::InvalidArgumentError(message)
            if message.contains("dimension 0")),
            "{error}"
        );
    }

    #[test]
    fn test_multivec_sliced_null_and_empty_rows() {
        let rows = multivecs_of::<Float32Type>(
            vec![
                vec![99.0, 99.0],
                vec![1.0, 2.0, 3.0, 4.0],
                vec![],
                vec![5.0, 6.0],
            ],
            2,
        );
        let rows = ListArray::new(
            Arc::new(Field::new("item", rows.value_type(), true)),
            rows.offsets().clone(),
            rows.values().clone(),
            Some(arrow_buffer::NullBuffer::from(vec![
                true, true, true, false,
            ])),
        )
        .slice(1, 3);
        let query = Float32Array::from(vec![99.0, 3.0, 4.0, 99.0]).slice(1, 2);
        let got = DistanceType::L2.multivec(&query, &rows).unwrap();
        assert_eq!(got[0], 0.0);
        assert!(got[1].is_nan());
        assert!(got[2].is_nan());
    }
}
