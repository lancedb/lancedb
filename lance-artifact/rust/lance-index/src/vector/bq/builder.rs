// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, UInt8Array};
use arrow_schema::{DataType, Field};
use bitvec::prelude::{BitVec, Lsb0};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray, FloatType};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use ndarray::{Axis, ShapeBuilder, s};
use num_traits::{AsPrimitive, FromPrimitive};
use rand_distr::Distribution;
use rayon::prelude::*;

use crate::vector::bq::storage::{
    RABIT_CODE_COLUMN, RABIT_METADATA_KEY, RabitQuantizationMetadata, RabitQuantizationStorage,
    RabitQueryEstimator, rabit_binary_code_field, rabit_ex_code_field,
};
use crate::vector::bq::transform::{
    ADD_FACTORS_FIELD, ERROR_FACTORS_FIELD, EX_ADD_FACTORS_FIELD, EX_SCALE_FACTORS_FIELD,
    SCALE_FACTORS_FIELD,
};
use crate::vector::bq::{
    RQBuildParams, RQRotationType, rabit_binary_code_bytes, rabit_ex_bits,
    rotation::{apply_fast_rotation, fast_rotation_signs_len, random_fast_rotation_signs},
    validate_rq_num_bits,
};
use crate::vector::quantizer::{Quantization, Quantizer, QuantizerBuildParams};

/// Build parameters for RabitQuantizer.
///
/// num_bits: the number of bits per dimension.
pub struct RabitBuildParams {
    pub num_bits: u8,
    pub rotation_type: RQRotationType,
}

impl Default for RabitBuildParams {
    fn default() -> Self {
        Self {
            num_bits: 1,
            rotation_type: RQRotationType::default(),
        }
    }
}

impl QuantizerBuildParams for RabitBuildParams {
    fn sample_size(&self) -> usize {
        // RabitQ doesn't need to sample any data
        0
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub struct RabitQuantizer {
    metadata: RabitQuantizationMetadata,
}

pub(crate) struct RabitQuantizedBatch {
    pub binary_codes: ArrayRef,
    pub ex_codes: Option<ArrayRef>,
    pub ex_res_dot_dists: Option<Vec<f32>>,
    pub rotated_residuals: Option<Vec<f32>>,
    pub ex_code_values: Option<Vec<u8>>,
}

#[inline]
fn pack_sign_bits(codes: &mut [u8], rotated: &[f32]) {
    codes.fill(0);
    for (bit_idx, value) in rotated.iter().enumerate() {
        if value.is_sign_positive() {
            codes[bit_idx / u8::BITS as usize] |= 1u8 << (bit_idx % u8::BITS as usize);
        }
    }
}

const EX_QUANTIZATION_EPSILON: f32 = 1.0e-5;
const EX_TIGHT_START: [f32; 9] = [0.0, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81];

fn best_ex_rescale_factor(abs_normalized: &[f32], ex_bits: u8) -> f32 {
    let max_value = abs_normalized
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .fold(0.0f32, f32::max);
    if max_value <= 0.0 {
        return 0.0;
    }

    let max_code = (1usize << ex_bits) - 1;
    let t_end = ((max_code + 10) as f32) / max_value;
    let t_start = t_end * EX_TIGHT_START[ex_bits as usize];

    let mut current_codes = Vec::with_capacity(abs_normalized.len());
    let mut squared_denominator = abs_normalized.len() as f32 * 0.25;
    let mut numerator = 0.0f32;
    let mut thresholds = Vec::with_capacity(abs_normalized.len() * max_code);

    for (idx, &value) in abs_normalized.iter().enumerate() {
        if value <= 0.0 || !value.is_finite() {
            current_codes.push(0usize);
            continue;
        }

        let current = ((t_start * value) + EX_QUANTIZATION_EPSILON)
            .floor()
            .clamp(0.0, max_code as f32) as usize;
        current_codes.push(current);
        squared_denominator += (current * current + current) as f32;
        numerator += (current as f32 + 0.5) * value;

        let mut next = current + 1;
        while next <= max_code {
            let threshold = next as f32 / value;
            if threshold < t_end {
                thresholds.push((threshold, idx));
            }
            next += 1;
        }
    }

    thresholds.sort_unstable_by(|(left, _), (right, _)| left.total_cmp(right));

    let mut best_inner_product = numerator / squared_denominator.sqrt();
    let mut best_t = t_start;
    for (threshold, idx) in thresholds {
        current_codes[idx] += 1;
        let updated = current_codes[idx];
        squared_denominator += (2 * updated) as f32;
        numerator += abs_normalized[idx];

        let current_inner_product = numerator / squared_denominator.sqrt();
        if current_inner_product > best_inner_product {
            best_inner_product = current_inner_product;
            best_t = threshold;
        }
    }

    best_t
}

fn quantize_ex_code(
    rotated: &[f32],
    ex_bits: u8,
    ex_code_dst: &mut [u8],
    ex_code_values_dst: &mut [u8],
) -> f32 {
    debug_assert_eq!(rotated.len(), ex_code_values_dst.len());
    let norm_squared = rotated.iter().map(|value| value * value).sum::<f32>();
    if norm_squared <= f32::EPSILON || !norm_squared.is_finite() {
        ex_code_dst.fill(0);
        ex_code_values_dst.fill(0);
        return 0.0;
    }

    let norm = norm_squared.sqrt();
    let abs_normalized = rotated
        .iter()
        .map(|value| value.abs() / norm)
        .collect::<Vec<_>>();
    let t = best_ex_rescale_factor(&abs_normalized, ex_bits);
    let max_code = ((1u16 << ex_bits) - 1) as u8;
    let mask = max_code;
    let code_bias = -((1u32 << ex_bits) as f32 - 0.5);
    let mut residual_dot_code = 0.0f32;

    for ((&value, &abs_value), ex_code_value) in rotated
        .iter()
        .zip(abs_normalized.iter())
        .zip(ex_code_values_dst.iter_mut())
    {
        let mut ex_code = ((t * abs_value) + EX_QUANTIZATION_EPSILON)
            .floor()
            .clamp(0.0, max_code as f32) as u8;
        if value.is_sign_negative() {
            ex_code = (!ex_code) & mask;
        }
        let sign_code = u8::from(value.is_sign_positive());
        let full_code = ((sign_code as u32) << ex_bits) + ex_code as u32;
        residual_dot_code += value * (full_code as f32 + code_bias);
        *ex_code_value = ex_code;
    }

    crate::vector::bq::ex_dot::pack_blocked_row(ex_code_values_dst, ex_bits, ex_code_dst);
    residual_dot_code
}

impl RabitQuantizer {
    pub fn new<T: ArrowFloatType>(num_bits: u8, dim: i32) -> Self {
        Self::new_with_rotation::<T>(num_bits, dim, RQRotationType::default())
    }

    pub fn new_with_rotation<T: ArrowFloatType>(
        num_bits: u8,
        dim: i32,
        rotation_type: RQRotationType,
    ) -> Self {
        debug_assert!(dim >= 0, "RabitQ dimension should be non-negative");
        let code_dim = dim as usize;
        let metadata = match rotation_type {
            RQRotationType::Matrix => {
                // we don't need to calculate the inverse of P, just take generated Q as P^{-1}
                let rotate_mat = random_orthogonal::<T>(code_dim);
                let (rotate_mat, _) = rotate_mat.into_raw_vec_and_offset();
                let rotate_mat = match T::FLOAT_TYPE {
                    FloatType::Float16 | FloatType::Float32 | FloatType::Float64 => {
                        let rotate_mat = <T::ArrayType as FloatArray<T>>::from_values(rotate_mat);
                        FixedSizeListArray::try_new_from_values(rotate_mat, code_dim as i32)
                            .unwrap()
                    }
                    _ => unimplemented!("RabitQ does not support data type: {:?}", T::FLOAT_TYPE),
                };
                RabitQuantizationMetadata {
                    rotate_mat: Some(rotate_mat),
                    rotate_mat_position: None,
                    fast_rotation_signs: None,
                    rotation_type,
                    code_dim: code_dim as u32,
                    num_bits,
                    packed: false,
                    query_estimator: RabitQueryEstimator::RawQuery,
                }
            }
            RQRotationType::Fast => RabitQuantizationMetadata {
                rotate_mat: None,
                rotate_mat_position: None,
                fast_rotation_signs: Some(random_fast_rotation_signs(code_dim)),
                rotation_type,
                code_dim: code_dim as u32,
                num_bits,
                packed: false,
                query_estimator: RabitQueryEstimator::RawQuery,
            },
        };
        Self { metadata }
    }

    pub fn num_bits(&self) -> u8 {
        self.metadata.num_bits
    }

    pub fn rotation_type(&self) -> RQRotationType {
        self.metadata.rotation_type
    }

    pub fn metadata_ref(&self) -> &RabitQuantizationMetadata {
        &self.metadata
    }

    fn from_supplied_rotation(params: &RQBuildParams, dim: usize) -> Result<Option<Self>> {
        let Some(metadata) = params.rotation.as_ref() else {
            return Ok(None);
        };

        if metadata.num_bits != params.num_bits {
            return Err(Error::invalid_input(format!(
                "rabitq_model num_bits={} does not match requested num_bits={}",
                metadata.num_bits, params.num_bits
            )));
        }

        let rotated_dim = metadata.rotated_dim();
        if rotated_dim != dim {
            return Err(Error::invalid_input(format!(
                "rabitq_model dimension={} does not match vector dimension={}",
                rotated_dim, dim
            )));
        }

        match metadata.rotation_type {
            RQRotationType::Fast => {
                let signs = metadata.fast_rotation_signs.as_ref().ok_or_else(|| {
                    Error::invalid_input(
                        "rabitq_model fast rotation is missing fast_rotation_signs".to_string(),
                    )
                })?;
                let expected_len = fast_rotation_signs_len(dim);
                if signs.len() != expected_len {
                    return Err(Error::invalid_input(format!(
                        "rabitq_model fast_rotation_signs length={} does not match expected length={} for dimension={}",
                        signs.len(),
                        expected_len,
                        dim
                    )));
                }
            }
            RQRotationType::Matrix => {
                let rotate_mat = metadata.rotate_mat.as_ref().ok_or_else(|| {
                    Error::invalid_input(
                        "rabitq_model matrix rotation is missing rotate_mat".to_string(),
                    )
                })?;
                if rotate_mat.len() != dim || rotate_mat.value_length() != dim as i32 {
                    return Err(Error::invalid_input(format!(
                        "rabitq_model matrix rotation shape=({}, {}) does not match vector dimension={}",
                        rotate_mat.len(),
                        rotate_mat.value_length(),
                        dim
                    )));
                }
            }
        }

        Ok(Some(Self {
            metadata: metadata.clone(),
        }))
    }

    #[inline]
    fn fast_rotation_signs(&self) -> &[u8] {
        self.metadata
            .fast_rotation_signs
            .as_ref()
            .expect("RabitQ fast rotation signs missing")
            .as_slice()
    }

    #[inline]
    fn rotate_mat_flat<T: ArrowFloatType>(&self) -> &[T::Native] {
        let rotate_mat = self.metadata.rotate_mat.as_ref().unwrap();
        rotate_mat
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice()
    }

    #[inline]
    fn rotate_mat<T: ArrowFloatType>(&'_ self) -> ndarray::ArrayView2<'_, T::Native> {
        let code_dim = self.code_dim();
        ndarray::ArrayView2::from_shape((code_dim, code_dim), self.rotate_mat_flat::<T>()).unwrap()
    }

    fn rotate_vectors<T: ArrowFloatType>(
        &self,
        vectors: ndarray::ArrayView2<'_, T::Native>,
    ) -> ndarray::Array2<f32>
    where
        T::Native: AsPrimitive<f32>,
    {
        let dim = vectors.nrows();
        let code_dim = self.code_dim();
        match self.rotation_type() {
            RQRotationType::Matrix => {
                let rotate_mat = self.rotate_mat::<T>();
                let rotate_mat = rotate_mat.slice(s![.., 0..dim]);
                rotate_mat.dot(&vectors).mapv(|v| v.as_())
            }
            RQRotationType::Fast => {
                let signs = self.fast_rotation_signs();
                let ncols = vectors.ncols();
                let mut rotated_data = vec![0.0f32; code_dim * ncols];
                rotated_data
                    .par_chunks_mut(code_dim)
                    .enumerate()
                    .for_each_init(
                        || vec![0.0f32; code_dim],
                        |scratch, (col_idx, dst)| {
                            let column = vectors.column(col_idx);
                            let input = column
                                .as_slice()
                                .expect("RabitQ input vectors should be contiguous");
                            apply_fast_rotation(input, scratch, signs);
                            dst.copy_from_slice(scratch);
                        },
                    );

                ndarray::Array2::from_shape_vec((code_dim, ncols).f(), rotated_data).unwrap()
            }
        }
    }

    pub fn rotate_fsl_to_f32(&self, vectors: &FixedSizeListArray) -> Result<Vec<f32>> {
        match vectors.value_type() {
            DataType::Float16 => self.rotate_fsl_to_f32_typed::<Float16Type>(vectors),
            DataType::Float32 => self.rotate_fsl_to_f32_typed::<Float32Type>(vectors),
            DataType::Float64 => self.rotate_fsl_to_f32_typed::<Float64Type>(vectors),
            value_type => Err(Error::invalid_input(format!(
                "Unsupported data type: {:?}",
                value_type
            ))),
        }
    }

    fn rotate_fsl_to_f32_typed<T: ArrowFloatType>(
        &self,
        vectors: &FixedSizeListArray,
    ) -> Result<Vec<f32>>
    where
        T::Native: AsPrimitive<f32> + Sync,
    {
        let dim = self.dim();
        if vectors.value_length() as usize != dim {
            return Err(Error::invalid_input(format!(
                "Vector dimension mismatch: {} != {}",
                vectors.value_length(),
                dim
            )));
        }
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Vector values have unexpected data type: {}",
                    vectors.value_type()
                ))
            })?
            .as_slice();
        let vec_mat = ndarray::ArrayView2::from_shape((vectors.len(), dim), values)
            .map_err(|e| Error::invalid_input(e.to_string()))?;
        let rotated = self.rotate_vectors::<T>(vec_mat.t());
        let code_dim = self.code_dim();
        let mut row_major = vec![0.0f32; vectors.len() * code_dim];
        for row_idx in 0..vectors.len() {
            for (dst, value) in row_major[row_idx * code_dim..(row_idx + 1) * code_dim]
                .iter_mut()
                .zip(rotated.column(row_idx).iter())
            {
                *dst = *value;
            }
        }
        Ok(row_major)
    }

    pub fn dim(&self) -> usize {
        self.code_dim()
    }

    // compute the dot product of v_q * v_r
    pub fn codes_res_dot_dists<T: ArrowFloatType>(
        &self,
        residual_vectors: &FixedSizeListArray,
    ) -> Result<Vec<f32>>
    where
        T::Native: AsPrimitive<f32> + Sync,
    {
        let dim = self.dim();
        if residual_vectors.value_length() as usize != dim {
            return Err(Error::invalid_input(format!(
                "Vector dimension mismatch: {} != {}",
                residual_vectors.value_length(),
                dim
            )));
        }

        let sqrt_dim = (dim as f32).sqrt();
        let values = residual_vectors
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();

        match self.rotation_type() {
            RQRotationType::Matrix => {
                // convert the vector to a dxN matrix
                let vec_mat =
                    ndarray::ArrayView2::from_shape((residual_vectors.len(), dim), values)
                        .map_err(|e| Error::invalid_input(e.to_string()))?;
                let vec_mat = vec_mat.t();
                let rotated_vectors = self.rotate_vectors::<T>(vec_mat);
                let norm_dists = rotated_vectors.mapv(f32::abs).sum_axis(Axis(0)) / sqrt_dim;
                debug_assert_eq!(norm_dists.len(), residual_vectors.len());
                Ok(norm_dists.to_vec())
            }
            RQRotationType::Fast => {
                let code_dim = self.code_dim();
                let signs = self.fast_rotation_signs();
                let mut norm_dists = vec![0.0f32; residual_vectors.len()];
                norm_dists
                    .par_iter_mut()
                    .zip(values.par_chunks_exact(dim))
                    .for_each_init(
                        || vec![0.0f32; code_dim],
                        |scratch, (dst, input)| {
                            apply_fast_rotation(input, scratch, signs);
                            *dst = scratch.iter().map(|v| v.abs()).sum::<f32>() / sqrt_dim;
                        },
                    );
                Ok(norm_dists)
            }
        }
    }

    fn transform<T: ArrowFloatType>(
        &self,
        residual_vectors: &FixedSizeListArray,
    ) -> Result<ArrayRef>
    where
        T::Native: AsPrimitive<f32> + Sync,
    {
        // we don't need to normalize the residual vectors,
        // because the sign of P^{-1} * v_r is the same as P^{-1} * v_r / ||v_r||
        let n = residual_vectors.len();
        let dim = self.dim();
        debug_assert_eq!(residual_vectors.values().len(), n * dim);
        let values = residual_vectors
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();
        let code_dim = self.code_dim();
        let code_bytes = rabit_binary_code_bytes(code_dim);

        match self.rotation_type() {
            RQRotationType::Matrix => {
                let vectors = ndarray::ArrayView2::from_shape((n, dim), values)
                    .map_err(|e| Error::invalid_input(e.to_string()))?;
                let vectors = vectors.t();
                let rotated_vectors = self.rotate_vectors::<T>(vectors);

                let quantized_vectors = rotated_vectors.t().mapv(|v| v.is_sign_positive());
                let bv: BitVec<u8, Lsb0> = BitVec::from_iter(quantized_vectors);

                let codes = UInt8Array::from(bv.into_vec());
                debug_assert_eq!(codes.len(), n * code_bytes);
                Ok(Arc::new(FixedSizeListArray::try_new_from_values(
                    codes,
                    code_bytes as i32,
                )?))
            }
            RQRotationType::Fast => {
                let signs = self.fast_rotation_signs();
                let mut encoded_codes = vec![0u8; n * code_bytes];
                encoded_codes
                    .par_chunks_mut(code_bytes)
                    .zip(values.par_chunks_exact(dim))
                    .for_each_init(
                        || vec![0.0f32; code_dim],
                        |scratch, (code_dst, input)| {
                            apply_fast_rotation(input, scratch, signs);
                            pack_sign_bits(code_dst, scratch);
                        },
                    );
                let codes = UInt8Array::from(encoded_codes);
                debug_assert_eq!(codes.len(), n * code_bytes);
                Ok(Arc::new(FixedSizeListArray::try_new_from_values(
                    codes,
                    code_bytes as i32,
                )?))
            }
        }
    }

    pub(crate) fn quantize_split(
        &self,
        vectors: &FixedSizeListArray,
    ) -> Result<RabitQuantizedBatch> {
        match vectors.value_type() {
            DataType::Float16 => self.transform_split::<Float16Type>(vectors),
            DataType::Float32 => self.transform_split::<Float32Type>(vectors),
            DataType::Float64 => self.transform_split::<Float64Type>(vectors),
            value_type => Err(Error::invalid_input(format!(
                "Unsupported data type: {:?}",
                value_type
            ))),
        }
    }

    fn transform_split<T: ArrowFloatType>(
        &self,
        residual_vectors: &FixedSizeListArray,
    ) -> Result<RabitQuantizedBatch>
    where
        T::Native: AsPrimitive<f32> + Sync,
    {
        let ex_bits = rabit_ex_bits(self.metadata.num_bits)?;
        let n = residual_vectors.len();
        let dim = self.dim();
        debug_assert_eq!(residual_vectors.values().len(), n * dim);
        let values = residual_vectors
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();
        let code_dim = self.code_dim();
        let code_bytes = rabit_binary_code_bytes(code_dim);
        let ex_code_bytes = if ex_bits == 0 {
            0
        } else {
            crate::vector::bq::ex_dot::blocked_ex_code_bytes(code_dim, ex_bits)
        };

        let mut encoded_codes = vec![0u8; n * code_bytes];
        let mut encoded_ex_codes = (ex_bits != 0).then(|| vec![0u8; n * ex_code_bytes]);
        let mut ex_res_dot_dists = (ex_bits != 0).then(|| vec![0.0f32; n]);
        let mut rotated_residuals = vec![0.0f32; n * code_dim];
        let mut ex_code_values = (ex_bits != 0).then(|| vec![0u8; n * code_dim]);

        match self.rotation_type() {
            RQRotationType::Matrix => {
                let vectors = ndarray::ArrayView2::from_shape((n, dim), values)
                    .map_err(|e| Error::invalid_input(e.to_string()))?;
                let vectors = vectors.t();
                let rotated_vectors = self.rotate_vectors::<T>(vectors);

                encoded_codes
                    .chunks_mut(code_bytes)
                    .zip(rotated_residuals.chunks_mut(code_dim))
                    .enumerate()
                    .for_each(|(row_idx, (code_dst, rotated_dst))| {
                        for (dst, value) in rotated_dst
                            .iter_mut()
                            .zip(rotated_vectors.column(row_idx).iter())
                        {
                            *dst = *value;
                        }
                        pack_sign_bits(code_dst, rotated_dst);
                    });
            }
            RQRotationType::Fast => {
                let signs = self.fast_rotation_signs();
                encoded_codes
                    .par_chunks_mut(code_bytes)
                    .zip(rotated_residuals.par_chunks_mut(code_dim))
                    .zip(values.par_chunks_exact(dim))
                    .for_each(|((code_dst, rotated_dst), input)| {
                        apply_fast_rotation(input, rotated_dst, signs);
                        pack_sign_bits(code_dst, rotated_dst);
                    });
            }
        }

        if ex_bits != 0 {
            let encoded_ex_codes = encoded_ex_codes
                .as_mut()
                .expect("ex-code buffer should exist for multi-bit RQ");
            let ex_res_dot_dists = ex_res_dot_dists
                .as_mut()
                .expect("ex dot buffer should exist for multi-bit RQ");
            let ex_code_values = ex_code_values
                .as_mut()
                .expect("ex-code value buffer should exist for multi-bit RQ");
            encoded_ex_codes
                .par_chunks_mut(ex_code_bytes)
                .zip(ex_code_values.par_chunks_mut(code_dim))
                .zip(ex_res_dot_dists.par_iter_mut())
                .zip(rotated_residuals.par_chunks(code_dim))
                .for_each(|(((ex_dst, ex_values_dst), ex_dot_dst), rotated)| {
                    *ex_dot_dst = quantize_ex_code(rotated, ex_bits, ex_dst, ex_values_dst);
                });
        }

        let binary_codes = UInt8Array::from(encoded_codes);
        let ex_codes = encoded_ex_codes.map(UInt8Array::from);
        Ok(RabitQuantizedBatch {
            binary_codes: Arc::new(FixedSizeListArray::try_new_from_values(
                binary_codes,
                code_bytes as i32,
            )?),
            ex_codes: ex_codes
                .map(|ex_codes| {
                    FixedSizeListArray::try_new_from_values(ex_codes, ex_code_bytes as i32)
                        .map(|array| Arc::new(array) as ArrayRef)
                })
                .transpose()?,
            ex_res_dot_dists,
            rotated_residuals: Some(rotated_residuals),
            ex_code_values,
        })
    }
}

impl Quantization for RabitQuantizer {
    type BuildParams = RQBuildParams;
    type Metadata = RabitQuantizationMetadata;
    type Storage = RabitQuantizationStorage;

    fn build(
        data: &dyn Array,
        _: lance_linalg::distance::DistanceType,
        params: &Self::BuildParams,
    ) -> Result<Self> {
        validate_rq_num_bits(params.num_bits)?;

        let dim = data.as_fixed_size_list().value_length() as usize;
        if !dim.is_multiple_of(u8::BITS as usize) {
            return Err(Error::invalid_input(
                "vector dimension must be divisible by 8 for IVF_RQ",
            ));
        }
        if let Some(q) = Self::from_supplied_rotation(params, dim)? {
            return Ok(q);
        }

        let q = match data.as_fixed_size_list().value_type() {
            DataType::Float16 => Self::new_with_rotation::<Float16Type>(
                params.num_bits,
                data.as_fixed_size_list().value_length(),
                params.rotation_type,
            ),
            DataType::Float32 => Self::new_with_rotation::<Float32Type>(
                params.num_bits,
                data.as_fixed_size_list().value_length(),
                params.rotation_type,
            ),
            DataType::Float64 => Self::new_with_rotation::<Float64Type>(
                params.num_bits,
                data.as_fixed_size_list().value_length(),
                params.rotation_type,
            ),
            dt => {
                return Err(Error::invalid_input(format!(
                    "Unsupported data type: {:?}",
                    dt
                )));
            }
        };
        Ok(q)
    }

    fn retrain(&mut self, _data: &dyn Array) -> Result<()> {
        Ok(())
    }

    fn code_dim(&self) -> usize {
        if self.metadata.code_dim > 0 {
            self.metadata.code_dim as usize
        } else {
            self.metadata
                .rotate_mat
                .as_ref()
                .map(|rotate_mat| rotate_mat.len())
                .unwrap_or(0)
        }
    }

    fn column(&self) -> &'static str {
        RABIT_CODE_COLUMN
    }

    fn use_residual(_: lance_linalg::distance::DistanceType) -> bool {
        true
    }

    fn quantize(&self, vectors: &dyn Array) -> Result<arrow_array::ArrayRef> {
        let vectors = vectors.as_fixed_size_list();
        match vectors.value_type() {
            DataType::Float16 => self.transform::<Float16Type>(vectors),
            DataType::Float32 => self.transform::<Float32Type>(vectors),
            DataType::Float64 => self.transform::<Float64Type>(vectors),
            value_type => Err(Error::invalid_input(format!(
                "Unsupported data type: {:?}",
                value_type
            ))),
        }
    }

    fn metadata_key() -> &'static str {
        RABIT_METADATA_KEY
    }

    fn quantization_type() -> crate::vector::quantizer::QuantizationType {
        crate::vector::quantizer::QuantizationType::Rabit
    }

    fn metadata(
        &self,
        args: Option<crate::vector::quantizer::QuantizationMetadata>,
    ) -> Self::Metadata {
        let mut metadata = self.metadata.clone();
        metadata.packed = args.map(|args| args.transposed).unwrap_or_default();
        metadata
    }

    fn from_metadata(
        metadata: &Self::Metadata,
        _: lance_linalg::distance::DistanceType,
    ) -> Result<Quantizer> {
        validate_rq_num_bits(metadata.num_bits)?;
        Ok(Quantizer::Rabit(Self {
            metadata: metadata.clone(),
        }))
    }

    fn field(&self) -> Field {
        rabit_binary_code_field(self.code_dim())
    }

    fn extra_fields(&self) -> Vec<Field> {
        let mut fields = vec![ADD_FACTORS_FIELD.clone(), SCALE_FACTORS_FIELD.clone()];
        if self.metadata.query_estimator == RabitQueryEstimator::RawQuery {
            fields.push(ERROR_FACTORS_FIELD.clone());
        }
        if let Some(ex_code_field) = rabit_ex_code_field(self.code_dim(), self.metadata.num_bits)
            .expect("RabitQ num_bits should be validated")
        {
            fields.push(ex_code_field);
            fields.push(EX_ADD_FACTORS_FIELD.clone());
            fields.push(EX_SCALE_FACTORS_FIELD.clone());
        }
        fields
    }
}

impl TryFrom<Quantizer> for RabitQuantizer {
    type Error = Error;

    fn try_from(quantizer: Quantizer) -> Result<Self> {
        match quantizer {
            Quantizer::Rabit(quantizer) => Ok(quantizer),
            _ => Err(Error::invalid_input(
                "Cannot convert non-RabitQuantizer to RabitQuantizer",
            )),
        }
    }
}

impl From<RabitQuantizer> for Quantizer {
    fn from(quantizer: RabitQuantizer) -> Self {
        Self::Rabit(quantizer)
    }
}

fn random_normal_matrix(n: usize) -> ndarray::Array2<f64> {
    let mut rng = rand::rng();
    let normal = rand_distr::Normal::new(0.0, 1.0).unwrap();
    ndarray::Array2::from_shape_simple_fn((n, n), || normal.sample(&mut rng))
}

// implement the householder qr decomposition referenced from https://en.wikipedia.org/wiki/Householder_transformation#QR_decomposition
fn householder_qr(a: ndarray::Array2<f64>) -> (ndarray::Array2<f64>, ndarray::Array2<f64>) {
    let (m, n) = a.dim();
    let mut q = ndarray::Array2::eye(m);
    let mut r = a;

    for k in 0..n.min(m - 1) {
        let mut x = r.slice(s![k.., k]).to_owned();
        let x_norm = x.dot(&x).sqrt();

        if x_norm < f64::EPSILON {
            continue;
        }

        // Create Householder vector
        let sign = if x[0] >= 0.0 { 1.0 } else { -1.0 };
        x[0] += sign * x_norm;
        let u = &x / x.dot(&x).sqrt();

        // Apply Householder transformation to R
        // Compute outer product manually
        let mut u_outer = ndarray::Array2::zeros((m - k, m - k));
        for i in 0..(m - k) {
            for j in 0..(m - k) {
                u_outer[[i, j]] = u[i] * u[j];
            }
        }
        let h = ndarray::Array2::eye(m - k) - 2.0 * u_outer;

        // Apply transformation to R
        let r_block = r.slice(s![k.., k..]).to_owned();
        let h_r = h.dot(&r_block);
        r.slice_mut(s![k.., k..]).assign(&h_r);

        // Apply transformation to Q
        let q_block = q.slice(s![.., k..]).to_owned();
        let q_h = q_block.dot(&h);
        q.slice_mut(s![.., k..]).assign(&q_h);
    }

    (q, r)
}

fn random_orthogonal<T: ArrowFloatType>(n: usize) -> ndarray::Array2<T::Native>
where
    T::Native: FromPrimitive,
{
    let a = random_normal_matrix(n);
    let (q, _) = householder_qr(a);

    // cast f64 matrix to T::Native matrix
    q.mapv(|v| T::Native::from_f64(v).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use arrow::datatypes::Float32Type;
    use arrow_array::{FixedSizeListArray, Float32Array};
    use lance_linalg::distance::DistanceType;
    use rstest::rstest;

    use crate::vector::bq::storage::RABIT_BLOCKED_EX_CODE_COLUMN;

    #[rstest]
    #[case(8)]
    #[case(16)]
    #[case(32)]
    fn test_householder_qr(#[case] n: usize) {
        let a = random_normal_matrix(n);
        let (m, n) = a.dim();

        let (q, r) = householder_qr(a.clone());

        // Check Q is orthogonal: Q^T * Q should be identity
        let q_t_q = q.t().dot(&q);
        for i in 0..m {
            for j in 0..m {
                let expected = if i == j { 1.0 } else { 0.0 };
                assert_relative_eq!(q_t_q[[i, j]], expected, epsilon = 1e-5);
            }
        }

        // Check QR decomposition: Q * R should equal original matrix
        let qr = q.dot(&r);
        for i in 0..m {
            for j in 0..n {
                assert_relative_eq!(qr[[i, j]], a[[i, j]], epsilon = 1e-5);
            }
        }

        // Check R is upper triangular
        for i in 1..n.min(m) {
            for j in 0..i {
                assert_relative_eq!(r[[i, j]], 0.0, epsilon = 1e-5);
            }
        }

        // Additional check: Q should have shape (m, m) and R should have shape (m, n)
        assert_eq!(q.dim(), (m, m));
        assert_eq!(r.dim(), (m, n));
    }

    #[test]
    fn test_rabit_quantizer_rotation_modes() {
        let fast_q = RabitQuantizer::new_with_rotation::<Float32Type>(1, 128, RQRotationType::Fast);
        assert_eq!(fast_q.rotation_type(), RQRotationType::Fast);
        assert_eq!(fast_q.dim(), 128);
        assert_eq!(fast_q.code_dim(), 128);

        let matrix_q =
            RabitQuantizer::new_with_rotation::<Float32Type>(1, 128, RQRotationType::Matrix);
        assert_eq!(matrix_q.rotation_type(), RQRotationType::Matrix);
        assert_eq!(matrix_q.dim(), 128);
        assert_eq!(matrix_q.code_dim(), 128);
    }

    #[test]
    fn test_rabit_quantizer_field_uses_binary_code_size() {
        let q = RabitQuantizer::new_with_rotation::<Float32Type>(1, 128, RQRotationType::Fast);
        let field = q.field();
        let DataType::FixedSizeList(_, code_bytes) = field.data_type() else {
            panic!("RabitQ code field should be FixedSizeList");
        };
        assert_eq!(*code_bytes, 16);
    }

    #[test]
    fn test_rabit_quantizer_extra_fields_include_raw_query_error_factor() {
        let q = RabitQuantizer::new_with_rotation::<Float32Type>(1, 128, RQRotationType::Fast);
        let fields = q.extra_fields();
        assert!(
            fields
                .iter()
                .any(|field| field.name() == ERROR_FACTORS_FIELD.name())
        );
        assert!(
            !fields
                .iter()
                .any(|field| field.name() == RABIT_BLOCKED_EX_CODE_COLUMN)
        );

        let q = RabitQuantizer::new_with_rotation::<Float32Type>(3, 128, RQRotationType::Fast);
        let fields = q.extra_fields();
        for expected in [
            ERROR_FACTORS_FIELD.name().as_str(),
            RABIT_BLOCKED_EX_CODE_COLUMN,
            EX_ADD_FACTORS_FIELD.name().as_str(),
            EX_SCALE_FACTORS_FIELD.name().as_str(),
        ] {
            assert!(
                fields.iter().any(|field| field.name().as_str() == expected),
                "missing {expected}"
            );
        }
    }

    #[test]
    fn test_rabit_quantizer_requires_dim_divisible_by_8() {
        let vectors = Float32Array::from(vec![0.0f32; 4 * 30]);
        let fsl = FixedSizeListArray::try_new_from_values(vectors, 30).unwrap();
        let params = RQBuildParams::new(1);

        let err = RabitQuantizer::build(&fsl, DistanceType::L2, &params).unwrap_err();
        assert!(
            err.to_string()
                .contains("vector dimension must be divisible by 8 for IVF_RQ"),
            "{}",
            err
        );
    }

    #[test]
    fn test_rabit_quantizer_reuses_supplied_rotation() {
        let vectors = Float32Array::from(vec![0.0f32; 4 * 32]);
        let fsl = FixedSizeListArray::try_new_from_values(vectors, 32).unwrap();
        let supplied =
            RabitQuantizer::new_with_rotation::<Float32Type>(3, 32, RQRotationType::Fast)
                .metadata(None);
        let supplied_signs = supplied.fast_rotation_signs.clone();

        let mut params = RQBuildParams::with_rotation_type(3, RQRotationType::Fast);
        params.rotation = Some(supplied);

        let quantizer = RabitQuantizer::build(&fsl, DistanceType::L2, &params).unwrap();
        let metadata = quantizer.metadata_ref();
        assert_eq!(metadata.num_bits, 3);
        assert_eq!(metadata.rotation_type, RQRotationType::Fast);
        assert_eq!(metadata.fast_rotation_signs, supplied_signs);
    }

    #[test]
    fn test_rabit_quantizer_validates_supplied_rotation() {
        let vectors = Float32Array::from(vec![0.0f32; 4 * 32]);
        let fsl = FixedSizeListArray::try_new_from_values(vectors, 32).unwrap();
        let supplied =
            RabitQuantizer::new_with_rotation::<Float32Type>(3, 32, RQRotationType::Fast)
                .metadata(None);

        let mut wrong_num_bits = supplied.clone();
        wrong_num_bits.num_bits = 1;
        let mut params = RQBuildParams::with_rotation_type(3, RQRotationType::Fast);
        params.rotation = Some(wrong_num_bits);
        let err = RabitQuantizer::build(&fsl, DistanceType::L2, &params).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not match requested num_bits")
        );

        let mut wrong_dim = supplied.clone();
        wrong_dim.code_dim = 64;
        let mut params = RQBuildParams::with_rotation_type(3, RQRotationType::Fast);
        params.rotation = Some(wrong_dim);
        let err = RabitQuantizer::build(&fsl, DistanceType::L2, &params).unwrap_err();
        assert!(err.to_string().contains("does not match vector dimension"));

        let mut wrong_sign_len = supplied;
        wrong_sign_len.fast_rotation_signs.as_mut().unwrap().pop();
        let mut params = RQBuildParams::with_rotation_type(3, RQRotationType::Fast);
        params.rotation = Some(wrong_sign_len);
        let err = RabitQuantizer::build(&fsl, DistanceType::L2, &params).unwrap_err();
        assert!(err.to_string().contains("fast_rotation_signs length"));
    }

    #[test]
    fn test_rabit_quantizer_accepts_multi_bit_range() {
        let vectors = Float32Array::from(vec![0.0f32; 4 * 32]);
        let fsl = FixedSizeListArray::try_new_from_values(vectors, 32).unwrap();

        let err =
            RabitQuantizer::build(&fsl, DistanceType::L2, &RQBuildParams::new(0)).unwrap_err();
        assert!(
            err.to_string().contains("IVF_RQ num_bits must be in"),
            "{}",
            err
        );

        for rotation_type in [RQRotationType::Fast, RQRotationType::Matrix] {
            let quantizer = RabitQuantizer::build(
                &fsl,
                DistanceType::L2,
                &RQBuildParams::with_rotation_type(9, rotation_type),
            )
            .unwrap();
            let quantized = quantizer.quantize_split(&fsl).unwrap();
            assert!(quantized.ex_codes.is_some());
            assert_eq!(
                quantized.binary_codes.as_fixed_size_list().value_length(),
                4
            );
            assert_eq!(
                quantized
                    .ex_codes
                    .unwrap()
                    .as_fixed_size_list()
                    .value_length(),
                // dim=32 is padded to one 64-dim block at ex_bits=8.
                64
            );
        }

        let err =
            RabitQuantizer::build(&fsl, DistanceType::L2, &RQBuildParams::new(10)).unwrap_err();
        assert!(
            err.to_string().contains("IVF_RQ num_bits must be in"),
            "{}",
            err
        );
    }
}
