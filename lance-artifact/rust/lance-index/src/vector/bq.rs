// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Binary Quantization (BQ)

use std::iter::once;
use std::str::FromStr;
use std::sync::Arc;

use crate::pb::vector_index_details::RabitQuantization;
use arrow_array::types::Float32Type;
use arrow_array::{Array, ArrayRef, UInt8Array, cast::AsArray};
use lance_core::{Error, Result};
use num_traits::Float;
use serde::{Deserialize, Serialize};

use crate::vector::bq::storage::RabitQuantizationMetadata;
use crate::vector::quantizer::QuantizerBuildParams;

pub mod builder;
pub(crate) mod dist_table_quant;
pub mod ex_dot;
pub mod prune;
pub mod rotation;
pub mod storage;
pub mod transform;

pub const RABIT_MIN_NUM_BITS: u8 = 1;
pub const RABIT_MAX_NUM_BITS: u8 = 9;
pub const RABIT_BINARY_NUM_BITS: u8 = 1;

#[derive(Clone, Default)]
pub struct BinaryQuantization {}

impl BinaryQuantization {
    /// Transform an array of float vectors to binary vectors.
    pub fn transform(&self, data: &dyn Array) -> Result<ArrayRef> {
        let fsl = data
            .as_fixed_size_list_opt()
            .ok_or(Error::index(format!(
                "Expect to be a float vector array, got: {:?}",
                data.data_type()
            )))?
            .clone();

        let data = fsl
            .values()
            .as_primitive_opt::<Float32Type>()
            .ok_or(Error::index(format!(
                "Expect to be a float32 vector array, got: {:?}",
                fsl.values().data_type()
            )))?;
        let dim = fsl.value_length() as usize;
        let code = data
            .values()
            .chunks_exact(dim)
            .flat_map(binary_quantization)
            .collect::<Vec<_>>();

        Ok(Arc::new(UInt8Array::from(code)))
    }
}

/// Binary quantization.
///
/// Use the sign bit of the float vector to represent the binary vector.
fn binary_quantization<T: Float>(data: &[T]) -> impl Iterator<Item = u8> + '_ {
    let iter = data.chunks_exact(8);
    iter.clone()
        .map(|c| {
            // Auto vectorized.
            // Before changing this code, please check the assembly output.
            let mut bits: u8 = 0;
            c.iter().enumerate().for_each(|(idx, v)| {
                bits |= (v.is_sign_positive() as u8) << idx;
            });
            bits
        })
        .chain(once(0).map(move |_| {
            let mut bits: u8 = 0;
            iter.remainder().iter().enumerate().for_each(|(idx, v)| {
                bits |= (v.is_sign_positive() as u8) << idx;
            });
            bits
        }))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RQRotationType {
    #[default]
    Fast,
    Matrix,
}

impl FromStr for RQRotationType {
    type Err = Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.to_lowercase().as_str() {
            "fast" | "fht_kac" | "fht-kac" => Ok(Self::Fast),
            "matrix" | "dense" => Ok(Self::Matrix),
            _ => Err(Error::invalid_input(format!(
                "Unknown RQ rotation type: {}. Expected one of: fast, matrix",
                value
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RQBuildParams {
    pub num_bits: u8,
    pub rotation_type: RQRotationType,
    /// Optional pre-built rotation to reuse instead of generating a fresh random one.
    ///
    /// Distributed `IVF_RQ` builds mint one rotation and broadcast it so every segment
    /// rotates vectors identically. This is transient build-time state and is never
    /// persisted to the `RabitQuantization` params proto.
    pub rotation: Option<RabitQuantizationMetadata>,
}

pub fn validate_rq_num_bits(num_bits: u8) -> Result<()> {
    if !(RABIT_MIN_NUM_BITS..=RABIT_MAX_NUM_BITS).contains(&num_bits) {
        return Err(Error::invalid_input(format!(
            "IVF_RQ num_bits must be in {}..={}, got {}",
            RABIT_MIN_NUM_BITS, RABIT_MAX_NUM_BITS, num_bits
        )));
    }
    Ok(())
}

pub fn validate_supported_rq_num_bits(num_bits: u8) -> Result<()> {
    validate_rq_num_bits(num_bits)
}

pub fn rabit_ex_bits(num_bits: u8) -> Result<u8> {
    validate_rq_num_bits(num_bits)?;
    Ok(num_bits - RABIT_BINARY_NUM_BITS)
}

pub fn rabit_binary_code_bytes(rotated_dim: usize) -> usize {
    rotated_dim.div_ceil(u8::BITS as usize)
}

pub fn rabit_ex_code_bytes(rotated_dim: usize, ex_bits: u8) -> Result<usize> {
    let total_bits = rotated_dim.checked_mul(ex_bits as usize).ok_or_else(|| {
        Error::invalid_input(format!(
            "IVF_RQ ex-code byte size overflow: rotated_dim={}, ex_bits={}",
            rotated_dim, ex_bits
        ))
    })?;
    Ok(total_bits.div_ceil(u8::BITS as usize))
}

impl RQBuildParams {
    pub fn new(num_bits: u8) -> Self {
        Self {
            num_bits,
            rotation_type: RQRotationType::default(),
            rotation: None,
        }
    }

    pub fn with_rotation_type(num_bits: u8, rotation_type: RQRotationType) -> Self {
        Self {
            num_bits,
            rotation_type,
            rotation: None,
        }
    }
}

impl From<&RQBuildParams> for RabitQuantization {
    fn from(value: &RQBuildParams) -> Self {
        use crate::pb::vector_index_details::rabit_quantization::RotationType;
        Self {
            num_bits: value.num_bits as u32,
            rotation_type: match value.rotation_type {
                RQRotationType::Fast => RotationType::Fast as i32,
                RQRotationType::Matrix => RotationType::Matrix as i32,
            },
        }
    }
}

impl QuantizerBuildParams for RQBuildParams {
    fn sample_size(&self) -> usize {
        0
    }
}

impl Default for RQBuildParams {
    fn default() -> Self {
        Self {
            num_bits: 1,
            rotation_type: RQRotationType::default(),
            rotation: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use half::{bf16, f16};

    fn test_bq<T: Float>() {
        let data: Vec<T> = [1.0, -1.0, 1.0, -5.0, -7.0, -1.0, 1.0, -1.0, -0.2, 1.2, 3.2]
            .iter()
            .map(|&v| T::from(v).unwrap())
            .collect();
        let expected = vec![0b01000101, 0b00000110];
        let result = binary_quantization(&data).collect::<Vec<_>>();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_binary_quantization() {
        test_bq::<bf16>();
        test_bq::<f16>();
        test_bq::<f32>();
        test_bq::<f64>();
    }

    #[test]
    fn test_rotation_type_parse() {
        assert_eq!(
            "fast".parse::<RQRotationType>().unwrap(),
            RQRotationType::Fast
        );
        assert_eq!(
            "matrix".parse::<RQRotationType>().unwrap(),
            RQRotationType::Matrix
        );
        assert!("invalid".parse::<RQRotationType>().is_err());
    }

    #[test]
    fn test_rabit_num_bits_validation() {
        validate_rq_num_bits(1).unwrap();
        validate_rq_num_bits(9).unwrap();

        let err = validate_rq_num_bits(0).unwrap_err();
        assert!(
            err.to_string().contains("IVF_RQ num_bits must be in"),
            "{}",
            err
        );

        let err = validate_rq_num_bits(10).unwrap_err();
        assert!(
            err.to_string().contains("IVF_RQ num_bits must be in"),
            "{}",
            err
        );

        validate_supported_rq_num_bits(1).unwrap();
        validate_supported_rq_num_bits(9).unwrap();
    }

    #[test]
    fn test_rabit_split_code_byte_sizing() {
        assert_eq!(rabit_ex_bits(1).unwrap(), 0);
        assert_eq!(rabit_ex_bits(9).unwrap(), 8);

        assert_eq!(rabit_binary_code_bytes(128), 16);
        assert_eq!(rabit_binary_code_bytes(129), 17);

        assert_eq!(rabit_ex_code_bytes(128, 0).unwrap(), 0);
        assert_eq!(rabit_ex_code_bytes(128, 3).unwrap(), 48);
        assert_eq!(rabit_ex_code_bytes(128, 8).unwrap(), 128);
        assert_eq!(rabit_ex_code_bytes(129, 3).unwrap(), 49);
    }
}
