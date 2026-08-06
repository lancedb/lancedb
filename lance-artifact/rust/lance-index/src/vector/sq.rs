// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{ops::Range, sync::Arc};

use arrow::array::AsArray;
use arrow::datatypes::{Float16Type, Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, UInt8Array};

use arrow_schema::{DataType, Field};
use builder::SQBuildParams;
use itertools::Itertools;
use lance_arrow::*;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use lance_linalg::distance::DistanceType;
use num_traits::*;
use storage::{SQ_METADATA_KEY, ScalarQuantizationMetadata, ScalarQuantizationStorage};

use super::SQ_CODE_COLUMN;
use super::quantizer::{Quantization, QuantizationMetadata, QuantizationType, Quantizer};

pub mod builder;
pub mod storage;
pub mod transform;

/// Scalar Quantization, optimized for [Apache Arrow] buffer memory layout.
///
//
// TODO: move this to be pub(crate) once we have a better way to test it.
#[derive(Debug, Clone)]
pub struct ScalarQuantizer {
    metadata: ScalarQuantizationMetadata,
}

impl DeepSizeOf for ScalarQuantizer {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        0
    }
}

impl ScalarQuantizer {
    pub fn new(num_bits: u16, dim: usize) -> Self {
        Self {
            metadata: ScalarQuantizationMetadata {
                num_bits,
                dim,
                bounds: Range::<f64> {
                    start: f64::MAX,
                    end: f64::MIN,
                },
            },
        }
    }

    pub fn with_bounds(num_bits: u16, dim: usize, bounds: Range<f64>) -> Self {
        let mut sq = Self::new(num_bits, dim);
        sq.metadata.bounds = bounds;
        sq
    }

    pub fn num_bits(&self) -> u16 {
        self.metadata.num_bits
    }

    pub fn update_bounds<T: ArrowFloatType>(
        &mut self,
        vectors: &FixedSizeListArray,
    ) -> Result<Range<f64>> {
        let data = vectors
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or(Error::index(format!(
                "Expect to be a float vector array, got: {:?}",
                vectors.value_type()
            )))?
            .as_slice();

        self.metadata.bounds = data.iter().fold(self.metadata.bounds.clone(), |f, v| {
            f.start.min(v.as_())..f.end.max(v.as_())
        });

        Ok(self.metadata.bounds.clone())
    }

    pub fn transform<T: ArrowFloatType>(&self, data: &dyn Array) -> Result<ArrayRef> {
        let fsl = data
            .as_fixed_size_list_opt()
            .ok_or(Error::index(format!(
                "Expect to be a FixedSizeList<float> vector array, got: {:?} array",
                data.data_type()
            )))?
            .clone();
        let data = fsl
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or(Error::index(format!(
                "Expect to be a float vector array, got: {:?}",
                fsl.value_type()
            )))?
            .as_slice();

        // TODO: support SQ4
        let builder: Vec<u8> = scale_to_u8::<T>(data, &self.metadata.bounds);

        Ok(Arc::new(FixedSizeListArray::try_new_from_values(
            UInt8Array::from(builder),
            fsl.value_length(),
        )?))
    }

    pub fn bounds(&self) -> Range<f64> {
        self.metadata.bounds.clone()
    }

    /// Whether to use residual as input or not.
    pub fn use_residual(&self) -> bool {
        false
    }
}

impl TryFrom<Quantizer> for ScalarQuantizer {
    type Error = Error;
    fn try_from(value: Quantizer) -> Result<Self> {
        match value {
            Quantizer::Scalar(sq) => Ok(sq),
            _ => Err(Error::index("Expect to be a ScalarQuantizer".to_string())),
        }
    }
}

impl Quantization for ScalarQuantizer {
    type BuildParams = SQBuildParams;
    type Metadata = ScalarQuantizationMetadata;
    type Storage = ScalarQuantizationStorage;

    fn build(data: &dyn Array, _: DistanceType, params: &Self::BuildParams) -> Result<Self> {
        let fsl = data.as_fixed_size_list_opt().ok_or(Error::index(format!(
            "SQ builder: input is not a FixedSizeList: {}",
            data.data_type()
        )))?;

        let mut quantizer = Self::new(params.num_bits, fsl.value_length() as usize);

        match fsl.value_type() {
            DataType::Float16 => {
                quantizer.update_bounds::<Float16Type>(fsl)?;
            }
            DataType::Float32 => {
                quantizer.update_bounds::<Float32Type>(fsl)?;
            }
            DataType::Float64 => {
                quantizer.update_bounds::<Float64Type>(fsl)?;
            }
            _ => {
                return Err(Error::index(format!(
                    "SQ builder: unsupported data type: {}",
                    fsl.value_type()
                )));
            }
        }

        Ok(quantizer)
    }

    fn retrain(&mut self, data: &dyn Array) -> Result<()> {
        let fsl = data.as_fixed_size_list_opt().ok_or(Error::index(format!(
            "SQ retrain: input is not a FixedSizeList: {}",
            data.data_type()
        )))?;

        match fsl.value_type() {
            DataType::Float16 => {
                self.update_bounds::<Float16Type>(fsl)?;
            }
            DataType::Float32 => {
                self.update_bounds::<Float32Type>(fsl)?;
            }
            DataType::Float64 => {
                self.update_bounds::<Float64Type>(fsl)?;
            }
            value_type => {
                return Err(Error::invalid_input(format!(
                    "unsupported data type {} for scalar quantizer",
                    value_type
                )));
            }
        }
        Ok(())
    }

    fn code_dim(&self) -> usize {
        self.metadata.dim
    }

    fn column(&self) -> &'static str {
        SQ_CODE_COLUMN
    }

    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef> {
        match vectors.as_fixed_size_list().value_type() {
            DataType::Float16 => self.transform::<Float16Type>(vectors),
            DataType::Float32 => self.transform::<Float32Type>(vectors),
            DataType::Float64 => self.transform::<Float64Type>(vectors),
            value_type => Err(Error::invalid_input(format!(
                "unsupported data type {} for scalar quantizer",
                value_type
            ))),
        }
    }

    fn metadata_key() -> &'static str {
        SQ_METADATA_KEY
    }

    fn quantization_type() -> QuantizationType {
        QuantizationType::Scalar
    }

    fn metadata(&self, _: Option<QuantizationMetadata>) -> Self::Metadata {
        self.metadata.clone()
    }

    fn from_metadata(metadata: &Self::Metadata, _: DistanceType) -> Result<Quantizer> {
        Ok(Quantizer::Scalar(Self {
            metadata: metadata.clone(),
        }))
    }

    fn field(&self) -> Field {
        Field::new(
            SQ_CODE_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, true)),
                self.code_dim() as i32,
            ),
            true,
        )
    }
}

pub(crate) fn scale_to_u8<T: ArrowFloatType>(values: &[T::Native], bounds: &Range<f64>) -> Vec<u8> {
    if bounds.start == bounds.end {
        return vec![0; values.len()];
    }

    let range = bounds.end - bounds.start;
    values
        .iter()
        .map(|&v| {
            let v = v.to_f64().unwrap();
            let v = (v - bounds.start) * 255.0 / range;
            v as u8 // rust `as` performs saturating cast when casting float to int, so it's safe and expected here
        })
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Float16Type, Float32Type, Float64Type};
    use arrow_array::{Float16Array, Float32Array, Float64Array};
    use half::f16;

    use super::*;

    #[tokio::test]
    async fn test_f16_sq8() {
        let float_values = Vec::from_iter((0..16).map(|v| f16::from_usize(v).unwrap()));
        let float_array = Float16Array::from_iter_values(float_values.clone());
        let vectors =
            FixedSizeListArray::try_new_from_values(float_array, float_values.len() as i32)
                .unwrap();
        let mut sq = ScalarQuantizer::new(8, float_values.len());

        sq.update_bounds::<Float16Type>(&vectors).unwrap();
        assert_eq!(sq.bounds().start, float_values[0].to_f64());
        assert_eq!(
            sq.bounds().end,
            float_values.last().cloned().unwrap().to_f64()
        );

        let sq_code = sq.transform::<Float16Type>(&vectors).unwrap();
        let sq_values = sq_code
            .as_fixed_size_list()
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        sq_values.values().iter().enumerate().for_each(|(i, v)| {
            assert_eq!(*v, (i * 17) as u8);
        });
    }

    #[tokio::test]
    async fn test_f32_sq8() {
        let float_values = Vec::from_iter((0..16).map(|v| v as f32));
        let float_array = Float32Array::from_iter_values(float_values.clone());
        let vectors =
            FixedSizeListArray::try_new_from_values(float_array, float_values.len() as i32)
                .unwrap();
        let mut sq = ScalarQuantizer::new(8, float_values.len());

        sq.update_bounds::<Float32Type>(&vectors).unwrap();
        assert_eq!(sq.bounds().start, float_values[0].to_f64().unwrap());
        assert_eq!(
            sq.bounds().end,
            float_values.last().cloned().unwrap().to_f64().unwrap()
        );

        let sq_code = sq.transform::<Float32Type>(&vectors).unwrap();
        let sq_values = sq_code
            .as_fixed_size_list()
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        sq_values.values().iter().enumerate().for_each(|(i, v)| {
            assert_eq!(*v, (i * 17) as u8,);
        });
    }

    #[tokio::test]
    async fn test_f64_sq8() {
        let float_values = Vec::from_iter((0..16).map(|v| v as f64));
        let float_array = Float64Array::from_iter_values(float_values.clone());
        let vectors =
            FixedSizeListArray::try_new_from_values(float_array, float_values.len() as i32)
                .unwrap();
        let mut sq = ScalarQuantizer::new(8, float_values.len());

        sq.update_bounds::<Float64Type>(&vectors).unwrap();
        assert_eq!(sq.bounds().start, float_values[0]);
        assert_eq!(sq.bounds().end, float_values.last().cloned().unwrap());

        let sq_code = sq.transform::<Float64Type>(&vectors).unwrap();
        let sq_values = sq_code
            .as_fixed_size_list()
            .values()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        sq_values.values().iter().enumerate().for_each(|(i, v)| {
            assert_eq!(*v, (i * 17) as u8,);
        });
    }

    #[tokio::test]
    async fn test_scale_to_u8_with_nan() {
        let values = vec![0.0, 1.0, 2.0, 3.0, f64::NAN];
        let bounds = Range::<f64> {
            start: 0.0,
            end: 3.0,
        };
        let u8_values = scale_to_u8::<Float64Type>(&values, &bounds);
        assert_eq!(u8_values, vec![0, 85, 170, 255, 0]);
    }
}
