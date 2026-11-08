// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared validation, support table, and dtype dispatch for Arrow vectors.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type, Int8Type, UInt8Type};
use arrow_array::{Array, ArrowPrimitiveType, FixedSizeListArray, Float32Array, ListArray};
use arrow_schema::{ArrowError, DataType};
use lance_arrow::FixedSizeListArrayExt;
use num_traits::AsPrimitive;

use super::{
    Cosine, DistanceFunc, DistanceType, Dot, L2, Normalize, cosine_distance_batch_into,
    dot_distance_batch_into, hamming, l2_distance_batch_into, norm_l2,
};
use crate::Result;

#[derive(Clone, Copy)]
pub(super) enum Norm {
    L2,
    Squared,
}

#[derive(Clone, Copy)]
pub(super) enum Operation<'a> {
    Batch(DistanceType),
    Multivec(DistanceType, &'a ListArray),
    Norm(Norm),
}

impl Operation<'_> {
    // Keep the accepted metric × dtype pairs here, including the deliberate
    // distinction between Int8 batches and multivectors.
    fn supports(self, dtype: &DataType) -> bool {
        matches!(
            (self, dtype),
            (
                Self::Batch(DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot)
                    | Self::Multivec(
                        DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot,
                        _
                    )
                    | Self::Norm(_),
                DataType::Float16 | DataType::Float32 | DataType::Float64,
            ) | (
                Self::Batch(DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot),
                DataType::Int8
            ) | (
                Self::Batch(DistanceType::Hamming) | Self::Multivec(DistanceType::Hamming, _),
                DataType::UInt8
            )
        )
    }

    fn supports_int8_query(self, dtype: &DataType) -> bool {
        matches!(
            (self, dtype),
            (
                Self::Batch(DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot),
                DataType::Float32 | DataType::Int8 | DataType::Int16 | DataType::Int32,
            )
        )
    }

    fn error(self, message: impl std::fmt::Display) -> ArrowError {
        let name = match self {
            Self::Batch(metric) => format!("{metric} batch"),
            Self::Multivec(metric, _) => format!("{metric} multivec"),
            Self::Norm(Norm::L2) => "L2 norm".to_string(),
            Self::Norm(Norm::Squared) => "squared L2 norm".to_string(),
        };
        ArrowError::InvalidArgumentError(format!("{name}: {message}"))
    }

    fn numeric<T>(self, query: &[T], values: &[T], dim: usize) -> Result<Vec<f32>>
    where
        T: L2 + Dot + Cosine + Normalize + AsPrimitive<f32> + AsPrimitive<f64>,
    {
        match self {
            Self::Batch(metric) => {
                let mut distances = vec![0.0; values.len() / dim];
                match metric {
                    DistanceType::L2 => l2_distance_batch_into(query, values, dim, &mut distances),
                    DistanceType::Cosine => {
                        cosine_distance_batch_into(query, values, dim, &mut distances)
                    }
                    DistanceType::Dot => {
                        dot_distance_batch_into(query, values, dim, &mut distances)
                    }
                    _ => return Err(self.error("Hamming requires UInt8 values")),
                }
                Ok(distances)
            }
            Self::Multivec(metric, rows) => {
                Ok(multivec(query, values, dim, rows, metric.try_func()?))
            }
            Self::Norm(Norm::L2) => Ok(values.chunks_exact(dim).map(norm_l2).collect()),
            Self::Norm(Norm::Squared) => Ok(values
                .chunks_exact(dim)
                .map(|v| {
                    // Preserve f64 accumulation for f64, and widen f16 before
                    // squaring so large and small half values remain finite.
                    if std::mem::size_of::<T>() > std::mem::size_of::<f32>() {
                        v.iter()
                            .map(|&v| {
                                let v: f64 = v.as_();
                                v * v
                            })
                            .sum::<f64>() as f32
                    } else {
                        v.iter()
                            .map(|&v| {
                                let v: f32 = v.as_();
                                v * v
                            })
                            .sum()
                    }
                })
                .collect()),
        }
    }

    fn binary(self, query: &[u8], values: &[u8], dim: usize) -> Result<Vec<f32>> {
        match self {
            Self::Batch(DistanceType::Hamming) => {
                Ok(hamming::hamming_distance_batch(query, values, dim).collect())
            }
            Self::Multivec(DistanceType::Hamming, rows) => {
                Ok(multivec(query, values, dim, rows, hamming::hamming))
            }
            _ => Err(self.error("UInt8 values require Hamming distance")),
        }
    }
}

/// Validate before the only Arrow dtype dispatch used by distances and norms.
pub(super) fn dispatch(
    operation: Operation<'_>,
    query: Option<&dyn Array>,
    vectors: &FixedSizeListArray,
) -> Result<Vec<f32>> {
    let dtype = vectors.value_type();
    let int8_query = query.is_some_and(|query| query.data_type() == &DataType::Int8)
        && operation.supports_int8_query(&dtype);
    if !operation.supports(&dtype) && !int8_query {
        return Err(operation.error(format!("unsupported vector element type {dtype}")));
    }
    if let Some(query) = query {
        if !int8_query && query.data_type() != &dtype {
            return Err(operation.error(format!(
                "query type {} does not match the stored vector type {dtype}",
                query.data_type()
            )));
        }
        if query.null_count() > 0 {
            return Err(operation.error(format!(
                "query must not contain nulls, found {} in {} values",
                query.null_count(),
                query.len()
            )));
        }
    }
    let dim = vectors.value_length() as usize;
    if dim == 0 {
        return Err(operation.error("stored vectors have dimension 0"));
    }
    if let Some(query) = query {
        match operation {
            Operation::Batch(_) if query.len() != dim => {
                return Err(operation.error(format!(
                    "query length {} must equal the vector dimension {dim}",
                    query.len()
                )));
            }
            Operation::Multivec(_, _) if query.is_empty() || !query.len().is_multiple_of(dim) => {
                return Err(operation.error(format!(
                    "query length {} must be a positive multiple of the vector dimension {dim}",
                    query.len()
                )));
            }
            _ => {}
        }
    }

    match dtype {
        DataType::Float16 => numeric::<Float16Type>(operation, query, vectors, dim),
        DataType::Float32 if int8_query => numeric_int8_query(operation, query, vectors, dim),
        DataType::Float32 => numeric::<Float32Type>(operation, query, vectors, dim),
        DataType::Float64 => numeric::<Float64Type>(operation, query, vectors, dim),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            numeric_int8_query(operation, query, vectors, dim)
        }
        DataType::UInt8 => operation.binary(
            query.map_or(&[], |q| q.as_primitive::<UInt8Type>().values().as_ref()),
            vectors.values().as_primitive::<UInt8Type>().values(),
            dim,
        ),
        _ => Err(operation.error(format!("unsupported vector element type {dtype}"))),
    }
}

fn numeric_int8_query(
    operation: Operation<'_>,
    query: Option<&dyn Array>,
    vectors: &FixedSizeListArray,
    dim: usize,
) -> Result<Vec<f32>> {
    let query = query.ok_or_else(|| operation.error("Int8 values require a query"))?;
    let query = query.as_primitive::<Int8Type>();
    let query = Float32Array::from(query.values().iter().map(|&v| v as f32).collect::<Vec<_>>());
    let vectors = vectors.convert_to_floating_point()?;
    numeric::<Float32Type>(operation, Some(&query), &vectors, dim)
}

fn numeric<T>(
    operation: Operation<'_>,
    query: Option<&dyn Array>,
    vectors: &FixedSizeListArray,
    dim: usize,
) -> Result<Vec<f32>>
where
    T: ArrowPrimitiveType,
    T::Native: L2 + Dot + Cosine + Normalize + AsPrimitive<f32> + AsPrimitive<f64>,
{
    operation.numeric(
        query.map_or(&[], |q| q.as_primitive::<T>().values().as_ref()),
        vectors.values().as_primitive::<T>().values(),
        dim,
    )
}

fn multivec<T>(
    query: &[T],
    values: &[T],
    dim: usize,
    rows: &ListArray,
    distance: DistanceFunc<T>,
) -> Vec<f32> {
    rows.value_offsets()
        .windows(2)
        .enumerate()
        .map(|(row, offsets)| {
            if rows.is_null(row) || offsets[0] == offsets[1] {
                return f32::NAN;
            }
            let values = &values[offsets[0] as usize * dim..offsets[1] as usize * dim];
            query
                .chunks_exact(dim)
                .map(|q| {
                    values
                        .chunks_exact(dim)
                        .map(|v| distance(q, v))
                        .min_by(f32::total_cmp)
                        .unwrap_or(f32::NAN)
                })
                .sum()
        })
        .collect()
}
