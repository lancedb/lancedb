// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema inference and bad-vector handling for incoming data streams.
//!
//! Detects variable-length list columns whose names suggest they are vectors
//! (contain "vector" or "embedding"), peeks the first batch to determine the
//! modal dimension, and converts them to `FixedSizeList` columns. Vectors with
//! mismatched lengths are handled via [`BadVectorStrategy`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, FixedSizeListBuilder, Float32Builder, UInt8Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Float32Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt8Array};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use futures::{stream, StreamExt, TryStreamExt};

use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::error::{Error, Result};

use super::or_boolean;

/// Strategy for handling vectors whose length doesn't match the modal dimension.
#[derive(Debug, Clone)]
pub enum BadVectorStrategy {
    /// Return an error if any vectors have mismatched lengths.
    Error,
    /// Replace mismatched vectors with null.
    Null,
    /// Drop rows containing mismatched vectors.
    Drop,
    /// Replace mismatched vectors with a vector filled with the given value.
    Fill(f64),
}

/// Path to a field within a schema, represented as integer offsets.
///
/// Supports nested struct fields, e.g. `FieldPath(vec![2, 0])` refers to the
/// first child of the third top-level field.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldPath(Vec<usize>);

impl FieldPath {
    pub fn new(indices: Vec<usize>) -> Self {
        Self(indices)
    }

    /// Extract the array at this path from a [`RecordBatch`].
    pub fn extract(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut arr: ArrayRef = batch.column(self.0[0]).clone();
        for &idx in &self.0[1..] {
            let struct_arr = arr.as_struct();
            arr = struct_arr.column(idx).clone();
        }
        Ok(arr)
    }

    /// The top-level column index.
    pub fn top_level_index(&self) -> usize {
        self.0[0]
    }
}

/// Returns true if `name` suggests the column is a vector column.
///
/// Checks (case-insensitively) whether the name contains "vector" or "embedding".
fn name_suggests_vector(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("vector") || lower.contains("embedding")
}

fn is_numeric_element(dt: &DataType) -> bool {
    dt.is_floating() || dt.is_integer()
}

/// Walk a schema recursively and find fields that look like vector candidates.
///
/// A field is a candidate if ALL of:
/// - Type is `List`, `LargeList`, or `ListView`
/// - Element type is integer or float
/// - Name contains "vector" or "embedding" (case insensitive)
///
/// Returns `(path, field_ref)` pairs. Already-fixed-size lists are excluded.
pub fn find_vector_candidates(schema: &Schema) -> Vec<(FieldPath, FieldRef)> {
    let mut result = Vec::new();
    for (i, field) in schema.fields().iter().enumerate() {
        find_candidates_recursive(field, vec![i], &mut result);
    }
    result
}

fn find_candidates_recursive(
    field: &FieldRef,
    path: Vec<usize>,
    result: &mut Vec<(FieldPath, FieldRef)>,
) {
    if name_suggests_vector(field.name())
        && is_variable_list_with_numeric_elements(field.data_type())
    {
        result.push((FieldPath::new(path.clone()), field.clone()));
    }

    // Recurse into struct children
    if let DataType::Struct(children) = field.data_type() {
        for (i, child) in children.iter().enumerate() {
            let mut child_path = path.clone();
            child_path.push(i);
            find_candidates_recursive(child, child_path, result);
        }
    }
}

fn is_variable_list_with_numeric_elements(dt: &DataType) -> bool {
    match dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::ListView(f) => {
            is_numeric_element(f.data_type())
        }
        _ => false,
    }
}

/// Compute the most common list length in an array.
///
/// Reads lengths directly from list offsets to avoid allocating a separate
/// lengths array.
fn modal_list_length(arr: &dyn Array) -> Result<i64> {
    let mut freq: HashMap<i64, usize> = HashMap::new();

    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_list::<i32>();
            let offsets = list.offsets();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let len = (offsets[i + 1] - offsets[i]) as i64;
                    *freq.entry(len).or_insert(0) += 1;
                }
            }
        }
        DataType::LargeList(_) => {
            let list = arr.as_list::<i64>();
            let offsets = list.offsets();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let len = offsets[i + 1] - offsets[i];
                    *freq.entry(len).or_insert(0) += 1;
                }
            }
        }
        _ => {
            return Err(Error::InvalidInput {
                message: format!("expected list type, got {:?}", arr.data_type()),
            });
        }
    }

    // Break ties by choosing the largest dimension
    freq.into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)))
        .map(|(len, _)| len)
        .ok_or_else(|| Error::InvalidInput {
            message: "cannot compute modal length of empty array".to_string(),
        })
}

/// Peek the first batch from a stream, returning the batch and a new stream
/// that yields the peeked batch followed by the remaining batches.
async fn peek_stream(
    mut stream: SendableRecordBatchStream,
) -> Result<(Option<RecordBatch>, SendableRecordBatchStream)> {
    let schema = stream.schema();
    match stream.try_next().await? {
        Some(first_batch) => {
            let cloned = first_batch.clone();
            let prepended = stream::once(futures::future::ready(Ok(cloned)));
            let new_stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
                prepended.chain(stream),
                schema,
            ));
            Ok((Some(first_batch), new_stream))
        }
        None => Ok((None, stream)),
    }
}

/// Determine the target element type for an integer list column based on peeked values.
fn infer_integer_element_type(arr: &dyn Array) -> DataType {
    let Some(flat) = flatten_list_values(arr) else {
        return DataType::UInt8;
    };

    if flat.len() - flat.null_count() == 0 {
        return DataType::UInt8;
    }

    // Use arrow's min/max kernels on the original integer type to avoid casting.
    fn check_range<T: ArrowPrimitiveType>(flat: &ArrayRef) -> bool
    where
        T::Native: PartialOrd + Into<i64>,
    {
        let arr = flat.as_primitive::<T>();
        let min_val: i64 = arrow::compute::min(arr).map(Into::into).unwrap_or(0);
        let max_val: i64 = arrow::compute::max(arr).map(Into::into).unwrap_or(0);
        min_val < 0 || max_val > 255
    }

    let out_of_u8_range = match flat.data_type() {
        DataType::Int8 => check_range::<arrow_array::types::Int8Type>(&flat),
        DataType::Int16 => check_range::<arrow_array::types::Int16Type>(&flat),
        DataType::Int32 => check_range::<Int32Type>(&flat),
        DataType::Int64 => check_range::<Int64Type>(&flat),
        DataType::UInt8 => false, // always 0..=255
        DataType::UInt16 => check_range::<arrow_array::types::UInt16Type>(&flat),
        DataType::UInt32 => check_range::<arrow_array::types::UInt32Type>(&flat),
        DataType::UInt64 => {
            // u64 doesn't implement Into<i64>, handle separately
            let arr = flat.as_primitive::<arrow_array::types::UInt64Type>();
            arrow::compute::max(arr).is_some_and(|v| v > 255)
        }
        _ => false,
    };

    if out_of_u8_range {
        DataType::Float32
    } else {
        DataType::UInt8
    }
}

/// Flatten the values from a variable-length list array.
fn flatten_list_values(arr: &dyn Array) -> Option<ArrayRef> {
    match arr.data_type() {
        DataType::List(_) => Some(arr.as_list::<i32>().values().clone()),
        DataType::LargeList(_) => Some(arr.as_list::<i64>().values().clone()),
        _ => None,
    }
}

/// Infer a vector schema from a stream by peeking the first batch.
///
/// For each candidate column (variable-length list with a name suggesting it's a vector),
/// determines the modal dimension and converts the column to a `FixedSizeList`.
///
/// Float elements become `FixedSizeList(Float32, dim)`.
/// Integer elements in 0..=255 become `FixedSizeList(UInt8, dim)`.
/// Integer elements outside that range become `FixedSizeList(Float32, dim)`.
/// All-null integer elements become `FixedSizeList(UInt8, dim)`.
///
/// Returns the new schema and a stream that yields transformed batches.
/// If there are no candidates, returns the original schema and stream unchanged.
pub async fn infer_vector_schema(
    stream: SendableRecordBatchStream,
    strategy: &BadVectorStrategy,
) -> Result<(SchemaRef, SendableRecordBatchStream)> {
    let schema = stream.schema();
    let candidates = find_vector_candidates(&schema);

    if candidates.is_empty() {
        return Ok((schema, stream));
    }

    // Only process top-level candidates for now (matching Python behavior)
    let top_level_candidates: Vec<_> = candidates
        .into_iter()
        .filter(|(path, _)| path.0.len() == 1)
        .collect();

    if top_level_candidates.is_empty() {
        return Ok((schema, stream));
    }

    let (peeked, stream) = peek_stream(stream).await?;
    let Some(peeked) = peeked else {
        return Ok((schema, stream));
    };

    // For each candidate, determine modal dim and target type
    let mut column_transforms: Vec<(usize, i64, DataType)> = Vec::new();

    for (path, field) in &top_level_candidates {
        let col_idx = path.top_level_index();
        let arr = peeked.column(col_idx);
        let dim = modal_list_length(arr.as_ref())?;

        let element_dt = match field.data_type() {
            DataType::List(f) | DataType::LargeList(f) | DataType::ListView(f) => {
                f.data_type().clone()
            }
            _ => unreachable!(),
        };

        let target_element_type = if element_dt.is_floating() {
            DataType::Float32
        } else if element_dt.is_integer() {
            infer_integer_element_type(arr.as_ref())
        } else {
            continue;
        };

        column_transforms.push((col_idx, dim, target_element_type));
    }

    if column_transforms.is_empty() {
        return Ok((schema, stream));
    }

    // Build new schema
    let mut new_fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    for &(col_idx, dim, ref target_element_type) in &column_transforms {
        let old_field = &schema.fields()[col_idx];
        let new_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", target_element_type.clone(), true)),
            dim as i32,
        );
        new_fields[col_idx] = Arc::new(Field::new(
            old_field.name(),
            new_type,
            old_field.is_nullable(),
        ));
    }
    let new_schema = Arc::new(Schema::new(new_fields));

    let strategy = strategy.clone();
    let transform_schema = new_schema.clone();
    let column_transforms: Arc<[(usize, i64, DataType)]> =
        Arc::from(column_transforms.into_boxed_slice());
    let new_stream = stream.and_then(move |batch| {
        let transforms = column_transforms.clone();
        let strategy = strategy.clone();
        let schema = transform_schema.clone();
        async move { transform_vector_batch(batch, &transforms, &strategy, &schema) }
    });

    let result_schema = new_schema.clone();
    let output: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream::new(
        new_stream,
        result_schema.clone(),
    ));
    Ok((result_schema, output))
}

/// Transform a single batch: convert variable-length list columns to FSL and handle bad vectors.
fn transform_vector_batch(
    batch: RecordBatch,
    transforms: &[(usize, i64, DataType)],
    strategy: &BadVectorStrategy,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    match strategy {
        BadVectorStrategy::Error => {
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            for &(col_idx, dim, ref target_element_type) in transforms {
                let arr = batch.column(col_idx);
                if any_wrong_dim(arr.as_ref(), dim)? {
                    let col_name = schema.field(col_idx).name();
                    return Err(Error::InvalidInput {
                        message: format!(
                            "column '{}' has vectors with lengths that don't match the \
                             modal dimension {}",
                            col_name, dim
                        ),
                    });
                }
                columns[col_idx] =
                    convert_list_to_fsl(arr.as_ref(), dim, target_element_type, strategy)?;
            }
            RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
        }
        BadVectorStrategy::Drop => {
            let num_rows = batch.num_rows();
            let mut any_bad = BooleanArray::from(vec![false; num_rows]);
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            for &(col_idx, dim, ref target_element_type) in transforms {
                let arr = batch.column(col_idx);
                let wrong = wrong_dim_mask(arr.as_ref(), dim)?;
                any_bad = or_boolean(&any_bad, &wrong)?;
                columns[col_idx] =
                    convert_list_to_fsl(arr.as_ref(), dim, target_element_type, strategy)?;
            }
            let keep = arrow::compute::not(&any_bad)?;
            let new_batch = RecordBatch::try_new(schema.clone(), columns)?;
            Ok(filter_record_batch(&new_batch, &keep)?)
        }
        _ => {
            // Null and Fill: conversion handles mismatched rows inline
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            for &(col_idx, dim, ref target_element_type) in transforms {
                let arr = batch.column(col_idx);
                columns[col_idx] =
                    convert_list_to_fsl(arr.as_ref(), dim, target_element_type, strategy)?;
            }
            RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
        }
    }
}

/// Check if any row has a list length different from the expected dimension.
///
/// Returns as soon as the first mismatch is found.
fn any_wrong_dim(arr: &dyn Array, expected_dim: i64) -> Result<bool> {
    let n = arr.len();
    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_list::<i32>();
            let offsets = list.offsets();
            let expected = expected_dim as i32;
            for i in 0..n {
                if !arr.is_null(i) && (offsets[i + 1] - offsets[i]) != expected {
                    return Ok(true);
                }
            }
        }
        DataType::LargeList(_) => {
            let list = arr.as_list::<i64>();
            let offsets = list.offsets();
            for i in 0..n {
                if !arr.is_null(i) && (offsets[i + 1] - offsets[i]) != expected_dim {
                    return Ok(true);
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

/// Build a per-row mask of rows with wrong list length.
fn wrong_dim_mask(arr: &dyn Array, expected_dim: i64) -> Result<BooleanArray> {
    let n = arr.len();
    let mut result = Vec::with_capacity(n);
    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_list::<i32>();
            let offsets = list.offsets();
            let expected = expected_dim as i32;
            for i in 0..n {
                result.push(!arr.is_null(i) && (offsets[i + 1] - offsets[i]) != expected);
            }
        }
        DataType::LargeList(_) => {
            let list = arr.as_list::<i64>();
            let offsets = list.offsets();
            for i in 0..n {
                result.push(!arr.is_null(i) && (offsets[i + 1] - offsets[i]) != expected_dim);
            }
        }
        _ => {
            result.resize(n, false);
        }
    }
    Ok(BooleanArray::from(result))
}

/// Convert a variable-length list array to a FixedSizeList array.
///
/// Casts the flat values buffer once up front, then indexes by offset to
/// avoid per-row `cast` calls.
fn convert_list_to_fsl(
    arr: &dyn Array,
    dim: i64,
    target_element_type: &DataType,
    strategy: &BadVectorStrategy,
) -> Result<ArrayRef> {
    // Get offsets and flat values from the list array
    let (offsets, flat_values) = get_list_offsets_and_values(arr)?;

    // Cast the entire flat values buffer once
    let cast_values = cast(&flat_values, target_element_type)?;

    match target_element_type {
        DataType::Float32 => {
            let typed = cast_values.as_primitive::<Float32Type>();
            build_fsl_from_offsets::<Float32Builder>(arr, &offsets, typed, dim, strategy)
        }
        DataType::UInt8 => {
            let typed = cast_values
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("cast to UInt8 should succeed");
            build_fsl_from_offsets::<UInt8Builder>(arr, &offsets, typed, dim, strategy)
        }
        _ => Err(Error::InvalidInput {
            message: format!("unsupported target element type: {:?}", target_element_type),
        }),
    }
}

/// Extract per-row offsets (as i64) and the flat values buffer from a list array.
fn get_list_offsets_and_values(arr: &dyn Array) -> Result<(Vec<i64>, ArrayRef)> {
    match arr.data_type() {
        DataType::List(_) => {
            let list = arr.as_list::<i32>();
            let offsets: Vec<i64> = list.offsets().iter().map(|&o| o as i64).collect();
            Ok((offsets, list.values().clone()))
        }
        DataType::LargeList(_) => {
            let list = arr.as_list::<i64>();
            let offsets: Vec<i64> = list.offsets().iter().copied().collect();
            Ok((offsets, list.values().clone()))
        }
        _ => Err(Error::InvalidInput {
            message: format!("expected list type, got {:?}", arr.data_type()),
        }),
    }
}

/// Build a FixedSizeList from pre-cast flat values using list offsets.
fn build_fsl_from_offsets<B: ArrowValueBuilder>(
    arr: &dyn Array,
    offsets: &[i64],
    cast_values: &B::ArrayType,
    dim: i64,
    strategy: &BadVectorStrategy,
) -> Result<ArrayRef> {
    let dim_usize = dim as usize;
    let n = arr.len();
    let mut builder =
        FixedSizeListBuilder::new(B::builder_with_capacity(n * dim_usize), dim as i32);

    for row in 0..n {
        if arr.is_null(row) {
            builder.append(false);
            B::append_nulls_n(builder.values(), dim_usize);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let row_len = end - start;

        if row_len != dim_usize {
            match strategy {
                BadVectorStrategy::Null | BadVectorStrategy::Error => {
                    builder.append(false);
                    B::append_nulls_n(builder.values(), dim_usize);
                }
                BadVectorStrategy::Fill(v) => {
                    builder.append(true);
                    B::append_fill(builder.values(), dim_usize, *v);
                }
                BadVectorStrategy::Drop => {
                    builder.append(true);
                    B::append_fill(builder.values(), dim_usize, 0.0);
                }
            }
        } else {
            builder.append(true);
            B::copy_range(builder.values(), cast_values, start, dim_usize);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Trait abstracting over Float32Builder and UInt8Builder for generic FSL construction.
trait ArrowValueBuilder: ArrayBuilder {
    type ArrayType: Array;

    fn builder_with_capacity(capacity: usize) -> Self;
    fn append_nulls_n(builder: &mut Self, count: usize);
    fn append_fill(builder: &mut Self, count: usize, value: f64);
    fn copy_range(builder: &mut Self, source: &Self::ArrayType, start: usize, len: usize);
}

impl ArrowValueBuilder for Float32Builder {
    type ArrayType = arrow_array::Float32Array;

    fn builder_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }
    fn append_nulls_n(builder: &mut Self, count: usize) {
        builder.append_nulls(count);
    }
    fn append_fill(builder: &mut Self, count: usize, value: f64) {
        let v = value as f32;
        for _ in 0..count {
            builder.append_value(v);
        }
    }
    fn copy_range(builder: &mut Self, source: &Self::ArrayType, start: usize, len: usize) {
        for j in 0..len {
            builder.append_value(source.value(start + j));
        }
    }
}

impl ArrowValueBuilder for UInt8Builder {
    type ArrayType = UInt8Array;

    fn builder_with_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }
    fn append_nulls_n(builder: &mut Self, count: usize) {
        builder.append_nulls(count);
    }
    fn append_fill(builder: &mut Self, count: usize, value: f64) {
        let v = value as u8;
        for _ in 0..count {
            builder.append_value(v);
        }
    }
    fn copy_range(builder: &mut Self, source: &Self::ArrayType, start: usize, len: usize) {
        for j in 0..len {
            builder.append_value(source.value(start + j));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::types::{Float32Type, Float64Type};
    use arrow_array::{Int32Array, ListArray, StringArray};
    use arrow_schema::Fields;
    use futures::TryStreamExt;

    use crate::arrow::SimpleRecordBatchStream;

    fn make_stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(SimpleRecordBatchStream::new(stream, schema))
    }

    // ====================== find_vector_candidates tests ======================

    #[test]
    fn test_find_candidates_list_float_named_vector() {
        let schema = Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, FieldPath::new(vec![0]));
    }

    #[test]
    fn test_find_candidates_name_patterns() {
        let schema = Schema::new(vec![
            Field::new(
                "user_vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "text_embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "VECTOR_COL",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]);
        let candidates = find_vector_candidates(&schema);
        assert_eq!(candidates.len(), 3);
    }

    #[test]
    fn test_find_candidates_rejects_normal_list() {
        let schema = Schema::new(vec![Field::new(
            "normal_list",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_integer_elements() {
        let schema = Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_find_candidates_rejects_non_numeric() {
        let schema = Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_rejects_fsl() {
        let schema = Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_find_candidates_large_list() {
        let schema = Schema::new(vec![Field::new(
            "embedding",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]);
        let candidates = find_vector_candidates(&schema);
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_find_candidates_nested_struct() {
        let inner_field = Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        );
        let struct_field = Field::new(
            "data",
            DataType::Struct(Fields::from(vec![inner_field])),
            true,
        );
        let schema = Schema::new(vec![struct_field]);
        let candidates = find_vector_candidates(&schema);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].0, FieldPath::new(vec![0, 0]));
    }

    #[test]
    fn test_find_candidates_empty_schema() {
        let schema = Schema::new(vec![] as Vec<Field>);
        let candidates = find_vector_candidates(&schema);
        assert!(candidates.is_empty());
    }

    // ====================== infer_vector_schema tests ======================

    #[tokio::test]
    async fn test_infer_float_list_uniform() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
                    Some(vec![Some(7.0), Some(8.0), Some(9.0)]),
                ])),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, mut stream) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(1).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );

        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        let fsl = result_batch.column(1).as_fixed_size_list();
        assert_eq!(fsl.value_length(), 3);
    }

    #[tokio::test]
    async fn test_infer_variable_lengths_modal() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        // 3 rows with dim=3, 1 row with dim=2 → modal dim is 3
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
                    Some(vec![Some(7.0), Some(8.0)]),
                    Some(vec![Some(10.0), Some(11.0), Some(12.0)]),
                ]),
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, mut stream) = infer_vector_schema(stream, &BadVectorStrategy::Null)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );

        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 4);
        let fsl = result_batch.column(0).as_fixed_size_list();
        assert!(fsl.is_null(2)); // row with wrong dim is null
        assert!(!fsl.is_null(0));
    }

    #[tokio::test]
    async fn test_infer_integer_0_255() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(100), Some(255)]),
                    Some(vec![Some(10), Some(20), Some(30)]),
                ],
            ))],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), 3)
        );
    }

    #[tokio::test]
    async fn test_infer_integer_negative() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(-1), Some(100), Some(200)]),
                    Some(vec![Some(10), Some(20), Some(30)]),
                ],
            ))],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );
    }

    #[tokio::test]
    async fn test_infer_integer_gt_255() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(256), Some(200)]),
                    Some(vec![Some(10), Some(20), Some(30)]),
                ],
            ))],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );
    }

    #[tokio::test]
    async fn test_infer_integer_all_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![Some(vec![None, None, None]), Some(vec![None, None, None])],
            ))],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), 3)
        );
    }

    #[tokio::test]
    async fn test_infer_no_candidates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let stream = make_stream(schema.clone(), vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(new_schema, schema);
    }

    #[tokio::test]
    async fn test_infer_all_batches_yielded() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
                    Some(1.0),
                    Some(2.0),
                ])]),
            )],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
                    Some(3.0),
                    Some(4.0),
                ])]),
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch1, batch2]);
        let (_, stream) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn test_infer_multiple_candidates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0)]),
                ])),
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    Some(vec![Some(3.0), Some(4.0), Some(5.0)]),
                ])),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (new_schema, _) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        assert_eq!(
            *new_schema.field(0).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2)
        );
        assert_eq!(
            *new_schema.field(1).data_type(),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
        );
    }

    #[tokio::test]
    async fn test_infer_error_strategy() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0)]),
                ]),
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (_, mut stream) = infer_vector_schema(stream, &BadVectorStrategy::Error)
            .await
            .unwrap();

        let result = stream.try_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_infer_drop_strategy() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0)]), // wrong dim
                    Some(vec![Some(7.0), Some(8.0), Some(9.0)]),
                ])),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (_, mut stream) = infer_vector_schema(stream, &BadVectorStrategy::Drop)
            .await
            .unwrap();

        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let ids = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn test_infer_fill_strategy() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0)]), // wrong dim
                ]),
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (_, stream) = infer_vector_schema(stream, &BadVectorStrategy::Fill(99.0))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let result_batch = &batches[0];
        assert_eq!(result_batch.num_rows(), 2);
        let fsl = result_batch.column(0).as_fixed_size_list();
        assert_eq!(fsl.value_length(), 3);
        // Row 1 (wrong dim) should be filled with 99.0
        let row1_values = fsl.value(1);
        let row1_f32 = row1_values.as_primitive::<Float32Type>();
        assert_eq!(row1_f32.value(0), 99.0);
        assert_eq!(row1_f32.value(1), 99.0);
        assert_eq!(row1_f32.value(2), 99.0);
    }

    #[tokio::test]
    async fn test_infer_null_strategy() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0), Some(5.0)]), // wrong dim
                    Some(vec![Some(7.0), Some(8.0), Some(9.0)]),
                ]),
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let (_, mut stream) = infer_vector_schema(stream, &BadVectorStrategy::Null)
            .await
            .unwrap();

        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        let fsl = result_batch.column(0).as_fixed_size_list();
        assert!(!fsl.is_null(0));
        assert!(fsl.is_null(1));
        assert!(!fsl.is_null(2));
    }
}
