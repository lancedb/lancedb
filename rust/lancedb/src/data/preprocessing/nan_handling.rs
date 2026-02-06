// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! NaN handling for float vector columns in incoming data streams.
//!
//! Scans ALL float vector columns — `FixedSizeList`, `List`, and `LargeList` —
//! (regardless of name) and handles rows containing NaN values via [`NanStrategy`].

use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use arrow_array::builder::NullBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, GenericListArray, OffsetSizeTrait,
    PrimitiveArray, RecordBatch,
};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, SchemaRef};
use arrow_select::filter::filter_record_batch;
use futures::TryStreamExt;
use num_traits::Float;

use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::error::{Error, Result};

use super::or_boolean;

/// Strategy for handling vectors that contain NaN values.
#[derive(Debug, Clone)]
pub enum NanStrategy {
    /// Return an error if any vectors contain NaN.
    Error,
    /// Replace NaN-containing vectors with null.
    Null,
    /// Drop rows containing NaN vectors.
    Drop,
    /// Replace NaN elements with the given value.
    Fill(f64),
}

fn is_float_element(inner: &Field) -> bool {
    matches!(
        inner.data_type(),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// Find all top-level float vector columns (FixedSizeList, List, or LargeList).
fn find_float_vector_columns(schema: &SchemaRef) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, field)| match field.data_type() {
            DataType::FixedSizeList(inner, _) if is_float_element(inner) => Some(i),
            DataType::List(inner) if is_float_element(inner) => Some(i),
            DataType::LargeList(inner) if is_float_element(inner) => Some(i),
            _ => None,
        })
        .collect()
}

/// Wrap a stream to handle NaN values in all float vector columns.
///
/// For each batch, detects rows containing NaN values in any float vector column
/// (`FixedSizeList`, `List`, or `LargeList`) and applies the given [`NanStrategy`].
///
/// If the schema contains no float vector columns, returns the stream unchanged.
pub fn handle_nan_vectors(
    stream: SendableRecordBatchStream,
    strategy: &NanStrategy,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let vec_cols = find_float_vector_columns(&schema);

    if vec_cols.is_empty() {
        return stream;
    }

    let strategy = strategy.clone();
    let transform_schema = schema.clone();
    let new_stream = stream.and_then(move |batch| {
        let cols = vec_cols.clone();
        let strat = strategy.clone();
        let schema = transform_schema.clone();
        async move { handle_nan_batch(batch, &cols, &strat, &schema) }
    });

    Box::pin(SimpleRecordBatchStream::new(new_stream, schema))
}

/// Check if any row in a column contains NaN, dispatching on column type.
fn any_nan_in_column(col: &dyn Array) -> bool {
    match col.data_type() {
        DataType::FixedSizeList(..) => any_nan_in_fsl(col.as_fixed_size_list()),
        DataType::List(_) => any_nan_in_list::<i32>(col),
        DataType::LargeList(_) => any_nan_in_list::<i64>(col),
        _ => false,
    }
}

/// Compute a per-row NaN mask, dispatching on column type.
fn nan_row_mask_for_column(col: &dyn Array) -> BooleanArray {
    match col.data_type() {
        DataType::FixedSizeList(..) => nan_row_mask(col.as_fixed_size_list()),
        DataType::List(_) => nan_row_mask_list::<i32>(col),
        DataType::LargeList(_) => nan_row_mask_list::<i64>(col),
        _ => BooleanArray::from(vec![false; col.len()]),
    }
}

/// Nullify bad rows, dispatching on column type.
fn nullify_column_rows(col: &dyn Array, is_bad: &BooleanArray) -> Result<ArrayRef> {
    match col.data_type() {
        DataType::FixedSizeList(..) => nullify_fsl_rows(col.as_fixed_size_list(), is_bad),
        DataType::List(_) => nullify_list_rows::<i32>(col, is_bad),
        DataType::LargeList(_) => nullify_list_rows::<i64>(col, is_bad),
        _ => unreachable!("only called for float vector columns"),
    }
}

/// Fill NaN elements, dispatching on column type.
fn fill_column_rows(col: &dyn Array, fill: f64) -> Result<ArrayRef> {
    match col.data_type() {
        DataType::FixedSizeList(..) => fill_fsl_rows(col.as_fixed_size_list(), fill),
        DataType::List(_) => fill_list_rows::<i32>(col, fill),
        DataType::LargeList(_) => fill_list_rows::<i64>(col, fill),
        _ => unreachable!("only called for float vector columns"),
    }
}

/// Process a single batch: detect and handle NaN values in float vector columns.
fn handle_nan_batch(
    batch: RecordBatch,
    vec_cols: &[usize],
    strategy: &NanStrategy,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    match strategy {
        NanStrategy::Error => {
            for &col_idx in vec_cols {
                if any_nan_in_column(batch.column(col_idx)) {
                    let col_name = schema.field(col_idx).name();
                    return Err(Error::InvalidInput {
                        message: format!("column '{}' contains vectors with NaN values", col_name),
                    });
                }
            }
            Ok(batch)
        }
        NanStrategy::Fill(v) => {
            let mut columns: Option<Vec<ArrayRef>> = None;
            for &col_idx in vec_cols {
                if any_nan_in_column(batch.column(col_idx)) {
                    let cols = columns.get_or_insert_with(|| batch.columns().to_vec());
                    cols[col_idx] = fill_column_rows(batch.column(col_idx), *v)?;
                }
            }
            match columns {
                Some(cols) => Ok(RecordBatch::try_new(schema.clone(), cols)?),
                None => Ok(batch),
            }
        }
        NanStrategy::Drop => {
            let num_rows = batch.num_rows();
            let mut any_bad = BooleanArray::from(vec![false; num_rows]);
            for &col_idx in vec_cols {
                let mask = nan_row_mask_for_column(batch.column(col_idx));
                any_bad = or_boolean(&any_bad, &mask)?;
            }
            let keep = arrow::compute::not(&any_bad)?;
            let filtered = filter_record_batch(&batch, &keep)?;
            Ok(filtered)
        }
        NanStrategy::Null => {
            let mut columns: Option<Vec<ArrayRef>> = None;
            for &col_idx in vec_cols {
                let mask = nan_row_mask_for_column(batch.column(col_idx));
                let has_any = mask.iter().any(|v| v == Some(true));
                if has_any {
                    let cols = columns.get_or_insert_with(|| batch.columns().to_vec());
                    cols[col_idx] = nullify_column_rows(batch.column(col_idx), &mask)?;
                }
            }
            match columns {
                Some(cols) => Ok(RecordBatch::try_new(schema.clone(), cols)?),
                None => Ok(batch),
            }
        }
    }
}

/// Check whether any row in the FSL contains a NaN value.
///
/// Returns as soon as the first NaN is found — no allocation.
fn any_nan_in_fsl(arr: &FixedSizeListArray) -> bool {
    let values = arr.values();
    let n = arr.len();
    let dim = arr.value_length() as usize;

    match values.data_type() {
        DataType::Float16 => {
            let vals = values.as_primitive::<Float16Type>();
            any_nan_scan(arr, n, dim, |idx| vals.value(idx).to_f32().is_nan())
        }
        DataType::Float32 => {
            let vals = values.as_primitive::<Float32Type>();
            any_nan_scan(arr, n, dim, |idx| vals.value(idx).is_nan())
        }
        DataType::Float64 => {
            let vals = values.as_primitive::<Float64Type>();
            any_nan_scan(arr, n, dim, |idx| vals.value(idx).is_nan())
        }
        _ => false,
    }
}

/// Compute a per-row boolean mask indicating which rows contain NaN values.
///
/// Needed by Drop and Null strategies to know exactly which rows are affected.
fn nan_row_mask(arr: &FixedSizeListArray) -> BooleanArray {
    let values = arr.values();
    let n = arr.len();
    let dim = arr.value_length() as usize;

    match values.data_type() {
        DataType::Float16 => {
            let vals = values.as_primitive::<Float16Type>();
            nan_mask_scan(arr, n, dim, |idx| vals.value(idx).to_f32().is_nan())
        }
        DataType::Float32 => {
            let vals = values.as_primitive::<Float32Type>();
            nan_mask_scan(arr, n, dim, |idx| vals.value(idx).is_nan())
        }
        DataType::Float64 => {
            let vals = values.as_primitive::<Float64Type>();
            nan_mask_scan(arr, n, dim, |idx| vals.value(idx).is_nan())
        }
        _ => BooleanArray::from(vec![false; n]),
    }
}

/// Scan rows for any NaN, returning true on first hit.
fn any_nan_scan(
    arr: &FixedSizeListArray,
    n: usize,
    dim: usize,
    is_nan: impl Fn(usize) -> bool,
) -> bool {
    for row in 0..n {
        if arr.is_null(row) {
            continue;
        }
        let offset = row * dim;
        for j in 0..dim {
            if is_nan(offset + j) {
                return true;
            }
        }
    }
    false
}

/// Build a per-row NaN mask, with early break within each row.
fn nan_mask_scan(
    arr: &FixedSizeListArray,
    n: usize,
    dim: usize,
    is_nan: impl Fn(usize) -> bool,
) -> BooleanArray {
    let mut result = Vec::with_capacity(n);
    for row in 0..n {
        if arr.is_null(row) {
            result.push(false);
            continue;
        }
        let offset = row * dim;
        let mut row_has_nan = false;
        for j in 0..dim {
            if is_nan(offset + j) {
                row_has_nan = true;
                break;
            }
        }
        result.push(row_has_nan);
    }
    BooleanArray::from(result)
}

// ---------------------------------------------------------------------------
// List / LargeList NaN detection and remediation
// ---------------------------------------------------------------------------

/// Check whether any non-null row in a List/LargeList contains a NaN value.
fn any_nan_in_list<O: OffsetSizeTrait>(arr: &dyn Array) -> bool {
    let list = arr.as_list::<O>();
    let values = list.values();
    let offsets = list.offsets();

    match values.data_type() {
        DataType::Float16 => {
            let vals = values.as_primitive::<Float16Type>();
            any_nan_scan_list(list, offsets, |idx| vals.value(idx).to_f32().is_nan())
        }
        DataType::Float32 => {
            let vals = values.as_primitive::<Float32Type>();
            any_nan_scan_list(list, offsets, |idx| vals.value(idx).is_nan())
        }
        DataType::Float64 => {
            let vals = values.as_primitive::<Float64Type>();
            any_nan_scan_list(list, offsets, |idx| vals.value(idx).is_nan())
        }
        _ => false,
    }
}

fn any_nan_scan_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    offsets: &OffsetBuffer<O>,
    is_nan: impl Fn(usize) -> bool,
) -> bool {
    for row in 0..list.len() {
        if list.is_null(row) {
            continue;
        }
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        for idx in start..end {
            if is_nan(idx) {
                return true;
            }
        }
    }
    false
}

/// Compute a per-row boolean mask for NaN in List/LargeList columns.
fn nan_row_mask_list<O: OffsetSizeTrait>(arr: &dyn Array) -> BooleanArray {
    let list = arr.as_list::<O>();
    let values = list.values();
    let offsets = list.offsets();

    match values.data_type() {
        DataType::Float16 => {
            let vals = values.as_primitive::<Float16Type>();
            nan_mask_scan_list(list, offsets, |idx| vals.value(idx).to_f32().is_nan())
        }
        DataType::Float32 => {
            let vals = values.as_primitive::<Float32Type>();
            nan_mask_scan_list(list, offsets, |idx| vals.value(idx).is_nan())
        }
        DataType::Float64 => {
            let vals = values.as_primitive::<Float64Type>();
            nan_mask_scan_list(list, offsets, |idx| vals.value(idx).is_nan())
        }
        _ => BooleanArray::from(vec![false; list.len()]),
    }
}

fn nan_mask_scan_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    offsets: &OffsetBuffer<O>,
    is_nan: impl Fn(usize) -> bool,
) -> BooleanArray {
    let mut result = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            result.push(false);
            continue;
        }
        let start = offsets[row].as_usize();
        let end = offsets[row + 1].as_usize();
        let mut row_has_nan = false;
        for idx in start..end {
            if is_nan(idx) {
                row_has_nan = true;
                break;
            }
        }
        result.push(row_has_nan);
    }
    BooleanArray::from(result)
}

/// Replace NaN-containing rows with null for List/LargeList by updating the validity bitmap.
fn nullify_list_rows<O: OffsetSizeTrait>(
    arr: &dyn Array,
    is_bad: &BooleanArray,
) -> Result<ArrayRef> {
    let list = arr.as_list::<O>();
    let n = list.len();
    let mut builder = NullBufferBuilder::new(n);
    for row in 0..n {
        builder.append(!is_bad.value(row) && !list.is_null(row));
    }
    let nulls = builder.finish();
    let field = list.data_type().clone();
    // Extract the inner field from the List/LargeList data type
    let inner_field = match &field {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };
    let new_list = GenericListArray::<O>::new(
        inner_field,
        list.offsets().clone(),
        list.values().clone(),
        nulls,
    );
    Ok(Arc::new(new_list))
}

/// Replace NaN elements with the fill value in List/LargeList columns.
fn fill_list_rows<O: OffsetSizeTrait>(arr: &dyn Array, fill: f64) -> Result<ArrayRef> {
    let list = arr.as_list::<O>();
    let values = list.values();
    let fill_f32 = fill as f32;

    let new_values: ArrayRef = match values.data_type() {
        DataType::Float32 => {
            let prim = values.as_primitive::<Float32Type>().clone();
            Arc::new(replace_nan_with(prim, fill_f32))
        }
        DataType::Float64 => {
            let prim = values.as_primitive::<Float64Type>().clone();
            Arc::new(replace_nan_with(prim, fill))
        }
        DataType::Float16 => {
            let casted = cast(values, &DataType::Float32)?;
            let prim = casted.as_primitive::<Float32Type>().clone();
            Arc::new(replace_nan_with(prim, fill_f32))
        }
        _ => return Ok(Arc::new(list.clone())),
    };

    let inner_field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };
    // Update field data type if cast changed it (Float16 → Float32)
    let inner_field = Arc::new(Field::new(
        inner_field.name(),
        new_values.data_type().clone(),
        inner_field.is_nullable(),
    ));
    let new_list = GenericListArray::<O>::new(
        inner_field,
        list.offsets().clone(),
        new_values,
        list.nulls().cloned(),
    );
    Ok(Arc::new(new_list))
}

/// Replace NaN-containing rows with null by updating the validity bitmap.
fn nullify_fsl_rows(fsl: &FixedSizeListArray, is_bad: &BooleanArray) -> Result<ArrayRef> {
    let n = fsl.len();
    let mut builder = NullBufferBuilder::new(n);
    for row in 0..n {
        builder.append(!is_bad.value(row) && !fsl.is_null(row));
    }
    let nulls = builder.finish();
    let field = Arc::new(Field::new("item", fsl.value_type(), true));
    let new_fsl = FixedSizeListArray::new(field, fsl.value_length(), fsl.values().clone(), nulls);
    Ok(Arc::new(new_fsl))
}

/// Replace NaN elements in-place with the fill value.
fn fill_fsl_rows(fsl: &FixedSizeListArray, fill: f64) -> Result<ArrayRef> {
    let values = fsl.values();
    let fill_f32 = fill as f32;

    let new_values: ArrayRef = match values.data_type() {
        DataType::Float32 => {
            let arr: PrimitiveArray<Float32Type> = values.as_primitive::<Float32Type>().clone();
            Arc::new(replace_nan_with(arr, fill_f32))
        }
        DataType::Float64 => {
            let arr: PrimitiveArray<Float64Type> = values.as_primitive::<Float64Type>().clone();
            Arc::new(replace_nan_with(arr, fill))
        }
        DataType::Float16 => {
            let casted = cast(values, &DataType::Float32)?;
            let arr: PrimitiveArray<Float32Type> = casted.as_primitive::<Float32Type>().clone();
            Arc::new(replace_nan_with(arr, fill_f32))
        }
        _ => return Ok(Arc::new(fsl.clone())),
    };

    let field = Arc::new(Field::new("item", new_values.data_type().clone(), true));
    let new_fsl =
        FixedSizeListArray::new(field, fsl.value_length(), new_values, fsl.nulls().cloned());
    Ok(Arc::new(new_fsl))
}

fn replace_nan_with<T>(arr: PrimitiveArray<T>, fill: T::Native) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::Float,
{
    match arr.unary_mut(|v| if v.is_nan() { fill } else { v }) {
        Ok(result) => result,
        Err(arr) => arr.unary(|v| if v.is_nan() { fill } else { v }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::types::Float32Type;
    use arrow_array::Int32Array;
    use arrow_schema::Schema;
    use futures::TryStreamExt;

    use crate::arrow::SimpleRecordBatchStream;

    fn make_stream(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Box::pin(SimpleRecordBatchStream::new(stream, schema))
    }

    fn make_fsl_f32(values: &[Option<Vec<f32>>], dim: i32) -> ArrayRef {
        let dim_usize = dim as usize;
        let n = values.len();
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::with_capacity(n * dim_usize), dim);
        for row in values {
            match row {
                Some(vals) => {
                    for v in vals {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    for _ in 0..dim_usize {
                        builder.values().append_null();
                    }
                    builder.append(false);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn fsl_f32_schema(name: &str, dim: i32) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        )]))
    }

    #[tokio::test]
    async fn test_nan_error_strategy() {
        let schema = fsl_f32_schema("vec", 3);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_fsl_f32(
                &[Some(vec![1.0, 2.0, 3.0]), Some(vec![f32::NAN, 5.0, 6.0])],
                3,
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result = stream.try_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nan_drop_strategy() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                make_fsl_f32(
                    &[
                        Some(vec![1.0, 2.0, 3.0]),
                        Some(vec![f32::NAN, 5.0, 6.0]),
                        Some(vec![7.0, 8.0, 9.0]),
                    ],
                    3,
                ),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Drop);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let ids = result_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn test_nan_fill_strategy() {
        let schema = fsl_f32_schema("vec", 3);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_fsl_f32(
                &[Some(vec![1.0, 2.0, 3.0]), Some(vec![f32::NAN, 5.0, 6.0])],
                3,
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Fill(0.0));
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let fsl = result_batch.column(0).as_fixed_size_list();
        // Only the NaN element is replaced; non-NaN values are preserved.
        let row1 = fsl.value(1);
        let row1_f32 = row1.as_primitive::<Float32Type>();
        assert_eq!(row1_f32.value(0), 0.0); // was NaN, now filled
        assert_eq!(row1_f32.value(1), 5.0); // preserved
        assert_eq!(row1_f32.value(2), 6.0); // preserved
    }

    #[tokio::test]
    async fn test_nan_null_strategy() {
        let schema = fsl_f32_schema("vec", 3);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_fsl_f32(
                &[
                    Some(vec![1.0, 2.0, 3.0]),
                    Some(vec![f32::NAN, 5.0, 6.0]),
                    Some(vec![7.0, 8.0, 9.0]),
                ],
                3,
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Null);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        let fsl = result_batch.column(0).as_fixed_size_list();
        assert!(!fsl.is_null(0));
        assert!(fsl.is_null(1));
        assert!(!fsl.is_null(2));
    }

    #[tokio::test]
    async fn test_nan_clean_data_passthrough() {
        let schema = fsl_f32_schema("vec", 2);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_fsl_f32(
                &[Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])],
                2,
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_nan_detects_regardless_of_name() {
        // Column is named "data", not "vector" or "embedding"
        let schema = fsl_f32_schema("data", 2);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_fsl_f32(
                &[Some(vec![f32::NAN, 2.0]), Some(vec![3.0, 4.0])],
                2,
            )],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result = stream.try_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nan_ignores_non_float_fsl() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![{
                use arrow_array::builder::{FixedSizeListBuilder, Int32Builder};
                let mut builder = FixedSizeListBuilder::new(Int32Builder::with_capacity(4), 2);
                builder.values().append_value(1);
                builder.values().append_value(2);
                builder.append(true);
                builder.values().append_value(3);
                builder.values().append_value(4);
                builder.append(true);
                Arc::new(builder.finish()) as ArrayRef
            }],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        // Should pass through without error since Int32 can't have NaN
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_nan_multiple_fsl_columns_drop() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec1",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new(
                "vec2",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                make_fsl_f32(
                    &[
                        Some(vec![f32::NAN, 2.0]), // bad in vec1
                        Some(vec![3.0, 4.0]),      // ok
                        Some(vec![5.0, 6.0]),      // ok
                    ],
                    2,
                ),
                make_fsl_f32(
                    &[
                        Some(vec![7.0, 8.0]),      // ok
                        Some(vec![9.0, f32::NAN]), // bad in vec2
                        Some(vec![11.0, 12.0]),    // ok
                    ],
                    2,
                ),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Drop);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        // Row 0 bad in vec1, row 1 bad in vec2 → only row 2 survives
        assert_eq!(result_batch.num_rows(), 1);
        let ids = result_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ids.value(0), 3);
    }

    #[tokio::test]
    async fn test_nan_f16_detection() {
        use half::f16;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "vec",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float16, true)), 2),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![{
                use arrow_array::builder::{FixedSizeListBuilder, Float16Builder};
                let mut builder = FixedSizeListBuilder::new(Float16Builder::with_capacity(4), 2);
                builder.values().append_value(f16::from_f32(1.0));
                builder.values().append_value(f16::NAN);
                builder.append(true);
                builder.values().append_value(f16::from_f32(3.0));
                builder.values().append_value(f16::from_f32(4.0));
                builder.append(true);
                Arc::new(builder.finish()) as ArrayRef
            }],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Null);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let fsl = result_batch.column(0).as_fixed_size_list();
        assert!(fsl.is_null(0)); // row with NaN is nullified
        assert!(!fsl.is_null(1));
    }

    // -----------------------------------------------------------------------
    // List / LargeList tests
    // -----------------------------------------------------------------------

    /// Build a `List<Float32>` array from optional rows.
    fn make_list_f32(values: &[Option<Vec<f32>>]) -> ArrayRef {
        use arrow_array::builder::{Float32Builder, ListBuilder};
        let mut builder = ListBuilder::new(Float32Builder::new());
        for row in values {
            match row {
                Some(vals) => {
                    for v in vals {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        Arc::new(builder.finish())
    }

    /// Build a `List<Float64>` array from optional rows.
    fn make_list_f64(values: &[Option<Vec<f64>>]) -> ArrayRef {
        use arrow_array::builder::{Float64Builder, ListBuilder};
        let mut builder = ListBuilder::new(Float64Builder::new());
        for row in values {
            match row {
                Some(vals) => {
                    for v in vals {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        Arc::new(builder.finish())
    }

    /// Build a `LargeList<Float32>` array from optional rows.
    fn make_large_list_f32(values: &[Option<Vec<f32>>]) -> ArrayRef {
        use arrow_array::builder::{Float32Builder, LargeListBuilder};
        let mut builder = LargeListBuilder::new(Float32Builder::new());
        for row in values {
            match row {
                Some(vals) => {
                    for v in vals {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    builder.append(false);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn list_f32_schema(name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]))
    }

    #[tokio::test]
    async fn test_nan_error_list_f32() {
        let schema = list_f32_schema("vec");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_list_f32(&[
                Some(vec![1.0, 2.0, 3.0]),
                Some(vec![f32::NAN, 5.0, 6.0]),
            ])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result = stream.try_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nan_drop_list_f32() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                make_list_f32(&[
                    Some(vec![1.0, 2.0, 3.0]),
                    Some(vec![f32::NAN, 5.0, 6.0]),
                    Some(vec![7.0, 8.0, 9.0]),
                ]),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Drop);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let ids = result_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 3);
    }

    #[tokio::test]
    async fn test_nan_null_list_f32() {
        let schema = list_f32_schema("vec");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_list_f32(&[
                Some(vec![1.0, 2.0, 3.0]),
                Some(vec![f32::NAN, 5.0, 6.0]),
                Some(vec![7.0, 8.0, 9.0]),
            ])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Null);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 3);
        let col = result_batch.column(0);
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[tokio::test]
    async fn test_nan_fill_list_f32() {
        let schema = list_f32_schema("vec");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_list_f32(&[
                Some(vec![1.0, 2.0, 3.0]),
                Some(vec![f32::NAN, 5.0, 6.0]),
            ])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Fill(0.0));
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
        let list = result_batch.column(0).as_list::<i32>();
        let row1 = list.value(1);
        let row1_f32 = row1.as_primitive::<Float32Type>();
        assert_eq!(row1_f32.value(0), 0.0); // was NaN, now filled
        assert_eq!(row1_f32.value(1), 5.0);
        assert_eq!(row1_f32.value(2), 6.0);
    }

    #[tokio::test]
    async fn test_nan_list_f64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vec",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_list_f64(&[
                Some(vec![1.0, 2.0]),
                Some(vec![f64::NAN, 4.0]),
            ])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result = stream.try_next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nan_large_list_f32() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vec",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_large_list_f32(&[
                Some(vec![1.0, 2.0]),
                Some(vec![f32::NAN, 4.0]),
                Some(vec![5.0, 6.0]),
            ])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Drop);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_nan_list_clean_passthrough() {
        let schema = list_f32_schema("vec");
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![make_list_f32(&[Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])])],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Error);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        assert_eq!(result_batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_nan_mixed_fsl_and_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "fsl_vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new(
                "list_vec",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                make_fsl_f32(
                    &[
                        Some(vec![f32::NAN, 2.0]), // bad in fsl
                        Some(vec![3.0, 4.0]),      // ok
                        Some(vec![5.0, 6.0]),      // ok
                    ],
                    2,
                ),
                make_list_f32(&[
                    Some(vec![7.0, 8.0]),      // ok
                    Some(vec![9.0, f32::NAN]), // bad in list
                    Some(vec![11.0, 12.0]),    // ok
                ]),
            ],
        )
        .unwrap();

        let stream = make_stream(schema, vec![batch]);
        let mut stream = handle_nan_vectors(stream, &NanStrategy::Drop);
        let result_batch = stream.try_next().await.unwrap().unwrap();
        // Row 0 bad in fsl, row 1 bad in list → only row 2 survives
        assert_eq!(result_batch.num_rows(), 1);
        let ids = result_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(ids.value(0), 3);
    }
}
