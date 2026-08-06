// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Arrow-native evaluation for MemWAL sharding specs.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::as_primitive_array;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance_core::{Error, Result, datatypes::Schema as LanceSchema};
use lance_index::mem_wal::{ShardingField, ShardingSpec};

const BUCKET_TRANSFORM: &str = "bucket";
const IDENTITY_TRANSFORM: &str = "identity";
const UNSHARDED_TRANSFORM: &str = "unsharded";
const NUM_BUCKETS_PARAM: &str = "num_buckets";
const COLUMN_PARAM: &str = "column";
const MURMUR3_SEED: i32 = 0;

/// Evaluate a MemWAL sharding specification against an Arrow record batch.
///
/// The returned batch has one column per [`ShardingField`] in `spec`, with the
/// column names taken from `field_id`. Bucket sharding returns `Int32` bucket
/// IDs. Identity sharding returns the source column unchanged. Unsharded
/// sharding returns `Int32` zeros.
pub fn evaluate_sharding_spec(
    batch: &RecordBatch,
    spec: &ShardingSpec,
    schema: &LanceSchema,
) -> Result<RecordBatch> {
    let source_id_to_column = source_id_to_column_map(schema);
    evaluate_sharding_spec_with_source_columns(batch, spec, &source_id_to_column)
}

/// Evaluate a MemWAL sharding specification that embeds source column names.
///
/// Prefer [`evaluate_sharding_spec`] for table-bound evaluation. This helper is
/// for specs that carry a `column` parameter for each source-dependent field.
pub fn evaluate_sharding_spec_with_embedded_columns(
    batch: &RecordBatch,
    spec: &ShardingSpec,
) -> Result<RecordBatch> {
    evaluate_sharding_spec_with_source_columns(batch, spec, &HashMap::new())
}

/// Evaluate a MemWAL sharding specification with an explicit field-id mapping.
///
/// Prefer [`evaluate_sharding_spec`] for table-bound evaluation. This helper is
/// intended for binding layers that have already derived the mapping from a
/// table schema.
pub fn evaluate_sharding_spec_with_source_columns(
    batch: &RecordBatch,
    spec: &ShardingSpec,
    source_id_to_column: &HashMap<i32, String>,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(spec.fields.len());
    let mut columns = Vec::with_capacity(spec.fields.len());

    for field in &spec.fields {
        let column = evaluate_sharding_field(batch, field, source_id_to_column)?;
        fields.push(Field::new(
            field.field_id.clone(),
            column.data_type().clone(),
            column.is_nullable(),
        ));
        columns.push(column);
    }

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

fn source_id_to_column_map(schema: &LanceSchema) -> HashMap<i32, String> {
    schema
        .fields_pre_order()
        .map(|field| {
            let column = schema
                .field_ancestry_by_id(field.id)
                .map(|path| {
                    path.iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>()
                        .join(".")
                })
                .unwrap_or_else(|| field.name.clone());
            (field.id, column)
        })
        .collect()
}

fn evaluate_sharding_field(
    batch: &RecordBatch,
    field: &ShardingField,
    source_id_to_column: &HashMap<i32, String>,
) -> Result<ArrayRef> {
    match field.transform.as_deref() {
        Some(BUCKET_TRANSFORM) => evaluate_bucket_sharding(batch, field, source_id_to_column),
        Some(IDENTITY_TRANSFORM) => evaluate_identity_sharding(batch, field, source_id_to_column),
        Some(UNSHARDED_TRANSFORM) => Ok(Arc::new(Int32Array::from(vec![0; batch.num_rows()]))),
        other => Err(Error::invalid_input(format!(
            "Unsupported MemWAL sharding transform for field '{}': {:?}",
            field.field_id, other
        ))),
    }
}

fn evaluate_identity_sharding(
    batch: &RecordBatch,
    field: &ShardingField,
    source_id_to_column: &HashMap<i32, String>,
) -> Result<ArrayRef> {
    let column_name = source_column_name(field, source_id_to_column)?;
    Ok(batch
        .column_by_name(&column_name)
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "Sharding source column '{}' not found in batch",
                column_name
            ))
        })?
        .clone())
}

fn evaluate_bucket_sharding(
    batch: &RecordBatch,
    field: &ShardingField,
    source_id_to_column: &HashMap<i32, String>,
) -> Result<ArrayRef> {
    let column_name = source_column_name(field, source_id_to_column)?;
    let num_buckets = field
        .parameters
        .get(NUM_BUCKETS_PARAM)
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "Bucket sharding field '{}' missing '{}' parameter",
                field.field_id, NUM_BUCKETS_PARAM
            ))
        })?
        .parse::<i32>()
        .map_err(|e| {
            Error::invalid_input(format!(
                "Bucket sharding field '{}' has invalid num_buckets '{}': {}",
                field.field_id, field.parameters[NUM_BUCKETS_PARAM], e
            ))
        })?;
    if num_buckets <= 0 {
        return Err(Error::invalid_input(format!(
            "Bucket sharding field '{}' requires positive num_buckets, got {}",
            field.field_id, num_buckets
        )));
    }

    let column = batch.column_by_name(&column_name).ok_or_else(|| {
        Error::invalid_input(format!(
            "Sharding source column '{}' not found in batch",
            column_name
        ))
    })?;
    let mut bucket_ids = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let hash = hash_array_value(column.as_ref(), row_idx, MURMUR3_SEED)?;
        bucket_ids.push((hash & i32::MAX) % num_buckets);
    }
    Ok(Arc::new(Int32Array::from(bucket_ids)))
}

fn source_column_name(
    field: &ShardingField,
    source_id_to_column: &HashMap<i32, String>,
) -> Result<String> {
    if let Some(column) = field.parameters.get(COLUMN_PARAM)
        && !column.trim().is_empty()
    {
        return Ok(column.clone());
    }
    let Some(source_id) = field.source_ids.first() else {
        return Err(Error::invalid_input(format!(
            "MemWAL sharding field '{}' has no source column",
            field.field_id
        )));
    };
    source_id_to_column.get(source_id).cloned().ok_or_else(|| {
        Error::invalid_input(format!(
            "MemWAL sharding field '{}' source id {} was not mapped to a batch column",
            field.field_id, source_id
        ))
    })
}

fn hash_array_value(array: &dyn Array, row_idx: usize, seed: i32) -> Result<i32> {
    if array.is_null(row_idx) {
        return Ok(seed);
    }
    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(hash_int(if array.value(row_idx) { 1 } else { 0 }, seed))
        }
        DataType::Int8 => hash_primitive_int::<Int8Type, _>(array, row_idx, seed, |v| v as i32),
        DataType::Int16 => hash_primitive_int::<Int16Type, _>(array, row_idx, seed, |v| v as i32),
        DataType::Int32 => hash_primitive_int::<Int32Type, _>(array, row_idx, seed, |v| v),
        DataType::Date32 => hash_primitive_int::<Date32Type, _>(array, row_idx, seed, |v| v),
        DataType::Int64 => hash_primitive::<Int64Type, _>(array, row_idx, seed, |v| v),
        DataType::UInt8 => hash_primitive_int::<UInt8Type, _>(array, row_idx, seed, |v| v as i32),
        DataType::UInt16 => hash_primitive_int::<UInt16Type, _>(array, row_idx, seed, |v| v as i32),
        DataType::UInt32 => hash_primitive_int::<UInt32Type, _>(array, row_idx, seed, |v| v as i32),
        DataType::UInt64 => hash_primitive::<UInt64Type, _>(array, row_idx, seed, |v| v as i64),
        DataType::Float32 => hash_primitive_int::<Float32Type, _>(array, row_idx, seed, |v| {
            canonical_f32_bits(v) as i32
        }),
        DataType::Float64 => {
            hash_primitive::<Float64Type, _>(array, row_idx, seed, |v| canonical_f64_bits(v) as i64)
        }
        DataType::Timestamp(_, _) => hash_timestamp(array, row_idx, seed),
        DataType::Time32(_) => hash_time32(array, row_idx, seed),
        DataType::Time64(_) => hash_time64(array, row_idx, seed),
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(hash_bytes(array.value(row_idx).as_bytes(), seed))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(hash_bytes(array.value(row_idx).as_bytes(), seed))
        }
        other => Err(Error::invalid_input(format!(
            "Unsupported bucket sharding column type: {:?}",
            other
        ))),
    }
}

fn hash_primitive_int<T, F>(array: &dyn Array, row_idx: usize, seed: i32, convert: F) -> Result<i32>
where
    T: arrow_array::types::ArrowPrimitiveType,
    F: Fn(T::Native) -> i32,
{
    let array = as_primitive_array::<T>(array);
    Ok(hash_int(convert(array.value(row_idx)), seed))
}

fn hash_primitive<T, F>(array: &dyn Array, row_idx: usize, seed: i32, convert: F) -> Result<i32>
where
    T: arrow_array::types::ArrowPrimitiveType,
    F: Fn(T::Native) -> i64,
{
    let array = as_primitive_array::<T>(array);
    Ok(hash_long(convert(array.value(row_idx)), seed))
}

fn hash_timestamp(array: &dyn Array, row_idx: usize, seed: i32) -> Result<i32> {
    match array.data_type() {
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            hash_primitive::<TimestampSecondType, _>(array, row_idx, seed, |v| v)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            hash_primitive::<TimestampMillisecondType, _>(array, row_idx, seed, |v| v)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            hash_primitive::<TimestampMicrosecondType, _>(array, row_idx, seed, |v| v)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            hash_primitive::<TimestampNanosecondType, _>(array, row_idx, seed, |v| v)
        }
        _ => unreachable!(),
    }
}

fn hash_time32(array: &dyn Array, row_idx: usize, seed: i32) -> Result<i32> {
    match array.data_type() {
        DataType::Time32(arrow_schema::TimeUnit::Second) => {
            hash_primitive_int::<Time32SecondType, _>(array, row_idx, seed, |v| v)
        }
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            hash_primitive_int::<Time32MillisecondType, _>(array, row_idx, seed, |v| v)
        }
        _ => unreachable!(),
    }
}

fn hash_time64(array: &dyn Array, row_idx: usize, seed: i32) -> Result<i32> {
    match array.data_type() {
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            hash_primitive::<Time64MicrosecondType, _>(array, row_idx, seed, |v| v)
        }
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            hash_primitive::<Time64NanosecondType, _>(array, row_idx, seed, |v| v)
        }
        _ => unreachable!(),
    }
}

fn canonical_f32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7fc0_0000
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        value.to_bits()
    }
}

fn hash_int(value: i32, seed: i32) -> i32 {
    fmix(mix_h1(seed, mix_k1(value)), 4)
}

fn hash_long(value: i64, seed: i32) -> i32 {
    let low = value as i32;
    let high = (value >> 32) as i32;
    let h1 = mix_h1(seed, mix_k1(low));
    let h1 = mix_h1(h1, mix_k1(high));
    fmix(h1, 8)
}

fn hash_bytes(bytes: &[u8], seed: i32) -> i32 {
    let mut h1 = seed;
    let remainder = bytes.len() % 4;
    let full_chunks_len = bytes.len() - remainder;
    for chunk in bytes[..full_chunks_len].chunks_exact(4) {
        let k1 = i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        h1 = mix_h1(h1, mix_k1(k1));
    }
    for byte in &bytes[full_chunks_len..] {
        h1 = mix_h1(h1, mix_k1((*byte as i8) as i32));
    }

    fmix(h1, bytes.len() as i32)
}

fn mix_k1(k1: i32) -> i32 {
    let k1 = k1.wrapping_mul(0xcc9e_2d51u32 as i32);
    k1.rotate_left(15).wrapping_mul(0x1b87_3593)
}

fn mix_h1(h1: i32, k1: i32) -> i32 {
    let h1 = h1 ^ k1;
    h1.rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe654_6b64u32 as i32)
}

fn fmix(h1: i32, length: i32) -> i32 {
    let mut h1 = h1 ^ length;
    h1 ^= (h1 as u32 >> 16) as i32;
    h1 = h1.wrapping_mul(0x85eb_ca6bu32 as i32);
    h1 ^= (h1 as u32 >> 13) as i32;
    h1 = h1.wrapping_mul(0xc2b2_ae35u32 as i32);
    h1 ^ ((h1 as u32 >> 16) as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, StringArray,
    };
    use lance_core::datatypes::Schema as LanceSchema;
    use lance_index::mem_wal::ShardingField;

    fn single_field_spec(field: ShardingField) -> ShardingSpec {
        ShardingSpec {
            spec_id: 1,
            fields: vec![field],
        }
    }

    fn bucket_field(num_buckets: i32) -> ShardingField {
        ShardingField {
            field_id: "bucket".to_string(),
            source_ids: vec![0],
            transform: Some(BUCKET_TRANSFORM.to_string()),
            expression: None,
            result_type: "int32".to_string(),
            parameters: HashMap::from([(NUM_BUCKETS_PARAM.to_string(), num_buckets.to_string())]),
        }
    }

    fn lance_schema(batch: &RecordBatch) -> LanceSchema {
        LanceSchema::try_from(batch.schema().as_ref()).unwrap()
    }

    fn bucket_field_for_source(source_id: i32, num_buckets: i32) -> ShardingField {
        ShardingField {
            source_ids: vec![source_id],
            ..bucket_field(num_buckets)
        }
    }

    #[test]
    fn test_evaluate_bucket_sharding_int32() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef,
        )])
        .unwrap();
        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field(8)),
            &lance_schema(&batch),
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[2, 7, 0, 1]);
    }

    #[test]
    fn test_evaluate_bucket_sharding_date32() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Date32Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef,
        )])
        .unwrap();
        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field(8)),
            &lance_schema(&batch),
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[2, 7, 0, 1]);
    }

    #[test]
    fn test_evaluate_bucket_sharding_string() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        )])
        .unwrap();
        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field(8)),
            &lance_schema(&batch),
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[1, 5, 0]);
    }

    #[test]
    fn test_evaluate_bucket_sharding_scalar_types() {
        let batch = RecordBatch::try_from_iter([
            ("bool", Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
            (
                "f32",
                Arc::new(Float32Array::from(vec![1.25_f32])) as ArrayRef,
            ),
            (
                "f64",
                Arc::new(Float64Array::from(vec![1.25_f64])) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = lance_schema(&batch);
        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field_for_source(0, 8)),
            &schema,
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[2]);

        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field_for_source(1, 8)),
            &schema,
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[0]);

        let result = evaluate_sharding_spec(
            &batch,
            &single_field_spec(bucket_field_for_source(2, 8)),
            &schema,
        )
        .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[0]);
    }

    #[test]
    fn test_evaluate_identity_sharding() {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef,
        )])
        .unwrap();
        let spec = single_field_spec(ShardingField {
            field_id: "identity".to_string(),
            source_ids: vec![0],
            transform: Some(IDENTITY_TRANSFORM.to_string()),
            expression: None,
            result_type: "utf8".to_string(),
            parameters: HashMap::new(),
        });
        let result = evaluate_sharding_spec(&batch, &spec, &lance_schema(&batch)).unwrap();
        assert_eq!(result.column(0).as_ref(), batch.column(0).as_ref());
    }

    #[test]
    fn test_evaluate_bucket_sharding_embedded_column() {
        let batch = RecordBatch::try_from_iter([(
            "key",
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
        )])
        .unwrap();
        let mut field = bucket_field(8);
        field.source_ids = Vec::new();
        field
            .parameters
            .insert(COLUMN_PARAM.to_string(), "key".to_string());
        let result =
            evaluate_sharding_spec_with_embedded_columns(&batch, &single_field_spec(field))
                .unwrap();
        let buckets = as_primitive_array::<Int32Type>(result.column(0).as_ref());
        assert_eq!(buckets.values(), &[1, 5, 0]);
    }
}
