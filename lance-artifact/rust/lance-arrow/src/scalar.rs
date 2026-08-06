// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_array::{ArrayRef, make_array};
use arrow_buffer::Buffer;
use arrow_data::{ArrayDataBuilder, transform::MutableArrayData};
use arrow_schema::{ArrowError, DataType};

use crate::DataTypeExt;

type Result<T> = std::result::Result<T, ArrowError>;

pub const INLINE_VALUE_MAX_BYTES: usize = 32;

pub fn extract_scalar_value(array: &ArrayRef, idx: usize) -> Result<ArrayRef> {
    if idx >= array.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Scalar index out of bounds".to_string(),
        ));
    }

    let data = array.to_data();
    let mut mutable = MutableArrayData::new(vec![&data], /*use_nulls=*/ true, 1);
    mutable.extend(0, idx, idx + 1);
    Ok(make_array(mutable.freeze()))
}

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > buf.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Invalid scalar value buffer: unexpected EOF".to_string(),
        ));
    }
    let bytes = [
        buf[*offset],
        buf[*offset + 1],
        buf[*offset + 2],
        buf[*offset + 3],
    ];
    *offset += 4;
    Ok(u32::from_le_bytes(bytes))
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    if *offset + len > buf.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Invalid scalar value buffer: unexpected EOF".to_string(),
        ));
    }
    let slice = &buf[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes);
}

pub fn encode_scalar_value_buffer(scalar: &ArrayRef) -> Result<Vec<u8>> {
    if scalar.len() != 1 || scalar.null_count() != 0 {
        return Err(ArrowError::InvalidArgumentError(
            "Scalar value buffer must be a single non-null value".to_string(),
        ));
    }
    let data = scalar.to_data();
    if data.offset() != 0 {
        return Err(ArrowError::InvalidArgumentError(
            "Scalar value buffer must have offset=0".to_string(),
        ));
    }
    if !data.child_data().is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Scalar value buffer does not support nested types".to_string(),
        ));
    }

    // Minimal format (RFC): store the Arrow value buffers for a length-1 array.
    // Null bitmap and child data are intentionally not supported here.
    //
    // | u32 num_buffers |
    // | u32 buffer_0_len | ... | u32 buffer_{n-1}_len |
    // | buffer_0 bytes | ... | buffer_{n-1} bytes |
    let mut out = Vec::with_capacity(128);
    let buffers = data.buffers();
    write_u32(&mut out, buffers.len() as u32);
    for b in buffers {
        write_u32(&mut out, b.len() as u32);
    }
    for b in buffers {
        write_bytes(&mut out, b.as_slice());
    }
    Ok(out)
}

pub fn decode_scalar_from_value_buffer(
    data_type: &DataType,
    value_buffer: &[u8],
) -> Result<ArrayRef> {
    if matches!(
        data_type,
        DataType::Struct(_) | DataType::FixedSizeList(_, _)
    ) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Scalar value buffer does not support nested data type {:?}",
            data_type
        )));
    }

    let mut offset = 0;
    let num_buffers = read_u32(value_buffer, &mut offset)? as usize;
    let buffer_lens = (0..num_buffers)
        .map(|_| read_u32(value_buffer, &mut offset).map(|l| l as usize))
        .collect::<Result<Vec<_>>>()?;

    let mut buffers = Vec::with_capacity(num_buffers);
    for len in buffer_lens {
        let bytes = read_bytes(value_buffer, &mut offset, len)?;
        buffers.push(Buffer::from_vec(bytes.to_vec()));
    }

    if offset != value_buffer.len() {
        return Err(ArrowError::InvalidArgumentError(
            "Invalid scalar value buffer: trailing bytes".to_string(),
        ));
    }

    let mut builder = ArrayDataBuilder::new(data_type.clone())
        .len(1)
        .null_count(0);
    for b in buffers {
        builder = builder.add_buffer(b);
    }
    Ok(make_array(builder.build()?))
}

pub fn decode_scalar_from_inline_value(
    data_type: &DataType,
    inline_value: &[u8],
) -> Result<ArrayRef> {
    // I expect our input to be safe here, but I added some debug_assert_eq statements just in case.
    // If they are triggered, we may need to change them to return actual errors.
    //
    // Boolean values are bit-packed in Arrow and therefore are not "fixed-stride" in bytes.
    // As a result, `byte_width_opt()` returns `None` for `DataType::Boolean`, even though a
    // length-1 scalar can be represented inline using a single byte (matching `try_inline_value`).
    if matches!(data_type, DataType::Boolean) {
        debug_assert_eq!(
            inline_value.len(),
            1,
            "Invalid boolean inline scalar length (expected 1 byte, got {})",
            inline_value.len()
        );
    } else if let Some(byte_width) = data_type.byte_width_opt() {
        debug_assert_eq!(
            inline_value.len(),
            byte_width,
            "Inline constant length mismatch for {:?}: expected {} bytes but got {}",
            data_type,
            byte_width,
            inline_value.len()
        );
    }

    let data = ArrayDataBuilder::new(data_type.clone())
        .len(1)
        .null_count(0)
        .add_buffer(Buffer::from_vec(inline_value.to_vec()))
        .build()?;
    Ok(make_array(data))
}

pub fn try_inline_value(scalar: &ArrayRef) -> Option<Vec<u8>> {
    if scalar.null_count() != 0 || scalar.len() != 1 {
        return None;
    }
    let data = scalar.to_data();
    if !data.child_data().is_empty() {
        return None;
    }
    if data.buffers().len() != 1 {
        return None;
    }
    let bytes = data.buffers()[0].as_slice();
    if bytes.len() > INLINE_VALUE_MAX_BYTES {
        return None;
    }
    Some(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{BooleanArray, FixedSizeBinaryArray, Int32Array, StringArray, cast::AsArray};

    use super::*;

    #[test]
    fn test_extract_scalar_value() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let scalar = extract_scalar_value(&array, 2).unwrap();
        assert_eq!(scalar.len(), 1);
        assert_eq!(
            scalar
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(0),
            3
        );
    }

    #[test]
    fn test_scalar_value_buffer_utf8_round_trip() {
        let scalar: ArrayRef = Arc::new(StringArray::from(vec!["hello"]));
        let buf = encode_scalar_value_buffer(&scalar).unwrap();
        let decoded = decode_scalar_from_value_buffer(&DataType::Utf8, &buf).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.null_count(), 0);
        assert_eq!(decoded.as_string::<i32>().value(0), "hello");
    }

    #[test]
    fn test_scalar_value_buffer_fixed_size_binary_round_trip() {
        let val = vec![0xABu8; 33];
        let scalar: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                std::iter::once(Some(val.as_slice())),
                33,
            )
            .unwrap(),
        );
        let buf = encode_scalar_value_buffer(&scalar).unwrap();
        let decoded =
            decode_scalar_from_value_buffer(&DataType::FixedSizeBinary(33), &buf).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.as_fixed_size_binary().value(0), val.as_slice());
    }

    #[test]
    fn test_inline_value_boolean_round_trip() {
        let scalar: ArrayRef = Arc::new(BooleanArray::from_iter([Some(true)]));
        let inline = try_inline_value(&scalar).unwrap();
        let decoded = decode_scalar_from_inline_value(&DataType::Boolean, &inline).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.null_count(), 0);
        assert!(decoded.as_boolean().value(0));
    }

    #[test]
    fn test_scalar_value_buffer_rejects_nested_type() {
        let field = Arc::new(arrow_schema::Field::new("item", DataType::Int32, false));
        let list: ArrayRef = Arc::new(arrow_array::FixedSizeListArray::new(
            field,
            2,
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        ));
        let scalar = list.slice(0, 1);
        assert!(encode_scalar_value_buffer(&scalar).is_err());
    }

    #[test]
    fn test_decode_scalar_from_value_buffer_rejects_nested_type() {
        let buf = Vec::<u8>::new();
        let res =
            decode_scalar_from_value_buffer(&DataType::Struct(arrow_schema::Fields::empty()), &buf);
        assert!(res.is_err());
    }

    #[test]
    fn test_decode_scalar_from_value_buffer_trailing_bytes() {
        // num_buffers = 0, plus an extra byte
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.push(1);
        let res = decode_scalar_from_value_buffer(&DataType::Int32, &bytes);
        assert!(res.is_err());
    }
}
