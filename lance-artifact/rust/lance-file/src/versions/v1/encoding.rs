// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance v1 column encoding grammar.
//!
//! These codecs own the v1 wire layout, including schema dictionary payloads.
//! They are intentionally not exposed through the version-free I/O layer.

use arrow_array::{
    Array, ArrayRef,
    types::{BinaryType, LargeBinaryType, LargeUtf8Type, Utf8Type},
};
use arrow_schema::DataType;
use async_recursion::async_recursion;

pub mod binary;
pub mod dictionary;
pub mod plain;

use lance_arrow::DataTypeExt;
use lance_core::{
    Error, Result,
    datatypes::{Field, Schema},
};
use lance_io::{
    ReadBatchParams,
    traits::{Reader, Writer},
};

use self::{
    binary::{BinaryDecoder, BinaryEncoder},
    plain::{PlainDecoder, PlainEncoder},
};

/// Decode a binary-like array from a v1 values region.
pub async fn read_binary_array(
    reader: &dyn Reader,
    data_type: &DataType,
    nullable: bool,
    position: usize,
    length: usize,
    params: impl Into<ReadBatchParams>,
) -> Result<ArrayRef> {
    use arrow_schema::DataType::*;

    let params = params.into();
    match data_type {
        Utf8 => {
            BinaryDecoder::<Utf8Type>::new(reader, position, length, nullable)
                .get(params)
                .await
        }
        Binary => {
            BinaryDecoder::<BinaryType>::new(reader, position, length, nullable)
                .get(params)
                .await
        }
        LargeUtf8 => {
            BinaryDecoder::<LargeUtf8Type>::new(reader, position, length, nullable)
                .get(params)
                .await
        }
        LargeBinary => {
            BinaryDecoder::<LargeBinaryType>::new(reader, position, length, nullable)
                .get(params)
                .await
        }
        _ => Err(lance_core::Error::invalid_input(format!(
            "unsupported v1 binary data type: {data_type}"
        ))),
    }
}

/// Decode a fixed-stride array from a v1 values region.
pub async fn read_fixed_stride_array(
    reader: &dyn Reader,
    data_type: &DataType,
    position: usize,
    length: usize,
    params: impl Into<ReadBatchParams>,
) -> Result<ArrayRef> {
    if !lance_arrow::DataTypeExt::is_fixed_stride(data_type) {
        return Err(lance_core::Error::schema(format!(
            "{data_type} is not a fixed stride type"
        )));
    }
    PlainDecoder::new(reader, data_type, position, length)?
        .get(params.into())
        .await
}

/// Persist every schema dictionary using the v1 value codecs.
pub async fn write_schema_dictionaries(writer: &mut dyn Writer, schema: &mut Schema) -> Result<()> {
    let max_field_id = schema.max_field_id().unwrap_or(-1);
    for field_id in 0..=max_field_id {
        let Some(field) = schema.mut_field_by_id(field_id) else {
            continue;
        };
        if !field.data_type().is_dictionary() {
            continue;
        }

        let dict_info = field.dictionary.as_mut().ok_or_else(|| {
            Error::io(format!(
                "v1 dictionary field '{}' is missing dictionary metadata",
                field.name
            ))
        })?;
        let values = dict_info.values.as_ref().ok_or_else(|| {
            Error::invalid_input(format!(
                "v1 dictionary field '{}' is missing dictionary values",
                field.name
            ))
        })?;

        let data_type = values.data_type();
        let position = if data_type.is_numeric() {
            PlainEncoder::new(writer, data_type)
                .encode(&[values])
                .await?
        } else if data_type.is_binary_like() {
            BinaryEncoder::new(writer).encode(&[values]).await?
        } else {
            return Err(Error::schema(format!(
                "v1 dictionary values do not support data type {data_type}"
            )));
        };
        dict_info.offset = position;
        dict_info.length = values.len();
    }
    Ok(())
}

#[async_recursion]
async fn populate_field_dictionary(field: &mut Field, reader: &dyn Reader) -> Result<()> {
    if let DataType::Dictionary(_, value_type) = field.data_type() {
        let dict_info = field.dictionary.as_mut().ok_or_else(|| {
            Error::io(format!(
                "v1 dictionary field '{}' is missing dictionary metadata",
                field.name
            ))
        })?;
        let values = if value_type.is_binary_like() {
            read_binary_array(
                reader,
                value_type.as_ref(),
                true,
                dict_info.offset,
                dict_info.length,
                ..,
            )
            .await?
        } else if matches!(
            value_type.as_ref(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) {
            read_fixed_stride_array(
                reader,
                value_type.as_ref(),
                dict_info.offset,
                dict_info.length,
                ..,
            )
            .await?
        } else {
            return Err(Error::schema(format!(
                "v1 dictionary values do not support data type {value_type}"
            )));
        };
        dict_info.values = Some(values);
    } else {
        for child in &mut field.children {
            populate_field_dictionary(child, reader).await?;
        }
    }
    Ok(())
}

/// Load every persisted v1 schema dictionary into its in-memory field.
pub async fn populate_schema_dictionaries(schema: &mut Schema, reader: &dyn Reader) -> Result<()> {
    for field in &mut schema.fields {
        populate_field_dictionary(field, reader).await?;
    }
    Ok(())
}
