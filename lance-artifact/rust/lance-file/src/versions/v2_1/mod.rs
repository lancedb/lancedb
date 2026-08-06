// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance v2.1 file composition.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use lance_core::{
    Error, Result,
    datatypes::{Field, Schema},
};
use lance_encoding::{
    compression_config::CompressionParams,
    encoder::{
        ColumnIndexSequence, EncodedBatch, FieldEncoder, FieldEncodingContext,
        FieldEncodingStrategy,
        structural::{
            PrimitiveFieldEncoding, PrimitivePageEncoding, try_create_binary_blob, try_create_list,
            try_create_struct,
        },
    },
};
use lance_io::traits::Writer as ObjectWriter;

use crate::{
    reader::{ReadProjection, structural},
    writer::FileWriterOptions,
};

mod compression;
mod reader;
mod writer;

pub use reader::{
    projection_from_column_names, projection_from_field_ids, projection_from_whole_schema,
};
pub use writer::Writer;

#[cfg(test)]
pub(crate) use reader::test_projection_length;
pub(crate) use reader::{
    decode_column_metadata, finish_metadata, finish_metadata_index, validate_global_buffers,
};

pub(crate) fn read_projection() -> Arc<dyn ReadProjection> {
    structural::read_projection(reader::decode_column)
}

/// Count physical columns represented by a field in a v2.1 footer.
pub fn physical_column_count(field: &Field) -> usize {
    structural::physical_column_count(field)
}

/// Build persisted field-to-column entries for a v2.1 data file.
pub fn data_file_columns(schema: &Schema) -> (Vec<i32>, Vec<i32>) {
    structural::data_file_columns(schema)
}

pub(super) fn field_id_to_column_index(schema: &Schema) -> BTreeMap<u32, u32> {
    structural::field_id_to_column_index(schema)
}

#[derive(Debug)]
struct FieldStrategy {
    primitive: PrimitiveFieldEncoding,
}

impl FieldEncodingStrategy for FieldStrategy {
    fn create_field_encoder(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        if let Some(encoder) =
            try_create_binary_blob(&self.primitive, field, column_index, context)?
        {
            return Ok(encoder);
        }
        if field.is_blob() {
            return Err(Error::invalid_input_source(
                format!(
                    "Blob encoding is not available for field '{}' with data type {}",
                    field.name,
                    field.data_type()
                )
                .into(),
            ));
        }
        if let Some(encoder) = self.primitive.try_create(field, column_index, context)? {
            return Ok(encoder);
        }
        if matches!(
            field.data_type(),
            arrow_schema::DataType::FixedSizeList(item, _)
                if matches!(item.data_type(), arrow_schema::DataType::Struct(_))
        ) {
            return Err(Error::not_supported_source(
                "FixedSizeList<Struct> is not enabled by the selected file format".into(),
            ));
        }
        if matches!(field.data_type(), arrow_schema::DataType::Map(_, _)) {
            return Err(Error::not_supported_source(
                "Map data type is not enabled by the selected file format".into(),
            ));
        }
        if let Some(encoder) = try_create_list(field, column_index, context)? {
            return Ok(encoder);
        }
        if let Some(encoder) = try_create_struct(field, column_index, context)? {
            return Ok(encoder);
        }
        Err(Error::not_supported_source(
            format!(
                "Lance v2.1 has no field encoding for '{}' with data type {}",
                field.name,
                field.data_type()
            )
            .into(),
        ))
    }
}

/// Compose the v2.1 field encoding mechanisms.
pub fn encoding_strategy(params: CompressionParams) -> Arc<dyn FieldEncodingStrategy> {
    let compression = Arc::new(compression::Strategy::new(params));
    Arc::new(FieldStrategy {
        primitive: PrimitiveFieldEncoding::new([
            PrimitivePageEncoding::reject_sparse(),
            PrimitivePageEncoding::dense_u16(compression),
        ]),
    })
}

/// Create a v2.1 writer with an explicit schema.
pub fn create_writer(
    object_writer: Box<dyn ObjectWriter>,
    schema: Schema,
    options: FileWriterOptions,
) -> Result<Writer> {
    Writer::try_new(object_writer, schema, options)
}

/// Create a v2.1 writer with explicit compression tuning.
pub fn create_writer_with_compression(
    object_writer: Box<dyn ObjectWriter>,
    schema: Schema,
    options: FileWriterOptions,
    compression: CompressionParams,
) -> Result<Writer> {
    Writer::try_new_with_compression(object_writer, schema, options, compression)
}

/// Create a v2.1 writer whose schema is inferred from the first batch.
pub fn create_lazy_writer(
    object_writer: Box<dyn ObjectWriter>,
    options: FileWriterOptions,
) -> Writer {
    Writer::new_lazy(object_writer, options)
}

/// Create a lazy v2.1 writer with explicit compression tuning.
pub fn create_lazy_writer_with_compression(
    object_writer: Box<dyn ObjectWriter>,
    options: FileWriterOptions,
    compression: CompressionParams,
) -> Writer {
    Writer::new_lazy_with_compression(object_writer, options, compression)
}

/// Encode a self-described v2.1 batch.
pub fn encode_self_described_batch(batch: &EncodedBatch) -> Result<Bytes> {
    writer::concat_lance_footer(batch, true)
}

/// Encode a mini-lance v2.1 batch.
pub fn encode_mini_batch(batch: &EncodedBatch) -> Result<Bytes> {
    writer::concat_lance_footer(batch, false)
}
