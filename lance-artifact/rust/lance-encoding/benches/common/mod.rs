// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_schema::DataType;
use lance_core::{Error, Result, datatypes::Field};
use lance_encoding::{
    compression::{
        BlockCompressor, CompressionStrategy, field_metadata_params, finalize_miniblock_compressor,
        reject_packed_struct_per_value, try_bitpacking_block, try_bitpacking_miniblock,
        try_byte_stream_split_miniblock, try_fixed_packed_struct_miniblock, try_fixed_u8_rle_block,
        try_fixed_u8_rle_miniblock, try_general_block, try_raw_block,
        try_raw_fixed_size_list_miniblock, try_raw_fixed_width_miniblock, try_raw_per_value,
        try_uncompressed_fixed_width_miniblock, try_variable_packed_struct_per_value,
        try_variable_width_miniblock, try_variable_width_per_value,
    },
    compression_config::{CompressionFieldParams, CompressionParams},
    data::DataBlock,
    encoder::{
        ColumnIndexSequence, FieldEncoder, FieldEncodingContext, FieldEncodingStrategy,
        structural::{
            PrimitiveFieldEncoding, PrimitivePageEncoding, try_create_binary_blob, try_create_list,
            try_create_map, try_create_struct, try_create_structural_blob,
            try_create_structural_fixed_size_list,
        },
    },
    encodings::logical::primitive::{fullzip::PerValueCompressor, miniblock::MiniBlockCompressor},
    format::pb21::CompressiveEncoding,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchEncoding {
    Array,
    StructuralU16,
    StructuralU32,
}

impl std::fmt::Display for BenchEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Array => "array",
            Self::StructuralU16 => "structural-u16",
            Self::StructuralU32 => "structural-u32",
        })
    }
}

#[derive(Debug, Clone)]
struct BenchCompressionStrategy {
    encoding: BenchEncoding,
    params: CompressionParams,
}

impl BenchCompressionStrategy {
    fn field_params(&self, field: &Field) -> CompressionFieldParams {
        let mut params = self
            .params
            .get_field_params(&field.name, &field.data_type());
        let mut metadata = field_metadata_params(field);
        if self.encoding == BenchEncoding::StructuralU16
            && metadata
                .minichunk_size
                .is_some_and(|size| size >= 32 * 1024)
        {
            metadata.minichunk_size = None;
        }
        params.merge(&metadata);
        params
    }
}

impl CompressionStrategy for BenchCompressionStrategy {
    fn create_miniblock_compressor(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<Box<dyn MiniBlockCompressor>> {
        let params = self.field_params(field);
        let compressor =
            if let Some(compressor) = try_uncompressed_fixed_width_miniblock(data, &params) {
                compressor
            } else if let Some(compressor) = try_byte_stream_split_miniblock(data, &params) {
                compressor
            } else if let Some(compressor) = try_fixed_u8_rle_miniblock(data, &params) {
                compressor
            } else if let Some(compressor) = try_bitpacking_miniblock(data) {
                compressor
            } else if let Some(compressor) = try_raw_fixed_width_miniblock(data) {
                compressor
            } else if let Some(compressor) = try_variable_width_miniblock(field, data, &params)? {
                compressor
            } else if let Some(compressor) = try_fixed_packed_struct_miniblock(data)? {
                compressor
            } else if let Some(compressor) = try_raw_fixed_size_list_miniblock(data) {
                compressor
            } else {
                return Err(Error::not_supported_source(
                    format!(
                        "Mini-block compression not yet supported for block type {}",
                        data.name()
                    )
                    .into(),
                ));
            };
        finalize_miniblock_compressor(data, compressor, &params)
    }

    fn create_per_value(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<Box<dyn PerValueCompressor>> {
        let params = self.field_params(field);
        if let Some(compressor) = try_raw_per_value(data) {
            return Ok(compressor);
        }
        let packed = match self.encoding {
            BenchEncoding::StructuralU16 => reject_packed_struct_per_value(field, data)?,
            BenchEncoding::StructuralU32 => {
                try_variable_packed_struct_per_value(Arc::new(self.clone()), field, data)?
            }
            BenchEncoding::Array => unreachable!(),
        };
        if let Some(compressor) = packed {
            return Ok(compressor);
        }
        if let Some(compressor) = try_variable_width_per_value(field, data, &params)? {
            return Ok(compressor);
        }
        Err(Error::not_supported_source(
            format!(
                "Per-value compression not yet supported for block type {}",
                data.name()
            )
            .into(),
        ))
    }

    fn create_block_compressor(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<(Box<dyn BlockCompressor>, CompressiveEncoding)> {
        let params = self.field_params(field);
        if self.encoding == BenchEncoding::StructuralU32
            && let Some(compressor) = try_fixed_u8_rle_block(data, &params)?
        {
            return Ok(compressor);
        }
        if let Some(compressor) = try_bitpacking_block(data) {
            return Ok(compressor);
        }
        if self.encoding == BenchEncoding::StructuralU32
            && let Some(compressor) = try_general_block(data, &params)?
        {
            return Ok(compressor);
        }
        if let Some(compressor) = try_raw_block(data) {
            return Ok(compressor);
        }
        Err(Error::not_supported_source(
            format!(
                "Block compression not yet supported for block type {}",
                data.name()
            )
            .into(),
        ))
    }
}

#[derive(Debug)]
struct BenchFieldEncodingStrategy {
    encoding: BenchEncoding,
    primitive: PrimitiveFieldEncoding,
}

impl FieldEncodingStrategy for BenchFieldEncodingStrategy {
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
        if self.encoding == BenchEncoding::StructuralU32
            && let Some(encoder) =
                try_create_structural_blob(&self.primitive, field, column_index, context)?
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
        if self.encoding == BenchEncoding::StructuralU32 {
            if let Some(encoder) = try_create_map(field, column_index, context)? {
                return Ok(encoder);
            }
            if let Some(encoder) =
                try_create_structural_fixed_size_list(field, column_index, context)?
            {
                return Ok(encoder);
            }
        }
        if let Some(encoder) = self.primitive.try_create(field, column_index, context)? {
            return Ok(encoder);
        }
        if self.encoding == BenchEncoding::StructuralU16 {
            if matches!(
                field.data_type(),
                DataType::FixedSizeList(item, _)
                    if matches!(item.data_type(), DataType::Struct(_))
            ) {
                return Err(Error::not_supported_source(
                    "FixedSizeList<Struct> is not enabled by the selected file format".into(),
                ));
            }
            if matches!(field.data_type(), DataType::Map(_, _)) {
                return Err(Error::not_supported_source(
                    "Map data type is not enabled by the selected file format".into(),
                ));
            }
        }
        if let Some(encoder) = try_create_list(field, column_index, context)? {
            return Ok(encoder);
        }
        if let Some(encoder) = try_create_struct(field, column_index, context)? {
            return Ok(encoder);
        }
        Err(Error::not_supported_source(
            format!(
                "{} has no field encoding for '{}' with data type {}",
                self.encoding,
                field.name,
                field.data_type()
            )
            .into(),
        ))
    }
}

pub fn encoding_strategy(encoding: BenchEncoding) -> Box<dyn FieldEncodingStrategy> {
    if encoding == BenchEncoding::Array {
        return Box::new(lance_encoding::encoder::ArrayFieldEncodingStrategy::new());
    }

    let compression = Arc::new(BenchCompressionStrategy {
        encoding,
        params: CompressionParams::default(),
    });
    let page_encodings = match encoding {
        BenchEncoding::StructuralU16 => vec![
            PrimitivePageEncoding::reject_sparse(),
            PrimitivePageEncoding::dense_u16(compression),
        ],
        BenchEncoding::StructuralU32 => vec![
            PrimitivePageEncoding::reject_sparse(),
            PrimitivePageEncoding::constant(),
            PrimitivePageEncoding::dense_u32(compression),
        ],
        BenchEncoding::Array => unreachable!(),
    };
    Box::new(BenchFieldEncodingStrategy {
        encoding,
        primitive: PrimitiveFieldEncoding::new(page_encodings),
    })
}
