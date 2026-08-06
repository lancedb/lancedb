// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::{Error, Result, datatypes::Field};
use lance_encoding::{
    compression::{
        BlockCompressor, CompressionStrategy, field_metadata_params, finalize_miniblock_compressor,
        reject_packed_struct_per_value, try_bitpacking_block, try_bitpacking_miniblock,
        try_byte_stream_split_miniblock, try_fixed_packed_struct_miniblock,
        try_fixed_u8_rle_miniblock, try_raw_block, try_raw_fixed_size_list_miniblock,
        try_raw_fixed_width_miniblock, try_raw_per_value, try_uncompressed_fixed_width_miniblock,
        try_variable_width_miniblock, try_variable_width_per_value,
    },
    compression_config::{CompressionFieldParams, CompressionParams},
    data::DataBlock,
    encodings::logical::primitive::{fullzip::PerValueCompressor, miniblock::MiniBlockCompressor},
    format::pb21::CompressiveEncoding,
};

#[derive(Debug, Clone)]
pub(super) struct Strategy {
    params: CompressionParams,
}

impl Strategy {
    pub(super) fn new(params: CompressionParams) -> Self {
        Self { params }
    }

    fn field_params(&self, field: &Field) -> CompressionFieldParams {
        let mut params = self
            .params
            .get_field_params(&field.name, &field.data_type());
        let mut metadata = field_metadata_params(field);
        if metadata
            .minichunk_size
            .is_some_and(|size| size >= 32 * 1024)
        {
            log::warn!(
                "minichunk_size '{}' is too large for the selected u16 miniblock layout, using default",
                metadata.minichunk_size.unwrap()
            );
            metadata.minichunk_size = None;
        }
        params.merge(&metadata);
        params
    }
}

impl CompressionStrategy for Strategy {
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
        if let Some(compressor) = reject_packed_struct_per_value(field, data)? {
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
        let _params = self.field_params(field);
        if let Some(compressor) = try_bitpacking_block(data) {
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
