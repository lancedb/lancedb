// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance_core::{Error, Result, datatypes::Field};
use lance_encoding::{
    compression::{
        BlockCompressor, CompressionStrategy, field_metadata_params, finalize_miniblock_compressor,
        try_bitpacking_block, try_bitpacking_miniblock, try_byte_stream_split_miniblock,
        try_fixed_packed_struct_miniblock, try_fixed_u8_rle_block, try_fixed_u8_rle_miniblock,
        try_general_block, try_raw_block, try_raw_fixed_size_list_miniblock,
        try_raw_fixed_width_miniblock, try_raw_per_value, try_uncompressed_fixed_width_miniblock,
        try_variable_packed_struct_per_value, try_variable_width_miniblock,
        try_variable_width_per_value,
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
        params.merge(&field_metadata_params(field));
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
        if let Some(compressor) =
            try_variable_packed_struct_per_value(Arc::new(self.clone()), field, data)?
        {
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
        if let Some(compressor) = try_fixed_u8_rle_block(data, &params)? {
            return Ok(compressor);
        }
        if let Some(compressor) = try_bitpacking_block(data) {
            return Ok(compressor);
        }
        if let Some(compressor) = try_general_block(data, &params)? {
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
