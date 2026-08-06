// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::BTreeMap, sync::Arc};

use lance_core::{Error, Result, datatypes::Schema};
use lance_encoding::{decoder::ColumnInfo, format::pb21};

use crate::{
    format::pbfile,
    reader::{
        BufferDescriptor, CachedFileMetadata, FileMetadataIndex, RawFileMetadata, ReaderProjection,
        structural,
    },
    version::ConcreteFileVersion,
};

fn required<'a, T>(value: Option<&'a T>, label: &str) -> Result<&'a T> {
    value.ok_or_else(|| {
        Error::invalid_input_source(
            format!("Lance v2.2 {label} is missing its nested encoding").into(),
        )
    })
}

fn validate_compressive_encoding(encoding: &pb21::CompressiveEncoding) -> Result<()> {
    use pb21::compressive_encoding::Compression;

    match encoding.compression.as_ref() {
        Some(Compression::Flat(_))
        | Some(Compression::InlineBitpacking(_))
        | Some(Compression::Constant(_)) => Ok(()),
        Some(Compression::Variable(variable)) => validate_compressive_encoding(required(
            variable.offsets.as_deref(),
            "variable offsets",
        )?),
        Some(Compression::OutOfLineBitpacking(bitpacking)) => {
            validate_compressive_encoding(required(
                bitpacking.values.as_deref(),
                "out-of-line bitpacking values",
            )?)
        }
        Some(Compression::Fsst(fsst)) => {
            validate_compressive_encoding(required(fsst.values.as_deref(), "FSST values")?)
        }
        Some(Compression::Dictionary(dictionary)) => {
            validate_compressive_encoding(required(
                dictionary.indices.as_deref(),
                "dictionary indices",
            )?)?;
            validate_compressive_encoding(required(
                dictionary.items.as_deref(),
                "dictionary items",
            )?)
        }
        Some(Compression::Rle(rle)) => {
            let values = required(rle.values.as_deref(), "RLE values")?;
            let run_lengths = required(rle.run_lengths.as_deref(), "RLE run lengths")?;
            let fixed_values = matches!(
                values.compression.as_ref(),
                Some(Compression::Flat(flat))
                    if matches!(flat.bits_per_value, 8 | 16 | 32 | 64)
                        && flat.data.is_none()
            );
            let fixed_u8_lengths = matches!(
                run_lengths.compression.as_ref(),
                Some(Compression::Flat(flat))
                    if flat.bits_per_value == 8 && flat.data.is_none()
            );
            if !fixed_values || !fixed_u8_lengths {
                return Err(Error::invalid_input_source(
                    "Lance v2.2 RLE requires flat values and flat u8 run lengths".into(),
                ));
            }
            Ok(())
        }
        Some(Compression::ByteStreamSplit(split)) => validate_compressive_encoding(required(
            split.values.as_deref(),
            "byte-stream-split values",
        )?),
        Some(Compression::General(general)) => validate_compressive_encoding(required(
            general.values.as_deref(),
            "general-compression values",
        )?),
        Some(Compression::FixedSizeList(list)) => validate_compressive_encoding(required(
            list.values.as_deref(),
            "fixed-size-list values",
        )?),
        Some(Compression::PackedStruct(packed)) => validate_compressive_encoding(required(
            packed.values.as_deref(),
            "packed-struct values",
        )?),
        Some(Compression::VariablePackedStruct(packed)) => {
            for field in &packed.fields {
                validate_compressive_encoding(required(
                    field.value.as_ref(),
                    "variable packed-struct field",
                )?)?;
            }
            Ok(())
        }
        None => Err(Error::invalid_input_source(
            "Lance v2.2 compressive encoding is missing its compression variant".into(),
        )),
    }
}

fn validate_page_layout(layout: &pb21::PageLayout) -> Result<()> {
    use pb21::page_layout::Layout;

    match layout.layout.as_ref() {
        Some(Layout::MiniBlockLayout(miniblock)) => {
            if !miniblock.has_large_chunk {
                return Err(Error::invalid_input_source(
                    "Lance v2.2 miniblock pages require the u32 chunk grammar".into(),
                ));
            }
            if let Some(rep) = miniblock.rep_compression.as_ref() {
                validate_compressive_encoding(rep)?;
            }
            if let Some(def) = miniblock.def_compression.as_ref() {
                validate_compressive_encoding(def)?;
            }
            validate_compressive_encoding(required(
                miniblock.value_compression.as_ref(),
                "miniblock values",
            )?)?;
            if let Some(dictionary) = miniblock.dictionary.as_ref() {
                validate_compressive_encoding(dictionary)?;
            }
            Ok(())
        }
        Some(Layout::FullZipLayout(fullzip)) => validate_compressive_encoding(required(
            fullzip.value_compression.as_ref(),
            "full-zip values",
        )?),
        Some(Layout::ConstantLayout(constant)) => {
            if let Some(rep) = constant.rep_compression.as_ref() {
                validate_compressive_encoding(rep)?;
            }
            if let Some(def) = constant.def_compression.as_ref() {
                validate_compressive_encoding(def)?;
            }
            Ok(())
        }
        Some(Layout::BlobLayout(blob)) => {
            let inner = blob.inner_layout.as_deref().ok_or_else(|| {
                Error::invalid_input_source(
                    "Lance v2.2 blob page layout is missing its inner layout".into(),
                )
            })?;
            validate_page_layout(inner)
        }
        Some(Layout::SparseLayout(_)) => Err(Error::invalid_input_source(
            "Sparse page layout is not part of the Lance v2.2 grammar".into(),
        )),
        None => Err(Error::invalid_input_source(
            "Lance v2.2 page is missing its page layout".into(),
        )),
    }
}

pub fn decode_column(
    column_index: u32,
    metadata: &pbfile::ColumnMetadata,
) -> Result<Arc<ColumnInfo>> {
    let page_infos = metadata
        .pages
        .iter()
        .enumerate()
        .map(|(page_index, page)| {
            let page_layout = structural::decode_page_layout(column_index, page_index, page)?;
            validate_page_layout(&page_layout)?;
            structural::build_page_info(column_index, page_index, page, page_layout)
        })
        .collect::<Result<Vec<_>>>()?;
    structural::build_column_info(column_index, metadata, page_infos)
}

pub fn decode_column_metadata(
    column_metadatas: &[pbfile::ColumnMetadata],
) -> Result<Vec<Arc<ColumnInfo>>> {
    column_metadatas
        .iter()
        .enumerate()
        .map(|(column_index, metadata)| {
            let column_index = u32::try_from(column_index).map_err(|_| {
                Error::invalid_input_source("File has more than u32::MAX columns".into())
            })?;
            decode_column(column_index, metadata)
        })
        .collect()
}

pub fn projection_from_field_ids(
    schema: &Schema,
    field_id_to_column_index: &BTreeMap<u32, u32>,
) -> ReaderProjection {
    structural::projection_from_field_ids(schema, field_id_to_column_index)
}

pub fn projection_from_whole_schema(schema: &Schema) -> ReaderProjection {
    structural::projection_from_field_ids(schema, &super::field_id_to_column_index(schema))
}

pub fn projection_from_column_names(
    schema: &Schema,
    column_names: &[&str],
) -> Result<ReaderProjection> {
    structural::projection_from_column_names(
        schema,
        column_names,
        &super::field_id_to_column_index(schema),
    )
}

pub fn finish_metadata(raw: RawFileMetadata) -> Result<CachedFileMetadata> {
    if (raw.footer.major_version, raw.footer.minor_version) != (2, 2) {
        return Err(Error::version_conflict(
            "Attempt to use the Lance v2.2 reader for a different file version".to_string(),
            raw.footer.major_version,
            raw.footer.minor_version,
        ));
    }
    validate_global_buffers(&raw.file_buffers)?;
    let column_infos = decode_column_metadata(&raw.column_metadatas)?;
    Ok(CachedFileMetadata {
        file_schema: raw.file_schema,
        column_metadatas: raw.column_metadatas,
        column_infos,
        num_rows: raw.num_rows,
        file_buffers: raw.file_buffers,
        num_data_bytes: raw.num_data_bytes,
        num_column_metadata_bytes: raw.num_column_metadata_bytes,
        num_global_buffer_bytes: raw.num_global_buffer_bytes,
        num_footer_bytes: raw.num_footer_bytes,
        major_version: raw.footer.major_version,
        minor_version: raw.footer.minor_version,
        version: ConcreteFileVersion::V2_2,
        file_size_bytes: raw.file_size_bytes,
        retained_global_buffers: raw.retained_global_buffers,
    })
}

pub fn validate_global_buffers(buffers: &[BufferDescriptor]) -> Result<()> {
    structural::validate_global_buffers(buffers)
}

pub fn finish_metadata_index(index: FileMetadataIndex) -> Result<FileMetadataIndex> {
    if index.version != ConcreteFileVersion::V2_2 {
        let (major, minor) = index.version.to_standard_footer_numbers();
        return Err(Error::version_conflict(
            "Attempt to use the Lance v2.2 reader for a different metadata index".to_string(),
            major,
            minor,
        ));
    }
    validate_global_buffers(&index.file_buffers)?;
    Ok(index)
}

#[cfg(test)]
mod grammar_tests {
    use super::*;
    use pb21::{
        CompressiveEncoding, Dictionary, Flat, FullZipLayout, PageLayout, Rle,
        compressive_encoding::Compression, page_layout::Layout,
    };

    fn flat(bits_per_value: u64) -> CompressiveEncoding {
        CompressiveEncoding {
            compression: Some(Compression::Flat(Flat {
                bits_per_value,
                data: None,
            })),
        }
    }

    #[test]
    fn rejects_nested_variable_width_rle() {
        let rle = CompressiveEncoding {
            compression: Some(Compression::Rle(Box::new(Rle {
                values: Some(Box::new(flat(32))),
                run_lengths: Some(Box::new(flat(16))),
            }))),
        };
        let dictionary = CompressiveEncoding {
            compression: Some(Compression::Dictionary(Box::new(Dictionary {
                indices: Some(Box::new(rle)),
                items: Some(Box::new(flat(32))),
                num_dictionary_items: 1,
            }))),
        };
        let layout = PageLayout {
            layout: Some(Layout::FullZipLayout(FullZipLayout {
                value_compression: Some(dictionary),
                ..Default::default()
            })),
        };

        let error = validate_page_layout(&layout).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("flat values and flat u8 run lengths")
        );
    }
}
