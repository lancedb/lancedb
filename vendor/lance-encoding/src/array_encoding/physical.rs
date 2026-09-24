// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use arrow_schema::DataType;
use lance_arrow::DataTypeExt;

use crate::{
    array_encoding::physical::{
        basic::BasicPageScheduler, binary::BinaryPageScheduler, bitmap::DenseBitmapScheduler,
        dictionary::DictionaryPageScheduler, fixed_size_list::FixedListScheduler,
        fsst::FsstPageScheduler, packed_struct::PackedStructPageScheduler,
        value::ValuePageScheduler,
    },
    buffer::LanceBuffer,
    decoder::{PageBuffers, PageScheduler},
    encodings::physical::block::{CompressionConfig, CompressionScheme},
    format::pb::{self, PackedStruct},
};

pub mod basic;
pub mod binary;
pub mod bitmap;
#[cfg(feature = "bitpacking")]
pub mod bitpack;
pub mod block;
pub mod dictionary;
pub mod fixed_size_binary;
pub mod fixed_size_list;
pub mod fsst;
pub mod packed_struct;
pub mod value;

// Translate a protobuf buffer description into a position in the file.  This could be a page
// buffer, a column buffer, or a file buffer.
fn get_buffer(buffer_desc: &pb::Buffer, buffers: &PageBuffers) -> (u64, u64) {
    let index = buffer_desc.buffer_index as usize;

    match pb::buffer::BufferType::try_from(buffer_desc.buffer_type).unwrap() {
        pb::buffer::BufferType::Page => buffers.positions_and_sizes[index],
        pb::buffer::BufferType::Column => buffers.column_buffers.positions_and_sizes[index],
        pb::buffer::BufferType::File => {
            buffers.column_buffers.file_buffers.positions_and_sizes[index]
        }
    }
}

/// Convert a protobuf buffer encoding into a physical page scheduler
fn get_buffer_decoder(encoding: &pb::Flat, buffers: &PageBuffers) -> Box<dyn PageScheduler> {
    let (buffer_offset, buffer_size) = get_buffer(encoding.buffer.as_ref().unwrap(), buffers);
    let compression_config: CompressionConfig = match encoding.compression.as_ref() {
        None => CompressionConfig::new(CompressionScheme::None, None),
        Some(compression) => CompressionConfig::new(
            compression.scheme.as_str().parse().unwrap(),
            compression.level,
        ),
    };
    match encoding.bits_per_value {
        1 => Box::new(DenseBitmapScheduler::new(buffer_offset)),
        bits_per_value => {
            if bits_per_value % 8 != 0 {
                todo!(
                    "bits_per_value ({}) that is not a multiple of 8",
                    bits_per_value
                );
            }
            Box::new(ValuePageScheduler::new(
                bits_per_value / 8,
                buffer_offset,
                buffer_size,
                compression_config,
            ))
        }
    }
}

#[cfg(feature = "bitpacking")]
fn get_bitpacked_buffer_decoder(
    encoding: &pb::Bitpacked,
    buffers: &PageBuffers,
) -> Box<dyn PageScheduler> {
    let (buffer_offset, _buffer_size) = get_buffer(encoding.buffer.as_ref().unwrap(), buffers);

    Box::new(bitpack::BitpackedScheduler::new(
        encoding.compressed_bits_per_value,
        encoding.uncompressed_bits_per_value,
        buffer_offset,
        encoding.signed,
    ))
}

#[cfg(feature = "bitpacking")]
fn get_bitpacked_for_non_neg_buffer_decoder(
    encoding: &pb::BitpackedForNonNeg,
    buffers: &PageBuffers,
) -> Box<dyn PageScheduler> {
    let (buffer_offset, _buffer_size) = get_buffer(encoding.buffer.as_ref().unwrap(), buffers);

    Box::new(bitpack::BitpackedForNonNegScheduler::new(
        encoding.compressed_bits_per_value,
        encoding.uncompressed_bits_per_value,
        buffer_offset,
    ))
}

fn decoder_from_packed_struct(
    packed_struct: &PackedStruct,
    buffers: &PageBuffers,
    data_type: &DataType,
) -> Box<dyn PageScheduler> {
    let inner_encodings = &packed_struct.inner;
    let fields = match data_type {
        DataType::Struct(fields) => Some(fields),
        _ => None,
    }
    .unwrap();

    let inner_datatypes = fields
        .iter()
        .map(|field| field.data_type())
        .collect::<Vec<_>>();

    let mut inner_schedulers = Vec::with_capacity(fields.len());
    for i in 0..fields.len() {
        let inner_encoding = &inner_encodings[i];
        let inner_datatype = inner_datatypes[i];
        let inner_scheduler = decoder_from_array_encoding(inner_encoding, buffers, inner_datatype);
        inner_schedulers.push(inner_scheduler);
    }

    let packed_buffer = packed_struct.buffer.as_ref().unwrap();
    let (buffer_offset, _) = get_buffer(packed_buffer, buffers);

    Box::new(PackedStructPageScheduler::new(
        inner_schedulers,
        data_type.clone(),
        buffer_offset,
    ))
}

/// Convert a protobuf array encoding into a physical page scheduler
pub fn decoder_from_array_encoding(
    encoding: &pb::ArrayEncoding,
    buffers: &PageBuffers,
    data_type: &DataType,
) -> Box<dyn PageScheduler> {
    match encoding.array_encoding.as_ref().unwrap() {
        pb::array_encoding::ArrayEncoding::Nullable(basic) => {
            match basic.nullability.as_ref().unwrap() {
                pb::nullable::Nullability::NoNulls(no_nulls) => Box::new(
                    BasicPageScheduler::new_non_nullable(decoder_from_array_encoding(
                        no_nulls.values.as_ref().unwrap(),
                        buffers,
                        data_type,
                    )),
                ),
                pb::nullable::Nullability::SomeNulls(some_nulls) => {
                    Box::new(BasicPageScheduler::new_nullable(
                        decoder_from_array_encoding(
                            some_nulls.validity.as_ref().unwrap(),
                            buffers,
                            data_type,
                        ),
                        decoder_from_array_encoding(
                            some_nulls.values.as_ref().unwrap(),
                            buffers,
                            data_type,
                        ),
                    ))
                }
                pb::nullable::Nullability::AllNulls(_) => {
                    Box::new(BasicPageScheduler::new_all_null())
                }
            }
        }
        #[cfg(feature = "bitpacking")]
        pb::array_encoding::ArrayEncoding::Bitpacked(bitpacked) => {
            get_bitpacked_buffer_decoder(bitpacked, buffers)
        }
        #[cfg(not(feature = "bitpacking"))]
        pb::array_encoding::ArrayEncoding::Bitpacked(_) => {
            panic!("Runtime built without bitpacking support")
        }
        pb::array_encoding::ArrayEncoding::Flat(flat) => get_buffer_decoder(flat, buffers),
        pb::array_encoding::ArrayEncoding::FixedSizeList(fixed_size_list) => {
            let item_encoding = fixed_size_list.items.as_ref().unwrap();
            let item_scheduler = decoder_from_array_encoding(item_encoding, buffers, data_type);
            Box::new(FixedListScheduler::new(
                item_scheduler,
                fixed_size_list.dimension,
            ))
        }
        // This is a column containing the list offsets.  This wrapper is superfluous at the moment
        // since we know it is a list based on the schema.  In the future there may be different ways
        // of storing the list offsets.
        pb::array_encoding::ArrayEncoding::List(list) => {
            decoder_from_array_encoding(list.offsets.as_ref().unwrap(), buffers, data_type)
        }
        pb::array_encoding::ArrayEncoding::Binary(binary) => {
            let indices_encoding = binary.indices.as_ref().unwrap();
            let bytes_encoding = binary.bytes.as_ref().unwrap();

            let indices_scheduler =
                decoder_from_array_encoding(indices_encoding, buffers, data_type);
            let bytes_scheduler = decoder_from_array_encoding(bytes_encoding, buffers, data_type);

            let offset_type = match data_type {
                DataType::LargeBinary | DataType::LargeUtf8 => DataType::Int64,
                _ => DataType::Int32,
            };

            Box::new(BinaryPageScheduler::new(
                indices_scheduler.into(),
                bytes_scheduler.into(),
                offset_type,
                binary.null_adjustment,
            ))
        }
        pb::array_encoding::ArrayEncoding::Fsst(fsst) => {
            let inner =
                decoder_from_array_encoding(fsst.binary.as_ref().unwrap(), buffers, data_type);

            Box::new(FsstPageScheduler::new(
                inner,
                LanceBuffer::from_bytes(fsst.symbol_table.clone(), 1),
            ))
        }
        pb::array_encoding::ArrayEncoding::Dictionary(dictionary) => {
            let indices_encoding = dictionary.indices.as_ref().unwrap();
            let items_encoding = dictionary.items.as_ref().unwrap();
            let num_dictionary_items = dictionary.num_dictionary_items;

            // We can get here in 2 ways.  The data is dictionary encoded and the user wants a dictionary or
            // the data is dictionary encoded, as an optimization, and the user wants the value type.  Figure
            // out the value type.
            let value_type = if let DataType::Dictionary(_, value_type) = data_type {
                value_type
            } else {
                data_type
            };

            // Note: we don't actually know the indices type here, passing down `data_type` works ok because
            // the dictionary indices are always integers and we don't need the data_type to figure out how
            // to decode integers.
            let indices_scheduler =
                decoder_from_array_encoding(indices_encoding, buffers, data_type);

            let items_scheduler = decoder_from_array_encoding(items_encoding, buffers, value_type);

            let should_decode_dict = !data_type.is_dictionary();

            Box::new(DictionaryPageScheduler::new(
                indices_scheduler.into(),
                items_scheduler.into(),
                num_dictionary_items,
                should_decode_dict,
            ))
        }
        pb::array_encoding::ArrayEncoding::FixedSizeBinary(fixed_size_binary) => {
            let bytes_encoding = fixed_size_binary.bytes.as_ref().unwrap();
            let bytes_scheduler = decoder_from_array_encoding(bytes_encoding, buffers, data_type);
            let bytes_per_offset = match data_type {
                DataType::LargeBinary | DataType::LargeUtf8 => 8,
                DataType::Binary | DataType::Utf8 => 4,
                _ => panic!("FixedSizeBinary only supports binary and utf8 types"),
            };

            Box::new(fixed_size_binary::FixedSizeBinaryPageScheduler::new(
                bytes_scheduler,
                fixed_size_binary.byte_width,
                bytes_per_offset,
            ))
        }
        pb::array_encoding::ArrayEncoding::PackedStruct(packed_struct) => {
            decoder_from_packed_struct(packed_struct, buffers, data_type)
        }
        #[cfg(feature = "bitpacking")]
        pb::array_encoding::ArrayEncoding::BitpackedForNonNeg(bitpacked) => {
            get_bitpacked_for_non_neg_buffer_decoder(bitpacked, buffers)
        }
        #[cfg(not(feature = "bitpacking"))]
        pb::array_encoding::ArrayEncoding::BitpackedForNonNeg(_) => {
            panic!("Runtime built without bitpacking support")
        }
        // Currently there is no way to encode struct nullability and structs are encoded with a "header" column
        // (that has no data).  We never actually decode that column and so this branch is never actually encountered.
        //
        // This will change in the future when we add support for struct nullability.
        pb::array_encoding::ArrayEncoding::Struct(_) => unreachable!(),
        // 2.1 only
        _ => unreachable!("Unsupported array encoding: {:?}", encoding),
    }
}

#[cfg(test)]
mod tests {
    use crate::array_encoding::physical::get_buffer_decoder;
    use crate::decoder::{ColumnBuffers, FileBuffers, PageBuffers};
    use crate::format::pb;

    #[test]
    fn test_get_buffer_decoder_for_compressed_buffer() {
        let page_scheduler = get_buffer_decoder(
            &pb::Flat {
                buffer: Some(pb::Buffer {
                    buffer_index: 0,
                    buffer_type: pb::buffer::BufferType::File as i32,
                }),
                bits_per_value: 8,
                compression: Some(pb::Compression {
                    scheme: "zstd".to_string(),
                    level: Some(0),
                }),
            },
            &PageBuffers {
                column_buffers: ColumnBuffers {
                    file_buffers: FileBuffers {
                        positions_and_sizes: &[(0, 100)],
                    },
                    positions_and_sizes: &[],
                },
                positions_and_sizes: &[],
            },
        );
        assert_eq!(
            format!("{:?}", page_scheduler).as_str(),
            "ValuePageScheduler { bytes_per_value: 1, buffer_offset: 0, buffer_size: 100, compression_config: CompressionConfig { scheme: Zstd, level: Some(0) } }"
        );
    }
}
