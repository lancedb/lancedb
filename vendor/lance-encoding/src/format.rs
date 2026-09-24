// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Protobuf definitions for encodings
///
/// These are the messages used for describing encoding in the 2.0 format
pub mod pb {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.encodings.rs"));
}

/// Protobuf definitions for encodings21
///
/// These are the messages used for describing encoding in the 2.1 format
/// and any newer formats.
pub mod pb21 {
    #![allow(clippy::all)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]
    #![allow(improper_ctypes)]
    #![allow(clippy::upper_case_acronyms)]
    #![allow(clippy::use_self)]
    include!(concat!(env!("OUT_DIR"), "/lance.encodings21.rs"));
}

use pb::{
    ArrayEncoding, Binary, Bitpacked, BitpackedForNonNeg, Block, Dictionary, FixedSizeBinary,
    FixedSizeList, Flat, Fsst, InlineBitpacking, Nullable, OutOfLineBitpacking, PackedStruct,
    PackedStructFixedWidthMiniBlock, Rle, Variable,
    array_encoding::ArrayEncoding as ArrayEncodingEnum,
    buffer::BufferType,
    nullable::{AllNull, NoNull, Nullability, SomeNull},
};

use crate::{encodings::physical::block::CompressionConfig, repdef::DefinitionInterpretation};

use self::pb::Constant;
use lance_core::Result;

// Utility functions for creating complex protobuf objects
pub struct ProtobufUtils {}

impl ProtobufUtils {
    pub fn constant(value: Vec<u8>) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Constant(Constant {
                value: value.into(),
            })),
        }
    }

    pub fn basic_all_null_encoding() -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Nullable(Box::new(Nullable {
                nullability: Some(Nullability::AllNulls(AllNull {})),
            }))),
        }
    }

    pub fn basic_some_null_encoding(
        validity: ArrayEncoding,
        values: ArrayEncoding,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Nullable(Box::new(Nullable {
                nullability: Some(Nullability::SomeNulls(Box::new(SomeNull {
                    validity: Some(Box::new(validity)),
                    values: Some(Box::new(values)),
                }))),
            }))),
        }
    }

    pub fn basic_no_null_encoding(values: ArrayEncoding) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Nullable(Box::new(Nullable {
                nullability: Some(Nullability::NoNulls(Box::new(NoNull {
                    values: Some(Box::new(values)),
                }))),
            }))),
        }
    }

    pub fn block(scheme: &str) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Block(Block {
                scheme: scheme.to_string(),
            })),
        }
    }

    pub fn flat_encoding(
        bits_per_value: u64,
        buffer_index: u32,
        compression: Option<CompressionConfig>,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Flat(Flat {
                bits_per_value,
                buffer: Some(pb::Buffer {
                    buffer_index,
                    buffer_type: BufferType::Page as i32,
                }),
                compression: compression.map(|compression_config| pb::Compression {
                    scheme: compression_config.scheme.to_string(),
                    level: compression_config.level,
                }),
            })),
        }
    }

    pub fn fsl_encoding(dimension: u64, items: ArrayEncoding, has_validity: bool) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::FixedSizeList(Box::new(FixedSizeList {
                dimension: dimension.try_into().unwrap(),
                items: Some(Box::new(items)),
                has_validity,
            }))),
        }
    }

    pub fn bitpacked_encoding(
        compressed_bits_per_value: u64,
        uncompressed_bits_per_value: u64,
        buffer_index: u32,
        signed: bool,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Bitpacked(Bitpacked {
                compressed_bits_per_value,
                buffer: Some(pb::Buffer {
                    buffer_index,
                    buffer_type: BufferType::Page as i32,
                }),
                uncompressed_bits_per_value,
                signed,
            })),
        }
    }

    pub fn bitpacked_for_non_neg_encoding(
        compressed_bits_per_value: u64,
        uncompressed_bits_per_value: u64,
        buffer_index: u32,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::BitpackedForNonNeg(BitpackedForNonNeg {
                compressed_bits_per_value,
                buffer: Some(pb::Buffer {
                    buffer_index,
                    buffer_type: BufferType::Page as i32,
                }),
                uncompressed_bits_per_value,
            })),
        }
    }
    pub fn inline_bitpacking(uncompressed_bits_per_value: u64) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::InlineBitpacking(InlineBitpacking {
                uncompressed_bits_per_value,
            })),
        }
    }
    pub fn out_of_line_bitpacking(
        uncompressed_bits_per_value: u64,
        compressed_bits_per_value: u64,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::OutOfLineBitpacking(
                OutOfLineBitpacking {
                    uncompressed_bits_per_value,
                    compressed_bits_per_value,
                },
            )),
        }
    }

    pub fn variable(bits_per_offset: u8) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Variable(Variable {
                bits_per_offset: bits_per_offset as u32,
            })),
        }
    }

    // Construct a `FsstMiniBlock` ArrayEncoding, the inner `binary_mini_block` encoding is actually
    // not used and `FsstMiniBlockDecompressor` constructs a `binary_mini_block` in a `hard-coded` fashion.
    // This can be an optimization later.
    pub fn fsst(data: ArrayEncoding, symbol_table: Vec<u8>) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Fsst(Box::new(Fsst {
                binary: Some(Box::new(data)),
                symbol_table: symbol_table.into(),
            }))),
        }
    }

    pub fn rle(bits_per_value: u64) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Rle(Rle { bits_per_value })),
        }
    }

    pub fn byte_stream_split(bits_per_value: u64) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::ByteStreamSplit(pb::ByteStreamSplit {
                bits_per_value,
            })),
        }
    }

    pub fn general_mini_block(
        inner: ArrayEncoding,
        compression: CompressionConfig,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::GeneralMiniBlock(Box::new(
                pb::GeneralMiniBlock {
                    inner: Some(Box::new(inner)),
                    compression: Some(pb::Compression {
                        scheme: compression.scheme.to_string(),
                        level: compression.level,
                    }),
                },
            ))),
        }
    }

    pub fn packed_struct(
        child_encodings: Vec<ArrayEncoding>,
        packed_buffer_index: u32,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::PackedStruct(PackedStruct {
                inner: child_encodings,
                buffer: Some(pb::Buffer {
                    buffer_index: packed_buffer_index,
                    buffer_type: BufferType::Page as i32,
                }),
            })),
        }
    }

    pub fn packed_struct_fixed_width_mini_block(
        data: ArrayEncoding,
        bits_per_values: Vec<u32>,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::PackedStructFixedWidthMiniBlock(
                Box::new(PackedStructFixedWidthMiniBlock {
                    flat: Some(Box::new(data)),
                    bits_per_values,
                }),
            )),
        }
    }

    pub fn binary(
        indices_encoding: ArrayEncoding,
        bytes_encoding: ArrayEncoding,
        null_adjustment: u64,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Binary(Box::new(Binary {
                bytes: Some(Box::new(bytes_encoding)),
                indices: Some(Box::new(indices_encoding)),
                null_adjustment,
            }))),
        }
    }

    pub fn dict_encoding(
        indices: ArrayEncoding,
        items: ArrayEncoding,
        num_items: u32,
    ) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::Dictionary(Box::new(Dictionary {
                indices: Some(Box::new(indices)),
                items: Some(Box::new(items)),
                num_dictionary_items: num_items,
            }))),
        }
    }

    pub fn fixed_size_binary(data: ArrayEncoding, byte_width: u32) -> ArrayEncoding {
        ArrayEncoding {
            array_encoding: Some(ArrayEncodingEnum::FixedSizeBinary(Box::new(
                FixedSizeBinary {
                    bytes: Some(Box::new(data)),
                    byte_width,
                },
            ))),
        }
    }
}

macro_rules! impl_common_protobuf_utils {
    ($module:ident, $struct_name:ident) => {
        pub struct $struct_name {}

        impl $struct_name {
            pub fn flat(
                bits_per_value: u64,
                values_compression: Option<crate::format::$module::BufferCompression>,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::Flat(
                            crate::format::$module::Flat {
                                bits_per_value,
                                data: values_compression,
                            },
                        ),
                    ),
                }
            }

            pub fn constant(
                value: Option<bytes::Bytes>,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::Constant(
                            crate::format::$module::Constant { value },
                        ),
                    ),
                }
            }

            pub fn fsl(
                items_per_value: u64,
                has_validity: bool,
                values: crate::format::$module::CompressiveEncoding,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::FixedSizeList(
                            Box::new(crate::format::$module::FixedSizeList {
                                items_per_value,
                                has_validity,
                                values: Some(Box::new(values)),
                            }),
                        ),
                    ),
                }
            }

            pub fn variable(
                offsets_desc: crate::format::$module::CompressiveEncoding,
                values_compression: Option<crate::format::$module::BufferCompression>,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::Variable(
                            Box::new(crate::format::$module::Variable {
                                offsets: Some(Box::new(offsets_desc)),
                                values: values_compression,
                            }),
                        ),
                    ),
                }
            }

            pub fn inline_bitpacking(
                uncompressed_bits_per_value: u64,
                values_compression: Option<crate::format::$module::BufferCompression>,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::InlineBitpacking(
                            crate::format::$module::InlineBitpacking {
                                uncompressed_bits_per_value,
                                values: values_compression,
                            },
                        ),
                    ),
                }
            }

            pub fn out_of_line_bitpacking(
                uncompressed_bits_per_value: u64,
                values: crate::format::$module::CompressiveEncoding,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::OutOfLineBitpacking(
                            Box::new(crate::format::$module::OutOfLineBitpacking {
                                uncompressed_bits_per_value,
                                values: Some(Box::new(values)),
                            }),
                        ),
                    ),
                }
            }

            pub fn buffer_compression(
                compression: CompressionConfig,
            ) -> Result<crate::format::$module::BufferCompression> {
                Ok(crate::format::$module::BufferCompression {
                    scheme: crate::format::$module::CompressionScheme::try_from(
                        compression.scheme,
                    )? as i32,
                    level: compression.level,
                })
            }

            pub fn wrapped(
                compression: CompressionConfig,
                values: crate::format::$module::CompressiveEncoding,
            ) -> Result<crate::format::$module::CompressiveEncoding> {
                Ok(crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::General(
                            Box::new(crate::format::$module::General {
                                compression: Some(Self::buffer_compression(compression)?),
                                values: Some(Box::new(values)),
                            }),
                        ),
                    ),
                })
            }

            pub fn rle(
                values: crate::format::$module::CompressiveEncoding,
                run_lengths: crate::format::$module::CompressiveEncoding,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::Rle(Box::new(
                            crate::format::$module::Rle {
                                values: Some(Box::new(values)),
                                run_lengths: Some(Box::new(run_lengths)),
                            },
                        )),
                    ),
                }
            }

            pub fn byte_stream_split(
                values: crate::format::$module::CompressiveEncoding,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::ByteStreamSplit(
                            Box::new(crate::format::$module::ByteStreamSplit {
                                values: Some(Box::new(values)),
                            }),
                        ),
                    ),
                }
            }

            pub fn fsst(
                data: crate::format::$module::CompressiveEncoding,
                symbol_table: Vec<u8>,
            ) -> crate::format::$module::CompressiveEncoding {
                crate::format::$module::CompressiveEncoding {
                    compression: Some(
                        crate::format::$module::compressive_encoding::Compression::Fsst(
                            Box::new(crate::format::$module::Fsst {
                                symbol_table: symbol_table.into(),
                                values: Some(Box::new(data)),
                            }),
                        ),
                    ),
                }
            }

            fn def_inter_to_repdef_layer(def: DefinitionInterpretation) -> i32 {
                match def {
                    DefinitionInterpretation::AllValidItem => {
                        crate::format::$module::RepDefLayer::RepdefAllValidItem as i32
                    }
                    DefinitionInterpretation::AllValidList => {
                        crate::format::$module::RepDefLayer::RepdefAllValidList as i32
                    }
                    DefinitionInterpretation::NullableItem => {
                        crate::format::$module::RepDefLayer::RepdefNullableItem as i32
                    }
                    DefinitionInterpretation::NullableList => {
                        crate::format::$module::RepDefLayer::RepdefNullableList as i32
                    }
                    DefinitionInterpretation::EmptyableList => {
                        crate::format::$module::RepDefLayer::RepdefEmptyableList as i32
                    }
                    DefinitionInterpretation::NullableAndEmptyableList => {
                        crate::format::$module::RepDefLayer::RepdefNullAndEmptyList as i32
                    }
                }
            }

            pub fn repdef_layer_to_def_interp(
                layer: i32,
            ) -> DefinitionInterpretation {
                let layer = crate::format::$module::RepDefLayer::try_from(layer).unwrap();
                match layer {
                    crate::format::$module::RepDefLayer::RepdefAllValidItem => {
                        DefinitionInterpretation::AllValidItem
                    }
                    crate::format::$module::RepDefLayer::RepdefAllValidList => {
                        DefinitionInterpretation::AllValidList
                    }
                    crate::format::$module::RepDefLayer::RepdefNullableItem => {
                        DefinitionInterpretation::NullableItem
                    }
                    crate::format::$module::RepDefLayer::RepdefNullableList => {
                        DefinitionInterpretation::NullableList
                    }
                    crate::format::$module::RepDefLayer::RepdefEmptyableList => {
                        DefinitionInterpretation::EmptyableList
                    }
                    crate::format::$module::RepDefLayer::RepdefNullAndEmptyList => {
                        DefinitionInterpretation::NullableAndEmptyableList
                    }
                    crate::format::$module::RepDefLayer::RepdefUnspecified => {
                        panic!("Unspecified repdef layer")
                    }
                }
            }

            #[allow(clippy::too_many_arguments)]
            pub fn miniblock_layout(
                rep_encoding: Option<crate::format::$module::CompressiveEncoding>,
                def_encoding: Option<crate::format::$module::CompressiveEncoding>,
                value_encoding: crate::format::$module::CompressiveEncoding,
                repetition_index_depth: u32,
                num_buffers: u64,
                dictionary_encoding: Option<(
                    crate::format::$module::CompressiveEncoding,
                    u64,
                )>,
                def_meaning: &[DefinitionInterpretation],
                num_items: u64,
                has_large_chunk: bool,
    ) -> crate::format::$module::PageLayout {
                assert!(!def_meaning.is_empty());
                let (dictionary, num_dictionary_items) = dictionary_encoding
                    .map(|(d, i)| (Some(d), i))
                    .unwrap_or((None, 0));
                crate::format::$module::PageLayout {
                    layout: Some(
                        crate::format::$module::page_layout::Layout::MiniBlockLayout(
                            crate::format::$module::MiniBlockLayout {
                                def_compression: def_encoding,
                                rep_compression: rep_encoding,
                                value_compression: Some(value_encoding),
                                repetition_index_depth,
                                num_buffers,
                                dictionary,
                                num_dictionary_items,
                                layers: def_meaning
                                    .iter()
                                    .map(|&def| Self::def_inter_to_repdef_layer(def))
                                    .collect(),
                                num_items,
                                has_large_chunk,
                },
                        ),
                    ),
                }
            }

            fn full_zip_layout(
                bits_rep: u8,
                bits_def: u8,
                details: crate::format::$module::full_zip_layout::Details,
                value_encoding: crate::format::$module::CompressiveEncoding,
                def_meaning: &[DefinitionInterpretation],
                num_items: u32,
                num_visible_items: u32,
            ) -> crate::format::$module::PageLayout {
                crate::format::$module::PageLayout {
                    layout: Some(
                        crate::format::$module::page_layout::Layout::FullZipLayout(
                            crate::format::$module::FullZipLayout {
                                bits_rep: bits_rep as u32,
                                bits_def: bits_def as u32,
                                details: Some(details),
                                value_compression: Some(value_encoding),
                                num_items,
                                num_visible_items,
                                layers: def_meaning
                                    .iter()
                                    .map(|&def| Self::def_inter_to_repdef_layer(def))
                                    .collect(),
                            },
                        ),
                    ),
                }
            }

            pub fn fixed_full_zip_layout(
                bits_rep: u8,
                bits_def: u8,
                bits_per_value: u32,
                value_encoding: crate::format::$module::CompressiveEncoding,
                def_meaning: &[DefinitionInterpretation],
                num_items: u32,
                num_visible_items: u32,
            ) -> crate::format::$module::PageLayout {
                Self::full_zip_layout(
                    bits_rep,
                    bits_def,
                    crate::format::$module::full_zip_layout::Details::BitsPerValue(
                        bits_per_value,
                    ),
                    value_encoding,
                    def_meaning,
                    num_items,
                    num_visible_items,
                )
            }

            pub fn variable_full_zip_layout(
                bits_rep: u8,
                bits_def: u8,
                bits_per_offset: u32,
                value_encoding: crate::format::$module::CompressiveEncoding,
                def_meaning: &[DefinitionInterpretation],
                num_items: u32,
                num_visible_items: u32,
            ) -> crate::format::$module::PageLayout {
                Self::full_zip_layout(
                    bits_rep,
                    bits_def,
                    crate::format::$module::full_zip_layout::Details::BitsPerOffset(
                        bits_per_offset,
                    ),
                    value_encoding,
                    def_meaning,
                    num_items,
                    num_visible_items,
                )
            }

            pub fn blob_layout(
                inner_layout: crate::format::$module::PageLayout,
                def_meaning: &[DefinitionInterpretation],
            ) -> crate::format::$module::PageLayout {
                crate::format::$module::PageLayout {
                    layout: Some(
                        crate::format::$module::page_layout::Layout::BlobLayout(Box::new(
                            crate::format::$module::BlobLayout {
                                inner_layout: Some(Box::new(inner_layout)),
                                layers: def_meaning
                                    .iter()
                                    .map(|&def| Self::def_inter_to_repdef_layer(def))
                                    .collect(),
                            },
                        )),
                    ),
                }
            }


        }
    };
}

impl_common_protobuf_utils!(pb21, ProtobufUtils21);

impl ProtobufUtils21 {
    pub fn constant_layout(
        def_meaning: &[DefinitionInterpretation],
        inline_value: Option<Vec<u8>>,
    ) -> crate::format::pb21::PageLayout {
        crate::format::pb21::PageLayout {
            layout: Some(crate::format::pb21::page_layout::Layout::ConstantLayout(
                crate::format::pb21::ConstantLayout {
                    inline_value: inline_value.map(bytes::Bytes::from),
                    rep_compression: None,
                    def_compression: None,
                    num_rep_values: 0,
                    num_def_values: 0,
                    layers: def_meaning
                        .iter()
                        .map(|&def| Self::def_inter_to_repdef_layer(def))
                        .collect(),
                },
            )),
        }
    }

    pub fn compressed_all_null_constant_layout(
        def_meaning: &[DefinitionInterpretation],
        rep_compression: Option<crate::format::pb21::CompressiveEncoding>,
        def_compression: Option<crate::format::pb21::CompressiveEncoding>,
        num_rep_values: u64,
        num_def_values: u64,
    ) -> crate::format::pb21::PageLayout {
        crate::format::pb21::PageLayout {
            layout: Some(crate::format::pb21::page_layout::Layout::ConstantLayout(
                crate::format::pb21::ConstantLayout {
                    inline_value: None,
                    rep_compression,
                    def_compression,
                    num_rep_values,
                    num_def_values,
                    layers: def_meaning
                        .iter()
                        .map(|&def| Self::def_inter_to_repdef_layer(def))
                        .collect(),
                },
            )),
        }
    }

    pub fn packed_struct(
        values: crate::format::pb21::CompressiveEncoding,
        bits_per_values: Vec<u64>,
    ) -> crate::format::pb21::CompressiveEncoding {
        crate::format::pb21::CompressiveEncoding {
            compression: Some(
                crate::format::pb21::compressive_encoding::Compression::PackedStruct(Box::new(
                    crate::format::pb21::PackedStruct {
                        bits_per_value: bits_per_values,
                        values: Some(Box::new(values)),
                    },
                )),
            ),
        }
    }

    pub fn packed_struct_variable(
        fields: Vec<crate::format::pb21::variable_packed_struct::FieldEncoding>,
    ) -> crate::format::pb21::CompressiveEncoding {
        crate::format::pb21::CompressiveEncoding {
            compression: Some(
                crate::format::pb21::compressive_encoding::Compression::VariablePackedStruct(
                    crate::format::pb21::VariablePackedStruct { fields },
                ),
            ),
        }
    }

    pub fn packed_struct_field_fixed(
        value_encoding: crate::format::pb21::CompressiveEncoding,
        bits_per_value: u64,
    ) -> crate::format::pb21::variable_packed_struct::FieldEncoding {
        crate::format::pb21::variable_packed_struct::FieldEncoding {
            value: Some(value_encoding),
            layout: Some(
                crate::format::pb21::variable_packed_struct::field_encoding::Layout::BitsPerValue(
                    bits_per_value,
                ),
            ),
        }
    }

    pub fn packed_struct_field_variable(
        value_encoding: crate::format::pb21::CompressiveEncoding,
        bits_per_length: u64,
    ) -> crate::format::pb21::variable_packed_struct::FieldEncoding {
        crate::format::pb21::variable_packed_struct::FieldEncoding {
            value: Some(value_encoding),
            layout: Some(
                crate::format::pb21::variable_packed_struct::field_encoding::Layout::BitsPerLength(
                    bits_per_length,
                ),
            ),
        }
    }
}
