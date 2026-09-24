// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Constants for Lance encoding metadata keys
//!
//! These constants define the metadata keys used in Arrow field metadata
//! to configure various encoding behaviors in Lance.

// Compression-related metadata keys
/// Metadata key for specifying compression scheme (e.g., "lz4", "zstd", "none")
pub const COMPRESSION_META_KEY: &str = "lance-encoding:compression";
/// Metadata key for specifying compression level (applies to schemes that support levels)
pub const COMPRESSION_LEVEL_META_KEY: &str = "lance-encoding:compression-level";
/// Metadata key for specifying RLE (Run-Length Encoding) threshold
pub const RLE_THRESHOLD_META_KEY: &str = "lance-encoding:rle-threshold";
/// Metadata key for specifying minichunk size
pub const MINICHUNK_SIZE_META_KEY: &str = "lance-encoding:minichunk-size";

// Dictionary encoding metadata keys
/// Metadata key for specifying dictionary encoding threshold divisor
/// Set to a large value to discourage dictionary encoding
/// Set to a small value to encourage dictionary encoding
pub const DICT_DIVISOR_META_KEY: &str = "lance-encoding:dict-divisor";
/// Metadata key for dictionary encoding size ratio threshold (0.0-1.0]
/// If estimated_dict_size/raw_size < ratio, use dictionary encoding.
/// Example: 0.8 means use dict if encoded size < 80% of raw size
/// Default: 0.8
pub const DICT_SIZE_RATIO_META_KEY: &str = "lance-encoding:dict-size-ratio";
/// Metadata key for selecting general compression scheme for dictionary values
/// Valid values: "lz4", "zstd", "none"
pub const DICT_VALUES_COMPRESSION_META_KEY: &str = "lance-encoding:dict-values-compression";
/// Metadata key for selecting compression level for dictionary values
/// Applies to schemes that support levels (e.g. zstd)
pub const DICT_VALUES_COMPRESSION_LEVEL_META_KEY: &str =
    "lance-encoding:dict-values-compression-level";

/// Environment variable for selecting general compression scheme for dictionary values
pub const DICT_VALUES_COMPRESSION_ENV_VAR: &str = "LANCE_ENCODING_DICT_VALUES_COMPRESSION";
/// Environment variable for selecting compression level for dictionary values
pub const DICT_VALUES_COMPRESSION_LEVEL_ENV_VAR: &str =
    "LANCE_ENCODING_DICT_VALUES_COMPRESSION_LEVEL";

// NOTE: BLOB_META_KEY is defined in lance-core to avoid circular dependency

// Packed struct encoding metadata keys
/// Legacy metadata key for packed struct encoding (deprecated)
pub const PACKED_STRUCT_LEGACY_META_KEY: &str = "packed";
/// Metadata key for packed struct encoding
pub const PACKED_STRUCT_META_KEY: &str = "lance-encoding:packed";

// Structural encoding metadata keys
/// Metadata key for specifying structural encoding type
pub const STRUCTURAL_ENCODING_META_KEY: &str = "lance-encoding:structural-encoding";
/// Value for miniblock structural encoding
pub const STRUCTURAL_ENCODING_MINIBLOCK: &str = "miniblock";
/// Value for fullzip structural encoding
pub const STRUCTURAL_ENCODING_FULLZIP: &str = "fullzip";
/// Value for sparse structural encoding
pub const STRUCTURAL_ENCODING_SPARSE: &str = "sparse";

// Byte stream split metadata keys
/// Metadata key for byte stream split encoding configuration
pub const BSS_META_KEY: &str = "lance-encoding:bss";
/// Default BSS mode
pub const DEFAULT_BSS_MODE: &str = "auto";
