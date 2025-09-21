// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::os::raw::{c_char, c_float, c_int, c_void};

/// Distance metrics for vector search
#[repr(C)]
pub enum DistanceType {
    L2 = 0,
    Cosine = 1,
    Dot = 2,
    Hamming = 3,
}

impl From<DistanceType> for lancedb::DistanceType {
    fn from(dt: DistanceType) -> Self {
        match dt {
            DistanceType::L2 => Self::L2,
            DistanceType::Cosine => Self::Cosine,
            DistanceType::Dot => Self::Dot,
            DistanceType::Hamming => Self::Hamming,
        }
    }
}

/// Vector index types
#[repr(C)]
pub enum IndexType {
    Auto = 0,
    IvfPq = 1,
    IvfFlat = 2,
    HnswPq = 3,
    HnswSq = 4,
    BTree = 5,
    Bitmap = 6,
    LabelList = 7,
    Fts = 8,
}

/// Query execution options
#[repr(C)]
pub struct QueryOptions {
    pub max_results: c_int,
    pub use_full_precision: bool,
    pub bypass_vector_index: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            max_results: 10,
            use_full_precision: false,
            bypass_vector_index: false,
        }
    }
}

/// Table creation options
#[repr(C)]
pub struct CreateTableOptions {
    pub mode: TableWriteMode,
    pub exist_ok: bool,
    pub storage_options_json: *const c_char,
}

/// Table write modes
#[repr(C)]
pub enum TableWriteMode {
    Create = 0,
    Overwrite = 1,
    Append = 2,
}

/// Vector column options
#[repr(C)]
pub struct VectorColumnOptions {
    pub dimension: c_int,
    pub data_type: VectorDataType,
}

/// Vector data types
#[repr(C)]
pub enum VectorDataType {
    Float16 = 0,
    Float32 = 1,
    Float64 = 2,
}

/// Index creation options
#[repr(C)]
pub struct IndexOptions {
    pub index_type: IndexType,
    pub replace: bool,
    pub num_partitions: c_int,
    pub num_sub_vectors: c_int,
    pub accelerator: *const c_char, // "cuda", "mps", or null for CPU
    pub distance_type: DistanceType,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            index_type: IndexType::Auto,
            replace: false,
            num_partitions: 256,
            num_sub_vectors: 16,
            accelerator: std::ptr::null(),
            distance_type: DistanceType::L2,
        }
    }
}

/// Statistics for table operations
#[repr(C)]
pub struct TableStats {
    pub num_rows: c_int,
    pub num_columns: c_int,
    pub size_bytes: i64,
    pub num_fragments: c_int,
}

/// Version information
#[repr(C)]
pub struct VersionInfo {
    pub version: c_int,
    pub timestamp: i64, // Unix timestamp
    pub metadata_json: *mut c_char,
}

/// Free a VersionInfo structure
#[no_mangle]
pub extern "C" fn lancedb_version_info_free(version: *mut VersionInfo) {
    if version.is_null() {
        return;
    }

    unsafe {
        let version = Box::from_raw(version);
        if !version.metadata_json.is_null() {
            let _ = std::ffi::CString::from_raw(version.metadata_json);
        }
    }
}
