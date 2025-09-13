// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Full Index API implementation for LanceDB C bindings
//!
//! This module provides complete index management operations

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_float, c_int};
use std::ptr;

use lancedb::index::scalar::{
    BTreeIndexBuilder, BitmapIndexBuilder, FtsIndexBuilder, LabelListIndexBuilder,
};
use lancedb::index::vector::{
    IvfFlatIndexBuilder, IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder,
};
use lancedb::index::Index;

use crate::connection::{get_runtime, LanceDBTable};
use crate::error::{
    handle_error, set_invalid_argument_message, set_unknown_error_message, LanceDBError,
};
use crate::types::LanceDBDistanceType;

/// Index type enum for C API
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum LanceDBIndexType {
    Auto = 0,
    BTree = 1,
    Bitmap = 2,
    LabelList = 3,
    FTS = 4,
    IvfFlat = 5,
    IvfPq = 6,
    IvfHnswPq = 7,
    IvfHnswSq = 8,
}

/// Configuration for vector indices
#[repr(C)]
#[derive(Clone)]
pub struct LanceDBVectorIndexConfig {
    pub num_partitions: c_int, // Number of partitions for IVF indices (-1 = auto)
    pub num_sub_vectors: c_int, // Number of sub-vectors for PQ indices (-1 = auto)
    pub max_iterations: c_int, // Maximum training iterations (-1 = default)
    pub sample_rate: c_float,  // Sampling rate for training (0.0 = default)
    pub distance_type: LanceDBDistanceType, // Distance metric
    pub accelerator: *const c_char, // GPU accelerator ("cuda", "mps", or NULL for CPU)
    pub replace: c_int,        // Replace existing index (1 = true, 0 = false)
}

impl Default for LanceDBVectorIndexConfig {
    fn default() -> Self {
        Self {
            num_partitions: -1,
            num_sub_vectors: -1,
            max_iterations: -1,
            sample_rate: 0.0,
            distance_type: LanceDBDistanceType::L2,
            accelerator: ptr::null(),
            replace: 1,
        }
    }
}

/// Configuration for scalar indices
#[repr(C)]
#[derive(Clone)]
pub struct LanceDBScalarIndexConfig {
    pub replace: c_int,                 // Replace existing index (1 = true, 0 = false)
    pub force_update_statistics: c_int, // Force update statistics (1 = true, 0 = false)
}

impl Default for LanceDBScalarIndexConfig {
    fn default() -> Self {
        Self {
            replace: 1,
            force_update_statistics: 0,
        }
    }
}

/// Configuration for full-text search indices
#[repr(C)]
#[derive(Clone)]
pub struct LanceDBFtsIndexConfig {
    pub base_tokenizer: *const c_char, // Base tokenizer ("simple", "whitespace", etc.)
    pub language: *const c_char,       // Language for stemming ("en", "es", etc.)
    pub max_tokens: c_int,             // Maximum tokens per document (-1 = no limit)
    pub lowercase: c_int,              // Convert to lowercase (1 = true, 0 = false)
    pub stem: c_int,                   // Apply stemming (1 = true, 0 = false)
    pub remove_stop_words: c_int,      // Remove stop words (1 = true, 0 = false)
    pub ascii_folding: c_int,          // Apply ASCII folding (1 = true, 0 = false)
    pub replace: c_int,                // Replace existing index (1 = true, 0 = false)
}

impl Default for LanceDBFtsIndexConfig {
    fn default() -> Self {
        Self {
            base_tokenizer: ptr::null(),
            language: ptr::null(),
            max_tokens: -1,
            lowercase: 1,
            stem: 0,
            remove_stop_words: 0,
            ascii_folding: 0,
            replace: 1,
        }
    }
}

/// Configuration for table optimize action
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum LanceDBOptimizeType {
    All = 0,
    Compat = 1,
    Prune = 2,
    Index = 3,
}

/// Create a vector index on table columns
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `columns` must be an array of valid null-terminated C strings
/// - `num_columns` must match the actual number of columns in the array
/// - `config` can be NULL for defaults
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_create_vector_index(
    table: *const LanceDBTable,
    columns: *const *const c_char,
    num_columns: usize,
    index_type: LanceDBIndexType,
    config: *const LanceDBVectorIndexConfig,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() || columns.is_null() || num_columns == 0 {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    // Extract column names
    let mut column_names = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        let col_ptr = *columns.add(i);
        if col_ptr.is_null() {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }

        let col_str = match CStr::from_ptr(col_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                set_invalid_argument_message(error_message);
                return LanceDBError::InvalidArgument;
            }
        };
        column_names.push(col_str);
    }

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    // Use default config if none provided
    let cfg = if config.is_null() {
        LanceDBVectorIndexConfig::default()
    } else {
        (*config).clone()
    };

    let index = match index_type {
        LanceDBIndexType::Auto => Index::Auto,
        LanceDBIndexType::IvfFlat => {
            let mut builder = IvfFlatIndexBuilder::default();
            if cfg.num_partitions > 0 {
                builder = builder.num_partitions(cfg.num_partitions as u32);
            }
            if cfg.max_iterations > 0 {
                builder = builder.max_iterations(cfg.max_iterations as u32);
            }
            if cfg.sample_rate > 0.0 {
                builder = builder.sample_rate(cfg.sample_rate as u32);
            }
            builder = builder.distance_type(cfg.distance_type.into());

            // Note: accelerator configuration is simplified
            let _ = cfg.accelerator;
            Index::IvfFlat(builder)
        }
        LanceDBIndexType::IvfPq => {
            let mut builder = IvfPqIndexBuilder::default();
            if cfg.num_partitions > 0 {
                builder = builder.num_partitions(cfg.num_partitions as u32);
            }
            if cfg.num_sub_vectors > 0 {
                builder = builder.num_sub_vectors(cfg.num_sub_vectors as u32);
            }
            if cfg.max_iterations > 0 {
                builder = builder.max_iterations(cfg.max_iterations as u32);
            }
            if cfg.sample_rate > 0.0 {
                builder = builder.sample_rate(cfg.sample_rate as u32);
            }
            builder = builder.distance_type(cfg.distance_type.into());

            // Note: accelerator configuration is simplified
            let _ = cfg.accelerator;
            Index::IvfPq(builder)
        }
        LanceDBIndexType::IvfHnswPq => {
            let mut builder = IvfHnswPqIndexBuilder::default();
            if cfg.num_partitions > 0 {
                builder = builder.num_partitions(cfg.num_partitions as u32);
            }
            if cfg.num_sub_vectors > 0 {
                builder = builder.num_sub_vectors(cfg.num_sub_vectors as u32);
            }
            if cfg.max_iterations > 0 {
                builder = builder.max_iterations(cfg.max_iterations as u32);
            }
            if cfg.sample_rate > 0.0 {
                builder = builder.sample_rate(cfg.sample_rate as u32);
            }
            builder = builder.distance_type(cfg.distance_type.into());

            // Note: accelerator configuration is simplified
            let _ = cfg.accelerator;
            Index::IvfHnswPq(builder)
        }
        LanceDBIndexType::IvfHnswSq => {
            let mut builder = IvfHnswSqIndexBuilder::default();
            if cfg.num_partitions > 0 {
                builder = builder.num_partitions(cfg.num_partitions as u32);
            }
            if cfg.max_iterations > 0 {
                builder = builder.max_iterations(cfg.max_iterations as u32);
            }
            if cfg.sample_rate > 0.0 {
                builder = builder.sample_rate(cfg.sample_rate as u32);
            }
            builder = builder.distance_type(cfg.distance_type.into());

            // Note: accelerator configuration is simplified
            let _ = cfg.accelerator;
            Index::IvfHnswSq(builder)
        }
        _ => {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }
    };

    match runtime.block_on(async {
        let mut index_builder = tbl.create_index(&column_names, index);
        if cfg.replace == 0 {
            index_builder = index_builder.replace(false);
        }
        index_builder.execute().await
    }) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Create a scalar index on table columns
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `columns` must be an array of valid null-terminated C strings
/// - `num_columns` must match the actual number of columns in the array
/// - `config` can be NULL for defaults
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_create_scalar_index(
    table: *const LanceDBTable,
    columns: *const *const c_char,
    num_columns: usize,
    index_type: LanceDBIndexType,
    config: *const LanceDBScalarIndexConfig,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() || columns.is_null() || num_columns == 0 {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    // Extract column names
    let mut column_names = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        let col_ptr = *columns.add(i);
        if col_ptr.is_null() {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }

        let col_str = match CStr::from_ptr(col_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                set_invalid_argument_message(error_message);
                return LanceDBError::InvalidArgument;
            }
        };
        column_names.push(col_str);
    }

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    // Use default config if none provided
    let cfg = if config.is_null() {
        LanceDBScalarIndexConfig::default()
    } else {
        (*config).clone()
    };

    let index = match index_type {
        LanceDBIndexType::BTree => {
            let builder = BTreeIndexBuilder::default();
            // Note: force_update_statistics is not available in current API
            let _ = cfg.force_update_statistics;
            Index::BTree(builder)
        }
        LanceDBIndexType::Bitmap => Index::Bitmap(BitmapIndexBuilder::default()),
        LanceDBIndexType::LabelList => Index::LabelList(LabelListIndexBuilder::default()),
        _ => {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }
    };

    match runtime.block_on(async {
        let mut index_builder = tbl.create_index(&column_names, index);
        if cfg.replace == 0 {
            index_builder = index_builder.replace(false);
        }
        index_builder.execute().await
    }) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Create a full-text search index on table columns
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `columns` must be an array of valid null-terminated C strings
/// - `num_columns` must match the actual number of columns in the array
/// - `config` can be NULL for defaults
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_create_fts_index(
    table: *const LanceDBTable,
    columns: *const *const c_char,
    num_columns: usize,
    config: *const LanceDBFtsIndexConfig,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() || columns.is_null() || num_columns == 0 {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    // Extract column names
    let mut column_names = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        let col_ptr = *columns.add(i);
        if col_ptr.is_null() {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }

        let col_str = match CStr::from_ptr(col_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                set_invalid_argument_message(error_message);
                return LanceDBError::InvalidArgument;
            }
        };
        column_names.push(col_str);
    }

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    // Use default config if none provided
    let cfg = if config.is_null() {
        LanceDBFtsIndexConfig::default()
    } else {
        (*config).clone()
    };

    let mut builder = FtsIndexBuilder::default();

    if !cfg.base_tokenizer.is_null() {
        if let Ok(tokenizer_str) = CStr::from_ptr(cfg.base_tokenizer).to_str() {
            builder = builder.base_tokenizer(tokenizer_str.to_string());
        }
    }

    if !cfg.language.is_null() {
        if let Ok(lang_str) = CStr::from_ptr(cfg.language).to_str() {
            match builder.language(lang_str) {
                Ok(new_builder) => builder = new_builder,
                Err(_) => {
                    set_invalid_argument_message(error_message);
                    return LanceDBError::InvalidArgument;
                }
            }
        }
    }

    // Note: Some FTS options are simplified in current implementation
    let _ = cfg.max_tokens;

    builder = builder
        .lower_case(cfg.lowercase != 0)
        .stem(cfg.stem != 0)
        .remove_stop_words(cfg.remove_stop_words != 0)
        .ascii_folding(cfg.ascii_folding != 0);

    let index = Index::FTS(builder);

    match runtime.block_on(async {
        let mut index_builder = tbl.create_index(&column_names, index);
        if cfg.replace == 0 {
            index_builder = index_builder.replace(false);
        }
        index_builder.execute().await
    }) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// List all indices on the table
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `indices_out` must be a valid pointer to receive the array of index info strings
/// - `count_out` must be a valid pointer to receive the count
/// - `error_message` can be NULL to ignore detailed error messages
/// - The caller is responsible for freeing the returned strings and array
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_list_indices(
    table: *const LanceDBTable,
    indices_out: *mut *mut *mut c_char,
    count_out: *mut usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() || indices_out.is_null() || count_out.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    match runtime.block_on(tbl.list_indices()) {
        Ok(indices) => {
            let count = indices.len();
            *count_out = count;

            if count == 0 {
                *indices_out = ptr::null_mut();
                return LanceDBError::Success;
            }

            // Allocate array of string pointers
            let indices_array =
                libc::malloc(count * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
            if indices_array.is_null() {
                set_unknown_error_message(error_message);
                return LanceDBError::Unknown;
            }

            // Convert each index info to string representation
            for (i, index_info) in indices.into_iter().enumerate() {
                let info_str = index_info.name;

                match CString::new(info_str) {
                    Ok(c_str) => {
                        *indices_array.add(i) = c_str.into_raw();
                    }
                    Err(_) => {
                        // Clean up already allocated strings
                        for j in 0..i {
                            let _ = CString::from_raw(*indices_array.add(j));
                        }
                        libc::free(indices_array as *mut libc::c_void);
                        set_unknown_error_message(error_message);
                        return LanceDBError::Unknown;
                    }
                }
            }

            *indices_out = indices_array;
            LanceDBError::Success
        }
        Err(e) => handle_error(&e, error_message),
    }
}

/// Drop an index
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `index_name` must be a valid null-terminated C string
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_drop_index(
    table: *const LanceDBTable,
    index_name: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() || index_name.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(index_name_str) = CStr::from_ptr(index_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    match runtime.block_on(tbl.drop_index(index_name_str)) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Optimize table (rebuild indices and compact files)
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_optimize(
    table: *const LanceDBTable,
    optimize_type: LanceDBOptimizeType,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if table.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let tbl = &(*table).inner;
    let runtime = get_runtime();

    use lancedb::table::{OptimizeAction, OptimizeOptions};

    let action = match optimize_type {
        LanceDBOptimizeType::All => OptimizeAction::All,
        LanceDBOptimizeType::Compat => OptimizeAction::Compact {
            options: Default::default(),
            remap_options: None,
        },
        LanceDBOptimizeType::Prune => OptimizeAction::Prune {
            older_than: None,
            delete_unverified: None,
            error_if_tagged_old_versions: None,
        },
        LanceDBOptimizeType::Index => OptimizeAction::Index(OptimizeOptions::default()),
    };

    match runtime.block_on(tbl.optimize(action)) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Free index list array returned by lancedb_table_list_indices
///
/// # Safety
/// - `indices` must be a pointer returned by `lancedb_table_list_indices`
/// - `count` must match the count returned by `lancedb_table_list_indices`
#[no_mangle]
pub unsafe extern "C" fn lancedb_free_index_list(indices: *mut *mut c_char, count: usize) {
    if !indices.is_null() {
        for i in 0..count {
            let index_ptr = *indices.add(i);
            if !index_ptr.is_null() {
                let _ = CString::from_raw(index_ptr);
            }
        }
        libc::free(indices as *mut libc::c_void);
    }
}
