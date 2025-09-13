// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Full Query API implementation for LanceDB C bindings
//!
//! This module provides complete query operations with proper Arrow integration

use std::ffi::CStr;
use std::os::raw::{c_char, c_float};
use std::ptr;
use std::sync::Arc;

use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_data::ArrayData;
use futures::TryStreamExt;

use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{DistanceType, Table};

use crate::connection::{get_runtime, LanceDBTable};
use crate::error::{set_invalid_argument_message, set_unknown_error_message, LanceDBError};
use crate::types::LanceDBDistanceType;

/// Opaque handle to a LanceDB Query
#[repr(C)]
pub struct LanceDBQuery {
    table: Arc<Table>,
    limit: Option<usize>,
    offset: Option<usize>,
    select: Option<Select>,
    filter: Option<String>,
}

/// Opaque handle to a LanceDB VectorQuery
#[repr(C)]
pub struct LanceDBVectorQuery {
    table: Arc<Table>,
    query_vector: Vec<f32>,
    column: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    select: Option<Select>,
    filter: Option<String>,
    distance_type: Option<DistanceType>,
    nprobes: Option<usize>,
    refine_factor: Option<u32>,
    ef: Option<usize>,
}

/// Query result handle for streaming results
#[repr(C)]
pub struct LanceDBQueryResult {
    inner:
        Box<dyn futures::Stream<Item = Result<RecordBatch, lancedb::error::Error>> + Send + Unpin>,
}

/// Create a new query for the given table
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
///
/// # Returns
/// - Non-null pointer to LanceDBQuery on success
/// - Null pointer on failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_new(table: *const LanceDBTable) -> *mut LanceDBQuery {
    if table.is_null() {
        return ptr::null_mut();
    }

    let tbl = &(*table).inner;
    let query = Box::new(LanceDBQuery {
        table: Arc::new(tbl.clone()),
        limit: None,
        offset: None,
        select: None,
        filter: None,
    });

    Box::into_raw(query)
}

/// Create a vector query from table with query vector
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `vector` must be a valid pointer to array of floats
/// - `dimension` must match the actual dimension of the vector column
///
/// # Returns
/// - Non-null pointer to LanceDBVectorQuery on success
/// - Null pointer on failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_new(
    table: *const LanceDBTable,
    vector: *const c_float,
    dimension: usize,
) -> *mut LanceDBVectorQuery {
    if table.is_null() || vector.is_null() || dimension == 0 {
        return ptr::null_mut();
    }

    let tbl = &(*table).inner;
    let vec_slice = std::slice::from_raw_parts(vector, dimension);
    let vec_data: Vec<f32> = vec_slice.to_vec();

    let vector_query = Box::new(LanceDBVectorQuery {
        table: Arc::new(tbl.clone()),
        query_vector: vec_data,
        column: None,
        limit: None,
        offset: None,
        select: None,
        filter: None,
        distance_type: None,
        nprobes: None,
        refine_factor: None,
        ef: None,
    });

    Box::into_raw(vector_query)
}

/// Set limit for query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_limit(
    query: *mut LanceDBQuery,
    limit: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).limit = Some(limit);
    LanceDBError::Success
}

/// Set offset for query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_offset(
    query: *mut LanceDBQuery,
    offset: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).offset = Some(offset);
    LanceDBError::Success
}

/// Set columns to select
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - `columns` must be an array of valid null-terminated C strings
/// - `num_columns` must match the actual number of strings in the array
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_select(
    query: *mut LanceDBQuery,
    columns: *const *const c_char,
    num_columns: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() || columns.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

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

    (*query).select = Some(Select::columns(
        &column_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    LanceDBError::Success
}

/// Set WHERE filter for query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - `filter` must be a valid null-terminated C string containing SQL WHERE clause
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_where_filter(
    query: *mut LanceDBQuery,
    filter: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() || filter.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let filter_str = match CStr::from_ptr(filter).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }
    };

    (*query).filter = Some(filter_str);
    LanceDBError::Success
}

/// Set limit for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_limit(
    query: *mut LanceDBVectorQuery,
    limit: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).limit = Some(limit);
    LanceDBError::Success
}

/// Set offset for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_offset(
    query: *mut LanceDBVectorQuery,
    offset: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).offset = Some(offset);
    LanceDBError::Success
}

/// Set vector column for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `column` must be a valid null-terminated C string containing the column name
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_column(
    query: *mut LanceDBVectorQuery,
    column: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() || column.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let column_str = match CStr::from_ptr(column).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }
    };

    (*query).column = Some(column_str);
    LanceDBError::Success
}

/// Set columns to select for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `columns` must be an array of valid null-terminated C strings
/// - `num_columns` must match the actual number of strings in the array
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_select(
    query: *mut LanceDBVectorQuery,
    columns: *const *const c_char,
    num_columns: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() || columns.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

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

    (*query).select = Some(Select::columns(
        &column_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    ));
    LanceDBError::Success
}

/// Set WHERE filter for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `filter` must be a valid null-terminated C string containing SQL WHERE clause
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_where_filter(
    query: *mut LanceDBVectorQuery,
    filter: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() || filter.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let filter_str = match CStr::from_ptr(filter).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        }
    };

    (*query).filter = Some(filter_str);
    LanceDBError::Success
}

/// Set distance type for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_distance_type(
    query: *mut LanceDBVectorQuery,
    distance_type: LanceDBDistanceType,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).distance_type = Some(distance_type.into());
    LanceDBError::Success
}

/// Set number of probes for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_nprobes(
    query: *mut LanceDBVectorQuery,
    nprobes: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).nprobes = Some(nprobes);
    LanceDBError::Success
}

/// Set refine factor for vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_refine_factor(
    query: *mut LanceDBVectorQuery,
    refine_factor: u32,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).refine_factor = Some(refine_factor);
    LanceDBError::Success
}

/// Set ef parameter for HNSW vector query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `error_message` can be NULL to ignore detailed error messages
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_ef(
    query: *mut LanceDBVectorQuery,
    ef: usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if query.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let _ = error_message; // No errors possible in this function
    (*query).ef = Some(ef);
    LanceDBError::Success
}

/// Execute query and return streaming result
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - This function consumes the query pointer; do not use it after calling
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_execute(
    query: *mut LanceDBQuery,
) -> *mut LanceDBQueryResult {
    if query.is_null() {
        return ptr::null_mut();
    }

    let query_box = Box::from_raw(query);
    let runtime = get_runtime();

    match runtime.block_on(async {
        let mut rust_query = query_box.table.query();

        if let Some(limit) = query_box.limit {
            rust_query = rust_query.limit(limit);
        }
        if let Some(offset) = query_box.offset {
            rust_query = rust_query.offset(offset);
        }
        if let Some(ref select) = query_box.select {
            rust_query = rust_query.select(select.clone());
        }
        if let Some(ref filter) = query_box.filter {
            rust_query = rust_query.only_if(filter);
        }

        rust_query.execute().await
    }) {
        Ok(stream) => {
            let result = Box::new(LanceDBQueryResult {
                inner: Box::new(stream),
            });
            Box::into_raw(result)
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Execute vector query and return streaming result
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - This function consumes the query pointer; do not use it after calling
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_execute(
    query: *mut LanceDBVectorQuery,
) -> *mut LanceDBQueryResult {
    if query.is_null() {
        return ptr::null_mut();
    }

    let query_box = Box::from_raw(query);
    let runtime = get_runtime();

    match runtime.block_on(async {
        let mut rust_query = match query_box
            .table
            .query()
            .nearest_to(query_box.query_vector.clone())
        {
            Ok(q) => q,
            Err(e) => return Err(e),
        };

        if let Some(ref column) = query_box.column {
            rust_query = rust_query.column(column);
        }
        if let Some(limit) = query_box.limit {
            rust_query = rust_query.limit(limit);
        }
        if let Some(offset) = query_box.offset {
            rust_query = rust_query.offset(offset);
        }
        if let Some(ref select) = query_box.select {
            rust_query = rust_query.select(select.clone());
        }
        if let Some(ref filter) = query_box.filter {
            rust_query = rust_query.only_if(filter);
        }
        if let Some(distance_type) = query_box.distance_type {
            rust_query = rust_query.distance_type(distance_type);
        }
        if let Some(nprobes) = query_box.nprobes {
            rust_query = rust_query.nprobes(nprobes);
        }
        if let Some(refine_factor) = query_box.refine_factor {
            rust_query = rust_query.refine_factor(refine_factor);
        }
        if let Some(ef) = query_box.ef {
            rust_query = rust_query.ef(ef);
        }

        rust_query.execute().await
    }) {
        Ok(stream) => {
            let result = Box::new(LanceDBQueryResult {
                inner: Box::new(stream),
            });
            Box::into_raw(result)
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Convert query result to Arrow RecordBatch array
///
/// # Safety
/// - `result` must be a valid pointer returned from query execution functions
/// - `batches_out`, `schema_out`, `count_out` must be valid pointers to receive results
/// - `error_message` can be NULL to ignore detailed error messages
/// - This function consumes the result pointer; do not use it after calling
/// - Caller must free returned arrays with `lancedb_free_arrow_arrays` and schema with `lancedb_free_arrow_schema`
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_result_to_arrow(
    result: *mut LanceDBQueryResult,
    batches_out: *mut *mut *mut arrow_array::ffi::FFI_ArrowArray,
    schema_out: *mut *mut arrow_schema::ffi::FFI_ArrowSchema,
    count_out: *mut usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if result.is_null() || batches_out.is_null() || schema_out.is_null() || count_out.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let result_box = Box::from_raw(result);
    let runtime = get_runtime();

    match runtime.block_on(async {
        let batches: Vec<RecordBatch> = result_box.inner.try_collect().await?;
        Ok::<Vec<RecordBatch>, lancedb::error::Error>(batches)
    }) {
        Ok(batches) => {
            let count = batches.len();
            *count_out = count;

            if count == 0 {
                *batches_out = ptr::null_mut();
                *schema_out = ptr::null_mut();
                return LanceDBError::Success;
            }

            // Get schema from first batch (all batches have same schema)
            let schema = batches[0].schema();
            let ffi_schema = match arrow_schema::ffi::FFI_ArrowSchema::try_from(&*schema) {
                Ok(schema) => Box::new(schema),
                Err(err) => {
                    if !error_message.is_null() {
                        let error_str = format!("Failed to convert Arrow schema to C ABI: {}", err);
                        if let Ok(c_str) = std::ffi::CString::new(error_str) {
                            *error_message = c_str.into_raw();
                        }
                    }
                    return LanceDBError::Unknown;
                }
            };
            *schema_out = Box::into_raw(ffi_schema);

            // Allocate array for Arrow C ABI array structures
            let arrays_ptr =
                libc::malloc(count * std::mem::size_of::<*mut arrow_array::ffi::FFI_ArrowArray>())
                    as *mut *mut arrow_array::ffi::FFI_ArrowArray;
            if arrays_ptr.is_null() {
                // Clean up schema on allocation failure
                let _ = Box::from_raw(*schema_out);
                *schema_out = ptr::null_mut();
                set_unknown_error_message(error_message);
                return LanceDBError::Unknown;
            }

            for (i, batch) in batches.into_iter().enumerate() {
                // Convert RecordBatch to StructArray first, then to FFI_ArrowArray
                let struct_array: StructArray = batch.clone().into();
                let array_data: ArrayData = struct_array.into_data();
                let ffi_array = Box::new(FFI_ArrowArray::new(&array_data));
                *arrays_ptr.add(i) = Box::into_raw(ffi_array);
            }

            *batches_out = arrays_ptr;
            LanceDBError::Success
        }
        Err(_) => {
            set_unknown_error_message(error_message);
            LanceDBError::Unknown
        }
    }
}

/// Free a Query
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_query_new`
/// - `query` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_free(query: *mut LanceDBQuery) {
    if !query.is_null() {
        let _ = Box::from_raw(query);
    }
}

/// Free a VectorQuery
///
/// # Safety
/// - `query` must be a valid pointer returned from `lancedb_vector_query_new`
/// - `query` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_vector_query_free(query: *mut LanceDBVectorQuery) {
    if !query.is_null() {
        let _ = Box::from_raw(query);
    }
}

/// Free a QueryResult
///
/// # Safety
/// - `result` must be a valid pointer returned from query execution functions
/// - `result` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_query_result_free(result: *mut LanceDBQueryResult) {
    if !result.is_null() {
        let _ = Box::from_raw(result);
    }
}

/// Free Arrow arrays returned by query result functions
///
/// # Safety
/// - `batches` must be a pointer returned by `lancedb_query_result_to_arrow`
/// - `count` must match the count returned by `lancedb_query_result_to_arrow`
/// - `batches` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_free_arrow_arrays(
    batches: *mut *mut arrow_array::ffi::FFI_ArrowArray,
    count: usize,
) {
    if !batches.is_null() {
        for i in 0..count {
            let batch_ptr = *batches.add(i);
            if !batch_ptr.is_null() {
                let _ = Box::from_raw(batch_ptr);
            }
        }
        libc::free(batches as *mut libc::c_void);
    }
}
