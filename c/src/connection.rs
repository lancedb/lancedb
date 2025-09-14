// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Connection-related FFI functions for LanceDB C bindings

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::Schema;
use lancedb::connection::{connect, ConnectBuilder, Connection};
use lancedb::database::{CreateNamespaceRequest, DropNamespaceRequest, ListNamespacesRequest};
use lancedb::Table;

use crate::error::{
    handle_error, set_invalid_argument_message, set_unknown_error_message, LanceDBError,
};
use crate::types::LanceDBRecordBatchReader;

/// Opaque handle to a ConnectBuilder
#[repr(C)]
pub struct LanceDBConnectBuilder {
    inner: Box<ConnectBuilder>,
}

/// Opaque handle to a Connection
#[repr(C)]
pub struct LanceDBConnection {
    pub(crate) inner: Connection,
}

/// Opaque handle to a Table
#[repr(C)]
pub struct LanceDBTable {
    pub(crate) inner: Table,
}

/// Runtime to handle async operations
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub(crate) fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"))
}

/// Create a ConnectBuilder for the given URI
///
/// # Safety
/// - `uri` must be a valid null-terminated C string
/// - The returned pointer must be freed with `lancedb_connect_builder_free`
///
/// # Returns
/// - Non-null pointer to LanceDBConnectBuilder on success
/// - Null pointer on failure (e.g., invalid UTF-8 in uri)
#[no_mangle]
pub unsafe extern "C" fn lancedb_connect(uri: *const c_char) -> *mut LanceDBConnectBuilder {
    if uri.is_null() {
        return ptr::null_mut();
    }

    let Ok(str_uri) = CStr::from_ptr(uri).to_str() else {
        return ptr::null_mut();
    };

    let builder = connect(str_uri);
    let boxed_builder = Box::new(LanceDBConnectBuilder {
        inner: Box::new(builder),
    });

    Box::into_raw(boxed_builder)
}

/// Execute the connection and return a Connection handle
///
/// # Safety
/// - `builder` must be a valid pointer returned from `lancedb_connect`
/// - `builder` will be consumed and must not be used after calling this function
///
/// # Returns
/// - Non-null pointer to LanceDBConnection on success
/// - Null pointer on failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connect_builder_execute(
    builder: *mut LanceDBConnectBuilder,
) -> *mut LanceDBConnection {
    if builder.is_null() {
        return ptr::null_mut();
    }

    let builder_box = Box::from_raw(builder);
    let connect_builder = *builder_box.inner;

    let runtime = get_runtime();
    match runtime.block_on(connect_builder.execute()) {
        Ok(connection) => {
            let boxed_connection = Box::new(LanceDBConnection { inner: connection });
            Box::into_raw(boxed_connection)
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Free a ConnectBuilder
///
/// # Safety
/// - `builder` must be a valid pointer returned from `lancedb_connect`
/// - `builder` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_connect_builder_free(builder: *mut LanceDBConnectBuilder) {
    if !builder.is_null() {
        let _ = Box::from_raw(builder);
    }
}

/// Get the URI of the connection
///
/// # Safety
/// - `connection` must be a valid pointer returned from `lancedb_connect_builder_execute`
/// - The returned string is valid until the connection is freed
/// - The caller must not free the returned string
///
/// # Returns
/// - Non-null pointer to null-terminated C string on success
/// - Null pointer on failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_uri(
    connection: *const LanceDBConnection,
) -> *const c_char {
    if connection.is_null() {
        return ptr::null();
    }

    let conn = &(*connection).inner;
    conn.uri().as_ptr() as *const c_char
}

/// Create a new table with Arrow schema and data
///
/// # Safety
/// - `connection` must be a valid pointer returned from `lancedb_connect_builder_execute`
/// - `table_name` must be a valid null-terminated C string
/// - `schema_ptr` must be a valid pointer to Arrow C ABI schema
/// - `reader` must be a valid pointer to LanceDBRecordBatchReader or NULL for empty table
/// - `table_out` must be a valid pointer to receive the created table
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_create(
    connection: *const LanceDBConnection,
    table_name: *const c_char,
    schema_ptr: *const arrow_schema::ffi::FFI_ArrowSchema,
    reader: *mut LanceDBRecordBatchReader,
    table_out: *mut *mut LanceDBTable,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || table_name.is_null() || schema_ptr.is_null() || table_out.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(table_name_str) = CStr::from_ptr(table_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    match runtime.block_on(async {
        // Import schema from Arrow C ABI
        let schema = match Schema::try_from(&*schema_ptr) {
            Ok(s) => Arc::new(s),
            Err(_) => {
                return Err(lancedb::error::Error::InvalidInput {
                    message: "Failed to import Arrow schema from C ABI".to_string(),
                })
            }
        };

        let batch_reader: Box<dyn RecordBatchReader + Send> = if reader.is_null() {
            // Create empty reader with the schema
            let empty_batches = RecordBatchIterator::new(
                std::iter::empty::<Result<RecordBatch, arrow::error::ArrowError>>(),
                schema.clone(),
            );
            Box::new(empty_batches)
        } else {
            // Take ownership of the reader
            let reader_box = Box::from_raw(reader);
            reader_box.into_inner()
        };

        conn.create_table(table_name_str, batch_reader)
            .execute()
            .await
    }) {
        Ok(table) => {
            let boxed_table = Box::new(LanceDBTable { inner: table });
            *table_out = Box::into_raw(boxed_table);
            LanceDBError::Success
        }
        Err(e) => handle_error(&e, error_message),
    }
}

/// Get table names from the connection
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `names_out` must be a valid pointer to receive the array of string pointers
/// - `count_out` must be a valid pointer to receive the count
/// - `error_message` can be NULL to ignore detailed error messages
/// - The caller is responsible for freeing the returned strings and array
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_table_names(
    connection: *const LanceDBConnection,
    names_out: *mut *mut *mut c_char,
    count_out: *mut usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || names_out.is_null() || count_out.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    match runtime.block_on(conn.table_names().execute()) {
        Ok(names) => {
            let count = names.len();
            *count_out = count;

            if count == 0 {
                *names_out = ptr::null_mut();
                return LanceDBError::Success;
            }

            // Allocate array of string pointers
            let names_array =
                libc::malloc(count * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
            if names_array.is_null() {
                set_unknown_error_message(error_message);
                return LanceDBError::Unknown;
            }

            // Convert each string and store pointer
            for (i, name) in names.into_iter().enumerate() {
                match CString::new(name) {
                    Ok(c_str) => {
                        *names_array.add(i) = c_str.into_raw();
                    }
                    Err(_) => {
                        // Clean up already allocated strings
                        for j in 0..i {
                            let _ = CString::from_raw(*names_array.add(j));
                        }
                        libc::free(names_array as *mut libc::c_void);
                        set_unknown_error_message(error_message);
                        return LanceDBError::Unknown;
                    }
                }
            }

            *names_out = names_array;
            LanceDBError::Success
        }
        Err(e) => handle_error(&e, error_message),
    }
}

/// Free table names array returned by lancedb_connection_table_names
///
/// # Safety
/// - `names` must be a pointer returned by `lancedb_connection_table_names`
/// - `count` must match the count returned by `lancedb_connection_table_names`
#[no_mangle]
pub unsafe extern "C" fn lancedb_free_table_names(names: *mut *mut c_char, count: usize) {
    if !names.is_null() {
        for i in 0..count {
            let name_ptr = *names.add(i);
            if !name_ptr.is_null() {
                let _ = CString::from_raw(name_ptr);
            }
        }
        libc::free(names as *mut libc::c_void);
    }
}

/// Free namespace list array returned by lancedb_connection_list_namespaces
///
/// # Safety
/// - `namespaces` must be a pointer returned by `lancedb_connection_list_namespaces`
/// - `count` must match the count returned by `lancedb_connection_list_namespaces`
#[no_mangle]
pub unsafe extern "C" fn lancedb_free_namespace_list(namespaces: *mut *mut c_char, count: usize) {
    if !namespaces.is_null() {
        for i in 0..count {
            let namespace_ptr = *namespaces.add(i);
            if !namespace_ptr.is_null() {
                let _ = CString::from_raw(namespace_ptr);
            }
        }
        libc::free(namespaces as *mut libc::c_void);
    }
}

/// Open an existing table
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `table_name` must be a valid null-terminated C string
///
/// # Returns
/// - Non-null pointer to LanceDBTable on success
/// - Null pointer on failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_open_table(
    connection: *const LanceDBConnection,
    table_name: *const c_char,
) -> *mut LanceDBTable {
    if connection.is_null() || table_name.is_null() {
        return ptr::null_mut();
    }

    let Ok(table_name_str) = CStr::from_ptr(table_name).to_str() else {
        return ptr::null_mut();
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    match runtime.block_on(conn.open_table(table_name_str).execute()) {
        Ok(table) => {
            let boxed_table = Box::new(LanceDBTable { inner: table });
            Box::into_raw(boxed_table)
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Drop a table from the database
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `table_name` must be a valid null-terminated C string
/// - `namespace` can be NULL for no namespace, or a valid null-terminated C string
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_drop_table(
    connection: *const LanceDBConnection,
    table_name: *const c_char,
    namespace: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || table_name.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(table_name_str) = CStr::from_ptr(table_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let result = if namespace.is_null() {
        runtime.block_on(conn.drop_table(table_name_str, &[]))
    } else {
        let Ok(namespace_str) = CStr::from_ptr(namespace).to_str() else {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        };
        runtime.block_on(conn.drop_table(table_name_str, &[String::from(namespace_str)]))
    };

    match result {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Rename a table in the database
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `old_name` and `new_name` must be valid null-terminated C strings
/// - `cur_namespace` and `new_namespace` can be NULL for no namespace, or valid null-terminated C strings
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_rename_table(
    connection: *const LanceDBConnection,
    old_name: *const c_char,
    new_name: *const c_char,
    cur_namespace: *const c_char,
    new_namespace: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || old_name.is_null() || new_name.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(old_name_str) = CStr::from_ptr(old_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let Ok(new_name_str) = CStr::from_ptr(new_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let cur_namespace_vec = if cur_namespace.is_null() {
        Vec::new()
    } else {
        let Ok(cur_namespace_str) = CStr::from_ptr(cur_namespace).to_str() else {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        };
        vec![String::from(cur_namespace_str)]
    };

    let new_namespace_vec = if new_namespace.is_null() {
        Vec::new()
    } else {
        let Ok(new_namespace_str) = CStr::from_ptr(new_namespace).to_str() else {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        };
        vec![String::from(new_namespace_str)]
    };

    match runtime.block_on(conn.rename_table(
        old_name_str,
        new_name_str,
        &cur_namespace_vec,
        &new_namespace_vec,
    )) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Drop all tables in the database
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `namespace` can be NULL for no namespace, or a valid null-terminated C string
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_drop_all_tables(
    connection: *const LanceDBConnection,
    namespace: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let result = if namespace.is_null() {
        runtime.block_on(conn.drop_all_tables(&[]))
    } else {
        let Ok(namespace_str) = CStr::from_ptr(namespace).to_str() else {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        };
        runtime.block_on(conn.drop_all_tables(&[String::from(namespace_str)]))
    };

    match result {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Create a new namespace
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `namespace_name` must be a valid null-terminated C string
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_create_namespace(
    connection: *const LanceDBConnection,
    namespace_name: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || namespace_name.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(namespace_str) = CStr::from_ptr(namespace_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let request = CreateNamespaceRequest {
        namespace: vec![namespace_str.to_string()],
    };
    match runtime.block_on(conn.create_namespace(request)) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// Drop a namespace
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `namespace_name` must be a valid null-terminated C string
/// - `error_message` can be NULL to ignore detailed error messages
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_drop_namespace(
    connection: *const LanceDBConnection,
    namespace_name: *const c_char,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || namespace_name.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let Ok(namespace_str) = CStr::from_ptr(namespace_name).to_str() else {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let request = DropNamespaceRequest {
        namespace: vec![namespace_str.to_string()],
    };
    match runtime.block_on(conn.drop_namespace(request)) {
        Ok(_) => LanceDBError::Success,
        Err(e) => handle_error(&e, error_message),
    }
}

/// List all namespaces in the database
///
/// # Safety
/// - `connection` must be a valid pointer
/// - `namespace_parent` can be NULL for root namespace, or a valid null-terminated C string
/// - `namespaces_out` must be a valid pointer to receive the array of string pointers
/// - `count_out` must be a valid pointer to receive the count
/// - `error_message` can be NULL to ignore detailed error messages
/// - The caller is responsible for freeing the returned strings and array
///
/// # Returns
/// - Error code indicating success or failure
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_list_namespaces(
    connection: *const LanceDBConnection,
    namespace_parent: *const c_char,
    namespaces_out: *mut *mut *mut c_char,
    count_out: *mut usize,
    error_message: *mut *mut c_char,
) -> LanceDBError {
    if connection.is_null() || namespaces_out.is_null() || count_out.is_null() {
        set_invalid_argument_message(error_message);
        return LanceDBError::InvalidArgument;
    }

    let parent_namespace = if namespace_parent.is_null() {
        Vec::new() // Root namespace
    } else {
        let Ok(parent_str) = CStr::from_ptr(namespace_parent).to_str() else {
            set_invalid_argument_message(error_message);
            return LanceDBError::InvalidArgument;
        };
        vec![parent_str.to_string()]
    };

    let conn = &(*connection).inner;
    let runtime = get_runtime();

    let request = ListNamespacesRequest {
        namespace: parent_namespace,
        page_token: None,
        limit: None,
    };
    match runtime.block_on(conn.list_namespaces(request)) {
        Ok(namespaces) => {
            let count = namespaces.len();
            *count_out = count;

            if count == 0 {
                *namespaces_out = ptr::null_mut();
                return LanceDBError::Success;
            }

            // Allocate array of string pointers
            let namespaces_array =
                libc::malloc(count * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
            if namespaces_array.is_null() {
                set_unknown_error_message(error_message);
                return LanceDBError::Unknown;
            }

            // Convert each string and store pointer
            for (i, namespace) in namespaces.into_iter().enumerate() {
                match CString::new(namespace) {
                    Ok(c_str) => {
                        *namespaces_array.add(i) = c_str.into_raw();
                    }
                    Err(_) => {
                        // Clean up already allocated strings
                        for j in 0..i {
                            let _ = CString::from_raw(*namespaces_array.add(j));
                        }
                        libc::free(namespaces_array as *mut libc::c_void);
                        set_unknown_error_message(error_message);
                        return LanceDBError::Unknown;
                    }
                }
            }

            *namespaces_out = namespaces_array;
            LanceDBError::Success
        }
        Err(e) => handle_error(&e, error_message),
    }
}

/// Free a Connection
///
/// # Safety
/// - `connection` must be a valid pointer returned from `lancedb_connect_builder_execute`
/// - `connection` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_connection_free(connection: *mut LanceDBConnection) {
    if !connection.is_null() {
        let _ = Box::from_raw(connection);
    }
}

/// Free a Table
///
/// # Safety
/// - `table` must be a valid pointer returned from `lancedb_connection_open_table`
/// - `table` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_table_free(table: *mut LanceDBTable) {
    if !table.is_null() {
        let _ = Box::from_raw(table);
    }
}
