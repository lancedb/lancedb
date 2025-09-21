// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use lancedb::connect;

use crate::{from_c_string, get_runtime, to_c_string, LanceDBResult};

/// Opaque handle for LanceDB connection
pub type ConnectionHandle = *mut c_void;

/// Connection options for LanceDB
#[repr(C)]
pub struct ConnectionOptions {
    pub api_key: *const c_char,
    pub region: *const c_char, 
    pub host_override: *const c_char,
    pub read_consistency_interval_secs: f64,
    pub storage_options_json: *const c_char, // JSON string of storage options
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            api_key: ptr::null(),
            region: ptr::null(),
            host_override: ptr::null(),
            read_consistency_interval_secs: -1.0, // -1 means None
            storage_options_json: ptr::null(),
        }
    }
}

/// Connect to a LanceDB database
/// 
/// # Arguments
/// * `uri` - The URI of the database
/// * `options` - Connection options (can be null for defaults)
/// * `handle` - Output parameter for the connection handle
/// 
/// # Returns
/// LanceDBResult indicating success or failure
#[no_mangle]
pub extern "C" fn lancedb_connect(
    uri: *const c_char,
    options: *const ConnectionOptions,
    handle: *mut ConnectionHandle,
) -> *mut LanceDBResult {
    let result = std::panic::catch_unwind(|| -> LanceDBResult {
        if uri.is_null() || handle.is_null() {
            return LanceDBResult::error("Invalid null arguments".to_string());
        }

        let uri_str = match from_c_string(uri) {
            Ok(s) => s,
            Err(e) => return LanceDBResult::error(format!("Invalid URI: {}", e)),
        };

        let rt = get_runtime();
        
        match rt.block_on(async {
            let connect_builder = connect(&uri_str);

            // Apply options if provided
            if !options.is_null() {
                let _opts = unsafe { &*options };
                
                // TODO: Apply connection options to the builder
                // This would require extending the connect builder in the Rust core
                // For now, we'll connect with default options
            }

            connect_builder.execute().await
        }) {
            Ok(conn) => {
                let boxed_conn = Box::new(conn);
                unsafe {
                    *handle = Box::into_raw(boxed_conn) as *mut c_void;
                }
                LanceDBResult::ok()
            }
            Err(e) => LanceDBResult::from(e),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(LanceDBResult::error("Panic in lancedb_connect".to_string()))),
    }
}

/// Close a LanceDB connection and free resources
#[no_mangle]
pub extern "C" fn lancedb_connection_close(handle: ConnectionHandle) -> *mut LanceDBResult {
    if handle.is_null() {
        return Box::into_raw(Box::new(LanceDBResult::error("Invalid null handle".to_string())));
    }

    let result = std::panic::catch_unwind(|| -> LanceDBResult {
        unsafe {
            let _conn = Box::from_raw(handle as *mut lancedb::Connection);
            // Connection will be dropped here, cleaning up resources
        }
        LanceDBResult::ok()
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(LanceDBResult::error("Panic in lancedb_connection_close".to_string()))),
    }
}

/// Get table names from the database
/// 
/// # Arguments
/// * `handle` - Connection handle
/// * `names` - Output parameter for array of table name strings
/// * `count` - Output parameter for number of table names
/// 
/// # Returns
/// LanceDBResult indicating success or failure
#[no_mangle]
pub extern "C" fn lancedb_list_tables(
    handle: ConnectionHandle,
    names: *mut *mut *mut c_char,
    count: *mut c_int,
) -> *mut LanceDBResult {
    let result = std::panic::catch_unwind(|| -> LanceDBResult {
        if handle.is_null() || names.is_null() || count.is_null() {
            return LanceDBResult::error("Invalid null arguments".to_string());
        }

        let conn = unsafe { &*(handle as *const lancedb::Connection) };
        let rt = get_runtime();

        match rt.block_on(async { conn.table_names().execute().await }) {
            Ok(table_names) => {
                let len = table_names.len();
                unsafe {
                    *count = len as c_int;
                    
                    if len == 0 {
                        *names = ptr::null_mut();
                        return LanceDBResult::ok();
                    }

                    // Allocate array of string pointers
                    let array = libc::malloc(len * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
                    if array.is_null() {
                        return LanceDBResult::error("Failed to allocate memory".to_string());
                    }

                    // Convert each string and store pointer
                    for (i, name) in table_names.iter().enumerate() {
                        let c_name = to_c_string(name.clone());
                        *array.add(i) = c_name;
                    }
                    
                    *names = array;
                }
                LanceDBResult::ok()
            }
            Err(e) => LanceDBResult::from(e),
        }
    });

    match result {
        Ok(res) => Box::into_raw(Box::new(res)),
        Err(_) => Box::into_raw(Box::new(LanceDBResult::error("Panic in lancedb_list_tables".to_string()))),
    }
}

/// Free table names array returned by lancedb_list_tables
#[no_mangle]
pub extern "C" fn lancedb_free_table_names(names: *mut *mut c_char, count: c_int) {
    if names.is_null() {
        return;
    }

    unsafe {
        for i in 0..count {
            let name_ptr = *names.add(i as usize);
            if !name_ptr.is_null() {
                let _ = CString::from_raw(name_ptr);
            }
        }
        libc::free(names as *mut c_void);
    }
}
