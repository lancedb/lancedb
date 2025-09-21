// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::{Arc, OnceLock};

use tokio::runtime::Runtime;

// Main library entry point - imports handled in individual modules

mod connection;
mod database;
mod error;
mod query_simple;
mod schema_simple;
mod simple;
mod table_simple;
mod types;

pub use connection::*;
pub use database::*;
pub use error::*;
pub use query_simple::*;
pub use schema_simple::*;
pub use simple::*;
pub use table_simple::*;
pub use types::*;

// Global runtime for async operations
static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

fn get_runtime() -> Arc<Runtime> {
    RUNTIME.get_or_init(|| {
        let rt = Runtime::new().expect("Failed to create tokio runtime");
        Arc::new(rt)
    }).clone()
}

/// Initialize the LanceDB library
/// Must be called before using any other functions
#[no_mangle]
pub extern "C" fn lancedb_init() -> c_int {
    env_logger::try_init().ok();
    log::info!("LanceDB Go bindings initialized");
    0
}

/// Free a C string allocated by the library
#[no_mangle]
pub extern "C" fn lancedb_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(s);
    }
}

/// Convert Rust string to C string
pub(crate) fn to_c_string(s: String) -> *mut c_char {
    CString::new(s).unwrap().into_raw()
}

/// Convert C string to Rust string
pub(crate) fn from_c_string(s: *const c_char) -> Result<String, Box<dyn std::error::Error>> {
    if s.is_null() {
        return Err("Null pointer".into());
    }
    let c_str = unsafe { CStr::from_ptr(s) };
    Ok(c_str.to_str()?.to_string())
}

/// Result type for C interface
#[repr(C)]
pub struct LanceDBResult {
    pub success: bool,
    pub error_message: *mut c_char,
}

impl LanceDBResult {
    pub fn ok() -> Self {
        Self {
            success: true,
            error_message: ptr::null_mut(),
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            success: false,
            error_message: to_c_string(msg),
        }
    }
}

impl From<lancedb::Error> for LanceDBResult {
    fn from(err: lancedb::Error) -> Self {
        Self::error(err.to_string())
    }
}

/// Free a LanceDBResult
#[no_mangle]
pub extern "C" fn lancedb_result_free(result: *mut LanceDBResult) {
    if result.is_null() {
        return;
    }
    unsafe {
        let result = Box::from_raw(result);
        if !result.error_message.is_null() {
            lancedb_free_string(result.error_message);
        }
    }
}
