// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::ffi::CString;
use std::os::raw::c_char;

use crate::to_c_string;

/// Error codes for the C API
#[derive(Clone, Copy)]
#[repr(C)]
pub enum LanceDBErrorCode {
    Success = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    PermissionDenied = 4,
    Internal = 5,
    InvalidData = 6,
    Io = 7,
    Runtime = 8,
    SchemaError = 9,
    InvalidInput = 10,
    IndexError = 11,
    NotImplemented = 12,
}

/// Detailed error information
#[repr(C)]
pub struct LanceDBError {
    pub code: LanceDBErrorCode,
    pub message: *mut c_char,
    pub details: *mut c_char,
}

impl LanceDBError {
    pub fn new(code: LanceDBErrorCode, message: String, details: Option<String>) -> Self {
        Self {
            code,
            message: to_c_string(message),
            details: match details {
                Some(d) => to_c_string(d),
                None => std::ptr::null_mut(),
            },
        }
    }

    pub fn success() -> Self {
        Self {
            code: LanceDBErrorCode::Success,
            message: std::ptr::null_mut(),
            details: std::ptr::null_mut(),
        }
    }
}

impl From<lancedb::Error> for LanceDBError {
    fn from(err: lancedb::Error) -> Self {
        let (code, message) = match &err {
            lancedb::Error::InvalidInput { message, .. } => {
                (LanceDBErrorCode::InvalidInput, message.clone())
            }
            lancedb::Error::Runtime { message, .. } => {
                (LanceDBErrorCode::Runtime, message.clone())
            }
            lancedb::Error::Schema { message, .. } => {
                (LanceDBErrorCode::SchemaError, message.clone())
            }
            _ => (LanceDBErrorCode::Internal, err.to_string()),
        };

        Self::new(code, message, Some(format!("{:?}", err)))
    }
}

/// Free a LanceDBError
#[no_mangle]
pub extern "C" fn lancedb_error_free(error: *mut LanceDBError) {
    if error.is_null() {
        return;
    }

    unsafe {
        let error = Box::from_raw(error);
        if !error.message.is_null() {
            let _ = CString::from_raw(error.message);
        }
        if !error.details.is_null() {
            let _ = CString::from_raw(error.details);
        }
    }
}

/// Get error code as integer
#[no_mangle]
pub extern "C" fn lancedb_error_code(error: *const LanceDBError) -> i32 {
    if error.is_null() {
        return LanceDBErrorCode::InvalidArgument as i32;
    }
    
    unsafe { (*error).code as i32 }
}

/// Get error message
#[no_mangle]
pub extern "C" fn lancedb_error_message(error: *const LanceDBError) -> *const c_char {
    if error.is_null() {
        return std::ptr::null();
    }
    
    unsafe { (*error).message }
}
