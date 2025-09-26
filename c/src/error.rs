// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Error handling utility functions for LanceDB C bindings

use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

/// Result codes for C API
#[repr(C)]
#[derive(Debug, PartialEq)]
pub enum LanceDBError {
    Success = 0,
    InvalidArgument = 1,
    InvalidTableName = 2,
    InvalidInput = 3,
    TableNotFound = 4,
    DatabaseNotFound = 5,
    DatabaseAlreadyExists = 6,
    IndexNotFound = 7,
    EmbeddingFunctionNotFound = 8,
    TableAlreadyExists = 9,
    CreateDir = 10,
    Schema = 11,
    Runtime = 12,
    Timeout = 13,
    ObjectStore = 14,
    Lance = 15,
    Http = 16,
    Retry = 17,
    Arrow = 18,
    NotSupported = 19,
    Other = 20,
    Unknown = 21,
}

/// Convert Rust Error to C error code
pub(crate) fn error_to_error_code(error: &lancedb::error::Error) -> LanceDBError {
    match error {
        lancedb::error::Error::InvalidTableName { .. } => LanceDBError::InvalidTableName,
        lancedb::error::Error::InvalidInput { .. } => LanceDBError::InvalidInput,
        lancedb::error::Error::TableNotFound { .. } => LanceDBError::TableNotFound,
        lancedb::error::Error::DatabaseNotFound { .. } => LanceDBError::DatabaseNotFound,
        lancedb::error::Error::DatabaseAlreadyExists { .. } => LanceDBError::DatabaseAlreadyExists,
        lancedb::error::Error::IndexNotFound { .. } => LanceDBError::IndexNotFound,
        lancedb::error::Error::EmbeddingFunctionNotFound { .. } => {
            LanceDBError::EmbeddingFunctionNotFound
        }
        lancedb::error::Error::TableAlreadyExists { .. } => LanceDBError::TableAlreadyExists,
        lancedb::error::Error::CreateDir { .. } => LanceDBError::CreateDir,
        lancedb::error::Error::Schema { .. } => LanceDBError::Schema,
        lancedb::error::Error::Runtime { .. } => LanceDBError::Runtime,
        lancedb::error::Error::Timeout { .. } => LanceDBError::Timeout,
        lancedb::error::Error::ObjectStore { .. } => LanceDBError::ObjectStore,
        lancedb::error::Error::Lance { .. } => LanceDBError::Lance,
        #[cfg(feature = "remote")]
        lancedb::error::Error::Http { .. } => LanceDBError::Http,
        #[cfg(feature = "remote")]
        lancedb::error::Error::Retry { .. } => LanceDBError::Retry,
        lancedb::error::Error::Arrow { .. } => LanceDBError::Arrow,
        lancedb::error::Error::NotSupported { .. } => LanceDBError::NotSupported,
        lancedb::error::Error::Other { .. } => LanceDBError::Other,
    }
}

/// Set error message for detailed error reporting
pub(crate) unsafe fn set_error_message(
    error_message_out: *mut *mut c_char,
    error: &lancedb::error::Error,
) {
    if error_message_out.is_null() {
        return;
    }

    let error_string = format!("{}", error);
    match CString::new(error_string) {
        Ok(c_str) => {
            *error_message_out = c_str.into_raw();
        }
        Err(_) => {
            *error_message_out = ptr::null_mut();
        }
    }
}

/// Handle error with optional message
pub(crate) unsafe fn handle_error(
    error: &lancedb::error::Error,
    error_message_out: *mut *mut c_char,
) -> LanceDBError {
    if !error_message_out.is_null() {
        set_error_message(error_message_out, error);
    }
    error_to_error_code(error)
}

/// Set simple error message for invalid argument cases
pub(crate) unsafe fn set_invalid_argument_message(error_message_out: *mut *mut c_char) {
    if error_message_out.is_null() {
        return;
    }

    match CString::new("Invalid argument") {
        Ok(c_str) => {
            *error_message_out = c_str.into_raw();
        }
        Err(_) => {
            *error_message_out = ptr::null_mut();
        }
    }
}

/// Set simple error message for the unknown error cases
pub(crate) unsafe fn set_unknown_error_message(error_message_out: *mut *mut c_char) {
    if error_message_out.is_null() {
        return;
    }

    match CString::new("Unknown error") {
        Ok(c_str) => {
            *error_message_out = c_str.into_raw();
        }
        Err(_) => {
            *error_message_out = ptr::null_mut();
        }
    }
}

/// Free string returned by LanceDB functions
///
/// # Safety
/// - `str` must be a pointer returned by LanceDB functions (e.g., error messages)
/// - `str` must not be used after calling this function
#[no_mangle]
pub unsafe extern "C" fn lancedb_free_string(str: *mut c_char) {
    if !str.is_null() {
        let _ = CString::from_raw(str);
    }
}
