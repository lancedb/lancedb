// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Namespace error types.
//!
//! This module defines fine-grained error types for Lance Namespace operations.
//! Each error type has a unique numeric code that is consistent across all
//! Lance Namespace implementations (Python, Java, Rust, REST).
//!
//! # Error Handling
//!
//! Namespace operations return [`NamespaceError`] which can be converted to
//! [`lance_core::Error`] for integration with the Lance ecosystem.
//!
//! ```rust,ignore
//! use lance_namespace::{NamespaceError, ErrorCode};
//!
//! // Create and use namespace errors
//! let err = NamespaceError::TableNotFound {
//!     message: "Table 'users' not found".into(),
//! };
//! assert_eq!(err.code(), ErrorCode::TableNotFound);
//!
//! // Convert to lance_core::Error
//! let lance_err: lance_core::Error = err.into();
//! ```

use snafu::Snafu;

/// Lance Namespace error codes.
///
/// These codes are globally unique across all Lance Namespace implementations
/// (Python, Java, Rust, REST). Use these codes for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ErrorCode {
    /// Operation not supported by this backend
    Unsupported = 0,
    /// The specified namespace does not exist
    NamespaceNotFound = 1,
    /// A namespace with this name already exists
    NamespaceAlreadyExists = 2,
    /// Namespace contains tables or child namespaces
    NamespaceNotEmpty = 3,
    /// The specified table does not exist
    TableNotFound = 4,
    /// A table with this name already exists
    TableAlreadyExists = 5,
    /// The specified table index does not exist
    TableIndexNotFound = 6,
    /// A table index with this name already exists
    TableIndexAlreadyExists = 7,
    /// The specified table tag does not exist
    TableTagNotFound = 8,
    /// A table tag with this name already exists
    TableTagAlreadyExists = 9,
    /// The specified transaction does not exist
    TransactionNotFound = 10,
    /// The specified table version does not exist
    TableVersionNotFound = 11,
    /// The specified table column does not exist
    TableColumnNotFound = 12,
    /// Malformed request or invalid parameters
    InvalidInput = 13,
    /// Optimistic concurrency conflict
    ConcurrentModification = 14,
    /// User lacks permission for this operation
    PermissionDenied = 15,
    /// Authentication credentials are missing or invalid
    Unauthenticated = 16,
    /// Service is temporarily unavailable
    ServiceUnavailable = 17,
    /// Unexpected server/implementation error
    Internal = 18,
    /// Table is in an invalid state for the operation
    InvalidTableState = 19,
    /// Table schema validation failed
    TableSchemaValidationError = 20,
    /// Request was throttled due to rate limiting or too many concurrent operations
    Throttling = 21,
    /// The specified table branch does not exist
    TableBranchNotFound = 22,
    /// A table branch with this name already exists
    TableBranchAlreadyExists = 23,
}

impl ErrorCode {
    /// Returns the numeric code value.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Creates an ErrorCode from a numeric code.
    ///
    /// Returns `None` if the code is not recognized.
    pub fn from_u32(code: u32) -> Option<Self> {
        match code {
            0 => Some(Self::Unsupported),
            1 => Some(Self::NamespaceNotFound),
            2 => Some(Self::NamespaceAlreadyExists),
            3 => Some(Self::NamespaceNotEmpty),
            4 => Some(Self::TableNotFound),
            5 => Some(Self::TableAlreadyExists),
            6 => Some(Self::TableIndexNotFound),
            7 => Some(Self::TableIndexAlreadyExists),
            8 => Some(Self::TableTagNotFound),
            9 => Some(Self::TableTagAlreadyExists),
            10 => Some(Self::TransactionNotFound),
            11 => Some(Self::TableVersionNotFound),
            12 => Some(Self::TableColumnNotFound),
            13 => Some(Self::InvalidInput),
            14 => Some(Self::ConcurrentModification),
            15 => Some(Self::PermissionDenied),
            16 => Some(Self::Unauthenticated),
            17 => Some(Self::ServiceUnavailable),
            18 => Some(Self::Internal),
            19 => Some(Self::InvalidTableState),
            20 => Some(Self::TableSchemaValidationError),
            21 => Some(Self::Throttling),
            22 => Some(Self::TableBranchNotFound),
            23 => Some(Self::TableBranchAlreadyExists),
            _ => None,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Unsupported => "Unsupported",
            Self::NamespaceNotFound => "NamespaceNotFound",
            Self::NamespaceAlreadyExists => "NamespaceAlreadyExists",
            Self::NamespaceNotEmpty => "NamespaceNotEmpty",
            Self::TableNotFound => "TableNotFound",
            Self::TableAlreadyExists => "TableAlreadyExists",
            Self::TableIndexNotFound => "TableIndexNotFound",
            Self::TableIndexAlreadyExists => "TableIndexAlreadyExists",
            Self::TableTagNotFound => "TableTagNotFound",
            Self::TableTagAlreadyExists => "TableTagAlreadyExists",
            Self::TransactionNotFound => "TransactionNotFound",
            Self::TableVersionNotFound => "TableVersionNotFound",
            Self::TableColumnNotFound => "TableColumnNotFound",
            Self::InvalidInput => "InvalidInput",
            Self::ConcurrentModification => "ConcurrentModification",
            Self::PermissionDenied => "PermissionDenied",
            Self::Unauthenticated => "Unauthenticated",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::Internal => "Internal",
            Self::InvalidTableState => "InvalidTableState",
            Self::TableSchemaValidationError => "TableSchemaValidationError",
            Self::Throttling => "Throttling",
            Self::TableBranchNotFound => "TableBranchNotFound",
            Self::TableBranchAlreadyExists => "TableBranchAlreadyExists",
        };
        write!(f, "{}", name)
    }
}

/// Lance Namespace error type.
///
/// This enum provides fine-grained error types for Lance Namespace operations.
/// Each variant corresponds to a specific error condition and has an associated
/// [`ErrorCode`] accessible via the [`code()`](NamespaceError::code) method.
///
/// # Converting to lance_core::Error
///
/// `NamespaceError` implements `Into<lance_core::Error>`, preserving the original
/// error so it can be downcast later:
///
/// ```rust,ignore
/// let ns_err = NamespaceError::TableNotFound { message: "...".into() };
/// let lance_err: lance_core::Error = ns_err.into();
///
/// // Later, extract the original error:
/// if let lance_core::Error::Namespace { source, .. } = &lance_err {
///     if let Some(ns_err) = source.downcast_ref::<NamespaceError>() {
///         println!("Error code: {:?}", ns_err.code());
///     }
/// }
/// ```
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum NamespaceError {
    /// Operation not supported by this backend.
    #[snafu(display("Unsupported: {message}"))]
    Unsupported { message: String },

    /// The specified namespace does not exist.
    #[snafu(display("Namespace not found: {message}"))]
    NamespaceNotFound { message: String },

    /// A namespace with this name already exists.
    #[snafu(display("Namespace already exists: {message}"))]
    NamespaceAlreadyExists { message: String },

    /// Namespace contains tables or child namespaces.
    #[snafu(display("Namespace not empty: {message}"))]
    NamespaceNotEmpty { message: String },

    /// The specified table does not exist.
    #[snafu(display("Table not found: {message}"))]
    TableNotFound { message: String },

    /// A table with this name already exists.
    #[snafu(display("Table already exists: {message}"))]
    TableAlreadyExists { message: String },

    /// The specified table index does not exist.
    #[snafu(display("Table index not found: {message}"))]
    TableIndexNotFound { message: String },

    /// A table index with this name already exists.
    #[snafu(display("Table index already exists: {message}"))]
    TableIndexAlreadyExists { message: String },

    /// The specified table tag does not exist.
    #[snafu(display("Table tag not found: {message}"))]
    TableTagNotFound { message: String },

    /// A table tag with this name already exists.
    #[snafu(display("Table tag already exists: {message}"))]
    TableTagAlreadyExists { message: String },

    /// The specified transaction does not exist.
    #[snafu(display("Transaction not found: {message}"))]
    TransactionNotFound { message: String },

    /// The specified table version does not exist.
    #[snafu(display("Table version not found: {message}"))]
    TableVersionNotFound { message: String },

    /// The specified table column does not exist.
    #[snafu(display("Table column not found: {message}"))]
    TableColumnNotFound { message: String },

    /// Malformed request or invalid parameters.
    #[snafu(display("Invalid input: {message}"))]
    InvalidInput { message: String },

    /// Optimistic concurrency conflict.
    #[snafu(display("Concurrent modification: {message}"))]
    ConcurrentModification { message: String },

    /// User lacks permission for this operation.
    #[snafu(display("Permission denied: {message}"))]
    PermissionDenied { message: String },

    /// Authentication credentials are missing or invalid.
    #[snafu(display("Unauthenticated: {message}"))]
    Unauthenticated { message: String },

    /// Service is temporarily unavailable.
    #[snafu(display("Service unavailable: {message}"))]
    ServiceUnavailable { message: String },

    /// Unexpected internal error.
    #[snafu(display("Internal error: {message}"))]
    Internal { message: String },

    /// Table is in an invalid state for the operation.
    #[snafu(display("Invalid table state: {message}"))]
    InvalidTableState { message: String },

    /// Table schema validation failed.
    #[snafu(display("Table schema validation error: {message}"))]
    TableSchemaValidationError { message: String },

    /// Request was throttled due to rate limiting or too many concurrent operations.
    #[snafu(display("Throttling: {message}"))]
    Throttling { message: String },

    /// The specified table branch does not exist.
    #[snafu(display("Table branch not found: {message}"))]
    TableBranchNotFound { message: String },

    /// A table branch with this name already exists.
    #[snafu(display("Table branch already exists: {message}"))]
    TableBranchAlreadyExists { message: String },
}

impl NamespaceError {
    /// Returns the inner message without the Display prefix.
    ///
    /// Useful when serializing across boundaries (e.g. REST) where
    /// the receiver will reconstruct the variant from the error code
    /// and re-apply its own Display formatting.
    pub fn message(&self) -> &str {
        match self {
            Self::Unsupported { message }
            | Self::NamespaceNotFound { message }
            | Self::NamespaceAlreadyExists { message }
            | Self::NamespaceNotEmpty { message }
            | Self::TableNotFound { message }
            | Self::TableAlreadyExists { message }
            | Self::TableIndexNotFound { message }
            | Self::TableIndexAlreadyExists { message }
            | Self::TableTagNotFound { message }
            | Self::TableTagAlreadyExists { message }
            | Self::TransactionNotFound { message }
            | Self::TableVersionNotFound { message }
            | Self::TableColumnNotFound { message }
            | Self::InvalidInput { message }
            | Self::ConcurrentModification { message }
            | Self::PermissionDenied { message }
            | Self::Unauthenticated { message }
            | Self::ServiceUnavailable { message }
            | Self::Internal { message }
            | Self::InvalidTableState { message }
            | Self::TableSchemaValidationError { message }
            | Self::Throttling { message }
            | Self::TableBranchNotFound { message }
            | Self::TableBranchAlreadyExists { message } => message,
        }
    }

    /// Returns the error code for this error.
    ///
    /// Use this for programmatic error handling across language boundaries.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Unsupported { .. } => ErrorCode::Unsupported,
            Self::NamespaceNotFound { .. } => ErrorCode::NamespaceNotFound,
            Self::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceAlreadyExists,
            Self::NamespaceNotEmpty { .. } => ErrorCode::NamespaceNotEmpty,
            Self::TableNotFound { .. } => ErrorCode::TableNotFound,
            Self::TableAlreadyExists { .. } => ErrorCode::TableAlreadyExists,
            Self::TableIndexNotFound { .. } => ErrorCode::TableIndexNotFound,
            Self::TableIndexAlreadyExists { .. } => ErrorCode::TableIndexAlreadyExists,
            Self::TableTagNotFound { .. } => ErrorCode::TableTagNotFound,
            Self::TableTagAlreadyExists { .. } => ErrorCode::TableTagAlreadyExists,
            Self::TransactionNotFound { .. } => ErrorCode::TransactionNotFound,
            Self::TableVersionNotFound { .. } => ErrorCode::TableVersionNotFound,
            Self::TableColumnNotFound { .. } => ErrorCode::TableColumnNotFound,
            Self::InvalidInput { .. } => ErrorCode::InvalidInput,
            Self::ConcurrentModification { .. } => ErrorCode::ConcurrentModification,
            Self::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            Self::Unauthenticated { .. } => ErrorCode::Unauthenticated,
            Self::ServiceUnavailable { .. } => ErrorCode::ServiceUnavailable,
            Self::Internal { .. } => ErrorCode::Internal,
            Self::InvalidTableState { .. } => ErrorCode::InvalidTableState,
            Self::TableSchemaValidationError { .. } => ErrorCode::TableSchemaValidationError,
            Self::Throttling { .. } => ErrorCode::Throttling,
            Self::TableBranchNotFound { .. } => ErrorCode::TableBranchNotFound,
            Self::TableBranchAlreadyExists { .. } => ErrorCode::TableBranchAlreadyExists,
        }
    }

    /// Creates a NamespaceError from an error code and message.
    ///
    /// This is useful when receiving errors from REST API or other language bindings.
    pub fn from_code(code: u32, message: impl Into<String>) -> Self {
        let message = message.into();
        match ErrorCode::from_u32(code) {
            Some(ErrorCode::Unsupported) => Self::Unsupported { message },
            Some(ErrorCode::NamespaceNotFound) => Self::NamespaceNotFound { message },
            Some(ErrorCode::NamespaceAlreadyExists) => Self::NamespaceAlreadyExists { message },
            Some(ErrorCode::NamespaceNotEmpty) => Self::NamespaceNotEmpty { message },
            Some(ErrorCode::TableNotFound) => Self::TableNotFound { message },
            Some(ErrorCode::TableAlreadyExists) => Self::TableAlreadyExists { message },
            Some(ErrorCode::TableIndexNotFound) => Self::TableIndexNotFound { message },
            Some(ErrorCode::TableIndexAlreadyExists) => Self::TableIndexAlreadyExists { message },
            Some(ErrorCode::TableTagNotFound) => Self::TableTagNotFound { message },
            Some(ErrorCode::TableTagAlreadyExists) => Self::TableTagAlreadyExists { message },
            Some(ErrorCode::TransactionNotFound) => Self::TransactionNotFound { message },
            Some(ErrorCode::TableVersionNotFound) => Self::TableVersionNotFound { message },
            Some(ErrorCode::TableColumnNotFound) => Self::TableColumnNotFound { message },
            Some(ErrorCode::InvalidInput) => Self::InvalidInput { message },
            Some(ErrorCode::ConcurrentModification) => Self::ConcurrentModification { message },
            Some(ErrorCode::PermissionDenied) => Self::PermissionDenied { message },
            Some(ErrorCode::Unauthenticated) => Self::Unauthenticated { message },
            Some(ErrorCode::ServiceUnavailable) => Self::ServiceUnavailable { message },
            Some(ErrorCode::Internal) => Self::Internal { message },
            Some(ErrorCode::InvalidTableState) => Self::InvalidTableState { message },
            Some(ErrorCode::TableSchemaValidationError) => {
                Self::TableSchemaValidationError { message }
            }
            Some(ErrorCode::Throttling) => Self::Throttling { message },
            Some(ErrorCode::TableBranchNotFound) => Self::TableBranchNotFound { message },
            Some(ErrorCode::TableBranchAlreadyExists) => Self::TableBranchAlreadyExists { message },
            None => Self::Internal { message },
        }
    }
}

/// Converts a NamespaceError into a lance_core::Error.
///
/// The original `NamespaceError` is preserved in the `source` field and can be
/// extracted via downcasting for programmatic error handling.
impl From<NamespaceError> for lance_core::Error {
    #[track_caller]
    fn from(err: NamespaceError) -> Self {
        Self::namespace_source(Box::new(err))
    }
}

/// Result type for namespace operations.
pub type Result<T> = std::result::Result<T, NamespaceError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_roundtrip() {
        for code in 0..=23 {
            let error_code = ErrorCode::from_u32(code).unwrap();
            assert_eq!(error_code.as_u32(), code);
        }
    }

    #[test]
    fn test_unknown_error_code() {
        assert!(ErrorCode::from_u32(999).is_none());
    }

    #[test]
    fn test_namespace_error_code() {
        let err = NamespaceError::TableNotFound {
            message: "test table".to_string(),
        };
        assert_eq!(err.code(), ErrorCode::TableNotFound);
        assert_eq!(err.code().as_u32(), 4);
    }

    #[test]
    fn test_from_code() {
        let err = NamespaceError::from_code(4, "table not found");
        assert_eq!(err.code(), ErrorCode::TableNotFound);
        assert!(err.to_string().contains("table not found"));
    }

    #[test]
    fn test_from_unknown_code() {
        let err = NamespaceError::from_code(999, "unknown error");
        assert_eq!(err.code(), ErrorCode::Internal);
    }

    #[test]
    fn test_convert_to_lance_error() {
        let ns_err = NamespaceError::TableNotFound {
            message: "users".to_string(),
        };
        let lance_err: lance_core::Error = ns_err.into();

        // Verify it's a Namespace error
        match &lance_err {
            lance_core::Error::Namespace { source, .. } => {
                // Downcast to get the original error
                let downcast = source.downcast_ref::<NamespaceError>();
                assert!(downcast.is_some());
                assert_eq!(downcast.unwrap().code(), ErrorCode::TableNotFound);
            }
            _ => panic!("Expected Namespace error"),
        }
    }

    #[test]
    fn test_error_display() {
        let err = NamespaceError::TableNotFound {
            message: "users".to_string(),
        };
        assert_eq!(err.to_string(), "Table not found: users");
    }
}
