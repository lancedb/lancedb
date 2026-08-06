// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dynamic context provider for per-request context overrides.
//!
//! This module provides the [`DynamicContextProvider`] trait that enables
//! per-request context injection (e.g., dynamic authentication headers).
//!
//! ## Usage
//!
//! Implement the trait and pass to namespace builders:
//!
//! ```ignore
//! use lance_namespace_impls::{RestNamespaceBuilder, DynamicContextProvider, OperationInfo};
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! #[derive(Debug)]
//! struct MyProvider;
//!
//! impl DynamicContextProvider for MyProvider {
//!     fn provide_context(&self, info: &OperationInfo) -> HashMap<String, String> {
//!         let mut context = HashMap::new();
//!         context.insert("headers.Authorization".to_string(), format!("Bearer {}", get_current_token()));
//!         context.insert("headers.X-Request-Id".to_string(), generate_request_id());
//!         context
//!     }
//! }
//!
//! let namespace = RestNamespaceBuilder::new("https://api.example.com")
//!     .context_provider(Arc::new(MyProvider))
//!     .build();
//! ```
//!
//! For RestNamespace, context keys that start with `headers.` are converted to HTTP headers
//! by stripping the prefix. For example, `{"headers.Authorization": "Bearer abc123"}`
//! becomes the `Authorization: Bearer abc123` header. Keys without the `headers.` prefix
//! are ignored for HTTP headers but may be used for other purposes.

use std::collections::HashMap;

/// Information about the namespace operation being executed.
///
/// This is passed to the [`DynamicContextProvider`] to allow it to make
/// context decisions based on the operation.
#[derive(Debug, Clone)]
pub struct OperationInfo {
    /// The operation name (e.g., "list_tables", "describe_table", "create_namespace")
    pub operation: String,
    /// The object ID for the operation (namespace or table identifier).
    /// This is the delimited string form, e.g., "workspace$table_name".
    pub object_id: String,
}

impl OperationInfo {
    /// Create a new OperationInfo.
    pub fn new(operation: impl Into<String>, object_id: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            object_id: object_id.into(),
        }
    }
}

/// Trait for providing dynamic request context.
///
/// Implementations can generate per-request context (e.g., authentication headers)
/// based on the operation being performed. The provider is called synchronously
/// before each namespace operation.
///
/// For RestNamespace, context keys that start with `headers.` are converted to
/// HTTP headers by stripping the prefix. For example, `{"headers.Authorization": "Bearer token"}`
/// becomes the `Authorization: Bearer token` header.
///
/// ## Thread Safety
///
/// Implementations must be `Send + Sync` as the provider may be called from
/// multiple threads concurrently.
///
/// ## Error Handling
///
/// If the provider needs to signal an error, it should return an empty HashMap
/// and log the error. The namespace operation will proceed without the
/// additional context.
pub trait DynamicContextProvider: Send + Sync + std::fmt::Debug {
    /// Provide context for a namespace operation.
    ///
    /// # Arguments
    ///
    /// * `info` - Information about the operation being performed
    ///
    /// # Returns
    ///
    /// Returns a HashMap of context key-value pairs. For HTTP headers, use keys
    /// with the `headers.` prefix (e.g., `headers.Authorization`).
    /// Returns an empty HashMap if no additional context is needed.
    fn provide_context(&self, info: &OperationInfo) -> HashMap<String, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockContextProvider {
        prefix: String,
    }

    impl DynamicContextProvider for MockContextProvider {
        fn provide_context(&self, info: &OperationInfo) -> HashMap<String, String> {
            let mut context = HashMap::new();
            context.insert(
                "test-header".to_string(),
                format!("{}-{}", self.prefix, info.operation),
            );
            context.insert("object-id".to_string(), info.object_id.clone());
            context
        }
    }

    #[test]
    fn test_operation_info_creation() {
        let info = OperationInfo::new("describe_table", "workspace$my_table");
        assert_eq!(info.operation, "describe_table");
        assert_eq!(info.object_id, "workspace$my_table");
    }

    #[test]
    fn test_context_provider_basic() {
        let provider = MockContextProvider {
            prefix: "test".to_string(),
        };

        let info = OperationInfo::new("list_tables", "workspace$ns");

        let context = provider.provide_context(&info);
        assert_eq!(
            context.get("test-header"),
            Some(&"test-list_tables".to_string())
        );
        assert_eq!(context.get("object-id"), Some(&"workspace$ns".to_string()));
    }

    #[test]
    fn test_empty_context() {
        #[derive(Debug)]
        struct EmptyProvider;

        impl DynamicContextProvider for EmptyProvider {
            fn provide_context(&self, _info: &OperationInfo) -> HashMap<String, String> {
                HashMap::new()
            }
        }

        let provider = EmptyProvider;
        let info = OperationInfo::new("list_tables", "ns");

        let context = provider.provide_context(&info);
        assert!(context.is_empty());
    }
}
