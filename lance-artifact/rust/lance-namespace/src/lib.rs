// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance Namespace Rust Client
//!
//! A Rust client for the Lance Namespace API that provides a unified interface
//! for managing namespaces and tables across different backend implementations.
//!
//! # Error Handling
//!
//! This crate provides fine-grained error types through the [`error`] module.
//! Each error type has a unique numeric code that is consistent across all
//! Lance Namespace implementations (Python, Java, Rust, REST).
//!
//! See [`error::ErrorCode`] for the list of error codes and
//! [`error::NamespaceError`] for the error types.

pub mod error;
pub mod namespace;
pub mod schema;

// Re-export the trait at the crate root
pub use lance_core::{Error, Result};
pub use namespace::LanceNamespace;

// Re-export error types
pub use error::{ErrorCode, NamespaceError, Result as NamespaceResult};

// Re-export reqwest client for convenience
pub use lance_namespace_reqwest_client as reqwest_client;

// Re-export commonly used models from the reqwest client
pub mod models {
    pub use lance_namespace_reqwest_client::models::*;
}

// Re-export APIs from the reqwest client
pub mod apis {
    pub use lance_namespace_reqwest_client::apis::*;
}
