// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! C FFI bindings for LanceDB
//!
//! This crate provides a C-compatible API for LanceDB, allowing integration
//! with C and C++ applications. The API follows standard C conventions with
//! opaque handles, explicit error codes, and manual memory management.

pub mod connection;
pub mod error;
pub mod index;
pub mod query;
pub mod table;
pub mod types;

// Re-export all public FFI functions
pub use connection::*;
pub use error::*;
pub use index::*;
pub use query::*;
pub use table::*;
pub use types::*;
