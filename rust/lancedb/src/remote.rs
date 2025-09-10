// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This module contains a remote client for a LanceDB server.  This is used
//! to communicate with LanceDB cloud.  It can also serve as an example for
//! building client/server applications with LanceDB or as a client for some
//! other custom LanceDB service.

pub(crate) mod client;
pub(crate) mod db;
mod retry;
pub(crate) mod table;
pub(crate) mod util;

const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
#[cfg(test)]
const ARROW_FILE_CONTENT_TYPE: &str = "application/vnd.apache.arrow.file";
#[cfg(test)]
const JSON_CONTENT_TYPE: &str = "application/json";

pub use client::{ClientConfig, HeaderProvider, RetryConfig, TimeoutConfig, TlsConfig};
pub use db::{RemoteDatabaseOptions, RemoteDatabaseOptionsBuilder};
