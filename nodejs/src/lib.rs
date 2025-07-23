// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use env_logger::Env;
use napi_derive::*;

mod connection;
mod error;
mod index;
mod iterator;
pub mod merge;
mod query;
pub mod remote;
mod rerankers;
mod session;
mod table;
mod util;

#[napi(object)]
#[derive(Debug)]
pub struct ConnectionOptions {
    /// (For LanceDB OSS only): The interval, in seconds, at which to check for
    /// updates to the table from other processes. If None, then consistency is not
    /// checked. For performance reasons, this is the default. For strong
    /// consistency, set this to zero seconds. Then every read will check for
    /// updates from other processes. As a compromise, you can set this to a
    /// non-zero value for eventual consistency. If more than that interval
    /// has passed since the last check, then the table will be checked for updates.
    /// Note: this consistency only applies to read operations. Write operations are
    /// always consistent.
    pub read_consistency_interval: Option<f64>,
    /// (For LanceDB OSS only): configuration for object storage.
    ///
    /// The available options are described at https://lancedb.github.io/lancedb/guides/storage/
    pub storage_options: Option<HashMap<String, String>>,

    /// (For LanceDB cloud only): configuration for the remote HTTP client.
    pub client_config: Option<remote::ClientConfig>,
    /// (For LanceDB cloud only): the API key to use with LanceDB Cloud.
    ///
    /// Can also be set via the environment variable `LANCEDB_API_KEY`.
    pub api_key: Option<String>,
    /// (For LanceDB cloud only): the region to use for LanceDB cloud.
    /// Defaults to 'us-east-1'.
    pub region: Option<String>,
    /// (For LanceDB cloud only): the host to use for LanceDB cloud. Used
    /// for testing purposes.
    pub host_override: Option<String>,
}

#[napi(object)]
pub struct OpenTableOptions {
    pub storage_options: Option<HashMap<String, String>>,
}

#[napi::module_init]
fn init() {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
}
