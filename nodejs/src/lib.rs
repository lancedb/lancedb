// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use env_logger::Env;
use napi_derive::*;

mod connection;
mod error;
mod header;
mod index;
mod iterator;
pub mod merge;
pub mod otel;
pub mod permutation;
mod query;
pub mod remote;
mod rerankers;
mod scannable;
mod session;
mod table;
mod util;

#[napi(object)]
#[derive(Debug)]
pub struct ConnectionOptions {
    /// The interval, in seconds, at which to check for updates to the table
    /// from other processes. If None, then consistency is not checked. For
    /// performance reasons, this is the default. For strong consistency, set
    /// this to zero seconds. Then every read will check for updates from other
    /// processes. As a compromise, you can set this to a non-zero value for
    /// eventual consistency. If more than that interval has passed since the
    /// last check, then the table will be checked for updates. Note: this
    /// consistency only applies to read operations. Write operations are
    /// always consistent.
    ///
    /// Stronger consistency is not free. The smaller the interval, the more
    /// often each read pays the cost of checking for updates against object
    /// storage, raising per-read latency and cost.
    pub read_consistency_interval: Option<f64>,
    /// (For LanceDB OSS only): configuration for object storage.
    ///
    /// The available options are described at https://docs.lancedb.com/storage/
    pub storage_options: Option<HashMap<String, String>>,
    /// (For LanceDB OSS only): use directory namespace manifests as the source
    /// of truth for table metadata. Existing directory-listed root tables are
    /// migrated into the manifest on access.
    pub manifest_enabled: Option<bool>,
    /// (For LanceDB OSS only): extra properties for the backing namespace
    /// client used by manifest-enabled native connections.
    pub namespace_client_properties: Option<HashMap<String, String>>,
    /// (For LanceDB OSS only): the session to use for this connection. Holds
    /// shared caches and other session-specific state.
    pub session: Option<session::Session>,

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
    /// (For LanceDB cloud only): OAuth configuration for IdP-based
    /// authentication (e.g., Azure Entra ID). When set, token acquisition
    /// and refresh are handled entirely in Rust. TypeScript users should pass
    /// the public `OAuthConfig` type exported from `@lancedb/lancedb`.
    pub oauth_config: Option<remote::OAuthConfig>,
}

#[napi(object)]
pub struct OpenTableOptions {
    pub storage_options: Option<HashMap<String, String>>,
}

#[napi(object)]
#[derive(Debug)]
pub struct ConnectNamespaceOptions {
    /// The interval, in seconds, at which to check for updates to the table
    /// from other processes. If None, then consistency is not checked. For
    /// performance reasons, this is the default. For strong consistency, set
    /// this to zero seconds. Then every read will check for updates from other
    /// processes. As a compromise, you can set this to a non-zero value for
    /// eventual consistency.
    pub read_consistency_interval: Option<f64>,
    /// Configuration for object storage. The available options are described
    /// at https://docs.lancedb.com/storage/
    pub storage_options: Option<HashMap<String, String>>,
    /// Extra properties for the backing namespace client.
    pub namespace_client_properties: Option<HashMap<String, String>>,
    /// The session to use for this connection. Holds shared caches and other
    /// session-specific state.
    pub session: Option<session::Session>,
}

#[napi_derive::module_init]
fn init() {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
}
