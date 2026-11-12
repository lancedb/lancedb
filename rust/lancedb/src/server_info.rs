// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Remote server provenance for diagnostics without submitting a query or job.

use serde::{Deserialize, Serialize};

/// Immutable identity compiled into a server executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerBuildInfo {
    /// Release of this executable.
    pub release: String,
    /// Git revision used to build the executable.
    pub git_sha: String,
    /// UTC RFC 3339 build timestamp, when known.
    pub build_date: Option<String>,
}

/// Identity of the query node that answered a discovery request.
///
/// Different requests can reach different versions during a rolling upgrade.
/// Configured images do not establish which version a running job uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerInfo {
    /// The responding executable; absent if its identity was not initialized.
    pub query_node: Option<ServerBuildInfo>,
    /// Lance library linked into the responding server.
    pub lance_version: String,
    /// LanceDB library linked into the responding server.
    pub lancedb_version: String,
    /// Function worker protocol supported by this server build.
    pub function_worker_protocol_version: u16,
    /// Configured query-node image, not an independently observed digest.
    pub configured_query_node_image: Option<String>,
    /// Default CPU/job worker image; existing jobs can retain earlier pins.
    pub configured_job_worker_image: Option<String>,
    /// CLI image configured for newly launched SQL jobs.
    pub configured_sql_job_image: Option<String>,
    /// Configured GPU worker image, not an observation of a running worker.
    pub configured_gpu_worker_image: Option<String>,
}
