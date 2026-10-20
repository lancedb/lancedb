// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

pub mod acl;
pub mod auth;
pub mod cli;
pub mod client;
pub mod connection;
pub mod dml_result;
pub mod error;
pub mod formatter;
pub mod load;
pub mod mocks;
pub mod profile;
pub mod server_config;
pub mod sql_executor;
pub mod table_writer;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub use cli::{run, Args, Commands};
pub(crate) fn format_host_port(host: &str, port: impl std::fmt::Display) -> String {
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]:{port}"),
        _ => format!("{host}:{port}"),
    }
}

pub(crate) fn lancedb_client_config(
    tls: &server_config::TlsOptions,
    authentication: &server_config::Authentication,
) -> Option<lancedb::remote::ClientConfig> {
    if tls == &server_config::TlsOptions::default()
        && matches!(authentication, server_config::Authentication::ApiKey(_))
    {
        return None;
    }
    let mut config = lancedb::remote::ClientConfig::default();
    if tls != &server_config::TlsOptions::default() {
        config.tls_config = Some(lancedb::remote::TlsConfig {
            ssl_ca_cert: tls.ca_cert.clone(),
            cert_file: tls.client_cert.clone(),
            key_file: tls.client_key.clone(),
            ..Default::default()
        });
    }
    if !matches!(authentication, server_config::Authentication::ApiKey(_)) {
        config.header_provider = Some(std::sync::Arc::new(authentication.clone()));
    }
    Some(config)
}

pub mod job;
pub mod job_children;

static BUILD_VERSION: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();
/// Set the version displayed by shared command handlers when embedded in another binary.
pub fn set_build_version(version: &'static str) {
    let _ = BUILD_VERSION.set(version);
}
pub fn build_version() -> &'static str {
    BUILD_VERSION.get().copied().unwrap_or(VERSION)
}
