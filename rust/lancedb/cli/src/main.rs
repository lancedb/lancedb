// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    lancedb::docs::set_sdk_ident("cli", env!("CARGO_PKG_VERSION"));
    let _ = rustls::crypto::ring::default_provider().install_default();
    lancedb_cli::run(lancedb_cli::Args::parse()).await
}
