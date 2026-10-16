// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::server_config::ConnectionArgs;
use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};

/// Public client commands. Runtime and administrative commands live in Enterprise.
#[derive(Parser, Debug)]
#[command(name = "lancedb", version, about = "LanceDB command-line client")]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manage OAuth login and cached interactive sessions
    Auth(crate::auth::AuthArgs),
    /// List, add, and delete access-control entries
    Acl(crate::acl::AclArgs),
    /// Manage connection profiles
    Profile(crate::profile::ProfileArgs),
    /// Run SQL or open an interactive shell against a remote server
    Sql(SqlArgs),
    /// Describe, cancel, list, and inspect jobs
    Job {
        #[command(flatten)]
        args: crate::job::JobArgs,
        #[arg(long, global = true, default_value = "table")]
        format: String,
    },
    /// Load data from external sources
    Load(Box<crate::load::LoadArgs>),
}

#[derive(clap::Args, Debug)]
pub struct SqlArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    /// Execute SQL and exit
    #[arg(short = 'c', long = "command", conflicts_with = "file")]
    pub command_string: Option<String>,
    /// Execute SQL commands from a file and exit
    #[arg(short = 'f', long)]
    pub file: Option<String>,
    /// Output format: table, csv, json, ndjson
    #[arg(long, default_value = "table")]
    pub format: String,
}

pub async fn run_sql(args: SqlArgs) -> Result<()> {
    let connection = args.connection.resolve(None)?;
    if let Some(file) = args.file {
        crate::client::run_client_script_with_writer(
            &connection,
            std::fs::read_to_string(file)?,
            args.format,
            None,
        )
        .await
    } else if let Some(sql) = args.command_string {
        crate::client::run_client_command_with_writer(&connection, sql, args.format, None).await
    } else {
        crate::client::run_client_with_writer(&connection, None).await
    }
}

pub async fn run(args: Args) -> Result<()> {
    match args.command {
        None => {
            Args::command().print_help()?;
            println!();
            Ok(())
        }
        Some(Commands::Auth(args)) => crate::auth::run(args, None).await,
        Some(Commands::Acl(args)) => crate::acl::run_command(args).await,
        Some(Commands::Profile(args)) => crate::profile::run_profile_command(args).await,
        Some(Commands::Sql(args)) => run_sql(args).await,
        Some(Commands::Job { args, format }) => crate::job::run_command(args, format).await,
        Some(Commands::Load(args)) => crate::load::run_load(*args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sql_is_explicit() {
        assert!(Args::try_parse_from(["lancedb"]).unwrap().command.is_none());
        assert!(matches!(
            Args::try_parse_from(["lancedb", "sql"]).unwrap().command,
            Some(Commands::Sql(_))
        ));
        assert!(Args::try_parse_from(["lancedb", "--command", "SELECT 1"]).is_err());
        assert!(
            Args::try_parse_from(["lancedb", "sql", "-c", "SELECT 1", "-f", "query.sql"]).is_err()
        );
        for command in [
            "server",
            "admin",
            "bench",
            "service",
            "pe",
            "agent",
            "management",
            "preflight",
            "run",
            "function-worker",
        ] {
            assert!(
                Args::try_parse_from(["lancedb", command]).is_err(),
                "{command}"
            );
        }
    }
}
