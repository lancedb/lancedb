// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use clap::{Args, Parser, Subcommand};
use lance_core::Result;

#[derive(Parser, Debug)]
#[command(
    name = "lance-tools",
    about = "Tools for interacting with Lance files and tables",
    version
)]
pub struct LanceToolsArgs {
    /// Subcommand to run
    #[command(subcommand)]
    command: LanceToolsCommand,
}

#[derive(Subcommand, Debug)]
pub enum LanceToolsCommand {
    /// Commands for interacting with Lance files.
    File(LanceFileArgs),
}

#[derive(Parser, Debug)]
pub struct LanceFileArgs {
    #[command(subcommand)]
    command: LanceFileCommand,
}

#[derive(Subcommand, Debug)]
pub enum LanceFileCommand {
    /// Display Lance file metadata.
    Meta(LanceFileMetaArgs),
}

#[derive(Args, Debug)]
pub struct LanceFileMetaArgs {
    // The Lance file to examine.
    #[arg(short = 's', long, value_name = "source")]
    pub(crate) source: String,
}

impl LanceToolsArgs {
    pub async fn run(&self, writer: impl std::io::Write) -> Result<()> {
        match &self.command {
            LanceToolsCommand::File(args) => match &args.command {
                LanceFileCommand::Meta(args) => crate::meta::show_file_meta(writer, args).await,
            },
        }
    }
}
