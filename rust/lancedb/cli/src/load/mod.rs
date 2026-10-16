// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Load data from external sources into LanceDB tables.
//!
//! Supports loading from:
//! - HuggingFace datasets
//! - OSS Lance datasets (local path or object-store URI)
//!
//! # Example
//!
//! ```bash
//! # Load from HuggingFace dataset
//! lancedb load --source huggingface \
//!   --dataset "KShivendu/dbpedia-entities-openai-1M" \
//!   --table embeddings \
//!   --vector-column openai
//!
//! # Load from an existing Lance dataset on disk or in object storage
//! lancedb load --source lance \
//!   --dataset s3://bucket/path/dataset.lance \
//!   --table embeddings \
//!   --vector-column openai
//! ```

pub mod schema;
pub mod sources;
pub mod writer;

use anyhow::{Context, Result};
use clap::Parser;
use sources::DataSource;
use tracing::info;

use crate::server_config::ConnectionArgs;

#[derive(Parser, Debug)]
#[command(
    about = "Load data from external sources into LanceDB",
    after_help = r#"EXAMPLES:
    # Load a HuggingFace dataset with vector index
    lancedb load -s hf --dataset "KShivendu/dbpedia-entities-openai-1M" -t dbpedia --vector-column openai

    # Load with both vector and FTS indexes, using 256-entry FTS posting blocks
    lancedb load -s hf --dataset "KShivendu/dbpedia-entities-openai-1M" -t dbpedia --vector-column openai --text-column text --fts-block-size 256

    # Load from an existing OSS Lance dataset
    lancedb load -s lance --dataset s3://bucket/path/dataset.lance -t mytable --vector-column embedding

    # Load into a local LanceDB database
    lancedb load -s lance --dataset /data/source.lance -t mytable --destination /data/my_lancedb
"#
)]
pub struct LoadArgs {
    /// Source type: huggingface (alias: hf), lance
    #[arg(long, short = 's')]
    pub source: String,

    /// Dataset identifier. HuggingFace dataset id (e.g.
    /// "KShivendu/dbpedia-entities-openai-1M") or a Lance dataset URI
    /// (`file:///path`, `s3://bucket/path`, etc).
    #[arg(long)]
    pub dataset: Option<String>,

    /// Target table name
    #[arg(long, short = 't')]
    pub table: String,

    /// Connection settings
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Destination HTTP endpoint or local database path; overrides the profile's HTTP endpoint
    #[arg(long, value_name = "URL_OR_PATH", conflicts_with = "http_server_url")]
    pub destination: Option<String>,

    /// Batch size for streaming imports
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Vector column name to build IVF-PQ index on
    #[arg(long)]
    pub vector_column: Option<String>,

    /// Text column name to build FTS index on
    #[arg(long)]
    pub text_column: Option<String>,

    /// FTS posting block size (128 or 256; 256 is experimental)
    #[arg(long, value_name = "SIZE", requires = "text_column")]
    pub fts_block_size: Option<usize>,

    /// Overwrite table if it exists
    #[arg(long)]
    pub overwrite: bool,

    /// HuggingFace dataset split (default: train)
    #[arg(long, default_value = "train")]
    pub split: String,

    /// Wait for indexes to finish building. Optionally specify stall timeout in minutes (default: 30).
    /// Stall detection triggers if indexed row count doesn't change within the timeout.
    #[arg(long, value_name = "MINUTES", default_missing_value = "30", num_args = 0..=1)]
    pub wait_for_index: Option<u64>,
}

impl LoadArgs {
    /// Get the dataset value
    pub fn get_dataset(&self) -> Result<&str> {
        self.dataset
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("--dataset must be specified"))
    }
}

/// Main entry point for the load command
pub async fn run_load(args: LoadArgs) -> Result<()> {
    let mut connection = args.connection.clone();
    if let Some(destination) = &args.destination {
        connection.http_server_url = Some(destination.clone());
    }
    let conn = connection.resolve(None)?;

    info!("Load configuration:");
    info!("  Source: {}", args.source);
    info!("  Target table: {}", args.table);
    info!("  Destination: {}", conn.http_server_url);
    info!("  Database: {}", conn.database);

    let dataset = args.get_dataset()?;
    info!("  Dataset: {}", dataset);

    if let Some(ref vc) = args.vector_column {
        info!("  Vector index column: {}", vc);
    }
    if let Some(ref tc) = args.text_column {
        info!("  FTS index column: {}", tc);
    }
    // Create the appropriate data source
    let source: Box<dyn DataSource> = match args.source.as_str() {
        "huggingface" | "hf" => Box::new(sources::huggingface::HuggingFaceSource::new(
            dataset.to_string(),
            args.split.clone(),
        )?),
        "lance" => Box::new(sources::lance::LanceSource::new(dataset.to_string())?),
        other => {
            anyhow::bail!(
                "Unknown source type: {}. Supported: huggingface (alias: hf), lance",
                other
            );
        }
    };

    // Run the load
    writer::load_to_lancedb(&args, &conn, source.as_ref())
        .await
        .context("Failed to load data")?;

    info!("Load completed successfully!");
    Ok(())
}
