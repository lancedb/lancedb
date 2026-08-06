// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]

use arrow::util::pretty::print_batches;
use arrow_array::RecordBatch;
use clap::{Parser, Subcommand, ValueEnum};
use futures::TryStreamExt;
use futures::stream::StreamExt;

use lance::dataset::Dataset;
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance::{Error, Result};
use lance_linalg::distance::MetricType;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Dataset inspection
    Inspect {
        /// The URI of the dataset.
        uri: String,
    },

    /// Query the dataset
    Query {
        uri: String,

        /// The counts of record to print.
        #[arg(short, default_value_t = 100)]
        n: i64,
    },

    /// Index operations
    Index {
        /// Actions on index
        #[arg(value_enum)]
        action: IndexAction,

        /// Dataset URI.
        uri: String,

        /// The column to build index on.
        #[arg(short, long, value_name = "NAME")]
        column: Option<String>,

        /// Index name.
        #[arg(short, long)]
        name: Option<String>,

        /// Set index type
        #[arg(short = 't', long = "type", value_enum, value_name = "TYPE")]
        index_type: Option<IndexType>,

        /// Number of IVF partitions. Only useful when the index type is 'ivf-pq'.
        #[arg(short = 'p', long, default_value_t = 64, value_name = "NUM")]
        num_partitions: usize,

        /// Number of sub-vectors in Product Quantizer
        #[arg(short = 's', long, default_value_t = 8, value_name = "NUM")]
        num_sub_vectors: usize,

        /// Distance metric type. Only support 'l2' and 'cosine'.
        #[arg(short = 'm', long, value_name = "DISTANCE")]
        metric_type: Option<String>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum IndexAction {
    Create,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum IndexType {
    IvfPQ,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        Commands::Inspect { uri } => {
            let dataset = Dataset::open(uri).await.unwrap();
            println!("Dataset URI: {}", uri);
            println!(
                "Latest version: {}, Total versions: {}",
                dataset.version().version,
                dataset.versions().await.unwrap().len()
            );
            println!("Total records: {}", dataset.count_rows(None).await.unwrap());
            println!("Schema:\n{}", dataset.schema());

            Ok(())
        }
        Commands::Query { uri, n } => {
            let dataset = Dataset::open(uri).await.unwrap();
            let mut scanner = dataset.scan();
            scanner.limit(Some(*n), None).unwrap();
            let stream = scanner.try_into_stream().await.unwrap();
            let batch: Vec<RecordBatch> = stream.take(1).try_collect::<Vec<_>>().await.unwrap();

            // pretty print the batch
            print_batches(&batch)?;

            Ok(())
        }
        Commands::Index {
            action,
            uri,
            column,
            name,
            index_type,
            num_partitions,
            num_sub_vectors,
            metric_type,
        } => {
            let mut dataset = Dataset::open(uri).await.unwrap();
            match action {
                IndexAction::Create => {
                    create_index(
                        &mut dataset,
                        name,
                        column,
                        index_type,
                        num_partitions,
                        num_sub_vectors,
                        metric_type,
                    )
                    .await
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_index(
    dataset: &mut Dataset,
    name: &Option<String>,
    column: &Option<String>,
    index_type: &Option<IndexType>,
    num_partitions: &usize,
    num_sub_vectors: &usize,
    metric_type: &Option<String>,
) -> Result<()> {
    let col = column
        .as_ref()
        .ok_or_else(|| Error::index("Must specify column".to_string()))?;
    let _ = index_type.ok_or_else(|| Error::index("Must specify index type".to_string()))?;
    let mt = match metric_type.as_ref().unwrap_or(&"l2".to_string()).as_str() {
        "l2" => MetricType::L2,
        "cosine" => MetricType::Cosine,
        _ => {
            return Err(Error::index(format!(
                "Only l2 and cosine metric type are supported, got: {}",
                metric_type.as_ref().unwrap_or(&"N/A".to_string())
            )));
        }
    };
    dataset
        .create_index(
            &[col],
            lance_index::IndexType::Vector,
            name.clone(),
            &VectorIndexParams::ivf_pq(*num_partitions, 8, *num_sub_vectors, mt, 100),
            true,
        )
        .await
        .expect("dataset create index");
    Ok(())
}
