// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use lance::dataset::ReadParams;
use lance::dataset::builder::DatasetBuilder;
use lance::index::{CreateIndexBuilder, DatasetIndexExt};
use lance::{Error, Result};
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_io::object_store::{ObjectStoreParams, StorageOptionsAccessor};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(value_enum)]
    action: Action,

    #[arg(long)]
    uri: String,

    #[arg(long, default_value = "full_content_idx")]
    index_name: String,

    #[arg(long, default_value = "full_content")]
    column: String,

    #[arg(long, default_value = "oailancepub")]
    storage_account: String,

    #[arg(long, value_parser = parse_storage_option)]
    storage_option: Vec<(String, String)>,

    #[arg(long, default_value_t = 1)]
    num_segments: u32,

    #[arg(long)]
    index_uuid: Option<Uuid>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Action {
    List,
    Drop,
    Create,
}

fn parse_storage_option(value: &str) -> std::result::Result<(String, String), String> {
    let Some((key, val)) = value.split_once('=') else {
        return Err("storage options must be key=value".to_string());
    };
    if key.is_empty() {
        return Err("storage option key cannot be empty".to_string());
    }
    Ok((key.to_string(), val.to_string()))
}

async fn open_dataset(args: &Args) -> Result<lance::Dataset> {
    let mut options = HashMap::from([("account_name".to_string(), args.storage_account.clone())]);
    for (key, val) in &args.storage_option {
        options.insert(key.clone(), val.clone());
    }
    let read_params = ReadParams {
        store_options: Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        }),
        ..Default::default()
    };

    DatasetBuilder::from_uri(&args.uri)
        .with_read_params(read_params)
        .load()
        .await
}

fn fm_params(num_segments: u32) -> ScalarIndexParams {
    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::Fm);
    if num_segments == 1 {
        params
    } else {
        params.with_params(&serde_json::json!({ "num_segments": num_segments }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.num_segments == 0 {
        return Err(Error::invalid_input(
            "--num-segments must be greater than 0",
        ));
    }

    println!("opening dataset {}", args.uri);
    let mut dataset = open_dataset(&args).await?;
    println!("dataset version {}", dataset.version().version);

    match args.action {
        Action::List => {
            let indices = dataset.load_indices().await?;
            println!("indices={}", indices.len());
            for index in indices.iter() {
                println!(
                    "name={} uuid={} version={} fields={:?} fragments={}",
                    index.name,
                    index.uuid,
                    index.index_version,
                    index.fields,
                    index
                        .fragment_bitmap
                        .as_ref()
                        .map(|bitmap| bitmap.len())
                        .unwrap_or(0)
                );
            }
        }
        Action::Drop => {
            let start = Instant::now();
            dataset.drop_index(&args.index_name).await?;
            println!(
                "dropped index {} in {:.3}s; new version {}",
                args.index_name,
                start.elapsed().as_secs_f64(),
                dataset.version().version
            );
        }
        Action::Create => {
            let params = fm_params(args.num_segments);
            let start = Instant::now();
            let mut builder = CreateIndexBuilder::new(
                &mut dataset,
                &[args.column.as_str()],
                lance_index::IndexType::Fm,
                &params,
            )
            .name(args.index_name.clone())
            .replace(true);
            if let Some(index_uuid) = args.index_uuid {
                builder = builder.index_uuid(index_uuid);
            }
            let metadata = builder.await?;
            println!(
                "created index {} uuid={} in {:.3}s; new version {}",
                metadata.name,
                metadata.uuid,
                start.elapsed().as_secs_f64(),
                dataset.version().version
            );
        }
    }
    Ok(())
}
