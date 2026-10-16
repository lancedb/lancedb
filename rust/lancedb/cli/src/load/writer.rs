// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Streaming writer for loading data into LanceDB.

use anyhow::{Context, Result};
use arrow_array::RecordBatchIterator;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::Index;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::info;

/// Polling interval for checking index stats
const INDEX_POLL_INTERVAL: Duration = Duration::from_secs(5);

use super::schema::{transform_batch, transform_schema, VectorColumnInfo};
use super::sources::DataSource;
use super::LoadArgs;
use crate::connection::create_database_connection;
use crate::server_config::ResolvedConnection;

/// Track the state of an index being built
struct IndexBuildState {
    /// Name of the index
    index_name: String,
    /// Column(s) the index covers
    columns: Vec<String>,
    /// Last observed indexed row count
    last_indexed_rows: usize,
    /// Time when we last observed progress
    last_progress_time: Instant,
    /// Total rows to be indexed
    total_rows: usize,
    /// Whether this index is complete
    complete: bool,
}

/// Wait for indexes to appear in the table's index list.
/// Returns the initial IndexBuildState for each found index.
async fn wait_for_indexes_to_appear(
    table: &lancedb::Table,
    index_columns: &[String],
    timeout: Duration,
) -> Result<Vec<IndexBuildState>> {
    println!(
        "Waiting for {} index(es) to appear (timeout: {} min)...",
        index_columns.len(),
        timeout.as_secs() / 60
    );
    println!("  Looking for indexes on columns: {:?}", index_columns);

    let start_time = Instant::now();

    loop {
        let indices = table
            .list_indices()
            .await
            .context("Failed to list indexes")?;

        let index_states: Vec<_> = indices
            .iter()
            .filter(|index| {
                index
                    .columns
                    .iter()
                    .any(|c| index_columns.iter().any(|ic| ic == c))
            })
            .map(|index| IndexBuildState {
                index_name: index.name.clone(),
                columns: index.columns.clone(),
                last_indexed_rows: 0,
                last_progress_time: Instant::now(),
                total_rows: 0,
                complete: false,
            })
            .collect();

        if index_states.len() >= index_columns.len() {
            println!(
                "Found {} index(es), waiting for builds to complete...",
                index_states.len()
            );
            return Ok(index_states);
        }

        // Show progress periodically
        if start_time.elapsed().as_secs().is_multiple_of(30) || start_time.elapsed().as_secs() < 5 {
            let all_columns: Vec<_> = indices.iter().map(|i| i.columns.clone()).collect();
            println!(
                "  Found {}/{} indexes. All index columns: {:?}",
                index_states.len(),
                index_columns.len(),
                all_columns
            );
        }

        if start_time.elapsed() > timeout {
            let found_columns: Vec<_> = index_states.iter().map(|s| s.columns.clone()).collect();
            anyhow::bail!(
                "Timed out waiting for indexes to appear after {} minutes. Expected columns: {:?}, found: {:?}",
                timeout.as_secs() / 60,
                index_columns,
                found_columns
            );
        }

        tokio::time::sleep(INDEX_POLL_INTERVAL).await;
    }
}

/// Poll index build progress until all indexes are complete or stalled.
async fn poll_index_completion(
    table: &lancedb::Table,
    index_states: &mut [IndexBuildState],
    stall_timeout: Duration,
) -> Result<()> {
    loop {
        let mut all_complete = true;
        let mut status_parts: Vec<String> = Vec::new();

        for state in index_states.iter_mut() {
            if state.complete {
                continue;
            }

            match table.index_stats(&state.index_name).await {
                Ok(Some(stats)) => {
                    let indexed_rows = stats.num_indexed_rows;
                    let unindexed_rows = stats.num_unindexed_rows;
                    let total = indexed_rows + unindexed_rows;

                    if total > 0 && state.total_rows == 0 {
                        state.total_rows = total;
                    }

                    if unindexed_rows == 0 && indexed_rows > 0 {
                        state.complete = true;
                        println!(
                            "  Index on '{}' complete ({} rows)",
                            state.columns.join(", "),
                            indexed_rows
                        );
                        continue;
                    }

                    if indexed_rows > state.last_indexed_rows {
                        state.last_indexed_rows = indexed_rows;
                        state.last_progress_time = Instant::now();
                    }

                    let percent = (indexed_rows * 100)
                        .checked_div(state.total_rows)
                        .unwrap_or(0);
                    status_parts.push(format!(
                        "{}: {}% ({}/{})",
                        state.columns.join(","),
                        percent,
                        indexed_rows,
                        state.total_rows
                    ));

                    if state.last_progress_time.elapsed() > stall_timeout {
                        anyhow::bail!(
                            "Index on {} appears stalled: no progress in {} minutes. Last indexed: {}, Unindexed: {}",
                            state.columns.join(", "),
                            stall_timeout.as_secs() / 60,
                            indexed_rows,
                            unindexed_rows
                        );
                    }

                    all_complete = false;
                }
                Ok(None) => {
                    all_complete = false;
                    status_parts.push(format!("{}: waiting...", state.columns.join(",")));
                }
                Err(e) => {
                    info!("Error getting index stats for {}: {}", state.index_name, e);
                    all_complete = false;
                }
            }
        }

        if all_complete {
            println!("All indexes complete!");
            return Ok(());
        }

        if !status_parts.is_empty() {
            println!("  Indexing: {}", status_parts.join(" | "));
        }

        tokio::time::sleep(INDEX_POLL_INTERVAL).await;
    }
}

/// Create a progress bar for tracking row loads.
fn create_load_progress_bar(total_rows: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_rows);
    let template = if total_rows > 0 {
        "[{bar:40}] {pos}/{len} ({percent}%) {msg}"
    } else {
        "[{bar:40}] {pos} rows {msg}"
    };
    pb.set_style(
        ProgressStyle::default_bar()
            .template(template)
            .unwrap()
            .progress_chars("=> "),
    );
    pb
}

/// Submit index builds for the specified columns.
/// Returns the list of columns that have indexes being built.
async fn submit_index_builds(
    table: &lancedb::Table,
    vector_column: Option<&str>,
    text_column: Option<&str>,
    fts_index_params: Option<FtsIndexBuilder>,
    vector_columns: &HashMap<String, VectorColumnInfo>,
) -> Result<Vec<String>> {
    let mut index_columns = Vec::new();

    if let Some(vc) = vector_column {
        if !vector_columns.contains_key(vc) {
            anyhow::bail!(
                "Column '{}' is not a vector column. Detected vector columns: {:?}",
                vc,
                vector_columns.keys().collect::<Vec<_>>()
            );
        }

        println!("Submitting IVF-PQ index build on '{}'...", vc);
        table
            .create_index(&[vc], Index::IvfPq(IvfPqIndexBuilder::default()))
            .execute()
            .await
            .context("Failed to submit vector index build")?;
        println!("IVF-PQ index on '{}' submitted", vc);
        index_columns.push(vc.to_string());
    }

    if let Some(tc) = text_column {
        let fts_index_params = fts_index_params
            .context("FTS index parameters are missing for the requested text column")?;
        println!("Submitting FTS index build on '{}'...", tc);
        table
            .create_index(&[tc], Index::FTS(fts_index_params))
            .execute()
            .await
            .context("Failed to submit FTS index build")?;
        println!("FTS index on '{}' submitted", tc);
        index_columns.push(tc.to_string());
    }

    Ok(index_columns)
}

fn build_fts_index_params(
    text_column: Option<&str>,
    block_size: Option<usize>,
) -> Result<Option<FtsIndexBuilder>> {
    if text_column.is_none() {
        if block_size.is_some() {
            anyhow::bail!("--fts-block-size requires --text-column");
        }
        return Ok(None);
    }

    let mut params = FtsIndexBuilder::default();
    if let Some(block_size) = block_size {
        params = params
            .block_size(block_size)
            .context("Invalid FTS block size")?;
    }
    Ok(Some(params))
}

/// Wait for all indexes to finish building.
async fn wait_for_indexes(
    table: &lancedb::Table,
    index_columns: &[String],
    stall_timeout_mins: u64,
) -> Result<()> {
    if index_columns.is_empty() {
        return Ok(());
    }

    let timeout = Duration::from_secs(stall_timeout_mins * 60);
    let mut index_states = wait_for_indexes_to_appear(table, index_columns, timeout).await?;
    poll_index_completion(table, &mut index_states, timeout).await
}

/// Load data from a source into a LanceDB table.
pub async fn load_to_lancedb(
    args: &LoadArgs,
    conn: &ResolvedConnection,
    source: &dyn DataSource,
) -> Result<()> {
    // Validate FTS options before connecting or loading any data. Lance owns
    // the supported block-size contract and returns the actionable error.
    let fts_index_params =
        build_fts_index_params(args.text_column.as_deref(), args.fts_block_size)?;

    // Connect to LanceDB
    let db = create_database_connection(conn)
        .await
        .context("Failed to connect to LanceDB")?;

    // Check if table exists
    let table_names = db.list_tables(Default::default()).await?.tables;
    if table_names.contains(&args.table) {
        if args.overwrite {
            info!("Dropping existing table '{}'", args.table);
            db.drop_table(&args.table, &[]).await?;
        } else {
            anyhow::bail!(
                "Table '{}' already exists. Use --overwrite to replace it.",
                args.table
            );
        }
    }

    // Get schema from source
    let source_schema = source.schema().await?;
    info!("Source schema: {:?}", source_schema);

    // Auto-detect vector columns. Each source decides whether to use its
    // schema or peek at a batch; see the trait default and per-source impls.
    let vector_columns = source.detect_vector_columns().await?;

    // Create target schema with FixedSizeList for all detected vector columns
    let target_schema = transform_schema(&source_schema, &vector_columns)?;
    info!("Target schema: {:?}", target_schema);

    // Get total row count for progress bar
    let total_row_count = source.total_rows().await?.unwrap_or(0);

    // Stream and load batches
    let load_start = Instant::now();
    let mut stream = source.stream_batches(args.batch_size);
    let mut rows_loaded = 0u64;
    let mut table: Option<lancedb::Table> = None;

    let pb = create_load_progress_bar(total_row_count);

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        // Transform batch to convert List -> FixedSizeList for vector columns
        let transformed = if !vector_columns.is_empty() {
            transform_batch(&batch, &target_schema, &vector_columns)?
        } else {
            batch
        };

        let batches: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(std::iter::once(Ok(transformed)), target_schema.clone()),
        );
        if let Some(existing) = &table {
            existing
                .add(batches)
                .execute()
                .await
                .context("Failed to append data")?;
        } else {
            info!("Creating table '{}'...", args.table);
            let created = db
                .create_table(&args.table, batches)
                .execute()
                .await
                .context("Failed to create table")?;
            table = Some(created);
        }

        rows_loaded += num_rows as u64;

        // Update progress bar
        let elapsed = load_start.elapsed().as_secs_f64();
        let rows_per_sec = if elapsed > 0.0 {
            rows_loaded as f64 / elapsed
        } else {
            0.0
        };
        pb.set_position(rows_loaded);
        pb.set_message(format!("{:.0} rows/sec", rows_per_sec));
    }

    let load_duration = load_start.elapsed();
    let final_rows_per_sec = if load_duration.as_secs_f64() > 0.0 {
        rows_loaded as f64 / load_duration.as_secs_f64()
    } else {
        0.0
    };
    pb.finish_with_message(format!(
        "{} rows in {:.1}s ({:.0} rows/sec)",
        rows_loaded,
        load_duration.as_secs_f64(),
        final_rows_per_sec
    ));

    // Create indexes if requested
    if let Some(ref table) = table {
        let index_columns = submit_index_builds(
            table,
            args.vector_column.as_deref(),
            args.text_column.as_deref(),
            fts_index_params,
            &vector_columns,
        )
        .await?;

        // Wait for indexes to complete if requested
        if let Some(stall_timeout_mins) = args.wait_for_index {
            if !index_columns.is_empty() {
                wait_for_indexes(table, &index_columns, stall_timeout_mins)
                    .await
                    .context("Failed waiting for indexes to complete")?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_fts_index_params_uses_default_block_size() {
        let params = build_fts_index_params(Some("text"), None).unwrap().unwrap();

        assert_eq!(params.posting_block_size(), 128);
    }

    #[test]
    fn test_build_fts_index_params_sets_block_size() {
        let params = build_fts_index_params(Some("text"), Some(256))
            .unwrap()
            .unwrap();

        assert_eq!(params.posting_block_size(), 256);
    }

    #[test]
    fn test_build_fts_index_params_uses_lance_validation() {
        let err = build_fts_index_params(Some("text"), Some(129)).unwrap_err();

        assert!(
            format!("{err:#}")
                .contains("FTS inverted index block_size must be one of 128 or 256, got 129"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn test_build_fts_index_params_requires_text_column() {
        let err = build_fts_index_params(None, Some(256)).unwrap_err();

        assert_eq!(err.to_string(), "--fts-block-size requires --text-column");
    }
}
