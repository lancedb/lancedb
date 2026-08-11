// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Release benchmark for opening a missing table as sibling-table cardinality grows.
//!
//! The fixture uses real `.lance` directories and marker files. Fixture creation is
//! outside the timed section. Defaults intentionally cover 1k, 10k, and 100k siblings
//! with 10 warmups and 100 distinct missing-table opens per scale:
//!
//! ```text
//! cargo run --release -p lancedb --example bench_open_missing_table
//! ```
//!
//! `BENCH_SIBLINGS`, `BENCH_WARMUPS`, and `BENCH_TRIALS` override those defaults.
//! Reduced settings are useful only as a smoke test. Performance comparisons require
//! the same machine, filesystem, fixture sizes, settings, lockfile, and alternating
//! baseline/candidate execution order.

use std::fs::{self, File};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use lancedb::connection::Connection;
use lancedb::{Error, connect};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn sibling_counts() -> Result<Vec<usize>> {
    let raw = std::env::var("BENCH_SIBLINGS").unwrap_or_else(|_| "1000,10000,100000".into());
    let mut counts = raw
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<usize>()
                .with_context(|| format!("invalid BENCH_SIBLINGS value: {value}"))
        })
        .collect::<Result<Vec<_>>>()?;
    counts.sort_unstable();
    counts.dedup();
    if counts.is_empty() || counts[0] == 0 {
        bail!("BENCH_SIBLINGS must contain positive cardinalities");
    }
    Ok(counts)
}

fn add_siblings(database_path: &std::path::Path, start: usize, end: usize) -> Result<()> {
    for index in start..end {
        let table_path = database_path.join(format!("sibling_{index:06}.lance"));
        fs::create_dir(&table_path)
            .with_context(|| format!("creating table prefix {table_path:?}"))?;
        File::create(table_path.join("_marker"))
            .with_context(|| format!("creating marker under {table_path:?}"))?;
    }
    Ok(())
}

async fn time_missing_open(db: &Connection, name: &str) -> Result<Duration> {
    let started = Instant::now();
    let result = db.open_table(name).execute().await;
    let elapsed = started.elapsed();
    match result {
        Err(Error::TableNotFound { .. }) => Ok(elapsed),
        Err(error) => bail!("expected TableNotFound for {name}, got {error:?}"),
        Ok(_) => bail!("benchmark missing-table name unexpectedly exists: {name}"),
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    sorted[rank]
}

#[tokio::main]
async fn main() -> Result<()> {
    let counts = sibling_counts()?;
    let warmups = env_usize("BENCH_WARMUPS", 10);
    let trials = env_usize("BENCH_TRIALS", 100);
    if warmups == 0 || trials == 0 {
        bail!("BENCH_WARMUPS and BENCH_TRIALS must be positive");
    }

    let fixture = tempfile::tempdir().context("creating benchmark fixture")?;
    let database_path = fixture.path().join("database");
    fs::create_dir(&database_path).context("creating benchmark database directory")?;
    let db = connect(database_path.to_str().context("non-UTF-8 fixture path")?)
        .execute()
        .await?;

    println!(
        "config: siblings={counts:?} warmups={warmups} trials={trials} profile={} os={} arch={}",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::OS,
        std::env::consts::ARCH,
    );
    println!("lower is better; fixture setup and teardown are excluded");
    println!("| siblings | samples | p50 | p95 | max |");
    println!("| ---: | ---: | ---: | ---: | ---: |");

    let mut created = 0;
    for sibling_count in counts {
        add_siblings(&database_path, created, sibling_count)?;
        created = sibling_count;

        for index in 0..warmups {
            let name = format!("__missing_warmup_{sibling_count}_{index}");
            let _ = time_missing_open(&db, &name).await?;
        }

        let mut samples = Vec::with_capacity(trials);
        for index in 0..trials {
            let name = format!("__missing_trial_{sibling_count}_{index}");
            samples.push(time_missing_open(&db, &name).await?);
        }
        samples.sort_unstable();

        println!(
            "| {sibling_count} | {} | {:?} | {:?} | {:?} |",
            samples.len(),
            percentile(&samples, 50),
            percentile(&samples, 95),
            samples[samples.len() - 1],
        );
    }

    Ok(())
}
