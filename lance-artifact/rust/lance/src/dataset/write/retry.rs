// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::time::{Duration, Instant};

use lance_core::utils::backoff::SlotBackoff;
use lance_core::{Error, Result};

use crate::Dataset;
use crate::io::commit::{maybe_timeout, timeout_error};

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub retry_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            retry_timeout: Duration::from_secs(30),
        }
    }
}

/// Trait for operations that can be retried on commit conflicts
pub trait RetryExecutor: Clone {
    type Data;
    type Result;

    /// Execute the operation logic without committing
    async fn execute_impl(&self) -> Result<Self::Data>;

    /// Commit the operation data
    async fn commit(&self, dataset: Arc<Dataset>, data: Self::Data) -> Result<Self::Result>;

    /// Update the dataset reference for retry attempts
    fn update_dataset(&mut self, dataset: Arc<Dataset>);
}

/// Execute an operation with retry logic for commit conflicts
pub async fn execute_with_retry<E: RetryExecutor>(
    executor: E,
    dataset: Arc<Dataset>,
    config: RetryConfig,
) -> Result<E::Result> {
    let start = Instant::now();
    let mut dataset_ref = dataset;
    let mut backoff = SlotBackoff::default();

    while backoff.attempt() <= config.max_retries {
        let mut executor_clone = executor.clone();
        executor_clone.update_dataset(dataset_ref.clone());

        let execute_fut = executor_clone.execute_impl();
        let execute_fut =
            maybe_timeout(backoff.attempt(), start, config.retry_timeout, execute_fut);
        let data = execute_fut.await??;

        let commit_future = executor.commit(dataset_ref.clone(), data);
        let commit_future = maybe_timeout(
            backoff.attempt(),
            start,
            config.retry_timeout,
            commit_future,
        );

        match commit_future.await? {
            Ok(result) => return Ok(result),
            Err(Error::RetryableCommitConflict { .. }) => {
                // Check whether we have exhausted our retries *before* we sleep.
                if backoff.attempt() >= config.max_retries {
                    break;
                }
                if start.elapsed() > config.retry_timeout {
                    return Err(timeout_error(config.retry_timeout, backoff.attempt() + 1));
                }
                if backoff.attempt() == 0 {
                    // We add 10% buffer here, to allow concurrent writes to complete.
                    // We pass the first attempt's time to the backoff so it's used
                    // as the unit for backoff time slots.
                    // See SlotBackoff implementation for more details on how this works.
                    backoff = backoff.with_unit((start.elapsed().as_millis() * 11 / 10) as u32);
                }

                let sleep_fut = tokio::time::sleep(backoff.next_backoff());
                let sleep_fut =
                    maybe_timeout(backoff.attempt(), start, config.retry_timeout, sleep_fut);
                sleep_fut.await?;

                let mut ds = dataset_ref.as_ref().clone();
                ds.checkout_latest().await?;
                dataset_ref = Arc::new(ds);
                continue;
            }
            Err(e) => return Err(e),
        }
    }

    Err(Error::too_much_write_contention(format!(
        "Attempted {} retries.",
        config.max_retries
    )))
}
