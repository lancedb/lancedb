// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::error::Result;
use crate::table::BaseTable;
use crate::Error;
use log::debug;
use std::time::{Duration, Instant};
use tokio::time::sleep;
/// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
/// are not fully indexed within the timeout.
pub async fn wait_for_index(
    table: &dyn BaseTable,
    index_names: &[&str],
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    let mut remaining = index_names.to_vec();

    // poll via list_indices() and index_stats() until all indices are created and fully indexed
    while start.elapsed() < timeout {
        let mut completed = vec![];
        let indices = table.list_indices().await?;

        for idx in index_names {
            if !indices.iter().any(|i| i.name == *idx) {
                debug!("still waiting for new index '{}'", idx);
                continue;
            }

            let stats = table.index_stats(idx.as_ref()).await?;
            match stats {
                None => {
                    debug!("still waiting for new index '{}'", idx);
                    continue;
                }
                Some(s) => {
                    if s.num_unindexed_rows == 0 {
                        completed.push(idx);
                        debug!(
                            "fully indexed '{}'. indexed rows: {}",
                            idx, s.num_indexed_rows
                        );
                    } else {
                        debug!(
                            "still waiting for index '{}'. unindexed rows: {}",
                            idx, s.num_unindexed_rows
                        );
                    }
                }
            }
        }
        remaining.retain(|idx| !completed.contains(&idx));
        if remaining.is_empty() {
            return Ok(());
        }
        sleep(Duration::from_millis(1000)).await;
    }
    Err(Error::Timeout {
        message: format!(
            "timed out waiting for indices: {:?} after {:?}",
            remaining, timeout
        )
        .to_string(),
    })
}
