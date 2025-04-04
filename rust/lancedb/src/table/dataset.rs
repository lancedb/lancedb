// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{self, Duration, Instant},
};

use futures::FutureExt;
use lance::Dataset;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::Result;

/// A wrapper around a [Dataset] that provides lazy-loading and consistency checks.
///
/// This can be cloned cheaply. It supports concurrent reads or exclusive writes.
#[derive(Debug, Clone)]
pub struct DatasetConsistencyWrapper(Arc<RwLock<DatasetRef>>);

/// A wrapper around a [Dataset] that provides consistency checks.
///
/// The dataset is lazily loaded, and starts off as None. On the first access,
/// the dataset is loaded.
#[derive(Debug)]
enum DatasetRef {
    /// In this mode, the dataset is always the latest version.
    Latest {
        dataset: Dataset,
        read_consistency_interval: Option<Duration>,
        last_consistency_check: Option<time::Instant>,
        /// A background task loading the next version of the dataset. This happens
        /// in the background so as not to block the current thread.
        refresh_task: Option<tokio::task::JoinHandle<Result<Dataset>>>,
    },
    /// In this mode, the dataset is a specific version. It cannot be mutated.
    TimeTravel { dataset: Dataset, version: u64 },
}

impl DatasetRef {
    /// Reload the dataset to the appropriate version.
    async fn reload(&mut self) -> Result<()> {
        match self {
            Self::Latest {
                dataset,
                last_consistency_check,
                refresh_task,
                ..
            } => {
                // Replace the refresh task
                if let Some(refresh_task) = refresh_task {
                    refresh_task.abort();
                }
                let mut new_dataset = dataset.clone();
                refresh_task.replace(tokio::spawn(async move {
                    new_dataset.checkout_latest().await?;
                    Ok(new_dataset)
                }));
                last_consistency_check.replace(Instant::now());
            }
            Self::TimeTravel { dataset, version } => {
                dataset.checkout_version(*version).await?;
            }
        }
        Ok(())
    }

    fn is_latest(&self) -> bool {
        matches!(self, Self::Latest { .. })
    }

    fn strong_consistency(&self) -> bool {
        matches!(
            self,
            Self::Latest { read_consistency_interval: Some(interval), .. }
            if interval.as_nanos() == 0
        )
    }

    async fn as_latest(&mut self, read_consistency_interval: Option<Duration>) -> Result<()> {
        match self {
            Self::Latest { .. } => Ok(()),
            Self::TimeTravel { dataset, .. } => {
                dataset.checkout_latest().await?;
                *self = Self::Latest {
                    dataset: dataset.clone(),
                    read_consistency_interval,
                    last_consistency_check: Some(Instant::now()),
                    refresh_task: None,
                };
                Ok(())
            }
        }
    }

    async fn as_time_travel(&mut self, target_version: u64) -> Result<()> {
        match self {
            Self::Latest { dataset, .. } => {
                *self = Self::TimeTravel {
                    dataset: dataset.checkout_version(target_version).await?,
                    version: target_version,
                };
            }
            Self::TimeTravel { dataset, version } => {
                if *version != target_version {
                    *self = Self::TimeTravel {
                        dataset: dataset.checkout_version(target_version).await?,
                        version: target_version,
                    };
                }
            }
        }
        Ok(())
    }

    fn time_travel_version(&self) -> Option<u64> {
        match self {
            Self::Latest { .. } => None,
            Self::TimeTravel { version, .. } => Some(*version),
        }
    }

    fn set_latest(&mut self, dataset: Dataset) {
        match self {
            Self::Latest {
                dataset: ref mut ds,
                refresh_task,
                last_consistency_check,
                ..
            } => {
                *ds = dataset;
                if let Some(refresh_task) = refresh_task {
                    refresh_task.abort();
                }
                *refresh_task = None;
                *last_consistency_check = Some(Instant::now());
            }
            _ => unreachable!("Dataset should be in latest mode at this point"),
        }
    }

    /// Wait for the background refresh task to complete.
    async fn await_refresh(&mut self) -> Result<()> {
        if let Self::Latest {
            refresh_task: Some(refresh_task),
            read_consistency_interval,
            ..
        } = self
        {
            let dataset = refresh_task.await.expect("Refresh task panicked")?;
            *self = Self::Latest {
                dataset,
                read_consistency_interval: *read_consistency_interval,
                last_consistency_check: Some(Instant::now()),
                refresh_task: None,
            };
        }
        Ok(())
    }

    /// Check if background refresh task is done, and if so, update the dataset.
    fn check_refresh(&mut self) -> Result<()> {
        if let Self::Latest {
            refresh_task: Some(refresh_task),
            read_consistency_interval,
            ..
        } = self
        {
            if refresh_task.is_finished() {
                let dataset = refresh_task
                    .now_or_never()
                    .unwrap()
                    .expect("Refresh task panicked")?;
                *self = Self::Latest {
                    dataset,
                    read_consistency_interval: *read_consistency_interval,
                    last_consistency_check: Some(Instant::now()),
                    refresh_task: None,
                };
            }
        }
        Ok(())
    }

    fn refresh_is_ready(&self) -> bool {
        matches!(
            self,
            Self::Latest {
                refresh_task: Some(refresh_task),
                ..
            }
            if refresh_task.is_finished()
        )
    }
}

impl DatasetConsistencyWrapper {
    /// Create a new wrapper in the latest version mode.
    pub fn new_latest(dataset: Dataset, read_consistency_interval: Option<Duration>) -> Self {
        Self(Arc::new(RwLock::new(DatasetRef::Latest {
            dataset,
            read_consistency_interval,
            last_consistency_check: Some(Instant::now()),
            refresh_task: None,
        })))
    }

    /// Get an immutable reference to the dataset.
    pub async fn get(&self) -> Result<DatasetReadGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetReadGuard {
            guard: self.0.read().await,
        })
    }

    /// Get a mutable reference to the dataset.
    ///
    /// If the dataset is in time travel mode this will fail
    pub async fn get_mut(&self) -> Result<DatasetWriteGuard<'_>> {
        self.ensure_mutable().await?;
        self.ensure_up_to_date().await?;
        Ok(DatasetWriteGuard {
            guard: self.0.write().await,
        })
    }

    /// Get a mutable reference to the dataset without requiring the
    /// dataset to be in a Latest mode.
    pub async fn get_mut_unchecked(&self) -> Result<DatasetWriteGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetWriteGuard {
            guard: self.0.write().await,
        })
    }

    /// Convert into a wrapper in latest version mode
    pub async fn as_latest(&self, read_consistency_interval: Option<Duration>) -> Result<()> {
        if self.0.read().await.is_latest() {
            return Ok(());
        }

        let mut write_guard = self.0.write().await;
        if write_guard.is_latest() {
            return Ok(());
        }

        write_guard.as_latest(read_consistency_interval).await
    }

    pub async fn as_time_travel(&self, target_version: u64) -> Result<()> {
        self.0.write().await.as_time_travel(target_version).await
    }

    /// Provide a known latest version of the dataset.
    ///
    /// This is usually done after some write operation, which inherently will
    /// have the latest version.
    pub async fn set_latest(&self, dataset: Dataset) {
        self.0.write().await.set_latest(dataset);
    }

    pub async fn reload(&self) -> Result<()> {
        let mut write_guard = self.0.write().await;
        write_guard.reload().await?;
        write_guard.await_refresh().await
    }

    /// Returns the version, if in time travel mode, or None otherwise
    pub async fn time_travel_version(&self) -> Option<u64> {
        self.0.read().await.time_travel_version()
    }

    pub async fn ensure_mutable(&self) -> Result<()> {
        let dataset_ref = self.0.read().await;
        match &*dataset_ref {
            DatasetRef::Latest { .. } => Ok(()),
            DatasetRef::TimeTravel { .. } => Err(crate::Error::InvalidInput {
                message: "table cannot be modified when a specific version is checked out"
                    .to_string(),
            }),
        }
    }

    async fn is_up_to_date(&self) -> Result<bool> {
        let dataset_ref = self.0.read().await;
        match &*dataset_ref {
            DatasetRef::Latest {
                read_consistency_interval,
                last_consistency_check,
                ..
            } => match (read_consistency_interval, last_consistency_check) {
                (None, _) => Ok(true),
                (Some(_), None) => Ok(false),
                (Some(read_consistency_interval), Some(last_consistency_check)) => {
                    if &last_consistency_check.elapsed() < read_consistency_interval {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
            },
            DatasetRef::TimeTravel { dataset, version } => {
                Ok(dataset.version().version == *version)
            }
        }
    }

    /// Ensures that the dataset is loaded and up-to-date with consistency and
    /// version parameters.
    async fn ensure_up_to_date(&self) -> Result<()> {
        // We may have previously created a background task to fetch the new
        // version of the dataset. If that task is done, we should update the
        // dataset.
        {
            let read_guard = self.0.read().await;
            if read_guard.refresh_is_ready() {
                drop(read_guard);
                self.0.write().await.check_refresh()?;
            }
        }

        if !self.is_up_to_date().await? {
            self.reload().await?;
        }

        // If we are in strong consistency mode, we should await the refresh task.
        if self.0.read().await.strong_consistency() {
            self.0.write().await.await_refresh().await?;
        }

        Ok(())
    }
}

pub struct DatasetReadGuard<'a> {
    guard: RwLockReadGuard<'a, DatasetRef>,
}

impl Deref for DatasetReadGuard<'_> {
    type Target = Dataset;

    fn deref(&self) -> &Self::Target {
        match &*self.guard {
            DatasetRef::Latest { dataset, .. } => dataset,
            DatasetRef::TimeTravel { dataset, .. } => dataset,
        }
    }
}

pub struct DatasetWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, DatasetRef>,
}

impl Deref for DatasetWriteGuard<'_> {
    type Target = Dataset;

    fn deref(&self) -> &Self::Target {
        match &*self.guard {
            DatasetRef::Latest { dataset, .. } => dataset,
            DatasetRef::TimeTravel { dataset, .. } => dataset,
        }
    }
}

impl DerefMut for DatasetWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut *self.guard {
            DatasetRef::Latest { dataset, .. } => dataset,
            DatasetRef::TimeTravel { dataset, .. } => dataset,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use lance::{dataset::WriteParams, io::ObjectStoreParams};

    use super::*;

    use crate::{connect, io::object_store::io_tracking::IoStatsHolder, table::WriteOptions};

    #[tokio::test]
    async fn test_iops_open_strong_consistency() {
        let db = connect("memory://")
            .read_consistency_interval(Some(Duration::ZERO))
            .execute()
            .await
            .expect("Failed to connect to database");
        let io_stats = IoStatsHolder::default();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let table = db
            .create_empty_table("test", schema)
            .write_options(WriteOptions {
                lance_write_params: Some(WriteParams {
                    store_params: Some(ObjectStoreParams {
                        object_store_wrapper: Some(Arc::new(io_stats.clone())),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();

        io_stats.incremental_stats();

        // We should only need 1 read IOP to check the schema: looking for the
        // latest version.
        table.schema().await.unwrap();
        let stats = io_stats.incremental_stats();
        assert_eq!(stats.read_iops, 1);
    }

    #[tokio::test]
    async fn test_iops_concurrent_refresh() {
        // We can't use the base file implementation, because it we bypass some
        // wrapper methods in it, so the IOPS tracker would miss some calls.
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_str().unwrap();
        let uri = format!("file-object-store://{tmp_path}");
        let db = connect(&uri)
            .read_consistency_interval(Some(Duration::ZERO))
            .execute()
            .await
            .expect("Failed to connect to database");
        let io_stats = IoStatsHolder::default();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let table = db
            .create_empty_table("test", schema)
            .write_options(WriteOptions {
                lance_write_params: Some(WriteParams {
                    store_params: Some(ObjectStoreParams {
                        object_store_wrapper: Some(Arc::new(io_stats.clone())),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();

        io_stats.incremental_stats();

        // Concurrently load the schema 3 times. We should only need to check for
        // the latest version once.
        let res = futures::future::join3(table.schema(), table.schema(), table.schema()).await;
        res.0.unwrap();
        res.1.unwrap();
        res.2.unwrap();

        let stats = io_stats.incremental_stats();
        assert_eq!(stats.read_iops, 1);
    }
}
