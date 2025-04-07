// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, TryLockError},
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
pub struct DatasetConsistencyWrapper {
    ds: Arc<RwLock<DatasetRef>>,
    /// A background task loading the next version of the dataset. This happens
    /// in the background so as not to block the current thread.
    refresh: RefreshTaskWrapper,
}

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
    },
    /// In this mode, the dataset is a specific version. It cannot be mutated.
    TimeTravel { dataset: Dataset, version: u64 },
}

impl DatasetRef {
    fn is_latest(&self) -> bool {
        matches!(self, Self::Latest { .. })
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
                if *version != target_version || dataset.version().version != target_version {
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
                last_consistency_check,
                ..
            } => {
                *ds = dataset;
                *last_consistency_check = Some(Instant::now());
            }
            _ => unreachable!("Dataset should be in latest mode at this point"),
        }
    }
}

impl DatasetConsistencyWrapper {
    /// Create a new wrapper in the latest version mode.
    pub fn new_latest(dataset: Dataset, read_consistency_interval: Option<Duration>) -> Self {
        Self {
            ds: Arc::new(RwLock::new(DatasetRef::Latest {
                dataset,
                read_consistency_interval,
                last_consistency_check: Some(Instant::now()),
            })),
            refresh: Default::default(),
        }
    }

    /// Get an immutable reference to the dataset.
    pub async fn get(&self) -> Result<DatasetReadGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetReadGuard {
            guard: self.ds.read().await,
        })
    }

    /// Get a mutable reference to the dataset.
    ///
    /// If the dataset is in time travel mode this will fail
    pub async fn get_mut(&self) -> Result<DatasetWriteGuard<'_>> {
        self.ensure_mutable().await?;
        self.ensure_up_to_date().await?;
        Ok(DatasetWriteGuard {
            guard: self.ds.write().await,
        })
    }

    /// Get a mutable reference to the dataset without requiring the
    /// dataset to be in a Latest mode.
    pub async fn get_mut_unchecked(&self) -> Result<DatasetWriteGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetWriteGuard {
            guard: self.ds.write().await,
        })
    }

    /// Convert into a wrapper in latest version mode
    pub async fn as_latest(&self, read_consistency_interval: Option<Duration>) -> Result<()> {
        if self.ds.read().await.is_latest() {
            return Ok(());
        }

        let mut write_guard = self.ds.write().await;
        if write_guard.is_latest() {
            return Ok(());
        }

        write_guard.as_latest(read_consistency_interval).await
    }

    pub async fn as_time_travel(&self, target_version: u64) -> Result<()> {
        self.ds.write().await.as_time_travel(target_version).await
    }

    /// Provide a known latest version of the dataset.
    ///
    /// This is usually done after some write operation, which inherently will
    /// have the latest version.
    pub async fn set_latest(&self, dataset: Dataset) {
        self.ds.write().await.set_latest(dataset);
    }

    pub async fn reload_latest(&self) -> Result<()> {
        self.refresh.initiate(self.ds.clone(), None, None);
        self.refresh.wait().await?;
        Ok(())
    }

    /// Returns the version, if in time travel mode, or None otherwise
    pub async fn time_travel_version(&self) -> Option<u64> {
        self.ds.read().await.time_travel_version()
    }

    pub async fn ensure_mutable(&self) -> Result<()> {
        let dataset_ref = self.ds.read().await;
        match &*dataset_ref {
            DatasetRef::Latest { .. } => Ok(()),
            DatasetRef::TimeTravel { .. } => Err(crate::Error::InvalidInput {
                message: "table cannot be modified when a specific version is checked out"
                    .to_string(),
            }),
        }
    }

    /// Returns true if we should wait for the refresh task to finish.
    async fn maybe_init_refresh(&self) -> Result<bool> {
        let dataset_ref = self.ds.read().await;
        match &*dataset_ref {
            DatasetRef::Latest {
                read_consistency_interval,
                last_consistency_check,
                ..
            } => match (read_consistency_interval, last_consistency_check) {
                (None, _) => Ok(false), // No task scheduled
                (Some(read_consistency_interval), last_consistency_check) => {
                    if !self.refresh.is_scheduled() {
                        // We subtract 30 ms to account for the time it takes to
                        // perform the refresh.
                        let next_refresh = last_consistency_check.unwrap_or_else(Instant::now)
                            + *read_consistency_interval
                            - Duration::from_millis(30);
                        self.refresh
                            .initiate(self.ds.clone(), Some(next_refresh), None);
                    }
                    if read_consistency_interval.is_zero() {
                        Ok(true) // Strong consistency, wait for task
                    } else {
                        Ok(false) // Task schedule, let run in background
                    }
                }
            },
            DatasetRef::TimeTravel { dataset, version } => {
                if dataset.version().version == *version {
                    Ok(false) // No task scheduled
                } else {
                    self.refresh.initiate(self.ds.clone(), None, Some(*version));
                    Ok(true) // Task scheduled
                }
            }
        }
    }

    /// Ensures that the dataset is loaded and up-to-date with consistency and
    /// version parameters.
    async fn ensure_up_to_date(&self) -> Result<()> {
        let should_wait = self.maybe_init_refresh().await?;
        if should_wait {
            self.refresh.wait().await?;
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

/// Handler for initializing and waiting for refresh tasks.
#[derive(Clone, Debug, Default)]
// We use a std::sync::Mutex here because we never need to hold the lock
// across an await point.
struct RefreshTaskWrapper(Arc<std::sync::Mutex<Option<RefreshTask>>>);

impl RefreshTaskWrapper {
    /// Schedule a refresh task to run after the given instant. This will start
    /// a background task that will refresh the dataset. If a task is already
    /// running, this will do nothing.
    ///
    /// If duration is None, the task will be scheduled immediately. If there is
    /// a task already scheduled, but it hasn't started yet, then it will be
    /// cancelled and an immediate task will be scheduled. If the task has
    /// already started, then this will do nothing.
    ///
    /// If duration is Some, then a task will be scheduled to run after the
    /// duration. If there is already a task scheduled, then nothing will
    /// happen.
    fn initiate(
        &self,
        dataset: Arc<RwLock<DatasetRef>>,
        start_time: Option<Instant>,
        version: Option<u64>,
    ) {
        // If we already have one started, we don't need to start another one.
        let mut task_slot = match self.0.lock() {
            Ok(task) => task,
            Err(err) => {
                // If the lock is poisoned, we can just clear the task and continue
                let mut guard = err.into_inner();
                guard.take();
                guard
            }
        };

        match &mut *task_slot {
            Some(task)
                if task.start_time > Instant::now()
                    && task.start_time > start_time.unwrap_or(task.start_time) =>
            {
                // There is a schedule task, but it's not yet running and not starting
                // as soon as requested. We can cancel it and start a new one.
                if let Some(task) = task_slot.take() {
                    task.handle.abort();
                }
                task_slot.replace(RefreshTask::new(dataset, start_time, version));
            }
            Some(_) => {
                // A task is already running, we don't need to start another one.
            }
            None => {
                // No task is running, we can start a new one.
                task_slot.replace(RefreshTask::new(dataset, start_time, version));
            }
        }
    }

    fn is_scheduled(&self) -> bool {
        match self.0.lock() {
            Ok(task) => task.is_some(),
            Err(_) => false,
        }
    }

    async fn wait(&self) -> Result<()> {
        let mut rx = match self.0.lock() {
            Ok(mut task) => {
                match &mut *task {
                    Some(inner) if inner.handle.is_finished() => {
                        // The task is finished, we can clear the slot
                        return task.take().unwrap().cleanup();
                    }
                    Some(RefreshTask { finished_rx, .. }) => finished_rx.clone(),
                    None => {
                        return Ok(());
                    }
                }
            }
            Err(err) => {
                // If the lock is poisoned, we can just clear the task and continue
                err.into_inner().take();
                return Ok(());
            }
        };
        let _ = rx.wait_for(|is_done| *is_done).await;

        match self.0.try_lock() {
            Ok(mut task) => {
                if let Some(task) = task.take() {
                    return task.cleanup();
                }
            }
            Err(TryLockError::Poisoned(err)) => {
                // If the lock is poisoned, we can just clear the slot and continue
                err.into_inner().take();
            }
            Err(TryLockError::WouldBlock) => {
                // If the lock is already held, we can assume another task is
                // handling the refresh.
                return Ok(());
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
struct RefreshTask {
    /// When the task will start.
    start_time: Instant,
    /// The task that is running the refresh operation.
    handle: tokio::task::JoinHandle<Result<()>>,
    /// A channel to notify when the task is finished.
    finished_rx: tokio::sync::watch::Receiver<bool>,
}

impl RefreshTask {
    fn new(
        dataset: Arc<RwLock<DatasetRef>>,
        start_time: Option<Instant>,
        version: Option<u64>,
    ) -> Self {
        let start_time = start_time.unwrap_or_else(Instant::now);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let refresh_future =
            Self::refresh_task_impl(dataset, start_time, version).map(move |res| {
                tx.send(true).unwrap();
                res
            });
        let handle = tokio::task::spawn(refresh_future);
        Self {
            handle,
            finished_rx: rx,
            start_time,
        }
    }

    async fn refresh_task_impl(
        dataset: Arc<RwLock<DatasetRef>>,
        start_time: std::time::Instant,
        version: Option<u64>,
    ) -> Result<()> {
        tokio::time::sleep_until(start_time.into()).await;

        if let Some(version) = version {
            // We don't mind blocking here as much.
            dataset.write().await.as_time_travel(version).await?;
            return Ok(());
        }

        // We try to do as much as possible **without holding the write lock**.
        // This is important because a write lock blocks all other reads (and
        // also must wait for initiated reads to finish).
        let new_dataset = {
            if let DatasetRef::Latest { dataset, .. } = &*dataset.read().await {
                let new_location = dataset.new_version_available().await?;
                if let Some(new_location) = new_location {
                    dataset.checkout_version(new_location).await
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }?;

        // A write lock should be held for as little time as possible.
        dataset.write().await.set_latest(new_dataset);

        Ok(())
    }

    fn cleanup(self) -> Result<()> {
        assert!(self.handle.is_finished());
        self.handle.now_or_never().map(|res| res.unwrap()).unwrap()
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
