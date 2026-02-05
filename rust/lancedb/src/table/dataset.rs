// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{self, Duration, Instant},
};

use lance::{dataset::refs, Dataset};
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
#[derive(Debug, Clone)]
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
                ..
            } => {
                if dataset.manifest().version > ds.manifest().version {
                    *ds = dataset;
                }
            }
            _ => unreachable!("Dataset should be in latest mode at this point"),
        }
    }
}

impl DatasetConsistencyWrapper {
    /// Create a new wrapper in the latest version mode.
    pub fn new_latest(dataset: Dataset, read_consistency_interval: Option<Duration>) -> Self {
        Self(Arc::new(RwLock::new(DatasetRef::Latest {
            dataset,
            read_consistency_interval,
            last_consistency_check: Some(Instant::now()),
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

    /// Convert into a wrapper in latest version mode.
    ///
    /// This method is designed to avoid holding the lock during async I/O operations,
    /// which could block the event loop for extended periods.
    pub async fn as_latest(&self, read_consistency_interval: Option<Duration>) -> Result<()> {
        // Step 1: Check if already latest, get clone if not
        let dataset = {
            let guard = self.0.read().await;
            match &*guard {
                DatasetRef::Latest { .. } => return Ok(()),
                DatasetRef::TimeTravel { dataset, .. } => dataset.clone(),
            }
        }; // Lock released

        // Step 2: I/O OUTSIDE lock
        let latest_version = dataset.latest_version_id().await?;
        let new_dataset = dataset.checkout_version(latest_version).await?;

        // Step 3: Quick state update (only memory ops under lock)
        let mut write_guard = self.0.write().await;
        // Re-check in case another task already converted to Latest
        if write_guard.is_latest() {
            return Ok(());
        }
        *write_guard = DatasetRef::Latest {
            dataset: new_dataset,
            read_consistency_interval,
            last_consistency_check: Some(Instant::now()),
        };
        Ok(())
    }

    /// Convert into a wrapper in time travel mode.
    ///
    /// This method is designed to avoid holding the lock during async I/O operations,
    /// which could block the event loop for extended periods.
    pub async fn as_time_travel(&self, target_version: impl Into<refs::Ref>) -> Result<()> {
        let target_ref = target_version.into();

        // Step 1: Get clone, check if checkout is needed
        let (dataset, should_checkout) = {
            let guard = self.0.read().await;
            match &*guard {
                DatasetRef::Latest { dataset, .. } => (dataset.clone(), true),
                DatasetRef::TimeTravel { dataset, version } => {
                    let needs = match &target_ref {
                        refs::Ref::Version(_, Some(target_ver)) => version != target_ver,
                        refs::Ref::Version(_, None) => true, // No specific version, always checkout
                        refs::Ref::VersionNumber(target_ver) => version != target_ver,
                        refs::Ref::Tag(_) => true, // Always checkout for tags
                    };
                    (dataset.clone(), needs)
                }
            }
        }; // Lock released

        if !should_checkout {
            return Ok(());
        }

        // Step 2: I/O OUTSIDE lock
        let new_dataset = dataset.checkout_version(target_ref).await?;
        let new_version = new_dataset.version().version;

        // Step 3: Quick state update (only memory ops under lock)
        let mut write_guard = self.0.write().await;
        *write_guard = DatasetRef::TimeTravel {
            dataset: new_dataset,
            version: new_version,
        };
        Ok(())
    }

    /// Provide a known latest version of the dataset.
    ///
    /// This is usually done after some write operation, which inherently will
    /// have the latest version.
    pub async fn set_latest(&self, dataset: Dataset) {
        self.0.write().await.set_latest(dataset);
    }

    /// Reload the dataset to the latest version.
    ///
    /// This method is designed to avoid holding the lock during async I/O operations,
    /// which could block the event loop for extended periods.
    pub async fn reload(&self) -> Result<()> {
        // Step 1: Quick read, clone dataset and get parameters (no I/O under lock)
        let (dataset, is_latest, target_version, read_consistency_interval) = {
            let guard = self.0.read().await;
            match &*guard {
                DatasetRef::Latest {
                    dataset,
                    read_consistency_interval,
                    ..
                } => (dataset.clone(), true, None, *read_consistency_interval),
                DatasetRef::TimeTravel { dataset, version } => {
                    (dataset.clone(), false, Some(*version), None)
                }
            }
        }; // Lock released

        // Step 2: Check if reload needed (I/O OUTSIDE lock)
        let current_version = dataset.version().version;
        let need_reload = if is_latest {
            dataset.latest_version_id().await? != current_version
        } else {
            target_version.is_some_and(|v| current_version != v)
        };
        if !need_reload {
            return Ok(());
        }

        // Step 3: Do reload (I/O OUTSIDE lock)
        let new_dataset = if is_latest {
            let latest = dataset.latest_version_id().await?;
            dataset.checkout_version(latest).await?
        } else {
            dataset.checkout_version(target_version.unwrap()).await?
        };

        // Step 4: Quick state update (only memory ops under lock)
        let mut write_guard = self.0.write().await;
        let should_update = match &*write_guard {
            DatasetRef::Latest {
                dataset: current, ..
            } => new_dataset.version().version > current.version().version,
            DatasetRef::TimeTravel {
                dataset: current,
                version,
            } => current.version().version != *version,
        };
        if should_update {
            match &mut *write_guard {
                DatasetRef::Latest {
                    dataset,
                    last_consistency_check,
                    read_consistency_interval: rci,
                    ..
                } => {
                    *dataset = new_dataset;
                    *rci = read_consistency_interval;
                    last_consistency_check.replace(Instant::now());
                }
                DatasetRef::TimeTravel { dataset, .. } => {
                    *dataset = new_dataset;
                }
            }
        }
        Ok(())
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
        if !self.is_up_to_date().await? {
            self.reload().await?;
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
            .read_consistency_interval(Duration::ZERO)
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
}
