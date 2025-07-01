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
    /// Reload the dataset to the appropriate version.
    async fn reload(&mut self) -> Result<()> {
        match self {
            Self::Latest {
                dataset,
                last_consistency_check,
                ..
            } => {
                dataset.checkout_latest().await?;
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

    async fn need_reload(&self) -> Result<bool> {
        Ok(match self {
            Self::Latest { dataset, .. } => {
                dataset.latest_version_id().await? != dataset.version().version
            }
            Self::TimeTravel { dataset, version } => dataset.version().version != *version,
        })
    }

    async fn as_latest(&mut self, read_consistency_interval: Option<Duration>) -> Result<()> {
        match self {
            Self::Latest { .. } => Ok(()),
            Self::TimeTravel { dataset, .. } => {
                dataset
                    .checkout_version(dataset.latest_version_id().await?)
                    .await?;
                *self = Self::Latest {
                    dataset: dataset.clone(),
                    read_consistency_interval,
                    last_consistency_check: Some(Instant::now()),
                };
                Ok(())
            }
        }
    }

    async fn as_time_travel(&mut self, target_version: impl Into<refs::Ref>) -> Result<()> {
        let target_ref = target_version.into();

        match self {
            Self::Latest { dataset, .. } => {
                let new_dataset = dataset.checkout_version(target_ref.clone()).await?;
                let version_value = new_dataset.version().version;

                *self = Self::TimeTravel {
                    dataset: new_dataset,
                    version: version_value,
                };
            }
            Self::TimeTravel { dataset, version } => {
                let should_checkout = match &target_ref {
                    refs::Ref::Version(target_ver) => version != target_ver,
                    refs::Ref::Tag(_) => true, // Always checkout for tags
                };

                if should_checkout {
                    let new_dataset = dataset.checkout_version(target_ref).await?;
                    let version_value = new_dataset.version().version;

                    *self = Self::TimeTravel {
                        dataset: new_dataset,
                        version: version_value,
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

    pub async fn as_time_travel(&self, target_version: impl Into<refs::Ref>) -> Result<()> {
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
        if !self.0.read().await.need_reload().await? {
            return Ok(());
        }

        let mut write_guard = self.0.write().await;
        // on lock escalation -- check if someone else has already reloaded
        if !write_guard.need_reload().await? {
            return Ok(());
        }

        // actually need reloading
        write_guard.reload().await
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
