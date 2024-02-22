// Copyright 2024 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{self, Duration, Instant},
};

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
                *dataset = dataset
                    .checkout_version(dataset.latest_version_id().await?)
                    .await?;
                last_consistency_check.replace(Instant::now());
            }
            Self::TimeTravel { dataset, version } => {
                dataset.checkout_version(*version).await?;
            }
        }
        Ok(())
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

    fn set_latest(&mut self, dataset: Dataset) {
        match self {
            Self::Latest {
                dataset: ref mut ds,
                ..
            } => {
                *ds = dataset;
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
            last_consistency_check: None,
        })))
    }

    /// Create a new wrapper in the time travel mode.
    pub fn new_time_travel(dataset: Dataset, version: u64) -> Self {
        Self(Arc::new(RwLock::new(DatasetRef::TimeTravel {
            dataset,
            version,
        })))
    }

    /// Create an independent copy of self.
    ///
    /// Unlike Clone, this will track versions independently of the original wrapper and
    /// will be tied to a different RwLock.
    pub async fn duplicate(&self) -> Self {
        let ds_ref = self.0.read().await;
        Self(Arc::new(RwLock::new((*ds_ref).clone())))
    }

    /// Get an immutable reference to the dataset.
    pub async fn get(&self) -> Result<DatasetReadGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetReadGuard {
            guard: self.0.read().await,
        })
    }

    /// Get a mutable reference to the dataset.
    pub async fn get_mut(&self) -> Result<DatasetWriteGuard<'_>> {
        self.ensure_up_to_date().await?;
        Ok(DatasetWriteGuard {
            guard: self.0.write().await,
        })
    }

    /// Convert into a wrapper in latest version mode
    pub async fn as_latest(&mut self, read_consistency_interval: Option<Duration>) -> Result<()> {
        self.0
            .write()
            .await
            .as_latest(read_consistency_interval)
            .await
    }

    /// Provide a known latest version of the dataset.
    ///
    /// This is usually done after some write operation, which inherently will
    /// have the latest version.
    pub async fn set_latest(&self, dataset: Dataset) {
        self.0.write().await.set_latest(dataset);
    }

    async fn reload(&self) -> Result<()> {
        self.0.write().await.reload().await
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
