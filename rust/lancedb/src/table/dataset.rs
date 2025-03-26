// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{self, Duration, Instant},
};

use lance::Dataset;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{error::Result, Error};
use lance::Error as LanceError;

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

    /// Run a function on the dataset.
    ///
    /// This is robust to cleanup and recreation of the dataset.
    pub async fn run_safe<T, F>(&self, f: impl Fn(&Dataset) -> F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let dataset = self.get().await?;
        match f(&*dataset).await {
            Ok(value) => Ok(value),
            // This likely means we are reading a version that doesn't exist anymore.
            Err(Error::Lance {
                source: LanceError::IO { source, .. },
            }) if source.to_string().contains("Not found") => {
                if dataset.guard.is_latest() {
                    self.reload().await?;
                    f(&*dataset).await
                } else {
                    Err(Error::InvalidInput {
                        message: "Checked out version no longer exists. This could be because it was deleted by Optimize or the table was dropped.".to_string(),
                    })
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn run_safe_mut<T, F>(&self, f: impl Fn(&mut Dataset) -> F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let mut dataset = self.get_mut().await?;
        match f(&mut *dataset).await {
            Ok(value) => Ok(value),
            // This likely means we are reading a version that doesn't exist anymore.
            Err(Error::Lance {
                source: LanceError::IO { source, .. },
            }) if source.to_string().contains("Not found") => {
                if dataset.guard.is_latest() {
                    self.reload().await?;
                    f(&mut *dataset).await
                } else {
                    Err(Error::InvalidInput {
                        message: "Checked out version no longer exists. This could be because it was deleted by Optimize or the table was dropped.".to_string(),
                    })
                }
            }
            Err(err) => Err(err),
        }
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
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use chrono::TimeDelta;
    use futures::StreamExt;
    use tempfile::tempdir;

    use crate::{
        connection::ConnectBuilder,
        query::ExecutableQuery,
        table::{AddDataMode, OptimizeAction},
        Table,
    };

    use super::*;

    fn batch_from_vec(data: Vec<i64>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let columns = vec![Arc::new(Int64Array::from(data)) as ArrayRef];
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn reader_from_batch(batch: RecordBatch) -> Box<dyn RecordBatchReader + Send + 'static> {
        let schema = batch.schema();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    async fn query_rows(table: &Table) -> Result<Vec<i64>> {
        let mut stream = table.query().execute().await?;
        let mut values = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let a = batch["a"]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values();
            values.extend_from_slice(a);
        }
        Ok(values)
    }

    #[tokio::test]
    async fn test_aggressive_cleanup() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        // TODO: if we change the consistency interval, make this none
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", reader_from_batch(batch_from_vec(vec![1])))
            .execute()
            .await
            .unwrap();

        // Checkout one handle as time travel, one as latest
        let table_latest = table;
        let table_time_travel = conn.open_table("my_table").execute().await.unwrap();
        table_time_travel.checkout(1).await.unwrap();

        // In another handle, create a new version
        let table_new = conn.open_table("my_table").execute().await.unwrap();
        table_new
            .add(reader_from_batch(batch_from_vec(vec![2, 3])))
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();

        // Check original handles are unchanged.
        assert_eq!(table_latest.version().await.unwrap(), 1);
        assert_eq!(table_time_travel.version().await.unwrap(), 1);
        assert_eq!(query_rows(&table_latest).await.unwrap(), vec![1]);
        assert_eq!(query_rows(&table_time_travel).await.unwrap(), vec![1]);
        assert_eq!(query_rows(&table_new).await.unwrap(), vec![2, 3]);

        // In new handle, optimize aggressively, to delete old version
        let stats = table_new
            .optimize(OptimizeAction::Prune {
                older_than: Some(TimeDelta::zero()),
                delete_unverified: Some(true),
                error_if_tagged_old_versions: None,
            })
            .await
            .unwrap();
        dbg!(stats);

        // Show that latest one moves on to new version
        assert_eq!(query_rows(&table_latest).await.unwrap(), vec![2, 3]);
        assert_eq!(table_latest.version().await.unwrap(), 2);

        // Show that the time travel one provides a sensible error.
        let res = query_rows(&table_latest).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Checked out version no longer exists."
        );
    }

    #[tokio::test]
    async fn test_recreate_table() {
        // Create a table
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", reader_from_batch(batch_from_vec(vec![1])))
            .execute()
            .await
            .unwrap();

        // Checkout one handle as time travel, one as latest
        let table_latest = table;
        let table_time_travel = conn.open_table("my_table").execute().await.unwrap();

        // In new handle, recreate the table
        conn.drop_table("my_table").await.unwrap();
        conn.create_table("my_table", reader_from_batch(batch_from_vec(vec![2, 3])))
            .execute()
            .await
            .unwrap();

        // Show that latest one moves on to new version
        assert_eq!(table_latest.version().await.unwrap(), 1);
        assert_eq!(query_rows(&table_latest).await.unwrap(), &[2, 3]);

        // Show that the time travel one provides a sensible error.
        let res = query_rows(&table_time_travel).await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Checked out version no longer exists."
        );
    }
}
