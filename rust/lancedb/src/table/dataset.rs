// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use lance::{dataset::refs, Dataset};

use crate::{error::Result, utils::background_cache::BackgroundCache, Error};

/// A wrapper around a [Dataset] that provides consistency checks.
///
/// This can be cloned cheaply. Callers get an [`Arc<Dataset>`] from [`get()`](Self::get)
/// and call [`update()`](Self::update) after writes to store the new version.
#[derive(Debug, Clone)]
pub struct DatasetConsistencyWrapper {
    state: Arc<Mutex<DatasetRef>>,
    bg_cache: Option<BackgroundCache<Arc<Dataset>, Error>>,
}

/// Internal state tracking the dataset and its consistency mode.
///
/// The dataset is stored as `Arc<Dataset>` for cheap cloning. The mutex
/// is never held across `.await` points.
#[derive(Debug, Clone)]
enum DatasetRef {
    /// In this mode, the dataset is always the latest version.
    Latest {
        dataset: Arc<Dataset>,
        read_consistency_interval: Option<Duration>,
        last_consistency_check: Option<Instant>,
    },
    /// In this mode, the dataset is a specific version. It cannot be mutated.
    TimeTravel { dataset: Arc<Dataset>, version: u64 },
}

impl DatasetRef {
    fn dataset(&self) -> &Arc<Dataset> {
        match self {
            Self::Latest { dataset, .. } => dataset,
            Self::TimeTravel { dataset, .. } => dataset,
        }
    }

    fn is_latest(&self) -> bool {
        matches!(self, Self::Latest { .. })
    }

    fn time_travel_version(&self) -> Option<u64> {
        match self {
            Self::Latest { .. } => None,
            Self::TimeTravel { version, .. } => Some(*version),
        }
    }

    fn is_up_to_date(&self) -> bool {
        match self {
            Self::Latest {
                read_consistency_interval,
                last_consistency_check,
                ..
            } => match (read_consistency_interval, last_consistency_check) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(interval), Some(last_check)) => last_check.elapsed() < *interval,
            },
            Self::TimeTravel { dataset, version } => dataset.version().version == *version,
        }
    }
}

impl DatasetConsistencyWrapper {
    /// Create a new wrapper in the latest version mode.
    pub fn new_latest(dataset: Dataset, read_consistency_interval: Option<Duration>) -> Self {
        let bg_cache = match read_consistency_interval {
            Some(d) if d > Duration::ZERO => Some(BackgroundCache::new(d, d / 2)),
            _ => None,
        };
        Self {
            state: Arc::new(Mutex::new(DatasetRef::Latest {
                dataset: Arc::new(dataset),
                read_consistency_interval,
                last_consistency_check: Some(Instant::now()),
            })),
            bg_cache,
        }
    }

    /// Get the current dataset.
    ///
    /// Behavior depends on the consistency mode:
    /// - **Lazy** (`None`): returns the cached dataset immediately.
    /// - **Strong** (`Some(ZERO)`): checks for a new version before returning.
    /// - **Eventual** (`Some(d)` where `d > 0`): returns a cached value immediately
    ///   while refreshing in the background when the TTL expires.
    pub async fn get(&self) -> Result<Arc<Dataset>> {
        if let Some(bg_cache) = &self.bg_cache {
            if let Some(dataset) = bg_cache.try_get() {
                return Ok(dataset);
            }
            let state = self.state.clone();
            return bg_cache
                .get(move || fetch_latest_dataset(state))
                .await
                .map_err(unwrap_shared_error);
        }

        // Lazy or strong consistency
        self.ensure_up_to_date().await?;
        let state = self.state.lock().unwrap();
        Ok(state.dataset().clone())
    }

    /// Store a new dataset version after a write operation.
    ///
    /// Only stores the dataset if its version is newer than the current one.
    /// Panics if called when not in Latest mode.
    pub fn update(&self, dataset: Dataset) {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            DatasetRef::Latest {
                dataset: current, ..
            } => {
                if dataset.manifest().version > current.manifest().version {
                    *current = Arc::new(dataset);
                }
            }
            _ => unreachable!("Dataset should be in latest mode when calling update"),
        }
        drop(state);
        self.invalidate_bg_cache();
    }

    /// Checkout a branch and track its HEAD for new versions.
    pub async fn as_branch(&self, _branch: impl Into<String>) -> Result<()> {
        todo!("Branch support not yet implemented")
    }

    /// Check that the dataset is in a mutable mode (Latest).
    pub fn ensure_mutable(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        match &*state {
            DatasetRef::Latest { .. } => Ok(()),
            DatasetRef::TimeTravel { .. } => Err(crate::Error::InvalidInput {
                message: "table cannot be modified when a specific version is checked out"
                    .to_string(),
            }),
        }
    }

    /// Returns the version, if in time travel mode, or None otherwise.
    pub fn time_travel_version(&self) -> Option<u64> {
        let state = self.state.lock().unwrap();
        state.time_travel_version()
    }

    /// Convert into a wrapper in latest version mode.
    pub async fn as_latest(&self, read_consistency_interval: Option<Duration>) -> Result<()> {
        let dataset = {
            let state = self.state.lock().unwrap();
            if state.is_latest() {
                return Ok(());
            }
            state.dataset().clone()
        };

        let latest_version = dataset.latest_version_id().await?;
        let new_dataset = dataset.checkout_version(latest_version).await?;

        let mut state = self.state.lock().unwrap();
        // Re-check in case another thread already switched to Latest
        if !state.is_latest() {
            *state = DatasetRef::Latest {
                dataset: Arc::new(new_dataset),
                read_consistency_interval,
                last_consistency_check: Some(Instant::now()),
            };
        }
        drop(state);
        self.invalidate_bg_cache();
        Ok(())
    }

    pub async fn as_time_travel(&self, target_version: impl Into<refs::Ref>) -> Result<()> {
        let target_ref = target_version.into();

        let (should_checkout, dataset) = {
            let state = self.state.lock().unwrap();
            let should = match &*state {
                DatasetRef::Latest { .. } => true,
                DatasetRef::TimeTravel { version, .. } => match &target_ref {
                    refs::Ref::Version(_, Some(target_ver)) => version != target_ver,
                    refs::Ref::Version(_, None) => true,
                    refs::Ref::VersionNumber(target_ver) => version != target_ver,
                    refs::Ref::Tag(_) => true,
                },
            };
            (should, state.dataset().clone())
        };

        if !should_checkout {
            return Ok(());
        }

        let new_dataset = dataset.checkout_version(target_ref).await?;
        let version_value = new_dataset.version().version;

        let mut state = self.state.lock().unwrap();
        *state = DatasetRef::TimeTravel {
            dataset: Arc::new(new_dataset),
            version: version_value,
        };
        drop(state);
        self.invalidate_bg_cache();
        Ok(())
    }

    pub async fn reload(&self) -> Result<()> {
        let (dataset, reload_info) = {
            let state = self.state.lock().unwrap();
            let ds = state.dataset().clone();
            let info = match &*state {
                DatasetRef::Latest { .. } => ReloadInfo::Latest,
                DatasetRef::TimeTravel { version, .. } => ReloadInfo::TimeTravel(*version),
            };
            (ds, info)
        };

        match reload_info {
            ReloadInfo::Latest => {
                let latest_version = dataset.latest_version_id().await?;
                if latest_version == dataset.version().version {
                    // Already up to date, just refresh the check time
                    let mut state = self.state.lock().unwrap();
                    if let DatasetRef::Latest {
                        last_consistency_check,
                        ..
                    } = &mut *state
                    {
                        *last_consistency_check = Some(Instant::now());
                    }
                    return Ok(());
                }

                let mut dataset_clone = (*dataset).clone();
                dataset_clone.checkout_latest().await?;

                let mut state = self.state.lock().unwrap();
                if let DatasetRef::Latest {
                    dataset,
                    last_consistency_check,
                    ..
                } = &mut *state
                {
                    *dataset = Arc::new(dataset_clone);
                    *last_consistency_check = Some(Instant::now());
                }
            }
            ReloadInfo::TimeTravel(version) => {
                if dataset.version().version == version {
                    return Ok(());
                }

                let new_dataset = dataset.checkout_version(version).await?;

                let mut state = self.state.lock().unwrap();
                if let DatasetRef::TimeTravel { dataset, .. } = &mut *state {
                    *dataset = Arc::new(new_dataset);
                }
            }
        }

        self.invalidate_bg_cache();
        Ok(())
    }

    /// Ensures that the dataset is loaded and up-to-date with consistency and
    /// version parameters.
    async fn ensure_up_to_date(&self) -> Result<()> {
        let up_to_date = {
            let state = self.state.lock().unwrap();
            state.is_up_to_date()
        };
        if !up_to_date {
            self.reload().await?;
        }
        Ok(())
    }

    fn invalidate_bg_cache(&self) {
        if let Some(bg_cache) = &self.bg_cache {
            bg_cache.invalidate();
        }
    }
}

async fn fetch_latest_dataset(state: Arc<Mutex<DatasetRef>>) -> Result<Arc<Dataset>> {
    let dataset = {
        let state = state.lock().unwrap();
        state.dataset().clone()
    };

    let latest_version = dataset.latest_version_id().await?;
    if latest_version == dataset.version().version {
        return Ok(dataset);
    }

    let mut ds = (*dataset).clone();
    ds.checkout_latest().await?;
    let new_arc = Arc::new(ds);

    // Update the mutex state so version comparisons stay current
    {
        let mut state = state.lock().unwrap();
        if let DatasetRef::Latest { dataset, .. } = &mut *state {
            if new_arc.manifest().version > dataset.manifest().version {
                *dataset = new_arc.clone();
            }
        }
    }

    Ok(new_arc)
}

fn unwrap_shared_error(arc: Arc<Error>) -> Error {
    match Arc::try_unwrap(arc) {
        Ok(err) => err,
        Err(arc) => Error::Runtime {
            message: arc.to_string(),
        },
    }
}

enum ReloadInfo {
    Latest,
    TimeTravel(u64),
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};

    use super::*;

    use crate::{connect, io::object_store::io_tracking::IoStatsHolder, table::WriteOptions};

    async fn create_test_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri,
            Some(WriteParams::default()),
        )
        .await
        .unwrap()
    }

    async fn append_to_dataset(uri: &str) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
        )
        .unwrap();
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    // Group 1: API change tests

    #[tokio::test]
    async fn test_get_returns_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;
        let version = ds.version().version;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        let ds1 = wrapper.get().await.unwrap();
        let ds2 = wrapper.get().await.unwrap();

        assert_eq!(ds1.version().version, version);
        assert_eq!(ds2.version().version, version);

        // Arc<Dataset> is independent — not borrowing from wrapper
        drop(wrapper);
        assert_eq!(ds1.version().version, version);
    }

    #[tokio::test]
    async fn test_update_stores_newer_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds_v1 = create_test_dataset(uri).await;
        assert_eq!(ds_v1.version().version, 1);

        let wrapper = DatasetConsistencyWrapper::new_latest(ds_v1, None);

        let ds_v2 = append_to_dataset(uri).await;
        assert_eq!(ds_v2.version().version, 2);

        wrapper.update(ds_v2);

        let ds = wrapper.get().await.unwrap();
        assert_eq!(ds.version().version, 2);
    }

    #[tokio::test]
    async fn test_update_ignores_older_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds_v1 = create_test_dataset(uri).await;
        let ds_v2 = append_to_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds_v2, None);
        wrapper.update(ds_v1);

        let ds = wrapper.get().await.unwrap();
        assert_eq!(ds.version().version, 2);
    }

    #[tokio::test]
    async fn test_ensure_mutable_allows_latest() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        assert!(wrapper.ensure_mutable().is_ok());
    }

    #[tokio::test]
    async fn test_ensure_mutable_rejects_time_travel() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        wrapper.as_time_travel(1u64).await.unwrap();

        assert!(wrapper.ensure_mutable().is_err());
    }

    #[tokio::test]
    async fn test_time_travel_version() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        assert_eq!(wrapper.time_travel_version(), None);

        wrapper.as_time_travel(1u64).await.unwrap();
        assert_eq!(wrapper.time_travel_version(), Some(1));
    }

    #[tokio::test]
    async fn test_as_latest_from_time_travel() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        wrapper.as_time_travel(1u64).await.unwrap();
        assert!(wrapper.ensure_mutable().is_err());

        wrapper.as_latest(None).await.unwrap();
        assert!(wrapper.ensure_mutable().is_ok());
        assert_eq!(wrapper.time_travel_version(), None);
    }

    // Group 2: Background refresh tests

    #[tokio::test]
    async fn test_lazy_consistency_never_refreshes() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, None);
        let v1 = wrapper.get().await.unwrap().version().version;

        // External write
        append_to_dataset(uri).await;

        // Lazy consistency should not pick up external write
        let v_after = wrapper.get().await.unwrap().version().version;
        assert_eq!(v1, v_after);
    }

    #[tokio::test]
    async fn test_strong_consistency_always_refreshes() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, Some(Duration::ZERO));
        let v1 = wrapper.get().await.unwrap().version().version;

        // External write
        append_to_dataset(uri).await;

        // Strong consistency should pick up external write
        let v_after = wrapper.get().await.unwrap().version().version;
        assert_eq!(v_after, v1 + 1);
    }

    #[tokio::test]
    async fn test_eventual_consistency_background_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds, Some(Duration::from_millis(200)));

        // Populate the cache
        let v1 = wrapper.get().await.unwrap().version().version;
        assert_eq!(v1, 1);

        // External write
        append_to_dataset(uri).await;

        // Should return cached value immediately (within TTL)
        let v_cached = wrapper.get().await.unwrap().version().version;
        assert_eq!(v_cached, 1);

        // Wait for TTL to expire, then get() should trigger a refresh
        tokio::time::sleep(Duration::from_millis(300)).await;
        let v_after = wrapper.get().await.unwrap().version().version;
        assert_eq!(v_after, 2);
    }

    #[tokio::test]
    async fn test_eventual_consistency_update_invalidates_cache() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let ds_v1 = create_test_dataset(uri).await;

        let wrapper = DatasetConsistencyWrapper::new_latest(ds_v1, Some(Duration::from_secs(60)));

        // Simulate a write that produces v2
        let ds_v2 = append_to_dataset(uri).await;
        wrapper.update(ds_v2);

        // get() should return v2 immediately (update invalidated the bg_cache,
        // and the mutex state was updated)
        let v = wrapper.get().await.unwrap().version().version;
        assert_eq!(v, 2);
    }

    // Group 3: Branch tests (todo)

    #[tokio::test]
    #[ignore]
    async fn test_as_branch() {
        // TODO: test checkout branch
    }

    #[tokio::test]
    #[ignore]
    async fn test_branch_picks_up_new_versions() {
        // TODO: test branch refresh picks up new versions
    }

    // Existing test

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
                    store_params: Some(lance::io::ObjectStoreParams {
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
