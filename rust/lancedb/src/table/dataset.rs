// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use lance::{dataset::refs, Dataset};

use crate::{error::Result, utils::background_cache::BackgroundCache, Error};

/// A wrapper around a [Dataset] that provides consistency checks.
///
/// This can be cloned cheaply. Callers get an [`Arc<Dataset>`] from [`get()`](Self::get)
/// and call [`update()`](Self::update) after writes to store the new version.
#[derive(Debug, Clone)]
pub struct DatasetConsistencyWrapper {
    state: Arc<Mutex<DatasetState>>,
    consistency: ConsistencyMode,
}

/// The current dataset and whether it is pinned to a specific version.
///
/// The mutex is never held across `.await` points.
#[derive(Debug)]
struct DatasetState {
    dataset: Arc<Dataset>,
    /// `Some(version)` = pinned to a specific version (time travel),
    /// `None` = tracking latest.
    pinned_version: Option<u64>,
}

#[derive(Debug, Clone)]
enum ConsistencyMode {
    Lazy,
    Strong(CoalescingReloader),
    Eventual(BackgroundCache<Arc<Dataset>, Error>),
}

type SharedResultFut = Shared<BoxFuture<'static, std::result::Result<Arc<Dataset>, Arc<Error>>>>;

struct InflightReload {
    started_at: Instant,
    future: SharedResultFut,
}

#[derive(Clone)]
struct CoalescingReloader {
    inflight: Arc<Mutex<Option<InflightReload>>>,
}

impl fmt::Debug for CoalescingReloader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoalescingReloader")
            .field("has_inflight", &self.inflight.lock().unwrap().is_some())
            .finish()
    }
}

impl CoalescingReloader {
    fn new() -> Self {
        Self {
            inflight: Arc::new(Mutex::new(None)),
        }
    }

    async fn get(&self, dataset_state: &Arc<Mutex<DatasetState>>) -> Result<Arc<Dataset>> {
        let request_time = Instant::now();

        loop {
            let (fut, started_at) = {
                let mut inflight = self.inflight.lock().unwrap();

                if let Some(existing) = inflight.as_ref() {
                    (existing.future.clone(), existing.started_at)
                } else {
                    let started_at = Instant::now();
                    let state = dataset_state.clone();
                    let inflight_ref = self.inflight.clone();

                    let fut = async move {
                        let result = refresh_latest(state).await.map_err(Arc::new);
                        *inflight_ref.lock().unwrap() = None;
                        result
                    }
                    .boxed()
                    .shared();

                    *inflight = Some(InflightReload {
                        started_at,
                        future: fut.clone(),
                    });
                    (fut, started_at)
                }
            };

            let result = fut.await.map_err(unwrap_shared_error)?;

            // A reload that started after our request guarantees we see
            // at least the state that existed when we asked.
            if started_at >= request_time {
                return Ok(result);
            }
            // Otherwise the reload was too old — loop to start/join a new one.
        }
    }
}

impl DatasetConsistencyWrapper {
    /// Create a new wrapper in the latest version mode.
    pub fn new_latest(dataset: Dataset, read_consistency_interval: Option<Duration>) -> Self {
        let consistency = match read_consistency_interval {
            Some(d) if d == Duration::ZERO => ConsistencyMode::Strong(CoalescingReloader::new()),
            Some(d) => ConsistencyMode::Eventual(BackgroundCache::new(d, d / 2)),
            None => ConsistencyMode::Lazy,
        };
        Self {
            state: Arc::new(Mutex::new(DatasetState {
                dataset: Arc::new(dataset),
                pinned_version: None,
            })),
            consistency,
        }
    }

    /// Get the current dataset.
    ///
    /// Behavior depends on the consistency mode:
    /// - **Lazy** (`None`): returns the cached dataset immediately.
    /// - **Strong** (`Some(ZERO)`): checks for a new version before returning.
    /// - **Eventual** (`Some(d)` where `d > 0`): returns a cached value immediately
    ///   while refreshing in the background when the TTL expires.
    ///
    /// If pinned to a specific version (time travel), always returns the
    /// pinned dataset regardless of consistency mode.
    pub async fn get(&self) -> Result<Arc<Dataset>> {
        {
            let state = self.state.lock().unwrap();
            if state.pinned_version.is_some() {
                return Ok(state.dataset.clone());
            }
        }

        match &self.consistency {
            ConsistencyMode::Eventual(bg_cache) => {
                if let Some(dataset) = bg_cache.try_get() {
                    return Ok(dataset);
                }
                let state = self.state.clone();
                bg_cache
                    .get(move || refresh_latest(state))
                    .await
                    .map_err(unwrap_shared_error)
            }
            ConsistencyMode::Strong(reloader) => reloader.get(&self.state).await,
            ConsistencyMode::Lazy => {
                let state = self.state.lock().unwrap();
                Ok(state.dataset.clone())
            }
        }
    }

    /// Store a new dataset version after a write operation.
    ///
    /// Only stores the dataset if its version is newer than the current one.
    /// Panics if called when not in Latest mode.
    pub fn update(&self, dataset: Dataset) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.pinned_version.is_none(),
            "Dataset should be in latest mode when calling update"
        );
        if dataset.manifest().version > state.dataset.manifest().version {
            state.dataset = Arc::new(dataset);
        }
        drop(state);
        if let ConsistencyMode::Eventual(bg_cache) = &self.consistency {
            bg_cache.invalidate();
        }
    }

    /// Checkout a branch and track its HEAD for new versions.
    pub async fn as_branch(&self, _branch: impl Into<String>) -> Result<()> {
        todo!("Branch support not yet implemented")
    }

    /// Check that the dataset is in a mutable mode (Latest).
    pub fn ensure_mutable(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        if state.pinned_version.is_some() {
            Err(crate::Error::InvalidInput {
                message: "table cannot be modified when a specific version is checked out"
                    .to_string(),
            })
        } else {
            Ok(())
        }
    }

    /// Returns the version, if in time travel mode, or None otherwise.
    pub fn time_travel_version(&self) -> Option<u64> {
        self.state.lock().unwrap().pinned_version
    }

    /// Convert into a wrapper in latest version mode.
    pub async fn as_latest(&self) -> Result<()> {
        let dataset = {
            let state = self.state.lock().unwrap();
            if state.pinned_version.is_none() {
                return Ok(());
            }
            state.dataset.clone()
        };

        let latest_version = dataset.latest_version_id().await?;
        let new_dataset = dataset.checkout_version(latest_version).await?;

        let mut state = self.state.lock().unwrap();
        if state.pinned_version.is_some() {
            state.dataset = Arc::new(new_dataset);
            state.pinned_version = None;
        }
        drop(state);
        if let ConsistencyMode::Eventual(bg_cache) = &self.consistency {
            bg_cache.invalidate();
        }
        Ok(())
    }

    pub async fn as_time_travel(&self, target_version: impl Into<refs::Ref>) -> Result<()> {
        let target_ref = target_version.into();

        let (should_checkout, dataset) = {
            let state = self.state.lock().unwrap();
            let should = match state.pinned_version {
                None => true,
                Some(version) => match &target_ref {
                    refs::Ref::Version(_, Some(target_ver)) => version != *target_ver,
                    refs::Ref::Version(_, None) => true,
                    refs::Ref::VersionNumber(target_ver) => version != *target_ver,
                    refs::Ref::Tag(_) => true,
                },
            };
            (should, state.dataset.clone())
        };

        if !should_checkout {
            return Ok(());
        }

        let new_dataset = dataset.checkout_version(target_ref).await?;
        let version_value = new_dataset.version().version;

        let mut state = self.state.lock().unwrap();
        state.dataset = Arc::new(new_dataset);
        state.pinned_version = Some(version_value);
        Ok(())
    }

    pub async fn reload(&self) -> Result<()> {
        let (dataset, pinned_version) = {
            let state = self.state.lock().unwrap();
            (state.dataset.clone(), state.pinned_version)
        };

        match pinned_version {
            None => {
                refresh_latest(self.state.clone()).await?;
                if let ConsistencyMode::Eventual(bg_cache) = &self.consistency {
                    bg_cache.invalidate();
                }
            }
            Some(version) => {
                if dataset.version().version == version {
                    return Ok(());
                }

                let new_dataset = dataset.checkout_version(version).await?;

                let mut state = self.state.lock().unwrap();
                if state.pinned_version == Some(version) {
                    state.dataset = Arc::new(new_dataset);
                }
            }
        }

        Ok(())
    }
}

async fn refresh_latest(state: Arc<Mutex<DatasetState>>) -> Result<Arc<Dataset>> {
    let dataset = { state.lock().unwrap().dataset.clone() };

    let mut ds = (*dataset).clone();
    ds.checkout_latest().await?;
    let new_arc = Arc::new(ds);

    {
        let mut state = state.lock().unwrap();
        if state.pinned_version.is_none()
            && new_arc.manifest().version >= state.dataset.manifest().version
        {
            state.dataset = new_arc.clone();
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{record_batch, Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::{
        dataset::{InsertBuilder, WriteMode, WriteParams},
        io::{ObjectStore, ObjectStoreParams},
    };
    use lance_table::{
        format::Manifest,
        io::commit::{
            CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
            ManifestNamingScheme, ManifestWriter,
        },
    };
    use object_store::path::Path;

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

        wrapper.as_latest().await.unwrap();
        assert!(wrapper.ensure_mutable().is_ok());
        assert_eq!(wrapper.time_travel_version(), None);
    }

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

    #[tokio::test]
    async fn test_strong_consistency_coalescing() {
        #[derive(Debug)]
        struct MockCommitHandler {
            inner: ConditionalPutCommitHandler,
            num_calls: Arc<AtomicUsize>,
        }

        impl Default for MockCommitHandler {
            fn default() -> Self {
                Self {
                    inner: ConditionalPutCommitHandler,
                    num_calls: Arc::new(AtomicUsize::new(0)),
                }
            }
        }

        #[async_trait::async_trait]
        impl CommitHandler for MockCommitHandler {
            async fn resolve_latest_location(
                &self,
                base_path: &Path,
                object_store: &ObjectStore,
            ) -> lance_core::Result<ManifestLocation> {
                self.num_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.inner
                    .resolve_latest_location(base_path, object_store)
                    .await
            }

            async fn commit(
                &self,
                manifest: &mut Manifest,
                indices: Option<Vec<lance_table::format::IndexMetadata>>,
                base_path: &Path,
                object_store: &ObjectStore,
                manifest_writer: ManifestWriter,
                naming_scheme: ManifestNamingScheme,
                transaction: Option<lance_table::format::Transaction>,
            ) -> std::result::Result<ManifestLocation, CommitError> {
                self.inner
                    .commit(
                        manifest,
                        indices,
                        base_path,
                        object_store,
                        manifest_writer,
                        naming_scheme,
                        transaction,
                    )
                    .await
            }
        }

        let handler = MockCommitHandler::default();
        let call_count = handler.num_calls.clone();
        let handler = Arc::new(handler) as Arc<dyn CommitHandler>;
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                commit_handler: Some(handler.clone()),
                ..Default::default()
            })
            .execute(vec![batch])
            .await
            .unwrap();

        // Reset after Dataset::write, which also calls resolve_latest_location
        call_count.store(0, Ordering::SeqCst);

        let wrapper = DatasetConsistencyWrapper::new_latest(dataset, Some(Duration::ZERO));
        let barrier = Arc::new(tokio::sync::Barrier::new(10));
        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..10 {
            let wrapper = wrapper.clone();
            let barrier = barrier.clone();
            join_set.spawn(async move {
                barrier.wait().await;
                wrapper.get().await.unwrap();
            });
        }
        for _ in 0..10 {
            join_set.join_next().await.unwrap().unwrap();
        }

        // We expect the sequence to look like this:
        // - Task 1 starts reload
        // - Tasks 2-10 request a reload while Task 1 is still running.
        // - Task 1 finishes reload (after 10ms sleep)
        // - Tasks 2-10 see the reload started before their request, so trigger a new reload
        // - Reload finishes, resolves tasks 2-10.

        let final_count = call_count.load(Ordering::SeqCst);
        assert!(
            final_count <= 2,
            "Expected 2 or fewer calls to resolve_latest_location, but got {}",
            final_count
        );
    }

    /// Regression test: before the fix, the reload fast-path (no version change)
    /// did not reset `last_consistency_check`, causing a list call on every
    /// subsequent query once the interval expired.
    #[tokio::test]
    async fn test_reload_resets_consistency_timer() {
        let db = connect("memory://")
            .read_consistency_interval(Duration::from_secs(1))
            .execute()
            .await
            .unwrap();
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

        let start = Instant::now();
        io_stats.incremental_stats(); // reset

        // Step 1: within interval — no list
        table.schema().await.unwrap();
        let s = io_stats.incremental_stats();
        assert_eq!(s.read_iops, 0, "step 1, elapsed={:?}", start.elapsed());

        // Step 2: still within interval — no list
        table.schema().await.unwrap();
        let s = io_stats.incremental_stats();
        assert_eq!(s.read_iops, 0, "step 2, elapsed={:?}", start.elapsed());

        // Step 3: sleep past the 1s boundary
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Step 4: interval expired — exactly 1 list, timer resets
        table.schema().await.unwrap();
        let s = io_stats.incremental_stats();
        assert_eq!(s.read_iops, 1, "step 4, elapsed={:?}", start.elapsed());

        // Step 5: 10 more calls — timer just reset, no lists (THIS is the regression test).
        for _ in 0..10 {
            table.schema().await.unwrap();
        }
        let s = io_stats.incremental_stats();
        assert_eq!(s.read_iops, 0, "step 5, elapsed={:?}", start.elapsed());
    }
}
