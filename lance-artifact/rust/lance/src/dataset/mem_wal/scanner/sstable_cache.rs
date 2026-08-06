// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cache of opened SSTable datasets for the LSM scanner.
//!
//! SSTables are written exactly once to a globally-unique,
//! content-addressed path (see `memtable/flush.rs`): a fresh random hash per
//! flush invocation means the same path always maps to the same immutable
//! bytes. A cached `Arc<Dataset>` therefore can never go stale and needs no
//! TTL or invalidation for correctness — pruning entries is a pure memory
//! optimization driven by the consumer at compaction time.
//!
//! ```text
//! query ──> open_sstable(path, session, cache)
//!                                  │
//!            cache.is_some() ──────┤────── cache.is_none()
//!                  │                              │
//!     SsTableCache::get_or_open      DatasetBuilder::from_uri
//!     (single-flight, shared Arc)           (cold open every call)
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStoreParams;

use crate::dataset::{Dataset, DatasetBuilder};
use crate::session::Session;

/// Cache of opened SSTable datasets, keyed by resolved path.
///
/// SSTables live at a globally-unique, immutable path, so cached
/// entries are never stale and require no TTL. Intended to be held by a
/// long-lived owner (one per process or per table) and injected into
/// per-request scanners via [`crate::dataset::mem_wal::scanner::LsmScanner`]
/// (and the point-lookup / vector-search planners).
///
/// The key is the resolved absolute SSTable path
/// (`{base}/_mem_wal/{shard}/{folder}`), which is globally unique, so a single
/// cache can safely span multiple tables.
///
/// `store_params` is deliberately *not* part of the key: the first caller to
/// open a path binds the store that every later hit reuses. Credential rotation
/// still works — a vended-credential store holds the live
/// `StorageOptionsAccessor` and re-resolves per request, so a cached handle
/// never carries expired credentials. What this does assume is that a given
/// path is only ever served under one store configuration. Serving one table
/// through a single cache under two different `ObjectStoreParams` would hand
/// every caller the store the first one opened with.
pub struct SsTableCache {
    // `moka`'s async cache gives a bounded size plus single-flight
    // `try_get_with`, so concurrent first-queries on a just-flushed
    // SSTable open the dataset exactly once. The opened dataset carries the
    // session index cache, which also backs each generation's standalone PK
    // dedup index (see `block_list::open_pk_index`) — no separate cache path.
    inner: moka::future::Cache<String, Arc<Dataset>>,
}

impl SsTableCache {
    /// Create a cache holding at most `max_entries` opened datasets.
    ///
    /// Eviction is size-only (no TTL): an evicted-then-re-requested generation
    /// simply re-opens, which is always correct because the path is immutable.
    pub fn new(max_entries: u64) -> Self {
        Self {
            inner: moka::future::Cache::builder()
                .max_capacity(max_entries)
                // Required for `retain_paths`: moka silently ignores
                // `invalidate_entries_if` unless closure support is opted
                // into at build time.
                .support_invalidation_closures()
                .build(),
        }
    }

    /// Get the dataset for `path`, opening it (exactly once) on a miss.
    /// Concurrent callers share a single open via `moka`'s single-flight
    /// `try_get_with`; hits are a pure `Arc::clone`. `session` / `store_params`
    /// configure the open.
    pub async fn get_or_open(
        &self,
        path: &str,
        session: Option<Arc<Session>>,
        store_params: Option<ObjectStoreParams>,
    ) -> Result<Arc<Dataset>> {
        self.inner
            .try_get_with(path.to_string(), async move {
                let mut builder = DatasetBuilder::from_uri(path);
                if let Some(session) = session {
                    builder = builder.with_session(session);
                }
                if let Some(store_params) = store_params {
                    builder = builder.with_store_params(store_params);
                }
                builder.load().await.map(Arc::new)
            })
            .await
            // `try_get_with` hands losing racers an `Arc<Error>`; the original
            // error keeps full context, clones collapse to `Error::Cloned`.
            .map_err(|e: Arc<Error>| Error::cloned(e.to_string()))
    }

    /// Drop cached entries whose path is not in `live_paths`.
    ///
    /// Called by the consumer after compaction retires generations. Purely a
    /// memory optimization: stale entries are unobservable because a retired
    /// generation's path never reappears in a shard snapshot, so correctness
    /// never depends on this running. Invalidation is applied lazily by
    /// `moka` during its next maintenance cycle.
    pub fn retain_paths(&self, live_paths: &HashSet<String>) {
        let live = live_paths.clone();
        // The only error is exceeding moka's registered-predicate cap, which
        // would just defer reclamation — never a correctness issue.
        let _ = self
            .inner
            .invalidate_entries_if(move |path, _| !live.contains(path));
    }
}

impl std::fmt::Debug for SsTableCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsTableCache")
            .field("entry_count", &self.inner.entry_count())
            .finish()
    }
}

/// Caching of opened SSTable datasets, keyed by immutable path. The
/// opened dataset carries the session index cache, which also backs each
/// generation's secondary indexes and its PK dedup sidecar (see
/// `block_list::open_pk_index`) — so a single `get_or_open` is the
/// whole caching surface. Implemented by [`SsTableCache`]; a
/// [`SsTableWarmer`] composes one to warm through it, and a consumer may
/// supply its own implementation.
#[async_trait]
pub trait DatasetCache: Send + Sync + std::fmt::Debug {
    async fn get_or_open(
        &self,
        path: &str,
        session: Option<Arc<Session>>,
        store_params: Option<ObjectStoreParams>,
    ) -> Result<Arc<Dataset>>;

    /// Drop cached entries whose path is not in `live_paths`. Async so an
    /// implementation can evict retired generations' index objects (e.g.
    /// `Session::invalidate_index_prefix`) without a later breaking signature
    /// change; [`SsTableCache`]'s own eviction is synchronous.
    async fn retain_paths(&self, live_paths: &HashSet<String>);
}

#[async_trait]
impl DatasetCache for SsTableCache {
    async fn get_or_open(
        &self,
        path: &str,
        session: Option<Arc<Session>>,
        store_params: Option<ObjectStoreParams>,
    ) -> Result<Arc<Dataset>> {
        Self::get_or_open(self, path, session, store_params).await
    }

    async fn retain_paths(&self, live_paths: &HashSet<String>) {
        Self::retain_paths(self, live_paths)
    }
}

/// Proactively warms an SSTable into the shared caches: open the
/// dataset and pre-load its secondary indexes and PK dedup sidecar so the first
/// query sees no cold reads. This is the **seam** the flush and read paths fire
/// — lance defines it; the consumer (e.g. the WAL pod) implements it. `None` =>
/// no warming, generations warm lazily on first read.
///
/// Everything a warmer touches is keyed by the immutable generation `path`
/// (opened dataset, its secondary indexes, its PK dedup sidecar), so `path` is
/// the only input it needs.
///
/// `warm` is fired fire-and-forget from every read path that opens a generation
/// (all four LSM planners) as well as pre-commit on flush, so the same path may
/// be warmed concurrently and repeatedly. Implementations **must be idempotent
/// and cheap when the path is already warm** (e.g. dedup in-flight and
/// completed paths) — a redundant call must not re-do work or fail.
#[async_trait]
pub trait SsTableWarmer: Send + Sync + std::fmt::Debug {
    async fn warm(&self, path: &str) -> Result<()>;
}

/// Open an SSTable dataset, shared by all three LSM open sites
/// (scan, point lookup, vector search).
///
/// - `cache` present: route through a [`DatasetCache`] (e.g.
///   [`SsTableCache`]: single-flight, shared `Arc`, manifest read
///   amortized across queries).
/// - `cache` absent: cold open via [`DatasetBuilder`]. Passing `session`
///   still reuses the shared index / metadata caches; `None`/`None`
///   reproduces the original per-query cold-open behavior exactly.
/// - `warmer` present: fire a fire-and-forget warm-on-open backstop behind the
///   returned handle (the warmer dedups already-warm paths). `None` => no warming.
pub async fn open_sstable(
    path: &str,
    session: Option<&Arc<Session>>,
    store_params: Option<&ObjectStoreParams>,
    cache: Option<&Arc<dyn DatasetCache>>,
    warmer: Option<&Arc<dyn SsTableWarmer>>,
) -> Result<Arc<Dataset>> {
    let dataset = match cache {
        Some(cache) => {
            cache
                .get_or_open(path, session.cloned(), store_params.cloned())
                .await?
        }
        None => {
            let mut builder = DatasetBuilder::from_uri(path);
            if let Some(session) = session {
                builder = builder.with_session(session.clone());
            }
            if let Some(store_params) = store_params {
                builder = builder.with_store_params(store_params.clone());
            }
            Arc::new(builder.load().await?)
        }
    };
    if let Some(warmer) = warmer {
        let warmer = Arc::clone(warmer);
        let path = path.to_string();
        tokio::spawn(async move {
            if let Err(e) = warmer.warm(&path).await {
                tracing::debug!(generation = %path, error = %e, "warm-on-open failed");
            }
        });
    }
    Ok(dataset)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use crate::dataset::WriteParams;

    async fn write_dataset(uri: &str, ids: &[i32]) {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_hit_returns_same_arc() {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/gen_1", temp_dir.path().to_str().unwrap());
        write_dataset(&uri, &[1, 2, 3]).await;

        let cache = SsTableCache::new(8);
        let first = cache.get_or_open(&uri, None, None).await.unwrap();
        let second = cache.get_or_open(&uri, None, None).await.unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "a cache hit must return the same Arc<Dataset>, not re-open"
        );
        assert_eq!(cache.inner.entry_count(), 0); // not yet flushed to count
        cache.inner.run_pending_tasks().await;
        assert_eq!(cache.inner.entry_count(), 1);
    }

    #[tokio::test]
    async fn test_concurrent_get_or_open_single_flight() {
        // moka's `try_get_with` must collapse concurrent first-queries on the
        // same path into exactly one open. We can't count opens through the
        // public API, so wrap the cache call: every task that observes the
        // same returned Arc proves they shared one open.
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/gen_1", temp_dir.path().to_str().unwrap());
        write_dataset(&uri, &[1, 2, 3]).await;

        let cache = Arc::new(SsTableCache::new(8));
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = cache.clone();
            let uri = uri.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                cache.get_or_open(&uri, None, None).await.unwrap()
            }));
        }

        let datasets: Vec<Arc<Dataset>> = futures::future::try_join_all(handles).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 16, "all tasks ran");
        let first = &datasets[0];
        for ds in &datasets {
            assert!(
                Arc::ptr_eq(first, ds),
                "all concurrent callers must share one opened dataset"
            );
        }
        cache.inner.run_pending_tasks().await;
        assert_eq!(cache.inner.entry_count(), 1, "exactly one entry cached");
    }

    #[tokio::test]
    async fn test_retain_paths_drops_unreferenced() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = temp_dir.path().to_str().unwrap();
        let keep_uri = format!("{}/gen_1", base);
        let drop_uri = format!("{}/gen_2", base);
        write_dataset(&keep_uri, &[1]).await;
        write_dataset(&drop_uri, &[2]).await;

        let cache = SsTableCache::new(8);
        cache.get_or_open(&keep_uri, None, None).await.unwrap();
        cache.get_or_open(&drop_uri, None, None).await.unwrap();
        cache.inner.run_pending_tasks().await;
        assert_eq!(cache.inner.entry_count(), 2);

        let live: HashSet<String> = [keep_uri.clone()].into_iter().collect();
        cache.retain_paths(&live);
        cache.inner.run_pending_tasks().await;

        assert_eq!(cache.inner.entry_count(), 1, "only live path retained");
        assert!(cache.inner.contains_key(&keep_uri));
        assert!(!cache.inner.contains_key(&drop_uri));
    }

    #[tokio::test]
    async fn test_open_sstable_no_cache_matches_direct_open() {
        // The `None`/`None` path must reproduce a plain cold open: same data,
        // independent Arc per call (no caching).
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/gen_1", temp_dir.path().to_str().unwrap());
        write_dataset(&uri, &[7, 8, 9]).await;

        let a = open_sstable(&uri, None, None, None, None).await.unwrap();
        let b = open_sstable(&uri, None, None, None, None).await.unwrap();
        assert!(
            !Arc::ptr_eq(&a, &b),
            "no-cache path must cold-open each call"
        );
        assert_eq!(a.count_rows(None).await.unwrap(), 3);

        // With a cache, the second call is a shared clone.
        let cache: Arc<dyn DatasetCache> = Arc::new(SsTableCache::new(8));
        let c = open_sstable(&uri, None, None, Some(&cache), None)
            .await
            .unwrap();
        let d = open_sstable(&uri, None, None, Some(&cache), None)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&c, &d), "cached path must reuse the Arc");
    }

    /// A warmer that records calls and signals each one.
    #[derive(Debug)]
    struct NotifyingWarmer {
        calls: Arc<AtomicUsize>,
        notify: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl SsTableWarmer for NotifyingWarmer {
        async fn warm(&self, _path: &str) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_one();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_open_sstable_fires_warm_on_open() {
        // The warm-on-open backstop fires the warmer (fire-and-forget) when a
        // generation is opened, so generations the flusher never warmed still
        // get warmed lazily on first read.
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("{}/gen_1", temp_dir.path().to_str().unwrap());
        write_dataset(&uri, &[1, 2, 3]).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(tokio::sync::Notify::new());
        let warmer: Arc<dyn SsTableWarmer> = Arc::new(NotifyingWarmer {
            calls: calls.clone(),
            notify: notify.clone(),
        });

        let ds = open_sstable(&uri, None, None, None, Some(&warmer))
            .await
            .unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 3);

        // The warm is spawned fire-and-forget; wait (bounded) for it to run.
        tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
            .await
            .expect("warm-on-open must fire");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "warmer fired once on open");
    }
}
