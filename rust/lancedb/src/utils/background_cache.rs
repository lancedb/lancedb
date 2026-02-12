// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! A cache that refreshes values in the background before they expire.
//!
//! See [`BackgroundCache`] for details.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;

type SharedFut<V, E> = Shared<BoxFuture<'static, Result<V, Arc<E>>>>;

enum State<V, E> {
    Empty,
    Current(V, clock::Instant),
    Refreshing {
        previous: Option<(V, clock::Instant)>,
        future: SharedFut<V, E>,
    },
}

impl<V: Clone, E> State<V, E> {
    fn fresh_value(&self, ttl: Duration, refresh_window: Duration) -> Option<V> {
        let fresh_threshold = ttl - refresh_window;
        match self {
            Self::Current(value, cached_at) => {
                if clock::now().duration_since(*cached_at) < fresh_threshold {
                    Some(value.clone())
                } else {
                    None
                }
            }
            Self::Refreshing {
                previous: Some((value, cached_at)),
                ..
            } => {
                if clock::now().duration_since(*cached_at) < fresh_threshold {
                    Some(value.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

struct CacheInner<V, E> {
    state: State<V, E>,
    /// Incremented on invalidation. Background fetches check this to avoid
    /// overwriting with stale data after a concurrent invalidation.
    generation: u64,
}

enum Action<V, E> {
    Return(V),
    Wait(SharedFut<V, E>),
}

/// A cache that refreshes values in the background before they expire.
///
/// The cache has three states:
/// - **Empty**: No cached value. The next [`get()`](Self::get) blocks until a fetch completes.
/// - **Current**: A valid cached value with a timestamp. Returns immediately if fresh.
/// - **Refreshing**: A fetch is in progress. Returns the previous value if still valid,
///   otherwise blocks until the fetch completes.
///
/// When the cached value enters the refresh window (close to TTL expiry),
/// [`get()`](Self::get) starts a background fetch and returns the current value
/// immediately. Multiple concurrent callers share a single in-flight fetch.
pub struct BackgroundCache<V, E> {
    inner: Arc<Mutex<CacheInner<V, E>>>,
    ttl: Duration,
    refresh_window: Duration,
}

impl<V, E> Clone for BackgroundCache<V, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            ttl: self.ttl,
            refresh_window: self.refresh_window,
        }
    }
}

impl<V, E> BackgroundCache<V, E>
where
    V: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub fn new(ttl: Duration, refresh_window: Duration) -> Self {
        assert!(
            refresh_window < ttl,
            "refresh_window ({refresh_window:?}) must be less than ttl ({ttl:?})"
        );
        Self {
            inner: Arc::new(Mutex::new(CacheInner {
                state: State::Empty,
                generation: 0,
            })),
            ttl,
            refresh_window,
        }
    }

    /// Returns the cached value if it's fresh (not in the refresh window).
    ///
    /// This is a cheap synchronous check useful as a fast path before
    /// constructing a fetch closure for [`get()`](Self::get).
    pub fn try_get(&self) -> Option<V> {
        let cache = self.inner.lock().unwrap();
        cache.state.fresh_value(self.ttl, self.refresh_window)
    }

    /// Get the cached value, fetching if needed.
    ///
    /// The closure is called to create the fetch future only when a new fetch
    /// is needed. If the cache already has an in-flight fetch, the closure is
    /// not called and the caller joins the existing fetch.
    pub async fn get<F, Fut>(&self, fetch: F) -> Result<V, Arc<E>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
    {
        // Fast path: check if cache is fresh
        {
            let cache = self.inner.lock().unwrap();
            if let Some(value) = cache.state.fresh_value(self.ttl, self.refresh_window) {
                return Ok(value);
            }
        }

        // Slow path
        let mut fetch = Some(fetch);
        let action = {
            let mut cache = self.inner.lock().unwrap();
            self.determine_action(&mut cache, &mut fetch)
        };

        match action {
            Action::Return(value) => Ok(value),
            Action::Wait(fut) => fut.await,
        }
    }

    /// Invalidate the cache. The next [`get()`](Self::get) will start a fresh fetch.
    ///
    /// Any in-flight background fetch from before this call will not update the
    /// cache (the generation counter prevents stale writes).
    pub fn invalidate(&self) {
        let mut cache = self.inner.lock().unwrap();
        cache.state = State::Empty;
        cache.generation += 1;
    }

    fn determine_action<F, Fut>(
        &self,
        cache: &mut CacheInner<V, E>,
        fetch: &mut Option<F>,
    ) -> Action<V, E>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
    {
        match &cache.state {
            State::Empty => {
                let f = fetch
                    .take()
                    .expect("fetch closure required for empty cache");
                let shared = self.start_fetch(cache, f, None);
                Action::Wait(shared)
            }
            State::Current(value, cached_at) => {
                let elapsed = clock::now().duration_since(*cached_at);
                if elapsed < self.ttl - self.refresh_window {
                    Action::Return(value.clone())
                } else if elapsed < self.ttl {
                    // In refresh window: start background fetch, return current value
                    let value = value.clone();
                    let previous = Some((value.clone(), *cached_at));
                    if let Some(f) = fetch.take() {
                        // The spawned task inside start_fetch drives the future;
                        // we don't need to await the returned handle here.
                        drop(self.start_fetch(cache, f, previous));
                    }
                    Action::Return(value)
                } else {
                    // Expired: must wait for fetch
                    let previous = Some((value.clone(), *cached_at));
                    let f = fetch
                        .take()
                        .expect("fetch closure required for expired cache");
                    let shared = self.start_fetch(cache, f, previous);
                    Action::Wait(shared)
                }
            }
            State::Refreshing { previous, future } => {
                // If the background fetch already completed (spawned task hasn't
                // run yet to update state), transition the state and re-evaluate.
                if let Some(result) = future.peek() {
                    match result {
                        Ok(value) => {
                            cache.state = State::Current(value.clone(), clock::now());
                        }
                        Err(_) => {
                            cache.state = match previous.clone() {
                                Some((v, t)) => State::Current(v, t),
                                None => State::Empty,
                            };
                        }
                    }
                    return self.determine_action(cache, fetch);
                }

                if let Some((value, cached_at)) = previous {
                    if clock::now().duration_since(*cached_at) < self.ttl {
                        Action::Return(value.clone())
                    } else {
                        Action::Wait(future.clone())
                    }
                } else {
                    Action::Wait(future.clone())
                }
            }
        }
    }

    fn start_fetch<F, Fut>(
        &self,
        cache: &mut CacheInner<V, E>,
        fetch: F,
        previous: Option<(V, clock::Instant)>,
    ) -> SharedFut<V, E>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
    {
        let generation = cache.generation;
        let shared = async move { (fetch)().await.map_err(Arc::new) }
            .boxed()
            .shared();

        // Spawn task to eagerly drive the future and update state on completion
        let inner = self.inner.clone();
        let fut_for_spawn = shared.clone();
        tokio::spawn(async move {
            let result = fut_for_spawn.await;
            let mut cache = inner.lock().unwrap();
            // Only update if no invalidation has happened since we started
            if cache.generation != generation {
                return;
            }
            match result {
                Ok(value) => {
                    cache.state = State::Current(value, clock::now());
                }
                Err(_) => {
                    let prev = match &cache.state {
                        State::Refreshing { previous, .. } => previous.clone(),
                        _ => None,
                    };
                    cache.state = match prev {
                        Some((v, t)) => State::Current(v, t),
                        None => State::Empty,
                    };
                }
            }
        });

        cache.state = State::Refreshing {
            previous,
            future: shared.clone(),
        };

        shared
    }
}

#[cfg(test)]
pub mod clock {
    use std::cell::Cell;
    use std::time::Duration;

    // Re-export Instant so callers use the same type
    pub use std::time::Instant;

    thread_local! {
        static MOCK_NOW: Cell<Option<Instant>> = const { Cell::new(None) };
    }

    pub fn now() -> Instant {
        MOCK_NOW.with(|mock| mock.get().unwrap_or_else(Instant::now))
    }

    pub fn advance_by(duration: Duration) {
        MOCK_NOW.with(|mock| {
            let current = mock.get().unwrap_or_else(Instant::now);
            mock.set(Some(current + duration));
        });
    }

    #[allow(dead_code)]
    pub fn clear_mock() {
        MOCK_NOW.with(|mock| mock.set(None));
    }
}

#[cfg(not(test))]
mod clock {
    // Re-export Instant so callers use the same type
    pub use std::time::Instant;

    pub fn now() -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct TestError(String);

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    const TEST_TTL: Duration = Duration::from_secs(30);
    const TEST_REFRESH_WINDOW: Duration = Duration::from_secs(5);

    fn new_cache() -> BackgroundCache<String, TestError> {
        BackgroundCache::new(TEST_TTL, TEST_REFRESH_WINDOW)
    }

    fn ok_fetcher(
        counter: Arc<AtomicUsize>,
        value: &str,
    ) -> impl FnOnce() -> BoxFuture<'static, Result<String, TestError>> + Send + 'static {
        let value = value.to_string();
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(value) }.boxed()
        }
    }

    fn err_fetcher(
        counter: Arc<AtomicUsize>,
        msg: &str,
    ) -> impl FnOnce() -> BoxFuture<'static, Result<String, TestError>> + Send + 'static {
        let msg = msg.to_string();
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Err(TestError(msg)) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_basic_caching() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        let v1 = cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();
        assert_eq!(v1, "hello");
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Second call triggers peek transition to Current, returns cached
        let v2 = cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();
        assert_eq!(v2, "hello");
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Third call still cached
        let v3 = cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();
        assert_eq!(v3, "hello");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_try_get_returns_none_when_empty() {
        let cache: BackgroundCache<String, TestError> = new_cache();
        assert!(cache.try_get().is_none());
    }

    #[tokio::test]
    async fn test_try_get_returns_value_when_fresh() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();
        // Peek transition
        cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();

        assert_eq!(cache.try_get().unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_try_get_returns_none_in_refresh_window() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "hello")).await.unwrap(); // peek

        clock::advance_by(Duration::from_secs(26));
        assert!(cache.try_get().is_none());
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek
        assert_eq!(count.load(Ordering::SeqCst), 1);

        clock::advance_by(Duration::from_secs(31));

        let v = cache.get(ok_fetcher(count.clone(), "v2")).await.unwrap();
        assert_eq!(v, "v2");
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_invalidate_forces_refetch() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek
        assert_eq!(count.load(Ordering::SeqCst), 1);

        cache.invalidate();

        let v = cache.get(ok_fetcher(count.clone(), "v2")).await.unwrap();
        assert_eq!(v, "v2");
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_concurrent_get_single_fetch() {
        let cache = Arc::new(new_cache());
        let count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let count = count.clone();
            handles.push(tokio::spawn(async move {
                cache.get(ok_fetcher(count, "hello")).await.unwrap()
            }));
        }

        let results: Vec<String> = futures::future::try_join_all(handles).await.unwrap();
        for r in &results {
            assert_eq!(r, "hello");
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_background_refresh_in_window() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        // Populate and transition to Current
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Move into refresh window
        clock::advance_by(Duration::from_secs(26));

        // Returns cached value and starts background fetch
        let v = cache.get(ok_fetcher(count.clone(), "v2")).await.unwrap();
        assert_eq!(v, "v1"); // Still old value
        assert_eq!(count.load(Ordering::SeqCst), 1); // bg task hasn't run yet

        // Advance past TTL to force waiting on the shared future
        clock::advance_by(Duration::from_secs(30));

        let v = cache.get(ok_fetcher(count.clone(), "v3")).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(v, "v2"); // Got the bg refresh result
    }

    #[tokio::test]
    async fn test_no_duplicate_background_refreshes() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        // Populate and transition to Current
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Move into refresh window
        clock::advance_by(Duration::from_secs(26));

        // Multiple calls should all return cached, only one bg fetch
        for _ in 0..5 {
            let v = cache.get(ok_fetcher(count.clone(), "v2")).await.unwrap();
            assert_eq!(v, "v1");
        }

        // Drive the shared future to completion
        clock::advance_by(Duration::from_secs(30));
        cache.get(ok_fetcher(count.clone(), "v3")).await.unwrap();

        // Only 1 additional fetch (the background refresh)
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_background_refresh_error_preserves_cache() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        // Populate and transition to Current
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Move into refresh window
        clock::advance_by(Duration::from_secs(26));

        // Start bg refresh that will fail, returns cached value
        let v = cache.get(err_fetcher(count.clone(), "fail")).await.unwrap();
        assert_eq!(v, "v1");

        // Still in refresh window, previous is valid
        let v = cache.get(err_fetcher(count.clone(), "fail")).await.unwrap();
        assert_eq!(v, "v1");

        // Advance past TTL to drive the failed future
        clock::advance_by(Duration::from_secs(30));

        // The peek error path restores previous, but it's expired,
        // so a new fetch is needed. This one also fails.
        let result = cache.get(err_fetcher(count.clone(), "fail again")).await;
        assert!(result.is_err());
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_invalidation_during_fetch_prevents_stale_update() {
        let cache = new_cache();
        let count = Arc::new(AtomicUsize::new(0));

        // Populate and transition to Current
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap();
        cache.get(ok_fetcher(count.clone(), "v1")).await.unwrap(); // peek

        // Move into refresh window to start background fetch
        clock::advance_by(Duration::from_secs(26));
        cache.get(ok_fetcher(count.clone(), "stale")).await.unwrap();

        // Invalidate before bg task completes
        cache.invalidate();

        // Advance past TTL
        clock::advance_by(Duration::from_secs(30));

        // Should get fresh data, not the stale background result
        let v = cache.get(ok_fetcher(count.clone(), "fresh")).await.unwrap();
        assert_eq!(v, "fresh");
    }
}
