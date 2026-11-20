// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Fork-safe wrapper around tokio + pyo3-async-runtimes.
//!
//! `pyo3_async_runtimes::tokio` keeps its multi-threaded runtime in a
//! `OnceLock` that can never be replaced. Tokio's worker threads do not
//! survive `fork()`, so once a child inherits a "frozen" runtime, every
//! `future_into_py` call hangs forever. Normal (non-fork) process exit has
//! its own gap: nothing tells the runtime to shut down, so its worker
//! threads keep running, uncoordinated with the interpreter, right up until
//! the process ends. If one of them is mid-task exactly as `Py_Finalize`
//! starts tearing down interpreter state, it can panic on state that's
//! already gone -- and since that happens on a background thread with no
//! PyO3-wrapped call frame to catch it, Rust aborts the whole process
//! rather than failing that one call. [`shutdown`], registered as a Python
//! `atexit` callback, closes that gap by giving the runtime a coordinated,
//! bounded exit while the interpreter is still fully valid.
//!
//! Getting both of these right at once took a few tries; the design here
//! rests on three separate mechanisms, each solving one problem the others
//! cannot:
//!
//! **A per-generation counter, not `Arc::strong_count`, decides when the
//! runtime is idle.** Early versions tried to infer "is anything still
//! using this runtime" from how many `Arc<Runtime>` clones existed. That
//! signal is wrong in both directions: a clone taken only for the instant a
//! task is *submitted* says nothing about whether that task has actually
//! finished running (`Runtime::shutdown_timeout` gives spawned, non-blocking
//! tasks no grace period at all -- a task "keeps running until it yields,
//! then is dropped" -- so a reclaim landing right after submission would
//! silently abandon it before it ever got to run); and a clone held for a
//! task's *whole* lifetime can end up making that task the final owner of
//! the `Runtime`, so completing it drops the `Runtime` from inside one of
//! its own worker threads, which tokio itself forbids ("cannot drop a
//! runtime in a context where blocking is not allowed") and panics.
//! [`GenerationCalls`] is an explicit courtesy counter instead: every
//! top-level `spawn`, `spawn_blocking`, or `block_on` call increments it
//! before it starts and decrements it (via [`OutstandingGuard`]) when it is
//! truly done, entirely decoupled from how many `Arc` clones exist at any
//! instant. `shutdown` waits for the current generation's counter to reach
//! zero before ever touching the runtime, which also closes a narrower
//! race: because the counter is incremented *before* `get_runtime()` is
//! even called, an install already in progress when `shutdown` runs is
//! never invisible to it the way an empty slot would be. If the counter
//! never reaches zero within the bound, `shutdown` stops waiting and forces
//! the retirement attempt anyway -- silently returning with the runtime and
//! its workers still fully alive would just recreate the exact race this
//! function exists to close, for any call slower than the grace period.
//!
//! **The counter is scoped to a fork generation, not the process.** A
//! single process-wide count cannot survive `fork()`: the child inherits the
//! parent's in-flight value, but the tasks that would decrement it do not
//! run there, and a guard copied onto the forking thread would later
//! decrement the child's count even though it belongs to the parent. Every
//! [`OutstandingGuard`] therefore holds an `Arc<GenerationCalls>` for the
//! generation it was created in. A stale guard can only decrement its own
//! counter, while the child's first call lazily creates a fresh counter, and
//! `shutdown` only waits on the counter matching the live generation.
//!
//! **Tasks never hold an `Arc<Runtime>`.** Because the generation counter
//! (not reference counting) is what `shutdown` waits on, a top-level task
//! only needs to carry an `OutstandingGuard` -- a token whose `Drop` is a
//! plain atomic decrement -- not a clone of the runtime itself. That is what
//! makes it impossible for a task's completion to become the final,
//! worker-thread-side drop of the `Runtime`: nothing a task holds ever
//! *is* the `Runtime`.
//!
//! **`atfork_child` touches nothing but a plain counter.** `future_into_py`
//! spawns a task that, once running, spawns a second one to do the real
//! work and awaits its `JoinHandle`; if a nested call re-resolved "the
//! current runtime" independently, a reclaim landing between the two calls
//! could bind them to different instances. `spawn`/`spawn_blocking` close
//! that with `Handle::try_current`: a call already running on one of our
//! worker threads stays pinned to that instance, so only the first,
//! outermost call of a chain ever consults [`get_runtime`]. That leaves
//! fork as the other place identity can change, and it has to be handled
//! without ever calling into `ArcSwapOption` from the child handler itself
//! -- `swap`/`compare_and_swap` reconcile reader "debts" internally (via
//! thread-local state, and potentially an allocation), none of which is
//! safe to run in a forked child that may have inherited another thread's
//! lock mid-acquisition. `atfork_child` therefore does nothing but bump a
//! bare `GENERATION` counter; [`get_runtime`] compares the generation its
//! installed runtime was built in against the live counter on every call,
//! from ordinary (non-signal) context, and treats a mismatch as "stale,
//! rebuild" -- exactly the check `atfork_child` used to perform directly.
//! [`shutdown`] makes the same comparison, because a child that exits
//! without ever installing its own runtime still finds the parent's stale
//! [`Tagged`] in the slot. It removes that slot and deliberately leaks the
//! instance instead of calling `shutdown_timeout` on it: its worker threads
//! do not exist in this process, so waiting for its counter or trying to
//! join them would hang, and a leaked reference prevents any inherited
//! `Arc` clone from becoming the final owner that drops it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

/// A runtime tagged with the fork generation it was built in, so a stale
/// (post-fork, dead-worker-threads) instance can be told apart from a live
/// one without `atfork_child` ever having to touch it directly.
struct Tagged {
    runtime: runtime::Runtime,
    generation: u64,
}

impl std::ops::Deref for Tagged {
    type Target = runtime::Runtime;
    fn deref(&self) -> &runtime::Runtime {
        &self.runtime
    }
}

static RUNTIME: ArcSwapOption<Tagged> = ArcSwapOption::const_empty();
/// Bumped only by `atfork_child`, and only ever read elsewhere. This is the
/// entire fork-safety mechanism: no lock, no allocation, no thread-local
/// access -- just one atomic add, which is all a `pthread_atfork` child
/// handler is ever safe to do.
static GENERATION: AtomicU64 = AtomicU64::new(0);
/// In-flight top-level calls for one fork generation. Guards hold an `Arc`
/// to this object for their whole lifetime, so one inherited across `fork()`
/// can only decrement the counter it was created against, never the child's
/// fresh one.
struct GenerationCalls {
    generation: u64,
    outstanding: AtomicUsize,
}
static CALLS: ArcSwapOption<GenerationCalls> = ArcSwapOption::const_empty();
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(test))]
fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

#[cfg(test)]
fn create_runtime() -> runtime::Runtime {
    let mut builder = runtime::Builder::new_multi_thread();
    builder.enable_all().thread_name("lancedb-tokio-worker");
    // Fork tests hold two runtimes open at once. Keeping their worker count
    // small avoids turning an unrelated parallel build on the same host into
    // an apparent hang when the process thread limit is close to full.
    let worker_threads = TEST_WORKER_THREADS.load(Ordering::Relaxed);
    if worker_threads != 0 {
        builder.worker_threads(worker_threads);
    }
    builder.build().expect("Failed to build tokio runtime")
}

/// Register the fork-child handler once, before any generation-scoped state
/// is created.
///
/// The first top-level call deliberately installs the hook before it starts
/// counting itself. Otherwise a fork between taking that first counter and
/// `get_runtime()` installing the hook would leave the child sharing the
/// parent's counter with no generation change to separate them.
fn ensure_atfork_installed() {
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
}

/// Get the in-flight counter for the current fork generation, creating it on
/// the first top-level call after a fork.
fn current_calls() -> Arc<GenerationCalls> {
    ensure_atfork_installed();
    let current_gen = GENERATION.load(Ordering::SeqCst);
    loop {
        let existing = CALLS.load_full();
        if let Some(existing) = &existing
            && existing.generation == current_gen
        {
            return Arc::clone(existing);
        }
        let candidate = Arc::new(GenerationCalls {
            generation: current_gen,
            outstanding: AtomicUsize::new(0),
        });
        let previous = CALLS.compare_and_swap(&existing, Some(Arc::clone(&candidate)));
        let won = match (&*previous, &existing) {
            (None, None) => true,
            (Some(prev), Some(exist)) => Arc::ptr_eq(prev, exist),
            _ => false,
        };
        if won {
            // A stale counter is only an integer: unlike a runtime it has no
            // worker threads to join, so letting the slot's old reference go
            // here is safe even in a forked child.
            return candidate;
        }
    }
}

/// Get a live, owned handle to the shared runtime, rebuilding it if the
/// installed one predates the most recent `fork()`.
fn get_runtime() -> Arc<Tagged> {
    ensure_atfork_installed();
    let current_gen = GENERATION.load(Ordering::SeqCst);
    loop {
        let existing = RUNTIME.load_full();
        if let Some(existing) = &existing
            && existing.generation == current_gen
        {
            return Arc::clone(existing);
        }
        // Built optimistically, outside any lock: on the rare race where two
        // threads both find the slot empty (or stale), one candidate wins
        // the compare-and-swap below and the other is simply dropped here,
        // tearing down its own (never shared, never used) worker pool the
        // ordinary way.
        let candidate = Arc::new(Tagged {
            runtime: create_runtime(),
            generation: current_gen,
        });
        let previous = RUNTIME.compare_and_swap(&existing, Some(Arc::clone(&candidate)));
        let won = match (&*previous, &existing) {
            (None, None) => true,
            (Some(prev), Some(exist)) => Arc::ptr_eq(prev, exist),
            _ => false,
        };
        if won {
            if let Some(stale) = existing {
                // A prior generation's runtime: its worker threads are dead
                // in this process (they do not survive fork), so dropping it
                // normally would try to join them and hang. Leak it instead.
                std::mem::forget(stale);
            }
            return candidate;
        }
        // Someone else's candidate (or a concurrent shutdown) won; go around
        // and reload.
    }
}

/// RAII token tracked by the current [`GenerationCalls`] counter. Held for
/// the duration of a top-level `block_on` call, or moved into a top-level
/// `spawn`/`spawn_blocking` task so it decrements only once that task's
/// *entire* body -- including anything it nested-spawns and awaits -- has run
/// to completion or been dropped without completing.
struct OutstandingGuard {
    calls: Arc<GenerationCalls>,
}

impl OutstandingGuard {
    fn new() -> Self {
        let calls = current_calls();
        calls.outstanding.fetch_add(1, Ordering::SeqCst);
        Self { calls }
    }
}

impl Drop for OutstandingGuard {
    fn drop(&mut self) {
        self.calls.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Block the current thread on a future using the shared runtime.
///
/// For sync `#[pyfunction]`s that need to drive an async operation (e.g.
/// building a namespace client). Must not be called from within the runtime's
/// own worker threads.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    let _guard = OutstandingGuard::new();
    get_runtime().block_on(fut)
}

/// Run a detached task, including when the caller is already on a runtime worker.
/// Keep it visible to [`shutdown`] until it finishes.
pub fn spawn_background<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let guard = OutstandingGuard::new();
    let task = async move {
        let _guard = guard;
        fut.await;
    };
    // Detaching is the point: dropping the `JoinHandle` leaves the task running, and the
    // `OutstandingGuard` it carries is what keeps it visible to `shutdown`. Written as `drop`
    // rather than `let _ =`, which reads as discarding an unpolled future.
    match runtime::Handle::try_current() {
        Ok(handle) => drop(handle.spawn(task)),
        Err(_) => drop(get_runtime().spawn(task)),
    }
}

/// Gracefully quiesce the shared runtime, meant to run at normal process exit.
///
/// Waits (bounded by `timeout`) for the current generation's
/// [`GenerationCalls`] counter to reach zero -- i.e. for every top-level call
/// already under way in *this* process to actually finish, not just for
/// `Arc::strong_count` to look low -- before ever touching the runtime. Work
/// inherited from a parent process at `fork()` belongs to an older
/// generation and is never waited on. If the bound elapses first, it stops
/// waiting and attempts retirement anyway: leaving the runtime and its worker
/// threads untouched would just recreate the exact race this function exists
/// to close, for any call slower than the grace period.
///
/// Retirement itself removes the runtime from the slot and calls
/// `shutdown_timeout` rather than a bare `drop`: dropping a tokio `Runtime`
/// waits (in the worst case indefinitely) for its worker threads to join,
/// whereas `shutdown_timeout` gives real in-flight work -- a connection
/// pool's keep-alive, a graceful close -- a bounded chance to finish first,
/// then forcibly ends whatever has not. Reclaiming can only proceed once
/// `Arc::try_unwrap` proves no other reference remains; if some transient
/// `get_runtime()` caller is, at that exact instant, still between loading
/// the slot and finishing its own call, this abandons the runtime instead
/// of forcing the issue -- the same trade `atfork_child` already makes.
///
/// A runtime tagged with a generation older than the live counter was
/// inherited through `fork()` and is never retired here; see the module docs.
///
/// Neither of the two ways this can fail to cleanly retire the runtime --
/// the wait timing out, or `try_unwrap` losing that race -- has any other
/// signal to report through (`shutdown_timeout` itself returns nothing),
/// so both log a warning instead of failing silently.
pub fn shutdown(timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let generation = GENERATION.load(Ordering::SeqCst);
    let calls = CALLS.load_full();
    let calls = calls
        .as_ref()
        .filter(|calls| calls.generation == generation);
    loop {
        let outstanding = calls.map_or(0, |calls| calls.outstanding.load(Ordering::SeqCst));
        if outstanding == 0 {
            break;
        }
        if Instant::now() >= deadline {
            log::warn!(
                "lancedb: runtime shutdown timed out with {outstanding} call(s) still in flight; forcing shutdown anyway, some in-flight work may be abandoned"
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let Some(current) = RUNTIME.load_full() else {
        return;
    };
    if current.generation != generation {
        // The child inherited the parent's installed runtime. Its worker
        // threads do not exist here, so waiting on its counter or calling
        // `shutdown_timeout` would wait for work that can never finish and
        // join threads that can never exit. Remove the slot in ordinary
        // context and deliberately leak the instance: inherited `Arc` clones
        // may still be dropped in this process, and the leaked reference
        // keeps a dropped clone from becoming the final owner.
        RUNTIME.compare_and_swap(&Some(Arc::clone(&current)), None);
        std::mem::forget(current);
        return;
    }
    RUNTIME.compare_and_swap(&Some(Arc::clone(&current)), None);
    match Arc::try_unwrap(current) {
        Ok(tagged) => {
            tagged
                .runtime
                .shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        }
        Err(_) => {
            // Some transient `get_runtime()` caller is, at this exact
            // instant, still between loading the slot and finishing its own
            // call: we have no owned handle to call `shutdown_timeout` on,
            // and no way to force one (tokio has no shutdown API over a
            // shared reference). Nothing more to do but say so.
            log::warn!(
                "lancedb: runtime shutdown could not obtain exclusive ownership; the shared runtime was left running"
            );
        }
    }
}

/// Runs in async-signal context after `fork()` in the child. Touches
/// nothing but a plain atomic add -- see the module docs for why even
/// `ArcSwapOption::swap` is not safe to call here.
extern "C" fn atfork_child() {
    GENERATION.fetch_add(1, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Marker type implementing [`Runtime`] over our fork-safe runtime slot.
pub struct LanceRuntime;

/// Newtype wrapper around `tokio::task::JoinError` so we can implement the
/// foreign [`JoinError`] trait without violating orphan rules.
pub struct LanceJoinError(task::JoinError);

impl JoinError for LanceJoinError {
    fn is_panic(&self) -> bool {
        self.0.is_panic()
    }
    fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        self.0.into_panic()
    }
}

impl Runtime for LanceRuntime {
    type JoinError = LanceJoinError;
    type JoinHandle = Pin<Box<dyn Future<Output = Result<(), Self::JoinError>> + Send>>;

    /// `pyo3_async_runtimes::generic::future_into_py` spawns a task that,
    /// once it starts running, spawns a second one to do the real work and
    /// awaits its `JoinHandle`. `Handle::try_current` pins that nested call
    /// to whatever runtime is already executing it, so only the first,
    /// outermost call of a chain -- one running on a thread outside any
    /// runtime -- ever consults [`get_runtime`] or [`OutstandingGuard`].
    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn(fut),
            Err(_) => {
                let guard = OutstandingGuard::new();
                get_runtime().spawn(async move {
                    let _guard = guard;
                    fut.await;
                })
            }
        };
        Box::pin(async move { handle.await.map_err(LanceJoinError) })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn_blocking(f),
            Err(_) => {
                let guard = OutstandingGuard::new();
                get_runtime().spawn_blocking(move || {
                    let _guard = guard;
                    f();
                })
            }
        };
        Box::pin(async move { handle.await.map_err(LanceJoinError) })
    }
}

tokio::task_local! {
    static TASK_LOCALS: std::cell::OnceCell<TaskLocals>;
}

impl ContextExt for LanceRuntime {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = std::cell::OnceCell::new();
        cell.set(locals).unwrap();
        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS
            .try_with(|c| c.get().cloned())
            .unwrap_or_default()
    }
}

/// Drop-in replacement for `pyo3_async_runtimes::tokio::future_into_py` that
/// uses our fork-safe runtime.
pub fn future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    pyo3_async_runtimes::generic::future_into_py::<LanceRuntime, _, T>(py, fut)
}

#[cfg(test)]
mod tests {
    use super::*;

    // RUNTIME, CALLS, GENERATION, and ATFORK_INSTALLED are process-wide
    // statics, and Rust's test harness runs tests in parallel by default,
    // so separate test functions below would otherwise race each other
    // through this shared state (this reproduced in CI: one test's
    // in-flight task got reclaimed by a *different* test's concurrent
    // `shutdown()` call, and another observed the in-flight counter left
    // non-zero by a still-running sibling). Every test takes this lock first
    // so only one of them touches the shared runtime state at a time; a
    // poisoned lock (a previous test's genuine failure) is still honored
    // rather than cascading into every later test as an unrelated panic.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_runtime_state_for_test() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // A task's own completion must never be the final drop of the shared
    // `Runtime`: tasks carry only an `OutstandingGuard` (a plain counter
    // token), never an `Arc<Runtime>`, specifically so this can't happen.
    // Getting this wrong panics ("cannot drop a runtime in a context where
    // blocking is not allowed") -- this reproduced unprompted, twice, in a
    // single run of `test_nested_spawn_survives_concurrent_shutdown` under
    // an earlier design that held the `Arc` for a task's whole lifetime.
    #[test]
    #[allow(unused_must_use)] // fire-and-forget spawn, same as future_into_py itself
    fn test_top_level_task_survives_concurrent_shutdown_reclaim() {
        use std::sync::mpsc;
        let _lock = lock_runtime_state_for_test();

        for _ in 0..50 {
            let (tx, rx) = mpsc::channel::<()>();

            LanceRuntime::spawn(async move {
                for _ in 0..5 {
                    task::yield_now().await;
                }
                let _ = tx.send(());
            });

            shutdown(Duration::from_secs(2));

            rx.recv_timeout(Duration::from_secs(5))
                .expect("top-level task was abandoned by a concurrent shutdown reclaim");
        }
    }

    #[test]
    fn test_shutdown_stops_and_the_runtime_rebuilds_lazily_after() {
        let _lock = lock_runtime_state_for_test();

        // No runtime created yet in this process: shutdown must be a no-op,
        // not a null-pointer dereference.
        shutdown(Duration::from_secs(1));

        // Force the runtime into existence, then shut it down. Bounded by
        // the timeout, so a hang here means shutdown itself is broken, not
        // that the test is slow.
        assert_eq!(block_on(async { 1 + 1 }), 2);
        shutdown(Duration::from_secs(5));

        // A caller after shutdown -- e.g. a stray call racing with the
        // atexit callback -- must get a fresh, working runtime rather than
        // a dangling reference into the one just torn down.
        assert_eq!(block_on(async { 2 + 2 }), 4);

        // Shutting down twice in a row (e.g. atexit firing more than once)
        // must not panic or double-free.
        shutdown(Duration::from_secs(5));
        shutdown(Duration::from_secs(5));
    }

    // Adapted from the concurrent reproducer that found the original bug:
    // many threads hammering the runtime while shutdown races them, not
    // just the sequential rebuild path above. Before every reader held its
    // own `Arc`, this could dereference a runtime `shutdown` had already
    // freed, or hang forever. Repeated, since a race is not guaranteed to
    // show up on any single attempt.
    #[test]
    fn test_shutdown_is_safe_concurrently_with_live_callers() {
        use std::sync::Barrier;
        use std::sync::atomic::AtomicBool as StopFlag;
        let _lock = lock_runtime_state_for_test();

        for _ in 0..50 {
            let barrier = Arc::new(Barrier::new(9));
            let stop = Arc::new(StopFlag::new(false));
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    let stop = Arc::clone(&stop);
                    std::thread::spawn(move || {
                        barrier.wait();
                        while !stop.load(Ordering::Relaxed) {
                            assert_eq!(block_on(async { 1 + 1 }), 2);
                        }
                    })
                })
                .collect();

            barrier.wait();
            shutdown(Duration::from_millis(50));
            stop.store(true, Ordering::Relaxed);
            for worker in workers {
                worker.join().unwrap();
            }
        }
    }

    // Reproduces the actual bug mechanism, not just concurrent `block_on`
    // traffic: `future_into_py` spawns an outer task that, once running,
    // spawns a second (inner) one for the real work and awaits its
    // `JoinHandle` (see `pyo3_async_runtimes::generic::future_into_py_with_locals`).
    // Before `spawn`/`spawn_blocking` pinned nested calls to whatever
    // runtime is already executing them, a reclaim landing between the
    // outer and inner spawn could bind them to two different runtime
    // instances -- and if the outer task's own runtime was the one torn
    // down while it awaited the inner task, it never resumed. Every wait
    // here is bounded so a reintroduced bug fails this test instead of
    // hanging the suite.
    #[test]
    #[allow(unused_must_use)] // fire-and-forget outer spawn, same as future_into_py itself
    fn test_nested_spawn_survives_concurrent_shutdown() {
        use std::sync::mpsc;
        let _lock = lock_runtime_state_for_test();

        for _ in 0..50 {
            let stop = Arc::new(AtomicBool::new(false));
            let (done_tx, done_rx) = mpsc::channel::<()>();

            let workers: Vec<_> = (0..8)
                .map(|_| {
                    let stop = Arc::clone(&stop);
                    let done_tx = done_tx.clone();
                    std::thread::spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            let (tx, rx) = mpsc::sync_channel::<()>(1);
                            LanceRuntime::spawn(async move {
                                let inner = LanceRuntime::spawn(async {
                                    let _ = 1 + 1;
                                });
                                let _ = inner.await;
                                let _ = tx.send(());
                            });
                            // A hang here across a concurrent shutdown is
                            // exactly the bug this test exists to catch.
                            let _ = rx.recv_timeout(Duration::from_secs(2));
                        }
                        let _ = done_tx.send(());
                    })
                })
                .collect();

            std::thread::sleep(Duration::from_millis(10));
            shutdown(Duration::from_millis(50));
            stop.store(true, Ordering::Relaxed);

            for _ in 0..8 {
                done_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("worker hung after shutdown raced a nested spawn");
            }
            for worker in workers {
                worker.join().unwrap();
            }
        }
    }

    // Forces the exact interleaving the install-race finding described: an
    // installer registers as outstanding (as `spawn`'s outermost branch
    // does, before it ever calls `get_runtime()`) and then pauses, while a
    // concurrent `shutdown()` must not decide "nothing here" and return
    // before that install actually completes and is retired in turn.
    #[test]
    fn test_shutdown_waits_for_a_racing_install() {
        use std::sync::mpsc;
        let _lock = lock_runtime_state_for_test();

        // Clean slate: no runtime installed, no in-flight calls.
        shutdown(Duration::from_secs(5));

        let (installer_ready_tx, installer_ready_rx) = mpsc::channel::<()>();
        let (proceed_tx, proceed_rx) = mpsc::channel::<()>();

        let installer = std::thread::spawn(move || {
            let _guard = OutstandingGuard::new();
            installer_ready_tx.send(()).unwrap();
            proceed_rx.recv().unwrap();
            assert_eq!(get_runtime().block_on(async { 1 + 1 }), 2);
        });

        installer_ready_rx.recv().unwrap();
        let shutdown_thread = std::thread::spawn(|| shutdown(Duration::from_secs(5)));
        // Give shutdown's polling loop several chances to (wrongly) observe
        // an idle runtime before the installer is allowed to proceed.
        std::thread::sleep(Duration::from_millis(50));
        proceed_tx.send(()).unwrap();

        installer.join().unwrap();
        shutdown_thread.join().unwrap();

        // shutdown must have waited for the install to finish and then
        // retired it, not returned early and left it stranded.
        assert!(RUNTIME.load_full().is_none());
        let calls = CALLS
            .load_full()
            .expect("install race left no generation counter");
        assert_eq!(calls.outstanding.load(Ordering::SeqCst), 0);
    }

    /// Keep fork-test runtimes small; see [`TEST_WORKER_THREADS`].
    #[cfg(target_os = "linux")]
    struct TestWorkerThreads(usize);

    #[cfg(target_os = "linux")]
    impl TestWorkerThreads {
        fn new(worker_threads: usize) -> Self {
            Self(TEST_WORKER_THREADS.swap(worker_threads, Ordering::SeqCst))
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for TestWorkerThreads {
        fn drop(&mut self) {
            TEST_WORKER_THREADS.store(self.0, Ordering::SeqCst);
        }
    }

    /// Run `f` in a forked child and return the `i32` it reports, or `None`
    /// if the child did not report within the bound. The child leaves with
    /// `_exit` so it never runs destructors inherited from the test process.
    #[cfg(target_os = "linux")]
    fn run_in_forked_child<F>(f: F) -> Option<i32>
    where
        F: FnOnce() -> i32,
    {
        let mut pipe = [0; 2];
        assert_eq!(unsafe { libc::pipe(pipe.as_mut_ptr()) }, 0, "pipe failed");
        let (read_fd, write_fd) = (pipe[0], pipe[1]);

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::close(read_fd) };
            let result = f();
            let bytes = result.to_ne_bytes();
            let written = unsafe { libc::write(write_fd, bytes.as_ptr().cast(), bytes.len()) };
            unsafe { libc::close(write_fd) };
            unsafe {
                libc::_exit(if written == bytes.len() as isize {
                    0
                } else {
                    1
                })
            };
        }

        unsafe { libc::close(write_fd) };
        let mut poll_fd = libc::pollfd {
            fd: read_fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = loop {
            let ready = unsafe { libc::poll(&mut poll_fd, 1, 10_000) };
            if ready >= 0 {
                break ready;
            }
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::Interrupted,
                "poll failed: {error}"
            );
        };
        if ready == 0 {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }

        let mut result = None;
        if ready == 1 {
            let mut bytes = [0u8; std::mem::size_of::<i32>()];
            let read = loop {
                let read = unsafe { libc::read(read_fd, bytes.as_mut_ptr().cast(), bytes.len()) };
                if read >= 0 {
                    break read;
                }
                let error = std::io::Error::last_os_error();
                assert_eq!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted,
                    "read failed: {error}"
                );
            };
            if read == bytes.len() as isize {
                result = Some(i32::from_ne_bytes(bytes));
            }
        }
        unsafe { libc::close(read_fd) };

        let mut status = 0;
        loop {
            let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
            if waited == pid {
                break;
            }
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::Interrupted,
                "waitpid failed: {error}"
            );
        }
        if ready == 0 {
            return None;
        }
        assert_eq!(status, 0, "forked child did not exit successfully");
        result
    }

    // The child inherits the parent's installed runtime and its outstanding
    // count, but not the worker thread that would ever release that count.
    // Its exit-time shutdown must therefore ignore both: waiting for parent
    // work would burn the whole grace period, and retiring the inherited
    // runtime would try to join worker threads that do not exist here.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_forked_child_shutdown_does_not_wait_for_parent_work() {
        use std::sync::mpsc;
        use tokio::sync::oneshot;

        let _lock = lock_runtime_state_for_test();
        let _worker_threads = TestWorkerThreads::new(2);
        shutdown(Duration::from_secs(5));

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<()>();
        drop(LanceRuntime::spawn(async move {
            started_tx.send(()).unwrap();
            let _ = release_rx.await;
            let _ = done_tx.send(());
        }));
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parent task did not start");

        let child_result = run_in_forked_child(|| {
            // Much longer than the watchdog: if this waits for inherited
            // parent work at all, the child fails to report in time instead
            // of the test comparing two elapsed durations.
            shutdown(Duration::from_secs(30));
            i32::from(RUNTIME.load_full().is_some())
        });
        assert_eq!(
            child_result,
            Some(0),
            "child shutdown waited for parent work or retired the inherited runtime"
        );

        release_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parent task was disturbed by the forked child");
        shutdown(Duration::from_secs(5));
    }

    // A child that builds its own runtime must drain that runtime's own
    // work, and only that work: the parent's held task is still outstanding
    // in the inherited copy of the counter but can never finish here.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_forked_child_drains_its_own_runtime_without_waiting_for_parent_work() {
        use std::sync::mpsc;
        use tokio::sync::oneshot;

        let _lock = lock_runtime_state_for_test();
        let _worker_threads = TestWorkerThreads::new(2);
        shutdown(Duration::from_secs(5));

        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<()>();
        drop(LanceRuntime::spawn(async move {
            started_tx.send(()).unwrap();
            let _ = release_rx.await;
            let _ = done_tx.send(());
        }));
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parent task did not start");

        let child_result = run_in_forked_child(|| {
            let child_done = Arc::new(AtomicBool::new(false));
            let child_done_flag = Arc::clone(&child_done);
            let (task_started_tx, task_started_rx) = mpsc::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let (child_tx, child_rx) = mpsc::channel();
            drop(LanceRuntime::spawn(async move {
                task_started_tx.send(()).unwrap();
                let _ = release_rx.await;
                child_done_flag.store(true, Ordering::SeqCst);
                let _ = child_tx.send(());
            }));
            task_started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("child task did not start");

            // Keep the child's own task outstanding while shutdown starts,
            // then release it once the shutdown thread is about to enter.
            let (shutdown_ready_tx, shutdown_ready_rx) = mpsc::channel();
            let shutdown_thread = std::thread::spawn(move || {
                shutdown_ready_tx.send(()).unwrap();
                shutdown(Duration::from_secs(30));
            });
            shutdown_ready_rx.recv().unwrap();
            release_tx.send(()).unwrap();
            shutdown_thread.join().unwrap();

            let drained = child_rx.recv_timeout(Duration::from_secs(5)).is_ok()
                && child_done.load(Ordering::SeqCst);
            let retired = RUNTIME.load_full().is_none();
            let rebuilt = block_on(async { 40 + 2 }) == 42;
            shutdown(Duration::from_secs(30));
            i32::from(!(drained && retired && rebuilt))
        });
        assert_eq!(
            child_result,
            Some(0),
            "child did not drain its own runtime or waited for parent work"
        );

        release_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parent task was disturbed by the forked child");
        shutdown(Duration::from_secs(5));
    }

    // A guard created before `fork()` belongs to the parent generation. If
    // the child drops that inherited guard, it must decrement the counter it
    // was created against, not the child's fresh counter (a naive reset in
    // the child would underflow here and make shutdown wait forever). The
    // second parent guard stays held across the fork without being dropped
    // in the child, so this also proves the child does not wait for work
    // that can only finish in the parent.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_inherited_guard_does_not_corrupt_child_generation_counter() {
        use std::sync::mpsc;
        use tokio::sync::oneshot;

        let _lock = lock_runtime_state_for_test();
        let _worker_threads = TestWorkerThreads::new(2);
        shutdown(Duration::from_secs(5));

        // This test forks before any runtime exists, so make the hook
        // initialization explicit through the same path the first top-level
        // call uses; otherwise this test only works after a preceding test
        // has installed the process-wide hook.
        ensure_atfork_installed();
        // Stands in for a top-level call that was in progress when the
        // parent forked and is dropped by the child after it starts work.
        let inherited_guard = OutstandingGuard::new();
        // Stays outstanding in the parent for the whole fork, as parent work
        // that no child task can ever complete.
        let parent_guard = OutstandingGuard::new();

        let child_result = run_in_forked_child(move || {
            let child_done = Arc::new(AtomicBool::new(false));
            let child_done_flag = Arc::clone(&child_done);
            let (task_started_tx, task_started_rx) = mpsc::channel();
            let (release_tx, release_rx) = oneshot::channel::<()>();
            let (child_tx, child_rx) = mpsc::channel();
            drop(LanceRuntime::spawn(async move {
                task_started_tx.send(()).unwrap();
                let _ = release_rx.await;
                child_done_flag.store(true, Ordering::SeqCst);
                let _ = child_tx.send(());
            }));
            task_started_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("child task did not start");

            // Drop the inherited guard while the child has its own task
            // outstanding, so a shared or reset counter would visibly
            // perturb the child's accounting.
            drop(inherited_guard);

            let (shutdown_ready_tx, shutdown_ready_rx) = mpsc::channel();
            let shutdown_thread = std::thread::spawn(move || {
                shutdown_ready_tx.send(()).unwrap();
                shutdown(Duration::from_secs(30));
            });
            shutdown_ready_rx.recv().unwrap();
            release_tx.send(()).unwrap();
            shutdown_thread.join().unwrap();

            let drained = child_rx.recv_timeout(Duration::from_secs(5)).is_ok()
                && child_done.load(Ordering::SeqCst);
            let retired = RUNTIME.load_full().is_none();
            i32::from(!(drained && retired))
        });
        assert_eq!(
            child_result,
            Some(0),
            "inherited guard corrupted the child generation counter"
        );

        drop(parent_guard);
        shutdown(Duration::from_secs(5));
    }
}
