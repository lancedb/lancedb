// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Fork-safe wrapper around tokio + pyo3-async-runtimes.
//!
//! `pyo3_async_runtimes::tokio` keeps its multi-threaded runtime in a
//! `OnceLock` that can never be replaced.  Tokio's worker threads do not
//! survive `fork()`, so once a child inherits a "frozen" runtime, every
//! `future_into_py` call hangs forever.
//!
//! We sidestep the global by routing every future through our own
//! [`LanceRuntime`] (a [`pyo3_async_runtimes::generic::Runtime`] impl) backed
//! by an [`ArcSwapOption`] holding the tokio runtime we own.  A
//! `pthread_atfork` child handler clears the slot without dropping what it
//! finds there (see `atfork_child`); the next call rebuilds the runtime in
//! the child.  This mirrors the pattern used in the Lance Python bindings.
//!
//! Normal (non-fork) process exit has its own gap: nothing tells the runtime
//! to shut down, so its worker threads keep running, uncoordinated with the
//! interpreter, right up until the process ends. If one of them is mid-task
//! exactly as `Py_Finalize` starts tearing down interpreter state, it can
//! panic on state that's already gone -- and since that happens on a
//! background thread with no PyO3-wrapped call frame to catch it, Rust
//! aborts the whole process rather than failing that one call. [`shutdown`],
//! registered as a Python `atexit` callback, closes that gap by giving the
//! runtime a coordinated, bounded exit while the interpreter is still fully
//! valid.
//!
//! Every reader holds its own `Arc` clone for as long as it is using the
//! runtime (see `get_runtime`), rather than a bare reference into the slot.
//! `shutdown` only ever reclaims (drops) the runtime once it can prove it
//! holds the only remaining reference; if some other clone is still
//! outstanding when its bound elapses, it abandons the runtime instead --
//! the same trade `atfork_child` already makes -- rather than ever risk
//! freeing memory a live caller might still be using.
//!
//! That alone is not enough, for two compounding reasons.
//!
//! First, `future_into_py` does not make one `get_runtime()`-mediated call
//! per logical operation -- it spawns a task that, once running, spawns a
//! second one to do the real work and awaits its `JoinHandle`. If the two
//! nested calls independently re-resolved "the current runtime", a reclaim
//! landing between them could bind the outer and inner task to two
//! *different* instances; tearing down the outer task's runtime while it
//! awaits the inner one then leaves it parked forever. `spawn`/
//! `spawn_blocking` below close that gap with `Handle::try_current`: a call
//! already running on one of our runtime's worker threads is pinned to that
//! same instance regardless of what the global slot holds, so only the
//! first, outermost call of a chain (from a thread outside any runtime)
//! ever consults it.
//!
//! Second, and more fundamentally: `Runtime::shutdown_timeout` does not
//! give spawned (non-blocking) tasks a bounded grace period at all -- per
//! its own docs, a task "keeps running until it yields, then is dropped".
//! An outermost call that only held its `Arc` for the instant it submitted
//! the task (as `get_runtime()`'s doc once assumed of every caller) let
//! `strong_count` fall back to baseline immediately, long before the task
//! itself finished -- so `shutdown` could, and empirically did, reclaim the
//! runtime while a top-level task was still in flight, silently abandoning
//! it and leaving whatever Python future it was going to resolve unresolved
//! forever. `spawn`/`spawn_blocking`'s outermost branch now moves an `Arc`
//! clone *into* the task itself, so `strong_count` stays elevated for the
//! task's entire lifetime, not just the submission call.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: ArcSwapOption<runtime::Runtime> = ArcSwapOption::const_empty();
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

/// Get a live, owned handle to the shared runtime.
///
/// Returns an owned `Arc` rather than a bare reference so the runtime cannot
/// be freed out from under a caller still using it: as long as any clone is
/// held, `shutdown` will not reclaim the runtime it points to. Callers below
/// rely on Rust's own temporary-drop timing to hold the returned `Arc` for
/// the duration of one `block_on`, `spawn`, or `spawn_blocking` call --
/// exactly as long as the runtime is actually in use, not just to obtain it.
fn get_runtime() -> Arc<runtime::Runtime> {
    loop {
        if let Some(existing) = RUNTIME.load_full() {
            return existing;
        }
        if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
            install_atfork();
        }
        // Built optimistically, outside any lock: on the rare race where two
        // threads both find the slot empty, one candidate wins the
        // compare-and-swap below and the other is simply dropped here,
        // tearing down its own (never shared, never used) worker pool the
        // ordinary way.
        let candidate = Arc::new(create_runtime());
        let previous =
            RUNTIME.compare_and_swap(&None::<Arc<runtime::Runtime>>, Some(Arc::clone(&candidate)));
        if previous.is_none() {
            return candidate;
        }
        // Someone else's candidate won; go around and load it.
    }
}

/// Block the current thread on a future using the shared runtime.
///
/// For sync `#[pyfunction]`s that need to drive an async operation (e.g.
/// building a namespace client). Must not be called from within the runtime's
/// own worker threads.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    get_runtime().block_on(fut)
}

/// Gracefully quiesce the shared runtime, meant to run at normal process exit.
///
/// Polls (bounded by `timeout`) for every other outstanding `Arc` clone --
/// each held by a caller still actually using the runtime, see
/// `get_runtime` -- to be dropped, and only once none remain does it remove
/// the runtime from the slot and reclaim it, so reclaiming can never free
/// memory anyone else might still touch. If the bound elapses first, it
/// leaves the runtime exactly where it is rather than forcing the issue:
/// the process is exiting either way, so an unreclaimed runtime here costs
/// nothing a crash would cost more.
///
/// Deliberately does not empty the slot up front and reclaim once ownership
/// clears, the way a first version of this function did: `get_runtime()`
/// treats an empty slot as "nothing built yet" and responds by building a
/// whole new multi-threaded runtime, worker pool included. Emptying the slot
/// before we are actually ready to consume what was in it means every other
/// concurrent caller sees that empty slot too, and a caller looping tightly
/// (a busy poller, say) would rebuild a brand new runtime on every single
/// call for as long as the slot stayed empty -- not a crash, but a
/// self-inflicted thundering herd that starves real work for the same
/// bounded window this function is supposed to just wait out quietly.
///
/// `shutdown_timeout` rather than a bare `drop`, once exclusive ownership is
/// proven: dropping a tokio `Runtime` waits (in the worst case indefinitely)
/// for its worker threads to join, whereas `shutdown_timeout` gives real
/// in-flight work -- a connection pool's keep-alive, a graceful close -- a
/// bounded chance to finish first, then forcibly ends whatever has not.
///
/// Safe to call even while operations are still in flight on the runtime
/// being reclaimed: `spawn`/`spawn_blocking` below pin every nested call
/// spawned from an already-running task to that same task's runtime (via
/// `Handle::try_current`), so reclaiming this instance out from under a
/// task that outlives the exclusivity check can, at worst, make
/// `shutdown_timeout` leak that task's worker threads to finish in the
/// background -- it can never split one logical operation across two
/// different runtime instances.
pub fn shutdown(timeout: Duration) {
    let Some(current) = RUNTIME.load_full() else {
        return;
    };
    let deadline = Instant::now() + timeout;
    loop {
        if Arc::strong_count(&current) <= 2 {
            RUNTIME.store(None);
            if let Ok(runtime) = Arc::try_unwrap(current) {
                runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
            }
            return;
        }
        if Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Runs in async-signal context after `fork()` in the child. We can only
/// touch the atomic slot here; we deliberately do not drop whatever runtime
/// we find, because dropping a tokio `Runtime` would try to join its
/// (now-dead) worker threads and hang.
extern "C" fn atfork_child() {
    if let Some(orphaned) = RUNTIME.swap(None) {
        std::mem::forget(orphaned);
    }
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
    /// once it starts running, spawns a second one to do the real work (and
    /// spawn_blocking calls beyond that to hand the result back to Python).
    /// If each of those calls re-resolved "the current runtime" independently
    /// via `get_runtime()`, a reclaim landing between them could bind the
    /// outer and inner tasks to two different runtime instances -- and if
    /// the outer task's own runtime is the one torn down while it awaits the
    /// inner task's `JoinHandle`, it never resumes. `Handle::try_current`
    /// sidesteps the global slot entirely whenever we're already running
    /// inside a runtime, pinning nested spawns to that same instance; only a
    /// call from a genuine outside thread (the first, outermost spawn of a
    /// call chain) falls through to `get_runtime()`.
    ///
    /// That call also has to hold its `Arc` for as long as the task it
    /// starts is actually running, not just for the moment it submits it:
    /// `Runtime::shutdown_timeout` does not wait for spawned (non-blocking)
    /// tasks to finish, it lets each run until it next yields and then
    /// *drops* it -- so `shutdown` reclaiming the runtime while a top-level
    /// task is still in flight would silently abandon it mid-await, and
    /// whatever Python future it was going to resolve would never resolve.
    /// Moving a clone into the task itself keeps `strong_count` elevated for
    /// the task's whole lifetime, so `shutdown`'s exclusivity check (see
    /// `shutdown`) does not consider the runtime free until every top-level
    /// task genuinely has completed or been abandoned by some earlier,
    /// already-accounted-for reclaim.
    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn(fut),
            Err(_) => {
                let rt = get_runtime();
                let rt_for_task = Arc::clone(&rt);
                rt.spawn(async move {
                    let _rt = rt_for_task;
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
                let rt = get_runtime();
                let rt_for_task = Arc::clone(&rt);
                rt.spawn_blocking(move || {
                    let _rt = rt_for_task;
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

    // Before an outermost `spawn`/`spawn_blocking` call held its `Arc` for
    // the task's whole lifetime, `strong_count` fell back to baseline as
    // soon as the task was merely *submitted* -- long before it actually
    // finished running. `shutdown_timeout` gives spawned tasks no grace
    // period of its own (it lets one run until it next yields, then drops
    // it), so a concurrent `shutdown()` could, and empirically did, reclaim
    // the runtime out from under a task still in flight, abandoning it
    // silently. Yielding a few times keeps this task genuinely in flight
    // (not yet started, and not yet finished) when `shutdown` runs its
    // exclusivity check immediately after.
    #[test]
    #[allow(unused_must_use)] // fire-and-forget spawn, same as future_into_py itself
    fn test_top_level_task_survives_concurrent_shutdown_reclaim() {
        use std::sync::mpsc;

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
}
