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
//! by an [`arc_swap::ArcSwapOption`] slot holding an `Arc<tokio::runtime::
//! Runtime>` that we own.
//!
//! Two independent synchronization concerns, kept deliberately separate:
//!
//! - **Reads** (`get_runtime()`, called on every operation) are lock-free via
//!   `ArcSwapOption::load_full`.
//! - **Writes** (first install, and [`reset_runtime`]) are serialized by
//!   [`INSTALL_LOCK`], a plain `Mutex` — safe here because neither is ever
//!   called from the `pthread_atfork` child handler. Sharing one lock
//!   between "build the first runtime" and "retire the current one" is what
//!   closes the race where an install that started *before* a `reset_runtime`
//!   call could otherwise publish its (now stale) result *after* the reset
//!   returns.
//!
//! [`atfork_child`] (the child handler) touches neither the read path nor
//! `INSTALL_LOCK` — it only does an `ArcSwapOption::swap` (which, unlike
//! `store`, hands ownership of the retired value back to the caller instead
//! of dropping it inline) followed by `mem::forget` on the result, so it
//! never runs a `tokio::Runtime`'s `Drop` (which would try to join the
//! child's now-dead worker threads and hang) from a context where only the
//! forking thread survives.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: ArcSwapOption<runtime::Runtime> = ArcSwapOption::const_empty();
/// Serializes "build the first runtime" against [`reset_runtime`]. Never
/// touched by [`atfork_child`] — see module docs.
static INSTALL_LOCK: Mutex<()> = Mutex::new(());
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

fn get_runtime() -> Arc<runtime::Runtime> {
    if let Some(rt) = RUNTIME.load_full() {
        return rt;
    }
    let _guard = INSTALL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Someone else may have installed (or reset, right after installing)
    // while we waited for the lock — check again under it.
    if let Some(rt) = RUNTIME.load_full() {
        return rt;
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_rt = Arc::new(create_runtime());
    RUNTIME.store(Some(new_rt.clone()));
    new_rt
}

/// Block the current thread on a future using the shared runtime.
///
/// For sync `#[pyfunction]`s that need to drive an async operation (e.g.
/// building a namespace client). Must not be called from within the runtime's
/// own worker threads.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    get_runtime().block_on(fut)
}

/// Runs after `fork()` in the child, in a context where only the forking
/// thread survives. **Atomic-only** — no lock, and no drop of a `Runtime`.
///
/// Two separate hazards this must avoid, both demonstrated in review:
///
/// 1. Acquiring `INSTALL_LOCK` (or any lock) here can deadlock forever if a
///    *different*, now-vanished thread held it at the moment of fork — the
///    child inherits the lock's memory state as "held by a thread that no
///    longer exists", and nothing can ever release it.
/// 2. `ArcSwapOption::store(None)` is implemented as `drop(self.swap(val))` —
///    if the slot holds the last `Arc`, that `drop` runs a `tokio::Runtime`'s
///    `Drop` impl synchronously, which tries to join its (now-dead) worker
///    threads and hangs just as badly as a stuck lock would.
///
/// `swap` alone (unlike `store`) hands the retired value back to us instead
/// of dropping it, so we can `mem::forget` it — the runtime's *memory* is
/// deliberately leaked (never joined, never freed), same trade-off the
/// pre-mitigation code already made here. The next `get_runtime()` call
/// (from the one live thread, in a normal, safe, lock-taking context) builds
/// a fresh runtime.
#[cfg_attr(windows, allow(dead_code))] // only wired up by install_atfork() on non-Windows
extern "C" fn atfork_child() {
    let retired = RUNTIME.swap(None);
    if let Some(rt) = retired {
        std::mem::forget(rt);
    }
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Publishes a fresh Tokio runtime generation for *future* `get_runtime()`
/// callers (`block_on`/`spawn`/`future_into_py`), without disturbing
/// in-flight work on the current one.
///
/// This exists as an explicit, opt-in mitigation for a Windows-specific
/// issue: the fork-reset hook above only fires on POSIX (there is no
/// `fork()` on Windows, so [`install_atfork`] is a no-op there), meaning the
/// single process-lifetime runtime — and whatever IOCP-backed reactor
/// `mio`/`tokio` set up under it — never gets recycled on its own in a
/// long-running Windows process. Host applications that keep a single
/// process alive across many thousands of `connect_async`/table operations
/// (e.g. a test suite, or an agent harness) can call this periodically
/// (e.g. every N operations) to recycle the runtime and its worker threads.
///
/// This does **not** fix the underlying resource leak — that is tracked
/// upstream in `mio` (<https://github.com/tokio-rs/mio/issues/1944>), not
/// under LanceDB's control — it only gives callers a way to recycle around
/// it. Safe to call at any point between operations, from any thread: a
/// call already mid-`block_on`, or a `spawn`/`spawn_blocking`ed task still
/// running, holds its own `Arc` clone of the *retired* runtime (captured
/// inside the actual spawned future/closure — see [`LanceRuntime::spawn`])
/// and keeps it alive until that specific call/task finishes — this
/// function never blocks waiting for that, it only publishes the new
/// generation so the *next* caller gets it. Serialized against a
/// concurrent first-install via [`INSTALL_LOCK`] so a build that started
/// before this call can never publish after it returns.
#[pyfunction]
pub fn reset_runtime(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
        let _guard = INSTALL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        RUNTIME.store(None);
    });
    Ok(())
}

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

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // The `Arc` keepalive must live inside the future *actually handed
        // to the runtime's scheduler*, not just inside the `JoinHandle`
        // wrapper we return: `pyo3_async_runtimes` does not always await
        // (or even keep) that wrapper, and once it's dropped, anything it
        // alone was holding drops with it — including a runtime a
        // concurrent `reset_runtime()` has already retired, cancelling the
        // still-running task. Wrapping `fut` itself ties the clone's
        // lifetime to the submitted task's real execution, which Tokio
        // keeps alive regardless of what happens to the `JoinHandle`.
        let rt = get_runtime();
        let task_rt = rt.clone();
        let handle = rt.spawn(async move {
            let _rt_keepalive = task_rt;
            fut.await
        });
        Box::pin(async move { handle.await.map_err(LanceJoinError) })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let rt = get_runtime();
        let task_rt = rt.clone();
        let handle = rt.spawn_blocking(move || {
            let _rt_keepalive = task_rt;
            f()
        });
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
