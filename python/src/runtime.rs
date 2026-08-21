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
//! by an `Arc<tokio::runtime::Runtime>` that we own. A `pthread_atfork`
//! child handler clears the slot; the next `spawn` rebuilds the runtime in
//! the child. This mirrors the pattern used in the Lance Python bindings.
//!
//! The runtime handle is reference-counted (not a bare pointer) so that
//! [`reset_runtime`] can swap in a fresh runtime for future callers without
//! invalidating a handle an in-flight `block_on`/`spawn` call is still
//! using on another thread — the old runtime is only actually shut down
//! once its last reference is dropped.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: RwLock<Option<Arc<runtime::Runtime>>> = RwLock::new(None);
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

fn get_runtime() -> Arc<runtime::Runtime> {
    if let Some(rt) = RUNTIME.read().expect("runtime lock poisoned").as_ref() {
        return rt.clone();
    }

    let mut guard = RUNTIME.write().expect("runtime lock poisoned");
    // Another thread may have installed a runtime while we waited for the
    // write lock — check again before building a second one.
    if let Some(rt) = guard.as_ref() {
        return rt.clone();
    }

    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_rt = Arc::new(create_runtime());
    *guard = Some(new_rt.clone());
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

/// Runs in async-signal context after `fork()` in the child.  We can only
/// touch atomics/locks here; we deliberately leak the previous runtime
/// (rather than let its `Drop` run) because dropping a tokio `Runtime`
/// would try to join its (now-dead) worker threads and hang.
extern "C" fn atfork_child() {
    if let Ok(mut guard) = RUNTIME.write() {
        if let Some(old) = guard.take() {
            std::mem::forget(old);
        }
    }
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Closes the process's current Tokio runtime (if nothing else is using it)
/// and forces the next `get_runtime()` call (`block_on`/`spawn`/
/// `future_into_py`) to build a fresh one.
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
/// it. Safe to call at any point between operations; if another thread is
/// still mid-`block_on`/`spawn` with its own clone of the runtime handle,
/// that in-flight call keeps its runtime alive until it finishes — this
/// function never blocks waiting for that, it only swaps the slot so the
/// *next* caller gets a fresh runtime.
#[pyfunction]
pub fn reset_runtime(py: Python<'_>) -> PyResult<()> {
    py.allow_threads(|| {
        let mut guard = RUNTIME.write().expect("runtime lock poisoned");
        // Dropping our reference here shuts the runtime down for real
        // (worker threads are alive in this path, unlike the fork case
        // above, so joining them is safe) once it's the last reference —
        // if another thread still holds a clone, the actual shutdown
        // happens on whichever thread drops that last clone instead.
        guard.take();
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
        let handle = get_runtime().spawn(fut);
        Box::pin(async move { handle.await.map_err(LanceJoinError) })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = get_runtime().spawn_blocking(f);
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
