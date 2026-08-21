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
//! Runtime>` that we own. A `pthread_atfork` child handler clears the slot
//! (atomic-only — see [`atfork_child`]); the next `spawn` rebuilds the
//! runtime in the child. This mirrors the pattern used in the Lance Python
//! bindings.
//!
//! [`reset_runtime`] lets a long-running host process (there is no
//! `fork()`-based reset on Windows — see that function's docs) publish a
//! fresh runtime generation for *future* callers without disturbing
//! in-flight work: `get_runtime()` returns an owned `Arc` clone, and
//! [`LanceRuntime::spawn`]/[`LanceRuntime::spawn_blocking`] hold their own
//! clone alive for the full lifetime of the spawned task (not just the
//! synchronous call that submits it) — a retired generation's `Runtime`
//! only actually shuts down once every such clone has been dropped, never
//! out from under work that's still using it. `ArcSwapOption` provides this
//! safely without a lock: reclamation of a retired generation is handled by
//! the crate's own lock-free algorithm, so there is no use-after-free
//! window between one thread swapping in a new generation and another
//! thread mid-dereference of the old one.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwapOption;
use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: ArcSwapOption<runtime::Runtime> = ArcSwapOption::const_empty();
static INSTALLING: AtomicBool = AtomicBool::new(false);
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

fn get_runtime() -> Arc<runtime::Runtime> {
    loop {
        if let Some(rt) = RUNTIME.load_full() {
            return rt;
        }
        if !INSTALLING.fetch_or(true, Ordering::SeqCst) {
            break;
        }
        std::thread::yield_now();
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_rt = Arc::new(create_runtime());
    RUNTIME.store(Some(new_rt.clone()));
    INSTALLING.store(false, Ordering::SeqCst);
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
/// thread survives. **Atomic-only** — any lock acquired here (even
/// indirectly) can deadlock forever if a *different*, now-vanished thread
/// held it at the moment of fork; the child inherits the lock's memory
/// state as "held by a thread that no longer exists" and nothing can ever
/// release it. `ArcSwapOption::store`/`AtomicBool::store` never take an OS
/// lock, so they're safe here — unlike, say, a `std::sync::RwLock`.
///
/// We don't drop the inherited runtime here (its worker threads are dead;
/// letting a `Runtime` actually shut down would try to join them and hang)
/// — clearing the slot just makes the next `get_runtime()` call (from the
/// one live thread, in a normal, safe context) build a fresh one and let
/// the retired `Arc` drop wherever its last clone happens to go out of
/// scope, same reclamation path as a normal [`reset_runtime`] call.
#[cfg_attr(windows, allow(dead_code))] // only wired up by install_atfork() on non-Windows
extern "C" fn atfork_child() {
    RUNTIME.store(None);
    INSTALLING.store(false, Ordering::SeqCst);
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
/// running, holds its own `Arc` clone of the *retired* runtime and keeps it
/// alive until that specific call/task finishes — this function never
/// blocks waiting for that, it only publishes the new generation so the
/// *next* caller gets it.
#[pyfunction]
pub fn reset_runtime(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
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
        // Keep our own `Arc` clone alive for the whole wrapper future, not
        // just this synchronous `spawn()` call — otherwise a concurrent
        // `reset_runtime()` could retire (and, once every other clone
        // drops, shut down) the runtime while this task is still running
        // on one of its worker threads, cancelling it out from under us.
        let rt = get_runtime();
        let handle = rt.spawn(fut);
        Box::pin(async move {
            let _rt_keepalive = rt;
            handle.await.map_err(LanceJoinError)
        })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let rt = get_runtime();
        let handle = rt.spawn_blocking(f);
        Box::pin(async move {
            let _rt_keepalive = rt;
            handle.await.map_err(LanceJoinError)
        })
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
