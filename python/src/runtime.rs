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
//! by an [`AtomicPtr`] to a tokio runtime that we own.  A `pthread_atfork`
//! child handler nulls the pointer; the next `spawn` rebuilds the runtime in
//! the child.  This mirrors the pattern used in the Lance Python bindings.
//!
//! Normal (non-fork) process exit has no equivalent handling: nothing ever
//! tells this runtime to shut down, so its worker threads are simply still
//! running, uncoordinated with the interpreter, right up until the process
//! ends. If one of them is mid-task exactly as `Py_Finalize` starts tearing
//! down interpreter state, it can panic on state that's already gone -- and
//! since that happens on a background thread with no PyO3-wrapped call frame
//! to catch it (or, worse, while another unwind is already in progress),
//! Rust aborts the whole process rather than failing that one call. See
//! [`shutdown`], registered as a Python `atexit` callback, which closes that
//! gap by giving the runtime a coordinated, bounded exit while the
//! interpreter is still fully valid.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::time::Duration;

use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: AtomicPtr<runtime::Runtime> = AtomicPtr::new(std::ptr::null_mut());
static RUNTIME_INSTALLING: AtomicBool = AtomicBool::new(false);
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

fn get_runtime() -> &'static runtime::Runtime {
    loop {
        let ptr = RUNTIME.load(Ordering::SeqCst);
        if !ptr.is_null() {
            return unsafe { &*ptr };
        }
        if !RUNTIME_INSTALLING.fetch_or(true, Ordering::SeqCst) {
            break;
        }
        std::thread::yield_now();
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_ptr = Box::into_raw(Box::new(create_runtime()));
    RUNTIME.store(new_ptr, Ordering::SeqCst);
    unsafe { &*new_ptr }
}

/// Block the current thread on a future using the shared runtime.
///
/// For sync `#[pyfunction]`s that need to drive an async operation (e.g.
/// building a namespace client). Must not be called from within the runtime's
/// own worker threads.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    get_runtime().block_on(fut)
}

/// Gracefully quiesce the owned runtime, meant to run at normal process exit.
///
/// Takes the runtime through the same atomic slot `get_runtime()` uses, so a
/// caller that races with this either gets the runtime before it is swapped
/// out, or transparently builds a fresh one after (safe, if pointless, this
/// late in the program's life) -- never a dangling reference.
///
/// `shutdown_timeout` rather than a bare `drop`: dropping a tokio `Runtime`
/// waits (in the worst case indefinitely) for its worker threads to join,
/// which is exactly what the fork handler above avoids by leaking instead.
/// A bounded timeout gives genuinely in-flight work -- a connection pool's
/// keep-alive, a graceful close -- a real chance to finish, then forcibly
/// abandons whatever has not, so this can never hang process exit.
///
/// Must reset `RUNTIME_INSTALLING` alongside `RUNTIME`, exactly as
/// `atfork_child` does: that flag only ever transitions false -> true, for
/// whichever thread wins the race to build the current runtime, and nothing
/// else ever clears it back to false again. `get_runtime()` relies on it
/// being false precisely when `RUNTIME` is null; leaving it true after
/// nulling `RUNTIME` here would strand every later call in its wait loop
/// forever, since no thread could ever again win that false -> true edge to
/// build a replacement.
pub fn shutdown(timeout: Duration) {
    let ptr = RUNTIME.swap(std::ptr::null_mut(), Ordering::SeqCst);
    if !ptr.is_null() {
        let runtime = unsafe { Box::from_raw(ptr) };
        runtime.shutdown_timeout(timeout);
    }
    RUNTIME_INSTALLING.store(false, Ordering::SeqCst);
}

/// Runs in async-signal context after `fork()` in the child.  We can only
/// touch atomics here; we deliberately leak the previous runtime because
/// dropping a tokio `Runtime` would try to join its (now-dead) worker
/// threads and hang.
extern "C" fn atfork_child() {
    RUNTIME.store(std::ptr::null_mut(), Ordering::SeqCst);
    RUNTIME_INSTALLING.store(false, Ordering::SeqCst);
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

#[cfg(test)]
mod tests {
    use super::*;

    // One test, not several: RUNTIME/RUNTIME_INSTALLING are process-wide
    // statics, and Rust runs tests in parallel by default, so separate test
    // functions touching them would race each other. Exercising the whole
    // create -> shutdown -> lazily-rebuild -> shutdown-again sequence in one
    // function keeps it self-contained instead of pulling in a
    // test-serialization dependency for a single file.
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
}
