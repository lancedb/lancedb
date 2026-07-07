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

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

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
