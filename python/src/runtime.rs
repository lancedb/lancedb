// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Fork-safe wrapper around tokio + pyo3-async-runtimes.
//!
//! `pyo3_async_runtimes::tokio` keeps its multi-threaded runtime in a
//! `OnceLock` that can never be replaced.  Tokio's worker threads do not
//! survive `fork()`, so once a child inherits a "frozen" runtime, every
//! `future_into_py` call hangs forever.
//!
//! We sidestep the global with our own [`LanceRuntime`] (a
//! [`pyo3_async_runtimes::generic::Runtime`] impl) backed by a raw
//! `AtomicPtr<Arc<tokio::runtime::Runtime>>` "current generation" slot.
//!
//! Three deliberately separate concerns, each solved with the narrowest
//! mechanism that's actually safe for it:
//!
//! - **Reads** (`get_runtime()`, the hot path, called on every operation)
//!   are lock-free: load the pointer, dereference, `.clone()` the `Arc`
//!   behind it. This is sound because **published pointers are never
//!   freed** — see [`retire`] — so a reader can always safely dereference
//!   whatever it just loaded, no matter what a concurrent writer does.
//! - **Writes** (first install, and [`reset_runtime`]) are serialized by
//!   [`INSTALL_LOCK`], a hand-rolled atomic spinlock (not a
//!   `std::sync::Mutex`) — because [`atfork_child`] needs to be able to
//!   force it back to "unlocked" after `fork()` with a single atomic store,
//!   which an OS mutex's opaque internal state doesn't let you do safely.
//! - **Final disposal** of a retired generation never happens inline on
//!   whichever thread's `Arc` clone happens to be the last one dropped —
//!   that could be one of the very Tokio worker threads the `Runtime` owns,
//!   and Tokio panics if you try to drop a runtime from inside itself. A
//!   dedicated, permanently-idle-otherwise reaper thread (never a Tokio
//!   worker) polls a retired `Arc` until it's the sole owner, then shuts it
//!   down explicitly from there.
//!
//! [`atfork_child`] touches **only** plain atomics: no lock, no allocation,
//! no thread-local access, and (critically) no `Arc`/`ArcSwap` reclamation
//! logic — even `arc_swap`'s own internal bookkeeping does TLS/allocator
//! work unsafe to run in a post-fork child, which is why this module uses a
//! raw `AtomicPtr` instead.

use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: AtomicPtr<Arc<runtime::Runtime>> = AtomicPtr::new(ptr::null_mut());
/// Hand-rolled spinlock (not `std::sync::Mutex`) guarding "build the first
/// runtime" / [`reset_runtime`] against each other. Never touched from
/// [`atfork_child`] itself, but — unlike a `Mutex`, whose internal OS state
/// a fork can leave permanently "owned by a vanished thread" — this is
/// reset by a single atomic store in the handler, so the child's first
/// normal (non-handler) call to acquire it always succeeds.
static INSTALL_LOCK: AtomicBool = AtomicBool::new(false);
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);

fn acquire_install_lock() {
    while INSTALL_LOCK
        .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        std::thread::yield_now();
    }
}

fn release_install_lock() {
    INSTALL_LOCK.store(false, Ordering::Release);
}

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

fn get_runtime() -> Arc<runtime::Runtime> {
    let ptr = RUNTIME.load(Ordering::Acquire);
    if !ptr.is_null() {
        // SAFETY: published pointers are never freed (see `retire`), so
        // dereferencing whatever we just loaded is always valid.
        return unsafe { (*ptr).clone() };
    }
    acquire_install_lock();
    // Someone else may have installed while we were spinning for the lock.
    let ptr = RUNTIME.load(Ordering::Acquire);
    if !ptr.is_null() {
        release_install_lock();
        return unsafe { (*ptr).clone() };
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_rt = Arc::new(create_runtime());
    let boxed = Box::new(new_rt.clone());
    RUNTIME.store(Box::into_raw(boxed), Ordering::Release);
    release_install_lock();
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
/// thread survives. **Atomic-only, nothing else** — two plain stores,
/// deliberately abandoning whatever the parent's generation/lock state was:
///
/// - `RUNTIME.store(null)`: never touches whatever the pointer used to
///   point to. We do **not** try to reclaim, clone through, or otherwise
///   read the retired generation here — any of that (even via `ArcSwap`,
///   which does its own TLS/allocator work internally) can deadlock if a
///   *different*, now-vanished thread held an allocator or bookkeeping lock
///   at the moment of fork.
/// - `INSTALL_LOCK.store(false)`: unconditionally "unlocks" it, regardless
///   of whether some vanished thread held it at fork time — a
///   `std::sync::Mutex` can't be reset this way (its internal state isn't a
///   plain bool we're allowed to poke), which is exactly why this module
///   uses a hand-rolled spinlock instead.
///
/// The next `get_runtime()` call (from the one live thread, in a normal,
/// safe, lock-taking context) builds a fresh runtime; the abandoned
/// generation's memory is simply never touched again by this process (a
/// deliberate, permanent, tiny leak — see [`retire`] for why that's the
/// standing trade-off this whole module makes, not something special-cased
/// for the fork path).
#[cfg_attr(windows, allow(dead_code))] // only wired up by install_atfork() on non-Windows
extern "C" fn atfork_child() {
    RUNTIME.store(ptr::null_mut(), Ordering::SeqCst);
    INSTALL_LOCK.store(false, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Dedicated, permanently-idle-otherwise thread that owns final disposal of
/// retired runtime generations — **never** a Tokio worker thread, so it's
/// always safe for it to actually shut one down. Lazily started on first
/// [`retire`] call.
fn reaper_sender() -> &'static mpsc::Sender<Arc<runtime::Runtime>> {
    static REAPER: OnceLock<mpsc::Sender<Arc<runtime::Runtime>>> = OnceLock::new();
    REAPER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<Arc<runtime::Runtime>>();
        std::thread::Builder::new()
            .name("lancedb-runtime-reaper".to_string())
            .spawn(move || {
                for mut retired in rx {
                    // Poll until we're the sole owner -- any in-flight
                    // `block_on`/`spawn`/`spawn_blocking` caller that took
                    // its own clone before this generation was retired
                    // keeps it alive until that specific call/task
                    // finishes, same as `Arc` always works. Once we're the
                    // last owner, shut it down explicitly and
                    // non-blockingly from *this* thread -- never let an
                    // arbitrary caller's `Arc` drop trigger the runtime's
                    // default (blocking-join) `Drop` on its own worker,
                    // which Tokio forbids and panics on.
                    loop {
                        match Arc::try_unwrap(retired) {
                            Ok(rt) => {
                                rt.shutdown_background();
                                break;
                            }
                            Err(still_shared) => {
                                retired = still_shared;
                                std::thread::sleep(Duration::from_millis(50));
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn lancedb-runtime-reaper thread");
        tx
    })
}

fn retire(rt: Arc<runtime::Runtime>) {
    // The reaper thread never exits (its channel sender is a `'static`
    // singleton with no other owners to drop), so a send failure here
    // would mean the reaper thread itself panicked -- fall back to a
    // normal drop rather than panicking ourselves over it.
    let _ = reaper_sender().send(rt);
}

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
/// and keeps it alive until that specific call/task finishes; the retired
/// runtime's actual shutdown is handled by a dedicated reaper thread once
/// every such clone is gone, never inline on whatever thread happens to
/// drop the last one. Serialized against a concurrent first-install via
/// [`INSTALL_LOCK`] so a build that started before this call can never
/// publish after it returns.
#[pyfunction]
pub fn reset_runtime(py: Python<'_>) -> PyResult<()> {
    py.detach(|| {
        acquire_install_lock();
        let old_ptr = RUNTIME.swap(ptr::null_mut(), Ordering::AcqRel);
        if !old_ptr.is_null() {
            // SAFETY: we never free a published pointer (see module docs)
            // — a concurrent lock-free reader may still be mid-dereference
            // of `old_ptr` right now. We only read through it once more
            // ourselves (to clone the `Arc` for the reaper) and then
            // permanently leak the tiny `Box` wrapper itself; only the
            // `Runtime` payload it pointed to (handed to the reaper as an
            // owned `Arc` clone) ever actually gets freed.
            let retired: Arc<runtime::Runtime> = unsafe { (*old_ptr).clone() };
            retire(retired);
        }
        release_install_lock();
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
        // (or even keep) that wrapper. Dropping this clone at task
        // completion is always safe now regardless of which thread does
        // it — even if it happens to be the last reference — because
        // actual runtime shutdown is handled exclusively by the reaper
        // thread (see `retire`/`reaper_sender`), never by an ordinary
        // `Arc` drop.
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
