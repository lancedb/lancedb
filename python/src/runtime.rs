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
//! `AtomicPtr<tokio::runtime::Runtime>` "current generation" slot, where the
//! pointer *is* an `Arc`'s own internal pointer (via `Arc::into_raw`/
//! `Arc::from_raw`) rather than a wrapper holding a clone. That distinction
//! matters: a wrapper holding a permanent clone can never be reclaimed (its
//! own clone keeps the strong count above zero forever), whereas treating
//! the slot itself as owning exactly one real strong reference means a
//! retired generation's count genuinely reaches zero once every caller's own
//! clone is gone, and its worker threads actually get shut down instead of
//! silently piling up on every [`reset_runtime`] call.
//!
//! Three deliberately separate concerns, each solved with the narrowest
//! mechanism that's actually safe for it:
//!
//! - **Reads** (`get_runtime()`, the hot path, called on every operation)
//!   register themselves in [`ACTIVE_READERS`] before dereferencing the
//!   slot and deregister immediately after bumping the `Arc`'s strong count
//!   — a brief, allocation-free reader-quiescence window, not a lock. A
//!   writer that wants to reclaim a retired generation waits for this
//!   counter to hit zero first, so it can never free memory a reader is
//!   still mid-dereference of.
//! - **Writes** (first install, and [`reset_runtime`]) are serialized by
//!   [`INSTALL_LOCK`], a hand-rolled atomic spinlock (not a
//!   `std::sync::Mutex`) — because [`atfork_child`] needs to be able to
//!   force it back to "unlocked" after `fork()` with a single atomic store,
//!   which an OS mutex's opaque internal state doesn't let you do safely.
//! - **Final disposal** of a retired generation never happens inline on
//!   whichever thread's `Arc` clone happens to be the last one dropped —
//!   that could be one of the very Tokio worker threads the `Runtime` owns,
//!   and Tokio panics if you try to drop a runtime from inside itself.
//!   [`retire`] spawns a dedicated, one-shot, non-Tokio thread *per retired
//!   generation* that polls until it's the sole owner, then shuts it down
//!   explicitly from there. One thread per retirement (rather than a shared
//!   channel feeding a single long-lived reaper) is deliberate: `reset_runtime`
//!   is a rare, explicit maintenance call, not a hot path, so the modest
//!   cost of spawning a thread per call is cheap compared to the two
//!   failure modes a shared reaper has — a slow-draining generation head-
//!   of-line-blocking every later one behind it in the same channel, and a
//!   shared channel's receiver-owning thread not surviving `fork()`, which
//!   would silently strand anything a forked child sends into it.
//!
//! [`atfork_child`] touches **only** plain atomics: no lock, no allocation,
//! no thread-local access, and no `Arc`/`ArcSwap` reclamation logic — even
//! `arc_swap`'s own internal bookkeeping does TLS/allocator work unsafe to
//! run in a post-fork child, which is why this module uses `Arc::into_raw`/
//! `from_raw` on a raw `AtomicPtr` instead of a crate that manages that for
//! you.

use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: AtomicPtr<runtime::Runtime> = AtomicPtr::new(ptr::null_mut());
/// Count of readers currently between loading [`RUNTIME`] and finishing
/// their `Arc::increment_strong_count`/`Arc::from_raw` dereference of it.
/// [`reset_runtime`] spins until this hits zero before reclaiming a retired
/// pointer, so it can never free memory a reader is still touching.
///
/// All operations on this counter and on [`RUNTIME`] use `SeqCst`: this
/// hand-rolled reader-quiescence scheme relies on a single global total
/// order between "a reader observed the old pointer" and "the writer
/// observed zero active readers" to be sound, which `SeqCst` gives for free
/// without having to hand-prove a weaker ordering correct.
static ACTIVE_READERS: AtomicUsize = AtomicUsize::new(0);
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
        .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        std::thread::yield_now();
    }
}

fn release_install_lock() {
    INSTALL_LOCK.store(false, Ordering::SeqCst);
}

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .expect("Failed to build tokio runtime")
}

/// Dereferences the current [`RUNTIME`] pointer into an owned `Arc` clone,
/// bracketed by the reader-quiescence counter so a concurrent
/// [`reset_runtime`] can never free the pointee out from under us.
///
/// # Safety
/// `ptr` must be a non-null value previously produced by `Arc::into_raw` on
/// an `Arc<runtime::Runtime>` whose reference is still live (i.e. read from
/// [`RUNTIME`] while registered in [`ACTIVE_READERS`]).
unsafe fn read_published(ptr: *mut runtime::Runtime) -> Arc<runtime::Runtime> {
    // SAFETY: caller guarantees `ptr` came from `Arc::into_raw` and is still
    // live for the duration of this call (see function doc).
    unsafe {
        Arc::increment_strong_count(ptr as *const runtime::Runtime);
        Arc::from_raw(ptr as *const runtime::Runtime)
    }
}

fn get_runtime() -> Arc<runtime::Runtime> {
    ACTIVE_READERS.fetch_add(1, Ordering::SeqCst);
    let ptr = RUNTIME.load(Ordering::SeqCst);
    if !ptr.is_null() {
        let rt = unsafe { read_published(ptr) };
        ACTIVE_READERS.fetch_sub(1, Ordering::SeqCst);
        return rt;
    }
    ACTIVE_READERS.fetch_sub(1, Ordering::SeqCst);

    acquire_install_lock();
    // Someone else may have installed while we were spinning for the lock.
    ACTIVE_READERS.fetch_add(1, Ordering::SeqCst);
    let ptr = RUNTIME.load(Ordering::SeqCst);
    if !ptr.is_null() {
        let rt = unsafe { read_published(ptr) };
        ACTIVE_READERS.fetch_sub(1, Ordering::SeqCst);
        release_install_lock();
        return rt;
    }
    ACTIVE_READERS.fetch_sub(1, Ordering::SeqCst);

    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_rt = Arc::new(create_runtime());
    // `Arc::into_raw` consumes one strong reference into the raw pointer —
    // the slot itself now *owns* that reference, rather than holding a
    // separate permanent clone the way a wrapper type would. `new_rt` below
    // still holds this call's own reference to return to the caller.
    let published = Arc::into_raw(new_rt.clone()) as *mut runtime::Runtime;
    RUNTIME.store(published, Ordering::SeqCst);
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
/// thread survives. **Atomic-only, nothing else** — three plain stores,
/// deliberately abandoning whatever the parent's generation/lock/reader
/// state was:
///
/// - `RUNTIME.store(null)`: never touches whatever the pointer used to
///   point to. We do **not** try to reclaim, clone through, or otherwise
///   read the retired generation here — any of that (even via `ArcSwap`,
///   which does its own TLS/allocator work internally) can deadlock if a
///   *different*, now-vanished thread held an allocator or bookkeeping lock
///   at the moment of fork. The abandoned generation's memory is simply
///   never touched again by this process — a deliberate, permanent, tiny
///   leak scoped to one generation per fork, not something a signal-safe
///   handler can avoid.
/// - `ACTIVE_READERS.store(0)`: a *different*, now-vanished thread could
///   have been between `fetch_add` and `fetch_sub` in [`get_runtime`] at
///   fork time, leaving this stuck above zero forever in the child (it can
///   never decrement — that thread doesn't exist here) and permanently
///   wedging any future [`reset_runtime`] call in the child. Forcing it
///   back to zero is safe: we've already nulled `RUNTIME` above, so no
///   reader in the child can be mid-dereference of anything.
/// - `INSTALL_LOCK.store(false)`: unconditionally "unlocks" it, regardless
///   of whether some vanished thread held it at fork time — a
///   `std::sync::Mutex` can't be reset this way (its internal state isn't a
///   plain bool we're allowed to poke), which is exactly why this module
///   uses a hand-rolled spinlock instead.
///
/// The next `get_runtime()` call (from the one live thread, in a normal,
/// safe, lock-taking context) builds a fresh runtime.
#[cfg_attr(windows, allow(dead_code))] // only wired up by install_atfork() on non-Windows
extern "C" fn atfork_child() {
    RUNTIME.store(ptr::null_mut(), Ordering::SeqCst);
    ACTIVE_READERS.store(0, Ordering::SeqCst);
    INSTALL_LOCK.store(false, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Spawns a dedicated, one-shot, non-Tokio thread that owns final disposal
/// of a single retired runtime generation — see the module docs for why
/// this is per-generation rather than a shared long-lived reaper.
fn retire(rt: Arc<runtime::Runtime>) {
    // Box the Arc and hand the closure only a raw pointer to it: a raw
    // pointer has no `Drop` impl of its own, so if `spawn` fails and the
    // std library drops the still-unrun closure, nothing happens to the
    // boxed `Arc` -- it is simply never reconstructed, not dropped. That
    // is exactly the fallback we want (see below) rather than letting the
    // normal move-into-closure ownership semantics drop the `Arc` --
    // possibly the last reference -- on whatever thread called `retire()`.
    let boxed: *mut Arc<runtime::Runtime> = Box::into_raw(Box::new(rt));
    let spawned = std::thread::Builder::new()
        .name("lancedb-runtime-reaper".to_string())
        .spawn(move || {
            // SAFETY: `boxed` was produced by `Box::into_raw` just above,
            // and this closure only ever runs if `spawn` succeeded, which
            // is the sole path that reclaims it.
            let mut retired = *unsafe { Box::from_raw(boxed) };
            // Poll until we're the sole owner -- any in-flight
            // `block_on`/`spawn`/`spawn_blocking` caller that took its own
            // clone before this generation was retired keeps it alive
            // until that specific call/task finishes, same as `Arc` always
            // works. Once we're the last owner, shut it down explicitly
            // and non-blockingly from *this* thread -- never let an
            // arbitrary caller's `Arc` drop trigger the runtime's default
            // (blocking-join) `Drop` on its own worker, which Tokio
            // forbids and panics on.
            loop {
                match Arc::try_unwrap(retired) {
                    Ok(rt) => {
                        rt.shutdown_background();
                        return;
                    }
                    Err(still_shared) => {
                        retired = still_shared;
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }
        });
    if spawned.is_err() {
        // Thread creation failed (extreme resource exhaustion). The closure
        // above never ran, so `boxed` was never reclaimed -- explicitly
        // reclaim and `mem::forget` it here rather than relying on that
        // implicitly. Preserving a non-worker retirement owner is the
        // point of this whole module; deliberately leaking the retired
        // runtime (never shut down, never freed -- the same trade-off
        // `atfork_child` already makes for the identical reason) is safer
        // than letting it drop on an arbitrary caller's thread, which could
        // be one of that very runtime's own workers and panic.
        //
        // SAFETY: `spawned` is `Err`, so the closure was dropped unrun and
        // never reclaimed `boxed` -- we still exclusively own it here.
        std::mem::forget(unsafe { Box::from_raw(boxed) });
    }
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
/// runtime's actual shutdown is handled by a dedicated one-shot thread (see
/// [`retire`]) once every such clone is gone, never inline on whatever
/// thread happens to drop the last one. Serialized against a concurrent
/// first-install via [`INSTALL_LOCK`] so a build that started before this
/// call can never publish after it returns; the pointer swap itself waits
/// for [`ACTIVE_READERS`] to quiesce before reclaiming the retired pointer,
/// so a concurrent [`get_runtime`] reader can never be left dereferencing
/// freed memory.
#[pyfunction]
pub fn reset_runtime(py: Python<'_>) -> PyResult<()> {
    py.detach(reset_runtime_impl);
    Ok(())
}

/// The actual reclaim logic behind [`reset_runtime`], factored out so it
/// takes no `Python` token: this crate builds with the `extension-module`
/// feature, which cannot embed/initialize an interpreter, so `#[test]`s
/// (which run as a standalone binary, not loaded by a live Python process)
/// can exercise this directly without needing a GIL token they have no way
/// to obtain.
fn reset_runtime_impl() {
    acquire_install_lock();
    let old_ptr = RUNTIME.swap(ptr::null_mut(), Ordering::SeqCst);
    release_install_lock();
    if !old_ptr.is_null() {
        // Wait for any reader that loaded `old_ptr` before the swap above
        // to finish its brief dereference window -- readers only ever hold
        // this open for the few instructions between the load and their
        // own `Arc::increment_strong_count`, so this drains almost
        // immediately in practice.
        while ACTIVE_READERS.load(Ordering::SeqCst) != 0 {
            std::thread::yield_now();
        }
        // SAFETY: no reader can be dereferencing `old_ptr` any more -- it
        // was swapped out of `RUNTIME` above, and we've just observed zero
        // active readers, so anyone who might have read the old value has
        // already finished with it. Reconstructing the `Arc` here (rather
        // than cloning through a still-live pointer) consumes exactly the
        // one strong reference the slot itself owned, so this generation's
        // count genuinely reaches zero once every caller-held clone is
        // gone.
        let retired: Arc<runtime::Runtime> =
            unsafe { Arc::from_raw(old_ptr as *const runtime::Runtime) };
        retire(retired);
    }
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
        // actual runtime shutdown is handled exclusively by a dedicated
        // reaper thread (see `retire`), never by an ordinary `Arc` drop.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    /// Regression for the exact defect the gatekeeper found in the prior
    /// revision: a `Box<Arc<Runtime>>`-wrapper design that permanently held
    /// one extra strong reference meant `Arc::try_unwrap` in the reaper
    /// could never succeed, so retired runtimes (and their worker threads)
    /// leaked forever instead of actually shutting down. This proves
    /// `reset_runtime`'s reclaim path genuinely drives a retired
    /// generation's shutdown, not just a successful pointer replacement.
    #[test]
    fn reset_runtime_actually_shuts_down_the_retired_generation() {
        // Install generation 1, capture a handle to prove it's alive.
        let gen1 = get_runtime();
        gen1.spawn(async {});

        reset_runtime_impl();

        // gen1's only remaining owner is this test's `gen1` binding plus
        // whatever `retire()` handed off internally; once we drop our own
        // clone, the retired generation should reach a strong count of
        // exactly what the reaper thread holds, and the reaper should be
        // able to unwrap and shut it down.
        let (tx, rx) = mpsc::channel();
        let weak = Arc::downgrade(&gen1);
        drop(gen1);
        std::thread::spawn(move || {
            for _ in 0..100 {
                if weak.strong_count() == 0 {
                    let _ = tx.send(true);
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = tx.send(false);
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(10)).unwrap(),
            "retired runtime generation was never fully reclaimed \
             (reaper failed to reach sole ownership within 5s)"
        );

        // A fresh call after reset must build (and be able to use) a brand
        // new generation, distinct from the retired one.
        let gen2 = get_runtime();
        gen2.block_on(async {});
    }

    #[test]
    fn concurrent_readers_never_observe_a_freed_pointer() {
        // Hammer get_runtime() from many threads while repeatedly calling
        // reset_runtime() from the main thread -- under the old
        // Box<Arc<Runtime>>-wrapper design this was safe by construction
        // (nothing was ever freed); this proves the new reclaim-on-quiesce
        // design is equally safe despite now actually freeing memory.
        let stop = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let rt = get_runtime();
                        rt.block_on(async {});
                    }
                })
            })
            .collect();

        for _ in 0..20 {
            reset_runtime_impl();
            std::thread::sleep(Duration::from_millis(10));
        }

        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
    }
}
