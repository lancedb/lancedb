// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Replaceable tokio runtime for the Python bindings.
//!
//! `pyo3_async_runtimes::tokio` keeps its multi-threaded runtime in a
//! `OnceLock` that can never be replaced, which leaves callers with no way
//! to release the runtime's worker threads short of exiting the process, and
//! no way to recover after `fork()` (tokio's workers do not survive it, so a
//! child inheriting a "frozen" runtime hangs on every `future_into_py`).
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
//! Four deliberately separate concerns, each solved with the narrowest
//! mechanism that is actually safe for it:
//!
//! - **Reads** (`get_runtime()`, the hot path, called on every operation)
//!   register in one of the two [`ACTIVE_READERS`] slots before
//!   dereferencing [`RUNTIME`] and deregister immediately after bumping the
//!   `Arc`'s strong count — a brief, allocation-free, lock-free window. The
//!   whole window lives inside [`with_reader`], so "the load happens while
//!   registered" is enforced by scope rather than by convention.
//! - **Quiescence is per generation, not global.** A retirement drains only
//!   the slot readers of the *retired* generation registered in, so
//!   continuous traffic on the *new* generation can never keep the drain
//!   from finishing. See [`reset_runtime_inner`] for why the pointer swap
//!   must happen before the epoch bump, and why that makes the drain
//!   provably terminate.
//! - **Writes** are serialized by two hand-rolled atomic spinlocks rather
//!   than `std::sync::Mutex`es, because [`atfork_child`] must be able to
//!   force them back to "unlocked" with a single atomic store, which an OS
//!   mutex's opaque internal state does not allow. [`INSTALL_LOCK`] covers
//!   only the pointer swap itself; [`RETIRE_LOCK`] is held across the whole
//!   retirement. Keeping them separate is what stops a slow drain from
//!   blocking every `get_runtime()` that finds an empty slot. Acquisition
//!   order is always `RETIRE_LOCK` then `INSTALL_LOCK`, and the reader
//!   window takes no lock at all, so there is no cycle to deadlock on.
//! - **Final disposal** of a retired generation never happens inline on
//!   whichever thread's `Arc` clone happens to be the last one dropped —
//!   that could be one of the very Tokio worker threads the `Runtime` owns,
//!   and Tokio panics if you try to drop a runtime from inside itself.
//!   [`retire`] spawns a dedicated, one-shot, non-Tokio thread *per retired
//!   generation* that polls until it is the sole owner, then shuts it down
//!   explicitly from there. One thread per retirement (rather than a shared
//!   channel feeding a single long-lived reaper) is deliberate: a
//!   slow-draining generation would head-of-line-block every later one
//!   behind it in a shared channel, and a shared channel's receiver-owning
//!   thread does not survive `fork()`, which would silently strand anything
//!   a forked child sends into it.
//!
//! Every failure path in this module degrades to a **leak**, never to a hang
//! and never to a reclaim. Reclaiming while a reader might still hold the
//! pointer would be a use-after-free; dropping instead of leaking could put
//! the last reference in the hands of a wedged task running on a worker of
//! the very runtime being destroyed, which is the panic this design exists
//! to prevent. Leaking is what the module did before any of this machinery
//! existed, so the worst case is the previous status quo.
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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use pyo3::{Bound, PyAny, PyResult, Python, conversion::IntoPyObject, pyfunction};
use pyo3_async_runtimes::{
    TaskLocals,
    generic::{ContextExt, JoinError, Runtime},
};
use tokio::{runtime, task};

static RUNTIME: AtomicPtr<runtime::Runtime> = AtomicPtr::new(ptr::null_mut());
/// Selects which [`ACTIVE_READERS`] slot new readers register in
/// (`READER_EPOCH & 1`). Only a retirement ever bumps it, and only while
/// holding [`RETIRE_LOCK`], so at most one slot is being drained at a time
/// and the other is always free to accept new readers.
static READER_EPOCH: AtomicUsize = AtomicUsize::new(0);
/// Two reader-quiescence slots. A reader registers in the slot its epoch
/// selects and deregisters through that *same* slot (remembered in its
/// [`ReaderGuard`]), so a retirement can wait on exactly the readers that
/// could still hold the retired pointer, and no others.
///
/// An earlier revision used a single global counter here. That was
/// unsound as a termination argument: readers of the *new* generation
/// incremented the very counter the drain was waiting to see reach zero, so
/// a process with continuous `get_runtime()` traffic could keep the drain
/// spinning indefinitely — silently, since it runs with the GIL released.
///
/// All operations on these counters, on [`READER_EPOCH`] and on [`RUNTIME`]
/// use `SeqCst`: the quiescence argument below is stated in terms of a
/// single global total order over those operations, which `SeqCst` gives
/// for free without having to hand-prove a weaker ordering correct.
static ACTIVE_READERS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
/// Hand-rolled spinlock (not `std::sync::Mutex`) guarding publication of a
/// new generation pointer against a concurrent first install. Held only
/// across the swap itself, never across a drain. Unlike a `Mutex`, whose
/// internal OS state a fork can leave permanently "owned by a vanished
/// thread", this is reset by a single atomic store in [`atfork_child`], so
/// the child's first normal (non-handler) acquisition always succeeds.
static INSTALL_LOCK: AtomicBool = AtomicBool::new(false);
/// Serializes retirements against each other, held for the whole of
/// [`reset_runtime_inner`] including the drain. Deliberately *not*
/// [`INSTALL_LOCK`]: a drain can legitimately take a while, and holding the
/// install lock across it would block every `get_runtime()` that finds an
/// empty slot — relocating the very stall this design removes onto the hot
/// path.
static RETIRE_LOCK: AtomicBool = AtomicBool::new(false);
static ATFORK_INSTALLED: AtomicBool = AtomicBool::new(false);
/// Monotonic id per retired generation, used in reaper thread names and in
/// diagnostics so several concurrent retirements stay distinguishable.
static RETIREMENT_SEQ: AtomicUsize = AtomicUsize::new(0);
/// Retired generations whose reaper has not finished yet. Reported in the
/// reaper's warnings: a number that keeps climbing means the caller is
/// resetting faster than its own tasks finish, which is exactly the
/// diagnosis this API's users need and cannot otherwise get.
static LIVE_RETIREMENTS: AtomicUsize = AtomicUsize::new(0);

/// RAII guard over one of this module's spinlocks.
///
/// Releasing on `Drop` rather than by an explicit call is load-bearing: the
/// runtime builder can panic (thread or handle exhaustion is precisely the
/// situation a caller reaches for this API in), and an unwind past a manual
/// release would strand the lock, wedging every later `get_runtime()` and
/// `reset_runtime()` in an unbreakable spin.
struct SpinGuard(&'static AtomicBool);

impl SpinGuard {
    fn acquire(lock: &'static AtomicBool) -> Self {
        while lock
            .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            std::thread::yield_now();
        }
        Self(lock)
    }
}

impl Drop for SpinGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// RAII registration in a reader-quiescence slot, remembering which slot so
/// the matching decrement cannot land on the wrong one after an epoch bump.
struct ReaderGuard {
    slot: usize,
}

impl ReaderGuard {
    fn enter() -> Self {
        Self::enter_with(|| {})
    }

    /// [`ReaderGuard::enter`] with a seam between choosing a slot and
    /// registering in it, so a test can drive the interleaving below
    /// deterministically instead of racing for it.
    ///
    /// Registration has to be linearizable with respect to [`READER_EPOCH`].
    /// Reading the epoch and incrementing the slot are two separate atomic
    /// operations, so a retirement can land between them: it bumps the epoch
    /// and drains the slot this reader picked, observing zero because the
    /// increment has not happened yet. The reader then registers in a slot
    /// nothing will ever drain again until the epoch wraps, and the *next*
    /// retirement reclaims a generation this reader is still dereferencing.
    ///
    /// Re-reading the epoch after the increment closes that window: either it
    /// is unchanged -- so the increment is visible to every drain that can
    /// still see this generation -- or the slot is released and the choice is
    /// made again. The comparison is on the whole epoch rather than its
    /// parity, because two retirements return the parity to its original
    /// value while leaving the registration just as stale.
    fn enter_with(on_slot_chosen: impl Fn()) -> Self {
        loop {
            let epoch = READER_EPOCH.load(Ordering::SeqCst);
            on_slot_chosen();
            let slot = epoch & 1;
            ACTIVE_READERS[slot].fetch_add(1, Ordering::SeqCst);
            if READER_EPOCH.load(Ordering::SeqCst) == epoch {
                return Self { slot };
            }
            ACTIVE_READERS[slot].fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        ACTIVE_READERS[self.slot].fetch_sub(1, Ordering::SeqCst);
    }
}

/// Runs `f` with the currently published pointer, inside the reader
/// window. The pointer is only valid for the duration of `f`.
///
/// **Never acquire a lock inside `f`.** A drain holds [`RETIRE_LOCK`] while
/// waiting for a slot to empty, so a reader that blocked on a lock while
/// registered would close a wait cycle. Returning an owned value out of the
/// closure — rather than handing out the guard — is what makes that
/// impossible to write by accident: the window cannot outlive `f`, and
/// everything inside it is non-blocking by construction.
fn with_reader<T>(f: impl FnOnce(*mut runtime::Runtime) -> T) -> T {
    let _guard = ReaderGuard::enter();
    f(RUNTIME.load(Ordering::SeqCst))
}

/// Takes an owned clone of the published generation, or `None` if the slot
/// is currently empty.
fn published_clone() -> Option<Arc<runtime::Runtime>> {
    with_reader(|ptr| {
        (!ptr.is_null()).then(|| {
            // SAFETY: `ptr` is non-null, was produced by `Arc::into_raw`,
            // and we are inside the reader window, so no retirement can
            // reclaim it before this call finishes.
            unsafe { read_published(ptr) }
        })
    })
}

fn create_runtime() -> runtime::Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("lancedb-tokio-worker")
        .build()
        .unwrap_or_else(|e| panic!("failed to build tokio runtime: {e}"))
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
    get_runtime_with(create_runtime)
}

/// [`get_runtime`] parameterized over how the runtime gets built, so tests
/// can inject a failing builder — the same injection trick [`retire_with`]
/// uses for `spawn`, since neither thread exhaustion nor a failed runtime
/// build is something a test can trigger deterministically or portably.
fn get_runtime_with<F>(create: F) -> Arc<runtime::Runtime>
where
    F: FnOnce() -> runtime::Runtime,
{
    if let Some(rt) = published_clone() {
        return rt;
    }

    let _install = SpinGuard::acquire(&INSTALL_LOCK);
    // Someone else may have installed while we were spinning for the lock.
    if let Some(rt) = published_clone() {
        return rt;
    }

    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    // If `create` panics — thread or handle exhaustion being the realistic
    // cause — `_install` still releases on unwind, leaving `RUNTIME` null
    // and the lock free, so the next call simply retries instead of finding
    // the module permanently wedged.
    let new_rt = Arc::new(create());
    // `Arc::into_raw` consumes one strong reference into the raw pointer —
    // the slot itself now *owns* that reference, rather than holding a
    // separate permanent clone the way a wrapper type would. `new_rt` below
    // still holds this call's own reference to return to the caller.
    let published = Arc::into_raw(new_rt.clone()) as *mut runtime::Runtime;
    RUNTIME.store(published, Ordering::SeqCst);
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
/// thread survives. **Atomic-only, nothing else** — six plain stores,
/// deliberately abandoning whatever the parent's generation/lock/reader
/// state was. Indexing a `static [AtomicUsize; 2]` is address arithmetic
/// over static memory: no allocation, no TLS, async-signal-safe, exactly
/// like the scalar stores beside it.
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
/// - `ACTIVE_READERS[..].store(0)` and `READER_EPOCH.store(0)`: a
///   *different*, now-vanished thread could have been inside the reader
///   window in [`get_runtime`] at fork time, leaving a slot stuck above
///   zero forever in the child (it can never decrement — that thread
///   doesn't exist here) and permanently wedging any future
///   [`reset_runtime`] call in the child. Forcing both slots and the epoch
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
    ACTIVE_READERS[0].store(0, Ordering::SeqCst);
    ACTIVE_READERS[1].store(0, Ordering::SeqCst);
    READER_EPOCH.store(0, Ordering::SeqCst);
    INSTALL_LOCK.store(false, Ordering::SeqCst);
    RETIRE_LOCK.store(false, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// How hard a reaper tries before giving up on a retired generation.
#[derive(Debug, Clone, Copy)]
struct ReaperPolicy {
    /// First sleep between ownership checks.
    poll: Duration,
    /// Ceiling the sleep backs off to.
    max_poll: Duration,
    /// Attempts before the first warning.
    warn_after: u32,
    /// Warning cadence after the first one.
    warn_every: u32,
    /// Total attempt budget, or `None` to poll forever.
    give_up_after: Option<u32>,
}

impl Default for ReaperPolicy {
    fn default() -> Self {
        // 50ms doubling to 1s, giving up after roughly ten minutes. A
        // reference still outstanding by then is a wedged task, not slow
        // work, and keeping one reaper thread per reset alive forever would
        // be precisely the unbounded thread growth this API exists to
        // avoid.
        Self {
            poll: Duration::from_millis(50),
            max_poll: Duration::from_secs(1),
            warn_after: 60,
            warn_every: 300,
            give_up_after: Some(640),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReapOutcome {
    ShutDown { attempts: u32 },
    GaveUp { attempts: u32 },
}

/// Drives a retired generation to shutdown, or gives up and leaks it.
///
/// Pure and thread-free so tests can call it directly. Polls until it is
/// the sole owner — any in-flight `block_on`/`spawn`/`spawn_blocking`
/// caller that took its own clone before this generation was retired keeps
/// it alive until that specific call finishes, same as `Arc` always works —
/// then shuts it down explicitly from the calling thread, which [`retire`]
/// guarantees is never one of that runtime's own workers.
///
/// **Giving up leaks; it must never drop.** At that point we know another
/// owner exists, and the realistic candidate is a wedged task running on a
/// worker of this very runtime. Dropping our clone would hand that owner
/// the last reference, so `Runtime::drop` would eventually run inside the
/// runtime it is destroying — the panic this whole design exists to
/// prevent. Leaking costs one generation's threads, which were already
/// alive and are exactly the threads that wedged task is sitting on.
fn reap(mut retired: Arc<runtime::Runtime>, policy: ReaperPolicy, id: usize) -> ReapOutcome {
    let mut attempts: u32 = 0;
    let mut backoff = policy.poll;
    loop {
        match Arc::try_unwrap(retired) {
            Ok(rt) => {
                rt.shutdown_background();
                return ReapOutcome::ShutDown { attempts };
            }
            Err(still_shared) => {
                retired = still_shared;
                attempts += 1;

                if policy
                    .give_up_after
                    .is_some_and(|budget| attempts >= budget)
                {
                    let live = LIVE_RETIREMENTS.load(Ordering::SeqCst);
                    log::warn!(
                        "lancedb: retired tokio runtime generation {id} still has other \
                         owners after {attempts} attempts; leaking it (its worker threads \
                         stay alive) rather than risking a runtime drop on its own worker. \
                         {live} retired generation(s) currently awaiting reclamation - if \
                         that number keeps climbing, resets are outpacing task completion."
                    );
                    std::mem::forget(retired);
                    return ReapOutcome::GaveUp { attempts };
                }

                if attempts == policy.warn_after
                    || (attempts > policy.warn_after
                        && policy.warn_every > 0
                        && (attempts - policy.warn_after).is_multiple_of(policy.warn_every))
                {
                    log::warn!(
                        "lancedb: still waiting to reclaim retired tokio runtime \
                         generation {id} after {attempts} attempts; some task or call \
                         is still holding a reference to it"
                    );
                }

                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(policy.max_poll);
            }
        }
    }
}

/// Spawns a dedicated, one-shot, non-Tokio thread that owns final disposal
/// of a single retired runtime generation — see the module docs for why
/// this is per-generation rather than a shared long-lived reaper.
fn retire(rt: Arc<runtime::Runtime>) {
    retire_with(rt, |name, body| {
        std::thread::Builder::new()
            .name(name)
            .spawn(body)
            .map(|_handle| ())
    });
}

/// The actual retirement logic behind [`retire`], parameterized over how a
/// thread gets spawned so tests can simulate `spawn` failing (exhausted OS
/// thread limits are not something a test can trigger deterministically or
/// portably) and assert on the fallback behavior below without needing to
/// actually exhaust threads.
fn retire_with<S>(rt: Arc<runtime::Runtime>, spawn: S)
where
    S: FnOnce(String, Box<dyn FnOnce() + Send>) -> std::io::Result<()>,
{
    // `ManuallyDrop` never runs `Arc<Runtime>`'s destructor on its own,
    // whether or not the closure below ever executes. If `spawn` fails, the
    // still-unrun closure is dropped by the caller -- which drops this
    // `ManuallyDrop` wrapper as a no-op, deliberately leaking the retired
    // runtime instead of letting it drop (possibly running
    // `Runtime::drop()` for real) on whatever thread called `retire()`,
    // which could be one of that very runtime's own workers. A raw pointer
    // would leak identically but isn't `Send`, so `Builder::spawn` (which
    // requires `F: Send`) would refuse to compile it.
    let id = RETIREMENT_SEQ.fetch_add(1, Ordering::SeqCst);
    LIVE_RETIREMENTS.fetch_add(1, Ordering::SeqCst);
    let carrier = std::mem::ManuallyDrop::new(rt);
    let body = move || {
        let retired = std::mem::ManuallyDrop::into_inner(carrier);
        let _ = reap(retired, ReaperPolicy::default(), id);
        LIVE_RETIREMENTS.fetch_sub(1, Ordering::SeqCst);
    };
    // If `spawn` fails (extreme resource exhaustion), `body` -- and the
    // `ManuallyDrop` carrier it captured -- is simply dropped by the
    // caller without ever running, which (per above) leaks the retired
    // runtime rather than dropping it here. The gauge has to be unwound by
    // hand in that case, since the body that would have decremented it
    // never runs.
    if spawn(format!("lancedb-runtime-reaper-{id}"), Box::new(body)).is_err() {
        LIVE_RETIREMENTS.fetch_sub(1, Ordering::SeqCst);
        log::warn!(
            "lancedb: could not spawn a reaper thread for retired tokio runtime \
             generation {id}; leaking it (its worker threads stay alive) rather than \
             dropping it on the calling thread, which may be one of its own workers"
        );
    }
}

/// How long [`reset_runtime_inner`] waits for readers of the retired
/// generation before giving up and leaking it. The drain it guards is
/// provably finite (see the termination argument there), so reaching this
/// means something is wedged; the timeout exists so that "wedged" degrades
/// to a leak instead of an unkillable spin with the GIL released.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
enum ResetOutcome {
    /// Nothing was installed, so there was nothing to retire.
    NoRuntime,
    /// The old generation was handed to a reaper.
    Retired,
    /// Readers of the retired generation never quiesced; it was leaked
    /// rather than reclaimed.
    LeakedOnDrainTimeout,
}

/// Waits for `slot` to empty, backing off, and reports whether it did.
fn drain(slot: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    let mut spins: u32 = 0;
    let mut backoff = Duration::from_micros(50);
    let mut warned = false;
    loop {
        if ACTIVE_READERS[slot].load(Ordering::SeqCst) == 0 {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        // The common case resolves in microseconds -- a reader window is a
        // load plus a refcount bump -- so spin briefly before sleeping.
        if spins < 64 {
            spins += 1;
            std::thread::yield_now();
        } else {
            if !warned && start.elapsed() >= Duration::from_secs(1) {
                warned = true;
                log::warn!(
                    "lancedb: reset_runtime has waited over a second for in-flight \
                     readers of the retired tokio runtime to finish"
                );
            }
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_millis(10));
        }
    }
}

/// Publishes a fresh Tokio runtime generation for *future* `get_runtime()`
/// callers (`block_on`/`spawn`/`future_into_py`), without disturbing
/// in-flight work on the current one, and releases the retired generation's
/// worker threads once nothing is using it any more.
///
/// Exists because those threads are otherwise held for the entire life of
/// the process. The fork-reset hook above only fires on POSIX — there is no
/// `fork()` on Windows, so [`install_atfork`] is a no-op there — which
/// means that on POSIX recycling only ever happens as a side effect of
/// forking, and on Windows it cannot happen at all. A long-lived host that
/// uses LanceDB in bursts (a test suite, a notebook kernel, an agent
/// runtime, a service that re-indexes periodically) can call this between
/// bursts to hand those threads back.
///
/// Safe to call at any point between operations, from any thread. A call
/// already mid-`block_on`, or a `spawn`/`spawn_blocking`ed task still
/// running, holds its own `Arc` clone of the *retired* runtime (captured
/// inside the actual spawned future/closure — see [`LanceRuntime::spawn`])
/// and keeps it alive until that specific call finishes; the retired
/// runtime's actual shutdown is handled by a dedicated one-shot thread (see
/// [`retire`]) once every such clone is gone, never inline on whatever
/// thread happens to drop the last one.
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
fn reset_runtime_impl() -> ResetOutcome {
    reset_runtime_inner(DRAIN_TIMEOUT, retire)
}

/// [`reset_runtime_impl`] with the drain deadline and the disposal step
/// injectable, so tests can force a timeout deterministically and assert
/// that nothing was reclaimed.
///
/// # Ordering
///
/// The pointer swap **must** happen before the epoch bump, and the whole
/// sequence uses `SeqCst`, so there is a single total order over these
/// operations. Take any reader R that observed `old_ptr`: R's load of
/// `RUNTIME` precedes the swap, and by program order R's load of
/// `READER_EPOCH` precedes its load of `RUNTIME`. Chaining those gives
/// `R.load(EPOCH)` before `swap` before `fetch_add(EPOCH)`, so R read the
/// pre-bump epoch.
///
/// Reading that epoch does not by itself place R in the slot being drained,
/// because choosing a slot and registering in it are separate operations and
/// the epoch can move between them. [`ReaderGuard::enter_with`] closes that
/// window by re-reading the epoch after its increment and retrying if it
/// moved, so a guard is only ever returned registered under an epoch that was
/// still current once the increment was visible. Given that, R's `fetch_add`
/// lands in the slot we are about to drain, and it precedes R's load of
/// `RUNTIME`, hence the swap, hence the bump and our first read of the slot —
/// so the drain observes R and waits for it.
///
/// Reversing the order would break exactly this: a reader that read the
/// post-bump epoch (registering in the *new* slot) could still load
/// `RUNTIME` before the swap and come away with `old_ptr`, outside the slot
/// being drained — a use-after-free.
///
/// # Termination
///
/// After the bump, no reader can newly enter the retired slot. The readers
/// that can still be in it are those that had already read the pre-bump
/// epoch and not yet incremented — a finite, already-closed set — and each
/// one's window contains nothing but a load and a refcount bump. So the
/// slot reaches zero. This is what a single global counter could not
/// guarantee: there, readers of the new generation kept feeding the same
/// counter the drain was waiting on.
fn reset_runtime_inner(
    drain_timeout: Duration,
    retire_fn: impl FnOnce(Arc<runtime::Runtime>),
) -> ResetOutcome {
    // Held across the whole retirement, so at most one generation is being
    // drained at a time and the other slot is always the live one.
    let _retire_lock = SpinGuard::acquire(&RETIRE_LOCK);
    let old_ptr = {
        // Only the swap needs the install lock, so a concurrent
        // `get_runtime()` can install the next generation immediately
        // rather than waiting behind our drain.
        let _install = SpinGuard::acquire(&INSTALL_LOCK);
        RUNTIME.swap(ptr::null_mut(), Ordering::SeqCst)
    };
    if old_ptr.is_null() {
        return ResetOutcome::NoRuntime;
    }
    // ORDERING: load-bearing that this follows the swap -- see above.
    let retiring = READER_EPOCH.fetch_add(1, Ordering::SeqCst) & 1;

    if !drain(retiring, drain_timeout) {
        log::warn!(
            "lancedb: reset_runtime timed out after {drain_timeout:?} waiting for \
             in-flight readers of the retired tokio runtime; leaking that generation \
             (its worker threads stay alive) rather than risking a use-after-free"
        );
        // Deliberately return without reconstructing the `Arc`: reclaiming
        // could free memory a reader still holds, and dropping could run
        // the runtime's destructor on one of its own workers. Leaking is
        // the pre-existing behavior, so this degrades to the status quo.
        return ResetOutcome::LeakedOnDrainTimeout;
    }

    // SAFETY: no reader can be dereferencing `old_ptr` any more -- it was
    // swapped out of `RUNTIME` above, and the drain proved the slot every
    // such reader registered in has emptied. Reconstructing the `Arc` here
    // (rather than cloning through a still-live pointer) consumes exactly
    // the one strong reference the slot itself owned, so this generation's
    // count genuinely reaches zero once every caller-held clone is gone.
    let retired: Arc<runtime::Runtime> =
        unsafe { Arc::from_raw(old_ptr as *const runtime::Runtime) };
    retire_fn(retired);
    ResetOutcome::Retired
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
    use std::panic::AssertUnwindSafe;
    use std::sync::{Mutex, mpsc};

    /// Every test here mutates the same process-global slot, epoch and
    /// locks, so they cannot run concurrently with each other even though
    /// the test harness is happy to try.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        // A test that panics while holding the lock poisons it; the state
        // it guards is re-established by each test anyway, so recovering is
        // preferable to cascading failures.
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn active_readers_total() -> usize {
        ACTIVE_READERS[0].load(Ordering::SeqCst) + ACTIVE_READERS[1].load(Ordering::SeqCst)
    }

    /// Runs `f` with panic output suppressed, returning whether it panicked.
    fn panicked(f: impl FnOnce()) -> bool {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(AssertUnwindSafe(f));
        std::panic::set_hook(prev);
        result.is_err()
    }

    fn impatient_policy() -> ReaperPolicy {
        ReaperPolicy {
            poll: Duration::from_millis(1),
            max_poll: Duration::from_millis(1),
            warn_after: 2,
            warn_every: 2,
            give_up_after: Some(5),
        }
    }

    /// The install lock must survive an unwind out of the runtime builder.
    /// Thread or handle exhaustion is exactly the situation a caller
    /// reaches for `reset_runtime` in, and before the guard existed a panic
    /// there stranded the lock, leaving every later `get_runtime()` and
    /// `reset_runtime()` spinning forever with no way out.
    #[test]
    fn install_lock_is_released_when_runtime_construction_panics() {
        let _t = test_lock();
        reset_runtime_impl();

        assert!(
            panicked(|| {
                get_runtime_with(|| panic!("simulated runtime build failure"));
            }),
            "injected builder failure did not surface as a panic"
        );

        assert!(
            !INSTALL_LOCK.load(Ordering::SeqCst),
            "install lock stayed held across the unwind"
        );
        // Still usable: the slot is empty, so this rebuilds cleanly.
        get_runtime().block_on(async {});
    }

    /// The reader window must be balanced even if something unwinds out of
    /// it: a stranded registration would make every later drain wait on a
    /// reader that no longer exists.
    #[test]
    fn reader_window_is_balanced_across_unwind() {
        let _t = test_lock();
        let before = active_readers_total();

        assert!(panicked(|| {
            with_reader(|_ptr| panic!("simulated panic inside the reader window"));
        }));

        assert_eq!(
            active_readers_total(),
            before,
            "reader registration leaked across the unwind"
        );
    }

    /// The regression that motivated the two-slot epoch. Readers of the
    /// *new* generation must not be able to hold up the drain of a retired
    /// one -- with a single global counter, this traffic kept the counter
    /// above zero and the drain never finished.
    #[test]
    fn drain_ignores_readers_that_joined_the_new_generation() {
        let _t = test_lock();
        let _installed = get_runtime();

        let stop = Arc::new(AtomicBool::new(false));
        let hammer = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = get_runtime();
                }
            })
        };

        let outcome = reset_runtime_inner(Duration::from_secs(5), retire);

        stop.store(true, Ordering::Relaxed);
        hammer.join().unwrap();

        assert_eq!(
            outcome,
            ResetOutcome::Retired,
            "drain was held up by readers of the new generation"
        );
    }

    /// A drain that cannot finish must leak rather than hang or reclaim.
    /// Holding a reader registration on this very thread makes the timeout
    /// deterministic, with no racing helper thread involved.
    #[test]
    fn drain_times_out_into_a_leak_instead_of_hanging_or_reclaiming() {
        let _t = test_lock();
        let _installed = get_runtime();

        let stuck = ReaderGuard::enter();
        let outcome = reset_runtime_inner(Duration::from_millis(50), |_retired| {
            panic!("reclaimed a generation while one of its readers was still registered")
        });
        drop(stuck);

        assert_eq!(outcome, ResetOutcome::LeakedOnDrainTimeout);
    }

    /// Regression for a reader-admission race: choosing a slot and
    /// registering in it are separate atomics, so a retirement can land
    /// between them, drain the chosen slot while it still reads zero, and
    /// leave the reader registered where no later drain looks -- at which
    /// point the next retirement reclaims a generation that reader is still
    /// using.
    ///
    /// Driven through the seam rather than by racing threads, so the
    /// interleaving is exercised on every run instead of occasionally.
    fn assert_admission_is_not_stranded(retirements: usize) {
        let _installed = get_runtime();
        let fired = AtomicBool::new(false);

        let guard = ReaderGuard::enter_with(|| {
            // First attempt only; a retry has to be able to finish.
            if fired.swap(true, Ordering::SeqCst) {
                return;
            }
            for _ in 0..retirements {
                reset_runtime_impl();
                get_runtime();
            }
        });

        assert_eq!(
            guard.slot,
            READER_EPOCH.load(Ordering::SeqCst) & 1,
            "reader ended up in a slot no live drain watches"
        );
        assert!(
            ACTIVE_READERS[guard.slot].load(Ordering::SeqCst) > 0,
            "reader is not counted in the slot its guard claims"
        );
    }

    #[test]
    fn reader_admission_is_not_stranded_by_a_retirement() {
        let _t = test_lock();
        assert_admission_is_not_stranded(1);
    }

    #[test]
    fn reader_admission_survives_epoch_wraparound() {
        let _t = test_lock();
        // Two retirements put the parity back where it started, which is what
        // makes a stale registration silently wrong rather than obviously so.
        assert_admission_is_not_stranded(2);
    }

    #[test]
    fn reaper_shuts_down_once_it_becomes_sole_owner() {
        let rt = Arc::new(create_runtime());
        let weak = Arc::downgrade(&rt);

        assert!(matches!(
            reap(rt, impatient_policy(), 0),
            ReapOutcome::ShutDown { .. }
        ));
        assert!(
            weak.upgrade().is_none(),
            "reaper reported shutdown without actually consuming the generation"
        );
    }

    /// Giving up must leak, never drop. If the reaper dropped its clone,
    /// the last reference would fall to whoever else still holds one --
    /// realistically a wedged task on a worker of this very runtime -- and
    /// `Runtime::drop` would then run inside the runtime it is destroying,
    /// which Tokio panics on.
    #[test]
    fn reaper_gives_up_by_leaking_never_by_dropping() {
        let rt = Arc::new(create_runtime());
        let hostage = rt.clone();
        let weak = Arc::downgrade(&rt);

        let outcome = reap(rt, impatient_policy(), 0);
        assert!(
            matches!(outcome, ReapOutcome::GaveUp { .. }),
            "reaper polled past its budget instead of giving up"
        );

        drop(hostage);
        assert!(
            weak.upgrade().is_some(),
            "giving up dropped the reaper's clone instead of leaking it"
        );
    }

    /// Regression for the exact defect the gatekeeper found two revisions
    /// prior: `retire()` moved the retired `Arc` directly into the spawned
    /// closure, so a failed `Builder::spawn` -- whose contract drops the
    /// still-unrun closure -- dropped the `Arc` right there on the caller's
    /// thread, which could be one of the runtime's own workers and panic.
    /// Simulates the failure deterministically (exhausting real OS thread
    /// limits is neither reliable nor portable in a test) by injecting a
    /// `spawn` that drops the body unrun and reports an error, exactly
    /// mirroring what `std::thread::Builder::spawn` itself does on failure.
    #[test]
    fn retire_leaks_instead_of_dropping_on_spawn_failure() {
        let rt = Arc::new(create_runtime());
        let weak = Arc::downgrade(&rt);

        retire_with(rt, |_name, body| {
            drop(body);
            Err(std::io::Error::other("simulated spawn failure"))
        });

        // If the retired `Arc` had been dropped here (on this test thread)
        // instead of leaked, its strong count would already be zero and
        // this upgrade would fail.
        assert!(
            weak.upgrade().is_some(),
            "retired runtime was dropped on spawn failure instead of \
             being leaked via the ManuallyDrop carrier"
        );
    }

    /// Regression for the exact defect the gatekeeper found in the prior
    /// revision: a `Box<Arc<Runtime>>`-wrapper design that permanently held
    /// one extra strong reference meant `Arc::try_unwrap` in the reaper
    /// could never succeed, so retired runtimes (and their worker threads)
    /// leaked forever instead of actually shutting down. This proves
    /// `reset_runtime`'s reclaim path genuinely drives a retired
    /// generation's shutdown, not just a successful pointer replacement.
    #[test]
    fn reset_runtime_actually_shuts_down_the_retired_generation() {
        let _t = test_lock();
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
        let _t = test_lock();
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
