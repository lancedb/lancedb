// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::atomic::Ordering;
use std::sync::{LazyLock, atomic};
use std::time::Duration;

use futures::{Future, FutureExt};
use tokio::runtime::{Builder, Runtime};
use tracing::Span;

/// We cache the call to num_cpus::get() because:
///
/// 1. It shouldn't change during the lifetime of the program
/// 2. It's a relatively expensive call (requires opening several files and examining them)
static NUM_COMPUTE_INTENSIVE_CPUS: LazyLock<usize> =
    LazyLock::new(calculate_num_compute_intensive_cpus);

pub fn get_num_compute_intensive_cpus() -> usize {
    *NUM_COMPUTE_INTENSIVE_CPUS
}

fn calculate_num_compute_intensive_cpus() -> usize {
    if let Ok(raw) = std::env::var("LANCE_CPU_THREADS") {
        return parse_env_usize("LANCE_CPU_THREADS", &raw, 1).unwrap_or_else(|e| panic!("{e}"));
    }

    let cpus = num_cpus::get();

    if cpus <= *IO_CORE_RESERVATION {
        // If the user is not setting a custom value for LANCE_IO_CORE_RESERVATION then we don't emit
        // a warning because they're just on a small machine and there isn't much they can do about it.
        if cpus > 2 {
            log::warn!(
                "Number of CPUs is less than or equal to the number of IO core reservations. \
                This is not a supported configuration. using 1 CPU for compute intensive tasks."
            );
        }
        return 1;
    }

    num_cpus::get() - *IO_CORE_RESERVATION
}

/// Parse an integer environment variable, rejecting values below `min`.
///
/// The error names the variable, so a bad value is diagnosable instead of
/// surfacing as a bare `ParseIntError` or, for `LANCE_CPU_THREADS=0`, a panic
/// deep inside tokio's `max_blocking_threads`.
fn parse_env_usize(name: &str, raw: &str, min: usize) -> Result<usize, String> {
    let value: usize = raw
        .trim()
        .parse()
        .map_err(|e| format!("environment variable {name} must be an integer, got {raw:?}: {e}"))?;
    if value < min {
        return Err(format!(
            "environment variable {name} must be at least {min}, got {value}"
        ));
    }
    Ok(value)
}

/// Number of CPU cores held back for I/O and control tasks.
///
/// Overridable via the `LANCE_IO_CORE_RESERVATION` environment variable;
/// defaults to `2` when unset. `0` is allowed (reserve nothing);
/// [`get_num_compute_intensive_cpus`] subtracts this from the core count to
/// size the compute pool. A non-integer value panics on first access with an
/// error naming the variable.
pub static IO_CORE_RESERVATION: LazyLock<usize> =
    LazyLock::new(|| match std::env::var("LANCE_IO_CORE_RESERVATION") {
        Ok(raw) => {
            parse_env_usize("LANCE_IO_CORE_RESERVATION", &raw, 0).unwrap_or_else(|e| panic!("{e}"))
        }
        Err(_) => 2,
    });

fn create_runtime() -> Runtime {
    Builder::new_multi_thread()
        .thread_name("lance-cpu")
        .max_blocking_threads(get_num_compute_intensive_cpus())
        .worker_threads(1)
        // keep the thread alive "forever"
        .thread_keep_alive(Duration::from_secs(u64::MAX))
        .build()
        .unwrap()
}

static CPU_RUNTIME: atomic::AtomicPtr<Runtime> = atomic::AtomicPtr::new(std::ptr::null_mut());

static RUNTIME_INSTALLED: atomic::AtomicBool = atomic::AtomicBool::new(false);

static ATFORK_INSTALLED: atomic::AtomicBool = atomic::AtomicBool::new(false);

fn global_cpu_runtime() -> &'static Runtime {
    loop {
        let ptr = CPU_RUNTIME.load(Ordering::SeqCst);
        if !ptr.is_null() {
            // SAFETY: `ptr` was produced by `Box::into_raw` below and is only ever
            // reset to null by `atfork_tokio_child` in the forked child (single-
            // threaded, async-signal context). The `Box` is never reclaimed, so the
            // `Runtime` lives for the rest of the process.
            return unsafe { &*ptr };
        }
        if !RUNTIME_INSTALLED.fetch_or(true, Ordering::SeqCst) {
            break;
        }
        std::thread::yield_now();
    }
    if !ATFORK_INSTALLED.fetch_or(true, Ordering::SeqCst) {
        install_atfork();
    }
    let new_ptr = Box::into_raw(Box::new(create_runtime()));
    CPU_RUNTIME.store(new_ptr, Ordering::SeqCst);
    // SAFETY: `new_ptr` was just obtained from `Box::into_raw`, so it is non-null,
    // aligned, and points to a live `Runtime` that is never reclaimed.
    unsafe { &*new_ptr }
}

/// After a fork() operation, force re-creation of the BackgroundExecutor. Note: this function
/// runs in "async-signal context" which means that we can't (safely) do much here.
extern "C" fn atfork_tokio_child() {
    CPU_RUNTIME.store(std::ptr::null_mut(), Ordering::SeqCst);
    RUNTIME_INSTALLED.store(false, Ordering::SeqCst);
}

#[cfg(not(windows))]
fn install_atfork() {
    unsafe { libc::pthread_atfork(None, None, Some(atfork_tokio_child)) };
}

#[cfg(windows)]
fn install_atfork() {}

/// Spawn a CPU intensive task
///
/// This task will be put onto a thread pool dedicated for CPU-intensive work
/// This keeps the tokio thread pool free so that we can always be ready to service
/// cheap I/O & control requests.
///
/// This can also be used to convert a big chunk of synchronous work into a future
/// so that it can be run in parallel with something like StreamExt::buffered()
///
/// # Only hand over substantial CPU work
///
/// Dispatching to the pool has real overhead (a `spawn_blocking` hop plus a oneshot
/// channel round trip). As a rule of thumb the closure should be expected to do at
/// least ~100µs of CPU work; below that the thread-pool overhead is likely to
/// outweigh any parallelism benefit, and the work is better left inline.
///
/// # The task must never wait on anything
///
/// The CPU pool is sized to [`get_num_compute_intensive_cpus`], which is
/// `max(1, num_cpus - LANCE_IO_CORE_RESERVATION)`. On a big host that is plenty of
/// workers (e.g. 62 on a 64-core box), but in resource-constrained environments it can
/// collapse to a **single blocking thread** — on machines with `<= 3` visible CPUs
/// (1-vCPU VMs, CI runners, CPU-limited Kubernetes pods) the pool has exactly one
/// worker. A closure passed to `spawn_cpu` occupies one of these threads for its entire
/// lifetime, including any time it spends *parked*. So the closure must only consume
/// CPU and return; it must
/// **never** block, wait, or park. Concretely, the closure must not, directly or
/// transitively:
///
/// * **No channels** — no blocking send/recv (`send_blocking`, blocking `recv`, etc.).
///   A full/empty channel parks the thread, and whatever would drain/fill the channel
///   may need the same pool to run.
/// * **No I/O** — no file, network, or object-store reads/writes, and no disk spills.
///   I/O parks the thread while making no progress on CPU work.
/// * **No locks** — no acquiring a contended lock (or any lock that is held across an
///   `.await` elsewhere). Waiting for the lock parks the thread.
/// * **No `block_on` / `.blocking_*`** — never drive or wait on another async task
///   from inside the closure.
///
/// If any of these hold, the parked thread can starve the exact work that would
/// unblock it, deadlocking the whole pool with no timeout and no error — a silent
/// hang at 0% CPU. (See <https://github.com/lancedb/lance/pull/7423>.) When work
/// needs to wait on a channel/lock/I/O, keep the waiting in an async task and only
/// hand the pure-CPU portion to `spawn_cpu`, e.g. build each batch with `spawn_cpu`
/// and dispatch it with `tx.send(batch).await` in the surrounding async code.
pub fn spawn_cpu<
    E: std::error::Error + Send + 'static,
    F: FnOnce() -> std::result::Result<R, E> + Send + 'static,
    R: Send + 'static,
>(
    func: F,
) -> impl Future<Output = std::result::Result<R, E>> {
    let (send, recv) = tokio::sync::oneshot::channel();
    // Propagate the current span into the task
    let span = Span::current();
    global_cpu_runtime().spawn_blocking(move || {
        let _span_guard = span.enter();
        let result = func();
        let _ = send.send(result);
    });
    recv.map(|res| res.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The env vars feed process-global `LazyLock`s that read once and are read
    // in parallel by other tests, so the pure parser is tested directly rather
    // than by mutating the environment.

    #[test]
    fn parses_valid_value_and_trims_surrounding_whitespace() {
        assert_eq!(parse_env_usize("VAR", "8", 1).unwrap(), 8);
        assert_eq!(parse_env_usize("VAR", " 8 ", 1).unwrap(), 8);
    }

    #[test]
    fn rejects_non_integer_naming_the_variable() {
        let err = parse_env_usize("LANCE_CPU_THREADS", "abc", 1).unwrap_err();
        assert!(err.contains("LANCE_CPU_THREADS"), "{err}");
        assert!(err.contains("must be an integer"), "{err}");
    }

    #[test]
    fn rejects_value_below_minimum() {
        // LANCE_CPU_THREADS=0 parses fine but would panic in tokio's
        // max_blocking_threads(0); the minimum stops it at the boundary.
        let err = parse_env_usize("LANCE_CPU_THREADS", "0", 1).unwrap_err();
        assert!(err.contains("at least 1"), "{err}");
    }

    #[test]
    fn allows_zero_when_minimum_is_zero() {
        // LANCE_IO_CORE_RESERVATION=0 is valid: no cores reserved for IO.
        assert_eq!(parse_env_usize("VAR", "0", 0).unwrap(), 0);
    }
}
