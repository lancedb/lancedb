# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import concurrent.futures
import os
import threading
import warnings


class BackgroundEventLoop:
    """
    A background event loop that can run futures.

    Used to bridge sync and async code, without messing with users event loops.
    """

    def __init__(self):
        # Guards `reset()` end-to-end (stop/join/close/_start as one unit) --
        # without it, two concurrent `reset()` calls can both snapshot the
        # same old loop/thread, both pass the join/close checks (`join()`
        # releases the GIL, so both can be mid-wait at once), and then both
        # call `_start()`, with the second overwriting `self.loop`/
        # `self.thread` and orphaning the first's brand-new thread.
        self._reset_lock = threading.Lock()
        self._start()

    def _start(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self.loop.run_forever,
            name="LanceDBBackgroundEventLoop",
            daemon=True,
        )
        self.thread.start()

    def run(self, future):
        concurrent_future = asyncio.run_coroutine_threadsafe(future, self.loop)
        try:
            return concurrent_future.result()
        except BaseException:
            concurrent_future.cancel()
            raise

    def reset(self, *, join_timeout: float = 5.0) -> None:
        """Stop this loop's background thread, close the loop, and start a
        fresh one.

        Unlike `_reset_after_fork` below (which runs after `fork()`, where
        the old thread is already dead and must be leaked to avoid a hang),
        this stops the old loop and actually joins its thread before
        starting the new one -- the thread is alive here, so joining it is
        safe. See `reset_background_loop` for why you'd want this.

        Raises `RuntimeError` (without starting a second loop) if the old
        thread doesn't stop within `join_timeout` -- proceeding anyway
        would risk two live background loops at once, and silently
        skipping `close()` would leave the old loop's resources (its
        Proactor/IOCP selector on Windows included) unreleased, defeating
        the whole point of calling this.

        Thread-safe: concurrent `reset()` calls are serialized (see
        `self._reset_lock`), so one always fully completes (or raises)
        before the next one reads `self.loop`/`self.thread`.
        """
        with self._reset_lock:
            old_loop = self.loop
            old_thread = self.thread
            old_loop.call_soon_threadsafe(old_loop.stop)
            old_thread.join(timeout=join_timeout)
            if old_thread.is_alive():
                msg = (
                    f"BackgroundEventLoop.reset: background thread did not "
                    f"stop within {join_timeout}s"
                )
                raise RuntimeError(msg)
            old_loop.close()
            self._start()


LOOP = BackgroundEventLoop()


def _new_embedding_executor() -> concurrent.futures.ThreadPoolExecutor:
    return concurrent.futures.ThreadPoolExecutor(thread_name_prefix="lancedb-embedding")


# Embedding functions can block for a long time -- a heavy local model or an
# HTTP request to a remote embeddings API. Running them on asyncio's default
# executor lets them starve the unrelated blocking I/O that shares that pool,
# so they get a dedicated one. See
# https://github.com/lancedb/lancedb/issues/3310.
_EMBEDDING_EXECUTOR = _new_embedding_executor()


def embedding_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Return the executor dedicated to running blocking embedding calls."""
    return _EMBEDDING_EXECUTOR


_FORK_WARNED = False


def _reset_after_fork():
    # A `threading.Lock` held by a *different* thread at fork time is
    # inherited by the child in a possibly-locked state that thread can
    # never release (it doesn't exist in the child) -- same hazard class as
    # the Rust-side runtime slot's own atfork handling. Give the child a
    # fresh, definitely-unlocked lock before anything else touches it,
    # rather than reusing whatever `LOOP._reset_lock` was doing in the
    # parent at the moment of fork.
    LOOP._reset_lock = threading.Lock()
    # Threads do not survive fork(), so the asyncio loop in LOOP.thread is
    # dead in the child. Re-initialize the singleton in place so existing
    # `from .background_loop import LOOP` references in other modules see
    # the new state. The Rust-side tokio runtime is reset analogously by a
    # pthread_atfork hook installed in the _lancedb extension.
    LOOP._start()
    # The embedding executor's worker threads are dead in the child as well.
    # Replace it with a fresh pool (threads are spawned lazily, so this is
    # cheap); we don't shut down the old one, since joining its dead workers
    # could hang.
    global _EMBEDDING_EXECUTOR
    _EMBEDDING_EXECUTOR = _new_embedding_executor()
    global _FORK_WARNED
    if not _FORK_WARNED:
        _FORK_WARNED = True
        warnings.warn(
            "lancedb fork support is experimental: the internal async "
            "runtime has been reset in the forked child, but a small chance "
            "of deadlock remains if other state was mid-operation at fork "
            "time. The 'forkserver' or 'spawn' multiprocessing start method "
            "is likely a safer alternative.",
            RuntimeWarning,
            stacklevel=2,
        )


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def reset_background_loop() -> None:
    """Release the background event loop, the embedding executor and the
    Rust-side Tokio runtime, replacing them with fresh ones.

    Using LanceDB creates a Tokio runtime with its worker threads, this
    background event loop thread, and an embedding thread pool. All three
    are held for the entire life of the process, and the only thing that
    recycles them is the ``fork()`` handler -- so on POSIX it happens only
    as a side effect of forking, and on Windows, where there is no
    ``fork()``, it cannot happen at all.

    Call this between bursts of work to hand those threads back. On a
    typical Windows setup a process goes from 13 threads at baseline to 25
    after touching LanceDB, and back to 16 after calling this.

    Not everything is reclaimable from here: a few threads belong to a
    process-global runtime inside ``lance-core`` that is never reclaimed,
    and cached session data is untouched. Memory is barely affected either
    -- the resource this gives back is threads.

    Safe to call between operations; do not call while another thread may
    still be using ``LOOP.run(...)`` or an in-flight lancedb operation.
    """
    LOOP.reset()
    global _EMBEDDING_EXECUTOR
    old_executor = _EMBEDDING_EXECUTOR
    _EMBEDDING_EXECUTOR = _new_embedding_executor()
    # Unlike the post-fork path, these workers are alive, so abandoning them
    # would strand real threads for the life of the process -- the opposite
    # of what this call is for. `wait=False` lets them retire on their own
    # once their current task finishes, without blocking the caller here.
    old_executor.shutdown(wait=False)
    from . import _lancedb

    _lancedb.reset_runtime()
