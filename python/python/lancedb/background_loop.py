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

    The loop and its thread are created by the first call to :meth:`run`, not
    by constructing this object, so importing lancedb does not leave an idle
    background thread behind.
    """

    def __init__(self):
        self.loop = None
        self.thread = None
        self._start_lock = threading.Lock()

    def _start(self):
        if self.thread is not None:
            return
        with self._start_lock:
            if self.thread is not None:
                return
            loop = asyncio.new_event_loop()
            thread = threading.Thread(
                target=loop.run_forever,
                name="LanceDBBackgroundEventLoop",
                daemon=True,
            )
            thread.start()
            # Publish the loop first and the thread last. Callers that reach
            # _start while this is in progress wait on _start_lock, so they
            # cannot observe a half-initialized loop/thread pair.
            self.loop = loop
            self.thread = thread

    def _reset_after_fork(self):
        # Threads do not survive fork(). Reset to the unstarted state without
        # calling into the inherited loop or thread: the thread is gone, and
        # it may have held _start_lock when the fork happened. A fresh lock
        # lets the child start a new loop on its first run().
        self.loop = None
        self.thread = None
        self._start_lock = threading.Lock()

    def run(self, future):
        self._start()
        concurrent_future = asyncio.run_coroutine_threadsafe(future, self.loop)
        try:
            return concurrent_future.result()
        except BaseException:
            concurrent_future.cancel()
            raise


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
    # Threads do not survive fork(), so the loop in LOOP.thread is dead in
    # the child. Reset the singleton in place so existing
    # `from .background_loop import LOOP` references in other modules see
    # the unstarted state and create a fresh loop on their first run(). Do
    # not restart it here: the child may never use LanceDB, and the at-fork
    # handler must not touch the vanished thread or acquire inherited locks.
    # The Rust-side tokio runtime is reset analogously by a pthread_atfork
    # hook installed in the _lancedb extension.
    LOOP._reset_after_fork()
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
