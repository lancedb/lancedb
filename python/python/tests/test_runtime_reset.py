# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for `lancedb.background_loop.reset_background_loop` /
`_lancedb.reset_runtime()` -- the opt-in way to release the process-lifetime
background event loop, embedding executor and Tokio runtime without exiting
the process. Outside of `fork()` (which does not exist on Windows) nothing
recycles them on its own. See the `background_loop.reset_background_loop`
and `_lancedb.reset_runtime` docstrings for the full explanation.
"""

import os
import subprocess
import sys
import threading
import time

import lancedb
import pyarrow as pa
import pytest
from lancedb.background_loop import LOOP, reset_background_loop


@pytest.mark.asyncio
async def test_reset_background_loop_still_works_afterwards(tmp_path):
    """Happy path, cross-platform: resetting mid-way through a sequence of
    operations doesn't break subsequent operations, and the old background
    thread is actually gone (a fresh one replaces it)."""
    old_thread = LOOP.thread
    db = await lancedb.connect_async(str(tmp_path))
    schema = pa.schema([pa.field("id", pa.string())])
    await db.create_table("before_reset", schema=schema)

    reset_background_loop()

    assert LOOP.thread is not old_thread
    assert not old_thread.is_alive()
    assert LOOP.thread.is_alive()

    # The runtime/loop being fresh must not affect functionality -- new
    # connections and tables work exactly as before.
    db2 = await lancedb.connect_async(str(tmp_path))
    await db2.create_table("after_reset", schema=schema)
    names = await db2.table_names()
    assert set(names) == {"before_reset", "after_reset"}


def test_reset_background_loop_is_idempotent_and_safe_to_call_repeatedly():
    """Error case: calling it several times in a row (e.g. a caller that
    resets after every batch) must not raise, deadlock, or leak threads
    that stay alive."""
    seen_threads = {LOOP.thread}
    for _ in range(5):
        reset_background_loop()
        seen_threads.add(LOOP.thread)

    # Each reset must have produced a distinct, live thread, and none of
    # the previous ones should still be running.
    assert len(seen_threads) == 6
    alive = [t for t in seen_threads if t.is_alive()]
    assert alive == [LOOP.thread]


def test_reset_closes_the_old_loop():
    """The old loop must be genuinely `close()`d, not just have its thread
    stopped -- an unclosed `ProactorEventLoop` keeps its IOCP selector
    handle open, so stopping without closing would leave the very resource
    this call exists to release still held."""
    old_loop = LOOP.loop

    reset_background_loop()

    assert old_loop.is_closed()


def test_reset_timeout_raises_and_does_not_replace_the_loop():
    """Error case: if the old thread does not stop within `join_timeout`,
    `reset()` raises `RuntimeError` and leaves `self.loop`/`self.thread`
    untouched -- it never ends up with two live loops at once, and never
    reports success it did not achieve."""
    original_loop = LOOP.loop
    original_thread = LOOP.thread

    # Block the loop's own thread with a callback that runs synchronously
    # for longer than the timeout -- it's already queued (and running)
    # by the time `reset()` schedules `stop()`, so the thread can't react
    # to `stop()` before the timeout elapses.
    block_done = threading.Event()

    def _block():
        time.sleep(1.0)
        block_done.set()

    original_loop.call_soon_threadsafe(_block)

    with pytest.raises(RuntimeError, match="did not stop"):
        LOOP.reset(join_timeout=0.01)

    assert LOOP.loop is original_loop
    assert LOOP.thread is original_thread
    assert original_thread.is_alive()

    # Let the blocking callback finish, then do a real reset so this test
    # doesn't leave a stuck thread behind for the rest of the suite.
    block_done.wait(timeout=5.0)
    reset_background_loop()


def test_concurrent_resets_do_not_orphan_threads():
    """Error case: two concurrent `reset()` calls must not orphan a thread.
    `LOOP.reset()` serializes the whole transition (stop/join/close/`_start`)
    behind `self._reset_lock`, so one always finishes (or raises) before the
    other reads `self.loop`/`self.thread`."""
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def _reset():
        barrier.wait(timeout=5.0)
        try:
            LOOP.reset()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_reset) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, errors
    # Exactly one live LanceDBBackgroundEventLoop thread should remain --
    # the current LOOP.thread -- with nothing orphaned from either reset.
    live_bg_threads = [
        t for t in threading.enumerate() if t.name == "LanceDBBackgroundEventLoop"
    ]
    assert live_bg_threads == [LOOP.thread]


def test_reset_shuts_down_the_old_embedding_executor():
    """The old embedding pool must be shut down, not just replaced.

    Its workers are alive at this point (unlike the post-fork path, where
    they are already dead and joining them could hang), so abandoning the
    pool would strand real threads for the life of the process -- the exact
    opposite of what this call is for.
    """
    from lancedb import background_loop

    old_executor = background_loop.embedding_executor()
    # Force a worker to actually exist, so "shut down" is observable rather
    # than vacuously true for an empty pool.
    old_executor.submit(lambda: None).result(timeout=5.0)

    reset_background_loop()

    assert background_loop.embedding_executor() is not old_executor
    # A shut-down executor refuses new work; an abandoned one would accept
    # it and keep its threads alive.
    with pytest.raises(RuntimeError):
        old_executor.submit(lambda: None)


_STRESS_WORKLOAD = """
import asyncio
import sys

import lancedb
import pyarrow as pa
from lancedb.background_loop import reset_background_loop


async def main(tmp_dir: str, n: int, reset_every: int) -> None:
    schema = pa.schema([pa.field("id", pa.string())])
    for i in range(n):
        db = await lancedb.connect_async(f"{tmp_dir}/db{i}")
        table = await db.create_table("t", schema=schema)
        await table.add([{"id": "1"}])
        await table.count_rows()
        if (i + 1) % reset_every == 0:
            reset_background_loop()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])))
"""


def test_resetting_repeatedly_under_load_stays_healthy(tmp_path):
    """Resetting while real work keeps arriving must not deadlock.

    This is the pattern the API is for -- a long-lived process resetting
    between batches -- and it is the one that would expose a mistake in the
    reader-quiescence handshake: a retirement has to wait only for readers
    of the generation it retired, so traffic on the fresh generation must
    never be able to hold it up.

    Runs in a **child process** that this test can actually time out and
    kill (`subprocess.run(..., timeout=...)`). A same-process watchdog
    thread could report a deadlock but not escape one, so pytest would hang
    regardless of what it detected.
    """
    script = tmp_path / "_stress_workload.py"
    script.write_text(_STRESS_WORKLOAD)
    workload_dir = tmp_path / "dbs"
    workload_dir.mkdir()

    try:
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(script), str(workload_dir), "150", "10"],
            capture_output=True,
            text=True,
            timeout=120.0,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"deadlock detected: workload did not finish within 120s "
            f"(stdout={exc.stdout!r} stderr={exc.stderr!r})",
            pytrace=False,
        )

    assert result.returncode == 0, (
        f"workload subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )


_FORK_LOCK_SCRIPT = """
import os
import sys
import threading

from lancedb.background_loop import LOOP

held = threading.Event()
release = threading.Event()


def holder():
    with LOOP._reset_lock:
        held.set()
        release.wait()


t = threading.Thread(target=holder)
t.start()
held.wait(timeout=5)

pid = os.fork()
if pid == 0:
    # Child: the lock above is inherited in a possibly-"locked" state --
    # the thread holding it in the parent doesn't exist here.
    # _reset_after_fork (registered via os.register_at_fork) must have
    # already replaced LOOP._reset_lock with a fresh one, or this hangs.
    try:
        LOOP.reset()
    except Exception as exc:
        print(f"CHILD_FAIL: {exc!r}", flush=True)
        os._exit(1)
    print("CHILD_OK", flush=True)
    os._exit(0)
else:
    release.set()
    t.join(timeout=5)
    _, status = os.waitpid(pid, 0)
    sys.exit(0 if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0 else 1)
"""


@pytest.mark.skipif(
    not hasattr(os, "fork"),
    reason="fork() doesn't exist on Windows -- this regresses the POSIX "
    "at-fork lock-reset path specifically.",
)
def test_reset_lock_is_fresh_in_forked_child_even_if_held_by_a_vanished_thread(
    tmp_path,
):
    """Regression: a `threading.Lock` held by a *different* thread at
    `fork()` time is inherited by the child in a possibly-locked state
    that thread can never release -- `_reset_after_fork` must replace
    `LOOP._reset_lock` with a fresh one, or every `reset()` call in the
    child (including this test's own, run in an actual forked child
    process) blocks forever."""
    script = tmp_path / "_fork_lock_script.py"
    script.write_text(_FORK_LOCK_SCRIPT)

    try:
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=30.0,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"hang detected: fork+reset did not finish within 30s "
            f"(stdout={exc.stdout!r} stderr={exc.stderr!r})",
            pytrace=False,
        )

    assert result.returncode == 0, (
        f"fork+reset script failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
