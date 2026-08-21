# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for `lancedb.background_loop.reset_background_loop` /
`_lancedb.reset_runtime()` -- the opt-in mitigation for a Windows-specific
issue where the process-lifetime background event loop and Tokio runtime
never get recycled on their own outside of `fork()` (which doesn't exist on
Windows). See `background_loop.reset_background_loop` and
`_lancedb.reset_runtime` docstrings for the full explanation.
"""

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
    """Erro/borda: calling it several times in a row (e.g. a caller that
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
    handle open, which is exactly the resource this mitigation exists to
    release."""
    old_loop = LOOP.loop

    reset_background_loop()

    assert old_loop.is_closed()


def test_reset_timeout_raises_and_does_not_replace_the_loop():
    """Erro/borda: se a thread antiga não parar dentro do `join_timeout`,
    `reset()` levanta `RuntimeError` e NÃO troca `self.loop`/`self.thread`
    -- nunca deixa dois loops vivos ao mesmo tempo nem finge sucesso."""
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
    """Erro/borda (regressão do achado de review): dois `reset()`
    concorrentes não podem deixar uma thread órfã -- `LOOP.reset()`
    serializa a transição inteira (stop/join/close/`_start`) via
    `self._reset_lock`, então um sempre termina (ou levanta) antes do
    outro ler `self.loop`/`self.thread`."""
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


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="Regression target is Windows-specific (IOCP resource pressure); "
    "this is a stress test, not worth the runtime cost elsewhere.",
)
def test_many_connections_with_periodic_reset_does_not_hang(tmp_path):
    """Regression guard for the Windows hang this mitigation exists for:
    opens/closes many `connect_async` connections and tables in a single
    process, calling `reset_background_loop()` every N connections.

    Runs the workload in a **child process** that this test can actually
    time out and kill (`subprocess.run(..., timeout=...)`): a same-process
    daemon-thread watchdog can flag a hang but can't do anything about a
    stuck main thread, so it wouldn't actually stop a hung test -- pytest
    would just hang anyway.

    This does not reproduce the original hang on its own (a bare loop like
    this was confirmed clean even against the affected lancedb release) --
    it exists so that *if* the real trigger is narrowed down later, there's
    already a place to plug it in without the test suite silently hanging
    CI.
    """
    script = tmp_path / "_stress_workload.py"
    script.write_text(_STRESS_WORKLOAD)
    workload_dir = tmp_path / "dbs"
    workload_dir.mkdir()

    try:
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(script), str(workload_dir), "300", "20"],
            capture_output=True,
            text=True,
            timeout=60.0,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"hang detected: workload did not finish within 60s "
            f"(stdout={exc.stdout!r} stderr={exc.stderr!r})",
            pytrace=False,
        )

    assert result.returncode == 0, (
        f"workload subprocess failed: stdout={result.stdout!r} "
        f"stderr={result.stderr!r}"
    )
