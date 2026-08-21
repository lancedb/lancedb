# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for `lancedb.background_loop.reset_background_loop` /
`_lancedb.reset_runtime()` -- the opt-in mitigation for a Windows-specific
issue where the process-lifetime background event loop and Tokio runtime
never get recycled on their own outside of `fork()` (which doesn't exist on
Windows). See `background_loop.reset_background_loop` and
`_lancedb.reset_runtime` docstrings for the full explanation.
"""

import sys
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


@pytest.mark.skipif(
    sys.platform != "win32",
    reason="Regression target is Windows-specific (IOCP resource pressure); "
    "this is a stress/watchdog test, not worth the runtime cost elsewhere.",
)
@pytest.mark.asyncio
async def test_many_connections_with_periodic_reset_does_not_hang(tmp_path):
    """Regression guard for the Windows hang this mitigation exists for:
    opens/closes many `connect_async` connections and tables in a single
    process, calling `reset_background_loop()` every `_RESET_EVERY`
    connections, with a watchdog that fails the test loudly instead of
    hanging forever if something regresses.

    This does not reproduce the original hang on its own (a bare loop like
    this was confirmed clean even against the affected lancedb release) --
    it exists so that *if* the real trigger is narrowed down later, there's
    already a watchdog-protected place to plug it in without the test
    suite silently hanging CI.
    """
    import faulthandler
    import threading

    _N = 300
    _RESET_EVERY = 20
    _TIMEOUT_S = 60.0

    done = threading.Event()

    def _watchdog():
        if done.wait(timeout=_TIMEOUT_S):
            return
        faulthandler.dump_traceback(all_threads=True)
        pytest.fail(f"hang detected after {_TIMEOUT_S}s", pytrace=False)

    watchdog = threading.Thread(target=_watchdog, daemon=True)
    watchdog.start()

    try:
        start = time.monotonic()
        for i in range(_N):
            db = await lancedb.connect_async(str(tmp_path / f"db{i}"))
            schema = pa.schema([pa.field("id", pa.string())])
            table = await db.create_table("t", schema=schema)
            await table.add([{"id": "1"}])
            await table.count_rows()
            if (i + 1) % _RESET_EVERY == 0:
                reset_background_loop()
        elapsed = time.monotonic() - start
    finally:
        done.set()

    assert elapsed < _TIMEOUT_S, (
        f"completed but suspiciously slow ({elapsed:.1f}s) -- investigate "
        "before trusting this as a clean pass"
    )
