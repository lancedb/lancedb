# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import subprocess
import sys
import textwrap

import pytest


BACKGROUND_THREAD_NAME = "LanceDBBackgroundEventLoop"


def _run_in_subprocess(code: str, timeout: int = 30) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )


def _assert_subprocess_ok(result: subprocess.CompletedProcess) -> None:
    assert result.returncode == 0, result.stderr or result.stdout


def test_import_and_inspection_do_not_start_background_loop():
    result = _run_in_subprocess(
        f"""
        import threading

        import lancedb
        from lancedb.background_loop import BackgroundEventLoop, LOOP

        def background_threads():
            return [
                thread
                for thread in threading.enumerate()
                if thread.name == {BACKGROUND_THREAD_NAME!r}
            ]

        assert LOOP.thread is None
        assert LOOP.loop is None
        assert background_threads() == []

        # Inspecting a fresh instance is also side-effect free.
        loop = BackgroundEventLoop()
        assert loop.thread is None
        assert loop.loop is None
        assert background_threads() == []
        """
    )
    _assert_subprocess_ok(result)


def test_first_sync_call_starts_one_daemon_thread():
    result = _run_in_subprocess(
        f"""
        import threading

        import lancedb
        from lancedb.background_loop import LOOP

        def background_threads():
            return [
                thread
                for thread in threading.enumerate()
                if thread.name == {BACKGROUND_THREAD_NAME!r}
            ]

        assert LOOP.thread is None
        assert LOOP.loop is None

        db = lancedb.connect("memory://")
        assert db.table_names() == []

        worker = LOOP.thread
        assert worker is not None
        assert worker is background_threads()[0]
        assert len(background_threads()) == 1
        assert worker.daemon
        assert worker.is_alive()
        assert LOOP.loop is not None
        """
    )
    _assert_subprocess_ok(result)


def test_concurrent_first_use_initializes_one_background_thread():
    result = _run_in_subprocess(
        f"""
        import asyncio
        import threading

        from lancedb.background_loop import LOOP

        worker_count = 8
        barrier = threading.Barrier(worker_count)
        results = []
        errors = []
        results_lock = threading.Lock()

        def submit(index):
            try:
                barrier.wait(timeout=10)
                value = LOOP.run(asyncio.sleep(0.05, result=index))
                with results_lock:
                    results.append(value)
            except BaseException as exc:
                with results_lock:
                    errors.append(repr(exc))

        threads = [
            threading.Thread(target=submit, args=(index,))
            for index in range(worker_count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert sorted(results) == list(range(worker_count))

        background_threads = [
            thread
            for thread in threading.enumerate()
            if thread.name == {BACKGROUND_THREAD_NAME!r}
        ]
        assert len(background_threads) == 1
        assert background_threads[0].daemon
        """
    )
    _assert_subprocess_ok(result)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason=(
        "fork() is unavailable on Windows and unsafe on macOS "
        "(Apple frameworks/TLS are not fork-safe)"
    ),
)
def test_forked_child_starts_background_loop_lazily():
    # Fork from a fresh interpreter: earlier tests in a shared pytest process
    # may have left native runtime state that is not part of this scenario.
    result = _run_in_subprocess(
        f"""
        import asyncio
        import multiprocessing as mp
        import threading
        import traceback

        from lancedb.background_loop import LOOP

        BACKGROUND_THREAD_NAME = {BACKGROUND_THREAD_NAME!r}

        def background_threads():
            return [
                thread
                for thread in threading.enumerate()
                if thread.name == BACKGROUND_THREAD_NAME
            ]

        def fork_child(queue):
            try:
                from lancedb.background_loop import LOOP

                inherited_thread_is_none = LOOP.thread is None
                inherited_loop_is_none = LOOP.loop is None
                background_threads_before = [
                    thread.name for thread in background_threads()
                ]

                import lancedb

                db = lancedb.connect("memory://")
                tables = db.table_names()

                worker = LOOP.thread
                queue.put(
                    dict(
                        error=None,
                        inherited_thread_is_none=inherited_thread_is_none,
                        inherited_loop_is_none=inherited_loop_is_none,
                        background_threads_before=background_threads_before,
                        tables=tables,
                        thread_alive=False if worker is None else worker.is_alive(),
                        thread_daemon=False if worker is None else worker.daemon,
                        background_thread_count=len(background_threads()),
                    )
                )
            except BaseException:
                queue.put(dict(error=traceback.format_exc()))

        assert LOOP.run(asyncio.sleep(0, result="parent-before")) == "parent-before"
        parent_thread = LOOP.thread
        assert parent_thread is not None

        ctx = mp.get_context("fork")
        queue = ctx.Queue()

        # Simulate forking while another thread held the initialization lock. The
        # child must replace the inherited lock instead of trying to acquire it.
        LOOP._start_lock.acquire()
        try:
            proc = ctx.Process(target=fork_child, args=(queue,))
            proc.start()
        finally:
            LOOP._start_lock.release()

        proc.join(timeout=20)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                proc.kill()
                proc.join()
            raise AssertionError("Forked child hung starting the background loop")

        assert proc.exitcode == 0, "child exited with code %s" % proc.exitcode
        payload = queue.get(timeout=5)
        queue.close()
        assert payload["error"] is None, payload["error"]
        assert payload["inherited_thread_is_none"] is True
        assert payload["inherited_loop_is_none"] is True
        assert payload["background_threads_before"] == []
        assert payload["tables"] == []
        assert payload["thread_alive"] is True
        assert payload["thread_daemon"] is True
        assert payload["background_thread_count"] == 1

        # The child's reset must not disturb the parent's loop.
        assert LOOP.thread is parent_thread
        assert parent_thread.is_alive()
        assert LOOP.run(asyncio.sleep(0, result="parent-after")) == "parent-after"
        """,
        # join(20) + terminate join(5) + queue get(5), plus startup and traceback.
        timeout=40,
    )
    _assert_subprocess_ok(result)
