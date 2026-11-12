# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Held diagnostic only: observe the two remote fork tests without changing them."""

import faulthandler
import multiprocessing.process
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

LOG_PATH = Path(os.environ["FORK_PROBE_LOG"])
LOG = os.open(LOG_PATH, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
ACTIVE = False
TARGETS = {
    "test_remote_connection_after_fork",
    "test_inherited_remote_table_reopens_after_fork",
}


def mark(message):
    os.write(
        LOG,
        f"{time.monotonic_ns()} pid={os.getpid()} tid={threading.get_ident()} "
        f"{message}\n".encode(),
    )


def profile(frame, event, arg):
    filename = frame.f_code.co_filename
    name = frame.f_code.co_name
    if event in ("c_call", "c_return", "c_exception"):
        # Only names, never arguments, locals, request headers or credentials.
        module = getattr(arg, "__module__", "") or ""
        if module.startswith("lancedb") or (
            "/lancedb/" in filename
            and getattr(arg, "__name__", "") in ("connect", "table_names", "count_rows")
        ):
            mark(f"NATIVE_BOUNDARY {event} {module}.{arg.__name__}")
        return
    if event not in ("call", "return"):
        return
    product = "/lancedb/" in filename and name in {
        "_start",
        "_reset_after_fork",
        "run",
        "connect",
        "connect_async",
        "__init__",
        "table_names",
        "count_rows",
    }
    process = "/multiprocessing/" in filename and name in {
        "_bootstrap",
        "run",
        "put",
        "_feed",
        "_finalize_join",
        "_finalize_close",
        "_exit_function",
        "_after_fork",
        "_flush_std_streams",
    }
    bridge = (
        "/asyncio/" in filename or "/concurrent/futures/" in filename
    ) and name in {
        "run_coroutine_threadsafe",
        "result",
        "run_forever",
        "_shutdown",
    }
    request = name in {"handler", "do_GET", "send_response"} and (
        filename.endswith("test_remote_db.py") or "/http/server.py" in filename
    )
    if (
        product
        or process
        or bridge
        or request
        or name
        in {
            "_remote_fork_child",
            "_remote_table_fork_child",
            "_shutdown",
        }
    ):
        # Coroutine call/return also denote resume/suspend, not necessarily completion.
        mark(f"{event} {filename}:{frame.f_lineno} {name}")


def early_child():
    if not ACTIVE:
        return
    mark("EARLY_CHILD")
    sys.setprofile(profile)
    threading.setprofile(profile)


original_register = os.register_at_fork
original_register(after_in_child=early_child)


def register(*, before=None, after_in_parent=None, after_in_child=None):
    kwargs = {}
    for key, callback in (
        ("before", before),
        ("after_in_parent", after_in_parent),
        ("after_in_child", after_in_child),
    ):
        if callback is None:
            continue

        def wrapped(cb=callback, phase=key):
            label = f"{getattr(cb, '__module__', '')}.{getattr(cb, '__qualname__', '')}"
            if ACTIVE:
                mark(f"ATFORK_ENTER {phase} {label}")
            cb()
            if ACTIVE:
                mark(f"ATFORK_EXIT {phase} {label}")

        kwargs[key] = wrapped
    return original_register(**kwargs)


os.register_at_fork = register
original_join = multiprocessing.process.BaseProcess.join


def snapshot_child(pid):
    snapshot = Path(f"{LOG_PATH}.proc-{pid}")
    rows = []
    try:
        tasks = list(Path(f"/proc/{pid}/task").iterdir())
        for task in tasks:
            for name in ("comm", "wchan", "stat", "status", "syscall", "stack"):
                try:
                    rows.append(f"{task / name} {(task / name).read_text()}")
                except OSError as error:
                    rows.append(f"{task / name}: {error}")
        snapshot.write_text("\n".join(rows))
        with Path(f"{snapshot}.gdb").open("w") as output:
            result = subprocess.run(
                [
                    "sudo",
                    "-n",
                    "timeout",
                    "--signal=KILL",
                    "4",
                    "gdb",
                    "-nx",
                    "-q",
                    "-batch",
                    "-iex",
                    "set debuginfod enabled off",
                    "-ex",
                    "set pagination off",
                    "-ex",
                    "set print frame-arguments none",
                    "-ex",
                    "thread apply all bt 24",
                    "-ex",
                    "detach",
                    "-p",
                    str(pid),
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=5,
                check=False,
            )
        mark(f"GDB_EXIT pid={pid} exit={result.returncode}")
    except (OSError, subprocess.TimeoutExpired) as error:
        # Collection errors must not replace the original test's failure/cleanup.
        mark(f"SNAPSHOT_ERROR pid={pid} {type(error).__name__}")


def join_with_snapshot(self, timeout=None):
    if not ACTIVE or timeout != 15:
        return original_join(self, timeout)
    started = time.monotonic()
    original_join(self, 7)
    if self.is_alive():
        mark(f"JOIN_CHILD_ALIVE_AT_7S pid={self.pid}")
        mark(f"PYTHON_STACK_REQUEST pid={self.pid}")
        try:
            os.kill(self.pid, signal.SIGUSR2)
        except ProcessLookupError:
            pass
        # Separate parent/child destinations keep concurrent C stack writes readable.
        with Path(f"{LOG_PATH}.parent-{os.getpid()}.python").open("w") as output:
            faulthandler.dump_traceback(file=output, all_threads=True)
        snapshot_child(self.pid)
    result = original_join(self, max(0, 15 - (time.monotonic() - started)))
    mark(
        f"JOIN_RETURN pid={self.pid} alive={self.is_alive()} "
        f"exit={self.exitcode} elapsed={time.monotonic() - started:.6f}"
    )
    return result


multiprocessing.process.BaseProcess.join = join_with_snapshot


def pytest_runtest_setup(item):
    global ACTIVE
    ACTIVE = item.name in TARGETS
    if ACTIVE:
        mark(f"TEST_SETUP {item.nodeid}")


def pytest_runtest_call(item):
    if ACTIVE:
        mark(f"TEST_CALL {item.nodeid}")
        # Installed before fork: even a child stuck before early_child can dump.
        faulthandler.register(signal.SIGUSR2, file=LOG, all_threads=True)
        # Child profiling continues through multiprocessing shutdown.
        sys.setprofile(profile)
        threading.setprofile(profile)


def pytest_runtest_teardown(item):
    global ACTIVE
    if ACTIVE:
        sys.setprofile(None)
        threading.setprofile(None)
        mark(f"TEST_TEARDOWN {item.nodeid}")
    ACTIVE = False


def pytest_collection_finish(session):
    # Retain complete selected order; never deselect, reorder or alter fixtures.
    Path(f"{LOG_PATH}.order").write_text(
        "\n".join(item.nodeid for item in session.items) + "\n"
    )
    identity_path = os.environ.get("FORK_PROBE_IDENTITY")
    if identity_path:
        import json

        expected = json.loads(Path(identity_path).read_text())["modules"]
        actual = {}
        for name, entry in expected.items():
            if not name.startswith("lancedb"):
                continue
            module = sys.modules.get(name)
            path = str(Path(module.__file__).resolve()) if module else None
            actual[name] = path
            if path != entry["path"]:
                raise RuntimeError(f"pytest import differs from verified wheel: {name}")
        Path(f"{LOG_PATH}.imports.json").write_text(json.dumps(actual, indent=2))
