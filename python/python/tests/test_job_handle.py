# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""JobHandle.wait() terminal-state handling.

Regression coverage for the cluster backfill-failure hang: the server reports a
doomed job as ``state="failed"`` within seconds, but ``wait()`` used to ignore
``failed`` and block until its (default 3600s) timeout. These tests pin that a
``failed`` job raises ``JobFailedError`` promptly, carrying the server error.
"""

import asyncio
import time

import pytest

from lancedb.udf import JobHandle, AsyncJobHandle, JobFailedError


class FakeJobInfo:
    """Mirror of the pyo3 builtins.JobInfo fields wait()/status() read."""

    def __init__(self, state, error=None, committed=False, units_total=None):
        self.state = state
        self.error = error
        self.committed = committed
        self.units_total = units_total
        self.units_done = None
        self.job_id = "job-1"


class FakeConn:
    """get_job() walks a scripted list of JobInfo (or None) snapshots, holding
    the last one once exhausted, so wait() polls a deterministic timeline."""

    def __init__(self, snapshots):
        self._snaps = list(snapshots)
        self.calls = 0

    def get_job(self, job_id, table=None):
        snap = self._snaps[min(self.calls, len(self._snaps) - 1)]
        self.calls += 1
        return snap


class AsyncFakeConn(FakeConn):
    async def get_job(self, job_id, table=None):
        return FakeConn.get_job(self, job_id, table)


def test_wait_raises_on_failed_promptly():
    # pending -> failed: wait() must raise the server error, not TimeoutError.
    conn = FakeConn(
        [None, FakeJobInfo("failed", error="multi-column backfill needs a STRUCT")]
    )
    jh = JobHandle(conn, "job-1", table="t")
    t0 = time.monotonic()
    with pytest.raises(JobFailedError) as exc:
        jh.wait(timeout=30, poll=0.01)
    assert time.monotonic() - t0 < 5  # prompt, nowhere near the 30s timeout
    assert "STRUCT" in str(exc.value)
    assert exc.value.error == "multi-column backfill needs a STRUCT"
    assert exc.value.job_id == "job-1"


def test_wait_returns_finished_on_success():
    # running -> finished (job left the inflight listing) returns normally.
    conn = FakeConn([FakeJobInfo("running", units_total=2), None])
    jh = JobHandle(conn, "job-1", table="t")
    jh._seen = True  # already observed, so a None now means "finished" not grace
    assert jh.wait(timeout=30, poll=0.01) == "finished"


def test_wait_returns_finished_on_committed():
    # A committed job that is still listed resolves to finished.
    conn = FakeConn([FakeJobInfo("running", committed=True, units_total=2)])
    jh = JobHandle(conn, "job-1", table="t")
    jh._seen = True
    assert jh.wait(timeout=30, poll=0.01) == "finished"


def test_async_wait_raises_on_failed_promptly():
    conn = AsyncFakeConn([None, FakeJobInfo("failed", error="boom")])
    jh = AsyncJobHandle(conn, "job-1", table="t")

    async def run():
        t0 = time.monotonic()
        with pytest.raises(JobFailedError) as exc:
            await jh.wait(timeout=30, poll=0.01)
        assert time.monotonic() - t0 < 5
        assert exc.value.error == "boom"

    asyncio.run(run())
