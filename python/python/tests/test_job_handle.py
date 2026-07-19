# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Job / AsyncJob against the platform jobs API.

The reference resolves its submission (manifest) id to a platform job id,
then polls describe for registry-backed state: terminal states are
first-class (DONE / FAILED / CANCELLED), progress comes from the
owner-written status payload, and a failed job raises ``JobFailedError``
promptly with the server error.
"""

import asyncio
import json
import time

import pytest

from lancedb.udf import Job, AsyncJob, JobFailedError


class FakeDescription:
    """Mirror of the pyo3 PlatformJobDescription fields the Job reads."""

    def __init__(self, job_state, status=None):
        self.job_id = "plat-1"
        self.job_type = "indexer"
        self.job_subtype = "udf"
        self.job_state = job_state
        self.creation_ms = 0
        self.status_json = json.dumps(status if status is not None else {})


class FakeConn:
    """Scripted timeline: resolve returns None until `resolve_after` calls,
    then the platform id; describe walks a list of descriptions (holding the
    last once exhausted)."""

    def __init__(self, descriptions, resolve_after=0):
        self._descs = list(descriptions)
        self._resolve_after = resolve_after
        self.resolve_calls = 0
        self.describe_calls = 0
        self.cancelled = []

    def resolve_platform_job_id(self, manifest_job_id, table=None):
        self.resolve_calls += 1
        if self.resolve_calls <= self._resolve_after:
            return None
        return "plat-1"

    def describe_platform_job(self, platform_job_id):
        assert platform_job_id == "plat-1"
        snap = self._descs[min(self.describe_calls, len(self._descs) - 1)]
        self.describe_calls += 1
        return snap

    def cancel_platform_job(self, platform_job_id):
        self.cancelled.append(platform_job_id)


class AsyncFakeConn(FakeConn):
    async def resolve_platform_job_id(self, manifest_job_id, table=None):
        return FakeConn.resolve_platform_job_id(self, manifest_job_id, table)

    async def describe_platform_job(self, platform_job_id):
        return FakeConn.describe_platform_job(self, platform_job_id)

    async def cancel_platform_job(self, platform_job_id):
        return FakeConn.cancel_platform_job(self, platform_job_id)


def test_status_maps_platform_states():
    for wire, want in [
        ("IN_PROGRESS", "running"),
        ("DONE", "finished"),
        ("FAILED", "failed"),
        ("CANCELLED", "cancelled"),
    ]:
        job = Job(FakeConn([FakeDescription(wire)]), "job-1", table="t")
        assert job.status() == want


def test_status_pending_before_resolution():
    job = Job(FakeConn([], resolve_after=10_000), "job-1", table="t")
    assert job.status() == "pending"


def test_progress_from_status_payload():
    conn = FakeConn(
        [
            FakeDescription(
                "IN_PROGRESS",
                status={"units_done": 3, "units_total": 8, "rows_committed": 100},
            )
        ]
    )
    job = Job(conn, "job-1", table="t")
    assert job.progress() == (3, 8)


def test_progress_none_for_uri_only_status():
    # Older records carry the status-store URI string, not a payload.
    desc = FakeDescription("IN_PROGRESS")
    desc.status_json = json.dumps("s3://bucket/job/job_status")
    job = Job(FakeConn([desc]), "job-1", table="t")
    assert job.progress() is None


def test_wait_raises_on_failed_promptly():
    conn = FakeConn(
        [
            FakeDescription("IN_PROGRESS"),
            FakeDescription(
                "FAILED", status={"error": "multi-column backfill needs a STRUCT"}
            ),
        ]
    )
    job = Job(conn, "job-1", table="t")
    t0 = time.monotonic()
    with pytest.raises(JobFailedError) as exc:
        job.wait(timeout=30, poll=0.01)
    assert time.monotonic() - t0 < 5  # prompt, nowhere near the 30s timeout
    assert "STRUCT" in str(exc.value)
    assert exc.value.error == "multi-column backfill needs a STRUCT"
    assert exc.value.job_id == "job-1"


def test_wait_returns_finished_on_done():
    conn = FakeConn([FakeDescription("IN_PROGRESS"), FakeDescription("DONE")])
    job = Job(conn, "job-1", table="t")
    assert job.wait(timeout=30, poll=0.01) == "finished"


def test_wait_returns_cancelled():
    conn = FakeConn([FakeDescription("CANCELLED")])
    job = Job(conn, "job-1", table="t")
    assert job.wait(timeout=30, poll=0.01) == "cancelled"


def test_wait_raises_when_job_never_registers():
    # An unresolved job past the grace window is a lost submission, not an
    # eternal "pending" hang.
    conn = FakeConn([], resolve_after=10_000)
    job = Job(conn, "job-1", table="t")
    job.GRACE_SECONDS = 0.05
    job._created = time.monotonic() - 1.0
    with pytest.raises(JobFailedError) as exc:
        job.wait(timeout=5, poll=0.01)
    assert "registry" in str(exc.value)


def test_cancel_resolves_then_cancels():
    conn = FakeConn([FakeDescription("IN_PROGRESS")], resolve_after=1)
    job = Job(conn, "job-1", table="t")
    job.cancel()
    assert conn.cancelled == ["plat-1"]


def test_async_wait_raises_on_failed_promptly():
    conn = AsyncFakeConn(
        [FakeDescription("FAILED", status={"error": "boom"})],
    )
    job = AsyncJob(conn, "job-1", table="t")

    async def run():
        t0 = time.monotonic()
        with pytest.raises(JobFailedError) as exc:
            await job.wait(timeout=30, poll=0.01)
        assert time.monotonic() - t0 < 5
        assert exc.value.error == "boom"

    asyncio.run(run())


def test_async_wait_returns_finished():
    conn = AsyncFakeConn([FakeDescription("IN_PROGRESS"), FakeDescription("DONE")])
    job = AsyncJob(conn, "job-1", table="t")

    async def run():
        assert await job.wait(timeout=30, poll=0.01) == "finished"

    asyncio.run(run())
