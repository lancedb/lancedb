# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import json
from datetime import timedelta

import lancedb
import pytest

from lancedb.job import AsyncJob, Job
from lancedb.index import BTree
from lancedb.remote.table import RemoteTable
from lancedb.table import AsyncTable, LanceTable


class FakeTable:
    def __init__(self, *descriptions):
        self.descriptions = list(descriptions)
        self.cancelled = []

    async def _describe_job(self, job_id):
        description = (
            self.descriptions.pop(0)
            if len(self.descriptions) > 1
            else self.descriptions[0]
        )
        return json.dumps({"job_id": job_id, **description})

    async def _cancel_job(self, job_id):
        self.cancelled.append(job_id)


class FakeCreateIndexTable(FakeTable):
    def __init__(self, job_id):
        super().__init__({"job_state": "IN_PROGRESS"})
        self.job_id = job_id
        self.closed = False

    def name(self):
        return "test_table"

    def _clone(self):
        return FakeCreateIndexTable(self.job_id)

    def close(self):
        self.closed = True

    async def create_index(self, *args, **kwargs):
        return self.job_id

    async def _describe_job(self, job_id):
        if self.closed:
            raise RuntimeError("table is closed")
        return await super()._describe_job(job_id)


class SlowDescribeTable(FakeTable):
    async def _describe_job(self, job_id):
        await asyncio.sleep(1)
        return await super()._describe_job(job_id)


def test_job_handles_are_exported():
    assert lancedb.Job is Job
    assert lancedb.AsyncJob is AsyncJob


@pytest.mark.asyncio
async def test_async_job_status_and_progress():
    table = FakeTable(
        {"job_state": "IN_PROGRESS", "status": None},
        {
            "job_state": "IN_PROGRESS",
            "status": {"rows_processed": 50, "total_rows": 200},
        },
    )
    job = AsyncJob(table, "j1_test")

    assert job.id == "j1_test"
    assert await job.status() == "in_progress"
    assert await job.progress() == {"rows_processed": 50, "total_rows": 200}
    assert repr(job) == "AsyncJob(id='j1_test')"


@pytest.mark.asyncio
async def test_async_job_waits_until_done():
    table = FakeTable(
        {"job_state": "IN_PROGRESS"},
        {"job_state": "IN_PROGRESS"},
        {"job_state": "DONE"},
    )
    job = AsyncJob(table, "j1_test")

    await job.wait(poll_interval=timedelta(milliseconds=1))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "message"),
    [
        ("FAILED", "Job j1_test failed"),
        ("CANCELLED", "Job j1_test was cancelled"),
    ],
)
async def test_async_job_wait_raises_for_unsuccessful_terminal_state(state, message):
    job = AsyncJob(
        FakeTable({"job_state": state, "status": {"error": "boom"}}), "j1_test"
    )

    with pytest.raises(RuntimeError, match=message):
        await job.wait()


@pytest.mark.asyncio
async def test_async_job_wait_timeout_and_argument_validation():
    job = AsyncJob(FakeTable({"job_state": "IN_PROGRESS"}), "j1_test")

    with pytest.raises(TimeoutError, match="Timed out waiting for job j1_test"):
        await job.wait(timeout=timedelta(0))
    with pytest.raises(TimeoutError, match="Timed out waiting for job j1_test"):
        await asyncio.wait_for(
            job.wait(
                timeout=timedelta(milliseconds=10),
                poll_interval=timedelta(seconds=1),
            ),
            timeout=0.2,
        )
    with pytest.raises(ValueError, match="timeout must be non-negative"):
        await job.wait(timeout=timedelta(seconds=-1))
    with pytest.raises(ValueError, match="poll_interval must be positive"):
        await job.wait(poll_interval=timedelta(0))


@pytest.mark.asyncio
async def test_async_job_wait_timeout_bounds_describe_request():
    job = AsyncJob(SlowDescribeTable({"job_state": "IN_PROGRESS"}), "j1_test")

    with pytest.raises(TimeoutError, match="Timed out waiting for job j1_test"):
        await asyncio.wait_for(
            job.wait(timeout=timedelta(milliseconds=10)),
            timeout=0.2,
        )


@pytest.mark.asyncio
async def test_async_job_cancel():
    table = FakeTable({"job_state": "IN_PROGRESS"})
    job = AsyncJob(table, "j1_test")

    await job.cancel()

    assert table.cancelled == ["j1_test"]


def test_job_sync_interface():
    table = FakeTable(
        {
            "job_state": "DONE",
            "status": {"rows_processed": 200, "total_rows": 200},
        }
    )
    job = Job(AsyncJob(table, "j1_test"))

    assert job.id == "j1_test"
    assert job.status() == "done"
    assert job.progress() == {"rows_processed": 200, "total_rows": 200}
    job.wait()
    job.cancel()
    assert table.cancelled == ["j1_test"]
    assert repr(job) == "Job(id='j1_test')"


@pytest.mark.asyncio
@pytest.mark.parametrize("job_id", ["j1_test", None])
async def test_async_table_wraps_create_index_job_id(job_id):
    table = AsyncTable(FakeCreateIndexTable(job_id))

    job = await table.create_index("vector")

    if job_id is None:
        assert job is None
    else:
        assert isinstance(job, AsyncJob)
        assert job.id == job_id


@pytest.mark.asyncio
async def test_async_job_survives_originating_table_close():
    inner = FakeCreateIndexTable("j1_test")
    table = AsyncTable(inner)

    job = await table.create_index("vector")
    table.close()

    assert inner.closed
    assert await job.status() == "in_progress"


def test_remote_table_wraps_async_create_index_job():
    table = AsyncTable(FakeCreateIndexTable("j1_test"))
    remote_table = RemoteTable(table, "test_db")

    job = remote_table.create_index("vector")

    assert isinstance(job, Job)
    assert job.id == "j1_test"


def test_lance_table_wraps_async_create_index_job():
    table = LanceTable.__new__(LanceTable)
    table._table = AsyncTable(FakeCreateIndexTable("j1_test"))

    job = table.create_index("vector", config=BTree())

    assert isinstance(job, Job)
    assert job.id == "j1_test"
    assert job.status() == "in_progress"
