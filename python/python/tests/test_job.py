# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
from datetime import timedelta

import pytest

from lancedb.job import AsyncJob, Job


class _PendingJob:
    id = "job-timeout"
    job_type = None

    async def wait(self):
        await asyncio.sleep(60)


class _PendingQuery:
    id = "query-timeout"
    job_type = "query"

    async def wait(self):
        await asyncio.sleep(60)


def _assert_timeout(error: TimeoutError) -> None:
    message = str(error)
    assert "job 'job-timeout' still running after 0.01 seconds" in message
    assert "continues on the server" in message
    assert "db.open_job('job-timeout').wait()" in message


@pytest.mark.asyncio
async def test_async_job_wait_timeout_explains_reattach():
    job = AsyncJob(_PendingJob())

    with pytest.raises(TimeoutError) as exc_info:
        await job.wait(timeout=timedelta(seconds=0.01))

    _assert_timeout(exc_info.value)


def test_job_wait_timeout_explains_reattach():
    job = Job(AsyncJob(_PendingJob()))

    with pytest.raises(TimeoutError) as exc_info:
        job.wait(timeout=timedelta(seconds=0.01))

    _assert_timeout(exc_info.value)


@pytest.mark.asyncio
async def test_query_wait_timeout_requires_keeping_original_handle():
    job = AsyncJob(_PendingQuery())

    with pytest.raises(TimeoutError) as exc_info:
        await job.wait(timeout=timedelta(seconds=0.01))

    message = str(exc_info.value)
    assert "query 'query-timeout' still running after 0.01 seconds" in message
    assert "continues on the server" in message
    assert "keep this handle to continue waiting" in message
    assert "db.open_job" not in message
