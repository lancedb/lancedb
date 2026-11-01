# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
from datetime import timedelta

import pytest

from lancedb.job import AsyncJob, Job


class _PendingJob:
    id = "job-timeout"

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
