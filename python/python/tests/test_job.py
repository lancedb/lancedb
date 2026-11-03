# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from lancedb.exceptions import JobCancelledError, JobFailedError
from lancedb.job import AsyncJob, Job
from lancedb.remote.errors import HttpError, RetryError


@pytest.fixture(params=["async", "sync"])
def wait_job(request):
    async def wait(inner, timeout=None, result_decoder=None):
        job = AsyncJob(inner, result_decoder)
        if request.param == "sync":
            return await asyncio.to_thread(Job(job).wait, timeout)
        return await job.wait(timeout)

    return wait


def transport_error(kind):
    if kind == "http":
        return HttpError("error sending request", "req-1", is_transport_error=True)
    error = RetryError("retries exhausted", "req-1", 0, 3, 0, 3, 3, 3, None)
    error.__cause__ = transport_error("http")
    return error


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["http", "retry"])
async def test_wait_recovers_from_transport_errors(wait_job, kind, monkeypatch):
    inner = MagicMock(id="job-1", job_type=None)
    inner.wait = AsyncMock(side_effect=[transport_error(kind), '{"rows": 7}'])
    inner.status = AsyncMock(side_effect=[transport_error(kind), "running"])
    sleep = AsyncMock()
    monkeypatch.setattr("lancedb.job.asyncio.sleep", sleep)

    result = await wait_job(
        inner, timedelta(hours=24), result_decoder=lambda value: ("decoded", value)
    )

    assert result == ("decoded", '{"rows": 7}')
    assert inner.wait.await_count == 2
    assert inner.status.await_count == 2
    assert [call.args[0] for call in sleep.await_args_list] == [0.25, 0.5]
    inner.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_wait_disconnection_timeout_explains_how_to_reconnect(wait_job):
    error = transport_error("http")
    inner = MagicMock(id="job-1", job_type=None)
    inner.wait = AsyncMock(side_effect=error)
    inner.status = AsyncMock(side_effect=error)

    with pytest.raises(
        ConnectionError, match="job 'job-1' may still be running"
    ) as exc:
        await wait_job(inner, timedelta(milliseconds=10))

    assert "db.open_job('job-1')" in str(exc.value)
    assert "call wait()" in str(exc.value)
    assert exc.value.__cause__ is error
    inner.cancel.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        HttpError("unauthorized", "req-1", 401),
        HttpError("not found", "req-1", 404),
        HttpError(
            "secret not found",
            "req-1",
            404,
            error_code="not_found",
            resource_kind="secret",
        ),
        HttpError("failed to parse job description", "req-1"),
        RetryError("service unavailable", "req-1", 3, 0, 0, 3, 3, 3, 503),
        JobFailedError("worker failed"),
        JobCancelledError("job cancelled"),
    ],
)
async def test_wait_propagates_permanent_and_terminal_errors(wait_job, error):
    inner = MagicMock(id="job-1")
    inner.wait = AsyncMock(side_effect=error)

    with pytest.raises(type(error)) as exc:
        await wait_job(inner, timedelta(seconds=1))

    assert exc.value is error
    assert inner.wait.await_count == 1
    inner.status.assert_not_called()


@pytest.mark.asyncio
async def test_wait_running_job_timeout_is_not_a_connection_error(wait_job):
    async def running():
        await asyncio.Event().wait()

    inner = MagicMock(id="job-1")
    inner.wait = AsyncMock(side_effect=running)
    with pytest.raises(TimeoutError):
        await wait_job(inner, timedelta(milliseconds=10))
    inner.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_wait_completed_job(wait_job):
    assert await wait_job(None) is None


@pytest.mark.asyncio
async def test_wait_retries_without_a_timeout_and_caps_backoff(wait_job, monkeypatch):
    inner = MagicMock(id="job-1", job_type=None)
    inner.wait = AsyncMock(side_effect=[transport_error("http"), None])
    inner.status = AsyncMock(side_effect=[transport_error("retry")] * 9 + ["running"])
    sleep = AsyncMock()
    monkeypatch.setattr("lancedb.job.asyncio.sleep", sleep)

    assert await wait_job(inner) is None
    assert [call.args[0] for call in sleep.await_args_list] == [
        0.25,
        0.5,
        1,
        2,
        4,
        8,
        16,
        30,
        30,
        30,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("job_type", [None, "query"])
async def test_wait_timeout_after_recovery_is_not_a_connection_error(
    wait_job, monkeypatch, job_type
):
    async def running():
        await asyncio.Event().wait()

    inner = MagicMock(id="job-1", job_type=job_type)
    inner.wait = AsyncMock()

    async def wait():
        if inner.wait.await_count == 1:
            raise transport_error("http")
        return await running()

    inner.wait.side_effect = wait
    inner.status = AsyncMock(return_value="running")
    monkeypatch.setattr("lancedb.job.asyncio.sleep", AsyncMock())

    with pytest.raises(TimeoutError) as exc:
        await wait_job(inner, timedelta(milliseconds=10))
    message = str(exc.value)
    assert "still running after 0.01 seconds" in message
    assert "continues on the server" in message
    if job_type == "query":
        assert "keep this handle to continue waiting" in message
        assert "db.open_job" not in message
    else:
        assert "db.open_job('job-1').wait()" in message
    assert inner.status.await_count == 1
    assert inner.wait.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [JobFailedError("failed"), JobCancelledError("cancel")]
)
async def test_wait_reports_terminal_error_after_recovery(wait_job, monkeypatch, error):
    inner = MagicMock(id="job-1", job_type=None)
    inner.wait = AsyncMock(side_effect=[transport_error("retry"), error])
    inner.status = AsyncMock(return_value="running")
    monkeypatch.setattr("lancedb.job.asyncio.sleep", AsyncMock())

    with pytest.raises(type(error)) as exc:
        await wait_job(inner, timedelta(seconds=1))
    assert exc.value is error
    assert inner.wait.await_count == 2


@pytest.mark.asyncio
async def test_wait_cancellation_interrupts_backoff(monkeypatch):
    inner = MagicMock(id="job-1", job_type=None)
    inner.wait = AsyncMock(side_effect=transport_error("http"))
    sleeping = asyncio.Event()

    async def sleep(delay):
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("lancedb.job.asyncio.sleep", sleep)
    task = asyncio.create_task(AsyncJob(inner).wait())
    await asyncio.wait_for(sleeping.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    inner.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_query_wait_disconnection_does_not_suggest_open_job(wait_job):
    inner = MagicMock(id="query-1", job_type="query")
    inner.wait = AsyncMock(side_effect=transport_error("http"))

    with pytest.raises(ConnectionError) as exc:
        await wait_job(inner, timedelta(milliseconds=10))

    assert "query-1" in str(exc.value)
    assert "this job handle" in str(exc.value)
    assert "open_job" not in str(exc.value)


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
