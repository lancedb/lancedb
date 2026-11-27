# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import json
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pyarrow as pa

from lancedb.exceptions import JobCancelledError, JobFailedError
from lancedb.db import DBConnection
from lancedb.job import AsyncJob, Job, JobState, _progress_eta
from lancedb.remote.errors import HttpError, RetryError


@pytest.mark.parametrize("job_class", [AsyncJob, Job])
def test_job_handle_data_access_uses_methods(job_class):
    assert all(
        name.startswith("_")
        for name, member in vars(job_class).items()
        if isinstance(member, property)
    )
    job = job_class(None)
    expected = {
        "id": None,
        "state": "finished",
        "job_type": None,
        "creation_ms": None,
        "spec": None,
        "result": None,
        "failure": None,
    }
    for name, value in expected.items():
        assert callable(getattr(job, name))
        assert getattr(job, name)() == value
        assert getattr(job, f"_{name}") == value


@pytest.fixture(params=[False, True], ids=["async", "sync"])
def job_snapshot(request):
    def create(inner):
        job = AsyncJob(inner)
        if request.param:
            job = Job(job)

        async def call(method="checkout_latest"):
            if request.param:
                return await asyncio.to_thread(getattr(job, method))
            return await getattr(job, method)()

        return job, call

    return create


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["checkout_latest", "refresh"])
@pytest.mark.parametrize("state", ["finished", "failed", "cancelled", "future_state"])
async def test_job_checkout_latest_refreshes_the_complete_snapshot(
    job_snapshot, method, state
):
    spec = {"column": "vector", "initialization": list(range(1000))}
    result = {"rows_assigned": 1000} if state == "finished" else None
    failure = SimpleNamespace(message="worker failed") if state == "failed" else None
    description = SimpleNamespace(
        creation_ms=2000, spec=spec, result=result, failure=failure
    )
    inner = SimpleNamespace(
        id="operation-1",
        _state=None,
        _description=None,
        job_type=None,
        status=AsyncMock(),
    )

    async def observe():
        inner._state = state
        inner.job_type = "refresh_column"
        inner._description = description
        inner.job_id = "attempt-2"
        inner.operation_id = "operation-1"
        inner.attempt = 2
        inner.retry_of = "attempt-1"
        inner.lifecycle_state = str(JobState(state))
        inner.source_table_version = 7
        inner.execution_epoch = 3

    inner.refresh = AsyncMock(side_effect=observe)
    job, call = job_snapshot(inner)
    assert job.state() is None
    assert job.job_id() == "operation-1"
    assert job.lifecycle_state() is None
    assert job.job_type() is None
    assert job.creation_ms() is None
    assert job.spec() is None
    assert job.result() is None
    assert job.failure() is None
    inner.refresh.assert_not_called()

    assert await call(method) is None
    assert job.state() == state
    assert job.job_type() == "refresh_column"
    assert job.creation_ms() == 2000
    assert job.spec() is spec
    assert job.result() is result
    assert job.failure() is failure
    assert job.id() == "operation-1"
    assert job.job_id() == "attempt-2"
    assert job.operation_id() == "operation-1"
    assert job.attempt() == 2
    assert job.retry_of() == "attempt-1"
    assert job.lifecycle_state() == JobState(state)
    assert job.source_table_version() == 7
    assert job.execution_epoch() == 3
    inner.refresh.assert_awaited_once_with()
    inner.status.assert_not_called()


@pytest.mark.asyncio
async def test_job_checkout_latest_keeps_cached_details_on_request_error(job_snapshot):
    error = HttpError("not found", "req-job", 404)
    description = SimpleNamespace(
        creation_ms=2000, spec={"column": "vector"}, result=None, failure=None
    )
    inner = SimpleNamespace(
        _state="running",
        _description=description,
        job_type="refresh_column",
        refresh=AsyncMock(side_effect=error),
    )
    job, call = job_snapshot(inner)

    with pytest.raises(HttpError) as exc:
        await call()
    assert exc.value is error
    assert job.state() == "running"
    assert job.spec() is description.spec
    assert job.result() is None
    assert job.failure() is None
    inner.refresh.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_job_checkout_latest_preserves_refresh_overrides(sync, monkeypatch):
    job = AsyncJob(None)
    if sync:
        job = Job(job)
    refresh = MagicMock() if sync else AsyncMock()
    monkeypatch.setattr(job, "refresh", refresh)

    if sync:
        assert await asyncio.to_thread(job.checkout_latest) is None
    else:
        assert await job.checkout_latest() is None
        refresh.assert_awaited_once_with()
    refresh.assert_called_once_with()


@pytest.mark.asyncio
async def test_job_checkout_latest_for_completed_local_jobs(job_snapshot):
    job, call = job_snapshot(None)
    assert await call() is None
    assert job.state() == "finished"
    assert job.job_type() is None
    assert job.spec() is None
    assert job.result() is None
    assert job.failure() is None
    assert Job(None).checkout_latest() is None


@pytest.mark.parametrize("sync", [False, True])
def test_local_jobs_retain_identity_and_typed_state(sync):
    job = AsyncJob(None)
    if sync:
        job = Job(job)
    assert job.job_id() == job.operation_id()
    assert job.job_id() == job.job_id()
    assert job.attempt() == 1
    assert job.retry_of() is None
    assert job.lifecycle_state() is JobState.SUCCEEDED


def test_unknown_job_state_is_preserved_without_becoming_success():
    state = JobState("future_publication_state")
    assert state.value == "future_publication_state"
    assert str(state) == "future_publication_state"
    assert not state.is_terminal
    assert not state.is_success


@pytest.mark.asyncio
async def test_history_follows_page_tokens():
    first = pa.table({"event_seq": [2, 1]}).replace_schema_metadata(
        {b"lancedb.job_coordinator.page_token": b"next"}
    )
    last = pa.table({"event_seq": [0]})
    inner = SimpleNamespace(events=AsyncMock(side_effect=[first, last]))
    pages = [page async for page in AsyncJob(inner).history(limit=2)]
    assert [page["event_seq"].to_pylist() for page in pages] == [[2, 1], [0]]
    assert inner.events.call_args_list[1].kwargs["page_token"] == "next"


@pytest.mark.parametrize("sync", [False, True])
def test_job_repr_bounds_large_payloads_without_fetching(sync):
    spec = {"initialization": list(range(100000)), "source": "x" * 100000}
    result = {"rows_computed": 7, "details": [{"data": b"x" * 10000}] * 1000}
    inner = SimpleNamespace(
        id="job-repr",
        _state="failed",
        job_type="function_refresh",
        _description=SimpleNamespace(
            creation_ms=2000,
            spec=spec,
            result=result,
            failure="worker failed " + "x" * 10000,
        ),
    )
    job = AsyncJob(inner)
    if sync:
        job = Job(job)
    rendered = repr(job)
    assert len(rendered) < 1600
    assert len(rendered.splitlines()) <= 10
    assert "id='job-repr'" in rendered
    assert "state='failed'" in rendered
    assert "job_type='function_refresh'" in rendered
    assert "rows_computed" in rendered
    assert "worker failed" in rendered
    assert "..." in rendered
    assert len(repr(job.status)) < 1700
    assert job.spec() is spec
    assert job.result() is result
    assert len(job.spec()["initialization"]) == 100000


@pytest.mark.parametrize("sync", [False, True])
def test_job_repr_handles_unobserved_and_local_jobs(sync):
    unobserved = AsyncJob(SimpleNamespace(id="job-pending", _state=None))
    local = AsyncJob(None)
    if sync:
        unobserved, local = Job(unobserved), Job(local)
    assert "id='job-pending', not refreshed" in repr(unobserved)
    assert "state='finished'" in repr(local)
    assert "failure=" not in repr(local)


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


class _ProgressJob:
    id = "job-progress"
    job_type = "refresh_column"

    async def status(self):
        return "finished"

    async def wait(self, progress=None):
        if progress is not None:
            progress({"state": "running", "rows_planned": 4, "rows_computed": 2})
            progress({"state": "finished", "rows_planned": 4, "rows_computed": 4})
        return None


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
async def test_progress_is_additive_to_string_status_and_returns_none_locally():
    inner = MagicMock(id="job-progress", job_type="refresh_column")
    inner.status = AsyncMock(return_value="running")
    inner._description = type(
        "Description", (), {"progress": {"state": "running", "rows_computed": 2}}
    )()
    job = AsyncJob(inner)

    assert await job.status() == "running"
    assert await job.progress() == {"state": "running", "rows_computed": 2}
    assert await AsyncJob(None).progress() is None
    assert await AsyncJob(_ProgressJob()).progress() is None
    assert Job(AsyncJob(None)).progress() is None


def test_progress_eta_uses_two_stable_advancing_same_epoch_samples():
    now = int(time.time() * 1_000)
    previous = {
        "state": "running",
        "execution_epoch": 7,
        "sample_at_millis": now - 1_000,
        "rows_planned": 100,
        "rows_computed": 20,
        "rows_skipped": 0,
        "rows_remaining": 80,
        "fragments_planned": 4,
    }
    current = {
        **previous,
        "sample_at_millis": now,
        "rows_computed": 40,
        "rows_remaining": 60,
    }
    assert _progress_eta(previous, current) == 3_000
    assert _progress_eta(previous, {**current, "rows_planned": 120}) is None
    assert _progress_eta(previous, {**current, "execution_epoch": 8}) is None
    assert _progress_eta(previous, {**current, "rows_computed": 20}) is None
    stale = {
        **current,
        "sample_at_millis": now - 11 * 60 * 1_000,
    }
    assert (
        _progress_eta({**previous, "sample_at_millis": now - 12 * 60 * 1_000}, stale)
        is None
    )
    assert _progress_eta(previous, {**current, "state": "finished"}) is None
    assert (
        _progress_eta(previous, {**current, "state": "finished", "rows_remaining": 0})
        == 0
    )


@pytest.mark.asyncio
async def test_progress_retains_eta_from_same_handle_observations():
    now = int(time.time() * 1_000)
    first = {
        "state": "running",
        "execution_epoch": 7,
        "sample_at_millis": now - 1_000,
        "rows_planned": 100,
        "rows_computed": 20,
        "rows_skipped": 0,
        "rows_remaining": 80,
        "fragments_planned": 4,
    }
    second = {
        **first,
        "sample_at_millis": now,
        "rows_computed": 40,
        "rows_remaining": 60,
    }
    inner = MagicMock(id="job-progress", job_type="refresh_column")
    inner.status = AsyncMock(return_value="running")
    description = type("Description", (), {"progress": first})()
    inner._description = description
    job = AsyncJob(inner)

    assert await job.progress() == first
    description.progress = second
    assert (await job.progress())["estimated_remaining_time_millis"] == 3_000


@pytest.mark.asyncio
async def test_wait_progress_callback_observes_backend_poll_updates():
    updates = []

    result = await AsyncJob(_ProgressJob()).wait(progress=updates.append)

    assert result is None
    assert updates == [
        {"state": "running", "rows_planned": 4, "rows_computed": 2},
        {"state": "finished", "rows_planned": 4, "rows_computed": 4},
    ]


@pytest.mark.asyncio
async def test_wait_progress_callback_uses_the_public_eta_enrichment(monkeypatch):
    now = 10_000_000
    monkeypatch.setattr("lancedb.job.time.time", lambda: now / 1_000)

    class EtaJob:
        id = "job-eta"
        job_type = "refresh_column"

        async def wait(self, progress):
            first = {
                "state": "running",
                "execution_epoch": 7,
                "sample_at_millis": now - 1_000,
                "rows_planned": 100,
                "rows_computed": 20,
                "rows_skipped": 0,
                "rows_remaining": 80,
                "fragments_planned": 4,
            }
            progress(first)
            progress(
                {
                    **first,
                    "sample_at_millis": now,
                    "rows_computed": 40,
                    "rows_remaining": 60,
                }
            )

    updates = []
    assert await AsyncJob(EtaJob()).wait(progress=updates.append) is None

    assert "estimated_remaining_time_millis" not in updates[0]
    assert updates[1]["estimated_remaining_time_millis"] == 3_000


@pytest.mark.asyncio
async def test_wait_progress_true_updates_and_closes_only_the_owned_bar(monkeypatch):
    tqdm_module = pytest.importorskip("tqdm.auto")

    class Bar:
        def __init__(self):
            self.total = None
            self.updates = []
            self.phases = []
            self.closed = False

        def update(self, delta):
            self.updates.append(delta)

        def set_postfix_str(self, phase):
            self.phases.append(phase)

        def close(self):
            self.closed = True

    bar = Bar()
    monkeypatch.setattr(tqdm_module, "tqdm", lambda **kwargs: bar)

    assert await AsyncJob(_ProgressJob()).wait(progress=True) is None
    assert bar.total == 4
    assert bar.updates == [2, 2]
    assert bar.phases == ["running", "finished"]
    assert bar.closed


@pytest.mark.asyncio
async def test_wait_progress_bar_resumes_caller_owned_bar_after_timeout():
    class ResumableJob:
        id = "job-resume"
        job_type = "refresh_column"

        def __init__(self, bar):
            self.calls = 0
            self.bar = bar

        async def wait(self, progress):
            self.calls += 1
            if self.calls == 1:
                progress(
                    {
                        "state": "running",
                        "execution_epoch": 1,
                        "rows_planned": 4,
                        "rows_remaining": 2,
                    }
                )
                await asyncio.sleep(0.1)
            else:
                progress(
                    {
                        "state": "running",
                        "execution_epoch": 2,
                        "rows_planned": 4,
                        "rows_remaining": 3,
                    }
                )
                assert self.bar.n == 1
            progress(
                {
                    "state": "finished",
                    "execution_epoch": 2,
                    "rows_planned": 4,
                    "rows_remaining": 0,
                }
            )

    class Bar:
        def __init__(self):
            self.total = None
            self.n = 0
            self.updates = []

        def update(self, delta):
            self.updates.append(delta)
            self.n += delta

        def set_postfix_str(self, _phase):
            pass

    bar = Bar()
    job = AsyncJob(ResumableJob(bar))

    with pytest.raises(TimeoutError):
        await job.wait(timeout=timedelta(seconds=0.01), progress=bar)

    assert await job.wait(progress=bar) is None
    assert bar.total == 4
    assert bar.n == 4
    assert bar.updates == [2, 3]


@pytest.mark.asyncio
async def test_wait_progress_bar_resets_display_for_new_plan_or_epoch():
    class ReplannedJob:
        id = "job-replanned"
        job_type = "refresh_column"

        async def wait(self, progress):
            progress(
                {
                    "state": "running",
                    "execution_epoch": 1,
                    "rows_planned": 6,
                    "rows_remaining": 2,
                }
            )
            progress(
                {
                    "state": "running",
                    "execution_epoch": 1,
                    "rows_planned": 4,
                    "rows_remaining": 3,
                }
            )
            progress(
                {
                    "state": "running",
                    "execution_epoch": 2,
                    "rows_planned": 4,
                    "rows_remaining": 3,
                }
            )
            progress(
                {
                    "state": "finished",
                    "execution_epoch": 2,
                    "rows_planned": 4,
                    "rows_remaining": 0,
                }
            )

    class Bar:
        def __init__(self):
            self.total = None
            self.n = 0
            self.updates = []
            self.resets = []

        def update(self, delta):
            self.updates.append(delta)
            self.n += delta

        def reset(self, total=None):
            self.resets.append(total)
            self.total = total
            self.n = 0

        def set_postfix_str(self, _phase):
            pass

    bar = Bar()

    assert await AsyncJob(ReplannedJob()).wait(progress=bar) is None
    assert bar.total == 4
    assert bar.n == 4
    assert bar.n <= bar.total
    assert bar.resets == [4, 4]
    assert bar.updates == [4, 1, 1, 3]


@pytest.mark.asyncio
async def test_wait_progress_bar_uses_remaining_rows_when_computation_is_repeated(
    monkeypatch,
):
    tqdm_module = pytest.importorskip("tqdm.auto")

    class Bar:
        def __init__(self):
            self.total = None
            self.updates = []
            self.closed = False

        def update(self, delta):
            self.updates.append(delta)

        def set_postfix_str(self, _phase):
            pass

        def close(self):
            self.closed = True

    class RepeatedComputationJob:
        id = "job-recomputed"
        job_type = "refresh_column"

        async def wait(self, progress):
            progress(
                {
                    "state": "running",
                    "rows_planned": 4,
                    "rows_computed": 6,
                    "rows_remaining": 2,
                }
            )
            progress(
                {
                    "state": "finished",
                    "rows_planned": 4,
                    "rows_computed": 8,
                    "rows_remaining": 0,
                }
            )

    bar = Bar()
    monkeypatch.setattr(tqdm_module, "tqdm", lambda **kwargs: bar)

    assert await AsyncJob(RepeatedComputationJob()).wait(progress=True) is None
    assert bar.total == 4
    assert bar.updates == [2, 2]
    assert bar.closed


@pytest.mark.asyncio
async def test_wait_progress_bar_tracks_late_planning_without_regressing(
    monkeypatch,
):
    tqdm_module = pytest.importorskip("tqdm.auto")

    class Bar:
        def __init__(self):
            self.total = None
            self.updates = []
            self.closed = False

        def update(self, delta):
            self.updates.append(delta)

        def set_postfix_str(self, _phase):
            pass

        def close(self):
            self.closed = True

    class RegressingPlanJob:
        id = "job-regressing-plan"
        job_type = "refresh_column"

        async def wait(self, progress):
            samples = [
                (0, 0),
                (4, 2),
                (4, 3),
                (4, 2),
                (6, 1),
                (6, 0),
            ]
            for planned, remaining in samples:
                progress(
                    {
                        "state": "running",
                        "rows_planned": planned,
                        "rows_remaining": remaining,
                    }
                )

    bar = Bar()
    monkeypatch.setattr(tqdm_module, "tqdm", lambda **kwargs: bar)

    assert await AsyncJob(RegressingPlanJob()).wait(progress=True) is None
    assert bar.total == 6
    # The remaining-row dip at (4, 3) must not rewind the high-water mark.
    assert bar.updates == [2, 3, 1]
    assert bar.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("progress", [False, None])
async def test_wait_progress_false_keeps_legacy_inner_wait_call(progress):
    class NoProgressArgument:
        id = "job-legacy"

        @property
        def job_type(self):
            raise AssertionError("quiet waits must not inspect progress metadata")

        async def wait(self):
            return None

    assert await AsyncJob(NoProgressArgument()).wait(progress=progress) is None


class _RegistrationQuery:
    id = "registration-query"
    job_type = "query"
    _description = None

    def __init__(self, error=None):
        self.ready = asyncio.Event()
        self.error = error
        self.state = "running"
        self.wait_calls = 0
        self.cancel_calls = 0

    async def status(self):
        return self.state

    async def wait(self, progress=None):
        self.wait_calls += 1
        await self.ready.wait()
        if self.error is not None:
            self.state = (
                "cancelled" if isinstance(self.error, JobCancelledError) else "failed"
            )
            raise self.error
        self.state = "finished"
        return '{"version": "1"}'

    async def cancel(self):
        self.cancel_calls += 1


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_query_progress_observes_one_registration_waiter(sync, monkeypatch):
    monkeypatch.setattr("lancedb.job._QUERY_PROGRESS_INTERVAL_SECONDS", 0)
    inner = _RegistrationQuery()
    job = AsyncJob(inner, json.loads)
    updates = []

    def observe(snapshot):
        updates.append(snapshot)
        if len(updates) == 2:
            inner.ready.set()

    if sync:
        result = await asyncio.to_thread(Job(job).wait, progress=observe)
    else:
        result = await job.wait(progress=observe)
    assert result == {"version": "1"}
    assert inner.wait_calls == 1
    assert inner.cancel_calls == 0
    assert updates[0]["state"] == "running"
    assert updates[-1]["state"] == "finished"
    assert all(snapshot["job_id"] == inner.id for snapshot in updates)
    elapsed = [snapshot["client_elapsed_time_millis"] for snapshot in updates]
    assert elapsed == sorted(elapsed)
    assert elapsed[0] >= 0
    assert all("phase" not in snapshot for snapshot in updates)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        JobFailedError("bake failed", job_id="registration-query"),
        JobCancelledError("cancelled"),
    ],
)
async def test_query_progress_preserves_terminal_exceptions(error):
    inner = _RegistrationQuery(error)
    inner.ready.set()
    updates = []
    with pytest.raises(type(error)) as raised:
        await AsyncJob(inner).wait(progress=updates.append)
    assert raised.value is error
    assert updates[-1]["state"] == inner.state
    assert inner.wait_calls == 1


@pytest.mark.asyncio
async def test_query_progress_display_has_no_row_counter_and_closes_on_failure(
    monkeypatch,
):
    tqdm_module = pytest.importorskip("tqdm.auto")
    bar = SimpleNamespace(
        n=0,
        total=None,
        update=MagicMock(),
        set_postfix_str=MagicMock(),
        close=MagicMock(),
    )
    constructor = MagicMock(return_value=bar)
    monkeypatch.setattr(tqdm_module, "tqdm", constructor)
    inner = _RegistrationQuery(JobFailedError("bake failed"))
    inner.ready.set()

    with pytest.raises(JobFailedError, match="bake failed"):
        await AsyncJob(inner).wait(progress=True)
    assert "elapsed" in constructor.call_args.kwargs["bar_format"]
    assert "rows" not in str(constructor.call_args)
    assert bar.set_postfix_str.call_args.args == ("failed",)
    bar.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("observer_error", ["status", "callback"])
async def test_query_progress_observer_errors_do_not_replace_result_or_server_failure(
    failed, observer_error, caplog
):
    error = (
        JobFailedError("bake failed", job_id="registration-query") if failed else None
    )
    inner = _RegistrationQuery(error)
    inner.ready.set()
    if observer_error == "status":
        inner.status = AsyncMock(side_effect=RuntimeError("observer failed"))

    def observe(snapshot):
        if observer_error == "callback":
            raise RuntimeError("observer failed")

    job = AsyncJob(inner, json.loads)
    if failed:
        with pytest.raises(JobFailedError) as raised:
            await job.wait(progress=observe)
        assert raised.value is error
    else:
        assert await job.wait(progress=observe) == {"version": "1"}
    assert inner.wait_calls == 1
    assert inner.cancel_calls == 0
    assert "job progress observer failed" in caplog.text


@pytest.mark.asyncio
async def test_query_progress_timeout_keeps_registration_available_on_same_handle():
    inner = _RegistrationQuery()
    job = AsyncJob(inner, json.loads)
    updates = []
    with pytest.raises(TimeoutError, match="keep this handle"):
        await job.wait(timeout=timedelta(seconds=0.01), progress=updates.append)
    assert inner.state == "running"
    assert inner.cancel_calls == 0
    inner.ready.set()
    assert await job.wait(progress=updates.append) == {"version": "1"}
    assert inner.wait_calls == 2
    assert updates[-1]["state"] == "finished"


@pytest.mark.parametrize("refresh_lock", [False, True])
def test_blocking_create_function_forwards_progress_to_registration_handle(
    refresh_lock,
):
    connection = MagicMock()
    definition = object()
    progress = object()
    result = DBConnection.create_function(
        connection, definition, refresh_lock=refresh_lock, progress=progress
    )
    connection.create_function_async.assert_called_once_with(
        definition,
        secrets=None,
        exist_ok=False,
        namespace_path=None,
        refresh_lock=refresh_lock,
    )
    connection.create_function_async.return_value.wait.assert_called_once_with(
        progress=progress
    )
    assert result is connection.create_function_async.return_value.wait.return_value


@pytest.mark.parametrize("progress", [False, None])
def test_blocking_create_function_preserves_quiet_wait_call(progress):
    connection = MagicMock()
    result = DBConnection.create_function(connection, object(), progress=progress)
    connection.create_function_async.return_value.wait.assert_called_once_with()
    assert result is connection.create_function_async.return_value.wait.return_value


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


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_stats_returns_fresh_report_without_waiting_or_printing(sync, capsys):
    reports = [
        None,
        "Function job: finished\nWorker utilization: unavailable\n",
        "Function job: finished\nWorker utilization: 25.0%\n",
    ]
    inner = SimpleNamespace(
        stats=AsyncMock(side_effect=reports),
        wait=AsyncMock(),
        cancel=AsyncMock(),
    )
    job = AsyncJob(inner)
    for expected in reports:
        actual = await asyncio.to_thread(Job(job).stats) if sync else await job.stats()
        assert actual == expected
    inner.wait.assert_not_awaited()
    inner.cancel.assert_not_awaited()
    assert capsys.readouterr().out == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_stats_preserves_errors_and_rejects_untracked_jobs(sync):
    error = RuntimeError("run reports are unavailable on this server")
    job = AsyncJob(SimpleNamespace(stats=AsyncMock(side_effect=error)))
    with pytest.raises(RuntimeError) as exc:
        if sync:
            await asyncio.to_thread(Job(job).stats)
        else:
            await job.stats()
    assert exc.value is error
    with pytest.raises(NotImplementedError, match="server-side Function job"):
        if sync:
            Job(None).stats()
        else:
            await AsyncJob(None).stats()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_export_timeline_writes_fresh_recording_without_waiting(sync, tmp_path):
    trace = {
        "traceEvents": [{"ph": "X", "ts": 0, "dur": 1000}],
        "metadata": {"partial": True},
    }
    inner = SimpleNamespace(
        timeline=AsyncMock(return_value=json.dumps(trace)),
        wait=AsyncMock(),
        cancel=AsyncMock(),
    )
    job = AsyncJob(inner)
    path = tmp_path / "job.trace.json"
    if sync:
        await asyncio.to_thread(Job(job).export_timeline, str(path))
    else:
        await job.export_timeline(path)
    assert json.loads(path.read_text()) == trace
    inner.timeline.assert_awaited_once_with()
    inner.wait.assert_not_awaited()
    inner.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_export_timeline_failure_preserves_existing_file(tmp_path):
    path = tmp_path / "job.trace.json"
    path.write_text("previous recording")
    job = AsyncJob(
        SimpleNamespace(
            timeline=AsyncMock(side_effect=RuntimeError("unsupported server"))
        )
    )
    with pytest.raises(RuntimeError, match="unsupported server"):
        await job.export_timeline(path)
    assert path.read_text() == "previous recording"
    with pytest.raises(NotImplementedError, match="server-side Function job"):
        await AsyncJob(None).export_timeline(path)
    with pytest.raises(NotImplementedError, match="server-side Function job"):
        Job(None).export_timeline(path)


@pytest.mark.asyncio
async def test_export_timeline_reports_filesystem_errors(tmp_path):
    job = AsyncJob(
        SimpleNamespace(timeline=AsyncMock(return_value='{"traceEvents":[]}'))
    )
    with pytest.raises(FileNotFoundError):
        await job.export_timeline(tmp_path / "missing" / "trace.json")


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_stack_preserves_evidence_and_does_not_wait(sync):
    evidence = {
        "worker_id": "worker",
        "status": "unavailable",
        "execution_epoch": 1,
        "stacks": [],
        "omitted_sessions": 0,
    }
    inner = SimpleNamespace(
        stack=AsyncMock(return_value=json.dumps(evidence)),
        wait=AsyncMock(),
        cancel=AsyncMock(),
    )
    job = AsyncJob(inner)
    actual = (
        await asyncio.to_thread(Job(job).stack, "worker")
        if sync
        else await job.stack("worker")
    )
    assert actual == evidence
    inner.stack.assert_awaited_once_with("worker")
    inner.wait.assert_not_awaited()
    inner.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_stack_propagates_errors_and_rejects_local_handles(sync):
    error = RuntimeError("system operate privilege required")
    job = AsyncJob(SimpleNamespace(stack=AsyncMock(side_effect=error)))
    with pytest.raises(RuntimeError) as failure:
        if sync:
            await asyncio.to_thread(Job(job).stack, "worker")
        else:
            await job.stack("worker")
    assert failure.value is error
    with pytest.raises(NotImplementedError):
        if sync:
            Job(None).stack("worker")
        else:
            await AsyncJob(None).stack("worker")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,planned_key,completed_key,unit",
    [
        ("compact", "rows_planned", "rows_rewritten", " rows"),
        (
            "prewarm_page_cache",
            "executors_targeted",
            "executors_completed",
            " executors",
        ),
    ],
)
async def test_maintenance_progress_bar_uses_operation_counters_and_handles_retry(
    monkeypatch, kind, planned_key, completed_key, unit
):
    tqdm_module = pytest.importorskip("tqdm.auto")

    class Bar:
        total = None
        n = 0
        unit = " rows"
        closed = False

        def update(self, delta):
            self.n += delta

        def close(self):
            self.closed = True

    bar = Bar()
    monkeypatch.setattr(tqdm_module, "tqdm", lambda **kwargs: bar)

    class MaintenanceJob:
        id = "maintenance-job"
        job_type = kind

        async def wait(self, progress):
            progress({"job_type": kind, "phase": "planning"})
            assert bar.total is None
            progress({"job_type": kind, planned_key: 4, completed_key: 2})
            assert bar.n == 2
            # Older responses can omit an epoch; revised observations still
            # replace the previous attempt's displayed work.
            progress({"job_type": kind, planned_key: 4, completed_key: 1})
            assert bar.n == 1
            progress({"job_type": kind, planned_key: 4, completed_key: 4})

    assert await AsyncJob(MaintenanceJob()).wait(progress=True) is None
    assert bar.n == 4
    assert bar.total == 4
    assert bar.unit == unit
    assert bar.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("known_empty", [False, True])
async def test_prewarm_progress_bar_preserves_unknown_and_zero_totals(
    monkeypatch, known_empty
):
    tqdm_module = pytest.importorskip("tqdm.auto")

    class Bar:
        total = None
        n = 0
        unit = " rows"

        def update(self, delta):
            self.n += delta

        def close(self):
            pass

    bar = Bar()
    monkeypatch.setattr(tqdm_module, "tqdm", lambda **kwargs: bar)

    class PrewarmJob:
        id = "prewarm-job"
        job_type = "prewarm_page_cache"

        async def wait(self, progress):
            snapshot = {"job_type": self.job_type, "phase": "succeeded"}
            if known_empty:
                snapshot.update(executors_targeted=0, executors_completed=0)
            progress(snapshot)

    assert await AsyncJob(PrewarmJob()).wait(progress=True) is None
    assert bar.n == 0
    assert bar.total == (0 if known_empty else None)
    assert bar.unit == " executors"


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_profile_wait_preserves_timeout_and_never_waits_or_cancels_parent(sync):
    error = TimeoutError("capture and refresh continue")
    inner = SimpleNamespace(
        wait_for_profile=AsyncMock(side_effect=error),
        wait=AsyncMock(),
        cancel=AsyncMock(),
    )
    job = AsyncJob(inner)
    kwargs = {
        "timeout": timedelta(milliseconds=50),
        "poll_interval": timedelta(seconds=5),
    }
    with pytest.raises(TimeoutError) as caught:
        if sync:
            await asyncio.to_thread(Job(job).wait_for_profile, "profile-1", **kwargs)
        else:
            await job.wait_for_profile("profile-1", **kwargs)
    assert caught.value.args == error.args
    # On Python 3.11 and 3.12, run_coroutine_threadsafe re-creates TimeoutError
    # when copying it across futures, so identity only holds on the async path.
    if not sync:
        assert caught.value is error
    inner.wait_for_profile.assert_awaited_once_with(
        "profile-1", timeout_seconds=0.05, poll_interval_seconds=5.0
    )
    inner.wait.assert_not_awaited()
    inner.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [False, True])
async def test_profile_export_failure_preserves_destination_and_server_error(
    sync, tmp_path
):
    error = HttpError("403: System Operate required", "request-1", 403)
    inner = SimpleNamespace(profile_artifact=AsyncMock(side_effect=error))
    job = AsyncJob(inner)
    path = tmp_path / "profile.json"
    path.write_text("previous capture")
    with pytest.raises(HttpError) as caught:
        if sync:
            await asyncio.to_thread(Job(job).export_profile, "profile-1", path)
        else:
            await job.export_profile("profile-1", path)
    assert caught.value is error
    assert path.read_text() == "previous capture"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,args",
    [
        ("capture_profile", ("worker-1",)),
        ("profile_status", ("profile-1",)),
        ("wait_for_profile", ("profile-1",)),
        ("profile_artifact", ("profile-1",)),
    ],
)
async def test_function_profiles_reject_embedded_handles(method, args):
    with pytest.raises(NotImplementedError, match="Enterprise Function refresh job"):
        await getattr(AsyncJob(None), method)(*args)
    with pytest.raises(NotImplementedError, match="Enterprise Function refresh job"):
        getattr(Job(None), method)(*args)
