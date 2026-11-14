# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
import json
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from lancedb.exceptions import JobCancelledError, JobFailedError
from lancedb.job import AsyncJob, Job, _progress_eta
from lancedb.remote.errors import HttpError, RetryError


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
    assert job.spec is spec
    assert job.result is result
    assert len(job.spec["initialization"]) == 100000


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
async def test_wait_progress_false_keeps_legacy_inner_wait_call():
    class NoProgressArgument:
        id = "job-legacy"
        job_type = None

        async def wait(self):
            return None

    assert await AsyncJob(NoProgressArgument()).wait(progress=False) is None


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
