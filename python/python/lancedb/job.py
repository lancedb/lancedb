# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to asynchronous registry jobs, SQL queries, and local operations."""

import asyncio
import json
import time
from datetime import timedelta
from typing import Any, Callable, Generic, Optional, TypeVar, cast

import pyarrow as pa

from lancedb.background_loop import LOOP

from . import _lancedb
from ._lancedb import JobDescription, JobFailureInfo, JobInfo, _append_doc_url
from .remote.errors import HttpError, RetryError

T = TypeVar("T")

__all__ = [
    "AsyncJob",
    "Job",
    "JobDescription",
    "JobFailureInfo",
    "JobInfo",
]


def _timeout_message(
    job_id: Optional[str], timeout: timedelta, job_type: Optional[str]
) -> str:
    seconds = f"{timeout.total_seconds():g}"
    unit = "second" if seconds == "1" else "seconds"
    if job_id is None:
        return (
            f"operation still running after {seconds} {unit}; "
            "it continues on the server"
        )
    if job_type == "query":
        return (
            f"query {job_id!r} still running after {seconds} {unit}; "
            "it continues on the server; keep this handle to continue waiting"
        )
    return (
        f"job {job_id!r} still running after {seconds} {unit}; "
        "it continues on the server; "
        f"reattach with db.open_job({job_id!r}).wait()"
    )


def _normalize_job_progress(progress):
    """Normalize ``wait(progress=...)`` using the same convention as writes."""
    if progress is True:
        from tqdm.auto import tqdm

        return tqdm(unit=" rows"), True
    if progress is False or progress is None:
        return None, False
    return progress, False


_ETA_MAX_SAMPLE_INTERVAL_MILLIS = 10 * 60 * 1_000


def _progress_eta(previous, current):
    """Best-effort remaining time from two same-execution observations."""
    if not previous or current.get("state") not in ("running", "finished"):
        return None
    remaining = current.get("rows_remaining")
    if current.get("state") == "finished":
        return 0 if remaining == 0 else None
    if current.get("paused") or current.get("phase") in (
        "queued",
        "starting",
        "paused",
    ):
        return None
    epoch = current.get("execution_epoch")
    if epoch is None or previous.get("execution_epoch") != epoch:
        return None
    sample_at = current.get("sample_at_millis")
    previous_at = previous.get("sample_at_millis")
    if not isinstance(sample_at, int) or not isinstance(previous_at, int):
        return None
    elapsed = sample_at - previous_at
    if not 0 < elapsed <= _ETA_MAX_SAMPLE_INTERVAL_MILLIS:
        return None
    if time.time() * 1_000 - sample_at > _ETA_MAX_SAMPLE_INTERVAL_MILLIS:
        return None
    planned = current.get("rows_planned")
    if not isinstance(planned, int) or not isinstance(remaining, int):
        return None
    if remaining == 0:
        return 0
    if planned != previous.get("rows_planned"):
        return None
    if current.get("fragments_planned") != previous.get("fragments_planned"):
        return None
    accounted = current.get("rows_computed", 0) + current.get("rows_skipped", 0)
    previous_accounted = previous.get("rows_computed", 0) + previous.get(
        "rows_skipped", 0
    )
    advanced = accounted - previous_accounted
    previous_remaining = previous.get("rows_remaining")
    if advanced <= 0 or not isinstance(previous_remaining, int):
        return None
    if remaining > previous_remaining:
        return None
    return remaining * elapsed // advanced


def _with_progress_eta(previous, current):
    snapshot = dict(current)
    estimate = _progress_eta(previous, snapshot)
    if estimate is not None:
        snapshot["estimated_remaining_time_millis"] = estimate
    return snapshot


def _job_progress_callback(progress, enrich):
    """Adapt a callback or tqdm-like object to structured job snapshots."""
    if progress is None:
        return None
    if callable(progress):
        return lambda snapshot: progress(enrich(snapshot))

    # A caller-owned bar can outlive one wait call. Resume from its displayed
    # position instead of adding the same completed rows on the next call.
    last_rows = getattr(progress, "n", 0) or 0
    last_planned = getattr(progress, "total", None)
    last_epoch = None

    def update(snapshot):
        nonlocal last_epoch, last_planned, last_rows
        snapshot = enrich(snapshot)
        planned = snapshot.get("rows_planned")
        remaining = snapshot.get("rows_remaining")
        computed = snapshot.get("rows_computed")
        if isinstance(planned, int):
            epoch = snapshot.get("execution_epoch")
            reset = (
                isinstance(last_planned, int) and planned < last_planned
            ) or (
                isinstance(last_epoch, int)
                and isinstance(epoch, int)
                and epoch != last_epoch
            )
            if reset:
                last_rows = 0
                if hasattr(progress, "reset"):
                    progress.reset(total=planned)
                elif hasattr(progress, "n"):
                    progress.n = 0
            progress.total = planned
            last_planned = planned
            if isinstance(epoch, int):
                last_epoch = epoch
        completed = None
        if isinstance(planned, int) and isinstance(remaining, int):
            completed = max(planned - remaining, 0)
        elif computed is not None:
            completed = computed
        if completed is not None:
            if isinstance(planned, int):
                completed = min(completed, planned)
            if completed > last_rows:
                delta = completed - last_rows
                last_rows = completed
                progress.update(delta)
        phase = snapshot.get("phase")
        if phase is not None and hasattr(progress, "set_postfix_str"):
            progress.set_postfix_str(phase)

    return update


class AsyncJob(Generic[T]):
    """A handle to an operation that may still be running.

    The operation may already be complete when the handle is created. ``T``
    is the endpoint's terminal result type; unit-result jobs resolve to
    ``None``.
    """

    def __init__(
        self,
        inner: Optional[Any],
        result_decoder: Optional[Callable[[Any], T]] = None,
    ):
        self._inner = inner
        self._result_decoder = result_decoder
        self._last_progress_snapshot = None

    def _enrich_progress(self, snapshot):
        enriched = _with_progress_eta(self._last_progress_snapshot, snapshot)
        self._last_progress_snapshot = enriched
        return enriched

    @property
    def id(self) -> Optional[str]:
        """The operation's opaque ID, interpreted according to :attr:`job_type`.

        Registry jobs expose a registry job ID; SQL-backed jobs expose a
        connection-scoped query ID as a string. Local operations return ``None``.
        Query IDs cannot be used with registry job APIs or to resume after a
        client restart.
        """
        return self._inner.id if self._inner is not None else None

    async def status(self) -> str:
        """The operation's current lifecycle state: "running", "finished",
        "failed", or "cancelled".

        A point snapshot; unlike `wait` it does not block or raise on a
        terminal failure state. States a newer server reports that this
        client version does not know pass through as-is.
        """
        if self._inner is None:
            return "finished"
        return await self._inner.status()

    async def progress(self) -> Optional[dict[str, Any]]:
        """Fetch a structured progress snapshot.

        Registry jobs always report lifecycle and queue fields. Job types that
        publish counters include them in the same mapping; older servers and
        in-process operations omit them. A missing counter is unknown rather
        than zero. This is additive to :meth:`status`, whose string contract is
        unchanged.
        """
        if self._inner is None:
            return None
        await self._inner.status()
        description = getattr(self._inner, "_description", None)
        if description is None or description.progress is None:
            return None
        return self._enrich_progress(description.progress)

    async def wait(
        self,
        timeout: Optional[timedelta] = None,
        progress: Any = False,
    ) -> T:
        """Wait until the operation reaches a terminal state.

        Returns the endpoint's typed result, or ``None`` for a unit-result
        job.

        ``progress=True`` renders a progress bar, ``progress=callable``
        receives each structured snapshot, and a tqdm-like object is updated in
        place. Observations use the backend's existing wait poller and never
        start a second job. The bar advances by remaining rows when available,
        because completed computation can include repeated work after a plan
        change. A new execution epoch or smaller plan resets the bar to that
        plan. ``False`` preserves the original wait behavior.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        Transient connection and read errors are retried with capped exponential
        backoff within the same timeout (indefinitely when no timeout is given).
        If the timeout expires while disconnected, raises `ConnectionError`
        instead, with the job ID and instructions to resume waiting. The job
        may still be running; timing out does not cancel it.

        The timeout error identifies the job and explains that it continues on
        the server. Registry jobs include the ``db.open_job(id).wait()``
        reattach call; SQL query handles must be kept because their IDs cannot
        be reopened through the registry jobs API.
        """
        if self._inner is None:
            return cast(T, None)
        progress_obj, owns = _normalize_job_progress(progress)
        progress_callback = _job_progress_callback(progress_obj, self._enrich_progress)
        last_error = None

        async def wait_with_retries():
            nonlocal last_error
            delay = 0.25
            while True:
                try:
                    if last_error is not None:
                        # Confirm recovery before restarting the backend poller,
                        # so a later timeout isn't blamed on an earlier outage.
                        await self._inner.status()
                        last_error = None
                        delay = 0.25
                    if progress_callback is None:
                        return await self._inner.wait()
                    return await self._inner.wait(progress=progress_callback)
                except (HttpError, RetryError) as error:
                    cause = error.__cause__ if isinstance(error, RetryError) else error
                    if not isinstance(cause, HttpError) or not cause.is_transport_error:
                        raise
                    last_error = error
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

        try:
            try:
                if timeout is None:
                    result = await wait_with_retries()
                else:
                    result = await asyncio.wait_for(
                        wait_with_retries(), timeout.total_seconds()
                    )
            except asyncio.TimeoutError as error:
                if last_error is None:
                    if timeout is None:
                        raise
                    raise TimeoutError(
                        _timeout_message(self.id, timeout, self.job_type)
                    ) from error
                identity = (
                    f"job {self.id!r}" if self.id is not None else "the operation"
                )
                if self.id is not None and self.job_type != "query":
                    reconnect = (
                        f"Use db.open_job({self.id!r}) to reconnect, then call wait() "
                        "on the returned job."
                    )
                else:
                    reconnect = "Call wait() on this job handle to reconnect."
                raise ConnectionError(
                    f"Lost connection while waiting and the timeout expired; "
                    f"{identity} may still be running. "
                    f"{reconnect}"
                ) from last_error
        finally:
            if owns:
                progress_obj.close()
        if self._result_decoder is not None:
            return self._result_decoder(result)
        return cast(T, result)

    async def cancel(self):
        """Request cancellation. Cancelling a finished operation is a no-op."""
        if self._inner is None:
            return
        await self._inner.cancel()

    async def refresh(self) -> None:
        """Ask the backend for this job's current state, and for a server-side
        job its full record, then cache it for the properties below.

        Registry details are `None` until this runs, because submitting an
        operation does not fetch its full registry record. Query-backed jobs
        expose only lifecycle state through this method. For registry jobs,
        `status` fetches the whole record too; `wait` records only the terminal
        state it establishes.
        """
        if self._inner is None:
            return
        await self._inner.refresh()

    @property
    def state(self) -> Optional[str]:
        """The last observed lifecycle state, without contacting the backend.

        `None` until the handle has talked to it. See :meth:`AsyncJob.refresh`.
        """
        if self._inner is None:
            return "finished"
        return self._inner._state

    @property
    def job_type(self) -> Optional[str]:
        """The job's kind of work, without contacting the backend.

        SQL-backed handles report ``"query"`` immediately. Registry handles
        report the server's job type after :meth:`refresh` or :meth:`status`.
        In-process handles return ``None``.
        """
        return self._inner.job_type if self._inner is not None else None

    @property
    def creation_ms(self) -> Optional[int]:
        """When the job was created, in milliseconds since the epoch."""
        return self._field("creation_ms")

    @property
    def spec(self) -> Optional[Any]:
        """The job-type-specific specification it was submitted with."""
        return self._field("spec")

    @property
    def result(self) -> Optional[Any]:
        """The job-type-specific terminal result, as reported data rather than
        the typed model :meth:`AsyncJob.wait` returns.

        `None` until the job succeeds, so a job that never terminates reports
        its progress through :meth:`AsyncJob.events` instead.
        """
        return self._field("result")

    @property
    def failure(self) -> Optional[JobFailureInfo]:
        """Why the job failed, when it failed and the server reports a reason."""
        return self._field("failure")

    @property
    def _spec_json(self) -> Optional[str]:
        return self._field("_spec_json")

    @property
    def _result_json(self) -> Optional[str]:
        return self._field("_result_json")

    def _field(self, name: str) -> Optional[Any]:
        description = self._inner._description if self._inner is not None else None
        return getattr(description, name) if description is not None else None

    async def events(
        self,
        *,
        limit: Optional[int] = None,
        filter: Optional[str] = None,
    ) -> "pa.Table":
        """This job's recorded lifecycle events.

        Where the properties above report a terminal result only once the job
        reaches one, events are written as the job runs and outlive the workers
        that produced them. A distributed job records a `claim`/`claim_complete`
        pair per unit of work, each carrying `rows_processed`, so a job that
        never finishes still accounts for what it did.

        Parameters
        ----------
        limit: int, optional
            Maximum event rows to return. The server caps results at 1000 by
            default and 10,000 at most, and truncates without saying so, so
            pass this for a job that emits an event per fragment.
        filter: str, optional
            SQL-like expression over the `state`, `updated_by`, `emitted_from`,
            `emitted_by`, and `claim_entity` columns, such as
            ``state = 'claim_complete'``.
        """
        if self._inner is None:
            raise NotImplementedError(
                _append_doc_url(
                    "job event history is only available for server-side jobs",
                    "jobs",
                )
            )
        return await self._inner.events(limit=limit, filter=filter)

    def __repr__(self) -> str:
        return _job_repr("AsyncJob", self)


_REPR_INDENT = " " * 4


def _repr_payload(value: Any) -> str:
    """Render a job payload as indented JSON, aligned under its field."""
    try:
        rendered = json.dumps(value, indent=4)
    except TypeError:
        return repr(value)
    return rendered.replace("\n", "\n" + _REPR_INDENT)


def _job_repr(kind: str, job: Any) -> str:
    """Render every field the handle currently knows, omitting the rest.

    One field per line, with the JSON payloads indented, because a refresh
    job's spec and result are the point of printing it.
    """
    state = job.state
    if state is None:
        # Nothing has been fetched yet, so there is nothing to lay out.
        known = f"id={job.id!r}, " if job.id is not None else ""
        return f"{kind}({known}not refreshed)"

    fields = []
    if job.id is not None:
        fields.append(f"id={job.id!r}")
    fields.append(f"state={state!r}")
    for name in ("job_type", "creation_ms"):
        value = getattr(job, name)
        if value is not None:
            fields.append(f"{name}={value!r}")
    for name in ("spec", "result"):
        value = getattr(job, name)
        if value is not None:
            fields.append(f"{name}={_repr_payload(value)}")
    if job.failure is not None:
        fields.append(f"failure={job.failure!r}")
    body = "".join(f"\n{_REPR_INDENT}{field}," for field in fields)
    return f"{kind}({body}\n)"


class Job(Generic[T]):
    """Synchronous counterpart of `AsyncJob` with the same result type."""

    def __init__(self, inner: Optional[AsyncJob[T]]):
        self._inner = inner

    @property
    def id(self) -> Optional[str]:
        """Identifies the operation on the server that is running it.

        See :attr:`AsyncJob.id`.
        """
        return self._inner.id if self._inner is not None else None

    def status(self) -> str:
        """The operation's current lifecycle state: "running", "finished",
        "failed", or "cancelled".

        See :meth:`AsyncJob.status`.
        """
        if self._inner is None:
            return "finished"
        return LOOP.run(self._inner.status())

    def wait(
        self,
        timeout: Optional[timedelta] = None,
        progress: Any = False,
    ) -> T:
        """Block until the operation reaches a terminal state.

        Returns the endpoint's typed result, or ``None`` for a unit-result
        job.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        Transient connection and read errors are retried within that timeout;
        if it expires while disconnected, raises `ConnectionError` with
        instructions to reconnect. Timing out does not cancel the job.

        The timeout error identifies the job and explains that it continues on
        the server. Registry jobs include the ``db.open_job(id).wait()``
        reattach call; SQL query handles must be kept because their IDs cannot
        be reopened through the registry jobs API.

        See :meth:`AsyncJob.wait`.
        """
        if self._inner is None:
            return cast(T, None)
        return LOOP.run(self._inner.wait(timeout, progress=progress))

    def progress(self) -> Optional[dict[str, Any]]:
        """Fetch a structured progress snapshot. See :meth:`AsyncJob.progress`."""
        if self._inner is None:
            return None
        return LOOP.run(self._inner.progress())

    def cancel(self):
        """Request cancellation. Cancelling a finished operation is a no-op."""
        if self._inner is None:
            return
        LOOP.run(self._inner.cancel())

    def refresh(self) -> None:
        """Ask the backend for this job's current state and record.

        See :meth:`AsyncJob.refresh`.
        """
        if self._inner is None:
            return
        LOOP.run(self._inner.refresh())

    @property
    def state(self) -> Optional[str]:
        """The last observed lifecycle state. See :attr:`AsyncJob.state`."""
        return self._inner.state if self._inner is not None else "finished"

    @property
    def job_type(self) -> Optional[str]:
        """The job's type. See :attr:`AsyncJob.job_type`."""
        return self._inner.job_type if self._inner is not None else None

    @property
    def creation_ms(self) -> Optional[int]:
        """When the job was created. See :attr:`AsyncJob.creation_ms`."""
        return self._field("creation_ms")

    @property
    def spec(self) -> Optional[Any]:
        """The job's specification. See :attr:`AsyncJob.spec`."""
        return self._field("spec")

    @property
    def result(self) -> Optional[Any]:
        """The job's terminal result. See :attr:`AsyncJob.result`."""
        return self._field("result")

    @property
    def failure(self) -> Optional[JobFailureInfo]:
        """Why the job failed. See :attr:`AsyncJob.failure`."""
        return self._field("failure")

    @property
    def _spec_json(self) -> Optional[str]:
        return self._field("_spec_json")

    @property
    def _result_json(self) -> Optional[str]:
        return self._field("_result_json")

    def _field(self, name: str) -> Optional[Any]:
        return getattr(self._inner, name) if self._inner is not None else None

    def events(
        self,
        *,
        limit: Optional[int] = None,
        filter: Optional[str] = None,
    ) -> "pa.Table":
        """This job's recorded lifecycle events.

        See :meth:`AsyncJob.events`.
        """
        if self._inner is None:
            raise NotImplementedError(
                _append_doc_url(
                    "job event history is only available for server-side jobs",
                    "jobs",
                )
            )
        return LOOP.run(self._inner.events(limit=limit, filter=filter))

    def __repr__(self) -> str:
        return _job_repr("Job", self)


def _typed_job(
    inner: "_lancedb.Job", result_decoder: Callable[[str], T]
) -> AsyncJob[T]:
    """Bind an internal JSON-producing job to its public result model."""
    return AsyncJob(inner, result_decoder)
