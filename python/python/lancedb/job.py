# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to asynchronous registry jobs, SQL queries, and local operations."""

import asyncio
import reprlib
import time
from contextlib import suppress
from datetime import timedelta
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Generic, Optional, TypeVar, cast

import pyarrow as pa

from lancedb.background_loop import LOOP

from . import _lancedb
from ._lancedb import JobDescription, JobFailureInfo, JobInfo, _append_doc_url
from .functions import RefreshColumnResult
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


def _normalize_job_progress(progress, *, query=False):
    """Normalize ``wait(progress=...)`` using the same convention as writes."""
    if progress is True:
        from tqdm.auto import tqdm

        if query:
            return tqdm(bar_format="{postfix} [{elapsed}]"), True
        return tqdm(unit=" rows"), True
    if progress is False or progress is None:
        return None, False
    return progress, False


_ETA_MAX_SAMPLE_INTERVAL_MILLIS = 10 * 60 * 1_000
_QUERY_PROGRESS_INTERVAL_SECONDS = 1.0


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
    first_snapshot = True

    def update(snapshot):
        nonlocal first_snapshot, last_epoch, last_planned, last_rows
        snapshot = enrich(snapshot)
        planned = snapshot.get("rows_planned")
        remaining = snapshot.get("rows_remaining")
        computed = snapshot.get("rows_computed")
        if isinstance(planned, int):
            epoch = snapshot.get("execution_epoch")
            reset = (isinstance(last_planned, int) and planned < last_planned) or (
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
            if first_snapshot and completed < last_rows:
                # The server may have started a new execution while a
                # caller-owned bar retained its old position.
                last_rows = completed
                if hasattr(progress, "n"):
                    progress.n = completed
                if hasattr(progress, "refresh"):
                    progress.refresh()
            if completed > last_rows:
                delta = completed - last_rows
                last_rows = completed
                progress.update(delta)
            first_snapshot = False
        phase = snapshot.get("phase") or snapshot.get("state")
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
        self._created_at = time.monotonic()

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

        Registry jobs always report lifecycle fields. SQL query handles report
        lifecycle state and ``client_elapsed_time_millis``, measured since this
        handle was created. They do not expose server build phases or logs.
        Queue metadata and
        counters are included when supplied by the server. Older servers may
        omit counters, and in-process operations return ``None``. A missing
        counter is unknown rather than zero. This is additive to :meth:`status`,
        whose string contract is unchanged.
        """
        if self._inner is None:
            return None
        state = await self._inner.status()
        description = getattr(self._inner, "_description", None)
        if description is None or description.progress is None:
            if self.job_type == "query":
                return {
                    "job_id": self.id,
                    "job_type": "query",
                    "state": state,
                    "client_elapsed_time_millis": int(
                        (time.monotonic() - self._created_at) * 1_000
                    ),
                }
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
        change. On each wait call, the first complete observation synchronizes
        a caller-owned bar with the server's current completed rows and total;
        later observations in a stable execution epoch use a high-water mark. A
        new execution epoch or smaller plan resets the bar to that plan.
        ``False`` preserves the original wait behavior.

        For SQL-backed Function registration, progress shows the query's
        lifecycle state and client elapsed time. It observes the existing
        query handle while its original waiter runs; it does not resubmit or
        cancel the registration. Detailed bake phases and build logs are not
        available from the current query handle. Failure exceptions retain the
        server's cause and request identifiers.

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
        progress_obj, owns = _normalize_job_progress(
            progress, query=self.job_type == "query"
        )
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

        async def observe_query():
            waiter = asyncio.create_task(wait_with_retries())
            try:
                while not waiter.done():
                    snapshot = await self.progress()
                    if snapshot is not None:
                        progress_callback(snapshot)
                    await asyncio.wait(
                        {waiter}, timeout=_QUERY_PROGRESS_INTERVAL_SECONDS
                    )
                try:
                    return await waiter
                finally:
                    snapshot = await self.progress()
                    if snapshot is not None:
                        progress_callback(snapshot)
            finally:
                if not waiter.done():
                    waiter.cancel()
                    with suppress(asyncio.CancelledError):
                        await waiter

        wait_for_result = (
            observe_query
            if self.job_type == "query" and progress_callback is not None
            else wait_with_retries
        )
        try:
            try:
                if timeout is None:
                    result = await wait_for_result()
                else:
                    result = await asyncio.wait_for(
                        wait_for_result(), timeout.total_seconds()
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

    async def stats(self) -> Optional[str]:
        """Return a printable run report for this Function job.

        The report includes recorded phase timings, finishing-supervisor slot
        occupancy, all-attempt split execution and batch latency, and accepted-row
        throughput. Each call reads the current server report.
        Returns ``None`` while the job is running or if no recorded profile is
        available, without waiting for completion. Missing measurements remain
        unavailable rather than becoming zero. The server formats the report;
        the SDK returns it verbatim.

        Requires a server-side Function job and a server supporting run reports.
        Local operations and SQL query handles do not support this operation.
        Unsupported operations raise ``NotImplementedError`` or a server error.
        This method returns text; use ``print`` to display it.

        Examples
        --------
        >>> print(await job.stats())  # doctest: +SKIP
        """
        if self._inner is None:
            raise NotImplementedError(
                "job statistics require a server-side Function job"
            )
        return await self._inner.stats()

    async def export_timeline(self, path: str | PathLike[str]) -> None:
        """Write a fresh recorded Function job timeline as Chrome trace JSON.

        Open the file in Perfetto or Chrome tracing. The recording contains
        retained split spans grouped by worker and timestamped job phases.
        Batch counts and latency statistics are included as details; individual
        batch timestamps are not recorded. Running jobs export a partial snapshot
        without waiting or cancelling. Download again to include later work.

        Requires a server-side Function job and a server supporting timeline
        export. Unsupported operations raise ``NotImplementedError`` or a server
        error. The destination is overwritten only after the trace is fetched.
        Filesystem errors propagate to the caller.

        Examples
        --------
        >>> await job.export_timeline("refresh.trace.json")  # doctest: +SKIP
        """
        if self._inner is None:
            raise NotImplementedError(
                "job timeline export requires a server-side Function job"
            )
        trace = await self._inner.timeline()
        await asyncio.to_thread(Path(path).write_text, trace, encoding="utf-8")

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
_REPR_MAX_FIELD_LENGTH = 240
_PAYLOAD_REPR = reprlib.Repr()
_PAYLOAD_REPR.maxlevel = 2
_PAYLOAD_REPR.maxdict = 4
_PAYLOAD_REPR.maxlist = 4
_PAYLOAD_REPR.maxtuple = 4
_PAYLOAD_REPR.maxstring = 120
_PAYLOAD_REPR.maxother = 120


def _repr_payload(value: Any) -> str:
    """Bound nested payload previews without changing the stored values."""
    rendered = _PAYLOAD_REPR.repr(value).replace("\n", "\\n")
    if len(rendered) > _REPR_MAX_FIELD_LENGTH:
        return rendered[: _REPR_MAX_FIELD_LENGTH - 3] + "..."
    return rendered


def _job_repr(kind: str, job: Any) -> str:
    """Show cached job identity and bounded previews without backend calls.

    The complete payloads remain available through ``spec`` and ``result``.
    """
    state = job.state
    if state is None:
        # Nothing has been fetched yet, so there is nothing to lay out.
        known = f"id={_repr_payload(job.id)}, " if job.id is not None else ""
        return f"{kind}({known}not refreshed)"

    fields = []
    if job.id is not None:
        fields.append(f"id={_repr_payload(job.id)}")
    fields.append(f"state={_repr_payload(state)}")
    for name in ("job_type", "creation_ms"):
        value = getattr(job, name)
        if value is not None:
            fields.append(f"{name}={_repr_payload(value)}")
    for name in ("spec", "result"):
        value = getattr(job, name)
        if value is not None:
            fields.append(f"{name}={_repr_payload(value)}")
    if job.failure is not None:
        fields.append(f"failure={_repr_payload(job.failure)}")
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

    def stats(self) -> Optional[str]:
        """Return a printable Function job run report.

        See [AsyncJob.stats][lancedb.job.AsyncJob.stats] for metrics and availability.

        Examples
        --------
        >>> print(job.stats())  # doctest: +SKIP
        """
        if self._inner is None:
            raise NotImplementedError(
                "job statistics require a server-side Function job"
            )
        return LOOP.run(self._inner.stats())

    def export_timeline(self, path: str | PathLike[str]) -> None:
        """Write Chrome trace JSON for this Function job.

        See :meth:`AsyncJob.export_timeline`. Open the file in Perfetto or
        Chrome tracing.

        Examples
        --------
        >>> job.export_timeline("refresh.trace.json")  # doctest: +SKIP
        """
        if self._inner is None:
            raise NotImplementedError(
                "job timeline export requires a server-side Function job"
            )
        return LOOP.run(self._inner.export_timeline(path))

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


def _opened_job(inner: "_lancedb.Job") -> AsyncJob:
    if inner.job_type in ("function_refresh", "computed_refresh", "refresh_column"):
        return _typed_job(inner, RefreshColumnResult.from_json)
    return AsyncJob(inner)
