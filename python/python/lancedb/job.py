# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to operations a server may run asynchronously."""

import asyncio
from datetime import timedelta
from typing import Any, Callable, Generic, Optional, TypeVar, cast

import pyarrow as pa

from lancedb.background_loop import LOOP

from . import _lancedb
from ._lancedb import JobDescription, JobFailureInfo, JobInfo

T = TypeVar("T")

__all__ = [
    "AsyncJob",
    "Job",
    "JobDescription",
    "JobFailureInfo",
    "JobInfo",
]


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

    @property
    def id(self) -> Optional[str]:
        """Identifies the operation on the server that is running it.

        Returned for correlating with server logs or the jobs API. Operations
        that run in this process have no server id and return `None`. The value
        is opaque: parsing it or storing it to resume the job later is not
        supported.
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

    async def wait(self, timeout: Optional[timedelta] = None) -> T:
        """Wait until the operation reaches a terminal state.

        Returns the endpoint's typed result, or ``None`` for a unit-result
        job.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        """
        if self._inner is None:
            return cast(T, None)
        if timeout is None:
            result = await self._inner.wait()
        else:
            result = await asyncio.wait_for(self._inner.wait(), timeout.total_seconds())
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

        The properties are all `None` until this runs, because submitting an
        operation returns only a job id. `status` and `wait` refresh too.
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
        """The job's type, as the server names it.

        `None` for an in-process job, which has no server-side record.
        """
        return self._field("job_type")

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
                "job event history is only available for server-side jobs"
            )
        return await self._inner.events(limit=limit, filter=filter)

    def __repr__(self) -> str:
        return _job_repr("AsyncJob", self)


def _job_repr(kind: str, job: Any) -> str:
    """Render every field the handle currently knows, omitting the rest."""
    fields = []
    if job.id is not None:
        fields.append(f"id={job.id!r}")
    state = job.state
    if state is None:
        fields.append("not refreshed")
    else:
        fields.append(f"state={state!r}")
        for name in ("job_type", "creation_ms", "spec", "result", "failure"):
            value = getattr(job, name)
            if value is not None:
                fields.append(f"{name}={value!r}")
    return f"{kind}({', '.join(fields)})"


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

    def wait(self, timeout: Optional[timedelta] = None) -> T:
        """Block until the operation reaches a terminal state.

        Returns the endpoint's typed result, or ``None`` for a unit-result
        job.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        """
        if self._inner is None:
            return cast(T, None)
        return LOOP.run(self._inner.wait(timeout))

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
        return self._field("job_type")

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
                "job event history is only available for server-side jobs"
            )
        return LOOP.run(self._inner.events(limit=limit, filter=filter))

    def __repr__(self) -> str:
        return _job_repr("Job", self)


def _typed_job(
    inner: "_lancedb.Job", result_decoder: Callable[[str], T]
) -> AsyncJob[T]:
    """Bind an internal JSON-producing job to its public result model."""
    return AsyncJob(inner, result_decoder)
