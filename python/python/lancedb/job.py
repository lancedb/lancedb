# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to operations a server may run asynchronously."""

import asyncio
from datetime import timedelta
from typing import Any, Callable, Generic, Optional, TypeVar, cast

from lancedb.background_loop import LOOP

from . import _lancedb

T = TypeVar("T")


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


def _typed_job(
    inner: "_lancedb.TypedJob", result_decoder: Callable[[str], T]
) -> AsyncJob[T]:
    """Bind an internal JSON-producing job to its public result model."""
    return AsyncJob(inner, result_decoder)
