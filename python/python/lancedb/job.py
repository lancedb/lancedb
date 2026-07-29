# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to operations a server may run asynchronously."""

import asyncio
from datetime import timedelta
from typing import Optional

from lancedb.background_loop import LOOP

from . import _lancedb


class AsyncJob:
    """A handle to an operation that may still be running.

    The operation may already be complete when the handle is created.
    """

    def __init__(self, inner: Optional["_lancedb.Job"]):
        self._inner = inner

    async def wait(self, timeout: Optional[timedelta] = None):
        """Wait until the operation reaches a terminal state.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        """
        if self._inner is None:
            return
        if timeout is None:
            await self._inner.wait()
        else:
            await asyncio.wait_for(self._inner.wait(), timeout.total_seconds())

    async def cancel(self):
        """Request cancellation. Cancelling a finished operation is a no-op."""
        if self._inner is None:
            return
        await self._inner.cancel()


class Job:
    """Synchronous counterpart of `AsyncJob`."""

    def __init__(self, inner: Optional[AsyncJob]):
        self._inner = inner

    def wait(self, timeout: Optional[timedelta] = None):
        """Block until the operation reaches a terminal state.

        Raises `JobFailedError` if the operation failed, `JobCancelledError`
        if it was cancelled, and `TimeoutError` if `timeout` elapses first.
        """
        if self._inner is None:
            return
        LOOP.run(self._inner.wait(timeout))

    def cancel(self):
        """Request cancellation. Cancelling a finished operation is a no-op."""
        if self._inner is None:
            return
        LOOP.run(self._inner.cancel())
