# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any, Dict, Optional

from .background_loop import LOOP


class AsyncJob:
    """A handle for an asynchronous LanceDB Cloud job."""

    __slots__ = ("_table", "_id")

    def __init__(self, table: Any, job_id: str):
        self._table = table
        self._id = job_id

    @property
    def id(self) -> str:
        """The opaque Job Registry ID."""
        return self._id

    async def _describe(self) -> Dict[str, Any]:
        return json.loads(await self._table._describe_job(self._id))

    async def status(self) -> str:
        """Return the current job state, such as ``"in_progress"``."""
        description = await self._describe()
        return str(description["job_state"]).lower()

    async def progress(self) -> Dict[str, Any]:
        """Return the latest job-specific progress payload."""
        progress = (await self._describe()).get("status")
        return progress if isinstance(progress, dict) else {}

    async def wait(
        self,
        timeout: Optional[timedelta] = None,
        poll_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Wait until the job completes or reaches another terminal state."""
        timeout_seconds = None if timeout is None else timeout.total_seconds()
        poll_seconds = poll_interval.total_seconds()
        if timeout_seconds is not None and timeout_seconds < 0:
            raise ValueError("timeout must be non-negative")
        if poll_seconds <= 0:
            raise ValueError("poll_interval must be positive")

        loop = asyncio.get_running_loop()
        deadline = None if timeout_seconds is None else loop.time() + timeout_seconds
        timeout_message = f"Timed out waiting for job {self._id}"
        while True:
            if deadline is None:
                description = await self._describe()
            else:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(timeout_message)
                try:
                    description = await asyncio.wait_for(
                        self._describe(), timeout=remaining
                    )
                except asyncio.TimeoutError as exc:
                    raise TimeoutError(timeout_message) from exc
            state = str(description["job_state"]).lower()
            if state == "done":
                return
            if state == "failed":
                raise RuntimeError(
                    f"Job {self._id} failed: {description.get('status')!r}"
                )
            if state == "cancelled":
                raise RuntimeError(f"Job {self._id} was cancelled")
            if deadline is None:
                sleep_seconds = poll_seconds
            else:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError(timeout_message)
                sleep_seconds = min(poll_seconds, remaining)
            await asyncio.sleep(sleep_seconds)

    async def cancel(self) -> None:
        """Request cancellation of the job."""
        await self._table._cancel_job(self._id)

    def __repr__(self) -> str:
        return f"AsyncJob(id={self._id!r})"


class Job:
    """A synchronous handle for an asynchronous LanceDB Cloud job."""

    __slots__ = ("_inner",)

    def __init__(self, inner: AsyncJob):
        self._inner = inner

    @property
    def id(self) -> str:
        """The opaque Job Registry ID."""
        return self._inner.id

    def status(self) -> str:
        """Return the current job state, such as ``"in_progress"``."""
        return LOOP.run(self._inner.status())

    def progress(self) -> Dict[str, Any]:
        """Return the latest job-specific progress payload."""
        return LOOP.run(self._inner.progress())

    def wait(
        self,
        timeout: Optional[timedelta] = None,
        poll_interval: timedelta = timedelta(seconds=1),
    ) -> None:
        """Block until the job completes or reaches another terminal state."""
        LOOP.run(self._inner.wait(timeout=timeout, poll_interval=poll_interval))

    def cancel(self) -> None:
        """Request cancellation of the job."""
        LOOP.run(self._inner.cancel())

    def __repr__(self) -> str:
        return f"Job(id={self.id!r})"
