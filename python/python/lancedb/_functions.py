# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Private first-class Function namespace facades for database connections.

These helpers are internal submission surfaces. They are not durable resources
and are not part of the public top-level export surface.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from . import _udf
from .job import AsyncJob, Job

if TYPE_CHECKING:
    from .db import AsyncConnection, DBConnection


class _SyncFunctions:
    """Synchronous `db.functions` facade."""

    __slots__ = ("_connection",)

    def __init__(self, connection: DBConnection) -> None:
        self._connection = connection

    def __repr__(self) -> str:
        return "_SyncFunctions()"

    def register(self, name: str, decorated_udf: Callable[..., object]) -> Job:
        """Register a decorated UDF and return a synchronous [Job][lancedb.job.Job]."""
        definition = _udf._build_function_definition(decorated_udf)
        native_job = self._connection._submit_register_function(name, definition)
        return Job(AsyncJob(native_job))


class _AsyncFunctions:
    """Asynchronous `async_db.functions` facade."""

    __slots__ = ("_connection",)

    def __init__(self, connection: AsyncConnection) -> None:
        self._connection = connection

    def __repr__(self) -> str:
        return "_AsyncFunctions()"

    async def register(
        self, name: str, decorated_udf: Callable[..., object]
    ) -> AsyncJob:
        """Register a decorated UDF and return an [AsyncJob][lancedb.job.AsyncJob]."""
        definition = _udf._build_function_definition(decorated_udf)
        native_job = await self._connection._register_function(name, definition)
        return AsyncJob(native_job)
