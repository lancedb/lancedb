# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to SQL queries running on a remote database."""

from typing import TYPE_CHECKING

from lancedb.background_loop import LOOP

from . import _lancedb

if TYPE_CHECKING:
    import pyarrow as pa

QueryDescription = _lancedb.QueryDescription


class AsyncQuery:
    """A handle to a submitted SQL query on an asynchronous connection."""

    def __init__(self, inner: "_lancedb.SqlQuery"):
        self._inner = inner

    @property
    def id(self) -> str:
        """The stable identifier scoped to the connection that submitted it."""
        return self._inner.id

    async def describe(self) -> QueryDescription:
        """Get a point-in-time description of the query."""
        return await self._inner.describe()

    async def result(self) -> "pa.Table":
        """Wait for the query to finish and return its Arrow table."""
        return await self._inner.result()

    async def cancel(self) -> None:
        """Request cancellation of the query."""
        await self._inner.cancel()


class Query:
    """Synchronous counterpart of :class:`AsyncQuery`."""

    def __init__(self, inner: AsyncQuery):
        self._inner = inner

    @property
    def id(self) -> str:
        """The stable identifier scoped to the connection that submitted it."""
        return self._inner.id

    def describe(self) -> QueryDescription:
        """Get a point-in-time description of the query."""
        return LOOP.run(self._inner.describe())

    def result(self) -> "pa.Table":
        """Block until the query finishes and return its Arrow table."""
        return LOOP.run(self._inner.result())

    def cancel(self) -> None:
        """Request cancellation of the query."""
        LOOP.run(self._inner.cancel())


__all__ = ["AsyncQuery", "Query", "QueryDescription"]
