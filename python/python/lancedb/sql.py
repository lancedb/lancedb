# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Lifecycle handles for submitted SQL and DataFrame queries."""

from uuid import UUID

import pyarrow as pa

from lancedb.background_loop import LOOP

from . import _lancedb
from .arrow import AsyncRecordBatchReader

QueryDescription = _lancedb.QueryDescription


class AsyncQuery:
    """A handle to a submitted SQL query on an asynchronous connection."""

    def __init__(self, inner: "_lancedb.SqlQuery"):
        self._inner = inner

    @property
    def id(self) -> UUID:
        """The stable identifier scoped to the connection that submitted it."""
        return self._inner.id

    async def describe(self) -> QueryDescription:
        """Get a point-in-time description of the query."""
        return await self._inner.describe()

    async def reader(self) -> AsyncRecordBatchReader:
        """Wait for the initial result stream and return its Arrow reader.

        Results are single-consumer. Calling this method more than once on the
        same query raises an error. Later batches are streamed as they become
        available without waiting for the full query to finish.
        """
        return AsyncRecordBatchReader(await self._inner.reader())

    async def cancel(self) -> None:
        """Request cancellation of the query."""
        await self._inner.cancel()


class Query:
    """Synchronous counterpart of :class:`AsyncQuery`."""

    def __init__(self, inner: AsyncQuery):
        self._inner = inner

    @property
    def id(self) -> UUID:
        """The stable identifier scoped to the connection that submitted it."""
        return self._inner.id

    def describe(self) -> QueryDescription:
        """Get a point-in-time description of the query."""
        return LOOP.run(self._inner.describe())

    def reader(self) -> pa.RecordBatchReader:
        """Wait for the initial result stream and return a blocking reader.

        Results are single-consumer. Calling this method more than once on the
        same query raises an error. Later batches block only until they become
        available, without waiting for the full query to finish.
        """
        reader = LOOP.run(self._inner.reader())

        def next_batch():
            try:
                return LOOP.run(reader.__anext__())
            except StopAsyncIteration:
                return None

        def batches():
            while (batch := next_batch()) is not None:
                yield batch

        return pa.RecordBatchReader.from_batches(reader.schema, batches())

    def cancel(self) -> None:
        """Request cancellation of the query."""
        LOOP.run(self._inner.cancel())


__all__ = ["AsyncQuery", "Query", "QueryDescription"]
