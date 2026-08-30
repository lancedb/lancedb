# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to SQL queries running on a remote database."""

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
    def id(self) -> str:
        """The stable identifier scoped to the connection that submitted it."""
        return self._inner.id

    async def describe(self) -> QueryDescription:
        """Get a point-in-time description of the query."""
        return await self._inner.describe()

    async def result(self) -> AsyncRecordBatchReader:
        """Stream Arrow record batches as they become available.

        Results are single-consumer. Calling this method more than once on the
        same query raises an error.
        """
        return AsyncRecordBatchReader(await self._inner.result())

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

    def result(self) -> pa.RecordBatchReader:
        """Return a blocking reader that streams available Arrow batches.

        Results are single-consumer. Calling this method more than once on the
        same query raises an error.
        """
        reader = LOOP.run(self._inner.result())

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
