# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Handles to SQL queries running on a remote database."""

from typing import Any, Mapping, Optional, Sequence, Union
from uuid import UUID

import numpy as np
import pyarrow as pa

from lancedb.background_loop import LOOP

from . import _lancedb
from .arrow import AsyncRecordBatchReader

QueryDescription = _lancedb.QueryDescription

QueryParameters = Union[pa.RecordBatch, pa.Table, Sequence[Any], Mapping[str, Any]]
"""Values for a statement's ``$1`` / ``$name`` placeholders.

A sequence binds by position (``$1``, ``$2``, ...), a mapping binds by name
(``$name``), and a one-row ``pyarrow.RecordBatch`` or ``pyarrow.Table`` binds
both ways: column ``i`` is ``$<i + 1>`` and, when its name is not a number,
also ``$<name>``.
"""


def _parameter_array(value: Any) -> pa.Array:
    """One parameter as a one-element array of its own type."""
    if isinstance(value, pa.Scalar):
        return pa.array([value.as_py()], type=value.type)
    # A 1-D array is a vector: keep its element type and make it the
    # fixed-size list a vector column stores, rather than a list of Python
    # floats that would come back as float64.
    if isinstance(value, pa.ChunkedArray):
        value = value.combine_chunks()
    if isinstance(value, np.ndarray) and value.ndim == 1:
        value = pa.array(value)
    if isinstance(value, pa.Array):
        return pa.FixedSizeListArray.from_arrays(value, len(value))
    return pa.array([value])


def to_parameter_batch(
    parameters: Optional[QueryParameters],
) -> Optional[pa.RecordBatch]:
    """Turn ``parameters`` into the one-row batch a parameterized query sends.

    Scalars keep the type pyarrow infers for them (numpy scalars keep their
    dtype), a ``pyarrow.Scalar`` keeps its own type, and a 1-D numpy or
    pyarrow array becomes a ``FixedSizeList`` of its element type, which is
    what a vector column stores.
    """
    if parameters is None:
        return None
    if isinstance(parameters, pa.Table):
        parameters = parameters.combine_chunks()
        batches = parameters.to_batches()
        parameters = (
            batches[0]
            if len(batches) == 1
            else pa.RecordBatch.from_pylist([], schema=parameters.schema)
        )
    if isinstance(parameters, pa.RecordBatch):
        batch = parameters
    elif isinstance(parameters, Mapping):
        batch = pa.RecordBatch.from_arrays(
            [_parameter_array(value) for value in parameters.values()],
            names=[str(name) for name in parameters.keys()],
        )
    elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
        batch = pa.RecordBatch.from_arrays(
            [_parameter_array(value) for value in parameters],
            names=[str(position + 1) for position in range(len(parameters))],
        )
    else:
        raise TypeError(
            "query parameters must be a sequence, a mapping, or a one-row "
            f"pyarrow RecordBatch or Table, not {type(parameters).__name__}"
        )
    if batch.num_rows != 1:
        raise ValueError(
            f"query parameters must be exactly one row, got {batch.num_rows}"
        )
    return batch


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


__all__ = ["AsyncQuery", "Query", "QueryDescription", "QueryParameters"]
