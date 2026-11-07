# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Row-level differences between two versions of a table."""

from typing import TYPE_CHECKING, Optional

import pyarrow as pa

from .arrow import AsyncRecordBatchReader
from .background_loop import LOOP

if TYPE_CHECKING:
    from ._lancedb import Table as LanceDBTable


class AsyncTableDelta:
    """The rows that changed between two versions of an async table.

    Get one from :meth:`lancedb.table.AsyncTable.delta`. Every method returns
    an :class:`~lancedb.arrow.AsyncRecordBatchReader` over the changed rows.

    Inserted, updated and upserted rows need stable row ids on the table. Create
    the table with ``storage_options={"new_table_enable_stable_row_ids": "true"}``.
    The returned batches hold the table columns plus ``_rowid``,
    ``_row_created_at_version`` and ``_row_last_updated_at_version``.
    """

    def __init__(
        self,
        inner: "LanceDBTable",
        *,
        compared_against: Optional[int] = None,
        begin_version: Optional[int] = None,
        end_version: Optional[int] = None,
    ):
        self._inner = inner
        self._range = {
            "compared_against": compared_against,
            "begin_version": begin_version,
            "end_version": end_version,
        }

    async def _rows(self, rows: str) -> AsyncRecordBatchReader:
        stream = await self._inner.delta_rows(rows, **self._range)
        return AsyncRecordBatchReader(stream)

    async def inserted_rows(self) -> AsyncRecordBatchReader:
        """Rows created in the version range."""
        return await self._rows("inserted")

    async def updated_rows(self) -> AsyncRecordBatchReader:
        """Rows that existed before the range and were updated in it."""
        return await self._rows("updated")

    async def upserted_rows(self) -> AsyncRecordBatchReader:
        """Rows inserted or updated in the version range."""
        return await self._rows("upserted")

    async def deleted_row_ids(self) -> AsyncRecordBatchReader:
        """The ``_rowid`` of every row removed in the version range."""
        return await self._rows("deleted_row_ids")


class TableDelta:
    """The rows that changed between two versions of a table.

    Get one from :meth:`lancedb.table.Table.delta`. Every method returns a
    ``pyarrow.RecordBatchReader`` that streams the changed rows.

    Inserted, updated and upserted rows need stable row ids on the table. Create
    the table with ``storage_options={"new_table_enable_stable_row_ids": "true"}``.
    The returned batches hold the table columns plus ``_rowid``,
    ``_row_created_at_version`` and ``_row_last_updated_at_version``.
    """

    def __init__(self, delta: AsyncTableDelta):
        self._delta = delta

    @staticmethod
    def _to_reader(async_reader: AsyncRecordBatchReader) -> pa.RecordBatchReader:
        def iter_sync():
            try:
                while True:
                    yield LOOP.run(async_reader.__anext__())
            except StopAsyncIteration:
                return

        return pa.RecordBatchReader.from_batches(async_reader.schema, iter_sync())

    def inserted_rows(self) -> pa.RecordBatchReader:
        """Rows created in the version range."""
        return self._to_reader(LOOP.run(self._delta.inserted_rows()))

    def updated_rows(self) -> pa.RecordBatchReader:
        """Rows that existed before the range and were updated in it."""
        return self._to_reader(LOOP.run(self._delta.updated_rows()))

    def upserted_rows(self) -> pa.RecordBatchReader:
        """Rows inserted or updated in the version range."""
        return self._to_reader(LOOP.run(self._delta.upserted_rows()))

    def deleted_row_ids(self) -> pa.RecordBatchReader:
        """The ``_rowid`` of every row removed in the version range."""
        return self._to_reader(LOOP.run(self._delta.deleted_row_ids()))
