# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import List, Optional, Tuple, Union

import pyarrow as pa

from ._lancedb import RecordBatchStream


class AsyncRecordBatchReader:
    """
    An async iterator over a stream of RecordBatches.

    Also allows access to the schema of the stream
    """

    def __init__(
        self,
        inner: Union[RecordBatchStream, pa.Table],
        max_batch_length: Optional[int] = None,
    ):
        """

        Attributes
        ----------
        schema : pa.Schema
            The schema of the batches produced by the stream.
            Accessing the schema does not consume any data from the stream
        """
        if isinstance(inner, pa.Table):
            self._inner = self._async_iter_from_table(inner, max_batch_length)
            self.schema: pa.Schema = inner.schema
        elif isinstance(inner, RecordBatchStream):
            self._inner = inner
            self.schema: pa.Schema = inner.schema
        else:
            raise TypeError("inner must be a RecordBatchStream or a Table")

    async def read_all(self) -> List[pa.RecordBatch]:
        """
        Read all the record batches from the stream

        This consumes the entire stream and returns a list of record batches

        If there are a lot of results this may consume a lot of memory
        """
        return [batch async for batch in self]

    def __aiter__(self):
        return self

    async def __anext__(self) -> pa.RecordBatch:
        return await self._inner.__anext__()

    @staticmethod
    async def _async_iter_from_table(
        table: pa.Table, max_batch_length: Optional[int] = None
    ):
        """
        Create an AsyncRecordBatchReader from a Table

        This is useful when you have a Table that you want to iterate
        over asynchronously
        """
        batches = table.to_batches(max_chunksize=max_batch_length)
        for batch in batches:
            yield batch


def peek_reader(
    reader: pa.RecordBatchReader,
) -> Tuple[pa.RecordBatch, pa.RecordBatchReader]:
    if not isinstance(reader, pa.RecordBatchReader):
        raise TypeError("reader must be a RecordBatchReader")
    batch = reader.read_next_batch()

    def all_batches():
        yield batch
        yield from reader

    return batch, pa.RecordBatchReader.from_batches(batch.schema, all_batches())
