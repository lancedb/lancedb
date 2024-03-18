from typing import List

import pyarrow as pa

from ._lancedb import RecordBatchStream


class AsyncRecordBatchReader:
    """
    An async iterator over a stream of RecordBatches.

    Also allows access to the schema of the stream
    """

    def __init__(self, inner: RecordBatchStream):
        self.inner_ = inner

    @property
    def schema(self) -> pa.Schema:
        """
        Get the schema of the batches produced by the stream

        Accessing the schema does not consume any data from the stream
        """
        return self.inner_.schema()

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
        next = await self.inner_.next()
        if next is None:
            raise StopAsyncIteration
        return next
