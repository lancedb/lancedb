import pyarrow as pa

from ._lancedb import RecordBatchStream


class AsyncRecordBatchReader(object):
    def __init__(self, inner: RecordBatchStream):
        self.inner_ = inner

    def schema(self) -> pa.Schema:
        self.inner_.schema()

    def __aiter__(self):
        return self

    async def __anext__(self) -> pa.RecordBatch:
        next = await self.inner_.next()
        if next is None:
            raise StopAsyncIteration
        return next
