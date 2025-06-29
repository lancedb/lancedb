import pyarrow as pa
import pytest

from lancedb.query import LanceHybridQueryBuilder
from lancedb.rerankers.answerdotai import AnswerDotAIReranker

@pytest.mark.asyncio
async def test_combine_hybrid_results_empty():
    schema = pa.schema([
        pa.field("_rowid", pa.int64()),
        pa.field("_distance", pa.float32()),
        pa.field("_score", pa.float32()),
        pa.field("content", pa.string())
    ])

    empty_vector_results = pa.Table.from_batches([], schema=schema)
    empty_fts_results = pa.Table.from_batches([], schema=schema)

    class DummyReranker:
        def rerank_hybrid(self, fts_query, vector_results, fts_results):
            return vector_results

    reranker = DummyReranker()

    results = LanceHybridQueryBuilder._combine_hybrid_results(
        fts_results=empty_fts_results,
        vector_results=empty_vector_results,
        norm="rank",
        fts_query="test query",
        reranker=reranker,
        limit=10,
        with_row_ids=True,
    )
    assert results.num_rows == 0


def test_rerank_empty_results():
    reranker = AnswerDotAIReranker(column="content")

    empty_table = pa.table({"content": pa.array([], pa.string())})

    result = reranker._rerank(empty_table, "test query")
    assert result.num_rows == 0
