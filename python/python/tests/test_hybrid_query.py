# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb

from lancedb.query import LanceHybridQueryBuilder
from lancedb.rerankers.rrf import RRFReranker
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import pytest_asyncio

from lancedb.index import FTS
from lancedb.table import AsyncTable, Table


@pytest.fixture
def sync_table(tmpdir_factory) -> Table:
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = lancedb.connect(tmp_path)
    data = pa.table(
        {
            "text": pa.array(["a", "b", "cat", "dog"]),
            "vector": pa.array(
                [[0.1, 0.1], [2, 2], [-0.1, -0.1], [0.5, -0.5]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = db.create_table("test", data)
    table.create_fts_index("text", with_position=False, use_tantivy=False)
    return table


@pytest_asyncio.fixture
async def table(tmpdir_factory) -> AsyncTable:
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = await lancedb.connect_async(tmp_path)
    data = pa.table(
        {
            "text": pa.array(["a", "b", "cat", "dog"]),
            "vector": pa.array(
                [[0.1, 0.1], [2, 2], [-0.1, -0.1], [0.5, -0.5]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = await db.create_table("test", data)
    await table.create_index("text", config=FTS(with_position=False))
    return table


@pytest.mark.asyncio
async def test_async_hybrid_query(table: AsyncTable):
    result = await (
        table.query().nearest_to([0.0, 0.4]).nearest_to_text("dog").limit(2).to_arrow()
    )
    assert len(result) == 2
    # ensure we get results that would match well for text and vector
    assert result["text"].to_pylist() == ["a", "dog"]

    # ensure there is no rowid by default
    assert "_rowid" not in result


@pytest.mark.asyncio
async def test_async_hybrid_query_with_row_ids(table: AsyncTable):
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .limit(2)
        .with_row_id()
        .to_arrow()
    )
    assert len(result) == 2
    # ensure we get results that would match well for text and vector
    assert result["text"].to_pylist() == ["a", "dog"]
    assert result["_rowid"].to_pylist() == [0, 3]


@pytest.mark.asyncio
async def test_async_hybrid_query_filters(table: AsyncTable):
    # test that query params are passed down from the regular builder to
    # child vector/fts builders
    result = await (
        table.query()
        .where("text not in ('a', 'dog')")
        .nearest_to([0.3, 0.3])
        .nearest_to_text("*a*")
        .distance_type("l2")
        .limit(2)
        .to_arrow()
    )
    assert len(result) == 2
    # ensure we get results that would match well for text and vector
    assert result["text"].to_pylist() == ["cat", "b"]


@pytest.mark.asyncio
async def test_async_hybrid_query_default_limit(table: AsyncTable):
    # add 10 new rows
    new_rows = []
    for i in range(100):
        if i < 2:
            new_rows.append({"text": "close_vec", "vector": [0.1, 0.1]})
        else:
            new_rows.append({"text": "far_vec", "vector": [5 * i, 5 * i]})
    await table.add(new_rows)
    result = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).to_arrow()
    )

    # assert we got the default limit of 10
    assert len(result) == 10

    # assert we got the closest vectors and the text searched for
    texts = result["text"].to_pylist()
    assert texts.count("close_vec") == 2
    assert texts.count("dog") == 1
    assert texts.count("a") == 1


def test_hybrid_query_distance_range(sync_table: Table):
    reranker = RRFReranker(return_score="all")
    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("cat and dog")
        .distance_range(lower_bound=0.2, upper_bound=0.5)
        .rerank(reranker)
        .limit(2)
        .to_arrow()
    )
    assert len(result) == 2
    print(result)
    for dist in result["_distance"]:
        if dist.is_valid:
            assert 0.2 <= dist.as_py() <= 0.5


@pytest.mark.asyncio
async def test_hybrid_query_distance_range_async(table: AsyncTable):
    reranker = RRFReranker(return_score="all")
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("cat and dog")
        .distance_range(lower_bound=0.2, upper_bound=0.5)
        .rerank(reranker)
        .limit(2)
        .to_arrow()
    )
    assert len(result) == 2
    for dist in result["_distance"]:
        if dist.is_valid:
            assert 0.2 <= dist.as_py() <= 0.5


@pytest.mark.asyncio
async def test_explain_plan(table: AsyncTable):
    plan = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).explain_plan(True)
    )

    assert "Vector Search Plan" in plan
    assert "KNNVectorDistance" in plan
    assert "FTS Search Plan" in plan
    assert "LanceRead" in plan


@pytest.mark.asyncio
async def test_analyze_plan(table: AsyncTable):
    res = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).analyze_plan()
    )

    assert "AnalyzeExec" in res
    assert "metrics=" in res


def test_normalize_scores():
    cases = [
        (pa.array([0.1, 0.4]), pa.array([0.0, 1.0])),
        (pa.array([2.0, 10.0, 20.0]), pa.array([0.0, 8.0 / 18.0, 1.0])),
        (pa.array([0.0, 0.0, 0.0]), pa.array([0.0, 0.0, 0.0])),
        (pa.array([10.0, 9.9999999999999]), pa.array([0.0, 0.0])),
    ]

    for input, expected in cases:
        for invert in [True, False]:
            result = LanceHybridQueryBuilder._normalize_scores(input, invert)

            if invert:
                expected = pc.subtract(1.0, expected)

            assert pc.equal(result, expected), (
                f"Expected {expected} but got {result} for invert={invert}"
            )


@pytest.mark.asyncio
async def test_async_hybrid_select_columns(table: AsyncTable):
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .select(["text"])
        .limit(2)
        .to_arrow()
    )
    # User-requested columns come first, auto-appended _relevance_score last
    assert result.column_names == ["text", "_relevance_score"]


@pytest.mark.asyncio
async def test_async_hybrid_select_relevance_score(table: AsyncTable):
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .select(["text", "_relevance_score"])
        .limit(2)
        .to_arrow()
    )
    assert result.column_names == ["text", "_relevance_score"]


@pytest.mark.asyncio
async def test_async_hybrid_select_score_error(table: AsyncTable):
    with pytest.raises(ValueError, match="_relevance_score"):
        await (
            table.query()
            .nearest_to([0.0, 0.4])
            .nearest_to_text("dog")
            .select(["_score"])
            .limit(2)
            .to_arrow()
        )
    with pytest.raises(ValueError, match="_relevance_score"):
        await (
            table.query()
            .nearest_to([0.0, 0.4])
            .nearest_to_text("dog")
            .select(["_distance"])
            .limit(2)
            .to_arrow()
        )


def test_sync_hybrid_select_columns(sync_table: Table):
    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["text"])
        .limit(2)
        .to_arrow()
    )
    assert result.column_names == ["text", "_relevance_score"]


def test_sync_hybrid_select_dynamic(sync_table: Table):
    """Dict (dynamic) selects are pushed to sub-queries and should work."""
    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select({"upper_text": "upper(text)"})
        .limit(2)
        .to_arrow()
    )
    assert "upper_text" in result.column_names
    assert "_relevance_score" in result.column_names


def test_sync_hybrid_to_arrow_restores_columns(sync_table: Table):
    """to_arrow() must restore _columns on success and after an exception."""
    builder = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["text"])
        .limit(2)
    )
    # First call succeeds
    result = builder.to_arrow()
    assert result.column_names == ["text", "_relevance_score"]

    # Second call on the same builder should also apply the selection
    result = builder.to_arrow()
    assert result.column_names == ["text", "_relevance_score"]

    # Force an error via an invalid filter, then verify _columns survived
    builder.where("THIS IS NOT VALID SQL")
    with pytest.raises(Exception):
        builder.to_arrow()
    assert builder._columns == ["text"]


@pytest.mark.asyncio
async def test_async_hybrid_select_dynamic(table: AsyncTable):
    """Dict (dynamic) selects are pushed to sub-queries and should work."""
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .select({"upper_text": "upper(text)"})
        .limit(2)
        .to_arrow()
    )
    assert "upper_text" in result.column_names
    assert "_relevance_score" in result.column_names


@pytest.mark.asyncio
async def test_async_hybrid_select_dict_clears_stale_list(table: AsyncTable):
    """Switching from a list select to a dict select must not apply the stale list."""
    query = table.query().nearest_to([0.0, 0.4]).nearest_to_text("dog").limit(2)
    # First set a list select
    query.select(["text"])
    # Then override with a dict select — the list must not be applied
    query.select({"upper_text": "upper(text)"})
    result = await query.to_arrow()
    assert "upper_text" in result.column_names
    assert "_relevance_score" in result.column_names


def test_sync_hybrid_select_score_error(sync_table: Table):
    with pytest.raises(ValueError, match="_relevance_score"):
        (
            sync_table.search(query_type="hybrid")
            .vector([0.0, 0.4])
            .text("dog")
            .select(["_score"])
            .limit(2)
            .to_arrow()
        )
    with pytest.raises(ValueError, match="_relevance_score"):
        (
            sync_table.search(query_type="hybrid")
            .vector([0.0, 0.4])
            .text("dog")
            .select(["_distance"])
            .limit(2)
            .to_arrow()
        )
