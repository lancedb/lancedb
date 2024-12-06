# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb

import pyarrow as pa
import pytest
import pytest_asyncio

from lancedb.index import FTS
from lancedb.table import AsyncTable


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


@pytest.mark.asyncio
async def test_explain_plan(table: AsyncTable):
    plan = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).explain_plan(True)
    )

    assert "Vector Search Plan" in plan
    assert "KNNVectorDistance" in plan
    assert "FTS Search Plan" in plan
    assert "LanceScan" in plan
