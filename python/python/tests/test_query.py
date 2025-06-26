# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import List, Union
import unittest.mock as mock
from datetime import timedelta
from pathlib import Path

import lancedb
from lancedb.db import AsyncConnection
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import get_registry, register
from lancedb.index import FTS, IvfPq
import lancedb.pydantic
import numpy as np
import pandas.testing as tm
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import pytest_asyncio
from lancedb.pydantic import LanceModel, Vector
from lancedb.query import (
    AsyncFTSQuery,
    AsyncHybridQuery,
    AsyncQueryBase,
    AsyncVectorQuery,
    LanceVectorQueryBuilder,
    MatchQuery,
    PhraseQuery,
    Query,
    FullTextSearchQuery,
)
from lancedb.rerankers.cross_encoder import CrossEncoderReranker
from lancedb.table import AsyncTable, LanceTable
from utils import exception_output
from importlib.util import find_spec


@pytest.fixture(scope="module")
def table(tmpdir_factory) -> lancedb.table.Table:
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = lancedb.connect(tmp_path)
    df = pa.table(
        {
            "vector": pa.array(
                [[1, 2], [3, 4]], type=pa.list_(pa.float32(), list_size=2)
            ),
            "id": pa.array([1, 2]),
            "str_field": pa.array(["a", "b"]),
            "float_field": pa.array([1.0, 2.0]),
        }
    )
    return db.create_table("test", df)


@pytest_asyncio.fixture
async def table_async(tmp_path) -> AsyncTable:
    conn = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    data = pa.table(
        {
            "vector": pa.array(
                [[1, 2], [3, 4]], type=pa.list_(pa.float32(), list_size=2)
            ),
            "id": pa.array([1, 2]),
            "str_field": pa.array(["a", "b"]),
            "float_field": pa.array([1.0, 2.0]),
            "text": pa.array(["a", "dog"]),
        }
    )
    return await conn.create_table("test", data)


@pytest_asyncio.fixture
async def table_struct_async(tmp_path) -> AsyncTable:
    conn = await lancedb.connect_async(
        tmp_path, read_consistency_interval=timedelta(seconds=0)
    )
    struct = pa.array([{"n_legs": 2, "animals": "Parrot"}, {"year": 2022, "n_legs": 4}])
    month = pa.array([4, 6])
    table = pa.Table.from_arrays([struct, month], names=["a", "month"])
    return await conn.create_table("test_struct", table)


@pytest.fixture
def multivec_table(vector_value_type=pa.float32()) -> lancedb.table.Table:
    db = lancedb.connect("memory://")
    # Generate 256 rows of data
    num_rows = 256

    # Generate data for each column
    vector_data = [
        [[i, i + 1], [i + 2, i + 3]] for i in range(num_rows)
    ]  # Adjust to match nested structure
    id_data = list(range(1, num_rows + 1))
    float_field_data = [float(i) for i in range(1, num_rows + 1)]

    # Create the Arrow table
    df = pa.table(
        {
            "vector": pa.array(
                vector_data, type=pa.list_(pa.list_(vector_value_type, list_size=2))
            ),
            "id": pa.array(id_data),
            "float_field": pa.array(float_field_data),
        }
    )
    return db.create_table("test", df)


@pytest_asyncio.fixture
async def multivec_table_async(vector_value_type=pa.float32()) -> AsyncTable:
    conn = await lancedb.connect_async(
        "memory://", read_consistency_interval=timedelta(seconds=0)
    )
    # Generate 256 rows of data
    num_rows = 256

    # Generate data for each column
    vector_data = [
        [[i, i + 1], [i + 2, i + 3]] for i in range(num_rows)
    ]  # Adjust to match nested structure
    id_data = list(range(1, num_rows + 1))
    float_field_data = [float(i) for i in range(1, num_rows + 1)]

    # Create the Arrow table
    df = pa.table(
        {
            "vector": pa.array(
                vector_data, type=pa.list_(pa.list_(vector_value_type, list_size=2))
            ),
            "id": pa.array(id_data),
            "float_field": pa.array(float_field_data),
        }
    )
    return await conn.create_table("test_async", df)


def test_cast(table):
    class TestModel(LanceModel):
        vector: Vector(2)
        id: int
        str_field: str
        float_field: float

    q = LanceVectorQueryBuilder(table, [0, 0], "vector").limit(1)
    results = q.to_pydantic(TestModel)
    assert len(results) == 1
    r0 = results[0]
    assert isinstance(r0, TestModel)
    assert r0.id == 1
    assert r0.vector == [1, 2]
    assert r0.str_field == "a"
    assert r0.float_field == 1.0


def test_offset(table):
    results_without_offset = LanceVectorQueryBuilder(table, [0, 0], "vector")
    assert len(results_without_offset.to_pandas()) == 2
    results_with_offset = LanceVectorQueryBuilder(table, [0, 0], "vector").offset(1)
    assert len(results_with_offset.to_pandas()) == 1


def test_query_builder(table):
    rs = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .limit(1)
        .select(["id", "vector"])
        .to_list()
    )
    assert rs[0]["id"] == 1
    assert all(np.array(rs[0]["vector"]) == [1, 2])


def test_with_row_id(table: lancedb.table.Table):
    rs = table.search().with_row_id(True).to_arrow()
    assert "_rowid" in rs.column_names
    assert rs["_rowid"].to_pylist() == [0, 1]


def test_distance_range(table: lancedb.table.Table):
    q = [0, 0]
    rs = table.search(q).to_arrow()
    dists = rs["_distance"].to_pylist()
    min_dist = dists[0]
    max_dist = dists[-1]

    res = table.search(q).distance_range(upper_bound=min_dist).to_arrow()
    assert len(res) == 0

    res = table.search(q).distance_range(lower_bound=max_dist).to_arrow()
    assert len(res) == 1
    assert res["_distance"].to_pylist() == [max_dist]

    res = table.search(q).distance_range(upper_bound=max_dist).to_arrow()
    assert len(res) == 1
    assert res["_distance"].to_pylist() == [min_dist]

    res = table.search(q).distance_range(lower_bound=min_dist).to_arrow()
    assert len(res) == 2
    assert res["_distance"].to_pylist() == [min_dist, max_dist]


@pytest.mark.asyncio
async def test_distance_range_async(table_async: AsyncTable):
    q = [0, 0]
    rs = await table_async.query().nearest_to(q).to_arrow()
    dists = rs["_distance"].to_pylist()
    min_dist = dists[0]
    max_dist = dists[-1]

    res = (
        await table_async.query()
        .nearest_to(q)
        .distance_range(upper_bound=min_dist)
        .to_arrow()
    )
    assert len(res) == 0

    res = (
        await table_async.query()
        .nearest_to(q)
        .distance_range(lower_bound=max_dist)
        .to_arrow()
    )
    assert len(res) == 1
    assert res["_distance"].to_pylist() == [max_dist]

    res = (
        await table_async.query()
        .nearest_to(q)
        .distance_range(upper_bound=max_dist)
        .to_arrow()
    )
    assert len(res) == 1
    assert res["_distance"].to_pylist() == [min_dist]

    res = (
        await table_async.query()
        .nearest_to(q)
        .distance_range(lower_bound=min_dist)
        .to_arrow()
    )
    assert len(res) == 2
    assert res["_distance"].to_pylist() == [min_dist, max_dist]


@pytest.mark.asyncio
async def test_distance_range_with_new_rows_async():
    conn = await lancedb.connect_async(
        "memory://", read_consistency_interval=timedelta(seconds=0)
    )
    data = pa.table(
        {
            "vector": pa.FixedShapeTensorArray.from_numpy_ndarray(
                np.random.rand(256, 2)
            ),
        }
    )
    table = await conn.create_table("test", data)
    await table.create_index(
        "vector", config=IvfPq(num_partitions=1, num_sub_vectors=2)
    )

    q = [0, 0]
    rs = await table.query().nearest_to(q).to_arrow()
    dists = rs["_distance"].to_pylist()
    min_dist = dists[0]
    max_dist = dists[-1]

    # append more rows so that execution plan would be mixed with ANN & Flat KNN
    new_data = pa.table(
        {
            "vector": pa.FixedShapeTensorArray.from_numpy_ndarray(np.random.rand(4, 2)),
        }
    )
    await table.add(new_data)

    res = (
        await table.query()
        .nearest_to(q)
        .distance_range(upper_bound=min_dist)
        .to_arrow()
    )
    assert len(res) == 0

    res = (
        await table.query()
        .nearest_to(q)
        .distance_range(lower_bound=max_dist)
        .to_arrow()
    )
    for dist in res["_distance"].to_pylist():
        assert dist >= max_dist

    res = (
        await table.query()
        .nearest_to(q)
        .distance_range(upper_bound=max_dist)
        .to_arrow()
    )
    for dist in res["_distance"].to_pylist():
        assert dist < max_dist

    res = (
        await table.query()
        .nearest_to(q)
        .distance_range(lower_bound=min_dist)
        .to_arrow()
    )
    for dist in res["_distance"].to_pylist():
        assert dist >= min_dist


@pytest.mark.parametrize(
    "multivec_table", [pa.float16(), pa.float32(), pa.float64()], indirect=True
)
def test_multivector(multivec_table: lancedb.table.Table):
    # create index on multivector
    multivec_table.create_index(
        metric="cosine",
        vector_column_name="vector",
        index_type="IVF_PQ",
        num_partitions=1,
        num_sub_vectors=2,
    )

    # query with single vector
    q = [1, 2]
    rs = multivec_table.search(q).to_arrow()

    # query with multiple vectors
    q = [[1, 2], [1, 2]]
    rs2 = multivec_table.search(q).to_arrow()
    assert len(rs2) == len(rs)
    for i in range(2):
        assert rs2["_distance"][i].as_py() == rs["_distance"][i].as_py() * 2

    # can't query with vector that dim not matched
    with pytest.raises(Exception):
        multivec_table.search([1, 2, 3]).to_arrow()
    # can't query with vector list that some dim not matched
    with pytest.raises(Exception):
        multivec_table.search([[1, 2], [1, 2, 3]]).to_arrow()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "multivec_table_async", [pa.float16(), pa.float32(), pa.float64()], indirect=True
)
async def test_multivector_async(multivec_table_async: AsyncTable):
    # create index on multivector
    await multivec_table_async.create_index(
        "vector",
        config=IvfPq(distance_type="cosine", num_partitions=1, num_sub_vectors=2),
    )

    # query with single vector
    q = [1, 2]
    rs = await multivec_table_async.query().nearest_to(q).to_arrow()

    # query with multiple vectors
    q = [[1, 2], [1, 2]]
    rs2 = await multivec_table_async.query().nearest_to(q).to_arrow()
    assert len(rs2) == len(rs)
    for i in range(2):
        assert rs2["_distance"][i].as_py() == rs["_distance"][i].as_py() * 2

    # can't query with vector that dim not matched
    with pytest.raises(Exception):
        await multivec_table_async.query().nearest_to([1, 2, 3]).to_arrow()
    # can't query with vector list that some dim not matched
    with pytest.raises(Exception):
        await multivec_table_async.query().nearest_to([[1, 2], [1, 2, 3]]).to_arrow()


def test_vector_query_with_no_limit(table):
    with pytest.raises(ValueError):
        LanceVectorQueryBuilder(table, [0, 0], "vector").limit(0).select(
            ["id", "vector"]
        ).to_list()

    with pytest.raises(ValueError):
        LanceVectorQueryBuilder(table, [0, 0], "vector").limit(None).select(
            ["id", "vector"]
        ).to_list()


def test_query_builder_batches(table):
    rs = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .limit(2)
        .select(["id", "vector"])
        .to_batches(1)
    )
    rs_list = []
    for item in rs:
        rs_list.append(item)
        assert isinstance(item, pa.RecordBatch)
    assert len(rs_list) == 2
    assert len(rs_list[0]["id"]) == 1
    assert all(rs_list[0].to_pandas()["vector"][0] == [1.0, 2.0])
    assert rs_list[0].to_pandas()["id"][0] == 1
    assert all(rs_list[1].to_pandas()["vector"][0] == [3.0, 4.0])
    assert rs_list[1].to_pandas()["id"][0] == 2

    rs = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .limit(2)
        .select(["id", "vector"])
        .to_batches(2)
    )
    rs_list = []
    for item in rs:
        rs_list.append(item)
        assert isinstance(item, pa.RecordBatch)
    assert len(rs_list) == 1
    assert len(rs_list[0]["id"]) == 2
    rs_list = rs_list[0].to_pandas()
    assert rs_list["id"][0] == 1
    assert rs_list["id"][1] == 2


def test_dynamic_projection(table):
    rs = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .limit(1)
        .select({"id": "id", "id2": "id * 2"})
        .to_list()
    )
    assert rs[0]["id"] == 1
    assert rs[0]["id2"] == 2


def test_query_builder_with_filter(table):
    rs = LanceVectorQueryBuilder(table, [0, 0], "vector").where("id = 2").to_list()
    assert rs[0]["id"] == 2
    assert all(np.array(rs[0]["vector"]) == [3, 4])


def test_invalid_nprobes_sync(table):
    with pytest.raises(ValueError, match="minimum_nprobes must be greater than 0"):
        LanceVectorQueryBuilder(table, [0, 0], "vector").minimum_nprobes(0).to_list()
    with pytest.raises(
        ValueError, match="maximum_nprobes must be greater than minimum_nprobes"
    ):
        LanceVectorQueryBuilder(table, [0, 0], "vector").maximum_nprobes(5).to_list()
    with pytest.raises(
        ValueError, match="minimum_nprobes must be less or equal to maximum_nprobes"
    ):
        LanceVectorQueryBuilder(table, [0, 0], "vector").minimum_nprobes(100).to_list()


@pytest.mark.asyncio
async def test_invalid_nprobes_async(table_async: AsyncTable):
    with pytest.raises(ValueError, match="minimum_nprobes must be greater than 0"):
        await table_async.vector_search([0, 0]).minimum_nprobes(0).to_list()
    with pytest.raises(
        ValueError, match="maximum_nprobes must be greater than minimum_nprobes"
    ):
        await table_async.vector_search([0, 0]).maximum_nprobes(5).to_list()
    with pytest.raises(
        ValueError, match="minimum_nprobes must be less or equal to maximum_nprobes"
    ):
        await table_async.vector_search([0, 0]).minimum_nprobes(100).to_list()


def test_query_builder_with_prefilter(table):
    df = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .where("id = 2", prefilter=True)
        .limit(1)
        .to_pandas()
    )
    assert df["id"].values[0] == 2
    assert all(df["vector"].values[0] == [3, 4])

    df = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .where("id = 2", prefilter=False)
        .limit(1)
        .to_pandas()
    )
    assert len(df) == 0

    # ensure the default prefilter = True
    df = (
        LanceVectorQueryBuilder(table, [0, 0], "vector")
        .where("id = 2")
        .limit(1)
        .to_pandas()
    )
    assert df["id"].values[0] == 2
    assert all(df["vector"].values[0] == [3, 4])


def test_query_builder_with_metric(table):
    query = [4, 8]
    vector_column_name = "vector"
    df_default = LanceVectorQueryBuilder(table, query, vector_column_name).to_pandas()
    df_l2 = (
        LanceVectorQueryBuilder(table, query, vector_column_name)
        .distance_type("l2")
        .to_pandas()
    )
    tm.assert_frame_equal(df_default, df_l2)

    df_cosine = (
        LanceVectorQueryBuilder(table, query, vector_column_name)
        .distance_type("cosine")
        .limit(1)
        .to_pandas()
    )
    assert df_cosine._distance[0] == pytest.approx(
        cosine_distance(query, df_cosine.vector[0]),
        abs=1e-6,
    )
    assert 0 <= df_cosine._distance[0] <= 1


def test_query_builder_with_different_vector_column():
    table = mock.MagicMock(spec=LanceTable)
    query = [4, 8]
    vector_column_name = "foo_vector"
    builder = (
        LanceVectorQueryBuilder(table, query, vector_column_name)
        .distance_type("cosine")
        .where("b < 10")
        .select(["b"])
        .limit(2)
    )
    ds = mock.Mock()
    table.to_lance.return_value = ds
    builder.to_arrow()
    table._execute_query.assert_called_once_with(
        Query(
            vector=query,
            filter="b < 10",
            limit=2,
            distance_type="cosine",
            columns=["b"],
            vector_column="foo_vector",
        ),
        batch_size=None,
        timeout=None,
    )


def cosine_distance(vec1, vec2):
    return 1 - np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


async def check_query(
    query: AsyncQueryBase, *, expected_num_rows=None, expected_columns=None
):
    num_rows = 0
    results = await query.to_batches()
    async for batch in results:
        if expected_columns is not None:
            assert batch.schema.names == expected_columns
        num_rows += batch.num_rows
    if expected_num_rows is not None:
        assert num_rows == expected_num_rows


@pytest.mark.asyncio
async def test_query_async(table_async: AsyncTable):
    await check_query(
        table_async.query(),
        expected_num_rows=2,
        expected_columns=["vector", "id", "str_field", "float_field", "text"],
    )
    await check_query(table_async.query().where("id = 2"), expected_num_rows=1)
    await check_query(
        table_async.query().select(["id", "vector"]), expected_columns=["id", "vector"]
    )
    await check_query(
        table_async.query().select({"foo": "id", "bar": "id + 1"}),
        expected_columns=["foo", "bar"],
    )

    await check_query(table_async.query().limit(1), expected_num_rows=1)
    await check_query(table_async.query().offset(1), expected_num_rows=1)

    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])), expected_num_rows=2
    )
    # Support different types of inputs for the vector query
    for vector_query in [
        [1, 2],
        [1.0, 2.0],
        np.array([1, 2]),
        (1, 2),
    ]:
        await check_query(
            table_async.query().nearest_to(vector_query), expected_num_rows=2
        )

    # No easy way to check these vector query parameters are doing what they say.  We
    # just check that they don't raise exceptions and assume this is tested at a lower
    # level.
    await check_query(
        table_async.query().where("id = 2").nearest_to(pa.array([1, 2])).postfilter(),
        expected_num_rows=1,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).refine_factor(1),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).nprobes(10),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).minimum_nprobes(10),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).maximum_nprobes(30),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query()
        .nearest_to(pa.array([1, 2]))
        .minimum_nprobes(10)
        .maximum_nprobes(20),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).bypass_vector_index(),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).distance_type("dot"),
        expected_num_rows=2,
    )
    await check_query(
        table_async.query().nearest_to(pa.array([1, 2])).distance_type("DoT"),
        expected_num_rows=2,
    )

    # Make sure we can use a vector query as a base query (e.g. call limit on it)
    # Also make sure `vector_search` works
    await check_query(table_async.vector_search([1, 2]).limit(1), expected_num_rows=1)

    # Also check an empty query
    await check_query(table_async.query().where("id < 0"), expected_num_rows=0)

    # with row id
    await check_query(
        table_async.query().select(["id", "vector"]).with_row_id(),
        expected_columns=["id", "vector", "_rowid"],
    )


@pytest.mark.asyncio
@pytest.mark.slow
async def test_query_reranked_async(table_async: AsyncTable):
    # CrossEncoderReranker requires torch
    if find_spec("torch") is None:
        pytest.skip("torch not installed")

    # FTS with rerank
    await table_async.create_index("text", config=FTS(with_position=False))
    await check_query(
        table_async.query().nearest_to_text("dog").rerank(CrossEncoderReranker()),
        expected_num_rows=1,
    )

    # Vector query with rerank
    await check_query(
        table_async.vector_search([1, 2]).rerank(
            CrossEncoderReranker(), query_string="dog"
        ),
        expected_num_rows=2,
    )


@pytest.mark.asyncio
async def test_query_to_arrow_async(table_async: AsyncTable):
    table = await table_async.to_arrow()
    assert table.num_rows == 2
    assert table.num_columns == 5

    table = await table_async.query().to_arrow()
    assert table.num_rows == 2
    assert table.num_columns == 5

    table = await table_async.query().where("id < 0").to_arrow()
    assert table.num_rows == 0
    assert table.num_columns == 5


@pytest.mark.asyncio
async def test_query_to_pandas_async(table_async: AsyncTable):
    df = await table_async.to_pandas()
    assert df.shape == (2, 5)

    df = await table_async.query().to_pandas()
    assert df.shape == (2, 5)

    df = await table_async.query().where("id < 0").to_pandas()
    assert df.shape == (0, 5)


@pytest.mark.asyncio
async def test_query_to_pandas_flatten_async(table_struct_async: AsyncTable):
    df = await table_struct_async.query().to_pandas()
    assert df.shape == (2, 2)

    df = await table_struct_async.query().to_pandas(flatten=True)
    assert df.shape == (2, 4)


@pytest.mark.asyncio
async def test_query_to_polars_async(table_async: AsyncTable):
    schema = await table_async.schema()
    num_columns = len(schema.names)
    df = await table_async.query().to_polars()
    assert df.shape == (2, num_columns)

    df = await table_async.query().where("id < 0").to_polars()
    assert df.shape == (0, num_columns)


@pytest.mark.asyncio
async def test_none_query(table_async: AsyncTable):
    with pytest.raises(ValueError):
        await table_async.query().nearest_to(None).to_arrow()


@pytest.mark.asyncio
async def test_fast_search_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    vectors = pa.FixedShapeTensorArray.from_numpy_ndarray(
        np.random.rand(256, 32)
    ).storage
    table = await db.create_table("test", pa.table({"vector": vectors}))
    await table.create_index(
        "vector", config=IvfPq(num_partitions=1, num_sub_vectors=1)
    )
    await table.add(pa.table({"vector": vectors}))

    q = [1.0] * 32
    plan = await table.query().nearest_to(q).explain_plan(True)
    assert "LanceScan" in plan
    plan = await table.query().nearest_to(q).fast_search().explain_plan(True)
    assert "LanceScan" not in plan


def test_analyze_plan(table):
    q = LanceVectorQueryBuilder(table, [0, 0], "vector")
    res = q.analyze_plan()
    assert "AnalyzeExec" in res
    assert "metrics=" in res


@pytest.mark.asyncio
async def test_analyze_plan_async(table_async: AsyncTable):
    res = await table_async.query().nearest_to(pa.array([1, 2])).analyze_plan()
    assert "AnalyzeExec" in res
    assert "metrics=" in res


def test_explain_plan(table):
    q = LanceVectorQueryBuilder(table, [0, 0], "vector")
    plan = q.explain_plan(verbose=True)
    assert "KNN" in plan


@pytest.mark.asyncio
async def test_explain_plan_async(table_async: AsyncTable):
    plan = await table_async.query().nearest_to(pa.array([1, 2])).explain_plan(True)
    assert "KNN" in plan


@pytest.mark.asyncio
async def test_explain_plan_fts(table_async: AsyncTable):
    """Test explain plan for FTS queries"""
    # Create FTS index
    from lancedb.index import FTS

    await table_async.create_index("text", config=FTS())

    # Test pure FTS query
    query = await table_async.search("dog", query_type="fts", fts_columns="text")
    plan = await query.explain_plan()
    # Currently this shows only LanceScan (issue #2465), but should show FTS details
    assert "LanceScan" in plan

    # Test FTS query with limit
    query_with_limit = await table_async.search(
        "dog", query_type="fts", fts_columns="text"
    )
    plan_with_limit = await query_with_limit.limit(1).explain_plan()
    assert "LanceScan" in plan_with_limit
    # TODO: Should also assert limit is shown in plan once issue is fixed

    # Test FTS query with offset and limit
    query_with_offset = await table_async.search(
        "dog", query_type="fts", fts_columns="text"
    )
    plan_with_offset = await query_with_offset.offset(1).limit(1).explain_plan()
    assert "LanceScan" in plan_with_offset
    # TODO: Should also assert offset/limit are shown in plan once issue is fixed


@pytest.mark.asyncio
async def test_explain_plan_vector_with_limit_offset(table_async: AsyncTable):
    """Test explain plan for vector queries with limit and offset"""
    # Test vector query with limit
    plan_with_limit = await (
        table_async.query().nearest_to(pa.array([1, 2])).limit(1).explain_plan()
    )
    assert "KNN" in plan_with_limit
    assert "GlobalLimitExec: skip=0, fetch=1" in plan_with_limit

    # Test vector query with offset and limit
    plan_with_offset = await (
        table_async.query()
        .nearest_to(pa.array([1, 2]))
        .offset(1)
        .limit(1)
        .explain_plan()
    )
    assert "KNN" in plan_with_offset
    assert "GlobalLimitExec: skip=1, fetch=1" in plan_with_offset


@pytest.mark.asyncio
async def test_explain_plan_with_filters(table_async: AsyncTable):
    """Test explain plan for queries with filters"""
    # Test vector query with filter
    plan_with_filter = await (
        table_async.query().nearest_to(pa.array([1, 2])).where("id = 1").explain_plan()
    )
    assert "KNN" in plan_with_filter
    assert "FilterExec" in plan_with_filter

    # Test FTS query with filter
    from lancedb.index import FTS

    await table_async.create_index("text", config=FTS())
    query_fts_filter = await table_async.search(
        "dog", query_type="fts", fts_columns="text"
    )
    plan_fts_filter = await query_fts_filter.where("id = 1").explain_plan()
    assert "LanceScan" in plan_fts_filter
    # TODO: Should show filter details once FTS explain plan is fixed


@pytest.mark.asyncio
async def test_query_camelcase_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("test", pa.table({"camelCase": pa.array([1, 2])}))

    result = await table.query().select(["camelCase"]).to_arrow()
    assert result == pa.table({"camelCase": pa.array([1, 2])})


@pytest.mark.asyncio
async def test_query_to_list_async(table_async: AsyncTable):
    list = await table_async.query().to_list()
    assert len(list) == 2
    assert list[0]["vector"] == [1, 2]
    assert list[1]["vector"] == [3, 4]


@pytest.mark.asyncio
async def test_query_with_f16(tmp_path: Path):
    db = await lancedb.connect_async(tmp_path)
    f16_arr = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float16)

    df = pa.table(
        {
            "vector": pa.FixedSizeListArray.from_arrays(f16_arr, 2),
            "id": pa.array([1, 2]),
        }
    )
    tbl = await db.create_table("test", df)
    results = await tbl.vector_search([np.float16(1), np.float16(2)]).to_pandas()
    assert len(results) == 2


@pytest.mark.asyncio
async def test_query_search_auto(mem_db_async: AsyncConnection):
    nrows = 1000
    data = pa.table(
        {
            "text": [str(i) for i in range(nrows)],
        }
    )

    @register("test2")
    class TestEmbedding(TextEmbeddingFunction):
        def ndims(self):
            return 4

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            embeddings = []
            for text in texts:
                vec = np.array([float(text) / 1000] * self.ndims())
                embeddings.append(vec)
            return embeddings

    registry = get_registry()
    func = registry.get("test2").create()

    class TestModel(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    tbl = await mem_db_async.create_table("test", data, schema=TestModel)

    funcs = await tbl.embedding_functions()
    assert len(funcs) == 1

    # No FTS or vector index
    # Search for vector -> vector query
    q = [0.1] * 4
    query = await tbl.search(q)
    assert isinstance(query, AsyncVectorQuery)

    # Search for string -> vector query
    query = await tbl.search("0.1")
    assert isinstance(query, AsyncVectorQuery)

    await tbl.create_index("text", config=FTS())

    query = await tbl.search("0.1")
    assert isinstance(query, AsyncHybridQuery)

    data_with_vecs = await tbl.to_arrow()
    data_with_vecs = data_with_vecs.replace_schema_metadata(None)
    tbl2 = await mem_db_async.create_table("test2", data_with_vecs)
    with pytest.raises(
        Exception,
        match=(
            "Cannot perform full text search unless an INVERTED index has been created"
        ),
    ):
        query = await (await tbl2.search("0.1")).to_arrow()


@pytest.mark.asyncio
async def test_query_search_specified(mem_db_async: AsyncConnection):
    nrows, ndims = 1000, 16
    data = pa.table(
        {
            "text": [str(i) for i in range(nrows)],
            "vector": pa.FixedSizeListArray.from_arrays(
                pc.random(nrows * ndims).cast(pa.float32()), ndims
            ),
        }
    )
    table = await mem_db_async.create_table("test", data)
    await table.create_index("text", config=FTS())

    # Validate that specifying fts, vector or hybrid gets the right query.
    q = [0.1] * ndims
    query = await table.search(q, query_type="vector")
    assert isinstance(query, AsyncVectorQuery)

    query = await table.search("0.1", query_type="fts")
    assert isinstance(query, AsyncFTSQuery)

    with pytest.raises(ValueError, match="Unknown query type: 'foo'"):
        await table.search("0.1", query_type="foo")

    with pytest.raises(
        ValueError, match="Column 'vector' has no registered embedding function"
    ) as e:
        await table.search("0.1", query_type="vector")

    assert "No embedding functions are registered for any columns" in exception_output(
        e
    )


# Helper method used in the following tests.  Looks at the simple python object `q` and
# checks that the properties match the expected values in kwargs.
def check_set_props(q, **kwargs):
    for k in dict(q):
        if not k.startswith("_"):
            if k in kwargs:
                assert kwargs[k] == getattr(q, k), (
                    f"{k} should be {kwargs[k]} but is {getattr(q, k)}"
                )
            else:
                assert getattr(q, k) is None, f"{k} should be None"


def test_query_serialization_sync(table: lancedb.table.Table):
    # Simple queries
    q = table.search().where("id = 1").limit(500).offset(10).to_query_object()
    check_set_props(q, limit=500, offset=10, filter="id = 1")

    q = table.search().select(["id", "vector"]).to_query_object()
    check_set_props(q, columns=["id", "vector"])

    q = table.search().with_row_id(True).to_query_object()
    check_set_props(q, with_row_id=True)

    # Vector queries
    q = table.search([5.0, 6.0]).limit(10).to_query_object()
    check_set_props(q, limit=10, vector_column="vector", vector=[5.0, 6.0])

    q = table.search([5.0, 6.0]).to_query_object()
    check_set_props(q, vector_column="vector", vector=[5.0, 6.0])

    q = (
        table.search([5.0, 6.0])
        .limit(10)
        .where("id = 1", prefilter=False)
        .to_query_object()
    )
    check_set_props(
        q,
        limit=10,
        vector_column="vector",
        filter="id = 1",
        postfilter=True,
        vector=[5.0, 6.0],
    )

    q = table.search([5.0, 6.0]).nprobes(10).refine_factor(5).to_query_object()
    check_set_props(
        q,
        vector_column="vector",
        vector=[5.0, 6.0],
        minimum_nprobes=10,
        maximum_nprobes=10,
        refine_factor=5,
    )

    q = table.search([5.0, 6.0]).minimum_nprobes(10).to_query_object()
    check_set_props(
        q,
        vector_column="vector",
        vector=[5.0, 6.0],
        minimum_nprobes=10,
        maximum_nprobes=None,
    )

    q = table.search([5.0, 6.0]).nprobes(50).to_query_object()
    check_set_props(
        q,
        vector_column="vector",
        vector=[5.0, 6.0],
        minimum_nprobes=50,
        maximum_nprobes=50,
    )

    q = table.search([5.0, 6.0]).maximum_nprobes(10).to_query_object()
    check_set_props(
        q,
        vector_column="vector",
        vector=[5.0, 6.0],
        maximum_nprobes=10,
        minimum_nprobes=None,
    )

    q = table.search([5.0, 6.0]).distance_range(0.0, 1.0).to_query_object()
    check_set_props(
        q, vector_column="vector", vector=[5.0, 6.0], lower_bound=0.0, upper_bound=1.0
    )

    q = table.search([5.0, 6.0]).distance_type("cosine").to_query_object()
    check_set_props(
        q, distance_type="cosine", vector_column="vector", vector=[5.0, 6.0]
    )

    q = table.search([5.0, 6.0]).ef(7).to_query_object()
    check_set_props(q, ef=7, vector_column="vector", vector=[5.0, 6.0])

    q = table.search([5.0, 6.0]).bypass_vector_index().to_query_object()
    check_set_props(
        q, bypass_vector_index=True, vector_column="vector", vector=[5.0, 6.0]
    )

    # FTS queries
    q = table.search("foo").limit(10).to_query_object()
    check_set_props(
        q, limit=10, full_text_query=FullTextSearchQuery(columns=[], query="foo")
    )

    q = table.search("foo", query_type="fts").to_query_object()
    check_set_props(q, full_text_query=FullTextSearchQuery(columns=[], query="foo"))


@pytest.mark.asyncio
async def test_query_serialization_async(table_async: AsyncTable):
    # Simple queries
    q = table_async.query().where("id = 1").limit(500).offset(10).to_query_object()
    check_set_props(q, limit=500, offset=10, filter="id = 1", with_row_id=False)

    q = table_async.query().select(["id", "vector"]).to_query_object()
    check_set_props(q, columns=["id", "vector"], with_row_id=False)

    q = table_async.query().with_row_id().to_query_object()
    check_set_props(q, with_row_id=True)

    sample_vector = [pa.array([5.0, 6.0], type=pa.float32())]

    # Vector queries
    q = (await table_async.search([5.0, 6.0])).limit(10).to_query_object()
    check_set_props(
        q,
        limit=10,
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
    )

    q = (await table_async.search([5.0, 6.0])).to_query_object()
    check_set_props(
        q,
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (await table_async.search([5.0, 6.0])).nprobes(50).to_query_object()
    check_set_props(
        q,
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=50,
        maximum_nprobes=50,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (
        (await table_async.search([5.0, 6.0]))
        .limit(10)
        .where("id = 1")
        .postfilter()
        .to_query_object()
    )
    check_set_props(
        q,
        limit=10,
        filter="id = 1",
        postfilter=True,
        vector=sample_vector,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
    )

    q = (
        (await table_async.search([5.0, 6.0]))
        .nprobes(10)
        .refine_factor(5)
        .to_query_object()
    )
    check_set_props(
        q,
        vector=sample_vector,
        minimum_nprobes=10,
        maximum_nprobes=10,
        refine_factor=5,
        postfilter=False,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (await table_async.search([5.0, 6.0])).minimum_nprobes(5).to_query_object()
    check_set_props(
        q,
        vector=sample_vector,
        minimum_nprobes=5,
        maximum_nprobes=20,
        postfilter=False,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (
        (await table_async.search([5.0, 6.0]))
        .distance_range(0.0, 1.0)
        .to_query_object()
    )
    check_set_props(
        q,
        vector=sample_vector,
        lower_bound=0.0,
        upper_bound=1.0,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (await table_async.search([5.0, 6.0])).distance_type("cosine").to_query_object()
    check_set_props(
        q,
        distance_type="cosine",
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (await table_async.search([5.0, 6.0])).ef(7).to_query_object()
    check_set_props(
        q,
        ef=7,
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        bypass_vector_index=False,
        limit=10,
    )

    q = (await table_async.search([5.0, 6.0])).bypass_vector_index().to_query_object()
    check_set_props(
        q,
        bypass_vector_index=True,
        vector=sample_vector,
        postfilter=False,
        minimum_nprobes=20,
        maximum_nprobes=20,
        with_row_id=False,
        limit=10,
    )

    # FTS queries
    match_query = MatchQuery("foo", "text")
    q = (await table_async.search(match_query)).limit(10).to_query_object()
    check_set_props(
        q,
        limit=10,
        full_text_query=FullTextSearchQuery(columns=None, query=match_query),
        with_row_id=False,
    )

    q = (await table_async.search(match_query)).to_query_object()
    check_set_props(
        q,
        full_text_query=FullTextSearchQuery(columns=None, query=match_query),
        with_row_id=False,
    )

    phrase_query = PhraseQuery("foo", "text", slop=1)
    q = (await table_async.search(phrase_query)).to_query_object()
    check_set_props(
        q,
        full_text_query=FullTextSearchQuery(columns=None, query=phrase_query),
        with_row_id=False,
    )


def test_query_timeout(tmp_path):
    # Use local directory instead of memory:// to add a bit of latency to
    # operations so a timeout of zero will trigger exceptions.
    db = lancedb.connect(tmp_path)
    data = pa.table(
        {
            "text": ["a", "b"],
            "vector": pa.FixedSizeListArray.from_arrays(
                pc.random(4).cast(pa.float32()), 2
            ),
        }
    )
    table = db.create_table("test", data)
    table.create_fts_index("text", use_tantivy=False)

    with pytest.raises(Exception, match="Query timeout"):
        table.search().where("text = 'a'").to_list(timeout=timedelta(0))

    with pytest.raises(Exception, match="Query timeout"):
        table.search([0.0, 0.0]).to_arrow(timeout=timedelta(0))

    with pytest.raises(Exception, match="Query timeout"):
        table.search("a", query_type="fts").to_pandas(timeout=timedelta(0))

    with pytest.raises(Exception, match="Query timeout"):
        table.search(query_type="hybrid").vector([0.0, 0.0]).text("a").to_arrow(
            timeout=timedelta(0)
        )


@pytest.mark.asyncio
async def test_query_timeout_async(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    data = pa.table(
        {
            "text": ["a", "b"],
            "vector": pa.FixedSizeListArray.from_arrays(
                pc.random(4).cast(pa.float32()), 2
            ),
        }
    )
    table = await db.create_table("test", data)
    await table.create_index("text", config=FTS())

    with pytest.raises(Exception, match="Query timeout"):
        await table.query().where("text != 'a'").to_list(timeout=timedelta(0))

    with pytest.raises(Exception, match="Query timeout"):
        await table.vector_search([0.0, 0.0]).to_arrow(timeout=timedelta(0))

    with pytest.raises(Exception, match="Query timeout"):
        await (await table.search("a", query_type="fts")).to_pandas(
            timeout=timedelta(0)
        )

    with pytest.raises(Exception, match="Query timeout"):
        await (
            table.query()
            .nearest_to_text("a")
            .nearest_to([0.0, 0.0])
            .to_list(timeout=timedelta(0))
        )
