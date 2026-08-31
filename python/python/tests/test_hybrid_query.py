# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from unittest import mock

import lancedb

from lancedb.query import LanceHybridQueryBuilder
from lancedb.rerankers.rrf import RRFReranker
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import pytest_asyncio

from lancedb.index import BTree, FTS, IvfPq
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
    table.create_fts_index("text", with_position=False)
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
async def test_hybrid_query_with_stale_fixed_size_binary_prefilter(
    tmpdir_factory,
):
    tmp_path = str(tmpdir_factory.mktemp("stale_scalar_prefilter"))
    db = await lancedb.connect_async(tmp_path)

    def fixed_size_binary(value: int) -> bytes:
        return value.to_bytes(16, byteorder="big")

    num_rows = 1000
    data = pa.table(
        {
            "space_id": pa.array(
                [fixed_size_binary(i) for i in range(num_rows)],
                type=pa.binary(16),
            ),
            "text": ["book"] * num_rows,
            "vector": pa.array(
                [[float(i), float(i)] for i in range(num_rows)],
                type=pa.list_(pa.float32(), 2),
            ),
        }
    )
    table = await db.create_table("test", data)
    await table.create_index(
        "vector", config=IvfPq(num_partitions=4, num_sub_vectors=2)
    )
    await table.create_index("space_id", config=BTree())
    await table.create_index("text", config=FTS(with_position=False))

    # Advance the search indices without advancing the scalar index. This is the
    # state that previously let hybrid search use an incomplete scalar prefilter.
    await table.add(data)
    lance_dataset = await table.to_lance()
    lance_dataset.optimize.optimize_indices(index_names=["vector_idx", "text_idx"])
    await table.checkout_latest()

    scalar_stats = await table.index_stats("space_id_idx")
    assert scalar_stats is not None
    assert scalar_stats.num_indexed_rows == num_rows
    assert scalar_stats.num_unindexed_rows == num_rows

    for index_name in ["vector_idx", "text_idx"]:
        search_stats = await table.index_stats(index_name)
        assert search_stats is not None
        assert search_stats.num_indexed_rows == num_rows * 2
        assert search_stats.num_unindexed_rows == 0

    matching_ids = [5, 10, 15, 20, 25, 30]
    literals = [
        f"arrow_cast(0x{fixed_size_binary(i).hex()}, 'FixedSizeBinary(16)')"
        for i in matching_ids
    ]
    predicate = f"space_id IN ({', '.join(literals)})"
    expected_ids = sorted(fixed_size_binary(i) for i in matching_ids for _ in range(2))

    vector_query = (
        table.query().where(predicate).nearest_to([5.0, 5.0]).limit(num_rows * 2)
    )
    vector_results = await vector_query.to_arrow()
    assert sorted(vector_results["space_id"].to_pylist()) == expected_ids

    fts_query = (
        table.query().where(predicate).nearest_to_text("book").limit(num_rows * 2)
    )
    fts_results = await fts_query.to_arrow()
    assert sorted(fts_results["space_id"].to_pylist()) == expected_ids

    hybrid_results = await (
        table.query()
        .where(predicate)
        .nearest_to([5.0, 5.0])
        .nearest_to_text("book")
        .limit(num_rows * 2)
        .to_arrow()
    )
    assert sorted(hybrid_results["space_id"].to_pylist()) == expected_ids


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


def test_hybrid_query_offset(sync_table: Table):
    # The offset window of a hybrid query must be a suffix of the same query
    # run without an offset -- it must not be silently ignored.
    full = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .limit(4)
        .with_row_id(True)
        .to_arrow()
    )
    assert len(full) == 4

    offset_result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .offset(2)
        .limit(2)
        .with_row_id(True)
        .to_arrow()
    )
    assert offset_result["_rowid"].to_pylist() == full["_rowid"].to_pylist()[2:]


def test_hybrid_query_minimum_nprobes_zero_raises(sync_table: Table):
    # minimum_nprobes(0) must raise the same validation error a plain vector
    # query raises, not silently no-op because 0 is falsy.
    with pytest.raises(ValueError, match="minimum_nprobes must be greater than 0"):
        (
            sync_table.search(query_type="hybrid")
            .vector([0.0, 0.4])
            .text("dog")
            .minimum_nprobes(0)
            .to_arrow()
        )


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


def test_hybrid_query_applies_zero_upper_distance_bound(sync_table: Table):
    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("elephant")
        .distance_range(upper_bound=0.0)
        .rerank(RRFReranker(return_score="all"))
        .limit(4)
        .to_arrow()
    )

    assert len(result) == 0


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

    assert "KNNVectorDistance" in plan
    assert "LanceRead" in plan


@pytest.mark.asyncio
async def test_analyze_plan(table: AsyncTable):
    res = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).analyze_plan()
    )

    assert "AnalyzeExec" in res
    assert "metrics=" in res


def test_hybrid_phrase_query_is_preserved_in_analyze_plan():
    table = mock.Mock()
    analyzed_queries = []
    distributed_metric_modes = []

    def capture_query(query, *, distributed_metrics="aggregate"):
        analyzed_queries.append(query)
        distributed_metric_modes.append(distributed_metrics)
        return ""

    table._analyze_plan.side_effect = capture_query

    (
        LanceHybridQueryBuilder(table)
        .vector([0.1, 0.2])
        .text("puppy runs")
        .phrase_query()
        .analyze_plan(distributed_metrics="full")
    )

    assert len(analyzed_queries) == 2
    assert analyzed_queries[1].full_text_query.query == '"puppy runs"'
    assert distributed_metric_modes == ["full", "full"]


@pytest.fixture
def table_with_id(tmpdir_factory) -> Table:
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = lancedb.connect(tmp_path)
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "text": pa.array(["a", "b", "cat", "dog"]),
            "vector": pa.array(
                [[0.1, 0.1], [2, 2], [-0.1, -0.1], [0.5, -0.5]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = db.create_table("test_with_id", data)
    table.create_fts_index("text", with_position=False)
    return table


def test_hybrid_prefilter_explain_plan(table_with_id: Table):
    """
    Verify that the prefilter logic is not inverted in LanceHybridQueryBuilder.
    """
    plan_prefilter = (
        table_with_id.search(query_type="hybrid")
        .vector([0.0, 0.0])
        .text("dog")
        .where("id = 1", prefilter=True)
        .limit(2)
        .explain_plan(verbose=True)
    )

    plan_postfilter = (
        table_with_id.search(query_type="hybrid")
        .vector([0.0, 0.0])
        .text("dog")
        .where("id = 1", prefilter=False)
        .limit(2)
        .explain_plan(verbose=True)
    )

    # prefilter=True: filter is pushed into the LanceRead scan.
    # The FTS sub-plan exposes this as "full_filter=id = Int64(1)" inside LanceRead.
    assert "full_filter=id = Int64(1)" in plan_prefilter, (
        f"Should push the filter into the scan.\nPlan:\n{plan_prefilter}"
    )

    # prefilter=False: filter is applied as a separate FilterExec after the search.
    # The filter must NOT be embedded in the scan.
    assert "full_filter=id = Int64(1)" not in plan_postfilter, (
        f"Should NOT push the filter into the scan.\nPlan:\n{plan_postfilter}"
    )


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
