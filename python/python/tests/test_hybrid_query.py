# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import random
from unittest import mock

import lancedb

from lancedb.query import AsyncFTSQuery, AsyncVectorQuery, LanceHybridQueryBuilder
from lancedb.rerankers.base import Reranker
from lancedb.rerankers.rrf import RRFReranker
import numpy as np
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
    # ensure we get results that would match well for text and vector.
    # 'dog' matches both the text query and the vector query, so rank fusion
    # over the full candidate window (#4758) puts it first.
    assert result["text"].to_pylist() == ["dog", "a"]

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
    # ensure we get results that would match well for text and vector.
    # 'dog' matches both the text query and the vector query, so rank fusion
    # over the full candidate window (#4758) puts it first.
    assert result["text"].to_pylist() == ["dog", "a"]
    assert result["_rowid"].to_pylist() == [3, 0]


def test_hybrid_fragment_constraints_fail_loud(sync_table: Table):
    query = (
        sync_table.search("dog", query_type="hybrid")
        .vector([0.5, -0.5])
        .fragment_ids([0])
    )

    with pytest.raises(NotImplementedError, match="fragment selection"):
        query.to_arrow()
    with pytest.raises(NotImplementedError, match="fragment selection"):
        query.explain_plan()


@pytest.mark.asyncio
async def test_async_hybrid_fragment_constraints_fail_loud(table: AsyncTable):
    query = (
        table.query().nearest_to([0.5, -0.5]).nearest_to_text("dog").fragment_ids([0])
    )

    with pytest.raises(NotImplementedError, match="fragment selection"):
        await query.to_arrow()
    with pytest.raises(NotImplementedError, match="fragment selection"):
        await query.analyze_plan()


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
    pytest.importorskip("lance")
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


@pytest.mark.asyncio
async def test_async_hybrid_query_offset(table: AsyncTable):
    # The offset window of a hybrid query must be a suffix of the same query
    # run without an offset. Skipping the first rows of each sub-query instead
    # of the first rows of the fused result silently changes which rows land in
    # the window.
    full = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .limit(4)
        .with_row_id()
        .to_arrow()
    )
    assert len(full) == 4

    second_page = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .offset(2)
        .limit(2)
        .with_row_id()
        .to_arrow()
    )
    assert second_page["_rowid"].to_pylist() == full["_rowid"].to_pylist()[2:]

    first_page = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .limit(2)
        .with_row_id()
        .to_arrow()
    )
    # Paging through the result must visit every row exactly once: no row
    # repeated from the previous page and none dropped between the two.
    paged = first_page["_rowid"].to_pylist() + second_page["_rowid"].to_pylist()
    assert sorted(paged) == sorted(full["_rowid"].to_pylist())


@pytest.mark.asyncio
async def test_async_hybrid_query_fts_first_default_limit(table: AsyncTable):
    # nearest_to() and nearest_to_text() build their new sibling sub-query from
    # scratch, and that is the sub-query the default limit ends up on. So the
    # side that carries the limit depends on the order the hybrid query was
    # built in, and looking at only one side loses the limit for half the ways
    # a hybrid query can be written. Without a limit the combined results are
    # not truncated at all and the whole union of both candidate lists is
    # returned.
    await table.add([{"text": "dog", "vector": [50.0 + i, 50.0]} for i in range(10)])

    result = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).to_arrow()
    )
    assert len(result) == 10

    offset_result = await (
        table.query().nearest_to_text("dog").nearest_to([0.1, 0.1]).offset(2).to_arrow()
    )
    assert len(offset_result) == 10


@pytest.mark.asyncio
async def test_async_hybrid_query_explain_plan_matches_execution(table: AsyncTable):
    # Paging rewrites the sub-queries: each one fetches enough rows to cover
    # limit + offset, and never fewer than the rank-fusion window (#4758),
    # with no offset of its own, and the window is sliced out after fusion.
    # The plans have to be built from those rewritten sub-queries, otherwise
    # explain_plan and analyze_plan describe a query that is never run.
    query = (
        table.query().nearest_to([0.0, 0.4]).nearest_to_text("dog").offset(2).limit(2)
    )
    await query.to_arrow()

    plan = await query.explain_plan()
    assert [
        line.strip() for line in plan.splitlines() if "GlobalLimitExec" in line
    ] == [
        "GlobalLimitExec: skip=0, fetch=50",
        "GlobalLimitExec: skip=0, fetch=50",
    ]

    analyzed = await query.analyze_plan()
    assert analyzed.count("skip=0, fetch=50") == 2
    assert "skip=2" not in analyzed


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


def test_hybrid_query_top_result_stable_across_limits(tmpdir_factory):
    # Regression test for #4758: the top result of a hybrid search must not
    # depend on `limit`. Each side used to fetch only `limit + offset` rows
    # before fusion, so asking for fewer results could return a different
    # first result instead of just a shorter list.
    rng = np.random.default_rng(0)
    random.seed(0)
    words = "alpha beta gamma delta epsilon zeta eta theta iota kappa".split()
    rows = [
        {
            "id": i,
            "text": " ".join(random.choice(words) for _ in range(12)),
            "vector": rng.random(16).astype("float32"),
        }
        for i in range(300)
    ]
    tmp_path = str(tmpdir_factory.mktemp("data"))
    table = lancedb.connect(tmp_path).create_table("t", data=rows)
    table.create_fts_index("text", with_position=False)

    query_vec = rng.random(16).astype("float32")

    def top_id(limit):
        result = (
            table.search(query_type="hybrid")
            .vector(query_vec)
            .text("alpha beta gamma")
            .limit(limit)
            .to_list()
        )
        assert len(result) == limit
        return result[0]["id"]

    assert top_id(1) == top_id(3) == top_id(10)


@pytest.mark.asyncio
async def test_async_hybrid_query_top_result_stable_across_limits(tmpdir_factory):
    # Async mirror of test_hybrid_query_top_result_stable_across_limits:
    # regression test for #4758. The top result of a hybrid search must not
    # depend on `limit`. The async path previously had no behavioral
    # assertion for this — only the explain-plan fetch=50 check — so a
    # regression on this side would have slipped through.
    rng = np.random.default_rng(0)
    random.seed(0)
    words = "alpha beta gamma delta epsilon zeta eta theta iota kappa".split()
    rows = [
        {
            "id": i,
            "text": " ".join(random.choice(words) for _ in range(12)),
            "vector": rng.random(16).astype("float32"),
        }
        for i in range(300)
    ]
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("t", data=rows)
    await table.create_index("text", config=FTS(with_position=False))

    query_vec = rng.random(16).astype("float32")

    async def top_id(limit):
        result = await (
            table.query()
            .nearest_to(query_vec)
            .nearest_to_text("alpha beta gamma")
            .limit(limit)
            .to_list()
        )
        assert len(result) == limit
        return result[0]["id"]

    assert await top_id(1) == await top_id(3) == await top_id(10)


@pytest.mark.parametrize(
    "query_vector",
    [
        pytest.param((0.0, 0.4), id="tuple"),
        pytest.param(pa.array([0.0, 0.4], type=pa.float32()), id="pa-array"),
        pytest.param(
            pa.chunked_array([[0.0], [0.4]], type=pa.float32()), id="pa-chunked-array"
        ),
    ],
)
def test_hybrid_query_accepts_every_vector_container(sync_table: Table, query_vector):
    # Vector search and the async hybrid API both accept tuples and pyarrow
    # arrays for the query vector (``VEC`` in ``lancedb/common.py``), so the
    # sync hybrid builder must accept them too.
    expected = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .limit(2)
        .with_row_id(True)
        .to_arrow()
    )

    result = (
        sync_table.search(query_type="hybrid")
        .vector(query_vector)
        .text("dog")
        .limit(2)
        .with_row_id(True)
        .to_arrow()
    )

    assert result["_rowid"].to_pylist() == expected["_rowid"].to_pylist()


def test_hybrid_query_default_limit(sync_table: Table):
    # The vector and FTS legs match disjoint rows, so fusing their results
    # yields more rows than the default limit.
    new_rows = []
    # Distinct distances and document lengths avoid ties in either leg,
    # so comparing offset windows does not depend on tie ordering.
    for i in range(20):
        coordinate = 0.11 + i * 0.001
        new_rows.append({"text": "close_vec", "vector": [coordinate, coordinate]})
        new_rows.append(
            {"text": "dog " + "padding " * (i + 1), "vector": [50.0 + i, 50.0 + i]}
        )
    sync_table.add(new_rows)

    def query():
        return (
            sync_table.search(query_type="hybrid")
            .vector([0.1, 0.1])
            .text("dog")
            .with_row_id(True)
        )

    # Like the async hybrid query, the default limit is 10.
    result = query().to_arrow()
    assert len(result) == 10

    # The offset window is taken after the default limit is applied.
    full = query().limit(15).to_arrow()
    offset_result = query().offset(5).to_arrow()
    assert offset_result["_rowid"].to_pylist() == full["_rowid"].to_pylist()[5:]


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


def test_hybrid_query_nprobes_remains_independent(sync_table: Table):
    query = sync_table.search(query_type="hybrid").nprobes(20)

    assert query._nprobes == 20
    assert query._minimum_nprobes is None
    assert query._maximum_nprobes is None

    query = query.minimum_nprobes(5).nprobes(20)

    assert query._nprobes == 20
    assert query._minimum_nprobes == 5
    assert query._maximum_nprobes is None


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


@pytest.mark.parametrize("normalize", ["score", "rank"])
def test_hybrid_single_fts_match_keeps_fts_weight(tmp_path, normalize):
    from lancedb.rerankers import LinearCombinationReranker

    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "test",
        [
            {"text": "apple banana", "vector": [0.0, 0.0]},
            {"text": "zebra unicorn", "vector": [0.1, 0.0]},
            {"text": "cherry grape", "vector": [5.0, 5.0]},
        ],
    )
    table.create_fts_index("text")

    results = (
        table.search(query_type="hybrid")
        .vector([0.0, 0.0])
        .text("zebra")
        .rerank(LinearCombinationReranker(weight=0.3), normalize=normalize)
        .limit(3)
        .to_arrow()
    )

    assert results["text"].to_pylist() == [
        "zebra unicorn",
        "apple banana",
        "cherry grape",
    ]


def test_hybrid_query_rejects_query_with_vector(sync_table: Table):
    # Pins the full message: the space between "hybrid search." and "But" is
    # load-bearing (see the _validate_query literal concatenation).
    with pytest.raises(ValueError) as excinfo:
        sync_table.search("dog", query_type="hybrid").vector([0.5, -0.5]).to_arrow()
    assert str(excinfo.value) == (
        "You can either provide a string query in search() method "
        "or set `vector()` and `text()` explicitly for hybrid search. "
        "But not both."
    )


class _RecordingReranker(Reranker):
    """Records how many candidates each side of a hybrid query hands over."""

    def __init__(self):
        super().__init__()
        self.calls = []

    def rerank_hybrid(self, query, vector_results, fts_results):
        self.calls.append((len(vector_results), len(fts_results)))
        combined = self.merge_results(vector_results, fts_results)
        scores = pa.array([1.0] * len(combined), type=pa.float32())
        return combined.append_column("_relevance_score", scores)


def _candidate_rows():
    rng = np.random.default_rng(0)
    return pa.table(
        {
            "id": pa.array(range(100), type=pa.int64()),
            "text": pa.array(["match"] * 100),
            "vector": pa.array(
                rng.random((100, 2)).astype("float32").tolist(),
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )


def test_hybrid_query_custom_reranker_keeps_candidate_budget(tmpdir_factory):
    # The #4758 candidate floor only applies to the default RRF reranker.
    # Custom and model rerankers get every candidate, so they must keep
    # receiving `limit + offset` rows per side.
    db = lancedb.connect(str(tmpdir_factory.mktemp("data")))
    table = db.create_table("t", _candidate_rows())
    table.create_fts_index("text", with_position=False)

    reranker = _RecordingReranker()
    result = (
        table.search(query_type="hybrid")
        .vector([0.5, 0.5])
        .text("match")
        .rerank(reranker)
        .limit(1)
        .to_list()
    )
    assert len(result) == 1
    assert reranker.calls == [(1, 1)]

    reranker = _RecordingReranker()
    result = (
        table.search(query_type="hybrid")
        .vector([0.5, 0.5])
        .text("match")
        .rerank(reranker)
        .offset(2)
        .limit(3)
        .to_list()
    )
    assert len(result) == 3
    assert reranker.calls == [(5, 5)]


@pytest.mark.asyncio
async def test_async_hybrid_query_custom_reranker_keeps_candidate_budget(
    tmpdir_factory,
):
    # Async mirror of test_hybrid_query_custom_reranker_keeps_candidate_budget.
    db = await lancedb.connect_async(str(tmpdir_factory.mktemp("data")))
    table = await db.create_table("t", _candidate_rows())
    await table.create_index("text", config=FTS(with_position=False))

    reranker = _RecordingReranker()
    result = await (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .rerank(reranker)
        .limit(1)
        .to_arrow()
    )
    assert len(result) == 1
    assert reranker.calls == [(1, 1)]

    reranker = _RecordingReranker()
    result = await (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .rerank(reranker)
        .offset(2)
        .limit(3)
        .to_arrow()
    )
    assert len(result) == 3
    assert reranker.calls == [(5, 5)]


def _counting_child_queries():
    # Count executions of the two hybrid sub-queries: a limit(0) page must
    # run neither retrieval, so both counters have to stay at zero.
    executions = []
    original_fts = AsyncFTSQuery.to_arrow
    original_vec = AsyncVectorQuery.to_arrow

    async def counting_fts(self, *args, **kwargs):
        executions.append("fts")
        return await original_fts(self, *args, **kwargs)

    async def counting_vec(self, *args, **kwargs):
        executions.append("vec")
        return await original_vec(self, *args, **kwargs)

    patchers = (
        mock.patch.object(AsyncFTSQuery, "to_arrow", counting_fts),
        mock.patch.object(AsyncVectorQuery, "to_arrow", counting_vec),
    )
    return executions, patchers


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [0, 2])
async def test_async_hybrid_query_empty_page_does_not_fetch_candidates(
    tmpdir_factory, offset
):
    # An empty page must not get the #4758 candidate floor, and must not
    # fetch or score anything at all: limit(0) is rejected before either
    # sub-query runs, whatever the offset. With offset(2) the sub-queries
    # would otherwise carry k=2 and fetch two rows per side for nothing.
    db = await lancedb.connect_async(str(tmpdir_factory.mktemp("data")))
    table = await db.create_table("t", _candidate_rows())
    await table.create_index("text", config=FTS(with_position=False))

    reranker = _RecordingReranker()
    executions, patchers = _counting_child_queries()
    query = (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .rerank(reranker)
        .offset(offset)
        .limit(0)
    )
    with patchers[0], patchers[1]:
        with pytest.raises(ValueError, match="k must be positive"):
            await query.to_arrow()
    assert reranker.calls == []
    assert executions == []


def _counting_leg_plans():
    # Count plan executions of the two hybrid legs: analyze_plan runs both
    # legs, so a limit(0) page must leave both counters at zero.
    executions = []
    original_fts = AsyncFTSQuery.analyze_plan
    original_vec = AsyncVectorQuery.analyze_plan

    async def counting_fts(self, *args, **kwargs):
        executions.append("fts")
        return await original_fts(self, *args, **kwargs)

    async def counting_vec(self, *args, **kwargs):
        executions.append("vec")
        return await original_vec(self, *args, **kwargs)

    patchers = (
        mock.patch.object(AsyncFTSQuery, "analyze_plan", counting_fts),
        mock.patch.object(AsyncVectorQuery, "analyze_plan", counting_vec),
    )
    return executions, patchers


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [0, 2])
async def test_async_hybrid_query_empty_page_analyze_plan_does_not_execute(
    tmpdir_factory, offset
):
    # analyze_plan does not go through _run_hybrid: it executes both legs
    # itself, so it has to reject an explicit zero limit before either leg
    # runs, whatever the offset.
    db = await lancedb.connect_async(str(tmpdir_factory.mktemp("data")))
    table = await db.create_table("t", _candidate_rows())
    await table.create_index("text", config=FTS(with_position=False))

    executions, patchers = _counting_leg_plans()
    query = (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .offset(offset)
        .limit(0)
    )
    with patchers[0], patchers[1]:
        with pytest.raises(ValueError, match="k must be positive"):
            await query.analyze_plan()
    assert executions == []


def _candidate_rows_with_blob():
    # _candidate_rows plus a blob v2 column, built from the storage struct
    # the way test_blob.py builds blob arrays.
    rows = _candidate_rows()
    field = lancedb.blob("payload")
    storage_type = field.type.storage_type
    storage = pa.StructArray.from_arrays(
        [
            pa.array(
                [b"blob-%d" % i for i in range(rows.num_rows)],
                type=pa.large_binary(),
            ),
            pa.array([None] * rows.num_rows, type=pa.string()),
            pa.array([None] * rows.num_rows, type=pa.uint64()),
            pa.array([None] * rows.num_rows, type=pa.uint64()),
        ],
        fields=list(storage_type),
    )
    payload = pa.ExtensionArray.from_storage(field.type, storage)
    return rows.append_column(field, payload)


def _counting_vector_output_schema():
    # Count Blob schema discovery calls: _blob_output_schema asks the
    # vector leg for its output schema, which on a remote table sends the
    # query itself, so a limit(0) page must leave this counter at zero.
    calls = []
    original = AsyncVectorQuery.output_schema

    async def counting(self, *args, **kwargs):
        calls.append(1)
        return await original(self, *args, **kwargs)

    patcher = mock.patch.object(AsyncVectorQuery, "output_schema", counting)
    return calls, patcher


@pytest.mark.asyncio
async def test_async_hybrid_query_empty_page_blob_bytes_skips_discovery(
    tmpdir_factory,
):
    # to_arrow(blob_mode="bytes") discovers the Blob output schema before
    # the query runs, and on a remote table that discovery sends the
    # vector query itself. An explicit zero limit has to be rejected
    # before discovery runs, not by it.
    db = await lancedb.connect_async(str(tmpdir_factory.mktemp("data")))
    table = await db.create_table("t", _candidate_rows_with_blob())
    await table.create_index("text", config=FTS(with_position=False))

    reranker = _RecordingReranker()
    executions, exec_patchers = _counting_child_queries()
    schema_calls, schema_patcher = _counting_vector_output_schema()
    query = (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .rerank(reranker)
        .limit(0)
    )
    with exec_patchers[0], exec_patchers[1], schema_patcher:
        with pytest.raises(ValueError, match="k must be positive") as excinfo:
            await query.to_arrow(blob_mode="bytes")
    # The rejection is the query builder's, not the native "Invalid input"
    # error the undiscovered schema call would have raised instead.
    assert str(excinfo.value) == (
        "k must be positive: a hybrid query needs limit(1) or more"
    )
    assert schema_calls == []
    assert executions == []
    assert reranker.calls == []

    # A positive limit still discovers the schema and returns the rows.
    result = await (
        table.query()
        .nearest_to([0.5, 0.5])
        .nearest_to_text("match")
        .limit(2)
        .to_arrow(blob_mode="bytes")
    )
    assert len(result) == 2


@pytest.mark.asyncio
async def test_async_hybrid_query_default_rrf_candidate_window(tmpdir_factory):
    # The default RRF reranker gets the #4758 floor: 50 candidates per side
    # even when a single row is requested.
    db = await lancedb.connect_async(str(tmpdir_factory.mktemp("data")))
    table = await db.create_table("t", _candidate_rows())
    await table.create_index("text", config=FTS(with_position=False))

    seen = []
    original = RRFReranker.rerank_hybrid

    def recording_rerank_hybrid(self, query, vector_results, fts_results, *a, **kw):
        seen.append((len(vector_results), len(fts_results)))
        return original(self, query, vector_results, fts_results, *a, **kw)

    with mock.patch.object(RRFReranker, "rerank_hybrid", recording_rerank_hybrid):
        result = await (
            table.query()
            .nearest_to([0.5, 0.5])
            .nearest_to_text("match")
            .limit(1)
            .to_arrow()
        )
    assert len(result) == 1
    assert seen == [(50, 50)]
