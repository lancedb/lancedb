# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import itertools

import lancedb

from lancedb.query import LanceHybridQueryBuilder
from lancedb.rerankers.base import Reranker
from lancedb.rerankers.rrf import RRFReranker
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import pytest_asyncio

from lancedb.index import FTS
from lancedb.table import AsyncTable, Table


def assert_select_projections(make_query, *, extra_columns=None):
    """Assert that .select() returns exactly the requested columns with correct data.

    Runs the query to discover default columns, then verifies that every
    permutation of every non-empty subset of columns (including extra_columns)
    produces results matching a projection of the full result.

    Parameters
    ----------
    make_query : callable
        Returns a fresh query builder (without .select()). Must support
        .select() and .to_arrow().
    extra_columns : list of str, optional
        Columns not in default output but available for selection
        (e.g. ["_score", "_distance"] for hybrid search).
    """
    default = make_query().to_arrow()
    all_columns = list(default.column_names)

    if extra_columns:
        for col in extra_columns:
            assert col not in default.column_names, (
                f"extra column {col!r} should not be in default output"
            )
        all_columns.extend(extra_columns)

    full = make_query().select(all_columns).to_arrow()

    for r in range(1, len(all_columns) + 1):
        for perm in itertools.permutations(all_columns, r):
            cols = list(perm)
            actual = make_query().select(cols).to_arrow()
            expected = full.select(cols)
            assert actual == expected, f"select({cols}) mismatch"


async def assert_select_projections_async(make_query, *, extra_columns=None):
    """Async version of assert_select_projections."""
    default = await make_query().to_arrow()
    all_columns = list(default.column_names)

    if extra_columns:
        for col in extra_columns:
            assert col not in default.column_names, (
                f"extra column {col!r} should not be in default output"
            )
        all_columns.extend(extra_columns)

    full = await make_query().select(all_columns).to_arrow()

    for r in range(1, len(all_columns) + 1):
        for perm in itertools.permutations(all_columns, r):
            cols = list(perm)
            actual = await make_query().select(cols).to_arrow()
            expected = full.select(cols)
            assert actual == expected, f"select({cols}) mismatch"


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
        .select(["text", "_distance"])
        .rerank(reranker)
        .limit(2)
        .to_arrow()
    )
    assert len(result) == 2
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
        .select(["text", "_distance"])
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
async def test_async_hybrid_query_select(table: AsyncTable):
    await assert_select_projections_async(
        lambda: table.query().nearest_to([0.0, 0.4]).nearest_to_text("dog").limit(2),
        extra_columns=["_score", "_distance"],
    )

    # Verify null patterns: each row came from at least one sub-query,
    # so at least one of _score/_distance must be non-null.
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .select(["_score", "_distance"])
        .limit(4)
        .to_arrow()
    )
    for i in range(len(result)):
        s = result["_score"][i].as_py()
        d = result["_distance"][i].as_py()
        assert s is not None or d is not None


def test_sync_hybrid_query_select(sync_table: Table):
    assert_select_projections(
        lambda: (
            sync_table.search(query_type="hybrid")
            .vector([0.0, 0.4])
            .text("dog")
            .limit(2)
        ),
        extra_columns=["_score", "_distance"],
    )

    # Verify null patterns: each row came from at least one sub-query,
    # so at least one of _score/_distance must be non-null.
    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["_score", "_distance"])
        .limit(4)
        .to_arrow()
    )
    for i in range(len(result)):
        s = result["_score"][i].as_py()
        d = result["_distance"][i].as_py()
        assert s is not None or d is not None


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
    assert result.column_names == ["upper_text", "_relevance_score"]


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
    assert result.column_names == ["text"]

    # Second call on the same builder should also apply the selection
    result = builder.to_arrow()
    assert result.column_names == ["text"]

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
    assert result.column_names == ["upper_text", "_relevance_score"]


@pytest.mark.asyncio
async def test_async_hybrid_select_dict_clears_stale_list(table: AsyncTable):
    """Switching from a list select to a dict select must not apply the stale list."""
    query = table.query().nearest_to([0.0, 0.4]).nearest_to_text("dog").limit(2)
    # First set a list select
    query.select(["text"])
    # Then override with a dict select — the list must not be applied
    query.select({"upper_text": "upper(text)"})
    result = await query.to_arrow()
    assert result.column_names == ["upper_text", "_relevance_score"]


def test_sync_hybrid_select_with_tantivy(tmpdir_factory):
    """Projection pushdown must not pass synthetic columns to lance take()."""
    tantivy = pytest.importorskip("tantivy")  # noqa: F841
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
    table = db.create_table("test_tantivy", data)
    table.create_fts_index("text", use_tantivy=True)
    result = (
        table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["text"])
        .limit(2)
        .to_arrow()
    )
    assert result.column_names == ["text"]
    assert len(result) == 2


class _ColumnReadingReranker(Reranker):
    """Minimal reranker that reads a specific column from the merged results."""

    def __init__(self, column="text"):
        super().__init__()
        self.column = column

    def rerank_hybrid(self, query, vector_results, fts_results):
        combined = self.merge_results(vector_results, fts_results)
        # Access the column — this will raise if projection pushdown excluded it
        _ = combined[self.column]
        combined = combined.append_column(
            "_relevance_score",
            pa.array([1.0 / (i + 1) for i in range(len(combined))], type=pa.float32()),
        )
        return self._keep_relevance_score(combined)


def test_sync_hybrid_select_reranker_column(tmpdir_factory):
    """Reranker's required column must be fetched even when not user-selected."""
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = lancedb.connect(tmp_path)
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4]),
            "text": pa.array(["a", "b", "cat", "dog"]),
            "vector": pa.array(
                [[0.1, 0.1], [2, 2], [-0.1, -0.1], [0.5, -0.5]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = db.create_table("test_reranker", data)
    table.create_fts_index("text", with_position=False, use_tantivy=False)
    reranker = _ColumnReadingReranker(column="text")
    result = (
        table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["id"])
        .rerank(reranker)
        .limit(2)
        .to_arrow()
    )
    # Reranker had access to "text" internally, but the final output
    # should only contain user-selected columns.
    assert result.column_names == ["id"]
    assert len(result) == 2


@pytest.mark.asyncio
async def test_async_hybrid_select_reranker_column(tmpdir_factory):
    """Async: reranker's required column must be fetched even when not selected."""
    tmp_path = str(tmpdir_factory.mktemp("data"))
    db = await lancedb.connect_async(tmp_path)
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4]),
            "text": pa.array(["a", "b", "cat", "dog"]),
            "vector": pa.array(
                [[0.1, 0.1], [2, 2], [-0.1, -0.1], [0.5, -0.5]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = await db.create_table("test_reranker", data)
    await table.create_index("text", config=FTS(with_position=False))
    reranker = _ColumnReadingReranker(column="text")
    result = await (
        table.query()
        .nearest_to([0.0, 0.4])
        .nearest_to_text("dog")
        .select(["id"])
        .rerank(reranker)
        .limit(2)
        .to_arrow()
    )
    assert result.column_names == ["id"]
    assert len(result) == 2


def test_sync_tantivy_select_only_score(tmpdir_factory):
    """Selecting only synthetic columns should return just those columns."""
    tantivy = pytest.importorskip("tantivy")  # noqa: F841
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
    table = db.create_table("test_synth", data)
    table.create_fts_index("text", use_tantivy=True)
    result = table.search("dog").select(["_score"]).limit(2).to_arrow()
    assert result.column_names == ["_score"]
    assert len(result) == 1  # only "dog" matches


def test_sync_tantivy_select_nonexistent_synthetic(tmpdir_factory):
    """Selecting a synthetic column that doesn't exist should raise."""
    tantivy = pytest.importorskip("tantivy")  # noqa: F841
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
    table = db.create_table("test_bad_synth", data)
    table.create_fts_index("text", use_tantivy=True)
    # _relevance_score only exists after reranking; standalone FTS doesn't have it
    with pytest.raises(ValueError, match="do not exist"):
        table.search("dog").select(["_relevance_score"]).limit(2).to_arrow()


def test_sync_hybrid_reranker_column_none(sync_table: Table):
    """Reranker with column=None should not break projection pushdown."""

    class _NoneColumnReranker(Reranker):
        def __init__(self):
            super().__init__()
            self.column = None

        def rerank_hybrid(self, query, vector_results, fts_results):
            combined = self.merge_results(vector_results, fts_results)
            combined = combined.append_column(
                "_relevance_score",
                pa.array(
                    [1.0 / (i + 1) for i in range(len(combined))],
                    type=pa.float32(),
                ),
            )
            return self._keep_relevance_score(combined)

    result = (
        sync_table.search(query_type="hybrid")
        .vector([0.0, 0.4])
        .text("dog")
        .select(["text"])
        .rerank(_NoneColumnReranker())
        .limit(2)
        .to_arrow()
    )
    assert result.column_names == ["text"]
    assert len(result) == 2


def test_reranker_column_deps():
    """_reranker_column_deps validates the column attribute."""
    assert LanceHybridQueryBuilder._reranker_column_deps(None) == []
    assert LanceHybridQueryBuilder._reranker_column_deps(RRFReranker()) == []

    class _HasColumn:
        column = "text"

    assert LanceHybridQueryBuilder._reranker_column_deps(_HasColumn()) == ["text"]

    class _NoneColumn:
        column = None

    assert LanceHybridQueryBuilder._reranker_column_deps(_NoneColumn()) == []

    class _EmptyColumn:
        column = ""

    assert LanceHybridQueryBuilder._reranker_column_deps(_EmptyColumn()) == []
